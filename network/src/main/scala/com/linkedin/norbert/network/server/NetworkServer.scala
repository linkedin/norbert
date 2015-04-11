/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package network
package server

import java.util.concurrent.atomic.AtomicBoolean
import java.net.InetAddress
import netty.{NettyNetworkServer, NetworkServerConfig}
import cluster._
import logging.Logging

object NetworkServer {
  def apply(config: NetworkServerConfig): NetworkServer = new NettyNetworkServer(config)
}

trait NetworkServer extends Logging {
  this: ClusterClientComponent with ClusterIoServerComponent with MessageExecutorComponent with MessageHandlerRegistryComponent =>

  @volatile private var markAvailableWhenConnected = true
  private var listenerKey: ClusterListenerKey = _
  private var nodeOption: Option[Node] = None
  private val shutdownSwitch = new AtomicBoolean

  /**
   * Registers a message handler with the <code>NetworkServer</code>. The <code>NetworkServer</code> will call the
   * provided handler when an incoming request of type <code>requestMessage</code> is received.  If a response is
   * expected then a response message should also be provided.
   *
   * @param requestMessage an instance of an incoming request message
   * @param responseMessage an instance of an outgoing response message
   * @param handler the function to call when an incoming message of type <code>requestMessage</code> is recieved
   */

  def registerHandler[RequestMsg, ResponseMsg](handler: RequestMsg => ResponseMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    messageHandlerRegistry.registerHandler(handler)
  }

  def addFilters(filters: List[Filter]) : Unit = messageExecutor.addFilters(filters)

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the unique URL (machine and port) of the machine. It will look through all nodes in zookeeper to identify it's
   * own nodeId. There may be issues with this for machines with multiple interfaces, etc as it uses InetAddress.getLocalHost.
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified machine's URL is configured in zookeeper
   * @throws NetworkingException thrown if unable to bind
   */
  def bindByPort(port: Int): Unit = bindByUrl(InetAddress.getLocalHost.getCanonicalHostName, port)

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the unique URL (machine and port) of the machine. It will look through all nodes in zookeeper to identify it's
   * own nodeId.
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified machine's URL is configured in zookeeper
   * @throws NetworkingException thrown if unable to bind
   */
  def bindByUrl(hostname: String, port: Int, markAvailable: Boolean = true): Unit = {
    log.debug("Ensuring ClusterClient is started")
    clusterClient.start
    clusterClient.awaitConnectionUninterruptibly

    val node = clusterClient.nodeByUrl(hostname, port).getOrElse(throw new InvalidNodeException("No node for URL: %s:%d exists".format(hostname, port)))

    //Bind with that node
    bindNode(node, markAvailable)
  }

  def bindOrCreateByUrl(hostname: String, port: Int, partitionBuilder:Option[Int => Set[Int]] = None): Unit = {
    try {
      bindByUrl(hostname, port)
    } catch {
      case ex:InvalidNodeException =>
        /* This node doesn't exist so we need to create it */
        clusterClient.addNodeByUrl(hostname, port, partitionBuilder)
        bindByUrl(hostname, port)
    }
  }

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the provided nodeId and automatically marks the <code>Node</code> available in the cluster.  A
   * <code>Node</code>'s url must be specified in the format hostname:port.
   *
   * @param nodeId the id of the <code>Node</code> this server is associated with.
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified <code>nodeId</code> exists
   * @throws NetworkingException thrown if unable to bind
   */
  def bind(nodeId: Int): Unit = bind(nodeId, true)

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the provided nodeId and marks the <code>Node</code> available in the cluster if <code>markAvailable</code> is true.  A
   * <code>Node</code>'s url must be specified in the format hostname:port.
   *
   * @param nodeId the id of the <code>Node</code> this server is associated with.
   * @param markAvailable if true marks the <code>Node</code> identified by <code>nodeId</code> as available after binding to
   * the port
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified <code>nodeId</code> exists or if the
   * format of the <code>Node</code>'s url isn't hostname:port
   * @throws NetworkingException thrown if unable to bind
   */
  def bind(nodeId: Int, markAvailable: Boolean, initialCapability: Long = 0L): Unit = doIfNotShutdown {
    if (nodeOption.isDefined) throw new NetworkingException("Attempt to bind an already bound NetworkServer")

    log.info("Starting NetworkServer...")

    log.debug("Ensuring ClusterClient is started")
    clusterClient.start
    clusterClient.awaitConnectionUninterruptibly

    val node = clusterClient.nodeWithId(nodeId).getOrElse(throw new InvalidNodeException("No node with id %d exists".format(nodeId)))
    bindNode(node, markAvailable, initialCapability)
  }

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the node passed in. The node will need to be found through other means first. Note: there is no validation to
   * check the validity of the node, that should be done before calling this method.
   *
   * @throws NetworkingException thrown if unable to bind
   */
  private def bindNode(node: Node, markAvailable: Boolean, initialCapability: Long = 0L): Unit = doIfNotShutdown {
    clusterIoServer.bind(node, true)

    nodeOption = Some(node)
    markAvailableWhenConnected = markAvailable

    log.debug("Registering with ClusterClient")
    listenerKey = clusterClient.addListener(new ClusterListener {
      def handleClusterEvent(event: ClusterEvent) = event match {
        case ClusterEvents.Connected(_) =>
          if (markAvailableWhenConnected) {
            log.debug("Marking node with id %d available".format(node.id))
            try {
              clusterClient.markNodeUnavailable(node.id)
              clusterClient.markNodeAvailable(node.id, initialCapability)
            } catch {
              case ex: ClusterException => log.error(ex, "Unable to mark node available")
            }
          }

        case ClusterEvents.Shutdown => doShutdown(true)

        case _ => // do nothing
      }
    })

    log.info("NetworkServer started")
  }

  /**
   * Returns the <code>Node</code> associated with this server.
   *
   * @return the <code>Node</code> associated with this server
   */
  def myNode: Node = doIfNotShutdown { nodeOption.getOrElse(throw new NetworkServerNotBoundException) }

  /**
   * Marks the node available in the cluster if the server is bound with initial capability string as 0L
   */
  def markAvailable : Unit =markAvailable(0L)

  /**
   * Marks the node available in the cluster if the server is bound with the initial capability string associated
   */
  def markAvailable(initialCapability: Long = 0L): Unit = {
    clusterClient.markNodeAvailable(myNode.id, initialCapability)
    markAvailableWhenConnected = true
  }

  /**
   * Marks the node unavailable in the cluster if bound.
   */
  def markUnavailable: Unit = {
    clusterClient.markNodeUnavailable(myNode.id)
    markAvailableWhenConnected = false
  }
  
  def setNodeCapabiity(capability: Long): Unit = {
    clusterClient.setNodeCapability(myNode.id, capability)
  }

  /**
   * Shuts down the network server. This results in unbinding from the port, closing the child sockets, and marking the node unavailable.
   */
  def shutdown: Unit = doShutdown(false)

  private def doShutdown(fromCluster: Boolean) {
    if (shutdownSwitch.compareAndSet(false, true)) {
      log.info("Shutting down NetworkServer for %s...".format(nodeOption.map(_.toString).getOrElse("[unbound]")))

      if (!fromCluster) {
        nodeOption.foreach { node =>
          try {
            log.debug("Unregistering from ClusterClient")
            clusterClient.removeListener(listenerKey)

            log.debug("Marking %s unavailable".format(node))
            clusterClient.markNodeUnavailable(node.id)
          } catch {
            case ex: ClusterShutdownException => // cluster already shut down, ignore
          }
        }
      }

      log.debug("Closing opened sockets")
      clusterIoServer.shutdown

      log.info("NetworkServer shut down")
    }
  }

  private def doIfNotShutdown[T](block: => T): T = if (shutdownSwitch.get) throw new NetworkShutdownException else block
}
