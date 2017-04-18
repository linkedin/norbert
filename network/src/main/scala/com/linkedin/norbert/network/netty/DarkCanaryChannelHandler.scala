/*
 * Copyright 2014- LinkedIn, Inc
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

package com.linkedin.norbert.network.netty

import org.jboss.netty.channel._
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.Request
import com.linkedin.norbert.network.common.{CanServeRequestStrategy, ClusterIoClientComponent}
import com.linkedin.norbert.cluster._
import java.util.concurrent.{ConcurrentHashMap => JConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.UUID
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.network.client.NetworkClientConfig
import com.linkedin.norbert.network.client.DarkCanaryResponseHandler

/**
 * This is a Netty channel handler which facilitates copying certain traffic to certain hosts based on configurations
 * received from Zookeeper. It is intended to be used as a way to send traffic to 'dark' replicas of a production service.
 * Responses from the 'dark' replica are never propagated to clients, and hence cannot affect the correctness of the
 * service.
 *
 * The way it works is as follows:
 *
 *   1) We instantiate a new Zookeeper client which connects to a new (dark) service name. This client listens to changes
 *      of membership / availability in this service name. The structure of this service name should be identical to the
 *      structure of the production service name whose traffic we wish to mirror.
 *
 *   2) Traffic is mirrored based on the Node ids of the nodes in the dark service name: when a request is being sent
 *      to production node id X, and if the dark service name also has a configuration for node id X, then the request
 *      will be copied to the node with id X in the dark configuration. This logic is handled by the DownStreamHandler
 *      class.
 *
 *   3) When a response comes back, its request id is checked by the UpstreamHandler class. If the request id of the
 *      response matches the request id of a previously replicated request, the response is dropped. This ensures that
 *      responses from the 'dark' hosts never reach clients.
 *
 * The DownstreamHandler is the first handler invoked in the downstream pipeline. THe UpstreamHandler is the first
 * handler invoked in the upstream pipeline.
 *
 * If no dark canary service name is configured, then the DownstreamHandler and UpstreamHandlers will be no-ops.
 *
 * This code is intended to be used with a NettyNetworkClient, which schedules each request and response on a separate
 * thread and executes requests asynchronously (ie. through select/poll/epoll etc. on channel sockets). This
 * architecture implies that failures / delays in the mirrored requests and responses will not affect the production
 * work load.
 */


class DarkCanaryChannelHandler extends Logging {
  private val hostToMirror = new JConcurrentHashMap[UUID, UUID]()
  private val mirrorToHost = new JConcurrentHashMap[UUID, UUID]()
  private val hostRequestMap = new JConcurrentHashMap[UUID, Request[Any,Any]]()
  private val mirrorRequestMap = new JConcurrentHashMap[UUID, Request[Any,Any]]()
  private val mirroredHosts= new JConcurrentHashMap[Int, Node]()
  private final val darkCanaryClientNameSuffix = "DarkCanary"
  private var clusterIoClient: Option[ClusterIoClientComponent#ClusterIoClient] = None
  private var clusterClient : Option[ClusterClient] = None
  private var staleRequestTimeoutMins : Int = 0
  private var staleRequestCleanupFrequencyMins : Int = 0
  private var darkCanaryResponseHandler: Option[DarkCanaryResponseHandler] = None
  private var canServeRequestStrategy : Option[CanServeRequestStrategy] = None

  class DarkCanaryException(message: String) extends Exception(message)

  def initialize(clientConfig : NetworkClientConfig,
                 clusterIoClient_ : ClusterIoClientComponent#ClusterIoClient,
                 strategy : CanServeRequestStrategy) = {
    clusterIoClient = Some(clusterIoClient_)
    darkCanaryResponseHandler =  clientConfig.darkCanaryResponseHandler
    canServeRequestStrategy = Some(strategy)

    // The configuration variables below determine the minimum age for cleaning up the Request objects in the
    // mirrorRequestMap. The clientConfig.{staleRequestTimeoutMins,staleRequestCleanupFrequenceMins} are the values used in
    // the upstream ClientChannelHandler to clean up stale requests. Hence the dark canary channel handler is configured
    // by default to both clean its request map less frequently, and also clean request objects which are older than
    // their counterparts in the ClientChannelHandler.
    //
    // This setup ensures request objects will be dropped from ClientChannelHandler.mirrorRequestMap _before_ they are dropped
    // from DarkCanaryChannelHandler.mirrorRequestMap, hence ensuring that dark canary responses can never be propagated
    // upstream.
    //
    // Do not change these configurations without careful thought.
    staleRequestTimeoutMins = clientConfig.staleRequestTimeoutMins + 1
    staleRequestCleanupFrequencyMins = clientConfig.staleRequestCleanupFrequenceMins + 1

    clientConfig.darkCanaryServiceName match {
      case None => {
        log.info("Dark canaries not configured for client %s".format(clientConfig.clientName))
      }
      case Some(serviceName) => {
        clusterClient = Some(ClusterClient(clientConfig.clientName + darkCanaryClientNameSuffix,
          serviceName,
          clientConfig.zooKeeperConnectString,
          clientConfig.zooKeeperSessionTimeoutMillis))

        clusterClient.get.addListener(new ClusterListener {
          /**
           * Handle a cluster event.
           *
           * @param event the <code>ClusterEvent</code> to handle
           */
          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(nodes) => updateCurrentState(nodes)
            case ClusterEvents.NodesChanged(nodes) => updateCurrentState(nodes)
            case ClusterEvents.Disconnected => updateCurrentState(Set.empty[Node])
          }

          private def updateCurrentState(nodes : Set[Node]) : Unit = {
            this.synchronized {
              mirroredHosts.clear()
              nodes.foreach { node =>
                mirroredHosts.put(node.id, node)
              }

            }
          }
        })

        val cleanupExecutor = new ScheduledThreadPoolExecutor(1)
        if (staleRequestCleanupFrequencyMins > 0) {
          cleanupExecutor.scheduleAtFixedRate(cleanupTask,
            staleRequestCleanupFrequencyMins,
            staleRequestCleanupFrequencyMins,
            TimeUnit.MINUTES)
        }

        log.info("Dark canaries configured for client: %s. Dark Canary configurations come from Zookeeper service name : %s".format(
          clientConfig.clientName,
          serviceName
        ))

        darkCanaryResponseHandler match {
          case Some(responseHandler) => {
            responseHandler.initialize()
          }
          case None =>
        }
      }
    }
  }

  class DownStreamHandler extends SimpleChannelDownstreamHandler {
    def createMirrorRequest(request: Request[Any, Any], mirroredNode: Node) : Request[Any, Any] = {
      val mirrorRequest = Request(request.message,
      mirroredNode,
      request.inputSerializer,
      request.outputSerializer,
      None,
      0)
      request.headers.foreach{kv_pair => mirrorRequest.addHeader(kv_pair._1, kv_pair._2)}
      mirrorRequest
    }

    override def writeRequested(ctx: ChannelHandlerContext, msg: MessageEvent): Unit = {
      try {
        if (!mirroredHosts.isEmpty) {
          msg.getMessage match {
            case request: Request[Any, Any] => {
              val mirroredNode = mirroredHosts.get(request.node.id)
              try {
                if (mirroredNode != null) {
                  if (request.node.url != mirroredNode.url) {
                    // This is a production request which we have to mirror.
                    if (canServeRequestStrategy.get.canServeRequest(mirroredNode)) {
                      try {
                        log.debug("mirroring message from : %s to %s".format(request.node.url, mirroredNode.url))
                        val newRequest = createMirrorRequest(request, mirroredNode)

                        darkCanaryResponseHandler match {
                          case Some(responseHandler) => {
                            hostRequestMap.put(request.id, request)
                            hostToMirror.put(request.id, newRequest.id)
                            mirrorToHost.put(newRequest.id, request.id)

                            responseHandler.downstreamCallback(request.id, request, newRequest)
                          }
                          case None =>
                        }

                        mirrorRequestMap.put(newRequest.id, newRequest)
                        clusterIoClient.get.sendMessage(newRequest.node, newRequest)
                      }
                    }
                  }
                }
              } catch {
                case e: Exception => {
                  log.error(e, "Exception while mirroring request to %s. Message: %s".format(mirroredNode.url,
                    e.getMessage))
                }
              }
            }
          }
        }
      } catch {
        case e: Exception => {
          log.error(e, "Exception while try to get mirrored host information from mirroredHosts map. " +
            "This error indicates that there might be concurrency issues in the usages of the mirroredHosts map." +
            "Need to resolve the concurrency issues.")
        }
      }
      super.writeRequested(ctx, msg) // will call ctx.sendDownstream(msg)
    }
  }

  class UpstreamHandler extends SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, msg: MessageEvent) {
      if (!mirrorRequestMap.isEmpty || !hostRequestMap.isEmpty) {
        msg.getMessage match {
          case message : NorbertProtos.NorbertMessage => {
            // Check if the request ID of the message corresponds to an existing mirrored request. If it does, then
            // just drop the message from the pipeline. This ensures that clients never see the message.
            val requestId = new UUID(message.getRequestIdMsb, message.getRequestIdLsb)
            mirrorRequestMap.get(requestId) match {
              case request: Request[Any,Any] =>  {
                mirrorRequestMap.remove(requestId)
                // Begin upstreamCallback
                darkCanaryResponseHandler match {
                  case Some(responseHandler) => {
                    mirrorToHost.remove(requestId) match {
                      case null => log.error("Could not find hostRequestId for darkCanaryRequestId: %s".format(requestId.toString))
                      case hostRequestId => {
                        responseHandler.upstreamCallback(true, hostRequestId, request, message)
                      }
                    }
                  }
                  case None =>
                }
                // End upstreamCallback
                if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
                  log.debug("Dropping successful response from %s".format(request.node.url))
                } else {
                  try {
                    // We did not get a successful response. Log an exception so that this is picked up by EKG.
                    throw new DarkCanaryException("Got bad status %s for mirrored request %s".format(
                      message.getStatus.toString,
                      request.toString()))
                  }
                  catch {
                    case e : Exception => {
                      log.error("DarkCanaryException : %s".format(e.getMessage))
                      log.error ("Stack trace : %s".format(e.getStackTraceString))
                    }
                  }
                }
                // This is a mirrored request. Don't propagate the response.
              }
              case _ => {
                // Begin upstreamCallback
                darkCanaryResponseHandler match {
                  case Some(responseHandler) => {
                    hostRequestMap.remove(requestId) match {
                      case request: Request[Any,Any] => {
                        hostToMirror.remove(requestId)
                        responseHandler.upstreamCallback(false, requestId, request, message)
                      }
                      case null => // Not a request from a host with a mirror
                    }
                  }
                  case None =>
                }
                // End upstreamCallback
                // This is not a mirrored request, Propagate the message upstream.
                super.messageReceived(ctx, msg)  // will call ctx.sendUpstream(msg)
              }
            }
          }
          // Propagate the message by default.
          case _ => super.messageReceived(ctx, msg)
        }
      } else {
        super.messageReceived(ctx, msg)
      }
    }
  }

  // Cleanup old requests from the request map. In case the dark canary host is very slow, we do not want the request
  // map to cause a memory leak by holding on to requests indefinitely.
  val cleanupTask = new Runnable() {
    val staleRequestTimeoutMillis = TimeUnit.MILLISECONDS.convert(staleRequestTimeoutMins, TimeUnit.MINUTES)
    override def run() {
      if (staleRequestTimeoutMins > 0) {
        try {
          import collection.JavaConversions._
          var expiredEntryCountHost = 0
          var expiredEntryCountDark = 0

          mirrorRequestMap.keySet.foreach { uuid =>
            val request = Option(mirrorRequestMap.get(uuid))
            val now = System.currentTimeMillis

            request.foreach { r =>
              if ((now - r.timestamp) > staleRequestTimeoutMillis) {
                mirrorRequestMap.remove(uuid)
                mirrorToHost.remove(uuid)
                expiredEntryCountDark += 1
              }
            }
          }

          hostRequestMap.keySet.foreach { uuid =>
            val request = Option(hostRequestMap.get(uuid))
            val now = System.currentTimeMillis

            request.foreach { r =>
              if ((now - r.timestamp) > staleRequestTimeoutMillis) {
                hostRequestMap.remove(uuid)
                hostToMirror.remove(uuid)
                expiredEntryCountHost += 1
              }
            }
          }

          if (expiredEntryCountDark > 0) {
            log.info("Expired %d stale dark canary requests".format(expiredEntryCountDark))
          }
          if (expiredEntryCountHost > 0) {
            log.info("Expired %d stale host canary requests".format(expiredEntryCountHost))
          }
        } catch {
          case e: InterruptedException =>
            Thread.currentThread.interrupt()
            log.error(e, "Interrupted exception in cleanup task")
          case e: Exception => log.error(e, "Exception caught in cleanup task, ignoring ")
        }
      }
    }
  }

  // These methods are currently only used to facilitate unit tests. There should be no calls to them from the rest of
  // code.
  def addNode(n : Node) : Unit = mirroredHosts.put(n.id, n)
  def removeNode(id: Int) : Unit = mirroredHosts.remove(id)
  def getInFlightRequestIds : Array[UUID] = mirrorRequestMap.keySet().toArray.map { e => e.asInstanceOf[UUID]}
}
