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
package cluster
package zookeeper

import common.{ClusterNotificationManagerComponent, ClusterManagerComponent, ClusterManagerHelper}
import logging.Logging
import actors.Actor
import Actor._
import java.io.IOException
import org.apache.zookeeper._
import protos.NorbertProtos
import java.lang.NumberFormatException

trait ZooKeeperClusterManagerComponent extends ClusterManagerComponent {
  this: ClusterNotificationManagerComponent =>

  sealed trait ZooKeeperMessage
  object ZooKeeperMessages {
    case object Connected extends ZooKeeperMessage
    case object Disconnected extends ZooKeeperMessage
    case object Expired extends ZooKeeperMessage
    //synchronization is not needed between this method
    case class NodeChildrenChanged(path: String) extends ZooKeeperMessage
    case class NodeDataChanged(path: String) extends ZooKeeperMessage
    case class NodeDeleted(path: String) extends ZooKeeperMessage
    case class NodeCreated(path: String) extends ZooKeeperMessage
  }

  class ZooKeeperClusterManager(connectString: String, sessionTimeout: Int, serviceName: String)
          (implicit zooKeeperFactory: (String, Int, Watcher) => ZooKeeper) extends Actor with ClusterManagerHelper with Logging {
    private val SERVICE_NODE = "/" + serviceName
    private val AVAILABILITY_NODE = SERVICE_NODE + "/available"
    private val MEMBERSHIP_NODE = SERVICE_NODE + "/members"

    private var currentNodes = scala.collection.mutable.Map[Int, Node]()
    private var zooKeeper: Option[ZooKeeper] = None
    private var watcher: ClusterWatcher = _
    def getWatcher():ClusterWatcher = {watcher}
    private var connected = false

    def act() = {
      log.debug("Connecting to ZooKeeper...")
      startZooKeeper

      while (true) {
        import ZooKeeperMessages._
        import ClusterManagerMessages._

        receive {
          case Connected => handleConnected

          case Disconnected => handleDisconnected

          case Expired => handleExpired

          case NodeChildrenChanged(path) => if (path == AVAILABILITY_NODE) {
            handleAvailabilityChanged
          } else if (path == MEMBERSHIP_NODE) {
            handleMembershipChanged
          } else {
            log.error("Received a notification for a path that shouldn't be monitored: %s".format(path))
          }

          case AddNode(node) => handleAddNode(node)

          case RemoveNode(nodeId) => handleRemoveNode(nodeId)

          case MarkNodeAvailable(nodeId, initialCapability) => handleMarkNodeAvailable(nodeId, initialCapability)

          case MarkNodeUnavailable(nodeId) => handleMarkNodeUnavailable(nodeId)

          case SetNodeCapability(nodeId, capability) => handleSetNodeCapability(nodeId, capability)

          case Shutdown => handleShutdown

          case NodeDataChanged(path) => if (path.startsWith(MEMBERSHIP_NODE)) {
            handleCapabilityMemberChanged(path)
          }

          case NodeDeleted(path) => if (path.equals(MEMBERSHIP_NODE) || path.equals(AVAILABILITY_NODE)) {
            //zookeeper data corrupted
            log.fatal("Received a zookeeper corruption message")
          } else if (path.startsWith(MEMBERSHIP_NODE) || path.startsWith(AVAILABILITY_NODE)) {
            log.info("Received an event where a node was deleted:%s".format(path))
          } else {
            log.error("Node deleted unexpectedly %s".format(path))
          }

          case NodeCreated(path) => {
            log.error("Received an unexpected create event:%s".format(path))
          }

          case m => log.error("Received unknown message: %s".format(m))
        }
      }
    }

    private def handleConnected {
      log.info("Handling a Connected message")

      if (connected) {
        log.error("Received a Connected message when already connected")
      } else {
        doWithZooKeeper("a Connected message") { zk =>
          verifyZooKeeperStructure(zk)
          lookupCurrentNodes(zk)
          connected = true
          clusterNotificationManager ! ClusterNotificationMessages.Connected(currentNodes)
        }
      }
    }

    private def handleDisconnected {
      log.info("Handling a Disconnected message")

      doIfConnected("a Disconnected message") {
        connected = false
        currentNodes.clear
        clusterNotificationManager ! ClusterNotificationMessages.Disconnected
      }
    }

    private def handleExpired {
      log.info("Handling an Expired message")

      log.error("Connection to ZooKeeper expired, reconnecting...")
      connected = false
      currentNodes.clear
      watcher.shutdown
      startZooKeeper
    }

    private def handleAvailabilityChanged {
      log.info("Handling an availability changed event")

      doIfConnectedWithZooKeeper("an availability changed event") { zk =>
        import scala.collection.JavaConversions._

        val availableSet = zk.getChildren(AVAILABILITY_NODE, true).foldLeft(Set[Int]()) { (set, i) => set + i.toInt }
        if (availableSet.size == 0) {
          currentNodes.foreach { case (id, _) => makeNodeUnavailable(id) }
        } else {
          val (available, unavailable) = currentNodes.partition { case (id, _) => availableSet.contains(id) }
          available.foreach { case (id, _) =>
            val capability = try {
              val cbytes : Array[Byte] = zk.getData("%s/%d".format(AVAILABILITY_NODE, id), false, null)
              if (cbytes != null) BigInt(cbytes).longValue else 0L
            } catch {
              case ex: KeeperException if ex.code == KeeperException.Code.NONODE => 0L
            }
            makeNodeAvailable(id, capability)
          }
          unavailable.foreach { case (id, _) => makeNodeUnavailable(id) }
        }

        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleCapabilityMemberChanged(path: String) {
      log.debug("Handling a member capability changed event")

      doIfConnectedWithZooKeeper("a membership changed event") { zk =>
        if (lookupMemberNode(zk, path))
          clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleMembershipChanged {
      log.info("Handling a membership changed event")

      doIfConnectedWithZooKeeper("a membership changed event") { zk =>
        lookupCurrentNodes(zk)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleAddNode(node: Node) {
      log.info("Handling an AddNode(%s) message".format(node))

      doIfConnectedWithZooKeeperWithResponse("an AddNode message", "adding node") { zk =>
        val path = "%s/%d".format(MEMBERSHIP_NODE, node.id)

        if (zk.exists(path, false) != null) {
          Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))
        } else {
          try {
            zk.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

            currentNodes += (node.id -> node)
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)

            None
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))
          }
        }
      }
    }

    private def handleRemoveNode(nodeId: Int) {
      log.debug("Handling a RemoveNode(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a RemoveNode message", "deleting node") { zk =>
        val path = "%s/%d".format(MEMBERSHIP_NODE, nodeId)

        if (zk.exists(path, false) != null) {
          try {
            zk.delete(path, -1)
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NONODE => // do nothing
          }
        }

        currentNodes -= nodeId
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleMarkNodeAvailable(nodeId: Int, initialCapability: Long) {
      log.debug("Handling a MarkNodeAvailable(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeAvailable message", "marking node available") { zk =>
        val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)

        if (zk.exists(path, false) == null) {
          try {
            zk.create(path, BigInt(initialCapability).toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => // do nothing
          }
        }

        makeNodeAvailable(nodeId, initialCapability)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleMarkNodeUnavailable(nodeId: Int) {
      log.debug("Handling a MarkNodeUnavailable(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeUnavailable message", "marking node unavailable") { zk =>
        val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)

        if (zk.exists(path, false) != null) {
          try {
            zk.delete(path, -1)
            None
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NONODE => // do nothing
          }
        }

        makeNodeUnavailable(nodeId)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleSetNodeCapability(nodeId: Int, capability: Long) {
      log.debug("Handling a SetNodeCapability (%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a SetNodeCapability message", "setting node capability "+ capability) {  zk =>
        val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)
        if (zk.exists(path, false) !=null) {
          try {
            zk.setData(path, BigInt(capability).toByteArray, -1)
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NONODE => // do nothing
          }
        }

        setNodeCapability(nodeId, capability)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleShutdown {
      log.debug("Handling a Shutdown message")

      try {
        watcher.shutdown
        zooKeeper.foreach(_.close)
      } catch {
        case ex: Exception => log.error(ex, "Exception when closing connection to ZooKeeper")
      }

      log.debug("ZooKeeperClusterManager shut down")
      exit
    }

    private def startZooKeeper {
      zooKeeper = try {
        watcher = new ClusterWatcher(self)
        val zk = Some(zooKeeperFactory(connectString, sessionTimeout, watcher))
        log.info("Connected to ZooKeeper")
        zk
      } catch {
        case ex: IOException =>
          log.error(ex, "Unable to connect to ZooKeeper")
          None

        case ex: Exception =>
          log.error(ex, "Exception while connecting to ZooKeeper")
          None
      }
    }

    private def verifyZooKeeperStructure(zk: ZooKeeper) {
      log.debug("Verifying ZooKeeper structure...")

      List(SERVICE_NODE, AVAILABILITY_NODE, MEMBERSHIP_NODE).foreach { path =>
        try {
          log.debug("Ensuring %s exists".format(path))
          if (zk.exists(path, false) == null) {
            log.debug("%s doesn't exist, creating".format(path))
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => // do nothing
        }
      }
    }

    /* This function does two things
      if the node is still a member that we are tracking we will modify its state in our store and return true
      else return false since we are not changing anything in our metadata
     */
    private def lookupMemberNode(zk: ZooKeeper, path: String):Boolean = {
      //parse out the member id
      var memberId = -1
      try {
        memberId = Integer.parseInt(path.substring(MEMBERSHIP_NODE.length+1).trim)
      } catch {
        case ex: NumberFormatException =>
          log.error(ex, "Encountered an exception while parsing the path from getData event")
          return false
      }
      val oldMember: Node = currentNodes.getOrElse(memberId, null)
      //make sure this node did not get its membership changed
      if (oldMember != null) {
        val membersBytes = zk.getData("%s/%s".format(MEMBERSHIP_NODE, memberId),watcher,null)
        currentNodes += memberId -> Node(memberId, membersBytes, oldMember.available, oldMember.capability)
        return true
      }
      return false
      //else leave the currentNodes unchanged
    }

    private def lookupCurrentNodes(zk: ZooKeeper) {
      import scala.collection.JavaConversions._

      val members = zk.getChildren(MEMBERSHIP_NODE, true)
      val available = zk.getChildren(AVAILABILITY_NODE, true)

      // currentNodes is not changed if some unexpected exception happens
      currentNodes = members.foldLeft(scala.collection.mutable.Map[Int, Node]()) {(nodes, member) =>
        val id = member.toInt
        val capabilityOption = if (available.contains(member)) {
          try {
            val cbytes : Array[Byte] = zk.getData("%s/%d".format(AVAILABILITY_NODE, id), false, null)
            if (cbytes != null) Some(BigInt(cbytes).longValue) else Some(0L)
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NONODE => Some(0L)
          }
        } else None

        try {
          nodes += (id -> Node(id, zk.getData("%s/%s".format(MEMBERSHIP_NODE, member), watcher, null), available.contains(member), capabilityOption))
        } catch {
          // it's possible that the node is deleted after the getChildren call
          case ex: KeeperException if ex.code == KeeperException.Code.NONODE => nodes
        }
      }
    }

    private def makeNodeAvailable(nodeId: Int, capability: Long) {
      currentNodes.get(nodeId).foreach { n => if (!n.available) currentNodes.update(n.id, n.copy(available = true, capability = Some(capability))) }
    }

    private def makeNodeUnavailable(nodeId: Int) {
      currentNodes.get(nodeId).foreach { n => if (n.available) currentNodes.update(n.id, n.copy(available = false, capability = None)) }
    }

    private def setNodeCapability(nodeId: Int, capability: Long) {
      currentNodes.get(nodeId).filter( _.available).map { n => currentNodes.update(n.id, n.copy(capability = Some(capability))) }
    }

    private def doIfConnected(what: String)(block: => Unit) {
      if (connected) {
        block
      } else {
        log.error("Received %s when not connected. Trying to teardown the state using handleExpired and start over".format(what))
        handleExpired
      }
    }

    private def doWithZooKeeper(what: String)(block: ZooKeeper => Unit) {
      zooKeeper match {
        case Some(zk) =>
          try {
            block(zk)
          } catch {
            case ex: KeeperException => log.error(ex, "ZooKeeper threw an exception")
            case ex: Exception => log.error(ex, "Unhandled exception while working with ZooKeeper")
          }

        case None => log.fatal(new Exception,
          "Received %s when ZooKeeper is None, this should never happen. Please report a bug including the stack trace provided.".format(what))
      }
    }

    private def doIfConnectedWithZooKeeper(what: String)(block: ZooKeeper => Unit) {
      doIfConnected(what) {
        doWithZooKeeper(what)(block)
      }
    }

    private def doIfConnectedWithZooKeeperWithResponse(what: String, exceptionDescription: String)(block: ZooKeeper => Option[ClusterException]) {
      import ClusterManagerMessages.ClusterManagerResponse

      if (connected) {
        doWithZooKeeper(what) { zk =>
          val response = try {
            block(zk)
          } catch {
            case ex: KeeperException => Some(new ClusterException("Error while %s".format(exceptionDescription), ex))
            case ex: Exception => Some(new ClusterException("Unexpected exception while %s".format(exceptionDescription), ex))
          }

          reply(ClusterManagerResponse(response))
        }
      } else {
        reply(ClusterManagerResponse(Some(new ClusterDisconnectedException("Error while %s, cluster is disconnected".format(exceptionDescription)))))
      }
    }
  }

  protected implicit def defaultZooKeeperFactory(connectString: String, sessionTimeout: Int, watcher: Watcher) = new ZooKeeper(connectString, sessionTimeout, watcher)

  class ClusterWatcher(zooKeeperManager: Actor) extends Watcher {
    @volatile private var shutdownSwitch = false

    def process(event: WatchedEvent) {
      import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
      import ZooKeeperMessages._

      if (shutdownSwitch) return

      event.getType match {
        case EventType.None =>
          event.getState match {
            case KeeperState.SyncConnected => zooKeeperManager ! Connected
            case KeeperState.Disconnected => zooKeeperManager ! Disconnected
            case KeeperState.Expired => zooKeeperManager ! Expired
          }

        case EventType.NodeChildrenChanged => zooKeeperManager ! NodeChildrenChanged(event.getPath)

        case EventType.NodeDataChanged => zooKeeperManager ! NodeDataChanged(event.getPath)

        case EventType.NodeCreated => zooKeeperManager ! NodeCreated(event.getPath)

        case EventType.NodeDeleted => zooKeeperManager ! NodeDeleted(event.getPath)
      }
    }

    def shutdown: Unit = shutdownSwitch = true
  }
}
