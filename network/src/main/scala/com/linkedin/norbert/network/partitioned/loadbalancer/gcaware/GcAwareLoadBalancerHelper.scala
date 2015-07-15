package com.linkedin.norbert.network.partitioned.loadbalancer.gcaware

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.GcDetector
import com.linkedin.norbert.network.client.loadbalancer.LoadBalancerHelpers
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.norbertutils.ClockComponent

/**
 * Created by sishah on 7/14/15.
 */
trait GcAwareLoadBalancerHelper extends LoadBalancerHelpers with Logging with GcDetector {

  this:  ClockComponent =>

  private val DUMMY_OFFSET = -1

  // Truth table for second part of condition:
  // No offset - TRUE, this node was registered without an offset, send it traffic
  // Offset and is currently GCing - FALSE, don't send this node traffic
  // Offset and isn't currently GCing - TRUE, send this node traffic
  override def isEndpointViable(capability: Option[Long], persistentCapability: Option[Long], endpoint: Endpoint): Boolean = {
    super.isEndpointViable(capability, persistentCapability, endpoint) &&
            (endpoint.node.offset.isEmpty || !isCurrentlyDownToGC(endpoint.node.offset.get))
  }

  /**
   * Given the current set of node endpoints, this method generates a mapping from the time-to-GC offset to the node.
   *
   * @param nodes the current available nodes (endpoints)
   *
   * @return a <code>Map</code> of offset to the <code>Node</code>s which service that partition
   * logs an error if every node doesn't have an offset assigned to it
   */
  def generateOffsetToNodeMap(nodes: Set[Endpoint]): Map[Int, Set[Endpoint]] = {
    val offsetNodeTuples = List.empty[(Int, Endpoint)]

    nodes foreach { (n: Endpoint) => (n.node.offset.getOrElse({logBadNode(n.node); DUMMY_OFFSET})
            , n) :: offsetNodeTuples }

    offsetNodeTuples.foldLeft(Map.empty[Int, Set[Endpoint]]) {
      case (map, (offset, node)) => map + (offset -> (map.getOrElse(offset, Set.empty[Endpoint]) + node))
    }
  }

  private def logBadNode(node: Node) = {
    log.error("Node %d doesn't have a GC offset".format(node.id))
  }

  def validateOffsets(nodes: Set[Endpoint]): Unit = {
    nodes foreach {(n: Endpoint) => if (n.node.offset.isEmpty) logBadNode(n.node)}
  }

}
