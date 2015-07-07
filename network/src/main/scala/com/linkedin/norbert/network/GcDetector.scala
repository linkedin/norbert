package com.linkedin.norbert.network

import com.linkedin.norbert.norbertutils.ClockComponent

/**
 * Created by sishah on 7/6/15.

 * Separate trait containing the function used to determine whether a node is GCing given its offset.
 * Used by load balancers on the client side, and by network servers to determine when to GC.
 *
 * cycleTime: The time period (in milliseconds) in which each node in the data center undergoes
 *            garbage collection exactly once
 * slotTime: The time (in milliseconds) for the nodes in one cluster to finish pending requests
 *           (SLA time) + the time to garbage collect (GC time).
 *
 *
 *    Offset 2 ------------------------------->
 *
 *    Offset 0 --->                                                                     <- Slot time ->
 *
 *                <-------------|-------------|-------------|-------------|-------------|------------->
 *
 *                <- Cycle Time ---------------------------------------------------------------------->
 *
 *
 * A node n is currently undergoing garbage collection if:
 *         [ [currentTime % cycleTime] / slotTime ]  == n.offset
 */
trait GcDetector {

  this: ClockComponent =>

  protected val gcCycleTime, gcSlotTime: Int

  def isCurrentlyDownToGC(nodeOffset: Int): Boolean = {
    val currentOffset = (clock.getCurrentTimeMilliseconds % gcCycleTime) / gcSlotTime
    currentOffset == nodeOffset
  }

  /**
   *
   * Calculates the time until the next GC should occur for this node
   */
  def timeTillNextGC(nodeOffset: Int): Long = {

    //How far along are we in the current cycle
    val timeOffsetWithinCycle: Long = clock.getCurrentTimeMilliseconds % gcCycleTime

    //How far along in the cycle should this node GC
    val nodeGcTimeOffsetWithinCycle: Long = nodeOffset * gcSlotTime

    if (nodeGcTimeOffsetWithinCycle > timeOffsetWithinCycle)
      nodeGcTimeOffsetWithinCycle - timeOffsetWithinCycle
    else
      (gcCycleTime - timeOffsetWithinCycle) + nodeGcTimeOffsetWithinCycle

  }

}
