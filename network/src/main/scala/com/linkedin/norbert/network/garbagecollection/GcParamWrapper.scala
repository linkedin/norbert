package com.linkedin.norbert.network.garbagecollection

/**
 * Created by sishah on 7/6/15.
 *
 * A wrapper object over three GC parameters -
 *  - slaTime: The SLA request time.
 *  - cycleTime: The time period in which each node in the data center undergoes garbage collection exactly once
 *  - slotTime: The time allotted for the nodes in one cluster to finish pending requests and garbage collect
 *
 *  GC Awareness on the server side kicks in only if all three have non-zero values, and the node bound to the server
 *  has a valid offset.
 */
case class GcParamWrapper(slaTime: Int, cycleTime: Int, slotTime: Int) {

  def this() = this(-1, -1, -1)

  val enableGcAwareness: Boolean = slaTime > 0 && cycleTime > 0 && slotTime > 0

}

object GcParamWrapper {

  val DEFAULT = new GcParamWrapper()

}
