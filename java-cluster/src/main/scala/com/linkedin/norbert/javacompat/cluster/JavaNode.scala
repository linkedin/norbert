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
package com.linkedin.norbert.javacompat
package cluster

import reflect.BeanProperty

object JavaNode {
  def apply(node: com.linkedin.norbert.cluster.Node): Node = {
    if (node == null) {
      null
    } else {
      var s = new java.util.HashSet[java.lang.Integer]
      if (node.partitionIds != null) {
        node.partitionIds.foreach {id => s.add(id)}
      }
      JavaNode(node.id, node.url, node.available, s, node.capability, node.persistentCapability)
    }
  }
}

case class JavaNode(@BeanProperty id: Int, @BeanProperty url: String, @BeanProperty available: Boolean, @BeanProperty partitionIds: java.util.Set[java.lang.Integer], capability: Option[Long] = None, persistentCapability: Option[Long] = None) extends Node {
  def isAvailable = available
  def isCapableOf(c: java.lang.Long) : Boolean = isCapableOf(c, 0L)
  def isCapableOf(c: java.lang.Long, pc: java.lang.Long) : Boolean =
    (capability, persistentCapability) match {
      case (Some(nc), Some(npc)) => ((nc & c.longValue) == c.longValue) && ((npc & pc.longValue) == pc.longValue)
      case (Some(nc), None) => (nc & c.longValue()) == c.longValue()
      case (nc, Some(pc)) => (pc & pc.longValue()) == pc.longValue()
      case (None, None) => c.longValue == 0L && pc.longValue() == 0L
    }
  def getCapability() : java.lang.Long = 
    capability match {
      case Some(nc) => nc.longValue
      case None => null
    }
  def getPersistentCapability() : java.lang.Long = 
    persistentCapability match {
      case Some(nc) => nc.longValue
      case None => null
    }
}
