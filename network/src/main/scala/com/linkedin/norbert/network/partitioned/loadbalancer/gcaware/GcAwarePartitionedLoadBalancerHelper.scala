/*
 * Copyright 2009-2015 LinkedIn, Inc
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
package partitioned
package loadbalancer
package gcaware

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.norbertutils.ClockComponent

/**
 * A mixin trait that provides functionality to help implement a hash based, GC AND partition aware <code>Router</code>.
 */
trait GcAwarePartitionedLoadBalancerHelper extends DefaultPartitionedLoadBalancerHelper with GcAwareLoadBalancerHelper {

  this: ClockComponent =>

  // This needs to be overridden here, as the default implementation just returns the next node in round-robin order.
  // This returns the next node in round robin order that IS NOT GCing
  override def nodeToReturnWhenNothingViableFound(endpoints: IndexedSeq[Endpoint], idx: Int): Some[Node] = {
    val es = endpoints.size
    var i = idx
    for(j <- 1 to es ) {
      if (endpoints(i % es).node.offset.isEmpty || !isCurrentlyDownToGC(endpoints(i % es).node.offset.get))
        return Some(endpoints(i % es).node)

      i = i + 1
    }

    Some(endpoints(idx % es).node)
  }

}
