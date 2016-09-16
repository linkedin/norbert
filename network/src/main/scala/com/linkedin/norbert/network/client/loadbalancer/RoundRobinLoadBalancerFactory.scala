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
package client
package loadbalancer

import common.Endpoint
import java.util.concurrent.atomic.AtomicInteger

class RoundRobinLoadBalancerFactory extends LoadBalancerFactory with LoadBalancerHelpers {
  def newLoadBalancer(endpointSet: Set[Endpoint]): LoadBalancer = new LoadBalancer {

    val counter = new AtomicInteger(0)
    val endpoints = endpointSet.toArray

    def nextNode(capability: Option[Long] = None, permanentCapability: Option[Long] = None) = {
      val activeEndpoints = endpoints.filter{ isEndpointViable(capability, permanentCapability, _) }

      if (activeEndpoints.isEmpty) {
        val capableEndpoints = endpoints.filter(_.node.isCapableOf(capability, permanentCapability))
        if (capableEndpoints.isEmpty)
          None
        else
          Some(chooseNext(capableEndpoints, counter).node)
      } else if (endpoints.isEmpty)
        None
      else
        Some(chooseNext(activeEndpoints, counter).node)
    }

  }

}
