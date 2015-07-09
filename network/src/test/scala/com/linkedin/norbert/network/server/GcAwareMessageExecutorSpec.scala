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

/**
 * @author: sishah
 * @date: 07/07/15
 * @version: 1.0
 */
package com.linkedin.norbert
package network
package server

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.netty.GcParamWrapper

class GcAwareMessageExecutorSpec extends MessageExecutorSpec {

  val cycleTime = 6000
  val slotTime = 2000
  val slaTime = 1000

  val goodGcParams = new GcParamWrapper(slaTime, cycleTime, slotTime)

  val badNode = Node(1, "localhost:31313", true)
  def noNode:Node = {throw new NetworkServerNotBoundException}
  val goodNode = Node(1, "localhost:31313", true, Set.empty, None, None, Some(0))

  var nodeFlag = "badNode"

  def getNode = {

    Some (
      nodeFlag match {
        case "badNode" => badNode
        case "noNode" => noNode
        case "goodNode" => goodNode
      }
    )

  }

  // MessageExecutorSpec by default runs all tests with no GcParams and no defined node.
  // This spec overrides the message executor to have valid GcParams, and a node based on a flag.
  override val messageExecutor = new ThreadPoolMessageExecutor(None, "service",
    messageHandlerRegistry,
    filters,
    1000L,
    1,
    1,
    1,
    100,
    1000L,
    -1,
    goodGcParams,
    getNode
  )

  "GcAwareMessageExecutor" should {

    doAfter {
      messageExecutor.shutdown
    }

    //No node is bound
    "successfully respond (with no bound node) in" in {
      nodeFlag = "noNode"

      generalExecutorTests
    }

    //A node is connected, but it receives the request in its GC period
    "throw a GC Exception (with a GC-ing bound node) in" in {

      nodeFlag = "goodNode"

      while(System.currentTimeMillis()%cycleTime != 0){}

      messageHandlerRegistry.handlerFor(request) returns returnHandler _

      messageExecutor.executeMessage(request, Some((either: Either[Exception, Ping]) => null:Unit), None) must throwA[GcException]

      waitFor(50.ms)

      there was no(messageHandlerRegistry).handlerFor(request)
    }

    //These tests occur outside the GC period
    "successfully respond (with a not-currently-GCing node) in" in {

      nodeFlag = "goodNode"

      while(System.currentTimeMillis()%cycleTime != 0){}
      waitFor((slotTime + 10).ms)

      generalExecutorTests
    }

  }

}
