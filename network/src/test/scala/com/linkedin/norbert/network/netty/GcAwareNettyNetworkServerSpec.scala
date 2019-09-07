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
package netty

import java.util.concurrent._

import com.linkedin.norbert.cluster._
import com.linkedin.norbert.network.common.SampleMessage
import com.linkedin.norbert.network.garbagecollection.GcParamWrapper
import com.linkedin.norbert.util.WaitFor
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

/**
  * @author: sishah
  * @date: 07/08/15
  * @version: 1.0
  */
class GcAwareNettyNetworkServerSpec extends SpecificationWithJUnit with Mockito with SampleMessage with WaitFor {

  trait NetworkServerSetup extends Scope {
    // After registering a node, I expect the first GC event in the recurring schedule to occur in some time 't'.
    // Since I won't always get exactly 't' milliseconds, this allows for slack in the expected and observed delay
    // before the first event
    val slackTimeInMillis: Int = 20

    val cycleTime = 6000
    val slotTime = 2000
    val slaTime = 1000

    val goodGcParams = new GcParamWrapper(slaTime, cycleTime, slotTime)

    val networkConfig = spy(new NetworkServerConfig)
    networkConfig.clusterClient = mock[ClusterClient]
    networkConfig.clusterClient.clientName returns Some("Test")
    networkConfig.clusterClient.serviceName returns "Test"
    networkConfig.gcParams = goodGcParams
    networkConfig.requestTimeoutMillis = 2000L
    networkConfig.requestThreadCorePoolSize = 1
    networkConfig.requestThreadMaxPoolSize = 1
    networkConfig.requestThreadKeepAliveTimeSecs = 2

    val networkServer = spy(new NettyNetworkServer(networkConfig))

    val node0 = Node(0, "", false, Set.empty, None, None, Some(0))
    val node1 = Node(1, "", false, Set.empty, None, None, Some(1))
    val node2 = Node(2, "", false, Set.empty, None, None, Some(0))

    val listenerKey: ClusterListenerKey = ClusterListenerKey(1)

    networkServer.clusterClient.nodeWithId(1) returns Some(node0)
    networkServer.clusterClient.addListener(any[ClusterListener]) returns listenerKey


    def waitTillStartOfNewCycle: Unit = {
      println("Waiting till start of new cycle")
      while (System.currentTimeMillis() % cycleTime != 0) {}
    }

    def verifyDelay(obsDelay: Long, expDelay: Long): Boolean = {
      expDelay - obsDelay <= slackTimeInMillis
    }
  }

  trait AfterSpecSetup extends NetworkServerSetup {
    def after = {
      networkServer.shutdown
    }
  }

  "NetworkServer" should {
    "have a valid GC Thread " in new NetworkServerSetup {

      networkServer.gcThread must be_!=(None)

    }

    "schedule a new recurring GC event" in new NetworkServerSetup {

      waitTillStartOfNewCycle

      val timeTillNextGC = networkServer.timeTillNextGC(node0.offset.get)
      verifyDelay(timeTillNextGC, cycleTime) must beTrue

      networkServer.schedulePeriodicGc(node0)
      networkServer.gcFuture must eventually(be_!=(None))
      networkServer.currOffset must be_==(0)

    }

    "adapt the GC event to the binding of a new node" in new NetworkServerSetup {

      networkServer.schedulePeriodicGc(node0)

      waitTillStartOfNewCycle

      networkServer.schedulePeriodicGc(node1)

      val timeTillNextGC = networkServer.timeTillNextGC(node1.offset.get)

      verifyDelay(timeTillNextGC, slotTime) must beTrue
      networkServer.gcFuture must eventually(be_!=(None))
      networkServer.currOffset must be_==(1)

    }

    "Not cancel the initial GC event if the offset of the new node is the same" in new AfterSpecSetup {

      networkServer.schedulePeriodicGc(node0)

      waitFor(100.ms)

      networkServer.gcFuture = Some(mock[ScheduledFuture[_]])
      networkServer.schedulePeriodicGc(node2)

      networkServer.gcFuture must eventually(be_!=(None))
      networkServer.currOffset must be_==(0)
      there was no(networkServer.gcFuture.get).cancel(true)
    }
  }
}