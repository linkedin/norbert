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
package common

import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.norbertutils.MockClock

class CachedNetworkStatisticsSpec extends SpecificationWithJUnit {
  val cachedNetworkStatistics = CachedNetworkStatistics[Int,Int](new MockClock, 100, 100)

  "CachedNetworkStatistics" should {
    "clear out maps properly" in {
      cachedNetworkStatistics.beginRequest(1,1,0)
      cachedNetworkStatistics.beginNetty(1,1,0)
      cachedNetworkStatistics.beginRequest(1,2,0)
      cachedNetworkStatistics.beginNetty(1,2,0)
      cachedNetworkStatistics.pendingTimings.get.get.get(1).get.length must be_==(2)
      cachedNetworkStatistics.endRequest(1,1)
      cachedNetworkStatistics.endRequest(1,2)
      cachedNetworkStatistics.pendingTimings.get.get.get(1).get.length must be_==(0)
    }
  }



}
