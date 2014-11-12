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
package jmx

import org.specs.SpecificationWithJUnit
import norbertutils.{SystemClock, MockClock, Clock, ClockComponent}

class RequestTimeTrackerSpec extends SpecificationWithJUnit {
  val mockClock = new MockClock

  "RequestTimeTracker" should {
    "cleanup request maps properly" in {
      val a = new RequestTimeTracker[Int](mockClock, 100)
      var t = 0
      (1 to 100).foreach{ k =>
        a.beginRequest(k,t)
        mockClock.currentTime = t
        t = t + 1
        a.beginNetty(k,t)
        mockClock.currentTime = t
        t = t + 1
      }
      (1 to 100).foreach{ k =>
        a.endNetty(k)
        a.endRequest(k)
      }
      a.pendingNettyTimeTracker.total must be_==(0)
      a.pendingRequestTimeTracker.total must be_==(0)
    }

    "use reset properly" in {
      val a = new RequestTimeTracker[Int](mockClock, 100)
      var t = 0
      (1 to 100).foreach{ k =>
        a.beginRequest(k,t)
        mockClock.currentTime = t
        t = t + 1
        a.beginNetty(k,t)
        mockClock.currentTime = t
        t = t + 1
        a.endNetty(k)
        a.endRequest(k)
      }
      a.reset
      a.pendingNettyTimeTracker.total must be_==(0)
      a.pendingRequestTimeTracker.total must be_==(0)
    }

  }
}
