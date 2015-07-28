/**
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

package com.linkedin.norbert.norbertutils

trait ClockComponent {
  val clock: Clock
}

trait Clock {
  def getCurrentTimeMilliseconds: Long
  //do not use this for absolute time
  //only for computing intervals
  def getCurrentTimeOffsetMicroseconds: Long
}

class MockClock extends Clock {
  var currentTime = 0L
  override def getCurrentTimeMilliseconds = currentTime
  override def getCurrentTimeOffsetMicroseconds = currentTime
}

object SystemClock extends Clock {
  def getCurrentTimeOffsetMicroseconds = System.nanoTime/1000
  def getCurrentTimeMilliseconds = System.currentTimeMillis
}

trait SystemClockComponent extends ClockComponent {
  val clock = SystemClock
}
