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

import norbertutils._
import collection.JavaConversions
import java.util.concurrent.atomic.AtomicInteger

// Threadsafe. Writers should always complete more or less instantly. Readers work via copy-on-write.
class FinishedRequestTimeTracker(clock: Clock, interval: Long) {
  private val q = new java.util.concurrent.ConcurrentLinkedQueue[(Long, Long)]()
  private val currentlyCleaning = new java.util.concurrent.atomic.AtomicBoolean

  private def clean {
    // Let only one thread clean at a time
    if(currentlyCleaning.compareAndSet(false, true)) {
      clean0
      currentlyCleaning.set(false)
    }
  }

  private def clean0 {
    while(!q.isEmpty) {
      val head = q.peek
      if(head == null)
        return

      val (completion, processingTime) = head
      if(clock.getCurrentTimeOffsetMicroseconds - completion > interval) {
        q.remove(head)
      } else {
        return
      }
    }
  }

  def addTime(processingTime: Long) {
    clean
    q.offer( (clock.getCurrentTimeOffsetMicroseconds, processingTime) )
  }

  def getArray: Array[(Long, Long)] = {
    clean
    q.toArray(Array.empty[(Long, Long)])
  }

  def getTimings: Array[Long] = {
    getArray.map(_._2).sorted
  }

  def total = {
    getTimings.sum
  }

  def reset {
    q.clear
  }
}

class TotalRequestProcessingTime[KeyT](clock:Clock, interval:Long) {
  private val q = new java.util.concurrent.ConcurrentLinkedQueue[(Long, Long)]()
  private val currentlyCleaning = new java.util.concurrent.atomic.AtomicBoolean

  private def clean {
    // Let only one thread clean at a time
    if(currentlyCleaning.compareAndSet(false, true)) {
      clean0
      currentlyCleaning.set(false)
    }
  }

  private def clean0 {
    while(!q.isEmpty) {
      val head = q.peek
      if(head == null)
        return

      val (completion, processingTime) = head
      if(clock.getCurrentTimeOffsetMicroseconds - completion > interval) {
        q.remove(head)
      } else {
        return
      }
    }
  }

  def addTime(processingTime: Long) {
    clean
    q.offer( (clock.getCurrentTimeOffsetMicroseconds, processingTime) )
  }

  def getArray: Array[(Long, Long)] = {
    clean
    q.toArray(Array.empty[(Long, Long)])
  }

  def getTimings: Array[Long] = {
    getArray.map(_._2).sorted
  }

  def total = {
    getTimings.sum
  }

  def reset {
    q.clear
  }
}

class QueueTimeTracker[KeyT](clock: Clock, interval: Long) {
    private val q = new java.util.concurrent.ConcurrentLinkedQueue[(Long, Long)]()
    private val currentlyCleaning = new java.util.concurrent.atomic.AtomicBoolean

    private def clean {
      // Let only one thread clean at a time
      if(currentlyCleaning.compareAndSet(false, true)) {
        clean0
        currentlyCleaning.set(false)
      }
    }

    private def clean0 {
      while(!q.isEmpty) {
        val head = q.peek
        if(head == null)
          return

        val (completion, processingTime) = head
        if(clock.getCurrentTimeOffsetMicroseconds - completion > interval) {
          q.remove(head)
        } else {
          return
        }
      }
    }

    def addTime(processingTime: Long) {
      clean
      q.offer( (clock.getCurrentTimeOffsetMicroseconds, processingTime) )
    }

    def getArray: Array[(Long, Long)] = {
      clean
      q.toArray(Array.empty[(Long, Long)])
    }

    def getTimings: Array[Long] = {
      getArray.map(_._2).sorted
    }

    def total = {
      getTimings.sum
    }

    def reset {
      q.clear
    }
}

// Threadsafe
class PendingRequestTimeTracker[KeyT](clock: Clock) {
  private val numRequests = new AtomicInteger()

  private val map : java.util.concurrent.ConcurrentMap[KeyT, Long] =
    new java.util.concurrent.ConcurrentHashMap[KeyT, Long]

  private val mapQueueTime : java.util.concurrent.ConcurrentMap[KeyT, Long] = 
    new java.util.concurrent.ConcurrentHashMap[KeyT, Long]

  def getStartTime(key: KeyT) = Option(map.get(key))

  //pre-condition for this method is the above method returns some 
  def getQueueTime(key: KeyT) = mapQueueTime.get(key)

  def beginRequest(key: KeyT, queueTime: Long) {
    numRequests.incrementAndGet
    val now = clock.getCurrentTimeOffsetMicroseconds
    map.put(key, now)
    mapQueueTime.put(key, queueTime)
  }

  def endRequest(key: KeyT) {
    map.remove(key)
    mapQueueTime.remove(key)
  }

  def getTimings = {
    val now = clock.getCurrentTimeOffsetMicroseconds
    val timings = map.values.toArray(Array.empty[java.lang.Long])
    timings.map(t => (now - t.longValue)).sorted
  }

  def reset {
    map.clear
  }

  def getTotalNumRequests = numRequests.get

  def total = getTimings.sum
}

class RequestTimeTracker[KeyT](clock: Clock, interval: Long) {
  val finishedRequestTimeTracker = new FinishedRequestTimeTracker(clock, interval)
  val finishedNettyTimeTracker = new FinishedRequestTimeTracker(clock, interval)
  val pendingNettyTimeTracker = new PendingRequestTimeTracker[KeyT](clock)
  val pendingRequestTimeTracker = new PendingRequestTimeTracker[KeyT](clock)
  val queueTimeTracker = new QueueTimeTracker[KeyT](clock, interval)//TODO
  val totalRequestProcessingTimeTracker = new TotalRequestProcessingTime[KeyT](clock, interval)

  def beginRequest(key: KeyT, queueTime: Long = 0) {
    pendingRequestTimeTracker.beginRequest(key, queueTime)
  }

  def beginNetty(key: KeyT, queueTime: Long) {
    pendingNettyTimeTracker.beginRequest(key, queueTime)
  }

  def endNetty(key: KeyT) {
    pendingNettyTimeTracker.getStartTime(key).foreach { startTime =>
      val queueTime = pendingNettyTimeTracker.getQueueTime(key)
      finishedNettyTimeTracker.addTime(queueTime + clock.getCurrentTimeOffsetMicroseconds - startTime)
    }
    pendingNettyTimeTracker.endRequest(key)
  }

  def endRequest(key: KeyT) {
    pendingRequestTimeTracker.getStartTime(key).foreach { startTime =>
      //over time we will retire this since this does not account for the amount of time the request
      //was stuck in the queue
      finishedRequestTimeTracker.addTime(clock.getCurrentTimeOffsetMicroseconds - startTime)
      val queueTime = pendingRequestTimeTracker.getQueueTime(key)
      queueTimeTracker.addTime(queueTime)
      totalRequestProcessingTimeTracker.addTime(queueTime + clock.getCurrentTimeOffsetMicroseconds - startTime)
    }
    pendingRequestTimeTracker.endRequest(key)
    endNetty(key)
  }

  def reset {
    finishedRequestTimeTracker.reset
    pendingNettyTimeTracker.reset
    pendingRequestTimeTracker.reset
    queueTimeTracker.reset
    totalRequestProcessingTimeTracker.reset
  }
}
