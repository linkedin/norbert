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
package server


import com.linkedin.norbert.network.garbagecollection.{GcParamWrapper, GcDetector}
import com.linkedin.norbert.logging.Logging
import jmx.JMX.MBean
import jmx.{FinishedRequestTimeTracker, JMX}
import java.util.concurrent.atomic.AtomicInteger
import norbertutils.{SystemClock, NamedPoolThreadFactory}
import java.util.concurrent._
import scala.collection.mutable.MutableList
import common.CachedNetworkStatistics
import norbertutils._
import cluster.{Node, ClusterDisconnectedException, ClusterClientComponent}
import network.netty.TimingKeys

/**
 * A component which submits incoming messages to their associated message handler.
 */
trait MessageExecutorComponent {
  val messageExecutor: MessageExecutor
}

trait MessageExecutor {
  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: Option[(Either[Exception, ResponseMsg]) => Unit])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg]) : Unit = executeMessage(request, responseHandler, None)
  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: Option[(Either[Exception, ResponseMsg]) => Unit], context: Option[RequestContext])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg]): Unit
  @volatile val filters : MutableList[Filter]
  def addFilters(filters: List[Filter]) : Unit = this.filters ++= (filters)
  def shutdown: Unit
}

class ThreadPoolMessageExecutor(clientName: Option[String],
                                serviceName: String,
                                messageHandlerRegistry: MessageHandlerRegistry,
                                val filters: MutableList[Filter],
                                var requestTimeout: Long,
                                corePoolSize: Int,
                                maxPoolSize: Int,
                                keepAliveTime: Int,
                                maxWaitingQueueSize: Int,
                                requestStatisticsWindow: Long,
                                responseGenerationTimeoutMillis: Long,
                                gcParams: GcParamWrapper,
                                myNode: => Option[Node] ) extends MessageExecutor with Logging with GcDetector with SystemClockComponent {

  def this(clientName: Option[String],
           serviceName: String,
           messageHandlerRegistry: MessageHandlerRegistry,
           filters: MutableList[Filter],
           requestTimeout: Long,
           corePoolSize: Int,
           maxPoolSize: Int,
           keepAliveTime: Int,
           maxWaitingQueueSize: Int,
           requestStatisticsWindow: Long,
           responseGenerationTimeoutMillis: Long) =
    this(clientName, serviceName, messageHandlerRegistry, filters, requestTimeout, corePoolSize, maxPoolSize, keepAliveTime, maxWaitingQueueSize, requestStatisticsWindow, responseGenerationTimeoutMillis, GcParamWrapper.DEFAULT, None)


  def this(clientName: Option[String],
           serviceName: String,
           messageHandlerRegistry: MessageHandlerRegistry,
           requestTimeout: Long,
           corePoolSize: Int,
           maxPoolSize: Int,
           keepAliveTime: Int,
           maxWaitingQueueSize: Int,
           requestStatisticsWindow: Long,
           responseGenerationTimeoutMillis: Long) =
    this(clientName, serviceName, messageHandlerRegistry, new MutableList[Filter], requestTimeout, corePoolSize, maxPoolSize, keepAliveTime, maxWaitingQueueSize, requestStatisticsWindow, responseGenerationTimeoutMillis, GcParamWrapper.DEFAULT, None)

  def this(clientName: Option[String],
           serviceName: String,
           messageHandlerRegistry: MessageHandlerRegistry,
           requestTimeout: Long,
           corePoolSize: Int,
           maxPoolSize: Int,
           keepAliveTime: Int,
           maxWaitingQueueSize: Int,
           requestStatisticsWindow: Long,
           responseGenerationTimeoutMillis: Long,
           gcParams: GcParamWrapper,
           myNode: => Option[Node]) =
    this(clientName, serviceName, messageHandlerRegistry, new MutableList[Filter], requestTimeout, corePoolSize, maxPoolSize, keepAliveTime, maxWaitingQueueSize, requestStatisticsWindow, responseGenerationTimeoutMillis, gcParams, myNode)


  private val statsActor = CachedNetworkStatistics[Int, Int](SystemClock, requestStatisticsWindow, 200L)
  private val totalNumRejected = new AtomicInteger
  private val totalRequestsInGcSlot = new AtomicInteger
  // In milliseconds
  private val timeBufferForAcceptableRequests = 50

  val gcCycleTime = gcParams.cycleTime
  val gcSlotTime = gcParams.slotTime

  //Note that myNode is call by name and will be lazily evaluated.
  //This is necessary as there may be no node bound to the server
  def enableGcAwareness: Boolean = {
    try {
      //Do this check first so that non GC-aware scenario isn't affected
      gcParams.enableGcAwareness && {
        //Evaluate myNode now - checks for bound node.
        //Is this check too expensive? AtomicBoolean.get on every request, if the gcParams are okay.
        //(Look at how myNode is evaluated in NetworkServer.scala)
        val node = myNode
        node.isDefined && node.get.offset.isDefined
      }
    }
    catch {
      case e:NetworkServerNotBoundException => false
      case e:NetworkShutdownException => false
    }
  }

  val requestQueue = new ArrayBlockingQueue[Runnable](maxWaitingQueueSize)

  val statsJmx = JMX.register(new RequestProcessorMBeanImpl(clientName, serviceName, statsActor, requestQueue, threadPool))

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, requestQueue,
    new NamedPoolThreadFactory("norbert-message-executor")) {

    override def beforeExecute(t: Thread, r: Runnable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]
      statsActor.beginRequest(0, rr.id, (System.currentTimeMillis() - rr.queuedAt) * 1000)
    }

    override def afterExecute(r: Runnable, t: Throwable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]
      statsActor.endRequest(0, rr.id)
    }
  }

  def setRequestTimeout(newValue : Long) = {
    requestTimeout = newValue
    log.info("Setting timeout to " + newValue)
  }

  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler:  Option[(Either[Exception, ResponseMsg]) => Unit], context: Option[RequestContext] = None)
                                             (implicit is: InputSerializer[RequestMsg, ResponseMsg]) {
    val rr = new RequestRunner(request, requestTimeout, context, filters, responseHandler, is = is)

    // Log messages that arrive post the GC start period.
    // The check for ~50ms is to filter out the corner case messages that come right at the slot transition time.
    if (enableGcAwareness && isCurrentlyDownToGC(myNode.get.offset.get) && wasDownToGcPreviously(myNode.get.offset.get, timeBufferForAcceptableRequests)) {
      totalRequestsInGcSlot.incrementAndGet()
      log.warn("Received a request in the node's GC slot, even though the node's slot started at least 50ms ago")
    }

    try {
      threadPool.execute(rr)
    } catch {
      case ex: RejectedExecutionException =>
        statsActor.endRequest(0, rr.id)

        totalNumRejected.incrementAndGet
        log.warn("Request processing queue full. Size is currently " + requestQueue.size)
        throw new HeavyLoadException
    }

  }

  def shutdown {
    threadPool.shutdown
    statsJmx.foreach { JMX.unregister(_) }
    log.debug("MessageExecutor shut down")
  }

  private val idGenerator = new AtomicInteger(0)

  private class RequestRunner[RequestMsg, ResponseMsg](request: RequestMsg,
                                                       val reqTimeout : Long,
                                                       context: Option[RequestContext],
                                                       filters: MutableList[Filter],
                                                       callback: Option[(Either[Exception, ResponseMsg]) => Unit],
                                                       val queuedAt: Long = System.currentTimeMillis,
                                                       val id: Int = idGenerator.getAndIncrement.abs,
                                                       implicit val is: InputSerializer[RequestMsg, ResponseMsg]) extends Runnable {
    def run = {
      val now = System.currentTimeMillis
      if (now - queuedAt > reqTimeout) {
        totalNumRejected.incrementAndGet
        log.warn("Request timed out, ignoring! Currently = " + now + ". Queued at = " + queuedAt + ". Timeout = " + requestTimeout)
        callback.foreach(_(Left(new HeavyLoadException)))
      } else {
        log.debug("Executing message: %s".format(request))

        val response: Option[Either[Exception, ResponseMsg]] =
        try {
          context match {
            case Some(null) => ()
            case Some(m) => if (m.attributes != null) {
              m.attributes += (TimingKeys.ON_REQUEST_TIME_ATTR -> System.currentTimeMillis)
            }
            case None => ()
          }
          filters.foreach(filter => continueOnError(filter.onRequest(request, context.getOrElse(null))))
          val handler = messageHandlerRegistry.handlerFor(request)
          try {
            val response = handler(request)
            val timeResponse = System.currentTimeMillis - queuedAt
            if (responseGenerationTimeoutMillis>0 && timeResponse > responseGenerationTimeoutMillis) {
              totalNumRejected.incrementAndGet
              log.warn("Request timed out by the time we generated response, ignoring! Currently = " + now + ". " +
                "Queued at = " + queuedAt + ". Timeout = " + requestTimeout)
              throw new Exception("Response took too long:%d".format(timeResponse))
            }
            response match {
              case _:Unit => None
              case null => None
              case _ => Some(Right(response))
            }
          } catch {
            case ex: Exception =>
              log.error(ex, "Message handler threw an exception while processing message")
              Some(Left(ex))
          }
        } catch {
          case ex: InvalidMessageException =>
            log.error(ex, "Received an invalid message: %s".format(request))
            Some(Left(ex))

          case ex: Exception =>
            log.error(ex, "Unexpected error while handling message: %s".format(request))
            Some(Left(ex))
        }
        context match {
          case Some(null) => ()
          case Some(m) => if (m.attributes != null) {
            m.attributes += (TimingKeys.ON_RESPONSE_TIME_ATTR -> System.currentTimeMillis)
          }
          case None => ()
        }
        response.foreach { (res) =>
          if (!callback.isEmpty) callback.get(res)
          res match {
            case Left(ex) => filters.reverse.foreach(filter => continueOnError(filter.onError(ex, context.getOrElse(null))))
            case Right(responseMsg) =>  filters.reverse.foreach(filter => continueOnError(filter.onResponse(responseMsg, context.getOrElse(null))))
          }
        }
      }
    }
  }


  def getThreadPoolStats: String = {

    "CurrentThreadPoolCount: " + threadPool.getPoolSize +
    "\nLargestPoolSize: " + threadPool.getLargestPoolSize +
    "\nActiveThreadCount: " + threadPool.getActiveCount +
    "\nQueueSize: " + requestQueue.size()

  }

  trait RequestProcessorMBean {
    def getQueueSize: Int

    def getTotalNumRejected: Int

    def getMedianTime: Double

    def getCurrentPoolSize: Int

    def getActivePoolSize: Int

    def getTotalRequestsInGcSlot: Int
  }

  class RequestProcessorMBeanImpl(clientName: Option[String], serviceName: String, val stats: CachedNetworkStatistics[Int, Int], queue: ArrayBlockingQueue[Runnable], threadPool: ThreadPoolExecutor)
    extends MBean(classOf[RequestProcessorMBean], JMX.name(clientName, serviceName)) with RequestProcessorMBean {
    def getQueueSize = queue.size

    def getTotalNumRejected = totalNumRejected.get.abs

    def getMedianTime = stats.getStatistics(0.5).map(_.finished.values.map(_.percentile).sum).getOrElse(0.0)

    def getCurrentPoolSize = threadPool.getPoolSize

    def getActivePoolSize = threadPool.getActiveCount

    def getTotalRequestsInGcSlot = totalRequestsInGcSlot.get.abs
  }
}

