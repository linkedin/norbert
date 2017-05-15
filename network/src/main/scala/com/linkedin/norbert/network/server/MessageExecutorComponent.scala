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

import com.linkedin.norbert.network.garbagecollection.{GcDetector, GcParamWrapper}
import com.linkedin.norbert.logging.Logging
import jmx.JMX.MBean
import jmx.JMX
import java.util.concurrent.atomic.AtomicInteger

import norbertutils.{NamedPoolThreadFactory, SystemClock}
import java.util.concurrent._

import scala.collection.mutable.MutableList
import common.CachedNetworkStatistics
import norbertutils._
import cluster.Node
import network.netty.TimingKeys

/**
 * A component which submits incoming messages to their associated message handler.
 */
trait MessageExecutorComponent {
  val messageExecutor: MessageExecutor
}

trait MessageExecutor {
  def executeMessage[RequestMsg, ResponseMsg] (request: RequestMsg, messageName: String,
                                               responseHandler: Option[(Either[Exception, ResponseMsg]) => Unit],
                                               context: Option[RequestContext] = None)

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

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, requestQueue,
    new NamedPoolThreadFactory("norbert-message-executor")
  )

  val statsJmx = JMX.register(new RequestProcessorMBeanImpl(clientName, serviceName, statsActor, requestQueue, threadPool))

  def setRequestTimeout(newValue : Long) = {
    requestTimeout = newValue
    log.info("Setting timeout to " + newValue)
  }

  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg,
                                       messageName: String,
                                       responseHandler: Option[(Either[Exception, ResponseMsg]) => Unit],
                                       context: Option[RequestContext] = None) {
    val rr: AbstractRequestRunner[RequestMsg, ResponseMsg] =
      messageHandlerRegistry.getHandler(messageName) match {
        case sync: SyncHandlerEntry[RequestMsg, ResponseMsg] =>
          new SyncRequestRunner[RequestMsg, ResponseMsg](request, requestTimeout, context, filters, sync, responseHandler)
        case async: AsyncHandlerEntry[RequestMsg, ResponseMsg] =>
          new AsyncRequestRunner[RequestMsg, ResponseMsg](request, requestTimeout, context, filters, async, responseHandler)
      }

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

  private abstract class AbstractRequestRunner[RequestMsg, ResponseMsg](request: RequestMsg,
                                                                        reqTimeout: Long,
                                                                        context: Option[RequestContext],
                                                                        filters: MutableList[Filter],
                                                                        callback: Option[(Either[Exception, ResponseMsg]) => Unit],
                                                                        val queuedAt: Long = System.currentTimeMillis,
                                                                        val id: Int = idGenerator.getAndIncrement.abs) extends Runnable {

    def beforeExecute(): Unit = {
      statsActor.beginRequest(0, id, (System.currentTimeMillis() - queuedAt) * 1000)

      val now = System.currentTimeMillis
      if (now - queuedAt > reqTimeout) {
        totalNumRejected.incrementAndGet
        log.warn("Request timed out, ignoring! Currently = " + now + ". Queued at = " + queuedAt + ". Timeout = " + requestTimeout)
        if (callback.isDefined) callback.get(Left(new HeavyLoadException))
      } else {
        log.debug("Executing message: %s".format(request))

        context match {
          case Some(null) => ()
          case Some(m) => if (m.attributes != null) {
            m.attributes += (TimingKeys.ON_REQUEST_TIME_ATTR -> now)
          }
          case None => ()
        }

        filters.foreach(filter => continueOnError(filter.onRequest(request, context.getOrElse(null))))
      }
    }

    def afterExecute(result: Either[Exception, ResponseMsg]): Unit = {
      val responseTime = System.currentTimeMillis - queuedAt
      if (responseGenerationTimeoutMillis > 0 && responseTime > responseGenerationTimeoutMillis) {
        totalNumRejected.incrementAndGet
        log.warn("Request timed out by the time we generated response, ignoring! Response latency = " + responseTime + ". " +
          "Queued at = " + queuedAt + ". Timeout = " + requestTimeout)
        if (callback.isDefined) callback.get(Left(new Exception("Response took too long:%d".format(responseTime))))
      } else {
        if (callback.isDefined) callback.get(result)
      }

      context match {
        case Some(null) => ()
        case Some(m) => if (m.attributes != null) {
          m.attributes += (TimingKeys.ON_RESPONSE_TIME_ATTR -> System.currentTimeMillis)
        }
        case None => ()
      }

      result match {
        case Left(ex) => {
          log.error(ex, "Message handler returned an exception")
          filters.reverse.foreach(filter => continueOnError(filter.onError(ex, context.getOrElse(null))))
        }
        case Right(responseMsg) => filters.reverse.foreach(filter => continueOnError(filter.onResponse(responseMsg, context.getOrElse(null))))
      }

      statsActor.endRequest(0, id)
    }
  }

  private class SyncRequestRunner[RequestMsg, ResponseMsg](request: RequestMsg,
                                                           reqTimeout: Long,
                                                           context: Option[RequestContext],
                                                           filters: MutableList[Filter],
                                                           handlerEntry: SyncHandlerEntry[RequestMsg, ResponseMsg],
                                                           callback: Option[(Either[Exception, ResponseMsg]) => Unit])
    extends AbstractRequestRunner[RequestMsg, ResponseMsg](request, reqTimeout, context, filters, callback) {

    def run() = {
      beforeExecute()

      val response: Option[Either[Exception, ResponseMsg]] =
        try {
          val response = handlerEntry.handler(request)
          response match {
            case _:Unit => None
            case null => None
            case _ => Some(Right(response))
          }
        } catch {
          case ex: Exception =>
            Some(Left(ex))
        }

      response match {
        case Some(resp) => afterExecute(resp)
        case None => statsActor.endRequest(0, id)
      }
    }
  }

  private class AsyncRequestRunner[RequestMsg, ResponseMsg](request: RequestMsg,
                                                            reqTimeout: Long,
                                                            context: Option[RequestContext],
                                                            filters: MutableList[Filter],
                                                            handlerEntry: AsyncHandlerEntry[RequestMsg, ResponseMsg],
                                                            callback: Option[(Either[Exception, ResponseMsg]) => Unit])
    extends AbstractRequestRunner[RequestMsg, ResponseMsg](request, reqTimeout, context, filters, callback) {

    val callbackCtx: CallbackContext[ResponseMsg] = new CallbackContext[ResponseMsg] {
      override def onResponse(responseMsg: ResponseMsg): Unit = afterExecute(Right(responseMsg))
      override def onError(ex: Exception): Unit = afterExecute(Left(ex))
      override def getCallbackExecutor(): Executor = threadPool
    }

    def run() = {
      beforeExecute()

      try {
        handlerEntry.handler(request, callbackCtx)
      } catch {
        case ex: Exception => afterExecute(Left(ex))
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

