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


import logging.Logging
import jmx.JMX.MBean
import jmx.{FinishedRequestTimeTracker, JMX}
import actors.DaemonActor
import java.util.concurrent.atomic.AtomicInteger
import norbertutils.{SystemClock, NamedPoolThreadFactory}
import java.util.concurrent._
import scala.collection.mutable.MutableList
import common.CachedNetworkStatistics
import util.ProtoUtils
import norbertutils._
import cluster.{ClusterDisconnectedException, ClusterClientComponent}

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
                                responseGenerationTimeoutMillis: Long) extends MessageExecutor with Logging {
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
    this(clientName, serviceName, messageHandlerRegistry, new MutableList[Filter], requestTimeout, corePoolSize, maxPoolSize, keepAliveTime, maxWaitingQueueSize, requestStatisticsWindow, responseGenerationTimeoutMillis)

  private val statsActor = CachedNetworkStatistics[Int, Int](SystemClock, requestStatisticsWindow, 200L)
  private val totalNumRejected = new AtomicInteger

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
      if(now - queuedAt > reqTimeout) {
        totalNumRejected.incrementAndGet
        log.warn("Request timed out, ignoring! Currently = " + now + ". Queued at = " + queuedAt + ". Timeout = " + requestTimeout)
        callback.foreach(_(Left(new HeavyLoadException)))
      } else {
        log.debug("Executing message: %s".format(request))

        val response: Option[Either[Exception, ResponseMsg]] =
        try {
          filters.foreach(filter => continueOnError(filter.onRequest(request, context.getOrElse(null))))
          val handler = messageHandlerRegistry.handlerFor(request)
          try {
            val response = handler(request)
            val timeResponse = System.currentTimeMillis - queuedAt
            if(responseGenerationTimeoutMillis>0 && timeResponse > responseGenerationTimeoutMillis) {
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
        response.foreach { (res) =>
          if(!callback.isEmpty) callback.get(res)
          res match {
            case Left(ex) => filters.reverse.foreach(filter => continueOnError(filter.onError(ex, context.getOrElse(null))))
            case Right(responseMsg) =>  filters.reverse.foreach(filter => continueOnError(filter.onResponse(responseMsg, context.getOrElse(null))))
          }
        }
      }
    }
  }

  trait RequestProcessorMBean {
    def getQueueSize: Int

    def getTotalNumRejected: Int

    def getMedianTime: Double
    
    def getCurrentPoolSize: Int
    
    def getActivePoolSize: Int
  }

  class RequestProcessorMBeanImpl(clientName: Option[String], serviceName: String, val stats: CachedNetworkStatistics[Int, Int], queue: ArrayBlockingQueue[Runnable], threadPool: ThreadPoolExecutor)
    extends MBean(classOf[RequestProcessorMBean], JMX.name(clientName, serviceName)) with RequestProcessorMBean {
    def getQueueSize = queue.size

    def getTotalNumRejected = totalNumRejected.get.abs

    def getMedianTime = stats.getStatistics(0.5).map(_.finished.values.map(_.percentile).sum).getOrElse(0.0)

    def getCurrentPoolSize = threadPool.getPoolSize

    def getActivePoolSize = threadPool.getActiveCount
  }
}

