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
package netty

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.jmx.JMX
import com.linkedin.norbert.jmx.JMX.MBean
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.common.{BackoffStrategy, CachedNetworkStatistics}
import com.linkedin.norbert.norbertutils.{Clock, SystemClock}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ConnectTimeoutException}

class ChannelPoolClosedException extends Exception

class ChannelPoolFactory(maxConnections: Int, openTimeoutMillis: Int, writeTimeoutMillis: Int,
                         bootstrap: ClientBootstrap,
                         errorStrategy: Option[BackoffStrategy],
                         staleRequestTimeoutMins: Int,
                         staleRequestCleanupFreqMins: Int,
                         closeChannelTimeMillis: Long,
                         stats: CachedNetworkStatistics[Node, UUID]) {

  def newChannelPool(address: InetSocketAddress): ChannelPool = {
    val group = new DefaultChannelGroup("norbert-client [%s]".format(address))
    new ChannelPool(address = address,
      maxConnections = maxConnections,
      openTimeoutMillis = openTimeoutMillis,
      writeTimeoutMillis = writeTimeoutMillis,
      bootstrap = bootstrap,
      channelGroup = group,
      closeChannelTimeMillis = closeChannelTimeMillis,
      staleRequestTimeoutMins = staleRequestTimeoutMins,
      staleRequestCleanupFreqMins = staleRequestCleanupFreqMins,
      errorStrategy = errorStrategy,
      clock = SystemClock,
      stats)
  }

  def shutdown: Unit = {
    bootstrap.releaseExternalResources
  }
}

class ChannelPool(address: InetSocketAddress, maxConnections: Int, openTimeoutMillis: Int, writeTimeoutMillis: Int,
                  bootstrap: ClientBootstrap,
                  channelGroup: ChannelGroup,
                  closeChannelTimeMillis: Long,
                  staleRequestTimeoutMins: Int,
                  staleRequestCleanupFreqMins: Int,
                  val errorStrategy: Option[BackoffStrategy],
                  clock: Clock,
                  stats: CachedNetworkStatistics[Node, UUID]) extends Logging {

  case class PoolEntry(channel: Channel, creationTime: Long) {
    def age = System.currentTimeMillis() - creationTime

    def reuseChannel(closeChannelTimeMillis: Long) = closeChannelTimeMillis < 0 || age < closeChannelTimeMillis
  }

  private val pool = new ArrayBlockingQueue[PoolEntry](maxConnections)
  private val waitingWrites = new LinkedBlockingQueue[BaseRequest[_]]
  private val poolSize = new AtomicInteger(0)
  private val closed = new AtomicBoolean
  private val softClosed = new AtomicBoolean
  private val requestsSent = new AtomicInteger(0)
  private val lock = new java.util.concurrent.locks.ReentrantReadWriteLock(true)


  val cleanupTask = new Runnable() {
    val staleRequestTimeoutMillis = TimeUnit.MILLISECONDS.convert(staleRequestTimeoutMins, TimeUnit.MINUTES)

    override def run() {
      if (staleRequestTimeoutMins > 0) {
        try {
          var expiredEntryCount = 0

          val waitingRequestIter = waitingWrites.iterator()
          while (waitingRequestIter.hasNext) {
            val request = waitingRequestIter.next()
            val now = System.currentTimeMillis
            if ((now - request.timestamp) > staleRequestTimeoutMillis) {
              waitingWrites.remove(request)
              expiredEntryCount += 1
            }
          }

          if (expiredEntryCount > 0) {
            log.info("Expired %d stale waiting writes from the write queue.".format(expiredEntryCount))
          }
        } catch {
          case e: InterruptedException =>
            Thread.currentThread.interrupt()
            log.info(e, "Interrupted exception in cleanup task")
          case e: Exception => log.error(e, "Exception caught in cleanup task, ignoring ")
        }
      }
    }
  }

  val cleanupExecutor = new ScheduledThreadPoolExecutor(1)
  if (staleRequestCleanupFreqMins > 0) {
    cleanupExecutor.scheduleAtFixedRate(cleanupTask,
      staleRequestCleanupFreqMins,
      staleRequestCleanupFreqMins,
      TimeUnit.MINUTES)
  }


  private val jmxHandle = JMX.register(new MBean(classOf[ChannelPoolMBean], "address=%s,port=%d".format(address.getHostName, address.getPort)) with ChannelPoolMBean {
    def getWriteQueueSize = waitingWrites.size

    def getOpenChannels = poolSize.get

    def getMaxChannels = maxConnections

    def getNumberRequestsSent = requestsSent.get.abs
  })

  def sendRequest[RequestMsg](request: BaseRequest[RequestMsg]): Unit = if (closed.get) {
    throw new ChannelPoolClosedException
  } else {
    checkoutChannel match {
      case Some(poolEntry) =>
        writeRequestToChannel(request, poolEntry.channel)
        checkinChannel(poolEntry)
      case None =>
        waitingWrites.offer(request)
        openChannel(request)
    }
  }
 
  /*
   * This is a soft close where we get rid of the JMX values only.
   * The reason for splitting it in two phases is to avoid extra synchronization overhead
  */ 
  def unregisterJMX {
    softClosed.set(true)
    jmxHandle.foreach {JMX.unregister(_)}
  }

  def close {
    if (closed.compareAndSet(false, true)) {
      if(!softClosed.get()) {
        jmxHandle.foreach {JMX.unregister(_)}
      }
      channelGroup.close.awaitUninterruptibly
    }
  }

  private def checkinChannel(poolEntry: PoolEntry, isFirstWriteToChannel: Boolean = false) {
    while (!waitingWrites.isEmpty) {
      waitingWrites.poll match {
        case null => // do nothing

        case request =>
          val timeout = if (isFirstWriteToChannel) writeTimeoutMillis + openTimeoutMillis else writeTimeoutMillis
          if((System.currentTimeMillis - request.timestamp) < timeout)
            writeRequestToChannel(request, poolEntry.channel)
          else
            request.onFailure(new TimeoutException("Timed out while waiting to write"))
      }
    }

    if(poolEntry.reuseChannel(closeChannelTimeMillis))
      pool.offer(poolEntry)
    else {
      poolSize.decrementAndGet()
      poolEntry.channel.close()
    }
  }

  private def checkoutChannel: Option[PoolEntry] = {
    var poolEntry: PoolEntry = null
    var found = false
    while (!pool.isEmpty && !found) {
      pool.poll match {
        case null => // do nothing

        case pe =>
          if (pe.channel.isConnected) {
            if(pe.reuseChannel(closeChannelTimeMillis)) {
              poolEntry = pe
              found = true
            } else {
              poolSize.decrementAndGet()
              pe.channel.close()
            }
          } else {
            poolSize.decrementAndGet()
          }
      }
    }

    Option(poolEntry)
  }

  private def openChannel(request: BaseRequest[_]) {
    if (poolSize.incrementAndGet > maxConnections) {
      poolSize.decrementAndGet
      log.warn("Unable to open channel, pool is full. Waiting for another channel to return to queue before processing")
    } else {
      log.debug("Opening a channel to: %s".format(address))

      bootstrap.connect(address).addListener(new ChannelFutureListener {
        def operationComplete(openFuture: ChannelFuture) = {
          if (openFuture.isSuccess) {
            val channel = openFuture.getChannel
            log.debug("Opened a channel to: %s".format(address))

            channelGroup.add(channel)

            val poolEntry = PoolEntry(channel, System.currentTimeMillis())
            checkinChannel(poolEntry, isFirstWriteToChannel = true)
          } else {
            openFuture.getCause match {
              case _:ConnectTimeoutException =>
                log.warn(openFuture.getCause, "Timeout when opening channel to: %s, marking offline".format(address))
              case _ =>
                log.error(openFuture.getCause, "Error when opening channel to: %s, marking offline".format(address))
            }
            errorStrategy.foreach(_.notifyFailure(request.node))
            poolSize.decrementAndGet

            request.onFailure(openFuture.getCause)
          }
        }
      })
    }
  }

  private def writeRequestToChannel(request: BaseRequest[_], channel: Channel) {
    log.debug("Writing to %s: %s".format(channel, request))
    requestsSent.incrementAndGet
    channel.write(request).addListener(new ChannelFutureListener {
      def operationComplete(writeFuture: ChannelFuture) = if (!writeFuture.isSuccess) {
        // Take the node out of rotation for a bit
        log.warn("IO exception for " + request.node + ", marking node offline")
        errorStrategy.foreach(_.notifyFailure(request.node))
        channel.close
        request.onFailure(writeFuture.getCause)
      } else {
        request.startNettyTiming(stats)
      }

    })
  }

  def numRequestsSent = requestsSent.get
}

trait ChannelPoolMBean {
  def getOpenChannels: Int
  def getMaxChannels: Int
  def getWriteQueueSize: Int
  def getNumberRequestsSent: Int
}
