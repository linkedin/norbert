package com.linkedin.norbert
package network
package common

import logging.Logging
import norbertutils._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import jmx.JMX.MBean
import cluster.Node
import java.util.{UUID, Map => JMap}
import netty.HealthScoreCalculator
import jmx.{JMX, RequestTimeTracker}
import netty.ClientStatisticsRequestStrategy

object CachedNetworkStatistics {
  def apply[GroupIdType, RequestIdType](clock: Clock, timeWindow: Long, refreshInterval: Long): CachedNetworkStatistics[GroupIdType, RequestIdType] = {
    new CachedNetworkStatistics(NetworkStatisticsTracker(clock, timeWindow), clock, refreshInterval)
  }
}

case class CacheMaintainer[T](clock: Clock, ttl: Long, fn: () => T) {
  val refreshing = new AtomicBoolean(false)
  val lastUpdateTime = new AtomicLong(0)
  @volatile var item: T = _

  private def refresh {
    val lut = lastUpdateTime.get
    val now = clock.getCurrentTimeMilliseconds

    if(item == null || now - lut > ttl) {
      // Let one thread pass through to update the calculation
      if(refreshing.compareAndSet(false, true)) {
        lastUpdateTime.set(now)

        refresh0

        refreshing.set(false)
      }
    }
  }

  private def refresh0 {
    item = fn()
  }

  def get: Option[T] = {
    refresh
    Option(item)
  }
}

class CachedNetworkStatistics[GroupIdType, RequestIdType](private val stats: NetworkStatisticsTracker[GroupIdType, RequestIdType], clock: Clock, refreshInterval: Long) {
  val finishedArray = CacheMaintainer(clock, refreshInterval, () => stats.getFinishedArrays)
  val timings = CacheMaintainer(clock, refreshInterval, () => stats.getTimings)
  val netty = CacheMaintainer(clock, refreshInterval, () => stats.getNettyTimings)
  val pendingTimings = CacheMaintainer(clock, refreshInterval, () => stats.getPendingTimings)
  val totalRequests = CacheMaintainer(clock, refreshInterval, () => stats.getTotalRequests )
  val finishedQueueTimings = CacheMaintainer(clock, refreshInterval, () => stats.getQueueTimings)
  val responseTimings = CacheMaintainer(clock, refreshInterval, () => stats.getResponseTimings)

  def beginRequest(groupId: GroupIdType, requestId: RequestIdType, queueTime: Long) {
    stats.beginRequest(groupId, requestId, queueTime)
  }

  def beginNetty(groupId: GroupIdType, requestId: RequestIdType, queueTime: Long) {
    stats.beginNetty(groupId, requestId, queueTime)
  }

  def endNetty( groupId: GroupIdType, requestId: RequestIdType) {
    stats.endNetty(groupId, requestId)
  }

  def endRequest(groupId: GroupIdType, requestId: RequestIdType) {
    stats.endRequest(groupId, requestId)
  }

  def reset { stats.reset }

  private def calculate(map: Map[GroupIdType, Array[Long]], p: Double) = {
    map.mapValues { v =>
      StatsEntry(calculatePercentile(v, p), v.length, v.sum)
      //StatsEntry(calculatePercentile(v,p), v.length, 0)
    }
  }

  val statisticsCache =
    new java.util.concurrent.ConcurrentHashMap[Double, CacheMaintainer[JoinedStatistics[GroupIdType]]]

  def getStatistics(p: Double) = {
    atomicCreateIfAbsent(statisticsCache, p) { k =>
      CacheMaintainer(clock, refreshInterval, () => {
        JoinedStatistics(
          finished = timings.get.map(calculate(_, p)).getOrElse(Map.empty),
          nettyTimings = netty.get.map(calculate(_, p)).getOrElse(Map.empty),
          pending = pendingTimings.get.map(calculate(_, p)).getOrElse(Map.empty),
          totalRequests = () => totalRequests.get.getOrElse(Map.empty),
          rps = () => finishedArray.get.map(_.mapValues(rps(_))).getOrElse(Map.empty),
          requestQueueSize = () => finishedArray.get.map(_.mapValues(_.length)).getOrElse(Map.empty),
          finishedQueueTime = finishedQueueTimings.get.map(calculate(_, p)).getOrElse(Map.empty),
          finishedResponse = responseTimings.get.map(calculate(_, p)).getOrElse(Map.empty))
      })
    }.get
  }

  private def rps(data: Array[(Long, Long)]): Int = {
    val now = clock.getCurrentTimeOffsetMicroseconds

    implicit val timeOrdering: Ordering[(Long, Long)] = new Ordering[(Long, Long)] {
      def compare(x: (Long, Long), y: (Long, Long)) = (x._1 - y._1).asInstanceOf[Int]
    }

    val bs = binarySearch(data, (now - 1000000L, 0L))
    val idx = if(bs < 0) -bs - 1 else bs
    data.size - idx
  }
}

case class StatsEntry(percentile: Double, size: Int, total: Long)
case class JoinedStatistics[K](finished: Map[K, StatsEntry],
                               nettyTimings: Map[K, StatsEntry],
                               pending: Map[K, StatsEntry],
                               rps: () => Map[K, Int],
                               totalRequests: () => Map[K, Int],
                               requestQueueSize: () => Map[K, Int],
                               finishedQueueTime: Map[K, StatsEntry],
                               finishedResponse: Map[K, StatsEntry])

private case class NetworkStatisticsTracker[GroupIdType, RequestIdType](clock: Clock, timeWindow: Long) extends Logging {
  private var timeTrackers: java.util.concurrent.ConcurrentMap[GroupIdType, RequestTimeTracker[RequestIdType]] =
    new java.util.concurrent.ConcurrentHashMap[GroupIdType, RequestTimeTracker[RequestIdType]]

  private def getTracker(groupId: GroupIdType) = {
    atomicCreateIfAbsent(timeTrackers, groupId) { k => new RequestTimeTracker(clock, timeWindow) }
  }

  def beginRequest(groupId: GroupIdType, requestId: RequestIdType, queueTime:Long = 0) {
    getTracker(groupId).beginRequest(requestId, queueTime)
  }

  def beginNetty(groupId: GroupIdType, requestId: RequestIdType, queueTime:Long = 0) {
    getTracker(groupId).beginNetty(requestId, queueTime)
  }

  def endNetty(groupId: GroupIdType, requestId: RequestIdType) {
    getTracker(groupId).endNetty(requestId)
  }

  def endRequest(groupId: GroupIdType, requestId: RequestIdType) {
    getTracker(groupId).endRequest(requestId)
  }

  import scala.collection.JavaConversions._

  def reset { timeTrackers.values.foreach(_.reset) }

  def getPendingTimings = {
    timeTrackers.toMap.mapValues( _.pendingRequestTimeTracker.getTimings)
  }

  def getTimings = {
    getFinishedArrays.mapValues(array => array.map(_._2).sorted)
  }

  def getNettyTimings = {
    getNettyArrays.mapValues(array => array.map(_._2).sorted)
  }

  def getFinishedArrays = {
    timeTrackers.toMap.mapValues( _.finishedRequestTimeTracker.getArray)
  }

  def getNettyArrays = {
    timeTrackers.toMap.mapValues( _.finishedNettyTimeTracker.getArray)
  }

  def getTotalRequests = timeTrackers.toMap.mapValues( _.pendingRequestTimeTracker.getTotalNumRequests )

  //this does not need to be sorted since we are not doing 90th and 99th percentile
  def getQueueTimings = {
    timeTrackers.toMap.mapValues(_.queueTimeTracker.getTimings)
  }

  def getResponseTimings = {
    getFinishedRequestTimeArrays.mapValues(array => array.map(_._2).sorted)
  }

  def getFinishedRequestTimeArrays = {
    timeTrackers.toMap.mapValues( _.totalRequestProcessingTimeTracker.getArray)
  }
}

trait NetworkClientStatisticsMBean {
  def getNumPendingRequests: JMap[Int, Int]

  def getMedianTimes: JMap[Int, Double]
  def get75thTimes: JMap[Int, Double]
  def get90thTimes: JMap[Int, Double]
  def get95thTimes: JMap[Int, Double]
  def get99thTimes: JMap[Int, Double]
  def getHealthScoreTimings: JMap[Int, Double]

  def getNettyMedianTimes: JMap[Int, Double]
  def getNetty75thTimes: JMap[Int, Double]
  def getNetty90thTimes: JMap[Int, Double]
  def getNetty95thTimes: JMap[Int, Double]
  def getNetty99thTimes: JMap[Int, Double]

  def getRPS: JMap[Int, Int]

  def getTotalRequests: JMap[Int, Int]

  def getClusterRPS: Int
  def getClusterAverageTime: Double
  def getClusterPendingTime: Double

  def getClusterMedianTime: Double
  def getCluster75thTimes: Double
  def getCluster90th: Double
  def getCluster95th: Double
  def getCluster99th: Double
  def getClusterHealthScoreTiming: Double

  def getClusterNettyMedianTime: Double
  def getClusterNetty75thTimes: Double
  def getClusterNetty90th: Double
  def getClusterNetty95th: Double
  def getClusterNetty99th: Double

  def getClusterTotalRequests: Int

  def reset

  // Jill will be very upset if I break her graphs
  def getRequestsPerSecond = getClusterRPS
  def getAverageRequestProcessingTime = getClusterAverageTime

  def getQueueSize: Int

  def getNodesMarkedDown: Long
  def getNumReroutedRequests: Long
}

class NetworkClientStatisticsMBeanImpl(clientName: Option[String], serviceName: String, val stats: CachedNetworkStatistics[Node, UUID], strategy: ClientStatisticsRequestStrategy)
  extends MBean(classOf[NetworkClientStatisticsMBean], JMX.name(clientName, serviceName)) with HealthScoreCalculator
  with NetworkClientStatisticsMBean {

  private def getPendingStats(p: Double) = stats.getStatistics(p).map(_.pending).getOrElse(Map.empty)
  private def getFinishedStats(p: Double) = stats.getStatistics(p).map(_.finished).getOrElse(Map.empty)
  private def getNettyFinishedStats(p: Double) = stats.getStatistics(p).map(_.nettyTimings).getOrElse(Map.empty)
  private def toMillis(statsMetric: Double):Double = statsMetric/1000
  def getNumPendingRequests = toJMap(getPendingStats(0.5).map(kv => (kv._1.id, kv._2.size)))

  def getMedianTimes =
    toJMap(getFinishedStats(0.5).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def get75thTimes =
    toJMap(getFinishedStats(0.75).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def get90thTimes =
    toJMap(getFinishedStats(0.90).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def get95thTimes =
    toJMap(getFinishedStats(0.95).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def get99thTimes =
    toJMap(getFinishedStats(0.99).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def getNettyMedianTimes =
    toJMap(getNettyFinishedStats(0.5).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def getNetty75thTimes =
    toJMap(getNettyFinishedStats(0.75).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def getNetty90thTimes =
    toJMap(getNettyFinishedStats(0.90).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def getNetty95thTimes =
    toJMap(getNettyFinishedStats(0.95).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def getNetty99thTimes =
    toJMap(getNettyFinishedStats(0.99).map(kv => (kv._1.id, toMillis(kv._2.percentile))))

  def getHealthScoreTimings = {
    val s = stats.getStatistics(0.5)
    val f = s.map(_.finished).getOrElse(Map.empty)
    val p = s.map(_.pending).getOrElse(Map.empty)

    toJMap(f.map { case (n, nodeN) =>
      val nodeP = p.get(n).getOrElse(StatsEntry(0.0, 0, 0))
      (n.id, toMillis(doCalculation(Map(0 -> nodeP),Map(0 -> nodeN))))
    })
  }

  def getRPS = toJMap(stats.getStatistics(0.5).map(_.rps().map(kv => (kv._1.id, kv._2))))

  def getTotalRequests = toJMap(stats.getStatistics(0.5).map(_.totalRequests().map(kv => (kv._1.id, kv._2))))

  def getClusterAverageTime = {
    val s = getFinishedStats(0.5)
    val total = s.values.map(_.total).sum
    val size = s.values.map(_.size).sum

    toMillis(safeDivide(total, size)(0.0))
  }

  def getClusterPendingTime = {
    val s = getPendingStats(0.5)
    toMillis(s.values.map(_.total).sum)
  }

  def getClusterMedianTime = toMillis(averagePercentiles(getFinishedStats(0.5)))

  def getCluster75thTimes = toMillis(averagePercentiles(getFinishedStats(0.75)))

  def getCluster90th = toMillis(averagePercentiles(getFinishedStats(0.90)))

  def getCluster95th = toMillis(averagePercentiles(getFinishedStats(0.95)))

  def getCluster99th = toMillis(averagePercentiles(getFinishedStats(0.99)))

  def getClusterNettyMedianTime = toMillis(averagePercentiles(getNettyFinishedStats(0.5)))

  def getClusterNetty75thTimes = toMillis(averagePercentiles(getNettyFinishedStats(0.75)))

  def getClusterNetty90th = toMillis(averagePercentiles(getNettyFinishedStats(0.90)))

  def getClusterNetty95th = toMillis(averagePercentiles(getNettyFinishedStats(0.95)))

  def getClusterNetty99th = toMillis(averagePercentiles(getNettyFinishedStats(0.99)))

  import scala.collection.JavaConversions._

  def getClusterRPS = {
    getRPS.values.sum
  }

  def getClusterTotalRequests = getTotalRequests.values.sum

  def getClusterHealthScoreTiming = toMillis(doCalculation(getPendingStats(0.5), getFinishedStats(0.5)))

  def getQueueSize = stats.getStatistics(0.5).map(_.requestQueueSize().values.sum) getOrElse(0)

  def getNodesMarkedDown = strategy.canServeRequests.get.getOrElse(Map.empty).count({ case (node, up) => up == false })

  def getNumReroutedRequests = strategy.totalNumReroutes.get

  def reset = stats.reset
}

