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

import java.util.concurrent._
import atomic.{AtomicLong, AtomicInteger}
import logging.Logging
import annotation.tailrec
import partitioned.PartitionedNetworkClient
import java.util
import com.sun.xml.internal.ws.resources.SoapMessages
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.client.NetworkClientConfig

class ResponseQueue[ResponseMsg] extends java.util.concurrent.LinkedBlockingQueue[Either[Throwable, ResponseMsg]] {
  def += (res: Either[Throwable, ResponseMsg]): ResponseQueue[ResponseMsg] = {
    add(res)
    this
  }
}

trait ListenableFuture {
  def addListener(listener:Runnable, executor:Executor)
}

/**
 *
 * If the underlying async request completes with success then we will get onCompleted handler invoked
 * Else the onThrowable handler will get invoked. Timeouts need to be handled outside of this.
 */
abstract class PromiseListener[ResponseMsg] {
  /**
   * Depending on the timing this method
   * could execute in the calling thread
   * of addListener or in the norbert client
   * thread. Fork another thread if this
   * is going to be a heavy call.
   */
  def onCompleted(response: ResponseMsg):Unit = {
  }
  /**
   * Depending on the timing this method
   * could execute in the calling thread
   * of addListener or in the norbert client
   * thread. Fork another thread if this
   * is going to be a heavy call.
   */
  def onThrowable(t: Throwable):Unit = {
  }
}

class FutureAdapterListener[ResponseMsg] extends FutureAdapter[ResponseMsg] {
  object InvokerThread {
    val none = 0;
    val requestingThread = 1;
    val norbertThread = 2;
  }
  @volatile var mListener: PromiseListener[ResponseMsg] = null
  val callbackInvokingThread:AtomicInteger = new AtomicInteger(InvokerThread.none)
  def addListener(listener: PromiseListener[ResponseMsg]) {
    mListener = listener
    if (isDone) {
      if (callbackInvokingThread.compareAndSet(InvokerThread.none, InvokerThread.requestingThread)) {
        response match {
          case Left(t) => listener.onThrowable(t)
          case Right(response) => listener.onCompleted(response)
          case _ => listener.onThrowable(new IllegalStateException("Response was neither throwable nor an exception"))
        }
      }
    }
  }

  override def apply(callback: Either[Throwable, ResponseMsg]): Unit = {
    super.apply(callback)
    if (mListener != null) {
      if (callbackInvokingThread.compareAndSet(InvokerThread.none, InvokerThread.norbertThread)) {
        response match {
          case Left(t) => mListener.onThrowable(t)
          case Right(response) => mListener.onCompleted(response)
          case _ => mListener.onThrowable(new IllegalStateException("Response was neither throwable nor an exception"))
        }
      }
    }
  }
}

class FutureAdapter[ResponseMsg] extends Future[ResponseMsg] with Function1[Either[Throwable, ResponseMsg], Unit] with ResponseHelper {
  protected val latch = new CountDownLatch(1)
  @volatile protected var response: Either[Throwable, ResponseMsg] = null

  override def apply(callback: Either[Throwable, ResponseMsg]): Unit = {
    response = callback
    latch.countDown
  }

  def cancel(mayInterruptIfRunning: Boolean) = false

  def isCancelled = false

  def isDone = latch.getCount == 0

  def get = {
    latch.await
    translateResponse(response)
  }

  def get(timeout: Long, timeUnit: TimeUnit) =
    if (latch.await(timeout, timeUnit)) translateResponse(response) else throw new TimeoutException
}

class NorbertResponseIterator[ResponseMsg](numResponses: Int, queue: ResponseQueue[ResponseMsg]) extends ResponseIterator[ResponseMsg] with ResponseHelper {
  protected val remaining = new AtomicInteger(numResponses)

  def next = {
    remaining.decrementAndGet
    translateResponse(queue.take)
  }

  def next(timeout: Long, unit: TimeUnit) = queue.poll(timeout, unit) match {
    case null =>
      remaining.decrementAndGet
      throw new TimeoutException("Timed out waiting for response")

    case e =>
      remaining.decrementAndGet
      translateResponse(e)
  }

  def nextAvailable = queue.size > 0

  def hasNext = remaining.get > 0
}

/**
 * Internal use only
 */
class NorbertDynamicResponseIterator[ResponseMsg](initialNumResponses: Int, queue: ResponseQueue[ResponseMsg]) extends NorbertResponseIterator[ResponseMsg](initialNumResponses, queue) with DynamicResponseIterator[ResponseMsg] {

  def addAndGet(delta: Int) = {
    remaining.addAndGet(delta)
  }

}

/**
 * An iterator that will timeout a'fter a set amount of time spent waiting on remote data
 */
case class TimeoutIterator[ResponseMsg](inner: ResponseIterator[ResponseMsg], timeout: Long = 5000L) extends ResponseIterator[ResponseMsg] {
  private val timeLeft = new AtomicInteger(timeout.asInstanceOf[Int])

  def hasNext = inner.hasNext && timeLeft.get() > 0

  def nextAvailable = inner.nextAvailable

  def next: ResponseMsg = {
    val before = System.currentTimeMillis

    try {
      return inner.next(timeLeft.get, TimeUnit.MILLISECONDS)
    } finally {
      val time = (System.currentTimeMillis - before).asInstanceOf[Int]
      timeLeft.addAndGet(-time)
    }
  }

  def next(t: Long, unit: TimeUnit): ResponseMsg = {
    val before = System.currentTimeMillis
    val methodTimeout = unit.toMillis(t)

    try  {
      return inner.next(math.min(methodTimeout, timeLeft.get), TimeUnit.MILLISECONDS)
    } finally {
      val time = (System.currentTimeMillis - before).asInstanceOf[Int]
      timeLeft.addAndGet(-time)
    }
  }
}

/**
 * An optional iterator you can use that exposes the success/failure options for each message
 * as a monad
 */
case class ExceptionIterator[ResponseMsg](inner: ResponseIterator[ResponseMsg]) extends ResponseIterator[Either[Exception, ResponseMsg]] {
  def hasNext = inner.hasNext

  def nextAvailable = inner.nextAvailable

  def next = try {
    val result = inner.next
    Right(result)
  } catch {
    case ex: Exception =>
    Left(ex)
  }

  def next(timeout: Long, unit: TimeUnit) = try {
    val result = inner.next(timeout, unit)
    Right(result)
  } catch {
    case ex: Exception =>
    Left(ex)
  }
}

/**
 * A "partial iterator" If there's an exception during one of the computations, the iterator will simply ignore
 * that exception
 * This is useful for scatter-gather algorithms that may be able to temporarily tolerate partial results for
 * stability, such as in the case of search.
 */
case class PartialIterator[ResponseMsg](inner: ExceptionIterator[ResponseMsg]) extends ResponseIterator[ResponseMsg] {
  var nextElem: Either[Exception, ResponseMsg] = null

  def hasNext: Boolean = hasNext0

  @tailrec private final def hasNext0: Boolean = {
    if (nextElem != null) {
      if (nextElem.isRight) true
      else {
        nextElem = null
        hasNext0
      }
    } else {
      if (inner.hasNext) {
        nextElem = inner.next
        hasNext0
      } else {
        false
      }
    }
  }

  def nextAvailable = inner.nextAvailable

  def next = {
    val hn = hasNext
    if (hn) {
      val result = nextElem.right.get
      nextElem = null
      result
    } else {
      throw new NoSuchElementException()
    }
  }

  def next(timeout: Long, unit: TimeUnit) = {
    next // ignore the timeout since we already must "prime the pump" by calling hasNext. You really should use a timeout iterator underneath the exception iterator.
  }
}

/**
 * This class encapsulates the different parameters which can be used to configure selective retry.
 * It also provides a hook onTimeout which can be overridden for tracking/logging purposes.
 * @param timeoutForRetry This value specifies the retry timeout in milliseconds (amount of time post retry to wait)
 * @param thresholdNodeFailures This value specifies the threshold of number of failed nodes which will be tolerated
 * @param nextRetryStrategy This value specifies if there is another retry which we want to specify should prev fail
 * @param initialTimeout This value is used to determine the very first timeout before the first retry kicks in
 */
class RetryStrategy(var timeoutForRetry: Long, var thresholdNodeFailures: Int, var initialTimeout:Long) extends Logging{
  var nextRetryStrategy: Option[RetryStrategy] = None

  def this(timeoutForRetry: Long, thresholdNodeFailures: Int, nextRetryStrategyArg: Option[RetryStrategy], initialTimeout: Long=5000L) {
    this(timeoutForRetry, thresholdNodeFailures, initialTimeout)
    nextRetryStrategy = nextRetryStrategyArg
  }
  /**
   * This method is a callback which we register with the iterator and is invoked on timeout
   * @param numNodeFailures total number of nodes which have failed thus far
   * @return Setup a future retry in case this retry fails
   */
  def onTimeout(numNodeFailures: Int):Tuple2[Option[RetryStrategy],Boolean] = {
    if (numNodeFailures <= thresholdNodeFailures) {
      log.info("RetryStrategy: retry kicked in for %d failures".format(numNodeFailures))
      return Tuple2(nextRetryStrategy,true)
    }
    log.info("RetryStrategy: too many failures %d more than retry threshold".format(numNodeFailures))
    return Tuple2(None,false)
  }
}

/**
 * The selective retry iterator essentially retries sub-requests which are part of larger request on timeout.
 * @param numRequests The number of different norbert servers this request has been sent to initially
 * @param timeoutForRetry The threshold of time for this retry attempt for which we are willing to wait
 * @param sendRequestFunctor This is an opaque function used to send the requests out during retry
 * @param setRequests This is a mapping from partition to node to track failed nodes and partitions to be retried.
 * @param queue This is response queue in which we store tuples (node, request, response)
 * @param calculateNodesFromIds This is an opaque function which calculates node -> partition mappings with exclusion list
 * @param requestBuilder This is a functor as well
 * @param is Serializer
 * @param os Serializer
 * @param retryStrategy This is the strategy to apply when we run into timeout situation.
 * @param duplicatesOk Whether or not we can have duplicates returned to higher application layer.
 * @tparam PartitionedId This is a type representing the partition id
 * @tparam RequestMsg This is a type representing the request message type
 * @tparam ResponseMsg This is a type representing the response message type
 */
class SelectiveRetryIterator[PartitionedId, RequestMsg, ResponseMsg](
                              numRequests: Int, var timeoutForRetry: Long = 5000L,
                              sendRequestFunctor: (PartitionedRequest[PartitionedId, RequestMsg, ResponseMsg] => Unit),
                              var setRequests: Map[PartitionedId, Node],
                              queue: ResponseQueue[Tuple3[Node, Set[PartitionedId], ResponseMsg]],
                              calculateNodesFromIds : ((Set[PartitionedId], Set[Node], Int) => scala.collection.immutable.Map[Node,Set[PartitionedId]]),
                              requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                              is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg],
                              var retryStrategy: Option[RetryStrategy], var duplicatesOk: Boolean = false)
                                extends ResponseIterator[ResponseMsg] with ResponseHelper{
  /**
   * Set of nodes which have failed in sending a response back in time for this larger request
   */
  var failedNodes : Set[Node] = Set.empty[Node]
  /**
   * The time we started a pass which is either the first attempt or post a retry
   */
  var timeStartedPass : Long = System.currentTimeMillis()
  /**
   * The number of machines/nodes which have not responded so far
   */
  var distinctResponsesLeft: Int = numRequests

  var retryMessage: String = ""

  /**
   * This function determines whether an entry received from the queue
   * can be returned back to the invoking higher layer
   * @param node norbert server machine
   * @param pIds set of pids that we got a response for from the node
   * @return true - this response should be returned to higher layer, false - if this response should not be returned
   */
  def isValidQueueEntry(node: Node, pIds: Set[PartitionedId]) : Boolean = {
    if (!duplicatesOk) {
      if (!failedNodes.contains(node))
        return true
    } else {
      val iter: Iterator[PartitionedId] = pIds.iterator
      while(iter.hasNext) {
        val partitionedId = iter.next
        if (setRequests.contains(partitionedId))
          return true
      }
    }
    return false
  }

  /**
   * @return true if a response is available without blocking, false otherwise
   */
  def nextAvailable:Boolean = {
    //go through the queue discarding the ones which are dups or in failed nodes
    var entry: Tuple3[Node, Set[PartitionedId], ResponseMsg] = null
    while(true) {
      queue.poll(0, TimeUnit.MILLISECONDS) match {
       case null => return false
       case Left(e) => throw e
       case Right(f) => entry = f
       case _ => return false
      }
      if (isValidQueueEntry(entry._1, entry._2))
        return true
    }
    false
  }

  /**
   *
   * @param timeout how long to wait before giving up, in terms of <code>unit</code>
   * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
   *
   * @return a response
   */
  def next(timeout: Long, unit: TimeUnit):ResponseMsg = {
    val timeEnded = System.currentTimeMillis + timeout
    while(true) {
      queue.poll(timeEnded - System.currentTimeMillis, unit) match {
        case null =>
          throw new TimeoutException("Timed out waiting for response")

        case f => {
          var e:Tuple3[Node, Set[PartitionedId], ResponseMsg] = null
          f match {
		            case Left(g) => throw g
                case Right(h) => e = h
                case _ => return null.asInstanceOf[ResponseMsg]
          }
          if (isValidQueueEntry(e._1, e._2)) {
            return e._3
          }
        }
      }
    }
    return null.asInstanceOf[ResponseMsg]
  }

  /**
   * This method will block upto a time threshold for getting the next response.
   * If a retry strategy has been setup we will at that point invoke a retry and continue waiting for responses.
   * If we call next after reaching the end behavior is undefined
   * @return a response
   */
  def next(): ResponseMsg = {
    var timeoutCutoff: Long = timeStartedPass + timeoutForRetry
    var conditionsRetryMet = true
    while(true) {
      queue.poll(timeoutCutoff - System.currentTimeMillis(), TimeUnit.MILLISECONDS) match {
        case null => {
          var ids:scala.collection.immutable.Set[PartitionedId] = null
          var failedNodesPrevPass: Set[Node] = failedNodes
          //if we have a retry strategy in place set new values
          retryStrategy match {
            case Some(e:RetryStrategy) => {

              //iterate through the set to figure out the failed nodes for this pass as well
              ids = setRequests.keySet.toSet
              val remainingRequestsIterator : Iterator[Tuple2[PartitionedId, Node]] = setRequests.iterator

              while(remainingRequestsIterator.hasNext) {
                val tuple : Tuple2[PartitionedId, Node] = remainingRequestsIterator.next
                failedNodes += tuple._2
              }

              //check if we meet the requirements for retry to occur or not
              var tuple:Tuple2[Option[RetryStrategy],Boolean] = e.onTimeout(failedNodes.size)
              retryStrategy = tuple._1
              conditionsRetryMet = tuple._2
              timeoutForRetry = e.timeoutForRetry

              //time started pass should be relative the start time
              timeStartedPass = timeoutCutoff
              timeoutCutoff = timeStartedPass + timeoutForRetry
            }
            case None => {
              if (!duplicatesOk)
                throw new TimeoutException("Timedout waiting for final %d nodes, retryInfo:%s ".format(distinctResponsesLeft, retryMessage))
              else
                throw new TimeoutException("Timedout waiting for final %d partitions to return, retryInfo:%s ".format(setRequests.size, retryMessage))
            }
          }

          if (conditionsRetryMet) {
            retryMessage = "Retry initiated at %d".format(System.currentTimeMillis)
            //If for a particular partition id only if 10/10 of the replicas are in trouble then quit
            val nodes = calculateNodesFromIds(ids, failedNodes, 10)

            if (duplicatesOk != true) {
              //only the responses from these new requests count
              log.debug("Adjust responseIterator to: %d".format(nodes.keySet.size))
              distinctResponsesLeft=nodes.keySet.size
              //reset the outstanding requests map
              setRequests = Map.empty[PartitionedId, Node]
            }

            nodes.foreach {
              case (node, idsForNode) => {
                def callback(a:Either[Throwable, ResponseMsg]):Unit = {
                  a match {
                    case Left(t) => queue += Left(t)
                    case Right(r) => queue += Right(Tuple3(node, idsForNode, r))
                  }
                }

                val request1 = PartitionedRequest(requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os, Some((a: Either[Throwable, ResponseMsg]) => {callback(a)}), 0, Some(this))
               idsForNode.foreach {
                 case id => setRequests = setRequests + (id -> node)
               }
               sendRequestFunctor(request1)
              }
            }
          } else {
            failedNodes = failedNodesPrevPass
          }
        }

        case e : Either[Throwable,Tuple3[Node, Set[PartitionedId], ResponseMsg]] => {
          //check if we received an exception or response
          e match {
            case Right(response) => {
              if (isValidQueueEntry(response._1, response._2)) {
                distinctResponsesLeft=distinctResponsesLeft - 1
                response._2.foreach {
                  partitionId => setRequests -= partitionId
                }
                  return response._3
                }
              }
              case Left(exception) => {
                throw exception
              }
            }
        }
          case _ => null.asInstanceOf[ResponseMsg]
      }
    }
     null.asInstanceOf[ResponseMsg]
  }

  def hasNext = {
    val returnVal = {
      if (!duplicatesOk)
        distinctResponsesLeft != 0
      else {
        setRequests.isEmpty != true
      }
    }
    if (!returnVal)
      log.debug("Completed processing the scatter gather: retryInfo:%s".format(retryMessage))
    returnVal
  }
}

private[common] trait ResponseHelper extends Logging {
  protected def translateResponse[T](response: Either[Throwable, T]) = {
    val r = if (response == null) Left(new NullPointerException("Null response found"))
            else response

    r.fold(ex => throw new ExecutionException(ex) , msg => msg)
 }
}
