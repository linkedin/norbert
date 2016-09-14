package com.linkedin.norbert
package network
package common

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import java.util
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network._

class NorbertResponseIteratorSpec extends SpecificationWithJUnit with Mockito with SampleMessage {
  val responseQueue = new ResponseQueue[Ping]
  val it = new NorbertResponseIterator[Ping](2, responseQueue)

  class DummyInputSerializer[X, Y] extends InputSerializer[X, Y]  {
    def requestName: String = {"dummy"}
    def requestFromBytes(bytes: Array[Byte]): X = {return null.asInstanceOf[X]}
    def responseFromBytes(bytes: Array[Byte]): Y = {return null.asInstanceOf[Y]}
  }

  class DummyOutputSerializer[X, Y] extends OutputSerializer[X, Y]  {
    def responseName: String = {"dummy"}
    def requestToBytes(request: X): Array[Byte] = {return Array.empty[Byte]}
    def responseToBytes(response: Y): Array[Byte] = {return Array.empty[Byte]}
  }

  "NorbertResponseIterator" should {
//    "return true for next until all responses have been consumed" in {
//      it.hasNext must beTrue
//
//      responseQueue += (Right(new Ping))
//      responseQueue += (Right(new Ping))
//      it.next must notBeNull
//      it.hasNext must beTrue
//
//      it.next must notBeNull
//      it.hasNext must beFalse
//    }
//
//    "return true for nextAvailable if any responses are available" in {
//      it.nextAvailable must beFalse
//      responseQueue += (Right(new Ping))
//      it.nextAvailable must beTrue
//    }
//
//    "throw a TimeoutException if no response is available" in {
//      it.next(1, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
//    }
//
//    "throw an ExecutionException for an error" in {
//      val ex = new Exception
//      responseQueue += (Left(ex))
//      it.next must throwA[ExecutionException]
//    }

    "Handle exceptions at the end of the stream" in {
      val responseQueue = new ResponseQueue[Int]
      responseQueue += Right(0)
      responseQueue += Right(1)

      val norbertResponseIterator = new NorbertResponseIterator[Int](3, responseQueue)
      val timeoutIterator = new TimeoutIterator(norbertResponseIterator, 40L)
      val exceptionIterator = new ExceptionIterator(timeoutIterator)

      val partialIterator = new PartialIterator(exceptionIterator)

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 0

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 1

      partialIterator.hasNext mustBe false
    }

    "Handle exceptions in the middle of the stream" in {
      val responseQueue = new ResponseQueue[Int]
      responseQueue += Right(0)
      responseQueue += Left(new TimeoutException)
      responseQueue += Right(1)

      val norbertResponseIterator = new NorbertResponseIterator[Int](3, responseQueue)
      val timeoutIterator = new TimeoutIterator(norbertResponseIterator, 100L)
      val exceptionIterator = new ExceptionIterator(timeoutIterator)

      val partialIterator = new PartialIterator(exceptionIterator)

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 0

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 1

      partialIterator.hasNext mustBe false
    }

    //duplicates not ok
    "if not timeout occurs behavior is unchanged in case initial timeout occurs but not the latter timeout" in {
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int) = Map.empty[Node, Set[Int]]
      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      val node1 = Node(1, "node1", true)
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = queue += Right(Tuple3(null.asInstanceOf[Node], Set.empty[Int] + 1, 2000))
      val insertQueueLate1 = new Thread(new Runnable {
        def run() {Thread.sleep(100); queue += Right(Tuple3(node1, Set.empty[Int] + 1, 1000)) }
      })
      insertQueueLate1.start()

      val iterator =  new SelectiveRetryIterator[Int, Int, Int](1, 50L,
                                          callback, Map.empty[Int, Node] + (1 -> node1),
                                          queue,
					                                calculateNodeFunctor,
					                                requestBuilderFunctor,
                                          new DummyInputSerializer[Int, Int],
                                          new DummyOutputSerializer[Int, Int],
                                          Some(new RetryStrategy(200L, 0, None, 10L)))
      iterator.hasNext mustBe true
      iterator.next mustEqual 1000
      iterator.failedNodes mustEqual Set.empty[Node]

      iterator.hasNext mustBe false
    }

    //duplicates ok
    "if not timeout occurs behavior is unchanged in case initial timeout occurs but not the latter timeout" in {
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int) = Map.empty[Node, Set[Int]]
      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      val node1 = Node(1, "node1", true)
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = queue += Right(Tuple3(null.asInstanceOf[Node], Set.empty[Int] + 1, 2000))
      val insertQueueLate1 = new Thread(new Runnable {
        def run() {Thread.sleep(100); queue += Right(Tuple3(node1, Set.empty[Int] + 1, 1000)) }
      })
      insertQueueLate1.start()

      val iterator =  new SelectiveRetryIterator[Int, Int, Int](1, 50L,
                                          callback, Map.empty[Int, Node] + (1 -> node1),
                                          queue,
                                                                        calculateNodeFunctor,
                                                                        requestBuilderFunctor,
                                          new DummyInputSerializer[Int, Int],
                                          new DummyOutputSerializer[Int, Int],
                                          Some(new RetryStrategy(200L, 0, None, 10L)))
      iterator.hasNext mustBe true
      iterator.next mustEqual 1000
      iterator.failedNodes mustEqual Set.empty[Node]

      iterator.hasNext mustBe false
    }

    //duplicates not ok and initial timeout occurs behavior should be unchanged if response comes in later
    //duplicates not ok
    "if not timeout occurs behavior is unchanged in case timeout occurs" in {
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int) = Map.empty[Node, Set[Int]]
      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      queue += Right(Tuple3(Node(10, "endpoint", true), Set.empty[Int] + 100, 1000))
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = queue += Right(Tuple3(null.asInstanceOf[Node], Set.empty[Int] + 1, 2000))
      val iterator =  new SelectiveRetryIterator[Int, Int, Int](1, 50L,
        callback, Map.empty[Int, Node],
        queue,
        calculateNodeFunctor,
        requestBuilderFunctor,
        new DummyInputSerializer[Int, Int],
        new DummyOutputSerializer[Int, Int],
        None)
      //can we test this independently of partitioned network client
      //insert stuff into the queue TODO
      iterator.hasNext mustBe true
      //queue.poll(0, TimeUnit.MILLISECONDS) mustEqual 1000
      iterator.next mustEqual 1000

      iterator.hasNext mustBe false
    }

    "if timeout occurs behavior is unchanged as compared to before" in {
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int) = Map.empty[Node, Set[Int]]
      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      queue += Right(Tuple3(Node(10, "endpoint", true), Set.empty[Int] + 100, 1000))
      queue += Right(Tuple3(Node(10, "endpoint", true), Set.empty[Int] + 200, 2000))
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = queue += Right(Tuple3(null.asInstanceOf[Node], Set.empty[Int] + 100, 1000))
      val iterator =  new SelectiveRetryIterator[Int, Int, Int](4, 50L,
        callback, Map.empty[Int, Node],
        queue,
        calculateNodeFunctor,
        requestBuilderFunctor,
        new DummyInputSerializer[Int, Int],
        new DummyOutputSerializer[Int, Int],
        None)

      iterator.hasNext mustBe true
      iterator.next mustEqual 1000

      iterator.hasNext mustBe true
      iterator.next mustEqual 2000

      val insertQueueTimely = new Thread(new Runnable {
        def run() {Thread.sleep(15); queue += Right(Tuple3(Node(10, "endpoint", true), Set.empty[Int] + 300, 3000)) }
      })
      insertQueueTimely.start()

      iterator.hasNext mustBe true
      iterator.next mustEqual 3000

      val insertQueueLate = new Thread(new Runnable {
        def run() {Thread.sleep(50); queue += Right(Tuple3(Node(10, "endpoint", true), Set.empty[Int] + 400, 4000)) }
      })
      insertQueueLate.start()

      var timeoutExceptionFlag = false
      try {
      iterator.hasNext mustBe true
      iterator.next
      } catch {
        case e: TimeoutException => timeoutExceptionFlag= true
      }
      timeoutExceptionFlag mustBe true
    }

    "if timeout occurs we try to find the next node multiple times" in {
      var invocationCount = 1
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int):Map[Node, Set[Int]] = {
        if (invocationCount == 1) {
          val failedNodes = Set.empty[Node] + Node(1, "node1", true) + Node(2, "node2", true)
          setNodes.size mustEqual 2
          setNodes mustEqual failedNodes
          val partitionsRetry = Set.empty[Int] + 1 + 2 + 3 + 4 + 5 + 6
          setPIds mustEqual partitionsRetry
          return Map.empty[Node, Set[Int]] + (Node(4,"node4", true) -> (Set.empty[Int] + 1 + 2)) + (Node(5,"node5",true) -> (Set.empty[Int] + 3 + 4)) + (Node(6,"node6",true) -> (Set.empty[Int] + 5 + 6))
        } else if (invocationCount == 2){
          val failedNodes = Set.empty[Node] + Node(6, "node6", true)
          setNodes.size mustEqual 1
          setNodes mustEqual failedNodes
          val partitionsRetry = Set.empty[Int] + 5 + 6
          setPIds mustEqual partitionsRetry
          return Map.empty[Node, Set[Int]] + (Node(7,"node7",true) -> (Set.empty[Int] + 5 + 6))
        }
        return Map.empty[Node, Set[Int]]
      }

      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      queue += Right(Tuple3(Node(3, "node3", true), Set.empty[Int] + 7 + 8 + 9, 1000))
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = {
        if (invocationCount == 1) {
          val insertQueueLate = new Thread(new Runnable {
            def run() {Thread.sleep(5000); queue += Right(Tuple3(Node(6, "endpoint", true), Set.empty[Int] + 5 + 6, 6000)) }
          })
          insertQueueLate.start()
          val insertQueueLate1 = new Thread(new Runnable {
            def run() {Thread.sleep(10); queue += Right(Tuple3(Node(4, "endpoint", true), Set.empty[Int] + 1 + 2, 4000)) }
          })
          insertQueueLate1.start()
          val insertQueueLate2 = new Thread(new Runnable {
            def run() {Thread.sleep(15); queue += Right(Tuple3(Node(5, "endpoint", true), Set.empty[Int] + 3 + 4, 5000)) }
          })
          insertQueueLate2.start()
          invocationCount += 1
        } else if (invocationCount == 2) {
          val insertQueueLate = new Thread(new Runnable {
            def run() {Thread.sleep(20); queue += Right(Tuple3(Node(7, "endpoint", true), Set.empty[Int] + 5 + 6, 9000)) }
          })
          insertQueueLate.start()
        }
      }
      //requests should contain some partitions to node mapping
      //if requests times out the selective retry case then we need to make
      //sure that
      val node3 = Node(3, "node3", true)
      val node2 = Node(2, "node2", true)
      val node1 = Node(1, "node1", true)
      val iterator =  new SelectiveRetryIterator[Int, Int, Int](3, 10L,
        callback, Map.empty[Int, Node] + (7 -> node3) + (8 -> node3) + (9 -> node3)
          + (1 -> node1) + (2 -> node1) + (3 -> node1)
          + (4 -> node2) + (5 -> node2) + (6 -> node2),
        queue,
        calculateNodeFunctor,
        requestBuilderFunctor,
        new DummyInputSerializer[Int, Int],
        new DummyOutputSerializer[Int, Int],
        Some(new RetryStrategy(100L,2,Some(new RetryStrategy(100L,3,None)))))

      iterator.hasNext mustBe true
      iterator.next mustEqual 1000

      iterator.hasNext mustBe true
      iterator.next mustEqual 4000
      invocationCount mustBe 2

      iterator.hasNext mustBe true
      iterator.next mustEqual 5000

      iterator.hasNext mustBe true
      iterator.next mustEqual 9000
      invocationCount mustBe 2
    }

    "in case of duplicates we should return duplicate values and stop as soon as we have complete partitions" in {
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int) = Map.empty[Node, Set[Int]]
      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      queue += Right(Tuple3(Node(1, "endpoint", true), Set.empty[Int] + 1 + 2, 1000))
      queue += Right(Tuple3(Node(2, "endpoint", true), Set.empty[Int] + 1 + 2, 2000))
      queue += Right(Tuple3(Node(3, "endpoint", true), Set.empty[Int] + 1, 3000))
      queue += Right(Tuple3(Node(4, "endpoint", true), Set.empty[Int] + 1 + 3, 4000))
      queue += Right(Tuple3(Node(5, "endpoint", true), Set.empty[Int] + 4, 5000))
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = queue += Right(Tuple3(null.asInstanceOf[Node], Set.empty[Int] + 100, 1000))
      val node6 = Node(6, "node1", true)
      val iterator =  new SelectiveRetryIterator[Int, Int, Int](4, 50L,
        callback, Map.empty[Int, Node] + (1->node6) + (2->node6) + (3->node6) + (4->node6),
        queue,
        calculateNodeFunctor,
        requestBuilderFunctor,
        new DummyInputSerializer[Int, Int],
        new DummyOutputSerializer[Int, Int],
        None, true)

      iterator.hasNext mustBe true
      iterator.next mustEqual 1000

      iterator.hasNext mustBe true
      iterator.next mustEqual 4000

      iterator.hasNext mustBe true
      iterator.next mustEqual 5000
    }

    "in case of duplicate outstanding requests the first one to complete wins" in {
      def calculateNodeFunctor(setPIds: Set[Int],setNodes: Set[Node],requestMsg: Int):Map[Node, Set[Int]] = {
        val failedNodes = Set.empty[Node] + Node(1, "node1", true) + Node(2, "node2", true)
        setNodes.size mustEqual 2
        setNodes mustEqual failedNodes
        val partitionsRetry = Set.empty[Int] + 1 + 2
        setPIds mustEqual partitionsRetry
        return Map.empty[Node, Set[Int]] + (Node(3,"node3",true) -> (Set.empty[Int] + 1)) + (Node(4,"node4",true) -> (Set.empty[Int] + 2))
      }

      def requestBuilderFunctor(node: Node, setPIds: Set[Int]):Int = 1
      val queue = new ResponseQueue[Tuple3[Node, Set[Int], Int]]
      def callback(pRequest: PartitionedRequest[Int,Int,Int]) = {
        val insertQueueLate = new Thread(new Runnable {
          def run() {Thread.sleep(10L); queue += Right(Tuple3(Node(3, "endpoint", true), Set.empty[Int] + 1, 3000)) }
        })
        insertQueueLate.start()
        val insertQueueLate1 = new Thread(new Runnable {
          def run() {Thread.sleep(100); queue += Right(Tuple3(Node(4, "endpoint", true), Set.empty[Int] + 2, 4000)) }
        })
        insertQueueLate1.start()
      }
      val node1 = Node(1, "node1", true)
      val node2 = Node(2, "node2", true)
      val iterator =  new SelectiveRetryIterator[Int, Int, Int](2, 20L,
        callback, Map.empty[Int, Node] + (1->node1) + (2->node2),
        queue,
        calculateNodeFunctor,
        requestBuilderFunctor,
        new DummyInputSerializer[Int, Int],
        new DummyOutputSerializer[Int, Int],
        Some(new RetryStrategy(200L, 2, None)), true)

      val insertQueueLate1 = new Thread(new Runnable {
        def run() {Thread.sleep(100); queue += Right(Tuple3(Node(1, "endpoint", true), Set.empty[Int] + 1, 1000)) }
      })
      insertQueueLate1.start()
      val insertQueueLate2 = new Thread(new Runnable {
        def run() {Thread.sleep(90); queue += Right(Tuple3(Node(2, "endpoint", true), Set.empty[Int] + 2, 2000)) }
      })
      insertQueueLate2.start()

      iterator.hasNext mustBe true
      iterator.next mustEqual 3000

      iterator.hasNext mustBe true
      iterator.next mustEqual 2000

      iterator.hasNext mustBe false
    }
  }
}
