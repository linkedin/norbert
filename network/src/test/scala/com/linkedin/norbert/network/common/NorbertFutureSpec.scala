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

import java.util.Random
import java.util.concurrent._

import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

class CurrentThreadExecutor extends Executor {
    def execute(r:Runnable) = {
        r.run();
    }
}

class NorbertFutureSpec extends SpecificationWithJUnit with Mockito with SampleMessage {
  case class ResponseExceptionWrapper(exception:ExecutionException, ping: Ping, isException:Boolean)
  class Task(queue: LinkedBlockingQueue[ResponseExceptionWrapper]) extends PromiseListener[Ping] {
    override def onCompleted(response: Ping):Unit = {
      queue.offer(ResponseExceptionWrapper(null, response, false))
    }
    override def onThrowable(t: Throwable):Unit = {
      queue.offer(ResponseExceptionWrapper(new ExecutionException(t), null, true))
    }
  }

  trait NorbertFutureSetup extends Scope {
    //val future = new FutureAdapter[Ping]
    val future = new FutureAdapterListener[Ping]
    val queue = new LinkedBlockingQueue[ResponseExceptionWrapper]()
    future.addListener(new Task(queue))
  }

  "NorbertFuture" should {
    "not be done when created" in new NorbertFutureSetup {
      future.isDone must beFalse
    }

    "be done when value is set" in new NorbertFutureSetup {
      future.apply(Right(new Ping))
      future.isDone must beTrue
      queue.size mustEqual (1)
    }

    "be done when value is set" in {
      val future = new FutureAdapterListener[Ping]
      val queue = new LinkedBlockingQueue[ResponseExceptionWrapper]()

      val msg = new Ping
      future.apply(Right(msg))
      future.isDone must beTrue
      queue.size mustEqual (0)
      future.addListener(new Task(queue))
      queue.size mustEqual (1)
      queue.poll mustEqual ResponseExceptionWrapper(null, msg, false)
    }

    "return the value that is set" in new NorbertFutureSetup {
      val message = new Ping
      future.apply(Right(request))
      future.get must be(request)
      future.get(1, TimeUnit.MILLISECONDS) must be(request)
    }

    "throw a TimeoutException if no response is available" in new NorbertFutureSetup {
      future.get(1, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
    }

    "throw an ExecutionExcetion for an error" in new NorbertFutureSetup {
      val ex = new Exception
      future.apply(Left(ex))
      future.get must throwA[ExecutionException]
      future.get(1, TimeUnit.MILLISECONDS) must throwA[ExecutionException]
      queue.size mustEqual (1)
      val response: ResponseExceptionWrapper = queue.poll
      response.isException mustEqual true
    }

    //FutureAdapterListener make sure if we come from norbert thread callback we get the response
    "norbert thread should be able to return the correct response" in {
      val future = new FutureAdapterListener[Ping]
      val queue = new LinkedBlockingQueue[ResponseExceptionWrapper]()

      future.addListener(new Task(queue))
      queue.size mustEqual (0)
      val msg = new Ping
      future.apply(Right(msg))
      future.isDone must beTrue
      queue.size mustEqual (1)
      queue.poll mustEqual ResponseExceptionWrapper(null, msg, false)
    }

    "concurrent norbert thread and calling thread should be able to handle the response" in new NorbertFutureSetup {
      //multiple runs with different thread execution orders hopefully
      for (i <- 0 to 15) {
        val future = new FutureAdapterListener[Ping]
        val queue = new LinkedBlockingQueue[ResponseExceptionWrapper]()
        val msg = new Ping
        queue.size mustEqual (0)

        val randomGen = new Random(12345)
        val thread1 = new Thread(new Runnable{
          def run {
            val timeToSleep = 20 + randomGen.nextInt() % 20
            Thread.sleep(timeToSleep)
            val task = new Task(queue)
            future.addListener(task)
          }
        })
        thread1.start()
        val thread2 = new Thread(new Runnable {
          def run {
            val timeToSleep = 20 + randomGen.nextInt() % 20
            Thread.sleep(timeToSleep)
            future.apply(Right(msg))
          }
        })
        thread2.start()

        //just to make sure the number of active threads is not very high
        thread1.join()
        thread2.join()

        future.isDone must beTrue
        queue.size() mustEqual 1
        queue.poll mustEqual ResponseExceptionWrapper(null, msg, false)
      }
    }
  }
}
