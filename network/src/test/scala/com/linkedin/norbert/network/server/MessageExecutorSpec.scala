/*
 * Copyright 2009-2015 LinkedIn, Inc
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

import com.linkedin.norbert.network.common.SampleMessage
import com.linkedin.norbert.util.WaitFor
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

import scala.collection.mutable.MutableList

class MessageExecutorSpec extends SpecificationWithJUnit with Mockito with WaitFor with SampleMessage {

  trait MessageExecutorSetup extends Scope {
    val messageHandlerRegistry = mock[MessageHandlerRegistry]
    val filter1 = mock[Filter]
    val filter2 = mock[Filter]
    val requestContext = mock[RequestContext]
    val exception = mock[Exception]
    val filters = new MutableList[Filter]
    filters ++= (List(filter1, filter2))

    val messageExecutor = new ThreadPoolMessageExecutor(None, "service",
      messageHandlerRegistry,
      filters,
      1000L,
      1,
      1,
      1,
      100,
      1000L,
      -1)

    var handlerCalled = false
    var either: Either[Exception, Ping] = null

    val unregisteredSerializer = {
      val s = mock[Serializer[Ping, Ping]]
      s.requestName returns ("Foo")
      s
    }

    def handler(e: Either[Exception, Ping]) {
      handlerCalled = true
      either = e
    }

    def generalExecutorTests = {

      "find the sync handler associated with the specified message" in {
        syncEntry.handler returns returnHandler _
        messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

        messageExecutor.executeMessage(request, requestName, Some((either: Either[Exception, Ping]) => null: Unit), None)

        waitFor(50.ms)
        there was one(messageHandlerRegistry).getHandler[Ping, Ping](requestName)
      }

      "find the async handler associated with the specified message" in {
        asyncEntry.handler returns asyncReturnHandler _
        messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns asyncEntry

        messageExecutor.executeMessage(request, requestName, Some((either: Either[Exception, Ping]) => null: Unit), None)

        waitFor(50.ms)
        there was one(messageHandlerRegistry).getHandler[Ping, Ping](requestName)
      }
    }

    def returnHandler(message: Ping): Ping = message

    def throwsHandler(message: Ping): Ping = throw exception

    def nullHandler(message: Ping): Ping = null

    def asyncReturnHandler(message: Ping, callback: CallbackContext[Ping]): Unit = callback.onResponse(message)

    def asyncThrowsHandler(message: Ping, callback: CallbackContext[Ping]): Unit = callback.onError(exception)

    val syncEntry = mock[SyncHandlerEntry[Ping, Ping]]
    val asyncEntry = mock[AsyncHandlerEntry[Ping, Ping]]

    val requestName: String = Ping.PingSerializer.requestName

    def filterSpecificTests = {

      "filters are executed when sync handler returns a valid message" in {
        syncEntry.handler returns returnHandler _
        messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

        messageExecutor.executeMessage(request, requestName, Some((either: Either[Exception, Ping]) => null: Unit), Some(requestContext))

        waitFor(5.ms)

        there was one(filter1).onRequest(request, requestContext) andThen one(filter2).onRequest(request, requestContext)
        there was one(filter2).onResponse(request, requestContext) andThen one(filter1).onResponse(request, requestContext)
      }
    }
  }

  trait AfterSpec extends MessageExecutorSetup {
    def after = {
      messageExecutor.shutdown
    }
  }

  "MessageExecutor" should {
    "satisfy general requirements in" in new MessageExecutorSetup {
      generalExecutorTests
    }

    "satisfy filter-specific requirements in" in new AfterSpec {
      filterSpecificTests
    }
  }


  "execute the sync handler associated with the specified message" in new MessageExecutorSetup {
    var wasCalled = false

    def h(message: Ping): Ping = {
      wasCalled = true
      message
    }

    syncEntry.handler returns h _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

    messageExecutor.executeMessage(request, requestName, Some((either: Either[Exception, Ping]) => null: Unit), None)

    wasCalled must eventually(beTrue)
  }

  "execute the async handler associated with the specified message" in new MessageExecutorSetup {
    var wasCalled = false

    def h(message: Ping, callbackContext: CallbackContext[Ping]): Unit = {
      wasCalled = true
    }

    asyncEntry.handler returns h _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns asyncEntry

    messageExecutor.executeMessage(request, requestName, Some((either: Either[Exception, Ping]) => null: Unit), None)

    wasCalled must eventually(beTrue)
  }

  "execute the sync responseHandler with Right(message) if the handler returns a valid message" in new MessageExecutorSetup {
    syncEntry.handler returns returnHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), None)

    handlerCalled must eventually(beTrue)
    either.isRight must beTrue
    either.right.get must be(request)
  }

  "execute the async responseHandler with Right(message) if the handler returns a valid message" in new MessageExecutorSetup {
    asyncEntry.handler returns asyncReturnHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns asyncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), None)

    handlerCalled must eventually(beTrue)
    either.isRight must beTrue
    either.right.get must be(request)
  }

  "not execute the responseHandler if the handler returns null" in new MessageExecutorSetup {
    syncEntry.handler returns nullHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), None)

    handlerCalled must eventually(beFalse)
  }

  "execute the sync responseHandler with Left(ex) if the handler throws an exception" in new MessageExecutorSetup {
    syncEntry.handler returns throwsHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), None)

    handlerCalled must eventually(beTrue)
    either.isLeft must beTrue
  }

  "execute the async responseHandler with Left(ex) if the handler throws an exception" in new MessageExecutorSetup {
    asyncEntry.handler returns asyncThrowsHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns asyncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), None)

    handlerCalled must eventually(beTrue)
    either.isLeft must beTrue
  }

  "not execute the responseHandler if the message is not registered" in new MessageExecutorSetup {
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) throws new InvalidMessageException("")

    messageExecutor.executeMessage(request, requestName, Some(handler _), None) must throwA[InvalidMessageException]

    waitFor(5.ms)
    handlerCalled must eventually(beFalse)
  }


  "filters are executed when async handler returns a valid message" in new MessageExecutorSetup {
    asyncEntry.handler returns asyncReturnHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns asyncEntry

    messageExecutor.executeMessage(request, requestName, Some((either: Either[Exception, Ping]) => null: Unit), Some(requestContext))

    waitFor(5.ms)
    there was one(filter1).onRequest(request, requestContext) andThen one(filter2).onRequest(request, requestContext)
    there was one(filter2).onResponse(request, requestContext) andThen one(filter1).onResponse(request, requestContext)
  }

  "filters are not executed when handler return null" in new MessageExecutorSetup {
    syncEntry.handler returns nullHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), Some(requestContext))

    waitFor(5.ms)
    there was one(filter1).onRequest(request, requestContext) andThen one(filter2).onRequest(request, requestContext)
    there was no(filter2).onResponse(null, requestContext) andThen no(filter1).onResponse(null, requestContext)
  }

  "filters are executed when sync handler throws an exception" in new MessageExecutorSetup {
    syncEntry.handler returns throwsHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns syncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), Some(requestContext))

    waitFor(5.ms)
    there was one(filter1).onRequest(request, requestContext) andThen one(filter2).onRequest(request, requestContext)
    there was one(filter2).onError(exception, requestContext) andThen one(filter1).onError(exception, requestContext)
    there was no(filter1).onResponse(request, requestContext)
    there was no(filter2).onResponse(request, requestContext)
  }

  "filters are executed when async handler throws an exception" in new MessageExecutorSetup {
    asyncEntry.handler returns asyncThrowsHandler _
    messageHandlerRegistry.getHandler[Ping, Ping](requestName) returns asyncEntry

    messageExecutor.executeMessage(request, requestName, Some(handler _), Some(requestContext))

    waitFor(5.ms)
    there was one(filter1).onRequest(request, requestContext) andThen one(filter2).onRequest(request, requestContext)
    there was one(filter2).onError(exception, requestContext) andThen one(filter1).onError(exception, requestContext)
    there was no(filter1).onResponse(request, requestContext)
    there was no(filter2).onResponse(request, requestContext)
  }

  "filters are added via addFilters" in new MessageExecutorSetup {
    val filter3 = mock[Filter]
    val filter4 = mock[Filter]
    messageExecutor.addFilters(List(filter3, filter4))
    messageExecutor.filters must be_==(List(filter1, filter2, filter3, filter4))
  }
}
