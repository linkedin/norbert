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

import com.linkedin.norbert.network.common.SampleMessage
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

class MessageHandlerRegistrySpec extends SpecificationWithJUnit with Mockito with SampleMessage {

  trait MessageHandlerRegistrySetup extends Scope {
    val messageHandlerRegistry = new MessageHandlerRegistry

    var handled: Ping = _
    val handler = (ping: Ping) => {
      handled = ping
      ping
    }
  }

  "MessageHandlerRegistry" should {
    "return the handler for the specified request message" in new MessageHandlerRegistrySetup {
      messageHandlerRegistry.registerHandler(handler)

      val h = messageHandlerRegistry.getHandler[Ping, Ping](Ping.PingSerializer.requestName) match {
        case sync: SyncHandlerEntry[Ping, Ping] => sync.handler
      }

      h(request) must be_==(request)
      handled must be_==(request)
    }

    "throw an InvalidMessageException if no sync handler is registered" in new MessageHandlerRegistrySetup {
      messageHandlerRegistry.getHandler(Ping.PingSerializer.requestName) must throwA[InvalidMessageException]
    }

    "throw an InvalidMessageException if no async handler is registered" in new MessageHandlerRegistrySetup {
      messageHandlerRegistry.getHandler(Ping.PingSerializer.requestName) must throwA[InvalidMessageException]
    }

    "return true if the provided response is a valid response for the given request" in new MessageHandlerRegistrySetup {
      messageHandlerRegistry.registerHandler(handler)
    }

    "return false if the provided response is not a valid response for the given request" in new MessageHandlerRegistrySetup {
      messageHandlerRegistry.registerHandler(handler)
    }
  }
}
