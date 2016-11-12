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

trait MessageHandlerRegistryComponent {
  val messageHandlerRegistry: MessageHandlerRegistry
}

abstract class HandlerEntry[RequestMsg, ResponseMsg]
(val is: InputSerializer[RequestMsg, ResponseMsg], val os: OutputSerializer[RequestMsg, ResponseMsg]) {}

case class SyncHandlerEntry[RequestMsg, ResponseMsg]
(override val is: InputSerializer[RequestMsg, ResponseMsg], override val os: OutputSerializer[RequestMsg, ResponseMsg], handler: (RequestMsg) => ResponseMsg)
  extends HandlerEntry[RequestMsg, ResponseMsg](is, os)

case class AsyncHandlerEntry[RequestMsg, ResponseMsg]
(override val is: InputSerializer[RequestMsg, ResponseMsg], override val os: OutputSerializer[RequestMsg, ResponseMsg], handler: (RequestMsg, CallbackContext[ResponseMsg]) => Unit)
  extends HandlerEntry[RequestMsg, ResponseMsg](is, os)


class MessageHandlerRegistry {
  @volatile private var handlerEntryMap = Map.empty[String, HandlerEntry[_ <: Any, _ <: Any]]

  def registerHandler[RequestMsg, ResponseMsg](handler: RequestMsg => ResponseMsg)
                                              (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    if (handler == null) throw new NullPointerException

    handlerEntryMap += (is.requestName -> SyncHandlerEntry(is, os, handler))
  }

  def registerAsyncHandler[RequestMsg, ResponseMsg](handler: (RequestMsg, CallbackContext[ResponseMsg]) => Unit)
                                                   (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    if (handler == null) throw new NullPointerException

    handlerEntryMap += (is.requestName -> AsyncHandlerEntry(is, os, handler))
  }

  def getHandler[RequestMsg, ResponseMsg](messageName: String): HandlerEntry[RequestMsg, ResponseMsg] = {
    handlerEntryMap.getOrElse(messageName, throw buildException(messageName)).asInstanceOf[HandlerEntry[RequestMsg, ResponseMsg]]
  }

  @throws(classOf[InvalidMessageException])
  def inputSerializerFor[RequestMsg, ResponseMsg](messageName: String): InputSerializer[RequestMsg, ResponseMsg] = {
    getHandler[RequestMsg, ResponseMsg](messageName).is
  }

  @throws(classOf[InvalidMessageException])
  def outputSerializerFor[RequestMsg, ResponseMsg](messageName: String): OutputSerializer[RequestMsg, ResponseMsg] = {
    getHandler[RequestMsg, ResponseMsg](messageName).os
  }

  def buildException(messageName: String) =
    new InvalidMessageException("%s is not a registered method. Methods registered are %s"
      .format(messageName, "(" + handlerEntryMap.keys.mkString(",") + ")")
    )
}