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

import java.util.concurrent.CompletableFuture

trait MessageHandlerRegistryComponent {
  val messageHandlerRegistry: MessageHandlerRegistry
}

private case class SyncHandlerEntry[RequestMsg, ResponseMsg]
(is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg], handler: RequestMsg => ResponseMsg)

private case class AsyncHandlerEntry[RequestMsg, ResponseMsg, Response]
(is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg], onRequestHandler: RequestMsg => CompletableFuture[Response], onResponseHandler: Response => ResponseMsg)

class MessageHandlerRegistry {
  @volatile private var syncHandlerMap = Map.empty[String, SyncHandlerEntry[_ <: Any, _ <: Any]]
  @volatile private var asyncHandlerMap = Map.empty[String, AsyncHandlerEntry[_ <: Any, _ <: Any, _ <: Any]]

  def registerHandler[RequestMsg, ResponseMsg](handler: RequestMsg => ResponseMsg)
                                              (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    if (handler == null) throw new NullPointerException

    syncHandlerMap += (is.requestName -> SyncHandlerEntry(is, os, handler))
  }

  def registerAsyncHandler[RequestMsg, ResponseMsg, Response](onRequestHandler: RequestMsg => CompletableFuture[Response], onResponseHandler: Response => ResponseMsg)
                                                             (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    if (onRequestHandler == null || onResponseHandler == null) throw new NullPointerException

    asyncHandlerMap += (is.requestName -> AsyncHandlerEntry(is, os, onRequestHandler, onResponseHandler))
  }

  @throws(classOf[InvalidMessageException])
  def inputSerializerFor[RequestMsg, ResponseMsg](messageName: String): InputSerializer[RequestMsg, ResponseMsg] = {
    syncHandlerMap.getOrElse(messageName,
      asyncHandlerMap.getOrElse(messageName,
        throw buildException(messageName))).asInstanceOf[InputSerializer[RequestMsg, ResponseMsg]]
  }

  @throws(classOf[InvalidMessageException])
  def outputSerializerFor[RequestMsg, ResponseMsg](messageName: String): OutputSerializer[RequestMsg, ResponseMsg] = {
    syncHandlerMap.getOrElse(messageName,
      asyncHandlerMap.getOrElse(messageName,
        throw buildException(messageName))).asInstanceOf[OutputSerializer[RequestMsg, ResponseMsg]]
  }

  def isSyncHandler[RequestMsg, ResponseMsg](messageName: String): Boolean = syncHandlerMap.contains(messageName)

  @throws(classOf[InvalidMessageException])
  def onRequestHandlerFor[RequestMsg, ResponseMsg, Response](messageName: String): RequestMsg => CompletableFuture[Response] = {
    asyncHandlerMap.getOrElse(messageName, throw buildException(messageName)).asInstanceOf[AsyncHandlerEntry[RequestMsg, ResponseMsg, Response]].onRequestHandler
  }

  @throws(classOf[InvalidMessageException])
  def onResponseHandlerFor[RequestMsg, ResponseMsg, Response](messageName: String): Response => ResponseMsg = {
    asyncHandlerMap.getOrElse(messageName, throw buildException(messageName)).asInstanceOf[AsyncHandlerEntry[RequestMsg, ResponseMsg, Response]].onResponseHandler
  }

  @throws(classOf[InvalidMessageException])
  def handlerFor[RequestMsg, ResponseMsg](request: RequestMsg)(implicit is: InputSerializer[RequestMsg, ResponseMsg]): RequestMsg => ResponseMsg = {
    handlerFor(is.requestName)
  }

  @throws(classOf[InvalidMessageException])
  def handlerFor[RequestMsg, ResponseMsg](messageName: String): RequestMsg => ResponseMsg = {
    syncHandlerMap.getOrElse(messageName, throw buildException(messageName)).asInstanceOf[SyncHandlerEntry[RequestMsg, ResponseMsg]].handler
  }

  def buildException(messageName: String) =
    new InvalidMessageException("%s is not a registered method. Methods registered are %s".format(messageName, "(" + syncHandlerMap.keys.mkString(",") + "," + asyncHandlerMap.keys.mkString(",") + ")"))
}