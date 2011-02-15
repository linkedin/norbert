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

import server.MessageExecutorComponent
import cluster.{Node, ClusterClientComponent}

trait LocalMessageExecution extends BaseNetworkClient {
  this: MessageExecutorComponent with ClusterClientComponent with ClusterIoClientComponent =>

  val myNode: Node

  override protected def doSendRequest[RequestMsg, ResponseMsg](node: Node, request: RequestMsg, callback: (Either[Throwable, ResponseMsg]) => Unit)
                                                               (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) = {
    if(node == myNode) messageExecutor.executeMessage(request, callback)
    else super.doSendRequest(node, request, callback)
  }
}
