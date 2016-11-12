package com.linkedin.norbert.network.server

import java.util.concurrent.Executor

trait CallbackContext[ResponseMsg] {
  def onResponse(responseMsg: ResponseMsg): Unit

  def onError(ex: Exception): Unit

  def getCallbackExecutor(): Executor
}
