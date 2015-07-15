package com.linkedin.norbert.network.netty

import com.linkedin.norbert.logging.Logging

/**
 * Created by sishah on 7/2/15.
 */
class GC(debugFunction: Option[()=>String]) extends Runnable with Logging {

  def this() = this(None)

  override def run(): Unit = {

    if(debugFunction.isDefined) {
      log.debug(System.currentTimeMillis().toString + debugFunction.get())
    }

    System.gc()

  }
}
