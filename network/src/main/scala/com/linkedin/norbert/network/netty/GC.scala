package com.linkedin.norbert.network.netty

/**
 * Created by sishah on 7/2/15.
 */
class GC extends Runnable{
  override def run(): Unit = {

    System.gc()

  }
}
