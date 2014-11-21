package com.linkedin.norbert

import _root_.com.linkedin.norbert.network.client.NetworkClientConfig
import _root_.com.linkedin.norbert.network.common.RetryStrategy
import com.linkedin.norbert.cluster.ClusterDisconnectedException
import com.linkedin.norbert.network._
import com.linkedin.norbert.network.client.NetworkClientConfig
import com.linkedin.norbert.network.common.RetryStrategy
import com.linkedin.norbert.network.partitioned.RoutingConfigs

object RoutingConfigs {
  val defaultRoutingConfigs = new RoutingConfigs(false, false)
  def getDefaultRoutingConfigs():RoutingConfigs = {
    defaultRoutingConfigs
  }
}

class RoutingConfigs(SelectiveRetry: Boolean, DuplicatesOk: Boolean ) {
  val selectiveRetry = SelectiveRetry
  val duplicatesOk = DuplicatesOk
}

//can have maxRetry without callback, but you cannot have callback without maxRetry
object RetryAndCallbackSpecs {
  def apply[ResponseMsg](maxRetry: Int = 0,
            callback: Option[Either[Throwable, ResponseMsg] => Unit] = temp,
            routingConfigs: RoutingConfigs = new RoutingConfigs(retryStrategy != None, duplicatesOk),
            retryStrategy: Option[RetryStrategy] = retryStrategy) = {
    new RetryAndCallbackSpecs[ResponseMsg](maxRetry, callback, routingConfigs, retryStrategy)
  }

  var duplicatesOk:Boolean = false
  var retryStrategy:Option[RetryStrategy] = None
  def setConfig(config:NetworkClientConfig): Unit = {
    duplicatesOk = config.duplicatesOk
    if(retryStrategy != null)
      retryStrategy = config.retryStrategy
  }

  // currently the function that callback is defaulted to; need to change this to be
  // defaulted to the function that is commented out below
  private def temp: Unit = {

  }

//  //callback defaults to this (in networkclient):
//  private[client] def retryCallback[RequestMsg, ResponseMsg](underlying: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])(res: Either[Throwable, ResponseMsg])
//                                                            (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
//    def propagate(t: Throwable) { underlying(Left(t)) }
//    def handleFailure(t: Throwable) {
//      t match {
//        case ra: RequestAccess[Request[RequestMsg, ResponseMsg]] =>
//          log.info("Caught exception(%s) for %s".format(t, ra.request))
//          val request = ra.request
//          if (request.retryAttempt < maxRetry) {
//            try {
//              val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb.nextNode(capability, persistentCapability).getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request.message))))
//              if (!node.equals(request.node)) { // simple check; partitioned version does retry here as well
//              val request1 = Request(request.message, node, is, os, Some(retryCallback[RequestMsg, ResponseMsg](underlying, maxRetry, capability, persistentCapability) _), request.retryAttempt + 1)
//                log.debug("Resend %s".format(request1))
//                doSendRequest(request1)
//              } else propagate(t)
//            } catch {
//              case t1: Throwable => propagate(t)  // propagate original ex (t) for now; may capture/chain t1 if useful
//            }
//          } else propagate(t)
//        case _ => propagate(t)
//      }
//    }
//    if (underlying == null)
//      throw new NullPointerException
//    if (maxRetry <= 0)
//      res.fold(t => handleFailure(t), result => underlying(Right(result)))
//    else
//      res.fold(t => handleFailure(t), result => underlying(Right(result)))
//  }
//
//}

class RetryAndCallbackSpecs[ResponseMsg](val maxRetry: Int,
                            val callback: Option[Either[Throwable, ResponseMsg] => Unit],
                            val routingConfigs: RoutingConfigs,
                            val retryStrategy: Option[RetryStrategy]) {

  //Validation checks go here:
  if (maxRetry == 0 && callback != None) {
    throw new IllegalArgumentException("maxRetry must be greater than 0 for callback options to work")
  }

}

object Testing {
  def main(args: Array[String]): Unit = {
    try {
      var test = RetryAndCallbackSpecs();
      //var partitionedTester = ;
      println("no error");
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}
