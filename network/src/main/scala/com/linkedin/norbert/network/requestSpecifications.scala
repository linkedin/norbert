

package com.linkedin.norbert.network

import com.linkedin.norbert.cluster.{ClusterException, Node}

object RequestSpec {
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       requestBuilder: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): RequestSpec[RequestMsg, PartitionedId] = {
    if(message == None && requestBuilder == None) {
      throw new IllegalArgumentException("need to specify either message or requestbuilder")
    }
    //error if both message and requestBuilder are none
    new RequestSpec(message, requestBuilder)
  }
}

class RequestSpec[RequestMsg, PartitionedId](val message: Option[RequestMsg],
                                             val requestBuilder: Option[(Node, Set[PartitionedId]) => RequestMsg]) {
  /**
   * Necessary functions:
   *
   * What is Scala convention on getters?
   *
   * getter for the message/message set that properly handle requestBuilder vs actual message?
   * Should requestBuilder or message have priority when returning a message? (for now use builder as primary)
   *
   * Should these be extractors? what exactly are extractors again?
   */

}


object helloworld {
  def main(args: Array[String]) {
    try {
      var tester = RequestSpec(None);
      println("Hello, world!")
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}