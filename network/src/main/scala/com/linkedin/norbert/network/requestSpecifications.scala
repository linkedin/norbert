
package com.linkedin.norbert.network
import com.linkedin.norbert.cluster.{ClusterException, Node}

/**
 * A RequestSpec object is used to store the necessary information to specify the request message
 * For the non-partitioned version this is currently just the actual message
 * You can either create the RequestSpec directly or convert a PartitionedRequestSpec
 */


object RequestSpec {
  def apply[RequestMsg](message: RequestMsg): RequestSpec[RequestMsg] = {
    new RequestSpec(message);
  }

  def convert[RequestMsg](partitionedSpec: PartitionedRequestSpec[RequestMsg, _]): RequestSpec[RequestMsg] = {
    new RequestSpec(partitionedSpec.message.get);
  }
  
}

class RequestSpec[RequestMsg](val message: RequestMsg) {

}

/**
 * This is the partitioned version of RequestSpec. It serves the same purpose.
 * In this partitioned version the request can be specified either by giving the actual method or by specifying a requestBuilder (rb)
 * which will generate a message from a set of partitionedIds. At least one of those must be specified.
 * You can also convert a RequestSpec into a ParitionedRequestSpec
 */

object PartitionedRequestSpec{
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       rb: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): PartitionedRequestSpec[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpec(message, rb)
  }
  def convert[RequestMsg, PartitionedId](requestSpec: RequestSpec[RequestMsg]): PartitionedRequestSpec[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpec(Some(requestSpec.message), None)
  }
}

class PartitionedRequestSpec[RequestMsg, PartitionedId](val message: Option[RequestMsg],
                                             val rb: Option[(Node, Set[PartitionedId]) => RequestMsg]) {
  if (message == None && rb == None) {
    //error if both message and requestBuilder are none
    throw new IllegalArgumentException("need to specify either message or requestbuilder")
  }

}

/**
 * Below is a main which provides some basic testing, it will most likely go away in the end
 */

object testing {
  def main(args: Array[String]) {
    try {
      val tester: RequestSpec[String] = RequestSpec[String]("test")
      val partitionedTester: PartitionedRequestSpec[String, Int] = PartitionedRequestSpec.convert[String, Int](tester)
      val partitionedTester2: PartitionedRequestSpec[String, Int] = PartitionedRequestSpec[String, Int](Some("partitionedTest"))
      val tester2: RequestSpec[String] = RequestSpec.convert[String](partitionedTester)
      println(tester2.message);
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}