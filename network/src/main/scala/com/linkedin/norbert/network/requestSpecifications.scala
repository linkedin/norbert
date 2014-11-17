
package com.linkedin.norbert.network
import com.linkedin.norbert.cluster.{ClusterException, Node}

/**
 * A RequestSpecification object is used to store the necessary information to specify the request message
 * For the non-partitioned version this is currently just the actual message
 * You can either create the RequestSpec directly or convert a PartitionedRequestSpec
 */


object RequestSpecification {
  def apply[RequestMsg](message: RequestMsg): RequestSpecification[RequestMsg] = {
    new RequestSpecification(message);
  }

  def convert[RequestMsg](partitionedSpec: PartitionedRequestSpecification[RequestMsg, _]): RequestSpecification[RequestMsg] = {
    new RequestSpecification(partitionedSpec.message.get);
  }

}

class RequestSpecification[RequestMsg](val message: RequestMsg) {

}

/**
 * This is the partitioned version of RequestSpec. It serves the same purpose.
 * In this partitioned version the request can be specified either by giving the actual method or by specifying a requestBuilder (rb)
 * which will generate a message from a set of partitionedIds. At least one of those must be specified.
 * You can also convert a RequestSpec into a ParitionedRequestSpec
 */

object PartitionedRequestSpecification{
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       rb: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): PartitionedRequestSpecification[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpecification(message, rb)
  }
  def convert[RequestMsg, PartitionedId](requestSpec: RequestSpecification[RequestMsg]): PartitionedRequestSpecification[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpecification(Some(requestSpec.message), None)
  }
}

class PartitionedRequestSpecification[RequestMsg, PartitionedId](val message: Option[RequestMsg],
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
      val tester: RequestSpecification[String] = RequestSpecification[String]("test")
      val partitionedTester: PartitionedRequestSpecification[String, Int] = PartitionedRequestSpecification.convert[String, Int](tester)
      val partitionedTester2: PartitionedRequestSpecification[String, Int] = PartitionedRequestSpecification[String, Int](Some("partitionedTest"))
      val tester2: RequestSpecification[String] = RequestSpecification.convert[String](partitionedTester)
      println(tester2.message);
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}