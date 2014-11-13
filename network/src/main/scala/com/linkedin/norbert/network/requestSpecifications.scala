
package com.linkedin.norbert.network
import com.linkedin.norbert.cluster.{ClusterException, Node}

/**
 * Insert proper comments for RequestSpec here
 */


object RequestSpec {
  def apply[RequestMsg](message: RequestMsg): RequestSpec[RequestMsg] = {
    new RequestSpec(message);
  }

  def apply[RequestMsg](partitionedSpec: PartitionedRequestSpec[RequestMsg, _]): RequestSpec[RequestMsg] = {
    new RequestSpec(partitionedSpec.message.get);
  }
}

class RequestSpec[RequestMsg](val message: RequestMsg) {

}

/**
 * Insert proper comments for PartitionedRequestSpec here
 */

object PartitionedRequestSpec{
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       rb: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): PartitionedRequestSpec[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpec(message, rb)
  }
  def apply[RequestMsg, PartitionedId](requestSpec: RequestSpec[RequestMsg]): PartitionedRequestSpec[RequestMsg, PartitionedId] = {
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
 * Put comments on testing
 */

object testing {
  def main(args: Array[String]) {
    try {
      val tester: RequestSpec[String] = RequestSpec[String]("test")
      val partitionedTester: PartitionedRequestSpec[String, Int] = PartitionedRequestSpec[String, Int](tester)
      val partitionedTester2: PartitionedRequestSpec[String, Int] = PartitionedRequestSpec[String, Int](Some("partitionedTest"))
      val tester2: RequestSpec[String] = RequestSpec[String](partitionedTester)
      println(tester2.message);
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}