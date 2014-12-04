

package com.linkedin.norbert.network
import com.linkedin.norbert.cluster.{ClusterException, Node}

object RequestSpec {
  def apply[RequestMsg](message: RequestMsg): RequestSpec[RequestMsg] = {
    new RequestSpec(message);
  }
}

class RequestSpec[RequestMsg](val message: RequestMsg) {

}



object PartitionedRequestSpec{
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       rb: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): PartitionedRequestSpec[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpec(message, rb)
  }
}

class PartitionedRequestSpec[RequestMsg, PartitionedId](val message: Option[RequestMsg],
                                             val rb: Option[(Node, Set[PartitionedId]) => RequestMsg]) {
  if (message == None && rb == None) {
    //error if both message and requestBuilder are none
    throw new IllegalArgumentException("need to specify either message or requestbuilder")
  }

}


object testing {
  def main(args: Array[String]) {
    try {
      var tester = RequestSpec[String]("test");
      var partitionedTester = PartitionedRequestSpec[String, Int]();
      println("no error");
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}