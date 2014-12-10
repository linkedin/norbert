
package com.linkedin.norbert.network
import com.linkedin.norbert.cluster.{ClusterException, Node}

/**
 * A RequestSpecification object is used to store the necessary information to specify the request message.
 * For the non-partitioned version this is just the actual message being sent.
 * There is a conversion from a PartitionedRequestSpec, but it will error if the PartitionedRequestSpec does not have a RequestMsg (which is possible).
 */


object RequestSpecification {
  def apply[RequestMsg](message: RequestMsg): RequestSpecification[RequestMsg] = {
    new RequestSpecification(message);
  }

  implicit def convert[RequestMsg](partitionedSpec: PartitionedRequestSpecification[RequestMsg, _]): RequestSpecification[RequestMsg] = {
    new RequestSpecification(partitionedSpec.message.get);
  }

}

/**
 * This is a RequestSpecification, it has a default constructor and no extra functionality. See the above companion object for more details.
 * @param message The requestMsg to be sent to the node.
 * @tparam RequestMsg The type of the request being sent to the node, should be the same as that used by the network client you will use to send the request.
 */
class RequestSpecification[RequestMsg](val message: RequestMsg) {

}

/**
 * This is the partitioned version of RequestSpec. It serves the same purpose of storing the information regarding the message being sent.
 * In this partitioned version the request can be specified either by giving the actual RequestMsg to be sent or by providing a requestBuilder (rb).
 * A RequestBuilder is a function which, given a node and a set of PartitionedIds will return a RequestMsg.
 * which will generate a message from a set of partitionedIds. At least one of those must be specified.
 * You can also convert a RequestSpec into a PartitionedRequestSpec, which will set the PartitionedRequestSpec's message to that of the RequestSpec and not specify a requestBuilder.
 */
object PartitionedRequestSpecification{
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       rb: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): PartitionedRequestSpecification[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpecification(message, rb)
  }
  implicit def convert[RequestMsg, PartitionedId](requestSpec: RequestSpecification[RequestMsg]): PartitionedRequestSpecification[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpecification(Some(requestSpec.message), None)
  }
}

/**
 * See above for more information on the capabilities of a PartitionedRequestSpecification. It currently can only be constructed
 * @param message
 * @param rb
 * @tparam RequestMsg
 * @tparam PartitionedId
 */
class PartitionedRequestSpecification[RequestMsg, PartitionedId](val message: Option[RequestMsg],
                                             val rb: Option[(Node, Set[PartitionedId]) => RequestMsg]) {
  if (message == None && rb == None) {
    //error if both message and requestBuilder are none
    throw new IllegalArgumentException("need to specify either message or requestbuilder")
  }

}

/**
 * Below is a main which provides some basic testing for RequestSpecification and PartitionedRequestSpecification.
 */

object testing {
  def main(args: Array[String]) {
    try {
      val tester: RequestSpecification[String] = RequestSpecification[String]("test")
      val partitionedTester: PartitionedRequestSpecification[String, Int] = tester
      val partitionedTester2: PartitionedRequestSpecification[String, Int] = PartitionedRequestSpecification[String, Int](Some("partitionedTest"))
      val tester2: RequestSpecification[String] = partitionedTester
      println(tester2.message);
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}
