
package com.linkedin.norbert.network
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.javaobjects.{RequestSpecification => JRequestSpecification, PartitionedRequestSpecification => JPartitionedRequestSpecification}

/**
 * A RequestSpecification object is used to store the necessary information to specify the request message.
 * For the non-partitioned version this is just the actual message being sent.
 * There is a conversion from a PartitionedRequestSpecification, but it will error if the PartitionedRequestSpecification does not have a RequestMsg (which is possible).
 */


object RequestSpecification {
  def apply[RequestMsg](message: RequestMsg): RequestSpecification[RequestMsg] = {
    new RequestSpecification(message)
  }

  implicit def convert[RequestMsg](partitionedSpec: PartitionedRequestSpecification[RequestMsg, _]): RequestSpecification[RequestMsg] = {
    new RequestSpecification(partitionedSpec.message.get)
  }

}

/**
 * This is a RequestSpecification, it has a default constructor and no extra functionality. See the above companion object for more details.
 * @param message The requestMsg to be sent to the node.
 * @tparam RequestMsg The type of the request being sent to the node, should be the same as that used by the network client you will use to send the request.
 */
class RequestSpecification[RequestMsg](val message: RequestMsg) extends JRequestSpecification[RequestMsg]{
  def getMessage() = message
}

/**
 * This is the partitioned version of RequestSpecification. It serves the same purpose of storing the information regarding the message being sent.
 * In this partitioned version the request can be specified either by giving the actual RequestMsg to be sent or by providing a requestBuilder.
 * A RequestBuilder is a function which, given a node and a set of PartitionedIds will return a RequestMsg.
 * which will generate a message from a set of partitionedIds. At least one of those must be specified.
 * You can also convert a RequestSpecification into a PartitionedRequestSpecification, which will set the PartitionedRequestSpecification's message to that of the RequestSpecification and not specify a requestBuilder.
 */
object PartitionedRequestSpecification{
  def apply[RequestMsg, PartitionedId](message: Option[RequestMsg] = None,
                                       requestBuilder: Option[(Node, Set[PartitionedId]) => RequestMsg] = None): PartitionedRequestSpecification[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpecification(message, requestBuilder)
  }
  implicit def convert[RequestMsg, PartitionedId](requestSpec: RequestSpecification[RequestMsg]): PartitionedRequestSpecification[RequestMsg, PartitionedId] = {
    new PartitionedRequestSpecification(Some(requestSpec.message), None)
  }
}

/**
 * See above for more information on the capabilities of a PartitionedRequestSpecification. It currently can only be constructed
 * @param message The requestMsg to be sent to the node.
 * @tparam RequestMsg The type of the request being sent to the node, should be the same as that used by the network client you will use to send the request.
 * @param requestBuilder Builds a request using the specified set of partitionedIds.
 */
class PartitionedRequestSpecification[RequestMsg, PartitionedId](val message: Option[RequestMsg],
                                             var requestBuilder: Option[(Node, Set[PartitionedId]) => RequestMsg]) extends JPartitionedRequestSpecification[RequestMsg, PartitionedId]{
  if (requestBuilder == None) {
    if (message == None) {
      
      /* error if both message and requestBuilder are none */
      throw new IllegalArgumentException("You must specify either message or requestBuilder")
    }
    requestBuilder = Some((node:Node, ids:Set[PartitionedId])=> message.getOrElse(throw new Exception("This should not happen")))
  }

  def getMessage() = message

  // Returns an optional anonymous function
  def getRequestBuilder() = requestBuilder

}


