package com.linkedin.norbert.network.javaobjects;

import com.linkedin.norbert.cluster.Node;
import scala.Option;
import scala.collection.immutable.Set;
import scala.Function2;

/**
 * A PartitionedRequestSpecification interface is to be extended by RequestSpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a PartitionedRequestSpecification.
 */

public interface PartitionedRequestSpecification <RequestMsg, PartitionedId>{
    Option<RequestMsg> getMessage();

    // Returns an optional anonymous function that takes two arguments
    Option<Function2<Node, Set<PartitionedId>, RequestMsg>> getRequestBuilder();
}
