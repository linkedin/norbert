package com.linkedin.norbert.network.javaobjects;

import com.linkedin.norbert.cluster.Node;
import scala.Option;
import scala.collection.immutable.Set;
import scala.Function2;

public interface PartitionedRequestSpecification <RequestMsg, PartitionedId>{
    Option<RequestMsg> getMessage();
    Option<Function2<Node, Set<PartitionedId>, RequestMsg>> getRequestBuilder();
}
