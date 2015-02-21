package com.linkedin.norbert.network.javaobjects;

import scala.Option;

public interface PartitionedNodeSpecification<PartitionedId> extends NodeSpecification {
    int getNumberOfReplicas();
    Option<int> getClusterId();
}
