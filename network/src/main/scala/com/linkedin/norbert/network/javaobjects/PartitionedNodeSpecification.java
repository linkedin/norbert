package com.linkedin.norbert.network.javaobjects;

public interface PartitionedNodeSpecification<PartitionedId> extends NodeSpecification {
    int getNumberOfReplicas();
    scala.Option<int> getClusterId();
}
