package com.linkedin.norbert.network.javaobjects;

import scala.collection.immutable.Set;

public interface PartitionedNodeSpecification<PartitionedId> extends NodeSpecification {
    int getNumberOfReplicas();
    Integer getClusterId();
    Set<PartitionedId> getIds();
}
