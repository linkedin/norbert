package com.linkedin.norbert.network.javaobjects;

import scala.collection.immutable.Set;

/**
 * A PartitionedNodeSpecification interface is to be extended by NodeSpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a PartitionedNodeSpecification.
 */

public interface PartitionedNodeSpecification<PartitionedId> extends NodeSpecification {
    int getNumberOfReplicas();
    Integer getClusterId();
    Set<PartitionedId> getIds();
}
