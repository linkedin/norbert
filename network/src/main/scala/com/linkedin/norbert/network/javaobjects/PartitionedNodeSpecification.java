package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Int;

public interface PartitionedNodeSpecification<PartitionedId> extends NodeSpecification {
    Int getNumberOfReplicas();
    Option<Int> getClusterId();
}
