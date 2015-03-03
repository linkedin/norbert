package com.linkedin.norbert.network.javaobjects;


/**
 * A NodeSpecification interface is to be extended by NodeSpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a NodeSpecification.
 */

public interface NodeSpecification {
    Long getCapability();
    Long getPersistentCapability();
}

