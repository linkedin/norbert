package com.linkedin.norbert.network.javaobjects;

/**
 * A RequestSpecification interface is to be extended by RequestSpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a RequestSpecification.
 */

public interface RequestSpecification <RequestMsg> {
     RequestMsg getMessage();
}

