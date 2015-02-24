package com.linkedin.norbert.network.javaobjects;

import scala.Option;

/**
 * Created by hwoodward on 11/16/14.
 */

public interface NodeSpecification {
    Option<scala.Long> getCapability();
    Option<scala.Long> getPersistentCapability();
}

