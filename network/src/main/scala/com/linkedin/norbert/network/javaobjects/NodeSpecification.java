package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Long;


/**
 * Created by hwoodward on 11/16/14.
 */

public interface NodeSpecification {
    Option<Long> getCapability();
    Option<Long> getPersistentCapability();
}

