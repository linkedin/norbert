package com.linkedin.norbert.network.javaobjects;

import com.linkedin.norbert.network.partitioned.RoutingConfigs;
import scala.Option;
import scala.Function1;
import scala.runtime.BoxedUnit;
import com.linkedin.norbert.network.common.RetryStrategy;
import scala.util.Either;


/**
 * A PartitionedRetrySpecification interface is to be extended by RetrySpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a PartitionedRetrySpecification.
 */

public interface PartitionedRetrySpecification <ResponseMsg> {
    int getMaxRetry();

    // Returns an anonymous function
    Function1<Either<Throwable, ResponseMsg>, BoxedUnit>  getCallback();

    Option<RetryStrategy> getRetryStrategy();
    RoutingConfigs getRoutingConfigs();

}
