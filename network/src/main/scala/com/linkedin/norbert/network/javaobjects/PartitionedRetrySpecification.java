package com.linkedin.norbert.network.javaobjects;

import com.linkedin.norbert.RoutingConfigs;
import scala.Option;
import scala.Either;
import scala.Function1;
import scala.runtime.BoxedUnit;
import com.linkedin.norbert.network.common.RetryStrategy;
import com.linkedin.norbert.RoutingConfigs;

/**
 * A PartitionedRetrySpecification interface is to be extended by RetrySpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a PartitionedRetrySpecification.
 */

public interface PartitionedRetrySpecification <ResponseMsg, Unit> {
    int getMaxRetry();

    // Returns an anonymous function
    Function1<Either<Throwable, ResponseMsg>, BoxedUnit>  getCallback();

    Option<RetryStrategy> getRetryStrategy();
    RoutingConfigs getRoutingConfigs();

}
