package com.linkedin.norbert.network.javaobjects;

import com.linkedin.norbert.RoutingConfigs;
import scala.Option;
import scala.Either;
import scala.Function1;
import com.linkedin.norbert.network.common.RetryStrategy;
import com.linkedin.norbert.RoutingConfigs;

public interface PartitionedRetrySpecification <ResponseMsg, Unit> {
    int getMaxRetry();
    Option<Function1<Either<Throwable, ResponseMsg>, Unit>>  getCallback();
    Option<RetryStrategy> getRetryStrategy();
    RoutingConfigs getRoutingConfigs();

}
