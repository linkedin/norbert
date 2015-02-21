package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import com.linkedin.norbert.network.common.RetryStrategy;

public interface PartitionedRetrySpecification <ResponseMsg>{
    scala.Option<RetryStrategy> getRetryStrategy();

}
