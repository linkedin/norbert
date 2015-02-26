package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Either;
import scala.Function1;

public interface RetrySpecification <ResponseMsg, Unit> {
   int getMaxRetry();

   Option<Function1<Either<Throwable, ResponseMsg>, Unit>>  getCallback();

}

