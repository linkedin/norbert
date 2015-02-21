package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Either;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.String;

public interface RetrySpecification <ResponseMsg> {
   int getMaxRetry();

   Option<Function1<Either<scala.Throwable, ResponseMsg>, scala.Unit>>  getCallback();

}

