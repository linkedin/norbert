package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Either;
import scala.Function1;
import scala.Throwable;
import scala.Unit;

public interface RetrySpecification <ResponseMsg> {
   int getMaxRetry();

   Option<Function1<Either<Throwable, ResponseMsg>, Unit>>  getCallback();

}

