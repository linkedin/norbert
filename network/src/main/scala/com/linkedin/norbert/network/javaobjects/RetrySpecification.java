package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Either;
import scala.Function1;

/**
 * A RetrySpecification interface is to be extended by RetrySpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a RetrySpecification.
 */

public interface RetrySpecification <ResponseMsg, Unit> {
   int getMaxRetry();

   // Returns an optional anonymous function
   Option<Function1<Either<Throwable, ResponseMsg>, Unit>>  getCallback();

}

