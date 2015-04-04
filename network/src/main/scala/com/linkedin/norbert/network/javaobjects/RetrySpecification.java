package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Either;
import scala.Function1;
import scala.runtime.BoxedUnit;

/**
 * A RetrySpecification interface is to be extended by RetrySpecification.scala so that sendRequest can
 * take java objects as arguments.  This file specifies getters for a RetrySpecification.
 */

public interface RetrySpecification <ResponseMsg> {
   int getMaxRetry();

   // Returns an anonymous function
   Function1<Either<Throwable, ResponseMsg>, BoxedUnit>  getCallback();

}

