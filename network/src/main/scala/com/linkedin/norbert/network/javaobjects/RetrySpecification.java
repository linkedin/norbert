package com.linkedin.norbert.network.javaobjects;

import scala.Option;
import scala.Either;
import scala.runtime;

public interface RetrySpecification <ResponseMsg> {
   int getMaxRetry();

    Function1<Option<Either<scala.Throwable, scala.String>>, scala.Unit> f = new AbstractFunction1<Option<Either<scala.Throwable, scala.String>>, scala.Unit>() {
        public scala.Unit apply(Option<Either<scala.Throwable, scala.String>> cb) {
            return getCallback(cb);
        }
    };
//    final Function1<Option<Either<scala.Throwable, scala.String>>, scala.Unit> f = new Function1<Option<Either<scala.Throwable, scala.String>>, scala.Unit>() {
//        public int $tag() {
//            return Function1$class.$tag(this);
//        }
//
//        public <A>Function1<A, scala.Unit> compose(Function1<A, Option<Either<scala.Throwable, scala.String>>> f) {
//            return Function1$class.compose(this, f);
//        }
//
//        public void apply(Option<Either<scala.Throwable, scala.String>> callback) {
//            return getCallback(callback);
//        }
//    };
}

