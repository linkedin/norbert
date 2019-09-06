package com.linkedin.norbert.util

import java.util.concurrent.TimeUnit.{MICROSECONDS, MILLISECONDS, NANOSECONDS, SECONDS}

/**
  * The WaitFor trait can be used to add some Thread.sleep instructions to an executed specification.<p>
  * Usage:<pre>
  * waitFor(10.ms) // wait for 10 milliseconds
  * </pre>
  */
trait WaitFor {
  /**
    * wait for a given time specified by the delay
    */
  def waitFor(delay: Delay): Unit = waitFor(delay.millis)

  /**
    * wait for a given number of milliseconds
    */
  def waitFor(i: Int): Unit = Thread.sleep(i)

  /**
    * @return a Delay object The WaitFor trait can be used to add some Thread.sleep instructions to an executed specification
    */
  implicit def intToDelay(i: Int) = Delay(i)

  /**
    * A Delay represents a time interval expressed with a TimeUnit in nanoseconds, microseconds, milliseconds or seconds.
    * Note that the symbol for micro is us with the u approximating the Greek symbol "mu"
    */
  case class Delay(i: Int) {
    var timeUnit = MILLISECONDS
    def ns = { timeUnit = NANOSECONDS; this }
    def us = { timeUnit = MICROSECONDS; this }
    def ms = { timeUnit = MILLISECONDS; this }
    def s = { timeUnit = SECONDS; this }

    /**
      * @return the delay in nanos
      */
    def nanos = {
      if (timeUnit == NANOSECONDS)
        i
      else if (timeUnit == MICROSECONDS)
        i * 1000
      else if (timeUnit == MILLISECONDS)
        i * 1000000
      else
        i * 1000000000
    }

    /**
      * @return the delay in micros
      */
    def micros = {
      if (timeUnit == NANOSECONDS)
        i / 1000
      else if (timeUnit == MICROSECONDS)
        i
      else if (timeUnit == MILLISECONDS)
        i * 1000
      else
        i * 1000000
    }

    /**
      * @return the delay in millis
      */
    def millis = {
      if (timeUnit == NANOSECONDS)
        i / 1000000
      else if (timeUnit == MICROSECONDS)
        i / 1000
      else if (timeUnit == MILLISECONDS)
        i
      else
        i * 1000
    }

    /**
      * @return the delay in seconds
      */
    def seconds = {
      if (timeUnit == NANOSECONDS)
        i / 1000000000
      else if (timeUnit == MICROSECONDS)
        i / 1000000
      else if (timeUnit == MILLISECONDS)
        i / 1000
      else
        i
    }
  }
}
