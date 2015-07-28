package com.linkedin.norbert.network.util

import java.util.concurrent.atomic.AtomicInteger

import com.linkedin.norbert.logging.Logging

/**
 * Created by sishah on 7/22/15.
 */
class ConcurrentCyclicCounter extends Logging {

  val counter = new AtomicInteger(0)

  def getAndIncrement: Int = {

    var prev = 0
    var next = 0

    do {

      prev = counter.get()
      next = (prev + 1) % Integer.MAX_VALUE

    } while(!counter.compareAndSet(prev, next))

    prev
  }

  def get: Int = {

    counter.get()

  }

  /** Compensate counter to origValue + count + 1, keeping in mind overflow */
  def compensate(origValue: Int, count:Int): Unit = {

    if(count <= 0 || origValue < 0 || origValue == Integer.MAX_VALUE)
      return

    val valueToSet = if(origValue+count+1 <= 0) { origValue + 1 - Integer.MAX_VALUE + count }
                     else {origValue+count+1}

    // If the counter has been touched since the original value being compensated was seen, do nothing.
    // This maintains a strictly increasing (modulo MAX_VALUE) order for the counter.
    // This is based on the assumption that rapid changes in the environment don't occur. That is, I assume situations
    // like the following don't occur:
    // OriginalCounter: 3.
    // Thread1: sees & gets 3, increments counter to 4.
    // Thread2: sees & gets 4, increments counter to 5.
    // Thread2 compensates counter to 6, post its operations (I.E. it didn't find node 4 viable, and picked node 5 instead)
    // Thread1 now doesn't find nodes 3, 4, 5 (which was available just before) or 6 viable, for some reason, picks 7 and tries to compensate to 8.
    // This will not allow the compensation, and the next call to the counter may send another request to 7.
    //
    // On the other hand, this will prevent situations like the following:
    // OriginalCounter: 3
    // Thread1: sees & gets 3, increments counter to 4.
    // Thread2: sees & gets 4, increments counter to 5.
    // Thread3: sees & gets 5, increments counter to 6.
    // Thread2 is happy with node 4, tries to compensate with count=0, function returns in the first check.
    // Thread3 is happy with node 5, and no compensation occurs here as well.
    // Thread1 didn't find 3 viable meanwhile, and has settled on 4 as well. It tries to compensate the counter to 5,
    // and without this check, it would be able to, causing the next request to hit node 5 again, instead of 6 or above.
    //
    // Additionally, I can't do a simple set-if-larger to ensure strict ordering, because of the wrapping around of the
    // counter. Unless, I put in some ugly heuristic which says set-if-larger-or-significantly-smaller (the latter
    // indicating that the counter has wrapped around and that the smaller value is really the more recent value)
    //
    // Also, finally, all of these are edge cases and may not even be worth consideration, so that.

    counter.compareAndSet((origValue+1) % Integer.MAX_VALUE, valueToSet % Integer.MAX_VALUE)
  }

}
