package com.linkedin.norbert
package network
package client

/**
 * Currently only handling one way IC
 */

trait Filter {
  def onRequest[RequestMsg](request: BaseRequest[RequestMsg]): Unit
}
