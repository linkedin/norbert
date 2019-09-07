/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package cluster
package common

import com.linkedin.norbert.util.WaitFor
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.{After, Before, Scope}

import scala.actors.Actor._

class ClusterNotificationManagerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor {

  trait ClusterNotificationManagerSetup extends Scope with After with Before with ClusterNotificationManagerComponent {

    import ClusterNotificationMessages._

    val clusterNotificationManager = new ClusterNotificationManager

    val shortNodes = Set(Node(1, "localhost:31313", false, Set(1, 2)))
    val nodes = shortNodes ++ List(Node(2, "localhost:31314", true, Set(3, 4)),
      Node(3, "localhost:31315", false, Set(5, 6)))

    def before = {
      clusterNotificationManager.start
    }

    def after = {
      clusterNotificationManager ! Shutdown
    }
  }

  trait ActorSetup extends ClusterNotificationManagerSetup {

    import ClusterNotificationMessages._

    override def after = {
      clusterNotificationManager ! Shutdown
      actors.Scheduler.shutdown
    }
  }

  "ClusterNotificationManager" should {
    "when handling an AddListener message" in new ClusterNotificationManagerSetup {
      "send a Connected event to the listener if the cluster is connected" in {
        import ClusterNotificationMessages._

        var callCount = 0
        val listener = actor {
          react {
            case ClusterEvents.Connected(_) => callCount += 1
          }
        }

        clusterNotificationManager ! Connected(nodes)

        var currentNodes: Set[Node] = Set()
        clusterNotificationManager ! AddListener(listener)
        callCount must eventually(be_==(1))

        currentNodes.foreach { node =>
          node.id must be_==(2)
        }

        currentNodes.size must be_==(1)
      }

      "not send a Connected event to the listener if the cluster is not connected" in {
        import ClusterNotificationMessages._

        var callCount = 0
        val listener = actor {
          react {
            case ClusterEvents.Connected(_) => callCount += 1
          }
        }

        clusterNotificationManager ! AddListener(listener)
        waitFor(20.ms)
        callCount must be_==(0)
      }

      "when handling a RemoveListener message remove the listener" in {

        import ClusterNotificationMessages._

        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(_) => callCount += 1
              case ClusterEvents.NodesChanged(_) => callCount += 1
            }
          }
        }

        val key = clusterNotificationManager !? AddListener(listener) match {
          case AddedListener(key) => key
        }

        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! RemoveListener(key)
        clusterNotificationManager ! NodesChanged(nodes)

        callCount must eventually(be_==(1))
      }
    }


    "when handling a Connected message" in new ActorSetup {

      import ClusterNotificationMessages._

      "notify listeners" in {
        var callCount = 0
        var currentNodes: Set[Node] = Set()
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(n) => callCount += 1; currentNodes = n
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)

        callCount must eventually(be_==(1))
        currentNodes.foreach { node =>
          node.id must be_==(2)
        }
        currentNodes.size must be_==(1)
      }

      "do nothing if already connected" in {
        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(_) => callCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Connected(nodes)

        callCount must eventually(be_==(1))
      }
    }


    "when handling a NodesChanged message" in new ActorSetup {

      import ClusterNotificationMessages._

      "notify listeners" in {
        var callCount = 0
        var currentNodes: Set[Node] = Set()
        val listener = actor {
          loop {
            react {
              case ClusterEvents.NodesChanged(n) =>
                callCount += 1
                currentNodes = n
            }
          }
        }

        clusterNotificationManager ! Connected(shortNodes)
        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! NodesChanged(nodes)

        callCount must eventually(be_==(1))
        currentNodes.foreach { node =>
          node.id must be_==(2)
        }
        currentNodes.size must be_==(1)
      }

      "do nothing is not connected" in {

        import ClusterNotificationMessages._

        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.NodesChanged(n) => callCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! Connected(shortNodes)
        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! NodesChanged(nodes)

        callCount must eventually(be_==(1))
      }

      "when handling a Disconnected message" in new ActorSetup {

        import ClusterNotificationMessages._

        "disconnects the cluster" in {
          clusterNotificationManager ! Connected(nodes)
          clusterNotificationManager ! Disconnected

          clusterNotificationManager !? GetCurrentNodes match {
            case CurrentNodes(nodes) => nodes.size must be_==(0)
          }
        }

        "notify listeners" in {
          var callCount = 0
          val listener = actor {
            loop {
              react {
                case ClusterEvents.Disconnected => callCount += 1
                case _ =>
              }
            }
          }

          clusterNotificationManager ! AddListener(listener)
          clusterNotificationManager ! Connected(nodes)
          clusterNotificationManager ! Disconnected

          callCount must eventually(be_==(1))
        }

        "do nothing if not connected" in {
          var callCount = 0
          val listener = actor {
            loop {
              react {
                case ClusterEvents.Disconnected => callCount += 1
                case _ =>
              }
            }
          }

          clusterNotificationManager ! AddListener(listener)
          clusterNotificationManager ! Disconnected

          callCount must eventually(be_==(0))
        }
      }

      "when handling a Shutdown message stop handling events after shutdown" in new ActorSetup {

        import ClusterNotificationMessages._

        var connectedCallCount = 0
        var shutdownCallCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(_) => connectedCallCount += 1
              case ClusterEvents.Shutdown => shutdownCallCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Shutdown
        clusterNotificationManager ! Connected(nodes)

        connectedCallCount must eventually(be_==(1))
        shutdownCallCount must eventually(be_==(1))
      }
    }
  }

}
