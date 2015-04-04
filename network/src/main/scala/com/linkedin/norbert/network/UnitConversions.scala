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
package com.linkedin.norbert.network

import runtime.BoxedUnit

/**
 * This class helps with converting the return type of callback between BoxedUnit and Unit for java/scala
 * interoperability
 */
class UnitConversions[ResponseMsg] {
  // Converts the return type from Unit to BoxedUnit.UNIT
  def curryImplicitly[A](f: A => Unit) =
    (a: A) => { f(a); BoxedUnit.UNIT }

  // Converts the return type from BoxedUnit to Unit
  def uncurryImplicitly[A](f: A => BoxedUnit) =
    (a: A) => { f(a); () }

}