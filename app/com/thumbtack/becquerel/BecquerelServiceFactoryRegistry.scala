/*
 *    Copyright 2017 Thumbtack
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.thumbtack.becquerel

import javax.inject.Singleton

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/**
  * Map of service type names to factories.
  *
  * Uses promises because we can't use DI to create dependencies on modules known only at runtime,
  * so the [[BecquerelServiceManager]] that uses this may try to create services before all of the
  * service factories are registered.
  */
@Singleton
class BecquerelServiceFactoryRegistry {

  private val constructors: mutable.Map[String, Promise[BecquerelServiceFactory]] = mutable.Map.empty

  /**
    * Because [[mutable.Map.WithDefault]] doesn't actually work like any other language's defaultdict.
    */
  private def promiseFor(typeName: String) = constructors.getOrElseUpdate(typeName, Promise())

  /**
    * Register a service type.
    */
  def update(typeName: String, factory: BecquerelServiceFactory): Unit = promiseFor(typeName).success(factory)

  /**
    * Get a service factory by type.
    */
  def apply(typeName: String): Future[BecquerelServiceFactory] = promiseFor(typeName).future
}
