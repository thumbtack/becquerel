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

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import org.apache.olingo.commons.api.data.{Entity, EntityCollection, Property, ValueType}
import org.apache.olingo.commons.api.edm._
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider
import org.apache.olingo.server.api.uri.queryoption._
import org.scalatest.{BeforeAndAfter, FunSuite, ParallelTestExecution}
import play.api.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

class ShadowServiceTest extends FunSuite with BeforeAndAfter with ParallelTestExecution {

  /**
    * Stub entity set that only has a name.
    */
  //noinspection NotImplementedCode
  case class TestEdmEntitySet(name: String) extends EdmEntitySet {

    override def getName: String = name

    override def isIncludeInServiceDocument: Boolean = ???
    override def getEntityType: EdmEntityType = ???
    override def getEntityContainer: EdmEntityContainer = ???
    override def getTitle: String = ???
    override def getNavigationPropertyBindings: java.util.List[EdmNavigationPropertyBinding] = ???
    override def getRelatedBindingTarget(path: String): EdmBindingTarget = ???
    override def getMapping: EdmMapping = ???
    override def getAnnotations: java.util.List[EdmAnnotation] = ???
    override def getAnnotation(term: EdmTerm, qualifier: String): EdmAnnotation = ???
  }

  val shadowTable: TestEdmEntitySet = TestEdmEntitySet("shadow_table")
  val fallbackTable: TestEdmEntitySet = TestEdmEntitySet("fallback_table")
  val regularTable: TestEdmEntitySet = TestEdmEntitySet("regular_table")

  /**
    * Queries return canned results, optionally slowly.
    */
  case class StubService(
    bsConfig: BecquerelServiceConfig,
    delay: Duration = Duration.Zero,
    results: Seq[Entity] = Seq.empty,
    throwDuringPrep: Throwable = null,
    throwDuringQuery: Throwable = null
  ) extends BecquerelService {

    override def logger: Logger = null

    override def metadata: CsdlEdmProvider = null

    var queryCount: AtomicInteger = new AtomicInteger()

    override def query(
      runID: Option[String],
      entitySet: EdmEntitySet,
      baseURI: URI,
      filter: Option[FilterOption],
      search: Option[SearchOption],
      select: Option[SelectOption],
      orderBy: Option[OrderByOption],
      top: Option[TopOption],
      skip: Option[SkipOption]
    ): Future[EntityCollection] = {
      implicit val ec: ExecutionContext = bsConfig.queryEC

      queryCount.incrementAndGet()

      if (throwDuringPrep != null) {
        throw throwDuringPrep
      }

      Future {
        blocking {
          Thread.sleep(delay.toMillis)

          if (throwDuringQuery != null) {
            throw throwDuringQuery
          }

          val entityCollection = new EntityCollection()
          results.foreach(entityCollection.getEntities.add)
          entityCollection
        }
      }
    }
  }

  /**
    * Test version of the service manager that has its own actor system.
    */
  case class TestBecquerelServiceManager(
    mkServices: Map[String, (ActorSystem, BecquerelServiceManager) => BecquerelService]
  ) extends BecquerelServiceManager with AutoCloseable {

    private val actorSystem = ActorSystem()

    private val promises = mkServices.keys.map(key => key -> Promise[BecquerelService]()).toMap

    override def apply(name: String): Future[BecquerelService] = promises(name).future

    mkServices.foreach { case (name, mkService) =>
      val service = mkService(actorSystem, this)
      service.start()
      promises(name).success(service)
    }

    /**
      * The real version gets cleaned up at the end of the Play app lifecycle, but we need to do it manually.
      */
    def close(): Unit = {
      val servicesShutdown = Future.sequence(promises.values.map(_.future.flatMap(_.shutdown())))
      val actorsShutdown = actorSystem.terminate()
      val shutdown = servicesShutdown.zip(actorsShutdown)
      Await.result(shutdown, 10.seconds)
    }
  }

  var testServiceManager: TestBecquerelServiceManager = _

  after {
    testServiceManager.close()
  }

  /**
    * Set up a shadow service backed by two stub services.
    */
  def serviceManagerWith(
    secondaryResults: Seq[Entity] = Seq.empty,
    secondaryDelay: Duration = Duration.Zero,
    secondaryTimeout: Duration = Duration.Inf,
    secondaryThrowDuringPrep: Throwable = null,
    secondaryThrowDuringQuery: Throwable = null
  ): TestBecquerelServiceManager = {

    TestBecquerelServiceManager(
      Map(
        "primary" -> ((actorSystem, _) => StubService(
          bsConfig = BecquerelServiceConfig(
            name = "primary",
            queryTimeout = null,
            metrics = null,
            actorSystem = actorSystem,
            metadataEC = null,
            queryEC = actorSystem.dispatcher
          ),
          results = Seq(
            {
              val entity = new Entity()
              entity.setId(new URI("id"))
              entity.getProperties.add(new Property(
                EdmPrimitiveTypeKind.String.getFullQualifiedName.toString,
                "key",
                ValueType.PRIMITIVE.getBaseType,
                "value"
              ))
              entity
            }
          )
        )),

        "secondary" -> ((actorSystem, _) => StubService(
          bsConfig = BecquerelServiceConfig(
            name = "secondary",
            queryTimeout = null,
            metrics = null,
            actorSystem = actorSystem,
            metadataEC = null,
            queryEC = actorSystem.dispatcher
          ),
          delay = secondaryDelay,
          results = secondaryResults,
          throwDuringPrep = secondaryThrowDuringPrep,
          throwDuringQuery = secondaryThrowDuringQuery
        )),

        "shadow" -> ((actorSystem, serviceManager) => new ShadowService(
          ShadowServiceConfig(
            bsConfig = BecquerelServiceConfig(
              name = "shadow",
              queryTimeout = null,
              metrics = null,
              actorSystem = actorSystem,
              metadataEC = null,
              queryEC = actorSystem.dispatcher
            ),
            serviceManager = serviceManager,
            primaryName = "primary",
            secondaryName = "secondary",
            secondaryTimeout = secondaryTimeout,
            shadowTables = Set(shadowTable.getName),
            fallbackTables = Set(fallbackTable.getName)
          )
        ))
      )
    )
  }

  /**
    * Query the shadow service and check that we got one result with the expected value.
    */
  def checkShadowQuery(table: EdmEntitySet, expectedValue: String = "value"): Unit = {
    val result = Await.result(
      testServiceManager("shadow")
        .flatMap(_.query(None, table, null, None, None, None, None, None, None)),
      1.minute
    )
    assert(result.getEntities.size() === 1)
    assert(result.getEntities.get(0).getProperty("key").getValue === expectedValue)
  }

  /**
    * Count queries on the backends.
    */
  def checkBackendsUsed(
    expectedPrimaryCount: Int = 0,
    expectedSecondaryCount: Int = 0
  ): Unit = {
    val (actualPrimaryCount, actualSecondaryCount) = Await.result(
      testServiceManager("primary").map(_.asInstanceOf[StubService].queryCount.get())
        .zip(testServiceManager("secondary").map(_.asInstanceOf[StubService].queryCount.get())),
      1.minute
    )
    assert(actualPrimaryCount === expectedPrimaryCount)
    assert(actualSecondaryCount === expectedSecondaryCount)
  }

  test("shadow query timeout") {
    // Make secondary slow enough to time out.
    testServiceManager = serviceManagerWith(
      secondaryDelay = 2000.millis,
      secondaryTimeout = 1000.millis
    )

    // The query should succeed.
    // The shadow service should return in around the same time as the configured secondary timeout.
    val tooLate = 1200.millis.fromNow
    checkShadowQuery(shadowTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
    assert(tooLate.hasTimeLeft(), "Shadow service took too long to return!")
  }

  test("shadow query match") {
    // Same values as primary.
    testServiceManager = serviceManagerWith(
      secondaryResults = Seq(
        {
          val entity = new Entity()
          entity.setId(new URI("id"))
          entity.getProperties.add(new Property(
            EdmPrimitiveTypeKind.String.getFullQualifiedName.toString,
            "key",
            ValueType.PRIMITIVE.getBaseType,
            "value"
          ))
          entity
        }
      )
    )

    // The query should succeed.
    checkShadowQuery(shadowTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
  }

  test("shadow query value mismatch") {
    // Secondary result has a different value from the primary.
    testServiceManager = serviceManagerWith(
      secondaryResults = Seq(
        {
          val entity = new Entity()
          entity.setId(new URI("id"))
          entity.getProperties.add(new Property(
            EdmPrimitiveTypeKind.String.getFullQualifiedName.toString,
            "key",
            ValueType.PRIMITIVE.getBaseType,
            "someothervalue"
          ))
          entity
        }
      )
    )

    // The query should succeed anyway.
    checkShadowQuery(shadowTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
  }

  test("shadow query length mismatch") {
    // Secondary returns nothing.
    testServiceManager = serviceManagerWith()

    // The query should succeed anyway.
    checkShadowQuery(shadowTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
  }

  test("shadow query fails during prep") {
    // Secondary fails to even prepare its query.
    testServiceManager = serviceManagerWith(
      secondaryThrowDuringPrep = new Exception("prep")
    )

    // The query should succeed anyway.
    checkShadowQuery(shadowTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
  }

  test("shadow query fails during execution") {
    // Secondary prepares its query but fails while executing it.
    testServiceManager = serviceManagerWith(
      secondaryThrowDuringQuery = new Exception("query")
    )

    // The query should succeed anyway.
    checkShadowQuery(shadowTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
  }

  test("fallback query success") {
    // The secondary has different data than the primary, but that's ok.
    testServiceManager = serviceManagerWith(
      secondaryResults = Seq(
        {
          val entity = new Entity()
          entity.setId(new URI("id"))
          entity.getProperties.add(new Property(
            EdmPrimitiveTypeKind.String.getFullQualifiedName.toString,
            "key",
            ValueType.PRIMITIVE.getBaseType,
            "someothervalue"
          ))
          entity
        }
      )
    )

    // The query should succeed.
    checkShadowQuery(fallbackTable, "someothervalue")
    checkBackendsUsed(expectedSecondaryCount = 1)
  }

  test("fallback query timeout") {
    // Make secondary slow enough to time out.
    testServiceManager = serviceManagerWith(
      secondaryDelay = 2000.millis,
      secondaryTimeout = 1000.millis
    )

    // The shadow service should return in around the same time as the configured secondary timeout.
    val tooLate = 1200.millis.fromNow
    checkShadowQuery(fallbackTable)
    checkBackendsUsed(expectedPrimaryCount = 1, expectedSecondaryCount = 1)
    assert(tooLate.hasTimeLeft(), s"Shadow service took too long to return!")
  }

  test("regular query timeout") {
    // The secondary should not be used at all.
    testServiceManager = serviceManagerWith()

    // The query should succeed without using the secondary.
    checkShadowQuery(regularTable)
    checkBackendsUsed(expectedPrimaryCount = 1)
  }
}
