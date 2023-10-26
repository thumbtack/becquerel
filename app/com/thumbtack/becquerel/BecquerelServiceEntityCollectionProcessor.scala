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

import java.io.{PrintWriter, StringWriter}
import java.net.URI
import scala.collection.JavaConversions._

import org.apache.olingo.commons.api.data.{ContextURL, EntityCollection}
import org.apache.olingo.commons.api.edm.EdmEntitySet
import org.apache.olingo.commons.api.format.ContentType
import org.apache.olingo.commons.api.http.{HttpHeader, HttpStatusCode}
import org.apache.olingo.server.api._
import org.apache.olingo.server.api.processor.{DefaultProcessor, EntityCollectionProcessor, ErrorProcessor}
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions
import org.apache.olingo.server.api.uri.{UriInfo, UriResourceEntitySet}
import play.api.Logger

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.util.control.NonFatal

/**
  * Bridge between Olingo and Becquerel services.
  *
  * Handles requests for table data by translating OData requests to service queries,
  * then translating the resulting rows to serialized OData entities.
  */
class BecquerelServiceEntityCollectionProcessor(
  val service: BecquerelService,
  val runID: Option[String] = None
) extends EntityCollectionProcessor with ErrorProcessor {

  protected var odata: OData = _
  protected var serviceMetadata: ServiceMetadata = _
  protected val defaultProcessor: DefaultProcessor = new DefaultProcessor()
  protected val logger = Logger(getClass)

  override def init(odata: OData, serviceMetadata: ServiceMetadata): Unit = {
    this.odata = odata
    this.serviceMetadata = serviceMetadata
    defaultProcessor.init(odata, serviceMetadata)
  }

  /**
    * This is an Olingo entry point and is mostly boilerplate adapted from the Olingo tutorial.
    *
    * @see https://olingo.apache.org/doc/odata4/tutorials/read/tutorial_read.html
    */
  override def readEntityCollection(
    request: ODataRequest,
    response: ODataResponse,
    uriInfo: UriInfo,
    responseFormat: ContentType
  ): Unit = {

    require(odata != null)
    require(serviceMetadata != null)

    if (Option(uriInfo.getCountOption).exists(_.getValue)) {
      throw odataFail("$count=true isn't supported.")
    }

    if (Option(uriInfo.getExpandOption).nonEmpty) {
      throw odataFail("$expand isn't supported.")
    }

    val resourcePaths = uriInfo.getUriResourceParts.asScala
    val uriResourceEntitySet = resourcePaths.head.asInstanceOf[UriResourceEntitySet]
    val entitySet = uriResourceEntitySet.getEntitySet

    val baseURI = new URI(request.getRawRequestUri)

    val entityCollection = odataTry("query") {
      Await.result(
        service.query(
          runID,
          entitySet,
          baseURI,
          Option(uriInfo.getFilterOption),
          Option(uriInfo.getSearchOption),
          Option(uriInfo.getSelectOption),
          Option(uriInfo.getOrderByOption),
          Option(uriInfo.getTopOption),
          Option(uriInfo.getSkipOption)
        ),
        service.queryTimeout
      )
    }

    odataTry("serialize") {
      serialize(
        request,
        response,
        uriInfo,
        responseFormat,
        entitySet,
        entityCollection
      )
    }
  }

  /**
    * Serialize an entity collection in the requested format.
    */
  protected def serialize(
    request: ODataRequest,
    response: ODataResponse,
    uriInfo: UriInfo,
    responseFormat: ContentType,
    entitySet: EdmEntitySet,
    entityCollection: EntityCollection
  ): Unit = {
    val serializer = odata.createSerializer(responseFormat)

    val edmEntityType = entitySet.getEntityType
    val contextURL = ContextURL.`with`()
      .entitySet(entitySet)
      .selectList(
        odata
          .createUriHelper()
          .buildContextURLSelectList(
            edmEntityType,
            null, // no expand option
            uriInfo.getSelectOption))
      .build()

    val id = s"${request.getRawBaseUri}${entitySet.getName}"
    val opts = EntityCollectionSerializerOptions.`with`()
      .id(id)
      .contextURL(contextURL)
      .select(uriInfo.getSelectOption)
      .build()
    val serializerResult = serializer
      .entityCollection(serviceMetadata, edmEntityType, entityCollection, opts)
    val serializedContent = serializerResult.getContent

    response.setContent(serializedContent)
    response.setStatusCode(HttpStatusCode.OK.getStatusCode)
    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString)
  }

  /**
    * @throws ODataApplicationException on errors, which will result in useful error output.
    */
  protected def odataTry[B](message: String)(block: => B): B = {
    try {
      block
    } catch {
      case NonFatal(e) => throw odataFail(message, e = Some(e))
    }
  }

  /**
    * @return An OData app exception with some detail.
    *
    * @note Will log everything as an error as a side effect.
    */
  protected def odataFail(
    message: String,
    e: Option[Throwable] = None
  ): ODataApplicationException = {
    val buf = new StringWriter()
    val printer = new PrintWriter(buf)
    printer.write(message)
    e match {
      case Some(t) => logger.error(buf.toString, t)
      case None => logger.error(buf.toString)
    }

    new ODataApplicationException(
      message,
      HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode,
      null, // message is not localized,
      e.orNull
    )
  }

  /**
    * Set the run ID as the code for any OData exception.
    */
  override def processError(
    request: ODataRequest,
    response: ODataResponse,
    serverError: ODataServerError,
    responseFormat: ContentType
  ): Unit = {
    runID.foreach(serverError.setCode)
    defaultProcessor.processError(request, response, serverError, responseFormat)
  }
}
