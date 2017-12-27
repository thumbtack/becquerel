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

package com.thumbtack.becquerel.util

import java.io.{ByteArrayOutputStream, PrintWriter}
import java.util.Locale
import javax.servlet.http.{Cookie, HttpServletResponse}
import javax.servlet.{ServletOutputStream, WriteListener}

import akka.util.ByteString
import org.apache.olingo.commons.api.http.HttpHeader
import play.api.http.HttpEntity
import play.api.mvc.{ResponseHeader, Result}

import scala.collection.mutable

/**
  * Implement enough of the Servlet API to make Olingo's HTTP handler work.
  *
  * @note Buffers entire response in memory. Not suitable for large responses.
  *       If we hit that problem, we can work around it by configuring the OData client
  *       (for example, Salesforce) to request smaller pages.
  *       A permanent solution would involve writing a Play-specific Olingo HTTP handler.
  */
//noinspection NotImplementedCode
class PlayResponseAdapter extends HttpServletResponse {
  private class ByteArrayServletOutputStream extends ServletOutputStream {
    val buf = new ByteArrayOutputStream()

    override def write(b: Int): Unit = buf.write(b)

    override def isReady: Boolean = ???

    override def setWriteListener(writeListener: WriteListener): Unit = ???
  }

  private val outputStream = new ByteArrayServletOutputStream()

  private val headers: mutable.Map[String, String] = mutable.Map.empty[String, String]

  private var status: Int = 0

  private var contentType: Option[String] = None

  def result: Result = {
    require(status != 0)

    Result(
      ResponseHeader(status, headers.toMap),
      HttpEntity.Strict(
        ByteString(outputStream.buf.toByteArray),
        contentType))
  }

  override def setStatus(sc: Int): Unit = {
    status = sc
  }

  override def addHeader(name: String, value: String): Unit = {
    name match {
      case HttpHeader.CONTENT_TYPE => contentType = Some(value)
      case _ => headers(name) = value
    }
  }

  override def getOutputStream: ServletOutputStream = outputStream

  // Methods below this line are not used by Olingo and thus not implemented.

  override def sendError(sc: Int, msg: String): Unit = ???

  override def sendError(sc: Int): Unit = ???

  override def containsHeader(name: String): Boolean = ???

  override def setStatus(sc: Int, sm: String): Unit = ???

  override def addIntHeader(name: String, value: Int): Unit = ???

  override def encodeURL(url: String): String = ???

  override def addCookie(cookie: Cookie): Unit = ???

  override def addDateHeader(name: String, date: Long): Unit = ???

  override def setDateHeader(name: String, date: Long): Unit = ???

  override def encodeRedirectUrl(url: String): String = ???

  override def sendRedirect(location: String): Unit = ???

  override def setHeader(name: String, value: String): Unit = ???

  override def setIntHeader(name: String, value: Int): Unit = ???

  override def encodeRedirectURL(url: String): String = ???

  override def encodeUrl(url: String): String = ???

  override def getLocale: Locale = ???

  override def isCommitted: Boolean = ???

  override def setBufferSize(size: Int): Unit = ???

  override def setContentLength(len: Int): Unit = ???

  override def setContentType(contentType: String): Unit = ???

  override def resetBuffer(): Unit = ???

  override def getCharacterEncoding: String = ???

  override def setCharacterEncoding(charset: String): Unit = ???

  override def getWriter: PrintWriter = ???

  override def flushBuffer(): Unit = ???

  override def setLocale(loc: Locale): Unit = ???

  override def getContentType: String = ???

  override def reset(): Unit = ???

  override def getBufferSize: Int = ???

  override def getHeader(x: String): String = ???

  override def getHeaderNames: java.util.Collection[String] = ???

  override def getHeaders(x: String): java.util.Collection[String] = ???

  override def getStatus: Int = ???

  override def setContentLengthLong(x: Long): Unit = ???
}
