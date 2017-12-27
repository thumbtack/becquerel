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

import java.io.{BufferedReader, ByteArrayInputStream}
import java.security.Principal
import java.util.Locale
import javax.servlet._
import javax.servlet.http._

import play.api.mvc.{RawBuffer, Request}

import scala.collection.JavaConverters._

/**
  * Implement enough of the Servlet API to make Olingo's HTTP handler work.
  *
  * @note Buffers entire request in memory. Not suitable for large requests,
  *       although since we're a read-only service, there won't be too many of those.
  *       Play's raw body parser will buffer large requests to disk,
  *       but this will try to read them in all at once.
  *
  * @see https://www.playframework.com/documentation/2.5.x/ScalaBodyParsers#max-content-length
  *
  * @param request The original Play request.
  * @param path The path part of the URL from after the OData service name.
  *             For example, /bq/foo would result in /foo.
  * @param reverseURL Reconstructed URL of the request from the Play reverse router.
  *                   Used for generating IDs and entity links.
  */
//noinspection NotImplementedCode
class PlayRequestAdapter(
  request: Request[RawBuffer],
  path: String,
  reverseURL: String
) extends HttpServletRequest {
  private val inputStream = new ServletInputStream {
    val buf = new ByteArrayInputStream(request.body.asBytes().get.toArray)

    override def read(): Int = buf.read()

    override def isReady: Boolean = ???

    override def setReadListener(readListener: ReadListener): Unit = ???

    override def isFinished: Boolean = ???
  }

  override def getHeaders(name: String): java.util.Enumeration[String] = {
    java.util.Collections.enumeration(request.headers.getAll(name).asJava)
  }

  override def getHeaderNames: java.util.Enumeration[String] = {
    java.util.Collections.enumeration(request.headers.keys.asJava)
  }

  override def getRequestURL: StringBuffer = {
    new StringBuffer(reverseURL)
  }

  override def getQueryString: String = {
    request.rawQueryString
  }

  override def getContextPath: String = {
    ""
  }

  override def getServletPath: String = {
    ""
  }

  override def getRequestURI: String = {
    path
  }

  override def getProtocol: String = {
    request.version
  }

  override def getInputStream: ServletInputStream = {
    inputStream
  }

  override def isRequestedSessionIdFromURL: Boolean = ???

  override def getRemoteUser: String = ???

  override def getUserPrincipal: Principal = ???

  override def getPathInfo: String = ???

  override def getAuthType: String = ???

  override def getCookies: Array[Cookie] = ???

  override def getPathTranslated: String = ???

  override def getIntHeader(name: String): Int = ???

  override def getRequestedSessionId: String = ???

  override def isRequestedSessionIdFromUrl: Boolean = ???

  override def isRequestedSessionIdValid: Boolean = ???

  override def getSession(create: Boolean): HttpSession = ???

  override def getSession: HttpSession = ???

  override def getMethod: String = request.method

  override def getDateHeader(name: String): Long = ???

  override def isUserInRole(role: String): Boolean = ???

  override def isRequestedSessionIdFromCookie: Boolean = ???

  override def getHeader(name: String): String = ???

  override def getParameter(name: String): String = ???

  override def getRequestDispatcher(path: String): RequestDispatcher = ???

  override def getRealPath(path: String): String = ???

  override def getLocale: Locale = ???

  override def getRemoteHost: String = ???

  override def getParameterNames: java.util.Enumeration[String] = ???

  override def isSecure: Boolean = ???

  override def getLocalPort: Int = ???

  override def getAttribute(name: String): AnyRef = ???

  override def removeAttribute(name: String): Unit = ???

  override def getLocalAddr: String = ???

  override def getCharacterEncoding: String = ???

  override def setCharacterEncoding(env: String): Unit = ???

  override def getParameterValues(name: String): Array[String] = ???

  override def getRemotePort: Int = ???

  override def getServerName: String = ???

  override def getLocales: java.util.Enumeration[Locale] = ???

  override def getAttributeNames: java.util.Enumeration[String] = ???

  override def setAttribute(name: String, o: Any): Unit = ???

  override def getRemoteAddr: String = ???

  override def getLocalName: String = ???

  override def getContentLength: Int = ???

  override def getServerPort: Int = ???

  override def getReader: BufferedReader = ???

  override def getContentType: String = ???

  override def getScheme: String = ???

  override def getParameterMap: java.util.Map[String, Array[String]] = ???

  override def logout(): Unit = ???

  override def changeSessionId(): String = ???

  override def upgrade[T <: HttpUpgradeHandler](handlerClass: Class[T]): T = ???

  override def authenticate(response: HttpServletResponse): Boolean = ???

  override def login(username: String, password: String): Unit = ???

  override def getParts: java.util.Collection[Part] = ???

  override def getPart(name: String): Part = ???

  override def getAsyncContext: AsyncContext = ???

  override def isAsyncSupported: Boolean = ???

  override def isAsyncStarted: Boolean = ???

  override def startAsync(): AsyncContext = ???

  override def startAsync(servletRequest: ServletRequest, servletResponse: ServletResponse): AsyncContext = ???

  override def getContentLengthLong: Long = ???

  override def getServletContext: ServletContext = ???

  override def getDispatcherType: DispatcherType = ???
}
