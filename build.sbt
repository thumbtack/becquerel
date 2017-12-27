import scala.util.Try
import ReleaseTransformations._

name := "becquerel"

enablePlugins(PlayScala)
enablePlugins(BuildInfoPlugin)

lazy val gitBuildInfoKeys: Seq[BuildInfoKey.Entry[_]] = {
  def gitRevParse(option: String): Option[String] = {
    Try(s"git rev-parse $option HEAD".!!).toOption.map(_.trim)
  }
  Seq(
    "gitShortRevision" -> gitRevParse("--short"),
    "gitRevision" -> gitRevParse(""),
    "gitBranchName" -> gitRevParse("--abbrev-ref")
  )
    .map(BuildInfoKey.constant(_))
}

lazy val ciBuildInfoKeys: Seq[BuildInfoKey.Entry[_]] = {

  def getEnvOpt(name: String): Option[String] = Option(System.getenv(name))

  def travisBuildTag: Option[String] = {
    Some(Seq("TRAVIS_REPO_SLUG", "TRAVIS_BRANCH", "TRAVIS_BUILD_NUMBER").map(getEnvOpt))
      .filter(_.forall(_.isDefined)).map(_.flatten.mkString("-"))
  }

  def jenkinsBuildTag: Option[String] = getEnvOpt("BUILD_TAG")

  Seq("ciBuildTag" -> travisBuildTag.orElse(jenkinsBuildTag))
    .map(BuildInfoKey.constant(_))
}

buildInfoKeys := Seq[BuildInfoKey.Entry[_]](
  name,
  version,
  isSnapshot,
  scalaVersion
) ++ gitBuildInfoKeys ++ ciBuildInfoKeys
buildInfoPackage := "com.thumbtack.becquerel"
buildInfoOptions ++= Seq(BuildInfoOption.ToMap, BuildInfoOption.ToJson)

scalaVersion := "2.11.8"

val googleCloudVersion = "0.8.3-beta"
val olingoVersion = "4.3.0"
val elastic4sVersion = "5.4.9"
val log4jVersion = "2.8.2"

libraryDependencies ++= Seq(
  filters,
  jdbc,

  // Explicitly depend on Servlet API 3.x.
  "javax.servlet" % "javax.servlet-api" % "3.1.0",

  "com.jsuereth" %% "scala-arm" % "2.0",
  "org.scalatestplus.play" %% "scalatestplus-play" % "2.0.1" % "test",
  "org.apache.olingo" % "odata-commons-api" % olingoVersion,
  "org.apache.olingo" % "odata-commons-core" % olingoVersion,
  "org.apache.olingo" % "odata-server-api" % olingoVersion,
  "org.apache.olingo" % "odata-server-core" % olingoVersion,
  // Provides a SQL AST and writer.
  "org.apache.calcite" % "calcite-core" % "1.12.0",
  // Log JSON.
  "net.logstash.logback" % "logstash-logback-encoder" % "4.8",
  // Format byte counts on debug page.
  "com.twitter" %% "util-core" % "6.42.0",
  // Play filter for DropWizard metrics library.
  // This is the forked version from https://github.com/breadfan/metrics-play that supports Play 2.5.
  "de.threedimensions" %% "metrics-play" % "2.5.13",
  // Report DropWizard metrics to InfluxDB.
  "com.izettle" % "dropwizard-metrics-influxdb" % "1.1.8" excludeAll(
    ExclusionRule("com.fasterxml.jackson.core", "jackson-core"),
    ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
    ExclusionRule("com.fasterxml.jackson.core", "jackson-annotations")
  ),

  // BigQuery backend.
  "com.google.cloud" % "google-cloud-bigquery" % googleCloudVersion,

  // JDBC drivers. H2 is included in Play somehow.
  "org.postgresql" % "postgresql" % "42.1.1",

  // Elasticsearch backend.
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-play-json" % elastic4sVersion,
  // elastic4s depends on the official ES client, which uses Log4j 2, which we don't want.
  // Redirect Log4j to SLF.
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-to-slf4j" % log4jVersion,

  // Demo only.
  "com.univocity" % "univocity-parsers" % "2.5.8",
  "com.google.cloud" % "google-cloud-storage" % googleCloudVersion
)

excludeDependencies ++= Seq(
  // Google libraries pull in an old version (2.5) of the Servlet API under a slightly different name.
  SbtExclusionRule("javax.servlet", "servlet-api")
)

javaOptions in Test += "-Dlogger.resource=logback-test.xml"

resolvers ++= Seq(
  "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
  Resolver.bintrayRepo("breadfan", "maven")
)

// Let Travis handle publishing release artifacts.
releaseProcess -= publishArtifacts
