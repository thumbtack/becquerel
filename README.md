# Becquerel

Becquerel is a gateway server that provides an [OData 4.0](http://www.odata.org/documentation/) interface to BigQuery, Elasticsearch, and SQL databases. Want your CRM or customer support system to be able to read from your big data warehouse? Becquerel has the hookup.

[Erica Ehrhardt](https://github.com/thumbtack-eee) developed Becquerel at [Thumbtack](https://www.thumbtack.com/), where it's currently used in production to provide Thumbtack's Salesforce instance with customer data from BigQuery and Elasticsearch.

[![license](https://img.shields.io/github/license/thumbtack/becquerel.svg)](LICENSE.txt)
[![Travis](https://img.shields.io/travis/thumbtack/becquerel.svg)](https://travis-ci.org/thumbtack/becquerel)
[![Codecov](https://img.shields.io/codecov/c/github/thumbtack/becquerel.svg)](https://codecov.io/gh/thumbtack/becquerel)
[![GitHub release](https://img.shields.io/github/release/thumbtack/becquerel.svg)](releases)
[![Github All Releases](https://img.shields.io/github/downloads/thumbtack/becquerel/total.svg)](releases)

# Table of contents

[//]: # (Generate this with gd-md-toc: https://github.com/ekalinin/github-markdown-toc.go)
[//]: # (gh-md-toc README.md | grep -v gh-md-toc | tail -n +5 | sed '/^$/d')

  * [Becquerel](#becquerel)
  * [Table of contents](#table-of-contents)
  * [Getting started](#getting-started)
    * [Configuring Becquerel](#configuring-becquerel)
    * [Running Becquerel](#running-becquerel)
    * [Testing Becquerel](#testing-becquerel)
      * [Loading the test data](#loading-the-test-data)
    * [Packaging Becquerel](#packaging-becquerel)
    * [Deploying Becquerel](#deploying-becquerel)
      * [Developer proxy setup with ngrok](#developer-proxy-setup-with-ngrok)
    * [Connecting to Salesforce](#connecting-to-salesforce)
  * [Getting involved](#getting-involved)
  * [Supported data sources](#supported-data-sources)
    * [BigQuery (BQ)](#bigquery-bq)
    * [Elasticsearch (ES)](#elasticsearch-es)
    * [JDBC](#jdbc)
  * [OData](#odata)
    * [Service documents](#service-documents)
    * [Queries](#queries)
    * [Metadata](#metadata)
      * [Collection naming](#collection-naming)
      * [Primary keys](#primary-keys)
      * [Fetching metadata](#fetching-metadata)
  * [Architecture notes](#architecture-notes)
    * [OData implementation](#odata-implementation)
    * [Status page](#status-page)
    * [Logging](#logging)
    * [Metrics](#metrics)
    * [Security considerations](#security-considerations)
  * [Limitations](#limitations)
  * [Alternatives](#alternatives)
    * [WSO2 DSS](#wso2-dss)

# Getting started

If you're not customizing it and just want to use it, download a prebuilt Becquerel release from our [GitHub Releases section](releases) and read the subsections below on [configuring](#configuring-becquerel), [deploying](#deploying-becquerel), and [connecting to Salesforce](#connecting-to-salesforce). For full development info, read all of this section.

## Configuring Becquerel

Becquerel uses [Play's `conf/application.conf` file](https://www.playframework.com/documentation/2.5.x/ConfigFile) for most configuration. You'll need to customize this for your Becquerel deployment by adding at least one OData service. We've provided an annotated [`conf/application-example.conf`](conf/application-example.conf) to document Becquerel's config options; copy it to `conf/application.conf` and edit the `services` section to get started.

Becquerel uses [Play's default logging](https://www.playframework.com/documentation/2.5.x/SettingsLogger), which is based on [Logback](https://logback.qos.ch/) and should be configured by copying [`conf/logback-example.xml`](conf/logback-example.xml) to `conf/logback.xml`. See the [Logging section](#logging) for more.

If you are using a BigQuery-backed service, the easiest way to get started is to install the Google Cloud SDK and then run `gcloud init`, which will generate application default credentials. These will grant your development instance of Becquerel access to the same BQ tables that you have access to. You can also use a service account; see the example config file for more.

## Running Becquerel

Becquerel can be run like any other Play app with `sbt run`, or the IntelliJ Play 2 integration, both of which will launch Becquerel at [`http://localhost:9000/`](http://localhost:9000/).

## Testing Becquerel

Run `sbt test`, or use IntelliJ's ScalaTest task to run all tests in the `test` folder.

By default, this will only run unit tests. Integration tests can be turned after creating config files by setting certain environment variables:

- `ES_TESTS=true`: Tests ES integration. Assumes you have an ES server running at `localhost:9200` containing the Dell DVD Store data.
- `PG_TESTS=true`: Tests JDBC integration with PG specifically. Assumes you have a PG server running at `localhost:5432` with a database named `ds2` containing the Dell DVD Store data.
- `INTEGRATION_TESTS=true`: End to end tests of parts of the entire Becquerel app, using the real Play HTTP stack. Requires at least one of the above.

### Loading the test data

Becquerel comes with some classes in the [`com.thumbtack.becquerel.demo`](app/com/thumbtack/becquerel/demo) namespace to make it easy to load test data into backing databases. Currently, it supports the data from [Dell DVD Store](https://linux.dell.com/dvdstore/), version 2.1.

```bash
# Fetch the data as well as MySQL and PG loader scripts.
pushd test_data
./ds2_download.sh
popd

# Copy and edit the demo configuration file to match your setup.
cp conf/demo-example.conf conf/demo.conf

# Load the data into one or more backing databases.
# Note that BQ requires the dataset to be created ahead of time, as well as a writable GCS bucket for temp files.
sbt 'runMain com.thumbtack.becquerel.demo.BqDemoLoader'
sbt 'runMain com.thumbtack.becquerel.demo.EsDemoLoader'
# Note that the data files must be on the same machine as the PG server for this loader to work.
sbt 'runMain com.thumbtack.becquerel.demo.PgDemoLoader'
```

## Packaging Becquerel

```bash
# Run the full test suite first.
# Configure application.conf for your data sources, and start their backing servers, then:
ES_TESTS=true PG_TESTS=true INTEGRATION_TESTS=true sbt test

# Package it into a tarball, without the integration test config.
rm -f conf/application.conf conf/demo.conf conf/logback.xml
sbt universal:packageZipTarball

# Copy that tarball to your target machine somehow.
scp target/becquerel/becquerel-*.tgz target:
```

## Deploying Becquerel

```bash
# On your target machine:
tar -xzf becquerel-*.tgz
rm becquerel-*.tgz
cd becquerel-*
# Note: if you used a zip release, you'll need to do this extra step to restore exec permissions:
chmod +x bin/becquerel

# Create and edit configuration files.
cp conf/application-example.conf conf/application.conf
cp conf/logback-example.xml conf/logback.xml

# Run Becquerel in production mode.
bin/becquerel
```

In production configurations, you should deploy Becquerel behind a [reverse proxy server](https://www.playframework.com/documentation/2.5.x/HTTPServer) such as [nginx](https://nginx.org/en/), especially if it's expected to connect to cloud services on the public Internet. The reverse proxy should be configured to use [HTTP basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication) and TLS. Multi-instance configurations should also use a load balancer.

### Developer proxy setup with ngrok

Since the details of that setup are likely specific to your organization, here's an example of how to use Becquerel with the [ngrok](https://ngrok.com/) developer proxy with HTTP basic auth and TLS:

```bash
# Assumes Becquerel is running on the default port 9000.
# Change username and password to a username and password you generate.
# ngrok will assign a random public URL like <https://3c81bfc9.ngrok.io>.
ngrok http -bind-tls=true -auth="username:password" 9000
```

## Connecting to Salesforce

See the [Salesforce setup instructions](docs/salesforce/salesforce.md). They assume you're already running a Becquerel instance with Elasticsearch, the DVD Store data, and an ngrok proxy using the above settings.
  
# Getting involved

We welcome feedback in the [GitHub issue tracker](issues), as well as pull requests!

# Supported data sources

## BigQuery (BQ)

[`com.thumbtack.becquerel.datasources.bigquery`](app/com/thumbtack/becquerel/datasources/bigquery)

Google's [BigQuery](https://cloud.google.com/bigquery/) analytics-oriented SQL database is Becquerel's original _raison d'être_ and is well-supported. While BQ can process colossal amounts of data, it's a high-latency system that takes a few seconds to return results from any query, no matter how small the result set.

## Elasticsearch (ES)

[`com.thumbtack.becquerel.datasources.elasticsearch`](app/com/thumbtack/becquerel/datasources/elasticsearch)

[Elasticsearch](https://www.elastic.co/products/elasticsearch) is a horizontally scalable NoSQL data store supporting key lookups and full-text search. ES is a good choice for storing precomputed output from batch processes for low-latency retrieval.

## JDBC

[`com.thumbtack.becquerel.datasources.jdbc`](app/com/thumbtack/becquerel/datasources/jdbc)

Becquerel has *experimental partial support* for SQL databases that provide [JDBC](http://www.oracle.com/technetwork/java/javase/jdbc/index.html) drivers. It's been tested with [PostgreSQL](https://www.postgresql.org/) (PG) 9.6 and 10.1, and the [H2](https://www.h2database.com/) version that ships with Play. Expect some string functions not to work.

# OData

OData is a refinement of HTTP REST APIs for database-like applications. It originated at Microsoft, was subsequently standardized, and is mostly used to connect enterprise business software to external data providers: some examples are [Salesforce Connect external data sources](https://help.salesforce.com/articleView?id=salesforce_connect_odata.htm), [Microsoft SharePoint with Business Connectivity Services](https://docs.microsoft.com/en-us/sharepoint/dev/general-development/using-odata-sources-with-business-connectivity-services-in-sharepoint), and [Microsoft Dynamics virtual entities](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/customize/create-edit-virtual-entities).

OData can be used from a browser. The following sections list details of Becquerel's OData implementation along with some useful URLs.

## Service documents

The service document ([`http://localhost:9000/bq/`](http://localhost:9000/bq/) if running locally with a service named `bq`) lists all of the OData entity collections for a service, which are roughly semantically equivalent to database tables. You can get this in either JSON or XML formats by changing the HTTP `Accept` header, or by appending `?$format=json` or `?$format=xml`.

## Queries

Let's say you have a service named `bq` running locally, backed by a BQ dataset named `dvdstore` with a table named `categories`. Becquerel will publish that table at [`http://localhost:9000/bq/dvdstore__categories`](http://localhost:9000/bq/dvdstore__categories), and querying that URL will return data from that table as an OData document. Becquerel supports the following OData system query options:

- [`$filter`](http://www.odata.org/getting-started/basic-tutorial/#filter): equivalent to SQL `WHERE`.
- [`$orderby`](http://www.odata.org/getting-started/basic-tutorial/#orderby): equivalent to SQL `ORDER BY`.
- [`$top` and `$skip`](http://www.odata.org/getting-started/basic-tutorial/#topskip): equivalent to SQL `LIMIT` and `OFFSET`.
- [`$select`](http://www.odata.org/getting-started/basic-tutorial/#select): equivalent to SQL `SELECT`.
- [`$search`](http://www.odata.org/getting-started/basic-tutorial/#search): full-text search. `$search` has no standard SQL equivalent, and in fact is emulated using `LIKE` for SQL data sources, but maps naturally to a subset of [ES query strings](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html).

Becquerel does not currently support the `$count` or `$expand` system query options, and supports a limited set of string functions for `$filter`/`$orderby`: `contains()`, `startswith()`, `endswith()`, `tolower()`, and `toupper()`.

## Metadata

OData is typed and Becquerel thus uses metadata such as table schemas from its data sources to generate OData entity types. Becquerel can map most SQL and ES primitive types to equivalent OData primitive types, and can map array/list and struct/record types to OData collection and complex types respectively. It does not support entity relationships (BQ and ES don't have foreign keys anyway).

The metadata document ([`http://localhost:9000/bq/$metadata`](http://localhost:9000/bq/$metadata) if running locally with a service named `bq`) lists all of the OData types for a service, which include entity types generated from tables, as well as complex types generated from any struct columns used by those tables. This is an [OData CSDL](https://docs.oasis-open.org/odata/odata/v4.0/odata-v4.0-part3-csdl.html) document and is only available in XML.

### Collection naming

OData entity collection names are built from the parts of the data source table identifier glued together with a double underscore (`__`), with runs of non-alphanumeric characters replaced with a single underscore (`_`). For example:

- The BQ table `gcp-example-project:dvdstore.categories` becomes `gcp_example_project__dvdstore__categories` with `omitProjectID` off, and `dvdstore__categories` with `omitProjectID` on.
- ES index names are used directly since they're in a flat namespace already, so `categories` is still `categories`. (Mapping types are ignored and are [deprecated in newer ES versions](https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html#removal-of-types).)
- The PG table `ds2.public.categories` (in database `ds2` with the default `public` schema) becomes `ds2__public__categories` with `omitCatalogID` off, and `public__categories` with `omitCatalogID` on.

### Primary keys

All OData entity types [*must* have a unique primary key (PK)](http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_Toc453752555), which is used to, among other things, form the canonical URL for an entity. Although Becquerel itself doesn't use the PK for anything, and can serve entity collections with non-unique PKs, this is behavior specifically disallowed by the OData standard, and your OData consumer may behave erratically when it can't tell two entities with the same PK apart.

The SQL backends will use the first column of the table as the primary key (since BQ doesn't have a notion of primary keys, or for that matter, any kind of index, and not all JDBC-compatible SQL databases will necessarily have a PK on every table), so *your SQL tables must start with a column containing unique values*. The ES backend will use the ES document ID, which is guaranteed to be unique.

### Fetching metadata

Rather than fetch table schemas with every query, or fetch them and cache them as new tables are requested, Becquerel fetches them ahead of time, at app startup and then every 10 minutes afterward (by default). This work is done on the `metadataRefresh` execution context, which is backed by a fixed-size thread pool. You can change the thread pool size in the `contexts` section of the config file. Note that OData requests won't work until the metadata has been fetched, but the status page will.

Multiple Becquerel instances do not share state. If launching multiple instances at once, you may want to set `metadataRefreshDelayMax` for your service in your config file: each instance will wait a random amount of time between zero seconds and the value of `metadataRefreshDelayMax` from the config file before fetching metadata. This will reduce the load on the data source, and in the case of BQ, reduce the chance of exhausting the [BQ API call quota](https://cloud.google.com/bigquery/quotas#apirequests).

# Architecture notes

Becquerel is a web service written in Scala and uses the [Play 2.5 webapp framework](https://www.playframework.com/documentation/2.5.x/ScalaHome), along with [Apache Olingo](https://olingo.apache.org/) 4 for OData support, and [Apache Calcite](https://calcite.apache.org/) for SQL manipulation. It provides an OData interface to several different kinds of data source, including Google BigQuery, Elasticsearch 5.x, and JDBC-compatible SQL databases, and translates OData requests to queries on the underlying data source, then translates the query results to OData responses.

## OData implementation

Becquerel uses [Apache Olingo](https://olingo.apache.org/) to parse OData requests and serialize responses as OData entities. [Olingo's tutorial](https://olingo.apache.org/doc/odata4/index.html) is quite good and much of the OData interface code is adapted from it. [`BecquerelServiceEntityCollectionProcessor`](app/com/thumbtack/becquerel/BecquerelServiceEntityCollectionProcessor.scala) and [`DataSourceMetadata`](app/com/thumbtack/becquerel/datasources/DataSourceMetadata.scala) are the two main interface points with Olingo, for data and metadata respectively.

Olingo's HTTP handler uses the Servlet API, and is therefore synchronous and not very Play-friendly. Rather than implement a new Olingo handler type on top of Akka streams, Becquerel has the [`PlayRequestAdapter`](app/com/thumbtack/becquerel/util/PlayRequestAdapter.scala) and [`PlayResponseAdapter`](app/com/thumbtack/becquerel/util/PlayResponseAdapter.scala) classes which buffer requests and responses in memory, and provide just enough of the Servlet `HttpServletRequest`/`HttpServletResponse` API for Olingo to work.

Since Olingo's HTTP handler, Google's BigQuery client library, and JDBC all do blocking I/O, Becquerel uses a large fixed-size thread pool for Play's default execution context, based on [Play's tuning guidelines for highly synchronous operations](https://www.playframework.com/documentation/2.5.x/ThreadPools#Highly-synchronous). The default size of 50 threads was based on load testing in Becquerel's production environment, and the optimal size may be different for your deployment.

## Status page

Becquerel's index page ([`http://localhost:9000/`](http://localhost:9000/) if running locally) displays the Git revision and Jenkins build info (in Jenkins builds only), the Play environment mode, info for each configured OData service, environment variables, request headers, Java runtime stats for CPUs and memory, and Java properties. Variables known to contain passwords or credentials can be optionally be censored (see the `censor` section of the config file), so you'll only see the first 4 characters followed by an ellipsis (…).

## Logging

Becquerel logs to `stdout` by default. If you're using something like [Logstash](https://www.elastic.co/products/logstash) or [Fluentd](https://www.fluentd.org/) that can handle structured logging, edit `conf/logback.xml` and select [`logstash-logback-encoder`](https://github.com/logstash/logstash-logback-encoder) to produce JSON logs instead of the default plain-text logs. This file is also where you'd change the logging destination.

All log entries resulting from HTTP requests are tagged with a "run ID" for tracing purposes, which is normally a random [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier), but may also be provided by setting a `Run-ID: XXX` header in the HTTP request. This is handled by [`RunIDFilter`](app/com/thumbtack/becquerel/filters/RunIDFilter.scala) and propagated to the logging library by [`MDCPropagatingDispatcher`](app/com/thumbtack/becquerel/util/MDCPropagatingDispatcher.scala) and [`MDCPropagatingExecutionContext`](app/com/thumbtack/becquerel/util/MDCPropagatingExecutionContext.scala). The HTTP response will also contain a `Run-ID: XXX` header, and any SQL queries will contain a `-- run_id: XXX` comment.

## Metrics

Becquerel uses [`breadfan`'s fork of the `metrics-play` library](https://github.com/breadfan/metrics-play), which is a convenient wrapper for [the DropWizard Metrics library](http://metrics.dropwizard.io/). It can send these to InfluxDB using [`dropwizard-metrics-influxdb`](https://github.com/iZettle/dropwizard-metrics-influxdb).

The `metrics-play` library provides a [metrics controller](https://github.com/breadfan/metrics-play#metrics-controller) that shows all registered metrics as a JSON document ([`http://localhost:9000/metrics`](http://localhost:9000/metrics) if running locally). Note that not all metrics are registered at app startup; some may not show up on the metrics page until the first OData request is served.

## Security considerations

Becquerel has no concept of authentication or authorization itself. In its production configuration at Thumbtack, authentication is between Salesforce and an HTTP reverse proxy in front of Becquerel. Salesforce has a single set of service account credentials, decides which of its users can access which external objects through Becquerel, and makes authenticated HTTP requests with its credentials on their behalf. Becquerel itself assumes that any request that reaches it through the proxy is authenticated by the proxy as being Salesforce, and that Salesforce authorized the Salesforce user to perform that action.

The BQ data source uses a single service account to connect to BQ, as does the JDBC data source (for databases that support authentication). The ES data source doesn't support authentication at all, because vanilla ES deployments don't either. Becquerel can thus potentially access any table its service accounts can access; although the BQ and ES backends can be configured to allow access to only certain tables, this will not help you if the data source credentials are somehow leaked.

# Limitations

Becquerel has only been used in production with Salesforce thus far. It may or may not work with your particular OData provider. If you get it working with something new, please let us know! Patches and feedback are welcome.

Becquerel is developed on macOS, and built and deployed on Linux. It will probably work on any modern UNIX-like OS, but has not been tested on Windows, although there's no obvious reason it wouldn't run. Again, please let us know if you run it on a new OS!

Becquerel does not currently support access to individual OData entities or entity properties. Salesforce doesn't use this capability, so we haven't added it yet, although it's unlikely to be very complex to implement.

Becquerel is not capable of streaming massive result sets yet, and retains them in memory while generating a response. This is an edge case, and we recommend you use OData's paging features, instead of executing queries so broad that the results don't fit in available RAM. In practice, this is only likely to come up if you try to load an entire table.

Becquerel is a *read-only* implementation of OData, and can't modify, add, or delete data in its data sources. This was out of scope for Thumbtack's customer support use case: reading customer data from replicas on our data platform is a fairly generic operation, but changing it requires specific business logic that didn't fit well into the OData model.

# Alternatives

## WSO2 DSS

Thumbtack briefly evaluated [WSO2 DSS](https://wso2.com/products/data-services-server/) but found it less capable than we'd hoped:

At the time of evaluation, it did not connect to BQ natively, or work with Elasticsearch at all, and was unable to publish BQ tables over OData: we attempted to use [the closed-source BigQuery JDBC driver](https://cloud.google.com/bigquery/partners/simba-drivers/), and ran into the issue that BQ doesn't have any notion of primary keys but OData requires them, and WSO2 DSS had no way to specify them out of band, or in code, as Becquerel does with its SQL data sources.

Finally, WSO2 DSS is rather difficult to configure or modify. Their preferred scenario involves a custom Eclipse fork and an XML DSL. Becquerel is only about six thousand lines of Scala code.

WSO2 DSS might be a good option if you only need to talk to conventional SQL databases with conservative schemas and available JDBC drivers.
