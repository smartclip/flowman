# Running Flowman with Spark Connect

Flowman can drive Spark either through the classic in-process API (`SparkContext`) or through
[Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html). In Connect mode the Flowman client
only talks to a remote Spark Connect server over gRPC; it has no `SparkContext`, no `SessionState` and no RDDs. This
page describes how to set up both sides and which features behave differently.

## Enabling Connect mode

Connect mode is selected by Spark itself: when `spark.api.mode=connect` is set, `SparkSession.builder()` creates a
Spark Connect client session instead of a classic one. Flowman picks up the resulting session transparently.

Add the following entries to the `config` section of your namespace or project YAML (or pass them as system
properties):

```yaml
config:
  - spark.api.mode=connect
  - spark.connect.remote=sc://spark-connect.example.com:15002
```

The same properties can be set via `SPARK_REMOTE` or the usual `-D` options when running the Flowman CLI.

## Setting up the Spark Connect server

The server must run Spark 4.1 (the version Flowman's Connect build is compiled against). Start it with the standard
Spark distribution:

```shell
sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.1.0
```

### Installing the Flowman server plugin

Some Flowman features cannot be expressed with the public Spark Connect API alone (enriched schema descriptions with
CHAR/VARCHAR lineage and comments, `nullable_struct` with classic semantics, server-side file-system checks and data
source capability probes). These are provided by the `flowman-connect-server` module, which must be installed on the
**server** classpath (for example in `$SPARK_HOME/jars`) together with `flowman-connect-common` and its dependencies.

Then register the plugin in the Spark Connect server configuration:

```properties
spark.sql.extensions=com.dimajix.flowman.connect.server.FlowmanSparkSessionExtensions
spark.connect.extensions.relation.classes=com.dimajix.flowman.connect.server.FlowmanRelationPlugin
spark.connect.extensions.expression.classes=com.dimajix.flowman.connect.server.FlowmanExpressionPlugin
spark.connect.extensions.command.classes=com.dimajix.flowman.connect.server.FlowmanCommandPlugin
```

The client detects the plugin with a version handshake (`FlowmanPing`). If the plugin is missing or the protocol
versions do not match, Flowman logs which features are disabled and falls back to the public client API. It does not
fail.

### Third-party data sources

Data sources that Flowman plugins provide (Delta, custom formats, …) execute on the **server**. Their jars (including the
matching Spark version of the data source, e.g. `delta-spark`) must therefore be available on the server classpath.
The client-side plugin jars are still needed for the Flowman relation/mapping specs and for direct database or
filesystem access.

## Client setup

The client needs the usual Flowman distribution, including the plugins used by your project. In addition:

* **Storage credentials**: Flowman performs dirty checks (file existence, partition discovery) from the client. The
  client therefore needs access to the storage systems, unless the Flowman server plugin is installed, in which case
  the file-system checks run on the server (`FlowmanFsCommand`).
* **JDBC drivers**: JDBC connections (DDL, merge, state stores) are opened by the client, so the drivers must be on
  the client classpath as usual. The Spark JDBC writer used to load data runs on the server, which needs the driver
  as well.
* **Static Spark configuration**: static settings such as `spark.sql.warehouse.dir` cannot be changed from a Connect
  client. Set them on the server; the client must not carry them in its `SparkConf`.

## Feature differences

| Feature | Classic mode | Connect mode |
|---|---|---|
| Record counters (`CountRecords`) | Accumulator on the driver | `df.count()` into a gauge (server accumulators are not readable by the client) |
| Schema descriptions | Analyzed plan on the driver | Plain `df.schema`, or enriched via the server plugin |
| Column lineage | Analyzed plan | Not available (logged and skipped) |
| `nullable_struct` | Driver-side expression | Server plugin, falling back to the public implementation |
| Custom-format counters | Spark accumulators | Not available (counters live on the server); the read itself works |
| JDBC merge/upsert | RDD-based per-partition merge | Temporary staging table + SQL `MERGE` |
| Structured streaming helpers | Full support | Not yet supported (tests are cancelled in Connect mode) |

Flowman's own test suite runs in both modes. Use the Maven profile `-Pspark-connect` to execute all tests against an
in-process Connect server. Projects that inherit from `flowman-parent` get the `spark.api.mode` system property
automatically and can switch with the same profile or `-Dflowman.test.spark.api.mode=connect`.

## Known limitations

* Spark 4.1's single-pass analyzer is still under development. If a server runs into relation-resolution problems
  with third-party catalogs, set `spark.sql.analyzer.singlePassResolver.enabled=false` (and
  `spark.sql.analyzer.singlePassResolver.enabledTentatively=false`) to use the fixed-point analyzer.
* Spark Connect serializes schemas as DDL. Field metadata (for example default values) does not survive the round
  trip, and Spark 4.1 cannot parse a `DEFAULT` clause it emits itself.
* `SparkSession.stop()` permanently invalidates a Connect session. Flowman never reuses a stopped Connect session and
  creates a fresh one per Flowman session.
