# Spark Connect

Flowman can run against a [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) endpoint.
In this mode the Flowman client only communicates with the Spark server via the Spark Connect protocol and does not
have access to the Spark driver's `SparkContext` or `SessionState`.

## Client mode

Start Flowman with `spark.api.mode=connect` and point it to the Connect endpoint:

```bash
spark-submit \
  --conf spark.api.mode=connect \
  --conf spark.remote=sc://spark-connect-server:15002 \
  ...
```

The client-side Flowman libraries do not include the Spark Connect client JVM shims (`spark-connect-shims`); those
must come from the regular Spark installation.

## Server plugin (optional)

The `flowman-connect-server` module provides an optional server-side plugin which restores classic-like behavior for
features that cannot be expressed through the public Spark Connect client API:

* enriched `describe` results (CHAR/VARCHAR lengths and column descriptions),
* the `nullable_struct` expression including metadata propagation,
* server-side capability probes (e.g. multi-path support of data sources).

The plugin is optional; the client detects it at runtime and otherwise falls back to the public API. Note that the
plugin requires Spark 4.x.

### Installation

The build produces a self-contained plugin jar with the `server` classifier
(`flowman-connect-server_2.13-<version>-server.jar`). It bundles only the Flowman classes; Spark, Hadoop, Scala and
all other third-party libraries are expected to be provided by the Spark installation. Copy that single jar into the
`jars` directory of the Spark Connect server (or add it to its classpath):

```bash
cp flowman-connect-server_2.13-<version>-server.jar $SPARK_HOME/jars/
```

Alternatively, the regular module jar can be used together with the `flowman-connect-common`,
`flowman-spark-extensions` and `flowman-core` jars.

The plugin classes are registered via the standard Spark Connect plugin service loader mechanism
(`META-INF/services`). Restart the Spark Connect server after installing the jar.

The client and server exchange a protocol version during the handshake. If the versions do not match, the client
disables all plugin features and logs a warning; Flowman then continues to work with the public Spark Connect API.

### Limitations

* External catalogs (for example Impala) are not available in Spark Connect mode; Flowman logs a warning and ignores
  them.
* Server-side file system operations are not supported.
