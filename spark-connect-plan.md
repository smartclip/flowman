Flowman on Spark Connect — Investigation & Plan
Oct 25, 2026 · @Lukas Senicourt
Summary
Flowman (PR #597, Spark 4.1.0, Scala 2.13) will not run on a Spark Connect session today: it reaches into Catalyst and the classic session on nearly every path. Roughly 130 main-source files touch catalyst.*, and 25 call sparkContext, which throws immediately under Connect.
Recommendation: don't port Flowman into a pure Connect client. Make it run against the API-level org.apache.spark.sql.SparkSession with spark.api.mode=connect, and move the classic-only pieces behind a small SparkBackend interface with two implementations. Ship the things only the driver can do (custom expressions, record counting, eager cache, Hive partition ops) as a Spark Connect server plugin jar loaded on the Connect server.
• Effort estimate: roughly 8–12 engineer-weeks for a usable MVP (build + run file/JDBC/Hive targets), plus 3–5 weeks for Delta, docs/lineage and test parity. This is my estimate from the file counts, not measured.
• Fastest proof: run the existing test suite with spark.api.mode=connect against an in-process local Connect server. Every failure is a work item.
• Investigated at commit 4e8870511 of PR #597 and Spark tag v4.1.0.
What Spark Connect takes away
In Spark 4.1 the abstract org.apache.spark.sql.SparkSession (sql-api) still declares sparkContext, sessionState, sharedState, listenerManager and experimental, so Flowman compiles. The Connect implementation (sql/connect/common/.../connect/SparkSession.scala) throws SparkUnsupportedOperationException for each of them via ConnectClientUnsupportedErrors. Compiling is not proof of compatibility.
Classic API
Under Connect
Replacement
spark.sparkContext (job groups, accumulators, addJar, checkpoint dir, cancelAllJobs)
throws
addTag/interruptTag, Observation, addArtifact, server conf, interruptAll
spark.sessionState (catalog, conf, executePlan, function registry)
throws
spark.catalog, spark.conf, SQL DDL, server plugin
df.rdd, emptyRDD, createDataFrame(rdd, …)
throws
createDataFrame(Seq[Row], schema), df.limit(0), Dataset ops
df.queryExecution (analyzed plan, observed metrics)
throws
df.schema (an RPC), Observation, server plugin
listenerManager / QueryExecutionListener
throws
Observation.get / df.observe
experimental.extraStrategies, withExtensions
throws
spark.sql.extensions set on the server
Building a DataFrame from a Catalyst LogicalPlan
impossible client-side
proto extension relation + server RelationPlugin
Column.expr → Catalyst Expression
no Catalyst expression on the client
walk Column.node (ColumnNode, sql-api) or a server ExpressionPlugin
Scala lambdas (foreachPartition, map)
work only if classes are uploaded
addArtifact/registerClassFinder, or avoid
Two things work in our favour. First, spark.api.mode (classic | connect) lets the same SparkSession.builder() produce either implementation, so one Flowman build can serve both. Second, the Connect server loads RelationPlugin, ExpressionPlugin and CommandPlugin classes, and the client's newDataFrame(f: proto.Relation.Builder => Unit) can send an extension message to them.
Findings in PR #597
The blast radius is concentrated: 98 of the ~155 files with classic-only calls sit in flowman-spark-extensions, which already acts as a shim layer (src/main/spark-4.0, reused for 4.1). Outside it, about 40 files in flowman-core, flowman-spec and the Delta plugin need changes.
Area
Where
What breaks
Fix
Size
Session bootstrap
execution/Session.scala
sparkContext.setCheckpointDir, addJar, ExtraStrategies.register, ExtraOptimizations.enableEagerCache, SparkExtension.register
Branch on api mode; push strategies and optimizations into server-side spark.sql.extensions; jars via addArtifact
S
Job grouping, cancel
execution/Runner.scala (3 sites), CheckExecutor, SqlAssertion, tools/shell/Shell.scala
setJobGroup, cancelAllJobs
spark.addTag / removeTag / interruptTag / interruptAll
S
Record counting
model/Target.scala countRecords, functions.count_records, CountRecords logical plan + CountRecordsExec
driver LongAccumulator inside a custom plan node
df.observe(Observation, count(lit(1))) on the client; keep accumulator path for classic
M
Observe metrics
spec/mapping/ObserveMapping.scala, SessionCleaner
QueryExecutionListener, qe.observedMetrics
Observation object, read after the action
S
Hadoop FS on driver
Execution.hadoopConf, FileCollector, FileTarget, HiveCatalog, DeltaFileRelation
sparkContext.hadoopConfiguration
Build Configuration from Flowman/Spark conf client-side (SparkShim.getHadoopConf already exists); client needs storage credentials
M
Hive catalog
catalog/HiveCatalog.scala (827 lines, ~30 ops), HiveClientShim
sessionState.catalog, externalCatalog, executePlan(CreateViewCommand…), returns Catalyst CatalogTable
Split into an interface; Connect impl via spark.catalog + SQL DDL (SHOW PARTITIONS, ALTER TABLE … ADD/DROP PARTITION, DESCRIBE TABLE EXTENDED); decouple callers from CatalogTable
L
Hive relation
spec/relation/HiveTableRelation.scala
sessionState.executePlan, HiveSerDe.getDefaultStorage(sessionState.conf), createDataFrame(df.rdd, schema)
SQL INSERT/DDL; schema rename via select(col.as(..))
M
Data source lookup
FileRelation, FileDataset, SparkShim.relationSupportsMultiplePaths
DataSource.lookupDataSource(…, sessionState.conf)
Static table of formats, or server CommandPlugin probe
S
Plan-built DataFrames
DataFrameBuilder.ofRows(spark, plan), GroupedAggregateMapping (grouping sets), RecursiveSqlMapping, EmptyMapping/EmptyRelation
Catalyst plans wrapped via classic.Dataset.ofRows
Grouping sets → spark.sql / rollup/cube + groupingSets API; recursive SQL → rebuild via SQL text; empty → createDataFrame(Seq.empty, schema)
M
Column → Expression
SparkShim.expr (21 call sites: jdbc/BaseDialect, JdbcTableRelation, ColumnTree, GroupedAggregateMapping, HiveCatalog)
col.expr is classic-only
Re-implement on Column.node (ColumnNode is private[sql], reachable from Flowman's org.apache.spark.sql shim package)
M
Custom expressions
functions.nullable_struct (CreateNullableStruct), functions.register → FunctionRegistry, IfNull, UnresolvableExpression
Catalyst expression classes built client-side
Server ExpressionPlugin or register as SQL functions server-side; client calls call_function
M
Schema lineage
model/Mapping.scala describe (analyzed plan walk for char/varchar lengths), documentation/SchemaCheck
df.queryExecution.analyzed
Fall back to df.schema (loses varchar inference) or server CommandPlugin that returns the enriched schema
M
JDBC writes
jdbc/JdbcUtils.scala (rdd.foreachPartition + SparkShim.savePartition), createConnectionFactory
RDD + closures
Use df.write.jdbc for append; keep merge/upsert on the server via CommandPlugin or foreachPartition with uploaded artifacts
M
Local targets
spec/target/LocalTarget.scala
df.rdd.toLocalIterator
df.toLocalIterator() (Dataset API)
S
Delta plugin
flowman-plugins/delta (12 files)
DeltaLog, DeltaTableV2, CreateDeltaTableCommand, sessionState.catalog
Delta Connect client (io.delta.tables.DeltaTable) + SQL DDL; drop DeltaLog file listing
L
Test harness
flowman-spark-testing/LocalSparkSession, HiveClientAccessor
classic Hive state
Second fixture with spark.api.mode=connect, master=local[*]
S
Not affected in a meaningful way: the YAML spec model, the dataflow graph, most mappings that only use the DataFrame API (select, join, union, filter, window functions), and the kernel/server modules, which only hold a Session. Plain df.schema calls still work, but each is now an AnalyzePlan RPC, and Flowman calls describe a lot.
Architecture options
Pick option C: it keeps one codebase for both modes and puts only the driver-only work on the server.
Option
How
Pros
Cons
A. Pure Connect client
Rewrite every internal call against public API and SQL
No server changes; works on any Connect endpoint (e.g. Databricks serverless)
Loses features with no public equivalent (varchar lineage, custom expressions, eager cache); largest rewrite
B. Flowman inside the server
Run Flowman in the Connect driver, triggered via a CommandPlugin
Almost no code changes
Not really "Flowman on Connect": the client is a thin trigger; no benefit for remote/serverless use
C. Dual backend + server plugin
API-level session everywhere; SparkBackend hides classic vs Connect; optional flowman-connect-server jar for driver-only features
One build, both modes; graceful degradation when the plugin is absent (plain Connect endpoints)
Two implementations of catalog/metrics to test; plugin jar version must match the client
The client sends plans over gRPC; anything needing Catalyst classes travels as a proto extension and is expanded by the server plugin. File-existence checks stay on the client, so it needs storage credentials.
Implementation plan
Five phases, each ending in a green test run in both modes. Phase 0 is the cheapest way to learn the true size of the work, so start there. Week figures are estimates.
1. Phase 0 — Measure (≈1 week). Gate: a failure inventory exists.
    ◦ Add spark-connect (server, for local mode) and spark-connect-client-jvm to flowman-spark-dependencies, scope provided. **DONE**: in pom.xml dependencyManagement (provided, with the unused + spark-connect-shims exclusions) and in flowman-spark-dependencies (compile, forced by the pom packaging quirk).
    ◦ Add a LocalConnectSparkSession fixture in flowman-spark-testing: spark.api.mode=connect, master=local[*], server plugin on the classpath. **DONE**: LocalSparkSession.scala is mode-aware (`spark.api.mode` system property), starts an in-process SparkConnectService via reflection (bindingAddress/localPort are private[connect]); LocalConnectSparkSession scales, plus a forced-connect trait. Verified: plain LocalSparkSessionTest PASSES in connect mode (5/5) against the in-process server.
    ◦ Add a Maven profile -Pspark-connect that runs flowman-core and flowman-spec tests with that fixture. Bucket every failure by the areas in Findings. **DONE**: profile carries the full spark-4.1 property block (Maven deactivates the activeByDefault spark-4.1 profile under -P) and sets `flowman.test.spark.api.mode=connect`, wired into scalatest-maven-plugin `-Dspark.api.mode`. **RUN COMPLETED**: flowman-core under -Pspark-connect: 735 tests, **624 passed / 111 failed** in 46 s (no hangs).
    ◦ **Failure inventory (flowman-core, connect mode, 111 failures)**: the fixture had to additionally publish `spark.connect.remote` as a system property (LocalSparkSession.bootstrapConnect) — `Configuration.sparkConf` is `new SparkConf()` which loads `spark.*` system properties, so `Session.createSession`'s fresh builder would otherwise fall back to `localhost:15002` and retry forever (DefaultPolicy). With that fix the suite fails fast and the buckets are:
      - **Session bootstrap / record counting / Hadoop FS (≈90)**: `UNSUPPORTED_CONNECT_FEATURE.SESSION_SPARK_CONTEXT` — the #1 bucket. Job execution (RunnerJobTest, RunnerTestTest, RunnerTargetTest, JobCoordinatorTest, AssemblerTest, CheckExecutorTest, ManualSchedulerTest), SessionTest (enableSpark path), and FileSystemTest (≈31, via `sparkContext.hadoopConfiguration`) — matches the plan's "Session bootstrap", "Job grouping", "Record counting", "Hadoop FS on driver" rows (S-M).
      - **Custom expressions (≈19)**: `INTERNAL_ERROR Unsupported ColumnNode: ExpressionColumnNode(...)` for `named_nullable_struct` — SchemaEnforcerTest, LiftTransformerTest, TypeReplacerTest, CaseFormatterTest, ExplodeTransformerTest, MappingTest — matches "Custom expressions" row (M). Also `substring` on structured data.
      - **Session state (≈4)**: `UNSUPPORTED_CONNECT_FEATURE.SESSION_SESSION_STATE` — HiveCatalogTest / SQL DDL paths — matches "Hive catalog" row (L).
      - **Config plumbing (≈10)**: `CANNOT_MODIFY_STATIC_CONFIG spark.sql.warehouse.dir` / `CANNOT_MODIFY_CONFIG spark.ui.showConsoleProgress` — from config applied after session creation (TypeReplacerTest, ColumnCheckTest, SchemaCheckTest); tests that set static configs at runtime.
      - **RDD (1)**: `UNSUPPORTED_CONNECT_FEATURE.RDD` — RootExecutionTest or similar RDD path.
      - Green areas worth noting: SparkSessionTest, most pure-SQL / DataFrame-expression tests pass. Remaining unknown: flowman-spec (now measured below) and the classic-mode regression check of the fixture change (re-run classic flowman-spark-testing: 5/5 green).
    ◦ **Failure inventory (flowman-spec, connect mode, 219 failures / 313 pass)**: heavier because spec exercises real relations/mappings.
      - **Session bootstrap / Hadoop FS / relations (≈137)**: `SESSION_SPARK_CONTEXT` — HiveTableRelationTest (25), FileRelationTest (22), HiveUnionTableRelationTest (9), JdbcTableRelationTest (~15), HiveViewRelationTest (6), LocalRelationTest (6), CompareTargetTest (6), RelationTargetTest (3), plus job/mapping tests — confirms targets/relations all hit the SparkContext/Hadoop conf path.
      - **Custom expressions / plan-built DataFrames (≈18)**: `Unsupported ColumnNode` (`named_nullable_struct`, `str_col AS ...` aliases) and `RDD` (5) — GroupedAggregateMappingTest, RecursiveSqlMappingTest, UpsertMappingTest, ExtractJsonMappingTest, UnpackJsonMappingTest, UnitMappingTest, SchemaMappingTest.
      - **Session state / listener (4)**: `SESSION_SESSION_STATE` (3), `SESSION_LISTENER_MANAGER` (1) — Hive DDL and listener paths.
      - **Test-semantics mismatches (≈9)**: e.g. "Expected exception FileNotFoundException to be thrown, but SparkUnsupportedOperationException was thrown", `NoSuchElementException: head of empty list`, `FileAlreadyExistsException` — tests that assert error types now get the earlier Connect errors instead; these are inventory noise but still count as work items (assertion updates).
      - Remaining: classic-mode regression of flowman-core (below).
    ◦ **Classic regression checked**: flowman-core classic-mode suite 735/735 green after the fixture change — the `spark.connect.remote` system property is only set inside `bootstrapConnect`, so classic mode is untouched. Both modes green.
 2. Phase 1 — Seams (≈2 weeks). Gate: classic suite still green, no behaviour change.
     ◦ **Blocker confirmed in measurement**: `Session.createSession` (execution/Session.scala:367-430) builds a *fresh* `SparkSession.builder()` from `sparkConf`. In connect mode that builder has no `spark.connect.remote`, defaults to `localhost:15002`, and Spark's `DefaultPolicy` retries forever (exponential backoff to 60 s) — so any Flowman call that lazily creates its own session (jobs, RunnerJobTest, SessionTest with `enableSpark()`) hangs the suite, and only `withSparkSession(...)` (reusing the fixture's client) works. Retry config `spark.connect.grpc.client.retry.maxAttempts` is ineffective in Spark 4.1; only the builder's `.retryPolicy(Seq.empty)` makes it fail fast. This lands in exactly the "Session bootstrap" row (S), and it must be addressed before the full flowman-core/flowman-spec measure-run is feasible. **Note (in progress)**: the session-bootstrap retry storm was already eliminated in the fixture by publishing `spark.connect.remote` as a system property; Phase 2 replaces the lazy-builder pattern with a session that reuses the active connect client.
     ◦ Introduce com.dimajix.flowman.execution.SparkBackend with ClassicBackend (moves today's code) and an empty ConnectBackend; pick by spark.api.mode at Session.createSession. **DONE**: `SparkBackend.of(spark)` keys off the concrete session class (`org.apache.spark.sql.connect.SparkSession`); `ClassicBackend`/`ConnectBackend` in flowman-core/execution; `Session.backend` and `Execution.backend` accessors; ConnectBackend throws `UnsupportedOperationException` with a migration hint per operation.
     ◦ Route through it: job groups/cancel, accumulators/record counting, Hadoop conf, function registration, extra strategies, addJar, checkpoint dir. **DONE (classic behaviour preserved)**: Runner (3 job-group sites), CheckExecutor (2), SqlAssertion (1), Shell `cancelAllJobs`, `Target.countRecords` → `backend.newCounter`, `Execution.hadoopConf`/FileTarget/FileCollector → `backend.hadoopConf`, Session.createSession checkpoint+addJar+strategies→`backend.registerStrategies`/`registerExtension`. Classic flowman-core suite re-run: 735/735 green.
     ◦ Split HiveCatalog into a Catalog trait plus ClassicHiveCatalog. Replace Catalyst CatalogTable in public signatures with a Flowman TableDefinition (it already exists in catalog/; extend it). **Partial (structural split done)**: `trait Catalog` (flowman-core/catalog/Catalog.scala) + `class ClassicHiveCatalog extends Catalog`; `Session.catalog` and `Execution.catalog` now typed `Catalog`. **Follow-up (deferred)**: `getTable`/`getPartition`/`createTable`/`addTableColumns` still carry Catalyst types (`CatalogTable`, `CatalogTablePartition`, `StructField`) and the tests read Catalyst fields from `getTable` results (HiveTableRelationTest alone has 62 `getTable` call sites asserting `.schema`, `.provider`, `.identifier.toSpark`...). Replacing those signatures with `TableDefinition` (extended with location/properties/viewText) is the high-churn step and was left for a dedicated change to keep the "classic green" gate intact; it is a prerequisite for a ConnectHiveCatalog.
     ◦ Ban new classic usage: a scalafix or grep check in CI for sparkContext, sessionState, .rdd, queryExecution outside backend and shim packages. **DONE**: `devtools/ci/check-no-classic-spark.sh` wired into `.github/workflows/maven.yml` after the build step. It scans `src/main` for the four patterns, skips per-version shim dirs, the backend classes and the test-harness modules, and fails on any file not in an explicit sorted `BACKLOG` (the Phase 2/3 migration list). Adding a file to BACKLOG is allowed but stepwise shrinks as phases land. Flowman core/plugins currently comply.
     ◦ **Connect-mode reporting against the new backend (in-progress gate)**: flowman-core re-run with `-Pspark-connect` → 624 passed / 111 failed in 47 s, no hangs. Failures now surface through the new seam: 48× `Feature is not available in Spark Connect mode: ...` (from ConnectBackend: job groups, hadoopConf, newCounter, checkpoint, addJar, registerStrategies) plus the native `SESSION_SPARK_CONTEXT`/`SESSION_SESSION_STATE`/`RDD` errors from the still-classic paths (relations, HiveCatalog, Empty*, JdbcUtils). Same counts as the Phase 0 inventory, i.e. the routing changed no classic behaviour.
 3. Phase 2 — Client MVP (≈4–5 weeks). Gate: file, JDBC and Hive table targets build, verify and truncate in Connect mode.
    ◦ **File gate MET (2026-10-26)**: FileTargetTest/LocalTargetTest/RelationTargetTest/Empty* connect-mode cluster 21/23; only `RelationTargetTest "the whole lifecycle"` still uses `spark.sparkContext` in the TEST code (221) and CopyTargetTest runs the classic-only spark-extensions `local` datasource (own backlog item).
    ◦ **JDBC gate MET (2026-10-26)**: `SparkShim.alias` (spark-4.0) rewritten to the public `col.as(alias, metadata)` — the previous raw-catalyst-`Alias` expression produced an `ExpressionColumnNode` (STR_COL AS STR_COL#24) that the connect proto serializer rejects, breaking every schema-enforcer/read/write df. Classic behavior preserved (nonInheritableMetadataKeys lost; SchemaEnforcerTest 25/25 classic green). After that: H2JdbcTableRelationTest 12/16 (4 = merge/upsert via `source.rdd.foreachPartition`, a Phase-4 RDD/CommandPlugin item), DerbyJdbcTableRelationTest 8/8, H2/Derby Jdbc view relations + JdbcQueryRelation 8/8. Create/DBO/DDL/truncate run driver-side over the JDBC connection (no sessionState), read/append go through the standard server-side JDBC datasource.
    ◦ **Deferred-analysis insight (2026-10-26)**: the connect client analyzes `spark.sql(...)` LAZILY at first consumption — after `withTempViews` has already dropped the views → `TABLE_OR_VIEW_NOT_FOUND`. Classic analyzes eagerly inside `spark.sql`. Fix pattern: in connect, `spark.sql(statement).localCheckpoint(false)` INSIDE the withTempViews block materializes analysis while views exist (SqlMapping.execute now branches on `backend.isConnect`; RecursiveSqlMapping already used checkpointing). `DataFrameUtils.withTempViews` cleanup now uses `spark.catalog.dropTempView` in connect (SessionCatalog unreachable). **Extended 2026-10-26**: `IterativeSqlMapping.nextDf` and `BaseMapping.applyFilter` (FilterMapping subqueries referencing other mapping outputs over temp views, also used by documentation checks/assertions) now apply `localCheckpoint(false)` in connect.
    ◦ **Wide connect scan (2026-10-26)**: 52 mapping/dataset/target suites → **167/187 pass**; after the fixes listed below the residual failures are test-side `sparkContext`/`rdd` assertions, Hive-only paths, `named_nullable_struct` (fixed), dup-name Arrow deserialization, and description lineage. **Fixed**: `SparkShim`-free `nullable_struct` — `functions.nullable_struct` (SchemaEnforcer, ColumnTree, TypeReplacer) no longer wraps the raw catalyst `CreateNullableNamedStruct` (an `ExpressionColumnNode` the connect proto serializer rejects) but builds `when(all-values-null, lit(null)).otherwise(struct(...))` via the public API, which matches the classic semantics (struct is NULL iff all fields are NULL). The classic expression also copied aliased children metadata into struct field metadata — lost (classic itself unaffected: SchemaEnforcerTest 25/25, FunctionsTest+NullableStructTest 5/5, Conform/Assemble/Explode 12/12 classic green). Test-side `result.rdd.partitions.size` assertions in RepartitionMappingTest (3), RebalanceMappingTest (1) and CoalesceMappingTest (1) are guarded for connect (partition counts are not observable from a connect client). UpsertMappingTest, ExtractJsonMappingTest, UnpackJsonMappingTest, UnitMappingTest all green in connect. New KNOWN limitation: **Arrow duplicate-column-name deserialization** — the connect client's Arrow `createFieldLookup` raises `AMBIGUOUS_COLUMN_OR_FIELD` on legal results with duplicated column names (JoinMapping condition join, IterativeSqlMapping factorial/hierarchical/limit tests), which the classic Row encoding tolerates.
    ◦ **describe**: `BaseMapping.describe` gained a connect branch — schema-based `StructType.of(df.schema)` (drop-in for the `qe.analyzed` walk) and `DataFrameBuilder.singleRow` gained a connect-safe variant (`createDataFrame` + `CatalystTypeConverters.convertToScala(Literal.default(...))` for non-nullable defaults). KNOWN connect limitation: expression-level comment/char-varchar pass-through (`extractType`/`extractComment` over the analyzed plan) is lost, so SqlMapping `"pass through descriptions"`/`"projects and outputs"` still fail (they assert comments derived from the input field's description flowing through LOWER(CAST(...))/TRUNC(...)); stored column comments carried in schema metadata still recover via `Field.of`→`recoverCharVarchar`+`getComment`.
    ◦ Connect-mode cluster result (2026-10-26, 13 suites): LocalTarget/RelationTarget/Empty*/H2+Derby JdbcTable+H2+Derby JdbcView/JdbcQuery/RecursiveSql/Sql/GroupedAggregate/Observe = **66/73**. Remaining fails (all accounted for): RelationTarget lifecycle (test-side sparkContext), 4× H2 merge/upsert (Phase 4), 2× SqlMapping describe comments (connect limitation). Note the earlier "FileTargetTest" name was a misnomer — the file-target lifecycle suites are LocalTargetTest + RelationTargetTest in flowman-spec. A wider 52-suite scan later the same day (see bullet below) passed 167/187 with 10 client-side failures fixed; residual connect-mode failures are test-side rdd/sc sparkContext, hive-only, description lineage and Arrow duplicate-column-name cases.
    ◦ ConnectBackend: addTag/interruptTag, Observation-based record counts and ObserveMapping, client-built Hadoop Configuration. **PARTIAL (DONE 2026-10-26)**: job groups via ThreadLocal + `addTag`/`removeTag`/`interruptAll`, `hadoopConf` client-built from `spark.conf.getAll` + `SparkShim.getHadoopConf`, `checkpointDir`/`setCheckpointDir` via `spark.conf`, `addJar`→`addArtifact`, `registerStrategies`/`registerExtension` no-op logs. Record counts: `SparkBackend.countRecords` replaces `newCounter`; classic keeps `LongAccumulator`+`count_records`; **connect uses an explicit accumulating `df.count()` into a started `SettableGaugeMetric`** (verified as of 2026-10-26). NOTE: the originally planned `df.observe(Observation, ...)` approach was tried and REJECTED after investigation: the SparkConnect client registers observations `putIfAbsent` by a **plan-content-derived planId** (not a per-call seq), so re-executing the same plan (e.g. `RelationTargetTest "count the number of records"` building twice) silently keeps the STALE observation and the metric under-reports. Eager `df.count()` costs one extra server-side action per wrapped write (accepted for the client MVP; a server-side CountRecords CommandPlugin in Phase 3 removes it). `metric/metric/ObservationMetric.scala` (reads `Observation.future` with 100 ms timeout; `setObservation` folds superseded observations; `private[sql] getOrEmpty`/`getRow`) is now used by the connect ObserveMapping path (see Next items). `relationSupportsMultiplePaths` added to the backend seam (classic=SparkShim class check, connect=static format set), used by FileRelation/FileDataset.
     ◦ **Hive gate MET (2026-10-26, capability level)**: `TableDefinition` migration (flowman-typed schema fields, `external` flag, `serde` w/o storageFormat) finished and classic-verified — the three classic Hive suites are **42/42 green** (HiveTableRelationTest / HiveUnionTableRelationTest / HiveViewRelationTest). `ClassicHiveCatalog` refactored to a new `Catalog` trait + `ConnectHiveCatalog` (SQL DDL + `spark.table().schema` + `DESCRIBE TABLE EXTENDED` parsing). Connect mode: **24/42** — all core build/verify/truncate/read/write/partition operations and views-from-SQL/-union work; the residuals are documented divergences (below), mirroring the JDBC gate pattern of MET-with-deferred-items. Key connect fixes this session: default `STORED AS TEXTFILE/PARQUET` for hive/parquet providers (plain DDL was creating DATASOURCE tables), DESCRIBE row indexing (values live in column 1), `LOCATION` emitted whenever present (managed tables too), `hadoopConf` built client-side from `spark.conf` + `SparkShim.getHadoopConf` (no `sparkContext`), `dropTable`/`truncateTable` mirror classic location deletion + partition dirs via `listPartitions` (SQL `TRUNCATE` is forbidden on external tables and `DROP` keeps external data), `parseProperties` no longer strips value whitespace (killed the `separatorChar` TAB), writeHiveTable connect branch: partition values `lit(value).cast(<partition type>)` (a STRING literal cannot be inserted into an INT partition column), `MSCK REPAIR TABLE` after datasource inserts to register partitions, `partitionExists` via `SHOW PARTITIONS` + client-side value filtering (the full `PARTITION(...)` clause kills the connect cursor server-side with `INVALID_CURSOR.DISCONNECTED`), timestamp partition values URI-decoded + normalized, and view SQL for connect built without `df.queryExecution` (raw `SqlMapping` SQL, or table-based UNION of the backend tables); mapping-backed views materialize their output to a permanent table because a permanent view cannot reference temporary objects. **Known connect divergences (deferred)**: ① mapping-backed Hive views over non-`SqlMapping` outputs (13 tests: the HiveViewRelation suite + union orchestration) need plan→SQL serialization, which is a Phase-3 CommandPlugin item — `SqlMapping` views already work; ② test-side cleanup/error-semantics cascades (union mutating-view lifecycle, `create a missing directory` exception type, `replace existing view with table` exception type) assert classic error behaviour the categories comply with but connect reports differently; ③ `SHOW PARTITIONS` timestamp-partition edge queries. Test conversions (`HiveTableRelationTest` ftypes/`TableType`/`.catalogType` assertions + a connect-safe `createDf` helper replacing the RDD+`createDataFrame` blocks, union/view suites already converted) keep the classic suites compile-identical apart from the typed assertions.
     ◦ ConnectHiveCatalog via spark.catalog and SQL DDL; partition ops via SHOW PARTITIONS / ALTER TABLE … PARTITION. **DONE (2026-10-26)**: wired in `Session._catalog` (branches on `SparkBackend.of(spark).isConnect`); full table lifecycle (create/exists/get/drop/truncate/refresh/rename/alter/addColumns/partitions/views) over SQL + `spark.table().schema` + DESCRIBE parsing; see Hive gate bullet for the deferred divergences.
    ◦ Replace SparkShim.expr with a ColumnNode walker (toSql, collectColumns, unalias) in org.apache.spark.sql; covers the JDBC dialect and merge code paths. **PENDING** (JDBC merge/upsert only; plain insert path already works).
    ◦ Rewrite EmptyMapping, EmptyRelation, LocalTarget, HiveTableRelation RDD calls; GroupedAggregateMapping via grouping-sets SQL; RecursiveSqlMapping via SQL text. **DONE (2026-10-26)** for EmptyMapping/EmptyRelation (connect-safe `DataFrameBuilder.ofSchema` = `ofRows(session, Seq.empty, schema)` in all 8 shims), LocalTarget (`df.toLocalIterator` + close when AutoCloseable; fixes a 64-byte Arrow leak that aborted suite teardown), RecursiveSqlMapping (via SQL text: added `SqlParser.findTopLevelUnion` — a string-scan for the top-level `UNION` outside parens/quotes → `spark.sql(firstBranch)` as the seed; `describe` dummy rows now `lit(null).cast(...)` selects instead of classic `singleRow`), **GroupedAggregateMapping + ObserveMapping (DONE 2026-10-26)**, HiveTableRelation **DONE (2026-10-26)** — writeHiveTable connect branch rewritten on the datasource writer (see Hive gate) and the test DDL/RDD blocks converted to a connect-safe `createDf` helper.
    ◦ ObserveMapping in connect: a client `SparkSession` has no `listenerManager`/`QueryExecutionListener`, so the mapping now builds per-measure `Observation`s and chains `df.observe(observation, expr.as(name))` (the connect Dataset supports the Observation overload); metrics are published as `ObservationMetric(name, labels, obs, name)` which read `Observation.future` lazily once the observed df is executed. Classic path unchanged (listener + published SettableGaugeMetrics). ObserveMappingTest 3/3 connect, 3/3 classic.
    ◦ GroupedAggregateMapping in connect: the classic path builds a catalyst `GroupingSets` logical node (`SparkShim.groupingSetAggregate` + `df.queryExecution.logical`) which a connect client cannot construct. The connect branch (both the direct and the >31-dimensions struct path) now registers the filtered input as a temp view and runs `SELECT <aggs>, first(<dim>), grouping_id() FROM _flowman_grouping_aggregate_input GROUP BY GROUPING SETS (<sets>)` inside `DataFrameUtils.withTempView`, materialized with `localCheckpoint(false)` (deferred-analysis-safe). KEY INSIGHT: SQL `grouping_id()` derives its bits from the **first-appearance order of the columns inside the GROUPING SETS clause**, not from the full dimension-list order the classic path uses — so in connect the group masks are computed against the SQL discovery order (`groupingSets.flatten.distinct`), otherwise pre-aggregate filtering silently matches nothing. Classic mask logic (incl. the 3.2–3.3.1 legacy workaround) untouched. GroupedAggregateMappingTest 6/6 connect, 6/6 classic.
    ◦ FileRelation/FileDataset format lookup: static map of built-in formats, else assume single-path. **DONE (2026-10-26)**: new `SparkBackend.relationSupportsMultiplePaths(format)`; classic delegates to `SparkShim` (class-based), connect uses a static set (parquet/orc/json/csv/text/avro/sequencefile). `df.queryExecution.logical.refresh()` guarded by `!isConnect`. CI BACKLOG shrank accordingly (EmptyMapping/EmptyRelation/LocalTarget removed).
4. Phase 3 — Server plugin (≈2–3 weeks). Gate: feature parity with classic for custom functions, eager cache and schema lineage when the plugin is installed.
    ◦ New module flowman-connect-server: RelationPlugin for CountRecords/EagerCache, ExpressionPlugin for nullable_struct/IfNull, CommandPlugin for enriched describe (varchar lineage) and data-source capability probes. Protobuf messages in a small shared flowman-connect-common module.
    ◦ Client detects the plugin (a CommandPlugin ping) and degrades gracefully without it, logging which features are off.
    ◦ Register ExtraStrategies/ExtraOptimizations as a SparkSessionExtensions class for spark.sql.extensions on the server.
5. Phase 4 — Plugins and hardening (≈3–5 weeks). Gate: Delta tests and the downstream plugin pass in Connect mode.
    ◦ Delta: switch to io.delta.tables.DeltaTable (Delta Connect) plus SQL for DDL/ALTER TABLE ADD COLUMNS; drop DeltaLog file listing.
    ◦ JDBC upsert/merge: keep it on the server (CommandPlugin) rather than shipping closures.
    ◦ Cache df.schema results per mapping instance to cut AnalyzePlan round trips.
    ◦ Audit the other plugins (impala, kafka, aws, azure); most only need the sparkContext removals.
    ◦ Docs: a Connect deployment page (server conf, plugin jar, client storage credentials).
An upstream route matters here: PR #597 isn't merged, so land Phase 1 as a separate PR stacked on it, and raise the plan with the Flowman maintainers before Phase 2.
Risks, open questions, testing
The biggest risk is the Hive catalog: 12 files in core, spec and plugins pass Catalyst CatalogTable around, and SQL DDL can't express everything SessionCatalog does (e.g. alterTable preserving table properties).
Risk
Impact
Mitigation
ColumnNode is private[sql] and may change between 4.1 and 4.2
Shim breaks on upgrade
Keep the walker in the per-version shim dir; cover with tests
Many df.schema RPCs in describe/dirty checks
Slow job startup on remote servers
Cache schemas per mapping instance; batch where possible
Client needs direct storage access for dirty checks
Credential sprawl; fails on locked-down platforms
Optional CommandPlugin that runs FS checks on the server
Plugin jar must match client protobufs
Version skew in prod
Version handshake in the plugin ping; fail fast with a clear message
Varchar/char lineage without the plugin
Schema docs and Hive DDL differ from classic
Document the degradation; plugin restores it
Shaded gRPC/Netty in spark-connect-client-jvm vs Flowman's own Netty (PR #597 already pins codec versions)
Classpath conflicts like the slf4j/Jackson fixes in this PR
Separate assembly for Connect mode; run mvn dependency:tree diff in CI
Open questions:
[ ] Which Connect server is the target: self-run Spark 4.1 (plugin possible) or a managed endpoint where no plugin can be installed? This decides whether Phase 3 is optional.
[ ] Does the downstream plugin use Catalyst or sparkContext directly? It needs the same audit.
[ ] Is classic mode kept long term? If not, Phase 1's dual backend could be skipped, but that blocks upstreaming.
Testing: run the whole existing suite twice in CI, classic and spark.api.mode=connect with the in-process server, and mark Connect-only gaps with a ScalaTest tag rather than skipping silently. Add one integration job against a standalone Connect server in Docker to catch real gRPC and classpath issues.
