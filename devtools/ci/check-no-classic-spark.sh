#!/usr/bin/env bash
#
# Checks that no *new* classic Spark API usage is introduced outside of the
# SparkBackend and the legacy shim packages. Flowman is being migrated to
# support Spark Connect, where SparkContext, SessionState, QueryExecution and
# RDDs are not available on the client.
#
# The patterns below are banned everywhere except:
#   - the SparkBackend implementations (execution/*Backend.scala)
#   - the per-Spark-version shim directories (src/main/spark-*)
#   - the org.apache.spark / com.dimajix.spark shim packages in
#     flowman-spark-extensions and flowman-spark-testing
#   - the explicit backlog below, which lists files that still legitimately
#     use these APIs and must be migrated in later phases. Work items get
#     removed from this list as they are migrated.
#
# Usage: devtools/ci/check-no-classic-spark.sh
# Exit code 0 if no new usage is found, 1 otherwise.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Banned patterns. Note that ".rdd" also matches "String.rdd" etc., so we
# match file paths containing these tokens via a plain grep.
PATTERNS='\.sparkContext|\.sessionState|\.queryExecution|\.rdd'

# Files known to still use the classic Spark API. These are the Phase 2/3
# migration backlog; keep this list sorted.
BACKLOG=(
    "flowman-core/src/main/scala/com/dimajix/flowman/catalog/HiveCatalog.scala"
    "flowman-core/src/main/scala/com/dimajix/flowman/documentation/SchemaCheck.scala"
    "flowman-core/src/main/scala/com/dimajix/flowman/jdbc/JdbcUtils.scala"
    "flowman-core/src/main/scala/com/dimajix/flowman/model/Mapping.scala"
    "flowman-core/src/main/scala/com/dimajix/flowman/model/SchemaExtractor.scala"
    "flowman-spec/src/main/scala/com/dimajix/flowman/spec/dataset/FileDataset.scala"
    "flowman-spec/src/main/scala/com/dimajix/flowman/spec/mapping/GroupedAggregateMapping.scala"
    "flowman-spec/src/main/scala/com/dimajix/flowman/spec/relation/FileRelation.scala"
    "flowman-spec/src/main/scala/com/dimajix/flowman/spec/relation/GenericRelation.scala"
    "flowman-spec/src/main/scala/com/dimajix/flowman/spec/relation/HiveTableRelation.scala"
    "flowman-spec/src/main/scala/com/dimajix/flowman/spec/target/LocalTarget.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/DataFrameUtils.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/catalyst/PlanUtils.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/functions.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/sources/empty/NullRelation.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/sources/fixedwidth/FixedWidthFormat.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/sources/sequencefile/SequenceFileFormat.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/streaming/StreamingUtils.scala"
    "flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/local/csv/CsvRelation.scala"
    "flowman-spark-extensions/src/main/scala/org/apache/spark/sql/hive/HiveClientShim.scala"
)

# Files that are always allowed (backend + shim infrastructure)
EXCLUDE_DIRS=(
    "flowman-core/src/main/scala/com/dimajix/flowman/execution/ClassicBackend.scala"
    "flowman-core/src/main/scala/com/dimajix/flowman/execution/ConnectBackend.scala"
    # The Spark Connect server plugin intentionally runs inside the Connect server (driver), where the classic
    # Spark API (SparkContext/SessionState/...) is available by design.
    "flowman-connect-server/src/main/scala/com/dimajix/flowman/connect/server/FlowmanRelationPlugin.scala"
    "flowman-connect-server/src/main/scala/com/dimajix/flowman/connect/server/FlowmanCommandPlugin.scala"
)

# Find offending files under any module's src/main
mapfile -t OFFENDERS < <(
    find "$ROOT" -path '*/src/main/*' -name '*.scala' \
        -exec grep -lE "$PATTERNS" {} \; | while read -r file; do
            # Only report matches in code, not in comments
            if grep -nE "$PATTERNS" "$file" | grep -qvE '^[0-9]+:[[:space:]]*(//|\*|/\*)'; then
                echo "$file"
            fi
        done
)

declare -a NEW_USAGE=()
for file in "${OFFENDERS[@]}"; do
    rel="${file#"$ROOT/"}"

    # Skip per-version shim directories and the implicit shim packages
    case "$rel" in
        */src/main/spark-*/*) continue ;;
    esac

    # Skip the test-harness modules, which deliberately support both modes
    case "$rel" in
        flowman-spark-testing/*) continue ;;
        flowman-testing/*) continue ;;
    esac

    # Skip explicit exclusions
    skip=false
    for exc in "${EXCLUDE_DIRS[@]}"; do
        if [[ "$rel" == "$exc" ]]; then skip=true; break; fi
    done
    if $skip; then continue; fi

    # Skip the documented backlog
    in_backlog=false
    for entry in "${BACKLOG[@]}"; do
        if [[ "$rel" == "$entry" ]]; then in_backlog=true; break; fi
    done
    if $in_backlog; then continue; fi

    NEW_USAGE+=("$rel")
done

if [[ ${#NEW_USAGE[@]} -gt 0 ]]; then
    echo "ERROR: New classic Spark API usage detected (sparkContext / sessionState / queryExecution / .rdd) outside of SparkBackend, shim packages and the migration backlog."
    echo "Migrate the file to the SparkBackend abstraction or, if it is part of the Spark Connect migration backlog, add it explicitly to BACKLOG in devtools/ci/check-no-classic-spark.sh."
    printf '  %s\n' "${NEW_USAGE[@]}"
    exit 1
fi

echo "OK: no new classic Spark API usage."
