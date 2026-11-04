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
#     use these APIs and must be migrated in later phases.
#
# The backlog records the *number* of offending lines per file. Adding a new
# classic API call to a backlog file therefore fails the check, and removing
# an occurrence (or deleting a file) requires updating the baseline, so stale
# entries cannot accumulate. Work items get removed from this list as they are
# migrated.
#
# Usage: devtools/ci/check-no-classic-spark.sh
# Exit code 0 if no new usage is found, 1 otherwise.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Banned patterns. Note that ".rdd" also matches "String.rdd" etc., so we
# match file contents via a plain grep.
PATTERNS='\.sparkContext|\.sessionState|\.queryExecution|\.rdd'

# Files known to still use the classic Spark API, with the expected number of
# offending lines. Keep this list sorted.
declare -A BACKLOG=(
    ["flowman-core/src/main/scala/com/dimajix/flowman/catalog/HiveCatalog.scala"]=4
    ["flowman-core/src/main/scala/com/dimajix/flowman/documentation/SchemaCheck.scala"]=1
    ["flowman-core/src/main/scala/com/dimajix/flowman/jdbc/JdbcUtils.scala"]=2
    ["flowman-core/src/main/scala/com/dimajix/flowman/model/Mapping.scala"]=2
    ["flowman-core/src/main/scala/com/dimajix/flowman/model/SchemaExtractor.scala"]=1
    ["flowman-spec/src/main/scala/com/dimajix/flowman/spec/dataset/FileDataset.scala"]=1
    ["flowman-spec/src/main/scala/com/dimajix/flowman/spec/mapping/GroupedAggregateMapping.scala"]=1
    ["flowman-spec/src/main/scala/com/dimajix/flowman/spec/relation/FileRelation.scala"]=1
    ["flowman-spec/src/main/scala/com/dimajix/flowman/spec/relation/GenericRelation.scala"]=1
    ["flowman-spec/src/main/scala/com/dimajix/flowman/spec/relation/HiveTableRelation.scala"]=5
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/DataFrameUtils.scala"]=1
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/catalyst/PlanUtils.scala"]=1
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/functions.scala"]=1
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/sources/empty/NullRelation.scala"]=2
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/sources/fixedwidth/FixedWidthFormat.scala"]=1
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/sources/sequencefile/SequenceFileFormat.scala"]=2
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/streaming/StreamingUtils.scala"]=1
    ["flowman-spark-extensions/src/main/scala/com/dimajix/spark/sql/local/csv/CsvRelation.scala"]=1
    ["flowman-spark-extensions/src/main/scala/org/apache/spark/sql/hive/HiveClientShim.scala"]=2
)

# Files that are always allowed (backend + shim infrastructure)
EXCLUDE_FILES=(
    "flowman-core/src/main/scala/com/dimajix/flowman/execution/ClassicBackend.scala"
    "flowman-core/src/main/scala/com/dimajix/flowman/execution/ConnectBackend.scala"
    # The Spark Connect server plugin intentionally runs inside the Connect server (driver), where the classic
    # Spark API (SparkContext/SessionState/...) is available by design.
    "flowman-connect-server/src/main/scala/com/dimajix/flowman/connect/server/FlowmanRelationPlugin.scala"
    "flowman-connect-server/src/main/scala/com/dimajix/flowman/connect/server/FlowmanCommandPlugin.scala"
)

# Validate the backlog baselines: stale entries must not accumulate.
declare -a STALE=()
for entry in "${!BACKLOG[@]}"; do
    expected="${BACKLOG[$entry]}"
    file="$ROOT/$entry"
    if [[ ! -f "$file" ]]; then
        STALE+=("$entry (file not found)")
        continue
    fi
    actual=$(grep -cE "$PATTERNS" "$file")
    if (( actual < expected )); then
        STALE+=("$entry (expected $expected, found $actual)")
    fi
done

# Find offending files under any module's src/main
mapfile -t OFFENDERS < <(
    find "$ROOT" -path '*/src/main/*' -name '*.scala' \
        -exec grep -lE "$PATTERNS" {} \;
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
    for exc in "${EXCLUDE_FILES[@]}"; do
        if [[ "$rel" == "$exc" ]]; then skip=true; break; fi
    done
    if $skip; then continue; fi

    # Check the documented backlog (including its line-count baseline)
    if [[ -v "BACKLOG[$rel]" ]]; then
        expected="${BACKLOG[$rel]}"
        actual=$(grep -cE "$PATTERNS" "$file")
        if (( actual > expected )); then
            NEW_USAGE+=("$rel (expected at most $expected classic API lines, found $actual)")
        fi
        continue
    fi

    NEW_USAGE+=("$rel")
done

if [[ ${#STALE[@]} -gt 0 ]]; then
    echo "ERROR: Stale entries in the classic Spark API backlog (update devtools/ci/check-no-classic-spark.sh):"
    printf '  %s\n' "${STALE[@]}"
    exit 1
fi

if [[ ${#NEW_USAGE[@]} -gt 0 ]]; then
    echo "ERROR: New classic Spark API usage detected (sparkContext / sessionState / queryExecution / .rdd) outside of SparkBackend, shim packages and the migration backlog."
    echo "Migrate the file to the SparkBackend abstraction or, if it is part of the Spark Connect migration backlog, add it explicitly to BACKLOG in devtools/ci/check-no-classic-spark.sh."
    printf '  %s\n' "${NEW_USAGE[@]}"
    exit 1
fi

echo "OK: no new classic Spark API usage."
