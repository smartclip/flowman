# Stream Mapping

The `stream` mapping is very similar to the [`read`](relation.md) mapping, except that it will create a streaming
result used for creating continuous event processing applications

## Example
```yaml
mappings:
  measurements-raw:
    kind: stream
    relation: measurements-raw
    columns:
      raw_data: String
    filter: "raw_data IS NOT NULL"

relations:
  measurements-raw:
    kind: kafka
    hosts:
      - kafka-01
      - kafka-02
    topics: measurements_raw
```

Since Flowman 0.18.0, you can also directly specify the relation inside the dataset definition. This saves you
from having to create a separate relation definition in the `relations` section.  This is only recommeneded, if you
do not access the target relation otherwise, such that a shared definition would not provide any benefir.
```yaml
mappings:
  measurements-raw:
    kind: stream
    relation:
      kind: kafka
      hosts:
        - kafka-01
        - kafka-02
      topics: measurements_raw  
    columns:
      raw_data: String
    filter: "raw_data IS NOT NULL"
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `stream`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*:
  Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
  Cache mode for the results of this mapping. Supported values are
    * `NONE` - Disables caching of teh results of this mapping
    * `DISK_ONLY` - Caches the results on disk
    * `MEMORY_ONLY` - Caches the results in memory. If not enough memory is available, records will be uncached.
    * `MEMORY_ONLY_SER` - Caches the results in memory in a serialized format. If not enough memory is available, records will be uncached.
    * `MEMORY_AND_DISK` - Caches the results first in memory and then spills to disk.
    * `MEMORY_AND_DISK_SER` - Caches the results first in memory in a serialized format and then spills to disk.

* `relation` **(mandatory)** *(type: string)*:
  Specifies the name of the relation to read from.

* `columns` **(optional)** *(type: map:data_type)* *(default: empty):
  Specifies the list of columns and types to read from the relation. This schema will be applied to the records after
  they have been read and interpreted by the underlying source. This schema will also be used as a subsitute for schema
  inference and therefore can be very helpful when using [`mock`](mock.md) mappings.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied for reading only a subset of records. The filter is applied
  *after* the schema as specified in `columns` is applied. This means that if you are using `columns`, then you
  can only access these columns in the `filter` expression.


## Outputs
* `main` - the only output of the mapping

