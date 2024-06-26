
# Earliest Mapping

The `earliest` mapping keeps only the earliest (oldest) record per ID. This is useful
when working with streams of change events and you only want to keep the first
event for each ID.

## Example
```
mappings:
  first_customer_updates:
    kind: earliest
    input: all_customer_updates
    versionColumns: ts
    keyColumns: customer_id
```

## Fields
* `kind` **(mandatory)** *(string)*: `earliest`

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

* `input` **(mandatory)** *(string)*:

* `versionColumns`
Specifies the columns where the version (or timestamp) is contained. For each ID only
the record with the highest value will be kept.

* `keyColumns`
Specifies one or more columns forming a primary key or ID. Different versions of the
same entity are then distinguished by the `versionColumns` 

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* the transformation itself.


## Outputs
* `main` - the only output of the mapping
