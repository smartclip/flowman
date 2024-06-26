# Kafka Relations

The Kafak relation is provided via the `flowman-kafka` plugin. It allows you to access Kafka topics both in batch
and in stream processing, both as sources and as sinks

## Plugin

This relation type is provided as part of the [`flowman-kafka` plugin](../../plugins/kafka.md), which needs to be enabled in your
`namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.

## Example

```yaml
relations:
  some_kafka_relation:
    kind: kafka
    hosts: 
      - kafka-01
      - kafka-02
    topics:
      - topic_a
      - topic_b_.*
    startOffset: earliest
    endOffset: latest
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `kafka`

* `options` **(optional)** *(map:string)* *(default: empty)*:
  All options are passed directly to the reader/writer backend and are specific to each
  supported format.

* `schema` **(optional)** *(type: schema)* *(default: empty)*:
  Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
  try to infer the schema.

* `description` **(optional)** *(type: string)* *(default: empty)*:
  A description of the relation. This is purely for informational purpose.

* `hosts` **(required)** *(type: list)* *(default: empty)*:
List of Kafka bootstrap servers to contact. This list does not need to be exhaustive, Flowman will automatically find 
  all  other Kafka brokers of the Kafka cluster

* `topics` **(required)** *(type: list)* *(default: empty)*:
  List of Kafka topics. When reading, a topic may be specified as a regular expression for subscribing to multiple 
  topics. Writing only supports a single topic, and this may also not be a regular expression.

* `startOffset` **(optional)** *(type: string)* *(default: earliest)*:
 Flowman will only process messages starting from this offset. When you want to process all data available in Kafka,
  you need to set this value to `earliest` (which is also the default).

* `endOffset` **(optional)** *(type: string)* *(default: latest)*:
 When reading from Kafka using batch processing, you can specify the latest offset to process. Per default this is set
  to `latest` which means that messages including the latest one will be processed


## Output Modes

### Batch Writing
The `kafa` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                         |
|---------------------|-----------|--------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the Kafka topic already exists |
| `ignoreIfExists`    | yes       | Do nothing if the Kafka topic already exists     |
| `overwrite`         | no        | -                                                |
| `overwrite_dynamic` | no        | -                                                |
| `append`            | yes       | Append new records to the existing Kafka topic   |
| `update`            | no        | -                                                |

### Stream Writing
In addition to batch writing, the Kafka relation also supports stream writing via the 
[`stream` target](../target/stream.md) with the following semantics:

| Output Mode | Supported | Comments                                                                      |
|-------------|-----------|-------------------------------------------------------------------------------|
| `append`    | yes       | Append new records from the streaming process once they don't change any more |
| `update`    | yes       | Append records every time they are updated                                    |
| `complete`  | no        | -                                                                             |
