# Verify Target

The `verify` target is used to execute a set of assertions *after* all targets have been built. This can be used to
verify the results to ensure that all processing was correct. In most cases it is advisable to use the similar 
[`validate`](validate.md) build target, which is executed in advance of the `CREATE` and `BUILD` phase and can be
used to validate any assumptions on the incoming data.

## Example

```yaml
targets:
  verify_output:
    kind: verify
    assertions:
      assert_primary_key:
        kind: sql
        tests:
          - query: "SELECT id,count(*) FROM output_table GROUP BY id HAVING count(*) > 0"
            expected: []
          - query: "SELECT id,count(*) FROM output_cube GROUP BY id HAVING count(*) > 0"
            expected: []
      
      assert_measurement_count:
        kind: sql
        query: "SELECT COUNT(*) FROM measurements_extracted"
        expected: 2
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `verify`

* `assertions` **(optional)** *(type: map:assertion)*:
  Map of [assertions](../assertion/index.md) to be executed. The verification is marked as *failed* if a single
  assertion fails.


## Supported Phases
* `VERIDY` - The specified assertions will be run in the `VERIFY` phase after the `CREATE` and `BUILD` phases.
