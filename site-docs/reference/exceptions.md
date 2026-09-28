# Exception Reference

Typed domain/configuration exceptions extend `FrameworkException`, which provides an error code, a context map for debugging, and a formatted message: `[ERROR_CODE] message (Context: key=value, ...)`. Recovery also uses operational runtime exceptions documented below.

The hierarchy is broader than the exceptions currently emitted. The tables explicitly mark public types retained as API vocabulary but not used by the present execution path; callers should handle observed runtime behavior, not assume every declared subtype is thrown.

## Hierarchy

```
FrameworkException (abstract)
├── ConfigurationException (abstract)
│   ├── YAMLSyntaxException              CONFIG_YAML_SYNTAX
│   ├── MissingConfigFieldException      CONFIG_MISSING_FIELD
│   ├── InvalidConfigTypeException       CONFIG_INVALID_TYPE
│   ├── ConfigFileException              CONFIG_FILE_ERROR
│   ├── InvalidReferenceException        CONFIG_INVALID_REFERENCE
│   └── CircularDependencyException      CONFIG_CIRCULAR_DEPENDENCY
├── ValidationException (abstract)
│   ├── ValidationConfigException        VALIDATION_CONFIG_ERROR
│   ├── SchemaValidationException        VALIDATION_SCHEMA
│   ├── PrimaryKeyViolationException     VALIDATION_PK_VIOLATION
│   ├── ForeignKeyViolationException     VALIDATION_FK_VIOLATION
│   └── MaxRejectionRateExceededException VALIDATION_MAX_REJECTION_RATE
├── DataProcessingException (abstract)
│   ├── InvariantViolationException      DATA_INVARIANT_VIOLATION
│   ├── DataSourceException              DATA_SOURCE_ERROR
│   ├── DataWriteException               DATA_WRITE_ERROR
│   └── MergeException                   DATA_MERGE_ERROR
├── TransformationException (abstract)
│   ├── PreValidationTransformationException  TRANSFORM_PRE_VALIDATION
│   └── PostValidationTransformationException TRANSFORM_POST_VALIDATION
├── AggregationException (abstract)
│   ├── DAGNodeExecutionException        DAG_NODE_EXECUTION
│   └── JoinException                    DAG_JOIN_ERROR
├── PluginException (abstract)
│   ├── CustomValidatorLoadException     PLUGIN_VALIDATOR_LOAD
│   └── CustomValidatorExecutionException PLUGIN_VALIDATOR_EXECUTION
├── OrchestrationException (abstract)
│   └── FlowExecutionException           ORCHESTRATION_FLOW_EXECUTION
├── BatchFailedException                  BATCH_FAILED
└── UnsupportedOperationException        UNSUPPORTED_OPERATION
```

## Configuration exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `YAMLSyntaxException` | `CONFIG_YAML_SYNTAX` | file, line, column, details | YAML file has syntax errors |
| `MissingConfigFieldException` | `CONFIG_MISSING_FIELD` | file, field, section | Required field missing in config |
| `InvalidConfigTypeException` | `CONFIG_INVALID_TYPE` | file, field, expectedType, actualType | Public type, not currently emitted; PureConfig conversion failures are wrapped in `ConfigFileException` |
| `ConfigFileException` | `CONFIG_FILE_ERROR` | file, message | I/O or parsing error |
| `InvalidReferenceException` | `CONFIG_INVALID_REFERENCE` | file, referenceType, referenceName, availableReferences | Public type, not currently emitted; structural reference issues are returned by `validate()` or wrapped during config loading |
| `CircularDependencyException` | `CONFIG_CIRCULAR_DEPENDENCY` | graphType, cycle | Circular dependency in flow FK graph or DAG |

## Validation exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `ValidationConfigException` | `VALIDATION_CONFIG_ERROR` | message | Invalid validation config (empty PK, invalid regex, etc.) |
| `SchemaValidationException` | `VALIDATION_SCHEMA` | flowName, missingColumns, typeMismatches | Public type, not currently emitted; schema failures are represented as rejected records |
| `PrimaryKeyViolationException` | `VALIDATION_PK_VIOLATION` | flowName, duplicateCount, keyColumns | Public type, not currently emitted; duplicate keys are represented as rejected records |
| `ForeignKeyViolationException` | `VALIDATION_FK_VIOLATION` | flowName, orphanCount, foreignKeyName, referencedFlow | Public type, not currently emitted; FK failures are represented as rejected records |
| `MaxRejectionRateExceededException` | `VALIDATION_MAX_REJECTION_RATE` | flowName, actualRate, maxRate, rejectedCount, totalCount | Rejection rate exceeds threshold |

## Data processing exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `InvariantViolationException` | `DATA_INVARIANT_VIOLATION` | flowName, inputCount, validCount, rejectedCount, difference | Not currently thrown by the framework. Retained in the hierarchy for backward compatibility. |
| `DataSourceException` | `DATA_SOURCE_ERROR` | sourceType, sourcePath, details | Failed to read source data |
| `DataWriteException` | `DATA_WRITE_ERROR` | outputType, outputPath, details | Public type, not currently emitted; write failures propagate into flow/batch result handling |
| `MergeException` | `DATA_MERGE_ERROR` | flowName, mergeStrategy, details | Public type, not currently emitted; MERGE failures propagate into commit reconciliation and flow result handling |

## Transformation exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `PreValidationTransformationException` | `TRANSFORM_PRE_VALIDATION` | flowName, details | Public type, not currently emitted; the original transform failure becomes a failed `FlowResult` |
| `PostValidationTransformationException` | `TRANSFORM_POST_VALIDATION` | flowName, details | Public type, not currently emitted; the original transform failure becomes a failed `FlowResult` |

## Aggregation exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `DAGNodeExecutionException` | `DAG_NODE_EXECUTION` | nodeId, details | Public type, not currently emitted; node failures propagate from Spark/framework code |
| `JoinException` | `DAG_JOIN_ERROR` | parentNode, childNode, joinStrategy, details | Public type, not currently emitted; join failures propagate from Spark/framework code |

## Plugin exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `CustomValidatorLoadException` | `PLUGIN_VALIDATOR_LOAD` | className, details | Public exception type, not currently emitted by the registry/reflection path; load/configuration failures surface as `ValidationConfigException` |
| `CustomValidatorExecutionException` | `PLUGIN_VALIDATOR_EXECUTION` | className, details | Public exception type, not currently emitted; validator exceptions propagate into flow failure handling |

## Orchestration exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `FlowExecutionException` | `ORCHESTRATION_FLOW_EXECUTION` | flowName, details | Public type, not currently emitted; flow failures are captured in `FlowResult` |

## Batch exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `BatchFailedException` | `BATCH_FAILED` | batchId, details | Thrown by `executeOrThrow()` whenever synchronous batch publication returns `success = false` |

## Other exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `UnsupportedOperationException` | `UNSUPPORTED_OPERATION` | operation, details | Unsupported file format, source type, data type, etc. |

## Recovery exceptions

Commit reconciliation also uses two runtime exceptions outside the `FrameworkException` hierarchy because they represent operational uncertainty/invariant failure rather than a typed business/configuration error:

| Exception | Meaning | Operator response |
|-----------|---------|-------------------|
| `AmbiguousCommitException` | The catalog could not prove whether an operation committed | Do not retry the write blindly. Restore catalog/history access and call `resume(batchId)` so reconciliation can run. |
| `DuplicateOperationCommitException` | The same deterministic operation ID appears in multiple snapshots | Stop the pipeline and investigate writers/history; automatic resume is blocked because the invariant is broken. |

Recovery APIs can also throw `IllegalArgumentException`, `IllegalStateException`, or `NoSuchElementException` for a changed pipeline identity, changed/unfingerprinted input, a concurrent state transition, a held lease, or an unknown batch. See [Recovery and Production Operations](../guides/recovery.md).

## Context keys

All context keys are defined in `ContextKeys` object:

| Key | Used by |
|-----|---------|
| `file` | Configuration exceptions |
| `line`, `column` | `YAMLSyntaxException` |
| `field`, `section` | `MissingConfigFieldException` |
| `expectedType`, `actualType` | `InvalidConfigTypeException` |
| `referenceType`, `referenceName`, `availableReferences` | `InvalidReferenceException` |
| `graphType`, `cycle` | `CircularDependencyException` |
| `flowName` | Validation, processing, transformation exceptions |
| `missingColumns`, `typeMismatches` | `SchemaValidationException` |
| `duplicateCount`, `keyColumns` | `PrimaryKeyViolationException` |
| `orphanCount`, `foreignKeyName`, `referencedFlow` | `ForeignKeyViolationException` |
| `actualRate`, `maxRate`, `rejectedCount`, `totalCount` | `MaxRejectionRateExceededException` |
| `inputCount`, `validCount`, `rejectedCount`, `difference` | `InvariantViolationException` |
| `sourceType`, `sourcePath` | `DataSourceException` |
| `outputType`, `outputPath` | `DataWriteException` |
| `mergeStrategy` | `MergeException` |
| `parentNode`, `childNode`, `joinStrategy` | `JoinException` |
| `batchId` | `BatchFailedException` |
| `details`, `message` | Various exceptions |

## Related

- [Architecture: Modules](../architecture/modules.md) — module responsibilities
- [Validation Engine](../guides/validation.md) — validation pipeline and rejection codes
- [Custom Validators](../guides/custom-validators.md) — validator loading and execution behavior
