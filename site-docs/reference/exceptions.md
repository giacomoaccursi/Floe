# Exception Reference

All framework exceptions extend `FrameworkException`, which provides an error code, a context map for debugging, and a formatted message: `[ERROR_CODE] message (Context: key=value, ...)`.

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
└── UnsupportedOperationException        UNSUPPORTED_OPERATION
```

## Configuration exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `YAMLSyntaxException` | `CONFIG_YAML_SYNTAX` | file, line, column, details | YAML file has syntax errors |
| `MissingConfigFieldException` | `CONFIG_MISSING_FIELD` | file, field, section | Required field missing in config |
| `InvalidConfigTypeException` | `CONFIG_INVALID_TYPE` | file, field, expectedType, actualType | Field has wrong type |
| `ConfigFileException` | `CONFIG_FILE_ERROR` | file, message | I/O or parsing error |
| `InvalidReferenceException` | `CONFIG_INVALID_REFERENCE` | file, referenceType, referenceName, availableReferences | Reference to non-existent entity (e.g., unknown domain name) |
| `CircularDependencyException` | `CONFIG_CIRCULAR_DEPENDENCY` | graphType, cycle | Circular dependency in flow FK graph or DAG |

## Validation exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `ValidationConfigException` | `VALIDATION_CONFIG_ERROR` | message | Invalid validation config (empty PK, invalid regex, etc.) |
| `SchemaValidationException` | `VALIDATION_SCHEMA` | flowName, missingColumns, typeMismatches | Schema mismatch during validation |
| `PrimaryKeyViolationException` | `VALIDATION_PK_VIOLATION` | flowName, duplicateCount, keyColumns | Duplicate PK values found |
| `ForeignKeyViolationException` | `VALIDATION_FK_VIOLATION` | flowName, orphanCount, foreignKeyName, referencedFlow | FK values not found in parent |
| `MaxRejectionRateExceededException` | `VALIDATION_MAX_REJECTION_RATE` | flowName, actualRate, maxRate, rejectedCount, totalCount | Rejection rate exceeds threshold |

## Data processing exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `InvariantViolationException` | `DATA_INVARIANT_VIOLATION` | flowName, inputCount, validCount, rejectedCount, difference | Not currently thrown by the framework. Retained in the hierarchy for backward compatibility. |
| `DataSourceException` | `DATA_SOURCE_ERROR` | sourceType, sourcePath, details | Failed to read source data |
| `DataWriteException` | `DATA_WRITE_ERROR` | outputType, outputPath, details | Failed to write output |
| `MergeException` | `DATA_MERGE_ERROR` | flowName, mergeStrategy, details | MERGE INTO operation failed |

## Transformation exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `PreValidationTransformationException` | `TRANSFORM_PRE_VALIDATION` | flowName, details | Pre-validation transform failed |
| `PostValidationTransformationException` | `TRANSFORM_POST_VALIDATION` | flowName, details | Post-validation transform failed |

## Aggregation exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `DAGNodeExecutionException` | `DAG_NODE_EXECUTION` | nodeId, details | DAG node failed to execute |
| `JoinException` | `DAG_JOIN_ERROR` | parentNode, childNode, joinStrategy, details | Join operation failed |

## Plugin exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `CustomValidatorLoadException` | `PLUGIN_VALIDATOR_LOAD` | className, details | Failed to load custom validator class |
| `CustomValidatorExecutionException` | `PLUGIN_VALIDATOR_EXECUTION` | className, details | Custom validator threw during execution |

## Orchestration exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `FlowExecutionException` | `ORCHESTRATION_FLOW_EXECUTION` | flowName, details | Flow execution failed |

## Other exceptions

| Exception | Error Code | Context Keys | When |
|-----------|-----------|--------------|------|
| `UnsupportedOperationException` | `UNSUPPORTED_OPERATION` | operation, details | Unsupported file format, source type, data type, etc. |

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
| `inputCount`, `validCount`, `difference` | `InvariantViolationException` |
| `sourceType`, `sourcePath` | `DataSourceException` |
| `outputType`, `outputPath` | `DataWriteException` |
| `mergeStrategy` | `MergeException` |
| `parentNode`, `childNode`, `joinStrategy` | `JoinException` |
| `details`, `message` | Various exceptions |

## Related

- [Architecture: Modules](../architecture/modules.md) — module responsibilities
- [Validation Engine](../guides/validation.md) — validation pipeline and rejection codes
- [Custom Validators](../guides/custom-validators.md) — plugin exceptions
