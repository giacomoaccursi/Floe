# Custom Validators

Guide to writing custom validation logic beyond the built-in regex, range, and domain rules.

## Overview

When the built-in validators are not enough, you can provide a custom validator class. There are two ways to wire it up:

1. **Registry (recommended)** — register the validator by name on the pipeline builder with `withCustomValidator`, then reference that name in YAML:

```scala
IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withCustomValidator("luhn", () => new LuhnCheckValidator())
  .build()
```

```yaml
- type: custom
  class: luhn
  column: credit_card_number
  config:
    strict: "true"
  onFailure: reject
```

2. **Reflection** — use the fully qualified class name directly in YAML. The framework loads it via `Class.forName`:

```yaml
- type: custom
  class: "com.mycompany.validators.LuhnCheckValidator"
  column: credit_card_number
  config:
    strict: "true"
  onFailure: reject
```

The registry has priority: if the `class` value matches a registered name, the registry is used; otherwise it falls back to reflection.

## The Validator trait

Every custom validator must implement `com.etl.framework.validation.Validator`:

```scala
import com.etl.framework.validation.{Validator, ValidationStepResult}
import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame

class MyValidator extends Validator {
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    // Your validation logic here
    // Return ValidationStepResult(validDf, Some(rejectedDf))
    // or ValidationStepResult(validDf, None) if all records pass
    ???
  }
}
```

Requirements:

1. The class must have a **no-argument constructor**
2. It must implement the `validate` method
3. It must be on the classpath at runtime

The `validate` method receives:

- `df` — the DataFrame to validate (only valid records from previous steps)
- `rule` — the `ValidationRule` config object, giving access to `column`, `config`, `description`, etc.

It returns a `ValidationStepResult`:

- `validDf` — records that passed validation
- `rejectedDf` — `Some(df)` with rejected records, or `None` if all passed

## Registering validators

The recommended way to wire custom validators is through the pipeline builder registry. Register a factory function with a short name, then reference that name in YAML:

```scala
import com.etl.framework.pipeline.IngestionPipeline

implicit val spark: SparkSession = SparkSession.builder()
  .appName("My ETL")
  .master("local[*]")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()

val pipeline = IngestionPipeline.builder()
  .withConfigDirectory("config")
  .withCustomValidator("luhn", () => new LuhnCheckValidator())
  .withCustomValidator("email-domain", () => new EmailDomainValidator())
  .build()

val result = pipeline.execute()
```

Flow YAML:

```yaml
validation:
  primaryKey: [order_id]
  rules:
    - type: custom
      class: luhn
      column: credit_card_number
      config:
        strict: "true"
      onFailure: reject
    - type: custom
      class: email-domain
      column: email
      config:
        domains: "example.com, mycompany.com"
      onFailure: reject
```

The `class` field is matched against registered names first. If no match is found, the framework falls back to reflection loading (see below). This means you can mix both approaches in the same pipeline — some validators registered by name, others loaded by fully qualified class name.

The factory function (`() => Validator`) is called each time the validator is needed, so each invocation gets a fresh instance.

## Reflection loading

The framework loads custom validators via `ValidatorFactory`:

1. Reads the `class` field from the rule config
2. Loads the class via `Class.forName(className)`
3. Instantiates it via the no-arg constructor: `clazz.getDeclaredConstructor().newInstance()`
4. Verifies it implements `Validator`

If any step fails, the framework throws a descriptive exception:

| Error | Exception |
|-------|-----------|
| `class` field missing | `ValidationConfigException` |
| Class not found | `ValidationConfigException` (wraps `ClassNotFoundException`) |
| Does not implement `Validator` | `ValidationConfigException` |
| Instantiation failure | `ValidationConfigException` |

## Example: email domain validator

A validator that checks email addresses belong to an allowed set of domains:

```scala
package com.mycompany.validators

import com.etl.framework.validation.{Validator, ValidationStepResult}
import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class EmailDomainValidator extends Validator {
  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val col_name = rule.column.getOrElse(
      throw new IllegalArgumentException("column is required for EmailDomainValidator")
    )

    val allowedDomains = rule.config
      .flatMap(_.get("domains"))
      .getOrElse("")
      .split(",")
      .map(_.trim.toLowerCase)
      .filter(_.nonEmpty)
      .toSet

    if (allowedDomains.isEmpty) {
      return ValidationStepResult(df, None)
    }

    val domainExpr = lower(substring_index(col(col_name), "@", -1))
    val domainList = allowedDomains.toSeq

    val valid = df.filter(
      col(col_name).isNull || domainExpr.isin(domainList: _*)
    )
    val rejected = df.filter(
      col(col_name).isNotNull && !domainExpr.isin(domainList: _*)
    )

    val rejectedCount = rejected.isEmpty
    if (rejectedCount) {
      ValidationStepResult(df, None)
    } else {
      ValidationStepResult(valid, Some(rejected))
    }
  }
}
```

YAML configuration (using a registered name):

```yaml
- type: custom
  class: email-domain
  column: email
  config:
    domains: "example.com, mycompany.com, partner.org"
  onFailure: reject
  description: "Email must belong to an approved domain"
```

Or using the fully qualified class name:

```yaml
- type: custom
  class: "com.mycompany.validators.EmailDomainValidator"
  column: email
  config:
    domains: "example.com, mycompany.com, partner.org"
  onFailure: reject
  description: "Email must belong to an approved domain"
```

## skipNull and onFailure

Custom validators interact with the standard `skipNull` and `onFailure` settings:

- **skipNull**: the framework handles `skipNull` for all rule types, including custom validators. When `skipNull` is `true` (the default), NULL values are filtered out before your `validate` method is called. You do not need to handle NULLs yourself.

- **onFailure**: the framework applies the `onFailure` action (reject, warn, skip) based on the `ValidationStepResult` you return. If `onFailure: skip`, your validator is never called.

## Related

- [Validation Engine](validation.md) — full validation pipeline
- [Flow Configuration — rules](../configuration/flows.md#rule-fields) — rule YAML reference
- [Reference: Exceptions](../reference/exceptions.md) — `CustomValidatorLoadException`, `CustomValidatorExecutionException`
