# Custom Validators

Guide to writing custom validation logic beyond the built-in regex, range, and domain rules.

## Overview

When the built-in validators are not enough, you can provide a custom validator class. The framework loads it via reflection, optionally configures it, and calls it during the validation pipeline alongside the other rules.

```yaml
- type: custom
  class: "com.mycompany.validators.LuhnCheckValidator"
  column: credit_card_number
  config:
    strict: "true"
  onFailure: reject
```

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

## ConfigurableValidator

If your validator needs configuration from the YAML `config` map, implement `ConfigurableValidator` in addition to `Validator`:

```scala
import com.etl.framework.validation.{Validator, ConfigurableValidator, ValidationStepResult}
import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame

class LuhnCheckValidator extends Validator with ConfigurableValidator {
  private var strict: Boolean = false

  override def configure(config: Map[String, String]): Unit = {
    strict = config.getOrElse("strict", "false").toBoolean
  }

  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    // Use this.strict and rule.column to implement validation
    ???
  }
}
```

The `configure` method is called once after instantiation, before `validate`. The `config` map comes from the YAML rule definition:

```yaml
- type: custom
  class: "com.mycompany.validators.LuhnCheckValidator"
  column: credit_card_number
  config:
    strict: "true"
    allowTestCards: "false"
  onFailure: reject
```

All config values are strings — parse them in `configure` as needed.

## Reflection loading

The framework loads custom validators via `ValidatorFactory`:

1. Reads the `class` field from the rule config
2. Loads the class via `Class.forName(className)`
3. Instantiates it via the no-arg constructor: `clazz.getDeclaredConstructor().newInstance()`
4. Verifies it implements `Validator`
5. If it also implements `ConfigurableValidator`, calls `configure(rule.config)`

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

import com.etl.framework.validation.{Validator, ConfigurableValidator, ValidationStepResult}
import com.etl.framework.config.ValidationRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class EmailDomainValidator extends Validator with ConfigurableValidator {
  private var allowedDomains: Set[String] = Set.empty

  override def configure(config: Map[String, String]): Unit = {
    allowedDomains = config
      .getOrElse("domains", "")
      .split(",")
      .map(_.trim.toLowerCase)
      .filter(_.nonEmpty)
      .toSet
  }

  override def validate(df: DataFrame, rule: ValidationRule): ValidationStepResult = {
    val col_name = rule.column.getOrElse(
      throw new IllegalArgumentException("column is required for EmailDomainValidator")
    )

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

YAML configuration:

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

- **skipNull**: the framework handles NULL filtering before calling your validator when `skipNull: true` (default). Your `validate` method receives a DataFrame where NULL values in the target column have already been separated. If `skipNull: false`, NULLs are included and your validator must handle them.

- **onFailure**: the framework applies the `onFailure` action (reject, warn, skip) based on the `ValidationStepResult` you return. If `onFailure: skip`, your validator is never called.

## Invariant

The validation engine enforces `input_count == valid_count + rejected_count` after every step. If your custom validator returns a `validDf` and `rejectedDf` whose combined row count doesn't match the input, the flow fails with `InvariantViolationException`.

!!!warning
    Make sure your validator partitions records cleanly — every input row must appear in either the valid or rejected output, never both, never neither.

## Related

- [Validation Engine](validation.md) — full validation pipeline
- [Flow Configuration — rules](../configuration/flows.md#rule-fields) — rule YAML reference
- [Reference: Exceptions](../reference/exceptions.md) — `CustomValidatorLoadException`, `CustomValidatorExecutionException`
