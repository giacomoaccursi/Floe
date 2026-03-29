# Domains Configuration

Reference for `domains.yaml` — defines named sets of allowed values for domain validation rules.

## Example

```yaml
domains:
  order_status:
    name: order_status
    description: "Valid order statuses"
    values: ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    caseSensitive: false

  country_code:
    name: country_code
    description: "ISO 3166-1 alpha-2 country codes"
    values: ["US", "GB", "DE", "FR", "IT", "ES"]
    caseSensitive: true
```

## Domain fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | Domain identifier (must match the map key) |
| `description` | yes | — | Human-readable description |
| `values` | yes | — | List of allowed values |
| `caseSensitive` | no | `true` | If false, comparison is case-insensitive |

## Usage in flow validation

Reference a domain in a flow's validation rules using `domainName`:

```yaml
# In flows/orders.yaml
validation:
  rules:
    - type: domain
      column: status
      domainName: order_status
      onFailure: reject
```

If the domain name referenced in a flow YAML is not found in `domains.yaml`, the framework throws a `ValidationConfigException` at runtime.

For the full validation reference, see [Validation Engine](../guides/validation.md#domain-validation).

## Case sensitivity

When `caseSensitive: false`, the comparison converts both the column value and the domain values to lowercase before matching. This means `"Shipped"`, `"SHIPPED"`, and `"shipped"` all match a domain value of `"shipped"`.

When `caseSensitive: true` (default), the match is exact.
