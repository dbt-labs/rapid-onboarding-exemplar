# Project style guide

This is the working style guide for `rapid_onboarding_exemplar`. It reflects the structure already in the repo and tightens a few conventions so contributors make changes the same way.

## Core principles

- Build in layers: `staging` → `intermediate` → `marts` → `aggregates` when needed.
- Keep each model at a clear grain. If the grain changes, that is usually a new model.
- Prefer boring, readable SQL over clever SQL.
- Centralize business logic once, then reuse it with `ref()`.
- Always document and test the contract of a model, not just its existence.

## Project structure

Use the existing directory layout in `dbt_project.yml`:

- `models/staging`: source-conformed models, light renaming and type cleanup
- `models/intermediate`: reusable joins, calculations, and grain-shaping steps
- `models/marts`: business-facing fact and dimension models
- `models/aggregates`: purpose-built rollups or presentation models
- `snapshots/`: historical tracking logic
- `seeds/`: static reference data only
- `macros/`: shared SQL or Jinja utilities
- `analysis/`: ad hoc analytical SQL that should not become part of the DAG
- `tests/`: custom data tests

Don’t put business logic into staging if it belongs in intermediate or marts. Staging should stay thin.

## Naming conventions

Match the patterns already used in the repo.

### Model names

- Staging: `stg_<source>__<entity>`
  - Example: `stg_tpch__orders`
- Intermediate: `int_<domain>_<purpose>`
  - Example: `int_order_items_joined`
- Facts: `fct_<entity>`
  - Example: `fct_orders`
- Dimensions: `dim_<entity>`
  - Example: `dim_customers`
- Aggregates: `agg_<purpose>`
  - Example: `agg_regions_segments`

Use double underscores in staging names to separate the source system from the entity. For non-staging models, prefer single underscores.

### File names

Keep SQL and YAML names aligned and colocated by folder/domain:

- SQL model in the domain folder
- YAML file for that folder, like `_finance__models.yml` or `_tpch__models.yml`
- Docs blocks in nearby markdown files only when reused across models

### Column names

Prefer explicit, analytics-friendly names:

- IDs end in `_id`
- Booleans use `is_`, `has_`, or another clearly boolean prefix
- Dates use `_date`
- Timestamps use `_at`
- Percent/rate fields should say `percentage` or `rate`
- Currency and additive measures should end in `_amount` when they represent money
- Codes should end in `_code` when they are not user-friendly labels

Avoid ambiguous names like `name`, `type`, `status`, or `date` unless the model context makes them unmistakable.

## SQL style

### General

- Use lowercase SQL keywords.
- Use one CTE per logical step.
- Prefer CTEs over nested subqueries.
- Use `{{ ref() }}` and `{{ source() }}` exclusively for dbt dependencies.
- Avoid `select *` in intermediate, marts, and aggregates. It is acceptable in staging only when immediately followed by a narrow rename/select step.
- Put derived logic in clearly named CTEs like `renamed`, `joined`, `final`, or something more specific.
- Keep joins and filters visually obvious.

Recommended shape:

```sql
with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select
        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderdate as order_date
    from source

),

final as (

    select
        order_id,
        customer_id,
        order_date
    from renamed

)

select * from final
```

### CTE conventions by layer

- `staging`: `source`, `renamed`, optionally `casted` or `final`
- `intermediate`: inputs named after the upstream entity, then a purposeful transformation CTE
- `marts`: input CTEs plus a `final` CTE for the business-facing output

### Joins

- Join on declared keys, not descriptive fields.
- Use `inner join` only when dropping unmatched records is intentional.
- Otherwise prefer `left join` and make the null-handling explicit.
- If a join changes grain, call that out in the model description.

### Aggregations

- Aggregate only after the model grain is clear.
- Be explicit about counts:
  - `1 as order_count` if the row grain is one order
  - `count(*)` only when counting grouped rows directly is the point
- Prefer explicit `group by` columns over opaque patterns when readability suffers. `dbt_utils.group_by()` is fine when the select list is stable and obvious.

### Ordering

- Don’t rely on `order by` in production models unless it is required for deterministic downstream behavior or debugging. Tables and views are unordered relations.
- `order by` is fine in `analysis/` queries and ad hoc exploration.

## Layer-specific guidance

### Staging models

Staging models should:

- map one source table to one model
- rename raw columns into clear analytics names
- do light casting and standardization
- avoid joins unless there is a very strong reason
- avoid business definitions that belong downstream

Good staging work:

- rename `o_orderkey` to `order_id`
- standardize `o_orderstatus` to `order_status_code`
- cast raw text dates to proper dates if needed

Bad staging work:

- joining orders to customers
- embedding revenue logic
- filtering out business states without a strong project-wide reason

### Intermediate models

Intermediate models are for reusable transformation steps:

- joins between conformed staging models
- derived calculations
- surrogate key generation
- grain changes that feed multiple downstream models

If logic is reused by more than one mart, it probably belongs here.

### Mart models

Marts are the business contract. They should:

- have a clear audience and business meaning
- expose polished, documented columns
- keep metrics and dimensions consistent across domains
- carry the strongest testing expectations

Facts should center on events or transactions. Dimensions should describe entities.

### Aggregate models

Only create aggregate models when they serve a concrete reporting or performance need. Don’t create a rollup just because a BI tool can query a fact table.

## Materializations

Follow the existing project defaults unless there is a clear reason to override:

- `staging`: `view`
- `intermediate`: `view`
- `marts`: `table`
- `_samples`: project-defined exceptions

Override materialization at the model level only when the model’s access pattern or cost profile justifies it.

If introducing an incremental model:

- document why incremental is necessary
- define a reliable `unique_key`
- filter on a stable timestamp or change indicator
- add tests that support the model contract

## YAML style

### General

- Keep model documentation in schema YAML files under `models/`.
- Use `data_tests:`, not `tests:`.
- Keep YAML close to the models it documents.
- Prefer reusable `doc()` blocks only when the same definition appears multiple times.

### Descriptions

Descriptions should explain meaning, not restate the name.

Weak:

```yaml
- name: order_date
  description: date of the order
```

Better:

```yaml
- name: order_date
  description: Date the customer placed the order.
```

For models, describe:

- the grain
- what the model represents
- any important exclusions or business rules

For columns, describe:

- business meaning
- units where relevant
- whether the value is a code, ID, derived metric, or human-readable label

### Tests

At a minimum, test the contract of important models:

- primary key columns: `unique` + `not_null`
- foreign keys used downstream: `relationships`
- constrained enums: `accepted_values`
- business-critical numeric fields: `not_null` where appropriate

Add tests where failure would be actionable. Don’t add low-signal tests just to increase counts.

## Documentation standards

Every business-facing mart should have:

- a meaningful model description
- documented key columns
- ownership where appropriate
- enough context that someone can use the model without reading the SQL

Use markdown docs blocks for shared definitions like standard financial metrics or common entity fields. Keep one-off explanations in YAML.

## Macros and Jinja

- Use macros to remove repeated logic, not to hide straightforward SQL.
- Prefer simple, obvious macro interfaces.
- Avoid heavy metaprogramming unless it clearly reduces maintenance cost.
- If a macro changes model SQL in a non-obvious way, document it.

## Snowflake-specific guidance

Since this project targets Snowflake:

- use Snowflake-safe SQL and built-ins consistently
- be deliberate with data types for numeric and date fields
- avoid unnecessary case-sensitive identifiers
- use pivots and warehouse-specific features only when they improve readability or performance enough to justify the lock-in

## Samples and training content

This repo includes sample content under `_samples/`. Treat it as illustrative, not as the standard to copy blindly into production-facing models.

When borrowing from samples:

- align naming with the real domain you’re working in
- remove training-only comments or shortcuts
- add proper docs and tests before treating the model as production-grade

## Pull request expectations

A solid change in this project should usually include:

- the model SQL change
- matching YAML updates
- appropriate tests
- validation via `dbt parse`, `dbt compile`, or a targeted `dbt build`
- a short explanation of grain and downstream impact in the PR

## Recommended defaults for new work

If you’re adding a new model, use this decision order:

1. Can I extend an existing model without muddying its grain?
2. If not, which layer should own this logic?
3. What is the model grain?
4. What are the key tests?
5. What should the business-facing names be?

If those five answers are clear, implementation usually goes smoothly.

## A few repo-specific cleanups worth following going forward

There are a couple of patterns in the current repo that I would not extend:

- avoid weak descriptions like “primary id of the model” or “date of the order”
- avoid unnecessary `order by` clauses in final model selects
- keep naming consistent between SQL and YAML column names, especially for status fields and metric names

That’s the bar I’d use for new contributions: match the folder and naming conventions already here, but write docs/tests a little tighter than some of the older examples.
