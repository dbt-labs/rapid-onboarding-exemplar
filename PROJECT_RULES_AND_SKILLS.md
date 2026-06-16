# Project rules and skills

This document is the practical companion to `STYLEGUIDE.md` for `rapid_onboarding_exemplar`.

Use it as the default operating guide when building or changing dbt assets in this repo.

## What this project is optimizing for

This project is set up to teach and reinforce good dbt habits on Snowflake:

- thin `staging` models
- reusable `intermediate` models
- business-facing `marts`
- selective `aggregates` when they solve a real need
- strong documentation and data tests
- readable SQL over clever SQL

If you are unsure where logic belongs, favor the simpler layered approach already present in the repo.

## Non-negotiable rules

### 1. Preserve the layer boundaries

Use the existing structure from `dbt_project.yml`:

- `models/staging`: source-conformed cleanup and renaming
- `models/intermediate`: joins, reshaping, grain changes, reusable calculations
- `models/marts`: fact and dimension models for end users
- `models/aggregates`: presentation or performance rollups only when justified
- `analysis`: ad hoc SQL, not part of the production DAG

Do not put business definitions into staging unless there is a very strong reason.

### 2. Every model needs a clear grain

Before writing SQL, be able to say what one row represents.

Examples:

- `fct_orders`: one row per order
- `fct_order_items`: one row per order item
- `dim_customers`: one row per customer

If the grain changes, that is usually a new model rather than a quiet edit.

### 3. Use explicit dependencies

Always use `ref()` and `source()` in model SQL.

Do not hardcode database objects in production models.

### 4. Keep marts explicit

For marts and aggregates:

- avoid `select *`
- prefer explicit select lists
- document important business logic in YAML
- test the contract, especially keys and relationships

### 5. YAML must use modern dbt syntax

Use `data_tests:` in schema YAML files, not `tests:`.

Keep model docs and tests in the colocated YAML file for the domain, such as:

- `models/marts/finance/_finance__models.yml`
- `models/intermediate/finance/_int_finance__models.yml`
- `models/staging/tpch/_tpch__models.yml`

### 6. Match existing naming patterns

Use the naming conventions already established in the repo:

- staging: `stg_<source>__<entity>`
- intermediate: `int_<domain>_<purpose>`
- facts: `fct_<entity>`
- dimensions: `dim_<entity>`
- aggregates: `agg_<purpose>`

Column naming should stay analytics-friendly:

- IDs end in `_id`
- dates end in `_date`
- timestamps end in `_at`
- money fields end in `_amount`
- codes end in `_code`

### 7. Don’t confuse training samples with production standards

This repo includes `_samples/` and template content. Treat those as examples, not as production-ready defaults.

If you borrow from them:

- rename things to the real business domain
- remove training shortcuts
- add proper docs and tests

## Working rules by asset type

### Staging models

Good staging models:

- map one source table to one model
- rename raw columns
- standardize types
- clean obvious null or formatting issues

Avoid in staging:

- broad joins
- metric logic
- business filters that remove valid source history

### Intermediate models

Intermediate is where reusable transformation work belongs:

- joining conformed staging models
- generating surrogate keys
- building reusable order-item level logic
- reshaping data for downstream marts

If more than one mart will need the logic, it probably belongs here.

### Mart models

Marts are the published contract.

They should:

- have a clearly stated grain
- expose polished column names
- aggregate or model business logic intentionally
- carry strong docs and tests

For a mart like `fct_orders`, the expectation is that it is understandable without reading upstream SQL.

### Aggregate models

Only build aggregate models when they improve performance or support a well-defined reporting pattern. Avoid creating rollups just because they seem convenient.

## Required documentation and tests

At a minimum, business-facing marts should have:

- a model description that states the grain
- descriptions for important dimensions and measures
- `unique` and `not_null` on the primary key
- `relationships` tests on important foreign keys
- `accepted_values` where a coded field has a controlled domain

Good docs explain business meaning, not just the field name.

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

## SQL rules for this repo

### Prefer readable CTEs

A good default shape is:

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

### Avoid hidden behavior in published models

Macros are fine when they remove repetition, but don’t hide simple business logic behind Jinja unless it genuinely improves maintainability.

### Be careful with aggregation helpers

`dbt_utils.group_by()` is acceptable, but if it makes the mart harder to read, switch to an explicit `group by` list.

### Don’t rely on `order by` in models

Tables and views are unordered relations. Keep `order by` for `analysis/` queries or debugging unless there is a specific reason it matters.

## Snowflake-specific expectations

Because this project targets Snowflake:

- be deliberate with numeric precision and date types
- avoid unnecessary quoted identifiers
- use Snowflake-specific features only when they clearly help
- keep SQL portable unless warehouse-specific behavior is the point

## Skills contributors should apply in this project

### 1. Modeling skill

Be able to:

- identify the right layer for new logic
- define model grain before writing SQL
- choose between fact, dimension, and aggregate patterns
- keep business logic centralized and reusable

### 2. Testing skill

Be able to:

- add key data tests in schema YAML
- test model contracts, not just row existence
- pick high-signal tests that will drive action when they fail

### 3. Documentation skill

Be able to:

- describe what a model represents
- explain grain and important business rules
- write useful column descriptions for IDs, codes, and measures

### 4. Refactoring skill

Be able to spot when:

- a staging model is doing too much
- a mart should be split into an intermediate plus mart pair
- repeated logic should move into a reusable upstream model or macro

### 5. Snowflake + performance judgment

Be able to:

- choose when a table is better than a view
- introduce incremental logic only when necessary
- avoid extra work in marts that are queried frequently

### 6. Repo hygiene

Be able to:

- keep SQL and YAML aligned in the same domain folder
- follow existing file naming patterns
- avoid editing generated or vendored directories like `target/` and `dbt_packages/`

## Recommended workflow for changes

1. Find the correct layer and domain folder.
2. Define the model grain.
3. Write or edit the SQL with explicit `ref()` or `source()` usage.
4. Update the colocated YAML with descriptions and `data_tests:`.
5. Validate the change with the lightest meaningful dbt command.
6. Make sure the final model is readable without detective work.

## Repo-specific examples

### A good finance mart change

If you need a new order-level finance metric:

- calculate reusable line-item logic in intermediate if multiple marts need it
- aggregate to the order grain in `models/marts/finance/`
- document the metric in `models/marts/finance/_finance__models.yml`
- test the model key and any important downstream relationships

### A bad finance mart change

Avoid editing `fct_orders` to:

- join directly to raw sources
- sneak in customer dimension cleanup that belongs in staging or intermediate
- expose dozens of inherited columns through `select *`

## When to update this document

Update this file when the repo changes in a way that affects how contributors should work, especially if:

- a new layer or major domain is introduced
- naming conventions change
- testing expectations change
- the team adopts new dbt patterns worth standardizing

## Related project docs

- `STYLEGUIDE.md`: detailed style and modeling conventions
- `README.md`: project setup and onboarding
- domain YAML files under `models/`: model-level documentation and tests
