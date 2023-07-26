## Some examples of helpful syntax for building models

1. Build dim_customers and all children

```
dbt build --select dim_customers+
```

2. Build dim_customers and all parents

```
dbt build --select +dim_customers
```

3. Build dim_customers and all parents and children

```
dbt build --select +dim_customers+
```

4. Build dim_customers, it's first degree parents, and it's first degree children

```
dbt build --select 1+dim_customers+1
```

5. Build dim_customers, it's children, and all of it's children's parents

```
dbt build --select @dim_customers
```

6. Build all models in the dbt_artifacts package

```
dbt build --select dbt_artifacts
```

7. Build all models in the marts/finance directory

```
dbt build --select marts.finance
```

8. Build all models in the marts/finance directory except fct_orders and it's children

```
dbt build --select marts.finance --exclude fct_orders+
```

9. Build all incremental models

```
dbt build --select config.materialized.incremental
```

10. Build all models (and seeds and snapshots) in the selector all_models_and_tagged_snapshots

```
dbt build --selector all_models_and_tagged_snapshots
```

11. Build version 2 (latest version) of a model called example_private_finance_model which has an alias of example_private_finance_model

```
dbt build --select example_private_finance_model
```

12. Build version 1 of a model called example_private_finance_model

```
dbt build --select example_private_finance_model.v1
```