## Some examples of helpful syntax for testing specific tests

1. Test all generic tests

```
dbt test --select test_type:generic
```

2. Test all tests named unique

```
dbt test --select test_name:unique
```

3. Test a source named jaffle_shop

```
dbt test --select source:jaffle_shop
```