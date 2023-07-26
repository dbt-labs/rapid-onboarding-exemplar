## Some examples of helpful syntax for using tags

1. Build all models that have the hourly tag

```
dbt build --select tag:hourly
```

2. Build all models and their downstream dependencies that have the hourly tag

```
dbt build --select tag:hourly+
```

3. Build all models that have both the finance tag AND the orders tag

```
dbt build --select tag:hourly,tag:orders
```

4. Build all models that have both the finance tag OR the orders tag

```
dbt build --select tag:hourly tag:orders
```
