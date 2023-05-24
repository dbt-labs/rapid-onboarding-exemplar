## Some examples of helpful syntax for using sources

### Testing Sources

1. Test all of your sources

```
dbt test --select source:*
```

2. Test all tables in your source named jaffle_shop

```
dbt test --select source:jaffle_shop
```

3. Test a specific source table named orders within your jaffle_shop source

```
dbt test --select source:jaffle_shop.orders
```

### Source Freshness
1. Run source freshness on all of your sources

```
dbt source freshness --select source:*
```

2. Run source freshness on all tables in your source named jaffle_shop

```
dbt source freshness --select source:jaffle_shop
```

3. Run source freshness on a specific source table named orders within your jaffle_shop source

```
dbt source freshness --select source:jaffle_shop.orders
```

### Building (running and testing) downstream sources 

1. Build (run and test) all models downstream of a source named jaffle_shop

```
dbt build --select source:jaffle_shop+
```

2. Build (run and test) all models downstream specific source table named orders within your jaffle_shop source

```
dbt build --select source:jaffle_shop.orders+
```