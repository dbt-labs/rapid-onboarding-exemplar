## Some examples of helpful syntax for using seeds

1. Create all seeds in your warehouse without testing any

```
dbt seed
```

2. Build a seed named alphabet_grouping in your warehouse including testing it

```
dbt build --select alphabet_grouping
```

3. Full refresh your seed so that dbt will drop and recreate the object instead of truncating and inserting.
This is useful when you add or remove columns from a seed

```
dbt build --select alphabet_grouping --full-refresh
```