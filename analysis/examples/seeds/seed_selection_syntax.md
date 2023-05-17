## Some examples of helpful syntax for using seeds

1. Create all seeds in your warehouse without testing any

```
dbt seed
```

2. Build a seed named alphabet_grouping in your warehouse including testing it

```
dbt build --select alphabet_grouping
```