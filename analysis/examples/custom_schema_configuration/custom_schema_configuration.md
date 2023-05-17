## Custom Schema Configuration

There are lots of options to configure the way dbt generates schema names. Documentation can be 
found [here](https://docs.getdbt.com/docs/build/custom-schemas#advanced-custom-schema-configuration), but 
there are also some examples listed below. These examples are obviously not exhaustive, but 
should help provide a template for custom logic needed for overriding the default schema configuration.

In order to override the default behavior for dbt, you must create
a macro in your project named generate_schema_name.

### In target=prod only use the custom schema if available. In other environments just use target schema.

This is a common use case. It checks if the target value is exactly equal to the string 'prod'. 
If target == 'prod':
- if there is a custom schema, it will just use that 
- if there is no custom schema, it will use the target schema 

Else:
- it will just use the target schema 

This is such a common behavior that the itself ships with dbt and can be found [here](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/include/global_project/macros/get_custom_name/get_custom_schema.sql#L47-L60)

```
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}
```


### In environment=prod only use the custom schema if available. In other environments just use target schema.

This is very similar to the above, except it relies on an environment variable instead of the target. Let's assume the environment 
variable we are using is called `DBT_ENV_NAME` and we're checking to see if the value is `'prod'`. Sometimes it might be preferable
to use environment variables as opposed to target.

```
{% macro generate_schema_name_for_env(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if env_var("DBT_ENV_NAME") == 'prod' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
```

### In environment=prod only use the custom schema if available. In other environments concatenate the schemas

Very similar to the above example, except concatenating schemas in non-prod environments.

```
{% macro generate_schema_name_for_env(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if env_var("DBT_ENV_NAME") == 'prod' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
```