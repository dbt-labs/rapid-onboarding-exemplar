
## Custom Alias Configuration

There are lots of options to configure the way dbt generates alias names. Documentation can be 
found [here](https://docs.getdbt.com/docs/build/custom-aliases), but 
there are also some examples listed below. These examples are obviously not exhaustive, but 
should help provide a template for custom logic needed for overriding the default alias configuration.

In order to override the default behavior for dbt, you must create
a macro in your project named generate_alias_name.

### In target.name = dev or target.name = CI (set in environment settings), use a specific alias for the model

If target.name = dev or target.name = CI (set in environment settings), use a specific alias configuration. 
This prepends the user's dev target schema (ie. dbt_bhipple or dbt_pr_42) to the model name. 
Everywhere else, use the actual model name for the object name. 
This is particularly useful when developers can't use their own dev schema but don't want to overwrite each other's objects, so they need a distinct object name in dev or in CI.

```
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if target.name == 'dev' or target.name == 'CI' -%}
        {%- if custom_alias_name is none -%}
            {{ target.schema }}__{{ node.name }}
        {%- else -%}
            {{ custom_alias_name | trim }}
        {%- endif -%}
    {%- else  -%}
        {%- if custom_alias_name is none -%}
            {{ node.name }}
        {%- else -%}
            {{ custom_alias_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
```