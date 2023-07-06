# Documentation of sample dbt concepts in this project

## Sources
**Source selection syntax [samples](sources/source_selection_syntax.md)**
- Includes samples of how to test sources, source freshness, and building downstream from a source

**Source freshness [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__sources.yml)**
- Includes freshness warnings, errors, and turning off source freshness on an individual source table

**Environment Variables to set source database [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__sources.yml)**
- Use an environment variable to control which database you're using for a source

**Target to set schema [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__sources.yml)**
- Use target to control which schema you're using for a source

**Quoting [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__sources.yml)**
- Set quoting on the source database, schema, and identifier for a given source table

**Loader [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__sources.yml)**
- Set the loader property on a source

## Models

**Model Configurations [samples](../models/_samples/staging/jaffle_shop/stg_jaffle_shop__customers.sql)**
- Set the grants, post_hook, alias, materialized, persist_docs, schema, and database configurations on a model
- The materialization strategy here overwrites the materialization set in the dbt_project.yml

**Set materialization in dbt_project.yml [samples](../dbt_project.yml)**
- Sets all models in the marts folder to be configured as a table by default
- Sets all models in the staging folder from the dbt_artifacts package to be materialized as ephemeral

## Tests

**Sample singular test on multiple models [samples](../tests/_samples/example_singular_test_multiple_models.sql)**
- Create a singular test on multiple models 

**Test configurations in a test [samples](../tests/_samples/audit_helper_compare_all_columns__stg_tpch__orders.sql)**
- Sets test configurations, including setting severity to warning, on a test

**Test configurations in a yml file [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__models.yml)**
- Configurations on a test including where, limit, severity, error if, warn if, and store failures

**Tests from packages [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__models.yml)**
- Shows tests from dbt_utils package 

**Helpful generic tests [samples](../tests/generic/)**
- filtered_unique.sql: a test to allow you to run a unique test on a subset of records. Column level test with custom argument/
- not_empty.sql: a simple test that makes sure that an object contains records. Model level test.
- unique_and_not_null.sql: a test that checks that a column is both unique and not null in a single test. Column level test

**Test a seed [samples](../seeds/samples/_seeds.yml)**
- Shows how to test a column on a seed

**Testing snapshots [samples](../snapshots/_samples/_example__snapshots.yml)**
- Shows how to test snapshots

**Store all test failures only in prod target [samples](../dbt_project.yml)**
- Sets the configurations in the dbt_project.yml file to store failures for tests only in prod

## Documentation
**Multi-line description [samples](../models/_samples/staging/jaffle_shop/_jaffle_shop__models.yml)**
- Shows multi-line description using `|` or `>`

**Create a doc block [samples](../models/staging/tpch/stg_tpch.md)**
- Shows how to create a doc block

**Setting description using a doc blocks [samples](../models/staging/tpch/_tpch__models.yml)**
- Shows how to call a doc block to set the description on an object

**Describe a seed [samples](../seeds/samples/_seeds.yml)**
- Shows how to add descriptions on seeds

**Documenting snapshots [samples](../snapshots/_samples/_example__snapshots.yml)**
- Shows how to document snapshots

## Seeds

**Create a seed [samples](../seeds/samples/alphabet_grouping.csv)**
- Example of a seed

**Test and describe a seed [samples](../seeds/samples/_seeds.yml)**
- Shows how to test a column on a seed

**Seed selection syntax [samples](seeds/seed_selection_syntax.md)**
- Commands for running seeds including `seed`, `build`, and using `--full-refresh`

## Incremental Models

**Incremental model [samples](../models/_samples/incremental/example_incremental_model.sql)**
- Example incremental model which filters records based on timestamp. Also includes sync_all_columns

**Incremental model with unique key [samples](../models/_samples/incremental/example_incremental_model_with_unique_key.sql)**
- Example incremental model which includes the unique_id configuration to ensure there are no duplicate reocrds

**Incremental model without a timestamp column [samples](../models/_samples/incremental/example_incremental_model_no_timestamp.sql)**
- Example incremental model which filters records based on an ID

**Incremental model with relative start and end dates to filter data [samples](../models/_samples/incremental/example_incremental_model_relative_start_and_end_dates.sql)**
- Example incremental model which filters records based on a relative timeframe

**Incremental model using project variables to define lookback window [samples](../models/_samples/incremental/example_incremental_model_using_project_variable_lookback_window.sql)**
- Example incremental model which filters records using a timestamp, but determining the window of data to process based on project variables

**Source data that incremental models are built upon [samples](../models/_samples/incremental/example_source_for_incremental.sql)**
- The "source" data that the incremental models are built on top of. The "source" dataset will have a record for every second (up until the current timestamp) in the current month
- This allows the source table to always have more data everytime we query it (which also explains why the source is confiugred as a view)
- To use the incremental models downstream of this "source" model, just build the "source" model once and then you can run any of the downstream incremental models as many times as you'd like

## Snapshots

**Example snapshot [samples](../snapshots/_samples/example_orders_snapshot.sql)**
- Example snapshot using the timestamp strategy

**Example snapshot with generate_schema macro [samples](../snapshots/_samples/example_generate_schema_snapshot.sql)**
- Example snapshot that sets the schema using the generate_schema macro. This allows the snapshot to go into a development schema when in the development environment

**Example snapshot using check strategy [samples](../snapshots/_samples/example_check_snapshot.sql)**
- Example snapshot using the check strategy

**Example model showing how to join snapshots [samples](../models/_samples/snapshot/example_join_snapshots.sql)**
- Example model which shows some of the complexity of joining snapshots

**Testing and documenting snapshots [samples](../snapshots/_samples/_example__snapshots.yml)**
- Shows how to test and document snapshots

## Python Models

**Simple python model using ref [samples](../models/_samples/python/py01__python_builtins__describe.py)**
- Creates python model using ref and builtin python describe

**Python model using target variable [samples](../models/_samples/python/py02__use_variables__customers_limit_10.py)**
- Creates python model and uses target variable

**Python model using and creating another python function [samples](../models/_samples/python/py03__define_function__payment_glitch.py)**
- Creates python model which uses another python function

**Python model using and creating a Snowpark UDF [samples](../models/_samples/python/py04__add_udf_function__payment_glitch.py)**
- Creates python model which creates and uses a Snowpark UDF

**YAML file for python models [samples](../models/_samples/python/_python__models.yml)**
- YAML file for python models which adds configurations for python models, including target variable

**Pyhon model with pypi package [samples](python/py03__import_pypi_package__holiday.py)**
- Creates python model which uses a pypi package. In the analysis folder because it takes a while to build

## Model Governance

**Example v1 of a model which is set as private in the finance group [samples](../models/_samples/model_governance/example_private_finance_model_v1.sql)**
- Example v1 of a model which is set as private in the finance group. The configurations are in the `_model_governance__models.yml` file

**Example model which is set as private in the finance group [samples](../models/_samples/model_governance/example_private_finance_model_v2.sql)**
- Example v2 of model which is set as private in the finance group. The configurations are in the `_model_governance__models.yml` file

**Example model selecting from v1 of a model [samples](../models/_samples/model_governance/example_selecting_from_old_version_of_private_model.sql)**
- Example model selecting from v1 of a model. The configurations are in the `_model_governance__models.yml` file

**Example model selecting from most recent version of a model [samples](../models/_samples/model_governance/example_selecting_from_private_model.sql)**
- Example model selecting from current version of a model. The configurations are in the `_model_governance__models.yml` file

**YAML file for model governance [samples](../models/_samples/model_governance/_model_governance__models.yml)**
- YAML file which sets group, access, contract, and versions of models

## Macros

**Macro using audit_helper to audit a model  [samples](../macros/_samples/audit_helper/audit_dim_customers.sql)**
- Macro which audits the dim_customers model to dim_customers in production 

**Custom schema macros [samples](custom_macros/custom_schema_configuration.md)**
- Markdown file which has lots of different macros in it to customize the way generate schema works in the dbt project

**Custom alias macros [samples](custom_macros/custom_alias_configuration.md)**
- Markdown file which has lots of different macros in it to customize the way generate alias works in the dbt project

## Tags

**Tag selection syntax [samples](tags/tag_selection_syntax.md)**

**Set tags in dbt_project [samples](../dbt_project.yml)**

## Node Selection Syntax

**Model selection syntax [samples](node_selection_syntax/model_selection_syntax.md)**

**Seed selection syntax [samples](node_selection_syntax/seed_selection_syntax.md)**

**Source selection syntax [samples](node_selection_syntax/source_selection_syntax.md)**

**Tag selection syntax [samples](node_selection_syntax/tag_selection_syntax.md)**

**Test selection syntax [samples](node_selection_syntax/test_selection_syntax.md)**

## Helpful Packages

### dbt_codegen

**Generate base model [samples](dbt_codegen/generate_base_model.sql)**
- How to use generate_base_model to create the staging model for a source

**Generate model import CTEs [samples](dbt_codegen/generate_model_import_ctes.sql)**
- Shows how to use generate_model_import_ctes to clean up a model

**Generate model yaml [samples](dbt_codegen/generate_model_yaml.sql)**
- How to use generate_model_yaml to create the yaml for a model

**Generate source [samples](dbt_codegen/generate_source.sql)**
- Shows how to generate the source yaml for a given database and schema

### dbt_utils

**Date spine [samples](dbt_utils/date_spine.sql)**

**Get column values [samples](dbt_utils/get_column_values.sql)**

**Pivot [samples](dbt_utils/pivot.sql)**


### dbt_project_evaluator

**Run dbt project evaluator [samples](dbt_project_evaluator/syntax_run_package.md)**
- Syntax to run the dbt project evaluator package

### dbt_audit_helper

**Compare queries [samples](audit_helper/compare_queries.sql)**

**Compare relation columns [samples](audit_helper/compare_relation_columns.sql)**

**Compare relations [samples](audit_helper/compare_relations.sql)**

### dbt_artifacts

**Run dbt artifacts [samples](dbt_artifacts/syntax_run_package.md)**
- Syntax to run the dbt artifacts package

**Query dbt invocations [samples](dbt_artifacts/dbt_artifacts_fct_dbt__invocations.sql)**
- Query to view dbt invocations which gets created as a model from the dbt artifacts package

**Query model executions [samples](dbt_artifacts/dbt_artifacts_fct_dbt__model_executions.sql)**
- Query to view each model execution which gets created as a model from the dbt artifacts package

**On run end hook for dbt_artifacts [samples](../dbt_project.yml)**
- Only runs dbt_artifacts on-run-end only when the target variable is set to prod


## Deployment

**Daily Production Job [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/38569)**
- Standard dbt build job scheduled to run once a day (but the schedule is turned off)

**Sample Hourly Job [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/347072)**
- Shows how to create a job using tag selection syntax, exclude syntax, and scheduled to run hourly (though the schedule is turned off)

**Sample Incremental Job Every 30 Minutes Except Sunday [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/365207)**
- Shows incremental job (not a full refresh) which is scheduled every 30 mintues except Sunday (schedule turned off). This is because on Sunday we want to run a full refresh once

**Sample Incremental Job Every 30 Minutes Sunday [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/365219)**
- Shows incremental job (not a full refresh) which is scheduled every 30 mintues on Sunday. Does not run from 5am to 8am on Sunday because we have a full refresh job running on Sunday at 5am

**Sample Full Refresh Job Sundays [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/365211)**
- Shows incremental job with a full refresh which is run at 5am on Sunday (schedule turned off). The other two incremental jobs work around this job so they don't run at the same time.

**Daily Staging Job [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/368562)**
- Standard dbt build job in the staging environment run once a day (but the schedule is turned off)

**CI Job [url](https://cloud.getdbt.com/deploy/26712/projects/48031/jobs/41092)**
- Standard CI job using dbt build --select state:modified+ to only build models you've modified in a PR and anything downstream of those models


## dbt Cloud API v2

**Trigger a dbt Cloud job using the API [samples](dbt_cloud_api_v2/trigger_job.py)**

## Other

### Working with JSON Columns in Snowflake

**Snowflake flatten JSON [samples](json/snowflake_flatten_json.sql)**
- Snowflake functionality to demo how SQL can be used to parse json in a single column 

**Snowflake identify key value pair [samples](json/snowflake_identify_value_key_pairs.sql)**
- Snowflake functionality to provide you with every key value pair using Snowflake functionality