# {{ organization_name }} Analytics

This is a [dbt](https://www.getdbt.com) project for managing {{ organization_name }}'s analytics project.

Our analytics stack:
- Loader: {{ your_data_loading_tools }}
- Warehouse: {{ your_warehouse }}
- Transformation: dbt
- Business Intelligence: {{ your_bi_tool }}

## Permissions

Access to the {{ warehouse }} warehouse is managed on a per-user basis by {{ person_or_team_name }}. 
If you need access, open a request in {{ tool_or_location }}.

## Using This Project

<details>
  
  <summary>Developing in the Cloud IDE</summary>
  <p></p>
  
  The easiest way to contribute to this project is by developing in dbt Cloud. Contact {{ person_or_team_name }}. 
  If you need access, open a request in {{ tool_or_location }}.
  
  Once you have access, navigate to the to the "Credentials" section of your Profile Settings and connect to {{ warehouse }}
  using {{ auth_method }}.
  
  In the command line bar at the bottom of the interface, run the following commands one at a time:
  - `dbt build` - builds out your models, snapshots, and seeds in your target schema and runs all tests
  - `dbt deps` - build out necessary dependencies in your project

  Our organization utilizes {{ documentation_tool }} to outline the development standards and expectations for this project.
  As you are developing, be sure to leverage the [dbt docs site](https://docs.getdbt.com/). For continued learning of dbt 
  and development best-practices, visit [courses.getdbt.com](https://courses.getdbt.com).
  
</details>
  
## Managing Job Failures

Job status alerts can be managed in the project's Notification Settings. 
Our team handles job failures by {{ escalation_and_management_procedure }}