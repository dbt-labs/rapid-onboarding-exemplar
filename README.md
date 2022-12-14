# dbt Training - Rapid Onboarding Exemplar

This is a [dbt](https://www.getdbt.com) project for dbt Lab's Rapid Onboarding training.

Our analytics stack:
- Loader: Snowflake's TPCH sample data
- Warehouse: Snowflake
- Transformation: dbt

## Permissions

Access to the Snowflake warehouse is managed on a per-user basis by the training team. 
If you need access, open a request in the #training-rapid-onboarding Slack channel.

## Using This Project

<details>
  
  <summary>Developing in the Cloud IDE</summary>
  <p></p>
  
  The easiest way to contribute to this project is by developing in dbt Cloud. If you need access, contact the training team
  in the #training-rapid-onboarding Slack channel.
  
  Once you have access, navigate to the develop tab in the menu and fill out any required information to get connected.
  
  In the command line bar at the bottom of the interface, run the following commands one at a time:
  - `dbt deps`  - installs any packages defined in the packages.yml file.
  - `dbt seed`  - builds any .csv files as tables in the warehouse. These are located in the data folder of the project.
  - `dbt run`   - builds the models found in the project into your dev schema in the warehouse.
  
</details>
  

<details>
  
  <summary>Local Development</summary>
  <p></p>
  
  1. ### Install Requirements
      [Install dbt](https://docs.getdbt.com/dbt-cli/installation).   
      Optionally, you can [set up venv to allow for environment switching](https://discourse.getdbt.com/t/setting-up-your-local-dbt-run-environments/2353). 

  2. ### Setup
      Open your terminal and navigate to your `profiles.yml`. This is in the `.dbt` hidden folder on your computer, located in your home directory.

      On macOS, you can open the file from your terminal similar to this (which is using the Atom text editor to open the file):
      ```bash
      $ atom ~/.dbt/profiles.yml
      ```

      Insert the following into your `profiles.yml` file and change out the bracketed lines with your own information.
      [Here is further documentation](https://docs.getdbt.com/docs/available-adapters#dbt-labs-supported) for setting up your profile.
      ```yaml
      my_project:                                          
       target: dev                                         
       outputs:                 
         dev:                                              
           type: [warehouse name]                                 
           threads: 8                                      
           account: [abc12345.us-west-1]                   
           user: [your_username]                           
           password: [your_password]                       
           role: transformer                               
           database: analytics                             
           warehouse: transforming                         
           schema: dbt_[your_name]                         
      ```
      | Configuration Key| Definition
      |-------------------------------|------------------------------------------------------------------------------------------------------------------|
      | my_project                    | This is defining a profile - this specific name should be the profile that is referenced in our dbt_project.yml  |
      | target: dev                   | This is the default environment that will be used during our runs.                                               |
      | outputs:                      | This is a prompt to start defining targets and their configurations. You likely won't need more than `dev`, but this and any other targets you define can be used to accomplish certain functionalities throughout dbt.|
      | dev:                          | This is defining a target named `dev`.                                                                           |
      | type: [warehouse_name]        | This is the type of target connection we are using, based on our warehouse.                                      |
      | threads: 8                    | This is the amount of concurrent models that can run against our warehouse, for this user, at one time when conducting a `dbt run` |
      | account: [abc12345.us-west-1] | Change this out to the warehouse's account.                                                                      |
      | user: [your_username]         | Change this to use your own username that you use to log in to the warehouse                                     |
      | password: [your_password]     | Change this to use your own password for the warehouse                                                           |
      | role: transformer             | This is the role that has the correct permissions for working in this project.                                   |
      | database: analytics           | This is the database name where our models will build                                                            |
      | schema: dbt_[your_name]       | Change this to a custom name. Follow the convention `dbt_[first initial][last_name]`. This is the schema that models will build into / test from when conducting runs locally.|

   3. ### Running dbt
      
      Run the following commands one at a time from your command line:
      - `dbt debug` - tests your connection. If this fails, check your profiles.yml.
      - `dbt deps`  - installs any packages defined in the packages.yml file.
      - `dbt seed`  - builds any .csv files as tables in the warehouse. These are located in the data folder of the project.
      - `dbt run`   - builds the models found in the project into your dev schema in the warehouse.
  
</details>

<details>

<summary>Local Development</summary>
<p></p>

### What to do when a dbt Cloud job fails

  - dbt Cloud is set up to notify us of job failures via the #analytics-status-updates Slack channel. In case of a failure, Erica will notify the greater company via the #analytics Slack channel. 
  - The Analytics Engineer from the relevant department will troubleshoot the failure within 24 hours and continue to update stakeholders via #analytics
  - The resolution and summarized post-mortem report will be posted in #analytics. This post-mortem will include:
    - Summary of the outage (e.g. failing tests in our Postgres db)
    - The cause of the outage and how we came to a resolution (e.g. ELT issue with Fivetran)
    - Measures we've taken to ensure this doesn't happen again (e.g. Adding new tests, updating processes, etc.)
    - Actual TTD and TTR

</details>
