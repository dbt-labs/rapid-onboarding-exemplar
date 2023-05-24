# The required model() function must return a single DataFrame
def model(dbt, session):
    # bring in variable from config.yml
    target_name = dbt.config.get("target_name")

    # bring in reference model as dataframe
    customers_df = dbt.ref("dim_customers")

    # limit data in dev, using if statement and variable from config.yml, 
    # IF your project target is set to "dev" (and not left as "default")
    if target_name == "dev":
        customers_df = customers_df.limit(10)

    # return dataframe
    return customers_df
    