# The required model() function must return a single DataFrame
def model(dbt, session):

    # bring in reference model as dataframe
    customers_df = dbt.ref("dim_customers")

    # do transformation - apply "describe" function
    described_df = customers_df.describe()

    # return dataframe
    return described_df
    