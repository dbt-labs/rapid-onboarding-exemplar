"""
UDFs
    You can use the @udf decorator or udf function to define an "anonymous" function
    and call it within your model function's DataFrame transformation. 
"""
# import Python packages
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import numpy

# define a temporary function, using a temporary snowpark UDF
def register_udf_add_random():
    add_random = F.udf(
        # use 'lambda' syntax, for simple functional behavior
        lambda x: x + numpy.random.randint(1,99),
        return_type=T.FloatType(),
        input_types=[T.FloatType()]
    )
    return add_random

# The required model() function must return a single DataFrame
def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["numpy"]
    )
    
    # bring in reference Python model as dataframe
    payments_glitch = dbt.ref("py04__define_function__payment_glitch")

    add_random = register_udf_add_random()

    # add money, who knows by how much, by calling function
    df = payments_glitch.withColumn("amount_plus_random", add_random("gross_item_sales_amount"))

    return df
