import string
import random
import pandas
import hashlib

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=['pandas']
    )

    df = dbt.ref("dim_orders").to_pandas()

    letters = string.ascii_lowercase + string.ascii_uppercase + string.digits
    default_salt = (''.join(random.choice(letters) for i in range(30)))

    # Define the variable to concatenate with
    variable_hash = 'some_secret_salt'

    # Use lambda to concatenate the column with variable_hash, and then hash the result
    df = df.assign(HASHED = lambda x: (x['ORDER_COUNTRY_ORIGIN'] + default_salt))

    return df
    