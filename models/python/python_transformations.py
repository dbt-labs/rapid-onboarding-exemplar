import string
import random
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")

    upstream_model = dbt.ref("dim_orders")

    letters = string.ascii_lowercase + string.ascii_uppercase + string.digits
    default_salt = (''.join(random.choice(letters) for i in range(30)))

    columns_without_cc_number = upstream_model[['order_sk', 'order_id', 'order_date', 'order_country_origin',  'cust_fname', 'cust_lname']]

    # Create DataFrame with the string as a single row in one column
    hashed_value = pd.DataFrame([default_salt], columns=['hashed_id'])

    return columns_without_cc_number
    