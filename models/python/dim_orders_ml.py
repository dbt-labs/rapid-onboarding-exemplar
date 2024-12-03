import numpy
import pandas as pd

def model(dbt, session):

    dbt.config(materialized="table")

    upstream_model = dbt.ref("dim_orders")
    sorted = upstream_model.sort_values(by="CUST_LNAME")

    print("This is a line in python")

    # Process data with the external package
    
    
    # Return the DataFrame as the model output
    return sorted

    