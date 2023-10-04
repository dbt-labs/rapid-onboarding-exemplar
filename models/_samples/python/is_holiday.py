import holidays
import pandas

#all python models need to be defined at the start with this specific syntax
def model(dbt, session):

#python models don't use Jinja. Here we are using dbt.config to create model configurations
#be sure to materialize python models as tables, and to specify the packages that were imported above
    dbt.config(
        materialized="table",
        packages=['pandas', 'holidays']
    )

    us_holidays = holidays.US()

#python models don't use Jinja. Here we are using dbt.ref to create model references
    df = dbt.ref('date_spine').to_pandas()

#when creating a new column, it is recommended that you make it all caps
    df['IS_HOLIDAY'] = df['DATE_DAY'].apply(lambda date: date in us_holidays)

#in dbt, you always need to return your data frame at the end of your models
    return df
    