import holidays
import pandas

def model(dbt, session):
    dbt.config(materialized = "table",
    packages = [
        'pandas', 'holidays'
    ]
    )
    us_holidays = holidays.US()
    df = dbt.ref("date_spine").toPandas()
    df['is_holiday'] = df['DATE_DAY'].apply(lambda date: date in us_holidays)
    df = session.createDataFrame(df)
    return df