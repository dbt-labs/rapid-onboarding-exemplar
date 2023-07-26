{# https://github.com/dbt-labs/dbt-utils/tree/1.1.0/#pivot-source #}

select
  order_id,
  {{ dbt_utils.pivot(
      'payment_method',
      dbt_utils.get_column_values(ref('stg_stripe__payments'), 'payment_method'),
      agg='sum',
      suffix='_amount',
      then_value='amount',

  ) }}
from {{ ref('stg_stripe__payments') }}
group by 1