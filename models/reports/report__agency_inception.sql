with

agency as (

    select * from {{ ref('dim_agency') }}

)

, final as (

    select

        sales_office_code
        ,sales_office_sk
        ,count(distinct policy_num) as num_pol
        ,min(case 
                when extract(year from policy_original_inception_date) <= {{ var('min_year') }} 
                then null
                else cast(policy_original_inception_date as date)
        end) as policy_first_inception_date

    from agency
    group by sales_office_code, sales_office_sk

)

select * from final