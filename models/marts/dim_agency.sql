{% set relations=[
    ref('int_ppa_agency')
    ,ref('int_ba_agency')
    ,ref('int_ho_agency')
    ,ref('int_ll_agency')
    ,ref('int_cmp_agency')
    ,ref('int_cmp_phx_agency')
] %}

{% set union_columns=[
    'lob'
    ,'underwriting_state_code'
    ,'policy_num'
    ,'policy_module_inception_date'
    ,'policy_original_inception_date'
    ,'month_end_date'
    ,'sales_office_sk'
    ,'sales_office_code'
] %}

with 

unioned as (
    
    {%- for model in relations %}
    
    select 
    
        {%- for column in union_columns %}
        {% if not loop.first %},{% endif %}{{ column }}
        {%- endfor %}
    
    from {{ model }}
    where rno = 1
    
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
    
)

select * from unioned 