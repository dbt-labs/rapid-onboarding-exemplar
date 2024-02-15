select max(date_day) from {{ ref('date_spine') }};

select * from {{ ref('sample_model_1') }};

select * from {{ ref('sample_model_2') }};

select * from {{ ref('sample_model_3') }};