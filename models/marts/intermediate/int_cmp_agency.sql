{{ join_coverage_and_policies(
    coverage_table=ref('stg_cmp__coverage_earned_premium_fact_v'),
    policy_table=ref('stg_cmp__policy_trx_v'),
    states=['AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA']
) }}