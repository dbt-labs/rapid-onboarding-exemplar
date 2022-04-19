{{ join_coverage_and_policies(
    coverage_table=ref('stg_ppa__coverage_earned_premium_fact_v'),
    policy_table=ref('stg_ppa__policy_trx_v'),
    states=['AZ','CA','FL','GA','IL','NJ','NV','NY','OK','TX','VA']
) }}