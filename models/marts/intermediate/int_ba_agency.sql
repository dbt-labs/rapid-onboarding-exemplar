{{ join_coverage_and_policies(
    coverage_table=ref('stg_ba__coverage_earned_premium_fact_v'),
    policy_table=ref('stg_ba__policy_trx_v'),
    states=['CA','FL','OK','TX']
) }}