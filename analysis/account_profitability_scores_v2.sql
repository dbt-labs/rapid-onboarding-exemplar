-- ## This is the legacy version of the code ## -- 

-- Matt Black
-- 2022-03-27

-- Extract important exposure, premium, claim, and loss data for all LOBs
--   ==> Earned Exposure Days (EE)
--   ==> Earned Premium (EP)
--   ==> Claim Counts: including closed w/o pay but excluding CATs
--   ==> Claim Count: excluding closed w/o pay but including CATs
--   ==> Net Incurred Losses
--   ==> Net Paid Losses

-- Test Agency Sales Office Codes
--      '0009999'
--      ,'109999'
--      ,'NV999'
--      ,'OK999'
--      ,'IL999'
--      ,'AZ999'
--      ,'NY999'
--      ,'VA999'
--      ,'NJ999'
--      ,'TX999'
--      ,'GA999'
--      ,'FL999'
--      ,'311111'
--      ,'A000'
--      ,'NV000'


-- ## Replaced with vars config in dbt_project.yml
----
---- Declare variables in table (this is how you have to do it for Redshift SQL)
----
drop table if exists #vars;
select
        cast('2019-01-01' as date) as start_date
        ,cast('2021-12-31' as date) as end_date
        ,cast('2022-03-31' as date) as end_trans_date
        ,1930 as min_year
        ,2900 as max_year
        ,10.0 as min_loss
        ,0.8000 as LRR_Green_Auto_6mo_Mod1
        ,0.8500 as LRR_Green_Auto_6mo_Mod2
        ,0.8800 as LRR_Green_Auto_6mo_Mod3
        ,0.9100 as LRR_Green_Auto_6mo_Mod4
        ,0.9300 as LRR_Green_Auto_6mo_Mod5
        ,0.9450 as LRR_Green_Auto_6mo_Mod6
        ,0.9600 as LRR_Green_Auto_6mo_Mod7
        ,0.9725 as LRR_Green_Auto_6mo_Mod8
        ,0.9850 as LRR_Green_Auto_6mo_Mod9
        ,0.9950 as LRR_Green_Auto_6mo_Mod10
        ,0.9350 as LRR_Green_Auto_1yr_Mod1
        ,0.9850 as LRR_Green_Auto_1yr_Mod2
        ,0.8850 as LRR_Green_Prop_1yr_Mod1
        ,0.9450 as LRR_Green_Prop_1yr_Mod2
        ,0.9800 as LRR_Green_Prop_1yr_Mod3
        ,50000  as Loss_Cap_PPA -- Phu caps each coverage at $25K in PPA cube
        ,100000 as Loss_Cap_BA  -- originally had $75K, but too many large losses
        ,150000 as Loss_Cap_HO  -- originally had $100K, but Phu caps at $150K in HO cube
        ,150000 as Loss_Cap_DF
        ,200000 as Loss_Cap_LL
        ,250000 as Loss_Cap_CMP
into #vars
;

----
---- Load Credibility Table
----
DROP TABLE IF EXISTS #zip2region;
SELECT 
        state
        ,lpad(zip, 5, '0') as zip
        ,region_xs
        ,region_s
        ,region_m
        ,region_l
        ,region_xl
        ,cast(replace(ee, ',', '') as bigint) as ee
        ,cast(replace(claim_counts_w_nopay_xcat, ',', '') as bigint) as claim_counts_w_nopay_xcat
        ,cast(replace(claim_counts_w_cat_wo_npay, ',', '') as bigint) as claim_counts_w_cat_wo_npay
INTO #zip2region
FROM analytics_product.zip_to_region_mappings_credibility
order by zip
;

----
---- Load Master Agency Code mappings
----
drop table if exists #mac;
select distinct
        trim(master_agency_code) as master_agency_code
        ,trim(child_producer) as child_producer
        ,accounting_month
into #mac
from analytics_product.master_agency_code
where cast(accounting_month as bigint) between 201701 and 202112
;

----
---- Assign every policy to an agency
----
drop table if exists #agency;
with ppa as (
        select
                'PPA' as lob
                ,t2.underwriting_state_code
                ,t2.policy_num
                ,t2.policy_module_inception_date
                ,t2.policy_original_inception_date -- added to determine agency appointment date
                ,t1.month_end_date
                ,t1.sales_office_sk
                ,trim(t2.sales_office_code) as sales_office_code
                ,row_number() over
                        (partition by 
                                t2.underwriting_state_code
                                ,t2.policy_num
                        order by 
                                t2.policy_module_inception_date desc
                                ,t1.month_end_date desc
                                ,t1.sales_office_sk desc
                                ,trim(t2.sales_office_code) desc
                        ) rno 
        FROM edw_dm_ppa_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
        inner join edw_dm_ppa_v.POLICY_TRX_V t2 on (t1.ppa_policy_trx_sk = t2.ppa_policy_trx_sk)
        WHERE 
                t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)
                and t2.underwriting_state_code in ('AZ','CA','FL','GA','IL','NJ','NV','NY','OK','TX','VA')
)
,ba as (
        SELECT
                'BA' as LOB
                ,t2.underwriting_state_code
                ,t2.policy_num
                ,t2.policy_module_inception_date
                ,t2.policy_original_inception_date -- added to determine agency appointment date                
                ,t1.month_end_date
                ,t1.sales_office_sk
                ,trim(t2.sales_office_code) as sales_office_code
                ,row_number() over
                        (partition by 
                                t2.underwriting_state_code
                                ,t2.policy_num
                        order by 
                                t2.policy_module_inception_date desc
                                ,t1.month_end_date desc
                                ,t1.sales_office_sk desc
                                ,trim(t2.sales_office_code) desc
                        ) rno 
        FROM edw_dm_ba_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
        inner join edw_dm_ba_v.POLICY_TRX_V t2 on (t1.ba_policy_trx_sk = t2.ba_policy_trx_sk)
        WHERE 
                t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
                and t2.underwriting_state_code in ('CA','FL','OK','TX')
)
,ho as (
        SELECT
                'HO' as LOB
                ,t2.underwriting_state_code
                ,t2.policy_num
                ,t2.policy_module_inception_date
                ,t2.policy_original_inception_date -- added to determine agency appointment date
                ,t1.month_end_date
                ,t1.sales_office_sk
                ,trim(t2.sales_office_code) as sales_office_code
                ,row_number() over
                        (partition by 
                                t2.underwriting_state_code
                                ,t2.policy_num
                        order by 
                                t2.policy_module_inception_date desc
                                ,t1.month_end_date desc
                                ,t1.sales_office_sk desc
                                ,trim(t2.sales_office_code) desc
                        ) rno 
                FROM edw_dm_ho_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
                inner join edw_dm_ho_v.POLICY_TRX_V t2 on (t1.ho_policy_trx_sk = t2.ho_policy_trx_sk)
                WHERE 
                        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
                        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
)
,ll as (
        SELECT
                'LL' as LOB
                ,t2.underwriting_state_code
                ,t2.policy_num
                ,t2.policy_module_inception_date
                ,t2.policy_original_inception_date -- added to determine agency appointment date
                ,t1.month_end_date
                ,t1.sales_office_sk
                ,trim(t2.sales_office_code) as sales_office_code
                ,row_number() over
                        (partition by 
                                t2.underwriting_state_code
                                ,t2.policy_num
                        order by 
                                t2.policy_module_inception_date desc
                                ,t1.month_end_date desc
                                ,t1.sales_office_sk desc
                                ,trim(t2.sales_office_code) desc
                        ) rno 
                FROM edw_dm_LL_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
                inner join edw_dm_LL_v.POLICY_TRX_V t2 on (t1.LL_policy_trx_sk = t2.LL_policy_trx_sk)
                WHERE 
                        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
                        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')

)
,cmp as (
        SELECT
                'CMP' as LOB
                ,t2.underwriting_state_code
                ,t2.policy_num
                ,t2.policy_module_inception_date
                ,t2.policy_original_inception_date -- added to determine agency appointment date                
                ,t1.month_end_date
                ,t1.sales_office_sk
                ,trim(t2.sales_office_code) as sales_office_code
                ,row_number() over
                        (partition by 
                                t2.underwriting_state_code
                                ,t2.policy_num
                        order by 
                                t2.policy_module_inception_date desc
                                ,t1.month_end_date desc
                                ,t1.sales_office_sk desc
                                ,trim(t2.sales_office_code) desc
                        ) rno 
                FROM edw_dm_cmp_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
                inner join edw_dm_cmp_v.POLICY_TRX_V t2 on (t1.cmp_policy_trx_sk = t2.cmp_policy_trx_sk)
                WHERE 
                        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
                        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
)
,cmp_phx as (
        SELECT
                'CMP PHX' as LOB
                ,pt.underwriting_state_code
                ,pt.policy_number as policy_num
                ,pt.module_effective_date as policy_module_inception_date
                ,pt.policy_original_inception_date -- added to determine agency appointment date                
                ,t1.month_end_date
                ,pt.f_sales_office_key as sales_office_sk
                ,trim(pt.sales_office_code) as sales_office_code
                ,row_number() over
                        (partition by 
                                pt.underwriting_state_code
                                ,pt.policy_number
                        order by 
                                pt.module_effective_date desc
                                ,t1.month_end_date desc
                                ,pt.transaction_effective_date desc
                                ,pt.last_update_ts desc
                                ,t1.module_effective_date desc
                                ,t1.transaction_effective_date desc
                                ,t1.last_update_ts desc
                                ,pt.f_sales_office_key desc
                                ,trim(pt.sales_office_code) desc
                        ) rno 
                FROM dwhub_cmp_df_v.policy_premium_monthly_fact_view t1
                inner join dwhub_cmp_df_v.POLICY_TRANSACTION_VIEW pt on (t1.f_policy_transaction_key = pt.policy_transaction_key)
                WHERE 
                        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
                        and pt.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')   
)
select lob, underwriting_state_code, policy_num, policy_module_inception_date, policy_original_inception_date, month_end_date, sales_office_sk, sales_office_code
into #agency
from ppa
where rno = 1
UNION
select lob, underwriting_state_code, policy_num, policy_module_inception_date, policy_original_inception_date, month_end_date, sales_office_sk, sales_office_code
from ba
where rno = 1
UNION
select lob, underwriting_state_code, policy_num, policy_module_inception_date, policy_original_inception_date, month_end_date, sales_office_sk, sales_office_code
from ho
where rno = 1
UNION
select lob, underwriting_state_code, policy_num, policy_module_inception_date, policy_original_inception_date, month_end_date, sales_office_sk, sales_office_code
from ll
where rno = 1
UNION
select lob, underwriting_state_code, policy_num, policy_module_inception_date, policy_original_inception_date, month_end_date, sales_office_sk, sales_office_code
from cmp
where rno = 1
UNION
select lob, underwriting_state_code, policy_num, policy_module_inception_date, policy_original_inception_date, month_end_date, sales_office_sk, sales_office_code
from cmp_phx
where rno = 1
;

----
---- extract number of policies incepted per year to determine agency inception year
----
drop table if exists #agency_inception;
select
        trim(sales_office_code) as sales_office_code
        ,sales_office_sk
        ,count(distinct policy_num) as num_pol
        ,min(case 
                when extract(year from policy_original_inception_date) <= (select min_year from #vars) then null
                else cast(policy_original_inception_date as date)
                end) as policy_first_inception_date
into #agency_inception
from #agency
group by
        trim(sales_office_code)
        ,sales_office_sk
;

----
---- Extract agency appointment dates
----
drop table if exists #agency_appt;
with appt as (
        select
                trim(sales_office_code) as sales_office_code
                ,case
                        when lob_name in ('Business Auto','Commercial Multi-Peril') then 'CL'
                        when lob_name in ('Personal Auto','Personal Property')      then 'PL'
                        else '*ERROR*'
                        end as lob
                ,product_name
                ,company_code
                ,state_code
                ,zone_code
                ,branch_id
                ,branch_code
                ,branch_name
                ,underwriter_name
                ,sales_office_appointment_date
                ,nb_sales_office_terminated_date
                ,rb_sales_office_terminated_date
                ,latest_record_ind
                ,row_number() over
                        (partition by 
                                trim(sales_office_code)
                                ,case
                                        when lob_name in ('Business Auto','Commercial Multi-Peril') then 'CL'
                                        when lob_name in ('Personal Auto','Personal Property')      then 'PL'
                                        else '*ERROR*'
                                        end
                                ,product_name
                                ,company_code
                                ,state_code
                                ,zone_code
                                ,branch_id
--                                ,branch_code
--                                ,branch_name
--                                ,underwriter_name
--                                ,sales_office_appointment_date
--                                ,nb_sales_office_terminated_date
--                                ,rb_sales_office_terminated_date
                        order by 
                                latest_record_ind desc
                                ,commission_effective_date desc
                                ,commission_termination_date desc

                        ) rno
        from salesmkt_dm_v.sales_office_commission_rate_view
        where 
                latest_record_ind > 0
                and product_name not in ('Commercial Taxi','CEA')
)
select
        trim(sales_office_code) as sales_office_code
        -- Personal Lines (PL)
--        ,min(case when lob = 'PL' then sales_office_appointment_date end) as PL_sales_office_appointment_date        
        ,min(case when lob = 'PL' then case
                        when extract(year from date(sales_office_appointment_date)) > (select max_year from #vars) then null
                        when extract(year from date(sales_office_appointment_date)) < (select min_year from #vars) then null
                        else sales_office_appointment_date
                        end
                end) as PL_sales_office_appointment_date
        ,max(case when lob = 'PL' and (extract(year from date(nb_sales_office_terminated_date)) < (select max_year from #vars) or extract(year from date(rb_sales_office_terminated_date)) < (select max_year from #vars)) then 1 else 0 end) as PL_sales_office_terminated_derived_ind
        ,min(case when lob = 'PL' then nb_sales_office_terminated_date end) as PL_nb_sales_office_terminated_date
        ,min(case when lob = 'PL' then rb_sales_office_terminated_date end) as PL_rb_sales_office_terminated_date
        -- Commercial Lines (CL)
--        ,min(case when lob = 'CL' then sales_office_appointment_date end) as CL_sales_office_appointment_date
        ,min(case when lob = 'CL' then case
                        when extract(year from date(sales_office_appointment_date)) > (select max_year from #vars) then null
                        when extract(year from date(sales_office_appointment_date)) < (select min_year from #vars) then null
                        else sales_office_appointment_date
                        end
                end) as CL_sales_office_appointment_date
        ,max(case when lob = 'CL' and (extract(year from date(nb_sales_office_terminated_date)) < (select max_year from #vars) or extract(year from date(rb_sales_office_terminated_date)) < (select max_year from #vars)) then 1 else 0 end) as CL_sales_office_terminated_derived_ind
        ,min(case when lob = 'CL' then nb_sales_office_terminated_date end) as CL_nb_sales_office_terminated_date
        ,min(case when lob = 'CL' then rb_sales_office_terminated_date end) as CL_rb_sales_office_terminated_date
--        ,min(sales_office_appointment_date) as sales_office_appointment_date
--        ,min(nb_sales_office_terminated_date) as nb_sales_office_terminated_date
--        ,min(rb_sales_office_terminated_date) as rb_sales_office_terminated_date
--        ,max(case when (extract(year from date(nb_sales_office_terminated_date)) > 9000 or extract(year from date(rb_sales_office_terminated_date)) > 9000) then 1 else 0 end) as sales_office_terminated_derived_ind        
into #agency_appt
from appt
where rno = 1
group by trim(sales_office_code)
;

----
---- Extract business model and business source
----
drop table if exists #agency_model;
with biz as (
        select
                sales_office_code
                ,office_nm
                ,state_cd
                ,business_model
                ,business_source
                ,row_number() over
                        (partition by 
                                sales_office_code
--                                ,office_nm
--                                ,state_cd
--                                ,business_model
--                                ,business_source
                        order by 
                                last_update_ts desc
                        ) rno
        from edw_xref.sales_office_grouping_umt
)
select
        sales_office_code
        ,office_nm
        ,business_model
        ,business_source
        ,state_cd
        ,rno
into #agency_model
from biz
where rno = 1
;

----
---- Merge in Master Agency Code
----
drop table if exists #agency_mac;
select
        a.lob
        ,a.underwriting_state_code
        ,a.policy_num
        ,a.policy_module_inception_date
        ,a.policy_original_inception_date
        ,a.month_end_date
        ,a.sales_office_sk
        ,a.sales_office_code
        ,trim(coalesce(mac.master_agency_code, a.sales_office_code)) as master_agency_code        
        ,m.office_nm
        ,m.business_model
        ,m.business_source        
        ,p.PL_sales_office_appointment_date
        ,p.PL_sales_office_terminated_derived_ind
        ,p.PL_nb_sales_office_terminated_date
        ,p.PL_rb_sales_office_terminated_date        
        ,p.CL_sales_office_appointment_date
        ,p.CL_sales_office_terminated_derived_ind
        ,p.CL_nb_sales_office_terminated_date
        ,p.CL_rb_sales_office_terminated_date               
        ,i.num_pol
--        ,i.num_pol_incept_garbage
--        ,i.num_pol_incept_40_70yr
--        ,i.num_pol_incept_30_40yr
--        ,i.num_pol_incept_20_30yr
--        ,i.num_pol_incept_10_20yr
--        ,i.num_pol_incept_05_10yr
--        ,i.num_pol_incept_03_05yr
--        ,i.num_pol_incept_02_03yr
--        ,i.num_pol_incept_01_02yr
--        ,i.num_pol_incept_00_01yr
        ,cast(i.policy_first_inception_date as date) as policy_first_inception_date
into #agency_mac
from #agency a
left join #agency_inception i on (a.sales_office_code = i.sales_office_code and a.sales_office_sk = i.sales_office_sk)
left join #agency_model m     on (a.sales_office_code = m.sales_office_code)
left join #agency_appt p      on (a.sales_office_code = p.sales_office_code)
left join #mac mac on ( to_char(a.policy_module_inception_date,'YYYYMM') = mac.accounting_month and trim(a.sales_office_code) = trim(mac.child_producer))
;

----
---- Get assigned marketing reps by LOB
----
drop table if exists #mrep;
with mrep as (
        select
                trim(s.sales_office_code) as sales_office_code
                ,trim(s.sales_office_name) as sales_office_name
                ,case 
                        when substring(s.sales_office_name,1,1)= '*' then 'Terminated'
                        when substring(s.sales_office_code,1,3) = '048' then '8000 code' 
                        else 'Active' 
                        end as sales_office_status
                ,s.sales_office_key
                ,s.sales_office_code_hkey
                ,case
                        when m.lob_name in ('Commercial Auto','Commercial Property') then 'CL'
                        when m.lob_name in ('Personal Property','Private Passenger Auto') then 'PL'
                        else m.lob_name
                        end as lob                
--                ,m.lob_name
--                ,m.product_name             
                ,m.state_code                
                ,case 
                        when m.zone_code = '<NOT FOUND>' then null 
                        else lpad(m.zone_code,3,0)
                        end as zone_code
--                m.zone_code
                ,m.marketing_rep_name
                ,s.national_agency_ind
                ,s.ais_agency_ind
                ,s.independent_agent_ind
                ,s.terminated_ind
                ,case
                        when left(trim(s.sales_office_name),1) = '*' then 'Y'
                        else 'N'
                        end as terminated_starts_w_asterisk_ind
                ,row_number() over
                        (partition by 
                                trim(s.sales_office_code)
                                ,case
                                        when m.lob_name in ('Commercial Auto','Commercial Property') then 'CL'
                                        when m.lob_name in ('Personal Property','Private Passenger Auto') then 'PL'
                                        else m.lob_name
                                        end
                                ,m.state_code
                                ,case 
                                        when m.zone_code = '<NOT FOUND>' then null 
                                        else lpad(m.zone_code,3,0)
                                        end
                        order by 
                                s.last_update_ts desc
                                ,m.latest_record_ind desc
                                ,m.effective_end_date desc
                                ,m.source_last_updated_ts desc                                
                                ,m.last_update_ts desc
                        ) rno         
        from edw_dm.sales_office_dim s
        left join edw_dm.sales_office_marketing_rep_dim m on (s.sales_office_code_hkey = m.sales_office_code_hkey and s.sales_office_key = m.f_sales_office_key)
) 
select *
into #mrep
from mrep
where rno = 1
;

----
---- Create table of agency names and details
----
drop table if exists #agency_details;
select distinct
        sales_office_code
        ,sales_office_name
        ,sales_office_status
        ,sales_office_key
        ,national_agency_ind
        ,ais_agency_ind
        ,independent_agent_ind
        ,terminated_ind
        ,terminated_starts_w_asterisk_ind
into #agency_details
from #mrep
where terminated_ind in ('N','Y') -- otherwise, this is a messed up agency code (or a test code)
;



----
---- EXPOSURES & PREMIUMS
----

/* PPA */
DROP TABLE IF EXISTS #exp_pol;
SELECT 
        'PPA' as LOB
        ,t2.policy_num
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5) as mailing_zip_code
--        ,extract(year from t1.month_end_date) as exp_year
        ,sum(t1.earned_car_days) as EE
        ,SUM(t1.earned_premium_amt) AS EP

INTO #exp_pol

FROM edw_dm_ppa_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
inner join edw_dm_ppa_v.POLICY_TRX_V t2 on (t1.ppa_policy_trx_sk = t2.ppa_policy_trx_sk)
WHERE 
        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)
        and t2.underwriting_state_code in ('AZ','CA','FL','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY       
        t2.policy_num
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5)
--        ,extract(year from t1.month_end_date)

UNION ALL

/* BA */
SELECT 
        'BA' as LOB  
        ,t2.policy_num
        ,t2.underwriting_state_code
        ,t2.business_mailing_state_code as mailing_state_code
        ,left(t2.business_mailing_zip_code,5) as mailing_zip_code
--        ,extract(year from t1.month_end_date) as exp_year
        ,sum(t1.earned_car_days) as EE
        ,SUM(t1.earned_premium_amt) AS EP
FROM edw_dm_ba_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
inner join edw_dm_ba_v.POLICY_TRX_V t2 on (t1.ba_policy_trx_sk = t2.ba_policy_trx_sk)
WHERE 
        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
        and t2.underwriting_state_code in ('CA','FL','OK','TX')
GROUP BY       
        t2.policy_num
        ,t2.underwriting_state_code
        ,t2.business_mailing_state_code
        ,left(t2.business_mailing_zip_code,5)
--        ,extract(year from t1.month_end_date)
            
UNION ALL

/* HO */
SELECT 
        'HO' as LOB   
        ,t2.policy_num      
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5) as mailing_zip_code       
--        ,extract(year from t1.month_end_date) as exp_year
        ,sum(t1.earned_days) as EE
        ,SUM(t1.earned_premium_amt) AS EP
FROM edw_dm_ho_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
inner join edw_dm_ho_v.POLICY_TRX_V t2 on (t1.ho_policy_trx_sk = t2.ho_policy_trx_sk)
WHERE 
        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY        
        t2.policy_num      
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5)
--        ,extract(year from t1.month_end_date)

UNION ALL

/* LL */
SELECT 
        'LL' as LOB    
        ,t2.policy_num
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5) as mailing_zip_code
--        ,extract(year from t1.month_end_date) as exp_year
        ,sum(t1.earned_days) as EE
        ,SUM(t1.earned_premium_amt) AS EP
FROM edw_dm_LL_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
inner join edw_dm_LL_v.POLICY_TRX_V t2 on (t1.LL_policy_trx_sk = t2.LL_policy_trx_sk)
WHERE 
        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY       
        t2.policy_num       
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5)
--        ,extract(year from t1.month_end_date)

UNION ALL

/* CMP */
SELECT
        'CMP' as LOB    
        ,t2.policy_num
        ,t2.underwriting_state_code
        ,coalesce(t6.state_code, t6.source_state_code) as mailing_state_code
        ,coalesce(left(t6.zip_code,5), left(t6.source_zip_code,5)) as mailing_zip_code
--        ,extract(year from t1.month_end_date) as exp_year
        ,sum(t1.earned_days) as EE
        ,SUM(t1.earned_premium_amt) AS EP
FROM edw_dm_cmp_v.COVERAGE_EARNED_PREMIUM_FACT_V  t1
inner join edw_dm_cmp_v.POLICY_TRX_V t2 on (t1.cmp_policy_trx_sk = t2.cmp_policy_trx_sk)
left join edw_dm_cmp_v.address_v t6 on (t2.mailing_address_hkey = t6.address_hkey)
WHERE 
        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY        
        t2.policy_num
        ,t2.underwriting_state_code     
        ,coalesce(t6.state_code, t6.source_state_code)
        ,coalesce(left(t6.zip_code,5), left(t6.source_zip_code,5))
--        ,extract(year from t1.month_end_date)        

UNION ALL

/* CMP Phoenix */
SELECT 
        'CMP PHX' as lob,
        pt.policy_number as policy_num,
        pt.underwriting_state_code,
        pv.state_code as mailing_state_code,
        left(pv.zip_code,5) as mailing_zip_code
--        extract(year from date(t1.month_end_date)) as exp_year
--        ,0 as EE
        ,count(distinct pt.policy_number) as EE -- this is just a proxy        
        ,SUM(t1.earned_premium_amt) AS EP
FROM dwhub_cmp_df_v.policy_premium_monthly_fact_view t1
inner join dwhub_cmp_df_v.POLICY_TRANSACTION_VIEW pt on (t1.f_policy_transaction_key = pt.policy_transaction_key)
left join dwhub_cmp_df_v.policyholder_view pv on (pt.f_policyholder_key = pv.policyholder_key)
WHERE 
        t1.month_end_date BETWEEN (select start_date from #vars) and (select end_date from #vars)  
        and pt.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')        
GROUP BY 
        pt.policy_number,
        pt.underwriting_state_code,
        pv.state_code,
        left(pv.zip_code,5)
--        extract(year from date(t1.month_end_date))
;



----
---- CLAIMS & LOSSES (AT POLICY & LOSS DATE)
----

/* PPA */
DROP TABLE IF EXISTS #claim_pol;
SELECT 
        'PPA' as LOB 
        ,t2.policy_num
        ,date(t1.loss_date) as loss_date             
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5) as mailing_zip_code,
--        extract(year from date(t1.loss_date)) as loss_year,
        max(left(t3.catastrophe_ind,1)) as cat_ind,
        SUM(t1.net_incurred_loss_amt)   AS net_inc_loss,
        SUM(t1.net_incurred_loss_amt * (
            case
                when t2.term_type = '6 month term' then case
                        when t2.module_num <= 1 then (select LRR_Green_Auto_6mo_Mod1 from #vars)
                        when t2.module_num = 2  then (select LRR_Green_Auto_6mo_Mod2 from #vars)
                        when t2.module_num = 3  then (select LRR_Green_Auto_6mo_Mod3 from #vars)
                        when t2.module_num = 4  then (select LRR_Green_Auto_6mo_Mod4 from #vars)
                        when t2.module_num = 5  then (select LRR_Green_Auto_6mo_Mod5 from #vars)
                        when t2.module_num = 6  then (select LRR_Green_Auto_6mo_Mod6 from #vars)
                        when t2.module_num = 7  then (select LRR_Green_Auto_6mo_Mod7 from #vars)
                        when t2.module_num = 8  then (select LRR_Green_Auto_6mo_Mod8 from #vars)
                        when t2.module_num = 9  then (select LRR_Green_Auto_6mo_Mod9 from #vars)
                        when t2.module_num = 10 then (select LRR_Green_Auto_6mo_Mod10 from #vars)
                        else 1.0
                        end
                else case
                        when t2.module_num <= 1 then (select LRR_Green_Auto_1yr_Mod1 from #vars)
                        when t2.module_num = 2  then (select LRR_Green_Auto_1yr_Mod2 from #vars)
                        else 1.0
                        end
                end))                   AS net_inc_loss_green,
        SUM(t1.net_paid_loss_amt)       AS net_paid_loss, 
        SUM(t1.gross_incurred_loss_amt) AS gross_inc_loss,
        SUM(t1.gross_paid_loss_amt)     AS gross_paid_loss 
        
INTO #claim_pol
        
FROM edw_dm_PPA_v.CLAIM_POLICY_LOSS_FACT_V t1
inner join edw_dm_PPA_v.POLICY_TRX_V t2 on (t1.PPA_policy_trx_sk = t2.PPA_policy_trx_sk)
left join edw_dm_PPA_v.CLAIM_V t3 on (t1.claim_sk = t3.claim_sk)
WHERE 
        date(t1.loss_date) BETWEEN (select start_date from #vars) and (select end_date from #vars) and
        date(t1.claim_trx_process_date) BETWEEN (select start_date from #vars) and (select end_trans_date from #vars)
        and t2.underwriting_state_code in ('AZ','CA','FL','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY   
        t2.policy_num
        ,date(t1.loss_date)            
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5)
--        ,extract(year from date(t1.loss_date))

UNION ALL

/* BA */
SELECT 
        'BA' as LOB 
        ,t2.policy_num
        ,date(t1.loss_date) as loss_date        
        ,t2.underwriting_state_code
        ,t2.business_mailing_state_code as mailing_state_code
        ,left(t2.business_mailing_zip_code,5) as mailing_zip_code,
--        extract(year from date(t1.loss_date)) as loss_year,
        max(left(t3.catastrophe_ind,1)) as cat_ind,
        SUM(t1.net_incurred_loss_amt)   AS net_inc_loss,
        SUM(t1.net_incurred_loss_amt * (case
                when t2.module_num <= 1 then (select LRR_Green_Auto_1yr_Mod1 from #vars)
                when t2.module_num  = 2 then (select LRR_Green_Auto_1yr_Mod2 from #vars)
                else 1.0
                end))                   AS net_inc_loss_green,
        SUM(t1.net_paid_loss_amt)       AS net_paid_loss, 
        SUM(t1.gross_incurred_loss_amt) AS gross_inc_loss,
        SUM(t1.gross_paid_loss_amt)     AS gross_paid_loss
FROM edw_dm_BA_v.CLAIM_POLICY_LOSS_FACT_V t1
inner join edw_dm_BA_v.POLICY_TRX_V t2 on (t1.BA_policy_trx_sk = t2.BA_policy_trx_sk)
left join edw_dm_BA_v.CLAIM_V t3 on (t1.claim_sk = t3.claim_sk)
WHERE 
        date(t1.loss_date) BETWEEN (select start_date from #vars) and (select end_date from #vars) and
        date(t1.claim_trx_process_date) BETWEEN (select start_date from #vars) and (select end_trans_date from #vars)
        and t2.underwriting_state_code in ('CA','FL','OK','TX')        
GROUP BY       
        t2.policy_num
        ,date(t1.loss_date)    
        ,t2.underwriting_state_code
        ,t2.business_mailing_state_code
        ,left(t2.business_mailing_zip_code,5)
--        ,extract(year from date(t1.loss_date))
       
UNION ALL

/* HO */
SELECT 
        'HO' as LOB   
        ,t2.policy_num
        ,date(t1.loss_date) as loss_date               
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5) as mailing_zip_code,        
--        extract(year from date(t1.loss_date)) as loss_year,  
        max(left(t3.catastrophe_ind,1)) as cat_ind,
        SUM(t1.net_loss_incurred_amt)   AS net_inc_loss,
        SUM(t1.net_loss_incurred_amt * (case
                when t2.module_num <= 1 then (select LRR_Green_Prop_1yr_Mod1 from #vars)
                when t2.module_num  = 2 then (select LRR_Green_Prop_1yr_Mod2 from #vars)
                when t2.module_num  = 3 then (select LRR_Green_Prop_1yr_Mod3 from #vars)
                else 1.0
                end))                   AS net_inc_loss_green,    
        SUM(t1.net_loss_paid_amt)       AS net_paid_loss, 
        SUM(t1.gross_loss_incurred_amt) AS gross_inc_loss,
        SUM(t1.gross_loss_paid_amt)     AS gross_paid_loss
FROM edw_dm_HO_v.CLAIM_POLICY_LOSS_FACT_V t1
inner join edw_dm_HO_v.POLICY_TRX_V t2 on (t1.HO_policy_trx_sk = t2.HO_policy_trx_sk)
left join edw_dm_HO_v.CLAIM_V t3 on (t1.claim_sk = t3.claim_sk)
WHERE 
        date(t1.loss_date) BETWEEN (select start_date from #vars) and (select end_date from #vars) and
        date(t1.claim_trx_process_date) BETWEEN (select start_date from #vars) and (select end_trans_date from #vars)
        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY          
        t2.policy_num
        ,date(t1.loss_date)             
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5)   
--        ,extract(year from date(t1.loss_date))

UNION ALL

/* LL */
SELECT 
        'LL' as LOB   
        ,t2.policy_num
        ,date(t1.loss_date) as loss_date               
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5) as mailing_zip_code,        
--        extract(year from date(t1.loss_date)) as loss_year,
        max(left(t3.catastrophe_ind,1)) as cat_ind,
        SUM(t1.net_loss_incurred_amt)   AS net_inc_loss, 
        SUM(t1.net_loss_incurred_amt * (case
                when t2.module_num <= 1 then (select LRR_Green_Prop_1yr_Mod1 from #vars)
                when t2.module_num  = 2 then (select LRR_Green_Prop_1yr_Mod2 from #vars)
                when t2.module_num  = 3 then (select LRR_Green_Prop_1yr_Mod3 from #vars)
                else 1.0
                end))                   AS net_inc_loss_green,
        SUM(t1.net_loss_paid_amt)       AS net_paid_loss, 
        SUM(t1.gross_loss_incurred_amt) AS gross_inc_loss,
        SUM(t1.gross_loss_paid_amt)     AS gross_paid_loss
FROM edw_dm_ll_v.CLAIM_POLICY_LOSS_FACT_V t1
inner join edw_dm_ll_v.POLICY_TRX_V t2 on (t1.ll_policy_trx_sk = t2.ll_policy_trx_sk)
left join edw_dm_ll_v.CLAIM_V t3 on (t1.claim_sk = t3.claim_sk)
WHERE 
        date(t1.loss_date) BETWEEN (select start_date from #vars) and (select end_date from #vars) and
        date(t1.claim_trx_process_date) BETWEEN (select start_date from #vars) and (select end_trans_date from #vars)
        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY            
        t2.policy_num
        ,date(t1.loss_date)           
        ,t2.underwriting_state_code
        ,t2.mailing_state_code
        ,left(t2.mailing_zip_code,5)
--        ,extract(year from date(t1.loss_date))
        
UNION ALL

/* CMP */
SELECT 
        'CMP' as LOB   
        ,t2.policy_num
        ,date(t1.loss_date) as loss_date
        ,t2.underwriting_state_code
        ,coalesce(t6.state_code, t6.source_state_code) as mailing_state_code
        ,coalesce(left(t6.zip_code,5), left(t6.source_zip_code,5)) as mailing_zip_code,
--        extract(year from date(t1.loss_date)) as loss_year,
        max(left(t3.catastrophe_ind,1)) as cat_ind,
        SUM(t1.net_loss_incurred_amt)   AS net_inc_loss,
        SUM(t1.net_loss_incurred_amt * (case
                when t2.module_num <= 1 then (select LRR_Green_Prop_1yr_Mod1 from #vars)
                when t2.module_num  = 2 then (select LRR_Green_Prop_1yr_Mod2 from #vars)
                when t2.module_num  = 3 then (select LRR_Green_Prop_1yr_Mod3 from #vars)
                else 1.0
                end))                   AS net_inc_loss_green,        
        SUM(t1.net_loss_paid_amt)       AS net_paid_loss, 
        SUM(t1.gross_loss_incurred_amt) AS gross_inc_loss,
        SUM(t1.gross_loss_paid_amt)     AS gross_paid_loss
FROM edw_dm_CMP_v.CLAIM_POLICY_LOSS_FACT_V t1
inner join edw_dm_CMP_v.POLICY_TRX_V t2 on (t1.CMP_policy_trx_sk = t2.CMP_policy_trx_sk)
inner join edw_dm_CMP_v.CLAIM_V t3 on (t1.claim_sk = t3.claim_sk)
left join edw_dm_cmp_v.address_v t6 on (t2.mailing_address_hkey = t6.address_hkey)
WHERE 
        date(t1.loss_date) BETWEEN (select start_date from #vars) and (select end_date from #vars) and
        date(t1.claim_trx_process_date) BETWEEN (select start_date from #vars) and (select end_trans_date from #vars)
        and t2.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')
GROUP BY           
        t2.policy_num
        ,date(t1.loss_date)
        ,t2.underwriting_state_code
        ,coalesce(t6.state_code, t6.source_state_code)
        ,coalesce(left(t6.zip_code,5), left(t6.source_zip_code,5))  
--        ,extract(year from date(t1.loss_date))

UNION ALL

/* CMP Phoenix */
SELECT 
        'CMP PHX' as lob    
        ,pt.policy_number as policy_num
        ,date(t1.loss_date) as loss_date,
        pt.underwriting_state_code,
        pv.state_code as mailing_state_code,
        left(pv.zip_code,5) as mailing_zip_code,
--        extract(year from date(t1.loss_date)) as loss_year,
        'N' as cat_ind,
        SUM(t1.loss_incurred_amt)                                   AS net_inc_loss,
        SUM(t1.loss_incurred_amt * (case
                when pt.module_num <= 1 then (select LRR_Green_Prop_1yr_Mod1 from #vars)
                when pt.module_num  = 2 then (select LRR_Green_Prop_1yr_Mod2 from #vars)
                when pt.module_num  = 3 then (select LRR_Green_Prop_1yr_Mod3 from #vars)
                else 1.0
                end))                                               AS net_inc_loss_green,          
        SUM(t1.loss_paid_amt)                                       AS net_paid_loss, 
        sum(t1.loss_incurred_amt-t1.salvage_amt-t1.subrogation_amt) AS gross_inc_loss, 
        sum(t1.loss_paid_amt-t1.salvage_amt-t1.subrogation_amt)     AS gross_paid_loss    
FROM dwhub_cmp_df_v.CLAIM_MONTHLY_DIRECT_FACT_VIEW t1
inner join dwhub_cmp_df_v.POLICY_TRANSACTION_VIEW pt on (t1.f_policy_transaction_key = pt.policy_transaction_key)
left join dwhub_cmp_df_v.policyholder_view pv on (pt.f_policyholder_key = pv.policyholder_key)
WHERE 
        t1.loss_date BETWEEN (select start_date from #vars) and (select end_date from #vars)
        AND t1.month_end_date <= (select end_trans_date from #vars)
        and pt.underwriting_state_code in ('AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA')        
GROUP BY      
        pt.policy_number
        ,date(t1.loss_date),
        pt.underwriting_state_code,
        pv.state_code,
        left(pv.zip_code,5)
--        extract(year from date(t1.loss_date))
;

----
---- Calculate Claim Counts and Apply Loss Cap
----
DROP TABLE IF EXISTS #claim_pol2;
select
        LOB 
        ,policy_num
        ,loss_date
        ,underwriting_state_code
        ,mailing_state_code
        ,mailing_zip_code
--        ,sum(case when net_inc_loss <= 10 and net_paid_loss <= 10 then 1 else 0 end) as claim_cnt_closed_wo_pay
--        ,sum(case when (net_inc_loss > 10 or net_paid_loss > 10) and left(cat_ind,1) = 'Y' then 1 else 0 end) as claim_cnt_CAT
--        ,sum(case when (net_inc_loss > 10 or net_paid_loss > 10) and left(cat_ind,1) not in ('Y') then 1 else 0 end) as claim_cnt_xCAT
        ,sum(case when net_inc_loss <= (select min_loss from #vars) then 1 else 0 end) as claim_cnt_closed_wo_pay
        ,sum(case when net_inc_loss > (select min_loss from #vars) and left(cat_ind,1) = 'Y' then 1 else 0 end) as claim_cnt_CAT
        ,sum(case when net_inc_loss > 10 and left(cat_ind,1) not in ('Y') then 1 else 0 end) as claim_cnt_xCAT      
        ,sum(case when net_inc_loss < 0 then 0 else net_inc_loss end) as net_inc_loss
        ,sum(case when net_inc_loss_green < 0 then 0 else net_inc_loss_green end) as net_inc_loss_green
        ,sum(case
                when net_inc_loss_green < 0 then 0
                when LOB in ('PPA') then case
                        when net_inc_loss_green > (select Loss_Cap_PPA from #vars) then (select Loss_Cap_PPA from #vars)
                        else net_inc_loss_green
                        end
                when LOB in ('BA') then case
                        when net_inc_loss_green > (select Loss_Cap_BA from #vars) then (select Loss_Cap_BA from #vars)
                        else net_inc_loss_green
                        end
                when LOB in ('HO') then case
                        when net_inc_loss_green > (select Loss_Cap_HO from #vars) then (select Loss_Cap_HO from #vars)
                        else net_inc_loss_green
                        end                
                when LOB in ('LL') then case
                        when net_inc_loss_green > (select Loss_Cap_LL from #vars) then (select Loss_Cap_LL from #vars)
                        else net_inc_loss_green
                        end        
                when LOB in ('CMP','CMP PHX') then case
                        when net_inc_loss_green > (select Loss_Cap_CMP from #vars) then (select Loss_Cap_CMP from #vars)
                        else net_inc_loss_green
                        end
                else net_inc_loss_green
                end)                    as net_inc_loss_green_cap
        ,sum(net_paid_loss)             as net_paid_loss
        ,sum(gross_inc_loss)            as gross_inc_loss
        ,sum(gross_paid_loss)           as gross_paid_loss
into #claim_pol2
from #claim_pol
group by
        LOB 
        ,policy_num
        ,loss_date             
        ,underwriting_state_code
        ,mailing_state_code
        ,mailing_zip_code
;

----
---- Combine multiple claims on 1 policy
----
DROP TABLE IF EXISTS #claim;
select
        LOB 
        ,policy_num         
        ,underwriting_state_code
        ,mailing_state_code
        ,mailing_zip_code
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(coalesce(claim_cnt_closed_wo_pay,0) + coalesce(claim_cnt_xcat,0)) as claim_counts_w_nopay_xcat
        ,sum(coalesce(claim_cnt_cat,0)           + coalesce(claim_cnt_xcat,0)) as claim_counts_w_cat_wo_npay         
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
        ,sum(gross_inc_loss) as gross_inc_loss
        ,sum(gross_paid_loss) as gross_paid_loss
into #claim
from #claim_pol2
group by
        LOB 
        ,policy_num         
        ,underwriting_state_code
        ,mailing_state_code
        ,mailing_zip_code
;



----
---- Merge Exposure/Premium and Claims/Loss Tables
----
/* Combined Outer */
DROP TABLE IF EXISTS #combined_outer;
select 
        coalesce(e.LOB, c.LOB) as LOB
        ,coalesce(e.underwriting_state_code, c.underwriting_state_code) as underwriting_state_code
        ,coalesce(e.mailing_state_code, c.mailing_state_code) as mailing_state_code
        ,coalesce(e.mailing_zip_code, c.mailing_zip_code) as mailing_zip_code
        ,coalesce(e.policy_num, c.policy_num) as policy_num
        ,e.EE
        ,e.EP
        ,c.claim_cnt_closed_wo_pay
        ,c.claim_cnt_CAT
        ,c.claim_cnt_xCAT
        ,c.claim_counts_w_nopay_xcat
        ,c.claim_counts_w_cat_wo_npay               
        ,c.net_inc_loss
        ,c.net_inc_loss_green
        ,c.net_inc_loss_green_cap
        ,c.net_paid_loss
        ,c.gross_inc_loss
        ,c.gross_paid_loss 
into #combined_outer   
from #exp_pol e
full outer join #claim c on (
        e.lob=c.lob 
        and e.underwriting_state_code = c.underwriting_state_code 
        and e.mailing_state_code = c.mailing_state_code
        and e.mailing_zip_code = c.mailing_zip_code
        and e.policy_num = c.policy_num
--        and e.exp_year = c.loss_year
        )
;
/* Combined Inner */
DROP TABLE IF EXISTS #combined_inner;
select 
        coalesce(e.LOB, c.LOB) as LOB
        ,coalesce(e.underwriting_state_code, c.underwriting_state_code) as underwriting_state_code
        ,coalesce(e.mailing_state_code, c.mailing_state_code) as mailing_state_code
        ,coalesce(e.mailing_zip_code, c.mailing_zip_code) as mailing_zip_code
        ,coalesce(e.policy_num, c.policy_num) as policy_num
        ,e.EE
        ,e.EP
        ,c.claim_cnt_closed_wo_pay
        ,c.claim_cnt_CAT
        ,c.claim_cnt_xCAT
        ,c.claim_counts_w_nopay_xcat
        ,c.claim_counts_w_cat_wo_npay    
        ,c.net_inc_loss
        ,c.net_inc_loss_green
        ,c.net_inc_loss_green_cap
        ,c.net_paid_loss
        ,c.gross_inc_loss
        ,c.gross_paid_loss 
into #combined_inner
from #exp_pol e
inner join #claim c on (
        e.lob=c.lob 
        and e.underwriting_state_code = c.underwriting_state_code 
        and e.mailing_state_code = c.mailing_state_code
        and e.mailing_zip_code = c.mailing_zip_code
        and e.policy_num = c.policy_num
--        and e.exp_year = c.loss_year
        )
;
/* Combined Left */
DROP TABLE IF EXISTS #combined_left;
select 
        coalesce(e.LOB, c.LOB) as LOB
        ,coalesce(e.underwriting_state_code, c.underwriting_state_code) as underwriting_state_code
        ,coalesce(e.mailing_state_code, c.mailing_state_code) as mailing_state_code
        ,coalesce(e.mailing_zip_code, c.mailing_zip_code) as mailing_zip_code
        ,coalesce(e.policy_num, c.policy_num) as policy_num
        ,e.EE
        ,e.EP
        ,c.claim_cnt_closed_wo_pay
        ,c.claim_cnt_CAT
        ,c.claim_cnt_xCAT
        ,c.claim_counts_w_nopay_xcat
        ,c.claim_counts_w_cat_wo_npay       
        ,c.net_inc_loss
        ,c.net_inc_loss_green
        ,c.net_inc_loss_green_cap
        ,c.net_paid_loss
        ,c.gross_inc_loss
        ,c.gross_paid_loss 
into #combined_left   
from #exp_pol e
left join #claim c on (
        e.lob=c.lob 
        and e.underwriting_state_code = c.underwriting_state_code 
        and e.mailing_state_code = c.mailing_state_code
        and e.mailing_zip_code = c.mailing_zip_code
        and e.policy_num = c.policy_num
--        and e.exp_year = c.loss_year
        )
;

----
---- Filter out bad rows to created "selected combined" table
----
DROP TABLE IF EXISTS #combined;
select 
        LOB
        ,underwriting_state_code
--        ,mailing_state_code
        ,mailing_zip_code
        ,policy_num
        ,EE
        ,EP
        ,claim_cnt_closed_wo_pay
        ,claim_cnt_CAT
        ,claim_cnt_xCAT
        ,claim_counts_w_nopay_xcat
        ,claim_counts_w_cat_wo_npay       
        ,net_inc_loss
        ,net_inc_loss_green
        ,net_inc_loss_green_cap
        ,net_paid_loss
        ,gross_inc_loss
        ,gross_paid_loss 
into #combined  
from #combined_left e
where
     EE >= 0
     and EP >= 0
     and underwriting_state_code = mailing_state_code
;


----
---- Create final table by merging in other relevant field
----
DROP TABLE IF EXISTS #combined_final;
select 
        case
                when c.LOB = 'CMP PHX' then 'CMP'
                else c.LOB
                end as LOB
        ,trim(a.master_agency_code) as master_agency_code       
        ,trim(a.sales_office_code) as sales_office_code
        ,a.office_nm
        ,a.business_model
        ,a.business_source        
        ,a.PL_sales_office_appointment_date
        ,a.PL_sales_office_terminated_derived_ind
        ,a.PL_nb_sales_office_terminated_date
        ,a.PL_rb_sales_office_terminated_date        
        ,a.CL_sales_office_appointment_date
        ,a.CL_sales_office_terminated_derived_ind
        ,a.CL_nb_sales_office_terminated_date
        ,a.CL_rb_sales_office_terminated_date               
        ,a.policy_first_inception_date
        ,a.num_pol
--        ,a.num_pol_incept_garbage
--        ,a.num_pol_incept_40_70yr
--        ,a.num_pol_incept_30_40yr
--        ,a.num_pol_incept_20_30yr
--        ,a.num_pol_incept_10_20yr
--        ,a.num_pol_incept_05_10yr
--        ,a.num_pol_incept_03_05yr
--        ,a.num_pol_incept_02_03yr
--        ,a.num_pol_incept_01_02yr
--        ,a.num_pol_incept_00_01yr
        ,d.sales_office_name
        ,d.sales_office_status
        ,d.sales_office_key
        ,d.national_agency_ind
        ,d.ais_agency_ind
        ,d.independent_agent_ind
        ,d.terminated_ind
        ,d.terminated_starts_w_asterisk_ind       
        ,c.policy_num
        ,c.underwriting_state_code
        ,c.mailing_zip_code
        ,z.region_xs
        ,z.region_s
        ,z.region_m
        ,z.region_l
        ,z.region_xl
        ,c.EE
        ,c.EP
        ,c.claim_cnt_closed_wo_pay
        ,c.claim_cnt_CAT
        ,c.claim_cnt_xCAT
        ,c.claim_counts_w_nopay_xcat
        ,c.claim_counts_w_cat_wo_npay       
        ,c.net_inc_loss
        ,c.net_inc_loss_green
        ,c.net_inc_loss_green_cap
        ,c.net_paid_loss
        ,c.gross_inc_loss
        ,c.gross_paid_loss 
into #combined_final  
from #combined c
left join #zip2region z on (c.underwriting_state_code = z.state and c.mailing_zip_code = z.zip)
left join #agency_mac a on (c.lob = a.lob and c.underwriting_state_code = a.underwriting_state_code and c.policy_num = a.policy_num)
left join #agency_details d on (trim(a.sales_office_code) = trim(d.sales_office_code))
;


----
---- CREATE FINAL TABLES
----
/* Agent-level */
DROP TABLE IF EXISTS #combined_LOB_region_agent;
select 
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
        ,master_agency_code
        ,sales_office_code
        ,sales_office_name
        ,office_nm        
        ,sales_office_key
        ,business_model
        ,business_source
        ,national_agency_ind
        ,ais_agency_ind
        ,independent_agent_ind
        ,PL_sales_office_appointment_date        
        ,CL_sales_office_appointment_date
        ,policy_first_inception_date
        ,case
                when PL_sales_office_appointment_date is null and CL_sales_office_appointment_date is null then policy_first_inception_date
                when coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date) <= coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date) then coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date)
                else coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date)
                end as sales_office_appointment_date
        ,num_pol
--        ,num_pol_incept_garbage
--        ,num_pol_incept_40_70yr
--        ,num_pol_incept_30_40yr
--        ,num_pol_incept_20_30yr
--        ,num_pol_incept_10_20yr
--        ,num_pol_incept_05_10yr
--        ,num_pol_incept_03_05yr
--        ,num_pol_incept_02_03yr
--        ,num_pol_incept_01_02yr
--        ,num_pol_incept_00_01yr
        ,sales_office_status        
        ,terminated_ind
        ,terminated_starts_w_asterisk_ind        
        ,PL_sales_office_terminated_derived_ind
        ,PL_nb_sales_office_terminated_date
        ,PL_rb_sales_office_terminated_date
        ,CL_sales_office_terminated_derived_ind
        ,CL_nb_sales_office_terminated_date
        ,CL_rb_sales_office_terminated_date
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_counts_w_nopay_xcat)  as claims_w_nopay
        ,sum(claim_counts_w_cat_wo_npay) as claims_w_cat
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
into #combined_LOB_region_agent
from #combined_final
where sales_office_code not in -- test agency codes
        (
                '0009999'
                ,'109999'
                ,'NV999'
                ,'OK999'
                ,'IL999'
                ,'AZ999'
                ,'NY999'
                ,'VA999'
                ,'NJ999'
                ,'TX999'
                ,'GA999'
                ,'FL999'
                ,'311111'
                ,'A000'
                ,'NV000'
        )
group by
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
        ,master_agency_code
        ,sales_office_code
        ,sales_office_name
        ,office_nm        
        ,sales_office_key
        ,business_model
        ,business_source
        ,national_agency_ind
        ,ais_agency_ind
        ,independent_agent_ind
        ,PL_sales_office_appointment_date        
        ,CL_sales_office_appointment_date
        ,policy_first_inception_date
        ,case
                when PL_sales_office_appointment_date is null and CL_sales_office_appointment_date is null then policy_first_inception_date
                when coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date) <= coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date) then coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date)
                else coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date)
                end        
        ,num_pol
--        ,num_pol_incept_garbage
--        ,num_pol_incept_40_70yr
--        ,num_pol_incept_30_40yr
--        ,num_pol_incept_20_30yr
--        ,num_pol_incept_10_20yr
--        ,num_pol_incept_05_10yr
--        ,num_pol_incept_03_05yr
--        ,num_pol_incept_02_03yr
--        ,num_pol_incept_01_02yr
--        ,num_pol_incept_00_01yr
        ,sales_office_status        
        ,terminated_ind
        ,terminated_starts_w_asterisk_ind        
        ,PL_sales_office_terminated_derived_ind
        ,PL_nb_sales_office_terminated_date
        ,PL_rb_sales_office_terminated_date
        ,CL_sales_office_terminated_derived_ind
        ,CL_nb_sales_office_terminated_date
        ,CL_rb_sales_office_terminated_date
;
/* Complement w/o agents */
DROP TABLE IF EXISTS #combined_LOB_region;
select 
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_counts_w_nopay_xcat)  as claims_w_nopay
        ,sum(claim_counts_w_cat_wo_npay) as claims_w_cat
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
into #combined_LOB_region
from #combined_final
where sales_office_code not in -- test agency codes
        (
                '0009999'
                ,'109999'
                ,'NV999'
                ,'OK999'
                ,'IL999'
                ,'AZ999'
                ,'NY999'
                ,'VA999'
                ,'NJ999'
                ,'TX999'
                ,'GA999'
                ,'FL999'
                ,'311111'
                ,'A000'
                ,'NV000'
        )
group by
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
;




--select *
--from #agency_details
--where sales_office_code = '04C492'
--;
--select *
--from #combined_LOB_region_agent
--where sales_office_code = '04C492'
--;
----
---- Print out samples of every table
----
--select top 100 * from #vars;
--select top 100 * from #zip2region;
--select top 100 * from #mac;
--select top 100 * from #agency;
--select top 100 * from #agency_inception
--select top 100 * from #agency_mac;
--select top 100 * from #mrep;
--select top 100 * from #exp_pol;
--select top 100 * from #claim_pol;
--select top 100 * from #claim_pol2;
--select top 100 * from #claim;
--select top 100 * from #combined_left;
--select top 100 * from #combined_inner;
--select top 100 * from #combined_outer;
--select top 100 * from #combined;
--select top 100 * from #combined_final;


----
---- Print out final tables for analysis
----

--> LOB, Region, & Agent-Level Table
select
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
        ,trim(concat('M_', master_agency_code)) as master_agency_code_w_prefix
        ,trim(concat('S_', sales_office_code))  as sales_office_code_w_prefix
        ,sales_office_name
        ,office_nm
        ,business_model
        ,business_source
        ,national_agency_ind
--        ,ais_agency_ind
        ,independent_agent_ind
        ,case
                when PL_sales_office_appointment_date is null and CL_sales_office_appointment_date is null then policy_first_inception_date
                when coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date) <= coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date) then coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date)
                else coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date)
                end as sales_office_appointment_date
        ,PL_sales_office_appointment_date        
        ,CL_sales_office_appointment_date
--        ,policy_first_inception_date
--        ,num_pol_incept_garbage
--        ,num_pol_incept_40_70yr
--        ,num_pol_incept_30_40yr
--        ,num_pol_incept_20_30yr
--        ,num_pol_incept_10_20yr
--        ,num_pol_incept_05_10yr
--        ,num_pol_incept_03_05yr
--        ,num_pol_incept_02_03yr
--        ,num_pol_incept_01_02yr
--        ,num_pol_incept_00_01yr
        ,sales_office_status        
        ,terminated_starts_w_asterisk_ind     
        ,terminated_ind
        ,PL_sales_office_terminated_derived_ind
--        ,PL_nb_sales_office_terminated_date
--        ,PL_rb_sales_office_terminated_date
        ,CL_sales_office_terminated_derived_ind
--        ,CL_nb_sales_office_terminated_date
--        ,CL_rb_sales_office_terminated_date    
        ,num_pol
        ,EE
        ,EP
        ,claims_w_nopay
        ,claims_w_cat
        ,net_inc_loss
        ,net_inc_loss_green
        ,net_inc_loss_green_cap
from #combined_LOB_region_agent 
where 
        region_xl is not null 
        and terminated_ind is not null
order by 
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
        ,master_agency_code_w_prefix
        ,sales_office_code_w_prefix
        ,sales_office_status
;

--> LOB and Region-level table
select * from #combined_LOB_region
where region_xl is not null
order by 
        LOB
        ,region_xl        
        ,region_l
        ,region_m
        ,region_s
        ,region_xs
;

--> Various information on each Agency code (child)
select
        trim(concat('S_', sales_office_code))  as sales_office_code_w_prefix
--        ,trim(concat('M_', master_agency_code)) as master_agency_code_w_prefix
--        ,lob
--        ,underwriting_state_code        
        ,office_nm
        ,business_model
        ,business_source
        ,case
                when PL_sales_office_appointment_date is null and CL_sales_office_appointment_date is null then policy_first_inception_date
                when coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date) <= coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date) then coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date)
                else coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date)
                end as sales_office_appointment_date            
        ,PL_sales_office_appointment_date
        ,PL_sales_office_terminated_derived_ind
--        ,PL_nb_sales_office_terminated_date
--        ,PL_rb_sales_office_terminated_date        
        ,CL_sales_office_appointment_date
        ,CL_sales_office_terminated_derived_ind
--        ,CL_nb_sales_office_terminated_date
--        ,CL_rb_sales_office_terminated_date
--        ,policy_first_inception_date
--        ,num_pol
--        ,num_pol_incept_garbage
--        ,num_pol_incept_40_70yr
--        ,num_pol_incept_30_40yr
--        ,num_pol_incept_20_30yr
--        ,num_pol_incept_10_20yr
--        ,num_pol_incept_05_10yr
--        ,num_pol_incept_03_05yr
--        ,num_pol_incept_02_03yr
--        ,num_pol_incept_01_02yr
--        ,num_pol_incept_00_01yr
        ,count(distinct policy_num) as num_pol
from #agency_mac
where sales_office_code not in -- test agency codes
        (
                '0009999'
                ,'109999'
                ,'NV999'
                ,'OK999'
                ,'IL999'
                ,'AZ999'
                ,'NY999'
                ,'VA999'
                ,'NJ999'
                ,'TX999'
                ,'GA999'
                ,'FL999'
                ,'311111'
                ,'A000'
                ,'NV000'
        )
group by
        trim(concat('S_', sales_office_code))
--        ,trim(concat('M_', master_agency_code)) as master_agency_code_w_prefix
--        ,lob
--        ,underwriting_state_code        
        ,office_nm
        ,business_model
        ,business_source
        ,case
                when PL_sales_office_appointment_date is null and CL_sales_office_appointment_date is null then policy_first_inception_date
                when coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date) <= coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date) then coalesce(PL_sales_office_appointment_date,CL_sales_office_appointment_date)
                else coalesce(CL_sales_office_appointment_date,PL_sales_office_appointment_date)
                end           
        ,PL_sales_office_appointment_date
        ,PL_sales_office_terminated_derived_ind
--        ,PL_nb_sales_office_terminated_date
--        ,PL_rb_sales_office_terminated_date        
        ,CL_sales_office_appointment_date
        ,CL_sales_office_terminated_derived_ind
--        ,CL_nb_sales_office_terminated_date
--        ,CL_rb_sales_office_terminated_date
--        ,policy_first_inception_date
--        ,num_pol
--        ,num_pol_incept_garbage
--        ,num_pol_incept_40_70yr
--        ,num_pol_incept_30_40yr
--        ,num_pol_incept_20_30yr
--        ,num_pol_incept_10_20yr
--        ,num_pol_incept_05_10yr
--        ,num_pol_incept_03_05yr
--        ,num_pol_incept_02_03yr
--        ,num_pol_incept_01_02yr
order by
        sales_office_code_w_prefix
--        ,trim(concat('M_', master_agency_code)) as master_agency_code_w_prefix
--        ,lob
--        ,underwriting_state_code        
        ,office_nm
        ,business_model
        ,business_source
;



----
---- SANITY CHECKS
----
select 'all 3 tables' as table, count(*) as num_rec
from #agency agent
inner join #agency_model m on (agent.sales_office_code = m.sales_office_code)
inner join #agency_appt  a on (agent.sales_office_code = a.sales_office_code)
UNION
select 'agency appt', count(*) as num_rec
from #agency agent
inner join #agency_appt  a on (agent.sales_office_code = a.sales_office_code)
UNION
select 'agency model', count(*) as num_rec
from #agency agent
inner join #agency_model m on (agent.sales_office_code = m.sales_office_code)
UNION
select 'model appt', count(*) as num_rec
from #agency_model m
inner join #agency_appt a on (m.sales_office_code = a.sales_office_code)
UNION
select '#agency' as table, count(*) as num_rec
from #agency
UNION
select '#agency_model' as table, count(*) as num_rec
from #agency_model
UNION
select '#agency_appt' as table, count(*) as num_rec
from #agency_appt
UNION
select '#agency_inception' as table, count(*) as num_rec
from #agency_inception
UNION
select '#agency_inception model' as table, count(*) as num_rec
from #agency_inception i
left join #agency_model m on (i.sales_office_code = m.sales_office_code)
UNION
select '#agency_inception appt' as table, count(*) as num_rec
from #agency_inception i
left join #agency_appt a on (i.sales_office_code = a.sales_office_code)
UNION
select '#mrep' as table, count(*) as num_rec
from #mrep
UNION
select '#agency_details' as table, count(*) as num_rec
from #agency_details
UNION
select '#agency_mac' as table, count(*) as num_rec
from #agency_mac
;




/*
select
        '#exp_pol' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,null as claim_cnt_closed_wo_pay
        ,null as claim_cnt_CAT
        ,null as claim_cnt_xCAT
        ,null as claim_counts_w_nopay_xcat
        ,null as claim_counts_w_cat_wo_npay
        ,null as net_inc_loss
        ,null as net_inc_loss_green
        ,null as net_inc_loss_green_cap        
        ,null as net_paid_loss
from #exp_pol
group by lob, underwriting_state_code

UNION

select
        '#claim_pol' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,null as EE
        ,null as EP
        ,null as claim_cnt_closed_wo_pay
        ,null as claim_cnt_CAT
        ,null as claim_cnt_xCAT
        ,null as claim_counts_w_nopay_xcat
        ,null as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,null as net_inc_loss_green
        ,null as net_inc_loss_green_cap        
        ,sum(net_paid_loss) as net_paid_loss
from #claim_pol
group by lob, underwriting_state_code

UNION

select
        '#claim_pol2' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,null as EE
        ,null as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,null as claim_counts_w_nopay_xcat
        ,null as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #claim_pol2
group by lob, underwriting_state_code

UNION

select
        '#claim' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,null as EE
        ,null as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(claim_counts_w_nopay_xcat) as claim_counts_w_nopay_xcat
        ,sum(claim_counts_w_cat_wo_npay) as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #claim
group by lob, underwriting_state_code

UNION

select
        '#combined_left' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(claim_counts_w_nopay_xcat) as claim_counts_w_nopay_xcat
        ,sum(claim_counts_w_cat_wo_npay) as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #combined_left
group by lob, underwriting_state_code

UNION

select
        '#combined_inner' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(claim_counts_w_nopay_xcat) as claim_counts_w_nopay_xcat
        ,sum(claim_counts_w_cat_wo_npay) as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #combined_inner
group by lob, underwriting_state_code

UNION

select
        '#combined_outer' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(claim_counts_w_nopay_xcat) as claim_counts_w_nopay_xcat
        ,sum(claim_counts_w_cat_wo_npay) as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #combined_outer
group by lob, underwriting_state_code

UNION

select
        '#combined' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(claim_counts_w_nopay_xcat) as claim_counts_w_nopay_xcat
        ,sum(claim_counts_w_cat_wo_npay) as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #combined
group by lob, underwriting_state_code

UNION

select
        '#combined_final' as tablename
        ,lob
        ,underwriting_state_code
        ,count(*) as num_rec        
        ,sum(EE) as EE
        ,sum(EP) as EP
        ,sum(claim_cnt_closed_wo_pay) as claim_cnt_closed_wo_pay
        ,sum(claim_cnt_CAT) as claim_cnt_CAT
        ,sum(claim_cnt_xCAT) as claim_cnt_xCAT
        ,sum(claim_counts_w_nopay_xcat) as claim_counts_w_nopay_xcat
        ,sum(claim_counts_w_cat_wo_npay) as claim_counts_w_cat_wo_npay
        ,sum(net_inc_loss) as net_inc_loss
        ,sum(net_inc_loss_green) as net_inc_loss_green
        ,sum(net_inc_loss_green_cap) as net_inc_loss_green_cap
        ,sum(net_paid_loss) as net_paid_loss
from #combined_final
group by lob, underwriting_state_code


ORDER BY
        tablename
        ,lob
        ,underwriting_state_code
;


*/