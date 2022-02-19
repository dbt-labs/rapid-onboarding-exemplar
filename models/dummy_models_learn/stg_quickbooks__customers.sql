with
invoices as (select * from {{ ref('base_quickbooks_inc__customers') }}),
customers as (select * from {{ ref('base_quickbooks__customers') }})
select * from invoices