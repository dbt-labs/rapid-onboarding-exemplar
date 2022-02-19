with

cards as (select * from {{ ref('stg_trello__cards') }}),
boards as (select * from {{ ref('stg_trello__boards') }}),
card_members as (select * from {{ ref('stg_trello__card_members') }}),
card_labels as (select * from {{ ref('stg_trello__card_labels') }}),

select * from cards