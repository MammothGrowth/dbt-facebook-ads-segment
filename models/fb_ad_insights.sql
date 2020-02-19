{{ config(
    alias="insights"
) }}

select

    date_start::date as date_day,
    nullif(ad_id,'') as ad_id,
    impressions,
    clicks,
    unique_clicks,
    spend,
    frequency,
    reach,
    link_clicks,
    inline_post_engagements
    

from {{ source('facebook_ads', 'insights') }}
WHERE nullif(ad_id,'') is not null 

