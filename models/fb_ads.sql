{{ config(
    alias="ads"
) }}

select distinct

    nullif(id,'') as ad_id,
    nullif(account_id,'') as account_id,
    nullif(adset_id,'') as adset_id,
    nullif(campaign_id,'') as campaign_id,
    nullif(name,'') as name,
    nullif(utm_term,'') as utm_term,
    nullif(utm_campaign,'') as utm_campaign,
    nullif(utm_medium,'') as utm_medium,
    nullif(utm_source,'') as utm_source,
    nullif(utm_content,'') as utm_content,
    nullif( {{ dbt_utils.get_url_parameter('url_parameters', var('fbaid') ) }}, '') as fbaid,
    url_parameters,
    RECEIVED_AT as updated_at
    
from {{ source('facebook_ads', 'ads') }}


