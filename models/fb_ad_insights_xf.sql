{{ config(
    materialized="ephimeral"
) }}

with ads1 as (

  select * from {{ref('fb_ads_xf')}}

), insights1 as (

  select * from {{ref('fb_ad_insights')}}

), campaigns1 as (

  select * from {{ref('fb_campaigns')}}

), adsets1 as (

  select * from {{ref('fb_adsets_xf')}}

), final1 as (

    select
    
        {{ dbt_utils.surrogate_key('i.date_day', 'i.ad_id') }} as id,
        i.*,
        coalesce(creatives.mg_fbaid, creatives.fbaid) as fbaid,
        ads.unique_id as ad_unique_id,
        ads.name as ad_name,
        campaigns.name as campaign_name,
        adsets.name as adset_name

    from insights1 as i
    left outer join ads1 as ads
        on i.ad_id = ads.ad_id
        and i.date_day >= date_trunc('day', ads.effective_from)::date
        and (i.date_day < date_trunc('day', ads.effective_to)::date or ads.effective_to is null)
    left outer join campaigns1 as campaigns on campaigns.campaign_id = i.campaign_id
    left outer join adsets1 as adsets on adsets.adset_id = i.adset_id

)

select * from final1
