{{ config(
    alias="campaigns"
) }}

select

    nullif(id,'') as campaign_id,
    nullif(name,'') as name
    
from {{ source('facebook_ads', 'campaigns') }}
