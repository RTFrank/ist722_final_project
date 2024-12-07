with stg_item as (
    select * from {{source('viper', 'vb_items')}}
)
select 
    {{dbt_utils.generate_surrogate_key([stg_item.itemid])}} as itemkey,
    stg_item.itemid as itemid,
    stg_item.item_name as itemname,
    stg_item.item_type as itemtype,
    stg_item.item_description as itemdescription,
    stg_item.item_enddate as itemsolddate,
    stg_item.item_sold as itemsold,
    stg_item.item_seller_user_id as itemselleruserid,
    stg_item.item_buyer_user_id as itembuyeruserid,
    stg_item.item_soldamount as itemsoldamount
from stg_item
