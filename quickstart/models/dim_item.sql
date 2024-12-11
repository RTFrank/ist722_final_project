with stg_item as (
    select * from {{source('viper', 'vb_items')}}
)
select 
    {{dbt_utils.generate_surrogate_key(['item_id'])}} as itemkey,
    item_id as itemid,
    item_name as itemname,
    item_type as itemtype,
    item_description as itemdescription,
    item_enddate as auctionenddate,
    item_sold as itemsold,
    item_seller_user_id as itemselleruserid,
    item_buyer_user_id as itembuyeruserid,
    item_soldamount as itemsoldamount
from stg_item 