with stg_bids as (
    select bid_id, bid_amount, bid_status, bid_item_id,
        bid_user_id,
        replace(to_date(bid_datetime)::varchar,'-','')::int as biddatekey
    from {{ source('viper','vb_bids')}}
),
stg_bidders as (
    select {{ dbt_utils.generate_surrogate_key(['user_id']) }} as bidderkey,
        user_id 
    from {{ source('viper','vb_users')}}
),
stg_sellers as (
    select {{ dbt_utils.generate_surrogate_key(['user_id']) }} as sellerkey,
        user_id
    from {{ source('viper','vb_users')}}
),
stg_items as (
    select {{ dbt_utils.generate_surrogate_key(['item_id']) }} as itemkey,
    item_id, item_seller_user_id
    from {{ source('viper','vb_items')}}
)
SELECT bd.bid_id as bidid, i.itemkey as itemkey, b.bidderkey as bidderkey,
     s.sellerkey as sellerkey, bd.biddatekey as biddatekey, bd.bid_amount as bidamount,
     bd.bid_status as bidstatus
FROM stg_bids bd
LEFT JOIN stg_bidders b ON bd.bid_user_id = b.user_id
LEFT JOIN stg_items i ON i.item_id = bd.bid_item_id
LEFT JOIN stg_sellers s ON s.user_id = i.item_seller_user_id
