with stg_sale as 
( 
    select 
    row_number() over (order by i.item_enddate) as transactionid,
    i.item_id,
    i.item_name,
    i.item_type,
    i.item_description,
    i.item_reserve,
    replace(to_date(i.item_enddate)::varchar,'-','')::int as item_enddate,
    i.item_sold,
    i.item_seller_user_id,
    i.item_buyer_user_id,
    i.item_soldamount,
    s.user_id as selleruserid,
    s.user_firstname as sellerfirstname,
    s.user_lastname as sellerlastname,
    s.user_zip_code as seller_zip_code,
    b.user_id as buyeruserid,
    b.user_firstname as buyerfirstname,
    b.user_lastname as buyerlastname,
    b.user_zip_code as buyer_zip_code
    from {{source('viper', 'vb_items')}} i
    left join {{source('viper', 'vb_users')}} s 
        on i.item_seller_user_id = s.user_id
    left join {{source('viper', 'vb_users')}} b
        on i.item_buyer_user_id = b.user_id
    where i.item_sold = TRUE 
)
select 
    {{dbt_utils.generate_surrogate_key(['transactionid'])}} as transactionkey,
    stg_sale.transactionid,
    stg_sale.item_id as itemid,
    stg_sale.selleruserid as sellerid,
    stg_sale.buyeruserid as buyerid,
    stg_sale.item_enddate as saledate,
    stg_sale.item_soldamount as saleamount,
    stg_sale.item_reserve as itemlistamount,
    stg_sale.buyer_zip_code as deliveryzipcode,
    stg_sale.seller_zip_code as salezipcode,
    stg_sale.item_soldamount - stg_sale.item_reserve as itemprofit 
from stg_sale
    
    

