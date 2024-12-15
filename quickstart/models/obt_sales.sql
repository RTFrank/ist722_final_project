WITH sales as (
    select * from {{ ref('fact_sales')}}
),
buyer as (
    SELECT
    userkey as buyerkey,
    userid as buyerid, userfirstname as buyerfirstname, userlastname as buyerlastname,
    usernamelastfirst as buyernamelastfirst, usernamefirstlast as buyernamefirstlast,
    useremail as buyeremail, usercity as buyercity, userstate as buyerstate, 
    userzipcode as buyerzipcode, userlatitude as buyerlatitude, userlongitude as buyerlongitude
    from {{ ref('dim_user')}}
),
seller as (
    SELECT 
    userkey as sellerkey,
    userid as sellerid, userfirstname as sellerfirstname, userlastname as sellerlastname,
    usernamelastfirst as sellernamelastfirst, usernamefirstlast as sellernamefirstlast,
    useremail as selleremail, usercity as sellercity, userstate as sellerstate, 
    userzipcode as sellerzipcode, userlatitude as sellerlatitude, userlongitude as sellerlongitude
    from {{ ref('dim_user')}}
),
item as (
    SELECT * FROM 
    {{ ref('dim_item')}}
),
date as (
    SELECT * FROM {{ ref('dim_date')}}
)
SELECT
    f.*, b.*,
    i.*, s.*, d.*
FROM sales f
LEFT JOIN buyer b ON f.buyerid = b.buyerid
LEFT JOIN seller s ON f.sellerid = s.sellerid
LEFT JOIN item i ON f.itemid = i.itemid
LEFT JOIN date d ON f.auctionenddate = d.datekey