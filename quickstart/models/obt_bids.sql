WITH f_bids as(
    SELECT * FROM {{ ref('fact_bids')}}
),
d_bidder as (
    SELECT
    userkey as bidderkey,
    userid as bidderid, userfirstname as bidderfirstname, userlastname as bidderlastname,
        usernamelastfirst as biddernamelastfirst, usernamefirstlast as biddernamefirstlast,
        useremail as bidderemail, usercity as biddercity, userstate as bidderstate, 
        userzipcode as bidderzipcode, userlatitude as bidderlatitude, userlongitude as bidderlongitude   
    FROM {{ ref('dim_user')}}
),
d_seller as (
    SELECT
    userkey as sellerkey,
    userid as sellerid, userfirstname as sellerfirstname, userlastname as sellerlastname,
        usernamelastfirst as sellernamelastfirst, usernamefirstlast as sellernamefirstlast,
        useremail as selleremail, usercity as sellercity, userstate as sellerstate, 
        userzipcode as sellerzipcode, userlatitude as sellerlatitude, userlongitude as sellerlongitude
    FROM {{ ref('dim_user')}}
),
d_item as (
    SELECT * FROM {{ ref('dim_item')}}
),
d_date as (
    SELECT * FROM {{ ref('dim_date')}}
)
SELECT
    f.bidid, f.biddatekey, f.bidamount, f.bidstatus, b.*,
    i.*, s.*, d.*
FROM f_bids f
LEFT JOIN d_bidder b ON f.bidderkey = b.bidderkey
LEFT JOIN d_seller s ON f.sellerkey = s.sellerkey
LEFT JOIN d_item i ON f.itemkey = i.itemkey
LEFT JOIN d_date d ON f.biddatekey = d.datekey