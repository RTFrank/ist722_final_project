with ratings as (
        select * from {{ source('viper','vb_user_ratings')}}
        ),
    items as (
        select * from {{ source('viper','vb_items')}}
        ),
    users as (
        select * from {{ source('viper', 'vb_users')}}
    )

select rating_id, 
    r.rating_by_user_id as rater_id,
    rater.user_email as rater_email,
    rater.user_firstname as rater_first_name,
    rater.user_lastname as rater_last_name,
    concat(rater.user_firstname, ' ', rater.user_lastname) as rater_full_name,
    rater.user_zip_code as rater_zip_code,
    coalesce(rater_reviews.review_ct, 0) as rater_reviews_received,
    coalesce(rater_avg.avg_ct, 0) as rater_reviews_avg,
    coalesce(rater_bought.bought_ct, 0) as rater_items_bought,
    coalesce(rater_sold.sold_ct, 0) as rater_items_sold,
    r.rating_for_user_id as ratee_id,
    ratee.user_email as ratee_email,
    ratee.user_firstname as ratee_first_name,
    ratee.user_lastname as ratee_last_name,
    concat(ratee.user_firstname, ' ', ratee.user_lastname) as ratee_full_name,
    ratee.user_zip_code as ratee_zip_code,
    coalesce(ratee_reviews.review_ct, 0) as ratee_reviews_received,
    coalesce(ratee_avg.avg_ct, 0) as ratee_reviews_avg,
    coalesce(ratee_bought.bought_ct, 0) as ratee_items_bought,
    coalesce(ratee_sold.sold_ct, 0) as ratee_items_sold,
    r.rating_astype,
    item_info.item_name,
    item_info.item_type,
    r.rating_value,
    r.rating_comment,
    from ratings r
    join users rater on r.rating_by_user_id = rater.user_id
    join users ratee on r.rating_for_user_id = ratee.user_id
    left join(select rating_by_user_id,
                COUNT(*) as review_ct
            from ratings
            group by rating_by_user_id) rater_reviews
            on r.rating_by_user_id = rater_reviews.rating_by_user_id
    left join(select rating_for_user_id,
                COUNT(*) as review_ct
            from ratings
            group by rating_for_user_id) ratee_reviews
            on r.rating_for_user_id = ratee_reviews.rating_for_user_id
    left join(select rating_by_user_id, 
                AVG(rating_value) as avg_ct
            from ratings
            group by rating_by_user_id) as rater_avg
        on r.rating_by_user_id = rater_avg.rating_by_user_id
    left join(select rating_for_user_id, 
                AVG(rating_value) as avg_ct
            from ratings
            group by rating_for_user_id) as ratee_avg
        on r.rating_for_user_id = ratee_avg.rating_for_user_id
    left join (select item_buyer_user_id, 
                count(*) as bought_ct
            from items
            where item_sold = true
            group by item_buyer_user_id) rater_bought
        on r.rating_by_user_id = rater_bought.item_buyer_user_id
    left join (select item_buyer_user_id, 
                count(*) as bought_ct
            from items
            where item_sold = true
            group by item_buyer_user_id) ratee_bought
        on r.rating_for_user_id = ratee_bought.item_buyer_user_id
    left join (select item_seller_user_id, 
                count(*) as sold_ct
            from items
            where item_sold = true
            group by item_seller_user_id) rater_sold
        on r.rating_by_user_id = rater_sold.item_seller_user_id
    left join (select item_seller_user_id, 
                count(*) as sold_ct
            from items
            where item_sold = true
            group by item_seller_user_id) ratee_sold
        on r.rating_for_user_id = ratee_sold.item_seller_user_id
    left join (select item_seller_user_id, item_buyer_user_id, item_name, item_type
            from items
            where item_sold = true) item_info
        on r.rating_by_user_id = item_info.item_seller_user_id
        and r.rating_for_user_id = item_info.item_buyer_user_id