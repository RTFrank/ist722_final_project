with stg_ratings as (
        select * from {{ source('viper','vb_user_ratings')}}
        ),
    stg_items as (
        select * from {{ source('viper','vb_items')}}
        )
select {{ dbt_utils.generate_surrogate_key(['r.rating_id']) }} as ratingkey,
    r.rating_id as ratingid, 
    r.rating_by_user_id as raterid,
    r.rating_for_user_id as rateeid, r.rating_value as ratingvalue,
    coalesce(rater_reviews.review_ct, 0) as raterreviewsreceived,
    coalesce(ratee_reviews.review_ct, 0) as rateereviewsgiven,
    coalesce(rater_avg.avg_ct, 0) as raterreviewsavg,
    coalesce(ratee_avg.avg_ct, 0) as rateereviewsavg,
    coalesce(rater_bought.bought_ct, 0) as rateritemsbought,
    coalesce(ratee_bought.bought_ct, 0) as rateeitemsbought,
    coalesce(rater_sold.sold_ct, 0) as rateritemssold,
    coalesce(ratee_sold.sold_ct, 0) as rateeitemssold,
    r.rating_comment as ratingcomment
    from stg_ratings r
    left join(select rating_by_user_id,
                COUNT(*) as review_ct
            from stg_ratings
            group by rating_by_user_id) rater_reviews
            on r.rating_by_user_id = rater_reviews.rating_by_user_id
    left join(select rating_for_user_id,
                COUNT(*) as review_ct
            from stg_ratings
            group by rating_for_user_id) ratee_reviews
            on r.rating_for_user_id = ratee_reviews.rating_for_user_id
    left join(select rating_by_user_id, 
                AVG(rating_value) as avg_ct
            from stg_ratings
            group by rating_by_user_id) rater_avg
        on r.rating_by_user_id = rater_avg.rating_by_user_id
    left join(select rating_for_user_id, 
                AVG(rating_value) as avg_ct
            from stg_ratings
            group by rating_for_user_id) ratee_avg
        on r.rating_for_user_id = ratee_avg.rating_for_user_id
    left join (select item_buyer_user_id, 
                count(*) as bought_ct
            from stg_items
            where item_sold = true
            group by item_buyer_user_id) rater_bought
        on r.rating_by_user_id = rater_bought.item_buyer_user_id
    left join (select item_buyer_user_id, 
                count(*) as bought_ct
            from stg_items
            where item_sold = true
            group by item_buyer_user_id) ratee_bought
        on r.rating_for_user_id = ratee_bought.item_buyer_user_id
    left join (select item_seller_user_id, 
                count(*) as sold_ct
            from stg_items
            where item_sold = true
            group by item_seller_user_id) rater_sold
        on r.rating_by_user_id = rater_sold.item_seller_user_id
    left join (select item_seller_user_id, 
                count(*) as sold_ct
            from stg_items
            where item_sold = true
            group by item_seller_user_id) ratee_sold
        on r.rating_for_user_id = ratee_sold.item_seller_user_id