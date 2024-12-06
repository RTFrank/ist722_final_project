with vb_user_ratings as (
        select * from {{ source('viper','vb_user_ratings')}}
        ),
    vb_items as (
        select * from {{ source('viper','vb_items')}}
        )
select {{ dbt_utils.generate_surrogate_key(['vb_user_ratings.rating_id']) }} as rating_key,
    r.rating_id, 
    r.rating_by_user_id as rater_id,
    r.rating_for_user_id as ratee_id, 
    coalesce(rater_reviews.review_ct, 0) as rater_reviews_received,
    coalesce(ratee_reviews.review_ct, 0) as ratee_reviews_received,
    coalesce(rater_avg.avg_ct, 0) as rater_reviews_avg,
    coalesce(ratee_avg.avg_ct, 0) as ratee_reviews_avg,
    coalesce(rater_bought.bought_ct, 0) as rater_items_bought,
    coalesce(ratee_bought.bought_ct, 0) as ratee_items_bought,
    coalesce(rater_sold.sold_ct, 0) as rater_items_sold,
    coalesce(ratee_sold.sold_ct, 0) as ratee_items_sold,
    rating_comment
    from vb_user_ratings r
    left join(select rating_by_user_id,
                COUNT(*) as review_ct
            from vb_user_ratings
            group by rating_by_user_id) rater_reviews
            on r.rating_by_user_id = rater_reviews.rating_by_user_id
    left join(select rating_for_user_id,
                COUNT(*) as review_ct
            from vb_user_ratings
            group by rating_for_user_id) ratee_reviews
            on r.rating_for_user_id = ratee_reviews.rating_for_user_id
    left join(select rating_by_user_id, 
                AVG(rating_value) as avg_ct
            from vb_user_ratings
            group by rating_by_user_id) rater_avg
        on r.rating_by_user_id = rater_avg.rating_by_user_id
    left join(select rating_for_user_id, 
                AVG(rating_value) as avg_ct
            from vb_user_ratings
            group by rating_for_user_id) ratee_avg
        on r.rating_for_user_id = ratee_avg.rating_for_user_id
    left join (select item_buyer_user_id, 
                count(*) as bought_ct
            from vb_items
            where item_sold = true
            group by item_buyer_user_id) rater_bought
        on r.rating_by_user_id = rater_bought.item_buyer_user_id
    left join (select item_buyer_user_id, 
                count(*) as bought_ct
            from vb_items
            where item_sold = true
            group by item_buyer_user_id) ratee_bought
        on r.rating_for_user_id = ratee_bought.item_buyer_user_id
    left join (select item_seller_user_id, 
                count(*) as sold_ct
            from vb_items
            where item_sold = true
            group by item_seller_user_id) rater_sold
        on r.rating_by_user_id = rater_sold.item_seller_user_id
    left join (select item_seller_user_id, 
                count(*) as sold_ct
            from vb_items
            where item_sold = true
            group by item_seller_user_id) ratee_sold
        on r.rating_for_user_id = ratee_sold.item_seller_user_id;

