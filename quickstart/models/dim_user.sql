with stg_users as (
    select * from {{ source('viper','vb_users')}}
),
stg_zip_codes as (
    select * from {{ source('viper','vb_zip_codes')}}
)
SELECT {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as userkey,
    u.user_id as userid, u.user_firstname as userfirstname, u.user_lastname as userlastname, 
    concat(u.user_firstname ,' ' , u.user_lastname) as usernamefirstlast,
    concat(u.user_lastname ,', ' , u.user_firstname) as usernamelastfirst,
    u.user_email as useremail, z.zip_city as usercity, z.zip_state as userstate, z.zip_code as userzipcode
FROM stg_users u
LEFT JOIN stg_zip_codes z ON u.user_zip_code = z.zip_code


