with cte as (select * from {{ref('DIM_PRODUCT')}})
select * from cte