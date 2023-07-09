---- 云店新客购买
select
    tt.sku_id,
    tt.sku_name,
    tt.is_ob,
    sum(case when tt.type = 'new' then tt.order_amt end) '云店新客购买商品gmv'
from
(
select
    t1.ds as ds, -- 下单日趋
    t1.user_union_id as user_union_id,  -- 下单客户
    t1.sku_id as sku_id,
    t1.sku_name as sku_name,
    t1.is_ob as is_ob,
    t1.order_amt as order_amt, -- GMV
    case when t1.ds > t3.d then 'old' else 'new' end as type  --该时间段下单时间大于首单时间为老客
from
(
select
    aa.ds as ds,
    aa.sku_id as sku_id,
    aa.sku_name as sku_name,
    aa.user_union_id as user_union_id,
    aa.order_amt as order_amt,
    bb.is_ob as is_ob
from
`01_dwb_wdc_events_v2` aa
LEFT JOIN
(
select
    sku_id,
    sku_name,
    is_ob,
    category_b_level_1_id
from `01_dim_product_v1`
group by sku_id,sku_name,is_ob,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>=DATE_SUB(to_date(now()),7) and aa.ds< to_date(now()) and aa.app_env='云店'
and aa.type='PayOrderDetailServer' and bb.category_b_level_1_id !='99' and aa.ba_id is not null
) t1
left join
(
     select 
        min(ds) d,
        user_union_id
from `01_dwb_wdc_events_v2`
where type = 'PayOrderServer' and app_env='云店'
group by user_union_id) t3
on t1.user_union_id = t3.user_union_id
) tt
group by tt.sku_id,tt.sku_name,tt.is_ob