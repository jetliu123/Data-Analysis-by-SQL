select 
    a.byweek,
    a.lxba as `有拉新BA数`,
    a.xz as `新增客户`,
    a.lx as `拉新客户数`,
    a.lxhy as `拉新会员数`,
    b.qiweibuynumber as `线上转化顾客数`,
    b.qiweiorder as `线上订单`,
    b.qiweigmv as `线上gmv`,
    b.oldqiweibuynumber as `老客转化顾客数`,
    b.oldqiweiorder as `老客订单数`,
    b.oldqiweigmv as `老客gmv`,
    b.ydnewcustomeraddqw as `云店新可有下单添加数`,
    c.dynewvisit as `云店新客添加企微访问云店数`,
    c.qwoldvisit as `企微老客添加企微访问云店数`,
    d.threemonthfirstba as `参与云店新客首单转化BA数`,
    d.threemonthsecondba as `参与云店新客二单转化BA数`,
    d.threemonththirdba as `参与云店新客三单转化BA数`,
    e.firstnumbers as `云店新客首单转化顾客数`,
    e.firstorders as `云店新客订单数`,
    e.firstgmv as `云店新客首单GMV`,
    e.secondnumbers as `云店新客二单转化顾客数`,
    e.secondorders as `云店新客二单订单数`,
    e.secondgmv as `云店新客二单GMV`,
    e.thirdnumbers as `云店新客三单转化顾客数`,
    e.thirdorders as `云店新客三单订单数`,
    e.thirdgmv as `云店新客三单GMV`
--- 解决有拉新新增会员相关数据
from 
(
select 
    'byweek' AS `byweek`,
    count(distinct customer_union_id) xz,
    count(distinct case when is_first_relation = 1 then ba_id end) lxba,
    count(distinct case when is_first_relation = 1 then customer_union_id end) lx,
    count(distinct case when is_first_relation = 1 and first_add_member_card_tier in ('Base','Elite','Elite Plus') then customer_union_id end) lxhy
from 
    01_dwb_qw_customer_staff_relation_v1
where add_time >= DATE_SUB(to_date(now()),7) and add_time < to_date(now())
and ds = to_date(subdate(NOW(),1))  
) a 
left join 
-- 解决是友好线上转化相关数据
(
select
    'byweek' AS `byweek`,
    -- 求是好友下单情况
    count(distinct t2.union_id) as qiweibuynumber, -- 线上转化数
    count(distinct case when t2.union_id is not null then t1.order_id end) as qiweiorder, -- 线上订单
    sum(case when t2.union_id is not null then t1.gmv end) as qiweigmv,  -- 线上gmv
    -- 解决老客的数据
    count(distinct case when t2.is_first_relation = 0 then t2.union_id end) as oldqiweibuynumber,  --云店老客转化数
    count(distinct case when t2.is_first_relation = 0 and  t2.union_id is not null then t1.order_id end) as oldqiweiorder, --云店老客订单
    sum(case when t2.is_first_relation = 0 and t2.union_id is not null then t1.gmv end) as oldqiweigmv, -- 云店老客gmv
    --- 是新客的数
    count(distinct case when t1.ds <= t3.d then t2.union_id end) as ydnewcustomeraddqw  -- 云店新客添加数(有下单行为)
from
(
    select
        aa.ds as ds,
        aa.ba_id as ba_id,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id,
        SUM(aa.order_amt) as gmv
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>= DATE_SUB(to_date(now()),7) and aa.ds< to_date(now()) and aa.app_env='云店' and aa.type='PayOrderDetailServer'
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
GROUP BY aa.ds,aa.ba_id,aa.user_union_id,aa.order_id) t1
left join
(select
    add_time,
    ba_id,
    is_first_relation,
    customer_union_id as union_id
from
    01_dwb_qw_customer_staff_relation_v1
where
add_time >= DATE_SUB(to_date(now()),7) and add_time < to_date(now())
and ds = to_date(subdate(NOW(),1))
group by add_time,ba_id,customer_union_id,is_first_relation
) t2
on t1.ba_id = t2.ba_id and t1.ds >= to_date(t2.add_time) and t1.user_union_id = t2.union_id
left join 
(
select 
    min(ds) d,
    user_union_id
from `01_dwb_wdc_events_v2`
where type = 'PayOrderServer' and app_env='云店'
group by user_union_id
) t3
on t1.user_union_id = t3.user_union_id
) b 
on a.byweek = b.byweek
left join 
-- 云店新客加企微访问uv 
(
select
    'byweek' AS `byweek`, -- 访问时间
    count(distinct case when t1.ds <= t3.d then t2.customer_union_id end) dynewvisit,   --云店新客添加企微访问人数
    count(distinct case when t1.ds > t3.d and t2.customer_union_id is not null then t1.user_union_id end) qwoldvisit  -- 云店老客添加企微访问
    -- t1.ba_id as ba_id,  -- BA
    -- t1.user_union_id as user_union_id,  -- 访问云店客户数
    -- t2.customer_union_id as union_id,  -- 这个是企微好友
    -- case when t1.ds > t3.d then 'old' else 'new' end as type  --该时间段访问时间大于首次访问时间为老客
from
(
select
    ds,
    ba_id,
    user_union_id
from
`01_dwb_wdc_events_v2`
where ds>= DATE_SUB(to_date(now()),7) and ds< to_date(now())
and app_env='云店' and type='load' and ba_id is not null
group by ds,ba_id,user_union_id
) t1
left join
(
select
    ba_id,
    add_time,
    customer_union_id,
    is_first_relation
from
01_dwb_qw_customer_staff_relation_v1
where add_time >= DATE_SUB(to_date(now()),7) and add_time < to_date(now())
and ds = to_date(subdate(NOW(),1))
group by ba_id,add_time,customer_union_id,is_first_relation
) t2
on t1.ba_id = t2.ba_id and t1.user_union_id = t2.customer_union_id and to_date(t2.add_time) <= t1.ds
left join
(
     select min(ds) d,
    user_union_id
from `01_dwb_wdc_events_v2`
where type = 'load' and app_env='云店'
group by user_union_id) t3
on t1.user_union_id = t3.user_union_id
) c 
on a.byweek = c.byweek
left join 
--- 解决新客参与BA数
(
select 
    'byweek' AS `byweek`,
    count(distinct case when aa1.ds <= t5.d and aa1.user_union_id is not null then t1.ba_id end) threemonthfirstba,   -- '180天内云店新客首单转化BA数',
    count(distinct case when aa1.ds <= t5.d and aa2.user_union_id is not null then t1.ba_id end) threemonthsecondba,  -----'180天内新客第二单转化BA数',
    count(distinct case when aa1.ds <= t5.d and aa3.user_union_id is not null then t1.ba_id end) threemonththirdba --'180天内新客第三单转化BA数'
from 
(
select 
    add_time,
    ba_id,
    date_add(add_time,180) gapt,
    customer_union_id
from  01_dwb_qw_customer_staff_relation_v1
where add_time >= DATE_SUB(to_date(now()),180)  -- add_time >= '2022-11-15 00:00:00'  -- 用某一周的最后一天减去180天
and ds = to_date(subdate(NOW(),1))
group by add_time,ba_id,date_add(add_time,180),customer_union_id
) t1
-- 解决第一单的问题 
left join 
( 
select 
    a1.ds as ds,
    a1.ba_id,
    a1.user_union_id as user_union_id,
    a1.order_id as order_id
from 
(
select 
    t4.ds as ds,
    t4.ba_id as ba_id,
    t4.user_union_id as user_union_id,
    t4.order_id as order_id,
    t4.orderseries as orderseries
from 
(
select 
    t3.ds as ds,
    t3.ba_id as ba_id,
    t3.time as time,
    t3.user_union_id as user_union_id,
    t3.order_id as order_id,
    row_number() over(partition by t3.ba_id,t3.user_union_id order by t3.time asc) as orderseries
from 
(
select
        aa.ds as ds,
        aa.ba_id,
        aa.time as time,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>= DATE_SUB(to_date(now()),7) and aa.ds< to_date(now()) 
and aa.app_env='云店' and aa.type='PayOrderDetailServer' 
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
group by aa.ds,aa.ba_id,aa.time,aa.user_union_id,aa.order_id
) t3 
) t4 
where t4.orderseries = 1
) a1 
) aa1 
on t1.ba_id = aa1.ba_id and t1.customer_union_id = aa1.user_union_id 
and aa1.ds >= to_date(t1.add_time) and aa1.ds <= t1.gapt 
-- 解决新老客
left join 
(
select min(ds) d,
    user_union_id
from `01_dwb_wdc_events_v2`
where type = 'PayOrderServer' and app_env='云店'
group by user_union_id) t5
on aa1.user_union_id = t5.user_union_id
left join 
--- 第二单的情况
( 
select 
    a2.ds as ds,
    a2.ba_id as ba_id,
    a2.user_union_id as user_union_id,
    a2.order_id as order_id
from 
(
select 
    t4.ds as ds,
    t4.ba_id,
    t4.user_union_id as user_union_id,
    t4.order_id as order_id,
    t4.orderseries as orderseries
from 
(
select 
    t3.ds as ds,
    t3.ba_id as ba_id,
    t3.user_union_id as user_union_id,
    t3.order_id as order_id,
    row_number() over(partition by t3.ba_id,t3.user_union_id order by t3.time asc) as orderseries
from 
(
select
        aa.ds as ds,
        aa.ba_id as ba_id,
        aa.time as time,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>=DATE_SUB(to_date(now()),7) and aa.ds< to_date(now())
and aa.app_env='云店' and aa.type='PayOrderDetailServer' 
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
group by aa.ds,aa.ba_id,aa.time,aa.user_union_id,aa.order_id
) t3 
) t4 
where t4.orderseries = 2 
) a2
) aa2
on t1.customer_union_id = aa2.user_union_id and t1.ba_id = aa2.ba_id 
and aa2.ds >= to_date(t1.add_time) and aa2.ds <= t1.gapt
-- 解决第三单情况
--- aa3的情况
left join 
( 
select 
    a3.ds as ds,
    a3.ba_id as ba_id,
    a3.user_union_id as user_union_id,
    a3.order_id as order_id
from 
(
select 
    t4.ds as ds,
    t4.ba_id as ba_id,
    t4.user_union_id as user_union_id,
    t4.order_id as order_id,
    t4.orderseries as orderseries
from 
(
select 
    t3.ds as ds,
    t3.ba_id as ba_id,
    t3.user_union_id as user_union_id,
    t3.order_id as order_id,
    row_number() over(partition by t3.user_union_id,t3.ba_id order by t3.time asc) as orderseries
from 
(
select
        aa.ds as ds,
        aa.ba_id as ba_id,
        aa.time as time,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_name,sku_id,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>= DATE_SUB(to_date(now()),7) and aa.ds < to_date(now()) -- 真真的计算起来就不用管时间了
and aa.app_env='云店' and aa.type='PayOrderDetailServer' 
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
group by aa.ds,aa.ba_id,aa.time,aa.user_union_id,aa.order_id
) t3 
) t4 
where t4.orderseries = 3 
) a3
) aa3
on t1.customer_union_id = aa3.user_union_id and t1.ba_id = aa3.ba_id and aa3.ds >= to_date(t1.add_time) and aa3.ds <= t1.gapt
) d
on a.byweek = d.byweek 
-- 解决新客三单销售的情况
left join 
(
select 
   'byweek' AS `byweek`,
    count(distinct case when aa1.ds <= t5.d then aa1.user_union_id end) firstnumbers,       -- '180天内新客第一单下单人数',
    count(distinct case when aa1.ds <= t5.d then aa1.order_id end) firstorders,               -- '180天内新客第一单订单数',
    sum(case when aa1.ds <= t5.d then aa1.gmv end) firstgmv,                         ---'180天内新客第一笔订单gmv', 
    count(distinct case when aa1.ds <= t5.d then aa2.user_union_id end) secondnumbers,                    -- '180天内新客第二单下单人数',
    count(distinct case when aa1.ds <= t5.d then aa2.order_id end) secondorders,                 --- '180天内新客第二单订单数',
    sum(case when aa1.ds <= t5.d then aa2.gmv end) secondgmv,                -- '180天第二单gmv',
    count(distinct case when aa1.ds <= t5.d then aa3.user_union_id end) thirdnumbers,     --- '180天内新客第三单下单人数',
    count(distinct case when aa1.ds <= t5.d then aa3.order_id end) thirdorders,         --'180天内新客第三单订单数',
    sum(case when aa1.ds <= t5.d then aa3.gmv end) thirdgmv                  --- '180天内新客第三单gmv'
from 
(
select 
    add_time,
    ba_id,
    date_add(add_time,180) gapt,
    customer_union_id
from  01_dwb_qw_customer_staff_relation_v1
where add_time >= DATE_SUB(to_date(now()),180)     -- '2022-11-15 00:00:00'  -- 用某一周的最后一天减去180天
and ds = to_date(subdate(NOW(),1))
group by add_time,ba_id,date_add(add_time,180),customer_union_id
) t1 
left join
( 
select 
    a1.ds as ds,
    a1.ba_id as ba_id,
    a1.user_union_id as user_union_id,
    a1.order_id as order_id,
    a1.gmv as gmv
from 
(
select 
    t4.ds as ds,
    t4.ba_id as ba_id,
    t4.user_union_id as user_union_id,
    t4.order_id as order_id,
    t4.gmv as gmv,
    t4.orderseries as orderseries
from 
(
select 
    t3.ds as ds,
    t3.ba_id as ba_id,
    t3.user_union_id as user_union_id,
    t3.order_id as order_id,
    t3.gmv as gmv,
    row_number() over(partition by t3.user_union_id,t3.ba_id order by t3.time asc) as orderseries
from 
(
select
        aa.ds as ds,
        aa.ba_id as ba_id,
        aa.time as time,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id,
        SUM(aa.order_amt) as gmv
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>=DATE_SUB(to_date(now()),7) and aa.ds<to_date(now()) -- 真真的计算起来就不用管时间了
and aa.app_env='云店' and aa.type='PayOrderDetailServer' 
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
group by aa.ds,aa.user_union_id,aa.order_id,aa.time,aa.ba_id
) t3 
) t4 
where t4.orderseries = 1
) a1 
) aa1 
on t1.customer_union_id = aa1.user_union_id and aa1.ds >= to_date(t1.add_time) and aa1.ds <= t1.gapt and t1.ba_id = aa1.ba_id
--- 判断新老客的情况
left join 
(
select min(ds) d,
    user_union_id
from `01_dwb_wdc_events_v2`
where type = 'PayOrderServer' and app_env='云店'
group by user_union_id) t5
on aa1.user_union_id = t5.user_union_id
left join 
--- 第二单的情况
( 
select 
    a2.ds as ds,
    a2.ba_id as ba_id,
    a2.user_union_id as user_union_id,
    a2.order_id as order_id,
    a2.gmv as gmv
from 
(
select 
    t4.ds as ds,
    t4.ba_id as ba_id,
    t4.user_union_id as user_union_id,
    t4.order_id as order_id,
    t4.gmv as gmv,
    t4.orderseries as orderseries
from 
(
select 
    t3.ds as ds,
    t3.ba_id as ba_id,
    t3.user_union_id as user_union_id,
    t3.order_id as order_id,
    t3.gmv as gmv,
    row_number() over(partition by t3.user_union_id,t3.ba_id order by t3.time asc) as orderseries
from 
(
select
        aa.ds as ds,
        aa.time as time,
        aa.ba_id as ba_id,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id,
        SUM(aa.order_amt) as gmv
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>= DATE_SUB(to_date(now()),7) and aa.ds< to_date(now()) -- 真真的计算起来就不用管时间了
and aa.app_env='云店' and aa.type='PayOrderDetailServer' 
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
group by aa.ds,aa.user_union_id,aa.order_id,aa.time,aa.ba_id
) t3 
) t4 
where t4.orderseries = 2 
) a2
) aa2
on t1.customer_union_id = aa2.user_union_id and aa2.ds >= to_date(t1.add_time) and aa2.ds <= t1.gapt and aa2.ba_id = t1.ba_id
--- aa3的情况
left join 
( 
select 
    a3.ds as ds,
    a3.ba_id as ba_id,
    a3.user_union_id as user_union_id,
    a3.order_id as order_id,
    a3.gmv as gmv
from 
(
select 
    t4.ds as ds,
    t4.ba_id as ba_id,
    t4.user_union_id as user_union_id,
    t4.order_id as order_id,
    t4.gmv as gmv,
    t4.orderseries as orderseries
from 
(
select 
    t3.ds as ds,
    t3.ba_id as ba_id,
    t3.user_union_id as user_union_id,
    t3.order_id as order_id,
    t3.gmv as gmv,
    row_number() over(partition by t3.user_union_id,t3.ba_id order by t3.time asc) as orderseries
from 
(
select
        aa.ds as ds,
        aa.ba_id as ba_id,
        aa.time as time,
        aa.user_union_id as user_union_id,
        aa.order_id as order_id,
        SUM(aa.order_amt) as gmv
from `01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    category_b_level_1_id
from
    `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>=DATE_SUB(to_date(now()),7) and aa.ds< to_date(now())  -- 真真的计算起来就不用管时间了
and aa.app_env='云店' and aa.type='PayOrderDetailServer' 
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
group by aa.ds,aa.user_union_id,aa.order_id,aa.time,aa.ba_id
) t3 
) t4 
where t4.orderseries = 3 
) a3
) aa3
on t1.customer_union_id = aa3.user_union_id and aa3.ds >= to_date(t1.add_time) and aa3.ds <= t1.gapt and aa3.ba_id = t1.ba_id
) e 
on a.byweek = e.byweek
