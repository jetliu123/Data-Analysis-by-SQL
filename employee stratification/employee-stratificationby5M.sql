-- 拉新，新增会员等信息
select
    a.staff_work_no,
    count(c.id) as xz,
    count(distinct case when a.first_add_member_card_tier in ('Base','Elite','Elite Plus') then c.id end) as xzhy,
    count(distinct case when a.is_first_relation =1 then c.id end) as lx,
    count(distinct case when a.is_first_relation = 1 and a.first_add_member_card_tier in ('Base','Elite','Elite Plus') then c.id end) as lxhy
from qw_customer_staff_relation a
left join qw_customer c
ON a.customer_id=c.id
WHERE a.add_time >='2023-05-01 00:00:00' and a.add_time <'2023-06-01 00:00:00'
group by a.staff_work_no;


--企微客户数
SELECT
a.staff_work_no,
count(c.id)
FROM qw_customer_staff_relation a LEFT JOIN qw_customer c
ON a.customer_id=c.id
where a.create_time >=  '2023-05-01 00:00:00' and a.create_time < '2023-05-01 00:00:00'
AND a.deleted=0 -- 是否删除 0未删除，1=已删除
group by a.staff_work_no;


-- 完全流失客户数
SELECT
a.employee_number,
COUNT(distinct a.customer_id)
FROM
(--每个顾客最后一条删除记录的时间及staff_id
SELECT a.customer_id,a.union_id, -- 客户union_id
a.staff_work_no as employee_number ,a.deleted_time
from qw_customer_staff_relation a left join qw_customer c
on a.customer_id=c.id
where a.deleted !=0 and (a.delete_type = 1  or a.delete_type = 4)
order by a.customer_id,a.deleted_time desc LIMIT 1 BY a.customer_id) a
left join (
-- 完全流失 未删除BA数=0即完全流失
select customer_id ,banum from
(select customer_id,COUNT(staff_work_no) as banum from qw_customer_staff_relation
WHERE deleted=0 group by customer_id) ta
) b
on a.customer_id=b.customer_id
WHERE banum = 0
AND a.deleted_time>='2023-05-01 00:00:00' and a.deleted_time<'2023-06-01 00:00:00'
group by a.employee_number;

select 
if(t1.ba_code is not null,t1.ba_code,t2.ba_code) ba_code,
t1.views,
t2.launch
from 
(
select 
    ba_code,
    count(ba_code) as views
from 
    events 
where event='$MPViewScreen' and mp_name='屈臣氏BA助手' 
and date>='2023-05-01' and date<='2023-05-31'
group by ba_code) t1 
full join 
(
--小程序启动
select 
    ba_code,
    count(ba_code) as launch 
from 
    events 
where event='$MPLaunch' and mp_name='屈臣氏BA助手' 
and date>='2023-05-01' and date<='2023-05-31' 
group by ba_code) t2 
on t1.ba_code = t2.ba_code;


--浏览云店UV
select 
  ba_code,
  count(distinct union_id)
from events 
where date >= '2022-05-01' and date <= '2022-05-31'
and event='load' and mp_name='云店'
group by ba_code;



-- 重新构造逻辑写
select
    t1.ba_id,
    --总的数据
    count(distinct t1.user_union_id) as totalbuynumber,
    count(distinct t1.order_id) as totalorder,
    sum(t1.gmv)  as totalgmv,
    -- 求是好友下单情况
    count(distinct t2.union_id)  as qiweibuynumber,
    count(distinct case when t2.union_id is not null then t1.order_id end)  as qiweiorder,
    sum(case when t2.union_id is not null then t1.gmv end)  as qiweigmv
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
group by sku_id,sku_name,category_b_level_1_id)  bb
on aa.sku_id=bb.sku_id
where aa.ds>='2023-05-01' and aa.ds<'2023-06-01' and aa.app_env='云店' and aa.type='PayOrderDetailServer'
and bb.category_b_level_1_id !='99' and aa.ba_id is not null
GROUP BY aa.ba_id,aa.user_union_id,aa.order_id,aa.ds) t1
left join
(select
    add_time,
    customer_union_id as union_id,
    ba_id
from
    01_dwb_qw_customer_staff_relation_v1
where add_time >= '2023-05-01 00:00:00' and add_time < '2023-06-01 00:00:00' 
and ds ='2025-05-14'
group by customer_union_id,ba_id,add_time
) t2
on t1.ba_id = t2.ba_id and t1.user_union_id = t2.union_id and t1.ds >= to_date(t2.add_time)
group by t1.ba_id;







--- 云店新客转化顾客数 by month 
select
    tt.ba_id as ba_id,
    count(distinct case when tt.type = 'new' then tt.union_id end) newcustomerpaid    ---  '云店新客转化顾客数'  -- 云店新客转化顾客数
from
(
select
    t1.ds as ds, -- 下单日趋
    t1.ba_id as ba_id,  -- 下单BA
    t1.user_union_id as user_union_id,  -- 下单客户
    t2.customer_union_id as union_id,  -- 这个是企微好友
    case when t1.ds > t3.d then 'old' else 'new' end as type  --该时间段下单时间大于首单时间为老客
from
(
select
    aa.ds as ds,
    aa.ba_id as ba_id,
    aa.user_union_id as user_union_id
from
`01_dwb_wdc_events_v2` aa
LEFT JOIN
(
select
    sku_id,
    sku_name,
    category_b_level_1_id
from `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>='2023-04-03' and aa.ds< '2023-04-10' and aa.app_env='云店'
and aa.type='PayOrderDetailServer' and bb.category_b_level_1_id !='99' and aa.ba_id is not null
) t1
left join
(
    select
    ba_id,
    add_time,
    customer_union_id
from
01_dwb_qw_customer_staff_relation_v1
where add_time >='2023-04-03 00:00:00' and add_time <'2023-04-10 00:00:00'
and ds = '2023-04-11'
group by ba_id,add_time,customer_union_id
) t2
on t1.ba_id = t2.ba_id and t1.user_union_id = t2.customer_union_id and to_date(t2.add_time) <= t1.ds  -- 后面的时间越大
left join
(
     select min(ds) d,
    user_union_id
from `01_dwb_wdc_events_v2`
where type = 'PayOrderServer' and app_env='云店'
group by user_union_id) t3
on t1.user_union_id = t3.user_union_id
) tt
group by tt.ba_id
     