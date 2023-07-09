---- 各个渠道的top20商品
select
    case when t2.qd in ('B端云店-order','企微-个人卡片','企微-企微领券','B端云店-post','企微-欢迎语','B端云店-bashare','B端ba其他') then 'BA引导'
         when t2.qd in ('B端云店-product','B端云店-topic','企微-每日素材','企微-tmp推送','企微-企微任务','B企微端其他') then 'BA自主访问'
         when t2.qd in ('C端-自摸','C端-小程序矩阵（除mystore&7screens)','C端-外部投放','C端-自有线下资源','C端-订阅消息','C端-公众号','C端-其他','C端-空值渠道') then 'C端'
else null end as type,
    t2.sku_id,
    t2.sku_name,
    t2.is_ob,
    t2.is_eb,
    count(distinct t2.user_union_id) '下单人数',
    count(distinct t2.order_id) '订单数',
    sum(t2.order_amt) 'GMV'
from
(
select
    case when (t1.channel_level_1='qiwei' and t1.channel_level_2='individualcard') then '企微-个人卡片'
    when (t1.channel_level_1='qiwei' and t1.channel_level_2='tmp') then '企微-tmp推送'
    when (t1.channel_level_1='qiwei' and t1.channel_level_2='coupon') then '企微-企微领券'
    when (t1.channel_level_1='qiwei' and t1.channel_level_2='hyy')  then '企微-欢迎语'
    when (t1.channel_level_1='qiwei' and t1.channel_level_2='qwrw') then '企微-企微任务'
    when (t1.channel_level_1='qiwei' and t1.channel_level_2='content' and t1.channel_level_3='mrsc-friend') then '企微-每日素材'
    when (t1.channel_level_1 = 'qiwei' and t1.channel_level_2 in ('shequn','','chance','pyq','zimo','grass-seeding','social','banner','article','zbzb','mrsc','bashare','epwlive','qd','ai','mdzyw','mystore','vipcsq')) then 'B企微端其他'
    when (t1.channel_level_1='ba' and t1.channel_level_2='bashare' and t1.channel_level_3='bashare') then 'B端云店-bashare'
    when (t1.channel_level_1='ba' and t1.channel_level_2='bashare' and t1.channel_level_3='product') then 'B端云店-product'
    when (t1.channel_level_1='ba' and t1.channel_level_2='bashare' and t1.channel_level_3='order')  then 'B端云店-order'
    when (t1.channel_level_1='ba' and t1.channel_level_2='bashare' and t1.channel_level_3='topic') then 'B端云店-topic'
    when (t1.channel_level_1='ba' and t1.channel_level_2='bashare' and t1.channel_level_3='post') then 'B端云店-post'
    when (t1.channel_level_1 = 'ba' and t1.channel_level_2 = 'bashare' and t1.channel_level_3 in ('cart','brand','content','fenxiao','discovery')) then 'B端ba其他'
    when (t1.channel_level_1 = 'zimo' or t1.channel_level_1 is null or t1.channel_level_1 = '') then 'C端-自摸'
    when (t1.channel_level_1 = 'miniprogram' and t1.channel_level_2 not in ('mystore','7screens')) then 'C端-小程序矩阵（除mystore&7screens)'
    when (t1.channel_level_1 = 'wbtf' )  then 'C端-外部投放'
    when (t1.channel_level_1 = 'offline') then 'C端-自有线下资源'
    when (t1.channel_level_1 = 'subscribe') then 'C端-订阅消息'
    when (t1.channel_level_1 in ('qcsfwzs','qcsgfzcj','qcsdyh','qcsgfdyh','qcsfls')) then 'C端-公众号'
    when (t1.channel_level_2 not in ('mystore','7screens')) then 'C端-其他'
else 'C端-空值渠道' end as qd,
    t1.user_union_id as user_union_id,
    t1.order_id as order_id,
    t1.sku_id as sku_id,
    t1.sku_name as sku_name,
    t1.is_eb as is_eb,
    t1.is_ob as is_ob,
    t1.order_amt as order_amt
from
(
select
    aa.channel_level_1 as channel_level_1,
    aa.channel_level_2 as channel_level_2,
    aa.channel_level_3 as channel_level_3,
    aa.order_id as order_id,
    bb.sku_name as sku_name,
    bb.sku_id as sku_id,
    bb.is_eb as is_eb,
    bb.is_ob as is_ob,
    aa.user_union_id as user_union_id,
    aa.order_amt as order_amt
from
`01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    is_ob,
    is_eb,
    category_b_level_1_id
from `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id,is_ob,is_eb
) bb
on aa.sku_id=bb.sku_id
where aa.ds>='2023-03-30' and aa.ds<='2023-04-26' and aa.app_env='云店'
and aa.type='PayOrderDetailServer' and bb.category_b_level_1_id !='99'
group by aa.channel_level_1,aa.channel_level_2,aa.channel_level_3,
aa.order_id,bb.sku_name,bb.sku_id,bb.is_ob,bb.is_eb,aa.user_union_id,aa.order_amt
) t1
) t2
group by type,t2.sku_id,t2.sku_name,t2.is_ob,t2.is_eb
order by type,sum(t2.order_amt) desc

---- 测算一下obeb的gmv

select
    sum(case when bb.is_eb = 'Y' then aa.order_amt end) ebgmv,
    sum(case when bb.is_ob = 'Y' then aa.order_amt end) obgmv 
    -- bb.sku_name as sku_name,
    -- bb.sku_id as sku_id,
    -- bb.is_eb as is_eb,
    -- bb.is_ob as is_ob,
    -- aa.user_union_id as user_union_id,
    -- aa.order_amt as order_amt
from
`01_dwb_wdc_events_v2` aa
LEFT JOIN
(select
    sku_id,
    sku_name,
    is_ob,
    is_eb,
    category_b_level_1_id
from `01_dim_product_v1`
group by sku_id,sku_name,category_b_level_1_id,is_ob,is_eb
) bb
on aa.sku_id=bb.sku_id
where aa.ds>='2023-03-30' and aa.ds<='2023-04-26' and aa.app_env='云店'
and aa.type='PayOrderDetailServer' and bb.category_b_level_1_id !='99'