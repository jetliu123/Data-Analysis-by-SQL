-- 访问时长大于5分钟的下单情况，各找5个人
--- 找出访问时长高于300s的人
select 
  t1.user_union_id as payunion_id,
  t1.session_id as psession_id,
  t1.qd as qd,
  unix_timestamp(t1.ptime) as `tp`,
  t2.user_union_id as vunion_id,
  t2.session_id as vsession_id,
  t2.qd as qdd,
  unix_timestamp(t2.vtime) as `tv`,
  unix_timestamp(t1.ptime)-unix_timestamp(t2.vtime) as gap
from 
(
select 
    aa.user_union_id as user_union_id,
    aa.session_id as session_id,
    if(aa.channel_level_1 in ('qiwei','ba') or aa.ba_id is not null,'b端','c端') qd,
    min(aa.time) ptime 
from 
01_dwb_wdc_events_v2 aa 
left join 
(
  select
    sku_id,
    sku_name,
    category_b_level_1_id
from `01_dim_product_v1` 
group by sku_id,sku_name,category_b_level_1_id) bb
on aa.sku_id=bb.sku_id
where aa.ds>='2023-04-03' and aa.ds< '2023-04-04' and aa.app_env='云店' 
and aa.type='PayOrderDetailServer' and bb.category_b_level_1_id !='99' 
and aa.user_union_id is not null
group by aa.user_union_id,aa.session_id,qd
) t1 
left join 
(
select 
    user_union_id,
    session_id,
    if(channel_level_1 in ('qiwei','ba') or ba_id is not null,'b端','c端') qd,
    min(time) vtime
from 
01_dwb_wdc_events_v2 
where type = 'load' and app_env='云店' and ds >= '2023-04-03' and ds < '2023-04-04'
and user_union_id is not null
group by 
user_union_id,session_id,qd
) t2 
on t1.user_union_id = t2.user_union_id and t1.session_id = t2.session_id and t1.qd = t2.qd
where unix_timestamp(t1.ptime)-unix_timestamp(t2.vtime) < 120;



-- 访问 / 下单
select 
    aa.user_union_id as user_union_id,
    aa.session_id as session_id,
    aa.time,
    aa.channel_level_1,aa.channel_level_2,aa.channel_level_3,
    aa.cur_page,aa.sku_name
from 
01_dwb_wdc_events_v2 aa 
where aa.ds>='2023-04-03' and aa.ds< '2023-04-04' and aa.app_env='云店' 
and aa.type = 'load'
-- aa.type='PayOrderDetailServer' 
and aa.user_union_id 
in (
'oWHdYwWlDmshkzWSdoVUkw3KS4V0',
'oWHdYwfKzRBBQOL0Y0EujgrGobDY',
'oWHdYwde_eecRhCVb6Fq2uF8nymk',
'oWHdYwUknvG3hMfj1kQvtfvImO-A',
'oWHdYwfTQ2Ws7Giryr7NYPafAQ7I')
and aa.session_id in (
'651c951b-4ddf-4536-ecb8-bdfcfeeb31dd',
'fed70d81-5e95-42ac-8e75-75f4ccf46c1e',
'df983d3f-cbfe-4d56-b369-f2400d6152dc',
'd97860da-4124-4ae9-eeb0-9c2121c68b14',
'2de41390-cb92-479e-c21e-58dbb5df48a7')
order by aa.time,aa.user_union_id,aa.session_id


---- 各个渠道的UV


select
    case when (aa.channel_level_1='qiwei' and aa.channel_level_2='individualcard') then '企微-个人卡片'
  when (aa.channel_level_1='qiwei' and aa.channel_level_2='tmp') then '企微-tmp推送'
  when (aa.channel_level_1='qiwei' and aa.channel_level_2='coupon') then '企微-企微领券'
  when (aa.channel_level_1='qiwei' and aa.channel_level_2='hyy')  then '企微-欢迎语'
  when (aa.channel_level_1='qiwei' and aa.channel_level_2='qwrw') then '企微-企微任务'
  when (aa.channel_level_1='qiwei' and aa.channel_level_2='content' and aa.channel_level_3='mrsc-friend') then '企微-每日素材'
  when (aa.channel_level_1 = 'qiwei' and aa.channel_level_2 in ('shequn','article','','aa.chance','pyq','zimo','grass-seeding','social',
'banner','article','zbzb','mrsc','bashare','epwlive','qd','ai','mdzyw','mystore')) then 'B企微端其他'
------
  when (aa.channel_level_1='ba' and aa.channel_level_2='bashare' and aa.channel_level_3='bashare') then 'B端云店-bashare'
  when (aa.channel_level_1='ba' and aa.channel_level_2='bashare' and aa.channel_level_3='product') then 'B端云店-product'
  when (aa.channel_level_1='ba' and aa.channel_level_2='bashare' and aa.channel_level_3='order')  then 'B端云店-order'
  when (aa.channel_level_1='ba' and aa.channel_level_2='bashare' and aa.channel_level_3='topic') then 'B端云店-topic'
  when (aa.channel_level_1='ba' and aa.channel_level_2='bashare' and aa.channel_level_3='post') then 'B端云店-post'
  when (aa.channel_level_1 = 'ba' and aa.channel_level_2 = 'bashare' and aa.channel_level_3 in ('cart','brand','content','fenxiao','discovery')) then 'B端ba其他' 
  when (aa.channel_level_1 = 'zimo' or aa.channel_level_1 is null or aa.channel_level_1 = '') then 'C端-自摸'
  when (aa.channel_level_1 = 'miniprogram' and aa.channel_level_2 not in ('mystore','7screens')) then 'C端-小程序矩阵（除mystore&7screens)'
  when (aa.channel_level_1 = 'wbtf' )  then 'C端-外部投放'
  when (aa.channel_level_1 = 'offline') then 'C端-自有线下资源'
  when (aa.channel_level_1 = 'subscribe') then 'C端-订阅消息'
  when (aa.channel_level_1 in ('qcsfwzs','qcsgfzcj','qcsdyh','qcsgfdyh','qcsfls')) then 'C端-公众号'
  when (aa.channel_level_2 not in ('mystore','7screens')) then 'C端-其他'
  else 'C端-空值渠道' end as qd,
    count(distinct aa.user_union_id) 
from 01_dwb_wdc_events_v2 aa
where aa.ds>='2023-04-03' and aa.ds< '2023-04-04' and aa.app_env='云店' 
and aa.type='load'
group by qd;

