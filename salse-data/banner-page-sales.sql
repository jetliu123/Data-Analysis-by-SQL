select
    a.date as "日期",
    '合计' as "BC划分",
    a.activity_id as "活动ID",
    a.activity_name as "活动名称",
    count(distinct a.union_id) as "加购人数",
    count(a.union_id) as "加购次数",
    count(distinct b.union_id) as "购买人数",
    count(distinct order_id) as "订单数",
    sum(commodity_quantity) as "商品数量",
    sum(order_actual_amount) as "销售额"
from
    (select 
         -- 【新增修改】同商品、同页面、同资源位加购多次取首次
         min(date) as "date",min(time) as "time",union_id,commodity_item_code,commodity_name,commodity_id,
         -- 【新增修改】修改非topic页面activity_id获取逻辑
        if(
          page_name in ('topic','pages/packageA/oneSixEight/index'),
          if(on_topic_page_id = '' or on_topic_page_id is null,activity_id,on_topic_page_id),
          if(
            page_name = 'commodity_detail' and last_page in ('topic','pages/packageA/oneSixEight/index'),
            if(from_topic_page_id = '' or from_topic_page_id is null,from_activity_id,from_topic_page_id),
            if( 
              page_name in ('homepage','mall','oversea','webview'),
              page_name,
              last_page
              )
            )
          )as activity_id
          ,if(
          page_name in ('topic','pages/packageA/oneSixEight/index'),
          if(on_topic_page_name = '' or on_topic_page_name is null,activity_name,on_topic_page_name),
          if(
            page_name = 'commodity_detail' and last_page in ('topic','pages/packageA/oneSixEight/index'),
            if(from_topic_page_name = '' or from_topic_page_name is null,from_activity_name,from_topic_page_name),
            if( 
              page_name in ('homepage','mall','oversea','webview'),
              page_name,
              last_page
              )
            )
          )as activity_name
          ,if(page_name in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index'),mkt_type,from_mkt_type) as mkt_type
          ,if(page_name in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index'),mkt_name,from_mkt_name) as mkt_name
    from events
    where mp_name = '云店'
        and date>='2023-03-02' 
        and date<='2023-03-28'
        and event = 'add_shoppingcart'
        and 
            ((page_name in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index')
            )
            or
            (page_name = 'commodity_detail'
            and last_page in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index')
            ))
      group by union_id,commodity_item_code,commodity_name,commodity_id,activity_id,activity_name,mkt_type,mkt_name
        ) a
left join
    (select 
        min(date) as "date",min(time) as "time",union_id,commodity_item_code,order_actual_amount,order_id,commodity_quantity
    from events
    where mp_name = '云店'
        and date>='2023-03-02' 
        and date<='2023-03-28'
        and event = 'PayOrderDetailServer'
    group by union_id,commodity_item_code,order_actual_amount,order_id,commodity_quantity) b
    on a.union_id = b.union_id
        and a.commodity_item_code = b.commodity_item_code
        and a.date = b.date -- 【新增修改】限制加购行为发生后当天支付
        and a.time<b.time
group by 
    a.date,
    a.activity_id,
    a.activity_name
order by
    a.activity_id,
    a.date
    
    
union all  


select
    a.date as "日期",
    a.ba_type as "BC划分",
    a.activity_id as "活动ID",
    a.activity_name as "活动名称",
    count(distinct a.union_id) as "加购人数",
    count(a.union_id) as "加购次数",
    count(distinct b.union_id) as "购买人数",
    count(distinct order_id) as "订单数",
    sum(commodity_quantity) as "商品数量",
    sum(order_actual_amount) as "销售额"
from
    (select 
         -- 【新增修改】同商品、同页面、同资源位加购多次取首次
         min(date) as "date",min(time) as "time",union_id,ba_type,commodity_item_code,commodity_name,commodity_id,
         -- 【新增修改】修改非topic页面activity_id获取逻辑
        if(
          page_name in ('topic','pages/packageA/oneSixEight/index'),
          if(on_topic_page_id = '' or on_topic_page_id is null,activity_id,on_topic_page_id),
          if(
            page_name = 'commodity_detail' and last_page in ('topic','pages/packageA/oneSixEight/index'),
            if(from_topic_page_id = '' or from_topic_page_id is null,from_activity_id,from_topic_page_id),
            if( 
              page_name in ('homepage','mall','oversea','webview'),
              page_name,
              last_page
              )
            )
          )as activity_id
          ,if(
          page_name in ('topic','pages/packageA/oneSixEight/index'),
          if(on_topic_page_name = '' or on_topic_page_name is null,activity_name,on_topic_page_name),
          if(
            page_name = 'commodity_detail' and last_page in ('topic','pages/packageA/oneSixEight/index'),
            if(from_topic_page_name = '' or from_topic_page_name is null,from_activity_name,from_topic_page_name),
            if( 
              page_name in ('homepage','mall','oversea','webview'),
              page_name,
              last_page
              )
            )
          )as activity_name
          ,if(page_name in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index'),mkt_type,from_mkt_type) as mkt_type
          ,if(page_name in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index'),mkt_name,from_mkt_name) as mkt_name
    from events
    where mp_name = '云店'
        and date>='2023-03-02' 
        and date<='2023-03-28'
        and event = 'add_shoppingcart'
        and 
            ((page_name in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index')
            )
            or
            (page_name = 'commodity_detail'
            and last_page in ('homepage','mall','topic','oversea','webview','pages/packageA/oneSixEight/index')
            ))
      group by union_id,ba_type,commodity_item_code,commodity_name,commodity_id,activity_id,activity_name,mkt_type,mkt_name
        ) a
left join
    (select 
        min(date) as "date",min(time) as "time",union_id,ba_type,commodity_item_code,order_actual_amount,order_id,commodity_quantity
    from events
    where mp_name = '云店'
        and date>='2023-03-02' 
        and date<='2023-03-28'
        and event = 'PayOrderDetailServer'
    group by union_id,ba_type,commodity_item_code,order_actual_amount,order_id,commodity_quantity) b
    on a.union_id = b.union_id
        and a.commodity_item_code = b.commodity_item_code
        and a.date = b.date -- 【新增修改】限制加购行为发生后当天支付
        and a.ba_type = b.ba_type -- 【新增修改】区分BC端
        and a.time<b.time
group by 
    a.date,
    a.ba_type,
    a.activity_id,
    a.activity_name
order by
    a.activity_id,
    a.ba_type,
    a.date;
    