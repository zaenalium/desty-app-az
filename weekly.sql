with add_date_period as (

    select 
        --quarter
        date_trunc(date_sub(today, INTERVAL 0 quarter),quarter) as current_quarter,
        if(date_trunc(date_sub(today, INTERVAL 1 quarter),quarter) > '2022-03-18',date_trunc(date_sub(today, INTERVAL 1 quarter),quarter), DATE('2022-03-18')) as last_quarter,
        -- month
        date_trunc(date_sub(today, INTERVAL 0 month),month) as current_month,
        if(date_trunc(date_sub(today, INTERVAL 1 month),month) > '2022-03-18',date_trunc(date_sub(today, INTERVAL 1 month),month), DATE('2022-03-18')) as last_month,
        -- week
        date_trunc(date_sub(today, INTERVAL 0 week),week(MONDAY)) current_week,
        date_trunc(date_sub(today, INTERVAL 1 week),week(MONDAY)) last_week,

    from (select current_date("Asia/Jakarta") as today)
)

,transaction_data as (

    SELECT 
        if(date_trunc(DATE(date),quarter) < '2022-03-18', '2022-03-18',date_trunc(DATE(date),quarter)) as quarter,
        if(date_trunc(DATE(date),month) < '2022-03-18', '2022-03-18',date_trunc(DATE(date),month)) as month,
        date_trunc(DATE(date),week(MONDAY)) as week,
        all_processed_gmv,
        all_transaction,
        no_of_sku,
        store_customer
    FROM `desty-data.desty_analytics_ops_omni.ops_do_daily_metrics`
    where date >= date_trunc(date_sub(current_date("Asia/Jakarta"), INTERVAL 1 quarter),quarter)

)

, tenant_data as (
    SELECT  
      tenant_id
      , tenant_name
      , shop_rel_id
      , order_id
      , order_create_time
      , total_price

      , if(date_trunc(DATE(order_create_time),quarter) < '2022-03-18', '2022-03-18',date_trunc(DATE(order_create_time),quarter)) as quarter
      , if(date_trunc(DATE(order_create_time),month) < '2022-03-18', '2022-03-18',date_trunc(DATE(order_create_time),month)) as month
      , date_trunc(DATE(order_create_time),week(MONDAY)) as week
    FROM `desty-data.desty_analytics_omni.mart_do_transaction`
    where DATE(order_create_time)>= date_trunc(date_sub(current_date("Asia/Jakarta"), INTERVAL 1 quarter),quarter)
)

,aggregation as (
    -- weekly
    select 
        'weekly' as period,
        case when curr.current_week is not null then 'current_week'
             when last.last_week is not null then 'last_week'
             else null end as period_label,
        week as date,
        -- transactions
        if(current_date() > last_day(week, week(monday)), date_diff(last_day(week, week(monday))+1, week, DAY),date_diff(current_date(), week, DAY)) as no_of_days,
        sum(all_transaction) as all_no_of_orders,
        sum(all_processed_gmv) as all_processed_gmv,
        sum(all_processed_gmv) / sum(all_transaction) as all_processed_aov,
        sum(no_of_sku) as no_of_sku,
        sum(store_customer) as no_of_customer

    from transaction_data
    left join add_date_period curr on transaction_data.week = curr.current_week
    left join add_date_period last on transaction_data.week = last.last_week
    where (curr.current_week is not null or last.last_week is not null)
    group by 1,2,3,4

    union all
    
    -- monthly
        select 
        'monthly' as period,
        case when curr.current_month is not null then 'current_month'
             when last.last_month is not null then 'last_month'
             else null end as period_label,
        month as date,
        -- transactions
        if(current_date() > last_day(month, MONTH), date_diff(last_day(month, MONTH)+1, month, DAY),date_diff(current_date(), month, DAY)) as no_of_days,
        sum(all_transaction) as all_no_of_orders,
        sum(all_processed_gmv) as all_processed_gmv,
        sum(all_processed_gmv) / sum(all_transaction) as all_processed_aov,
        sum(no_of_sku) as no_of_sku,
        sum(store_customer) as no_of_customer

    from transaction_data
    left join add_date_period curr on transaction_data.month = curr.current_month
    left join add_date_period last on transaction_data.month = last.last_month
    where (curr.current_month is not null or last.last_month is not null)
    group by 1,2,3,4

    union all 

    -- quarterly
        select 
        'quarterly' as period,
        case when curr.current_quarter is not null then 'current_quarter'
             when last.last_quarter is not null then 'last_quarter'
             else null end as period_label,
        -- transactions
        quarter as date,
        if(current_date() > last_day(quarter, quarter), date_diff(last_day(quarter, quarter)+1, quarter, DAY),date_diff(current_date(), quarter, DAY)) as no_of_days,
        sum(all_transaction) as all_no_of_orders,
        sum(all_processed_gmv) as all_processed_gmv,
        sum(all_processed_gmv) / sum(all_transaction) as all_processed_aov,
        sum(no_of_sku) as no_of_sku,
        sum(store_customer) as no_of_customer

    from transaction_data
    left join add_date_period curr on transaction_data.quarter = curr.current_quarter
    left join add_date_period last on transaction_data.quarter = last.last_quarter
    where (curr.current_quarter is not null or last.last_quarter is not null)
    group by 1,2,3,4

)

, merchant_aov as (

    -- weekly
    select 
        'weekly' as period,
        case when curr.current_week is not null then 'current_week'
             when last.last_week is not null then 'last_week'
             else null end as period_label,
        week as date,
        -- transactions
        COUNT(order_id) / COUNT(DISTINCT tenant_id) as aov_merchant

    from tenant_data
    left join add_date_period curr on tenant_data.week = curr.current_week
    left join add_date_period last on tenant_data.week = last.last_week
    where (curr.current_week is not null or last.last_week is not null)
    group by 1,2,3

    union all
    
    -- monthly
        select 
        'monthly' as period,
        case when curr.current_month is not null then 'current_month'
             when last.last_month is not null then 'last_month'
             else null end as period_label,
        month as date,
        -- transactions
        COUNT(order_id) / COUNT(DISTINCT tenant_id) as aov_merchant

    from tenant_data
    left join add_date_period curr on tenant_data.month = curr.current_month
    left join add_date_period last on tenant_data.month = last.last_month
    where (curr.current_month is not null or last.last_month is not null)
    group by 1,2,3

    union all 

    -- quarterly
        select 
        'quarterly' as period,
        case when curr.current_quarter is not null then 'current_quarter'
             when last.last_quarter is not null then 'last_quarter'
             else null end as period_label,
        quarter as date,
        -- transactions
        COUNT(order_id) / COUNT(DISTINCT tenant_id) as aov_merchant

    from tenant_data
    left join add_date_period curr on tenant_data.quarter = curr.current_quarter
    left join add_date_period last on tenant_data.quarter = last.last_quarter
    where (curr.current_quarter is not null or last.last_quarter is not null)
    group by 1,2,3

)

,final_metrics as (

    select 
        agg.period,
        agg.period_label,
        agg.date,

        cast(all_no_of_orders as float64) as all_no_of_orders,
        cast(all_processed_gmv as float64) as all_processed_gmv,
        cast(all_processed_aov as float64) as all_processed_aov,
        cast(all_no_of_orders/( if(no_of_days=0,1,no_of_days) )  as float64) as avg_daily_orders,
        cast(all_processed_gmv/( if(no_of_days=0,1,no_of_days) )  as float64) as avg_daily_gmv,
        cast(no_of_sku as float64) as no_of_sku,
        cast(no_of_customer as float64) as no_of_customer,
        cast(aov_merchant as float64) as aov_merchant

    from aggregation agg
        left join merchant_aov using (period,period_label,date)
)


-- UNPIVOT METRICS
,unpivoting as (
    select
        *
    from final_metrics
    UNPIVOT (values FOR metrics IN (
        all_no_of_orders as '1 No of Orders',
        all_processed_gmv as '2 Processed GMV',
        all_processed_aov as '3 Processed AOV',
        avg_daily_orders as '4 Avg Daily Orders',
        avg_daily_gmv as '5 Avg Daily GMV',
        no_of_sku as '6 New SKU',
        no_of_customer as '7 New Customers',
        aov_merchant as '8 Avg Orders per Merchant'
    ))
)

,growth_calculation as (
    select 
        period,
        metrics,
        max(date) as date,
        SAFE_DIVIDE(sum(if(period_label='current_quarter',values,0)),sum(if(period_label='last_quarter',values,0)))-1 as values
    from unpivoting
    where period = 'quarterly'
    group by 1,2

    union all

    select 
        period,
        metrics,
        max(date) as date,
        SAFE_DIVIDE(sum(if(period_label='current_month',values,0)),sum(if(period_label='last_month',values,0)))-1 as values
    from unpivoting
    where period = 'monthly'
    group by 1,2

    union all

    select 
        period,
        metrics,
        max(date) as date,
        SAFE_DIVIDE(sum(if(period_label='current_week',values,0)),sum(if(period_label='last_week',values,0)))-1 as values
    from unpivoting
    where period = 'weekly'
    group by 1,2

)

select 
    period,
    'growth' as period_label,
    date,
    values,
    metrics
from growth_calculation

union all

select *
from unpivoting