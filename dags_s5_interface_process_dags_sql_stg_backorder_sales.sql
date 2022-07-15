create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.stg_backordered_qty` as
select  i.order_number,
        i.sku,
        catalog,
        order_source,
        count(*) backordered_qty,
    from rootdb_public.inventory i
        inner join (select distinct transactions.transaction_extid,customers.customer_firstname as customer_firstname,
    customers.customer_lastname as customer_lastname ,customers.customer_email as customer_email,
    shipmodes.shipmode_full_name as shipmode_full_name,transactions.transaction_status as transaction_status,
    site_channels.website_storefront_rank as website_storefront_rank,site_channels.website_name as catalog, transactions.order_source_name as order_source
    from elcap.vi_nested_sb_customer_transactions
        where lower(transactions.transaction_status) not in ('billed', 'closed', 'pending billing')
        and sb_event_date >= '2016-10-16'
        and transactions.transaction_event_type = 'demand')
            o on o.transaction_extid = i.order_number
        left join elcap.sb_products dp on dp.sku = i.sku
    where i.status = 'backordered' 
    and i._fivetran_deleted= false
    group by i.order_number,
        i.sku,
        transaction_status,
        catalog,
        order_source;

create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.stg_demand_sales` as
select distinct transactions.transaction_extid as order_number
    , n_sb_transactions_d.sku
    , fds.catalogs.catalog_name
    , sum(n_sb_transactions_d.quantity) as demand_quantity
    , sum(n_sb_transactions_d.amount) demand_sales
    , min(fds.sb_event_date) as orderline_date
from elcap.vi_nested_sb_customer_transactions fds
      left join unnest(details) as n_sb_transactions_d
      where exists (select  1 from `backcountry-data-team.{{ params.final_dataset_id }}.stg_backordered_qty` i where fds.transactions.transaction_extid = i.order_number 
      and n_sb_transactions_d.sku= i.sku)
      and fds.transactions.transaction_event_type = 'demand'
      and fds.sb_event_date >= '2016-10-16'
group by fds.transactions.transaction_extid
    , n_sb_transactions_d.sku
    , fds.employees.employee_extid
    , fds.catalogs.catalog_name
having sum(n_sb_transactions_d.quantity) <> 0;

Create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.stg_backorder_sales` as
select i.catalog,
        i.order_source,
        i.order_number,
        i.sku,
        DATE(bo.orderline_date, 'America/Denver') as orderline_date, --demand_order_date
        bo.demand_quantity as demand_quantity,
        bo.demand_sales as item_demand_sales,
        i.backordered_qty PENDING_BACKORDER_U , 
   ROUND(((bo.demand_sales/bo.demand_quantity)*i.backordered_qty), 2) PENDING_BACKORDER_V,
  (dim_products.wholesale_price*i.backordered_qty) PENDING_BACKORDER_C
from `backcountry-data-team.{{ params.final_dataset_id }}.stg_backordered_qty` i
left join `backcountry-data-team.{{ params.final_dataset_id }}.stg_demand_sales` bo on i.order_number = bo.order_number and i.sku = bo.sku
left join (select distinct transaction_extid from elcap.vi_sb_customer_transactions_daily
where transaction_event_date >= '2016-10-16'
and transaction_event_type = 'demand') dfs 
on dfs.transaction_extid = i.order_number
left join `backcountry-data-team.elcap.sb_products` dim_products on i.sku = dim_products.sku
where bo.demand_quantity <> 0 and dim_products.is_exclude_item_for_s5 is false;