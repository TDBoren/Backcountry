--Catalog
create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.s5_catalog` as
SELECT
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('catalog', CATALOG_NAME))))) as MEMBER_ID,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('catalog', CATALOG_NAME))))) as CLIENT_ID,
CATALOG_EXTID as MEMBER_NAME,
CATALOG_NAME as MEMBER_DESC, 
'catalog' as LOC_LEVEL,
CAST(CATALOG_ID as STRING) as CATALOG_ID
 FROM `backcountry-data-team.netsuite_sc.catalog`
 where _fivetran_deleted = false
 union distinct 
  select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('catalog', 'miscellaneous'))))) as MEMBER_ID,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('catalog', 'miscellaneous'))))) as CLIENT_ID,
    'miscellaneous' as MEMBER_NAME,
    'miscellaneous' as MEMBER_DESC,
    'catalog' as LOC_LEVEL,
    'miscellaneous' as CATALOG_ID;

--Selling Channel
create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.s5_selling_channel` as
select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('selling_channel', CONCAT(CAST(a.catalog_name AS STRING), CAST('-' AS STRING), 
    CAST(b.order_source_name AS STRING))))))) as MEMBER_ID,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('selling_channel', CONCAT(CAST(a.catalog_name AS STRING), CAST('-' AS STRING), 
    CAST(b.order_source_name AS STRING))))))) as CLIENT_ID,
    CONCAT(CAST(a.catalog_name AS STRING), CAST('-' AS STRING), 
    CAST(b.order_source_name AS STRING)) as MEMBER_NAME,
        CONCAT(CAST(a.catalog_name AS STRING), CAST('-' AS STRING), 
    CAST(b.order_source_name AS STRING)) as MEMBER_DESC,
    'selling_channel'  as LOC_LEVEL,
    CAST(A.CATALOG_ID as STRING) as CATALOG_ID
  from netsuite_sc.catalog a full join netsuite_sc.order_source b on 1=1
  where a._fivetran_deleted = false and b._fivetran_deleted = false
  union distinct 
  select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('selling_channel', 'miscellaneous'))))) as MEMBER_ID,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('selling_channel', 'miscellaneous'))))) as CLIENT_ID,
    'miscellaneous' as MEMBER_NAME,
    'miscellaneous' as MEMBER_DESC,
    'selling_channel' as LOC_LEVEL,
    'miscellaneous' as CATALOG_ID;

--Location
create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.s5_location` as
--retail stores
select DISTINCT TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', CASE WHEN a.catalog_name='Backcountry' THEN concat(a.catalog_name, '-', order_source_name, '-', b.name) ELSE concat(a.catalog_name, '-', order_source_name) END))))) as member_id,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', CASE WHEN a.catalog_name='Backcountry' THEN concat(a.catalog_name, '-', order_source_name, '-', b.name) ELSE concat(a.catalog_name, '-', order_source_name) END))))) as client_id,
CASE WHEN a.catalog_name='Backcountry' THEN concat(a.catalog_name, '-', order_source_name, '-', b.name) ELSE concat(a.catalog_name, '-', order_source_name) END as member_name, 
CASE WHEN a.catalog_name='Backcountry' THEN concat(a.catalog_name, '-', order_source_name, '-', b.name) ELSE concat(a.catalog_name, '-', order_source_name) END as member_desc,
'location'  as LOC_LEVEL,
concat(a.catalog_name, '-', order_source_name) as selling_channel,
a.catalog_extid as catalog_id,
a.catalog_name as catalog_name,
cast(a.order_source_id as string) as order_source_id,
a.order_source_name as order_source_name,
case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end as order_source_group,
concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end) as selling_channel_group,
CASE WHEN a.catalog_name='Backcountry' THEN concat(a.catalog_name, '-', order_source_name, '-', b.name) ELSE concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end) end as location_group,
CASE WHEN a.catalog_name='Backcountry' THEN b.name ELSE 'miscellaneous' END as merch_location_group, 
CASE WHEN a.catalog_name='Backcountry' THEN b.city else null end as city,
CASE WHEN a.catalog_name='Backcountry' THEN b.state else null end as state,
CASE WHEN a.catalog_name='Backcountry' THEN substr(b.zipcode, 0, 5) else null end as zipcode,
CASE WHEN a.catalog_name='Backcountry' THEN 'TRUE' else 'FALSE' end as ACTIVE_STATUS,
CASE WHEN a.catalog_name='Backcountry' THEN date(b.date_last_modified, 'America/Denver') else cast(null as date) end as STORE_OPEN_DATE,
cast(null as date) as STORE_CLOSE_DATE,
cast(null as string) as CLIMATE,
cast(null as string) as CORP_VOL_GROUP,
cast(null as string) as CAPACITY,
cast(null as string) as SQ_FOOTAGE,
cast(null as string) as LATITUDE,
cast(null as string) as LONGITUDE,
cast(null as string) as STORE_BANNER,
cast(null as string) as DROP_SHIP,
case 
      when location_extid = 'CVDC_Closeout' then 'Closeout'
      when location_extid = 'CVDC' then 'Unknown'
      when location_extid = 'SLCW_Retail' then 'Retail'
      when location_extid = 'SLCW_Closeout' then 'Closeout'
      when location_extid = 'CVDC_Retail' then 'Retail'
      when location_extid = 'SLCW' then 'Unknown'
      when location_extid = 'CVDC_InspectQuarantine' then 'Unknown'
      when location_extid = 'SLCW_InspectQuarantine' then 'Unknown'
      when location_extid in ('1302', '1002') then 'Retail'
      when location_extid in ('1304', '1003') then 'Retail'
      when location_extid in ('1301', '1001') then 'Retail'
     when location_extid = '10034' then 'Retail'
     when location_extid in ('0035', '1005') then 'Retail'
     when location_extid in ('10038', '1008', '1009') then 'Retail'
    else 'Unknown' end as DC_PO_TYPE_LOCATION,
    '0' as DC_FLAG,
    '0' as VDC_FLAG
from
(select catalog_id, catalog_extid, catalog_name, order_source_id, order_source_extid, order_source_name 
  from netsuite_sc.catalog a full join netsuite_sc.order_source b on 1=1
where a._fivetran_deleted = false and b._fivetran_deleted = false) a
  full join  netsuite_sc.locations b on 1=1 where order_source_name='Offline' and b.location_id in (23,25,30,33,35,41,43)
  and b._fivetran_deleted = false
  --***Stubbing this out since we have actual Backcountry retail stores. Need to check if there are any sales against Backcountry-Offline location & see if if it is ok to use miscellenious for those transactions as location**.
--   union distinct 
--   select DISTINCT TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', concat(a.catalog_name, '-', order_source_name)))))) as member_id,
-- TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', concat(a.catalog_name, '-', order_source_name)))))) as client_id,
-- concat(a.catalog_name, '-', order_source_name) as member_name, 
-- concat(a.catalog_name, '-', order_source_name) as member_desc,
-- 'location'  as LOC_LEVEL,
-- concat(a.catalog_name, '-', order_source_name) as selling_channel,
-- a.catalog_extid as catalog_id,
-- a.catalog_name as catalog_name,
-- cast(a.order_source_id as string) as order_source_id,
-- a.order_source_name as order_source_name,
-- case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
-- when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
-- else order_source_name end as order_source_group,
-- concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
-- when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
-- else order_source_name end) as selling_channel_group,
-- concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
-- when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
-- else order_source_name end) as location_group,
-- cast(null as string) as city,
-- cast(null as string) as state,
-- cast(null as string) as zipcode,
-- 'Retail' as DC_PO_TYPE_LOCATION,
--     '0' as DC_FLAG,
--     '0' as VDC_FLAG
-- from
-- (select catalog_id, catalog_extid, catalog_name, order_source_id, order_source_extid, order_source_name 
--   from netsuite_sc.catalog a full join netsuite_sc.order_source b on 1=1
-- where a._fivetran_deleted = false and b._fivetran_deleted = false) a  
-- where order_source_name='Offline' 
--   and a.catalog_name='Backcountry'
  union distinct 
  --Websites
  select 
  TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', concat(a.catalog_name, '-', b.order_source_name)))))) as member_id,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', concat(a.catalog_name, '-', b.order_source_name)))))) as client_id,
concat(a.catalog_name, '-', order_source_name) as member_name, 
concat(a.catalog_name, '-', order_source_name) as member_desc,
'location'  as LOC_LEVEL,
concat(a.catalog_name, '-', order_source_name) as selling_channel,
a.catalog_extid as catalog_id,
a.catalog_name as catalog_name,
cast(b.order_source_id as string) as order_source_id,
b.order_source_name as order_source_name,
case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end as order_source_group,
concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end) as selling_channel_group,
case when order_source_name='Offline' then concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end) 
else concat(a.catalog_name, '-', case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then 'Website'
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end) end as location_group,
case when order_source_name in ('GHO', 'Website', 'GHX', 'Mobile', 'Borderfree') then a.catalog_name
when order_source_name in ('Amazon', 'Amazon Prime', 'eBay') then 'Marketplace' 
else order_source_name end as MERCH_LOCATION_GROUP,
cast(null as string) as city,
cast(null as string) as state,
cast(null as string) as zipcode,
CASE WHEN b.order_source_name in ('eBay', 'Group Sales') THEN 'FALSE' ELSE 'TRUE' END as ACTIVE_STATUS,
cast(null as date) as STORE_OPEN_DATE,
cast(null as date) as STORE_CLOSE_DATE,
cast(null as string) as CLIMATE,
cast(null as string) as CORP_VOL_GROUP,
cast(null as string) as CAPACITY,
cast(null as string) as SQ_FOOTAGE,
cast(null as string) as LATITUDE,
cast(null as string) as LONGITUDE,
cast(null as string) as STORE_BANNER,
cast(null as string) as DROP_SHIP,
'Unknown' as DC_PO_TYPE_LOCATION,
'0' as DC_FLAG,
'0' as VDC_FLAG
  from netsuite_sc.catalog a full join netsuite_sc.order_source b on 1=1
  where order_source_name<>'Offline' and a._fivetran_deleted = false and b._fivetran_deleted = false
  union distinct 
--Distribution Centers
select distinct TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', b.name))))) as member_id,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', b.name))))) as client_id,
b.name as member_name, 
b.name as member_desc,
'DC_VDC'  as LOC_LEVEL,
cast(null as string) as selling_channel,
cast(null as string) as catalog_id,
cast(null as string) as catalog_name,
cast(null as string) as order_source_id,
cast(null as string) as order_source_name,
cast(null as string) as order_source_group,
cast(null as string) as selling_channel_group,
cast(null as string) as location_group,
cast(null as string) as MERCH_LOCATION_GROUP,
b.city,
b.state,
substr(b.zipcode, 0, 5) zipcode,
'FALSE' as ACTIVE_STATUS,
cast(null as date) as STORE_OPEN_DATE,
cast(null as date) as STORE_CLOSE_DATE,
cast(null as string) as CLIMATE,
cast(null as string) as CORP_VOL_GROUP,
cast(null as string) as CAPACITY,
cast(null as string) as SQ_FOOTAGE,
cast(null as string) as LATITUDE,
cast(null as string) as LONGITUDE,
cast(null as string) as STORE_BANNER,
cast(null as string) as DROP_SHIP,
    case 
      when b.location_extid = 'CVDC_Closeout' then 'Closeout'
      when b.location_extid = 'CVDC' then 'Retail or Closeout'
      when b.location_extid = 'SLCW_Retail' then 'Retail'
      when b.location_extid = 'SLCW_Closeout' then 'Closeout'
      when b.location_extid = 'CVDC_Retail' then 'Retail'
      when b.location_extid = 'SLCW' then 'Retail or Closeout'
      when b.location_extid = 'CVDC_InspectQuarantine' then 'Unknown'
      when b.location_extid = 'SLCW_InspectQuarantine' then 'Unknown'
      when b.location_extid in ('1302', '1002') then 'Retail'
      when b.location_extid in ('1304', '1003') then 'Retail'
      when b.location_extid in ('1301', '1001') then 'Retail'
     when b.location_extid = '10034' then 'Retail'
     when b.location_extid in ('0035', '1005') then 'Retail'
     when b.location_extid in ('10038', '1008', '1009') then 'Retail'
    else 'Unknown' end as DC_PO_TYPE_LOCATION,
    case 
      when b.location_extid = 'CVDC_Closeout' then '0'
      when b.location_extid = 'CVDC' then '1'
      when b.location_extid = 'SLCW_Retail' then '0'
      when b.location_extid = 'SLCW_Closeout' then '0'
      when b.location_extid = 'CVDC_Retail' then '0'
      when b.location_extid = 'SLCW' then '1'
      when b.location_extid = 'CVDC_InspectQuarantine' then '0'
      when b.location_extid = 'SLCW_InspectQuarantine' then '0'
      when b.location_extid in ('1302', '1002') then '0'
      when b.location_extid in ('1304', '1003') then '0'
      when b.location_extid in ('1301', '1001') then '0'
      when b.location_extid = '10034' then '0'
      when b.location_extid in ('0035', '1005') then '0'
      when b.location_extid in ('10038', '1008', '1009') then '0'
    else '0' end as DC_FLAG,
    case 
      when b.location_extid = 'CVDC_Closeout' then '1'
      when b.location_extid = 'CVDC' then '0'
      when b.location_extid = 'SLCW_Retail' then '1'
      when b.location_extid = 'SLCW_Closeout' then '1'
      when b.location_extid = 'CVDC_Retail' then '1'
      when b.location_extid = 'SLCW' then '0'
      when b.location_extid = 'CVDC_InspectQuarantine' then '0'
      when b.location_extid = 'SLCW_InspectQuarantine' then '0'
      when b.location_extid in ('1302', '1002') then '0'
      when b.location_extid in ('1304', '1003') then '0'
      when b.location_extid in ('1301', '1001') then '0'
      when b.location_extid = '10034' then '0'
      when b.location_extid in ('0035', '1005') then '0'
      when b.location_extid in ('10038', '1008', '1009') then '0'
    else '0' end as VDC_FLAG
from netsuite_sc.locations b
     where b.LOCATION_ID IN (2,7) and b._fivetran_deleted = false
     union distinct
--miscellaneous
select TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', 'miscellaneous'))))) as member_id,
TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', 'miscellaneous'))))) as client_id,
'miscellaneous' as member_name, 
'miscellaneous' as member_desc,
'location'  as LOC_LEVEL,
'miscellaneous' as selling_channel,
'miscellaneous' as catalog_id,
'miscellaneous' as catalog_name,
'miscellaneous' as order_source_id,
'miscellaneous' as order_source_name,
'miscellaneous' as order_source_group,
'miscellaneous' as selling_channel_group,
'miscellaneous' as location_group,
'miscellaneous' as MERCH_LOCATION_GROUP,
cast(null as string) as city,
cast(null as string) as state,
cast(null as string) as zipcode,
'FALSE' as ACTIVE_STATUS,
cast(null as date) STORE_OPEN_DATE,
cast(null as date) STORE_CLOSE_DATE,
cast(null as string) as CLIMATE,
cast(null as string) as CORP_VOL_GROUP,
cast(null as string) as CAPACITY,
cast(null as string) as SQ_FOOTAGE,
cast(null as string) as LATITUDE,
cast(null as string) as LONGITUDE,
cast(null as string) as STORE_BANNER,
cast(null as string) as DROP_SHIP,
'miscellaneous' as DC_PO_TYPE_LOCATION,
    '0' as DC_FLAG,
    '0' as VDC_FLAG;