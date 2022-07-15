create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.S5_LocMaster` as
--Total Company
Select  TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('totalcompany', 'totalcompany'))))) as member_id,
  'Total_BC' as member_name,
  'Total_BC' as member_desc,
  'totalcompany' as LOC_LEVEL
union distinct
  --Channel
Select  TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('channel', 'totalcompany'))))) as member_id,
  'channel' as member_name,
  'channel' as member_desc,
  'channel' as LOC_LEVEL
union distinct
--Catalog
SELECT
MEMBER_ID,
MEMBER_NAME,
MEMBER_DESC, 
LOC_LEVEL
 FROM `backcountry-data-team.{{ params.final_dataset_id }}.s5_catalog`
union distinct
--Selling Channel
select 
MEMBER_ID,
MEMBER_NAME,
MEMBER_DESC, 
LOC_LEVEL
  from `backcountry-data-team.{{ params.final_dataset_id }}.s5_selling_channel`
union distinct
--Area
select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('area', sc.member_name))))) as MEMBER_ID,
    sc.member_name as MEMBER_NAME,
    sc.member_desc as MEMBER_DESC,
    'area'  as LOC_LEVEL
  from `backcountry-data-team.{{ params.final_dataset_id }}.s5_selling_channel` sc
union distinct
--Region
select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('region', sc.member_name))))) as MEMBER_ID,
    sc.member_name as MEMBER_NAME,
    sc.member_desc as MEMBER_DESC,
    'region'  as LOC_LEVEL
  from `backcountry-data-team.{{ params.final_dataset_id }}.s5_selling_channel` sc
union distinct
--District
select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('district', sc.member_name))))) as MEMBER_ID,
    sc.member_name as MEMBER_NAME,
    sc.member_desc as MEMBER_DESC,
    'district'  as LOC_LEVEL
  from `backcountry-data-team.{{ params.final_dataset_id }}.s5_selling_channel` sc
union distinct
--Account
select 
    TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('account', sc.member_name))))) as MEMBER_ID,
    sc.member_name as MEMBER_NAME,
    sc.member_desc as MEMBER_DESC,
    'account'  as LOC_LEVEL
  from `backcountry-data-team.{{ params.final_dataset_id }}.s5_selling_channel` sc
union distinct
--Location
select 
MEMBER_ID,
MEMBER_NAME,
MEMBER_DESC, 
LOC_LEVEL
  from `backcountry-data-team.{{ params.final_dataset_id }}.s5_location`
where loc_level<>'DC_VDC';