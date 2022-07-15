create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.S5_SKUSizeGroup` as
select distinct sku, coalesce(brd.AXIS2, mc.AXIS2, axs.AXIS2) as axis_2, concat(productgroupdesc, ' ', coalesce(brd.group_name, mc.group_name, axs.group_name)) as size_group_name
from `backcountry-data-team.elcap.sb_products` prd
left join (SELECT AXIS2, BRAND, GROUP_NAME FROM (
SELECT AXIS2, OPTIONAL_BRAND AS BRAND, GROUP_NAME, CASE WHEN OPTIONAL_DISABLE IS NULL THEN 'FALSE' ELSE 'TRUE' END as DISABLE, 
ROW_NUMBER() OVER (PARTITION BY AXIS2, OPTIONAL_BRAND ORDER BY OPTIONAL_BRAND) AS ROW_NUM FROM `backcountry-data-team.{{ params.final_dataset_id }}.S5_Sizegroup_Stg`
) WHERE DISABLE = 'FALSE' AND ROW_NUM=1) brd on prd.axis2value=brd.axis2 and prd.brandname=brd.brand
left join (SELECT AXIS2, MC, GROUP_NAME FROM (
SELECT AXIS2, OPTIONAL_MC AS MC, GROUP_NAME, CASE WHEN OPTIONAL_DISABLE IS NULL THEN 'FALSE' ELSE 'TRUE' END as DISABLE, 
ROW_NUMBER() OVER (PARTITION BY AXIS2, OPTIONAL_MC ORDER BY OPTIONAL_BRAND) AS ROW_NUM FROM `backcountry-data-team.{{ params.final_dataset_id }}.S5_Sizegroup_Stg`
) WHERE DISABLE = 'FALSE' AND ROW_NUM=1) mc on prd.axis2value=mc.axis2 and prd.merchandise_class=mc.MC
left join (SELECT AXIS2, GROUP_NAME FROM (
SELECT AXIS2, GROUP_NAME, CASE WHEN OPTIONAL_DISABLE IS NULL THEN 'FALSE' ELSE 'TRUE' END as DISABLE, 
ROW_NUMBER() OVER (PARTITION BY AXIS2 ORDER BY OPTIONAL_BRAND) AS ROW_NUM FROM `backcountry-data-team.{{ params.final_dataset_id }}.S5_Sizegroup_Stg`
) WHERE DISABLE = 'FALSE' AND ROW_NUM=1) axs on prd.axis2value=axs.axis2
where prd.is_exclude_item_for_s5 is false;