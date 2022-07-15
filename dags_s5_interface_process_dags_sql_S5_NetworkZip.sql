CREATE OR REPLACE TABLE `backcountry-data-team.{{ params.final_dataset_id }}.S5_NetworkZip` as
SELECT a.ZIPCODE as ZIP_CODE, TO_BASE64(FROM_HEX(TO_HEX(MD5(concat('location', b.name))))) as INV_LOC_ID, a.priority as PRIORITY FROM (
select zipcode, optimal_facility as facility_code, 1 as priority from `backcountry-data-team.{{ params.final_dataset_id }}.S5_NetworkZip_Stg`
UNION DISTINCT
select zipcode, BACKUP as facility_code, 2 as priority from `backcountry-data-team.{{ params.final_dataset_id }}.S5_NetworkZip_Stg`
) a, `backcountry-data-team.netsuite_sc.locations` b where a.facility_code=b.location_extid and b._fivetran_deleted = false 
;