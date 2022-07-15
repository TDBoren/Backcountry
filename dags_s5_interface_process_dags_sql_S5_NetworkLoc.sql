create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.S5_NetworkLoc` as
select b.member_id as LOCATION_ID, a.inv_loc_id as INV_LOC_ID, a.priority as PRIORITY
from `backcountry-data-team.{{ params.final_dataset_id }}.S5_NetworkZip` a, `backcountry-data-team.{{ params.final_dataset_id }}.s5_location` b
where a.zip_code=substr(b.zipcode, 0, 3) and b.DC_PO_TYPE_LOCATION='Retail' and b.zipcode is not null;