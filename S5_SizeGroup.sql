create or replace table `backcountry-data-team.{{ params.final_dataset_id }}.S5_SizeGroup` as
select distinct size_group_name, axis_2 from `backcountry-data-team.{{ params.final_dataset_id }}.S5_SKUSizeGroup` where size_group_name is not null and axis_2 is not null;
