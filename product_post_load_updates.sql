MERGE INTO `elcap.sb_products` PRD
USING (select sku FROM `backcountry-data-team.elcap.sb_products` PRODUCTS
LEFT OUTER JOIN `backcountry-data-team.netsuite2.brand` BR
ON cast(PRODUCTS.BRANDID as string)=BR.brand_extid
WHERE COALESCE(UPPER(BR.BRAND_NAME), 'X') NOT LIKE '%DO NOT USE%' and COALESCE(BR.prefix, 'X')<>'ZZZ'
AND item_type in ('inventory item','item group', '')
and products.sku not in ('#', '##')
and coalesce(UPPER(SKUDESC), 'x') not in ('NA', 'UNKNOWN')
and is_gso is false
and industry<>'Service & Labor'
and styleid is not null
and concat(styleid, '-',REGEXP_EXTRACT(products.sku,r'-\[?([^-]+)+-')) is not null
and cast(coalesce(netsuite_item_id, 0) as string) not like '-%') PRD1
ON PRD.sku=prd1.sku
WHEN MATCHED THEN UPDATE SET is_exclude_item_for_s5=false
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET is_exclude_item_for_s5=true;