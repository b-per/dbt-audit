

select
    p_partkey as part_id,
        P_NAME as name,
        P_TYPE as type,
        P_SIZE as size,
        P_MFGR as manufacturer,
        P_BRAND as brand,
        p_comment as comment,
        p_container as container,
    p_retailprice as retail_price
from {{ source('tpch', 'part') }}
