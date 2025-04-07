{{
config(
materialized='incremental',
unique_key='PK_ODS_ES_EQAI_TRANSPORTER_ID',
merge_no_update_columns = ['SYS_CREATE_DTM'],
tags=["ods","es-eqai","scheduled-nightly"]
)
}}
-- Read the data from staging table as incrementally
with CTE_source as (
select
*
from {{ source('STAGING', 'STG_ES_EQAI_TRANSPORTER') }}
{% if is_incremental() %}
where SF_INSERT_TIMESTAMP > '{{ get_max_event_time('SF_INSERT_TIMESTAMP',not_minus3=True) }}'
{% endif %}
)
-- Apply deduplication logic
, CTE_dedup as (
select *
from CTE_source
qualify row_number() over (partition by TRANSPORTER_CODE order by DATE_MODIFIED desc, SF_INSERT_TIMESTAMP desc) = 1
)

-- Create a batch id
, CTE_ins_batch_id as (
select TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as ins_batch_id
)
-- Create the final table
, CTE_final as (
select
{{ generate_surrogate_key(['TRANSPORTER_CODE']) }} as PK_ODS_ES_EQAI_TRANSPORTER_ID,
CURRENT_TIMESTAMP as SYS_CREATE_DTM,
CTE_ins_batch_id.ins_batch_id as SYS_EXEC_ID,
CURRENT_TIMESTAMP as SYS_LAST_UPDATE_DTM,
TRANSPORTER_CODE,
TRANSPORTERSTATUS as TRANSPORTER_STATUS,
TRANSPORTERNAME as TRANSPORTER_NAME,
TRANSPORTER_ADDR1,
TRANSPORTER_ADDR2,
TRANSPORTER_ADDR3,
TRANSPORTER_EPAID,
TRANSPORTER_PHONE,
TRANSPORTER_FAX,
TRANSPORTER_CONTACT,
TRANSPORTER_CONTACT_PHONE,
COMMENTS,
DOT_ID,
TRANSPORTER_CITY,
TRANSPORTER_STATE,
TRANSPORTER_ZIP_CODE,
TRANSPORTER_COUNTRY,
EQ_FLAG,
EQ_COMPANY,
EQ_PROFIT_CTR,
ADDED_BY,
DATE_ADDED,
MODIFIEDBY as MODIFIED_BY,
DATE_MODIFIED,
USE_FLAG,
SF_INSERT_TIMESTAMP
from CTE_dedup
join CTE_ins_batch_id on 1=1
)
select * from CTE_final