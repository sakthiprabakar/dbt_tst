{{
config(
materialized='incremental',
unique_key='PK_ODS_ES_EQAI_BILLING_DETAIL_ID',
merge_no_update_columns = ['SYS_CREATE_DTM'],
tags=["ods","es-eqai","scheduled-nightly"]
)
}}
-- Read the data from staging table as incrementally
with CTE_source as (
select
*
from {{ source('STAGING', 'STG_ES_EQAI_BILLING_DETAIL') }}
{% if is_incremental() %}
where SF_INSERT_TIMESTAMP > '{{ get_max_event_time('SF_INSERT_TIMESTAMP',not_minus3=True) }}'
{% endif %}
)
-- Apply deduplication logic
, CTE_dedup as (
select *
from CTE_source
qualify row_number() over (partition by BILLINGDETAIL_UID order by DATE_MODIFIED desc, SF_INSERT_TIMESTAMP desc) = 1
)

-- Create a batch id
, CTE_ins_batch_id as (
select TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as ins_batch_id
)
-- Create the final table
, CTE_final as (
select
{{ generate_surrogate_key(['BILLINGDETAIL_UID']) }} as PK_ODS_ES_EQAI_BILLING_DETAIL_ID,
CURRENT_TIMESTAMP as SYS_CREATE_DTM,
CTE_ins_batch_id.ins_batch_id as SYS_EXEC_ID,
CURRENT_TIMESTAMP as SYS_LAST_UPDATE_DTM,
BILLINGDETAIL_UID,
BILLING_UID,
REF_BILLINGDETAIL_UID,
BILLINGTYPE_UID,
BILLING_TYPE,
COMPANY_ID,
PROFIT_CTR_ID,
RECEIPT_ID,
LINE_ID,
PRICE_ID,
TRANS_SOURCE,
TRANS_TYPE,
PRODUCT_ID,
DIST_COMPANY_ID,
DIST_PROFIT_CTR_ID,
SALES_TAX_ID,
APPLIED_PERCENT,
EXTENDED_AMT,
JDE_BU,
JDE_OBJECT,
GL_ACCOUNT_CODE,
ADDED_BY,
DATE_ADDED,
MODIFIED_BY,
DATE_MODIFIED,
AX_MAINACCOUNT,
AX_DIMENSION_1,
AX_DIMENSION_2,
AX_DIMENSION_3,
AX_DIMENSION_4,
AX_DIMENSION_5_PART_1,
AX_DIMENSION_5_PART_2,
AX_DIMENSION_6,
DISC_AMOUNT,
CURRENCY_CODE,
SF_INSERT_TIMESTAMP
from CTE_dedup
join CTE_ins_batch_id on 1=1
)
select * from CTE_final