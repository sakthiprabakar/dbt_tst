import streamlit as st
import boto3
import os
import shutil
import json
import re
import snowflake.connector

def create_folders():
    os.makedirs("processed", exist_ok=True)
    os.makedirs("backup", exist_ok=True)

def process_sql_file(uploaded_file):
    file_path = os.path.join("processed", uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    shutil.move(file_path, os.path.join("backup", uploaded_file.name))
    return content

def generate_response(prompt):
    bedrock = boto3.client('bedrock-runtime', region_name="us-east-1")
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
    }
    try:
        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps(payload),
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response["body"].read())
        return response_body["content"][0]["text"]
    except Exception as e:
        return f"Error: {e}"

def generate_dbt_files(input_string):
    table_name_match = re.search(r"- name: (\w+)", input_string)
    if table_name_match:
        table_name = table_name_match.group(1)
    else:
        st.error("Table name not found in the input string.")
        return

    folder_name = os.path.join("processed", table_name)
    os.makedirs(folder_name, exist_ok=True)

    sql_part, yml_part = input_string.split("models:\n", 1)
    with open(os.path.join(folder_name, f"{table_name}.sql"), "w") as sql_file:
        sql_file.write(sql_part.strip())
    with open(os.path.join(folder_name, "ods.yml"), "w") as yml_file:
        yml_file.write("models:\n" + yml_part.strip())
    
    st.success(f"DBT files generated in '{folder_name}'.")

st.title("Snowflake DBT Script Generator")
create_folders()

prompt = """
Create the DBT Script and DBT Model for the above Snowflake table with the following template :

**Instruction** : 
1. Reformat column names by adding underscores between meaningful words while keeping the original capitalization. Ensure the names remain valid database column names in final table. If the Column name is meaningless means you have to generate Alias for that column. Example:  CUSTOMERDETAIL_ID as CUSTOMER_DETAIL_ID, TRANSPORTERCONTACT_PHONE as TRANSPORTER_CONTACT_PHONE, CONSOLIDATIONGROUP_NAME as CONSOLIDATION_GROUP_NAME, etc.
2. Reformat Primary Key Column names by adding underscores between meaningful words while keeping the original capitalization. Ensure the names remain valid database column names in final table. Example:  RETAILERUNIQUE_ID as RETAILER_UNIQUE_ID, etc.
3. If the Column names are not in English language then you have to create Alias for those columns in english. Example :  NOM_LEGAL_CLIENT AS CUSTOMER_LEGAL_NAME, TYPE_COMPG AS COMPANY_TYPE, etc.
4. If the Primary key Column names are not in English language then you have to create Alias for those columns in english. Example :  NUM_CLIENT AS CUSTOMER_ID, etc.

DBT Script Template :

{{
config(
materialized='incremental',
unique_key='PK_ODS_ES_EQAI_{<Table Name>}_ID',
merge_no_update_columns = ['SYS_CREATE_DTM'],
tags=["ods","es-eqai","scheduled-nightly"]
)
}}
-- Read the data from staging table as incrementally
with CTE_source as (
select
*
from {{ source('STAGING', 'STG_ES_EQAI_{<Table Name>}') }}
{% if is_incremental() %}
where SF_INSERT_TIMESTAMP > '{{ get_max_event_time('SF_INSERT_TIMESTAMP',not_minus3=True) }}'
{% endif %}
)
-- Apply deduplication logic
, CTE_dedup as (
select *
from CTE_source
qualify row_number() over (partition by <PRIMARY_KEY and NOT NULL Columns> order by <Modified date(AUDIT DATE COLUMN if present)> desc, SF_INSERT_TIMESTAMP desc) = 1
)

-- Create a batch id
, CTE_ins_batch_id as (
select TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as ins_batch_id
)
-- Create the final table
, CTE_final as (
select
{{ generate_surrogate_key(['<Primary Keys and NOT NULL Columns>']) }} as PK_ODS_ES_EQAI_<Table name>_ID,
CURRENT_TIMESTAMP as SYS_CREATE_DTM,
CTE_ins_batch_id.ins_batch_id as SYS_EXEC_ID,
CURRENT_TIMESTAMP as SYS_LAST_UPDATE_DTM,
<All Table columns in separate lines>,    //If the column name is meaningless means generate Alias for that column with delimited by _.
SF_INSERT_TIMESTAMP
from CTE_dedup
join CTE_ins_batch_id on 1=1
)
select * from CTE_final


DBT Model :

models:

#ODS_ES_EQAI_<Table Name>
    - name: ODS_ES_EQAI_<Table name>
      columns:
      - name: PK_ODS_ES_EQAI_<Table name>_ID
        tests:
          - unique:
              config:
                severity: warn
          - not_null:
              config:
                severity: warn
      tests:
        - test_columns_not_null:
            column_list: [<Primary keys and NOT NULL Columns>, 'SF_INSERT_TIMESTAMP']
            config:
              severity: warn

**Note** : 
1. Strictly include all the columns and primary keys in the output DBT Script and DBT model.
2. You should return only the DBT Script and DBT Model alone. Do not retrun any content in the output.
"""


option = st.radio("Select an option", ["Upload SQL file", "Connect to Snowflake Database"])

if option == "Upload SQL file":
    uploaded_file = st.file_uploader("Upload SQL file", type=["sql"])
    primary_keys = st.text_input("Enter Primary Keys (comma-separated)")
    not_null_columns = st.text_input("Enter Not Null Columns (comma-separated)")
    script_type = st.radio("Select DBT Script Type", ["Default DBT Script", "Custom DBT Script Generation"])
    
    custom_instructions = ""
    if script_type == "Custom DBT Script Generation":
        custom_instructions = st.text_area("Enter additional instructions for custom DBT script")

    if st.button("Generate DBT Script"):
        if uploaded_file and primary_keys:
            sql_content = process_sql_file(uploaded_file)
            
            base_prompt = f"# Snowflake Table:\n\n{sql_content}\n\nPrimary Keys : {primary_keys}\n\nNot Null Columns : {not_null_columns}\n\n"
            if script_type == "Custom DBT Script Generation":
                prompt = base_prompt  +"\n\n"+ f"""Before Create a batch id and final table in the given template Create a Separate DBT Script as CTE with the logic instead of adding conditions in final table : {custom_instructions} \n\n Example : \n\n -- Apply data cleaning logic. \n\n Custom DBT Script Generation input : cust_id can't be 0 and negative values for profit_clrr_id are not valid. \n\n Then the CTE for this logic is : \n\n , CTE_clean as (
                    select *
                        from CTE_source
                        where cust_id != 0
                        and profit_clrr_id >= 0
                    )

                    #Note : This custom CTE logic should be created before Read the data from staging table as incrementally CTE and before Apply deduplication logic CTE.
                    """ + prompt
            else:
                prompt = base_prompt + prompt
            
            dbt_data = generate_response(prompt)
            generate_dbt_files(dbt_data)
        else:
            st.error("Please upload an SQL file and enter primary keys.")

elif option == "Connect to Snowflake Database":
    with st.form("snowflake_connection_form"):
        snowflake_account = st.text_input("Snowflake Account")
        snowflake_user = st.text_input("Username")
        snowflake_password = st.text_input("Password", type="password")
        snowflake_database = st.text_input("Database")
        snowflake_schema = st.text_input("Schema")
        connect_button = st.form_submit_button("Connect to Snowflake")

    if connect_button:
        if all([snowflake_account, snowflake_user, snowflake_password, snowflake_database, snowflake_schema]):
            try:
                conn = snowflake.connector.connect(
                    user=snowflake_user,
                    password=snowflake_password,
                    account=snowflake_account,
                    database=snowflake_database,
                    schema=snowflake_schema
                )
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = [row[1] for row in cursor.fetchall()]
                st.session_state["tables"] = tables
                st.session_state["conn"] = conn
                st.success("Connected to Snowflake! Select tables below.")
            except Exception as e:
                st.error(f"Error connecting to Snowflake: {e}")

    if "tables" in st.session_state:
        script_type = st.radio("Select DBT Script Type", ["Default DBT Script", "Custom DBT Script Generation"])
        
        if script_type == "Custom DBT Script Generation":
            selected_tables = st.selectbox("Select One Table", st.session_state["tables"])
            custom_instructions = st.text_area("Enter additional instructions for custom DBT script")
        else:
            selected_tables = st.multiselect("Select Tables", st.session_state["tables"])

        if selected_tables and "conn" in st.session_state:
            if st.button("Generate DBT Scripts"):
                tables_to_process = [selected_tables] if script_type == "Custom DBT Script Generation" else selected_tables

                for table in tables_to_process:
                    cursor = st.session_state["conn"].cursor()

                    cursor.execute(f"""
                        SELECT COLUMN_NAME, IS_NULLABLE 
                        FROM {snowflake_database}.INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = '{snowflake_schema}' AND TABLE_NAME = '{table}'
                    """)
                    columns_result = cursor.fetchall()
                    all_columns = [row[0] for row in columns_result]
                    not_null_columns = [row[0] for row in columns_result if row[1] == 'NO']

                    cursor.execute(f"SHOW PRIMARY KEYS IN TABLE {snowflake_database}.{snowflake_schema}.{table}")
                    primary_keys_result = cursor.fetchall()
                    primary_keys = [row[4] for row in primary_keys_result]

                    base_prompt = f"# Snowflake Table: {table}\nPrimary Key: {', '.join(primary_keys)}\nColumns: {', '.join(all_columns)}\n\n"
                    
                    if script_type == "Custom DBT Script Generation":
                        final_prompt = base_prompt + f"Before Create a batch id, Create the final table in the given template Create a Scripts with the logic : {custom_instructions} \n\n " + prompt
                    else:
                        final_prompt = base_prompt + prompt

                    dbt_data = generate_response(final_prompt)
                    generate_dbt_files(dbt_data)
