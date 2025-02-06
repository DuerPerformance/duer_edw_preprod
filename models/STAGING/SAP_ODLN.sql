{{ config(
    materialized='table',
        post_hook = [
        "drop view if exists EDW_PREPROD.STAGING.TEMP_SAP_ODLN_US",
        "drop view if exists EDW_PREPROD.STAGING.TEMP_SAP_ODLN_CA"
    ]
) }}

{{ dbt_utils.union_relations([ref('TEMP_SAP_ODLN_US'), ref('TEMP_SAP_ODLN_CA')]) }}