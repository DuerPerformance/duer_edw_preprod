{{ config(
    materialized = 'table',
        post_hook = [
        "drop view if exists EDW_PREPROD.STAGING.TEMP_SAP_OCRD_US",
        "drop view if exists EDW_PREPROD.STAGING.TEMP_SAP_OCRD_CA"
    ]
) }}
{{ dbt_utils.union_relations([ref('TEMP_SAP_OCRD_US'), ref('TEMP_SAP_OCRD_CA')]) }}