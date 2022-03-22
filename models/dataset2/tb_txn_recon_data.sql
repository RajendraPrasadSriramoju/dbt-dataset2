{{
    config(
        materialized='incremental',
        incremental_strategy = 'insert_overwrite',
        on_schema_change='ignore'
    )
}}


with filter_data_source_1 as (

    select s.*, right(s.txt_txn_desc,22) as rtgs_utr_no from set2_hdfc_auto s 

),
filter_data_source_2 as (
    select s.* from set2_los_rtgs_report s
),
filter_matched_data_source_1 as (
    select fds1.*, fds2.app_id_c as agreement_no from filter_data_source_1 fds1 left outer join filter_data_source_2 fds2
     on fds1.amt_txn = fds2.disb_amount_n
     and fds1.rtgs_utr_no = fds2.utr
    and fds1.cod_drcr = 'D' and fds1.txt_txn_desc like 'RTGS%'
),
filter_matched_data_source_2 as (
    select fds2.*, fds1.rtgs_utr_no as rtgs_utr_no from filter_data_source_1 fds1 right outer join  filter_data_source_2 fds2
     on fds1.amt_txn = fds2.disb_amount_n
     and fds1.rtgs_utr_no = fds2.utr
)

select
    223 as recon_unit_id,
    amt_txn::text as column1,
    dat_txn as column2,
    dat_value as column3,
    ref_chq_no as column4,
    cod_acct_no as column5,
    cod_auth_id as column6,
    amt_od_limit as column7,
    txt_txn_desc as column8,
    bal_available as column9,
    cod_cc_brn_txn as column10,
    cod_txn_literal as column11,
    cod_txn_mnemonic as column12,
    rtgs_utr_no as column13,
    null as column14,
    null as column15,
    null as column16,
    null as column17,
    null as column18,
    null as column19,
    null as column20,
    null as column21,
    null as column22,
    null as column23,
    null as column24,
    null as column25,
    null as column26,
    null as column27,
    null as column28,
    null as column29,
    null as column30,
    99 as created_by,
    now() as created_date,
    1 as data_src_config_id,
    false as is_deleted,
    'O' as oc_status,
    agreement_no as recon_link_id,
    CASE WHEN agreement_no is not null then 1
    ELSE
     0
    end as recon_status_id,
    null as recon_notes,
    0 as updated_by,
    now() as updated_date,
    false as case_open,
    null as case_status,
    nextval('raw_data_id_seq') as raw_data_id
from filter_matched_data_source_1
union all
select
    223 as recon_unit_id,
    utr as column1,
    app_id_c as column2,
    las_edit_d as column3,
    disb_amount_n as column4,
    disb_msg_gen_date as column5,
    null as column6,
    null as column7,
    null as column8,
    null as column9,
    null as column10,
    null as column11,
    null as column12,
    null as column13,
    null as column14,
    null as column15,
    null as column16,
    null as column17,
    null as column18,
    null as column19,
    null as column20,
    null as column21,
    null as column22,
    null as column23,
    null as column24,
    null as column25,
    null as column26,
    null as column27,
    null as column28,
    null as column29,
    null as column30,
    99 as created_by,
    now() as created_date,
    2 as data_src_config_id,
    false as is_deleted,
    'O' as oc_status,
    app_id_c as recon_link_id,
    CASE WHEN rtgs_utr_no is not null then 1
    ELSE
     0
    END as recon_status_id,
    null as recon_notes,
    0 as updated_by,
    now() as updated_date,
    false as case_open,
    null as case_status,
    nextval('raw_data_id_seq') as raw_data_id
from filter_matched_data_source_2

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  --where event_time > (select max(event_time) from {{ this }})

{% endif %}
