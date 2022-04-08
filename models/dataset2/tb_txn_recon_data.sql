{{
    config(
        materialized='incremental',
        incremental_strategy = 'insert_overwrite',
        on_schema_change='ignore'
    )
}}


with filter_data_source_1 as (
    select s.* from set2_hdfc_auto s where 
    s.cod_drcr = 'D'
),
filter_data_source_2 as (
    select s.* from set2_hdfc_auto s where 
    s.cod_drcr = 'C'
),
filter_ref_file as (
    select s.* from set2_los_rtgs_report s
),
filter_debit_source_enrich1 as (
    select fds1.*, 
    fds2.app_id_c as ref_agr1,
    fds2.app_id_c as ref_agr2,
    fds2.app_id_c as ref_agr3 from filter_data_source_1 fds1, filter_ref_file fds2 
      where fds1.txt_txn_desc like 'RTGS%'
      and fds1.amt_txn = fds2.disb_amount_n
     and trim(right(fds1.txt_txn_desc,22)) = trim(fds2.utr)
    UNION all
   select fds2.*,  trim(regexp_replace(split_part(fds2."txt_txn_desc" , '-', 1),'[[:alpha:]]','','g'))  as ref_agr1,
    trim(regexp_replace(split_part(fds2."txt_txn_desc" , '-',2),'[[:alpha:]]','','g'))  as ref_agr2,
    trim(regexp_replace(split_part(fds2."txt_txn_desc" , '-', 3),'[[:alpha:]]','','g')) as ref_agr3
     from filter_data_source_1 fds2
    where fds2.txt_txn_desc not like 'RTGS%'
),
filter_credit_source_enrich1 as (
    select fds2.*,  trim(regexp_replace(split_part(fds2."txt_txn_desc" , '-', 1),'[[:alpha:]]','','g'))  as ref_agr1,
    trim(regexp_replace(split_part(fds2."txt_txn_desc" , '-',2),'[[:alpha:]]','','g'))  as ref_agr2,
    trim(regexp_replace(split_part(fds2."txt_txn_desc" , '-', 3),'[[:alpha:]]','','g')) as ref_agr3 from filter_data_source_2 fds2
),
filter_debit_source_data as (
    select s.*,  
    case 
        when ref_agr1 = ref_agr2 and ref_agr2 = ref_agr3 then
            ref_agr1
        when length(ref_agr3) > 7 and length(ref_agr3) <14 then
            ref_agr3
        when length(ref_agr2) > 7 and length(ref_agr2) <14 then
            ref_agr2
        when length(ref_agr1) > 7 and length(ref_agr1) <14 then
            ref_agr3
        ELSE
            null    
    end as agreement_no from filter_debit_source_enrich1 s
),
filter_credit_source_data as (
    select s.*,  
    case 
        when ref_agr1 = ref_agr2 and ref_agr2 = ref_agr3 then
            ref_agr1
        when length(ref_agr3) > 7 and length(ref_agr3) <14 then
            ref_agr3
        when length(ref_agr2) > 7 and length(ref_agr2) <14 then
            ref_agr2
        when length(ref_agr1) > 7 and length(ref_agr1) <14 then
            ref_agr1
        ELSE
            null    
    end as agreement_no  from filter_credit_source_enrich1 s
),
filter_debit_matched_data as (
    select sd.*, 
    agreement_no as recon_link_id
    from filter_debit_source_data sd
    where agreement_no in (
            select dbt_grp.agreement_no from
            (select sum(amt_txn) as dbt_grp_amt, agreement_no from filter_debit_source_data 
            group by agreement_no) dbt_grp,
            (select sum(amt_txn) as cdt_grp_amt, agreement_no from filter_credit_source_data 
            group by agreement_no) cdt_grp
            where dbt_grp.agreement_no = cdt_grp.agreement_no 
            and dbt_grp.dbt_grp_amt = cdt_grp.cdt_grp_amt
    )
    
),
filter_credit_matched_data as (
    select sc.*, agreement_no as recon_link_id
    from filter_credit_source_data sc  
    where agreement_no in (
            select cdt_grp.agreement_no from 
            (select sum(amt_txn) as dbt_grp_amt, agreement_no from filter_debit_source_data 
            group by agreement_no) dbt_grp,
            (select sum(amt_txn) as cdt_grp_amt, agreement_no from filter_credit_source_data 
            group by agreement_no) cdt_grp
            where dbt_grp.agreement_no = cdt_grp.agreement_no 
            and dbt_grp.dbt_grp_amt = cdt_grp.cdt_grp_amt
        )
),
filter_credit_unmatched_data as (
    select fcs.amt_txn,
        fcs.dat_txn,
        fcs.dat_value,
        fcs.ref_chq_no,
        fcs.cod_acct_no,
        fcs.cod_auth_id,
        fcs.amt_od_limit,
        fcs.txt_txn_desc,
        fcs.bal_available,
        fcs.cod_cc_brn_txn,
        fcs.cod_txn_literal,
        fcs.cod_txn_mnemonic,
        fcs.ref_agr1,
        fcs.ref_agr2,
        fcs.ref_agr3,
        cod_drcr,
        agreement_no from filter_credit_source_data fcs 
    except 
    select 
        fcm.amt_txn,
        fcm.dat_txn,
        fcm.dat_value,
        fcm.ref_chq_no,
        fcm.cod_acct_no,
        fcm.cod_auth_id,
        fcm.amt_od_limit,
        fcm.txt_txn_desc,
        fcm.bal_available,
        fcm.cod_cc_brn_txn,
        fcm.cod_txn_literal,
        fcm.cod_txn_mnemonic,
        fcm.ref_agr1,
        fcm.ref_agr2,
        fcm.ref_agr3,
        cod_drcr,
        agreement_no from filter_credit_matched_data fcm
),
filter_debit_unmatched_data as (
    select fds.amt_txn,
        fds.dat_txn,
        fds.dat_value,
        fds.ref_chq_no,
        fds.cod_acct_no,
        fds.cod_auth_id,
        fds.amt_od_limit,
        fds.txt_txn_desc,
        fds.bal_available,
        fds.cod_cc_brn_txn,
        fds.cod_txn_literal,
        fds.cod_txn_mnemonic,
        fds.ref_agr1,
        fds.ref_agr2,
        fds.ref_agr3,
        cod_drcr,
        agreement_no from filter_debit_source_data fds 
    except select 
        fdm.amt_txn,
        fdm.dat_txn,
        fdm.dat_value,
        fdm.ref_chq_no,
        fdm.cod_acct_no,
        fdm.cod_auth_id,
        fdm.amt_od_limit,
        fdm.txt_txn_desc,
        fdm.bal_available,
        fdm.cod_cc_brn_txn,
        fdm.cod_txn_literal,
        fdm.cod_txn_mnemonic,
        fdm.ref_agr1,
        fdm.ref_agr2,
        fdm.ref_agr3,
        cod_drcr,
        agreement_no from filter_debit_matched_data fdm
)


select
    223 as recon_unit_id,
    amt_txn as column1,
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
    ref_agr1 as column13,
    ref_agr2 as column14,
    ref_agr3 as column15,
    agreement_no as column16,
    cod_drcr as column17,
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
    null as recon_link_id,
    0 as recon_status_id,
    null as recon_notes,
    0 as updated_by,
    now() as updated_date,
    false as case_open,
    null as case_status,
    nextval('raw_data_id_seq') as raw_data_id
from filter_debit_unmatched_data
union all
select
    223 as recon_unit_id,
    amt_txn as column1,
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
    ref_agr1 as column13,
    ref_agr2 as column14,
    ref_agr3 as column15,
    agreement_no as column16,
    cod_drcr as column17,
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
    null as recon_link_id,
    0 as recon_status_id,
    null as recon_notes,
    0 as updated_by,
    now() as updated_date,
    false as case_open,
    null as case_status,
    nextval('raw_data_id_seq') as raw_data_id
from filter_credit_unmatched_data
union all
select
    223 as recon_unit_id,
    amt_txn as column1,
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
    ref_agr1 as column13,
    ref_agr2 as column14,
    ref_agr3 as column15,
    agreement_no as column16,
    cod_drcr as column17,
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
    recon_link_id as recon_link_id,
    1 as recon_status_id,
    null as recon_notes,
    0 as updated_by,
    now() as updated_date,
    false as case_open,
    null as case_status,
    nextval('raw_data_id_seq') as raw_data_id
from filter_debit_matched_data
union all
select
    223 as recon_unit_id,
    amt_txn as column1,
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
    ref_agr1 as column13,
    ref_agr2 as column14,
    ref_agr3 as column15,
    agreement_no as column16,
    cod_drcr as column17,
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
    recon_link_id as recon_link_id,
    1 as recon_status_id,
    null as recon_notes,
    0 as updated_by,
    now() as updated_date,
    false as case_open,
    null as case_status,
    nextval('raw_data_id_seq') as raw_data_id
from filter_credit_matched_data

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  --where event_time > (select max(event_time) from {{ this }})

{% endif %}

