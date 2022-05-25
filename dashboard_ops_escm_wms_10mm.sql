/*
Author : jungwook.kim
FileName : dashboard_ops_escm_wms_10mm.sql
Note
@Summary : Ops Dashboard escm/wms Order Processing Data for Date/Hour/Min/Center/Warehouse/Dlvy_rnd/Keep_level
@Method : UPSERT
@Schedule : 10mm
@Version 1.0 : (2022-03-22)
@Cron : 3-53/10 0-3,9-23 * * * $py_path && python3 rs_write.py -SQL 'dev_SOMS_Master_v2.sql' -TABLE 'dev_ops_dashboard_escm_wms_10mm_v2' -SCHEMA 'mkrs_aa_dp_schema' -SC '#slack_alarm_test_yeswook' -PK 'processing_date' 'processing_hour' 'processing_min''center_cd' 'warehouse_cd' 'dlvy_rnd' 'dlvy_rnd_lv2' 'dlvy_type' 'keep_level' 'prcs_type' >> $log_path/dev_ops_dashboard_escm_wms_10mm_`date "+\%Y\%m\%d"`.log 2>&1
*/
-- code review
/* 오더타입과 보관단위 변환용 im2 */
with scan_type as( 
    select 
      user_value as ord_type,
      desc1 as keep_level
   from mkrs_aa_schema.infra_master 
   where user_key = 'scan_alarm'     
/* 오더타입 및 센터 정보 변화  im0 */   
), cc_ord_type as ( 
    select 
      user_arg1 as warehouse_cd,
      user_arg2 as ord_type,
      user_value as center_cd
    from mkrs_aa_schema.infra_master
    where user_key = 'CC_CD'
/* 회차정보 im1 */
), dlvy_type as ( 
    select 
      desc2::varchar as dlvy_type,
      user_arg1::varchar as wave,
      user_arg2 as region_group_code,
      user_arg3 as wave_dlvy
    from mkrs_aa_schema.infra_master 
    where user_key = 'ops_dash_wave'
/* 출고 정보 */
), tm as (
    select 
      order_no,
      order_type,
      delivery_date,
      delivery_round,
      max(categorize_date) as categorize_date,
      substring(max(categorize_date+categorize_time),9,6) as categorize_time, /* 익일 새벽에 출고되는 경우, 그 시간이 나오도록 처리 */
      max(categorize) as categorize
--     from mkrs_aa_schema.dp_wms_tmipt_10mm as tm
    from mkrs_aa_schema.stream_aa_dp_wms_tmipt as tm
    where 1=1
      and delivery_date::date >= '<user_dateEnd>'::date-7 /* 여유롭게 7일가량의 데이터를 조회 */
    group by 
      order_no, 
      order_type, 
      delivery_date, 
      delivery_round
/* 재사용주문 테이블 key로 필터링 */
), re_use as ( 
    select 
      order_no,
      key
    from mkrs_schema.cms_order_logistics_remarks
    where key = 'reusablePackingType' /* 재사용 주문 */
), ioa_base as (
    select
      ioa.escm_dt::date as processing_date,
      case when (SUBSTRING(iw.cretim, 1,2) BETWEEN '00' and '07') THEN (SUBSTRING(iw.cretim, 1,2)::int + 24 ::VARCHAR) ELSE SUBSTRING(iw.cretim, 1,2) end as processing_hour,
      SUBSTRING(iw.cretim, 3,1)||'0' as processing_min,
      CASE when (SUBSTRING(tm.categorize_time, 1,2) BETWEEN '00' AND '07') THEN (SUBSTRING(tm.categorize_time, 1,2)::int + 24)::VARCHAR ELSE SUBSTRING(tm.categorize_time, 1,2) END as finhour,
      SUBSTRING(tm.categorize_time, 3,1)||'0' as finmin,
      cc_ord_type.center_cd,
      cc_ord_type.warehouse_cd,
      case when ioa.delivery_round is null then '2' 
            else ioa.delivery_round
            end as dlvy_rnd,
      dlvy_type.wave_dlvy as dlvy_rnd_lv2,
      case when ioa.delivery_type = 0 then '직배'
            when ioa.delivery_type in (1,3) then '택배'
            else '택배' 
            end as dlvy_type,
      scan_type.ord_type as ord_type,
      scan_type.keep_level as keep_level,
      ioa.ord_cd as ord_cd,
      nvl(iw.lfimg,0) :: integer as unit,
      nvl(tm.categorize, 'N') :: varchar as is_scan,
      nvl2(re_use."KEY", 'Y', 'N') :: varchar as is_reuse,
      tm.categorize_date, 
      tm.categorize_time  /* 출고 데이터에서 조건절로 활용 */
    from mkrs_aa_schema.invt_outbound_all_10mm as ioa
--         inner join mkrs_aa_schema.dp_wms_ifwms113_10mm as iw /* cretim 계산, flink방식으로 변경 희망 */
        inner join mkrs_aa_schema.stream_dp_wms_ifwms113 as iw
            on ioa.center_cd = iw.wareky
                and ioa.ord_type = iw.bwart 
                and ioa.ord_cd = iw.vbeln
                and ioa.prd_cd = iw.skukey
        left join tm /* SCAN 여부확인 */
             on ioa.ord_cd = tm.order_no 
                and ioa.ord_type = tm.order_type
        left join re_use
            on ioa.ord_cd = re_use.order_no :: varchar
        inner join cc_ord_type 
            on ioa.center_cd = cc_ord_type.warehouse_cd 
                and ioa.ord_type = cc_ord_type.ord_type 
        left join dlvy_type
            on ioa.delivery_type  = dlvy_type.dlvy_type
                and ioa.delivery_round =  dlvy_type.wave
                and (ioa.delivery_type = '0' and length(ioa.region_group_code) = 1 and dlvy_type.region_group_code is null /* 1/2/3 회차 */
                    or ioa.delivery_type = '0' and length(ioa.region_group_code) != 1 and dlvy_type.region_group_code is not null and dlvy_type.region_group_code = ioa.region_group_code /* 샛별 부산/울산 */
                    or ioa.delivery_type != '0' and dlvy_type.region_group_code = ioa.region_group_code) /* 택배/샛별 충청 대구  */
        inner join scan_type 
            on ioa.ord_type = scan_type.ord_type 
    where 1=1
        and ioa.escm_dt = case when (extract(hour from current_timestamp) <= 8) 
                           then to_char(('<user_dateEnd>'::date - interval '1 day'),'yyyy-mm-dd')
                           else  '<user_dateEnd>'::date::varchar
                           end
        and ioa.cancel_yn = '0' /* 취소주문 제외 */
), ioa_agg_by_keep_level_in as ( /* keep_level 단위로 집계 (인입) */
    select 
      processing_date,
      processing_hour,
      processing_min,
      center_cd,
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level,
      is_reuse,
      '인입'::varchar as prcs_type,
      count (distinct ord_cd || ord_type) as order_cnt,
      sum(unit) as unit_cnt
    from ioa_base
    group by
      processing_date, 
      processing_hour, 
      processing_min,
      center_cd,
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level, 
      is_reuse
), ioa_agg_by_keep_level_out as ( /* keep_level 단위로 집계 (출고) */
    select 
      processing_date,
      finhour as processing_hour,
      finmin as processing_min,
      center_cd,
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level,
      is_reuse,
      '출고'::varchar as prcs_type,
      count (distinct ord_cd || ord_type) as order_cnt,
      sum(unit) as unit_cnt
    from ioa_base
    where 1=1
        and is_scan = 'Y'
        and categorize_date is not null
        and categorize_time is not null
    group by 
      processing_date, 
      finhour,
      finmin ,
      center_cd,
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level, 
      is_reuse
), ioa_agg_total_in as ( /* 주문단위 집계 (warehouse_cd 별도) */
    select 
      processing_date,
      processing_hour,
      processing_min,
      center_cd,
      '총' :: varchar as warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      '총':: varchar as keep_level,
      is_reuse,
      '인입'::varchar as prcs_type,
      count (distinct ord_cd) as order_cnt,
      sum(unit) as unit_cnt
    from ioa_base
    group by 
      processing_date, 
      processing_hour,
      processing_min,
      center_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type, 
      is_reuse
), ioa_agg_total_out as ( /* 주문단위 집계 (warehouse_cd 별도) 출고는 keep_level별로 스캔완료시간이 달라, 일일 총 집계만 진행 */
    select 
      processing_date,
      '총' ::varchar as processing_hour,
      '총' ::varchar as processing_min,
      center_cd,
      '총'::varchar as warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      '총'::varchar as keep_level,
      is_reuse,
      '출고'::varchar as prcs_type,
      count (distinct ord_cd) as order_cnt,
      sum(unit) as unit_cnt
    from ioa_base
    where 1=1
        and is_scan='Y'
        and categorize_date is not null
        and categorize_time is not null
    group by 
      processing_date, 
      center_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type, 
      is_reuse
), ioa_agg as (
    select *
    from ioa_agg_by_keep_level_in
    union all
    select *
    from ioa_agg_total_in
    union all
    select *
    from ioa_agg_by_keep_level_out
    union all
    select *
    from ioa_agg_total_out
), ioa_order as ( 
    select 
      processing_date,
      processing_hour,
      processing_min,
      center_cd, 
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level,
      prcs_type,
      sum(order_cnt) as order_cnt,
      sum(unit_cnt) as unit_cnt
    from ioa_agg
    group by 
      processing_date,
      processing_hour,
      processing_min,
      center_cd,
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level,
      prcs_type
), ioa_reuse as ( -- 재사용 
    select 
      processing_date,
      processing_hour,
      processing_min,
      center_cd, 
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level,
      prcs_type,
      sum(order_cnt) as reuse_ord_cnt,
      sum(unit_cnt) as reuse_unit_cnt
    from ioa_agg
    where is_reuse = 'Y'
    group by 
      processing_date,
      processing_hour,
      processing_min,
      center_cd,
      warehouse_cd,
      dlvy_rnd,
      dlvy_rnd_lv2,
      dlvy_type,
      keep_level,
      prcs_type 
), master_tb as (
    select 
        ioa_order.processing_date :: date,
        ioa_order.processing_hour::varchar,
        ioa_order.processing_min::varchar,
        ioa_order.center_cd::varchar,
        ioa_order.warehouse_cd::varchar,
        ioa_order.dlvy_rnd::varchar,
        ioa_order.dlvy_rnd_lv2::varchar,
        ioa_order.dlvy_type::varchar,
        ioa_order.keep_level::varchar,
        ioa_order.prcs_type ::varchar,
        ioa_order.order_cnt ::int as ord_count,
        ioa_order.unit_cnt::int as unit_count,
        nvl(ioa_reuse.reuse_ord_cnt,0)::int as reuse_ord_count,
        nvl(ioa_reuse.reuse_unit_cnt,0)::int as reuse_unit_count,
        getdate ( )::timestamp AS update_dt
     from ioa_order
     left join ioa_reuse
        on  ioa_order.processing_date   =   ioa_reuse.processing_date
            and ioa_order.processing_hour   =   ioa_reuse.processing_hour
            and ioa_order.processing_min    =   ioa_reuse.processing_min
            and ioa_order.center_cd     =   ioa_reuse.center_cd
            and ioa_order.warehouse_cd  =   ioa_reuse.warehouse_cd
            and ioa_order.dlvy_rnd      =   ioa_reuse.dlvy_rnd
            and ioa_order.dlvy_rnd_lv2  =   ioa_reuse.dlvy_rnd_lv2
            and ioa_order.dlvy_type     =   ioa_reuse.dlvy_type
            and ioa_order.keep_level    =   ioa_reuse.keep_level
            and ioa_order.prcs_type     =   ioa_reuse.prcs_type
)
select *
from master_tb 
