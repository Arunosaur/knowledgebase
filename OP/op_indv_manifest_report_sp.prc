CREATE OR REPLACE PROCEDURE op_indv_manifest_report_sp(
  i_llr_num    IN      NUMBER,
  i_div        IN      VARCHAR2,
  i_create_ts  IN      VARCHAR2 DEFAULT NULL,
  i_rpt_typ    IN      VARCHAR2 DEFAULT NULL,
  o_status     OUT     VARCHAR2
) IS
  /*
  ||----------------------------------------------------------------------------
  || OP_INDV_MANIFEST_REPORT_SP
  ||  Used to call individual OPLD reports or ALL OPLD reports at once
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/15/02 | SKADALI | original
  || 08/26/10 | rhalpai | Change to remove status parm from calls to
  ||                    | OP_MANIFEST_REPORTS_PK. Convert to use standard error
  ||                    | handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT typ.t_maxfqnm := 'OP_INDV_MANIFEST_REPORT_SP';
  lar_parm             logs.tar_parm;
  l_div_part           NUMBER;
  l_create_ts          DATE;
  l_file_nm            VARCHAR2(100);
  l_cumulative_sw      VARCHAR2(1)   := 'N';
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'CreateTs', i_create_ts);
  logs.add_parm(lar_parm, 'RptTyp', i_rpt_typ);
  logs.info('ENTRY', lar_parm);
  o_status := 'Good';
  l_div_part := div_pk.div_part_fn(i_div);

  IF i_rpt_typ IS NOT NULL THEN
    IF i_create_ts IS NULL THEN
      SELECT MAX(create_ts)
        INTO l_create_ts
        FROM mclane_manifest_rpts rpts
       WHERE rpts.div_part = l_div_part
         AND rpts.llr_date = i_llr_num;
    ELSE
      l_create_ts := TO_DATE(i_create_ts, 'YYYYMMDDHH24MISS');
    END IF;

    SELECT MAX('Y')
      INTO l_cumulative_sw
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.llr_date = i_llr_num
       AND rpts.create_ts = l_create_ts
       AND rpts.strategy_id = 0
       AND ROWNUM = 1;

    IF l_cumulative_sw = 'Y' THEN
      CASE i_rpt_typ
        WHEN 'OPLD01' THEN
          logs.dbg('Execute LOADING_MANIFEST_SP procedure');
          op_manifest_reports_pk.loading_manifest_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        WHEN 'OPLD02' THEN
          logs.dbg('Execute LOAD_DEPT_SUMMARY_SP procedure');
          op_manifest_reports_pk.load_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        WHEN 'OPLD03' THEN
          logs.dbg('Execute SUMMARY_LOAD_DEPT_SUMMARY_SP procedure');
          op_manifest_reports_pk.summary_load_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        WHEN 'OPLD07' THEN
          logs.dbg('Execute STOP_ORDER_RECAP_SP procedure');
          op_manifest_reports_pk.stop_order_recap_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        WHEN 'OPLD08' THEN
          logs.dbg('Execute STOP_SUMMARY_SP procedure');
          op_manifest_reports_pk.stop_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        ELSE
          o_status := 'Error';
      END CASE;
    ELSE
      CASE i_rpt_typ
        WHEN 'OPLD04' THEN
          logs.dbg('Execute RELEASE_DEPT_SUMMARY_SP procedure');
          op_manifest_reports_pk.release_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        WHEN 'OPLD05' THEN
          logs.dbg('Execute SUMMARY_REL_DEPT_SUMMARY_SP procedure');
          op_manifest_reports_pk.summary_rel_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        WHEN 'OPLD06' THEN
          logs.dbg('Execute TOTE_RECAP_SP procedure');
          op_manifest_reports_pk.tote_recap_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
        ELSE
          o_status := 'Error';
      END CASE;
    END IF;   -- l_cumulative_sw = 'Y'
  ELSE
    op_manifest_reports_pk.manifest_reports_sp(i_llr_num, i_div, i_create_ts);
  END IF;   -- i_rpt_typ IS NOT NULL

  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    o_status := 'Error';
    logs.err(lar_parm);
END op_indv_manifest_report_sp;
/

