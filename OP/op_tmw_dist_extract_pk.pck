CREATE OR REPLACE PACKAGE op_tmw_dist_extract_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION llr_ts_fn(
    i_div_part  IN  NUMBER,
    i_load_num  IN  VARCHAR2,
    i_eta_ts    IN  DATE
  )
    RETURN DATE;

  FUNCTION depart_ts_fn(
    i_div_part  IN  NUMBER,
    i_load_num  IN  VARCHAR2,
    i_eta_ts    IN  DATE
  )
    RETURN DATE;

  FUNCTION unassigned_ship_dates_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION corp_cur_fn(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  VARCHAR2,
    i_ship_dt_to   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION audit_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION extract_cur_fn(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  DATE,
    i_ship_dt_to   IN  DATE,
    i_corp_list    IN  CLOB
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE extract_file_sp(
    i_div          IN  VARCHAR2,
    i_file_nm      IN  VARCHAR2,
    i_ship_dt_frm  IN  VARCHAR2,
    i_ship_dt_to   IN  VARCHAR2,
    i_corp_list    IN  VARCHAR2
  );

  PROCEDURE extract_sp(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  DATE,
    i_ship_dt_to   IN  DATE,
    i_corp_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE import_sp(
    i_div  IN  VARCHAR2
  );

END op_tmw_dist_extract_pk;
/

CREATE OR REPLACE PACKAGE BODY op_tmw_dist_extract_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || UPD_STAT_SP
  ||  Update status of CustDistRteReq.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE upd_stat_sp(
    i_div_part     IN  NUMBER,
    i_stat_cd      IN  VARCHAR2,
    i_new_stat_cd  IN  VARCHAR2
  ) IS
    l_c_sysdate  CONSTANT DATE := SYSDATE;
  BEGIN
    UPDATE cust_dist_rte_req_op5c r
       SET r.stat_cd = i_new_stat_cd,
           r.last_chg_ts = l_c_sysdate
     WHERE r.div_part = i_div_part
       AND r.stat_cd = i_stat_cd;
  END upd_stat_sp;

  /*
  ||-----------------------------------------------------------------------------
  || VALIDATE_SP
  ||  Validate CustDistRteReq.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE validate_sp(
    i_div_part  IN  NUMBER,
    i_stat_cd   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_DIST_EXTRACT_PK.VALIDATE_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'StatCd', i_stat_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Check No open Order found for Cust/ShipDt');

    UPDATE cust_dist_rte_req_op5c crr
       SET crr.stat_cd = 'CAN',
           crr.err_msg = 'No open Order found for Cust/ShipDt!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND NOT EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = crr.div_part
                         AND ld.llr_ts = DATE '1900-01-01'
                         AND ld.load_num BETWEEN 'P00P' AND 'P99P'
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.cust_id = crr.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.dsorda = 'D'
                         AND a.shpja = crr.ship_dt - DATE '1900-02-28'
                         AND a.stata = 'O');

    logs.dbg('Check New Load does not exist');

    UPDATE cust_dist_rte_req_op5c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'New Load does not exist!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND NOT EXISTS(SELECT 1
                        FROM mclp120c ld
                       WHERE ld.div_part = crr.div_part
                         AND ld.loadc = crr.new_load_num);

    logs.dbg('Check Another Cust Order found on New Load/Stop');

    UPDATE cust_dist_rte_req_op5c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'Another Cust Order found on New Load/Stop!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND (   EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = crr.div_part
                         AND ld.llr_ts = op_tmw_dist_extract_pk.llr_ts_fn(crr.div_part, crr.new_load_num,
                                                                          crr.new_eta_ts)
                         AND ld.load_num = crr.new_load_num
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = crr.new_stop_num
                         AND se.cust_id <> crr.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata IN('P', 'R', 'A'))
            OR EXISTS(SELECT 1
                        FROM cust_dist_rte_req_op5c crr2
                       WHERE crr2.div_part = crr.div_part
                         AND crr2.stat_cd = i_stat_cd
                         AND crr2.new_load_num = crr.new_load_num
                         AND crr2.new_stop_num = crr.new_stop_num
                         AND crr2.cust_id <> crr.cust_id)
           );

    logs.dbg('Check ETA Out of Sequence');

    UPDATE cust_dist_rte_req_op5c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'ETA Out of Sequence!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM cust_dist_rte_req_op5c crr2
                   WHERE crr2.div_part = crr.div_part
                     AND op_tmw_dist_extract_pk.llr_ts_fn(crr2.div_part, crr2.new_load_num, crr2.new_eta_ts) =
                                        op_tmw_dist_extract_pk.llr_ts_fn(crr.div_part, crr.new_load_num, crr.new_eta_ts)
                     AND crr2.new_load_num = crr.new_load_num
                     AND crr2.stat_cd = i_stat_cd
                     AND (   (    crr2.new_stop_num < crr.new_stop_num
                              AND crr2.new_eta_ts < crr.new_eta_ts)
                          OR (    crr2.new_stop_num > crr.new_stop_num
                              AND crr2.new_eta_ts > crr.new_eta_ts)
                         ));

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_sp;

  /*
  ||-----------------------------------------------------------------------------
  || MOVE_ORDS_SP
  ||  Move Orders for Div per CustDistRteReq.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE move_ords_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_TMW_DIST_EXTRACT_PK.MOVE_ORDS_SP';
    lar_parm             logs.tar_parm;

    TYPE l_rt_load_ord IS RECORD(
      cust_id       sysp200c.acnoc%TYPE,
      llr_ts        DATE,
      load_num      mclp120c.loadc%TYPE,
      depart_ts     DATE,
      stop_num      NUMBER,
      eta_ts        DATE,
      t_ord_nums    type_ntab,
      new_llr_dt    DATE,
      new_load_num  mclp120c.loadc%TYPE,
      new_stop_num  NUMBER,
      new_eta_ts    DATE
    );

    TYPE l_tt_load_ords IS TABLE OF l_rt_load_ord;

    l_t_load_ords        l_tt_load_ords;
    l_llr_ts_save        DATE                  := DATE '0001-01-01';
    l_load_num_save      mclp120c.loadc%TYPE   := '~';
    l_load_depart_sid    NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Table of Order Load Info for Moves');

    SELECT   se.cust_id,
             ld.llr_ts,
             ld.load_num,
             ld.depart_ts,
             se.stop_num,
             se.eta_ts,
             CAST
               (MULTISET(SELECT a.ordnoa
                           FROM ordp100a a
                          WHERE a.div_part = se.div_part
                            AND a.load_depart_sid = se.load_depart_sid
                            AND a.custa = se.cust_id
                            AND a.dsorda = 'D'
                            AND a.shpja = crr.ship_dt - DATE '1900-02-28'
                            AND a.stata = 'O'
                        ) AS type_ntab
               ) AS ord_nums,
             op_tmw_dist_extract_pk.llr_ts_fn(i_div_part, crr.new_load_num, crr.new_eta_ts) AS new_llr_dt,
             crr.new_load_num,
             crr.new_stop_num,
             crr.new_eta_ts
    BULK COLLECT INTO l_t_load_ords
        FROM cust_dist_rte_req_op5c crr, load_depart_op1f ld, stop_eta_op1g se
       WHERE crr.div_part = i_div_part
         AND crr.stat_cd = 'WRK'
         AND ld.div_part = crr.div_part
         AND ld.llr_ts = DATE '1900-01-01'
         AND ld.load_num BETWEEN 'P00P' AND 'P99P'
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = crr.cust_id
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = se.div_part
                       AND a.load_depart_sid = se.load_depart_sid
                       AND a.custa = se.cust_id
                       AND a.dsorda = 'D'
                       AND a.shpja = crr.ship_dt - DATE '1900-02-28'
                       AND a.stata = 'O')
    ORDER BY new_llr_dt, crr.new_load_num, se.load_depart_sid, se.cust_id;

    IF l_t_load_ords.COUNT > 0 THEN
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        IF NOT(    l_t_load_ords(i).new_llr_dt = l_llr_ts_save
               AND l_t_load_ords(i).new_load_num = l_load_num_save) THEN
          l_llr_ts_save := l_t_load_ords(i).new_llr_dt;
          l_load_num_save := l_t_load_ords(i).new_load_num;
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part,
                                                                   l_t_load_ords(i).new_llr_dt,
                                                                   l_t_load_ords(i).new_load_num
                                                                  );
        END IF;   -- NOT (l_t_load_ords(i).new_llr_dt = l_llr_ts_save AND l_t_load_ords(i).new_load_num = l_load_num_save)

        logs.dbg('Move Orders');
        op_order_load_pk.move_ords_sp(i_div_part,
                                      l_t_load_ords(i).cust_id,
                                      l_load_depart_sid,
                                      l_t_load_ords(i).new_stop_num,
                                      l_t_load_ords(i).new_eta_ts,
                                      l_t_load_ords(i).llr_ts,
                                      l_t_load_ords(i).load_num,
                                      l_t_load_ords(i).depart_ts,
                                      l_t_load_ords(i).stop_num,
                                      l_t_load_ords(i).eta_ts,
                                      'TMW_RTE',
                                      'TMW',
                                      l_t_load_ords(i).t_ord_nums
                                     );
      END LOOP;
    END IF;   -- l_t_load_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_ords_sp;

  /*
  ||-----------------------------------------------------------------------------
  || MOVE_SP
  ||  Validate and appy Moves for CustDistRteReq recs for Div.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE move_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_DIST_EXTRACT_PK.MOVE_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Set Work Status');
    upd_stat_sp(i_div_part, 'OPN', 'WRK');
    logs.dbg('Validate');
    validate_sp(i_div_part, 'WRK');
    logs.dbg('Move Orders');
    move_ords_sp(i_div_part);
    logs.dbg('Set Complete Status');
    upd_stat_sp(i_div_part, 'WRK', 'CMP');
    COMMIT;
    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END move_sp;

  /*
  ||-----------------------------------------------------------------------------
  || IMPORT_RPT_SP
  ||  Create and send TMW_DIST_POST_IMPORT csv file to TMW system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE import_rpt_sp(
    i_div_part   IN  NUMBER,
    i_create_ts  IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_TMW_DIST_EXTRACT_PK.IMPORT_RPT_SP';
    lar_parm             logs.tar_parm;
    l_div                VARCHAR2(2);
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_prod_test          VARCHAR2(50);
    l_file_nm            VARCHAR2(50);
    l_t_rpt_lns          typ.tas_maxvc2;
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CreateTs', i_create_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div := div_pk.div_id_fn(i_div_part);
    l_appl_srvr := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_appl_srvr);
    l_prod_test :=(CASE
                     WHEN UPPER(SYS_CONTEXT('USERENV', 'DB_NAME')) = 'OPCIGP' THEN 'Prod'
                     ELSE 'Test'
                   END);
    l_file_nm := 'TMW_DIST_POST_IMPORT_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS') || '_' || l_div || '.csv';
    logs.dbg('Get Report Data');

    SELECT x.rpt_ln
    BULK COLLECT INTO l_t_rpt_lns
      FROM (SELECT 'CustDistRteReqSid'
                   || ','
                   || 'DivId'
                   || ','
                   || 'CustId'
                   || ','
                   || 'ShipDt'
                   || ','
                   || 'NewLoadNum'
                   || ','
                   || 'NewStopNum'
                   || ','
                   || 'NewEtaTs'
                   || ','
                   || 'StatCd'
                   || ','
                   || 'ErrMsg'
                   || ','
                   || 'CreateTs'
                   || ','
                   || 'LastChgTs' AS rpt_ln
              FROM DUAL
            UNION ALL
            SELECT rpt_ln
              FROM (SELECT   r.cust_dist_rte_req_sid
                             || ','
                             || d.div_id
                             || ','
                             || r.cust_id
                             || ','
                             || TO_CHAR(r.ship_dt, 'YYYY-MM-DD')
                             || ','
                             || r.new_load_num
                             || ','
                             || r.new_stop_num
                             || ','
                             || TO_CHAR(r.new_eta_ts, 'YYYY-MM-DD HH24:MI')
                             || ','
                             || r.stat_cd
                             || ','
                             || r.err_msg
                             || ','
                             || TO_CHAR(r.create_ts, 'YYYY-MM-DD HH24:MI:SS')
                             || ','
                             || TO_CHAR(r.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS rpt_ln
                        FROM cust_dist_rte_req_op5c r, div_mstr_di1d d
                       WHERE r.div_part = i_div_part
                         AND r.create_ts = i_create_ts
                         AND d.div_part = r.div_part
                    ORDER BY r.cust_dist_rte_req_sid)) x;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_file_nm);
    logs.dbg('OS Command Setup');
    l_cmd := 'mv /ftptrans/'
             || l_file_nm
             || ' '
             || '/TMW/'
             || l_prod_test
             || '/;mailx -s ''TMW OP Dist Post Import Report for '
             || l_div
             || ''' TMWOPExceptions@mclane.mclaneco.com <<EOM'
             || cnst.newline_char
             || 'file:///\\mclane.mclaneco.com\MCLANE-ROOT$\APP-DATA\DivDATA\MC\TMW\'
             || l_prod_test
             || '\'
             || l_file_nm
             || cnst.newline_char
             || 'EOM'
             || cnst.newline_char;
    logs.dbg('Process Command' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.dbg('OS Result' || cnst.newline_char || l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END import_rpt_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || LLR_TS_FN
  ||  Get LlrTs for Div/Load/EtaTs
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  FUNCTION llr_ts_fn(
    i_div_part  IN  NUMBER,
    i_load_num  IN  VARCHAR2,
    i_eta_ts    IN  DATE
  )
    RETURN DATE IS
    l_llr_ts     DATE;
    l_depart_ts  DATE;
  BEGIN
    op_order_moves_pk.get_llr_depart_for_load_eta_sp(div_pk.div_id_fn(i_div_part),
                                                     i_load_num,
                                                     i_eta_ts,
                                                     l_llr_ts,
                                                     l_depart_ts
                                                    );
    RETURN(l_llr_ts);
  END llr_ts_fn;

  /*
  ||-----------------------------------------------------------------------------
  || DEPART_TS_FN
  ||  Get DepartTs for Div/Load/EtaTs
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  FUNCTION depart_ts_fn(
    i_div_part  IN  NUMBER,
    i_load_num  IN  VARCHAR2,
    i_eta_ts    IN  DATE
  )
    RETURN DATE IS
    l_llr_ts     DATE;
    l_depart_ts  DATE;
  BEGIN
    op_order_moves_pk.get_llr_depart_for_load_eta_sp(div_pk.div_id_fn(i_div_part),
                                                     i_load_num,
                                                     i_eta_ts,
                                                     l_llr_ts,
                                                     l_depart_ts
                                                    );
    RETURN(l_depart_ts);
  END depart_ts_fn;

  /*
  ||----------------------------------------------------------------------------
  || UNASSIGNED_SHIP_DATES_CUR_FN
  ||  Returns cursor of ship dates for special (P%%) distribution orders on their default loads (P%%P).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION unassigned_ship_dates_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_DIST_EXTRACT_PK.UNASSIGNED_SHIP_DATES_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS shp_dt
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_ts = DATE '1900-01-01'
            AND ld.load_num BETWEEN 'P00P' AND 'P99P'
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.excptn_sw = 'N'
            AND a.stata = 'O'
            AND a.dsorda = 'D'
            AND a.ldtypa BETWEEN 'P00' AND 'P99'
       GROUP BY a.shpja
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END unassigned_ship_dates_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || CORP_CUR_FN
  ||  Returns cursor of corp codes for ship date range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION corp_cur_fn(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  VARCHAR2,
    i_ship_dt_to   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_DIST_EXTRACT_PK.CORP_CUR_FN';
    lar_parm             logs.tar_parm;
    l_from_shp_dt        NUMBER;
    l_to_shp_dt          NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ShipDtFrm', i_ship_dt_frm);
    logs.add_parm(lar_parm, 'ShipDtTo', i_ship_dt_to);
    logs.dbg('ENTRY', lar_parm);
    l_from_shp_dt := TO_DATE(i_ship_dt_frm, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_to_shp_dt := TO_DATE(i_ship_dt_to, 'YYYY-MM-DD') - DATE '1900-02-28';

    OPEN l_cv
     FOR
       SELECT   c.corp_cd, c.corp_nm
           FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, corp_cd_dm1c c, ordp100a a
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_ts = DATE '1900-01-01'
            AND ld.load_num BETWEEN 'P00P' AND 'P99P'
            AND se.div_part = ld.div_part
            AND se.load_depart_sid = ld.load_depart_sid
            AND cx.div_part = se.div_part
            AND cx.custb = se.cust_id
            AND c.corp_cd = cx.corpb
            AND a.div_part = se.div_part
            AND a.load_depart_sid = se.load_depart_sid
            AND a.custa = se.cust_id
            AND a.excptn_sw = 'N'
            AND a.stata = 'O'
            AND a.dsorda = 'D'
            AND a.ldtypa BETWEEN 'P00' AND 'P99'
            AND a.shpja BETWEEN l_from_shp_dt AND l_to_shp_dt
       GROUP BY c.corp_cd, c.corp_nm
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END corp_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || AUDIT_CUR_FN
  ||  Returns cursor of audit history of selection criteria.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION audit_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_DIST_EXTRACT_PK.AUDIT_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   d.div_id, a.ship_dt_frm, a.ship_dt_to, a.corp_list, a.last_chg_ts, a.user_id
           FROM div_mstr_di1d d, cust_dist_rte_extr_op6c a
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
       ORDER BY a.last_chg_ts DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END audit_cur_fn;

  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_SQL_FN
  ||  Return SQL for TMW Special Dist Routing Extract.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  FUNCTION extract_sql_fn(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  VARCHAR2,
    i_ship_dt_to   IN  VARCHAR2,
    i_corp_list    IN  VARCHAR2,
    i_file_nm      IN  VARCHAR2
  )
    RETURN CLOB IS
    l_sql  CLOB;
  BEGIN
    l_sql :=
      TO_CLOB
        ('#!/bin/ksh
DIV=' || i_div || '
. /local/prodcode/properties/op/xxopGeneric.properties
. /usr/local/bin/oraenv
sqlplus -S opcig_user/${DIV_PSWD}@${DSN_NAME} 1>/dev/null <<EOF
SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 0
SET LINESIZE 1000
SET DEFINE OFF
SET HEADING OFF
SET FEEDBACK OFF
EXEC env.set_app_cd(''OPCIG'');
SPOOL /ftptrans/'
         || i_file_nm
         || '
WITH o AS(
  SELECT d.fin_div_cd, d.div_id, d.div_part, ld.load_num, a.custa AS cust_id, 0 AS stop_num,
         LPAD(cx.corpb, 3, ''0'') AS corp_cd,
         DATE ''1900-02-28'' + a.shpja AS ship_dt,
         (CASE m.catg_typ_cd WHEN ''K'' THEN ''COOLER'' WHEN ''F'' THEN ''FREEZER'' ELSE ''DRY'' END) AS categ,
         b.totctb, ct.outerb, ct.innerb, ct.boxb, ct.totcnb,
         SUM(DECODE(b.totctb, NULL, b.ordqtb)) AS cases,
         DECODE(ct.boxb,
                ''N'', DECODE(ct.pccntb,
                            ''Y'', CEIL(SUM(b.ordqtb) / ct.totcnb),
                            ''N'', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                      / DECODE(ct.innerb, NULL, .000001, 0, .000001, ct.innerb)
                                     )
                           )
               ) AS tote_cnt,
         SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube,
         SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
    FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, ordp120b b, sawp505e e, mclp210c m, mclp200b ct
   WHERE d.div_id = ''' || i_div ||'''
     AND ld.div_part = d.div_part
     AND ld.llr_ts = DATE ''1900-01-01''
     AND ld.load_num BETWEEN ''P00P'' AND ''P99P''
     AND a.div_part = ld.div_part
     AND a.load_depart_sid = ld.load_depart_sid
     AND a.excptn_sw = ''N''
     AND a.stata = ''O''
     AND a.dsorda = ''D''
     AND a.ldtypa BETWEEN ''P00'' AND ''P99''
     AND a.shpja BETWEEN DATE ''' || i_ship_dt_frm || ''' - DATE ''1900-02-28'' AND DATE ''' || i_ship_dt_to || ''' - DATE ''1900-02-28''
     AND (   ''' || i_corp_list || ''' IS NULL
          OR EXISTS(SELECT 1
                      FROM mclp020b cx, TABLE(framework.lob2table.separatedcolumns(TO_CLOB(''' || i_corp_list || '''), ''~'')) t
                     WHERE cx.div_part = a.div_part
                       AND cx.custb = a.custa
                       AND cx.corpb = TO_NUMBER(t.column1))
         )
     AND cx.div_part = a.div_part
     AND cx.custb = a.custa
     AND b.div_part = a.div_part
     AND b.ordnob = a.ordnoa
     AND b.excptn_sw = ''N''
     AND b.statb = ''O''
     AND b.subrcb < 999
     AND b.ordqtb > 0
     AND b.ntshpb IS NULL
     AND e.iteme = b.itemnb
     AND e.uome = b.sllumb
     AND m.div_part = b.div_part
     AND m.manctc = b.manctb
     AND ct.div_part(+) = b.div_part
     AND ct.totctb(+) = b.totctb
GROUP BY d.fin_div_cd, d.div_id, d.div_part, a.shpja, ld.load_num, a.custa, cx.corpb,
         (CASE m.catg_typ_cd WHEN ''K'' THEN ''COOLER'' WHEN ''F'' THEN ''FREEZER'' ELSE ''DRY'' END),
         b.manctb, b.totctb, ct.outerb, ct.innerb, ct.pccntb, ct.boxb, ct.totcnb
), x AS(
  SELECT o.div_id, o.fin_div_cd, o.div_part, o.cust_id, o.load_num, o.stop_num, o.corp_cd,
         ''MAN'' AS load_typ,
         o.ship_dt,
         SUM(o.cases) AS cases,
         NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
         SUM(o.prod_cube) AS prod_cube,
         SUM(o.prod_wt) AS prod_wt,
         SUM(DECODE(o.categ, ''FREEZER'', DECODE(o.totctb, NULL, o.cases, 0), 0)) AS fzr_case,
         SUM(DECODE(o.categ, ''FREEZER'', o.prod_wt, 0)) AS fzr_wt,
         SUM(DECODE(o.categ, ''FREEZER'', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb), 0)) AS fzr_vol,
         SUM(DECODE(o.categ, ''COOLER'', DECODE(o.totctb, NULL, o.cases, 0), 0)) AS cool_case,
         SUM(DECODE(o.categ, ''COOLER'', o.prod_wt, 0)) AS cool_wt,
         SUM(DECODE(o.categ, ''COOLER'', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb), 0)) AS cool_vol,
         SUM(DECODE(o.categ, ''DRY'', DECODE(o.totctb, NULL, o.cases, 0), 0)) AS dry_case,
         SUM(DECODE(o.categ, ''DRY'', o.prod_wt, 0)) AS dry_wt,
         SUM(DECODE(o.categ, ''DRY'', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb), 0)) AS dry_vol
    FROM o
  GROUP BY o.div_id, o.fin_div_cd, o.div_part, o.cust_id, o.load_num, o.stop_num, o.corp_cd, o.ship_dt
)
SELECT ''RowAction''
       || CHR(9) || ''BillTo''
       || CHR(9) || ''Shipper''
       || CHR(9) || ''Consignee''
       || CHR(9) || ''Earliest''
       || CHR(9) || ''Latest''
       || CHR(9) || ''LoadDate''
       || CHR(9) || ''Source''
       || CHR(9) || ''ExternalIdMcLaneOrder''
       || CHR(9) || ''RollIntoExternalId''
       || CHR(9) || ''RevType1Subsidiary''
       || CHR(9) || ''truncRevType2Division''
       || CHR(9) || ''RevType3ChainCorp''
       || CHR(9) || ''ReferenceType1''
       || CHR(9) || ''ReferenceNumber1''
       || CHR(9) || ''ReferenceType2''
       || CHR(9) || ''ReferenceNumber2''
       || CHR(9) || ''ReferenceType3''
       || CHR(9) || ''ReferenceNumber3''
       || CHR(9) || ''ReferenceType4''
       || CHR(9) || ''ReferenceNumber4''
       || CHR(9) || ''ReferenceType5''
       || CHR(9) || ''ReferenceNumber5''
       || CHR(9) || ''Commodity1''
       || CHR(9) || ''Count1''
       || CHR(9) || ''CountUnit1''
       || CHR(9) || ''Weight1''
       || CHR(9) || ''WeightUnit1''
       || CHR(9) || ''Volume1''
       || CHR(9) || ''VolumeUnit1''
       || CHR(9) || ''Count1A''
       || CHR(9) || ''CountUnit1A''
       || CHR(9) || ''Commodity2''
       || CHR(9) || ''Count2''
       || CHR(9) || ''CountUnit2''
       || CHR(9) || ''Weight2''
       || CHR(9) || ''WeightUnit2''
       || CHR(9) || ''Volume2''
       || CHR(9) || ''VolumeUnit2''
       || CHR(9) || ''Count2A''
       || CHR(9) || ''CountUnit2A''
       || CHR(9) || ''Commodity3''
       || CHR(9) || ''Count3''
       || CHR(9) || ''CountUnit3''
       || CHR(9) || ''Weight3''
       || CHR(9) || ''WeightUnit3''
       || CHR(9) || ''Volume3''
       || CHR(9) || ''VolumeUnit3''
       || CHR(9) || ''Count3A''
       || CHR(9) || ''CountUnit3A''
       || CHR(9) || ''ExtraData1''
       || CHR(9) || ''ExtraData2''
       || CHR(9) || ''ExtraData3''
  FROM dual
UNION ALL
SELECT extr
  FROM (SELECT
               ''A''--''ADD''
               || CHR(9)
               || ''GRO''
               || CHR(9)
               || x.fin_div_cd
               || x.div_id
               || CHR(9)
               || x.cust_id
               || CHR(9)
               || ''1950-01-01''
               || CHR(9)
               || ''2049-12-31''
               || CHR(9)
               || TO_CHAR(x.ship_dt, ''YYYY-MM-DD'')
               || CHR(9)
               || ''GROCERY''
               || CHR(9)
               || x.div_id
               || TO_CHAR(x.ship_dt, ''YYMMDD'')
               || x.cust_id
               || CHR(9)
               || CHR(9)
               || ''GRO''
               || CHR(9)
               || x.fin_div_cd
               || x.div_id
               || CHR(9)
               || x.corp_cd
               || CHR(9)
               || ''ROUTE''
               || CHR(9)
               || x.load_num
               || CHR(9)
               || ''STOP''
               || CHR(9)
               || x.stop_num
               || CHR(9)
               || ''SERIES''
               || CHR(9)
               || ''0000''
               || CHR(9)
               || ''ORDTYP''
               || CHR(9)
               || ''''
               || CHR(9)
               || ''SLOT''
               || CHR(9)
               || ''MAN''
               || CHR(9)
               || ''FREEZER''
               || CHR(9)
               || x.fzr_case
               || CHR(9)
               || ''CAS''
               || CHR(9)
               || x.fzr_wt
               || CHR(9)
               || ''LBS''
               || CHR(9)
               || x.fzr_vol
               || CHR(9)
               || ''CUB''
               || CHR(9)
               || 0
               || CHR(9)
               || ''HAZLBS''
               || CHR(9)
               || ''COOLER''
               || CHR(9)
               || x.cool_case
               || CHR(9)
               || ''CAS''
               || CHR(9)
               || x.cool_wt
               || CHR(9)
               || ''LBS''
               || CHR(9)
               || x.cool_vol
               || CHR(9)
               || ''CUB''
               || CHR(9)
               || 0
               || CHR(9)
               || ''HAZLBS''
               || CHR(9)
               || ''DRY''
               || CHR(9)
               || x.dry_case
               || CHR(9)
               || ''CAS''
               || CHR(9)
               || x.dry_wt
               || CHR(9)
               || ''LBS''
               || CHR(9)
               || x.dry_vol
               || CHR(9)
               || ''CUB''
               || CHR(9)
               || 0
               || CHR(9)
               || ''HAZLBS''
               || CHR(9)
               || '' ''
               || CHR(9)
               || '' ''
               || CHR(9)
               || '' '' AS extr
          FROM x
         ORDER BY x.cust_id, x.ship_dt
       );
SPOOL OFF
quit
EOF
'
        );
    RETURN(l_sql);
  END extract_sql_fn;

  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_CUR_FN
  ||  Return Cursor for TMW Special Dist Routing Extract.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||-----------------------------------------------------------------------------
  */
  FUNCTION extract_cur_fn(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  DATE,
    i_ship_dt_to   IN  DATE,
    i_corp_list    IN  CLOB
  )
    RETURN SYS_REFCURSOR IS
    l_div_part  NUMBER;
    l_cv  SYS_REFCURSOR;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
      WITH oh AS(
        SELECT h.*
          FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, DATE '1900-01-01')) h,
               load_depart_op1f ld, mclp020b cx
         WHERE h.ord_stat = 'O'
           AND h.ord_typ = 'D'
           AND h.ship_dt BETWEEN i_ship_dt_frm AND i_ship_dt_to
           AND ld.div_part = l_div_part
           AND ld.load_depart_sid = h.load_depart_sid
                 AND ld.load_num BETWEEN 'P00P' AND 'P99P'
           AND cx.div_part = l_div_part
           AND cx.custb = h.cust_id
                 AND (   i_corp_list IS NULL
                OR cx.corpb IN(SELECT TO_NUMBER(t.column1)
                                 FROM TABLE(framework.lob2table.separatedcolumns(i_corp_list, '~')) t)
                     )
      ), x AS(
        SELECT   d.div_id, d.fin_div_cd, d.div_part, o.cust_id, o.load_num, o.stop_num, o.corp_cd, 'MAN' AS load_typ,
                 o.ship_dt, SUM(o.case_cnt) AS cases, NVL(SUM(o.tote_cnt), 0) AS tote_cnt, SUM(o.prod_cube) AS prod_cube,
                     SUM(o.prod_wt) AS prod_wt,
                 SUM(DECODE(o.categ, 'FREEZER', DECODE(o.tote_catg, NULL, o.case_cnt, 0), 0)) AS fzr_case,
                     SUM(DECODE(o.categ, 'FREEZER', o.prod_wt, 0)) AS fzr_wt,
                 SUM(DECODE(o.categ, 'FREEZER', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube), 0)
                        ) AS fzr_vol,
                 SUM(DECODE(o.categ, 'COOLER', DECODE(o.tote_catg, NULL, o.case_cnt, 0), 0)) AS cool_case,
                     SUM(DECODE(o.categ, 'COOLER', o.prod_wt, 0)) AS cool_wt,
                 SUM(DECODE(o.categ, 'COOLER', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube), 0)
                        ) AS cool_vol,
                 SUM(DECODE(o.categ, 'DRY', DECODE(o.tote_catg, NULL, o.case_cnt, 0), 0)) AS dry_case,
                     SUM(DECODE(o.categ, 'DRY', o.prod_wt, 0)) AS dry_wt,
                 SUM(DECODE(o.categ, 'DRY', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube), 0)) AS dry_vol
            FROM (SELECT   oh.ship_dt, od.load_num, od.corp_cd, od.cust_id, od.stop_num,
                           (CASE m.catg_typ_cd
                              WHEN 'K' THEN 'COOLER'
                              WHEN 'F' THEN 'FREEZER'
                              ELSE 'DRY'
                            END) AS categ,
                           od.tote_catg, NVL(od.outer_cube, 0) AS outer_cube,
                           NVL(SUM(od.case_cnt), 0) AS case_cnt, SUM(od.tote_cnt) AS tote_cnt,
                           NVL(SUM(od.prod_cube), 0) AS prod_cube, NVL(SUM(od.prod_wt), 0) AS prod_wt
                      FROM oh, load_depart_op1f ld,
                           TABLE(op_load_balance_pk.ord_dtl_fn(
                                   l_div_part,
                                   CURSOR(
                                     SELECT *
                                       FROM oh
                                   ),
                                   'ALL',
                                   'N'
                                  )
                                ) od, mclp210c m
                    WHERE ld.div_part = l_div_part
                      AND ld.load_depart_sid = oh.load_depart_sid
                      AND od.llr_dt = ld.llr_dt
                      AND od.load_num = ld.load_num
                      AND od.cust_id = oh.cust_id
                      AND m.div_part(+) = l_div_part
                      AND m.manctc(+) = od.mfst_catg
                  GROUP BY oh.ship_dt, od.load_num, od.corp_cd, od.cust_id, od.stop_num,
                           (CASE m.catg_typ_cd
                              WHEN 'K' THEN 'COOLER'
                              WHEN 'F' THEN 'FREEZER'
                              ELSE 'DRY'
                            END),
                           od.tote_catg, od.outer_cube
                 ) o, div_mstr_di1d d
           WHERE d.div_part = l_div_part
        GROUP BY d.div_id, d.fin_div_cd, d.div_part, o.cust_id, o.load_num, o.stop_num, o.corp_cd, o.ship_dt
      )
      SELECT 'RowAction'
             || CHR(9)
             || 'BillTo'
             || CHR(9)
             || 'Shipper'
             || CHR(9)
             || 'Consignee'
             || CHR(9)
             || 'Earliest'
             || CHR(9)
             || 'Latest'
             || CHR(9)
             || 'LoadDate'
             || CHR(9)
             || 'Source'
             || CHR(9)
             || 'ExternalIdMcLaneOrder'
             || CHR(9)
             || 'RollIntoExternalId'
             || CHR(9)
             || 'RevType1Subsidiary'
             || CHR(9)
             || 'truncRevType2Division'
             || CHR(9)
             || 'RevType3ChainCorp'
             || CHR(9)
             || 'ReferenceType1'
             || CHR(9)
             || 'ReferenceNumber1'
             || CHR(9)
             || 'ReferenceType2'
             || CHR(9)
             || 'ReferenceNumber2'
             || CHR(9)
             || 'ReferenceType3'
             || CHR(9)
             || 'ReferenceNumber3'
             || CHR(9)
             || 'ReferenceType4'
             || CHR(9)
             || 'ReferenceNumber4'
             || CHR(9)
             || 'ReferenceType5'
             || CHR(9)
             || 'ReferenceNumber5'
             || CHR(9)
             || 'Commodity1'
             || CHR(9)
             || 'Count1'
             || CHR(9)
             || 'CountUnit1'
             || CHR(9)
             || 'Weight1'
             || CHR(9)
             || 'WeightUnit1'
             || CHR(9)
             || 'Volume1'
             || CHR(9)
             || 'VolumeUnit1'
             || CHR(9)
             || 'Count1A'
             || CHR(9)
             || 'CountUnit1A'
             || CHR(9)
             || 'Commodity2'
             || CHR(9)
             || 'Count2'
             || CHR(9)
             || 'CountUnit2'
             || CHR(9)
             || 'Weight2'
             || CHR(9)
             || 'WeightUnit2'
             || CHR(9)
             || 'Volume2'
             || CHR(9)
             || 'VolumeUnit2'
             || CHR(9)
             || 'Count2A'
             || CHR(9)
             || 'CountUnit2A'
             || CHR(9)
             || 'Commodity3'
             || CHR(9)
             || 'Count3'
             || CHR(9)
             || 'CountUnit3'
             || CHR(9)
             || 'Weight3'
             || CHR(9)
             || 'WeightUnit3'
             || CHR(9)
             || 'Volume3'
             || CHR(9)
             || 'VolumeUnit3'
             || CHR(9)
             || 'Count3A'
             || CHR(9)
             || 'CountUnit3A'
             || CHR(9)
             || 'ExtraData1'
             || CHR(9)
             || 'ExtraData2'
             || CHR(9)
             || 'ExtraData3'
        FROM DUAL
      UNION ALL
      SELECT extr
        FROM (SELECT   'A'   --'ADD'
                       || CHR(9)
                       || 'GRO'
                       || CHR(9)
                       || x.fin_div_cd
                       || x.div_id
                       || CHR(9)
                       || x.cust_id
                       || CHR(9)
                       || '1950-01-01'
                       || CHR(9)
                       || '2049-12-31'
                       || CHR(9)
                       || TO_CHAR(x.ship_dt, 'YYYY-MM-DD')
                       || CHR(9)
                       || 'GROCERY'
                       || CHR(9)
                       || x.div_id
                       || TO_CHAR(x.ship_dt, 'YYMMDD')
                       || x.cust_id
                       || CHR(9)
                       || CHR(9)
                       || 'GRO'
                       || CHR(9)
                       || x.fin_div_cd
                       || x.div_id
                       || CHR(9)
                       || x.corp_cd
                       || CHR(9)
                       || 'ROUTE'
                       || CHR(9)
                       || x.load_num
                       || CHR(9)
                       || 'STOP'
                       || CHR(9)
                       || x.stop_num
                       || CHR(9)
                       || 'SERIES'
                       || CHR(9)
                       || '0000'
                       || CHR(9)
                       || 'ORDTYP'
                       || CHR(9)
                       || ''
                       || CHR(9)
                       || 'SLOT'
                       || CHR(9)
                       || 'MAN'
                       || CHR(9)
                       || 'FREEZER'
                       || CHR(9)
                       || x.fzr_case
                       || CHR(9)
                       || 'CAS'
                       || CHR(9)
                       || x.fzr_wt
                       || CHR(9)
                       || 'LBS'
                       || CHR(9)
                       || x.fzr_vol
                       || CHR(9)
                       || 'CUB'
                       || CHR(9)
                       || 0
                       || CHR(9)
                       || 'HAZLBS'
                       || CHR(9)
                       || 'COOLER'
                       || CHR(9)
                       || x.cool_case
                       || CHR(9)
                       || 'CAS'
                       || CHR(9)
                       || x.cool_wt
                       || CHR(9)
                       || 'LBS'
                       || CHR(9)
                       || x.cool_vol
                       || CHR(9)
                       || 'CUB'
                       || CHR(9)
                       || 0
                       || CHR(9)
                       || 'HAZLBS'
                       || CHR(9)
                       || 'DRY'
                       || CHR(9)
                       || x.dry_case
                       || CHR(9)
                       || 'CAS'
                       || CHR(9)
                       || x.dry_wt
                       || CHR(9)
                       || 'LBS'
                       || CHR(9)
                       || x.dry_vol
                       || CHR(9)
                       || 'CUB'
                       || CHR(9)
                       || 0
                       || CHR(9)
                       || 'HAZLBS'
                       || CHR(9)
                       || ' '
                       || CHR(9)
                       || ' '
                       || CHR(9)
                       || ' ' AS extr
                  FROM x
              ORDER BY x.cust_id, x.ship_dt);

    RETURN(l_cv);
  END extract_cur_fn;

  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_FILE_SP
  ||  Create TMW Special Dist Routing Extract file.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 02/21/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE extract_file_sp(
    i_div          IN  VARCHAR2,
    i_file_nm      IN  VARCHAR2,
    i_ship_dt_frm  IN  VARCHAR2,
    i_ship_dt_to   IN  VARCHAR2,
    i_corp_list    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_TMW_DIST_EXTRACT_PK.EXTRACT_FILE_SP';
    lar_parm             logs.tar_parm;
    l_ship_dt_frm        DATE;
    l_ship_dt_to         DATE;
    l_cv                 SYS_REFCURSOR;
    l_t_rpt_lns          typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.add_parm(lar_parm, 'ShipDtFrm', i_ship_dt_frm);
    logs.add_parm(lar_parm, 'ShipDtTo', i_ship_dt_to);
    logs.add_parm(lar_parm, 'CorpList', i_corp_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_ship_dt_frm := TO_DATE(i_ship_dt_frm, 'YYYY-MM-DD');
    l_ship_dt_to := TO_DATE(i_ship_dt_frm, 'YYYY-MM-DD');
    logs.dbg('Get Extract Cursor');
    l_cv := op_tmw_dist_extract_pk.extract_cur_fn(i_div, l_ship_dt_frm, l_ship_dt_to, TO_CLOB(i_corp_list));
    logs.dbg('Fetch Extract Cursor');

    FETCH l_cv
    BULK COLLECT INTO l_t_rpt_lns;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, i_file_nm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extract_file_sp;

  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_SP
  ||  Create and send TMW Special Dist Routing Extract file to TMW system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE extract_sp(
    i_div          IN  VARCHAR2,
    i_ship_dt_frm  IN  DATE,
    i_ship_dt_to   IN  DATE,
    i_corp_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                       := 'OP_TMW_DIST_EXTRACT_PK.EXTRACT_SP';
    lar_parm              logs.tar_parm;
    l_c_prcs_id  CONSTANT VARCHAR2(30)                        := 'TMW_DIST_EXTRACT';
    l_c_sysdate  CONSTANT DATE                                := SYSDATE;
    l_div_part            NUMBER;
    l_appl_srvr           appl_sys_parm_ap1s.vchar_val%TYPE;
    l_file_nm             VARCHAR2(80);
    l_cmd                 typ.t_maxvc2;
    l_os_result           typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ShipDtFrm', i_ship_dt_frm);
    logs.add_parm(lar_parm, 'ShipDtTo', i_ship_dt_to);
    logs.add_parm(lar_parm, 'CorpList', i_corp_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_file_nm := i_div || '_TMW_DIST_EXTR_' || TO_CHAR(l_c_sysdate, 'YYYYMMDDHH24MISS') || '.txt';
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(l_c_prcs_id, op_process_control_pk.g_c_active, i_user_id, l_div_part);
    logs.dbg('OS Command Setup');
    l_cmd := '/local/prodcode/bin/xxopTMWDistExtr.scr '
             || i_div
             || ' '
             || TO_CHAR(i_ship_dt_frm, 'YYYY-MM-DD')
             || ' '
             || TO_CHAR(i_ship_dt_to, 'YYYY-MM-DD')
             || ' '
             || i_corp_list
             || ' '
             || l_file_nm;
    logs.info('Process Command' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info('OS Result' || cnst.newline_char || l_os_result);
    logs.dbg('Log Extract');

    INSERT INTO cust_dist_rte_extr_op6c
                (div_part, ship_dt_frm, ship_dt_to, corp_list, last_chg_ts, user_id
                )
         VALUES (l_div_part, i_ship_dt_frm, i_ship_dt_to, i_corp_list, SYSDATE, SUBSTR(i_user_id, 1, 8)
                );

--    COMMIT;
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(l_c_prcs_id, op_process_control_pk.g_c_inactive, i_user_id, l_div_part);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(l_c_prcs_id,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END extract_sp;

  /*
  ||-----------------------------------------------------------------------------
  || IMPORT_SP
  ||  Get CustDistRteReq for each Div and apply Moves.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/11/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE import_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_DIST_EXTRACT_PK.IMPORT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_create_ts          DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get CreateTs');

    SELECT MAX(r.create_ts)
      INTO l_create_ts
      FROM cust_dist_rte_req_op5c r
     WHERE r.div_part = l_div_part
       AND r.stat_cd = 'OPN';

    IF l_create_ts IS NOT NULL THEN
      logs.dbg('Remove unprocessed previous requests');

      DELETE FROM cust_dist_rte_req_op5c r
            WHERE r.div_part = l_div_part
              AND r.create_ts IN(SELECT   rr.create_ts
                                     FROM cust_dist_rte_req_op5c rr
                                    WHERE rr.div_part = l_div_part
                                      AND rr.stat_cd = 'OPN'
                                      AND rr.create_ts < l_create_ts
                                 GROUP BY rr.create_ts);

      logs.dbg('Process Move');
      move_sp(l_div_part);
      logs.dbg('Process ImportRpt');
      import_rpt_sp(l_div_part, l_create_ts);
    END IF;   -- l_create_ts IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END import_sp;
END op_tmw_dist_extract_pk;
/

