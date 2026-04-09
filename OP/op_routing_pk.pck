CREATE OR REPLACE PACKAGE op_routing_pk IS
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

  FUNCTION tmw_dist_extr_sw_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION unassigned_ship_dates_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION corp_cur_fn(
    i_div          IN  VARCHAR2,
    i_from_shp_dt  IN  VARCHAR2,
    i_to_shp_dt    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION routing_sum_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION routing_dtl_cur_fn(
    i_div     IN  VARCHAR2,
    i_shp_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stat_sum_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stat_dtl_cur_fn(
    i_div        IN  VARCHAR2,
    i_rte_grp    IN  VARCHAR2,
    i_create_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION file_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION tmw_extract_cur_fn(
    i_div          IN  VARCHAR2,
    i_create_dt    IN  DATE,
    i_cancel_sw    IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE tmw_extract_file_sp(
    i_div          IN  VARCHAR2,
    i_create_dt    IN  DATE,
    i_cancel_sw    IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE stage_sp(
    i_div          IN  VARCHAR2,
    i_from_shp_dt  IN  VARCHAR2,
    i_to_shp_dt    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE stage_for_routing_sp(
    i_div          IN  VARCHAR2,
    i_from_shp_dt  IN  VARCHAR2,
    i_to_shp_dt    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE cancel_stage_sp(
    i_div          IN  VARCHAR2,
    i_shp_dt_list  IN  VARCHAR2
  );

  PROCEDURE send_routing_sp(
    i_div          IN  VARCHAR2,
    i_shp_dt_list  IN  VARCHAR2,
    i_rte_grp      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE cancel_sp(
    i_div        IN  VARCHAR2,
    i_rte_grp    IN  VARCHAR2,
    i_create_dt  IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE import_sp(
    i_div          IN  VARCHAR2,
    i_rmt_file_nm  IN  VARCHAR2
  );

  PROCEDURE process_routed_sp(
    i_div            IN  VARCHAR2,
    i_local_file_nm  IN  VARCHAR2,
    i_rmt_file_nm    IN  VARCHAR2
  );
END op_routing_pk;
/

CREATE OR REPLACE PACKAGE BODY op_routing_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_tt_prbs IS TABLE OF VARCHAR2(4000);

  TYPE g_rt_import IS RECORD(
    div         div_mstr_di1d.div_id%TYPE,
    rte_grp     rte_grp_rt2g.rte_grp%TYPE,
    create_dt   DATE,
    mcl_cust    rte_stat_rt1s.mcl_cust%TYPE,
    orig_stop   NUMBER,
    new_load    rte_stat_rt1s.new_load_num%TYPE,
    new_stop    NUMBER,
    new_eta_dt  DATE
  );

  TYPE g_tt_import IS TABLE OF g_rt_import;

  g_c_stat_sent   CONSTANT VARCHAR2(3) := 'SNT';
  g_c_stat_cancl  CONSTANT VARCHAR2(3) := 'CAN';
  g_c_stat_work   CONSTANT VARCHAR2(3) := 'WRK';
  g_c_stat_err    CONSTANT VARCHAR2(3) := 'ERR';
  g_c_stat_cmplt  CONSTANT VARCHAR2(3) := 'CMP';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || STOP_FOR_CUST_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to include Routing Group. PIR5882
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_for_cust_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.STOP_FOR_CUST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_stop_num           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor for Existing Stop');

    OPEN l_cv
     FOR
       SELECT se.stop_num
         FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx
        WHERE d.div_id = i_div
          AND ld.div_part = d.div_part
          AND ld.llr_ts = DATE '1900-01-01'
          AND ld.load_num = 'ROUT'
          AND se.div_part = ld.div_part
          AND se.load_depart_sid = ld.load_depart_sid
          AND se.cust_id = i_cust_id
          AND cx.div_part = d.div_part
          AND cx.custb = i_cust_id
          AND EXISTS(SELECT 1
                       FROM ordp100a a
                      WHERE a.div_part = se.div_part
                        AND a.load_depart_sid = se.load_depart_sid
                        AND a.custa = se.cust_id
                        AND a.excptn_sw = 'N'
                        AND a.stata = 'O'
                        AND a.dsorda = 'D'
                        AND NOT EXISTS(SELECT 1
                                         FROM rte_stat_rt1s r, rte_grp_rt2g g
                                        WHERE g.div_part = d.div_part
                                          AND r.rte_grp_num = g.rte_grp_num
                                          AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                          AND r.stop_num = se.stop_num
                                          AND r.mcl_cust = cx.mccusb
                                          AND r.stat_cd IN(g_c_stat_sent, g_c_stat_work)));

    logs.dbg('Fetch Cursor for Existing Stop');

    FETCH l_cv
     INTO l_stop_num;

    IF l_cv%NOTFOUND THEN
      logs.dbg('Open Cursor for First Available Stop');

      OPEN l_cv
       FOR
         SELECT MIN(y.column_value)
           FROM (SELECT s.column_value
                   FROM TABLE(pivot_fn(10000, 0)) s,
                        (SELECT se.stop_num
                           FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se
                          WHERE d.div_id = i_div
                            AND ld.div_part = d.div_part
                            AND ld.llr_ts = DATE '1900-01-01'
                            AND ld.load_num = 'ROUT'
                            AND se.div_part = ld.div_part
                            AND se.load_depart_sid = ld.load_depart_sid
                            AND EXISTS(SELECT 1
                                         FROM ordp100a a
                                        WHERE a.div_part = se.div_part
                                          AND a.load_depart_sid = se.load_depart_sid
                                          AND a.custa = se.cust_id
                                          AND a.excptn_sw = 'N'
                                          AND a.stata = 'O'
                                          AND a.dsorda = 'D')) x
                  WHERE x.stop_num(+) = s.column_value
                    AND x.stop_num IS NULL) y;

      logs.dbg('Fetch Cursor for First Available Stop');

      FETCH l_cv
       INTO l_stop_num;
    END IF;   -- l_cv%NOTFOUND

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_stop_num);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_for_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || NOTIFY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/27/08 | rhalpai | Changed to handle large email notifications. PIR5882
  ||----------------------------------------------------------------------------
  */
  PROCEDURE notify_sp(
    i_div      IN             VARCHAR2,
    io_t_prbs  IN OUT NOCOPY  g_tt_prbs
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm  := 'OP_ROUTING_PK.NOTIFY_SP';
    lar_parm                logs.tar_parm;
    l_c_prcs_id    CONSTANT VARCHAR2(30)   := 'ROUTING';
    l_c_mail_subj  CONSTANT VARCHAR2(30)   := i_div || ' OP Routing Problems';
    l_mail_msg              VARCHAR2(4000);

    TYPE l_tt_mail_msg IS TABLE OF VARCHAR2(4000);

    l_t_mail_msg            l_tt_mail_msg  := l_tt_mail_msg('', '', '', '', '');
    l_msg_idx               PLS_INTEGER    := 1;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    IF io_t_prbs IS NOT NULL THEN
      logs.dbg('Build Mail Msg');
      FOR i IN io_t_prbs.FIRST .. io_t_prbs.LAST LOOP
        l_mail_msg := SUBSTR(io_t_prbs(i), 1, 3999);

        BEGIN
          l_t_mail_msg(l_msg_idx) := l_t_mail_msg(l_msg_idx) || l_mail_msg || cnst.newline_char;
        EXCEPTION
          WHEN VALUE_ERROR THEN
            IF l_msg_idx < l_t_mail_msg.COUNT THEN
              l_msg_idx := l_msg_idx + 1;

              BEGIN
                l_t_mail_msg(l_msg_idx) := l_t_mail_msg(l_msg_idx) || l_mail_msg || cnst.newline_char;
              EXCEPTION
                WHEN VALUE_ERROR THEN
                  NULL;
              END;
            END IF;   -- l_msg_idx < l_t_mail_msg.COUNT
        END;
      END LOOP;
      logs.dbg('Notify Group');
      op_process_common_pk.notify_group_sp(i_div,
                                           l_c_prcs_id,
                                           l_c_mail_subj,
                                           l_t_mail_msg(1),
                                           l_t_mail_msg(2),
                                           l_t_mail_msg(3),
                                           l_t_mail_msg(4),
                                           l_t_mail_msg(5)
                                          );
    END IF;   -- io_t_prbs IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END notify_sp;

  /*
  ||----------------------------------------------------------------------------
  || REASSIGN_ORDS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/09/08 | rhalpai | Original. PIR5882
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reassign_ords_sp(
    i_div     IN  VARCHAR2,
    i_t_ords  IN  type_ntab
  ) IS
    l_div_part  NUMBER;
  BEGIN
    IF (    i_t_ords IS NOT NULL
        AND i_t_ords.COUNT > 0) THEN
      l_div_part := div_pk.div_part_fn(i_div);
      op_order_load_pk.syncload_sp(l_div_part, 'RTECANCL', i_t_ords, 'ROUTING');
    END IF;   -- i_t_ords IS NOT NULL AND i_t_ords.COUNT > 0
  END reassign_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_SENT_ORDS_SP
  ||  Create and send Delete transactions for cancels for TMW when TMWDistExtrSw is Y.
  ||  Move Orders for Cancel Sent Entries Back to their Default Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/14/26 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_sent_ords_sp(
    i_div         IN  VARCHAR2,
    i_rte_grp     IN  VARCHAR2,
    i_create_dt   IN  DATE
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.REASSIGN_ORDS_SP';
    lar_parm              logs.tar_parm;
    l_t_ords              type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Orders for Cancel Sent');

    SELECT a.ordnoa AS ord_num
    BULK COLLECT INTO l_t_ords
      FROM div_mstr_di1d d, load_depart_op1f ld, rte_grp_rt2g g, rte_grp_ord_rt3o o, rte_stat_rt1s r, ordp100a a,
           stop_eta_op1g se, mclp020b cx
     WHERE d.div_id = i_div
       AND ld.div_part = d.div_part
       AND ld.llr_ts = DATE '1900-01-01'
       AND ld.load_num = 'ROUT'
       AND g.div_part = d.div_part
       AND g.rte_grp = i_rte_grp
       AND g.create_dt = i_create_dt
       AND o.rte_grp_num = g.rte_grp_num
       AND r.rte_grp_num = g.rte_grp_num
       AND r.stat_cd = g_c_stat_cancl
       AND a.div_part = ld.div_part
       AND a.ordnoa = o.ord_num
       AND a.load_depart_sid = ld.load_depart_sid
       AND a.excptn_sw = 'N'
       AND a.stata = 'O'
       AND a.dsorda = 'D'
       AND a.shpja = r.shp_dt - DATE '1900-02-28'
       AND se.div_part = ld.div_part
       AND se.load_depart_sid = ld.load_depart_sid
       AND se.cust_id = a.custa
       AND se.stop_num = r.stop_num
       AND cx.div_part = se.div_part
       AND cx.custb = se.cust_id
       AND cx.mccusb = r.mcl_cust;

    IF l_t_ords.COUNT > 0 THEN
      IF tmw_dist_extr_sw_fn(i_div) = 'Y' THEN
        logs.dbg('TMW Extract File - Del Transactions');
        tmw_extract_file_sp(i_div, i_create_dt, 'Y');
      END IF;   -- tmw_dist_extr_sw_fn(i_div) = 'Y'

      logs.dbg('Reassign Orders');
      reassign_ords_sp(i_div, l_t_ords);
    END IF;   -- l_t_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancel_sent_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || EXORT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 05/21/08 | rhalpai | Changed to execute /local/prodcode/bin/XXOPRoutingExport.sub
  ||                    | with Div/LocalFile parms via oscmd_fn. PIR5819
  || 05/13/13 | rhalpai | Change logic to call xxopRoutingExport.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Replace
  ||                    | op_parms_pk.get_val_fn with op_parms_pk.val_fn. PIR14660
  || 10/17/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE export_sp(
    i_div            IN  VARCHAR2,
    i_local_file_nm  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_ROUTING_PK.EXPORT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                VARCHAR2(2000);
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LocalFileNm', i_local_file_nm);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_cmd := '/local/prodcode/bin/xxopRoutingExport.sub "' || i_div || '" "' || i_local_file_nm || '"';
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END export_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_STAT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group and to move orders
  ||                    | for cancels back to their default loads. PIR5882
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 02/27/17 | rhalpai | Add upd_cnt out parm. SDHD-94141
  || 01/14/26 | rhalpai | Remove logic for call to REASSIGN_ORDS_SP. PIR18901
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_stat_sp(
    i_div         IN  VARCHAR2,
    i_rte_grp     IN  VARCHAR2,
    i_create_dt   IN  DATE,
    i_stop_num    IN  NUMBER,
    i_mcl_cust    IN  VARCHAR2,
    i_stat_cd     IN  VARCHAR2,
    i_user_id     IN  VARCHAR2,
    o_upd_cnt     OUT NUMBER,
    i_end_dt      IN  DATE DEFAULT NULL,
    i_new_llr_dt  IN  DATE DEFAULT NULL,
    i_new_load    IN  VARCHAR2 DEFAULT NULL,
    i_new_stop    IN  NUMBER DEFAULT NULL,
    i_new_eta_dt  IN  DATE DEFAULT NULL
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.UPD_STAT_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'StatCd', i_stat_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'NewLLRDt', i_new_llr_dt);
    logs.add_parm(lar_parm, 'NewLoad', i_new_load);
    logs.add_parm(lar_parm, 'NewStop', i_new_stop);
    logs.add_parm(lar_parm, 'NewEtaDt', i_new_eta_dt);
    logs.dbg('ENTRY', lar_parm);

    IF i_stat_cd IN(g_c_stat_cancl, g_c_stat_work) THEN
      logs.dbg('Change Route Status to Cancel/Work');

      UPDATE rte_stat_rt1s r
         SET r.stat_cd = i_stat_cd,
             r.end_dt = DECODE(i_stat_cd, g_c_stat_cancl, i_end_dt, r.end_dt),
             r.last_chg_ts = l_c_sysdate,
             r.user_id = i_user_id
       WHERE r.rte_grp_num = (SELECT g.rte_grp_num
                                FROM div_mstr_di1d d, rte_grp_rt2g g
                               WHERE d.div_id = i_div
                                 AND g.div_part = d.div_part
                                 AND g.rte_grp = i_rte_grp
                                 AND g.create_dt = i_create_dt)
         AND r.stop_num = NVL(i_stop_num, r.stop_num)
         AND r.mcl_cust = NVL(i_mcl_cust, r.mcl_cust)
         AND r.stat_cd = g_c_stat_sent;

      o_upd_cnt := SQL%ROWCOUNT;
    ELSIF i_stat_cd IN(g_c_stat_cmplt, g_c_stat_err) THEN
      logs.dbg('Change Route Status to Complete/Error');

      UPDATE rte_stat_rt1s r
         SET r.stat_cd = i_stat_cd,
             r.new_llr_dt = i_new_llr_dt,
             r.new_load_num = i_new_load,
             r.new_stop_num = i_new_stop,
             r.new_eta_dt = i_new_eta_dt,
             r.end_dt = i_end_dt,
             r.last_chg_ts = l_c_sysdate,
             r.user_id = i_user_id
       WHERE r.rte_grp_num = (SELECT g.rte_grp_num
                                FROM div_mstr_di1d d, rte_grp_rt2g g
                               WHERE d.div_id = i_div
                                 AND g.div_part = d.div_part
                                 AND g.rte_grp = i_rte_grp
                                 AND g.create_dt = i_create_dt)
         AND r.stop_num = i_stop_num
         AND r.mcl_cust = i_mcl_cust
         AND r.stat_cd = g_c_stat_work;

      o_upd_cnt := SQL%ROWCOUNT;
    END IF;   -- i_stat_cd IN(g_c_stat_cancl, g_c_stat_work)

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_sp(
    i_file_nm   IN      VARCHAR2,
    o_t_import  OUT     g_tt_import
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm      := 'OP_ROUTING_PK.PARSE_SP';
    lar_parm               logs.tar_parm;
    l_file_handle          UTL_FILE.file_type;
    l_c_file_dir  CONSTANT VARCHAR2(50)       := '/ftptrans';
    l_buffer               typ.t_maxvc2;
    l_r_import             g_rt_import;
    l_r_import_empty       g_rt_import;
  BEGIN
    o_t_import := g_tt_import();
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.dbg('Open File');
    l_file_handle := UTL_FILE.fopen(l_c_file_dir, i_file_nm, 'r');
    <<read_loop>>
    LOOP
      BEGIN
        logs.dbg('Get Line');
        UTL_FILE.get_line(l_file_handle, l_buffer);
        logs.dbg('Add to Table');
        o_t_import.EXTEND;
        l_r_import := l_r_import_empty;
        l_r_import.div := SUBSTR(l_buffer, 1, 2);
        l_r_import.rte_grp := TRIM(SUBSTR(l_buffer, 5, 25));
        l_r_import.create_dt := TO_DATE(SUBSTR(l_buffer, 30, 14), 'YYYYMMDDHH24MISS');
        l_r_import.mcl_cust := SUBSTR(l_buffer, 44, 6);
        l_r_import.orig_stop := TO_NUMBER(SUBSTR(l_buffer, 50, 4));
        l_r_import.new_load := SUBSTR(l_buffer, 54, 4);
        l_r_import.new_stop := TO_NUMBER(SUBSTR(l_buffer, 58, 2));
        l_r_import.new_eta_dt := TO_DATE(SUBSTR(l_buffer, 60, 13), 'YYYYMMDDHH24:MI');
        o_t_import(o_t_import.LAST) := l_r_import;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          EXIT read_loop;
      END;
    END LOOP read_loop;
    logs.dbg('Close File');
    UTL_FILE.fclose(l_file_handle);
  EXCEPTION
    WHEN OTHERS THEN
      o_t_import := g_tt_import();

      IF UTL_FILE.is_open(l_file_handle) THEN
        UTL_FILE.fclose(l_file_handle);
      END IF;   -- UTL_FILE.is_open(l_file_handle)

      logs.warn(SQLERRM, lar_parm);
  END parse_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_ORDERS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_orders_sp(
    i_div         IN      VARCHAR2,
    i_rte_grp     IN      VARCHAR2,
    i_create_dt   IN      DATE,
    i_stop_num    IN      NUMBER,
    i_mcl_cust    IN      VARCHAR2,
    i_new_load    IN      VARCHAR2,
    i_new_stop    IN      NUMBER,
    i_new_eta_dt  IN      DATE,
    o_msg         OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ROUTING_PK.MOVE_ORDERS_SP';
    lar_parm             logs.tar_parm;
    l_cust_id            sysp200c.acnoc%TYPE;
    l_ord_list           typ.t_maxvc2;
    l_new_eta_dt         VARCHAR2(10);
    l_new_eta_tm         NUMBER;

    CURSOR l_cur_ords(
      b_div        VARCHAR2,
      b_rte_grp    VARCHAR2,
      b_create_dt  DATE,
      b_stop_num   NUMBER,
      b_mcl_cust   VARCHAR2
    ) IS
      SELECT a.ordnoa AS ord_num, se.cust_id
        FROM div_mstr_di1d d, mclp020b cx, load_depart_op1f ld, rte_grp_rt2g g, rte_grp_ord_rt3o o, rte_stat_rt1s r,
             ordp100a a, stop_eta_op1g se
       WHERE d.div_id = b_div
         AND cx.div_part = d.div_part
         AND cx.mccusb = b_mcl_cust
         AND ld.div_part = d.div_part
         AND ld.llr_ts = DATE '1900-01-01'
         AND ld.load_num = 'ROUT'
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = cx.custb
         AND se.stop_num = b_stop_num
         AND g.div_part = d.div_part
         AND g.rte_grp = b_rte_grp
         AND g.create_dt = b_create_dt
         AND o.rte_grp_num = g.rte_grp_num
         AND r.rte_grp_num = g.rte_grp_num
         AND r.stop_num = se.stop_num
         AND r.mcl_cust = cx.mccusb
         AND r.stat_cd = g_c_stat_work
         AND a.div_part = se.div_part
         AND a.ordnoa = o.ord_num
         AND a.load_depart_sid = se.load_depart_sid
         AND a.custa = se.cust_id
         AND a.excptn_sw = 'N'
         AND a.stata = 'O'
         AND a.dsorda = 'D'
         AND a.shpja = r.shp_dt - DATE '1900-02-28';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'NewLoad', i_new_load);
    logs.add_parm(lar_parm, 'NewStop', i_new_stop);
    logs.add_parm(lar_parm, 'NewEtaDt', i_new_eta_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_new_eta_dt := TO_CHAR(i_new_eta_dt, 'YYYY-MM-DD');
    l_new_eta_tm := TO_NUMBER(TO_CHAR(i_new_eta_dt, 'HH24MI'));
    logs.dbg('Build Order List');
    FOR l_r_ord IN l_cur_ords(i_div, i_rte_grp, i_create_dt, i_stop_num, i_mcl_cust) LOOP
      BEGIN
        l_cust_id := l_r_ord.cust_id;
        l_ord_list := l_ord_list || l_r_ord.ord_num || '`';
      EXCEPTION
        WHEN VALUE_ERROR THEN
          NULL;
      END;
    END LOOP;

    IF l_ord_list IS NULL THEN
      o_msg := 'E~Orders not found.';
    ELSE
      logs.dbg('Move Orders');
      op_order_moves_pk.move_orders_sp(i_div,
                                       l_cust_id,
                                       i_new_load,
                                       i_new_stop,
                                       l_new_eta_dt,
                                       l_new_eta_tm,
                                       l_ord_list,
                                       'QROUTE',
                                       o_msg,
                                       'QROUTE'
                                      );
    END IF;   -- l_ord_list IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_orders_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_PROBLEM_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_problem_sp(
    io_t_prbs     IN OUT NOCOPY  g_tt_prbs,
    i_prb         IN             VARCHAR2,
    i_rte_grp     IN             VARCHAR2,
    i_create_dt   IN             DATE,
    i_stop_num    IN             NUMBER,
    i_mcl_cust    IN             VARCHAR2,
    i_new_load    IN             VARCHAR2,
    i_new_stop    IN             NUMBER,
    i_new_eta_dt  IN             DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.ADD_PROBLEM_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'Prb', i_prb);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'NewLoad', i_new_load);
    logs.add_parm(lar_parm, 'NewStop', i_new_stop);
    logs.add_parm(lar_parm, 'NewEtaDt', i_new_eta_dt);
    logs.dbg('Initialize');

    IF io_t_prbs IS NULL THEN
      io_t_prbs := g_tt_prbs();
    END IF;   -- io_t_prbs IS NULL

    logs.dbg('Add to Table');
    io_t_prbs.EXTEND;
    io_t_prbs(io_t_prbs.LAST) := i_prb
                                 || cnst.newline_char
                                 || 'RteGrp: '
                                 || i_rte_grp
                                 || ' CreateDt: '
                                 || TO_CHAR(i_create_dt, 'YYYYMMDDHH24MISS')
                                 || ' OrigStop: '
                                 || LPAD(i_stop_num, 4, '0')
                                 || ' Cust: '
                                 || i_mcl_cust
                                 || ' NewLoad: '
                                 || i_new_load
                                 || ' NewStop: '
                                 || LPAD(i_new_stop, 2, '0')
                                 || ' NewETA: '
                                 || TO_CHAR(i_new_eta_dt, 'YYYYMMDDHH24MI')
                                 || cnst.newline_char;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_problem_sp;

  /*
  ||----------------------------------------------------------------------------
  || ARCHIVE_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 04/04/08 | rhalpai | Changed to handle file names containing spaces. PIR5882
  || 06/12/08 | rhalpai | Change logic to ssh from app server when running OS command. SDHD-505263
  || 10/17/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  || 01/14/26 | rhalpai | Add logic to handle TMWDist. PIR18901
  ||----------------------------------------------------------------------------
  */
  PROCEDURE archive_sp(
    i_local_file_nm  IN  VARCHAR2,
    i_tmw_dir_sw     IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm                       := 'OP_ROUTING_PK.ARCHIVE_SP';
    lar_parm                  logs.tar_parm;
    l_appl_srvr               appl_sys_parm_ap1s.vchar_val%TYPE;
    l_c_file_dir     CONSTANT VARCHAR2(50)                        := '/ftptrans';
    l_c_archive_dir  CONSTANT VARCHAR2(50)                        := '/ftptrans/transmitted_files';
    l_cmd                     VARCHAR2(2000);
    l_os_result               VARCHAR2(2000);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LocalFileNm', i_local_file_nm);
    logs.add_parm(lar_parm, 'TmwDirSw', i_tmw_dir_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_appl_srvr := op_parms_pk.val_fn(0, op_const_pk.prm_appl_srvr);
    IF i_tmw_dir_sw = 'Y' THEN
      l_cmd := 'ssh -q '
               || l_appl_srvr
               || ' ''cd /TMW/'
               || (CASE l_appl_srvr WHEN 'popsap01' THEN 'Prod' ELSE 'Test' END)
               || cnst.newline_char
               || 'mv -f "'
               || i_local_file_nm
               || '" ./Archive"''';
    ELSE
      l_cmd := 'ssh -q '
               || l_appl_srvr
               || ' ''cd '
               || l_c_file_dir
               || cnst.newline_char
               || 'zip -m -9 "'
               || i_local_file_nm
               || '.zip" "'
               || i_local_file_nm
               || '"'
               || cnst.newline_char
               || 'mv -f "'
               || i_local_file_nm
               || '.zip" '
               || l_c_archive_dir
               || '''';
    END IF;   -- i_tmw_dir_sw = 'Y'
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd);
    logs.info(l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END archive_sp;

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
  || 01/14/26 | rhalpai | Original. PIR18901
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
  ||----------------------------------------------------------------------------
  || TMW_DIST_EXTR_SW_FN
  ||  Returns Y/N whether Div is turned ON for TMW_DIST_EXTR_DIV.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/14/26 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION tmw_dist_extr_sw_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.TMW_DIST_EXTR_SW_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_sw                 VARCHAR2(1);
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_sw := NVL(op_parms_pk.val_fn(l_div_part, 'TMW_DIST_EXTR_DIV_' || i_div), 'N');
    RETURN(l_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tmw_dist_extr_sw_fn;

  /*
  ||----------------------------------------------------------------------------
  || UNASSIGNED_SHIP_DATES_CUR_FN
  ||  Returns cursor of ship dates for special (P%%) distribution orders on
  ||  their default loads (P%%P).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 06/16/08 | rhalpai | Added sort by ShipDt to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION unassigned_ship_dates_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.UNASSIGNED_SHIP_DATES_CUR_FN';
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
  || 03/27/08 | rhalpai | Original. PIR5882
  || 06/16/08 | rhalpai | Added sort by CorpCd to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION corp_cur_fn(
    i_div          IN  VARCHAR2,
    i_from_shp_dt  IN  VARCHAR2,
    i_to_shp_dt    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.CORP_CUR_FN';
    lar_parm             logs.tar_parm;
    l_from_shp_dt        NUMBER;
    l_to_shp_dt          NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FromShpDt', i_from_shp_dt);
    logs.add_parm(lar_parm, 'ToShpDt', i_to_shp_dt);
    logs.dbg('ENTRY', lar_parm);
    l_from_shp_dt := TO_DATE(i_from_shp_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_to_shp_dt := TO_DATE(i_to_shp_dt, 'YYYY-MM-DD') - DATE '1900-02-28';

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
  || ROUTING_SUM_CUR_FN
  ||  Returns cursor of summarized info by ship date of orders on route staging
  ||  load (ROUT).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Removed restriction for special distributions. PIR5882
  || 06/16/08 | rhalpai | Added sort by ShipDt to cursor.
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 10/19/10 | rhalpai | Changed cursor to use cube from CorpItem table,
  ||                    | SAWP505E, instead of from OrderDetail table. PIR5878
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers. PIR14660
  ||----------------------------------------------------------------------------
  */
  FUNCTION routing_sum_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.ROUTING_SUM_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + x.shpja, 'YYYY-MM-DD') AS shp_dt, COUNT(DISTINCT x.stop_num) AS stop_cnt,
                CEIL(SUM(x.prod_wt)) AS orig_wt,
                CEIL(SUM(DECODE(x.tote_cnt, NULL, x.prod_cube, x.tote_cnt * x.outerb))) AS orig_cube
           FROM (SELECT   a.shpja, se.stop_num, ct.outerb,
                          DECODE(ct.boxb,
                                 'N', DECODE(ct.pccntb,
                                             'Y', CEIL(SUM(b.ordqtb) / ct.totcnb),
                                             'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                                       / DECODE(ct.innerb, NULL, .000001, 0, .000001, ct.innerb)
                                                      )
                                            )
                                ) AS tote_cnt,
                          SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube, SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
                     FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, sysp200c c, mclp100a g,
                          ordp100a a, ordp120b b, sawp505e e, mclp200b ct
                    WHERE d.div_id = i_div
                      AND ld.div_part = d.div_part
                      AND ld.llr_ts = DATE '1900-01-01'
                      AND ld.load_num = 'ROUT'
                      AND se.div_part = ld.div_part
                      AND se.load_depart_sid = ld.load_depart_sid
                      AND cx.div_part = se.div_part
                      AND cx.custb = se.cust_id
                      AND c.div_part = se.div_part
                      AND c.acnoc = se.cust_id
                      AND g.div_part = c.div_part
                      AND g.cstgpa = c.retgpc
                      AND a.div_part = se.div_part
                      AND a.load_depart_sid = se.load_depart_sid
                      AND a.custa = se.cust_id
                      AND a.dsorda = 'D'
                      AND a.stata = 'O'
                      AND a.excptn_sw = 'N'
                      AND a.ipdtsa NOT IN(SELECT s.ord_src
                                            FROM div_mstr_di1d dv, sub_prcs_ord_src s
                                           WHERE dv.div_id = i_div
                                             AND s.div_part = dv.div_part
                                             AND s.prcs_id = 'LOAD BALANCE'
                                             AND s.prcs_sbtyp_cd = 'BLB')
                      AND NOT EXISTS(SELECT 1
                                       FROM rte_grp_rt2g rg, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                      WHERE rg.div_part = d.div_part
                                        AND r.rte_grp_num = rg.rte_grp_num
                                        AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                        AND r.stop_num = se.stop_num
                                        AND r.mcl_cust = cx.mccusb
                                        AND r.stat_cd IN(g_c_stat_sent, g_c_stat_work)
                                        AND o.rte_grp_num = rg.rte_grp_num
                                        AND o.ord_num = a.ordnoa)
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb = 'O'
                      AND b.subrcb < 999
                      AND b.ordqtb > 0
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb
                      AND ct.div_part(+) = b.div_part
                      AND ct.totctb(+) = b.totctb
                 GROUP BY a.shpja, se.stop_num, DECODE(g.cntnr_trckg_sw, 'N', NULL, 'Y', RTRIM(REPLACE(a.cpoa, '0'))),
                          b.manctb, b.totctb, ct.outerb, ct.innerb, ct.boxb, ct.pccntb, ct.totcnb) x
       GROUP BY x.shpja
       ORDER BY x.shpja;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END routing_sum_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ROUTING_DTL_CUR_FN
  ||  Returns cursor of stop detail info for ship date of orders on route
  ||  staging load (ROUT).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Removed restriction for special distributions. PIR5882
  || 06/16/08 | rhalpai | Added sort by Stop/Cust to cursor.
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 10/19/10 | rhalpai | Changed cursor to use cube from CorpItem table,
  ||                    | SAWP505E, instead of from OrderDetail table. PIR5878
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers. PIR14660
  ||----------------------------------------------------------------------------
  */
  FUNCTION routing_dtl_cur_fn(
    i_div     IN  VARCHAR2,
    i_shp_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.ROUTING_DTL_CUR_FN';
    lar_parm             logs.tar_parm;
    l_shp_dt             NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ShpDt', i_shp_dt);
    logs.dbg('ENTRY', lar_parm);
    l_shp_dt := TO_DATE(i_shp_dt, 'YYYY-MM-DD') - DATE '1900-02-28';

    OPEN l_cv
     FOR
       SELECT   x.stop_num, x.mcl_cust, x.namec, x.shpctc, x.shpstc, CEIL(SUM(x.prod_wt)) AS orig_wt,
                CEIL(SUM(DECODE(x.tote_cnt, NULL, x.prod_cube, x.tote_cnt * x.outerb))) AS orig_cube
           FROM (SELECT   se.stop_num, cx.mccusb AS mcl_cust, c.namec, c.shpctc, c.shpstc, ct.outerb,
                          DECODE(ct.boxb,
                                 'N', DECODE(ct.pccntb,
                                             'Y', CEIL(SUM(b.ordqtb) / ct.totcnb),
                                             'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                                       / DECODE(ct.innerb, NULL, .000001, 0, .000001, ct.innerb)
                                                      )
                                            )
                                ) AS tote_cnt,
                          SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube, SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
                     FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, mclp020b cx, sysp200c c,
                          mclp100a g, ordp120b b, sawp505e e, mclp200b ct
                    WHERE d.div_id = i_div
                      AND ld.div_part = d.div_part
                      AND ld.llr_ts = DATE '1900-01-01'
                      AND ld.load_num = 'ROUT'
                      AND a.div_part = ld.div_part
                      AND a.load_depart_sid = ld.load_depart_sid
                      AND a.shpja = l_shp_dt
                      AND a.dsorda = 'D'
                      AND a.stata = 'O'
                      AND a.excptn_sw = 'N'
                      AND a.ipdtsa NOT IN(SELECT s.ord_src
                                            FROM div_mstr_di1d dv, sub_prcs_ord_src s
                                           WHERE dv.div_id = i_div
                                             AND s.div_part = dv.div_part
                                             AND s.prcs_id = 'LOAD BALANCE'
                                             AND s.prcs_sbtyp_cd = 'BLB')
                      AND se.div_part = a.div_part
                      AND se.load_depart_sid = a.load_depart_sid
                      AND se.cust_id = a.custa
                      AND cx.div_part = se.div_part
                      AND cx.custb = se.cust_id
                      AND NOT EXISTS(SELECT 1
                                       FROM rte_grp_rt2g rg, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                      WHERE rg.div_part = d.div_part
                                        AND r.rte_grp_num = rg.rte_grp_num
                                        AND r.shp_dt = DATE '1900-02-28' + l_shp_dt
                                        AND r.stop_num = se.stop_num
                                        AND r.mcl_cust = cx.mccusb
                                        AND r.stat_cd IN(g_c_stat_sent, g_c_stat_work)
                                        AND o.rte_grp_num = rg.rte_grp_num
                                        AND o.ord_num = a.ordnoa)
                      AND c.div_part = a.div_part
                      AND c.acnoc = a.custa
                      AND g.div_part = c.div_part
                      AND g.cstgpa = c.retgpc
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb = 'O'
                      AND b.subrcb < 999
                      AND b.ordqtb > 0
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb
                      AND ct.div_part(+) = b.div_part
                      AND ct.totctb(+) = b.totctb
                 GROUP BY se.stop_num, cx.mccusb, c.namec, c.shpctc, c.shpstc,
                          DECODE(g.cntnr_trckg_sw, 'N', NULL, 'Y', RTRIM(REPLACE(a.cpoa, '0'))), b.manctb, b.totctb,
                          ct.outerb, ct.innerb, ct.boxb, ct.pccntb, ct.totcnb) x
       GROUP BY x.stop_num, x.mcl_cust, x.namec, x.shpctc, x.shpstc
       ORDER BY x.stop_num, x.mcl_cust;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END routing_dtl_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || STAT_SUM_CUR_FN
  ||  Returns cursor of summarized routing status info by ShipDate/CreateDate.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to include Routing Group. PIR5882
  || 06/16/08 | rhalpai | Added sort by RteGrp to cursor.
  ||----------------------------------------------------------------------------
  */
  FUNCTION stat_sum_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.STAT_SUM_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   g.rte_grp, COUNT(r.stop_num) AS stop_cnt, SUM(r.orig_wt) AS orig_wt, SUM(r.orig_cube) AS orig_cube,
                TO_CHAR(g.create_dt, 'YYYY-MM-DD HH24:MI:SS') AS create_dt, r.create_user_id,
                to_list_fn(CURSOR(SELECT   r2.stat_cd
                                      FROM rte_stat_rt1s r2
                                     WHERE r2.rte_grp_num = g.rte_grp_num
                                  GROUP BY r2.stat_cd), '/') AS stat_list
           FROM div_mstr_di1d d, rte_grp_rt2g g, rte_stat_rt1s r
          WHERE d.div_id = i_div
            AND g.div_part = d.div_part
            AND r.rte_grp_num = g.rte_grp_num
       GROUP BY g.rte_grp_num, g.rte_grp, g.create_dt, r.create_user_id
       ORDER BY g.rte_grp;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stat_sum_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || STAT_DTL_CUR_FN
  ||  Returns cursor of routing status detail info for RteGrp.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  ||----------------------------------------------------------------------------
  */
  FUNCTION stat_dtl_cur_fn(
    i_div        IN  VARCHAR2,
    i_rte_grp    IN  VARCHAR2,
    i_create_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.STAT_DTL_CUR_FN';
    lar_parm             logs.tar_parm;
    l_create_dt          DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.dbg('ENTRY', lar_parm);
    l_create_dt := TO_DATE(i_create_dt, 'YYYY-MM-DD HH24:MI:SS');

    OPEN l_cv
     FOR
       SELECT   r.stop_num, r.mcl_cust,
                to_list_fn(CURSOR(SELECT   TO_CHAR(r2.shp_dt, 'YYYY-MM-DD')
                                      FROM rte_stat_rt1s r2
                                     WHERE r2.rte_grp_num = g.rte_grp_num
                                       AND r2.stop_num = r.stop_num
                                       AND r2.mcl_cust = r.mcl_cust
                                  GROUP BY r2.shp_dt
                                 ),
                           ', '
                          ) AS shp_dt_list,
                SUM(r.orig_wt) AS orig_wt, SUM(r.orig_cube) AS orig_cube, r.stat_cd,
                TO_CHAR(r.new_llr_dt, 'YYYY-MM-DD') AS new_llr_dt, r.new_load_num, r.new_stop_num,
                TO_CHAR(r.new_eta_dt, 'YYYY-MM-DD HH24:MI') AS new_eta_dt,
                TO_CHAR(r.end_dt, 'YYYY-MM-DD HH24:MI:SS') AS end_dt, r.user_id
           FROM div_mstr_di1d d, rte_grp_rt2g g, rte_stat_rt1s r
          WHERE d.div_id = i_div
            AND g.div_part = d.div_part
            AND g.rte_grp = i_rte_grp
            AND g.create_dt = l_create_dt
            AND r.rte_grp_num = g.rte_grp_num
       GROUP BY g.rte_grp_num, r.stop_num, r.mcl_cust, r.stat_cd, r.new_llr_dt, r.new_load_num, r.new_stop_num,
                r.new_eta_dt, r.end_dt, r.user_id
       ORDER BY r.stop_num, r.mcl_cust, shp_dt_list;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stat_dtl_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || FILE_LIST_FN
  ||  Returns cursor of text files.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/26/08 | rhalpai | Original. PIR5882
  || 08/11/09 | cnativi | Change c_ftp_user from "SVC_DIVDFTP_PARAGON_IM-EX@mclane.mclaneco.com"
  ||                       to "mclane\SVC_DIVDFTP_PARAGON_"
  || 08/11/09 | cnativi | IM523650 - change c_ftp_user from "mclane\SVC_DIVDFTP_PARAGON_"
  ||                       to "mclane\\\SVC_DIVDFTP_PARAGON_"
  || 10/17/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  || 01/14/26 | rhalpai | Change logic to only return data in file list cursor when TMWDistExtrSw is N. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION file_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_ROUTING_PK.FILE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                VARCHAR2(500);
    l_file_list          typ.t_maxvc2;
    l_t_files            type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    IF tmw_dist_extr_sw_fn(i_div) = 'Y' THEN
      OPEN l_cv
       FOR
         SELECT NULL AS file_nm
           FROM DUAL;
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);
      l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
      logs.dbg('OS Command Setup');
      l_cmd := 'cd /DivData/' || i_div || '/Paragon/Import-Export;ls -1 *.txt *.TXT 2>&1 | egrep -v "ls:"';
      logs.dbg(l_cmd);
      l_file_list := REPLACE(TRIM(oscmd_fn(l_cmd, l_appl_srvr)),
                             'Only McLane Authorized users are permitted to login to the McLane Network.'
                            );
      logs.dbg(l_file_list);

      IF l_file_list IS NOT NULL THEN
        logs.dbg('Parse');
        l_t_files := str.parse_list(l_file_list, cnst.newline_char);
      END IF;   -- l_file_list IS NOT NULL

      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT   t.column_value AS file_nm
             FROM TABLE(CAST(l_t_files AS type_stab)) t
         ORDER BY 1;
    END IF;   -- tmw_dist_extr_sw_fn(i_div) = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END file_list_fn;

  /*
  ||-----------------------------------------------------------------------------
  || TMW_EXTRACT_CUR_FN
  ||  Return Cursor for TMW Special Dist Routing Extract.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/14/26 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  FUNCTION tmw_extract_cur_fn(
    i_div          IN  VARCHAR2,
    i_create_dt    IN  DATE,
    i_cancel_sw    IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_div_part  NUMBER;
    l_cv  SYS_REFCURSOR;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
      WITH o AS(
        SELECT   r.shp_dt AS ship_dt, ld.load_num, cx.corpb AS corp_cd, se.cust_id, se.stop_num,
                 (CASE m.catg_typ_cd
                    WHEN 'K' THEN 'COOLER'
                    WHEN 'F' THEN 'FREEZER'
                    ELSE 'DRY'
                  END) AS categ,
                 b.totctb AS tote_catg,
                 NVL(ct.outerb, 0) AS outer_cube,
                 NVL(SUM(DECODE(b.totctb, NULL, b.ordqtb)), 0) AS case_cnt,
                 DECODE(ct.pccntb,
                        'Y', CEIL(SUM(b.ordqtb) / ct.totcnb),
                        'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                  / DECODE(ct.innerb,
                                           NULL, .000001,
                                           0, .000001,
                                           ct.innerb
                                          )
                                 )
                       ) AS tote_cnt,
                 SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube,
                 SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
            FROM load_depart_op1f ld, rte_grp_rt2g rg, rte_grp_ord_rt3o o, ordp100a a, stop_eta_op1g se,
                 mclp020b cx, rte_stat_rt1s r, sysp200c c, mclp100a g, ordp120b b, sawp505e e, mclp210c m, mclp200b ct
           WHERE ld.div_part = l_div_part
             AND ld.llr_ts = DATE '1900-01-01'
             AND ld.load_num = 'ROUT'
             AND rg.div_part = l_div_part
             AND rg.create_dt = i_create_dt
             AND o.rte_grp_num = rg.rte_grp_num
             AND a.div_part = ld.div_part
             AND a.ordnoa = o.ord_num
             AND a.load_depart_sid = ld.load_depart_sid
             AND a.dsorda = 'D'
             AND a.excptn_sw = 'N'
             AND a.stata = 'O'
             AND se.div_part = a.div_part
             AND se.load_depart_sid = a.load_depart_sid
             AND se.cust_id = a.custa
             AND cx.div_part = se.div_part
             AND cx.custb = se.cust_id
             AND r.rte_grp_num = o.rte_grp_num
             AND r.mcl_cust = cx.mccusb
             AND r.stop_num = se.stop_num
             AND r.shp_dt = DATE '1900-02-28' + a.shpja
             AND r.stat_cd = DECODE(i_cancel_sw, 'Y', g_c_stat_cancl, 'N', g_c_stat_sent)
             AND c.div_part = se.div_part
             AND c.acnoc = se.cust_id
             AND g.div_part = c.div_part
             AND g.cstgpa = c.retgpc
             AND b.div_part = a.div_part
             AND b.ordnob = a.ordnoa
             AND e.iteme = b.itemnb
             AND e.uome = b.sllumb
             AND m.div_part(+) = b.div_part
             AND m.manctc(+) = b.manctb
             AND ct.div_part(+) = b.div_part
             AND ct.totctb(+) = b.totctb
        GROUP BY r.shp_dt, ld.load_num, cx.corpb, se.cust_id, se.stop_num, cx.mccusb,
                 (CASE m.catg_typ_cd
                    WHEN 'K' THEN 'COOLER'
                    WHEN 'F' THEN 'FREEZER'
                    ELSE 'DRY'
                  END),
                 DECODE(g.cntnr_trckg_sw, 'N', NULL, 'Y', RTRIM(REPLACE(a.cpoa, '0'))),
                 b.manctb, b.totctb, ct.outerb, ct.innerb, ct.pccntb, ct.totcnb
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
            FROM o, div_mstr_di1d d
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
        FROM (SELECT   DECODE(i_cancel_sw, 'Y', 'D', 'N', 'A')   --'ADD' 'DEL'
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
                       || 'SD'
                       || x.div_id
                       || TO_CHAR(x.ship_dt, 'YYMMDD')
                       || TO_CHAR(i_create_dt, 'YYMMDDHH24MISS')
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
  END tmw_extract_cur_fn;

  /*
  ||-----------------------------------------------------------------------------
  || TMW_EXTRACT_FILE_SP
  ||  Create and Send TMW Special Dist Routing Extract file.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/14/26 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE tmw_extract_file_sp(
    i_div        IN  VARCHAR2,
    i_create_dt  IN  DATE,
    i_cancel_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                       := 'OP_ROUTING_PK.TMW_EXTRACT_FILE_SP';
    lar_parm               logs.tar_parm;
    l_test_db_sw           VARCHAR2(1);
    l_file_ts              DATE;
    l_file_nm              VARCHAR2(60);
    l_cv                   SYS_REFCURSOR;
    l_t_rpt_lns            typ.tas_maxvc2;
    l_appl_srvr            appl_sys_parm_ap1s.vchar_val%TYPE;
    l_c_file_dir  CONSTANT VARCHAR2(50)                        := '/ftptrans';
    l_c_rmt_dir   CONSTANT VARCHAR2(50)                        := '/local/data';
    l_rmt_file_nm          VARCHAR2(60);
    l_cmd                  VARCHAR2(2000);
    l_os_result            VARCHAR2(2000);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.add_parm(lar_parm, 'CancelSw', i_cancel_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_test_db_sw :=(CASE
                      WHEN SUBSTR(ora_database_name, -1) = 'P' THEN 'N'
                      ELSE 'Y'
                    END);
    l_file_ts := (CASE i_cancel_sw
                    WHEN 'N' THEN i_create_dt
                    ELSE SYSDATE
                  END);
    l_file_nm := 'TMWDIST_' || i_div || '_' || TO_CHAR(l_file_ts, 'YYYYMMDDHH24MISS');
    l_rmt_file_nm := (CASE l_test_db_sw
                        WHEN 'Y' THEN 'TESTING_'
                      END)
                     || 'Order-Dist'
                     || i_div
                     || '-'
                     || TO_CHAR(l_file_ts, 'MMDDYYYY-HH24MI')
                     || '.txt';
    logs.dbg('Get Extract Cursor');
    l_cv := tmw_extract_cur_fn(i_div, i_create_dt, i_cancel_sw);
    logs.dbg('Fetch Extract Cursor');

    FETCH l_cv
    BULK COLLECT INTO l_t_rpt_lns;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_file_nm);
    logs.dbg('Copy file to Remote');
    l_appl_srvr := op_parms_pk.val_fn(0, op_const_pk.prm_appl_srvr);
    l_cmd := 'ssh -q '
             || l_appl_srvr
             || ' ''cp -p -T '
             || l_c_file_dir
             || '/'
             || l_file_nm
             || ' '
             || l_c_rmt_dir
             || '/'
             || l_rmt_file_nm
             || '''';
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd);
    logs.info(l_os_result);
    logs.dbg('Archive');
    archive_sp(l_file_nm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tmw_extract_file_sp;

  /*
  ||----------------------------------------------------------------------------
  || STAGE_SP
  ||  Moves special (P%%) distribution orders for ship date from their default
  ||  loads (P%%P) to the route staging load (ROUT).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/27/08 | rhalpai | Changed to handle ship date range and corp list. PIR5882
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 04/12/16 | rhalpai | Change cursor to use div_part. PIR14660
  ||----------------------------------------------------------------------------
  */
  PROCEDURE stage_sp(
    i_div          IN  VARCHAR2,
    i_from_shp_dt  IN  VARCHAR2,
    i_to_shp_dt    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ROUTING_PK.STAGE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_from_shp_dt        NUMBER;
    l_to_shp_dt          NUMBER;
    l_t_crps             type_stab;
    l_shp_dt_save        NUMBER                := -1;
    l_eta_dt_char        VARCHAR2(10);
    l_eta_tm             NUMBER;
    l_cust_save          sysp200c.acnoc%TYPE   := '?';
    l_ord_list           typ.t_maxvc2;
    l_stop_num           NUMBER;
    l_move_msg           typ.t_maxvc2;
    l_is_too_many_ords   BOOLEAN               := FALSE;
    l_is_move_err        BOOLEAN               := FALSE;
    l_t_prbs             g_tt_prbs             := g_tt_prbs();

    CURSOR l_cur_ords(
      b_div_part     NUMBER,
      b_from_shp_dt  NUMBER,
      b_to_shp_dt    NUMBER,
      b_crp_list     VARCHAR2,
      b_t_crps       type_stab
    ) IS
      SELECT   a.custa AS cust_id, a.shpja AS shp_dt, a.ordnoa AS ord_num
          FROM load_depart_op1f ld, ordp100a a
         WHERE ld.div_part = b_div_part
           AND ld.llr_ts = DATE '1900-01-01'
           AND ld.load_num BETWEEN 'P00P' AND 'P99P'
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND a.stata = 'O'
           AND a.dsorda = 'D'
           AND a.ldtypa BETWEEN 'P00' AND 'P99'
           AND a.shpja BETWEEN b_from_shp_dt AND b_to_shp_dt
           AND (   b_crp_list IS NULL
                OR EXISTS(SELECT 1
                            FROM mclp020b cx, TABLE(CAST(b_t_crps AS type_stab)) t
                           WHERE cx.div_part = a.div_part
                             AND cx.custb = a.custa
                             AND cx.corpb = TO_NUMBER(t.column_value))
               )
      ORDER BY a.custa, a.shpja, a.ordnoa;

    PROCEDURE add_prb_sp(
      i_prb  IN  VARCHAR2
    ) IS
    BEGIN
      IF l_t_prbs.COUNT = 0 THEN
        l_t_prbs.EXTEND;
        l_t_prbs(l_t_prbs.LAST) := 'Staging Error!' || cnst.newline_char || logs.parm_list(lar_parm)
                                   || cnst.newline_char;
      END IF;   -- l_t_prbs.COUNT = 0

      l_t_prbs.EXTEND;
      l_t_prbs(l_t_prbs.LAST) := SUBSTR(i_prb, 1, 3999) || cnst.newline_char;
    END add_prb_sp;

    PROCEDURE add_to_list_sp(
      i_ord_num  IN  NUMBER
    ) IS
    BEGIN
      IF NOT l_is_too_many_ords THEN
        l_ord_list := l_ord_list ||(CASE
                                      WHEN l_ord_list IS NOT NULL THEN '`'
                                    END) || i_ord_num;
      END IF;   -- NOT l_is_too_many_ords
    EXCEPTION
      WHEN VALUE_ERROR THEN
        l_is_too_many_ords := TRUE;
        logs.warn('Order list is too large and was truncated!', lar_parm);
        add_prb_sp('There are too many orders to process at once.'
                   || cnst.newline_char
                   || 'Process continues with as many orders as possible.'
                   || cnst.newline_char
                   || 'The remaining orders will need to be staged again.'
                  );
    END add_to_list_sp;

    PROCEDURE move_sp IS
    BEGIN
      IF NOT l_is_move_err THEN
        op_order_moves_pk.move_orders_sp(i_div,
                                         l_cust_save,
                                         'ROUT',
                                         l_stop_num,
                                         l_eta_dt_char,
                                         l_eta_tm,
                                         l_ord_list,
                                         i_user_id,
                                         l_move_msg,
                                         'QROUTE'
                                        );

        IF SUBSTR(l_move_msg, 1, 1) = 'E' THEN
          l_is_move_err := TRUE;
          logs.warn(l_move_msg,
                    lar_parm,
                    'Div: '
                    || i_div
                    || ' CustId: '
                    || l_cust_save
                    || ' Load: ROUT StopNum: '
                    || l_stop_num
                    || ' EtaDt: '
                    || l_eta_dt_char
                    || ' EtaTm: '
                    || l_eta_tm
                    || ' OrdList: '
                    || l_ord_list
                   );
          add_prb_sp('Order move error.'
                     || cnst.newline_char
                     || SQLERRM
                     || cnst.newline_char
                     || 'CustId: '
                     || l_cust_save
                     || ' Load: ROUT StopNum: '
                     || l_stop_num
                     || ' EtaDt: '
                     || l_eta_dt_char
                     || ' EtaTm: '
                     || l_eta_tm
                     || ' OrdList: '
                     || l_ord_list
                    );
        END IF;   -- SUBSTR(l_move_msg, 1, 1) = 'E'
      END IF;   -- NOT l_is_move_err
    END move_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FromShpDt', i_from_shp_dt);
    logs.add_parm(lar_parm, 'ToShpDt', i_to_shp_dt);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_from_shp_dt := TO_DATE(i_from_shp_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_to_shp_dt := TO_DATE(i_to_shp_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_eta_tm := 0;

    IF i_crp_list IS NOT NULL THEN
      l_t_crps := str.parse_list(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list IS NOT NULL

    FOR l_r_ord IN l_cur_ords(l_div_part, l_from_shp_dt, l_to_shp_dt, i_crp_list, l_t_crps) LOOP
      -- check for change in cust_id
      IF l_r_ord.cust_id <> l_cust_save THEN
        IF (    l_ord_list IS NOT NULL
            AND l_stop_num IS NOT NULL) THEN
          logs.dbg('Move to Routing Load');
          move_sp;
          l_ord_list := NULL;
        END IF;   -- l_ord_list IS NOT NULL AND l_stop_num IS NOT NULL

        l_cust_save := l_r_ord.cust_id;
        l_shp_dt_save := -1;
        logs.dbg('Get Stop for Cust');
        -- Check for cust_id already on routing load and not in SNT or WRK status
        -- for ship date.
        -- If not found use first available stop on routing load.
        l_stop_num := stop_for_cust_fn(i_div, l_r_ord.cust_id);
      END IF;   -- l_r_ord.cust_id <> l_cust_save

      -- use max ship date as ETA
      IF l_r_ord.shp_dt > l_shp_dt_save THEN
        l_shp_dt_save := l_r_ord.shp_dt;
        l_eta_dt_char := TO_CHAR(DATE '1900-02-28' + l_r_ord.shp_dt, 'YYYY-MM-DD');
      END IF;   -- l_r_ord.shp_dt > l_shp_dt_save

      logs.dbg('Add OrderNum to List');
      add_to_list_sp(l_r_ord.ord_num);
    END LOOP;

    IF (    l_ord_list IS NOT NULL
        AND l_stop_num IS NOT NULL) THEN
      logs.dbg('Move to Routing Load for Final Cust');
      move_sp;
    END IF;   -- l_ord_list IS NOT NULL AND l_stop_num IS NOT NULL

    IF l_t_prbs.COUNT > 0 THEN
      logs.dbg('Notify');
      notify_sp(i_div, l_t_prbs);
    END IF;   -- l_t_prbs.COUNT > 0

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stage_sp;

  /*
  ||----------------------------------------------------------------------------
  || STAGE_FOR_ROUTING_SP
  ||  Start STAGE_SP in a separate thread.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/01/08 | rhalpai | Changed unix script to specify Korn shell #!/bin/ksh.
  ||                    | PIR5819
  || 03/27/08 | rhalpai | Changed to handle ship date range and corp list. PIR5882
  || 05/21/08 | rhalpai | Changed to execute /local/prodcode/bin/XXOPRoutingStage.sub
  ||                    | with Div/FromShip/ToShip/CorpList/UserID/OracleSID
  ||                    | parms via oscmd_fn. PIR5819
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 05/13/13 | rhalpai | Change logic to call xxopRoutingStage.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Replace
  ||                    | op_parms_pk.get_val_fn with op_parms_pk.val_fn. PIR14660
  || 10/17/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE stage_for_routing_sp(
    i_div          IN  VARCHAR2,
    i_from_shp_dt  IN  VARCHAR2,
    i_to_shp_dt    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_ROUTING_PK.STAGE_FOR_ROUTING_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_sid                VARCHAR2(10);
    l_cmd                VARCHAR2(2000);
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FromShpDt', i_from_shp_dt);
    logs.add_parm(lar_parm, 'ToShpDt', i_to_shp_dt);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_sid := SYS_CONTEXT('USERENV', 'DB_NAME');
    l_cmd := '/local/prodcode/bin/xxopRoutingStage.sub "'
             || i_div
             || '" "'
             || i_from_shp_dt
             || '" "'
             || i_to_shp_dt
             || '" "'
             || i_crp_list
             || '" "'
             || i_user_id
             || '" "'
             || l_sid
             || '"';
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END stage_for_routing_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_STAGE_SP
  ||  Move orders to their default loads.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/09/08 | rhalpai | Original. PIR5882
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_stage_sp(
    i_div          IN  VARCHAR2,
    i_shp_dt_list  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ROUTING_PK.CANCEL_STAGE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_shp_dts          type_stab;
    l_cv                 SYS_REFCURSOR;
    l_t_ords             type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ShpDtList', i_shp_dt_list);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.dbg('Parse');
    l_t_shp_dts := str.parse_list(i_shp_dt_list, op_const_pk.field_delimiter);
    logs.dbg('Open Order Cursor');

    OPEN l_cv
     FOR
       SELECT a.ordnoa
         FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
        WHERE d.div_id = i_div
          AND ld.div_part = d.div_part
          AND ld.llr_ts = DATE '1900-01-01'
          AND ld.load_num = 'ROUT'
          AND se.div_part = ld.div_part
          AND se.load_depart_sid = ld.load_depart_sid
          AND cx.div_part = se.div_part
          AND cx.custb = se.cust_id
          AND a.div_part = ld.div_part
          AND a.load_depart_sid = ld.load_depart_sid
          AND a.custa = se.cust_id
          AND a.excptn_sw = 'N'
          AND a.stata = 'O'
          AND a.dsorda = 'D'
          AND a.shpja IN(SELECT TO_DATE(t.column_value, 'YYYY-MM-DD') - DATE '1900-02-28'
                           FROM TABLE(CAST(l_t_shp_dts AS type_stab)) t)
          AND NOT EXISTS(SELECT 1
                           FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                          WHERE g.div_part = d.div_part
                            AND r.rte_grp_num = g.rte_grp_num
                            AND r.shp_dt = DATE '1900-02-28' + a.shpja
                            AND r.stop_num = se.stop_num
                            AND r.mcl_cust = cx.mccusb
                            AND r.stat_cd IN(g_c_stat_sent, g_c_stat_work)
                            AND o.rte_grp_num = g.rte_grp_num
                            AND o.ord_num = a.ordnoa);

    logs.dbg('Fetch Order Cursor');

    FETCH l_cv
    BULK COLLECT INTO l_t_ords;

    IF l_cv%ROWCOUNT > 0 THEN
      logs.dbg('Reassign Orders');
      reassign_ords_sp(i_div, l_t_ords);
      COMMIT;
    END IF;   -- l_cv%ROWCOUNT > 0

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END cancel_stage_sp;

  /*
  ||----------------------------------------------------------------------------
  || SEND_ROUTING_SP
  ||  For each ShipDate in ShipDateList the route status table is loaded and
  ||  routing file is created and sent with ShipDate/Stop/Cust/OrigWt/OrigCube
  ||  info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 06/16/08 | rhalpai | Added sort by Stop/Cust to cursor.
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 10/19/10 | rhalpai | Changed cursor to use cube from CorpItem table,
  ||                    | SAWP505E, instead of from OrderDetail table. PIR5878
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers.
  ||                    | Change to use common div_part_fn. PIR14660
  || 02/02/17 | rhalpai | Add logic to create CSV file. PIR16183
  || 09/09/19 | rhalpai | Change CSV extract to rename column GroupName to WaveName
  ||                    | and remove YearDate column. PIR19778
  || 10/17/19 | rhalpai | Add logic to restrict Route Group Name to valid characters
  ||                    | for file name. SDHD-578614
  || 01/14/26 | rhalpai | Add logic to call TMW Extract when TMWDistExtrSw is Y. PIR18901
  || 02/20/26 | rhalpai | Change logic to always create the Direct Routing csv and txt files on div H-drive. SDHD-2600394
  ||----------------------------------------------------------------------------
  */
  PROCEDURE send_routing_sp(
    i_div          IN  VARCHAR2,
    i_shp_dt_list  IN  VARCHAR2,
    i_rte_grp      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm               := 'OP_ROUTING_PK.SEND_ROUTING_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_create_dt            DATE;
    l_rte_grp              rte_grp_rt2g.rte_grp%TYPE;
    l_tmw_dist_extr_sw     VARCHAR2(1);
    l_base_file_nm         VARCHAR2(60);
    l_txt_file_nm          VARCHAR2(60);
    l_csv_file_nm          VARCHAR2(60);
    l_t_shp_dts            type_stab;
    l_rte_grp_num          NUMBER;
    l_t_txt_rpt_lns        typ.tas_maxvc2;
    l_t_csv_rpt_lns        typ.tas_maxvc2;
    l_c_file_dir  CONSTANT VARCHAR2(50)                := '/ftptrans';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ShpDtList', i_shp_dt_list);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_create_dt := SYSDATE;
    l_rte_grp := REPLACE(UPPER(TRIM(i_rte_grp)), ' ', '_');
    excp.assert((l_rte_grp IS NOT NULL), 'RteGrp is required');
    excp.assert((regexp_like(l_rte_grp, '^[A-Z0-9_-]+$')),
                'RteGrp "' || i_rte_grp || '" contains invalid characters for a file name'
               );
    l_tmw_dist_extr_sw := tmw_dist_extr_sw_fn(i_div);

    IF l_tmw_dist_extr_sw = 'Y' THEN
      l_rte_grp := 'TMWDIST_' || SUBSTR(l_rte_grp, 1, 17);
    END IF;   -- l_tmw_dist_extr_sw = 'Y'

    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Parse');
    l_t_shp_dts := str.parse_list(i_shp_dt_list, op_const_pk.field_delimiter);

    IF l_t_shp_dts.COUNT > 0 THEN
      logs.dbg('Add Route Grouping');

      INSERT INTO rte_grp_rt2g
                  (rte_grp_num, div_part, rte_grp, create_dt
                  )
           VALUES (rte_grp_num_seq.NEXTVAL, l_div_part, l_rte_grp, l_create_dt
                  )
        RETURNING rte_grp_num
             INTO l_rte_grp_num;

      logs.dbg('Add Route Orders');

      INSERT INTO rte_grp_ord_rt3o
                  (rte_grp_num, ord_num)
        SELECT l_rte_grp_num, a.ordnoa
          FROM load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
         WHERE ld.div_part = l_div_part
           AND ld.llr_ts = DATE '1900-01-01'
           AND ld.load_num = 'ROUT'
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND cx.div_part = se.div_part
           AND cx.custb = se.cust_id
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.custa = se.cust_id
           AND a.excptn_sw = 'N'
           AND a.stata = 'O'
           AND a.dsorda = 'D'
           AND a.shpja IN(SELECT TO_DATE(t.column_value, 'YYYY-MM-DD') - DATE '1900-02-28'
                            FROM TABLE(CAST(l_t_shp_dts AS type_stab)) t)
           AND NOT EXISTS(SELECT 1
                            FROM sub_prcs_ord_src s
                           WHERE s.prcs_id = 'LOAD BALANCE'
                             AND s.prcs_sbtyp_cd = 'BLB'
                             AND s.div_part = a.div_part
                             AND s.ord_src = a.ipdtsa)
           AND NOT EXISTS(SELECT 1
                            FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                           WHERE g.div_part = a.div_part
                             AND r.rte_grp_num = g.rte_grp_num
                             AND r.shp_dt = DATE '1900-02-28' + a.shpja
                             AND r.stop_num = se.stop_num
                             AND r.mcl_cust = cx.mccusb
                             AND r.stat_cd IN(g_c_stat_sent, g_c_stat_work)
                             AND o.rte_grp_num = g.rte_grp_num
                             AND o.ord_num = a.ordnoa);

      logs.dbg('Add to Route Status');

      INSERT INTO rte_stat_rt1s
                  (rte_grp_num, shp_dt, stop_num, mcl_cust, create_user_id, orig_wt, orig_cube, stat_cd, user_id)
        SELECT   l_rte_grp_num, DATE '1900-02-28' + x.shpja AS shp_dt, x.stop_num, x.mcl_cust, i_user_id,
                 CEIL(SUM(x.prod_wt)) AS orig_wt,
                 CEIL(SUM(DECODE(x.tote_cnt, NULL, x.prod_cube, x.tote_cnt * x.outerb))) AS orig_cube, g_c_stat_sent,
                 i_user_id
            FROM (SELECT   a.shpja, se.stop_num, cx.mccusb AS mcl_cust, ct.outerb,
                           DECODE(ct.boxb,
                                  'N', DECODE(ct.pccntb,
                                              'Y', CEIL(SUM(b.ordqtb) / ct.totcnb),
                                              'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                                        / DECODE(ct.innerb, NULL, .000001, 0, .000001, ct.innerb)
                                                       )
                                             )
                                 ) AS tote_cnt,
                           SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube, SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
                      FROM load_depart_op1f ld, rte_grp_rt2g rg, rte_grp_ord_rt3o o, ordp100a a, stop_eta_op1g se,
                           mclp020b cx, sysp200c c, mclp100a g, ordp120b b, sawp505e e, mclp200b ct
                     WHERE ld.div_part = l_div_part
                       AND ld.llr_ts = DATE '1900-01-01'
                       AND ld.load_num = 'ROUT'
                       AND rg.div_part = l_div_part
                       AND rg.rte_grp = l_rte_grp
                       AND rg.create_dt = l_create_dt
                       AND o.rte_grp_num = rg.rte_grp_num
                       AND a.div_part = ld.div_part
                       AND a.ordnoa = o.ord_num
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.dsorda = 'D'
                       AND a.excptn_sw = 'N'
                       AND a.stata = 'O'
                       AND se.div_part = a.div_part
                       AND se.load_depart_sid = a.load_depart_sid
                       AND se.cust_id = a.custa
                       AND cx.div_part = se.div_part
                       AND cx.custb = se.cust_id
                       AND c.div_part = se.div_part
                       AND c.acnoc = se.cust_id
                       AND g.div_part = c.div_part
                       AND g.cstgpa = c.retgpc
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND e.iteme = b.itemnb
                       AND e.uome = b.sllumb
                       AND ct.div_part(+) = b.div_part
                       AND ct.totctb(+) = b.totctb
                  GROUP BY a.shpja, se.stop_num, cx.mccusb,
                           DECODE(g.cntnr_trckg_sw, 'N', NULL, 'Y', RTRIM(REPLACE(a.cpoa, '0'))), b.manctb, b.totctb,
                           ct.outerb, ct.innerb, ct.boxb, ct.pccntb, ct.totcnb) x
        GROUP BY x.stop_num, x.mcl_cust, x.shpja;

      IF l_tmw_dist_extr_sw = 'Y' THEN
        logs.dbg('TMW Extract File');
        tmw_extract_file_sp(i_div, l_create_dt, 'N');
      END IF;   -- l_tmw_dist_extr_sw = 'Y'

      l_base_file_nm := i_div || 'WV' || l_rte_grp || '_' || TO_CHAR(l_create_dt, 'YYYYMMDDHH24MISS');
      l_txt_file_nm := l_base_file_nm || '.txt';
      l_csv_file_nm := l_base_file_nm || '.csv';
      logs.dbg('Get TXT Report Lines');

      WITH z AS
           (SELECT   i_div
                     || 'DC     '
                     || RPAD(l_rte_grp, 25)
                     || ' '
                     || TO_CHAR(l_create_dt, 'YYYYMMDDHH24MISS')
                     || ' '
                     || r.mcl_cust
                     || '     '
                     || LPAD(r.stop_num, 4, '0')
                     || '     '
                     || TO_CHAR(MAX(r.shp_dt), 'YYYYMMDD')
                     || ' '
                     || LPAD(SUM(r.orig_wt), 5, '0')
                     || '     '
                     || LPAD(SUM(r.orig_cube), 4, '0') AS txt
                FROM rte_stat_rt1s r
               WHERE r.rte_grp_num = l_rte_grp_num
            GROUP BY r.stop_num, r.mcl_cust
            ORDER BY r.stop_num, r.mcl_cust)
      SELECT zz.txt
      BULK COLLECT INTO l_t_txt_rpt_lns
      FROM   (SELECT 'TDATA02>>TDATA01>>>>>>>>>>>>>>>>>>>TDATA04>>>>>>>>ID>>>>>>>>>TDATA03>>TDATA05>>MEASURE1>>MEASURE2>' AS txt
                FROM DUAL
              UNION ALL
              SELECT z.txt
                FROM z) zz;

      logs.dbg('Write TXT File');
      write_sp(l_t_txt_rpt_lns, l_txt_file_nm, l_c_file_dir);
      l_t_txt_rpt_lns.DELETE;   -- release memory
      logs.dbg('Export TXT File to Remote and Archive');
      export_sp(i_div, l_txt_file_nm);
      logs.dbg('Get CSV Report Lines');

      WITH z AS
           (SELECT   i_div
                     || 'DC,'
                     || l_rte_grp
                     || ','
                     || TO_CHAR(l_create_dt, 'YYYYMMDDHH24MISS')
                     || ','
                     || r.mcl_cust
                     || ','
                     || r.stop_num
                     || ','
                     || SUM(r.orig_wt)
                     || ','
                     || SUM(r.orig_cube) AS csv
                FROM rte_stat_rt1s r
               WHERE r.rte_grp_num = l_rte_grp_num
            GROUP BY r.stop_num, r.mcl_cust
            ORDER BY r.stop_num, r.mcl_cust)
      SELECT zz.csv
      BULK COLLECT INTO l_t_csv_rpt_lns
      FROM   (SELECT 'DIV,WaveName,UniqueID,Account,UniqueID2,Weight,Cube' AS csv
                FROM DUAL
              UNION ALL
              SELECT z.csv
                FROM z) zz;

      logs.dbg('Write CSV File');
      write_sp(l_t_csv_rpt_lns, l_csv_file_nm, l_c_file_dir);
      l_t_csv_rpt_lns.DELETE;   -- release memory
      logs.dbg('Export CSV File to Remote and Archive');
      export_sp(i_div, l_csv_file_nm);

      COMMIT;
    END IF;   -- t_shp_dts.COUNT > 0

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN excp.gx_assert_fail THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END send_routing_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_SP
  ||  Update routing status table to cancel status for selected entries.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  || 02/27/17 | rhalpai | Change logic to include upd_cnt parm in call to UPD_STAT_SP. SDHD-94141
  || 01/14/26 | rhalpai | Add logic to call CANCEL_SENT_ORDS_SP. PIR18901
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_sp(
    i_div        IN  VARCHAR2,
    i_rte_grp    IN  VARCHAR2,
    i_create_dt  IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_ROUTING_PK.CANCEL_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_create_dt          DATE;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
    l_stop_num           NUMBER;
    l_mcl_cust           mclp020b.mccusb%TYPE;
    l_upd_cnt            NUMBER;
    l_c_end_dt  CONSTANT DATE                   := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrp', i_rte_grp);
    logs.add_parm(lar_parm, 'CreateDt', i_create_dt);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_create_dt := TO_DATE(i_create_dt, 'YYYY-MM-DD HH24:MI:SS');
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Parse Groups');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_t_fields := NULL;
        logs.dbg('Parse Fields');
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
        l_stop_num := val_at_idx_fn(l_t_fields, 1);
        l_mcl_cust := val_at_idx_fn(l_t_fields, 2);
        logs.dbg('Update Status to Cancel');
        upd_stat_sp(i_div,
                    i_rte_grp,
                    l_create_dt,
                    l_stop_num,
                    l_mcl_cust,
                    g_c_stat_cancl,
                    i_user_id,
                    l_upd_cnt,
                    l_c_end_dt
                   );
        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;

      logs.dbg('Cancel Sent Orders');
      cancel_sent_ords_sp(i_div, i_rte_grp, l_create_dt);
      COMMIT;
    END IF;   -- l_t_grps IS NOT NULL

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END cancel_sp;

  /*
  ||----------------------------------------------------------------------------
  || IMPORT_SP
  ||  Generate and process Control-M script to get the passed RemoteFile and
  ||  call process_routed_sp for processing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 05/21/08 | rhalpai | Changed to execute /local/prodcode/bin/XXOPRoutingImport.sub
  ||                    | with Div/LocalFile/RemoteFile/OracleSID parms via
  ||                    | oscmd_fn. PIR5819
  || 05/13/13 | rhalpai | Change logic to call xxopRoutingImport.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  || 10/17/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE import_sp(
    i_div          IN  VARCHAR2,
    i_rmt_file_nm  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_ROUTING_PK.IMPORT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_local_file_nm      VARCHAR2(200);
    l_sid                VARCHAR2(10);
    l_cmd                VARCHAR2(2000);
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RmtFileNm', i_rmt_file_nm);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_local_file_nm := i_div || '_ROUTED_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
    l_sid := SYS_CONTEXT('USERENV', 'DB_NAME');
    l_cmd := '/local/prodcode/bin/xxopRoutingImport.sub "'
             || i_div
             || '" "'
             || l_local_file_nm
             || '" "'
             || i_rmt_file_nm
             || '" "'
             || l_sid
             || '"';
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
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
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END import_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_ROUTED_SP
  ||  Parses the RoutedFile and moves orders to new Load/Stop/Eta as directed
  ||  by the RouteFile and updates the route status table and notifies of any
  ||  problems.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/07 | rhalpai | Original. PIR3643
  || 03/28/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  || 02/27/17 | rhalpai | Change logic to include upd_cnt parm in call to UPD_STAT_SP. SDHD-94141
  || 01/14/26 | rhalpai | Add logic to handle TMWDist updates when TMWDistExtrSw is Y.
  ||                    | Add logic to call CANCEL_SENT_ORDS_SP following Cancel Any Remaining Sent Entries for Grouping. PIR18901
  ||----------------------------------------------------------------------------
  */
  PROCEDURE process_routed_sp(
    i_div            IN  VARCHAR2,
    i_local_file_nm  IN  VARCHAR2,
    i_rmt_file_nm    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm          := 'OP_ROUTING_PK.PROCESS_ROUTED_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_sysdate  CONSTANT DATE                   := SYSDATE;
    l_tmw_dist_extr_sw    VARCHAR2(1);
    l_last_chg_ts         DATE;
    l_t_import            g_tt_import            := g_tt_import();
    l_rte_grp             VARCHAR2(30);
    l_create_dt           DATE;
    l_orig_stop           PLS_INTEGER;
    l_mcl_cust            mclp020b.mccusb%TYPE;
    l_new_load            mclp120c.loadc%TYPE;
    l_new_stop            PLS_INTEGER;
    l_new_eta_ts          DATE;
    l_upd_cnt             NUMBER;
    l_found_sw            VARCHAR2(1);
    l_cv                  SYS_REFCURSOR;
    l_new_llr_ts          DATE;
    l_new_dep_ts          DATE;
    l_move_msg            typ.t_maxvc2;
    l_t_prbs              g_tt_prbs              := g_tt_prbs();

    PROCEDURE error_sp(
      i_msg         IN  VARCHAR2,
      i_new_llr_dt  IN  DATE DEFAULT NULL
    ) IS
    BEGIN
      logs.dbg('Update to Error Status');
      upd_stat_sp(i_div,
                  l_rte_grp,
                  l_create_dt,
                  l_orig_stop,
                  l_mcl_cust,
                  g_c_stat_err,
                  'QROUTE',
                  l_upd_cnt,
                  l_c_sysdate,
                  i_new_llr_dt,
                  l_new_load,
                  l_new_stop,
                  l_new_eta_ts
                 );
      logs.dbg('Add Problem');
      add_problem_sp(l_t_prbs,
                     i_msg,
                     l_rte_grp,
                     l_create_dt,
                     l_orig_stop,
                     l_mcl_cust,
                     l_new_load,
                     l_new_stop,
                     l_new_eta_ts
                    );
    END error_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LocalFileNm', i_local_file_nm);
    logs.add_parm(lar_parm, 'RmtFileNm', i_rmt_file_nm);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_tmw_dist_extr_sw := tmw_dist_extr_sw_fn(i_div);

    IF l_tmw_dist_extr_sw = 'Y' THEN
      logs.dbg('Get Latest Request Grouping');

      SELECT MAX(r.last_chg_ts)
        INTO l_last_chg_ts
        FROM cust_dist_rte_req_op5c r
       WHERE r.div_part = l_div_part
         AND r.stat_cd = 'OPN';

      IF l_last_chg_ts IS NOT NULL THEN
        logs.dbg('Remove unprocessed previous requests');

        DELETE FROM cust_dist_rte_req_op5c r
              WHERE r.div_part = l_div_part
                AND r.last_chg_ts IN(SELECT   rr.last_chg_ts
                                         FROM cust_dist_rte_req_op5c rr
                                        WHERE rr.div_part = l_div_part
                                          AND rr.stat_cd = 'OPN'
                                          AND rr.last_chg_ts < l_last_chg_ts
                                     GROUP BY rr.last_chg_ts);

        logs.dbg('Get Import Collection');

        SELECT i_div,
               g.rte_grp,
               g.create_dt,
               s.mcl_cust,
               s.stop_num,
               cr.new_load_num,
               cr.new_stop_num,
               cr.new_eta_ts
        BULK COLLECT INTO l_t_import
          FROM cust_dist_rte_req_op5c cr, mclp020b cx, rte_grp_rt2g g, rte_stat_rt1s s
         WHERE cr.div_part = l_div_part
           AND cr.stat_cd = 'OPN'
           AND cr.last_chg_ts = l_last_chg_ts
           AND cx.div_part = cr.div_part
           AND cx.custb = cr.cust_id
           AND g.div_part = cr.div_part
           AND g.create_dt = cr.create_ts
           AND s.rte_grp_num = g.rte_grp_num
           AND s.mcl_cust = cx.mccusb
           AND s.shp_dt = cr.ship_dt
           AND s.stat_cd = 'SNT';
      END IF;   -- l_last_chg_ts IS NOT NULL
    ELSE
      logs.dbg('Parse');
      parse_sp(i_local_file_nm, l_t_import);
    END IF;   -- l_tmw_dist_extr_sw = 'Y'

    IF l_t_import.COUNT > 0 THEN
      logs.dbg('Process Import');
      <<import_loop>>
      FOR i IN l_t_import.FIRST .. l_t_import.LAST LOOP
        l_rte_grp := l_t_import(i).rte_grp;
        l_create_dt := l_t_import(i).create_dt;
        l_orig_stop := l_t_import(i).orig_stop;
        l_mcl_cust := l_t_import(i).mcl_cust;
        l_new_load := l_t_import(i).new_load;
        l_new_stop := l_t_import(i).new_stop;
        l_new_eta_ts := l_t_import(i).new_eta_dt;
        l_upd_cnt := 0;
        logs.dbg('Set to Work Status');
        upd_stat_sp(i_div, l_rte_grp, l_create_dt, l_orig_stop, l_mcl_cust, g_c_stat_work, 'QROUTE', l_upd_cnt);

        IF l_upd_cnt > 0 THEN
          logs.dbg('Validate Load');
          l_found_sw := 'N';

          OPEN l_cv
           FOR
             SELECT 'Y'
               FROM mclp120c c
              WHERE c.div_part = l_div_part
                AND c.loadc = l_new_load;

          FETCH l_cv
           INTO l_found_sw;

          IF l_found_sw = 'N' THEN
            logs.dbg('Process Error for Invalid Load');
            error_sp('New load does not exist.', NULL);
          ELSE
            logs.dbg('Get LLR/Depart');
            op_order_moves_pk.get_llr_depart_for_load_eta_sp(i_div,
                                                             l_new_load,
                                                             l_new_eta_ts,
                                                             l_new_llr_ts,
                                                             l_new_dep_ts
                                                            );
            logs.dbg('Move Orders');
            -- validate and then move orders
            move_orders_sp(i_div,
                           l_rte_grp,
                           l_create_dt,
                           l_orig_stop,
                           l_mcl_cust,
                           l_new_load,
                           l_new_stop,
                           l_new_eta_ts,
                           l_move_msg
                          );

            IF SUBSTR(l_move_msg, 1, 1) = 'I' THEN
              logs.dbg('Set Complete Status');
              upd_stat_sp(i_div,
                          l_rte_grp,
                          l_create_dt,
                          l_orig_stop,
                          l_mcl_cust,
                          g_c_stat_cmplt,
                          'QROUTE',
                          l_upd_cnt,
                          l_c_sysdate,
                          TRUNC(l_new_llr_ts),
                          l_new_load,
                          l_new_stop,
                          l_new_eta_ts
                         );
            ELSE
              logs.dbg('Process Error for Order Moves');
              error_sp(SUBSTR(l_move_msg, 3), TRUNC(l_new_llr_ts));
            END IF;   -- SUBSTR(l_move_msg, 1, 1) = 'I'
          END IF;   -- l_found_sw = 'N'
        ELSE
          logs.dbg('Add Problem for Record Not Found');
          add_problem_sp(l_t_prbs,
                         'Record Not Found!',
                         l_rte_grp,
                         l_create_dt,
                         l_orig_stop,
                         l_mcl_cust,
                         l_new_load,
                         l_new_stop,
                         l_new_eta_ts
                        );
        END IF;   -- l_upd_cnt > 0
      END LOOP import_loop;
      logs.dbg('Cancel Any Remaining Sent Entries for Grouping');
      upd_stat_sp(i_div, l_rte_grp, l_create_dt, NULL, NULL, g_c_stat_cancl, 'QROUTE', l_upd_cnt, l_c_sysdate);
      logs.dbg('Cancel Sent Orders');
      cancel_sent_ords_sp(i_div, l_rte_grp, l_create_dt);

      IF l_tmw_dist_extr_sw = 'Y' THEN
        UPDATE cust_dist_rte_req_op5c cr
           SET cr.stat_cd = 'CMP',
               cr.last_chg_ts = l_c_sysdate
         WHERE cr.div_part = l_div_part
           AND cr.stat_cd = 'OPN'
           AND cr.last_chg_ts = l_last_chg_ts;
      END IF;   -- l_tmw_dist_extr_sw = 'Y'

      COMMIT;

      IF l_t_prbs.COUNT > 0 THEN
        logs.dbg('Notify');
        notify_sp(i_div, l_t_prbs);
      END IF;   -- l_t_prbs.COUNT > 0

      IF l_tmw_dist_extr_sw = 'Y' THEN
        logs.dbg('TMW Archive');
        archive_sp(i_rmt_file_nm, 'Y');
      ELSE
        logs.dbg('OP Archive');
        archive_sp(i_local_file_nm);
      END IF;   -- l_tmw_dist_extr_sw = 'Y'
    ELSE
      logs.dbg('Add File Problem');
      l_t_prbs.EXTEND;

      IF l_tmw_dist_extr_sw = 'Y' THEN
        l_t_prbs(l_t_prbs.LAST) := 'TMW Import failed! No valid records found to process.'
                                   || cnst.newline_char
                                   || 'RemoteFile: '
                                   || i_rmt_file_nm
                                   || cnst.newline_char
                                   || 'LocalFile : '
                                   || i_local_file_nm;
      ELSE
        l_t_prbs(l_t_prbs.LAST) := 'Import failed! The file cannot be accessed or has no valid records to process.'
                                   || cnst.newline_char
                                   || 'RemoteFile: '
                                   || i_rmt_file_nm
                                   || cnst.newline_char
                                   || 'LocalFile : '
                                   || i_local_file_nm;
      END IF;   -- l_tmw_dist_extr_sw = 'Y'

      logs.dbg('Notify for File Problem');
      notify_sp(i_div, l_t_prbs);
    END IF;   -- l_t_import.COUNT > 0

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END process_routed_sp;
END op_routing_pk;
/

