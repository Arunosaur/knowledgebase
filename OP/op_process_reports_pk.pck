CREATE OR REPLACE PACKAGE op_process_reports_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_PROCESS_REPORTS_PK
  ||  Report Specific procedures depending on Process-id, to generate reports
  ||  and send that to corresponding mail group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 05/06/02  |Santosh | Added Declarations for all reports to be able to call
  ||                    | directly or thru some external procedures.
  || 05/10/02 | Santosh | Removed gen_sql_util_errors_rep
  ||----------------------------------------------------------------------------
  */
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
-- To convert Transmission date(Rensoft) and time to its corresponding Timestamp
  FUNCTION convert_to_trans_ts(
    i_trnsmt_dt  IN  NUMBER,
    i_trnsmt_tm  IN  NUMBER
  )
    RETURN DATE;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
-- To Generate Late Orders Report
  PROCEDURE gen_late_order_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  );

-- To Generate Very Late Orders Report
  PROCEDURE gen_very_late_order_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  );

-- To Generate Orders Not Released Report
  PROCEDURE gen_orders_not_released_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  );

-- To Generate Loads Not Closed Report
  PROCEDURE gen_loads_not_closed_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  );

-- It calls various Report Specific procedures depending on the process_id
  PROCEDURE gen_reports_sp(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER DEFAULT 0
  );
END op_process_reports_pk;
/

CREATE OR REPLACE PACKAGE BODY op_process_reports_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_PROCESS_REPORTS_PK
  ||  Report Specific procedures depending on Process-id, to generate reports
  ||  and send that to corresponding mail group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/07/02 | Santosh | Fixed one bug, replaced Load no. with Order no. in all
  ||                    | report procedure within order_cur cursor.
  ||                    | Changed report's format to get better report.
  || 03/15/02 | Santosh | Deleted the function GET_GRP_ID, modified all report
  ||                    | procedures. Changed passing parameters oin the call of
  ||                    | Notigy_Group_sp from p_group_id to p_prcs_id and i_div,
  ||                    | to be in sync with the modified
  ||                    | op_process_common_pk.notify_group_sp.
  || 03/19/02 | Santosh | Changed Customer Name Column to print as a last column
  ||                    | in all the reports, to align the remaining column,
  ||                    | changed get_ord_msg and get_ord_header_msg functions
  || 03/25/02 | Santosh | Modified gen_loads_not_closed_rep, Added l_cur_ords for to
  ||                    | check whether any orders exists that are NOT IN('A','S','C')
  ||                    | for that load, then only notify about that load.
  || 04/25/02 | Santosh | Modified gen_loads_not_closed_rep, to fix logic error,
  ||                    | as few closed loads were being reported, Also some
  ||                    | loads were being reported more than one time in single
  ||                    | email.
  || 05/03/02 | Santosh | Added gen_sql_util_errors_rep procedure to create a
  ||                    | report having every entry since last run, from
  ||                    | sql_utilties table for all errors (sql_type = 'E').
  || 05/10/02 | Santosh | Removed gen_sql_util_errors_rep, and moved to
  ||                    | op_process_common_pk, to make execution of the
  ||                    | procedure prcs_dfn.intrvl_in_mins dependent
  || 06/13/02 | Sarat N | Modified SQLs to prevent any orders worked on by CSRs
  ||                    | from appearing in Late and Very Late Order reports.
  || 01/13/02 | Sarat N | Updated Later Order and Very Later Orders logic to use
  ||                    | Order Receipt timestamp instead of Comet transmitted
  ||                    | timestamp when checking orders. Comet timestamp will
  ||                    | still be used to determine if a given order is later or
  ||                    | very late order.
  || 06/12/08 | rhalpai | Change variable g_c_file_dir reference from '/ftptrans'
  ||                    | to '/opcig/ftptrans'. SDHD-505263
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_file_dir  CONSTANT VARCHAR2(50) := '/opcig/ftptrans';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || GET_HEADING_INFO_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_heading_info_fn(
    i_div          VARCHAR2,
    i_prcs_id      VARCHAR2,
    i_last_run_ts  DATE,
    i_cur_ts       DATE,
    i_cutoff_adj   NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.GET_HEADING_INFO_FN';
    lar_parm             logs.tar_parm;
    l_heading            VARCHAR2(400) := '';
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTs', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurTs', i_cur_ts);
    logs.add_parm(lar_parm, 'CutOffAdj', i_cutoff_adj);
    l_heading := '  '
                 || UPPER(i_prcs_id)
                 || ' Report, Division: '
                 || i_div
                 || cnst.newline_char
                 || '     Current Time: '
                 || TO_CHAR(i_cur_ts, 'MM/DD/YYYY HH24:MI')
                 || cnst.newline_char
                 || '     Last Run Time: '
                 || TO_CHAR(i_last_run_ts, 'MM/DD/YYYY HH24:MI')
                 || cnst.newline_char
                 || '     Cut off Adj(in Min) : '
                 || i_cutoff_adj
                 || cnst.newline_char
                 || cnst.newline_char
                 || cnst.newline_char;
    RETURN(l_heading);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_heading_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_TEMP_CNT_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_temp_cnt_fn
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.GET_TEMP_CNT_FN';
    lar_parm             logs.tar_parm;
    l_cnt                NUMBER;
  BEGIN
    SELECT COUNT(ROWID)
      INTO l_cnt
      FROM temp_load_cut_off;

    RETURN(l_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_temp_cnt_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LOAD_MSG_FN
  ||  Get the load info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_load_msg_fn(
    i_load_num          IN  VARCHAR2,
    i_load_dest         IN  VARCHAR2,
    i_ord_cutoff_ts     IN  DATE,
    i_llr_cutoff_ts     IN  DATE,
    i_close_cutoff_ts   IN  DATE,
    i_depart_cutoff_ts  IN  DATE
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.GET_LOAD_MSG_FN';
    lar_parm             logs.tar_parm;
    l_load_msg           VARCHAR2(400);
  BEGIN
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'LoadDest', i_load_dest);
    logs.add_parm(lar_parm, 'OrdCutOffTs', i_ord_cutoff_ts);
    logs.add_parm(lar_parm, 'LlrCutOffTs', i_llr_cutoff_ts);
    logs.add_parm(lar_parm, 'CloseCutOffTs', i_close_cutoff_ts);
    logs.add_parm(lar_parm, 'DepartCutOffTs', i_depart_cutoff_ts);
    l_load_msg := cnst.newline_char
                  || '-----------------------------------------------------------------------------------------'
                  || cnst.newline_char
                  || ' Load No: '
                  || i_load_num
                  || cnst.newline_char
                  || ' Load Name: '
                  || i_load_dest
                  || cnst.newline_char
                  || cnst.newline_char
                  || ' Cut Off Timestamps: '
                  || cnst.newline_char
                  || '    ORD:  '
                  || TO_CHAR(i_ord_cutoff_ts, 'MM/DD/YYYY HH24:MI')
                  || cnst.newline_char
                  || '    LLR:  '
                  || TO_CHAR(i_llr_cutoff_ts, 'MM/DD/YYYY HH24:MI')
                  || cnst.newline_char
                  || '    CLO:  '
                  || TO_CHAR(i_close_cutoff_ts, 'MM/DD/YYYY HH24:MI')
                  || cnst.newline_char
                  || '    DEP:  '
                  || TO_CHAR(i_depart_cutoff_ts, 'MM/DD/YYYY HH24:MI')
                  || cnst.newline_char;
    RETURN(l_load_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_load_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ORD_HEADER_MSG_FN
  ||  To get Ordere header info
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/19/02 | Santosh | Changed Customer Name Column to print as a last
  ||                    | column in all the reports, to align the remaining
  ||                    | column
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_ord_header_msg_fn(
    i_rpt_typ  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.GET_ORD_HEADER_MSG_FN';
    lar_parm             logs.tar_parm;
    l_ord_msg            VARCHAR2(400);
  BEGIN
    logs.add_parm(lar_parm, 'RptTyp', i_rpt_typ);

    IF i_rpt_typ = 'Orders Not Released' THEN
      l_ord_msg := cnst.newline_char
                   || ' Corp#'
                   || ' MCL Cust# '
                   || ' Stop No   '
                   || ' Order No '
                   || ' Total Lines '
                   || RPAD(' Customer Name ', 30, '  ')
                   || '        '
                   || cnst.newline_char
                   || cnst.newline_char;
    ELSE
      l_ord_msg := cnst.newline_char
                   || ' Corp#'
                   || ' MCL Cust# '
                   || ' Stop No   '
                   || ' Order No '
                   || ' Total Lines '
                   || ' Trans. TS '
                   || RPAD(' Customer Name ', 30, '  ')
                   || '        '
                   || cnst.newline_char
                   || cnst.newline_char;
    END IF;

    RETURN(l_ord_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_ord_header_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ORD_MSG_FN
  ||  To get info for a Order line
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/19/02 | Santosh | Changed Customer Name Column to print as a last
  ||                    | column in all the reports, to align the remaining
  ||                    | column
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_ord_msg_fn(
    i_crp_cd      IN  NUMBER,
    i_mcl_cust    IN  VARCHAR2,
    i_cust_nm     IN  VARCHAR2,
    i_stop_num    IN  NUMBER,
    i_ord_num     IN  NUMBER,
    i_ord_ln_cnt  IN  NUMBER,
    i_trnsmt_ts   IN  DATE DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.GET_ORD_MSG_FN';
    lar_parm             logs.tar_parm;
    l_ord_msg            VARCHAR2(400);
  BEGIN
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'CustNm', i_cust_nm);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLnCnt', i_ord_ln_cnt);
    logs.add_parm(lar_parm, 'TrnsmtTs', i_trnsmt_ts);

    IF i_trnsmt_ts IS NULL THEN
      l_ord_msg := '  '
                   || LPAD(TO_CHAR(i_crp_cd), 3, ' ')
                   || '  '
                   || '  '
                   || i_mcl_cust
                   || '  '
                   || ''
                   || LPAD(TO_CHAR(i_stop_num), 9, ' ')
                   || ' '
                   || ''
                   || LPAD(TO_CHAR(i_ord_num), 11, ' ')
                   || '  '
                   || '  '
                   || LPAD(TO_CHAR(i_ord_ln_cnt), 7, ' ')
                   || '  '
                   || '      '
                   || RPAD(i_cust_nm, 30, ' ')
                   || '  '
                   || cnst.newline_char;
    ELSE
      l_ord_msg := '  '
                   || LPAD(TO_CHAR(i_crp_cd), 3, ' ')
                   || '  '
                   || '  '
                   || i_mcl_cust
                   || '  '
                   || ''
                   || LPAD(TO_CHAR(i_stop_num), 9, ' ')
                   || ' '
                   || ''
                   || LPAD(TO_CHAR(i_ord_num), 11, ' ')
                   || '  '
                   || '  '
                   || LPAD(TO_CHAR(i_ord_ln_cnt), 7, ' ')
                   || '  '
                   || '  '
                   || TO_CHAR(i_trnsmt_ts, 'MM/DD/YY HH24:MI')
                   || '   '
                   || '      '
                   || RPAD(i_cust_nm, 30, ' ')
                   || '  '
                   || cnst.newline_char;
    END IF;

    RETURN(l_ord_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_ord_msg_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || CONVERT_TO_TRANS_TS
  ||  Convert Transmission date(Rensoft) and time to its corresponding Timestamp
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION convert_to_trans_ts(
    i_trnsmt_dt  IN  NUMBER,
    i_trnsmt_tm  IN  NUMBER
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.CONVERT_TO_TRANS_TS';
    lar_parm             logs.tar_parm;
    l_trnsmt_ts          DATE;
  BEGIN
    logs.add_parm(lar_parm, 'TrnsmtDt', i_trnsmt_dt);
    logs.add_parm(lar_parm, 'TrnsmtTm', i_trnsmt_tm);
    l_trnsmt_ts := TO_DATE('19000228' || LPAD(TO_CHAR(i_trnsmt_tm), 6, '0'), 'YYYYMMDDHH24MISS') + i_trnsmt_dt;
    RETURN(l_trnsmt_ts);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END convert_to_trans_ts;

  /*
  ||----------------------------------------------------------------------------
  || GEN_LATE_ORDER_REP
  ||  To Generate Late Orders Report
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/07/02 | Santosh | Fixed one bug, replaced Load no. with Order no.
  ||                    | within l_cur_ords cursor.
  || 03/15/02 | Santosh | Changed passing parameters in the call of
  ||                    | Notigy_Group_sp from p_group_id to p_prcs_id and
  ||                    | i_div, to be in sync with the modified
  ||                    | op_process_common_pk.notify_group_sp.
  || 01/03/02 | Sarat N | Updated logic to check all orders received by OP
  ||                    | since the last execution of this process. We were
  ||                    | using Comet Transmission time before, but this could
  ||                    | be off due to Order Blackouts on Mainframe.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_late_order_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_REPORTS_PK.GEN_LATE_ORDER_REP';
    lar_parm             logs.tar_parm;
    l_subj               VARCHAR2(256);
    l_heading            VARCHAR2(400);
    l_t_rpt_lns          typ.tas_maxvc2;
    l_load_msg           VARCHAR2(2000);
    l_ord_msg            VARCHAR2(2000);
    l_load_cnt           PLS_INTEGER    := 0;
    l_temp_cnt           PLS_INTEGER    := 0;
    l_flag               BOOLEAN        := FALSE;
    l_file_nm            VARCHAR2(50);
    l_ord_flag           BOOLEAN        := TRUE;
    l_load_flag          BOOLEAN        := FALSE;

    -- Cursor to get all load records from TEMP table
    CURSOR l_cur_temp IS
      SELECT load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
        FROM temp_load_cut_off;

    -- Cursor to get all Customer and Order info corresponding to each load
    CURSOR l_cur_ords(
      b_div             VARCHAR2,
      b_ord_cut_off_ts  DATE,
      b_llr_cut_off_ts  DATE,
      b_last_run_ts     DATE,
      b_cur_ts          DATE,
      b_load_num        VARCHAR2
    ) IS
      SELECT   ld.load_num, cx.corpb AS crp_cd, se.cust_id, se.stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm,
               a.ordnoa AS ord_num,
               (SELECT COUNT(*)
                  FROM ordp120b b
                 WHERE b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, convert_to_trans_ts(a.trndta, a.trntma) AS trnsmt_ts
          FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, mclp020b cx, sysp200c c
         WHERE d.div_id = b_div
           AND ld.div_part = d.div_part
           AND ld.load_num = b_load_num
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND convert_to_trans_ts(a.trndta, a.trntma) BETWEEN b_ord_cut_off_ts AND b_llr_cut_off_ts
           AND a.ord_rcvd_ts BETWEEN b_last_run_ts AND b_cur_ts
           AND NVL(a.ipdtsa, 'X') <> 'CSRWRK'
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND cx.div_part = a.div_part
           AND cx.custb = a.custa
           AND c.div_part = a.div_part
           AND c.acnoc = a.custa
      ORDER BY ld.load_num, cx.corpb, se.cust_id, a.ordnoa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTs', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurTs', i_cur_ts);
    logs.add_parm(lar_parm, 'CutOffAdj', i_cutoff_adj);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || 'Late_Orders.dat';
    l_subj := 'LATE ORDERS Report, Div: ' || i_div || ', ' || TO_CHAR(i_cur_ts, 'MM/DD/YYYY HH24:MI');
    l_heading := get_heading_info_fn(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    util.append(l_t_rpt_lns, l_heading);
    logs.dbg('Get Temp Count');
    l_temp_cnt := get_temp_cnt_fn();
    logs.dbg('Get Load Data');
    FOR l_r_temp IN l_cur_temp LOOP
      l_load_msg := get_load_msg_fn(l_r_temp.load_num,
                                    l_r_temp.load_nm,
                                    l_r_temp.ord_cut_off_ts,
                                    l_r_temp.llr_cut_off_ts,
                                    l_r_temp.clos_cut_off_ts,
                                    l_r_temp.deptur_cut_off_ts
                                   );
      l_ord_msg := get_ord_header_msg_fn();
      logs.dbg('Get Order Data');
      FOR l_r_ord IN l_cur_ords(i_div,
                                l_r_temp.ord_cut_off_ts,
                                l_r_temp.llr_cut_off_ts,
                                i_last_run_ts,
                                i_cur_ts,
                                l_r_temp.load_num
                               ) LOOP
        IF l_ord_flag THEN
          util.append(l_t_rpt_lns, l_load_msg);
          util.append(l_t_rpt_lns, l_ord_msg);
        END IF;

        logs.dbg('Get Order Lines');
        l_ord_msg := get_ord_msg_fn(l_r_ord.crp_cd,
                                    l_r_ord.mcl_cust,
                                    l_r_ord.cust_nm,
                                    l_r_ord.stop_num,
                                    l_r_ord.ord_num,
                                    l_r_ord.ord_ln_cnt,
                                    l_r_ord.trnsmt_ts
                                   );
        l_flag := TRUE;
        l_ord_flag := FALSE;
        l_load_flag := TRUE;
        util.append(l_t_rpt_lns, l_ord_msg);
      END LOOP;

      IF l_flag THEN
        l_load_cnt := l_load_cnt + 1;
        l_flag := FALSE;
        l_ord_flag := TRUE;
      END IF;
    END LOOP;
    util.append(l_t_rpt_lns,
                cnst.newline_char || cnst.newline_char || '         ----------- End Of The Report -----------'
               );
    write_sp(l_t_rpt_lns, l_file_nm);

    IF l_load_flag THEN
      op_process_common_pk.notify_group_sp(i_div, i_prcs_id, l_subj, g_c_file_dir || '/' || l_file_nm, l_heading);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_late_order_rep;

  /*
  ||----------------------------------------------------------------------------
  || GEN_VERY_LATE_ORDER_REP
  ||  To Generate Very Late Orders Report
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/07/02 | Santosh | Fixed one bug, replaced Load no. with Order no.
  ||                    | within l_cur_ords cursor.
  || 03/15/02 | Santosh | Changed passing parameters in the call of
  ||                    | Notigy_Group_sp from p_group_id to p_prcs_id and
  ||                    | i_div, to be in sync with the modified
  ||                    | op_process_common_pk.notify_group_sp.
  || 01/03/03 | Sarat N | Updated logic to check all orders received by OP
  ||                    | since the last execution of this process. We were
  ||                    | using Comet Transmission time before, but this could
  ||                    | be off due to Order Blackouts on Mainframe.
  || 01/23/03 | Sudheer | Updated logic to include customers' current load and
  ||                    | next load forcomparison. When an order is received
  ||                    | after LLRCutoff, the order is automatically added to
  ||                    | the customer's next load. This order still needs to
  ||                    | be reported in the VeryLaterOrders report and
  ||                    | therefore we need to include current and next loads
  ||                    | for comparisons.
  || 02/04/03 | Sarat N | Updated l_cur_ords cursor sql to exclude any
  ||                    | Distribution ('DIST') orders that were received
  ||                    | during the Very Late Order interval. Also, corrected
  ||                    | spelling error in message printed on report.
  || 03/04/03 | Sarat N | Added new line character (CHR(13)) at the end of each
  ||                    | later order added to the report.
  || 11/10/10 | rhalpai | Replace reference to unused column divsnb with swhsb.
  ||                    | Convert to use standard error handling logic. PIR5878
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_very_late_order_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_REPORTS_PK.GEN_VERY_LATE_ORDER_REP';
    lar_parm             logs.tar_parm;
    l_heading            VARCHAR2(1000) := '';
    l_subj               VARCHAR2(256)  := '';
    l_load_msg           VARCHAR2(2000) := '';
    l_ord_msg            VARCHAR2(2000) := '';
    l_load_cnt           PLS_INTEGER    := 0;
    l_temp_cnt           PLS_INTEGER    := 0;
    l_flag               BOOLEAN        := FALSE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_file_nm            VARCHAR2(50);
    l_ord_flag           BOOLEAN        := TRUE;
    l_load_flag          BOOLEAN        := FALSE;

    CURSOR l_cur_temp IS
      SELECT load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
        FROM temp_load_cut_off;

    CURSOR l_cur_custs(
      b_div       VARCHAR2,
      b_load_num  VARCHAR2
    ) IS
      SELECT md.custd
        FROM div_mstr_di1d d, mclp040d md
       WHERE d.div_id = b_div
         AND md.div_part = d.div_part
         AND md.loadd = b_load_num;

    CURSOR l_cur_ords(
      b_div          VARCHAR2,
      b_llr_ts       DATE,
      b_last_run_ts  DATE,
      b_cur_ts       DATE,
      b_cust_id      VARCHAR2,
      b_cut_off_adj  NUMBER
    ) IS
      SELECT   ld.load_num, cx.corpb AS crp_cd, se.cust_id, se.stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm,
               a.ordnoa AS ord_num,
               (SELECT COUNT(*)
                  FROM ordp120b b
                 WHERE b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, convert_to_trans_ts(a.trndta, a.trntma) AS trnsmt_ts
          FROM div_mstr_di1d d, mclp020b cx, sysp200c c, mclp040d md, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
         WHERE d.div_id = b_div
           AND cx.div_part = d.div_part
           AND cx.custb = b_cust_id
           AND c.div_part = cx.div_part
           AND c.acnoc = cx.custb
           AND md.div_part = d.div_part
           AND md.custd = cx.custb
           AND a.div_part = cx.div_part
           AND a.custa = cx.custb
           AND convert_to_trans_ts(a.trndta, a.trntma) BETWEEN b_llr_ts AND(b_llr_ts +(b_cut_off_adj /(24 * 60)))
           AND convert_to_trans_ts(a.trndta, a.trntma) >= b_last_run_ts
           AND a.ord_rcvd_ts BETWEEN b_last_run_ts AND b_cur_ts
           AND NVL(a.ipdtsa, 'X') <> 'CSRWRK'
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND ld.load_num <> 'DIST'
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
      ORDER BY ld.load_num, cx.corpb, se.cust_id, a.ordnoa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTs', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurTs', i_cur_ts);
    logs.add_parm(lar_parm, 'CutOffAdj', i_cutoff_adj);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || 'Very_Late_Orders.dat';
    l_subj := 'VERY LATE ORDERS Report, Div: ' || i_div || ', ' || TO_CHAR(i_cur_ts, 'MM/DD/YYYY HH24:MI');
    l_heading := get_heading_info_fn(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    util.append(l_t_rpt_lns, l_heading);
    logs.dbg('Get Temp Count');
    l_temp_cnt := get_temp_cnt_fn();
    logs.dbg('Get Load Data');
    FOR l_r_temp IN l_cur_temp LOOP
      l_load_msg := get_load_msg_fn(l_r_temp.load_num,
                                    l_r_temp.load_nm,
                                    l_r_temp.ord_cut_off_ts,
                                    l_r_temp.llr_cut_off_ts,
                                    l_r_temp.clos_cut_off_ts,
                                    l_r_temp.deptur_cut_off_ts
                                   );
      l_ord_msg := get_ord_header_msg_fn();
      logs.dbg('Get Customer');
      FOR l_r_cust IN l_cur_custs(i_div, l_r_temp.load_num) LOOP
        logs.dbg('Get Order Data');
        FOR l_r_ord IN l_cur_ords(i_div, l_r_temp.llr_cut_off_ts, i_last_run_ts, i_cur_ts, l_r_cust.custd,
                                  i_cutoff_adj) LOOP
          IF (l_ord_flag) THEN
            util.append(l_t_rpt_lns, l_load_msg);
            util.append(l_t_rpt_lns, l_ord_msg);
          END IF;

          logs.dbg('Get Order Lines');
          l_ord_msg := get_ord_msg_fn(l_r_ord.crp_cd,
                                      l_r_ord.mcl_cust,
                                      l_r_ord.cust_nm,
                                      l_r_ord.stop_num,
                                      l_r_ord.ord_num,
                                      l_r_ord.ord_ln_cnt,
                                      l_r_ord.trnsmt_ts
                                     );

          IF l_r_temp.load_num <> l_r_ord.load_num THEN
            l_ord_msg := l_ord_msg
                         || ' ** Note:This is a Very Late Order for Load:'
                         || l_r_temp.load_num
                         || '. This order has been Moved to Load: '
                         || l_r_ord.load_num
                         || '.'
                         || cnst.newline_char;
          END IF;

          l_flag := TRUE;
          l_ord_flag := FALSE;
          l_load_flag := TRUE;
          util.append(l_t_rpt_lns, l_ord_msg);
        END LOOP;
      END LOOP;

      IF l_flag THEN
        l_load_cnt := l_load_cnt + 1;
        l_flag := FALSE;
        l_ord_flag := TRUE;
      END IF;
    END LOOP;
    util.append(l_t_rpt_lns,
                cnst.newline_char || cnst.newline_char || '         ----------- End Of The Report -----------'
               );
    write_sp(l_t_rpt_lns, l_file_nm);

    IF l_load_flag THEN
      op_process_common_pk.notify_group_sp(i_div, i_prcs_id, l_subj, g_c_file_dir || '/' || l_file_nm, l_heading);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_very_late_order_rep;

  /*
  ||----------------------------------------------------------------------------
  || GEN_ORDERS_NOT_RELEASED_REP
  ||  To Generate Orders Not Released Report
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/07/02 | Santosh | Fixed one bug, replaced Load no. with Order no.
  ||                    | within l_cur_ords cursor.
  || 03/15/02 | Santosh | Changed passing parameters in the call of
  ||                    | Notigy_Group_sp from p_group_id to i_prcs_id and
  ||                    | i_div, to be in sync with the modified
  ||                    | op_process_common_pk.notify_group_sp.
  || 01/21/03 | Sarat N | Updated cursor where clause to use to_rendate_dt
  ||                    | function instead of comparing a date object with
  ||                    | varchar2 date value that was resulting in "literal
  ||                    | does not match format string" error.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_orders_not_released_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_REPORTS_PK.GEN_ORDERS_NOT_RELEASED_REP';
    lar_parm             logs.tar_parm;
    l_heading            VARCHAR2(1000) := '';
    l_subj               VARCHAR2(256)  := '';
    l_load_msg           VARCHAR2(2000) := '';
    l_ord_msg            VARCHAR2(2000) := '';
    l_load_cnt           PLS_INTEGER    := 0;
    l_temp_cnt           PLS_INTEGER    := 0;
    l_flag               BOOLEAN        := FALSE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_file_nm            VARCHAR2(50);
    l_ord_flag           BOOLEAN        := TRUE;
    l_load_flag          BOOLEAN        := FALSE;

    CURSOR l_cur_temp IS
      SELECT load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
        FROM temp_load_cut_off;

    CURSOR l_cur_ords(
      b_div       VARCHAR2,
      b_llr_ts    DATE,
      b_load_num  VARCHAR2
    ) IS
      SELECT   ld.load_num, cx.corpb AS crp_cd, se.cust_id, se.stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm,
               a.ordnoa AS ord_num,
               (SELECT COUNT(*)
                  FROM ordp120b b
                 WHERE b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.lineb = FLOOR(b.lineb)
                   AND b.statb IN('O', 'I')) AS ord_ln_cnt
          FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, mclp020b cx, sysp200c c
         WHERE d.div_id = b_div
           AND ld.div_part = d.div_part
           AND ld.llr_dt = TRUNC(b_llr_ts)
           AND ld.load_num = b_load_num
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND EXISTS(SELECT 1
                        FROM ordp120b b
                       WHERE b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.statb IN('O', 'I'))
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND cx.div_part = a.div_part
           AND cx.custb = a.custa
           AND c.div_part = a.div_part
           AND c.acnoc = a.custa
      ORDER BY ld.load_num, cx.corpb, se.cust_id, a.ordnoa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTs', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurTs', i_cur_ts);
    logs.add_parm(lar_parm, 'CutOffAdj', i_cutoff_adj);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || 'Orders_Not_Released.dat';
    l_subj := 'ORDERS NOT RELEASED Report, Div: ' || i_div || ', ' || TO_CHAR(i_cur_ts, 'MM/DD/YYYY HH24:MI');
    l_heading := get_heading_info_fn(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    util.append(l_t_rpt_lns, l_heading);
    logs.dbg('Get Temp Count');
    l_temp_cnt := get_temp_cnt_fn();
    logs.dbg('Get Load Data');
    FOR l_r_temp IN l_cur_temp LOOP
      l_load_msg := get_load_msg_fn(l_r_temp.load_num,
                                    l_r_temp.load_nm,
                                    l_r_temp.ord_cut_off_ts,
                                    l_r_temp.llr_cut_off_ts,
                                    l_r_temp.clos_cut_off_ts,
                                    l_r_temp.deptur_cut_off_ts
                                   );
      l_ord_msg := get_ord_header_msg_fn('Orders Not Released');
      logs.dbg('Get Order Data');
      FOR l_r_ord IN l_cur_ords(i_div, l_r_temp.llr_cut_off_ts, l_r_temp.load_num) LOOP
        IF (l_ord_flag) THEN
          util.append(l_t_rpt_lns, l_load_msg);
          util.append(l_t_rpt_lns, l_ord_msg);
        END IF;

        logs.dbg('Get Order Lines');
        l_ord_msg := get_ord_msg_fn(l_r_ord.crp_cd,
                                    l_r_ord.mcl_cust,
                                    l_r_ord.cust_nm,
                                    l_r_ord.stop_num,
                                    l_r_ord.ord_num,
                                    l_r_ord.ord_ln_cnt,
                                    NULL
                                   );
        l_flag := TRUE;
        l_ord_flag := FALSE;
        l_load_flag := TRUE;
        util.append(l_t_rpt_lns, l_ord_msg);
      END LOOP;

      IF l_flag THEN
        l_load_cnt := l_load_cnt + 1;
        l_flag := FALSE;
        l_ord_flag := TRUE;
      END IF;
    END LOOP;
    util.append(l_t_rpt_lns,
                cnst.newline_char || cnst.newline_char || '         ----------- End Of The Report -----------'
               );
    write_sp(l_t_rpt_lns, l_file_nm);

    IF l_load_flag THEN
      op_process_common_pk.notify_group_sp(i_div, i_prcs_id, l_subj, g_c_file_dir || '/' || l_file_nm, l_heading);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_orders_not_released_rep;

  /*
  ||----------------------------------------------------------------------------
  || GEN_LOADS_NOT_CLOSED_REP
  ||  To Generate Loads Not Closed Report
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/07/02 | Santosh | Fixed one bug, replaced Load no. with Order no.
  ||                    | within l_cur_ords cursor.
  || 03/15/02 | Santosh | Changed passing parameters in the call of
  ||                    | Notigy_Group_sp from p_group_id to i_prcs_id and
  ||                    | i_div, to be in sync with the modified
  ||                    | op_process_common_pk.notify_group_sp.
  || 03/25/02 | Santosh | Added l_cur_ords for to check whether any orders
  ||                    | exists that are NOT IN ('A', 'S', 'C') for that load,
  ||                    | then only notify about that load.
  || 04/25/02 | Santosh | Modified l_cur_ords to fix logic error, as few
  ||                    | closed loads were being reported, Also some loads
  ||                    | were being reported more than one time in single
  ||                    | email. Now using mclp370C instead of ordp120b.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  || 10/15/13 | rhalpai | Change to include Div in file name. IM-121701
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_loads_not_closed_rep(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_REPORTS_PK.GEN_LOADS_NOT_CLOSED_REP';
    lar_parm             logs.tar_parm;
    l_heading            VARCHAR2(1000) := '';
    l_subj               VARCHAR2(256)  := '';
    l_load_msg           VARCHAR2(2000) := '';
    l_temp_cnt           NUMBER         := 0;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_file_nm            VARCHAR2(50);
    l_load_flag          BOOLEAN        := FALSE;

    CURSOR l_cur_temp IS
      SELECT load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
        FROM temp_load_cut_off;

    CURSOR l_cur_ords(
      b_div            VARCHAR2,
      b_load_num       VARCHAR2,
      b_llr_cutoff_ts  DATE
    ) IS
      SELECT 'X'
        FROM DUAL
       WHERE EXISTS(SELECT 'X'
                      FROM div_mstr_di1d d, mclp370c mc
                     WHERE d.div_id = b_div
                       AND mc.div_part = d.div_part
                       AND mc.load_status <> 'A'
                       AND mc.loadc = b_load_num
                       AND mc.llr_date = b_llr_cutoff_ts - DATE '1900-02-28');
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTs', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurTs', i_cur_ts);
    logs.add_parm(lar_parm, 'CutOffAdj', i_cutoff_adj);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || 'Loads_Not_Closed.dat';
    l_subj := 'LOADS NOT CLOSED Report, Div: ' || i_div || ', ' || TO_CHAR(i_cur_ts, 'MM/DD/YYYY HH24:MI');
    l_heading := get_heading_info_fn(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    util.append(l_t_rpt_lns, l_heading);
    logs.dbg('Get Temp Count');
    l_temp_cnt := get_temp_cnt_fn();
    logs.dbg('Get Load Data');
    FOR l_r_temp IN l_cur_temp LOOP
      FOR l_r_ord IN l_cur_ords(i_div, l_r_temp.load_num, l_r_temp.llr_cut_off_ts) LOOP
        l_load_msg := get_load_msg_fn(l_r_temp.load_num,
                                      l_r_temp.load_nm,
                                      l_r_temp.ord_cut_off_ts,
                                      l_r_temp.llr_cut_off_ts,
                                      l_r_temp.clos_cut_off_ts,
                                      l_r_temp.deptur_cut_off_ts
                                     );
        util.append(l_t_rpt_lns, l_load_msg);
        l_load_flag := TRUE;
      END LOOP;
    END LOOP;
    util.append(l_t_rpt_lns,
                cnst.newline_char || cnst.newline_char || '         ----------- End Of The Report -----------'
               );
    write_sp(l_t_rpt_lns, l_file_nm);

    IF l_load_flag THEN
      op_process_common_pk.notify_group_sp(i_div, i_prcs_id, l_subj, g_c_file_dir || '/' || l_file_nm, l_heading);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_loads_not_closed_rep;

  /*
  ||----------------------------------------------------------------------------
  || GEN_REPORTS_SP
  ||  It calls various Report Specific procedures depending on the process_id
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_reports_sp(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_cur_ts       IN  DATE,
    i_cutoff_adj   IN  NUMBER DEFAULT 0
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_REPORTS_PK.GEN_REPORTS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTs', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurTs', i_cur_ts);
    logs.add_parm(lar_parm, 'CutOffAdj', i_cutoff_adj);
    logs.info('ENTRY', lar_parm);

    IF i_prcs_id = 'Late Orders' THEN
      gen_late_order_rep(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    ELSIF i_prcs_id = 'Very Late Orders' THEN
      gen_very_late_order_rep(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    ELSIF i_prcs_id = 'Orders Not Released' THEN
      gen_orders_not_released_rep(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    ELSIF i_prcs_id = 'Loads Not Closed' THEN
      gen_loads_not_closed_rep(i_div, i_prcs_id, i_last_run_ts, i_cur_ts, i_cutoff_adj);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_reports_sp;
END op_process_reports_pk;
/

