CREATE OR REPLACE PACKAGE op_status_inquiry_pk IS
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
  FUNCTION actv_prcs_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_recpt_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION last_ord_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_waiting_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_in_prcss_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION last_rlse_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION trans_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION inv_trans_cur_fn(
    i_div        IN  VARCHAR2,
    i_from_dt    IN  VARCHAR2,
    i_to_dt      IN  VARCHAR2,
    i_tran_typ   IN  VARCHAR2 DEFAULT NULL,
    i_catlg_num  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION audit_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION forc_load_clos_cur_fn(
    i_div      IN  VARCHAR2,
    i_from_dt  IN  VARCHAR2,
    i_to_dt    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION except_log_cur_fn(
    i_div       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION level_1_except_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION lineout_cur_fn(
    i_div      IN  VARCHAR2,
    i_from_dt  IN  VARCHAR2,
    i_to_dt    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_inq_cur_fn(
    i_div       IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_sum_cur_fn(
    i_div       IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION max_qty_applied_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION no_order_cur_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION order_qty_cur_fn(
    i_div      IN  VARCHAR2,
    i_ord_qty  IN  PLS_INTEGER
  )
    RETURN SYS_REFCURSOR;

  FUNCTION pallet_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION specl_dist_ship_dates_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION unbilled_specl_dist_cur_fn(
    i_div          IN  VARCHAR2,
    i_max_ship_dt  IN  VARCHAR2,
    i_sort         IN  VARCHAR2 DEFAULT 'SHIPDATE'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION tobacco_cur_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION allocated_items_cur_fn(
    i_div             IN  VARCHAR2,
    i_catlg_num_list  IN  VARCHAR2,
    i_llr_dt          IN  VARCHAR2,
    i_load_num        IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION item_slot_info_cur_fn(
    i_div             IN  VARCHAR2,
    i_catlg_num_list  IN  VARCHAR2,
    i_jrsdctn         IN  VARCHAR2 DEFAULT NULL,
    i_item_descr      IN  VARCHAR2 DEFAULT NULL,
    i_slot            IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE ord_recpt_sp(
    i_div               IN      VARCHAR2,
    o_cur_last_ord      OUT     SYS_REFCURSOR,
    o_cur_ord_waiting   OUT     SYS_REFCURSOR,
    o_cur_ord_in_prcss  OUT     SYS_REFCURSOR
  );

  PROCEDURE mq_status_sp(
    i_div              IN      VARCHAR2,
    o_cur_get_waiting  OUT     SYS_REFCURSOR,
    o_cur_get_in_prcs  OUT     SYS_REFCURSOR,
    o_cur_put_in_prcs  OUT     SYS_REFCURSOR
  );

  PROCEDURE load_eta_issues_cur_sp(
    i_div  IN  VARCHAR2,
    o_cur  OUT SYS_REFCURSOR
  );
END op_status_inquiry_pk;
/

CREATE OR REPLACE PACKAGE BODY op_status_inquiry_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

  /*
  ||----------------------------------------------------------------------------
  || ACTV_PRCS_CUR_FN
  ||  Build a cursor of active processes.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/15/07 | rhalpai | Original - Created for PIR3593
  || 02/25/15 | rhalpai | Change 'Check PrcsCntl Active' logic to replace
  ||                    | SELECT from PRCS_CNTL tables with call to
  ||                    | OP_PROCESS_CONTROL_PK.GET_ACTIVE_RESTRICTIONS_FN to
  ||                    | gather active processes. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION actv_prcs_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_div_part     NUMBER;
    l_t_actv_prcs  type_stab;
    l_cv           SYS_REFCURSOR;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_actv_prcs := op_process_control_pk.get_active_restrictions_fn(NULL, l_div_part);

    OPEN l_cv
     FOR
       SELECT   SUBSTR(t.column_value, 1, INSTR(t.column_value, ' ') - 1) AS prcs_id,
                SUBSTR(t.column_value, INSTR(t.column_value, ' ') + 1) AS descr
           FROM TABLE(CAST(l_t_actv_prcs AS type_stab)) t
       ORDER BY 1;

    RETURN(l_cv);
  END actv_prcs_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_RECPT_FN
  ||  Displays Order Receipt status.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add logic to process with wrapper for ssh to
  ||                    | Application Server. PIR11038
  || 11/02/20 | rhalpai | Change oscmd_fn to ssh to new MQ server. SDHD-813035
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_recpt_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_STATUS_INQUIRY_PK.ORD_RECPT_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_mq_srvr            mclane_mq_gp_control.hostname%TYPE;
    l_msg                typ.t_maxvc2;
    l_cv                 SYS_REFCURSOR;
    l_cmd                typ.t_maxvc2;

    CURSOR l_cur_ords(
      b_div_part  NUMBER
    ) IS
      SELECT   t.msg_descr AS typ,
               DECODE(m.msg_status,
                      'O', 'In-Process',
                      'F', 'Waiting to Re-attempt',
                      'E', 'Errored',
                      'C', 'Cancelled',
                      'Inactive'
                     ) AS stat,
               COUNT(m.msg_seq) AS cnt
          FROM mclane_order_receipt_msgs m,
               (SELECT 'DIS' AS msg_type, 'Distribution' AS msg_descr
                  FROM DUAL
                UNION ALL
                SELECT 'REG' AS msg_type, 'Regular' AS msg_descr
                  FROM DUAL
                UNION ALL
                SELECT 'CSR' AS msg_type, 'CSR' AS msg_descr
                  FROM DUAL) t
         WHERE m.div_part(+) = b_div_part
           AND m.msg_type(+) = t.msg_type
      GROUP BY t.msg_descr, m.msg_status
      ORDER BY t.msg_descr, m.msg_status;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('Get MQ Server');

    SELECT c.hostname
      INTO l_mq_srvr
      FROM mclane_mq_gp_control c
     WHERE c.div_part = l_div_part
       AND c.mq_msg_id = 'QOCOL01';

    logs.dbg('Open Cursor for Last Order Processed');

    OPEN l_cv
     FOR
       SELECT LPAD(a.ordnoa, 11) || ' ' || TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_info
         FROM ordp100a a
        WHERE a.div_part = l_div_part
          AND a.ordnoa = (SELECT MAX(o.ordnoa) AS ord_num
                            FROM ordp100a o
                           WHERE o.div_part = l_div_part);

    logs.dbg('Fetch Cursor for Last Order Processed');

    FETCH l_cv
     INTO l_msg;

    l_msg := 'Last Order Processed'
             || cnst.newline_char
             || 'OrderNum    OrderReceiptTS'
             || cnst.newline_char
             || '----------- -------------------'
             || cnst.newline_char
             || l_msg;
    logs.dbg('Get Orders Waiting to be Processed');
    l_cmd := 'ssh '
             || l_mq_srvr
             || ' runmqsc -e << EOF | grep -e RENA.OP -e CURDEPTH |\'
             || cnst.newline_char
             || 'sed -e "/.*QUEUE.*/{'
             || cnst.newline_char
             || 'N'
             || cnst.newline_char
             || 's/.*QOCSR01.*\\n.*CURDEPTH/CSR          /'
             || cnst.newline_char
             || 's/.*QDIST01.*\\n.*CURDEPTH/Distribution /'
             || cnst.newline_char
             || 's/.*QOCOL01.*\\n.*CURDEPTH/Regular      /'
             || cnst.newline_char
             || '}" |\'
             || cnst.newline_char
             || 'sed -e "s/(0)/NONE/"'
             || cnst.newline_char
             || '  dis ql('
             || i_div
             || '.RENA.OP.QOCSR01) curdepth'
             || cnst.newline_char
             || '  dis ql('
             || i_div
             || '.RENA.OP.QDIST01) curdepth'
             || cnst.newline_char
             || '  dis ql('
             || i_div
             || '.RENA.OP.QOCOL01) curdepth'
             || cnst.newline_char
             || 'EOF'
             || cnst.newline_char;
    l_msg := l_msg
             || cnst.newline_char
             || cnst.newline_char
             || cnst.newline_char
             || 'Orders Waiting to be Processed <MQ>'
             || cnst.newline_char
             || 'OrderType    Count'
             || cnst.newline_char
             || '------------ --------'
             || cnst.newline_char
             || TRIM(REPLACE(oscmd_fn(l_cmd, l_appl_srvr),
                             'Only McLane Authorized users are permitted to login to the McLane Network.'
                             || cnst.newline_char
                            )
                    );
    logs.dbg('Get Orders Being Processed');
    l_msg := l_msg
             || cnst.newline_char
             || cnst.newline_char
             || 'Orders Being Processed'
             || cnst.newline_char
             || 'OrderType    Status                Count'
             || cnst.newline_char
             || '------------ --------------------- --------'
             || cnst.newline_char;
    FOR l_r_ord IN l_cur_ords(l_div_part) LOOP
      l_msg := l_msg
               || RPAD(l_r_ord.typ, 12)
               || ' '
               || RPAD(l_r_ord.stat, 21)
               || ' '
               || LPAD(l_r_ord.cnt, 8)
               || cnst.newline_char;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_recpt_fn;

  /*
  ||----------------------------------------------------------------------------
  || LAST_ORD_CUR_FN
  ||  Build a cursor of order info for last order received.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/12/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION last_ord_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LAST_ORD_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT LPAD(a.ordnoa, 11) AS ord_num, TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts
         FROM ordp100a a
        WHERE (a.div_part, a.ordnoa) = (SELECT   d.div_part, MAX(o.ordnoa)
                                            FROM div_mstr_di1d d, ordp100a o
                                           WHERE d.div_id = i_div
                                             AND o.div_part = d.div_part
                                        GROUP BY d.div_part);

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END last_ord_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_WAITING_CUR_FN
  ||  Build a cursor of MQ order msgs waiting to be processed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/12/08 | rhalpai | Original - Created for PIR3593
  || 05/13/13 | rhalpai | Add logic to process with wrapper for ssh to
  ||                    | Application Server. PIR11038
  || 11/02/20 | rhalpai | Change oscmd_fn to ssh to new MQ server. SDHD-813035
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_waiting_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.ORD_WAITING_CUR_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          VARCHAR2(20);
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);

    OPEN l_cv
     FOR
       SELECT   c.mq_msg_id, c.mq_msg_id_desc,
                TRIM(REPLACE(oscmd_fn('ssh '
                                      || c.hostname
                                      || ' runmqsc -e '
                                      || c.mq_qmanager
                                      || ' << EOF | grep CURDEPTH |\'
                                      || cnst.newline_char
                                      || '  sed -e "s/.*CURDEPTH(//" |\'
                                      || cnst.newline_char
                                      || '  sed -e "s/)//"'
                                      || cnst.newline_char
                                      || 'DIS QL('
                                      || c.mq_queue
                                      || ') CURDEPTH'
                                      || cnst.newline_char
                                      || 'EOF'
                                      || cnst.newline_char,
                                      l_appl_srvr
                                     ),
                             'Only McLane Authorized users are permitted to login to the McLane Network.'
                             || cnst.newline_char
                            )
                    ) AS cnt
           FROM mclane_mq_gp_control c
          WHERE c.div_part = l_div_part
            AND c.gp_type = 'GET'
            AND c.mq_msg_id IN('QOCSR01', 'QOCOL01', 'QDIST01')
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_waiting_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_IN_PRCSS_CUR_FN
  ||  Build a cursor of orders in-process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/12/08 | rhalpai | Original - Created for PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_in_prcss_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.ORD_IN_PRCSS_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   c.mq_msg_id, c.mq_msg_id_desc,
                DECODE(m.msg_status,
                       'O', 'In-Process',
                       'F', 'Waiting to Re-attempt',
                       'E', 'Errored',
                       'C', 'Cancelled',
                       'Inactive'
                      ) AS stat,
                COUNT(m.msg_seq) AS cnt
           FROM div_mstr_di1d d, mclane_mq_gp_control c, mclane_order_receipt_msgs m
          WHERE d.div_id = i_div
            AND c.div_part = d.div_part
            AND c.gp_type = 'GET'
            AND c.mq_qmanager LIKE '%A'
            AND c.mq_msg_id IN('QOCSR01', 'QOCOL01', 'QDIST01')
            AND m.div_part(+) = c.div_part
            AND m.msg_type(+) = DECODE(c.mq_msg_id, 'QOCSR01', 'CSR', 'QOCOL01', 'REG', 'QDIST01', 'DIS')
       GROUP BY c.mq_msg_id, c.mq_msg_id_desc, m.msg_status
       ORDER BY c.mq_msg_id, c.mq_msg_id_desc, m.msg_status;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_in_prcss_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || LAST_RLSE_CUR_FN
  ||  Build a cursor of last SetRelease info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/15/07 | rhalpai | Original - Created for PIR3593
  || 09/14/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 02/08/11 | rhalpai | Change sort of cursor to be by descending
  ||                    | SEQ_OF_EVENTS. PIR0024
  ||----------------------------------------------------------------------------
  */
  FUNCTION last_rlse_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LAST_RLSE_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   r.user_id, TO_CHAR(r.rlse_ts, 'YYYY-MM-DD HH24:MI:SS') AS rlse_ts,
                DECODE(r.stat_cd, 'P', 'In-Process', 'R', 'Complete', 'E', 'Error', r.stat_cd) AS stat, r.ord_ln_cnt,
                TO_CHAR(r.llr_dt, 'YYYY-MM-DD') AS llr_dt,
                DECODE(r.test_bil_cd, '~', 'No', 'R', 'Reg', 'D', 'Dis', 'A', 'All') AS tst, r.forc_inv_sw AS frc,
                td.seq || ' ' || rl.typ_id, TO_CHAR(rl.create_ts, 'YYYY-MM-DD HH24:MI:SS') AS create_ts, td.descr
           FROM rlse_op1z r, rlse_log_op2z rl, rlse_typ_dmn_op9z td
          WHERE (r.div_part, r.rlse_ts) = (SELECT   d.div_part, MAX(r2.rlse_ts)
                                               FROM div_mstr_di1d d, rlse_op1z r2
                                              WHERE d.div_id = i_div
                                                AND r2.div_part = d.div_part
                                           GROUP BY d.div_part)
            AND rl.rlse_id = r.rlse_id
            AND rl.div_part = r.div_part
            AND td.typ_id = rl.typ_id
            AND (   td.seq > -1
                 OR td.typ_id IN('BEGANLZ', 'ENDANLZ'))
       ORDER BY rl.create_ts DESC, rl.seq_of_events DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  END last_rlse_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || TRANS_TYP_LIST_FN
  ||  Build a cursor of transaction types with descriptions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION trans_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT   e.trntye, e.trdsce
           FROM sawp652e e
          WHERE e.state = '1'
            AND e.trntye > '00'
       ORDER BY e.trntye;

    RETURN(l_cv);
  END trans_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || INV_TRANS_CUR_FN
  ||  Build a cursor of Inventory Transactions for date range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 07/28/08 | rhalpai | Changed to set v_to_dt using p_to_dt parm and removed
  ||                    | join between transaction history table (WHSP900R)
  ||                    | using FromSlot and warehouse inventory table
  ||                    | (WHSP300C) using ItemSlot. IM432974
  || 09/17/08 | rhalpai | Removed jurisdiction parm. Combined slot and
  ||                    | jurisdiction in slot_jrsdctn column of cursor. IM445462
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  || 11/02/11 | rhalpai | Change cursor to use outer join to handle Inventory
  ||                    | Adjustments which do not have associated Order Lines.
  ||                    | IM-033903
  ||----------------------------------------------------------------------------
  */
  FUNCTION inv_trans_cur_fn(
    i_div        IN  VARCHAR2,
    i_from_dt    IN  VARCHAR2,
    i_to_dt      IN  VARCHAR2,
    i_tran_typ   IN  VARCHAR2 DEFAULT NULL,
    i_catlg_num  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.INV_TRANS_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_from_dt            DATE;
    l_to_dt              DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FromDt', i_from_dt);
    logs.add_parm(lar_parm, 'ToDt', i_to_dt);
    logs.add_parm(lar_parm, 'TranTyp', i_tran_typ);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.dbg('ENTRY', lar_parm);
    l_from_dt := TO_DATE(i_from_dt, 'YYYY-MM-DD');
    l_to_dt := TO_DATE(i_to_dt, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   op2t.pgm_id, t.trdsce, t.trntye, op2o.ord_num, op2o.ord_ln, op2i.qty,
                LPAD(op2i.catlg_num, 6, '0') AS catlg_num,
                op2i.inv_aisle
                || op2i.inv_bin
                || op2i.inv_lvl
                || ' - '
                || DECODE(op2i.inv_zone, '~', NULL, op2i.inv_zone) AS slot_jrsdctn,
                TO_CHAR(op2t.create_ts, 'YYYY-MM-DD HH24:MI:SS')
           FROM div_mstr_di1d d, rlse_op1z r, tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i, sawp652e t
          WHERE d.div_id = i_div
            AND r.div_part = d.div_part
            AND op2t.div_part = r.div_part
            AND op2t.rlse_id = r.rlse_id
            AND TRUNC(op2t.create_ts) BETWEEN l_from_dt AND l_to_dt
            AND op2t.tran_typ BETWEEN 0 AND 99
            AND (   i_tran_typ IS NULL
                 OR op2t.tran_typ = i_tran_typ)
            AND (   i_catlg_num IS NULL
                 OR op2i.catlg_num = i_catlg_num)
            AND op2i.div_part = op2t.div_part
            AND op2i.tran_id = op2t.tran_id
            AND t.trntye = op2t.tran_typ
            AND op2o.div_part(+) = op2t.div_part
            AND op2o.tran_id(+) = op2t.tran_id
       ORDER BY op2t.create_ts, t.trdsce;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END inv_trans_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || AUDIT_CUR_FN
  ||  Build a cursor of audit information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION audit_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_min_dt  CONSTANT NUMBER        := TRUNC(SYSDATE) - DATE '1900-02-28' - 365;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT   TO_CHAR(TO_DATE('19000228' || LPAD(a.timea, 6, '0'), 'YYYYMMDDHH24MISS') + a.datea,
                        'YYYY-MM-DD HH24:MI:SS'
                       ) AS chg_ts,
                a.ordnoa AS ord_num, a.linea AS ord_ln, a.acnoa AS cust_num, a.fldnma AS changed, a.florga AS bef,
                a.flchga AS aft, a.usera AS user_id, x.rsncda AS code, x.desca AS reason
           FROM div_mstr_di1d d, sysp296a a, mclp140a x
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
            AND a.datea >= l_c_min_dt
            AND x.rsncda(+) = a.rsncda
       ORDER BY a.ordnoa DESC, a.linea, chg_ts;

    RETURN(l_cv);
  END audit_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || FORC_LOAD_CLOS_CUR_FN
  ||  Build a cursor of Force Load Close Log info for date range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/12/19 | rhalpai | Original - Created for PIR18852
  ||----------------------------------------------------------------------------
  */
  FUNCTION forc_load_clos_cur_fn(
    i_div      IN  VARCHAR2,
    i_from_dt  IN  VARCHAR2,
    i_to_dt    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.FORC_LOAD_CLOS_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_from_dt            DATE;
    l_to_dt              DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FromDt', i_from_dt);
    logs.add_parm(lar_parm, 'ToDt', i_to_dt);
    logs.dbg('ENTRY', lar_parm);
    l_from_dt := TO_DATE(i_from_dt, 'YYYY-MM-DD');
    l_to_dt := TO_DATE(i_to_dt, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   l.llr_dt, l.load_num, l.typ, l.cust_id, l.cust_nm, l.corp_cd, l.eta_dt, r.rsn_descr, r.user_id,
                r.create_ts
           FROM div_mstr_di1d d, load_log_op3z l, load_rsn_op3r r
          WHERE d.div_id = i_div
            AND l.div_part = d.div_part
            AND l.llr_dt BETWEEN l_from_dt AND l_to_dt
            AND r.div_part = l.div_part
            AND r.rsn_id = l.rsn_id
       ORDER BY r.create_ts, l.llr_dt, l.load_num, l.corp_cd, l.cust_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END forc_load_clos_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || EXCEPT_LOG_CUR_FN
  ||  Build a cursor of exception log info for cust, order, order/line.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION except_log_cur_fn(
    i_div       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.EXCEPT_LOG_CUR_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_mcl_cust IS NOT NULL THEN
      logs.dbg('Open Cust Cursor');

      OPEN l_cv
       FOR
         SELECT   cx.mccusb, d.ordnod, d.ordlnd, TO_CHAR(d.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts,
                  d.exlvld, d.reasnd, d.descd, d.itemd, d.uomd, d.qtyfrd, d.qtytod, d.exdesd, d.resexd, d.resusd,
                  TO_CHAR(TO_DATE('19000228' || LPAD(d.restmd, 6, '0'), 'YYYYMMDDHH24MISS') + d.resdtd,
                          'YYYY-MM-DD HH24:MI:SS'
                         ) AS rslv_date
             FROM mclp020b cx, ordp100a a, mclp300d d
            WHERE cx.div_part = l_div_part
              AND cx.mccusb = i_mcl_cust
              AND a.div_part = cx.div_part
              AND a.custa = cx.custb
              AND d.div_part = a.div_part
              AND d.ordnod = a.ordnoa
         UNION ALL
         SELECT   cx.mccusb, d.ordnod, d.ordlnd, TO_CHAR(d.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts,
                  d.exlvld, d.reasnd, d.descd, d.itemd, d.uomd, d.qtyfrd, d.qtytod, d.exdesd, d.resexd, d.resusd,
                  TO_CHAR(TO_DATE('19000228' || LPAD(d.restmd, 6, '0'), 'YYYYMMDDHH24MISS') + d.resdtd,
                          'YYYY-MM-DD HH24:MI:SS'
                         ) AS rslv_date
             FROM mclp020b cx, ordp900a a, mclp900d d
            WHERE cx.div_part = l_div_part
              AND cx.mccusb = i_mcl_cust
              AND a.div_part = cx.div_part
              AND a.custa = cx.custb
              AND d.div_part = a.div_part
              AND d.ordnod = a.ordnoa
         ORDER BY ordnod, ordlnd, last_chg_ts DESC;
    ELSIF i_ord_num IS NOT NULL THEN
      logs.dbg('Open Order Cursor');

      OPEN l_cv
       FOR
         SELECT   cx.mccusb, d.ordnod, d.ordlnd, TO_CHAR(d.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts,
                  d.exlvld, d.reasnd, d.descd, d.itemd, d.uomd, d.qtyfrd, d.qtytod, d.exdesd, d.resexd, d.resusd,
                  TO_CHAR(TO_DATE('19000228' || LPAD(d.restmd, 6, '0'), 'YYYYMMDDHH24MISS') + d.resdtd,
                          'YYYY-MM-DD HH24:MI:SS'
                         ) AS rslv_date
             FROM mclp300d d, ordp100a a, mclp020b cx
            WHERE d.div_part = l_div_part
              AND d.ordnod = i_ord_num
              AND d.ordlnd = NVL(i_ord_ln, d.ordlnd)
              AND a.div_part = d.div_part
              AND a.ordnoa = d.ordnod
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
         UNION ALL
         SELECT   cx.mccusb, d.ordnod, d.ordlnd, TO_CHAR(d.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts,
                  d.exlvld, d.reasnd, d.descd, d.itemd, d.uomd, d.qtyfrd, d.qtytod, d.exdesd, d.resexd, d.resusd,
                  TO_CHAR(TO_DATE('19000228' || LPAD(d.restmd, 6, '0'), 'YYYYMMDDHH24MISS') + d.resdtd,
                          'YYYY-MM-DD HH24:MI:SS'
                         ) AS rslv_date
             FROM mclp900d d, ordp900a a, mclp020b cx
            WHERE d.div_part = l_div_part
              AND d.ordnod = i_ord_num
              AND d.ordlnd = NVL(i_ord_ln, d.ordlnd)
              AND a.div_part = d.div_part
              AND a.ordnoa = d.ordnod
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
         ORDER BY ordlnd, last_chg_ts DESC;
    END IF;   -- i_mcl_cust IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END except_log_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || LEVEL_1_EXCEPT_CUR_FN
  ||  Build a cursor of level-1 exceptions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION level_1_except_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LEVEL_1_EXCEPT_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   cx.mccusb, a.ordnoa, a.connba, md.descd
           FROM div_mstr_di1d d, ordp100a a, mclp300d md, mclp020b cx
          WHERE d.div_id = i_div
            AND md.div_part = d.div_part
            AND md.exlvld = 1
            AND md.resexd = '0'
            AND md.exdesd IS NULL
            AND a.div_part = md.div_part
            AND a.ordnoa = md.ordnod
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
       GROUP BY cx.mccusb, a.ordnoa, a.connba, md.descd
       ORDER BY 1, 2;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err;
  END level_1_except_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || LINEOUT_CUR_FN
  ||  Build a cursor of line-out info for date range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 06/16/08 | rhalpai | Added sort by LLRDt/CategDescr/Slot/Item to cursor.
  || 07/28/08 | rhalpai | Changed to use exception description instead of the
  ||                    | exception code when available. IM432974
  || 07/10/12 | rhalpai | Remove unused column, TICKTB. PIR11038
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/22/15 | rhalpai | Remove div_part from MCLP230A. PIR15202
  ||----------------------------------------------------------------------------
  */
  FUNCTION lineout_cur_fn(
    i_div      IN  VARCHAR2,
    i_from_dt  IN  VARCHAR2,
    i_to_dt    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LINEOUT_CUR_FN';
    lar_parm             logs.tar_parm;
    l_from_dt            DATE;
    l_to_dt              DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FromDt', i_from_dt);
    logs.add_parm(lar_parm, 'ToDt', i_to_dt);
    logs.dbg('ENTRY', lar_parm);
    l_from_dt := TO_DATE(i_from_dt, 'YYYY-MM-DD');
    l_to_dt := TO_DATE(i_to_dt, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr, w.zonec, scb.desca, w.aislc || w.binc || w.levlc AS slot,
                b.orditb, e.ctdsce, w.qohc, TO_CHAR(SUM(NVL(b.alcqtb, 0) - NVL(b.pckqtb, 0)), 'FM9990') AS lineouts,
                NVL(ma.desca, b.ntshpb) AS not_shp_rsn
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, ordp120b b, sawp505e e, mclp230a scb,
                whsp300c w, mclp110b di, mclp140a ma
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt BETWEEN l_from_dt AND l_to_dt
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb = 'A'
            AND b.alcqtb > 0
            AND b.excptn_sw = 'N'
            AND b.subrcb < 999
            AND b.alcqtb > b.pckqtb
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND scb.sbcata = e.scbcte
            AND di.div_part = b.div_part
            AND di.itemb = b.itemnb
            AND di.uomb = b.sllumb
            AND w.div_part = b.div_part
            AND w.itemc = b.itemnb
            AND w.uomc IN(b.sllumb, di.suomb)
            AND w.uomc NOT IN('CII', 'CIR', 'CIC')
            AND ma.rsncda(+) = b.ntshpb
       GROUP BY ld.llr_dt, w.zonec, scb.desca, w.aislc, w.binc, w.levlc, b.orditb, e.ctdsce, w.qohc,
                NVL(ma.desca, b.ntshpb)
       UNION ALL
       SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr, w.zonec, scb.desca, w.aislc || w.binc || w.levlc AS slot,
                b.orditb, e.ctdsce, w.qohc, TO_CHAR(SUM(NVL(b.alcqtb, 0) - NVL(b.pckqtb, 0)), '9990') AS lineouts,
                NVL(ma.desca, b.ntshpb) AS not_shp_rsn
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, mclp030c ct, ordp120b b, sawp505e e,
                mclp230a scb, mclp110b di, whsp300c w, mclp140a ma
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt BETWEEN l_from_dt AND l_to_dt
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND ct.div_part = a.div_part
            AND ct.custc = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb = 'A'
            AND b.alcqtb > 0
            AND b.excptn_sw = 'N'
            AND b.subrcb < 999
            AND b.alcqtb > b.pckqtb
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND scb.sbcata = e.scbcte
            AND di.div_part = b.div_part
            AND di.itemb = b.itemnb
            AND di.uomb = b.sllumb
            AND w.div_part = b.div_part
            AND w.itemc = b.itemnb
            AND w.uomc IN(b.sllumb, di.suomb)
            AND w.uomc IN('CII', 'CIR', 'CIC')
            AND w.taxjrc = ct.taxjrc
            AND ma.rsncda(+) = b.ntshpb
       GROUP BY ld.llr_dt, w.zonec, scb.desca, w.aislc, w.binc, w.levlc, b.orditb, e.ctdsce, w.qohc,
                NVL(ma.desca, b.ntshpb)
       UNION ALL
       SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr, 'XXXX' AS ZONE, scb.desca, 'XXXXXXX' AS slot, b.orditb,
                e.ctdsce, 0, TO_CHAR(SUM(NVL(b.alcqtb, 0) - NVL(b.pckqtb, 0)), '9990') AS lineouts,
                NVL(ma.desca, b.ntshpb) AS not_shp_rsn
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, ordp120b b, sawp505e e, mclp230a scb,
                mclp110b di, mclp140a ma
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt BETWEEN l_from_dt AND l_to_dt
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb = 'A'
            AND b.alcqtb > 0
            AND b.excptn_sw = 'N'
            AND b.subrcb < 999
            AND b.alcqtb > b.pckqtb
            AND NOT EXISTS(SELECT 1
                             FROM whsp300c w
                            WHERE w.div_part = b.div_part
                              AND w.itemc = b.itemnb)
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND scb.sbcata = e.scbcte
            AND di.div_part = b.div_part
            AND di.itemb = b.itemnb
            AND di.uomb = b.sllumb
            AND ma.rsncda(+) = b.ntshpb
       GROUP BY ld.llr_dt, scb.desca, b.orditb, e.ctdsce, NVL(ma.desca, b.ntshpb)
       ORDER BY 1, 3, 4, 5;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END lineout_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_INQ_CUR_FN
  ||  Build a cursor of Load Inquiry info for a load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/23/11 | rhalpai | Original - Created for PIR9030
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_inq_cur_fn(
    i_div       IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LOAD_INQ_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   ld.loadc AS load_num, md.stopd AS stop_num, DECODE(ld.lbsgpc, 'Y', 'GMP', 'GRO') AS load_typ,
                md.prod_typ, md.dayrcd AS eta_dy, LPAD(md.etad, 4, '0') AS eta_tm, md.wkoffd AS wk, c.acnoc AS cust_id,
                c.namec AS cust_nm, c.shad1c AS addr, c.shpctc AS city, c.shpstc AS st, c.dist_frst_day
           FROM div_mstr_di1d d, mclp120c ld, mclp040d md, sysp200c c
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.loadc = i_load_num
            AND md.div_part = ld.div_part
            AND md.loadd = ld.loadc
            AND c.div_part = md.div_part
            AND c.acnoc = md.custd
       ORDER BY md.stopd;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_inq_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_SUM_CUR_FN
  ||  Build a cursor of Load Summary (BO580) info for a load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 11/28/11 | rhalpai | Add new Test status 4 to cursor. PIR10211
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_sum_cur_fn(
    i_div       IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LOAD_SUM_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_max_eta_dt         DATE;
    l_max_shp_dt         PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    l_max_eta_dt := TRUNC(SYSDATE + 30);
    l_max_shp_dt := TRUNC(SYSDATE + 7) - DATE '1900-02-28';

    OPEN l_cv
     FOR
       WITH zz AS
            (SELECT tmp.grp_num, tmp.cust_id, tmp.load_typ, tmp.ord_num, tmp.cust_nm, tmp.load_num,
                    (CASE
                       WHEN tmp.stop_num > 99 THEN 0
                       ELSE tmp.stop_num
                     END) AS stop_num, tmp.ord_stat, tmp.trans_dt, tmp.eta_dt, tmp.ord_ln_cnt, tmp.ord_src, tmp.conf_num,
                    tmp.fax_num, tmp.phone_num, tmp.ord_typ, tmp.no_ord_sw, NVL(bkt.bucket1, 0) AS bucket1,
                    NVL(bkt.bucket2, 0) AS bucket2, NVL(bkt.bucket3, 0) AS bucket3, NVL(bkt.bucket4, 0) AS bucket4,
                    NVL(bkt.bucket5, 0) AS bucket5, NVL(bkt.bucket6, 0) AS bucket6, ' ' AS orig_ord_stat
               FROM (SELECT x.grp_num, x.cust_id, NVL(x.load_typ, 'GRO') AS load_typ, x.ord_num, x.cust_nm, x.load_num,
                            NVL(x.stop_num, 0) AS stop_num, x.ord_stat, 0 AS trans_dt,
                            TO_CHAR((CASE
                                       WHEN x.ord_typ = 'D'
                                       AND (   x.load_num = 'DIST'
                                            OR x.load_num BETWEEN 'P00P' AND 'P99P') THEN DATE '1900-02-28' + x.shp_dt
                                       ELSE x.eta_ts
                                     END
                                    ),
                                    'YYYY-MM-DD'
                                   ) AS eta_dt,
                            x.ord_ln_cnt, x.ord_src,
                            NVL(DECODE(x.ord_src, 'RI', x.legcy_ref, 'NRI', x.legcy_ref, 'PB', x.legcy_ref, x.conf_num),
                                ' '
                               ) AS conf_num,
                            NVL((CASE
                                   WHEN x.ord_typ = 'D'
                                   AND (   x.load_num = 'DIST'
                                        OR x.load_num BETWEEN 'P00P' AND 'P99P') THEN NULL
                                   ELSE x.fax_num
                                 END
                                ),
                                ' '
                               ) AS fax_num,
                            NVL((CASE
                                   WHEN x.ord_typ = 'D'
                                   AND (   x.load_num = 'DIST'
                                        OR x.load_num BETWEEN 'P00P' AND 'P99P') THEN NULL
                                   ELSE x.phone_num
                                 END
                                ),
                                ' '
                               ) AS phone_num,
                            x.ord_typ, x.no_ord_sw
                       FROM (SELECT TO_NUMBER(SUBSTR(c.retgpc, 3, 3), '999') AS grp_num, c.acnoc AS cust_id,
                                    a.ldtypa AS load_typ, a.ordnoa AS ord_num, c.namec AS cust_nm, ld.load_num, se.stop_num,
                                    a.shpja AS shp_dt, se.eta_ts,
                                    (SELECT COUNT(*)
                                       FROM ordp120b b
                                      WHERE b.div_part = a.div_part
                                        AND b.ordnob = a.ordnoa
                                        AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt,
                                    a.ipdtsa AS ord_src, a.legrfa AS legcy_ref, a.connba AS conf_num, c.cnfaxc AS fax_num,
                                    c.cnphnc AS phone_num, a.dsorda AS ord_typ, DECODE(a.dsorda, 'N', '1', '0') AS no_ord_sw,
                                    (CASE
                                       WHEN a.stata = 'S' THEN 'SUSP'
                                       WHEN a.stata = 'C' THEN 'CANC'
                                       WHEN a.stata = 'O' THEN 'OPEN'
                                       WHEN a.stata IN('P', 'R', 'A') THEN 'BILL'
                                       ELSE 'OTHR'
                                     END
                                    ) AS ord_stat
                               FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, sysp200c c
                              WHERE d.div_id = i_div
                                AND ld.div_part = d.div_part
                                AND ld.load_num = i_load_num
                                AND a.div_part = ld.div_part
                                AND a.load_depart_sid = ld.load_depart_sid
                                AND a.stata <> 'C'
                                AND a.dsorda <> 'N'
                                AND se.div_part = ld.div_part
                                AND se.load_depart_sid = ld.load_depart_sid
                                AND se.cust_id = a.custa
                                AND (   (    TRUNC(se.eta_ts) < l_max_eta_dt
                                         AND ld.load_num <> 'DIST'
                                         AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
                                        )
                                     OR (    a.dsorda = 'D'
                                         AND (   ld.load_num = 'DIST'
                                              OR ld.load_num BETWEEN 'P00P' AND 'P99P')
                                         AND a.shpja < l_max_shp_dt
                                        )
                                    )
                                AND c.div_part = se.div_part
                                AND c.acnoc = se.cust_id
                                AND c.statc IN('1', '3', '4')
                             UNION ALL
                             SELECT TO_NUMBER(SUBSTR(c.retgpc, 3, 3), '999') AS grp_num, c.acnoc AS cust_id,
                                    a.ldtypa AS load_typ, a.ordnoa AS ord_num, c.namec AS cust_nm, ld.load_num, se.stop_num,
                                    a.shpja AS shp_dt, se.eta_ts,
                                    (SELECT COUNT(*)
                                       FROM ordp120b b
                                      WHERE b.div_part = a.div_part
                                        AND b.ordnob = a.ordnoa
                                        AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt,
                                    a.ipdtsa AS ord_src, a.legrfa AS legcy_ref, a.connba AS conf_num, c.cnfaxc AS fax_num,
                                    c.cnphnc AS phone_num, a.dsorda AS ord_typ, DECODE(a.dsorda, 'N', '1', '0') AS no_ord_sw,
                                    'OPEN' AS ord_stat
                               FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, sysp200c c
                              WHERE d.div_id = i_div
                                AND ld.div_part = d.div_part
                                AND ld.load_num = NVL(i_load_num, ld.load_num)
                                AND a.div_part = ld.div_part
                                AND a.load_depart_sid = ld.load_depart_sid
                                AND a.dsorda <> 'N'
                                AND a.stata IN('P', 'R')
                                AND EXISTS(SELECT 1
                                             FROM ordp120b b
                                            WHERE b.div_part = a.div_part
                                              AND b.ordnob = a.ordnoa
                                              AND b.statb = 'O')
                                AND EXISTS(SELECT 1
                                             FROM ordp120b b
                                            WHERE b.div_part = a.div_part
                                              AND b.ordnob = a.ordnoa
                                              AND b.statb NOT IN('O', 'I', 'S', 'C'))
                                AND se.div_part = ld.div_part
                                AND se.load_depart_sid = ld.load_depart_sid
                                AND se.cust_id = a.custa
                                AND (   (    TRUNC(se.eta_ts) < l_max_eta_dt
                                         AND ld.load_num <> 'DIST'
                                         AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
                                        )
                                     OR (    a.dsorda = 'D'
                                         AND (   ld.load_num = 'DIST'
                                              OR ld.load_num BETWEEN 'P00P' AND 'P99P')
                                         AND a.shpja < l_max_shp_dt
                                        )
                                    )
                                AND c.div_part = se.div_part
                                AND c.acnoc = se.cust_id
                                AND c.statc IN('1', '3', '4')) x
                     UNION ALL
                     -- "No Order" Orders from Customers...
                     SELECT TO_NUMBER(SUBSTR(c.retgpc, 3, 3), '999') AS grp_num, c.acnoc AS cust_id,
                            NVL(a.ldtypa, 'GRO') AS load_typ, a.ordnoa AS ord_num, c.namec AS cust_nm, ld.load_num,
                            se.stop_num, '    ' AS ord_stat, 9 AS trans_dt, '1900-01-01' AS eta_dt,
                            (SELECT COUNT(*)
                               FROM ordp120b b
                              WHERE b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.ipdtsa AS ord_src,
                            NVL(DECODE(a.ipdtsa, 'RI', a.legrfa, 'NRI', a.legrfa, 'PB', a.legrfa, a.connba), ' ') AS conf_num,
                            NVL(c.cnfaxc, ' ') AS fax_num, NVL(c.cnphnc, ' ') AS phone_num, a.dsorda AS ord_typ,
                            '1' AS no_ord_sw
                       FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, sysp200c c
                      WHERE d.div_id = i_div
                        AND ld.div_part = d.div_part
                        AND ld.load_num = i_load_num
                        AND a.div_part = ld.div_part
                        AND a.load_depart_sid = ld.load_depart_sid
                        AND a.dsorda = 'N'
                        AND a.excptn_sw = 'N'
                        AND se.div_part = ld.div_part
                        AND se.load_depart_sid = ld.load_depart_sid
                        AND se.cust_id = a.custa
                        AND c.div_part = se.div_part
                        AND c.acnoc = se.cust_id
                        AND c.statc IN('1', '3', '4')) tmp,
                    (SELECT   x.ord_num, x.ord_stat, SUM(x.bucket1) AS bucket1, SUM(x.bucket2) AS bucket2,
                              SUM(x.bucket3) AS bucket3, SUM(x.bucket4) AS bucket4, SUM(x.bucket5) AS bucket5,
                              SUM(x.bucket6) AS bucket6
                         FROM (SELECT   b.ordnob AS ord_num,
                                        DECODE(b.statb,
                                               'O', 'OPEN',
                                               'R', 'BILL',
                                               'A', 'BILL',
                                               'P', 'BILL',
                                               'T', 'BILL',
                                               'S', 'SUSP',
                                               'C', 'CANC',
                                               'OTHR'
                                              ) AS ord_stat,
                                        DECODE(v.seqv, 1, COUNT(*), 0) AS bucket1, DECODE(v.seqv, 2, COUNT(*), 0) AS bucket2,
                                        DECODE(v.seqv, 3, COUNT(*), 0) AS bucket3, DECODE(v.seqv, 4, COUNT(*), 0) AS bucket4,
                                        (CASE
                                           WHEN v.seqv IN(5, 6)
                                           AND e.uome LIKE 'GM%' THEN COUNT(*)
                                           ELSE 0
                                         END) AS bucket5,
                                        (CASE
                                           WHEN(   (    v.seqv IN(5, 6)
                                                    AND e.uome NOT LIKE 'GM%')
                                                OR v.seqv NOT IN(1, 2, 3, 4, 5, 6)) THEN COUNT(*)
                                           ELSE 0
                                         END
                                        ) AS bucket6
                                   FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b,
                                        sawp505e e, mclp220d n, invp250v v
                                  WHERE d.div_id = i_div
                                    AND ld.div_part = d.div_part
                                    AND ld.load_num = i_load_num
                                    AND a.div_part = ld.div_part
                                    AND a.load_depart_sid = ld.load_depart_sid
                                    AND se.div_part = ld.div_part
                                    AND se.load_depart_sid = ld.load_depart_sid
                                    AND se.cust_id = a.custa
                                    AND (   (    TRUNC(se.eta_ts) < l_max_eta_dt
                                             AND ld.load_num <> 'DIST'
                                             AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
                                            )
                                         OR (    a.dsorda = 'D'
                                             AND (   ld.load_num = 'DIST'
                                                  OR ld.load_num BETWEEN 'P00P' AND 'P99P')
                                             AND a.shpja < l_max_shp_dt
                                            )
                                        )
                                    AND b.div_part = a.div_part
                                    AND b.ordnob = a.ordnoa
                                    AND b.statb <> 'C'
                                    AND b.subrcb < 999
                                    AND e.iteme = b.itemnb
                                    AND e.uome = b.sllumb
                                    AND n.nacsd = e.nacse
                                    AND v.itemv = n.nacshd
                               GROUP BY b.ordnob, b.statb, e.uome, v.seqv) x
                     GROUP BY x.ord_num, x.ord_stat) bkt
              WHERE bkt.ord_num(+) = tmp.ord_num
                AND bkt.ord_stat(+) = tmp.ord_stat),
            eoe_sum AS
            (SELECT zz.grp_num, zz.cust_id, zz.load_typ, zz.ord_num, zz.cust_nm, zz.load_num, zz.stop_num, zz.ord_stat,
                    zz.trans_dt, zz.eta_dt, zz.ord_ln_cnt, zz.ord_src, zz.conf_num, zz.fax_num, zz.phone_num, zz.ord_typ,
                    zz.no_ord_sw, zz.bucket1, zz.bucket2, zz.bucket3, zz.bucket4, zz.bucket5, zz.bucket6, zz.orig_ord_stat
               FROM zz
             UNION ALL
             -- Missing or incomplete orders (AKA "Below the line")
             SELECT   TO_NUMBER(SUBSTR(c.retgpc, 3, 3), '999') AS grp_num, c.acnoc, ' ', 0, c.namec AS cust_nm, md.loadd,
                      md.stopd, ' ', 0, '1900-01-01', 0, ' ', ' ', NVL(c.cnfaxc, ' ') AS fax_num,
                      NVL(c.cnphnc, ' ') AS phone_num, ' ', '2', 0, 0, 0, 0, 0, 0, ' '
                 FROM div_mstr_di1d d, mclp040d md, sysp200c c
                WHERE d.div_id = i_div
                  AND md.div_part = d.div_part
                  AND md.loadd = i_load_num
                  AND c.div_part = md.div_part
                  AND c.acnoc = md.custd
                  AND c.statc IN('1', '3', '4')
                  AND (
                          -- handle no order submitted
                          NOT EXISTS(SELECT 1
                                       FROM zz
                                      WHERE zz.load_num = md.loadd
                                        AND zz.cust_id = md.custd
                                        AND zz.ord_typ = 'R'
                                        AND (   zz.no_ord_sw = '1'
                                             OR zz.bucket1 > 0
                                             OR zz.bucket2 > 0
                                             OR zz.bucket3 > 0
                                             OR zz.bucket4 > 0
                                             OR zz.bucket5 > 0
                                             OR zz.bucket6 > 0
                                            ))
                       OR
                          -- handle order submitted but NO CIG items and more GMP than GRO items
                          (    EXISTS(SELECT 1
                                        FROM zz
                                       WHERE zz.load_num = md.loadd
                                         AND zz.cust_id = md.custd
                                         AND zz.ord_typ = 'R'
                                         AND zz.no_ord_sw = '0'
                                         AND zz.bucket2 = 0
                                         AND zz.bucket5 > zz.bucket6)
                           AND NOT EXISTS(SELECT 1
                                            FROM zz
                                           WHERE zz.load_num = md.loadd
                                             AND zz.cust_id = md.custd
                                             AND zz.ord_typ = 'R'
                                             AND (   zz.no_ord_sw = '1'
                                                  OR zz.bucket6 > zz.bucket5))
                          )
                      )
             GROUP BY c.retgpc, c.acnoc, c.namec, md.loadd, md.stopd, c.cnfaxc, c.cnphnc)
       SELECT   a.load_num, a.load_typ, a.no_ord_sw, a.cust_id, a.stop_num, a.eta_dt, a.ord_stat, a.cust_nm, a.fax_num,
                a.phone_num, SUM(a.bucket1) AS fountain, SUM(a.bucket2) AS cigs, SUM(a.bucket3) AS candy,
                SUM(a.bucket4) AS supplies, SUM(a.bucket5) AS gmp, SUM(a.bucket6) AS gro_othr,
                SUM(a.bucket1 + a.bucket2 + a.bucket3 + a.bucket4 + a.bucket5 + a.bucket6) AS ttl
           FROM eoe_sum a
          WHERE EXISTS(SELECT 1
                         FROM eoe_sum x
                        WHERE x.load_num = a.load_num
                          AND x.no_ord_sw = '0')
       GROUP BY a.load_num, a.no_ord_sw, a.cust_id, a.stop_num, a.cust_nm, a.fax_num, a.phone_num, a.eta_dt, a.load_typ,
                a.ord_stat
       ORDER BY a.load_num, a.no_ord_sw, a.cust_id, a.eta_dt, a.load_typ, a.ord_stat;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_sum_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || MAX_QTY_APPLIED_CUR_FN
  ||  Build a cursor of order info where max-qty has been applied.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION max_qty_applied_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.MAX_QTY_APPLIED_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   ld.load_num, se.stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm, b.ordnob AS ord_num,
                b.lineb AS ord_ln, b.orditb AS catlg_num, b.orgqtb AS orig_qty, b.maxqtb AS max_qty,
                DECODE(b.bymaxb, 'Y', 'Y', '1', 'Y', 'N') AS byp_max_sw, e.shppke AS pack, e.sizee AS sz,
                e.ctdsce AS item_descr
           FROM div_mstr_di1d d, mclp300d md, ordp120b b, ordp100a a, load_depart_op1f ld, stop_eta_op1g se,
                mclp020b cx, sysp200c c, sawp505e e
          WHERE d.div_id = i_div
            AND md.div_part = d.div_part
            AND md.reasnd = '002'
            AND b.div_part = md.div_part
            AND b.ordnob = md.ordnod
            AND b.lineb = md.ordlnd
            AND b.statb = 'O'
            AND a.div_part = b.div_part
            AND a.ordnoa = b.ordnob
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
       GROUP BY ld.load_num, se.stop_num, b.ordnob, b.lineb, cx.mccusb, c.namec, b.orditb, b.orgqtb, b.maxqtb, b.bymaxb,
                e.shppke, e.sizee, e.ctdsce
       ORDER BY 1, 2, 5, 6;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END max_qty_applied_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || NO_ORDER_CUR_FN
  ||  Build a cursor of No-Order Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION no_order_cur_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.NO_ORDER_CUR_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_llr_num            NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_llr_num := l_llr_dt - DATE '1900-02-28';

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_ts, 'YYYY-MM-DD') AS llr_dt, ld.load_num, se.stop_num, cx.mccusb AS mcl_cust,
                se.cust_id, c.namec AS cust_nm, c.cnnamc AS cntct_nm, c.cnphnc AS cntct_phone
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, sysp200c c, mclp020b cx
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = l_llr_dt
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.dsorda = 'N'
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
       UNION
       SELECT   TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt, a.orrtea AS load_num,
                a.stopsa AS stop_num, cx.mccusb AS mcl_cust, c.acnoc AS cust_id, c.namec AS cust_nm,
                c.cnnamc AS cntct_nm, c.cnphnc AS cntct_phone
           FROM div_mstr_di1d d, ordp900a a, sysp200c c, mclp020b cx
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
            AND a.ctofda = l_llr_num
            AND a.dsorda = 'N'
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
       ORDER BY 1, 2, 3;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END no_order_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORDER_QTY_CUR_FN
  ||  Build a cursor of order details with order quantities greater thant or
  ||  equal to requested order quantity.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION order_qty_cur_fn(
    i_div      IN  VARCHAR2,
    i_ord_qty  IN  PLS_INTEGER
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.ORDER_QTY_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   ld.load_num, c.retgpc, cx.mccusb, b.orditb, b.ordnob, e.shppke, e.sizee, e.ctdsce, e.scbcte, e.uome,
                b.ordqtb
           FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, mclp020b cx, sysp200c c, ordp120b b, sawp505e e
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
            AND a.dsorda = 'R'
            AND a.excptn_sw = 'N'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb = 'O'
            AND b.excptn_sw = 'N'
            AND b.subrcb < 999
            AND b.ordqtb >= i_ord_qty
            AND e.catite = b.orditb
       ORDER BY ld.load_num, c.retgpc, cx.mccusb, b.orditb;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END order_qty_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || PALLET_CUR_FN
  ||  Build a cursor of pallet order info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION pallet_cur_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.PALLET_CUR_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    OPEN l_cv
     FOR
       SELECT   b.orditb, b.ordqtb, cx.mccusb
           FROM sysp200c c, mclp020b cx, ordp100a a, load_depart_op1f ld, ordp120b b
          WHERE c.div_part = l_div_part
            AND c.shporc = 'Y'
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
            AND a.div_part = c.div_part
            AND a.custa = c.acnoc
            AND a.excptn_sw = 'N'
            AND a.dsorda <> 'N'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb = 'O'
            AND b.subrcb = 0
            AND b.excptn_sw = 'N'
       ORDER BY b.orditb, cx.mccusb;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END pallet_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || SPECL_DIST_SHIP_DATES_FN
  ||  Build a cursor of ship dates for special distributions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 06/16/08 | rhalpai | Added sort by ShipDt to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  ||----------------------------------------------------------------------------
  */
  FUNCTION specl_dist_ship_dates_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_dt
           FROM div_mstr_di1d d, ordp100a a
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
            AND a.stata = 'O'
            AND a.excptn_sw = 'N'
            AND a.dsorda = 'D'
            AND a.ldtypa LIKE 'P%'
       GROUP BY a.shpja
       ORDER BY a.shpja;

    RETURN(l_cv);
  END specl_dist_ship_dates_fn;

  /*
  ||----------------------------------------------------------------------------
  || UNBILLED_SPECL_DIST_CUR_FN
  ||  Build a cursor of unbilled special distribution info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION unbilled_specl_dist_cur_fn(
    i_div          IN  VARCHAR2,
    i_max_ship_dt  IN  VARCHAR2,
    i_sort         IN  VARCHAR2 DEFAULT 'SHIPDATE'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.UNBILLED_SPECL_DIST_CUR_FN';
    lar_parm             logs.tar_parm;
    l_max_ship_dt        PLS_INTEGER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MaxShipDt', i_max_ship_dt);
    logs.add_parm(lar_parm, 'Sort', i_sort);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_max_ship_dt := TO_DATE(i_max_ship_dt, 'YYYY-MM-DD') - DATE '1900-02-28';

    IF NVL(UPPER(i_sort), 'SHIPDATE') = 'SHIPDATE' THEN
      logs.dbg('Open Cursor Sorted by ShipDate');

      OPEN l_cv
       FOR
         SELECT   TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_dt, ld.load_num, cx.mccusb, c.namec,
                  a.ldtypa, COUNT(1) AS cnt
             FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, sysp200c c, mclp020b cx
            WHERE d.div_id = i_div
              AND a.div_part = d.div_part
              AND a.dsorda = 'D'
              AND a.excptn_sw = 'N'
              AND a.stata = 'O'
              AND a.ldtypa BETWEEN 'P00' AND 'P99'
              AND a.shpja <= l_max_ship_dt
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND c.div_part = a.div_part
              AND c.acnoc = a.custa
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
         GROUP BY a.shpja, ld.load_num, cx.mccusb, c.namec, a.ldtypa
         ORDER BY a.shpja, ld.load_num, c.namec, a.ldtypa;
    ELSE
      logs.dbg('Open Cursor Sorted by Customer');

      OPEN l_cv
       FOR
         SELECT   TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_dt, ld.load_num, cx.mccusb, c.namec,
                  a.ldtypa, COUNT(1) AS cnt
             FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, sysp200c c, mclp020b cx
            WHERE d.div_id = i_div
              AND a.div_part = d.div_part
              AND a.dsorda = 'D'
              AND a.excptn_sw = 'N'
              AND a.stata = 'O'
              AND a.ldtypa BETWEEN 'P00' AND 'P99'
              AND a.shpja <= l_max_ship_dt
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND c.div_part = a.div_part
              AND c.acnoc = a.custa
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
         GROUP BY a.shpja, ld.load_num, cx.mccusb, c.namec, a.ldtypa
         ORDER BY cx.mccusb, a.shpja, ld.load_num, a.ldtypa;
    END IF;   -- NVL(UPPER(i_sort), 'SHIPDATE') = 'SHIPDATE'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END unbilled_specl_dist_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || TOBACCO_CUR_FN
  ||  Build a cursor of order quantities for tobacco items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 06/16/08 | rhalpai | Added sort by LLRDt/Load/Cust to cursor.
  || 05/09/09 | VXRANGA | IM502002 - Added items '282947', '618165' to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION tobacco_cur_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.TOBACCO_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr, ld.load_num, cx.mccusb, c.namec,
                SUM(DECODE(b.orditb, '175190', b.ordqtb, 0)) AS "175190",
                SUM(DECODE(b.orditb, '066233', b.ordqtb, 0)) AS "066233",
                SUM(DECODE(b.orditb, '066225', b.ordqtb, 0)) AS "066225",
                SUM(DECODE(b.orditb, '516690', b.ordqtb, 0)) AS "516690",
                SUM(DECODE(b.orditb, '554139', b.ordqtb, 0)) AS "554139",
                SUM(DECODE(b.orditb, '554162', b.ordqtb, 0)) AS "554162",
                SUM(DECODE(b.orditb, '554170', b.ordqtb, 0)) AS "554170",
                SUM(DECODE(b.orditb, '516716', b.ordqtb, 0)) AS "516716",
                SUM(DECODE(b.orditb, '282947', b.ordqtb, 0)) AS "282947",
                SUM(DECODE(b.orditb, '618165', b.ordqtb, 0)) AS "618165"
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, ordp120b b, mclp020b cx, sysp200c c, sawp505e e
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = l_llr_dt
            AND ld.load_num = NVL(i_load_num, ld.load_num)
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.excptn_sw = 'N'
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.excptn_sw = 'N'
            AND b.statb IN('R', 'A')
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND e.catite IN('175190',
                            '066233',
                            '066225',
                            '516690',
                            '554139',
                            '554162',
                            '554170',
                            '516716',
                            '282947',
                            '618165'
                           )
       GROUP BY ld.llr_dt, ld.load_num, cx.mccusb, c.namec
       ORDER BY 1, 2, 3;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tobacco_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ALLOCATED_ITEMS_CUR_FN
  ||  Build a cursor of allocated order quantities for selected items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 06/16/08 | rhalpai | Added sort by LLRDt/Load/Cust/Item to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION allocated_items_cur_fn(
    i_div             IN  VARCHAR2,
    i_catlg_num_list  IN  VARCHAR2,
    i_llr_dt          IN  VARCHAR2,
    i_load_num        IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.ALLOCATED_ITEMS_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_llr_dt             DATE;
    l_t_catlg_nums       type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CatlgNumList', i_catlg_num_list);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Parse');
    l_t_catlg_nums := str.parse_list(i_catlg_num_list, ',');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(l_llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num, cx.mccusb, c.namec, e.catite, e.ctdsce,
                e.shppke, e.sizee, SUM(b.ordqtb) AS ord_qty
           FROM TABLE(CAST(l_t_catlg_nums AS type_stab)) t, sawp505e e, div_mstr_di1d d, load_depart_op1f ld,
                ordp100a a, ordp120b b, mclp020b cx, sysp200c c
          WHERE e.catite = t.column_value
            AND d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = l_llr_dt
            AND ld.load_num = NVL(i_load_num, ld.load_num)
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.excptn_sw = 'N'
            AND b.statb IN('R', 'A')
            AND b.itemnb = e.iteme
            AND b.sllumb = e.uome
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
       GROUP BY ld.load_num, cx.mccusb, c.namec, e.catite, e.ctdsce, e.shppke, e.sizee
       ORDER BY ld.load_num, cx.mccusb, e.catite;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END allocated_items_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_SLOT_INFO_CUR_FN
  ||  Build a cursor of item slot qty info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_slot_info_cur_fn(
    i_div             IN  VARCHAR2,
    i_catlg_num_list  IN  VARCHAR2,
    i_jrsdctn         IN  VARCHAR2 DEFAULT NULL,
    i_item_descr      IN  VARCHAR2 DEFAULT NULL,
    i_slot            IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.ITEM_SLOT_INFO_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_t_catlg_nums       type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CatlgNumList', i_catlg_num_list);
    logs.add_parm(lar_parm, 'Jrsdctn', i_jrsdctn);
    logs.add_parm(lar_parm, 'ItemDescr', i_item_descr);
    logs.add_parm(lar_parm, 'Slot', i_slot);
    logs.dbg('ENTRY', lar_parm);

    IF i_catlg_num_list IS NOT NULL THEN
      logs.dbg('Parse');
      l_t_catlg_nums := str.parse_list(i_catlg_num_list, ',');
      logs.dbg('Open Cursor for Items');

      OPEN l_cv
       FOR
         SELECT   w.itemc, w.uomc, e.catite, di.statb, e.ctdsce, e.shppke, e.sizee, w.taxjrc,
                  w.aislc || w.binc || w.levlc AS slot, w.qohc, w.qalc, w.qavc
             FROM div_mstr_di1d d, sawp505e e, mclp110b di, whsp300c w, TABLE(CAST(l_t_catlg_nums AS type_stab)) t
            WHERE d.div_id = i_div
              AND di.div_part = d.div_part
              AND di.itemb = e.iteme
              AND di.uomb = e.uome
              AND w.div_part = di.div_part
              AND w.itemc = di.itemb
              AND w.uomc = di.uomb
              AND e.catite = t.column_value
              AND (   i_jrsdctn IS NULL
                   OR w.taxjrc = i_jrsdctn)
              AND (   i_item_descr IS NULL
                   OR e.ctdsce LIKE '%' || i_item_descr || '%')
              AND (   i_slot IS NULL
                   OR w.aislc || w.binc || w.levlc LIKE i_slot || '%')
         ORDER BY slot;
    ELSE
      logs.dbg('Open Cursor for Item Selection Info');

      OPEN l_cv
       FOR
         SELECT   w.itemc, w.uomc, e.catite, di.statb, e.ctdsce, e.shppke, e.sizee, w.taxjrc,
                  w.aislc || w.binc || w.levlc AS slot, w.qohc, w.qalc, w.qavc
             FROM div_mstr_di1d d, sawp505e e, mclp110b di, whsp300c w
            WHERE d.div_id = i_div
              AND di.div_part = d.div_part
              AND di.itemb = e.iteme
              AND di.uomb = e.uome
              AND w.div_part = di.div_part
              AND w.itemc = di.itemb
              AND w.uomc = di.uomb
              AND (   i_jrsdctn IS NOT NULL
                   OR i_item_descr IS NOT NULL
                   OR i_slot IS NOT NULL)
              AND (   i_jrsdctn IS NULL
                   OR w.taxjrc = i_jrsdctn)
              AND (   i_item_descr IS NULL
                   OR e.ctdsce LIKE '%' || i_item_descr || '%')
              AND (   i_slot IS NULL
                   OR w.aislc || w.binc || w.levlc LIKE i_slot || '%')
         ORDER BY slot;
    END IF;   -- i_mcl_item_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_slot_info_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_RECPT_SP
  ||  Build cursors of Last Order Received, MQ Order Msgs Waiting to be Processed,
  ||  Orders In-Process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/12/08 | rhalpai | Original - Created for PIR3593
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_recpt_sp(
    i_div               IN      VARCHAR2,
    o_cur_last_ord      OUT     SYS_REFCURSOR,
    o_cur_ord_waiting   OUT     SYS_REFCURSOR,
    o_cur_ord_in_prcss  OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.ORD_RECPT_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Cursor for Last Order Processed');
    o_cur_last_ord := last_ord_cur_fn(i_div);
    logs.dbg('Get Cursor for MQ Order Msgs Waiting');
    o_cur_ord_waiting := ord_waiting_cur_fn(i_div);
    logs.dbg('Get Cursor for Orders In-Process');
    o_cur_ord_in_prcss := ord_in_prcss_cur_fn(i_div);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_recpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || MQ_STATUS_SP
  ||  Builds cursors of MQ GET Msgs Waiting, MQ GET Msgs In-process, MQ PUT Msgs
  ||  In-Process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 06/16/08 | rhalpai | Added sort by MQMsgId to cursors.
  || 05/13/13 | rhalpai | Add logic to process with wrapper for ssh to
  ||                    | Application Server. PIR11038
  || 11/02/20 | rhalpai | Change oscmd_fn to ssh to new MQ server. SDHD-813035
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_status_sp(
    i_div              IN      VARCHAR2,
    o_cur_get_waiting  OUT     SYS_REFCURSOR,
    o_cur_get_in_prcs  OUT     SYS_REFCURSOR,
    o_cur_put_in_prcs  OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.MQ_STATUS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cmd                typ.t_maxvc2;
    l_t_msg              type_stab     := type_stab();
    l_appl_srvr          VARCHAR2(20);
    l_os_result          typ.t_maxvc2;
    l_t_cnt              type_stab;

    CURSOR l_cur_mq(
      b_div_part  NUMBER
    ) IS
      SELECT   c.hostname, c.mq_msg_id, c.mq_msg_id_desc, c.mq_qmanager, 'dis ql(' || c.mq_queue || ') curdepth' AS cmd
          FROM mclane_mq_gp_control c
         WHERE c.div_part = b_div_part
           AND c.gp_type = 'GET'
      ORDER BY 1;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('MQ Get Waiting');
    FOR l_r_mq IN l_cur_mq(l_div_part) LOOP
      IF l_cmd IS NULL THEN
        l_cmd := 'ssh '
                 || l_r_mq.hostname
                 ||' runmqsc -e '
                 || l_r_mq.mq_qmanager
                 || ' << EOF | grep CURDEPTH | sed -e "s/.*RENA.OP.//g" -e "s/).*CURDEPTH/~/g" -e "s/[()]//g"'
                 || cnst.newline_char;
      END IF;   -- l_cmd IS NULL

      l_cmd := l_cmd || l_r_mq.cmd || cnst.newline_char;
      l_t_msg.EXTEND;
      l_t_msg(l_t_msg.LAST) := RPAD(l_r_mq.mq_msg_id, 8) || RPAD(l_r_mq.mq_msg_id_desc, 80);
    END LOOP;

    IF l_cmd IS NOT NULL THEN
      l_cmd := l_cmd || 'EOF' || cnst.newline_char;
      logs.info(l_cmd);
      l_os_result := SUBSTR(oscmd_fn(l_cmd, l_appl_srvr), 2);
      logs.info(l_os_result);
      logs.dbg('Parse');
      l_t_cnt := str.parse_list(l_os_result, cnst.newline_char);
    END IF;   -- l_cmd IS NOT NULL

    logs.dbg('Open Get Waiting Cursor');

    OPEN o_cur_get_waiting
     FOR
       SELECT m.msg_id, m.descr, c.cnt
         FROM (SELECT RTRIM(SUBSTR(t.column_value, 1, 8)) AS msg_id, RTRIM(SUBSTR(t.column_value, 9, 80)) AS descr
                 FROM TABLE(CAST(l_t_msg AS type_stab)) t) m,
              (SELECT SUBSTR(t.column_value, 1, INSTR(t.column_value, '~') - 1) AS msg_id,
                      SUBSTR(t.column_value, INSTR(t.column_value, '~') + 1) AS cnt
                 FROM TABLE(CAST(l_t_cnt AS type_stab)) t) c
        WHERE c.msg_id = m.msg_id
          AND c.cnt <> '0';

    logs.dbg('Open Get In-Process Cursor');

    OPEN o_cur_get_in_prcs
     FOR
       SELECT   g.mq_msg_id, c.mq_msg_id_desc, COUNT(1) AS cnt
           FROM mclane_mq_gp_control c, mclane_mq_get g
          WHERE c.div_part = l_div_part
            AND g.div_part = c.div_part
            AND g.mq_msg_id = c.mq_msg_id
            AND g.mq_msg_status IN('OPN', 'WRK')
       GROUP BY g.mq_msg_id, c.mq_msg_id_desc
       ORDER BY g.mq_msg_id;

    logs.dbg('Open Put In-Process Cursor');

    OPEN o_cur_put_in_prcs
     FOR
       SELECT   p.mq_msg_id, c.mq_msg_id_desc, COUNT(1) AS cnt
           FROM mclane_mq_gp_control c, mclane_mq_put p
          WHERE c.div_part = l_div_part
            AND p.mq_msg_id = c.mq_msg_id
            AND p.div_part = c.div_part
            AND p.mq_msg_status = 'OPN'
       GROUP BY p.mq_msg_id, c.mq_msg_id_desc
       ORDER BY p.mq_msg_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mq_status_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_ETA_ISSUES_CUR_SP
  ||  Build a cursor of Load Depart/ETA issues.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/31/08 | rhalpai | Original - Created for PIR3593
  || 04/10/14 | rhalpai | Change logic to return cursor of possible Load
  ||                    | Depart/ETA time issues. PIR13465
  || 09/03/14 | dlbeal  | Changed function into a procedure. PIR13465
  ||----------------------------------------------------------------------------
  */
  PROCEDURE load_eta_issues_cur_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STATUS_INQUIRY_PK.LOAD_ETA_ISSUES_CUR_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN o_cur
     FOR
       SELECT   x.mccusb AS mcl_cust, x.custd AS cbr_cust, x.loadc AS load_num, LPAD(x.stopd, 2, '0') AS stop_num,
                x.prod_typ, x.llrwkc || ' ' || x.llrcdc || ' ' || LPAD(x.llrctc, 4, '0') AS llr,
                x.depwkc || ' ' || x.depdac || ' ' || LPAD(x.deptmc, 4, '0') AS dep,
                x.wkoffd || ' ' || x.dayrcd || ' ' || LPAD(x.etad, 4, '0') AS eta,
                TO_CHAR(SYSDATE, 'YYYY-MM-DD') AS sys_dt, TO_CHAR(x.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                TO_CHAR(x.dep_ts, 'YYYY-MM-DD HH24:MI') AS dep_ts, TO_CHAR(x.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts
           FROM (SELECT y.mccusb, y.custd, y.loadc, y.stopd, y.prod_typ, y.llrcdc, y.llrctc, y.llrwkc, y.depdac,
                        y.deptmc, y.depwkc, y.dayrcd, y.etad, y.wkoffd, y.llr_ts, y.dep_ts,
                        NEXT_DAY(TO_DATE(TO_CHAR(y.dep_ts
                                                 - 1
                                                 + 7 * y.wkoffd
                                                 +(CASE
                                                     WHEN(    y.wkoffd = 0
                                                          AND y.dayrcd = y.depdac
                                                          AND y.etad < y.deptmc) THEN 7
                                                     ELSE 0
                                                   END
                                                  ),
                                                 'YYYYMMDD'
                                                )
                                         || LPAD(y.etad, 4, '0'),
                                         'YYYYMMDDHH24MI'
                                        ),
                                 y.dayrcd
                                ) AS eta_ts
                   FROM (SELECT z.mccusb, z.custd, z.loadc, z.stopd, z.prod_typ, z.llrcdc, z.llrctc, z.llrwkc,
                                z.depdac, z.deptmc, z.depwkc, z.dayrcd, z.etad, z.wkoffd, z.llr_ts,
                                NEXT_DAY(TO_DATE(TO_CHAR(z.llr_ts
                                                         - 1
                                                         + 7 * z.depwkc
                                                         +(CASE
                                                             WHEN(    z.depwkc = 0
                                                                  AND z.depdac = z.llrcdc
                                                                  AND z.deptmc < z.llrctc
                                                                 ) THEN 7
                                                             ELSE 0
                                                           END
                                                          ),
                                                         'YYYYMMDD'
                                                        )
                                                 || LPAD(z.deptmc, 4, '0'),
                                                 'YYYYMMDDHH24MI'
                                                ),
                                         z.depdac
                                        ) AS dep_ts
                           FROM (SELECT cx.mccusb, md.custd, c.loadc, md.stopd, md.prod_typ, c.llrcdc, c.llrctc,
                                        c.llrwkc, c.depdac, c.deptmc, c.depwkc, md.dayrcd, md.etad, md.wkoffd,
                                        NEXT_DAY(TO_DATE(TO_CHAR(SYSDATE - 1 + 7 * c.llrwkc, 'YYYYMMDD')
                                                         || LPAD(c.llrctc, 4, '0'),
                                                         'YYYYMMDDHH24MI'
                                                        ),
                                                 c.llrcdc
                                                ) AS llr_ts
                                   FROM div_mstr_di1d d, mclp120c c, mclp040d md, mclp020b cx
                                  WHERE d.div_id = i_div
                                    AND c.div_part = d.div_part
                                    AND md.div_part = c.div_part
                                    AND md.loadd = c.loadc
                                    AND cx.div_part = md.div_part
                                    AND cx.custb = md.custd) z) y) x
          WHERE (   (    x.llrcdc = x.depdac
                     AND x.llrctc > x.deptmc)
                 OR (    x.depdac = x.dayrcd
                     AND x.deptmc > x.etad)
                 OR x.dep_ts - x.llr_ts > 6 + 7 * x.depwkc
                 OR x.eta_ts - x.dep_ts > 6 + 7 * x.wkoffd
                )
       ORDER BY x.loadc, x.stopd;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_eta_issues_cur_sp;
END op_status_inquiry_pk;
/

