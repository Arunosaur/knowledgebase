CREATE OR REPLACE PACKAGE op_bus_move_pk IS
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

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE move_cust_sp(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2,
    i_old_div  IN  VARCHAR2
  );
END op_bus_move_pk;
/

CREATE OR REPLACE PACKAGE BODY op_bus_move_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || SET_USERINFO_SP
  ||  Set User Info Context to new service
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/07/14 | rhalpai | Original for PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE set_userinfo_sp(
    i_svc_nm  IN  VARCHAR2
  ) IS
  BEGIN
    EXECUTE IMMEDIATE 'BEGIN set_userinfo_ctx.set_userinfo(''' || i_svc_nm || '''); END;';
  END set_userinfo_sp;

  /*
  ||----------------------------------------------------------------------------
  || MQ_PUT_SP
  ||  Put msgs to MQ
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/07/14 | rhalpai | Original for PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_put_sp(
    i_div      IN  VARCHAR2,
    i_msg_id   IN  VARCHAR2,
    i_corr_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BUS_MOVE_PK.MQ_PUT_SP';
    lar_parm             logs.tar_parm;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'CorrId', i_corr_id);
    logs.info('ENTRY', lar_parm);
    op_mq_message_pk.mq_put_sp(i_msg_id, i_div, i_corr_id, l_rc);
    excp.assert((l_rc = 0), 'Failed to put msgs to MQ');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mq_put_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_WKLY_MAX_QTY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/07/14 | rhalpai | Original for PIR11038
  || 12/01/15 | rhalpai | Change logic to handle table RI. PIR15202
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_wkly_max_qty_sp(
    i_div_part      IN  NUMBER,
    i_cust_id       IN  VARCHAR2,
    i_old_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BUS_MOVE_PK.UPD_WKLY_MAX_QTY_SP';
    lar_parm             logs.tar_parm;
    l_t_sid              type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'OldDivPart', i_old_div_part);
    logs.info('ENTRY', lar_parm);
    set_userinfo_sp('ALL');

    SELECT ci.cust_item_sid
    BULK COLLECT INTO l_t_sid
      FROM wkly_max_cust_item_op1m ci
     WHERE ci.div_part = i_old_div_part
       AND ci.cust_id = i_cust_id;

    IF l_t_sid.COUNT > 0 THEN
      FORALL i IN l_t_sid.FIRST .. l_t_sid.LAST
        INSERT INTO wkly_max_cust_item_op1m
                    (cust_item_sid, div_part, cust_id, catlg_num, pick_qty)
          SELECT cust_item_sid, i_div_part, cust_id, catlg_num, pick_qty
            FROM wkly_max_cust_item_op1m ci
           WHERE ci.div_part = i_old_div_part
             AND ci.cust_item_sid = l_t_sid(i);
      FORALL i IN l_t_sid.FIRST .. l_t_sid.LAST
        UPDATE wkly_max_qty_op2m q
           SET q.div_part = i_div_part
         WHERE q.div_part = i_old_div_part
           AND q.cust_item_sid = l_t_sid(i);
      FORALL i IN l_t_sid.FIRST .. l_t_sid.LAST
        UPDATE wkly_max_log_op3m l
           SET l.div_part = i_div_part
         WHERE l.div_part = i_old_div_part
           AND l.cust_item_sid = l_t_sid(i);
      FORALL i IN l_t_sid.FIRST .. l_t_sid.LAST
        DELETE FROM wkly_max_cust_item_op1m ci
              WHERE ci.div_part = i_old_div_part
                AND ci.cust_item_sid = l_t_sid(i);
    END IF;   -- l_t_sid.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END upd_wkly_max_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_HIST_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/07/14 | rhalpai | Original for PIR11038
  || 12/01/15 | rhalpai | Change logic to pass div_part to
  ||                    | OP_CLEANUP_PK.MOVE_ORDER_TO_HIST_SP. PIR15202
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_hist_sp(
    i_div           IN  VARCHAR2,
    i_div_part      IN  NUMBER,
    i_cust_id       IN  VARCHAR2,
    i_old_div_part  IN  NUMBER,
    i_old_div       IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BUS_MOVE_PK.UPD_ORD_HIST_SP';
    lar_parm             logs.tar_parm;
    l_from_eta_dt        DATE;
    l_to_eta_dt          DATE;
    l_t_ord_nums         type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'OldDivPart', i_old_div_part);
    logs.add_parm(lar_parm, 'OldDiv', i_old_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialze');
    set_userinfo_sp('ALL');
    l_from_eta_dt := NEXT_DAY(TRUNC(SYSDATE) - 7, 'SUN');
    l_to_eta_dt := NEXT_DAY(TRUNC(SYSDATE) - 1, 'SAT');
    logs.dbg('Get ShpOrds');

    SELECT a.ordnoa AS ord_num
    BULK COLLECT INTO l_t_ord_nums
      FROM stop_eta_op1g se, ordp100a a
     WHERE se.div_part = i_div_part
       AND se.cust_id = i_cust_id
       AND TRUNC(se.eta_ts) BETWEEN l_from_eta_dt AND l_to_eta_dt
       AND a.div_part = i_div_part
       AND a.load_depart_sid = se.load_depart_sid
       AND a.custa = se.cust_id
       AND a.stata = 'A'
       AND a.excptn_sw = 'N';

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Move ShpOrds to Hist');
      set_userinfo_sp(i_old_div || '_OP');
      FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
        op_cleanup_pk.move_order_to_hist_sp(i_old_div_part, l_t_ord_nums(i));
      END LOOP;
      l_t_ord_nums := NULL;
      set_userinfo_sp('ALL');
      logs.dbg('Upd OrdHist');

      EXECUTE IMMEDIATE 'UPDATE ordp900a a'
                        || ' SET a.div_part = :div_part'
                        || ' WHERE a.div_part = :old_div_part'
                        || ' AND a.custa = :cust_id'
                        || ' AND a.etadta BETWEEN :from_eta_dt AND :to_eta_dt'
                        || ' AND a.stata = ''A'''
                  USING i_div_part,
                        i_old_div_part,
                        i_cust_id,
                        l_from_eta_dt - DATE '1900-02-28',
                        l_to_eta_dt - DATE '1900-02-28';
    END IF;   -- l_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_hist_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/07/14 | rhalpai | Original for PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  || 12/01/15 | rhalpai | Change logic to update div_part for related ord tables
  ||                    | and pass div_part to OP_ORDER_LOAD_PK.STOP_NUM_FN and
  ||                    | OP_ORDER_LOAD_PK.MERGE_STOP_ETA_SP. PIR15202
  || 01/05/16 | rhalpai | Change logic to remove restriction requiring existence
  ||                    | of EDI order prior to updating suspended and open
  ||                    | orders. PIR15531
  || 09/14/21 | rhalpai | Change call to Reprice to pass 2999-12-31 as llr_dt_to in parm list. SDHD-1008135
  || 10/26/21 | rhalpai | Change call to Reprice to pass batch_ftp as extr_dest and pass 1900-01-01 as llr_dt_to in parm list. SDHD-1008135
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_sp(
    i_div           IN  VARCHAR2,
    i_div_part      IN  NUMBER,
    i_cust_id       IN  VARCHAR2,
    i_mcl_cust      IN  VARCHAR2,
    i_old_div_part  IN  NUMBER,
    i_old_div       IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BUS_MOVE_PK.UPD_ORD_SP';
    lar_parm             logs.tar_parm;
    l_t_msg_data         type_stab;
    l_put_sw             VARCHAR2(1);

    PROCEDURE upd_ord_tbls_sp(
      i_t_ord_nums  IN  type_ntab
    ) IS
    BEGIN
      IF i_t_ord_nums.COUNT > 0 THEN
        FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
          UPDATE ordp120b b
             SET b.div_part = i_div_part
           WHERE b.div_part = i_old_div_part
             AND b.ordnob = i_t_ord_nums(i);
        FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
          UPDATE ordp140c c
             SET c.div_part = i_div_part
           WHERE c.div_part = i_old_div_part
             AND c.ordnoc = i_t_ord_nums(i);
        FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
          UPDATE mclp300d md
             SET md.div_part = i_div_part
           WHERE md.div_part = i_old_div_part
             AND md.ordnod = i_t_ord_nums(i);
        FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
          UPDATE sysp296a sa
             SET sa.div_part = i_div_part
           WHERE sa.div_part = i_old_div_part
             AND sa.ordnoa = i_t_ord_nums(i);
        FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
          DELETE FROM gov_cntl_log_p680a l
                WHERE l.div_part = i_old_div_part
                  AND l.ord_num = i_t_ord_nums(i);
        FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
          DELETE FROM bill_cntnr_id_bc1c bc
                WHERE bc.div_part = i_old_div_part
                  AND bc.ord_num = i_t_ord_nums(i);
      END IF;   -- i_t_ord_nums.COUNT > 0
    END upd_ord_tbls_sp;

    PROCEDURE upd_suspnd_ord_sp IS
      l_load_depart_sid  NUMBER;
      l_stop_num         NUMBER;
      l_t_ord_nums       type_ntab;
    BEGIN
      logs.dbg('Get LoadDepartSid');
      set_userinfo_sp(i_div || '_OP');
      l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, DATE '1900-01-01', 'DFLT');
      logs.dbg('Get Stop');
      l_stop_num := op_order_load_pk.stop_num_fn(i_div_part, l_load_depart_sid, i_cust_id);
      logs.dbg('Upd Ord Hdr');
      set_userinfo_sp('ALL');

      UPDATE    ordp100a a
            SET a.div_part = i_div_part,
                a.load_depart_sid = l_load_depart_sid
          WHERE a.div_part = i_old_div_part
            AND a.custa = i_cust_id
            AND a.dsorda = 'R'
            AND a.stata = 'S'
      RETURNING         a.ordnoa
      BULK COLLECT INTO l_t_ord_nums;

      logs.dbg('Upd Ord Tbls');
      upd_ord_tbls_sp(l_t_ord_nums);
      logs.dbg('Merge Stop Eta');
      set_userinfo_sp(i_div || '_OP');
      op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, i_cust_id, DATE '1900-01-01', l_stop_num);
    END upd_suspnd_ord_sp;

    PROCEDURE upd_open_ord_sp IS
      l_load_depart_sid  NUMBER;
      l_stop_num         NUMBER;
      l_t_ord_nums       type_ntab;
    BEGIN
      logs.dbg('Get LoadDepartSid');
      set_userinfo_sp(i_div || '_OP');
      l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, DATE '1900-01-01', 'DFLT');
      logs.dbg('Get Stop');
      l_stop_num := op_order_load_pk.stop_num_fn(i_div_part, l_load_depart_sid, i_cust_id);
      logs.dbg('Upd Ord Hdr');
      set_userinfo_sp('ALL');

      UPDATE    ordp100a a
            SET a.div_part = i_div_part,
                a.load_depart_sid = l_load_depart_sid,
                a.connba = 'B' || ordp100a_connba_seq.NEXTVAL
          WHERE a.div_part = i_old_div_part
            AND a.custa = i_cust_id
            AND a.dsorda = 'R'
            AND a.stata = 'O'
            AND NOT EXISTS(SELECT 1
                             FROM strct_ord_op1o so
                            WHERE so.div_part = a.div_part
                              AND so.ord_num = a.ordnoa
                              AND so.stat <> 'URC')
      RETURNING         a.ordnoa
      BULK COLLECT INTO l_t_ord_nums;

      logs.dbg('Upd Ord Tbls');
      upd_ord_tbls_sp(l_t_ord_nums);
      logs.dbg('Get Ords for Processing');
      set_userinfo_sp(i_div || '_OP');

      SELECT a.ordnoa
      BULK COLLECT INTO l_t_ord_nums
        FROM ordp100a a
       WHERE a.div_part = i_div_part
         AND a.custa = i_cust_id
         AND a.stata IN('O', 'S')
         AND a.load_depart_sid = l_load_depart_sid;

      IF l_t_ord_nums.COUNT > 0 THEN
        logs.dbg('Merge Stop Eta');
        op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, i_cust_id, DATE '1900-01-01', l_stop_num);
        logs.dbg('Process Reprice');
        op_reprice_pk.reprice_bulk_sp(i_div,
                                      op_reprice_pk.g_c_cust,
                                      op_reprice_pk.g_c_batch_ftp,
                                      '1900-01-01~1900-01-01~' || i_mcl_cust
                                     );
        logs.dbg('Process Syncload');
        op_order_load_pk.syncload_sp(i_div_part, 'BUSMOVE', l_t_ord_nums);
      END IF;   -- l_t_ord_nums.COUNT > 0
    END upd_open_ord_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'OldDivPart', i_old_div_part);
    logs.add_parm(lar_parm, 'OldDiv', i_old_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get Put Msg Data');
    set_userinfo_sp(i_old_div || '_OP');

    SELECT x.msg_data
    BULK COLLECT INTO l_t_msg_data
      FROM (SELECT i_div
                   || RPAD('BUSMOVEMF', 51)
                   || i_old_div
                   || i_div
                   || i_cust_id
                   || rpad_fn(a.cpoa, 30)
                   || TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD"-"HH24"."MI"."SS".000000"') AS msg_data
              FROM ordp100a a
             WHERE a.custa = i_cust_id
               AND a.div_part = i_old_div_part
               AND a.stata = 'S'
               AND a.dsorda = 'R'
               AND a.ipdtsa = 'EDI'
            UNION
            SELECT i_div
                   || RPAD('BUSMOVEMF', 51)
                   || i_old_div
                   || i_div
                   || i_cust_id
                   || rpad_fn(a.cpoa, 30)
                   || TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD"-"HH24"."MI"."SS".000000"') AS msg_data
              FROM ordp100a a
             WHERE a.custa = i_cust_id
               AND a.div_part = i_old_div_part
               AND a.stata = 'O'
               AND a.dsorda = 'R'
               AND a.ipdtsa = 'EDI'
               AND NOT EXISTS(SELECT 1
                                FROM strct_ord_op1o so
                               WHERE so.div_part = a.div_part
                                 AND so.ord_num = a.ordnoa
                                 AND so.stat <> 'URC')) x;

    IF l_t_msg_data.COUNT > 0 THEN
      logs.dbg('Ins PUT Records');
      set_userinfo_sp(i_div || '_OP');
      FORALL i IN l_t_msg_data.FIRST .. l_t_msg_data.LAST
        INSERT INTO mclane_mq_put
                    (mq_msg_id, div_part, mq_msg_status, mq_msg_data
                    )
             VALUES ('BUSMOVEMF', i_div_part, 'OPN', l_t_msg_data(i)
                    );
      l_put_sw :=(CASE
                    WHEN SQL%ROWCOUNT > 0 THEN 'Y'
                  END);

      IF l_put_sw = 'Y' THEN
        COMMIT;
        set_userinfo_sp(i_div || '_OP');
        mq_put_sp('BUSMOVEMF', i_div);
      END IF;   -- l_put_sw = 'Y'
    END IF;   -- l_t_msg_data.COUNT > 0

    logs.dbg('Upd Suspnd Ords');
    upd_suspnd_ord_sp;
    logs.dbg('Upd Opn Ords');
    upd_open_ord_sp;
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || MOVE_CUST_SP
  ||  Used for Business Moves / Business Continuity
  ||  Import WklyMaxQty info, Orders in Shipped status for current week Sun-Sat,
  ||  orders in Suspended status, and orders in Open status from Old Division.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/07/14 | rhalpai | Original for PIR11038
  || 12/01/15 | rhalpai | Copy logic from NEW_TO_NEW_SP. PIR15202
  || 10/26/21 | rhalpai | Change logic to get MclCust for new div. SDHD-1008135
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_cust_sp(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2,
    i_old_div  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_BUS_MOVE_PK.MOVE_CUST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_mcl_cust           mclp020b.mccusb%TYPE;
    l_old_div_part       NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'OldDiv', i_old_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get DivPart/MclCust/OldDivPart');
    set_userinfo_sp('ALL');

    SELECT d.div_part, cx.mccusb, d2.div_part
      INTO l_div_part, l_mcl_cust, l_old_div_part
      FROM div_mstr_di1d d, div_mstr_di1d d2, sysp200c c, mclp020b cx
     WHERE d.div_id = i_div
       AND d2.div_id = i_old_div
       AND cx.div_part = d.div_part
       AND cx.custb = i_cust_id
       AND c.acnoc = cx.custb
       AND c.div_part = cx.div_part;

    logs.dbg('Upd WklyMaxQty');
    upd_wkly_max_qty_sp(l_div_part, i_cust_id, l_old_div_part);
    logs.dbg('Upd Ord Hist');
    upd_ord_hist_sp(i_div, l_div_part, i_cust_id, l_old_div_part, i_old_div);
    logs.dbg('Upd Ord');
    upd_ord_sp(i_div, l_div_part, i_cust_id, l_mcl_cust, l_old_div_part, i_old_div);
    logs.dbg('Set User Info Context back to Div');
    set_userinfo_sp(i_div || '_OP');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      set_userinfo_sp(i_div || '_OP');
      logs.err(lar_parm);
  END move_cust_sp;
END op_bus_move_pk;
/

