CREATE OR REPLACE PACKAGE op_load_close_pk IS
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
  FUNCTION loc_typ_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_loc_typ  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE get_acs_force_loads_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR,
    o_msg  OUT     VARCHAR2
  );

  PROCEDURE force_acs_close_sp(
    i_div          IN  VARCHAR2,
    i_parm_list    IN  VARCHAR2,
    i_rsn          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'UNKNOWN',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE cancl_opn_ord_lns_sp(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  );

  PROCEDURE start_load_close_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  );

  PROCEDURE tag_for_close_sp(
    i_div        IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    i_parm_list  IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  );

  PROCEDURE close_tagged_loads_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );
END op_load_close_pk;
/

CREATE OR REPLACE PACKAGE BODY op_load_close_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_tagged IS RECORD(
    load_num  mclp120c.loadc%TYPE,
    llr_dt    DATE
  );

  TYPE g_tt_tagged IS TABLE OF g_rt_tagged;

  g_c_tbill_load      CONSTANT VARCHAR2(1) := 'Y';
  g_c_non_tbill_load  CONSTANT VARCHAR2(1) := 'N';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || UPD_EVNT_LOG_SP
  ||  Update the event log
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/11 | rhalpai | Original for PIR10475
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_evnt_log_sp(
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER,
    i_evnt_msg     IN  VARCHAR2,
    i_finish_cd    IN  NUMBER DEFAULT 0
  ) IS
  BEGIN
    cig_event_mgr_pk.update_log_message(i_evnt_que_id,
                                        i_cycl_id,
                                        i_cycl_dfn_id,
                                        SUBSTR(i_evnt_msg, 1, 512),
                                        i_finish_cd
                                       );
  END upd_evnt_log_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_SP
  ||   Parse groups of Load/LLR Date lists into Load and LLR Date PLSQL-Tables.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3593
  || 10/14/17 | rhalpai | Change to use constants package OP_CONST_PK. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_sp(
    i_parm_list    IN      VARCHAR2,
    o_t_loads      OUT     type_stab,
    o_t_llr_dates  OUT     type_ntab
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.PARSE_SP';
    lar_parm             logs.tar_parm;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    o_t_loads := type_stab();
    o_t_llr_dates := type_ntab();
    logs.dbg('Parse Groups of Parm Field Lists');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_t_fields := NULL;
        logs.dbg('Parse Parm Field List');
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
        o_t_loads.EXTEND;
        o_t_loads(l_idx) := l_t_fields(1);
        o_t_llr_dates.EXTEND;
        o_t_llr_dates(l_idx) := TO_DATE(l_t_fields(2), 'YYYY-MM-DD') - DATE '1900-02-28';
        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;
    END IF;   -- l_t_grps IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END parse_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_BUNDL_DIST_CUST_SP
  ||   Add entry to Bundle Dist Cust Override table when one or more items but
  ||   not all items are pick-adjusted to zero. This will allow these
  ||   pick-adjusted orders to bill as regular distributions instead of
  ||   "all-or-nothing" bundle distributions when they are sent back down from
  ||   the mainframe after billing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/21/05 | rhalpai | Original - created for PIR2545
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP. PIR3593
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_bundl_dist_cust_sp(
    i_div_part  IN  NUMBER,
    i_llr_dt    IN  DATE,
    i_load_num  IN  VARCHAR2
  ) IS
  BEGIN
    INSERT INTO bundl_dist_cust_bd1c
                (dist_id, dist_sfx, cust_id)
      SELECT   bi.dist_id, bi.dist_sfx, a.custa
          FROM load_depart_op1f ld, ordp100a a, ordp120b b, bundl_dist_item_bd1i bi
         WHERE ld.div_part = i_div_part
           AND ld.llr_ts = i_llr_dt
           AND ld.load_num = i_load_num
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.dsorda = 'D'
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb = 'R'
           AND b.excptn_sw = 'N'
           AND b.subrcb = 0
           AND b.pckqtb = 0
           AND bi.div_part = a.div_part
           AND bi.dist_id = SUBSTR(a.legrfa, 1, 10)
           AND LPAD(bi.dist_sfx, 2, '0') = SUBSTR(a.legrfa, 12, 2)
           AND bi.item_num = b.itemnb
           AND bi.unq_cd = b.sllumb
           AND EXISTS(SELECT 1
                        FROM ordp100a a2, ordp120b b2, bundl_dist_item_bd1i bi2
                       WHERE a2.div_part = a.div_part
                         AND a2.load_depart_sid = a.load_depart_sid
                         AND a2.custa = a.custa
                         AND a2.dsorda = 'D'
                         AND b2.div_part = a2.div_part
                         AND b2.ordnob = a2.ordnoa
                         AND b2.statb = 'R'
                         AND b2.excptn_sw = 'N'
                         AND b2.subrcb = 0
                         AND b2.pckqtb > 0
                         AND bi2.div_part = bi.div_part
                         AND bi2.dist_id = bi.dist_id
                         AND bi2.dist_sfx = bi.dist_sfx
                         AND bi2.item_num = b2.itemnb
                         AND bi2.unq_cd = b2.sllumb)
           AND NOT EXISTS(SELECT 1
                            FROM bundl_dist_cust_bd1c bc
                           WHERE bc.div_part = bi.div_part
                             AND bc.dist_id = bi.dist_id
                             AND bc.dist_sfx = bi.dist_sfx
                             AND bc.cust_id = a.custa)
      GROUP BY bi.dist_id, bi.dist_sfx, a.custa;
  END ins_bundl_dist_cust_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_QOPRC08_SP
  ||   Add "Tote Information Extract" (QOPRC08) MQ Messages.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 04/05/06 | rhalpai | Added constant for TestBill Load indicator.
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and removed dependency
  ||                    | for mclp370c.load_status = 'R'. PIR3593
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  || 07/01/19 | rhalpai | Add Peco pallet count. PIR19620
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_qoprc08_sp(
    i_div_part   IN      NUMBER,
    i_llr_dt     IN      DATE,
    i_load_num   IN      VARCHAR2,
    i_tbill_sw   IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_cnt        OUT     PLS_INTEGER
  ) IS
    l_c_llr_num  CONSTANT PLS_INTEGER                    := TRUNC(i_llr_dt) - DATE '1900-02-28';
    l_mq_msg_id           mclane_mq_put.mq_msg_id%TYPE;
  BEGIN
    l_mq_msg_id :=(CASE i_tbill_sw
                     WHEN g_c_tbill_load THEN 'QTPRC08'
                     ELSE 'QOPRC08'
                   END);

    -- QOPRC08 len: 133
    INSERT INTO mclane_mq_put
                (div_part, mq_msg_id, mq_msg_data, mq_msg_status, create_ts)
      SELECT i_div_part, l_mq_msg_id, x.msg_data, 'OPN', i_create_ts
        FROM (SELECT   d.div_id
                       || RPAD(l_mq_msg_id, 38)
                       || RPAD('ADD', 13)
                       || i_load_num
                       || lpad_fn(c.stopc, 2, '0')
                       || lpad_fn(c.custc, 8, '0')
                       || lpad_fn(cx.mccusb, 6, '0')
                       || lpad_fn(c.manctc, 3, '0')
                       || lpad_fn(c.totctc, 3, '0')
                       || lpad_fn(SUM(c.totsmc), 9, '0')
                       || lpad_fn(SUM(c.boxsmc), 9, '0')
                       || lpad_fn(SUM(c.bagsmc), 9, '0')
                       || lpad_fn(SUM(c.palsmc), 9, '0')
                       || lpad_fn(SUM(c.cpasmc), 9, '0')
                       || lpad_fn(SUM(c.peco_pallet_cnt), 9, '0') AS msg_data
                  FROM div_mstr_di1d d, mclp370c c, mclp020b cx
                 WHERE d.div_part = i_div_part
                   AND c.div_part = d.div_part
                   AND c.loadc = i_load_num
                   AND c.llr_date = l_c_llr_num
                   AND cx.div_part = c.div_part
                   AND cx.custb = c.custc
              GROUP BY d.div_id, c.stopc, c.custc, cx.mccusb, c.totctc, c.manctc) x;

    o_cnt := SQL%ROWCOUNT;
  END ins_qoprc08_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_QOPRC17_SP
  ||   Add "Line-Out Extract" (QOPRC17) MQ Messages.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 04/05/06 | rhalpai | Added constant for TestBill Load indicator.
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP. PIR3593
  || 07/10/12 | rhalpai | Remove unused column, TICKTB. PIR11038
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_qoprc17_sp(
    i_div_part   IN      NUMBER,
    i_llr_dt     IN      DATE,
    i_load_num   IN      VARCHAR2,
    i_tbill_sw   IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_cnt        OUT     PLS_INTEGER
  ) IS
    l_mq_msg_id  mclane_mq_put.mq_msg_id%TYPE;
  BEGIN
    l_mq_msg_id :=(CASE i_tbill_sw
                     WHEN g_c_tbill_load THEN 'QTPRC17'
                     ELSE 'QOPRC17'
                   END);

    -- QOPRC17 len: 208
    INSERT INTO mclane_mq_put
                (div_part, mq_msg_id, mq_msg_data, mq_msg_status, create_ts)
      SELECT i_div_part, l_mq_msg_id, x.msg_data, 'OPN', i_create_ts
        FROM (SELECT d.div_id
                     || RPAD(l_mq_msg_id, 38)
                     || RPAD('ADD', 13)
                     || i_load_num
                     || lpad_fn(se.stop_num, 2, '0')
                     || TO_CHAR(se.eta_ts, 'YYYYMMDD')
                     || lpad_fn(cx.mccusb, 6, '0')
                     || lpad_fn(se.cust_id, 8, '0')
                     || lpad_fn(e.catite, 6, '0')
                     || lpad_fn(b.itemnb, 9, '0')
                     || rpad_fn(b.sllumb, 3)
                     || lpad_fn(b.ordnob, 11, '0')
                     || TO_CHAR(b.lineb, 'FM0999999V99')
                     || lpad_fn(b.ordqtb, 7, '0')
                     || lpad_fn(b.alcqtb, 7, '0')
                     || lpad_fn(b.pckqtb, 7, '0')
                     || TO_CHAR(se.eta_ts, 'YYYYMMDDHH24MI')
                     || rpad_fn(e.scbcte, 3)
                     || (SELECT DECODE(MAX(p.cust_num), NULL, 'N', 'N', 'N', 'Y')
                           FROM prepost_load_op1p p
                          WHERE p.div_part = ld.div_part
                            AND p.load_num = ld.load_num
                            AND p.stop_num = se.stop_num
                            AND p.cust_num = se.cust_id
                            AND p.llr_date = i_llr_dt)
                     || RPAD(NVL(b.ntshpb, '120'), 3)
                     || TO_CHAR(NVL(b.hdprcb, 0), 'FM0999999V99')
                     || TO_CHAR(NVL(b.hdrtab, 0), 'FM0999999V99')
                     || RPAD(' ', 10)
                     || lpad_fn(b.cusitb, 9, '0')
                     || lpad_fn((CASE
                                   WHEN(    a.dsorda = 'R'
                                        AND SUBSTR(b.itpasb, 17, 3) = 'WMT') THEN SUBSTR(b.itpasb, 6, 2)
                                   WHEN(    a.dsorda = 'D'
                                        AND SUBSTR(b.itpasb, 18, 3) = 'WMT') THEN SUBSTR(b.itpasb, 7, 2)
                                   ELSE '00'
                                 END
                                ),
                                2,
                                '0'
                               )
                     || lpad_fn((CASE
                                   WHEN(    a.dsorda = 'R'
                                        AND SUBSTR(b.itpasb, 17, 3) = 'WMT') THEN SUBSTR(a.cpoa, 1, 2)
                                   WHEN(    a.dsorda = 'D'
                                        AND SUBSTR(b.itpasb, 18, 3) = 'WMT') THEN SUBSTR(a.cpoa, 20, 2)
                                   ELSE '00'
                                 END
                                ),
                                2,
                                '0'
                               )
                     || RPAD(' ', 8) AS msg_data
                FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, mclp020b cx,
                     sawp505e e
               WHERE d.div_part = i_div_part
                 AND ld.div_part = d.div_part
                 AND ld.llr_dt = i_llr_dt
                 AND ld.load_num = i_load_num
                 AND a.div_part = ld.div_part
                 AND a.load_depart_sid = ld.load_depart_sid
                 AND se.div_part = a.div_part
                 AND se.load_depart_sid = a.load_depart_sid
                 AND se.cust_id = a.custa
                 AND cx.div_part = a.div_part
                 AND cx.custb = a.custa
                 AND b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.statb = 'R'
                 AND b.excptn_sw = 'N'
                 AND b.subrcb < 999
                 AND b.alcqtb > 0
                 AND b.alcqtb > b.pckqtb
                 AND e.iteme = b.itemnb
                 AND e.uome = b.sllumb
                 AND NOT EXISTS(SELECT 1
                                  FROM bulk_out_bo1o bo1o
                                 WHERE bo1o.div_part = b.div_part
                                   AND bo1o.ord_num = b.ordnob
                                   AND bo1o.ord_ln_num = b.lineb
                                   AND NOT EXISTS(SELECT 1
                                                    FROM sysp200c c
                                                   WHERE c.div_part = a.div_part
                                                     AND c.acnoc = a.custa
                                                     AND c.tclscc = 'PRP'))) x;

    o_cnt := SQL%ROWCOUNT;
  END ins_qoprc17_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_QOPRC28_SP
  ||   Add "Billing Container Id Change Extract" (QOPRC28) MQ Messages.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/05/06 | rhalpai | Original. PIR3209
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_qoprc28_sp(
    i_div_part   IN      NUMBER,
    i_llr_dt     IN      DATE,
    i_load_num   IN      VARCHAR2,
    i_tbill_sw   IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_cnt        OUT     PLS_INTEGER
  ) IS
    l_mq_msg_id  mclane_mq_put.mq_msg_id%TYPE;
  BEGIN
    l_mq_msg_id :=(CASE i_tbill_sw
                     WHEN g_c_tbill_load THEN 'QTPRC28'
                     ELSE 'QOPRC28'
                   END);

    -- QOPRC28 len: 143
    INSERT INTO mclane_mq_put
                (div_part, mq_msg_id, mq_msg_data, mq_msg_status, create_ts)
      SELECT i_div_part, l_mq_msg_id, x.msg_data, 'OPN', i_create_ts
        FROM (SELECT d.div_id
                     || RPAD(l_mq_msg_id, 38)
                     || RPAD('ADD', 13)
                     || i_load_num
                     || lpad_fn(se.stop_num, 2, '0')
                     || LPAD(bc.ord_num, 11, '0')
                     || TO_CHAR(bc.ord_ln_num, 'FM0999999V99')
                     || rpad_fn(bc.orig_cntnr_id, 20)
                     || rpad_fn(bc.adj_cntnr_id, 20)
                     || lpad_fn(bc.orig_qty, 7, '0')
                     || lpad_fn(bc.adj_qty, 7, '0') AS msg_data
                FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b,
                     bill_cntnr_id_bc1c bc
               WHERE d.div_part = i_div_part
                 AND ld.div_part = d.div_part
                 AND ld.llr_dt = i_llr_dt
                 AND ld.load_num = i_load_num
                 AND a.div_part = ld.div_part
                 AND a.load_depart_sid = ld.load_depart_sid
                 AND se.div_part = a.div_part
                 AND se.load_depart_sid = a.load_depart_sid
                 AND se.cust_id = a.custa
                 AND b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.excptn_sw = 'N'
                 AND b.statb = 'R'
                 AND bc.div_part = b.div_part
                 AND bc.ord_num = b.ordnob
                 AND bc.ord_ln_num = b.lineb
                 AND (   bc.adj_cntnr_id <> bc.orig_cntnr_id
                      OR bc.adj_qty <> bc.orig_qty)
                 AND NOT EXISTS(SELECT 1
                                  FROM bulk_out_bo1o bo1o
                                 WHERE bo1o.div_part = b.div_part
                                   AND bo1o.ord_num = b.ordnob
                                   AND bo1o.ord_ln_num = b.lineb
                                   AND NOT EXISTS(SELECT 1
                                                    FROM sysp200c c
                                                   WHERE c.div_part = a.div_part
                                                     AND c.acnoc = a.custa
                                                     AND c.tclscc = 'PRP'))) x;

    o_cnt := SQL%ROWCOUNT;
  END ins_qoprc28_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_QOPRC21_SP
  ||   Add "Load Close Information" (QOPRC21) MQ Messages.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 04/05/06 | rhalpai | Added constant for TestBill Load indicator.
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP. PIR3593
  || 12/05/06 | rhalpai | Added counts for QOPRC28 Billing Container Id Change
  ||                    | Extract. PIR3209
  || 09/18/08 | rhalpai | Added LLR Date to QOPRC21 msg.
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_qoprc21_sp(
    i_div_part     IN  NUMBER,
    i_llr_dt       IN  DATE,
    i_load_num     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_tbill_sw     IN  VARCHAR2,
    i_create_ts    IN  DATE,
    i_qoprc08_cnt  IN  PLS_INTEGER,
    i_qoprc17_cnt  IN  PLS_INTEGER,
    i_qoprc28_cnt  IN  PLS_INTEGER
  ) IS
    l_c_llr_num      CONSTANT PLS_INTEGER                    := TRUNC(i_llr_dt) - DATE '1900-02-28';
    l_c_llr_dt_char  CONSTANT VARCHAR2(10)                   := TO_CHAR(i_llr_dt, 'YYYY-MM-DD');
    l_mq_msg_id               mclane_mq_put.mq_msg_id%TYPE;
  BEGIN
    l_mq_msg_id :=(CASE i_tbill_sw
                     WHEN g_c_tbill_load THEN 'QTPRC21'
                     ELSE 'QOPRC21'
                   END);

    -- QOPRC21 len: 126
    INSERT INTO mclane_mq_put
                (div_part, mq_msg_id, mq_msg_data, mq_msg_status, create_ts)
      SELECT i_div_part, l_mq_msg_id, x.msg_data, 'OPN', i_create_ts
        FROM (SELECT   d.div_id
                       || RPAD(l_mq_msg_id, 38)
                       || RPAD('ADD', 13)
                       || rpad_fn(i_user_id, 20)
                       || lpad_fn(i_qoprc08_cnt, 9, '0')
                       || lpad_fn(i_qoprc17_cnt, 9, '0')
                       || i_load_num
                       || rpad_fn(MAX(TO_CHAR(DATE '1900-02-28' + c.depdtc, 'YYYYMMDD') || LPAD(ld.deptmc, 4, '0')), 12)
                       || lpad_fn(i_qoprc28_cnt, 9, '0')
                       || l_c_llr_dt_char AS msg_data
                  FROM div_mstr_di1d d, mclp120c ld, mclp370c c
                 WHERE d.div_part = i_div_part
                   AND ld.div_part = d.div_part
                   AND ld.loadc = i_load_num
                   AND c.div_part = d.div_part
                   AND c.loadc = ld.loadc
                   AND c.llr_date = l_c_llr_num
              GROUP BY d.div_id) x;
  END ins_qoprc21_sp;

  /*
  ||----------------------------------------------------------------------------
  || CIG_CLOS_LOAD_SP
  ||  Close Load in Cig System.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/27/10 | rhalpai | Original - Created for PIR0024
  || 08/10/10 | rhalpai | Corrected SQL matching to load_num. PIR0024
  || 03/16/11 | rhalpai | Remove logic to pass pick adjustments to CMS. PIR0024
  || 01/19/12 | rhalpai | Add logic to pass event parms. PIR10475
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cig_clos_load_sp(
    i_div          IN  VARCHAR2,
    i_llr_dt       IN  DATE,
    i_load_num     IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.CIG_CLOS_LOAD_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Close Load in Cig');
    cig_op_allocate_maint_pk.close_load(i_div, i_llr_dt, i_load_num, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cig_clos_load_sp;

  /*
  ||---------------------------------------------------------------------------
  || LOCK_INV_TBL_SP
  ||  Attempt to lock inventory table.
  ||---------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||---------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||---------------------------------------------------------------------------
  || 08/14/14 | rhalpai | Original for IM-192249
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||---------------------------------------------------------------------------
  */
  PROCEDURE lock_inv_tbl_sp(
    i_div_part  IN      NUMBER,
    o_lock_sw   OUT     VARCHAR2
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT     'Y'
             FROM whsp300c w
            WHERE w.div_part = i_div_part
              AND w.uomc NOT IN('CII', 'CIR', 'CIC')
       FOR UPDATE NOWAIT;

    FETCH l_cv
     INTO o_lock_sw;
  EXCEPTION
    WHEN excp.gx_row_locked THEN
      o_lock_sw := 'N';
      logs.warn('Lock on WHSP300C not available for DivPart: ' || util.to_str(i_div_part));
  END lock_inv_tbl_sp;

  /*
  ||---------------------------------------------------------------------------
  || LOCK_INV_TBL_SP
  ||  Loop until lock is obtained for inventory table.
  ||---------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||---------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||---------------------------------------------------------------------------
  || 08/14/14 | rhalpai | Original for IM-192249
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||---------------------------------------------------------------------------
  */
  PROCEDURE lock_inv_tbl_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_lock_sw  VARCHAR2(1);
  BEGIN
    LOOP
      lock_inv_tbl_sp(i_div_part, l_lock_sw);
      EXIT WHEN l_lock_sw = 'Y';
      DBMS_LOCK.sleep(5);
    END LOOP;
  END lock_inv_tbl_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_INV_SP
  ||  Process Inventory Adjustments
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/27/10 | rhalpai | Original - Created for PIR0024
  || 06/09/10 | rhalpai | Change update of QtyAlloc to be: QtyAlloc - PickQty
  ||                    | IM591758
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  || 01/19/12 | rhalpai | Add logic to pass event parms. PIR10475
  || 01/20/12 | rhalpai | Remove p_use_cig_inv_sw Parm and convert logic to
  ||                    | assume CMS uses its own inventory.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_inv_sp(
    i_div_part     IN  NUMBER,
    i_llr_dt       IN  DATE,
    i_load_num     IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'OP_LOAD_CLOSE_PK.UPD_INV_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_div_id             div_mstr_di1d.div_id%TYPE;
    l_rlse_id            NUMBER;
    l_part_id            NUMBER;
    l_t_items            type_stab;
    l_t_uoms             type_stab;
    l_t_aisls            type_stab;
    l_t_bins             type_stab;
    l_t_lvls             type_stab;
    l_t_pick_qtys        type_ntab;
    l_t_pick_adj_qtys    type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.info('ENTRY', lar_parm);
    l_div_id := div_pk.div_id_fn(i_div_part);
    logs.dbg('Get Release Info');

    OPEN l_cv
     FOR
       SELECT x.rlse_id, x.part_id
         FROM (SELECT DISTINCT r.rlse_id, TO_NUMBER(TO_CHAR(r.rlse_ts, 'DD')) - 1 AS part_id,
                               FIRST_VALUE(rtd.seq) OVER(ORDER BY rl2.create_ts DESC, rtd.seq DESC) AS seq
                          FROM rlse_op1z r, rlse_log_op2z rl, rlse_log_op2z rl2, rlse_typ_dmn_op9z rtd
                         WHERE r.div_part = i_div_part
                           AND r.llr_dt = i_llr_dt
                           AND rl.div_part = r.div_part
                           AND rl.rlse_id = r.rlse_id
                           AND rl.typ_id = 'LOAD'
                           AND rl.val = i_load_num
                           AND rl2.div_part = r.div_part
                           AND rl2.rlse_id = r.rlse_id
                           AND rtd.typ_id = rl2.typ_id) x
        WHERE x.seq = 990;

    FETCH l_cv
     INTO l_rlse_id, l_part_id;

    IF l_cv%FOUND THEN
      logs.dbg('Get Non-Cig Pick Qtys');

      SELECT   e.iteme, e.uome, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl, SUM(b.pckqtb) AS pick_qty,
               SUM(b.alcqtb - b.pckqtb) AS pick_adj_qty
      BULK COLLECT INTO l_t_items, l_t_uoms, l_t_aisls, l_t_bins, l_t_lvls, l_t_pick_qtys,
               l_t_pick_adj_qtys
          FROM tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i, sawp505e e, load_depart_op1f ld, ordp100a a,
               ordp120b b
         WHERE op2t.div_part = i_div_part
           AND op2t.rlse_id = l_rlse_id
           AND op2t.part_id = l_part_id
           AND op2t.tran_typ = 11
           AND op2o.div_part = op2t.div_part
           AND op2o.tran_id = op2t.tran_id
           AND op2o.part_id = l_part_id
           AND op2i.div_part = op2t.div_part
           AND op2i.tran_id = op2t.tran_id
           AND op2i.part_id = l_part_id
           AND op2i.inv_zone = '~'
           AND e.catite = LPAD(op2i.catlg_num, 6, '0')
           AND ld.div_part = i_div_part
           AND ld.llr_dt = i_llr_dt
           AND ld.load_num = i_load_num
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.ordnoa = op2o.ord_num
           AND NOT EXISTS(SELECT 1
                            FROM sub_prcs_ord_src s
                           WHERE s.div_part = a.div_part
                             AND s.prcs_id = 'ALLOCATE'
                             AND s.prcs_sbtyp_cd = 'BZI'
                             AND s.ord_src = a.ipdtsa)
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.ordnob = op2o.ord_num
           AND b.lineb = op2o.ord_ln
           AND b.statb = 'R'
           AND b.sllumb NOT IN('CII', 'CIR', 'CIC')
      GROUP BY e.iteme, e.uome, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl;

      IF l_t_items.COUNT > 0 THEN
        logs.dbg('Upd Non-Cig Inventory');
        ------------------------------------------------------
        -- qty on hand = qty on hand - pick qty
        -- qty alloc   = qty alloc - pick qty
        -- qty avail   = qty avail + (qty alloc - pick qty)
        -- qty rsv     = qty rsv - pick qty
        ------------------------------------------------------
        FORALL i IN l_t_items.FIRST .. l_t_items.LAST
          UPDATE whsp300c w
             SET w.qohc = w.qohc - l_t_pick_qtys(i),
                 w.qalc = w.qalc - l_t_pick_qtys(i)
           WHERE w.div_part = i_div_part
             AND w.itemc = l_t_items(i)
             AND w.uomc = l_t_uoms(i)
             AND w.zonec = l_div_id
             AND w.aislc = l_t_aisls(i)
             AND w.binc = l_t_bins(i)
             AND w.levlc = l_t_lvls(i);
      END IF;   -- l_t_items.COUNT > 0
    END IF;   -- l_cv%FOUND

    logs.dbg('Close Load in Cig');
    cig_clos_load_sp(l_div_id, i_llr_dt, i_load_num, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_TRANS_SP
  ||  Process Allocation Transactions for Load Close.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP. PIR3593
  || 08/11/08 | rhalpai | Reformatted cursor. PIR6364
  || 02/11/10 | rhalpai | Changed Update Inventory section to exclude setting
  ||                    | value for qty available (qavc). PIR8377
  || 04/27/10 | rhalpai | Moved inventory update logic before transaction
  ||                    | cursor. PIR0024
  || 08/29/11 | rhalpai | Convert to use new transaction tables. Removed call
  ||                    | to MOVE_TRANS_TO_HIST_SP. PIR7990
  || 01/19/12 | rhalpai | Add logic to pass event parms. PIR10475
  || 07/10/12 | rhalpai | Remove unused column, SHPQTB. PIR11038
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  ||                    | Remove p_use_cig_inv_sw Parm.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 08/14/14 | rhalpai | Change logic to obtain lock on inventory table while
  ||                    | preventing deadlock. IM-192249
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_trans_sp(
    i_div_part     IN  NUMBER,
    i_llr_dt       IN  DATE,
    i_load_num     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.PRCS_TRANS_SP';
    lar_parm             logs.tar_parm;
    l_inv_qty            PLS_INTEGER   := 0;
    l_ttl_qty            PLS_INTEGER   := 0;
    l_ttl_alc_qty        PLS_INTEGER   := 0;

    CURSOR l_cur_trans(
      b_div_part  NUMBER,
      b_llr_dt    DATE,
      b_load_num  VARCHAR2
    ) IS
      SELECT op2o.ord_num, op2o.ord_ln, b.pckqtb AS pck_qty, b.alcqtb AS alc_qty, op2i.qty AS inv_qty,
             (SELECT NVL(MAX('Y'), 'N')
                FROM tran_stamp_op2c op2c
               WHERE op2c.div_part = op2i.div_part
                 AND op2c.tran_id = op2i.tran_id
                 AND op2c.stamp_item = op2i.catlg_num) AS stamp_sw,
             DECODE(s.ord_src, NULL, 'N', 'Y') AS spcl_ord
        FROM load_depart_op1f ld, ordp100a a, ordp120b b, tran_ord_op2o op2o, tran_item_op2i op2i, sawp505e e,
             sub_prcs_ord_src s
       WHERE ld.div_part = b_div_part
         AND ld.llr_dt = b_llr_dt
         AND ld.load_num = b_load_num
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.excptn_sw = 'N'
         AND b.statb = 'R'
         AND op2o.div_part = b.div_part
         AND op2o.ord_num = b.ordnob
         AND op2o.ord_ln = b.lineb
         AND op2i.div_part = op2o.div_part
         AND op2i.tran_id = op2o.tran_id
         AND e.catite = LPAD(op2i.catlg_num, 6, '0')
         AND s.div_part(+) = a.div_part
         AND s.prcs_id(+) = 'ALLOCATE'
         AND s.prcs_sbtyp_cd(+) = 'BZI'
         AND s.ord_src(+) = a.ipdtsa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Lock WHSP300C');
    lock_inv_tbl_sp(i_div_part);
    logs.dbg('Upd Inventory');
    upd_inv_sp(i_div_part, i_llr_dt, i_load_num, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
    FOR l_r_trans IN l_cur_trans(i_div_part, i_llr_dt, i_load_num) LOOP
      -------------------------------------------------------------------------
      -- inv_qty is used to handle post stamps.
      -- The cursor will contain 2 entries per order line for post stamp items,
      -- one for the cig item and one for the stamp.
      -- The inv_qty column conatins 1 for items and 10 for stamps.
      -------------------------------------------------------------------------
      IF l_r_trans.alc_qty > 0 THEN
        l_inv_qty :=(l_r_trans.inv_qty / l_r_trans.alc_qty);
      ELSE
        l_inv_qty := 0;
      END IF;   -- l_r_trans.alc_qty > 0

      l_ttl_qty := l_r_trans.pck_qty * l_inv_qty;
      l_ttl_alc_qty := l_r_trans.alc_qty * l_inv_qty;

      -- adjust protected inventory tables for any outs
      -- skip for special orders (i.e. cross-dock)
      IF (    l_r_trans.pck_qty < l_r_trans.alc_qty
          AND l_r_trans.spcl_ord = 'N') THEN
        -- Number of items picked is less that allocated quantity.
        -- So, there were some outs. Adjust protected inventory for outs..
        logs.dbg('Upd Protected Inventory for Outs');
        op_protected_inventory_pk.upd_prtctd_inv_log_sp(i_div_part,
                                                        op_protected_inventory_pk.g_c_pick,
                                                        l_r_trans.ord_num,
                                                        l_r_trans.ord_ln
                                                       );
      END IF;   -- l_r_trans.pck_qty < l_r_trans.alc_qty AND l_r_trans.spcl_ord = 'N'

      -- only update order line for 1st entry found for that order
      IF l_r_trans.stamp_sw = 'N' THEN
        logs.dbg('Upd Order Line');

        UPDATE ordp120b b
           SET b.statb = 'A'
         WHERE b.div_part = i_div_part
           AND b.ordnob = l_r_trans.ord_num
           AND b.lineb = l_r_trans.ord_ln;
      END IF;   -- l_r_trans.stamp_sw = 'N'
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prcs_trans_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_STATS_SP
  ||   Update order header and detail lines to "Shipped" status.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP. PIR3593
  || 04/27/10 | rhalpai | Changed logic to update all order headers and details
  ||                    | for Div/LLR/Load from allocated (R) status to
  ||                    | shipped (A) status. PIR0024
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_stats_sp(
    i_div_part  IN  NUMBER,
    i_llr_dt    IN  DATE,
    i_load_num  IN  VARCHAR2
  ) IS
  BEGIN
    -- Update Order Details Without Warehouse Entries
    UPDATE ordp120b b
       SET b.statb = 'A'
     WHERE b.div_part = i_div_part
       AND b.statb = 'R'
       AND b.ordnob IN(SELECT a.ordnoa
                         FROM load_depart_op1f ld, ordp100a a
                        WHERE ld.div_part = i_div_part
                          AND ld.llr_dt = i_llr_dt
                          AND ld.load_num = i_load_num
                          AND a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.stata = 'R');

    -- Update Order Headers
    UPDATE ordp100a a
       SET a.stata = 'A'
     WHERE a.stata = 'R'
       AND a.div_part = i_div_part
       AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                  FROM load_depart_op1f ld
                                 WHERE ld.div_part = i_div_part
                                   AND ld.llr_dt = i_llr_dt
                                   AND ld.load_num = i_load_num);
  END upd_ord_stats_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOCK_LOAD_SP
  ||   Lock Load/Tote Category table MCLP370C.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/06 | rhalpai | Original
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP.
  ||                    | Removed dependency for mclp370c.load_status = 'R' and
  ||                    | converted it to include new LOAD_CLOS_CNTRL_BC2C table.
  ||                    | PIR3593
  || 04/27/10 | rhalpai | Moved TO_RENDATE_DT function call from within cursor
  ||                    | to constant that is referenced by cursor. PIR0024
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE lock_load_sp(
    i_div_part  IN  NUMBER,
    i_llr_dt    IN  DATE,
    i_load_num  IN  VARCHAR2
  ) IS
    l_c_llr_num  CONSTANT PLS_INTEGER   := TRUNC(i_llr_dt) - DATE '1900-02-28';
    l_cv                  SYS_REFCURSOR;
  BEGIN
    -- Lock Loads
    OPEN l_cv
     FOR
       SELECT     lc.ROWID, mc.ROWID
             FROM load_clos_cntrl_bc2c lc, mclp370c mc
            WHERE mc.div_part = i_div_part
              AND mc.loadc = lc.load_num
              AND mc.llr_date = l_c_llr_num
              AND lc.div_part = mc.div_part
              AND lc.llr_dt = i_llr_dt
              AND lc.load_num = i_load_num
       FOR UPDATE NOWAIT;
  END lock_load_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_LOAD_STAT_SP
  ||  Update load status to "Shipped" on Load Close Control and Load/Tote Count
  ||  tables.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original PIR3593
  || 04/27/10 | rhalpai | Moved TO_RENDATE_DT function call from within cursor
  ||                    | to constant that is referenced by cursor. PIR0024
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_load_stat_sp(
    i_div_part  IN  NUMBER,
    i_llr_dt    IN  DATE,
    i_load_num  IN  VARCHAR2,
    i_err_sw    IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_sysdate  CONSTANT DATE        := SYSDATE;
    l_c_llr_num  CONSTANT PLS_INTEGER := TRUNC(i_llr_dt) - DATE '1900-02-28';
  BEGIN
    UPDATE load_clos_cntrl_bc2c lc
       SET lc.load_status = DECODE(i_err_sw, NULL, 'A', 'E')
     WHERE lc.div_part = i_div_part
       AND lc.llr_dt = i_llr_dt
       AND lc.load_num = i_load_num;

    UPDATE mclp370c c
       SET c.user_id = DECODE(i_err_sw, NULL, NULL, 'Error'),
           c.load_status = DECODE(i_err_sw, NULL, 'A', c.load_status),
           c.last_ts_chg = l_c_sysdate
     WHERE c.div_part = i_div_part
       AND c.loadc = i_load_num
       AND c.llr_date = l_c_llr_num;

    COMMIT;
  END upd_load_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || CLOSE_LOAD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/23/03 | rhalpai | Original
  || 02/03/03 | rhalpai | Changed to include departure in QOPRC21
  || 01/13/04 | rhalpai | Added logic to support WAWA.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status parm.
  ||                    | Removed status parm from call to
  ||                    | op_protected_inventory_pk.upd_prtctd_inv_log_sp.
  ||                    | Changed section 'Update Order Headers' to use index
  ||                    | column, swhsb, on WHSP120B instead of dswhsb.
  || 08/12/05 | rhalpai | Added logic to section "Build Line-Out Extract (QOPRC17)"
  ||                    | to bypass "Bulk Outs" for non-"Pre-Post" customers.
  ||                    | PIR2608
  ||                    | Changed cursor LINE_OUT_EXTRACT_CUR to use the
  ||                    | not-ship-reason-code for the out code and default to
  ||                    | a line-out when the the not-ship-reason-code is null.
  ||                    | PIR179
  || 03/02/06 | rhalpai | Replaced existing logic with calls to smaller code
  ||                    | modules and removed the commit which was releasing
  ||                    | the load lock.
  || 04/05/06 | rhalpai | Changed to lock and commit for each load. IM218063
  ||                    | Added logic to call CIG_SHIP_ALLOCATION_SP when
  ||                    | processing the last load for a non-testbill release.
  || 04/21/05 | rhalpai | Added logic to insert Bundle Dist Cust Overrides when
  ||                    | one or more items but not all items of a Bundle Dist
  ||                    | have been pick-adjusted to zero. PIR2545
  || 12/05/06 | rhalpai | Added QOPRC28 Billing Container Id Change Extract.
  ||                    | PIR3209
  || 12/06/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and added p_create_ts
  ||                    | in-parm to replace v_sysdate variable using current
  ||                    | time and replaced call to UPDATE_MCLP_SP with call to
  ||                    | UPD_LOAD_STAT_SP to set load status to "Shipped".
  ||                    | PIR3593
  || 04/27/10 | rhalpai | Removed call to CIG_SHIP_WHEN_LAST_LOAD_SP. PIR0024
  || 01/19/12 | rhalpai | Add logic to pass event parms. PIR10475
  || 01/20/12 | rhalpai | Remove p_use_cig_inv_sw Parm.
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE close_load_sp(
    i_div_part     IN  NUMBER,
    i_llr_dt       IN  DATE,
    i_load_num     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_create_ts    IN  DATE,
    i_tbill_sw     IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.CLOSE_LOAD_SP';
    lar_parm             logs.tar_parm;
    l_cnt_08             PLS_INTEGER   := 0;
    l_cnt_17             PLS_INTEGER   := 0;
    l_cnt_28             PLS_INTEGER   := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.add_parm(lar_parm, 'TBillSw', i_tbill_sw);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Lock Load for Processing');
    lock_load_sp(i_div_part, i_llr_dt, i_load_num);

    IF i_tbill_sw = g_c_non_tbill_load THEN
      logs.dbg('Add Bundle Dist Cust Overrides for Pick Adjustments');
      -----------------------------------------------------------------------
      -- Add entry to Bundle Dist Cust Override table when one or more items
      -- but not all items are pick-adjusted to zero. This will allow these
      -- pick-adjusted orders to bill as regular distributions instead of
      -- "all-or-nothing" bundle distributions when they are sent back down
      -- from the mainframe after billing.
      -----------------------------------------------------------------------
      ins_bundl_dist_cust_sp(i_div_part, i_llr_dt, i_load_num);
    END IF;   -- i_tbill_sw = g_c_non_tbill_load

    logs.dbg('Build Tote Count Extract (QOPRC08)');
    ins_qoprc08_sp(i_div_part, i_llr_dt, i_load_num, i_tbill_sw, i_create_ts, l_cnt_08);
    logs.dbg('Build Line-Out Extract (QOPRC17)');
    ins_qoprc17_sp(i_div_part, i_llr_dt, i_load_num, i_tbill_sw, i_create_ts, l_cnt_17);
    logs.dbg('Build Billing Container Id Change Extract (QOPRC28)');
    ins_qoprc28_sp(i_div_part, i_llr_dt, i_load_num, i_tbill_sw, i_create_ts, l_cnt_28);
    logs.dbg('Build Load Close Information (QOPRC21)');
    ins_qoprc21_sp(i_div_part, i_llr_dt, i_load_num, i_user_id, i_tbill_sw, i_create_ts, l_cnt_08, l_cnt_17, l_cnt_28);
    logs.dbg('Process Allocation Transactions');
    prcs_trans_sp(i_div_part, i_llr_dt, i_load_num, i_user_id, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
    logs.dbg('Upd Orders to Shipped Status');
    -- Update Order Details Without Transaction Entries and Update Order Headers
    upd_ord_stats_sp(i_div_part, i_llr_dt, i_load_num);
    logs.dbg('Upd Load Status');
    -- reset user_id column value to null
    -- (also, this will automatically reset load_status to shipped 'A')
    upd_load_stat_sp(i_div_part, i_llr_dt, i_load_num, NULL);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_row_locked THEN
      logs.warn('RESOURCE_BUSY_NOWAIT occurred', lar_parm);
    WHEN OTHERS THEN
      ROLLBACK;
      upd_load_stat_sp(i_div_part, i_llr_dt, i_load_num, 'Error');
      logs.err(lar_parm);
  END close_load_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALL_TBILL_LOADS_CLOSED_FN
  ||  Indicate whether all Test-Bill Loads have been closed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 08/31/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and converted it to use
  ||                    | new LOAD_CLOS_CNTRL_BC2C table. PIR3593
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION all_tbill_loads_closed_fn(
    i_div_part  IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_found_sw  VARCHAR2(1) := 'N';
  BEGIN
    BEGIN
      SELECT 'Y'
        INTO l_found_sw
        FROM load_clos_cntrl_bc2c lc
       WHERE lc.div_part = i_div_part
         AND lc.load_status = 'R'
         AND lc.test_bil_load_sw = 'Y'
         AND ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_found_sw := 'N';
    END;

    RETURN(l_found_sw = 'N');
  END all_tbill_loads_closed_fn;

  /*
  ||----------------------------------------------------------------------------
  || TAGGED_LOADS_FN
  ||  Return collection of tagged loads.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 04/05/06 | rhalpai | Removed logic for locking of all tagged loads.
  || 12/05/06 | rhalpai | Converted it to use new LOAD_CLOS_CNTRL_BC2C table.
  ||                    | PIR3209
  || 12/06/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and converted it to use
  ||                    | new LOAD_CLOS_CNTRL_BC2C table. PIR3593
  || 04/27/10 | rhalpai | Changed from Open Cursor/Fetch Bulk Collect Into to
  ||                    | just a Select Bulk Collect Into since the Bulk Collect
  ||                    | will eliminate the NoDataFound error and initialize
  ||                    | the return collection variable. PIR0024
  || 10/14/17 | rhalpai | Change to use div_part input parm.
  ||                    | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION tagged_loads_fn(
    i_div_part  IN  NUMBER,
    i_tbill_sw  IN  VARCHAR2
  )
    RETURN g_tt_tagged IS
    l_t_tagged          g_tt_tagged;
    l_acs_load_clos_sw  VARCHAR2(1);
  BEGIN
    l_acs_load_clos_sw := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_acs_load_close);

    SELECT lc.load_num,
           lc.llr_dt
    BULK COLLECT INTO l_t_tagged
      FROM load_clos_cntrl_bc2c lc
     WHERE lc.div_part = i_div_part
       AND lc.load_status = 'T'
       AND lc.test_bil_load_sw = i_tbill_sw
       AND (   i_tbill_sw = 'Y'
            OR lc.acs_load_clos_sw = l_acs_load_clos_sw);

    RETURN(l_t_tagged);
  END tagged_loads_fn;

  /*
  ||----------------------------------------------------------------------------
  || MQ_PUT_SP
  ||  Send open messages for msgID on MCLANE_MQ_PUT table to MQ for routing to
  ||  mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original PIR3593
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_put_sp(
    i_div        IN  VARCHAR2,
    i_msg_id     IN  VARCHAR2,
    i_create_ts  IN  DATE
  ) IS
    l_rc               PLS_INTEGER;
    l_e_mq_put_failed  EXCEPTION;

    FUNCTION msg_exists_fn
      RETURN BOOLEAN IS
      l_cv         SYS_REFCURSOR;
      l_exists_sw  VARCHAR2(1);
    BEGIN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM div_mstr_di1d d, mclane_mq_put p
          WHERE d.div_id = i_div
            AND p.div_part = d.div_part
            AND p.mq_msg_id = i_msg_id
            AND p.create_ts = i_create_ts
            AND p.mq_msg_status = 'OPN';

      FETCH l_cv
       INTO l_exists_sw;

      RETURN(l_exists_sw IS NOT NULL);
    END msg_exists_fn;

    FUNCTION is_test_db_fn
      RETURN BOOLEAN IS
      l_test_db_sw  VARCHAR2(1);
    BEGIN
      l_test_db_sw :=(CASE SUBSTR(SYS.database_name, 1, 1)
                        WHEN 'T' THEN 'Y'
                        ELSE 'N'
                      END);
      RETURN(l_test_db_sw = 'Y');
    END is_test_db_fn;
  BEGIN
    IF msg_exists_fn THEN
      op_mq_message_pk.mq_put_sp(i_msg_id, i_div, NULL, l_rc);

      IF (    l_rc <> 0
          AND NOT is_test_db_fn) THEN
        RAISE l_e_mq_put_failed;
      END IF;   -- l_rc <> 0 AND NOT is_test_db_fn
    END IF;   -- msg_exists_fn
  END mq_put_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_TAGGED_LOADS_SP
  ||  Process Load Close for Tagged Loads
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/06 | rhalpai | Replaced PROCESS_TBILLS_SP and PROCESS_NON_TBILLS_SP
  ||                    | with common logic here.
  || 12/05/06 | rhalpai | Added QOPRC28 Billing Container Id Change Extract.
  ||                    | PIR3209
  || 12/06/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and added logic to
  ||                    | put the MQ msgs on the MQ queues. PIR3593
  || 04/27/10 | rhalpai | Changed logic to set UseCigInvSw and pass as parm
  ||                    | when closing each load within loop. PIR0024
  || 01/19/12 | rhalpai | Add logic to pass event parms. PIR10475
  || 01/20/12 | rhalpai | Remove logic referencing CIG_USE_INVENTORY Parm.
  || 10/14/17 | rhalpai | Change to pass div_part in calls to
  ||                    | TAGGED_LOADS_FN, CLOSE_LOAD_SP,
  ||                    | ALL_TBILL_LOADS_CLOSED_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_tagged_loads_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_tbill_sw     IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.PRCS_TAGGED_LOADS_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_t_tagged              g_tt_tagged;
    l_idx                   PLS_INTEGER;
    l_is_tagged_found       BOOLEAN;
    l_c_create_ts  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'TBillSw', i_tbill_sw);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Collection of Tagged Loads');
    l_t_tagged := tagged_loads_fn(l_div_part, i_tbill_sw);
    l_idx := l_t_tagged.FIRST;
    l_is_tagged_found :=(l_idx IS NOT NULL);
    WHILE l_idx IS NOT NULL LOOP
      logs.dbg('Close Load');
      close_load_sp(l_div_part,
                    l_t_tagged(l_idx).llr_dt,
                    l_t_tagged(l_idx).load_num,
                    i_user_id,
                    l_c_create_ts,
                    i_tbill_sw,
                    i_evnt_que_id,
                    i_cycl_id,
                    i_cycl_dfn_id
                   );
      l_idx := l_t_tagged.NEXT(l_idx);
    END LOOP;

    IF i_tbill_sw = g_c_non_tbill_load THEN
      logs.dbg('Send QOPRC08 MQ Messages');
      mq_put_sp(i_div, 'QOPRC08', l_c_create_ts);
      logs.dbg('Send QOPRC17 MQ Messages');
      mq_put_sp(i_div, 'QOPRC17', l_c_create_ts);
      logs.dbg('Send QOPRC28 MQ Message');
      mq_put_sp(i_div, 'QOPRC28', l_c_create_ts);
      logs.dbg('Send QOPRC21 MQ Message');
      mq_put_sp(i_div, 'QOPRC21', l_c_create_ts);
    ELSIF     i_tbill_sw = g_c_tbill_load
          AND l_is_tagged_found
          AND all_tbill_loads_closed_fn(l_div_part) THEN
      logs.dbg('Send QTPRC08 MQ Messages');
      mq_put_sp(i_div, 'QTPRC08', l_c_create_ts);
      logs.dbg('Send QTPRC17 MQ Messages');
      mq_put_sp(i_div, 'QTPRC17', l_c_create_ts);
      logs.dbg('Send QTPRC28 MQ Message');
      mq_put_sp(i_div, 'QTPRC28', l_c_create_ts);
      logs.dbg('Send QTPRC21 MQ Message');
      mq_put_sp(i_div, 'QTPRC21', l_c_create_ts);
      logs.dbg('Call Backout for TestBill');
      op_backout_llr_pk.backout_sp(i_div);
    END IF;   -- i_tbill_sw = g_c_non_tbill_load

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END prcs_tagged_loads_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || LOC_TYP_LIST_FN
  ||   Build a cursor of location types.
  ||
  ||  LocTyp: XDOCK, Local (non-XDOCK), ALL (both)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/09 | rhalpai | Original. PIR7342
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION loc_typ_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.LOC_TYP_LIST_FN';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_xdock_pick_compl_sw  VARCHAR2(1);
    l_cv                   SYS_REFCURSOR;
    l_t_vals               type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_xdock_pick_compl_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_xdock_pick_compl);

    IF l_xdock_pick_compl_sw = 'Y' THEN
      l_t_vals := type_stab('ALL', 'LOCAL', 'XDOCK');
    ELSE
      l_t_vals := type_stab('LOCAL');
    END IF;   -- l_xdock_pick_compl_sw = 'Y'

    OPEN l_cv
     FOR
       SELECT   t.column_value
           FROM TABLE(CAST(l_t_vals AS type_stab)) t
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END loc_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FN
  ||   Build a cursor of loads that are in "Released" (R) status.
  ||
  ||  LocTyp: XDOCK, Local (non-XDOCK), ALL (both)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3593
  || 08/11/08 | rhalpai | Changed cursor to check test_bil_load_sw for 'Y'
  ||                    | instead of IS NULL in Sel column. Removed existence
  ||                    | check for order details in bill status. PIR6364
  || 08/03/09 | rhalpai | Add logic to selection column in cursor to also
  ||                    | require PICK_COMPL_SW to match XDOCK_PICK_COMPL
  ||                    | for non-TestBills. Add LocationType parm for selection
  ||                    | of XDOCK,LOCAL,ALL. PIR7342
  || 12/02/09 | rhalpai | Convert identification of XDOCK Loads from using
  ||                    | parms for load ranges to parms for non-contiguous
  ||                    | loads. PIR7342
  || 11/19/10 | rhalpai | Remove Care Package logic. PIR5152
  || 09/10/12 | rhalpai | Change logic to include Catchweight complete switch
  ||                    | and disable selection when Catchweight not complete.
  ||                    | PIR10251
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FN,
  ||                    | OP_PARMS_PK.IDX_VALS_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_loc_typ  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module    CONSTANT typ.t_maxfqnm             := 'OP_LOAD_CLOSE_PK.LOAD_LIST_FN';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_loc_typ              VARCHAR2(5);
    l_t_xdock_loads        type_stab;
    l_t_parms              op_types_pk.tt_varchars_v;
    l_acs_load_clos_sw     VARCHAR2(1);
    l_cubing_of_totes_sw   VARCHAR2(1);
    l_xdock_pick_compl_sw  VARCHAR2(1);
    l_cv                   SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LocTyp', i_loc_typ);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_loc_typ := NVL(SUBSTR(i_loc_typ, 1, 5), 'ALL');
    logs.dbg('Get Parms');
    l_t_xdock_loads := op_parms_pk.vals_fn(l_div_part, op_const_pk.prm_xdock_load);
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                         op_const_pk.prm_acs_load_close
                                         || ','
                                         || op_const_pk.prm_cubing_of_totes
                                         || ','
                                         || op_const_pk.prm_xdock_pick_compl
                                        );
    l_acs_load_clos_sw := l_t_parms(op_const_pk.prm_acs_load_close);
    l_cubing_of_totes_sw := l_t_parms(op_const_pk.prm_cubing_of_totes);
    l_xdock_pick_compl_sw := NVL(l_t_parms(op_const_pk.prm_xdock_pick_compl), 'N');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   (CASE
                   WHEN(    x.status IN('Billed', 'Billed w/Open')
                        AND x.test_bil_load_sw = 'Y') THEN 'Y'
                   WHEN(    x.status IN('Billed', 'Billed w/Open')
                        AND x.acs_compl = 'Y'
                        AND x.dspstn_err_sw = 'N'
                        AND x.pick_compl = 'Y'
                        AND x.cwt_compl_sw = 'Y'
                       ) THEN 'Y'
                   ELSE 'N'
                 END
                ) AS sel,
                x.load_num, x.destc, x.llr_date, x.status, x.test_bil_load_sw, x.acs_compl, x.pct_dscrpncy,
                x.dspstn_err_sw, x.has_cntnr_trckng_cust, x.pick_compl, x.cwt_compl_sw
           FROM (SELECT lc.load_num, l.destc, TO_CHAR(lc.llr_dt, 'YYYY-MM-DD') AS llr_date,
                        (SELECT load_stat_udf(lc.div_part, lc.llr_dt, lc.load_num)
                           FROM DUAL) AS status, lc.test_bil_load_sw,
                        DECODE(l_acs_load_clos_sw, 'N', 'Y', lc.acs_load_clos_sw, 'Y', 'N') AS acs_compl,
                        (CASE
                           WHEN(   lc.pick_compl_sw = l_xdock_pick_compl_sw
                                OR lc.load_num NOT IN(SELECT t.column_value
                                                        FROM TABLE(CAST(l_t_xdock_loads AS type_stab)) t)
                               ) THEN 'Y'
                           ELSE 'N'
                         END
                        ) AS pick_compl,
                        lc.cwt_compl_sw, lc.pct_dscrpncy, lc.dspstn_err_sw,
                        (CASE
                           WHEN(    l_cubing_of_totes_sw = 'Y'
                                AND EXISTS(SELECT 1
                                             FROM mclp370c mc, sysp200c sc, mclp100a ma
                                            WHERE mc.div_part = lc.div_part
                                              AND mc.llr_date = lc.llr_dt - DATE '1900-02-28'
                                              AND mc.loadc = lc.load_num
                                              AND sc.div_part = mc.div_part
                                              AND sc.acnoc = mc.custc
                                              AND ma.div_part = sc.div_part
                                              AND ma.cstgpa = sc.retgpc
                                              AND ma.cntnr_trckg_sw = 'Y')
                               ) THEN 'Y'
                           ELSE 'N'
                         END
                        ) AS has_cntnr_trckng_cust
                   FROM load_clos_cntrl_bc2c lc, mclp120c l
                  WHERE l.div_part = l_div_part
                    AND l.loadc = lc.load_num
                    AND lc.div_part = l.div_part
                    AND (   l_loc_typ = 'ALL'
                         OR (    l_loc_typ = 'LOCAL'
                             AND (   l_xdock_pick_compl_sw = 'N'
                                  OR lc.load_num NOT IN(SELECT t.column_value
                                                          FROM TABLE(CAST(l_t_xdock_loads AS type_stab)) t)
                                 )
                            )
                         OR (    l_loc_typ = 'XDOCK'
                             AND lc.load_num IN(SELECT t.column_value
                                                  FROM TABLE(CAST(l_t_xdock_loads AS type_stab)) t))
                        )
                    AND lc.load_status <> 'A') x
       ORDER BY sel DESC, load_num, llr_date;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ACS_FORCE_LOADS_SP
  ||   Get Loads that are eligible for ACS Force Load process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/03/05 | SNAGABH | Initial creation. PIR# 1672/1566.
  || 03/02/06 | rhalpai | Replaced ACS parm SELECT statement with call to
  ||                    | common function.
  || 12/06/06 | rhalpai | Added dspstn_err_sw and has_cntnr_trckng_cust
  ||                    | indicator to cursor.  PIR3209
  || 12/07/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and added CPO status
  ||                    | parm. PIR3593
  || 11/19/10 | rhalpai | Remove Care Package logic. PIR5152
  || 09/10/12 | rhalpai | Change logic to include Catchweight not complete.
  ||                    | PIR10251
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.IDX_VALS_FN. PIR15427
  || 06/12/19 | rhalpai | Change cursor to allow selection when CwtComplSw=Y and
  ||                    | DspstnErrSw=N (previously blocked when AcsLoadClosSw=N
  ||                    | and allowed CwtComplSw=N). Move msg that indicates a
  ||                    | Container Tracking Cust exists on load to new msg
  ||                    | column in cursor. PIR18852
  || 12/12/22 | rhalpai | Add logic to exclude ECOM Load Ranges from selection. PIR21755
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_acs_force_loads_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR,
    o_msg  OUT     VARCHAR2
  ) IS
    l_c_module            CONSTANT typ.t_maxfqnm             := 'OP_LOAD_CLOSE_PK.GET_ACS_FORCE_LOADS_SP';
    lar_parm                       logs.tar_parm;
    l_div_part                     NUMBER;
    l_t_parms                      op_types_pk.tt_varchars_v;
    l_cubing_of_totes_sw           VARCHAR2(1);
    l_acs_load_clos_sw             VARCHAR2(1);
    l_c_cwt_compl_msg     CONSTANT VARCHAR2(100)             := 'Load has Catchweight that has not been completed.';
    l_c_cntnr_trckng_msg  CONSTANT VARCHAR2(100)             := 'Load has Container Tracking Customers.';
    l_c_dspstn_err_msg    CONSTANT VARCHAR2(100)             := 'Load has a disposition error.';
    l_c_rstr_load_range_msg  CONSTANT VARCHAR2(100)             := 'Load within restricted load range.';
    l_c_help_desk_msg     CONSTANT VARCHAR2(100)          := 'Please contact Help Desk if ACS force close is required.';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                         op_const_pk.prm_cubing_of_totes || ',' || op_const_pk.prm_acs_load_close
                                        );
    -- Get the final ACS flag status needed to be able to Close Loads.
    l_cubing_of_totes_sw := l_t_parms(op_const_pk.prm_cubing_of_totes);
    -- Get the final ACS flag status needed to be able to Close Loads.
    l_acs_load_clos_sw := NVL(l_t_parms(op_const_pk.prm_acs_load_close), 'N');

    IF l_acs_load_clos_sw <> 'Y' THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'Division not set up for ACS Load Close.';
    END IF;   --  l_acs_load_clos_sw <> 'Y'

    ------------------------------------------------------------------------
    -- Build a cursor that returns loads that are in Released (R) status and
    -- none of the Order Detail lines are in processing statuses (P, X, T)
    -- A value of 'N' in v_acs_status indicates that the division does not
    -- require ACS processing of loads.
    ------------------------------------------------------------------------
    logs.dbg('Open Cursor');

    OPEN o_cur
     FOR
       SELECT   (CASE
                   WHEN(    x.cwt_compl_sw = 'Y'
                        AND x.dspstn_err_sw = 'N'
                        AND x.rstr_load_range_sw = 'N') THEN 'Y'
                   ELSE op_const_pk.msg_typ_err
                        || op_const_pk.field_delimiter
                        || DECODE(x.cwt_compl_sw, 'N', l_c_cwt_compl_msg)
                        ||(CASE
                             WHEN(    x.cwt_compl_sw = 'N'
                                  AND x.dspstn_err_sw = 'Y') THEN cnst.newline_char
                           END)
                        || DECODE(x.dspstn_err_sw, 'Y', l_c_dspstn_err_msg)
                        || DECODE(x.rstr_load_range_sw, 'Y', l_c_rstr_load_range_msg)
                        || cnst.newline_char
                        || l_c_help_desk_msg
                 END
                ) AS sel,
                x.load_num, x.destc, x.llr_dt, x.acs_load_clos_sw, x.cwt_compl_sw,
                (CASE
                   WHEN x.has_cntnr_trckng_cust = 'Y' THEN op_const_pk.msg_typ_warn
                                                           || op_const_pk.field_delimiter
                                                           || l_c_cntnr_trckng_msg
                 END
                ) AS msg
           FROM (SELECT lc.load_num, l.destc, TO_CHAR(lc.llr_dt, 'YYYY-MM-DD') AS llr_dt, lc.dspstn_err_sw,
                        lc.acs_load_clos_sw, lc.cwt_compl_sw,
                        (CASE
                           WHEN(    l_cubing_of_totes_sw = 'Y'
                                AND EXISTS(SELECT 1
                                             FROM mclp370c mc, sysp200c sc, mclp100a ma
                                            WHERE mc.div_part = lc.div_part
                                              AND mc.llr_date = lc.llr_dt - DATE '1900-02-28'
                                              AND mc.loadc = lc.load_num
                                              AND sc.div_part = mc.div_part
                                              AND sc.acnoc = mc.custc
                                              AND ma.div_part = sc.div_part
                                              AND ma.cstgpa = sc.retgpc
                                              AND ma.cntnr_trckg_sw = 'Y')
                               ) THEN 'Y'
                           ELSE 'N'
                         END
                        ) AS has_cntnr_trckng_cust,
                        (CASE
                           WHEN EXISTS(
                                 SELECT 1
                                   FROM (SELECT DISTINCT SUBSTR
                                                           (FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                                                            1,
                                                            4
                                                           ) AS min_load,
                                                         SUBSTR
                                                           (FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                                                            -4
                                                           ) AS max_load
                                                    FROM appl_sys_parm_ap1s p
                                                   WHERE p.parm_id LIKE 'LOAD_RANGE%'
                                                     AND p.div_part IN(0, l_div_part)) r
                                  WHERE lc.load_num BETWEEN r.min_load AND r.max_load) THEN 'Y'
                           ELSE 'N'
                         END
                        ) AS rstr_load_range_sw
                   FROM load_clos_cntrl_bc2c lc, mclp120c l
                  WHERE lc.div_part = l_div_part
                    AND lc.load_status = 'R'
                    AND lc.test_bil_load_sw = 'N'
                    AND (   (    l_acs_load_clos_sw = 'Y'
                             AND lc.acs_load_clos_sw = 'N')
                         OR lc.cwt_compl_sw = 'N')
                    AND NOT EXISTS(SELECT 1
                                     FROM load_depart_op1f ld, ordp100a a, ordp120b b
                                    WHERE ld.div_part = lc.div_part
                                      AND ld.llr_dt = lc.llr_dt
                                      AND ld.load_num = lc.load_num
                                      AND a.div_part = ld.div_part
                                      AND a.load_depart_sid = ld.load_depart_sid
                                      AND b.div_part = a.div_part
                                      AND b.ordnob = a.ordnoa
                                      AND b.statb IN('P', 'X', 'T'))
                    AND l.div_part = lc.div_part
                    AND l.loadc = lc.load_num) x
       ORDER BY sel, load_num, llr_dt;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_acs_force_loads_sp;

  /*
  ||----------------------------------------------------------------------------
  || FORCE_ACS_CLOSE_SP
  ||   Sets the ACS load close switch to enable ACS loads to be closed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/03/05 | SNAGABH | Initial creation. PIR# 1672/1566.
  || 03/02/06 | rhalpai | Replaced calls to EMPTY_TEMP_TBL_SP and ADD_TO_TEMP_SP
  ||                    | and use of temp table with common function for parsing
  ||                    | comma-delimited strings.
  || 04/03/06 | rhalpai | Changed logic to parse load list to collection and
  ||                    | then use a "FORALL objects in collection" update.
  ||                    | This fixed problem where SO received "ORA-22905:
  ||                    | cannot access rows from a non-nested table item".
  ||                    | IM217964
  || 12/06/06 | rhalpai | Added logic to prevent updates for loads with
  ||                    | discrepancy errors or container-tracking customers
  ||                    | and converted it to use new LOAD_CLOS_CNTRL_BC2C table.
  ||                    | PIR3209
  || 12/22/06 | rhalpai | Moved from OP_SHIP_CONFIRM_SP and changed loads parm
  ||                    | to a parm list which will include groups of load and
  ||                    | LLR date combinations. PIR3593
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 09/10/12 | rhalpai | Add reason parm. Change logic to complete Catchweight.
  ||                    | PIR10251
  || 10/14/17 | rhalpai | Change to use new CIG_EVENT_MGR_PK.CREATE_INSTANCE.
  ||                    | Change to call new OP_PARMS_PK.IDX_VALS_FN. PIR15427
  || 06/12/19 | rhalpai | Change logic to populate new columns in LOAD_LOG_OP3Z
  ||                    | and send email. PIR18852
  || 01/03/20 | rhalpai | Change logic to ensure only one unique row is inserted
  ||                    | to LOAD_RSN_OP3R for same DIV_PART,LOAD_NUM,LLR_DT,TYP,CUST_ID.
  ||                    | SDHD-624356
  ||----------------------------------------------------------------------------
  */
  PROCEDURE force_acs_close_sp(
    i_div          IN  VARCHAR2,
    i_parm_list    IN  VARCHAR2,
    i_rsn          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'UNKNOWN',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                       := 'OP_LOAD_CLOSE_PK.FORCE_ACS_CLOSE_SP';
    lar_parm              logs.tar_parm;
    l_section             VARCHAR2(80);
    l_c_sysdate  CONSTANT DATE                                := SYSDATE;
    l_div_part            NUMBER;
    l_appl_srvr           appl_sys_parm_ap1s.vchar_val%TYPE;
    l_org_id              NUMBER;
    l_evnt_parms          CLOB;
    l_evnt_que_id         NUMBER;
    l_t_parms             op_types_pk.tt_varchars_v;
    l_t_loads             type_stab;
    l_t_llr_dts           type_ntab;
    l_load_list           typ.t_maxvc2;
    l_rsn_id              NUMBER;
    l_cmd                 typ.t_maxvc2;
    l_os_result           typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'Rsn', i_rsn);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Get OrgId');
      l_org_id := cig_organization_pk.get_div_id(i_div);
      l_evnt_parms := '<parameters>'
                      || '<row><sequence>'
                      || 1
                      || '</sequence><value>'
                      || i_div
                      || '</value></row>'
                      || '<row><sequence>'
                      || 2
                      || '</sequence><value>'
                      || i_parm_list
                      || '</value></row>'
                      || '<row><sequence>'
                      || 3
                      || '</sequence><value>'
                      || i_rsn
                      || '</value></row>'
                      || '<row><sequence>'
                      || 4
                      || '</sequence><value>'
                      || i_user_id
                      || '</value></row>'
                      || '</parameters>';
      logs.dbg('Create Event');
      cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                       i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                       i_event_dfn_id         => cig_constants_events_pk.evd_forc_acs,
                                       i_parameters           => l_evnt_parms,
                                       i_div_nm               => i_div,
                                       i_is_script_fw_exec    => 'N',
                                       i_is_complete          => 'Y',
                                       i_pgm_id               => 'PLSQL',
                                       i_user_id              => i_user_id,
                                       o_event_que_id         => l_evnt_que_id
                                      );
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);
      l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                           op_const_pk.prm_acs_load_close || ',' || op_const_pk.prm_cubing_of_totes
                                          );
      l_section := 'Parse Groups of Load/LLR Date Lists to PLSQL-Tables';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
      parse_sp(i_parm_list, l_t_loads, l_t_llr_dts);

      IF l_t_loads IS NOT NULL THEN
        l_section := 'Add Log Entries';
        logs.dbg(l_section);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);

        INSERT INTO load_rsn_op3r
                    (div_part, rsn_id, rsn_descr, user_id, create_ts
                    )
             VALUES (l_div_part, op3r_rsn_id_seq.NEXTVAL, i_rsn, i_user_id, l_c_sysdate
                    )
          RETURNING rsn_id
               INTO l_rsn_id;

        INSERT INTO load_log_op3z
                    (div_part, load_num, llr_dt, typ, rsn_id, cust_id, cust_nm, corp_cd, eta_dt)
        WITH lds AS
               (SELECT ROWNUM AS seq, t.column_value AS load_num
                  FROM TABLE(CAST(l_t_loads AS type_stab)) t),
               llr AS
               (SELECT ROWNUM AS seq, DATE '1900-02-28' + t.column_value AS llr_dt
                  FROM TABLE(CAST(l_t_llr_dts AS type_ntab)) t)
          SELECT   lc.div_part, lc.load_num, lc.llr_dt, 'FRCACS', l_rsn_id, se.cust_id, c.namec, cx.corpb, TRUNC(se.eta_ts)
              FROM lds, llr, load_clos_cntrl_bc2c lc, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx
             WHERE l_t_parms(op_const_pk.prm_acs_load_close) = 'Y'
               AND lds.seq = llr.seq
               AND lc.div_part = l_div_part
               AND lc.load_num = lds.load_num
               AND lc.llr_dt = llr.llr_dt
               AND lc.acs_load_clos_sw = 'N'
               AND ld.div_part = lc.div_part
               AND ld.llr_dt = lc.llr_dt
               AND ld.load_num = lc.load_num
               AND se.div_part = ld.div_part
               AND se.load_depart_sid = ld.load_depart_sid
               AND EXISTS(SELECT 1
                            FROM ordp100a a
                           WHERE a.div_part = se.div_part
                             AND a.load_depart_sid = se.load_depart_sid
                             AND a.custa = se.cust_id
                             AND a.stata = 'R')
               AND EXISTS(SELECT 1
                            FROM mclp370c m
                           WHERE m.div_part = lc.div_part
                             AND m.llr_date = lc.llr_dt - DATE '1900-02-28'
                             AND m.loadc = lc.load_num
                             AND m.custc = se.cust_id)
               AND c.div_part = se.div_part
               AND c.acnoc = se.cust_id
               AND cx.div_part = se.div_part
               AND cx.custb = se.cust_id
          UNION ALL
          SELECT   lc.div_part, lc.load_num, lc.llr_dt, 'FRCCWT', l_rsn_id, se.cust_id, c.namec, cx.corpb, TRUNC(se.eta_ts)
              FROM lds, llr, load_clos_cntrl_bc2c lc, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx
             WHERE lds.seq = llr.seq
               AND lc.div_part = l_div_part
               AND lc.load_num = lds.load_num
               AND lc.llr_dt = llr.llr_dt
               AND lc.cwt_compl_sw = 'N'
               AND ld.div_part = lc.div_part
               AND ld.llr_dt = lc.llr_dt
               AND ld.load_num = lc.load_num
               AND se.div_part = ld.div_part
               AND se.load_depart_sid = ld.load_depart_sid
               AND EXISTS(SELECT 1
                            FROM ordp100a a
                           WHERE a.div_part = se.div_part
                             AND a.load_depart_sid = se.load_depart_sid
                             AND a.custa = se.cust_id
                             AND a.stata = 'R')
               AND EXISTS(SELECT 1
                            FROM mclp370c m
                           WHERE m.div_part = lc.div_part
                             AND m.llr_date = lc.llr_dt - DATE '1900-02-28'
                             AND m.loadc = lc.load_num
                             AND m.custc = se.cust_id)
               AND c.div_part = se.div_part
               AND c.acnoc = se.cust_id
               AND cx.div_part = se.div_part
               AND cx.custb = se.cust_id
          ORDER BY 1, 2, 3, 4, 6;

        l_section := 'Update LOAD_CLOS_CNTRL_BC2C';
        logs.dbg(l_section);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
        FORALL i IN l_t_loads.FIRST .. l_t_loads.LAST
          UPDATE load_clos_cntrl_bc2c lc
             SET lc.acs_load_clos_sw = 'Y',
                 lc.cwt_compl_sw = 'Y'
           WHERE lc.div_part = l_div_part
             AND lc.llr_dt = DATE '1900-02-28' + l_t_llr_dts(i)
             AND lc.load_num = l_t_loads(i)
             AND (   lc.acs_load_clos_sw = 'N'
                  OR lc.cwt_compl_sw = 'N')
             AND lc.dspstn_err_sw = 'N';
        COMMIT;
        logs.dbg('Send Email');
        l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
        l_load_list := util.to_str(l_t_loads);
        logs.dbg('OS Command Setup');
        l_cmd :=
          'mailx -s ''Load Forced Closed'
          || ''' '
          || i_div
          || '_Force_Close@mclaneco.com <<EOM'
          || cnst.newline_char
          || 'User '
          || i_user_id
          || ' used the ACS Force Close screen in OP, force closing the following:'
          || cnst.newline_char
          || 'Loads: '
          || l_load_list
          || cnst.newline_char
          || 'Reason: '
          || i_rsn
          || cnst.newline_char
          || cnst.newline_char
          || 'The driver handheld and EDI documents sent to the customer will not match the actual product in the truck.'
          || cnst.newline_char
          || 'EOM';
        logs.dbg('Process Command' || cnst.newline_char || l_cmd);
        l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
        logs.dbg('OS Result' || cnst.newline_char || l_os_result);
      END IF;   -- l_t_loads IS NOT NULL

      upd_evnt_log_sp(i_evnt_que_id,
                      i_cycl_id,
                      i_cycl_dfn_id,
                      'ACS Force Complete for ' || i_user_id || ' Loads: ' || l_load_list,
                      1
                     );
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END force_acs_close_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCL_OPN_ORD_LNS_SP
  ||   Cancel all remaining open order lines to allow close of load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/10 | rhalpai | Original.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Change logic to include order lines in mainframe
  ||                    | status during cancel. PIR11038
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to pass div_part in calls to
  ||                    | OP_SYSP296A_PK.INS_SP, LOG_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancl_opn_ord_lns_sp(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.CANCL_OPN_ORD_LNS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_t_ord_nums         type_ntab;
    l_t_ord_lns          type_ntab;

    PROCEDURE log_sp(
      i_div_part   IN  NUMBER,
      i_ord_num    IN  NUMBER,
      i_t_ord_lns  IN  type_ntab
    ) IS
    BEGIN
      IF i_t_ord_lns.COUNT > 0 THEN
        FOR i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST LOOP
          op_sysp296a_pk.ins_sp(i_div_part,
                                i_ord_num,
                                i_t_ord_lns(i),
                                i_user_id,
                                'ORDP120B',
                                'STATB',
                                'O',
                                'C',
                                'C',
                                'RCANC7',
                                'LOADCLOSE',
                                'CANCEL OPEN ORDLNS FOR LOADCLOSE'
                               );
        END LOOP;
      END IF;   -- i_t_ord_lns.count > 0
    END log_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Upd Status for Partially Allocated Orders to Allocated');

    UPDATE    ordp100a a
          SET a.stata = 'R'
        WHERE a.stata = 'P'
          AND a.div_part = l_div_part
          AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                     FROM load_depart_op1f ld
                                    WHERE ld.div_part = l_div_part
                                      AND ld.llr_dt = l_llr_dt
                                      AND ld.load_num = i_load_num)
    RETURNING         a.ordnoa
    BULK COLLECT INTO l_t_ord_nums;

    IF l_t_ord_nums.COUNT > 0 THEN
      FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
        logs.dbg('Cancel Open OrdLns in Good Well');

        UPDATE    ordp120b b
              SET b.statb = 'C'
            WHERE b.div_part = l_div_part
              AND b.ordnob = l_t_ord_nums(i)
              AND b.statb IN('O', 'I')
        RETURNING         b.lineb
        BULK COLLECT INTO l_t_ord_lns;

        logs.dbg('Log Cancels');
        log_sp(l_div_part, l_t_ord_nums(i), l_t_ord_lns);
      END LOOP;
    END IF;   -- l_t_ord_nums.COUNT > 0

    logs.dbg('Cancel Open Orders in Bad Well');

    UPDATE    ordp100a a
          SET a.stata = 'C'
        WHERE a.stata = 'O'
          AND a.div_part = l_div_part
          AND a.excptn_sw = 'Y'
          AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                     FROM load_depart_op1f ld
                                    WHERE ld.div_part = l_div_part
                                      AND ld.llr_dt = l_llr_dt
                                      AND ld.load_num = i_load_num)
    RETURNING         a.ordnoa
    BULK COLLECT INTO l_t_ord_nums;

    IF l_t_ord_nums.COUNT > 0 THEN
      FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
        logs.dbg('Cancel ALL Open OrdLns in Bad Well');

        UPDATE    ordp120b b
              SET b.statb = 'C'
            WHERE b.div_part = l_div_part
              AND b.ordnob = l_t_ord_nums(i)
              AND b.statb IN('O', 'I')
              AND b.excptn_sw = 'Y'
        RETURNING         b.lineb
        BULK COLLECT INTO l_t_ord_lns;

        logs.dbg('Log ALL Cancels from Bad Well');
        log_sp(l_div_part, l_t_ord_nums(i), l_t_ord_lns);
      END LOOP;
    END IF;   -- l_t_ord_nums.COUNT > 0

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cancl_opn_ord_lns_sp;

  /*
  ||----------------------------------------------------------------------------
  || START_LOAD_CLOSE_SP
  ||  Start the load close process to run in the background.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original PIR3593
  || 05/21/08 | rhalpai | Changed to execute /local/prodcode/bin/XXOPLoadClose.sub
  ||                    | with Div/UserID/OracleSID parms via oscmd_fn. PIR5819
  || 05/13/13 | rhalpai | Change logic to call xxopLoadClose.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  || 07/01/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE start_load_close_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_LOAD_CLOSE_PK.START_LOAD_CLOSE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_sid                VARCHAR2(10);
    l_cmd                typ.t_maxvc2;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_sid := SYS_CONTEXT('USERENV', 'DB_NAME');
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_cmd := '/local/prodcode/bin/xxopLoadClose.sub "'
             || i_div
             || '" "'
             || i_user_id
             || '" "'
             || l_sid
             || '"';
    logs.dbg('Run Control-M Sub Script in Background');
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END start_load_close_sp;

  /*
  ||----------------------------------------------------------------------------
  || TAG_FOR_CLOSE_SP
  ||   Tag Load/LLR Date selections for close.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3593
  || 05/19/08 | rhalpai | Changed update of LOAD_CLOS_CNTRL_BC2C to check
  ||                    | test_bil_load_sw for Y instead of NULL. IM412076
  || 08/03/09 | rhalpai | Add logic to require PICK_COMPL_SW to match
  ||                    | XDOCK_PICK_COMPL_xx when updating status of
  ||                    | LOAD_CLOS_CNTRL_BC2C for non-TestBills. PIR7342
  || 12/02/09 | rhalpai | Convert identification of XDOCK Loads from using
  ||                    | parms for load ranges to parms for non-contiguous
  ||                    | loads. PIR7342
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 04/09/13 | rhalpai | Move ProcessControl from TagForClose to CloseTaggedLoads.
  ||                    | PIR11923
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FN,
  ||                    | OP_PARMS_PK.IDX_VALS_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE tag_for_close_sp(
    i_div        IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    i_parm_list  IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm             := 'OP_LOAD_CLOSE_PK.TAG_FOR_CLOSE_SP';
    lar_parm                  logs.tar_parm;
    l_c_sysdate      CONSTANT DATE                      := SYSDATE;
    l_c_tag_user_id  CONSTANT VARCHAR2(8)               := 'SHPCNFRM';
    l_div_part                NUMBER;
    l_t_xdock_loads           type_stab;
    l_t_parms                 op_types_pk.tt_varchars_v;
    l_acs_load_clos_sw        VARCHAR2(1);
    l_xdock_pick_compl_sw     VARCHAR2(1);
    l_t_loads                 type_stab;
    l_t_llr_dts               type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xdock_loads := op_parms_pk.vals_fn(l_div_part, op_const_pk.prm_xdock_load);
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                         op_const_pk.prm_acs_load_close || ',' || op_const_pk.prm_xdock_pick_compl
                                        );
    l_acs_load_clos_sw := l_t_parms(op_const_pk.prm_acs_load_close);
    l_xdock_pick_compl_sw := NVL(l_t_parms(op_const_pk.prm_xdock_pick_compl), 'N');
    logs.dbg('Parse Groups of Load/LLR Date Lists to PLSQL-Tables');
    parse_sp(i_parm_list, l_t_loads, l_t_llr_dts);

    IF l_t_loads IS NOT NULL THEN
      ----------------------------------------------------------------------
      -- Update/tag load for close only if given load, division and llr_date
      -- combination has not been tagged for close or already closed.
      -- This prevents cases where multiple users try to close the same load
      -- simultaneously or from stale data on ship confirm screen.
      ----------------------------------------------------------------------
      logs.dbg('Tag Loads for Close');
      FORALL i IN l_t_loads.FIRST .. l_t_loads.LAST
        UPDATE load_clos_cntrl_bc2c lc
           SET lc.load_status = 'T'
         WHERE lc.div_part = l_div_part
           AND lc.llr_dt = DATE '1900-02-28' + l_t_llr_dts(i)
           AND lc.load_num = l_t_loads(i)
           AND (   lc.test_bil_load_sw = 'Y'
                OR (    lc.acs_load_clos_sw = l_acs_load_clos_sw
                    AND lc.dspstn_err_sw = 'N'
                    AND (   lc.pick_compl_sw = l_xdock_pick_compl_sw
                         OR lc.load_num NOT IN(SELECT t.column_value
                                                 FROM TABLE(CAST(l_t_xdock_loads AS type_stab)) t)
                        )
                   )
               )
           AND lc.load_status = 'R';

      IF SQL%FOUND THEN
        FORALL i IN l_t_loads.FIRST .. l_t_loads.LAST
          UPDATE mclp370c mc
             SET mc.user_id = l_c_tag_user_id,
                 mc.last_ts_chg = l_c_sysdate
           WHERE mc.div_part = l_div_part
             AND mc.loadc = l_t_loads(i)
             AND mc.llr_date = l_t_llr_dts(i)
             AND EXISTS(SELECT 1
                          FROM load_clos_cntrl_bc2c lc
                         WHERE lc.div_part = l_div_part
                           AND lc.llr_dt = DATE '1900-02-28' - l_t_llr_dts(i)
                           AND lc.load_num = l_t_loads(i)
                           AND lc.load_status = 'T');
        COMMIT;
        logs.dbg('Start Load Close Process in Background');
        start_load_close_sp(i_div, i_user_id);
      ELSE
        o_err_msg := 'Nothing found to close.'
                     || cnst.newline_char
                     || 'Load/LLR Date selection may have already been processed by another user.';
      END IF;   -- SQL%FOUND
    END IF;   -- l_t_loads IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tag_for_close_sp;

  /*
  ||----------------------------------------------------------------------------
  || CLOSE_TAGGED_LOADS_SP
  ||  Close all tagged loads.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original PIR3593
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 01/19/12 | rhalpai | Add logic to pass event parms. PIR10475
  || 04/09/13 | rhalpai | Move ProcessControl from TagForClose to CloseTaggedLoads.
  ||                    | PIR11923
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.CREATE_INSTANCE.
  ||                    | Change to use constants package OP_CONST_PK.
  ||                    | Change to pass div_part in calls to OP_ANALYZE_BY_PARM_SP,
  ||                    | OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP,
  ||                    | OP_PROCESS_CONTROL_PK.RESTRICTED_MSG_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE close_tagged_loads_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CLOSE_PK.CLOSE_TAGGED_LOADS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;
    l_load_list          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Get OrgId');
      l_org_id := cig_organization_pk.get_div_id(i_div);
      l_evnt_parms := '<parameters>'
                      || '<row><sequence>'
                      || 1
                      || '</sequence><value>'
                      || i_div
                      || '</value></row>'
                      || '<row><sequence>'
                      || 2
                      || '</sequence><value>'
                      || i_user_id
                      || '</value></row>'
                      || '</parameters>';
      logs.dbg('Create Event');
      cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                       i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                       i_event_dfn_id         => cig_constants_events_pk.evd_load_close,
                                       i_parameters           => l_evnt_parms,
                                       i_div_nm               => i_div,
                                       i_is_script_fw_exec    => 'N',
                                       i_is_complete          => 'Y',
                                       i_pgm_id               => 'PLSQL',
                                       i_user_id              => i_user_id,
                                       o_event_que_id         => l_evnt_que_id
                                      );
    ELSE
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_load_clos,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.dbg('Get Load List');

      SELECT to_list_fn(CURSOR(SELECT   lc.load_num
                                   FROM load_clos_cntrl_bc2c lc
                                  WHERE lc.div_part = l_div_part
                                    AND lc.load_status = 'T'
                               ORDER BY 1))
        INTO l_load_list
        FROM DUAL;

      IF l_load_list IS NOT NULL THEN
        logs.dbg('Analyze Tables');
        op_analyze_by_parm_sp(l_div_part, 'ANLYZ_LOADCLOS_' || i_div);
        upd_evnt_log_sp(i_evnt_que_id,
                        i_cycl_id,
                        i_cycl_dfn_id,
                        'Load Close Initiated for ' || i_user_id || ' Loads: ' || l_load_list
                       );
        logs.dbg('Process TestBill Loads');
        prcs_tagged_loads_sp(i_div, i_user_id, g_c_tbill_load, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
        logs.dbg('Process Non-Testbill Loads');
        prcs_tagged_loads_sp(i_div, i_user_id, g_c_non_tbill_load, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
        upd_evnt_log_sp(i_evnt_que_id,
                        i_cycl_id,
                        i_cycl_dfn_id,
                        'Load Close Complete for ' || i_user_id || ' Loads: ' || l_load_list,
                        1
                       );
      END IF;   -- l_load_list IS NOT NULL

      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_load_clos,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      upd_evnt_log_sp(i_evnt_que_id,
                      i_cycl_id,
                      i_cycl_dfn_id,
                      op_process_control_pk.restricted_msg_fn(op_const_pk.prcs_load_clos, l_div_part),
                      1
                     );
    WHEN OTHERS THEN
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_load_clos,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END close_tagged_loads_sp;
END op_load_close_pk;
/

