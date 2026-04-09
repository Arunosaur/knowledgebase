CREATE OR REPLACE PACKAGE op_lost_load_pk IS
--  PRAGMA SERIALLY_REUSABLE;
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_lost IS RECORD(
    ord_num        NUMBER,
    is_excptn_hdr  VARCHAR2(1),
    is_hist_hdr    VARCHAR2(1)
  );

  TYPE g_tt_lost IS TABLE OF g_rt_lost;

  TYPE g_cvt_lost IS REF CURSOR
    RETURN g_rt_lost;

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION llr_date_list_fn(
    i_div      IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_llr_dt   IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2,
    i_hist_sw   IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION manifest_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ords_cur_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  NUMBER,
    i_load_num   IN  VARCHAR2,
    i_stop_list  IN  VARCHAR2 DEFAULT NULL,
    i_mfst_list  IN  VARCHAR2 DEFAULT NULL,
    i_hist_sw    IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN g_cvt_lost;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE create_orders_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    i_load_num   IN      VARCHAR2,
    i_stop_list  IN      VARCHAR2,
    i_mfst_list  IN      VARCHAR2,
    i_hist_sw    IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2
  );
END op_lost_load_pk;
/

CREATE OR REPLACE PACKAGE BODY op_lost_load_pk IS
--  PRAGMA SERIALLY_REUSABLE;
/*
||-----------------------------------------------------------------------------
||             C H A N G E     L O G
||-----------------------------------------------------------------------------
|| Date     | USERID  | Changes
||-----------------------------------------------------------------------------
|| 09/17/01 | RHALPAI | Original
|| 04/15/02 | Fei Wu  | 1.Change status to 'O' from 'A' on ORDP100A.
||                    |   Populate SHPCMA and CPOA
||                    | 2.ORDP120B -- shpidb = null, populate RETGPB and SCOMPB
||                    | 3.Populate ORDP140C
|| 01/14/03 | $RSPEC3 | For history orders move header from ordp900A to
||                    | ordp100A/whsp100a.
|| 02/02/04 | RHALPAI | Changed to exclude recreation orders by order sourcd.
||                    | This will allow excluding Wawa (cross-dock) orders and
||                    | distributions. Fixed LLR on order header to be in sync
||                    | with detail on newly created order.
||-----------------------------------------------------------------------------
*/
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || RLSE_LIST_FN
  ||   Build a cursor of Allocation Release info for shipped orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/22/07 | rhalpai | Original. PIR3593
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/15/15 | rhalpai | Change to use common DIV_PK. IM-324418
  ||----------------------------------------------------------------------------
  */
  FUNCTION rlse_list_fn(
    i_div      IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.RLSE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(r.rlse_ts, 'YYYY-MM-DD HH24:MI:SS') AS rlse_ts, TO_CHAR(r.llr_dt, 'YYYY-MM-DD') AS llr_dt,
                r.ord_ln_cnt, r.user_id
           FROM rlse_op1z r
          WHERE r.div_part = l_div_part
            AND r.stat_cd = 'R'
            AND 990 = (SELECT MAX(td.seq)
                         FROM rlse_log_op2z rl, rlse_typ_dmn_op9z td
                        WHERE rl.div_part = l_div_part
                          AND rl.rlse_id = r.rlse_id
                          AND td.typ_id = rl.typ_id)
            AND r.test_bil_cd = '~'
            AND r.ord_ln_cnt > 0
            AND (   EXISTS(SELECT 1
                             FROM load_depart_op1f ld, ordp100a a, ordp120b b
                            WHERE ld.div_part = r.div_part
                              AND ld.llr_dt = r.llr_dt
                              AND a.div_part = ld.div_part
                              AND a.load_depart_sid = ld.load_depart_sid
                              AND a.excptn_sw = 'N'
                              AND b.div_part = a.div_part
                              AND b.ordnob = a.ordnoa
                              AND b.excptn_sw = 'N'
                              AND b.shpidb = TO_CHAR(r.rlse_ts, 'YYYYMMDDHH24MISS')
                              AND b.statb = 'A')
                 OR EXISTS(SELECT 1
                             FROM ordp900a a, ordp920b b
                            WHERE i_hist_sw = 'Y'
                              AND a.div_part = r.div_part
                              AND a.ctofda = r.llr_dt - DATE '1900-02-28'
                              AND a.stata = 'A'
                              AND b.div_part = a.div_part
                              AND b.ordnob = a.ordnoa
                              AND b.statb = 'A'
                              AND b.shpidb = TO_CHAR(r.rlse_ts, 'YYYYMMDDHH24MISS'))
                )
       ORDER BY r.rlse_ts DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rlse_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GOV_CNTL_BACKOUT_SP
  ||  Back Out Government Control
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/07 | rhalpai | Original. PIR3593
  || 10/15/15 | rhalpai | Change input parameter from div to div_part to avoid
  ||                    | lookup. IM-324418
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gov_cntl_backout_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_mfst_list  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.GOV_CNTL_BACKOUT_SP';
    lar_parm             logs.tar_parm;
    l_t_mfst_catgs       type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.info('ENTRY', lar_parm);

    IF i_mfst_list IS NOT NULL THEN
      l_t_mfst_catgs := str.parse_list(i_mfst_list, op_const_pk.field_delimiter);
    END IF;   -- i_mfst_list IS NOT NULL

    logs.dbg('Reset Customer Gov Control Table');

    UPDATE gov_cntl_cust_p640a a
       SET (a.shp_pts, a.tot_pts, a.status) =
             (SELECT a.shp_pts - SUM(log1.shp_pts), a.tot_pts - SUM(log1.tot_pts), DECODE(MIN(log1.status), 0, 0, 1)
                FROM gov_cntl_log_p680a log1, ordp100a oa, ordp120b b
               WHERE log1.div_part = a.div_part
                 AND log1.gov_cntl_id = a.gov_cntl_id
                 AND log1.cust_num = a.cust_num
                 AND log1.prd_beg_ts = a.prd_beg_ts
                 AND oa.div_part = a.div_part
                 AND oa.ordnoa = i_ord_num
                 AND oa.dsorda = 'R'
                 AND b.div_part = log1.div_part
                 AND b.ordnob = log1.ord_num
                 AND b.lineb = log1.ord_ln
                 AND b.statb = 'A'
                 AND b.lineb = FLOOR(b.lineb)
                 AND b.subrcb = 0
                 AND b.alcqtb > 0
                 AND b.ordnob = i_ord_num
                 AND (   i_mfst_list IS NULL
                      OR b.manctb IN(SELECT t.column_value
                                       FROM TABLE(CAST(l_t_mfst_catgs AS type_stab)) t)))
     WHERE a.div_part = i_div_part
       AND EXISTS(SELECT 1
                    FROM gov_cntl_log_p680a log4, ordp100a oa, ordp120b b
                   WHERE log4.div_part = a.div_part
                     AND log4.gov_cntl_id = a.gov_cntl_id
                     AND log4.cust_num = a.cust_num
                     AND log4.prd_beg_ts = a.prd_beg_ts
                     AND oa.div_part = log4.div_part
                     AND oa.ordnoa = i_ord_num
                     AND oa.dsorda = 'R'
                     AND b.div_part = log4.div_part
                     AND b.ordnob = log4.ord_num
                     AND b.lineb = log4.ord_ln
                     AND b.statb = 'A'
                     AND b.lineb = FLOOR(b.lineb)
                     AND b.subrcb = 0
                     AND b.alcqtb > 0
                     AND b.ordnob = i_ord_num
                     AND (   i_mfst_list IS NULL
                          OR b.manctb IN(SELECT t.column_value
                                           FROM TABLE(CAST(l_t_mfst_catgs AS type_stab)) t)));

    -- Remove GC Customer Entries that were created because an existing
    -- period had expired
    logs.dbg('Remove GC Customer Entries');

    DELETE FROM gov_cntl_cust_p640a c
          WHERE c.div_part = i_div_part
            AND c.shp_pts = 0
            AND c.tot_pts = 0
            AND c.status = 1
            AND EXISTS(SELECT 1
                         FROM gov_cntl_cust_p640a c2
                        WHERE c2.div_part = c.div_part
                          AND c2.gov_cntl_id = c.gov_cntl_id
                          AND c2.cust_num = c.cust_num
                          AND c2.prd_beg_ts < c.prd_beg_ts
                          AND c2.status = 1)
            AND EXISTS(SELECT 1
                         FROM gov_cntl_log_p680a log1, ordp100a a, ordp120b b
                        WHERE log1.div_part = c.div_part
                          AND log1.gov_cntl_id = c.gov_cntl_id
                          AND log1.cust_num = c.cust_num
                          AND log1.prd_beg_ts = c.prd_beg_ts
                          AND log1.release_ts = c.prd_beg_ts
                          AND a.div_part = log1.div_part
                          AND a.ordnoa = i_ord_num
                          AND a.dsorda = 'R'
                          AND b.div_part = log1.div_part
                          AND b.ordnob = log1.ord_num
                          AND b.lineb = log1.ord_ln
                          AND b.statb = 'A'
                          AND b.lineb = FLOOR(b.lineb)
                          AND b.subrcb = 0
                          AND b.alcqtb > 0
                          AND b.ordnob = i_ord_num
                          AND (   i_mfst_list IS NULL
                               OR b.manctb IN(SELECT t.column_value
                                                FROM TABLE(CAST(l_t_mfst_catgs AS type_stab)) t)))
            AND NOT EXISTS(SELECT 1
                             FROM gov_cntl_log_p680a log1
                            WHERE log1.div_part = c.div_part
                              AND log1.gov_cntl_id = c.gov_cntl_id
                              AND log1.cust_num = c.cust_num
                              AND log1.prd_beg_ts = c.prd_beg_ts
                              AND log1.release_ts = c.prd_beg_ts
                              AND NOT EXISTS(SELECT 1
                                               FROM gov_cntl_log_p680a log4, ordp100a a, ordp120b b
                                              WHERE log4.div_part = log1.div_part
                                                AND log4.ord_num = log1.ord_num
                                                AND log4.ord_ln = log1.ord_ln
                                                AND a.div_part = log4.div_part
                                                AND a.ordnoa = i_ord_num
                                                AND a.dsorda = 'R'
                                                AND b.div_part = log4.div_part
                                                AND b.ordnob = log4.ord_num
                                                AND b.lineb = log4.ord_ln
                                                AND b.statb = 'A'
                                                AND b.lineb = FLOOR(b.lineb)
                                                AND b.subrcb = 0
                                                AND b.alcqtb > 0
                                                AND b.ordnob = i_ord_num
                                                AND (   i_mfst_list IS NULL
                                                     OR b.manctb IN(SELECT t.column_value
                                                                      FROM TABLE(CAST(l_t_mfst_catgs AS type_stab)) t)
                                                    )));

    logs.dbg('Remove Gov Control Log Entries');

    DELETE FROM gov_cntl_log_p680a lg
          WHERE lg.div_part = i_div_part
            AND lg.ord_num = i_ord_num
            AND lg.ord_ln IN(SELECT b.lineb
                               FROM ordp100a a, ordp120b b
                              WHERE a.div_part = i_div_part
                                AND a.ordnoa = i_ord_num
                                AND a.dsorda = 'R'
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND b.statb = 'A'
                                AND b.lineb = FLOOR(b.lineb)
                                AND b.subrcb = 0
                                AND b.alcqtb > 0
                                AND (   i_mfst_list IS NULL
                                     OR b.manctb IN(SELECT t.column_value
                                                      FROM TABLE(CAST(l_t_mfst_catgs AS type_stab)) t)
                                    ));

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END gov_cntl_backout_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROT_INV_BACKOUT_SP
  ||  Back Out Protected Inventory
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/07 | rhalpai | Original. PIR3593
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 10/15/15 | rhalpai | Add div_part input parameter to remove dependence upon
  ||                    | global context variable. IM-324418
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prot_inv_backout_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_hist_sw    IN  VARCHAR2,
    i_mfst_list  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.PROT_INV_BACKOUT_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;

    CURSOR l_cur_ords(
      b_div_part   NUMBER,
      b_log_typ    VARCHAR2,
      b_ord_num    NUMBER,
      b_hist_sw    VARCHAR2,
      b_mfst_list  VARCHAR2
    ) IS
      SELECT a.prtctd_id, a.ln_num, a.qty
        FROM prtctd_alloc_log_op1a a,
             (SELECT   MAX(a2.tran_id) AS tran_id
                  FROM prtctd_alloc_log_op1a a2
                 WHERE a2.div_part = b_div_part
                   AND a2.stat_cd = b_log_typ
                   AND a2.ord_num = b_ord_num
                   AND a2.ln_num IN(SELECT b.lineb
                                      FROM ordp100a a, ordp120b b
                                     WHERE a.div_part = b_div_part
                                       AND a.ordnoa = b_ord_num
                                       AND a.dsorda = 'R'
                                       AND b.div_part = b_div_part
                                       AND b.ordnob = b_ord_num
                                       AND b_hist_sw = 'N'
                                       AND b.statb = 'A'
                                       AND b.excptn_sw = 'N'
                                       AND b.lineb = FLOOR(b.lineb)
                                       AND b.subrcb = 0
                                       AND b.alcqtb > 0
                                       AND (   b_mfst_list IS NULL
                                            OR b.manctb IN(SELECT t.column_value
                                                             FROM TABLE(CAST(str.parse_list(b_mfst_list, '~') AS type_stab
                                                                            )
                                                                       ) t)
                                           )
                                    UNION ALL
                                    SELECT b.lineb
                                      FROM ordp900a a, ordp920b b
                                     WHERE b_hist_sw = 'Y'
                                       AND a.div_part = b_div_part
                                       AND a.ordnoa = b_ord_num
                                       AND a.dsorda = 'R'
                                       AND b.div_part = b_div_part
                                       AND b.ordnob = b_ord_num
                                       AND b.statb = 'A'
                                       AND b.excptn_sw = 'N'
                                       AND b.lineb = FLOOR(b.lineb)
                                       AND b.subrcb = 0
                                       AND b.alcqtb > 0
                                       AND (   b_mfst_list IS NULL
                                            OR b.manctb IN(SELECT t.column_value
                                                             FROM TABLE(CAST(str.parse_list(b_mfst_list, '~') AS type_stab
                                                                            )
                                                                       ) t)
                                           ))
              GROUP BY a2.ln_num) a3
       WHERE a.div_part = b_div_part
         AND a.tran_id = a3.tran_id;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Process Backout Ship');
    <<backout_ship_loop>>
    FOR l_r_ords IN l_cur_ords(i_div_part, op_protected_inventory_pk.g_c_pick, i_ord_num, i_hist_sw, i_mfst_list) LOOP
      logs.dbg('Backout Ship - Update Protected Inventory');

      UPDATE prtctd_inv_op1i i
         SET i.prtctd_qty =(i.prtctd_qty - l_r_ords.qty),
             i.user_id = op_protected_inventory_pk.g_c_pick,
             i.last_chg_ts = l_c_sysdate
       WHERE i.div_part = i_div_part
         AND i.prtctd_id = l_r_ords.prtctd_id;

      logs.dbg('Backout Ship - Insert Log Record');

      INSERT INTO prtctd_alloc_log_op1a
                  (tran_id, prtctd_id, ord_num, ln_num, qty, last_chg_ts,
                   stat_cd, div_part
                  )
           VALUES (op1a_tran_id_seq.NEXTVAL, l_r_ords.prtctd_id, i_ord_num, l_r_ords.ln_num, l_r_ords.qty, l_c_sysdate,
                   op_protected_inventory_pk.g_c_pick, i_div_part
                  );

      COMMIT;
    END LOOP backout_ship_loop;
    logs.dbg('Process Backout Release');
    <<backout_release_loop>>
    FOR l_r_ords IN l_cur_ords(i_div_part, op_protected_inventory_pk.g_c_rlse, i_ord_num, i_hist_sw, i_mfst_list) LOOP
      logs.dbg('Backout Release - Update Protected Inventory');

      UPDATE prtctd_inv_op1i i
         SET i.prtctd_qty =(i.prtctd_qty + l_r_ords.qty),
             i.user_id = op_protected_inventory_pk.g_c_rlse,
             i.last_chg_ts = l_c_sysdate
       WHERE i.div_part = i_div_part
         AND i.prtctd_id = l_r_ords.prtctd_id;

      logs.dbg('Backout Release - Insert Log Record');

      INSERT INTO prtctd_alloc_log_op1a
                  (tran_id, prtctd_id, ord_num, ln_num, qty, last_chg_ts,
                   stat_cd, div_part
                  )
           VALUES (op1a_tran_id_seq.NEXTVAL, l_r_ords.prtctd_id, i_ord_num, l_r_ords.ln_num, l_r_ords.qty, l_c_sysdate,
                   op_protected_inventory_pk.g_c_rlse, i_div_part
                  );

      COMMIT;
    END LOOP backout_release_loop;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END prot_inv_backout_sp;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTLS_FN
  ||  Return table of order details for Lost Load order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/07 | rhalpai | Original. PIR3593
  || 10/15/15 | rhalpai | Add div_part input parameter to remove dependence upon
  ||                    | global context variable. IM-324418
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtls_fn(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_hist_sw    IN  VARCHAR2,
    i_excptn_sw  IN  VARCHAR2,
    i_mfst_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN csr_orders_pk.g_tt_msg_dtls IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'OP_LOST_LOAD_PK.ORD_DTLS_FN';
    lar_parm             logs.tar_parm;
    l_t_mfsts            type_stab;
    l_t_ord_dtls         csr_orders_pk.g_tt_msg_dtls;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.add_parm(lar_parm, 'ExcptnSw', i_excptn_sw);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Parse Manifest List');
    l_t_mfsts := str.parse_list(i_mfst_list, op_const_pk.field_delimiter);

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Get History Order Detail');

      SELECT   b.orditb AS catlg_num,
               b.itemnb AS cbr_item,
               b.sllumb AS uom,
               b.ordqtb AS ord_qty,
               b.cusitb AS cust_item,
               b.itpasb AS item_pass_area,
               (CASE
                  WHEN b.rtfixb IN('1', 'Y') THEN 'Y'
                  ELSE 'N'
                END) AS hard_rtl_sw,
               b.hdrtab AS rtl_amt,
               b.hdrtmb AS rtl_mult,
               (CASE
                  WHEN b.prfixb IN('1', 'Y') THEN 'Y'
                  ELSE 'N'
                END) AS hard_price_sw,
               b.hdprcb AS price_amt,
               b.orgqtb AS orig_qty,
               (CASE
                  WHEN b.bymaxb IN('1', 'Y') THEN 'Y'
                  ELSE 'N'
                END) AS byp_max_sw,
               b.maxqtb AS max_qty,
               b.qtmulb AS qty_mult,
               NULL AS ord_ln
      BULK COLLECT INTO l_t_ord_dtls
          FROM ordp920b b
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.statb <> 'C'
           AND b.lineb = FLOOR(b.lineb)
           AND (   i_mfst_list IS NULL
                OR b.manctb IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_mfsts AS type_stab)) t))
      ORDER BY b.lineb;
    ELSE
      logs.dbg('Get Order Detail');

      SELECT   b.orditb AS catlg_num,
               b.itemnb AS cbr_item,
               b.sllumb AS uom,
               b.ordqtb AS ord_qty,
               b.cusitb AS cust_item,
               b.itpasb AS item_pass_area,
               (CASE
                  WHEN b.rtfixb IN('1', 'Y') THEN 'Y'
                  ELSE 'N'
                END) AS hard_rtl_sw,
               b.hdrtab AS rtl_amt,
               b.hdrtmb AS rtl_mult,
               (CASE
                  WHEN b.prfixb IN('1', 'Y') THEN 'Y'
                  ELSE 'N'
                END) AS hard_price_sw,
               b.hdprcb AS price_amt,
               b.orgqtb AS orig_qty,
               (CASE
                  WHEN b.bymaxb IN('1', 'Y') THEN 'Y'
                  ELSE 'N'
                END) AS byp_max_sw,
               b.maxqtb AS max_qty,
               b.qtmulb AS qty_mult,
               NULL AS ord_ln
      BULK COLLECT INTO l_t_ord_dtls
          FROM ordp120b b
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.statb <> 'C'
           AND b.excptn_sw = DECODE(i_excptn_sw, 'Y', 'Y', b.excptn_sw)
           AND b.lineb = FLOOR(b.lineb)
           AND (   i_mfst_list IS NULL
                OR b.manctb IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_mfsts AS type_stab)) t))
      ORDER BY b.lineb;
    END IF;   -- i_hist_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_ord_dtls);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_dtls_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_HDR_FN
  ||  Return record of order header for Lost Load order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/07 | rhalpai | Original. PIR3593
  || 11/10/10 | rhalpai | Remove unused column from cursor. PIR5878
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  || 10/15/15 | rhalpai | Add div_part input parameter to remove dependence upon
  ||                    | global context variable. IM-324418
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_fn(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_hist_sw    IN  VARCHAR2,
    i_excptn_sw  IN  VARCHAR2
  )
    RETURN csr_orders_pk.g_rt_msg_hdr IS
    l_c_module  CONSTANT typ.t_maxfqnm              := 'OP_LOST_LOAD_PK.ORD_HDR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_r_ord_hdr          csr_orders_pk.g_rt_msg_hdr;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.add_parm(lar_parm, 'ExcptnSw', i_excptn_sw);
    logs.info('ENTRY', lar_parm);

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open History Order Header Cursor');

      OPEN l_cv
       FOR
         SELECT d.div_part, d.div_id, a.custa, cx.mccusb, a.ldtypa, a.dsorda AS ord_typ, a.ipdtsa, a.telsla, a.cspasa,
                a.hdexpa, a.legrfa, a.cpoa,
                TO_DATE('19000228' || lpad_fn(a.trntma, 6, '0'), 'YYYYMMDDHH24MISS') + a.trndta AS trnsmt_ts,
                (CASE
                   WHEN a.pshipa IN('1', 'Y') THEN 'Y'
                   ELSE 'N'
                 END) AS allw_partl_sw, DATE '1900-02-28' + a.shpja AS shp_dt
           FROM div_mstr_di1d d, ordp900a a, mclp020b cx
          WHERE d.div_part = i_div_part
            AND a.div_part = d.div_part
            AND a.ordnoa = i_ord_num
            AND a.excptn_sw = DECODE(i_excptn_sw, 'Y', 'Y', a.excptn_sw)
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa;
    ELSE
      logs.dbg('Open Order Header Cursor');

      OPEN l_cv
       FOR
         SELECT d.div_part, d.div_id, a.custa, cx.mccusb, a.ldtypa, a.dsorda AS ord_typ, a.ipdtsa, a.telsla, a.cspasa,
                a.hdexpa, a.legrfa, a.cpoa,
                TO_DATE('19000228' || lpad_fn(a.trntma, 6, '0'), 'YYYYMMDDHH24MISS') + a.trndta AS trnsmt_ts,
                (CASE
                   WHEN a.pshipa IN('1', 'Y') THEN 'Y'
                   ELSE 'N'
                 END) AS allw_partl_sw, DATE '1900-02-28' + a.shpja AS shp_dt
           FROM div_mstr_di1d d, ordp100a a, mclp020b cx
          WHERE d.div_part = i_div_part
            AND a.div_part = d.div_part
            AND a.ordnoa = i_ord_num
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa;
    END IF;   -- i_hist_sw = 'Y'

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_r_ord_hdr.div_part, l_r_ord_hdr.div, l_r_ord_hdr.cust_id, l_r_ord_hdr.mcl_cust, l_r_ord_hdr.load_typ,
          l_r_ord_hdr.ord_typ, l_r_ord_hdr.ord_src, l_r_ord_hdr.ser_num, l_r_ord_hdr.cust_pass_area,
          l_r_ord_hdr.hdr_excptn_cd, l_r_ord_hdr.legcy_ref, l_r_ord_hdr.po_num, l_r_ord_hdr.trnsmt_ts,
          l_r_ord_hdr.allw_partl_sw, l_r_ord_hdr.shp_dt;

    IF l_cv%FOUND THEN
      l_r_ord_hdr.conf_num := 'L' || csr_orders_pk.next_conf_num_fn;
    END IF;   -- l_cv%FOUND

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_r_ord_hdr);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_hdr_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || LLR_DATE_LIST_FN
  ||   Build a cursor of LLR Dates for shipped orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/15/07 | rhalpai | Original. PIR3593
  || 07/16/12 | rhalpai | Change logic to improve efficiency for non-history
  ||                    | queries. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/15/15 | rhalpai | Changed to use common DIV_PK. IM-324418
  ||----------------------------------------------------------------------------
  */
  FUNCTION llr_date_list_fn(
    i_div      IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.LLR_DATE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open Cursor with History');

      OPEN l_cv
       FOR
         SELECT   TO_CHAR(x.llr_dt, 'YYYY-MM-DD')
             FROM (SELECT ld.llr_dt AS llr_dt
                     FROM load_depart_op1f ld
                    WHERE ld.div_part = l_div_part
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = ld.div_part
                                    AND a.load_depart_sid = ld.load_depart_sid
                                    AND a.dsorda = 'R'
                                    AND a.stata = 'A')
                   UNION
                   SELECT DATE '1900-02-28' + a.ctofda
                     FROM ordp900a a
                    WHERE a.div_part = l_div_part
                      AND a.dsorda = 'R'
                      AND a.stata = 'A') x
         ORDER BY 1;
    ELSE
      logs.dbg('Open Cursor without History');

      OPEN l_cv
       FOR
         SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt
             FROM load_depart_op1f ld
            WHERE ld.div_part = l_div_part
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.dsorda = 'R'
                            AND a.stata = 'A')
         GROUP BY ld.llr_dt
         ORDER BY 1;
    END IF;   -- i_hist_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END llr_date_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FN
  ||   Build a cursor of loads for shipped orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/15/07 | rhalpai | Original. PIR3593
  || 07/16/12 | rhalpai | Change logic to improve efficiency for non-history
  ||                    | queries. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/15/15 | rhalpai | Changed to use common DIV_PK. IM-324418
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_llr_dt   IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open Cursor with History');

      OPEN l_cv
       FOR
         SELECT   x.load_num, c.destc
             FROM (SELECT ld.load_num
                     FROM load_depart_op1f ld
                    WHERE ld.div_part = l_div_part
                      AND ld.llr_dt = l_llr_dt
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = ld.div_part
                                    AND a.load_depart_sid = ld.load_depart_sid
                                    AND a.dsorda = 'R'
                                    AND a.stata = 'A')
                   UNION
                   SELECT a.orrtea
                     FROM ordp900a a
                    WHERE a.div_part = l_div_part
                      AND a.dsorda = 'R'
                      AND a.stata = 'A'
                      AND a.ctofda = l_llr_dt - DATE '1900-02-28') x,
                  mclp120c c
            WHERE c.div_part(+) = l_div_part
              AND c.loadc(+) = x.load_num
         ORDER BY 1;
    ELSE
      logs.dbg('Open Cursor without History');

      OPEN l_cv
       FOR
         SELECT   ld.load_num, c.destc
             FROM load_depart_op1f ld, mclp120c c
            WHERE ld.div_part = l_div_part
              AND ld.llr_dt = l_llr_dt
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.dsorda = 'R'
                            AND a.stata = 'A')
              AND c.div_part(+) = ld.div_part
              AND c.loadc(+) = ld.load_num
         GROUP BY ld.load_num, c.destc
         ORDER BY 1;
    END IF;   -- i_hist_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_LIST_FN
  ||  Return cursor of stops available for LLR/Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/15/07 | rhalpai | Original. PIR3593
  || 07/16/12 | rhalpai | Change logic to improve efficiency for non-history
  ||                    | queries. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2,
    i_hist_sw   IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.STOP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open Cursor with History');

      OPEN l_cv
       FOR
         SELECT   (CASE
                     WHEN x.stop_num < 10 THEN '0' || x.stop_num
                     ELSE TO_CHAR(x.stop_num)
                   END) AS stop_num, x.mcl_cust, c.namec AS cust_name, c.zipc AS zip
             FROM (SELECT d.div_part, se.stop_num, se.cust_id, cx.mccusb AS mcl_cust
                     FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx
                    WHERE d.div_id = i_div
                      AND ld.div_part = d.div_part
                      AND ld.llr_dt = l_llr_dt
                      AND ld.load_num = i_load_num
                      AND se.div_part = ld.div_part
                      AND se.load_depart_sid = ld.load_depart_sid
                      AND cx.div_part = se.div_part
                      AND cx.custb = se.cust_id
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = se.div_part
                                    AND a.load_depart_sid = se.load_depart_sid
                                    AND a.custa = se.cust_id
                                    AND a.dsorda = 'R'
                                    AND a.stata = 'A')
                   UNION
                   SELECT d.div_part, a.stopsa, a.custa, cx.mccusb
                     FROM div_mstr_di1d d, ordp900a a, mclp020b cx
                    WHERE d.div_id = i_div
                      AND a.div_part = d.div_part
                      AND a.dsorda = 'R'
                      AND a.stata = 'A'
                      AND a.ctofda = l_llr_dt - DATE '1900-02-28'
                      AND a.orrtea = i_load_num
                      AND cx.div_part = a.div_part
                      AND cx.custb = a.custa) x,
                  sysp200c c
            WHERE c.div_part = x.div_part
              AND c.acnoc = x.cust_id
         ORDER BY 1;
    ELSE
      logs.dbg('Open Cursor without History');

      OPEN l_cv
       FOR
         SELECT   (CASE
                     WHEN se.stop_num < 10 THEN '0' || se.stop_num
                     ELSE TO_CHAR(se.stop_num)
                   END) AS stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_name, c.zipc AS zip
             FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, sysp200c c
            WHERE d.div_id = i_div
              AND ld.div_part = d.div_part
              AND ld.llr_dt = l_llr_dt
              AND ld.load_num = i_load_num
              AND se.div_part = ld.div_part
              AND se.load_depart_sid = ld.load_depart_sid
              AND cx.div_part = se.div_part
              AND cx.custb = se.cust_id
              AND c.div_part = se.div_part
              AND c.acnoc = se.cust_id
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = se.div_part
                            AND a.load_depart_sid = se.load_depart_sid
                            AND a.custa = se.cust_id
                            AND a.dsorda = 'R'
                            AND a.stata = 'A')
         GROUP BY se.stop_num, cx.mccusb, c.namec, c.zipc
         ORDER BY 1;
    END IF;   -- p_hist_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || MANIFEST_LIST_FN
  ||  Return cursor of manifest categories.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/15/07 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION manifest_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.MANIFEST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   m.manctc, m.descc
           FROM div_mstr_di1d d, mclp210c m
          WHERE d.div_id = i_div
            AND m.div_part = d.div_part
       ORDER BY m.manctc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END manifest_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORDS_CUR_FN
  ||  Return cursor of orders for Lost Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/07 | rhalpai | Original. PIR3593
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ords_cur_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  NUMBER,
    i_load_num   IN  VARCHAR2,
    i_stop_list  IN  VARCHAR2 DEFAULT NULL,
    i_mfst_list  IN  VARCHAR2 DEFAULT NULL,
    i_hist_sw    IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN g_cvt_lost IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOST_LOAD_PK.ORDS_CUR_FN';
    lar_parm             logs.tar_parm;
    l_c_llr_dt  CONSTANT DATE          := DATE '1900-02-28' + i_llr_dt;
    l_cv_lost            g_cvt_lost;
    l_t_stops            type_ntab;
    l_t_mfsts            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Parse Stop List');
    l_t_stops := num.parse_list(i_stop_list, op_const_pk.field_delimiter);
    logs.dbg('Parse Manifest List');
    l_t_mfsts := str.parse_list(i_mfst_list, op_const_pk.field_delimiter);
    logs.dbg('Open Cursor');

    OPEN l_cv_lost
     FOR
       SELECT a.ordnoa AS ord_num, a.excptn_sw AS is_excptn_hdr, 'N' AS is_hist_hdr
         FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se
        WHERE d.div_id = i_div
          AND ld.div_part = d.div_part
          AND ld.llr_dt = l_c_llr_dt
          AND ld.load_num = i_load_num
          AND a.div_part = ld.div_part
          AND a.load_depart_sid = ld.load_depart_sid
          AND a.stata = 'A'
          AND a.dsorda = 'R'
          AND NOT EXISTS(SELECT 1
                           FROM sub_prcs_ord_src s
                          WHERE s.div_part = a.div_part
                            AND s.prcs_id = 'LOST LOAD'
                            AND s.prcs_sbtyp_cd = 'BLL'
                            AND s.ord_src = a.ipdtsa)
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa
          AND (   i_stop_list IS NULL
               OR se.stop_num IN(SELECT t.column_value
                                   FROM TABLE(CAST(l_t_stops AS type_ntab)) t))
          AND EXISTS(SELECT 1
                       FROM ordp120b b
                      WHERE b.div_part = a.div_part
                        AND b.ordnob = a.ordnoa
                        AND b.lineb = FLOOR(b.lineb)
                        AND (   i_mfst_list IS NULL
                             OR b.manctb IN(SELECT t.column_value
                                              FROM TABLE(CAST(l_t_mfsts AS type_stab)) t)))
       UNION ALL
       SELECT a.ordnoa AS ord_num, a.excptn_sw AS is_excptn_hdr, 'Y' AS is_hist_hdr
         FROM div_mstr_di1d d, ordp900a a
        WHERE UPPER(i_hist_sw) = 'Y'
          AND d.div_id = i_div
          AND a.div_part = d.div_part
          AND a.stata IN('A', 'E')
          AND a.dsorda = 'R'
          AND a.ctofda = i_llr_dt
          AND a.orrtea = i_load_num
          AND (   i_stop_list IS NULL
               OR a.stopsa IN(SELECT t.column_value
                                FROM TABLE(CAST(l_t_stops AS type_ntab)) t))
          AND EXISTS(SELECT 1
                       FROM ordp920b b
                      WHERE b.div_part = a.div_part
                        AND b.ordnob = a.ordnoa
                        AND b.lineb = FLOOR(b.lineb)
                        AND (   i_mfst_list IS NULL
                             OR b.manctb IN(SELECT t.column_value
                                              FROM TABLE(CAST(l_t_mfsts AS type_stab)) t)))
          AND NOT EXISTS(SELECT 1
                           FROM sub_prcs_ord_src s
                          WHERE s.div_part = a.div_part
                            AND s.prcs_id = 'LOST LOAD'
                            AND s.prcs_sbtyp_cd = 'BLL'
                            AND s.ord_src = a.ipdtsa);

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv_lost);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ords_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || CREATE_ORDERS_SP
  ||  Create orders for Lost Load
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/07 | rhalpai | Original. PIR3593
  || 10/15/15 | rhalpai | Add div_part in call to CSR_ORDERS_PK.ORD_COMMENT_FN.
  ||                    | Change calls to GOV_CNTL_BACKOUT_SP, PROT_INV_BACKOUT_SP,
  ||                    | ORD_DTLS_FN, ORD_HDR_FN to pass div_part. IM-324418
  || 01/12/18 | rhalpai | Add logic to remove Process Control upon successful completion.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE create_orders_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    i_load_num   IN      VARCHAR2,
    i_stop_list  IN      VARCHAR2,
    i_mfst_list  IN      VARCHAR2,
    i_hist_sw    IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'OP_LOST_LOAD_PK.CREATE_ORDERS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             PLS_INTEGER;
    l_cv_lost            g_cvt_lost;
    l_t_lost             g_tt_lost                   := g_tt_lost();
    l_idx                PLS_INTEGER;
    l_t_ord_dtls         csr_orders_pk.g_tt_msg_dtls;
    l_r_ord_hdr          csr_orders_pk.g_rt_msg_hdr;
    l_ord_cnt            PLS_INTEGER                 := 0;
    l_ord_comnt          ordp140c.commc%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_lost_load,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Get Orders Cursor');
    l_cv_lost := ords_cur_fn(i_div, l_llr_dt, i_load_num, i_stop_list, i_mfst_list, i_hist_sw);
    logs.dbg('Fetch Orders Cursor');

    FETCH l_cv_lost
    BULK COLLECT INTO l_t_lost;

    l_idx := l_t_lost.FIRST;
    WHILE l_idx IS NOT NULL LOOP
      logs.dbg('Load Order Details Table');
      l_t_ord_dtls := ord_dtls_fn(l_div_part,
                                  l_t_lost(l_idx).ord_num,
                                  l_t_lost(l_idx).is_hist_hdr,
                                  l_t_lost(l_idx).is_excptn_hdr,
                                  i_mfst_list
                                 );

      IF (    l_t_ord_dtls IS NOT NULL
          AND l_t_ord_dtls.COUNT > 0) THEN
        l_ord_cnt := l_ord_cnt + 1;
        logs.dbg('Get Order Header Record');
        l_r_ord_hdr := ord_hdr_fn(l_div_part,
                                  l_t_lost(l_idx).ord_num,
                                  l_t_lost(l_idx).is_hist_hdr,
                                  l_t_lost(l_idx).is_excptn_hdr
                                 );
        logs.dbg('Get Order Comment');
        l_ord_comnt := csr_orders_pk.ord_comment_fn(l_div_part, l_t_lost(l_idx).ord_num, l_t_lost(l_idx).is_hist_hdr);
        logs.dbg('Insert New Order and Send to Mainframe');
        csr_orders_pk.ins_ord_sp(l_r_ord_hdr, l_t_ord_dtls, i_user_id, l_ord_comnt);
        logs.dbg('Log Copied Order');

        INSERT INTO cpy_ord_cp3o
                    (conf_num, orig_ord_num
                    )
             VALUES (l_r_ord_hdr.conf_num, l_t_lost(l_idx).ord_num
                    );

        COMMIT;

        IF l_t_lost(l_idx).is_excptn_hdr = 'N' THEN
          IF l_t_lost(l_idx).is_hist_hdr = 'N' THEN
            logs.dbg('Government Control Backout');
            gov_cntl_backout_sp(l_div_part, l_t_lost(l_idx).ord_num, i_mfst_list);
          END IF;   -- l_t_lost(l_idx).is_hist_hdr = 'N'

          logs.dbg('Protected Inventory Backout');
          prot_inv_backout_sp(l_div_part, l_t_lost(l_idx).ord_num, l_t_lost(l_idx).is_hist_hdr, i_mfst_list);
        END IF;   -- l_t_lost(l_idx).is_excptn_hdr = 'N'
      END IF;   -- l_t_ord_dtls IS NOT NULL AND l_t_ord_dtls.COUNT > 0

      l_idx := l_t_lost.NEXT(l_idx);
    END LOOP;

    IF l_ord_cnt > 0 THEN
      o_msg := op_const_pk.msg_typ_info || op_const_pk.field_delimiter || l_ord_cnt || ' orders recreated.';
    ELSE
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'No orders available to recreate.';
    END IF;   -- l_ord_cnt > 0

    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_lost_load,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_lost_load,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END create_orders_sp;
END op_lost_load_pk;
/

