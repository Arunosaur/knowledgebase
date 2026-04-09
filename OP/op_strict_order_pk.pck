CREATE OR REPLACE PACKAGE op_strict_order_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_has_strict_item_msg  CONSTANT VARCHAR2(250)
    := 'Strict Items exist on order. When resent, it may be split and/or moved'
       || ' to another LLRDate/Load depending on vendor lead times.';
  g_c_recapped_msg         CONSTANT VARCHAR2(250)
    := 'Order contains Strict Items that have been recapped (POs may have been'
       || ' created) so changes may lead to order outs and/or inventory'
       || ' shortages/overages for these time-sensitive products.';
  g_c_miss_po_cutoff_msg   CONSTANT VARCHAR2(250)
    := 'Order contains unrecapped (no PO created) Strict Items that will now'
       || ' miss the PO Cutoff for vendor due to order move. If order is not'
       || ' moved to a future date, the product will not be ordered leading'
       || ' to inventory shortage and order outs.';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION to_date_time_fn(
    i_dt    IN  DATE,
    i_time  IN  NUMBER
  )
    RETURN DATE;

  FUNCTION prev_prod_rcpt_ts_fn(
    i_div          IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER,
    i_llr_ts       IN  DATE
  )
    RETURN DATE;

  FUNCTION nxt_prod_rcpt_ts_fn(
    i_div              IN  VARCHAR2,
    i_cust_id          IN  VARCHAR2,
    i_cbr_vndr_id      IN  NUMBER,
    i_ts               IN  DATE,
    i_run_intrvl_mins  IN  PLS_INTEGER DEFAULT NULL
  )
    RETURN DATE;

  FUNCTION po_cutoff_ts_fn(
    i_div          IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER,
    i_llr_ts       IN  DATE
  )
    RETURN DATE;

  FUNCTION recap_ords_cur_fn(
    i_div              IN  VARCHAR2,
    i_run_ts           IN  DATE,
    i_run_intrvl_mins  IN  PLS_INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION is_strict_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN BOOLEAN;

  FUNCTION is_recapped_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  )
    RETURN BOOLEAN;

  FUNCTION ord_will_miss_po_cutoff_fn(
    i_div_part    IN  NUMBER,
    i_ord_num     IN  NUMBER,
    i_new_llr_ts  IN  DATE
  )
    RETURN BOOLEAN;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE recap_sp(
    i_div     IN  VARCHAR2,
    i_run_ts  IN  DATE DEFAULT SYSDATE
  );

  PROCEDURE bi_rpt_extr_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  );

  PROCEDURE enforc_po_qty_sp(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER,
    i_po_qty   IN  NUMBER
  );
END op_strict_order_pk;
/

CREATE OR REPLACE PACKAGE BODY op_strict_order_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || UPD_STRCT_ORD_SP
  ||  Update RecapTS/LLRAtRecap/ProductReceiptTS on Strict Order table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/10/07 | rhalpai | Original - Created for PIR5002
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_strct_ord_sp(
    i_new_stat          IN  VARCHAR2,
    i_new_recap_ts      IN  DATE,
    i_new_llr_at_recap  IN  DATE,
    i_new_prod_rcpt_ts  IN  DATE,
    i_div_part          IN  NUMBER,
    i_ord_num           IN  NUMBER,
    i_ord_ln            IN  NUMBER,
    i_recap_qty         IN  NUMBER,
    i_cbr_vndr_id       IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm := 'OP_STRICT_ORDER_PK.UPD_STRCT_ORD_SP';
    lar_parm                    logs.tar_parm;
    l_c_unrecapped_ts  CONSTANT DATE          := TO_DATE('29990101', 'YYYYMMDD');
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'NewStat', i_new_stat);
    logs.add_parm(lar_parm, 'NewRecapTs', i_new_recap_ts);
    logs.add_parm(lar_parm, 'NewLLRAtRecap', i_new_llr_at_recap);
    logs.add_parm(lar_parm, 'NewProdRcptTs', i_new_prod_rcpt_ts);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'RecapQty', i_recap_qty);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_ord_ln IS NOT NULL THEN
      UPDATE strct_ord_op1o so
         SET so.recap_ts = i_new_recap_ts,
             so.stat = i_new_stat,
             so.llr_at_recap = i_new_llr_at_recap,
             so.prod_rcpt_ts = i_new_prod_rcpt_ts,
             so.recap_qty = i_recap_qty
       WHERE so.div_part = i_div_part
         AND so.ord_num = i_ord_num
         AND so.ord_ln = i_ord_ln
         AND so.recap_ts = l_c_unrecapped_ts
         AND so.stat = 'URC';
    ELSE
      UPDATE strct_ord_op1o so
         SET so.recap_ts = i_new_recap_ts,
             so.stat = i_new_stat,
             so.llr_at_recap = i_new_llr_at_recap,
             so.prod_rcpt_ts = i_new_prod_rcpt_ts,
             so.recap_qty = 0
       WHERE so.div_part = i_div_part
         AND so.ord_num = i_ord_num
         AND so.cbr_vndr_id = i_cbr_vndr_id
         AND so.recap_ts = l_c_unrecapped_ts
         AND so.stat = 'URC';
    END IF;   -- i_ord_ln IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END upd_strct_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCT_ORD_DTL_EXTR_SP
  ||  Create and send Strict Order Detail extract.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/10/15 | rhalpai | Original - Created for PIR15456
  || 01/02/24 | rhalpai | Add logic to exclude ENFORC_PO_QTY_SW = Y. PC-9546
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strct_ord_dtl_extr_sp(
    i_div       IN  VARCHAR2,
    i_recap_ts  IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_STRICT_ORDER_PK.STRCT_ORD_DTL_EXTR_SP';
    lar_parm             logs.tar_parm;
    l_file_nm            VARCHAR2(80);
    l_t_rpt_lns          typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RecapTs', i_recap_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || '_STRICT_ORD_DTL_' || TO_CHAR(i_recap_ts, 'YYYYMMDDHH24MISS') || '.CSV';
    logs.dbg('Build Extract');

    SELECT   rpad_fn(v.vndr_nm, 11, '_')
             || '_STO,'
             || se.cust_id
             || ','
             || rpad_fn(cx.storeb, 6)
             || ','
             || rpad_fn(c.namec, 40)
             || ','
             || ld.load_num
             || ','
             || LPAD(se.stop_num, 2, '0')
             || ','
             || e.catite
             || ','
             || LPAD(SUM(b.ordqtb), 5)
             || ','
             || lpad_fn(e.shppke, 4, '0')
             || ','
             || rpad_fn(e.sizee, 8)
             || ','
             || rpad_fn(e.ctdsce, 25)
             || ','
             || lpad_fn(e.upce, 14, '0')
    BULK COLLECT INTO l_t_rpt_lns
        FROM div_mstr_di1d d, vndr_mstr_op1v v, strct_ord_op1o so, ordp120b b, sawp505e e, ordp100a a, stop_eta_op1g se,
             load_depart_op1f ld, sysp200c c, mclp020b cx
       WHERE d.div_id = i_div
         AND v.div_part = d.div_part
         AND v.cust_lvl_dtl_sw = 'Y'
         AND v.enforc_po_qty_sw = 'N'
         AND so.div_part = v.div_part
         AND so.cbr_vndr_id = v.cbr_vndr_id
         AND so.stat = 'RCP'
         AND so.recap_ts = i_recap_ts
         AND b.div_part = so.div_part
         AND b.ordnob = so.ord_num
         AND b.lineb = so.ord_ln
         AND b.excptn_sw = 'N'
         AND b.statb = 'O'
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND a.div_part = so.div_part
         AND a.ordnoa = so.ord_num
         AND a.excptn_sw = 'N'
         AND se.div_part = a.div_part
         AND se.load_depart_sid = a.load_depart_sid
         AND se.cust_id = a.custa
         AND ld.div_part = a.div_part
         AND ld.load_depart_sid = a.load_depart_sid
         AND c.div_part = a.div_part
         AND c.acnoc = a.custa
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa
    GROUP BY v.vndr_nm, se.cust_id, cx.storeb, c.namec, ld.load_num, se.stop_num, e.catite, e.shppke, e.sizee, e.ctdsce,
             e.upce
    ORDER BY 1;

    IF l_t_rpt_lns.COUNT > 0 THEN
      logs.dbg('Write');
      write_sp(l_t_rpt_lns, l_file_nm);
      logs.dbg('FTE File');
      fte_sp(i_div,
             l_file_nm,
             NULL,
             NULL,
             'STRCTORD',
             'Y',
             'PFTEAG01',
             'B2BPRDAGT',
             '/GVOB/STRICTORD/' || i_div || '_STRICT_ORD_DTL.csv'
            );
    END IF;   -- l_t_rpt_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END strct_ord_dtl_extr_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCT_ORDLN_EXTR_SP
  ||  Create and send Strict Order Line extract (for CH Robinson/Talor Farms indicated by ENFORC_PO_QTY_SW = Y).
  ||  Model from STRCT_ORD_DTL_EXTR_SP and add DivId,CbrVndrId,McCust,OrdNum,OrdLn,RecapTS for ENFORC_PO_QTY_SW = Y.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/02/24 | rhalpai | Original - Created for PC-9546
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strct_ordln_extr_sp(
    i_div       IN  VARCHAR2,
    i_recap_ts  IN  DATE
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_STRICT_ORDER_PK.STRCT_ORDLN_EXTR_SP';
    lar_parm               logs.tar_parm;
    l_c_rmt_file  CONSTANT VARCHAR2(20)   := 'STRICT.ORDLN.RECAP';
    l_file_nm              VARCHAR2(80);
    l_t_rpt_lns            typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RecapTs', i_recap_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || '_STRICT_ORDLN_RECAP_' || TO_CHAR(i_recap_ts, 'YYYYMMDDHH24MISS') || '.CSV';
    logs.dbg('Build Extract');

    SELECT   rpad_fn(v.vndr_nm, 40)
             || se.cust_id
             || rpad_fn(cx.storeb, 6)
             || rpad_fn(c.namec, 40)
             || ld.load_num
             || LPAD(se.stop_num, 2, '0')
             || e.catite
             || LPAD(b.ordqtb, 5, '0')
             || lpad_fn(e.shppke, 4, '0')
             || rpad_fn(e.sizee, 8)
             || rpad_fn(e.ctdsce, 25)
             || lpad_fn(e.upce, 14, '0')
             || LPAD(d.div_part, 3, '0')
             || LPAD(v.cbr_vndr_id, 10, '0')
             || cx.mccusb
             || lpad_fn(b.ordnob, 11, '0')
             || TO_CHAR(b.lineb, 'FM0000000.00')
             || b.manctb
             || lpad_fn(b.totctb, 3, '0')
             || TO_CHAR(so.recap_ts, 'YYYY-MM-DD-HH24.MI.SS')
    BULK COLLECT INTO l_t_rpt_lns
        FROM div_mstr_di1d d, vndr_mstr_op1v v, strct_ord_op1o so, ordp120b b, sawp505e e, ordp100a a, stop_eta_op1g se,
             load_depart_op1f ld, sysp200c c, mclp020b cx
       WHERE d.div_id = i_div
         AND v.div_part = d.div_part
         AND v.enforc_po_qty_sw = 'Y'
         AND so.div_part = v.div_part
         AND so.cbr_vndr_id = v.cbr_vndr_id
         AND so.stat = 'RCP'
         AND so.recap_ts = i_recap_ts
         AND b.div_part = so.div_part
         AND b.ordnob = so.ord_num
         AND b.lineb = so.ord_ln
         AND b.excptn_sw = 'N'
         AND b.statb = 'O'
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND a.div_part = so.div_part
         AND a.ordnoa = so.ord_num
         AND a.excptn_sw = 'N'
         AND se.div_part = a.div_part
         AND se.load_depart_sid = a.load_depart_sid
         AND se.cust_id = a.custa
         AND ld.div_part = a.div_part
         AND ld.load_depart_sid = a.load_depart_sid
         AND c.div_part = a.div_part
         AND c.acnoc = a.custa
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa
    ORDER BY 1;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_file_nm);
    logs.dbg('FTP to mainframe');
    op_ftp_sp(i_div, l_file_nm, l_c_rmt_file);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END strct_ordln_extr_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || TO_DATE_TIME_FN
  ||  Return date containing date and time given date and numeric time.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/10/07 | rhalpai | Original - Created for PIR5002
  ||----------------------------------------------------------------------------
  */
  FUNCTION to_date_time_fn(
    i_dt    IN  DATE,
    i_time  IN  NUMBER
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STRICT_ORDER_PK.TO_DATE_TIME_FN';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'Dt', i_dt);
    logs.add_parm(lar_parm, 'Time', i_time);
    RETURN(TO_DATE(TO_CHAR(i_dt, 'YYYYMMDD') || LPAD(i_time, 4, '0'), 'YYYYMMDDHH24MI'));
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END to_date_time_fn;

  /*
  ||----------------------------------------------------------------------------
  || PREV_PROD_RCPT_TS_FN
  ||  Calculates product-receipt date/time for vendor for a given LLR date/time.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/26/11 | rhalpai | Original - Created for IM-004248
  ||----------------------------------------------------------------------------
  */
  FUNCTION prev_prod_rcpt_ts_fn(
    i_div          IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER,
    i_llr_ts       IN  DATE
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STRICT_ORDER_PK.PREV_PROD_RCPT_TS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_offset_mins    PLS_INTEGER;
    l_llr_offset_ts      DATE;
    l_prod_rcpt_ts       DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.add_parm(lar_parm, 'LLRTS', i_llr_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_offset_mins := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_llr_offset_mins), 0);
    l_llr_offset_ts := i_llr_ts +(l_llr_offset_mins / 24 / 60);
    logs.dbg('Get PO Cutoff Date/Time');

    SELECT MAX(p.ts)
      INTO l_prod_rcpt_ts
      FROM vndr_ts_op4v p
     WHERE p.div_part = l_div_part
       AND p.cbr_vndr_id = i_cbr_vndr_id
       AND p.ts_typ = 'PRC'
       AND p.ts <= l_llr_offset_ts;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_prod_rcpt_ts);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prev_prod_rcpt_ts_fn;

  /*
  ||----------------------------------------------------------------------------
  || NXT_PROD_RCPT_TS_FN
  ||  Retrieves next product-receipt date/time for vendor for a given date/time.
  ||  The date/time is used to get the PO cutoff date/time for the vendor.
  ||  Starting with midnite on the PO cutoff date, we move forward to the last
  ||  date using the available lead dates for the vendors required number of
  ||  lead days. The lead dates will all have the time truncated to midnite.
  ||  We now select the first product-receipt date/time that is greater-than or
  ||  equal-to this date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/10/07 | rhalpai | Original - Created for PIR5002
  || 07/04/08 | rhalpai | Changed to use new VNDR_TS_OP4V table to calculate
  ||                    | the next product-receipt timestamp. PIR5002
  || 12/23/10 | rhalpai | Changed SQL to handle 0 day lead. PIR9632
  || 01/26/11 | rhalpai | Add CustId parm and logic to check for existence of
  ||                    | eligible Reroute entry and when found obtain
  ||                    | ProdRcptTS using Reroute LLRTS in call to
  ||                    | PREV_PROD_RCPT_TS_FN. IM-004248
  ||----------------------------------------------------------------------------
  */
  FUNCTION nxt_prod_rcpt_ts_fn(
    i_div              IN  VARCHAR2,
    i_cust_id          IN  VARCHAR2,
    i_cbr_vndr_id      IN  NUMBER,
    i_ts               IN  DATE,
    i_run_intrvl_mins  IN  PLS_INTEGER DEFAULT NULL
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STRICT_ORDER_PK.NXT_PROD_RCPT_TS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_run_intrvl_mins    PLS_INTEGER;
    l_offset_ts          DATE;
    l_prod_rcpt_ts       DATE;
    l_reroute_llr_ts     DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.add_parm(lar_parm, 'TS', i_ts);
    logs.add_parm(lar_parm, 'RunIntrvlMins', i_run_intrvl_mins);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_run_intrvl_mins := COALESCE(i_run_intrvl_mins,
                                  op_parms_pk.val_fn(l_div_part, op_const_pk.prm_strctr_intrvl_min),
                                  0
                                 );
    -- include recap process run interval
    l_offset_ts := i_ts +(l_run_intrvl_mins / 24 / 60);
    logs.dbg('Get Product-Receipt Date/Time');

    SELECT MIN(r.ts)
      INTO l_prod_rcpt_ts
      FROM vndr_ts_op4v r
     WHERE r.div_part = l_div_part
       AND r.cbr_vndr_id = i_cbr_vndr_id
       AND r.ts_typ = 'PRC'
       AND r.ts > (SELECT MIN(p.ts)
                     FROM vndr_ts_op4v p
                    WHERE p.div_part = l_div_part
                      AND p.cbr_vndr_id = i_cbr_vndr_id
                      AND p.ts_typ = 'PO'
                      AND p.ts >= l_offset_ts)
       AND r.ts > (SELECT w.wrk_dt - INTERVAL '1' SECOND AS lead_dt
                     FROM (SELECT l.ts AS wrk_dt, ROW_NUMBER() OVER(ORDER BY l.ts) - 1 AS row_num
                             FROM vndr_ts_op4v l
                            WHERE l.div_part = l_div_part
                              AND l.cbr_vndr_id = i_cbr_vndr_id
                              AND l.ts_typ = 'LED'
                              AND TRUNC(l.ts) >= (SELECT TRUNC(MIN(p.ts))
                                                    FROM vndr_ts_op4v p
                                                   WHERE p.div_part = l_div_part
                                                     AND p.cbr_vndr_id = i_cbr_vndr_id
                                                     AND p.ts_typ = 'PO'
                                                     AND p.ts >= l_offset_ts)) w
                    WHERE w.row_num = (SELECT v.lead_days
                                         FROM vndr_mstr_op1v v
                                        WHERE v.div_part = l_div_part
                                          AND v.cbr_vndr_id = i_cbr_vndr_id));

    logs.dbg('Get Reroute LLR Date/Time');

    SELECT MIN(TO_DATE(TO_CHAR(r.llr_dt, 'YYYYMMDD') || LPAD(r.llr_time, 4, '0'), 'YYYYMMDDHH24MI'))
      INTO l_reroute_llr_ts
      FROM reroute_rt1r r
     WHERE r.div_part = l_div_part
       AND r.cust_id = i_cust_id
       AND l_prod_rcpt_ts BETWEEN r.eff_ts AND r.end_ts
       AND l_prod_rcpt_ts <= TO_DATE(TO_CHAR(r.llr_dt, 'YYYYMMDD') || LPAD(r.llr_time, 4, '0'), 'YYYYMMDDHH24MI');

    IF l_reroute_llr_ts IS NOT NULL THEN
      logs.dbg('Get Previous ProdRcptTS from Reroute LLR Date/Time');
      l_prod_rcpt_ts := prev_prod_rcpt_ts_fn(i_div, i_cbr_vndr_id, l_reroute_llr_ts);
    END IF;   -- l_reroute_llr_ts IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_prod_rcpt_ts);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END nxt_prod_rcpt_ts_fn;

  /*
  ||----------------------------------------------------------------------------
  || PO_CUTOFF_TS_FN
  ||  Calculate PO cutoff date/time for vendor given an order's LLR date/time.
  ||  Calculation:
  ||   Find the first product-receipt date/time for vendor prior to the LLR.
  ||   Find the nth lead date working back from and including the prod-rcpt date.
  ||   Use the first PO date/time prior to the resulting lead date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/08 | rhalpai | Original - Created for PIR5002
  || 12/23/10 | rhalpai | Changed SQL to handle 0 day lead. PIR9632
  ||----------------------------------------------------------------------------
  */
  FUNCTION po_cutoff_ts_fn(
    i_div          IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER,
    i_llr_ts       IN  DATE
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STRICT_ORDER_PK.PO_CUTOFF_TS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_offset_mins    PLS_INTEGER;
    l_llr_offset_ts      DATE;
    l_po_cutoff_ts       DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.add_parm(lar_parm, 'LLRTS', i_llr_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_offset_mins := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_llr_offset_mins), 0);
    l_llr_offset_ts := i_llr_ts +(l_llr_offset_mins / 24 / 60);
    logs.dbg('Get PO Cutoff Date/Time');

    SELECT NVL(MAX(p.ts), TO_DATE('19000101', 'YYYYMMDD'))
      INTO l_po_cutoff_ts
      FROM vndr_ts_op4v p
     WHERE p.div_part = l_div_part
       AND p.cbr_vndr_id = i_cbr_vndr_id
       AND p.ts_typ = 'PO'
       AND p.ts <= (SELECT w.wrk_dt + 1 - INTERVAL '1' SECOND AS lead_dt
                      FROM (SELECT l.ts AS wrk_dt, ROW_NUMBER() OVER(ORDER BY l.ts DESC) - 1 AS row_num
                              FROM vndr_ts_op4v l
                             WHERE l.div_part = l_div_part
                               AND l.cbr_vndr_id = i_cbr_vndr_id
                               AND l.ts_typ = 'LED'
                               AND l.ts <= (SELECT TRUNC(MAX(r.ts))
                                              FROM vndr_ts_op4v r
                                             WHERE r.div_part = l_div_part
                                               AND r.cbr_vndr_id = i_cbr_vndr_id
                                               AND r.ts_typ = 'PRC'
                                               AND r.ts <= l_llr_offset_ts)) w
                     WHERE w.row_num = (SELECT v.lead_days
                                          FROM vndr_mstr_op1v v
                                         WHERE v.div_part = l_div_part
                                           AND v.cbr_vndr_id = i_cbr_vndr_id));

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_po_cutoff_ts);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END po_cutoff_ts_fn;

  /*
  ||----------------------------------------------------------------------------
  || RECAP_ORDS_CUR_FN
  ||  Retrieves cursor of recap orders for strict vendor items for PO creation.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/08 | rhalpai | Original - Created for PIR5002
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  || 12/30/08 | rhalpai | Changed cursor to exclude reserved loads. IM468705
  || 04/23/10 | rhalpai | Changed cursor to use LLR Date/Time from Reroute table
  ||                    | for llr_ts when match is found. PIR7415
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 04/04/11 | rhalpai | Change cursor logic to exclude DFLT load only when
  ||                    | there are no qualifying REROUTE entries for customer.
  ||                    | SP11BI2
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION recap_ords_cur_fn(
    i_div              IN  VARCHAR2,
    i_run_ts           IN  DATE,
    i_run_intrvl_mins  IN  PLS_INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module   CONSTANT typ.t_maxfqnm             := 'OP_STRICT_ORDER_PK.RECAP_ORDS_CUR_FN';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                      := SYSDATE;
    l_div_part            NUMBER;
    l_t_xloads            type_stab;
    l_t_parms             op_types_pk.tt_varchars_v;
    l_run_intrvl_mins     PLS_INTEGER;
    l_offset_ts           DATE;
    l_llr_offset_mins     PLS_INTEGER;
    l_recap_grace_mins    PLS_INTEGER;
    l_cv                  SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RunTS', i_run_ts);
    logs.add_parm(lar_parm, 'RunIntrvlMins', i_run_intrvl_mins);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                         op_const_pk.prm_strctr_intrvl_min
                                         || ','
                                         || op_const_pk.prm_llr_offset_mins
                                         || ','
                                         || op_const_pk.prm_strctr_grace_min
                                        );
    l_run_intrvl_mins := COALESCE(i_run_intrvl_mins, l_t_parms(op_const_pk.prm_strctr_intrvl_min), 0);
    l_offset_ts := i_run_ts +(l_run_intrvl_mins / 24 / 60) - INTERVAL '1' SECOND;
    l_llr_offset_mins := NVL(l_t_parms(op_const_pk.prm_llr_offset_mins), 0);
    l_recap_grace_mins := NVL(l_t_parms(op_const_pk.prm_strctr_grace_min), 0);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   y.cbr_vndr_id, y.prod_rcpt_ts, y.ord_num, y.ord_ln, y.ord_qty, y.llr_dt, y.po_cutoff,
                (CASE
                   WHEN i_run_ts -(l_recap_grace_mins / 24 / 60) > y.po_cutoff THEN 'Y'
                   ELSE 'N'
                 END) AS miss_po_cutoff_sw
           FROM (SELECT x.cbr_vndr_id, x.prcs_po_cutoff, x.ord_num, x.ord_ln, x.ord_qty, x.llr_dt,
                        op_strict_order_pk.po_cutoff_ts_fn(i_div, x.cbr_vndr_id, x.llr_ts) AS po_cutoff,
                        (SELECT MAX(r.ts)
                           FROM vndr_ts_op4v r
                          WHERE r.div_part = l_div_part
                            AND r.cbr_vndr_id = x.cbr_vndr_id
                            AND r.ts_typ = 'PRC'
                            AND r.ts <= x.llr_ts +(l_llr_offset_mins / 24 / 60)) AS prod_rcpt_ts
                   FROM (SELECT v.cbr_vndr_id, v.prcs_po_cutoff, so.ord_num, so.ord_ln, b.ordqtb AS ord_qty, ld.llr_dt,
                                NVL((SELECT MIN(TO_DATE(TO_CHAR(rr.llr_dt, 'YYYYMMDD') || LPAD(rr.llr_time, 4, '0'),
                                                        'YYYYMMDDHH24MI'
                                                       )
                                               ) AS llr_ts
                                       FROM reroute_rt1r rr
                                      WHERE rr.div_part = l_div_part
                                        AND rr.cust_id = a.custa
                                        AND GREATEST(so.prod_rcpt_ts, l_c_sysdate) BETWEEN rr.eff_ts AND rr.end_ts
                                        AND GREATEST(so.prod_rcpt_ts, l_c_sysdate) <=
                                              TO_DATE(TO_CHAR(rr.llr_dt, 'YYYYMMDD') || LPAD(rr.llr_time, 4, '0'),
                                                      'YYYYMMDDHH24MI'
                                                     )),
                                    ld.llr_ts
                                   ) AS llr_ts
                           FROM strct_ord_op1o so,
                                (SELECT vm.cbr_vndr_id,
                                        (SELECT MAX(p.ts)
                                           FROM vndr_ts_op4v p
                                          WHERE p.div_part = vm.div_part
                                            AND p.cbr_vndr_id = vm.cbr_vndr_id
                                            AND p.ts_typ = 'PO'
                                            AND p.ts <= l_offset_ts) AS prcs_po_cutoff
                                   FROM vndr_mstr_op1v vm
                                  WHERE vm.div_part = l_div_part
                                    AND EXISTS(SELECT 1
                                                 FROM vndr_ts_op4v p
                                                WHERE p.div_part = vm.div_part
                                                  AND p.cbr_vndr_id = vm.cbr_vndr_id
                                                  AND p.ts_typ = 'PO'
                                                  AND p.ts <= l_offset_ts)
                                    AND EXISTS(SELECT 1
                                                 FROM strct_ord_op1o so, ordp120b b
                                                WHERE so.div_part = vm.div_part
                                                  AND so.cbr_vndr_id = vm.cbr_vndr_id
                                                  AND so.stat = 'URC'
                                                  AND b.div_part = so.div_part
                                                  AND b.ordnob = so.ord_num
                                                  AND b.lineb = so.ord_ln
                                                  AND b.excptn_sw = 'N'
                                                  AND b.statb = 'O')) v,
                                ordp120b b, ordp100a a, load_depart_op1f ld
                          WHERE so.div_part = l_div_part
                            AND so.cbr_vndr_id = v.cbr_vndr_id
                            AND so.stat = 'URC'
                            AND b.div_part = so.div_part
                            AND b.ordnob = so.ord_num
                            AND b.lineb = so.ord_ln
                            AND b.excptn_sw = 'N'
                            AND b.statb = 'O'
                            AND a.div_part = so.div_part
                            AND a.ordnoa = so.ord_num
                            AND a.excptn_sw = 'N'
                            AND ld.div_part = a.div_part
                            AND ld.load_depart_sid = a.load_depart_sid
                            AND ld.load_num NOT IN(SELECT t.column_value
                                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                                    WHERE t.column_value <> 'DFLT')
                            AND (   ld.load_num <> 'DFLT'
                                 OR EXISTS(SELECT 1
                                             FROM reroute_rt1r rr
                                            WHERE rr.div_part = a.div_part
                                              AND rr.cust_id = a.custa
                                              AND GREATEST(so.prod_rcpt_ts, l_c_sysdate) BETWEEN rr.eff_ts AND rr.end_ts)
                                )) x) y
          WHERE y.po_cutoff <= y.prcs_po_cutoff
       ORDER BY y.cbr_vndr_id, y.prod_rcpt_ts, y.ord_num, y.ord_ln;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END recap_ords_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_STRICT_ORD_FN
  ||  Indicate whether Order contains Strict Items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/30/08 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_strict_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_cv             SYS_REFCURSOR;
    l_strict_ord_sw  VARCHAR2(1)   := 'N';
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM strct_ord_op1o so
        WHERE so.div_part = i_div_part
          AND so.ord_num = i_ord_num;

    FETCH l_cv
     INTO l_strict_ord_sw;

    RETURN(l_strict_ord_sw = 'Y');
  END is_strict_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_RECAPPED_FN
  ||  Indicate whether Order or Order Line has been recapped.
  ||
  ||  29990101 indicates initial status
  ||  19000101 indicates cancelled status
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_recapped_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  )
    RETURN BOOLEAN IS
    l_cv           SYS_REFCURSOR;
    l_recapped_sw  VARCHAR2(1)   := 'N';
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM strct_ord_op1o so
        WHERE so.div_part = i_div_part
          AND so.ord_num = i_ord_num
          AND (   i_ord_ln IS NULL
               OR so.ord_ln = i_ord_ln)
          AND so.stat = 'RCP';

    FETCH l_cv
     INTO l_recapped_sw;

    RETURN(l_recapped_sw = 'Y');
  END is_recapped_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_WILL_MISS_PO_CUTOFF_FN
  ||  Indicate whether new LLR for order will cause unrecapped order lines for
  ||  vendors to miss their next PO Cutoff.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/17/08 | rhalpai | Original
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  || 07/04/13 | rhalpai | Convert to use LLRTs parm. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_will_miss_po_cutoff_fn(
    i_div_part    IN  NUMBER,
    i_ord_num     IN  NUMBER,
    i_new_llr_ts  IN  DATE
  )
    RETURN BOOLEAN IS
    l_c_module  CONSTANT typ.t_maxfqnm             := 'OP_STRICT_ORDER_PK.ORD_WILL_MISS_PO_CUTOFF_FN';
    lar_parm             logs.tar_parm;
    l_t_parms            op_types_pk.tt_varchars_v;
    l_strctr_intrvl_min  NUMBER;
    l_strctr_grace_min   NUMBER;
    l_min_ts             DATE;
    l_miss_po_cutoff_sw  VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'NewLLRTs', i_new_llr_ts);
    logs.dbg('ENTRY', lar_parm);
    l_t_parms := op_parms_pk.idx_vals_fn(i_div_part,
                                         op_const_pk.prm_strctr_intrvl_min || ',' || op_const_pk.prm_strctr_grace_min
                                        );
    l_strctr_intrvl_min := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_strctr_intrvl_min)), 0);
    l_strctr_grace_min := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_strctr_grace_min)), 0);
    l_min_ts := SYSDATE + (l_strctr_intrvl_min - l_strctr_grace_min) / 24 / 60;

    SELECT NVL(MAX('Y'), 'N')
      INTO l_miss_po_cutoff_sw
      FROM DUAL
     WHERE EXISTS(SELECT   1
                      FROM strct_ord_op1o so
                     WHERE so.div_part = i_div_part
                       AND so.ord_num = i_ord_num
                       AND so.stat = 'URC'
                  GROUP BY so.cbr_vndr_id
                    HAVING op_strict_order_pk.po_cutoff_ts_fn(i_div_part, so.cbr_vndr_id, i_new_llr_ts) <
                             (SELECT MIN(t.ts) AS nxt_po_cutoff
                                FROM vndr_ts_op4v t
                               WHERE t.div_part = i_div_part
                                 AND t.cbr_vndr_id = so.cbr_vndr_id
                                 AND t.ts_typ = 'PO'
                                 AND t.ts > l_min_ts));

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_miss_po_cutoff_sw = 'Y');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_will_miss_po_cutoff_fn;

  /*
  ||----------------------------------------------------------------------------
  || RECAP_SP
  ||  Recap orders for strict vendor items and ftp to mainframe to create PO.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/10/07 | rhalpai | Original - Created for PIR5002
  || 06/16/08 | rhalpai | Added sort by Vendor/Item/ProdRcptTS to cursor.
  || 07/04/08 | rhalpai | Changed to call new RECAP_ORDS_CUR_FN for cursor of
  ||                    | order lines to include in recap process. PIR5002
  || 08/01/08 | rhalpai | Changed call to STRCT_VNDR_RECAP_RPT_SP to include
  ||                    | remote file to allow automatic ftp. PIR5002
  || 09/24/08 | rhalpai | Changed to bypass ftp to mainframe if there is nothing
  ||                    | to recap. PIR5002
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  || 09/08/11 | rhalpai | Added logic to push out unrecapped orders for
  ||                    | matching CbrVendor/ProdRcptTS. IM-026343
  || 09/12/12 | rhalpai | Change logic to Push out ProdRcptTS to include status
  ||                    | O,I,S. IM-066417
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 11/10/15 | rhalpai | Add logic to support new cust_lvl_dtl_sw. PIR15456
  || 01/02/24 | rhalpai | Add call to new STRCT_ORDLN_EXTR_SP. PC-9546
  || 04/01/24 | rhalpai | Change logic to round up to master case qty for US Foods. Utilize new table STRCT_MSTR_CS_OP3I
  ||                    | to identify master case qty items and maintain residual retail qty. PC-9784
  || 10/24/25 | rhalpai | Remove rsdl_qty. PC-10528
  || 11/05/25 | rhalpai | Add logic to round up to master case qty for FC item. PC-10528
  ||----------------------------------------------------------------------------
  */
  PROCEDURE recap_sp(
    i_div     IN  VARCHAR2,
    i_run_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module                 CONSTANT typ.t_maxfqnm                     := 'OP_STRICT_ORDER_PK.RECAP_SP';
    lar_parm                            logs.tar_parm;
    l_div_part                          NUMBER;
    l_c_run_ts                 CONSTANT DATE                              := NVL(i_run_ts, SYSDATE);
    l_cv                                SYS_REFCURSOR;

    TYPE l_rt_recap_ord IS RECORD(
      cbr_vndr_id        vndr_mstr_op1v.cbr_vndr_id%TYPE,
      prod_rcpt_ts       DATE,
      ord_num            NUMBER,
      ord_ln             NUMBER,
      ord_qty            NUMBER,
      llr_dt             DATE,
      po_cutoff          DATE,
      miss_po_cutoff_sw  VARCHAR2(1)
    );

    l_r_recap_ord                       l_rt_recap_ord;
    l_llr_dt                            DATE;
    l_prod_rcpt_ts                      DATE;
    l_c_stat_recapped          CONSTANT VARCHAR2(3)                       := 'RCP';
    l_c_stat_missed_po_cutoff  CONSTANT VARCHAR2(3)                       := 'MPC';
    l_c_init_val               CONSTANT PLS_INTEGER                       := -999;
    l_save_ord_num                      strct_ord_op1o.ord_num%TYPE       := l_c_init_val;
    l_save_cbr_vendor                   strct_ord_op1o.cbr_vndr_id%TYPE   := l_c_init_val;
    l_c_file_dir               CONSTANT VARCHAR2(50)                      := '/ftptrans';
    l_t_rpt_lns                         typ.tas_maxvc2;
    l_c_rpt_rmt_file           CONSTANT VARCHAR2(20)                      := 'STRICT.RECAP.RPT';
    l_c_data_file_nm           CONSTANT VARCHAR2(30)                      := i_div || '_STRICT_ITEM_RECAP';
    l_c_data_rmt_file          CONSTANT VARCHAR2(20)                      := 'STRICT.ITEM.RECAP';

    CURSOR l_cur_recap(
      b_div_part  NUMBER,
      b_recap_ts  DATE
    ) IS
      SELECT   x.dcs_vndr_id, x.catlg_num, x.prod_rcpt_ts, SUM(x.recap_qty) AS recap_qty
          FROM (SELECT   v.dcs_vndr_id, b.orditb AS catlg_num, so.prod_rcpt_ts, SUM(so.recap_qty) AS recap_qty
                    FROM strct_ord_op1o so, vndr_mstr_op1v v, ordp120b b
                   WHERE so.div_part = b_div_part
                     AND so.recap_ts = b_recap_ts
                     AND so.stat = 'RCP'
                     AND v.div_part = so.div_part
                     AND v.cbr_vndr_id = so.cbr_vndr_id
                     AND b.div_part = so.div_part
                     AND b.ordnob = so.ord_num
                     AND b.lineb = so.ord_ln
                     AND b.excptn_sw = 'N'
                     AND b.statb = 'O'
                     AND b.orditb NOT IN(SELECT smc.ss_item
                                           FROM strct_mstr_cs_op3i smc
                                          WHERE smc.div_part = b_div_part)
                GROUP BY v.dcs_vndr_id, b.orditb, so.prod_rcpt_ts
                UNION ALL
                SELECT   v.dcs_vndr_id, smc.fc_item AS catlg_num, so.prod_rcpt_ts,
                         CEIL((SUM(so.recap_qty)) / e.mulsle) AS recap_qty
                    FROM strct_ord_op1o so, strct_mstr_cs_op3i smc, vndr_mstr_op1v v, ordp120b b, sawp505e e
                   WHERE so.div_part = b_div_part
                     AND so.recap_ts = b_recap_ts
                     AND so.stat = 'RCP'
                     AND smc.div_part = so.div_part
                     AND smc.cbr_vndr_id = so.cbr_vndr_id
                     AND v.div_part = so.div_part
                     AND v.cbr_vndr_id = so.cbr_vndr_id
                     AND b.div_part = so.div_part
                     AND b.ordnob = so.ord_num
                     AND b.lineb = so.ord_ln
                     AND b.excptn_sw = 'N'
                     AND b.statb = 'O'
                     AND b.orditb = smc.ss_item
                     AND e.catite = b.orditb
                GROUP BY v.dcs_vndr_id, smc.fc_item, so.prod_rcpt_ts, e.mulsle) x
      GROUP BY x.dcs_vndr_id, x.catlg_num, x.prod_rcpt_ts
      ORDER BY dcs_vndr_id, catlg_num, prod_rcpt_ts;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strict_recap,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.dbg('Get recap orders cursor');
    l_cv := recap_ords_cur_fn(i_div, l_c_run_ts);
    <<recap_ords_loop>>
    LOOP
      logs.dbg('Fetch recap orders cursor');

      FETCH l_cv
       INTO l_r_recap_ord;

      EXIT WHEN l_cv%NOTFOUND;

      IF l_r_recap_ord.ord_num <> l_save_ord_num THEN
        l_llr_dt := l_r_recap_ord.llr_dt;
        l_prod_rcpt_ts := l_r_recap_ord.prod_rcpt_ts;

        IF l_save_ord_num <> l_c_init_val THEN
          logs.dbg('Tag remaining order lines unavailable for recap');
          -- (i.e. order lines in bad order well)
          upd_strct_ord_sp(l_c_stat_missed_po_cutoff,
                           l_c_run_ts,
                           l_llr_dt,
                           l_prod_rcpt_ts,
                           l_div_part,
                           l_save_ord_num,
                           NULL,
                           0,
                           l_save_cbr_vendor
                          );
        END IF;   -- l_save_ord_num <> l_c_init_val

        l_save_ord_num := l_r_recap_ord.ord_num;
        l_save_cbr_vendor := l_r_recap_ord.cbr_vndr_id;
      END IF;   -- l_r_recap_ord.ord_num <> l_save_ord_num

      IF l_r_recap_ord.miss_po_cutoff_sw = 'N' THEN
        logs.dbg('Tag order lines as recapped');
        upd_strct_ord_sp(l_c_stat_recapped,
                         l_c_run_ts,
                         l_llr_dt,
                         l_prod_rcpt_ts,
                         l_div_part,
                         l_r_recap_ord.ord_num,
                         l_r_recap_ord.ord_ln,
                         l_r_recap_ord.ord_qty
                        );
      END IF;   -- l_r_recap_ord.miss_po_cutoff_sw = 'N'
    END LOOP recap_ords_loop;

    IF l_save_ord_num <> l_c_init_val THEN
      logs.dbg('Final update to tag order lines unavailable for recap');
      upd_strct_ord_sp(l_c_stat_missed_po_cutoff,
                       l_c_run_ts,
                       l_llr_dt,
                       l_prod_rcpt_ts,
                       l_div_part,
                       l_save_ord_num,
                       NULL,
                       0,
                       l_save_cbr_vendor
                      );
      logs.dbg('Create Strict Item Vendor Recap Report');
      op_misc_reports_pk.strct_vndr_recap_rpt_sp(i_div, l_c_rpt_rmt_file, l_c_run_ts);
      logs.dbg('Create Strict Order Detail Extract file');
      strct_ord_dtl_extr_sp(i_div, l_c_run_ts);
      logs.dbg('Create Strict Order Line Recap Extract file');
      strct_ordln_extr_sp(i_div, l_c_run_ts);
      logs.dbg('Build the recap file');
      <<recap_data_loop>>
      FOR l_r_recap IN l_cur_recap(l_div_part, l_c_run_ts) LOOP
        IF l_r_recap.recap_qty > 0 THEN
          util.append(l_t_rpt_lns,
                      i_div
                      || lpad_fn(l_r_recap.dcs_vndr_id, 10, '0')
                      || RPAD(lpad_fn(l_r_recap.catlg_num, 6, '0'), 10)
                      || lpad_fn(l_r_recap.recap_qty, 9, '0')
                      || TO_CHAR(l_r_recap.prod_rcpt_ts, 'YYYY-MM-DD')
                     );
        END IF;   -- l_r_recap.recap_qty > 0
      END LOOP recap_data_loop;
      logs.dbg('Write File');
      write_sp(l_t_rpt_lns, l_c_data_file_nm, l_c_file_dir);
      logs.dbg('FTP to mainframe');
      op_ftp_sp(i_div, l_c_data_file_nm, l_c_data_rmt_file);
      logs.dbg('Push out ProdRcptTS');

      UPDATE strct_ord_op1o so
         SET so.prod_rcpt_ts = op_strict_order_pk.nxt_prod_rcpt_ts_fn(i_div,
                                                                      (SELECT a.custa
                                                                         FROM ordp100a a
                                                                        WHERE a.div_part = so.div_part
                                                                          AND a.ordnoa = so.ord_num),
                                                                      so.cbr_vndr_id,
                                                                      l_c_run_ts
                                                                     )
       WHERE so.div_part = l_div_part
         AND so.stat = 'URC'
         AND (so.cbr_vndr_id, so.prod_rcpt_ts) IN(SELECT   op1o.cbr_vndr_id, op1o.prod_rcpt_ts
                                                      FROM strct_ord_op1o op1o
                                                     WHERE op1o.div_part = l_div_part
                                                       AND op1o.recap_ts = l_c_run_ts
                                                       AND op1o.stat = 'RCP'
                                                  GROUP BY op1o.cbr_vndr_id, op1o.prod_rcpt_ts)
         AND EXISTS(SELECT 1
                      FROM ordp120b b
                     WHERE b.div_part = so.div_part
                       AND b.ordnob = so.ord_num
                       AND b.lineb = so.ord_ln
                       AND b.excptn_sw = 'N'
                       AND b.statb IN('O', 'I', 'S'));

      COMMIT;
    END IF;   -- l_save_ord_num <> l_c_init_val

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strict_recap,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strict_recap,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END recap_sp;

  /*
  ||----------------------------------------------------------------------------
  || BI_RPT_EXTR_SP
  ||  New SP to return a cursor for BI Reporting
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/10/13 | dlbeal  | Original
  || 11/04/13 | dlbeal  | Modified SP to include canceled order lines
  || 07/16/25 | rhalpai | Add customer name to cursor. PC-10410
  ||----------------------------------------------------------------------------
  */
  PROCEDURE bi_rpt_extr_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_STRICT_ORDER_PK.BI_RPT_EXTR_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN o_cur
     FOR
       SELECT   'CURRENT ' AS order_well, d.div_id AS div, TO_CHAR(m.dcs_vndr_id) AS vendor_id,
                m.vndr_nm AS vendor_name, b.orditb AS item, e.ctdsce AS descr, x.custb AS cbr_cust, x.storeb AS store#,
                x.mccusb AS acct#, TO_CHAR(b.ordnob) AS order#, b.lineb AS ord_ln, NVL(b.ntshpb, ' ') AS ord_exception,
                f.load_num AS load#, TO_CHAR(a.ord_rcvd_ts, 'YYYYMMDD HH24:MI:SS') AS order_date,
                TO_CHAR(f.llr_ts, 'YYYYMMDD') AS bill_date, TO_CHAR(f.llr_ts, 'HH24:MI') AS bill_time,
                TO_CHAR(g.eta_ts, 'YYYYMMDD') AS eta_date, b.ordqtb AS ord_qty, b.alcqtb AS bill_qty,
                b.pckqtb AS ship_qty, b.shpidb AS billing_ts, a.cpoa AS cust_po#, b.statb AS status,
                x.corpb AS corp_code, a.ipdtsa AS order_srce, c.retgpc AS group_code, NVL(a.connba, ' ') AS confirm#,
                CASE
                  WHEN o.ord_ln = b.lineb THEN o.recap_ts
                  ELSE NULL
                END AS recap_ts, CASE
                  WHEN o.ord_ln = b.lineb THEN o.prod_rcpt_ts
                  ELSE NULL
                END AS prod_rcpt_ts, CASE
                  WHEN o.ord_ln = b.lineb THEN o.llr_at_recap
                  ELSE NULL
                END AS llr_at_recap, CASE
                  WHEN o.ord_ln = b.lineb THEN o.stat
                  ELSE 'N/A'
                END AS stat, CASE
                  WHEN o.ord_ln = b.lineb THEN o.recap_qty
                  ELSE NULL
                END AS recap_qty, c.namec AS cust_nm
           FROM div_mstr_di1d d, vndr_mstr_op1v m, strct_item_op3v v, sawp505e e, ordp120b b, strct_ord_op1o o,
                ordp100a a, sysp200c c, stop_eta_op1g g, mclp020b x, load_depart_op1f f
          WHERE d.div_id = i_div
            AND m.div_part = d.div_part
            AND v.div_part = m.div_part
            AND v.cbr_vndr_id = m.cbr_vndr_id
            AND e.iteme = v.item_num
            AND e.uome = v.uom
            AND b.div_part = v.div_part
            AND b.itemnb = v.item_num
            AND b.sllumb = v.uom
            AND b.orditb = e.catite
            AND o.div_part = b.div_part
            AND o.ord_num = b.ordnob
            AND o.ord_ln = FLOOR(b.lineb)
            AND a.div_part = b.div_part
            AND a.ordnoa = b.ordnob
            AND a.dsorda = 'R'
            AND a.custa = x.custb
            AND c.div_part = d.div_part
            AND c.acnoc = a.custa
            AND g.div_part = a.div_part
            AND g.load_depart_sid = a.load_depart_sid
            AND g.cust_id = a.custa
            AND x.div_part = a.div_part
            AND x.custb = a.custa
            AND f.div_part = a.div_part
            AND f.load_depart_sid = a.load_depart_sid
       UNION ALL
       SELECT   'HISTORY ' AS order_well, d.div_id AS div, TO_CHAR(m.dcs_vndr_id) AS vendor_id,
                m.vndr_nm AS vendor_name, b.orditb AS item, e.ctdsce AS descr, x.custb AS cbr_cust, x.storeb AS store#,
                x.mccusb AS acct#, TO_CHAR(b.ordnob) AS order#, b.lineb AS ord_ln, NVL(b.ntshpb, ' ') AS ord_exception,
                a.orrtea AS load#, TO_CHAR(a.ord_rcvd_ts, 'YYYYMMDD HH24:MI:SS') AS order_date,
                TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYYMMDD') AS bill_date,
                SUBSTR(TO_CHAR(a.ctofta, '0000'), 2, 2) || ':' || SUBSTR(TO_CHAR(a.ctofta, '0000'), 4, 2) AS bill_time,
                TO_CHAR(DATE '1900-02-28' + a.etadta, 'YYYYMMDD') AS eta_date, b.ordqtb AS ord_qty,
                b.alcqtb AS bill_qty, b.pckqtb AS ship_qty, b.shpidb AS billing_ts, a.cpoa AS cust_po#,
                b.statb AS status, x.corpb AS corp_code, a.ipdtsa AS order_srce, c.retgpc AS group_code,
                NVL(a.connba, ' ') AS confirm#, CASE
                  WHEN o.ord_ln = b.lineb THEN o.recap_ts
                  ELSE NULL
                END AS recap_ts, CASE
                  WHEN o.ord_ln = b.lineb THEN o.prod_rcpt_ts
                  ELSE NULL
                END AS prod_rcpt_ts, CASE
                  WHEN o.ord_ln = b.lineb THEN o.llr_at_recap
                  ELSE NULL
                END AS llr_at_recap, CASE
                  WHEN o.ord_ln = b.lineb THEN o.stat
                  ELSE 'N/A'
                END AS stat, CASE
                  WHEN o.ord_ln = b.lineb THEN o.recap_qty
                  ELSE NULL
                END AS recap_qty, c.namec AS cust_nm
           FROM div_mstr_di1d d, strct_ord_op1o o, ordp920b b, strct_item_op3v v, vndr_mstr_op1v m, sawp505e e,
                ordp900a a, mclp020b x, sysp200c c
          WHERE d.div_id = i_div
            AND o.div_part = d.div_part
            AND b.div_part = o.div_part
            AND b.ordnob = o.ord_num
            AND o.ord_ln = FLOOR(b.lineb)
            AND v.div_part = b.div_part
            AND v.item_num = b.itemnb
            AND v.uom = b.sllumb
            AND m.div_part = v.div_part
            AND m.cbr_vndr_id = v.cbr_vndr_id
            AND e.catite = b.orditb
            AND e.iteme = v.item_num
            AND e.uome = v.uom
            AND a.div_part = b.div_part
            AND a.ordnoa = b.ordnob
            AND a.dsorda = 'R'
            AND x.div_part = a.div_part
            AND x.custb = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
       ORDER BY 4, 15, 13, 7, 10, 11;   -- vendor name; bill date; load #; CBR Cust #; Order #; Order Line #;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END bi_rpt_extr_sp;

  /*
  ||----------------------------------------------------------------------------
  || ENFORC_PO_QTY_SP
  ||  Update Order Line Qty to Enforce Strict PO Qty.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/02/24 | rhalpai | Original - Created for PC-9546
  ||----------------------------------------------------------------------------
  */
  PROCEDURE enforc_po_qty_sp(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER,
    i_po_qty   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_STRICT_ORDER_PK.ENFORC_PO_QTY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_r_ord_ln           ordp120b%ROWTYPE;
    l_ord_qty            NUMBER;
    l_r_new_ord_ln       ordp120b%ROWTYPE;
BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'PoQty', i_po_qty);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get OrdLn');
    l_r_ord_ln := op_ord_dtl_pk.sel_fn(l_div_part, i_ord_num, i_ord_ln, 'O');

    IF (    l_r_ord_ln.ordnob IS NOT NULL
        AND l_r_ord_ln.ntshpb IS NULL) THEN
      l_ord_qty := l_r_ord_ln.ordqtb;

      IF l_ord_qty <> i_po_qty THEN
        IF (    l_ord_qty > i_po_qty
            AND i_po_qty > 0
            AND l_r_ord_ln.subrcb = 0) THEN
          l_r_new_ord_ln := l_r_ord_ln;
          logs.dbg('Set New OrdLn');

          SELECT COUNT(*) + 1
            INTO l_r_new_ord_ln.lineb
            FROM ordp120b b
           WHERE b.div_part = l_div_part
             AND b.ordnob = i_ord_num
             AND b.lineb = FLOOR(b.lineb);

          l_r_new_ord_ln.ordqtb := l_ord_qty - i_po_qty;
          l_r_new_ord_ln.orgqtb := l_ord_qty - i_po_qty;
          l_r_new_ord_ln.actqtb := l_ord_qty - i_po_qty;
          l_r_new_ord_ln.ntshpb := 'STRCTQTY';
          l_r_new_ord_ln.excptn_sw := 'Y';
          logs.dbg('Add New OrdLn');
          op_ord_dtl_pk.ins_sp(l_r_new_ord_ln);
          logs.dbg('Log Exception');
          op_mclp300d_pk.ins_sp(l_div_part,
                                i_ord_num,
                                l_r_new_ord_ln.lineb,
                                'STRCTQTY',
                                l_r_new_ord_ln.itemnb,
                                l_r_new_ord_ln.sllumb,
                                l_r_new_ord_ln.ordqtb,
                                l_r_new_ord_ln.ordqtb
                               );
          logs.dbg('Add New StrctOrd Entry');

          INSERT INTO strct_ord_op1o
                      (div_part, cbr_vndr_id, ord_num, ord_ln, prod_rcpt_ts, recap_ts, recap_qty, llr_at_recap, stat)
            SELECT s.div_part, s.cbr_vndr_id, s.ord_num, l_r_new_ord_ln.lineb, s.prod_rcpt_ts, s.recap_ts, 0, NULL,
                   'XCP'
              FROM strct_ord_op1o s
             WHERE s.div_part = l_div_part
               AND s.ord_num = i_ord_num
               AND s.ord_ln = i_ord_ln;
        END IF;   -- l_ord_qty > i_po_qty AND i_po_qty > 0 AND l_r_ord_ln.subrcb = 0

        IF i_po_qty = 0 THEN
          l_r_ord_ln.ntshpb := 'STRCTQTY';
          l_r_ord_ln.excptn_sw := 'Y';
          logs.dbg('Log Exception');
          op_mclp300d_pk.ins_sp(l_div_part,
                                i_ord_num,
                                l_r_ord_ln.lineb,
                                'STRCTQTY',
                                l_r_ord_ln.itemnb,
                                l_r_ord_ln.sllumb,
                                l_ord_qty,
                                l_ord_qty
                               );
        ELSE
          l_r_ord_ln.ordqtb := i_po_qty;
          l_r_ord_ln.orgqtb := i_po_qty;
          logs.dbg('Log OrdQty Change');
          op_sysp296a_pk.ins_sp(l_div_part,
                                i_ord_num,
                                i_ord_ln,
                                'ENFORCPOQTY',
                                'ORDP120B',
                                'ORDQTB',
                                l_ord_qty,
                                i_po_qty,
                                'M',
                                'QCHG09',
                                'STRCTQTY',
                                NULL
                               );
        END IF;   -- i_po_qty = 0

        logs.dbg('Upd OrdLn');
        op_ord_dtl_pk.upd_sp(l_r_ord_ln);
      END IF;   -- l_ord_qty <> i_po_qty
    END IF;   -- l_r_ord_ln.ordnob IS NOT NULL AND l_r_ord_ln.ntshpb IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END enforc_po_qty_sp;
BEGIN
  env.set_app_cd('OPCIG');
END op_strict_order_pk;
/

