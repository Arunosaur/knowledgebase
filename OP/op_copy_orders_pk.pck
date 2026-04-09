CREATE OR REPLACE PACKAGE op_copy_orders_pk IS
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

  FUNCTION search_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION order_detail_fn(
    i_div         IN  VARCHAR2,
    i_search_typ  IN  VARCHAR2,
    i_parm_list   IN  VARCHAR2,
    i_hist_sw     IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE copy_sp(
    i_div          IN      VARCHAR2,
    i_ord_list     IN      VARCHAR2,
    i_user_id      IN      VARCHAR2,
    o_msg          OUT     VARCHAR2,
    i_hist_sw      IN      VARCHAR2 DEFAULT 'N',
    i_cust_typ     IN      VARCHAR2 DEFAULT 'MCL',
    i_new_cust     IN      VARCHAR2 DEFAULT NULL,
    i_ovrrd_po_sw  IN      VARCHAR2 DEFAULT 'N',
    i_new_po       IN      VARCHAR2 DEFAULT '~',
    i_create_typ   IN      VARCHAR2 DEFAULT csr_orders_pk.g_c_copy_order
  );
END op_copy_orders_pk;
/

CREATE OR REPLACE PACKAGE BODY op_copy_orders_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_date_fmt           CONSTANT VARCHAR2(10) := 'YYYY-MM-DD';
  g_c_srch_ord_num       CONSTANT VARCHAR2(10) := 'ORDNUM';
  g_c_srch_cust          CONSTANT VARCHAR2(10) := 'CUST';
  g_c_srch_llr_load_stp  CONSTANT VARCHAR2(10) := 'LLR/LD/STP';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ORD_HDR_FN
  ||  Return order header record.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  || 01/14/09 | rhalpai | Changed logic to exclude cancelled orders from cursor.
  ||                    | IM466947
  || 01/05/12 | rhalpai | Add logic to clear out the CustPassArea on OrdHdr
  ||                    | based on new CorpCd Parm. PIR9604
  || 07/16/12 | rhalpai | Change logic to include ShipDate on OrdHdr. PIR11044
  ||                    | Remove reference to column RESCDA.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||                    | Change logic to match new references to fields in
  ||                    | CSR_ORDERS_PK.G_RT_MSG_HDR.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_fn(
    i_div_part    IN  NUMBER,
    i_ord_num     IN  NUMBER,
    i_create_typ  IN  VARCHAR2 DEFAULT csr_orders_pk.g_c_copy_order
  )
    RETURN csr_orders_pk.g_rt_msg_hdr IS
    l_r_ord_hdr                csr_orders_pk.g_rt_msg_hdr;
    l_cv                       SYS_REFCURSOR;
    l_t_clear_cust_pass_corps  type_stab;
  BEGIN
    OPEN l_cv
     FOR
       SELECT d.div_part, d.div_id, a.custa, cx.mccusb, a.ldtypa, a.dsorda AS ord_typ, a.ipdtsa, a.telsla, a.cspasa,
              a.legrfa, a.cpoa,(CASE
                                  WHEN a.pshipa IN('1', 'Y') THEN 'Y'
                                  ELSE 'N'
                                END) AS allw_partl_sw, DATE '1900-02-28' + a.shpja AS shp_dt
         FROM div_mstr_di1d d, ordp100a a, mclp020b cx
        WHERE d.div_part = i_div_part
          AND a.div_part = d.div_part
          AND a.ordnoa = i_ord_num
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND a.dsorda = 'R'
          AND a.stata <> 'C'
          AND NOT EXISTS(SELECT 1
                           FROM sub_prcs_ord_src s
                          WHERE s.div_part = a.div_part
                            AND s.prcs_id = 'COPY ORDER'
                            AND s.prcs_sbtyp_cd = 'BCO'
                            AND s.ord_src = a.ipdtsa)
       UNION ALL
       SELECT d.div_part, d.div_id, a.custa, cx.mccusb, a.ldtypa, a.dsorda AS ord_typ, a.ipdtsa, a.telsla, a.cspasa,
              a.legrfa, a.cpoa,(CASE
                                  WHEN a.pshipa IN('1', 'Y') THEN 'Y'
                                  ELSE 'N'
                                END) AS allw_partl_sw, DATE '1900-02-28' + a.shpja AS shp_dt
         FROM div_mstr_di1d d, ordp900a a, mclp020b cx
        WHERE d.div_part = i_div_part
          AND a.div_part = d.div_part
          AND a.ordnoa = i_ord_num
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND a.dsorda = 'R'
          AND a.stata <> 'C'
          AND NOT EXISTS(SELECT 1
                           FROM sub_prcs_ord_src s
                          WHERE s.div_part = a.div_part
                            AND s.prcs_id = 'COPY ORDER'
                            AND s.prcs_sbtyp_cd = 'BCO'
                            AND s.ord_src = a.ipdtsa);

    FETCH l_cv
     INTO l_r_ord_hdr.div_part, l_r_ord_hdr.div, l_r_ord_hdr.cust_id, l_r_ord_hdr.mcl_cust, l_r_ord_hdr.load_typ,
          l_r_ord_hdr.ord_typ, l_r_ord_hdr.ord_src, l_r_ord_hdr.ser_num, l_r_ord_hdr.cust_pass_area,
          l_r_ord_hdr.legcy_ref, l_r_ord_hdr.po_num, l_r_ord_hdr.allw_partl_sw, l_r_ord_hdr.shp_dt;

    IF l_cv%FOUND THEN
      l_t_clear_cust_pass_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_clear_cust_pass);

      -- Clear CustPassArea for Corps indicated by Parm
      SELECT DECODE(MAX(cx.corpb), NULL, l_r_ord_hdr.cust_pass_area)
        INTO l_r_ord_hdr.cust_pass_area
        FROM TABLE(CAST(l_t_clear_cust_pass_corps AS type_stab)) t, mclp020b cx
       WHERE cx.div_part = i_div_part
         AND cx.custb = l_r_ord_hdr.cust_id
         AND cx.corpb = TO_NUMBER(t.column_value);

      l_r_ord_hdr.conf_num := i_create_typ || csr_orders_pk.next_conf_num_fn;
      l_r_ord_hdr.trnsmt_ts := SYSDATE;
    END IF;   -- l_cv%FOUND

    RETURN(l_r_ord_hdr);
  END ord_hdr_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTLS_FN
  ||  Return table of order details.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 04/08/13 | rhalpai | Change cursor to return hard retail. IM-101893
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtls_fn(
    i_div_part    IN  NUMBER,
    i_ord_num     IN  NUMBER,
    i_create_typ  IN  VARCHAR2 DEFAULT csr_orders_pk.g_c_copy_order
  )
    RETURN csr_orders_pk.g_tt_msg_dtls IS
    l_t_ord_dtls  csr_orders_pk.g_tt_msg_dtls := csr_orders_pk.g_tt_msg_dtls();
  BEGIN
    SELECT   x.catlg_num,
             x.cbr_item,
             x.uom,
             (CASE i_create_typ
                WHEN csr_orders_pk.g_c_copy_order THEN x.orig_qty
                ELSE x.ord_qty
              END) AS ord_qty,
             x.cust_item,
             x.item_pass_area,
             x.hard_rtl_sw,
             x.rtl_amt,
             x.rtl_mult,
             x.hard_price_sw,
             x.price_amt,
             x.orig_qty,
             x.byp_max_sw,
             x.max_qty,
             x.qty_mult,
             NULL AS ord_ln
    BULK COLLECT INTO l_t_ord_dtls
        FROM (SELECT b.orditb AS catlg_num, b.itemnb AS cbr_item, b.sllumb AS uom, b.ordqtb AS ord_qty,
                     b.cusitb AS cust_item, b.itpasb AS item_pass_area,
                     (CASE
                        WHEN b.rtfixb IN('1', 'Y') THEN 'Y'
                        ELSE 'N'
                      END) AS hard_rtl_sw, b.hdrtab AS rtl_amt, b.hdrtmb AS rtl_mult,
                     (CASE
                        WHEN b.prfixb IN('1', 'Y') THEN 'Y'
                        ELSE 'N'
                      END) AS hard_price_sw, b.hdprcb AS price_amt, b.orgqtb AS orig_qty,
                     (CASE
                        WHEN b.bymaxb IN('1', 'Y') THEN 'Y'
                        ELSE 'N'
                      END) AS byp_max_sw, b.maxqtb AS max_qty, b.qtmulb AS qty_mult, b.lineb AS ord_ln
                FROM ordp120b b
               WHERE b.div_part = i_div_part
                 AND b.ordnob = i_ord_num
                 AND b.lineb = FLOOR(b.lineb)
                 AND b.statb <> 'C'
              UNION ALL
              SELECT b.orditb AS catlg_num, b.itemnb AS cbr_item, b.sllumb AS uom, b.ordqtb AS ord_qty,
                     b.cusitb AS cust_item, b.itpasb AS item_pass_area,
                     (CASE
                        WHEN b.rtfixb IN('1', 'Y') THEN 'Y'
                        ELSE 'N'
                      END) AS hard_rtl_sw, b.hdrtab AS rtl_amt, b.hdrtmb AS rtl_mult,
                     (CASE
                        WHEN b.prfixb IN('1', 'Y') THEN 'Y'
                        ELSE 'N'
                      END) AS hard_price_sw, b.hdprcb AS price_amt, b.orgqtb AS orig_qty,
                     (CASE
                        WHEN b.bymaxb IN('1', 'Y') THEN 'Y'
                        ELSE 'N'
                      END) AS byp_max_sw, b.maxqtb AS max_qty, b.qtmulb AS qty_mult, b.lineb AS ord_ln
                FROM ordp920b b
               WHERE b.div_part = i_div_part
                 AND b.ordnob = i_ord_num
                 AND b.lineb = FLOOR(b.lineb)
                 AND b.statb <> 'C') x
    ORDER BY x.ord_ln;

    RETURN(l_t_ord_dtls);
  END ord_dtls_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || LLR_DATE_LIST_FN
  ||   Build a cursor of LLR Dates for regular orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  || 01/14/09 | rhalpai | Changed logic to exclude cancelled orders and to
  ||                    | exclude TEST load from cursor. IM466947
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 07/16/12 | rhalpai | Change logic to improve efficiency for non-history
  ||                    | queries. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION llr_date_list_fn(
    i_div      IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_COPY_ORDERS_PK.LLR_DATE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open Cursor with History');

      OPEN l_cv
       FOR
         SELECT   x.llr_dt
             FROM (SELECT TO_CHAR(ld.llr_ts, g_c_date_fmt) AS llr_dt
                     FROM load_depart_op1f ld
                    WHERE ld.div_part = l_div_part
                      AND ld.load_num NOT IN(SELECT t.column_value
                                               FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = ld.div_part
                                    AND a.load_depart_sid = ld.load_depart_sid
                                    AND a.dsorda = 'R'
                                    AND a.stata <> 'C')
                   UNION
                   SELECT TO_CHAR(DATE '1900-02-28' + a.ctofda, g_c_date_fmt) AS llr_dt
                     FROM ordp900a a
                    WHERE a.div_part = l_div_part
                      AND a.dsorda = 'R'
                      AND a.stata <> 'C'
                      AND a.orrtea NOT IN(SELECT t.column_value
                                            FROM TABLE(CAST(l_t_xloads AS type_stab)) t)) x
         ORDER BY 1;
    ELSE
      logs.dbg('Open Cursor without History');

      OPEN l_cv
       FOR
         SELECT   TO_CHAR(ld.llr_ts, g_c_date_fmt)
             FROM load_depart_op1f ld
            WHERE ld.div_part = l_div_part
              AND ld.load_num NOT IN(SELECT t.column_value
                                       FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.dsorda = 'R'
                            AND a.stata <> 'C')
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
  ||   Build a cursor of loads for regular orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  || 01/14/09 | rhalpai | Changed logic to exclude cancelled orders and to
  ||                    | exclude TEST load from cursor. IM466947
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 07/16/12 | rhalpai | Change logic to improve efficiency for non-history
  ||                    | queries. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_llr_dt   IN  VARCHAR2,
    i_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_COPY_ORDERS_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_llr_num            NUMBER;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, g_c_date_fmt);
    l_llr_num := l_llr_dt - DATE '1900-02-28';
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open Cursor with History');

      OPEN l_cv
       FOR
         SELECT   x.load_num, mc.destc
             FROM (SELECT ld.load_num
                     FROM load_depart_op1f ld
                    WHERE ld.div_part = l_div_part
                      AND ld.llr_dt = l_llr_dt
                      AND ld.load_num NOT IN(SELECT t.column_value
                                               FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = ld.div_part
                                    AND a.load_depart_sid = ld.load_depart_sid
                                    AND a.dsorda = 'R'
                                    AND a.stata <> 'C')
                   UNION
                   SELECT a.orrtea
                     FROM ordp900a a
                    WHERE a.div_part = l_div_part
                      AND a.dsorda = 'R'
                      AND a.stata <> 'C'
                      AND a.orrtea NOT IN(SELECT t.column_value
                                            FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND a.ctofda = l_llr_num) x,
                  mclp120c mc
            WHERE mc.div_part(+) = l_div_part
              AND mc.loadc(+) = x.load_num
         ORDER BY x.load_num;
    ELSE
      logs.dbg('Open Cursor without History');

      OPEN l_cv
       FOR
         SELECT   ld.load_num, mc.destc
             FROM load_depart_op1f ld, mclp120c mc
            WHERE ld.div_part = l_div_part
              AND ld.llr_dt = l_llr_dt
              AND ld.load_num NOT IN(SELECT t.column_value
                                       FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.dsorda = 'R'
                            AND a.stata <> 'C')
              AND mc.div_part(+) = ld.div_part
              AND mc.loadc(+) = ld.load_num
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
  || 11/30/07 | rhalpai | Original. PIR3593
  || 01/14/09 | rhalpai | Changed logic to exclude cancelled orders from cursor.
  ||                    | IM466947
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
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_COPY_ORDERS_PK.STOP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_llr_num            NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, g_c_date_fmt);
    l_llr_num := l_llr_dt - DATE '1900-02-28';

    IF i_hist_sw = 'Y' THEN
      logs.dbg('Open Cursor with History');

      OPEN l_cv
       FOR
         SELECT   (CASE
                     WHEN x.stop_num < 10 THEN '0' || x.stop_num
                     ELSE TO_CHAR(x.stop_num)
                   END) AS stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_name, c.zipc AS zip
             FROM (SELECT se.stop_num, se.cust_id
                     FROM load_depart_op1f ld, stop_eta_op1g se
                    WHERE ld.div_part = l_div_part
                      AND ld.llr_dt = l_llr_dt
                      AND ld.load_num = i_load_num
                      AND se.div_part = ld.div_part
                      AND se.load_depart_sid = ld.load_depart_sid
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = se.div_part
                                    AND a.load_depart_sid = se.load_depart_sid
                                    AND a.custa = se.cust_id
                                    AND a.dsorda = 'R'
                                    AND a.stata <> 'C')
                   UNION
                   SELECT a.stopsa, a.custa
                     FROM ordp900a a
                    WHERE a.div_part = l_div_part
                      AND a.dsorda = 'R'
                      AND a.stata <> 'C'
                      AND a.ctofda = l_llr_num
                      AND a.orrtea = i_load_num) x,
                  sysp200c c, mclp020b cx
            WHERE c.div_part = l_div_part
              AND c.acnoc = x.cust_id
              AND cx.div_part = c.div_part
              AND cx.custb = c.acnoc
         ORDER BY stop_num;
    ELSE
      logs.dbg('Open Cursor without History');

      OPEN l_cv
       FOR
         SELECT   (CASE
                     WHEN se.stop_num < 10 THEN '0' || se.stop_num
                     ELSE TO_CHAR(se.stop_num)
                   END) AS stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_name, c.zipc AS zip
             FROM load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx
            WHERE ld.div_part = l_div_part
              AND ld.llr_dt = l_llr_dt
              AND ld.load_num = i_load_num
              AND se.div_part = ld.div_part
              AND se.load_depart_sid = ld.load_depart_sid
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = se.div_part
                            AND a.load_depart_sid = se.load_depart_sid
                            AND a.custa = se.cust_id
                            AND a.dsorda = 'R'
                            AND a.stata <> 'C')
              AND c.div_part = se.div_part
              AND c.acnoc = se.cust_id
              AND cx.div_part = c.div_part
              AND cx.custb = c.acnoc
         ORDER BY 1;
    END IF;   -- i_hist_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEARCH_TYP_LIST_FN
  ||  Return cursor of search type options.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION search_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_COPY_ORDERS_PK.SEARCH_TYP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_t_search_typs      type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_search_typs := type_stab(RPAD(g_c_srch_ord_num, 10) || 'Order No.',
                                 RPAD(g_c_srch_cust, 10) || 'Customer No.',
                                 RPAD(g_c_srch_llr_load_stp, 10) || 'LLR/Load/Stop'
                                );
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT RTRIM(SUBSTR(t.column_value, 1, 10)) AS typ, SUBSTR(t.column_value, 11) AS descr
         FROM TABLE(CAST(l_t_search_typs AS type_stab)) t;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END search_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORDER_DETAIL_FN
  ||  Return cursor of order detail.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  || 06/16/08 | rhalpai | Added sort by Load/Stop/OrdNum to cursors.
  || 01/14/09 | rhalpai | Changed logic to exclude cancelled orders and to
  ||                    | exclude reserved loads from cursor. IM466947
  || 08/26/10 | rhalpai | Change cursor to use weight and cube from CorpItem
  ||                    | table SAWP505E instead of from OrderDetail table.
  ||                    | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 07/16/12 | rhalpai | Change logic to improve efficiency by using ord hdr
  ||                    | as driver within cursors. PIR11044
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION order_detail_fn(
    i_div         IN  VARCHAR2,
    i_search_typ  IN  VARCHAR2,
    i_parm_list   IN  VARCHAR2,
    i_hist_sw     IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_COPY_ORDERS_PK.ORDER_DETAIL_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;

    PROCEDURE ord_num_sp(
      i_div_part  IN      NUMBER,
      i_ord_num   IN      NUMBER,
      i_hist_sw   IN      VARCHAR2,
      o_cur       OUT     SYS_REFCURSOR
    ) IS
    BEGIN
      OPEN o_cur
       FOR
         SELECT   x.load_num,(CASE
                                WHEN x.stop_num < 10 THEN '0' || x.stop_num
                                ELSE TO_CHAR(x.stop_num)
                              END) AS stop_num, x.load_typ, x.ord_num, DECODE(x.ord_typ, 'D', 'DIS', 'REG') AS ord_typ,
                  x.po_num, TO_CHAR(x.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                  TO_CHAR(x.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                  TO_CHAR(x.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  TO_CHAR(ROUND(SUM(e.wghte * x.orig_qty), 1), 'FM999999990.0') AS prod_wt,
                  TO_CHAR(ROUND(SUM(e.cubee * x.orig_qty), 1), 'FM999999990.0') AS prod_cube,
                  DECODE(x.ord_typ, 'T', 'Y', 'N') AS test_sw
             FROM (SELECT ld.load_num, se.stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num, a.dsorda AS ord_typ,
                          a.cpoa AS po_num, ld.llr_ts, ld.depart_ts, se.eta_ts, b.orgqtb AS orig_qty,
                          b.orditb AS catlg_num
                     FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se, ordp120b b
                    WHERE a.div_part = i_div_part
                      AND a.ordnoa = i_ord_num
                      AND ld.div_part = a.div_part
                      AND ld.load_depart_sid = a.load_depart_sid
                      AND ld.load_num NOT IN(SELECT t.column_value
                                               FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND se.div_part = a.div_part
                      AND se.load_depart_sid = a.load_depart_sid
                      AND se.cust_id = a.custa
                      AND a.dsorda IN('R', 'T')
                      AND a.stata <> 'C'
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)
                   UNION ALL
                   SELECT a.orrtea AS load_num, a.stopsa AS stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num,
                          a.dsorda AS ord_typ, a.cpoa AS po_num,
                          TO_DATE('19000228' || LPAD(a.ctofta, 4, '0'), 'YYYYMMDDHH24MI') + a.ctofda AS llr_ts,
                          TO_DATE('19000228' || LPAD(b.deptmb, 4, '0'), 'YYYYMMDDHH24MI') + b.depdtb AS depart_ts,
                          TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta AS eta_ts,
                          b.orgqtb AS orig_qty, b.orditb AS catlg_num
                     FROM ordp900a a, ordp920b b
                    WHERE i_hist_sw = 'Y'
                      AND a.div_part = i_div_part
                      AND a.ordnoa = i_ord_num
                      AND a.dsorda IN('R', 'T')
                      AND a.stata <> 'C'
                      AND a.orrtea NOT IN(SELECT t.column_value
                                            FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) x,
                  sawp505e e
            WHERE e.catite = x.catlg_num
         GROUP BY x.load_num, x.stop_num, x.load_typ, x.ord_num, x.ord_typ, x.po_num, x.llr_ts, x.depart_ts, x.eta_ts
         ORDER BY x.load_num, x.stop_num, x.ord_num;
    END ord_num_sp;

    PROCEDURE cust_sp(
      i_div_part   IN      NUMBER,
      i_cust_list  IN      VARCHAR2,
      i_hist_sw    IN      VARCHAR2,
      o_cur        OUT     SYS_REFCURSOR
    ) IS
      l_t_fields  type_stab;
      l_cust_typ  VARCHAR2(3);
      l_cust      sysp200c.acnoc%TYPE;
      l_cust_id   sysp200c.acnoc%TYPE;
    BEGIN
      logs.dbg('Parse Parm Field List for Cust');
      l_t_fields := str.parse_list(i_cust_list, op_const_pk.field_delimiter);

      IF l_t_fields IS NOT NULL THEN
        l_cust_typ := l_t_fields(1);
        l_cust := l_t_fields(2);
        l_cust_id :=(CASE l_cust_typ
                       WHEN 'MCL' THEN csr_customers_pk.cbr_cust_fn(i_div_part, l_cust)
                       ELSE l_cust
                     END);
      END IF;   -- l_t_fields IS NOT NULL

      OPEN o_cur
       FOR
         SELECT   x.load_num,(CASE
                                WHEN x.stop_num < 10 THEN '0' || x.stop_num
                                ELSE TO_CHAR(x.stop_num)
                              END) AS stop_num, x.load_typ, x.ord_num, DECODE(x.ord_typ, 'D', 'DIS', 'REG') AS ord_typ,
                  x.po_num, TO_CHAR(x.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                  TO_CHAR(x.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                  TO_CHAR(x.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  TO_CHAR(ROUND(SUM(e.wghte * x.orig_qty), 1), 'FM999999990.0') AS prod_wt,
                  TO_CHAR(ROUND(SUM(e.cubee * x.orig_qty), 1), 'FM999999990.0') AS prod_cube,
                  DECODE(x.ord_typ, 'T', 'Y', 'N') AS test_sw
             FROM (SELECT ld.load_num, se.stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num, a.dsorda AS ord_typ,
                          a.cpoa AS po_num, ld.llr_ts, ld.depart_ts, se.eta_ts, b.orgqtb AS orig_qty,
                          b.orditb AS catlg_num
                     FROM stop_eta_op1g se, load_depart_op1f ld, ordp100a a, ordp120b b
                    WHERE se.div_part = i_div_part
                      AND se.cust_id = l_cust_id
                      AND ld.div_part = se.div_part
                      AND ld.load_depart_sid = se.load_depart_sid
                      AND ld.load_num NOT IN(SELECT t.column_value
                                               FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND a.div_part = se.div_part
                      AND a.load_depart_sid = se.load_depart_sid
                      AND a.custa = se.cust_id
                      AND a.dsorda IN('R', 'T')
                      AND a.stata <> 'C'
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)
                   UNION ALL
                   SELECT a.orrtea AS load_num, a.stopsa AS stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num,
                          a.dsorda AS ord_typ, a.cpoa AS po_num,
                          TO_DATE('19000228' || LPAD(a.ctofta, 4, '0'), 'YYYYMMDDHH24MI') + a.ctofda AS llr_ts,
                          TO_DATE('19000228' || LPAD(b.deptmb, 4, '0'), 'YYYYMMDDHH24MI') + b.depdtb AS depart_ts,
                          TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta AS eta_ts,
                          b.orgqtb AS orig_qty, b.orditb AS catlg_num
                     FROM ordp900a a, ordp920b b
                    WHERE i_hist_sw = 'Y'
                      AND a.div_part = i_div_part
                      AND a.custa = l_cust_id
                      AND a.dsorda IN('R', 'T')
                      AND a.stata <> 'C'
                      AND a.orrtea NOT IN(SELECT t.column_value
                                            FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) x,
                  sawp505e e
            WHERE e.catite = x.catlg_num
         GROUP BY x.load_num, x.stop_num, x.load_typ, x.ord_num, x.ord_typ, x.po_num, x.llr_ts, x.depart_ts, x.eta_ts
         ORDER BY x.load_num, x.stop_num, x.ord_num;
    END cust_sp;

    PROCEDURE llr_load_stop_sp(
      i_div_part            IN      NUMBER,
      i_llr_load_stop_list  IN      VARCHAR2,
      i_hist_sw             IN      VARCHAR2,
      o_cur                 OUT     SYS_REFCURSOR
    ) IS
      l_t_fields  type_stab;
      l_llr_dt    DATE;
      l_llr_num   NUMBER;
      l_load_num  mclp120c.loadc%TYPE;
      l_stop_num  NUMBER;
    BEGIN
      logs.dbg('Parse Parm Field List for LLR/Load/Stop');
      l_t_fields := str.parse_list(i_llr_load_stop_list, op_const_pk.field_delimiter);

      IF l_t_fields IS NOT NULL THEN
        l_llr_dt := TO_DATE(l_t_fields(1), g_c_date_fmt);
        l_llr_num := l_llr_dt - DATE '1900-02-28';
        l_load_num := l_t_fields(2);
        l_stop_num := l_t_fields(3);
      END IF;   -- l_t_fields IS NOT NULL

      OPEN o_cur
       FOR
         SELECT   x.load_num,(CASE
                                WHEN x.stop_num < 10 THEN '0' || x.stop_num
                                ELSE TO_CHAR(x.stop_num)
                              END) AS stop_num, x.load_typ, x.ord_num, DECODE(x.ord_typ, 'D', 'DIS', 'REG') AS ord_typ,
                  x.po_num, TO_CHAR(x.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                  TO_CHAR(x.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                  TO_CHAR(x.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  TO_CHAR(ROUND(SUM(e.wghte * x.orig_qty), 1), 'FM999999990.0') AS prod_wt,
                  TO_CHAR(ROUND(SUM(e.cubee * x.orig_qty), 1), 'FM999999990.0') AS prod_cube,
                  DECODE(x.ord_typ, 'T', 'Y', 'N') AS test_sw
             FROM (SELECT ld.load_num, se.stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num, a.dsorda AS ord_typ,
                          a.cpoa AS po_num, ld.llr_ts, ld.depart_ts, se.eta_ts, b.orgqtb AS orig_qty,
                          b.orditb AS catlg_num
                     FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
                    WHERE ld.div_part = i_div_part
                      AND ld.llr_dt = l_llr_dt
                      AND ld.load_num = l_load_num
                      AND ld.load_num NOT IN(SELECT t.column_value
                                               FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND se.div_part = ld.div_part
                      AND se.load_depart_sid = ld.load_depart_sid
                      AND se.stop_num = l_stop_num
                      AND a.div_part = se.div_part
                      AND a.load_depart_sid = se.load_depart_sid
                      AND a.custa = se.cust_id
                      AND a.dsorda IN('R', 'T')
                      AND a.stata <> 'C'
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)
                   UNION ALL
                   SELECT a.orrtea AS load_num, a.stopsa AS stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num,
                          a.dsorda AS ord_typ, a.cpoa AS po_num,
                          TO_DATE('19000228' || LPAD(a.ctofta, 4, '0'), 'YYYYMMDDHH24MI') + a.ctofda AS llr_ts,
                          TO_DATE('19000228' || LPAD(b.deptmb, 4, '0'), 'YYYYMMDDHH24MI') + b.depdtb AS depart_ts,
                          TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta AS eta_ts,
                          b.orgqtb AS orig_qty, b.orditb AS catlg_num
                     FROM ordp900a a, ordp920b b
                    WHERE i_hist_sw = 'Y'
                      AND a.div_part = i_div_part
                      AND a.ctofda = l_llr_num
                      AND a.orrtea = l_load_num
                      AND a.orrtea NOT IN(SELECT t.column_value
                                            FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                      AND a.stopsa = l_stop_num
                      AND a.dsorda IN('R', 'T')
                      AND a.stata <> 'C'
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) x,
                  sawp505e e
            WHERE e.catite = x.catlg_num
         GROUP BY x.load_num, x.stop_num, x.load_typ, x.ord_num, x.ord_typ, x.po_num, x.llr_ts, x.depart_ts, x.eta_ts
         ORDER BY x.load_num, x.stop_num, x.ord_num;
    END llr_load_stop_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'SearchTyp', i_search_typ);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    CASE i_search_typ
      WHEN g_c_srch_ord_num THEN
        ord_num_sp(l_div_part, i_parm_list, UPPER(i_hist_sw), l_cv);
      WHEN g_c_srch_cust THEN
        cust_sp(l_div_part, i_parm_list, UPPER(i_hist_sw), l_cv);
      WHEN g_c_srch_llr_load_stp THEN
        llr_load_stop_sp(l_div_part, i_parm_list, UPPER(i_hist_sw), l_cv);
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END order_detail_fn;

  /*
  ||----------------------------------------------------------------------------
  || COPY_SP
  ||  Copy order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/30/07 | rhalpai | Original. PIR3593
  || 07/16/12 | rhalpai | Change logic to improve readability. PIR11044
  || 01/20/12 | rhalpai | Change logic to match new references to fields in
  ||                    | CSR_ORDERS_PK.G_RT_MSG_HDR.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE copy_sp(
    i_div          IN      VARCHAR2,
    i_ord_list     IN      VARCHAR2,
    i_user_id      IN      VARCHAR2,
    o_msg          OUT     VARCHAR2,
    i_hist_sw      IN      VARCHAR2 DEFAULT 'N',
    i_cust_typ     IN      VARCHAR2 DEFAULT 'MCL',
    i_new_cust     IN      VARCHAR2 DEFAULT NULL,
    i_ovrrd_po_sw  IN      VARCHAR2 DEFAULT 'N',
    i_new_po       IN      VARCHAR2 DEFAULT '~',
    i_create_typ   IN      VARCHAR2 DEFAULT csr_orders_pk.g_c_copy_order
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'OP_COPY_ORDERS_PK.COPY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_new_mcl_cust       mclp020b.mccusb%TYPE;
    l_new_cbr_cust       sysp200c.acnoc%TYPE;
    l_t_ords             type_ntab;
    l_ord_cnt            PLS_INTEGER                 := 0;
    l_r_ord_hdr          csr_orders_pk.g_rt_msg_hdr;
    l_t_ord_dtls         csr_orders_pk.g_tt_msg_dtls;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdList', i_ord_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.add_parm(lar_parm, 'CustTyp', i_cust_typ);
    logs.add_parm(lar_parm, 'NewCust', i_new_cust);
    logs.add_parm(lar_parm, 'OvrrdPO', i_ovrrd_po_sw);
    logs.add_parm(lar_parm, 'NewPO', i_new_po);
    logs.add_parm(lar_parm, 'CreateTyp', i_create_typ);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Parse Order List');
    l_t_ords := num.parse_list(i_ord_list, op_const_pk.field_delimiter);

    IF l_t_ords.COUNT > 0 THEN
      IF i_new_cust IS NOT NULL THEN
        logs.dbg('Open Cursor for New Cust Nums');

        OPEN l_cv
         FOR
           SELECT cx.mccusb, cx.custb
             FROM mclp020b cx
            WHERE cx.div_part = l_div_part
              AND i_new_cust = DECODE(i_cust_typ, 'MCL', cx.mccusb, 'CBR', cx.custb);

        logs.dbg('Fetch Cursor for New Cust Nums');

        FETCH l_cv
         INTO l_new_mcl_cust, l_new_cbr_cust;
      END IF;   -- i_new_cust IS NOT NULL

      FOR i IN l_t_ords.FIRST .. l_t_ords.LAST LOOP
        logs.dbg('Get Order Header');
        l_r_ord_hdr := ord_hdr_fn(l_div_part, l_t_ords(i), i_create_typ);

        IF i_ovrrd_po_sw = 'Y' THEN
          l_r_ord_hdr.po_num := i_new_po;
        END IF;   -- i_ovrrd_po_sw = 'Y'

        IF l_r_ord_hdr.conf_num IS NOT NULL THEN
          IF l_new_mcl_cust IS NOT NULL THEN
            l_r_ord_hdr.mcl_cust := l_new_mcl_cust;
            l_r_ord_hdr.cust_id := l_new_cbr_cust;
          END IF;   -- l_new_mcl_cust IS NOT NULL

          logs.dbg('Add CPY_ORD_CP3O Entry');

          INSERT INTO cpy_ord_cp3o
                      (conf_num, orig_ord_num
                      )
               VALUES (l_r_ord_hdr.conf_num, l_t_ords(i)
                      );

          logs.dbg('Get Order Details');
          l_t_ord_dtls := ord_dtls_fn(l_div_part, l_t_ords(i), i_create_typ);
          logs.dbg('Insert Order and Send Order to Mainframe');
          -- procedure contains internal commit/rollback
          csr_orders_pk.ins_ord_sp(l_r_ord_hdr, l_t_ord_dtls, i_user_id);
          l_ord_cnt := l_ord_cnt + 1;
        END IF;   -- l_r_ord_hdr.conf_num IS NOT NULL
      END LOOP;
    END IF;   -- l_t_ords.COUNT > 0

    IF l_ord_cnt > 0 THEN
      o_msg := op_const_pk.msg_typ_info || op_const_pk.field_delimiter || l_ord_cnt || ' order(s) recreated.';
    ELSE
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'No orders available to recreate.';
    END IF;   -- l_ord_cnt > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END copy_sp;
END op_copy_orders_pk;
/

