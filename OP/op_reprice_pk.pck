CREATE OR REPLACE PACKAGE op_reprice_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_mass       CONSTANT VARCHAR2(4)  := 'MASS';
  g_c_dist       CONSTANT VARCHAR2(4)  := 'DIST';
  g_c_othr       CONSTANT VARCHAR2(4)  := 'OTHR';
  g_c_load       CONSTANT VARCHAR2(4)  := 'LOAD';
  g_c_load_stop  CONSTANT VARCHAR2(4)  := 'LDST';
  g_c_cust       CONSTANT VARCHAR2(4)  := 'CUST';
  g_c_item       CONSTANT VARCHAR2(4)  := 'ITEM';
  g_c_cust_item  CONSTANT VARCHAR2(4)  := 'CSIT';
  g_c_ord_num    CONSTANT VARCHAR2(4)  := 'ORDN';
  g_c_ord_ln     CONSTANT VARCHAR2(4)  := 'ORDL';
  g_c_real_time  CONSTANT VARCHAR2(4)  := 'REAL';
  g_c_batch_ftp  CONSTANT VARCHAR2(3)  := 'FTP';
  g_c_batch_mq   CONSTANT VARCHAR2(2)  := 'MQ';
  g_c_date_fmt   CONSTANT VARCHAR2(10) := 'YYYY-MM-DD';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION type_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_fn(
    i_div         IN  VARCHAR2,
    i_llr_dt_fro  IN  VARCHAR2,
    i_llr_dt_to   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION pricing_ord_list_fn(
    i_div         IN  VARCHAR2,
    i_llr_dt_fro  IN  VARCHAR2,
    i_llr_dt_to   IN  VARCHAR2,
    i_mcl_cust    IN  VARCHAR2 DEFAULT NULL,
    i_catlg_num   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION qoprc01_msg_fn(
    i_div             IN  VARCHAR2,
    i_pricing_typ     IN  VARCHAR2,
    i_ord_num         IN  NUMBER,
    i_ord_ln          IN  NUMBER,
    i_ord_typ         IN  VARCHAR2,
    i_load_typ        IN  VARCHAR2,
    i_cust_id         IN  VARCHAR2,
    i_mcl_cust        IN  VARCHAR2,
    i_invc_dt         IN  VARCHAR2,
    i_mcl_item        IN  VARCHAR2,
    i_item_num        IN  VARCHAR2,
    i_uom             IN  VARCHAR2,
    i_hard_rtl_sw     IN  VARCHAR2,
    i_rtl_amt         IN  NUMBER,
    i_rtl_mult        IN  NUMBER,
    i_hard_price_sw   IN  VARCHAR2,
    i_price_amt       IN  NUMBER,
    i_kit_sw          IN  VARCHAR2,
    i_item_pass_area  IN  VARCHAR2,
    i_legcy_ref       IN  VARCHAR2
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE reprice_bulk_sp(
    i_div          IN  VARCHAR2,
    i_pricing_typ  IN  VARCHAR2,
    i_extr_dest    IN  VARCHAR2 DEFAULT g_c_real_time,
    i_parm_list    IN  CLOB DEFAULT NULL
  );

  PROCEDURE reprice_ord_ln_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  );

  PROCEDURE reprice_update_sp(
    i_div  IN  VARCHAR2
  );
END op_reprice_pk;
/

CREATE OR REPLACE PACKAGE BODY op_reprice_pk IS
  /*
  ||----------------------------------------------------------------------------
  ||  Package-Level Changes
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/17/07 | rhalpai | Changed reprice_mass_cur calculation for hours since
  ||                    | last pricing to correct issue with order lines
  ||                    | recently repriced being included. IM281302
  || 07/16/09 | CNATIVI | Changed "current reprice ts" to include HH24MISS
  ||                    | (iso HH24) - consistent with "last reprice ts" calc
  ||                    | Change to inv_date format from etadtb to varchar2
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || IS_SUB_LINE_FN
  ||  Indicate whether order line passed is a sub line (contains decimal).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_sub_line_fn(
    i_ord_ln  IN  NUMBER
  )
    RETURN BOOLEAN IS
  BEGIN
    RETURN(i_ord_ln > FLOOR(i_ord_ln));
  END is_sub_line_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_ORD_LN_AVAIL_FN
  ||  Indicate whether order line is available..
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/14/10 | rhalpai | Move from old ORDER_LINE_UPDATE_SP to stand-alone
  ||                    | function. PIR7043
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_ord_ln_avail_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.IS_ORD_LN_AVAIL_FN';
    lar_parm             logs.tar_parm;
    l_is_sub_line        BOOLEAN;
    l_cv                 SYS_REFCURSOR;

    TYPE l_tt_row_id IS TABLE OF ROWID;

    l_t_row_id           l_tt_row_id;
    l_row_cnt            PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Check order line is for sub');
    l_is_sub_line := is_sub_line_fn(i_ord_ln);
    logs.dbg('Open Cursor');

    -- Also lock orig order line for sub lines
    OPEN l_cv
     FOR
       SELECT     ROWID
             FROM ordp120b b
            WHERE b.div_part = i_div_part
              AND b.ordnob = i_ord_num
              AND FLOOR(b.lineb) = FLOOR(i_ord_ln)
              AND b.statb IN('O', 'P')
       FOR UPDATE NOWAIT;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
    BULK COLLECT INTO l_t_row_id;

    l_row_cnt := l_cv%ROWCOUNT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(   (    NOT l_is_sub_line
               AND l_row_cnt > 0)
           OR (    l_is_sub_line
               AND l_row_cnt > 1));
  EXCEPTION
    WHEN excp.gx_row_locked THEN
      logs.warn('Order Line unavailable for Reprice updates.', lar_parm);
      RETURN(FALSE);
  END is_ord_ln_avail_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_TMP_MASS_SP
  ||  Populate temp table with order lines for MASS Reprice.
  ||  The count of inserted rows will be returned in the out parameter.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/26/13 | rhalpai | Change logic for invoice date to use current date in
  ||                    | place of 1900-01-01. IM-119664
  || 10/17/13 | wzrobin | Change calc to use hours instead of days. IM-122810
  || 04/01/14 | rhalpai | Change logic to include all exception distributions.
  ||                    | PIR13694
  || 07/03/14 | rhalpai | Change logic exclude Test orders and orders on DIST
  ||                    | or P##P loads. PIR14009
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_tmp_mass_sp(
    i_div_part  IN      NUMBER,
    o_cnt       OUT     PLS_INTEGER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_TMP_MASS_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_min_llr_ts          DATE;
    l_min_price_ts        DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Div Info');

    SELECT l_c_sysdate + NVL(llr_future_hrs, 24) / 24, l_c_sysdate - NVL(hrs_since_prc_ts, 8) / 24
      INTO l_min_llr_ts, l_min_price_ts
      FROM mclp130d
     WHERE div_part = i_div_part;

    logs.dbg('Empty Temp Table');

    DELETE FROM temp_reprice_orders;

    logs.dbg('Load MASS Orders');

    INSERT INTO temp_reprice_orders
                (cursor_name, ord_num, ord_line_num, sub_ind, ord_type, load_type, cbr_cust, mclane_cust, inv_date,
                 order_item, catlg_item, cbr_item, cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl, hard_price_ind,
                 passed_price, display_ind, item_pass_area, gmp_ind, distrib_id)
      SELECT DECODE(b.excptn_sw, 'N', 'ORDERS_ORDP120B', 'Y', 'ORDERS_WHSP120B') AS cur_nm, b.ordnob, b.lineb,
             DECODE(b.lineb - FLOOR(b.lineb), 0, 'N', 'Y'), DECODE(a.dsorda, 'D', 'DIS', 'REG'), NVL(a.ldtypa, 'GRO'),
             lpad_fn(se.cust_id, 8), lpad_fn(cx.mccusb, 6, '0'),
             rpad_fn(TO_CHAR((CASE
                                WHEN(    a.dsorda = 'D'
                                     AND (   ld.load_num = 'DIST'
                                          OR ld.load_num BETWEEN 'P00P' AND 'P99P')) THEN DATE '1900-02-28' + a.shpja
                                ELSE DECODE(se.eta_ts, DATE '1900-01-01', TRUNC(l_c_sysdate), TRUNC(se.eta_ts))
                              END
                             ),
                             'YYYYMMDD'
                            ),
                     8
                    ) AS invc_dt,
             lpad_fn(e.catite, 6, '0'), lpad_fn(e.catite, 6, '0'), lpad_fn(e.iteme, 9, '0'), rpad_fn(e.uome, 3),
             DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0),
             DECODE(b.prfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdprcb, 0), DECODE(e.kite, '1', 'Y', 'Y', 'Y', 'N'),
             rpad_fn(b.itpasb, 20), DECODE(SUBSTR(e.uome, 1, 2), 'GM', 'Y', 'N'), rpad_fn(a.legrfa, 10)
        FROM load_depart_op1f ld, ordp100a a, ordp120b b, mclp020b cx, stop_eta_op1g se, sawp505e e, mclp110b di
       WHERE ld.div_part = i_div_part
         AND ld.load_num <> 'DIST'
         AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
         AND l_min_llr_ts > ld.llr_ts
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.dsorda IN('R', 'D')
         AND NOT EXISTS(SELECT 1
                          FROM sub_prcs_ord_src s
                         WHERE s.prcs_id = 'REPRICE'
                           AND s.prcs_sbtyp_cd = 'BRP'
                           AND s.div_part = a.div_part
                           AND s.ord_src = a.ipdtsa)
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND l_min_price_ts > TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb
         AND (   (    b.excptn_sw = 'N'
                  AND b.ntshpb IS NULL)
              OR (    b.excptn_sw = 'Y'
                  AND b.ntshpb IN(SELECT ma.rsncda
                                    FROM mclp140a ma
                                   WHERE ma.rsntpa = 99)
                  AND b.subrcb = 0)
             )
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa
         AND se.div_part = a.div_part
         AND se.load_depart_sid = a.load_depart_sid
         AND se.cust_id = a.custa
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND di.div_part = ld.div_part
         AND di.itemb = e.iteme
         AND di.uomb = e.uome;

    o_cnt := SQL%ROWCOUNT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_tmp_mass_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_TMP_DIST_SP
  ||  Populate temp table with order lines for Distribution Reprice.
  ||  The count of inserted rows will be returned in the out parameter.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_tmp_dist_sp(
    i_div_part  IN      NUMBER,
    o_cnt       OUT     PLS_INTEGER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_TMP_DIST_SP';
    lar_parm             logs.tar_parm;
    l_curr_rendate       PLS_INTEGER;
    l_max_shp_dt         PLS_INTEGER;
    l_max_price_dt       PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    l_curr_rendate := TRUNC(SYSDATE) - DATE '1900-02-28';
    l_max_price_dt := l_curr_rendate - 1;
    logs.dbg('Get Div Info');

    SELECT l_curr_rendate + dislad
      INTO l_max_shp_dt
      FROM mclp130d
     WHERE div_part = i_div_part;

    logs.dbg('Empty Temp Table');

    DELETE FROM temp_reprice_orders;

    logs.dbg('Load DIST Orders');

    INSERT INTO temp_reprice_orders
                (cursor_name, ord_num, ord_line_num, sub_ind, ord_type, load_type, cbr_cust, mclane_cust, inv_date,
                 order_item, catlg_item, cbr_item, cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl, hard_price_ind,
                 passed_price, display_ind, item_pass_area, gmp_ind, distrib_id)
      SELECT DECODE(b.excptn_sw, 'N', 'ORDERS_ORDP120B', 'Y', 'ORDERS_WHSP120B') AS cur_nm, b.ordnob, b.lineb,
             DECODE(b.lineb - FLOOR(b.lineb), 0, 'N', 'Y'), DECODE(a.dsorda, 'D', 'DIS', 'REG'), NVL(a.ldtypa, 'GRO'),
             lpad_fn(a.custa, 8), lpad_fn(cx.mccusb, 6, '0'),
             TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYYMMDD') AS invc_dt, lpad_fn(e.catite, 6, '0'),
             lpad_fn(e.catite, 6, '0'), lpad_fn(e.iteme, 9, '0'), rpad_fn(e.uome, 3),
             DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0),
             DECODE(b.prfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdprcb, 0), DECODE(e.kite, '1', 'Y', 'Y', 'Y', 'N'),
             rpad_fn(b.itpasb, 20), DECODE(SUBSTR(e.uome, 1, 2), 'GM', 'Y', 'N'), rpad_fn(a.legrfa, 10)
        FROM load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e, mclp110b di, mclp020b cx
       WHERE ld.div_part = i_div_part
         AND ld.llr_ts = DATE '1900-01-01'
         AND (   ld.load_num = 'DIST'
              OR ld.load_num BETWEEN 'P00P' AND 'P99P')
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.div_part = ld.div_part
         AND a.dsorda = 'D'
         AND a.shpja < l_max_shp_dt
         AND NOT EXISTS(SELECT 1
                          FROM sub_prcs_ord_src s
                         WHERE s.prcs_id = 'REPRICE'
                           AND s.prcs_sbtyp_cd = 'BRP'
                           AND s.div_part = a.div_part
                           AND s.ord_src = a.ipdtsa)
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND (   (    b.excptn_sw = 'N'
                  AND b.ntshpb IS NULL)
              OR (    b.excptn_sw = 'Y'
                  AND b.ntshpb IN(SELECT ma.rsncda
                                    FROM mclp140a ma
                                   WHERE ma.rsntpa = 99)
                  AND b.subrcb = 0)
             )
         AND b.prstdb < l_max_price_dt
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND di.div_part = ld.div_part
         AND di.itemb = e.iteme
         AND di.uomb = e.uome
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa;

    o_cnt := SQL%ROWCOUNT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_tmp_dist_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_TMP_OTHR_SP
  ||  Populate temp table with order lines for OTHR Reprice.
  ||  The count of inserted rows will be returned in the out parameter.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/06/08 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/26/13 | rhalpai | Change logic for invoice date to use current date in
  ||                    | place of 1900-01-01. IM-119664
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_tmp_othr_sp(
    i_div_part  IN      NUMBER,
    o_cnt       OUT     PLS_INTEGER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_TMP_OTHR_SP';
    lar_parm              logs.tar_parm;
    l_mq_msg_status       VARCHAR2(3)   := 'WRK';
    l_c_sysdate  CONSTANT DATE          := SYSDATE;

    PROCEDURE upd_mq_msgs_sp IS
    BEGIN
      UPDATE mclane_mq_get
         SET last_chg_ts = l_c_sysdate,
             mq_msg_status = l_mq_msg_status
       WHERE mq_msg_status = 'WRK'
         AND mq_msg_id = 'QOPRC03'
         AND div_part = i_div_part;
    END upd_mq_msgs_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    o_cnt := 0;
    logs.dbg('Set MQ Msgs to WRK Status');

    UPDATE mclane_mq_get g
       SET g.mq_msg_status = 'WRK'
     WHERE g.mq_msg_id = 'QOPRC03'
       AND g.mq_msg_status = 'OPN'
       AND g.div_part = i_div_part;

    IF SQL%ROWCOUNT > 0 THEN
      COMMIT;
      logs.dbg('Empty RepriceOrders Temp Table');

      DELETE FROM temp_reprice_orders;

      SAVEPOINT mclane_mq_get_wrk;
      logs.dbg('Load OTHR Orders');

      INSERT INTO temp_reprice_parm
                  (reprice_typ, catlg_item, cbr_item, cbr_item_uom, load_num, stop_num, ord_num, ord_ln_num)
        SELECT DISTINCT (CASE
                           WHEN(    TO_NUMBER(SUBSTR(g.mq_msg_data, 82, 8)) > 0
                                AND TO_NUMBER(SUBSTR(g.mq_msg_data, 91, 7) || '.' || SUBSTR(g.mq_msg_data, 99, 1)) > 0
                               ) THEN 'ORDL'
                           WHEN TO_NUMBER(SUBSTR(g.mq_msg_data, 82, 8)) > 0 THEN 'ORD#'
                           WHEN(    RTRIM(SUBSTR(g.mq_msg_data, 73, 4)) IS NOT NULL
                                AND RTRIM(SUBSTR(g.mq_msg_data, 78, 2)) IS NOT NULL
                               ) THEN 'LDST'
                           WHEN RTRIM(SUBSTR(g.mq_msg_data, 73, 4)) IS NOT NULL THEN 'LOAD'
                           WHEN(   TO_NUMBER(SUBSTR(g.mq_msg_data, 66, 6)) > 0
                                OR TO_NUMBER(SUBSTR(g.mq_msg_data, 54, 9)) > 0
                               ) THEN 'ITEM'
                         END
                        ) AS reprice_typ,
                        SUBSTR(g.mq_msg_data, 66, 6) AS catlg_item, SUBSTR(g.mq_msg_data, 54, 9) AS cbr_item,
                        RTRIM(SUBSTR(g.mq_msg_data, 63, 3)) AS cbr_item_uom, SUBSTR(g.mq_msg_data, 73, 4) AS load_num,
                        TO_NUMBER(TRIM(SUBSTR(g.mq_msg_data, 78, 2))) AS stop_num,
                        TO_NUMBER(TRIM(SUBSTR(g.mq_msg_data, 82, 8))) AS ord_num,
                        TO_NUMBER(NVL(TRIM(SUBSTR(g.mq_msg_data, 91, 7)), '0')
                                  || '.'
                                  || NVL(TRIM(SUBSTR(g.mq_msg_data, 99, 1)), '0')
                                 ) AS ord_ln_num
                   FROM mclane_mq_get g
                  WHERE g.div_part = i_div_part
                    AND g.mq_msg_id = 'QOPRC03'
                    AND g.mq_msg_status = 'WRK';

      logs.dbg('Add Reprice Orders for QOPRC03');

      INSERT INTO temp_reprice_orders
                  (cursor_name, ord_num, ord_line_num, sub_ind, ord_type, load_type, cbr_cust, mclane_cust, inv_date,
                   order_item, catlg_item, cbr_item, cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl,
                   hard_price_ind, passed_price, display_ind, item_pass_area, gmp_ind, distrib_id)
        SELECT DECODE(b.excptn_sw, 'N', 'ORDERS_ORDP120B', 'Y', 'ORDERS_WHSP120B') AS cur_nm, b.ordnob, b.lineb,
               DECODE(b.lineb - FLOOR(b.lineb), 0, 'N', 'Y'), DECODE(a.dsorda, 'D', 'DIS', 'REG'), NVL(a.ldtypa, 'GRO'),
               lpad_fn(a.custa, 8), lpad_fn(cx.mccusb, 6, '0'),
               TO_CHAR((CASE
                          WHEN(    a.dsorda = 'D'
                               AND (   ld.load_num = 'DIST'
                                    OR ld.load_num BETWEEN 'P00P' AND 'P99P')) THEN DATE '1900-02-28' + a.shpja
                          ELSE DECODE(se.eta_ts, DATE '1900-01-01', TRUNC(l_c_sysdate), TRUNC(se.eta_ts))
                        END
                       ),
                       'YYYYMMDD'
                      ) AS invc_dt,
               lpad_fn(e.catite, 6, '0'), lpad_fn(e.catite, 6, '0'), lpad_fn(e.iteme, 9, '0'), rpad_fn(e.uome, 3),
               DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0),
               DECODE(b.prfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdprcb, 0), DECODE(e.kite, '1', 'Y', 'Y', 'Y', 'N'),
               rpad_fn(b.itpasb, 20), DECODE(SUBSTR(e.uome, 1, 2), 'GM', 'Y', 'N'), rpad_fn(a.legrfa, 10)
          FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp120b b, sawp505e e, mclp110b di
         WHERE EXISTS(SELECT 1
                        FROM temp_reprice_parm t
                       WHERE t.reprice_typ IN('ORDL', 'ORD#', 'LDST', 'LOAD', 'ITEM'))
           AND a.div_part = i_div_part
           AND NOT EXISTS(SELECT 1
                            FROM sub_prcs_ord_src s
                           WHERE s.prcs_id = 'REPRICE'
                             AND s.prcs_sbtyp_cd = 'BRP'
                             AND s.ord_src = a.ipdtsa
                             AND s.div_part = a.div_part)
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_reprice_parm p
                               WHERE p.reprice_typ = 'ORD#')
                OR a.ordnoa IN(SELECT p.ord_num
                                 FROM temp_reprice_parm p
                                WHERE p.reprice_typ = 'ORD#'))
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_reprice_parm p
                               WHERE p.reprice_typ = 'LOAD')
                OR ld.load_num IN(SELECT p.load_num
                                    FROM temp_reprice_parm p
                                   WHERE p.reprice_typ = 'LOAD'))
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_reprice_parm p
                               WHERE p.reprice_typ = 'LDST')
                OR (ld.load_num, se.stop_num) IN(SELECT p.load_num, p.stop_num
                                                   FROM temp_reprice_parm p
                                                  WHERE p.reprice_typ = 'LDST'))
           AND cx.div_part = se.div_part
           AND cx.custb = se.cust_id
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_reprice_parm p
                               WHERE p.reprice_typ = 'ORDL')
                OR (b.ordnob, b.lineb) IN(SELECT p.ord_num, p.ord_ln_num
                                            FROM temp_reprice_parm p
                                           WHERE p.reprice_typ = 'ORDL'))
           AND b.statb IN('O', 'P')
           AND (   (    b.excptn_sw = 'N'
                    AND b.ntshpb IS NULL)
                OR (    b.excptn_sw = 'Y'
                    AND b.ntshpb IN(SELECT ma.rsncda
                                      FROM mclp140a ma
                                     WHERE ma.rsntpa = 99)
                    AND b.subrcb = 0)
               )
           AND e.iteme = b.itemnb
           AND e.uome = b.sllumb
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_reprice_parm p
                               WHERE p.reprice_typ = 'ITEM')
                OR e.catite IN(SELECT p.catlg_item
                                 FROM temp_reprice_parm p
                                WHERE p.reprice_typ = 'ITEM')
                OR (b.itemnb, b.sllumb) IN(SELECT p.cbr_item, p.cbr_item_uom
                                             FROM temp_reprice_parm p
                                            WHERE p.reprice_typ = 'ITEM')
               )
           AND di.div_part = a.div_part
           AND di.itemb = e.iteme
           AND di.uomb = e.uome;

      o_cnt := SQL%ROWCOUNT;
      logs.dbg('Set MQ Msgs to CMP Status');
      l_mq_msg_status := 'CMP';
      upd_mq_msgs_sp;
      COMMIT;
    END IF;   -- SQL%ROWCOUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT mclane_mq_get_wrk;
      l_mq_msg_status := 'PRB';
      upd_mq_msgs_sp;
      COMMIT;
      logs.err(lar_parm);
  END ins_tmp_othr_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_PARM_TMP_SP
  ||  Populate parm temp table with parameters from UI for selecting order lines
  ||  to reprice.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_parm_tmp_sp(
    i_pricing_typ  IN  VARCHAR2,
    i_parm_list    IN  CLOB
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_PARM_TMP_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    logs.dbg('Empty Temp Table');

    DELETE FROM temp_reprice_parm;

    logs.dbg('Populate Temp Table');

    CASE i_pricing_typ
      WHEN g_c_load THEN
        INSERT INTO temp_reprice_parm
                    (reprice_typ, ord_num, load_num)
          SELECT i_pricing_typ AS reprice_typ, TO_DATE(t.column1, g_c_date_fmt) - DATE '1900-02-28' AS llr_num,
                 t.column2 AS load_num
            FROM TABLE(lob2table.separatedcolumns(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter)) t;
      WHEN g_c_load_stop THEN
        INSERT INTO temp_reprice_parm
                    (reprice_typ, ord_num, load_num, stop_num)
          SELECT i_pricing_typ AS reprice_typ, TO_DATE(t.column1, g_c_date_fmt) - DATE '1900-02-28' AS llr_num,
                 t.column2 AS load_num, TO_NUMBER(t.column3) AS stop_num
            FROM TABLE(lob2table.separatedcolumns(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter)) t;
      WHEN g_c_ord_num THEN
        INSERT INTO temp_reprice_parm
                    (reprice_typ, ord_num)
          SELECT i_pricing_typ AS reprice_typ, TO_NUMBER(t.column1) AS ord_num
            FROM TABLE(lob2table.separatedcolumns(i_parm_list, op_const_pk.grp_delimiter)) t;
      WHEN g_c_ord_ln THEN
        INSERT INTO temp_reprice_parm
                    (reprice_typ, ord_num, ord_ln_num)
          SELECT i_pricing_typ AS reprice_typ, TO_NUMBER(t.column1) AS ord_num, TO_NUMBER(t.column2) AS ord_ln
            FROM TABLE(lob2table.separatedcolumns(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter)) t;
      ELSE
        NULL;
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_parm_tmp_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_TMP_ORD_LN_SP
  ||  Populate temp table with order lines for OrdNum/OrdNum,OrdLn Reprice.
  ||  Selection may be a delimited list of OrderNums or OrderNum,Order Line.
  ||  The count of inserted rows will be returned in the out parameter.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/26/13 | rhalpai | Change logic for invoice date to use current date in
  ||                    | place of 1900-01-01. IM-119664
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_tmp_ord_ln_sp(
    i_div_part     IN      NUMBER,
    i_pricing_typ  IN      VARCHAR2,
    i_parm_list    IN      CLOB,
    o_cnt          OUT     PLS_INTEGER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_TMP_ORD_LN_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    o_cnt := 0;
    logs.dbg('Add Parms to Temp');
    ins_parm_tmp_sp(i_pricing_typ, i_parm_list);
    logs.dbg('Empty RepriceOrders Temp Table');

    DELETE FROM temp_reprice_orders;

    logs.dbg('Add Reprice Orders for OrdNum/OrdLn');

    INSERT INTO temp_reprice_orders
                (cursor_name, ord_num, ord_line_num, sub_ind, ord_type, load_type, cbr_cust, mclane_cust, inv_date,
                 order_item, catlg_item, cbr_item, cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl,
                 hard_price_ind, passed_price, display_ind, item_pass_area, gmp_ind, distrib_id)
      SELECT DECODE(b.excptn_sw, 'N', 'ORDERS_ORDP120B', 'Y', 'ORDERS_WHSP120B') AS cur_nm, b.ordnob, b.lineb,
             DECODE(b.lineb - FLOOR(b.lineb), 0, 'N', 'Y'), DECODE(a.dsorda, 'D', 'DIS', 'REG'), NVL(a.ldtypa, 'GRO'),
             lpad_fn(a.custa, 8), lpad_fn(cx.mccusb, 6, '0'),
             TO_CHAR((CASE
                        WHEN(    a.dsorda = 'D'
                             AND (   ld.load_num = 'DIST'
                                  OR ld.load_num BETWEEN 'P00P' AND 'P99P')) THEN DATE '1900-02-28' + a.shpja
                        ELSE DECODE(se.eta_ts, DATE '1900-01-01', TRUNC(l_c_sysdate), TRUNC(se.eta_ts))
                      END
                     ),
                     'YYYYMMDD'
                    ) AS invc_dt,
             lpad_fn(e.catite, 6, '0'), lpad_fn(e.catite, 6, '0'), lpad_fn(e.iteme, 9, '0'), rpad_fn(e.uome, 3),
             DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0),
             DECODE(b.prfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdprcb, 0), DECODE(e.kite, '1', 'Y', 'Y', 'Y', 'N'),
             rpad_fn(b.itpasb, 20), DECODE(SUBSTR(e.uome, 1, 2), 'GM', 'Y', 'N'), rpad_fn(a.legrfa, 10)
        FROM temp_reprice_parm p, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp120b b,
             sawp505e e, mclp110b di
       WHERE p.reprice_typ = i_pricing_typ
         AND a.div_part = i_div_part
         AND a.ordnoa = p.ord_num
         AND NOT EXISTS(SELECT 1
                          FROM sub_prcs_ord_src s
                         WHERE s.prcs_id = 'REPRICE'
                           AND s.prcs_sbtyp_cd = 'BRP'
                           AND s.div_part = a.div_part
                           AND s.ord_src = a.ipdtsa)
         AND ld.div_part = a.div_part
         AND ld.load_depart_sid = a.load_depart_sid
         AND se.div_part = a.div_part
         AND se.load_depart_sid = a.load_depart_sid
         AND se.cust_id = a.custa
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND (   p.reprice_typ = 'ORDN'
              OR (    p.reprice_typ = 'ORDL'
                  AND p.ord_ln_num = b.lineb))
         AND b.statb IN('O', 'P')
         AND (   (    b.excptn_sw = 'N'
                  AND b.ntshpb IS NULL)
              OR (    b.excptn_sw = 'Y'
                  AND b.ntshpb IN(SELECT ma.rsncda
                                    FROM mclp140a ma
                                   WHERE ma.rsntpa = 99)
                  AND b.subrcb = 0)
             )
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND di.div_part = a.div_part
         AND di.itemb = e.iteme
         AND di.uomb = e.uome;

    o_cnt := SQL%ROWCOUNT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_tmp_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_TMP_LOAD_STOP_SP
  ||  Populate temp table with order lines for LLR,Load/LLR,Load,Stop Reprice.
  ||  Selection may be a delimited list of LLR Date,Load or LLR Date,Load,Stop.
  ||  The count of inserted rows will be returned in the out parameter.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/26/13 | rhalpai | Change logic for invoice date to use current date in
  ||                    | place of 1900-01-01. IM-119664
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_tmp_load_stop_sp(
    i_div_part     IN      NUMBER,
    i_pricing_typ  IN      VARCHAR2,
    i_parm_list    IN      CLOB,
    o_cnt          OUT     PLS_INTEGER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_TMP_LOAD_STOP_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    o_cnt := 0;
    logs.dbg('Add Parms to Temp');
    ins_parm_tmp_sp(i_pricing_typ, i_parm_list);
    logs.dbg('Empty RepriceOrders Temp Table');

    DELETE FROM temp_reprice_orders;

    logs.dbg('Add Reprice Orders for Load/Stop');

    INSERT INTO temp_reprice_orders
                (cursor_name, ord_num, ord_line_num, sub_ind, ord_type, load_type, cbr_cust, mclane_cust, inv_date,
                 order_item, catlg_item, cbr_item, cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl,
                 hard_price_ind, passed_price, display_ind, item_pass_area, gmp_ind, distrib_id)
      SELECT DECODE(b.excptn_sw, 'N', 'ORDERS_ORDP120B', 'Y', 'ORDERS_WHSP120B') AS cur_nm, b.ordnob, b.lineb,
             DECODE(b.lineb - FLOOR(b.lineb), 0, 'N', 'Y'), DECODE(a.dsorda, 'D', 'DIS', 'REG'), NVL(a.ldtypa, 'GRO'),
             lpad_fn(a.custa, 8), lpad_fn(cx.mccusb, 6, '0'),
             TO_CHAR((CASE
                        WHEN(    a.dsorda = 'D'
                             AND (   ld.load_num = 'DIST'
                                  OR ld.load_num BETWEEN 'P00P' AND 'P99P')) THEN DATE '1900-02-28' + a.shpja
                        ELSE DECODE(se.eta_ts, DATE '1900-01-01', TRUNC(l_c_sysdate), TRUNC(se.eta_ts))
                      END
                     ),
                     'YYYYMMDD'
                    ) AS invc_dt,
             lpad_fn(e.catite, 6, '0'), lpad_fn(e.catite, 6, '0'), lpad_fn(e.iteme, 9, '0'), rpad_fn(e.uome, 3),
             DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0),
             DECODE(b.prfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdprcb, 0), DECODE(e.kite, '1', 'Y', 'Y', 'Y', 'N'),
             rpad_fn(b.itpasb, 20), DECODE(SUBSTR(e.uome, 1, 2), 'GM', 'Y', 'N'), rpad_fn(a.legrfa, 10)
        FROM temp_reprice_parm p, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, mclp020b cx, ordp120b b,
             sawp505e e, mclp110b di
       WHERE p.reprice_typ = i_pricing_typ
         AND ld.div_part = i_div_part
         AND ld.llr_dt = DATE '1900-02-28' + p.ord_num
         AND ld.load_num = p.load_num
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND NOT EXISTS(SELECT 1
                          FROM sub_prcs_ord_src s
                         WHERE s.prcs_id = 'REPRICE'
                           AND s.prcs_sbtyp_cd = 'BRP'
                           AND s.div_part = a.div_part
                           AND s.ord_src = a.ipdtsa)
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = a.custa
         AND (   p.reprice_typ = 'LOAD'
              OR (    p.reprice_typ = 'LDST'
                  AND se.stop_num = p.stop_num))
         AND cx.div_part = se.div_part
         AND cx.custb = se.cust_id
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb IN('O', 'P')
         AND (   (    b.excptn_sw = 'N'
                  AND b.ntshpb IS NULL)
              OR (    b.excptn_sw = 'Y'
                  AND b.ntshpb IN(SELECT ma.rsncda
                                    FROM mclp140a ma
                                   WHERE ma.rsntpa = 99)
                  AND b.subrcb = 0)
             )
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND di.div_part = ld.div_part
         AND di.itemb = e.iteme
         AND di.uomb = e.uome;

    o_cnt := SQL%ROWCOUNT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_tmp_load_stop_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_TMP_CUST_ITEM_SP
  ||  Populate temp table with order lines for Cust/Item/Cust,Item Reprice.
  ||  Selection is one of the following based on type passed:
  ||    LLR Date From, LLR Date To, McCust
  ||    LLR Date From, LLR Date To, McItem
  ||    LLR Date From, LLR Date To, McCust, McItem
  ||  The count of inserted rows will be returned in the out parameter.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/26/13 | rhalpai | Change logic for invoice date to use current date in
  ||                    | place of 1900-01-01. IM-119664
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  || 06/03/20 | rhalpai | Rewrite insert to improve performance. SDHD-714711
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_tmp_cust_item_sp(
    i_div_part     IN      NUMBER,
    i_pricing_typ  IN      VARCHAR2,
    i_parm_list    IN      CLOB,
    o_cnt          OUT     PLS_INTEGER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.INS_TMP_CUST_ITEM_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    o_cnt := 0;
    logs.dbg('Empty RepriceOrders Temp Table');

    DELETE FROM temp_reprice_orders;

    logs.dbg('Add Reprice Orders for Cust/Item');

    INSERT INTO temp_reprice_orders
                (cursor_name, ord_num, ord_line_num, sub_ind, ord_type, load_type, cbr_cust, mclane_cust, inv_date,
                 order_item, catlg_item, cbr_item, cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl,
                 hard_price_ind, passed_price, display_ind, item_pass_area, gmp_ind, distrib_id)
      SELECT DECODE(b.excptn_sw, 'N', 'ORDERS_ORDP120B', 'Y', 'ORDERS_WHSP120B') AS cur_nm, b.ordnob, b.lineb,
             DECODE(b.lineb - FLOOR(b.lineb), 0, 'N', 'Y'), DECODE(a.dsorda, 'D', 'DIS', 'REG'), NVL(a.ldtypa, 'GRO'),
             lpad_fn(a.custa, 8), lpad_fn(cx.mccusb, 6, '0'),
             TO_CHAR((CASE
                        WHEN(    a.dsorda = 'D'
                             AND (   ld.load_num = 'DIST'
                                  OR ld.load_num BETWEEN 'P00P' AND 'P99P')) THEN DATE '1900-02-28' + a.shpja
                        ELSE DECODE(se.eta_ts, DATE '1900-01-01', TRUNC(l_c_sysdate), TRUNC(se.eta_ts))
                      END
                     ),
                     'YYYYMMDD'
                    ) AS invc_dt,
             lpad_fn(e.catite, 6, '0'), lpad_fn(e.catite, 6, '0'), lpad_fn(e.iteme, 9, '0'), rpad_fn(e.uome, 3),
             DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0),
             DECODE(b.prfixb, '1', 'Y', 'Y', 'Y', 'N'), NVL(b.hdprcb, 0), DECODE(e.kite, '1', 'Y', 'Y', 'Y', 'N'),
             rpad_fn(b.itpasb, 20), DECODE(SUBSTR(e.uome, 1, 2), 'GM', 'Y', 'N'), rpad_fn(a.legrfa, 10)
        FROM (SELECT TO_DATE(t.column1, g_c_date_fmt) AS llr_dt_fro, TO_DATE(t.column2, g_c_date_fmt) AS llr_dt_to,
                     (CASE
                        WHEN i_pricing_typ IN(g_c_cust, g_c_cust_item) THEN t.column3
                      END) AS mcl_cust,
                     (CASE i_pricing_typ
                        WHEN g_c_item THEN t.column3
                        WHEN g_c_cust_item THEN t.column4
                      END) AS mcl_item
                FROM TABLE(lob2table.separatedcolumns(i_parm_list,
                                                      op_const_pk.grp_delimiter,
                                                      op_const_pk.field_delimiter
                                                     )
                          ) t) x,
             mclp020b cx, stop_eta_op1g se, load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e
       WHERE cx.div_part = i_div_part
         AND cx.mccusb =(CASE
                           WHEN i_pricing_typ IN(g_c_cust, g_c_cust_item) THEN x.mcl_cust
                           ELSE cx.mccusb
                         END)
         AND se.div_part = cx.div_part
         AND se.cust_id = cx.custb
         AND ld.div_part = se.div_part
         AND ld.load_depart_sid = se.load_depart_sid
         AND ld.llr_dt BETWEEN x.llr_dt_fro AND x.llr_dt_to
         AND a.div_part = se.div_part
         AND a.load_depart_sid = se.load_depart_sid
         AND a.custa = se.cust_id
         AND NOT EXISTS(SELECT 1
                          FROM sub_prcs_ord_src s
                         WHERE s.prcs_id = 'REPRICE'
                           AND s.prcs_sbtyp_cd = 'BRP'
                           AND s.div_part = a.div_part
                           AND s.ord_src = a.ipdtsa)
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb IN('O', 'P')
         AND (   (    b.excptn_sw = 'N'
                  AND b.ntshpb IS NULL)
              OR (    b.excptn_sw = 'Y'
                  AND b.ntshpb IN(SELECT ma.rsncda
                                    FROM mclp140a ma
                                   WHERE ma.rsntpa = 99)
                  AND b.subrcb = 0)
             )
         AND e.catite = b.orditb
         AND e.catite =(CASE
                          WHEN i_pricing_typ IN(g_c_item, g_c_cust_item) THEN x.mcl_item
                          ELSE e.catite
                        END);

    o_cnt := o_cnt + SQL%ROWCOUNT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_tmp_cust_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || REVERT_SUB_SP
  ||  Revert sub back to original line.
  ||  Issue a SavePoint and call the Revert Sub process which will delete the
  ||  sub line and manually reprice the original order line which may re-create
  ||  the sub line. Since the Sub process calls the pricing routine during
  ||  creation, any newly created sub will have current info.
  ||  Any failures returned by the Revert Sub process will cause a RollBack to
  ||  the SavePoint.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 12/08/15 | rhalpai | Add DivPart input parm and pass DivPart in call to
  ||                    | OP_MAINTAIN_SUBS_PK.REVERT_SUB_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE revert_sub_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.REVERT_SUB_SP';
    lar_parm             logs.tar_parm;
    l_lock_sw            VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
    SAVEPOINT b4_revert;
    logs.dbg('Check Order Line Available for Update');
    op_ord_dtl_pk.lock_ord_ln_sp(i_div_part, i_ord_num, i_ord_ln, l_lock_sw);

    IF l_lock_sw = 'Y' THEN
      logs.dbg('Call Sub Revert Process');
      op_maintain_subs_pk.revert_sub_sp(i_div_part, i_ord_num, i_ord_ln);
    ELSE
      logs.warn('Sub (and/or Orig) Line unavailable to Revert for Reprice updates.', lar_parm);
    END IF;   -- l_lock_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm, 'Error while Reverting Sub Line to Orig.', FALSE);
      ROLLBACK TO SAVEPOINT b4_revert;
  END revert_sub_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_SUBS_SP
  ||  Call the Sub Revert process for sub order lines in TEMP_REPRICE_ORDERS.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE process_subs_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.PROCESS_SUBS_SP';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_subs IS
      SELECT ord_num, ord_line_num
        FROM temp_reprice_orders t
       WHERE sub_ind = 'Y';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    FOR l_r_sub IN l_cur_subs LOOP
      revert_sub_sp(i_div_part, l_r_sub.ord_num, l_r_sub.ord_line_num);
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END process_subs_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_LN_SP
  ||  Update order line with the values retreived from the online pricing routine.
  ||  When called for a sub line it will call the Revert Sub process which will
  ||  delete the sub line and manually reprice the original order line which
  ||  may re-create the sub line. Since the Sub process calls the pricing
  ||  routine during creation, any newly created sub will have current info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 01/14/10 | rhalpai | Add logic to handle strict_recapped orders and call
  ||                    | IS_ORD_LN_AVAIL_FN as done in ORDER_LINE_UPDATE_SP.
  ||                    | PIR7043
  || 08/26/10 | rhalpai | Removed update of unused column, NOTOTB. PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm and add
  ||                    | DivPart in call to OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_ln_sp(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_ord_ln       IN  NUMBER,
    i_price_amt    IN  NUMBER,
    i_rtl_amt      IN  NUMBER,
    i_rtl_mult     IN  NUMBER,
    i_mfst_catg    IN  VARCHAR2,
    i_tote_catg    IN  VARCHAR2,
    i_lbl_catg     IN  NUMBER,
    i_invc_catg    IN  NUMBER,
    i_auth_cd      IN  VARCHAR2,
    i_not_shp_rsn  IN  VARCHAR2,
    i_price_dt     IN  NUMBER,
    i_price_tm     IN  NUMBER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.UPD_ORD_LN_SP';
    lar_parm              logs.tar_parm;
    l_is_strict_recapped  VARCHAR2(1);

    PROCEDURE validate_order_details_sp IS
    BEGIN
      SAVEPOINT b4_dtl_lvl_validation;
      op_order_validation_pk.validate_details_sp(i_div_part, i_ord_num, i_ord_ln);
    EXCEPTION
      WHEN OTHERS THEN
        logs.err(lar_parm, 'Order Detail Validation Error', FALSE);
        ROLLBACK TO SAVEPOINT b4_dtl_lvl_validation;
    END validate_order_details_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'PriceAmt', i_price_amt);
    logs.add_parm(lar_parm, 'RtlAmt', i_rtl_amt);
    logs.add_parm(lar_parm, 'RtlMult', i_rtl_mult);
    logs.add_parm(lar_parm, 'MfstCatg', i_mfst_catg);
    logs.add_parm(lar_parm, 'ToteCatg', i_tote_catg);
    logs.add_parm(lar_parm, 'LblCatg', i_lbl_catg);
    logs.add_parm(lar_parm, 'InvcCat', i_invc_catg);
    logs.add_parm(lar_parm, 'AuthCd', i_auth_cd);
    logs.add_parm(lar_parm, 'NotShpRsn', i_not_shp_rsn);
    logs.add_parm(lar_parm, 'PriceDt', i_price_dt);
    logs.add_parm(lar_parm, 'PriceTm', i_price_tm);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Check Order Line is Recapped for Strict Item');
    l_is_strict_recapped :=(CASE
                              WHEN op_strict_order_pk.is_recapped_fn(i_div_part, i_ord_num, i_ord_ln) THEN 'Y'
                              ELSE 'N'
                            END);
    logs.dbg('Check Order Line Available for Update');

    IF is_ord_ln_avail_fn(i_div_part, i_ord_num, i_ord_ln) THEN
      IF (    is_sub_line_fn(i_ord_ln)
          AND l_is_strict_recapped = 'N') THEN
        logs.dbg('Revert Sub');
        revert_sub_sp(i_div_part, i_ord_ln, i_ord_ln);
      ELSE
        -------------------------------------------------------------------
        -- Update order with the values retreived from the pricing routine
        -------------------------------------------------------------------
        logs.dbg('Update ORDP120B');

        UPDATE ordp120b b
           SET b.hdprcb = i_price_amt,
               b.hdrtab = i_rtl_amt,
               b.hdrtmb = i_rtl_mult,
               b.manctb = i_mfst_catg,
               b.totctb = DECODE(i_tote_catg, '   ', NULL, '000', NULL, i_tote_catg),
               b.labctb = i_lbl_catg,
               b.invctb = i_invc_catg,
               b.authb = DECODE(l_is_strict_recapped, 'Y', b.authb, DECODE(i_auth_cd, 'Y', '1', '0')),
               b.ntshpb = DECODE(l_is_strict_recapped, 'Y', b.ntshpb, RTRIM(i_not_shp_rsn)),
               b.excptn_sw = NVL2(DECODE(l_is_strict_recapped, 'Y', b.ntshpb, RTRIM(i_not_shp_rsn)), 'Y', 'N'),
               b.prstdb = i_price_dt,
               b.prsttb = i_price_tm
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.lineb = i_ord_ln
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = i_div_part
                         AND a.ordnoa = i_ord_num)
           AND b.statb IN('O', 'P');

        IF SQL%FOUND THEN
          logs.dbg('Reset ExcptnSw OrdHdr If Needed');

          UPDATE ordp100a a
             SET a.excptn_sw = 'N'
           WHERE a.div_part = i_div_part
             AND a.ordnoa = i_ord_num
             AND a.excptn_sw = 'Y'
             AND EXISTS(SELECT 1
                          FROM ordp120b b
                         WHERE b.div_part = i_div_part
                           AND b.ordnob = i_ord_num
                           AND b.excptn_sw = 'N');

          IF l_is_strict_recapped = 'N' THEN
            logs.dbg('Validate Order Details');
            validate_order_details_sp;
          END IF;   -- l_is_strict_recapped = 'N'
        END IF;   -- SQL%FOUND
      END IF;   -- is_sub_line_fn(i_ord_ln) AND l_is_strict_recapped = 'N'
    ELSE
      logs.warn('Order Line unavailable for Reprice updates.', lar_parm);
    END IF;   -- is_ord_ln_avail_fn(i_ord_ln, i_ord_ln)

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || REAL_TIME_SP
  ||  Reprice order lines in temp table using real-time process.
  ||  For each order line in the temp table, update with results from calls to
  ||  the online pricing routine.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE real_time_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module           CONSTANT typ.t_maxfqnm               := 'OP_REPRICE_PK.REAL_TIME_SP';
    lar_parm                      logs.tar_parm;
    l_c_sysdate          CONSTANT DATE                        := SYSDATE;
    l_curr_rendate                PLS_INTEGER;
    l_curr_time                   PLS_INTEGER;
    l_rtl_amt                     NUMBER                      := 0;
    l_rtl_mult                    NUMBER                      := 0;
    l_price_amt                   NUMBER                      := 0;
    l_mfst_catg                   ordp120b.manctb%TYPE        := '000';
    l_tote_catg                   ordp120b.totctb%TYPE        := '000';
    l_lbl_catg                    NUMBER                      := 0;
    l_invc_catg                   NUMBER                      := 0;
    l_not_shp_rsn                 ordp120b.ntshpb%TYPE;
    l_auth_cd                     ordp120b.authb%TYPE;
    l_err_sw                      VARCHAR2(1)                 := 'N';
    l_cnt                         PLS_INTEGER                 := 0;
    l_dsply_cnt                   PLS_INTEGER                 := 1000;
    l_c_dsply_increment  CONSTANT PLS_INTEGER                 := 5000;

    CURSOR l_cur_ords IS
      SELECT ord_num, ord_line_num, inv_date, item_pass_area, cbr_cust, mclane_cust, catlg_item, cbr_item,
             cbr_item_uom, hard_rtl_ind, rtl_amount, mult_for_rtl, hard_price_ind, passed_price, gmp_ind, display_ind,
             sub_ind, distrib_id
        FROM temp_reprice_orders;

    l_r_ord                       l_cur_ords%ROWTYPE;
    l_div                         div_mstr_di1d.div_id%TYPE;

    PROCEDURE get_pricing_sp IS
      lar_pricing_parm  logs.tar_parm;
      l_lbl_catg_x      VARCHAR2(3);
      l_invc_catg_x     VARCHAR2(3);
      l_cust_item       ordp120b.cusitb%TYPE;
      l_price_ts        VARCHAR2(14);
      l_err_msg         VARCHAR2(500);
    BEGIN
      logs.add_parm(lar_pricing_parm, 'Div', l_div);
      logs.add_parm(lar_pricing_parm, 'OrdNum', l_r_ord.ord_num);
      logs.add_parm(lar_pricing_parm, 'OrdLn', l_r_ord.ord_line_num);
      logs.add_parm(lar_pricing_parm, 'MclCust', l_r_ord.mclane_cust);
      logs.add_parm(lar_pricing_parm, 'CatlgNum', l_r_ord.catlg_item);
      l_mfst_catg := ' ';
      l_tote_catg := ' ';
      l_lbl_catg_x := ' ';
      l_invc_catg_x := ' ';
      l_auth_cd := ' ';
      l_cust_item := ' ';
      l_not_shp_rsn := ' ';
      l_price_ts := ' ';
      l_err_sw := ' ';
      l_err_msg := ' ';
      -- Move price and retail values to non-cursor fields
      -- for stored procedure call
      l_price_amt := l_r_ord.passed_price;
      l_rtl_amt := l_r_ord.rtl_amount;
      l_rtl_mult := l_r_ord.mult_for_rtl;
      logs.dbg('Retrieve Pricing');
      op_pricing_pk.retrieve_pricing_sp(l_r_ord.mclane_cust,
                                        l_r_ord.catlg_item,
                                        l_div,
                                        l_r_ord.cbr_cust,
                                        l_r_ord.cbr_item,
                                        l_r_ord.cbr_item_uom,
                                        l_r_ord.inv_date,
                                        l_r_ord.gmp_ind,
                                        l_r_ord.sub_ind,
                                        l_r_ord.display_ind,
                                        l_r_ord.hard_price_ind,
                                        l_price_amt,
                                        l_r_ord.hard_rtl_ind,
                                        l_rtl_amt,
                                        l_rtl_mult,
                                        l_r_ord.item_pass_area,
                                        l_r_ord.distrib_id,
                                        l_mfst_catg,
                                        l_tote_catg,
                                        l_lbl_catg_x,
                                        l_invc_catg_x,
                                        l_auth_cd,
                                        l_cust_item,
                                        l_not_shp_rsn,
                                        l_price_ts,
                                        l_err_sw,
                                        l_err_msg
                                       );

      IF l_err_sw = 'Y' THEN
        logs.warn('Call to OP_PRICING_PK.RETRIEVE_PRICING_SP returned Error: ' || l_err_msg,
                  lar_parm,
                  logs.parm_list(lar_pricing_parm) || ' ErrorSW: ' || l_err_sw
                 );
      ELSE
        -- Update order with the values retreived from the pricing routine
        l_lbl_catg := TO_NUMBER(l_lbl_catg_x);
        l_invc_catg := TO_NUMBER(l_invc_catg_x);

        -- Reset price and/or retail if hard price and/or retail indicator = 'Y'
        IF l_r_ord.hard_price_ind = 'Y' THEN
          l_price_amt := l_r_ord.passed_price;
        END IF;   -- hard price indicator = 'Y'

        IF l_r_ord.hard_rtl_ind = 'Y' THEN
          l_rtl_amt := l_r_ord.rtl_amount;
          l_rtl_mult := l_r_ord.mult_for_rtl;
        END IF;   -- hard retail indicator = 'Y'
      END IF;   -- l_err_sw = 'Y'
    EXCEPTION
      WHEN OTHERS THEN
        logs.err(SQLERRM,
                 lar_parm,
                 'Pricing error' || cnst.newline_char || logs.parm_list(lar_pricing_parm) || ' ErrorSW: ' || l_err_sw
                );
    END get_pricing_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div := div_pk.div_id_fn(i_div_part);
    l_curr_rendate := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_curr_time := TO_CHAR(l_c_sysdate, 'HH24MISS');
    FOR l_r_order IN l_cur_ords LOOP
      l_r_ord := l_r_order;

      IF l_r_ord.sub_ind = 'Y' THEN
        logs.dbg('Revert Sub');
        revert_sub_sp(i_div_part, l_r_ord.ord_num, l_r_ord.ord_line_num);
        COMMIT;   -- Commit after each order line processed
        l_cnt := l_cnt + 1;
      ELSE
        logs.dbg('Get Pricing');
        get_pricing_sp;

        IF l_err_sw <> 'Y' THEN
          logs.dbg('Update Order Line');
          upd_ord_ln_sp(i_div_part,
                        l_r_ord.ord_num,
                        l_r_ord.ord_line_num,
                        l_price_amt,
                        l_rtl_amt,
                        l_rtl_mult,
                        l_mfst_catg,
                        l_tote_catg,
                        l_lbl_catg,
                        l_invc_catg,
                        l_auth_cd,
                        l_not_shp_rsn,
                        l_curr_rendate,
                        l_curr_time
                       );
          COMMIT;   -- Commit after each order line processed
          l_cnt := l_cnt + 1;
        END IF;   -- l_err_sw <> 'Y'
      END IF;   -- l_r_ord.sub_ind = 'Y'

      IF l_cnt = l_dsply_cnt THEN
        l_dsply_cnt := l_dsply_cnt + l_c_dsply_increment;
        logs.info('Real-Time Reprice Records Processed So Far: ' || l_cnt, lar_parm);
      END IF;   -- l_cnt = l_dsply_cnt
    END LOOP;
    logs.info('Total Real-Time Reprice Records Processed: ' || l_cnt, lar_parm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(SQLERRM,
               lar_parm,
               'OrdNum: ' || util.to_str(l_r_ord.ord_num) || ' OrdLn: ' || util.to_str(l_r_ord.ord_line_num)
              );
  END real_time_sp;

  /*
  ||----------------------------------------------------------------------------
  || BATCH_FTP_SP
  ||  Reprice order lines in temp table using batch ftp process.
  ||  Create a file in QOPRC01 format containing each order line in the temp
  ||  table and then ftp to mainframe for batch repricing. The mainframe will
  ||  send back MQ QOPRC02 msgs which will be processed by the Reprice Update
  ||  process to update the order lines.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to PROCESS_SUBS_SP.
  || 02/09/16 | rhalpai | Change filename to include nano seconds and change
  ||                    | logic for pricing type OTHR (REPRICE) to call a linux
  ||                    | script that will ftp the file to a unique DSN on the
  ||                    | mainframe and ftp a job to the mainframe with the DSN
  ||                    | embedded in the JCL. This will eliminate the possibility
  ||                    | of a subsequent ftp from overlaying a file on the
  ||                    | mainframe before it can be processed. PIR15945
  || 01/04/17 | rhalpai | Change logic to ftp empty file when no records found
  ||                    | for pricing_typ of MASS or DIST. SDHD-73606
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE batch_ftp_sp(
    i_div          IN  VARCHAR2,
    i_pricing_typ  IN  VARCHAR2
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm  := 'OP_REPRICE_PK.BATCH_FTP_SP';
    lar_parm                    logs.tar_parm;
    l_div_part                  NUMBER;
    l_c_file_dir       CONSTANT VARCHAR2(50)   := '/ftptrans';
    l_file_nm                   VARCHAR2(50);
    l_t_rpt_lns                 typ.tas_maxvc2;
    l_c_mass_rmt_file  CONSTANT VARCHAR2(7)    := 'RPCMASS';
    l_c_dist_rmt_file  CONSTANT VARCHAR2(7)    := 'RPCDIST';
    l_c_othr_rmt_file  CONSTANT VARCHAR2(7)    := 'REPRICE';
    l_rmt_file                  VARCHAR2(7);

    PROCEDURE send_to_mf_sp(
      i_div       IN  VARCHAR2,
      i_div_part  IN  NUMBER,
      i_file_nm   IN  VARCHAR2
    ) IS
      l_appl_srvr  appl_sys_parm_ap1s.vchar_val%TYPE;
      l_cmd        typ.t_maxvc2;
      l_os_result  typ.t_maxvc2;
    BEGIN
      -- this script will zip and ftp the file to the MF as well as
      -- ftping the jcl containing the zip file to process it.
      l_appl_srvr := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_appl_srvr);
      l_cmd := '/local/prodcode/bin/XXOPRP0J.ksh "'
               || i_div
               || '" "'
               || i_file_nm
               || '"';
      logs.info(l_cmd);
      l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
      logs.info(l_os_result);
    END send_to_mf_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_file_nm := i_div || '_REPRICE_ORDERS_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF') || '.txt';
    l_rmt_file :=(CASE i_pricing_typ
                    WHEN g_c_mass THEN l_c_mass_rmt_file
                    WHEN g_c_dist THEN l_c_dist_rmt_file
                    ELSE l_c_othr_rmt_file
                  END
                 );
    logs.dbg('Open Cursor');

    SELECT op_reprice_pk.qoprc01_msg_fn(i_div,
                                        i_pricing_typ,
                                        t.ord_num,
                                        t.ord_line_num,
                                        t.ord_type,
                                        t.load_type,
                                        t.cbr_cust,
                                        t.mclane_cust,
                                        t.inv_date,
                                        t.catlg_item,
                                        t.cbr_item,
                                        t.cbr_item_uom,
                                        t.hard_rtl_ind,
                                        t.rtl_amount,
                                        t.mult_for_rtl,
                                        t.hard_price_ind,
                                        t.passed_price,
                                        t.display_ind,
                                        t.item_pass_area,
                                        t.distrib_id
                                       ) AS msg
    BULK COLLECT INTO l_t_rpt_lns
      FROM temp_reprice_orders t
     WHERE t.sub_ind = 'N';

    IF (   l_t_rpt_lns.COUNT > 0
        OR i_pricing_typ IN(g_c_mass, g_c_dist)) THEN
      logs.dbg('Write File');
      write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);

      IF i_pricing_typ IN(g_c_mass, g_c_dist) THEN
        logs.dbg('FTP to Mainframe');
        op_ftp_sp(i_div, l_file_nm, l_rmt_file);
      ELSE
        logs.dbg('Send to Mainframe');
        send_to_mf_sp(i_div, l_div_part, l_file_nm);
      END IF;   -- i_pricing_typ IN(g_c_mass, g_c_dist)
    END IF;   -- l_t_rpt_lns.COUNT > 0 OR i_pricing_typ IN(g_c_mass, g_c_dist)

    logs.dbg('Process Subs');
    process_subs_sp(l_div_part);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END batch_ftp_sp;

  /*
  ||----------------------------------------------------------------------------
  || BATCH_MQ_SP
  ||  Reprice order lines in temp table using batch MQ QOPRC01 process.
  ||  Create QOPRC01 msgs for each order line in the temp table and then send
  ||  to mainframe for batch repricing. The mainframe will send back MQ QOPRC02
  ||  msgs which will be processed by the Reprice Update process to update the
  ||  order lines.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to PROCESS_SUBS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE batch_mq_sp(
    i_div          IN  VARCHAR2,
    i_pricing_typ  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.BATCH_MQ_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Add QOPRC01 Records');

    INSERT INTO mclane_mq_put
                (div_part, mq_msg_id, mq_msg_status, mq_msg_data)
      SELECT l_div_part, 'QOPRC01', 'OPN',
             op_reprice_pk.qoprc01_msg_fn(i_div,
                                          i_pricing_typ,
                                          t.ord_num,
                                          t.ord_line_num,
                                          t.ord_type,
                                          t.load_type,
                                          t.cbr_cust,
                                          t.mclane_cust,
                                          t.inv_date,
                                          t.catlg_item,
                                          t.cbr_item,
                                          t.cbr_item_uom,
                                          t.hard_rtl_ind,
                                          t.rtl_amount,
                                          t.mult_for_rtl,
                                          t.hard_price_ind,
                                          t.passed_price,
                                          t.display_ind,
                                          t.item_pass_area,
                                          t.distrib_id
                                         )
        FROM temp_reprice_orders t
       WHERE t.sub_ind = 'N';

    -- Commit before calling process to send msgs to mainframe via MQ
    -- since it runs in a separate session
    COMMIT;
    logs.dbg('Send QOPRC01 MQ Msgs');
    op_mq_message_pk.mq_put_sp('QOPRC01', i_div, NULL, l_rc);
    excp.assert((l_rc = 0), 'Failed to put QOPRC01 msgs to MQ');
    logs.dbg('Process Subs');
    process_subs_sp(l_div_part);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END batch_mq_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_LOAD_DEPART_SID_SP
  ||  Ensure all orders for div/llr_dt/load point to the same load_depart_sid.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/13/22 | rhalpai | Original. SDHD-1458184
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_load_depart_sid_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.UPD_LOAD_DEPART_SID_SP';
    lar_parm             logs.tar_parm;
    l_t_new_sid          type_ntab;
    l_t_sid              type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Check for multiple load_depart_sid for same div/llr_dt/load');
    WITH x AS(
      SELECT ld.llr_dt, ld.load_num
        FROM load_depart_op1f ld
       WHERE ld.div_part = i_div_part
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = ld.div_part
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.stata IN('O', 'S'))
      GROUP BY ld.llr_dt, ld.load_num
      HAVING COUNT(*) > 1
    ), y AS(
      SELECT ld.load_depart_sid, COUNT(*) AS cnt
        FROM x, load_depart_op1f ld, ordp100a a
       WHERE ld.div_part = i_div_part
         AND ld.llr_dt = x.llr_dt
         AND ld.load_num = x.load_num
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.stata IN('O', 'S')
      GROUP BY ld.load_depart_sid
    ), z AS(
      SELECT FIRST_VALUE(y.load_depart_sid) OVER(ORDER BY y.cnt DESC) AS new_load_depart_sid,
             y.load_depart_sid
        FROM y
    )
    SELECT z.new_load_depart_sid, z.load_depart_sid
      BULK COLLECT INTO l_t_new_sid, l_t_sid
      FROM z
     WHERE z.new_load_depart_sid <> z.load_depart_sid;

    IF l_t_sid.COUNT > 0 THEN
      logs.dbg('Upd to same load_depart_sid');
      FORALL i IN l_t_sid.FIRST .. l_t_sid.LAST
        UPDATE ordp100a a
           SET a.load_depart_sid = l_t_new_sid(i)
         WHERE a.div_part = i_div_part
           AND a.stata IN('O', 'S')
           AND a.load_depart_sid = l_t_sid(i);
    END IF;   -- l_t_sid.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_load_depart_sid_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || TYPE_LIST_FN
  ||  Build a cursor of type selections for Reprice UI.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION type_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.TYPE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_t_typs             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    l_t_typs := type_stab(g_c_load || 'Load',
                          g_c_load_stop || 'Load/Stop',
                          g_c_item || 'Item',
                          g_c_cust || 'Customer',
                          g_c_cust_item || 'Customer/Item',
                          g_c_ord_num || 'OrderNum',
                          g_c_ord_ln || 'OrderNum/OrderLine'
                         );

    OPEN l_cv
     FOR
       SELECT SUBSTR(t.column_value, 1, 4) AS typ_cd, SUBSTR(t.column_value, 5) AS typ_nm
         FROM TABLE(l_t_typs) t;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END type_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FN
  ||   Build a cursor of loads with open orders between LLR Date range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original. PIR3593
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div         IN  VARCHAR2,
    i_llr_dt_fro  IN  VARCHAR2,
    i_llr_dt_to   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt_fro         DATE;
    l_llr_dt_to          DATE;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDtFro', i_llr_dt_fro);
    logs.add_parm(lar_parm, 'LLRDtTo', i_llr_dt_to);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt_fro := TO_DATE(i_llr_dt_fro, 'YYYY-MM-DD');
    l_llr_dt_to := TO_DATE(i_llr_dt_to, 'YYYY-MM-DD');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    OPEN l_cv
     FOR
       SELECT   ld.load_num, l.destc, TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt
           FROM load_depart_op1f ld, mclp120c l
          WHERE ld.div_part = l_div_part
            AND ld.llr_dt BETWEEN l_llr_dt_fro AND l_llr_dt_to
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.stata = 'O')
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(l_t_xloads) t)
            AND l.div_part = ld.div_part
            AND l.loadc = ld.load_num
       GROUP BY ld.load_num, l.destc, ld.llr_dt
       ORDER BY 1, 3;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_LIST_FN
  ||   Build a cursor of stops for Div/LLR-Date/Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original. PIR3593
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.STOP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   (CASE
                   WHEN se.stop_num < 10 THEN '0' || se.stop_num
                   ELSE TO_CHAR(se.stop_num)
                 END) AS stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm
           FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = l_llr_dt
            AND ld.load_num = i_load_num
            AND se.div_part = ld.div_part
            AND se.load_depart_sid = ld.load_depart_sid
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = se.div_part
                          AND a.load_depart_sid = se.load_depart_sid
                          AND a.custa = se.cust_id
                          AND a.stata = 'O')
            AND c.div_part = se.div_part
            AND c.acnoc = se.cust_id
            AND cx.div_part = se.div_part
            AND cx.custb = se.cust_id
       ORDER BY stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRICING_ORD_LIST_FN
  ||   Build a cursor of order lines for pricing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original. PIR3593
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. Change to use OrdTyp to
  ||                    | indicate TestSw. PIR11038
  || 11/10/20 | rhalpai | Change logic to allow inclusion of distributions. PIR20587
  ||----------------------------------------------------------------------------
  */
  FUNCTION pricing_ord_list_fn(
    i_div         IN  VARCHAR2,
    i_llr_dt_fro  IN  VARCHAR2,
    i_llr_dt_to   IN  VARCHAR2,
    i_mcl_cust    IN  VARCHAR2 DEFAULT NULL,
    i_catlg_num   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.PRICING_ORD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt_fro         DATE;
    l_llr_dt_to          DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDtFro', i_llr_dt_fro);
    logs.add_parm(lar_parm, 'LLRDtTo', i_llr_dt_to);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.info('ENTRY', lar_parm);
    l_llr_dt_fro := TO_DATE(i_llr_dt_fro, 'YYYY-MM-DD');
    l_llr_dt_to := TO_DATE(i_llr_dt_to, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_ts, 'YYYY-MM-DD') AS llr_dt, ld.load_num,
                (CASE
                   WHEN se.stop_num < 10 THEN '0' || se.stop_num
                   ELSE TO_CHAR(se.stop_num)
                 END) AS stop_num, DECODE(a.dsorda, 'T', 'T') AS tst, cx.mccusb AS mcl_cust, b.ordnob AS ord_num,
                a.dsorda AS ord_typ, b.lineb AS ord_ln, e.catite AS catlg_num, e.ctdsce AS item_descr,
                e.shppke AS pack, e.sizee AS sz, a.cpoa AS po_num,
                TO_CHAR(TO_DATE('19000228' || LPAD(NVL(b.prsttb, 0), 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                        'YYYY-MM-DD HH24:MI:SS'
                       ) AS price_ts
           FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a, ordp120b b,
                sawp505e e, mclp110b di
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt BETWEEN l_llr_dt_fro AND l_llr_dt_to
            AND se.div_part = ld.div_part
            AND se.load_depart_sid = ld.load_depart_sid
            AND cx.div_part = se.div_part
            AND cx.custb = se.cust_id
            AND (   i_mcl_cust IS NULL
                 OR cx.mccusb = i_mcl_cust)
            AND a.div_part = se.div_part
            AND a.load_depart_sid = se.load_depart_sid
            AND a.custa = se.cust_id
            AND a.stata = 'O'
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND (   i_catlg_num IS NULL
                 OR e.catite = i_catlg_num)
            AND di.div_part = d.div_part
            AND di.itemb = e.iteme
            AND di.uomb = e.uome
            AND b.statb = 'O'
            AND (   b.excptn_sw = 'N'
                 OR b.ntshpb IN(SELECT ma.rsncda
                                  FROM mclp140a ma
                                 WHERE ma.rsntpa = 99))
            AND b.subrcb < 999
       ORDER BY mccusb, llr_dt, load_num, stop_num, ord_num, ord_ln;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END pricing_ord_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || QOPRC01_MSG_FN
  ||  Return values formatted to a QOPRC01 MQ data message.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 01/14/10 | rhalpai | Changed to use OrdTyp of REG or DIS. PIR7043
  ||----------------------------------------------------------------------------
  */
  FUNCTION qoprc01_msg_fn(
    i_div             IN  VARCHAR2,
    i_pricing_typ     IN  VARCHAR2,
    i_ord_num         IN  NUMBER,
    i_ord_ln          IN  NUMBER,
    i_ord_typ         IN  VARCHAR2,
    i_load_typ        IN  VARCHAR2,
    i_cust_id         IN  VARCHAR2,
    i_mcl_cust        IN  VARCHAR2,
    i_invc_dt         IN  VARCHAR2,
    i_mcl_item        IN  VARCHAR2,
    i_item_num        IN  VARCHAR2,
    i_uom             IN  VARCHAR2,
    i_hard_rtl_sw     IN  VARCHAR2,
    i_rtl_amt         IN  NUMBER,
    i_rtl_mult        IN  NUMBER,
    i_hard_price_sw   IN  VARCHAR2,
    i_price_amt       IN  NUMBER,
    i_kit_sw          IN  VARCHAR2,
    i_item_pass_area  IN  VARCHAR2,
    i_legcy_ref       IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN(RPAD(i_div, 2)
           || 'QOPRC01 '
           || RPAD('REPRICE ' || i_pricing_typ, 30)
           || 'ADD'
           || RPAD(' ', 10)
           || RPAD(i_pricing_typ, 4)
           || lpad_fn(i_ord_num, 11, '0')
           || lpad_fn(TO_CHAR(i_ord_ln, 'FM9999999V99'), 9, '0')
           || '    '
           || rpad_fn(i_cust_id, 8)
           || rpad_fn(i_mcl_cust, 6, '0')
           || rpad_fn(i_mcl_item, 6, '0')
           || lpad_fn(i_item_num, 9, '0')
           || rpad_fn(i_uom, 3)
           || RPAD(i_invc_dt, 8)
           || rpad_fn(i_item_pass_area, 25)
           ||(CASE
                WHEN i_hard_rtl_sw IN('1', 'Y') THEN 'Y'
                ELSE 'N'
              END)
           || lpad_fn(TO_CHAR(i_rtl_amt, 'FM9999999V99'), 9, '0')
           || lpad_fn(i_rtl_mult, 5, '0')
           ||(CASE
                WHEN i_hard_price_sw IN('1', 'Y') THEN 'Y'
                ELSE 'N'
              END)
           || lpad_fn(TO_CHAR(i_price_amt, 'FM9999999V99'), 9, '0')
           ||(CASE
                WHEN SUBSTR(i_uom, 1, 2) = 'GM' THEN 'Y'
                ELSE 'N'
              END)
           || 'N'
           ||(CASE
                WHEN i_kit_sw IN('1', 'Y') THEN 'Y'
                ELSE 'N'
              END)
           || rpad_fn(i_legcy_ref, 10)
           || RPAD(NVL(i_load_typ, 'GRO'), 3)
           || rpad_fn(i_ord_typ, 3)
          );
  END qoprc01_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || REPRICE_BULK_SP
  ||  Reprice order lines in order well.
  ||  This process may be called directly by the OP Reprice Screen or via Unix
  ||  script triggered when a batch (OS/390) reprice request (QOPRC03) is
  ||  received on the request queue.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 02/06/08 | rhalpai | Added logic for Reprice OTHR QOPRC03 msgs. PIR3593
  || 12/08/15 | rhalpai | Change logic to pass DivPart in calls to INS_TMP_MASS_SP,
  ||                    | INS_TMP_DIST_SP, INS_TMP_OTHR_SP, INS_TMP_ORD_LN_SP,
  ||                    | INS_TMP_LOAD_STOP_SP, INS_TMP_CUST_ITEM_SP, REAL_TIME_SP.
  || 01/04/17 | rhalpai | Change logic to call batch_ftp_sp when extr_dest is
  ||                    | bach_ftp or reprice_cnt > 0. SDHD-73606
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reprice_bulk_sp(
    i_div          IN  VARCHAR2,
    i_pricing_typ  IN  VARCHAR2,
    i_extr_dest    IN  VARCHAR2 DEFAULT g_c_real_time,
    i_parm_list    IN  CLOB DEFAULT NULL
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.REPRICE_BULK_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_reprice_cnt           PLS_INTEGER;
    l_reprice_realtime_max  NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PricingTyp', i_pricing_typ);
    logs.add_parm(lar_parm, 'ExtrDest', i_extr_dest);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_extr_dest = g_c_real_time THEN
      l_reprice_realtime_max := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_rpc_realtime_max);
    END IF;   -- i_extr_dest = g_c_real_time

    CASE
      WHEN i_pricing_typ = g_c_mass THEN
        logs.dbg('Add Temp Order Recs for MASS');
        ins_tmp_mass_sp(l_div_part, l_reprice_cnt);
      WHEN i_pricing_typ = g_c_dist THEN
        logs.dbg('Add Temp Order Recs for DIST');
        ins_tmp_dist_sp(l_div_part, l_reprice_cnt);
      WHEN i_pricing_typ = g_c_othr THEN
        logs.dbg('Add Temp Order Recs for OTHR');
        ins_tmp_othr_sp(l_div_part, l_reprice_cnt);
      WHEN i_pricing_typ IN(g_c_ord_num, g_c_ord_ln) THEN
        logs.dbg('Add Temp Order Recs for OrdNum/OrdLn');
        ins_tmp_ord_ln_sp(l_div_part, i_pricing_typ, i_parm_list, l_reprice_cnt);
      WHEN i_pricing_typ IN(g_c_load, g_c_load_stop) THEN
        logs.dbg('Add Temp Order Recs for Load/Stop');
        ins_tmp_load_stop_sp(l_div_part, i_pricing_typ, i_parm_list, l_reprice_cnt);
      WHEN i_pricing_typ IN(g_c_cust, g_c_item, g_c_cust_item) THEN
        logs.dbg('Add Temp Order Recs for Cust/Item');
        ins_tmp_cust_item_sp(l_div_part, i_pricing_typ, i_parm_list, l_reprice_cnt);
    END CASE;

    -- commit temp inserts
    COMMIT;

    CASE
      WHEN(    i_extr_dest = g_c_real_time
           AND l_reprice_cnt > 0
           AND l_reprice_cnt <= l_reprice_realtime_max) THEN
        logs.dbg('Process Real-Time');
        real_time_sp(l_div_part);
      WHEN(    i_extr_dest = g_c_batch_mq
           AND l_reprice_cnt > 0) THEN
        logs.dbg('Process Batch MQ');
        batch_mq_sp(i_div, i_pricing_typ);
      WHEN(   i_extr_dest = g_c_batch_ftp
           OR l_reprice_cnt > 0) THEN
        logs.dbg('Process Batch FTP');
        batch_ftp_sp(i_div, i_pricing_typ);
      ELSE
        NULL;
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END reprice_bulk_sp;

  /*
  ||----------------------------------------------------------------------------
  || REPRICE_ORD_LN_SP
  ||  Reprice an order line or all lines for order number when line not passed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/08/07 | rhalpai | Original. PIR3593
  || 12/08/15 | rhalpai | Add DivPart input parm and change logic to pass DivPart
  ||                    | in calls to INS_TMP_ORD_LN_SP, REAL_TIME_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reprice_ord_ln_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPRICE_PK.REPRICE_ORD_LN_SP';
    lar_parm             logs.tar_parm;
    l_parm_list          CLOB;
    l_reprice_cnt        PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);

    IF i_ord_ln IS NULL THEN
      l_parm_list := TO_CHAR(i_ord_num);
      logs.dbg('Add Temp Order Recs for OrdNum');
      ins_tmp_ord_ln_sp(i_div_part, g_c_ord_num, l_parm_list, l_reprice_cnt);
    ELSE
      l_parm_list := i_ord_num || op_const_pk.field_delimiter || i_ord_ln;
      logs.dbg('Add Temp Order Recs for OrdNum/OrdLn');
      ins_tmp_ord_ln_sp(i_div_part, g_c_ord_ln, l_parm_list, l_reprice_cnt);
    END IF;   -- i_ord_ln IS NULL

    logs.dbg('Process Real-Time');
    real_time_sp(i_div_part);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reprice_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || REPRICE_UPDATE_SP
  ||  This procedure will process QOPRC02 (price reply) rows from the
  ||  mclane_mq_get table and update the order wells (good and exception) with
  ||  the pricing inforamtion retrieved from the mainframe from the QOPRC01 price
  ||  request.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/28/01 | JBARTON | Original
  || 07/12/02 | JBARTON | Modified ORDP120B update statement to set NOTOTB to NULL
  ||                    |  to resolve an issue where this field was being
  ||                    |  populated in load balance for distribution orders
  ||                    |  downloaded on the same day that load balancing was
  ||                    |  performed and the tote category for the item had not
  ||                    |  been setup yet thus causing this field to be set to
  ||                    |  the order quantity which should only be done for items
  ||                    |  that are not to be placed in totes -- this column is
  ||                    |  populated in Load Balance if it contains a NULL value
  || 11/08/02 | JBARTON | Modified the insert statements for the whsp100a and
  ||                    | ordp100a tables to include the ord_rcvd_ts when inserting
  ||                    | using values from the opposite well--this is to retain
  ||                    | the original order received timestamp when moving from
  ||                    | the good well to the bad well and vice versa
  || 07/07/03 | CNATIVI | Modified to correct a reported problem where the sub line
  ||                    | of an order line that has been repriced has become an
  ||                    | exception and needs to be reverted back to the orig line
  || 07/22/03 | CNATIVI | Modified to correct a reported problem where the sub line
  ||                    | of an order line that has been repriced has become an
  ||                    | exception and needs to be reverted back to the orig line
  ||                    | (this occurs when "source" has been tagged with ORDP
  || 01/13/04 | rhalpai | Eliminated use of MCLANE_DATE_CONVERT table.
  ||                    | Changed to use common logic.
  || 06/04/04 | rhalpai | Changed to handle repricing of subs in good well that
  ||                    | are still authorized and skip repricing of orders in
  ||                    | bad well that are not authorized.
  || 01/26/05 | rhalpai | Changed error handler and warning log to new standard
  ||                    | format.
  ||                    | Removed out status parm.
  ||                    | Changed logic for reverting to original sub line,
  ||                    | op_maintain_subs_pk.revert_to_original_sub_line_sp,
  ||                    | to no longer use a return status parm and also handle
  ||                    | exceptions that may now be raised by this call.
  ||                    | Changed the logic for updating the status of QOPRC02
  ||                    | messages.
  || 12/08/06 | rhalpai | Changed to remove the CASE statement and always call
  ||                    | ORDER_LINE_UPDATE_SP. PIR4166
  || 01/14/10 | rhalpai | Changed to call UPD_ORD_LN_SP. PIR7043
  || 03/12/14 | rhalpai | Add logic to update LAST_REPRICEMASS_TS parm to SYSDATE
  ||                    | once last Reprice Mass entry is processed. PIR13614
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to UPD_ORD_LN_SP.
  || 12/13/22 | rhalpai | Add logic to ensure all orders for div/llr_dt/load point
  ||                    | to the same load_depart_sid when completing processing
  ||                    | for a REPRICE MASS. SDHD-1458184
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reprice_update_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm          := 'OP_REPRICE_PK.REPRICE_UPDATE_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_sysdate  CONSTANT DATE                   := SYSDATE;
    l_curr_rendate        PLS_INTEGER;
    l_curr_time           PLS_INTEGER;
    l_mq_get_id           NUMBER;
    l_comnt               VARCHAR2(30);
    l_ord_num             NUMBER;
    l_ord_ln              NUMBER;
    l_hard_rtl_sw         ordp120b.rtfixb%TYPE;
    l_rtl_amt             NUMBER;
    l_rtl_mult            NUMBER;
    l_hard_price_sw       ordp120b.prfixb%TYPE;
    l_price_amt           NUMBER;
    l_auth_cd             ordp120b.authb%TYPE;
    l_not_shp_rsn         ordp120b.ntshpb%TYPE;
    l_mfst_catg           ordp120b.manctb%TYPE;
    l_tote_catg           ordp120b.totctb%TYPE;
    l_invc_catg           NUMBER;
    l_lbl_catg            NUMBER;
    l_reprice_mass_sw     VARCHAR2(1)            := 'N';
    l_cnt                 PLS_INTEGER            := 0;
    l_dsply_cnt           PLS_INTEGER            := 1000;
    l_mq_msg_stat         VARCHAR2(3);

    CURSOR l_cur_mq_get(
      b_div_part  NUMBER,
      b_msg_id    VARCHAR2
    ) IS
      SELECT mq_get_id, mq_msg_data
        FROM mclane_mq_get
       WHERE div_part = b_div_part
         AND mq_msg_id = b_msg_id
         AND mq_msg_status IN('OPN', 'WRK');
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_dsply_cnt := 1000;
    l_cnt := 0;
    l_curr_rendate := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_curr_time := TO_CHAR(l_c_sysdate, 'HH24MISS');
    logs.dbg('Process MQ Get Cursor for QOPRC02');
    FOR l_r_mq_get IN l_cur_mq_get(l_div_part, 'QOPRC02') LOOP
      l_mq_msg_stat := 'CMP';
      l_mq_get_id := l_r_mq_get.mq_get_id;
      ---------------------------------------------------------------
      -- Parse the MQ message data to be used in the Update process
      ---------------------------------------------------------------
      logs.dbg('Parse');
      l_comnt := SUBSTR(l_r_mq_get.mq_msg_data, 11, 30);
      l_ord_num := SUBSTR(l_r_mq_get.mq_msg_data, 58, 11);
      l_ord_ln := SUBSTR(l_r_mq_get.mq_msg_data, 69, 7) || '.' || SUBSTR(l_r_mq_get.mq_msg_data, 76, 2);
      l_hard_rtl_sw := SUBSTR(l_r_mq_get.mq_msg_data, 82, 1);
      l_rtl_amt := SUBSTR(l_r_mq_get.mq_msg_data, 83, 7) || '.' || SUBSTR(l_r_mq_get.mq_msg_data, 90, 2);
      l_rtl_mult := SUBSTR(l_r_mq_get.mq_msg_data, 92, 5);
      l_hard_price_sw := SUBSTR(l_r_mq_get.mq_msg_data, 97, 1);
      l_price_amt := SUBSTR(l_r_mq_get.mq_msg_data, 98, 7) || '.' || SUBSTR(l_r_mq_get.mq_msg_data, 105, 2);
      l_auth_cd := SUBSTR(l_r_mq_get.mq_msg_data, 107, 1);
      l_not_shp_rsn := SUBSTR(l_r_mq_get.mq_msg_data, 108, 8);
      l_mfst_catg := SUBSTR(l_r_mq_get.mq_msg_data, 116, 3);
      l_tote_catg := SUBSTR(l_r_mq_get.mq_msg_data, 119, 3);
      l_invc_catg := SUBSTR(l_r_mq_get.mq_msg_data, 122, 3);
      l_lbl_catg := SUBSTR(l_r_mq_get.mq_msg_data, 125, 3);

      IF l_comnt LIKE 'REPRICE MASS%' THEN
        l_reprice_mass_sw := 'Y';
      END IF;   -- l_comnt LIKE 'REPRICE MASS%'

      logs.dbg('Upd Ord Ln');
      upd_ord_ln_sp(l_div_part,
                    l_ord_num,
                    l_ord_ln,
                    l_price_amt,
                    l_rtl_amt,
                    l_rtl_mult,
                    l_mfst_catg,
                    l_tote_catg,
                    l_lbl_catg,
                    l_invc_catg,
                    l_auth_cd,
                    l_not_shp_rsn,
                    l_curr_rendate,
                    l_curr_time
                   );
      --------------------------------------------------------
      -- Update the reprice update cursor row just processed
      --------------------------------------------------------
      logs.dbg('Set MQ Msg Status');

      UPDATE mclane_mq_get
         SET last_chg_ts = l_c_sysdate,
             mq_msg_status = l_mq_msg_stat
       WHERE div_part = l_div_part
         AND mq_get_id = l_mq_get_id;

      COMMIT;   -- Commit after each reprice update cursor row processed
      l_cnt := l_cnt + 1;

      IF l_cnt = l_dsply_cnt THEN
        logs.info('Reprice Update Records Processed so far' || cnst.newline_char || ' Count: ' || l_cnt, lar_parm);
        l_dsply_cnt := l_dsply_cnt + 5000;
      END IF;   -- l_cnt = l_dsply_cnt
    END LOOP;   -- Reprice Update Cursor Loop

    logs.dbg('Set Final MQ LastChgTs');

    UPDATE mclane_mq_get
       SET last_chg_ts = SYSDATE
     WHERE div_part = l_div_part
       AND mq_msg_id = 'QOPRC02'
       AND last_chg_ts = l_c_sysdate;

    COMMIT;

    IF l_reprice_mass_sw = 'Y' THEN
      logs.dbg('Upd LastRepriceMassTS Parm');
      op_parms_pk.merge_sp(l_div_part,
                           op_const_pk.prm_last_rpcmass_ts,
                           op_parms_pk.g_c_dt,
                           TO_CHAR(l_c_sysdate, 'YYYYMMDDHH24MISS'),
                           'REPRICE'
                          );
      logs.dbg('Ensure all orders for div/llr_dt/load point to the same load_depart_sid');
      upd_load_depart_sid_sp(l_div_part);
      COMMIT;
    END IF;   -- l_reprice_mass_sw = 'Y'

    logs.info('Total Reprice Update Records Processed' || cnst.newline_char || ' Count: ' || l_cnt, lar_parm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm, 'OrdNum: ' || util.to_str(l_ord_num) || ' OrdLn: ' || util.to_str(l_ord_ln));
  END reprice_update_sp;
END op_reprice_pk;
/

