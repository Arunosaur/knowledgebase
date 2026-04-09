CREATE OR REPLACE PACKAGE op_order_moves_pk IS
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

  FUNCTION split_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION llr_date_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ship_date_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_typ_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_for_ords_fn(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  VARCHAR2 DEFAULT NULL,
    i_ord_typ        IN  VARCHAR2 DEFAULT NULL,
    i_incl_xload_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_eta_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION cust_detail_fn(
    i_div         IN  VARCHAR2,
    i_llr_dt      IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_fro  IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_to   IN  VARCHAR2 DEFAULT NULL,
    i_load_typ    IN  VARCHAR2 DEFAULT NULL,
    i_ord_typ     IN  VARCHAR2 DEFAULT NULL,
    i_load_num    IN  VARCHAR2 DEFAULT NULL,
    i_ord_num     IN  NUMBER DEFAULT NULL,
    i_crp_cd      IN  NUMBER DEFAULT NULL,
    i_grp_cd      IN  VARCHAR2 DEFAULT NULL,
    i_cust_typ    IN  VARCHAR2 DEFAULT 'MCL',
    i_cust_id     IN  VARCHAR2 DEFAULT NULL,
    i_po_num      IN  VARCHAR2 DEFAULT NULL,
    i_split_typ   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION order_detail_fn(
    i_div         IN  VARCHAR2,
    i_cust_id     IN  VARCHAR2,
    i_llr_dt      IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_fro  IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_to   IN  VARCHAR2 DEFAULT NULL,
    i_load_typ    IN  VARCHAR2 DEFAULT NULL,
    i_ord_typ     IN  VARCHAR2 DEFAULT NULL,
    i_load_num    IN  VARCHAR2 DEFAULT NULL,
    i_ord_num     IN  NUMBER DEFAULT NULL,
    i_po_num      IN  VARCHAR2 DEFAULT NULL,
    i_split_typ   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_detail_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE get_llr_depart_for_load_eta_sp(
    i_div        IN      VARCHAR2,
    i_load_num   IN      VARCHAR2,
    i_eta_ts     IN      DATE,
    o_llr_ts     OUT     DATE,
    o_depart_ts  OUT     DATE
  );

  PROCEDURE move_orders_sp(
    i_div       IN      VARCHAR2,
    i_cust_id   IN      VARCHAR2,
    i_load_num  IN      VARCHAR2,
    i_stop_num  IN      PLS_INTEGER,
    i_eta_dt    IN      VARCHAR2,
    i_eta_tm    IN      NUMBER,
    i_ord_list  IN      CLOB,
    i_user_id   IN      VARCHAR2,
    o_msg       OUT     VARCHAR2,
    i_rsn_cd    IN      VARCHAR2 DEFAULT 'FORCELD'
  );

  PROCEDURE move_orders_by_stop_sp(
    i_div         IN      VARCHAR2,
    i_llr_dt      IN      VARCHAR2,
    i_cust_id     IN      VARCHAR2,
    i_load_num    IN      VARCHAR2,
    i_stop_num    IN      PLS_INTEGER,
    i_new_load    IN      VARCHAR2,
    i_new_stop    IN      NUMBER,
    i_new_eta_dt  IN      VARCHAR2,
    i_new_eta_tm  IN      NUMBER,
    i_user_id     IN      VARCHAR2,
    o_msg         OUT     VARCHAR2,
    i_rsn_cd      IN      VARCHAR2 DEFAULT 'LOADBAL'
  );

  PROCEDURE move_stops_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    i_from_load  IN      VARCHAR2,
    i_to_load    IN      VARCHAR2,
    i_parm_list  IN      CLOB,
    i_user_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2
  );

  PROCEDURE upd_eta_time_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    i_load_num   IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    i_parm_list  IN      CLOB,
    o_msg        OUT     VARCHAR2
  );
END op_order_moves_pk;
/

CREATE OR REPLACE PACKAGE BODY op_order_moves_pk IS
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
  || SPLIT_TYP_LIST_FN
  ||   Build a cursor of Split Types.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/07 | rhalpai | Original. PIR5341
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.SPLIT_TYP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   sd.split_typ, sd.descr
           FROM split_dmn_op8s sd
       ORDER BY sd.split_typ;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LLR_DATE_LIST_FN
  ||   Build a cursor of LLR Dates for open orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Original. PIR3593
  || 06/16/08 | rhalpai | Added sort by LLRDt to cursor.
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION llr_date_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.LLR_DATE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_ts, 'YYYY-MM-DD')
           FROM div_mstr_di1d d, load_depart_op1f ld
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.stata = 'O')
       GROUP BY TO_CHAR(ld.llr_ts, 'YYYY-MM-DD')
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END llr_date_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || SHIP_DATE_LIST_FN
  ||   Build a cursor of Ship Dates for unassigned distribution orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Original. PIR3593
  || 06/16/08 | rhalpai | Added sort by ShipDt to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ship_date_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.SHIP_DATE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_ts = DATE '1900-01-01'
            AND (   ld.load_num = 'DIST'
                 OR ld.load_num BETWEEN 'P00P' AND 'P99P')
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.excptn_sw = 'N'
            AND a.dsorda = 'D'
            AND a.stata = 'O'
       GROUP BY a.shpja
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ship_date_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_TYP_LIST_FN
  ||   Build a cursor of load types.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Original. PIR3593
  || 04/10/08 | rhalpai | Changed to include exceptions.
  || 08/04/08 | rhalpai | Added PAL to cursor. IM432974
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_typ_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.LOAD_TYP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   'PAL'
           FROM DUAL
       UNION
       SELECT   a.ldtypa
           FROM div_mstr_di1d d, ordp100a a
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
            AND a.stata = 'O'
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FN
  ||   Build a cursor of defined loads.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Original. PIR3593
  || 01/31/08 | rhalpai | Removed exclusion of DFLT,DIST load and added special
  ||                    | distribution loads. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   c.loadc, c.destc
           FROM div_mstr_di1d d, mclp120c c
          WHERE d.div_id = i_div
            AND c.div_part = d.div_part
            AND c.loadc NOT IN('LOST', 'COPY')
       UNION ALL
       SELECT   'P' || LPAD(t.column_value, 2, '0') || 'P', 'Special Distribution Load'
           FROM TABLE(pivot_fn(100, 0)) t
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FOR_ORDS_FN
  ||   Build a cursor of loads for open orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Original. PIR3593
  || 12/05/07 | rhalpai | Changed exclude DIST load only when indicated by
  ||                    | parm P_INCL_RESERVED_LOADS. PIR3593
  || 01/31/08 | rhalpai | Changed to include P%%P loads when indicated by
  ||                    | parm P_INCL_RESERVED_LOADS. PIR3593
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_for_ords_fn(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  VARCHAR2 DEFAULT NULL,
    i_ord_typ        IN  VARCHAR2 DEFAULT NULL,
    i_incl_xload_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.LOAD_LIST_FOR_ORDS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_incl_xload_sw      VARCHAR2(1);
    l_t_xloads           type_stab;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'InclXLoadSw', i_incl_xload_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_incl_xload_sw := NVL(UPPER(i_incl_xload_sw), 'N');
    l_t_xloads :=(CASE
                    WHEN l_incl_xload_sw = 'N' THEN op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload)
                  END
                 );
    l_llr_dt :=(CASE
                  WHEN i_llr_dt IS NOT NULL THEN TO_DATE(i_llr_dt, 'YYYY-MM-DD')
                END);

    OPEN l_cv
     FOR
       SELECT   x.load_num, c.destc
           FROM (SELECT   ld.load_num
                     FROM load_depart_op1f ld, ordp100a a
                    WHERE ld.div_part = l_div_part
                      AND ld.llr_dt = DECODE(i_llr_dt, NULL, ld.llr_dt, l_llr_dt)
                      AND (   l_incl_xload_sw <> 'N'
                           OR ld.load_num NOT IN(SELECT t.column_value
                                                   FROM TABLE(CAST(l_t_xloads AS type_stab)) t))
                      AND a.div_part = ld.div_part
                      AND a.load_depart_sid = ld.load_depart_sid
                      AND (   i_ord_typ IS NULL
                           OR a.dsorda = i_ord_typ)
                      AND a.stata = 'O'
                 GROUP BY ld.div_part, ld.load_num) x,
                mclp120c c
          WHERE c.div_part(+) = l_div_part
            AND c.loadc(+) = x.load_num
       ORDER BY x.load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_for_ords_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_LIST_FN
  ||  Return cursor of stops available for ETA/Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/04/06 | rhalpai | Original. PIR3593
  || 03/14/11 | rhalpai | Changed cursor to include new override table for
  ||                    | LLRDate/Cust/Load/Stop to obtain assignment info.
  ||                    | PIR9348
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/08/13 | rhalpai | Assume 23:59 as ETA time since only date portion of
  ||                    | ETA is passed. IM-121053
  || 10/28/13 | rhalpai | Change logic to include Load as part of primary key
  ||                    | of CUST_RTE_OVRRD_RT3C and include StopNum when
  ||                    | StopOvrrdSw is ON. IM-123463
  || 01/08/15 | rhalpai | Change logic to restrict stop zero from non-reserved
  ||                    | loads (such as DFLT,DIST,etc). IM-228705
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_eta_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.STOP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_eta_ts             DATE;
    l_t_xloads           type_stab;
    l_num_rows           NUMBER;
    l_start_num          NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_llr_ts             DATE;
    l_depart_ts          DATE;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EtaDt', i_eta_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_eta_ts := TO_DATE(i_eta_dt || ' 23:59', 'YYYY-MM-DD HH24:MI');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Allow starting with Stop 0 for XLOADs');

    IF (    i_load_num <> 'ROUT'
        AND i_load_num MEMBER OF l_t_xloads) THEN
      l_num_rows := 100;
      l_start_num := 0;
    ELSE
      l_num_rows := 99;
      l_start_num := 1;
    END IF;   -- i_load_num <> 'ROUT' AND i_load_num MEMBER OF l_t_xloads

    logs.dbg('Get LLR Date for Load/ETA');
    get_llr_depart_for_load_eta_sp(i_div, i_load_num, l_eta_ts, l_llr_ts, l_depart_ts);
    l_llr_dt := TRUNC(l_llr_ts);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   NVL(x.assgn_sw, 'N') AS assgn_sw, LPAD(t.column_value, 2, '0') AS stop_num,
                (SELECT cx.mccusb
                   FROM mclp020b cx
                  WHERE cx.div_part = l_div_part
                    AND cx.custb = x.cust_id
                    AND ROWNUM = 1) AS mcl_cust, x.namec AS cust_name, x.shpzpc AS zip,
                TO_CHAR(x.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts, x.calc_eta
           FROM TABLE(pivot_fn(l_num_rows, l_start_num)) t,
                (SELECT NVL((SELECT MAX('Y')
                               FROM cust_rte_ovrrd_rt3c r
                              WHERE r.div_part = se.div_part
                                AND r.cust_id = se.cust_id
                                AND r.llr_dt = l_llr_dt
                                AND r.load_num = i_load_num
                                AND r.stop_num = se.stop_num
                                AND r.stop_ovrrd_sw = 'Y'),
                            (SELECT MAX('Y')
                               FROM mclp040d d
                              WHERE d.div_part = se.div_part
                                AND d.custd = se.cust_id
                                AND d.loadd = i_load_num
                                AND d.stopd = se.stop_num)
                           ) AS assgn_sw,
                        se.stop_num, se.cust_id, c.namec, c.shpzpc, se.eta_ts, '' AS calc_eta
                   FROM load_depart_op1f ld, stop_eta_op1g se, sysp200c c
                  WHERE ld.div_part = l_div_part
                    AND ld.llr_ts = l_llr_ts
                    AND ld.load_num = i_load_num
                    AND se.div_part = ld.div_part
                    AND se.load_depart_sid = ld.load_depart_sid
                    AND EXISTS(SELECT 1
                                 FROM ordp100a a, ordp120b b
                                WHERE a.div_part = ld.div_part
                                  AND a.load_depart_sid = ld.load_depart_sid
                                  AND a.custa = se.cust_id
                                  AND a.stata IN('O', 'P')
                                  AND b.div_part = a.div_part
                                  AND b.ordnob = a.ordnoa
                                  AND b.statb = 'O')
                    AND c.div_part = se.div_part
                    AND c.acnoc = se.cust_id
                 UNION
                 SELECT 'Y' AS assgn_sw, r.stop_num, r.cust_id, c.namec, c.shpzpc, r.eta_ts, '' AS calc_eta
                   FROM mclp120c l, cust_rte_ovrrd_rt3c r, sysp200c c
                  WHERE l.div_part = l_div_part
                    AND l.loadc = i_load_num
                    AND r.div_part = l.div_part
                    AND r.llr_dt = l_llr_dt
                    AND r.load_num = l.loadc
                    AND r.stop_ovrrd_sw = 'Y'
                    AND c.div_part = r.div_part
                    AND c.acnoc = r.cust_id
                 UNION
                 SELECT 'Y' AS assgn_sw, d.stopd AS stop_num, d.custd AS cust_id, c.namec, c.shpzpc,
                        TO_DATE(TO_CHAR(NEXT_DAY(l_depart_ts - 1, d.dayrcd) +(NVL(d.wkoffd, 0) * 7), 'YYYYMMDD')
                                || lpad_fn(d.etad, 4, '0'),
                                'YYYYMMDDHH24MI'
                               ) AS eta_ts,
                        'Y' AS calc_eta
                   FROM mclp120c l, mclp040d d, sysp200c c
                  WHERE l.div_part = l_div_part
                    AND l.loadc = i_load_num
                    AND d.div_part = l.div_part
                    AND d.loadd = l.loadc
                    AND NOT EXISTS(SELECT 1
                                     FROM cust_rte_ovrrd_rt3c r
                                    WHERE r.div_part = d.div_part
                                      AND r.cust_id = d.custd
                                      AND r.llr_dt = l_llr_dt
                                      AND r.load_num = i_load_num)
                    AND NOT EXISTS(SELECT 1
                                     FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
                                    WHERE ld.div_part = d.div_part
                                      AND ld.llr_ts = l_llr_ts
                                      AND ld.load_num = i_load_num
                                      AND se.div_part = ld.div_part
                                      AND se.load_depart_sid = ld.load_depart_sid
                                      AND se.cust_id = d.custd
                                      AND a.div_part = ld.div_part
                                      AND a.load_depart_sid = ld.load_depart_sid
                                      AND a.custa = se.cust_id
                                      AND a.stata IN('O', 'P')
                                      AND b.div_part = a.div_part
                                      AND b.ordnob = a.ordnoa
                                      AND b.statb = 'O')
                    AND c.div_part = d.div_part
                    AND c.acnoc = d.custd) x
          WHERE x.stop_num(+) = t.column_value
       ORDER BY stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CUST_DETAIL_FN
  ||   Retrieve customer info for customers with orders meeting selection
  ||   criteria.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/07 | rhalpai | Original
  || 12/12/07 | rhalpai | Added Split Typ parm and changed to allow NULL LLR Date.
  ||                    | PIR3593
  || 04/02/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 08/04/08 | rhalpai | Changed to include customer for order on DIST and
  ||                    | P__P loads when searching by order number.
  ||                    | Added logic to handle PAL load type which will include
  ||                    | all unassigned special distributions. IM432974
  || 08/22/08 | rhalpai | Changed to allow case-insensitive search on PO.
  || 09/08/08 | rhalpai | Removed check for detail with status not in O,I,S,C in
  ||                    | cursors. PIR6364
  || 10/01/10 | rhalpai | Changed cursors to allow search on split type to include
  ||                    | distributions. PIR8859
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_detail_fn(
    i_div         IN  VARCHAR2,
    i_llr_dt      IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_fro  IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_to   IN  VARCHAR2 DEFAULT NULL,
    i_load_typ    IN  VARCHAR2 DEFAULT NULL,
    i_ord_typ     IN  VARCHAR2 DEFAULT NULL,
    i_load_num    IN  VARCHAR2 DEFAULT NULL,
    i_ord_num     IN  NUMBER DEFAULT NULL,
    i_crp_cd      IN  NUMBER DEFAULT NULL,
    i_grp_cd      IN  VARCHAR2 DEFAULT NULL,
    i_cust_typ    IN  VARCHAR2 DEFAULT 'MCL',
    i_cust_id     IN  VARCHAR2 DEFAULT NULL,
    i_po_num      IN  VARCHAR2 DEFAULT NULL,
    i_split_typ   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm        := 'OP_ORDER_MOVES_PK.CUST_DETAIL_FN';
    lar_parm             logs.tar_parm;
    l_po_num             ordp100a.cpoa%TYPE;
    l_llr_dt             DATE;
    l_shp_dt_fro         NUMBER;
    l_shp_dt_to          NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ShpDtFro', i_shp_dt_fro);
    logs.add_parm(lar_parm, 'ShpDtTo', i_shp_dt_to);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'GrpCd', i_grp_cd);
    logs.add_parm(lar_parm, 'CustTyp', i_cust_typ);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_po_num := UPPER(i_po_num);

    IF i_llr_dt IS NOT NULL THEN
      l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    END IF;   -- i_llr_dt IS NOT NULL

    IF i_shp_dt_fro IS NOT NULL THEN
      l_shp_dt_fro := TO_DATE(i_shp_dt_fro, 'YYYY-MM-DD') - DATE '1900-02-28';
      l_shp_dt_to := TO_DATE(i_shp_dt_to, 'YYYY-MM-DD') - DATE '1900-02-28';
    END IF;   -- i_shp_dt_fro IS NOT NULL

    CASE
      WHEN(    i_load_typ = 'PAL'
           AND l_shp_dt_fro IS NOT NULL) THEN
        logs.dbg('Open Cursor for Customers with Unassigned Special Dist');

        OPEN l_cv
         FOR
           SELECT   cx.mccusb, cx.custb, c.namec, c.shpctc, c.shpstc, c.shpzpc,
                    op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone
               FROM div_mstr_di1d d, mclp020b cx, sysp200c c
              WHERE d.div_id = i_div
                AND cx.div_part = d.div_part
                AND (   i_crp_cd IS NULL
                     OR cx.corpb = i_crp_cd)
                AND (   i_cust_id IS NULL
                     OR (    i_cust_typ = 'MCL'
                         AND cx.mccusb = i_cust_id)
                     OR (    i_cust_typ = 'CBR'
                         AND cx.custb = i_cust_id)
                    )
                AND c.div_part = cx.div_part
                AND c.acnoc = cx.custb
                AND (   i_grp_cd IS NULL
                     OR c.retgpc = i_grp_cd)
                AND EXISTS(SELECT 1
                             FROM load_depart_op1f ld, ordp100a a
                            WHERE a.div_part = cx.div_part
                              AND a.custa = cx.custb
                              AND a.stata = 'O'
                              AND a.dsorda = 'D'
                              AND a.shpja BETWEEN l_shp_dt_fro AND l_shp_dt_to
                              AND a.ldtypa BETWEEN 'P00' AND 'P99'
                              AND ld.div_part = a.div_part
                              AND ld.load_depart_sid = a.load_depart_sid
                              AND ld.llr_ts = DATE '1900-01-01'
                              AND ld.load_num IN('DIST', a.ldtypa || 'P')
                              AND (   i_split_typ IS NULL
                                   OR EXISTS(SELECT 1
                                               FROM split_ord_op2s s
                                              WHERE s.div_part = a.div_part
                                                AND s.ord_num = a.ordnoa
                                                AND s.split_typ = i_split_typ)
                                   OR EXISTS(SELECT 1
                                               FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND s.div_part = a.div_part
                                                AND si.div_part = s.div_part
                                                AND si.cbr_vndr_id = s.cbr_vndr_id
                                                AND e.iteme = si.item_num
                                                AND e.uome = si.uom
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0
                                                AND e.catite IN(b.orgitb, b.orditb))
                                   OR EXISTS(SELECT 1
                                               FROM split_sta_itm_op1s s, mclp030c ct, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND ct.div_part = a.div_part
                                                AND ct.custc = a.custa
                                                AND ct.taxjrc = s.state_cd
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                   OR EXISTS(SELECT 1
                                               FROM split_cus_itm_op1c s, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND s.div_part = a.div_part
                                                AND s.cbr_cust = a.custa
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                  )
                              AND (   l_po_num IS NULL
                                   OR UPPER(a.cpoa) LIKE l_po_num || '%'))
           ORDER BY cx.mccusb;
      WHEN l_shp_dt_fro IS NOT NULL THEN
        logs.dbg('Open Cursor for Customers with Unassigned Distributions');

        OPEN l_cv
         FOR
           SELECT   cx.mccusb, cx.custb, c.namec, c.shpctc, c.shpstc, c.shpzpc,
                    op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone
               FROM div_mstr_di1d d, mclp020b cx, sysp200c c
              WHERE d.div_id = i_div
                AND cx.div_part = d.div_part
                AND (   i_crp_cd IS NULL
                     OR cx.corpb = i_crp_cd)
                AND (   i_cust_id IS NULL
                     OR (    i_cust_typ = 'MCL'
                         AND cx.mccusb = i_cust_id)
                     OR (    i_cust_typ = 'CBR'
                         AND cx.custb = i_cust_id)
                    )
                AND c.div_part = cx.div_part
                AND c.acnoc = cx.custb
                AND (   i_grp_cd IS NULL
                     OR c.retgpc = i_grp_cd)
                AND EXISTS(SELECT 1
                             FROM load_depart_op1f ld, ordp100a a
                            WHERE a.div_part = cx.div_part
                              AND a.custa = cx.custb
                              AND a.stata = 'O'
                              AND a.dsorda = 'D'
                              AND a.shpja BETWEEN l_shp_dt_fro AND l_shp_dt_to
                              AND ld.div_part = a.div_part
                              AND ld.load_depart_sid = a.load_depart_sid
                              AND ld.llr_ts = DATE '1900-01-01'
                              AND ld.load_num IN('DIST', a.ldtypa || 'P')
                              AND (   i_load_typ IS NULL
                                   OR (    a.ldtypa = i_load_typ
                                       AND (   i_load_typ NOT BETWEEN 'P00' AND 'P99'
                                            OR ld.load_num = i_load_typ || 'P')
                                      )
                                  )
                              AND (   i_split_typ IS NULL
                                   OR EXISTS(SELECT 1
                                               FROM split_ord_op2s s
                                              WHERE s.div_part = a.div_part
                                                AND s.ord_num = a.ordnoa
                                                AND s.split_typ = i_split_typ)
                                   OR EXISTS(SELECT 1
                                               FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND s.div_part = a.div_part
                                                AND si.div_part = s.div_part
                                                AND si.cbr_vndr_id = s.cbr_vndr_id
                                                AND e.iteme = si.item_num
                                                AND e.uome = si.uom
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND e.catite IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                   OR EXISTS(SELECT 1
                                               FROM split_sta_itm_op1s s, mclp030c ct, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND ct.div_part = a.div_part
                                                AND ct.custc = a.custa
                                                AND ct.taxjrc = s.state_cd
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                   OR EXISTS(SELECT 1
                                               FROM split_cus_itm_op1c s, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND s.div_part = a.div_part
                                                AND s.cbr_cust = a.custa
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                  )
                              AND (   l_po_num IS NULL
                                   OR UPPER(a.cpoa) LIKE l_po_num || '%'))
           ORDER BY cx.mccusb;
      WHEN i_ord_num IS NOT NULL THEN
        logs.dbg('Open Cursor for Customer for Order Num');

        OPEN l_cv
         FOR
           SELECT cx.mccusb, cx.custb, c.namec, c.shpctc, c.shpstc, c.shpzpc,
                  op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_id = i_div
              AND cx.div_part = d.div_part
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd)
              AND (   i_cust_id IS NULL
                   OR (    i_cust_typ = 'MCL'
                       AND cx.mccusb = i_cust_id)
                   OR (    i_cust_typ = 'CBR'
                       AND cx.custb = i_cust_id)
                  )
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb
              AND (   i_grp_cd IS NULL
                   OR c.retgpc = i_grp_cd)
              AND EXISTS(SELECT 1
                           FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se
                          WHERE a.div_part = cx.div_part
                            AND a.ordnoa = i_ord_num
                            AND a.custa = cx.custb
                            AND a.stata = 'O'
                            AND (   i_ord_typ IS NULL
                                 OR a.dsorda = i_ord_typ)
                            AND (   l_po_num IS NULL
                                 OR UPPER(a.cpoa) LIKE l_po_num || '%')
                            AND ld.div_part = a.div_part
                            AND ld.load_depart_sid = a.load_depart_sid
                            AND (   l_llr_dt IS NULL
                                 OR ld.llr_dt = l_llr_dt)
                            AND (   i_load_num IS NULL
                                 OR ld.load_num = i_load_num)
                            AND se.div_part = a.div_part
                            AND se.load_depart_sid = a.load_depart_sid
                            AND se.cust_id = a.custa
                            AND (   i_split_typ IS NULL
                                 OR EXISTS(SELECT 1
                                             FROM split_ord_op2s s
                                            WHERE s.div_part = a.div_part
                                              AND s.ord_num = a.ordnoa
                                              AND s.split_typ = i_split_typ)
                                 OR EXISTS(SELECT 1
                                             FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                            WHERE s.split_typ = i_split_typ
                                              AND s.div_part = a.div_part
                                              AND si.div_part = s.div_part
                                              AND si.cbr_vndr_id = s.cbr_vndr_id
                                              AND e.iteme = si.item_num
                                              AND e.uome = si.uom
                                              AND b.div_part = a.div_part
                                              AND b.ordnob = a.ordnoa
                                              AND e.catite IN(b.orgitb, b.orditb)
                                              AND b.excptn_sw = 'N'
                                              AND b.statb = 'O'
                                              AND b.subrcb = 0)
                                 OR EXISTS(SELECT 1
                                             FROM split_sta_itm_op1s s, mclp030c ct, ordp120b b
                                            WHERE s.split_typ = i_split_typ
                                              AND ct.div_part = a.div_part
                                              AND ct.custc = a.custa
                                              AND ct.taxjrc = s.state_cd
                                              AND b.div_part = a.div_part
                                              AND b.ordnob = a.ordnoa
                                              AND s.mcl_item IN(b.orgitb, b.orditb)
                                              AND b.excptn_sw = 'N'
                                              AND b.statb = 'O'
                                              AND b.subrcb = 0)
                                 OR EXISTS(SELECT 1
                                             FROM split_cus_itm_op1c s, ordp120b b
                                            WHERE s.split_typ = i_split_typ
                                              AND s.div_part = a.div_part
                                              AND s.cbr_cust = a.custa
                                              AND b.div_part = a.div_part
                                              AND b.ordnob = a.ordnoa
                                              AND s.mcl_item IN(b.orgitb, b.orditb)
                                              AND b.excptn_sw = 'N'
                                              AND b.statb = 'O'
                                              AND b.subrcb = 0)
                                )
                            AND NOT EXISTS(SELECT 1
                                             FROM rte_stat_rt1s r, rte_grp_ord_rt3o o
                                            WHERE ld.load_num = 'ROUT'
                                              AND a.dsorda = 'D'
                                              AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                              AND r.stop_num = se.stop_num
                                              AND r.mcl_cust = cx.mccusb
                                              AND r.stat_cd IN('SNT', 'WRK')
                                              AND o.rte_grp_num = r.rte_grp_num
                                              AND o.ord_num = a.ordnoa));
      ELSE
        logs.dbg('Open Cursor Customers for Order Info');

        OPEN l_cv
         FOR
           SELECT   cx.mccusb, cx.custb, c.namec, c.shpctc, c.shpstc, c.shpzpc,
                    op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone
               FROM div_mstr_di1d d, mclp020b cx, sysp200c c
              WHERE d.div_id = i_div
                AND cx.div_part = d.div_part
                AND (   i_crp_cd IS NULL
                     OR cx.corpb = i_crp_cd)
                AND (   i_cust_id IS NULL
                     OR (    i_cust_typ = 'MCL'
                         AND cx.mccusb = i_cust_id)
                     OR (    i_cust_typ = 'CBR'
                         AND cx.custb = i_cust_id)
                    )
                AND c.div_part = cx.div_part
                AND c.acnoc = cx.custb
                AND (   i_grp_cd IS NULL
                     OR c.retgpc = i_grp_cd)
                AND EXISTS(SELECT 1
                             FROM ordp100a a, load_depart_op1f ld
                            WHERE a.div_part = cx.div_part
                              AND a.custa = cx.custb
                              AND a.stata = 'O'
                              AND (   i_ord_typ IS NULL
                                   OR a.dsorda = i_ord_typ)
                              AND (   l_po_num IS NULL
                                   OR UPPER(a.cpoa) LIKE l_po_num || '%')
                              AND ld.div_part = d.div_part
                              AND ld.load_depart_sid = a.load_depart_sid
                              AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
                              AND ld.load_num <> 'DIST'
                              AND ld.llr_dt = DECODE(l_llr_dt, NULL, ld.llr_dt, l_llr_dt)
                              AND (   i_load_num IS NULL
                                   OR ld.load_num = i_load_num)
                              AND (   i_split_typ IS NULL
                                   OR EXISTS(SELECT 1
                                               FROM split_ord_op2s s
                                              WHERE s.div_part = a.div_part
                                                AND s.ord_num = a.ordnoa
                                                AND s.split_typ = i_split_typ)
                                   OR EXISTS(SELECT 1
                                               FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND s.div_part = a.div_part
                                                AND si.div_part = s.div_part
                                                AND si.cbr_vndr_id = s.cbr_vndr_id
                                                AND e.iteme = si.item_num
                                                AND e.uome = si.uom
                                                AND b.div_part = s.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND e.catite IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                   OR EXISTS(SELECT 1
                                               FROM split_sta_itm_op1s s, mclp030c ct, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND ct.div_part = a.div_part
                                                AND ct.custc = a.custa
                                                AND ct.taxjrc = s.state_cd
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                   OR EXISTS(SELECT 1
                                               FROM split_cus_itm_op1c s, ordp120b b
                                              WHERE s.split_typ = i_split_typ
                                                AND s.div_part = a.div_part
                                                AND s.cbr_cust = a.custa
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = a.ordnoa
                                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                                AND b.excptn_sw = 'N'
                                                AND b.statb = 'O'
                                                AND b.subrcb = 0)
                                  )
                              AND NOT EXISTS(SELECT 1
                                               FROM rte_stat_rt1s r, rte_grp_ord_rt3o o, stop_eta_op1g se
                                              WHERE ld.load_num = 'ROUT'
                                                AND a.dsorda = 'D'
                                                AND se.div_part = a.div_part
                                                AND se.load_depart_sid = a.load_depart_sid
                                                AND se.cust_id = a.custa
                                                AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                                AND r.stop_num = se.stop_num
                                                AND r.mcl_cust = cx.mccusb
                                                AND r.stat_cd IN('SNT', 'WRK')
                                                AND o.rte_grp_num = r.rte_grp_num
                                                AND o.ord_num = a.ordnoa))
           ORDER BY cx.mccusb;
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_detail_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORDER_DETAIL_FN
  ||   Retrieve order info for selection criteria.
  ||   Requirements:  Div,Cust and (LLRDate or FromShip,ToShip,LoadTyp)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/06 | rhalpai | Original
  || 12/12/07 | rhalpai | Added Split Typ parm and changed to allow NULL LLR Date.
  ||                    | PIR3593
  || 04/02/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 06/16/08 | rhalpai | Added sort by Load/Stop/OrdTyp/OrdNum to cursor.
  || 08/04/08 | rhalpai | Changed to include order on DIST and P__P loads when
  ||                    | searching by order number.
  ||                    | Added logic to handle PAL load type which will include
  ||                    | all unassigned special distributions. IM432974
  || 08/22/08 | rhalpai | Changed to allow case-insensitive search on PO.
  || 09/08/08 | rhalpai | Removed check for detail with status not in O,I,S,C in
  ||                    | cursors. PIR6364
  || 08/26/10 | rhalpai | Changed cursor to use weight and cube from CorpItem
  ||                    | table SAWP505E instead of from OrderDetail table.
  ||                    | PIR8531
  || 10/01/10 | rhalpai | Changed cursors to allow search on split type to include
  ||                    | distributions. PIR8859
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/09/13 | rhalpai | Change logic to split the cursor into one for
  ||                    | Unassigned Distributions process and one for Move
  ||                    | Orders process as a work-around for bug in 10G.
  ||                    | IM-118376
  ||----------------------------------------------------------------------------
  */
  FUNCTION order_detail_fn(
    i_div         IN  VARCHAR2,
    i_cust_id     IN  VARCHAR2,
    i_llr_dt      IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_fro  IN  VARCHAR2 DEFAULT NULL,
    i_shp_dt_to   IN  VARCHAR2 DEFAULT NULL,
    i_load_typ    IN  VARCHAR2 DEFAULT NULL,
    i_ord_typ     IN  VARCHAR2 DEFAULT NULL,
    i_load_num    IN  VARCHAR2 DEFAULT NULL,
    i_ord_num     IN  NUMBER DEFAULT NULL,
    i_po_num      IN  VARCHAR2 DEFAULT NULL,
    i_split_typ   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm        := 'OP_ORDER_MOVES_PK.ORDER_DETAIL_FN';
    lar_parm             logs.tar_parm;
    l_po_num             ordp100a.cpoa%TYPE;
    l_llr_dt             DATE;
    l_shp_dt_fro         NUMBER;
    l_shp_dt_to          NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ShpDtFro', i_shp_dt_fro);
    logs.add_parm(lar_parm, 'ShpDtTo', i_shp_dt_to);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_po_num := UPPER(i_po_num);

    IF i_shp_dt_fro IS NOT NULL THEN
      l_shp_dt_fro := TO_DATE(i_shp_dt_fro, 'YYYY-MM-DD') - DATE '1900-02-28';
      l_shp_dt_to := TO_DATE(i_shp_dt_to, 'YYYY-MM-DD') - DATE '1900-02-28';
      logs.dbg('Open Unassigned Dist Cursor');

      OPEN l_cv
       FOR
         SELECT   ld.load_num,(CASE
                                 WHEN se.stop_num < 10 THEN '0' || se.stop_num
                                 ELSE TO_CHAR(se.stop_num)
                               END) AS stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num,
                  DECODE(a.dsorda, 'D', 'DIS', 'REG') AS ord_typ, a.cpoa AS po_num,
                  TO_CHAR(ld.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                  TO_CHAR(ld.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                  TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_dt,
                  (SELECT TO_CHAR(NVL(ROUND(SUM(e.wghte * b.ordqtb), 1), 0), 'FM999999990.0')
                     FROM ordp120b b, sawp505e e
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb = 'O'
                      AND b.ntshpb IS NULL
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb) AS prod_wt,
                  (SELECT TO_CHAR(NVL(ROUND(SUM(e.cubee * b.ordqtb), 1), 0), 'FM999999990.0')
                     FROM ordp120b b, sawp505e e
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb = 'O'
                      AND b.ntshpb IS NULL
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb) AS prod_cube,
                  'N' AS test_sw, NULL AS split_typ
             FROM div_mstr_di1d d, mclp020b cx, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
            WHERE d.div_id = i_div
              AND cx.div_part = d.div_part
              AND cx.custb = i_cust_id
              AND a.div_part = cx.div_part
              AND a.custa = cx.custb
              AND a.stata = 'O'
              AND a.dsorda = 'D'
              AND a.shpja BETWEEN l_shp_dt_fro AND l_shp_dt_to
              AND (   l_po_num IS NULL
                   OR UPPER(a.cpoa) LIKE l_po_num || '%')
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND ld.llr_ts = DATE '1900-01-01'
              AND (   ld.load_num = 'DIST'
                   OR ld.load_num BETWEEN 'P00P' AND 'P99P')
              AND (   i_load_typ IS NULL
                   OR (    i_load_typ = 'PAL'
                       AND a.ldtypa BETWEEN 'P00' AND 'P99'
                       AND ld.load_num IN('DIST', a.ldtypa || 'P')
                      )
                   OR (    a.ldtypa = i_load_typ
                       AND (   i_load_typ NOT BETWEEN 'P00' AND 'P99'
                            OR ld.load_num = i_load_typ || 'P')
                      )
                  )
              AND se.div_part = a.div_part
              AND se.load_depart_sid = a.load_depart_sid
              AND se.cust_id = a.custa
         ORDER BY 1, 2, 3, 4;
    ELSE
      IF i_llr_dt IS NOT NULL THEN
        l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
      END IF;   -- i_llr_dt IS NOT NULL

      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT   ld.load_num,(CASE
                                 WHEN se.stop_num < 10 THEN '0' || se.stop_num
                                 ELSE TO_CHAR(se.stop_num)
                               END) AS stop_num, a.ldtypa AS load_typ, a.ordnoa AS ord_num,
                  DECODE(a.dsorda, 'D', 'DIS', 'REG') AS ord_typ, a.cpoa AS po_num,
                  TO_CHAR(ld.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                  TO_CHAR(ld.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                  TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                  (SELECT TO_CHAR(NVL(ROUND(SUM(e.wghte * b.ordqtb), 1), 0), 'FM999999990.0')
                     FROM ordp120b b, sawp505e e
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb = 'O'
                      AND b.ntshpb IS NULL
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb) AS prod_wt,
                  (SELECT TO_CHAR(NVL(ROUND(SUM(e.cubee * b.ordqtb), 1), 0), 'FM999999990.0')
                     FROM ordp120b b, sawp505e e
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb = 'O'
                      AND b.ntshpb IS NULL
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb) AS prod_cube,
                  DECODE(a.dsorda, 'T', 'Y', 'N') AS test_sw,
                  (SELECT DISTINCT FIRST_VALUE(sd.split_typ) OVER(ORDER BY sd.priorty)
                              FROM split_dmn_op8s sd
                             WHERE sd.split_typ = DECODE(i_split_typ, NULL, sd.split_typ, i_split_typ)
                               AND (   EXISTS(SELECT 1
                                                FROM split_ord_op2s so
                                               WHERE so.div_part = a.div_part
                                                 AND so.ord_num = a.ordnoa
                                                 AND so.split_typ = sd.split_typ)
                                    OR EXISTS(SELECT 1
                                                FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                               WHERE s.split_typ = sd.split_typ
                                                 AND s.div_part = a.div_part
                                                 AND si.div_part = s.div_part
                                                 AND si.cbr_vndr_id = s.cbr_vndr_id
                                                 AND e.iteme = si.item_num
                                                 AND e.uome = si.uom
                                                 AND b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND e.catite IN(b.orgitb, b.orditb)
                                                 AND b.excptn_sw = 'N'
                                                 AND b.statb = 'O'
                                                 AND b.subrcb = 0)
                                    OR EXISTS(SELECT 1
                                                FROM split_sta_itm_op1s s, mclp030c mc, ordp120b b
                                               WHERE s.split_typ = sd.split_typ
                                                 AND mc.div_part = a.div_part
                                                 AND mc.custc = i_cust_id
                                                 AND mc.taxjrc = s.state_cd
                                                 AND b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND s.mcl_item IN(b.orgitb, b.orditb)
                                                 AND b.excptn_sw = 'N'
                                                 AND b.statb = 'O'
                                                 AND b.subrcb = 0)
                                    OR EXISTS(SELECT 1
                                                FROM split_cus_itm_op1c s, ordp120b b
                                               WHERE s.split_typ = sd.split_typ
                                                 AND s.div_part = a.div_part
                                                 AND s.cbr_cust = i_cust_id
                                                 AND b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND s.mcl_item IN(b.orgitb, b.orditb)
                                                 AND b.excptn_sw = 'N'
                                                 AND b.statb = 'O'
                                                 AND b.subrcb = 0)
                                   )) AS split_typ
             FROM div_mstr_di1d d, mclp020b cx, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
            WHERE d.div_id = i_div
              AND cx.div_part = d.div_part
              AND cx.custb = i_cust_id
              AND a.div_part = cx.div_part
              AND a.custa = cx.custb
              AND a.stata = 'O'
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND se.div_part = a.div_part
              AND se.load_depart_sid = a.load_depart_sid
              AND se.cust_id = a.custa
              AND NOT EXISTS(SELECT 1
                               FROM rte_stat_rt1s r, rte_grp_ord_rt3o o
                              WHERE ld.load_num = 'ROUT'
                                AND a.dsorda = 'D'
                                AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                AND r.stop_num = se.stop_num
                                AND r.mcl_cust = cx.mccusb
                                AND r.stat_cd IN('SNT', 'WRK')
                                AND o.rte_grp_num = r.rte_grp_num
                                AND o.ord_num = a.ordnoa)
              AND (   i_split_typ IS NULL
                   OR EXISTS(SELECT 1
                               FROM split_ord_op2s s
                              WHERE s.div_part = a.div_part
                                AND s.ord_num = a.ordnoa
                                AND s.split_typ = i_split_typ)
                   OR EXISTS(SELECT 1
                               FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                              WHERE s.split_typ = i_split_typ
                                AND s.div_part = a.div_part
                                AND si.div_part = s.div_part
                                AND si.cbr_vndr_id = s.cbr_vndr_id
                                AND e.iteme = si.item_num
                                AND e.uome = si.uom
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND e.catite IN(b.orgitb, b.orditb)
                                AND b.statb = 'O'
                                AND b.subrcb = 0)
                   OR EXISTS(SELECT 1
                               FROM split_sta_itm_op1s s, mclp030c ct, ordp120b b
                              WHERE s.split_typ = i_split_typ
                                AND ct.div_part = a.div_part
                                AND ct.custc = i_cust_id
                                AND ct.taxjrc = s.state_cd
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                AND b.statb = 'O'
                                AND b.subrcb = 0)
                   OR EXISTS(SELECT 1
                               FROM split_cus_itm_op1c s, ordp120b b
                              WHERE s.split_typ = i_split_typ
                                AND s.div_part = a.div_part
                                AND s.cbr_cust = i_cust_id
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND s.mcl_item IN(b.orgitb, b.orditb)
                                AND b.statb = 'O'
                                AND b.subrcb = 0)
                  )
              AND (   a.ordnoa = i_ord_num
                   OR (    i_ord_num IS NULL
                       AND (    ld.load_num <> 'DIST'
                            AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
                            AND (   l_llr_dt IS NULL
                                 OR ld.llr_dt = l_llr_dt)
                            AND (   i_ord_typ IS NULL
                                 OR a.dsorda = i_ord_typ)
                            AND (   i_load_num IS NULL
                                 OR ld.load_num = i_load_num)
                           )
                       AND (   l_po_num IS NULL
                            OR UPPER(a.cpoa) LIKE l_po_num || '%')
                      )
                  )
         ORDER BY 1, 2, 3, 4;
    END IF;   -- i_shp_dt_fro IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END order_detail_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_DETAIL_FN
  ||   Retrieve customer stop info for div/llr/load.
  ||
  ||  Called by Force Load (Move Stops) UI and ETA Maintenance UI.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/27/06 | rhalpai | Original
  || 04/02/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 06/04/08 | rhalpai | Added sort by stop to cursor. IM417037
  || 09/08/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 08/26/10 | rhalpai | Changed cursor to use weight and cube from CorpItem
  ||                    | table SAWP505E instead of from OrderDetail table.
  ||                    | PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_detail_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.STOP_DETAIL_FN';
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
       SELECT   x.load_typ,(CASE
                              WHEN x.stop_num < 10 THEN '0' || x.stop_num
                              ELSE TO_CHAR(x.stop_num)
                            END) AS stop_num, x.mcl_cust, x.cust_id, x.cust_nm,
                TO_CHAR(x.llr_ts, 'YYYY-MM-DD HH24:MI') AS llr_ts,
                TO_CHAR(x.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                TO_CHAR(x.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts, x.shpctc, x.shpstc, x.shpzpc,
                op_customer_pk.formatted_phone_fn(x.cnphnc) AS phone, SUM(x.wghte * x.ord_qty) AS prod_wt,
                SUM(x.cubee * x.ord_qty) AS prod_cube
           FROM (SELECT a.ldtypa AS load_typ, se.stop_num, cx.mccusb AS mcl_cust, se.cust_id, c.namec AS cust_nm,
                        ld.llr_ts, ld.depart_ts, se.eta_ts, c.shpctc, c.shpstc, c.shpzpc, c.cnphnc, e.wghte, e.cubee,
                        DECODE(b.excptn_sw, 'Y', 0, b.ordqtb) AS ord_qty
                   FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, mclp020b cx,
                        sysp200c c, sawp505e e
                  WHERE d.div_id = i_div
                    AND ld.div_part = d.div_part
                    AND ld.llr_dt = l_llr_dt
                    AND ld.load_num = i_load_num
                    AND a.div_part = ld.div_part
                    AND a.load_depart_sid = ld.load_depart_sid
                    AND a.stata = 'O'
                    AND se.div_part = a.div_part
                    AND se.load_depart_sid = a.load_depart_sid
                    AND se.cust_id = a.custa
                    AND b.div_part = a.div_part
                    AND b.ordnob = a.ordnoa
                    AND b.statb = 'O'
                    AND RTRIM(b.ntshpb) IS NULL
                    AND cx.div_part = a.div_part
                    AND cx.custb = a.custa
                    AND c.div_part = cx.div_part
                    AND c.acnoc = cx.custb
                    AND e.iteme = b.itemnb
                    AND e.uome = b.sllumb
                    AND NOT EXISTS(SELECT 1
                                     FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                    WHERE ld.load_num = 'ROUT'
                                      AND a.dsorda = 'D'
                                      AND g.div_part = d.div_part
                                      AND r.rte_grp_num = g.rte_grp_num
                                      AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                      AND r.stop_num = se.stop_num
                                      AND r.mcl_cust = cx.mccusb
                                      AND r.stat_cd IN('SNT', 'WRK')
                                      AND o.rte_grp_num = g.rte_grp_num
                                      AND o.ord_num = a.ordnoa)) x
       GROUP BY x.load_typ, x.stop_num, x.mcl_cust, x.cust_id, x.cust_nm, x.shpctc, x.shpstc, x.shpzpc, x.cnphnc,
                x.llr_ts, x.depart_ts, x.eta_ts
       ORDER BY x.stop_num DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_detail_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LLR_DEPART_FOR_LOAD_ETA_SP
  ||   Used to calculate the LLR and Departure for a given Load and ETA.
  ||
  ||   ETA WEEK OFFSETS MUST BE SUBTRACTED FROM ETA DATE PRIOR TO PASSING!!!
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/06/03 | rhalpai | Original
  || 07/04/13 | rhalpai | Convert to use date parms which include time. PIR11038
  || 10/28/13 | rhalpai | Change logic to include DepartTs from
  ||                    | CUST_RTE_OVRRD_RT3C when DepartOvrrdSw is ON.
  ||                    | IM-123463
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_llr_depart_for_load_eta_sp(
    i_div        IN      VARCHAR2,
    i_load_num   IN      VARCHAR2,
    i_eta_ts     IN      DATE,
    o_llr_ts     OUT     DATE,
    o_depart_ts  OUT     DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_MOVES_PK.GET_LLR_DEPART_FOR_LOAD_ETA_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_div_part           NUMBER;
    l_llr_day            VARCHAR2(3);
    l_llr_tm             NUMBER;
    l_depart_day         VARCHAR2(3);
    l_depart_tm          NUMBER;
    l_depart_wk          NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'EtaTs', i_eta_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT d.div_part, c.llrcdc, c.llrctc, c.depdac, c.deptmc, c.depwkc
         FROM div_mstr_di1d d, mclp120c c
        WHERE d.div_id = i_div
          AND c.div_part = d.div_part
          AND c.loadc = i_load_num;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_div_part, l_llr_day, l_llr_tm, l_depart_day, l_depart_tm, l_depart_wk;

    IF l_cv%NOTFOUND THEN
      o_llr_ts := DATE '1900-01-01';
      o_depart_ts := DATE '1900-01-01';
    ELSE
      logs.dbg('Assign Values');
      o_depart_ts := TO_DATE(TO_CHAR(NEXT_DAY((i_eta_ts - 7), l_depart_day), 'YYYYMMDD') || lpad_fn(l_depart_tm, 4, '0'),
                             'YYYYMMDDHH24MI'
                            )
                     + NVL(l_depart_wk, 0) * 7;

      IF o_depart_ts > i_eta_ts THEN
        o_depart_ts := o_depart_ts - 7;
      END IF;   -- o_depart_ts > i_eta_ts

      o_llr_ts := TO_DATE(TO_CHAR(NEXT_DAY((o_depart_ts - 7), l_llr_day), 'YYYYMMDD') || lpad_fn(l_llr_tm, 4, '0'),
                          'YYYYMMDDHH24MI'
                         );

      IF o_llr_ts > o_depart_ts THEN
        o_llr_ts := o_llr_ts - 7;
      END IF;   -- o_llr_ts > o_depart_ts

      logs.dbg('Override Depart if Necessary');

      OPEN l_cv
       FOR
         SELECT   cro.depart_ts
             FROM cust_rte_ovrrd_rt3c cro
            WHERE cro.div_part = l_div_part
              AND cro.load_num = i_load_num
              AND cro.llr_dt = TRUNC(o_llr_ts)
              AND cro.depart_ovrrd_sw = 'Y'
         ORDER BY cro.depart_ts DESC;

      FETCH l_cv
       INTO o_depart_ts;
    END IF;   -- l_cv%NOTFOUND

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_llr_depart_for_load_eta_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_ORDERS_SP
  ||  Move list of unbilled orders for customer to new load/stop/eta.
  ||
  ||  Called by Force Load (Move Orders and Move Unassigned Distributions) UI.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Created from UPDATELOADSTOPFORORDNUM_SP. PIR3593
  || 11/21/07 | rhalpai | Commented out validation for ETA Stop sequence validation.
  ||                    | IM355287
  || 12/05/07 | rhalpai | Added msg to notify of changes to orders for recapped
  ||                    | strict items. PIR5002
  || 02/04/08 | rhalpai | Changed to allow moves to P%%P loads. PIR3593
  || 04/02/08 | rhalpai | Changed to handle Routing Group. PIR5882
  ||                    | Changed logic for cust_on_another_stop to exclude orders
  ||                    | in passed order list.
  || 07/17/08 | rhalpai | Changed to provide waning msgs with listed order numbers
  ||                    | for Strict Orders that have either been recapped or the
  ||                    | move to an earlier LLR date will cause unrecapped order
  ||                    | lines to miss their PO Cutoff resulting in order outs.
  ||                    | PIR5002
  || 09/08/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 03/14/11 | rhalpai | Changed logic to include new override table during
  ||                    | validation. When an Override matching Cust/LLRDate is
  ||                    | found, validate that the New Load/Stop matches the
  ||                    | Override Load/Stop and use the ETA from the Override.
  ||                    | PIR9348
  || 05/24/11 | rhalpai | Changed logic to check for orphaned reg distributions
  ||                    | after move and when found move them to DIST load.
  ||                    | PIR7132
  || 07/10/12 | rhalpai | Change to handle stop assignment for reserved loads
  ||                    | (found with XLOAD parmid on AP1S).
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/28/13 | rhalpai | Change logic to include DepartTs,StopNum,EtaTs from
  ||                    | CUST_RTE_OVRRD_RT3C when corresponding OvrrdSw is ON.
  ||                    | IM-123463
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  || 10/14/17 | rhalpai | Add DivPart in calls to OP_STRICT_ORDER_PK.IS_RECAPPED_FN,
  ||                    | OP_STRICT_ORDER_PK.ORD_WILL_MISS_PO_CUTOFF_FN,
  ||                    | OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP,
  ||                    | OP_PROCESS_CONTROL_PK.RESTRICTED_MSG_FN.
  ||                    | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN.
  ||                    | Add ProcessId for UserId in call to
  ||                    | OP_ORDER_LOAD_PK.SYNCLOAD_SP. PIR15427
  || 11/23/22 | rhalpai | Add validation logic to prevent moving order from restricted load range. 00000021755
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_orders_sp(
    i_div       IN      VARCHAR2,
    i_cust_id   IN      VARCHAR2,
    i_load_num  IN      VARCHAR2,
    i_stop_num  IN      PLS_INTEGER,
    i_eta_dt    IN      VARCHAR2,
    i_eta_tm    IN      NUMBER,
    i_ord_list  IN      CLOB,
    i_user_id   IN      VARCHAR2,
    o_msg       OUT     VARCHAR2,
    i_rsn_cd    IN      VARCHAR2 DEFAULT 'FORCELD'
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm          := 'OP_ORDER_MOVES_PK.MOVE_ORDERS_SP';
    lar_parm                    logs.tar_parm;
    l_t_ord_nums                type_stab;
    l_user_id                   mclp300d.resusd%TYPE;
    l_eta_ts                    DATE;
    l_t_xloads                  type_stab;
    l_xload_sw                  VARCHAR2(1);
    l_div_part                  NUMBER;
    l_is_load_assigned_to_cust  BOOLEAN                := FALSE;
    l_cust_rte_ovrrd_sw         VARCHAR2(1)            := 'N';
    l_llr_ts                    DATE;
    l_depart_ts                 DATE;
    l_c_force_load     CONSTANT VARCHAR2(8)            := 'FORCELD';
    l_c_routing        CONSTANT VARCHAR2(8)            := 'QROUTE';
    l_rsn_cd                    mclp300d.reasnd%TYPE   := NVL(i_rsn_cd, l_c_force_load);
    l_recapped_ords             VARCHAR2(32000);
    l_miss_po_cutoff_ords       VARCHAR2(32000);

    FUNCTION validate_parms_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
    BEGIN
      logs.dbg('Validate Parms');

      IF RTRIM(i_div) IS NULL THEN
        l_msg := 'Division is required';
      ELSIF RTRIM(i_cust_id) IS NULL THEN
        l_msg := 'Customer is required';
      ELSIF RTRIM(i_load_num) IS NULL THEN
        l_msg := 'Load is required';
      ELSIF(    NVL(i_stop_num, 100) NOT BETWEEN 0 AND 99
            AND l_xload_sw = 'N') THEN
        l_msg := 'Invalid stop number';
      ELSIF RTRIM(i_eta_dt) IS NULL THEN
        l_msg := 'ETA date is required';
      ELSIF i_eta_tm IS NULL THEN
        l_msg := 'ETA time is required';
      ELSIF(   i_eta_tm NOT BETWEEN 0 AND 2359
            OR SUBSTR(LPAD(i_eta_tm, 4, '0'), 1, 2) NOT BETWEEN 0 AND 23
            OR SUBSTR(LPAD(i_eta_tm, 4, '0'), 3) NOT BETWEEN 0 AND 59
           ) THEN
        l_msg := 'Invalid ETA time';
      ELSIF(   RTRIM(i_ord_list) IS NULL
            OR l_t_ord_nums.COUNT = 0) THEN
        l_msg := 'Order list is required';
      ELSIF RTRIM(i_user_id) IS NULL THEN
        l_msg := 'UserID is required';
      END IF;

      RETURN(l_msg);
    END validate_parms_fn;

    FUNCTION load_info_fn
      RETURN VARCHAR2 IS
      l_cv        SYS_REFCURSOR;
      l_exist_sw  VARCHAR2(1)   := 'N';
      l_msg       VARCHAR2(100);
    BEGIN
      IF i_load_num NOT BETWEEN 'P00P' AND 'P99P' THEN
        logs.dbg('Get Load Info');

        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM mclp120c c
            WHERE c.div_part = l_div_part
              AND c.loadc = i_load_num;

        FETCH l_cv
         INTO l_exist_sw;

        IF l_exist_sw = 'N' THEN
          l_msg := 'Invalid Load Entered';
        END IF;
      END IF;   -- i_load_num NOT BETWEEN 'P00P' AND 'P99P'

      RETURN(l_msg);
    END load_info_fn;

    FUNCTION load_typ_fn
      RETURN VARCHAR2 IS
      l_cv        SYS_REFCURSOR;
      l_load_typ  VARCHAR2(3);
      l_msg       VARCHAR2(200);
    BEGIN
      IF i_load_num BETWEEN 'P00P' AND 'P99P' THEN
        l_load_typ := SUBSTR(i_load_num, 1, 3);
        logs.dbg('Check Load Type');

        OPEN l_cv
         FOR
           SELECT 'Selected default special distribution load does not match'
                  || ' load type on order.'
                  || cnst.newline_char
                  || '(Must use P00P load for P00 load type)'
             FROM TABLE(CAST(l_t_ord_nums AS type_stab)) t
            WHERE EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = l_div_part
                            AND a.ordnoa = t.column_value
                            AND a.ldtypa <> l_load_typ);

        FETCH l_cv
         INTO l_msg;
      END IF;   -- i_load_num BETWEEN 'P00P' AND 'P99P'

      RETURN(l_msg);
    END load_typ_fn;

    FUNCTION cust_rte_ovrrd_fn
      RETURN VARCHAR2 IS
      l_msg         VARCHAR2(100);
      l_llr_dt      DATE;
      l_cv          SYS_REFCURSOR;
      l_ovrrd_stop  PLS_INTEGER;
    BEGIN
      l_llr_dt := TRUNC(l_llr_ts);
      logs.dbg('Get Override ETA');

      OPEN l_cv
       FOR
         SELECT r.eta_ts
           FROM cust_rte_ovrrd_rt3c r
          WHERE r.div_part = l_div_part
            AND r.cust_id = i_cust_id
            AND r.llr_dt = l_llr_dt
            AND r.load_num = i_load_num
            AND r.eta_ovrrd_sw = 'Y';

      FETCH l_cv
       INTO l_eta_ts;

      logs.dbg('Get Override DepartTs');

      OPEN l_cv
       FOR
         SELECT r.depart_ts
           FROM cust_rte_ovrrd_rt3c r
          WHERE r.div_part = l_div_part
            AND r.cust_id = i_cust_id
            AND r.llr_dt = l_llr_dt
            AND r.load_num = i_load_num
            AND r.depart_ovrrd_sw = 'Y';

      FETCH l_cv
       INTO l_depart_ts;

      logs.dbg('Get Override Stop for Compare');

      OPEN l_cv
       FOR
         SELECT r.stop_num
           FROM cust_rte_ovrrd_rt3c r
          WHERE r.div_part = l_div_part
            AND r.cust_id = i_cust_id
            AND r.llr_dt = l_llr_dt
            AND r.load_num = i_load_num
            AND r.stop_ovrrd_sw = 'Y';

      FETCH l_cv
       INTO l_ovrrd_stop;

      IF l_cv%FOUND THEN
        l_cust_rte_ovrrd_sw := 'Y';
        l_is_load_assigned_to_cust := FALSE;

        IF i_stop_num <> l_ovrrd_stop THEN
          l_msg := 'Load/Stop ('
                   || i_load_num
                   || '/'
                   || i_stop_num
                   || ') does not match Cust Route Override ('
                   || i_load_num
                   || '/'
                   || l_ovrrd_stop
                   || ') for Calculated LLRDate '
                   || TO_CHAR(l_llr_ts, 'YYYY-MM-DD');
        END IF;   -- i_stop_num <> l_ovrrd_stop
      END IF;   -- l_cv%FOUND

      RETURN(l_msg);
    END cust_rte_ovrrd_fn;

    FUNCTION stop_assigned_to_another_fn
      RETURN VARCHAR2 IS
      l_msg     VARCHAR2(100);
      l_llr_dt  DATE;
      l_cv      SYS_REFCURSOR;
    BEGIN
      l_llr_dt := TRUNC(l_llr_ts);
      logs.dbg('Check Stop Assigned to Another Customer');

      OPEN l_cv
       FOR
         SELECT 'Load and stop assigned to another customer. Order not moved.'
           FROM DUAL
          WHERE EXISTS(SELECT 1
                         FROM mclp040d md
                        WHERE md.div_part = l_div_part
                          AND md.loadd = i_load_num
                          AND md.stopd = i_stop_num
                          AND md.custd <> i_cust_id
                          AND l_cust_rte_ovrrd_sw = 'N')
             OR EXISTS(SELECT 1
                         FROM cust_rte_ovrrd_rt3c cro
                        WHERE cro.div_part = l_div_part
                          AND cro.llr_dt = l_llr_dt
                          AND cro.load_num = i_load_num
                          AND cro.stop_num = i_stop_num
                          AND cro.cust_id <> i_cust_id);

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END stop_assigned_to_another_fn;

    FUNCTION stop_in_use_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
      l_cv   SYS_REFCURSOR;
    BEGIN
      IF i_load_num = 'ROUT' THEN
        logs.dbg('Check for load/stop already in use for ship date');

        OPEN l_cv
         FOR
           SELECT 'Load/Stop/Ship already used by another customer. Order not moved.'
             FROM DUAL
            WHERE EXISTS(SELECT 1
                           FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                          WHERE ld.div_part = l_div_part
                            AND ld.load_num = i_load_num
                            AND se.div_part = ld.div_part
                            AND se.load_depart_sid = ld.load_depart_sid
                            AND se.stop_num = i_stop_num
                            AND se.cust_id <> i_cust_id
                            AND a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.custa = se.cust_id
                            AND a.dsorda = 'D'
                            AND a.ldtypa LIKE 'P__'
                            AND a.shpja =(TRUNC(l_eta_ts) - DATE '1900-02-28' - 1)
                            AND a.stata NOT IN('A', 'C'));
      ELSIF l_xload_sw = 'N' THEN
        logs.dbg('Check Load/Stop/LLR already in use for current week');

        OPEN l_cv
         FOR
           SELECT 'Load/Stop/LLR already used by another customer. Order not moved.'
             FROM DUAL
            WHERE EXISTS(SELECT 1
                           FROM load_depart_op1f ld, stop_eta_op1g se
                          WHERE ld.div_part = l_div_part
                            AND ld.load_num = i_load_num
                            AND se.div_part = ld.div_part
                            AND se.load_depart_sid = ld.load_depart_sid
                            AND se.stop_num = i_stop_num
                            AND TRUNC(se.eta_ts) BETWEEN TRUNC(l_eta_ts) - 6 AND TRUNC(l_eta_ts) + 6
                            AND se.cust_id <> i_cust_id
                            AND EXISTS(SELECT 1
                                         FROM ordp100a a
                                        WHERE a.div_part = se.div_part
                                          AND a.load_depart_sid = se.load_depart_sid
                                          AND a.custa = se.cust_id
                                          AND a.stata NOT IN('A', 'C')));
      END IF;   -- i_load_num = 'ROUT'

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END stop_in_use_fn;

    FUNCTION stop_info_fn
      RETURN VARCHAR2 IS
      l_msg       VARCHAR2(100);
      l_cv        SYS_REFCURSOR;
      l_stop_num  NUMBER;
      l_eta_tm    NUMBER;
      l_eta_day   VARCHAR2(3);
      l_eta_wk    PLS_INTEGER   := 0;
    BEGIN
      -------------------------------------------------------------------------
      -- Determine stop info
      --   If the load was passed without a valid stop then the load must be
      --     assigned to the customer.
      --   If the load was passed with a valid stop and the load is assigned
      --     to the customer then the stop must match the stop assignment.
      --   If the load is not assigned to the customer then make sure the stop
      --     has not already been assigned.
      -------------------------------------------------------------------------
      IF l_xload_sw = 'Y' THEN
        l_llr_ts := DATE '1900-01-01';
        l_depart_ts := DATE '1900-01-01';
      ELSE
        logs.dbg('Customer Stop Info');

        -- override eta time passed if customer assigned to load
        OPEN l_cv
         FOR
           SELECT stopd, etad, dayrcd, NVL(wkoffd, 0)
             FROM mclp040d
            WHERE div_part = l_div_part
              AND custd = i_cust_id
              AND loadd = i_load_num;

        FETCH l_cv
         INTO l_stop_num, l_eta_tm, l_eta_day, l_eta_wk;

        IF l_cv%FOUND THEN
          -- Load Is Assigned to Customer
          l_is_load_assigned_to_cust := TRUE;
        ELSE
          -- Load Not Assigned to Customer
          --
          -- Use eta week offset of load order is moving from when order is from an assigned load
          -- No eta week offset will be used for orders from unassigned loads
          logs.dbg('Get ETA Week Offset');

          SELECT NVL(MAX(md.wkoffd), 0)
            INTO l_eta_wk
            FROM mclp040d md
           WHERE md.div_part = l_div_part
             AND md.custd = i_cust_id
             AND md.loadd = (SELECT ld.load_num
                               FROM ordp100a a, load_depart_op1f ld
                              WHERE a.div_part = l_div_part
                                AND a.ordnoa = l_t_ord_nums(l_t_ord_nums.FIRST)
                                AND ld.div_part = a.div_part
                                AND ld.load_depart_sid = a.load_depart_sid);
        END IF;   --  l_cv%found

        logs.dbg('Assign LLR and Departure for Load/ETA');
        -- (ETA must be passed after subtracting offset)
        get_llr_depart_for_load_eta_sp(i_div, i_load_num,(l_eta_ts -(l_eta_wk * 7)), l_llr_ts, l_depart_ts);
        l_msg := COALESCE(cust_rte_ovrrd_fn, stop_assigned_to_another_fn, stop_in_use_fn);

        IF l_msg IS NULL THEN
          IF (    l_msg IS NULL
              AND l_is_load_assigned_to_cust
              AND l_cust_rte_ovrrd_sw = 'N') THEN
            -- Determine if we have a valid stop number passed
            -- that matches the stop for customer's load
            IF l_stop_num <> i_stop_num THEN
              l_msg := 'Must use customer assigned stop: ' || l_stop_num;
            ELSE
              logs.dbg('Validate ETA date input');

              IF TO_CHAR(l_eta_ts, 'DY') <> l_eta_day THEN
                l_msg := 'ETA date is not a ' || l_eta_day || ' as defined by load/stop';
              END IF;   -- TO_CHAR(l_eta_ts, 'DY') <> l_eta_day
            END IF;   -- l_stop_num <> i_stop_num
          END IF;   -- l_msg IS NULL AND l_is_load_assigned_to_cust AND l_cust_rte_ovrrd_sw = 'N'
        END IF;   -- l_msg_IS NULL
      END IF;   -- l_xload_sw = 'Y'

      RETURN(l_msg);
    END stop_info_fn;

    FUNCTION rstr_load_range_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
      l_cv   SYS_REFCURSOR;
    BEGIN
      logs.dbg('Check for restricted load range');

      -- Check for new load not allowed for order source with restricted load range
      OPEN l_cv
       FOR
         SELECT 'New load not allowed for order with restricted load range. Order not moved.'
           FROM DUAL
          WHERE EXISTS(SELECT 1
                         FROM (SELECT SUBSTR(x.parm_id, INSTR(x.parm_id, '_', -1) +1) AS ord_src,
                                      SUBSTR(x.val, 1, 4) AS min_load,
                                      SUBSTR(x.val, -4) AS max_load
                                 FROM (SELECT DISTINCT
                                              FIRST_VALUE(p.parm_id) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS parm_id,
                                              FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS val
                                         FROM appl_sys_parm_ap1s p
                                        WHERE p.parm_id LIKE 'LOAD_RANGE%'
                                          AND p.div_part IN(0, l_div_part)
                                      ) x) prm,
                              ordp100a a
                        WHERE a.div_part = l_div_part
                          AND a.ordnoa IN(SELECT TO_NUMBER(t.column_value)
                                            FROM TABLE(CAST(l_t_ord_nums AS type_stab)) t)
                          AND a.stata NOT IN('A', 'C')
                          AND a.ipdtsa = prm.ord_src
                          AND i_load_num NOT BETWEEN prm.min_load AND prm.max_load
                      );

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END rstr_load_range_fn;

    FUNCTION formatted_date_calcs_fn(
      i_err_msg    IN  VARCHAR2,
      i_llr_ts     IN  DATE,
      i_depart_ts  IN  DATE,
      i_eta_ts     IN  DATE
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN i_err_msg
             || '|'
             || TO_CHAR(i_llr_ts, 'DY||YYYY-MM-DD||HH24:MI')
             || '&'
             || TO_CHAR(i_depart_ts, 'DY||YYYY-MM-DD||HH24:MI')
             || '&'
             || TO_CHAR(i_eta_ts, 'DY||YYYY-MM-DD||HH24:MI')
             || '|';
    END formatted_date_calcs_fn;

    FUNCTION validate_eta_fn
      RETURN VARCHAR2 IS
      l_c_sysdate  CONSTANT DATE         := SYSDATE;
      l_err_msg             typ.t_maxvc2;

      FUNCTION llr_less_today_fn
        RETURN VARCHAR2 IS
        l_msg  VARCHAR2(200);
      BEGIN
        IF TRUNC(l_llr_ts) < TRUNC(l_c_sysdate) THEN
          l_msg := formatted_date_calcs_fn('The calculated LLR Date is less than Current Date',
                                           l_llr_ts,
                                           l_depart_ts,
                                           l_eta_ts
                                          );
        END IF;   -- TRUNC(l_llr_ts) <= TRUNC(l_c_sysdate)

        RETURN(l_msg);
      END llr_less_today_fn;

      FUNCTION llr_today_after_time_fn
        RETURN VARCHAR2 IS
        l_msg  VARCHAR2(200);
      BEGIN
        -- << l_rsn_cd NOT IN(l+c_force_load, l_c_routing) AND LLR for today but after cutoff time >>
        IF (    l_rsn_cd NOT IN(l_c_force_load, l_c_routing)
            AND TRUNC(l_llr_ts) = TRUNC(l_c_sysdate)
            AND l_llr_ts < l_c_sysdate
           ) THEN
          l_msg := formatted_date_calcs_fn('The calculated LLR Date is today but after LLR-CUT-OFF Time',
                                           l_llr_ts,
                                           l_depart_ts,
                                           l_eta_ts
                                          );
        END IF;   -- << l_rsn_cd NOT IN(l_c_force_load, l_c_routing) AND LLR for today but after cutoff time >>

        RETURN(l_msg);
      END llr_today_after_time_fn;

      FUNCTION eta_time_gt_llr_fn
        RETURN VARCHAR2 IS
        l_msg  VARCHAR2(200);
      BEGIN
        -- << ETA date matches LLR date but ETA time not greater than LLR time >>
        IF (    TRUNC(l_eta_ts) = TRUNC(l_llr_ts)
            AND l_eta_ts <= l_llr_ts) THEN
          l_msg := formatted_date_calcs_fn('Invalid ETA Time. ETA Time must be > LLR Time if ETA Date = LLR Date.',
                                           l_llr_ts,
                                           l_depart_ts,
                                           l_eta_ts
                                          );
        END IF;   -- << ETA date matches LLR date but ETA time not greater than LLR time >>

        RETURN(l_msg);
      END eta_time_gt_llr_fn;

      FUNCTION eta_time_gt_depart_fn
        RETURN VARCHAR2 IS
        l_msg  VARCHAR2(200);
      BEGIN
        -- << ETA date matches Depart date but ETA time not greater than Depart time >>
        IF (    TRUNC(l_eta_ts) = TRUNC(l_depart_ts)
            AND l_eta_ts <= l_depart_ts) THEN
          l_msg :=
            formatted_date_calcs_fn('Invalid ETA Time. ETA Time must be > Depart Time if ETA Date = Depart Date.',
                                    l_llr_ts,
                                    l_depart_ts,
                                    l_eta_ts
                                   );
        END IF;   -- << ETA date matches Depart date but ETA time not greater than Depart time >>

        RETURN(l_msg);
      END eta_time_gt_depart_fn;
    BEGIN
      IF l_xload_sw = 'N' THEN
        logs.dbg('Validate Date Calculations for ETA');
        l_err_msg := COALESCE(llr_less_today_fn, llr_today_after_time_fn, eta_time_gt_llr_fn, eta_time_gt_depart_fn);
      END IF;   -- l_xload_sw = 'N'

      RETURN(l_err_msg);
    END validate_eta_fn;

    FUNCTION cust_on_another_stop_fn
      RETURN VARCHAR2 IS
      l_cv        SYS_REFCURSOR;
      l_msg       VARCHAR2(200);
      l_stop_num  NUMBER;
    BEGIN
      IF (    NOT l_is_load_assigned_to_cust
          AND (   l_xload_sw = 'N'
               OR i_load_num = 'ROUT')) THEN
        -----------------------------------------------------------------
        -- If customer already has an order for the passed Load/LLR the
        -- stop passed must match the stop already assigned to the order
        -----------------------------------------------------------------
        OPEN l_cv
         FOR
           SELECT   se.stop_num
               FROM load_depart_op1f ld, stop_eta_op1g se
              WHERE ld.div_part = l_div_part
                AND ld.llr_ts = l_llr_ts
                AND ld.load_num = i_load_num
                AND se.div_part = ld.div_part
                AND se.load_depart_sid = ld.load_depart_sid
                AND se.cust_id = i_cust_id
                AND se.stop_num <> i_stop_num
                AND EXISTS(SELECT 1
                             FROM ordp100a a
                            WHERE a.div_part = se.div_part
                              AND a.load_depart_sid = se.load_depart_sid
                              AND a.custa = se.cust_id
                              AND a.ordnoa NOT IN(SELECT TO_NUMBER(t.column_value)
                                                    FROM TABLE(CAST(l_t_ord_nums AS type_stab)) t)
                              AND a.stata NOT IN('A', 'C'))
           ORDER BY 1;

        FETCH l_cv
         INTO l_stop_num;

        IF l_cv%FOUND THEN
          l_msg := 'Stop '
                   || l_stop_num
                   || ' already has orders for customer on load for LLR '
                   || TO_CHAR(l_llr_ts, 'YYYY-MM-DD')
                   || '. Please use that stop.';
        END IF;
      END IF;   -- NOT l_load_assigned_to_customer

      RETURN(l_msg);
    END cust_on_another_stop_fn;

    FUNCTION eta_stop_seq_fn
      RETURN VARCHAR2 IS
      l_c_eta_ts_str  CONSTANT VARCHAR2(8)   := TO_CHAR(l_eta_ts, 'YYYYMMDD HH24MI');
      l_cv                     SYS_REFCURSOR;
      l_msg                    VARCHAR2(200);
    BEGIN
      IF l_xload_sw = 'N' THEN
        logs.dbg('Validate ETA/Stop Seq');

        OPEN l_cv
         FOR
           SELECT 'Stop [' || i_stop_num || '] out of sequence for ETA [' || l_c_eta_ts_str || ']. Order not moved.'
             FROM DUAL
            WHERE EXISTS(SELECT 1
                           FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                          WHERE ld.div_part = l_div_part
                            AND ld.llr_ts = l_llr_ts
                            AND ld.load_num = i_load_num
                            AND se.div_part = ld.div_part
                            AND se.load_depart_sid = ld.load_depart_sid
                            AND (   (    se.stop_num > i_stop_num
                                     AND se.eta_ts > l_eta_ts)
                                 OR (    se.stop_num < i_stop_num
                                     AND se.eta_ts < l_eta_ts)
                                )
                            AND se.cust_id <> i_cust_id
                            AND a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.stata NOT IN('A', 'C'));

        FETCH l_cv
         INTO l_msg;
      END IF;   -- l_xload_sw = 'N'

      RETURN(l_msg);
    END eta_stop_seq_fn;

    PROCEDURE reassign_lone_dist_sp(
      i_old_llr_ts  IN  DATE,
      i_old_load    IN  VARCHAR2
    ) IS
      l_t_lone_dist_ord_nums  type_ntab;
    BEGIN
      IF i_old_load NOT MEMBER OF l_t_xloads THEN
        SELECT a.ordnoa
        BULK COLLECT INTO l_t_lone_dist_ord_nums
          FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
         WHERE ld.div_part = l_div_part
           AND ld.llr_ts = i_old_llr_ts
           AND ld.load_num = i_old_load
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND se.cust_id = i_cust_id
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.custa = i_cust_id
           AND a.stata = 'O'
           AND a.dsorda = 'D'
           AND a.ldtypa NOT BETWEEN 'P00' AND 'P99'
           AND NOT EXISTS(SELECT 1
                            FROM ordp100a a2
                           WHERE a2.div_part = a.div_part
                             AND a2.custa = i_cust_id
                             AND a2.load_depart_sid = a.load_depart_sid
                             AND a2.excptn_sw = 'N'
                             AND a2.stata = 'O'
                             AND a2.dsorda = 'R');

        IF l_t_lone_dist_ord_nums.COUNT > 0 THEN
          op_order_load_pk.syncload_sp(l_div_part, l_rsn_cd, l_t_lone_dist_ord_nums, op_const_pk.prcs_ord_mov);
        END IF;   -- l_t_lone_dist_ord_nums.COUNT > 0
      END IF;   -- i_old_load NOT MEMBER OF l_t_xloads
    END reassign_lone_dist_sp;

    FUNCTION move_orders_fn
      RETURN VARCHAR2 IS
      TYPE l_rt_load_ords IS RECORD(
        llr_ts      DATE,
        load_num    mclp120c.loadc%TYPE,
        depart_ts   DATE,
        stop_num    NUMBER,
        eta_ts      DATE,
        t_ord_nums  type_ntab
      );

      TYPE tt_load_ords IS TABLE OF l_rt_load_ords;

      l_t_load_ords                tt_load_ords;
      l_load_depart_sid            NUMBER;
      l_ord_num                    NUMBER;
      l_t_mov_ord_nums             type_ntab       := type_ntab();
      l_t_mssng_ords               type_ntab;
      l_c_prb             CONSTANT VARCHAR2(3)     := 'PRB';
      l_c_recapped        CONSTANT VARCHAR2(3)     := 'RCP';
      l_c_miss_po_cutoff  CONSTANT VARCHAR2(3)     := 'MPC';
      l_prb_ords                   VARCHAR2(32000);
      l_not_moved_cnt              PLS_INTEGER     := 0;
      l_msg                        typ.t_maxvc2;

      PROCEDURE add_prob_ord_sp(
        i_ord_num  IN  PLS_INTEGER,
        i_prb_cd   IN  VARCHAR2
      ) IS
      BEGIN
        CASE i_prb_cd
          WHEN l_c_recapped THEN
            l_recapped_ords := l_recapped_ords ||(CASE
                                                    WHEN l_recapped_ords IS NOT NULL THEN ','
                                                  END) || i_ord_num;
          WHEN l_c_miss_po_cutoff THEN
            l_miss_po_cutoff_ords := l_miss_po_cutoff_ords
                                     ||(CASE
                                          WHEN l_miss_po_cutoff_ords IS NOT NULL THEN ','
                                        END)
                                     || i_ord_num;
          WHEN l_c_prb THEN
            l_not_moved_cnt := l_not_moved_cnt + 1;
            l_prb_ords := l_prb_ords ||(CASE
                                          WHEN l_prb_ords IS NOT NULL THEN ','
                                        END) || i_ord_num;
        END CASE;
      EXCEPTION
        WHEN VALUE_ERROR THEN
          NULL;
      END add_prob_ord_sp;
    BEGIN
      logs.dbg('Get Load Order Info');

      -- Make sure status has not changed since selection
      -- and that not attempting to move from or to a stop locked for Routing.
      SELECT   ld.llr_ts,
               ld.load_num,
               ld.depart_ts,
               se.stop_num,
               se.eta_ts,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id
                                AND a.ordnoa IN(SELECT TO_NUMBER(t.column_value)
                                                  FROM TABLE(CAST(l_t_ord_nums AS type_stab)) t)
                                AND (   l_rsn_cd = 'QROUTE'
                                     OR a.dsorda IN('R', 'T', 'N')
                                     OR (    i_load_num BETWEEN 'P00P' AND 'P99P'
                                         AND i_load_num <> a.ldtypa || 'P')
                                     OR (    a.dsorda = 'D'
                                         AND NOT EXISTS(SELECT 1
                                                          FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                                         WHERE g.div_part = a.div_part
                                                           AND r.rte_grp_num = g.rte_grp_num
                                                           AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                                           AND r.stop_num =
                                                                 (CASE
                                                                    WHEN ld.load_num = 'ROUT' THEN se.stop_num
                                                                    WHEN i_load_num = 'ROUT' THEN i_stop_num
                                                                  END
                                                                 )
                                                           AND r.mcl_cust = cx.mccusb
                                                           AND r.stat_cd IN('SNT', 'WRK')
                                                           AND o.rte_grp_num = g.rte_grp_num
                                                           AND o.ord_num = a.ordnoa)
                                        )
                                    )
                                AND a.stata = 'O'
                            ) AS type_ntab
                   ) AS ord_nums
      BULK COLLECT INTO l_t_load_ords
          FROM load_depart_op1f ld, stop_eta_op1g se, mclp020b cx
         WHERE ld.div_part = l_div_part
           AND se.div_part = ld.div_part
           AND se.cust_id = i_cust_id
           AND se.load_depart_sid = ld.load_depart_sid
           AND cx.div_part = se.div_part
           AND cx.custb = se.cust_id
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.ordnoa IN(SELECT TO_NUMBER(t.column_value)
                                           FROM TABLE(CAST(l_t_ord_nums AS type_stab)) t)
                         AND a.div_part = ld.div_part
                         AND a.custa = i_cust_id
                         AND a.load_depart_sid = ld.load_depart_sid
                         AND (   l_rsn_cd = 'QROUTE'
                              OR a.dsorda IN('R', 'T', 'N')
                              OR (    i_load_num BETWEEN 'P00P' AND 'P99P'
                                  AND i_load_num <> a.ldtypa || 'P')
                              OR (    a.dsorda = 'D'
                                  AND NOT EXISTS(SELECT 1
                                                   FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                                  WHERE g.div_part = a.div_part
                                                    AND r.rte_grp_num = g.rte_grp_num
                                                    AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                                    AND r.stop_num =
                                                          (CASE
                                                             WHEN ld.load_num = 'ROUT' THEN se.stop_num
                                                             WHEN i_load_num = 'ROUT' THEN i_stop_num
                                                           END
                                                          )
                                                    AND r.mcl_cust = cx.mccusb
                                                    AND r.stat_cd IN('SNT', 'WRK')
                                                    AND o.rte_grp_num = g.rte_grp_num
                                                    AND o.ord_num = a.ordnoa)
                                 )
                             )
                         AND a.stata = 'O')
      ORDER BY se.load_depart_sid, se.cust_id;

      IF l_t_load_ords.COUNT > 0 THEN
        logs.dbg('Get LoadDepartSid');
        l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(l_div_part, l_llr_ts, i_load_num);
        FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
          logs.dbg('Move Orders');
          op_order_load_pk.move_ords_sp(l_div_part,
                                        i_cust_id,
                                        l_load_depart_sid,
                                        i_stop_num,
                                        l_eta_ts,
                                        l_t_load_ords(i).llr_ts,
                                        l_t_load_ords(i).load_num,
                                        l_t_load_ords(i).depart_ts,
                                        l_t_load_ords(i).stop_num,
                                        l_t_load_ords(i).eta_ts,
                                        l_rsn_cd,
                                        l_user_id,
                                        l_t_load_ords(i).t_ord_nums
                                       );

          IF l_t_load_ords(i).llr_ts > DATE '1900-01-01' THEN
            reassign_lone_dist_sp(l_t_load_ords(i).llr_ts, l_t_load_ords(i).load_num);
          END IF;   -- l_t_load_ords(i).llr_ts > DATE '1900-01-01'

          FOR j IN l_t_load_ords(i).t_ord_nums.FIRST .. l_t_load_ords(i).t_ord_nums.LAST LOOP
            BEGIN
              l_ord_num := l_t_load_ords(i).t_ord_nums(j);
              l_t_mov_ord_nums.EXTEND;
              l_t_mov_ord_nums(l_t_mov_ord_nums.LAST) := l_ord_num;
              logs.dbg('Check Recapped Strict Order');

              IF op_strict_order_pk.is_recapped_fn(l_div_part, l_ord_num) THEN
                add_prob_ord_sp(l_ord_num, l_c_recapped);
              END IF;   -- op_strict_order_pk.is_recapped_fn(l_div_part, l_ord_num)

              logs.dbg('Check Move will make Order Lines Miss PO Cutoff');

              IF op_strict_order_pk.ord_will_miss_po_cutoff_fn(l_div_part, l_ord_num, l_llr_ts) THEN
                add_prob_ord_sp(l_ord_num, l_c_miss_po_cutoff);
              END IF;   -- op_strict_order_pk.ord_will_miss_po_cutoff_fn(l_ord_num, l_llr_ts)
            EXCEPTION
              WHEN OTHERS THEN
                logs.warn('Unhandled Error for OrdNum: ' || l_ord_num || cnst.newline_char || SQLERRM, lar_parm);
                add_prob_ord_sp(l_ord_num, l_c_prb);
            END;
          END LOOP;
        END LOOP;
        logs.dbg('Orders Not Available for Move');

        SELECT x.ord_num
        BULK COLLECT INTO l_t_mssng_ords
          FROM (SELECT TO_NUMBER(t.column_value) AS ord_num
                  FROM TABLE(CAST(l_t_ord_nums AS type_stab)) t
                MINUS
                SELECT t.column_value AS ord_num
                  FROM TABLE(CAST(l_t_mov_ord_nums AS type_ntab)) t) x;

        IF l_t_mssng_ords.COUNT > 0 THEN
          FOR i IN l_t_mssng_ords.FIRST .. l_t_mssng_ords.LAST LOOP
            add_prob_ord_sp(l_t_mssng_ords(i), l_c_prb);
          END LOOP;
        END IF;   -- l_t_mssng_ords.COUNT > 0
      END IF;   -- l_t_load_ords.COUNT > 0

      IF l_not_moved_cnt = l_t_ord_nums.COUNT THEN
        l_msg := 'No orders moved.' || cnst.newline_char || 'Orders were not found and/or status changed.';
      ELSIF l_not_moved_cnt > 0 THEN
        l_msg := 'Some orders moved.'
                 || cnst.newline_char
                 || 'However, the following order were not moved due to not found and/or status change:'
                 || cnst.newline_char
                 || l_prb_ords;
      END IF;   -- l_not_moved_cnt = l_t_ord_nums.COUNT

      COMMIT;
      RETURN(l_msg);
    END move_orders_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'EtaDt', i_eta_dt);
    logs.add_parm(lar_parm, 'EtaTm', i_eta_tm);
    logs.add_parm(lar_parm, 'OrdList', i_ord_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_ord_nums := strsplit_fn(i_ord_list, op_const_pk.grp_delimiter);
    l_user_id := SUBSTR(i_user_id, 1, 20);
    l_eta_ts := TO_DATE(i_eta_dt || lpad_fn(i_eta_tm, 4, '0'), 'YYYY-MM-DDHH24MI');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    l_xload_sw :=(CASE
                    WHEN i_load_num MEMBER OF l_t_xloads THEN 'Y'
                    ELSE 'N'
                  END);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Validate, Get Info, and Process');
    o_msg := COALESCE(validate_parms_fn,
                      load_info_fn,
                      load_typ_fn,
                      stop_info_fn,
                      rstr_load_range_fn,
                      validate_eta_fn,
                      cust_on_another_stop_fn,
--                      eta_stop_seq_fn,
                      move_orders_fn
                     );

    IF o_msg IS NULL THEN
      o_msg := op_const_pk.msg_typ_info
               || op_const_pk.field_delimiter
               || 'Selected Orders, Stops are Moved to Specified Load';
    ELSE
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || o_msg;
    END IF;   -- o_msg IS NULL

    IF l_recapped_ords IS NOT NULL THEN
      o_msg := o_msg
               || cnst.newline_char
               || cnst.newline_char
               || op_strict_order_pk.g_c_recapped_msg
               || cnst.newline_char
               || l_recapped_ords;
    END IF;   -- l_recapped_ords IS NOT NULL

    IF l_miss_po_cutoff_ords IS NOT NULL THEN
      o_msg := o_msg
               || cnst.newline_char
               || cnst.newline_char
               || op_strict_order_pk.g_c_miss_po_cutoff_msg
               || cnst.newline_char
               || l_miss_po_cutoff_ords;
    END IF;   -- l_miss_po_cutoff_ords IS NOT NULL

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_msg := op_const_pk.msg_typ_err
               || op_const_pk.field_delimiter
               || op_process_control_pk.restricted_msg_fn(op_const_pk.prcs_ord_mov, l_div_part);
      logs.warn(SQLERRM, lar_parm);
    WHEN OTHERS THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'Selected Orders NOT Moved -- Error Occurred';
      logs.err(lar_parm, NULL, FALSE);
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
  END move_orders_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_ORDERS_BY_STOP_SP
  ||  Move all unbilled orders for llr/load/stop/cust to new load/stop/eta.
  ||
  ||  Called by Load Balance (Move Stop) UI.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Created from OP_BALANCE_LOAD_PK.OP_UPDATE_MOVE_STOP_SP.
  ||                    | PIR3593
  || 04/02/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 09/08/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_orders_by_stop_sp(
    i_div         IN      VARCHAR2,
    i_llr_dt      IN      VARCHAR2,
    i_cust_id     IN      VARCHAR2,
    i_load_num    IN      VARCHAR2,
    i_stop_num    IN      PLS_INTEGER,
    i_new_load    IN      VARCHAR2,
    i_new_stop    IN      NUMBER,
    i_new_eta_dt  IN      VARCHAR2,
    i_new_eta_tm  IN      NUMBER,
    i_user_id     IN      VARCHAR2,
    o_msg         OUT     VARCHAR2,
    i_rsn_cd      IN      VARCHAR2 DEFAULT 'LOADBAL'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_ORDER_MOVES_PK.MOVE_ORDERS_BY_STOP_SP';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_rsn_cd             mclp300d.reasnd%TYPE;
    l_cv                 SYS_REFCURSOR;
    l_ord_list           typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'NewLoad', i_new_load);
    logs.add_parm(lar_parm, 'NewStop', i_new_stop);
    logs.add_parm(lar_parm, 'NewEtaDt', i_new_eta_dt);
    logs.add_parm(lar_parm, 'NewEtaTm', i_new_eta_tm);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_rsn_cd := NVL(RTRIM(i_rsn_cd), 'LOADBAL');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT a.ordnoa
         FROM div_mstr_di1d d, mclp020b cx, load_depart_op1f ld, stop_eta_op1g se, ordp100a a
        WHERE d.div_id = i_div
          AND cx.div_part = d.div_part
          AND cx.custb = i_cust_id
          AND ld.div_part = d.div_part
          AND ld.llr_dt = l_llr_dt
          AND ld.load_num = i_load_num
          AND se.div_part = ld.div_part
          AND se.load_depart_sid = ld.load_depart_sid
          AND se.cust_id = i_cust_id
          AND se.stop_num = i_stop_num
          AND a.div_part = ld.div_part
          AND a.load_depart_sid = ld.load_depart_sid
          AND a.custa = i_cust_id
          AND (   l_rsn_cd = 'QROUTE'
               OR a.dsorda IN('R', 'T', 'N')
               OR (    a.dsorda = 'D'
                   AND NOT EXISTS(SELECT 1
                                    FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                   WHERE g.div_part = a.div_part
                                     AND r.rte_grp_num = g.rte_grp_num
                                     AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                     AND r.stop_num =(CASE
                                                        WHEN i_load_num = 'ROUT' THEN i_stop_num
                                                        WHEN i_new_load = 'ROUT' THEN i_new_stop
                                                      END
                                                     )
                                     AND r.mcl_cust = cx.mccusb
                                     AND r.stat_cd IN('SNT', 'WRK')
                                     AND o.rte_grp_num = g.rte_grp_num
                                     AND o.ord_num = a.ordnoa)
                  )
              )
          AND a.stata = 'O';

    logs.dbg('Create Order List');
    l_ord_list := to_list_fn(l_cv, op_const_pk.grp_delimiter);

    IF l_ord_list IS NULL THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'No Orders Found to Move';
    ELSE
      logs.dbg('Move Orders');
      move_orders_sp(i_div,
                     i_cust_id,
                     i_new_load,
                     i_new_stop,
                     i_new_eta_dt,
                     i_new_eta_tm,
                     l_ord_list,
                     i_user_id,
                     o_msg,
                     l_rsn_cd
                    );
    END IF;   -- l_ord_list IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'Selected Orders NOT Moved -- Error Occurred';
      logs.err(lar_parm, NULL, FALSE);
  END move_orders_by_stop_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_STOPS_SP
  ||  Move all unbilled orders for stop/cust list on llr/load to new load.
  ||  Existing stop numbers and ETA will be preserved unless customer has an
  ||  assigned stop on new load.
  ||
  ||  Called by Force Load (Move Stops) UI.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/06 | rhalpai | Created from OP_UPDATE_MOVE_LOAD_SP. PIR3593
  || 12/05/07 | rhalpai | Added msg to notify of changes to orders for recapped
  ||                    | strict items. PIR5002
  || 04/02/08 | rhalpai | Changed to handle Routing Group. PIR5882
  || 07/17/08 | rhalpai | Changed to provide waning msgs with listed order numbers
  ||                    | for Strict Orders that have either been recapped or the
  ||                    | move to an earlier LLR date will cause unrecapped order
  ||                    | lines to miss their PO Cutoff resulting in order outs.
  ||                    | PIR5002
  || 09/08/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 03/14/11 | rhalpai | Changed logic to include new override table during
  ||                    | validation. When an Override matching Cust/LLRDate is
  ||                    | found, validate that the New Load matches the
  ||                    | Override Load and use the Stop and ETA from the
  ||                    | Override. PIR9348
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/28/13 | rhalpai | Change logic to include DepartTs,StopNum,EtaTs from
  ||                    | CUST_RTE_OVRRD_RT3C when corresponding OvrrdSw is ON.
  ||                    | IM-123463
  || 10/14/17 | rhalpai | Add DivPart in calls to OP_STRICT_ORDER_PK.IS_RECAPPED_FN,
  ||                    | OP_STRICT_ORDER_PK.ORD_WILL_MISS_PO_CUTOFF_FN,
  ||                    | OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP,
  ||                    | OP_PROCESS_CONTROL_PK.RESTRICTED_MSG_FN. PIR15427
  || 11/23/22 | rhalpai | Add validation logic to prevent moving order from restricted load range. 00000021755
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_stops_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    i_from_load  IN      VARCHAR2,
    i_to_load    IN      VARCHAR2,
    i_parm_list  IN      CLOB,
    i_user_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm          := 'OP_ORDER_MOVES_PK.MOVE_STOPS_SP';
    lar_parm               logs.tar_parm;
    l_cv                   SYS_REFCURSOR;
    l_t_parms              lob_rows_t;
    l_user_id              mclp300d.resusd%TYPE;
    l_llr_dt               DATE;
    l_div_part             NUMBER;
    l_from_stop            NUMBER;
    l_to_stop              NUMBER;
    l_cust_id              sysp200c.acnoc%TYPE;
    l_llr_ts               DATE;
    l_depart_ts            DATE;
    l_eta_ts               DATE;
    l_cust_rte_ovrrd_sw    VARCHAR2(1)            := 'N';
    l_move_msg             typ.t_maxvc2;
    l_recapped_ords        VARCHAR2(32000);
    l_miss_po_cutoff_ords  VARCHAR2(32000);

    FUNCTION validate_parms_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
    BEGIN
      logs.dbg('Validate Parms');

      IF RTRIM(i_div) IS NULL THEN
        l_msg := 'Division is required';
      ELSIF RTRIM(i_llr_dt) IS NULL THEN
        l_msg := 'LLR date is required';
      ELSIF RTRIM(i_from_load) IS NULL THEN
        l_msg := 'From-Load is required';
      ELSIF RTRIM(i_to_load) IS NULL THEN
        l_msg := 'To-Load is required';
      ELSIF(   RTRIM(i_parm_list) IS NULL
            OR l_t_parms.COUNT = 0) THEN
        l_msg := 'From-List is required';
      ELSIF RTRIM(i_user_id) IS NULL THEN
        l_msg := 'UserID is required';
      END IF;

      RETURN(l_msg);
    END validate_parms_fn;

    FUNCTION load_info_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
    BEGIN
      logs.dbg('Get Load Info');
      op_order_load_pk.get_llr_depart_sp(l_div_part, l_llr_dt, i_to_load, l_llr_ts, l_depart_ts);

      IF l_llr_ts IS NULL THEN
        l_msg := 'Invalid Load Entered';
      END IF;   -- l_llr_ts IS NULL

      RETURN(l_msg);
    END load_info_fn;

    FUNCTION stop_assigned_to_another_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
    BEGIN
      logs.dbg('Check Stop Assigned to Another Customer');

      OPEN l_cv
       FOR
         SELECT 'Load and stop assigned to another customer. Order not moved.'
           FROM DUAL
          WHERE EXISTS(SELECT 1
                         FROM mclp040d md
                        WHERE md.div_part = l_div_part
                          AND md.loadd = i_to_load
                          AND md.stopd = l_to_stop
                          AND md.custd <> l_cust_id
                          AND l_cust_rte_ovrrd_sw = 'N')
             OR EXISTS(SELECT 1
                         FROM cust_rte_ovrrd_rt3c cro
                        WHERE cro.div_part = l_div_part
                          AND cro.llr_dt = l_llr_dt
                          AND cro.load_num = i_to_load
                          AND cro.stop_num = l_to_stop
                          AND cro.cust_id <> l_cust_id);

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END stop_assigned_to_another_fn;

    FUNCTION stop_in_use_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
    BEGIN
      logs.dbg('Check for Load/Stop already in use');

      -- Check for load/stop already in use for current week
      OPEN l_cv
       FOR
         SELECT 'Load/Stop/LLR already used by another customer. Order not moved.'
           FROM DUAL
          WHERE EXISTS(SELECT 1
                         FROM load_depart_op1f ld, stop_eta_op1g se
                        WHERE ld.div_part = l_div_part
                          AND ld.llr_ts = l_llr_ts
                          AND ld.load_num = i_to_load
                          AND se.div_part = ld.div_part
                          AND se.load_depart_sid = ld.load_depart_sid
                          AND se.stop_num = l_to_stop
                          AND se.cust_id <> l_cust_id
                          AND EXISTS(SELECT 1
                                       FROM ordp100a a
                                      WHERE a.div_part = se.div_part
                                        AND a.load_depart_sid = se.load_depart_sid
                                        AND a.custa = se.cust_id
                                        AND a.stata NOT IN('A', 'C')));

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END stop_in_use_fn;

    FUNCTION rstr_load_range_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
      l_cv   SYS_REFCURSOR;
    BEGIN
      logs.dbg('Check for restricted load range');

      -- Check for new load not allowed for order source with restricted load range
      OPEN l_cv
       FOR
         SELECT 'New load not allowed for order with restricted load range. Order not moved.'
           FROM DUAL
          WHERE EXISTS(SELECT 1
                         FROM (SELECT SUBSTR(x.parm_id, INSTR(x.parm_id, '_', -1) +1) AS ord_src,
                                      SUBSTR(x.val, 1, 4) AS min_load,
                                      SUBSTR(x.val, -4) AS max_load
                                 FROM (SELECT DISTINCT
                                              FIRST_VALUE(p.parm_id) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS parm_id,
                                              FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS val
                                         FROM appl_sys_parm_ap1s p
                                        WHERE p.parm_id LIKE 'LOAD_RANGE%'
                                          AND p.div_part IN(0, l_div_part)
                                      ) x) prm,
                              load_depart_op1f ld, ordp100a a
                        WHERE ld.div_part = l_div_part
                          AND ld.llr_dt = l_llr_dt
                          AND ld.load_num = i_from_load
                          AND a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.custa = l_cust_id
                          AND a.stata NOT IN('A', 'C')
                          AND a.ipdtsa = prm.ord_src
                          AND i_to_load NOT BETWEEN prm.min_load AND prm.max_load
                      );

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END rstr_load_range_fn;

    FUNCTION cust_rte_ovrrd_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
      l_cv   SYS_REFCURSOR;
    BEGIN
      logs.dbg('Get Override DepartTs');

      OPEN l_cv
       FOR
         SELECT r.depart_ts
           FROM cust_rte_ovrrd_rt3c r
          WHERE r.div_part = l_div_part
            AND r.cust_id = l_cust_id
            AND r.llr_dt = l_llr_dt
            AND r.load_num = i_to_load
            AND r.depart_ovrrd_sw = 'Y';

      FETCH l_cv
       INTO l_depart_ts;

      logs.dbg('Get Override Stop');

      OPEN l_cv
       FOR
         SELECT r.stop_num
           FROM cust_rte_ovrrd_rt3c r
          WHERE r.div_part = l_div_part
            AND r.cust_id = l_cust_id
            AND r.llr_dt = l_llr_dt
            AND r.load_num = i_to_load
            AND r.stop_ovrrd_sw = 'Y';

      FETCH l_cv
       INTO l_to_stop;

      IF l_cv%FOUND THEN
        l_cust_rte_ovrrd_sw := 'Y';
      END IF;   -- l_cv%FOUND

      logs.dbg('Get Override ETA');

      OPEN l_cv
       FOR
         SELECT r.eta_ts
           FROM cust_rte_ovrrd_rt3c r
          WHERE r.div_part = l_div_part
            AND r.cust_id = l_cust_id
            AND r.llr_dt = l_llr_dt
            AND r.load_num = i_to_load
            AND r.eta_ovrrd_sw = 'Y';

      FETCH l_cv
       INTO l_eta_ts;

      IF l_cv%FOUND THEN
        l_cust_rte_ovrrd_sw := 'Y';
      END IF;   -- l_cv%FOUND

      RETURN(l_msg);
    END cust_rte_ovrrd_fn;

    FUNCTION stop_info_fn
      RETURN VARCHAR2 IS
      l_msg      VARCHAR2(100);
      l_eta_day  VARCHAR2(3);
      l_eta_tm   NUMBER;
      l_eta_wk   NUMBER        := 0;
    BEGIN
      -------------------------------------------------------------------------
      -- Determine stop info
      --   If the load is not assigned to the customer then make sure the stop
      --     has not already been assigned.
      -------------------------------------------------------------------------
      logs.dbg('Customer Stop Info');

      -- override stop if customer assigned to load
      OPEN l_cv
       FOR
         SELECT stopd, etad, dayrcd, NVL(wkoffd, 0)
           FROM mclp040d
          WHERE div_part = l_div_part
            AND custd = l_cust_id
            AND loadd = i_to_load;

      FETCH l_cv
       INTO l_to_stop, l_eta_tm, l_eta_day, l_eta_wk;

      logs.dbg('Check for Cust Route Override');
      l_msg := cust_rte_ovrrd_fn;

      IF l_msg IS NULL THEN
        IF l_cv%FOUND THEN
          ----------------------------------
          -- Load Is Assigned to Customer
          ----------------------------------
          IF l_cust_rte_ovrrd_sw = 'N' THEN
            l_eta_ts := TO_DATE(TO_CHAR(NEXT_DAY(l_depart_ts - 1 +(NVL(l_eta_wk, 0) * 7), l_eta_day), 'YYYYMMDD')
                                || LPAD(l_eta_tm, 4, '0'),
                                'YYYYMMDDHH24MI'
                               );

            IF l_eta_ts < l_depart_ts THEN
              l_eta_ts := l_eta_ts + 7;
            END IF;   -- l_eta_ts < l_depart_ts
          END IF;   -- l_cust_rte_ovrrd_sw = 'N'
        ELSE
          ----------------------------------
          -- Load Not Assigned to Customer
          ----------------------------------
          l_msg := COALESCE(stop_assigned_to_another_fn, stop_in_use_fn, rstr_load_range_fn);

          IF l_msg IS NULL THEN
            IF l_cust_rte_ovrrd_sw = 'N' THEN
              logs.dbg('Get ETA From Original LLR/Load/Stop/Cust');

              OPEN l_cv
               FOR
                 SELECT se.eta_ts
                   FROM load_depart_op1f ld, stop_eta_op1g se
                  WHERE ld.div_part = l_div_part
                    AND ld.llr_dt = l_llr_dt
                    AND ld.load_num = i_from_load
                    AND se.div_part = ld.div_part
                    AND se.load_depart_sid = ld.load_depart_sid
                    AND se.cust_id = l_cust_id
                    AND se.stop_num = l_from_stop
                    AND EXISTS(SELECT 1
                                 FROM ordp100a a
                                WHERE a.div_part = ld.div_part
                                  AND a.load_depart_sid = ld.load_depart_sid
                                  AND a.custa = l_cust_id
                                  AND a.stata = 'O');

              FETCH l_cv
               INTO l_eta_ts;
            END IF;   -- l_cust_rte_ovrrd_sw = 'N'
          END IF;   -- l_msg IS NULL
        END IF;   --  l_cv%found
      END IF;   -- l_msg IS NULL

      RETURN(l_msg);
    END stop_info_fn;

    FUNCTION move_orders_fn
      RETURN VARCHAR2 IS
      TYPE l_rt_load_ords IS RECORD(
        llr_ts                     DATE,
        depart_ts                  DATE,
        eta_ts                     DATE,
        t_ord_nums                 type_ntab,
        t_locked_routing_ord_nums  type_ntab := type_ntab()
      );

      TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

      l_t_load_ords                l_tt_load_ords;
      l_load_depart_sid            NUMBER;
      l_ord_cnt                    PLS_INTEGER     := 0;
      l_c_prb             CONSTANT VARCHAR2(3)     := 'PRB';
      l_c_recapped        CONSTANT VARCHAR2(3)     := 'RCP';
      l_c_miss_po_cutoff  CONSTANT VARCHAR2(3)     := 'MPC';
      l_prb_ords                   VARCHAR2(32000);
      l_not_moved_cnt              PLS_INTEGER     := 0;
      l_msg                        typ.t_maxvc2;

      PROCEDURE add_prob_ord_sp(
        i_ord_num  IN  PLS_INTEGER,
        i_prb_cd   IN  VARCHAR2
      ) IS
      BEGIN
        CASE i_prb_cd
          WHEN l_c_recapped THEN
            l_recapped_ords := l_recapped_ords ||(CASE
                                                    WHEN l_recapped_ords IS NOT NULL THEN ','
                                                  END) || i_ord_num;
          WHEN l_c_miss_po_cutoff THEN
            l_miss_po_cutoff_ords := l_miss_po_cutoff_ords
                                     ||(CASE
                                          WHEN l_miss_po_cutoff_ords IS NOT NULL THEN ','
                                        END)
                                     || i_ord_num;
          WHEN l_c_prb THEN
            l_not_moved_cnt := l_not_moved_cnt + 1;
            l_prb_ords := l_prb_ords ||(CASE
                                          WHEN l_prb_ords IS NOT NULL THEN ','
                                        END) || i_ord_num;
        END CASE;
      EXCEPTION
        WHEN VALUE_ERROR THEN
          NULL;
      END add_prob_ord_sp;
    BEGIN
      logs.dbg('Get Load Order Info');

      SELECT   ld.llr_ts,
               ld.depart_ts,
               se.eta_ts,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = l_cust_id
                                AND a.stata = 'O'
                                AND NOT EXISTS(SELECT 1
                                                 FROM rte_grp_rt2g g, rte_stat_rt1s r, rte_grp_ord_rt3o o
                                                WHERE 'ROUT' IN(i_from_load, i_to_load)
                                                  AND a.dsorda = 'D'
                                                  AND g.div_part = a.div_part
                                                  AND r.rte_grp_num = g.rte_grp_num
                                                  AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                                  AND r.stop_num = se.stop_num
                                                  AND r.mcl_cust = cx.mccusb
                                                  AND r.stat_cd IN('SNT', 'WRK')
                                                  AND o.rte_grp_num = g.rte_grp_num
                                                  AND o.ord_num = a.ordnoa)
                            ) AS type_ntab
                   ) AS ord_nums,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a, rte_stat_rt1s r, rte_grp_rt2g g, rte_grp_ord_rt3o o
                              WHERE 'ROUT' IN(i_from_load, i_to_load)
                                AND a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = l_cust_id
                                AND a.stata = 'O'
                                AND a.dsorda = 'D'
                                AND g.div_part = a.div_part
                                AND r.rte_grp_num = g.rte_grp_num
                                AND r.shp_dt = DATE '1900-02-28' + a.shpja
                                AND r.stop_num = se.stop_num
                                AND r.mcl_cust = cx.mccusb
                                AND r.stat_cd IN('SNT', 'WRK')
                                AND o.rte_grp_num = g.rte_grp_num
                                AND o.ord_num = a.ordnoa
                            ) AS type_ntab
                   ) AS locked_routing_ord_nums
      BULK COLLECT INTO l_t_load_ords
          FROM load_depart_op1f ld, stop_eta_op1g se, mclp020b cx
         WHERE cx.div_part = l_div_part
           AND cx.custb = l_cust_id
           AND ld.div_part = l_div_part
           AND ld.llr_dt = l_llr_dt
           AND ld.load_num = i_from_load
           AND se.div_part = l_div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND se.cust_id = l_cust_id
           AND se.stop_num = l_from_stop
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.custa = se.cust_id
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.stata = 'O')
      ORDER BY ld.load_depart_sid;

      IF l_t_load_ords.COUNT > 0 THEN
        FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(l_div_part, l_llr_ts, i_to_load);
          logs.dbg('Move Orders');
          op_order_load_pk.move_ords_sp(l_div_part,
                                        l_cust_id,
                                        l_load_depart_sid,
                                        l_to_stop,
                                        l_eta_ts,
                                        l_t_load_ords(i).llr_ts,
                                        i_from_load,
                                        l_t_load_ords(i).depart_ts,
                                        l_from_stop,
                                        l_t_load_ords(i).eta_ts,
                                        'FORCELD',
                                        l_user_id,
                                        l_t_load_ords(i).t_ord_nums
                                       );

          IF l_t_load_ords(i).t_ord_nums.COUNT > 0 THEN
            l_ord_cnt := l_ord_cnt + l_t_load_ords(i).t_ord_nums.COUNT;
            FOR j IN l_t_load_ords(i).t_ord_nums.FIRST .. l_t_load_ords(i).t_ord_nums.LAST LOOP
              logs.dbg('Check Recapped Strict Order');

              IF op_strict_order_pk.is_recapped_fn(l_div_part, l_t_load_ords(i).t_ord_nums(j)) THEN
                add_prob_ord_sp(l_t_load_ords(i).t_ord_nums(j), l_c_recapped);
              END IF;   -- op_strict_order_pk.is_recapped_fn(l_t_load_ords(i).t_ord_nums(j))

              logs.dbg('Check Move will Order Lines to Miss PO Cutoff');

              IF op_strict_order_pk.ord_will_miss_po_cutoff_fn(l_div_part, l_t_load_ords(i).t_ord_nums(j), l_llr_ts) THEN
                add_prob_ord_sp(l_t_load_ords(i).t_ord_nums(j), l_c_miss_po_cutoff);
              END IF;   -- op_strict_order_pk.ord_will_miss_po_cutoff_fn(l_t_load_ords(i).t_ord_nums(j), l_llr_ts)
            END LOOP;
          END IF;   -- l_t_load_ords(i).t_ord_nums.COUNT > 0

          IF l_t_load_ords(i).t_locked_routing_ord_nums.COUNT > 0 THEN
            logs.dbg('Locked Routing Orders');
            FOR j IN l_t_load_ords(i).t_locked_routing_ord_nums.FIRST .. l_t_load_ords(i).t_locked_routing_ord_nums.LAST LOOP
              add_prob_ord_sp(l_t_load_ords(i).t_locked_routing_ord_nums(j), l_c_prb);
            END LOOP;
          END IF;   -- l_t_load_ords(i).t_locked_routing_ord_nums.COUNT > 0
        END LOOP;
      END IF;   -- l_t_load_ords.COUNT > 0

      IF l_not_moved_cnt > 0 THEN
        IF l_not_moved_cnt = l_ord_cnt THEN
          l_msg := 'No orders moved for Cust: '
                   || l_cust_id
                   || ' Stop: '
                   || l_from_stop
                   || '. Orders were not found and/or status changed.';
        ELSE
          l_msg := 'Some orders moved for Cust: '
                   || l_cust_id
                   || ' Stop: '
                   || l_from_stop
                   || '.'
                   || cnst.newline_char
                   || 'However, the following order(s) were not moved due to not found and/or status change:'
                   || cnst.newline_char
                   || l_prb_ords;
        END IF;   -- l_not_moved_cnt = l_ord_cnt
      END IF;   -- l_not_moved_cnt > 0

      COMMIT;
      RETURN(l_msg);
    END move_orders_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'FromLoad', i_from_load);
    logs.add_parm(lar_parm, 'ToLoad', i_to_load);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_user_id := SUBSTR(i_user_id, 1, 20);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Parse');
    l_t_parms := lob2table.separatedcolumns2(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter);
    logs.dbg('Validate, Get Load Info');
    o_msg := COALESCE(validate_parms_fn, load_info_fn);

    IF o_msg IS NULL THEN
      FOR i IN l_t_parms.FIRST .. l_t_parms.LAST LOOP
        l_from_stop := l_t_parms(i).column1;
        l_cust_id := l_t_parms(i).column2;
        l_to_stop := l_from_stop;

        -- check for existing order for customer on llr/load
        OPEN l_cv
         FOR
           SELECT se.stop_num, ld.depart_ts, se.eta_ts
             FROM load_depart_op1f ld, stop_eta_op1g se
            WHERE ld.div_part = l_div_part
              AND ld.llr_ts = l_llr_ts
              AND ld.load_num = i_to_load
              AND se.div_part = ld.div_part
              AND se.load_depart_sid = ld.load_depart_sid
              AND se.cust_id = l_cust_id
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = se.div_part
                            AND a.load_depart_sid = se.load_depart_sid
                            AND a.custa = se.cust_id
                            AND a.stata NOT IN('A', 'C'));

        FETCH l_cv
         INTO l_to_stop, l_depart_ts, l_eta_ts;

        IF l_cv%NOTFOUND THEN
          o_msg := stop_info_fn;
        END IF;   -- l_cv%NOTFOUND

        IF o_msg IS NULL THEN
          l_move_msg := l_move_msg ||(CASE
                                        WHEN l_move_msg IS NOT NULL THEN cnst.newline_char
                                      END) || move_orders_fn;
        END IF;   -- o_msg IS NULL
      END LOOP;

      IF l_move_msg IS NOT NULL THEN
        o_msg := o_msg ||(CASE
                            WHEN o_msg IS NOT NULL THEN cnst.newline_char
                          END) || l_move_msg;
      END IF;   -- l_move_msg IS NOT NULL
    END IF;   -- o_msg IS NULL

    IF o_msg IS NULL THEN
      o_msg := op_const_pk.msg_typ_info
               || op_const_pk.field_delimiter
               || 'Selected Orders, Stops are Moved to Specified Load';
    ELSE
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || o_msg;
    END IF;   -- o_msg IS NULL

    IF l_recapped_ords IS NOT NULL THEN
      o_msg := o_msg
               || cnst.newline_char
               || cnst.newline_char
               || op_strict_order_pk.g_c_recapped_msg
               || cnst.newline_char
               || l_recapped_ords;
    END IF;   -- l_recapped_ords IS NOT NULL

    IF l_miss_po_cutoff_ords IS NOT NULL THEN
      o_msg := o_msg
               || cnst.newline_char
               || cnst.newline_char
               || op_strict_order_pk.g_c_miss_po_cutoff_msg
               || cnst.newline_char
               || l_miss_po_cutoff_ords;
    END IF;   -- l_miss_po_cutoff_ords IS NOT NULL

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_msg := op_const_pk.msg_typ_err
               || op_const_pk.field_delimiter
               || op_process_control_pk.restricted_msg_fn(op_const_pk.prcs_ord_mov, l_div_part);
      logs.warn(SQLERRM, lar_parm);
    WHEN OTHERS THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'Selected Orders NOT Moved -- Error Occurred';
      logs.err(lar_parm, NULL, FALSE);
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
  END move_stops_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ETA_TIME_SP
  ||   Update ETA for LLR/Load with list of Stop/Cust/New ETA.
  ||
  ||  Called by ETA Maintenance UI.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/27/06 | rhalpai | Original
  || 08/22/08 | rhalpai | Changed to handle order updates to ETA date and time
  ||                    | instead of just time. IM438778
  || 09/08/08 | rhalpai | Changed to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/15/13 | rhalpai | Add logic to use Cust Route Override table. IM-122562
  || 10/28/13 | rhalpai | Change logic to include Load as part of primary key
  ||                    | of CUST_RTE_OVRRD_RT3C and to set corresponding
  ||                    | OvrrdSw for Eta,Depart,Stop. IM-123463
  || 10/14/17 | rhalpai | Add DivPart in calls to
  ||                    | OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP,
  ||                    | OP_PROCESS_CONTROL_PK.RESTRICTED_MSG_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_eta_time_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    i_load_num   IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    i_parm_list  IN      CLOB,
    o_msg        OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_ORDER_MOVES_PK.UPD_ETA_TIME_SP';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_user_id            mclp300d.resusd%TYPE;
    l_div_part           NUMBER;
    l_t_parms            lob_rows_t;
    l_stop_num           NUMBER;
    l_cust_id            sysp200c.acnoc%TYPE;
    l_eta_str            VARCHAR2(16);

    FUNCTION validate_parms_fn
      RETURN VARCHAR2 IS
      l_msg  VARCHAR2(100);
    BEGIN
      logs.dbg('Validate Parms');

      IF RTRIM(i_div) IS NULL THEN
        l_msg := 'Division is required';
      ELSIF RTRIM(i_llr_dt) IS NULL THEN
        l_msg := 'LLR date is required';
      ELSIF RTRIM(i_load_num) IS NULL THEN
        l_msg := 'Load is required';
      ELSIF(   RTRIM(i_parm_list) IS NULL
            OR l_t_parms.COUNT = 0) THEN
        l_msg := 'Stop/Cust/ETA Parm-List is required';
      ELSIF RTRIM(i_user_id) IS NULL THEN
        l_msg := 'UserID is required';
      END IF;

      RETURN(l_msg);
    END validate_parms_fn;

    FUNCTION validate_eta_stop_seq_fn
      RETURN VARCHAR2 IS
      l_cv   SYS_REFCURSOR;
      l_msg  VARCHAR2(150);
    BEGIN
      logs.dbg('Validate ETA/Stop Seq');

      OPEN l_cv
       FOR
         SELECT   'Stop out of sequence for ETA!'
                  || cnst.newline_char
                  || 'Stop: '
                  || x.stop_num
                  || ' Cust: '
                  || x.cust_id
                  || ' ETA: '
                  || x.eta_ts
                  || ' scheduled before '
                  || cnst.newline_char
                  || 'Stop: '
                  || x.prev_stop
                  || ' Cust: '
                  || x.prev_cust
                  || ' ETA: '
                  || x.prev_eta AS seq_msg
             FROM (SELECT o.stop_num, o.cust_id, NVL(p.eta_ts, o.eta_ts) AS eta_ts,
                          LAG(o.stop_num, 1, NULL) OVER(ORDER BY NVL(p.eta_ts, o.eta_ts)) AS prev_stop,
                          LAG(o.cust_id, 1, NULL) OVER(ORDER BY NVL(p.eta_ts, o.eta_ts)) AS prev_cust,
                          LAG(NVL(p.eta_ts, o.eta_ts), 1, NULL) OVER(ORDER BY NVL(p.eta_ts, o.eta_ts)) AS prev_eta
                     FROM (SELECT se.stop_num, se.cust_id, TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts
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
                                            AND a.stata NOT IN('A', 'C'))) o,
                          (SELECT t.column1 AS stop_num, t.column2 AS cust_id, t.column3 AS eta_ts
                             FROM TABLE(l_t_parms) t) p
                    WHERE p.stop_num(+) = o.stop_num
                      AND p.cust_id(+) = o.cust_id) x
            WHERE x.stop_num > x.prev_stop
         ORDER BY x.eta_ts;

      logs.dbg('Fetch for ETA/Stop Seq');

      FETCH l_cv
       INTO l_msg;

      RETURN(l_msg);
    END validate_eta_stop_seq_fn;

    FUNCTION upd_fn
      RETURN VARCHAR2 IS
      TYPE l_rt_load_ords IS RECORD(
        llr_ts      DATE,
        depart_ts   DATE,
        eta_ts      DATE,
        assgnd_sw   VARCHAR2(1),
        t_ord_nums  type_ntab
      );

      TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

      l_t_load_ords      l_tt_load_ords;
      l_eta_ts           DATE;
      l_load_depart_sid  NUMBER;
      l_msg              typ.t_maxvc2;
    BEGIN
      logs.dbg('Upd Order ETA');

      SELECT ld.llr_ts,
             ld.depart_ts,
             se.eta_ts,
             (CASE
                WHEN EXISTS(SELECT 1
                              FROM mclp040d md
                             WHERE md.div_part = se.div_part
                               AND md.custd = se.cust_id
                               AND md.loadd = ld.load_num) THEN 'Y'
                ELSE 'N'
              END
             ) AS assgnd_sw,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = se.div_part
                              AND a.load_depart_sid = se.load_depart_sid
                              AND a.custa = se.cust_id
                              AND a.stata = 'O'
                          ) AS type_ntab
                 ) AS ord_nums
      BULK COLLECT INTO l_t_load_ords
        FROM load_depart_op1f ld, stop_eta_op1g se
       WHERE ld.div_part = l_div_part
         AND ld.llr_dt = l_llr_dt
         AND ld.load_num = i_load_num
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = l_cust_id
         AND se.stop_num = l_stop_num
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = se.div_part
                       AND a.load_depart_sid = se.load_depart_sid
                       AND a.custa = se.cust_id
                       AND a.stata = 'O');

      IF l_t_load_ords.COUNT > 0 THEN
        l_eta_ts := TO_DATE(l_eta_str, 'YYYY-MM-DD HH24:MI');
        FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
          IF l_t_load_ords(i).assgnd_sw = 'Y' THEN
            logs.dbg('Add/Upd CustRteOvrrd');
            MERGE INTO cust_rte_ovrrd_rt3c cro
                 USING (SELECT 1 tst
                          FROM DUAL) x
                    ON (    cro.div_part = l_div_part
                        AND cro.cust_id = l_cust_id
                        AND cro.llr_dt = l_llr_dt
                        AND cro.load_num = i_load_num
                        AND x.tst > 0)
              WHEN MATCHED THEN
                UPDATE
                   SET cro.depart_ts = l_t_load_ords(i).depart_ts, cro.stop_num = l_stop_num, cro.eta_ts = l_eta_ts,
                       cro.eta_ovrrd_sw = 'Y'
              WHEN NOT MATCHED THEN
                INSERT(div_part, cust_id, llr_dt, load_num, depart_ts, stop_num, eta_ts, depart_ovrrd_sw, stop_ovrrd_sw,
                       eta_ovrrd_sw)
                VALUES(l_div_part, l_cust_id, l_llr_dt, i_load_num, l_t_load_ords(i).depart_ts, l_stop_num, l_eta_ts,
                       'N', 'N', 'Y');
          END IF;   -- l_t_load_ords(i).assgnd_sw = 'Y'

          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(l_div_part, l_t_load_ords(i).llr_ts, i_load_num);
          logs.dbg('Move Orders');
          op_order_load_pk.move_ords_sp(l_div_part,
                                        l_cust_id,
                                        l_load_depart_sid,
                                        l_stop_num,
                                        l_eta_ts,
                                        l_t_load_ords(i).llr_ts,
                                        i_load_num,
                                        l_t_load_ords(i).depart_ts,
                                        l_stop_num,
                                        l_t_load_ords(i).eta_ts,
                                        'ETAMAINT',
                                        l_user_id,
                                        l_t_load_ords(i).t_ord_nums
                                       );
        END LOOP;
        COMMIT;
      ELSE
        l_msg := 'No orders updated for Cust: '
                 || l_cust_id
                 || ' Stop: '
                 || l_stop_num
                 || ' ETA: '
                 || l_eta_str
                 || '. Orders were not found and/or status changed.';
      END IF;   -- l_t_load_ords.COUNT > 0

      RETURN(l_msg);
    END upd_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_user_id := SUBSTR(i_user_id, 1, 20);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Parse');
    l_t_parms := lob2table.separatedcolumns2(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter);
    logs.dbg('Validate');
    o_msg := COALESCE(validate_parms_fn, validate_eta_stop_seq_fn);

    IF o_msg IS NULL THEN
      logs.dbg('Process');
      FOR i IN l_t_parms.FIRST .. l_t_parms.LAST LOOP
        logs.dbg('Set Parms');
        l_stop_num := l_t_parms(i).column1;
        l_cust_id := l_t_parms(i).column2;
        l_eta_str := l_t_parms(i).column3;
        logs.dbg('Upd');
        o_msg := o_msg ||(CASE
                            WHEN o_msg IS NOT NULL THEN cnst.newline_char
                          END) || upd_fn;
      END LOOP;
    END IF;   -- o_msg IS NULL

    IF o_msg IS NULL THEN
      o_msg := op_const_pk.msg_typ_info || op_const_pk.field_delimiter || 'Selected Orders, Stops have been updated';
    ELSE
      o_msg := op_const_pk.msg_typ_err
               || op_const_pk.field_delimiter
               || 'No Updates Applied!'
               || cnst.newline_char
               || o_msg;
    END IF;

    -- o_msg IS NULL
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_msg := op_const_pk.msg_typ_err
               || op_const_pk.field_delimiter
               || op_process_control_pk.restricted_msg_fn(op_const_pk.prcs_ord_mov, l_div_part);
      logs.warn(SQLERRM, lar_parm);
    WHEN OTHERS THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || 'Unhandled Error. No updates applied.';
      logs.err(lar_parm, NULL, FALSE);
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ord_mov,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
  END upd_eta_time_sp;
END op_order_moves_pk;
/

