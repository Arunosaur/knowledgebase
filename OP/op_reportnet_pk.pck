CREATE OR REPLACE PACKAGE op_reportnet_pk IS
/*
||------------------------------------------------------------------------------
|| The procedures in this package are called by Cognos (BI Team).
||------------------------------------------------------------------------------
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

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE p00_dist_rpt_sp(
    i_div_part  IN      NUMBER,
    i_dt        IN      DATE,
    o_cur       OUT     SYS_REFCURSOR
  );

  PROCEDURE p00_dist_rpt_by_item_sp(
    i_div_part  IN      NUMBER,
    i_dt        IN      DATE,
    o_cur       OUT     SYS_REFCURSOR
  );

  PROCEDURE mcldd20(
    i_div_part  IN      NUMBER,
    i_load_num  IN      VARCHAR2,
    o_cur       OUT     SYS_REFCURSOR
  );
END op_reportnet_pk;
/

CREATE OR REPLACE PACKAGE BODY op_reportnet_pk IS
/*
||------------------------------------------------------------------------------
|| The procedures in this package are called by Cognos (BI Team).
||------------------------------------------------------------------------------
*/
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
  || P00_DIST_RPT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || ??/??/?? | unknown | Original
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/25/13 | dlbeal  | Restore alias for cognos-IM120141
  ||----------------------------------------------------------------------------
  */
  PROCEDURE p00_dist_rpt_sp(
    i_div_part  IN      NUMBER,
    i_dt        IN      DATE,
    o_cur       OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPORTNET_PK.P00_DIST_RPT_SP';
    lar_parm             logs.tar_parm;
    l_ship_dt            NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Dt', i_dt);
    logs.info('ENTRY', lar_parm);
    l_ship_dt := i_dt - DATE '1900-02-28';

    OPEN o_cur
     FOR
       SELECT   d.div_id AS div, cx.mccusb AS cust, c.namec AS custname, ld.load_num AS route,
                TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_date, a.ldtypa AS loadtype,
                ct.taxjrc AS taxjur, COUNT(*) AS COUNT, TO_CHAR(se.eta_ts, 'YYYY-MM-DD') AS eta_date
           FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp030c ct,
                mclp020b cx
          WHERE d.div_part = i_div_part
            AND a.div_part = d.div_part
            AND a.dsorda = 'D'
            AND a.ldtypa BETWEEN 'P00' AND 'P99'
            AND a.shpja <= l_ship_dt
            AND a.excptn_sw = 'N'
            AND a.stata = 'O'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND ct.div_part = a.div_part
            AND ct.custc = a.custa
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
       GROUP BY cx.mccusb, c.namec, ld.load_num, a.shpja, a.ldtypa, ct.taxjrc, d.div_id, se.eta_ts
       ORDER BY ct.taxjrc, cx.mccusb, a.shpja, ld.load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END p00_dist_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || P00_DIST_RPT_BY_ITEM_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || ??/??/?? | unknown | Original
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/25/13 | dlbeal  | Restore alias for cognos-IM120141
  ||----------------------------------------------------------------------------
  */
  PROCEDURE p00_dist_rpt_by_item_sp(
    i_div_part  IN      NUMBER,
    i_dt        IN      DATE,
    o_cur       OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPORTNET_PK.P00_DIST_RPT_BY_ITEM_SP';
    lar_parm             logs.tar_parm;
    l_ship_dt            NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Dt', i_dt);
    logs.info('ENTRY', lar_parm);
    l_ship_dt := i_dt - DATE '1900-02-28';

    OPEN o_cur
     FOR
       SELECT   d.div_id AS div, cx.mccusb AS cust, c.namec AS custname, ct.taxjrc AS taxjur, b.orditb AS item,
                b.ordqtb AS ordqty, ld.load_num AS route,
                TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_date, a.ldtypa AS loadtype,
                TO_CHAR(se.eta_ts, 'YYYY-MM-DD') AS eta_date
           FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, ordp120b b,
                mclp030c ct, mclp020b cx
          WHERE d.div_part = i_div_part
            AND a.div_part = d.div_part
            AND a.dsorda = 'D'
            AND a.ldtypa BETWEEN 'P00' AND 'P99'
            AND a.shpja <= l_ship_dt
            AND a.excptn_sw = 'N'
            AND a.stata = 'O'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND ct.div_part = a.div_part
            AND ct.custc = a.custa
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
       ORDER BY ct.taxjrc, cx.mccusb, a.shpja, ld.load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END p00_dist_rpt_by_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || MCLDD20
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || ??/??/?? | unknown | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mcldd20(
    i_div_part  IN      NUMBER,
    i_load_num  IN      VARCHAR2,
    o_cur       OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_REPORTNET_PK.MCLDD20';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.info('ENTRY', lar_parm);

    OPEN o_cur
     FOR
       SELECT   rpts.load_num AS load_num, l.destc AS destination, COUNT(DISTINCT(rpts.stop_num)) AS stops,
                NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, rpts.qty_alloc, 0)), 0) AS cases,
                NVL(SUM(rpts.tote_count), 0) AS totes, NVL(SUM(rpts.product_weight), 0) AS weight,
                NVL(SUM(rpts.product_cube), 0) AS product_cube,
                NVL(SUM(DECODE(rpts.tote_count, 0, rpts.product_cube, rpts.tote_count * b.outerb)), 0) AS tote_cube,
                l.depdac AS dep_day, l.deptmc AS dep_tm, d.div_id AS divc,
                CASE MAX(DISTINCT(bc2c.load_status))
                  WHEN 'A' THEN 'CLOSED'
                  WHEN 'R' THEN 'RELEASED'
                  ELSE 'UNKNOWN'
                END AS load_status
           FROM div_mstr_di1d d, mclp120c l, mclane_manifest_rpts rpts, load_clos_cntrl_bc2c bc2c, mclp200b b
          WHERE d.div_part = i_div_part
            AND l.div_part = d.div_part
            AND l.loadc = i_load_num
            AND bc2c.div_part = l.div_part
            AND bc2c.load_num = l.loadc
            AND bc2c.test_bil_load_sw = 'N'
            AND rpts.div_part = bc2c.div_part
            AND rpts.load_num = bc2c.load_num
            AND rpts.create_ts = (SELECT MAX(rz.create_ts)
                                    FROM mclane_manifest_rpts rz
                                   WHERE rz.div_part = i_div_part
                                     AND rz.strategy_id > 0
                                     AND rz.load_num = i_load_num)
            AND rpts.strategy_id = 0
            AND b.div_part(+) = rpts.div_part
            AND b.totctb(+) = rpts.tote_cat
       GROUP BY d.div_id, rpts.load_num, l.destc, l.depdac, l.deptmc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END mcldd20;
BEGIN
  env.set_app_cd('OPCIG');
END op_reportnet_pk;
/

