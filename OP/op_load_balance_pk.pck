CREATE OR REPLACE PACKAGE op_load_balance_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_ord_hdr IS RECORD(
    load_depart_sid  NUMBER,
    cust_id          VARCHAR2(8),
    ord_num          NUMBER,
    ord_typ          VARCHAR2(1),
    ord_src          VARCHAR2(8),
    ord_stat         VARCHAR2(1),
    po_num           VARCHAR2(30),
    ship_dt          DATE
  );

  TYPE g_tt_ord_hdr IS TABLE OF g_rt_ord_hdr;
  TYPE g_cvt_ord_hdr IS REF CURSOR RETURN g_rt_ord_hdr;

  TYPE g_rt_ord_dtl IS RECORD(
    llr_dt           DATE,
    load_num         VARCHAR2(4),
    depart_ts        DATE,
    stop_num         NUMBER,
    eta_ts           DATE,
    corp_cd          VARCHAR2(3),
    cust_id          VARCHAR2(8),
    mfst_catg        VARCHAR2(3),
    tote_catg        VARCHAR2(3),
    outer_cube       NUMBER,
    inner_cube       NUMBER,
    case_cnt         NUMBER,
    tote_cnt         NUMBER,
    units_in_totes   NUMBER,
    prod_cube        NUMBER,
    prod_wt          NUMBER,
    dist_cube        NUMBER,
    dist_wt          NUMBER
  );

  TYPE g_tt_ord_dtl IS TABLE OF g_rt_ord_dtl;
  TYPE g_cvt_ord_dtl IS REF CURSOR RETURN g_rt_ord_dtl;

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION ord_hdr_cur_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_load_num   IN  VARCHAR2 DEFAULT NULL,
    i_stop_num   IN  NUMBER DEFAULT NULL
  )
    RETURN g_cvt_ord_hdr;

  FUNCTION ord_hdr_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_load_num   IN  VARCHAR2 DEFAULT NULL,
    i_stop_num   IN  NUMBER DEFAULT NULL
  )
    RETURN g_tt_ord_hdr PIPELINED;

  FUNCTION ord_dtl_fn(
    i_div_part              IN  NUMBER,
    i_cv_ord_hdr            IN  g_cvt_ord_hdr,
    i_load_stat             IN  VARCHAR2 DEFAULT 'ALL',
    i_excl_no_inv_avail_sw  IN  VARCHAR2 DEFAULT 'Y'
  )
    RETURN g_tt_ord_dtl PIPELINED;

  FUNCTION status_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load       IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION manifest_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load       IN  VARCHAR2 DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop       IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION item_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_mfst_catg  IN  VARCHAR2,
    i_load       IN  VARCHAR2 DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop       IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION cube_wgt_by_stop_cur_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END op_load_balance_pk;
/

CREATE OR REPLACE PACKAGE BODY op_load_balance_pk IS
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
  || ORD_HDR_CUR_FN
  ||  Build a cursor of order header info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/23 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_cur_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_load_num   IN  VARCHAR2 DEFAULT NULL,
    i_stop_num   IN  NUMBER DEFAULT NULL
  )
    RETURN g_cvt_ord_hdr IS
    l_c_module  CONSTANT typ.t_maxfqnm    := 'OP_LOAD_BALANCE_PK.ORD_HDR_CUR_FN';
    lar_parm             logs.tar_parm;
    l_load_stat          VARCHAR2(30);
    l_cv                 g_cvt_ord_hdr;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_load_stat := UPPER(NVL(i_load_stat, 'ALL'));

    OPEN l_cv
     FOR
      WITH noa_ord_src AS(
        SELECT s.ord_src
         FROM sub_prcs_ord_src s
        WHERE s.div_part = i_div_part
          AND s.prcs_id = 'LOAD BALANCE'
          AND s.prcs_sbtyp_cd = 'NAO'
      ), blb_ord_src AS(
        SELECT s.ord_src
         FROM sub_prcs_ord_src s
        WHERE s.div_part = i_div_part
          AND s.prcs_id = 'LOAD BALANCE'
          AND s.prcs_sbtyp_cd = 'BLB'
      ), blb_noa_ord_src AS(
        SELECT s.ord_src
          FROM blb_ord_src s
        UNION
        SELECT s.ord_src
          FROM noa_ord_src s
      ), noa_ord AS(
        SELECT a.ordnoa
          FROM load_depart_op1f ld, ordp100a a
         WHERE ld.div_part = i_div_part
           AND (   i_llr_dt IS NULL
                OR ld.llr_dt = i_llr_dt)
           AND (   i_load_num IS NULL
                OR ld.load_num = i_load_num)
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND (   i_stop_num IS NULL
                OR i_stop_num = (SELECT se.stop_num
                                   FROM stop_eta_op1g se
                                  WHERE se.div_part = a.div_part
                                    AND se.load_depart_sid = a.load_depart_sid
                                    AND se.cust_id = a.custa)
               )
           AND a.stata IN('O', 'I')
           AND l_load_stat IN('ALL', 'OPEN', 'PARTIAL', 'PARTIAL W/OPEN', 'BILLED W/OPEN')
           AND a.excptn_sw = 'N'
           AND a.dsorda IN('R', 'D')
           AND a.ipdtsa IN(SELECT s.ord_src
                             FROM noa_ord_src s)
           AND NOT EXISTS(SELECT 1
                            FROM ordp100a a2
                           WHERE a2.div_part = a.div_part
                             AND a2.load_depart_sid = a.load_depart_sid
                             AND a2.custa = a.custa
                             AND a2.ipdtsa NOT IN(SELECT s2.ord_src
                                                    FROM blb_noa_ord_src s2)
                             AND a2.stata IN('O', 'I', 'P', 'R')
                             AND a2.excptn_sw = 'N'
                             AND a2.dsorda = 'R')
      )
      SELECT a.load_depart_sid, a.custa AS cust_id, a.ordnoa AS ord_num, a.dsorda AS ord_typ,
             a.ipdtsa AS ord_src, a.stata AS ord_stat,
             DECODE(g.cntnr_trckg_sw, 'N', NULL, 'Y', RTRIM(REPLACE(a.cpoa, '0'))) AS po_num,
             DATE '1900-02-28' + a.shpja AS ship_dt
        FROM load_depart_op1f ld, ordp100a a, sysp200c c, mclp100a g
       WHERE ld.div_part = i_div_part
         AND (   i_llr_dt IS NULL
              OR ld.llr_dt = i_llr_dt)
         AND (   i_load_num IS NULL
              OR ld.load_num = i_load_num)
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND (   i_stop_num IS NULL
              OR i_stop_num = (SELECT se.stop_num
                                 FROM stop_eta_op1g se
                                WHERE se.div_part = a.div_part
                                  AND se.load_depart_sid = a.load_depart_sid
                                  AND se.cust_id = a.custa)
             )
         AND a.excptn_sw = 'N'
         AND a.dsorda IN('R', 'D')
         AND a.stata IN('O', 'I', 'P', 'R', 'A')
         AND (   l_load_stat IN('ALL', 'PARTIAL', 'PARTIAL W/OPEN', 'BILLED W/OPEN')
              OR (    l_load_stat = 'SHIPPED'
                  AND a.stata = 'A')
              OR (    l_load_stat = 'BILLED'
                  AND a.stata = 'R')
              OR (    l_load_stat = 'OPEN'
                  AND a.stata IN('O', 'I')
                 )
             )
         AND a.ipdtsa NOT IN(SELECT s.ord_src
                               FROM blb_ord_src s)
         AND a.ordnoa NOT IN(SELECT noa.ordnoa
                               FROM noa_ord noa)
         AND EXISTS(SELECT 1
                      FROM ordp120b b
                     WHERE b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND (   (    b.statb IN('O', 'I')
                                AND b.ordqtb > 0
                                AND b.ntshpb IS NULL)
                            OR (    b.statb IN('P', 'T', 'R', 'A')
                                AND b.pckqtb > 0)
                           )
                       AND b.excptn_sw = 'N'
                       AND b.subrcb < 999)
         AND c.div_part = a.div_part
         AND c.acnoc = a.custa
         AND c.statc IN('1', '3')
         AND g.div_part = c.div_part
         AND g.cstgpa = c.retgpc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_hdr_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_HDR_FN
  ||  Pipeline cursor of order header info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/23 | rhalpai | Original. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_load_num   IN  VARCHAR2 DEFAULT NULL,
    i_stop_num   IN  NUMBER DEFAULT NULL
  )
    RETURN g_tt_ord_hdr PIPELINED IS
    l_c_module  CONSTANT typ.t_maxfqnm    := 'OP_LOAD_BALANCE_PK.ORD_HDR_FN';
    lar_parm             logs.tar_parm;
    l_load_stat          VARCHAR2(30);
    l_cv                 g_cvt_ord_hdr;
    l_t_ord_hdr          g_tt_ord_hdr;
    l_idx                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_load_stat := UPPER(NVL(i_load_stat, 'ALL'));
    l_cv := ord_hdr_cur_fn(i_div_part, i_llr_dt, l_load_stat, i_load_num, i_stop_num);

    LOOP
      FETCH l_cv
      BULK COLLECT INTO l_t_ord_hdr
      LIMIT 100;

      EXIT WHEN l_t_ord_hdr.COUNT = 0;
      l_idx := l_t_ord_hdr.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        PIPE ROW(l_t_ord_hdr(l_idx));
        l_idx := l_t_ord_hdr.NEXT(l_idx);
      END LOOP;
    END LOOP;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN;
  EXCEPTION
    WHEN no_data_needed THEN
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_hdr_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTL_FN
  ||  Pipeline cursor of order detail info at cust level.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/23 | rhalpai | Original. PIR18901
  || 07/21/23 | rhalpai | Remove PO_NUM column since billed data pulled from MCLP370C Tote table does not contain PO to match on.
  ||                    | Fixes problem where data is rolled up at customer/PO level while tote counts for billed loads
  ||                    | is at customer level so the summed tote count for customers with multiple POs are overstated. SDHD-1647963
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtl_fn(
    i_div_part              IN  NUMBER,
    i_cv_ord_hdr            IN  g_cvt_ord_hdr,
    i_load_stat             IN  VARCHAR2 DEFAULT 'ALL',
    i_excl_no_inv_avail_sw  IN  VARCHAR2 DEFAULT 'Y'
  )
    RETURN g_tt_ord_dtl PIPELINED IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.ORD_DTL_FN';
    lar_parm                logs.tar_parm;
    l_load_stat             VARCHAR2(30);
    l_excl_no_inv_avail_sw  VARCHAR2(1);
    l_t_ord_hdr             g_tt_ord_hdr;
    l_t_ord_dtl             g_tt_ord_dtl;
    l_idx                   PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.add_parm(lar_parm, 'ExclNoInvAvailSw', i_excl_no_inv_avail_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_load_stat := NVL(UPPER(i_load_stat), 'ALL');
    l_excl_no_inv_avail_sw := NVL(UPPER(i_excl_no_inv_avail_sw), 'Y');

    FETCH i_cv_ord_hdr
    BULK COLLECT INTO l_t_ord_hdr;

    WITH ord_hdr AS(
      SELECT ld.llr_dt, ld.load_num, ld.depart_ts, se.stop_num, se.eta_ts, LPAD(cx.corpb, 3, '0') AS corp_cd,
             se.cust_id, a.ordnoa AS ord_num, a.dsorda AS ord_typ, a.ipdtsa AS ord_src,
             a.stata AS ord_stat, oh.po_num
        FROM TABLE(l_t_ord_hdr) oh, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
       WHERE ld.div_part = i_div_part
         AND ld.load_depart_sid = oh.load_depart_sid
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = oh.cust_id
         AND cx.div_part = se.div_part
         AND cx.custb = se.cust_id
         AND a.div_part = i_div_part
         AND a.ordnoa = oh.ord_num
    ), o AS(
      SELECT   oh.llr_dt, oh.load_num, oh.depart_ts, oh.stop_num, oh.eta_ts, oh.corp_cd, oh.cust_id,
               b.manctb AS mfst_catg, b.totctb AS tote_catg, ct.outerb AS outer_cube, ct.innerb AS inner_cube,
               SUM(DECODE(b.totctb, NULL, b.ordqtb)) AS case_cnt,
/*               DECODE(ct.boxb,
                      'N', DECODE(ct.pccntb,
                                  'Y', CEIL(SUM(b.ordqtb) / ct.totcnb),
                                  'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                            / DECODE(ct.innerb,
                                                     NULL, .000001,
                                                     0, .000001,
                                                     ct.innerb
                                                    )
                                           )
                                 )
                     ) AS tote_cnt,*/
               DECODE(ct.pccntb,
                      'Y', CEIL(SUM(b.ordqtb) / ct.totcnb),
                      'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                / DECODE(ct.innerb,
                                         NULL, .000001,
                                         0, .000001,
                                         ct.innerb
                                        )
                               )
                     ) AS tote_cnt,
               SUM(DECODE(b.totctb, NULL, 0, b.ordqtb)) AS units_in_totes,
               SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube,
               SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt,
               SUM(DECODE(oh.ord_typ, 'D',(NVL(e.cubee, .01) * b.ordqtb))) AS dist_cube,
               SUM(DECODE(oh.ord_typ, 'D',(NVL(e.wghte, .01) * b.ordqtb))) AS dist_wt
          FROM ord_hdr oh
               LEFT OUTER JOIN ordp120b b
                 ON (    b.div_part = i_div_part
                     AND b.ordnob = oh.ord_num
                     AND b.excptn_sw = 'N'
                     AND b.statb IN('O', 'I')
                     AND b.subrcb < 999
                     AND b.ordqtb > 0
                     AND b.ntshpb IS NULL
                     AND (   l_excl_no_inv_avail_sw = 'N'
                          OR NOT EXISTS(SELECT 1
                                          FROM whsp300c w
                                         WHERE w.div_part = b.div_part
                                           AND w.itemc = b.itemnb
                                           AND w.uomc = b.sllumb
                                           AND w.taxjrc IS NULL
                                           AND w.qavc = 0)
                         )
                    )
               LEFT OUTER JOIN sawp505e e
                 ON (    e.iteme = b.itemnb
                     AND e.uome = b.sllumb)
               LEFT OUTER JOIN mclp200b ct
                 ON (    ct.div_part = b.div_part
                     AND ct.totctb = b.totctb)
         WHERE l_load_stat NOT IN('BILLED', 'SHIPPED')
           AND oh.ord_stat IN('O', 'I')
      GROUP BY oh.llr_dt, oh.load_num, oh.depart_ts, oh.stop_num, oh.eta_ts, oh.corp_cd, oh.cust_id, oh.po_num,
               b.manctb, b.totctb, ct.outerb, ct.innerb, ct.boxb, ct.pccntb, ct.totcnb
      UNION ALL
      SELECT   oh.llr_dt, oh.load_num, oh.depart_ts, oh.stop_num, oh.eta_ts, oh.corp_cd, oh.cust_id,
               b.manctb AS mfst_catg, b.totctb AS tote_catg, ct.outerb AS outer_cube, ct.innerb AS inner_cube,
               SUM(CASE
                     WHEN b.totctb IS NOT NULL THEN 0
                     WHEN NOT EXISTS(SELECT 1
                                       FROM kit_item_mstr_kt1m k
                                      WHERE k.div_part = i_div_part
                                        AND k.comp_item_num = e.catite) THEN b.pckqtb
                     ELSE b.pckqtb
                          / (SELECT MAX(k.comp_qty)
                               FROM kit_item_mstr_kt1m k
                              WHERE k.div_part = i_div_part
                                AND k.comp_item_num = e.catite
                                AND k.comp_item_num = (SELECT MAX(k2.comp_item_num)
                                                         FROM kit_item_mstr_kt1m k2
                                                        WHERE k2.div_part = k.div_part
                                                          AND k2.kit_typ = k.kit_typ
                                                          AND k2.item_num = k.item_num))
                   END
                  ) AS case_cnt,
               (SELECT SUM(DECODE(ct.boxb, 'Y', mc.boxsmc, mc.totsmc))
                  FROM mclp370c mc
                 WHERE mc.div_part = i_div_part
                   AND mc.llr_date = oh.llr_dt - DATE '1900-02-28'
                   AND mc.loadc = oh.load_num
                   AND mc.stopc = oh.stop_num
                   AND mc.custc = oh.cust_id
                   AND mc.manctc = b.manctb
                   AND mc.totctc = b.totctb) AS tote_cnt,
               SUM(DECODE(b.totctb, NULL, 0, b.pckqtb)) AS units_in_totes,
               SUM(NVL(e.cubee, .01) * b.pckqtb) AS prod_cube,
               SUM(NVL(e.wghte, .01) * b.pckqtb) AS prod_wt,
               SUM(DECODE(oh.ord_typ, 'D',(NVL(e.cubee, .01) * b.pckqtb))) AS dist_cube,
               SUM(DECODE(oh.ord_typ, 'D',(NVL(e.wghte, .01) * b.pckqtb))) AS dist_wt
          FROM ord_hdr oh, ordp120b b, sawp505e e, mclp200b ct
         WHERE oh.ord_stat IN('P', 'R', 'A')
           AND b.div_part = i_div_part
           AND b.ordnob = oh.ord_num
           AND b.excptn_sw = 'N'
           AND b.statb IN('R', 'A')
           AND (   l_load_stat = 'ALL'
                OR b.statb = DECODE(l_load_stat,
                                    'SHIPPED', 'A',
                                    'BILLED', 'R',
                                    'PARTIAL', 'R',
                                    'PARTIAL W/OPEN', 'R',
                                    'BILLED W/OPEN', 'R'
                                   )
               )
           AND b.subrcb < 999
           AND b.pckqtb > 0
           AND e.iteme = b.itemnb
           AND e.uome = b.sllumb
           AND ct.div_part(+) = b.div_part
           AND ct.totctb(+) = b.totctb
      GROUP BY oh.llr_dt, oh.load_num, oh.depart_ts, oh.stop_num, oh.eta_ts, oh.corp_cd, oh.cust_id,
               b.manctb, b.totctb, ct.outerb, ct.innerb, ct.boxb, ct.pccntb, ct.totcnb
    )
    SELECT o.llr_dt, o.load_num, o.depart_ts, o.stop_num, o.eta_ts, o.corp_cd, o.cust_id, o.mfst_catg, o.tote_catg,
           o.outer_cube, o.inner_cube, o.case_cnt, o.tote_cnt, o.units_in_totes, o.prod_cube, o.prod_wt, o.dist_cube, o.dist_wt
      BULK COLLECT INTO l_t_ord_dtl
      FROM o;

    l_idx := l_t_ord_dtl.FIRST;
    WHILE l_idx IS NOT NULL LOOP
      PIPE ROW(l_t_ord_dtl(l_idx));
      l_idx := l_t_ord_dtl.NEXT(l_idx);
    END LOOP;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN;
  EXCEPTION
    WHEN no_data_needed THEN
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_dtl_fn;

  /*
  ||----------------------------------------------------------------------------
  || STATUS_LIST_FN
  ||  Build a cursor of load status selections.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/04/06 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION status_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.STATUS_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_t_load_stats       type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    l_t_load_stats := type_stab('All', 'Open', 'Partial', 'Partial w/Open', 'Billed', 'Billed w/Open', 'Shipped');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT t.column_value
         FROM TABLE(CAST(l_t_load_stats AS type_stab)) t;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END status_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FN
  ||  Return a cursor of available loads for given LLR Date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/09/18 | rhalpai | Original. PIR18335
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_load_stat          VARCHAR2(30);
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_load_stat := UPPER(NVL(i_load_stat, 'ALL'));
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   l.loadc, l.destc
           FROM mclp120c l
          WHERE l.div_part = l_div_part
            AND l.loadc NOT IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                WHERE t.column_value NOT BETWEEN 'P00P' AND 'P99P')
            AND (   l_load_stat = 'ALL'
                 OR l_load_stat = UPPER((SELECT load_stat_udf(l_div_part, l_llr_dt, l.loadc)
                                           FROM DUAL)))
            AND EXISTS(SELECT 1
                         FROM load_depart_op1f ld, ordp100a a
                        WHERE ld.div_part = l_div_part
                          AND ld.llr_dt = l_llr_dt
                          AND ld.load_num = l.loadc
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.div_part = l_div_part
                          AND a.excptn_sw = 'N'
                          AND a.stata IN('O', 'I', 'P', 'R', 'A')
                          AND (   l_load_stat IN('ALL', 'PARTIAL', 'PARTIAL W/OPEN', 'BILLED W/OPEN')
                               OR (    l_load_stat = 'SHIPPED'
                                   AND a.stata = 'A')
                               OR (    l_load_stat = 'BILLED'
                                   AND a.stata = 'R')
                               OR (    l_load_stat = 'OPEN'
                                   AND a.stata IN('O', 'I'))
                              )
                          AND a.dsorda IN('R', 'D')
                          AND a.ipdtsa NOT IN(SELECT s.ord_src
                                                FROM sub_prcs_ord_src s
                                               WHERE s.div_part = l_div_part
                                                 AND s.prcs_id = 'LOAD BALANCE'
                                                 AND s.prcs_sbtyp_cd IN('BLB', 'NAO')))
       ORDER BY l.loadc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_DETAILS_FN
  ||  Return cursor of loads for LLR with balance info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/04/06 | rhalpai | Original. PIR3593
  || 05/05/08 | rhalpai | Corrected the tote/cube calculations to be summed and
  ||                    | rounded by stop. IM404756
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 12/06/12 | rhalpai | Change cursor to calculate ToteCnt using BoxSwitch,
  ||                    | BOXB, instead of PieceCountSwitch, PCCNTB. PIR12038
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 01/13/16 | rhalpai | Change logic to use new div_pk.div_part_fn and cursor
  ||                    | to use scalar subquery caching for function call
  ||                    | within sql. PIR15617
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers. PIR14660
  || 02/09/18 | rhalpai | Add LoadList parm and change logic to restrict to
  ||                    | those loads. PIR18335
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.LOAD_DETAILS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_load_stat          VARCHAR2(30);
    l_t_xloads           type_stab;
    l_t_loads            type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_load_stat := UPPER(NVL(i_load_stat, 'ALL'));
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    IF i_load_list IS NOT NULL THEN
      l_t_loads := str.parse_list(i_load_list, op_const_pk.field_delimiter);
    END IF;

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
      WITH oh AS(
        SELECT *
          FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, l_load_stat))
      ), load_stat AS(
        SELECT l.loadc, l.destc, l.accubc, l.acwgtc,
                (SELECT load_stat_udf(l_div_part, l_llr_dt, l.loadc)
                   FROM DUAL) AS status
              FROM mclp120c l
             WHERE l.div_part = l_div_part
               AND (   i_load_list IS NULL
                    OR l.loadc IN(SELECT t.column_value
                                    FROM TABLE(l_t_loads) t))
               AND l.loadc NOT IN(SELECT t.column_value
                                    FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                   WHERE t.column_value NOT BETWEEN 'P00P' AND 'P99P')
               AND (   l_load_stat = 'ALL'
                    OR l_load_stat =
                         UPPER((SELECT load_stat_udf(l_div_part, l_llr_dt, l.loadc)
                                  FROM DUAL)
                              )
                   )
               AND EXISTS(SELECT 1
                        FROM oh))
      SELECT   o.load_num, ls.destc AS destination,
               TO_CHAR(NVL(ROUND(SUM(DECODE(o.tote_cnt,
                                            NULL, o.prod_cube,
                                            o.tote_cnt * o.outer_cube
                                           )
                                    ),
                                 1
                                ),
                           0
                          ),
                       'FM999999990.0'
                      ) AS ttl_cube,
               NVL(CEIL(SUM(DECODE(o.tote_cnt,
                                   NULL, o.prod_cube,
                                   o.tote_cnt * o.outer_cube
                                  )
                           )
                        / DECODE(ls.accubc, 0, NULL, ls.accubc)
                        * 100
                       ),
                   0
                  ) AS cube_pct_full,
               TO_CHAR(NVL(ROUND(SUM(o.prod_cube), 1), 0), 'FM999999990.0') AS prod_cube,
               TO_CHAR(NVL(ROUND(SUM(o.prod_wt), 1), 0), 'FM999999990.0') AS prod_wt,
               NVL(CEIL(SUM(o.prod_wt) / DECODE(ls.acwgtc, 0, NULL, ls.acwgtc) * 100), 0) AS wt_pct_full,
               COUNT(DISTINCT(o.stop_num)) AS stop_cnt,
               TO_CHAR(NVL(ROUND(SUM(o.dist_cube), 1), 0), 'FM999999990.0') AS dist_cube,
               TO_CHAR(NVL(ROUND(SUM(o.dist_wt), 1), 0), 'FM999999990.0') AS dist_wt,
               ls.status, NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
               TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.outer_cube), 1), 0), 'FM999999990.0') AS tote_cube
          FROM (SELECT *
                  FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                               l_div_part,
                               CURSOR(
                                 SELECT *
                                   FROM oh
                               ),
                               l_load_stat
                              )
                            ) od
--                 WHERE od.prod_cube > 0
               ) o, load_stat ls
         WHERE ls.loadc = o.load_num
      GROUP BY o.load_num, ls.destc, ls.accubc, ls.acwgtc, ls.status
      ORDER BY o.load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_details_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_DETAILS_FN
  ||  Return cursor of stops for LLR/Load with balance info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/04/06 | rhalpai | Original. PIR3593
  || 05/05/08 | rhalpai | Corrected the tote/cube calculations to be summed and
  ||                    | rounded by stop. IM404756
  || 12/08/08 | rhalpai | Added ship-to address to cursor. IM465107
  || 12/06/12 | rhalpai | Change cursor to calculate ToteCnt using BoxSwitch,
  ||                    | BOXB, instead of PieceCountSwitch, PCCNTB. PIR12038
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 01/13/16 | rhalpai | Change logic to use new div_pk.div_part_fn and cursor
  ||                    | to use scalar subquery caching for function call
  ||                    | within sql. PIR15617
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers. PIR14660
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load       IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.STOP_DETAILS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_load_stat          VARCHAR2(30);
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'Load', i_load);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_load_stat := NVL(UPPER(i_load_stat), 'ALL');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   (CASE
                   WHEN o.stop_num < 10 THEN '0'
                 END) || o.stop_num AS stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm, c.shpctc AS city, c.shpstc AS st,
                c.shpzpc AS zip, op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone,
                TO_CHAR(o.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts, NVL(SUM(o.case_cnt), 0) AS cases,
                NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
                TO_CHAR(NVL(ROUND(SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube)), 1), 0),
                        'FM999999990.0'
                       ) AS ttl_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_cube), 1), 0), 'FM999999990.0') AS prod_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_wt), 1), 0), 'FM999999990.0') AS prod_wt,
                NVL(ROUND(100
                          -(SUM(DECODE(o.tote_cnt, NULL, NULL, o.prod_cube))
                            / SUM(DECODE(o.tote_cnt * o.inner_cube, 0, NULL, o.tote_cnt * o.inner_cube))
                            * 100
                           )
                         ),
                    0
                   ) AS tote_air_pct,
                TO_CHAR(NVL(ROUND(SUM(o.dist_cube), 1), 0), 'FM999999990.0') AS dist_cube,
                TO_CHAR(NVL(ROUND(SUM(o.dist_wt), 1), 0), 'FM999999990.0') AS dist_wt, o.cust_id,
                (SELECT load_stat_udf(l_div_part, l_llr_dt, i_load, o.stop_num)
                   FROM DUAL) AS stat,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.outer_cube), 1), 0), 'FM999999990.0') AS tote_cube,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.inner_cube), 1), 0), 'FM999999990.0') AS tote_inner_cube,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.inner_cube - o.prod_cube), 1), 0), 'FM999999990.0') AS tote_air_cube,
                c.shad1c AS addr_ln1, c.shad2c AS addr_ln2
           FROM (SELECT *
                   FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                                l_div_part,
                                CURSOR(
                                   SELECT *
                                     FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, l_load_stat, i_load)) oh
                                 ),
                                 l_load_stat
                               )
                             ) od
                ) o, sysp200c c, mclp020b cx
          WHERE c.div_part = l_div_part
            AND c.acnoc = o.cust_id
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
       GROUP BY o.stop_num, cx.mccusb, o.cust_id, c.namec, c.shpctc, c.shpstc, c.shpzpc, c.cnphnc, c.shad1c, c.shad2c, o.eta_ts
       ORDER BY o.stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_details_fn;

  /*
  ||----------------------------------------------------------------------------
  || MANIFEST_DETAILS_FN
  ||  Return cursor of manifest categories with balance info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/04/06 | rhalpai | Original. PIR3593
  || 05/05/08 | rhalpai | Corrected the tote/cube calculations to be summed and
  ||                    | rounded by stop. IM404756
  || 06/06/08 | rhalpai | Changed cursor to include box counts in the tote count
  ||                    | column. IM417717
  || 12/06/12 | rhalpai | Change cursor to calculate ToteCnt using BoxSwitch,
  ||                    | BOXB, instead of PieceCountSwitch, PCCNTB. PIR12038
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 05/15/14 | rhalpai | Add pallet_sw to cursor to indicate category is for
  ||                    | pallet items. PIR12503
  || 01/13/16 | rhalpai | Change logic to use new div_pk.div_part_fn and cursor
  ||                    | to use scalar subquery caching for function call
  ||                    | within sql. PIR15617
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers. PIR14660
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION manifest_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load       IN  VARCHAR2 DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop       IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.MANIFEST_DETAILS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_load_stat          VARCHAR2(30);
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'Load', i_load);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.add_parm(lar_parm, 'Stop', i_stop);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_load_stat := NVL(UPPER(i_load_stat), 'ALL');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       WITH oh AS(
         SELECT h.*
           FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, l_load_stat, i_load, i_stop)) h,
                load_depart_op1f ld
          WHERE ld.div_part = l_div_part
            AND ld.load_depart_sid = h.load_depart_sid
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                    WHERE t.column_value NOT BETWEEN 'P00P' AND 'P99P')
       ), mfst AS(
         SELECT   ld.llr_dt, ld.load_num, oh.cust_id, b.manctb AS mfst_catg,
                  MAX(DECODE(e.sizee, 'PALLET  ', 'Y', 'N')) AS pallet_sw
             FROM oh, load_depart_op1f ld, ordp120b b, sawp505e e
            WHERE ld.div_part = l_div_part
              AND ld.load_depart_sid = oh.load_depart_sid
              AND b.div_part = l_div_part
              AND b.ordnob = oh.ord_num
              AND b.excptn_sw = 'N'
              AND b.subrcb < 999
              AND (   (    b.statb IN('O', 'I')
                       AND l_load_stat NOT IN('BILLED', 'SHIPPED')
                       AND b.ordqtb > 0
                       AND NOT EXISTS(SELECT 1
                                        FROM whsp300c w
                                       WHERE w.div_part = b.div_part
                                         AND w.itemc = b.itemnb
                                         AND w.uomc = b.sllumb
                                         AND w.taxjrc IS NULL
                                         AND w.qavc = 0))
                   OR (    b.statb IN('R', 'A')
                       AND l_load_stat  <> 'OPEN'
                AND (   l_load_stat = 'ALL'
                            OR b.statb = DECODE(l_load_stat,
                                                'SHIPPED', 'A',
                                                'BILLED', 'R',
                                                'PARTIAL', 'R',
                                                'PARTIAL W/OPEN', 'R',
                                                'BILLED W/OPEN', 'R'
                                               )
                           )
                       AND b.pckqtb > 0)
                  )
              AND e.iteme = b.itemnb
              AND e.uome = b.sllumb
         GROUP BY ld.llr_dt, ld.load_num, oh.cust_id, b.manctb
       ), o AS(
         SELECT /*+ NO_QUERY_TRANSFORMATION */
                od.mfst_catg, od.outer_cube, od.inner_cube, od.case_cnt, od.tote_cnt, od.units_in_totes, od.prod_cube,
                od.prod_wt, od.dist_cube, od.dist_wt, mfst.pallet_sw
           FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                        l_div_part,
                        CURSOR(
                          SELECT *
                            FROM oh
                        ),
                        l_load_stat
                       )
                     ) od,
                mfst
          WHERE od.prod_cube > 0
            AND mfst.llr_dt = od.llr_dt
            AND mfst.load_num = od.load_num
            AND mfst.cust_id = od.cust_id
            AND mfst.mfst_catg = od.mfst_catg
       )
       SELECT   o.mfst_catg, m.descc AS mfst_descr, NVL(SUM(o.case_cnt), 0) AS cases,
                NVL(SUM(o.units_in_totes), 0) AS units_in_totes,
                TO_CHAR(NVL(ROUND(SUM(DECODE(o.tote_cnt, 0, 0, o.tote_cnt * o.inner_cube - o.prod_cube)), 1), 0),
                        'FM999999990.0'
                       ) AS tote_air_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_cube), 1), 0), 'FM999999990.0') AS prod_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_wt), 1), 0), 'FM999999990.0') AS prod_wt,
                TO_CHAR(NVL(ROUND(SUM(o.dist_cube), 1), 0), 'FM999999990.0') AS dist_cube,
                TO_CHAR(NVL(ROUND(SUM(o.dist_wt), 1), 0), 'FM999999990.0') AS dist_wt,
                NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.outer_cube), 1), 0), 'FM999999990.0') AS tote_cube,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.inner_cube), 1), 0), 'FM999999990.0') AS tote_inner_cube,
                NVL(ROUND(100
                          -(SUM(DECODE(o.tote_cnt, NULL, NULL, o.prod_cube))
                            / SUM(DECODE(o.tote_cnt * o.inner_cube, 0, NULL, o.tote_cnt * o.inner_cube))
                            * 100
                           )
                         ),
                    0
                   ) AS tote_air_pct,
                o.pallet_sw
           FROM o, mclp210c m
          WHERE m.div_part = l_div_part
            AND m.manctc = o.mfst_catg
       GROUP BY o.mfst_catg, m.descc, o.pallet_sw
       ORDER BY o.mfst_catg;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END manifest_details_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_DETAILS_FN
  ||  Return cursor of Item details for manifest category.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/15/14 | rhalpai | Original. PIR12503
  || 01/13/16 | rhalpai | Change logic to use new div_pk.div_part_fn and cursor
  ||                    | to use scalar subquery caching for function call
  ||                    | within sql. PIR15617
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr process. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_mfst_catg  IN  VARCHAR2,
    i_load       IN  VARCHAR2 DEFAULT NULL,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop       IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.ITEM_DETAILS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_load_stat          VARCHAR2(30);
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'MfstCatg', i_mfst_catg);
    logs.add_parm(lar_parm, 'Load', i_load);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.add_parm(lar_parm, 'Stop', i_stop);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_load_stat := NVL(UPPER(i_load_stat), 'ALL');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   e.catite AS catlg_num, e.ctdsce AS item_descr,
                          (SELECT w.aislc || w.binc || w.levlc
                             FROM whsp300c w
                            WHERE w.div_part = l_div_part
                              AND w.itemc = e.iteme
                              AND w.uomc = e.uome
                              AND w.uomc NOT IN('CII', 'CIC', 'CIR')
                              AND ROWNUM = 1) AS slot,
                e.upce AS case_upc,
                SUM(DECODE(b.statb, 'O', b.ordqtb, 'I', b.ordqtb, b.pckqtb)) AS qty
           FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, l_load_stat, i_load, i_stop)) oh,
                load_depart_op1f ld, ordp120b b, sawp505e e
                    WHERE ld.div_part = l_div_part
            AND ld.load_depart_sid = oh.load_depart_sid
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                    WHERE t.column_value NOT BETWEEN 'P00P' AND 'P99P')
            AND b.div_part = l_div_part
            AND b.ordnob = oh.ord_num
                      AND b.manctb = i_mfst_catg
                      AND b.excptn_sw = 'N'
                      AND b.subrcb < 999
            AND (   (    b.statb IN('O', 'I')
                     AND l_load_stat NOT IN('BILLED', 'SHIPPED')
                      AND b.ordqtb > 0
                      AND NOT EXISTS(SELECT 1
                                       FROM whsp300c w
                                      WHERE w.div_part = b.div_part
                                        AND w.itemc = b.itemnb
                                        AND w.uomc = b.sllumb
                                        AND w.taxjrc IS NULL
                                       AND w.qavc = 0))
                 OR (    b.statb IN('R', 'A')
                     AND l_load_stat  <> 'OPEN'
                     AND (   l_load_stat  = 'ALL'
                          OR b.statb = DECODE(l_load_stat ,
                                              'SHIPPED', 'A',
                                              'BILLED', 'R',
                                              'PARTIAL', 'R',
                                              'PARTIAL W/OPEN', 'R',
                                              'BILLED W/OPEN', 'R'
                                             )
                         )
                     AND b.pckqtb > 0)
                )
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb
                 GROUP BY e.catite, e.iteme, e.uome, e.ctdsce, e.upce
       ORDER BY slot, e.catite;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_details_fn;

  /*
  ||----------------------------------------------------------------------------
  || CUBE_WGT_BY_STOP_CUR_FN
  ||  Return cursor of Cube and Weight info for Div/LLR by Load/Stop.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/08 | rhalpai | Original. PIR7045
  || 12/06/12 | rhalpai | Change cursor to calculate ToteCnt using BoxSwitch,
  ||                    | BOXB, instead of PieceCountSwitch, PCCNTB. PIR12038
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 11/05/14 | rhalpai | Add logic to exclude ADC/ADK orders without an
  ||                    | available Reg Order. PIR12893
  || 01/13/16 | rhalpai | Change logic to use new div_pk.div_part_fn and cursor
  ||                    | to use scalar subquery caching for function call
  ||                    | within sql. PIR15617
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers. PIR14660
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||----------------------------------------------------------------------------
  */
  FUNCTION cube_wgt_by_stop_cur_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_BALANCE_PK.CUBE_WGT_BY_STOP_CUR_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   o.load_num, RTRIM(l.destc) AS dest, (CASE
                                                       WHEN o.stop_num < 10 THEN '0'
                                                     END) || o.stop_num AS stop_num,
                (SELECT load_stat_udf(l_div_part, l_llr_dt, o.load_num, o.stop_num)
                   FROM DUAL) AS stp_stat, TO_CHAR(o.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts, o.cust_id,
                cx.mccusb AS mcl_cust, c.namec AS cust_nm,
                DECODE(c.shad2c, NULL, c.shad1c, c.shad1c || ' ' || c.shad2c) AS addr, c.shpctc AS city,
                c.shpstc AS st,
                TO_CHAR(NVL(ROUND(SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube)), 1), 0),
                        'FM999999990.0'
                       ) AS ttl_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_cube), 1), 0), 'FM999999990.0') AS prod_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_wt), 1), 0), 'FM999999990.0') AS prod_wt,
                TO_CHAR(NVL(ROUND(SUM(o.dist_cube), 1), 0), 'FM999999990.0') AS dist_cube,
                TO_CHAR(NVL(ROUND(SUM(o.dist_wt), 1), 0), 'FM999999990.0') AS dist_wt, c.shpzpc AS zip,
                c.cnphnc AS phone
           FROM (SELECT *
                   FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                                l_div_part,
                                CURSOR(
                                  SELECT *
                                    FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt)) oh
                                )
                               )
                             ) od
                ) o, sysp200c c, mclp020b cx, mclp120c l
          WHERE c.div_part = l_div_part
            AND c.acnoc = o.cust_id
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
            AND l.div_part = l_div_part
            AND l.loadc = o.load_num
            AND l.loadc NOT IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                WHERE t.column_value NOT BETWEEN 'P00P' AND 'P99P')
       GROUP BY o.load_num, l.destc, o.stop_num, cx.mccusb, o.cust_id, c.namec, c.shad1c, c.shad2c, c.shpctc, c.shpstc,
                c.shpzpc, c.cnphnc, o.eta_ts
       ORDER BY o.load_num, o.stop_num DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cube_wgt_by_stop_cur_fn;
END op_load_balance_pk;
/

