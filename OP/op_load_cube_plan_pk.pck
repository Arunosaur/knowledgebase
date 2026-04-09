CREATE OR REPLACE PACKAGE op_load_cube_plan_pk IS
  TYPE g_rt_load_dtl IS RECORD(
    load_num       mclp120c.loadc%TYPE,
    destination    mclp120c.destc%TYPE,
    ttl_cube       VARCHAR2(11),
    cube_pct_full  NUMBER,
    prod_cube      VARCHAR2(11),
    prod_wgt       VARCHAR2(11),
    wgt_pct_full   NUMBER,
    stop_cnt       NUMBER,
    dist_cube      VARCHAR2(11),
    dist_wgt       VARCHAR2(11),
    status         VARCHAR2(30),
    tote_cnt       NUMBER,
    tote_cube      VARCHAR2(11),
    sel_sw         VARCHAR2(1),
    last_extr_ts   DATE,
    extr_user_id   VARCHAR2(30)
  );

  TYPE g_tt_load_dtls IS TABLE OF g_rt_load_dtl;

  TYPE g_rt_cntnr_ord IS RECORD(
    ord_num         VARCHAR2(11),
    ord_ln          VARCHAR2(7),
    load_num        VARCHAR2(4),
    stop_num        VARCHAR2(2),
    cust_id         VARCHAR2(8),
    corp_cd         VARCHAR2(3),
    grp_cd          VARCHAR2(3),
    cntnr_trckg_sw  VARCHAR2(1),
    po_num          VARCHAR2(30),
    cntnr_id        VARCHAR2(20),
    tote_grp_id     VARCHAR2(12),
    pick_slot       VARCHAR2(7),
    catlg_num       VARCHAR2(6),
    item_wght       VARCHAR2(11),
    item_cube       VARCHAR2(11),
    ord_qty         NUMBER
  );

  TYPE g_tt_cntnr_ords IS TABLE OF g_rt_cntnr_ord;

  FUNCTION load_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN g_tt_load_dtls PIPELINED;

  FUNCTION kit_cntnrs_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2,
    i_ts         IN  DATE
  ) RETURN g_tt_cntnr_ords PIPELINED;

  FUNCTION plan_ord_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2,
    i_ts         IN  DATE
  )
    RETURN g_tt_cntnr_ords PIPELINED;

  PROCEDURE get_load_details_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    o_extr_max   OUT     NUMBER,
    o_cur        OUT     SYS_REFCURSOR,
    i_load_stat  IN      VARCHAR2 DEFAULT 'ALL'
  );

  PROCEDURE plan_sp(
    i_div          IN  VARCHAR2,
    i_llr_dt       IN  VARCHAR2,
    i_load_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );
END op_load_cube_plan_pk;
/

CREATE OR REPLACE PACKAGE BODY op_load_cube_plan_pk IS
  TYPE g_rt_plan_ord IS RECORD(
    grp             VARCHAR2(40),
    typ             VARCHAR2(3),
    pick_slot       VARCHAR2(7),
    pick_zone       VARCHAR2(3),
    ord_num         NUMBER,
    ord_ln          NUMBER,
    catlg_num       VARCHAR2(6),
    cbr_item        VARCHAR2(9),
    uom             VARCHAR2(3),
    ord_qty         PLS_INTEGER,
    item_cube       NUMBER,
    item_wght       NUMBER,
    inner_cube      NUMBER,
    hc_qty          PLS_INTEGER,
    box_qty         PLS_INTEGER,
    mc_qty          PLS_INTEGER,
    cube_by_hc      VARCHAR2(1),
    load_num        VARCHAR2(4),
    stop_num        NUMBER,
    cust_id         VARCHAR2(8),
    corp_cd         VARCHAR2(3),
    grp_cd          VARCHAR2(3),
    cntnr_trckg_sw  VARCHAR2(1),
    po_num          VARCHAR2(30),
    tote_grp_id     VARCHAR2(12)
  );

  TYPE g_cvt_plan_ord IS REF CURSOR RETURN g_rt_plan_ord;

  TYPE g_rt_precube_ord IS RECORD(
    ord_num         NUMBER,
    ord_ln          NUMBER,
    load_num        VARCHAR2(4),
    stop_num        NUMBER,
    cust_id         VARCHAR2(8),
    corp_cd         VARCHAR2(3),
    grp_cd          VARCHAR2(3),
    cntnr_trckg_sw  VARCHAR2(1),
    po_num          VARCHAR2(30),
    tote_grp_id     VARCHAR2(12),
    pick_slot       VARCHAR2(7),
    catlg_num       VARCHAR2(6),
    item_wght       NUMBER,
    item_cube       NUMBER
  );

  /*
  ||----------------------------------------------------------------------------
  || LOAD_DETAILS_FN
  ||  Return cursor for Load Details.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | PIR15617
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_details_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_stat  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN g_tt_load_dtls PIPELINED IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_LOAD_CUBE_PLAN_PK.LOAD_DETAILS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;

    TYPE l_rt_load_dtl IS RECORD(
      load_num       mclp120c.loadc%TYPE,
      destination    mclp120c.destc%TYPE,
      ttl_cube       VARCHAR2(11),
      cube_pct_full  NUMBER,
      prod_cube      VARCHAR2(11),
      prod_wgt       VARCHAR2(11),
      wgt_pct_full   NUMBER,
      stop_cnt       NUMBER,
      dist_cube      VARCHAR2(11),
      dist_wgt       VARCHAR2(11),
      status         VARCHAR2(30),
      tote_cnt       NUMBER,
      tote_cube      VARCHAR2(11)
    );

    TYPE l_tt_load_dtls IS TABLE OF l_rt_load_dtl;

    l_t_load_dtls        l_tt_load_dtls;
    l_r_load_dtl         g_rt_load_dtl;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Get Load Details Cursor');
    l_cv := op_load_balance_pk.load_details_fn(i_div, i_llr_dt, i_load_stat);
    logs.dbg('Fetch Load Details');

    FETCH l_cv
    BULK COLLECT INTO l_t_load_dtls;

    IF l_t_load_dtls.COUNT > 0 THEN
      logs.dbg('Process Load Details');
      FOR i IN l_t_load_dtls.FIRST .. l_t_load_dtls.LAST LOOP
        l_r_load_dtl.load_num := l_t_load_dtls(i).load_num;
        l_r_load_dtl.destination := l_t_load_dtls(i).destination;
        l_r_load_dtl.ttl_cube := l_t_load_dtls(i).ttl_cube;
        l_r_load_dtl.cube_pct_full := l_t_load_dtls(i).cube_pct_full;
        l_r_load_dtl.prod_cube := l_t_load_dtls(i).prod_cube;
        l_r_load_dtl.prod_wgt := l_t_load_dtls(i).prod_wgt;
        l_r_load_dtl.wgt_pct_full := l_t_load_dtls(i).wgt_pct_full;
        l_r_load_dtl.stop_cnt := l_t_load_dtls(i).stop_cnt;
        l_r_load_dtl.dist_cube := l_t_load_dtls(i).dist_cube;
        l_r_load_dtl.dist_wgt := l_t_load_dtls(i).dist_wgt;
        l_r_load_dtl.status := l_t_load_dtls(i).status;
        l_r_load_dtl.tote_cnt := l_t_load_dtls(i).tote_cnt;
        l_r_load_dtl.tote_cube := l_t_load_dtls(i).tote_cube;
        l_r_load_dtl.sel_sw :=(CASE l_t_load_dtls(i).status
                                 WHEN 'Open' THEN 'Y'
                                 ELSE 'N'
                               END);

        SELECT MAX(e.last_extr_ts), MAX(e.user_id)
          INTO l_r_load_dtl.last_extr_ts, l_r_load_dtl.extr_user_id
          FROM load_cube_plan_extr_op1e e
         WHERE e.div_part = l_div_part
           AND e.llr_dt = l_llr_dt
           AND e.load_num = l_t_load_dtls(i).load_num;

        PIPE ROW(l_r_load_dtl);
      END LOOP;
    END IF;   -- l_t_load_dtls.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN;
  EXCEPTION
    WHEN no_data_needed THEN
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_details_fn;

  /*
  ||----------------------------------------------------------------------------
  || PLAN_ORD_CUR_FN
  ||  Return cursor for Load Pre-Cube Planning process.
  ||
  ||   i_load_list : comma-delimited list of loads. i.e.: 0101,0102,0103
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | Change to call new OP_PARMS_PK.VAL_FN,
  ||                    | OP_PARMS_PK.GET_PARMS_FOR_PRFX_SP,
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN. PIR15617
  ||----------------------------------------------------------------------------
  */
  FUNCTION plan_ord_cur_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2,
    i_ts         IN  DATE
  )
    RETURN g_cvt_plan_ord IS
    l_c_module       CONSTANT typ.t_maxfqnm := 'OP_LOAD_CUBE_PLAN_PK.PLAN_ORD_CUR_FN';
    lar_parm                  logs.tar_parm;
    l_cube_all_by_hc_sw       VARCHAR2(1);
    l_t_po_lvl_crps           type_stab;
    l_t_po_lvl_vals           type_stab;
    l_t_sngl_po_crps          type_stab;
    l_t_sngl_po_vals          type_stab;
    l_t_mstr_cs_crps          type_stab;
    l_t_mstr_cs_vals          type_stab;
    l_t_nacs_tobacco_catgs    type_stab;
    l_cv                      SYS_REFCURSOR;
    l_c_ts           CONSTANT VARCHAR2(9)   := TO_CHAR(i_ts, 'YMMDDHH24MI');
    l_c_dflt_cig_po  CONSTANT VARCHAR2(10)  := 'C' || l_c_ts;
    l_c_dflt_tob_po  CONSTANT VARCHAR2(10)  := 'T' || l_c_ts;
    l_c_dflt_gro_po  CONSTANT VARCHAR2(10)  := 'G' || l_c_ts;
    l_c_dflt_oth_po  CONSTANT VARCHAR2(10)  := 'O' || l_c_ts;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'Ts', i_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_cube_all_by_hc_sw := NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_cube_all_by_hc), 'N');
    op_parms_pk.get_parms_for_prfx_sp(i_div_part, op_const_pk.prm_cntnr_trk_po_lvl, l_t_po_lvl_crps, l_t_po_lvl_vals,
                                      3);
    op_parms_pk.get_parms_for_prfx_sp(i_div_part,
                                      op_const_pk.prm_po_ovride_snglpo,
                                      l_t_sngl_po_crps,
                                      l_t_sngl_po_vals,
                                      3
                                     );
    op_parms_pk.get_parms_for_prfx_sp(i_div_part,
                                      op_const_pk.prm_alloc_mstr_cs,
                                      l_t_mstr_cs_crps,
                                      l_t_mstr_cs_vals,
                                      3
                                     );
    l_t_nacs_tobacco_catgs := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_nacs_tobacco);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
      WITH cust AS
           (
             SELECT ld.load_depart_sid, ld.load_num, se.stop_num, se.cust_id, LPAD(cx.corpb, 3, '0') AS corp_cd,
                    NVL(SUBSTR(c.retgpc, 3), '000') AS grp_cd, NVL(g.cntnr_trckg_sw, 'N') AS cntnr_trckg_sw,
                    NVL(g.cube_by_hc_sw, 'N') AS cube_by_hc_sw,
                    (SELECT v.parm_val
                       FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd, ROWNUM AS seq
                               FROM TABLE(CAST(l_t_po_lvl_crps AS type_stab)) t) i,
                            (SELECT t.column_value AS parm_val, ROWNUM AS seq
                               FROM TABLE(CAST(l_t_po_lvl_vals AS type_stab)) t) v
                      WHERE i.seq = v.seq
                        AND cx.corpb = i.crp_cd) AS cntnr_trk_po_lvl_cd,
                    (SELECT v.parm_val
                       FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd, ROWNUM AS seq
                               FROM TABLE(CAST(l_t_sngl_po_crps AS type_stab)) t) i,
                            (SELECT t.column_value AS parm_val, ROWNUM AS seq
                               FROM TABLE(CAST(l_t_sngl_po_vals AS type_stab)) t) v
                      WHERE i.seq = v.seq
                        AND cx.corpb = i.crp_cd) AS po_ovride_snglpo_sw,
                    (SELECT v.parm_val
                       FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd, ROWNUM AS seq
                               FROM TABLE(CAST(l_t_mstr_cs_crps AS type_stab)) t) i,
                            (SELECT t.column_value AS parm_val, ROWNUM AS seq
                               FROM TABLE(CAST(l_t_mstr_cs_vals AS type_stab)) t) v
                      WHERE i.seq = v.seq
                        AND cx.corpb = i.crp_cd) AS mstr_cs_cust_sw,
                    ct.taxjrc AS cust_jrsdctn
               FROM load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx, mclp100a g, mclp030c ct
              WHERE ld.div_part = i_div_part
                AND ld.llr_dt = i_llr_dt
                AND INSTR(',' || i_load_list || ',', ',' || ld.load_num || ',') > 0
                AND se.div_part = ld.div_part
                AND se.load_depart_sid = ld.load_depart_sid
                AND EXISTS(
                      SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND NOT EXISTS(SELECT 1
                                          FROM sub_prcs_ord_src s
                                         WHERE s.div_part = a.div_part
                                           AND s.ord_src = a.ipdtsa
                                           AND s.prcs_id = 'LOAD BALANCE'
                                           AND s.prcs_sbtyp_cd = 'BLB')
                         AND NOT EXISTS(SELECT 1
                                          FROM sub_prcs_ord_src s
                                         WHERE s.div_part = a.div_part
                                           AND s.ord_src = a.ipdtsa
                                           AND s.prcs_id = 'LOAD BALANCE'
                                           AND s.prcs_sbtyp_cd = 'NAO'
                                           AND NOT EXISTS(SELECT 1
                                                            FROM ordp100a a2
                                                           WHERE a2.div_part = a.div_part
                                                             AND a2.load_depart_sid = a.load_depart_sid
                                                             AND a2.custa = a.custa
                                                             AND a2.ipdtsa NOT IN(
                                                                   SELECT s2.ord_src
                                                                     FROM sub_prcs_ord_src s2
                                                                    WHERE s2.div_part = i_div_part
                                                                      AND s2.prcs_id = 'LOAD BALANCE'
                                                                      AND s2.prcs_sbtyp_cd IN('BLB', 'NAO'))
                                                             AND a2.stata IN('O', 'I', 'P', 'R')
                                                             AND a2.excptn_sw = 'N'
                                                             AND a2.dsorda = 'R'))
                         AND a.excptn_sw = 'N'
                         AND a.dsorda IN('R', 'D')
                         AND a.stata = 'O')
                AND c.div_part = se.div_part
                AND c.acnoc = se.cust_id
                AND cx.div_part = se.div_part
                AND cx.custb = se.cust_id
                AND g.div_part(+) = c.div_part
                AND g.cstgpa(+) = c.retgpc
                AND ct.div_part(+) = se.div_part
                AND ct.custc(+) = se.cust_id),
           nao AS
           (SELECT a.ordnoa AS ord_num
              FROM sub_prcs_ord_src s, cust, ordp100a a
             WHERE s.div_part = i_div_part
               AND s.prcs_id = 'LOAD BALANCE'
               AND s.prcs_sbtyp_cd = 'NAO'
               AND a.div_part = s.div_part
               AND a.load_depart_sid = cust.load_depart_sid
               AND a.custa = cust.cust_id
               AND a.ipdtsa = s.ord_src
               AND a.stata = 'O'
               AND a.excptn_sw = 'N'
               AND a.dsorda IN('R', 'D')
               AND NOT EXISTS(SELECT 1
                                FROM ordp100a a2
                               WHERE a2.div_part = a.div_part
                                 AND a2.load_depart_sid = a.load_depart_sid
                                 AND a2.custa = a.custa
                                 AND a2.ipdtsa NOT IN(SELECT s2.ord_src
                                                        FROM sub_prcs_ord_src s2
                                                       WHERE s2.div_part = i_div_part
                                                         AND s2.prcs_id = 'LOAD BALANCE'
                                                         AND s2.prcs_sbtyp_cd IN('BLB', 'NAO'))
                                 AND a2.stata IN('O', 'I', 'P', 'R')
                                 AND a2.excptn_sw = 'N'
                                 AND a2.dsorda = 'R')),
           z AS
           (SELECT cust.cntnr_trk_po_lvl_cd, cust.load_depart_sid, cust.cust_id, cust.po_ovride_snglpo_sw, a.cpoa AS po_num,
                   b.ordnob AS ord_num, b.lineb AS ord_ln, b.excptn_sw,
                   (CASE
                      WHEN e.bsgrpe = 'CIG' THEN 'CIG'
                      WHEN(    cust.cntnr_trk_po_lvl_cd = 'WLG'
                           AND e.nacse IN(SELECT t.column_value
                                            FROM TABLE(CAST(l_t_nacs_tobacco_catgs AS type_stab)) t)
                          ) THEN 'TOB'
                      WHEN cust.cntnr_trk_po_lvl_cd = 'WLG' THEN 'GRO'
                      ELSE 'OTH'
                    END
                   ) AS typ
              FROM cust, ordp100a a, ordp120b b, sawp505e e
             WHERE cust.cntnr_trk_po_lvl_cd IS NOT NULL
               AND a.div_part = i_div_part
               AND a.load_depart_sid = cust.load_depart_sid
               AND a.custa = cust.cust_id
               AND a.stata = 'O'
               AND a.excptn_sw = 'N'
               AND NOT EXISTS(SELECT 1
                                FROM sub_prcs_ord_src s
                               WHERE s.div_part = a.div_part
                                 AND s.ord_src = a.ipdtsa
                                 AND s.prcs_id = 'LOAD BALANCE'
                                 AND s.prcs_sbtyp_cd = 'BLB')
               AND a.ordnoa NOT IN(SELECT nao.ord_num
                                     FROM nao)
               AND b.div_part = a.div_part
               AND b.ordnob = a.ordnoa
               AND b.statb = 'O'
               AND e.iteme = b.itemnb
               AND e.uome = b.sllumb),
           poo AS
           (
             SELECT o.ord_num, o.ord_ln,
                    COALESCE
                      (
                       -- New Group/PO
                       (SELECT MAX(y.po_num)
                          FROM (SELECT   x.load_depart_sid, x.cust_id, x.typ, x.po_num, COUNT(1) AS cnt,
                                         MAX(COUNT(1))
                                           KEEP (DENSE_RANK LAST ORDER BY COUNT(1))
                                           OVER(PARTITION BY x.load_depart_sid, x.cust_id, x.typ) AS hi
                                    FROM (SELECT z.load_depart_sid, z.cust_id, z.po_num, z.typ
                                            FROM z
                                           WHERE RTRIM(REPLACE(z.po_num, '0')) IS NOT NULL
                                             AND NOT EXISTS(SELECT 1
                                                              FROM mclp300d md
                                                             WHERE md.div_part = i_div_part
                                                               AND md.ordnod = z.ord_num
                                                               AND md.reasnd = 'SPLITORD')) x
                                GROUP BY x.load_depart_sid, x.cust_id, x.typ, x.po_num) y
                         WHERE y.cnt = y.hi
                           AND y.load_depart_sid = o.load_depart_sid
                           AND y.cust_id = o.cust_id
                           AND y.typ = o.typ),
                       -- Defaults
                       DECODE(o.cntnr_trk_po_lvl_cd,
                              'WLG', DECODE(o.typ, 'CIG', l_c_dflt_cig_po, 'TOB', l_c_dflt_tob_po, l_c_dflt_gro_po),
                              DECODE(o.typ, 'CIG', l_c_dflt_cig_po, l_c_dflt_oth_po)
                             )
                      ) AS po_num
               FROM (SELECT z.cntnr_trk_po_lvl_cd, z.ord_num, z.ord_ln, z.load_depart_sid, z.cust_id, z.typ
                       FROM z
                      WHERE z.excptn_sw = 'N'
                        AND (   RTRIM(REPLACE(z.po_num, '0')) IS NULL
                             OR EXISTS(SELECT 1
                                         FROM mclp300d md
                                        WHERE md.div_part = i_div_part
                                          AND md.ordnod = z.ord_num
                                          AND md.reasnd = 'SPLITORD')
                             OR 'Y' = z.po_ovride_snglpo_sw
                            )) o),
           ord AS
           (SELECT cust.load_num
                   || LPAD(cust.stop_num, 2, '0')
                   || cust.cust_id
                   || DECODE(cust.cntnr_trckg_sw, 'Y', DECODE(RTRIM(REPLACE(a.cpoa, '0')), NULL, poo.po_num, a.cpoa), NULL)
                   || LPAD(b.labctb, 3, '0')
                   || b.totctb AS grp,
                   (CASE
                      WHEN cust.mstr_cs_cust_sw = 'Y' THEN 'MCQ'
                      WHEN b.totctb IS NULL THEN 'FC'
                      WHEN b.sllumb IN('CII', 'CIR', 'CIC') THEN 'CIG'
                      ELSE 'OTH'
                    END
                   ) AS typ,
                   w.aislc || w.binc || w.levlc AS pick_slot, w.zonec AS pick_zone, b.ordnob AS ord_num, b.lineb AS ord_ln,
                   e.catite AS catlg_num, b.itemnb AS cbr_item, b.sllumb AS uom, b.ordqtb AS ord_qty, cust.load_num,
                   cust.stop_num, cust.cust_id, cust.corp_cd, cust.grp_cd, cust.cntnr_trckg_sw,
                   NVL(poo.po_num, a.cpoa) AS po_num, LPAD(NVL(b.manctb, '0'), 3, '0') AS mfst_catg,
                   LPAD(NVL(b.totctb, '0'), 3, '0') AS tote_catg, LPAD(NVL(TO_CHAR(b.labctb), '0'), 3, '0') AS labl_catg,
                   e.cubee AS item_cube, e.wghte AS item_wght, t.innerb AS inner_cube,
                   (CASE
                      WHEN(    'Y' IN(l_cube_all_by_hc_sw, cust.cube_by_hc_sw)
                           AND b.ordqtb >= e.fmqtye) THEN e.fmqtye
                      ELSE t.totcnb
                    END
                   ) AS hc_qty,
                   t.totcnb AS box_qty, e.mulsle AS mc_qty,
                   (CASE
                      WHEN 'Y' IN(l_cube_all_by_hc_sw, cust.cube_by_hc_sw) THEN 'Y'
                      ELSE 'N'
                    END) AS cube_by_hc
              FROM cust, ordp100a a, ordp120b b, poo, sawp505e e, mclp110b di, whsp300c w, mclp200b t
             WHERE a.div_part = i_div_part
               AND a.load_depart_sid = cust.load_depart_sid
               AND a.custa = cust.cust_id
               AND a.stata = 'O'
               AND a.excptn_sw = 'N'
               AND NOT EXISTS(SELECT 1
                                FROM sub_prcs_ord_src s
                               WHERE s.div_part = a.div_part
                                 AND s.ord_src = a.ipdtsa
                                 AND s.prcs_id = 'LOAD BALANCE'
                                 AND s.prcs_sbtyp_cd = 'BLB')
               AND a.ordnoa NOT IN(SELECT nao.ord_num
                                     FROM nao)
               AND b.div_part = a.div_part
               AND b.ordnob = a.ordnoa
               AND b.statb = 'O'
               AND b.excptn_sw = 'N'
               AND b.subrcb < 999
               AND b.ntshpb IS NULL
--               AND b.sllumb NOT IN('CII', 'CIR', 'CIC')
               AND b.ordqtb > 0
               AND NOT EXISTS(SELECT 1
                                FROM kit_item_mstr_kt1m k
                               WHERE k.div_part = b.div_part
                                 AND k.comp_item_num = b.orditb
                                 AND k.kit_typ = 'AGG'
                                 AND b.subrcb = 0)
               AND poo.ord_num(+) = b.ordnob
               AND poo.ord_ln(+) = b.lineb
               AND e.iteme = b.itemnb
               AND e.uome = b.sllumb
               AND di.div_part = b.div_part
               AND di.itemb = b.itemnb
               AND di.uomb = b.sllumb
               AND w.div_part = di.div_part
               AND w.itemc = DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb)
               AND w.uomc = NVL(TRIM(di.suomb), di.uomb)
               AND w.taxjrc IS NULL
               AND t.div_part(+) = b.div_part
               AND t.totctb(+) = b.totctb
            UNION ALL
            SELECT cust.load_num
                   || LPAD(cust.stop_num, 2, '0')
                   || cust.cust_id
                   || DECODE(cust.cntnr_trckg_sw, 'Y', DECODE(RTRIM(REPLACE(a.cpoa, '0')), NULL, poo.po_num, a.cpoa), NULL)
                   || LPAD(b.labctb, 3, '0')
                   || b.totctb AS grp,
                   (CASE
                      WHEN cust.mstr_cs_cust_sw = 'Y' THEN 'MCQ'
                      WHEN b.totctb IS NULL THEN 'FC'
                      WHEN b.sllumb IN('CII', 'CIR', 'CIC') THEN 'CIG'
                      ELSE 'OTH'
                    END
                   ) AS typ,
                   w.aislc || w.binc || w.levlc AS pick_slot, w.taxjrc AS pick_zone, b.ordnob AS ord_num, b.lineb AS ord_ln,
                   e.catite AS catlg_num, b.itemnb AS cbr_item, b.sllumb AS uom, b.ordqtb AS ord_qty, cust.load_num,
                   cust.stop_num, cust.cust_id, cust.corp_cd, cust.grp_cd, cust.cntnr_trckg_sw,
                   NVL(poo.po_num, a.cpoa) AS po_num, LPAD(NVL(b.manctb, '0'), 3, '0') AS mfst_catg,
                   LPAD(NVL(b.totctb, '0'), 3, '0') AS tote_catg, LPAD(NVL(TO_CHAR(b.labctb), '0'), 3, '0') AS labl_catg,
                   e.cubee AS item_cube, e.wghte AS item_wght, t.innerb AS inner_cube,
                   (CASE
                      WHEN(    'Y' IN(l_cube_all_by_hc_sw, cust.cube_by_hc_sw)
                           AND b.ordqtb >= e.fmqtye) THEN e.fmqtye
                      ELSE t.totcnb
                    END
                   ) AS hc_qty,
                   t.totcnb AS box_qty, e.mulsle AS mc_qty,
                   (CASE
                      WHEN 'Y' IN(l_cube_all_by_hc_sw, cust.cube_by_hc_sw) THEN 'Y'
                      ELSE 'N'
                    END) AS cube_by_hc
              FROM cust, ordp100a a, ordp120b b, poo, sawp505e e, mclp110b di, mclp260d wj, whsp300c w, mclp200b t
             WHERE a.div_part = i_div_part
               AND a.load_depart_sid = cust.load_depart_sid
               AND a.custa = cust.cust_id
               AND a.stata = 'O'
               AND a.excptn_sw = 'N'
               AND NOT EXISTS(SELECT 1
                                FROM sub_prcs_ord_src s
                               WHERE s.div_part = a.div_part
                                 AND s.ord_src = a.ipdtsa
                                 AND s.prcs_id = 'LOAD BALANCE'
                                 AND s.prcs_sbtyp_cd = 'BLB')
               AND a.ordnoa NOT IN(SELECT nao.ord_num
                                     FROM nao)
               AND b.div_part = a.div_part
               AND b.ordnob = a.ordnoa
               AND b.statb = 'O'
               AND b.excptn_sw = 'N'
               AND b.subrcb < 999
               AND b.ntshpb IS NULL
               AND b.sllumb IN('CII', 'CIR', 'CIC')
               AND b.ordqtb > 0
               AND poo.ord_num(+) = b.ordnob
               AND poo.ord_ln(+) = b.lineb
               AND e.iteme = b.itemnb
               AND e.uome = b.sllumb
               AND di.div_part = b.div_part
               AND di.itemb = b.itemnb
               AND di.uomb = b.sllumb
               AND wj.div_part = a.div_part
               AND wj.txjrd = cust.cust_jrsdctn
               AND w.div_part = di.div_part
               AND w.itemc = di.itemb
               AND w.uomc = di.uomb
               AND w.taxjrc = wj.stzond
               AND t.div_part(+) = b.div_part
               AND t.totctb(+) = b.totctb)
      SELECT   ord.grp, ord.typ, ord.pick_slot, ord.pick_zone, ord.ord_num, ord.ord_ln, ord.catlg_num, ord.cbr_item, ord.uom,
               ord.ord_qty, ord.item_cube, ord.item_wght, ord.inner_cube, ord.hc_qty, ord.box_qty, ord.mc_qty, ord.cube_by_hc,
               ord.load_num, ord.stop_num, ord.cust_id, ord.corp_cd, ord.grp_cd, ord.cntnr_trckg_sw, ord.po_num,
               ord.mfst_catg
               || ord.tote_catg
               || ord.labl_catg
               || LPAD
                   ((CASE
                       WHEN ord.typ IN('FC', 'MCQ') THEN 0
                       ELSE DECODE
                             (ord.cntnr_trckg_sw,
                              'N', 0,
                              DENSE_RANK() OVER(PARTITION BY ord.cust_id, ord.mfst_catg, ord.tote_catg, ord.labl_catg
                                                ORDER BY ord.po_num)
                              - 1
                             )
                     END
                    ),
                    3,
                    '0'
                   ) AS tote_grp_id
          FROM ord
      ORDER BY ord.grp, ord.typ, ord.pick_slot, ord.pick_zone, ord.catlg_num, ord.ord_qty DESC, ord.ord_num, ord.ord_ln;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END plan_ord_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRECUBE_ORD_FN
  ||  Map plan order record to pre-cube order record.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION precube_ord_fn(
    i_r_plan_ord  IN  g_rt_plan_ord
  )
    RETURN g_rt_precube_ord IS
    l_r_precube_ord  g_rt_precube_ord;
  BEGIN
    l_r_precube_ord.ord_num := i_r_plan_ord.ord_num;
    l_r_precube_ord.ord_ln := i_r_plan_ord.ord_ln;
    l_r_precube_ord.load_num := i_r_plan_ord.load_num;
    l_r_precube_ord.stop_num := i_r_plan_ord.stop_num;
    l_r_precube_ord.cust_id := i_r_plan_ord.cust_id;
    l_r_precube_ord.corp_cd := i_r_plan_ord.corp_cd;
    l_r_precube_ord.grp_cd := i_r_plan_ord.grp_cd;
    l_r_precube_ord.cntnr_trckg_sw := i_r_plan_ord.cntnr_trckg_sw;
    l_r_precube_ord.po_num := i_r_plan_ord.po_num;
    l_r_precube_ord.tote_grp_id := i_r_plan_ord.tote_grp_id;
    l_r_precube_ord.pick_slot := i_r_plan_ord.pick_slot;
    l_r_precube_ord.catlg_num := i_r_plan_ord.catlg_num;
    l_r_precube_ord.item_wght := i_r_plan_ord.item_wght;
    l_r_precube_ord.item_cube := i_r_plan_ord.item_cube;
    RETURN(l_r_precube_ord);
  END precube_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || CNTNR_ORD_FN
  ||  Map pre-cube order record to container order record.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION cntnr_ord_fn(
    i_r_precube_ord  IN  g_rt_precube_ord,
    i_cntnr_id       IN  VARCHAR2,
    i_qty            IN  NUMBER
  )
    RETURN op_load_cube_plan_pk.g_rt_cntnr_ord IS
    l_r_cntnr_ord  op_load_cube_plan_pk.g_rt_cntnr_ord;
  BEGIN
    l_r_cntnr_ord.ord_num := i_r_precube_ord.ord_num;
    l_r_cntnr_ord.ord_ln := i_r_precube_ord.ord_ln;
    l_r_cntnr_ord.load_num := i_r_precube_ord.load_num;
    l_r_cntnr_ord.stop_num := i_r_precube_ord.stop_num;
    l_r_cntnr_ord.cust_id := i_r_precube_ord.cust_id;
    l_r_cntnr_ord.corp_cd := i_r_precube_ord.corp_cd;
    l_r_cntnr_ord.grp_cd := i_r_precube_ord.grp_cd;
    l_r_cntnr_ord.cntnr_trckg_sw := i_r_precube_ord.cntnr_trckg_sw;
    l_r_cntnr_ord.po_num := i_r_precube_ord.po_num;
    l_r_cntnr_ord.cntnr_id := i_cntnr_id;
    l_r_cntnr_ord.tote_grp_id := i_r_precube_ord.tote_grp_id;
    l_r_cntnr_ord.pick_slot := i_r_precube_ord.pick_slot;
    l_r_cntnr_ord.catlg_num := i_r_precube_ord.catlg_num;
    l_r_cntnr_ord.item_wght := i_r_precube_ord.item_wght;
    l_r_cntnr_ord.item_cube := i_r_precube_ord.item_cube;
    l_r_cntnr_ord.ord_qty := i_qty;
    RETURN(l_r_cntnr_ord);
  END cntnr_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || KIT_CNTNRS_FN
  ||  Assign one container per aggregate kit item for component qty multiple.
  ||  Example:
  ||    KitItemK contains:
  ||      ComponentItemA with Qty 1
  ||      ComponentItemB with Qty 2
  ||    Customer orders 2 of KitItemK represented by :
  ||      Order1 Line1 for ComponentItemA with Qty 2
  ||      Order2 Line1 for ComponentItemB with Qty 4
  ||    ContainerA contains:
  ||      Order1 Line1 ComponentItemA Qty 1
  ||      Order2 Line1 ComponentItemB Qty 2
  ||    ContainerB contains:
  ||      Order1 Line1 ComponentItemA Qty 1
  ||      Order2 Line1 ComponentItemB Qty 2
  ||
  ||   i_load_list : comma-delimited list of loads. i.e.: 0101,0102,0103
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | PIR15617
  ||----------------------------------------------------------------------------
  */
  FUNCTION kit_cntnrs_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2,
    i_ts         IN  DATE
  )
    RETURN g_tt_cntnr_ords PIPELINED IS
    l_c_module  CONSTANT typ.t_maxfqnm    := 'OP_LOAD_CUBE_PLAN_PK.KIT_CNTNRS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;

    TYPE l_rt_kit_cntnr IS RECORD(
      kit_item   sawp505e.iteme%TYPE,
      kit_uom    sawp505e.uome%TYPE,
      ratio_qty  PLS_INTEGER,
      ord_num    NUMBER,
      ord_ln     NUMBER,
      comp_qty   NUMBER
    );

    TYPE l_tt_kit_cntnrs IS TABLE OF l_rt_kit_cntnr;

    CURSOR l_cur_kit_ords(
      b_div_part   NUMBER,
      b_llr_dt     DATE,
      b_load_list  VARCHAR2
    ) IS
      SELECT   ki.iteme AS kit_item, ki.uome AS kit_uom, k.comp_qty, b.ordnob AS ord_num, b.lineb AS ord_ln,
               (b.ordqtb / k.comp_qty) AS ratio_qty,
               DENSE_RANK() OVER(ORDER BY ld.load_num, se.stop_num, se.cust_id, a.cpoa, a.dsorda, k.item_num) AS grp,
               DENSE_RANK() OVER(PARTITION BY ld.load_num, se.stop_num, se.cust_id, a.cpoa, a.dsorda, k.item_num ORDER BY k.comp_item_num)
                                                                                                                 AS seq,
               ld.load_num, se.stop_num, se.cust_id, LPAD(cx.corpb, 3, '0') AS corp_cd,
               NVL(SUBSTR(c.retgpc, 3), '000') AS grp_cd, NVL(g.cntnr_trckg_sw, 'N') AS cntnr_trckg_sw,
               a.cpoa AS po_num,
               LPAD(NVL(b.manctb, '0'), 3, '0')
               || LPAD(NVL(b.totctb, '0'), 3, '0')
               || LPAD(NVL(TO_CHAR(b.labctb), '0'), 3, '0')
               || LPAD(DECODE(NVL(g.cntnr_trckg_sw, 'N'),
                              'N', 0,
                              DENSE_RANK() OVER(PARTITION BY se.cust_id ORDER BY a.cpoa) - 1
                             ),
                       3,
                       '0'
                      ) AS tote_grp_id,
               w.aislc || w.binc || w.levlc AS pick_slot, kc.catite AS catlg_num, kc.wghte AS item_wght,
               kc.cubee AS item_cube
          FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, sysp200c c, mclp020b cx, mclp100a g, ordp120b b,
               sawp505e kc, kit_item_mstr_kt1m k, sawp505e ki, whsp300c w
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND INSTR(',' || b_load_list || ',', ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.stata = 'O'
           AND a.excptn_sw = 'N'
           AND NOT EXISTS(SELECT 1
                            FROM sub_prcs_ord_src s
                           WHERE s.div_part = a.div_part
                             AND s.ord_src = a.ipdtsa
                             AND s.prcs_id = 'LOAD BALANCE'
                             AND s.prcs_sbtyp_cd = 'BLB')
           AND NOT EXISTS(SELECT 1
                            FROM sub_prcs_ord_src s
                           WHERE s.div_part = a.div_part
                             AND s.ord_src = a.ipdtsa
                             AND s.prcs_id = 'LOAD BALANCE'
                             AND s.prcs_sbtyp_cd = 'NAO'
                             AND NOT EXISTS(SELECT 1
                                              FROM ordp100a a2
                                             WHERE a2.div_part = a.div_part
                                               AND a2.load_depart_sid = a.load_depart_sid
                                               AND a2.custa = a.custa
                                               AND a2.ipdtsa NOT IN(
                                                     SELECT s2.ord_src
                                                       FROM sub_prcs_ord_src s2
                                                      WHERE s2.div_part = b_div_part
                                                        AND s2.prcs_id = 'LOAD BALANCE'
                                                        AND s2.prcs_sbtyp_cd IN('BLB', 'NAO'))
                                               AND a2.stata IN('O', 'I', 'P', 'R')
                                               AND a2.excptn_sw = 'N'
                                               AND a2.dsorda = 'R'))
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND c.div_part = se.div_part
           AND c.acnoc = se.cust_id
           AND cx.div_part = se.div_part
           AND cx.custb = se.cust_id
           AND g.div_part(+) = c.div_part
           AND g.cstgpa(+) = c.retgpc
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb = 'O'
           AND b.excptn_sw = 'N'
           AND b.ordqtb > 0
           AND b.subrcb = 0
           AND kc.iteme = b.itemnb
           AND kc.uome = b.sllumb
           AND k.div_part = b_div_part
           AND k.kit_typ = 'AGG'
           AND k.item_num = DECODE('Y', 'Y', k.item_num)   -- force index
           AND k.comp_item_num = kc.catite
           AND ki.catite = k.item_num
           AND w.div_part = b.div_part
           AND w.itemc = b.itemnb
           AND w.uomc = b.sllumb
           AND w.taxjrc IS NULL
      ORDER BY grp, seq;

    TYPE l_tt_kit_ords IS TABLE OF l_cur_kit_ords%ROWTYPE;

    l_t_kit_cntnrs       l_tt_kit_cntnrs  := l_tt_kit_cntnrs();
    l_t_kit_ords         l_tt_kit_ords    := l_tt_kit_ords();
    l_r_precube_ord      g_rt_precube_ord;
    l_t_cntnr_ords       g_tt_cntnr_ords  := g_tt_cntnr_ords();
    l_idx                PLS_INTEGER;

    FUNCTION precube_ord_fn(
      i_r_kit_ord  IN  l_cur_kit_ords%ROWTYPE
    )
      RETURN g_rt_precube_ord IS
      l_r_precube_ord  g_rt_precube_ord;
    BEGIN
      l_r_precube_ord.ord_num := i_r_kit_ord.ord_num;
      l_r_precube_ord.ord_ln := i_r_kit_ord.ord_ln;
      l_r_precube_ord.load_num := i_r_kit_ord.load_num;
      l_r_precube_ord.stop_num := i_r_kit_ord.stop_num;
      l_r_precube_ord.cust_id := i_r_kit_ord.cust_id;
      l_r_precube_ord.corp_cd := i_r_kit_ord.corp_cd;
      l_r_precube_ord.grp_cd := i_r_kit_ord.grp_cd;
      l_r_precube_ord.cntnr_trckg_sw := i_r_kit_ord.cntnr_trckg_sw;
      l_r_precube_ord.po_num := i_r_kit_ord.po_num;
      l_r_precube_ord.tote_grp_id := i_r_kit_ord.tote_grp_id;
      l_r_precube_ord.pick_slot := i_r_kit_ord.pick_slot;
      l_r_precube_ord.catlg_num := i_r_kit_ord.catlg_num;
      l_r_precube_ord.item_wght := i_r_kit_ord.item_wght;
      l_r_precube_ord.item_cube := i_r_kit_ord.item_cube;
      RETURN(l_r_precube_ord);
    END precube_ord_fn;

    PROCEDURE add_cntnr_sp(
      i_r_precube_ord  IN             g_rt_precube_ord,
      i_cntnr_id       IN             VARCHAR2,
      i_qty            IN             NUMBER,
      io_t_cntnr_ords  IN OUT NOCOPY  g_tt_cntnr_ords
    ) IS
      l_r_cntnr_ord  g_rt_cntnr_ord;
    BEGIN
      IF io_t_cntnr_ords IS NULL THEN
        io_t_cntnr_ords := g_tt_cntnr_ords();
      END IF;

      l_r_cntnr_ord := cntnr_ord_fn(i_r_precube_ord, i_cntnr_id, i_qty);
      io_t_cntnr_ords.EXTEND;
      io_t_cntnr_ords(io_t_cntnr_ords.LAST) := l_r_cntnr_ord;
    END add_cntnr_sp;

    PROCEDURE add_cntnrs_sp(
      i_div            IN      VARCHAR2,
      i_ts             IN      DATE,
      i_t_kit_cntnrs   IN      l_tt_kit_cntnrs,
      i_r_precube_ord  IN      g_rt_precube_ord,
      o_t_cntnr_ords   OUT     g_tt_cntnr_ords
    ) IS
      l_cntnr_id  bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    BEGIN
      o_t_cntnr_ords := g_tt_cntnr_ords();

      IF i_t_kit_cntnrs.COUNT > 0 THEN
        <<ratio_qty_loop>>
        FOR i IN 1 .. i_t_kit_cntnrs(i_t_kit_cntnrs.FIRST).ratio_qty LOOP
          l_cntnr_id := op_allocate_pk.container_id_fn(i_div,
                                                       i_ts,
                                                       i_t_kit_cntnrs(i_t_kit_cntnrs.FIRST).kit_item,
                                                       i_t_kit_cntnrs(i_t_kit_cntnrs.FIRST).kit_uom
                                                      );
          <<cntnr_loop>>
          FOR j IN i_t_kit_cntnrs.FIRST .. i_t_kit_cntnrs.LAST LOOP
            add_cntnr_sp(i_r_precube_ord, l_cntnr_id, i_t_kit_cntnrs(j).comp_qty, o_t_cntnr_ords);
          END LOOP cntnr_loop;
        END LOOP ratio_qty_loop;
      END IF;   -- l_t_kit_cntnrs.COUNT > 0
    END add_cntnrs_sp;

    PROCEDURE add_kit_cntnr_sp(
      i_r_kit_ord      IN             l_cur_kit_ords%ROWTYPE,
      io_t_kit_cntnrs  IN OUT NOCOPY  l_tt_kit_cntnrs
    ) IS
    BEGIN
      io_t_kit_cntnrs.EXTEND;
      io_t_kit_cntnrs(io_t_kit_cntnrs.LAST).kit_item := i_r_kit_ord.kit_item;
      io_t_kit_cntnrs(io_t_kit_cntnrs.LAST).kit_uom := i_r_kit_ord.kit_uom;
      io_t_kit_cntnrs(io_t_kit_cntnrs.LAST).ratio_qty := i_r_kit_ord.ratio_qty;
      io_t_kit_cntnrs(io_t_kit_cntnrs.LAST).ord_num := i_r_kit_ord.ord_num;
      io_t_kit_cntnrs(io_t_kit_cntnrs.LAST).ord_ln := i_r_kit_ord.ord_ln;
      io_t_kit_cntnrs(io_t_kit_cntnrs.LAST).comp_qty := i_r_kit_ord.comp_qty;
    END add_kit_cntnr_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'Ts', i_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Open Kit Order Cursor');

    OPEN l_cur_kit_ords(l_div_part, i_llr_dt, i_load_list);

    <<ords_cur_loop>>
    LOOP
      logs.dbg('Fetch Kit Order Cursor');

      FETCH l_cur_kit_ords
      BULK COLLECT INTO l_t_kit_ords LIMIT 100;

      EXIT WHEN l_t_kit_ords.COUNT = 0;
      logs.dbg('Process KitOrds Collection');
      <<ords_tbl_loop>>
      FOR i IN l_t_kit_ords.FIRST .. l_t_kit_ords.LAST LOOP
        logs.dbg('Map KitOrd to PreCubeOrd');
        l_r_precube_ord := precube_ord_fn(l_t_kit_ords(i));

        IF l_t_kit_ords(i).seq = 1 THEN
          logs.dbg('Add Kit Containers');
          add_cntnrs_sp(i_div, i_ts, l_t_kit_cntnrs, l_r_precube_ord, l_t_cntnr_ords);
          l_idx := l_t_cntnr_ords.FIRST;
          WHILE l_idx IS NOT NULL LOOP
            PIPE ROW(l_t_cntnr_ords(l_idx));
            l_idx := l_t_cntnr_ords.NEXT(l_idx);
          END LOOP;
          -- set to empty
          l_t_cntnr_ords := g_tt_cntnr_ords();
          l_t_kit_cntnrs := l_tt_kit_cntnrs();
        END IF;

        logs.dbg('Store in KitCntnrs Collection');
        add_kit_cntnr_sp(l_t_kit_ords(i), l_t_kit_cntnrs);
      END LOOP ords_tbl_loop;
    END LOOP ords_cur_loop;
    logs.dbg('Close Kit Order Cursor');

    CLOSE l_cur_kit_ords;

    logs.dbg('Final Add');
    add_cntnrs_sp(i_div, i_ts, l_t_kit_cntnrs, l_r_precube_ord, l_t_cntnr_ords);
    logs.dbg('Final Pipe');
    l_idx := l_t_cntnr_ords.FIRST;
    WHILE l_idx IS NOT NULL LOOP
      PIPE ROW(l_t_cntnr_ords(l_idx));
      l_idx := l_t_cntnr_ords.NEXT(l_idx);
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN;
  EXCEPTION
    WHEN no_data_needed THEN
      IF l_cur_kit_ords%ISOPEN THEN
        CLOSE l_cur_kit_ords;
      END IF;

      RAISE;
    WHEN OTHERS THEN
      IF l_cur_kit_ords%ISOPEN THEN
        CLOSE l_cur_kit_ords;
      END IF;

      logs.err(lar_parm);
  END kit_cntnrs_fn;

  /*
  ||----------------------------------------------------------------------------
  || PLAN_ORD_FN
  ||  Pre-cube container order record to container order record.
  ||
  ||   i_load_list : comma-delimited list of loads. i.e.: 0101,0102,0103
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | Change to call new LOAD_STAT_UDF, OP_PARMS_PK.VAL_FN,
  ||                    | OP_PARMS_PK.GET_PARMS_FOR_PRFX_SP,
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN. PIR15617
  || 01/24/17 | rhalpai | Change logic in PRCS_CIG_SP to process Cigs when
  ||                    | cube_by_hc_sw is set to N by initializing l_ord_qty
  ||                    | variable. SDHD-82670
  ||----------------------------------------------------------------------------
  */
  FUNCTION plan_ord_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2,
    i_ts         IN  DATE
  )
    RETURN g_tt_cntnr_ords PIPELINED IS
    l_c_module  CONSTANT typ.t_maxfqnm                           := 'OP_LOAD_CUBE_PLAN_PK.PLAN_ORD_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv_plan_ord        g_cvt_plan_ord;
    l_r_plan_ord         g_rt_plan_ord;
    l_load_stat          VARCHAR2(30);
    l_r_precube_ord      g_rt_precube_ord;
    l_grp_save           VARCHAR2(50)                            := '~';
    l_cntnr_id           bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    l_cig_qty            PLS_INTEGER;
    l_t_cntnr_ords       g_tt_cntnr_ords;
    l_inner_cube         NUMBER;
    l_idx                PLS_INTEGER;

    PROCEDURE prcs_kits_sp(
      i_div           IN      VARCHAR2,
      i_llr_dt        IN      DATE,
      i_load_list     IN      VARCHAR2,
      i_ts            IN      DATE,
      o_t_cntnr_ords  OUT     g_tt_cntnr_ords
    ) IS
    BEGIN
      SELECT *
      BULK COLLECT INTO o_t_cntnr_ords
        FROM TABLE(kit_cntnrs_fn(i_div, i_llr_dt, i_load_list, i_ts)) t;
    END prcs_kits_sp;

    PROCEDURE add_cntnr_sp(
      i_r_precube_ord  IN             g_rt_precube_ord,
      i_cntnr_id       IN             VARCHAR2,
      i_qty            IN             NUMBER,
      io_t_cntnr_ords  IN OUT NOCOPY  g_tt_cntnr_ords
    ) IS
      l_r_cntnr_ord  g_rt_cntnr_ord;
    BEGIN
      IF io_t_cntnr_ords IS NULL THEN
        io_t_cntnr_ords := g_tt_cntnr_ords();
      END IF;

      l_r_cntnr_ord := cntnr_ord_fn(i_r_precube_ord, i_cntnr_id, i_qty);
      io_t_cntnr_ords.EXTEND;
      io_t_cntnr_ords(io_t_cntnr_ords.LAST) := l_r_cntnr_ord;
    END add_cntnr_sp;

    PROCEDURE add_compl_cntnr_sp(
      i_div            IN             VARCHAR2,
      i_ts             IN             DATE,
      i_cbr_item       IN             VARCHAR2,
      i_uom            IN             VARCHAR2,
      i_qty            IN             NUMBER,
      i_r_precube_ord  IN             g_rt_precube_ord,
      io_t_cntnr_ords  IN OUT NOCOPY  g_tt_cntnr_ords
    ) IS
      l_cmpl_cntnr_id  bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    BEGIN
      l_cmpl_cntnr_id := op_allocate_pk.container_id_fn(i_div, i_ts, i_cbr_item, i_uom);
      add_cntnr_sp(i_r_precube_ord, l_cmpl_cntnr_id, i_qty, io_t_cntnr_ords);
    END add_compl_cntnr_sp;

    PROCEDURE prcs_mstr_cs_qty_sp(
      i_div            IN      VARCHAR2,
      i_ts             IN      DATE,
      i_r_plan_ord     IN      g_rt_plan_ord,
      i_r_precube_ord  IN      g_rt_precube_ord,
      o_t_cntnr_ords   OUT     g_tt_cntnr_ords
    ) IS
    BEGIN
      o_t_cntnr_ords := g_tt_cntnr_ords();
      FOR i IN 1 .. i_r_plan_ord.ord_qty / i_r_plan_ord.mc_qty LOOP
        logs.dbg('MCQ - Add Complete Container');
        add_compl_cntnr_sp(i_div,
                           i_ts,
                           i_r_plan_ord.cbr_item,
                           i_r_plan_ord.uom,
                           i_r_plan_ord.mc_qty,
                           i_r_precube_ord,
                           o_t_cntnr_ords
                          );
      END LOOP;
    END prcs_mstr_cs_qty_sp;

    PROCEDURE prcs_fc_sp(
      i_div            IN      VARCHAR2,
      i_ts             IN      DATE,
      i_r_plan_ord     IN      g_rt_plan_ord,
      i_r_precube_ord  IN      g_rt_precube_ord,
      o_t_cntnr_ords   OUT     g_tt_cntnr_ords
    ) IS
    BEGIN
      o_t_cntnr_ords := g_tt_cntnr_ords();
      FOR i IN 1 .. i_r_plan_ord.ord_qty LOOP
        logs.dbg('FC - Add Complete Container');
        add_compl_cntnr_sp(i_div, i_ts, i_r_plan_ord.cbr_item, i_r_plan_ord.uom, 1, i_r_precube_ord, o_t_cntnr_ords);
      END LOOP;
    END prcs_fc_sp;

    PROCEDURE prcs_cig_sp(
      i_div            IN      VARCHAR2,
      i_ts             IN      DATE,
      i_r_plan_ord     IN      g_rt_plan_ord,
      i_r_precube_ord  IN      g_rt_precube_ord,
      io_grp_save      IN OUT  VARCHAR2,
      io_cntnr_id      IN OUT  VARCHAR2,
      io_cig_qty       IN OUT  PLS_INTEGER,
      o_t_cntnr_ords   OUT     g_tt_cntnr_ords
    ) IS
      l_ord_qty  PLS_INTEGER;
      l_wrk_qty  PLS_INTEGER;
      l_hc_cnt   PLS_INTEGER;
    BEGIN
      o_t_cntnr_ords := g_tt_cntnr_ords();

      IF i_r_plan_ord.grp <> io_grp_save THEN
        io_grp_save := i_r_plan_ord.grp;
        io_cntnr_id := NULL;
        io_cig_qty := i_r_plan_ord.box_qty;
      END IF;   -- i_r_plan_ord.grp <> io_grp_save

      -- Put HC qtys to their own containers
      IF (    i_r_plan_ord.cube_by_hc = 'Y'
          AND i_r_plan_ord.hc_qty > 0
          AND i_r_plan_ord.ord_qty >= i_r_plan_ord.hc_qty) THEN
        l_hc_cnt := FLOOR(i_r_plan_ord.ord_qty / i_r_plan_ord.hc_qty);
        FOR i IN 1 .. l_hc_cnt LOOP
          logs.dbg('CIG HC - Add Complete Container');
          add_compl_cntnr_sp(i_div,
                             i_ts,
                             i_r_plan_ord.cbr_item,
                             i_r_plan_ord.uom,
                             i_r_plan_ord.hc_qty,
                             i_r_precube_ord,
                             o_t_cntnr_ords
                            );
        END LOOP;
        -- Remaining qty
        l_ord_qty := i_r_plan_ord.ord_qty -(l_hc_cnt * i_r_plan_ord.hc_qty);
      ELSE
        l_ord_qty := i_r_plan_ord.ord_qty;
      END IF;   -- i_r_plan_ord.cube_by_hc = 'Y' AND i_r_plan_ord.hc_qty > 0 AND i_r_plan_ord.ord_qty >= i_r_plan_ord.hc_qty

      -- Allocated Qty could now be zero if it was a multiple of HC Qty
      IF l_ord_qty > 0 THEN
        l_wrk_qty := 0;
        logs.dbg('CIG - Process Order Qty Loop');
        <<cig_ord_qty_loop>>
        FOR i IN 1 .. l_ord_qty LOOP
          IF io_cntnr_id IS NULL THEN
            logs.dbg('CIG - Get Container ID');
            io_cntnr_id := op_allocate_pk.container_id_fn(i_div, i_ts, i_r_plan_ord.cbr_item, i_r_plan_ord.uom);
          END IF;   -- io_cntnr_id IS NULL

          io_cig_qty := io_cig_qty - 1;

          IF io_cig_qty >= 0 THEN
            -- Fits in tote
            l_wrk_qty := l_wrk_qty + 1;
          ELSE
            -- Will not fit in tote
            IF l_wrk_qty > 0 THEN
              -- Container for qty so far that did fit
              logs.dbg('CIG - Add Container for Qty so far that Fit');
              add_cntnr_sp(i_r_precube_ord, io_cntnr_id, l_wrk_qty, o_t_cntnr_ords);
            END IF;   -- l_wrk_qty > 0

            -- Handle remaining qty
            logs.dbg('CIG - Add Container for Remaining Qty');
            io_cntnr_id := op_allocate_pk.container_id_fn(i_div, i_ts, i_r_plan_ord.cbr_item, i_r_plan_ord.uom);
            io_cig_qty := i_r_plan_ord.box_qty - 1;
            l_wrk_qty := 1;
          END IF;   -- io_cig_qty >= 0
        END LOOP cig_ord_qty_loop;
        -- Container for remaining qty for order line
        -- Do not generate new container ID since next order line
        -- will use current container ID if it also fits in tote
        logs.dbg('CIG - Add Container for Remaining Qty for Order Line');
        add_cntnr_sp(i_r_precube_ord, io_cntnr_id, l_wrk_qty, o_t_cntnr_ords);
      END IF;   -- io_ord_qty > 0
    END prcs_cig_sp;

    PROCEDURE prcs_totes_sp(
      i_div            IN      VARCHAR2,
      i_ts             IN      DATE,
      i_r_plan_ord     IN      g_rt_plan_ord,
      i_r_precube_ord  IN      g_rt_precube_ord,
      io_grp_save      IN OUT  VARCHAR2,
      io_cntnr_id      IN OUT  VARCHAR2,
      io_inner_cube    IN OUT  NUMBER,
      o_t_cntnr_ords   OUT     g_tt_cntnr_ords
    ) IS
      l_wrk_qty  PLS_INTEGER;
    BEGIN
      o_t_cntnr_ords := g_tt_cntnr_ords();

      IF i_r_plan_ord.grp <> io_grp_save THEN
        io_grp_save := i_r_plan_ord.grp;
        io_cntnr_id := NULL;
        io_inner_cube := i_r_plan_ord.inner_cube;
      END IF;   -- i_r_plan_ord.grp <> io_grp_save

      l_wrk_qty := 0;
      logs.dbg('OTH - Process Order Qty Loop');
      <<oth_ord_qty_loop>>
      FOR i IN 1 .. i_r_plan_ord.ord_qty LOOP
        IF io_cntnr_id IS NULL THEN
          logs.dbg('OTH - Get Container ID');
          io_cntnr_id := op_allocate_pk.container_id_fn(i_div, i_ts, i_r_plan_ord.cbr_item, i_r_plan_ord.uom);
        END IF;   -- io_cntnr_id IS NULL

        IF i_r_plan_ord.item_cube > i_r_plan_ord.inner_cube THEN
          -- Single item will not fit in tote
          IF io_inner_cube <> i_r_plan_ord.inner_cube THEN
            -- Generate new container ID when already partially filled
            -- from previous iteration
            logs.dbg('OTH - Get Container ID for Partially Filled from Prev Iteration');
            io_cntnr_id := op_allocate_pk.container_id_fn(i_div, i_ts, i_r_plan_ord.cbr_item, i_r_plan_ord.uom);
          END IF;   -- io_inner_cube <> i_r_plan_ord.inner_cube

          logs.dbg('OTH - Add Container for Partially Filled from Prev Iteration');
          add_cntnr_sp(i_r_precube_ord, io_cntnr_id, 1, o_t_cntnr_ords);
          io_cntnr_id := NULL;
          io_inner_cube := i_r_plan_ord.inner_cube;
        ELSE
          -- Single item fits in tote
          -- Reduce "remaining qty of tote inner cube" by cube of item
          io_inner_cube := io_inner_cube - i_r_plan_ord.item_cube;

          IF io_inner_cube >= 0 THEN
            -- Fits in tote
            l_wrk_qty := l_wrk_qty + 1;
          ELSE
            -- Will not fit in tote
            IF l_wrk_qty > 0 THEN
              -- Container for qty so far that did fit
              logs.dbg('OTH - Add Container for Qty so far that Fit');
              add_cntnr_sp(i_r_precube_ord, io_cntnr_id, l_wrk_qty, o_t_cntnr_ords);
            END IF;   -- l_wrk_qty > 0

            -- Handle remaining qty
            logs.dbg('OTH - Add Container for Remaining Qty');
            io_cntnr_id := op_allocate_pk.container_id_fn(i_div, i_ts, i_r_plan_ord.cbr_item, i_r_plan_ord.uom);
            io_inner_cube := i_r_plan_ord.inner_cube - i_r_plan_ord.item_cube;
            l_wrk_qty := 1;
          END IF;   -- io_inner_cube > 0
        END IF;   -- i_r_plan_ord.item_cube > i_r_plan_ord.inner_cube
      END LOOP oth_ord_qty_loop;

      IF l_wrk_qty > 0 THEN
        -- Container for remaining qty for order line
        -- Do not generate new container ID since next order line
        -- will use current container ID if it also fits in tote
        logs.dbg('OTH - Add Container for Remaining Qty for Order Line');
        add_cntnr_sp(i_r_precube_ord, io_cntnr_id, l_wrk_qty, o_t_cntnr_ords);
      END IF;   -- l_wrk_qty > 0
    END prcs_totes_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'Ts', i_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Process Kit Containers');
    prcs_kits_sp(i_div, i_llr_dt, i_load_list, i_ts, l_t_cntnr_ords);
    l_idx := l_t_cntnr_ords.FIRST;
    WHILE l_idx IS NOT NULL LOOP
      PIPE ROW(l_t_cntnr_ords(l_idx));
      l_idx := l_t_cntnr_ords.NEXT(l_idx);
    END LOOP;
    -- set to empty
    l_t_cntnr_ords := g_tt_cntnr_ords();
    logs.dbg('Get Pre-Cube Planning Order Cursor');
    l_cv_plan_ord := plan_ord_cur_fn(l_div_part, i_llr_dt, i_load_list, i_ts);
    LOOP
      FETCH l_cv_plan_ord
       INTO l_r_plan_ord;

      EXIT WHEN l_cv_plan_ord%NOTFOUND;
      logs.dbg('Ensure Load is still in Open Status');

      SELECT load_stat_udf(l_div_part, i_llr_dt, l_r_plan_ord.load_num)
        INTO l_load_stat
        FROM DUAL;

      IF l_load_stat = 'Open' THEN
        logs.dbg('Map PlanOrd to PreCubeOrd');
        l_r_precube_ord := precube_ord_fn(l_r_plan_ord);

        CASE l_r_plan_ord.typ
          WHEN 'MCQ' THEN
            logs.dbg('Process MstrCsQty');
            prcs_mstr_cs_qty_sp(i_div, i_ts, l_r_plan_ord, l_r_precube_ord, l_t_cntnr_ords);
          WHEN 'FC' THEN
            logs.dbg('Process FullCase');
            prcs_fc_sp(i_div, i_ts, l_r_plan_ord, l_r_precube_ord, l_t_cntnr_ords);
          WHEN 'CIG' THEN
            logs.dbg('Process Cig');
            prcs_cig_sp(i_div, i_ts, l_r_plan_ord, l_r_precube_ord, l_grp_save, l_cntnr_id, l_cig_qty, l_t_cntnr_ords);
          WHEN 'OTH' THEN
            logs.dbg('Process Totes');
            prcs_totes_sp(i_div,
                          i_ts,
                          l_r_plan_ord,
                          l_r_precube_ord,
                          l_grp_save,
                          l_cntnr_id,
                          l_inner_cube,
                          l_t_cntnr_ords
                         );
        END CASE;

        l_idx := l_t_cntnr_ords.FIRST;
        WHILE l_idx IS NOT NULL LOOP
          PIPE ROW(l_t_cntnr_ords(l_idx));
          l_idx := l_t_cntnr_ords.NEXT(l_idx);
        END LOOP;
        -- set to empty
        l_t_cntnr_ords := g_tt_cntnr_ords();
      END IF;   -- l_load_stat = 'Open'
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN;
  EXCEPTION
    WHEN no_data_needed THEN
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END plan_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LOAD_DETAILS_SP
  ||  Return extract max selection count and cursor or load details for selection.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | Change to call new OP_PARMS_PK.VAL_FN. PIR15617
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_load_details_sp(
    i_div        IN      VARCHAR2,
    i_llr_dt     IN      VARCHAR2,
    o_extr_max   OUT     NUMBER,
    o_cur        OUT     SYS_REFCURSOR,
    i_load_stat  IN      VARCHAR2 DEFAULT 'ALL'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_CUBE_PLAN_PK.GET_LOAD_DETAILS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadStat', i_load_stat);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get Extract Max');
    o_extr_max := NVL(TO_NUMBER(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_load_plan_extr_max)), 0);
    logs.dbg('Open Cursor');

    OPEN o_cur
     FOR
       SELECT *
         FROM TABLE(load_details_fn(i_div, i_llr_dt, i_load_stat)) t;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_load_details_sp;

  /*
  ||----------------------------------------------------------------------------
  || PLAN_SP
  ||  Extract container (pre-cubing) info for given loads and send to mainframe.
  ||
  ||   i_llr_dt    : LLR Date - expected format is YYYY-MM-DD
  ||   i_load_list : comma-delimited list of loads. i.e.: 0101,0102,0103
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/05/15 | rhalpai | Original
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | Change logic to use MERGE instead of INSERT to
  ||                    | LOAD_CUBE_PLAN_EXTR_OP1E to handle subsequent calls
  ||                    | for the same load. PIR15617
  || 01/17/17 | rhalpai | Change logic to expand weight and cube fields in
  ||                    | extract to 11 bytes each. SDHD-80479
  || 10/14/17 | rhalpai | Change to use new CIG_EVENT_MGR_PK.CREATE_INSTANCE and
  ||                    | CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE plan_sp(
    i_div          IN  VARCHAR2,
    i_llr_dt       IN  VARCHAR2,
    i_load_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_LOAD_CUBE_PLAN_PK.PLAN_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_c_ts        CONSTANT DATE           := SYSDATE;
    l_c_rmt_file  CONSTANT VARCHAR2(30)   := 'CUBEPLAN';
    l_file_nm              VARCHAR2(80);
    l_llr_dt               DATE;
    l_org_id               NUMBER;
    l_evnt_parms           CLOB;
    l_evnt_que_id          NUMBER;
    l_t_rpt_lns            typ.tas_maxvc2;
    l_t_loads              type_stab;

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
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.dbg('ENTRY', lar_parm);

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
                      || i_llr_dt
                      || '</value></row>'
                      || '<row><sequence>'
                      || 3
                      || '</sequence><value>'
                      || i_load_list
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
                                       i_event_dfn_id         => cig_constants_events_pk.evd_op_load_cube_plan,
                                       i_parameters           => l_evnt_parms,
                                       i_div_nm               => i_div,
                                       i_is_script_fw_exec    => 'Y',
                                       i_is_complete          => 'Y',
                                       i_pgm_id               => 'PLSQL',
                                       i_user_id              => i_user_id,
                                       o_event_que_id         => l_evnt_que_id
                                      );
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      l_file_nm := i_div || '_PRECUBE_' || TO_CHAR(l_c_ts, 'YYYYMMDDHH24MISS');
      l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_load_cube_plan,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.dbg('Build Extract Collection');

      SELECT   i_llr_dt
               || t.load_num
               || LPAD(t.stop_num, 2, '0')
               || t.cust_id
               || t.corp_cd
               || t.grp_cd
               || t.cntnr_trckg_sw
               || rpad_fn(t.po_num, 30)
               || t.cntnr_id
               || t.tote_grp_id
               || t.pick_slot
               || t.catlg_num
               || TO_CHAR(t.item_wght, 'FM099999999V99')
               || TO_CHAR(t.item_cube, 'FM09999999V999')
               || LPAD(SUM(t.ord_qty), 7, '0')
      BULK COLLECT INTO l_t_rpt_lns
          FROM TABLE(plan_ord_fn(i_div, l_llr_dt, i_load_list, l_c_ts)) t
      GROUP BY t.load_num, t.stop_num, t.cust_id, t.corp_cd, t.grp_cd, t.cntnr_trckg_sw, t.po_num, t.cntnr_id,
               t.tote_grp_id, t.pick_slot, t.catlg_num, t.item_wght, t.item_cube
      ORDER BY 1;

      IF l_t_rpt_lns.COUNT > 0 THEN
        logs.dbg('Write');
        write_sp(l_t_rpt_lns, l_file_nm);
        logs.dbg('FTP File');
        op_ftp_sp(i_div, l_file_nm, l_c_rmt_file);
        logs.dbg('Get Loads');

        SELECT DISTINCT SUBSTR(t.column_value, 11, 4) AS load_num
        BULK COLLECT INTO l_t_loads
                   FROM TABLE(l_t_rpt_lns) t;

        logs.dbg('Log Extract');
        MERGE INTO load_cube_plan_extr_op1e e
             USING (SELECT t.column_value AS load_num
                      FROM TABLE(l_t_loads) t) x
                ON (    e.div_part = l_div_part
                    AND e.llr_dt = l_llr_dt
                    AND e.load_num = x.load_num)
          WHEN MATCHED THEN
            UPDATE
               SET e.user_id = i_user_id, e.last_extr_ts = l_c_ts
          WHEN NOT MATCHED THEN
            INSERT(div_part, llr_dt, load_num, user_id, last_extr_ts)
            VALUES(l_div_part, l_llr_dt, x.load_num, i_user_id, l_c_ts);
        COMMIT;
      END IF;   -- l_t_rpt_lns.COUNT > 0

      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_load_cube_plan,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, op_const_pk.prcs_load_cube_plan || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_load_cube_plan,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END plan_sp;
END op_load_cube_plan_pk;
/

