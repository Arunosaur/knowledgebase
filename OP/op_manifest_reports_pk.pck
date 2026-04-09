CREATE OR REPLACE PACKAGE op_manifest_reports_pk IS
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
  PROCEDURE build_report_table_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_strtg_id   IN  NUMBER,
    i_create_ts  IN  DATE
  );

  PROCEDURE loading_manifest_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE load_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE release_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE summary_load_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE summary_rel_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE stop_order_recap_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE tote_recap_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE stop_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  );

  PROCEDURE manifest_reports_sp(
    i_llr_num    IN  NUMBER,
    i_div        IN  VARCHAR2,
    i_create_ts  IN  VARCHAR2 DEFAULT NULL
  );

  PROCEDURE test_stop_order_recap_sp(
    i_llr_num    IN  NUMBER,
    i_div        IN  VARCHAR2,
    i_create_ts  IN  VARCHAR2
  );

  PROCEDURE test_sp(
    i_llr_num    IN  NUMBER,
    i_div        IN  VARCHAR2,
    i_create_ts  IN  VARCHAR2
  );
END op_manifest_reports_pk;
/

CREATE OR REPLACE PACKAGE BODY op_manifest_reports_pk IS
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 09/26/01 | rhalpai | Replaced sql_error_handler with sql_utilities_pkg.sql_errors
  || 10/09/01 | rhalpai | Added Stop Summary report
  || 03/19/02 | JUSTANI | Added Bags to reports
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_file_dir    CONSTANT VARCHAR2(50)  := '/ftptrans';
  g_rpt_id                 VARCHAR2(6);
  g_file_nm                VARCHAR2(100);
  g_cnt                    PLS_INTEGER   := 1;
  g_ln_cnt                 PLS_INTEGER   := 99;
  g_pg_cnt                 PLS_INTEGER   := 0;
  g_c_lns_per_pg  CONSTANT PLS_INTEGER   := 80;
  g_heading                VARCHAR2(44);
  g_lm_h2_ln               VARCHAR2(133);
  g_lm_h3_ln               VARCHAR2(133);
  g_lm_h4_ln               VARCHAR2(133);
  g_lm_h5_ln               VARCHAR2(133);
  g_lm_h6_ln               VARCHAR2(133);
  g_lm_h7_ln               VARCHAR2(133);
  g_lm_h8_ln               VARCHAR2(133);
  g_lm_m1_ln               VARCHAR2(133);
  g_lm_t1_ln               VARCHAR2(133);
  g_lm_ttls_ln             VARCHAR2(133);
  g_ld_h2_ln               VARCHAR2(133);
  g_ld_h3_ln               VARCHAR2(133);
  g_ld_m1_ln               VARCHAR2(133);
  g_ld_ttls_ln             VARCHAR2(133);
  g_tr_h2_ln               VARCHAR2(133);
  g_tr_h3_ln               VARCHAR2(133);
  g_tr_h4_ln               VARCHAR2(133);
  g_tr_s1_ln               VARCHAR2(133);
  g_tr_ttls_ln             VARCHAR2(133);
  g_ss_h2_ln               VARCHAR2(133);
  g_ss_h3_ln               VARCHAR2(133);
  g_ss_ttls2_ln            VARCHAR2(133);
  g_ss_ttls3_ln            VARCHAR2(133);
  g_ss_dtl1_ln             VARCHAR2(133);
  g_ss_dtl2_ln             VARCHAR2(133);
  g_ss_dtl3_ln             VARCHAR2(133);

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || Build_Report_Table_SP
  ||   This procedure will build the Manifest Reports table entries for a single
  ||   release of orders or when passed a strategy_id of zero (final run) it
  ||   will include all releases for an LLR date.
  ||   When run for a single release it creates 2 types of entries on the table,
  ||   one with a strategy id of 0 -- this is for "Cumulative Reports (using
  ||   all orders for that Load to create the entries) and an entry with the
  ||   strategy Id that was used to release the orders by the Set Release screen.
  ||   All entries are created with the "TIMESTAMP" for that release. This module
  ||   is similar to the Tote Forecast module in OP_ALLOCATE_PK in that it uses
  ||   specific formulas to calculate the number of totes on a stop.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/13/05 | rhalpai | Copied from OP_ALLOCATE_PK and added logic to create
  ||                    | summary entries for all releases for an LLR date.
  ||                    | This will be used to create entries for a final OPLD03
  ||                    | (Summary Load Department Summary) report for the day.
  ||                    | PIR2652
  || 10/20/06 | rhalpai | Converted to use container tracking. PIR3209
  ||                    | Changed qty_alloc to show kit quantities when
  ||                    | applicable. PIR400
  || 11/20/06 | rhalpai | Added NVL's to Tote Final Cursor for manifest category
  ||                    | tote category as MCLANE_MANIFEST_RPTS will not allow
  ||                    | inserts with Nulls in these columns.
  || 03/05/07 | rhalpai | Changed to use MCLP370C for cumulative reports and
  ||                    | calculate bags for containers when div has Cubing of Totes
  ||                    | turned on. IM290595
  || 03/19/07 | rhalpai | Added logic to override bags to boxes using corp code
  ||                    | driven NO_BAGS parm. IM290595
  || 03/30/07 | rhalpai | Changed to cumulative portion of Tote Forecast Cursor
  ||                    | to sum totes,boxes,bags from MCLP370C in a table
  ||                    | expression instead of a direct join to eliminate
  ||                    | possible cartesian resulting in overstated totals.
  ||                    | IM290595
  || 04/23/07 | rhalpai | Changed tote_forecast_cur to ensure mclp200b.totcnb
  ||                    | contains qty greater than zero to eliminate possible
  ||                    | divide by zero exception. IM302686
  || 08/24/07 | rhalpai | Changed tote_forecast_cur to handle multiple bag counts
  ||                    | for container tracking customers. IM332375
  || 10/19/07 | rhalpai | Change to use new CUBE_BY_HC_SW column on MCLP100A.
  ||                    | PIR3209
  || 05/08/08 | rhalpai | Reformatted tote/cube calculations in cursor
  ||                    | tote_forecast_cur. IM404756
  || 05/19/08 | rhalpai | Change to avoid DivideByZero exception if half-case
  ||                    | (FMQTYE) is set to zero. IM411577
  || 06/01/08 | rhalpai | Change box calculation to be based on box max for
  ||                    | tote category (mclp200b.totcnb) instead of item
  ||                    | half-case qty (sawp505e.fmqtye). IM416187
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  || 11/15/11 | rhalpai | Change logic to handle cigs in totes instead of boxes.
  ||                    | PIR10475
  || 04/04/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 01/08/14 | rhalpai | Change logic ToteFinal cursor to not overstate counts
  ||                    | due to multiple default (NULL tote) entries on
  ||                    | MCLP370C. IM-135799
  || 03/01/16 | rhalpai | Change l_cur_tote_final cursor to correctly join on
  ||                    | departure dates. SDOPS-218
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_report_table_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_strtg_id   IN  NUMBER,
    i_create_ts  IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MANIFEST_REPORTS_PK.BUILD_REPORT_TABLE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_no_bags_corps    type_stab;

    TYPE l_rt_tote IS RECORD(
      strtg_id   NUMBER,
      depart_dt  NUMBER,
      load_num   mclane_manifest_rpts.load_num%TYPE,
      stop_num   NUMBER,
      cust_id    sysp200c.acnoc%TYPE,
      eta_dt     NUMBER,
      eta_tm     NUMBER,
      mfst_catg  mclp210c.manctc%TYPE,
      tote_catg  mclp200b.totctb%TYPE,
      qty_alloc  NUMBER,
      prod_wgt   NUMBER,
      prod_cube  NUMBER,
      tote_cnt   NUMBER,
      box_cnt    NUMBER,
      bag_cnt    NUMBER
    );

    CURSOR l_cur_tote_forecast(
      b_div_part         NUMBER,
      b_llr_dt           DATE,
      b_strtg_id         NUMBER,
      b_t_no_bags_corps  type_stab
    ) RETURN l_rt_tote IS
      WITH yy AS
           (SELECT   b_strtg_id AS strtg_id, y.depart_ts, y.load_num, y.stop_num, y.cust_id, y.eta_ts, y.manctb, y.totctb,
                     SUM(y.qty_alloc) AS qty_alloc, SUM(y.prod_wt) AS prod_wt, SUM(y.prod_cube) AS prod_cube, y.tote_cnt,
                     DECODE(y.boxb, 'Y', y.box_cnt) AS box_cnt, DECODE(y.max_bag_qty, 0, 0, y.bag_cnt) AS bag_cnt
                FROM (SELECT   /*+ ORDERED */
                               ld.depart_ts, ld.load_num, se.stop_num, se.cust_id, se.eta_ts, b.manctb, b.totctb, t.boxb,
                               t.totcnb,
                               (CASE
                                  WHEN(    t.boxb = 'Y'
                                       AND nb.corp_cd IS NULL
                                       AND t.max_bag_qty > 0) THEN t.max_bag_qty
                                  ELSE 0
                                END
                               ) AS max_bag_qty,
                               (CASE
                                  WHEN t.pccntb = 'N' THEN (SELECT /*+ ORDERED */
                                                                   COUNT(DISTINCT c.orig_cntnr_id)
                                                              FROM ordp100a a2, ordp120b b2, bill_cntnr_id_bc1c c
                                                             WHERE a2.div_part = b_div_part
                                                               AND a2.load_depart_sid = ld.load_depart_sid
                                                               AND a2.custa = se.cust_id
                                                               AND a2.excptn_sw = 'N'
                                                               AND b2.div_part = a2.div_part
                                                               AND b2.ordnob = a2.ordnoa
                                                               AND b2.excptn_sw = 'N'
                                                               AND b2.statb = 'T'
                                                               AND b2.manctb = b.manctb
                                                               AND b2.totctb = b.totctb
                                                               AND c.div_part = b2.div_part
                                                               AND c.ord_num = b2.ordnob
                                                               AND c.ord_ln_num = b2.lineb)
                                  WHEN(    t.pccntb = 'Y'
                                       AND t.boxb = 'N') THEN (SELECT   /*+ ORDERED */
                                                                        NVL(SUM(COUNT(DISTINCT c.orig_cntnr_id)), 0)
                                                                   FROM ordp100a a2, ordp120b b2, bill_cntnr_id_bc1c c
                                                                  WHERE a2.div_part = b_div_part
                                                                    AND a2.load_depart_sid = ld.load_depart_sid
                                                                    AND a2.custa = se.cust_id
                                                                    AND a2.excptn_sw = 'N'
                                                                    AND b2.div_part = a2.div_part
                                                                    AND b2.ordnob = a2.ordnoa
                                                                    AND b2.excptn_sw = 'N'
                                                                    AND b2.statb = 'T'
                                                                    AND b2.manctb = b.manctb
                                                                    AND b2.totctb = b.totctb
                                                                    AND c.div_part = b2.div_part
                                                                    AND c.ord_num = b2.ordnob
                                                                    AND c.ord_ln_num = b2.lineb
                                                               GROUP BY c.orig_cntnr_id
                                                                 HAVING SUM(c.orig_qty) >
                                                                            DECODE(nb.corp_cd,
                                                                                   NULL, NVL(t.max_bag_qty, 0),
                                                                                   0
                                                                                  ))
                                END
                               ) AS tote_cnt,
                               DECODE(t.boxb,
                                      'Y', (SELECT   /*+ ORDERED */
                                                     NVL(SUM(COUNT(DISTINCT c.orig_cntnr_id)), 0)
                                                FROM ordp100a a2, ordp120b b2, bill_cntnr_id_bc1c c
                                               WHERE a2.div_part = b_div_part
                                                 AND a2.load_depart_sid = ld.load_depart_sid
                                                 AND a2.custa = se.cust_id
                                                 AND a2.excptn_sw = 'N'
                                                 AND b2.div_part = a2.div_part
                                                 AND b2.ordnob = a2.ordnoa
                                                 AND b2.excptn_sw = 'N'
                                                 AND b2.statb = 'T'
                                                 AND b2.manctb = b.manctb
                                                 AND b2.totctb = b.totctb
                                                 AND c.div_part = b2.div_part
                                                 AND c.ord_num = b2.ordnob
                                                 AND c.ord_ln_num = b2.lineb
                                            GROUP BY c.orig_cntnr_id
                                              HAVING SUM(c.orig_qty) > DECODE(nb.corp_cd, NULL, NVL(t.max_bag_qty, 0), 0))
                                     ) AS box_cnt,
                               (CASE
                                  WHEN(    t.boxb = 'Y'
                                       AND t.max_bag_qty > 0
                                       AND nb.corp_cd IS NULL) THEN (SELECT   /*+ ORDERED */
                                                                              NVL(SUM(COUNT(DISTINCT c.orig_cntnr_id)), 0)
                                                                         FROM ordp100a a2, ordp120b b2, bill_cntnr_id_bc1c c
                                                                        WHERE a2.div_part = b_div_part
                                                                          AND a2.load_depart_sid = ld.load_depart_sid
                                                                          AND a2.custa = se.cust_id
                                                                          AND a2.excptn_sw = 'N'
                                                                          AND b2.div_part = a2.div_part
                                                                          AND b2.ordnob = a2.ordnoa
                                                                          AND b2.excptn_sw = 'N'
                                                                          AND b2.statb = 'T'
                                                                          AND b2.manctb = b.manctb
                                                                          AND b2.totctb = b.totctb
                                                                          AND c.div_part = b2.div_part
                                                                          AND c.ord_num = b2.ordnob
                                                                          AND c.ord_ln_num = b2.lineb
                                                                     GROUP BY c.orig_cntnr_id
                                                                       HAVING SUM(c.orig_qty) <= t.max_bag_qty)
                                END
                               ) AS bag_cnt,
                               SUM(NVL(e.cubee, .01) * b.pckqtb) AS prod_cube, SUM(NVL(e.wghte, .01) * b.pckqtb) AS prod_wt,
                               SUM(CASE
                                     WHEN NOT EXISTS(SELECT 1
                                                       FROM kit_item_mstr_kt1m k
                                                      WHERE k.div_part = b_div_part
                                                        AND k.comp_item_num = b.orditb) THEN b.pckqtb
                                     ELSE b.pckqtb
                                          / (SELECT MAX(k.comp_qty)
                                               FROM kit_item_mstr_kt1m k
                                              WHERE k.div_part = b_div_part
                                                AND k.comp_item_num = b.orditb
                                                AND k.comp_item_num =
                                                      (SELECT MAX(k2.comp_item_num)
                                                         FROM kit_item_mstr_kt1m k2
                                                        WHERE k2.div_part = k.div_part
                                                          AND k2.kit_typ = k.kit_typ
                                                          AND k2.item_num = k.item_num))
                                   END
                                  ) AS qty_alloc
                          FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, sawp505e e, mclp020b cx,
                               mclp200b t, (SELECT TO_NUMBER(t.column_value) AS corp_cd
                                              FROM TABLE(CAST(b_t_no_bags_corps AS type_stab)) t) nb
                         WHERE ld.div_part = b_div_part
                           AND ld.llr_dt = b_llr_dt
                           AND a.div_part = ld.div_part
                           AND a.load_depart_sid = ld.load_depart_sid
                           AND a.excptn_sw = 'N'
                           AND se.div_part = a.div_part
                           AND se.load_depart_sid = a.load_depart_sid
                           AND se.cust_id = a.custa
                           AND b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa
                           AND b.excptn_sw = 'N'
                           AND b.statb = 'T'
                           AND e.iteme = b.itemnb
                           AND e.uome = b.sllumb
                           AND cx.div_part = a.div_part
                           AND cx.custb = a.custa
                           AND t.div_part(+) = b.div_part
                           AND t.totctb(+) = b.totctb
                           AND nb.corp_cd(+) = cx.corpb
                           AND b_strtg_id > 0
                      GROUP BY ld.depart_ts, ld.load_num, ld.load_depart_sid, se.stop_num, se.cust_id, se.eta_ts, b.manctb,
                               b.totctb, t.boxb, t.pccntb, t.innerb, t.totcnb, t.max_bag_qty, nb.corp_cd) y
            GROUP BY y.depart_ts, y.load_num, y.stop_num, y.cust_id, y.eta_ts, y.manctb, y.totctb, y.boxb, y.tote_cnt,
                     y.box_cnt, y.bag_cnt, y.max_bag_qty, y.totcnb)
      SELECT /*+ NO_PARALLEL */
             x.strtg_id, TRUNC(x.depart_ts) - DATE '1900-02-28' AS depart_dt, x.load_num, x.stop_num, x.cust_id,
             TRUNC(x.eta_ts) - DATE '1900-02-28' AS eta_dt, TO_NUMBER(TO_CHAR(x.eta_ts, 'HH24MI')) AS eta_tm,
             NVL(x.manctb, '000') AS mfst_catg, NVL(x.totctb, '000') AS tote_catg, NVL(x.qty_alloc, 0) AS qty_alloc,
             NVL(x.prod_wt, 0) AS prod_wt, NVL(x.prod_cube, 0) AS prod_cube, NVL(x.tote_cnt, 0) AS tote_cnt,
             NVL(x.box_cnt, 0) AS box_cnt, NVL(x.bag_cnt, 0) AS bag_cnt
        FROM (SELECT y.strtg_id, y.depart_ts, y.load_num, y.stop_num, y.cust_id, y.eta_ts, y.manctb, y.totctb, y.qty_alloc,
                     y.prod_wt, y.prod_cube, y.tote_cnt, y.box_cnt, y.bag_cnt
                FROM yy y
              UNION ALL
              -- Cumulative
              SELECT   0 AS strtg_id, z.depart_ts, z.load_num, z.stop_num, z.cust_id, z.eta_ts, z.manctb, z.totctb,
                       SUM(z.qty_alloc) AS qty_alloc, SUM(z.prod_wt) AS prod_wt, SUM(z.prod_cube) AS prod_cube,
                       SUM(z.tote_cnt) AS tote_cnt, SUM(z.box_cnt) AS box_cnt, SUM(z.bag_cnt) AS bag_cnt
                  FROM (SELECT   /*+ ORDERED */
                                 ld.depart_ts, ld.load_num, se.stop_num, se.cust_id, se.eta_ts, b.manctb, b.totctb,
                                 SUM(CASE
                                       WHEN NOT EXISTS(SELECT 1
                                                         FROM kit_item_mstr_kt1m k
                                                        WHERE k.div_part = b_div_part
                                                          AND k.comp_item_num = b.orditb) THEN b.pckqtb
                                       ELSE b.pckqtb
                                            / (SELECT MAX(k.comp_qty)
                                                 FROM kit_item_mstr_kt1m k
                                                WHERE k.div_part = b_div_part
                                                  AND k.comp_item_num = b.orditb
                                                  AND k.comp_item_num =
                                                        (SELECT MAX(k2.comp_item_num)
                                                           FROM kit_item_mstr_kt1m k2
                                                          WHERE k2.div_part = k.div_part
                                                            AND k2.kit_typ = k.kit_typ
                                                            AND k2.item_num = k.item_num))
                                     END
                                    ) AS qty_alloc,
                                 SUM(NVL(e.wghte, .01) * b.pckqtb) AS prod_wt, SUM(NVL(e.cubee, .01) * b.pckqtb) AS prod_cube,
                                 mc.tote_cnt, mc.box_cnt, mc.bag_cnt
                            FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, sawp505e e, mclp200b t,
                                 (SELECT   c.loadc, c.stopc, c.depdtc, c.manctc, c.totctc, SUM(c.totsmc) AS tote_cnt,
                                           SUM(c.boxsmc) AS box_cnt, SUM(c.bagsmc) AS bag_cnt
                                      FROM mclp370c c
                                     WHERE c.div_part = b_div_part
                                       AND c.llr_date = b_llr_dt - DATE '1900-02-28'
                                  GROUP BY c.loadc, c.stopc, c.depdtc, c.manctc, c.totctc) mc
                           WHERE ld.div_part = b_div_part
                             AND ld.llr_dt = b_llr_dt
                             AND a.div_part = ld.div_part
                             AND a.load_depart_sid = ld.load_depart_sid
                             AND a.excptn_sw = 'N'
                             AND se.div_part = a.div_part
                             AND se.load_depart_sid = a.load_depart_sid
                             AND se.cust_id = a.custa
                             AND b.div_part = a.div_part
                             AND b.ordnob = a.ordnoa
                             AND b.excptn_sw = 'N'
                             AND b.statb IN('R', 'A')
                             AND e.iteme = b.itemnb
                             AND e.uome = b.sllumb
                             AND mc.loadc = ld.load_num
                             AND mc.stopc = se.stop_num
                             AND mc.depdtc = TRUNC(ld.depart_ts) - DATE '1900-02-28'
                             AND NVL(mc.manctc, '000') = NVL(b.manctb, '000')
                             AND NVL(mc.totctc, '000') = NVL(b.totctb, '000')
                             AND t.div_part(+) = b.div_part
                             AND t.totctb(+) = b.totctb
                        GROUP BY ld.depart_ts, ld.load_num, se.stop_num, se.cust_id, se.eta_ts, b.manctb, b.totctb,
                                 mc.tote_cnt, mc.box_cnt, mc.bag_cnt
                        UNION ALL
                        SELECT y.depart_ts, y.load_num, y.stop_num, y.cust_id, y.eta_ts, y.manctb, y.totctb, y.qty_alloc,
                               y.prod_wt, y.prod_cube, y.tote_cnt, y.box_cnt, y.bag_cnt
                          FROM yy y) z
              GROUP BY z.depart_ts, z.load_num, z.stop_num, z.cust_id, z.eta_ts, z.manctb, z.totctb) x;

    CURSOR l_cur_tote_final(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) RETURN l_rt_tote IS
      SELECT   0 AS strtg_id, ld.llr_dt - DATE '1900-02-28' AS depart_dt, ld.load_num, se.stop_num, se.cust_id,
               TRUNC(se.eta_ts) - DATE '1900-02-28' AS eta_dt, TO_NUMBER(TO_CHAR(se.eta_ts, 'HH24MI')) AS eta_tm,
               NVL(b.manctb, '000') AS mfst_catg, NVL(b.totctb, '000') AS tote_catg,
               NVL(SUM(CASE
                         WHEN NOT EXISTS(SELECT 1
                                           FROM kit_item_mstr_kt1m k
                                          WHERE k.div_part = ld.div_part
                                            AND k.comp_item_num = b.orditb) THEN b.pckqtb
                         ELSE b.pckqtb
                              / (SELECT MAX(k.comp_qty)
                                   FROM kit_item_mstr_kt1m k
                                  WHERE k.div_part = ld.div_part
                                    AND k.comp_item_num = b.orditb
                                    AND k.comp_item_num = (SELECT MAX(k2.comp_item_num)
                                                             FROM kit_item_mstr_kt1m k2
                                                            WHERE k2.div_part = k.div_part
                                                              AND k2.kit_typ = k.kit_typ
                                                              AND k2.item_num = k.item_num))
                       END
                      ),
                   0
                  ) AS qty_alloc,
               NVL(SUM(NVL(e.wghte, .01) * b.pckqtb), 0) prod_wgt, NVL(SUM(NVL(e.cubee, .01) * b.pckqtb), 0) prod_cube,
               mc.tote_cnt, mc.box_cnt, mc.bag_cnt
          FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, sawp505e e, mclp200b t,
               (SELECT   c.loadc, c.stopc, c.depdtc, c.manctc, c.totctc, SUM(c.totsmc) AS tote_cnt,
                         SUM(c.boxsmc) AS box_cnt, SUM(c.bagsmc) AS bag_cnt
                    FROM mclp370c c
                   WHERE c.div_part = b_div_part
                     AND c.llr_date = b_llr_dt - DATE '1900-02-28'
                GROUP BY c.loadc, c.stopc, c.depdtc, c.manctc, c.totctc) mc
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.excptn_sw = 'N'
           AND b.statb IN('R', 'A')
           AND e.iteme = b.itemnb
           AND e.uome = b.sllumb
           AND mc.loadc = ld.load_num
           AND mc.stopc = se.stop_num
           AND mc.depdtc = TRUNC(ld.depart_ts) - DATE '1900-02-28'
           AND NVL(mc.manctc, '000') = NVL(b.manctb, '000')
           AND NVL(mc.totctc, '000') = NVL(b.totctb, '000')
           AND t.div_part(+) = b.div_part
           AND t.totctb(+) = b.totctb
      GROUP BY ld.div_part, ld.llr_dt, ld.load_num, se.stop_num, se.cust_id, se.eta_ts, b.manctb, b.totctb, mc.tote_cnt,
               mc.box_cnt, mc.bag_cnt;

    PROCEDURE load_tbl_sp(
      i_r_tote  IN  l_rt_tote
    ) IS
      l_c_llr_num  CONSTANT NUMBER := i_llr_dt - DATE '1900-02-28';
    BEGIN
      MERGE INTO mclane_manifest_rpts r
           USING (SELECT d.div_part
                    FROM div_mstr_di1d d
                   WHERE d.div_id = i_div) x
              ON (    r.div_part = x.div_part
                  AND r.create_ts = i_create_ts
                  AND r.strategy_id = i_r_tote.strtg_id
                  AND r.llr_date = l_c_llr_num
                  AND r.load_num = i_r_tote.load_num
                  AND r.departure_date = i_r_tote.depart_dt
                  AND r.stop_num = i_r_tote.stop_num
                  AND r.eta_date = i_r_tote.eta_dt
                  AND r.eta_time = i_r_tote.eta_tm
                  AND r.cust_num = i_r_tote.cust_id
                  AND r.manifest_cat = i_r_tote.mfst_catg
                  AND r.tote_cat = i_r_tote.tote_catg)
        WHEN MATCHED THEN
          UPDATE
             SET qty_alloc = NVL(qty_alloc, 0) + i_r_tote.qty_alloc,
                 product_weight = NVL(product_weight, 0) + i_r_tote.prod_wgt,
                 product_cube = NVL(product_cube, 0) + i_r_tote.prod_cube,
                 box_count = NVL(box_count, 0) + i_r_tote.box_cnt, bag_count = NVL(bag_count, 0) + i_r_tote.bag_cnt,
                 tote_count = NVL(tote_count, 0) + i_r_tote.tote_cnt
        WHEN NOT MATCHED THEN
          INSERT(div_part, create_ts, strategy_id, llr_date, load_num, stop_num, eta_date, eta_time, manifest_cat,
                 cust_num, tote_cat, tote_count, box_count, bag_count, departure_date, qty_alloc, product_weight,
                 product_cube)
          VALUES(x.div_part, i_create_ts, i_r_tote.strtg_id, l_c_llr_num, i_r_tote.load_num, i_r_tote.stop_num,
                 i_r_tote.eta_dt, i_r_tote.eta_tm, i_r_tote.mfst_catg, i_r_tote.cust_id, i_r_tote.tote_catg,
                 i_r_tote.tote_cnt, i_r_tote.box_cnt, i_r_tote.bag_cnt, i_r_tote.depart_dt, i_r_tote.qty_alloc,
                 i_r_tote.prod_wgt, i_r_tote.prod_cube);
    END load_tbl_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'StrtgId', i_strtg_id);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_strtg_id = 0 THEN
      logs.dbg('Build Tote Final Report');
      FOR l_r_tote IN l_cur_tote_final(l_div_part, i_llr_dt) LOOP
        load_tbl_sp(l_r_tote);
      END LOOP;
    ELSE
      l_t_no_bags_corps := op_parms_pk.parms_for_val_fn(l_div_part, op_const_pk.prm_no_bags, 'Y', 3);
      logs.dbg('Build Tote Forecast Report');
      FOR l_r_tote IN l_cur_tote_forecast(l_div_part, i_llr_dt, i_strtg_id, l_t_no_bags_corps) LOOP
        load_tbl_sp(l_r_tote);
      END LOOP;
    END IF;   -- i_strtg_id = 0

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END build_report_table_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOADING_MANIFEST_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 03/19/02 | JUSTANI | Added Bag Counts to Report
  || 04/22/02 | rhalpai | Changed cursors to reflect only loads that are being
  ||                    | billed in that release.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 03/08/12 | rhalpai | Change logic to use parm to indicate whether report
  ||                    | will be cumulative. PIR10845
  || 03/01/16 | rhalpai | Change to use new OP_CONST_PK and OP_PARMS_PK.VAL_FN.
  ||                    | PIR15427
  || 11/03/16 | jxpazho | Change the format of the page number in the header to
  ||                    | accommodate 4 digit numbers. PIR 016911
  || 07/01/19 | rhalpai | Add column for Peco pallets. PIR19620
  ||----------------------------------------------------------------------------
  */
  PROCEDURE loading_manifest_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm        := 'OP_MANIFEST_REPORTS_PK.LOADING_MANIFEST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE                 := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_cumulative_sw      VARCHAR2(1);
    l_save_load          VARCHAR2(4)          := '    ';
    l_save_stop_num      NUMBER(7)            := 0;
    l_wrk_field          VARCHAR2(10);
    l_wrk_tote_units     PLS_INTEGER;
    l_wrk_tote_cnt       PLS_INTEGER;
    l_wrk_box_cnt        PLS_INTEGER;
    l_wrk_bag_cnt        PLS_INTEGER;
    l_row_cnt            PLS_INTEGER;
    l_lm_h3_eta_dt       VARCHAR2(8);
    l_lm_h3_eta_tm       VARCHAR2(5);
    l_lm_h5_phone        VARCHAR2(12);
    l_lm_m1_mfst_units   VARCHAR2(7);
    l_lm_m1_mfst_cases   VARCHAR2(7);
    l_lm_m1_mfst_totes   VARCHAR2(4);
    l_lm_m1_mfst_boxes   VARCHAR2(4);
    l_lm_m1_mfst_bags    VARCHAR2(4);
    l_lm_m1_mfst_wgt     VARCHAR2(8);
    l_lm_m1_mfst_cube    VARCHAR2(7);
    l_lm_t1_tote_descr   VARCHAR2(21);
    l_lm_t1_tote_units   VARCHAR2(7);
    l_lm_t1_tote_totes   VARCHAR2(4);
    l_lm_t1_tote_boxes   VARCHAR2(4);
    l_lm_t1_tote_bags    VARCHAR2(4);
    l_stop_units         PLS_INTEGER          := 0;
    l_load_units         PLS_INTEGER          := 0;
    l_stop_cases         PLS_INTEGER          := 0;
    l_load_cases         PLS_INTEGER          := 0;
    l_stop_totes         PLS_INTEGER          := 0;
    l_load_totes         PLS_INTEGER          := 0;
    l_stop_boxes         PLS_INTEGER          := 0;
    l_load_boxes         PLS_INTEGER          := 0;
    l_stop_bags          PLS_INTEGER          := 0;
    l_load_bags          PLS_INTEGER          := 0;
    l_stop_wgt           NUMBER(11, 1)        := 0;
    l_load_wgt           NUMBER(11, 1)        := 0;
    l_stop_cube          NUMBER(11, 1)        := 0;
    l_load_cube          NUMBER(11, 1)        := 0;
    l_lm_totals_descr    VARCHAR2(12);
    l_lm_totals_units    VARCHAR2(7)          := RPAD(' ', 7);
    l_lm_totals_cases    VARCHAR2(7)          := RPAD(' ', 7);
    l_lm_totals_totes    VARCHAR2(4)          := RPAD(' ', 4);
    l_lm_totals_boxes    VARCHAR2(4)          := RPAD(' ', 4);
    l_lm_totals_bags     VARCHAR2(4)          := RPAD(' ', 4);
    l_lm_totals_wgt      VARCHAR2(8)          := RPAD(' ', 8);
    l_lm_totals_cube     VARCHAR2(7)          := RPAD(' ', 7);

    CURSOR l_cur_mfst(
      b_create_ts      DATE,
      b_div_part       NUMBER,
      b_llr_dt         NUMBER,
      b_cumulative_sw  VARCHAR2
    ) IS
      SELECT   rpts.load_num, rpts.stop_num, rpts.manifest_cat AS mfst_catg,
               NVL(c.descc, 'INVALID MANIFEST CATEGORY') AS descr, NVL(rpts.eta_time, 0) AS eta_tm,
               NVL(rpts.eta_date, 0) AS eta_dt, NVL(rpts.cust_num, '00000000') AS cbr_cust,
               NVL(x.mccusb, '000000') AS mcl_cust, NVL(cust.namec, 'NAME UNKNOWN') AS cust_nm,
               NVL(cust.cnnamc, 'CONTACT NOT FOUND') AS cntct, NVL(cust.cnphnc, '0000000000') AS phone,
               NVL(cust.shad1c, ' ') AS addr, NVL(cust.shpctc, ' ') AS city, NVL(cust.shpstc, '  ') AS st,
               NVL(cust.shpzpc, '000000000') AS zip,
               NVL(SUM(DECODE(NVL(rpts.tote_count, 0) + NVL(rpts.box_count, 0) + NVL(rpts.bag_count, 0),
                              0, rpts.qty_alloc,
                              0
                             )
                      ),
                   0
                  ) AS case_cnt,
               NVL(SUM(rpts.product_weight), 0) AS prod_wgt,
               NVL(SUM(DECODE(rpts.tote_count, 0, product_cube, rpts.tote_count * b.outerb)), 0) AS prod_cube
          FROM mclane_manifest_rpts rpts, mclp210c c, mclp200b b, sysp200c cust, mclp020b x,
               (SELECT   r2.create_ts, r2.llr_date, r2.load_num
                    FROM mclane_manifest_rpts r2
                   WHERE r2.div_part = b_div_part
                     AND r2.strategy_id > 0
                     AND r2.create_ts = b_create_ts
                     AND r2.llr_date = b_llr_dt
                GROUP BY r2.create_ts, r2.llr_date, r2.load_num) rpt2
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.strategy_id =(CASE
                                    WHEN b_cumulative_sw = 'Y' THEN 0
                                    WHEN(    b_cumulative_sw = 'N'
                                         AND rpts.strategy_id > 0) THEN rpts.strategy_id
                                  END
                                 )
           AND x.div_part = rpts.div_part
           AND x.custb = rpts.cust_num
           AND cust.div_part = rpts.div_part
           AND cust.acnoc = rpts.cust_num
           AND rpt2.load_num = rpts.load_num
           AND c.div_part(+) = rpts.div_part
           AND c.manctc(+) = rpts.manifest_cat
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
      GROUP BY rpts.load_num, rpts.stop_num, c.seqc, rpts.manifest_cat, c.descc, rpts.eta_time, rpts.eta_date,
               rpts.cust_num, x.mccusb, cust.namec, cust.cnnamc, cust.cnphnc, cust.shad1c, cust.shpctc, cust.shpstc,
               cust.shpzpc
      ORDER BY rpts.load_num, rpts.stop_num, c.seqc, rpts.manifest_cat;

    CURSOR l_cur_tote(
      b_create_ts      DATE,
      b_div_part       NUMBER,
      b_llr_dt         NUMBER,
      b_load_num       VARCHAR2,
      b_stop_num       NUMBER,
      b_mfst_catg      VARCHAR2,
      b_cumulative_sw  VARCHAR2
    ) IS
      SELECT   NVL(b.descb, 'INVALID TOTE CATEGORY') AS descr, NVL(SUM(rpts.qty_alloc), 0) AS unit_cnt,
               NVL(SUM(rpts.tote_count), 0) AS tote_cnt, NVL(SUM(rpts.box_count), 0) AS box_cnt,
               NVL(SUM(rpts.bag_count), 0) AS bag_cnt
          FROM mclane_manifest_rpts rpts, mclp200b b,
               (SELECT   r2.create_ts, r2.llr_date, r2.load_num
                    FROM mclane_manifest_rpts r2
                   WHERE r2.div_part = b_div_part
                     AND r2.strategy_id > 0
                     AND r2.create_ts = b_create_ts
                     AND r2.llr_date = b_llr_dt
                GROUP BY r2.create_ts, r2.llr_date, r2.load_num) rpt2
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.load_num = b_load_num
           AND rpts.stop_num = b_stop_num
           AND rpts.manifest_cat = b_mfst_catg
           AND rpts.strategy_id =(CASE
                                    WHEN b_cumulative_sw = 'Y' THEN 0
                                    WHEN(    b_cumulative_sw = 'N'
                                         AND rpts.strategy_id > 0) THEN rpts.strategy_id
                                  END
                                 )
           AND (   rpts.tote_count > 0
                OR rpts.box_count > 0
                OR rpts.bag_count > 0)
           AND rpt2.load_num = rpts.load_num
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
      GROUP BY rpts.tote_cat, b.descb
      ORDER BY b.descb, rpts.tote_cat;

    l_r_tote             l_cur_tote%ROWTYPE;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_lm_h1_ln  VARCHAR2(133);
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line');
        l_lm_h1_ln := '1'
                      || RPAD('LOADING MANIFEST  [' ||(CASE
                                                         WHEN l_cumulative_sw = 'N' THEN 'NON-'
                                                       END) || 'CUMULATIVE]', 44)
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_lm_h1_ln);
        util.append(l_t_rpt_lns, g_lm_h2_ln);
        util.append(l_t_rpt_lns, g_lm_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, g_lm_h4_ln);
        util.append(l_t_rpt_lns, g_lm_h5_ln);
        util.append(l_t_rpt_lns, g_lm_h6_ln);
        util.append(l_t_rpt_lns, g_lm_h7_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, g_lm_h8_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 11;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_lm_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'MANIFEST' THEN
        logs.dbg('Add Manifest Line');
        util.append(l_t_rpt_lns, g_lm_m1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'MANIFEST'

      IF i_ln_typ = 'TOTE' THEN
        logs.dbg('Add Tote Line');
        util.append(l_t_rpt_lns, g_lm_t1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTE'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD01';
    g_file_nm := RPAD(i_div, 2) || '_OPLD01_LOADING_MANIFEST_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    l_cumulative_sw := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_opld01_cumulative), 'Y');
    g_lm_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Build Report Detail Cursor');
    <<manifest_detail_loop>>
    FOR l_r_mfst IN l_cur_mfst(i_create_ts, l_div_part, i_llr_num, l_cumulative_sw) LOOP
      logs.dbg('Check for different Load / Stop');

      IF (   l_r_mfst.load_num <> l_save_load
          OR l_r_mfst.stop_num <> l_save_stop_num) THEN
        IF g_cnt = 1 THEN
          l_save_load := l_r_mfst.load_num;
          l_save_stop_num := l_r_mfst.stop_num;
        END IF;   -- g_cnt = 1

        IF g_cnt <> 1 THEN
          logs.dbg('Format Stop Totals');
          l_lm_totals_descr := 'STOP TOTALS:';
          l_lm_totals_units := LPAD(TO_CHAR(l_stop_units, 'FM999,999'), 7);
          l_lm_totals_cases := LPAD(TO_CHAR(l_stop_cases, 'FM999,999'), 7);
          l_lm_totals_totes := LPAD(TO_CHAR(l_stop_totes, 'FM9999'), 4);
          l_lm_totals_boxes := LPAD(TO_CHAR(l_stop_boxes, 'FM9999'), 4);
          l_lm_totals_bags := LPAD(TO_CHAR(l_stop_bags, 'FM9999'), 4);
          l_lm_totals_wgt := LPAD(TO_CHAR(l_stop_wgt, 'FM99,999.0'), 8);
          l_lm_totals_cube := LPAD(TO_CHAR(l_stop_cube, 'FM9,999.0'), 7);
          g_lm_ttls_ln := ' '
                          || l_lm_totals_descr
                          || RPAD(' ', 10)
                          || l_lm_totals_units
                          || l_lm_totals_cases
                          || ' '
                          || l_lm_totals_totes
                          || l_lm_totals_boxes
                          || RPAD(' ', 15)
                          || l_lm_totals_bags
                          || l_lm_totals_wgt
                          || ' '
                          || l_lm_totals_cube;
          logs.dbg('Add Stop Totals');
          add_sp('TOTALS');
          l_stop_units := 0;
          l_stop_cases := 0;
          l_stop_totes := 0;
          l_stop_boxes := 0;
          l_stop_bags := 0;
          l_stop_wgt := 0;
          l_stop_cube := 0;

          IF l_r_mfst.load_num <> l_save_load THEN
            logs.dbg('Format Load Totals');
            l_lm_totals_descr := 'LOAD TOTALS:';
            l_lm_totals_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
            l_lm_totals_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
            l_lm_totals_totes := LPAD(TO_CHAR(l_load_totes, 'FM9999'), 4);
            l_lm_totals_boxes := LPAD(TO_CHAR(l_load_boxes, 'FM9999'), 4);
            l_lm_totals_bags := LPAD(TO_CHAR(l_load_bags, 'FM9999'), 4);
            l_lm_totals_wgt := LPAD(TO_CHAR(l_load_wgt, 'FM99,999.0'), 8);
            l_lm_totals_cube := LPAD(TO_CHAR(l_load_cube, 'FM9,999.0'), 7);
            g_lm_ttls_ln := ' '
                            || l_lm_totals_descr
                            || RPAD(' ', 10)
                            || l_lm_totals_units
                            || l_lm_totals_cases
                            || ' '
                            || l_lm_totals_totes
                            || l_lm_totals_boxes
                            || RPAD(' ', 15)
                            || l_lm_totals_bags
                            || l_lm_totals_wgt
                            || ' '
                            || l_lm_totals_cube;
            logs.dbg('Add Load Totals');
            add_sp('TOTALS');
            l_load_units := 0;
            l_load_cases := 0;
            l_load_totes := 0;
            l_load_boxes := 0;
            l_load_bags := 0;
            l_load_wgt := 0;
            l_load_cube := 0;
            l_save_load := l_r_mfst.load_num;
          END IF;   -- l_r_mfst.load_num <> l_save_load

          l_save_stop_num := l_r_mfst.stop_num;
          g_ln_cnt := 99;
        END IF;   -- g_cnt <> 1
      END IF;   -- l_r_mfst.load_num <> l_save_load OR l_r_mfst.stop_num <> v_save_stop_num

      IF g_ln_cnt > g_c_lns_per_pg THEN
        logs.dbg('Format H3 Line');
        l_wrk_field := TO_CHAR(DATE '1900-02-28' + l_r_mfst.eta_dt, 'YYYYMMDD');
        l_lm_h3_eta_dt := RPAD(SUBSTR(l_wrk_field, 5, 2)
                               || '/'
                               || SUBSTR(l_wrk_field, 7, 2)
                               || '/'
                               || SUBSTR(l_wrk_field, 3, 2),
                               8
                              );
        l_wrk_field := LPAD(l_r_mfst.eta_tm, 5, '0');
        l_lm_h3_eta_tm := RPAD(SUBSTR(l_wrk_field, 2, 2) || ':' || SUBSTR(l_wrk_field, 4, 2), 5);
        g_lm_h3_ln := ' '
                      || 'LOAD: '
                      || RPAD(l_r_mfst.load_num, 4)
                      || '  STOP '
                      || LPAD(l_r_mfst.stop_num, 2, '0')
                      || '  ETA: '
                      || RPAD(TO_CHAR(TO_DATE(l_lm_h3_eta_dt, 'MM/DD/YY'), 'DY'), 3)
                      || '  '
                      || l_lm_h3_eta_dt
                      || '  '
                      || l_lm_h3_eta_tm;
        logs.dbg('Format H4 Line');
        g_lm_h4_ln := ' '
                      || 'CUSTOMER: '
                      || RPAD(l_r_mfst.mcl_cust, 6)
                      || '  CBR CUST: '
                      || RPAD(l_r_mfst.cbr_cust, 8)
                      || '  '
                      || l_r_mfst.cust_nm;
        logs.dbg('Format H5 Line');
        l_wrk_field := SUBSTR(l_r_mfst.phone, 1, 10);
        l_lm_h5_phone := RPAD(SUBSTR(l_wrk_field, 1, 3)
                              || '-'
                              || SUBSTR(l_wrk_field, 4, 3)
                              || '-'
                              || SUBSTR(l_wrk_field, 7, 4),
                              12
                             );
        g_lm_h5_ln := ' ' || 'CONTACT:  ' || RPAD(l_r_mfst.cntct, 40) || '  TELEPHONE: ' || l_lm_h5_phone;
        logs.dbg('Format H6 Line');
        g_lm_h6_ln := ' ' || 'ADDRESS:  ' || l_r_mfst.addr;
        logs.dbg('Format H7 Line');
        g_lm_h7_ln := ' '
                      || '          '
                      || RPAD(l_r_mfst.city, 30)
                      || '  '
                      || RPAD(l_r_mfst.st, 2)
                      || '  '
                      || SUBSTR(l_r_mfst.zip, 1, 5);
        logs.dbg('Format H8 Line');
        g_lm_h8_ln := ' ' || 'MANIFEST CATEGORY       UNITS  CASES TOTE BOX PALT CHEP PECO BAG  WEIGHT    CUBE';
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- g_ln_cnt > g_c_lns_per_pg

      logs.dbg('Format Manifest Line');
      l_lm_m1_mfst_wgt := LPAD(TO_CHAR(l_r_mfst.prod_wgt, 'FM99,999.0'), 8);
      l_lm_m1_mfst_cube := LPAD(TO_CHAR(l_r_mfst.prod_cube, 'FM9,999.0'), 7);
      l_lm_m1_mfst_cases := RPAD(' ', 7);

      IF l_r_mfst.case_cnt > 0 THEN
        l_lm_m1_mfst_cases := LPAD(TO_CHAR(l_r_mfst.case_cnt, 'FM999,999'), 7);
      END IF;

      logs.dbg('Build TOTE Cursor');
      l_wrk_tote_cnt := 0;
      l_wrk_box_cnt := 0;
      l_wrk_bag_cnt := 0;
      l_wrk_tote_units := 0;
      l_lm_m1_mfst_units := RPAD(' ', 6);
      l_lm_m1_mfst_totes := RPAD(' ', 4);
      l_lm_m1_mfst_boxes := RPAD(' ', 4);
      l_lm_m1_mfst_bags := RPAD(' ', 4);

      -- Use the Tote Information on the Manifest Line if only One Tote Category
      SELECT COUNT(*)
        INTO l_row_cnt
        FROM mclane_manifest_rpts rpts, mclp200b b
       WHERE rpts.div_part = l_div_part
         AND rpts.create_ts = i_create_ts
         AND rpts.llr_date = i_llr_num
         AND rpts.load_num = l_r_mfst.load_num
         AND rpts.stop_num = l_r_mfst.stop_num
         AND rpts.manifest_cat = l_r_mfst.mfst_catg
         AND rpts.strategy_id = 0
         AND (   rpts.tote_count > 0
              OR rpts.box_count > 0
              OR rpts.bag_count > 0)
         AND b.div_part(+) = rpts.div_part
         AND b.totctb(+) = rpts.tote_cat;

      IF l_row_cnt = 1 THEN
        OPEN l_cur_tote(i_create_ts,
                        l_div_part,
                        i_llr_num,
                        l_r_mfst.load_num,
                        l_r_mfst.stop_num,
                        l_r_mfst.mfst_catg,
                        l_cumulative_sw
                       );

        FETCH l_cur_tote
         INTO l_r_tote;

        IF l_cur_tote%FOUND THEN
          l_wrk_tote_cnt := l_r_tote.tote_cnt;
          l_wrk_box_cnt := l_r_tote.box_cnt;
          l_wrk_bag_cnt := l_r_tote.bag_cnt;
          l_wrk_tote_units := l_r_tote.unit_cnt;
          l_lm_m1_mfst_units := LPAD(TO_CHAR(l_r_tote.unit_cnt, 'FM999,999'), 6);
          l_lm_m1_mfst_totes := LPAD(l_r_tote.tote_cnt, 4);
          l_lm_m1_mfst_boxes := LPAD(l_r_tote.box_cnt, 4);
          l_lm_m1_mfst_bags := LPAD(l_r_tote.bag_cnt, 4);
        ELSE
          l_row_cnt := 9;
        END IF;

        CLOSE l_cur_tote;
      END IF;

      g_lm_m1_ln := ' '
                    || RPAD(l_r_mfst.descr, 23)
                    || l_lm_m1_mfst_units
                    || l_lm_m1_mfst_cases
                    || ' '
                    || l_lm_m1_mfst_totes
                    || l_lm_m1_mfst_boxes
                    || RPAD(' ', 15)
                    || l_lm_m1_mfst_bags
                    || l_lm_m1_mfst_wgt
                    || ' '
                    || l_lm_m1_mfst_cube;
      logs.dbg('Add Manifest Line');
      add_sp('MANIFEST');
      l_stop_units := l_stop_units + l_wrk_tote_units;
      l_load_units := l_load_units + l_wrk_tote_units;
      l_stop_cases := l_stop_cases + l_r_mfst.case_cnt;
      l_load_cases := l_load_cases + l_r_mfst.case_cnt;
      l_stop_totes := l_stop_totes + l_wrk_tote_cnt;
      l_load_totes := l_load_totes + l_wrk_tote_cnt;
      l_stop_boxes := l_stop_boxes + l_wrk_box_cnt;
      l_load_boxes := l_load_boxes + l_wrk_box_cnt;
      l_stop_bags := l_stop_bags + l_wrk_bag_cnt;
      l_load_bags := l_load_bags + l_wrk_bag_cnt;
      l_stop_wgt := l_stop_wgt + l_r_mfst.prod_wgt;
      l_load_wgt := l_load_wgt + l_r_mfst.prod_wgt;
      l_stop_cube := l_stop_cube + l_r_mfst.prod_cube;
      l_load_cube := l_load_cube + l_r_mfst.prod_cube;

      IF l_row_cnt > 1 THEN
        <<tote_detail_loop>>
        FOR l_r_tote IN l_cur_tote(i_create_ts,
                                   l_div_part,
                                   i_llr_num,
                                   l_r_mfst.load_num,
                                   l_r_mfst.stop_num,
                                   l_r_mfst.mfst_catg,
                                   l_cumulative_sw
                                  ) LOOP
          logs.dbg('Format Tote Line');
          l_lm_t1_tote_descr := RPAD(l_r_tote.descr, 21);
          l_lm_t1_tote_units := LPAD(TO_CHAR(l_r_tote.unit_cnt, 'FM99,999'), 6);
          l_lm_t1_tote_totes := LPAD(l_r_tote.tote_cnt, 4);
          l_lm_t1_tote_boxes := LPAD(l_r_tote.box_cnt, 4);
          l_lm_t1_tote_bags := LPAD(l_r_tote.bag_cnt, 4);
          g_lm_t1_ln := ' '
                        || '  '
                        || l_lm_t1_tote_descr
                        || l_lm_t1_tote_units
                        || RPAD(' ', 8)
                        || l_lm_t1_tote_totes
                        || l_lm_t1_tote_boxes
                        || RPAD(' ', 15)
                        || l_lm_t1_tote_bags;
          logs.dbg('Add Tote Line');
          add_sp('TOTE');
          l_stop_units := l_stop_units + l_r_tote.unit_cnt;
          l_load_units := l_load_units + l_r_tote.unit_cnt;
          l_stop_totes := l_stop_totes + l_r_tote.tote_cnt;
          l_load_totes := l_load_totes + l_r_tote.tote_cnt;
          l_stop_boxes := l_stop_boxes + l_r_tote.box_cnt;
          l_load_boxes := l_load_boxes + l_r_tote.box_cnt;
          l_stop_bags := l_stop_bags + l_r_tote.bag_cnt;
          l_load_bags := l_load_bags + l_r_tote.bag_cnt;
        END LOOP tote_detail_loop;
      END IF;   -- l_row_cnt > 1
    END LOOP manifest_detail_loop;
    logs.dbg('Format Stop Totals - End');
    l_lm_totals_descr := 'STOP TOTALS:';
    l_lm_totals_units := LPAD(TO_CHAR(l_stop_units, 'FM999,999'), 7);
    l_lm_totals_cases := LPAD(TO_CHAR(l_stop_cases, 'FM999,999'), 7);
    l_lm_totals_totes := LPAD(TO_CHAR(l_stop_totes, 'FM9999'), 4);
    l_lm_totals_boxes := LPAD(TO_CHAR(l_stop_boxes, 'FM9999'), 4);
    l_lm_totals_bags := LPAD(TO_CHAR(l_stop_bags, 'FM9999'), 4);
    l_lm_totals_wgt := LPAD(TO_CHAR(l_stop_wgt, 'FM99,999.0'), 8);
    l_lm_totals_cube := LPAD(TO_CHAR(l_stop_cube, 'FM9,999.0'), 7);
    g_lm_ttls_ln := ' '
                    || l_lm_totals_descr
                    || RPAD(' ', 10)
                    || l_lm_totals_units
                    || l_lm_totals_cases
                    || ' '
                    || l_lm_totals_totes
                    || l_lm_totals_boxes
                    || RPAD(' ', 15)
                    || l_lm_totals_bags
                    || l_lm_totals_wgt
                    || ' '
                    || l_lm_totals_cube;
    logs.dbg('Add Stop Totals - End');
    add_sp('TOTALS');
    logs.dbg('Format Load Totals - End');
    l_lm_totals_descr := 'LOAD TOTALS:';
    l_lm_totals_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
    l_lm_totals_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
    l_lm_totals_totes := LPAD(TO_CHAR(l_load_totes, 'FM9999'), 4);
    l_lm_totals_boxes := LPAD(TO_CHAR(l_load_boxes, 'FM9999'), 4);
    l_lm_totals_bags := LPAD(TO_CHAR(l_load_bags, 'FM9999'), 4);
    l_lm_totals_wgt := LPAD(TO_CHAR(l_load_wgt, 'FM99,999.0'), 8);
    l_lm_totals_cube := LPAD(TO_CHAR(l_load_cube, 'FM9,999.0'), 7);
    g_lm_ttls_ln := ' '
                    || l_lm_totals_descr
                    || RPAD(' ', 10)
                    || l_lm_totals_units
                    || l_lm_totals_cases
                    || ' '
                    || l_lm_totals_totes
                    || l_lm_totals_boxes
                    || RPAD(' ', 15)
                    || l_lm_totals_bags
                    || l_lm_totals_wgt
                    || ' '
                    || l_lm_totals_cube;
    logs.dbg('Add Load Totals - End');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_tote%ISOPEN THEN
        CLOSE l_cur_tote;
      END IF;

      logs.err(lar_parm);
  END loading_manifest_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_DEPT_SUMMARY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 03/11/02 | rhalpai | Included Bag Count in the Box Count
  || 04/22/02 | rhalpai | Changed cursor to reflect only loads that are being
  ||                    | billed in that release.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 11/03/16 | jxpazho | Change the format of the page number in the header to
  ||                    | accommodate 4 digit numbers. PIR 016911
  ||----------------------------------------------------------------------------
  */
  PROCEDURE load_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MANIFEST_REPORTS_PK.LOAD_DEPT_SUMMARY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE           := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_save_load          VARCHAR2(4)    := '    ';
    l_ld_m1_mfst_descr   VARCHAR2(23)   := RPAD(' ', 23);
    l_ld_m1_units        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_cases        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_totes        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_prod_wgt     VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_m1_prod_cube    VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_m1_ttl_cube     VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_stops      VARCHAR2(3)    := RPAD(' ', 3);
    l_ld_ttls_units      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_cases      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_totes      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_wgt        VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_ttls_prod_cube  VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_ttl_cube   VARCHAR2(10)   := RPAD(' ', 10);
    l_load_units         PLS_INTEGER    := 0;
    l_load_cases         PLS_INTEGER    := 0;
    l_load_totes         PLS_INTEGER    := 0;
    l_load_boxes         PLS_INTEGER    := 0;
    l_load_wgt           NUMBER(11, 2)  := 0;
    l_load_ttl_cube      NUMBER(11, 2)  := 0;
    l_load_prod_cube     NUMBER(11, 2)  := 0;
    l_load_stop_cnt      PLS_INTEGER    := 0;

    CURSOR l_cur_mfst(
      b_create_ts  DATE,
      b_div_part   NUMBER,
      b_llr_dt     NUMBER
    ) IS
      SELECT   rpts.load_num, NVL(c.descc, 'INVALID MANIFEST CATEGORY') AS descr,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count + rpts.bag_count, 0, rpts.qty_alloc, 0)), 0)
                                                                                                           AS case_cnt,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count + rpts.bag_count, 0, 0, rpts.qty_alloc)), 0)
                                                                                                           AS unit_cnt,
               NVL(SUM(rpts.tote_count), 0) AS tote_cnt, NVL(SUM(rpts.box_count + rpts.bag_count), 0) AS box_cnt,
               NVL(SUM(rpts.product_weight), 0) AS prod_wgt, NVL(SUM(rpts.product_cube), 0) AS prod_cube,
               NVL(SUM(DECODE(rpts.tote_count, 0, rpts.product_cube, rpts.tote_count * b.outerb)), 0) AS ttl_cube
          FROM mclane_manifest_rpts rpts, mclp210c c, mclp200b b,
               (SELECT   r2.create_ts, r2.llr_date, r2.load_num
                    FROM mclane_manifest_rpts r2
                   WHERE r2.div_part = b_div_part
                     AND r2.strategy_id > 0
                     AND r2.create_ts = b_create_ts
                     AND r2.llr_date = b_llr_dt
                GROUP BY r2.create_ts, r2.llr_date, r2.load_num) rpt2
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.strategy_id = 0
           AND c.div_part(+) = rpts.div_part
           AND c.manctc(+) = rpts.manifest_cat
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
           AND rpts.load_num = rpt2.load_num
      GROUP BY rpts.load_num, c.descc, c.seqc, rpts.manifest_cat
      ORDER BY rpts.load_num, c.seqc, rpts.manifest_cat;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_ld_h1_ln             VARCHAR2(133);
      l_c_ld_h4_ln  CONSTANT VARCHAR2(133) := ' ' || LPAD(' BOXES/               PRODUCT    CUBE OF', 80);
      l_c_ld_h5_ln  CONSTANT VARCHAR2(133)
                     := ' ' || 'MANIFEST CATEGORY         UNITS   CASES ' || '  TOTES     WEIGHT      CUBE      TOTES ';
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line-1');
        l_ld_h1_ln := '1'
                      || g_heading
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_ld_h1_ln);
        util.append(l_t_rpt_lns, g_ld_h2_ln);
        util.append(l_t_rpt_lns, g_ld_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, l_c_ld_h4_ln);
        util.append(l_t_rpt_lns, l_c_ld_h5_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 7;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_ld_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'MANIFEST' THEN
        logs.dbg('Add Manifest Line');
        util.append(l_t_rpt_lns, g_ld_m1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'MANIFEST'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD02';
    g_file_nm := RPAD(i_div, 2) || '_OPLD02_LOAD_SUMMARY_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    g_heading := RPAD('LOAD DEPARTMENT SUMMARY', 44);
    g_ld_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Build Report Detail Cursor');
    FOR l_r_mfst IN l_cur_mfst(i_create_ts, l_div_part, i_llr_num) LOOP
      logs.dbg('Check for different Load / Stop');

      IF l_r_mfst.load_num <> l_save_load THEN
        IF g_cnt = 1 THEN
          l_save_load := l_r_mfst.load_num;
        END IF;   -- g_cnt = 1

        IF g_cnt <> 1 THEN
          logs.dbg('Format Load Totals1');

          SELECT COUNT(DISTINCT rpts.stop_num)
            INTO l_load_stop_cnt
            FROM mclane_manifest_rpts rpts
           WHERE rpts.div_part = l_div_part
             AND rpts.create_ts = i_create_ts
             AND rpts.llr_date = i_llr_num
             AND rpts.strategy_id = 0
             AND rpts.load_num = l_save_load;

          l_ld_ttls_stops := LPAD(TO_CHAR(l_load_stop_cnt, 'FM999'), 3);
          l_ld_ttls_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
          l_ld_ttls_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
          l_ld_ttls_totes := LPAD(TO_CHAR(l_load_totes + l_load_boxes, 'FM999,999'), 7);
          l_ld_ttls_wgt := LPAD(TO_CHAR(l_load_wgt, 'FM9999,999.09'), 11);
          l_ld_ttls_prod_cube := LPAD(TO_CHAR(l_load_prod_cube, 'FM999,999.00'), 10);
          l_ld_ttls_ttl_cube := LPAD(TO_CHAR(l_load_ttl_cube, 'FM999,999.00'), 10);
          g_ld_ttls_ln := ' '
                          || 'STOPS: '
                          || l_ld_ttls_stops
                          || '  TOTALS:     '
                          || l_ld_ttls_units
                          || ' '
                          || l_ld_ttls_cases
                          || ' '
                          || l_ld_ttls_totes
                          || l_ld_ttls_wgt
                          || ' '
                          || l_ld_ttls_prod_cube
                          || ' '
                          || l_ld_ttls_ttl_cube;
          logs.dbg('Add Load Totals');
          add_sp('TOTALS');
          l_load_units := 0;
          l_load_cases := 0;
          l_load_totes := 0;
          l_load_boxes := 0;
          l_load_wgt := 0;
          l_load_prod_cube := 0;
          l_load_ttl_cube := 0;
          l_save_load := l_r_mfst.load_num;
          g_ln_cnt := 99;
        END IF;   -- g_cnt <> 1
      END IF;   -- l_r_mfst.load_num <> l_save_load

      IF g_ln_cnt > g_c_lns_per_pg THEN
        logs.dbg('Format H3 Line');
        g_ld_h3_ln := ' ' || LPAD('LOAD: ', 43) || l_r_mfst.load_num;
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- g_ln_cnt > g_c_lns_per_pg

      logs.dbg('Format Manifest Line');
      l_ld_m1_mfst_descr := RPAD(l_r_mfst.descr, 23);
      l_ld_m1_prod_wgt := LPAD(TO_CHAR(l_r_mfst.prod_wgt, 'FM9999,999.00'), 11);
      l_ld_m1_prod_cube := LPAD(TO_CHAR(l_r_mfst.prod_cube, 'FM999,999.00'), 10);
      l_ld_m1_ttl_cube := LPAD(TO_CHAR(l_r_mfst.ttl_cube, 'FM999,999.00'), 10);
      l_ld_m1_totes := RPAD(' ', 7);

      IF (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0 THEN
        l_ld_m1_totes := LPAD(TO_CHAR(l_r_mfst.box_cnt + l_r_mfst.tote_cnt, 'FM999,999'), 7);
      END IF;   -- (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0

      l_ld_m1_units := RPAD(' ', 7);

      IF l_r_mfst.unit_cnt > 0 THEN
        l_ld_m1_units := LPAD(TO_CHAR(l_r_mfst.unit_cnt, 'FM999,999'), 7);
      END IF;   -- l_r_mfst.unit_cnt > 0

      l_ld_m1_cases := RPAD(' ', 7);

      IF l_r_mfst.case_cnt > 0 THEN
        l_ld_m1_cases := LPAD(TO_CHAR(l_r_mfst.case_cnt, 'FM999,999'), 7);
      END IF;   -- l_r_mfst.case_cnt > 0

      g_ld_m1_ln := ' '
                    || l_ld_m1_mfst_descr
                    || ' '
                    || l_ld_m1_units
                    || ' '
                    || l_ld_m1_cases
                    || ' '
                    || l_ld_m1_totes
                    || l_ld_m1_prod_wgt
                    || ' '
                    || l_ld_m1_prod_cube
                    || ' '
                    || l_ld_m1_ttl_cube;
      logs.dbg('Add Manifest Line');
      add_sp('MANIFEST');
      l_load_units := l_load_units + l_r_mfst.unit_cnt;
      l_load_cases := l_load_cases + l_r_mfst.case_cnt;
      l_load_totes := l_load_totes + l_r_mfst.tote_cnt;
      l_load_boxes := l_load_boxes + l_r_mfst.box_cnt;
      l_load_wgt := l_load_wgt + l_r_mfst.prod_wgt;
      l_load_prod_cube := l_load_prod_cube + l_r_mfst.prod_cube;
      l_load_ttl_cube := l_load_ttl_cube + l_r_mfst.ttl_cube;
    END LOOP;
    logs.dbg('Format Load Totals2');

    SELECT COUNT(DISTINCT stop_num)
      INTO l_load_stop_cnt
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = i_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id = 0
       AND rpts.load_num = l_save_load;

    l_ld_ttls_stops := LPAD(TO_CHAR(l_load_stop_cnt, 'FM999'), 3);
    l_ld_ttls_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
    l_ld_ttls_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
    l_ld_ttls_totes := LPAD(TO_CHAR(l_load_totes + l_load_boxes, 'FM999,999'), 7);
    l_ld_ttls_wgt := LPAD(TO_CHAR(l_load_wgt, 'FM9999,999.00'), 11);
    l_ld_ttls_prod_cube := LPAD(TO_CHAR(l_load_prod_cube, 'FM999,999.00'), 10);
    l_ld_ttls_ttl_cube := LPAD(TO_CHAR(l_load_ttl_cube, 'FM999,999.00'), 10);
    g_ld_ttls_ln := ' '
                    || 'STOPS: '
                    || l_ld_ttls_stops
                    || '  TOTALS:     '
                    || l_ld_ttls_units
                    || ' '
                    || l_ld_ttls_cases
                    || ' '
                    || l_ld_ttls_totes
                    || l_ld_ttls_wgt
                    || ' '
                    || l_ld_ttls_prod_cube
                    || ' '
                    || l_ld_ttls_ttl_cube;
    logs.dbg('Add Load Totals');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_dept_summary_sp;

  /*
  ||----------------------------------------------------------------------------
  || RELEASE_DEPT_SUMMARY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 03/11/02 | rhalpai | Included Bag Count in the Box Count
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 04/03/12 | rhalpai | Convert to use new Strategy table. PIR10651
  || 11/03/16 | jxpazho | Change the format of the page number in the header to
  ||                    | accommodate 4 digit numbers. PIR 016911
  ||----------------------------------------------------------------------------
  */
  PROCEDURE release_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MANIFEST_REPORTS_PK.RELEASE_DEPT_SUMMARY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE           := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_save_load          VARCHAR2(4)    := '    ';
    l_ld_m1_mfst_descr   VARCHAR2(23)   := RPAD(' ', 23);
    l_ld_m1_units        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_cases        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_totes        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_prod_wgt     VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_m1_prod_cube    VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_m1_ttl_cube     VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_stops      VARCHAR2(3)    := RPAD(' ', 3);
    l_ld_ttls_units      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_cases      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_totes      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_wgt        VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_ttls_prod_cube  VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_ttl_cube   VARCHAR2(10)   := RPAD(' ', 10);
    l_load_units         PLS_INTEGER    := 0;
    l_load_cases         PLS_INTEGER    := 0;
    l_load_totes         PLS_INTEGER    := 0;
    l_load_boxes         PLS_INTEGER    := 0;
    l_load_weight        NUMBER(11, 2)  := 0;
    l_load_ttl_cube      NUMBER(11, 2)  := 0;
    l_load_prod_cube     NUMBER(11, 2)  := 0;
    l_load_stop_cnt      PLS_INTEGER    := 0;

    CURSOR l_cur_mfst(
      b_create_ts  DATE,
      b_div_part   NUMBER,
      b_llr_dt     NUMBER
    ) IS
      SELECT   NVL(rs.strtg_nm, 'STRATEGY NOT FOUND') AS strtg_nm, rpts.load_num,
               NVL(c.descc, 'INVALID MANIFEST CATEGORY') AS descr,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, rpts.qty_alloc, 0)), 0) AS case_cnt,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, 0, rpts.qty_alloc)), 0) AS unit_cnt,
               NVL(SUM(rpts.tote_count), 0) AS tote_cnt, NVL(SUM(rpts.box_count + rpts.bag_count), 0) AS box_cnt,
               NVL(SUM(rpts.product_weight), 0) AS prod_wgt, NVL(SUM(rpts.product_cube), 0) AS prod_cube,
               NVL(SUM(DECODE(rpts.tote_count, 0, rpts.product_cube, rpts.tote_count * b.outerb)), 0) AS ttl_cube
          FROM mclane_manifest_rpts rpts, mclp210c c, mclp200b b, rlse_strtg_op4t rs
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.strategy_id > 0
           AND c.div_part(+) = rpts.div_part
           AND c.manctc(+) = rpts.manifest_cat
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
           AND rs.div_part(+) = rpts.div_part
           AND rs.strtg_id(+) = rpts.strategy_id
      GROUP BY rpts.load_num, rs.strtg_nm, c.descc, c.seqc, rpts.manifest_cat
      ORDER BY rpts.load_num, c.seqc, rpts.manifest_cat;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_ld_h1_ln             VARCHAR2(133);
      l_c_ld_h4_ln  CONSTANT VARCHAR2(133) := ' ' || LPAD(' BOXES/               PRODUCT    CUBE OF', 80);
      l_c_ld_h5_ln  CONSTANT VARCHAR2(133)
                     := ' ' || 'MANIFEST CATEGORY         UNITS   CASES ' || '  TOTES     WEIGHT      CUBE      TOTES ';
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line-1');
        l_ld_h1_ln := '1'
                      || g_heading
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_ld_h1_ln);
        util.append(l_t_rpt_lns, g_ld_h2_ln);
        util.append(l_t_rpt_lns, g_ld_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, l_c_ld_h4_ln);
        util.append(l_t_rpt_lns, l_c_ld_h5_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 7;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_ld_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'MANIFEST' THEN
        logs.dbg('Add Manifest Line');
        util.append(l_t_rpt_lns, g_ld_m1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'MANIFEST'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD04';
    g_file_nm := RPAD(i_div, 2) || '_OPLD04_RELEASE_SUMMARY_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    g_heading := RPAD('RELEASE DEPARTMENT SUMMARY', 44);
    g_ld_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Build Report Detail Cursor');
    FOR l_r_mfst IN l_cur_mfst(i_create_ts, l_div_part, i_llr_num) LOOP
      logs.dbg('Check for different Load / Stop');

      IF l_r_mfst.load_num <> l_save_load THEN
        IF g_cnt = 1 THEN
          l_save_load := l_r_mfst.load_num;
        END IF;   -- g_cnt = 1

        IF g_cnt <> 1 THEN
          logs.dbg('Format Load Totals');

          SELECT COUNT(DISTINCT stop_num)
            INTO l_load_stop_cnt
            FROM mclane_manifest_rpts rpts
           WHERE rpts.div_part = l_div_part
             AND rpts.create_ts = i_create_ts
             AND rpts.llr_date = i_llr_num
             AND rpts.strategy_id > 0
             AND l_save_load = rpts.load_num;

          l_ld_ttls_stops := LPAD(TO_CHAR(l_load_stop_cnt, 'FM999'), 3);
          l_ld_ttls_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
          l_ld_ttls_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
          l_ld_ttls_totes := LPAD(TO_CHAR(l_load_totes + l_load_boxes, 'FM999,999'), 7);
          l_ld_ttls_wgt := LPAD(TO_CHAR(l_load_weight, 'FM9999,999.00'), 11);
          l_ld_ttls_prod_cube := LPAD(TO_CHAR(l_load_prod_cube, 'FM999,999.00'), 10);
          l_ld_ttls_ttl_cube := LPAD(TO_CHAR(l_load_ttl_cube, 'FM999,999.00'), 10);
          g_ld_ttls_ln := ' '
                          || 'STOPS: '
                          || l_ld_ttls_stops
                          || '  TOTALS:     '
                          || l_ld_ttls_units
                          || ' '
                          || l_ld_ttls_cases
                          || ' '
                          || l_ld_ttls_totes
                          || l_ld_ttls_wgt
                          || ' '
                          || l_ld_ttls_prod_cube
                          || ' '
                          || l_ld_ttls_ttl_cube;
          logs.dbg('Add Load Totals');
          add_sp('TOTALS');
          l_load_units := 0;
          l_load_cases := 0;
          l_load_totes := 0;
          l_load_boxes := 0;
          l_load_weight := 0;
          l_load_prod_cube := 0;
          l_load_ttl_cube := 0;
          l_save_load := l_r_mfst.load_num;
          g_ln_cnt := 99;
        END IF;   -- g_cnt <> 1
      END IF;   -- l_r_mfst.load_num <> l_save_load

      IF g_ln_cnt > g_c_lns_per_pg THEN
        logs.dbg('Format H3 Line');
        g_ld_h3_ln := ' ' || 'RELEASE: ' || RPAD(l_r_mfst.strtg_nm, 27) || ' LOAD: ' || l_r_mfst.load_num;
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- g_ln_cnt > g_c_lns_per_pg

      logs.dbg('Format Manifest Line');
      l_ld_m1_mfst_descr := RPAD(l_r_mfst.descr, 23);
      l_ld_m1_prod_wgt := LPAD(TO_CHAR(l_r_mfst.prod_wgt, 'FM9999,999.00'), 11);
      l_ld_m1_prod_cube := LPAD(TO_CHAR(l_r_mfst.prod_cube, 'FM999,999.00'), 10);
      l_ld_m1_ttl_cube := LPAD(TO_CHAR(l_r_mfst.ttl_cube, 'FM999,999.00'), 10);
      l_ld_m1_totes := LPAD(' ', 7);

      IF (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0 THEN
        l_ld_m1_totes := LPAD(TO_CHAR(l_r_mfst.box_cnt + l_r_mfst.tote_cnt, 'FM999,999'), 7);
      END IF;   -- (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0

      l_ld_m1_units := RPAD(' ', 7);

      IF l_r_mfst.unit_cnt > 0 THEN
        l_ld_m1_units := LPAD(TO_CHAR(l_r_mfst.unit_cnt, 'FM999,999'), 7);
      END IF;   -- l_r_mfst.unit_cnt > 0

      l_ld_m1_cases := RPAD(' ', 7);

      IF l_r_mfst.case_cnt > 0 THEN
        l_ld_m1_cases := LPAD(TO_CHAR(l_r_mfst.case_cnt, 'FM999,999'), 7);
      END IF;   -- l_r_mfst.case_cnt > 0

      g_ld_m1_ln := ' '
                    || l_ld_m1_mfst_descr
                    || ' '
                    || l_ld_m1_units
                    || ' '
                    || l_ld_m1_cases
                    || ' '
                    || l_ld_m1_totes
                    || l_ld_m1_prod_wgt
                    || ' '
                    || l_ld_m1_prod_cube
                    || ' '
                    || l_ld_m1_ttl_cube;
      logs.dbg('Add Manifest Line');
      add_sp('MANIFEST');
      l_load_units := l_load_units + l_r_mfst.unit_cnt;
      l_load_cases := l_load_cases + l_r_mfst.case_cnt;
      l_load_totes := l_load_totes + l_r_mfst.tote_cnt;
      l_load_boxes := l_load_boxes + l_r_mfst.box_cnt;
      l_load_weight := l_load_weight + l_r_mfst.prod_wgt;
      l_load_prod_cube := l_load_prod_cube + l_r_mfst.prod_cube;
      l_load_ttl_cube := l_load_ttl_cube + l_r_mfst.ttl_cube;
    END LOOP;
    logs.dbg('Format Load Totals');

    SELECT COUNT(DISTINCT rpts.stop_num)
      INTO l_load_stop_cnt
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = i_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id > 0
       AND l_save_load = rpts.load_num;

    l_ld_ttls_stops := LPAD(TO_CHAR(l_load_stop_cnt, 'FM999'), 3);
    l_ld_ttls_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
    l_ld_ttls_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
    l_ld_ttls_totes := LPAD(TO_CHAR(l_load_totes + l_load_boxes, 'FM999,999'), 7);
    l_ld_ttls_wgt := LPAD(TO_CHAR(l_load_weight, 'FM9999,999.00'), 11);
    l_ld_ttls_prod_cube := LPAD(TO_CHAR(l_load_prod_cube, 'FM999,999.00'), 10);
    l_ld_ttls_ttl_cube := LPAD(TO_CHAR(l_load_ttl_cube, 'FM999,999.00'), 10);
    g_ld_ttls_ln := ' '
                    || 'STOPS: '
                    || l_ld_ttls_stops
                    || '  TOTALS:     '
                    || l_ld_ttls_units
                    || ' '
                    || l_ld_ttls_cases
                    || ' '
                    || l_ld_ttls_totes
                    || l_ld_ttls_wgt
                    || ' '
                    || l_ld_ttls_prod_cube
                    || ' '
                    || l_ld_ttls_ttl_cube;
    logs.dbg('Add Load Totals');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END release_dept_summary_sp;

  /*
  ||----------------------------------------------------------------------------
  || SUMMARY_LOAD_DEPT_SUMMARY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 03/11/02 | rhalpai | Included Bag Count in the Box Count
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 11/03/16 | jxpazho | Change the format of the page number in the header to
  ||                    | accommodate 4 digit numbers. PIR 016911
  ||----------------------------------------------------------------------------
  */
  PROCEDURE summary_load_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MANIFEST_REPORTS_PK.SUMMARY_LOAD_DEPT_SUMMARY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE           := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_ld_h3_load_cnt     VARCHAR2(4)    := RPAD(' ', 4);
    l_ld_h3_stop_cnt     VARCHAR2(5)    := RPAD(' ', 5);
    l_ld_m1_mfst_descr   VARCHAR2(23)   := RPAD(' ', 23);
    l_ld_m1_units        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_cases        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_totes        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_prod_wgt     VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_m1_prod_cube    VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_m1_ttl_cube     VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_units      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_cases      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_totes      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_wgt        VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_ttls_prod_cube  VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_ttl_cube   VARCHAR2(10)   := RPAD(' ', 10);
    l_load_units         PLS_INTEGER    := 0;
    l_load_cases         PLS_INTEGER    := 0;
    l_load_totes         PLS_INTEGER    := 0;
    l_load_boxes         PLS_INTEGER    := 0;
    l_load_wgt           NUMBER(11, 2)  := 0;
    l_load_ttl_cube      NUMBER(11, 2)  := 0;
    l_load_prod_cube     NUMBER(11, 2)  := 0;

    CURSOR l_cur_mfst(
      b_create_ts  DATE,
      b_div_part   NUMBER,
      b_llr_dt     NUMBER
    ) IS
      SELECT   NVL(c.descc, 'INVALID MANIFEST CATEGORY') AS descr,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, rpts.qty_alloc, 0)), 0) AS case_cnt,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, 0, rpts.qty_alloc)), 0) AS unit_cnt,
               NVL(SUM(rpts.tote_count), 0) AS tote_cnt, NVL(SUM(rpts.box_count + rpts.bag_count), 0) AS box_cnt,
               NVL(SUM(rpts.product_weight), 0) AS prod_wgt, NVL(SUM(rpts.product_cube), 0) AS prod_cube,
               NVL(SUM(DECODE(rpts.tote_count, 0, rpts.product_cube, rpts.tote_count * b.outerb)), 0) AS ttl_cube
          FROM mclane_manifest_rpts rpts, mclp210c c, mclp200b b
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.strategy_id = 0
           AND c.div_part(+) = rpts.div_part
           AND c.manctc(+) = rpts.manifest_cat
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
      GROUP BY c.descc, c.seqc, rpts.manifest_cat
      ORDER BY c.seqc, rpts.manifest_cat;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_ld_h1_ln             VARCHAR2(133);
      l_c_ld_h4_ln  CONSTANT VARCHAR2(133) := ' ' || LPAD(' BOXES/               PRODUCT    CUBE OF', 80);
      l_c_ld_h5_ln  CONSTANT VARCHAR2(133)
                     := ' ' || 'MANIFEST CATEGORY         UNITS   CASES ' || '  TOTES     WEIGHT      CUBE      TOTES ';
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line-1');
        l_ld_h1_ln := '1'
                      || g_heading
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_ld_h1_ln);
        util.append(l_t_rpt_lns, g_ld_h2_ln);
        util.append(l_t_rpt_lns, g_ld_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, l_c_ld_h4_ln);
        util.append(l_t_rpt_lns, l_c_ld_h5_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 7;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_ld_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'MANIFEST' THEN
        logs.dbg('Add Manifest Line');
        util.append(l_t_rpt_lns, g_ld_m1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'MANIFEST'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD03';
    g_file_nm := RPAD(i_div, 2) || '_OPLD03_LOAD_RECAP_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    g_heading := RPAD('SUMMARY LOAD DEPARTMENT SUMMARY', 44);
    g_ld_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Count of Load Numbers');

    SELECT LPAD(COUNT(DISTINCT rpts.load_num), 4)
      INTO l_ld_h3_load_cnt
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = i_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id = 0;

    logs.dbg('Count of Stop Numbers');

    SELECT LPAD(COUNT(DISTINCT rpts.load_num || rpts.stop_num), 5)
      INTO l_ld_h3_stop_cnt
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = i_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id = 0;

    logs.dbg('Build Report Detail Cursor');
    FOR l_r_mfst IN l_cur_mfst(i_create_ts, l_div_part, i_llr_num) LOOP
      IF g_ln_cnt > g_c_lns_per_pg THEN
        logs.dbg('Format H3 Line');
        g_ld_h3_ln := ' ' || LPAD('LOADS: ', 44) || l_ld_h3_load_cnt || '   STOPS: ' || l_ld_h3_stop_cnt;
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- g_ln_cnt > g_c_lns_per_pg

      logs.dbg('Format Manifest Line');
      l_ld_m1_mfst_descr := RPAD(l_r_mfst.descr, 23);
      l_ld_m1_prod_wgt := LPAD(TO_CHAR(l_r_mfst.prod_wgt, 'FM9999,999.00'), 11);
      l_ld_m1_prod_cube := LPAD(TO_CHAR(l_r_mfst.prod_cube, 'FM999,999.00'), 10);
      l_ld_m1_ttl_cube := LPAD(TO_CHAR(l_r_mfst.ttl_cube, 'FM999,999.00'), 10);
      l_ld_m1_totes := RPAD(' ', 7);

      IF (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0 THEN
        l_ld_m1_totes := LPAD(TO_CHAR(l_r_mfst.box_cnt + l_r_mfst.tote_cnt, 'FM999,999'), 7);
      END IF;   -- (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0

      l_ld_m1_units := RPAD(' ', 7);

      IF l_r_mfst.unit_cnt > 0 THEN
        l_ld_m1_units := LPAD(TO_CHAR(l_r_mfst.unit_cnt, 'FM999,999'), 7);
      END IF;   -- l_r_mfst.unit_cnt > 0

      l_ld_m1_cases := RPAD(' ', 7);

      IF l_r_mfst.case_cnt > 0 THEN
        l_ld_m1_cases := LPAD(TO_CHAR(l_r_mfst.case_cnt, 'FM999,999'), 7);
      END IF;   -- l_r_mfst.case_cnt > 0

      g_ld_m1_ln := ' '
                    || l_ld_m1_mfst_descr
                    || ' '
                    || l_ld_m1_units
                    || ' '
                    || l_ld_m1_cases
                    || ' '
                    || l_ld_m1_totes
                    || l_ld_m1_prod_wgt
                    || ' '
                    || l_ld_m1_prod_cube
                    || ' '
                    || l_ld_m1_ttl_cube;
      logs.dbg('Add Manifest Line');
      add_sp('MANIFEST');
      l_load_units := l_load_units + l_r_mfst.unit_cnt;
      l_load_cases := l_load_cases + l_r_mfst.case_cnt;
      l_load_totes := l_load_totes + l_r_mfst.tote_cnt;
      l_load_boxes := l_load_boxes + l_r_mfst.box_cnt;
      l_load_wgt := l_load_wgt + l_r_mfst.prod_wgt;
      l_load_prod_cube := l_load_prod_cube + l_r_mfst.prod_cube;
      l_load_ttl_cube := l_load_ttl_cube + l_r_mfst.ttl_cube;
    END LOOP;
    logs.dbg('Format Report Totals');
    l_ld_ttls_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
    l_ld_ttls_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
    l_ld_ttls_totes := LPAD(TO_CHAR(l_load_totes + l_load_boxes, 'FM999,999'), 7);
    l_ld_ttls_wgt := LPAD(TO_CHAR(l_load_wgt, 'FM9999,999.00'), 11);
    l_ld_ttls_prod_cube := LPAD(TO_CHAR(l_load_prod_cube, 'FM999,999.00'), 10);
    l_ld_ttls_ttl_cube := LPAD(TO_CHAR(l_load_ttl_cube, 'FM999,999.00'), 10);
    g_ld_ttls_ln := ' '
                    || RPAD('GRAND TOTALS:', 24)
                    || l_ld_ttls_units
                    || ' '
                    || l_ld_ttls_cases
                    || ' '
                    || l_ld_ttls_totes
                    || l_ld_ttls_wgt
                    || ' '
                    || l_ld_ttls_prod_cube
                    || ' '
                    || l_ld_ttls_ttl_cube;
    logs.dbg('Add Load Totals');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END summary_load_dept_summary_sp;

  /*
  ||----------------------------------------------------------------------------
  || SUMMARY_REL_DEPT_SUMMARY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 03/11/02 | rhalpai | Included Bag Count in the Box Count
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 04/03/12 | rhalpai | Convert to use new Strategy table. PIR10651
  || 11/03/16 | jxpazho | Change the format of the page number in the header to
  ||                    | accommodate 4 digit numbers. PIR 016911
  ||----------------------------------------------------------------------------
  */
  PROCEDURE summary_rel_dept_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MANIFEST_REPORTS_PK.SUMMARY_REL_DEPT_SUMMARY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE           := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_ld_h3_load_cnt     VARCHAR2(4)    := RPAD(' ', 4);
    l_ld_h3_stop_cnt     VARCHAR2(5)    := RPAD(' ', 5);
    l_ld_m1_mfst_descr   VARCHAR2(23)   := RPAD(' ', 23);
    l_ld_m1_units        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_cases        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_totes        VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_m1_prod_wgt     VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_m1_prod_cube    VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_m1_ttl_cube     VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_units      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_cases      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_totes      VARCHAR2(7)    := RPAD(' ', 7);
    l_ld_ttls_wgt        VARCHAR2(11)   := RPAD(' ', 11);
    l_ld_ttls_prod_cube  VARCHAR2(10)   := RPAD(' ', 10);
    l_ld_ttls_ttl_cube   VARCHAR2(10)   := RPAD(' ', 10);
    l_load_units         NUMBER(11)     := 0;
    l_load_cases         NUMBER(11)     := 0;
    l_load_totes         NUMBER(11)     := 0;
    l_load_boxes         NUMBER(11)     := 0;
    l_load_wgt           NUMBER(11, 2)  := 0;
    l_load_ttl_cube      NUMBER(11, 2)  := 0;
    l_load_prod_cube     NUMBER(11, 2)  := 0;

    CURSOR l_cur_mfst(
      b_create_ts  DATE,
      b_div_part   NUMBER,
      b_llr_dt     NUMBER
    ) IS
      SELECT   NVL(rs.strtg_nm, 'STRATEGY NOT FOUND') AS strtg_nm, NVL(c.descc, 'INVALID MANIFEST CATEGORY') AS descr,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, rpts.qty_alloc, 0)), 0) AS case_cnt,
               NVL(SUM(DECODE(rpts.tote_count + rpts.box_count, 0, 0, rpts.qty_alloc)), 0) AS unit_cnt,
               NVL(SUM(rpts.tote_count), 0) AS tote_cnt, NVL(SUM(rpts.box_count + rpts.bag_count), 0) AS box_cnt,
               NVL(SUM(rpts.product_weight), 0) AS prod_wgt, NVL(SUM(rpts.product_cube), 0) AS prod_cube,
               NVL(SUM(DECODE(rpts.tote_count, 0, rpts.product_cube, rpts.tote_count * b.outerb)), 0) AS ttl_cube
          FROM mclane_manifest_rpts rpts, mclp210c c, mclp200b b, rlse_strtg_op4t rs
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.strategy_id > 0
           AND c.div_part(+) = rpts.div_part
           AND c.manctc(+) = rpts.manifest_cat
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
           AND rs.div_part(+) = rpts.div_part
           AND rs.strtg_id(+) = rpts.strategy_id
      GROUP BY rs.strtg_nm, c.descc, c.seqc, rpts.manifest_cat
      ORDER BY c.seqc, rpts.manifest_cat;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_ld_h1_ln             VARCHAR2(133);
      l_c_ld_h4_ln  CONSTANT VARCHAR2(133) := ' ' || LPAD(' BOXES/               PRODUCT    CUBE OF', 80);
      l_c_ld_h5_ln  CONSTANT VARCHAR2(133)
                     := ' ' || 'MANIFEST CATEGORY         UNITS   CASES ' || '  TOTES     WEIGHT      CUBE      TOTES ';
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line-1');
        l_ld_h1_ln := '1'
                      || g_heading
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_ld_h1_ln);
        util.append(l_t_rpt_lns, g_ld_h2_ln);
        util.append(l_t_rpt_lns, g_ld_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, l_c_ld_h4_ln);
        util.append(l_t_rpt_lns, l_c_ld_h5_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 7;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_ld_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'MANIFEST' THEN
        logs.dbg('Add Manifest Line');
        util.append(l_t_rpt_lns, g_ld_m1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'MANIFEST'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD05';
    g_file_nm := RPAD(i_div, 2) || '_OPLD05_RELEASE_RECAP_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    g_heading := RPAD('SUMMARY RELEASE DEPARTMENT SUMMARY', 44);
    g_ld_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Count of Load Numbers');

    SELECT LPAD(TO_CHAR(COUNT(DISTINCT load_num), 'FM9999'), 4)
      INTO l_ld_h3_load_cnt
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = i_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id > 0;

    logs.dbg('Count of Stop Numbers');

    SELECT LPAD(TO_CHAR(COUNT(DISTINCT load_num || stop_num), 'FM99999'), 5)
      INTO l_ld_h3_stop_cnt
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = i_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id > 0;

    logs.dbg('Build Report Detail Cursor');
    FOR l_r_mfst IN l_cur_mfst(i_create_ts, l_div_part, i_llr_num) LOOP
      IF g_ln_cnt > g_c_lns_per_pg THEN
        logs.dbg('Format H3 Line');
        g_ld_h3_ln := ' '
                      || 'RELEASE: '
                      || RPAD(l_r_mfst.strtg_nm, 27)
                      || 'LOADS: '
                      || l_ld_h3_load_cnt
                      || '   STOPS: '
                      || l_ld_h3_stop_cnt;
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- g_ln_cnt > g_c_lns_per_pg

      logs.dbg('Format Manifest Line');
      l_ld_m1_mfst_descr := RPAD(l_r_mfst.descr, 23);
      l_ld_m1_prod_wgt := LPAD(TO_CHAR(l_r_mfst.prod_wgt, 'FM9999,999.00'), 11);
      l_ld_m1_prod_cube := LPAD(TO_CHAR(l_r_mfst.prod_cube, 'FM999,999.00'), 10);
      l_ld_m1_ttl_cube := LPAD(TO_CHAR(l_r_mfst.ttl_cube, 'FM999,999.00'), 10);
      l_ld_m1_totes := RPAD(' ', 7);

      IF (l_r_mfst.box_cnt + l_r_mfst.tote_cnt) > 0 THEN
        l_ld_m1_totes := LPAD(TO_CHAR(l_r_mfst.box_cnt + l_r_mfst.tote_cnt, 'FM999,999'), 7);
      END IF;

      l_ld_m1_units := RPAD(' ', 7);

      IF l_r_mfst.unit_cnt > 0 THEN
        l_ld_m1_units := LPAD(TO_CHAR(l_r_mfst.unit_cnt, 'FM999,999'), 7);
      END IF;

      l_ld_m1_cases := RPAD(' ', 7);

      IF l_r_mfst.case_cnt > 0 THEN
        l_ld_m1_cases := LPAD(TO_CHAR(l_r_mfst.case_cnt, 'FM999,999'), 7);
      END IF;

      g_ld_m1_ln := ' '
                    || l_ld_m1_mfst_descr
                    || ' '
                    || l_ld_m1_units
                    || ' '
                    || l_ld_m1_cases
                    || ' '
                    || l_ld_m1_totes
                    || l_ld_m1_prod_wgt
                    || ' '
                    || l_ld_m1_prod_cube
                    || ' '
                    || l_ld_m1_ttl_cube;
      logs.dbg('Add Manifest Line');
      add_sp('MANIFEST');
      l_load_units := l_load_units + l_r_mfst.unit_cnt;
      l_load_cases := l_load_cases + l_r_mfst.case_cnt;
      l_load_totes := l_load_totes + l_r_mfst.tote_cnt;
      l_load_boxes := l_load_boxes + l_r_mfst.box_cnt;
      l_load_wgt := l_load_wgt + l_r_mfst.prod_wgt;
      l_load_prod_cube := l_load_prod_cube + l_r_mfst.prod_cube;
      l_load_ttl_cube := l_load_ttl_cube + l_r_mfst.ttl_cube;
    END LOOP;
    logs.dbg('Format Report Totals');
    l_ld_ttls_units := LPAD(TO_CHAR(l_load_units, 'FM999,999'), 7);
    l_ld_ttls_cases := LPAD(TO_CHAR(l_load_cases, 'FM999,999'), 7);
    l_ld_ttls_totes := LPAD(TO_CHAR(l_load_totes + l_load_boxes, 'FM999,999'), 7);
    l_ld_ttls_wgt := LPAD(TO_CHAR(l_load_wgt, 'FM9999,999.00'), 11);
    l_ld_ttls_prod_cube := LPAD(TO_CHAR(l_load_prod_cube, 'FM999,999.00'), 10);
    l_ld_ttls_ttl_cube := LPAD(TO_CHAR(l_load_ttl_cube, 'FM999,999.00'), 10);
    g_ld_ttls_ln := ' '
                    || RPAD('GRAND TOTALS:', 24)
                    || l_ld_ttls_units
                    || ' '
                    || l_ld_ttls_cases
                    || ' '
                    || l_ld_ttls_totes
                    || l_ld_ttls_wgt
                    || ' '
                    || l_ld_ttls_prod_cube
                    || ' '
                    || l_ld_ttls_ttl_cube;
    logs.dbg('Add Load Totals');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END summary_rel_dept_summary_sp;

  /*
  ||----------------------------------------------------------------------------
  || STOP_ORDER_RECAP_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 04/19/02 | SNAGABH | Added NO_DATA_FOUND exception handlers for "Stop Line1"
  ||                      and "Stop Line 2" SQLs.
  || 04/22/02 | rhalpai | Changed load cursor to reflect only loads that are being
  ||                    | billed in that release.  Added a call to close the UTL
  ||                    | file to flush remaining output to be written to fix
  ||                    | missing lines from the end of the report.
  || 06/13/08 | rhalpai | Added sort by load to cursor CUR_LOAD. IM419804
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 04/04/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 02/25/15 | rhalpai | Remove unreferenced (cartesian) DIV_MSTR_DI1D table
  ||                    | from cursor. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE stop_order_recap_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm             := 'OP_MANIFEST_REPORTS_PK.STOP_ORDER_RECAP_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE                      := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_llr_dt             DATE                      := DATE '1900-02-28' + i_llr_num;
    l_tr_s1_stop_num1    VARCHAR2(3)               := RPAD(' ', 3);
    l_tr_s1_cust_num1    VARCHAR2(8)               := RPAD(' ', 8);
    l_tr_s1_cust_nm1     VARCHAR2(26)              := RPAD(' ', 26);
    l_tr_s1_stop_num2    VARCHAR2(3)               := RPAD(' ', 3);
    l_tr_s1_cust_num2    VARCHAR2(8)               := RPAD(' ', 8);
    l_tr_s1_cust_nm2     VARCHAR2(26)              := RPAD(' ', 26);
    l_loop_cnt           PLS_INTEGER               := 0;

    CURSOR l_cur_load(
      b_create_ts  DATE,
      b_div_part   NUMBER,
      b_llr_num    NUMBER
    ) IS
      SELECT   rpts.load_num, NVL(c.destc, 'INVALID LOAD') AS load_dest
          FROM mclane_manifest_rpts rpts, mclp120c c,
               (SELECT   r2.create_ts, r2.llr_date, r2.load_num
                    FROM mclane_manifest_rpts r2
                   WHERE r2.div_part = b_div_part
                     AND r2.strategy_id > 0
                     AND r2.create_ts = b_create_ts
                     AND r2.llr_date = b_llr_num
                GROUP BY r2.create_ts, r2.llr_date, r2.load_num) rpt2
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND c.div_part = rpts.div_part
           AND c.loadc = rpts.load_num
           AND rpt2.load_num = rpts.load_num
      GROUP BY rpts.load_num, c.destc
      ORDER BY rpts.load_num;

    CURSOR l_cur_stop_col1(
      b_div_part  NUMBER,
      b_llr_dt    DATE,
      b_load_num  VARCHAR2
    ) IS
      SELECT   t.stop_num AS stop_num1, x.cust_id AS cust_num1, MAX(NVL(x.ord_num, 0)) AS ord_num1
          FROM eoe_sum_rpt_temp t,
               (SELECT se.stop_num, se.cust_id, a.ordnoa AS ord_num
                  FROM eoe_sum_rpt_temp t2, stop_eta_op1g se, ordp100a a
                 WHERE t2.stop_num > 49
                   AND se.div_part = b_div_part
                   AND se.load_depart_sid IN(SELECT ld.load_depart_sid
                                               FROM load_depart_op1f ld
                                              WHERE ld.div_part = b_div_part
                                                AND ld.llr_dt = b_llr_dt
                                                AND ld.load_num = b_load_num)
                   AND se.stop_num = t2.stop_num
                   AND a.div_part = se.div_part
                   AND a.load_depart_sid = se.load_depart_sid
                   AND a.custa = se.cust_id
                   AND a.excptn_sw = 'N') x
         WHERE t.stop_num > 49
           AND x.stop_num(+) = t.stop_num
      GROUP BY t.stop_num, x.cust_id
      ORDER BY 1 DESC;

    CURSOR l_cur_stop_col2(
      b_div_part  NUMBER,
      b_llr_dt    DATE,
      b_load_num  VARCHAR2
    ) IS
      SELECT   t.stop_num AS stop_num2, x.cust_id AS cust_num2, MAX(NVL(x.ord_num, 0)) AS ord_num2
          FROM eoe_sum_rpt_temp t,
               (SELECT se.stop_num, se.cust_id, a.ordnoa AS ord_num
                  FROM eoe_sum_rpt_temp t2, load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                 WHERE t2.stop_num < 50
                   AND ld.div_part = b_div_part
                   AND ld.llr_dt = b_llr_dt
                   AND ld.load_num = b_load_num
                   AND se.div_part = ld.div_part
                   AND se.load_depart_sid = ld.load_depart_sid
                   AND se.stop_num = t2.stop_num
                   AND a.div_part = se.div_part
                   AND a.load_depart_sid = se.load_depart_sid
                   AND a.custa = se.cust_id
                   AND a.excptn_sw = 'N') x
         WHERE t.stop_num < 50
           AND x.stop_num(+) = t.stop_num
      GROUP BY t.stop_num, x.cust_id
      ORDER BY 1 DESC;

    l_r_stop_col1        l_cur_stop_col1%ROWTYPE;
    l_r_stop_col2        l_cur_stop_col2%ROWTYPE;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_tr_h1_ln  VARCHAR2(133);
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line');
        l_tr_h1_ln := '1'
                      || g_heading
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_tr_h1_ln);
        util.append(l_t_rpt_lns, g_tr_h2_ln);
        util.append(l_t_rpt_lns, g_tr_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, g_tr_h4_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 6;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_tr_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'STOP' THEN
        logs.dbg('Add Stop Line');
        util.append(l_t_rpt_lns, g_tr_s1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'STOP'

      IF i_ln_typ = 'STOP RECAP' THEN
        logs.dbg('Add Stop Line');
        util.append(l_t_rpt_lns, g_tr_s1_ln);
        g_ln_cnt := g_ln_cnt + 1;
      END IF;   -- i_ln_typ = 'STOP RECAP'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD07';
    g_file_nm := RPAD(i_div, 2) || '_OPLD07_STOP_ORDER_RECAP_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    g_heading := RPAD('STOP ORDER RECAP', 44);
    g_tr_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Build Stop Sequences');

    DELETE FROM eoe_sum_rpt_temp;

    l_loop_cnt := 0;
    <<build_seq_loop>>
    LOOP
      INSERT INTO eoe_sum_rpt_temp
                  (stop_num
                  )
           VALUES (l_loop_cnt
                  );

      l_loop_cnt := l_loop_cnt + 1;
      EXIT build_seq_loop WHEN l_loop_cnt > 99;
    END LOOP build_seq_loop;
    logs.dbg('Build Load Cursor');
    <<load_loop>>
    FOR l_r_load IN l_cur_load(i_create_ts, l_div_part, i_llr_num) LOOP
      g_ln_cnt := 99;
      logs.dbg('Open Stop Cursors');

      OPEN l_cur_stop_col1(l_div_part, l_llr_dt, l_r_load.load_num);

      OPEN l_cur_stop_col2(l_div_part, l_llr_dt, l_r_load.load_num);

      logs.dbg('Loop Thru each Stop Sequence');
      l_loop_cnt := 1;
      <<print_stops_loop>>
      LOOP
        logs.dbg('Fetch Stop1 Column');

        FETCH l_cur_stop_col1
         INTO l_r_stop_col1;

        logs.dbg('Fetch Stop2 Column');

        FETCH l_cur_stop_col2
         INTO l_r_stop_col2;

        IF l_loop_cnt = 1 THEN
          logs.dbg('Format H3 Line');
          g_tr_h3_ln := ' ' || ' LOAD: ' || RPAD(l_r_load.load_num, 4) || ' ' || l_r_load.load_dest;
          logs.dbg('Format H4 Line');
          g_tr_h4_ln := ' ' || RPAD('STOP ACCNT#   CUSTOMER NAME', 40) || 'STOP ACCNT#   CUSTOMER NAME';
        END IF;   -- l_loop_cnt = 1

        logs.dbg('Format Stop Recap Line');
        l_tr_s1_stop_num1 := LPAD(TO_CHAR(NVL(l_r_stop_col1.stop_num1, 0), 'FM999'), 3);
        l_tr_s1_cust_num1 := RPAD(NVL(l_r_stop_col1.cust_num1, 'NO ORDER'), 8);
        l_tr_s1_cust_nm1 := RPAD(' ', 26);

        IF l_tr_s1_cust_num1 <> 'NO ORDER' THEN
          BEGIN
            logs.dbg('Get Stop Line1 Cust Info');

            SELECT   RPAD(NVL(c.namec, ' '), 26), RPAD(MAX(NVL(cx.mccusb, 'NO ORDER')), 8)
                INTO l_tr_s1_cust_nm1, l_tr_s1_cust_num1
                FROM sysp200c c, ordp100a o, mclp020b cx
               WHERE c.div_part = l_div_part
                 AND c.acnoc = l_r_stop_col1.cust_num1
                 AND o.div_part = c.div_part
                 AND o.ordnoa = l_r_stop_col1.ord_num1
                 AND cx.div_part(+) = o.div_part
                 AND cx.custb(+) = o.custa
            GROUP BY c.namec;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_tr_s1_cust_nm1 := RPAD(' ', 26);
              l_tr_s1_cust_num1 := 'PROBLEM ';
              logs.warn('NO_DATA_FOUND Exception', lar_parm);
          END;
        END IF;   -- l_tr_s1_cust_num1 <> 'NO ORDER'

        l_tr_s1_stop_num2 := LPAD(TO_CHAR(NVL(l_r_stop_col2.stop_num2, 0), 'FM999'), 3);
        l_tr_s1_cust_num2 := RPAD(NVL(l_r_stop_col2.cust_num2, 'NO ORDER'), 8);
        l_tr_s1_cust_nm2 := RPAD(' ', 26);

        IF l_tr_s1_cust_num2 <> 'NO ORDER' THEN
          BEGIN
            logs.dbg('Get Stop Line2 Cust Info');

            SELECT   RPAD(NVL(c.namec, ' '), 26), RPAD(MAX(NVL(cx.mccusb, 'NO ORDER')), 8)
                INTO l_tr_s1_cust_nm2, l_tr_s1_cust_num2
                FROM sysp200c c, ordp100a o, mclp020b cx
               WHERE c.div_part = l_div_part
                 AND c.acnoc = l_r_stop_col2.cust_num2
                 AND o.div_part = c.div_part
                 AND o.ordnoa = l_r_stop_col2.ord_num2
                 AND cx.div_part(+) = o.div_part
                 AND cx.custb(+) = o.custa
            GROUP BY c.namec;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_tr_s1_cust_nm2 := RPAD(' ', 26);
              l_tr_s1_cust_num2 := 'PROBLEM ';
              logs.warn('NO_DATA_FOUND Exception', lar_parm);
          END;
        END IF;   -- l_tr_s1_cust_num2 <> 'NO ORDER'

        g_tr_s1_ln := ' '
                      || ' '
                      || l_tr_s1_stop_num1
                      || ' '
                      || l_tr_s1_cust_num1
                      || ' '
                      || l_tr_s1_cust_nm1
                      || ' '
                      || l_tr_s1_stop_num2
                      || ' '
                      || l_tr_s1_cust_num2
                      || ' '
                      || l_tr_s1_cust_nm2;
        logs.dbg('Add Stop Recap Line');
        add_sp('STOP RECAP');
        l_loop_cnt := l_loop_cnt + 1;
        EXIT print_stops_loop WHEN l_loop_cnt > 50;
      END LOOP print_stops_loop;
      logs.dbg('Close Stop Cursors');

      CLOSE l_cur_stop_col1;

      CLOSE l_cur_stop_col2;
    END LOOP load_loop;
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_stop_col1%ISOPEN THEN
        CLOSE l_cur_stop_col1;
      END IF;

      IF l_cur_stop_col2%ISOPEN THEN
        CLOSE l_cur_stop_col2;
      END IF;

      logs.err(lar_parm);
  END stop_order_recap_sp;

  /*
  ||----------------------------------------------------------------------------
  || TOTE_RECAP_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 03/11/02 | rhalpai | Included Bag Count in the Box Count
  || 03/02/07 | rhalpai | Changed to use Bag Count in its own column and to
  ||                    | produce a Cumulative or Non-Cumulative report based
  ||                    | on new divisional OPLD06_IS_CUMULATIVE parm. IM290595
  ||                    | Changed error handler to new standard format. PIR2051
  || 03/15/07 | rhalpai | Changed to include only loads from current release in
  ||                    | cumulative reports. IM290595
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 04/04/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 03/01/16 | rhalpai | Change to use new OP_CONST_PK and OP_PARMS_PK.VAL_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE tote_recap_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MANIFEST_REPORTS_PK.TOTE_RECAP_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE           := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_cumulative_sw      VARCHAR2(1);
    l_save_tote_catg     VARCHAR2(3)    := '   ';
    l_save_load          VARCHAR2(4)    := '    ';
    l_tr_s1_units        VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_s1_totes        VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_s1_boxes        VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_s1_bags         VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_ttls_stops      VARCHAR2(3)    := RPAD(' ', 3);
    l_tr_ttls_units      VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_ttls_totes      VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_ttls_boxes      VARCHAR2(7)    := RPAD(' ', 7);
    l_tr_ttls_bags       VARCHAR2(7)    := RPAD(' ', 7);
    l_units              PLS_INTEGER    := 0;
    l_totes              PLS_INTEGER    := 0;
    l_boxes              PLS_INTEGER    := 0;
    l_bags               PLS_INTEGER    := 0;
    l_stops              PLS_INTEGER    := 0;
    l_is_box             BOOLEAN;
    l_is_box_save        BOOLEAN;

    CURSOR l_cur_stop(
      b_div_part       NUMBER,
      b_llr_dt         DATE,
      b_create_ts      DATE,
      b_cumulative_sw  VARCHAR2
    ) IS
      SELECT   rpts.load_num, rpts.stop_num, b.totctb AS tote_catg,(CASE
                                                                      WHEN t.boxb IN('Y', '1') THEN 'Y'
                                                                    END) AS box_sw,
               DECODE(NVL(t.boxb, 'N'),
                      'Y', 'CARTONS   BOXES    BAGS',
                      '1', 'CARTONS   BOXES    BAGS',
                      '  UNITS   TOTES'
                     ) AS stop_heading,
               b.totctb || '-' || NVL(t.descb, 'INVALID TOTE CATEGORY') AS tote_descr, rpts.units, rpts.tote_cnt,
               rpts.box_cnt, rpts.bag_cnt
          FROM (SELECT   ld.load_depart_sid, r.cust_num, r.load_num, r.stop_num, r.tote_cat, SUM(r.qty_alloc) AS units,
                         SUM(r.tote_count) AS tote_cnt, SUM(r.box_count) AS box_cnt, SUM(r.bag_count) AS bag_cnt
                    FROM mclane_manifest_rpts r, load_depart_op1f ld
                   WHERE r.div_part = b_div_part
                     AND r.create_ts = b_create_ts
                     AND r.llr_date = b_llr_dt - DATE '1900-02-28'
                     AND (   (    b_cumulative_sw = 'Y'
                              AND r.strategy_id = 0
                              AND r.load_num IN(SELECT r2.load_num
                                                  FROM mclane_manifest_rpts r2
                                                 WHERE r2.div_part = b_div_part
                                                   AND r2.create_ts = b_create_ts
                                                   AND r2.llr_date = b_llr_dt - DATE '1900-02-28'
                                                   AND r2.strategy_id > 0)
                             )
                          OR (    b_cumulative_sw = 'N'
                              AND r.strategy_id > 0)
                         )
                     AND r.tote_cat <> '000'
                     AND ld.div_part = r.div_part
                     AND ld.llr_dt = b_llr_dt
                     AND ld.load_num = r.load_num
                     AND EXISTS(SELECT 1
                                  FROM stop_eta_op1g se
                                 WHERE se.div_part = ld.div_part
                                   AND se.load_depart_sid = ld.load_depart_sid
                                   AND se.cust_id = r.cust_num
                                   AND se.stop_num = r.stop_num)
                GROUP BY ld.load_depart_sid, r.cust_num, r.load_num, r.stop_num, r.tote_cat) rpts,
               ordp100a a, ordp120b b, mclp200b t
         WHERE a.div_part = b_div_part
           AND a.load_depart_sid = rpts.load_depart_sid
           AND a.custa = rpts.cust_num
           AND a.excptn_sw = 'N'
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.totctb = rpts.tote_cat
           AND b.excptn_sw = 'N'
           AND t.div_part(+) = b.div_part
           AND t.totctb(+) = b.totctb
      GROUP BY rpts.load_num, rpts.stop_num, b.totctb, t.boxb, t.descb, rpts.units, rpts.tote_cnt, rpts.box_cnt,
               rpts.bag_cnt
      ORDER BY load_num, tote_catg, stop_num;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_tr_h1_ln  VARCHAR2(133);
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line');
        l_tr_h1_ln := '1'
                      || g_heading
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_tr_h1_ln);
        util.append(l_t_rpt_lns, g_tr_h2_ln);
        util.append(l_t_rpt_lns, g_tr_h3_ln);
        util.append(l_t_rpt_lns, '');
        util.append(l_t_rpt_lns, g_tr_h4_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := 6;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, g_tr_ttls_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'TOTALS'

      IF i_ln_typ = 'STOP' THEN
        logs.dbg('Add Stop Line');
        util.append(l_t_rpt_lns, g_tr_s1_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 2;
      END IF;   -- i_ln_typ = 'STOP'

      IF i_ln_typ = 'STOP RECAP' THEN
        logs.dbg('Add Stop Line');
        util.append(l_t_rpt_lns, g_tr_s1_ln);
        g_ln_cnt := g_ln_cnt + 1;
      END IF;   -- i_ln_typ = 'STOP RECAP'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD06';
    g_file_nm := RPAD(i_div, 2) || '_OPLD06_TOTE_RECAP_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    l_cumulative_sw := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_opld06_cumulative), 'N');
    g_heading := RPAD((CASE l_cumulative_sw
                         WHEN 'Y' THEN 'TOTE BOX RECAP  [CUMULATIVE]'
                         ELSE 'TOTE BOX RECAP  [NON-CUMULATIVE]'
                       END
                      ),
                      44
                     );
    g_tr_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || '    ' || i_div || ' ' || div_pk.div_nm_fn(i_div);
    logs.dbg('Build Loads for the Report');

    INSERT INTO rpt_load_temp
                (LOAD)
      SELECT   rpts.load_num
          FROM mclane_manifest_rpts rpts
         WHERE rpts.div_part = l_div_part
           AND rpts.create_ts = i_create_ts
           AND rpts.llr_date = i_llr_num
           AND rpts.strategy_id > 0
      GROUP BY rpts.load_num;

    logs.dbg('Build Report Detail Cursor');
    FOR l_r_stop IN l_cur_stop(l_div_part, DATE '1900-02-28' + i_llr_num, i_create_ts, l_cumulative_sw) LOOP
      l_is_box :=(l_r_stop.box_sw = 'Y');
      logs.dbg('Check for different Load / TOTE CATEGORY');

      IF (   l_r_stop.load_num <> l_save_load
          OR l_r_stop.tote_catg <> l_save_tote_catg) THEN
        IF g_cnt = 1 THEN
          l_save_load := l_r_stop.load_num;
          l_save_tote_catg := l_r_stop.tote_catg;
        END IF;   -- g_cnt = 1

        IF g_cnt <> 1 THEN
          logs.dbg('Format Tote Category Totals');
          l_tr_ttls_stops := LPAD(l_stops, 3);
          l_tr_ttls_units := LPAD(TO_CHAR(l_units, 'FM999,999'), 7);

          IF l_is_box_save THEN
            l_tr_ttls_boxes := LPAD(TO_CHAR(l_boxes, 'FM999,999'), 7);
            l_tr_ttls_bags := LPAD(TO_CHAR(l_bags, 'FM999,999'), 7);
            g_tr_ttls_ln := ' '
                            || RPAD(' TOTALS:', 11)
                            || l_tr_ttls_stops
                            || ' '
                            || l_tr_ttls_units
                            || ' '
                            || l_tr_ttls_boxes
                            || ' '
                            || l_tr_ttls_bags;
          ELSE
            l_tr_ttls_totes := LPAD(TO_CHAR(l_totes, 'FM999,999'), 7);
            g_tr_ttls_ln := ' '
                            || RPAD(' TOTALS:', 11)
                            || l_tr_ttls_stops
                            || ' '
                            || l_tr_ttls_units
                            || ' '
                            || l_tr_ttls_totes;
          END IF;   -- l_is_box_save

          logs.dbg('Add Category Totals');
          add_sp('TOTALS');
          l_stops := 0;
          l_units := 0;
          l_totes := 0;
          l_boxes := 0;
          l_bags := 0;
          l_save_load := l_r_stop.load_num;
          l_save_tote_catg := l_r_stop.tote_catg;
          g_ln_cnt := 99;
        END IF;   -- g_cnt <> 1
      END IF;   -- l_r_stop.load_num <> l_save_load OR l_r_stop.tote_catg <> l_save_tote_catg

      IF g_ln_cnt > g_c_lns_per_pg THEN
        logs.dbg('Format H3 Line');
        g_tr_h3_ln := ' '
                      || 'LOAD: '
                      || RPAD(l_r_stop.load_num, 4)
                      || RPAD(' ', 4)
                      || RPAD('TOTE CATEGORY: ', 15)
                      || l_r_stop.tote_descr;
        logs.dbg('Format H4 Line');
        g_tr_h4_ln := ' ' || LPAD('STOP ', 15) || l_r_stop.stop_heading;
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- g_ln_cnt > g_c_lns_per_pg

      logs.dbg('Format Stop Line');
      l_tr_s1_units := RPAD(' ', 7);

      IF l_r_stop.units > 0 THEN
        l_tr_s1_units := LPAD(TO_CHAR(l_r_stop.units, 'FM999,999'), 7);
      END IF;   -- l_r_stop.units > 0

      IF l_is_box THEN
        l_tr_s1_boxes := RPAD(' ', 7);

        IF l_r_stop.box_cnt > 0 THEN
          l_tr_s1_boxes := LPAD(TO_CHAR(l_r_stop.box_cnt, 'FM999,999'), 7);
        END IF;   -- l_r_stop.box_cnt > 0

        l_tr_s1_bags := RPAD(' ', 7);

        IF l_r_stop.bag_cnt > 0 THEN
          l_tr_s1_bags := LPAD(TO_CHAR(l_r_stop.bag_cnt, 'FM999,999'), 7);
        END IF;   -- l_r_stop.bag_cnt > 0

        g_tr_s1_ln := ' '
                      || RPAD(' ', 11)
                      || LPAD(l_r_stop.stop_num, 3)
                      || ' '
                      || l_tr_s1_units
                      || ' '
                      || l_tr_s1_boxes
                      || ' '
                      || l_tr_s1_bags;
      ELSE
        l_tr_s1_totes := RPAD(' ', 7);

        IF l_r_stop.tote_cnt > 0 THEN
          l_tr_s1_totes := LPAD(TO_CHAR(l_r_stop.tote_cnt, 'FM999,999'), 7);
        END IF;   -- l_r_stop.tote_cnt > 0

        g_tr_s1_ln := ' ' || RPAD(' ', 11) || LPAD(l_r_stop.stop_num, 3) || ' ' || l_tr_s1_units || ' ' || l_tr_s1_totes;
      END IF;   -- l_is_box

      logs.dbg('Add Stop Line');
      add_sp('STOP');
      l_stops := l_stops + 1;
      l_units := l_units + l_r_stop.units;

      IF l_is_box THEN
        l_boxes := l_boxes + l_r_stop.box_cnt;
        l_bags := l_bags + l_r_stop.bag_cnt;
      ELSE
        l_totes := l_totes + l_r_stop.tote_cnt;
      END IF;   -- l_is_box

      l_is_box_save := l_is_box;
    END LOOP;   -- Stop
    logs.dbg('Add Final Stop Totals');
    l_tr_ttls_stops := LPAD(l_stops, 3);
    l_tr_ttls_units := LPAD(TO_CHAR(l_units, 'FM999,999'), 7);

    IF l_is_box_save THEN
      l_tr_ttls_boxes := LPAD(TO_CHAR(l_boxes, 'FM999,999'), 7);
      l_tr_ttls_bags := LPAD(TO_CHAR(l_bags, 'FM999,999'), 7);
      g_tr_ttls_ln := ' '
                      || RPAD(' TOTALS:', 11)
                      || l_tr_ttls_stops
                      || ' '
                      || l_tr_ttls_units
                      || ' '
                      || l_tr_ttls_boxes
                      || ' '
                      || l_tr_ttls_bags;
    ELSE
      l_tr_ttls_totes := LPAD(TO_CHAR(l_totes, 'FM999,999'), 7);
      g_tr_ttls_ln := ' ' || RPAD(' TOTALS:', 11) || l_tr_ttls_stops || ' ' || l_tr_ttls_units || ' '
                      || l_tr_ttls_totes;
    END IF;   -- l_is_box_save

    logs.dbg('Add Category Totals');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tote_recap_sp;

  /*
  ||----------------------------------------------------------------------------
  || STOP_SUMMARY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/05/01 | rhalpai | Original
  || 02/01/02 | rhalpai | Corrected total calculation by adding missing left join
  ||                    | on MCLP200B.DIVB in cur_stop_summary.
  || 04/22/02 | rhalpai | Changed cursor to reflect only loads that are being
  ||                    | billed in that release.
  || 06/13/08 | rhalpai | Added sort by load/stop to cursor. IM419804
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | unused columns from cursor. Remove status out parm.
  ||                    | Reformat and move print procedure within. PIR8531
  || 07/07/21 | jxpazho | Added eta_dt and corp_cd in report
  ||----------------------------------------------------------------------------
  */
  PROCEDURE stop_summary_sp(
    i_llr_num    IN      NUMBER,
    i_div        IN      VARCHAR2,
    i_create_ts  IN      DATE,
    o_file_nm    OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MANIFEST_REPORTS_PK.STOP_SUMMARY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_sysdate          DATE           := SYSDATE;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_save_load          VARCHAR2(4)    := '    ';
    l_load_ttl_cube      NUMBER(11, 3)  := 0;
    l_load_prod_cube     NUMBER(11, 3)  := 0;
    l_load_wgt           NUMBER(11, 2)  := 0;
    l_load_stops         NUMBER(3)      := 0;

    CURSOR l_cur_stop(
      b_create_ts  DATE,
      b_div_part   NUMBER,
      b_llr_dt     NUMBER
    ) IS
      SELECT   rpts.load_num, rpts.stop_num, NVL(x.mccusb, '000000') AS mcl_cust,
               NVL(c.namec, 'NAME UNKNOWN') AS cust_nm, NVL(c.shad1c, ' ') AS addr, NVL(c.shpctc, ' ') AS city,
               NVL(c.shpstc, '  ') AS st, NVL(c.shpzpc, '000000000') AS zip,
               DECODE(c.cnphnc,
                      NULL, 'N/A         ',
                      'N/A', 'N/A         ',
                      SUBSTR(c.cnphnc, 1, 3) || '-' || SUBSTR(c.cnphnc, 4, 3) || '-' || SUBSTR(c.cnphnc, 7, 4)
                     ) AS phone,
               SUBSTR(LPAD(NVL(rpts.eta_time, 0), 4, '0'), 1, 2)
               || ':'
               || SUBSTR(LPAD(NVL(rpts.eta_time, 0), 4, '0'), 3, 2) AS eta_tm,
               NVL(SUM(rpts.product_cube), 0) AS prod_cube,
               NVL(SUM(DECODE(rpts.tote_count, 0, rpts.product_cube, rpts.tote_count * b.outerb)), 0) AS ttl_cube,
               NVL(SUM(rpts.product_weight), 0) wgt,
               x.corpb AS corp_cd,
               DATE '1900-02-28' + rpts.eta_date AS eta_dt
          FROM mclane_manifest_rpts rpts, sysp200c c, mclp200b b, mclp020b x,
               (SELECT   r2.create_ts, r2.llr_date, r2.load_num
                    FROM mclane_manifest_rpts r2
                   WHERE r2.div_part = b_div_part
                     AND r2.strategy_id > 0
                     AND r2.create_ts = b_create_ts
                     AND r2.llr_date = b_llr_dt
                GROUP BY r2.create_ts, r2.llr_date, r2.load_num) rpt2
         WHERE rpts.div_part = b_div_part
           AND rpts.create_ts = b_create_ts
           AND rpts.llr_date = b_llr_dt
           AND rpts.strategy_id = 0
           AND c.div_part = rpts.div_part
           AND c.acnoc = rpts.cust_num
           AND x.div_part = rpts.div_part
           AND x.custb = rpts.cust_num
           AND rpt2.load_num = rpts.load_num
           AND b.div_part(+) = rpts.div_part
           AND b.totctb(+) = rpts.tote_cat
      GROUP BY rpts.load_num, rpts.stop_num, x.mccusb, c.namec, c.shad1c, c.shpctc, c.shpstc, c.shpzpc, c.cnphnc,
               rpts.eta_time, x.corpb, rpts.eta_date
      ORDER BY rpts.load_num, rpts.stop_num;

    PROCEDURE add_sp(
      i_ln_typ  IN  VARCHAR2
    ) IS
      l_ss_h1_ln                VARCHAR2(133);
      l_ss_h4_ln                VARCHAR2(133);
      l_ss_h5_ln                VARCHAR2(133);
      l_c_ss_h6_ln     CONSTANT VARCHAR2(133)
          := ' ---- ------' || ' -------------------------' || ' ------------' || ' --------------' || ' -------------';
      l_c_ss_ttls1_ln  CONSTANT VARCHAR2(133) := LPAD('------------------------------------------------', 80);
    BEGIN
      IF (   g_ln_cnt > g_c_lns_per_pg
          OR i_ln_typ = 'HEADINGS') THEN
        g_pg_cnt := g_pg_cnt + 1;
        logs.dbg('Format H1 Line');
        l_ss_h1_ln := '1'
                      || RPAD('STOP SUMMARY', 44)
                      || 'CREATED: '
                      || TO_CHAR(l_c_sysdate, 'MM/DD/YY HH24:MI:SS')
                      || ' PAGE '
                      || LPAD(g_pg_cnt, 4);
        logs.dbg('Format H4 Line');
        l_ss_h4_ln := ' ' || '     CUST/' || LPAD('CUBE OF TOTES/', 55);
        logs.dbg('Format H5 Line');
        l_ss_h5_ln := ' '
                      || RPAD('STOP CORP   CUSTOMER NAME/ADDRESS', 38)
                      || RPAD('PHONE/ETA    PRODUCT CUBE   WEIGHT', 34);

        IF g_cnt = 1 THEN
          g_cnt := 0;
        END IF;   -- g_cnt = 1

        logs.dbg('Add Headings');
        util.append(l_t_rpt_lns, l_ss_h1_ln);
        util.append(l_t_rpt_lns, g_ss_h2_ln);
        util.append(l_t_rpt_lns, g_ss_h3_ln);
        util.append(l_t_rpt_lns, l_ss_h4_ln);
        util.append(l_t_rpt_lns, l_ss_h5_ln);
        util.append(l_t_rpt_lns, l_c_ss_h6_ln);
        g_ln_cnt := 6;
      END IF;   -- g_ln_cnt > g_c_lns_per_pg OR i_ln_typ = 'HEADINGS'

      IF i_ln_typ = 'DETAIL' THEN
        logs.dbg('Add Detail Lines');
        util.append(l_t_rpt_lns, g_ss_dtl1_ln);
        util.append(l_t_rpt_lns, g_ss_dtl2_ln);
        util.append(l_t_rpt_lns, g_ss_dtl3_ln);
        util.append(l_t_rpt_lns, '');
        g_ln_cnt := g_ln_cnt + 4;
      END IF;   -- i_ln_typ = 'DETAIL'

      IF i_ln_typ = 'TOTALS' THEN
        logs.dbg('Add Totals Line');
        util.append(l_t_rpt_lns, l_c_ss_ttls1_ln);
        util.append(l_t_rpt_lns, g_ss_ttls2_ln);
        util.append(l_t_rpt_lns, g_ss_ttls3_ln);
        g_ln_cnt := g_ln_cnt + 3;
      END IF;   -- i_ln_typ = 'TOTALS'
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    g_cnt := 1;
    g_ln_cnt := 99;
    g_pg_cnt := 0;
    g_rpt_id := 'OPLD08';
    g_file_nm := RPAD(i_div, 2) || '_OPLD08_STOP_SUMMARY_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS');
    o_file_nm := g_c_file_dir || '/' || g_file_nm;
    g_ss_h2_ln := ' ' || 'REPORT: ' || i_div || g_rpt_id || RPAD(' ', 13) || i_div || ' ' || div_pk.div_nm_fn(i_div);
    FOR l_r_stop IN l_cur_stop(i_create_ts, l_div_part, i_llr_num) LOOP
      IF l_r_stop.load_num <> l_save_load THEN
        IF g_cnt = 1 THEN
          l_save_load := l_r_stop.load_num;
        ELSE
          logs.dbg('Format Load Totals Line 2');
          g_ss_ttls2_ln := ' '
                           || LPAD('STOPS:', 37)
                           || TO_CHAR(l_load_stops, '990')
                           || '  TOTALS: '
                           || LPAD(TO_CHAR(l_load_ttl_cube, '999,990.000'), 14)
                           || ' '
                           || LPAD(TO_CHAR(l_load_wgt, '9999,990.00'), 13);
          logs.dbg('Format Load Totals Line 3');
          g_ss_ttls3_ln := ' ' || LPAD(TO_CHAR(l_load_prod_cube, '999,990.000'), 65);
          logs.dbg('Add Load Totals');
          add_sp('TOTALS');
          -- reset for next time
          l_save_load := l_r_stop.load_num;
          l_load_stops := 0;
          l_load_ttl_cube := 0;
          l_load_prod_cube := 0;
          l_load_wgt := 0;
          g_ln_cnt := 99;   -- force headings
        END IF;   -- g_cnt = 1
      END IF;   -- l_r_stop.load_num <> l_save_load

      IF (g_ln_cnt + 4) > g_c_lns_per_pg THEN   -- format and print headings
        logs.dbg('Format H3 Line');
        g_ss_h3_ln := ' ' || 'LOAD: ' || l_r_stop.load_num;
        logs.dbg('Add Heading Lines');
        add_sp('HEADINGS');
      END IF;   -- (g_ln_cnt + 4) > g_c_lns_per_pg

      logs.dbg('Format Detail Line 1');
      g_ss_dtl1_ln := ' '
                      || LPAD(TO_CHAR(l_r_stop.stop_num, '00'), 4)
                      || ' '
                      || LPAD(l_r_stop.mcl_cust, 6, '0')
                      || ' '
                      || RPAD(l_r_stop.cust_nm, 25)
                      || ' '
                      || l_r_stop.phone
                      || ' '
                      || LPAD(TO_CHAR(l_r_stop.ttl_cube, '999,990.000'), 14)
                      || ' '
                      || LPAD(TO_CHAR(l_r_stop.wgt, '9999,990.00'), 13);
      logs.dbg('Format Detail Line 2');
      g_ss_dtl2_ln := ' '
                      || LPAD(' ', 7)
                      || TO_CHAR(l_r_stop.corp_cd, '000')
                      || ' '
                      || RPAD(l_r_stop.addr, 25)
                      || ' '
                      || RPAD(TO_CHAR(l_r_stop.eta_dt, 'MM/DD/YY'), 12)
                      || ' '
                      || LPAD(TO_CHAR(l_r_stop.prod_cube, '999,990.000'), 14);
      logs.dbg('Format Detail Line 3');
      g_ss_dtl3_ln := ' '
                      || LPAD(' ', 12)
                      || RPAD(l_r_stop.city, 16)
                      || ' '
                      || RPAD(l_r_stop.st, 2)
                      || ' '
                      || LPAD(l_r_stop.zip, 5, '0')
                      || ' '
                      || RPAD(l_r_stop.eta_tm, 12) ;

      logs.dbg('Add Detail Lines');
      add_sp('DETAIL');
      l_load_ttl_cube := l_load_ttl_cube + l_r_stop.ttl_cube;
      l_load_prod_cube := l_load_prod_cube + l_r_stop.prod_cube;
      l_load_wgt := l_load_wgt + l_r_stop.wgt;
      l_load_stops := l_load_stops + 1;
    END LOOP;
    -- Format and Add Totals for Last Load
    logs.dbg('Format Load Totals Line 2');
    g_ss_ttls2_ln := ' '
                     || LPAD('STOPS:', 37)
                     || TO_CHAR(l_load_stops, '990')
                     || '  TOTALS: '
                     || LPAD(TO_CHAR(l_load_ttl_cube, '999,990.000'), 14)
                     || ' '
                     || LPAD(TO_CHAR(l_load_wgt, '9999,990.00'), 13);
    logs.dbg('Format Load Totals Line 3');
    g_ss_ttls3_ln := ' ' || LPAD(TO_CHAR(l_load_prod_cube, '999,990.000'), 65);
    logs.dbg('Add Load Totals');
    add_sp('TOTALS');
    write_sp(l_t_rpt_lns, g_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_summary_sp;

  /*
  ||----------------------------------------------------------------------------
  || MANIFEST_REPORTS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | status out parm and reformat. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE manifest_reports_sp(
    i_llr_num    IN  NUMBER,
    i_div        IN  VARCHAR2,
    i_create_ts  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MANIFEST_REPORTS_PK.MANIFEST_REPORTS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_create_ts          DATE;
    l_file_nm            VARCHAR2(100);
    l_cumulative_sw      VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_create_ts IS NULL THEN
      SELECT MAX(rpts.create_ts)
        INTO l_create_ts
        FROM mclane_manifest_rpts rpts
       WHERE rpts.div_part = l_div_part
         AND rpts.llr_date = i_llr_num;
    ELSE
      l_create_ts := TO_DATE(i_create_ts, 'YYYYMMDDHH24MISS');
    END IF;   -- i_create_ts IS NULL

    SELECT MAX('Y')
      INTO l_cumulative_sw
      FROM mclane_manifest_rpts rpts
     WHERE rpts.div_part = l_div_part
       AND rpts.create_ts = l_create_ts
       AND rpts.llr_date = i_llr_num
       AND rpts.strategy_id = 0
       AND ROWNUM = 1;

    logs.dbg('Execute RELEASE_DEPT_SUMMARY_SP');
    release_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
    logs.dbg('Execute SUMMARY_REL_DEPT_SUMMARY_SP');
    summary_rel_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
    logs.dbg('Execute TOTE_RECAP_SP');
    tote_recap_sp(i_llr_num, i_div, l_create_ts, l_file_nm);

    IF l_cumulative_sw = 'Y' THEN
      logs.dbg('Execute LOAD_DEPT_SUMMARY_SP');
      load_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
      logs.dbg('Execute SUMMARY_LOAD_DEPT_SUMMARY_SP');
      summary_load_dept_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
      logs.dbg('Execute LOADING_MANIFEST_SP');
      loading_manifest_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
      logs.dbg('Execute STOP_ORDER_RECAP_SP');
      stop_order_recap_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
      logs.dbg('Execute STOP_SUMMARY_SP');
      stop_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
    END IF;   -- l_cumulative_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END manifest_reports_sp;

  /*
  ||----------------------------------------------------------------------------
  || TEST_STOP_ORDER_RECAP_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | status out parm. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE test_stop_order_recap_sp(
    i_llr_num    IN  NUMBER,
    i_div        IN  VARCHAR2,
    i_create_ts  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                         := 'OP_MANIFEST_REPORTS_PK.TEST_STOP_ORDER_RECAP_SP';
    lar_parm             logs.tar_parm;
    l_file_nm            VARCHAR2(100);
    l_create_ts          mclane_manifest_rpts.create_ts%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    l_create_ts := TO_DATE(i_create_ts, 'YYYYMMDDHH24MISS');
    stop_order_recap_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
    logs.dbg('File: ' || l_file_nm);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END test_stop_order_recap_sp;

  /*
  ||----------------------------------------------------------------------------
  || TEST_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/20/01 | JUSTANI | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. Remove
  ||                    | status out parm. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE test_sp(
    i_llr_num    IN  NUMBER,
    i_div        IN  VARCHAR2,
    i_create_ts  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                         := 'OP_MANIFEST_REPORTS_PK.TEST_SP';
    lar_parm             logs.tar_parm;
    l_file_nm            VARCHAR2(100);
    l_create_ts          mclane_manifest_rpts.create_ts%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    l_create_ts := TO_DATE(i_create_ts, 'YYYYMMDDHH24MISS');
    stop_summary_sp(i_llr_num, i_div, l_create_ts, l_file_nm);
    DBMS_OUTPUT.put_line('File: ' || l_file_nm);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END test_sp;
END op_manifest_reports_pk;
/

