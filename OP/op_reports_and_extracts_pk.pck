CREATE OR REPLACE PACKAGE op_reports_and_extracts_pk IS
  /**
  ||----------------------------------------------------------------------------
  || Package used to create order reports.
  ||----------------------------------------------------------------------------
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
  /**
  ||----------------------------------------------------------------------------
  || Print order summary by load, customer, order.
  || #param i_div        Division ID ie: MW,NE,SW,etc.
  || #param o_file_nm    The directory and file name of the created report.
  || #param i_div_nm     Division name ie: WESTERN,NORTHEAST,SOUTHWEST,etc.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_load_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Print order summary by load, customer.
  || #param i_div        Division ID ie: MW,NE,SW,etc.
  || #param o_file_nm    The directory and file name of the created report.
  || #param i_div_nm     Division name ie: WESTERN,NORTHEAST,SOUTHWEST,etc.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_load_sum_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Print order summary by group, customer.
  || #param i_div        Division ID ie: MW,NE,SW,etc.
  || #param o_file_nm    The directory and file name of the created report.
  || #param i_div_nm     Division name ie: WESTERN,NORTHEAST,SOUTHWEST,etc.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_group_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Print missing orders by group, customer.
  || #param i_div        Division ID ie: MW,NE,SW,etc.
  || #param o_file_nm    The directory and file name of the created report.
  || #param i_div_nm     Division name ie: WESTERN,NORTHEAST,SOUTHWEST,etc.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE missing_ord_by_grp_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Create and email the COMET Error Report.
  || #param i_div        Division ID ie: MW,NE,SW,etc.
  || #param i_div_nm     Division name ie: WESTERN,NORTHEAST,SOUTHWEST,etc.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE comet_error_rpt_sp(
    i_div     IN  VARCHAR2,
    i_div_nm  IN  VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Create and send extract of 711 Dist with no retails.
  || #param i_run_ts     Run date
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dist_no_rtl_extr_sp(
    i_run_ts  IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || Entry point and controlling procedure to process all reports.
  || #param i_div        Division ID ie: MW,NE,SW,etc.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE main_sp(
    i_div  IN  VARCHAR2
  );
END op_reports_and_extracts_pk;
/

CREATE OR REPLACE PACKAGE BODY op_reports_and_extracts_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_lns_per_pg       CONSTANT NUMBER(3)    := 60;
  g_c_heading          CONSTANT VARCHAR2(1)  := 'H';
  g_c_detail           CONSTANT VARCHAR2(1)  := 'D';
  g_c_file_dir         CONSTANT VARCHAR2(80) := '/ftptrans';
  g_c_ord_rpt_tmp_tbl  CONSTANT VARCHAR2(30) := 'EOE_SUM_RPT_TEMP';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || TEMP_DECODE_FN
  ||   Simulates a decode statement to allow usage outside SQL.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/13/01 | JUSTANI | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION temp_decode_fn(
    i_val          IN  VARCHAR2,
    i_compare_val  IN  VARCHAR2,
    i_match_val    IN  VARCHAR2,
    i_nomatch_val  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_val  VARCHAR2(100);
  BEGIN
    IF TRIM(i_val) = i_compare_val THEN
      l_val := i_match_val;
    ELSE
      l_val := NVL(i_nomatch_val, i_val);
    END IF;

    RETURN(l_val);
  END temp_decode_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_EOE_SUM_RPT_TEMP_SP
  ||   Populate the eoe_sum_rpt_temp table with order info.
  ||   Include unassigned distribution orders (DIST load) with ship dates less
  ||   than 7 days out and all other orders with ETA less than 30 days out.
  ||   Include all "No Orders". Summarized order detail counts by category from
  ||   table INVP250V will be included in the buckets of the temp table.
  ||   A 2nd insert is made to include customers with load assignments that
  ||   have not submitted an order or have submitted an order but it contains
  ||   no CIG items and more GMP than GRO items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/25/04 | rhalpai | Original
  || 04/05/07 | rhalpai | Changed bucket counts to be a left outer join so not
  ||                    | to exclude no-orders which either have no detail lines
  ||                    | or contain a line for item '999995' which does not
  ||                    | exist in the corp item table (sawp505e).
  || 05/11/07 | rhalpai | Changed to use zero for NULL bucket counts.
  ||                    | Also, change to include a Below the Line entry for
  ||                    | customer when the only regular order on the load has
  ||                    | no valid items (all bucket counts are zero because
  ||                    | none of the items ordered exists in the corp item
  ||                    | table). IM306570
  || 12/07/10 | dlbeal  | Removed insert for Below the Line.
  || 01/25/11 | dlbeal  | Added insert for Below the Line reporting for WJ. PIR9476
  || 11/28/11 | rhalpai | Add new Test status 4 to cursor. PIR10211
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE load_eoe_sum_rpt_temp_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_div_part          NUMBER;
    l_eta_dt            DATE;
    l_shp_dt            NUMBER;
    l_bo580_missing_sw  VARCHAR2(1);
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);
    l_eta_dt := TRUNC(SYSDATE) + 30;
    l_shp_dt := TRUNC(SYSDATE + 7) - DATE '1900-02-28';
    l_bo580_missing_sw := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_incl_bo580_missng), 'N');

    -- make sure temp table is empty
    DELETE FROM eoe_sum_rpt_temp;

    -- populate temp table
    INSERT INTO eoe_sum_rpt_temp
                (grp_num, cust_num, load_typ, ord_num, cust_name, LOAD, stop_num, order_stat, trans_date, eta,
                 num_ord_lines, order_src, confirm_num, fax_num, phone_num, order_typ, no_ord_flg, bucket1, bucket2,
                 bucket3, bucket4, bucket5, bucket6, orig_order_stat)
      SELECT tmp.grp_num, tmp.mcl_cust, tmp.load_typ, tmp.ord_num, tmp.cust_nm, tmp.load_num,
             (CASE
                WHEN tmp.stop_num > 99 THEN 0
                ELSE tmp.stop_num
              END) AS stop_num, tmp.ord_stat, tmp.trans_dt, tmp.eta_dt, tmp.ord_ln_cnt, tmp.ord_src, tmp.conf_num,
             tmp.fax_num, tmp.phone_num, DECODE(tmp.ord_typ, 'D', 'D', 'R') AS ord_typ,
             DECODE(tmp.ord_typ, 'N', '1', '0') AS no_ord_flg, NVL(bkt.bucket1, 0) AS bucket1,
             NVL(bkt.bucket2, 0) AS bucket2, NVL(bkt.bucket3, 0) AS bucket3, NVL(bkt.bucket4, 0) AS bucket4,
             NVL(bkt.bucket5, 0) AS bucket5, NVL(bkt.bucket6, 0) AS bucket6, ' ' AS orig_ord_stat
        FROM (SELECT   TO_NUMBER(SUBSTR(c.subgpc, 3, 3), '999') AS grp_num, cx.mccusb AS mcl_cust,
                       NVL(a.ldtypa, 'GRO') AS load_typ, a.ordnoa AS ord_num, c.namec AS cust_nm, ld.load_num,
                       se.stop_num,
                       DECODE(b.statb,
                              'O', 'OPEN',
                              'R', 'BILL',
                              'A', 'BILL',
                              'P', 'BILL',
                              'T', 'BILL',
                              'S', 'SUSP',
                              'C', 'CANC',
                              'OTHR'
                             ) AS ord_stat,
                       0 AS trans_dt,
                       TO_CHAR((CASE
                                  WHEN a.dsorda = 'D'
                                  AND (   ld.load_num = 'DIST'
                                       OR ld.load_num BETWEEN 'P00P' AND 'P99P') THEN DATE '1900-02-28' + a.shpja
                                  ELSE se.eta_ts
                                END
                               ),
                               'YYYY-MM-DD'
                              ) AS eta_dt,
                       (SELECT COUNT(*)
                          FROM ordp120b b2
                         WHERE b2.div_part = a.div_part
                           AND b2.ordnob = a.ordnoa
                           AND b2.lineb = FLOOR(b2.lineb)) AS ord_ln_cnt,
                       a.ipdtsa AS ord_src,
                       NVL(DECODE(a.ipdtsa, 'RI', a.legrfa, 'NRI', a.legrfa, 'PB', a.legrfa, a.connba),
                           ' ') AS conf_num,
                       NVL((CASE
                              WHEN a.dsorda = 'D'
                              AND (   ld.load_num = 'DIST'
                                   OR ld.load_num BETWEEN 'P00P' AND 'P99P') THEN NULL
                              ELSE c.cnfaxc
                            END
                           ),
                           ' '
                          ) AS fax_num,
                       NVL((CASE
                              WHEN a.dsorda = 'D'
                              AND (   ld.load_num = 'DIST'
                                   OR ld.load_num BETWEEN 'P00P' AND 'P99P') THEN NULL
                              ELSE c.cnphnc
                            END
                           ),
                           ' '
                          ) AS phone_num,
                       a.dsorda AS ord_typ
                  FROM ordp100a a, sysp200c c, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp120b b
                 WHERE a.div_part = l_div_part
                   AND a.dsorda IN('R', 'D', 'T')
                   AND a.stata <> 'C'
                   AND a.ordnoa = b.ordnob
                   AND c.div_part = a.div_part
                   AND c.acnoc = a.custa
                   AND c.statc IN('1', '3', '4')
                   AND ld.div_part = a.div_part
                   AND ld.load_depart_sid = a.load_depart_sid
                   AND se.div_part = a.div_part
                   AND se.load_depart_sid = a.load_depart_sid
                   AND se.cust_id = a.custa
                   AND (   (    TRUNC(se.eta_ts) < l_eta_dt
                            AND ld.load_num <> 'DIST'
                            AND ld.load_num NOT BETWEEN 'P00P' AND 'P99P'
                           )
                        OR (    a.shpja < l_shp_dt
                            AND a.dsorda = 'D'
                            AND (   ld.load_num = 'DIST'
                                 OR ld.load_num BETWEEN 'P00P' AND 'P99P')
                           )
                       )
                   AND cx.div_part = a.div_part
                   AND cx.custb = a.custa
                   AND b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.statb <> 'C'
              GROUP BY a.div_part, c.subgpc, cx.mccusb, a.ldtypa, a.ordnoa, c.namec, ld.load_num, se.stop_num, b.statb,
                       a.shpja, se.eta_ts, a.ipdtsa, a.legrfa, a.connba, c.cnfaxc, c.cnphnc, a.dsorda
              UNION ALL
              -- "No Order" Orders from Customers...
              SELECT TO_NUMBER(SUBSTR(c.subgpc, 3, 3), '999') AS grp_num, cx.mccusb AS mcl_cust,
                     NVL(a.ldtypa, 'GRO') AS load_typ, a.ordnoa AS ord_num, c.namec AS cust_nm, ld.load_num,
                     se.stop_num, '    ' AS ord_stat, 9 AS trans_dt, '1900-01-01' AS eta_dt,
                     (SELECT COUNT(*)
                        FROM ordp120b b2
                       WHERE b2.div_part = a.div_part
                         AND b2.ordnob = a.ordnoa
                         AND b2.lineb = FLOOR(b2.lineb)) AS ord_ln_cnt,
                     a.ipdtsa AS ord_src,
                     NVL(DECODE(a.ipdtsa, 'RI', a.legrfa, 'NRI', a.legrfa, 'PB', a.legrfa, a.connba), ' ') AS conf_num,
                     NVL(c.cnfaxc, ' ') AS fax_num, NVL(c.cnphnc, ' ') AS phone_num, a.dsorda AS ord_typ
                FROM ordp100a a, sysp200c c, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx
               WHERE a.div_part = l_div_part
                 AND a.dsorda = 'N'
                 AND a.excptn_sw = 'N'
                 AND c.div_part = a.div_part
                 AND c.acnoc = a.custa
                 AND c.statc IN('1', '3', '4')
                 AND ld.div_part = a.div_part
                 AND ld.load_depart_sid = a.load_depart_sid
                 AND se.div_part = a.div_part
                 AND se.load_depart_sid = a.load_depart_sid
                 AND se.cust_id = a.custa
                 AND cx.div_part = a.div_part
                 AND cx.custb = a.custa) tmp,
             (SELECT   x.ordnob AS ord_num, x.ord_stat, SUM(x.bucket1) AS bucket1, SUM(x.bucket2) AS bucket2,
                       SUM(x.bucket3) AS bucket3, SUM(x.bucket4) AS bucket4, SUM(x.bucket5) AS bucket5,
                       SUM(x.bucket6) AS bucket6
                  FROM (SELECT   b.ordnob,
                                 DECODE(b.statb,
                                        'O', 'OPEN',
                                        'R', 'BILL',
                                        'A', 'BILL',
                                        'P', 'BILL',
                                        'T', 'BILL',
                                        'S', 'SUSP',
                                        'C', 'CANC',
                                        'OTHR'
                                       ) AS ord_stat,
                                 DECODE(v.seqv, 1, COUNT(*), 0) AS bucket1, DECODE(v.seqv, 2, COUNT(*), 0) AS bucket2,
                                 DECODE(v.seqv, 3, COUNT(*), 0) AS bucket3, DECODE(v.seqv, 4, COUNT(*), 0) AS bucket4,
                                 (CASE
                                    WHEN v.seqv IN(5, 6)
                                    AND e.uome LIKE 'GM%' THEN COUNT(*)
                                    ELSE 0
                                  END) AS bucket5,
                                 (CASE
                                    WHEN(   (    v.seqv IN(5, 6)
                                             AND e.uome NOT LIKE 'GM%')
                                         OR v.seqv NOT IN(1, 2, 3, 4, 5, 6)) THEN COUNT(*)
                                    ELSE 0
                                  END
                                 ) AS bucket6
                            FROM ordp120b b, sawp505e e, mclp220d d, invp250v v
                           WHERE b.div_part = l_div_part
                             AND b.itemnb = e.iteme
                             AND b.sllumb = e.uome
                             AND b.statb <> 'C'
                             AND e.nacse = d.nacsd
                             AND d.nacshd = v.itemv
                        GROUP BY b.ordnob, b.statb, e.uome, v.seqv) x
              GROUP BY x.ordnob, x.ord_stat) bkt
       WHERE bkt.ord_num(+) = tmp.ord_num
         AND bkt.ord_stat(+) = tmp.ord_stat;

    IF l_bo580_missing_sw = 'Y' THEN
      ------------------------------------------------------
      -- Missing or incomplete orders (AKA "Below the line")
      ------------------------------------------------------
      -- Moved this code from the initial insert to after the bucket
      -- calculations in order to handle the case where the customer
      -- only has a GMP order and still needs to print in the "below
      -- the line section of the report - JBARTON 05/13/02
      -- Code was removed and then returned for WJ as part of
      -- PIR9476 - DLBEAL 01/25/11
      INSERT INTO eoe_sum_rpt_temp
                  (grp_num, cust_num, load_typ, ord_num, cust_name, LOAD, stop_num, order_stat, trans_date, eta,
                   num_ord_lines, order_src, confirm_num, fax_num, phone_num, order_typ, no_ord_flg, bucket1, bucket2,
                   bucket3, bucket4, bucket5, bucket6, orig_order_stat)
        SELECT   TO_NUMBER(SUBSTR(c.subgpc, 3, 3), '999') AS grp_num, cx.mccusb, ' ', 0, c.namec, d.loadd, d.stopd,
                 ' ', 0, '1900-01-01', 0, ' ', ' ', NVL(c.cnfaxc, ' '), NVL(c.cnphnc, ' '), ' ', '2', 0, 0, 0, 0, 0, 0,
                 ' '
            FROM mclp020b cx, mclp040d d, sysp200c c
           WHERE c.div_part = l_div_part
             AND c.statc IN('1', '3', '4')
             AND cx.div_part = c.div_part
             AND cx.custb = c.acnoc
             AND d.div_part = cx.div_part
             AND d.custd = cx.custb
             AND (
                     -- handle no order submitted
                     NOT EXISTS(SELECT 1
                                  FROM eoe_sum_rpt_temp t
                                 WHERE t.LOAD = d.loadd
                                   AND t.cust_num = cx.mccusb
                                   AND t.order_typ = 'R'
                                   AND (   t.no_ord_flg = '1'
                                        OR t.bucket1 > 0
                                        OR t.bucket2 > 0
                                        OR t.bucket3 > 0
                                        OR t.bucket4 > 0
                                        OR t.bucket5 > 0
                                        OR t.bucket6 > 0
                                       ))
                  OR
                     -- handle order submitted but NO CIG items and more GMP than GRO items
                     (    EXISTS(SELECT 1
                                   FROM eoe_sum_rpt_temp t
                                  WHERE t.LOAD = d.loadd
                                    AND t.cust_num = cx.mccusb
                                    AND t.order_typ = 'R'
                                    AND t.no_ord_flg = '0'
                                    AND t.bucket2 = 0
                                    AND t.bucket5 > t.bucket6)
                      AND NOT EXISTS(SELECT 1
                                       FROM eoe_sum_rpt_temp t
                                      WHERE t.LOAD = d.loadd
                                        AND t.cust_num = cx.mccusb
                                        AND t.order_typ = 'R'
                                        AND (   t.no_ord_flg = '1'
                                             OR t.bucket6 > t.bucket5))
                     )
                 )
        GROUP BY TO_NUMBER(SUBSTR(c.subgpc, 3, 3), '999'), cx.mccusb, c.namec, d.loadd, d.stopd, NVL(c.cnfaxc, ' '),
                 NVL(c.cnphnc, ' ');
    END IF;   -- l_bo580_missing_sw = 'Y'

    COMMIT;
  END load_eoe_sum_rpt_temp_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_RPT_LN_SP
  ||  Append data to report table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/11/14 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_rpt_ln_sp(
    i_typ         IN             VARCHAR2,
    i_hdr_grp     IN             VARCHAR2,
    i_t_heads     IN             type_stab,
    io_t_rpt_lns  IN OUT NOCOPY  typ.tas_maxvc2,
    io_ln_cnt     IN OUT         NUMBER,
    io_pg_cnt     IN OUT         NUMBER,
    i_dtl_ln      IN             VARCHAR2 DEFAULT NULL
  ) IS
    --  cc = cariage control
    l_c_cc_sp      CONSTANT VARCHAR2(1) := ' ';
    l_c_cc_new_pg  CONSTANT VARCHAR2(1) := '1';
  BEGIN
    IF (   i_typ = g_c_heading
        OR io_ln_cnt >= g_c_lns_per_pg) THEN
      io_pg_cnt := io_pg_cnt + 1;
      io_ln_cnt := 0;
      FOR i IN i_t_heads.FIRST .. i_t_heads.LAST LOOP
        util.append(io_t_rpt_lns,
                    (CASE
                       WHEN i = 1 THEN l_c_cc_new_pg
                       ELSE l_c_cc_sp
                     END)
                    || i_t_heads(i)
                    ||(CASE
                         WHEN i = 3 THEN LPAD(io_pg_cnt, 4)
                         WHEN i = 4 THEN i_hdr_grp
                       END)
                   );
        io_ln_cnt := io_ln_cnt + 1;
      END LOOP;
    END IF;   -- i_typ = g_c_heading OR io_ln_cnt >= g_c_lns_per_pg

    IF i_typ = g_c_detail THEN
      util.append(io_t_rpt_lns, l_c_cc_sp || i_dtl_ln);
      io_ln_cnt := io_ln_cnt + 1;
    END IF;   -- i_typ = g_c_detail
  END add_rpt_ln_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||---------------------------------------------------------------------------
  || ORD_LOAD_RPT_SP
  ||   Print order summary by load, customer, order.
  ||---------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||---------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||---------------------------------------------------------------------------
  || 02/13/01 | JUSTANI | Original
  || 02/27/01 | JUSTANI | Remove the to_date function from the bucket counts.
  || 03/09/01 | JUSTANI | Add logic to automate ftp proc
  || 03/30/01 | CNATIVI | Add distribution process logic
  || 04/25/01 | JBARTON | Bypass distributions until a until a third report can
  ||                    | be written with distributions
  || 05/03/01 | JBARTON | Added division check to where for "no order" extract
  ||                    | for load of eoe_sum_rpt_temp
  || 09/26/01 | rhalpai | Replaced sql_error_handler with
  ||                    | sql_utilities_pkg.sql_errors
  || 10/12/01 | rhalpai | Replaced table FSYP009I with INVP220W
  || 10/30/01 | rhalpai | Changed logic to include 7-ll 2B info from legrfa in
  ||                    | the confirmation number
  || 05/13/02 | JBARTON | Modified to handle the issue where a customer only
  ||                    | had a GMP order and was not showing up in the
  ||                    | "below the line" section of the report as was done in
  ||                    | the legacy system report
  || 06/24/02 | JBARTON | (1) Modify "Missing Orders logic to handle the case
  ||                    |     where CIG items exist on order where more GMP
  ||                    |     items exist than GRO
  ||                    | (2) Correct EXCEPTION handling to be specific for the
  ||                    |     exception being handled
  || 07/15/02 | JBARTON | Modify to use the UOM (unique code) value to
  ||                    | determine the the Grocery and GMP groupings as NACS
  ||                    | categories do not provide that distinction
  || 08/12/02 | JBARTON | Modified to print the correct item count for each
  ||                    | line printed when the order is in multiple status'
  ||                    | (i.e. OPEN and BILLED)
  || 01/14/02 | SNAGABH | Several update to improve performance.
  || 03/27/03 | JBARTON | Modified to handle the case where releases/load
  ||                    | closes were changing the order line status and
  ||                    | causing incorrect counts to be displayed
  || 02/25/04 | rhalpai | Redesigned to use common procedure to load temp table.
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 04/05/07 | rhalpai | Changed to include first 10 bytes of PO on report.
  ||                    | PIR4274
  || 12/12/07 | rhalpai | Replaced PO with Split Type. PIR5341
  || 12/07/10 | dlbeal  | Removed logic for: Detail for No-Order/Missing Order
  || 01/26/11 | dlbeal  | Returned logic for: Detail for No-Order/Missing Order
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  ||---------------------------------------------------------------------------
  */
  PROCEDURE ord_load_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm                       := 'OP_REPORTS_AND_EXTRACTS_PK.ORD_LOAD_RPT_SP';
    lar_parm                    logs.tar_parm;
    l_file_nm                   VARCHAR2(30);
    l_t_heads                   type_stab;
    l_t_rpt_lns                 typ.tas_maxvc2;
    l_ln_cnt                    PLS_INTEGER;
    l_pg_cnt                    PLS_INTEGER                         := 0;
    l_c_no_ord_ln      CONSTANT VARCHAR2(133)
      := ' ************************************************** THESE STORES HAVE NOT ORDERED **************************************************';
    l_dtl_ln                    VARCHAR2(133);
    l_save_load                 mclp120c.loadc%TYPE                 := 'ZZZZ';
    l_is_no_ord_ln              BOOLEAN;
    l_c_ord_placed     CONSTANT VARCHAR2(1)                         := '0';
    l_c_no_ord_placed  CONSTANT VARCHAR2(1)                         := '1';
    l_conf_num                  eoe_sum_rpt_temp.confirm_num%TYPE;
    l_save_cust                 eoe_sum_rpt_temp.cust_num%TYPE      := ' ';
    l_mcl_cust                  eoe_sum_rpt_temp.cust_num%TYPE;

    CURSOR l_cur_ord_load IS
      SELECT   a.LOAD, a.load_typ, a.no_ord_flg, a.cust_num, a.cust_name, a.stop_num, a.ord_num, a.order_src,
               a.order_typ, a.order_stat, a.eta, a.confirm_num, a.phone_num, a.fax_num, a.bucket1, a.bucket2,
               a.bucket3, a.bucket4, a.bucket5, a.bucket6,
               (a.bucket1 + a.bucket2 + a.bucket3 + a.bucket4 + a.bucket5 + a.bucket6) AS bucket_total,
               (SELECT SUBSTR(MAX(LPAD(1000 - sd.priorty, 3, '0') || sd.split_typ), 4)
                  FROM split_ord_op2s so, split_dmn_op8s sd
                 WHERE so.div_part = (SELECT d.div_part
                                        FROM div_mstr_di1d d
                                       WHERE d.div_id = i_div)
                   AND so.ord_num = a.ord_num
                   AND sd.split_typ = so.split_typ) AS split_typ
          FROM eoe_sum_rpt_temp a
         WHERE a.order_typ <> 'D'
           AND EXISTS(SELECT 1
                        FROM eoe_sum_rpt_temp x
                       WHERE x.LOAD = a.LOAD
                         AND x.no_ord_flg = '0')
      ORDER BY a.LOAD, a.no_ord_flg, a.cust_num, a.ord_num, a.order_stat;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivNm', i_div_nm);
    logs.info('ENTRY', lar_parm);
    l_file_nm := i_div || 'QBO580R_ftp';
    o_file_nm := g_c_file_dir || '/' || l_file_nm;
    l_t_heads :=
      type_stab
        ('REPORT: QBC-COSMOS'
         || str.ctr(NVL(i_div_nm, div_pk.div_nm_fn(i_div)), str.sp, 132 -(18 * 2))
         || LPAD('DATE: ' || TO_CHAR(SYSDATE, 'MM/DD/YY'), 18),
         RPAD('PGM ID: SP-BO580RPT', 56) || RPAD('ORDER ENTRY SUMMARY', 62) || 'TIME: '
         || TO_CHAR(SYSDATE, 'HH24:MI:SS'),
         LPAD('PAGE: ', 124),
         ' LOAD NO:  ',
         '',
         'ACCT   LOD      ORDER                           STP      INVOICE    FOUN                       GRO/  TOTAL  CONFIRM',
         'NUMBER TYP     NUMBER STORE NAME                NBR STAT DATE       TAIN CIGS CANDY SUPPL  GMP OTHER PURCHS NUMBER       SPLIT TYPE',
         '------ --- ---------- ------------------------- --- ---- ---------- ---- ---- ----- ----- ---- ----- ------ ------------ ----------'
        );

    IF op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl) THEN
      logs.dbg('Load Temp Table');
      load_eoe_sum_rpt_temp_sp(i_div);
    END IF;   -- op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl)

    logs.dbg('Process Cursor');
    FOR l_r_ord_load IN l_cur_ord_load LOOP
      -- save load
      IF l_save_load <> l_r_ord_load.LOAD THEN
        l_save_load := l_r_ord_load.LOAD;
        l_is_no_ord_ln := FALSE;
        add_rpt_ln_sp(g_c_heading, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt);
      END IF;   -- l_save_load <> l_r_ord_load.load

      -- Use confirm_num for Telxon and 7-Eleven 2B orders
      -- and input source for everything else
      IF l_r_ord_load.order_src IN('TLX', 'RI', 'NRI', 'PB') THEN
        l_conf_num := SUBSTR(l_r_ord_load.confirm_num, 1, 12);
      ELSE
        l_conf_num := SUBSTR(l_r_ord_load.order_src, 1, 12);
      END IF;   -- l_r_ord_load.order_src IN('TLX','RI','NRI','PB')

      IF l_save_cust <> l_r_ord_load.cust_num THEN
        l_save_cust := l_r_ord_load.cust_num;
        l_mcl_cust := l_r_ord_load.cust_num;
      ELSE
        l_mcl_cust := '      ';
      END IF;   -- l_save_cust = l_r_ord_load.cust_num

      IF l_r_ord_load.no_ord_flg = l_c_ord_placed THEN
        logs.dbg('Detail for Order Placed');
        l_dtl_ln := RPAD(l_mcl_cust, 6)
                    || ' '
                    || RPAD(l_r_ord_load.load_typ, 3)
                    || ' '
                    || LPAD(l_r_ord_load.ord_num, 10)
                    || ' '
                    || RPAD(l_r_ord_load.cust_name, 25)
                    || '  '
                    || LPAD(l_r_ord_load.stop_num, 2, '0')
                    || ' '
                    || RPAD(l_r_ord_load.order_stat, 4)
                    || ' '
                    || l_r_ord_load.eta
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket1, 0, ' '), 4)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket2, 0, ' '), 4)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket3, 0, ' '), 5)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket4, 0, ' '), 5)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket5, 0, ' '), 4)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket6, 0, ' '), 5)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket_total, 0, ' '), 6)
                    || ' '
                    || rpad_fn(l_conf_num, 12)
                    || ' '
                    || SUBSTR(l_r_ord_load.split_typ, 1, 10);
      ELSE
        logs.dbg('Detail for No-Order/Missing Order');

        -- print no-order line once per load
        IF NOT l_is_no_ord_ln THEN
          logs.dbg('Print No-Order Line');
          l_is_no_ord_ln := TRUE;
          add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, NULL);
          add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, l_c_no_ord_ln);
          add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, NULL);
        END IF;   -- NOT l_is_no_ord_ln

        l_dtl_ln := RPAD(l_mcl_cust, 22)
                    || RPAD(l_r_ord_load.cust_name, 25)
                    || '  '
                    || LPAD(l_r_ord_load.stop_num, 2, '0')
                    || ' '
                    || RPAD(temp_decode_fn(l_r_ord_load.no_ord_flg, l_c_no_ord_placed, 'NO ORDER IND', ' '), 12)
                    || ' '
                    || 'PH: '
                    || RPAD(l_r_ord_load.phone_num, 12)
                    || '     FAX: '
                    || l_r_ord_load.fax_num;
      END IF;   -- l_r_ord_load.no_ord_flg = l_c_ord_placed

      add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, l_dtl_ln);
    END LOOP;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_load_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || ORD_LOAD_SUM_RPT_SP
  ||   Print order summary by load, customer.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/12/01 | JUSTANI | Original - copied from bo580
  || 10/12/01 | rhalpai | Replaced table FSYP009I with INVP220W
  || 06/24/02 | JBARTON | Correct EXCEPTION handling to be specific for the
  ||                    | exception being handled
  || 08/12/02 | JBARTON | Modified to print the correct item count for each
  ||                    | line printed when the order is in multiple status'
  ||                    | (i.e. OPEN and BILLED)
  || 02/25/04 | rhalpai | Redesigned to use common procedure to load temp table.
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 12/07/10 | dlbeal  | Removed logic for: Detail for No-Order/Missing Order
  || 01/26/11 | dlbeal  | Returned logic for: Detail for No-Order/Missing Order
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_load_sum_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm                    := 'OP_REPORTS_AND_EXTRACTS_PK.ORD_LOAD_SUM_RPT_SP';
    lar_parm                    logs.tar_parm;
    l_file_nm                   VARCHAR2(30);
    l_t_heads                   type_stab;
    l_t_rpt_lns                 typ.tas_maxvc2;
    l_ln_cnt                    PLS_INTEGER;
    l_pg_cnt                    PLS_INTEGER                      := 0;
    l_c_no_ord_ln      CONSTANT VARCHAR2(133)
      := ' ************************************************** THESE STORES HAVE NOT ORDERED **************************************************';
    l_dtl_ln                    VARCHAR2(133);
    l_save_load                 mclp120c.loadc%TYPE              := 'ZZZZ';
    l_is_no_ord_ln              BOOLEAN;
    l_c_ord_placed     CONSTANT VARCHAR2(1)                      := '0';
    l_c_no_ord_placed  CONSTANT VARCHAR2(1)                      := '1';
    l_save_cust                 eoe_sum_rpt_temp.cust_num%TYPE   := ' ';
    l_mcl_cust                  eoe_sum_rpt_temp.cust_num%TYPE;

    CURSOR l_cur_ord_load_sum IS
      SELECT   a.LOAD, a.load_typ, a.no_ord_flg, a.cust_num, a.stop_num, a.eta, a.order_stat, a.cust_name, a.fax_num,
               a.phone_num, SUM(a.bucket1) bucket1, SUM(a.bucket2) bucket2, SUM(a.bucket3) bucket3,
               SUM(a.bucket4) bucket4, SUM(a.bucket5) bucket5, SUM(a.bucket6) bucket6,
               SUM(a.bucket1 + a.bucket2 + a.bucket3 + a.bucket4 + a.bucket5 + a.bucket6) bucket_total
          FROM eoe_sum_rpt_temp a
         WHERE EXISTS(SELECT 1
                        FROM eoe_sum_rpt_temp x
                       WHERE x.LOAD = a.LOAD
                         AND x.no_ord_flg = '0')
      GROUP BY a.LOAD, a.no_ord_flg, a.cust_num, a.stop_num, a.cust_name, a.fax_num, a.phone_num, a.eta, a.load_typ,
               a.order_stat
      ORDER BY a.LOAD, a.no_ord_flg, a.cust_num, a.eta, a.load_typ, a.order_stat;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivNm', i_div_nm);
    logs.info('ENTRY', lar_parm);
    l_file_nm := i_div || 'QBO585R_ftp';
    o_file_nm := g_c_file_dir || '/' || l_file_nm;
    l_t_heads :=
      type_stab('REPORT: QBC-COSMOS'
                || str.ctr(NVL(i_div_nm, div_pk.div_nm_fn(i_div)), str.sp, 132 -(18 * 2))
                || LPAD('DATE: ' || TO_CHAR(SYSDATE, 'MM/DD/YY'), 18),
                RPAD('PGM ID: SP-BO585RPT', 56)
                || RPAD('CUSTOMER ORDER SUMMARY', 63)
                || 'TIME: '
                || TO_CHAR(SYSDATE, 'HH24:MI:SS'),
                LPAD('PAGE: ', 124),
                ' LOAD NO:  ',
                '',
                'ACCT   LOD                           STP      INVOICE    FOUN                       GRO/  TOTAL',
                'NUMBER TYP STORE NAME                NBR STAT DATE       TAIN CIGS CANDY SUPPL  GMP OTHER PURCHS',
                '------ --- ------------------------- --- ---- ---------- ---- ---- ----- ----- ---- ----- ------'
               );

    IF op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl) THEN
      logs.dbg('Load Temp Table');
      load_eoe_sum_rpt_temp_sp(i_div);
    END IF;   -- op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl)

    logs.dbg('Process Cursor');
    FOR l_r_ord_load IN l_cur_ord_load_sum LOOP
      -- save load
      IF l_save_load <> l_r_ord_load.LOAD THEN
        l_save_load := l_r_ord_load.LOAD;
        l_is_no_ord_ln := FALSE;
        add_rpt_ln_sp(g_c_heading, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt);
      END IF;   -- l_save_load <> l_r_ord_load.load

      IF l_save_cust <> l_r_ord_load.cust_num THEN
        l_save_cust := l_r_ord_load.cust_num;
        l_mcl_cust := l_r_ord_load.cust_num;
      ELSE
        l_mcl_cust := '      ';
      END IF;   -- l_save_cust = l_r_ord_load.cust_num

      IF l_r_ord_load.no_ord_flg = l_c_ord_placed THEN
        logs.dbg('Detail for Order Placed');
        l_dtl_ln := RPAD(l_mcl_cust, 6)
                    || ' '
                    || RPAD(l_r_ord_load.load_typ, 3)
                    || ' '
                    || RPAD(l_r_ord_load.cust_name, 25)
                    || '  '
                    || LPAD(l_r_ord_load.stop_num, 2, '0')
                    || ' '
                    || RPAD(l_r_ord_load.order_stat, 4)
                    || ' '
                    || l_r_ord_load.eta
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket1, 0, ' '), 4)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket2, 0, ' '), 4)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket3, 0, ' '), 5)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket4, 0, ' '), 5)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket5, 0, ' '), 4)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket6, 0, ' '), 5)
                    || ' '
                    || LPAD(temp_decode_fn(l_r_ord_load.bucket_total, 0, ' '), 6);
      ELSE
        logs.dbg('Detail for No-Order/Missing Order');

        -- print no-order line once per load
        IF NOT l_is_no_ord_ln THEN
          logs.dbg('Print No-Order Line');
          l_is_no_ord_ln := TRUE;
          add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, NULL);
          add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, l_c_no_ord_ln);
          add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, NULL);
        END IF;   -- NOT l_is_no_ord_ln

        l_dtl_ln := RPAD(l_mcl_cust, 22)
                    || RPAD(l_r_ord_load.cust_name, 25)
                    || '  '
                    || LPAD(l_r_ord_load.stop_num, 2, '0')
                    || ' '
                    || RPAD(temp_decode_fn(l_r_ord_load.no_ord_flg, l_c_no_ord_placed, 'NO ORDER IND', ' '), 12)
                    || ' '
                    || 'PH: '
                    || RPAD(l_r_ord_load.phone_num, 12)
                    || '     FAX: '
                    || l_r_ord_load.fax_num;
      END IF;   -- l_r_ord_load.no_ord_flg = l_c_ord_placed

      add_rpt_ln_sp(g_c_detail, l_r_ord_load.LOAD, l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, l_dtl_ln);
    END LOOP;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_load_sum_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || ORD_GROUP_RPT_SP
  ||   Print order summary by group, customer.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/25/04 | rhalpai | Original
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_group_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                     := 'OP_REPORTS_AND_EXTRACTS_PK.ORD_GROUP_RPT_SP';
    lar_parm             logs.tar_parm;
    l_file_nm            VARCHAR2(30);
    l_t_heads            type_stab;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_ln_cnt             PLS_INTEGER;
    l_pg_cnt             PLS_INTEGER                       := 0;
    l_dtl_ln             VARCHAR2(133);
    l_save_grp           NUMBER                            := -1;
    l_save_cust          eoe_sum_rpt_temp.cust_num%TYPE    := ' ';
    l_mcl_cust           eoe_sum_rpt_temp.cust_num%TYPE;
    l_cust_nm            eoe_sum_rpt_temp.cust_name%TYPE;

    CURSOR l_cur_ord_grp IS
      SELECT   a.grp_num, a.cust_num, a.LOAD, a.stop_num, a.load_typ, a.eta, a.order_stat, a.cust_name, a.fax_num,
               a.phone_num, SUM(a.bucket1) bucket1, SUM(a.bucket2) bucket2, SUM(a.bucket3) bucket3,
               SUM(a.bucket4) bucket4, SUM(a.bucket5) bucket5, SUM(a.bucket6) bucket6,
               SUM(a.bucket1 + a.bucket2 + a.bucket3 + a.bucket4 + a.bucket5 + a.bucket6) bucket_total
          FROM eoe_sum_rpt_temp a
         WHERE a.no_ord_flg = '0'
      GROUP BY a.grp_num, a.cust_num, a.LOAD, a.stop_num, a.cust_name, a.fax_num, a.phone_num, a.eta, a.load_typ,
               a.order_stat
      ORDER BY a.grp_num, a.cust_num, a.LOAD, a.stop_num;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivNm', i_div_nm);
    logs.info('ENTRY', lar_parm);
    l_file_nm := i_div || 'EOE801_ftp';
    o_file_nm := g_c_file_dir || '/' || l_file_nm;
    l_t_heads :=
      type_stab
              ('REPORT: QBC-COSMOS'
               || str.ctr(NVL(i_div_nm, div_pk.div_nm_fn(i_div)), str.sp, 132 -(18 * 2))
               || LPAD('DATE: ' || TO_CHAR(SYSDATE, 'MM/DD/YY'), 18),
               RPAD('PGM ID: SP-EOE801RPT', 52)
               || RPAD('ORDER ENTRY SUMMARY BY GROUP', 66)
               || 'TIME: '
               || TO_CHAR(SYSDATE, 'HH24:MI:SS'),
               LPAD('PAGE: ', 124),
               '   GROUP:  ',
               '',
               'ACCT                             LOD LOAD STP      INVOICE    FOUN                       GRO/  TOTAL',
               'NUMBER STORE NAME                TYP NBR  NBR STAT DATE       TAIN CIGS CANDY SUPPL  GMP OTHER PURCHS',
               '------ ------------------------- --- ---- --- ---- ---------- ---- ---- ----- ----- ---- ----- ------'
              );

    IF op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl) THEN
      logs.dbg('Load Temp Table');
      load_eoe_sum_rpt_temp_sp(i_div);
    END IF;   -- op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl)

    FOR l_r_ord_grp IN l_cur_ord_grp LOOP
      -- save group
      IF l_save_grp <> l_r_ord_grp.grp_num THEN
        l_save_grp := l_r_ord_grp.grp_num;
        add_rpt_ln_sp(g_c_heading, LPAD(l_r_ord_grp.grp_num, 3, '0'), l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt);
      END IF;   -- l_save_load <> l_r_ord_grp.load

      IF l_save_cust <> l_r_ord_grp.cust_num THEN
        l_save_cust := l_r_ord_grp.cust_num;
        l_mcl_cust := l_r_ord_grp.cust_num;
        l_cust_nm := l_r_ord_grp.cust_name;
      ELSE
        l_mcl_cust := ' ';
        l_cust_nm := ' ';
      END IF;   -- l_save_cust = l_r_ord_grp.cust_num

      logs.dbg('Detail for Order Placed');
      l_dtl_ln := RPAD(l_mcl_cust, 6)
                  || ' '
                  || RPAD(l_cust_nm, 25)
                  || ' '
                  || RPAD(l_r_ord_grp.load_typ, 3)
                  || ' '
                  || RPAD(l_r_ord_grp.LOAD, 4)
                  || '  '
                  || LPAD(l_r_ord_grp.stop_num, 2, '0')
                  || ' '
                  || RPAD(l_r_ord_grp.order_stat, 4)
                  || ' '
                  || l_r_ord_grp.eta
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket1, 0, ' '), 4)
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket2, 0, ' '), 4)
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket3, 0, ' '), 5)
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket4, 0, ' '), 5)
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket5, 0, ' '), 4)
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket6, 0, ' '), 5)
                  || ' '
                  || LPAD(temp_decode_fn(l_r_ord_grp.bucket_total, 0, ' '), 6);
      add_rpt_ln_sp(g_c_detail, LPAD(l_r_ord_grp.grp_num, 3, '0'), l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, l_dtl_ln);
    END LOOP;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_group_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || MISSING_ORD_BY_GRP_RPT_SP
  ||   Print missing orders by group, customer.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/08/04 | rhalpai | Original
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 06/13/08 | rhalpai | Added sort by group/cust to cursor. IM419804
  || 05/13/13 | rhalpai | Change to include Div in file name. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE missing_ord_by_grp_rpt_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    i_div_nm   IN      VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_REPORTS_AND_EXTRACTS_PK.MISSING_ORD_BY_GRP_RPT_SP';
    lar_parm             logs.tar_parm;
    l_file_nm            VARCHAR2(30);
    l_t_heads            type_stab;
    l_t_rpt_lns          typ.tas_maxvc2;
    l_c_day     CONSTANT VARCHAR2(3)    := TO_CHAR(SYSDATE, 'DY');
    l_ln_cnt             PLS_INTEGER;
    l_pg_cnt             PLS_INTEGER    := 0;
    l_dtl_ln             VARCHAR2(133);
    l_save_grp           NUMBER         := -1;

    CURSOR l_cur_ord_grp(
      b_llr_day  mclp120c.llrcdc%TYPE
    ) IS
      SELECT   t.grp_num, t.cust_num, t.cust_name, t.phone_num,
               MAX(DECODE(c.llrcdc, 'MON', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) mon,
               MAX(DECODE(c.llrcdc, 'TUE', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) tue,
               MAX(DECODE(c.llrcdc, 'WED', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) wed,
               MAX(DECODE(c.llrcdc, 'THU', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) thu,
               MAX(DECODE(c.llrcdc, 'FRI', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) fri,
               MAX(DECODE(c.llrcdc, 'SAT', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) sat,
               MAX(DECODE(c.llrcdc, 'SUN', t.LOAD || '/' || LPAD(t.stop_num, 2, '0'), ' ')) sun
          FROM eoe_sum_rpt_temp t, mclp120c c
         WHERE c.loadc = t.LOAD
           AND t.no_ord_flg = '2'
           AND c.div_part = (SELECT div_part
                               FROM div_mstr_di1d
                              WHERE div_id = i_div)
           AND EXISTS(SELECT 1
                        FROM mclp020b b, mclp040d d, mclp120c c2
                       WHERE b.div_part = c.div_part
                         AND b.mccusb = t.cust_num
                         AND d.div_part = b.div_part
                         AND d.custd = b.custb
                         AND c2.div_part = d.div_part
                         AND c2.loadc = d.loadd
                         AND c2.llrcdc = b_llr_day)
      GROUP BY t.grp_num, t.cust_num, t.cust_name, t.phone_num
      ORDER BY t.grp_num, t.cust_num;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivNm', i_div_nm);
    logs.info('ENTRY', lar_parm);
    l_file_nm := i_div || 'ORD251_ftp';
    o_file_nm := g_c_file_dir || '/' || l_file_nm;
    l_t_heads :=
      type_stab
              ('REPORT: QBC-COSMOS'
               || str.ctr(NVL(i_div_nm, div_pk.div_nm_fn(i_div)), str.sp, 132 -(18 * 2))
               || LPAD('DATE: ' || TO_CHAR(SYSDATE, 'MM/DD/YY'), 18),
               RPAD('PGM ID: SP-ORD251RPT', 54)
               || RPAD('MISSING ORDERS BY GROUP', 64)
               || 'TIME: '
               || TO_CHAR(SYSDATE, 'HH24:MI:SS'),
               LPAD('PAGE: ', 124),
               '   GROUP:  ',
               '',
               'ACCT                                            MON     TUE     WED     THU     FRI     SAT     SUN',
               'NUMBER STORE NAME                PHONE        LOAD/ST LOAD/ST LOAD/ST LOAD/ST LOAD/ST LOAD/ST LOAD/ST',
               '------ ------------------------- ------------ ------- ------- ------- ------- ------- ------- -------'
              );

    IF op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl) THEN
      logs.dbg('Load Temp Table');
      load_eoe_sum_rpt_temp_sp(i_div);
    END IF;   -- op_is_table_empty_fn(g_c_ord_rpt_tmp_tbl)

    logs.dbg('Process Cursor');
    FOR l_r_ord_grp IN l_cur_ord_grp(l_c_day) LOOP
      -- save group
      IF l_save_grp <> l_r_ord_grp.grp_num THEN
        l_save_grp := l_r_ord_grp.grp_num;
        add_rpt_ln_sp(g_c_heading, LPAD(l_r_ord_grp.grp_num, 3, '0'), l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt);
      END IF;   -- l_save_load <> l_r_ord_grp.load

      IF LENGTH(l_r_ord_grp.phone_num) = 10 THEN
        l_r_ord_grp.phone_num := SUBSTR(l_r_ord_grp.phone_num, 1, 3)
                                 || '/'
                                 || SUBSTR(l_r_ord_grp.phone_num, 4, 3)
                                 || '-'
                                 || SUBSTR(l_r_ord_grp.phone_num, 7);
      END IF;   -- l_save_cust = l_r_ord_grp.cust_num

      logs.dbg('Detail for Order Placed');
      l_dtl_ln := RPAD(l_r_ord_grp.cust_num, 6)
                  || ' '
                  || RPAD(l_r_ord_grp.cust_name, 25)
                  || ' '
                  || RPAD(l_r_ord_grp.phone_num, 12)
                  || ' '
                  || RPAD(l_r_ord_grp.mon, 7)
                  || ' '
                  || RPAD(l_r_ord_grp.tue, 7)
                  || ' '
                  || RPAD(l_r_ord_grp.wed, 7)
                  || ' '
                  || RPAD(l_r_ord_grp.thu, 7)
                  || ' '
                  || RPAD(l_r_ord_grp.fri, 7)
                  || ' '
                  || RPAD(l_r_ord_grp.sat, 7)
                  || ' '
                  || RPAD(l_r_ord_grp.sun, 7);
      add_rpt_ln_sp(g_c_detail, LPAD(l_r_ord_grp.grp_num, 3, '0'), l_t_heads, l_t_rpt_lns, l_ln_cnt, l_pg_cnt, l_dtl_ln);
    END LOOP;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, g_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END missing_ord_by_grp_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || COMET_ERROR_RPT_SP
  ||   Create and email the COMET Error Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/04 | rhalpai | Original
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 09/20/10 | rhalpai | Changed cursor to include only order sources used
  ||                    | during OrderValidation and expanded msg variable.
  ||                    | IM615145
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE comet_error_rpt_sp(
    i_div     IN  VARCHAR2,
    i_div_nm  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                 := 'OP_REPORTS_AND_EXTRACTS_PK.COMET_ERROR_RPT_SP';
    lar_parm               logs.tar_parm;
    l_mail_msg             typ.t_maxvc2;
    l_is_print_head        BOOLEAN                       := TRUE;
    l_c_pg_width  CONSTANT NUMBER(3)                     := 80;
    l_div_nm               div_mstr_di1d.div_nm%TYPE;
    l_c_dt        CONSTANT VARCHAR2(8)                   := TO_CHAR(SYSDATE, 'MM/DD/YY');
    l_time        CONSTANT VARCHAR2(8)                   := TO_CHAR(SYSDATE, 'HH24:MI:SS');
    l_head1                VARCHAR2(80);
    l_head2                VARCHAR2(80);
    l_c_hd2_1     CONSTANT VARCHAR2(80)                  := 'COMET ERROR REPORT';
    l_c_head3     CONSTANT VARCHAR2(80)                := '    OrderNum McCust ExceptionMessage               Location';
    l_c_head4     CONSTANT VARCHAR2(80)               := ' ----------- ------ ------------------------------ ---------';
    l_c_prcs_is   CONSTANT prcs_typ_descr.prcs_id%TYPE   := 'Comet Error Report';
    l_c_subj      CONSTANT VARCHAR2(80)                  := 'Comet Error Report';

    CURSOR l_cur_comet_errs IS
      SELECT DECODE(a.excptn_sw, 'N', 'Good-Well', 'Y', 'Bad-Well') AS well, a.ordnoa AS ord_num,
             cx.mccusb AS mcl_cust,
             DECODE(a.hdexpa,
                    'DCH', 'DUP ORD-NOT UPLOADED',
                    'CIN', 'INACTIVE CUSTOMER',
                    'CAH', 'ACCOUNTING HOLD',
                    'ICN', 'INVALID CUSTOMER',
                    'DOO', 'DUP ORD-UPLD PER CUS',
                    a.hdexpa || ' - OTHER-EXCEPTION'
                   ) AS msg
        FROM ordp100a a, mclp020b cx
       WHERE a.div_part = (SELECT d.div_part
                             FROM div_mstr_di1d d
                            WHERE d.div_id = i_div)
         AND a.hdexpa IS NOT NULL
         AND a.hdexpa <> ' '
         AND a.stata = 'O'
         AND a.ipdtsa IN(SELECT s.ord_src
                           FROM div_mstr_di1d d, sub_prcs_ord_src s
                          WHERE d.div_id = i_div
                            AND s.div_part = d.div_part
                            AND s.prcs_id = 'ORDER VALIDATION'
                            AND s.prcs_sbtyp_cd = 'VDP')
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'DivNm', i_div_nm);
    logs.info('ENTRY', lar_parm);
    l_div_nm := NVL(i_div_nm, div_pk.div_nm_fn(i_div));
    logs.dbg('Center Div Name');
    l_head1 := RPAD(' ', 8) || str.ctr(l_div_nm, str.sp, l_c_pg_width -(8 * 2)) || l_c_dt;
    logs.dbg('Center Report Title');
    l_head2 := RPAD(' ', 8) || str.ctr(l_c_hd2_1, str.sp, l_c_pg_width -(8 * 2)) || l_time;
    logs.dbg('Mail Msg Loop');
    FOR l_r_comet_err IN l_cur_comet_errs LOOP
      IF l_is_print_head THEN
        l_is_print_head := FALSE;
        l_mail_msg := l_mail_msg || l_c_head3 || str.lf || l_c_head4 || str.lf;
      END IF;

      l_mail_msg := l_mail_msg
                    || ' '
                    || LPAD(l_r_comet_err.ord_num, 11)
                    || ' '
                    || RPAD(l_r_comet_err.mcl_cust, 6)
                    || ' '
                    || RPAD(l_r_comet_err.msg, 30)
                    || ' '
                    || l_r_comet_err.well
                    || str.lf;
    END LOOP;
    logs.dbg('Final Mail Msg');
    l_mail_msg := l_head1 || str.lf || l_head2 || str.lf || str.lf || l_mail_msg;
    logs.dbg('Email Msg');
    op_process_common_pk.notify_group_sp(i_div, l_c_prcs_is, l_c_subj, l_mail_msg);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END comet_error_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || DIST_NO_RTL_EXTR_SP
  ||   Extract of 711 Dist with no retails
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/16/21 | rhalpai | Original for PIR21225
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dist_no_rtl_extr_sp(
    i_run_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_REPORTS_AND_EXTRACTS_PK.DIST_NO_RTL_EXTR_SP';
    lar_parm               logs.tar_parm;
    l_extr_days            NUMBER;
    l_c_div       CONSTANT VARCHAR2(2)    := 'MC';
    l_c_rmt_file  CONSTANT VARCHAR2(30)   := 'MC7ER01J';
    l_local_file           VARCHAR2(30);
    l_t_rpt_lns            typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'RunTs', i_run_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_extr_days := op_parms_pk.val_fn(0, 'DIST_NO_RTL_DAYS');
    l_local_file := l_c_div || '_' || l_c_rmt_file || '_' || TO_CHAR(i_run_ts, 'YYYYMMDDHH24MISS');
    logs.dbg('Get Data');

    SELECT   LPAD(a.div_part, 4, '0')
             || a.custa
             || b.orditb
             || SUBSTR(a.legrfa, 1, 10)
             || SUBSTR(a.legrfa, 12, 2)
             || TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS extr_dat
    BULK COLLECT INTO l_t_rpt_lns
        FROM mclp020b cx, ordp100a a, ordp120b b
       WHERE cx.corpb = 713
         AND a.div_part = cx.div_part
         AND a.custa = cx.custb
         AND a.dsorda = 'D'
         AND a.stata = 'O'
         AND a.shpja <= TRUNC(i_run_ts) + l_extr_days - DATE '1900-02-28'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.hdrtab = 0
    GROUP BY a.div_part, a.custa, b.orditb, a.legrfa, a.shpja;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_local_file);
    logs.dbg('FTP');
    op_ftp_sp(l_c_div, l_local_file, l_c_rmt_file);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dist_no_rtl_extr_sp;

  /*
  ||----------------------------------------------------------------------------
  || MAIN_SP
  ||   Entry point and controlling procedure to process all reports.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/25/04 | rhalpai | Original
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  ||                    | Removed status from calls to OP_FTP_SP and
  ||                    | OP_SEND_MQ_FINISHED_MSG_SP.
  || 05/01/07 | rhalpai | Added Process Control logic to prevent it from running
  ||                    | against allocation. IM286970
  ||----------------------------------------------------------------------------
  */
  PROCEDURE main_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm               := 'OP_REPORTS_AND_EXTRACTS_PK.MAIN_SP';
    lar_parm                   logs.tar_parm;
    l_div_part                 NUMBER;
    l_div_nm                   div_mstr_di1d.div_nm%TYPE;
    l_ord_load_file_nm         VARCHAR2(30);
    l_ord_load_sum_file_nm     VARCHAR2(30);
    l_ord_grp_file_nm          VARCHAR2(30);
    l_missing_ord_grp_file_nm  VARCHAR2(30);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rpts_and_extr,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.dbg('Get Div Name');
    l_div_nm := div_pk.div_nm_fn(i_div);
    logs.dbg('Load Temp Table');
    load_eoe_sum_rpt_temp_sp(i_div);

    -- create reports
    BEGIN
      logs.dbg('Order Load Report');
      ord_load_rpt_sp(i_div, l_ord_load_file_nm, l_div_nm);
      logs.dbg('Order Load Summary Report');
      ord_load_sum_rpt_sp(i_div, l_ord_load_sum_file_nm, l_div_nm);
      logs.dbg('Order Group Report');
      ord_group_rpt_sp(i_div, l_ord_grp_file_nm, l_div_nm);
      logs.dbg('Missing Orders by Group Report');
      missing_ord_by_grp_rpt_sp(i_div, l_missing_ord_grp_file_nm, l_div_nm);
    END;

    -- FTP reports and send MQ finished msg
    DECLARE
      l_local_file                           VARCHAR2(30);
      l_c_ord_load_rmt_file         CONSTANT VARCHAR2(30) := 'BO5801';
      l_c_ord_load_sum_rmt_file     CONSTANT VARCHAR2(30) := 'EOE802';
      l_c_ord_grp_rmt_file          CONSTANT VARCHAR2(30) := 'EOE801';
      l_c_missing_ord_grp_rmt_file  CONSTANT VARCHAR2(30) := 'ORD251';
      l_c_mq_msg_id                 CONSTANT VARCHAR2(30) := 'QBO580R';
      l_c_finish_file_basename      CONSTANT VARCHAR2(30) := 'MQ_QBO580R_ftp';
      l_c_no_archive                CONSTANT VARCHAR2(1)  := 'N';

      FUNCTION get_basename_fn(
        i_val  IN  VARCHAR2
      )
        RETURN VARCHAR2 IS
      BEGIN
        RETURN SUBSTR(i_val, INSTR(i_val, '/', -1) + 1);
      END get_basename_fn;
    BEGIN
      logs.dbg('FTP Order Load Report');
      l_local_file := get_basename_fn(l_ord_load_file_nm);
      op_ftp_sp(i_div, l_local_file, l_c_ord_load_rmt_file, l_c_no_archive);
      logs.dbg('FTP Order Load Summary Report');
      l_local_file := get_basename_fn(l_ord_load_sum_file_nm);
      op_ftp_sp(i_div, l_local_file, l_c_ord_load_sum_rmt_file, l_c_no_archive);
      logs.dbg('FTP Order Group Report');
      l_local_file := get_basename_fn(l_ord_grp_file_nm);
      op_ftp_sp(i_div, l_local_file, l_c_ord_grp_rmt_file, l_c_no_archive);
      logs.dbg('FTP Missing Orders by Group Report');
      l_local_file := get_basename_fn(l_missing_ord_grp_file_nm);
      op_ftp_sp(i_div, l_local_file, l_c_missing_ord_grp_rmt_file, l_c_no_archive);
      logs.dbg('Send MQ Finished Msg');
      op_send_mq_finished_msg_sp(i_div, l_c_mq_msg_id, l_c_finish_file_basename);
    END;

    logs.dbg('COMET Error Report');
    comet_error_rpt_sp(i_div, l_div_nm);

    UPDATE mclane_mq_get g
       SET g.mq_msg_status = 'CMP'
     WHERE g.mq_msg_id = 'QORPT04'
       AND g.div_part = l_div_part
       AND g.mq_msg_status = 'OPN';

    COMMIT;
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rpts_and_extr,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.err(lar_parm);
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rpts_and_extr,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END main_sp;
END op_reports_and_extracts_pk;
/

