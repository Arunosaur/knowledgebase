CREATE OR REPLACE PACKAGE op_haz_mat_report_pk IS
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
  PROCEDURE report_sp(
    i_div            IN      VARCHAR2,
    i_is_cancl_ords  IN      BOOLEAN,
    o_file_nm        OUT     VARCHAR2,
    o_status         OUT     VARCHAR2
  );
END op_haz_mat_report_pk;
/

CREATE OR REPLACE PACKAGE BODY op_haz_mat_report_pk IS
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
  || REPORT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/25/03 | Sarat N | Removed discontinued items and updated order cancel logic
  ||                      to use NVL function on ipdtsa column. The ipdtsa column
  ||                      has NULLs for some Sam's customers resulting in not canceling
  ||                      these orders, which will be fixed with this change.
  || 04/15/04 | Sarat N | Added logic to include orders received via HZD order source and
  ||                      additional changes to make this report available for Pacific div.
  || 04/26/04 | Sarat N | Correct HazMat order lines cursor to exclude HZD order source orders.
  || 06/16/08 | rhalpai | Added sort by Cust/Item to cursor.
  || 11/10/10 | rhalpai | Remove unused columns. Convert to use standard error
  ||                    | handling logic. PIR5878
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE report_sp(
    i_div            IN      VARCHAR2,
    i_is_cancl_ords  IN      BOOLEAN,
    o_file_nm        OUT     VARCHAR2,
    o_status         OUT     VARCHAR2
  ) IS
    l_c_module          CONSTANT typ.t_maxfqnm               := 'OP_HAZ_MAT_REPORT_PK.REPORT_SP';
    lar_parm                     logs.tar_parm;
    l_div_part                   NUMBER;
    l_c_file_dir        CONSTANT VARCHAR2(50)                := '/ftptrans';
    l_file_nm                    VARCHAR2(80);
    l_div_nm                     div_mstr_di1d.div_nm%TYPE;
    l_c_sysdate         CONSTANT DATE                        := SYSDATE;
    l_t_heads                    type_stab;
    l_c_lines_per_page  CONSTANT PLS_INTEGER                 := 80;
    l_line_cnt                   PLS_INTEGER                 := 99;
    l_page_cnt                   PLS_INTEGER                 := 0;
    l_c_heading         CONSTANT VARCHAR2(1)                 := 'H';
    l_c_detail          CONSTANT VARCHAR2(1)                 := 'D';
    l_is_new_page                BOOLEAN                     := FALSE;
    l_t_rpt_lns                  typ.tas_maxvc2;
    l_prev_mcl_cust              mclp020b.mccusb%TYPE        := '~';
    l_ttl_ord_qty                PLS_INTEGER                 := 0;
    l_dtl_ln                     VARCHAR2(81)                := ' ';

    CURSOR l_cur_haz_ords(
      b_div_part  NUMBER
    ) IS
      SELECT   cx.mccusb AS mcl_cust, ld.load_num, se.stop_num, a.ordnoa AS ord_num
          FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se, mclp040d md, mclp020b cx
         WHERE a.div_part = b_div_part
           AND a.ipdtsa = 'HZD'
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND md.div_part = se.div_part
           AND md.custd = se.cust_id
           AND md.loadd = ld.load_num
           AND md.stopd = se.stop_num
           AND cx.div_part = a.div_part
           AND cx.custb = a.custa
      GROUP BY cx.mccusb, ld.load_num, se.stop_num, a.ordnoa
      ORDER BY cx.mccusb, ld.load_num, se.stop_num, a.ordnoa;

    CURSOR l_cur_haz_ord_lns(
      b_div_part  NUMBER
    ) IS
      SELECT   x.mcl_cust, x.cust_id, x.catlg_num, e.shppke AS item_pack, e.sizee AS item_sz, e.ctdsce AS item_descr,
               SUM(x.orig_qty) AS orig_qty
          FROM (SELECT cx.mccusb AS mcl_cust, a.custa AS cust_id, NVL(b.orgitb, b.orditb) AS catlg_num,
                       b.orgqtb AS orig_qty, b.sllumb
                  FROM rpt_parm_ap1e parm, ordp100a a, ordp120b b, mclp020b cx
                 WHERE parm.div_part = b_div_part
                   AND parm.rpt_nm = 'HAZMAT_RPT'
                   AND parm.rpt_typ = 'ITEM'
                   AND b.div_part = parm.div_part
                   AND (   b.orditb = parm.val_cd
                        OR b.orgitb = parm.val_cd)
                   AND b.statb = 'O'
                   AND a.div_part = b.div_part
                   AND a.ordnoa = b.ordnob
                   AND a.dsorda = 'R'
                   AND NVL(a.ipdtsa, 'XXX') NOT IN('CSRWRK', 'HZD')
                   AND cx.div_part = a.div_part
                   AND cx.custb = a.custa) x,
               sawp505e e, mclp030c c
         WHERE c.div_part = b_div_part
           AND c.taxjrc IN('AK', 'HI')
           AND e.catite = x.catlg_num
           AND e.uome = x.sllumb
           AND x.cust_id = c.custc
      GROUP BY x.mcl_cust, x.cust_id, x.catlg_num, e.shppke, e.sizee, e.ctdsce
      ORDER BY x.mcl_cust, x.catlg_num;

    PROCEDURE load_tbl_sp(
      i_typ  IN  VARCHAR2
    ) IS
      --  cc = cariage control
      l_c_cc_space     CONSTANT VARCHAR2(1) := ' ';
      l_c_cc_new_page  CONSTANT VARCHAR2(1) := '1';
    BEGIN
      IF (   i_typ = l_c_heading
          OR l_line_cnt >= l_c_lines_per_page) THEN
        l_page_cnt := l_page_cnt + 1;
        l_is_new_page := TRUE;
        l_line_cnt := 0;
        FOR i IN l_t_heads.FIRST .. l_t_heads.LAST LOOP
          util.append(l_t_rpt_lns,
                      (CASE
                         WHEN i = 1 THEN l_c_cc_new_page
                         ELSE l_c_cc_space
                       END)
                      || l_t_heads(i)
                      ||(CASE
                           WHEN i = 1 THEN LPAD(l_page_cnt, 4)
                         END)
                     );
          l_line_cnt := l_line_cnt + 1;
        END LOOP;
      END IF;   -- i_typ = l_c_heading OR l_line_cnt >= l_c_lines_per_page

      IF i_typ = l_c_detail THEN
        IF NOT l_is_new_page THEN
          -- double-space
          util.append(l_t_rpt_lns, l_c_cc_space);
          l_line_cnt := l_line_cnt + 1;
        END IF;   -- NOT l_is_new_page

        util.append(l_t_rpt_lns, l_c_cc_space || l_dtl_ln);
        l_is_new_page := FALSE;
      END IF;   -- i_typ = l_c_detail
    END load_tbl_sp;

    PROCEDURE cancl_ord_lns_sp(
      i_cust_id    IN  VARCHAR2,
      i_catlg_num  IN  VARCHAR2
    ) IS
    BEGIN
      IF i_is_cancl_ords THEN
        UPDATE ordp120b b
           SET b.statb = 'C'
         WHERE b.statb = 'O'
           AND b.div_part = l_div_part
           AND i_catlg_num IN(b.orgitb, b.orditb)
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = b.div_part
                         AND a.ordnoa = b.ordnob
                         AND a.custa = i_cust_id
                         AND NVL(a.ipdtsa, 'XXX') <> 'CSRWRK'
                         AND a.dsorda = 'R');
      END IF;   -- i_is_cancl_ords
    END cancl_ord_lns_sp;

    PROCEDURE cancl_ord_hdrs_sp IS
    BEGIN
      IF i_is_cancl_ords THEN
        -- Cancel Order Headers, if they are in open status and none of the details are in open status
        UPDATE ordp100a a
           SET a.stata = 'C'
         WHERE a.div_part = l_div_part
           AND a.stata = 'O'
           AND NOT EXISTS(SELECT 1
                            FROM ordp120b b
                           WHERE b.div_part = a.div_part
                             AND b.ordnob = a.ordnoa
                             AND b.statb IN('O', 'P', 'I', 'T', 'R', 'A', 'S'));

        -- Commit updates
        COMMIT;
      END IF;   -- i_is_cancl_ords
    END cancl_ord_hdrs_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'IsCanclOrds', i_is_cancl_ords);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_file_nm := i_div || '_OP_HAZ_MAT_REPORT';
    o_file_nm := l_c_file_dir || '/' || l_file_nm;
    l_div_nm := div_pk.div_nm_fn(i_div);
    l_t_heads := type_stab(LPAD('PAGE:', 75),
                           '',
                           RPAD('ORDER PROCESS', 25) || RPAD(l_div_nm, 31) || TO_CHAR(l_c_sysdate, 'MM/DD/YY'),
                           '',
                           LPAD('ALASKA/HAWAII FLAMMABLES', 40),
                           '',
                           '',
                           '',
                           'CUSTNO  LOAD  STOP  ORDER#',
                           '',
                           '------  ----  ----  ------'
                          );
    logs.dbg('HZD Ord Src Cursor Loop');
    -- Create a seperate page on the report for orders received via the HZD order source
    FOR l_r_haz_ord IN l_cur_haz_ords(l_div_part) LOOP
      logs.dbg('Processing results from HZD order source cursor');

      -- print each customer number only once
      IF l_prev_mcl_cust <> l_r_haz_ord.mcl_cust THEN
        l_dtl_ln := LPAD(l_r_haz_ord.mcl_cust, 6);
        l_prev_mcl_cust := l_r_haz_ord.mcl_cust;
      ELSE
        l_dtl_ln := LPAD(' ', 6);
      END IF;   -- l_prev_mcl_cust <> l_r_haz_ord.mcl_cust

      l_dtl_ln := l_dtl_ln
                  || LPAD(l_r_haz_ord.load_num, 6, ' ')
                  || LPAD(l_r_haz_ord.stop_num, 6, ' ')
                  || LPAD(l_r_haz_ord.ord_num, 8, ' ');
      load_tbl_sp(l_c_detail);
    END LOOP;
    l_t_heads := type_stab(LPAD('PAGE:', 75),
                           '',
                           RPAD('ORDER PROCESS', 25) || RPAD(l_div_nm, 31) || TO_CHAR(l_c_sysdate, 'MM/DD/YY'),
                           '',
                           LPAD('ALASKA/HAWAII FLAMMABLES', 40),
                           '',
                           '',
                           '',
                           '                 QTY',
                           '',
                           'CUSTNO  ITEMNO   ORD   PACK    SIZE           DESCRIPTION',
                           '',
                           '------  ------  -----  ----  --------  -------------------------'
                          );
    -- start a new page to list the remaining HazMat items received from other order sources
    load_tbl_sp(l_c_heading);
    logs.dbg('Non-HZD Ord Src Cursor Loop');
    FOR l_r_haz_ord_ln IN l_cur_haz_ord_lns(l_div_part) LOOP
      logs.dbg('Processing results from Non-HZD order source cursor');

      -- print each customer number only once
      IF l_prev_mcl_cust <> l_r_haz_ord_ln.mcl_cust THEN
        l_dtl_ln := LPAD(l_r_haz_ord_ln.mcl_cust, 6);
        l_prev_mcl_cust := l_r_haz_ord_ln.mcl_cust;
      ELSE
        l_dtl_ln := LPAD(' ', 6);
      END IF;   -- l_prev_mcl_cust <> l_r_haz_ord_ln.mcl_cust

      l_dtl_ln := l_dtl_ln
                  || LPAD(l_r_haz_ord_ln.catlg_num, 8)
                  || LPAD(l_r_haz_ord_ln.orig_qty, 7)
                  || LPAD(l_r_haz_ord_ln.item_pack, 6)
                  || LPAD(l_r_haz_ord_ln.item_sz, 10)
                  || LPAD(l_r_haz_ord_ln.item_descr, 27);
      load_tbl_sp(l_c_detail);
      l_ttl_ord_qty := l_ttl_ord_qty + TO_NUMBER(TRIM(l_r_haz_ord_ln.orig_qty));
      logs.dbg('Cancel matching order lines');
      cancl_ord_lns_sp(l_r_haz_ord_ln.cust_id, l_r_haz_ord_ln.catlg_num);
    END LOOP;
    cancl_ord_hdrs_sp;
    logs.dbg('Print Totals');
    l_dtl_ln := LPAD('=====', 21);
    load_tbl_sp(l_c_detail);
    l_dtl_ln := LPAD(l_ttl_ord_qty, 21);
    load_tbl_sp(l_c_detail);
    logs.dbg('Write Report File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END report_sp;
END op_haz_mat_report_pk;
/

