CREATE OR REPLACE PACKAGE op_misc_reports_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_rpt_ln IS RECORD(
    col_01  VARCHAR2(100),
    col_02  VARCHAR2(100),
    col_03  VARCHAR2(100),
    col_04  VARCHAR2(100),
    col_05  VARCHAR2(100),
    col_06  VARCHAR2(100),
    col_07  VARCHAR2(100),
    col_08  VARCHAR2(100),
    col_09  VARCHAR2(100),
    col_10  VARCHAR2(100),
    col_11  VARCHAR2(100),
    col_12  VARCHAR2(100),
    col_13  VARCHAR2(100),
    col_14  VARCHAR2(100),
    col_15  VARCHAR2(100),
    col_16  VARCHAR2(100),
    col_17  VARCHAR2(100),
    col_18  VARCHAR2(100),
    col_19  VARCHAR2(100),
    col_20  VARCHAR2(100)
  );

  TYPE g_tt_rpt_ln IS TABLE OF g_rt_rpt_ln;

  TYPE g_cvt_rpt IS REF CURSOR
    RETURN g_rt_rpt_ln;

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_portrait   CONSTANT VARCHAR2(1)     := 'P';
  g_c_landscape  CONSTANT VARCHAR2(1)     := 'L';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE gen_report_sp(
    i_div                IN  VARCHAR2,
    i_file_dir           IN  VARCHAR2,
    i_file_nm            IN  VARCHAR2,
    i_rpt_id             IN  VARCHAR2,
    i_rpt_nm             IN  VARCHAR2,
    i_t_grps             IN  type_stab,
    i_t_heads            IN  type_stab,
    i_cur                IN  g_cvt_rpt,
    i_rpt_orient         IN  VARCHAR2 DEFAULT g_c_portrait,
    i_spc_aft_col        IN  NUMBER DEFAULT NULL,
    i_dbl_spc_sw         IN  VARCHAR2 DEFAULT 'N',
    i_ftp_rmt_file       IN  VARCHAR2 DEFAULT NULL,
    i_skip_empty_cur_sw  IN  VARCHAR2 DEFAULT 'N',
    i_arc_aftr_ftp_sw    IN  VARCHAR2 DEFAULT 'Y',
    i_ftp_to_gdg_sw      IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE item_ration_rpt_sp(
    i_div      IN      VARCHAR2,
    i_rlse_ts  IN      DATE,
    o_file_nm  OUT     VARCHAR2
  );

  PROCEDURE skipld_items_rpt_sp(
    i_div  IN  VARCHAR2
  );

  PROCEDURE skipld_move_rpt_sp(
    i_div     IN  VARCHAR2,
    i_log_ts  IN  DATE
  );

  PROCEDURE skipld_detail_rpt_sp(
    i_div  IN  VARCHAR2
  );

  PROCEDURE skipld_sum_rpt_sp(
    i_div  IN  VARCHAR2
  );

  PROCEDURE hazmat_rpt_sp(
    i_div  IN  VARCHAR2
  );

  PROCEDURE strct_vndr_recap_rpt_sp(
    i_div           IN  VARCHAR2,
    i_ftp_rmt_file  IN  VARCHAR2 DEFAULT NULL,
    i_recap_ts      IN  DATE DEFAULT NULL
  );

  PROCEDURE strct_ord_dtl_rpt_sp(
    i_div           IN  VARCHAR2,
    i_ftp_rmt_file  IN  VARCHAR2 DEFAULT NULL,
    i_bill_dt       IN  DATE DEFAULT SYSDATE - 1
  );

  PROCEDURE opxd01_rpt_sp(
    i_div          IN  VARCHAR2,
    i_rpt_ts       IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  );

  PROCEDURE opxd02_rpt_sp(
    i_div          IN  VARCHAR2,
    i_rpt_ts       IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  );

  PROCEDURE opxd03_rpt_sp(
    i_div          IN  VARCHAR2,
    i_rpt_ts       IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  );

  PROCEDURE wawa_po_rpt_sp(
    i_email_addr  IN  VARCHAR2 DEFAULT 'Wawa.divisions@mclaneco.com'
  );
END op_misc_reports_pk;
/

CREATE OR REPLACE PACKAGE BODY op_misc_reports_pk IS
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
  || GEN_REPORT_SP
  ||  Generic report creation.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original
  || 11/06/07 | rhalpai | Added p_is_dbl_space parm. Added logic to handle
  ||                    | multi-line text (via in-line CHR(10)) within cursor.
  || 08/01/08 | rhalpai | Added parms and logic to ftp to mainframe location and
  ||                    | to skip report creation for empty cursor. PIR5002
  || 08/25/09 | rhalpai | Added archive_after_ftp and is_ftp_to_gdg parms.
  ||                    | PIR7342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_report_sp(
    i_div                IN  VARCHAR2,
    i_file_dir           IN  VARCHAR2,
    i_file_nm            IN  VARCHAR2,
    i_rpt_id             IN  VARCHAR2,
    i_rpt_nm             IN  VARCHAR2,
    i_t_grps             IN  type_stab,
    i_t_heads            IN  type_stab,
    i_cur                IN  g_cvt_rpt,
    i_rpt_orient         IN  VARCHAR2 DEFAULT g_c_portrait,
    i_spc_aft_col        IN  NUMBER DEFAULT NULL,
    i_dbl_spc_sw         IN  VARCHAR2 DEFAULT 'N',
    i_ftp_rmt_file       IN  VARCHAR2 DEFAULT NULL,
    i_skip_empty_cur_sw  IN  VARCHAR2 DEFAULT 'N',
    i_arc_aftr_ftp_sw    IN  VARCHAR2 DEFAULT 'Y',
    i_ftp_to_gdg_sw      IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_MISC_REPORTS_PK.GEN_REPORT_SP';
    lar_parm               logs.tar_parm;
    l_lines_per_page       NUMBER(3);
    l_line_sz              PLS_INTEGER;
    l_sz                   PLS_INTEGER;
    l_line_cnt             PLS_INTEGER    := 99;
    l_page_cnt             PLS_INTEGER    := 0;
    l_t_rpt_lns            typ.tas_maxvc2;
    l_c_rpt_date  CONSTANT VARCHAR2(10)   := TO_CHAR(SYSDATE, 'MM/DD/YY');
    l_c_rpt_time  CONSTANT VARCHAR2(8)    := TO_CHAR(SYSDATE, 'HH24:MI:SS');
    l_c_heading   CONSTANT VARCHAR2(1)    := 'H';
    l_c_detail    CONSTANT VARCHAR2(1)    := 'D';
    l_t_genrpt_lns         g_tt_rpt_ln;
    l_idx                  PLS_INTEGER;
    l_t_grps               type_stab;
    l_grp_compare          typ.t_maxvc2;
    l_grp_save             typ.t_maxvc2   := '~';
    l_h1_line              VARCHAR2(132);
    l_h2_line              VARCHAR2(132);
    l_h3_line              VARCHAR2(132);
    l_detail_line          typ.t_maxvc2;
    l_col_save             VARCHAR2(132)  := '~';
    l_is_xtra_space        BOOLEAN        := FALSE;
    l_is_new_page          BOOLEAN        := FALSE;

    FUNCTION col_fn(
      i_val           IN  PLS_INTEGER,
      i_t_genrpt_lns  IN  g_tt_rpt_ln,
      i_idx           IN  PLS_INTEGER
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN((CASE i_val
                WHEN 1 THEN i_t_genrpt_lns(i_idx).col_01
                WHEN 2 THEN i_t_genrpt_lns(i_idx).col_02
                WHEN 3 THEN i_t_genrpt_lns(i_idx).col_03
                WHEN 4 THEN i_t_genrpt_lns(i_idx).col_04
                WHEN 5 THEN i_t_genrpt_lns(i_idx).col_05
                WHEN 6 THEN i_t_genrpt_lns(i_idx).col_06
                WHEN 7 THEN i_t_genrpt_lns(i_idx).col_07
                WHEN 8 THEN i_t_genrpt_lns(i_idx).col_08
                WHEN 9 THEN i_t_genrpt_lns(i_idx).col_09
                WHEN 10 THEN i_t_genrpt_lns(i_idx).col_10
                WHEN 11 THEN i_t_genrpt_lns(i_idx).col_11
                WHEN 12 THEN i_t_genrpt_lns(i_idx).col_12
                WHEN 13 THEN i_t_genrpt_lns(i_idx).col_13
                WHEN 14 THEN i_t_genrpt_lns(i_idx).col_14
                WHEN 15 THEN i_t_genrpt_lns(i_idx).col_15
                WHEN 16 THEN i_t_genrpt_lns(i_idx).col_16
                WHEN 17 THEN i_t_genrpt_lns(i_idx).col_17
                WHEN 18 THEN i_t_genrpt_lns(i_idx).col_18
                WHEN 19 THEN i_t_genrpt_lns(i_idx).col_19
                WHEN 20 THEN i_t_genrpt_lns(i_idx).col_20
              END
             )
            );
    END col_fn;

    FUNCTION grp_compare_fn(
      i_t_genrpt_lns  IN  g_tt_rpt_ln,
      i_idx           IN  PLS_INTEGER
    )
      RETURN VARCHAR2 IS
      l_grp   VARCHAR2(132);
      l_grps  typ.t_maxvc2;
    BEGIN
      l_t_grps := type_stab();
      FOR i IN i_t_grps.FIRST .. i_t_grps.LAST LOOP
        l_grp := col_fn(i, i_t_genrpt_lns, i_idx);
        util.append(l_t_grps, i_t_grps(i) || l_grp);
        l_grps := l_grps || l_grp;
      END LOOP;
      RETURN(l_grps);
    END grp_compare_fn;

    FUNCTION detail_fn(
      i_t_genrpt_lns  IN  g_tt_rpt_ln,
      i_idx           IN  PLS_INTEGER
    )
      RETURN VARCHAR2 IS
      l_start  PLS_INTEGER;
      l_col    VARCHAR2(132);
      l_dtl    typ.t_maxvc2;
    BEGIN
      l_start :=(CASE
                   WHEN i_t_grps IS NULL THEN 1
                   ELSE i_t_grps.COUNT + 1
                 END);
      FOR i IN l_start .. 20 LOOP
        l_col := col_fn(i, i_t_genrpt_lns, i_idx);

        IF (    i = i_spc_aft_col
            AND l_col_save <> l_col) THEN
          l_col_save := l_col;
          l_is_xtra_space := TRUE;
        END IF;   -- i = i_spc_aft_col AND l_col_save <> l_col

        l_dtl := l_dtl ||(CASE
                            WHEN(    l_dtl IS NOT NULL
                                 AND l_col IS NOT NULL) THEN ' '
                          END) || l_col;
      END LOOP;
      RETURN(l_dtl);
    END detail_fn;

    PROCEDURE load_tbl_sp(
      i_typ  IN  VARCHAR2
    ) IS
      --  cc = cariage control
      l_c_cc_space     CONSTANT VARCHAR2(1) := ' ';
      l_c_cc_new_page  CONSTANT VARCHAR2(1) := '1';
      l_t_dtl_lns               type_stab;
    BEGIN
      IF (   i_typ = l_c_heading
          OR l_line_cnt >= l_lines_per_page) THEN
        l_page_cnt := l_page_cnt + 1;
        l_is_new_page := TRUE;
        util.append(l_t_rpt_lns, l_c_cc_new_page || l_h1_line || LPAD(l_page_cnt, 4));
        util.append(l_t_rpt_lns, l_c_cc_space || l_h2_line);
        util.append(l_t_rpt_lns, l_c_cc_space || l_h3_line);
        l_line_cnt := 3;

        IF l_t_grps IS NOT NULL THEN
          FOR i IN l_t_grps.FIRST .. l_t_grps.LAST LOOP
            util.append(l_t_rpt_lns, l_c_cc_space || l_t_grps(i));
            l_line_cnt := l_line_cnt + 1;
          END LOOP;
        END IF;   -- l_t_grps IS NOT NULL

        util.append(l_t_rpt_lns, l_c_cc_space);
        l_line_cnt := l_line_cnt + 1;

        IF i_t_heads IS NOT NULL THEN
          FOR i IN i_t_heads.FIRST .. i_t_heads.LAST LOOP
            util.append(l_t_rpt_lns, l_c_cc_space || i_t_heads(i));
            l_line_cnt := l_line_cnt + 1;
          END LOOP;
        END IF;   -- i_t_heads IS NOT NULL
      END IF;   -- i_type = l_c_heading OR l_line_cnt >= l_c_lines_per_page

      IF i_typ = l_c_detail THEN
        IF NOT l_is_new_page THEN
          IF i_dbl_spc_sw = 'Y' THEN
            util.append(l_t_rpt_lns, l_c_cc_space);
            l_line_cnt := l_line_cnt + 1;
          END IF;   -- i_dbl_spc_sw = 'Y'

          IF l_is_xtra_space THEN
            util.append(l_t_rpt_lns, l_c_cc_space);
            l_line_cnt := l_line_cnt + 1;
          END IF;   -- l_is_xtra_space
        END IF;   -- NOT l_is_new_page

        -- Handle "New Line" character within detail line
        l_t_dtl_lns := str.parse_list(l_detail_line, cnst.newline_char, 'N');
        FOR i IN l_t_dtl_lns.FIRST .. l_t_dtl_lns.LAST LOOP
          util.append(l_t_rpt_lns, l_c_cc_space || l_t_dtl_lns(i));
          l_line_cnt := l_line_cnt + 1;
        END LOOP;
        l_is_xtra_space := FALSE;
        l_is_new_page := FALSE;
      END IF;   -- i_typ = l_c_detail
    END load_tbl_sp;

    PROCEDURE print_sp IS
    BEGIN
      IF l_t_rpt_lns.COUNT > 0 THEN
        logs.dbg('Write File');
        write_sp(l_t_rpt_lns, i_file_nm, i_file_dir);

        IF i_ftp_rmt_file IS NOT NULL THEN
          logs.dbg('FTP to mainframe');
          op_ftp_sp(i_div, i_file_nm, i_ftp_rmt_file, i_arc_aftr_ftp_sw, i_ftp_to_gdg_sw);
        END IF;   -- i_ftp_rmt_file IS NOT NULL
      END IF;   -- l_t_rpt_lns.COUNT > 0
    END print_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FileDir', i_file_dir);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.add_parm(lar_parm, 'RptId', i_rpt_id);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'Grps', i_t_grps);
    logs.add_parm(lar_parm, 'Heads', i_t_heads);
    logs.add_parm(lar_parm, 'RptOrient', i_rpt_orient);
    logs.add_parm(lar_parm, 'SpaceAftCol', i_spc_aft_col);
    logs.add_parm(lar_parm, 'DblSpcSw', i_dbl_spc_sw);
    logs.add_parm(lar_parm, 'FtpRmtFile', i_ftp_rmt_file);
    logs.add_parm(lar_parm, 'SkipEmptyCurSw', i_skip_empty_cur_sw);
    logs.add_parm(lar_parm, 'ArcAftrFtpSw', i_arc_aftr_ftp_sw);
    logs.add_parm(lar_parm, 'FtpToGdgSw', i_ftp_to_gdg_sw);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Div is required');
    excp.assert((i_file_dir IS NOT NULL), 'File Directory is required');
    excp.assert((i_file_nm IS NOT NULL), 'File Name is required');
    excp.assert((i_rpt_id IS NOT NULL), 'Report ID is required');
    excp.assert((i_rpt_nm IS NOT NULL), 'Report Name is required');

    IF i_rpt_orient = g_c_portrait THEN
      l_lines_per_page := 80;
      l_line_sz := 80;
    ELSE
      l_lines_per_page := 60;
      l_line_sz := 132;
    END IF;   -- i_rpt_orient = g_c_portrait

    logs.dbg('Set Heading Lines');
    l_sz := GREATEST(8 + LENGTH(i_rpt_id), 14);
    l_h1_line := RPAD('REPORT: ' || i_rpt_id, l_sz)
                 || str.ctr(div_pk.div_nm_fn(i_div), str.sp, l_line_sz -(l_sz * 2))
                 || LPAD('PAGE: ', l_sz - 8);
    l_h2_line := RPAD(' ', 14) || str.ctr(TRIM(UPPER(i_rpt_nm)), str.sp, l_line_sz - 28) || 'DATE: ' || l_c_rpt_date;
    l_h3_line := LPAD('TIME: ' || l_c_rpt_time, l_line_sz);
    logs.dbg('Fetch Cursor');

    FETCH i_cur
    BULK COLLECT INTO l_t_genrpt_lns;

    IF i_cur%ROWCOUNT > 0 THEN
      l_idx := l_t_genrpt_lns.FIRST;
      LOOP
        EXIT WHEN l_idx IS NULL;

        IF i_t_grps IS NOT NULL THEN
          logs.dbg('Compare Grouping');
          l_grp_compare := grp_compare_fn(l_t_genrpt_lns, l_idx);

          IF l_grp_save <> l_grp_compare THEN
            l_grp_save := l_grp_compare;
            logs.dbg('Load Heading for Grouping Change');
            load_tbl_sp(l_c_heading);
          END IF;   -- l_grp_save <> l_grp_compare
        END IF;   -- i_t_grps IS NOT NULL

        logs.dbg('Build Detail Line');
        l_detail_line := detail_fn(l_t_genrpt_lns, l_idx);
        logs.dbg('Load Detail Line');
        load_tbl_sp(l_c_detail);
        l_idx := l_t_genrpt_lns.NEXT(l_idx);
      END LOOP;
    ELSIF i_skip_empty_cur_sw = 'N' THEN
      logs.dbg('Load Heading for Empty Report');
      load_tbl_sp(l_c_heading);
    END IF;   -- i_cur%ROWCOUNT > 0

    logs.dbg('Print');
    print_sp;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_report_sp;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_RATION_RPT_SP
  ||   Create item ration report if rationing was applied for release timestamp.
  ||   The file base-name will be returned if a report is created, otherwise,
  ||   it will be null. The file base-name will end with a 'T' if created for a
  ||   Test-Bill.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/02/05 | rhalpai | Original - PIR1289
  || 08/26/10 | rhalpai | Changed to use standard error hanlder. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE item_ration_rpt_sp(
    i_div      IN      VARCHAR2,
    i_rlse_ts  IN      DATE,
    o_file_nm  OUT     VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.ITEM_RATION_RPT_SP';
    lar_parm               logs.tar_parm;
    l_c_rpt_id    CONSTANT VARCHAR2(10)  := i_div || 'RATION';
    l_rpt_nm               VARCHAR2(100);
    l_c_file_dir  CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm   CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps               type_stab;
    l_t_heads              type_stab;
    l_cv_rpt               g_cvt_rpt;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    o_file_nm := l_c_file_nm;
    l_rpt_nm := 'OP ITEM RATIONING REPORT';
    l_t_heads := type_stab('RELEASE: ' || TO_CHAR(i_rlse_ts, 'YYYYMMDDHHMISS'),
                           ' ',
                           '                                               QTY     QTY     CUST RATION',
                           'ITEM   DESCRIPTION               PACK SIZE     AVAIL   DEMAND  CNT  PCT',
                           '------ ------------------------- ---- -------- ------- ------- ---- ------'
                          );
    logs.dbg('Open Cursor');

    OPEN l_cv_rpt
     FOR
       SELECT   e.catite AS catlg_num, rpad_fn(e.ctdsce, 25) AS descr, lpad_fn(e.shppke, 4) AS pack,
                rpad_fn(e.sizee, 8) AS sz, lpad_fn(r.qty_avail, 7) AS qty_avail, lpad_fn(r.qty_dmd, 7) AS qty_dmd,
                lpad_fn(r.cust_cnt, 4) AS cust_cnt,
                lpad_fn((CASE
                           WHEN r.cust_cnt > r.qty_avail THEN 0
                           ELSE ROUND(r.qty_avail / r.qty_dmd, 2) * 100
                         END),
                        5
                       )
                || '%' AS ration_pct,
                '', '', '', '', '', '', '', '', '', '', '', ''
           FROM div_mstr_di1d d, ration_item_log_rl1i r, sawp505e e
          WHERE d.div_id = i_div
            AND r.div_part = d.div_part
            AND r.release_ts = i_rlse_ts
            AND e.catite = r.item_num
       ORDER BY e.catite;

    logs.dbg('Generate Report');
    gen_report_sp(i_div, l_c_file_dir, l_c_file_nm, l_c_rpt_id, l_rpt_nm, l_t_grps, l_t_heads, l_cv_rpt, g_c_portrait);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_ration_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || SKIPLD_ITEMS_RPT_SP
  ||  Create "SKIPLD" Item Recap Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original - created for PIR3937
  || 06/16/08 | rhalpai | Added sort by Grouping/Item to cursor.
  || 08/26/10 | rhalpai | Changed to use standard error hanlder. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE skipld_items_rpt_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.SKIPLD_ITEMS_RPT_SP';
    lar_parm                logs.tar_parm;
    l_c_skip_load  CONSTANT VARCHAR2(6)   := 'SKIPLD';
    l_c_rpt_id     CONSTANT VARCHAR2(10)  := i_div || 'SKPLDI';
    l_rpt_nm                VARCHAR2(100);
    l_c_file_dir   CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm    CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps                type_stab;
    l_t_heads               type_stab;
    l_cv_rpt                g_cvt_rpt;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_grps := type_stab('SECTION : ');
    l_t_heads := type_stab('ITEM # PACK SIZE     DESCRIPTION               CASE UPC        SLOT',
                           '------ ---- -------- ------------------------- --------------- -------'
                          );
    logs.dbg('Set Report Name');

    SELECT rn.rpt_nm || ' ITEM RECAP'
      INTO l_rpt_nm
      FROM div_mstr_di1d d, rpt_name_ap7r rn
     WHERE d.div_id = i_div
       AND rn.div_part = d.div_part
       AND rn.user_id = l_c_skip_load;

    logs.dbg('Open Cursor');

    OPEN l_cv_rpt
     FOR
       SELECT   rp.rpt_typ, lpad_fn(rp.val_cd, 6), lpad_fn(e.shppke, 4), rpad_fn(e.sizee, 8), rpad_fn(e.ctdsce, 25),
                rpad_fn(e.upccse, 15) case_upc,
                (SELECT lpad_fn(MAX(w.aislc || w.binc || w.levlc), 7)
                   FROM whsp300c w
                  WHERE w.div_part = rp.div_part
                    AND w.itemc = e.iteme
                    AND w.uomc = e.uome
                    AND (   e.uome LIKE 'C%'
                         OR w.taxjrc IS NULL)) slot,
                '', '', '', '', '', '', '', '', '', '', '', '', ''
           FROM div_mstr_di1d d, rpt_name_ap7r rn, rpt_parm_ap1e rp, sawp505e e
          WHERE d.div_id = i_div
            AND rn.div_part = d.div_part
            AND rn.user_id = l_c_skip_load
            AND rp.div_part = rn.div_part
            AND rp.rpt_nm = rn.rpt_nm
            AND e.catite(+) = rp.val_cd
       GROUP BY rp.div_part, rp.rpt_typ, rp.val_cd, e.shppke, e.sizee, e.ctdsce, e.upccse, e.iteme, e.uome
       ORDER BY 1, 2;

    logs.dbg('Generate Report');
    gen_report_sp(i_div, l_c_file_dir, l_c_file_nm, l_c_rpt_id, l_rpt_nm, l_t_grps, l_t_heads, l_cv_rpt, g_c_portrait);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END skipld_items_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || SKIPLD_MOVE_RPT_SP
  ||  Create "SKIPLD" Move Log Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original - created for PIR3937
  || 08/26/10 | rhalpai | Changed to use standard error hanlder. PIR8531
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE skipld_move_rpt_sp(
    i_div     IN  VARCHAR2,
    i_log_ts  IN  DATE
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.SKIPLD_MOVE_RPT_SP';
    lar_parm                logs.tar_parm;
    l_c_skip_load  CONSTANT VARCHAR2(6)   := 'SKIPLD';
    l_c_rpt_id     CONSTANT VARCHAR2(10)  := i_div || 'SKPLDM';
    l_rpt_nm                VARCHAR2(100);
    l_c_file_dir   CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm    CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps                type_stab;
    l_t_heads               type_stab;
    l_cv_rpt                g_cvt_rpt;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LogTS', i_log_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_heads := type_stab('                            BEFORE..................... AFTER......................',
                           'ORDER #     CUST   CBR CUST LOAD ST LLR      ETA        LOAD ST LLR      ETA',
                           '----------- ------ -------- ---- -- ------ ------------ ---- -- ------ ------------'
                          );
    logs.dbg('Set Report Name');

    SELECT rn.rpt_nm || ' MOVE LOG'
      INTO l_rpt_nm
      FROM div_mstr_di1d d, rpt_name_ap7r rn
     WHERE d.div_id = i_div
       AND rn.div_part = d.div_part
       AND rn.user_id = l_c_skip_load;

    logs.dbg('Open Cursor');

    OPEN l_cv_rpt
     FOR
       SELECT   LPAD(d.ordnod, 11), lpad_fn(cx.mccusb, 6), lpad_fn(a.custa, 8), SUBSTR(d.descd, 12, 4) AS b_ld,
                SUBSTR(d.descd, 17, 2) AS b_st, SUBSTR(d.descd, 20, 6) AS b_llr,
                TO_CHAR(TO_DATE(SUBSTR(d.descd, 27, 10), 'YYMMDDHH24MI'), 'YYMMDD HH24:MI') AS b_eta,
                SUBSTR(d.itemd, 1, 4) AS a_ld, SUBSTR(d.itemd, 6, 2) AS a_st, SUBSTR(d.itemd, 9, 6) AS a_llr,
                TO_CHAR(TO_DATE(SUBSTR(d.itemd, 16, 10), 'YYMMDDHH24MI'), 'YYMMDD HH24:MI') AS a_eta, '', '', '', '',
                '', '', '', '', ''
           FROM div_mstr_di1d dv, mclp300d d, ordp100a a, mclp020b cx
          WHERE dv.div_id = i_div
            AND d.div_part = dv.div_part
            AND d.ordlnd = 0
            AND d.reasnd = l_c_skip_load
            AND d.last_chg_ts >= i_log_ts
            AND a.div_part = d.div_part
            AND a.ordnoa = d.ordnod
            AND a.excptn_sw = 'N'
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
       ORDER BY d.ordnod;

    logs.dbg('Generate Report');
    gen_report_sp(i_div, l_c_file_dir, l_c_file_nm, l_c_rpt_id, l_rpt_nm, l_t_grps, l_t_heads, l_cv_rpt, g_c_landscape);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END skipld_move_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || SKIPLD_DETAIL_RPT_SP
  ||  Create "SKIPLD" Order Detail Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original - created for PIR3937
  || 11/13/06 | rhalpai | Changed report to include break on order type and to
  ||                    | include orders regardless of whether they have been
  ||                    | moved by the "Skip Load" process. IM267352
  || 06/16/08 | rhalpai | Added sort by Grouping/Corp/LLR/OrdTyp/Load/Stop/OrdNum/Item
  ||                    | to cursor.
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 08/26/10 | rhalpai | Changed to use standard error hanlder. PIR8531
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F, OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE skipld_detail_rpt_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.SKIPLD_DETAIL_RPT_SP';
    lar_parm                  logs.tar_parm;
    l_c_skip_load    CONSTANT VARCHAR2(6)   := 'SKIPLD';
    l_c_rpt_id       CONSTANT VARCHAR2(10)  := i_div || 'SKPLDD';
    l_rpt_nm                  VARCHAR2(100);
    l_c_file_dir     CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm      CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps                  type_stab;
    l_t_heads                 type_stab;
    l_cv_rpt                  g_cvt_rpt;
    l_c_spc_aft_col  CONSTANT NUMBER(2)     := 8;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_grps := type_stab('SECTION  : ', 'CORP     : ', 'LLR DATE : ', 'ORDER TYP: ');
    l_t_heads :=
      type_stab
        ('LOAD ST CUST   CBR CUST ORDER #     ITEM # PACK SIZE     DESCRIPTION               CASE UPC        SLOT    ORD QTY ADJUSTMENT',
         '---- -- ------ -------- ----------- ------ ---- -------- ------------------------- --------------- ------- ------- ----------'
        );
    logs.dbg('Set Report Name');

    SELECT rn.rpt_nm || ' ORDER DETAIL REPORT'
      INTO l_rpt_nm
      FROM div_mstr_di1d d, rpt_name_ap7r rn
     WHERE d.div_id = i_div
       AND rn.div_part = d.div_part
       AND rn.user_id = l_c_skip_load;

    logs.dbg('Open Cursor');

    OPEN l_cv_rpt
     FOR
       SELECT   rp.rpt_typ, LPAD(cx.corpb, 3, '0'), TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr,
                DECODE(a.dsorda, 'D', 'DIS', 'REG'), lpad_fn(ld.load_num, 4), LPAD(se.stop_num, 2, '0'),
                lpad_fn(cx.mccusb, 6), lpad_fn(cx.custb, 8), LPAD(b.ordnob, 11), lpad_fn(e.catite, 6),
                lpad_fn(e.shppke, 4), rpad_fn(e.sizee, 8), rpad_fn(e.ctdsce, 25), rpad_fn(e.upccse, 15) AS case_upc,
                (SELECT lpad_fn(MAX(w.aislc || w.binc || w.levlc), 7)
                   FROM whsp300c w
                  WHERE w.div_part = rp.div_part
                    AND w.itemc = e.iteme
                    AND w.uomc = e.uome
                    AND (   e.uome LIKE 'C%'
                         OR w.taxjrc IS NULL)) AS slot,
                lpad_fn(SUM(b.ordqtb), 7) AS ord_qty, '__________', '', '', ''
           FROM div_mstr_di1d d, rpt_name_ap7r rn, rpt_parm_ap1e rp, sawp505e e, mclp020b cx, ordp100a a,
                load_depart_op1f ld, stop_eta_op1g se, ordp120b b, mclp040d md
          WHERE d.div_id = i_div
            AND rn.div_part = d.div_part
            AND rn.user_id = l_c_skip_load
            AND rp.div_part = rn.div_part
            AND rp.rpt_nm = rn.rpt_nm
            AND rp.val_cd = e.catite
            AND cx.div_part = d.div_part
            AND LPAD(cx.corpb, 3, '0') = rp.user_id
            AND a.div_part = cx.div_part
            AND a.custa = cx.custb
            AND a.excptn_sw = 'N'
            AND a.stata = 'O'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = cx.custb
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.excptn_sw = 'N'
            AND b.statb = 'O'
            AND b.itemnb = e.iteme
            AND b.sllumb = e.uome
            AND md.div_part = ld.div_part
            AND md.custd = a.custa
            AND md.loadd = ld.load_num
       GROUP BY rp.div_part, rp.rpt_typ, cx.corpb, ld.llr_dt, a.dsorda, ld.load_num, se.stop_num, cx.mccusb, cx.custb,
                b.ordnob, e.catite, e.shppke, e.sizee, e.ctdsce, e.upccse, e.iteme, e.uome
       ORDER BY 1, 2, 3, 4, 5, 6, 9;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_c_file_nm,
                  l_c_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv_rpt,
                  g_c_landscape,
                  l_c_spc_aft_col
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END skipld_detail_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || SKIPLD_SUM_RPT_SP
  ||  Create "SKIPLD" Order Summary Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original - created for PIR3937
  || 11/13/06 | rhalpai | Changed report to include break on order type and to
  ||                    | include orders regardless of whether they have been
  ||                    | moved by the "Skip Load" process. IM267352
  || 06/16/08 | rhalpai | Added sort by Grouping/Corp/LLR/OrdTyp/Item to cursor.
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 08/26/10 | rhalpai | Changed to use standard error hanlder. PIR8531
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE skipld_sum_rpt_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.SKIPLD_SUM_RPT_SP';
    lar_parm                logs.tar_parm;
    l_c_skip_load  CONSTANT VARCHAR2(6)   := 'SKIPLD';
    l_c_rpt_id     CONSTANT VARCHAR2(10)  := i_div || 'SKPLDS';
    l_rpt_nm                VARCHAR2(100);
    l_c_file_dir   CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm    CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps                type_stab;
    l_t_heads               type_stab;
    l_cv_rpt                g_cvt_rpt;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_grps := type_stab('SECTION  : ', 'CORP     : ', 'LLR DATE : ', 'ORDER TYP: ');
    l_t_heads :=
      type_stab('ITEM # PACK SIZE     DESCRIPTION               CASE UPC        SLOT    DEMAND  ADJUSTMENT',
                '------ ---- -------- ------------------------- --------------- ------- ------- ----------'
               );
    logs.dbg('Set Report Name');

    SELECT rn.rpt_nm || ' ORDER SUMMARY REPORT'
      INTO l_rpt_nm
      FROM div_mstr_di1d d, rpt_name_ap7r rn
     WHERE d.div_id = i_div
       AND rn.div_part = d.div_part
       AND rn.user_id = l_c_skip_load;

    logs.dbg('Open Cursor');

    OPEN l_cv_rpt
     FOR
       SELECT   rp.rpt_typ, LPAD(cx.corpb, 3, '0'), TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr,
                DECODE(a.dsorda, 'D', 'DIS', 'REG'), lpad_fn(e.catite, 6), lpad_fn(e.shppke, 4), rpad_fn(e.sizee, 8),
                rpad_fn(e.ctdsce, 25), rpad_fn(e.upccse, 15) AS case_upc,
                (SELECT lpad_fn(MAX(w.aislc || w.binc || w.levlc), 7)
                   FROM whsp300c w
                  WHERE w.div_part = rp.div_part
                    AND w.itemc = e.iteme
                    AND w.uomc = e.uome
                    AND (   e.uome LIKE 'C%'
                         OR w.taxjrc IS NULL)) AS slot,
                LPAD(SUM(b.ordqtb), 7) AS ord_qty, '__________', '', '', '', '', '', '', '', ''
           FROM div_mstr_di1d d, rpt_name_ap7r rn, rpt_parm_ap1e rp, sawp505e e, mclp020b cx, ordp100a a,
                load_depart_op1f ld, ordp120b b, mclp040d md
          WHERE d.div_id = i_div
            AND rn.div_part = d.div_part
            AND rn.user_id = l_c_skip_load
            AND rp.div_part = rn.div_part
            AND rp.rpt_nm = rn.rpt_nm
            AND rp.val_cd = e.catite
            AND cx.div_part = rp.div_part
            AND LPAD(cx.corpb, 3, '0') = rp.user_id
            AND a.div_part = cx.div_part
            AND a.custa = cx.custb
            AND a.excptn_sw = 'N'
            AND a.stata = 'O'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.excptn_sw = 'N'
            AND b.statb = 'O'
            AND b.itemnb = e.iteme
            AND b.sllumb = e.uome
            AND md.div_part = ld.div_part
            AND md.custd = a.custa
            AND md.loadd = ld.load_num
       GROUP BY rp.div_part, rp.rpt_typ, cx.corpb, ld.llr_dt, a.dsorda, e.catite, e.shppke, e.sizee, e.ctdsce, e.upccse,
                e.iteme, e.uome
       ORDER BY 1, 2, 3, 4, 5;

    logs.dbg('Generate Report');
    gen_report_sp(i_div, l_c_file_dir, l_c_file_nm, l_c_rpt_id, l_rpt_nm, l_t_grps, l_t_heads, l_cv_rpt, g_c_landscape);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END skipld_sum_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || HAZMAT_RPT_SP
  ||  Create Hazardous Materials Order Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/06/07 | rhalpai | Original - created for PIR5132
  || 03/10/08 | rhalpai | Changed cursor to include distributions.
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F, OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE hazmat_rpt_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.HAZMAT_RPT_SP';
    lar_parm                  logs.tar_parm;
    l_c_rpt_id       CONSTANT VARCHAR2(10)  := i_div || 'OPHZMT';
    l_rpt_nm                  VARCHAR2(100);
    l_c_file_dir     CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm      CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps                  type_stab;
    l_t_heads                 type_stab;
    l_cv                      g_cvt_rpt;
    l_c_spc_aft_col  CONSTANT PLS_INTEGER   := 6;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_rpt_nm := 'HAZARDOUS MATERIALS ORDER REPORT';
    l_t_grps := type_stab('LLR DATE : ');
    l_t_heads :=
      type_stab
         ('LOAD STP MCCUST ST   ORDER NUM ORDSRC   MIX SUB MCITEM PACK SIZE     DESCRIPTION                    ORDQTY',
          '---- --- ------ -- ----------- -------- --- --- ------ ---- -------- ------------------------------ ------'
         );
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       WITH split_dtl AS
            (SELECT d.div_part, b.ordnob AS ord_num, b.lineb AS ord_ln, ld.llr_dt AS llr_dt, ld.load_num, se.stop_num,
                    cx.mccusb AS mcl_cust, ct.taxjrc, b.subrcb AS sub_cd, e.catite, e.shppke, e.sizee, e.ctdsce,
                    b.orgqtb AS orig_qty
               FROM div_mstr_di1d d, split_sta_itm_op1s s, mclp030c ct, stop_eta_op1g se, load_depart_op1f ld, ordp100a a,
                    mclp020b cx, ordp120b b, sawp505e e, mclp040d md
              WHERE s.split_typ = op_split_ord_pk.g_c_split_typ_hazmat
                AND d.div_id = i_div
                AND ct.div_part = d.div_part
                AND ct.taxjrc = s.state_cd
                AND se.div_part = ct.div_part
                AND se.cust_id = ct.custc
                AND ld.div_part = se.div_part
                AND ld.load_depart_sid = se.load_depart_sid
                AND a.div_part = se.div_part
                AND a.load_depart_sid = se.load_depart_sid
                AND a.custa = se.cust_id
                AND a.stata = 'O'
                AND cx.div_part = se.div_part
                AND cx.custb = se.cust_id
                AND b.div_part = a.div_part
                AND b.ordnob = a.ordnoa
                AND b.statb = 'O'
                AND e.iteme = b.itemnb
                AND e.uome = b.sllumb
                AND s.mcl_item = e.catite
                AND md.div_part = se.div_part
                AND md.custd = se.cust_id
                AND md.loadd = ld.load_num
                AND md.stopd = se.stop_num),
            x AS
            (SELECT sd.ord_num, sd.ord_ln, sd.llr_dt, sd.load_num, sd.stop_num, sd.mcl_cust, sd.taxjrc, sd.sub_cd, sd.catite,
                    sd.shppke, sd.sizee, sd.ctdsce, sd.orig_qty
               FROM split_dtl sd
             UNION
             SELECT sd.ord_num, b.lineb AS ord_ln, sd.llr_dt, sd.load_num, sd.stop_num, sd.mcl_cust, sd.taxjrc,
                    b.subrcb AS sub_cd, e.catite, e.shppke, e.sizee, e.ctdsce, b.orgqtb AS orig_qty
               FROM split_dtl sd, ordp120b b, sawp505e e
              WHERE b.div_part = sd.div_part
                AND b.ordnob = sd.ord_num
                AND FLOOR(b.lineb) = FLOOR(sd.ord_ln)
                AND b.lineb <> sd.ord_ln
                AND e.iteme = b.itemnb
                AND e.uome = b.sllumb),
            o AS
            (SELECT   sd.ord_num,
                      (CASE
                         WHEN EXISTS(SELECT 1
                                       FROM ordp120b b
                                      WHERE b.div_part = sd.div_part
                                        AND b.ordnob = sd.ord_num
                                        AND b.statb <> 'C'
                                        AND NOT EXISTS(SELECT 1
                                                         FROM split_sta_itm_op1s s
                                                        WHERE s.split_typ = op_split_ord_pk.g_c_split_typ_hazmat
                                                          AND s.state_cd = sd.taxjrc
                                                          AND s.mcl_item IN(b.orgitb, b.orditb))) THEN 'Y'
                         ELSE 'N'
                       END
                      ) AS mix_sw
                 FROM split_dtl sd
             GROUP BY sd.div_part, sd.ord_num, sd.taxjrc)
       SELECT   TO_CHAR(x.llr_dt, 'YYYY-MM-DD') AS llr_dt, x.load_num,
                lpad_fn((CASE
                           WHEN x.stop_num < 10 THEN '0' || x.stop_num
                           ELSE TO_CHAR(x.stop_num)
                         END), 3) AS stop_num, x.mcl_cust, rpad_fn(x.taxjrc, 2) AS st_cd, LPAD(x.ord_num, 11) AS ord_num,
                (SELECT rpad_fn(a.ipdtsa, 8)
                   FROM ordp100a a
                  WHERE a.ordnoa = x.ord_num) AS ord_src, RPAD(o.mix_sw, 3) AS mix_sw,
                DECODE(o.mix_sw, 'N', '   ', 'Y', DECODE(x.sub_cd, 0, '   ', 999, 'ORG', 'SUB')) AS sub_cd,
                lpad_fn(DECODE(o.mix_sw, 'Y', x.catite), 6) AS mcl_item, lpad_fn(DECODE(o.mix_sw, 'Y', x.shppke), 4) AS pack,
                rpad_fn(DECODE(o.mix_sw, 'Y', x.sizee), 8) AS sz, rpad_fn(DECODE(o.mix_sw, 'Y', x.ctdsce), 30) AS descr,
                lpad_fn(DECODE(o.mix_sw, 'Y', SUM(x.orig_qty)), 6) AS ord_qty, '', '', '', '', '', ''
           FROM x, o
          WHERE o.ord_num = x.ord_num
       GROUP BY x.llr_dt, x.load_num, x.stop_num, x.mcl_cust, x.taxjrc, x.ord_num, o.mix_sw, x.sub_cd, x.catite, x.shppke,
                x.sizee, x.ctdsce
       ORDER BY x.llr_dt, x.load_num, x.stop_num, x.ord_num, x.sub_cd, x.catite;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_c_file_nm,
                  l_c_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv,
                  g_c_landscape,
                  l_c_spc_aft_col
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END hazmat_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCT_VNDR_RECAP_RPT_SP
  ||  Create Strict Item Vendor Recap Report.
  ||
  ||  i_recap_ts is not null : Run for current recap timestamp
  ||  i_recap_ts is null     : Run for recapped within last 24 hours
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/07/07 | rhalpai | Original - created for PIR5002
  || 07/17/08 | rhalpai | Changed to use calculated PO Cutoff and added parm
  ||                    | for ftp remote file and logic to skip report for an
  ||                    | empty cursor. PIR5002
  || 09/24/08 | rhalpai | Changed to use show summed qtys by item. PIR5002
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strct_vndr_recap_rpt_sp(
    i_div           IN  VARCHAR2,
    i_ftp_rmt_file  IN  VARCHAR2 DEFAULT NULL,
    i_recap_ts      IN  DATE DEFAULT NULL
  ) IS
    l_c_module             CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.STRCT_VNDR_RECAP_RPT_SP';
    lar_parm                        logs.tar_parm;
    l_recap_ts                      DATE;
    l_rpt_id                        VARCHAR2(10);
    l_rpt_nm                        VARCHAR2(100);
    l_c_file_dir           CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_file_nm                       VARCHAR2(30);
    l_t_grps                        type_stab;
    l_t_heads                       type_stab;
    l_cv                            g_cvt_rpt;
    l_c_spc_aft_col        CONSTANT PLS_INTEGER   := NULL;
    l_c_dbl_spc_sw         CONSTANT VARCHAR2(1)   := 'N';
    l_c_skip_empty_cur_sw  CONSTANT VARCHAR2(1)   := 'Y';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FtpRmtFile', i_ftp_rmt_file);
    logs.add_parm(lar_parm, 'RecapTS', i_recap_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');

    IF i_recap_ts IS NULL THEN
      l_recap_ts := SYSDATE - 1;
      l_rpt_id := i_div || 'STRCTRS';
      l_rpt_nm := 'STRICT ITEM VENDOR RECAP SUMMARY REPORT';
    ELSE
      l_recap_ts := i_recap_ts;
      l_rpt_id := i_div || 'STRCTR';
      l_rpt_nm := 'STRICT ITEM VENDOR RECAP REPORT';
    END IF;   -- i_recap_ts IS NULL

    l_file_nm := l_rpt_id;
    l_t_grps := type_stab('DCS/CBR VENDOR ID: ',
                          'VENDOR NAME      : ',
                          'LEAD DAYS        : ',
                          'RECAP TS         : ',
                          'PO CUTOFF TS     : ',
                          'PROD RECEIPT TS  : '
                         );
    l_t_heads := type_stab('MCITEM CBRITEM   UOM PACK SIZE     DESCRIPTION                   QTY',
                           '------ --------- --- ---- -------- ------------------------- -------'
                          );
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   v.dcs_vndr_id || ' / ' || v.cbr_vndr_id, v.vndr_nm, v.lead_days,
                TO_CHAR(x.recap_ts, 'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(x.po_cutoff, 'YYYY-MM-DD HH24:MI'),
                TO_CHAR(x.prod_rcpt_ts, 'YYYY-MM-DD HH24:MI'), lpad_fn(e.catite, 6), lpad_fn(e.iteme, 9),
                rpad_fn(e.uome, 3), lpad_fn(e.shppke, 4), rpad_fn(e.sizee, 8), rpad_fn(e.ctdsce, 25),
                lpad_fn(SUM(x.qty), 7), '', '', '', '', '', '', ''
           FROM sawp505e e, vndr_mstr_op1v v,
                (SELECT   d.div_part, so.cbr_vndr_id, so.recap_ts,
                          op_strict_order_pk.po_cutoff_ts_fn(i_div, so.cbr_vndr_id, ld.llr_ts) AS po_cutoff,
                          so.prod_rcpt_ts, b.orditb, SUM(so.recap_qty) AS qty
                     FROM div_mstr_di1d d, strct_ord_op1o so, split_ord_op2s s, ordp120b b, ordp100a a,
                          load_depart_op1f ld
                    WHERE d.div_id = i_div
                      AND so.div_part = d.div_part
                      AND so.recap_ts >= l_recap_ts
                      AND so.stat = 'RCP'
                      AND s.div_part = so.div_part
                      AND s.split_typ = op_split_ord_pk.g_c_split_typ_strict_ord
                      AND s.ord_num = so.ord_num
                      AND s.ord_ln = so.ord_ln
                      AND b.div_part = so.div_part
                      AND b.ordnob = so.ord_num
                      AND b.lineb = so.ord_ln
                      AND b.excptn_sw = 'N'
                      AND a.div_part = b.div_part
                      AND a.ordnoa = b.ordnob
                      AND ld.div_part = a.div_part
                      AND ld.load_depart_sid = a.load_depart_sid
                 GROUP BY d.div_part, so.cbr_vndr_id, so.recap_ts, ld.llr_ts, so.prod_rcpt_ts, b.orditb) x
          WHERE e.catite = x.orditb
            AND v.div_part = x.div_part
            AND v.cbr_vndr_id = x.cbr_vndr_id
       GROUP BY v.dcs_vndr_id, v.cbr_vndr_id, v.vndr_nm, v.lead_days, x.recap_ts, x.po_cutoff, x.prod_rcpt_ts, e.catite,
                e.iteme, e.uome, e.shppke, e.sizee, e.ctdsce
       ORDER BY v.dcs_vndr_id, x.recap_ts, x.po_cutoff, x.prod_rcpt_ts, e.catite;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_file_nm,
                  l_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv,
                  g_c_portrait,
                  l_c_spc_aft_col,
                  l_c_dbl_spc_sw,
                  i_ftp_rmt_file,
                  l_c_skip_empty_cur_sw
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END strct_vndr_recap_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCT_ORD_DTL_RPT_SP
  ||  Create Strict Item Order Detail Report for orders billed within last 24 hours.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/07/07 | rhalpai | Original - created for PIR5002
  || 08/01/08 | rhalpai | Added parms for ftp remote file, starting bill date,
  ||                    | and logic to skip report for an empty cursor. PIR5002
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F, OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strct_ord_dtl_rpt_sp(
    i_div           IN  VARCHAR2,
    i_ftp_rmt_file  IN  VARCHAR2 DEFAULT NULL,
    i_bill_dt       IN  DATE DEFAULT SYSDATE - 1
  ) IS
    l_c_module             CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.STRCT_ORD_DTL_RPT_SP';
    lar_parm                        logs.tar_parm;
    l_bill_dt                       VARCHAR2(8);
    l_c_rpt_id             CONSTANT VARCHAR2(10)  := i_div || 'STRCTD';
    l_rpt_nm                        VARCHAR2(100);
    l_c_file_dir           CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm            CONSTANT VARCHAR2(30)  := l_c_rpt_id;
    l_t_grps                        type_stab;
    l_t_heads                       type_stab;
    l_cv                            g_cvt_rpt;
    l_c_spc_aft_col        CONSTANT PLS_INTEGER   := NULL;
    l_c_dbl_spc_sw         CONSTANT VARCHAR2(1)   := 'Y';
    l_c_skip_empty_cur_sw  CONSTANT VARCHAR2(1)   := 'Y';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FtpRmtFile', i_ftp_rmt_file);
    logs.add_parm(lar_parm, 'BillDt', i_bill_dt);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_rpt_nm := 'STRICT ITEM ORDER DETAIL REPORT';
    l_bill_dt := TO_CHAR(i_bill_dt, 'YYYYMMDD');
    l_t_grps := type_stab('LOAD    : ', 'LLR DATE: ');
    l_t_heads :=
      type_stab
        ('STP MCCUST NAME                      PROD RECEIPT TS  MCITEM ITEM      UOM PACK SIZE     DESCRIPTION               ORDQTY RECAPQTY',
         '--- ------ ------------------------- ---------------- ------ --------- --- ---- -------- ------------------------- ------ --------',
         '     ORDNUM  ORDLN STA ORD RCVD TS         BILL TS             RECAP TS            RECAP LLR  PROD RCPT TS',
         '----------- ------ --- ------------------- ------------------- ------------------- ---------- -------------------'
        );
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   ld.load_num, TO_CHAR(ld.llr_ts, 'YYYY-MM-DD') AS llr_dt, lpad_fn(se.stop_num, 3) AS stop_num,
                cx.mccusb AS mcl_cust, rpad_fn(c.namec, 25) AS cust_nm,
                TO_CHAR(so.prod_rcpt_ts, 'YYYY-MM-DD HH24:MI') AS prod_rcpt_ts,
                e.catite || ' ' || e.iteme || ' ' || e.uome AS mcitem_cbritem_uom, lpad_fn(e.shppke, 4) AS pack,
                rpad_fn(e.sizee, 8) AS sz, rpad_fn(e.ctdsce, 25) AS item_descr,
                lpad_fn(CASE
                          WHEN b.statb IN('O', 'I', 'S', 'C') THEN b.ordqtb
                          ELSE b.pckqtb
                        END, 6) AS ord_qty, lpad_fn(so.recap_qty, 8) || cnst.newline_char AS recap_qty,
                lpad_fn(b.ordnob, 11) AS ord_num, LPAD(TO_CHAR(b.lineb, 'FM9999.0'), 6) AS ord_ln,
                (CASE
                   WHEN b.statb = 'C' THEN 'CAN'
                   WHEN b.statb = 'S' THEN 'SUS'
                   WHEN b.statb IN('O', 'I') THEN 'OPN'
                   ELSE 'BIL'
                 END
                ) AS stat,
                TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                rpad_fn(CASE
                          WHEN b.statb IN('O', 'I', 'S', 'C') THEN ' '
                          ELSE TO_CHAR(TO_DATE(b.shpidb, 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS')
                        END,
                        19
                       ) AS bill_ts,
                rpad_fn(CASE
                          WHEN so.stat = 'URC' THEN ' '
                          WHEN so.stat = 'XCP' THEN 'RECVD AS EXCEPTION'
                          WHEN so.stat = 'MPC' THEN 'MISSED PO CUTOFF'
                          ELSE TO_CHAR(so.recap_ts, 'YYYY-MM-DD HH24:MI:SS')
                        END,
                        19
                       ) AS recap_ts,
                rpad_fn(TO_CHAR(so.llr_at_recap, 'YYYY-MM-DD'), 10) AS llr_at_recap,
                rpad_fn(TO_CHAR(so.prod_rcpt_ts, 'YYYY-MM-DD HH24:MI'), 16) AS prod_rcpt_ts
           FROM div_mstr_di1d d, strct_ord_op1o so, split_ord_op2s s, ordp120b b, ordp100a a, load_depart_op1f ld,
                stop_eta_op1g se, sysp200c c, mclp020b cx, sawp505e e
          WHERE d.div_id = i_div
            AND so.div_part = d.div_part
            AND s.div_part = so.div_part
            AND s.split_typ = op_split_ord_pk.g_c_split_typ_strict_ord
            AND s.ord_num = so.ord_num
            AND s.ord_ln = so.ord_ln
            AND b.div_part = so.div_part
            AND b.ordnob = so.ord_num
            AND b.lineb = so.ord_ln
            AND b.excptn_sw = 'N'
            AND (   b.statb IN('O', 'I', 'S', 'C')
                 OR SUBSTR(b.shpidb, 1, 8) >= l_bill_dt)
            AND a.div_part = b.div_part
            AND a.ordnoa = b.ordnob
            AND a.excptn_sw = 'N'
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
       ORDER BY ld.load_num, ld.llr_ts, se.stop_num, recap_ts, so.prod_rcpt_ts, e.catite;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_c_file_nm,
                  l_c_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv,
                  g_c_landscape,
                  l_c_spc_aft_col,
                  l_c_dbl_spc_sw,
                  i_ftp_rmt_file,
                  l_c_skip_empty_cur_sw
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END strct_ord_dtl_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || OPXD01_RPT_SP
  ||  Create XDOCK LOADING MANIFEST Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/09 | rhalpai | Original - created for PIR7342
  || 12/02/09 | rhalpai | Added TestBilSW parm and logic to separate Test-Bill
  ||                    | files from production files. PIR7342
  || 12/30/09 | rhalpai | Added DocRef (for barcode) and PONum to report.
  ||                    | Changed logic to not ftp to GDG. PIR7342
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F, OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE opxd01_rpt_sp(
    i_div          IN  VARCHAR2,
    i_rpt_ts       IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  ) IS
    l_c_module             CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.OPXD01_RPT_SP';
    lar_parm                        logs.tar_parm;
    l_test                          VARCHAR2(1)   :=(CASE i_test_bil_sw
                                                       WHEN 'Y' THEN 'T'
                                                     END);
    l_c_rpt_id             CONSTANT VARCHAR2(10)  := i_div || 'OPXD01' || l_test;
    l_rpt_nm                        VARCHAR2(100);
    l_c_file_dir           CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm            CONSTANT VARCHAR2(80)
                 := i_div || '_' || l_test || 'OPXD01_XDOCK_LOADING_MANIFEST_' || TO_CHAR(i_rpt_ts, 'YYYYMMDDHH24MISS');
    l_c_ftp_rmt_file       CONSTANT VARCHAR2(80)
                         := l_test || 'OPXD01.D' || TO_CHAR(i_rpt_ts, 'RRMMDD') || '.T'
                            || TO_CHAR(i_rpt_ts, 'HH24MISS');
    l_t_grps                        type_stab;
    l_t_heads                       type_stab;
    l_cv                            g_cvt_rpt;
    l_c_spc_aft_col        CONSTANT PLS_INTEGER   := NULL;
    l_c_dbl_spc_sw         CONSTANT VARCHAR2(1)   := 'Y';
    l_c_skip_empty_cur_sw  CONSTANT VARCHAR2(1)   := 'N';
    l_c_arc_aftr_ftp_sw    CONSTANT VARCHAR2(1)   := 'Y';
    l_c_ftp_to_gdg_sw      CONSTANT VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptTS', i_rpt_ts);
    logs.add_parm(lar_parm, 'TestBilSW', i_test_bil_sw);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_rpt_nm := 'XDOCK LOADING MANIFEST';
    l_t_grps := type_stab('LOAD    : ',
                          'DOC REF : ',
                          'CUSTOMER: ',
                          'CONTACT : ',
                          'ADDRESS : ',
                          '          ',
                          'PO NUM  : '
                         );
    l_t_heads := type_stab('MANIFEST CATEGORY         UNITS  CASES TOTE  BOX PALT CHEP BAGS   WEIGHT    CUBE', '');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   x.load_num
                || '  STOP '
                || LPAD(x.stop_num, 2, '0')
                || '  ETA: '
                || TO_CHAR(x.eta, 'DY  MM/DD/YY  HH24:MI')
                || cnst.newline_char,
                i_div
                || 'Z'
                || TO_CHAR(x.eta, 'MM')
                || TO_CHAR(x.eta, 'DD')
                || x.load_num
                || NVL(cx.mccusb, '000000') AS doc_ref,
                NVL(cx.mccusb, '000000')
                || '  CBR CUST: '
                || NVL(x.cbr_cust, '00000000')
                || '  '
                || NVL(c.namec, 'NAME UNKNOWN'),
                RPAD(c.cnnamc, 40)
                || '  TELEPHONE: '
                || SUBSTR(NVL(c.cnphnc, '0000000000'), 1, 3)
                || '-'
                || SUBSTR(NVL(c.cnphnc, '0000000000'), 4, 3)
                || '-'
                || SUBSTR(NVL(c.cnphnc, '0000000000'), 7, 4),
                NVL(c.shad1c, ' '),
                RPAD(NVL(c.shpctc, ' '), 30)
                || '  '
                || RPAD(NVL(c.shpstc, ' '), 2)
                || '  '
                || SUBSTR(NVL(c.shpzpc, '00000'), 1, 5),
                (SELECT MAX(a.cpoa)
                   FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                  WHERE ld.div_part = x.div_part
                    AND ld.llr_dt = (SELECT DATE '1900-02-28' + r.llr_date
                                       FROM div_mstr_di1d d, mclane_manifest_rpts r
                                      WHERE d.div_id = i_div
                                        AND r.div_part = d.div_part
                                        AND r.create_ts = i_rpt_ts
                                        AND r.strategy_id = 0
                                        AND ROWNUM = 1)
                    AND ld.load_num = x.load_num
                    AND se.div_part = ld.div_part
                    AND se.load_depart_sid = ld.load_depart_sid
                    AND se.stop_num = x.stop_num
                    AND a.div_part = se.div_part
                    AND a.load_depart_sid = se.load_depart_sid
                    AND a.custa = se.cust_id
                    AND a.excptn_sw = 'N'
                    AND a.stata IN('P', 'R', 'A')) AS po_num,
                RPAD(x.catg_descr, 23) AS catg_descr,
                LPAD(DECODE(NVL(x.tote_units, 0), 0, ' ', TO_CHAR(x.tote_units, 'FM999,999')), 7) AS units,
                LPAD(DECODE(x.case_cnt, 0, ' ', TO_CHAR(x.case_cnt, 'FM99,999')), 6) AS case_cnt,
                LPAD((CASE
                        WHEN x.typ IN(3, 4) THEN TO_CHAR(NVL(x.tote_cnt, 0), 'FM9999')
                        ELSE DECODE(NVL(x.tote_cnt, 0), 0, ' ', TO_CHAR(x.tote_cnt, 'FM9999'))
                      END
                     ),
                     4
                    ) AS tote_cnt,
                LPAD((CASE
                        WHEN x.typ IN(3, 4) THEN TO_CHAR(NVL(x.box_cnt, 0), 'FM9999')
                        ELSE DECODE(NVL(x.box_cnt, 0), 0, ' ', TO_CHAR(x.box_cnt, 'FM9999'))
                      END
                     ),
                     4
                    ) AS box_cnt,
                LPAD((CASE
                        WHEN x.typ IN(3, 4) THEN TO_CHAR(NVL(x.bag_cnt, 0), 'FM9999')
                        ELSE DECODE(NVL(x.bag_cnt, 0), 0, ' ', TO_CHAR(x.bag_cnt, 'FM9999'))
                      END
                     ),
                     14
                    ) AS bag_cnt,
                LPAD(NVL(TO_CHAR(x.prod_wt, 'FM99,999.0'), ' '), 8) AS prod_wt,
                LPAD(NVL(TO_CHAR(x.prod_cube, 'FM9,999.0'), ' '), 7) AS prod_cube, '', '', '', '', ''
           FROM (SELECT   1 AS typ, d.div_part, r.load_num, r.stop_num, r.manifest_cat, m.seqc,
                          DECODE(tc.tote_catg_cnt,
                                 1, tc.tote_descr,
                                 NVL(m.descc, 'INVALID MANIFEST CATEGORY')
                                ) AS catg_descr,
                          tc.tote_cat,
                          TO_DATE('19000228' || LPAD(NVL(r.eta_time, 0), 4, '0'), 'YYYYMMDDHH24MI')
                          + NVL(r.eta_date, 0) AS eta,
                          NVL(r.cust_num, '00000000') AS cbr_cust,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          tc.tote_units, tc.tote_cnt, tc.box_cnt, tc.bag_cnt, NVL(SUM(r.product_weight), 0) AS prod_wt,
                          NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS prod_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp210c m, mclp200b t,
                          (SELECT   r.load_num, r.stop_num, r.manifest_cat, r.tote_cat,
                                    NVL(t.descb, 'INVALID TOTE CATEGORY') AS tote_descr,
                                    NVL(SUM(r.qty_alloc), 0) AS tote_units, NVL(SUM(r.tote_count), 0) AS tote_cnt,
                                    NVL(SUM(r.box_count), 0) AS box_cnt, NVL(SUM(r.bag_count), 0) AS bag_cnt,
                                    COUNT(DISTINCT r.tote_cat) OVER(PARTITION BY r.load_num, r.stop_num, r.manifest_cat)
                                                                                                       AS tote_catg_cnt
                               FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp200b t
                              WHERE d.div_id = i_div
                                AND r.div_part = d.div_part
                                AND r.create_ts = i_rpt_ts
                                AND r.strategy_id = 0
                                AND t.div_part(+) = r.div_part
                                AND t.totctb(+) = r.tote_cat
                                AND (   r.tote_count > 0
                                     OR r.box_count > 0
                                     OR r.bag_count > 0)
                           GROUP BY r.load_num, r.stop_num, r.manifest_cat, r.tote_cat, t.descb) tc
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND m.div_part(+) = r.div_part
                      AND m.manctc(+) = r.manifest_cat
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                      AND tc.load_num(+) = r.load_num
                      AND tc.stop_num(+) = r.stop_num
                      AND tc.manifest_cat(+) = r.manifest_cat
                      AND tc.tote_cat(+) = r.tote_cat
                      AND tc.tote_catg_cnt(+) = 1
                 GROUP BY d.div_part, r.load_num, r.stop_num, r.cust_num, r.manifest_cat, m.descc, m.seqc, r.eta_date,
                          r.eta_time, tc.tote_catg_cnt, tc.tote_descr, tc.tote_cat, tc.tote_units, tc.tote_cnt,
                          tc.box_cnt, tc.bag_cnt
                 UNION ALL
                 SELECT   2 AS typ, d.div_part, r.load_num, r.stop_num, r.manifest_cat, m.seqc,
                          '  ' || NVL(t.descb, 'INVALID TOTE CATEGORY') AS catg_descr, t.totctb AS tote_cat,
                          TO_DATE('19000228' || LPAD(NVL(r.eta_time, 0), 4, '0'), 'YYYYMMDDHH24MI')
                          + NVL(r.eta_date, 0) AS eta,
                          NVL(r.cust_num, '00000000') AS cbr_cust,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          SUM(r.qty_alloc) AS tote_units, SUM(r.tote_count) AS tote_cnt, SUM(r.box_count) AS box_cnt,
                          SUM(r.bag_count) AS bag_cnt, NULL AS prod_wt, NULL AS prod_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp210c m, mclp200b t
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND (   r.tote_count > 0
                           OR r.box_count > 0
                           OR r.bag_count > 0)
                      AND EXISTS(SELECT 1
                                   FROM mclane_manifest_rpts r3
                                  WHERE r3.div_part = r.div_part
                                    AND r3.strategy_id = r.strategy_id
                                    AND r3.create_ts = r.create_ts
                                    AND r3.load_num = r.load_num
                                    AND r3.stop_num = r.stop_num
                                    AND r3.manifest_cat = r.manifest_cat
                                 HAVING COUNT(DISTINCT r3.tote_cat) > 1)
                      AND m.div_part(+) = r.div_part
                      AND m.manctc(+) = r.manifest_cat
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                 GROUP BY d.div_part, r.load_num, r.stop_num, r.cust_num, r.manifest_cat, m.descc, m.seqc, r.eta_date,
                          r.eta_time, t.totctb, t.descb
                 UNION ALL
                 SELECT   3 AS typ, d.div_part, r.load_num, r.stop_num, NULL AS manifest_cat, 9999 AS seq,
                          'STOP TOTALS:             ' AS catg_descr, NULL AS tote_cat,
                          TO_DATE('19000228' || LPAD(NVL(r.eta_time, 0), 4, '0'), 'YYYYMMDDHH24MI')
                          + NVL(r.eta_date, 0) AS eta,
                          NVL(r.cust_num, '00000000') AS cbr_cust,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, 0,
                                         r.qty_alloc
                                        )
                                 ),
                              0
                             ) AS tote_units,
                          SUM(r.tote_count) AS tote_cnt, SUM(r.box_count) AS box_cnt, SUM(r.bag_count) AS bag_cnt,
                          NVL(SUM(r.product_weight), 0) AS prod_wt,
                          NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS prod_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp200b t
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                 GROUP BY d.div_part, r.load_num, r.stop_num, r.cust_num, r.eta_date, r.eta_time
                 UNION ALL
                 SELECT   4 AS typ, d.div_part, r.load_num, ls.stop_num, NULL AS manifest_cat, NULL AS seq,
                          'LOAD TOTALS:             ' AS catg_descr, NULL AS tote_cat,
                          TO_DATE('19000228' || LPAD(NVL(ls.eta_time, 0), 4, '0'), 'YYYYMMDDHH24MI')
                          + NVL(ls.eta_date, 0) AS eta,
                          NVL(ls.cust_num, '00000000') AS cbr_cust,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, 0,
                                         r.qty_alloc
                                        )
                                 ),
                              0
                             ) AS tote_units,
                          SUM(r.tote_count) AS tote_cnt, SUM(r.box_count) AS box_cnt, SUM(r.bag_count) AS bag_cnt,
                          NVL(SUM(r.product_weight), 0) AS prod_wt,
                          NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS prod_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp200b t,
                          (SELECT   r2.load_num, r2.stop_num, r2.cust_num, r2.eta_date, r2.eta_time
                               FROM div_mstr_di1d d2, mclane_manifest_rpts r2
                              WHERE d2.div_id = i_div
                                AND r2.div_part = d2.div_part
                                AND r2.create_ts = i_rpt_ts
                                AND r2.strategy_id = 0
                                AND (r2.load_num, r2.stop_num) IN(
                                      SELECT   r3.load_num, MAX(r3.stop_num)
                                          FROM div_mstr_di1d d3, mclane_manifest_rpts r3
                                         WHERE d3.div_id = i_div
                                           AND r3.div_part = d3.div_part
                                           AND r3.create_ts = i_rpt_ts
                                           AND r3.strategy_id = 0
                                      GROUP BY r3.load_num)
                           GROUP BY r2.load_num, r2.stop_num, r2.cust_num, r2.eta_date, r2.eta_time) ls
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND ls.load_num = r.load_num
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                 GROUP BY d.div_part, r.load_num, ls.stop_num, ls.cust_num, ls.eta_date, ls.eta_time) x,
                sysp200c c, mclp020b cx
          WHERE c.div_part(+) = x.div_part
            AND c.acnoc(+) = x.cbr_cust
            AND cx.div_part(+) = c.div_part
            AND cx.custb(+) = c.acnoc
       ORDER BY x.load_num, x.stop_num, x.seqc, x.manifest_cat, x.typ, x.catg_descr, x.tote_cat;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_c_file_nm,
                  l_c_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv,
                  g_c_portrait,
                  l_c_spc_aft_col,
                  l_c_dbl_spc_sw,
                  l_c_ftp_rmt_file,
                  l_c_skip_empty_cur_sw,
                  l_c_arc_aftr_ftp_sw,
                  l_c_ftp_to_gdg_sw
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END opxd01_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || OPXD02_RPT_SP
  ||  Create XDOCK LOAD DEPARTMENT SUMMARY Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/09 | rhalpai | Original - created for PIR7342
  || 12/02/09 | rhalpai | Added TestBilSW parm and logic to separate Test-Bill
  ||                    | files from production files. PIR7342
  || 12/30/09 | rhalpai | Changed logic to not ftp to GDG. PIR7342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE opxd02_rpt_sp(
    i_div          IN  VARCHAR2,
    i_rpt_ts       IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  ) IS
    l_c_module             CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.OPXD02_RPT_SP';
    lar_parm                        logs.tar_parm;
    l_test                          VARCHAR2(1)   :=(CASE i_test_bil_sw
                                                       WHEN 'Y' THEN 'T'
                                                     END);
    l_c_rpt_id             CONSTANT VARCHAR2(10)  := i_div || 'OPXD02' || l_test;
    l_rpt_nm                        VARCHAR2(100);
    l_c_file_dir           CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm            CONSTANT typ.t_maxfqnm
                     := i_div || '_' || l_test || 'OPXD02_XDOCK_LOAD_SUMMARY_' || TO_CHAR(i_rpt_ts, 'YYYYMMDDHH24MISS');
    l_c_ftp_rmt_file       CONSTANT VARCHAR2(80)
                         := l_test || 'OPXD02.D' || TO_CHAR(i_rpt_ts, 'RRMMDD') || '.T'
                            || TO_CHAR(i_rpt_ts, 'HH24MISS');
    l_t_grps                        type_stab;
    l_t_heads                       type_stab;
    l_cv                            g_cvt_rpt;
    l_c_spc_aft_col        CONSTANT PLS_INTEGER   := NULL;
    l_c_dbl_spc_sw         CONSTANT VARCHAR2(1)   := 'Y';
    l_c_skip_empty_cur_sw  CONSTANT VARCHAR2(1)   := 'N';
    l_c_arc_aftr_ftp_sw    CONSTANT VARCHAR2(1)   := 'Y';
    l_c_ftp_to_gdg_sw      CONSTANT VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptTS', i_rpt_ts);
    logs.add_parm(lar_parm, 'TestBilSW', i_test_bil_sw);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_rpt_nm := 'XDOCK LOAD DEPARTMENT SUMMARY';
    l_t_grps := type_stab('LOAD: ');
    l_t_heads := type_stab('                                         BOXES/               PRODUCT    CUBE OF',
                           'MANIFEST CATEGORY         UNITS   CASES   TOTES     WEIGHT      CUBE      TOTES',
                           ''
                          );
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   x.load_num, RPAD(x.catg_descr, 23) AS catg_descr,
                LPAD(DECODE(x.tote_units, 0, ' ', TO_CHAR(x.tote_units, 'FM999,999')), 7) AS units,
                LPAD(DECODE(x.case_cnt, 0, ' ', TO_CHAR(x.case_cnt, 'FM999,999')), 7) AS case_cnt,
                LPAD(DECODE(x.tote_cnt, 0, ' ', TO_CHAR(x.tote_cnt, 'FM999,999')), 7) AS tote_cnt,
                LPAD(NVL(TO_CHAR(x.prod_wt, 'FM9999,999.0'), ' '), 10) AS prod_wt,
                LPAD(NVL(TO_CHAR(x.prod_cube, 'FM9999,999.0'), ' '), 10) AS prod_cube,
                LPAD(NVL(TO_CHAR(x.tote_cube, 'FM9999,999.0'), ' '), 10) AS tote_cube, '', '', '', '', '', '', '', '',
                '', '', '', ''
           FROM (SELECT   1 AS typ, r.load_num, r.manifest_cat, m.seqc AS seq,
                          NVL(m.descc, 'INVALID MANIFEST CATEGORY') AS catg_descr,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, 0,
                                         r.qty_alloc
                                        )
                                 ),
                              0
                             ) AS tote_units,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          NVL(SUM(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0)), 0) AS tote_cnt,
                          NVL(SUM(r.product_weight), 0) AS prod_wt, NVL(SUM(r.product_cube), 0) AS prod_cube,
                          NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS tote_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp210c m, mclp200b t
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND m.div_part(+) = r.div_part
                      AND m.manctc(+) = r.manifest_cat
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                 GROUP BY r.load_num, m.seqc, m.descc, r.manifest_cat
                 UNION ALL
                 SELECT   2 AS typ, r.load_num, NULL AS manifest_cat, 9999 AS seq,
                          'STOPS:'
                          || (SELECT LPAD(COUNT(DISTINCT r2.stop_num), 4)
                                FROM mclane_manifest_rpts r2
                               WHERE r2.div_part = d.div_part
                                 AND r2.create_ts = i_rpt_ts
                                 AND r2.load_num = r.load_num)
                          || '  TOTALS:' AS catg_descr,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, 0,
                                         r.qty_alloc
                                        )
                                 ),
                              0
                             ) AS tote_units,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          NVL(SUM(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0)), 0) AS tote_cnt,
                          NVL(SUM(r.product_weight), 0) AS prod_wt, NVL(SUM(r.product_cube), 0) AS prod_cube,
                          NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS tote_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp200b t
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                 GROUP BY d.div_part, r.load_num) x
       ORDER BY x.load_num, x.typ, x.seq, x.manifest_cat;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_c_file_nm,
                  l_c_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv,
                  g_c_portrait,
                  l_c_spc_aft_col,
                  l_c_dbl_spc_sw,
                  l_c_ftp_rmt_file,
                  l_c_skip_empty_cur_sw,
                  l_c_arc_aftr_ftp_sw,
                  l_c_ftp_to_gdg_sw
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END opxd02_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || OPXD03_RPT_SP
  ||  Create XDOCK SUMMARY LOAD DEPARTMENT SUMMARY Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/09 | rhalpai | Original - created for PIR7342
  || 12/02/09 | rhalpai | Added TestBilSW parm and logic to separate Test-Bill
  ||                    | files from production files. PIR7342
  || 12/30/09 | rhalpai | Changed logic to not ftp to GDG. PIR7342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE opxd03_rpt_sp(
    i_div          IN  VARCHAR2,
    i_rpt_ts       IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  ) IS
    l_c_module             CONSTANT typ.t_maxfqnm := 'OP_MISC_REPORTS_PK.OPXD03_RPT_SP';
    lar_parm                        logs.tar_parm;
    l_test                          VARCHAR2(1)   :=(CASE i_test_bil_sw
                                                       WHEN 'Y' THEN 'T'
                                                     END);
    l_c_rpt_id             CONSTANT VARCHAR2(10)  := i_div || 'OPXD03' || l_test;
    l_rpt_nm                        VARCHAR2(100);
    l_c_file_dir           CONSTANT VARCHAR2(9)   := '/ftptrans';
    l_c_file_nm            CONSTANT typ.t_maxfqnm
                       := i_div || '_' || l_test || 'OPXD03_XDOCK_LOAD_RECAP_' || TO_CHAR(i_rpt_ts, 'YYYYMMDDHH24MISS');
    l_c_ftp_rmt_file       CONSTANT VARCHAR2(80)
                         := l_test || 'OPXD03.D' || TO_CHAR(i_rpt_ts, 'RRMMDD') || '.T'
                            || TO_CHAR(i_rpt_ts, 'HH24MISS');
    l_t_grps                        type_stab;
    l_t_heads                       type_stab;
    l_cv                            g_cvt_rpt;
    l_c_spc_aft_col        CONSTANT PLS_INTEGER   := NULL;
    l_c_dbl_spc_sw         CONSTANT VARCHAR2(1)   := 'Y';
    l_c_skip_empty_cur_sw  CONSTANT VARCHAR2(1)   := 'N';
    l_c_arc_aftr_ftp_sw    CONSTANT VARCHAR2(1)   := 'Y';
    l_c_ftp_to_gdg_sw      CONSTANT VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptTS', i_rpt_ts);
    logs.add_parm(lar_parm, 'TestBilSW', i_test_bil_sw);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_rpt_nm := 'XDOCK SUMMARY LOAD DEPARTMENT SUMMARY';
    l_t_grps := type_stab('LOADS: ');
    l_t_heads := type_stab('                                         BOXES/               PRODUCT    CUBE OF',
                           'MANIFEST CATEGORY         UNITS   CASES   TOTES     WEIGHT      CUBE      TOTES',
                           ''
                          );
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   (SELECT LPAD(COUNT(DISTINCT r2.load_num), 4)
                        || '   STOPS: '
                        || LPAD(COUNT(DISTINCT r2.stop_num), 5)
                   FROM mclane_manifest_rpts r2
                  WHERE r2.div_part = x.div_part
                    AND r2.create_ts = i_rpt_ts
                    AND r2.strategy_id = 0) AS loads_stops,
                RPAD(x.catg_descr, 23) AS catg_descr,
                LPAD(DECODE(x.tote_units, 0, ' ', TO_CHAR(x.tote_units, 'FM999,999')), 7) AS units,
                LPAD(DECODE(x.case_cnt, 0, ' ', TO_CHAR(x.case_cnt, 'FM999,999')), 7) AS case_cnt,
                LPAD(DECODE(x.tote_cnt, 0, ' ', TO_CHAR(x.tote_cnt, 'FM999,999')), 7) AS tote_cnt,
                LPAD(NVL(TO_CHAR(x.prod_wt, 'FM9999,999.0'), ' '), 10) AS prod_wt,
                LPAD(NVL(TO_CHAR(x.prod_cube, 'FM9999,999.0'), ' '), 10) AS prod_cube,
                LPAD(NVL(TO_CHAR(x.tote_cube, 'FM9999,999.0'), ' '), 10) AS tote_cube, '', '', '', '', '', '', '', '',
                '', '', '', ''
           FROM (SELECT   1 AS typ, d.div_part, r.manifest_cat, m.seqc AS seq,
                          NVL(m.descc, 'INVALID MANIFEST CATEGORY') AS catg_descr,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, 0,
                                         r.qty_alloc
                                        )
                                 ),
                              0
                             ) AS tote_units,
                          NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                         0, r.qty_alloc,
                                         0
                                        )
                                 ),
                              0
                             ) AS case_cnt,
                          NVL(SUM(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0)), 0) AS tote_cnt,
                          NVL(SUM(r.product_weight), 0) AS prod_wt, NVL(SUM(r.product_cube), 0) AS prod_cube,
                          NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS tote_cube
                     FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp210c m, mclp200b t
                    WHERE d.div_id = i_div
                      AND r.div_part = d.div_part
                      AND r.create_ts = i_rpt_ts
                      AND r.strategy_id = 0
                      AND m.div_part(+) = r.div_part
                      AND m.manctc(+) = r.manifest_cat
                      AND t.div_part(+) = r.div_part
                      AND t.totctb(+) = r.tote_cat
                 GROUP BY m.seqc, m.descc, r.manifest_cat
                 UNION ALL
                 SELECT 2 AS typ, d.div_part, NULL AS manifest_cat, 9999 AS seq, 'GRAND TOTALS:' AS catg_descr,
                        NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                       0, 0,
                                       r.qty_alloc
                                      )
                               ),
                            0
                           ) AS tote_units,
                        NVL(SUM(DECODE(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0),
                                       0, r.qty_alloc,
                                       0
                                      )
                               ),
                            0
                           ) AS case_cnt,
                        NVL(SUM(NVL(r.tote_count, 0) + NVL(r.box_count, 0) + NVL(r.bag_count, 0)), 0) AS tote_cnt,
                        NVL(SUM(r.product_weight), 0) AS prod_wt, NVL(SUM(r.product_cube), 0) AS prod_cube,
                        NVL(SUM(DECODE(r.tote_count, 0, r.product_cube, r.tote_count * t.outerb)), 0) AS tote_cube
                   FROM div_mstr_di1d d, mclane_manifest_rpts r, mclp200b t
                  WHERE d.div_id = i_div
                    AND r.div_part = d.div_part
                    AND r.create_ts = i_rpt_ts
                    AND r.strategy_id = 0
                    AND t.div_part(+) = r.div_part
                    AND t.totctb(+) = r.tote_cat) x
       ORDER BY x.typ, x.seq, x.manifest_cat;

    logs.dbg('Generate Report');
    gen_report_sp(i_div,
                  l_c_file_dir,
                  l_c_file_nm,
                  l_c_rpt_id,
                  l_rpt_nm,
                  l_t_grps,
                  l_t_heads,
                  l_cv,
                  g_c_portrait,
                  l_c_spc_aft_col,
                  l_c_dbl_spc_sw,
                  l_c_ftp_rmt_file,
                  l_c_skip_empty_cur_sw,
                  l_c_arc_aftr_ftp_sw,
                  l_c_ftp_to_gdg_sw
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END opxd03_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || WAWA_PO_RPT_SP
  ||  Create and email Wawa PO Report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/06/24 | rhalpai | Original - created for PC-9938
  ||----------------------------------------------------------------------------
  */
  PROCEDURE wawa_po_rpt_sp(
    i_email_addr  IN  VARCHAR2 DEFAULT 'Wawa.divisions@mclaneco.com'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_MISC_REPORTS_PK.WAWA_PO_RPT_SP';
    lar_parm             logs.tar_parm;
    l_file_nm            VARCHAR2(50);
    l_t_rpt_lns          typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := 'WAWA_PO_EXTR';
    logs.dbg('Get Report Data');

    SELECT z.rpt_ln
    BULK COLLECT INTO l_t_rpt_lns
      FROM (SELECT 'WAWA PO SUMMARY REPORT' AS rpt_ln
              FROM DUAL
            UNION ALL
            SELECT 'Div ShipDate   LLRDate    POPrfx POCnt CustCnt'
              FROM DUAL
            UNION ALL
            SELECT '--- ---------- ---------- ------ ----- -------'
              FROM DUAL
            UNION ALL
            SELECT x.rpt_ln
              FROM (SELECT   RPAD(d.div_id, 3)
                             || ' '
                             || TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')
                             || ' '
                             || TO_CHAR(ld.llr_dt, 'YYYY-MM-DD')
                             || ' '
                             || LPAD(SUBSTR(a.cpoa, 1, 1), 6)
                             || ' '
                             || LPAD(COUNT(DISTINCT(a.cpoa)), 5)
                             || ' '
                             || LPAD(COUNT(DISTINCT(a.custa)), 7) AS rpt_ln
                        FROM mclp020b cx, div_mstr_di1d d, ordp100a a, load_depart_op1f ld
                       WHERE cx.corpb = 465
                         AND d.div_part = cx.div_part
                         AND a.div_part = cx.div_part
                         AND a.custa = cx.custb
                         AND a.dsorda = 'R'
                         AND a.stata = 'O'
                         AND a.ipdtsa = 'EDI'
                         AND ld.div_part = a.div_part
                         AND ld.load_depart_sid = a.load_depart_sid
                    GROUP BY d.div_id, a.shpja, ld.llr_dt, SUBSTR(a.cpoa, 1, 1)
                    ORDER BY 1) x
            UNION ALL
            SELECT LPAD('*', 80, '*')
              FROM DUAL
            UNION ALL
            SELECT 'WAWA PO DETAIL REPORT'
              FROM DUAL
            UNION ALL
            SELECT 'Div Load CBRCust  LLRDate    POPrfx POCnt'
              FROM DUAL
            UNION ALL
            SELECT '--- ---- -------- ---------- ------ -----'
              FROM DUAL
            UNION ALL
            SELECT y.rpt_ln
              FROM (SELECT   RPAD(x.div_id, 3)
                             || ' '
                             || x.load_num
                             || ' '
                             || x.custa
                             || ' '
                             || TO_CHAR(x.llr_dt, 'YYYY-MM-DD')
                             || ' '
                             || LPAD(x.po_prfx, 6)
                             || ' '
                             || LPAD(x.po_cnt, 5) AS rpt_ln
                        FROM (SELECT   d.div_id, ld.load_num, a.custa, ld.llr_dt, SUBSTR(a.cpoa, 1, 1) AS po_prfx,
                                       COUNT(DISTINCT SUBSTR(a.cpoa, 1, 1)) OVER(PARTITION BY d.div_id, ld.llr_dt, ld.load_num, a.custa) AS po_prfx_cnt,
                                       COUNT(DISTINCT(a.cpoa)) AS po_cnt
                                  FROM mclp020b cx, div_mstr_di1d d, ordp100a a, load_depart_op1f ld
                                 WHERE cx.corpb = 465
                                   AND d.div_part = cx.div_part
                                   AND a.div_part = cx.div_part
                                   AND a.custa = cx.custb
                                   AND a.dsorda = 'R'
                                   AND a.stata = 'O'
                                   AND a.ipdtsa = 'EDI'
                                   AND ld.div_part = a.div_part
                                   AND ld.load_depart_sid = a.load_depart_sid
                              GROUP BY d.div_id, ld.llr_dt, ld.load_num, a.custa, SUBSTR(a.cpoa, 1, 1)) x
                       WHERE (CASE
                                WHEN x.div_id = 'WJ'
                                AND x.po_prfx_cnt < 3 THEN 1
                                WHEN x.div_id <> 'WJ'
                                AND x.po_prfx_cnt < 2 THEN 1
                              END
                             ) = 1
                    ORDER BY 1) y) z;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_file_nm);
    logs.dbg('Send Mail');
    sql_utilities_pkg.send_mail(i_email_addr, 'WAWA PO REPORT', '/ftptrans/' || l_file_nm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END wawa_po_rpt_sp;
END op_misc_reports_pk;
/

