CREATE OR REPLACE PACKAGE op_pick_confirm_pk IS
  /*
  ||----------------------------------------------------------------------------
  ||  OP_PICK_CONFIRM_PK
  ||  All the procedures in this package are called by pick confirm screens
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes     (Package Level Changes)
  ||----------------------------------------------------------------------------
  || 10/06/05 | cxamart | Original
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
  FUNCTION out_reason_code_fn(
    i_rsn_typ  IN  NUMBER DEFAULT 12
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_data_display_fn(
    i_div        IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION cntnr_id_list_fn(
    i_div        IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION cntnr_item_info_fn(
    i_div        IN  VARCHAR2,
    i_cntnr_id   IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION pick_compl_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE upd_picks_sp(
    i_div           IN      VARCHAR2,
    i_parm_list     IN      VARCHAR2,
    i_not_shp_rsn   IN      VARCHAR2,
    i_bill_err_rsn  IN      VARCHAR2,
    i_user_id       IN      VARCHAR2,
    o_msg           OUT     VARCHAR2
  );

  PROCEDURE cntnr_adjust_sp(
    i_div            IN      VARCHAR2,
    i_load_num       IN      VARCHAR2,
    i_stop_num       IN      NUMBER,
    i_cust_id        IN      VARCHAR2,
    i_catlg_num      IN      VARCHAR2,
    i_from_cntnr_id  IN      VARCHAR2,
    i_to_cntnr_id    IN      VARCHAR2,
    i_move_qty       IN      NUMBER,
    i_out_qty        IN      NUMBER,
    i_not_shp_rsn    IN      VARCHAR2,
    i_bill_err_rsn   IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  );

  PROCEDURE pick_compl_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2,
    i_alloc_sw   IN  VARCHAR2 DEFAULT 'N'
  );

  FUNCTION rlse_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  DATE
  )
    RETURN SYS_REFCURSOR;

  FUNCTION not_shp_rsn_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_llr_dt   IN  DATE,
    i_rlse_ts  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  DATE,
    i_rlse_ts   IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION mfst_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION tote_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION item_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION mass_pick_adj_sum_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_mfst_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_tote_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_item_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  PROCEDURE mass_pick_adj_sp(
    i_div           IN  VARCHAR2,
    i_llr_dt        IN  DATE,
    i_rlse_ts       IN  VARCHAR2,
    i_user_id       IN  VARCHAR2,
    i_not_shp_rsn   IN  VARCHAR2,
    i_bill_err_rsn  IN  VARCHAR2 DEFAULT NULL,
    i_load_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_mfst_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_tote_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_item_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_evnt_que_id   IN  NUMBER DEFAULT NULL,
    i_cycl_id       IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id   IN  NUMBER DEFAULT NULL
  );

END op_pick_confirm_pk;
/

CREATE OR REPLACE PACKAGE BODY op_pick_confirm_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || PRINT_SP
  ||  Print cursor to file.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/09 | rhalpai | Original PIR7342
  || 12/02/09 | rhalpai | Added parm to return count of rows processed. PIR7342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE print_sp(
    i_cv        IN      SYS_REFCURSOR,
    i_file_nm   IN      VARCHAR2,
    i_file_dir  IN      VARCHAR2 DEFAULT '/ftptrans',
    o_cnt       OUT     PLS_INTEGER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_PICK_CONFIRM_PK.PRINT_SP';
    lar_parm             logs.tar_parm;
    l_t_rpt_lns          typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.add_parm(lar_parm, 'FileDir', i_file_dir);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Add Report Lines');

    FETCH i_cv
    BULK COLLECT INTO l_t_rpt_lns;

    o_cnt := l_t_rpt_lns.COUNT;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, i_file_nm, i_file_dir, 'W');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END print_sp;

  /*
  ||----------------------------------------------------------------------------
  || EXTR_OPXDEAR_SP
  ||  Extract OP XDock EAR (Doc Imaging Keword File) and ftp to mainframe.
  ||  Uses XXEARREC format
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/30/09 | rhalpai | Original PIR7342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE extr_opxdear_sp(
    i_div          IN  VARCHAR2,
    i_ts           IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.EXTR_OPXDEAR_SP';
    lar_parm                    logs.tar_parm;
    l_div_part                  NUMBER;
    l_cv                        SYS_REFCURSOR;
    l_test_cd                   VARCHAR2(1)   :=(CASE i_test_bil_sw
                                                   WHEN 'Y' THEN 'T'
                                                 END);
    l_c_ts             CONSTANT VARCHAR2(14)  := TO_CHAR(i_ts, 'YYYYMMDDHH24MISS');
    l_c_file_dir       CONSTANT VARCHAR2(30)  := '/ftptrans';
    l_c_local_file_nm  CONSTANT VARCHAR2(30)  := i_div || '_' || l_test_cd || 'OPXDEAR_' || l_c_ts;
    l_c_rmt_file       CONSTANT VARCHAR2(80)
                             := l_test_cd || 'OPXDEAR.D' || TO_CHAR(i_ts, 'RRMMDD') || '.T'
                                || TO_CHAR(i_ts, 'HH24MISS');
    l_c_archive        CONSTANT VARCHAR2(1)   := 'Y';
    l_c_no_gdg         CONSTANT VARCHAR2(1)   := 'N';
    l_cnt                       PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'TS', i_ts);
    logs.add_parm(lar_parm, 'TestBilSW', i_test_bil_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Open OPXDEAR Cursor');

    OPEN l_cv
     FOR
       SELECT   i_div
                || 'Z'
                || TO_CHAR(DATE '1900-02-28' + r.eta_date, 'MMDD')
                || r.load_num
                || NVL(cx.mccusb, '000000')   -- doc_ref
                || i_div   -- div
                || 'Z'   -- typ
                || TO_CHAR(DATE '1900-02-28' + r.eta_date, 'YYYY-MM-DD')   -- eta_dt
                || r.load_num
                || NVL(cx.mccusb, '000000')   -- load_cust for invc
                || NVL(cx.mccusb, '000000')   -- mcl_cust
                || SUBSTR(c.retgpc, -3)   -- grp
                || lpad_fn(cx.corpb, 3, '0')   -- crp
                || RPAD(' ', 28)
           FROM mclane_manifest_rpts r, sysp200c c, mclp020b cx
          WHERE r.div_part = l_div_part
            AND r.create_ts = i_ts
            AND r.strategy_id = 0
            AND c.div_part = r.div_part
            AND c.acnoc = r.cust_num
            AND cx.div_part = r.div_part
            AND cx.custb = r.cust_num
       GROUP BY r.load_num, cx.mccusb, cx.corpb, c.retgpc, r.eta_date;

    print_sp(l_cv, l_c_local_file_nm, l_c_file_dir, l_cnt);
    logs.dbg('FTP OPXDEAR to mainframe');
    op_ftp_sp(i_div, l_c_local_file_nm, l_c_rmt_file, l_c_archive, l_c_no_gdg);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extr_opxdear_sp;

  /*
  ||----------------------------------------------------------------------------
  || EXTR_LINE_OUTS_SP
  ||  Extract LineOuts, Container Adjustments, Load Counts and ftp to mainframe.
  ||  Uses QOPRC17, QOPRC28 and QOPRC21 formats
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/09 | rhalpai | Original PIR7342
  || 12/02/09 | rhalpai | Added TestBilSW parm and logic to separate Test-Bill
  ||                    | files from production files. Added OPXD21 Load Counts
  ||                    | modeled after QOPRC21. PIR7342
  || 12/30/09 | rhalpai | Changed logic to not ftp to GDG. PIR7342
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE extr_line_outs_sp(
    i_div          IN  VARCHAR2,
    i_t_load_llrs  IN  type_stab,
    i_ts           IN  DATE,
    i_test_bil_sw  IN  VARCHAR2
  ) IS
    l_c_module                CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.EXTR_LINE_OUTS_SP';
    lar_parm                           logs.tar_parm;
    l_div_part                         NUMBER;
    l_cv                               SYS_REFCURSOR;
    l_test_cd                          VARCHAR2(1)   :=(CASE i_test_bil_sw
                                                          WHEN 'Y' THEN 'T'
                                                        END);
    l_c_ts                    CONSTANT VARCHAR2(14)  := TO_CHAR(i_ts, 'YYYYMMDDHH24MISS');
    l_c_rmt_ts                CONSTANT VARCHAR2(15)
                                                  := 'D' || TO_CHAR(i_ts, 'RRMMDD') || '.T'
                                                     || TO_CHAR(i_ts, 'HH24MISS');
    l_c_file_dir              CONSTANT VARCHAR2(30)  := '/ftptrans';
    l_c_opxd17_local_file_nm  CONSTANT VARCHAR2(30)  := i_div || '_' || l_test_cd || 'OPXD17_' || l_c_ts;
    l_c_opxd28_local_file_nm  CONSTANT VARCHAR2(30)  := i_div || '_' || l_test_cd || 'OPXD28_' || l_c_ts;
    l_c_opxd21_local_file_nm  CONSTANT VARCHAR2(30)  := i_div || '_' || l_test_cd || 'OPXD21_' || l_c_ts;
    l_c_opxd17_rmt_file       CONSTANT VARCHAR2(80)  := l_test_cd || 'OPXD17.' || l_c_rmt_ts;
    l_c_opxd28_rmt_file       CONSTANT VARCHAR2(80)  := l_test_cd || 'OPXD28.' || l_c_rmt_ts;
    l_c_opxd21_rmt_file       CONSTANT VARCHAR2(80)  := l_test_cd || 'OPXD21.' || l_c_rmt_ts;
    l_c_archive               CONSTANT VARCHAR2(1)   := 'Y';
    l_c_no_gdg                CONSTANT VARCHAR2(1)   := 'N';
    l_opxc17_cnt                       PLS_INTEGER;
    l_opxc28_cnt                       PLS_INTEGER;
    l_opxc21_cnt                       PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LoadLLRsTab', i_t_load_llrs);
    logs.add_parm(lar_parm, 'TS', i_ts);
    logs.add_parm(lar_parm, 'TestBilSW', i_test_bil_sw);
    logs.info('ENTRY', lar_parm);

    IF (    i_t_load_llrs IS NOT NULL
        AND i_t_load_llrs.COUNT > 0) THEN
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Open OPXD17 Cursor');

      OPEN l_cv
       FOR
         SELECT rpad_fn(i_div, 2)
                || RPAD('OPXD17', 38)
                || RPAD('ADD', 13)
                || rpad_fn(x.load_num, 4)
                || lpad_fn(x.stop_num, 2, '0')
                || TO_CHAR(x.eta_ts, 'YYYYMMDD')
                || lpad_fn(cx.mccusb, 6, '0')
                || lpad_fn(a.custa, 8, '0')
                || lpad_fn(e.catite, 6, '0')
                || lpad_fn(b.itemnb, 9, '0')
                || rpad_fn(b.sllumb, 3)
                || lpad_fn(b.ordnob, 11, '0')
                || TO_CHAR(b.lineb, 'FM0999999V99')
                || lpad_fn(b.ordqtb, 7, '0')
                || lpad_fn(b.alcqtb, 7, '0')
                || lpad_fn(b.pckqtb, 7, '0')
                || TO_CHAR(x.eta_ts, 'YYYYMMDDHH24MI')
                || rpad_fn(e.scbcte, 3)
                || DECODE(p.cust_num, NULL, 'N', 'N', 'N', 'Y')
                || RPAD(NVL(b.ntshpb, '120'), 3)
                || TO_CHAR(NVL(b.hdprcb, 0), 'FM0999999V99')
                || TO_CHAR(NVL(b.hdrtab, 0), 'FM0999999V99')
                || RPAD(' ', 10)
                || lpad_fn(b.cusitb, 9, '0')
                || lpad_fn((CASE
                              WHEN(    a.dsorda = 'R'
                                   AND SUBSTR(b.itpasb, 17, 3) = 'WMT') THEN SUBSTR(b.itpasb, 6, 2)
                              WHEN(    a.dsorda = 'D'
                                   AND SUBSTR(b.itpasb, 18, 3) = 'WMT') THEN SUBSTR(b.itpasb, 7, 2)
                              ELSE '00'
                            END
                           ),
                           2,
                           '0'
                          )
                || lpad_fn((CASE
                              WHEN(    a.dsorda = 'R'
                                   AND SUBSTR(b.itpasb, 17, 3) = 'WMT') THEN SUBSTR(a.cpoa, 1, 2)
                              WHEN(    a.dsorda = 'D'
                                   AND SUBSTR(b.itpasb, 18, 3) = 'WMT') THEN SUBSTR(a.cpoa, 20, 2)
                              ELSE '00'
                            END
                           ),
                           2,
                           '0'
                          )
                || RPAD(' ', 8) AS DATA
           FROM (SELECT ld.load_depart_sid, ld.llr_dt AS llr_dt, ld.load_num, se.cust_id, se.stop_num, se.eta_ts
                   FROM load_depart_op1f ld, stop_eta_op1g se
                  WHERE ld.div_part = l_div_part
                    AND (ld.llr_dt, ld.load_num) IN(SELECT SUBSTR(t.column_value, 1, 4) AS load_num,
                                                           TO_DATE(SUBSTR(t.column_value, 5), 'YYYYMMDD') AS llr_dt
                                                      FROM TABLE(CAST(i_t_load_llrs AS type_stab)) t)
                    AND se.div_part = ld.div_part
                    AND se.load_depart_sid = ld.load_depart_sid) x,
                ordp100a a, ordp120b b, mclp020b cx, sawp505e e, prepost_load_op1p p
          WHERE a.div_part = l_div_part
            AND a.load_depart_sid = x.load_depart_sid
            AND a.custa = x.cust_id
            AND a.excptn_sw = 'N'
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.excptn_sw = 'N'
            AND b.statb = 'R'
            AND b.alcqtb > 0
            AND b.alcqtb > b.pckqtb
            AND NOT EXISTS(SELECT 1
                             FROM bulk_out_bo1o bo1o
                            WHERE bo1o.div_part = b.div_part
                              AND bo1o.ord_num = b.ordnob
                              AND bo1o.ord_ln_num = b.lineb
                              AND NOT EXISTS(SELECT 1
                                               FROM sysp200c c
                                              WHERE c.div_part = a.div_part
                                                AND c.acnoc = a.custa
                                                AND c.tclscc = 'PRP'))
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND p.div_part(+) = l_div_part
            AND p.load_num(+) = x.load_num
            AND p.stop_num(+) = x.stop_num
            AND p.cust_num(+) = x.cust_id
            AND p.llr_date(+) = x.llr_dt;

      print_sp(l_cv, l_c_opxd17_local_file_nm, l_c_file_dir, l_opxc17_cnt);
      logs.dbg('Open OPXD28 Cursor');

      OPEN l_cv
       FOR
         SELECT rpad_fn(i_div, 2)
                || RPAD('OPXD28', 38)
                || RPAD('ADD', 13)
                || rpad_fn(ld.load_num, 4)
                || lpad_fn(se.stop_num, 2, '0')
                || LPAD(bc.ord_num, 11, '0')
                || TO_CHAR(bc.ord_ln_num, 'FM0999999V99')
                || rpad_fn(bc.orig_cntnr_id, 20)
                || rpad_fn(bc.adj_cntnr_id, 20)
                || lpad_fn(bc.orig_qty, 7, '0')
                || lpad_fn(bc.adj_qty, 7, '0') AS DATA
           FROM (SELECT SUBSTR(t.column_value, 1, 4) AS load_num,
                        TO_DATE(SUBSTR(t.column_value, 5), 'YYYYMMDD') AS llr_dt
                   FROM TABLE(CAST(i_t_load_llrs AS type_stab)) t) x,
                load_depart_op1f ld, ordp100a a, ordp120b b, bill_cntnr_id_bc1c bc, stop_eta_op1g se
          WHERE ld.div_part = l_div_part
            AND ld.llr_dt = x.llr_dt
            AND ld.load_num = x.load_num
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.excptn_sw = 'N'
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.excptn_sw = 'N'
            AND b.statb = 'R'
            AND bc.div_part = b.div_part
            AND bc.ord_num = b.ordnob
            AND bc.ord_ln_num = b.lineb
            AND (   bc.adj_cntnr_id <> bc.orig_cntnr_id
                 OR bc.adj_qty <> bc.orig_qty)
            AND NOT EXISTS(SELECT 1
                             FROM bulk_out_bo1o bo1o
                            WHERE bo1o.div_part = b.div_part
                              AND bo1o.ord_num = b.ordnob
                              AND bo1o.ord_ln_num = b.lineb
                              AND NOT EXISTS(SELECT 1
                                               FROM sysp200c c
                                              WHERE c.div_part = a.div_part
                                                AND c.acnoc = a.custa
                                                AND c.tclscc = 'PRP'))
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa;

      print_sp(l_cv, l_c_opxd28_local_file_nm, l_c_file_dir, l_opxc28_cnt);
      logs.dbg('Open OPXD21 Cursor');

      OPEN l_cv
       FOR
         SELECT   rpad_fn(i_div, 2)
                  || RPAD('OPXD21', 38)
                  || RPAD('ADD', 13)
                  || rpad_fn('PICK_COMPL', 20)   -- userid
                  || LPAD('0', 9, '0')   -- qoprc08
                  || lpad_fn(l_opxc17_cnt, 9, '0')
                  || RPAD(c.loadc, 4)
                  || rpad_fn(MAX(TO_CHAR(DATE '1900-02-28' + c.depdtc, 'YYYYMMDD') || LPAD(ld.deptmc, 4, '0')), 12)
                  || lpad_fn(l_opxc28_cnt, 9, '0')
                  || TO_CHAR(DATE '1900-02-28' + c.llr_date, 'YYYY-MM-DD') AS msg_data
             FROM (SELECT SUBSTR(t.column_value, 1, 4) AS load_num,
                          TO_DATE(SUBSTR(t.column_value, 5), 'YYYYMMDD') - DATE '1900-02-28' AS llr_num
                     FROM TABLE(CAST(i_t_load_llrs AS type_stab)) t) x,
                  mclp370c c, mclp120c ld
            WHERE c.div_part = l_div_part
              AND c.loadc = x.load_num
              AND c.llr_date = x.llr_num
              AND ld.div_part = c.div_part
              AND ld.loadc = c.loadc
         GROUP BY c.llr_date, c.loadc
         ORDER BY c.llr_date, c.loadc;

      print_sp(l_cv, l_c_opxd21_local_file_nm, l_c_file_dir, l_opxc21_cnt);
      logs.dbg('FTP OPXD17 to mainframe');
      op_ftp_sp(i_div, l_c_opxd17_local_file_nm, l_c_opxd17_rmt_file, l_c_archive, l_c_no_gdg);
      logs.dbg('FTP OPXD28 to mainframe');
      op_ftp_sp(i_div, l_c_opxd28_local_file_nm, l_c_opxd28_rmt_file, l_c_archive, l_c_no_gdg);
      logs.dbg('FTP OPXD21 to mainframe');
      op_ftp_sp(i_div, l_c_opxd21_local_file_nm, l_c_opxd21_rmt_file, l_c_archive, l_c_no_gdg);
    END IF;   -- i_t_load_llrs IS NOT NULL AND i_t_load_llrs.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extr_line_outs_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_PICK_COMPL_SP
  ||  For each LLR/Load passed, set to Pick-Completed and create XDock reports
  ||  and ftp to mainframe.
  ||
  ||  ParmList Format:
  ||  LLRDt~Load`LLRDt~Load`LLRDt~Load
  ||  (LLRDt in YYYY-MM-DD format)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/02/09 | rhalpai | Moved process logic from PIC_COMPL_SP. Converted
  ||                    | the identification of XDOCK Loads in cursor for
  ||                    | Pick Complete Parm List from using parms for load
  ||                    | ranges to parms for non-contiguous loads. Changed to
  ||                    | use TestBilSW parm to indicate whether processing for
  ||                    | a TestBill or Production. PIR7342
  || 12/30/09 | rhalpai | Added call to EXTR_OPXDEAR_SP. PIR7342
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_pick_compl_sp(
    i_div          IN  VARCHAR2,
    i_parm_list    IN  VARCHAR2,
    i_test_bil_sw  IN  VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.PRCS_PICK_COMPL_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_xdock_pick_compl_sw  VARCHAR2(1);
    l_t_xdock_loads        type_stab;
    l_t_load_llrs          type_stab;
    l_c_rpt_ts    CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'TestBilSW', i_test_bil_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_xdock_pick_compl_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_xdock_pick_compl);
    l_t_xdock_loads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xdock_load);

    IF l_xdock_pick_compl_sw = 'Y' THEN
      logs.dbg('Set LLR/Load to Pick Completed');

      UPDATE    load_clos_cntrl_bc2c lc
            SET lc.pick_compl_sw = 'Y'
          WHERE lc.div_part = l_div_part
            AND lc.pick_compl_sw = 'N'
            AND lc.llr_dt = DECODE(INSTR('`' || i_parm_list || '`',
                                         '`' || TO_CHAR(lc.llr_dt, 'YYYY-MM-DD') || '~' || lc.load_num || '`'
                                        ),
                                   0, NULL,
                                   lc.llr_dt
                                  )
            AND lc.load_num = DECODE(INSTR('`' || i_parm_list || '`',
                                           '`' || TO_CHAR(lc.llr_dt, 'YYYY-MM-DD') || '~' || lc.load_num || '`'
                                          ),
                                     0, NULL,
                                     lc.load_num
                                    )
            AND lc.load_num IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_xdock_loads AS type_stab)) t)
            AND lc.test_bil_load_sw = i_test_bil_sw
            AND lc.load_status IN('P', 'R')
      RETURNING         lc.load_num || TO_CHAR(lc.llr_dt, 'YYYYMMDD')
      BULK COLLECT INTO l_t_load_llrs;

      IF l_t_load_llrs.COUNT > 0 THEN
        logs.dbg('Add Mfst Rpt Data');

        INSERT INTO mclane_manifest_rpts
                    (create_ts, strategy_id, div_part, llr_date, load_num, stop_num, eta_date, eta_time, manifest_cat,
                     cust_num, tote_cat, tote_count, box_count, bag_count, departure_date, qty_alloc, product_weight,
                     product_cube)
          SELECT   l_c_rpt_ts, 0 AS strtg_id, l_div_part, ld.llr_dt - DATE '1900-02-28' AS llr_dt, ld.load_num,
                   se.stop_num, TRUNC(se.eta_ts) - DATE '1900-02-28' AS eta_dt,
                   TO_NUMBER(TO_CHAR(se.eta_ts, 'HH24MI')) AS eta_tm, NVL(b.manctb, '000') AS manf_catg, se.cust_id,
                   NVL(b.totctb, '000') AS tote_catg, c.totsmc AS tote_cnt, c.boxsmc AS box_cnt, c.bagsmc AS bag_cnt,
                   TRUNC(ld.depart_ts) - DATE '1900-02-28' AS depart_dt,
                   NVL(SUM(CASE
                             WHEN NOT EXISTS(SELECT 1
                                               FROM kit_item_mstr_kt1m k
                                              WHERE k.div_part = l_div_part
                                                AND k.comp_item_num = b.orditb) THEN b.pckqtb
                             ELSE b.pckqtb
                                  / (SELECT MAX(k.comp_qty)
                                       FROM kit_item_mstr_kt1m k
                                      WHERE k.div_part = b.div_part
                                        AND k.comp_item_num = b.orditb
                                        AND k.comp_item_num =
                                              (SELECT MAX(k2.comp_item_num)
                                                 FROM kit_item_mstr_kt1m k2
                                                WHERE k2.div_part = k.div_part
                                                  AND k2.kit_typ = k.kit_typ
                                                  AND k2.item_num = k.item_num))
                           END
                          ),
                       0
                      ) AS qty_alloc,
                   NVL(SUM(NVL(e.wghte, .01) * b.pckqtb), 0) AS prod_wt,
                   NVL(SUM(NVL(e.cubee, .01) * b.pckqtb), 0) AS prod_cube
              FROM (SELECT SUBSTR(t.column_value, 1, 4) AS load_num,
                           TO_DATE(SUBSTR(t.column_value, 5, 8), 'YYYYMMDD') AS llr_dt
                      FROM TABLE(CAST(l_t_load_llrs AS type_stab)) t) x,
                   load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, sawp505e e, mclp370c c, mclp200b t
             WHERE ld.div_part = l_div_part
               AND ld.llr_dt = x.llr_dt
               AND ld.load_num = x.load_num
               AND a.div_part = ld.div_part
               AND a.load_depart_sid = ld.load_depart_sid
               AND a.excptn_sw = 'N'
               AND se.div_part = a.div_part
               AND se.load_depart_sid = a.load_depart_sid
               AND se.cust_id = a.custa
               AND b.div_part = a.div_part
               AND b.ordnob = a.ordnoa
               AND b.excptn_sw = 'N'
               AND b.statb IN('T', 'R', 'A')
               AND e.iteme = b.itemnb
               AND e.uome = b.sllumb
               AND c.div_part = ld.div_part
               AND c.llr_date = ld.llr_dt - DATE '1900-02-28'
               AND c.loadc = ld.load_num
               AND c.stopc = se.stop_num
               AND c.depdtc = TRUNC(ld.depart_ts) - DATE '1900-02-28'
               AND c.manctc = NVL(b.manctb, '000')
               AND NVL(c.totctc, '000') = NVL(b.totctb, '000')
               AND c.test_bil_load_sw = i_test_bil_sw
               AND t.div_part(+) = b.div_part
               AND t.totctb(+) = b.totctb
          GROUP BY ld.llr_dt, TRUNC(ld.depart_ts), ld.load_num, se.stop_num, se.cust_id, se.eta_ts, b.manctb, b.totctb,
                   c.totsmc, c.bagsmc, c.boxsmc;

        logs.dbg('Process OPXD01 XDock Loading Manifest Report');
        op_misc_reports_pk.opxd01_rpt_sp(i_div, l_c_rpt_ts, i_test_bil_sw);
        logs.dbg('Process OPXD02 XDock Load Summary Report');
        op_misc_reports_pk.opxd02_rpt_sp(i_div, l_c_rpt_ts, i_test_bil_sw);
        logs.dbg('Process OPXD03 XDock Load Recap Report');
        op_misc_reports_pk.opxd03_rpt_sp(i_div, l_c_rpt_ts, i_test_bil_sw);
        logs.dbg('Extract OPXDEAR Data for OPXD01');
        extr_opxdear_sp(i_div, l_c_rpt_ts, i_test_bil_sw);
        logs.dbg('Extract Line-Outs');
        extr_line_outs_sp(i_div, l_t_load_llrs, l_c_rpt_ts, i_test_bil_sw);
      END IF;   -- l_t_load_llrs.COUNT > 0
    END IF;   -- l_xdock_pick_compl_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prcs_pick_compl_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_AGG_COMP_QTY_SP
  ||  Check for Aggregate component item with invalid out quantity.
  ||  Check if order line is for a component of an Aggregate Kit and if so ensure
  ||  the out quantity is a multiple of the component quantity.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/02/05 | rhalpai | Initial creation. PIR# 2909.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_agg_comp_qty_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_ord_ln    IN      NUMBER,
    i_out_qty   IN      NUMBER,
    o_err_msg   OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.CHECK_AGG_COMP_QTY_SP';
    lar_parm             logs.tar_parm;
    l_comp_qty           PLS_INTEGER;
    l_rcmd_qty           PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'OutQty', i_out_qty);
    logs.dbg('ENTRY', lar_parm);

    IF NVL(i_out_qty, 0) <> 0 THEN
      SELECT k.comp_qty
        INTO l_comp_qty
        FROM ordp120b o, kit_item_mstr_kt1m k
       WHERE o.div_part = i_div_part
         AND o.ordnob = i_ord_num
         AND o.lineb = i_ord_ln
         AND k.div_part = o.div_part
         AND k.comp_item_num = o.orditb
         AND k.kit_typ = 'AGG';

      IF MOD(i_out_qty, l_comp_qty) <> 0 THEN
        l_rcmd_qty := CEIL(i_out_qty / l_comp_qty) * l_comp_qty;

        IF l_rcmd_qty = 0 THEN
          l_rcmd_qty := FLOOR(i_out_qty / l_comp_qty) * l_comp_qty;
        END IF;   -- l_rcmd_qty = 0

        o_err_msg := 'Item is a component of a kit and out quantity of '
                     || i_out_qty
                     || ' needs to be a multiple of '
                     || l_comp_qty
                     || '. Recommend using '
                     || l_rcmd_qty
                     || ' as the out quantity.';
      END IF;   -- MOD(i_out_qty, l_comp_qty) <> 0
    END IF;   -- NVL(i_out_qty, 0) <> 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_agg_comp_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_PICK_SP
  ||  Update out quantity.
  ||  The "check kit" parm should be set to TRUE or allowed to default to TRUE.
  ||  It performs a check to see if the item for the order line is a kit
  ||  component.  If a component item is found the out quantity is validated
  ||  to be a multiple of the component quantity and then all component items
  ||  for the customer are adjusted at the same ratio by making recursive
  ||  calls for each component item to this procedure passing FALSE to the
  ||  "check kit" parm to apply the update.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/13/05 | CXAMART | Initial creation. PIR# 179.
  || 11/02/05 | rhalpai | Added Aggregate Item logic. PIR# 2909.
  || 02/26/06 | rhalpai | Fix object return type mismatch for kit_rec_fn.
  || 12/12/06 | rhalpai | Convert to select not-ship-reason codes from MCLP140A
  ||                    | using reason type = 12 rather than using a hard-coded
  ||                    | range (120-128) for valid Pick Adjustment codes.
  ||                    | Changed to set not-ship-rsn to NULL when reversing a
  ||                    | pick (using a negative out-qty) back to allocated qty.
  ||                    | Added msg-typ and auto-commit parms. PIR3209
  || 02/19/07 | Arun    | Removed sysp296a insert and changed to call
  ||                    | op_sysp296a_pk instead.
  || 11/10/10 | rhalpai | Remove unused columns. Convert to use standard error
  ||                    | handling logic. PIR5878
  || 07/05/01 | rhalpai | Added logic to adjust PickQty on WklyMaxCustItem and
  ||                    | WklyMaxLog tables. PIR6235
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 12/30/13 | rhalpai | Change logic to bypass lineouts for Catchweight items
  ||                    | and return error msg indicating these linouts must be
  ||                    | done in Catchweight system. PIR12765
  || 03/10/14 | rhalpai | Change logic to return error msg when out qty is not
  ||                    | a multiple of MasterCase Qty for MasterCase Customers.
  ||                    | PIR13399
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_pick_sp(
    i_div_part        IN      NUMBER,
    i_ord_num         IN      NUMBER,
    i_ord_ln          IN      NUMBER,
    i_out_qty         IN      NUMBER,
    i_not_shp_rsn     IN      VARCHAR2,
    i_bill_err_rsn    IN      VARCHAR2,
    i_user_id         IN      VARCHAR2,
    o_msg             OUT     VARCHAR2,
    o_msg_typ         OUT     VARCHAR2,
    i_is_check_kit    IN      BOOLEAN DEFAULT TRUE,
    i_is_auto_commit  IN      BOOLEAN DEFAULT TRUE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.UPD_PICK_SP';
    lar_parm             logs.tar_parm;

    FUNCTION kit_rec_fn(
      i_div_part  IN  NUMBER,
      i_ord_num   IN  NUMBER,
      i_ord_ln    IN  NUMBER
    )
      RETURN kit_t IS
      l_cv     SYS_REFCURSOR;
      l_r_kit  kit_t         := kit_t(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    BEGIN
      OPEN l_cv
       FOR
         SELECT kit_t(d.div_id,
                      ld.llr_dt - DATE '1900-02-28',
                      k.kit_typ,
                      a.dsorda,
                      k.item_num,
                      a.custa,
                      ld.load_num,
                      se.stop_num,
                      TRUNC(se.eta_ts) - DATE '1900-02-28',
                      a.cpoa
                     )
           FROM div_mstr_di1d d, ordp120b b, kit_item_mstr_kt1m k, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
          WHERE d.div_part = i_div_part
            AND b.div_part = d.div_part
            AND b.ordnob = i_ord_num
            AND b.lineb = i_ord_ln
            AND k.div_part = b.div_part
            AND k.comp_item_num = b.orditb
            AND k.kit_typ = 'AGG'
            AND a.div_part = b.div_part
            AND a.ordnoa = b.ordnob
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa;

      FETCH l_cv
       INTO l_r_kit;

      RETURN(l_r_kit);
    END kit_rec_fn;

    PROCEDURE process_kit_comp_sp(
      i_r_kit           IN      kit_t,
      i_div_part        IN      NUMBER,
      i_ord_num         IN      NUMBER,
      i_ord_ln          IN      NUMBER,
      i_not_shp_rsn     IN      VARCHAR2,
      i_bill_err_rsn    IN      VARCHAR2,
      i_user_id         IN      VARCHAR2,
      i_is_auto_commit  IN      BOOLEAN,
      o_msg             OUT     VARCHAR2,
      o_msg_typ         OUT     VARCHAR2
    ) IS
      l_t_kit_ords                  kit_ords_t;
      l_c_billed_ord_stat  CONSTANT VARCHAR2(1) := 'R';
      l_idx                         PLS_INTEGER;
      l_seq                         PLS_INTEGER;
      l_new_ratio                   PLS_INTEGER;
      l_out_qty                     PLS_INTEGER;
      l_cnt                         PLS_INTEGER;
    BEGIN
      logs.dbg('Get table of kit orders');
      l_t_kit_ords := op_allocate_pk.kit_ord_tab_fn(l_c_billed_ord_stat, i_r_kit);
      ---------------------------------------------------------------------
      -- Note:
      --   When pick qty is greater than 0 it is stored in
      --   l_t_kit_ords(i).ord_qty
      ---------------------------------------------------------------------
      logs.dbg('Find seq and new ratio for order');
      l_idx := l_t_kit_ords.FIRST;
      WHILE(    l_idx IS NOT NULL
            AND l_seq IS NULL) LOOP
        IF (    l_t_kit_ords(l_idx).order_num = i_ord_num
            AND l_t_kit_ords(l_idx).order_ln = i_ord_ln) THEN
          l_seq := l_t_kit_ords(l_idx).seq;
          l_new_ratio := (l_t_kit_ords(l_idx).ord_qty - i_out_qty) / l_t_kit_ords(l_idx).comp_qty;
        END IF;

        l_idx := l_t_kit_ords.NEXT(l_idx);
      END LOOP;
      logs.dbg('Process each component order for seq');
      l_cnt := 0;
      FOR i IN l_t_kit_ords.FIRST .. l_t_kit_ords.LAST LOOP
        IF (    o_msg_typ <> op_const_pk.msg_typ_err
            AND l_t_kit_ords(i).seq = l_seq) THEN
          logs.dbg('Recursive call for component');
          l_out_qty := l_t_kit_ords(i).ord_qty -(l_t_kit_ords(i).comp_qty * l_new_ratio);
          upd_pick_sp(i_div_part,
                      l_t_kit_ords(i).order_num,
                      l_t_kit_ords(i).order_ln,
                      l_out_qty,
                      i_not_shp_rsn,
                      i_bill_err_rsn,
                      i_user_id,
                      o_msg,
                      o_msg_typ,
                      FALSE,
                      i_is_auto_commit
                     );
          l_cnt := l_cnt + 1;
        END IF;   -- o_msg_typ <> op_const_pk.msg_typ_err AND l_t_kit_ords(i).seq = l_seq
      END LOOP;

      IF o_msg_typ <> op_const_pk.msg_typ_err THEN
        o_msg := l_cnt || ' order lines updated for Kit Item ' || i_r_kit.kit_item_num || '.';
      END IF;   -- o_msg_typ <> op_const_pk.msg_typ_err
    END process_kit_comp_sp;

    PROCEDURE check_kit_comp_sp(
      i_div_part        IN      NUMBER,
      i_ord_num         IN      NUMBER,
      i_ord_ln          IN      NUMBER,
      i_out_qty         IN      NUMBER,
      i_not_shp_rsn     IN      VARCHAR2,
      i_bill_err_rsn    IN      VARCHAR2,
      i_user_id         IN      VARCHAR2,
      i_is_auto_commit  IN      BOOLEAN,
      o_msg             OUT     VARCHAR2,
      o_msg_typ         OUT     VARCHAR2
    ) IS
      l_r_kit  kit_t;
    BEGIN
      logs.dbg('Get record of kit info');
      l_r_kit := kit_rec_fn(i_div_part, i_ord_num, i_ord_ln);

      -- was kit found
      IF l_r_kit.kit_item_num IS NOT NULL THEN
        logs.dbg('Validate out qty is multiple of comp qty');
        check_agg_comp_qty_sp(i_div_part, i_ord_num, i_ord_ln, i_out_qty, o_msg);

        -- if no out qty error found (fall out and return error msg when found)
        IF o_msg IS NOT NULL THEN
          o_msg_typ := op_const_pk.msg_typ_err;
        ELSE
          logs.dbg('Process kit component');
          process_kit_comp_sp(l_r_kit,
                              i_div_part,
                              i_ord_num,
                              i_ord_ln,
                              i_not_shp_rsn,
                              i_bill_err_rsn,
                              i_user_id,
                              i_is_auto_commit,
                              o_msg,
                              o_msg_typ
                             );
        END IF;   -- io_msg IS NOT NULL
      ELSE
        -- no kit found so process current order line
        logs.dbg('Recursive call for non-component');
        upd_pick_sp(i_div_part,
                    i_ord_num,
                    i_ord_ln,
                    i_out_qty,
                    i_not_shp_rsn,
                    i_bill_err_rsn,
                    i_user_id,
                    o_msg,
                    o_msg_typ,
                    FALSE,
                    i_is_auto_commit
                   );
      END IF;   -- l_r_kit.kit_item_num IS NOT NULL
    END check_kit_comp_sp;

    PROCEDURE process_pick_sp(
      i_div_part        IN      NUMBER,
      i_ord_num         IN      NUMBER,
      i_ord_ln          IN      NUMBER,
      i_out_qty         IN      NUMBER,
      i_not_shp_rsn     IN      VARCHAR2,
      i_bill_err_rsn    IN      VARCHAR2,
      i_user_id         IN      VARCHAR2,
      i_is_auto_commit  IN      BOOLEAN,
      o_msg             OUT     VARCHAR2,
      o_msg_typ         OUT     VARCHAR2
    ) IS
      l_t_mstr_cs_corps      type_stab;
      l_rcmd_qty             PLS_INTEGER;
      l_c_bill_err  CONSTANT VARCHAR2(3)         := '122';
      l_new_pick_qty         PLS_INTEGER;

      CURSOR l_cur_ord(
        b_div_part         NUMBER,
        b_ord_num          NUMBER,
        b_ord_ln           NUMBER,
        b_t_mstr_cs_corps  type_stab
      ) IS
        SELECT b.pckqtb AS pick_qty, b.ntshpb AS nt_shp_rsn, di.cwt_sw, e.mulsle AS mstr_cs_qty,
               NVL(mcc.val, 'N') AS mstr_cs_cust_sw
          FROM ordp120b b, ordp100a a, mclp020b cx, mclp110b di, sawp505e e,
               (SELECT TO_NUMBER(t.column_value) AS corp_cd, 'Y' AS val
                  FROM TABLE(CAST(b_t_mstr_cs_corps AS type_stab)) t) mcc
         WHERE b.div_part = b_div_part
           AND b.ordnob = b_ord_num
           AND b.lineb = b_ord_ln
           AND a.div_part = b.div_part
           AND a.ordnoa = b.ordnob
           AND cx.div_part = a.div_part
           AND cx.custb = a.custa
           AND mcc.corp_cd(+) = cx.corpb
           AND di.div_part = b.div_part
           AND di.itemb = b.itemnb
           AND di.uomb = b.sllumb
           AND e.iteme = b.itemnb
           AND e.uome = b.sllumb;

      l_r_ord                l_cur_ord%ROWTYPE;

      PROCEDURE log_sp(
        i_div_part      IN  NUMBER,
        i_ord_num       IN  NUMBER,
        i_ord_ln        IN  NUMBER,
        i_user_id       IN  VARCHAR2,
        i_old_pick_qty  IN  NUMBER,
        i_new_pick_qty  IN  NUMBER,
        i_rsn_cd        IN  VARCHAR2,
        i_rsn_txt       IN  VARCHAR2
      ) IS
        l_r_sysp296a  sysp296a%ROWTYPE;
      BEGIN
        l_r_sysp296a.div_part := i_div_part;
        l_r_sysp296a.ordnoa := i_ord_num;
        l_r_sysp296a.linea := i_ord_ln;
        l_r_sysp296a.usera := i_user_id;
        l_r_sysp296a.tblnma := 'ORDP120B';
        l_r_sysp296a.fldnma := 'PCKQTB';
        l_r_sysp296a.florga := i_old_pick_qty;
        l_r_sysp296a.flchga := i_new_pick_qty;
        l_r_sysp296a.actna := 'O';
        l_r_sysp296a.rsncda := i_rsn_cd;
        l_r_sysp296a.rsntxa := i_rsn_txt;
        op_sysp296a_pk.ins_sp(l_r_sysp296a);
      END log_sp;
    BEGIN
      logs.dbg('Initialize');
      l_t_mstr_cs_corps := op_parms_pk.parms_for_val_fn(i_div_part, op_const_pk.prm_alloc_mstr_cs, 'Y', 3);
      logs.dbg('Get order info');

      OPEN l_cur_ord(i_div_part, i_ord_num, i_ord_ln, l_t_mstr_cs_corps);

      FETCH l_cur_ord
       INTO l_r_ord;

      CLOSE l_cur_ord;

      -- was order found
      IF l_r_ord.cwt_sw IS NOT NULL THEN
        IF l_r_ord.cwt_sw = 'Y' THEN
          o_msg_typ := op_const_pk.msg_typ_err;
          o_msg := 'Catchweight line outs must be done in the catchweight system!';
        ELSIF(    l_r_ord.mstr_cs_cust_sw = 'Y'
              AND l_r_ord.mstr_cs_qty > 0
              AND MOD(i_out_qty, l_r_ord.mstr_cs_qty) > 0) THEN
          l_rcmd_qty := CEIL(i_out_qty / l_r_ord.mstr_cs_qty) * l_r_ord.mstr_cs_qty;

          IF l_rcmd_qty = 0 THEN
            l_rcmd_qty := FLOOR(i_out_qty / l_r_ord.mstr_cs_qty) * l_r_ord.mstr_cs_qty;
          END IF;   -- l_rcmd_qty = 0

          o_msg_typ := op_const_pk.msg_typ_err;
          o_msg := 'Out quantity of '
                   || i_out_qty
                   || ' needs to be a multiple of '
                   || l_r_ord.mstr_cs_qty
                   || '. Recommend using '
                   || l_rcmd_qty
                   || ' as the out quantity.';
        ELSE
          l_new_pick_qty := l_r_ord.pick_qty - i_out_qty;

          IF l_new_pick_qty >= 0 THEN
            logs.dbg('Apply pick adjustment');

            -- set not-ship-rsn to NULL if reverse pick back to allocated qty
            UPDATE ordp120b
               SET pckqtb = l_new_pick_qty,
                   ntshpb = DECODE(l_new_pick_qty, alcqtb, NULL, i_not_shp_rsn)
             WHERE div_part = i_div_part
               AND ordnob = i_ord_num
               AND lineb = i_ord_ln
               AND (   ntshpb IS NULL
                    OR ntshpb IN(SELECT rsncda
                                   FROM mclp140a
                                  WHERE rsntpa = 12));

            UPDATE wkly_max_log_op3m l
               SET l.qty = GREATEST(l.qty - i_out_qty, 0)
             WHERE l.div_part = i_div_part
               AND l.qty_typ = 'PCK'
               AND l.ord_num = i_ord_num
               AND l.ord_ln = i_ord_ln;

            UPDATE wkly_max_cust_item_op1m ci
               SET ci.pick_qty = GREATEST(ci.pick_qty - i_out_qty, 0)
             WHERE ci.div_part = i_div_part
               AND (ci.cust_id, ci.catlg_num) = (SELECT a.custa, b.orditb
                                                   FROM ordp100a a, ordp120b b
                                                  WHERE a.div_part = i_div_part
                                                    AND a.ordnoa = i_ord_num
                                                    AND b.div_part = a.div_part
                                                    AND b.ordnob = i_ord_num
                                                    AND b.lineb = i_ord_ln);

            -- Insert record into SYSP296A if the current or
            -- previous reason code is Billing Error
            IF l_c_bill_err IN(i_not_shp_rsn, l_r_ord.nt_shp_rsn) THEN
              logs.dbg('Log change for billing error');
              log_sp(i_div_part,
                     i_ord_num,
                     i_ord_ln,
                     i_user_id,
                     l_r_ord.pick_qty,
                     l_new_pick_qty,
                     i_not_shp_rsn,
                     i_bill_err_rsn
                    );
            END IF;   -- l_c_bill_err IN(i_not_shp_rsn, l_r_ord.ntshpb)
          END IF;   -- l_new_pick_qty >= 0

          IF i_is_auto_commit THEN
            COMMIT;
          END IF;   -- i_is_auto_commit
        END IF;   -- l_r_ord.cwt_sw = 'Y'
      END IF;   -- l_r_ord.cwt_sw IS NOT NULL
    EXCEPTION
      WHEN OTHERS THEN
        IF l_cur_ord%ISOPEN THEN
          CLOSE l_cur_ord;
        END IF;

        RAISE;
    END process_pick_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'OutQty', i_out_qty);
    logs.add_parm(lar_parm, 'NotShpRsn', i_not_shp_rsn);
    logs.add_parm(lar_parm, 'BillErrRsn', i_bill_err_rsn);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'IsCheckKit', i_is_check_kit);
    logs.add_parm(lar_parm, 'IsAutoCommit', i_is_auto_commit);
    logs.info('ENTRY', lar_parm);
    o_msg_typ := op_const_pk.msg_typ_info;

    IF i_is_check_kit THEN
      logs.dbg('Check for kit component');
      check_kit_comp_sp(i_div_part,
                        i_ord_num,
                        i_ord_ln,
                        i_out_qty,
                        i_not_shp_rsn,
                        i_bill_err_rsn,
                        i_user_id,
                        i_is_auto_commit,
                        o_msg,
                        o_msg_typ
                       );
    ELSE
      logs.dbg('Process pick adjustment');
      process_pick_sp(i_div_part,
                      i_ord_num,
                      i_ord_ln,
                      i_out_qty,
                      i_not_shp_rsn,
                      i_bill_err_rsn,
                      i_user_id,
                      i_is_auto_commit,
                      o_msg,
                      o_msg_typ
                     );
    END IF;   -- i_is_check_kit

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_pick_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || OUT_REASON_CODE_FN
  ||  Return Out Reason code and description
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/06/05 | CXAMART | Original PIR# 179.
  || 08/29/06 | rhalpai | Default rsn_typ to 12. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION out_reason_code_fn(
    i_rsn_typ  IN  NUMBER DEFAULT 12
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.OUT_REASON_CODE_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'RsnTyp', i_rsn_typ);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   rsncda, desca
           FROM mclp140a
          WHERE rsntpa = i_rsn_typ
       ORDER BY rsncda;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END out_reason_code_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_DATA_DISPLAY_FN
  ||  Return cursor
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/10/05 | CXAMART | Initial creation. PIR# 179.
  || 08/29/06 | rhalpai | Changed cursor to include MCLP020B and SAWP505E to
  ||                    | utilize indexes.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_data_display_fn(
    i_div        IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.RETRIEVE_DATA_DISPLAY_FN';
    lar_parm               logs.tar_parm;
    l_cv                   SYS_REFCURSOR;
    l_c_bulk_out  CONSTANT VARCHAR2(3)   := '129';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln, a.dsorda AS ord_typ, ld.load_num, se.stop_num, b.alcqtb,
                b.pckqtb AS pck_qty,(b.alcqtb - b.pckqtb) AS out_qty,
                (SELECT m.desca
                   FROM mclp140a m
                  WHERE m.rsncda = b.ntshpb
                    AND m.rsncda <> l_c_bulk_out) AS rsn_descr
           FROM div_mstr_di1d d, sawp505e e, mclp020b cx, ordp100a a, ordp120b b, load_depart_op1f ld,
                stop_eta_op1g se
          WHERE d.div_id = i_div
            AND e.catite = i_catlg_num
            AND cx.div_part = d.div_part
            AND cx.mccusb = i_mcl_cust
            AND a.div_part = cx.div_part
            AND a.custa = cx.custb
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.itemnb = e.iteme
            AND b.sllumb = e.uome
            AND b.excptn_sw = 'N'
            AND b.statb = 'R'
            AND b.alcqtb > 0
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
       ORDER BY a.dsorda DESC, a.pshipa;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_data_display_fn;

  /*
  ||----------------------------------------------------------------------------
  || CNTNR_ID_LIST_FN
  ||  Return cursor of container ID's for customer/item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original PIR3209
  || 06/19/07 | rhalpai | Changed cursor to enhance performance. IM315516
  || 06/16/08 | rhalpai | Added sort by ContainerId to cursor.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  ||----------------------------------------------------------------------------
  */
  FUNCTION cntnr_id_list_fn(
    i_div        IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.CNTNR_ID_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   NVL(bc.adj_cntnr_id, bc.orig_cntnr_id) AS cntnr_id
           FROM div_mstr_di1d d, mclp020b cx, ordp100a a, bill_cntnr_id_bc1c bc, sawp505e e, ordp120b b
          WHERE d.div_id = i_div
            AND cx.div_part = d.div_part
            AND cx.mccusb = i_mcl_cust
            AND a.div_part = cx.div_part
            AND a.custa = cx.custb
            AND bc.div_part = a.div_part
            AND bc.ord_num = a.ordnoa
            AND e.catite = i_catlg_num
            AND b.div_part = bc.div_part
            AND b.ordnob = bc.ord_num
            AND b.lineb = bc.ord_ln_num
            AND b.itemnb = e.iteme
            AND b.sllumb = e.uome
            AND b.excptn_sw = 'N'
            AND b.statb = 'R'
       GROUP BY NVL(bc.adj_cntnr_id, bc.orig_cntnr_id)
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cntnr_id_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CNTNR_ITEM_INFO_FN
  ||  Retrieve load,stop,total orig qty,total adj qty for an item in a container.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original PIR3209
  || 06/16/08 | rhalpai | Added sort by Load/Stop to cursor.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION cntnr_item_info_fn(
    i_div        IN  VARCHAR2,
    i_cntnr_id   IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.CNTNR_ITEM_INFO_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CntnrID', i_cntnr_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   ld.load_num, LPAD(se.stop_num, 2, '0') AS stop_num, SUM(bc.orig_qty) AS orig_qty,
                SUM(bc.adj_qty) AS adj_qty
           FROM div_mstr_di1d d, bill_cntnr_id_bc1c bc, ordp120b b, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
          WHERE d.div_id = i_div
            AND bc.div_part = d.div_part
            AND i_cntnr_id = NVL(bc.adj_cntnr_id, bc.orig_cntnr_id)
            AND b.div_part = bc.div_part
            AND b.ordnob = bc.ord_num
            AND b.lineb = bc.ord_ln_num
            AND b.orditb = i_catlg_num
            AND b.excptn_sw = 'N'
            AND b.statb = 'R'
            AND a.div_part = b.div_part
            AND a.ordnoa = b.ordnob
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
       GROUP BY ld.load_num, se.stop_num
       ORDER BY ld.load_num, se.stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cntnr_item_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || PICK_COMPL_LIST_FN
  ||  Retrieve LLR/Load list to be Pick-Completed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/09 | rhalpai | Original PIR7342
  || 12/02/09 | rhalpai | Convert identification of XDOCK Loads in cursor for
  ||                    | Pick Complete Parm List from using parms for load
  ||                    | ranges to parms for non-contiguous loads. Removed
  ||                    | restriction for non-TBills. Added column to indicate
  ||                    | TestBills to end of cursor. PIR7342
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION pick_compl_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.PICK_COMPL_LIST_FN';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_xdock_pick_compl_sw  VARCHAR2(1);
    l_xdock_loads          type_stab;
    l_cv                   SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_xdock_pick_compl_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_xdock_pick_compl);
    l_xdock_loads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xdock_load);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + c.llr_date, 'YYYY-MM-DD') AS llr_dt, c.loadc, mc.destc,
                mc.depdac || ' ' || LPAD(mc.deptmc, 4, '0') AS depart, SUM(x.prod_wt) AS prod_wt,
                NVL(SUM(DECODE(c.totsmc, 0, x.prod_cube, c.totsmc * t.outerb)), 0) AS prod_cube, c.test_bil_load_sw
           FROM mclp370c c, mclp120c mc, mclp200b t,
                (SELECT   ld.llr_dt - DATE '1900-02-28' AS llr_dt, ld.load_num, se.stop_num,
                          TRUNC(ld.depart_ts) - DATE '1900-02-28' AS depart_dt, b.manctb AS mfst_catg,
                          b.totctb AS tote_catg, NVL(SUM(NVL(e.wghte, .01) * b.pckqtb), 0) AS prod_wt,
                          NVL(SUM(NVL(e.cubee, .01) * b.pckqtb), 0) AS prod_cube
                     FROM load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e, stop_eta_op1g se
                    WHERE l_xdock_pick_compl_sw = 'Y'
                      AND ld.div_part = l_div_part
                      AND (ld.llr_dt, ld.load_num) IN(
                            SELECT lc.llr_dt, lc.load_num
                              FROM load_clos_cntrl_bc2c lc
                             WHERE lc.div_part = l_div_part
                               AND lc.load_num IN(SELECT z.column_value
                                                    FROM TABLE(CAST(l_xdock_loads AS type_stab)) z)
                               AND lc.load_status = 'R'
                               AND lc.pick_compl_sw = 'N')
                      AND a.div_part = ld.div_part
                      AND a.load_depart_sid = ld.load_depart_sid
                      AND a.excptn_sw = 'N'
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.excptn_sw = 'N'
                      AND b.statb IN('R', 'A')
                      AND b.pckqtb > 0
                      AND e.iteme = b.itemnb
                      AND e.uome = b.sllumb
                      AND se.div_part = a.div_part
                      AND se.load_depart_sid = a.load_depart_sid
                      AND se.cust_id = a.custa
                 GROUP BY ld.llr_dt, ld.load_num, se.stop_num, TRUNC(ld.depart_ts), b.manctb, b.totctb) x
          WHERE c.div_part = l_div_part
            AND c.llr_date = x.llr_dt
            AND c.loadc = x.load_num
            AND c.stopc = x.stop_num
            AND c.depdtc = x.depart_dt
            AND NVL(c.manctc, '000') = NVL(x.mfst_catg, '000')
            AND NVL(c.totctc, '000') = NVL(x.tote_catg, '000')
            AND c.load_status = 'R'
            AND mc.div_part = c.div_part
            AND mc.loadc = c.loadc
            AND t.div_part(+) = l_div_part
            AND t.totctb(+) = x.tote_catg
       GROUP BY c.llr_date, c.loadc, c.test_bil_load_sw, mc.destc, mc.depdac, mc.deptmc
       ORDER BY c.llr_date, c.loadc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END pick_compl_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_PICKS_SP
  ||  Batch update of out quantities per order line.
  ||
  || ParmList: OrdNum~OrdLn~OutQty`OrdNum~OrdLn~OutQty
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/29/06 | rhalpai | Original PIR3593
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_picks_sp(
    i_div           IN      VARCHAR2,
    i_parm_list     IN      VARCHAR2,
    i_not_shp_rsn   IN      VARCHAR2,
    i_bill_err_rsn  IN      VARCHAR2,
    i_user_id       IN      VARCHAR2,
    o_msg           OUT     VARCHAR2
  ) IS
    l_c_module          CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.UPD_PICKS_SP';
    lar_parm                     logs.tar_parm;
    l_div_part                   NUMBER;
    l_t_grps                     type_stab;
    l_idx                        PLS_INTEGER;
    l_t_fields                   type_stab;
    l_ord_num                    NUMBER;
    l_ord_ln                     NUMBER;
    l_out_qty                    PLS_INTEGER;
    l_msg                        typ.t_maxvc2;
    l_msg_typ                    VARCHAR2(1);
    l_c_check_kit       CONSTANT BOOLEAN       := TRUE;
    l_c_no_auto_commit  CONSTANT BOOLEAN       := FALSE;
    l_e_udp_pick_err             EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'NotShpRsn', i_not_shp_rsn);
    logs.add_parm(lar_parm, 'BillErrRsn', i_bill_err_rsn);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Parse Groups of Parm Field Lists');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        logs.dbg('Parse Parm Field List');
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
        l_ord_num := l_t_fields(1);
        l_ord_ln := l_t_fields(2);
        l_out_qty := l_t_fields(3);
        l_msg := NULL;
        logs.dbg('Call Update Pick Process');
        upd_pick_sp(l_div_part,
                    l_ord_num,
                    l_ord_ln,
                    l_out_qty,
                    i_not_shp_rsn,
                    i_bill_err_rsn,
                    i_user_id,
                    l_msg,
                    l_msg_typ,
                    l_c_check_kit,
                    l_c_no_auto_commit
                   );

        IF l_msg_typ = op_const_pk.msg_typ_err THEN
          RAISE l_e_udp_pick_err;
        END IF;   -- l_msg_typ = op_const_pk.msg_typ_err

        IF l_msg IS NOT NULL THEN
          BEGIN
            o_msg := o_msg
                     ||(CASE
                          WHEN o_msg IS NULL THEN op_const_pk.msg_typ_info || op_const_pk.grp_delimiter
                          ELSE cnst.newline_char || op_const_pk.field_delimiter
                        END
                       )
                     || 'Order: '
                     || l_ord_num
                     || ' Line: '
                     || l_ord_ln
                     || ' - '
                     || l_msg;
          EXCEPTION
            WHEN VALUE_ERROR THEN
              NULL;
          END;
        END IF;   -- l_msg IS NOT NULL

        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;
      COMMIT;
    END IF;   -- l_t_grps IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_udp_pick_err THEN
      o_msg := op_const_pk.msg_typ_err
               || op_const_pk.grp_delimiter
               || 'Order: '
               || l_ord_num
               || ' Line: '
               || l_ord_ln
               || ' - '
               || l_msg;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_picks_sp;

  /*
  ||----------------------------------------------------------------------------
  || CNTNR_ADJUST_SP
  ||  Apply container adjustments.
  ||
  ||  Note: The adj_cntnr_id on bill_cntnr_id_bc1c will contain the same value
  ||        as the orig_cntnr_id when an adjustment is made to the qty.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/06 | rhalpai | Original PIR3209
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cntnr_adjust_sp(
    i_div            IN      VARCHAR2,
    i_load_num       IN      VARCHAR2,
    i_stop_num       IN      NUMBER,
    i_cust_id        IN      VARCHAR2,
    i_catlg_num      IN      VARCHAR2,
    i_from_cntnr_id  IN      VARCHAR2,
    i_to_cntnr_id    IN      VARCHAR2,
    i_move_qty       IN      NUMBER,
    i_out_qty        IN      NUMBER,
    i_not_shp_rsn    IN      VARCHAR2,
    i_bill_err_rsn   IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                           := 'OP_PICK_CONFIRM_PK.CNTNR_ADJUST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_to_cntnr_id        bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    l_c_out     CONSTANT VARCHAR2(3)                             := 'OUT';
    l_c_move    CONSTANT VARCHAR2(4)                             := 'MOVE';
    l_e_udp_pick_err     EXCEPTION;

    TYPE l_rt_ord IS RECORD(
      ord_num         NUMBER,
      ord_ln          NUMBER,
      llr_dt          NUMBER,
      depart_dt       NUMBER,
      mfst_catg       mclp210c.manctc%TYPE,
      tote_catg       mclp200b.totctb%TYPE,
      rlse_ts         DATE,
      box_sw          mclp200b.boxb%TYPE,
      alloc_qty       NUMBER,
      pick_qty        NUMBER,
      cntnr_qty       NUMBER,
      adj_cntnr_id    bill_cntnr_id_bc1c.adj_cntnr_id%TYPE,
      cntnr_orig_qty  NUMBER,
      cntnr_adj_qty   NUMBER
    );

    TYPE l_tt_ords IS TABLE OF l_rt_ord;

    FUNCTION out_qty_more_than_pick_fn(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_out_qty        IN  NUMBER
    )
      RETURN VARCHAR2 IS
      l_msg       typ.t_maxvc2;
      l_pick_qty  PLS_INTEGER  := 0;
    BEGIN
      IF i_out_qty > 0 THEN
        logs.dbg('Check Out-Qty > Total Pick Qty');

        OPEN l_cv
         FOR
           SELECT SUM(NVL(bc.adj_qty, bc.orig_qty)) AS pick_qty
             FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, bill_cntnr_id_bc1c bc
            WHERE ld.div_part = i_div_part
              AND ld.load_num = i_load_num
              AND se.div_part = ld.div_part
              AND se.load_depart_sid = ld.load_depart_sid
              AND se.cust_id = i_cust_id
              AND se.stop_num = i_stop_num
              AND a.div_part = se.div_part
              AND a.load_depart_sid = se.load_depart_sid
              AND a.custa = se.cust_id
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.orditb = i_catlg_num
              AND b.excptn_sw = 'N'
              AND b.statb = 'R'
              AND b.pckqtb > 0
              AND bc.div_part = b.div_part
              AND bc.ord_num = b.ordnob
              AND bc.ord_ln_num = b.lineb
              AND bc.orig_cntnr_id = i_from_cntnr_id;

        FETCH l_cv
         INTO l_pick_qty;

        IF i_out_qty > l_pick_qty THEN
          l_msg := 'Out-Qty (' || i_out_qty || ') cannot be greater than remaining pick qty (' || l_pick_qty || ').';
        END IF;   -- i_out_qty > l_pick_qty
      END IF;   -- i_out_qty > 0

      RETURN(l_msg);
    END out_qty_more_than_pick_fn;

    FUNCTION add_qty_more_than_alloc_fn(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_out_qty        IN  NUMBER
    )
      RETURN VARCHAR2 IS
      l_msg        typ.t_maxvc2;
      l_alloc_qty  PLS_INTEGER  := 0;
    BEGIN
      IF i_out_qty < 0 THEN
        logs.dbg('Check Reverse-Out > Alloc Qty');

        OPEN l_cv
         FOR
           SELECT SUM(b.alcqtb - b.pckqtb) AS alloc_qty
             FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
            WHERE ld.div_part = i_div_part
              AND ld.load_num = i_load_num
              AND se.div_part = ld.div_part
              AND se.load_depart_sid = ld.load_depart_sid
              AND se.cust_id = i_cust_id
              AND se.stop_num = i_stop_num
              AND a.div_part = se.div_part
              AND a.load_depart_sid = se.load_depart_sid
              AND a.custa = se.cust_id
              AND a.excptn_sw = 'N'
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.orditb = i_catlg_num
              AND b.excptn_sw = 'N'
              AND b.statb = 'R'
              AND b.alcqtb > 0
              AND EXISTS(SELECT 1
                           FROM bill_cntnr_id_bc1c bc
                          WHERE bc.div_part = b.div_part
                            AND bc.ord_num = b.ordnob
                            AND bc.ord_ln_num = b.lineb
                            AND bc.orig_cntnr_id = i_from_cntnr_id);

        FETCH l_cv
         INTO l_alloc_qty;

        IF ABS(i_out_qty) > l_alloc_qty THEN
          l_msg := 'Cannot use negative Out-Qty ('
                   || i_out_qty
                   || ') to increase qty greater than originally allocated ('
                   || l_alloc_qty
                   || ').';
        END IF;   -- ABS(i_out_qty) > l_alloc_qty
      END IF;   -- i_out_qty < 0

      RETURN(l_msg);
    END add_qty_more_than_alloc_fn;

    FUNCTION valid_out_qty_fn(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_out_qty        IN  NUMBER
    )
      RETURN VARCHAR2 IS
      l_msg  typ.t_maxvc2;
    BEGIN
      IF i_out_qty <> 0 THEN
        IF i_out_qty > 0 THEN
          l_msg := out_qty_more_than_pick_fn(i_div_part,
                                             i_load_num,
                                             i_stop_num,
                                             i_cust_id,
                                             i_catlg_num,
                                             i_from_cntnr_id,
                                             i_out_qty
                                            );
        ELSE
          l_msg := add_qty_more_than_alloc_fn(i_div_part,
                                              i_load_num,
                                              i_stop_num,
                                              i_cust_id,
                                              i_catlg_num,
                                              i_from_cntnr_id,
                                              i_out_qty
                                             );
        END IF;   -- i_out_qty > 0
      END IF;   -- i_out_qty <> 0

      RETURN(l_msg);
    END valid_out_qty_fn;

    FUNCTION move_more_than_cntnr_qty_fn(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_move_qty       IN  NUMBER,
      i_out_qty        IN  NUMBER
    )
      RETURN VARCHAR2 IS
      l_msg        typ.t_maxvc2;
      l_cntnr_qty  PLS_INTEGER  := 0;
    BEGIN
      IF i_move_qty > 0 THEN
        logs.dbg('Check Move-Qty > Remaining Container Qty');

        OPEN l_cv
         FOR
           SELECT SUM(NVL(bc.adj_qty, bc.orig_qty)) AS cntnr_qty
             FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, bill_cntnr_id_bc1c bc
            WHERE ld.div_part = i_div_part
              AND ld.load_num = i_load_num
              AND se.div_part = ld.div_part
              AND se.load_depart_sid = ld.load_depart_sid
              AND se.cust_id = i_cust_id
              AND se.stop_num = i_stop_num
              AND a.div_part = se.div_part
              AND a.load_depart_sid = se.load_depart_sid
              AND a.custa = se.cust_id
              AND a.excptn_sw = 'N'
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.orditb = i_catlg_num
              AND b.excptn_sw = 'N'
              AND b.statb = 'R'
              AND b.alcqtb > 0
              AND bc.div_part = b.div_part
              AND bc.ord_num = b.ordnob
              AND bc.ord_ln_num = b.lineb
              AND bc.orig_cntnr_id = i_from_cntnr_id;

        FETCH l_cv
         INTO l_cntnr_qty;

        IF i_move_qty >(l_cntnr_qty - NVL(i_out_qty, 0)) THEN
          l_msg := 'Move-Qty ('
                   || i_move_qty
                   || ') cannot be greater than remaining container qty ('
                   || l_cntnr_qty
                   || ') less Out-Qty ('
                   || NVL(i_out_qty, 0)
                   || ').';
        END IF;   -- i_move_qty >(l_cntnr_qty - NVL(i_out_qty, 0))
      END IF;   -- i_move_qty > 0

      RETURN(l_msg);
    END move_more_than_cntnr_qty_fn;

    FUNCTION valid_move_qty_fn(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_move_qty       IN  NUMBER,
      i_out_qty        IN  NUMBER
    )
      RETURN VARCHAR2 IS
      l_msg  typ.t_maxvc2;
    BEGIN
      IF i_move_qty <> 0 THEN
        IF i_move_qty > 0 THEN
          l_msg := move_more_than_cntnr_qty_fn(i_div_part,
                                               i_load_num,
                                               i_stop_num,
                                               i_cust_id,
                                               i_catlg_num,
                                               i_from_cntnr_id,
                                               i_move_qty,
                                               i_out_qty
                                              );
        ELSE
          l_msg := 'Move qty (' || i_move_qty || ') cannot be negative.';
        END IF;   -- i_move_qty > 0
      END IF;   -- i_move_qty <> 0

      RETURN(l_msg);
    END valid_move_qty_fn;

    FUNCTION to_cntnr_id_fn(
      i_div_part     IN  NUMBER,
      i_load_num     IN  VARCHAR2,
      i_stop_num     IN  NUMBER,
      i_cust_id      IN  VARCHAR2,
      i_to_cntnr_id  IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
      l_msg  typ.t_maxvc2;
    BEGIN
      IF i_move_qty > 0 THEN
        logs.dbg('Validate To-Container ID');

        IF TRIM(i_to_cntnr_id) IS NULL THEN
          l_msg := 'Container ID is required.';
        ELSE
          BEGIN
            SELECT   x.cntnr_id
                INTO l_to_cntnr_id
                FROM (SELECT bc.orig_cntnr_id AS cntnr_id
                        FROM bill_cntnr_id_bc1c bc
                       WHERE bc.div_part = i_div_part
                         AND bc.orig_cntnr_id LIKE '%' || TRIM(i_to_cntnr_id)
                         AND (bc.ord_num, bc.ord_ln_num) IN(SELECT b.ordnob, b.lineb
                                                              FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a,
                                                                   ordp120b b
                                                             WHERE ld.div_part = i_div_part
                                                               AND ld.load_num = i_load_num
                                                               AND se.div_part = ld.div_part
                                                               AND se.load_depart_sid = ld.load_depart_sid
                                                               AND se.cust_id = i_cust_id
                                                               AND se.stop_num = i_stop_num
                                                               AND a.div_part = se.div_part
                                                               AND a.load_depart_sid = se.load_depart_sid
                                                               AND a.custa = se.cust_id
                                                               AND a.excptn_sw = 'N'
                                                               AND b.div_part = a.div_part
                                                               AND b.ordnob = a.ordnoa
                                                               AND b.excptn_sw = 'N'
                                                               AND b.statb = 'R')
                      UNION ALL
                      SELECT ac.cntnr_id
                        FROM addl_cntnr_id_bc3c ac
                       WHERE ac.cntnr_id LIKE '%' || TRIM(i_to_cntnr_id)) x
            GROUP BY x.cntnr_id;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_msg := 'Move-To Container ID not found.';
            WHEN TOO_MANY_ROWS THEN
              l_msg := 'Partial Move-To Container ID (' || TRIM(i_to_cntnr_id) || ') not unique. Use last 7 digits.';
          END;
        END IF;   -- TRIM(i_from_cntnr_id) IS NULL
      END IF;   -- i_move_qty > 0

      RETURN(l_msg);
    END to_cntnr_id_fn;

    FUNCTION ords_fn(
      i_typ            IN  VARCHAR2,
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_out_qty        IN  NUMBER
    )
      RETURN l_tt_ords IS
      l_dist_sort  PLS_INTEGER;
      l_reg_sort   PLS_INTEGER;
      l_t_ords     l_tt_ords;
    BEGIN
      IF i_typ = l_c_out THEN
        IF i_out_qty > 0 THEN
          l_dist_sort := 1;
          l_reg_sort := 0;
        ELSE
          l_dist_sort := 0;
          l_reg_sort := 1;
        END IF;   -- i_out_qty > 0
      END IF;   -- i_typ = c_out

      -- when i_typ = c_move
      --   pick qty and container qty must be > 0
      --   order by container qty descending
      -- when i_typ = c_out
      --   order by pick qty descending for reg orders and ascending for dist orders
      --   if i_out_qty > 0
      --     order with reg orders first
      --   else
      --     order with dist orders first
      OPEN l_cv
       FOR
         SELECT        b.ordnob AS ord_num, b.lineb AS ord_ln, ld.llr_dt - DATE '1900-02-28' AS llr_dt,
                       TRUNC(ld.depart_ts) - DATE '1900-02-28' AS depart_dt, b.manctb AS mfst_catg,
                       RTRIM(b.totctb) AS tote_catg, TO_DATE(b.shpidb, 'YYYYMMDDHH24MISS') AS rlse_ts,
                       (SELECT cx.boxb
                          FROM mclp200b cx
                         WHERE cx.div_part = b.div_part
                           AND cx.totctb = b.totctb) AS box_sw, b.alcqtb AS alloc_qty, b.pckqtb AS pick_qty,
                       NVL(bc.adj_qty, bc.orig_qty) AS cntnr_qty, bc.adj_cntnr_id, bc.orig_qty, bc.adj_qty
                  FROM bill_cntnr_id_bc1c bc, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
                 WHERE bc.div_part = i_div_part
                   AND bc.orig_cntnr_id = i_from_cntnr_id
                   AND ld.div_part = bc.div_part
                   AND ld.load_num = i_load_num
                   AND se.div_part = ld.div_part
                   AND se.load_depart_sid = ld.load_depart_sid
                   AND se.cust_id = i_cust_id
                   AND se.stop_num = i_stop_num
                   AND a.div_part = se.div_part
                   AND a.load_depart_sid = se.load_depart_sid
                   AND a.custa = se.cust_id
                   AND b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.ordnob = bc.ord_num
                   AND b.lineb = bc.ord_ln_num
                   AND b.orditb = i_catlg_num
                   AND b.excptn_sw = 'N'
                   AND b.statb = 'R'
                   AND b.alcqtb > 0
                   AND (   i_typ = l_c_out
                        OR (    b.pckqtb > 0
                            AND NVL(bc.adj_qty, bc.orig_qty) > 0))
              ORDER BY DECODE(i_typ, l_c_out, DECODE(a.dsorda, 'D', l_dist_sort, l_reg_sort), cntnr_qty * -1),
                       DECODE(i_typ, l_c_out, DECODE(a.dsorda, 'D', b.pckqtb, b.pckqtb * -1))
         FOR UPDATE OF bc.adj_qty;

      FETCH l_cv
      BULK COLLECT INTO l_t_ords;

      RETURN(l_t_ords);
    END ords_fn;

    PROCEDURE upd_from_cntnr_sp(
      i_div_part       IN  NUMBER,
      i_from_cntnr_id  IN  VARCHAR2,
      i_ord_num        IN  NUMBER,
      i_ord_ln         IN  NUMBER,
      i_adj_qty        IN  NUMBER
    ) IS
    BEGIN
      -- If adj_qty = orig_qty when reversing an out or move then set
      -- adj_cntnr_id and adj_qty to NULLs to indicate no change
      -- from original allocation.
      UPDATE bill_cntnr_id_bc1c bc
         SET bc.adj_cntnr_id = DECODE(i_adj_qty, bc.orig_qty, NULL, bc.orig_cntnr_id),
             bc.adj_qty = DECODE(i_adj_qty, bc.orig_qty, NULL, i_adj_qty)
       WHERE bc.div_part = i_div_part
         AND bc.ord_num = i_ord_num
         AND bc.ord_ln_num = i_ord_ln
         AND bc.orig_cntnr_id = i_from_cntnr_id;
    END upd_from_cntnr_sp;

    FUNCTION total_cntnr_qty_fn(
      i_div_part  IN  NUMBER,
      i_cntnr_id  IN  VARCHAR2
    )
      RETURN NUMBER IS
      l_ttl_cntnr_qty  PLS_INTEGER;
    BEGIN
      SELECT NVL(SUM(NVL(bc.adj_qty, bc.orig_qty)), 0) AS cntnr_qty
        INTO l_ttl_cntnr_qty
        FROM bill_cntnr_id_bc1c bc
       WHERE bc.div_part = i_div_part
         AND bc.orig_cntnr_id = i_cntnr_id;

      RETURN(l_ttl_cntnr_qty);
    END total_cntnr_qty_fn;

    PROCEDURE del_tote_box_sp(
      i_div_part   IN  NUMBER,
      i_load_num   IN  VARCHAR2,
      i_stop_num   IN  NUMBER,
      i_cust_id    IN  VARCHAR2,
      i_box_sw     IN  VARCHAR2,
      i_llr_dt     IN  NUMBER,
      i_depart_dt  IN  NUMBER,
      i_mfst_catg  IN  VARCHAR2,
      i_tote_catg  IN  VARCHAR2
    ) IS
    BEGIN
      -- It's possible this was an additional container that was
      -- originally added to the mclp370c by an order line from
      -- another release so handle this by selecting the max rowid
      -- from multiple releases with tote/box count > zero.
      UPDATE mclp370c mc
         SET mc.totsmc = DECODE(i_box_sw, 'Y', mc.totsmc, mc.totsmc - 1),
             mc.boxsmc = DECODE(i_box_sw, 'Y', mc.boxsmc - 1, mc.boxsmc)
       WHERE mc.ROWID = (SELECT MAX(mc2.ROWID)
                           FROM mclp370c mc2
                          WHERE mc2.div_part = i_div_part
                            AND mc2.llr_date = i_llr_dt
                            AND mc2.loadc = i_load_num
                            AND mc2.stopc = i_stop_num
                            AND mc2.custc = i_cust_id
                            AND mc2.depdtc = i_depart_dt
                            AND mc2.manctc = i_mfst_catg
                            AND mc2.totctc = i_tote_catg
                            AND DECODE(i_box_sw, 'Y', mc2.boxsmc, mc2.totsmc) > 0);
    END del_tote_box_sp;

    PROCEDURE add_tote_box_sp(
      i_div_part   IN  NUMBER,
      i_load_num   IN  VARCHAR2,
      i_stop_num   IN  NUMBER,
      i_cust_id    IN  VARCHAR2,
      i_box_sw     IN  VARCHAR2,
      i_llr_dt     IN  NUMBER,
      i_depart_dt  IN  NUMBER,
      i_mfst_catg  IN  VARCHAR2,
      i_tote_catg  IN  VARCHAR2,
      i_rlse_ts    IN  DATE
    ) IS
    BEGIN
      UPDATE mclp370c mc
         SET mc.totsmc = DECODE(i_box_sw, 'Y', mc.totsmc, mc.totsmc + 1),
             mc.boxsmc = DECODE(i_box_sw, 'Y', mc.boxsmc + 1, mc.boxsmc)
       WHERE mc.div_part = i_div_part
         AND mc.llr_date = i_llr_dt
         AND mc.loadc = i_load_num
         AND mc.stopc = i_stop_num
         AND mc.custc = i_cust_id
         AND mc.depdtc = i_depart_dt
         AND mc.manctc = i_mfst_catg
         AND mc.totctc = i_tote_catg
         AND mc.release_ts = i_rlse_ts;
    END add_tote_box_sp;

    PROCEDURE apply_outs_sp(
      i_div_part       IN      NUMBER,
      i_load_num       IN      VARCHAR2,
      i_stop_num       IN      NUMBER,
      i_cust_id        IN      VARCHAR2,
      i_catlg_num      IN      VARCHAR2,
      i_from_cntnr_id  IN      VARCHAR2,
      i_out_qty        IN      NUMBER,
      i_not_shp_rsn    IN      VARCHAR2,
      i_bill_err_rsn   IN      VARCHAR2,
      i_user_id        IN      VARCHAR2,
      o_msg            OUT     VARCHAR2
    ) IS
      l_t_ords                     l_tt_ords;
      l_idx                        PLS_INTEGER;
      l_remain_out_qty             PLS_INTEGER;
      l_r_ord                      l_rt_ord;
      l_wrk_out_qty                PLS_INTEGER;
      l_msg                        typ.t_maxvc2;
      l_msg_typ                    VARCHAR2(1);
      l_c_check_kit       CONSTANT BOOLEAN      := TRUE;
      l_c_no_auto_commit  CONSTANT BOOLEAN      := FALSE;
    BEGIN
      IF i_out_qty <> 0 THEN
        logs.dbg('Get Table of Order Lines for Pick-Outs');
        l_t_ords := ords_fn(l_c_out,
                            i_div_part,
                            i_load_num,
                            i_stop_num,
                            i_cust_id,
                            i_catlg_num,
                            i_from_cntnr_id,
                            i_out_qty
                           );

        IF l_t_ords IS NOT NULL THEN
          l_remain_out_qty := i_out_qty;
          l_idx := l_t_ords.FIRST;
          WHILE(    l_idx IS NOT NULL
                AND l_remain_out_qty <> 0) LOOP
            l_r_ord := l_t_ords(l_idx);

            IF i_out_qty > 0 THEN
              -- Outting (subtracting qty)
              l_wrk_out_qty := LEAST(l_remain_out_qty, l_r_ord.cntnr_qty);
            ELSE
              -- Reversing Out (adding qty via negative out qty)
              l_wrk_out_qty := GREATEST(l_remain_out_qty,
                                        (l_r_ord.cntnr_orig_qty - NVL(l_r_ord.cntnr_adj_qty, 0)) * -1,
                                        (l_r_ord.alloc_qty - l_r_ord.pick_qty
                                        ) * -1
                                       );
            END IF;   -- i_out_qty > 0

            IF l_wrk_out_qty <> 0 THEN
              l_remain_out_qty := l_remain_out_qty - l_wrk_out_qty;
              logs.dbg('Call UPD_PICK_SP');
              upd_pick_sp(i_div_part,
                          l_r_ord.ord_num,
                          l_r_ord.ord_ln,
                          l_wrk_out_qty,
                          i_not_shp_rsn,
                          i_bill_err_rsn,
                          i_user_id,
                          l_msg,
                          l_msg_typ,
                          l_c_check_kit,
                          l_c_no_auto_commit
                         );

              IF l_msg_typ = op_const_pk.msg_typ_err THEN
                o_msg := l_msg;
                RAISE l_e_udp_pick_err;
              END IF;   -- l_msg_typ = op_const_pk.msg_typ_err

              BEGIN
                o_msg := o_msg ||(CASE
                                    WHEN o_msg IS NOT NULL THEN cnst.newline_char
                                  END) || l_msg;
              EXCEPTION
                WHEN VALUE_ERROR THEN
                  NULL;
              END;

              logs.dbg('Update From-Container for Pick-Out');
              upd_from_cntnr_sp(i_div_part,
                                i_from_cntnr_id,
                                l_r_ord.ord_num,
                                l_r_ord.ord_ln,
                                l_r_ord.cntnr_qty - l_wrk_out_qty
                               );
            END IF;   -- l_wrk_out_qty <> 0

            -- Update mclp370c if container is now empty or was empty but is now
            -- being used again due to a reversal of outs.
            IF l_r_ord.tote_catg IS NOT NULL THEN
              logs.dbg('Check From-Container Total After Pick-Out');

              IF (    l_wrk_out_qty > 0
                  AND total_cntnr_qty_fn(i_div_part, i_from_cntnr_id) = 0) THEN
                -- Out was processed and now total container qty = zero
                -- so decrease mclp370c.
                logs.dbg('Decrease MCLP370C for Pick-Out of Last Item');
                del_tote_box_sp(i_div_part,
                                i_load_num,
                                i_stop_num,
                                i_cust_id,
                                l_r_ord.box_sw,
                                l_r_ord.llr_dt,
                                l_r_ord.depart_dt,
                                l_r_ord.mfst_catg,
                                l_r_ord.tote_catg
                               );
              ELSIF(    l_wrk_out_qty < 0
                    AND total_cntnr_qty_fn(i_div_part, i_from_cntnr_id) = ABS(l_wrk_out_qty)) THEN
                -- Out was reversed and now total container qty = qty reversed
                -- indicates total container qty was zero prior to reversal
                -- so increase mclp370c.
                logs.dbg('Increase MCLP370C for Reverse-Out - First Item in Container');
                add_tote_box_sp(i_div_part,
                                i_load_num,
                                i_stop_num,
                                i_cust_id,
                                l_r_ord.box_sw,
                                l_r_ord.llr_dt,
                                l_r_ord.depart_dt,
                                l_r_ord.mfst_catg,
                                l_r_ord.tote_catg,
                                l_r_ord.rlse_ts
                               );
              END IF;   -- l_wrk_out_qty > 0 AND total_cntnr_qty_fn(i_div_part, i_from_cntnr_id) = 0
            END IF;   -- l_r_ord.tote_catg IS NOT NULL

            l_idx := l_t_ords.NEXT(l_idx);
          END LOOP;
        END IF;   -- l_t_ords IS NOT NULL
      END IF;   -- i_out_qty <> 0
    END apply_outs_sp;

    PROCEDURE apply_moves_sp(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_stop_num       IN  NUMBER,
      i_cust_id        IN  VARCHAR2,
      i_catlg_num      IN  VARCHAR2,
      i_from_cntnr_id  IN  VARCHAR2,
      i_move_qty       IN  NUMBER,
      i_out_qty        IN  NUMBER
    ) IS
      l_c_sysdate  CONSTANT DATE        := SYSDATE;
      l_t_ords              l_tt_ords;
      l_idx                 PLS_INTEGER;
      l_remain_move_qty     PLS_INTEGER;
      l_r_ord               l_rt_ord;
      l_wrk_move_qty        PLS_INTEGER;
    BEGIN
      IF i_move_qty > 0 THEN
        logs.dbg('Get Table for Order Lines for Move');
        l_t_ords := ords_fn(l_c_move,
                            i_div_part,
                            i_load_num,
                            i_stop_num,
                            i_cust_id,
                            i_catlg_num,
                            i_from_cntnr_id,
                            i_out_qty
                           );

        IF l_t_ords IS NOT NULL THEN
          l_remain_move_qty := i_move_qty;
          l_idx := l_t_ords.FIRST;
          WHILE(    l_idx IS NOT NULL
                AND l_remain_move_qty <> 0) LOOP
            l_r_ord := l_t_ords(l_idx);
            l_wrk_move_qty := LEAST(l_remain_move_qty, l_r_ord.cntnr_qty);
            l_remain_move_qty := l_remain_move_qty - l_wrk_move_qty;

            IF l_wrk_move_qty < l_r_ord.cntnr_qty THEN
              -- There's enough container qty for this order line to
              -- satisfy the move.
              logs.dbg('Update From-Container for Move');
              upd_from_cntnr_sp(i_div_part,
                                i_from_cntnr_id,
                                l_r_ord.ord_num,
                                l_r_ord.ord_ln,
                                l_r_ord.cntnr_qty - l_wrk_move_qty
                               );
            ELSE
              IF l_r_ord.cntnr_orig_qty = 0 THEN
                -- Orig Qty = Zero indicates an additional entry due to
                -- a previous move.
                logs.dbg('Remove Order/Line Entry for From-Container');

                DELETE FROM bill_cntnr_id_bc1c bc
                      WHERE bc.div_part = i_div_part
                        AND bc.ord_num = l_r_ord.ord_num
                        AND bc.ord_ln_num = l_r_ord.ord_ln
                        AND bc.orig_cntnr_id = i_from_cntnr_id;

                IF SUBSTR(i_from_cntnr_id, 3, 1) = 4 THEN
                  logs.dbg('Return Manually-Added Container to Pool');

                  -- Manually-added container to be returned to pool of available containers
                  INSERT INTO addl_cntnr_id_bc3c
                              (cntnr_id, create_ts
                              )
                       VALUES (i_from_cntnr_id, l_c_sysdate
                              );
                END IF;   -- SUBSTR(i_from_cntnr_id, 3, 1) = 4
              ELSE
                logs.dbg('Update From-Container to Zero for Move');
                upd_from_cntnr_sp(i_div_part, i_from_cntnr_id, l_r_ord.ord_num, l_r_ord.ord_ln, 0);
              END IF;   -- l_r_ord.cntnr_orig_qty = 0
            END IF;   -- l_wrk_move_qty < l_r_ord.cntnr_qty

            logs.dbg('Merge for To-Container Move');
            MERGE INTO bill_cntnr_id_bc1c bc
                 USING (SELECT 1 tst
                          FROM DUAL) x
                    ON (bc.div_part = i_div_part
                    AND bc.ord_num = l_r_ord.ord_num
                    AND bc.ord_ln_num = l_r_ord.ord_ln
                    AND bc.orig_cntnr_id = l_to_cntnr_id
                    AND x.tst > 0)
              WHEN MATCHED THEN
                UPDATE
                   SET bc.adj_cntnr_id = DECODE(NVL(bc.adj_qty, bc.orig_qty) + l_wrk_move_qty,
                                                bc.orig_qty, NULL,
                                                bc.orig_cntnr_id
                                               ),
                       bc.adj_qty = DECODE(NVL(bc.adj_qty, bc.orig_qty) + l_wrk_move_qty,
                                           bc.orig_qty, NULL,
                                           NVL(bc.adj_qty, bc.orig_qty) + l_wrk_move_qty
                                          )
              WHEN NOT MATCHED THEN
                INSERT(div_part, ord_num, ord_ln_num, orig_cntnr_id, orig_qty, adj_cntnr_id, adj_qty)
                VALUES(i_div_part, l_r_ord.ord_num, l_r_ord.ord_ln, l_to_cntnr_id, 0, l_to_cntnr_id, l_wrk_move_qty);

            -- Update mclp370c if (the from-container is now empty or not found)
            -- OR (the to-container was empty but is now being used again due
            -- to movement into the container or a new container was used
            -- from the addl_cntnr_id_bc3c).
            IF l_r_ord.tote_catg IS NOT NULL THEN
              logs.dbg('Check From-Container Qty After Move');

              IF total_cntnr_qty_fn(i_div_part, i_from_cntnr_id) = 0 THEN
                -- Move was processed and now total container qty = zero
                -- so decrease mclp370c.
                logs.dbg('Decrease MCLP370C for Move of Last Item');
                del_tote_box_sp(i_div_part,
                                i_load_num,
                                i_stop_num,
                                i_cust_id,
                                l_r_ord.box_sw,
                                l_r_ord.llr_dt,
                                l_r_ord.depart_dt,
                                l_r_ord.mfst_catg,
                                l_r_ord.tote_catg
                               );
              END IF;   -- total_cntnr_qty_fn(i_div_part, i_from_cntnr_id) = 0

              logs.dbg('Check To-Container Qty After Move');

              IF total_cntnr_qty_fn(i_div_part, l_to_cntnr_id) = l_wrk_move_qty THEN
                -- Move was processed to a previously empty container
                -- so increase mclp370c.
                logs.dbg('Increase MCLP370C for Move to New Container');
                add_tote_box_sp(i_div_part,
                                i_load_num,
                                i_stop_num,
                                i_cust_id,
                                l_r_ord.box_sw,
                                l_r_ord.llr_dt,
                                l_r_ord.depart_dt,
                                l_r_ord.mfst_catg,
                                l_r_ord.tote_catg,
                                l_r_ord.rlse_ts
                               );
              END IF;   -- total_cntnr_qty_fn(i_div_part, l_to_cntnr_id) = l_wrk_move_qty
            END IF;   -- l_r_ord.tote_catg IS NOT NULL

            IF SUBSTR(l_to_cntnr_id, 3, 1) = '4' THEN
              logs.dbg('Remove Manually-Added Container from Pool');

              DELETE FROM addl_cntnr_id_bc3c
                    WHERE cntnr_id = l_to_cntnr_id;
            END IF;   -- SUBSTR(l_to_cntnr_id, 3, 1) = '4'

            l_idx := l_t_ords.NEXT(l_idx);
          END LOOP;
        END IF;   -- l_t_ords IS NOT NULL
      END IF;   -- i_move_qty > 0
    END apply_moves_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'FromCntnrID', i_from_cntnr_id);
    logs.add_parm(lar_parm, 'ToCntnrID', i_to_cntnr_id);
    logs.add_parm(lar_parm, 'MoveQty', i_move_qty);
    logs.add_parm(lar_parm, 'OutQty', i_out_qty);
    logs.add_parm(lar_parm, 'NotShpRsn', i_not_shp_rsn);
    logs.add_parm(lar_parm, 'BillErrRsn', i_bill_err_rsn);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Validate');
    o_msg := COALESCE(valid_out_qty_fn(l_div_part,
                                       i_load_num,
                                       i_stop_num,
                                       i_cust_id,
                                       i_catlg_num,
                                       i_from_cntnr_id,
                                       i_out_qty
                                      ),
                      valid_move_qty_fn(l_div_part,
                                        i_load_num,
                                        i_stop_num,
                                        i_cust_id,
                                        i_catlg_num,
                                        i_from_cntnr_id,
                                        i_move_qty,
                                        i_out_qty
                                       ),
                      to_cntnr_id_fn(l_div_part, i_load_num, i_stop_num, i_cust_id, i_to_cntnr_id)
                     );

    IF o_msg IS NULL THEN
      logs.dbg('Apply Outs');
      apply_outs_sp(l_div_part,
                    i_load_num,
                    i_stop_num,
                    i_cust_id,
                    i_catlg_num,
                    i_from_cntnr_id,
                    i_out_qty,
                    i_not_shp_rsn,
                    i_bill_err_rsn,
                    i_user_id,
                    o_msg
                   );
      logs.dbg('Apply Moves');
      apply_moves_sp(l_div_part, i_load_num, i_stop_num, i_cust_id, i_catlg_num, i_from_cntnr_id, i_move_qty, i_out_qty);
      COMMIT;
    END IF;   -- o_msg IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_udp_pick_err THEN
      ROLLBACK;   -- return error msg already stored in o_msg
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cntnr_adjust_sp;

  /*
  ||----------------------------------------------------------------------------
  || PICK_COMPL_SP
  ||  For each LLR/Load passed, set to Pick-Completed and create XDock reports
  ||  and ftp to mainframe.
  ||
  ||  ParmList Format:
  ||  LLRDt~Load`LLRDt~Load`LLRDt~Load
  ||  (LLRDt in YYYY-MM-DD format)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/09 | rhalpai | Original PIR7342
  || 12/02/09 | rhalpai | Converted to wrapper for processing Pick Completes
  ||                    | for Test-Bills and then for Production Runs. PIR7342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE pick_compl_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2,
    i_alloc_sw   IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.PICK_COMPL_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'AllocSw', i_alloc_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_pick_compl,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.dbg('Process Pick Completed for Test-Bill');
    prcs_pick_compl_sp(i_div, i_parm_list, 'Y');
    COMMIT;
    logs.dbg('Process Pick Completed for Production Run');
    prcs_pick_compl_sp(i_div, i_parm_list, 'N');
    COMMIT;
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_pick_compl,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);

      IF i_alloc_sw = 'Y' THEN
        pick_compl_sp(i_div, i_parm_list, i_alloc_sw);
      ELSE
        RAISE;
      END IF;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_pick_compl,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END pick_compl_sp;

  /*
  ||----------------------------------------------------------------------------
  || RLSE_LIST_FN
  ||  Return ReleaseTimestamp list
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION rlse_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  DATE
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.RLSE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   b.shpidb AS rlse_ts
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, ordp120b b
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = i_llr_dt
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.stata = 'R'
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.alcqtb > 0
            AND b.subrcb < 999
       GROUP BY b.shpidb
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rlse_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || NOT_SHP_RSN_LIST_FN
  ||  Return cursor of NotShipReason code and description
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION not_shp_rsn_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.NOT_SHP_RSN_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   a.rsncda AS rsn_cd, a.desca AS descr
           FROM mclp140a a
          WHERE a.rsncda IN('120', '121', '122')
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END not_shp_rsn_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_LIST_FN
  ||  Return cursor of Load and destination
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div      IN  VARCHAR2,
    i_llr_dt   IN  DATE,
    i_rlse_ts  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   ld.load_num, l.destc AS dest
           FROM div_mstr_di1d d, load_depart_op1f ld, mclp120c l
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = i_llr_dt
            AND EXISTS(SELECT 1
                         FROM ordp100a a, ordp120b b
                        WHERE a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.stata = 'R'
                          AND b.div_part = a.div_part
                          AND b.ordnob = a.ordnoa
                          AND b.shpidb = i_rlse_ts
                          AND b.alcqtb > 0
                          AND b.subrcb < 999)
            AND l.div_part = ld.div_part
            AND l.loadc = ld.load_num
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
  || STOP_LIST_FN
  ||  Return cursor of StopNum
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  DATE,
    i_rlse_ts   IN  VARCHAR2,
    i_load_num  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.STOP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   se.stop_num
           FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = i_llr_dt
            AND ld.load_num = i_load_num
            AND se.div_part = ld.div_part
            AND se.load_depart_sid = ld.load_depart_sid
            AND EXISTS(SELECT 1
                         FROM ordp100a a, ordp120b b
                        WHERE a.div_part = se.div_part
                          AND a.load_depart_sid = se.load_depart_sid
                          AND a.custa = se.cust_id
                          AND a.stata = 'R'
                          AND b.div_part = a.div_part
                          AND b.ordnob = a.ordnoa
                          AND b.shpidb = i_rlse_ts
                          AND b.alcqtb > 0
                          AND b.subrcb < 999)
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || MFST_LIST_FN
  ||  Return cursor of ManifestCategory code and description
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION mfst_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.MFST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   m.manctc AS mfst_catg, m.manctc || ' ' || m.descc AS descr
           FROM div_mstr_di1d d, mclp210c m
          WHERE d.div_id = i_div
            AND m.div_part = d.div_part
            AND m.manctc IN(SELECT b.manctb
                              FROM div_mstr_di1d d, ordp120b b, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
                             WHERE d.div_id = i_div
                               AND b.div_part = d.div_part
                               AND b.shpidb = i_rlse_ts
                               AND b.alcqtb > 0
                               AND b.subrcb < 999
                               AND a.div_part = b.div_part
                               AND a.ordnoa = b.ordnob
                               AND a.stata = 'R'
                               AND ld.div_part = a.div_part
                               AND ld.load_depart_sid = a.load_depart_sid
                               AND ld.llr_dt = i_llr_dt
                               AND (   i_load_list = 'ALL'
                                    OR ld.load_num IN(SELECT t.column_value
                                                        FROM TABLE(str.parse_list(i_load_list,
                                                                                  op_const_pk.field_delimiter
                                                                                 )
                                                                  ) t)
                                   )
                               AND se.div_part = a.div_part
                               AND se.load_depart_sid = a.load_depart_sid
                               AND se.cust_id = a.custa
                               AND (   i_stop_list = 'ALL'
                                    OR se.stop_num IN(SELECT t.column_value
                                                        FROM TABLE(str.parse_list(i_stop_list,
                                                                                  op_const_pk.field_delimiter
                                                                                 )
                                                                  ) t)
                                   ))
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mfst_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || TOTE_LIST_FN
  ||  Return cursor of ToteCategory code and description
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION tote_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.TOTE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   t.totctb AS tote_categ, t.totctb || ' ' || t.descb AS descr
           FROM div_mstr_di1d d,  mclp200b t
          WHERE d.div_id = i_div
            AND t.div_part = d.div_part
            AND t.totctb IN(SELECT b.totctb
                              FROM div_mstr_di1d d, ordp120b b, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
                             WHERE d.div_id = i_div
                               AND b.div_part = d.div_part
                               AND b.shpidb = i_rlse_ts
                               AND b.alcqtb > 0
                               AND b.subrcb < 999
                               AND a.div_part = b.div_part
                               AND a.ordnoa = b.ordnob
                               AND a.stata = 'R'
                               AND ld.div_part = a.div_part
                               AND ld.load_depart_sid = a.load_depart_sid
                               AND ld.llr_dt = i_llr_dt
                               AND (   i_load_list = 'ALL'
                                    OR ld.load_num IN(SELECT t.column_value
                                                        FROM TABLE(str.parse_list(i_load_list,
                                                                                  op_const_pk.field_delimiter
                                                                                 )
                                                                  ) t)
                                   )
                               AND se.div_part = a.div_part
                               AND se.load_depart_sid = a.load_depart_sid
                               AND se.cust_id = a.custa
                               AND (   i_stop_list = 'ALL'
                                    OR se.stop_num IN(SELECT t.column_value
                                                        FROM TABLE(str.parse_list(i_stop_list,
                                                                                  op_const_pk.field_delimiter
                                                                                 )
                                                                  ) t)
                                   ))
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tote_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_LIST_FN
  ||  Return cursor of Catalog ItemNum and description
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_list_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.ITEM_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   e.catite AS catlg_num, RPAD(e.ctdsce, 30) || ' | ' || LPAD(e.shppke, 4) || ' | ' || e.sizee AS descr
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, sawp505e e
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = i_llr_dt
            AND (   i_load_list = 'ALL'
                 OR ld.load_num IN(SELECT t.column_value
                                     FROM TABLE(str.parse_list(i_load_list, op_const_pk.field_delimiter)) t)
                )
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.stata = 'R'
            AND se.div_part = a.div_part
            AND se.load_depart_sid = a.load_depart_sid
            AND se.cust_id = a.custa
            AND (   i_stop_list = 'ALL'
                 OR se.stop_num IN(SELECT t.column_value
                                     FROM TABLE(str.parse_list(i_stop_list, op_const_pk.field_delimiter)) t)
                )
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.shpidb = i_rlse_ts
            AND b.alcqtb > 0
            AND b.subrcb < 999
            AND e.catite = b.orditb
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || MASS_PICK_ADJ_SUM_FN
  ||  Return cursor of MassPickAdjustment Summary
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  FUNCTION mass_pick_adj_sum_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  DATE,
    i_rlse_ts    IN  VARCHAR2,
    i_load_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_mfst_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_tote_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_item_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.MASS_PICK_ADJ_SUM_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'ToteList', i_tote_list);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT i_llr_dt AS llr_dt, i_rlse_ts AS rlse_ts, i_load_list AS load_list, i_stop_list AS stop_list,
              i_mfst_list AS mfst_list, i_tote_list AS tote_list, i_item_list AS item_list, COUNT(*) AS cnt
         FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b
        WHERE d.div_id = i_div
          AND ld.div_part = d.div_part
          AND ld.llr_dt = i_llr_dt
          AND (   i_load_list = 'ALL'
               OR ld.load_num IN(SELECT t.column_value
                                   FROM TABLE(str.parse_list(i_load_list, op_const_pk.field_delimiter)) t)
              )
          AND a.div_part = ld.div_part
          AND a.load_depart_sid = ld.load_depart_sid
          AND a.stata = 'R'
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa
          AND (   i_stop_list = 'ALL'
               OR se.stop_num IN(SELECT t.column_value
                                   FROM TABLE(str.parse_list(i_stop_list, op_const_pk.field_delimiter)) t)
              )
          AND b.div_part = a.div_part
          AND b.ordnob = a.ordnoa
          AND b.shpidb = i_rlse_ts
          AND (   i_mfst_list = 'ALL'
               OR b.manctb IN(SELECT t.column_value
                                FROM TABLE(str.parse_list(i_mfst_list, op_const_pk.field_delimiter)) t)
              )
          AND (   i_tote_list = 'ALL'
               OR b.totctb IN(SELECT t.column_value
                                FROM TABLE(str.parse_list(i_tote_list, op_const_pk.field_delimiter)) t)
              )
          AND (   i_item_list = 'ALL'
               OR b.orditb IN(SELECT t.column_value
                                FROM TABLE(str.parse_list(i_item_list, op_const_pk.field_delimiter)) t)
              )
          AND b.alcqtb > 0
          AND b.subrcb < 999;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mass_pick_adj_sum_fn;

  /*
  ||----------------------------------------------------------------------------
  || MASS_PICK_ADJ_SP
  ||  Mass update of pick adjustments
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/21 | rhalpai | Original for PIR20766
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mass_pick_adj_sp(
    i_div           IN  VARCHAR2,
    i_llr_dt        IN  DATE,
    i_rlse_ts       IN  VARCHAR2,
    i_user_id       IN  VARCHAR2,
    i_not_shp_rsn   IN  VARCHAR2,
    i_bill_err_rsn  IN  VARCHAR2 DEFAULT NULL,
    i_load_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_stop_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_mfst_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_tote_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_item_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_evnt_que_id   IN  NUMBER DEFAULT NULL,
    i_cycl_id       IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id   IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PICK_CONFIRM_PK.MASS_PICK_ADJ_SP';
    lar_parm             logs.tar_parm;
    l_valid_sw           VARCHAR2(1);
    l_t_load             type_stab;
    l_t_stop             type_stab;
    l_t_mfst             type_stab;
    l_t_tote             type_stab;
    l_t_item             type_stab;
    l_t_parm             type_stab;
    l_parts              PLS_INTEGER;
    l_t_parm_list        type_stab;
    l_msg                typ.t_maxvc2;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;

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
    logs.add_parm(lar_parm, 'RlseTs', i_rlse_ts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'NotShpRsn', i_not_shp_rsn);
    logs.add_parm(lar_parm, 'BillErrRsn', i_bill_err_rsn);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'StopList', i_stop_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'ToteList', i_tote_list);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Div is required');
    excp.assert((i_llr_dt IS NOT NULL), 'LLRDt is required');
    excp.assert((i_rlse_ts IS NOT NULL), 'RlseTs is required');

    SELECT DECODE(validate_conversion(i_rlse_ts AS DATE, 'YYYYMMDDHH24MISS'), 1, 'Y', 'N')
      INTO l_valid_sw
      FROM DUAL;

    excp.assert((l_valid_sw = 'Y'), 'RlseTs [' || i_rlse_ts || '] must be a valid date in YYYYMMDDHH24MISS format');
    excp.assert((i_user_id IS NOT NULL), 'UserId is required');
    excp.assert((i_not_shp_rsn IS NOT NULL), 'NotShpRsn is required');
    excp.assert((i_not_shp_rsn IN('120', '121', '122')), 'Invalid NotShpRsn');

    IF i_not_shp_rsn = '122' THEN
      excp.assert((i_bill_err_rsn IS NOT NULL), 'BillErrRsn is required');
    END IF;   -- i_not_shp_rsn = '122'

    IF i_evnt_que_id IS NULL THEN
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
                      || i_rlse_ts
                      || '</value></row>'
                      || '<row><sequence>'
                      || 4
                      || '</sequence><value>'
                      || i_user_id
                      || '</value></row>'
                      || '<row><sequence>'
                      || 5
                      || '</sequence><value>'
                      || i_not_shp_rsn
                      || '</value></row>'
                      || '<row><sequence>'
                      || 6
                      || '</sequence><value>'
                      || i_bill_err_rsn
                      || '</value></row>'
                      || '<row><sequence>'
                      || 7
                      || '</sequence><value>'
                      || i_load_list
                      || '</value></row>'
                      || '<row><sequence>'
                      || 8
                      || '</sequence><value>'
                      || i_stop_list
                      || '</value></row>'
                      || '<row><sequence>'
                      || 9
                      || '</sequence><value>'
                      || i_mfst_list
                      || '</value></row>'
                      || '<row><sequence>'
                      || 10
                      || '</sequence><value>'
                      || i_tote_list
                      || '</value></row>'
                      || '<row><sequence>'
                      || 11
                      || '</sequence><value>'
                      || i_item_list
                      || '</value></row>'
                      || '</parameters>';
      logs.dbg('Create Event');
      cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                       i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                       i_event_dfn_id         => cig_constants_events_pk.evd_op_mass_pick_adj,
                                       i_parameters           => l_evnt_parms,
                                       i_div_nm               => i_div,
                                       i_is_script_fw_exec    => 'Y',
                                       i_is_complete          => 'Y',
                                       i_pgm_id               => 'PLSQL',
                                       i_user_id              => i_user_id,
                                       o_event_que_id         => l_evnt_que_id
                                      );
    ELSE
      IF i_load_list <> 'ALL' THEN
        l_t_load := str.parse_list(i_load_list, op_const_pk.field_delimiter);
      END IF;   -- i_load_list <> 'ALL'

      IF i_stop_list <> 'ALL' THEN
        excp.assert((l_t_load.COUNT = 1), 'StopList not allowed with multiple Loads');
        l_t_stop := str.parse_list(i_stop_list, op_const_pk.field_delimiter);
      END IF;   -- i_stop_list <> 'ALL'

      IF i_mfst_list <> 'ALL' THEN
        excp.assert((    i_tote_list = 'ALL'
                     AND i_item_list = 'ALL'), 'MfstList not allowed with ToteList or ItemList');
        l_t_mfst := str.parse_list(i_mfst_list, op_const_pk.field_delimiter);
      END IF;   -- i_stop_list <> 'ALL'

      IF i_tote_list <> 'ALL' THEN
        excp.assert((    i_mfst_list = 'ALL'
                     AND i_item_list = 'ALL'), 'ToteList not allowed with MfstList or ItemList');
        l_t_tote := str.parse_list(i_tote_list, op_const_pk.field_delimiter);
      END IF;   -- i_tote_list <> 'ALL'

      IF i_item_list <> 'ALL' THEN
        excp.assert((    i_mfst_list = 'ALL'
                     AND i_tote_list = 'ALL'), 'ItemList not allowed with MfstList or ToteList');
        l_t_item := str.parse_list(i_item_list, op_const_pk.field_delimiter);
      END IF;   -- i_item_list <> 'ALL'

      logs.dbg('Get OrdLnList');

      SELECT b.ordnob || '~' || b.lineb || '~' || b.pckqtb
      BULK COLLECT INTO l_t_parm
        FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b
       WHERE d.div_id = i_div
         AND ld.div_part = d.div_part
         AND ld.llr_dt = i_llr_dt
         AND (   i_load_list = 'ALL'
              OR ld.load_num MEMBER OF l_t_load)
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.stata = 'R'
         AND se.div_part = a.div_part
         AND se.load_depart_sid = a.load_depart_sid
         AND se.cust_id = a.custa
         AND (   i_stop_list = 'ALL'
              OR se.stop_num IN(SELECT TO_NUMBER(t.column_value)
                                  FROM TABLE(l_t_stop) t))
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.shpidb = i_rlse_ts
         AND (   i_mfst_list = 'ALL'
              OR b.manctb MEMBER OF l_t_mfst)
         AND (   i_tote_list = 'ALL'
              OR b.totctb MEMBER OF l_t_tote)
         AND (   i_item_list = 'ALL'
              OR b.orditb MEMBER OF l_t_item)
         AND b.alcqtb > 0
         AND b.subrcb < 999;

      IF l_t_parm.COUNT > 0 THEN
        IF i_not_shp_rsn <> '120' THEN
          logs.dbg('Upd NotShpRsn');

          UPDATE ordp120b b
             SET b.ntshpb = i_not_shp_rsn
           WHERE b.div_part = (SELECT d.div_part
                                 FROM div_mstr_di1d d
                                WHERE d.div_id = i_div)
             AND b.ntshpb = '120'
             AND b.shpidb = i_rlse_ts
             AND (   i_mfst_list = 'ALL'
                  OR b.manctb MEMBER OF l_t_mfst)
             AND (   i_tote_list = 'ALL'
                  OR b.totctb MEMBER OF l_t_tote)
             AND (   i_item_list = 'ALL'
                  OR b.orditb MEMBER OF l_t_item)
             AND EXISTS(SELECT 1
                          FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se
                         WHERE ld.div_part = b.div_part
                           AND ld.llr_dt = i_llr_dt
                           AND (   i_load_list = 'ALL'
                                OR ld.load_num MEMBER OF l_t_load)
                           AND a.div_part = ld.div_part
                           AND a.load_depart_sid = ld.load_depart_sid
                           AND a.stata = 'R'
                           AND se.div_part = a.div_part
                           AND se.load_depart_sid = a.load_depart_sid
                           AND se.cust_id = a.custa
                           AND (   i_stop_list = 'ALL'
                                OR se.stop_num IN(SELECT TO_NUMBER(t.column_value)
                                                    FROM TABLE(l_t_stop) t))
                           AND b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa);
        END IF;   -- i_not_shp_rsn <> '120'

        l_parts := CEIL(l_t_parm.COUNT / 1000);
        logs.dbg('Get ParmList');

        SELECT   LISTAGG(x.parm, '`') WITHIN GROUP (ORDER BY x.parm) AS parm_list
        BULK COLLECT INTO l_t_parm_list
            FROM (SELECT t.column_value AS parm, NTILE(l_parts) OVER(ORDER BY t.column_value) AS part
                    FROM TABLE(l_t_parm) t) x
        GROUP BY x.part;

        IF l_t_parm_list.COUNT > 0 THEN
          logs.dbg('Upd Picks');
          FOR i IN l_t_parm_list.FIRST .. l_t_parm_list.LAST LOOP
            op_pick_confirm_pk.upd_picks_sp(i_div, l_t_parm_list(i), i_not_shp_rsn, i_bill_err_rsn, i_user_id, l_msg);
          END LOOP;
        END IF;   -- l_t_parm_list.COUNT > 0
      END IF;   -- l_t_parm.COUNT > 0

      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, op_const_pk.prcs_load_cube_plan || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END mass_pick_adj_sp;
END op_pick_confirm_pk;
/

