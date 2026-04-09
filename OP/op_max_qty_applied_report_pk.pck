CREATE OR REPLACE PACKAGE op_max_qty_applied_report_pk IS
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
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    o_status   OUT     VARCHAR2
  );
END op_max_qty_applied_report_pk;
/

CREATE OR REPLACE PACKAGE BODY op_max_qty_applied_report_pk IS
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
  ||  Create report showing orders with items where order quantities were
  ||  exceptional and the max quantity was applied.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/08/02 | rhalpai | Original
  || 06/16/08 | rhalpai | Added sort by Load/Stop/OrdNum to cursor.
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE report_sp(
    i_div      IN      VARCHAR2,
    o_file_nm  OUT     VARCHAR2,
    o_status   OUT     VARCHAR2
  ) IS
    l_c_module          CONSTANT typ.t_maxfqnm  := 'OP_MAX_QTY_APPLIED_REPORT_PK.REPORT_SP';
    lar_parm                     logs.tar_parm;
    l_c_sysdate         CONSTANT DATE           := SYSDATE;
    l_c_file_dir        CONSTANT VARCHAR2(50)   := '/ftptrans';
    l_file_nm                    VARCHAR2(80);
    l_t_heads                    type_stab;
    l_c_lines_per_page  CONSTANT PLS_INTEGER    := 80;
    l_line_cnt                   PLS_INTEGER    := 99;
    l_page_cnt                   PLS_INTEGER    := 0;
    l_t_rpt_lns                  typ.tas_maxvc2;
    l_detail_line                VARCHAR2(81);
    l_save_ord_num               NUMBER         := -1;

    CURSOR l_cur_rpt(
      b_div  VARCHAR2
    ) IS
      SELECT   ld.load_num, se.stop_num, b.ordnob AS ord_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm,
               b.orditb AS ord_item, b.orgqtb AS orig_qty, b.maxqtb AS max_qty,
               DECODE(b.bymaxb, 'Y', ' Y ', '1', ' Y ', '   ') AS byp_max_sw, e.shppke AS item_pack,
               e.sizee AS item_sz, e.ctdsce AS item_descr
          FROM div_mstr_di1d dv, mclp300d d, ordp120b b, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, sysp200c c,
               mclp020b cx, sawp505e e
         WHERE dv.div_id = b_div
           AND d.div_part = dv.div_part
           AND d.reasnd = '002'
           AND b.div_part = d.div_part
           AND b.ordnob = d.ordnod
           AND b.lineb = d.ordlnd
           AND a.div_part = b.div_part
           AND a.ordnoa = b.ordnob
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
      GROUP BY ld.load_num, se.stop_num, b.ordnob, cx.mccusb, c.namec, b.orditb, b.orgqtb, b.maxqtb, b.bymaxb, e.shppke,
               e.sizee, e.ctdsce
      ORDER BY load_num, stop_num, ord_num;

    PROCEDURE load_tbl_sp IS
      --  cc = cariage control
      l_c_cc_space     CONSTANT VARCHAR2(1) := ' ';
      l_c_cc_new_page  CONSTANT VARCHAR2(1) := '1';
    BEGIN
      IF l_line_cnt >= l_c_lines_per_page THEN
        l_page_cnt := l_page_cnt + 1;
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
      ELSE
        -- double-space
        util.append(l_t_rpt_lns, l_c_cc_space);
        l_line_cnt := l_line_cnt + 1;
      END IF;   -- l_line_cnt >= l_c_lines_per_page

      util.append(l_t_rpt_lns, l_c_cc_space || l_detail_line);
    END load_tbl_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    o_status := 'Good';
    l_file_nm := i_div || '_MAX_QTY_APPLIED_' || TO_CHAR(l_c_sysdate, 'YYYYMMDDHH24MISS');
    o_file_nm := l_c_file_dir || '/' || l_file_nm;
    l_t_heads :=
      type_stab
        ('DATE: '
         || TO_CHAR(l_c_sysdate, 'YY/MM/DD')
         || RPAD(' ', 24)
         || str.ctr(i_div || ' ' || div_pk.div_nm_fn(i_div), str.sp, 50)
         || LPAD('PAGE: ', 39),
         'TIME: ' || TO_CHAR(l_c_sysdate, 'HH24:MI:SS') || LPAD('MAX QUANTITY APPLIED', 59),
         LPAD('ORDER REPORT', 69),
         '',
         '',
         'LOAD STOP ORDER NO. CUST NO. NAME                      ITEM NO. QTY  MAX  BYP PACK SIZE     DESCRIPTION',
         '---- ---- --------- -------- ------------------------- -------- ---- ---- --- ---- -------- -------------------------'
        );
    logs.dbg('Report Cursor Loop');
    FOR l_r_rpt IN l_cur_rpt(i_div) LOOP
      IF l_r_rpt.ord_num <> l_save_ord_num THEN
        l_save_ord_num := l_r_rpt.ord_num;
        logs.dbg('Format New-Order Detail Line');
        l_detail_line := l_r_rpt.load_num
                         || ' '
                         || LPAD(TO_CHAR(l_r_rpt.stop_num, '09'), 4)
                         || ' '
                         || LTRIM(TO_CHAR(l_r_rpt.ord_num, '099999999'))
                         || ' '
                         || LPAD(l_r_rpt.mcl_cust, 8)
                         || ' '
                         || RPAD(l_r_rpt.cust_nm, 25);
      ELSE
        logs.dbg('Format Same-Order Detail Line');
        l_detail_line := RPAD(' ', 4)
                         || ' '
                         || RPAD(' ', 4)
                         || ' '
                         || RPAD(' ', 9)
                         || ' '
                         || LPAD(' ', 8)
                         || ' '
                         || RPAD(' ', 25);
      END IF;   -- l_r_rpt.ord_num <> l_save_ord_num

      l_detail_line := l_detail_line
                       || ' '
                       || LPAD(l_r_rpt.ord_item, 8)
                       || ' '
                       || LPAD(LTRIM(TO_CHAR(l_r_rpt.orig_qty, '9999')), 4)
                       || ' '
                       || LPAD(LTRIM(TO_CHAR(l_r_rpt.max_qty, '9999')), 4)
                       || ' '
                       || l_r_rpt.byp_max_sw
                       || ' '
                       || LPAD(LTRIM(TO_CHAR(l_r_rpt.item_pack, '9999')), 4)
                       || ' '
                       || RPAD(l_r_rpt.item_sz, 8)
                       || ' '
                       || l_r_rpt.item_descr;
      load_tbl_sp;
    END LOOP;
    logs.dbg('Write Report File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_status := 'Error';
      logs.err(lar_parm);
  END report_sp;
END op_max_qty_applied_report_pk;
/

