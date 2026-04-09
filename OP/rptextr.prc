CREATE OR REPLACE PROCEDURE rptextr(
  i_div       IN      VARCHAR2,
  i_rpt_id    IN      VARCHAR2,
  i_load_typ  IN      VARCHAR2,
  i_dist      IN      VARCHAR2,
  o_status    OUT     VARCHAR2,
  o_file_nm   OUT     VARCHAR2
) IS
  /*
  ||----------------------------------------------------------------------------
  || RPTEXTR
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/24/01 | JUSTANI | Original
  || 02/05/02 | SUDHEER | Qualify the column names
  || 11/10/10 | rhalpai | Replace reference to unused column UMB with DSOUMB.
  ||                    | Convert to use standard error handling logic. PIR5878
  || 07/10/12 | rhalpai | Removed unused columns. PIR11038
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  l_c_module    CONSTANT typ.t_maxfqnm  := 'RPTEXTR';
  lar_parm               logs.tar_parm;
  l_c_file_dir  CONSTANT VARCHAR2(80)   := '/ftptrans';
  l_rpt_id               VARCHAR2(8)    := RPAD(i_rpt_id, 8);
  l_file_nm              VARCHAR2(50)   := i_div || i_rpt_id || '_ftp';
  l_t_rpt_lns            typ.tas_maxvc2;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'RptId', i_rpt_id);
  logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
  logs.add_parm(lar_parm, 'Dist', i_dist);
  logs.info('ENTRY', lar_parm);
  logs.dbg('Initialize');
  o_status := 'Good';
  o_file_nm := l_c_file_dir || '/' || l_file_nm;
  logs.dbg('Add each order for the UNIX Output file');

  SELECT i_div
         || RPAD(l_rpt_id, 8)
         || RPAD(TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISSSSS'), 30)
         || 'ADD'
         || RPAD(NVL(a.ipdtsa, ' '), 3)
         || RPAD(' ', 7)
         || DECODE(l_rpt_id, 'QGROPB  ', 'HRD', 'QGMPPB  ', 'HRD', 'TST')
         || RPAD(NVL(ld.load_num, ' '), 4)
         || LPAD(NVL(se.stop_num, 0), 2, '0')
         || RPAD(NVL(se.cust_id, ' '), 8)
         || RPAD(NVL(a.legrfa, ' '), 25)
         || RPAD(' ', 10)   -- invc_num
         || RPAD(' ', 15)
         || RPAD(LPAD(b.ordnob, 11, '0'), 25)
         || TO_CHAR(NVL(b.lineb, 0), 'FM0999V99999')
         || RPAD(NVL(b.orditb, '0'), 6, '0')
         || SUBSTR(NVL(b.itemnb, '000000000'), 1, 9)
         || RPAD(NVL(b.sllumb, ' '), 3)
         || TO_CHAR(NVL(b.ordqtb, 0), 'FM0999999')
         || '0000000'
         || '0000000'
         || TO_CHAR(NVL(b.hdprcb, 0), 'FM0999999V99')
         || TO_CHAR(NVL(b.hdrtab, 0), 'FM0999999V99')
         || TO_CHAR(NVL(b.hdrtmb, 0), 'FM09999')
         || RPAD(NVL(b.ntshpb, ' '), 8)
         || RPAD(NVL(b.manctb, '000'), 3)
         || RPAD(NVL(b.totctb, '000'), 3)
         || SUBSTR(NVL(b.itemnb, '000000000'), 1, 9)
         || RPAD(NVL(b.sllumb, ' '), 3)
         || DECODE(b.subrcb, 1, 'UNC', 2, 'REP', 3, 'RND', '   ')
         || 'N'   -- rstfeb
         || LPAD(NVL(b.invctb, 0), 3, '0')
         || LPAD(NVL(b.labctb, 0), 3, '0')
         || RPAD(' ', 7)
         || TO_CHAR(se.eta_ts, 'YYYYMMDD')
         || RPAD(NVL(a.cpoa, ' '), 30)
         || RPAD(NVL(a.cspasa, ' '), 25)
         || RPAD(NVL(b.itpasb, ' '), 25)
         || LPAD(NVL(cx.mccusb, '0'), 6, '0')
         || NVL(b.rtfixb, 'N')
         || NVL(b.prfixb, 'N')
         || ' '
         || DECODE(a.dsorda, 'D', 'DIS', 'REG')
         || RPAD(NVL(b.cusitb, ' '), 10)
         || NVL(b.orgitb, '000000')
         || RPAD(NVL(a.ldtypa, 'GRO'), 3)
         || TO_CHAR(se.eta_ts, 'HH24MI')
         || DECODE(a.dsorda, 'N', 'Y', 'N')
         || RPAD(NVL(a.connba, ' '), 8) AS rpt_ln
  BULK COLLECT INTO l_t_rpt_lns
    FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp120b b
   WHERE d.div_id = i_div
     AND a.div_part = d.div_part
     AND a.ldtypa IN('GRO', 'GMP', 'XBI')
     AND a.ldtypa = DECODE(i_load_typ, 'COM', a.ldtypa, i_load_typ)
     AND a.dsorda = DECODE(i_dist, 'E', 'D', 'N', 'R', a.dsorda)
     AND ld.div_part = a.div_part
     AND ld.load_depart_sid = a.load_depart_sid
     AND se.div_part = a.div_part
     AND se.load_depart_sid = a.load_depart_sid
     AND se.cust_id = a.custa
     AND cx.div_part = a.div_part
     AND cx.custb = a.custa
     AND b.div_part = a.div_part
     AND b.ordnob = a.ordnoa
     AND (   b.excptn_sw = 'N'
          OR EXISTS(SELECT 1
                      FROM mclp140a ma
                     WHERE ma.rsncda = b.ntshpb
                       AND ma.exlvla <> 1));

  logs.dbg('Add the Finished Record');
  util.append(l_t_rpt_lns,
              RPAD(i_div
                   || RPAD(l_rpt_id, 8)
                   || RPAD('FINISHED', 30)
                   || 'ADD'
                   || RPAD(' REPORT ', 10)
                   ||(CASE l_rpt_id
                        WHEN 'QGROPB  ' THEN 'HRD'
                        WHEN 'QGMPPB  ' THEN 'HRD'
                        ELSE 'TST'
                      END),
                   391
                  )
             );
  logs.dbg('Write the File');
  write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END rptextr;
/

