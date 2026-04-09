CREATE OR REPLACE PROCEDURE op_xata_extract_sp(
  i_div       IN  VARCHAR2,
  i_begin_dt  IN  DATE,
  i_end_dt    IN  DATE
) IS
  /*
  ||----------------------------------------------------------------------------
  || OP_XATA_EXTRACT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/07/06 | JCLONTS | Original
  || 08/18/06 | rhalpai | Changed cursor from UNION to UNION ALL and added a
  ||                    | substring for the date portion of SHPIDB.
  || 10/04/06  |pcunnin | Add not ship reason to file being built
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 01/07/22 | rhalpai | Change logic to zip file before ftping to MF. SDHD-1155355
  ||----------------------------------------------------------------------------
  */
  l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_XATA_EXTRACT_SP';
  lar_parm               logs.tar_parm;
  l_begin_dt_str         VARCHAR2(8);
  l_end_dt_str           VARCHAR2(8);
  l_c_file_dir  CONSTANT VARCHAR2(80)   := '/ftptrans';
  l_file_nm              VARCHAR2(30);
  l_t_rpt_lns            typ.tas_maxvc2;

  PROCEDURE ftp_report_sp IS
    l_c_remote_file  CONSTANT VARCHAR2(80) := 'XATA.OPLO.DATA';
    -- 'Y' creates Zip file with proper type 'U'
    -- 'N' creates unzipped file which must be preallocated as type FBA
    l_c_archive_sw   CONSTANT VARCHAR2(1)  := 'Y';
  BEGIN
    op_ftp_sp(i_div, l_file_nm, l_c_remote_file, l_c_archive_sw);
  END ftp_report_sp;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'BeginDt', i_begin_dt);
  logs.add_parm(lar_parm, 'EndDt', i_end_dt);
  logs.info('ENTRY', lar_parm);
  logs.dbg('Initialize');
  l_begin_dt_str := TO_CHAR(i_begin_dt, 'YYYYMMDD');
  l_end_dt_str := TO_CHAR(i_end_dt, 'YYYYMMDD');
  l_file_nm := i_div || '_XATA_OPLO_ftp';
  logs.dbg('Add Report Items');

  SELECT   i_div
           || totals.cust_id
           || TO_CHAR(totals.catlg_num, 'FM000000000')
           || SUBSTR(totals.rlse_ts, 1, 4)
           || '-'
           || SUBSTR(totals.rlse_ts, 5, 2)
           || '-'
           || SUBSTR(totals.rlse_ts, 7, 2)
           || totals.load_num
           || TO_CHAR(totals.price_amt, 'FM00000.00')
           || TO_CHAR(SUM(totals.lineouts), 'FM000000000')
           || totals.nt_shp_rsn AS rpt_ln
  BULK COLLECT INTO l_t_rpt_lns
      FROM ((SELECT   a.custa AS cust_id, b.orditb AS catlg_num, b.shpidb AS rlse_ts, ld.load_num,
                      b.hdprcb AS price_amt, SUM(NVL(b.alcqtb, 0) - NVL(pckqtb, 0)) AS lineouts,
                      (CASE b.ntshpb
                         WHEN '120' THEN 'LINEOUT'
                         WHEN '121' THEN 'ORDERR'
                         WHEN '122' THEN 'BILLERR'
                         ELSE b.ntshpb
                       END
                      ) AS nt_shp_rsn
                 FROM div_mstr_di1d d, mclp140a ma, ordp120b b, ordp100a a, load_depart_op1f ld
                WHERE d.div_id = i_div
                  AND ma.rsntpa = 12
                  AND b.div_part = d.div_part
                  AND b.ntshpb = ma.rsncda
                  AND b.statb = 'A'
                  AND NVL(b.alcqtb, 0) <> NVL(b.pckqtb, 0)
                  AND SUBSTR(b.shpidb, 1, 8) BETWEEN l_begin_dt_str AND l_end_dt_str
                  AND a.div_part = b.div_part
                  AND a.ordnoa = b.ordnob
                  AND NOT EXISTS(SELECT 1
                                   FROM sysp200c c
                                  WHERE c.div_part = a.div_part
                                    AND c.acnoc = a.custa
                                    AND c.tclscc = 'PRP')
                  AND ld.div_part = a.div_part
                  AND ld.load_depart_sid = a.load_depart_sid
             GROUP BY a.custa, b.orditb, b.shpidb, ld.load_num, b.hdprcb, b.ntshpb)
            UNION ALL
            (SELECT   a.custa AS cust_id, b.orditb AS catlg_num, b.shpidb AS rlse_ts, a.orrtea AS load_num,
                      b.hdprcb AS price_amt, SUM(NVL(b.alcqtb, 0) - NVL(pckqtb, 0)) AS lineouts,
                      (CASE b.ntshpb
                         WHEN '120' THEN 'LINEOUT'
                         WHEN '121' THEN 'ORDERR'
                         WHEN '122' THEN 'BILLERR'
                         ELSE b.ntshpb
                       END
                      ) AS nt_shp_rsn
                 FROM div_mstr_di1d d, mclp140a ma, ordp920b b, ordp900a a
                WHERE d.div_id = i_div
                  AND ma.rsntpa = 12
                  AND b.div_part = d.div_part
                  AND b.ntshpb = ma.rsncda
                  AND b.statb = 'A'
                  AND NVL(b.alcqtb, 0) <> NVL(b.pckqtb, 0)
                  AND SUBSTR(b.shpidb, 1, 8) BETWEEN l_begin_dt_str AND l_end_dt_str
                  AND a.div_part = b.div_part
                  AND a.ordnoa = b.ordnob
                  AND NOT EXISTS(SELECT 1
                                   FROM sysp200c c
                                  WHERE c.div_part = a.div_part
                                    AND c.acnoc = a.custa
                                    AND c.tclscc = 'PRP')
             GROUP BY a.custa, b.orditb, b.shpidb, a.orrtea, b.hdprcb, b.ntshpb)) totals
  GROUP BY totals.cust_id, totals.catlg_num, totals.rlse_ts, totals.load_num, totals.price_amt, totals.nt_shp_rsn;

  logs.dbg('Write Report');
  write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
  logs.dbg('FTP Report');
  ftp_report_sp;
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END op_xata_extract_sp;
/

