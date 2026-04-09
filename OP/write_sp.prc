CREATE OR REPLACE PROCEDURE write_sp(
  i_t_rpt_lns  IN  typ.tas_maxvc2,
  i_file_nm    IN  VARCHAR2,
  i_file_dir   IN  VARCHAR2 DEFAULT '/ftptrans',
  i_mode       IN  VARCHAR2 DEFAULT 'W'
) IS
  /*
  ||----------------------------------------------------------------------------
  || WRITE_SP
  ||  Write array output to file
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/17/14 | rhalpai | Initial
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT VARCHAR2(30)  := 'WRITE_SP';
  lar_parm             logs.tar_parm;
BEGIN
  logs.add_parm(lar_parm, 'RptLnsCount', i_t_rpt_lns.COUNT);
  logs.add_parm(lar_parm, 'FileNm', i_file_nm);
  logs.add_parm(lar_parm, 'FileDir', i_file_dir);
  logs.add_parm(lar_parm, 'Mode', i_mode);
  logs.dbg('Writing ' || i_t_rpt_lns.COUNT || ' lines to file ' || i_file_dir || '/' || i_file_nm);
  timer.startme('write_file');
  io.write_lines(i_t_rpt_lns, i_file_nm, i_file_dir, i_mode);
  timer.stopme('write_file');
  logs.dbg('Write took ' || timer.elapsed('write_file') || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END write_sp;
/

