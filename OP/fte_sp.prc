CREATE OR REPLACE PROCEDURE fte_sp(
  i_div              IN  VARCHAR2,
  i_local_file_list  IN  VARCHAR2,
  i_rmt_file         IN  VARCHAR2,
  i_zip_file         IN  VARCHAR2 DEFAULT NULL,
  i_job_nm           IN  VARCHAR2 DEFAULT NULL,
  i_ovrwrite_sw      IN  VARCHAR2 DEFAULT 'N',
  i_dest_qmgr        IN  VARCHAR2 DEFAULT NULL,
  i_dest_agnt        IN  VARCHAR2 DEFAULT NULL,
  i_dest_file        IN  VARCHAR2 DEFAULT NULL
) IS
  /*
  ||----------------------------------------------------------------------------
  || FTE_SP
  ||  Will send local files on linux to remote destination via MQ FTE.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT typ.t_maxfqnm                       := 'FTE_SP';
  lar_parm             logs.tar_parm;
  l_div_part           NUMBER;
  l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
  l_cmd                typ.t_maxvc2;
  l_os_result          typ.t_maxvc2;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'LocalFileList', i_local_file_list);
  logs.add_parm(lar_parm, 'RmtFile', i_rmt_file);
  logs.add_parm(lar_parm, 'ZipFile', i_zip_file);
  logs.add_parm(lar_parm, 'JobNm', i_job_nm);
  logs.add_parm(lar_parm, 'OvrwriteSw', i_ovrwrite_sw);
  logs.add_parm(lar_parm, 'DestQmgr', i_dest_qmgr);
  logs.add_parm(lar_parm, 'DestAgnt', i_dest_agnt);
  logs.add_parm(lar_parm, 'DestFile', i_dest_file);
  logs.info('ENTRY', lar_parm);
  l_div_part := div_pk.div_part_fn(i_div);
  l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
  l_cmd := '/local/prodcode/bin/xxopFTE.scr "'
           || i_div
           || '" "'
           || i_local_file_list
           || '" "'
           || i_rmt_file
           || '" "'
           || i_zip_file
           || '" "'
           || i_job_nm
           || '" "'
           || i_ovrwrite_sw
           || '" "'
           || i_dest_qmgr
           || '" "'
           || i_dest_agnt
           || '" "'
           || i_dest_file
           || '"';
  logs.info(l_cmd);
  l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
  logs.info(l_os_result);
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
END fte_sp;
/

