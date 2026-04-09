CREATE OR REPLACE PROCEDURE trigger_msg_sp(
  i_div     IN  VARCHAR2,
  i_job_id  IN  VARCHAR2
) IS
  /*
  ||----------------------------------------------------------------------------
  || NAME                : trigger_msg_sp
  || CREATED BY          : David Beal
  || CREATE DATE         : 05-July-2013
  || DESCRIPTION         : create a new sp to be called by the ui that generates a
  ||                       trigger message to request the cwm03j.
  ||----------------------------------------------------------------------------
  || CHANGELOG
  ||----------------------------------------------------------------------------
  || DATE       | USER ID |    CHANGES
  ||----------------------------------------------------------------------------
  ||
  ||----------------------------------------------------------------------------
  */
  l_c_module    CONSTANT VARCHAR2(200) := 'TRIGGER_MSG_SP';
  lar_parm               logs.tar_parm;
  l_div_part             NUMBER;
  l_c_file_dir  CONSTANT VARCHAR2(9)   := '/ftptrans';
  l_file_nm              VARCHAR2(30);
  l_rmt_file             VARCHAR2(30);
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'JobId', i_job_id);
  logs.info('ENTRY', lar_parm);
  l_div_part := div_pk.div_part_fn(i_div);
  l_file_nm := i_div || '_' || i_job_id || '_TRIGGER';
  l_rmt_file := 'FTE.'
                || op_parms_pk.val_fn(l_div_part, op_const_pk.prm_fte_rmt_loc)
                || '.'
                || i_div
                || i_job_id
                || '.TRIGGER';
  io.write_line(' ', l_file_nm, l_c_file_dir, 'W');
  fte_sp(i_div, l_file_nm, l_rmt_file, NULL, l_c_module);
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END trigger_msg_sp;
/

