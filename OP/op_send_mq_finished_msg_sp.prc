CREATE OR REPLACE PROCEDURE op_send_mq_finished_msg_sp(
  i_div                   IN  VARCHAR2,
  i_mq_msg_id             IN  VARCHAR2,
  i_finish_file_basename  IN  VARCHAR2
) IS
  /**
  ||----------------------------------------------------------------------------
  || Will send a "finished msg" from Application Server to the mainframe via MQ.
  || The "finished msg" is normally used as a trigger for a mainframe job.
  || #param i_div                   Division ID ie: MW,NE,SW,etc.
  || #param i_mq_msg_id             The MQ message id for the MQ queue to send
  ||                                msg.
  || #param i_finish_file_basename  This is the file name without the extention
  ||                                containing the finished msg. These files
  ||                                end with ".msg" and are located in the
  ||                                "/local/prodcode/bin/std_messages/" dir.
  ||----------------------------------------------------------------------------
  */

  /*
  ||----------------------------------------------------------------------------
  || OP_SEND_MQ_FINISHED_MSG_SP
  ||  Will send a "finished msg" from Application Server to the mainframe via MQ.
  ||  The "finished msg" is normally used as a trigger for a mainframe job.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/04 | rhalpai | Original
  || 12/12/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 07/15/12 | rhalpai | Change logic to use OSCMD_FN. PIR11038
  || 05/13/13 | rhalpai | Add wrapper for ssh to Application Server. PIR11038
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  || 10/25/20 | rhalpai | Change oscmd_fn to ssh to new MQ lpdstmqop01 server.
  || 03/15/21 | rhalpai | Change logic to use hostname from mclane_mq_gp_control to determine (test/prod) MQ server.
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT VARCHAR2(30)                            := 'OP_SEND_MQ_FINISHED_MSG_SP';
  lar_parm             logs.tar_parm;
  l_div_part           NUMBER;
  l_hostname           mclane_mq_gp_control.hostname%TYPE;
  l_mq_qmanager        mclane_mq_gp_control.mq_qmanager%TYPE;
  l_cmd                typ.t_maxvc2;
  l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
  l_os_result          typ.t_maxvc2;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'MqMsgId', i_mq_msg_id);
  logs.add_parm(lar_parm, 'FinishFileBasename', i_finish_file_basename);
  logs.dbg('ENTRY', lar_parm);
  logs.dbg('Initialize');
  l_div_part := div_pk.div_part_fn(i_div);
  l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);

  SELECT hostname, mq_qmanager
    INTO l_hostname, l_mq_qmanager
    FROM mclane_mq_gp_control
   WHERE mq_msg_id = i_mq_msg_id
     AND div_part = l_div_part;

  logs.dbg('Build Command');
  l_cmd := 'ssh '
           || l_hostname
           || ' /opt/mqm/samp/bin/amqsput '
           || i_div
           || '.MC21.OP.'
           || i_mq_msg_id
           || ' '
           || l_mq_qmanager
           || ' < /local/prodcode/bin/std_messages/'
           || i_finish_file_basename
           || '.msg';
  logs.dbg(l_cmd);
  l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
  logs.dbg(l_os_result);
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END op_send_mq_finished_msg_sp;
/

