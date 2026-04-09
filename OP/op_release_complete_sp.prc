CREATE OR REPLACE PROCEDURE op_release_complete_sp(
  i_div  IN  VARCHAR2
) IS
  /**
  ||----------------------------------------------------------------------------
  || Called by the Unix XXOPRLCMPL.scr script to complete the Set Release process.
  || It is triggered via MQ msg after the mainframe invoice jobs have completed.
  ||
  || Analyze tables in APPL_SYS_PARM_AP1S with 'ANLYZ_RLSE_CMPL_' Parm ID prefix.
  || Update MCLANE_LOAD_LABEL_RLSE for release completion.
  || Update the load status on LOAD_CLOS_CNTRL_BC2C, MCLP370C.
  ||
  || #param i_div    Division ID.
  ||----------------------------------------------------------------------------
  */
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/10/07 | rhalpai | Original PIR3209
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. Convert to use
  ||                    | standard error handling logic. PIR8531
  || 11/17/11 | rhalpai | Add logic to turn OFF RLSE_COMPL Process Control at
  ||                    | end of procedure. IM-033180
  || 02/15/12 | rhalpai | Moved analyze logic to beginning of procedure.
  ||                    | IM-043061
  || 02/19/16 | rhalpai | Change logic to handle an execution with nothing to do.
  ||                    | This will prevent the OPRLCMPL script failures if
  ||                    | Control-M decides to kick off a 2nd execution a couple
  ||                    | minutes after successful completion. SDLIS-146
  || 10/14/17 | rhalpai | Change to use constants package OP_CONST_PK. PIR15427
  ||----------------------------------------------------------------------------
  */
  l_c_module   CONSTANT typ.t_maxfqnm := 'OP_RELEASE_COMPLETE_SP';
  lar_parm              logs.tar_parm;
  l_c_sysdate  CONSTANT DATE          := SYSDATE;
  l_div_part            NUMBER;
  l_rlse_id             NUMBER;
  l_llr_dt              DATE;
  l_llr_num             NUMBER;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.info('ENTRY', lar_parm);
  l_div_part := div_pk.div_part_fn(i_div);
  logs.dbg('Analyze Tables for Parm');
  op_analyze_by_parm_sp(l_div_part, 'ANLYZ_RLSE_CMPL_');
  logs.dbg('Upd Release');

  UPDATE    rlse_op1z r
        SET r.stat_cd = 'R',
            r.end_ts = l_c_sysdate
      WHERE r.div_part = l_div_part
        AND r.rlse_ts = (SELECT MAX(r2.rlse_ts)
                           FROM rlse_op1z r2
                          WHERE r2.div_part = l_div_part
                            AND r2.stat_cd = 'P')
  RETURNING r.rlse_id, r.llr_dt
       INTO l_rlse_id, l_llr_dt;

  IF l_rlse_id IS NOT NULL THEN
    l_llr_num := l_llr_dt - DATE '1900-02-28';
    logs.dbg('Add Release Log Entry');

    INSERT INTO rlse_log_op2z
                (div_part, rlse_id, typ_id, create_ts
                )
         VALUES (l_div_part, l_rlse_id, 'RLSECMP', l_c_sysdate
                );

    logs.dbg('Upd LoadClosCntrl');

    UPDATE load_clos_cntrl_bc2c lc
       SET lc.load_status = 'R'
     WHERE lc.div_part = l_div_part
       AND lc.llr_dt = l_llr_dt
       AND lc.load_status = 'P';

    logs.dbg('Upd MCLP370C');

    UPDATE mclp370c mc
       SET mc.load_status = 'R'
     WHERE mc.div_part = l_div_part
       AND mc.llr_date = l_llr_num
       AND mc.load_status = 'P';
  END IF;   -- l_rlse_id IS NOT NULL

  COMMIT;
  logs.dbg('Set RlseCompl Process Inactive');
  op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                              op_process_control_pk.g_c_inactive,
                                              USER,
                                              l_div_part
                                             );
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END op_release_complete_sp;
/

