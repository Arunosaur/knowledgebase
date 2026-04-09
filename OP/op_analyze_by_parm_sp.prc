CREATE OR REPLACE PROCEDURE op_analyze_by_parm_sp(
  i_div_part      IN  NUMBER,
  i_parm_id_prfx  IN  VARCHAR2,
  i_appl          IN  VARCHAR2 DEFAULT 'OP'
) IS
  /**
  ||----------------------------------------------------------------------------
  || Will analyze tables for tables found for Parm prefix.
  || #param i_div_part       DivPart.
  || #param i_parm_id_prfx   Parm ID prefix.
  || #param i_appl           Parm Application ID.
  ||----------------------------------------------------------------------------
  */
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/10/07 | rhalpai | Original PIR3209
  || 03/14/11 | rhalpai | Changed to use DBA analyze procedure.
  || 10/14/17 | rhalpai | Add div_part input parm. Change to call new
  ||                    | OP_PARMS_PK.IDX_VALS_FOR_PRFX_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT typ.t_maxfqnm                     := 'OP_ANALYZE_BY_PARM_SP';
  lar_parm             logs.tar_parm;
  l_t_tbls             op_types_pk.tt_varchars_v;
  l_idx                appl_sys_parm_ap1s.parm_id%TYPE;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'DivPart', i_div_part);
  logs.add_parm(lar_parm, 'ParmIdPrfx', i_parm_id_prfx);
  logs.add_parm(lar_parm, 'Appl', i_appl);
  logs.dbg('ENTRY', lar_parm);
  logs.dbg('Get tables for parms');
  l_t_tbls := op_parms_pk.idx_vals_for_prfx_fn(i_div_part, i_parm_id_prfx, i_appl);
  l_idx := l_t_tbls.FIRST;
  WHILE l_idx IS NOT NULL LOOP
    logs.info('Analyzing ' || l_t_tbls(l_idx));
    analyze_table_sp(l_t_tbls(l_idx));
    l_idx := l_t_tbls.NEXT(l_idx);
  END LOOP;
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END op_analyze_by_parm_sp;
/

