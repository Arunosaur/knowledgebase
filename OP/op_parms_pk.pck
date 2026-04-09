CREATE OR REPLACE PACKAGE op_parms_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_op    CONSTANT VARCHAR2(2) := 'OP';
  g_c_csr   CONSTANT VARCHAR2(3) := 'CSR';
  g_c_vchr  CONSTANT VARCHAR2(4) := 'VCHR';
  g_c_int   CONSTANT VARCHAR2(3) := 'INT';
  g_c_dec   CONSTANT VARCHAR2(3) := 'DEC';
  g_c_dt    CONSTANT VARCHAR2(2) := 'DT';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION val_fn(
    i_div_part  IN  NUMBER,
    i_parm_id   IN  VARCHAR2,
    i_appl_id   IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN VARCHAR2;

  FUNCTION vals_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_list  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN type_stab;

  FUNCTION idx_vals_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_list  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN op_types_pk.tt_varchars_v;

  FUNCTION vals_for_prfx_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_prfx  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN type_stab;

  FUNCTION idx_vals_for_prfx_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_prfx  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN op_types_pk.tt_varchars_v;

  FUNCTION val_exists_for_prfx_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_prfx  IN  VARCHAR2,
    i_val           IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN VARCHAR2;

  FUNCTION parms_for_val_fn(
    i_div_part        IN  NUMBER,
    i_parm_id_prfx    IN  VARCHAR2,
    i_val             IN  VARCHAR2,
    i_substr_parm_id  IN  NUMBER DEFAULT NULL,
    i_appl_id         IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN type_stab;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE load_idx_vchr_tab_sp(
    i_t_idxs  IN      type_stab,
    i_t_vals  IN      type_stab,
    o_t_vchr  OUT     op_types_pk.tt_varchars_v
  );

  PROCEDURE get_parms_sp(
    i_div_part      IN      NUMBER,
    i_parm_id_list  IN      VARCHAR2,
    o_t_ids         OUT     type_stab,
    o_t_vals        OUT     type_stab,
    i_appl_id       IN      VARCHAR2 DEFAULT g_c_op
  );

  PROCEDURE get_parms_for_prfx_sp(
    i_div_part        IN      NUMBER,
    i_parm_id_prfx    IN      VARCHAR2,
    o_t_ids           OUT     type_stab,
    o_t_vals          OUT     type_stab,
    i_substr_parm_id  IN      NUMBER DEFAULT NULL,
    i_appl_id         IN      VARCHAR2 DEFAULT g_c_op
  );

  PROCEDURE merge_sp(
    i_div_part  IN  NUMBER,
    i_parm_id   IN  VARCHAR2,
    i_col_typ   IN  VARCHAR2,
    i_val       IN  VARCHAR2,
    i_user_id   IN  VARCHAR2,
    i_appl_id   IN  VARCHAR2 DEFAULT g_c_op,
    i_parm_typ  IN  VARCHAR2 DEFAULT 'DFT'
  );

  PROCEDURE del_sp(
    i_div_part  IN  NUMBER,
    i_parm_id   IN  VARCHAR2,
    i_appl_id   IN  VARCHAR2 DEFAULT g_c_op
  );
END op_parms_pk;
/

CREATE OR REPLACE PACKAGE BODY op_parms_pk IS
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
  || VAL_FN
  ||  Returns value for ParmId.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION val_fn(
    i_div_part  IN  NUMBER,
    i_parm_id   IN  VARCHAR2,
    i_appl_id   IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_PARMS_PK.VAL_FN';
    lar_parm             logs.tar_parm;
    l_val                appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmId', i_parm_id);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   DECODE(p.col_typ,
                       g_c_vchr, p.vchar_val,
                       g_c_int, TO_CHAR(p.intgr_val),
                       g_c_dec, TO_CHAR(p.dec_val),
                       g_c_dt, TO_CHAR(p.dt_val, 'YYYYMMDDHH24MISS')
                      )
           FROM appl_sys_parm_ap1s p
          WHERE p.div_part IN(0, i_div_part)
            AND p.appl_id = i_appl_id
            AND p.parm_id = i_parm_id
       ORDER BY p.div_part DESC;

    FETCH l_cv
     INTO l_val;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_val);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END val_fn;

  /*
  ||----------------------------------------------------------------------------
  || VALS_FN
  ||  Returns a nested PLSQL table of parm values for a given ParmId list.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION vals_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_list  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN type_stab IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.VALS_FN';
    lar_parm             logs.tar_parm;
    l_t_ids              type_stab;
    l_t_vals             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdList', i_parm_id_list);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Parm Ids/Values for ParmId List');
    get_parms_sp(i_div_part, i_parm_id_list, l_t_ids, l_t_vals, i_appl_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_vals);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END vals_fn;

  /*
  ||----------------------------------------------------------------------------
  || IDX_VALS_FN
  ||  Returns an index-by PLSQL table of ParmValues indexed by ParmId for a
  ||  given ParmId list.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION idx_vals_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_list  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN op_types_pk.tt_varchars_v IS
    l_c_module  CONSTANT typ.t_maxfqnm             := 'OP_PARMS_PK.IDX_VALS_FN';
    lar_parm             logs.tar_parm;
    l_t_ids              type_stab;
    l_t_vals             type_stab;
    l_t_vals_v           op_types_pk.tt_varchars_v;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdList', i_parm_id_list);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Parm Ids/Values for ParmId List');
    get_parms_sp(i_div_part, i_parm_id_list, l_t_ids, l_t_vals, i_appl_id);
    logs.dbg('Load index-by table');
    load_idx_vchr_tab_sp(l_t_ids, l_t_vals, l_t_vals_v);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_vals_v);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END idx_vals_fn;

  /*
  ||----------------------------------------------------------------------------
  || VALS_FOR_PRFX_FN
  ||  Returns a nested PLSQL table of ParmValues for a given ParmId prefix.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION vals_for_prfx_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_prfx  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN type_stab IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.VALS_FOR_PRFX_FN';
    lar_parm             logs.tar_parm;
    l_t_ids              type_stab;
    l_t_vals             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdPrfx', i_parm_id_prfx);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Parm Values for ParmId Prefix');
    get_parms_for_prfx_sp(i_div_part, i_parm_id_prfx, l_t_ids, l_t_vals, NULL, i_appl_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_vals);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END vals_for_prfx_fn;

  /*
  ||----------------------------------------------------------------------------
  || IDX_VALS_FOR_PRFX_FN
  ||  Returns an index-by PLSQL table of ParmValues indexed by ParmId for a
  ||  given ParmId prefix.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION idx_vals_for_prfx_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_prfx  IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN op_types_pk.tt_varchars_v IS
    l_c_module  CONSTANT typ.t_maxfqnm             := 'OP_PARMS_PK.IDX_VALS_FOR_PRFX_FN';
    lar_parm             logs.tar_parm;
    l_t_ids              type_stab;
    l_t_vals             type_stab;
    l_t_vals_v           op_types_pk.tt_varchars_v;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdPrfx', i_parm_id_prfx);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Parm Values for ParmId Prefix');
    get_parms_for_prfx_sp(i_div_part, i_parm_id_prfx, l_t_ids, l_t_vals, NULL, i_appl_id);
    logs.dbg('Load index-by table');
    load_idx_vchr_tab_sp(l_t_ids, l_t_vals, l_t_vals_v);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_vals_v);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END idx_vals_for_prfx_fn;

  /*
  ||----------------------------------------------------------------------------
  || VAL_EXISTS_FOR_PRFX_FN
  ||  Return Y if value passed exists for any ParmId matching ParmId prefix
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION val_exists_for_prfx_fn(
    i_div_part      IN  NUMBER,
    i_parm_id_prfx  IN  VARCHAR2,
    i_val           IN  VARCHAR2,
    i_appl_id       IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.VAL_EXISTS_FOR_PRFX_FN';
    lar_parm             logs.tar_parm;
    l_t_ids              type_stab;
    l_t_vals             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdPrfx', i_parm_id_prfx);
    logs.add_parm(lar_parm, 'Val', i_val);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Parm Values for ParmId Prefix');
    get_parms_for_prfx_sp(i_div_part, i_parm_id_prfx, l_t_ids, l_t_vals, NULL, i_appl_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN((CASE
              WHEN i_val MEMBER OF l_t_vals THEN 'Y'
              ELSE 'N'
            END));
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END val_exists_for_prfx_fn;

  /*
  ||----------------------------------------------------------------------------
  || PARMS_FOR_VAL_FN
  ||  Returns nested PLSQL table of ParmIds for ParmId prefix matching ParmValue.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION parms_for_val_fn(
    i_div_part        IN  NUMBER,
    i_parm_id_prfx    IN  VARCHAR2,
    i_val             IN  VARCHAR2,
    i_substr_parm_id  IN  NUMBER DEFAULT NULL,
    i_appl_id         IN  VARCHAR2 DEFAULT g_c_op
  )
    RETURN type_stab IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.PARMS_FOR_VAL_FN';
    lar_parm             logs.tar_parm;
    l_t_ids              type_stab;
    l_t_vals             type_stab;
    l_t_matching_ids     type_stab     := type_stab();
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdPrfx', i_parm_id_prfx);
    logs.add_parm(lar_parm, 'Val', i_val);
    logs.add_parm(lar_parm, 'SubstrParmId', i_substr_parm_id);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Parm Values for ParmId Prefix');
    get_parms_for_prfx_sp(i_div_part, i_parm_id_prfx, l_t_ids, l_t_vals, i_substr_parm_id, i_appl_id);

    IF l_t_ids.COUNT > 0 THEN
      FOR i IN l_t_ids.FIRST .. l_t_ids.LAST LOOP
        IF l_t_vals(i) = i_val THEN
          l_t_matching_ids.EXTEND;
          l_t_matching_ids(l_t_matching_ids.LAST) := l_t_ids(i);
        END IF;   -- l_t_vals(i) = i_val
      END LOOP;
    END IF;   -- l_t_ids.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_matching_ids);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END parms_for_val_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_IDX_VCHR_TAB_SP
  ||  Populate an index-by PLSQL table with values from passed "Value" nested
  ||  table indexed by values from passed "Index" nested table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - PIR2909
  ||----------------------------------------------------------------------------
  */
  PROCEDURE load_idx_vchr_tab_sp(
    i_t_idxs  IN      type_stab,
    i_t_vals  IN      type_stab,
    o_t_vchr  OUT     op_types_pk.tt_varchars_v
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.LOAD_IDX_VCHR_TAB_SP';
    lar_parm             logs.tar_parm;
    l_idx                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'IdxsTab', i_t_idxs);
    logs.add_parm(lar_parm, 'ValsTab', i_t_vals);
    logs.dbg('ENTRY', lar_parm);

    IF (    i_t_idxs IS NOT NULL
        AND i_t_vals IS NOT NULL
        AND i_t_idxs.COUNT = i_t_vals.COUNT) THEN
      l_idx := i_t_vals.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        -- store values in table indexed by parm_id
        o_t_vchr(i_t_idxs(l_idx)) := i_t_vals(l_idx);
        l_idx := i_t_vals.NEXT(l_idx);
      END LOOP;
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_idx_vchr_tab_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_PARMS_SP
  ||  Get nested PLSQL tables of Parm Ids and Values for a given ParmId list.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_parms_sp(
    i_div_part      IN      NUMBER,
    i_parm_id_list  IN      VARCHAR2,
    o_t_ids         OUT     type_stab,
    o_t_vals        OUT     type_stab,
    i_appl_id       IN      VARCHAR2 DEFAULT g_c_op
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.GET_PARMS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdList', i_parm_id_list);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);

    SELECT DISTINCT t.column_value,
                    DECODE(FIRST_VALUE(p.col_typ) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                           g_c_vchr, FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                           g_c_int, TO_CHAR(FIRST_VALUE(p.intgr_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC)
                                           ),
                           g_c_dec, TO_CHAR(FIRST_VALUE(p.dec_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC)),
                           g_c_dt, TO_CHAR(FIRST_VALUE(p.dt_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                                           'YYYYMMDDHH24MISS'
                                          )
                          )
    BULK COLLECT INTO o_t_ids,
                    o_t_vals
               FROM TABLE(CAST(str.parse_list(i_parm_id_list) AS type_stab)) t, appl_sys_parm_ap1s p
              WHERE p.div_part(+) IN(0, i_div_part)
                AND p.appl_id(+) = i_appl_id
                AND p.parm_id(+) = t.column_value
           ORDER BY t.column_value;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_parms_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_PARMS_FOR_PRFX_SP
  ||  Get nested PLSQL tables of Parm Ids and Values for a given ParmId prefix.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_parms_for_prfx_sp(
    i_div_part        IN      NUMBER,
    i_parm_id_prfx    IN      VARCHAR2,
    o_t_ids           OUT     type_stab,
    o_t_vals          OUT     type_stab,
    i_substr_parm_id  IN      NUMBER DEFAULT NULL,
    i_appl_id         IN      VARCHAR2 DEFAULT g_c_op
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.GET_PARMS_FOR_PRFX_SP';
    lar_parm             logs.tar_parm;
    l_pos                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmIdPrfx', i_parm_id_prfx);
    logs.add_parm(lar_parm, 'SubstrParmId', i_substr_parm_id);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_substr_parm_id IS NOT NULL THEN
      l_pos := ABS(i_substr_parm_id) * -1;
    END IF;   -- i_substr_parm_id IS NOT NULL

    logs.dbg('Get ParmIds/Values for ParmId Prefix');

    SELECT DISTINCT DECODE(l_pos, NULL, p.parm_id, SUBSTR(p.parm_id, l_pos)),
                    DECODE(FIRST_VALUE(p.col_typ) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                           g_c_vchr, FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                           g_c_int, TO_CHAR(FIRST_VALUE(p.intgr_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC)
                                           ),
                           g_c_dec, TO_CHAR(FIRST_VALUE(p.dec_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC)),
                           g_c_dt, TO_CHAR(FIRST_VALUE(p.dt_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                                           'YYYYMMDDHH24MISS'
                                          )
                          )
    BULK COLLECT INTO o_t_ids,
                    o_t_vals
               FROM appl_sys_parm_ap1s p
              WHERE p.div_part IN(0, i_div_part)
                AND p.appl_id = i_appl_id
                AND p.parm_id LIKE i_parm_id_prfx || '%'
           ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_parms_for_prfx_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_SP
  ||  Insert/Update Parm Data.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Original - PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_sp(
    i_div_part  IN  NUMBER,
    i_parm_id   IN  VARCHAR2,
    i_col_typ   IN  VARCHAR2,
    i_val       IN  VARCHAR2,
    i_user_id   IN  VARCHAR2,
    i_appl_id   IN  VARCHAR2 DEFAULT g_c_op,
    i_parm_typ  IN  VARCHAR2 DEFAULT 'DFT'
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                       := 'OP_PARMS_PK.MERGE_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                                := SYSDATE;
    l_vchr                appl_sys_parm_ap1s.vchar_val%TYPE   := ' ';
    l_int                 appl_sys_parm_ap1s.intgr_val%TYPE   := 0;
    l_dec                 appl_sys_parm_ap1s.dec_val%TYPE     := 0;
    l_dt                  DATE;
    l_user_id             appl_sys_parm_ap1s.user_id%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmId', i_parm_id);
    logs.add_parm(lar_parm, 'ColTyp', i_col_typ);
    logs.add_parm(lar_parm, 'Val', i_val);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.add_parm(lar_parm, 'ParmTyp', i_parm_typ);
    logs.dbg('ENTRY', lar_parm);

    CASE i_col_typ
      WHEN g_c_vchr THEN
        l_vchr := i_val;
      WHEN g_c_int THEN
        l_int := i_val;
      WHEN g_c_dec THEN
        l_dec := i_val;
      WHEN g_c_dt THEN
        l_dt := TO_DATE(i_val, 'YYYYMMDDHH24MISS');
    END CASE;

    l_user_id := SUBSTR(i_user_id, 1, 8);
    MERGE INTO appl_sys_parm_ap1s p
         USING (SELECT 'Y' AS val
                  FROM DUAL) x
            ON (    p.div_part = i_div_part
                AND p.appl_id = i_appl_id
                AND p.parm_id = i_parm_id
                AND x.val = 'Y')
      WHEN MATCHED THEN
        UPDATE
           SET p.user_id = l_user_id, p.last_chg_ts = l_c_sysdate, p.vchar_val = l_vchr, p.intgr_val = l_int,
               p.dec_val = l_dec, p.dt_val = l_dt
      WHEN NOT MATCHED THEN
        INSERT(appl_id, parm_id, parm_typ, col_typ, vchar_val, intgr_val, dec_val, dt_val, user_id, last_chg_ts,
               div_part)
        VALUES(i_appl_id, i_parm_id, i_parm_typ, i_col_typ, l_vchr, l_int, l_dec, l_dt, l_user_id, l_c_sysdate,
               i_div_part);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END merge_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_SP
  ||  Remove parm entry.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/02/17 | rhalpai | Original - PIR14910
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_sp(
    i_div_part  IN  NUMBER,
    i_parm_id   IN  VARCHAR2,
    i_appl_id   IN  VARCHAR2 DEFAULT g_c_op
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PARMS_PK.DEL_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ParmId', i_parm_id);
    logs.add_parm(lar_parm, 'ApplId', i_appl_id);
    logs.dbg('ENTRY', lar_parm);

    DELETE FROM appl_sys_parm_ap1s p
          WHERE p.div_part = i_div_part
            AND p.appl_id = i_appl_id
            AND p.parm_id = i_parm_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_sp;
END op_parms_pk;
/

