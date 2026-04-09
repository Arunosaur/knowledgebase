CREATE OR REPLACE PACKAGE op_setup_pk IS
  PROCEDURE get_rstrns_sp(
    i_div               IN      VARCHAR2,
    i_crp_cd            IN      NUMBER,
    o_rstrn_parms_cur   OUT     SYS_REFCURSOR,
    o_test_ord_src_cur  OUT     SYS_REFCURSOR
  );

  PROCEDURE upd_rstrns_sp(
    i_div                 IN  VARCHAR2,
    i_add_on_ord_sw       IN  VARCHAR2,
    i_crp_cd              IN  NUMBER,
    i_rstr_add_ln_sw      IN  VARCHAR2,
    i_rstr_chg_cust_sw    IN  VARCHAR2,
    i_rstr_chg_qty_sw     IN  VARCHAR2,
    i_rstr_del_ln_sw      IN  VARCHAR2,
    i_rstr_create_ord_sw  IN  VARCHAR2,
    i_excl_repl_sw        IN  VARCHAR2,
    i_ord_src_sw_list     IN  VARCHAR2,
    i_user_id             IN  VARCHAR2
  );
END op_setup_pk;
/

CREATE OR REPLACE PACKAGE BODY op_setup_pk IS
  /*
  ||----------------------------------------------------------------------------
  || GET_RSTRN_PARMS_SP
  ||  Return cursor of restriction parameters for a given corp code.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/02/17 | rhalpai | Original - PIR14910
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_rstrn_parms_sp(
    i_div_part  IN      NUMBER,
    i_crp_cd    IN      NUMBER,
    o_cur       OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SETUP_PK.GET_RSTRN_PARMS_SP';
    lar_parm             logs.tar_parm;
    l_crp_cd             VARCHAR2(3);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_crp_cd := lpad_fn(i_crp_cd, 3, '0');
    logs.dbg('Open Cursor');

    OPEN o_cur
     FOR
       SELECT DISTINCT t.column_value AS parm_id,
                       FIRST_VALUE(DECODE(DECODE(p.col_typ, 'VCHR', p.vchar_val, 'INT', LPAD(p.intgr_val, 3, '0')),
                                          NULL, 'N',
                                          '000', 'N',
                                          DECODE(t.column_value, 'ADD_ON_ORD', p.vchar_val, 'Y')
                                         )
                                  ) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS sw,
                       FIRST_VALUE(p.user_id) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS user_id,
                       TO_CHAR(FIRST_VALUE(p.last_chg_ts) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC),
                               'YYYY-MM-DD HH24:MI:SS'
                              ) AS last_chg_ts
                  FROM TABLE(CAST(type_stab('ADD_ON_ORD',
                                            'ADD_LN',
                                            'CHG_CUST',
                                            'CHG_QTY',
                                            'DEL_LN',
                                            'CREATE_ORD',
                                            'EXCL_REPL'
                                           ) AS type_stab
                                 )
                            ) t,
                       appl_sys_parm_ap1s p
                 WHERE p.div_part(+) IN(0, i_div_part)
                   AND p.appl_id(+) =(CASE
                                        WHEN t.column_value IN('ADD_ON_ORD', 'EXCL_REPL') THEN 'OP'
                                        ELSE 'CSR'
                                      END)
                   AND p.parm_id(+) = DECODE(t.column_value,
                                             'ADD_ON_ORD', 'ADD_ON_ORD',
                                             'ADD_LN', 'RESTRICT_ADDLN_' || l_crp_cd,
                                             'CHG_CUST', 'RESTRICT_CNUMCHG_' || l_crp_cd,
                                             'CHG_QTY', 'RESTRICT_CORDQTY_' || l_crp_cd,
                                             'DEL_LN', 'RESTRICT_DELLN_' || l_crp_cd,
                                             'CREATE_ORD', 'RESTRICT_ORDCR8_' || l_crp_cd,
                                             'EXCL_REPL', 'EXCL_REPL_' || l_crp_cd
                                            )
              ORDER BY DECODE(t.column_value,
                              'ADD_ON_ORD', 1,
                              'ADD_LN', 2,
                              'CHG_CUST', 3,
                              'CHG_QTY', 4,
                              'DEL_LN', 5,
                              'CREATE_ORD', 6,
                              'EXCL_REPL', 7
                             );

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_rstrn_parms_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_TEST_ORD_SRC_SP
  ||  Return cursor of test order sources for a given corp code.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/02/17 | rhalpai | Original - PIR14910
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_test_ord_src_sp(
    i_crp_cd  IN      NUMBER,
    o_cur     OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SETUP_PK.GET_TEST_ORD_SRC_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.dbg('ENTRY', lar_parm);

    OPEN o_cur
     FOR
       SELECT   t.ord_src, t.enable_sw, t.user_id, TO_CHAR(t.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts
           FROM test_ord_src_crp_cd t
          WHERE t.crp_cd = i_crp_cd
       ORDER BY t.ord_src;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_test_ord_src_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_RSTRNS_SP
  ||  Return cursor of restriction parameters and cursor of test order sources
  ||  for a given corp code.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/02/17 | rhalpai | Original - PIR14910
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_rstrns_sp(
    i_div               IN      VARCHAR2,
    i_crp_cd            IN      NUMBER,
    o_rstrn_parms_cur   OUT     SYS_REFCURSOR,
    o_test_ord_src_cur  OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SETUP_PK.GET_RSTRNS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get Restriction Parms Cursor');
    get_rstrn_parms_sp(l_div_part, i_crp_cd, o_rstrn_parms_cur);
    logs.dbg('Get Test OrdSrc Cursor');
    get_test_ord_src_sp(i_crp_cd, o_test_ord_src_cur);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_rstrns_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_RSTRNS_SP
  ||  Enable/Disable restrictions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/02/17 | rhalpai | Original - PIR14910
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_rstrns_sp(
    i_div                 IN  VARCHAR2,
    i_add_on_ord_sw       IN  VARCHAR2,
    i_crp_cd              IN  NUMBER,
    i_rstr_add_ln_sw      IN  VARCHAR2,
    i_rstr_chg_cust_sw    IN  VARCHAR2,
    i_rstr_chg_qty_sw     IN  VARCHAR2,
    i_rstr_del_ln_sw      IN  VARCHAR2,
    i_rstr_create_ord_sw  IN  VARCHAR2,
    i_excl_repl_sw        IN  VARCHAR2,
    i_ord_src_sw_list     IN  VARCHAR2,
    i_user_id             IN  VARCHAR2
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm := 'OP_SETUP_PK.UPD_RSTRNS_SP';
    lar_parm                  logs.tar_parm;
    l_div_part                NUMBER;
    l_crp_cd                  VARCHAR2(3);
    l_t_grps                  type_stab     := type_stab();
    l_c_add_ln       CONSTANT VARCHAR2(30)  := 'RESTRICT_ADDLN_';
    l_c_chg_cust     CONSTANT VARCHAR2(30)  := 'RESTRICT_CNUMCHG_';
    l_c_chg_ord_qty  CONSTANT VARCHAR2(30)  := 'RESTRICT_CORDQTY_';
    l_c_del_ln       CONSTANT VARCHAR2(30)  := 'RESTRICT_DELLN_';
    l_c_create_ord   CONSTANT VARCHAR2(30)  := 'RESTRICT_ORDCR8_';
    l_c_excl_repl    CONSTANT VARCHAR2(30)  := 'EXCL_REPL_';

    PROCEDURE rstr_sp(
      i_typ  IN  VARCHAR2,
      i_sw   IN  VARCHAR2
    ) IS
      l_col_typ             appl_sys_parm_ap1s.col_typ%TYPE;
      l_appl_id             appl_sys_parm_ap1s.appl_id%TYPE;
      l_parm_id             appl_sys_parm_ap1s.parm_id%TYPE;
      l_new_val             appl_sys_parm_ap1s.vchar_val%TYPE;
      l_curr_val            appl_sys_parm_ap1s.vchar_val%TYPE;
      l_mc_val              appl_sys_parm_ap1s.vchar_val%TYPE;
      l_c_off_val  CONSTANT VARCHAR2(3)                         := '000';
    BEGIN
      IF i_typ = l_c_excl_repl THEN
        l_col_typ := op_parms_pk.g_c_int;
        l_appl_id := op_parms_pk.g_c_op;
      ELSE
        l_col_typ := op_parms_pk.g_c_vchr;
        l_appl_id := op_parms_pk.g_c_csr;
      END IF;   -- i_typ = l_c_excl_repl

      l_parm_id := i_typ || l_crp_cd;
      l_new_val :=(CASE i_sw
                     WHEN 'Y' THEN l_crp_cd
                     WHEN 'N' THEN l_c_off_val
                   END);
      l_curr_val := LPAD(NVL(op_parms_pk.val_fn(l_div_part, l_parm_id, l_appl_id), l_c_off_val), 3, '0');

      IF l_curr_val <> l_new_val THEN
        l_mc_val := LPAD(NVL(op_parms_pk.val_fn(0, l_parm_id, l_appl_id), l_c_off_val), 3, '0');

        IF (   (    i_sw = 'Y'
                AND l_curr_val = l_c_off_val
                AND l_curr_val <> l_mc_val)
            OR (    i_sw = 'N'
                AND l_mc_val = l_c_off_val)
           ) THEN
          op_parms_pk.del_sp(l_div_part, l_parm_id, l_appl_id);
        ELSE
          op_parms_pk.merge_sp(l_div_part, l_parm_id, l_col_typ, l_new_val, i_user_id, l_appl_id, 'CRP');
        END IF;
      END IF;   -- l_curr_val <> l_new_val
    END rstr_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'AddOnOrdSw', i_add_on_ord_sw);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'RstrAddLnSw', i_rstr_add_ln_sw);
    logs.add_parm(lar_parm, 'RstrChgCustSw', i_rstr_chg_cust_sw);
    logs.add_parm(lar_parm, 'RstrChgQtySw', i_rstr_chg_qty_sw);
    logs.add_parm(lar_parm, 'RstrDelLnSw', i_rstr_del_ln_sw);
    logs.add_parm(lar_parm, 'RstrCreateOrdSw', i_rstr_create_ord_sw);
    logs.add_parm(lar_parm, 'ExclReplSw', i_excl_repl_sw);
    logs.add_parm(lar_parm, 'OrdSrcSwList', i_ord_src_sw_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Div cannot be NULL');
    excp.assert((i_add_on_ord_sw IS NOT NULL), 'AddOnOrdSw must be Y or N');
    excp.assert((i_crp_cd IS NOT NULL), 'CrpCd cannot be NULL');
    excp.assert((i_rstr_chg_cust_sw IS NOT NULL), 'RstrChgCustSw must be Y or N');
    excp.assert((i_rstr_chg_qty_sw IS NOT NULL), 'RstrChgQtySw must be Y or N');
    excp.assert((i_rstr_del_ln_sw IN('Y', 'N')), 'RstrDelLnSw must be Y or N');
    excp.assert((i_rstr_create_ord_sw IN('Y', 'N')), 'RstrCreateOrdSw must be Y or N');
    excp.assert((i_excl_repl_sw IN('Y', 'N')), 'ExclReplSw must be Y or N');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_crp_cd := lpad_fn(i_crp_cd, 3, '0');
    logs.dbg('Override OrdSrc ADK TO KEY in OrdRcpt');

    IF i_add_on_ord_sw = 'Y' THEN
      op_parms_pk.merge_sp(l_div_part, 'ADD_ON_ORD', op_parms_pk.g_c_vchr, 'Y', i_user_id, op_parms_pk.g_c_op);
    ELSIF i_add_on_ord_sw = 'N' THEN
      op_parms_pk.del_sp(l_div_part, 'ADD_ON_ORD', op_parms_pk.g_c_op);
    END IF;   -- i_add_on_ord_sw = 'Y'

    logs.dbg('Restrict Add OrdLn');
    rstr_sp(l_c_add_ln, i_rstr_add_ln_sw);
    logs.dbg('Restrict Chg CustId');
    rstr_sp(l_c_chg_cust, i_rstr_chg_cust_sw);
    logs.dbg('Restrict Chg OrdQty');
    rstr_sp(l_c_chg_ord_qty, i_rstr_chg_qty_sw);
    logs.dbg('Restrict Del OrdLn');
    rstr_sp(l_c_del_ln, i_rstr_del_ln_sw);
    logs.dbg('Restrict Create Order');
    rstr_sp(l_c_create_ord, i_rstr_create_ord_sw);
    logs.dbg('Exclude Replacements');
    rstr_sp(l_c_excl_repl, i_excl_repl_sw);

    IF i_ord_src_sw_list IS NOT NULL THEN
      logs.dbg('Parse Groups');
      l_t_grps := str.parse_list(i_ord_src_sw_list, op_const_pk.grp_delimiter);

      IF l_t_grps IS NOT NULL THEN
        logs.dbg('Upd TestOrdSrcCrpCd');
        FORALL i IN l_t_grps.FIRST .. l_t_grps.LAST
          UPDATE test_ord_src_crp_cd t
             SET t.enable_sw = SUBSTR(l_t_grps(i), -1),
                 t.user_id = i_user_id,
                 t.last_chg_ts = SYSDATE
           WHERE t.crp_cd = i_crp_cd
             AND t.ord_src = SUBSTR(l_t_grps(i), 1, INSTR(l_t_grps(i), op_const_pk.field_delimiter) - 1)
             AND t.enable_sw <> SUBSTR(l_t_grps(i), -1);
      END IF;   -- l_t_grps IS NOT NULL
    END IF;   -- i_ord_src_sw_list IS NOT NULL

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_rstrns_sp;
END op_setup_pk;
/

