CREATE OR REPLACE PACKAGE op_rpt_parm_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION rpt_nm_cur_fn(
    i_div          IN  VARCHAR2,
    i_rpt_nm_prfx  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION rpt_parm_cur_fn(
    i_div     IN  VARCHAR2,
    i_rpt_nm  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION item_demand_cur_fn(
    i_div       IN  VARCHAR2,
    i_rpt_nm    IN  VARCHAR2,
    i_llr_from  IN  VARCHAR2,
    i_llr_to    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE add_rpt_sp(
    i_div            IN      VARCHAR2,
    i_rpt_nm_prfx    IN      VARCHAR2,
    i_rpt_descr      IN      VARCHAR2,
    i_ord_typ        IN      VARCHAR2,
    i_mcl_item_list  IN      VARCHAR2,
    i_mcl_cust_list  IN      VARCHAR2,
    i_grp_list       IN      VARCHAR2,
    i_crp_cd_list    IN      VARCHAR2,
    i_state_list     IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  );

  PROCEDURE chg_rpt_sp(
    i_div            IN      VARCHAR2,
    i_rpt_nm         IN      VARCHAR2,
    i_rpt_descr      IN      VARCHAR2,
    i_ord_typ        IN      VARCHAR2,
    i_mcl_item_list  IN      VARCHAR2,
    i_mcl_cust_list  IN      VARCHAR2,
    i_grp_list       IN      VARCHAR2,
    i_crp_cd_list    IN      VARCHAR2,
    i_state_list     IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  );

  PROCEDURE del_rpt_sp(
    i_div     IN      VARCHAR2,
    i_rpt_nm  IN      VARCHAR2,
    o_msg     OUT     VARCHAR2
  );
END op_rpt_parm_pk;
/

CREATE OR REPLACE PACKAGE BODY op_rpt_parm_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_rpt_nm_prfx  CONSTANT VARCHAR2(8) := 'ITEMDMND';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || PARM_TYP_CUR_FN
  ||  Return cursor of parameter types
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  FUNCTION parm_typ_cur_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.PARM_TYP_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT t.column_value AS parm_typ
         FROM TABLE(type_stab('ORDTYP', 'ITEM', 'CUST', 'GROUP', 'CORP', 'STATE')) t;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END parm_typ_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || RPT_NM_EXISTS_FN
  ||  Indicate whether report name exists.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  FUNCTION rpt_nm_exists_fn(
    i_div_part  IN  NUMBER,
    i_rpt_nm    IN  VARCHAR2
  )
    RETURN BOOLEAN IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.RPT_NM_EXISTS_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_exists_sw          VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM rpt_name_ap7r rn
        WHERE rn.div_part = i_div_part
          AND rn.rpt_nm = i_rpt_nm;

    FETCH l_cv
     INTO l_exists_sw;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_exists_sw = 'Y');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rpt_nm_exists_fn;

  /*
  ||----------------------------------------------------------------------------
  || RPT_DESCR_EXISTS_FN
  ||  Indicate whether report description exists.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  FUNCTION rpt_descr_exists_fn(
    i_div_part     IN  NUMBER,
    i_rpt_nm_prfx  IN  VARCHAR2,
    i_rpt_descr    IN  VARCHAR2,
    i_excl_rpt_nm  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN BOOLEAN IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.RPT_DESCR_EXISTS_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_exists_sw          VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNmPrfx', i_rpt_nm_prfx);
    logs.add_parm(lar_parm, 'RptDescr', i_rpt_descr);
    logs.add_parm(lar_parm, 'ExclRptNm', i_excl_rpt_nm);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM rpt_name_ap7r rn
        WHERE rn.div_part = i_div_part
          AND rn.rpt_nm LIKE i_rpt_nm_prfx || '%'
          AND rn.descr = i_rpt_descr
          AND (   i_excl_rpt_nm IS NULL
               OR rn.rpt_nm <> i_excl_rpt_nm);

    FETCH l_cv
     INTO l_exists_sw;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_exists_sw = 'Y');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rpt_descr_exists_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_RPT_NAME_SP
  ||  Add report name entry.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_rpt_name_sp(
    i_div_part   IN  NUMBER,
    i_rpt_nm     IN  VARCHAR2,
    i_rpt_descr  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.INS_RPT_NAME_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'RptDescr', i_rpt_descr);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    INSERT INTO rpt_name_ap7r
                (div_part, rpt_nm, descr, user_id, last_chg_ts
                )
         VALUES (i_div_part, i_rpt_nm, i_rpt_descr, i_user_id, SYSDATE
                );

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_rpt_name_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_RPT_NAME_SP
  ||  Change report name entry.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_rpt_name_sp(
    i_div_part   IN  NUMBER,
    i_rpt_nm     IN  VARCHAR2,
    i_rpt_descr  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.UPD_RPT_NAME_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'RptDescr', i_rpt_descr);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    UPDATE rpt_name_ap7r rn
       SET rn.descr = i_rpt_descr,
           rn.user_id = i_user_id,
           rn.last_chg_ts = l_c_sysdate
     WHERE rn.div_part = i_div_part
       AND rn.rpt_nm = i_rpt_nm
       AND rn.descr <> i_rpt_descr;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_rpt_name_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_RPT_NAME_SP
  ||  Remove report name entry.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_rpt_name_sp(
    i_div_part  IN  NUMBER,
    i_rpt_nm    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.DEL_RPT_NAME_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.dbg('ENTRY', lar_parm);

    DELETE FROM rpt_name_ap7r rn
          WHERE rn.div_part = i_div_part
            AND rn.rpt_nm = i_rpt_nm;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_rpt_name_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_RPT_PARM_SP
  ||  Add report parm entries.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_rpt_parm_sp(
    i_div_part   IN  NUMBER,
    i_rpt_nm     IN  VARCHAR2,
    i_rpt_typ    IN  VARCHAR2,
    i_t_rpt_val  IN  type_stab,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.INS_RPT_PARM_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'RptTyp', i_rpt_typ);
    logs.add_parm(lar_parm, 'RptValTab', i_t_rpt_val);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF (    i_t_rpt_val IS NOT NULL
        AND i_t_rpt_val.COUNT > 0) THEN
      logs.dbg('Add Report Parm Entries');
      FORALL i IN i_t_rpt_val.FIRST .. i_t_rpt_val.LAST
        INSERT INTO rpt_parm_ap1e
                    (div_part, rpt_nm, rpt_typ, val_cd, user_id, last_chg_ts
                    )
             VALUES (i_div_part, i_rpt_nm, i_rpt_typ, i_t_rpt_val(i), i_user_id, l_c_sysdate
                    );
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_rpt_parm_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_RPT_PARMS_SP
  ||  Remove all report parm entries for report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_rpt_parms_sp(
    i_div_part  IN  NUMBER,
    i_rpt_nm    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.DEL_RPT_PARMS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.dbg('ENTRY', lar_parm);

    DELETE FROM rpt_parm_ap1e rp
          WHERE rp.div_part = i_div_part
            AND rp.rpt_nm = i_rpt_nm;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_rpt_parms_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_RPT_PARM_SP
  ||  Add report parm entries for parm type.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_rpt_parm_sp(
    i_div_part  IN  NUMBER,
    i_rpt_nm    IN  VARCHAR2,
    i_parm_typ  IN  VARCHAR2,
    i_list      IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.ADD_RPT_PARM_SP';
    lar_parm             logs.tar_parm;
    l_list               typ.t_maxvc2;
    l_t_parms            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'ParmTyp', i_parm_typ);
    logs.add_parm(lar_parm, 'List', i_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    l_list := REPLACE(i_list, ' ');

    IF l_list IS NOT NULL THEN
      logs.dbg('Parse list');
      l_t_parms := str.parse_list(l_list);
      logs.dbg('Add report parm entries');
      ins_rpt_parm_sp(i_div_part, i_rpt_nm, i_parm_typ, l_t_parms, i_user_id);
    END IF;   -- l_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_rpt_parm_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_RPT_PARMS_SP
  ||  Add report parms.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_rpt_parms_sp(
    i_div_part       IN  NUMBER,
    i_rpt_nm         IN  VARCHAR2,
    i_ord_typ        IN  VARCHAR2,
    i_mcl_item_list  IN  VARCHAR2,
    i_mcl_cust_list  IN  VARCHAR2,
    i_grp_list       IN  VARCHAR2,
    i_crp_cd_list    IN  VARCHAR2,
    i_state_list     IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.ADD_RPT_PARMS_SP';
    lar_parm              logs.tar_parm;
    l_c_ord_typ  CONSTANT VARCHAR2(6)   := 'ORDTYP';
    l_c_item     CONSTANT VARCHAR2(4)   := 'ITEM';
    l_c_cust     CONSTANT VARCHAR2(4)   := 'CUST';
    l_c_group    CONSTANT VARCHAR2(5)   := 'GROUP';
    l_c_corp     CONSTANT VARCHAR2(4)   := 'CORP';
    l_c_state    CONSTANT VARCHAR2(5)   := 'STATE';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'MclItemList', i_mcl_item_list);
    logs.add_parm(lar_parm, 'MclCustList', i_mcl_cust_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CrpCdList', i_crp_cd_list);
    logs.add_parm(lar_parm, 'StateList', i_state_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_ord_typ IS NOT NULL THEN
      logs.dbg('Add report parm entry for order type');
      ins_rpt_parm_sp(i_div_part, i_rpt_nm, l_c_ord_typ, type_stab(i_ord_typ), i_user_id);
    END IF;   -- i_ord_typ IS NOT NULL

    logs.dbg('Add report parm entries for items');
    add_rpt_parm_sp(i_div_part, i_rpt_nm, l_c_item, i_mcl_item_list, i_user_id);
    logs.dbg('Add report parm entries for custs');
    add_rpt_parm_sp(i_div_part, i_rpt_nm, l_c_cust, i_mcl_cust_list, i_user_id);
    logs.dbg('Add report parm entries for groups');
    add_rpt_parm_sp(i_div_part, i_rpt_nm, l_c_group, i_grp_list, i_user_id);
    logs.dbg('Add report parm entries for corps');
    add_rpt_parm_sp(i_div_part, i_rpt_nm, l_c_corp, i_crp_cd_list, i_user_id);
    logs.dbg('Add report parm entries for states');
    add_rpt_parm_sp(i_div_part, i_rpt_nm, l_c_state, i_state_list, i_user_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_rpt_parms_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || RPT_NM_CUR_FN
  ||  Return cursor of report names
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  FUNCTION rpt_nm_cur_fn(
    i_div          IN  VARCHAR2,
    i_rpt_nm_prfx  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.RPT_NM_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptNmPrfx', i_rpt_nm_prfx);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   rn.rpt_nm, rn.descr,
                (SELECT rp.user_id
                   FROM rpt_parm_ap1e rp
                  WHERE rp.div_part = rn.div_part
                    AND rp.rpt_nm = rn.rpt_nm
                    AND ROWNUM = 1) AS user_id, TO_CHAR(rn.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts
           FROM div_mstr_di1d d, rpt_name_ap7r rn
          WHERE d.div_id = i_div
            AND rn.div_part = d.div_part
            AND rn.rpt_nm LIKE i_rpt_nm_prfx || '%'
            AND EXISTS(SELECT 1
                         FROM rpt_parm_ap1e rp
                        WHERE rp.div_part = rn.div_part
                          AND rp.rpt_nm = rn.rpt_nm)
       ORDER BY rn.rpt_nm;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rpt_nm_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || RPT_PARM_CUR_FN
  ||  Return cursor of report parameters
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  FUNCTION rpt_parm_cur_fn(
    i_div     IN  VARCHAR2,
    i_rpt_nm  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.RPT_PARM_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT t.column_value AS typ,
              to_list_fn(CURSOR(SELECT r.val_cd
                                  FROM div_mstr_di1d d, rpt_parm_ap1e r
                                 WHERE d.div_id = i_div
                                   AND r.div_part = d.div_part
                                   AND r.rpt_nm = i_rpt_nm
                                   AND r.rpt_typ = t.column_value
                               )
                        ) AS val_list
         FROM TABLE(type_stab('ORDTYP', 'ITEM', 'CUST', 'GROUP', 'CORP', 'STATE')) t;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rpt_parm_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_DEMAND_CUR_FN
  ||  Return Item Demand cursor.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_demand_cur_fn(
    i_div       IN  VARCHAR2,
    i_rpt_nm    IN  VARCHAR2,
    i_llr_from  IN  VARCHAR2,
    i_llr_to    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.ITEM_DEMAND_CUR_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'LLRFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LLRTo', i_llr_to);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    l_llr_from := TO_DATE(i_llr_from, 'YYYY-MM-DD');
    l_llr_to := TO_DATE(i_llr_to, 'YYYY-MM-DD');

    OPEN l_cv
     FOR
       SELECT   e.catite, e.shppke, e.sizee, e.ctdsce, w.aislc || w.binc || w.levlc AS slot, w.taxjrc, w.qavc,
                TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, SUM(b.ordqtb) AS ord_qty
           FROM rpt_parm_ap1e rp, sawp505e e, load_depart_op1f ld, ordp100a a, ordp120b b, mclp020b cx, sysp200c c,
                mclp030c ct, mclp110b di, whsp300c w
          WHERE rp.div_part = l_div_part
            AND rp.rpt_nm = i_rpt_nm
            AND rp.rpt_typ = 'ITEM'
            AND e.catite = rp.val_cd
            AND ld.div_part = l_div_part
            AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.excptn_sw = 'N'
            AND (   NOT EXISTS(SELECT 1
                                 FROM rpt_parm_ap1e rg
                                WHERE rg.div_part = l_div_part
                                  AND rg.rpt_nm = i_rpt_nm
                                  AND rg.rpt_typ = 'ORDTYP')
                 OR a.dsorda IN(SELECT rg.val_cd
                                  FROM rpt_parm_ap1e rg
                                 WHERE rg.div_part = l_div_part
                                   AND rg.rpt_nm = i_rpt_nm
                                   AND rg.rpt_typ = 'ORDTYP')
                )
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.itemnb = e.iteme
            AND b.sllumb = e.uome
            AND b.excptn_sw = 'N'
            AND b.statb = 'O'
            AND b.ntshpb IS NULL
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND (   NOT EXISTS(SELECT 1
                                 FROM rpt_parm_ap1e rg
                                WHERE rg.div_part = l_div_part
                                  AND rg.rpt_nm = i_rpt_nm
                                  AND rg.rpt_typ = 'CUST')
                 OR cx.mccusb IN(SELECT rg.val_cd
                                   FROM rpt_parm_ap1e rg
                                  WHERE rg.div_part = l_div_part
                                    AND rg.rpt_nm = i_rpt_nm
                                    AND rg.rpt_typ = 'CUST')
                )
            AND (   NOT EXISTS(SELECT 1
                                 FROM rpt_parm_ap1e rg
                                WHERE rg.div_part = l_div_part
                                  AND rg.rpt_nm = i_rpt_nm
                                  AND rg.rpt_typ = 'CORP')
                 OR cx.corpb IN(SELECT TO_NUMBER(rg.val_cd)
                                  FROM rpt_parm_ap1e rg
                                 WHERE rg.div_part = l_div_part
                                   AND rg.rpt_nm = i_rpt_nm
                                   AND rg.rpt_typ = 'CORP')
                )
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND (   NOT EXISTS(SELECT 1
                                 FROM rpt_parm_ap1e rg
                                WHERE rg.div_part = l_div_part
                                  AND rg.rpt_nm = i_rpt_nm
                                  AND rg.rpt_typ = 'GROUP')
                 OR c.retgpc IN(SELECT i_div || LPAD(rg.val_cd, 3, '0')
                                  FROM rpt_parm_ap1e rg
                                 WHERE rg.div_part = l_div_part
                                   AND rg.rpt_nm = i_rpt_nm
                                   AND rg.rpt_typ = 'GROUP')
                )
            AND ct.div_part = a.div_part
            AND ct.custc = a.custa
            AND (   NOT EXISTS(SELECT 1
                                 FROM rpt_parm_ap1e rg
                                WHERE rg.div_part = l_div_part
                                  AND rg.rpt_nm = i_rpt_nm
                                  AND rg.rpt_typ = 'STATE')
                 OR ct.taxjrc IN(SELECT rg.val_cd
                                   FROM rpt_parm_ap1e rg
                                  WHERE rg.div_part = l_div_part
                                    AND rg.rpt_nm = i_rpt_nm
                                    AND rg.rpt_typ = 'STATE')
                )
            AND di.div_part = l_div_part
            AND di.itemb = e.iteme
            AND di.uomb = e.uome
            AND w.div_part = l_div_part
            AND w.itemc = DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb)
            AND w.uomc = DECODE(TRIM(di.suomb), NULL, di.uomb, di.suomb)
            AND (   w.taxjrc IS NULL
                 OR w.taxjrc = 'USB'
                 OR w.taxjrc = (SELECT md.stzond
                                  FROM mclp260d md
                                 WHERE md.div_part = l_div_part
                                   AND md.txjrd = ct.taxjrc)
                )
       GROUP BY e.catite, e.shppke, e.sizee, e.ctdsce, w.aislc, w.binc, w.levlc, w.taxjrc, w.qavc, ld.llr_dt
       ORDER BY e.catite, w.taxjrc, llr_dt;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_demand_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || ADD_RPT_SP
  ||  Add report entries for report name and report parms.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_rpt_sp(
    i_div            IN      VARCHAR2,
    i_rpt_nm_prfx    IN      VARCHAR2,
    i_rpt_descr      IN      VARCHAR2,
    i_ord_typ        IN      VARCHAR2,
    i_mcl_item_list  IN      VARCHAR2,
    i_mcl_cust_list  IN      VARCHAR2,
    i_grp_list       IN      VARCHAR2,
    i_crp_cd_list    IN      VARCHAR2,
    i_state_list     IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm               := 'OP_RPT_PARM_PK.ADD_RPT_SP';
    lar_parm                  logs.tar_parm;
    l_div_part                NUMBER;
    l_rpt_descr               rpt_name_ap7r.descr%TYPE;
    l_rpt_nm                  rpt_name_ap7r.rpt_nm%TYPE;
    l_c_rpt_user_id  CONSTANT VARCHAR2(8)                 := 'STAT_INQ';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptNmPrfx', i_rpt_nm_prfx);
    logs.add_parm(lar_parm, 'RptDescr', i_rpt_descr);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'MclItemList', i_mcl_item_list);
    logs.add_parm(lar_parm, 'MclCustList', i_mcl_cust_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CrpCdList', i_crp_cd_list);
    logs.add_parm(lar_parm, 'StateList', i_state_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_rpt_descr := UPPER(TRIM(i_rpt_descr));
    logs.dbg('Validate');
    o_msg :=(CASE
               WHEN l_rpt_descr IS NULL THEN 'Description is required!'
               WHEN rpt_descr_exists_fn(l_div_part, i_rpt_nm_prfx, l_rpt_descr) THEN 'Description already exists for another!'
               WHEN TRIM(i_mcl_item_list) IS NULL THEN 'At least one item is required!'
               WHEN 4000 < GREATEST(LENGTH(i_mcl_item_list),
                                    LENGTH(i_mcl_cust_list),
                                    LENGTH(i_grp_list),
                                    LENGTH(i_crp_cd_list),
                                    LENGTH(i_state_list)
                                   ) THEN 'List cannot contain more than 4000 characters!'
             END
            );

    IF o_msg IS NULL THEN
      logs.dbg('Assign unique report name');
      LOOP
        l_rpt_nm := i_rpt_nm_prfx || TO_CHAR(SYSDATE, 'YYMMDDHH24MISS');
        EXIT WHEN NOT rpt_nm_exists_fn(l_div_part, l_rpt_nm);
        DBMS_LOCK.sleep(1);
      END LOOP;
      logs.dbg('Add report entry');
      ins_rpt_name_sp(l_div_part, l_rpt_nm, l_rpt_descr, l_c_rpt_user_id);
      logs.dbg('Add report parm entries');
      add_rpt_parms_sp(l_div_part,
                       l_rpt_nm,
                       i_ord_typ,
                       i_mcl_item_list,
                       i_mcl_cust_list,
                       i_grp_list,
                       i_crp_cd_list,
                       i_state_list,
                       i_user_id
                      );
      COMMIT;
    END IF;   -- o_msg IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END add_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHG_RPT_SP
  ||  Change report entries for report name and report parms.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE chg_rpt_sp(
    i_div            IN      VARCHAR2,
    i_rpt_nm         IN      VARCHAR2,
    i_rpt_descr      IN      VARCHAR2,
    i_ord_typ        IN      VARCHAR2,
    i_mcl_item_list  IN      VARCHAR2,
    i_mcl_cust_list  IN      VARCHAR2,
    i_grp_list       IN      VARCHAR2,
    i_crp_cd_list    IN      VARCHAR2,
    i_state_list     IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm              := 'OP_RPT_PARM_PK.CHG_RPT_SP';
    lar_parm                  logs.tar_parm;
    l_div_part                NUMBER;
    l_rpt_descr               rpt_name_ap7r.descr%TYPE;
    l_c_rpt_user_id  CONSTANT VARCHAR2(8)                := 'STAT_INQ';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.add_parm(lar_parm, 'RptDescr', i_rpt_descr);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'MclItemList', i_mcl_item_list);
    logs.add_parm(lar_parm, 'MclCustList', i_mcl_cust_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CrpCdList', i_crp_cd_list);
    logs.add_parm(lar_parm, 'StateList', i_state_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_rpt_descr := UPPER(TRIM(i_rpt_descr));
    logs.dbg('Validate');
    o_msg :=(CASE
               WHEN l_rpt_descr IS NULL THEN 'Description is required!'
               WHEN rpt_descr_exists_fn(l_div_part, SUBSTR(i_rpt_nm, 1, 8), l_rpt_descr, i_rpt_nm) THEN 'Description already exists for another!'
               WHEN TRIM(i_mcl_item_list) IS NULL THEN 'At least one item is required!'
               WHEN 4000 < GREATEST(LENGTH(i_mcl_item_list),
                                    LENGTH(i_mcl_cust_list),
                                    LENGTH(i_grp_list),
                                    LENGTH(i_crp_cd_list),
                                    LENGTH(i_state_list)
                                   ) THEN 'List cannot contain more than 4000 characters!'
               WHEN NOT rpt_nm_exists_fn(l_div_part, i_rpt_nm) THEN 'Entry not found to update!'
             END
            );

    IF o_msg IS NULL THEN
      logs.dbg('Change report name');
      upd_rpt_name_sp(l_div_part, i_rpt_nm, l_rpt_descr, l_c_rpt_user_id);
      logs.dbg('Remove report parm entries');
      del_rpt_parms_sp(l_div_part, i_rpt_nm);
      logs.dbg('Add report parm entries');
      add_rpt_parms_sp(l_div_part,
                       i_rpt_nm,
                       i_ord_typ,
                       i_mcl_item_list,
                       i_mcl_cust_list,
                       i_grp_list,
                       i_crp_cd_list,
                       i_state_list,
                       i_user_id
                      );
      COMMIT;
    END IF;   -- o_msg IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END chg_rpt_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_RPT_SP
  ||  Remove report entries for report name and report parms.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/27/08 | rhalpai | Initial creation for PIR6128
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_rpt_sp(
    i_div     IN      VARCHAR2,
    i_rpt_nm  IN      VARCHAR2,
    o_msg     OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_RPT_PARM_PK.DEL_RPT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RptNm', i_rpt_nm);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Validate');
    o_msg :=(CASE
               WHEN NOT rpt_nm_exists_fn(l_div_part, i_rpt_nm) THEN 'Entry not found to update!'
             END);

    IF o_msg IS NULL THEN
      logs.dbg('Remove report parm entries');
      del_rpt_parms_sp(l_div_part, i_rpt_nm);
      logs.dbg('Remove report name entry');
      del_rpt_name_sp(l_div_part, i_rpt_nm);
      COMMIT;
    END IF;   -- o_msg IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END del_rpt_sp;
END op_rpt_parm_pk;
/

