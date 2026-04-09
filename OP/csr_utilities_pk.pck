CREATE OR REPLACE PACKAGE csr_utilities_pk IS
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
  FUNCTION is_action_restricted_fn(
    i_div       IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN INTEGER;

  FUNCTION is_order_mntn_restricted_fn(
    i_div            IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_prcs_id        IN  VARCHAR2,
    i_prcs_sbtyp_cd  IN  VARCHAR2
  )
    RETURN INTEGER;

  FUNCTION get_valid_load_types_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_order_statuses_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_max_create_lines_fn(
    i_div  IN  VARCHAR2
  )
    RETURN PLS_INTEGER;

  FUNCTION get_max_add_lines_fn(
    i_div  IN  VARCHAR2
  )
    RETURN PLS_INTEGER;

  FUNCTION get_orders_mntn_by_user_fn(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_reason_desc_fn(
    i_rsn_cd  IN  VARCHAR2
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE get_customer_restrictions_sp(
    i_div          IN      VARCHAR2,
    i_mcl_cust     IN      VARCHAR2,
    o_create_sw    OUT     INTEGER,
    o_add_ln_sw    OUT     INTEGER,
    o_del_ln_sw    OUT     INTEGER,
    o_resend_sw    OUT     INTEGER,
    o_chg_cust_sw  OUT     INTEGER,
    o_chg_qty_sw   OUT     INTEGER
  );

  PROCEDURE get_max_limit_sp(
    i_div           IN      VARCHAR2,
    o_create_limit  OUT     INTEGER,
    o_add_limit     OUT     INTEGER
  );
END csr_utilities_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_utilities_pk IS
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
  || IS_ACTION_RESTRICTED_FN
  ||  For a given mclane customer number and the action type. Returns true if
  ||  the action is restriced and false otherwise.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/09/03 | SNAGABH | Original
  || 06/02/17 | rhalpai | Change to call OP_PARMS_PK.VALS_FOR_PRFX_FN for parms.
  ||                    | PIR14910
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_action_restricted_fn(
    i_div       IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.IS_ACTION_RESTRICTED_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_rstrct_corps     type_stab;
    l_rstrct_sw          INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_rstrct_corps := op_parms_pk.vals_for_prfx_fn(l_div_part,
                                                     (CASE i_actn_cd
                                                        WHEN 'CREATE' THEN op_const_pk.prm_rstr_ord_create
                                                        WHEN 'ADDLINE' THEN op_const_pk.prm_rstr_addln
                                                        WHEN 'DELLINE' THEN op_const_pk.prm_rstr_delln
                                                        WHEN 'RESEND' THEN op_const_pk.prm_rstr_resnd
                                                        WHEN 'CHANGECUST' THEN op_const_pk.prm_rstr_custid_chg
                                                        WHEN 'CHANGEQTY' THEN op_const_pk.prm_rstr_chg_ord_qty
                                                      END
                                                     ),
                                                     op_parms_pk.g_c_csr
                                                    );

    -- Look for the restriction type passed and
    -- convert it to actual restriction code in db
    SELECT NVL(MAX(1), 0)
      INTO l_rstrct_sw
      FROM TABLE(CAST(l_t_rstrct_corps AS type_stab)) t, mclp020b cx,
           (SELECT TO_NUMBER(t.column_value) AS corp_cd
              FROM TABLE(CAST(l_t_rstrct_corps AS type_stab)) t) v
     WHERE cx.div_part = l_div_part
       AND cx.mccusb = i_mcl_cust
       AND TO_NUMBER(t.column_value) = cx.corpb;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_rstrct_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_action_restricted_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_ORDER_MNTN_RESTRICTED_FN
  ||  Function to check if given activity is restricted for the Order Source.
  ||  This will initially be used for Cross-Dock orders for which Order
  ||  Maintenance is restricted.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/22/04 | SNAGABH | Original.
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_order_mntn_restricted_fn(
    i_div            IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_prcs_id        IN  VARCHAR2,
    i_prcs_sbtyp_cd  IN  VARCHAR2
  )
    RETURN INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.IS_ORDER_MNTN_RESTRICTED_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_rstrct_sw          INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'PrcsSbTypCd', i_prcs_sbtyp_cd);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    SELECT NVL(MAX(1), 0)
      INTO l_rstrct_sw
      FROM ordp100a a, sub_prcs_ord_src s
     WHERE a.div_part = l_div_part
       AND a.ordnoa = i_ord_num
       AND s.div_part = a.div_part
       AND s.prcs_id = i_prcs_id
       AND s.prcs_sbtyp_cd = i_prcs_sbtyp_cd
       AND s.ord_src = a.ipdtsa;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_rstrct_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_order_mntn_restricted_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_VALID_LOAD_TYPES_FN
  ||  Return list of valid load types (VARCHAR2 format).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/11/05 | snagabh | Initial creation.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_valid_load_types_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_VALID_LOAD_TYPES_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT DISTINCT TRIM(tratyc)
                  FROM mclp120c
                 WHERE div_part = l_div_part;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_valid_load_types_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ORDER_STATUSES_FN
  ||  Return list of valid order status codes with descriptions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/11/05 | snagabh | Initial creation.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_order_statuses_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_ORDER_STATUSES_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT 'I' AS stat_cd, 'MF' AS stat_cd2, 'Mainframe' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'O' AS stat_cd, 'U' AS stat_cd2, 'Unbilled' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'P' AS stat_cd, 'B' AS stat_cd2, 'Billed' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'R' AS stat_cd, 'B' AS stat_cd2, 'Billed' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'A' AS stat_cd, 'SH' AS stat_cd2, 'Shipped' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'S' AS stat_cd, 'S' AS stat_cd2, 'Suspended' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'C' AS stat_cd, 'C' AS stat_cd2, 'Cancelled' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'E' AS stat_cd, 'E' AS stat_cd2, 'Exception' AS stat_descr
         FROM DUAL;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_order_statuses_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_MAX_CREATE_LINES_FN
  ||  Return a integer value that specifies the Max Number of Order Lines
  ||  that can be create on a Create Order request.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/11/05 | snagabh | This replaces standalone CSR_GET_MAXLIMIT_SP procedure.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_max_create_lines_fn(
    i_div  IN  VARCHAR2
  )
    RETURN PLS_INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_MAX_CREATE_LINES_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_create_max         PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_create_max := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_max_create_lns, op_parms_pk.g_c_csr), '0');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_create_max);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_max_create_lines_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_MAX_ADD_LINES_FN
  ||  Return a integer value that specifies the Max Number of Order Lines
  ||  that can be Added to an existing order (Maintenance).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/11/05 | snagabh | This replaces standalone CSR_GET_MAXLIMIT_SP procedure.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_max_add_lines_fn(
    i_div  IN  VARCHAR2
  )
    RETURN PLS_INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_MAX_ADD_LINES_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_add_max            PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_add_max := NVL(TO_NUMBER(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_max_add_lns, op_parms_pk.g_c_csr)), 0);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_add_max);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_max_add_lines_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ORDERS_MNTN_BY_USER_FN
  ||  Return list of Order Number that the current user had opened or was in
  ||  in the process of creation, but not taken to a final status (Completed or
  ||  Cancelled or Suspended).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/09/05 | snagabh | Initial creation.
  || 03/06/06 | snagabh | Include table information.
  || 03/20/06 | snagabh | Updates to only return maintainable orders ('O','S','I') status.
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_orders_mntn_by_user_fn(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_ORDERS_MNTN_BY_USER_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT a.ordnoa AS ord_num, a.connba AS conf_num, 0 AS tbl
         FROM ordp100a a
        WHERE a.div_part = l_div_part
          AND a.stata IN('O', 'S', 'I')
          AND UPPER(TRIM(a.mntusa)) = UPPER(i_user_id);

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_orders_mntn_by_user_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_REASON_DESC_FN
  ||  Get reason description associated with the passed reason code.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/25/05 | snagabh | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_reason_desc_fn(
    i_rsn_cd  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_UTILITIES_PK.GET_REASON_DESC_FN';
    lar_parm             logs.tar_parm;
    l_rsn_descr          mclp140a.desca%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.dbg('ENTRY', lar_parm);

    SELECT MAX(a.desca)
      INTO l_rsn_descr
      FROM mclp140a a
     WHERE a.rsncda = i_rsn_cd;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_rsn_descr);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_reason_desc_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_CUSTOMER_RESTRICTIONS_SP
  ||  Procedure to return all restrictions in CSR for a given customer.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/09/05 | SNAGABH | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_customer_restrictions_sp(
    i_div          IN      VARCHAR2,
    i_mcl_cust     IN      VARCHAR2,
    o_create_sw    OUT     INTEGER,
    o_add_ln_sw    OUT     INTEGER,
    o_del_ln_sw    OUT     INTEGER,
    o_resend_sw    OUT     INTEGER,
    o_chg_cust_sw  OUT     INTEGER,
    o_chg_qty_sw   OUT     INTEGER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_CUSTOMER_RESTRICTIONS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    o_create_sw := is_action_restricted_fn(i_div, 'CREATE', i_mcl_cust);
    o_add_ln_sw := is_action_restricted_fn(i_div, 'ADDLINE', i_mcl_cust);
    o_del_ln_sw := is_action_restricted_fn(i_div, 'DELLINE', i_mcl_cust);
    o_resend_sw := is_action_restricted_fn(i_div, 'RESEND', i_mcl_cust);
    o_chg_cust_sw := is_action_restricted_fn(i_div, 'CHANGECUST', i_mcl_cust);
    o_chg_qty_sw := is_action_restricted_fn(i_div, 'CHANGEQTY', i_mcl_cust);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_customer_restrictions_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_MAX_LIMIT_SP
  ||  Procedure to return order create and add limits.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/09/05 | SNAGABH | Original
  || 10/25/16 | rhalpai | Moved logic from CSR_GET_MAXLIMIT_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_max_limit_sp(
    i_div           IN      VARCHAR2,
    o_create_limit  OUT     INTEGER,
    o_add_limit     OUT     INTEGER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_UTILITIES_PK.GET_MAX_LIMIT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    o_create_limit := NVL(TO_NUMBER(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_max_create_lns, op_parms_pk.g_c_csr)),
                          0
                         );
    o_add_limit := NVL(TO_NUMBER(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_max_add_lns, op_parms_pk.g_c_csr)), 0);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_max_limit_sp;
END csr_utilities_pk;
/

