CREATE OR REPLACE PACKAGE op_protected_inventory_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_PROTECTED_INVENTORY_PK
  ||   This package is used to maintain Protected Inventory.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 02/03/12 | rhalpai | Reformatted
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_prtctd_inv IS RECORD(
    prtctd_id           NUMBER      DEFAULT -1,
    prtctd_item_qty     PLS_INTEGER DEFAULT 0,
    prtctd_cust_qty     PLS_INTEGER DEFAULT 0,
    ord_qty             PLS_INTEGER,
    item_avail_qty      PLS_INTEGER,
    item_adj_avail_qty  PLS_INTEGER DEFAULT 0
  );

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  -- Public Variable Declarations
  g_err_msg                     VARCHAR2(80);
  -- Public Constant Declarations
  g_c_rlse             CONSTANT VARCHAR2(3)  := 'RLS';
  g_c_pick             CONSTANT VARCHAR2(3)  := 'PCK';
  g_c_backout_ship     CONSTANT VARCHAR2(3)  := 'BKS';
  g_c_backout_rlse     CONSTANT VARCHAR2(3)  := 'BKR';
  g_c_conditional_sub  CONSTANT PLS_INTEGER  := 4;

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION ord_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION grp_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION prtctd_inv_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION prtctd_inv_fn(
    i_r_prtctd_inv  IN  prtctd_inv_op1i%ROWTYPE
  )
    RETURN prtctd_inv_op1i%ROWTYPE;

  FUNCTION prtctd_item_qty_fn(
    i_div        IN  VARCHAR2,
    i_zone_id    IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2,
    i_dt         IN  DATE,
    i_cbr_item   IN  VARCHAR2 DEFAULT NULL,
    i_uom        IN  VARCHAR2 DEFAULT NULL
  )
    RETURN PLS_INTEGER;

  FUNCTION prtctd_inv_info_fn(
    i_div        IN  VARCHAR2,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_avail_qty  IN  NUMBER DEFAULT NULL
  )
    RETURN op_protected_inventory_pk.g_rt_prtctd_inv;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE expire_sp(
    i_div        IN      VARCHAR2,
    i_prtctd_id  IN      NUMBER,
    i_user_id    IN      VARCHAR2,
    o_stat       OUT     VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  );

  PROCEDURE upd_sp(
    i_div        IN      VARCHAR2,
    i_prtctd_id  IN      NUMBER,
    i_grp_id     IN      VARCHAR2,
    i_catlg_num  IN      VARCHAR2,
    i_eff_dt     IN      DATE,
    i_end_dt     IN      DATE,
    i_ord_typ    IN      VARCHAR2,
    i_zone_id    IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    o_stat       OUT     VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  );

  PROCEDURE ins_sp(
    i_div         IN      VARCHAR2,
    i_grp_id      IN      VARCHAR2,
    i_catlg_num   IN      VARCHAR2,
    i_prtctd_qty  IN      NUMBER,
    i_ord_typ     IN      VARCHAR2,
    i_eff_dt      IN      DATE,
    i_end_dt      IN      DATE,
    i_user_id     IN      VARCHAR2,
    o_prtctd_id   OUT     NUMBER,
    o_stat        OUT     VARCHAR2,
    o_err_msg     OUT     VARCHAR2
  );

  PROCEDURE upd_prtctd_inv_log_sp(
    i_div_part  IN  NUMBER,
    i_log_typ   IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  );

  PROCEDURE upd_log_for_partls_sp(
    i_div_part  IN  NUMBER,
    i_log_typ   IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  );

  PROCEDURE backout_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE,
    i_log_typ   IN  VARCHAR2
  );

  PROCEDURE cleanup_sp(
    i_div_part  IN      NUMBER,
    o_stat      OUT     VARCHAR2
  );
END op_protected_inventory_pk;
/

CREATE OR REPLACE PACKAGE BODY op_protected_inventory_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_PROTECTED_INVENTORY_PK
  ||   This package is used to maintain Protected Inventory.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes  (Please record only global changes here)
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  -- Public Variable Declarations
  g_valid_dates_stat                      VARCHAR2(80);
  g_valid_item_stat                       VARCHAR2(5);
  -- Public Constant Declarations
  g_c_dates_are_valid            CONSTANT g_valid_dates_stat%TYPE   := 'Good';
  g_c_item_is_valid              CONSTANT g_valid_item_stat%TYPE    := 'TRUE';
  g_c_item_is_invalid            CONSTANT g_valid_item_stat%TYPE    := 'FALSE';
  -- Error Messages
  g_c_process_restricted_msg     CONSTANT g_err_msg%TYPE      := 'PROCESS RESTRICTED AT THIS TIME - WAIT AND TRY AGAIN';
  g_c_end_date_before_begin_msg  CONSTANT g_err_msg%TYPE            := 'END DATE CANNOT BE BEFORE START DATE';
  g_c_end_date_before_today_msg  CONSTANT g_err_msg%TYPE            := 'END DATE CANNOT BE BEFORE TODAY';
  g_c_invalid_item_msg           CONSTANT g_err_msg%TYPE            := 'INVALID ITEM NUMBER';
  g_c_invalid_prtctd_id_msg      CONSTANT g_err_msg%TYPE            := 'INVALID PROTECT ID';
  g_c_prtctd_date_overlap_msg    CONSTANT g_err_msg%TYPE            := 'DATE OVERLAPS WITH EXISTING PROTECT ID';
  g_c_alloc_conflict_msg         CONSTANT g_err_msg%TYPE            := 'CANNOT CONTINUE WHILE ALLOCATE IS PROCESSING';
  g_c_restricted_upd_msg         CONSTANT g_err_msg%TYPE          := 'UPDATE NOT ALLOWED - CREATED BY SPECIAL SYSTEMID';
  g_c_in_use_by_billing_msg      CONSTANT g_err_msg%TYPE
                                                    := 'PROTECT ID IN USE BY BILLING - TRY AGAIN WHEN LOADS ARE CLOSED';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || IS_RESTRICTED_FN
  ||  Indicates whether maintenance on rows is restricted.
  ||  Rows with future effective dates that were created by special systemid's
  ||  are restricted.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/12/04 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_restricted_fn(
    i_div_part   IN  NUMBER,
    i_prtctd_id  IN  NUMBER,
    i_user_id    IN  VARCHAR2
  )
    RETURN BOOLEAN IS
    l_dt           DATE;
    l_t_system_id  type_stab;
    l_restrict_sw  VARCHAR2(1) := 'N';
  BEGIN
    l_dt := TRUNC(SYSDATE);
    l_t_system_id := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_prtctd_inv_systemid);

    IF i_user_id NOT MEMBER OF l_t_system_id THEN
      BEGIN
        SELECT 'Y'
          INTO l_restrict_sw
          FROM prtctd_inv_op1i i
         WHERE i.div_part = i_div_part
           AND i.prtctd_id = i_prtctd_id
           AND i.eff_dt > l_dt
           AND i.create_user IN(SELECT t.column_value
                                  FROM TABLE(l_t_system_id) t);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_restrict_sw := 'N';
      END;
    END IF;   -- i_user_id NOT MEMBER OF l_t_system_id

    RETURN(l_restrict_sw = 'Y');
  END is_restricted_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_IN_USE_BY_BILLING_FN
  ||  Indicates whether the protectID has been logged during allocation for an
  ||  order that has not yet shipped (load not closed).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/26/05 | rhalpai | Original
  || 02/28/06 | rhalpai | Changed to look at status of most recent log entry.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_in_use_by_billing_fn(
    i_div_part   IN  NUMBER,
    i_prtctd_id  IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_exist_sw  VARCHAR2(1) := 'N';
  BEGIN
    BEGIN
      SELECT 'Y'
        INTO l_exist_sw
        FROM prtctd_alloc_log_op1a a, ordp120b b
       WHERE a.div_part = i_div_part
         AND b.div_part = a.div_part
         AND b.ordnob = a.ord_num
         AND b.lineb = a.ln_num
         AND b.statb <> 'A'
         AND a.prtctd_id = i_prtctd_id
         AND a.stat_cd IN('RLS', 'PCK')
         AND a.tran_id = (SELECT MAX(a2.tran_id)
                            FROM prtctd_alloc_log_op1a a2
                           WHERE a2.div_part = a.div_part
                             AND a2.prtctd_id = a.prtctd_id
                             AND a2.ord_num = a.ord_num
                             AND a2.ln_num = a.ln_num);
    EXCEPTION
      WHEN TOO_MANY_ROWS THEN
        l_exist_sw := 'Y';
      WHEN NO_DATA_FOUND THEN
        l_exist_sw := 'N';
    END;

    RETURN(l_exist_sw = 'Y');
  END is_in_use_by_billing_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_VALID_ITEM_FN
  ||  Used to indicate whether the passed item is valid.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/10/05 | rhalpai | Changes to support Cig System as cig inventory master.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_valid_item_fn(
    i_div_part   IN  NUMBER,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm            := 'OP_PROTECTED_INVENTORY_PK.IS_VALID_ITEM_FN';
    lar_parm             logs.tar_parm;
    l_valid_sw           g_valid_item_stat%TYPE   := g_c_item_is_invalid;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Validate Item');

    SELECT g_c_item_is_valid
      INTO l_valid_sw
      FROM sawp505e e, mclp110b di
     WHERE e.catite = i_catlg_num
       AND di.div_part = i_div_part
       AND di.itemb = e.iteme
       AND di.uomb = e.uome
       AND di.statb = 'ACT';

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_valid_sw);
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN(l_valid_sw);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_valid_item_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_VALID_DATES_STATUS_FN
  ||  Used to validate start and end dates.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_valid_dates_status_fn(
    i_eff_dt  IN  DATE,
    i_end_dt  IN  DATE
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN((CASE
              WHEN i_end_dt < i_eff_dt THEN g_c_end_date_before_begin_msg
              WHEN i_end_dt < TRUNC(SYSDATE) THEN g_c_end_date_before_today_msg
              ELSE g_c_dates_are_valid
            END
           )
          );
  END get_valid_dates_status_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRTCTD_ID_FN
  ||  Return existing protection id.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 04/12/04 | rhalpai | Added logic to handle non-inventory items.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 12/17/12 | rhalpai | Change logic to remove check for EndDt between EffDt
  ||                    | and NewEndDt. IM-075605
  ||----------------------------------------------------------------------------
  */
  FUNCTION prtctd_id_fn(
    i_div        IN  VARCHAR2,
    i_zone_id    IN  VARCHAR2,
    i_grp_id     IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2,
    i_ord_typ    IN  VARCHAR2,
    i_eff_dt     IN  DATE,
    i_end_dt     IN  DATE
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_PROTECTED_INVENTORY_PK.PRTCTD_ID_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_zone_id            prtctd_inv_op1i.zone_id%TYPE;
    l_catlg_num          prtctd_inv_op1i.ord_item_num%TYPE;
    l_prtctd_id          NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ZoneId', i_zone_id);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'EffDt', i_eff_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.dbg('ENTRY', lar_parm);

    BEGIN
      l_div_part := div_pk.div_part_fn(i_div);
      l_zone_id := NVL(i_zone_id, i_div);
      l_catlg_num := op_item_pk.inv_item_fn(l_div_part, i_catlg_num);

      SELECT i.prtctd_id
        INTO l_prtctd_id
        FROM prtctd_inv_op1i i
       WHERE i.div_part = l_div_part
         AND i.zone_id = l_zone_id
         AND i.grp_id = i_grp_id
         AND i.ord_item_num = l_catlg_num
         AND i.ord_typ_cd = i_ord_typ
         AND (   i_eff_dt BETWEEN i.eff_dt AND i.end_dt
              OR i_end_dt BETWEEN i.eff_dt AND i.end_dt
              OR i.eff_dt BETWEEN i_eff_dt AND i_end_dt
             )
         AND i.stat_cd = 'ACT';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN OTHERS THEN
        logs.err(lar_parm, NULL, FALSE);
    END;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_prtctd_id);
  END prtctd_id_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ORD_TYP_LIST_FN
  ||  Used to retrieve a cursor of order types on ORDP991O.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/06/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROTECTED_INVENTORY_PK.ORD_TYP_LIST_FN';
    l_cv                 SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT   o.otypeo, o.otdsco
           FROM ordp991o o
       ORDER BY o.otdsco;

    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err;
  END ord_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GRP_LIST_FN
  ||  Return cursor of customer group information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 06/16/08 | rhalpai | Added sort by Grp to cursor.
  ||----------------------------------------------------------------------------
  */
  FUNCTION grp_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROTECTED_INVENTORY_PK.GRP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    l_cv := op_customer_pk.group_list_fn(i_div);
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END grp_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRTCTD_INV_LIST_FN
  ||  Return cursor of protected inventory info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/10/05 | rhalpai | Changes to support Cig System as cig inventory master.
  || 06/16/08 | rhalpai | Added sort by PrtctdId to cursor.
  ||----------------------------------------------------------------------------
  */
  FUNCTION prtctd_inv_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROTECTED_INVENTORY_PK.PRTCTD_INV_LIST_FN';
    lar_parm             logs.tar_parm;
    l_dt                 DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_dt := TRUNC(SYSDATE);

    OPEN l_cv
     FOR
       SELECT   i.prtctd_id, d.div_id, i.grp_id, a.group_name, i.ord_item_num, e.ctdsce, i.eff_dt, i.end_dt,
                i.ord_typ_cd, i.orig_qty, i.prtctd_qty,
                prtctd_item_qty_fn(d.div_id, i.zone_id, i.ord_item_num, l_dt) AS ttl_prtctd_qty,
                (SELECT MAX(c.qavc)
                   FROM whsp300c c
                  WHERE c.div_part = i.div_part
                    AND c.itemc = e.iteme
                    AND c.uomc = e.uome
                    AND c.taxjrc IS NULL) AS qavc, i.zone_id, NULL AS tax_jrsdctn, i.create_ts, i.create_user,
                i.last_chg_ts, i.user_id, i.stat_cd
           FROM div_mstr_di1d d, prtctd_inv_op1i i, mclp100a a, sawp505e e
          WHERE d.div_id = i_div
            AND i.div_part = d.div_part
            AND i.stat_cd = 'ACT'
            AND a.div_part = i.div_part
            AND a.cstgpa = i.grp_id
            AND e.catite = i.ord_item_num
       ORDER BY i.prtctd_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prtctd_inv_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRTCTD_INV_FN
  ||  Find and return an existing record of protected inventory.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/02/04 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||----------------------------------------------------------------------------
  */
  FUNCTION prtctd_inv_fn(
    i_r_prtctd_inv  IN  prtctd_inv_op1i%ROWTYPE
  )
    RETURN prtctd_inv_op1i%ROWTYPE IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_PROTECTED_INVENTORY_PK.PRTCTD_INV_FN';
    lar_parm             logs.tar_parm;
    l_div                div_mstr_di1d.div_id%TYPE;
    l_zone_id            prtctd_inv_op1i.zone_id%TYPE;
    l_ord_item           prtctd_inv_op1i.ord_item_num%TYPE;
    l_cv                 SYS_REFCURSOR;
    l_r_prtctd_inv       prtctd_inv_op1i%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_r_prtctd_inv.div_part);
    logs.add_parm(lar_parm, 'ZoneId', i_r_prtctd_inv.zone_id);
    logs.add_parm(lar_parm, 'GrpId', i_r_prtctd_inv.grp_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_r_prtctd_inv.ord_item_num);
    logs.add_parm(lar_parm, 'OrdTyp', i_r_prtctd_inv.ord_typ_cd);
    logs.add_parm(lar_parm, 'EffDt', i_r_prtctd_inv.eff_dt);
    logs.add_parm(lar_parm, 'EndDt', i_r_prtctd_inv.end_dt);
    logs.dbg('ENTRY', lar_parm);
    l_div := div_pk.div_id_fn(i_r_prtctd_inv.div_part);
    l_zone_id := NVL(i_r_prtctd_inv.zone_id, l_div);
    l_ord_item := op_item_pk.inv_item_fn(i_r_prtctd_inv.div_part, i_r_prtctd_inv.ord_item_num);

    OPEN l_cv
     FOR
       SELECT *
         FROM prtctd_inv_op1i i
        WHERE i.div_part = i_r_prtctd_inv.div_part
          AND i.zone_id = l_zone_id
          AND i.grp_id = i_r_prtctd_inv.grp_id
          AND i.ord_item_num = l_ord_item
          AND i.ord_typ_cd = i_r_prtctd_inv.ord_typ_cd
          AND (   i_r_prtctd_inv.eff_dt BETWEEN i.eff_dt AND i.end_dt
               OR i_r_prtctd_inv.end_dt BETWEEN i.eff_dt AND i.end_dt
              )
          AND i.stat_cd = 'ACT';

    FETCH l_cv
     INTO l_r_prtctd_inv;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_r_prtctd_inv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prtctd_inv_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRTCTD_ITEM_QTY_FN
  ||  Returns total inventory quantity protected for an item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 04/12/04 | rhalpai | Added logic to handle non-inventory items.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/10/05 | rhalpai | Changes to support Cig System as cig inventory master.
  ||----------------------------------------------------------------------------
  */
  FUNCTION prtctd_item_qty_fn(
    i_div        IN  VARCHAR2,
    i_zone_id    IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2,
    i_dt         IN  DATE,
    i_cbr_item   IN  VARCHAR2 DEFAULT NULL,
    i_uom        IN  VARCHAR2 DEFAULT NULL
  )
    RETURN PLS_INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_PROTECTED_INVENTORY_PK.PRTCTD_ITEM_QTY_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_zone_id            prtctd_inv_op1i.zone_id%TYPE;
    l_catlg_num          sawp505e.catite%TYPE;
    l_inv_item           prtctd_inv_op1i.ord_item_num%TYPE;
    l_prtctd_item_qty    PLS_INTEGER                         := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ZoneId', i_zone_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'Dt', i_dt);
    logs.add_parm(lar_parm, 'CbrItem', i_cbr_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_zone_id := NVL(i_zone_id, i_div);
    l_catlg_num :=(CASE
                     WHEN i_cbr_item IS NULL THEN i_catlg_num
                     ELSE op_item_pk.catlg_num_str_fn(i_cbr_item, i_uom)
                   END);
    logs.dbg('Get Inventory Item');
    l_inv_item := op_item_pk.inv_item_fn(l_div_part, l_catlg_num);
    logs.dbg('Get Total Protected for Order Item');

    SELECT NVL(SUM(i.prtctd_qty), 0)
      INTO l_prtctd_item_qty
      FROM prtctd_inv_op1i i
     WHERE i.div_part = l_div_part
       AND i.ord_item_num = l_inv_item
       AND i.zone_id = l_zone_id
       AND i_dt BETWEEN i.eff_dt AND i.end_dt
       AND i.stat_cd = 'ACT';

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_prtctd_item_qty);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prtctd_item_qty_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRTCTD_INV_INFO_FN
  ||  Return record of protected inventory info for an order line.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 04/12/04 | rhalpai | Added logic to handle non-inventory items and use
  ||                    | SYSDATE instead of LLR Date.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/10/05 | rhalpai | Changes to support Cig System as cig inventory master.
  || 07/04/13 | rhalpai | Convert cursor to use fields from OrdHdr. PIR11038
  || 10/02/13 | rhalpai | Change logic to use truncated sysdate. IM-118078
  ||----------------------------------------------------------------------------
  */
  FUNCTION prtctd_inv_info_fn(
    i_div        IN  VARCHAR2,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_avail_qty  IN  NUMBER DEFAULT NULL
  )
    RETURN op_protected_inventory_pk.g_rt_prtctd_inv IS
    l_c_module   CONSTANT typ.t_maxfqnm                             := 'OP_PROTECTED_INVENTORY_PK.PRTCTD_INV_INFO_FN';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_r_prtctd_inv        op_protected_inventory_pk.g_rt_prtctd_inv;
    l_grp_id              prtctd_inv_op1i.grp_id%TYPE;
    l_catlg_num           prtctd_inv_op1i.ord_item_num%TYPE;
    l_ord_typ             prtctd_inv_op1i.ord_typ_cd%TYPE;
    l_c_sysdate  CONSTANT DATE                                      := TRUNC(SYSDATE);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'AvailQty', i_avail_qty);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    BEGIN
      IF i_avail_qty IS NOT NULL THEN
        l_r_prtctd_inv.item_avail_qty := i_avail_qty;
      ELSE
        logs.dbg('Get Item Avail Qty');

        SELECT NVL((SELECT MAX(c.qavc)
                      FROM whsp300c c
                     WHERE c.div_part = di.div_part
                       AND c.itemc = e.iteme
                       AND c.uomc = e.uome
                       AND c.taxjrc IS NULL), 0) AS qav
          INTO l_r_prtctd_inv.item_avail_qty
          FROM ordp100a a, ordp120b b, mclp110b di, sawp505e e
         WHERE a.div_part = l_div_part
           AND a.ordnoa = i_ord_num
           AND b.div_part = a.div_part
           AND b.ordnob = i_ord_num
           AND b.lineb = i_ord_ln
           AND di.div_part = a.div_part
           AND di.itemb = b.itemnb
           AND di.uomb = b.sllumb
           AND e.iteme =(CASE
                           WHEN TRIM(di.suomb) IS NOT NULL THEN di.sitemb
                           ELSE di.itemb
                         END)
           AND e.uome =(CASE
                          WHEN TRIM(di.suomb) IS NOT NULL THEN di.suomb
                          ELSE di.uomb
                        END);
      END IF;   -- i_qty_avail IS NOT NULL

      logs.dbg('Retrieve Order Info');

      SELECT c.retgpc, e.catite, b.ordqtb, a.dsorda
        INTO l_grp_id, l_catlg_num, l_r_prtctd_inv.ord_qty, l_ord_typ
        FROM ordp120b b, ordp100a a, sysp200c c, mclp110b di, sawp505e e
       WHERE b.div_part = l_div_part
         AND b.ordnob = i_ord_num
         AND b.lineb = i_ord_ln
         AND a.div_part = l_div_part
         AND a.ordnoa = i_ord_num
         AND c.div_part = a.div_part
         AND c.acnoc = a.custa
         AND di.div_part = b.div_part
         AND di.itemb = b.itemnb
         AND di.uomb = b.sllumb
         AND e.iteme =(CASE
                         WHEN TRIM(di.suomb) IS NOT NULL THEN di.sitemb
                         ELSE di.itemb
                       END)
         AND e.uome =(CASE
                        WHEN TRIM(di.suomb) IS NOT NULL THEN di.suomb
                        ELSE di.uomb
                      END);

      logs.dbg('Get Protected Item Qty');
      l_r_prtctd_inv.prtctd_item_qty := prtctd_item_qty_fn(i_div, NULL, l_catlg_num, l_c_sysdate);

      BEGIN
        logs.dbg('Retrieve Customer Protected Inventory Info');

        SELECT i.prtctd_id, i.prtctd_qty
          INTO l_r_prtctd_inv.prtctd_id, l_r_prtctd_inv.prtctd_cust_qty
          FROM prtctd_inv_op1i i
         WHERE i.div_part = l_div_part
           AND i.zone_id = i_div
           AND i.grp_id = l_grp_id
           AND i.ord_item_num = l_catlg_num
           AND l_c_sysdate BETWEEN i.eff_dt AND i.end_dt
           AND i.ord_typ_cd = l_ord_typ
           AND i.stat_cd = 'ACT';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;   -- just continue (unprotected customer)
      END;

      logs.dbg('Determine Adjusted Inventory Qty Available');
      l_r_prtctd_inv.item_adj_avail_qty := l_r_prtctd_inv.item_avail_qty;

      IF l_r_prtctd_inv.prtctd_item_qty > 0 THEN
        IF l_r_prtctd_inv.prtctd_cust_qty > 0 THEN
          IF l_r_prtctd_inv.item_adj_avail_qty >= l_r_prtctd_inv.prtctd_cust_qty THEN
            IF l_r_prtctd_inv.ord_qty > l_r_prtctd_inv.prtctd_cust_qty THEN
              l_r_prtctd_inv.item_adj_avail_qty := l_r_prtctd_inv.item_adj_avail_qty - l_r_prtctd_inv.prtctd_item_qty;

              IF l_r_prtctd_inv.item_adj_avail_qty < 0 THEN
                l_r_prtctd_inv.item_adj_avail_qty := l_r_prtctd_inv.prtctd_cust_qty;
              ELSE
                l_r_prtctd_inv.item_adj_avail_qty := l_r_prtctd_inv.prtctd_cust_qty + l_r_prtctd_inv.item_adj_avail_qty;
              END IF;   -- l_r_prtctd_inv.item_adj_avail_qty < 0
            END IF;   -- l_r_prtctd_inv.ord_qty > l_r_prtctd_inv.prtctd_cust_qty
          END IF;   -- l_r_prtctd_inv.item_adj_avail_qty >= l_r_prtctd_inv.prtctd_cust_qty
        ELSE   -- unprotected cust
          l_r_prtctd_inv.item_adj_avail_qty := l_r_prtctd_inv.item_avail_qty - l_r_prtctd_inv.prtctd_item_qty;
        END IF;   -- l_r_prtctd_inv.prtctd_cust_qty > 0
      END IF;   -- l_r_prtctd_inv.prtctd_item_qty > 0
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN OTHERS THEN
        logs.err(lar_parm, NULL, FALSE);
        l_r_prtctd_inv.prtctd_item_qty := NULL;
    END;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_r_prtctd_inv);
  END prtctd_inv_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || EXPIRE_SP
  ||  Used to expire inventory protection on PRTCTD_INV_OP1I by setting the end
  ||  date to yesterday and setting the status to inactive.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 04/12/04 | rhalpai | Added logic to restrict maintenance on rows created by
  ||                    | special systemid's for future protection.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 05/26/05 | rhalpai | Added logic to restrict changes to a protectID that has
  ||                    | been logged during allocation for an order that is not
  ||                    | yet in shipped status. IM150222
  || 03/02/06 | rhalpai | Added process control logic. IM200261
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 02/12/20 | rhalpai | Remove SUBSTR of SQLERRM so entire SQLERRM is returned to o_err_msg. SDHD-622870
  ||----------------------------------------------------------------------------
  */
  PROCEDURE expire_sp(
    i_div        IN      VARCHAR2,
    i_prtctd_id  IN      NUMBER,
    i_user_id    IN      VARCHAR2,
    o_stat       OUT     VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_PROTECTED_INVENTORY_PK.EXPIRE_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_c_end_dt   CONSTANT DATE          := TRUNC(l_c_sysdate - 1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrtctdId', i_prtctd_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    o_stat := 'Good';
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    excp.assert(NOT is_restricted_fn(l_div_part, i_prtctd_id, i_user_id), g_c_restricted_upd_msg);
    excp.assert(NOT is_in_use_by_billing_fn(l_div_part, i_prtctd_id), g_c_in_use_by_billing_msg);
    logs.dbg('Update PRTCTD_INV_OP1I');

    UPDATE prtctd_inv_op1i i
       SET i.end_dt = l_c_end_dt,
           i.user_id = i_user_id,
           i.last_chg_ts = l_c_sysdate,
           i.stat_cd = 'INA'
     WHERE i.prtctd_id = i_prtctd_id
       AND i.div_part = l_div_part;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_stat := 'Error';
      o_err_msg := g_c_process_restricted_msg;
      logs.warn(SQLERRM, lar_parm);
    WHEN excp.gx_assert_fail THEN
      o_stat := 'Error';
      o_err_msg := SQLERRM;
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    WHEN OTHERS THEN
      logs.err(lar_parm, NULL, FALSE);
      o_err_msg := 'Unhandled Error: ' || SQLERRM;
      o_stat := 'Error';
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
  END expire_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||  Used to update inventory protection on PRTCTD_INV_OP1I.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 04/12/04 | rhalpai | Added logic to restrict maintenance on rows created by
  ||                    | special systemid's for future protection.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/10/05 | rhalpai | Changes to support Cig System as cig inventory master.
  || 05/26/05 | rhalpai | Added logic to restrict changes to a protectID that has
  ||                    | been logged during allocation for an order that is not
  ||                    | yet in shipped status. IM150222
  || 03/02/06 | rhalpai | Added process control logic. IM200261
  || 04/09/09 | VXRANGA | Venkateswaran Ranganathan updated for IM492071 -
  ||                    | Updated code block towards fix for above IM to handle
  ||                    | overlap date check.
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 12/17/12 | rhalpai | Change logic to remove check for EndDt between EffDt
  ||                    | and NewEndDt. IM-075605
  || 02/12/20 | rhalpai | Remove SUBSTR of SQLERRM so entire SQLERRM is returned to o_err_msg. SDHD-622870
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_div        IN      VARCHAR2,
    i_prtctd_id  IN      NUMBER,
    i_grp_id     IN      VARCHAR2,
    i_catlg_num  IN      VARCHAR2,
    i_eff_dt     IN      DATE,
    i_end_dt     IN      DATE,
    i_ord_typ    IN      VARCHAR2,
    i_zone_id    IN      VARCHAR2,
    i_user_id    IN      VARCHAR2,
    o_stat       OUT     VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                  := 'OP_PROTECTED_INVENTORY_PK.UPD_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                           := SYSDATE;
    l_div_part            NUMBER;
    l_zone_id             prtctd_inv_op1i.zone_id%TYPE;

    FUNCTION is_valid_prtctd_id_fn(
      i_div_part   IN  NUMBER,
      i_prtctd_id  IN  NUMBER
    )
      RETURN BOOLEAN IS
      l_exist_sw  VARCHAR2(1) := 'N';
    BEGIN
      BEGIN
        SELECT 'Y'
          INTO l_exist_sw
          FROM prtctd_inv_op1i i
         WHERE i.div_part = i_div_part
           AND i.prtctd_id = i_prtctd_id
           AND i.stat_cd = 'ACT';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_exist_sw := 'N';
      END;

      RETURN(l_exist_sw = 'Y');
    END is_valid_prtctd_id_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrtctdId', i_prtctd_id);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'EffDt', i_eff_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'ZoneId', i_zone_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    o_stat := 'Good';
    l_div_part := div_pk.div_part_fn(i_div);
    l_zone_id := NVL(i_zone_id, i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Perform Validation');
    excp.assert(NOT is_restricted_fn(l_div_part, i_prtctd_id, i_user_id), g_c_restricted_upd_msg);

    DECLARE
      l_valid_dt_stat  g_valid_dates_stat%TYPE;
    BEGIN
      l_valid_dt_stat := get_valid_dates_status_fn(i_eff_dt, i_end_dt);
      excp.assert((l_valid_dt_stat = g_c_dates_are_valid), TO_CHAR(i_end_dt, 'YYYY-MM-DD') || ' ' || l_valid_dt_stat);
    END;

    excp.assert((is_valid_item_fn(l_div_part, i_catlg_num) = g_c_item_is_valid),
                g_c_invalid_item_msg || ' ' || i_catlg_num
               );
    excp.assert(is_valid_prtctd_id_fn(l_div_part, i_prtctd_id), g_c_invalid_prtctd_id_msg || ' ' || i_prtctd_id);

    DECLARE
      l_inv_item   prtctd_inv_op1i.ord_item_num%TYPE;
      l_prtctd_id  NUMBER;
    BEGIN
      l_inv_item := op_item_pk.inv_item_fn(l_div_part, i_catlg_num);

      SELECT MAX(i.prtctd_id)
        INTO l_prtctd_id
        FROM prtctd_inv_op1i i
       WHERE i.div_part = l_div_part
         AND i.zone_id = l_zone_id
         AND i.grp_id = i_grp_id
         AND i.ord_item_num = l_inv_item
         AND i.ord_typ_cd = i_ord_typ
         AND (   i_eff_dt BETWEEN i.eff_dt AND i.end_dt
              OR i_end_dt BETWEEN i.eff_dt AND i.end_dt
              OR i.eff_dt BETWEEN i_eff_dt AND i_end_dt
             )
         AND i.prtctd_id <> i_prtctd_id
         AND i.stat_cd = 'ACT';

      excp.assert((l_prtctd_id IS NULL), g_c_prtctd_date_overlap_msg || ' ' || l_prtctd_id);
    END;

    excp.assert(NOT is_in_use_by_billing_fn(l_div_part, i_prtctd_id), g_c_in_use_by_billing_msg);
    excp.assert(NOT(op_allocate_pk.is_processing_fn(i_div) = op_allocate_pk.g_c_alloc_is_processing),
                g_c_alloc_conflict_msg
               );
    logs.dbg('Update PRTCTD_INV_OP1I');

    UPDATE prtctd_inv_op1i
       SET grp_id = i_grp_id,
           ord_item_num = i_catlg_num,
           eff_dt = i_eff_dt,
           end_dt = i_end_dt,
           ord_typ_cd = i_ord_typ,
           zone_id = l_zone_id,
           stat_cd = 'ACT',
           user_id = i_user_id,
           last_chg_ts = l_c_sysdate
     WHERE div_part = l_div_part
       AND prtctd_id = i_prtctd_id;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_stat := 'Error';
      o_err_msg := g_c_process_restricted_msg;
      logs.warn(SQLERRM, lar_parm);
    WHEN excp.gx_assert_fail THEN
      o_stat := 'Error';
      o_err_msg := SQLERRM;
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    WHEN OTHERS THEN
      logs.err(lar_parm, NULL, FALSE);
      o_err_msg := 'Unhandled Error: ' || SQLERRM;
      o_stat := 'Error';
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||  Used to insert inventory protection on PRTCTD_INV_OP1I.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 04/12/04 | rhalpai | Added logic to handle non-inventory items.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/10/05 | rhalpai | Changes to support Cig System as cig inventory master.
  || 03/02/06 | rhalpai | Added process control logic. IM200261
  || 04/09/09 | VXRANGA | Venkateswaran Ranganathan updated for IM492071 -
  ||                    | Updated code block towards fix for above IM to handle
  ||                    | overlap date check.
  || 09/14/10 | dlbeal  | Add userid/last chg ts
  || 02/12/20 | rhalpai | Remove SUBSTR of SQLERRM so entire SQLERRM is returned to o_err_msg. SDHD-622870
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_div         IN      VARCHAR2,
    i_grp_id      IN      VARCHAR2,
    i_catlg_num   IN      VARCHAR2,
    i_prtctd_qty  IN      NUMBER,
    i_ord_typ     IN      VARCHAR2,
    i_eff_dt      IN      DATE,
    i_end_dt      IN      DATE,
    i_user_id     IN      VARCHAR2,
    o_prtctd_id   OUT     NUMBER,
    o_stat        OUT     VARCHAR2,
    o_err_msg     OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_PROTECTED_INVENTORY_PK.INS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_inv_item           prtctd_inv_op1i.ord_item_num%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'PrtctdQty', i_prtctd_qty);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'EffDt', i_eff_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    o_stat := 'Good';
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Get Inventory Item');
    l_inv_item := op_item_pk.inv_item_fn(l_div_part, i_catlg_num);
    logs.dbg('Perform Validation');
    excp.assert((is_valid_item_fn(l_div_part, l_inv_item) = g_c_item_is_valid),
                g_c_invalid_item_msg || ' ' || i_catlg_num
               );

    DECLARE
      l_valid_dt_stat  g_valid_dates_stat%TYPE;
    BEGIN
      l_valid_dt_stat := get_valid_dates_status_fn(i_eff_dt, i_end_dt);
      excp.assert((l_valid_dt_stat = g_c_dates_are_valid), TO_CHAR(i_end_dt, 'YYYY-MM-DD') || ' ' || l_valid_dt_stat);
    END;

    DECLARE
      l_prtctd_id  prtctd_inv_op1i.prtctd_id%TYPE;
    BEGIN
      l_prtctd_id := prtctd_id_fn(i_div, i_div, i_grp_id, l_inv_item, i_ord_typ, i_eff_dt, i_end_dt);
      excp.assert((l_prtctd_id IS NULL), g_c_prtctd_date_overlap_msg || ' ' || l_prtctd_id);
    END;

    excp.assert(NOT(op_allocate_pk.is_processing_fn(i_div) = op_allocate_pk.g_c_alloc_is_processing),
                g_c_alloc_conflict_msg
               );
    logs.dbg('Insert PRTCTD_INV_OP1I');

    INSERT INTO prtctd_inv_op1i
                (prtctd_id, div_part, zone_id, grp_id, ord_item_num, eff_dt, end_dt, ord_typ_cd,
                 orig_qty, prtctd_qty, create_user, user_id, stat_cd
                )
         VALUES (op1i_prtctd_id_seq.NEXTVAL, l_div_part, i_div, i_grp_id, l_inv_item, i_eff_dt, i_end_dt, i_ord_typ,
                 i_prtctd_qty, i_prtctd_qty, i_user_id, i_user_id, 'ACT'
                )
      RETURNING prtctd_id
           INTO o_prtctd_id;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_stat := 'Error';
      o_err_msg := g_c_process_restricted_msg;
      logs.warn(SQLERRM, lar_parm);
    WHEN excp.gx_assert_fail THEN
      o_stat := 'Error';
      o_err_msg := SQLERRM;
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    WHEN OTHERS THEN
      logs.err(lar_parm, NULL, FALSE);
      o_err_msg := 'Unhandled Error: ' || SQLERRM;
      o_stat := 'Error';
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_prtctd_inv_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_PRTCTD_INV_LOG_SP
  ||  Used to track and update usage of protected inventory.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 06/14/05 | rhalpai | Handled no data found error IM156740
  || 03/02/06 | rhalpai | Removed commit to allow control by Load Close Process.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_prtctd_inv_log_sp(
    i_div_part  IN  NUMBER,
    i_log_typ   IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                            := 'OP_PROTECTED_INVENTORY_PK.UPD_PRTCTD_INV_LOG_SP';
    lar_parm              logs.tar_parm;
    l_div                 div_mstr_di1d.div_id%TYPE;
    l_c_sysdate  CONSTANT DATE                                      := SYSDATE;
    l_alloc_qty           PLS_INTEGER;
    l_pick_qty            PLS_INTEGER;
    l_sub_cd              PLS_INTEGER;
    l_adj_qty             PLS_INTEGER;
    l_r_prtctd_inv        op_protected_inventory_pk.g_rt_prtctd_inv;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LogTyp', i_log_typ);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
    l_div := div_pk.div_id_fn(i_div_part);
    logs.dbg('Get Protected Item Info');
    l_r_prtctd_inv := prtctd_inv_info_fn(l_div, i_ord_num, i_ord_ln, 0);

    IF l_r_prtctd_inv.prtctd_id > 0 THEN
      logs.dbg('Get Order Info');

      SELECT b.alcqtb, b.pckqtb, b.subrcb
        INTO l_alloc_qty, l_pick_qty, l_sub_cd
        FROM ordp120b b
       WHERE b.div_part = i_div_part
         AND b.ordnob = i_ord_num
         AND b.lineb = i_ord_ln;

      IF (    i_log_typ = op_protected_inventory_pk.g_c_rlse
          AND l_sub_cd <> g_c_conditional_sub) THEN
        l_adj_qty :=(CASE
                       WHEN l_r_prtctd_inv.prtctd_cust_qty < l_alloc_qty THEN l_r_prtctd_inv.prtctd_cust_qty
                       ELSE l_alloc_qty
                     END
                    );

        -- Don't log zeros
        IF l_adj_qty > 0 THEN
          logs.dbg('Release - Update Protected Inventory');

          UPDATE prtctd_inv_op1i i
             SET i.prtctd_qty = i.prtctd_qty - l_adj_qty,
                 i.user_id = i_log_typ,
                 i.last_chg_ts = l_c_sysdate
           WHERE i.div_part = i_div_part
             AND i.prtctd_id = l_r_prtctd_inv.prtctd_id;

          logs.dbg('Release - Insert Log Record');

          INSERT INTO prtctd_alloc_log_op1a
                      (div_part, tran_id, prtctd_id, ord_num, ln_num, qty,
                       last_chg_ts, stat_cd
                      )
               VALUES (i_div_part, op1a_tran_id_seq.NEXTVAL, l_r_prtctd_inv.prtctd_id, i_ord_num, i_ord_ln, l_adj_qty,
                       l_c_sysdate, i_log_typ
                      );
        END IF;
      ELSIF i_log_typ = op_protected_inventory_pk.g_c_pick THEN
        logs.dbg('Pick - Get Protected Portion of Alloc Qty');

        SELECT a.qty
          INTO l_adj_qty
          FROM prtctd_alloc_log_op1a a
         WHERE a.div_part = i_div_part
           AND a.tran_id = (SELECT MAX(a2.tran_id)
                              FROM prtctd_alloc_log_op1a a2
                             WHERE a2.div_part = i_div_part
                               AND a2.prtctd_id = l_r_prtctd_inv.prtctd_id
                               AND a2.ord_num = i_ord_num
                               AND a2.ln_num = i_ord_ln
                               AND a2.stat_cd = 'RLS');

        IF l_pick_qty < l_adj_qty THEN
          logs.dbg('Pick - Update Protected Inventory');

          UPDATE prtctd_inv_op1i i
             SET i.prtctd_qty = i.prtctd_qty +(l_adj_qty - l_pick_qty),
                 i.user_id = i_log_typ,
                 i.last_chg_ts = l_c_sysdate
           WHERE i.div_part = i_div_part
             AND i.prtctd_id = l_r_prtctd_inv.prtctd_id;

          logs.dbg('Pick - Insert Log Record');

          INSERT INTO prtctd_alloc_log_op1a
                      (div_part, tran_id, prtctd_id, ord_num, ln_num,
                       qty, last_chg_ts, stat_cd
                      )
               VALUES (i_div_part, op1a_tran_id_seq.NEXTVAL, l_r_prtctd_inv.prtctd_id, i_ord_num, i_ord_ln,
                       (l_adj_qty - l_pick_qty
                       ), l_c_sysdate, i_log_typ
                      );
        END IF;   -- l_pick_qty < l_adj_qty
      END IF;   -- i_log_typ = op_protected_inventory_pk.g_c_rlse AND l_sub_cd <> g_c_conditional_sub
    END IF;   -- l_r_prtctd_inv.prtctd_id > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_prtctd_inv_log_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_LOG_FOR_PARTLS_SP
  ||  Used to update the order line in the protected inventory log for partials
  ||  allocations.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_log_for_partls_sp(
    i_div_part  IN  NUMBER,
    i_log_typ   IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_PROTECTED_INVENTORY_PK.UPD_LOG_FOR_PARTLS_SP';
    lar_parm                logs.tar_parm;
    l_c_partl_sub  CONSTANT NUMBER        := .1;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LogTyp', i_log_typ);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);

    IF (i_ord_ln - FLOOR(i_ord_ln)) = l_c_partl_sub THEN
      ------------------------------------------------------------------
      -- Order Line has already been logged but has now been converted
      -- to a Partial Conditional Sub so we are keeping the log in sync
      -- with the Transaction table in case of backout.
      ------------------------------------------------------------------
      logs.dbg('Update Prot Inv Log Order Line to Partial Sub Line');

      UPDATE prtctd_alloc_log_op1a a
         SET a.ln_num = i_ord_ln
       WHERE a.div_part = i_div_part
         AND a.tran_id = (SELECT MAX(a2.tran_id)
                            FROM prtctd_alloc_log_op1a a2
                           WHERE a2.div_part = i_div_part
                             AND a2.ord_num = i_ord_num
                             AND a2.ln_num = FLOOR(i_ord_ln)
                             AND a2.stat_cd = i_log_typ);
    END IF;   -- (i_ord_ln - FLOOR(i_ord_ln)) = l_c_partl_sub

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_log_for_partls_sp;

  /*
  ||----------------------------------------------------------------------------
  || BACKOUT_SP
  ||  Used to back out the updates made to the protected inventory table during
  ||  allocate and/or ship confirm. This procedure is called within
  ||  op_backout_llr_pk.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  ||----------------------------------------------------------------------------
  */
  PROCEDURE backout_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE,
    i_log_typ   IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm         := 'OP_PROTECTED_INVENTORY_PK.BACKOUT_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                  := SYSDATE;
    l_c_part_id  CONSTANT NUMBER                := TO_NUMBER(TO_CHAR(i_rlse_ts, 'DD')) - 1;
    l_ord_stat            ordp120b.statb%TYPE;
    l_t_prtctd_ids        type_ntab;
    l_t_ord_nums          type_ntab;
    l_t_ord_lns           type_ntab;
    l_t_qtys              type_ntab;
    l_plus_minus_num      PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'LogTyp', i_log_typ);
    logs.dbg('ENTRY', lar_parm);
    l_ord_stat :=(CASE i_log_typ
                    WHEN op_protected_inventory_pk.g_c_backout_rlse THEN 'T'
                    WHEN op_protected_inventory_pk.g_c_backout_ship THEN 'A'
                  END
                 );
    logs.dbg('Get Protected Inventory Info for Release');

    SELECT a.prtctd_id, a.ord_num, a.ln_num, a.qty
    BULK COLLECT INTO l_t_prtctd_ids, l_t_ord_nums, l_t_ord_lns, l_t_qtys
      FROM prtctd_alloc_log_op1a a,
           (SELECT   MAX(a2.tran_id) AS tran_id
                FROM prtctd_alloc_log_op1a a2, rlse_op1z r, tran_op2t op2t, tran_ord_op2o op2o, ordp120b b
               WHERE a2.div_part = i_div_part
                 AND a2.stat_cd = i_log_typ
                 AND r.div_part = i_div_part
                 AND r.rlse_ts = i_rlse_ts
                 AND op2t.div_part = r.div_part
                 AND op2t.rlse_id = r.rlse_id
                 AND op2t.part_id = l_c_part_id
                 AND op2o.div_part = op2t.div_part
                 AND op2o.tran_id = op2t.tran_id
                 AND op2o.part_id = l_c_part_id
                 AND op2o.ord_num = a2.ord_num
                 AND op2o.ord_ln = a2.ln_num
                 AND b.div_part = op2o.div_part
                 AND b.ordnob = op2o.ord_num
                 AND b.lineb = op2o.ord_ln
                 AND b.statb = l_ord_stat
            GROUP BY a2.ord_num, a2.ln_num) a3
     WHERE a.div_part = i_div_part
       AND a.tran_id = a3.tran_id;

    IF l_t_prtctd_ids.COUNT > 0 THEN
      l_plus_minus_num :=(CASE i_log_typ
                            WHEN op_protected_inventory_pk.g_c_backout_rlse THEN 1
                            WHEN op_protected_inventory_pk.g_c_backout_ship THEN -1
                          END
                         );
      logs.dbg('Upd Protected Inventory');
      FORALL i IN l_t_prtctd_ids.FIRST .. l_t_prtctd_ids.LAST
        UPDATE prtctd_inv_op1i pi
           SET pi.prtctd_qty = pi.prtctd_qty +(l_t_qtys(i) * l_plus_minus_num),
               pi.user_id = i_log_typ,
               pi.last_chg_ts = l_c_sysdate
         WHERE pi.div_part = i_div_part
           AND pi.prtctd_id = l_t_prtctd_ids(i);
      logs.dbg('Add Log Record');
      FORALL i IN l_t_prtctd_ids.FIRST .. l_t_prtctd_ids.LAST
        INSERT INTO prtctd_alloc_log_op1a
                    (div_part, tran_id, prtctd_id, ord_num, ln_num,
                     qty, last_chg_ts, stat_cd
                    )
             VALUES (i_div_part, op1a_tran_id_seq.NEXTVAL, l_t_prtctd_ids(i), l_t_ord_nums(i), l_t_ord_lns(i),
                     l_t_qtys(i), l_c_sysdate, i_log_typ
                    );
      COMMIT;
    END IF;   -- l_t_prtctd_ids.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END backout_sp;

  /*
  ||----------------------------------------------------------------------------
  || CLEANUP_SP
  ||  Used to clean up the protected inventory tables.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cleanup_sp(
    i_div_part  IN      NUMBER,
    o_stat      OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm             := 'OP_PROTECTED_INVENTORY_PK.CLEANUP_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                      := SYSDATE;
    l_t_parms             op_types_pk.tt_varchars_v;
    l_max_inv_sav_dt      DATE;
    l_max_log_sav_dt      DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    o_stat := 'Good';
    logs.dbg('Initialize');
    l_t_parms := op_parms_pk.idx_vals_fn(i_div_part,
                                         op_const_pk.prm_prtctd_inv_days || ',' || op_const_pk.prm_prtctd_inv_log_days
                                        );
    l_max_inv_sav_dt := TRUNC(l_c_sysdate) - TO_NUMBER(l_t_parms(op_const_pk.prm_prtctd_inv_days));
    l_max_log_sav_dt := TRUNC(l_c_sysdate) - TO_NUMBER(l_t_parms(op_const_pk.prm_prtctd_inv_log_days));
    logs.dbg('Upd Status for Expired');

    UPDATE prtctd_inv_op1i i
       SET i.stat_cd = 'INA',
           i.user_id = 'END-DT',
           i.last_chg_ts = l_c_sysdate
     WHERE i.div_part = i_div_part
       AND i.stat_cd = 'ACT'
       AND i.end_dt < TRUNC(l_c_sysdate);

    logs.dbg('Upd Status for Inactive Items');

    UPDATE prtctd_inv_op1i i
       SET i.stat_cd = 'INA',
           i.user_id = 'INA-ITEM',
           i.last_chg_ts = l_c_sysdate
     WHERE i.div_part = i_div_part
       AND i.stat_cd = 'ACT'
       AND EXISTS(SELECT 1
                    FROM sawp505e e
                   WHERE e.catite = i.ord_item_num
                     AND e.state = 2);

    logs.dbg('Upd Status for Zero Protected Inv');

    UPDATE prtctd_inv_op1i i
       SET i.stat_cd = 'INA',
           i.user_id = 'ZERO-PI',
           i.last_chg_ts = l_c_sysdate
     WHERE i.div_part = i_div_part
       AND i.stat_cd = 'ACT'
       AND i.prtctd_qty = 0;

    logs.dbg('Protected Inv Cleanup');

    DELETE FROM prtctd_inv_op1i i
          WHERE i.div_part = i_div_part
            AND i.stat_cd = 'INA'
            AND i.last_chg_ts < l_max_inv_sav_dt;

    logs.dbg('Protected Log Cleanup');

    DELETE FROM prtctd_alloc_log_op1a a
          WHERE a.div_part = i_div_part
            AND a.last_chg_ts < l_max_log_sav_dt
            AND NOT EXISTS(SELECT 1
                             FROM ordp120b b
                            WHERE b.div_part = a.div_part
                              AND b.ordnob = a.ord_num
                              AND b.lineb = a.ln_num);

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cleanup_sp;
END op_protected_inventory_pk;
/

