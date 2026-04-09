CREATE OR REPLACE PACKAGE op_maintain_subs_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_group     CONSTANT VARCHAR2(10) := 'GROUP';
  g_c_customer  CONSTANT VARCHAR2(10) := 'CUST';
  g_c_item      CONSTANT VARCHAR2(10) := 'ITEM';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || GET_DESCRIPTION_FN
  ||   Used to retrieve a description for Group, Customer or Item
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_description_fn(
    i_div_part  IN  NUMBER,
    i_typ       IN  VARCHAR2,
    i_val       IN  VARCHAR2,
    i_uom       IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_SUBS_FN
  ||   Used to retrieve a cursor of conditional and unconditional subs for a
  ||   group or customer.
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_subs_fn(
    i_div_part       IN  NUMBER,
    i_cbr            IN  VARCHAR2,
    i_cust_id        IN  VARCHAR2,
    i_grp            IN  VARCHAR2,
    i_sub_typ        IN  VARCHAR2,
    i_item_num       IN  VARCHAR2,
    i_uom            IN  VARCHAR2,
    i_sub_item       IN  VARCHAR2,
    i_sub_uom        IN  VARCHAR2,
    i_start_dt       IN  VARCHAR2,
    i_end_dt         IN  VARCHAR2,
    i_sub_item_stat  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || REVERT_SUB_SP
  ||  Used to delete sub order lines and reprice and update the original order
  ||  line to replace the deleted sub order line.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE revert_sub_sp(
    i_div_part     IN  NUMBER,
    i_sub_ord_num  IN  NUMBER,
    i_sub_ord_ln   IN  NUMBER,
    i_leave_sub    IN  VARCHAR2 DEFAULT NULL
  );

  /*
  ||----------------------------------------------------------------------------
  || MAINT_SUB_SP
  ||  Driver for sub maintenance.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE maint_sub_sp(
    i_actn       IN      VARCHAR2,
    i_div_part   IN      NUMBER,
    i_cls_typ    IN      VARCHAR2,
    i_cls_id     IN      VARCHAR2,
    i_catlg_num  IN      NUMBER,
    i_sub_typ    IN      VARCHAR2,
    i_sub_item   IN      NUMBER,
    i_qty_fctor  IN      NUMBER,
    i_start_dt   IN      DATE,
    i_end_dt     IN      DATE,
    i_user_id    IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );
END op_maintain_subs_pk;
/

CREATE OR REPLACE PACKAGE BODY op_maintain_subs_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_date_fmt  CONSTANT VARCHAR2(10) := 'MMDDYY';
  g_c_add       CONSTANT VARCHAR2(3)  := 'ADD';
  g_c_chg       CONSTANT VARCHAR2(3)  := 'CHG';
  g_c_del       CONSTANT VARCHAR2(3)  := 'DEL';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || SUB_REC_FN
  ||  Get sub record matching passed parameters.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/03/09 | rhalpai | Original (Created for PIR4342)
  ||----------------------------------------------------------------------------
  */
  FUNCTION sub_rec_fn(
    i_div_part   IN  NUMBER,
    i_cls_typ    IN  VARCHAR2,
    i_cls_id     IN  VARCHAR2,
    i_catlg_num  IN  NUMBER,
    i_sub_typ    IN  VARCHAR2
  )
    RETURN sub_mstr_op5s%ROWTYPE IS
    l_cv     SYS_REFCURSOR;
    l_r_sub  sub_mstr_op5s%ROWTYPE;
  BEGIN
    OPEN l_cv
     FOR
       SELECT *
         FROM sub_mstr_op5s s
        WHERE s.div_part = i_div_part
          AND s.cls_typ = i_cls_typ
          AND s.cls_id = i_cls_id
          AND s.catlg_num = i_catlg_num
          AND s.sub_typ = i_sub_typ;

    FETCH l_cv
     INTO l_r_sub;

    RETURN(l_r_sub);
  END sub_rec_fn;

  /*
  ||----------------------------------------------------------------------------
  || DEL_SP
  ||  Remove sub row.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/03/09 | rhalpai | Original (Created for PIR4342)
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_sp(
    i_div_part   IN  NUMBER,
    i_cls_typ    IN  VARCHAR2,
    i_cls_id     IN  VARCHAR2,
    i_catlg_num  IN  NUMBER,
    i_sub_typ    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MAINTAIN_SUBS_PK.DEL_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ClsTyp', i_cls_typ);
    logs.add_parm(lar_parm, 'ClsId', i_cls_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
    logs.dbg('ENTRY', lar_parm);

    DELETE FROM sub_mstr_op5s s
          WHERE s.div_part = i_div_part
            AND s.cls_typ = i_cls_typ
            AND s.cls_id = i_cls_id
            AND s.catlg_num = i_catlg_num
            AND s.sub_typ = i_sub_typ;

    IF SQL%ROWCOUNT = 0 THEN
      logs.warn('No rows removed!', lar_parm);
    END IF;   -- SQL%ROWCOUNT = 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_SP
  ||  Add/Change sub row.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/03/09 | rhalpai | Original (Created for PIR4342)
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_sp(
    i_div_part   IN  NUMBER,
    i_cls_typ    IN  VARCHAR2,
    i_cls_id     IN  VARCHAR2,
    i_catlg_num  IN  NUMBER,
    i_sub_typ    IN  VARCHAR2,
    i_sub_item   IN  NUMBER,
    i_qty_fctor  IN  NUMBER,
    i_start_dt   IN  DATE,
    i_end_dt     IN  DATE,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MAINTAIN_SUBS_PK.MERGE_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ClsTyp', i_cls_typ);
    logs.add_parm(lar_parm, 'ClsId', i_cls_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
    logs.add_parm(lar_parm, 'SubItem', i_sub_item);
    logs.add_parm(lar_parm, 'QtyFctor', i_qty_fctor);
    logs.add_parm(lar_parm, 'StartDt', i_start_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    MERGE INTO sub_mstr_op5s s
         USING (SELECT 1 AS tst
                  FROM DUAL) x
            ON (    s.div_part = i_div_part
                AND s.cls_typ = i_cls_typ
                AND s.cls_id = i_cls_id
                AND s.catlg_num = i_catlg_num
                AND s.sub_typ = i_sub_typ
                AND x.tst > 0)
      WHEN MATCHED THEN
        UPDATE
           SET s.sub_item = i_sub_item, s.qty_fctor = i_qty_fctor, s.start_dt = i_start_dt, s.end_dt = i_end_dt,
               s.user_id = i_user_id, s.last_chg_ts = l_c_sysdate
      WHEN NOT MATCHED THEN
        INSERT(div_part, cls_typ, cls_id, catlg_num, sub_typ, sub_item, qty_fctor, start_dt, end_dt, user_id,
               last_chg_ts)
        VALUES(i_div_part, i_cls_typ, i_cls_id, i_catlg_num, i_sub_typ, i_sub_item, i_qty_fctor, i_start_dt, i_end_dt,
               i_user_id, l_c_sysdate);

    IF SQL%ROWCOUNT = 0 THEN
      logs.warn('No rows applied for MERGE!', lar_parm);
    END IF;   -- SQL%ROWCOUNT = 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END merge_sp;

  /*
  ||----------------------------------------------------------------------------
  || REVERT_SUB_ORDS_SP
  ||  Calls RevertSub process to existing delete sub order lines during sub
  ||  maintenance where the sub has been deleted or the effective date has
  ||  been changed causing existing sub order lines to be outside the sub's
  ||  effective date range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/11/08 | rhalpai | Combined logic for updating customer-level and group-
  ||                    | level unconditional subs into common module.
  ||                    | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 10/28/09 | rhalpai | Created from REVERT_UNC_SUB_ORDS_SP and changed logic to
  ||                    | use parms associated with columns on SUB_MSTR_OP5S.
  ||                    | PIR4342
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1G. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to REVERT_SUB_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE revert_sub_ords_sp(
    i_div_part      IN  NUMBER,
    i_cls_typ       IN  VARCHAR2,
    i_cls_id        IN  VARCHAR2,
    i_catlg_num     IN  NUMBER,
    i_sub_typ       IN  VARCHAR2,
    i_sub_item      IN  NUMBER,
    i_start_dt      IN  DATE,
    i_end_dt        IN  DATE,
    i_new_start_dt  IN  DATE DEFAULT NULL,
    i_new_end_dt    IN  DATE DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MAINTAIN_SUBS_PK.REVERT_SUB_ORDS_SP';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_unc_sub_ords(
      b_div_part      NUMBER,
      b_cls_typ       VARCHAR2,
      b_cls_id        VARCHAR2,
      b_catlg_num     VARCHAR2,
      b_sub_typ       VARCHAR2,
      b_sub_item      VARCHAR2,
      b_start_dt      DATE,
      b_end_dt        DATE,
      b_new_start_dt  DATE,
      b_new_end_dt    DATE
    ) IS
      SELECT b.ordnob AS ord_num, b.lineb AS ord_ln
        FROM sawp505e e, ordp120b b,
             (SELECT b.ordnob AS ord_num, b.lineb AS ord_ln
                FROM sawp505e e, ordp120b b, ordp100a a, stop_eta_op1g se
               WHERE e.catite = b_catlg_num
                 AND b.div_part = b_div_part
                 AND b.itemnb = e.iteme
                 AND b.sllumb = e.uome
                 AND b.statb = 'O'
                 AND b.subrcb = 999
                 AND a.div_part = b.div_part
                 AND a.ordnoa = b.ordnob
                 AND (   b_sub_typ = 'RPI'
                      OR a.custa IN(SELECT c.acnoc
                                      FROM sysp200c c
                                     WHERE c.div_part = b_div_part
                                       AND b_cls_id = DECODE(b_cls_typ, 'CUS', c.acnoc, 'GRP', c.subgpc))
                     )
                 AND a.stata = 'O'
                 AND se.div_part = a.div_part
                 AND se.load_depart_sid = a.load_depart_sid
                 AND se.cust_id = a.custa
                 AND TRUNC(se.eta_ts) BETWEEN b_start_dt AND b_end_dt
                 AND (   TRUNC(se.eta_ts) < NVL(b_new_start_dt, b_end_dt + 1)
                      OR TRUNC(se.eta_ts) > NVL(b_new_end_dt, b_start_dt - 1)
                     )) orig
       WHERE e.catite = b_sub_item
         AND b.div_part = b_div_part
         AND b.ordnob = orig.ord_num
         AND FLOOR(b.lineb) = orig.ord_ln
         AND b.itemnb = e.iteme
         AND b.sllumb = e.uome
         AND b.statb = 'O'
         AND b.subrcb = DECODE(b_sub_typ, 'UCS', 1, 'UGP', 1, 'RPI', 2);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ClsTyp', i_cls_typ);
    logs.add_parm(lar_parm, 'ClsId', i_cls_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
    logs.add_parm(lar_parm, 'SubItem', i_sub_item);
    logs.add_parm(lar_parm, 'StartDt', i_start_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'NewStartDt', i_new_start_dt);
    logs.add_parm(lar_parm, 'NewEndDt', i_new_end_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Existing Unconditional Sub Orders');
    FOR l_r_ord IN l_cur_unc_sub_ords(i_div_part,
                                      i_cls_typ,
                                      i_cls_id,
                                      LPAD(i_catlg_num, 6, '0'),
                                      i_sub_typ,
                                      LPAD(i_sub_item, 6, '0'),
                                      i_start_dt,
                                      i_end_dt,
                                      i_new_start_dt,
                                      i_new_end_dt
                                     ) LOOP
      logs.dbg('Revert Sub Line');
      revert_sub_sp(i_div_part, l_r_ord.ord_num, l_r_ord.ord_ln);
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END revert_sub_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || CREATE_SUB_ORDS_SP
  ||  Find existing (non-subbed) order lines to create new unconditional sub.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/11/08 | rhalpai | Combined logic for creating new customer-level and
  ||                    | group-level unconditional subs into common module.
  ||                    | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 10/28/09 | rhalpai | Created from NEW_UNC_SUB_ORDS_SP and changed logic to
  ||                    | use parms associated with columns on SUB_MSTR_OP5S.
  ||                    | PIR4342
  || 07/10/12 | rhalpai | Change call to OP_GET_SUBS_PK to remove unused parms.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1G. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to OP_GET_SUBS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE create_sub_ords_sp(
    i_div_part   IN  NUMBER,
    i_cls_typ    IN  VARCHAR2,
    i_cls_id     IN  VARCHAR2,
    i_catlg_num  IN  NUMBER,
    i_sub_typ    IN  VARCHAR2,
    i_start_dt   IN  DATE,
    i_end_dt     IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MAINTAIN_SUBS_PK.CREATE_SUB_ORDS_SP';
    lar_parm             logs.tar_parm;
    l_getsub_msg         VARCHAR2(100);
    l_getsub_sub_found   VARCHAR2(3);

    CURSOR l_cur_ords(
      b_div_part   NUMBER,
      b_cls_typ    VARCHAR2,
      b_cls_id     VARCHAR2,
      b_catlg_num  VARCHAR2,
      b_sub_typ    VARCHAR2,
      b_start_dt   DATE,
      b_end_dt     DATE
    ) IS
      SELECT b.ordnob AS ord_num, b.lineb AS ord_ln
        FROM sawp505e e, ordp120b b, ordp100a a, stop_eta_op1g se
       WHERE e.catite = b_catlg_num
         AND b.div_part = b_div_part
         AND b.itemnb = e.iteme
         AND b.sllumb = e.uome
         AND b.statb = 'O'
         AND b.subrcb = 0
         AND a.div_part = b.div_part
         AND a.ordnoa = b.ordnob
         AND a.stata = 'O'
         AND a.dsorda = 'R'
         AND (   b_sub_typ = 'RPI'
              OR a.custa IN(SELECT c.acnoc
                              FROM sysp200c c
                             WHERE c.div_part = b_div_part
                               AND b_cls_id = DECODE(b_cls_typ, 'CUS', c.acnoc, 'GRP', c.subgpc))
             )
         AND se.div_part = a.div_part
         AND se.load_depart_sid = a.load_depart_sid
         AND se.cust_id = a.custa
         AND TRUNC(se.eta_ts) BETWEEN b_start_dt AND b_end_dt;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ClsTyp', i_cls_typ);
    logs.add_parm(lar_parm, 'ClsId', i_cls_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
    logs.add_parm(lar_parm, 'StartDt', i_start_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process New Unconditional Sub Orders');
    FOR l_r_ord IN l_cur_ords(i_div_part,
                              i_cls_typ,
                              i_cls_id,
                              LPAD(i_catlg_num, 6, '0'),
                              i_sub_typ,
                              i_start_dt,
                              i_end_dt
                             ) LOOP
      logs.dbg('Call OP_GET_SUBS_SP');
      op_get_subs_sp(i_div_part, 'UNCSUB', l_r_ord.ord_num, l_r_ord.ord_ln, l_getsub_msg, l_getsub_sub_found);
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END create_sub_ords_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || GET_DESCRIPTION_FN
  ||   Used to retrieve a description for Group, Customer or Item
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/27/03 | rhalpai | Original
  || 08/11/08 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | PIR6364
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_description_fn(
    i_div_part  IN  NUMBER,
    i_typ       IN  VARCHAR2,
    i_val       IN  VARCHAR2,
    i_uom       IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MAINTAIN_SUBS_PK.GET_DESCRIPTION_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_descr              VARCHAR2(100);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Typ', i_typ);
    logs.add_parm(lar_parm, 'Val', i_val);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.dbg('ENTRY', lar_parm);

    CASE i_typ
      WHEN op_maintain_subs_pk.g_c_group THEN
        logs.dbg('Open Cursor for Group');

        OPEN l_cv
         FOR
           SELECT a.group_name
             FROM mclp100a a
            WHERE a.div_part = i_div_part
              AND a.cstgpa = i_val;
      WHEN op_maintain_subs_pk.g_c_customer THEN
        logs.dbg('Open Cursor for Cust');

        OPEN l_cv
         FOR
           SELECT c.namec
             FROM mclp020b cx, sysp200c c
            WHERE cx.div_part = i_div_part
              AND i_val IN(cx.mccusb, cx.custb)
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb;
      WHEN op_maintain_subs_pk.g_c_item THEN
        logs.dbg('Open Cursor for Item');

        OPEN l_cv
         FOR
           SELECT e.ctdsce || ' - ' || e.shppke || ' [' || TRIM(e.sizee) || '] '
             FROM sawp505e e
            WHERE e.catite = i_val
               OR (    e.iteme = i_val
                   AND e.uome = i_uom);
    END CASE;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_descr;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_descr);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_description_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_SUBS_FN
  ||   Used to retrieve a cursor of conditional and unconditional subs for a
  ||   group or customer.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/22/02 | rhalpai | Original
  || 01/27/03 | rhalpai | Changed logic to handle multiple queries returning
  ||                    | the same record format.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 06/16/08 | rhalpai | Removed owner from package variable definition to
  ||                    | allow use of public synonym.
  || 08/11/08 | rhalpai | Converted cursor from dynamic to fixed SQL and added
  ||                    |  standard error handling logic. PIR6364
  || 10/28/09 | rhalpai | Converted to use SUB_MSTR_OP5S. PIR4342
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_subs_fn(
    i_div_part       IN  NUMBER,
    i_cbr            IN  VARCHAR2,
    i_cust_id        IN  VARCHAR2,
    i_grp            IN  VARCHAR2,
    i_sub_typ        IN  VARCHAR2,
    i_item_num       IN  VARCHAR2,
    i_uom            IN  VARCHAR2,
    i_sub_item       IN  VARCHAR2,
    i_sub_uom        IN  VARCHAR2,
    i_start_dt       IN  VARCHAR2,
    i_end_dt         IN  VARCHAR2,
    i_sub_item_stat  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_MAINTAIN_SUBS_PK.RETRIEVE_SUBS_FN';
    lar_parm               logs.tar_parm;
    l_cv                   SYS_REFCURSOR;
    l_c_start_dt  CONSTANT DATE          := TO_DATE(i_start_dt, g_c_date_fmt);
    l_c_end_dt    CONSTANT DATE          := TO_DATE(i_end_dt, g_c_date_fmt);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Cbr', i_cbr);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'Grp', i_grp);
    logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
    logs.add_parm(lar_parm, 'ItemNum', i_item_num);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'SubItem', i_sub_item);
    logs.add_parm(lar_parm, 'SubUOM', i_sub_uom);
    logs.add_parm(lar_parm, 'StartDt', i_start_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'SubItemStat', i_sub_item_stat);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT (SELECT cx.mccusb
                 FROM mclp020b cx
                WHERE cx.div_part = s.div_part
                  AND cx.custb = s.cls_id
                  AND s.cls_typ = 'CUS') AS mcl_cust, DECODE(s.cls_typ, 'CUS', s.cls_id) AS cust,
              DECODE(s.cls_typ,
                     'GRP', SUBSTR(s.cls_id, 3),
                     'CUS', (SELECT SUBSTR(c.retgpc, 3)
                               FROM sysp200c c
                              WHERE c.div_part = s.div_part
                                AND c.acnoc = s.cls_id)
                    ) AS sub_group,
              SUBSTR(s.sub_typ, 1, 1) AS sub_typ, e.catite AS mcl_item, e.iteme AS item, e.uome AS item_uom,
              e2.catite AS sub_mcl_item, e2.iteme AS sub_item, e2.uome AS sub_item_uom,
              NVL((SELECT di.statb
                     FROM mclp110b di
                    WHERE di.div_part = s.div_part
                      AND di.itemb = e2.iteme
                      AND di.uomb = e2.uome),
                  'N/A'
                 ) AS sub_item_status,
              TO_CHAR(s.start_dt, g_c_date_fmt) AS start_dt, TO_CHAR(s.end_dt, g_c_date_fmt) AS end_dt
         FROM sub_mstr_op5s s, sawp505e e, sawp505e e2
        WHERE s.div_part = i_div_part
          AND s.sub_typ IN('UCS', 'UGP', 'CCS', 'CGP')
          AND (   i_sub_typ IS NULL
               OR SUBSTR(s.sub_typ, 1, 1) = i_sub_typ)
          AND s.cls_typ IN('CUS', 'GRP')
          AND s.cls_typ = DECODE(s.sub_typ, 'UCS', 'CUS', 'CCS', 'CUS', 'GRP')
          AND (   (    i_cust_id IS NULL
                   AND i_grp IS NULL)
               OR s.cls_id = DECODE(s.cls_typ,
                                    'CUS', DECODE(i_cbr,
                                                  NULL, (SELECT cx.custb
                                                           FROM mclp020b cx
                                                          WHERE cx.div_part = i_div_part
                                                            AND cx.mccusb = i_cust_id),
                                                  i_cust_id
                                                 ),
                                    'GRP', i_grp
                                   )
              )
          AND e.catite = s.catlg_num
          AND e2.catite = s.sub_item
          AND (   i_item_num IS NULL
               OR (   (    i_cbr IS NULL
                       AND s.catlg_num = i_item_num)
                   OR (    i_cbr IS NOT NULL
                       AND e.iteme = i_item_num
                       AND e.uome = i_uom)
                  )
              )
          AND (   i_sub_item IS NULL
               OR (   (    i_cbr IS NULL
                       AND s.catlg_num = i_sub_item)
                   OR (    i_cbr IS NOT NULL
                       AND e.iteme = i_sub_item
                       AND e.uome = i_sub_uom)
                  )
              )
          AND (   (    i_start_dt IS NULL
                   AND i_end_dt IS NULL)
               OR (    i_start_dt IS NOT NULL
                   AND (   (    i_end_dt IS NULL
                            AND s.start_dt = l_c_start_dt)
                        OR (    i_end_dt IS NOT NULL
                            AND s.start_dt <= l_c_start_dt
                            AND s.end_dt >= l_c_end_dt)
                       )
                  )
               OR (    i_start_dt IS NULL
                   AND s.end_dt = l_c_end_dt)
              )
          AND (   i_sub_item_stat IS NULL
               OR i_sub_item_stat = (SELECT i.statb
                                       FROM mclp110b i
                                      WHERE i.div_part = s.div_part
                                        AND i.itemb = e2.iteme
                                        AND i.uomb = e2.uome)
              );

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_subs_fn;

  /*
  ||----------------------------------------------------------------------------
  || REVERT_SUB_SP
  ||  Used to delete sub order lines and reprice and update the original order
  ||  line to replace the deleted sub order line.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/22/02 | rhalpai | Original
  || 11/18/02 | rhalpai | Added call to OP_GET_SUBS_SP
  || 08/28/03 | rhalpai | Changed to only call OP_GET_SUBS_SP for order lines
  ||                    | in the good well.
  || 11/17/03 | rhalpai | Changed to call OP_GET_SUBS_SP for order lines in the
  ||                    | good or bad well.
  || 01/26/05 | rhalpai | Changed logic to capture deleted sub info for
  ||                    | exception log.
  ||                    | Changed error handler to new standard format.
  ||                    | Removed out status parm.
  || 01/06/06 | rhalpai | Changed section 'Update Original Order Line' to load
  ||                    | the original exception stored in zipcdb to ntshpb
  ||                    | (not-ship-reason) when the original is in the
  ||                    | exception well.
  || 12/08/06 | rhalpai | Removed call to OP_GET_SUBS_SP since this is already
  ||                    | being performed when repricing original order line.
  ||                    | Changed to use package OP_MCLP300D_PK for logging.
  ||                    | PIR4166
  || 02/26/07 | rhalpai | Removed commit/rollback. IM289541
  || 08/11/08 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | PIR6364
  || 10/28/09 | rhalpai | Created from REVERT_TO_ORIGINAL_SUB_LINE_SP and
  ||                    | changed logic to use a DELETE RETURNING to get the
  ||                    | sub Item/UOM for logging. PIR4342
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm and pass it in call to
  ||                    | OP_REPRICE_PK.REPRICE_ORD_LN_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE revert_sub_sp(
    i_div_part     IN  NUMBER,
    i_sub_ord_num  IN  NUMBER,
    i_sub_ord_ln   IN  NUMBER,
    i_leave_sub    IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_MAINTAIN_SUBS_PK.REVERT_SUB_SP';
    lar_parm             logs.tar_parm;
    l_item_num           sawp505e.iteme%TYPE;
    l_uom                sawp505e.uome%TYPE;
    l_r_mclp300d         mclp300d%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'SubOrdNum', i_sub_ord_num);
    logs.add_parm(lar_parm, 'SubOrdLn', i_sub_ord_ln);
    logs.add_parm(lar_parm, 'LeaveSub', i_leave_sub);
    logs.dbg('ENTRY', lar_parm);

    IF i_leave_sub IS NULL THEN
      logs.dbg('Delete Sub Order Line');

      DELETE FROM ordp120b
            WHERE div_part = i_div_part
              AND ordnob = i_sub_ord_num
              AND lineb = i_sub_ord_ln
        RETURNING itemnb, sllumb
             INTO l_item_num, l_uom;

      logs.dbg('Insert Log Msg');
      l_r_mclp300d.div_part := i_div_part;
      l_r_mclp300d.ordnod := i_sub_ord_num;
      l_r_mclp300d.ordlnd := i_sub_ord_ln;
      l_r_mclp300d.reasnd := 'SUBDEL';
      l_r_mclp300d.descd := 'ORDER SUB REVERTED';
      l_r_mclp300d.exlvld := 4;
      l_r_mclp300d.itemd := l_item_num;
      l_r_mclp300d.uomd := l_uom;
      l_r_mclp300d.resexd := 0;
      op_mclp300d_pk.ins_sp(l_r_mclp300d);
    END IF;   -- i_leave_sub IS NULL

    logs.dbg('Update Original Order Line');

    UPDATE ordp120b
       SET subrcb = 0,
           ntshpb = DECODE(excptn_sw, 'Y', zipcdb),
           zipcdb = NULL
     WHERE div_part = i_div_part
       AND ordnob = i_sub_ord_num
       AND lineb = FLOOR(i_sub_ord_ln);

    logs.dbg('Reprice Original Order Line');
    op_reprice_pk.reprice_ord_ln_sp(i_div_part, i_sub_ord_num, FLOOR(i_sub_ord_ln));
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END revert_sub_sp;

  /*
  ||----------------------------------------------------------------------------
  || MAINT_SUB_SP
  ||  Driver for sub maintenance.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/27/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed out status parm.
  ||                    | Removed status parm from calls to
  ||                    | maintain_group_sub_sp and maintain_cust_sub_sp.
  || 08/11/08 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | PIR6364
  || 10/28/09 | rhalpai | Created from MAINTAIN_SUB_SP and changed logic to
  ||                    | use parms associated with columns on SUB_MSTR_OP5S.
  ||                    | PIR4342
  || 02/18/21 | rhalpai | Remove logic to check item status. SDHD-859009
  ||----------------------------------------------------------------------------
  */
  PROCEDURE maint_sub_sp(
    i_actn       IN      VARCHAR2,
    i_div_part   IN      NUMBER,
    i_cls_typ    IN      VARCHAR2,
    i_cls_id     IN      VARCHAR2,
    i_catlg_num  IN      NUMBER,
    i_sub_typ    IN      VARCHAR2,
    i_sub_item   IN      NUMBER,
    i_qty_fctor  IN      NUMBER,
    i_start_dt   IN      DATE,
    i_end_dt     IN      DATE,
    i_user_id    IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm           := 'OP_MAINTAIN_SUBS_PK.MAINT_SUB_SP';
    lar_parm             logs.tar_parm;
    l_r_orig_sub         sub_mstr_op5s%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Actn', i_actn);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ClsTyp', i_cls_typ);
    logs.add_parm(lar_parm, 'ClsId', i_cls_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
    logs.add_parm(lar_parm, 'SubItem', i_sub_item);
    logs.add_parm(lar_parm, 'QtyFctor', i_qty_fctor);
    logs.add_parm(lar_parm, 'StartDt', i_start_dt);
    logs.add_parm(lar_parm, 'EndDt', i_end_dt);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'IsCommit', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);

    IF i_actn IN(g_c_del, g_c_add, g_c_chg) THEN
      logs.dbg('Get Orig Sub Info');
      l_r_orig_sub := sub_rec_fn(i_div_part, i_cls_typ, i_cls_id, i_catlg_num, i_sub_typ);

      IF i_actn = g_c_del THEN
        logs.dbg('Remove Entry');
        del_sp(i_div_part, i_cls_typ, i_cls_id, i_catlg_num, i_sub_typ);
      ELSE
        logs.dbg('Add/Chg Entry');
        merge_sp(i_div_part,
                 i_cls_typ,
                 i_cls_id,
                 i_catlg_num,
                 i_sub_typ,
                 i_sub_item,
                 i_qty_fctor,
                 i_start_dt,
                 i_end_dt,
                 i_user_id
                );
      END IF;   -- i_actn = g_c_del

      -- existing sub
      IF l_r_orig_sub.sub_item IS NOT NULL THEN
        -- sub removal or sub item change
        IF (    i_sub_typ IN('UCS', 'UGP', 'RPI')
            AND (   i_actn = g_c_del
                 OR i_sub_item <> l_r_orig_sub.sub_item)) THEN
          logs.dbg('Revert Existing Unconditional Sub Lines');
          revert_sub_ords_sp(i_div_part,
                             i_cls_typ,
                             i_cls_id,
                             i_catlg_num,
                             i_sub_typ,
                             l_r_orig_sub.sub_item,
                             l_r_orig_sub.start_dt,
                             l_r_orig_sub.end_dt
                            );
        -- sub effective date change
        ELSIF(    i_sub_typ IN('UCS', 'UGP', 'RPI')
              AND i_actn <> g_c_del
              AND (   i_start_dt <> l_r_orig_sub.start_dt
                   OR i_end_dt <> l_r_orig_sub.end_dt)
             ) THEN
          logs.dbg('Revert Existing Subs With Dates Out of Range');
          revert_sub_ords_sp(i_div_part,
                             i_cls_typ,
                             i_cls_id,
                             i_catlg_num,
                             i_sub_typ,
                             l_r_orig_sub.sub_item,
                             l_r_orig_sub.start_dt,
                             l_r_orig_sub.end_dt,
                             i_start_dt,
                             i_end_dt
                            );
        END IF;   -- sub removal or sub item change

        logs.warn('Sub Maint Audit'
                  || cnst.newline_char
                  || 'OLD SubItem: '
                  || l_r_orig_sub.sub_item
                  || ' QtyFctor: '
                  || l_r_orig_sub.qty_fctor
                  || ' StartDt: '
                  || TO_CHAR(l_r_orig_sub.start_dt, 'YYYYMMDD')
                  || ' EndDt: '
                  || TO_CHAR(l_r_orig_sub.end_dt, 'YYYYMMDD')
                  || ' UserId: '
                  || l_r_orig_sub.user_id
                  || ' LastChgTS: '
                  || TO_CHAR(l_r_orig_sub.last_chg_ts, 'YYYYMMDDHH24MISS'),
                  lar_parm
                 );
      END IF;   -- l_r_orig_sub.sub_item IS NOT NULL

      IF (    i_sub_typ IN('UCS', 'UGP', 'RPI')
          AND i_actn <> g_c_del) THEN
        -- orig sub not found
        logs.dbg('Create Sub Orders');
        create_sub_ords_sp(i_div_part, i_cls_typ, i_cls_id, i_catlg_num, i_sub_typ, i_start_dt, i_end_dt);
      END IF;   -- i_sub_typ IN('UCS', 'UGP', 'RPI') AND i_actn <> g_c_del

      IF i_commit_sw = 'Y' THEN
        COMMIT;
      END IF;   -- i_commit_sw = 'Y'
    ELSE
      o_err_msg := 'Invalid Action:' || NVL(i_actn, 'NULL');
    END IF;   -- i_actn IN(g_c_del, g_c_add, g_c_chg)

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END maint_sub_sp;
END op_maintain_subs_pk;
/

