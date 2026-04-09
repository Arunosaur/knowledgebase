CREATE OR REPLACE PACKAGE op_sbscrptn_ord_pk IS
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
  FUNCTION cust_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION move_dtl_list_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE move_sp(
    i_div          IN  VARCHAR2,
    i_cust_id      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_parm_list    IN  CLOB,
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );
END op_sbscrptn_ord_pk;
/

CREATE OR REPLACE PACKAGE BODY op_sbscrptn_ord_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || UPD_EVNT_LOG_SP
  ||  Update the event log
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/13 | rhalpai | Original for PIR11038
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_evnt_log_sp(
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER,
    i_evnt_msg     IN  VARCHAR2,
    i_finish_cd    IN  NUMBER DEFAULT 0
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SBSCRPTN_ORD_PK.UPD_EVNT_LOG_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.add_parm(lar_parm, 'EvntMsg', i_evnt_msg);
    logs.add_parm(lar_parm, 'FinishCd', i_finish_cd);
    logs.info('ENTRY', lar_parm);
    cig_event_mgr_pk.update_log_message(i_evnt_que_id,
                                        i_cycl_id,
                                        i_cycl_dfn_id,
                                        SUBSTR(i_evnt_msg, 1, 512),
                                        i_finish_cd
                                       );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END upd_evnt_log_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || CUST_LIST_FN
  ||  Build a cursor of Subscription Order Customers.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/12/13 | rhalpai | Original. PIR12239
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SBSCRPTN_ORD_PK.CUST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_goodies_custs    type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_goodies_custs := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_goodies_cus);

    OPEN l_cv
     FOR
       SELECT   c.acnoc, c.namec
           FROM TABLE(CAST(l_t_goodies_custs AS type_stab)) t, sysp200c c
          WHERE c.div_part = l_div_part
            AND c.acnoc = t.column_value
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_DTL_LIST_FN
  ||  Build a cursor of Subscription Order Move Detail by ShipDt,LLRDt,Load,Item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/12/13 | rhalpai | Original. PIR12239
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION move_dtl_list_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SBSCRPTN_ORD_PK.MOVE_DTL_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + x.shp_dt, 'YYYY-MM-DD') AS shp_dt,
                TO_CHAR(x.llr_dt, 'YYYY-MM-DD') AS llr_dt, x.load_num, x.catlg_num, y.ctdsce AS item_descr,
                w.qavc AS qty_avl, x.ord_cnt
           FROM (SELECT   a.shpja AS shp_dt, ld.llr_dt AS llr_dt, ld.load_num, b.orditb AS catlg_num,
                          COUNT(DISTINCT a.ordnoa) AS ord_cnt
                     FROM div_mstr_di1d d, stop_eta_op1g se, ordp100a a, load_depart_op1f ld, ordp120b b
                    WHERE d.div_id = i_div
                      AND se.div_part = d.div_part
                      AND se.cust_id = i_cust_id
                      AND a.div_part = se.div_part
                      AND a.load_depart_sid = se.load_depart_sid
                      AND a.custa = se.cust_id
                      AND a.excptn_sw = 'N'
                      AND a.stata = 'O'
                      AND a.dsorda = 'R'
                      AND ld.div_part = se.div_part
                      AND ld.load_depart_sid = se.load_depart_sid
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                 GROUP BY a.shpja, ld.llr_dt, ld.load_num, b.orditb) x,
                (SELECT e.catite, e.iteme, e.uome, e.ctdsce, d.div_part
                   FROM div_mstr_di1d d, sawp505e e
                  WHERE d.div_id = i_div) y, whsp300c w
          WHERE y.catite = x.catlg_num
            AND w.div_part(+) = y.div_part
            AND w.itemc(+) = y.iteme
            AND w.uomc(+) = y.uome
       ORDER BY 1, 2, 3, 4;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_dtl_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_SP
  ||  Move given qty of Subscription Orders to new ship date.
  ||  Create MQ msgs to notify mainframe of changes.
  ||  Parmlist format:
  ||   ShpDt~LLRDt~Load~CatlgNum~MoveQty~NewShpDt`ShpDt~LLRDt~Load~CatlgNum~MoveQty~NewShpDt
  ||   (format for ShpDt, LLRDt, NewShpDt: YYYY-MM-DD)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/12/13 | rhalpai | Original. PIR12239
  || 05/13/13 | rhalpai | Change logic to pad GENTOMF MQ msgs to 250 characters.
  ||                    | PIR11910
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||                    | Add logic to process via event. PIR12685
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.CREATE_INSTANCE.
  ||                    | Change to use constants package OP_CONST_PK.
  ||                    | Add div_part in call to OP_SYSP296A_PK.INS_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_sp(
    i_div          IN  VARCHAR2,
    i_cust_id      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_parm_list    IN  CLOB,
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_SBSCRPTN_ORD_PK.MOVE_SP';
    lar_parm             logs.tar_parm;
    l_section            typ.t_maxvc2;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;
    l_div_part           NUMBER;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
    l_shp_dt             NUMBER;
    l_llr_dt             DATE;
    l_load_num           mclp120c.loadc%TYPE;
    l_catlg_num          sawp505e.catite%TYPE;
    l_move_qty           NUMBER;
    l_new_shp_dt         NUMBER;
    l_t_ord_nums         type_ntab;
    l_t_po_nums          type_stab;
    l_t_old_shp_dts      type_ntab;
    l_rc                 PLS_INTEGER;
    l_e_mq_put_failed    EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Get OrgId');
      l_org_id := cig_organization_pk.get_div_id(i_div);
      l_evnt_parms := '<parameters>'
                      || '<row><sequence>'
                      || 1
                      || '</sequence><value>'
                      || i_div
                      || '</value></row>'
                      || '<row><sequence>'
                      || 2
                      || '</sequence><value>'
                      || i_cust_id
                      || '</value></row>'
                      || '<row><sequence>'
                      || 3
                      || '</sequence><value>'
                      || i_user_id
                      || '</value></row>'
                      || '<row><sequence>'
                      || 4
                      || '</sequence><value>'
                      || i_parm_list
                      || '</value></row>'
                      || '</parameters>';
      logs.dbg('Create Event');
      cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                       i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                       i_event_dfn_id         => cig_constants_events_pk.evd_sbscrptn_ord_move,
                                       i_parameters           => l_evnt_parms,
                                       i_div_nm               => i_div,
                                       i_is_script_fw_exec    => 'Y',
                                       i_is_complete          => 'Y',
                                       i_pgm_id               => 'PLSQL',
                                       i_user_id              => i_user_id,
                                       o_event_que_id         => l_evnt_que_id
                                      );
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Parse Groups of Parm Field Lists');
      l_t_grps := strsplit_fn(i_parm_list, op_const_pk.grp_delimiter);

      IF l_t_grps IS NOT NULL THEN
        l_idx := l_t_grps.FIRST;
        WHILE l_idx IS NOT NULL LOOP
          logs.dbg('Parse Parm Field List');
          l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
          l_shp_dt := TO_DATE(val_at_idx_fn(l_t_fields, 1), 'YYYY-MM-DD') - DATE '1900-02-28';
          l_llr_dt := TO_DATE(val_at_idx_fn(l_t_fields, 2), 'YYYY-MM-DD');
          l_load_num := val_at_idx_fn(l_t_fields, 3);
          l_catlg_num := val_at_idx_fn(l_t_fields, 4);
          l_move_qty := val_at_idx_fn(l_t_fields, 5);
          l_new_shp_dt := TO_DATE(val_at_idx_fn(l_t_fields, 6), 'YYYY-MM-DD') - DATE '1900-02-28';
          l_section := 'Get Orders';
          logs.dbg(l_section);
          upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);

          SELECT x.ord_num, x.po_num, x.old_shp_dt
          BULK COLLECT INTO l_t_ord_nums, l_t_po_nums, l_t_old_shp_dts
            FROM (SELECT a.ordnoa AS ord_num, a.cpoa AS po_num, a.shpja AS old_shp_dt,
                         ROW_NUMBER() OVER(ORDER BY a.ordnoa) AS seq
                    FROM load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e
                   WHERE ld.div_part = l_div_part
                     AND ld.llr_dt = l_llr_dt
                     AND ld.load_num = l_load_num
                     AND a.div_part = ld.div_part
                     AND a.load_depart_sid = ld.load_depart_sid
                     AND a.custa = i_cust_id
                     AND a.shpja = l_shp_dt
                     AND a.excptn_sw = 'N'
                     AND a.stata = 'O'
                     AND a.dsorda = 'R'
                     AND b.div_part = a.div_part
                     AND b.ordnob = a.ordnoa
                     AND e.iteme = b.itemnb
                     AND e.uome = b.sllumb
                     AND e.catite = l_catlg_num) x
           WHERE x.seq <= l_move_qty;

          IF l_t_ord_nums.COUNT > 0 THEN
            l_section := 'Upd Ship Dates';
            logs.dbg(l_section);
            upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
            FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
              UPDATE ordp100a a
                 SET a.shpja = l_new_shp_dt
               WHERE a.div_part = l_div_part
                 AND a.ordnoa = l_t_ord_nums(i);
            l_section := 'Add MQ PUT Entries';
            logs.dbg(l_section);
            upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
            FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
              INSERT INTO mclane_mq_put
                          (div_part, mq_msg_id, mq_msg_status,
                           mq_msg_data
                          )
                   VALUES (l_div_part, 'GENTOMF', 'OPN',
                           RPAD(i_div
                                || 'MVSUBORD'
                                || RPAD(' ', 30)
                                || 'ADD'
                                || RPAD(' ', 10)
                                || LPAD(l_div_part, 4, '0')
                                || i_cust_id
                                || rpad_fn(l_t_po_nums(i), 30)
                                || TO_CHAR(DATE '1900-02-28' + l_new_shp_dt, 'YYYY-MM-DD')
                                || UPPER(RPAD(i_user_id, 8)),
                                250
                               )
                          );
            l_section := 'Log Ship Date Changes';
            logs.dbg(l_section);
            upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
            FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
              op_sysp296a_pk.ins_sp(l_div_part,
                                    l_t_ord_nums(i),
                                    0,
                                    i_user_id,
                                    'ORDP100A',
                                    'SHPJA',
                                    l_t_old_shp_dts(i),
                                    l_new_shp_dt,
                                    'C',
                                    'MVSUBORD',
                                    'SBSCRPTN_MOVE',
                                    'MOVE SHIP DATE FOR SUBSCRIPTION ORDER'
                                   );
            END LOOP;
            l_section := 'SyncLoad';
            logs.dbg(l_section);
            upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
            op_order_load_pk.syncload_sp(l_div_part, 'MVSUBORD', l_t_ord_nums);
          END IF;   -- l_t_ord_nums.COUNT > 0

          l_idx := l_t_grps.NEXT(l_idx);
        END LOOP;
        -- Commit before calling process to send msgs to mainframe via MQ
        -- since it runs in a separate session
        COMMIT;
        l_section := 'Send QOPRC01 MQ Msgs';
        logs.dbg(l_section);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
        op_mq_message_pk.mq_put_sp('GENTOMF', i_div, NULL, l_rc);

        IF l_rc <> 0 THEN
          RAISE l_e_mq_put_failed;
        END IF;   -- l_rc <> 0
      END IF;   -- l_t_grps IS NOT NULL

      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'SBSCRPTN_ORD_MOVE Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END move_sp;
END op_sbscrptn_ord_pk;
/

