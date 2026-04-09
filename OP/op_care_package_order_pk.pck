CREATE OR REPLACE PACKAGE op_care_package_order_pk IS
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

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE store_typ_list_sp(
    o_cur  OUT  SYS_REFCURSOR
  );

  PROCEDURE item_tmplt_list_sp(
    i_div           IN      VARCHAR2,
    i_store_typ_cd  IN      VARCHAR2,
    o_cur           OUT     SYS_REFCURSOR
  );

  PROCEDURE cp_ord_list_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  );

  PROCEDURE del_item_tmplt_sp(
    i_div           IN  VARCHAR2,
    i_store_typ_cd  IN  VARCHAR2
  );

  PROCEDURE ins_item_tmplt_sp(
    i_div           IN  VARCHAR2,
    i_store_typ_cd  IN  VARCHAR2,
    i_parm_list     IN  CLOB,
    i_user_id       IN  VARCHAR2
  );

  PROCEDURE ins_cp_ords_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  );

  PROCEDURE evnt_ins_cp_ords_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  CLOB,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE upd_cp_ords_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  CLOB,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE mov_cp_ords_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  );

  PROCEDURE evnt_mov_cp_ords_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  );
END op_care_package_order_pk;
/

CREATE OR REPLACE PACKAGE BODY op_care_package_order_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ADD_EVNT_SP
  ||  Set parameters and initiate event for processing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/23/16 | rhalpai | Move event logic to common module and change to use new
  ||                    | CIG_EVENT_MGR_PK.CREATE_INSTANCE. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_evnt_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.ADD_EVNT_SP';
    lar_parm             logs.tar_parm;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntDfnId', i_evnt_dfn_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
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
                    || i_user_id
                    || '</value></row>'
                    || '</parameters>';
    logs.dbg('Create Event');
    cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                     i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                     i_event_dfn_id         => i_evnt_dfn_id,
                                     i_parameters           => l_evnt_parms,
                                     i_div_nm               => i_div,
                                     i_is_script_fw_exec    => 'Y',
                                     i_is_complete          => 'Y',
                                     i_pgm_id               => 'PLSQL',
                                     i_user_id              => i_user_id,
                                     o_event_que_id         => l_evnt_que_id
                                    );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_evnt_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_EVNT_LOG_SP
  ||  Update the event log
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  || 02/23/16 | rhalpai | Change to call CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE.
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
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.UPD_EVNT_LOG_SP';
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
  || STORE_TYP_LIST_SP
  ||  Return cursor of StoreTyp Codes and Descriptions
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  ||----------------------------------------------------------------------------
  */
  PROCEDURE store_typ_list_sp(
    o_cur  OUT  SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.STORE_TYP_LIST_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.info('ENTRY', lar_parm);

    OPEN o_cur
     FOR
       SELECT   dct.dmn_cd, dct.descr
           FROM op_dmn_cd_typ dct
          WHERE dct.dmn_typ = 'CUSTYP'
       ORDER BY dct.descr;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END store_typ_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_TMPLT_LIST_SP
  ||  Return cursor of Item Template info
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  ||----------------------------------------------------------------------------
  */
  PROCEDURE item_tmplt_list_sp(
    i_div           IN      VARCHAR2,
    i_store_typ_cd  IN      VARCHAR2,
    o_cur           OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.ITEM_TMPLT_LIST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StoreTypCd', i_store_typ_cd);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN o_cur
     FOR
       SELECT   LPAD(cpi.catlg_num, 6, '0') AS catlg_num, cpi.qty, e.ctdsce, e.shppke, e.sizee,
                NVL(di.statb, '???') AS item_stat, cpi.user_id,
                TO_CHAR(cpi.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts
           FROM care_pkg_item_cp1i cpi, sawp505e e, mclp110b di
          WHERE cpi.div_part = l_div_part
            AND cpi.store_typ_cd = i_store_typ_cd
            AND e.catite(+) = cpi.catlg_num
            AND di.div_part(+) = l_div_part
            AND di.itemb(+) = e.iteme
            AND di.uomb(+) = e.uome
       ORDER BY cpi.catlg_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_tmplt_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || CP_ORD_LIST_SP
  ||  Return cursor of CarePackage Order info
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cp_ord_list_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.CP_ORD_LIST_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);

    OPEN o_cur
     FOR
       SELECT   TO_CHAR(cpo.llr_dt, 'YYYY-MM-DD') AS llr_dt, cpo.load_num, cpo.stop_num, cpo.cust_id, c.namec,
                DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD') AS cust_stat, cpo.store_typ_cd,
                dct.descr AS store_typ_descr, cpo.conf_num, cpo.ord_num, cpo.po_num,
                TO_CHAR(ld.llr_ts, 'YYYY-MM-DD') AS ord_llr, ld.load_num AS ord_load, se.stop_num AS ord_stp,
                cpo.stat_cd, cpo.user_id, TO_CHAR(cpo.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts
           FROM div_mstr_di1d d, care_pkg_ord_cp1c cpo, op_dmn_cd_typ dct, sysp200c c, ordp100a a, load_depart_op1f ld,
                stop_eta_op1g se
          WHERE d.div_id = i_div
            AND cpo.div_part = d.div_part
            AND dct.dmn_typ = 'CUSTYP'
            AND dct.dmn_cd = cpo.store_typ_cd
            AND c.div_part(+) = cpo.div_part
            AND c.acnoc(+) = cpo.cust_id
            AND a.div_part(+) = cpo.div_part
            AND a.ordnoa(+) = cpo.ord_num
            AND ld.div_part(+) = a.div_part
            AND ld.load_depart_sid(+) = a.load_depart_sid
            AND se.div_part(+) = a.div_part
            AND se.load_depart_sid(+) = a.load_depart_sid
            AND se.cust_id(+) = a.custa
       ORDER BY cpo.llr_dt, cpo.load_num, cpo.stop_num, cpo.cust_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cp_ord_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_ITEM_TMPLT_SP
  ||  Remove all entries from CarePackage Item Template for StoreTypCd
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_item_tmplt_sp(
    i_div           IN  VARCHAR2,
    i_store_typ_cd  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.DEL_ITEM_TMPLT_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StoreTypCd', i_store_typ_cd);
    logs.info('ENTRY', lar_parm);

    DELETE FROM care_pkg_item_cp1i cpi
          WHERE cpi.div_part = (SELECT d.div_part
                                  FROM div_mstr_di1d d
                                 WHERE d.div_id = i_div)
            AND cpi.store_typ_cd = i_store_typ_cd;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_item_tmplt_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_ITEM_TMPLT_SP
  ||  Add entries for CarePackage Item Template
  ||  ParmList: CatlgNum~Qty`CatlgNum~Qty
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  || 02/23/16 | rhalpai | Change to use constants package OP_CONST_PK. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_item_tmplt_sp(
    i_div           IN  VARCHAR2,
    i_store_typ_cd  IN  VARCHAR2,
    i_parm_list     IN  CLOB,
    i_user_id       IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.INS_ITEM_TMPLT_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StoreTypCd', i_store_typ_cd);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    excp.assert((INSTR(i_parm_list, op_const_pk.field_delimiter) > 0), 'ParmList is invalid');
    logs.dbg('Remove All Entries for StoreTypCd');
    del_item_tmplt_sp(i_div, i_store_typ_cd);
    logs.dbg('Add New Entries');

    INSERT INTO care_pkg_item_cp1i
                (div_part, store_typ_cd, catlg_num, qty, user_id, last_chg_ts)
      SELECT d.div_part, i_store_typ_cd, TO_NUMBER(t.column1) AS catlg_num, TO_NUMBER(t.column2) AS qty, i_user_id,
             l_c_sysdate
        FROM TABLE(lob2table.separatedcolumns(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter)) t,
             div_mstr_di1d d
       WHERE d.div_id = i_div;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_item_tmplt_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_CP_ORDS_SP
  ||  Add entries for CarePackage Orders
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  || 03/18/11 | rhalpai | Added logic to process via event. PIR5152
  || 11/02/11 | rhalpai | Remove logic to Close Cursor after call to TO_LIST_FN
  ||                    | which now closes the cursor. IM-033903
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_cp_ords_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'OP_CARE_PACKAGE_ORDER_PK.INS_CP_ORDS_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80);

    TYPE l_tt_cpo IS TABLE OF care_pkg_ord_cp1c%ROWTYPE;

    l_t_cpos             l_tt_cpo;
    l_mcl_cust           mclp020b.mccusb%TYPE;
    l_prev_store_typ_cd  op_dmn_cd_typ.dmn_cd%TYPE   := '~';
    l_cv                 SYS_REFCURSOR;
    l_ord_dtl_list       typ.t_maxvc2;
    l_c_shp_dt  CONSTANT DATE                        := DATE '1900-01-01';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    l_section := 'Get New CarePackage Order Info';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);

    SELECT   *
    BULK COLLECT INTO l_t_cpos
        FROM care_pkg_ord_cp1c cpo
       WHERE cpo.div_part = (SELECT d.div_part
                               FROM div_mstr_di1d d
                              WHERE d.div_id = i_div)
         AND cpo.stat_cd = 'NEW'
    ORDER BY cpo.store_typ_cd;

    IF l_t_cpos.COUNT > 0 THEN
      FOR i IN l_t_cpos.FIRST .. l_t_cpos.LAST LOOP
        logs.dbg('Get MclCust');

        SELECT cx.mccusb
          INTO l_mcl_cust
          FROM mclp020b cx
         WHERE cx.div_part = l_t_cpos(i).div_part
           AND cx.custb = l_t_cpos(i).cust_id;

        IF l_prev_store_typ_cd <> l_t_cpos(i).store_typ_cd THEN
          l_prev_store_typ_cd := l_t_cpos(i).store_typ_cd;
          l_section := 'Open OrdDtl Cursor';
          logs.dbg(l_section);
          upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);

          OPEN l_cv
           FOR
             SELECT e.catite || '~' || e.iteme || '~' || e.uome || '~' || cpi.qty AS dtl
               FROM care_pkg_item_cp1i cpi, sawp505e e
              WHERE cpi.div_part = l_t_cpos(i).div_part
                AND cpi.store_typ_cd = l_t_cpos(i).store_typ_cd
                AND e.catite = cpi.catlg_num;

          l_section := 'Get OrdDtlList';
          logs.dbg(l_section);
          upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
          l_ord_dtl_list := to_list_fn(l_cv, '`');
        END IF;   -- l_prev_store_typ_cd <> l_t_cpos(i).store_typ_cd

        l_section := 'Create Order and Send to Mainframe';
        logs.dbg(l_section);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
        csr_orders_pk.ins_ord_sp(i_div,
                                 l_t_cpos(i).conf_num,
                                 NULL,
                                 l_mcl_cust,
                                 l_t_cpos(i).cust_id,
                                 l_t_cpos(i).po_num,
                                 'GRO',
                                 'R',
                                 'CAREPKG',
                                 l_c_shp_dt,
                                 i_user_id,
                                 l_ord_dtl_list,
                                 NULL
                                );
      END LOOP;
    END IF;   -- l_t_cpos.COUNT > 0

    upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Processing Complete', 1);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END ins_cp_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || EVNT_INS_CP_ORDS_SP
  ||  Create event to add entries for CarePackage Orders
  ||  ParmList:
  ||  CustId~StoreTypCd~LLRDt~Load~Stop~PO`CustId~StoreTypCd~LLRDt~Load~Stop~PO
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/18/11 | rhalpai | Original for PIR5152
  || 02/23/16 | rhalpai | Replace event logic with call to ADD_EVNT_SP.
  ||                    | Change to use constants package OP_CONST_PK. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE evnt_ins_cp_ords_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  CLOB,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.EVNT_INS_CP_ORDS_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    excp.assert((INSTR(i_parm_list, op_const_pk.field_delimiter) > 0), 'ParmList is invalid');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Add CPOrd Entries');

    INSERT INTO care_pkg_ord_cp1c
                (conf_num, div_part, cust_id, store_typ_cd, llr_dt, load_num, stop_num, po_num, ord_num, stat_cd,
                 user_id, last_chg_ts)
      SELECT 'P' || ordp100a_connba_seq.NEXTVAL, l_div_part, y.cust_id, TRIM(y.store_typ_cd),
             TO_DATE(y.llr_dt, 'YYYY-MM-DD'), y.load_num, TO_NUMBER(y.stop_num), TRIM(y.po_num), 0, 'NEW', i_user_id,
             l_c_sysdate
        FROM (SELECT t.column1 AS cust_id, t.column2 AS store_typ_cd, t.column3 AS llr_dt, t.column4 AS load_num,
                     t.column5 AS stop_num, t.column6 AS po_num
                FROM TABLE(lob2table.separatedcolumns(i_parm_list,
                                                      op_const_pk.grp_delimiter,
                                                      op_const_pk.field_delimiter
                                                     )
                          ) t) y,
             mclp020b cx
       WHERE cx.div_part = l_div_part
         AND cx.custb = y.cust_id;

    IF SQL%ROWCOUNT > 0 THEN
      COMMIT;
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_ins_cp_ords);
    END IF;   -- SQL%ROWCOUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END evnt_ins_cp_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_CP_ORDS_SP
  ||  Update LLR/Load/Stop info for matching CarePackage Orders entries
  ||  ParmList: ConfNum~LLRDt~Load~Stop`ConfNum~LLRDt~Load~Stop
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/23/16 | rhalpai | Change to use constants package OP_CONST_PK. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_cp_ords_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  CLOB,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.UPD_CP_ORDS_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_t_parms             lob_rows_t;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    excp.assert((INSTR(i_parm_list, op_const_pk.field_delimiter) > 0), 'ParmList is invalid');
    logs.dbg('Parse');
    l_t_parms := lob2table.separatedcolumns2(i_parm_list, op_const_pk.grp_delimiter, op_const_pk.field_delimiter);

    IF l_t_parms.COUNT > 0 THEN
      logs.dbg('Upd CPOrd Entry');
      FORALL i IN l_t_parms.FIRST .. l_t_parms.LAST
        UPDATE care_pkg_ord_cp1c cpo
           SET cpo.llr_dt = TO_DATE(l_t_parms(i).column2, 'YYYY-MM-DD'),
               cpo.load_num = l_t_parms(i).column3,
               cpo.stop_num = l_t_parms(i).column4,
               cpo.stat_cd = 'STG',
               cpo.user_id = i_user_id,
               cpo.last_chg_ts = l_c_sysdate
         WHERE cpo.conf_num = l_t_parms(i).column1
           AND cpo.div_part = (SELECT d.div_part
                                 FROM div_mstr_di1d d
                                WHERE d.div_id = i_div)
           AND cpo.stat_cd IN('STG', 'ERR')
           AND EXISTS(SELECT 1
                        FROM ordp100a a, load_depart_op1f ld
                       WHERE a.div_part = cpo.div_part
                         AND a.stata = 'O'
                         AND a.connba = cpo.conf_num
                         AND ld.div_part = a.div_part
                         AND ld.load_depart_sid = a.load_depart_sid
                         AND ld.load_num = 'CARE');
      COMMIT;
    END IF;   -- l_t_parms.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_cp_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOV_CP_ORDS_SP
  ||  Move staged CarePackage Orders entries to LLR/Load/Stop assignments
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/19/10 | rhalpai | Original for PIR5152
  || 03/18/11 | rhalpai | Added logic to process via event. PIR5152
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mov_cp_ords_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.MOV_CP_ORDS_SP';
    lar_parm              logs.tar_parm;
    l_section             VARCHAR2(80);
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_div_part            NUMBER;
    l_t_conf_nums         type_stab;
    l_t_cust_ids          type_stab;
    l_t_load_nums         type_stab;
    l_t_stop_nums         type_ntab;
    l_t_eta_dts           type_stab;
    l_t_eta_tms           type_ntab;
    l_t_ord_nums          type_ntab;
    l_stat_cd             VARCHAR2(3);

    PROCEDURE move_sp(
      i_div       IN      VARCHAR2,
      i_cust_id   IN      VARCHAR2,
      i_load_num  IN      VARCHAR2,
      i_stop_num  IN      NUMBER,
      i_eta_dt    IN      VARCHAR2,
      i_eta_tm    IN      NUMBER,
      i_ord_list  IN      VARCHAR2,
      i_user_id   IN      VARCHAR2,
      o_stat_cd   OUT     VARCHAR2
    ) IS
      l_mov_msg  typ.t_maxvc2;
    BEGIN
      op_order_moves_pk.move_orders_sp(i_div,
                                       i_cust_id,
                                       i_load_num,
                                       i_stop_num,
                                       i_eta_dt,
                                       i_eta_tm,
                                       i_ord_list,
                                       i_user_id,
                                       l_mov_msg,
                                       'CPMOVE'
                                      );
      o_stat_cd :=(CASE
                     WHEN l_mov_msg LIKE 'E%' THEN 'ERR'
                     ELSE 'CMP'
                   END);
    EXCEPTION
      WHEN OTHERS THEN
        o_stat_cd := 'ERR';
    END move_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_section := 'Get Move Orders Info';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);

    SELECT x.conf_num, x.cust_id, x.load_num, x.stop_num, TO_CHAR(x.eta_dt, 'YYYY-MM-DD') AS eta_dt,
           TO_NUMBER(TO_CHAR(x.eta_dt, 'HH24MI')) AS eta_tm, x.ord_num
    BULK COLLECT INTO l_t_conf_nums, l_t_cust_ids, l_t_load_nums, l_t_stop_nums, l_t_eta_dts,
           l_t_eta_tms, l_t_ord_nums
      FROM (SELECT cpo.conf_num, cpo.cust_id, cpo.load_num, cpo.stop_num,
                   TO_DATE(TO_CHAR(NEXT_DAY(cpo.llr_dt - 1, l.depdac) +(l.depwkc * 7), 'YYYYMMDD')
                           || LPAD(l.deptmc, 4, '0'),
                           'YYYYMMDDHH24MI'
                          )
                   + INTERVAL '1' MINUTE AS eta_dt,
                   a.ordnoa AS ord_num
              FROM care_pkg_ord_cp1c cpo, mclp120c l, ordp100a a, load_depart_op1f ld
             WHERE cpo.div_part = l_div_part
               AND cpo.stat_cd = 'STG'
               AND l.div_part = cpo.div_part
               AND l.loadc = cpo.load_num
               AND a.div_part = cpo.div_part
               AND a.connba = cpo.conf_num
               AND a.stata = 'O'
               AND ld.div_part = a.div_part
               AND ld.load_depart_sid = a.load_depart_sid
               AND ld.load_num = 'CARE') x;

    IF l_t_conf_nums.COUNT > 0 THEN
      l_section := 'Upd StatCd to MOV';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
      FORALL i IN l_t_conf_nums.FIRST .. l_t_conf_nums.LAST
        UPDATE care_pkg_ord_cp1c cpo
           SET cpo.stat_cd = 'MOV',
               cpo.user_id = i_user_id,
               cpo.last_chg_ts = l_c_sysdate
         WHERE cpo.div_part = l_div_part
           AND cpo.conf_num = l_t_conf_nums(i);
      COMMIT;
      FOR i IN l_t_conf_nums.FIRST .. l_t_conf_nums.LAST LOOP
        l_section := 'Move Orders';
        logs.dbg(l_section);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);
        move_sp(i_div,
                l_t_cust_ids(i),
                l_t_load_nums(i),
                l_t_stop_nums(i),
                l_t_eta_dts(i),
                l_t_eta_tms(i),
                l_t_ord_nums(i),
                i_user_id,
                l_stat_cd
               );
        l_section := 'Upd StatCd to CMP or ERR';
        logs.dbg(l_section);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_section);

        UPDATE care_pkg_ord_cp1c cpo
           SET cpo.stat_cd = l_stat_cd,
               cpo.user_id = i_user_id,
               cpo.last_chg_ts = l_c_sysdate
         WHERE cpo.div_part = l_div_part
           AND cpo.conf_num = l_t_conf_nums(i);

        COMMIT;
      END LOOP;
    END IF;   -- t_conf_nums.COUNT > 0

    upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Processing Complete', 1);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END mov_cp_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || EVNT_MOV_CP_ORDS_SP
  ||  Create event to move staged CarePackage Orders entries to LLR/Load/Stop
  ||  assignments
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/18/11 | rhalpai | Original for PIR5152
  || 02/23/16 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE evnt_mov_cp_ords_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CARE_PACKAGE_ORDER_PK.EVNT_MOV_CP_ORDS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Add Event');
    add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mov_cp_ords);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END evnt_mov_cp_ords_sp;
END op_care_package_order_pk;
/

