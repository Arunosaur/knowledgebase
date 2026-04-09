CREATE OR REPLACE PACKAGE op_allocate_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE rt_cig_alloc IS RECORD(
    div            div_mstr_di1d.div_id%TYPE,
    rlse_ts        DATE,
    ord_num        NUMBER,
    ord_ln         NUMBER,
    cust_id        sysp200c.acnoc%TYPE,
    load_num       mclp120c.loadc%TYPE,
    stop_num       NUMBER,
    catlg_num      NUMBER,
    ord_qty        PLS_INTEGER,
    allw_partl_sw  VARCHAR2(1)
  );

  TYPE rt_stamp IS RECORD(
    stamp_item     NUMBER,
    stamp_apld_cd  VARCHAR2(1)
  );

  TYPE tt_stamps IS TABLE OF rt_stamp;

  TYPE rt_allocd_cig IS RECORD(
    ord_ln         NUMBER,
    alloc_qty      PLS_INTEGER,
    inv_zone       VARCHAR2(3),
    inv_slot       VARCHAR2(7),
    pick_zone      VARCHAR2(10),
    pick_slot      VARCHAR2(7),
    cig_sel_cd     VARCHAR2(1),
    hand_stamp_sw  VARCHAR2(1),
    cust_tax_jrsdctn  NUMBER,
    t_stamps       tt_stamps
  );

  TYPE tt_allocd_cigs IS TABLE OF rt_allocd_cig;

  SUBTYPE g_st_processing IS VARCHAR2(1);

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_alloc_is_processing        CONSTANT g_st_processing := 'Y';
  g_c_alloc_is_not_processing    CONSTANT g_st_processing := 'N';
  g_c_appl_id                    CONSTANT VARCHAR2(2)     := 'OP';
  g_c_last_step_parm             CONSTANT VARCHAR2(18)   := 'ALLOCATE LAST STEP';
  g_c_processing_status          CONSTANT VARCHAR2(1)     := 'P';
  g_c_customer_is_not_protected  CONSTANT VARCHAR2(1)     := 'N';
  g_c_customer_is_protected      CONSTANT VARCHAR2(1)     := 'Y';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION is_processing_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION kit_ord_tab_fn(
    i_ord_stat  IN  VARCHAR2,
    i_o_kit     IN  kit_t
  )
    RETURN kit_ords_t;

  FUNCTION container_id_fn(
    i_div        IN  VARCHAR2,
    i_rlse_ts    IN  DATE,
    i_item       IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_manual_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE prcs_cigs_sp(
    i_div          IN  VARCHAR2,
    i_rlse_ts      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  );

  PROCEDURE evnt_cig_alloc_sp(
    i_div      IN  VARCHAR2,
    i_rlse_ts  IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  );

  PROCEDURE allocate_sp(
    i_div          IN  VARCHAR2,
    i_rlse_ts      IN  DATE,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  );

  PROCEDURE rlse_alloc_sp(
    i_div          IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  );

  PROCEDURE evnt_alloc_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  );

  PROCEDURE start_alloc_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  );

  PROCEDURE add_dummyqoprc20entry_sp(
    i_div           IN      VARCHAR2,
    i_rlse_ts_char  IN      VARCHAR2,
    o_status        OUT     VARCHAR2
  );
END op_allocate_pk;
/

CREATE OR REPLACE PACKAGE BODY op_allocate_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_inv IS RECORD(
    row_id           ROWID,
    div              div_mstr_di1d.div_id%TYPE,
    item             whsp300c.itemc%TYPE,
    uom              whsp300c.uomc%TYPE,
    whse_zone        whsp300c.taxjrc%TYPE,
    slot_zone        whsp300c.zonec%TYPE,
    qty_avail        PLS_INTEGER,
    trnsfr_qty_mult  PLS_INTEGER,
    stamps_per_item  PLS_INTEGER,
    aisl             whsp300c.aislc%TYPE,
    bin              whsp300c.binc%TYPE,
    lvl              whsp300c.levlc%TYPE,
    mstr_cs_qty      PLS_INTEGER
  );

  TYPE g_rt_bundle_item IS RECORD(
    item_num      bundl_dist_item_bd1i.item_num%TYPE,
    unq_cd        bundl_dist_item_bd1i.unq_cd%TYPE,
    wrk_item_num  whsp300c.itemc%TYPE,
    wrk_uom       whsp300c.uomc%TYPE
  );

  TYPE g_tt_bundle_items IS TABLE OF g_rt_bundle_item;

  TYPE g_rt_bundle_ord IS RECORD(
    ord_num       NUMBER,
    ord_ln        NUMBER,
    item_num      bundl_dist_item_bd1i.item_num%TYPE,
    unq_cd        bundl_dist_item_bd1i.unq_cd%TYPE,
    wrk_item_num  whsp300c.itemc%TYPE,
    wrk_uom       whsp300c.uomc%TYPE,
    ord_qty       PLS_INTEGER
  );

  TYPE g_tt_bundle_ords IS TABLE OF g_rt_bundle_ord;

  g_c_rensoft_seed_dt         CONSTANT DATE                                := TO_DATE('19000228', 'YYYYMMDD');
  g_c_dist                    CONSTANT VARCHAR2(1)                         := 'D';
  g_c_aggregate_item_typ      CONSTANT VARCHAR2(3)                         := 'AGG';
  g_c_trans_inv               CONSTANT VARCHAR2(2)                         := '99';
  g_c_trans_pick              CONSTANT VARCHAR2(2)                         := '11';
  g_c_trans_cut_from          CONSTANT VARCHAR2(2)                         := '23';
  g_c_trans_cut_to            CONSTANT VARCHAR2(2)                         := '24';
  g_c_cig_alloc_stat_inprcs   CONSTANT VARCHAR2(20)                        := 'IN-PROCESS';
  g_c_cig_alloc_stat_compl    CONSTANT VARCHAR2(20)                        := 'COMPLETE';
  g_c_cig_alloc_stat_fail     CONSTANT VARCHAR2(20)                        := 'FAILURE';
--
  g_c_prcs_beg_anlyz          CONSTANT VARCHAR2(10)                        := 'BEGANLZ';
  g_c_prcs_end_anlyz          CONSTANT VARCHAR2(10)                        := 'ENDANLZ';
  g_c_prcs_beg_alloc          CONSTANT VARCHAR2(10)                        := 'BEGALC';
  g_c_prcs_start_cig          CONSTANT VARCHAR2(10)                        := 'STRTCIG';
  g_c_prcs_beg_cig            CONSTANT VARCHAR2(10)                        := 'BEGCIG';
  g_c_prcs_cig_init           CONSTANT VARCHAR2(10)                        := 'CIGINIT';
  g_c_prcs_cig_ord_alloc      CONSTANT VARCHAR2(10)                        := 'CIGORDALC';
  g_c_prcs_cig_del_sub        CONSTANT VARCHAR2(10)                        := 'CIGDELSUB';
  g_c_prcs_cig_org_alloc      CONSTANT VARCHAR2(10)                        := 'CIGORGALC';
  g_c_prcs_cig_reset_org      CONSTANT VARCHAR2(10)                        := 'CIGRSETORG';
  g_c_prcs_cig_create_sub     CONSTANT VARCHAR2(10)                        := 'CIGCRE8SUB';
  g_c_prcs_cig_sub_alloc      CONSTANT VARCHAR2(10)                        := 'CIGSUBALC';
  g_c_prcs_cig_vndrcmp        CONSTANT VARCHAR2(10)                        := 'CIGVNDRCMP';
  g_c_prcs_cig_final          CONSTANT VARCHAR2(10)                        := 'CIGFINAL';
  g_c_prcs_end_cig            CONSTANT VARCHAR2(10)                        := 'ENDCIG';
  g_c_prcs_start_noncig       CONSTANT VARCHAR2(10)                        := 'STRTNCIG';
  g_c_prcs_xdock              CONSTANT VARCHAR2(10)                        := 'XDOCK';
  g_c_prcs_bndl               CONSTANT VARCHAR2(10)                        := 'BNDLALC';
  g_c_prcs_kit                CONSTANT VARCHAR2(10)                        := 'KITALC';
  g_c_prcs_itm                CONSTANT VARCHAR2(10)                        := 'ITMALC';
  g_c_prcs_ord                CONSTANT VARCHAR2(10)                        := 'ORDALC';
  g_c_prcs_del_subs           CONSTANT VARCHAR2(10)                        := 'DELSUB';
  g_c_prcs_orig_ords          CONSTANT VARCHAR2(10)                        := 'ORGALC';
  g_c_prcs_create_subs        CONSTANT VARCHAR2(10)                        := 'CRE8SUB';
  g_c_prcs_sub                CONSTANT VARCHAR2(10)                        := 'SUBALC';
  g_c_prcs_vndr_cmp           CONSTANT VARCHAR2(10)                        := 'VNDRCMP';
  g_c_prcs_end_noncig         CONSTANT VARCHAR2(10)                        := 'ENDNCIG';
  g_c_prcs_wait_cig           CONSTANT VARCHAR2(10)                        := 'WAITCIG';
  g_c_prcs_upd_partls         CONSTANT VARCHAR2(10)                        := 'UPPARTL';
  g_c_prcs_ords_allocd        CONSTANT VARCHAR2(10)                        := 'ORDSALC';
  g_c_prcs_ins_po_ovrrd       CONSTANT VARCHAR2(10)                        := 'INSPOOV';
  g_c_prcs_cube_tote          CONSTANT VARCHAR2(10)                        := 'CUBETOT';
  g_c_prcs_ext_wrk_ords       CONSTANT VARCHAR2(10)                        := 'BEGEXTW';
  g_c_prcs_upd_wrk_ord_stats  CONSTANT VARCHAR2(10)                        := 'UPDWOST';
  g_c_prcs_tag_excpt_ords     CONSTANT VARCHAR2(10)                        := 'TAGXORD';
  g_c_prcs_gov_cntl_rstr      CONSTANT VARCHAR2(10)                        := 'GOVRSTR';
  g_c_prcs_ext_ords           CONSTANT VARCHAR2(10)                        := 'BEGEXTO';
  g_c_prcs_build_mfst         CONSTANT VARCHAR2(10)                        := 'BLDMFST';
  g_c_prcs_tote_fcst          CONSTANT VARCHAR2(10)                        := 'BEGTOTF';
  g_c_prcs_ext_tote_msgs      CONSTANT VARCHAR2(10)                        := 'BEGTOTM';
  g_c_prcs_ecom_moq_extr      CONSTANT VARCHAR2(10)                        := 'ECOMMOQ';
  g_c_prcs_upd_ord_stat       CONSTANT VARCHAR2(10)                        := 'UPORDST';
  g_c_prcs_unlock_ld_clos     CONSTANT VARCHAR2(10)                        := 'ULLDCLS';
  g_c_prcs_itm_ration_rpt     CONSTANT VARCHAR2(10)                        := 'RATNRPT';
  g_c_prcs_wkmaxqty_cut       CONSTANT VARCHAR2(10)                        := 'WKMAXCUT';
  g_c_prcs_end_alloc          CONSTANT VARCHAR2(10)                        := 'ENDALC';
  g_c_prcs_qoprc12            CONSTANT VARCHAR2(10)                        := 'QOPRC12';
  g_c_prcs_qoprc18            CONSTANT VARCHAR2(10)                        := 'QOPRC18';
  g_c_prcs_qoprc06            CONSTANT VARCHAR2(10)                        := 'QOPRC06';
  g_c_prcs_qoprc07            CONSTANT VARCHAR2(10)                        := 'QOPRC07';
  g_c_prcs_imq62              CONSTANT VARCHAR2(10)                        := 'IMQ62';
  g_c_prcs_create_mfst        CONSTANT VARCHAR2(10)                        := 'CRE8MFST';
  g_c_prcs_opld01             CONSTANT VARCHAR2(10)                        := 'OPLD01';
  g_c_prcs_opld02             CONSTANT VARCHAR2(10)                        := 'OPLD02';
  g_c_prcs_opld03             CONSTANT VARCHAR2(10)                        := 'OPLD03';
  g_c_prcs_opld04             CONSTANT VARCHAR2(10)                        := 'OPLD04';
  g_c_prcs_opld05             CONSTANT VARCHAR2(10)                        := 'OPLD05';
  g_c_prcs_opld06             CONSTANT VARCHAR2(10)                        := 'OPLD06';
  g_c_prcs_opld07             CONSTANT VARCHAR2(10)                        := 'OPLD07';
  g_c_prcs_opld08             CONSTANT VARCHAR2(10)                        := 'OPLD08';
  g_c_prcs_qoprc20            CONSTANT VARCHAR2(10)                        := 'QOPRC20';
  g_parms_sw                           VARCHAR2(1)                         := 'N';
  g_t_rlse_loads                       type_stab;
  g_appl_srvr                          appl_sys_parm_ap1s.vchar_val%TYPE;
  g_log_inv_trans_sw                   VARCHAR2(1);
  g_ration_items_sw                    VARCHAR2(1);
  g_cubing_of_totes_sw                 VARCHAR2(1);
  g_cube_all_by_hc_sw                  VARCHAR2(1);
  g_xdock_pick_compl_sw                VARCHAR2(1);
  g_cwt_compl_sw                       VARCHAR2(1);
  g_t_mstr_cs_crps                     type_stab;
  g_t_nacs_tobacco_catgs               type_stab;
  g_t_cntnr_trk_po_lvl_crps            type_stab;
  g_t_cntnr_trk_po_lvl_vals            type_stab;
  g_t_snglpo_crps                      type_stab;
  g_t_rstr_out_qty_crps                type_stab;
  g_t_xdock_loads                      type_stab;
  g_t_xdock_mfsts                      type_stab;
  g_ins_inv_trans_first_sw             VARCHAR2(1)                         := 'Y';
  g_e_error                            EXCEPTION;

  CURSOR g_cur_rlse(
    b_div          VARCHAR2,
    b_evnt_que_id  NUMBER,
    b_cycl_id      NUMBER,
    b_cycl_dfn_id  NUMBER
  ) IS
    SELECT d.div_id, d.div_part, r.rlse_id, r.rlse_ts, TO_CHAR(r.rlse_ts, 'YYYYMMDDHH24MISS') AS rlse_ts_char, r.llr_dt,
           r.test_bil_cd, r.forc_inv_sw, r.strtg_id, r.user_id, TO_NUMBER(TO_CHAR(r.rlse_ts, 'DD')) - 1 AS tran_part_id,
           (SELECT ',' || LISTAGG(l.val, ',') WITHIN GROUP (ORDER BY l.val) || ','
              FROM rlse_log_op2z l
             WHERE l.div_part = r.div_part
               AND l.rlse_id = r.rlse_id
               AND l.typ_id = 'LOAD') AS load_list, b_evnt_que_id AS evnt_que_id, b_cycl_id AS cycl_id,
           b_cycl_dfn_id AS cycl_dfn_id
      FROM div_mstr_di1d d, rlse_op1z r
     WHERE d.div_id = b_div
       AND r.div_part = d.div_part
       AND r.rlse_id = (SELECT DISTINCT FIRST_VALUE(r2.rlse_id) OVER(ORDER BY r2.rlse_ts DESC)
                                   FROM div_mstr_di1d d2, rlse_op1z r2
                                  WHERE d2.div_id = b_div
                                    AND r2.div_part = d2.div_part
                                    AND r2.stat_cd = 'P');

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || GET_PARMS_SP
  ||  Retrieve parms to global variables.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/17 | rhalpai | Original. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_parms_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_t_parms  op_types_pk.tt_varchars_v;
  BEGIN
    IF g_parms_sw = 'N' THEN
      g_t_rlse_loads := str.parse_list(i_r_rlse.load_list);
      l_t_parms := op_parms_pk.idx_vals_fn(i_r_rlse.div_part,
                                           op_const_pk.prm_appl_srvr
                                           || ','
                                           || op_const_pk.prm_alloc_log_inv_trans
                                           || ','
                                           || op_const_pk.prm_ration_items
                                           || ','
                                           || op_const_pk.prm_cubing_of_totes
                                           || ','
                                           || op_const_pk.prm_cube_all_by_hc
                                           || ','
                                           || op_const_pk.prm_xdock_pick_compl
                                           || ','
                                           || op_const_pk.prm_cwt_compl
                                          );
      g_appl_srvr := l_t_parms(op_const_pk.prm_appl_srvr);
      g_log_inv_trans_sw := NVL(l_t_parms(op_const_pk.prm_alloc_log_inv_trans), 'N');
      g_ration_items_sw := NVL(l_t_parms(op_const_pk.prm_ration_items), 'N');
      g_cubing_of_totes_sw := NVL(l_t_parms(op_const_pk.prm_cubing_of_totes), 'N');
      g_cube_all_by_hc_sw := NVL(l_t_parms(op_const_pk.prm_cube_all_by_hc), 'N');
      g_xdock_pick_compl_sw := NVL(l_t_parms(op_const_pk.prm_xdock_pick_compl), 'N');
      g_cwt_compl_sw := NVL(l_t_parms(op_const_pk.prm_cwt_compl), 'N');
      g_t_mstr_cs_crps := op_parms_pk.parms_for_val_fn(i_r_rlse.div_part, op_const_pk.prm_alloc_mstr_cs, 'Y', 3);
      g_t_nacs_tobacco_catgs := op_parms_pk.vals_for_prfx_fn(i_r_rlse.div_part, op_const_pk.prm_nacs_tobacco);
      op_parms_pk.get_parms_for_prfx_sp(i_r_rlse.div_part,
                                        op_const_pk.prm_cntnr_trk_po_lvl,
                                        g_t_cntnr_trk_po_lvl_crps,
                                        g_t_cntnr_trk_po_lvl_vals,
                                        3
                                       );
      g_t_snglpo_crps := op_parms_pk.parms_for_val_fn(i_r_rlse.div_part, op_const_pk.prm_po_ovride_snglpo, 'Y', 3);
      g_t_rstr_out_qty_crps := op_parms_pk.vals_for_prfx_fn(i_r_rlse.div_part, op_const_pk.prm_rstr_out_qty);
      g_t_xdock_loads := op_parms_pk.vals_for_prfx_fn(i_r_rlse.div_part, op_const_pk.prm_xdock_load);
      g_t_xdock_mfsts := op_parms_pk.vals_for_prfx_fn(i_r_rlse.div_part, op_const_pk.prm_xdock_mfst);
      g_parms_sw := 'Y';
    END IF;   -- g_parms_sw = 'N'
  END get_parms_sp;

  /*
  ||----------------------------------------------------------------------------
  || RLSE_INFO_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/15 | rhalpai | Original
  || 10/14/17 | rhalpai | Add call to get_parms_sp. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION rlse_info_fn(
    i_div          IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  )
    RETURN g_cur_rlse%ROWTYPE IS
    l_r_rlse  g_cur_rlse%ROWTYPE;
  BEGIN
    OPEN g_cur_rlse(i_div, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);

    FETCH g_cur_rlse
     INTO l_r_rlse;

    CLOSE g_cur_rlse;

    get_parms_sp(l_r_rlse);
    RETURN(l_r_rlse);
  END rlse_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_LN_CNT_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_ln_cnt_fn(
    i_r_rlse      IN  g_cur_rlse%ROWTYPE,
    i_item_typ    IN  VARCHAR2,
    i_ord_stat_1  IN  VARCHAR2,
    i_ord_stat_2  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN NUMBER IS
    l_cnt  PLS_INTEGER;
  BEGIN
    SELECT COUNT(1)
      INTO l_cnt
      FROM load_depart_op1f ld, ordp100a a, ordp120b b
     WHERE ld.div_part = i_r_rlse.div_part
       AND ld.llr_dt = i_r_rlse.llr_dt
       AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
       AND a.div_part = ld.div_part
       AND a.load_depart_sid = ld.load_depart_sid
       AND a.excptn_sw = 'N'
       AND a.stata = 'P'
       AND b.div_part = a.div_part
       AND b.ordnob = a.ordnoa
       AND b.excptn_sw = 'N'
       AND b.statb IN(i_ord_stat_1, i_ord_stat_2)
       AND (   i_item_typ IS NULL
            OR (    i_item_typ = 'CIG'
                AND b.sllumb IN('CII', 'CIR', 'CIC'))
            OR (    i_item_typ = 'NONCIG'
                AND b.sllumb NOT IN('CII', 'CIR', 'CIC'))
           )
       AND b.ntshpb IS NULL;

    RETURN(l_cnt);
  END ord_ln_cnt_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOG_STEP_SP
  ||  Log allocation process step
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_step_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_typ_id  IN  VARCHAR2,
    i_val     IN  VARCHAR2
  ) IS
  BEGIN
    INSERT INTO rlse_log_op2z
                (div_part, rlse_id, typ_id, create_ts, val
                )
         VALUES (i_r_rlse.div_part, i_r_rlse.rlse_id, i_typ_id, SYSDATE, NVL(i_val, '~')
                );

    COMMIT;
  END log_step_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOG_PRCS_STEP_SP
  ||  Log allocation process step and set Release step
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_prcs_step_sp(
    i_r_rlse   IN  g_cur_rlse%ROWTYPE,
    i_prcs_cd  IN  VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_c_module  CONSTANT typ.t_maxfqnm            := 'OP_ALLOCATE_PK.LOG_PRCS_STEP_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_log_val            rlse_log_op2z.val%TYPE;
    l_item_typ           VARCHAR2(6);
    l_t_tbls             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'PrcsCd', i_prcs_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Release and ItemTyp Info');

    OPEN l_cv
     FOR
       SELECT     DECODE(op9z.parnt_typ, g_c_prcs_start_cig, 'CIG', 'NONCIG')
             FROM rlse_typ_dmn_op9z op9z
            WHERE op9z.typ_id = i_prcs_cd
       START WITH op9z.parnt_typ IN(g_c_prcs_start_cig, g_c_prcs_start_noncig)
       CONNECT BY PRIOR op9z.typ_id = op9z.parnt_typ;

    FETCH l_cv
     INTO l_item_typ;

    CLOSE l_cv;

    logs.dbg('Set LogVal with OrdLnCnts as needed');
    l_log_val :=(CASE
                   WHEN i_prcs_cd = g_c_prcs_beg_alloc THEN 'RecCnt: ' || ord_ln_cnt_fn(i_r_rlse, l_item_typ, 'P')
                   WHEN(   i_prcs_cd = g_c_prcs_ords_allocd
                        OR l_item_typ IN('CIG', 'NONCIG')) THEN 'AlcCnt: '
                                                                || ord_ln_cnt_fn(i_r_rlse, l_item_typ, 'T')
                                                                || ' TtlCnt: '
                                                                || ord_ln_cnt_fn(i_r_rlse, l_item_typ, 'P', 'T')
                 END
                );
    log_step_sp(i_r_rlse, i_prcs_cd, l_log_val);
    logs.dbg('Check for Analyze Parm');
    l_t_tbls := op_parms_pk.vals_for_prfx_fn(i_r_rlse.div_part, 'ANLYZ_' || i_prcs_cd);

    IF l_t_tbls.COUNT > 0 THEN
      FOR i IN l_t_tbls.FIRST .. l_t_tbls.LAST LOOP
        logs.dbg('Analyze Table ' || l_t_tbls(i));
        log_step_sp(i_r_rlse, g_c_prcs_beg_anlyz, l_t_tbls(i));
        analyze_table_sp(l_t_tbls(i));
      END LOOP;
      log_step_sp(i_r_rlse, g_c_prcs_end_anlyz, NULL);
    END IF;   -- l_t_tbls.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END log_prcs_step_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_EVNT_LOG_SP
  ||  Update the event log
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/10 | rhalpai | Original for PIR8377
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_evnt_log_sp(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_evnt_msg   IN  VARCHAR2,
    i_finish_cd  IN  NUMBER DEFAULT 0
  ) IS
  BEGIN
    cig_event_mgr_pk.update_log_message(i_r_rlse.evnt_que_id,
                                        i_r_rlse.cycl_id,
                                        i_r_rlse.cycl_dfn_id,
                                        i_evnt_msg,
                                        i_finish_cd
                                       );
  END upd_evnt_log_sp;

  /*
  ||----------------------------------------------------------------------------
  || RLSE_HAS_CIGS_FN
  ||  Indicate whether release contains order lines for cig items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION rlse_has_cigs_fn(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  )
    RETURN BOOLEAN IS
    l_cv         SYS_REFCURSOR;
    l_exists_sw  VARCHAR2(1)   := 'N';
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM load_depart_op1f ld, ordp100a a, ordp120b b
        WHERE ld.div_part = i_r_rlse.div_part
          AND ld.llr_dt = i_r_rlse.llr_dt
          AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
          AND a.div_part = ld.div_part
          AND a.load_depart_sid = ld.load_depart_sid
          AND a.excptn_sw = 'N'
          AND b.div_part = a.div_part
          AND b.ordnob = a.ordnoa
          AND b.excptn_sw = 'N'
          AND b.statb = 'P'
          AND b.subrcb < 999
          AND b.sllumb IN('CII', 'CIR', 'CIC');

    FETCH l_cv
     INTO l_exists_sw;

    RETURN(l_exists_sw = 'Y');
  END rlse_has_cigs_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_XDOCK_SP
  ||  Set cross-dock order lines to allocated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 05/20/09 | rhalpai | Changed to log transactions using a select from the
  ||                    | order details for lines just updated to T status
  ||                    | instead of loading collections of order line info
  ||                    | using RETURNING BULK COLLECT on the UPDATE. This was
  ||                    | done to reduce memory required for the collections
  ||                    | loaded by the UPDATE RETURNING BULK COLLECT. IM506029
  || 03/01/10 | rhalpai | Removed LLR parm and changed logic to capture updated
  ||                    | order line info to be used for logging transactions.
  ||                    | PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  || 07/10/12 | rhalpai | Remove unused column, TICKTB. PIR11038
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F.
  ||                    | Convert INSERT ALL to single inserts to allow for
  ||                    | Edition-Based Redefinition. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_xdock_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.UPD_XDOCK_SP';
    lar_parm             logs.tar_parm;
    l_t_ord_nums         type_ntab;
    l_t_ord_lns          type_ntab;
    l_t_items            type_stab;
    l_t_uoms             type_stab;
    l_t_ord_qtys         type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.dbg('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Upd Ords');

    UPDATE    ordp120b b
          SET b.statb = 'T',
              b.alcqtb = b.ordqtb,
              b.pckqtb = b.ordqtb
        WHERE b.excptn_sw = 'N'
          AND b.statb = 'P'
          AND b.div_part = i_r_rlse.div_part
          AND b.ordnob IN(SELECT a.ordnoa
                            FROM load_depart_op1f ld, ordp100a a, sub_prcs_ord_src s
                           WHERE ld.div_part = i_r_rlse.div_part
                             AND ld.llr_dt = i_r_rlse.llr_dt
                             AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                             AND a.div_part = ld.div_part
                             AND a.load_depart_sid = ld.load_depart_sid
                             AND s.div_part = a.div_part
                             AND s.ord_src = a.ipdtsa
                             AND s.prcs_id = 'ALLOCATE'
                             AND s.prcs_sbtyp_cd = 'BZI')
    RETURNING         b.ordnob, b.lineb, b.itemnb, b.sllumb, b.ordqtb
    BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns, l_t_items, l_t_uoms, l_t_ord_qtys;

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Log Transactions');
      FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
        INSERT INTO tran_op2t
                    (tran_id, part_id, rlse_id, tran_typ,
                     pgm_id, div_part
                    )
             VALUES (op1a_tran_id_seq.NEXTVAL, i_r_rlse.tran_part_id, i_r_rlse.rlse_id, g_c_trans_pick,
                     'UPD_XDOCK_SP', i_r_rlse.div_part
                    );

        INSERT INTO tran_ord_op2o
                    (tran_id, part_id, ord_num, ord_ln, alloc_qty,
                     div_part
                    )
             VALUES (op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, l_t_ord_nums(i), l_t_ord_lns(i), l_t_ord_qtys(i),
                     i_r_rlse.div_part
                    );

        INSERT INTO tran_item_op2i
                    (tran_id, part_id, catlg_num, inv_zone, inv_aisle, inv_bin, inv_lvl, pick_zone, pick_aisle,
                     pick_bin, pick_lvl, qty, div_part)
          SELECT op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, e.catite, '~', w1.aislc, w1.binc, w1.levlc, '~',
                 w1.aislc, w1.binc, w1.levlc, l_t_ord_qtys(i), i_r_rlse.div_part
            FROM sawp505e e, whsp300c w1
           WHERE e.iteme = l_t_items(i)
             AND e.uome = l_t_uoms(i)
             AND w1.ROWID = (SELECT MAX(w2.ROWID)
                               FROM whsp300c w2
                              WHERE w2.div_part = i_r_rlse.div_part
                                AND w2.itemc = l_t_items(i)
                                AND w2.uomc = l_t_uoms(i)
                                AND w2.taxjrc IS NULL
                                AND w2.qavc = (SELECT MAX(w3.qavc)
                                                 FROM whsp300c w3
                                                WHERE w3.div_part = i_r_rlse.div_part
                                                  AND w3.itemc = l_t_items(i)
                                                  AND w3.uomc = l_t_uoms(i)
                                                  AND w3.taxjrc IS NULL));
      END LOOP;
    END IF;   -- l_t_ord_nums.count > 0

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_xdock_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_INV_TRANS_SP
  ||  Log inventory transaction.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  || 10/14/17 | rhalpai | Remove call to get parm as it is now referenced in a
  ||                    | global variable and loaded earlier. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_inv_trans_sp(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_item       IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_aisl       IN  VARCHAR2,
    i_bin        IN  VARCHAR2,
    i_lvl        IN  VARCHAR2,
    i_qty        IN  NUMBER,
    i_pgm_id     IN  VARCHAR2,
    i_whse_zone  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_whse_zone    tran_item_op2i.inv_zone%TYPE   := NVL(i_whse_zone, '~');
    l_catlg_num    NUMBER;
    l_new_item_sw  VARCHAR2(1)                    := 'Y';

    PROCEDURE ins_tran_sp(
      i_div_part  IN  NUMBER,
      i_rlse_id   IN  NUMBER,
      i_part_id   IN  NUMBER,
      i_pgm_id    IN  VARCHAR2
    ) IS
    BEGIN
      INSERT INTO tran_op2t
                  (tran_id, div_part, part_id, rlse_id, tran_typ, pgm_id
                  )
           VALUES (op1a_tran_id_seq.NEXTVAL, i_div_part, i_part_id, i_rlse_id, g_c_trans_inv, i_pgm_id
                  );
    END ins_tran_sp;
  BEGIN
    IF g_log_inv_trans_sw = 'Y' THEN
      l_catlg_num := op_item_pk.catlg_num_fn(i_item, i_uom);

      IF g_ins_inv_trans_first_sw = 'Y' THEN
        ins_tran_sp(i_r_rlse.div_part, i_r_rlse.rlse_id, i_r_rlse.tran_part_id, i_pgm_id);
        g_ins_inv_trans_first_sw := 'N';
      ELSE
        SELECT NVL(MAX('Y'), 'N')
          INTO l_new_item_sw
          FROM DUAL
         WHERE EXISTS(SELECT 1
                        FROM tran_op2t t
                       WHERE t.div_part = i_r_rlse.div_part
                         AND t.rlse_id = i_r_rlse.rlse_id
                         AND t.part_id = i_r_rlse.tran_part_id
                         AND t.tran_typ = g_c_trans_inv
                         AND NOT EXISTS(SELECT 1
                                          FROM tran_item_op2i ti
                                         WHERE ti.div_part = t.div_part
                                           AND ti.tran_id = t.tran_id
                                           AND ti.part_id = t.part_id
                                           AND ti.catlg_num = l_catlg_num
                                           AND ti.inv_zone = l_whse_zone));
      END IF;   -- g_ins_inv_trans_first_sw = 'Y'

      IF l_new_item_sw = 'Y' THEN
        IF g_ins_inv_trans_first_sw = 'N' THEN
          ins_tran_sp(i_r_rlse.div_part, i_r_rlse.rlse_id, i_r_rlse.tran_part_id, i_pgm_id);
        END IF;   -- g_ins_inv_trans_first_sw = 'N'

        INSERT INTO tran_item_op2i
                    (tran_id, part_id, catlg_num, inv_zone, inv_aisle, inv_bin, inv_lvl,
                     pick_zone, pick_aisle, pick_bin, pick_lvl, qty, div_part
                    )
             VALUES (op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, l_catlg_num, l_whse_zone, i_aisl, i_bin, i_lvl,
                     l_whse_zone, i_aisl, i_bin, i_lvl, i_qty, i_r_rlse.div_part
                    );
      END IF;   -- l_new_item_sw = 'Y'
    END IF;   -- g_log_inv_trans_sw = 'Y'
  END ins_inv_trans_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_INV_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - PIR2909
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  || 06/30/09 | rhalpai | Added MstrCaseQty to be returned with inventory
  ||                    | record. PIR7548
  || 03/01/10 | rhalpai | Added ReleaseTS parm and changed logic to include
  ||                    | logging inventory transactions. PIR0024
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_inv_fn(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_item       IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_whse_zone  IN  VARCHAR2 DEFAULT NULL,
    i_rec_typ    IN  VARCHAR2 DEFAULT NULL
  )
    RETURN g_rt_inv IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.GET_INV_FN';
    lar_parm             logs.tar_parm;
    l_r_inv              g_rt_inv;

    CURSOR l_cur_inv(
      b_div_id     VARCHAR2,
      b_div_part   NUMBER,
      b_item       VARCHAR2,
      b_uom        VARCHAR2,
      b_whse_zone  VARCHAR2,
      b_rec_typ    VARCHAR2
    ) RETURN g_rt_inv IS
      SELECT        w1.ROWID AS row_id, b_div_id AS div, w1.itemc AS item, w1.uomc AS uom, w1.taxjrc AS whse_zone,
                    w1.zonec AS slot_zone, w1.qavc AS qty_avail, e.fmqtye AS trnsfr_qty_mult,
                    (CASE b_rec_typ
                       WHEN 'POST_STAMP' THEN e.nustme
                       ELSE 0
                     END) AS stamps_per_item, w1.aislc AS aisl, w1.binc AS bin, w1.levlc AS lvl,
                    e.mulsle AS mstr_cs_qty
               FROM whsp300c w1, sawp505e e
              WHERE w1.div_part = b_div_part
                AND w1.ROWID = (SELECT MAX(w2.ROWID)
                                  FROM whsp300c w2
                                 WHERE w2.div_part = b_div_part
                                   AND w2.itemc = b_item
                                   AND w2.uomc = b_uom
                                   AND NVL(w2.taxjrc, ' ') = NVL(b_whse_zone, ' ')
                                   AND w2.qavc = (SELECT MAX(w3.qavc)
                                                    FROM whsp300c w3
                                                   WHERE w3.div_part = w2.div_part
                                                     AND w3.itemc = w2.itemc
                                                     AND w3.uomc = w2.uomc
                                                     AND NVL(w3.taxjrc, ' ') = NVL(w2.taxjrc, ' ')))
                AND e.iteme = w1.itemc
                AND e.uome = w1.uomc
      FOR UPDATE OF w1.qavc;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'WhseZone', i_whse_zone);
    logs.add_parm(lar_parm, 'RecTyp', i_rec_typ);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor');

    OPEN l_cur_inv(i_r_rlse.div_id, i_r_rlse.div_part, i_item, i_uom, i_whse_zone, i_rec_typ);

    logs.dbg('Fetch Cursor');

    FETCH l_cur_inv
     INTO l_r_inv;

    CLOSE l_cur_inv;

    IF l_r_inv.item IS NOT NULL THEN
      logs.dbg('Add Inventory Transaction');
      ins_inv_trans_sp(i_r_rlse,
                       l_r_inv.item,
                       l_r_inv.uom,
                       l_r_inv.aisl,
                       l_r_inv.bin,
                       l_r_inv.lvl,
                       l_r_inv.qty_avail,
                       'GET_INV_FN',
                       l_r_inv.whse_zone
                      );
    END IF;   -- l_r_inv.item IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_r_inv);
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_inv%ISOPEN THEN
        CLOSE l_cur_inv;
      END IF;   -- l_cur_inv%ISOPEN

      logs.err(lar_parm);
  END get_inv_fn;

  /*
  ||----------------------------------------------------------------------------
  || ADD_TO_TAB_SP
  ||  Append value to numbers collection table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/07/06 | rhalpai | Moved from ALLOC_KITS_SP to allow calls from
  ||                    | ALLOC_KITS_SP and new ALLOC_BUNDLE_DIST_SP.
  ||                    | PIR2545
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_to_tab_sp(
    io_tab  IN OUT NOCOPY  type_ntab,
    i_val   IN             NUMBER
  ) IS
  BEGIN
    IF io_tab IS NULL THEN
      io_tab := type_ntab(NULL);
    ELSE
      io_tab.EXTEND;
    END IF;   -- io_tab IS NULL

    io_tab(io_tab.COUNT) := i_val;
  END add_to_tab_sp;

  /*
  ||----------------------------------------------------------------------------
  || OUT_ORDS_SP
  ||  Update orders to "out of stock".
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/07/06 | rhalpai | Moved from ALLOC_KITS_SP to allow calls from
  ||                    | ALLOC_KITS_SP and new ALLOC_BUNDLE_DIST_SP.
  ||                    | PIR2545
  ||----------------------------------------------------------------------------
  */
  PROCEDURE out_ords_sp(
    i_r_rlse      IN  g_cur_rlse%ROWTYPE,
    i_t_ord_nums  IN  type_ntab,
    i_t_ord_lns   IN  type_ntab
  ) IS
  BEGIN
    IF (    i_t_ord_nums IS NOT NULL
        AND i_t_ord_nums.COUNT > 0) THEN
      env.tag();
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        UPDATE ordp120b b
           SET b.statb = 'T',
               b.alcqtb = 0,
               b.pckqtb = 0,
               b.ntshpb = DECODE((SELECT a.dsorda
                                    FROM ordp100a a
                                   WHERE a.div_part = i_r_rlse.div_part
                                     AND a.ordnoa = i_t_ord_nums(i)),
                                 'D', 'DISOUT',
                                 'INVOUT'
                                )
         WHERE b.div_part = i_r_rlse.div_part
           AND b.ordnob = i_t_ord_nums(i)
           AND b.lineb = i_t_ord_lns(i)
           AND b.excptn_sw = 'N';
      env.untag();
    END IF;   -- i_t_ord_nums IS NOT NULL
  END out_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_ORD_LN_SP
  ||  Set order line to allocated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 07/10/12 | rhalpai | Remove unused column, TICKTB. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_ord_ln_sp(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_alloc_qty  IN  NUMBER,
    i_ord_stat   IN  VARCHAR2
  ) IS
  BEGIN
    UPDATE ordp120b
       SET statb = i_ord_stat,
           alcqtb = i_alloc_qty,
           pckqtb = i_alloc_qty
     WHERE div_part = i_r_rlse.div_part
       AND ordnob = i_ord_num
       AND lineb = i_ord_ln;
  END alloc_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_ORIG_ORD_LN_SP
  ||  Update of original order line if sub is allocated
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_orig_ord_ln_sp(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_alloc_qty  IN  PLS_INTEGER,
    i_sub_cd     IN  NUMBER
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_ORIG_ORD_LN_SP';
    lar_parm                  logs.tar_parm;
    l_c_orig_ord_ln  CONSTANT NUMBER        := FLOOR(i_ord_ln);
    l_ord_stat                VARCHAR2(3);
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'AllocQty', i_alloc_qty);
    logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
    logs.dbg('ENTRY', lar_parm);

    IF l_c_orig_ord_ln <> i_ord_ln THEN
      timer.startme(l_c_module || env.get_session_id);
      logs.dbg('Upd Orig Line');
      -- Check for Order Receipt Sub
      l_ord_stat :=(CASE
                      WHEN(i_ord_ln - l_c_orig_ord_ln) > .69 THEN 'T'
                      ELSE 'P'
                    END);

      UPDATE ordp120b b
         SET b.statb = l_ord_stat,
             b.alcqtb = DECODE(l_ord_stat, 'T', b.ordqtb, NVL(b.alcqtb, 0) + i_alloc_qty),
             b.ntshpb = DECODE(i_sub_cd,
                               1, 'UCONDSU',
                               2, 'ITEMREP',
                               3, 'FCRD-SS',
                               4, 'CONDSUB',
                               5, 'ITEMSUB',
                               'ITEMSUB'
                              )
       WHERE b.div_part = i_r_rlse.div_part
         AND b.ordnob = i_ord_num
         AND b.lineb = l_c_orig_ord_ln;

      timer.stopme(l_c_module || env.get_session_id);
      logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   -- l_c_orig_ord_ln <> i_ord_ln
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END alloc_orig_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_PICK_TRANS_SP
  ||  Log pick transaction.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 05/20/09 | rhalpai | Changed logic to use an IF-condition and 2 INSERTS
  ||                    | to log transactions instead of using a single INSERT
  ||                    | with a DECODE. A DECODE for CURRVAL or NEXTVAL will
  ||                    | cause the NEXTVAL to be fired for every row even if
  ||                    | the result of the DECODE is CURRVAL. IM506029
  || 03/01/10 | rhalpai | Replaced inv_rowid parm with parms for item/uom/
  ||                    | inventory slot/cig pick slot and changed logic to log
  ||                    | the cig pick slot in the to-slot columns. PIR0024
  || 08/17/10 | rhalpai | Added inventory zone parm and changed logic to use it
  ||                    | when logging cig non-stamp transactions. PIR0024
  || 08/29/11 | rhalpai | Convert to use new transaction tables.
  ||                    | Replace TranLn parm with StampSw and add parms for
  ||                    | CigSelCd,HandStampSw,StampTab.
  ||                    | PIR7990
  || 07/04/13 | rhalpai | Convert INSERT ALL to single inserts to allow for
  ||                    | Edition-Based Redefinition. PIR11038
  || 11/22/21 | rhalpai | Add cust_tax_jrsdctn. PIR21509
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_pick_trans_sp(
    i_r_rlse         IN  g_cur_rlse%ROWTYPE,
    i_stamp_sw       IN  VARCHAR2,
    i_item           IN  VARCHAR2,
    i_uom            IN  VARCHAR2,
    i_pick_aisl      IN  VARCHAR2,
    i_pick_bin       IN  VARCHAR2,
    i_pick_lvl       IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_ord_ln         IN  NUMBER,
    i_qty            IN  NUMBER,
    i_pgm_id         IN  VARCHAR2,
    i_pick_zone      IN  VARCHAR2 DEFAULT NULL,
    i_inv_zone       IN  VARCHAR2 DEFAULT NULL,
    i_inv_aisl       IN  VARCHAR2 DEFAULT NULL,
    i_inv_bin        IN  VARCHAR2 DEFAULT NULL,
    i_inv_lvl        IN  VARCHAR2 DEFAULT NULL,
    i_cig_sel_cd     IN  VARCHAR2 DEFAULT NULL,
    i_hand_stamp_sw  IN  VARCHAR2 DEFAULT NULL,
    i_cust_tax_jrsdctn  IN  NUMBER DEFAULT NULL,
    i_t_stamps       IN  op_allocate_pk.tt_stamps DEFAULT NULL
  ) IS
    l_catlg_num  PLS_INTEGER;
  BEGIN
    IF i_stamp_sw = 'N' THEN
      INSERT INTO tran_op2t
                  (tran_id, part_id, rlse_id, tran_typ,
                   pgm_id, div_part
                  )
           VALUES (op1a_tran_id_seq.NEXTVAL, i_r_rlse.tran_part_id, i_r_rlse.rlse_id, g_c_trans_pick,
                   SUBSTR(i_pgm_id, 1, 20), i_r_rlse.div_part
                  );

      INSERT INTO tran_ord_op2o
                  (tran_id, part_id, ord_num, ord_ln, alloc_qty, div_part
                  )
           VALUES (op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, i_ord_num, i_ord_ln, i_qty, i_r_rlse.div_part
                  );
    ELSE
      INSERT INTO mclp240b
                  (cntrb, div_part, wtypb, tofrmb, itemb, uomb, txjrb, qtyb,
                   pksltb, statb, last_chg_ts
                  )
           VALUES (mclp240b_cntrb_seq.NEXTVAL, i_r_rlse.div_part, 'STP', 'F', i_item, i_uom, NULL, i_qty,
                   i_pick_aisl || i_pick_bin || i_pick_lvl, 'P', i_r_rlse.rlse_ts
                  );
    END IF;   -- i_stamp_sw = 'N'

    l_catlg_num := op_item_pk.catlg_num_fn(i_item, i_uom);

    INSERT INTO tran_item_op2i
                (tran_id, part_id, catlg_num, inv_zone,
                 inv_aisle, inv_bin, inv_lvl,
                 pick_zone, pick_aisle, pick_bin, pick_lvl, qty, cig_sel_cd, hand_stamp_sw,
                 cust_tax_jrsdctn, div_part
                )
         VALUES (op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, l_catlg_num, NVL(i_inv_zone, '~'),
                 NVL(i_inv_aisl, i_pick_aisl), NVL(i_inv_bin, i_pick_bin), NVL(i_inv_lvl, i_pick_lvl),
                 NVL(i_pick_zone, '~'), i_pick_aisl, i_pick_bin, i_pick_lvl, i_qty, i_cig_sel_cd, i_hand_stamp_sw,
                 i_cust_tax_jrsdctn, i_r_rlse.div_part
                );

    IF i_stamp_sw = 'Y' THEN
      INSERT INTO tran_stamp_op2c
                  (tran_id, part_id, stamp_item, stamp_apld_cd, div_part
                  )
           VALUES (op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, l_catlg_num, 'Y', i_r_rlse.div_part
                  );
    END IF;   -- i_stamp_sw = 'Y'

    IF (    i_t_stamps IS NOT NULL
        AND i_t_stamps.COUNT > 0) THEN
      FOR i IN i_t_stamps.FIRST .. i_t_stamps.LAST LOOP
        INSERT INTO tran_stamp_op2c
                    (div_part, tran_id, part_id, stamp_item,
                     stamp_apld_cd
                    )
             VALUES (i_r_rlse.div_part, op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, i_t_stamps(i).stamp_item,
                     i_t_stamps(i).stamp_apld_cd
                    );
      END LOOP;
    END IF;   -- i_t_stamps IS NOT NULL AND i_t_stamps.COUNT > 0
  END ins_pick_trans_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_CUTDOWN_TRANS_SP
  ||  Log cutown transaction.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 03/01/10 | rhalpai | Replaced inv_rowid parm with parms for item/uom/slot
  ||                    | and changed logic to use them when logging
  ||                    | transactions. PIR0024
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_cutdown_trans_sp(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_trans_typ  IN  VARCHAR2,
    i_item       IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_aisl       IN  VARCHAR2,
    i_bin        IN  VARCHAR2,
    i_lvl        IN  VARCHAR2,
    i_qty        IN  NUMBER,
    i_pgm_id     IN  VARCHAR2
  ) IS
  BEGIN
--    env.tag();

    INSERT INTO tran_op2t
                (tran_id, part_id, rlse_id, tran_typ,
                 pgm_id, div_part
                )
         VALUES (op1a_tran_id_seq.NEXTVAL, i_r_rlse.tran_part_id, i_r_rlse.rlse_id, i_trans_typ,
                 SUBSTR(i_pgm_id, 1, 20), i_r_rlse.div_part
                );

    INSERT INTO tran_item_op2i
                (tran_id, part_id, catlg_num, inv_zone, inv_aisle, inv_bin, inv_lvl, pick_zone, pick_aisle, pick_bin,
                 pick_lvl, qty, div_part)
      SELECT op1a_tran_id_seq.CURRVAL, i_r_rlse.tran_part_id, e.catite, '~', i_aisl, i_bin, i_lvl, '~', i_aisl, i_bin,
             i_lvl, i_qty, i_r_rlse.div_part
        FROM sawp505e e
       WHERE e.iteme = i_item
         AND e.uome = i_uom;

    INSERT INTO mclp240b
                (cntrb, wtypb, tofrmb, itemb, uomb,
                 txjrb, qtyb, pksltb, statb, last_chg_ts, div_part
                )
         VALUES (mclp240b_cntrb_seq.NEXTVAL, 'CUT', DECODE(i_trans_typ, g_c_trans_cut_from, 'F', 'T'), i_item, i_uom,
                 NULL, i_qty, i_aisl || i_bin || i_lvl, 'P', i_r_rlse.rlse_ts, i_r_rlse.div_part
                );

--    env.untag();
  END ins_cutdown_trans_sp;

  /*
  ||----------------------------------------------------------------------------
  || WKLY_MAX_INFO_SP
  ||  Get info for Weekly Max Qty
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/11 | rhalpai | Original for PIR6235
  ||----------------------------------------------------------------------------
  */
  PROCEDURE wkly_max_info_sp(
    i_r_rlse     IN      g_cur_rlse%ROWTYPE,
    i_cust_id    IN      VARCHAR2,
    i_catlg_num  IN      NUMBER,
    i_eff_dt     IN      DATE,
    o_max_qty    OUT     NUMBER,
    o_pick_qty   OUT     NUMBER,
    o_dist_sw    OUT     VARCHAR2
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
--    env.tag();

    OPEN l_cv
     FOR
       SELECT q.max_qty, ci.pick_qty, q.dist_sw
         FROM wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q
        WHERE ci.div_part = i_r_rlse.div_part
          AND ci.cust_id = i_cust_id
          AND ci.catlg_num = i_catlg_num
          AND q.div_part = ci.div_part
          AND q.cust_item_sid = ci.cust_item_sid
          AND q.eff_dt = i_eff_dt;

    FETCH l_cv
     INTO o_max_qty, o_pick_qty, o_dist_sw;

--    env.untag();
  END wkly_max_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || APPLY_WKLY_MAX_SP
  ||  Apply Weekly Max Qty to Cust/Item in Current Release.
  ||  Revert any subs where new OrdQty is set to zero.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/11 | rhalpai | Original for PIR6235
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in calls to
  ||                    | OP_MAINTAIN_SUBS_PK.REVERT_SUB_SP, OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE apply_wkly_max_sp(
    i_r_rlse       IN  g_cur_rlse%ROWTYPE,
    i_cust_id      IN  VARCHAR2,
    i_catlg_num    IN  NUMBER,
    i_eff_dt       IN  DATE,
    i_ttl_ord_qty  IN  NUMBER,
    i_sub_cd       IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ALLOCATE_PK.APPLY_WKLY_MAX_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_max_qty            PLS_INTEGER;
    l_pick_qty           PLS_INTEGER;
    l_dist_sw            VARCHAR2(1);
    l_ttl_ord_qty        PLS_INTEGER;
    l_ord_num            NUMBER;
    l_ord_ln             NUMBER;
    l_item               sawp505e.iteme%TYPE;
    l_uom                sawp505e.uome%TYPE;
    l_ord_qty            PLS_INTEGER;
    l_sub_cd             NUMBER;
    l_qty_over_max       PLS_INTEGER;
    l_new_ord_qty        PLS_INTEGER;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'EffDt', i_eff_dt);
    logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
    logs.dbg('ENTRY', lar_parm);

    IF i_r_rlse.test_bil_cd = '~' THEN
      timer.startme(l_c_module || env.get_session_id);
--      env.tag();
      logs.dbg('Get Wkly Max Info');
      wkly_max_info_sp(i_r_rlse, i_cust_id, i_catlg_num, i_eff_dt, l_max_qty, l_pick_qty, l_dist_sw);

      IF l_max_qty > 0 THEN
        logs.dbg('Get Total OrdQty');
        l_ttl_ord_qty := i_ttl_ord_qty;

        IF l_ttl_ord_qty > 0 THEN
          -- include current pick qty in total order qty
          l_ttl_ord_qty := l_ttl_ord_qty + l_pick_qty;

          IF l_ttl_ord_qty > l_max_qty THEN
            l_qty_over_max := l_ttl_ord_qty - l_max_qty;
            logs.dbg('Open Order Cursor');

            OPEN l_cv
             FOR
               SELECT   b.ordnob, b.lineb, b.itemnb, b.sllumb, b.ordqtb, b.subrcb
                   FROM sawp505e e, load_depart_op1f ld, ordp100a a, ordp120b b
                  WHERE e.catite = i_catlg_num
                    AND ld.div_part = i_r_rlse.div_part
                    AND ld.llr_dt = i_r_rlse.llr_dt
                    AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                    AND a.div_part = ld.div_part
                    AND a.load_depart_sid = ld.load_depart_sid
                    AND a.custa = i_cust_id
                    AND a.dsorda = DECODE(l_dist_sw, 'N', 'R', a.dsorda)
                    AND a.ipdtsa NOT IN('DVC', 'DVT')
                    AND a.stata = 'P'
                    AND a.excptn_sw = 'N'
                    AND b.div_part = a.div_part
                    AND b.ordnob = a.ordnoa
                    AND b.itemnb = e.iteme
                    AND b.sllumb = e.uome
                    AND b.statb = 'P'
                    AND b.excptn_sw = 'N'
                    AND b.ordqtb > 0
                    AND b.subrcb < 999
                    AND b.subrcb = NVL(i_sub_cd, b.subrcb)
                    AND b.ntshpb IS NULL
               ORDER BY a.pshipa DESC, b.subrcb, b.ordqtb DESC;

            LOOP
              logs.dbg('Fetch Order Cursor');

              FETCH l_cv
               INTO l_ord_num, l_ord_ln, l_item, l_uom, l_ord_qty, l_sub_cd;

              EXIT WHEN(   l_qty_over_max <= 0
                        OR l_cv%NOTFOUND);
              l_new_ord_qty := GREATEST(0, l_ord_qty - l_qty_over_max);
              logs.dbg('Log Weekly Max Order Qty Violation for OrdLn');
              op_mclp300d_pk.ins_sp(i_r_rlse.div_part,
                                    l_ord_num,
                                    l_ord_ln,
                                    'WKMAXQTY',
                                    l_item,
                                    l_uom,
                                    l_ord_qty,
                                    l_new_ord_qty
                                   );

              IF (    l_new_ord_qty = 0
                  AND l_sub_cd > 0) THEN
                logs.dbg('Revert Sub');
                op_maintain_subs_pk.revert_sub_sp(i_r_rlse.div_part, l_ord_num, l_ord_ln);
              ELSE
                logs.dbg('Adjust Qty for Order Line');

                UPDATE ordp120b b
                   SET b.ordqtb = l_new_ord_qty
                 WHERE b.div_part = i_r_rlse.div_part
                   AND b.ordnob = l_ord_num
                   AND b.lineb = l_ord_ln;
              END IF;   -- l_new_ord_qty = 0 AND l_sub_cd > 0

              l_qty_over_max := l_qty_over_max -(l_ord_qty - l_new_ord_qty);
            END LOOP;
          END IF;   -- l_ttl_ord_qty > l_max_qty
        END IF;   -- l_ttl_ord_qty > 0
      END IF;   -- l_max_qty > 0

--      env.untag();
      timer.stopme(l_c_module || env.get_session_id);
      logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   -- i_r_rlse.test_bil_cd = '~'
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END apply_wkly_max_sp;

  /*
  ||----------------------------------------------------------------------------
  || APPLY_WKLY_MAXS_SP
  ||  Reduce OrdQty and log as necessary to meet any effective Weekly Max Qtys
  ||  for tagged OrdLns within current Release
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/11 | rhalpai | Original for PIR6235
  || 12/08/11 | rhalpai | Changed cursor to improve performance. IM-037317
  || 06/25/12 | rhalpai | Change Weekly Max Cursor to exclude order lines for
  ||                    | DVC Compliance Customers/Items. PIR11524
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 11/26/13 | rhalpai | Changed logic to improve effeciency. IM-127350
  ||----------------------------------------------------------------------------
  */
  PROCEDURE apply_wkly_maxs_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_cig_sw  IN  VARCHAR2 DEFAULT NULL,
    i_sub_cd  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_ALLOCATE_PK.APPLY_WKLY_MAXS_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;

    TYPE l_rt_wkly_max IS RECORD(
      cust_id    sysp200c.acnoc%TYPE,
      catlg_num  NUMBER,
      eff_dt     DATE,
      ord_qty    NUMBER
    );

    TYPE l_tt_wkly_maxs IS TABLE OF l_rt_wkly_max;

    l_t_wkly_maxs        l_tt_wkly_maxs;
  BEGIN
    IF i_r_rlse.test_bil_cd = '~' THEN
      logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
      logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
      logs.add_parm(lar_parm, 'CigSw', i_cig_sw);
      logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
      logs.dbg('ENTRY', lar_parm);
      timer.startme(l_c_module || env.get_session_id);
      env.tag();
      workaround_for_ora600_kkqcscpopnwithmap_sp('SESSION', 'ENABLE');
      logs.dbg('Open Weekly Max Cursor');

      OPEN l_cv
       FOR
        WITH x AS
             (SELECT   a.custa AS cust_id, TO_NUMBER(e.catite) AS catlg_num,
                       NVL(SUM(DECODE(a.dsorda, 'R', b.ordqtb)), 0) AS reg_ord_qty,
                       NVL(SUM(b.ordqtb), 0) AS ord_qty
                  FROM load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e
                 WHERE ld.div_part = i_r_rlse.div_part
                   AND ld.llr_dt = i_r_rlse.llr_dt
                   AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                   AND a.div_part = ld.div_part
                   AND a.load_depart_sid = ld.load_depart_sid
                   AND a.stata = 'P'
                   AND a.excptn_sw = 'N'
                   AND b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.statb = 'P'
                   AND b.excptn_sw = 'N'
                   AND b.ordqtb > 0
                   AND b.subrcb < 999
                   AND b.subrcb = NVL(i_sub_cd, b.subrcb)
                   AND b.ntshpb IS NULL
                   AND e.iteme = b.itemnb
                   AND e.uome = b.sllumb
                   AND (   i_cig_sw IS NULL
                        OR (    i_cig_sw = 'Y'
                            AND e.uome IN('CII', 'CIR', 'CIC'))
                        OR (    i_cig_sw = 'N'
                            AND e.uome NOT IN('CII', 'CIR', 'CIC'))
                       )
              GROUP BY a.custa, e.catite)
         SELECT z.cust_id, z.catlg_num, z.eff_dt, DECODE(z.dist_sw, 'N', x.reg_ord_qty, x.ord_qty) AS ord_qty
           FROM x,
                (SELECT DISTINCT ci.cust_id, ci.catlg_num,
                                 FIRST_VALUE(q.eff_dt) OVER(PARTITION BY q.cust_item_sid ORDER BY q.eff_dt DESC) AS eff_dt,
                                 FIRST_VALUE(q.dist_sw) OVER(PARTITION BY q.cust_item_sid ORDER BY q.eff_dt DESC) AS dist_sw
                            FROM wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q, x
                           WHERE ci.div_part = i_r_rlse.div_part
                             AND q.div_part = ci.div_part
                             AND q.cust_item_sid = ci.cust_item_sid
                             AND i_r_rlse.llr_dt BETWEEN q.eff_dt AND q.end_dt
                             AND x.cust_id = ci.cust_id
                             AND x.catlg_num = ci.catlg_num
                             AND (ci.cust_id, ci.catlg_num) NOT IN(
                                   SELECT vcc.cust_id, vci.catlg_num
                                     FROM vndr_cmp_prof_op3l vcp, vndr_cmp_item_op1l vci, vndr_cmp_cust_op2l vcc
                                    WHERE vcp.typ = 'DVC'
                                      AND vci.prof_id = vcp.prof_id
                                      AND vcc.div_part = i_r_rlse.div_part
                                      AND vcc.prof_id = vcp.prof_id)) z
          WHERE x.cust_id = z.cust_id
            AND x.catlg_num = z.catlg_num;

      LOOP
        logs.dbg('Fetch Weekly Max Cursor');

        FETCH l_cv
        BULK COLLECT INTO l_t_wkly_maxs LIMIT 100;

        EXIT WHEN l_t_wkly_maxs.COUNT = 0;
        FOR i IN l_t_wkly_maxs.FIRST .. l_t_wkly_maxs.LAST LOOP
          logs.dbg('Apply Weekly Max');
          apply_wkly_max_sp(i_r_rlse,
                            l_t_wkly_maxs(i).cust_id,
                            l_t_wkly_maxs(i).catlg_num,
                            l_t_wkly_maxs(i).eff_dt,
                            l_t_wkly_maxs(i).ord_qty,
                            i_sub_cd
                           );
        END LOOP;
      END LOOP;
      workaround_for_ora600_kkqcscpopnwithmap_sp('SESSION', 'DISABLE');
      env.untag();
      timer.stopme(l_c_module || env.get_session_id);
      logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   -- i_r_rlse.test_bil_cd = '~'
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END apply_wkly_maxs_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_AND_LOG_WKLY_MAXS_SP
  ||  Adjust PickQty for Weekly Max Cust Item and add Weekly Max Log entries
  ||  for allocated OrdLns within Release
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/11 | rhalpai | Original for PIR6235
  || 12/08/11 | rhalpai | Changed update logic to improve performance. IM-037317
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_and_log_wkly_maxs_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_cig_sw  IN  VARCHAR2 DEFAULT NULL,
    i_sub_cd  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.UPD_AND_LOG_WKLY_MAXS_SP';
    lar_parm             logs.tar_parm;

    PROCEDURE upd_sp(
      i_r_rlse  IN  g_cur_rlse%ROWTYPE
    ) IS
      l_t_cust_item_sids  type_ntab;
      l_t_pick_qtys       type_ntab;
    BEGIN
      SELECT   ci.cust_item_sid, SUM(b.alcqtb) AS pick_qty
      BULK COLLECT INTO l_t_cust_item_sids, l_t_pick_qtys
          FROM wkly_max_cust_item_op1m ci, sawp505e e, wkly_max_qty_op2m q, load_depart_op1f ld, ordp100a a, ordp120b b
         WHERE e.catite = ci.catlg_num
           AND q.div_part = i_r_rlse.div_part
           AND q.cust_item_sid = ci.cust_item_sid
           AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                             FROM wkly_max_qty_op2m q2
                            WHERE q2.div_part = i_r_rlse.div_part
                              AND q2.cust_item_sid = ci.cust_item_sid
                              AND i_r_rlse.llr_dt BETWEEN q2.eff_dt AND q2.end_dt)
           AND ci.div_part = q.div_part
           AND i_r_rlse.llr_dt BETWEEN q.eff_dt AND q.end_dt
           AND ld.div_part = i_r_rlse.div_part
           AND ld.llr_dt = i_r_rlse.llr_dt
           AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.custa = ci.cust_id
           AND a.dsorda = DECODE(q.dist_sw, 'N', 'R', a.dsorda)
           AND a.stata = 'P'
           AND a.excptn_sw = 'N'
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.itemnb = e.iteme
           AND b.sllumb = e.uome
           AND b.statb = 'T'
           AND b.excptn_sw = 'N'
           AND b.alcqtb > 0
           AND b.subrcb < 999
           AND b.subrcb = NVL(i_sub_cd, b.subrcb)
           AND b.ntshpb IS NULL
           AND (   i_cig_sw IS NULL
                OR (    i_cig_sw = 'Y'
                    AND e.uome IN('CII', 'CIR', 'CIC'))
                OR (    i_cig_sw = 'N'
                    AND e.uome NOT IN('CII', 'CIR', 'CIC'))
               )
           AND NOT EXISTS(SELECT 1
                            FROM wkly_max_log_op3m l
                           WHERE l.div_part = i_r_rlse.div_part
                             AND l.rlse_ts = i_r_rlse.rlse_ts
                             AND l.qty_typ = 'PCK'
                             AND l.cust_item_sid = ci.cust_item_sid
                             AND l.ord_num = b.ordnob
                             AND l.ord_ln = b.lineb)
      GROUP BY ci.cust_item_sid;

      FORALL i IN l_t_cust_item_sids.FIRST .. l_t_cust_item_sids.LAST
        UPDATE wkly_max_cust_item_op1m ci
           SET ci.pick_qty = ci.pick_qty + l_t_pick_qtys(i)
         WHERE ci.div_part = i_r_rlse.div_part
           AND ci.cust_item_sid = l_t_cust_item_sids(i);
    END upd_sp;
  BEGIN
    IF i_r_rlse.test_bil_cd = '~' THEN
      logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
      logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
      logs.add_parm(lar_parm, 'CigSw', i_cig_sw);
      logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
      logs.dbg('ENTRY', lar_parm);
      timer.startme(l_c_module || env.get_session_id);
      env.tag();
      logs.dbg('Adjust Pick Qty for Weekly Max Qty');
      upd_sp(i_r_rlse);
      logs.dbg('Add Weekly Max Log Pick Entries');

      INSERT INTO wkly_max_log_op3m
                  (rlse_ts, qty_typ, ord_num, ord_ln, cust_item_sid, qty, div_part)
        SELECT i_r_rlse.rlse_ts, 'PCK', b.ordnob, b.lineb, ci.cust_item_sid, b.alcqtb, i_r_rlse.div_part
          FROM wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q, sawp505e e, load_depart_op1f ld, ordp100a a,
               ordp120b b
         WHERE ci.div_part = i_r_rlse.div_part
           AND q.div_part = i_r_rlse.div_part
           AND q.cust_item_sid = ci.cust_item_sid
           AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                             FROM wkly_max_qty_op2m q2
                            WHERE q2.div_part = i_r_rlse.div_part
                              AND q2.cust_item_sid = ci.cust_item_sid
                              AND i_r_rlse.llr_dt BETWEEN q2.eff_dt AND q2.end_dt)
           AND e.catite = ci.catlg_num
           AND i_r_rlse.llr_dt BETWEEN q.eff_dt AND q.end_dt
           AND ld.div_part = i_r_rlse.div_part
           AND ld.llr_dt = i_r_rlse.llr_dt
           AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.custa = ci.cust_id
           AND a.dsorda = DECODE(q.dist_sw, 'N', 'R', a.dsorda)
           AND a.stata = 'P'
           AND a.excptn_sw = 'N'
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.itemnb = e.iteme
           AND b.sllumb = e.uome
           AND b.statb = 'T'
           AND b.excptn_sw = 'N'
           AND b.alcqtb > 0
           AND b.subrcb < 999
           AND b.subrcb = NVL(i_sub_cd, b.subrcb)
           AND b.ntshpb IS NULL
           AND (   i_cig_sw IS NULL
                OR (    i_cig_sw = 'Y'
                    AND e.uome IN('CII', 'CIR', 'CIC'))
                OR (    i_cig_sw = 'N'
                    AND e.uome NOT IN('CII', 'CIR', 'CIC'))
               )
           AND NOT EXISTS(SELECT 1
                            FROM wkly_max_log_op3m l
                           WHERE l.div_part = i_r_rlse.div_part
                             AND l.rlse_ts = i_r_rlse.rlse_ts
                             AND l.qty_typ = 'PCK'
                             AND l.cust_item_sid = ci.cust_item_sid
                             AND l.ord_num = b.ordnob
                             AND l.ord_ln = b.lineb);

      IF (    i_cig_sw IS NULL
          AND i_sub_cd IS NULL) THEN
        logs.dbg('Add Weekly Max Log Cut Entries');

        INSERT INTO wkly_max_log_op3m
                    (rlse_ts, qty_typ, ord_num, ord_ln, cust_item_sid, qty, div_part)
          SELECT i_r_rlse.rlse_ts, 'CUT', b.ordnob, b.lineb, ci.cust_item_sid,
                 (SELECT SUM(lg.qtyfrd - lg.qtytod)
                    FROM mclp300d lg
                   WHERE lg.div_part = i_r_rlse.div_part
                     AND lg.reasnd = 'WKMAXQTY'
                     AND lg.ordnod = b.ordnob
                     AND lg.ordlnd = b.lineb
                     AND NOT EXISTS(SELECT 1
                                      FROM mclp300d lg2
                                     WHERE lg2.div_part = lg.div_part
                                       AND lg2.ordnod = lg.ordnod
                                       AND lg2.ordlnd = lg.ordlnd
                                       AND lg2.last_chg_ts > lg.last_chg_ts
                                       AND lg2.reasnd = 'WKMAXDEL')) AS qty,
                 i_r_rlse.div_part
            FROM wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q, sawp505e e, load_depart_op1f ld, ordp100a a,
                 ordp120b b
           WHERE ci.div_part = i_r_rlse.div_part
             AND q.div_part = i_r_rlse.div_part
             AND q.cust_item_sid = ci.cust_item_sid
             AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                               FROM wkly_max_qty_op2m q2
                              WHERE q2.div_part = i_r_rlse.div_part
                                AND q2.cust_item_sid = ci.cust_item_sid
                                AND i_r_rlse.llr_dt BETWEEN q2.eff_dt AND q2.end_dt)
             AND e.catite = ci.catlg_num
             AND i_r_rlse.llr_dt BETWEEN q.eff_dt AND q.end_dt
             AND ld.div_part = i_r_rlse.div_part
             AND ld.llr_dt = i_r_rlse.llr_dt
             AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
             AND a.div_part = ld.div_part
             AND a.load_depart_sid = ld.load_depart_sid
             AND a.custa = ci.cust_id
             AND a.dsorda = DECODE(q.dist_sw, 'N', 'R', a.dsorda)
             AND a.stata = 'P'
             AND a.excptn_sw = 'N'
             AND b.div_part = a.div_part
             AND b.ordnob = a.ordnoa
             AND b.itemnb = e.iteme
             AND b.sllumb = e.uome
             AND b.statb IN('P', 'T', 'R')
             AND b.excptn_sw = 'N'
             AND b.ordqtb < b.orgqtb
             AND b.subrcb < 999
             AND EXISTS(SELECT 1
                          FROM mclp300d lg
                         WHERE lg.div_part = b.div_part
                           AND lg.reasnd = 'WKMAXQTY'
                           AND lg.ordnod = b.ordnob
                           AND lg.ordlnd = b.lineb
                           AND NOT EXISTS(SELECT 1
                                            FROM mclp300d lg2
                                           WHERE lg2.div_part = lg.div_part
                                             AND lg2.ordnod = lg.ordnod
                                             AND lg2.ordlnd = lg.ordlnd
                                             AND lg2.last_chg_ts > lg.last_chg_ts
                                             AND lg2.reasnd = 'WKMAXDEL'));
      END IF;   -- i_cig_sw IS NULL AND i_sub_cd IS NULL

      env.untag();
      timer.stopme(l_c_module || env.get_session_id);
      logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   --  i_r_rlse.test_bil_cd = '~'
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_and_log_wkly_maxs_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_ORDS_SP
  ||  This procedure is called when it determined that an orderline is to be
  ||  allocated.  It updates the allocated quantity information on the order
  ||  table and also updates the Inventory Table.  It inserts Stamp Work Order
  ||  entries for post-stamped orders.  It creates an entry in the Inventory
  ||  Transaction table for the Order Line to "Log" the Inventory reduction.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/26/03 | rhalpai | Moved to table function
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 10/14/05 | rhalpai | Moved from table function - PIR2909
  || 03/01/10 | rhalpai | Replaced inv_rec parm with parms for item/uom/slot,
  ||                    | removed stamp_qty parm and removed logic for logging
  ||                    | stamp transactions. Changed logic to use sub code to
  ||                    | identify subs rather than decimal order line number
  ||                    | since allocations from multiple cig slot locations
  ||                    | will also be represented with decimal order lines.
  ||                    | PIR0024
  || 08/29/11 | rhalpai | Change call to INS_PICK_TRANS_SP to replace TranLn
  ||                    | parm with StampSw.. PIR7990
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_ords_sp(
    i_r_rlse     IN  g_cur_rlse%ROWTYPE,
    i_item       IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_aisl       IN  VARCHAR2,
    i_bin        IN  VARCHAR2,
    i_lvl        IN  VARCHAR2,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_alloc_qty  IN  NUMBER,
    i_ord_stat   IN  VARCHAR2,
    i_sub_cd     IN  NUMBER,
    i_pgm_id     IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_ORDS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'Slot', i_aisl || i_bin || i_lvl);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'AllocQty', i_alloc_qty);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
    logs.add_parm(lar_parm, 'PgmId', i_pgm_id);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    logs.dbg('Allocate Order Line');
    alloc_ord_ln_sp(i_r_rlse, i_ord_num, i_ord_ln, i_alloc_qty, i_ord_stat);
    logs.dbg('Add Work Order entry to Tran Tbl for Pick Slot');
    ins_pick_trans_sp(i_r_rlse, 'N', i_item, i_uom, i_aisl, i_bin, i_lvl, i_ord_num, i_ord_ln, i_alloc_qty, i_pgm_id);

    IF i_sub_cd BETWEEN 1 AND 997 THEN
      logs.dbg('Allocate Orig OrdLn if Sub is allocated');
      alloc_orig_ord_ln_sp(i_r_rlse, i_ord_num, i_ord_ln, i_alloc_qty, i_sub_cd);
    END IF;   -- i_sub_cd BETWEEN 1 AND 997

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END alloc_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_INV_QTY_SP
  ||  This procedure is called when an it is determined that there is sufficient
  ||  inventory for an item and that the item will be allocated for shipping.
  ||  This procedure reduces the Inventory amount from the Inventory table.  It
  ||  also handles the reduction of stamps that are applied to the product after
  ||  it is selected.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_inv_qty_sp(
    i_inv_rowid  IN  ROWID,
    i_qty        IN  PLS_INTEGER
  ) IS
  BEGIN
    UPDATE whsp300c w
       SET w.qalc = NVL(w.qalc, 0) + i_qty,
           w.qavc = NVL(w.qavc, 0) - i_qty,
           w.qstmoc = NVL(w.qstmoc, 0) + i_qty
     WHERE w.ROWID = i_inv_rowid;
  END upd_inv_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SPLIT_PICK_SP
  ||  Create Split Pick order lines to handle allocation of a single order line
  ||  from multiple pick locations.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/10 | rhalpai | Original for PIR0024
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_split_pick_sp(
    i_r_rlse      IN  g_cur_rlse%ROWTYPE,
    i_ord_num     IN  NUMBER,
    i_new_ord_ln  IN  NUMBER,
    i_alloc_qty   IN  NUMBER
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm      := 'OP_ALLOCATE_PK.INS_SPLIT_PICK_SP';
    lar_parm                  logs.tar_parm;
    l_c_orig_ord_ln  CONSTANT NUMBER             := i_new_ord_ln - MOD(i_new_ord_ln, .1);
    l_r_split                 ordp120b%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'NewOrdLn', i_new_ord_ln);
    logs.add_parm(lar_parm, 'AllocQty', i_alloc_qty);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    logs.dbg('Create SplitPick Line from Orig Line');

    SELECT *
      INTO l_r_split
      FROM ordp120b
     WHERE div_part = i_r_rlse.div_part
       AND ordnob = i_ord_num
       AND lineb = l_c_orig_ord_ln;

    l_r_split.lineb := i_new_ord_ln;
    l_r_split.ordqtb := i_alloc_qty;
    l_r_split.orgqtb := i_alloc_qty;
    logs.dbg('Add SplitPick Line');

    INSERT INTO ordp120b
         VALUES l_r_split;

    logs.dbg('Upd Orig Line');

    UPDATE ordp120b
       SET orgqtb = orgqtb - i_alloc_qty,
           ordqtb = ordqtb - i_alloc_qty
     WHERE div_part = i_r_rlse.div_part
       AND ordnob = i_ord_num
       AND lineb = l_c_orig_ord_ln;

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_split_pick_sp;

  /*
  ||----------------------------------------------------------------------------
  || CREATE_PARTIAL_SP
  ||  This procedure creates a partial sub for an order line. It should be
  ||  called for partially allocated order lines prior to checking for
  ||  conditional subs for the remaining unallocated portion. The order line on
  ||  the transaction table (WHSP200R) is updated to the partial sub line. This
  ||  is also done for the Protected Inventory Log table (PRTCTD_ALLOC_LOG_OP1A).
  ||  The original order line is partially updated. It still needs the order
  ||  status, sub code and not-ship-reason to be updated. These columns are to
  ||  be updated separately in case conditional subbing is done after this
  ||  process. A call to Upd_Orig_For_Partial_SP should be done following the
  ||  call to this procedure (but after any conditional subbing) to finish
  ||  updating the original order line.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - Moved logic from Create_Sub_Lines_SP to
  ||                    | share logic with new kit allocation logic - PIR2909
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  || 08/26/10 | rhalpai | Removed dead column from update of order detail.
  ||                    | PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction order table. PIR7990
  || 07/10/12 | rhalpai | Remove unused column, TICKTB. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE create_partial_sp(
    i_r_rlse   IN  g_cur_rlse%ROWTYPE,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_ALLOCATE_PK.CREATE_PARTIAL_SP';
    lar_parm             logs.tar_parm;
    l_r_partial          ordp120b%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();

    SELECT *
      INTO l_r_partial
      FROM ordp120b
     WHERE div_part = i_r_rlse.div_part
       AND ordnob = i_ord_num
       AND lineb = i_ord_ln;

    l_r_partial.lineb := l_r_partial.lineb + .10;
    l_r_partial.statb := 'T';
    l_r_partial.subrcb := 0;
    l_r_partial.ordqtb := l_r_partial.pckqtb;
    l_r_partial.orgqtb := l_r_partial.pckqtb;

    INSERT INTO ordp120b
         VALUES l_r_partial;

    logs.dbg('Upd Transaction entry for Partial');

    UPDATE tran_ord_op2o op2o
       SET op2o.ord_ln = l_r_partial.lineb
     WHERE op2o.div_part = i_r_rlse.div_part
       AND op2o.ord_num = i_ord_num
       AND op2o.ord_ln = i_ord_ln
       AND op2o.part_id = i_r_rlse.tran_part_id
       AND EXISTS(SELECT 1
                    FROM tran_op2t op2t
                   WHERE op2t.div_part = i_r_rlse.div_part
                     AND op2t.rlse_id = i_r_rlse.rlse_id
                     AND op2t.part_id = i_r_rlse.tran_part_id
                     AND op2t.tran_id = op2o.tran_id);

    IF l_r_partial.sllumb IN('CII', 'CIR', 'CIC') THEN
      logs.dbg('Sync OrdLn in CMS');
      cig_op_allocate_maint_pk.upd_ord_ln(i_r_rlse.div_id, i_r_rlse.rlse_ts, i_ord_num, i_ord_ln, l_r_partial.lineb);
    END IF;   -- l_r_partial.sllumb IN('CII', 'CIR', 'CIC')

    -- Keep PRTCTD_ALLOC_LOG_OP1A in Sync with Transaction Order table
    logs.dbg('Upd Prot Inv Log for Partials');
    op_protected_inventory_pk.upd_log_for_partls_sp(i_r_rlse.div_part,
                                                    op_protected_inventory_pk.g_c_rlse,
                                                    l_r_partial.ordnob,
                                                    l_r_partial.lineb
                                                   );
    logs.dbg('Upd Orig Entry for Out on Partial Before Subbing');

    UPDATE ordp120b
       SET orgqtb = orgqtb - pckqtb,
           ordqtb = ordqtb - pckqtb,
           alcqtb = 0,
           pckqtb = 0
     WHERE div_part = i_r_rlse.div_part
       AND ordnob = i_ord_num
       AND lineb = i_ord_ln;

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END create_partial_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORIG_FOR_PARTIAL_SP
  ||  This procedure is called to finish updating the original order line for a
  ||  partial sub created by a call to CREATE_PARTIAL_SP.
  ||  The order status, sub code and not-ship-reason are updated separately
  ||  here to allow conditional subbing prior to this process.
  ||
  ||  The not-ship-reason will be overriden if a conditional sub is found and
  ||  allocated. This happens during the allocation of the sub line
  ||  (ALLOCATION_SP.ALLOC_ORDS_SP).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - Moved logic from Create_Sub_Lines_SP to
  ||                    | share logic with new kit allocation logic - PIR2909
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_orig_for_partial_sp(
    i_r_rlse          IN  g_cur_rlse%ROWTYPE,
    i_ord_num         IN  NUMBER,
    i_ord_ln          IN  NUMBER,
    i_not_shp_rsn_cd  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.UPD_ORIG_FOR_PARTIAL_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'NotShpRsnCd', i_not_shp_rsn_cd);
    logs.dbg('Update Original Entry for Out on Partial After Subbing');

    UPDATE ordp120b
       SET subrcb = 999,
           statb = 'T',
           ntshpb = i_not_shp_rsn_cd
     WHERE div_part = i_r_rlse.div_part
       AND ordnob = i_ord_num
       AND lineb = i_ord_ln;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_orig_for_partial_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_KITS_SP
  ||  When a kit item is ordered the component items are exploded into
  ||  individual orders before being loaded into OP. The order quantities are
  ||  the result of multiplying the quantity of the original kit item by its
  ||  componenty quantity.
  ||  Kit items are identified by grouping the following component orders:
  ||    Div,LLRdate,Customer,Load,Stop,EtaDate,PO
  ||  Rules for kit type AGG (Aggregate Items)
  ||  * If any kit component item is missing then all orders for the kit must
  ||    be outed.
  ||  * If kit is a distribution and any component cannot be fully allocated
  ||    then all must be outed.
  ||  * If kit is a regular order and components cannot be fully allocated
  ||    then all orders for components will be reduced to the minimum ratio
  ||    (OrderQty / ComponentQty) that can be allocated. They will be allocated
  ||    as partial subs and the original lines will have their OrderQty reduced
  ||    and then outed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - PIR2909
  || 03/27/06 | rhalpai | Changed MAX_COMP_ITEMS_ORD_RATIOS_FN and GET_RATIOS_SP
  ||                    | to handle null PO's IM225137
  || 04/07/06 | rhalpai | Moved OUT_ORDS_SP to allow calls from ALLOCATE_KITS_SP
  ||                    | and new ALLOC_BUNDLE_DIST_SP. PIR2545
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Changed cursors to use load list from
  ||                    | MCLANE_LOAD_LABEL_RLSE.
  || 03/01/10 | rhalpai | Removed LLR parm. Replaced calls to ALLOC_INV_SP with
  ||                    | UPD_INV_QTY_SP. Added logic to process all kit types
  ||                    | within loop (currently only AGG kit type exists).
  ||                    | PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_kits_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_KITS_SP';
    lar_parm                   logs.tar_parm;
    l_t_kit_typs               type_stab     := type_stab();
    l_c_ord_stat_pnd  CONSTANT VARCHAR2(1)   := 'P';

    TYPE l_rt_kit_comp IS RECORD(
      comp_item_num  VARCHAR2(6),
      comp_qty       NUMBER(9),
      seq            NUMBER
    );

    TYPE l_tt_kit_comps IS TABLE OF l_rt_kit_comp;

    TYPE l_tt_kit_comp_qtys_i IS TABLE OF NUMBER
      INDEX BY VARCHAR2(6);

    TYPE l_tt_kit_seqs_i IS TABLE OF l_tt_kit_comp_qtys_i
      INDEX BY PLS_INTEGER;

    FUNCTION kit_tab_fn(
      i_r_rlse    IN  g_cur_rlse%ROWTYPE,
      i_kit_typ   IN  VARCHAR2,
      i_ord_stat  IN  VARCHAR2
    )
      RETURN kits_t IS
      l_t_kits  kits_t;
      l_cv      SYS_REFCURSOR;
    BEGIN
      OPEN l_cv
       FOR
         SELECT kit_t(i_r_rlse.div_id,
                      i_r_rlse.llr_dt - g_c_rensoft_seed_dt,
                      i_kit_typ,
                      x.ord_typ,
                      x.kit_item,
                      x.cust_id,
                      x.load_num,
                      x.stop_num,
                      x.eta_dt - g_c_rensoft_seed_dt,
                      x.po_num
                     )
           FROM (SELECT   a.dsorda AS ord_typ, k.item_num kit_item, se.cust_id, ld.load_num, se.stop_num,
                          TRUNC(se.eta_ts) AS eta_dt, a.cpoa AS po_num
                     FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, kit_item_mstr_kt1m k
                    WHERE ld.div_part = i_r_rlse.div_part
                      AND ld.llr_dt = i_r_rlse.llr_dt
                      AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                      AND a.div_part = ld.div_part
                      AND a.load_depart_sid = ld.load_depart_sid
                      AND a.excptn_sw = 'N'
                      AND se.div_part = a.div_part
                      AND se.load_depart_sid = a.load_depart_sid
                      AND se.cust_id = a.custa
                      AND b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.statb = i_ord_stat
                      AND b.excptn_sw = 'N'
                      AND b.ordqtb > 0
                      AND b.subrcb = 0
                      AND k.div_part = b.div_part
                      AND k.kit_typ = i_kit_typ
                      AND k.comp_item_num = b.orditb
                 GROUP BY a.dsorda, k.item_num, se.cust_id, ld.load_num, se.stop_num, TRUNC(se.eta_ts), a.cpoa) x;

      FETCH l_cv
      BULK COLLECT INTO l_t_kits;

      CLOSE l_cv;

      RETURN(l_t_kits);
    END kit_tab_fn;

    PROCEDURE out_kit_ords_sp(
      i_r_rlse      IN  g_cur_rlse%ROWTYPE,
      i_t_kit_ords  IN  kit_ords_t,
      i_seq         IN  PLS_INTEGER
    ) IS
      l_idx         PLS_INTEGER;
      l_t_ord_nums  type_ntab;
      l_t_ord_lns   type_ntab;
    BEGIN
      IF i_t_kit_ords IS NOT NULL THEN
        l_idx := i_t_kit_ords.FIRST;
        WHILE l_idx IS NOT NULL LOOP
          IF i_t_kit_ords(l_idx).seq = i_seq THEN
            add_to_tab_sp(l_t_ord_nums, i_t_kit_ords(l_idx).order_num);
            add_to_tab_sp(l_t_ord_lns, i_t_kit_ords(l_idx).order_ln);
          END IF;   -- i_t_kit_ords(v_idx).seq = p_seq

          l_idx := i_t_kit_ords.NEXT(l_idx);
        END LOOP;
        out_ords_sp(i_r_rlse, l_t_ord_nums, l_t_ord_lns);
      END IF;   -- i_t_kit_ords IS NOT NULL
    END out_kit_ords_sp;

    FUNCTION max_comp_items_ord_ratios_fn(
      i_ord_stat  IN  VARCHAR2,
      i_o_kit     IN  kit_t
    )
      RETURN NUMBER IS
      l_llr_dt  DATE;
      l_eta_dt  DATE;
      l_cnt     PLS_INTEGER;
    BEGIN
      l_llr_dt := DATE '1900-02-28' + i_o_kit.llr_dt;
      l_eta_dt := DATE '1900-02-28' + i_o_kit.eta_date;

      -- max count of component item orders or order ratios for kit item
      SELECT MAX(x.cnt)
        INTO l_cnt
        FROM (SELECT   COUNT(*) AS cnt
                  FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b,
                       kit_item_mstr_kt1m k
                 WHERE d.div_id = i_o_kit.div_id
                   AND ld.div_part = d.div_part
                   AND ld.llr_dt = l_llr_dt
                   AND ld.load_num = i_o_kit.load_num
                   AND se.div_part = d.div_part
                   AND se.load_depart_sid = ld.load_depart_sid
                   AND se.cust_id = i_o_kit.cust_num
                   AND se.stop_num = i_o_kit.stop_num
                   AND TRUNC(se.eta_ts) = l_eta_dt
                   AND a.div_part = d.div_part
                   AND a.load_depart_sid = se.load_depart_sid
                   AND a.custa = se.cust_id
                   AND a.dsorda = i_o_kit.ord_typ
                   AND NVL(a.cpoa, ' ') = NVL(i_o_kit.po_num, ' ')
                   AND a.excptn_sw = 'N'
                   AND b.div_part = d.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.statb = i_ord_stat
                   AND b.excptn_sw = 'N'
                   AND b.ordqtb > 0
                   AND b.subrcb = 0
                   AND k.div_part = d.div_part
                   AND k.kit_typ = i_o_kit.kit_typ
                   AND k.item_num = i_o_kit.kit_item_num
                   AND k.comp_item_num = b.orditb
              GROUP BY b.orditb
              UNION ALL
              SELECT COUNT(DISTINCT(b.ordqtb / k.comp_qty)) AS cnt
                FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b,
                     kit_item_mstr_kt1m k
               WHERE d.div_id = i_o_kit.div_id
                 AND ld.div_part = d.div_part
                 AND ld.llr_dt = l_llr_dt
                 AND ld.load_num = i_o_kit.load_num
                 AND se.div_part = d.div_part
                 AND se.load_depart_sid = ld.load_depart_sid
                 AND se.cust_id = i_o_kit.cust_num
                 AND se.stop_num = i_o_kit.stop_num
                 AND TRUNC(se.eta_ts) = l_eta_dt
                 AND a.div_part = d.div_part
                 AND a.load_depart_sid = se.load_depart_sid
                 AND a.custa = se.cust_id
                 AND a.dsorda = i_o_kit.ord_typ
                 AND NVL(a.cpoa, ' ') = NVL(i_o_kit.po_num, ' ')
                 AND a.excptn_sw = 'N'
                 AND b.div_part = d.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.statb = i_ord_stat
                 AND b.excptn_sw = 'N'
                 AND b.ordqtb > 0
                 AND b.subrcb = 0
                 AND k.div_part = d.div_part
                 AND k.kit_typ = i_o_kit.kit_typ
                 AND k.item_num = i_o_kit.kit_item_num
                 AND k.comp_item_num = b.orditb) x;

      RETURN(l_cnt);
    END max_comp_items_ord_ratios_fn;

    FUNCTION kit_comp_tab_fn(
      i_div_part  IN  NUMBER,
      i_ord_stat  IN  VARCHAR2,
      i_o_kit     IN  kit_t
    )
      RETURN l_tt_kit_comps IS
      TYPE l_cvt_kit_comps IS REF CURSOR
        RETURN l_rt_kit_comp;

      l_cv_kit_comps  l_cvt_kit_comps;
      l_t_kit_comps   l_tt_kit_comps  := l_tt_kit_comps(NULL);
      l_cnt           PLS_INTEGER;
    BEGIN
      l_cnt := max_comp_items_ord_ratios_fn(i_ord_stat, i_o_kit);

      IF l_cnt > 0 THEN
        OPEN l_cv_kit_comps
         FOR
           SELECT k.comp_item_num, k.comp_qty, t.column_value seq
             FROM kit_item_mstr_kt1m k, TABLE(pivot_fn(l_cnt)) t
            WHERE k.div_part = i_div_part
              AND k.kit_typ = i_o_kit.kit_typ
              AND k.item_num = i_o_kit.kit_item_num;

        FETCH l_cv_kit_comps
        BULK COLLECT INTO l_t_kit_comps;

        CLOSE l_cv_kit_comps;
      END IF;   -- l_cnt > 0

      RETURN(l_t_kit_comps);
    END kit_comp_tab_fn;

    FUNCTION seq_tab_fn(
      i_div_part  IN  NUMBER,
      i_ord_stat  IN  VARCHAR2,
      i_o_kit     IN  kit_t
    )
      RETURN l_tt_kit_seqs_i IS
      l_t_kit_comps        l_tt_kit_comps;
      l_t_kit_comp_qtys_i  l_tt_kit_comp_qtys_i;
      l_t_kit_seqs_i       l_tt_kit_seqs_i;
      l_seq                PLS_INTEGER;
      l_seq_save           PLS_INTEGER;
    BEGIN
      -- values stored in t_kit_comp will be grouped by seq, comp_item_num
      l_t_kit_comps := kit_comp_tab_fn(i_div_part, i_ord_stat, i_o_kit);
      -- default to first seq
      l_seq_save := l_t_kit_comps(l_t_kit_comps.FIRST).seq;

      IF l_seq_save IS NOT NULL THEN
        FOR i IN l_t_kit_comps.FIRST .. l_t_kit_comps.LAST LOOP
          l_seq := l_t_kit_comps(i).seq;
          l_t_kit_comp_qtys_i(l_t_kit_comps(i).comp_item_num) := l_t_kit_comps(i).comp_qty;

          -- assign when seq changes
          IF l_seq <> l_seq_save THEN
            l_seq_save := l_seq;
            l_t_kit_seqs_i(l_seq) := l_t_kit_comp_qtys_i;
          END IF;   -- v_seq <> v_seq_save
        END LOOP;
        -- final assignment
        l_t_kit_seqs_i(l_seq) := l_t_kit_comp_qtys_i;
      END IF;   -- l_seq_save IS NOT NULL

      RETURN(l_t_kit_seqs_i);
    END seq_tab_fn;

    PROCEDURE prcs_miss_comp_sp(
      i_r_rlse    IN  g_cur_rlse%ROWTYPE,
      i_ord_stat  IN  VARCHAR2,
      l_o_kit     IN  kit_t
    ) IS
      TYPE l_tt_ord_seqs_i IS TABLE OF NUMBER
        INDEX BY VARCHAR2(9);

      l_t_kit_ords         kit_ords_t;
      l_t_ord_seqs_i       l_tt_ord_seqs_i;
      l_ord_idx            VARCHAR2(9);
      l_t_kit_seqs_i       l_tt_kit_seqs_i;
      l_seq_idx            PLS_INTEGER;
      l_is_match           BOOLEAN;
      l_t_kit_comp_qtys_i  l_tt_kit_comp_qtys_i;
      l_comp_idx           kit_item_mstr_kt1m.comp_item_num%TYPE;

      FUNCTION ord_idx_fn(
        i_seq            IN  PLS_INTEGER,
        i_comp_item_num  IN  VARCHAR2
      )
        RETURN VARCHAR2 IS
      BEGIN
        RETURN(LPAD(i_seq, 3, '0') || i_comp_item_num);
      END ord_idx_fn;
    BEGIN
      -- Get kit order table
      l_t_kit_ords := kit_ord_tab_fn(i_ord_stat, l_o_kit);
      -- Populate order seq table
      FOR i IN l_t_kit_ords.FIRST .. l_t_kit_ords.LAST LOOP
        l_ord_idx := ord_idx_fn(l_t_kit_ords(i).seq, l_t_kit_ords(i).comp_item_num);
        l_t_ord_seqs_i(l_ord_idx) := NULL;
      END LOOP;
      -- Get kit seq table
      l_t_kit_seqs_i := seq_tab_fn(i_r_rlse.div_part, i_ord_stat, l_o_kit);
      -- check for missing components
      l_seq_idx := l_t_kit_seqs_i.FIRST;
      WHILE l_seq_idx IS NOT NULL LOOP
        l_is_match := TRUE;
        l_t_kit_comp_qtys_i := l_t_kit_seqs_i(l_seq_idx);
        l_comp_idx := l_t_kit_comp_qtys_i.FIRST;
        WHILE(    l_is_match
              AND l_comp_idx IS NOT NULL) LOOP
          l_ord_idx := ord_idx_fn(l_seq_idx, l_comp_idx);
          l_is_match := l_t_ord_seqs_i.EXISTS(l_ord_idx);
          l_comp_idx := l_t_kit_comp_qtys_i.NEXT(l_comp_idx);
        END LOOP;

        IF NOT l_is_match THEN
          -- out all orders in the seq
          out_kit_ords_sp(i_r_rlse, l_t_kit_ords, l_seq_idx);
        END IF;   -- NOT v_is_match

        l_seq_idx := l_t_kit_seqs_i.NEXT(l_seq_idx);
      END LOOP;
    END prcs_miss_comp_sp;

    PROCEDURE get_ratios_sp(
      i_ord_stat     IN      VARCHAR2,
      i_o_kit        IN      kit_t,
      i_seq          IN      PLS_INTEGER,
      o_ord_ratio    OUT     PLS_INTEGER,
      o_avail_ratio  OUT     PLS_INTEGER
    ) IS
    BEGIN
      -- order ratio will be the same across all component items of same seq
      SELECT   ord.ratio, MIN(FLOOR((CASE
                                       WHEN w.qavc < ord.ord_qty THEN w.qavc
                                       ELSE ord.ord_qty
                                     END) / ord.comp_qty)) avail_ratio
          INTO o_ord_ratio, o_avail_ratio
          FROM div_mstr_di1d d, whsp300c w, sawp505e e,
               TABLE(kit_ord_fn(CURSOR(SELECT   b.statb, d2.div_id, ld.llr_dt - g_c_rensoft_seed_dt AS llr_dt,
                                                k.kit_typ, a.dsorda, k.item_num, se.cust_id, ld.load_num, se.stop_num,
                                                TRUNC(se.eta_ts) - g_c_rensoft_seed_dt AS eta_dt, a.cpoa, b.orditb,
                                                k.comp_qty
                                           FROM div_mstr_di1d d2, load_depart_op1f ld, stop_eta_op1g se, ordp100a a,
                                                ordp120b b, kit_item_mstr_kt1m k
                                          WHERE d2.div_id = i_o_kit.div_id
                                            AND ld.div_part = d2.div_part
                                            AND ld.llr_dt = g_c_rensoft_seed_dt + i_o_kit.llr_dt
                                            AND ld.load_num = i_o_kit.load_num
                                            AND se.div_part = ld.div_part
                                            AND se.load_depart_sid = ld.load_depart_sid
                                            AND se.cust_id = i_o_kit.cust_num
                                            AND se.stop_num = i_o_kit.stop_num
                                            AND TRUNC(se.eta_ts) = g_c_rensoft_seed_dt + i_o_kit.eta_date
                                            AND a.div_part = se.div_part
                                            AND a.load_depart_sid = se.load_depart_sid
                                            AND a.custa = se.cust_id
                                            AND a.dsorda = i_o_kit.ord_typ
                                            AND NVL(a.cpoa, ' ') = NVL(i_o_kit.po_num, ' ')
                                            AND a.excptn_sw = 'N'
                                            AND b.div_part = a.div_part
                                            AND b.ordnob = a.ordnoa
                                            AND b.statb = i_ord_stat
                                            AND b.excptn_sw = 'N'
                                            AND b.ordqtb > 0
                                            AND b.subrcb = 0
                                            AND k.div_part = b.div_part
                                            AND k.kit_typ = i_o_kit.kit_typ
                                            AND k.item_num = i_o_kit.kit_item_num
                                            AND k.comp_item_num = b.orditb
                                       GROUP BY b.statb, d2.div_id, ld.llr_dt, k.kit_typ, a.dsorda, k.item_num,
                                                se.cust_id, ld.load_num, se.stop_num, TRUNC(se.eta_ts), a.cpoa,
                                                b.orditb, k.comp_qty
                                      )
                               )
                    ) ord
         WHERE d.div_id = i_o_kit.div_id
           AND e.catite = ord.comp_item_num
           AND ord.seq = i_seq
           AND w.div_part = d.div_part
           AND w.ROWID = (SELECT MAX(w2.ROWID)
                            FROM div_mstr_di1d d3, whsp300c w2
                           WHERE d3.div_id = i_o_kit.div_id
                             AND w2.div_part = d3.div_part
                             AND w2.itemc = e.iteme
                             AND w2.uomc = e.uome
                             AND w2.taxjrc IS NULL
                             AND w2.qavc = (SELECT MAX(w3.qavc)
                                              FROM whsp300c w3
                                             WHERE w3.div_part = d3.div_part
                                               AND w3.itemc = w2.itemc
                                               AND w3.uomc = w2.uomc
                                               AND w3.taxjrc IS NULL))
      GROUP BY ord.ratio;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN TOO_MANY_ROWS THEN
        NULL;
    END get_ratios_sp;

    PROCEDURE alloc_kit_ord_sp(
      i_r_rlse     IN  g_cur_rlse%ROWTYPE,
      i_ord_num    IN  NUMBER,
      i_ord_ln     IN  NUMBER,
      i_alloc_qty  IN  NUMBER,
      i_item       IN  VARCHAR2,
      i_uom        IN  VARCHAR2
    ) IS
      l_r_inv  g_rt_inv;
    BEGIN
      l_r_inv := get_inv_fn(i_r_rlse, i_item, i_uom);
      alloc_ords_sp(i_r_rlse,
                    i_item,
                    i_uom,
                    l_r_inv.aisl,
                    l_r_inv.bin,
                    l_r_inv.lvl,
                    i_ord_num,
                    i_ord_ln,
                    i_alloc_qty,
                    'T',
                    0,
                    'ALLOC_KITS_SP'
                   );
      upd_inv_qty_sp(l_r_inv.row_id, i_alloc_qty);
    END alloc_kit_ord_sp;

    PROCEDURE alloc_kit_sp(
      i_r_rlse      IN  g_cur_rlse%ROWTYPE,
      i_t_kit_ords  IN  kit_ords_t,
      i_seq         IN  PLS_INTEGER
    ) IS
    BEGIN
      FOR i IN i_t_kit_ords.FIRST .. i_t_kit_ords.LAST LOOP
        IF i_t_kit_ords(i).seq = i_seq THEN
          alloc_kit_ord_sp(i_r_rlse,
                           i_t_kit_ords(i).order_num,
                           i_t_kit_ords(i).order_ln,
                           i_t_kit_ords(i).ord_qty,
                           i_t_kit_ords(i).item_num,
                           i_t_kit_ords(i).uom
                          );
        END IF;   -- i_t_kit_ords(i).seq = i_seq
      END LOOP;
    END alloc_kit_sp;

    PROCEDURE partial_kits_sp(
      i_r_rlse           IN  g_cur_rlse%ROWTYPE,
      i_t_kit_comp_qtys  IN  l_tt_kit_comp_qtys_i,
      i_t_kit_ords       IN  kit_ords_t,
      i_seq              IN  PLS_INTEGER,
      i_avail_ratio      IN  PLS_INTEGER
    ) IS
      l_new_ord_qty  PLS_INTEGER;
    BEGIN
      FOR i IN i_t_kit_ords.FIRST .. i_t_kit_ords.LAST LOOP
        IF i_t_kit_ords(i).seq = i_seq THEN
          l_new_ord_qty := i_t_kit_comp_qtys(i_t_kit_ords(i).comp_item_num) * i_avail_ratio;
          alloc_kit_ord_sp(i_r_rlse,
                           i_t_kit_ords(i).order_num,
                           i_t_kit_ords(i).order_ln,
                           l_new_ord_qty,
                           i_t_kit_ords(i).item_num,
                           i_t_kit_ords(i).uom
                          );
          create_partial_sp(i_r_rlse, i_t_kit_ords(i).order_num, i_t_kit_ords(i).order_ln);
          upd_orig_for_partial_sp(i_r_rlse, i_t_kit_ords(i).order_num, i_t_kit_ords(i).order_ln, 'INVOUT');
        END IF;   -- i_t_kit_ords(i).seq = i_seq
      END LOOP;
    END partial_kits_sp;

    PROCEDURE prcs_agg_item_kits_sp(
      i_r_rlse  IN  g_cur_rlse%ROWTYPE
    ) IS
      l_t_kits        kits_t;
      l_t_kit_ords    kit_ords_t;
      l_t_kit_seqs_i  l_tt_kit_seqs_i;
      l_seq           PLS_INTEGER;
      l_ord_ratio     PLS_INTEGER;
      l_avail_ratio   PLS_INTEGER;
    BEGIN
      l_t_kits := kit_tab_fn(i_r_rlse, g_c_aggregate_item_typ, l_c_ord_stat_pnd);

      IF l_t_kits.COUNT > 0 THEN
        <<kit_loop>>
        FOR i IN l_t_kits.FIRST .. l_t_kits.LAST LOOP
          logs.dbg('Process Missing Components');
          prcs_miss_comp_sp(i_r_rlse, l_c_ord_stat_pnd, l_t_kits(i));
          logs.dbg('Get Kit Sequence');
          l_t_kit_seqs_i := seq_tab_fn(i_r_rlse.div_part, l_c_ord_stat_pnd, l_t_kits(i));
          logs.dbg('Get table of orders');
          l_t_kit_ords := kit_ord_tab_fn(l_c_ord_stat_pnd, l_t_kits(i));
          <<seq_loop>>
          l_seq := l_t_kit_seqs_i.FIRST;
          WHILE l_seq IS NOT NULL LOOP
            logs.dbg('Get Ratios');
            get_ratios_sp(l_c_ord_stat_pnd, l_t_kits(i), l_seq, l_ord_ratio, l_avail_ratio);

            IF l_avail_ratio = l_ord_ratio THEN
              logs.dbg('Allocate all kit orders');
              alloc_kit_sp(i_r_rlse, l_t_kit_ords, l_seq);
            ELSE
              IF    l_avail_ratio = 0
                 OR l_t_kits(i).ord_typ = g_c_dist THEN
                logs.dbg('Out all kit orders for current seq');
                out_kit_ords_sp(i_r_rlse, l_t_kit_ords, l_seq);
              ELSE
                logs.dbg('Partially allocate and out the remainder');
                partial_kits_sp(i_r_rlse, l_t_kit_seqs_i(l_seq), l_t_kit_ords, l_seq, l_avail_ratio);
              END IF;   -- l_avail_ratio = 0 OR l_t_kits(i).ord_typ = l_c_dist
            END IF;   -- l_avail_ratio = l_ord_ratio

            l_seq := l_t_kit_seqs_i.NEXT(l_seq);
          END LOOP seq_loop;
          COMMIT;
        END LOOP kit_loop;
      END IF;   -- l_t_kit.COUNT > 0
    END prcs_agg_item_kits_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'LLRDt', i_r_rlse.llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_r_rlse.load_list);
    logs.dbg('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Get Kit Types');

    SELECT   k.kit_typ
    BULK COLLECT INTO l_t_kit_typs
        FROM kit_item_mstr_kt1m k
       WHERE k.div_part = i_r_rlse.div_part
    GROUP BY k.kit_typ;

    IF l_t_kit_typs.COUNT > 0 THEN
      FOR i IN l_t_kit_typs.FIRST .. l_t_kit_typs.LAST LOOP
        CASE l_t_kit_typs(i)
          WHEN g_c_aggregate_item_typ THEN
            logs.dbg('Process Aggregat Item Kits');
            prcs_agg_item_kits_sp(i_r_rlse);
          ELSE
            NULL;
        END CASE;
      END LOOP;
    END IF;

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END alloc_kits_sp;

  /*
  ||----------------------------------------------------------------------------
  || CUTDOWNS_SP
  ||  This procedure is called when there is not enough inventory for an item
  ||  that is not in a Tax Jurisdiction. It checks to see if the same item exists
  ||  somewhere else in the Warehouse with an "equal or higher" pack level.  If
  ||  inventory is found somewhere else, this procedure creates a Work Order for
  ||  the warehouse and transfers inventory from one item/location to another.
  ||  It handles the differences in Pack.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/14/02 | sudheer | qualify the columns with table  name
  || 03/27/02 | justani | Change to handle new FC to SS Cutdown controls (new table)
  || 09/04/02 | rhalpai | Added logic to protect FC item from customers who do not
  ||                    | have protection on the SS item but to bypass FC item
  ||                    | protection for customers with SS protected items
  || 04/15/04 | rhalpai | Changed call to
  ||                    | OP_PROTECTED_INVENTORY_PK.PRTCTD_ITEM_QTY_FN to
  ||                    | pass SYSDATE instead of LLR date.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 04/07/06 | rhalpai | Moved from ALLOCATION_SP to allow calls from
  ||                    | ALLOC_BUNDLE_DIST_SP. PIR2545
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Changed input parm to SSEL inventory record from
  ||                    | ROWID and removed SSEL lookup cursor.
  ||                    | Converted to use new INS_CUTDOWN_TRANS_SP.
  || 03/01/10 | rhalpai | Changed FC inventory cursor to return slot and changed
  ||                    | calls to INS_CUTDOWN_TRANS_SP pass item/uom/slot
  ||                    | instead of row_id. PIR0024
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cutdowns_sp(
    i_r_rlse          IN      g_cur_rlse%ROWTYPE,
    i_r_ss_inv        IN      g_rt_inv,
    i_max_qty         IN      PLS_INTEGER,
    i_min_qty         IN      PLS_INTEGER,
    o_ss_trnsfr_qty   OUT     PLS_INTEGER,
    i_prtctd_cust_sw  IN      VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm      := 'OP_ALLOCATE_PK.CUTDOWNS_SP';
    lar_parm                    logs.tar_parm;
    l_fc_trnsfr_qty             PLS_INTEGER;
    l_c_trans_user_id  CONSTANT VARCHAR2(20)       := 'CUTDOWNS_SP';

    CURSOR l_cur_fc(
      b_div_part  NUMBER,
      b_item      VARCHAR2,
      b_uom       VARCHAR2
    ) IS
      SELECT        w1.ROWID AS fc_rowid, w1.itemc AS item, w1.uomc AS uom, w1.aislc AS aisl, w1.binc AS bin,
                    w1.levlc AS lvl, w1.qavc AS qty_avail, mb.qty_fctr
               FROM whsp300c w1, div_item_alt mb
              WHERE w1.div_part = b_div_part
                AND w1.itemc = mb.alt_item
                AND w1.uomc = mb.alt_uom
                AND mb.div_part = b_div_part
                AND mb.item_num = b_item
                AND mb.item_uom = b_uom
                AND mb.alt_typ = 'SS'
                AND w1.taxjrc IS NULL
                AND w1.qavc > 0
                AND w1.ROWID = (SELECT MAX(w2.ROWID)
                                  FROM whsp300c w2
                                 WHERE w2.div_part = w1.div_part
                                   AND w2.itemc = w1.itemc
                                   AND w2.uomc = w1.uomc
                                   AND w2.taxjrc IS NULL
                                   AND w2.qavc = (SELECT MAX(w3.qavc)
                                                    FROM whsp300c w3
                                                   WHERE w3.div_part = w2.div_part
                                                     AND w3.itemc = w2.itemc
                                                     AND w3.uomc = w2.uomc
                                                     AND w3.taxjrc IS NULL))
      FOR UPDATE OF w1.qavc;

    l_r_fc                      l_cur_fc%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'SSItem', i_r_ss_inv.item);
    logs.add_parm(lar_parm, 'SSUom', i_r_ss_inv.uom);
    logs.add_parm(lar_parm, 'SSSlot', i_r_ss_inv.aisl || i_r_ss_inv.bin || i_r_ss_inv.lvl);
    logs.add_parm(lar_parm, 'SSRowid', ROWIDTOCHAR(i_r_ss_inv.row_id));
    logs.add_parm(lar_parm, 'MaxQty', i_max_qty);
    logs.add_parm(lar_parm, 'MinQty', i_min_qty);
    logs.add_parm(lar_parm, 'PrtctdCustSW', i_prtctd_cust_sw);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    o_ss_trnsfr_qty := 0;

    BEGIN
      logs.dbg('Check for FC to SS Cutdowns and Lock FC Item');

      OPEN l_cur_fc(i_r_rlse.div_part, i_r_ss_inv.item, i_r_ss_inv.uom);

      FETCH l_cur_fc
       INTO l_r_fc;

      CLOSE l_cur_fc;
    EXCEPTION
      WHEN OTHERS THEN
        IF l_cur_fc%ISOPEN THEN
          CLOSE l_cur_fc;
        END IF;   -- l_cur_fc%ISOPEN

        RAISE;
    END;

    IF l_r_fc.item IS NOT NULL THEN
      -- Protect FC item from customers who do not have protection on the SS item
      -- but bypass FC item protection for customers with SS protected items
      IF i_prtctd_cust_sw = 'N' THEN
        logs.dbg('Get Proctected Item Qty');

        DECLARE
          l_prtctd_item_qty  PLS_INTEGER;
        BEGIN
          l_prtctd_item_qty := op_protected_inventory_pk.prtctd_item_qty_fn(i_r_rlse.div_id,
                                                                            NULL,
                                                                            NULL,
                                                                            TRUNC(SYSDATE),
                                                                            l_r_fc.item,
                                                                            l_r_fc.uom
                                                                           );
          l_r_fc.qty_avail := l_r_fc.qty_avail - l_prtctd_item_qty;

          IF l_r_fc.qty_avail < 0 THEN
            l_r_fc.qty_avail := 0;
          END IF;   -- l_r_fc.qty_avail < 0
        END;
      END IF;   -- i_prtctd_cust_sw = 'N'

      IF (l_r_fc.qty_avail * l_r_fc.qty_fctr) >= i_min_qty THEN
        IF (l_r_fc.qty_avail * l_r_fc.qty_fctr) >= i_max_qty THEN
          l_fc_trnsfr_qty := CEIL(i_max_qty / l_r_fc.qty_fctr);
        ELSE
          -- Handle Partially fulfilled Transfers
          l_fc_trnsfr_qty := l_r_fc.qty_avail;
        END IF;   -- (l_r_fc.qty_avail * l_r_fc.qty_fctr) >= i_max_qty

        o_ss_trnsfr_qty := l_fc_trnsfr_qty * l_r_fc.qty_fctr;
        logs.dbg('Upd SS Item for FC to SS Cutdowns');

        UPDATE whsp300c w
           SET w.qohc = w.qohc + o_ss_trnsfr_qty,
               w.qavc = w.qavc + o_ss_trnsfr_qty
         WHERE w.ROWID = i_r_ss_inv.row_id;

        logs.dbg('Upd FC Item for FC to SS Cutdowns');

        UPDATE whsp300c w
           SET w.qohc = w.qohc - l_fc_trnsfr_qty,
               w.qavc = w.qavc - l_fc_trnsfr_qty
         WHERE w.ROWID = l_r_fc.fc_rowid;

        logs.dbg('Log Cutdown Transaction for FC Slot');
        ins_cutdown_trans_sp(i_r_rlse,
                             g_c_trans_cut_from,
                             l_r_fc.item,
                             l_r_fc.uom,
                             l_r_fc.aisl,
                             l_r_fc.bin,
                             l_r_fc.lvl,
                             l_fc_trnsfr_qty,
                             l_c_trans_user_id
                            );
        logs.dbg('Log Cutdown Transaction for SS Slot');
        ins_cutdown_trans_sp(i_r_rlse,
                             g_c_trans_cut_to,
                             i_r_ss_inv.item,
                             i_r_ss_inv.uom,
                             i_r_ss_inv.aisl,
                             i_r_ss_inv.bin,
                             i_r_ss_inv.lvl,
                             o_ss_trnsfr_qty,
                             l_c_trans_user_id
                            );
      END IF;   -- (l_r_fc.qty_avail * l_r_fc.qty_fctr) >= i_min_qty

      timer.stopme(l_c_module || env.get_session_id);
      logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   -- l_r_fc.item IS NOT NULL

--    env.untag();
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cutdowns_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_BUNDLE_ITEMS_TAB_SP
  ||  Get array of bundle item info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_bundle_items_tab_sp(
    i_r_rlse    IN             g_cur_rlse%ROWTYPE,
    i_dist_id   IN             VARCHAR2,
    i_dist_sfx  IN             NUMBER,
    io_t_items  IN OUT NOCOPY  g_tt_bundle_items
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    env.tag();

    OPEN l_cv
     FOR
       SELECT bi.item_num, bi.unq_cd, DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb) AS wrk_item_num,
              DECODE(TRIM(di.suomb), NULL, di.uomb, di.suomb) AS wrk_uom
         FROM bundl_dist_item_bd1i bi, mclp110b di
        WHERE bi.div_part = i_r_rlse.div_part
          AND bi.dist_id = i_dist_id
          AND bi.dist_sfx = i_dist_sfx
          AND di.div_part = bi.div_part
          AND di.itemb = bi.item_num
          AND di.uomb = bi.unq_cd;

    FETCH l_cv
    BULK COLLECT INTO io_t_items;

    env.untag();
  END get_bundle_items_tab_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_ORDS_TAB_SP
  ||  Get array of order info for bundle distributions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 03/01/10 | rhalpai | Replaced LLRDate parm with RelseaseTS and changed
  ||                    | logic from Open Cursor/Fetch Bulk Collect Into to
  ||                    | just a Select Bulk Collect Into since the Bulk Collect
  ||                    | will eliminate the NoDataFound error and initialize
  ||                    | the return collection variable. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_ords_tab_sp(
    i_r_rlse    IN             g_cur_rlse%ROWTYPE,
    i_dist_id   IN             VARCHAR2,
    i_dist_sfx  IN             NUMBER,
    i_cust_id   IN             VARCHAR2,
    i_load_num  IN             VARCHAR2,
    i_stop_num  IN             NUMBER,
    i_eta_dt    IN             NUMBER,
    io_t_ords   IN OUT NOCOPY  g_tt_bundle_ords
  ) IS
  BEGIN
--    env.tag();

    SELECT b.ordnob,
           b.lineb,
           b.itemnb,
           b.sllumb,
           DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb) AS wrk_item_num,
           DECODE(TRIM(di.suomb), NULL, di.uomb, di.suomb) AS wrk_uom,
           b.ordqtb
    BULK COLLECT INTO io_t_ords
      FROM rlse_op1z r, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, bundl_dist_item_bd1i bi,
           mclp110b di
     WHERE r.div_part = i_r_rlse.div_part
       AND r.rlse_id = i_r_rlse.rlse_id
       AND ld.div_part = r.div_part
       AND ld.llr_dt = r.llr_dt
       AND ld.load_num = i_load_num
       AND se.div_part = ld.div_part
       AND se.load_depart_sid = ld.load_depart_sid
       AND se.cust_id = i_cust_id
       AND se.stop_num = i_stop_num
       AND TRUNC(se.eta_ts) = g_c_rensoft_seed_dt + i_eta_dt
       AND a.div_part = se.div_part
       AND a.load_depart_sid = se.load_depart_sid
       AND a.custa = se.cust_id
       AND a.excptn_sw = 'N'
       AND a.dsorda = 'D'
       AND b.div_part = a.div_part
       AND b.ordnob = a.ordnoa
       AND bi.div_part = a.div_part
       AND bi.dist_id = SUBSTR(a.legrfa, 1, 10)
       AND LPAD(bi.dist_sfx, 2, '0') = SUBSTR(a.legrfa, 12, 2)
       AND bi.item_num = b.itemnb
       AND bi.unq_cd = b.sllumb
       AND di.div_part(+) = b.div_part
       AND di.itemb(+) = b.itemnb
       AND di.uomb(+) = b.sllumb
       AND bi.dist_id = i_dist_id
       AND bi.dist_sfx = i_dist_sfx
       AND b.statb = 'P'
       AND b.excptn_sw = 'N'
       AND b.subrcb = 0;

--    env.untag();
  END get_ords_tab_sp;

  /*
  ||----------------------------------------------------------------------------
  || HAS_MISS_ITEM_FN
  ||  Indicate whether a bundle distribution has missing items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION has_miss_item_fn(
    i_t_ords   IN  g_tt_bundle_ords,
    i_t_items  IN  g_tt_bundle_items
  )
    RETURN BOOLEAN IS
    l_exists    BOOLEAN               := TRUE;
    l_idx       PLS_INTEGER;
    l_t_ords_v  op_types_pk.tt_nums_v;
    l_ord_idx   VARCHAR2(12);
  BEGIN
    IF i_t_ords IS NOT NULL THEN
      l_idx := i_t_ords.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_ord_idx := i_t_ords(l_idx).item_num || i_t_ords(l_idx).unq_cd;
        -- value assigned is unimportant
        -- only care about index for existence chacking
        l_t_ords_v(l_ord_idx) := NULL;
        l_idx := i_t_ords.NEXT(l_idx);
      END LOOP;
    END IF;   -- i_t_ords IS NOT NULL

    IF i_t_items IS NOT NULL THEN
      l_idx := i_t_items.FIRST;
      WHILE(    l_idx IS NOT NULL
            AND l_exists) LOOP
        l_ord_idx := i_t_items(l_idx).item_num || i_t_items(l_idx).unq_cd;
        l_exists := l_t_ords_v.EXISTS(l_ord_idx);
        l_idx := i_t_items.NEXT(l_idx);
      END LOOP;
    END IF;   -- i_t_items IS NOT NULL

    RETURN(NOT l_exists);
  END has_miss_item_fn;

  /*
  ||----------------------------------------------------------------------------
  || AVAIL_INV_SP
  ||  Indicate whether enough inventory qty is available to for order lines.
  ||  Will also process FC to SSEL cut-downs to meet demand for SSEL item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 03/01/10 | rhalpai | Changed logic to pass ReleaseTS to GET_INV_FN.
  ||                    | PIR0024
  ||----------------------------------------------------------------------------
  */
  PROCEDURE avail_inv_sp(
    i_r_rlse        IN      g_cur_rlse%ROWTYPE,
    i_t_ords        IN      g_tt_bundle_ords,
    i_t_items       IN      g_tt_bundle_items,
    o_inv_is_avail  OUT     BOOLEAN
  ) IS
    l_t_ord_qtys_v  op_types_pk.tt_nums_v;
    l_idx           PLS_INTEGER;
    l_ord_idx       VARCHAR2(12);
    l_r_inv         g_rt_inv;
    l_trnsfr_qty    PLS_INTEGER;
  BEGIN
    IF     i_t_items IS NOT NULL
       AND i_t_ords IS NOT NULL THEN
--      env.tag();
      -- get total order quantity by item
      l_idx := i_t_ords.FIRST;
      <<ord_loop>>
      WHILE l_idx IS NOT NULL LOOP
        l_ord_idx := i_t_ords(l_idx).wrk_item_num || i_t_ords(l_idx).wrk_uom;

        IF l_t_ord_qtys_v.EXISTS(l_ord_idx) THEN
          l_t_ord_qtys_v(l_ord_idx) := l_t_ord_qtys_v(l_ord_idx) + i_t_ords(l_idx).ord_qty;
        ELSE
          l_t_ord_qtys_v(l_ord_idx) := i_t_ords(l_idx).ord_qty;
        END IF;

        l_idx := i_t_ords.NEXT(l_idx);
      END LOOP ord_loop;
      o_inv_is_avail := TRUE;
      l_idx := i_t_items.FIRST;
      <<item_loop>>
      WHILE(    l_idx IS NOT NULL
            AND o_inv_is_avail) LOOP
        -- get available inventory
        l_r_inv := get_inv_fn(i_r_rlse, i_t_ords(l_idx).wrk_item_num, i_t_ords(l_idx).wrk_uom);
        -- check inventory available for total ord qty
        l_ord_idx := i_t_items(l_idx).wrk_item_num || i_t_items(l_idx).wrk_uom;

        IF l_r_inv.qty_avail < l_t_ord_qtys_v(l_ord_idx) THEN
          -- process FC to SSEL cut-downs
          cutdowns_sp(i_r_rlse,
                      l_r_inv,
                      l_t_ord_qtys_v(l_ord_idx) - l_r_inv.qty_avail,
                      l_t_ord_qtys_v(l_ord_idx) - l_r_inv.qty_avail,
                      l_trnsfr_qty
                     );

          IF l_trnsfr_qty > 0 THEN
            l_r_inv.qty_avail := l_r_inv.qty_avail + l_trnsfr_qty;
          END IF;   -- v_trnsfr_qty > 0
        END IF;   -- l_r_inv.qty_avail < t_ord_qtys_v(v_ord_idx)

        o_inv_is_avail :=(l_r_inv.qty_avail >= l_t_ord_qtys_v(l_ord_idx));
        l_idx := i_t_items.NEXT(l_idx);
      END LOOP item_loop;
--      env.untag();
    END IF;   -- i_t_items IS NOT NULL AND i_t_ords IS NOT NULL
  END avail_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || OUT_BUNDLE_ORDS_SP
  ||  Set bundle order lines to outs.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE out_bundle_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_t_ords  IN  g_tt_bundle_ords
  ) IS
    l_idx         PLS_INTEGER;
    l_t_ord_nums  type_ntab;
    l_t_ord_lns   type_ntab;
  BEGIN
    IF i_t_ords IS NOT NULL THEN
      l_idx := i_t_ords.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        add_to_tab_sp(l_t_ord_nums, i_t_ords(l_idx).ord_num);
        add_to_tab_sp(l_t_ord_lns, i_t_ords(l_idx).ord_ln);
        l_idx := i_t_ords.NEXT(l_idx);
      END LOOP;
      out_ords_sp(i_r_rlse, l_t_ord_nums, l_t_ord_lns);
    END IF;   -- i_t_ords IS NOT NULL
  END out_bundle_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_BUNDLE_ORDS_SP
  ||  Set bundle order lines to allocated and allocate inventory.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 03/01/10 | rhalpai | Changed logic to pass ReleaseTS to GET_INV_FN, pass
  ||                    | Div/Item/UOM/Slot to ALLOC_ORDS_SP instead of inv_rec,
  ||                    | and replaced call to ALLOC_INV_SP with UPD_INV_QTY_SP.
  ||                    | PIR0024
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_bundle_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_t_ords  IN  g_tt_bundle_ords
  ) IS
    l_idx    PLS_INTEGER;
    l_r_inv  g_rt_inv;
  BEGIN
    IF i_t_ords IS NOT NULL THEN
      l_idx := i_t_ords.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_r_inv := get_inv_fn(i_r_rlse, i_t_ords(l_idx).wrk_item_num, i_t_ords(l_idx).wrk_uom);
        alloc_ords_sp(i_r_rlse,
                      l_r_inv.item,
                      l_r_inv.uom,
                      l_r_inv.aisl,
                      l_r_inv.bin,
                      l_r_inv.lvl,
                      i_t_ords(l_idx).ord_num,
                      i_t_ords(l_idx).ord_ln,
                      i_t_ords(l_idx).ord_qty,
                      'T',
                      0,
                      'ALLOC_BUNDLE_ORDS_SP'
                     );
        upd_inv_qty_sp(l_r_inv.row_id, i_t_ords(l_idx).ord_qty);
        l_idx := i_t_ords.NEXT(l_idx);
      END LOOP;
    END IF;   -- i_t_ords IS NOT NULL
  END alloc_bundle_ords_sp;

  /*
  ||-------------------------------------------------------------------------
  || ALLOC_BUNDLE_DIST_SP
  ||  Perform allocation for a Bundled Item Distribution.
  ||
  ||  Distribution orders have their distribution ID and suffix stored in the
  ||  legacy reference area (legrfa) on the order header and each order
  ||  contains only one item. Distributions for bundled items are identified
  ||  by matching the distribution ID and suffix from the Bundled
  ||  Distribution Item table to the orders. Distributions for Bundled Items
  ||  require all items to be allocated in the same release. If an item is
  ||  missing from the release or there is insufficient available inventory
  ||  after processing any necessary full-case to single-sell cutdowns, all
  ||  orders are outed. Distribution orders that are outed are sent back down
  ||  from the mainframe to OP as new orders. If all items are present with
  ||  available inventory, all orders are allocated.
  ||
  ||  Bundled Item Distributions are identified by grouping the following:
  ||    Div,LLRdate,DistID,DistSfx,Customer,Load,Stop,EtaDate
  ||  Rules for Bundled Item Distributions
  ||  * If any item is missing then all orders must be outed.
  ||  * If any item cannot be fully allocated after processing any necessary
  ||    FC to SSEL cutdowns then all orders must be outed.
  ||  * No Subs/Replacements (not done for Reg Dist and not here either)
  ||  * No Cig items
  ||  * No Government Control items
  ||  * No Protected Inventory items
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/07/06 | rhalpai | Original - PIR2545
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Moved GET_BUNDLE_ITEMS_TAB_SP, GET_ORDS_TAB_SP,
  ||                    | HAS_MISS_ITEM_FN, AVAIL_INV_SP, ALLOC_BUNDLE_ORDS_SP
  ||                    | to stand-alone modules.
  ||                    | Added performance logic to Bulk-Fetch using Limit
  ||                    | within a loop.
  || 03/01/10 | rhalpai | Removed LLRDt parm and changed logic to pass ReleaseTS
  ||                    | instead of LLRDt to GET_ORDS_TAB_SP. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_bundle_dist_sp(
    i_r_rlse             IN  g_cur_rlse%ROWTYPE,
    i_dist_id            IN  VARCHAR2,
    i_dist_sfx           IN  NUMBER,
    i_invalid_bundle_sw  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm     := 'OP_ALLOCATE_PK.ALLOC_BUNDLE_DIST_SP';
    lar_parm             logs.tar_parm;
    l_inv_is_avail       BOOLEAN;
    l_t_bundle_items     g_tt_bundle_items := g_tt_bundle_items();
    l_t_ords             g_tt_bundle_ords  := g_tt_bundle_ords();

    TYPE l_rt_cust IS RECORD(
      cust_id   sysp200c.acnoc%TYPE,
      load_num  mclp120c.loadc%TYPE,
      stop_num  NUMBER,
      eta_dt    NUMBER
    );

    TYPE l_tt_custs IS TABLE OF l_rt_cust;

    l_t_custs            l_tt_custs        := l_tt_custs();

    -- At this point the Bundle Dist Cursor has already ensured all items are
    -- set up in the warehouse for inventory and has excluded any distribution
    -- which has items with cigs, government control or protected inventory.
    -- We now need only exclude customers in the Override Table.
    CURSOR l_cur_cust(
      b_div_part   NUMBER,
      b_llr_dt     DATE,
      b_load_list  VARCHAR2,
      b_dist_id    VARCHAR2,
      b_dist_sfx   NUMBER
    ) IS
      SELECT   se.cust_id, ld.load_num, se.stop_num, TRUNC(se.eta_ts) - g_c_rensoft_seed_dt AS eta_dt
          FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, bundl_dist_item_bd1i bi
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND a.dsorda = 'D'
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb = 'P'
           AND b.excptn_sw = 'N'
           AND b.subrcb = 0
           AND bi.div_part = b.div_part
           AND bi.dist_id = SUBSTR(a.legrfa, 1, 10)
           AND LPAD(bi.dist_sfx, 2, '0') = SUBSTR(a.legrfa, 12, 2)
           AND bi.item_num = b.itemnb
           AND bi.unq_cd = b.sllumb
           AND bi.dist_id = b_dist_id
           AND bi.dist_sfx = b_dist_sfx
           AND NOT EXISTS(SELECT 1   -- exclude if Cust in Override Table
                            FROM bundl_dist_cust_bd1c bc
                           WHERE bc.div_part = bi.div_part
                             AND bc.dist_id = bi.dist_id
                             AND bc.dist_sfx = bi.dist_sfx
                             AND bc.cust_id = a.custa)
      GROUP BY ld.depart_ts, se.cust_id, ld.load_num, se.stop_num, TRUNC(se.eta_ts)
      ORDER BY ld.depart_ts, se.stop_num;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'DistID', i_dist_id);
    logs.add_parm(lar_parm, 'DistSfx', i_dist_sfx);
    logs.add_parm(lar_parm, 'InvalidBundleSw', i_invalid_bundle_sw);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    logs.dbg('Get Collection of Items for Dist Bundle');
    get_bundle_items_tab_sp(i_r_rlse, i_dist_id, i_dist_sfx, l_t_bundle_items);
    logs.dbg('Open Customer Cursor');

    OPEN l_cur_cust(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list, i_dist_id, i_dist_sfx);

    <<cust_cur_loop>>
    LOOP
      logs.dbg('Fetch Customer Cursor');

      FETCH l_cur_cust
      BULK COLLECT INTO l_t_custs LIMIT 100;

      EXIT WHEN l_t_custs.COUNT = 0;
      logs.dbg('Process Customers for Dist Bundle');
      <<cust_tbl_loop>>
      FOR i IN l_t_custs.FIRST .. l_t_custs.LAST LOOP
        logs.dbg('Get Collection of Dist Bundle Orders for Customer');
        get_ords_tab_sp(i_r_rlse,
                        i_dist_id,
                        i_dist_sfx,
                        l_t_custs(i).cust_id,
                        l_t_custs(i).load_num,
                        l_t_custs(i).stop_num,
                        l_t_custs(i).eta_dt,
                        l_t_ords
                       );
        logs.dbg('Check for Invalid Bundle or Missing Items');

        IF    i_invalid_bundle_sw = 'Y'
           OR has_miss_item_fn(l_t_ords, l_t_bundle_items) THEN
          logs.dbg('Invalid Bundle or Missing Items So Out Bundle Orders');
          out_bundle_ords_sp(i_r_rlse, l_t_ords);
        ELSE
          logs.dbg('Ensure Inventory Available for all Orders');
          avail_inv_sp(i_r_rlse, l_t_ords, l_t_bundle_items, l_inv_is_avail);

          IF l_inv_is_avail THEN
            logs.dbg('Allocate Bundle Orders');
            alloc_bundle_ords_sp(i_r_rlse, l_t_ords);
          ELSE
            logs.dbg('Insufficient Inventory So Out Bundle Orders');
            out_bundle_ords_sp(i_r_rlse, l_t_ords);
          END IF;   -- l_inv_is_avail
        END IF;   -- i_invalid_bundle_sw = 'Y' OR has_miss_item_fn(l_t_ords, l_t_bundle_items)
      END LOOP cust_tbl_loop;
    END LOOP cust_cur_loop;

    CLOSE l_cur_cust;

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_cust%ISOPEN THEN
        CLOSE l_cur_cust;
      END IF;

      logs.err(lar_parm);
  END alloc_bundle_dist_sp;

  /*
  ||-------------------------------------------------------------------------
  || ALLOC_BUNDLE_DISTS_SP
  ||  Perform allocation for Bundled Item Distributions.
  ||
  ||  Distribution orders have their distribution ID and suffix stored in the
  ||  legacy reference area (legrfa) on the order header and each order
  ||  contains only one item. Distributions for bundled items are identified
  ||  by matching the distribution ID and suffix from the Bundled
  ||  Distribution Item table to the orders. Distributions for Bundled Items
  ||  require all items to be allocated in the same release. If an item is
  ||  missing from the release or there is insufficient available inventory
  ||  after processing any necessary full-case to single-sell cutdowns, all
  ||  orders are outed. Distribution orders that are outed are sent back down
  ||  from the mainframe to OP as new orders. If all items are present with
  ||  available inventory, all orders are allocated.
  ||
  ||  Bundled Item Distributions are identified by grouping the following:
  ||    Div,LLRdate,DistID,DistSfx,Customer,Load,Stop,EtaDate
  ||  Rules for Bundled Item Distributions
  ||  * If any item is missing then all orders must be outed.
  ||  * If any item cannot be fully allocated after processing any necessary
  ||    FC to SSEL cutdowns then all orders must be outed.
  ||  * No Subs/Replacements (not done for Reg Dist and not here either)
  ||  * No Cig items
  ||  * No Government Control items
  ||  * No Protected Inventory items
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/07/06 | rhalpai | Original - PIR2545
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Changed cursors to use load list from
  ||                    | MCLANE_LOAD_LABEL_RLSE.
  ||                    | Added performance logic to Bulk-Fetch using Limit
  ||                    | within a loop.
  || 03/01/10 | rhalpai | Removed LLRDt parm. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_bundle_dists_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_BUNDLE_DISTS_SP';
    lar_parm             logs.tar_parm;

    TYPE l_rt_bundle IS RECORD(
      dist_id   bundl_dist_item_bd1i.dist_id%TYPE,
      dist_sfx  NUMBER,
      invalid   VARCHAR2(1)
    );

    TYPE l_tt_bundles IS TABLE OF l_rt_bundle;

    l_t_bundles          l_tt_bundles  := l_tt_bundles();

    CURSOR l_cur_bundle_dist(
      b_div_part   NUMBER,
      b_llr_dt     DATE,
      b_load_list  VARCHAR2
    ) IS
      SELECT   bi.dist_id, bi.dist_sfx,
               MAX
                 ((CASE
                     WHEN EXISTS(SELECT 1
                                   FROM bundl_dist_item_bd1i bi2
                                  WHERE bi2.div_part = b_div_part
                                    AND bi2.dist_id = bi.dist_id
                                    AND bi2.dist_sfx = bi.dist_sfx
                                    AND (   NOT EXISTS(SELECT 1   -- exclude if any item is cig or not set up in warehouse
                                                         FROM mclp110b di, sawp505e se, whsp300c w1
                                                        WHERE di.div_part = b_div_part
                                                          AND di.itemb = bi2.item_num
                                                          AND di.uomb = bi2.unq_cd
                                                          AND se.iteme =
                                                                       DECODE(TRIM(di.suomb),
                                                                              NULL, di.itemb,
                                                                              di.sitemb
                                                                             )
                                                          AND se.uome = DECODE(TRIM(di.suomb), NULL, di.uomb, di.suomb)
                                                          AND w1.div_part = b_div_part
                                                          AND w1.itemc = se.iteme
                                                          AND w1.uomc = se.uome
                                                          AND w1.uomc NOT IN('CII', 'CIR', 'CIC')
                                                          AND w1.taxjrc IS NULL)
                                         OR EXISTS(SELECT 1   -- exclude if Gov Cntl for any Bundled Item
                                                     FROM mclp110b di, gov_cntl_item_p660a p660a
                                                    WHERE di.div_part = b_div_part
                                                      AND di.itemb = bi2.item_num
                                                      AND di.uomb = bi2.unq_cd
                                                      AND p660a.div_part = di.div_part
                                                      AND p660a.item_num =
                                                                       DECODE(TRIM(di.suomb),
                                                                              NULL, di.itemb,
                                                                              di.sitemb
                                                                             )
                                                      AND p660a.uom = DECODE(TRIM(di.suomb), NULL, di.uomb, di.suomb))
                                         OR EXISTS(SELECT 1   -- exclude if Protected Inventory for any Bundled Item
                                                     FROM mclp110b di, sawp505e se, whsp300c w1, prtctd_inv_op1i pi
                                                    WHERE di.div_part = b_div_part
                                                      AND di.itemb = bi2.item_num
                                                      AND di.uomb = bi2.unq_cd
                                                      AND se.iteme = DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb)
                                                      AND se.uome = DECODE(TRIM(di.suomb), NULL, di.uomb, di.suomb)
                                                      AND w1.div_part = b_div_part
                                                      AND w1.itemc = se.iteme
                                                      AND w1.uomc = se.uome
                                                      AND w1.taxjrc IS NULL
                                                      AND pi.div_part = w1.div_part
                                                      AND pi.zone_id = w1.zonec
                                                      AND pi.tax_jrsdctn IS NULL
                                                      AND pi.ord_item_num = se.catite
                                                      AND TRUNC(SYSDATE) BETWEEN pi.eff_dt AND pi.end_dt
                                                      AND pi.stat_cd = 'ACT')
                                        )) THEN 'Y'
                     ELSE 'N'
                   END
                  )
                 ) AS invalid
          FROM bundl_dist_item_bd1i bi
         WHERE bi.div_part = b_div_part
           AND EXISTS(SELECT 1
                        FROM load_depart_op1f ld, ordp100a a, ordp120b b
                       WHERE ld.div_part = b_div_part
                         AND ld.llr_dt = b_llr_dt
                         AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                         AND a.div_part = ld.div_part
                         AND a.load_depart_sid = ld.load_depart_sid
                         AND a.excptn_sw = 'N'
                         AND a.dsorda = 'D'
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.statb = 'P'
                         AND b.excptn_sw = 'N'
                         AND b.ordqtb > 0
                         AND b.subrcb = 0
                         AND SUBSTR(a.legrfa, 1, 10) = bi.dist_id
                         AND SUBSTR(a.legrfa, 12, 2) = LPAD(bi.dist_sfx, 2, '0')
                         AND b.itemnb = bi.item_num
                         AND b.sllumb = bi.unq_cd
                         AND NOT EXISTS(SELECT 1
                                          FROM bundl_dist_cust_bd1c bc
                                         WHERE bc.div_part = a.div_part
                                           AND bc.dist_id = bi.dist_id
                                           AND bc.dist_sfx = bi.dist_sfx
                                           AND bc.cust_id = a.custa))
      GROUP BY bi.dist_id, bi.dist_sfx;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Bundle Dist Cursor');
    env.tag();

    OPEN l_cur_bundle_dist(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list);

    <<cursor_loop>>
    LOOP
      logs.dbg('Fetch Bundle Dist Cursor');

      FETCH l_cur_bundle_dist
      BULK COLLECT INTO l_t_bundles LIMIT 100;

      EXIT WHEN l_t_bundles.COUNT = 0;
      logs.dbg('Allocate Bundle Dist');
      <<tbl_loop>>
      FOR i IN l_t_bundles.FIRST .. l_t_bundles.LAST LOOP
        alloc_bundle_dist_sp(i_r_rlse, l_t_bundles(i).dist_id, l_t_bundles(i).dist_sfx, l_t_bundles(i).invalid);
      END LOOP tbl_loop;
    END LOOP cursor_loop;

    CLOSE l_cur_bundle_dist;

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_bundle_dist%ISOPEN THEN
        CLOSE l_cur_bundle_dist;
      END IF;

      logs.err(lar_parm);
  END alloc_bundle_dists_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRTCTD_INV_SP
  ||  Return record of Protected Inventory info for Order Line
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prtctd_inv_sp(
    i_r_rlse         IN      g_cur_rlse%ROWTYPE,
    i_ord_num        IN      NUMBER,
    i_ord_ln         IN      NUMBER,
    i_inv_qty_avail  IN      PLS_INTEGER,
    o_rt_prtctd_inv  OUT     op_protected_inventory_pk.g_rt_prtctd_inv
  ) IS
  BEGIN
    o_rt_prtctd_inv := op_protected_inventory_pk.prtctd_inv_info_fn(i_r_rlse.div_id,
                                                                    i_ord_num,
                                                                    i_ord_ln,
                                                                    i_inv_qty_avail
                                                                   );
    excp.assert((o_rt_prtctd_inv.prtctd_item_qty IS NOT NULL),
                'Error in Call to OP_PROTECTED_INVENTORY_PK.PRTCTD_INV_INFO_FN('
                || i_ord_num
                || ','
                || i_ord_ln
                || ','
                || i_inv_qty_avail
                || ')'
               );
  END prtctd_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOG_PRTCTD_INV_SP
  ||  Adjust protected qty and log usage for Order Line
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_prtctd_inv_sp(
    i_r_rlse           IN  g_cur_rlse%ROWTYPE,
    i_prtctd_id        IN  NUMBER,
    i_prtctd_cust_qty  IN  PLS_INTEGER,
    i_ord_num          IN  NUMBER,
    i_ord_ln           IN  NUMBER,
    i_adj_ord_qty      IN  PLS_INTEGER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.LOG_PRTCTD_INV_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_prtctd_adj_qty      PLS_INTEGER;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'PrtctdID', i_prtctd_id);
    logs.add_parm(lar_parm, 'PrtctdCustQty', i_prtctd_cust_qty);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'AdjOrdQty', i_adj_ord_qty);
    logs.dbg('ENTRY', lar_parm);
    l_prtctd_adj_qty :=(CASE
                          WHEN i_prtctd_cust_qty < i_adj_ord_qty THEN i_prtctd_cust_qty
                          ELSE i_adj_ord_qty
                        END);

    -- Don't log zeros
    IF l_prtctd_adj_qty > 0 THEN
      timer.startme(l_c_module || env.get_session_id);
      logs.dbg('Upd Protected Inventory');

      UPDATE prtctd_inv_op1i i
         SET i.prtctd_qty = i.prtctd_qty - l_prtctd_adj_qty,
             i.user_id = op_protected_inventory_pk.g_c_rlse,
             i.last_chg_ts = l_c_sysdate
       WHERE i.div_part = i_r_rlse.div_part
         AND i.prtctd_id = i_prtctd_id;

      logs.dbg('Log Protected Inventory');

      INSERT INTO prtctd_alloc_log_op1a
                  (tran_id, prtctd_id, ord_num, ln_num, qty, last_chg_ts,
                   stat_cd, div_part
                  )
           VALUES (op1a_tran_id_seq.NEXTVAL, i_prtctd_id, i_ord_num, i_ord_ln, l_prtctd_adj_qty, l_c_sysdate,
                   op_protected_inventory_pk.g_c_rlse, i_r_rlse.div_part
                  );

      timer.stopme(l_c_module || env.get_session_id);
      logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   -- l_prtctd_adj_qty > 0
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END log_prtctd_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_UNALLOC_CON_SUB_SP
  ||  Remove unallocated conditional sub order line
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_unalloc_con_sub_sp(
    i_r_rlse   IN  g_cur_rlse%ROWTYPE,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ALLOCATE_PK.DEL_UNALLOC_CON_SUB_SP';
    lar_parm             logs.tar_parm;
    l_item               sawp505e.iteme%TYPE;
    l_uom                sawp505e.uome%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);

    IF i_ord_ln - FLOOR(i_ord_ln) BETWEEN .01 AND .69 THEN
      timer.startme(l_c_module || env.get_session_id);
      logs.dbg('Remove Unallocated Conditional Sub Line');

      DELETE FROM ordp120b
            WHERE div_part = i_r_rlse.div_part
              AND ordnob = i_ord_num
              AND lineb = i_ord_ln
        RETURNING itemnb, sllumb
             INTO l_item, l_uom;

      logs.dbg('Log Conditional Sub Removal');

      INSERT INTO mclp300d
                  (ordnod, ordlnd, reasnd, descd, exlvld, itemd, uomd, resexd,
                   exdesd, div_part
                  )
           VALUES (i_ord_num, i_ord_ln, 'CNSUBDEL', 'Unallocated Conditional Sub Delete', 4, l_item, l_uom, '0',
                   i_r_rlse.rlse_ts_char, i_r_rlse.div_part
                  );

      logs.dbg('Upd Orig Line to No Sub');

      UPDATE ordp120b b
         SET b.ordqtb = b.orgqtb,
             b.subrcb = 0,
             b.ntshpb = NULL,
             b.statb = 'P'
       WHERE b.div_part = i_r_rlse.div_part
         AND b.ordnob = i_ord_num
         AND b.lineb = FLOOR(i_ord_ln)
         AND NOT EXISTS(SELECT 1
                          FROM ordp120b b2
                         WHERE b2.div_part = i_r_rlse.div_part
                           AND b2.ordnob = i_ord_num
                           AND FLOOR(b2.lineb) = FLOOR(i_ord_ln)
                           AND b2.lineb <> FLOOR(i_ord_ln)
                           AND b2.excptn_sw = 'N'
                           AND b2.statb = 'T');

      timer.stopme(l_c_module || env.get_session_id);
      logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    END IF;   -- i_ord_ln - FLOOR(i_ord_ln) BETWEEN .01 AND .69
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_unalloc_con_sub_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_SUB_LNS_SP
  ||  This procedure will delete sub lines that have not been allocated
  ||  (created by the Order Receipt process) and will update the Original order
  ||  so that it can be allocated (sub code 998 will be changed to 0) by the
  ||  order allocation process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/18/02 | rhalpai | Added call to Reprice Module for Original Order Line
  ||                    | when Deleting a Sub Line.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  ||                    | Removed status parm from call to
  ||                    | OP_REPRICE_PK.REPRICE_ORDER_LINE_SP.
  ||                    | Added SAVEPOINT and exception handler for call to
  ||                    | OP_REPRICE_PK.REPRICE_ORDER_LINE_SP since it may now
  ||                    | raise an exception and the current logic continues
  ||                    | execution after logging the failure.
  || 01/13/06 | rhalpai | Added update of original order lines in exception well
  ||                    | to set the sub code to zero and the not-ship-reason to
  ||                    | a NULL before Repricing the order line. PIR3159
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Replaced LLRDate input parm with ReleaseTS.
  || 03/01/10 | rhalpai | Added CigSw parm and used in cursor to select cigs or
  ||                    | non-cigs. Changed to set not-ship-reason to
  ||                    | prev-not-ship-reason instead of NULL. Changed to call
  ||                    | REPRICE_ORD_LN_SP instead of REPRICE_ORDER_LINE_SP.
  ||                    | PIR0024
  || 07/15/10 | rhalpai | Changed logic to set not-ship-rsn to NULL for original
  ||                    | order lines in the Good Well prior to Repricing them.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/05/11 | rhalpai | Added logic to adjust PickQty for Weekly Max Cust Item
  ||                    | and add Weekly Max Log entries for matching Cig/NonCig
  ||                    | allocated OrdLns. Added logic to apply Weekly Max Qtys
  ||                    | for matching Cig/NonCig OrdLns with SubCd 998.
  ||                    | PIR6235
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in calls to
  ||                    | OP_REPRICE_PK.REPRICE_ORD_LN_SP, OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_sub_lns_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_cig_sw  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.DEL_SUB_LNS_SP';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_subs(
      b_div_part   NUMBER,
      b_llr_dt     DATE,
      b_load_list  VARCHAR2,
      b_cig_sw     VARCHAR2
    ) IS
      SELECT b.ordnob AS ord_num, b.lineb AS ord_ln, b.itemnb AS item, b.sllumb AS uom
        FROM load_depart_op1f ld, ordp100a a, ordp120b b
       WHERE ld.div_part = b_div_part
         AND ld.llr_dt = b_llr_dt
         AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.excptn_sw = 'N'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'P'
         AND b.excptn_sw = 'N'
         AND b.subrcb BETWEEN 1 AND 997
         AND b.ordqtb > 0
         AND NVL(b.pckqtb, 0) = 0
         AND (   (    b_cig_sw = 'Y'
                  AND b.sllumb IN('CII', 'CIR', 'CIC'))
              OR (    b_cig_sw = 'N'
                  AND b.sllumb NOT IN('CII', 'CIR', 'CIC'))
             );
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'CigSw', i_cig_sw);
    logs.dbg('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Upd and Log Allocated Wkly Max Qtys');
    upd_and_log_wkly_maxs_sp(i_r_rlse, i_cig_sw);
    logs.dbg('Build Non Allocated Subs Cursor');
    FOR l_r_sub IN l_cur_subs(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list, NVL(i_cig_sw, 'N')) LOOP
      logs.dbg('Log OrderReceipt Sub Removal');
      op_mclp300d_pk.ins_sp(i_r_rlse.div_part,
                            l_r_sub.ord_num,
                            l_r_sub.ord_ln,
                            'ORSUBDEL',
                            l_r_sub.item,
                            l_r_sub.uom,
                            NULL,
                            NULL
                           );
      logs.dbg('Delete Unallocated Sub Line');

      DELETE FROM ordp120b
            WHERE div_part = i_r_rlse.div_part
              AND ordnob = l_r_sub.ord_num
              AND lineb = l_r_sub.ord_ln;

      logs.dbg('Upd Orig Order Line');

      -- Orig OrdLn in needs to have ntshpb of NULL to be included in RepriceOrderLine
      UPDATE ordp120b b
         SET b.ntshpb = DECODE(b.excptn_sw, 'Y', b.zipcdb)
       WHERE b.div_part = i_r_rlse.div_part
         AND b.ordnob = l_r_sub.ord_num
         AND b.lineb = FLOOR(l_r_sub.ord_ln);

      logs.dbg('Reprice Order Line');

      BEGIN
        SAVEPOINT b4_reprice_ord_ln;
        op_reprice_pk.reprice_ord_ln_sp(i_r_rlse.div_part, l_r_sub.ord_num, FLOOR(l_r_sub.ord_ln));
      EXCEPTION
        WHEN OTHERS THEN
          logs.warn('Failed to Reprice Original Order Line for Deleted Sub'
                    || cnst.newline_char
                    || ' OrdNum: '
                    || util.to_str(l_r_sub.ord_num)
                    || ' OrdLn: '
                    || util.to_str(FLOOR(l_r_sub.ord_ln)),
                    lar_parm
                   );
          ROLLBACK TO SAVEPOINT b4_reprice_ord_ln;
      END;

      logs.dbg('Upd Orig Order Line');

      UPDATE ordp120b
         SET subrcb = 998,
             ntshpb = NULL,
             orgqtb = ordqtb
       WHERE div_part = i_r_rlse.div_part
         AND ordnob = l_r_sub.ord_num
         AND lineb = FLOOR(l_r_sub.ord_ln)
         AND excptn_sw = 'N';

      COMMIT;
    END LOOP;
    logs.dbg('Apply Wkly Max Qtys to Orig OrdLns');
    apply_wkly_maxs_sp(i_r_rlse, i_cig_sw, 998);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_sub_lns_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_TAGGED_ORIG_SUB_CDS_SP
  ||  Reset sub code to zero for original order lines temporarily tagged with
  ||  sub code 998 after subs were removed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_tagged_orig_sub_cds_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_cig_sw  IN  VARCHAR2
  ) IS
  BEGIN
    env.tag();

    UPDATE ordp120b b
       SET b.subrcb = 0
     WHERE EXISTS(SELECT 1
                    FROM load_depart_op1f ld, ordp100a a
                   WHERE ld.div_part = i_r_rlse.div_part
                     AND ld.llr_dt = i_r_rlse.llr_dt
                     AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                     AND a.div_part = ld.div_part
                     AND a.load_depart_sid = ld.load_depart_sid
                     AND a.ordnoa = b.ordnob)
       AND b.div_part = i_r_rlse.div_part
       AND b.statb IN('P', 'T')
       AND b.excptn_sw = 'N'
       AND b.subrcb = (SELECT subrcb_cd s
                         FROM mclane_subrcb_codes s
                        WHERE s.allocate_prcs_sw = 'S')
       AND (   (    i_cig_sw = 'Y'
                AND b.sllumb IN('CII', 'CIR', 'CIC'))
            OR (    i_cig_sw = 'N'
                AND b.sllumb NOT IN('CII', 'CIR', 'CIC'))
           );

    COMMIT;
    env.untag();
  END reset_tagged_orig_sub_cds_sp;

  /*
  ||----------------------------------------------------------------------------
  || CREATE_SUB_LNS_SP
  ||  This procedure will create sub lines for orderlines that have not been
  ||  fully allocated. It will call the Subs procedure to create all of the
  ||  possible subs for the order line.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/04/02 | rhalpai | Added logic to update Protected Inventory Log for partials.
  || 05/13/03 | rhalpai | Added logic to handle Government Controlled (DEA) Items
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  ||                    | Removed status parm from calls to OP_GET_SUBS_SP and
  ||                    | OP_PROTECTED_INVENTORY_PK.UPD_LOG_FOR_PARTLS_SP.
  || 10/14/05 | rhalpai | Moved logic for creating partial subs to Create_Partial_SP
  ||                    | and logic for updating original order line after
  ||                    | conditional subbing to Upd_Orig_For_Partial_SP. This
  ||                    | was done to share logic with new Kit Allocation logic.
  ||                    | PIR2909
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Replaced LLRDate input parm with ReleaseTS.
  ||                    | Changed cursor to use MCLANE_LOAD_LABEL_RLSE for
  ||                    | matching load list and LLR date.
  || 03/01/10 | rhalpai | Added CigSw parm and used in cursor to select cigs or
  ||                    | non-cigs. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/05/11 | rhalpai | Added logic to adjust PickQty for Weekly Max Cust Item
  ||                    | and add Weekly Max Log entries for matching Cig/NonCig
  ||                    | allocated OrdLns. Added logic to apply Weekly Max Qtys
  ||                    | for matching Cig/NonCig OrdLns and revert and subs
  ||                    | where applying Weekly Max sets the order qty to zero.
  ||                    | PIR6235
  || 07/10/12 | rhalpai | Change call to OP_GET_SUBS_SP to remove unused parms.
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to OP_GET_SUBS_SP.
  || 04/20/22 | rhalpai | Change logic to use NotShipRsn '006' for dist with allw_partl_sw (ordp100a.pshipa) turned on. PIR21059
  ||----------------------------------------------------------------------------
  */
  PROCEDURE create_sub_lns_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_cig_sw  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_ALLOCATE_PK.CREATE_SUB_LNS_SP';
    lar_parm             logs.tar_parm;
    l_sub_found          VARCHAR2(3);
    l_sub_msg            VARCHAR2(100);
    l_gov_cntl_shp_pts   gov_cntl_cust_p640a.shp_pts%TYPE;
    l_gov_cntl_tot_pts   gov_cntl_cust_p640a.tot_pts%TYPE;
    l_gov_cntl_applied   VARCHAR2(1);
    l_not_shp_rsn        ordp120b.ntshpb%TYPE;

    CURSOR l_cur_ords(
      b_div_part   NUMBER,
      b_llr_dt     DATE,
      b_load_list  VARCHAR2,
      b_cig_sw     VARCHAR2
    ) IS
      SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln, b.pckqtb AS pck_qty, b.ordqtb AS ord_qty,
               b.orgqtb AS orig_ord_qty, b.ntshpb AS not_shp_rsn, b.subrcb AS sub_cd, a.dsorda AS ord_typ,
               DECODE(a.pshipa, '0', 'N', 'N', 'N', 'Y') AS allw_partl_sw
          FROM load_depart_op1f ld, ordp100a a, ordp120b b, sysp200c c
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND c.div_part = a.div_part
           AND c.acnoc = a.custa
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb = 'P'
           AND b.excptn_sw = 'N'
           AND b.subrcb = 0
           AND b.ordqtb > 0
           AND NVL(a.pshipa, 'Y') IN('Y', '1')
           AND (   (    b_cig_sw = 'Y'
                    AND b.sllumb IN('CII', 'CIR', 'CIC'))
                OR (    b_cig_sw = 'N'
                    AND b.sllumb NOT IN('CII', 'CIR', 'CIC'))
               )
      ORDER BY c.retgpc, a.custa, a.cpoa, ld.load_num, b.ordnob, b.lineb;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'CigSw', i_cig_sw);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Upd and Log Allocated Wkly Max Qtys');
    upd_and_log_wkly_maxs_sp(i_r_rlse, i_cig_sw);
    logs.dbg('Get Unallocated Orders Cursor');
    FOR l_r_ord IN l_cur_ords(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list, i_cig_sw) LOOP
      logs.dbg('Check for Gov Control Limitation');

      SELECT NVL(MAX('Y'), 'N'), NVL(MAX(a.shp_pts), 0), NVL(MAX(a.tot_pts), 0)
        INTO l_gov_cntl_applied, l_gov_cntl_shp_pts, l_gov_cntl_tot_pts
        FROM gov_cntl_log_p680a a
       WHERE a.div_part = i_r_rlse.div_part
         AND a.ord_num = l_r_ord.ord_num
         AND a.ord_ln = l_r_ord.ord_ln;

      logs.dbg('Check for Partial Shipment');

      -- << Partial Shipment >>
      IF (    l_r_ord.pck_qty > 0
          AND l_r_ord.ord_qty > l_r_ord.pck_qty
          AND l_r_ord.not_shp_rsn IS NULL
          AND l_r_ord.sub_cd = 0
         ) THEN
        logs.dbg('Create Partial Entry');
        create_partial_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln);

        -- << Partial Due to Gov Control Limitation >>
        IF (    l_gov_cntl_applied = 'Y'
            AND l_gov_cntl_shp_pts <> l_gov_cntl_tot_pts) THEN
          l_not_shp_rsn := 'ITMSTRST';
        ELSE
          l_not_shp_rsn :=(CASE
                             WHEN(    l_r_ord.ord_typ = 'D'
                                  AND l_r_ord.allw_partl_sw = 'Y') THEN '006'
                             ELSE 'INVOUT'
                           END);
          logs.dbg('Create Conditional Subs for Partial');
          op_get_subs_sp(i_r_rlse.div_part, 'CONSUB', l_r_ord.ord_num, l_r_ord.ord_ln, l_sub_msg, l_sub_found);
        END IF;   -- << Partial Due to Gov Control Limitation >>

        logs.dbg('Upd Original Entry for Out on Partial after subbing');
        -- The not-ship-reason will be overriden if a conditional sub
        -- is found and allocated. This happens during the allocation
        -- of the sub line (ALLOC_ORDS_SP).
        upd_orig_for_partial_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln, l_not_shp_rsn);
      ELSE
        -- No Inventory Available (Complete Out of Stock)
        IF l_gov_cntl_applied = 'N' THEN
          logs.dbg('Create Conditional Subs for Complete Out');
          op_get_subs_sp(i_r_rlse.div_part, 'CONSUB', l_r_ord.ord_num, l_r_ord.ord_ln, l_sub_msg, l_sub_found);
        END IF;   -- l_gov_cntl_applied = 'N'
      END IF;   -- << Partial Shipment >>

      logs.dbg('Commit Add of Sub for Orderline');
      COMMIT;
    END LOOP;
    logs.dbg('Apply Wkly Max Qtys');
    -- Will also Revert any Subs with new OrdQty of Zero
    apply_wkly_maxs_sp(i_r_rlse, i_cig_sw);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END create_sub_lns_sp;

  /*
  ||----------------------------------------------------------------------------
  || DVQ_VNDR_CMP_SP
  ||  Apply Qty Vendor Compliance.
  ||  This process will increase the DVQ (Default Vendor Compliance for Item Qty)
  ||  order quantity as needed to meet compliance and then allocate them.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/11 | rhalpai | Original for PIR10460
  || 01/10/13 | rhalpai | Change logic go handle EITHER/OR items by grouping on
  ||                    | the ParentItem field for a Profile and using the
  ||                    | first Item according to Priority within
  ||                    | VendorComplianceItem. PIR12091
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 12/29/14 | rhalpai | Add CigSw parm and logic to separate calls for Cig
  ||                    | and Non-Cig. Remove call to ALLOC_NONCIG_ORDS_SP.
  ||                    | IM-234795
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dvq_vndr_cmp_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_cig_sw  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.DVQ_VNDR_CMP_SP';
    lar_parm             logs.tar_parm;
    l_t_ord_nums         type_ntab;
    l_t_ord_lns          type_ntab;
    l_t_need_qtys        type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'CigSw', i_cig_sw);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Get DVQ Info');

    WITH vc AS
         (SELECT   c.cust_id, c.prof_id, c.cmp_qty, c.beg_dt, c.end_dt, MAX(a.ordnoa) AS ord_num
              FROM vndr_cmp_prof_op3l p, vndr_cmp_cust_op2l c, load_depart_op1f ld, stop_eta_op1g se, ordp100a a
             WHERE p.typ = 'DVQ'
               AND c.div_part = i_r_rlse.div_part
               AND c.prof_id = p.prof_id
               AND ld.div_part = i_r_rlse.div_part
               AND ld.llr_dt = i_r_rlse.llr_dt
               AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
               AND se.div_part = ld.div_part
               AND se.load_depart_sid = ld.load_depart_sid
               AND se.cust_id = c.cust_id
               AND TRUNC(se.eta_ts) BETWEEN c.beg_dt AND c.end_dt
               AND a.div_part = ld.div_part
               AND a.load_depart_sid = ld.load_depart_sid
               AND a.custa = se.cust_id
               AND a.excptn_sw = 'N'
               AND a.ipdtsa = 'DVQ'
               AND a.stata = 'P'
               AND EXISTS(SELECT 1
                            FROM ordp120b b, sawp505e e, vndr_cmp_item_op1l i
                           WHERE b.div_part = a.div_part
                             AND b.ordnob = a.ordnoa
                             AND e.iteme = b.itemnb
                             AND e.uome = b.sllumb
                             AND i.prof_id = p.prof_id
                             AND i.catlg_num = e.catite
                             AND b.excptn_sw = 'N'
                             AND b.ntshpb IS NULL)
          GROUP BY c.cust_id, c.prof_id, c.cmp_qty, c.beg_dt, c.end_dt)
    SELECT y.ord_num, b.lineb, y.need_qty
    BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns, l_t_need_qtys
      FROM (SELECT   x.ord_num, x.catlg_num, x.cmp_qty - SUM(x.ord_qty) AS need_qty
                FROM (SELECT vc.cmp_qty, vc.ord_num, NVL(b.pckqtb, 0) AS ord_qty,
                             FIRST_VALUE(vci.catlg_num) OVER(PARTITION BY vci.parnt_item ORDER BY vci.priorty) AS catlg_num
                        FROM vc, stop_eta_op1g se, ordp100a a, ordp120b b, sawp505e e, vndr_cmp_item_op1l vci
                       WHERE se.div_part = i_r_rlse.div_part
                         AND se.cust_id = vc.cust_id
                         AND TRUNC(se.eta_ts) BETWEEN vc.beg_dt AND vc.end_dt
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.excptn_sw = 'N'
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.subrcb < 999
                         AND b.statb IN('P', 'T', 'R', 'A')
                         AND b.excptn_sw = 'N'
                         AND (   (    i_cig_sw = 'Y'
                                  AND b.sllumb IN('CII', 'CIR', 'CIC'))
                              OR (    i_cig_sw = 'N'
                                  AND b.sllumb NOT IN('CII', 'CIR', 'CIC'))
                             )
                         AND e.iteme = b.itemnb
                         AND e.uome = b.sllumb
                         AND vci.prof_id = vc.prof_id
                         AND vci.catlg_num = e.catite
                      UNION ALL
                      SELECT h.cmp_qty, h.ord_num, h.ord_qty,
                             FIRST_VALUE(h.catlg_num) OVER(PARTITION BY h.parnt_item ORDER BY h.priorty) AS catlg_num
                        FROM (SELECT   vc.prof_id, vc.cust_id, vc.cmp_qty, vc.ord_num, vci.catlg_num, vci.parnt_item,
                                       vci.priorty, SUM(b.pckqtb) AS ord_qty
                                  FROM vc, ordp900a a, ordp920b b, vndr_cmp_item_op1l vci
                                 WHERE a.div_part = i_r_rlse.div_part
                                   AND a.excptn_sw = 'N'
                                   AND a.custa = vc.cust_id
                                   AND a.etadta BETWEEN vc.beg_dt - g_c_rensoft_seed_dt AND vc.end_dt
                                                                                            - g_c_rensoft_seed_dt
                                   AND b.div_part = a.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND b.subrcb < 999
                                   AND b.statb = 'A'
                                   AND b.excptn_sw = 'N'
                                   AND b.pckqtb > 0
                                   AND (   (    i_cig_sw = 'Y'
                                            AND b.sllumb IN('CII', 'CIR', 'CIC'))
                                        OR (    i_cig_sw = 'N'
                                            AND b.sllumb NOT IN('CII', 'CIR', 'CIC'))
                                       )
                                   AND vci.prof_id = vc.prof_id
                                   AND vci.catlg_num = b.orditb
                              GROUP BY vc.prof_id, vc.cust_id, vc.cmp_qty, vc.ord_num, vci.parnt_item, vci.priorty,
                                       vci.catlg_num) h) x
            GROUP BY x.cmp_qty, x.ord_num, x.catlg_num
              HAVING x.cmp_qty > SUM(x.ord_qty)) y,
           sawp505e e, ordp120b b
     WHERE e.catite = LPAD(y.catlg_num, 6, '0')
       AND b.div_part = i_r_rlse.div_part
       AND b.ordnob = y.ord_num
       AND b.excptn_sw = 'N'
       AND b.itemnb = e.iteme
       AND b.sllumb = e.uome;

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Upd OrdQty for DVQ OrdLn');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        UPDATE ordp120b b
           SET b.ordqtb = b.ordqtb + l_t_need_qtys(i)
         WHERE b.div_part = i_r_rlse.div_part
           AND b.ordnob = l_t_ord_nums(i)
           AND b.lineb = l_t_ord_lns(i);
    END IF;   -- l_t_ord_nums.COUNT > 0

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dvq_vndr_cmp_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_CIGS_CMS_INV_SP
  ||  Process Cig allocation when Cig Mgmt System is master of its inventory
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 05/20/10 | rhalpai | Added logic to update event log. Added logic to
  ||                    | insert SplitPick order lines when allocated from
  ||                    | multiple locations.PIR8377
  || 08/17/10 | rhalpai | Removed logic to extract and ftp Cig slot override
  ||                    | file containing defferences in inventory slot verses
  ||                    | pick slot when using CMS inventory. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 12/29/14 | rhalpai | Add logic to apply DVQ Vendor Compliance and allocate
  ||                    | the adjusted order lines (call to DVQ_VNDR_CMP_SP
  ||                    | followed by ALLOC_CIGS_SP). IM-234795
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_cigs_cms_inv_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.PRCS_CIGS_CMS_INV_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80)  := 'Initial';

    /*
    ||----------------------------------------------------------------------------
    || ALLOC_CIG_ORD_LN_SP
    ||  Allocate Cig Order Line using CMS inventory
    ||----------------------------------------------------------------------------
    ||             C H A N G E     L O G
    ||----------------------------------------------------------------------------
    || Date     | USERID  | Changes
    ||----------------------------------------------------------------------------
    || 03/01/10 | rhalpai | Original
    || 08/17/10 | rhalpai | Changed logic to pass inventory zone for allocated
    ||                    | cig to INS_PICK_TRANS_SP for logging pick transaction.
    ||                    | PIR0024
    || 08/29/11 | rhalpai | Change call to INS_PICK_TRANS_SP to replace TranLn
    ||                    | parm with StampSw and add parms for
    ||                    | CigSelCd,HandStampSw,StampTab. PIR7990
    || 11/22/21 | rhalpai | Add cust_tax_jrsdctn in call to INS_PICK_TRANS_SP. PIR21509
    ||----------------------------------------------------------------------------
    */
    PROCEDURE alloc_cig_ord_ln_sp(
      i_r_rlse         IN  g_cur_rlse%ROWTYPE,
      i_ord_num        IN  NUMBER,
      i_ord_ln         IN  NUMBER,
      i_ord_qty        IN  PLS_INTEGER,
      i_sub_cd         IN  NUMBER,
      i_item           IN  VARCHAR2,
      i_uom            IN  VARCHAR2,
      i_t_allocd_cigs  IN  tt_allocd_cigs
    ) IS
      l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ALLOCATE_PK.PRCS_CIGS_CMS_INV_SP.ALLOC_CIG_ORD_LN_SP';
      lar_parm             logs.tar_parm;
      l_r_allocd_cig       rt_allocd_cig;
      l_inv_aisl           VARCHAR2(2);
      l_inv_bin            VARCHAR2(3);
      l_inv_lvl            VARCHAR2(2);
      l_pick_aisl          VARCHAR2(2);
      l_pick_bin           VARCHAR2(3);
      l_pick_lvl           VARCHAR2(2);
      l_ord_stat           ordp120b.statb%TYPE;
    BEGIN
      logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
      logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
      logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
      logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
      logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
      logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
      logs.add_parm(lar_parm, 'Item', i_item);
      logs.add_parm(lar_parm, 'UOM', i_uom);
      logs.add_parm(lar_parm,
                    'AllocdCigsTab',
                    (CASE
                       WHEN i_t_allocd_cigs IS NULL THEN NULL
                       WHEN i_t_allocd_cigs.COUNT = 0 THEN 'Empty'
                       ELSE 'OrdLn:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).ord_ln
                            || '~AllocQty:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).alloc_qty
                            || '~InvZone:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).inv_zone
                            || '~InvSlot:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).inv_slot
                            || '~PickZone:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).pick_zone
                            || '~PickSlot:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).pick_slot
                            || '~CigSelCd:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).cig_sel_cd
                            || '~HandStampSw:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).hand_stamp_sw
                            || '~CustTaxJrsdctn:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).cust_tax_jrsdctn
                            || '~StampTab Count:'
                            || i_t_allocd_cigs(i_t_allocd_cigs.FIRST).t_stamps.COUNT
                     END
                    )
                   );

      IF (    i_t_allocd_cigs IS NOT NULL
          AND i_t_allocd_cigs.COUNT > 0) THEN
        logs.dbg('Process Allocated Cig Order Lines');
        FOR i IN i_t_allocd_cigs.FIRST .. i_t_allocd_cigs.LAST LOOP
          l_r_allocd_cig := i_t_allocd_cigs(i);
          l_pick_aisl := SUBSTR(l_r_allocd_cig.pick_slot, 1, 2);
          l_pick_bin := SUBSTR(l_r_allocd_cig.pick_slot, 3, 3);
          l_pick_lvl := SUBSTR(l_r_allocd_cig.pick_slot, 6, 2);
          l_inv_aisl := SUBSTR(l_r_allocd_cig.inv_slot, 1, 2);
          l_inv_bin := SUBSTR(l_r_allocd_cig.inv_slot, 3, 3);
          l_inv_lvl := SUBSTR(l_r_allocd_cig.inv_slot, 6, 2);

          IF MOD(l_r_allocd_cig.ord_ln, .1) BETWEEN .01 AND .09 THEN
            logs.dbg('Add SplitPick');
            ins_split_pick_sp(i_r_rlse, i_ord_num, l_r_allocd_cig.ord_ln, l_r_allocd_cig.alloc_qty);
            l_ord_stat := 'T';
          ELSE
            l_ord_stat :=(CASE
                            WHEN(    l_r_allocd_cig.alloc_qty < i_ord_qty
                                 AND MOD(l_r_allocd_cig.ord_ln, 1) = 0) THEN 'P'
                            ELSE 'T'
                          END
                         );
          END IF;   -- MOD(l_r_allocd_cig.ord_ln, .1) BETWEEN .01 AND .09

          logs.dbg('Allocate Order Line');
          alloc_ord_ln_sp(i_r_rlse, i_ord_num, l_r_allocd_cig.ord_ln, l_r_allocd_cig.alloc_qty, l_ord_stat);
          logs.dbg('Add Work Order entry to Tran Tbl for Pick Slot');
          ins_pick_trans_sp(i_r_rlse,
                            'N',
                            i_item,
                            i_uom,
                            l_pick_aisl,
                            l_pick_bin,
                            l_pick_lvl,
                            i_ord_num,
                            l_r_allocd_cig.ord_ln,
                            l_r_allocd_cig.alloc_qty,
                            'ALLOC_CIG_ORD_LN_SP',
                            l_r_allocd_cig.pick_zone,
                            l_r_allocd_cig.inv_zone,
                            l_inv_aisl,
                            l_inv_bin,
                            l_inv_lvl,
                            l_r_allocd_cig.cig_sel_cd,
                            l_r_allocd_cig.hand_stamp_sw,
                            l_r_allocd_cig.cust_tax_jrsdctn,
                            l_r_allocd_cig.t_stamps
                           );

          IF i_sub_cd BETWEEN 1 AND 997 THEN
            -- for subs there will only be one row in i_t_allocd_cigs
            logs.dbg('Allocate Orig OrdLn if Sub is allocated');
            alloc_orig_ord_ln_sp(i_r_rlse, i_ord_num, l_r_allocd_cig.ord_ln, l_r_allocd_cig.alloc_qty, i_sub_cd);
          END IF;   -- i_sub_cd BETWEEN 1 AND 997
        END LOOP;
      ELSE
        IF i_ord_ln - FLOOR(i_ord_ln) BETWEEN .01 AND .69 THEN
          logs.dbg('Remove Unallocated Conditional Sub Line');
          del_unalloc_con_sub_sp(i_r_rlse, i_ord_num, i_ord_ln);
        END IF;   -- i_ord_ln - FLOOR(i_ord_ln) BETWEEN .01 AND .69
      END IF;   -- i_t_allocd_cigs IS NOT NULL AND i_t_allocd_cigs.COUNT > 0
    EXCEPTION
      WHEN OTHERS THEN
        logs.err(lar_parm);
    END alloc_cig_ord_ln_sp;

    /*
    ||----------------------------------------------------------------------------
    || ALLOC_CIGS_SP
    ||  Allocate Cig Order Lines using CMS inventory
    ||----------------------------------------------------------------------------
    ||             C H A N G E     L O G
    ||----------------------------------------------------------------------------
    || Date     | USERID  | Changes
    ||----------------------------------------------------------------------------
    || 03/01/10 | rhalpai | Original
    || 05/20/10 | rhalpai | Changed cursor to exclude order qty of zero. PIR8377
    || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
    || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
    || 04/09/13 | rhalpai | Add ProcessControl to prevent running CMS updates
    ||                    | against LoadClose. PIR11923
    || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
    || 10/14/17 | rhalpai | Change logic to use global variable containing nested
    ||                    | table of parm values. PIR15427
    ||----------------------------------------------------------------------------
    */
    PROCEDURE alloc_cigs_sp(
      i_r_rlse  IN  g_cur_rlse%ROWTYPE,
      i_sub_cd  IN  NUMBER DEFAULT NULL
    ) IS
      l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_ALLOCATE_PK.PRCS_CIGS_CMS_INV_SP.ALLOC_CIGS_SP';
      lar_parm             logs.tar_parm;
      l_r_cig_alloc        rt_cig_alloc;
      l_t_allocd_cigs      tt_allocd_cigs;
      l_cig_alloc_msg      VARCHAR2(400);

      CURSOR l_cur_ords(
        b_div_part        NUMBER,
        b_llr_dt          DATE,
        b_load_list       VARCHAR2,
        b_sub_cd          NUMBER,
        b_t_mstr_cs_crps  type_stab
      ) IS
        SELECT   o.ordnob AS ord_num, o.lineb AS ord_ln, o.cust_id, o.load_num, o.stop_num, o.orditb AS catlg_num,
                 o.itemnb AS item, o.sllumb AS uom,
                 DECODE(o.mstr_cs_cust_sw, 'Y', o.ord_qty - MOD(o.ord_qty, o.mstr_cs_qty), o.ord_qty) AS ord_qty,
                 o.allw_partl_sw, o.subrcb AS sub_cd
            FROM (SELECT b.ordnob, b.lineb, se.cust_id, ld.load_num, se.stop_num, b.orditb, b.itemnb, b.sllumb,
                         b.ordqtb - NVL(b.pckqtb, 0) AS ord_qty, DECODE(mc.custb, NULL, 'N', 'Y') AS mstr_cs_cust_sw,
                         e.mulsle AS mstr_cs_qty, DECODE(a.pshipa, '0', 'N', 'N', 'N', 'Y') AS allw_partl_sw, b.subrcb,
                         t.taxjrc AS cust_jrsdctn, c.retgpc AS grp_id, a.dsorda, ld.depart_ts, a.cpoa
                    FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, mclp030c t, sysp200c c,
                         mclp110b di, sawp505e e,
                         (SELECT cx.custb
                            FROM TABLE(CAST(b_t_mstr_cs_crps AS type_stab)) crp, mclp020b cx
                           WHERE cx.div_part = b_div_part
                             AND cx.corpb = TO_NUMBER(crp.column_value)) mc
                   WHERE ld.div_part = b_div_part
                     AND ld.llr_dt = b_llr_dt
                     AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                     AND a.div_part = ld.div_part
                     AND a.load_depart_sid = ld.load_depart_sid
                     AND a.excptn_sw = 'N'
                     AND se.div_part = a.div_part
                     AND se.load_depart_sid = a.load_depart_sid
                     AND se.cust_id = a.custa
                     AND t.div_part = a.div_part
                     AND t.custc = a.custa
                     AND c.div_part = a.div_part
                     AND c.acnoc = a.custa
                     AND b.div_part = a.div_part
                     AND b.ordnob = a.ordnoa
                     AND b.subrcb BETWEEN NVL(b_sub_cd, 0) AND NVL(b_sub_cd, 998)
                     AND b.statb = 'P'
                     AND b.excptn_sw = 'N'
                     AND b.ordqtb > 0
                     AND b.ntshpb IS NULL
                     AND b.sllumb IN('CII', 'CIR', 'CIC')
                     AND di.div_part = b.div_part
                     AND di.itemb = b.itemnb
                     AND di.uomb = b.sllumb
                     AND e.iteme = b.itemnb
                     AND e.uome = b.sllumb
                     AND mc.custb(+) = a.custa) o
        ORDER BY o.subrcb, o.itemnb, o.sllumb, o.dsorda, o.depart_ts, o.stop_num, o.cpoa, o.ordnob, o.lineb DESC;

      TYPE l_tt_ords IS TABLE OF l_cur_ords%ROWTYPE;

      l_t_ords             l_tt_ords;
    BEGIN
      logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
      logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
      logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_alloc_cigs,
                                                  op_process_control_pk.g_c_active,
                                                  i_r_rlse.user_id,
                                                  i_r_rlse.div_part
                                                 );
      l_r_cig_alloc.div := i_r_rlse.div_id;
      l_r_cig_alloc.rlse_ts := i_r_rlse.rlse_ts;
      logs.dbg('Open Order Cursor');

      OPEN l_cur_ords(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list, i_sub_cd, g_t_mstr_cs_crps);

      <<cur_loop>>
      LOOP
        logs.dbg('Fetch Order Cursor');

        FETCH l_cur_ords
        BULK COLLECT INTO l_t_ords LIMIT 100;

        EXIT WHEN l_t_ords.COUNT = 0;
        <<tbl_loop>>
        FOR i IN l_t_ords.FIRST .. l_t_ords.LAST LOOP
          l_r_cig_alloc.ord_num := l_t_ords(i).ord_num;
          l_r_cig_alloc.ord_ln := l_t_ords(i).ord_ln;
          l_r_cig_alloc.cust_id := l_t_ords(i).cust_id;
          l_r_cig_alloc.load_num := l_t_ords(i).load_num;
          l_r_cig_alloc.stop_num := l_t_ords(i).stop_num;
          l_r_cig_alloc.catlg_num := l_t_ords(i).catlg_num;
          l_r_cig_alloc.ord_qty := l_t_ords(i).ord_qty;
          l_r_cig_alloc.allw_partl_sw := l_t_ords(i).allw_partl_sw;
          logs.dbg('CMS Allocation for Order Line');
          cig_op_allocate_maint_pk.ALLOCATE(l_r_cig_alloc,
                                            l_t_allocd_cigs,
                                            i_r_rlse.evnt_que_id,
                                            i_r_rlse.cycl_id,
                                            i_r_rlse.cycl_dfn_id,
                                            l_cig_alloc_msg
                                           );
          logs.dbg('Allocate Cig Order Line');
          alloc_cig_ord_ln_sp(i_r_rlse,
                              l_t_ords(i).ord_num,
                              l_t_ords(i).ord_ln,
                              l_t_ords(i).ord_qty,
                              l_t_ords(i).sub_cd,
                              l_t_ords(i).item,
                              l_t_ords(i).uom,
                              l_t_allocd_cigs
                             );
        END LOOP tbl_loop;
      END LOOP cur_loop;
      logs.dbg('Close Order Cursor');

      CLOSE l_cur_ords;

      COMMIT;   -- Final
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_alloc_cigs,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_r_rlse.user_id,
                                                  i_r_rlse.div_part
                                                 );
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;

        IF l_cur_ords%ISOPEN THEN
          CLOSE l_cur_ords;
        END IF;

        logs.err(lar_parm);
    END alloc_cigs_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'EvntQueId', i_r_rlse.evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_r_rlse.cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_r_rlse.cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    l_section := 'Initialize CMS Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_init);
    cig_op_allocate_maint_pk.init(i_r_rlse.div_id,
                                  i_r_rlse.rlse_ts,
                                  i_r_rlse.llr_dt,
                                  (CASE i_r_rlse.test_bil_cd
                                     WHEN '~' THEN 'N'
                                     ELSE 'Y'
                                   END),
                                  i_r_rlse.forc_inv_sw,
                                  i_r_rlse.user_id,
                                  i_r_rlse.evnt_que_id,
                                  i_r_rlse.cycl_id,
                                  i_r_rlse.cycl_dfn_id
                                 );
    l_section := 'Order-Level Allocation for All';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_ord_alloc);
    alloc_cigs_sp(i_r_rlse);
    l_section := 'Remove Unallocated Sub Lines';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_del_sub);
    del_sub_lns_sp(i_r_rlse, 'Y');
    l_section := 'Order-Level Allocation for Orig Ord Lns of Deleted Subs';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_org_alloc);
    alloc_cigs_sp(i_r_rlse, 998);
    l_section := 'Reset Tagged (998) Orig Sub Codes';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_reset_org);
    reset_tagged_orig_sub_cds_sp(i_r_rlse, 'Y');
    l_section := 'Create Conditional Subs';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_create_sub);
    create_sub_lns_sp(i_r_rlse, 'Y');
    l_section := 'Order-Level Allocation for Conditional Subs';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_sub_alloc);
    alloc_cigs_sp(i_r_rlse);
    l_section := 'Apply Cig Vendor Compliance';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_vndrcmp);
    dvq_vndr_cmp_sp(i_r_rlse, 'Y');
    l_section := 'Allocate Cig Vendor Compliance OrdLns';
    logs.dbg(l_section);
    alloc_cigs_sp(i_r_rlse, 0);
    l_section := 'Final Call Notification for CMS Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cig_final);
    cig_op_allocate_maint_pk.finalize(i_r_rlse.evnt_que_id, i_r_rlse.cycl_id, i_r_rlse.cycl_dfn_id);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prcs_cigs_cms_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_CIG_ALLOC_STAT_SP
  ||  Set Cig Allocation status which is used to indicate when Cig allocation
  ||  has completed and whether it was successful.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_cig_alloc_stat_sp(
    i_r_rlse    IN  g_cur_rlse%ROWTYPE,
    i_new_stat  IN  VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_c_sysdate  CONSTANT DATE := SYSDATE;
  BEGIN
    MERGE INTO appl_sys_parm_ap1s p
         USING (SELECT 'Y' AS val
                  FROM DUAL) x
            ON (    p.div_part = i_r_rlse.div_part
                AND p.appl_id = 'OP'
                AND p.parm_id = 'CIG_ALLOC_STAT'
                AND x.val = 'Y')
      WHEN MATCHED THEN
        UPDATE
           SET p.vchar_val = i_new_stat, p.dt_val = i_r_rlse.rlse_ts, p.user_id = 'ALLOCATE',
               p.last_chg_ts = l_c_sysdate
      WHEN NOT MATCHED THEN
        INSERT(appl_id, parm_id, parm_typ, col_typ, intgr_val, vchar_val, dec_val, dt_val, user_id, div_part,
               last_chg_ts)
        VALUES('OP', 'CIG_ALLOC_STAT', 'DFT', 'VCHR', 0, i_new_stat, 0.0, i_r_rlse.rlse_ts, 'ALLOCATE',
               i_r_rlse.div_part, l_c_sysdate);
    COMMIT;
  END upd_cig_alloc_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || START_CIG_ALLOC_SP
  ||  Starts Cig allocation in a separate thread to allow non-Cig allocation to
  ||  process at the same time
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 05/20/10 | rhalpai | Added userid parm and passed in call to new Control-M
  ||                    | script: /local/prodcode/bin/XXOPCigAlloc.sub
  ||                    | PIR8377
  || 05/13/13 | rhalpai | Change logic to call xxopCigAlloc.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  || 07/01/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE start_cig_alloc_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.START_CIG_ALLOC_SP';
    lar_parm             logs.tar_parm;
    l_sid                VARCHAR2(10);
    l_cmd                typ.t_maxvc2;
    l_appl_srvr          VARCHAR2(20);
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'UserId', i_r_rlse.user_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Initialize');
    l_sid := SYS_CONTEXT('USERENV', 'DB_NAME');
    l_appl_srvr := op_parms_pk.val_fn(i_r_rlse.div_part, op_const_pk.prm_appl_srvr);
    l_cmd := '/local/prodcode/bin/xxopCigAlloc.sub "'
             || i_r_rlse.div_id
             || '" "'
             || i_r_rlse.rlse_ts_char
             || '" "'
             || i_r_rlse.user_id
             || '" "'
             || l_sid
             || '"';
    logs.dbg('Run Control-M Sub Script in Background' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END start_cig_alloc_sp;

  /*
  ||----------------------------------------------------------------------------
  || WAIT_FOR_CIG_ALLOC_SP
  ||  Waits for Cig Allocation to complete, checks status and raises error if
  ||  it indicates failure
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE wait_for_cig_alloc_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_stat                  appl_sys_parm_ap1s.vchar_val%TYPE;
    l_is_first_wait         BOOLEAN                             := TRUE;
    l_c_wait_secs  CONSTANT NUMBER                              := 10;
  BEGIN
    env.tag();
    LOOP
      l_stat := op_parms_pk.val_fn(i_r_rlse.div_part, op_const_pk.prm_cig_alloc_stat);
      EXIT WHEN l_stat IN(g_c_cig_alloc_stat_compl, g_c_cig_alloc_stat_fail);

      IF l_is_first_wait THEN
        l_is_first_wait := FALSE;
        log_prcs_step_sp(i_r_rlse, g_c_prcs_wait_cig);
      END IF;   -- l_is_first_wait

      DBMS_LOCK.sleep(l_c_wait_secs);
    END LOOP;
    excp.assert((l_stat = g_c_cig_alloc_stat_compl), 'Cig Allocation Failure');
    env.untag();
  END wait_for_cig_alloc_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOG_ITEM_RATION_SP
  ||  Log rationing of item
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_item_ration_sp(
    i_r_rlse      IN  g_cur_rlse%ROWTYPE,
    i_item        IN  VARCHAR2,
    i_uom         IN  VARCHAR2,
    l_qty_avail   IN  NUMBER,
    l_qty_demand  IN  NUMBER,
    l_cust_cnt    IN  NUMBER
  ) IS
  BEGIN
    INSERT INTO ration_item_log_rl1i
                (div_part, release_ts, item_num, qty_avail, qty_dmd, cust_cnt)
      SELECT i_r_rlse.div_part, i_r_rlse.rlse_ts, e.catite, l_qty_avail, l_qty_demand, l_cust_cnt
        FROM sawp505e e
       WHERE e.iteme = i_item
         AND e.uome = i_uom;
  END log_item_ration_sp;

  /*
  ||----------------------------------------------------------------------------
  || RATION_SP
  ||  Apply item rationing to spread the available quantity to as many
  ||  customers as possible.
  ||
  ||  Item Rationing is applied to all items for a division when its parm is
  ||  set to 'Y' with the following exceptions:
  ||    No X-Dock items - processed before item-level allocation
  ||    No kit items (i.e.: aggregate) - processed before item-level allocation
  ||    No conditional subs for item - processed after item-level allocation
  ||      (unconditional subs are included)
  ||    No distribution orders for item on billing pass
  ||    No single-item (no-inventory items)
  ||    No cigs (tax jurisdiction is NULL)
  ||    No inventory protection
  ||    No government control
  ||  Item Rationing is an attempt to assure each customer gets at least
  ||  one unit and distribute any remaining inventory for each customer
  ||  using a percentage of total ordered.
  ||  Rationing is applied in the same sequence as normal allocation.
  ||  If there is not enough available inventory for all customers to get
  ||  at least one unit then each customer in alloation sequence will get
  ||  one unit until there is none left.
  ||  If there is more available inventory after all customers have at least
  ||  one unit then the remaining available inventory is calculated as
  ||  follows:
  ||    RationPct = (AvailInv - CustCnt) / (TotOrdQty - CustCnt)
  ||    RationQty = CEIL(TotCustOrdQty * RationPct)  (CEIL = rounded up)
  ||  RationQty is rounded up to the next unit to assure all available
  ||  inventory is utilized.  If there is less available inventory than the
  ||  calculated RationQty then the remaining inventory is used.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Moved logic from GET_TAGGED_ORDS_FOR_ITEM_FN. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ration_sp(
    i_r_rlse       IN      g_cur_rlse%ROWTYPE,
    i_item         IN      VARCHAR2,
    i_uom          IN      VARCHAR2,
    i_qty_avail    IN      PLS_INTEGER,
    i_ttl_ord_qty  IN      PLS_INTEGER,
    i_cust_cnt     IN      PLS_INTEGER,
    i_ord_stat     IN      VARCHAR2,
    o_t_ords       OUT     tagged_ords_t
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_ALLOCATE_PK.RATION_SP';
    lar_parm             logs.tar_parm;
    l_adj_qty_avail      PLS_INTEGER;

    TYPE l_rt_cust IS RECORD(
      cust_num     sysp200c.acnoc%TYPE,
      ttl_ord_qty  PLS_INTEGER,
      seq          VARCHAR2(100)
    );

    TYPE l_tt_custs IS TABLE OF l_rt_cust;

    l_t_custs            l_tt_custs;

    TYPE l_tt_tagged_ords_v IS TABLE OF tagged_ord_t
      INDEX BY VARCHAR2(20);

    l_t_ords             l_tt_tagged_ords_v;
    l_idx                VARCHAR2(20);

    PROCEDURE one_per_cust_sp IS
      l_cust_idx   PLS_INTEGER;
      l_r_cust     l_rt_cust;
      l_ord_num    NUMBER;
      l_ord_ln     NUMBER;
      l_sub_cd     NUMBER;
      l_ord_qty    PLS_INTEGER;
      l_alloc_qty  PLS_INTEGER;
      l_idx        VARCHAR2(20);
    BEGIN
      l_alloc_qty := 1;
      l_cust_idx := l_t_custs.FIRST;
      LOOP
        EXIT WHEN(   l_adj_qty_avail = 0
                  OR l_cust_idx IS NULL);
        l_r_cust := l_t_custs(l_cust_idx);
        l_ord_num := TO_NUMBER(SUBSTR(l_r_cust.seq, 45, 11));
        l_ord_ln := 9999999.99 - TO_NUMBER(SUBSTR(l_r_cust.seq, 56, 10));
        l_sub_cd := TO_NUMBER(SUBSTR(l_r_cust.seq, 66, 2));
        l_ord_qty := TO_NUMBER(SUBSTR(l_r_cust.seq, 68));
        l_idx := LPAD(l_ord_num, 11, '0') || l_ord_ln;
        l_t_ords(l_idx) := tagged_ord_t(l_ord_num,
                                        l_ord_ln,
                                        TO_NUMBER(SUBSTR(l_r_cust.seq, 66, 2)),
                                        TO_NUMBER(SUBSTR(l_r_cust.seq, 68)),
                                        l_alloc_qty
                                       );
        l_adj_qty_avail := l_adj_qty_avail - l_alloc_qty;
        l_cust_idx := l_t_custs.NEXT(l_cust_idx);
      END LOOP;
    END one_per_cust_sp;

    PROCEDURE ration_ords_sp IS
      l_ration_pct  FLOAT;
      l_ration_qty  PLS_INTEGER;
      l_cust_idx    PLS_INTEGER;
      l_r_cust      l_rt_cust;
      l_idx         VARCHAR2(20);
      l_alloc_qty   PLS_INTEGER;

      CURSOR l_cur_ration_ords(
        b_div_part   NUMBER,
        b_llr_dt     DATE,
        b_load_list  VARCHAR2,
        b_item       VARCHAR2,
        b_uom        VARCHAR2,
        b_cust_id    VARCHAR2,
        b_ord_stat   VARCHAR2
      ) IS
        SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln, b.subrcb AS sub_cd, b.ordqtb AS ord_qty
            FROM load_depart_op1f ld, ordp100a a, ordp120b b, mclp110b di
           WHERE ld.div_part = b_div_part
             AND ld.llr_dt = b_llr_dt
             AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
             AND a.div_part = ld.div_part
             AND a.load_depart_sid = ld.load_depart_sid
             AND a.custa = b_cust_id
             AND a.excptn_sw = 'N'
             AND a.dsorda = 'R'
             AND b.div_part = a.div_part
             AND b.ordnob = a.ordnoa
             AND b.subrcb < 999
             AND b.statb = b_ord_stat
             AND b.excptn_sw = 'N'
             AND b.itemnb = b_item
             AND b.sllumb = b_uom
             AND di.div_part = b.div_part
             AND di.itemb = b.itemnb
             AND di.uomb = b.sllumb
             AND TRIM(di.suomb) IS NULL
        ORDER BY a.cpoa, b.ordnob, b.lineb DESC;

      l_r_ord       l_cur_ration_ords%ROWTYPE;
    BEGIN
      l_ration_pct := (i_qty_avail - i_cust_cnt) /(i_ttl_ord_qty - i_cust_cnt);
      l_cust_idx := l_t_custs.FIRST;
      <<ration_cust_loop>>
      LOOP
        EXIT ration_cust_loop WHEN(   l_adj_qty_avail = 0
                                   OR l_cust_idx IS NULL);
        l_r_cust := l_t_custs(l_cust_idx);
        l_ration_qty := LEAST(CEIL(l_r_cust.ttl_ord_qty * l_ration_pct), l_adj_qty_avail);
        logs.dbg('Open Cursor');

        OPEN l_cur_ration_ords(i_r_rlse.div_part,
                               i_r_rlse.llr_dt,
                               i_r_rlse.load_list,
                               i_item,
                               i_uom,
                               l_r_cust.cust_num,
                               i_ord_stat
                              );

        <<ration_ord_loop>>
        LOOP
          logs.dbg('Fetch Cursor');

          FETCH l_cur_ration_ords
           INTO l_r_ord;

          EXIT ration_ord_loop WHEN(   l_ration_qty = 0
                                    OR l_cur_ration_ords%NOTFOUND);
          l_idx := LPAD(l_r_ord.ord_num, 11, '0') || l_r_ord.ord_ln;

          IF l_t_ords.EXISTS(l_idx) THEN
            logs.dbg('Set AllocQty for Existing Entry');
            l_alloc_qty := LEAST(l_t_ords(l_idx).ord_qty - l_t_ords(l_idx).adj_ord_qty, l_ration_qty);
            l_t_ords(l_idx).adj_ord_qty := l_t_ords(l_idx).adj_ord_qty + l_alloc_qty;
          ELSE
            logs.dbg('Add Entry');
            l_alloc_qty := LEAST(l_r_ord.ord_qty, l_ration_qty);
            l_t_ords(l_idx) := tagged_ord_t(l_r_ord.ord_num,
                                            l_r_ord.ord_ln,
                                            l_r_ord.sub_cd,
                                            l_r_ord.ord_qty,
                                            l_alloc_qty
                                           );
          END IF;   -- l_t_ords.EXISTS(v_idx)

          l_ration_qty := l_ration_qty - l_alloc_qty;
          l_adj_qty_avail := l_adj_qty_avail - l_alloc_qty;
        END LOOP ration_ord_loop;
        logs.dbg('Close Cursor');

        CLOSE l_cur_ration_ords;

        l_cust_idx := l_t_custs.NEXT(l_cust_idx);
      END LOOP ration_cust_loop;
    EXCEPTION
      WHEN OTHERS THEN
        IF l_cur_ration_ords%ISOPEN THEN
          CLOSE l_cur_ration_ords;
        END IF;

        logs.err(lar_parm);
    END ration_ords_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'QtyAvail', i_qty_avail);
    logs.add_parm(lar_parm, 'TtlOrdQty', i_ttl_ord_qty);
    logs.add_parm(lar_parm, 'CustCnt', i_cust_cnt);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.dbg('ENTRY', lar_parm);

    IF i_qty_avail > 0 THEN
      env.tag();
      l_adj_qty_avail := i_qty_avail;

      SELECT   se.cust_id,
               SUM(b.ordqtb) AS ttl_ord_qty,
               MAX(TO_CHAR(ld.depart_ts, 'YYYYMMDDHH24MI')
                   || LPAD(se.stop_num, 2, '0')
                   || RPAD(NVL(a.cpoa, ' '), 30)
                   || LPAD(b.ordnob, 11, '0')
                   || TO_CHAR(9999999.99 - b.lineb, 'FM0000000.00')
                   || LPAD(b.subrcb, 2, '0')
                   || b.ordqtb
                  ) AS seq
      BULK COLLECT INTO l_t_custs
          FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, mclp110b di
         WHERE ld.div_part = i_r_rlse.div_part
           AND ld.llr_dt = i_r_rlse.llr_dt
           AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND a.dsorda = 'R'
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.subrcb < 999
           AND b.excptn_sw = 'N'
           AND b.statb = i_ord_stat
           AND b.itemnb = i_item
           AND b.sllumb = i_uom
           AND di.div_part = b.div_part
           AND di.itemb = b.itemnb
           AND di.uomb = b.sllumb
           AND TRIM(di.suomb) IS NULL
      GROUP BY se.cust_id
      ORDER BY seq;

      IF     l_t_custs IS NOT NULL
         AND l_t_custs.COUNT > 0 THEN
        logs.dbg('One Unit Per Cust');
        one_per_cust_sp;

        IF l_adj_qty_avail > 0 THEN
          logs.dbg('Ration Orders');
          ration_ords_sp;
        END IF;   -- l_adj_qty_avail > 0

        logs.dbg('Load Orders');
        l_idx := l_t_ords.FIRST;
        <<load_ords_loop>>
        WHILE l_idx IS NOT NULL LOOP
          IF l_idx = l_t_ords.FIRST THEN
            o_t_ords := tagged_ords_t();
          END IF;   -- l_idx = l_t_ords.FIRST

          o_t_ords.EXTEND;
          o_t_ords(o_t_ords.LAST) := l_t_ords(l_idx);
          l_idx := l_t_ords.NEXT(l_idx);
        END LOOP load_ords_loop;
      END IF;   -- l_t_cust IS NOT NULL AND l_t_cust.COUNT > 0

      env.untag();
    END IF;   -- i_qty_avail > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ration_sp;

  /*
  ||----------------------------------------------------------------------------
  || ORDS_FOR_ITEM_SP
  ||  Returns table of order lines and demand after rationing if applicable
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Moved logic from GET_TAGGED_ORDS_FOR_ITEM_FN. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ords_for_item_sp(
    i_r_rlse             IN      g_cur_rlse%ROWTYPE,
    i_item               IN      VARCHAR2,
    i_uom                IN      VARCHAR2,
    i_qty_avail          IN      PLS_INTEGER,
    i_ttl_ord_qty        IN      PLS_INTEGER,
    i_ttl_shp_compl_qty  IN      PLS_INTEGER,
    i_cust_cnt           IN      PLS_INTEGER,
    i_ration_items_sw    IN      VARCHAR2,
    i_ord_stat           IN      VARCHAR2,
    o_t_tagged_ords      OUT     tagged_ords_t
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ORDS_FOR_ITEM_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'QtyAvail', i_qty_avail);
    logs.add_parm(lar_parm, 'TtlOrdQty', i_ttl_ord_qty);
    logs.add_parm(lar_parm, 'TtlShpComplQty', i_ttl_shp_compl_qty);
    logs.add_parm(lar_parm, 'CustCnt', i_cust_cnt);
    logs.add_parm(lar_parm, 'RationItemsSW', i_ration_items_sw);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.dbg('ENTRY', lar_parm);
    o_t_tagged_ords := tagged_ords_t();

    IF i_qty_avail > 0 THEN
--      env.tag();

      IF i_qty_avail >= i_ttl_ord_qty THEN
        logs.dbg('Full Allocation Orders');

        SELECT tagged_ord_t(b.ordnob, b.lineb, b.subrcb, b.ordqtb, b.ordqtb)
        BULK COLLECT INTO o_t_tagged_ords
          FROM load_depart_op1f ld, ordp100a a, ordp120b b, mclp110b di
         WHERE ld.div_part = i_r_rlse.div_part
           AND ld.llr_dt = i_r_rlse.llr_dt
           AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND (   b.subrcb = 0
                OR (    i_ration_items_sw = 'Y'
                    AND b.subrcb < 999))
           AND b.excptn_sw = 'N'
           AND b.statb = i_ord_stat
           AND b.itemnb = i_item
           AND b.sllumb = i_uom
           AND di.div_part = b.div_part
           AND di.itemb = b.itemnb
           AND di.uomb = b.sllumb
           AND TRIM(di.suomb) IS NULL;
      ELSIF     i_ration_items_sw = 'Y'
            AND i_ttl_shp_compl_qty = 0 THEN
        logs.dbg('Ration Allocation Orders');
        ration_sp(i_r_rlse, i_item, i_uom, i_qty_avail, i_ttl_ord_qty, i_cust_cnt, i_ord_stat, o_t_tagged_ords);
      END IF;   -- i_qty_avail >= i_ttl_ord_qty

--      env.untag();
    END IF;   -- i_qty_avail > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ords_for_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_ORDS_FOR_ITEM_SP
  ||  Gets order lines and demand for item and if enough inventory exists then
  ||  the order lines are allocated.
  ||  Item Rationing may be applied when obtaining order lines and demand.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_ords_for_item_sp(
    i_r_rlse             IN      g_cur_rlse%ROWTYPE,
    i_r_inv              IN      g_rt_inv,
    i_ttl_ord_qty        IN      PLS_INTEGER,
    i_ttl_shp_compl_qty  IN      PLS_INTEGER,
    i_cust_cnt           IN      PLS_INTEGER,
    i_ration_items_sw    IN      VARCHAR2,
    o_alloc_qty          OUT     PLS_INTEGER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ALLOCATE_PK.ALLOC_ORDS_FOR_ITEM_SP';
    lar_parm             logs.tar_parm;
    l_t_tagged_ords      tagged_ords_t;
    l_ord_stat           ordp120b.statb%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_r_inv.item);
    logs.add_parm(lar_parm, 'UOM', i_r_inv.uom);
    logs.add_parm(lar_parm, 'Slot', i_r_inv.aisl || i_r_inv.bin || i_r_inv.lvl);
    logs.add_parm(lar_parm, 'QtyAvail', i_r_inv.qty_avail);
    logs.add_parm(lar_parm, 'TtlOrdQty', i_ttl_ord_qty);
    logs.add_parm(lar_parm, 'TtlShpComplQty', i_ttl_shp_compl_qty);
    logs.add_parm(lar_parm, 'CustCnt', i_cust_cnt);
    logs.add_parm(lar_parm, 'RationItemsSW', i_ration_items_sw);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    o_alloc_qty := 0;
    logs.dbg('Get Tagged Orders for Item');
    ords_for_item_sp(i_r_rlse,
                     i_r_inv.item,
                     i_r_inv.uom,
                     i_r_inv.qty_avail,
                     i_ttl_ord_qty,
                     i_ttl_shp_compl_qty,
                     i_cust_cnt,
                     i_ration_items_sw,
                     'P',
                     l_t_tagged_ords
                    );

    IF l_t_tagged_ords.COUNT > 0 THEN
      FOR i IN l_t_tagged_ords.FIRST .. l_t_tagged_ords.LAST LOOP
        IF l_t_tagged_ords(i).adj_ord_qty > 0 THEN
          l_ord_stat :=(CASE
                          WHEN(    l_t_tagged_ords(i).adj_ord_qty < l_t_tagged_ords(i).ord_qty
                               AND l_t_tagged_ords(i).line_num - FLOOR(l_t_tagged_ords(i).line_num) BETWEEN .01 AND .69
                              ) THEN 'P'
                          ELSE 'T'
                        END
                       );
          logs.dbg('Allocate Orders');
          alloc_ords_sp(i_r_rlse,
                        i_r_inv.item,
                        i_r_inv.uom,
                        i_r_inv.aisl,
                        i_r_inv.bin,
                        i_r_inv.lvl,
                        l_t_tagged_ords(i).order_num,
                        l_t_tagged_ords(i).line_num,
                        l_t_tagged_ords(i).adj_ord_qty,
                        l_ord_stat,
                        l_t_tagged_ords(i).sub_code,
                        'ALLOC_ORDS_FOR_ITEM_SP'
                       );
          o_alloc_qty := o_alloc_qty + l_t_tagged_ords(i).adj_ord_qty;
        END IF;   -- l_t_tagged_ords(i).adj_ord_qty > 0
      END LOOP;
    END IF;   -- l_t_tagged_ords.COUNT > 0

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END alloc_ords_for_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_NONCIG_ITEM_SP
  ||  Allocates non-cig item when available inventory meets demand.
  ||  Applies FC to SSEL cutdowns as necessary to meet demand.
  ||  Logs Item Rationing when applied.
  ||  Allocates order lines for item when enough inventory exists or Item
  ||  Rationing is applied.
  ||  Adjust inventory quantity for items that are allocated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_noncig_item_sp(
    i_r_rlse             IN  g_cur_rlse%ROWTYPE,
    i_item               IN  VARCHAR2,
    i_uom                IN  VARCHAR2,
    i_ttl_ord_qty        IN  PLS_INTEGER,
    i_ttl_shp_compl_qty  IN  PLS_INTEGER,
    i_cust_cnt           IN  PLS_INTEGER,
    i_ration_items_sw    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_NONCIG_ITEM_SP';
    lar_parm             logs.tar_parm;
    l_r_inv              g_rt_inv;
    l_max_qty            PLS_INTEGER;
    l_min_qty            PLS_INTEGER;
    l_trnsfr_qty         NUMBER(11)    := 0;
    l_alloc_qty          PLS_INTEGER   := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'TtlOrdQty', i_ttl_ord_qty);
    logs.add_parm(lar_parm, 'TtlShpComplQty', i_ttl_shp_compl_qty);
    logs.add_parm(lar_parm, 'CustCnt', i_cust_cnt);
    logs.add_parm(lar_parm, 'RationItemsSW', i_ration_items_sw);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    logs.dbg('Get Inv and Lock Item');
    l_r_inv := get_inv_fn(i_r_rlse, i_item, i_uom);

    IF l_r_inv.row_id IS NOT NULL THEN
      IF l_r_inv.qty_avail < i_ttl_ord_qty THEN
        l_max_qty := i_ttl_ord_qty - l_r_inv.qty_avail;
        -- Min qty should be at least one but should include enough qty to
        -- meet the ship-complete requirements when added to qty available.
        l_min_qty :=(CASE
                       WHEN l_r_inv.qty_avail > i_ttl_shp_compl_qty THEN 1
                       ELSE i_ttl_shp_compl_qty - l_r_inv.qty_avail
                     END);
        logs.dbg('FC to SSEL Cutdowns');
        cutdowns_sp(i_r_rlse, l_r_inv, l_max_qty, l_min_qty, l_trnsfr_qty);

        IF l_trnsfr_qty > 0 THEN
          l_r_inv.qty_avail := l_r_inv.qty_avail + l_trnsfr_qty;
        END IF;   -- l_trnsfr_qty > 0
      END IF;   -- l_r_inv.qty_avail < i_ttl_ord_qty

      IF l_r_inv.qty_avail > 0 THEN
        IF (    i_ration_items_sw = 'Y'
            AND i_ttl_shp_compl_qty = 0
            AND l_r_inv.qty_avail < i_ttl_ord_qty) THEN
          logs.dbg('Log Ration Pct');
          log_item_ration_sp(i_r_rlse, l_r_inv.item, l_r_inv.uom, l_r_inv.qty_avail, i_ttl_ord_qty, i_cust_cnt);
        END IF;   -- i_ration_items_sw = 'Y' AND i_ttl_shp_compl_qty = 0 AND l_r_inv.qty_avail < i_ttl_ord_qty

        logs.dbg('Allocate Orders');
        alloc_ords_for_item_sp(i_r_rlse,
                               l_r_inv,
                               i_ttl_ord_qty,
                               i_ttl_shp_compl_qty,
                               i_cust_cnt,
                               i_ration_items_sw,
                               l_alloc_qty
                              );

        IF l_alloc_qty > 0 THEN
          logs.dbg('Upd Inventory Qty');
          upd_inv_qty_sp(l_r_inv.row_id, l_alloc_qty);
        END IF;   -- l_alloc_qty > 0
      END IF;   -- l_r_inv.qty_avail > 0
    END IF;   -- l_r_inv.row_id IS NOT NULL

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END alloc_noncig_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_NONCIG_ITEMS_SP
  ||  Process cursor of non-cig items in release for allocation when available
  ||  inventory meets demand.
  ||  This process is created for Efficiency. We can allocate a high percentage
  ||  of the orders at the Item level instead of by order line.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/12/12 | rhalpai | Change logic to indicate item rationing when item
  ||                    | exists in ITEM_GRP_OP2E for type RATION. PIR10298
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Remove call to get parm as it is now referenced in a
  ||                    | global variable and loaded earlier.
  ||                    | Change logic to use global variable containing nested
  ||                    | table of parm values. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_noncig_items_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_NONCIG_ITEMS_SP';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_items(
      b_div              VARCHAR2,
      b_div_part         NUMBER,
      b_llr_dt           DATE,
      b_load_list        VARCHAR2,
      b_rlse_ts          DATE,
      b_ration_items_sw  VARCHAR2,
      b_t_mstr_cs_crps   type_stab
    ) IS
      SELECT   di.itemb AS item, di.uomb AS uom,
               (CASE
                  WHEN SUM(DECODE(a.pshipa, '0', 1, 'N', 1, 0)) > 0 THEN 'N'
                  WHEN b_ration_items_sw = 'Y' THEN 'Y'
                  WHEN EXISTS(SELECT 1
                                FROM item_grp_op2e ig
                               WHERE ig.div_part = b_div_part
                                 AND ig.cls_typ = 'RATION'
                                 AND ig.catlg_num = b.orditb) THEN 'Y'
                  ELSE 'N'
                END
               ) AS ration_sw,
               DECODE(SUM(b.subrcb), 0, 'N', 'Y') AS sub_sw, SUM(b.ordqtb) AS ord_qty,
               SUM(DECODE(a.pshipa, '0', b.ordqtb, 'N', b.ordqtb, 0)) AS shp_compl_qty,
               COUNT(DISTINCT a.custa) AS cust_cnt
          FROM load_depart_op1f ld, ordp100a a, ordp120b b, mclp110b di, whsp300c w, sawp505e e
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.div_part = ld.div_part
           AND a.stata = 'P'
           AND a.excptn_sw = 'N'
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb = 'P'
           AND b.excptn_sw = 'N'
           AND b.sllumb NOT IN('CII', 'CIR', 'CIC')
           AND b.subrcb < 999
           AND di.div_part = b.div_part
           AND di.itemb = b.itemnb
           AND di.uomb = b.sllumb
           AND TRIM(di.suomb) IS NULL
           AND w.div_part = di.div_part
           AND w.itemc = di.itemb
           AND w.uomc = di.uomb
           AND w.taxjrc IS NULL
           AND e.iteme = di.itemb
           AND e.uome = di.uomb
           AND e.catite NOT IN(SELECT e2.catite
                                 FROM TABLE(CAST(b_t_mstr_cs_crps AS type_stab)) crp, mclp020b cx, load_depart_op1f ld2,
                                      stop_eta_op1g se, ordp100a a2, ordp120b b2, sawp505e e2
                                WHERE cx.div_part = b_div_part
                                  AND cx.corpb = TO_NUMBER(crp.column_value)
                                  AND ld2.div_part = b_div_part
                                  AND ld2.llr_dt = b_llr_dt
                                  AND se.div_part = ld2.div_part
                                  AND se.load_depart_sid = ld2.load_depart_sid
                                  AND se.cust_id = cx.custb
                                  AND a2.div_part = se.div_part
                                  AND a2.load_depart_sid = se.load_depart_sid
                                  AND a2.custa = se.cust_id
                                  AND a2.excptn_sw = 'N'
                                  AND b2.div_part = a2.div_part
                                  AND b2.ordnob = a2.ordnoa
                                  AND b2.statb = 'P'
                                  AND b2.excptn_sw = 'N'
                                  AND e2.iteme = b2.itemnb
                                  AND e2.uome = b2.sllumb
                                  AND MOD(b2.ordqtb, e2.mulsle) > 0)
           AND e.catite NOT IN(SELECT i.ord_item_num
                                 FROM prtctd_inv_op1i i
                                WHERE i.div_part = b_div_part
                                  AND i.zone_id = b_div
                                  AND i.tax_jrsdctn IS NULL
                                  AND TRUNC(b_rlse_ts) BETWEEN i.eff_dt AND i.end_dt
                                  AND i.stat_cd = 'ACT')
           AND (e.iteme, e.uome) NOT IN(SELECT p660a.item_num, p660a.uom
                                          FROM gov_cntl_item_p660a p660a
                                         WHERE p660a.div_part = b_div_part)
      GROUP BY di.itemb, di.uomb, b.orditb
      ORDER BY di.itemb, di.uomb;

    TYPE l_tt_items IS TABLE OF l_cur_items%ROWTYPE;

    l_t_items            l_tt_items;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Open Items Cursor');

    OPEN l_cur_items(i_r_rlse.div_id,
                     i_r_rlse.div_part,
                     i_r_rlse.llr_dt,
                     i_r_rlse.load_list,
                     i_r_rlse.rlse_ts,
                     g_ration_items_sw,
                     g_t_mstr_cs_crps
                    );

    <<cur_loop>>
    LOOP
      logs.dbg('Fetch Items Cursor');

      FETCH l_cur_items
      BULK COLLECT INTO l_t_items LIMIT 100;

      EXIT WHEN l_t_items.COUNT = 0;
      <<tbl_loop>>
      FOR i IN l_t_items.FIRST .. l_t_items.LAST LOOP
        IF (   l_t_items(i).sub_sw = 'N'
            OR l_t_items(i).ration_sw = 'Y') THEN
          logs.dbg('Allocate NonCig Item');
          alloc_noncig_item_sp(i_r_rlse,
                               l_t_items(i).item,
                               l_t_items(i).uom,
                               l_t_items(i).ord_qty,
                               l_t_items(i).shp_compl_qty,
                               l_t_items(i).cust_cnt,
                               l_t_items(i).ration_sw
                              );
          COMMIT;
        END IF;   -- l_t_items(i).sub_sw = 'N' OR l_t_items(i).ration_sw = 'Y'
      END LOOP tbl_loop;
    END LOOP cur_loop;
    logs.dbg('Close Items Cursor');

    CLOSE l_cur_items;

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_cur_items%ISOPEN THEN
        CLOSE l_cur_items;
      END IF;

      logs.err(lar_parm);
  END alloc_noncig_items_sp;

  /*
  ||----------------------------------------------------------------------------
  || APPLY_GOV_CNTL_SP
  ||  Apply Government Control allocation quantity restrictions
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE apply_gov_cntl_sp(
    i_r_rlse               IN      g_cur_rlse%ROWTYPE,
    i_gov_cntl_id          IN      NUMBER,
    i_gov_cntl_prd         IN      NUMBER,
    i_gov_cntl_amt         IN      NUMBER,
    i_gov_cntl_pt          IN      NUMBER,
    i_cust_id              IN      VARCHAR2,
    io_alloc_qty           IN OUT  PLS_INTEGER,
    o_is_gov_cntl_item     OUT     BOOLEAN,
    o_gov_cntl_applied_sw  OUT     VARCHAR2,
    o_gov_cntl_stat        OUT     NUMBER,
    o_is_init_stat         OUT     BOOLEAN,
    o_is_exprd_prd         OUT     BOOLEAN,
    o_prd_beg_ts           OUT     DATE,
    o_new_prd_beg_ts       OUT     DATE,
    o_is_thrshld_prev_met  OUT     BOOLEAN
  ) IS
    l_cv                SYS_REFCURSOR;
    l_gov_cntl_shp_pts  gov_cntl_cust_p640a.shp_pts%TYPE;
    l_ord_qty_pts       gov_cntl_p600a.gov_cntl_amt%TYPE;
    l_avail_pts         gov_cntl_p600a.gov_cntl_amt%TYPE;
  BEGIN
    o_gov_cntl_applied_sw := 'N';
    o_is_gov_cntl_item :=(i_gov_cntl_id IS NOT NULL);

    IF o_is_gov_cntl_item THEN
--      env.tag();

      OPEN l_cv
       FOR
         SELECT     p640a.prd_beg_ts, p640a.shp_pts, p640a.status
               FROM gov_cntl_cust_p640a p640a
              WHERE p640a.div_part = i_r_rlse.div_part
                AND p640a.gov_cntl_id = i_gov_cntl_id
                AND p640a.cust_num = i_cust_id
                AND p640a.prd_beg_ts = (SELECT MAX(a.prd_beg_ts)
                                          FROM gov_cntl_cust_p640a a
                                         WHERE a.div_part = i_r_rlse.div_part
                                           AND a.gov_cntl_id = i_gov_cntl_id
                                           AND a.cust_num = i_cust_id)
         FOR UPDATE;

      FETCH l_cv
       INTO o_prd_beg_ts, l_gov_cntl_shp_pts, o_gov_cntl_stat;

      CLOSE l_cv;

      o_is_init_stat :=(o_gov_cntl_stat = 0);
      o_is_exprd_prd :=(o_prd_beg_ts <(i_r_rlse.rlse_ts - i_gov_cntl_prd));

      -- << Initial Status OR Expired Control Period >>
      IF (   o_is_init_stat
          OR o_is_exprd_prd) THEN
        o_new_prd_beg_ts := i_r_rlse.rlse_ts;
        l_gov_cntl_shp_pts := 0;
      ELSE
        o_new_prd_beg_ts := o_prd_beg_ts;
      END IF;   -- << Initial Status OR Expired Control Period >>

      -- << Check Gov Control Threshold Already Met >>
      o_is_thrshld_prev_met :=(l_gov_cntl_shp_pts >= i_gov_cntl_amt);

      IF o_is_thrshld_prev_met THEN
        o_gov_cntl_applied_sw := 'Y';
        io_alloc_qty := 0;
      ELSE
        l_ord_qty_pts := i_gov_cntl_pt * io_alloc_qty;
        l_avail_pts := i_gov_cntl_amt - l_gov_cntl_shp_pts;

        -- << Gov Control Threshold Exceeded by Order Qty Points >>
        IF (    l_ord_qty_pts > l_avail_pts
            AND i_gov_cntl_pt > 0) THEN
          o_gov_cntl_applied_sw := 'Y';
          io_alloc_qty := FLOOR(l_avail_pts / i_gov_cntl_pt);
        END IF;   -- l_ord_qty_pts > l_avail_pts AND i_gov_cntl_pt > 0
      END IF;   -- i_is_thrshld_prev_met

--      env.untag();
    END IF;   -- i_is_gov_cntl_item
  END apply_gov_cntl_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_CUTDOWNS_SP
  ||  Process FC to SSEL cutdowns as necessary to meet demand for SSEL item
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_cutdowns_sp(
    i_r_rlse               IN      g_cur_rlse%ROWTYPE,
    i_prtctd_cust_sw       IN      VARCHAR2,
    i_mstr_cs_cust_sw      IN      VARCHAR2,
    i_allw_partl_sw        IN      VARCHAR2,
    i_r_ss_inv             IN      g_rt_inv,
    io_item_adj_avail_qty  IN OUT  PLS_INTEGER,
    io_inv_qty_avail       IN OUT  PLS_INTEGER,
    io_alloc_qty           IN OUT  PLS_INTEGER
  ) IS
    l_min_qty     PLS_INTEGER;
    l_max_qty     PLS_INTEGER;
    l_trnsfr_qty  PLS_INTEGER;
  BEGIN
--    env.tag();
    l_max_qty := io_alloc_qty - io_item_adj_avail_qty;
    l_min_qty :=(CASE
                   WHEN i_allw_partl_sw = 'N' THEN io_alloc_qty - io_item_adj_avail_qty
                   WHEN i_mstr_cs_cust_sw = 'Y' THEN MOD((io_alloc_qty - io_item_adj_avail_qty), i_r_ss_inv.mstr_cs_qty)
                   ELSE 1
                 END
                );

    IF l_max_qty >= l_min_qty THEN
      cutdowns_sp(i_r_rlse, i_r_ss_inv, io_alloc_qty - io_item_adj_avail_qty, l_min_qty, l_trnsfr_qty,
                  i_prtctd_cust_sw);

      IF l_trnsfr_qty > 0 THEN
        io_inv_qty_avail := io_inv_qty_avail + l_trnsfr_qty;
        io_item_adj_avail_qty := io_item_adj_avail_qty + l_trnsfr_qty;

        IF io_item_adj_avail_qty < 0 THEN
          io_item_adj_avail_qty := 0;
        END IF;   -- io_item_adj_avail_qty < 0
      END IF;   -- l_trnsfr_qty > 0
    END IF;   -- l_max_qty >= l_min_qty

    IF io_alloc_qty > io_item_adj_avail_qty THEN
      io_alloc_qty := io_item_adj_avail_qty;
    END IF;   -- io_alloc_qty > io_item_adj_avail_qty

--    env.untag();
  END prcs_cutdowns_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_GOV_CNTL_SP
  ||  Applies updates for Government Control.
  ||  Adds log entry when Government Control is applied to order line.
  ||  Handles update for initial status, insert for Expired Control Period,
  ||  and point updates for Current Control Period.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_gov_cntl_sp(
    i_r_rlse               IN  g_cur_rlse%ROWTYPE,
    i_gov_cntl_id          IN  NUMBER,
    i_gov_cntl_pt          IN  NUMBER,
    i_cust_id              IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_ord_ln               IN  NUMBER,
    i_ord_qty              IN  PLS_INTEGER,
    i_alloc_qty            IN  PLS_INTEGER,
    i_gov_cntl_applied_sw  IN  VARCHAR2,
    i_gov_cntl_stat        IN  NUMBER,
    i_is_init_stat         IN  BOOLEAN,
    i_is_exprd_prd         IN  BOOLEAN,
    i_prd_beg_ts           IN  DATE,
    i_new_prd_beg_ts       IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.UPD_GOV_CNTL_SP';
    lar_parm             logs.tar_parm;
    l_current_shp_pts    NUMBER;
    l_current_tot_pts    NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'GovCntlId', i_gov_cntl_id);
    logs.add_parm(lar_parm, 'GovCntlPt', i_gov_cntl_pt);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.add_parm(lar_parm, 'AllocQty', i_alloc_qty);
    logs.add_parm(lar_parm, 'GovCntlAppliedSW', i_gov_cntl_applied_sw);
    logs.add_parm(lar_parm, 'GovCntlStat', i_gov_cntl_stat);
    logs.add_parm(lar_parm, 'IsInitStat', i_is_init_stat);
    logs.add_parm(lar_parm, 'IsExprdPrd', i_is_exprd_prd);
    logs.add_parm(lar_parm, 'PrdBegTS', i_prd_beg_ts);
    logs.add_parm(lar_parm, 'NewPrdBegTS', i_new_prd_beg_ts);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    -- Set Current Points
    l_current_shp_pts := i_gov_cntl_pt * i_alloc_qty;
    l_current_tot_pts :=(CASE i_gov_cntl_applied_sw
                           WHEN 'Y' THEN i_gov_cntl_pt * i_ord_qty
                           ELSE i_gov_cntl_pt * i_alloc_qty
                         END
                        );

    -- do not log outs unless gov control was applied
    IF (   i_is_init_stat
        OR l_current_tot_pts > 0) THEN
      logs.dbg('Add Gov Cntl Log Entry');

      INSERT INTO gov_cntl_log_p680a
                  (div_part, ord_num, ord_ln, gov_cntl_id, cust_num, prd_beg_ts,
                   release_ts, shp_pts, tot_pts, status
                  )
           VALUES (i_r_rlse.div_part, i_ord_num, i_ord_ln, i_gov_cntl_id, i_cust_id, i_new_prd_beg_ts,
                   i_r_rlse.rlse_ts, l_current_shp_pts, l_current_tot_pts, i_gov_cntl_stat
                  );
    END IF;   -- i_is_init_stat OR l_current_tot_pts > 0

    CASE
      WHEN i_is_init_stat THEN
        logs.dbg('Upd for Initial Status');

        UPDATE gov_cntl_cust_p640a a
           SET a.prd_beg_ts = i_r_rlse.rlse_ts,
               a.status = 1,
               a.shp_pts = l_current_shp_pts,
               a.tot_pts = l_current_tot_pts
         WHERE a.div_part = i_r_rlse.div_part
           AND a.gov_cntl_id = i_gov_cntl_id
           AND a.cust_num = i_cust_id
           AND a.prd_beg_ts = i_prd_beg_ts;
      WHEN i_is_exprd_prd THEN
        logs.dbg('Add for Expired Control Period');

        INSERT INTO gov_cntl_cust_p640a
                    (div_part, gov_cntl_id, cust_num, prd_beg_ts, shp_pts,
                     tot_pts, status
                    )
             VALUES (i_r_rlse.div_part, i_gov_cntl_id, i_cust_id, i_r_rlse.rlse_ts, l_current_shp_pts,
                     l_current_tot_pts, 1
                    );
      ELSE
        logs.dbg('Upd for Current Control Period');

        UPDATE gov_cntl_cust_p640a a
           SET a.shp_pts = a.shp_pts + l_current_shp_pts,
               a.tot_pts = a.tot_pts + l_current_tot_pts
         WHERE a.gov_cntl_id = i_gov_cntl_id
           AND a.div_part = i_r_rlse.div_part
           AND a.cust_num = i_cust_id
           AND a.prd_beg_ts = i_prd_beg_ts;
    END CASE;

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_gov_cntl_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_NONCIG_ORD_LN_SP
  ||  Reduce allocate qty as necessary for Protected Inventory.
  ||  Reduce allocate qty as necessary for Government Control.
  ||  Process FC to SSEL Cutdowns as necessary.
  ||  Update Tables for any Gov Controlled Item as necessary.
  ||  When qty to allocate:
  ||    Allocate order line
  ||    Update inventory qty
  ||    Log Protected Inventory when applicable
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_noncig_ord_ln_sp(
    i_r_rlse           IN             g_cur_rlse%ROWTYPE,
    i_ord_num          IN             NUMBER,
    i_ord_ln           IN             NUMBER,
    i_ord_qty          IN             PLS_INTEGER,
    i_sub_cd           IN             NUMBER,
    i_cust_id          IN             VARCHAR2,
    i_mstr_cs_cust_sw  IN             VARCHAR2,
    i_allw_partl_sw    IN             VARCHAR2,
    i_gov_cntl_id      IN             NUMBER,
    i_gov_cntl_amt     IN             NUMBER,
    i_gov_cntl_prd     IN             NUMBER,
    i_gov_cntl_pt      IN             NUMBER,
    io_r_inv           IN OUT NOCOPY  g_rt_inv
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                             := 'OP_ALLOCATE_PK.ALLOC_NONCIG_ORD_LN_SP';
    lar_parm               logs.tar_parm;
    l_r_prtctd_inv         op_protected_inventory_pk.g_rt_prtctd_inv;
    l_prtctd_cust_sw       VARCHAR2(1);
    l_alloc_qty            PLS_INTEGER                               := 0;
    l_is_gov_cntl_item     BOOLEAN;
    l_prd_beg_ts           DATE;
    l_new_prd_beg_ts       DATE;
    l_gov_cntl_stat        gov_cntl_cust_p640a.status%TYPE;
    l_gov_cntl_applied_sw  VARCHAR2(1);
    l_is_thrshld_prev_met  BOOLEAN;
    l_is_init_stat         BOOLEAN;
    l_is_exprd_prd         BOOLEAN;
    l_ord_stat             ordp120b.statb%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'MstrCsCustSW', i_mstr_cs_cust_sw);
    logs.add_parm(lar_parm, 'AllwPartlSW', i_allw_partl_sw);
    logs.add_parm(lar_parm, 'GovCntlId', i_gov_cntl_id);
    logs.add_parm(lar_parm, 'GovCntlAmt', i_gov_cntl_amt);
    logs.add_parm(lar_parm, 'GovCntlPrd', i_gov_cntl_prd);
    logs.add_parm(lar_parm, 'GovCntlPt', i_gov_cntl_pt);
    logs.add_parm(lar_parm, 'InvRowID', ROWIDTOCHAR(io_r_inv.row_id));
    logs.add_parm(lar_parm, 'Item', io_r_inv.item);
    logs.add_parm(lar_parm, 'UOM', io_r_inv.uom);
    logs.add_parm(lar_parm, 'Slot', io_r_inv.aisl || io_r_inv.bin || io_r_inv.lvl);
    logs.add_parm(lar_parm, 'QtyAvail', io_r_inv.qty_avail);
    logs.add_parm(lar_parm, 'MstrCsQty', io_r_inv.mstr_cs_qty);
    logs.dbg('ENTRY', lar_parm);
--    env.tag();
    logs.dbg('Get Protected Inventory Info');
    prtctd_inv_sp(i_r_rlse, i_ord_num, i_ord_ln, io_r_inv.qty_avail, l_r_prtctd_inv);
    l_prtctd_cust_sw :=(CASE
                          WHEN l_r_prtctd_inv.prtctd_id > 0 THEN 'Y'
                          ELSE 'N'
                        END);
    l_alloc_qty :=(CASE
                     WHEN i_mstr_cs_cust_sw = 'Y' THEN i_ord_qty - MOD(i_ord_qty, io_r_inv.mstr_cs_qty)
                     ELSE i_ord_qty
                   END);
    logs.dbg('Adjust Order Quantity for Gov Control Limit');
    apply_gov_cntl_sp(i_r_rlse,
                      i_gov_cntl_id,
                      i_gov_cntl_prd,
                      i_gov_cntl_amt,
                      i_gov_cntl_pt,
                      i_cust_id,
                      l_alloc_qty,
                      l_is_gov_cntl_item,
                      l_gov_cntl_applied_sw,
                      l_gov_cntl_stat,
                      l_is_init_stat,
                      l_is_exprd_prd,
                      l_prd_beg_ts,
                      l_new_prd_beg_ts,
                      l_is_thrshld_prev_met
                     );

    IF l_r_prtctd_inv.item_adj_avail_qty < l_alloc_qty THEN
      logs.dbg('Process FC to SSEL Cutdowns');
      prcs_cutdowns_sp(i_r_rlse,
                       l_prtctd_cust_sw,
                       i_mstr_cs_cust_sw,
                       i_allw_partl_sw,
                       io_r_inv,
                       l_r_prtctd_inv.item_adj_avail_qty,
                       io_r_inv.qty_avail,
                       l_alloc_qty
                      );
    END IF;   -- l_r_prtctd_inv.item_adj_avail_qty < l_alloc_qty

    IF l_alloc_qty <> i_ord_qty THEN
      IF i_allw_partl_sw = 'N' THEN
        l_alloc_qty := 0;
        -- Unfulfilled orders flagged as ship complete (distributions)
        -- should bypass Gov Control
        l_gov_cntl_applied_sw := 'N';
      ELSIF i_mstr_cs_cust_sw = 'Y' THEN
        l_alloc_qty := l_alloc_qty - MOD(l_alloc_qty, io_r_inv.mstr_cs_qty);
      END IF;   -- i_allw_partl_sw = 'N'
    END IF;   -- l_alloc_qty <> i_ord_qty

    IF (    l_is_gov_cntl_item
        AND (   l_alloc_qty > 0
             OR l_gov_cntl_applied_sw = 'Y')) THEN
      logs.dbg('Upd Tables for Gov Controlled Item');
      upd_gov_cntl_sp(i_r_rlse,
                      i_gov_cntl_id,
                      i_gov_cntl_pt,
                      i_cust_id,
                      i_ord_num,
                      i_ord_ln,
                      i_ord_qty,
                      l_alloc_qty,
                      l_gov_cntl_applied_sw,
                      l_gov_cntl_stat,
                      l_is_init_stat,
                      l_is_exprd_prd,
                      l_prd_beg_ts,
                      l_new_prd_beg_ts
                     );
    END IF;   -- l_is_gov_cntl_item AND (l_alloc_qty > 0 OR l_gov_cntl_applied_sw = 'Y')

    IF l_alloc_qty > 0 THEN
      l_ord_stat :=(CASE
                      WHEN(    l_alloc_qty < i_ord_qty
                           AND i_ord_ln - FLOOR(i_ord_ln) = 0) THEN 'P'
                      ELSE 'T'
                    END);
      logs.dbg('Allocate Orders');
      alloc_ords_sp(i_r_rlse,
                    io_r_inv.item,
                    io_r_inv.uom,
                    io_r_inv.aisl,
                    io_r_inv.bin,
                    io_r_inv.lvl,
                    i_ord_num,
                    i_ord_ln,
                    l_alloc_qty,
                    l_ord_stat,
                    i_sub_cd,
                    'ALLOC_NONCIG_ORD_LN_SP'
                   );
      logs.dbg('Upd Inventory Qty');
      upd_inv_qty_sp(io_r_inv.row_id, l_alloc_qty);

      IF (    l_r_prtctd_inv.prtctd_id > 0
          AND i_sub_cd <> op_protected_inventory_pk.g_c_conditional_sub) THEN
        logs.dbg('Log Protected Inventory');
        log_prtctd_inv_sp(i_r_rlse,
                          l_r_prtctd_inv.prtctd_id,
                          l_r_prtctd_inv.prtctd_cust_qty,
                          i_ord_num,
                          i_ord_ln,
                          l_alloc_qty
                         );
      END IF;   -- l_r_prtctd_inv.prtctd_id > 0 AND i_sub_cd <> op_protected_inventory_pk.g_c_conditional_sub

      io_r_inv.qty_avail := io_r_inv.qty_avail - l_alloc_qty;
    ELSE
      IF i_ord_ln - FLOOR(i_ord_ln) BETWEEN .01 AND .69 THEN
        logs.dbg('Remove Unallocated Conditional Sub Line');
        del_unalloc_con_sub_sp(i_r_rlse, i_ord_num, i_ord_ln);
      END IF;   -- i_ord_ln - FLOOR(i_ord_ln) BETWEEN .01 AND .69
    END IF;   -- l_alloc_qty > 0

--    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END alloc_noncig_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOC_NONCIG_ORDS_SP
  ||  Process cursor of orders for non-cig items in release for allocation.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 05/20/10 | rhalpai | Changed cursor to exclude order qty of zero. PIR8377
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change logic to use global variable containing nested
  ||                    | table of parm values. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE alloc_noncig_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_sub_cd  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOC_NONCIG_ORDS_SP';
    lar_parm             logs.tar_parm;
    l_cmpr_item          VARCHAR2(40);
    l_save_item          VARCHAR2(40)  := ' ';
    l_qty_avail          PLS_INTEGER;
    l_r_inv              g_rt_inv;
    l_r_inv_empty        g_rt_inv;

    CURSOR l_cur_ords(
      b_div_part        NUMBER,
      b_llr_dt          DATE,
      b_load_list       VARCHAR2,
      b_sub_cd          NUMBER,
      b_t_mstr_cs_crps  type_stab
    ) IS
      SELECT   o.wrk_item, o.wrk_uom, o.ordnob AS ord_num, o.lineb AS ord_ln, o.subrcb AS sub_cd, o.custa AS cust_id,
               o.allw_partl_sw, o.mstr_cs_cust_sw, o.ord_qty, gov.gov_cntl_id, gov.gov_cntl_amt, gov.gov_cntl_prd,
               gov.gov_cntl_pt
          FROM (SELECT DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb) AS wrk_item,
                       NVL(TRIM(di.suomb), di.uomb) AS wrk_uom, b.ordnob, b.lineb, b.subrcb, a.custa,
                       DECODE(a.pshipa, '0', 'N', 'N', 'N', 'Y') AS allw_partl_sw,
                       DECODE(mc.custb, NULL, 'N', 'Y') AS mstr_cs_cust_sw, b.ordqtb - NVL(b.pckqtb, 0) AS ord_qty,
                       a.dsorda, ld.depart_ts, se.stop_num, a.cpoa
                  FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, mclp110b di,
                       (SELECT cx.custb
                          FROM TABLE(CAST(b_t_mstr_cs_crps AS type_stab)) crp, mclp020b cx
                         WHERE cx.div_part = b_div_part
                           AND cx.corpb = TO_NUMBER(crp.column_value)) mc
                 WHERE ld.div_part = b_div_part
                   AND ld.llr_dt = b_llr_dt
                   AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                   AND a.div_part = ld.div_part
                   AND a.load_depart_sid = ld.load_depart_sid
                   AND a.excptn_sw = 'N'
                   AND se.div_part = a.div_part
                   AND se.load_depart_sid = a.load_depart_sid
                   AND se.cust_id = a.custa
                   AND b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.subrcb BETWEEN NVL(b_sub_cd, 0) AND NVL(b_sub_cd, 998)
                   AND b.ordqtb > 0
                   AND b.statb = 'P'
                   AND b.excptn_sw = 'N'
                   AND b.ntshpb IS NULL
                   AND di.div_part = b.div_part
                   AND di.itemb = b.itemnb
                   AND di.uomb = b.sllumb
                   AND EXISTS(SELECT 1
                                FROM whsp300c w1
                               WHERE w1.div_part = di.div_part
                                 AND w1.itemc = DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb)
                                 AND w1.uomc = NVL(TRIM(di.suomb), di.uomb)
                                 AND w1.taxjrc IS NULL)
                   AND mc.custb(+) = a.custa) o,
               (SELECT x.cust_num, x.item_num, x.uom, x.gov_cntl_id, x.gov_cntl_amt, x.gov_cntl_prd, x.gov_cntl_pt
                  FROM (SELECT c.cust_num, i.item_num, i.uom, g.gov_cntl_id, g.gov_cntl_amt, g.gov_cntl_prd,
                               i.gov_cntl_pt, g.gov_cntl_hier_seq,
                               MAX(g.gov_cntl_hier_seq) OVER(PARTITION BY c.cust_num, i.item_num, i.uom) AS seq
                          FROM gov_cntl_p600a g, gov_cntl_cust_p640a c, gov_cntl_item_p660a i
                         WHERE g.div_part = b_div_part
                           AND g.gov_cntl_id = c.gov_cntl_id
                           AND i.div_part = g.div_part
                           AND c.div_part = i.div_part
                           AND c.gov_cntl_id = i.gov_cntl_id
                           AND c.prd_beg_ts = (SELECT MAX(c2.prd_beg_ts)
                                                 FROM gov_cntl_cust_p640a c2
                                                WHERE c2.div_part = c.div_part
                                                  AND c2.gov_cntl_id = c.gov_cntl_id
                                                  AND c2.cust_num = c.cust_num
                                                  AND c2.status < 2)) x
                 WHERE x.gov_cntl_hier_seq = x.seq) gov
         WHERE gov.cust_num(+) = o.custa
           AND gov.item_num(+) = o.wrk_item
           AND gov.uom(+) = o.wrk_uom
      ORDER BY o.subrcb, o.wrk_item, o.wrk_uom, o.dsorda, o.depart_ts, o.stop_num, o.cpoa, o.ordnob, o.lineb DESC;

    TYPE l_tt_ords IS TABLE OF l_cur_ords%ROWTYPE;

    l_t_ords             l_tt_ords;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'SubCd', i_sub_cd);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Open Order Cursor');

    OPEN l_cur_ords(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list, i_sub_cd, g_t_mstr_cs_crps);

    <<cur_loop>>
    LOOP
      logs.dbg('Fetch Order Cursor');

      FETCH l_cur_ords
      BULK COLLECT INTO l_t_ords LIMIT 100;

      EXIT WHEN l_t_ords.COUNT = 0;
      <<tbl_loop>>
      FOR i IN l_t_ords.FIRST .. l_t_ords.LAST LOOP
        -- Look up available inventory for each new item and lock item and stamp.
        l_cmpr_item := l_t_ords(i).wrk_item || l_t_ords(i).wrk_uom;

        -------------------------------------------------------------------------
        -- Note...
        -- Since this lookup picks the zone with most available inventory and
        -- multiple zones are possible we need to perform this task again once
        -- the adjusted value stored in l_qty_avail falls below the order qty.
        -------------------------------------------------------------------------
        IF (   l_cmpr_item <> l_save_item
            OR l_qty_avail < l_t_ords(i).ord_qty) THEN
          -- commit for previous item
          COMMIT;
          -- empty records for new item
          l_r_inv := l_r_inv_empty;
          logs.dbg('Get Inv and Lock Item');
          l_r_inv := get_inv_fn(i_r_rlse, l_t_ords(i).wrk_item, l_t_ords(i).wrk_uom);
          l_qty_avail := NVL(l_r_inv.qty_avail, 0);
          l_save_item := l_cmpr_item;
        END IF;   -- l_cmpr_item <> l_save_item

        IF l_r_inv.row_id IS NOT NULL THEN
          logs.dbg('Allocate NonCig Order Line');
          alloc_noncig_ord_ln_sp(i_r_rlse,
                                 l_t_ords(i).ord_num,
                                 l_t_ords(i).ord_ln,
                                 l_t_ords(i).ord_qty,
                                 l_t_ords(i).sub_cd,
                                 l_t_ords(i).cust_id,
                                 l_t_ords(i).mstr_cs_cust_sw,
                                 l_t_ords(i).allw_partl_sw,
                                 l_t_ords(i).gov_cntl_id,
                                 l_t_ords(i).gov_cntl_amt,
                                 l_t_ords(i).gov_cntl_prd,
                                 l_t_ords(i).gov_cntl_pt,
                                 l_r_inv
                                );
        END IF;
      END LOOP tbl_loop;
    END LOOP cur_loop;
    logs.dbg('Close Order Cursor');

    CLOSE l_cur_ords;

    COMMIT;   -- Final
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_cur_ords%ISOPEN THEN
        CLOSE l_cur_ords;
      END IF;

      logs.err(lar_parm);
  END alloc_noncig_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || DVT_VNDR_CMP_SP
  ||  Apply USST Vendor Compliance for tobacco items.
  ||  This process will determine whether customers on the program and in the
  ||  current Release have ordered and allocated enough inventory to meet
  ||  compliance. This is done by comparing the total pick qty of all program
  ||  items for the customer for orders within a date range (for previously
  ||  billed orders as well as the current Release) to the compliance quantity.
  ||  If compliance has not been met, this process will use the Default Vendor
  ||  Compliance Tobacco (DVT) order, which contains compliance items with zero
  ||  order qtys, to meet compliance. This process will cycle thru all order
  ||  lines on the DVT order, in order line sequence, and increase the qty by
  ||  one for available inventory until compliance is met or no available
  ||  inventory exists.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/15/10 | rhalpai | Original for PIR8936
  || 11/02/10 | rhalpai | Change cursor to include sum of compliance item
  ||                    | allocated qtys from current release and handle zero
  ||                    | allocated qty. IM626720
  || 11/02/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 09/29/11 | rhalpai | Moved logic from VNDR_CMP_SP. PIR10460
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dvt_vndr_cmp_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.DVT_VNDR_CMP_SP';
    lar_parm             logs.tar_parm;

    TYPE l_tt_rowids IS TABLE OF ROWID;

    l_t_ord_lns          type_ntab;
    l_t_sub_cds          type_ntab;
    l_t_inv_row_ids      l_tt_rowids;
    l_t_items            type_stab;
    l_t_uoms             type_stab;
    l_t_aisls            type_stab;
    l_t_bins             type_stab;
    l_t_lvls             type_stab;
    l_t_qty_avails       type_ntab;
    l_t_alloc_qtys       type_ntab;

    CURSOR l_cur_ords(
      b_div_part         NUMBER,
      b_llr_dt           DATE,
      b_load_list        VARCHAR2,
      b_rensoft_seed_dt  DATE
    ) IS
      WITH vc AS
           (SELECT   c.cust_id, c.prof_id, c.cmp_qty, c.beg_dt, c.end_dt, MAX(a.ordnoa) AS ord_num
                FROM vndr_cmp_prof_op3l p, vndr_cmp_cust_op2l c, load_depart_op1f ld, stop_eta_op1g se, ordp100a a
               WHERE p.typ = 'DVT'
                 AND c.div_part = b_div_part
                 AND c.prof_id = p.prof_id
                 AND ld.div_part = b_div_part
                 AND ld.llr_dt = b_llr_dt
                 AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                 AND se.div_part = ld.div_part
                 AND se.load_depart_sid = ld.load_depart_sid
                 AND se.cust_id = c.cust_id
                 AND TRUNC(se.eta_ts) BETWEEN c.beg_dt AND c.end_dt
                 AND a.div_part = se.div_part
                 AND a.load_depart_sid = se.load_depart_sid
                 AND a.custa = se.cust_id
                 AND a.excptn_sw = 'N'
                 AND a.ipdtsa = 'DVT'
                 AND a.stata = 'P'
                 AND EXISTS(SELECT 1
                              FROM ordp120b b, sawp505e e, vndr_cmp_item_op1l i
                             WHERE b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND b.excptn_sw = 'N'
                               AND b.ntshpb IS NULL
                               AND e.iteme = b.itemnb
                               AND e.uome = b.sllumb
                               AND i.prof_id = p.prof_id
                               AND i.catlg_num = e.catite)
            GROUP BY c.cust_id, c.prof_id, c.cmp_qty, c.beg_dt, c.end_dt)
      SELECT   y.ord_num, y.need_qty,
               (SELECT TO_CHAR(ld.depart_ts, 'YYYYMMDDHH24MI')
                       || LPAD(se.stop_num, 2, '0')
                       || LPAD(NVL(a.cpoa, ' '), 30)
                       || LPAD(a.ordnoa, 11, '0')
                  FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se
                 WHERE a.div_part = b_div_part
                   AND a.ordnoa = y.ord_num
                   AND a.excptn_sw = 'N'
                   AND ld.div_part = a.div_part
                   AND ld.load_depart_sid = a.load_depart_sid
                   AND se.div_part = a.div_part
                   AND se.load_depart_sid = a.load_depart_sid
                   AND se.cust_id = a.custa) AS seq
          FROM (SELECT   x.ord_num, x.cmp_qty - SUM(x.ord_qty) AS need_qty
                    FROM (SELECT   vc.cmp_qty, vc.ord_num, NVL(SUM(b.pckqtb), 0) AS ord_qty
                              FROM vc, stop_eta_op1g se, ordp100a a, ordp120b b, sawp505e e, vndr_cmp_item_op1l vci
                             WHERE se.div_part = b_div_part
                               AND se.cust_id = vc.cust_id
                               AND TRUNC(se.eta_ts) BETWEEN vc.beg_dt AND vc.end_dt
                               AND a.div_part = se.div_part
                               AND a.load_depart_sid = se.load_depart_sid
                               AND a.custa = se.cust_id
                               AND a.excptn_sw = 'N'
                               AND b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND b.subrcb < 999
                               AND b.statb IN('P', 'T', 'R', 'A')
                               AND b.excptn_sw = 'N'
                               AND e.iteme = b.itemnb
                               AND e.uome = b.sllumb
                               AND vci.prof_id = vc.prof_id
                               AND vci.catlg_num = e.catite
                          GROUP BY vc.cmp_qty, vc.ord_num
                          UNION ALL
                          SELECT vc.cmp_qty, vc.ord_num, h.ord_qty
                            FROM vc,
                                 (SELECT   c.prof_id, c.cust_id, SUM(b.pckqtb) AS ord_qty
                                      FROM vndr_cmp_prof_op3l p, vndr_cmp_cust_op2l c, ordp900a a, ordp920b b,
                                           vndr_cmp_item_op1l vci
                                     WHERE p.typ = 'DVT'
                                       AND c.div_part = b_div_part
                                       AND c.prof_id = p.prof_id
                                       AND a.div_part = c.div_part
                                       AND a.custa = c.cust_id
                                       AND a.etadta BETWEEN c.beg_dt - b_rensoft_seed_dt AND c.end_dt
                                                                                             - b_rensoft_seed_dt
                                       AND a.excptn_sw = 'N'
                                       AND b.div_part = a.div_part
                                       AND b.ordnob = a.ordnoa
                                       AND b.subrcb < 999
                                       AND b.statb = 'A'
                                       AND b.excptn_sw = 'N'
                                       AND b.pckqtb > 0
                                       AND vci.prof_id = p.prof_id
                                       AND vci.catlg_num = b.orditb
                                  GROUP BY c.prof_id, c.cust_id) h
                           WHERE vc.prof_id = h.prof_id
                             AND vc.cust_id = h.cust_id) x
                GROUP BY x.cmp_qty, x.ord_num
                  HAVING x.cmp_qty > SUM(x.ord_qty)) y
      ORDER BY seq;

    PROCEDURE get_ord_ln_inv_sp(
      i_div_part       IN      NUMBER,
      i_ord_num        IN      NUMBER,
      o_t_ord_lns      OUT     type_ntab,
      o_t_sub_cds      OUT     type_ntab,
      o_t_inv_row_ids  OUT     l_tt_rowids,
      o_t_items        OUT     type_stab,
      o_t_uoms         OUT     type_stab,
      o_t_aisls        OUT     type_stab,
      o_t_bins         OUT     type_stab,
      o_t_lvls         OUT     type_stab,
      o_t_qty_avails   OUT     type_ntab,
      o_t_alloc_qtys   OUT     type_ntab
    ) IS
    BEGIN
      SELECT        b.lineb, b.subrcb, w.ROWID, w.itemc, w.uomc, w.aislc, w.binc, w.levlc,
                    w.qavc, 0 AS alloc_qty
      BULK COLLECT INTO o_t_ord_lns, o_t_sub_cds, o_t_inv_row_ids, o_t_items, o_t_uoms, o_t_aisls, o_t_bins, o_t_lvls,
                    o_t_qty_avails, o_t_alloc_qtys
               FROM ordp100a a, ordp120b b, mclp110b di, whsp300c w
              WHERE a.div_part = i_div_part
                AND a.ordnoa = i_ord_num
                AND b.div_part = a.div_part
                AND b.ordnob = i_ord_num
                AND di.div_part = b.div_part
                AND di.itemb = b.itemnb
                AND di.uomb = b.sllumb
                AND w.div_part = di.div_part
                AND w.itemc = DECODE(TRIM(di.suomb), NULL, di.itemb, di.sitemb)
                AND w.uomc = NVL(TRIM(di.suomb), di.uomb)
                AND w.taxjrc IS NULL
                AND w.qavc > 0
                AND b.excptn_sw = 'N'
                AND b.ntshpb IS NULL
                AND b.subrcb < 999
           ORDER BY b.lineb
      FOR UPDATE OF w.qavc, b.ordqtb;
    END get_ord_ln_inv_sp;

    PROCEDURE spread_need_qty_sp(
      i_need_qty       IN             PLS_INTEGER,
      io_t_qty_avails  IN OUT NOCOPY  type_ntab,
      io_t_alloc_qtys  IN OUT NOCOPY  type_ntab
    ) IS
      l_need_qty       PLS_INTEGER := i_need_qty;
      l_ttl_inv_avail  PLS_INTEGER;
      l_idx            PLS_INTEGER;
    BEGIN
      SELECT NVL(SUM(t.column_value), 0)
        INTO l_ttl_inv_avail
        FROM TABLE(io_t_qty_avails) t;

      LOOP
        IF l_idx IS NULL THEN
          l_idx := io_t_qty_avails.FIRST;
        END IF;   -- l_idx IS NULL

        EXIT WHEN(   l_idx IS NULL
                  OR 0 IN(l_need_qty, l_ttl_inv_avail));

        IF io_t_qty_avails(l_idx) > 0 THEN
          io_t_alloc_qtys(l_idx) := io_t_alloc_qtys(l_idx) + 1;
          io_t_qty_avails(l_idx) := io_t_qty_avails(l_idx) - 1;
          l_need_qty := l_need_qty - 1;
          l_ttl_inv_avail := l_ttl_inv_avail - 1;
        END IF;   -- io_t_qty_avails(l_idx) > 0

        l_idx := io_t_qty_avails.NEXT(l_idx);
      END LOOP;
    END spread_need_qty_sp;

    PROCEDURE upd_ord_qtys_sp(
      i_div_part      IN  NUMBER,
      i_ord_num       IN  NUMBER,
      i_t_ord_lns     IN  type_ntab,
      i_t_alloc_qtys  IN  type_ntab
    ) IS
    BEGIN
      FORALL i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST
        UPDATE ordp120b b
           SET b.ordqtb = b.ordqtb + i_t_alloc_qtys(i)
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.lineb = i_t_ord_lns(i)
           AND i_t_alloc_qtys(i) > 0;
    END upd_ord_qtys_sp;

    PROCEDURE alloc_ord_lns_upd_inv_sp(
      i_r_rlse         IN  g_cur_rlse%ROWTYPE,
      i_ord_num        IN  NUMBER,
      i_t_ord_lns      IN  type_ntab,
      i_t_sub_cds      IN  type_ntab,
      i_t_inv_row_ids  IN  l_tt_rowids,
      i_t_items        IN  type_stab,
      i_t_uoms         IN  type_stab,
      i_t_aisls        IN  type_stab,
      i_t_bins         IN  type_stab,
      i_t_lvls         IN  type_stab,
      i_t_alloc_qtys   IN  type_ntab
    ) IS
    BEGIN
      FOR i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST LOOP
        IF i_t_alloc_qtys(i) > 0 THEN
          alloc_ords_sp(i_r_rlse,
                        i_t_items(i),
                        i_t_uoms(i),
                        i_t_aisls(i),
                        i_t_bins(i),
                        i_t_lvls(i),
                        i_ord_num,
                        i_t_ord_lns(i),
                        i_t_alloc_qtys(i),
                        'T',
                        i_t_sub_cds(i),
                        'VNDR_CMP_SP'
                       );
          upd_inv_qty_sp(i_t_inv_row_ids(i), i_t_alloc_qtys(i));
        END IF;   -- i_t_alloc_qtys(i) > 0
      END LOOP;
    END alloc_ord_lns_upd_inv_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Process Order Cursor');
    <<ord_cur_loop>>
    FOR l_r_ord IN l_cur_ords(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list, g_c_rensoft_seed_dt) LOOP
      logs.dbg('Get Order Lines and Available Inventory');
      get_ord_ln_inv_sp(i_r_rlse.div_part,
                        l_r_ord.ord_num,
                        l_t_ord_lns,
                        l_t_sub_cds,
                        l_t_inv_row_ids,
                        l_t_items,
                        l_t_uoms,
                        l_t_aisls,
                        l_t_bins,
                        l_t_lvls,
                        l_t_qty_avails,
                        l_t_alloc_qtys
                       );

      IF l_t_ord_lns.COUNT > 0 THEN
        logs.dbg('Spread Needed Qty Among OrdLns of DVT Order');
        spread_need_qty_sp(l_r_ord.need_qty, l_t_qty_avails, l_t_alloc_qtys);
        logs.dbg('Upd OrdQty for DVT OrdLns');
        upd_ord_qtys_sp(i_r_rlse.div_part, l_r_ord.ord_num, l_t_ord_lns, l_t_alloc_qtys);
        logs.dbg('Allocate DVT OrdLns and Upd Inv');
        alloc_ord_lns_upd_inv_sp(i_r_rlse,
                                 l_r_ord.ord_num,
                                 l_t_ord_lns,
                                 l_t_sub_cds,
                                 l_t_inv_row_ids,
                                 l_t_items,
                                 l_t_uoms,
                                 l_t_aisls,
                                 l_t_bins,
                                 l_t_lvls,
                                 l_t_alloc_qtys
                                );
      END IF;   -- l_t_ord_lns.COUNT > 0
    END LOOP ord_cur_loop;
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dvt_vndr_cmp_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDR_CMP_SP
  ||  Driver for processing Vendor Compliance.
  ||  This process will call the Vendor Compliance modules to increase order
  ||  quantity as needed to meet compliance and then allocate them.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/11 | rhalpai | Moved logic to DVT_VNDR_CMP_SP. Use as driver for
  ||                    | processing Vendor Compliance with calls to
  ||                    | DVQ_VNDR_CMP_SP and DVT_VNDR_CMP_SP. PIR10460
  || 12/29/14 | rhalpai | Add CigSw parm in call to DVQ_VNDR_CMP_SP. Add call
  ||                    | to ALLOC_NONCIG_ORDS_SP. IM-234795
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndr_cmp_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.VNDR_CMP_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Process DVQ Vendor Compliance Item Qty');
    dvq_vndr_cmp_sp(i_r_rlse, 'N');
    logs.dbg('Allocate NonCig DVQ OrdLns');
    alloc_noncig_ords_sp(i_r_rlse, 0);
    logs.dbg('Process DVT USST Tobacco Vendor Compliance');
    dvt_vndr_cmp_sp(i_r_rlse);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END vndr_cmp_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_NONCIGS_SP
  ||  Driver for processing allocation of Non-Cig order lines.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 05/20/10 | rhalpai | Added logic to update event log. PIR8377
  || 07/15/10 | rhalpai | Add logic to call new VNDR_CMP_SP to end of Non-Cig
  ||                    | allocation. PIR8936
  || 08/26/10 | rhalpai | Removed logging of order counts as they are now
  ||                    | automatically done when logging steps. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_noncigs_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.PRCS_NONCIGS_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80)  := 'Initial';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    l_section := 'Upd X-Dock Orders';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_xdock);
    upd_xdock_sp(i_r_rlse);
    COMMIT;
    l_section := 'Bundled-Dist-Level Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_bndl);
    alloc_bundle_dists_sp(i_r_rlse);
    l_section := 'Kit-Level Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_kit);
    alloc_kits_sp(i_r_rlse);
    l_section := 'Item-Level Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_itm);
    alloc_noncig_items_sp(i_r_rlse);
    l_section := 'Order-Level Allocation for All';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_ord);
    alloc_noncig_ords_sp(i_r_rlse, NULL);
    l_section := 'Remove Unallocated Sub Lines';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_del_subs);
    del_sub_lns_sp(i_r_rlse, 'N');
    l_section := 'Order-Level Allocation for Orig Ord Lns of Deleted Subs';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_orig_ords);
    alloc_noncig_ords_sp(i_r_rlse, 998);
    l_section := 'Reset Tagged (998) Orig Sub Codes';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    reset_tagged_orig_sub_cds_sp(i_r_rlse, 'N');
    l_section := 'Create Sub Lines';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_create_subs);
    create_sub_lns_sp(i_r_rlse, 'N');
    l_section := 'Allocate New Sub Lines';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_sub);
    alloc_noncig_ords_sp(i_r_rlse, NULL);
    l_section := 'Apply Vendor Compliance';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_vndr_cmp);
    vndr_cmp_sp(i_r_rlse);
    l_section := 'Log End of NonCig Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_end_noncig);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prcs_noncigs_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_PARTL_ALLOCD_ORDS_SP
  ||  Update status for Partially Allocated Order Lines
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_partl_allocd_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
  BEGIN
    env.tag();

    UPDATE ordp120b b
       SET b.statb = 'T'
     WHERE b.div_part = i_r_rlse.div_part
       AND b.statb = 'P'
       AND b.excptn_sw = 'N'
       AND b.pckqtb > 0
       AND b.ordnob IN(SELECT a.ordnoa
                         FROM load_depart_op1f ld, ordp100a a
                        WHERE ld.div_part = i_r_rlse.div_part
                          AND ld.llr_dt = i_r_rlse.llr_dt
                          AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                          AND a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.excptn_sw = 'N'
                          AND a.stata = 'P');

    COMMIT;
    env.untag();
  END upd_partl_allocd_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_PO_OVRRDS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/06 | rhalpai | Original - created for PIR3209
  || 02/27/07 | rhalpai | Changed logic to exclude POs containing only zeros
  ||                    | as a selection for override. IM290595
  || 03/05/07 | rhalpai | Changed logic to override at single PO per
  ||                    | LLR/Load/Stop/Customer/Type based on Corp-Level parm.
  ||                    | All Container-Tracking Customer order lines for
  ||                    | customers in Corp will be overridden. It will use PO
  ||                    | for customer on order with most lines for item type
  ||                    | (CIG,TOB,GRO,OTH) on release. When no PO is available
  ||                    | it will attempt to use one previously overridden for
  ||                    | same LLR/Load/Stop/Customer/Type. When not found the
  ||                    | default PO will continue to be used. IM290595
  || 04/05/07 | rhalpai | Changed to treat POs for Split Orders as NULL. PIR4274
  || 07/31/07 | rhalpai | Remove restrictions for existence of
  ||                    | PO_OVRIDE_SNGLPO_### parm containing Y value from
  ||                    | insert to PO override table. IM325473
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Replaced LLRDate input parm with ReleaseTS.
  ||                    | Changed cursor to use MCLANE_LOAD_LABEL_RLSE for
  ||                    | matching load list and LLR date.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/15 | rhalpai | Change logic to use parameter table to determine NACS
  ||                    | tobacco categories. PIR15408
  || 10/14/17 | rhalpai | Change logic to use global variables containing nested
  ||                    | table of parm values. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_po_ovrrds_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.INS_PO_OVRRDS_SP';
    lar_parm                  logs.tar_parm;
    l_c_rlse_ts      CONSTANT VARCHAR2(9)   := TO_CHAR(i_r_rlse.rlse_ts, 'YMMDDHH24MI');
    l_c_dflt_cig_po  CONSTANT VARCHAR2(10)  := 'C' || l_c_rlse_ts;
    l_c_dflt_tob_po  CONSTANT VARCHAR2(10)  := 'T' || l_c_rlse_ts;
    l_c_dflt_gro_po  CONSTANT VARCHAR2(10)  := 'G' || l_c_rlse_ts;
    l_c_dflt_oth_po  CONSTANT VARCHAR2(10)  := 'O' || l_c_rlse_ts;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Add PO Overrides');

    INSERT INTO bill_po_ovride_bc1p
                (div_part, ord_num, ord_ln_num, po_num)
      WITH po_lvl AS(
        SELECT i.crp_cd, v.parm_val
          FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd, ROWNUM AS seq
                  FROM TABLE(CAST(g_t_cntnr_trk_po_lvl_crps AS type_stab)) t) i,
               (SELECT t.column_value AS parm_val, ROWNUM AS seq
                  FROM TABLE(CAST(g_t_cntnr_trk_po_lvl_vals AS type_stab)) t) v
         WHERE i.seq = v.seq
      )
      SELECT i_r_rlse.div_part, o.ord_num, o.ord_ln,
             COALESCE
               (
                -- New Group/PO
                (SELECT MAX(y.po_num)
                   FROM (SELECT   x.load_depart_sid, x.cust_id, x.typ, x.po_num, COUNT(1) AS cnt,
                                  MAX(COUNT(1))
                                    KEEP (DENSE_RANK LAST ORDER BY COUNT(1))
                                    OVER(PARTITION BY x.load_depart_sid, x.cust_id, x.typ) AS hi
                             FROM (SELECT ld.load_depart_sid, se.cust_id, a.cpoa AS po_num,
                                          (CASE
                                             WHEN e.bsgrpe = 'CIG' THEN 'CIG'
                                             WHEN(    po_lvl.parm_val = 'WLG'
                                                  AND e.nacse IN(SELECT t.column_value
                                                                   FROM TABLE(CAST(g_t_nacs_tobacco_catgs AS type_stab)) t)
                                                 ) THEN 'TOB'
                                             WHEN po_lvl.parm_val = 'WLG' THEN 'GRO'
                                             ELSE 'OTH'
                                           END
                                          ) AS typ
                                     FROM po_lvl, mclp020b cx, load_depart_op1f ld, stop_eta_op1g se, ordp100a a,
                                          ordp120b b, sawp505e e
                                    WHERE cx.div_part = i_r_rlse.div_part
                                      AND cx.corpb = po_lvl.crp_cd
                                      AND ld.div_part = i_r_rlse.div_part
                                      AND ld.llr_dt = i_r_rlse.llr_dt
                                      AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                                      AND se.div_part = ld.div_part
                                      AND se.load_depart_sid = ld.load_depart_sid
                                      AND se.cust_id = cx.custb
                                      AND a.div_part = se.div_part
                                      AND a.load_depart_sid = se.load_depart_sid
                                      AND a.custa = se.cust_id
                                      AND RTRIM(REPLACE(a.cpoa, '0')) IS NOT NULL
                                      AND NOT EXISTS(SELECT 1
                                                       FROM mclp300d md
                                                      WHERE md.div_part = a.div_part
                                                        AND md.ordnod = a.ordnoa
                                                        AND md.reasnd = 'SPLITORD')
                                      AND b.div_part = a.div_part
                                      AND b.ordnob = a.ordnoa
                                      AND b.statb IN('P', 'T')
                                      AND e.iteme = b.itemnb
                                      AND e.uome = b.sllumb) x
                         GROUP BY x.load_depart_sid, x.cust_id, x.typ, x.po_num) y
                  WHERE y.cnt = y.hi
                    AND y.load_depart_sid = o.load_depart_sid
                    AND y.cust_id = o.cust_id
                    AND y.typ = o.typ),

                -- Existing Group/PO
                (SELECT MAX(bp.po_num)
                   FROM ordp100a a, ordp120b b, bill_po_ovride_bc1p bp, sawp505e e
                  WHERE a.div_part = i_r_rlse.div_part
                    AND a.load_depart_sid = o.load_depart_sid
                    AND a.custa = o.cust_id
                    AND b.div_part = a.div_part
                    AND b.ordnob = a.ordnoa
                    AND bp.div_part = b.div_part
                    AND bp.ord_num = b.ordnob
                    AND bp.ord_ln_num = b.lineb
                    AND e.iteme = b.itemnb
                    AND e.uome = b.sllumb
                    AND (CASE
                           WHEN e.bsgrpe = 'CIG' THEN 'CIG'
                           WHEN(    o.parm_val = 'WLG'
                                AND e.nacse IN(SELECT t.column_value
                                                 FROM TABLE(CAST(g_t_nacs_tobacco_catgs AS type_stab)) t)
                               ) THEN 'TOB'
                           WHEN o.parm_val = 'WLG' THEN 'GRO'
                           ELSE 'OTH'
                         END
                        ) = o.typ),
                -- Defaults
                DECODE(o.parm_val,
                       'WLG', DECODE(o.typ, 'CIG', l_c_dflt_cig_po, 'TOB', l_c_dflt_tob_po, l_c_dflt_gro_po),
                       DECODE(o.typ, 'CIG', l_c_dflt_cig_po, l_c_dflt_oth_po)
                      )
               ) AS po_num
        FROM (SELECT po_lvl.parm_val, b.ordnob AS ord_num, b.lineb AS ord_ln, se.load_depart_sid, se.cust_id,
                     (CASE
                        WHEN e.bsgrpe = 'CIG' THEN 'CIG'
                        WHEN(    po_lvl.parm_val = 'WLG'
                             AND e.nacse IN(SELECT t.column_value
                                              FROM TABLE(CAST(g_t_nacs_tobacco_catgs AS type_stab)) t)
                            ) THEN 'TOB'
                        WHEN po_lvl.parm_val = 'WLG' THEN 'GRO'
                        ELSE 'OTH'
                      END
                     ) AS typ
                FROM po_lvl, mclp020b cx, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, sawp505e e
               WHERE cx.div_part = i_r_rlse.div_part
                 AND cx.corpb = po_lvl.crp_cd
                 AND ld.div_part = i_r_rlse.div_part
                 AND ld.llr_dt = i_r_rlse.llr_dt
                 AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                 AND se.div_part = ld.div_part
                 AND se.load_depart_sid = ld.load_depart_sid
                 AND se.cust_id = cx.custb
                 AND a.div_part = se.div_part
                 AND a.load_depart_sid = se.load_depart_sid
                 AND a.custa = se.cust_id
                 AND (   RTRIM(REPLACE(a.cpoa, '0')) IS NULL
                      OR EXISTS(SELECT 1
                                  FROM mclp300d md
                                 WHERE md.div_part = a.div_part
                                   AND md.ordnod = a.ordnoa
                                   AND md.reasnd = 'SPLITORD')
                      OR EXISTS(SELECT 1
                                  FROM TABLE(CAST(g_t_snglpo_crps AS type_stab)) t
                                 WHERE TO_NUMBER(t.column_value) = cx.corpb)
                     )
                 AND b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.excptn_sw = 'N'
                 AND b.statb IN('P', 'T')
                 AND e.iteme = b.itemnb
                 AND e.uome = b.sllumb) o
       WHERE NOT EXISTS(SELECT 1
                          FROM bill_po_ovride_bc1p po
                         WHERE po.div_part = i_r_rlse.div_part
                           AND po.ord_num = o.ord_num
                           AND po.ord_ln_num = o.ord_ln);

    COMMIT;
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END ins_po_ovrrds_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_DIGIT_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/06 | rhalpai | Original - created for PIR3209
  ||----------------------------------------------------------------------------
  */
  FUNCTION check_digit_fn(
    i_num  IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.CHECK_DIGIT_FN';
    lar_parm             logs.tar_parm;
    l_num_str            VARCHAR2(19);
    l_odd                PLS_INTEGER   := 0;
    l_even               PLS_INTEGER   := 0;
    l_check_digit        VARCHAR2(1);
  BEGIN
    logs.add_parm(lar_parm, 'Num', i_num);

    IF i_num IS NOT NULL THEN
      l_num_str := LPAD(i_num, 19, '0');
      FOR i IN 1 .. 19 LOOP
        IF MOD(i, 2) = 0 THEN
          l_even := l_even + TO_NUMBER(SUBSTR(l_num_str, i, 1));
        ELSE
          l_odd := l_odd + TO_NUMBER(SUBSTR(l_num_str, i, 1));
        END IF;   -- MOD(i, 2) = 0
      END LOOP;
      l_check_digit := SUBSTR(l_odd * 3 + l_even, -1);

      IF l_check_digit <> '0' THEN
        l_check_digit := 10 - TO_NUMBER(l_check_digit);
      END IF;   -- l_check_digit <> '0'
    END IF;   -- i_num IS NOT NULL

    RETURN(l_check_digit);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_digit_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_CNTNR_SP
  ||  Add container entry.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_cntnr_sp(
    i_r_rlse    IN  g_cur_rlse%ROWTYPE,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_cntnr_id  IN  VARCHAR2,
    i_qty       IN  NUMBER
  ) IS
  BEGIN
    INSERT INTO bill_cntnr_id_bc1c
                (div_part, ord_num, ord_ln_num, orig_cntnr_id, orig_qty
                )
         VALUES (i_r_rlse.div_part, i_ord_num, i_ord_ln, i_cntnr_id, i_qty
                );
  END ins_cntnr_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_COMPL_CNTNR_SP
  ||  Add entry for complete container.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_compl_cntnr_sp(
    i_r_rlse   IN  g_cur_rlse%ROWTYPE,
    i_item     IN  VARCHAR2,
    i_uom      IN  VARCHAR2,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER,
    i_qty      IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                           := 'OP_ALLOCATE_PK.ADD_COMPL_CNTNR_SP';
    lar_parm             logs.tar_parm;
    l_cmp_cntnr_id       bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'Qty', i_qty);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Container ID');
    l_cmp_cntnr_id := container_id_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, i_item, i_uom);
    logs.dbg('Add to Container');
    ins_cntnr_sp(i_r_rlse, i_ord_num, i_ord_ln, l_cmp_cntnr_id, i_qty);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_compl_cntnr_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_KIT_CNTNRS_SP
  ||  Assign one container per aggregate kit item for component qty multiple.
  ||  Example:
  ||    KitItemK contains:
  ||      ComponentItemA with Qty 1
  ||      ComponentItemB with Qty 2
  ||    Customer orders 2 of KitItemK represented by :
  ||      Order1 Line1 for ComponentItemA with Qty 2
  ||      Order2 Line1 for ComponentItemB with Qty 4
  ||    ContainerA contains:
  ||      Order1 Line1 ComponentItemA Qty 1
  ||      Order2 Line1 ComponentItemB Qty 2
  ||    ContainerB contains:
  ||      Order1 Line1 ComponentItemA Qty 1
  ||      Order2 Line1 ComponentItemB Qty 2
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/09 | rhalpai | Original - created for IM466943
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Removed LLRDate parm.
  ||                    | Changed cursor to use MCLANE_LOAD_LABEL_RLSE for
  ||                    | matching load list and LLR date.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_kit_cntnrs_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ADD_KIT_CNTNRS_SP';
    lar_parm             logs.tar_parm;

    TYPE l_rt_cntnr IS RECORD(
      kit_item   sawp505e.iteme%TYPE,
      kit_uom    sawp505e.uome%TYPE,
      ratio_qty  PLS_INTEGER,
      ord_num    NUMBER,
      ord_ln     NUMBER,
      comp_qty   NUMBER
    );

    TYPE l_tt_cntnrs IS TABLE OF l_rt_cntnr;

    l_t_cntnrs           l_tt_cntnrs   := l_tt_cntnrs();

    CURSOR l_cur_ords(
      b_div_part   NUMBER,
      b_llr_dt     DATE,
      b_load_list  VARCHAR2
    ) IS
      SELECT   ki.iteme, ki.uome, k.comp_qty, b.ordnob, b.lineb,(b.alcqtb / k.comp_qty) AS ratio_qty,
               DENSE_RANK() OVER(ORDER BY ld.load_num, se.stop_num, se.cust_id, a.cpoa, a.dsorda, k.item_num) AS grp,
               DENSE_RANK() OVER(PARTITION BY ld.load_num, se.stop_num, se.cust_id, a.cpoa, a.dsorda, k.item_num ORDER BY k.comp_item_num)
                                                                                                                 AS seq
          FROM load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b, sawp505e kc, kit_item_mstr_kt1m k,
               sawp505e ki
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.excptn_sw = 'N'
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb = 'T'
           AND b.excptn_sw = 'N'
           AND b.alcqtb > 0
           AND b.subrcb = 0
           AND kc.iteme = b.itemnb
           AND kc.uome = b.sllumb
           AND k.div_part = b_div_part
           AND k.kit_typ = 'AGG'
           AND k.item_num = DECODE('Y', 'Y', k.item_num)   -- force index
           AND k.comp_item_num = kc.catite
           AND ki.catite = k.item_num
      ORDER BY grp, seq;

    TYPE l_tt_ords IS TABLE OF l_cur_ords%ROWTYPE;

    l_t_ords             l_tt_ords     := l_tt_ords();

    PROCEDURE add_cntnrs_sp IS
      l_cntnr_id  bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    BEGIN
      IF l_t_cntnrs.COUNT > 0 THEN
        <<ratio_qty_loop>>
        FOR i IN 1 .. l_t_cntnrs(l_t_cntnrs.FIRST).ratio_qty LOOP
          l_cntnr_id := container_id_fn(i_r_rlse.div_id,
                                        i_r_rlse.rlse_ts,
                                        l_t_cntnrs(l_t_cntnrs.FIRST).kit_item,
                                        l_t_cntnrs(l_t_cntnrs.FIRST).kit_uom
                                       );
          <<cntnr_loop>>
          FOR j IN l_t_cntnrs.FIRST .. l_t_cntnrs.LAST LOOP
            ins_cntnr_sp(i_r_rlse, l_t_cntnrs(j).ord_num, l_t_cntnrs(j).ord_ln, l_cntnr_id, l_t_cntnrs(j).comp_qty);
          END LOOP cntnr_loop;
        END LOOP ratio_qty_loop;
      END IF;   -- l_t_cntnrs.COUNT > 0
    END add_cntnrs_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Open Kit Order Cursor');

    OPEN l_cur_ords(i_r_rlse.div_part, i_r_rlse.llr_dt, i_r_rlse.load_list);

    <<ords_cur_loop>>
    LOOP
      logs.dbg('Fetch Kit Order Cursor');

      FETCH l_cur_ords
      BULK COLLECT INTO l_t_ords LIMIT 100;

      EXIT WHEN l_t_ords.COUNT = 0;
      logs.dbg('Process Kit Order Cursor');
      <<ords_tbl_loop>>
      FOR i IN l_t_ords.FIRST .. l_t_ords.LAST LOOP
        IF l_t_ords(i).seq = 1 THEN
          logs.dbg('Add Kit Containers');
          add_cntnrs_sp;
          -- set to empty
          l_t_cntnrs := l_tt_cntnrs();
        END IF;

        logs.dbg('Store in collection');
        l_t_cntnrs.EXTEND;
        l_t_cntnrs(l_t_cntnrs.LAST).kit_item := l_t_ords(i).iteme;
        l_t_cntnrs(l_t_cntnrs.LAST).kit_uom := l_t_ords(i).uome;
        l_t_cntnrs(l_t_cntnrs.LAST).ratio_qty := l_t_ords(i).ratio_qty;
        l_t_cntnrs(l_t_cntnrs.LAST).ord_num := l_t_ords(i).ordnob;
        l_t_cntnrs(l_t_cntnrs.LAST).ord_ln := l_t_ords(i).lineb;
        l_t_cntnrs(l_t_cntnrs.LAST).comp_qty := l_t_ords(i).comp_qty;
      END LOOP ords_tbl_loop;
    END LOOP ords_cur_loop;
    logs.dbg('Close Kit Order Cursor');

    CLOSE l_cur_ords;

    logs.dbg('Final Add');
    add_cntnrs_sp;
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_ords%ISOPEN THEN
        CLOSE l_cur_ords;
      END IF;

      logs.err(lar_parm);
  END add_kit_cntnrs_sp;

  /*
  ||----------------------------------------------------------------------------
  || CUBING_OF_TOTES_SP
  ||
  || Note:
  || CUBE_BY_HC parm will always use the max piece count (mclp200b.totcnb) even
  || if USE_BOX_MAX_FOR_HC is off. This is because of the hardcoding on the
  || mainframe side for SAMS HalfCase looking for 30's.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/06 | rhalpai | Original - created for PIR3209
  || 01/08/07 | rhalpai | Changed logic for "Add Container for Qty so far that
  ||                    | Fit" when processing CIG and OTH to only do so for
  ||                    | qty greater than zero. PIR3209
  || 02/26/07 | rhalpai | Changed cursor to use fmqtye for HC instead of fmqtye/2.
  ||                    | Also changed cursor to include default PO from
  ||                    | PO Override table when PO is NULL for Container Tracking
  ||                    | Customers. IM290595
  || 02/27/07 | rhalpai | Changed logic to put order qtys for Cig HC qtys into
  ||                    | their own containers. IM290595
  || 03/05/07 | rhalpai | Changed logic to break multiples of 30s into separate
  ||                    | containers based on Corp-Level parm. IM290595
  || 10/19/07 | rhalpai | Change to use new CUBE_BY_HC_SW column on MCLP100A.
  ||                    | PIR3209
  || 05/28/08 | rhalpai | Changed logic to add to BILL_CNTNR_ID_BC1C within loop
  ||                    | instead of using a FORALL INSERT. This was done to
  ||                    | handle an out-of-memory error caused by excessive data.
  ||                    | IM414623
  || 02/11/09 | rhalpai | Changed to assign container by aggregate item for
  ||                    | AGG Kit Item Orders. IM466943
  || 05/05/09 | rhalpai | Removed LLRDate parm.
  ||                    | Changed cursor to use MCLANE_LOAD_LABEL_RLSE for
  ||                    | matching load list and LLR date.
  || 05/15/09 | rhalpai | Added logic to create containers for master-case
  ||                    | quantities. PIR7548
  || 11/17/09 | rhalpai | Added logic to use item HC qty when HC cust with
  ||                    | allocated qty >= item HC qty, otherwise use BoxMax
  ||                    | from tote category (normally set to 30) as HC qty.
  ||                    | Also changes logic to always use BoxMax for non-HC
  ||                    | cust or any remaining qty from HC cust OrdQty.
  ||                    | PIR7958
  || 04/12/10 | rhalpai | Changed logic in cursor to use parm to indicate div
  ||                    | wants to cube by HC for all customers. PIR7640
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction tables.
  ||                    | Change cursor to order by PickSlot for Cigs when CMS
  ||                    | is live. PIR7990
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW. Remove logic
  ||                    | referencing CIG_USE_INVENTORY Parm from cursor.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/28/14 | rhalpai | Change cursor to do an additional sort on AllocQty
  ||                    | (descending) prior to sorting by OrdNum. IM-213869
  || 10/14/17 | rhalpai | Remove call to get parm as it is now referenced in a
  ||                    | global variable and loaded earlier.
  ||                    | Change logic to use global variable containing nested
  ||                    | table of parm values. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cubing_of_totes_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                           := 'OP_ALLOCATE_PK.CUBING_OF_TOTES_SP';
    lar_parm             logs.tar_parm;
    l_grp_save           VARCHAR2(50)                            := '~';
    l_cntnr_id           bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    l_cig_qty            PLS_INTEGER;
    l_wrk_qty            PLS_INTEGER;
    l_hc_cnt             PLS_INTEGER;
    l_inner_cube         NUMBER;

    CURSOR l_cur_ords(
      b_div_part           VARCHAR2,
      b_rlse_id            NUMBER,
      b_llr_dt             DATE,
      b_load_list          VARCHAR2,
      b_part_id            NUMBER,
      b_cube_all_by_hc_sw  VARCHAR2,
      b_t_mstr_cs_crps     type_stab
    ) IS
      SELECT   ld.load_num
               || LPAD(se.stop_num, 2, '0')
               || se.cust_id
               || DECODE(g.cntnr_trckg_sw,
                         'Y', DECODE(RTRIM(REPLACE(a.cpoa, '0')),
                                     NULL, (SELECT bp.po_num
                                              FROM bill_po_ovride_bc1p bp
                                             WHERE bp.div_part = b_div_part
                                               AND bp.ord_num = b.ordnob
                                               AND bp.ord_ln_num = b.lineb),
                                     a.cpoa
                                    ),
                         NULL
                        )
               || LPAD(b.labctb, 3, '0')
               || b.totctb AS grp,
               (CASE
                  WHEN EXISTS(SELECT 1
                                FROM TABLE(CAST(b_t_mstr_cs_crps AS type_stab)) t, mclp020b cx
                               WHERE cx.div_part = b_div_part
                                 AND cx.corpb = TO_NUMBER(t.column_value)
                                 AND cx.custb = a.custa) THEN 'MCQ'
                  WHEN b.totctb IS NULL THEN 'FC'
                  WHEN b.sllumb IN('CII', 'CIR', 'CIC') THEN 'CIG'
                  ELSE 'OTH'
                END
               ) AS typ,
               b.ordnob AS ord_num, b.lineb AS ord_ln, b.itemnb AS cbr_item, b.sllumb AS uom, b.alcqtb, b.totctb,
               t.innerb AS inner_cube, e.cubee AS item_cube,
               (CASE
                  WHEN(    'Y' IN(b_cube_all_by_hc_sw, g.cube_by_hc_sw)
                       AND b.alcqtb >= e.fmqtye) THEN e.fmqtye
                  ELSE t.totcnb
                END
               ) AS hc_qty,
               t.totcnb AS box_qty, e.mulsle AS mc_qty,
               (CASE
                  WHEN 'Y' IN(b_cube_all_by_hc_sw, g.cube_by_hc_sw) THEN 'Y'
                  ELSE 'N'
                END) AS cube_by_hc
          FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, sysp200c c, mclp100a g, ordp120b b,
               tran_ord_op2o op2o, tran_op2t op2t, sawp505e e, mclp200b t, tran_item_op2i op2i
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND a.div_part = se.div_part
           AND a.load_depart_sid = se.load_depart_sid
           AND a.custa = se.cust_id
           AND a.stata = 'P'
           AND a.excptn_sw = 'N'
           AND c.div_part = a.div_part
           AND c.acnoc = a.custa
           AND g.div_part(+) = c.div_part
           AND g.cstgpa(+) = c.retgpc
           AND b.ordnob = a.ordnoa
           AND b.statb = 'T'
           AND b.excptn_sw = 'N'
           AND b.alcqtb > 0
           AND NOT EXISTS(SELECT 1
                            FROM kit_item_mstr_kt1m k
                           WHERE k.div_part = b.div_part
                             AND k.comp_item_num = b.orditb
                             AND k.kit_typ = 'AGG'
                             AND b.subrcb = 0)
           AND op2o.div_part = b.div_part
           AND op2o.part_id = b_part_id
           AND op2o.ord_num = b.ordnob
           AND op2o.ord_ln = b.lineb
           AND op2t.div_part = op2o.div_part
           AND op2t.tran_id = op2o.tran_id
           AND op2t.part_id = op2o.part_id
           AND op2t.rlse_id = b_rlse_id
           AND e.iteme = b.itemnb
           AND e.uome = b.sllumb
           AND t.div_part(+) = b.div_part
           AND t.totctb(+) = b.totctb
           AND op2i.div_part = op2t.div_part
           AND op2i.tran_id = op2t.tran_id
           AND op2i.part_id = op2t.part_id
           AND NOT EXISTS(SELECT 1
                            FROM tran_stamp_op2c op2c
                           WHERE op2c.div_part = op2i.div_part
                             AND op2c.tran_id = op2i.tran_id
                             AND op2c.part_id = op2i.part_id
                             AND op2c.stamp_item = op2i.catlg_num)
      ORDER BY grp, typ, op2i.pick_aisle, op2i.pick_bin, op2i.pick_lvl, op2i.pick_zone, e.catite, b.alcqtb DESC,
               b.ordnob, b.lineb;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();

    IF g_cubing_of_totes_sw = 'Y' THEN
      logs.dbg('Add Kit Containers');
      add_kit_cntnrs_sp(i_r_rlse);
      logs.dbg('Process Order Cursor');
      <<order_loop>>
      FOR l_r_ord IN l_cur_ords(i_r_rlse.div_part,
                                i_r_rlse.rlse_id,
                                i_r_rlse.llr_dt,
                                i_r_rlse.load_list,
                                i_r_rlse.tran_part_id,
                                g_cube_all_by_hc_sw,
                                g_t_mstr_cs_crps
                               ) LOOP
        CASE l_r_ord.typ
          WHEN 'MCQ' THEN
            logs.dbg('MCQ - Process Order Qty Loop');
            <<mc_ord_qty_loop>>
            FOR i IN 1 .. l_r_ord.alcqtb / l_r_ord.mc_qty LOOP
              logs.dbg('FC - Add Complete Container');
              add_compl_cntnr_sp(i_r_rlse,
                                 l_r_ord.cbr_item,
                                 l_r_ord.uom,
                                 l_r_ord.ord_num,
                                 l_r_ord.ord_ln,
                                 l_r_ord.mc_qty
                                );
            END LOOP mc_ord_qty_loop;
          WHEN 'FC' THEN
            logs.dbg('FC - Process Order Qty Loop');
            <<fc_ord_qty_loop>>
            FOR i IN 1 .. l_r_ord.alcqtb LOOP
              logs.dbg('FC - Add Complete Container');
              add_compl_cntnr_sp(i_r_rlse, l_r_ord.cbr_item, l_r_ord.uom, l_r_ord.ord_num, l_r_ord.ord_ln, 1);
            END LOOP fc_ord_qty_loop;
          WHEN 'CIG' THEN
            IF l_r_ord.grp <> l_grp_save THEN
              l_grp_save := l_r_ord.grp;
              l_cntnr_id := NULL;
              l_cig_qty := l_r_ord.box_qty;
            END IF;   -- l_r_ord.grp <> l_grp_save

            -- Put HC qtys to their own containers
            IF (    l_r_ord.cube_by_hc = 'Y'
                AND l_r_ord.hc_qty > 0
                AND l_r_ord.alcqtb >= l_r_ord.hc_qty) THEN
              l_hc_cnt := FLOOR(l_r_ord.alcqtb / l_r_ord.hc_qty);
              FOR i IN 1 .. l_hc_cnt LOOP
                logs.dbg('CIG HC - Add Complete Container');
                add_compl_cntnr_sp(i_r_rlse,
                                   l_r_ord.cbr_item,
                                   l_r_ord.uom,
                                   l_r_ord.ord_num,
                                   l_r_ord.ord_ln,
                                   l_r_ord.hc_qty
                                  );
              END LOOP;
              -- Remaining qty
              l_r_ord.alcqtb := l_r_ord.alcqtb -(l_hc_cnt * l_r_ord.hc_qty);
            END IF;   -- l_r_ord.cube_by_hc = 'Y' AND l_r_ord.hc_qty > 0 AND l_r_ord.alcqtb >= l_r_ord.hc_qty

            -- Allocated Qty could now be zero if it was a multiple of HC Qty
            IF l_r_ord.alcqtb > 0 THEN
              l_wrk_qty := 0;
              logs.dbg('CIG - Process Order Qty Loop');
              <<cig_ord_qty_loop>>
              FOR i IN 1 .. l_r_ord.alcqtb LOOP
                IF l_cntnr_id IS NULL THEN
                  logs.dbg('CIG - Get Container ID');
                  l_cntnr_id := container_id_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_r_ord.cbr_item, l_r_ord.uom);
                END IF;   -- l_cntnr_id IS NULL

                l_cig_qty := l_cig_qty - 1;

                IF l_cig_qty >= 0 THEN
                  -- Fits in tote
                  l_wrk_qty := l_wrk_qty + 1;
                ELSE
                  -- Will not fit in tote
                  IF l_wrk_qty > 0 THEN
                    -- Container for qty so far that did fit
                    logs.dbg('CIG - Add Container for Qty so far that Fit');
                    ins_cntnr_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln, l_cntnr_id, l_wrk_qty);
                  END IF;   -- l_wrk_qty > 0

                  -- Handle remaining qty
                  logs.dbg('CIG - Add Container for Remaining Qty');
                  l_cntnr_id := container_id_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_r_ord.cbr_item, l_r_ord.uom);
                  l_cig_qty := l_r_ord.box_qty - 1;
                  l_wrk_qty := 1;
                END IF;   -- l_cig_qty >= 0
              END LOOP cig_ord_qty_loop;
              -- Container for remaining qty for order line
              -- Do not generate new container ID since next order line
              -- will use current container ID if it also fits in tote
              logs.dbg('CIG - Add Container for Remaining Qty for Order Line');
              ins_cntnr_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln, l_cntnr_id, l_wrk_qty);
            END IF;   -- l_r_ord.alcqtb > 0
          WHEN 'OTH' THEN
            IF l_r_ord.grp <> l_grp_save THEN
              l_grp_save := l_r_ord.grp;
              l_cntnr_id := NULL;
              l_inner_cube := l_r_ord.inner_cube;
            END IF;   -- l_r_ord.grp <> l_grp_save

            l_wrk_qty := 0;
            logs.dbg('OTH - Process Order Qty Loop');
            <<oth_ord_qty_loop>>
            FOR i IN 1 .. l_r_ord.alcqtb LOOP
              IF l_cntnr_id IS NULL THEN
                logs.dbg('OTH - Get Container ID');
                l_cntnr_id := container_id_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_r_ord.cbr_item, l_r_ord.uom);
              END IF;   -- l_cntnr_id IS NULL

              IF l_r_ord.item_cube > l_r_ord.inner_cube THEN
                -- Single item will not fit in tote
                IF l_inner_cube <> l_r_ord.inner_cube THEN
                  -- Generate new container ID when already partially filled
                  -- from previous iteration
                  logs.dbg('OTH - Get Container ID for Partially Filled from Prev Iteration');
                  l_cntnr_id := container_id_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_r_ord.cbr_item, l_r_ord.uom);
                END IF;   -- l_inner_cube <> l_r_ord.inner_cube

                logs.dbg('OTH - Add Container for Partially Filled from Prev Iteration');
                ins_cntnr_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln, l_cntnr_id, 1);
                l_cntnr_id := NULL;
                l_inner_cube := l_r_ord.inner_cube;
              ELSE
                -- Single item fits in tote
                -- Reduce "remaining qty of tote inner cube" by cube of item
                l_inner_cube := l_inner_cube - l_r_ord.item_cube;

                IF l_inner_cube >= 0 THEN
                  -- Fits in tote
                  l_wrk_qty := l_wrk_qty + 1;
                ELSE
                  -- Will not fit in tote
                  IF l_wrk_qty > 0 THEN
                    -- Container for qty so far that did fit
                    logs.dbg('OTH - Add Container for Qty so far that Fit');
                    ins_cntnr_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln, l_cntnr_id, l_wrk_qty);
                  END IF;   -- l_wrk_qty > 0

                  -- Handle remaining qty
                  logs.dbg('OTH - Add Container for Remaining Qty');
                  l_cntnr_id := container_id_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_r_ord.cbr_item, l_r_ord.uom);
                  l_inner_cube := l_r_ord.inner_cube - l_r_ord.item_cube;
                  l_wrk_qty := 1;
                END IF;   -- l_inner_cube > 0
              END IF;   -- l_r_ord.item_cube > l_r_ord.inner_cube
            END LOOP oth_ord_qty_loop;

            IF l_wrk_qty > 0 THEN
              -- Container for remaining qty for order line
              -- Do not generate new container ID since next order line
              -- will use current container ID if it also fits in tote
              logs.dbg('OTH - Add Container for Remaining Qty for Order Line');
              ins_cntnr_sp(i_r_rlse, l_r_ord.ord_num, l_r_ord.ord_ln, l_cntnr_id, l_wrk_qty);
            END IF;   -- l_wrk_qty > 0
        END CASE;
      END LOOP order_loop;
      COMMIT;
    END IF;   -- g_cubing_of_totes_sw = 'Y'

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cubing_of_totes_sp;

  /*
  ||----------------------------------------------------------------------------
  || EXTR_WRK_ORDS_SP
  ||  This procedure builds the Work Order Messages for the mainframe process
  ||  and writes them to the MQ Put Table. It uses the Work Orders table to
  ||  build the entries. After the work orders are written to the MQ Put Table,
  ||  the Status and Timestamp on the Work Orders Table updated to Status = 'R'
  ||  and LastChgTS = RlseTS.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/9/01  | JUSTANI | Original
  || 12/13/02 | rhalpai | Added test bill automation logic to change mq queue
  ||                    | names for test bills.
  || 02/21/03 | rhalpai | Added call to the required replenishment in Cig
  ||                    | Forcast System when appropriate.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Added TestBilCd parm.
  ||                    | Replaced cursor loop with single insert to
  ||                    | MCLANE_MQ_PUT.
  || 03/01/10 | rhalpai | Removed parms for test_bil_cd, use_cig_inv and
  ||                    | use_cig_req_rplnsh, removed logic to process
  ||                    | EXEC_CIG_REQD_RPLNSH_SP, removed logic to set work
  ||                    | order entries to released status. PIR0024
  || 06/07/10 | rhalpai | Change logic to include Post-Stamps transactions
  ||                    | (type 11, line 2). IM591616
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  ||----------------------------------------------------------------------------
  */
  PROCEDURE extr_wrk_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.EXTR_WRK_ORDS_SP';
    lar_parm                    logs.tar_parm;
    l_msg_id                    VARCHAR2(8);
    l_c_rlse_ts_sssss  CONSTANT VARCHAR2(30)  := TO_CHAR(i_r_rlse.rlse_ts, 'YYYYMMDDHH24MISSSSS');
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    l_msg_id :=(CASE i_r_rlse.test_bil_cd
                  WHEN '~' THEN 'QOPRC12'
                  ELSE 'QTPRC12'
                END);
    logs.dbg('Add Work Order MQ Entries');

    INSERT INTO mclane_mq_put
                (mq_msg_id, div_part, mq_msg_status, create_ts, last_chg_ts, mq_corr_put_id, mq_msg_data)
      SELECT   l_msg_id, i_r_rlse.div_part, 'OPN', i_r_rlse.rlse_ts, i_r_rlse.rlse_ts, 0,
               i_r_rlse.div_id
               || RPAD(l_msg_id, 8)
               || RPAD(l_c_rlse_ts_sssss, 30)
               || RPAD('ADD', 13)
               || SUBSTR(LPAD(y.seq, 11, '0'), 5, 7)
               || y.typ
               || NVL(y.frm_to, ' ')
               || lpad_fn(y.iteme, 9, '0')
               || rpad_fn(y.uome, 3)
               || rpad_fn(DECODE(y.inv_zone, '~', ' ', y.inv_zone), 3)
               || rpad_fn(y.slot, 7)
               || lpad_fn(y.qty, 7, '0')
          FROM (SELECT x.iteme, x.uome, x.inv_zone, x.qty, x.typ, x.frm_to,
                       (CASE
                          WHEN x.tran_typ = 22
                          AND LAG(x.inv_zone) OVER(PARTITION BY x.typ ORDER BY x.tran_id) = 'USP' THEN 'UNSPULL'
                          ELSE x.slot
                        END
                       ) AS slot,
                       DECODE(x.frm_to,
                              'T', ROW_NUMBER() OVER(PARTITION BY x.frm_to ORDER BY x.typ, x.tran_id),
                              ROW_NUMBER() OVER(PARTITION BY x.frm_to ORDER BY x.typ, x.tran_id)
                             ) AS seq
                  FROM (SELECT e.iteme, e.uome, op2i.inv_zone, op2i.inv_aisle || op2i.inv_bin || op2i.inv_lvl AS slot,
                               op2i.qty, op2t.tran_typ,
                               (CASE
                                  WHEN EXISTS(SELECT 1
                                                FROM tran_stamp_op2c op2c
                                               WHERE op2c.div_part = op2i.div_part
                                                 AND op2c.tran_id = op2i.tran_id
                                                 AND op2c.part_id = op2i.part_id
                                                 AND op2c.stamp_item = op2i.catlg_num) THEN 'STP'
                                  WHEN op2t.tran_typ IN(23, 24) THEN 'CUT'
                                  ELSE 'CIG'
                                END
                               ) AS typ,
                               op2t.tran_id, DECODE(op2t.tran_typ, 22, 'T', 24, 'T', 'F') AS frm_to
                          FROM tran_op2t op2t, tran_item_op2i op2i, sawp505e e
                         WHERE op2t.div_part = i_r_rlse.div_part
                           AND op2t.rlse_id = i_r_rlse.rlse_id
                           AND op2t.part_id = i_r_rlse.tran_part_id
                           AND op2t.tran_typ IN(11, 21, 22, 23, 24)
                           AND op2i.div_part = op2t.div_part
                           AND op2i.tran_id = op2t.tran_id
                           AND op2i.part_id = op2t.part_id
                           AND (   op2t.tran_typ IN(22, 23, 24)
                                OR (    op2t.tran_typ = 21
                                    AND NOT EXISTS(SELECT 1
                                                     FROM tran_stamp_op2c op2c
                                                    WHERE op2c.div_part = op2i.div_part
                                                      AND op2c.tran_id = op2i.tran_id
                                                      AND op2c.part_id = op2i.part_id
                                                      AND op2c.stamp_item = op2i.catlg_num)
                                   )
                                OR (    op2t.tran_typ = 11
                                    AND EXISTS(SELECT 1
                                                 FROM tran_stamp_op2c op2c
                                                WHERE op2c.div_part = op2i.div_part
                                                  AND op2c.tran_id = op2i.tran_id
                                                  AND op2c.part_id = op2i.part_id
                                                  AND op2c.stamp_item = op2i.catlg_num)
                                   )
                               )
                           AND e.catite = op2i.catlg_num) x) y
      ORDER BY y.typ, y.seq, y.frm_to;

    COMMIT;
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extr_wrk_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_WRK_ORD_STATS_SP
  ||  Set work order entries to Released status
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_wrk_ord_stats_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
  BEGIN
    env.tag();

    UPDATE mclp240b b
       SET b.statb = 'R',
           b.last_chg_ts = i_r_rlse.rlse_ts
     WHERE b.div_part = i_r_rlse.div_part
       AND b.statb = 'P';

    COMMIT;
    env.untag();
  END upd_wrk_ord_stats_sp;

  /*
  ||----------------------------------------------------------------------------
  || TAG_EXCPT_ORDS_SP
  ||  Tag orders in the Exception Well when all orders in Good Well are
  ||  released for a Load
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE tag_excpt_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
  BEGIN
    env.tag();

    UPDATE ordp120b b
       SET b.statb = 'P'
     WHERE b.div_part = i_r_rlse.div_part
       AND b.statb = 'O'
       AND b.excptn_sw = 'Y'
       AND b.ordnob IN(SELECT a.ordnoa
                         FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                        WHERE r.div_part = i_r_rlse.div_part
                          AND r.rlse_ts = i_r_rlse.rlse_ts
                          AND rl.div_part = r.div_part
                          AND rl.rlse_id = r.rlse_id
                          AND rl.typ_id = 'LOAD'
                          AND ld.div_part = r.div_part
                          AND ld.llr_dt = r.llr_dt
                          AND ld.load_num = rl.val
                          AND a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.stata = 'P'
                          AND EXISTS(SELECT 1
                                       FROM ordp120b b2
                                      WHERE b2.div_part = a.div_part
                                        AND b2.ordnob = a.ordnoa
                                        AND b2.statb = 'O'
                                        AND b2.excptn_sw = 'Y')
                          AND NOT EXISTS(SELECT 1
                                           FROM ordp120b b2
                                          WHERE b2.div_part = a.div_part
                                            AND b2.ordnob = a.ordnoa
                                            AND b2.statb IN('O', 'I')
                                            AND b2.excptn_sw = 'N'));

    COMMIT;
    env.untag();
  END tag_excpt_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || GOV_CNTL_RSTR_SP
  ||  State Restrict items with log entries that had sufficient inventory
  ||  (where one more items would have exceeded gov threshold)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gov_cntl_rstr_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
  BEGIN
    env.tag();

    UPDATE ordp120b b
       SET b.ntshpb = 'ITMSTRST'
     WHERE EXISTS(SELECT 1
                    FROM gov_cntl_log_p680a a, gov_cntl_p600a g, gov_cntl_cust_p640a c, gov_cntl_item_p660a i
                   WHERE a.div_part = b.div_part
                     AND a.ord_num = b.ordnob
                     AND a.ord_ln = b.lineb
                     AND g.div_part = a.div_part
                     AND g.gov_cntl_id = a.gov_cntl_id
                     AND c.div_part = a.div_part
                     AND c.gov_cntl_id = a.gov_cntl_id
                     AND c.cust_num = a.cust_num
                     AND c.prd_beg_ts = a.prd_beg_ts
                     AND i.div_part = a.div_part
                     AND i.gov_cntl_id = a.gov_cntl_id
                     AND i.item_num = b.itemnb
                     AND i.uom = b.sllumb
                     AND i.gov_cntl_pt >(g.gov_cntl_amt - c.shp_pts))
       AND EXISTS(SELECT 1
                    FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                   WHERE r.div_part = i_r_rlse.div_part
                     AND r.rlse_ts = i_r_rlse.rlse_ts
                     AND rl.div_part = r.div_part
                     AND rl.rlse_id = r.rlse_id
                     AND rl.typ_id = 'LOAD'
                     AND ld.div_part = r.div_part
                     AND ld.llr_dt = r.llr_dt
                     AND ld.load_num = rl.val
                     AND a.div_part = ld.div_part
                     AND a.load_depart_sid = ld.load_depart_sid
                     AND a.ordnoa = b.ordnob)
       AND b.div_part = i_r_rlse.div_part
       AND b.statb = 'P'
       AND b.excptn_sw = 'N'
       AND b.ntshpb IS NULL;

    COMMIT;
    env.untag();
  END gov_cntl_rstr_sp;

  /*
  ||----------------------------------------------------------------------------
  || EXTR_ORDS_SP
  ||  This procedure builds the Order Messages for the Mainframe process.
  ||  If a complete Load was released, the corresponding Orders in the Exception
  ||  Well will be updated to a Status of 'P'. All orders that were Released are
  ||  extracted from the Order Well and Exception Well and formatted for
  ||  Mainframe processing.  The Entries are written to a UNIX file that is
  ||  ZIPPED and FTPd to the mainframe by a separate process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/25/01 | JUSTANI | Original
  || 05/10/01 | SKADALI | Modified 'DISOUT ' to 'DISOUT'
  || 07/25/02 | rhalpai | Change QOPRC07 msg built from ords_to_extract_cur
  ||                    | cursor to include LLR date (YYYY-MM-DD) and pre-post
  ||                    | complete flag. Also pad the output record to 500 bytes.
  || 12/13/02 | rhalpai | Added logic to set billing run type ('HRD' or 'TST')
  ||                    | in QOPRC07 msg based on new test bill flag on
  ||                    | mclane_load_label_rlse.
  || 01/20/03 | rhalpai | Added logic to create a departure sequence number for
  ||                    | QOPRC07 msg to be used for sorting on the mainframe.
  || 03/25/03 | rhalpai | Changed sequence number in cursor to be unique for
  ||                    | multiple loads with the same departure.
  || 05/13/03 | rhalpai | Added logic to handle Government Controlled (DEA) Items
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 10/20/06 | rhalpai | Changed cursor to retrieve PO from Billing PO Override
  ||                    | when available and also added container info from the
  ||                    | Billing Container ID table. Added QOPRC06 ftp file.
  ||                    | PIR3209
  || 04/05/07 | rhalpai | Changed order extract cursor to clear POs used for
  ||                    | Split Orders. PIR4274
  || 08/29/07 | rhalpai | Changed order extract cursor to clear POs containing
  ||                    | 'STANDING' used for Default Orders. PIR4556
  || 08/11/08 | rhalpai | Changed to use DISOUT or INVOUT as not-ship-reason if
  ||                    | picked qty is zero.
  || 05/05/09 | rhalpai | Changed QOPRC06 file to include Load/Stop/Item/KitCd
  ||                    | ('A' = Aggregate kit component) in last 13 bytes.
  ||                    | PIR6515
  || 03/01/10 | rhalpai | Removed parms for LLRDt, bil_run and test_bil_cd.
  ||                    | Removed logic to tag exception order lines when all
  ||                    | orders are released for a load.
  ||                    | Removed logic to update order detail not-ship-reasons
  ||                    | for Gov Control Restricted.
  ||                    | Removed logic to update order detail not-ship-reasons
  ||                    | for outs. PIR0024
  || 05/20/10 | rhalpai | Changed cursor to not pass a not-ship-rsn of INVOUT
  ||                    | or DISOUT when ordqty is zero. PIR8377
  || 08/17/10 | rhalpai | Changed logic to add inventory zone and inventory slot
  ||                    | to QOPRC07. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction tables and add Stamp
  ||                    | info to QOPRC07. PIR7990
  || 07/10/12 | rhalpai | Remove unused columns.
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  || 02/05/13 | rhalpai | Add logic for Restricted Outs. This will reflect
  ||                    | reductions in order qty such as due to MaxQty
  ||                    | application. It will show ITMSTRST as NotShpRsn when
  ||                    | OrdQty = 0 and OrdQty < OrigQty. It will show OrigQty
  ||                    | as OrdQty when OrdQty < OrigQty and OrigQty > 0 and
  ||                    | NotShpRsn is QTYZERO or INVOUT. It will include an
  ||                    | additional .2 OrdLn with ITMSTRST as NotShpRsn and
  ||                    | (OrigQty - PckQty) as OrdQty for fully allocated
  ||                    | order lines when OrdQty < OrigQty. PIR12285
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  || 10/06/15 | rhalpai | Change logic to immediately write to QOPRC06/QOPRC07
  ||                    | files to improve performance. IM-321633
  || 10/14/17 | rhalpai | Change logic to use global variable containing nested
  ||                    | table of parm values. PIR15427
  || 03/27/18 | rhalpai | Change logic to prevent duplicate order line when
  ||                    | creating additional .2 order line for restricted out.
  ||                    | Use FLOOR(line) + .2 instead of line + .2. SDHD-280324
  || 11/22/21 | rhalpai | Add cust_tax_jrsdctn zero left-padded for 6 bytes to end of extract. PIR21509
  || 04/20/22 | rhalpai | Add partl_dist_sw to QOPRC07. PIR21059
  ||----------------------------------------------------------------------------
  */
  PROCEDURE extr_ords_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm      := 'OP_ALLOCATE_PK.EXTR_ORDS_SP';
    lar_parm                    logs.tar_parm;
    l_qoprc06_file              UTL_FILE.file_type;
    l_qoprc07_file              UTL_FILE.file_type;
    l_c_file_dir       CONSTANT VARCHAR2(50)       := '/ftptrans';
    l_c_qoprc06_fname  CONSTANT VARCHAR2(50)       := i_r_rlse.div_id || '_CONTAINER_ORDERS_' || i_r_rlse.rlse_ts_char;
    l_c_qoprc07_fname  CONSTANT VARCHAR2(50)       := i_r_rlse.div_id || '_ALLOCATE_ORDERS_' || i_r_rlse.rlse_ts_char;
    l_cnt                       PLS_INTEGER        := 0;
    l_is_first_qoprc06          BOOLEAN            := TRUE;
    l_qoprc07_ln                typ.t_maxvc2;
    l_missing_slot_cnt          PLS_INTEGER        := 0;

    CURSOR l_cur_qoprc07(
      b_div_part             NUMBER,
      b_rlse_id              NUMBER,
      b_llr_dt               DATE,
      b_load_list            VARCHAR2,
      b_part_id              NUMBER,
      b_test_bil_cd          VARCHAR2,
      b_t_rstr_out_qty_crps  type_stab
    ) IS
      SELECT   /* + NO_PARALLEL */
               x.ord_src, DECODE(b_test_bil_cd, '~', 'HRD', 'TST') AS bil_run_typ, x.load_num, x.stop_num, x.cust_id,
               x.legcy_ref, x.ord_num, x.ord_ln, x.catlg_num, x.cbr_item, x.uom, x.ord_qty, x.pick_qty,
               x.pick_qty AS shp_qty, x.price_amt, x.rtl_amt, x.rtl_mult,
               (CASE
                  WHEN(    x.not_shp_rsn IS NULL
                       AND x.ord_qty > 0
                       AND NVL(x.pick_qty, 0) = 0) THEN DECODE(x.ord_typ, 'D', 'DISOUT', 'INVOUT')
                  ELSE x.not_shp_rsn
                END
               ) AS not_shp_rsn,
               x.mfst_catg, x.tote_catg, NVL(x.orig_cbr_item, x.cbr_item) AS orig_cbr_item,
               NVL(x.orig_uom, x.uom) AS orig_uom,
               DECODE(NVL(x.sub_cd, 0), 1, 'UNC', 2, 'REP', 3, 'RND', 4, 'CON', 5, 'SUB', '   ') AS orig_not_shp_rsn,
               x.invc_catg, x.lbl_catg, x.pick_slot, x.eta_ts,
               NVL((SELECT p.po_num
                      FROM bill_po_ovride_bc1p p
                     WHERE p.div_part = b_div_part
                       AND p.ord_num = x.ord_num
                       AND p.ord_ln_num = x.ord_ln),
                   (CASE
                      WHEN x.po_num = 'STANDING' THEN ' '
                      WHEN EXISTS(SELECT 1
                                    FROM mclp300d md
                                   WHERE md.div_part = b_div_part
                                     AND md.ordnod = x.ord_num
                                     AND md.reasnd = 'SPLITORD') THEN ' '
                      ELSE x.po_num
                    END
                   )
                  ) AS po_num,
               x.cust_pass_area, x.item_pass_area, x.mcl_cust, x.hard_rtl_sw, x.hard_price_sw,
               DECODE(x.ord_typ, 'D', 'DIS', 'REG') AS ord_typ, x.cust_item,
               NVL(x.orig_item, x.catlg_num) AS orig_catlg_num, x.load_typ,
               DECODE(x.ord_typ, 'N', 'Y', 'N') AS no_ord_sw, x.conf_num, x.comnt AS ord_comnt, x.llr_ts, x.prepost_sw,
               DENSE_RANK() OVER(ORDER BY x.depart_ts, x.load_num) AS depart_seq, x.inv_zone,
               (CASE
                  WHEN x.uom IN('CII', 'CIC', 'CIR') THEN NVL(x.inv_slot, x.pick_slot)
                END) AS inv_slot, x.cig_sel_cd, x.hand_stamp_sw, x.cust_tax_jrsdctn, x.stamp_list,
               (CASE
                  WHEN(    x.ord_typ = 'D'
                       AND x.allw_partl_sw = 'Y'
                       AND (   x.sub_cd = 999
                            OR MOD(x.ord_ln, 1) = .1)) THEN 'Y'
                  ELSE 'N'
                END) AS partl_dist_sw
          FROM (SELECT oa.ipdtsa AS ord_src, ld.load_num, ld.depart_ts, se.stop_num, se.cust_id,
                       oa.legrfa AS legcy_ref, ob.ordnob AS ord_num, ob.lineb AS ord_ln, ob.orditb AS catlg_num,
                       ob.itemnb AS cbr_item, ob.sllumb AS uom,
                       (CASE
                          WHEN(    pm.crp_cd = cx.corpb
                               AND ob.ordqtb < ob.orgqtb
                               AND NVL(ob.ntshpb, 'INVOUT') IN('INVOUT', 'QTYZERO')
                               AND oa.dsorda = 'R'
                               AND NVL(ob.pckqtb, 0) = 0
                               AND ob.orgqtb > 0
                              ) THEN ob.orgqtb
                          ELSE ob.ordqtb
                        END
                       ) AS ord_qty,
                       ob.pckqtb AS pick_qty, ob.hdprcb AS price_amt, ob.hdrtab AS rtl_amt, ob.hdrtmb AS rtl_mult,
                       (CASE
                          WHEN(    pm.crp_cd = cx.corpb
                               AND ob.ordqtb = 0
                               AND ob.orgqtb > 0) THEN 'ITMSTRST'
                          ELSE ob.ntshpb
                        END
                       ) AS not_shp_rsn,
                       ob.manctb AS mfst_catg, ob.totctb AS tote_catg, ob.subrcb AS sub_cd, ob.invctb AS invc_catg,
                       ob.labctb AS lbl_catg, se.eta_ts, oa.cpoa AS po_num, oa.cspasa AS cust_pass_area,
                       ob.itpasb AS item_pass_area, cx.mccusb AS mcl_cust, ob.rtfixb AS hard_rtl_sw,
                       ob.prfixb AS hard_price_sw, oa.dsorda AS ord_typ, ob.cusitb AS cust_item,
                       ob.orgitb AS orig_item, oa.ldtypa AS load_typ, oa.connba AS conf_num, oc.commc AS comnt,
                       e.iteme AS orig_cbr_item, e.uome AS orig_uom, t.pick_slot,
                       (SELECT DECODE(MAX(pp.cust_num), NULL, 'N', 'Y')
                          FROM prepost_load_op1p pp
                         WHERE pp.div_part = b_div_part
                           AND pp.load_num = ld.load_num
                           AND pp.stop_num = se.stop_num
                           AND pp.cust_num = se.cust_id
                           AND pp.llr_date = b_llr_dt) AS prepost_sw,
                       ld.llr_ts, t.inv_zone, t.inv_slot, ld.load_depart_sid, NVL(t.cig_sel_cd, 'X') AS cig_sel_cd,
                       t.hand_stamp_sw, t.cust_tax_jrsdctn, t.stamp_list,
                       DECODE(oa.pshipa, '0', 'N', 'N', 'N', 'Y') AS allw_partl_sw
                  FROM load_depart_op1f ld, ordp100a oa, stop_eta_op1g se, ordp120b ob, ordp140c oc, mclp020b cx,
                       sawp505e e, (SELECT TO_NUMBER(t.column_value) AS crp_cd
                                      FROM TABLE(CAST(b_t_rstr_out_qty_crps AS type_stab)) t) pm,
                       (SELECT op2o.ord_num, op2o.ord_ln,
                               op2i.pick_aisle || op2i.pick_bin || op2i.pick_lvl AS pick_slot,
                               DECODE(op2i.inv_zone, '~', ' ', op2i.inv_zone) AS inv_zone,
                               op2i.inv_aisle || op2i.inv_bin || op2i.inv_lvl AS inv_slot, op2i.cig_sel_cd,
                               op2i.hand_stamp_sw, op2i.cust_tax_jrsdctn,
                               to_list_fn(CURSOR(SELECT LPAD(op2c.stamp_item, 6, '0') || op2c.stamp_apld_cd
                                                   FROM tran_stamp_op2c op2c
                                                  WHERE op2c.div_part = op2t.div_part
                                                    AND op2c.tran_id = op2t.tran_id
                                                    AND op2c.part_id = op2t.part_id
                                                ),
                                          NULL
                                         ) AS stamp_list
                          FROM tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i
                         WHERE op2t.div_part = b_div_part
                           AND op2t.rlse_id = b_rlse_id
                           AND op2t.part_id = b_part_id
                           AND op2o.div_part = op2t.div_part
                           AND op2o.tran_id = op2t.tran_id
                           AND op2o.part_id = op2t.part_id
                           AND op2i.div_part = op2t.div_part
                           AND op2i.tran_id = op2t.tran_id
                           AND op2i.part_id = op2t.part_id
                           AND NOT EXISTS(SELECT 1
                                            FROM tran_stamp_op2c op2c
                                           WHERE op2c.div_part = op2i.div_part
                                             AND op2c.tran_id = op2i.tran_id
                                             AND op2c.part_id = op2i.part_id
                                             AND op2c.stamp_item = op2i.catlg_num)) t
                 WHERE ld.div_part = b_div_part
                   AND ld.llr_dt = b_llr_dt
                   AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                   AND oa.div_part = ld.div_part
                   AND oa.load_depart_sid = ld.load_depart_sid
                   AND oa.stata = 'P'
                   AND se.div_part = oa.div_part
                   AND se.load_depart_sid = oa.load_depart_sid
                   AND se.cust_id = oa.custa
                   AND oc.div_part = oa.div_part
                   AND oc.ordnoc = oa.ordnoa
                   AND oc.seqc = 0   -- only get first comment line
                   AND cx.div_part = oa.div_part
                   AND cx.custb = oa.custa
                   AND ob.div_part = oa.div_part
                   AND ob.ordnob = oa.ordnoa
                   AND ob.statb IN('P', 'T')
                   AND pm.crp_cd(+) = cx.corpb
                   AND e.catite(+) = ob.orgitb
                   AND t.ord_num(+) = ob.ordnob
                   AND t.ord_ln(+) = ob.lineb
                UNION ALL
                SELECT oa.ipdtsa AS ord_src, ld.load_num, ld.depart_ts, se.stop_num, se.cust_id, oa.legrfa AS legcy_ref,
                       ob.ordnob AS ord_num, FLOOR(ob.lineb) + .2 AS ord_ln, ob.orditb AS catlg_num,
                       ob.itemnb AS cbr_item, ob.sllumb AS uom, ob.orgqtb - NVL(ob.pckqtb, 0) AS ord_qty, 0 AS pick_qty,
                       ob.hdprcb AS price_amt, ob.hdrtab AS rtl_amt, ob.hdrtmb AS rtl_mult, 'ITMSTRST' AS not_shp_rsn,
                       ob.manctb AS mfst_catg, ob.totctb AS tote_catg, ob.subrcb AS sub_cd, ob.invctb AS invc_catg,
                       ob.labctb AS lbl_catg, se.eta_ts, oa.cpoa AS po_num, oa.cspasa AS cust_pass_area,
                       ob.itpasb AS item_pass_area, cx.mccusb AS mcl_cust, ob.rtfixb AS hard_rtl_sw,
                       ob.prfixb AS hard_price_sw, oa.dsorda AS ord_typ, ob.cusitb AS cust_item, ob.orgitb AS orig_item,
                       oa.ldtypa AS load_typ, oa.connba AS conf_num, oc.commc AS comnt, e.iteme AS orig_cbr_item,
                       e.uome AS orig_uom, NULL AS pick_slot,
                       (SELECT DECODE(MAX(pp.cust_num), NULL, 'N', 'Y')
                          FROM prepost_load_op1p pp
                         WHERE pp.div_part = b_div_part
                           AND pp.load_num = ld.load_num
                           AND pp.stop_num = se.stop_num
                           AND pp.cust_num = se.cust_id
                           AND pp.llr_date = b_llr_dt) AS prepost_sw,
                       ld.llr_ts, NULL AS inv_zone, NULL AS inv_slot, ld.load_depart_sid, 'X' AS cig_sel_cd,
                       NULL AS hand_stamp_sw, NULL AS cust_tax_jrsdctn, NULL AS stamp_list,
                       DECODE(oa.pshipa, '0', 'N', 'N', 'N', 'Y') AS allw_partl_sw
                  FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd
                          FROM TABLE(CAST(b_t_rstr_out_qty_crps AS type_stab)) t) pm, load_depart_op1f ld, ordp100a oa,
                       stop_eta_op1g se, ordp120b ob, ordp140c oc, mclp020b cx, sawp505e e
                 WHERE ld.div_part = b_div_part
                   AND ld.llr_dt = b_llr_dt
                   AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                   AND oa.div_part = ld.div_part
                   AND oa.load_depart_sid = ld.load_depart_sid
                   AND oa.stata = 'P'
                   AND se.div_part = oa.div_part
                   AND se.load_depart_sid = oa.load_depart_sid
                   AND se.cust_id = oa.custa
                   AND oc.div_part = oa.div_part
                   AND oc.ordnoc = oa.ordnoa
                   AND oc.seqc = 0   -- only get first comment line
                   AND cx.div_part = oa.div_part
                   AND cx.custb = oa.custa
                   AND cx.corpb = pm.crp_cd
                   AND ob.div_part = oa.div_part
                   AND ob.ordnob = oa.ordnoa
                   AND ob.ordqtb < ob.orgqtb
                   AND ob.ntshpb IS NULL
                   AND ob.pckqtb = ob.ordqtb
                   AND ob.ordqtb > 0
                   AND ob.statb IN('P', 'T')
                   AND e.catite(+) = ob.orgitb) x
      ORDER BY x.ord_num, x.ord_ln;

    TYPE l_tt_qoprc07 IS TABLE OF l_cur_qoprc07%ROWTYPE;

    l_t_qoprc07                 l_tt_qoprc07;

    PROCEDURE write_qoprc06_sp(
      i_div        IN  VARCHAR2,
      i_div_part   IN  NUMBER,
      i_ord_num    IN  NUMBER,
      i_ord_ln     IN  NUMBER,
      i_load_num   IN  VARCHAR2,
      i_stop_num   IN  NUMBER,
      i_catlg_num  IN  VARCHAR2
    ) IS
      l_kit_cd       VARCHAR2(1);
      l_t_cntnr_ids  type_stab    := type_stab();
      l_t_qtys       type_ntab    := type_ntab();
      l_qoprc06_ln   VARCHAR2(64);
    BEGIN
      SELECT DECODE(MAX(k.item_num), NULL, ' ', 'A')
        INTO l_kit_cd
        FROM kit_item_mstr_kt1m k
       WHERE k.div_part = i_div_part
         AND k.comp_item_num = i_catlg_num;

      SELECT c.orig_cntnr_id, c.orig_qty
      BULK COLLECT INTO l_t_cntnr_ids, l_t_qtys
        FROM bill_cntnr_id_bc1c c
       WHERE c.div_part = i_div_part
         AND c.ord_num = i_ord_num
         AND c.ord_ln_num = i_ord_ln;

      IF l_t_cntnr_ids.COUNT > 0 THEN
        FOR i IN l_t_cntnr_ids.FIRST .. l_t_cntnr_ids.LAST LOOP
          l_qoprc06_ln := RPAD(i_div, 2)
                          || LPAD(i_ord_num, 11, '0')
                          || TO_CHAR(i_ord_ln, 'FM0999V99999')
                          || RPAD(l_t_cntnr_ids(i), 20)
                          || LPAD(l_t_qtys(i), 9, '0')
                          || LPAD(i_load_num, 4)
                          || LPAD(i_stop_num, 2, '0')
                          || LPAD(i_catlg_num, 6)
                          || l_kit_cd;

          IF l_is_first_qoprc06 THEN
            l_is_first_qoprc06 := FALSE;
            UTL_FILE.putf(l_qoprc06_file, l_qoprc06_ln);
          ELSE
            UTL_FILE.putf(l_qoprc06_file, '\n' || l_qoprc06_ln);
          END IF;   -- l_is_first_qoprc06
        END LOOP;
      END IF;   -- l_t_cntnr_ids.COUNT > 0
    END write_qoprc06_sp;

    PROCEDURE check_empty_slots_sp(
      i_div_part    IN      NUMBER,
      i_rlse_id     IN      NUMBER,
      i_part_id     IN      NUMBER,
      io_r_qoprc07  IN OUT  l_cur_qoprc07%ROWTYPE
    ) IS
      l_cv  SYS_REFCURSOR;
    BEGIN
      IF (    io_r_qoprc07.pick_slot IS NULL
          AND io_r_qoprc07.pick_qty > 0) THEN
        logs.warn('Missing Slot for Allocated Order Line'
                  || cnst.newline_char
                  || ' OrdNum: '
                  || util.to_str(io_r_qoprc07.ord_num)
                  || ' OrdLn: '
                  || util.to_str(FLOOR(io_r_qoprc07.ord_ln))
                  || ' CatlgNum: '
                  || util.to_str(io_r_qoprc07.catlg_num),
                  lar_parm
                 );

        OPEN l_cv
         FOR
           SELECT op2i.pick_aisle || op2i.pick_bin || op2i.pick_lvl AS pick_slot,
                  DECODE(op2i.inv_zone, '~', ' ', op2i.inv_zone) AS inv_zone,
                  (CASE
                     WHEN e.uome IN('CII', 'CIC', 'CIR') THEN NVL(op2i.inv_aisle || op2i.inv_bin || op2i.inv_lvl,
                                                                  op2i.pick_aisle || op2i.pick_bin || op2i.pick_lvl
                                                                 )
                   END
                  ) AS inv_slot,
                  NVL(op2i.cig_sel_cd, 'X') AS cig_sel_cd, op2i.hand_stamp_sw, op2i.cust_tax_jrsdctn,
                  to_list_fn(CURSOR(SELECT LPAD(op2c.stamp_item, 6, '0') || op2c.stamp_apld_cd
                                      FROM tran_stamp_op2c op2c
                                     WHERE op2c.div_part = op2t.div_part
                                       AND op2c.tran_id = op2t.tran_id
                                       AND op2c.part_id = op2t.part_id
                                   ),
                             NULL
                            ) AS stamp_list
             FROM tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i, sawp505e e
            WHERE op2t.div_part = i_div_part
              AND op2t.rlse_id = i_rlse_id
              AND op2t.part_id = i_part_id
              AND op2o.div_part = op2t.div_part
              AND op2o.tran_id = op2t.tran_id
              AND op2o.part_id = op2t.part_id
              AND op2o.ord_num = io_r_qoprc07.ord_num
              AND op2o.ord_ln = io_r_qoprc07.ord_ln
              AND op2i.div_part = op2t.div_part
              AND op2i.tran_id = op2t.tran_id
              AND op2i.part_id = op2t.part_id
              AND e.catite = op2i.catlg_num
              AND NOT EXISTS(SELECT 1
                               FROM tran_stamp_op2c op2c
                              WHERE op2c.div_part = op2i.div_part
                                AND op2c.tran_id = op2i.tran_id
                                AND op2c.part_id = op2i.part_id
                                AND op2c.stamp_item = op2i.catlg_num);

        FETCH l_cv
         INTO io_r_qoprc07.pick_slot, io_r_qoprc07.inv_zone, io_r_qoprc07.inv_slot, io_r_qoprc07.cig_sel_cd,
              io_r_qoprc07.hand_stamp_sw, io_r_qoprc07.cust_tax_jrsdctn, io_r_qoprc07.stamp_list;
      END IF;   -- io_r_qoprc07.pick_slot IS NULL AND io_r_qoprc07.pick_qty > 0
    END check_empty_slots_sp;

    PROCEDURE format_qoprc07_ln_sp(
      i_div         IN      VARCHAR2,
      i_rlse_ts     IN      DATE,
      i_r_qoprc07   IN      l_cur_qoprc07%ROWTYPE,
      o_qoprc07_ln  OUT     VARCHAR2
    ) IS
    BEGIN
      o_qoprc07_ln := RPAD(i_div, 2)
                      || 'QGROPB  '
                      || RPAD(TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISSSSS'), 30)
                      || 'ADD'
                      || rpad_fn(i_r_qoprc07.ord_src, 3)
                      || RPAD(' ', 7)
                      || RPAD(i_r_qoprc07.bil_run_typ, 3)
                      || rpad_fn(i_r_qoprc07.load_num, 4)
                      || lpad_fn(i_r_qoprc07.stop_num, 2, '0')
                      || rpad_fn(i_r_qoprc07.cust_id, 8)
                      || rpad_fn(i_r_qoprc07.legcy_ref, 25)
                      || RPAD(' ', 10)   -- invc_num
                      || RPAD(' ', 15)   -- cust_invc_num
                      || lpad_fn(i_r_qoprc07.ord_num, 11, '0')
                      || RPAD(' ', 14)   -- remaining bytes for ord_num (defined PIC X(25))
                      || TO_CHAR(NVL(i_r_qoprc07.ord_ln, 0), 'FM0999V99999')
                      || lpad_fn(i_r_qoprc07.catlg_num, 6, '0')
                      || lpad_fn(i_r_qoprc07.cbr_item, 9, '0')
                      || rpad_fn(i_r_qoprc07.uom, 3)
                      || lpad_fn(i_r_qoprc07.ord_qty, 7, '0')
                      || lpad_fn(i_r_qoprc07.pick_qty, 7, '0')
                      || lpad_fn(i_r_qoprc07.shp_qty, 7, '0')
                      || TO_CHAR(NVL(i_r_qoprc07.price_amt, 0), 'FM0999999V99')
                      || TO_CHAR(NVL(i_r_qoprc07.rtl_amt, 0), 'FM0999999V99')
                      || lpad_fn(i_r_qoprc07.rtl_mult, 5, '0')
                      || rpad_fn(i_r_qoprc07.not_shp_rsn, 8)
                      || lpad_fn(i_r_qoprc07.mfst_catg, 3, '0')
                      || lpad_fn(i_r_qoprc07.tote_catg, 3, '0')
                      || lpad_fn(i_r_qoprc07.orig_cbr_item, 9, '0')
                      || rpad_fn(i_r_qoprc07.orig_uom, 3)
                      || rpad_fn(i_r_qoprc07.orig_not_shp_rsn, 3)
                      || 'N'   -- restk_fee_sw
                      || lpad_fn(i_r_qoprc07.invc_catg, 3, '0')
                      || lpad_fn(i_r_qoprc07.lbl_catg, 3, '0')
                      || rpad_fn(i_r_qoprc07.pick_slot, 7)
                      || TO_CHAR(i_r_qoprc07.eta_ts, 'YYYYMMDD')
                      || rpad_fn(i_r_qoprc07.po_num, 30)
                      || rpad_fn(i_r_qoprc07.cust_pass_area, 25)
                      || rpad_fn(i_r_qoprc07.item_pass_area, 25)
                      || lpad_fn(i_r_qoprc07.mcl_cust, 6, '0')
                      || NVL(i_r_qoprc07.hard_rtl_sw, 'N')
                      || NVL(i_r_qoprc07.hard_price_sw, 'N')
                      || ' '   -- kit_sw
                      || RPAD(i_r_qoprc07.ord_typ, 3)
                      || rpad_fn(i_r_qoprc07.cust_item, 10)
                      || lpad_fn(i_r_qoprc07.orig_catlg_num, 6, '0')
                      || RPAD(NVL(i_r_qoprc07.load_typ, 'GRO'), 3)
                      || TO_CHAR(i_r_qoprc07.eta_ts, 'HH24MI')
                      || rpad_fn(i_r_qoprc07.no_ord_sw, 1)
                      || rpad_fn(i_r_qoprc07.conf_num, 8)
                      || rpad_fn(i_r_qoprc07.ord_comnt, 25)
                      || TO_CHAR(i_r_qoprc07.llr_ts, 'YYYY-MM-DD')
                      || rpad_fn(i_r_qoprc07.prepost_sw, 1)
                      || LPAD(i_r_qoprc07.depart_seq, 2, '0')
                      || ' '   -- ovrrd_rtl_sw
                      || rpad_fn(i_r_qoprc07.inv_zone, 3)
                      || rpad_fn(i_r_qoprc07.inv_slot, 7)
                      || rpad_fn(i_r_qoprc07.cig_sel_cd, 1, 'X')
                      || rpad_fn(i_r_qoprc07.hand_stamp_sw, 1)
                      || rpad_fn(i_r_qoprc07.stamp_list, 42)   -- StampItem||StampApldCd (repeated 6 times)
                      || lpad_fn(i_r_qoprc07.cust_tax_jrsdctn, 6, '0')
                      || i_r_qoprc07.partl_dist_sw;
    END format_qoprc07_ln_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Open the UTL Files');
    l_qoprc06_file := UTL_FILE.fopen(l_c_file_dir, l_c_qoprc06_fname, 'w');
    l_qoprc07_file := UTL_FILE.fopen(l_c_file_dir, l_c_qoprc07_fname, 'w');
    logs.dbg('Open Order Extract Cursor');

    OPEN l_cur_qoprc07(i_r_rlse.div_part,
                       i_r_rlse.rlse_id,
                       i_r_rlse.llr_dt,
                       i_r_rlse.load_list,
                       i_r_rlse.tran_part_id,
                       i_r_rlse.test_bil_cd,
                       g_t_rstr_out_qty_crps
                      );

    <<cursor_loop>>
    LOOP
      logs.dbg('Fetch Order Extract Cursor');

      FETCH l_cur_qoprc07
      BULK COLLECT INTO l_t_qoprc07 LIMIT 1000;

      EXIT cursor_loop WHEN l_t_qoprc07.COUNT = 0;
      <<qoprc07_tbl_loop>>
      FOR i IN l_t_qoprc07.FIRST .. l_t_qoprc07.LAST LOOP
        l_cnt := l_cnt + 1;
        logs.dbg('Write QOPRC06 File');
        write_qoprc06_sp(i_r_rlse.div_id,
                         i_r_rlse.div_part,
                         l_t_qoprc07(i).ord_num,
                         l_t_qoprc07(i).ord_ln,
                         l_t_qoprc07(i).load_num,
                         l_t_qoprc07(i).stop_num,
                         l_t_qoprc07(i).catlg_num
                        );
        logs.dbg('Check for Empty Slots');
        check_empty_slots_sp(i_r_rlse.div_part, i_r_rlse.rlse_id, i_r_rlse.tran_part_id, l_t_qoprc07(i));

        IF (    l_t_qoprc07(i).pick_slot IS NULL
            AND l_t_qoprc07(i).pick_qty > 0) THEN
          l_missing_slot_cnt := l_missing_slot_cnt + 1;
        END IF;   -- l_t_qoprc07(i).pick_slot IS NULL AND l_t_qoprc07(i).pick_qty > 0

        logs.dbg('Format QOPRC07 Line');
        format_qoprc07_ln_sp(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_t_qoprc07(i), l_qoprc07_ln);
        logs.dbg('Write QOPRC07 File');

        IF l_cnt = 1 THEN
          UTL_FILE.putf(l_qoprc07_file, l_qoprc07_ln);
        ELSE
          UTL_FILE.putf(l_qoprc07_file, '\n' || l_qoprc07_ln);
        END IF;   -- l_cnt = 1
      END LOOP qoprc07_tbl_loop;
    END LOOP cursor_loop;

    CLOSE l_cur_qoprc07;

    logs.dbg('Close the UTL Files');
    UTL_FILE.fflush(l_qoprc06_file);
    UTL_FILE.fclose(l_qoprc06_file);
    UTL_FILE.fflush(l_qoprc07_file);
    UTL_FILE.fclose(l_qoprc07_file);
    logs.dbg('Upd Release Entry with Order Line Count');

    UPDATE rlse_op1z r
       SET r.ord_ln_cnt = l_cnt
     WHERE r.div_part = i_r_rlse.div_part
       AND r.rlse_ts = i_r_rlse.rlse_ts;

    COMMIT;

    IF l_missing_slot_cnt > 0 THEN
      logs.err('Missing Slots in QOPRC07! Occurrences: ' || l_missing_slot_cnt, lar_parm, NULL, FALSE);
    END IF;   -- l_missing_slot_cnt > 0

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_qoprc07%ISOPEN THEN
        CLOSE l_cur_qoprc07;
      END IF;   -- l_cur_qoprc07%ISOPEN

      IF UTL_FILE.is_open(l_qoprc06_file) THEN
        UTL_FILE.fflush(l_qoprc06_file);
        UTL_FILE.fclose(l_qoprc06_file);
      END IF;   -- UTL_FILE.is_open(l_qoprc06_file)

      IF UTL_FILE.is_open(l_qoprc07_file) THEN
        UTL_FILE.fflush(l_qoprc07_file);
        UTL_FILE.fclose(l_qoprc07_file);
      END IF;   -- UTL_FILE.is_open(l_qoprc07_file)

      logs.err(lar_parm);
  END extr_ords_sp;
  PROCEDURE extr_ords_sp2(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm  := 'OP_ALLOCATE_PK.EXTR_ORDS_SP';
    lar_parm                    logs.tar_parm;
    l_t_qoprc07_lns             typ.tas_maxvc2;
    l_c_file_dir       CONSTANT VARCHAR2(50)   := '/ftptrans';
    l_c_qoprc06_fname  CONSTANT VARCHAR2(50)   := i_r_rlse.div_id || '_CONTAINER_ORDERS_' || i_r_rlse.rlse_ts_char;
    l_c_qoprc07_fname  CONSTANT VARCHAR2(50)   := i_r_rlse.div_id || '_ALLOCATE_ORDERS_' || i_r_rlse.rlse_ts_char;
    l_cnt                       PLS_INTEGER    := 0;
    l_missing_slot_cnt          PLS_INTEGER    := 0;

    CURSOR l_cur_qoprc07(
      b_div_part             NUMBER,
      b_rlse_id              NUMBER,
      b_llr_dt               DATE,
      b_load_list            VARCHAR2,
      b_part_id              NUMBER,
      b_test_bil_cd          VARCHAR2,
      b_t_rstr_out_qty_crps  type_stab
    ) IS
      SELECT   /*+ NO_PARALLEL */
               x.ord_src, DECODE(b_test_bil_cd, '~', 'HRD', 'TST') AS bil_run_typ, x.load_num, x.stop_num, x.cust_id,
               x.legcy_ref, x.ord_num, x.ord_ln, x.catlg_num, x.cbr_item, x.uom, x.ord_qty, x.pick_qty,
               x.pick_qty AS shp_qty, x.price_amt, x.rtl_amt, x.rtl_mult,
               (CASE
                  WHEN(    x.not_shp_rsn IS NULL
                       AND x.ord_qty > 0
                       AND NVL(x.pick_qty, 0) = 0) THEN DECODE(x.ord_typ, 'D', 'DISOUT', 'INVOUT')
                  ELSE x.not_shp_rsn
                END
               ) AS not_shp_rsn,
               x.mfst_catg, x.tote_catg, NVL(x.orig_cbr_item, x.cbr_item) AS orig_cbr_item,
               NVL(x.orig_uom, x.uom) AS orig_uom,
               DECODE(NVL(x.sub_cd, 0), 1, 'UNC', 2, 'REP', 3, 'RND', 4, 'CON', 5, 'SUB', '   ') AS orig_not_shp_rsn,
               x.invc_catg, x.lbl_catg, x.pick_slot, x.eta_ts,
               NVL((SELECT p.po_num
                      FROM bill_po_ovride_bc1p p
                     WHERE p.div_part = b_div_part
                       AND p.ord_num = x.ord_num
                       AND p.ord_ln_num = x.ord_ln),
                   (CASE
                      WHEN x.po_num = 'STANDING' THEN ' '
                      WHEN EXISTS(SELECT 1
                                    FROM mclp300d md
                                   WHERE md.div_part = b_div_part
                                     AND md.ordnod = x.ord_num
                                     AND md.reasnd = 'SPLITORD') THEN ' '
                      ELSE x.po_num
                    END
                   )
                  ) AS po_num,
               x.cust_pass_area, x.item_pass_area, x.mcl_cust, x.hard_rtl_sw, x.hard_price_sw,
               DECODE(x.ord_typ, 'D', 'DIS', 'REG') AS ord_typ, x.cust_item,
               NVL(x.orig_item, x.catlg_num) AS orig_catlg_num, x.load_typ,
               DECODE(x.ord_typ, 'N', 'Y', 'N') AS no_ord_sw, x.conf_num, x.comnt AS ord_comnt, x.llr_ts, x.prepost_sw,
               DENSE_RANK() OVER(ORDER BY x.depart_ts, x.load_num) AS depart_seq, x.inv_zone,
               (CASE
                  WHEN x.uom IN('CII', 'CIC', 'CIR') THEN NVL(x.inv_slot, x.pick_slot)
                END) AS inv_slot, x.cig_sel_cd, x.hand_stamp_sw, x.cust_tax_jrsdctn, x.stamp_list
          FROM (SELECT oa.ipdtsa AS ord_src, ld.load_num, ld.depart_ts, se.stop_num, se.cust_id,
                       oa.legrfa AS legcy_ref, ob.ordnob AS ord_num, ob.lineb AS ord_ln, ob.orditb AS catlg_num,
                       ob.itemnb AS cbr_item, ob.sllumb AS uom,
                       (CASE
                          WHEN(    pm.crp_cd = cx.corpb
                               AND ob.ordqtb < ob.orgqtb
                               AND NVL(ob.ntshpb, 'INVOUT') IN('INVOUT', 'QTYZERO')
                               AND oa.dsorda = 'R'
                               AND NVL(ob.pckqtb, 0) = 0
                               AND ob.orgqtb > 0
                              ) THEN ob.orgqtb
                          ELSE ob.ordqtb
                        END
                       ) AS ord_qty,
                       ob.pckqtb AS pick_qty, ob.hdprcb AS price_amt, ob.hdrtab AS rtl_amt, ob.hdrtmb AS rtl_mult,
                       (CASE
                          WHEN(    pm.crp_cd = cx.corpb
                               AND ob.ordqtb = 0
                               AND ob.orgqtb > 0) THEN 'ITMSTRST'
                          ELSE ob.ntshpb
                        END
                       ) AS not_shp_rsn,
                       ob.manctb AS mfst_catg, ob.totctb AS tote_catg, ob.subrcb AS sub_cd, ob.invctb AS invc_catg,
                       ob.labctb AS lbl_catg, se.eta_ts, oa.cpoa AS po_num, oa.cspasa AS cust_pass_area,
                       ob.itpasb AS item_pass_area, cx.mccusb AS mcl_cust, ob.rtfixb AS hard_rtl_sw,
                       ob.prfixb AS hard_price_sw, oa.dsorda AS ord_typ, ob.cusitb AS cust_item,
                       ob.orgitb AS orig_item, oa.ldtypa AS load_typ, oa.connba AS conf_num, oc.commc AS comnt,
                       e.iteme AS orig_cbr_item, e.uome AS orig_uom, t.pick_slot,
                       (SELECT DECODE(MAX(pp.cust_num), NULL, 'N', 'Y')
                          FROM prepost_load_op1p pp
                         WHERE pp.div_part = b_div_part
                           AND pp.load_num = ld.load_num
                           AND pp.stop_num = se.stop_num
                           AND pp.cust_num = se.cust_id
                           AND pp.llr_date = b_llr_dt) AS prepost_sw,
                       ld.llr_ts, t.inv_zone, t.inv_slot, ld.load_depart_sid, NVL(t.cig_sel_cd, 'X') AS cig_sel_cd,
                       t.hand_stamp_sw, t.cust_tax_jrsdctn,t.stamp_list
                  FROM load_depart_op1f ld, ordp100a oa, stop_eta_op1g se, ordp120b ob, ordp140c oc, mclp020b cx,
                       sawp505e e, (SELECT TO_NUMBER(t.column_value) AS crp_cd
                                      FROM TABLE(CAST(b_t_rstr_out_qty_crps AS type_stab)) t) pm,
                       (SELECT op2o.ord_num, op2o.ord_ln,
                               op2i.pick_aisle || op2i.pick_bin || op2i.pick_lvl AS pick_slot,
                               DECODE(op2i.inv_zone, '~', ' ', op2i.inv_zone) AS inv_zone,
                               op2i.inv_aisle || op2i.inv_bin || op2i.inv_lvl AS inv_slot, op2i.cig_sel_cd,
                               op2i.hand_stamp_sw, op2i.cust_tax_jrsdctn,
                               to_list_fn(CURSOR(SELECT LPAD(op2c.stamp_item, 6, '0') || op2c.stamp_apld_cd
                                                   FROM tran_stamp_op2c op2c
                                                  WHERE op2c.div_part = op2t.div_part
                                                    AND op2c.tran_id = op2t.tran_id
                                                    AND op2c.part_id = op2t.part_id
                                                ),
                                          NULL
                                         ) AS stamp_list
                          FROM tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i
                         WHERE op2t.div_part = b_div_part
                           AND op2t.rlse_id = b_rlse_id
                           AND op2t.part_id = b_part_id
                           AND op2o.div_part = op2t.div_part
                           AND op2o.tran_id = op2t.tran_id
                           AND op2o.part_id = op2t.part_id
                           AND op2i.div_part = op2t.div_part
                           AND op2i.tran_id = op2t.tran_id
                           AND op2i.part_id = op2t.part_id
                           AND NOT EXISTS(SELECT 1
                                            FROM tran_stamp_op2c op2c
                                           WHERE op2c.div_part = op2i.div_part
                                             AND op2c.tran_id = op2i.tran_id
                                             AND op2c.part_id = op2i.part_id
                                             AND op2c.stamp_item = op2i.catlg_num)) t
                 WHERE ld.div_part = b_div_part
                   AND ld.llr_dt = b_llr_dt
                   AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                   AND oa.div_part = ld.div_part
                   AND oa.load_depart_sid = ld.load_depart_sid
                   AND oa.stata = 'P'
                   AND se.div_part = oa.div_part
                   AND se.load_depart_sid = oa.load_depart_sid
                   AND se.cust_id = oa.custa
                   AND oc.div_part = oa.div_part
                   AND oc.ordnoc = oa.ordnoa
                   AND oc.seqc = 0   -- only get first comment line
                   AND cx.div_part = oa.div_part
                   AND cx.custb = oa.custa
                   AND ob.div_part = oa.div_part
                   AND ob.ordnob = oa.ordnoa
                   AND ob.statb IN('P', 'T')
                   AND pm.crp_cd(+) = cx.corpb
                   AND e.catite(+) = ob.orgitb
                   AND t.ord_num(+) = ob.ordnob
                   AND t.ord_ln(+) = ob.lineb
                UNION ALL
                SELECT oa.ipdtsa AS ord_src, ld.load_num, ld.depart_ts, se.stop_num, se.cust_id, oa.legrfa AS legcy_ref,
                       ob.ordnob AS ord_num, FLOOR(ob.lineb) + .2 AS ord_ln, ob.orditb AS catlg_num,
                       ob.itemnb AS cbr_item, ob.sllumb AS uom, ob.orgqtb - NVL(ob.pckqtb, 0) AS ord_qty, 0 AS pick_qty,
                       ob.hdprcb AS price_amt, ob.hdrtab AS rtl_amt, ob.hdrtmb AS rtl_mult, 'ITMSTRST' AS not_shp_rsn,
                       ob.manctb AS mfst_catg, ob.totctb AS tote_catg, ob.subrcb AS sub_cd, ob.invctb AS invc_catg,
                       ob.labctb AS lbl_catg, se.eta_ts, oa.cpoa AS po_num, oa.cspasa AS cust_pass_area,
                       ob.itpasb AS item_pass_area, cx.mccusb AS mcl_cust, ob.rtfixb AS hard_rtl_sw,
                       ob.prfixb AS hard_price_sw, oa.dsorda AS ord_typ, ob.cusitb AS cust_item, ob.orgitb AS orig_item,
                       oa.ldtypa AS load_typ, oa.connba AS conf_num, oc.commc AS comnt, e.iteme AS orig_cbr_item,
                       e.uome AS orig_uom, NULL AS pick_slot,
                       (SELECT DECODE(MAX(pp.cust_num), NULL, 'N', 'Y')
                          FROM prepost_load_op1p pp
                         WHERE pp.div_part = b_div_part
                           AND pp.load_num = ld.load_num
                           AND pp.stop_num = se.stop_num
                           AND pp.cust_num = se.cust_id
                           AND pp.llr_date = b_llr_dt) AS prepost_sw,
                       ld.llr_ts, NULL AS inv_zone, NULL AS inv_slot, ld.load_depart_sid, 'X' AS cig_sel_cd,
                       NULL AS hand_stamp_sw, NULL AS cust_tax_jrsdctn, NULL AS stamp_list
                  FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd
                          FROM TABLE(CAST(b_t_rstr_out_qty_crps AS type_stab)) t) pm, load_depart_op1f ld, ordp100a oa,
                       stop_eta_op1g se, ordp120b ob, ordp140c oc, mclp020b cx, sawp505e e
                 WHERE ld.div_part = b_div_part
                   AND ld.llr_dt = b_llr_dt
                   AND INSTR(b_load_list, ',' || ld.load_num || ',') > 0
                   AND oa.div_part = ld.div_part
                   AND oa.load_depart_sid = ld.load_depart_sid
                   AND oa.stata = 'P'
                   AND se.div_part = oa.div_part
                   AND se.load_depart_sid = oa.load_depart_sid
                   AND se.cust_id = oa.custa
                   AND oc.div_part = oa.div_part
                   AND oc.ordnoc = oa.ordnoa
                   AND oc.seqc = 0   -- only get first comment line
                   AND cx.div_part = oa.div_part
                   AND cx.custb = oa.custa
                   AND cx.corpb = pm.crp_cd
                   AND ob.div_part = oa.div_part
                   AND ob.ordnob = oa.ordnoa
                   AND ob.ordqtb < ob.orgqtb
                   AND ob.ntshpb IS NULL
                   AND ob.pckqtb = ob.ordqtb
                   AND ob.ordqtb > 0
                   AND ob.statb IN('P', 'T')
                   AND e.catite(+) = ob.orgitb) x
      ORDER BY x.ord_num, x.ord_ln;

    TYPE l_tt_qoprc07 IS TABLE OF l_cur_qoprc07%ROWTYPE;

    l_t_qoprc07                 l_tt_qoprc07;

    PROCEDURE write_qoprc06_sp(
      i_div        IN  VARCHAR2,
      i_div_part   IN  NUMBER,
      i_ord_num    IN  NUMBER,
      i_ord_ln     IN  NUMBER,
      i_load_num   IN  VARCHAR2,
      i_stop_num   IN  NUMBER,
      i_catlg_num  IN  VARCHAR2
    ) IS
      l_kit_cd         VARCHAR2(1);
      l_t_cntnr_ids    type_stab;
      l_t_qtys         type_ntab;
      l_t_qoprc06_lns  typ.tas_maxvc2;
    BEGIN
      SELECT DECODE(MAX(k.item_num), NULL, ' ', 'A')
        INTO l_kit_cd
        FROM kit_item_mstr_kt1m k
       WHERE k.div_part = i_div_part
         AND k.comp_item_num = i_catlg_num;

      SELECT c.orig_cntnr_id, c.orig_qty
      BULK COLLECT INTO l_t_cntnr_ids, l_t_qtys
        FROM bill_cntnr_id_bc1c c
       WHERE c.div_part = i_div_part
         AND c.ord_num = i_ord_num
         AND c.ord_ln_num = i_ord_ln;

      IF l_t_cntnr_ids.COUNT > 0 THEN
        FOR i IN l_t_cntnr_ids.FIRST .. l_t_cntnr_ids.LAST LOOP
          l_t_qoprc06_lns(l_t_qoprc06_lns.COUNT + 1) := RPAD(i_div, 2)
                                                        || LPAD(i_ord_num, 11, '0')
                                                        || TO_CHAR(i_ord_ln, 'FM0999V99999')
                                                        || RPAD(l_t_cntnr_ids(i), 20)
                                                        || LPAD(l_t_qtys(i), 9, '0')
                                                        || LPAD(i_load_num, 4)
                                                        || LPAD(i_stop_num, 2, '0')
                                                        || LPAD(i_catlg_num, 6)
                                                        || l_kit_cd;
        END LOOP;
        write_sp(l_t_qoprc06_lns, l_c_qoprc06_fname, l_c_file_dir, 'A');
      END IF;   -- l_t_cntnr_ids.COUNT > 0
    END write_qoprc06_sp;

    PROCEDURE check_empty_slots_sp(
      i_div_part    IN      NUMBER,
      i_rlse_id     IN      NUMBER,
      i_part_id     IN      NUMBER,
      io_r_qoprc07  IN OUT  l_cur_qoprc07%ROWTYPE
    ) IS
      l_cv  SYS_REFCURSOR;
    BEGIN
      IF (    io_r_qoprc07.pick_slot IS NULL
          AND io_r_qoprc07.pick_qty > 0) THEN
        logs.warn('Missing Slot for Allocated Order Line'
                  || cnst.newline_char
                  || ' OrdNum: '
                  || util.to_str(io_r_qoprc07.ord_num)
                  || ' OrdLn: '
                  || util.to_str(FLOOR(io_r_qoprc07.ord_ln))
                  || ' CatlgNum: '
                  || util.to_str(io_r_qoprc07.catlg_num),
                  lar_parm
                 );

        OPEN l_cv
         FOR
           SELECT op2i.pick_aisle || op2i.pick_bin || op2i.pick_lvl AS pick_slot,
                  DECODE(op2i.inv_zone, '~', ' ', op2i.inv_zone) AS inv_zone,
                  (CASE
                     WHEN e.uome IN('CII', 'CIC', 'CIR') THEN NVL(op2i.inv_aisle || op2i.inv_bin || op2i.inv_lvl,
                                                                  op2i.pick_aisle || op2i.pick_bin || op2i.pick_lvl
                                                                 )
                   END
                  ) AS inv_slot,
                  NVL(op2i.cig_sel_cd, 'X') AS cig_sel_cd, op2i.hand_stamp_sw, op2i.cust_tax_jrsdctn,
                  to_list_fn(CURSOR(SELECT LPAD(op2c.stamp_item, 6, '0') || op2c.stamp_apld_cd
                                      FROM tran_stamp_op2c op2c
                                     WHERE op2c.div_part = op2t.div_part
                                       AND op2c.tran_id = op2t.tran_id
                                       AND op2c.part_id = op2t.part_id
                                   ),
                             NULL
                            ) AS stamp_list
             FROM tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i, sawp505e e
            WHERE op2t.div_part = i_div_part
              AND op2t.rlse_id = i_rlse_id
              AND op2t.part_id = i_part_id
              AND op2o.div_part = op2t.div_part
              AND op2o.tran_id = op2t.tran_id
              AND op2o.part_id = op2t.part_id
              AND op2o.ord_num = io_r_qoprc07.ord_num
              AND op2o.ord_ln = io_r_qoprc07.ord_ln
              AND op2i.div_part = op2t.div_part
              AND op2i.tran_id = op2t.tran_id
              AND op2i.part_id = op2t.part_id
              AND e.catite = op2i.catlg_num
              AND NOT EXISTS(SELECT 1
                               FROM tran_stamp_op2c op2c
                              WHERE op2c.div_part = op2i.div_part
                                AND op2c.tran_id = op2i.tran_id
                                AND op2c.part_id = op2i.part_id
                                AND op2c.stamp_item = op2i.catlg_num);

        FETCH l_cv
         INTO io_r_qoprc07.pick_slot, io_r_qoprc07.inv_zone, io_r_qoprc07.inv_slot, io_r_qoprc07.cig_sel_cd,
              io_r_qoprc07.hand_stamp_sw, io_r_qoprc07.cust_tax_jrsdctn, io_r_qoprc07.stamp_list;
      END IF;   -- io_r_qoprc07.pick_slot IS NULL AND io_r_qoprc07.pick_qty > 0
    END check_empty_slots_sp;

    FUNCTION qoprc07_ln_fn(
      i_div        IN  VARCHAR2,
      i_rlse_ts    IN  DATE,
      i_r_qoprc07  IN  l_cur_qoprc07%ROWTYPE
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN(RPAD(i_div, 2)
             || 'QGROPB  '
             || RPAD(TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISSSSS'), 30)
             || 'ADD'
             || rpad_fn(i_r_qoprc07.ord_src, 3)
             || RPAD(' ', 7)
             || RPAD(i_r_qoprc07.bil_run_typ, 3)
             || rpad_fn(i_r_qoprc07.load_num, 4)
             || lpad_fn(i_r_qoprc07.stop_num, 2, '0')
             || rpad_fn(i_r_qoprc07.cust_id, 8)
             || rpad_fn(i_r_qoprc07.legcy_ref, 25)
             || RPAD(' ', 10)   -- invc_num
             || RPAD(' ', 15)   -- cust_invc_num
             || lpad_fn(i_r_qoprc07.ord_num, 11, '0')
             || RPAD(' ', 14)   -- remaining bytes for ord_num (defined PIC X(25))
             || TO_CHAR(NVL(i_r_qoprc07.ord_ln, 0), 'FM0999V99999')
             || lpad_fn(i_r_qoprc07.catlg_num, 6, '0')
             || lpad_fn(i_r_qoprc07.cbr_item, 9, '0')
             || rpad_fn(i_r_qoprc07.uom, 3)
             || lpad_fn(i_r_qoprc07.ord_qty, 7, '0')
             || lpad_fn(i_r_qoprc07.pick_qty, 7, '0')
             || lpad_fn(i_r_qoprc07.shp_qty, 7, '0')
             || TO_CHAR(NVL(i_r_qoprc07.price_amt, 0), 'FM0999999V99')
             || TO_CHAR(NVL(i_r_qoprc07.rtl_amt, 0), 'FM0999999V99')
             || lpad_fn(i_r_qoprc07.rtl_mult, 5, '0')
             || rpad_fn(i_r_qoprc07.not_shp_rsn, 8)
             || lpad_fn(i_r_qoprc07.mfst_catg, 3, '0')
             || lpad_fn(i_r_qoprc07.tote_catg, 3, '0')
             || lpad_fn(i_r_qoprc07.orig_cbr_item, 9, '0')
             || rpad_fn(i_r_qoprc07.orig_uom, 3)
             || rpad_fn(i_r_qoprc07.orig_not_shp_rsn, 3)
             || 'N'   -- restk_fee_sw
             || lpad_fn(i_r_qoprc07.invc_catg, 3, '0')
             || lpad_fn(i_r_qoprc07.lbl_catg, 3, '0')
             || rpad_fn(i_r_qoprc07.pick_slot, 7)
             || TO_CHAR(i_r_qoprc07.eta_ts, 'YYYYMMDD')
             || rpad_fn(i_r_qoprc07.po_num, 30)
             || rpad_fn(i_r_qoprc07.cust_pass_area, 25)
             || rpad_fn(i_r_qoprc07.item_pass_area, 25)
             || lpad_fn(i_r_qoprc07.mcl_cust, 6, '0')
             || NVL(i_r_qoprc07.hard_rtl_sw, 'N')
             || NVL(i_r_qoprc07.hard_price_sw, 'N')
             || ' '   -- kit_sw
             || RPAD(i_r_qoprc07.ord_typ, 3)
             || rpad_fn(i_r_qoprc07.cust_item, 10)
             || lpad_fn(i_r_qoprc07.orig_catlg_num, 6, '0')
             || RPAD(NVL(i_r_qoprc07.load_typ, 'GRO'), 3)
             || TO_CHAR(i_r_qoprc07.eta_ts, 'HH24MI')
             || rpad_fn(i_r_qoprc07.no_ord_sw, 1)
             || rpad_fn(i_r_qoprc07.conf_num, 8)
             || rpad_fn(i_r_qoprc07.ord_comnt, 25)
             || TO_CHAR(i_r_qoprc07.llr_ts, 'YYYY-MM-DD')
             || rpad_fn(i_r_qoprc07.prepost_sw, 1)
             || LPAD(i_r_qoprc07.depart_seq, 2, '0')
             || ' '   -- ovrrd_rtl_sw
             || rpad_fn(i_r_qoprc07.inv_zone, 3)
             || rpad_fn(i_r_qoprc07.inv_slot, 7)
             || rpad_fn(i_r_qoprc07.cig_sel_cd, 1, 'X')
             || rpad_fn(i_r_qoprc07.hand_stamp_sw, 1)
             || rpad_fn(i_r_qoprc07.stamp_list, 42)   -- StampItem||StampApldCd (repeated 6 times)
             || NULL
            );
    END qoprc07_ln_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Open Order Extract Cursor');

    OPEN l_cur_qoprc07(i_r_rlse.div_part,
                       i_r_rlse.rlse_id,
                       i_r_rlse.llr_dt,
                       i_r_rlse.load_list,
                       i_r_rlse.tran_part_id,
                       i_r_rlse.test_bil_cd,
                       g_t_rstr_out_qty_crps
                      );

    <<cursor_loop>>
    LOOP
      logs.dbg('Fetch Order Extract Cursor');

      FETCH l_cur_qoprc07
      BULK COLLECT INTO l_t_qoprc07 LIMIT 100;

      EXIT cursor_loop WHEN l_t_qoprc07.COUNT = 0;
      <<qoprc07_tbl_loop>>
      FOR i IN l_t_qoprc07.FIRST .. l_t_qoprc07.LAST LOOP
        l_cnt := l_cnt + 1;
        logs.dbg('Write QOPRC06 File');
        write_qoprc06_sp(i_r_rlse.div_id,
                         i_r_rlse.div_part,
                         l_t_qoprc07(i).ord_num,
                         l_t_qoprc07(i).ord_ln,
                         l_t_qoprc07(i).load_num,
                         l_t_qoprc07(i).stop_num,
                         l_t_qoprc07(i).catlg_num
                        );
        logs.dbg('Check for Empty Slots');
        check_empty_slots_sp(i_r_rlse.div_part, i_r_rlse.rlse_id, i_r_rlse.tran_part_id, l_t_qoprc07(i));

        IF (    l_t_qoprc07(i).pick_slot IS NULL
            AND l_t_qoprc07(i).pick_qty > 0) THEN
          l_missing_slot_cnt := l_missing_slot_cnt + 1;
        END IF;   -- l_t_qoprc07(i).pick_slot IS NULL AND l_t_qoprc07(i).pick_qty > 0

        logs.dbg('Append Formatted QOPRC07 Line');
        l_t_qoprc07_lns(l_t_qoprc07_lns.COUNT + 1) := qoprc07_ln_fn(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_t_qoprc07(i));
      END LOOP qoprc07_tbl_loop;
      logs.dbg('Write QOPRC07 File');
      write_sp(l_t_qoprc07_lns, l_c_qoprc07_fname, l_c_file_dir, 'A');
    END LOOP cursor_loop;

    CLOSE l_cur_qoprc07;

    logs.dbg('Upd Release Entry with Order Line Count');

    UPDATE rlse_op1z r
       SET r.ord_ln_cnt = l_cnt
     WHERE r.div_part = i_r_rlse.div_part
       AND r.rlse_ts = i_r_rlse.rlse_ts;

    COMMIT;

    IF l_missing_slot_cnt > 0 THEN
      logs.err('Missing Slots in QOPRC07! Occurrences: ' || l_missing_slot_cnt, lar_parm, NULL, FALSE);
    END IF;   -- l_missing_slot_cnt > 0

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_qoprc07%ISOPEN THEN
        CLOSE l_cur_qoprc07;
      END IF;   -- l_cur_qoprc07%ISOPEN

      logs.err(lar_parm);
  END extr_ords_sp2;

  /*
  ||----------------------------------------------------------------------------
  || TOTE_FCAST_SP
  ||  This procedure will build the Tote Forecast entries for the
  ||  Release of Orders. Orders for the release are summarized for entry into
  ||  the Tote Forecast Table using formulas that are specific for Tote
  ||  Calculations.
  ||  There are 2 primary calculations to determine how many totes are
  ||  calculated for a Tote Category / Load / Stop
  ||  1)  Plastic Tote calculations take the total extended cube for
  ||      Allocated orders within a Tote Category and divides by the Inner
  ||      Cube of the Tote.  The result is always rounded up to the next
  ||      whole number - this is the number of totes for that
  ||      Tote Category / Load / Stop.  The calculated number of totes then
  ||      multiplied by the Outer Cube to get the "Cube of Totes".
  ||
  ||  2)  Cigarette Box calculations are made by taking the number of cartons
  ||      allocated and dividing it by the number of cartons in a "full box".
  ||      The remainder of this calculation is then accumulated with all of
  ||      the "other remainders" and then divided by a "common box quantity"
  ||      for that tote category. This result is always rounded up to the
  ||      next whole number - this is the number of Mixed boxes.
  ||      The number of full boxes is then added to the number of mixed boxes
  ||      - this is the total number of boxes for that
  ||      Tote Category / Load / Stop.  The "Box" Tote Categories generally
  ||      have a flag set that tells the system to not use the Outer Cube for
  ||      the "Cube of Totes" extension - instead it needs to use the actual
  ||      product cube (this is because they sometimes replace boxes with
  ||      bags).
  ||
  ||  Entries are also created in the Tote Forecast table for non-allocated
  ||  orders.  This is for the Ship Confirm process to use for Invoices with
  ||  no allocated orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | rhalpai | Updated to calculate bags in addition to boxes. Updating
  ||                    | MCLP370C.BAGSMC with new MCLP200B.MAX_BAG_QTY column.
  || 12/13/02 | rhalpai | Added logic for new test_bil_load_sw column on MLCP370C.
  || 02/21/03 | rhalpai | Changed Inserts and Updates of MCLP370C to use status of
  ||                    | 'P'ending to prevent closing of loads being Allocated
  ||                    | until billing completes and the release complete script
  ||                    | resets the 'P' status's back to 'R's or if this is a
  ||                    | Test Bill then at the end of Allocate.
  || 04/04/03 | rhalpai | Changed to use new RLSE_TS when doing inserts and
  ||                    | updates for MCLP370C with the current release timestamp.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 10/20/06 | rhalpai | Changed to use MERGE statements instead of INSERT/UPDATE
  ||                    | Eliminated redundant INSERT for "NULL Entry" which would
  ||                    | always result in a "Duplicate Values" error which would
  ||                    | increment a redundant-insert counter variable. Eliminated
  ||                    | the redundant-insert counter variable as it was not used.
  ||                    | Changed to use summarized data on MCLANE_MANIFEST_RPTS
  ||                    | instead of using ORDP120B. The data on MCLANE_MANIFEST_RPTS
  ||                    | is built from OP_MANIFEST_REPORTS_PK.BUILD_REPORT_TABLE_SP
  ||                    | and now uses container counts from Billing Container ID
  ||                    | table when division has "Cubing of Totes" turned on.
  ||                    | Converted a cursor loop with an INSERT to just an INSERT.
  ||                    | Added insert to new LOAD_CLOS_CNTRL_BC2C table.
  ||                    | PIR3209
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Replaced LLRDate input parm with ReleaseTS.
  ||                    | Changed cursors to use MCLANE_LOAD_LABEL_RLSE for
  ||                    | matching load list and LLR date.
  || 08/04/09 | rhalpai | Add logic to include PICK_COMPL_SW defaulted to
  ||                    | XDOCK_PICK_COMPL value when adding
  ||                    | LOAD_CLOS_CNTRL_BC2C entries. Set PICK_COMPL_SW to N
  ||                    | for loads between XDOCK_LOAD_BEG_xx and
  ||                    | XDOCK_LOAD_END_xx for order lines in current release
  ||                    | with mfst catgs not matching XDOCK_MFST_###.
  ||                    | Add call to OP_PICK_CONFIRM_PK.PICK_COMPL_SP for new
  ||                    | XDock loads in release with only catgs matching
  ||                    | XDOCK_MFST_###. PIR7342
  || 12/02/09 | rhalpai | Convert identification of XDOCK Loads in cursor for
  ||                    | Pick Complete Parm List from using parms for load
  ||                    | ranges to parms for non-contiguous loads. Removed
  ||                    | restriction for non-TBills. PIR7342
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 09/07/10 | rhalpai | Change logic to set Catchweight Complete switch based
  ||                    | on whether any Catchweight items were included on
  ||                    | load. PIR10251
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/20/14 | rhalpai | Change logic to update CwtComplSw to N when LLR/Load
  ||                    | already exists with CwtComplSw set to Y and there now
  ||                    | exists Catchweight items on the Load. PIR12765
  || 10/14/17 | rhalpai | Remove call to get parms as it is now referenced in a
  ||                    | global variable and loaded earlier.
  ||                    | Change logic to use global variable containing nested
  ||                    | table of parm values. PIR15427
  || 11/21/17 | rhalpai | Change logic to set ACS flag to Y when nothing
  ||                    | allocated for Load and set ACS flag to N when Y and
  ||                    | Load is included in new release with allocated order
  ||                    | lines. SDHD-86358
  || 07/01/19 | rhalpai | Add default entries for Peco pallet count. PIR19620
  ||----------------------------------------------------------------------------
  */
  PROCEDURE tote_fcast_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.TOTE_FCAST_SP';
    lar_parm                logs.tar_parm;
    l_cv                    SYS_REFCURSOR;
    l_pick_compl_parm_list  typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();

    IF g_cwt_compl_sw = 'Y' THEN
      logs.dbg('Upd CwtComplSw for existing Loads');

      UPDATE load_clos_cntrl_bc2c lc
         SET lc.cwt_compl_sw = 'N'
       WHERE lc.div_part = i_r_rlse.div_part
         AND lc.llr_dt = i_r_rlse.llr_dt
         AND lc.cwt_compl_sw = 'Y'
         AND lc.load_num IN(SELECT ld.load_num
                              FROM load_depart_op1f ld
                             WHERE ld.div_part = i_r_rlse.div_part
                               AND ld.llr_dt = i_r_rlse.llr_dt
                               AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                               AND EXISTS(SELECT 1
                                            FROM tran_op2t t, tran_ord_op2o o, ordp120b b, mclp110b di, ordp100a a
                                           WHERE t.div_part = i_r_rlse.div_part
                                             AND t.rlse_id = i_r_rlse.rlse_id
                                             AND o.div_part = t.div_part
                                             AND o.tran_id = t.tran_id
                                             AND o.ord_num = b.ordnob
                                             AND o.ord_ln = b.lineb
                                             AND di.div_part = b.div_part
                                             AND di.itemb = b.itemnb
                                             AND di.uomb = b.sllumb
                                             AND di.cwt_sw = 'Y'
                                             AND a.div_part = b.div_part
                                             AND a.ordnoa = b.ordnob
                                             AND a.load_depart_sid = ld.load_depart_sid));
    END IF;   -- g_cwt_compl_sw = 'Y'

    logs.dbg('Upd AcsLoadClosSw for existing Loads now with Allocated OrdLns');

    UPDATE load_clos_cntrl_bc2c lc
       SET lc.acs_load_clos_sw = 'N'
     WHERE lc.div_part = i_r_rlse.div_part
       AND lc.llr_dt = i_r_rlse.llr_dt
       AND lc.acs_load_clos_sw = 'Y'
       AND lc.load_num IN(SELECT ld.load_num
                            FROM load_depart_op1f ld
                           WHERE ld.div_part = i_r_rlse.div_part
                             AND ld.llr_dt = i_r_rlse.llr_dt
                             AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
                             AND EXISTS(SELECT 1
                                          FROM ordp100a a, ordp120b b, tran_op2t t, tran_ord_op2o o
                                         WHERE a.div_part = i_r_rlse.div_part
                                           AND a.load_depart_sid = ld.load_depart_sid
                                           AND b.div_part = a.div_part
                                           AND b.ordnob = a.ordnoa
                                           AND t.div_part = i_r_rlse.div_part
                                           AND t.rlse_id = i_r_rlse.rlse_id
                                           AND o.div_part = t.div_part
                                           AND o.tran_id = t.tran_id
                                           AND o.ord_num = b.ordnob
                                           AND o.ord_ln = b.lineb));

    logs.dbg('Insert LOAD_CLOS_CNTRL_BC2C Entries');

    INSERT INTO load_clos_cntrl_bc2c
                (div_part, llr_dt, load_num, dspstn_err_sw, pct_dscrpncy, load_status, test_bil_load_sw,
                 acs_load_clos_sw, pick_compl_sw, cwt_compl_sw)
      SELECT ld.div_part, ld.llr_dt, ld.load_num, 'N', 0, 'P', DECODE(i_r_rlse.test_bil_cd, '~', 'N', 'Y'),
             (CASE
                WHEN EXISTS(SELECT 1
                              FROM ordp100a a, ordp120b b, tran_op2t t, tran_ord_op2o o
                             WHERE a.div_part = ld.div_part
                               AND a.load_depart_sid = ld.load_depart_sid
                               AND b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND t.div_part = ld.div_part
                               AND t.rlse_id = i_r_rlse.rlse_id
                               AND o.div_part = t.div_part
                               AND o.tran_id = t.tran_id
                               AND o.ord_num = b.ordnob
                               AND o.ord_ln = b.lineb) THEN 'N'
                ELSE 'Y'
              END
             ),
             'N',
             (CASE
                WHEN g_cwt_compl_sw = 'N' THEN 'Y'
                WHEN EXISTS(SELECT 1
                              FROM ordp100a a, ordp120b b, mclp110b di, tran_op2t t, tran_ord_op2o o
                             WHERE a.div_part = ld.div_part
                               AND a.load_depart_sid = ld.load_depart_sid
                               AND b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND di.div_part = b.div_part
                               AND di.itemb = b.itemnb
                               AND di.uomb = b.sllumb
                               AND di.cwt_sw = 'Y'
                               AND t.div_part = ld.div_part
                               AND t.rlse_id = i_r_rlse.rlse_id
                               AND o.div_part = t.div_part
                               AND o.tran_id = t.tran_id
                               AND o.ord_num = b.ordnob
                               AND o.ord_ln = b.lineb) THEN 'N'
                ELSE 'Y'
              END
             )
        FROM load_depart_op1f ld
       WHERE ld.div_part = i_r_rlse.div_part
         AND ld.llr_dt = i_r_rlse.llr_dt
         AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
         AND EXISTS(SELECT 1
                      FROM ordp100a a, ordp120b b
                     WHERE a.div_part = ld.div_part
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.stata = 'P'
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb NOT IN('O', 'I', 'S', 'C', 'A'))
         AND NOT EXISTS(SELECT 1
                          FROM load_clos_cntrl_bc2c c
                         WHERE c.div_part = ld.div_part
                           AND c.llr_dt = ld.llr_dt
                           AND c.load_num = ld.load_num);

    logs.dbg('Insert MCLP370C Null Entry');

    INSERT INTO mclp370c
                (div_part, loadc, stopc, manctc, custc, nutotc, totctc, totsmc, bagsmc, boxsmc, palsmc, cpasmc,
                 peco_pallet_cnt, depdtc, llr_date, load_status, test_bil_load_sw, release_ts)
      SELECT   i_r_rlse.div_part, mr.load_num, mr.stop_num, NVL(mr.manifest_cat, '000'), mr.cust_num, 0, NULL, 0, 0, 0,
               0, 0, 0, mr.departure_date, i_r_rlse.llr_dt - g_c_rensoft_seed_dt, 'P',
               DECODE(i_r_rlse.test_bil_cd, '~', 'N', 'Y'), i_r_rlse.rlse_ts
          FROM mclane_manifest_rpts mr
         WHERE mr.div_part = i_r_rlse.div_part
           AND mr.create_ts = i_r_rlse.rlse_ts
           AND mr.strategy_id = i_r_rlse.strtg_id
           AND mr.llr_date = i_r_rlse.llr_dt - g_c_rensoft_seed_dt
           AND INSTR(i_r_rlse.load_list, ',' || mr.load_num || ',') > 0
           AND NOT EXISTS(SELECT 1
                            FROM mclp370c mc
                           WHERE mc.div_part = mr.div_part
                             AND mc.llr_date = mr.llr_date
                             AND mc.loadc = mr.load_num
                             AND mc.stopc = mr.stop_num
                             AND mc.depdtc = mr.departure_date
                             AND mc.manctc = NVL(mr.manifest_cat, '000')
                             AND mc.totctc IS NULL
                             AND mc.release_ts = i_r_rlse.rlse_ts)
      GROUP BY mr.departure_date, mr.load_num, mr.stop_num, mr.cust_num, mr.manifest_cat;

    logs.dbg('Merge MCLP370C');
    MERGE INTO mclp370c c
         USING (SELECT mr.departure_date, mr.load_num, mr.stop_num, mr.cust_num, mr.manifest_cat, mr.tote_cat,
                       mr.tote_count, mr.box_count, mr.bag_count, i_r_rlse.llr_dt - g_c_rensoft_seed_dt AS llr_dt,
                       DECODE(i_r_rlse.test_bil_cd, '~', 'N', 'Y') AS test_bil_sw
                  FROM mclane_manifest_rpts mr
                 WHERE mr.div_part = i_r_rlse.div_part
                   AND mr.create_ts = i_r_rlse.rlse_ts
                   AND mr.strategy_id = i_r_rlse.strtg_id
                   AND mr.llr_date = i_r_rlse.llr_dt - g_c_rensoft_seed_dt
                   AND INSTR(i_r_rlse.load_list, ',' || mr.load_num || ',') > 0) x
            ON (    c.div_part = i_r_rlse.div_part
                AND c.llr_date = x.llr_dt
                AND c.loadc = x.load_num
                AND c.stopc = x.stop_num
                AND c.depdtc = x.departure_date
                AND c.manctc = NVL(x.manifest_cat, '000')
                AND NVL(c.totctc, '000') = NVL(x.tote_cat, '000')
                AND c.release_ts = i_r_rlse.rlse_ts)
      WHEN MATCHED THEN
        UPDATE
           SET boxsmc = boxsmc + NVL(x.box_count, 0), bagsmc = bagsmc + NVL(x.bag_count, 0),
               totsmc = totsmc + NVL(x.tote_count, 0), load_status = 'P'
      WHEN NOT MATCHED THEN
        INSERT(div_part, loadc, stopc, manctc, custc, nutotc, totctc, totsmc, bagsmc, boxsmc, palsmc, cpasmc,
               peco_pallet_cnt, depdtc, llr_date, load_status, test_bil_load_sw, release_ts)
        VALUES(i_r_rlse.div_part, x.load_num, x.stop_num, NVL(x.manifest_cat, '000'), x.cust_num, 0, x.tote_cat,
               NVL(x.tote_count, 0), NVL(x.bag_count, 0), NVL(x.box_count, 0), 0, 0, 0, x.departure_date, x.llr_dt, 'P',
               x.test_bil_sw, i_r_rlse.rlse_ts);
    COMMIT;
    -- Build Default Entries for Stops that have no Allocated Quantity
    -- (Load Close needs these to generate Invoices)
    logs.dbg('Insert Default MCLP370C Manifest Entry');

    INSERT INTO mclp370c
                (div_part, loadc, stopc, manctc, custc, nutotc, totctc, totsmc, bagsmc, boxsmc, palsmc, cpasmc,
                 peco_pallet_cnt, depdtc, llr_date, load_status, test_bil_load_sw, release_ts)
      SELECT i_r_rlse.div_part, ld.load_num, se.stop_num, '000', se.cust_id, 0, NULL, 0, 0, 0, 0, 0, 0,
             TRUNC(ld.depart_ts) - g_c_rensoft_seed_dt, i_r_rlse.llr_dt - g_c_rensoft_seed_dt, 'P',
             DECODE(i_r_rlse.test_bil_cd, '~', 'N', 'Y'), i_r_rlse.rlse_ts
        FROM load_depart_op1f ld, stop_eta_op1g se
       WHERE ld.div_part = i_r_rlse.div_part
         AND ld.llr_dt = i_r_rlse.llr_dt
         AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND EXISTS(SELECT 1
                      FROM ordp100a a, ordp120b b
                     WHERE a.div_part = se.div_part
                       AND a.load_depart_sid = se.load_depart_sid
                       AND a.custa = se.cust_id
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb = 'P')
         AND NOT EXISTS(SELECT 1
                          FROM mclp370c mc
                         WHERE mc.div_part = ld.div_part
                           AND mc.llr_date = ld.llr_dt - g_c_rensoft_seed_dt
                           AND mc.loadc = ld.load_num
                           AND mc.stopc = se.stop_num
                           AND mc.custc = se.cust_id
                           AND mc.depdtc = TRUNC(ld.depart_ts) - g_c_rensoft_seed_dt);

    COMMIT;

    IF g_xdock_pick_compl_sw = 'Y' THEN
      logs.dbg('Get Pick Complete Parm List');

/*      OPEN l_cv
       FOR
         SELECT to_list_fn
                  (CURSOR
                     (SELECT TO_CHAR(lc.llr_dt, 'YYYY-MM-DD')
                             || '~'
                             || lc.load_num
                        FROM appl_sys_parm_ap1s xl, load_clos_cntrl_bc2c lc
                       WHERE xl.div_part = i_r_rlse.div_part
                         AND xl.appl_id = 'OP'
                         AND xl.parm_id LIKE 'XDOCK_LOAD%'
                         AND INSTR(i_r_rlse.load_list,
                                   ',' || xl.vchar_val || ','
                                  ) > 0
                         AND lc.div_part = xl.div_part
                         AND lc.llr_dt = i_r_rlse.llr_dt
                         AND lc.load_num = xl.vchar_val
                         AND lc.load_status = 'P'
                         AND lc.pick_compl_sw = 'N'
                      MINUS
                      SELECT TO_CHAR(ld.llr_dt, 'YYYY-MM-DD')
                             || '~'
                             || ld.load_num
                        FROM appl_sys_parm_ap1s xl, load_depart_op1f ld
                       WHERE xl.div_part = i_r_rlse.div_part
                         AND xl.appl_id = 'OP'
                         AND xl.parm_id LIKE 'XDOCK_LOAD%'
                         AND INSTR(i_r_rlse.load_list,
                                   ',' || xl.vchar_val || ','
                                  ) > 0
                         AND ld.div_part = xl.div_part
                         AND ld.llr_dt = i_r_rlse.llr_dt
                         AND ld.load_num = xl.vchar_val
                         AND EXISTS(
                               SELECT 1
                                 FROM ordp100a a, ordp120b b
                                WHERE a.div_part = ld.div_part
                                  AND a.load_depart_sid = ld.load_depart_sid
                                  AND b.div_part = a.div_part
                                  AND b.ordnob = a.ordnoa
                                  AND b.manctb NOT IN(
                                        SELECT p.vchar_val
                                          FROM appl_sys_parm_ap1s p
                                         WHERE p.div_part = i_r_rlse.div_part
                                           AND p.appl_id = 'OP'
                                           AND p.parm_id LIKE 'XDOCK_MFST%')
                                  AND b.statb NOT IN('O', 'I', 'S', 'C', 'A')
                                  AND b.excptn_sw = 'N'
                                  AND b.pckqtb > 0
                                  AND b.subrcb < 999)
                     ),
                   '`'
                  )
           FROM DUAL;
*/
      OPEN l_cv
       FOR
         SELECT LISTAGG(x.llr_load, '`') WITHIN GROUP(ORDER BY x.llr_load)
           FROM (SELECT TO_CHAR(lc.llr_dt, 'YYYY-MM-DD') || '~' || lc.load_num AS llr_load
                   FROM TABLE(CAST(g_t_xdock_loads AS type_stab)) xl, load_clos_cntrl_bc2c lc
                  WHERE INSTR(i_r_rlse.load_list, ',' || xl.column_value || ',') > 0
                    AND lc.div_part = i_r_rlse.div_part
                    AND lc.llr_dt = i_r_rlse.llr_dt
                    AND lc.load_num = xl.column_value
                    AND lc.load_status = 'P'
                    AND lc.pick_compl_sw = 'N'
                 MINUS
                 SELECT TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') || '~' || ld.load_num AS llr_load
                   FROM TABLE(CAST(g_t_xdock_loads AS type_stab)) xl, load_depart_op1f ld
                  WHERE INSTR(i_r_rlse.load_list, ',' || xl.column_value || ',') > 0
                    AND ld.div_part = i_r_rlse.div_part
                    AND ld.llr_dt = i_r_rlse.llr_dt
                    AND ld.load_num = xl.column_value
                    AND EXISTS(SELECT 1
                                 FROM ordp100a a, ordp120b b
                                WHERE a.div_part = ld.div_part
                                  AND a.load_depart_sid = ld.load_depart_sid
                                  AND b.div_part = a.div_part
                                  AND b.ordnob = a.ordnoa
                                  AND b.manctb NOT IN(SELECT t.column_value
                                                        FROM TABLE(CAST(g_t_xdock_mfsts AS type_stab)) t)
                                  AND b.statb NOT IN('O', 'I', 'S', 'C', 'A')
                                  AND b.excptn_sw = 'N'
                                  AND b.pckqtb > 0
                                  AND b.subrcb < 999)) x;

      FETCH l_cv
       INTO l_pick_compl_parm_list;

      CLOSE l_cv;

      IF l_pick_compl_parm_list IS NOT NULL THEN
        logs.dbg('Process Pick Complete');
        op_pick_confirm_pk.pick_compl_sp(i_r_rlse.div_part, l_pick_compl_parm_list, 'Y');
      END IF;   -- l_pick_compl_parm_list IS NOT NULL
    END IF;   -- g_xdock_pick_compl_sw = 'Y'

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tote_fcast_sp;

  /*
  ||----------------------------------------------------------------------------
  || EXTR_TOTE_FCAST_MSGS_SP
  ||  This procedure builds the Tote Forecast Messages for the mainframe process
  ||  and writes them to the MQ Put Table. It uses the Manifest Reports table to
  ||  build the entries. This procedure also writes the Mainframe Trigger Message
  ||  that kicks-off the mainframe process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/01 | JUSTANI | Original
  || 03/11/02 | rhalpai | Added bag_count to tote_forecast cursor and referenced
  ||                    | it in section 'Build Detail Forecast Record'
  || 12/13/02 | rhalpai | Added test bill automation logic to change mq queue
  ||                    | names for test bills.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Removed LLRDate parm.
  ||                    | Replaced cursor loop with single insert to
  ||                    | MCLANE_MQ_PUT.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE extr_tote_fcast_msgs_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.EXTR_TOTE_FCAST_MSGS_SP';
    lar_parm                logs.tar_parm;
    l_strtg_nm              VARCHAR2(40);
    l_ord_ln_cnt            PLS_INTEGER;
    l_mq_msgid_totes        VARCHAR2(7)   := 'QOPRC18';
    l_mq_msgid_wrk_ords     VARCHAR2(7)   := 'QOPRC12';
    l_mq_msgid_trigger      VARCHAR2(7)   := 'QOPRC20';
    l_c_rlse_char  CONSTANT VARCHAR2(30)  := TO_CHAR(i_r_rlse.rlse_ts, 'YYYYMMDDHH24MISSSSS');
    l_tote_cnt              PLS_INTEGER   := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Get Create_TS and Strategy Name');

    SELECT s.strtg_nm, r.ord_ln_cnt
      INTO l_strtg_nm, l_ord_ln_cnt
      FROM rlse_op1z r, rlse_strtg_op4t s
     WHERE r.div_part = i_r_rlse.div_part
       AND r.rlse_id = i_r_rlse.rlse_id
       AND r.stat_cd = 'P'
       AND s.div_part(+) = r.div_part
       AND s.strtg_id(+) = r.strtg_id;

    IF i_r_rlse.test_bil_cd <> '~' THEN
      l_mq_msgid_wrk_ords := 'QTPRC12';
      l_mq_msgid_totes := 'QTPRC18';
      l_mq_msgid_trigger := 'QTPRC20';
    END IF;   -- i_r_rlse.test_bil_cd <> '~'

    --------------------------------------------------------------
    -- SELECT the Tote Forecast Entries FROM mclane_manifest_rpts
    --------------------------------------------------------------
    -- QOPRC18 Tote Forecast
    logs.dbg('Add Tote Forecast MQ Entries');

    INSERT INTO mclane_mq_put
                (mq_msg_id, div_part, mq_msg_status, create_ts, last_chg_ts, mq_corr_put_id, mq_msg_data)
      SELECT   l_mq_msgid_totes, i_r_rlse.div_part, 'OPN', i_r_rlse.rlse_ts, i_r_rlse.rlse_ts, 0,
               i_r_rlse.div_id
               || RPAD(l_mq_msgid_totes, 8)
               || RPAD(l_c_rlse_char, 30)
               || RPAD('ADD', 13)
               || rpad_fn(mr.load_num, 4)
               || lpad_fn(mr.stop_num, 2, '0')
               || lpad_fn(cx.mccusb, 6, '0')
               || lpad_fn(mr.tote_cat, 3, '0')
               || lpad_fn(NVL(SUM(mr.tote_count), 0) + NVL(SUM(mr.box_count), 0) + NVL(SUM(mr.bag_count), 0), 9, '0')
          FROM mclane_manifest_rpts mr, mclp020b cx
         WHERE mr.div_part = i_r_rlse.div_part
           AND mr.strategy_id > 0
           AND mr.create_ts = i_r_rlse.rlse_ts
           AND mr.llr_date = i_r_rlse.llr_dt - g_c_rensoft_seed_dt
           AND INSTR(i_r_rlse.load_list, ',' || mr.load_num || ',') > 0
           AND cx.div_part = mr.div_part
           AND cx.custb = mr.cust_num
      GROUP BY mr.load_num, mr.stop_num, cx.mccusb, mr.tote_cat
      ORDER BY mr.load_num, mr.stop_num, cx.mccusb, mr.tote_cat;

    l_tote_cnt := SQL%ROWCOUNT;
    logs.dbg('Add Finished Forecast MQ Record');

    INSERT INTO mclane_mq_put
                (mq_msg_id, div_part, mq_msg_status, create_ts, last_chg_ts, mq_corr_put_id, mq_msg_data)
      SELECT l_mq_msgid_trigger, i_r_rlse.div_part, 'OPN', i_r_rlse.rlse_ts, i_r_rlse.rlse_ts, 0,
             i_r_rlse.div_id
             || RPAD(l_mq_msgid_trigger, 8)
             || RPAD(l_c_rlse_char, 30)
             || RPAD('ADD', 13)
             || rpad_fn(l_strtg_nm, 40)
             || rpad_fn(i_r_rlse.user_id, 20)
             || LPAD(l_ord_ln_cnt, 9, '0')
             || LPAD(wo.wrk_ord_cnt, 9, '0')
             || LPAD(l_tote_cnt, 9, '0')
             || '000'
        FROM (SELECT COUNT(1) AS wrk_ord_cnt
                FROM mclane_mq_put p
               WHERE p.div_part = i_r_rlse.div_part
                 AND p.mq_msg_id = l_mq_msgid_wrk_ords
                 AND p.last_chg_ts = i_r_rlse.rlse_ts
                 AND p.mq_msg_status = 'OPN') wo;

    COMMIT;
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extr_tote_fcast_msgs_sp;

  /*
  ||----------------------------------------------------------------------------
  || ECOM_MOQ_EXTR_SP
  ||  Extract ECOM orders within allocation where all lines had qty reduced to zero.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/22/20 | rhalpai | Original for PIR19810
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ecom_moq_extr_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_ALLOCATE_PK.ECOM_MOQ_EXTR_SP';
    lar_parm               logs.tar_parm;
    l_t_ord_src            type_stab;
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/ftptrans';
    l_c_file_nm   CONSTANT VARCHAR2(50)   := i_r_rlse.div_id || '_ECOM_MOQ_' || i_r_rlse.rlse_ts_char;
    l_t_rpt_lns            typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'LLRDt', i_r_rlse.llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_r_rlse.load_list);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Initialize');
    l_t_ord_src := op_parms_pk.vals_for_prfx_fn(i_r_rlse.div_part, 'ECOM_ORDSRC');
    logs.dbg('Get Ecom MOQ Lines');

    SELECT d.div_id || a.custa || rpad_fn(a.ipdtsa, 8) || rpad_fn(a.connba, 25) || rpad_fn(a.cpoa, 40)
    BULK COLLECT INTO l_t_rpt_lns
      FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a
     WHERE d.div_part = i_r_rlse.div_part
       AND ld.div_part = d.div_part
       AND ld.llr_dt = i_r_rlse.llr_dt
       AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0
       AND a.div_part = ld.div_part
       AND a.load_depart_sid = ld.load_depart_sid
       AND a.ipdtsa IN(SELECT t.column_value
                         FROM TABLE(l_t_ord_src) t)
       AND a.stata = 'P'
       AND EXISTS(SELECT 1
                    FROM ordp120b b
                   WHERE b.div_part = a.div_part
                     AND b.ordnob = a.ordnoa
                  HAVING SUM(b.ordqtb) = 0);

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_c_file_nm, l_c_file_dir);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ecom_moq_extr_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_EXCPTN_LOG_SP
  ||  Set log entries for order lines to resolved.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_excptn_log_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_t_ords  IN  type_ntab
  ) IS
  BEGIN
    env.tag();
    FORALL i IN i_t_ords.FIRST .. i_t_ords.LAST
      UPDATE mclp300d d
         SET d.resexd = '1',
             d.exdesd = i_r_rlse.rlse_ts_char
       WHERE d.div_part = i_r_rlse.div_part
         AND d.ordnod = i_t_ords(i)
         AND d.resexd = '0';
    env.untag();
  END upd_excptn_log_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_HDR_STAT_SP
  ||  Set status for Order Headers.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 05/20/09 | rhalpai | Changed logic to use header status of P. IM506029
  || 03/01/10 | rhalpai | Removed LLRDate parm and changed logic to look it up
  ||                    | using the ReleaseTS. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_hdr_stat_sp(
    i_r_rlse  IN      g_cur_rlse%ROWTYPE,
    o_t_ords  OUT     type_ntab
  ) IS
  BEGIN
    env.tag();
    o_t_ords := type_ntab();

    UPDATE    ordp100a a
          SET a.stata =(CASE
                          WHEN EXISTS(SELECT 1
                                        FROM ordp120b b
                                       WHERE b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND b.statb IN('O', 'I')) THEN 'P'
                          ELSE 'R'
                        END
                       ),
              a.uschga = NVL(a.uschga, i_r_rlse.rlse_ts_char)
        WHERE a.div_part = i_r_rlse.div_part
          AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                     FROM load_depart_op1f ld
                                    WHERE ld.div_part = i_r_rlse.div_part
                                      AND ld.llr_dt = i_r_rlse.llr_dt
                                      AND INSTR(i_r_rlse.load_list, ',' || ld.load_num || ',') > 0)
          AND a.stata = 'P'
          AND EXISTS(SELECT 1
                       FROM ordp120b b
                      WHERE b.div_part = a.div_part
                        AND b.ordnob = a.ordnoa
                        AND b.statb IN('P', 'T', 'X'))
    RETURNING         a.ordnoa
    BULK COLLECT INTO o_t_ords;

    env.untag();
  END upd_ord_hdr_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_DTL_STAT_SP
  ||  Set order detail statuses to Released.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/05/09 | rhalpai | Original
  || 05/20/10 | rhalpai | Changed to not update not-ship-rsn to INVOUT or
  ||                    | DISOUT when ordqty is zero. PIR8377
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_dtl_stat_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE,
    i_t_ords  IN  type_ntab
  ) IS
  BEGIN
    env.tag();
    FORALL i IN i_t_ords.FIRST .. i_t_ords.LAST
      UPDATE ordp120b b
         SET b.statb = 'R',
             b.shpidb = i_r_rlse.rlse_ts_char,
             b.ntshpb =(CASE
                          WHEN(    b.excptn_sw = 'N'
                               AND b.ntshpb IS NULL
                               AND b.ordqtb > 0
                               AND NVL(b.pckqtb, 0) = 0) THEN DECODE((SELECT a.dsorda
                                                                        FROM ordp100a a
                                                                       WHERE a.div_part = i_r_rlse.div_part
                                                                         AND a.ordnoa = i_t_ords(i)),
                                                                     'D', 'DISOUT',
                                                                     'INVOUT'
                                                                    )
                          ELSE b.ntshpb
                        END
                       )
       WHERE b.div_part = i_r_rlse.div_part
         AND b.ordnob = i_t_ords(i)
         AND b.statb IN('P', 'T', 'X');
    env.untag();
  END upd_ord_dtl_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_STATS_SP
  ||  This procedure takes each order that has been released and updates each of
  ||  the orderlines released for that order.  The status is set to a 'R' for all
  ||  orders released.  Depending on whether or not the order line was allocated
  ||  determines what columns are updated for the order.  The exception well is
  ||  updated also. Order Headers for the Order Well and Exception well are also
  ||  updated with specific information - status is set to a 'R'. The Resolved
  ||  flag is set on the Exception message table for all order lines updated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/10/02 | SKADALI | Modified update mclp300d statement
  || 12/22/03 | rhalpai | Included 'X' status for temp cross-dock orders
  || 02/19/04 | rhalpai | Removed update of resusd (userid) on MCLP300D
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  || 08/11/08 | rhalpai | Changed to use DISOUT or INVOUT as not-ship-reason if
  ||                    | picked qty is zero.
  ||                    | Changed to update order header status to R when there
  ||                    | are no more unbilled order lines or P when there are
  ||                    | unbilled order lines. PIR6364
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Changed to call new modules: UPD_EXCPTN_LOG_SP,
  ||                    | UPD_EXCPTN_ORD_DTL_STAT_SP, UPD_ORD_HDR_STAT_SP,
  ||                    | UPD_EXCPTN_ORD_DTL_STAT_SP, UPD_ORD_DTL_STAT_SP.
  || 03/01/10 | rhalpai | Removed LLRDate parm and removed it from calls to
  ||                    | UPD_EXCPTN_ORD_HDR_STAT_SP and UPD_ORD_HDR_STAT_SP.
  ||                    | PIR0024
  || 03/20/12 | rhalpai | Change logic to remove calls to old
  ||                    | UPD_EXCPTN_ORD_HDR_STAT_SP, UPD_EXCPTN_ORD_DTL_STAT_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_stats_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.UPD_ORD_STATS_SP';
    lar_parm             logs.tar_parm;
    l_t_ords             type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Upd Ord Hdr');
    upd_ord_hdr_stat_sp(i_r_rlse, l_t_ords);

    IF l_t_ords.COUNT > 0 THEN
      logs.dbg('Upd Excptn Log for Ord Hdr');
      upd_excptn_log_sp(i_r_rlse, l_t_ords);
      logs.dbg('Upd Ord Dtl for Ord Hdr');
      upd_ord_dtl_stat_sp(i_r_rlse, l_t_ords);
      COMMIT;
    END IF;   -- l_t_ords.COUNT > 0

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_stats_sp;

  /*
  ||----------------------------------------------------------------------------
  || UNLOCK_LOAD_CLOS_SP
  ||  Unlock Load Close for TestBills.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE unlock_load_clos_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.UNLOCK_LOAD_CLOS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();

    IF i_r_rlse.test_bil_cd <> '~' THEN
      logs.dbg('Upd LOAD_CLOS_CNTRL_BC2C status for Test Bill');

      UPDATE load_clos_cntrl_bc2c c
         SET c.load_status = 'R'
       WHERE c.div_part = i_r_rlse.div_part
         AND c.load_status = 'P'
         AND c.llr_dt = i_r_rlse.llr_dt;

      logs.dbg('Upd MCLP370C status for Test Bill');

      UPDATE mclp370c c
         SET c.load_status = 'R'
       WHERE c.div_part = i_r_rlse.div_part
         AND c.llr_date = i_r_rlse.llr_dt - g_c_rensoft_seed_dt
         AND c.load_status = 'P'
         AND INSTR(i_r_rlse.load_list, ',' || c.loadc || ',') > 0;
    END IF;   -- i_r_rlse.test_bil_cd <> '~'

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END unlock_load_clos_sp;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_RATION_RPT_SP
  ||  Create Item Ration Report and ftp to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE item_ration_rpt_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ITEM_RATION_RPT_SP';
    lar_parm               logs.tar_parm;
    l_file_nm              VARCHAR2(10);
    l_c_rmt_file  CONSTANT VARCHAR2(10)  := i_r_rlse.div_id || 'RATION';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Create Item Ration Report');
    op_misc_reports_pk.item_ration_rpt_sp(i_r_rlse.div_id, i_r_rlse.rlse_ts, l_file_nm);

    IF l_file_nm IS NOT NULL THEN
      logs.dbg('Ftp Item Ration Report to Mainframe');
      op_ftp_sp(i_r_rlse.div_id, l_file_nm, l_c_rmt_file);
    END IF;   -- l_file_nm IS NOT NULL

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_ration_rpt_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || IS_PROCESSING_FN
  ||  Returns an indication of whether or not Allocate is processing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/04/02 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 05/05/09 | rhalpai | Reformatted
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 10/22/15 | rhalpai | Change cursor to join div_part to div_part instead of
  ||                    | div_part to div_id.
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_processing_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm   := 'OP_ALLOCATE_PK.IS_PROCESSING_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_alloc_last_step    NUMBER;
    l_alloc_stat         g_st_processing;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_alloc_last_step := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_alloc_last_step);
    l_alloc_stat := g_c_alloc_is_not_processing;
    logs.dbg('Check Allocate Status');

    OPEN l_cv
     FOR
       SELECT g_c_alloc_is_processing
         FROM rlse_op1z r
        WHERE r.div_part = l_div_part
          AND r.stat_cd = 'P'
          AND l_alloc_last_step >= (SELECT NVL(MAX(rtd.seq), 0)
                                      FROM rlse_log_op2z rl, rlse_typ_dmn_op9z rtd
                                     WHERE rl.div_part = r.div_part
                                       AND rl.rlse_id = r.rlse_id
                                       AND rtd.typ_id = rl.typ_id
                                       AND rtd.seq >= 0);

    FETCH l_cv
     INTO l_alloc_stat;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_alloc_stat);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_processing_fn;

  /*
  ||----------------------------------------------------------------------------
  || KIT_ORD_TAB_FN
  ||  Returns a table of kit component order info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - PIR2909
  || 03/27/06 | rhalpai | Changed cursor to handle null PO's IM225137
  || 03/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION kit_ord_tab_fn(
    i_ord_stat  IN  VARCHAR2,
    i_o_kit     IN  kit_t
  )
    RETURN kit_ords_t IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.KIT_ORD_TAB_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_eta_dt             DATE;
    l_t_kit_ords         kit_ords_t;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm,
                  'Kit',
                  (CASE
                     WHEN i_o_kit IS NULL THEN NULL
                     ELSE ' Div: '
                          || util.to_str(i_o_kit.div_id)
                          || ' LLRNum: '
                          || util.to_str(i_o_kit.llr_dt)
                          || ' KitTyp: '
                          || util.to_str(i_o_kit.kit_typ)
                          || ' OrdTyp: '
                          || util.to_str(i_o_kit.ord_typ)
                          || ' ItemNum: '
                          || util.to_str(i_o_kit.kit_item_num)
                          || ' CustId: '
                          || util.to_str(i_o_kit.cust_num)
                          || ' LoadNum: '
                          || util.to_str(i_o_kit.load_num)
                          || ' StopNum: '
                          || util.to_str(i_o_kit.stop_num)
                          || ' EtaNum: '
                          || util.to_str(i_o_kit.eta_date)
                          || ' PoNum: '
                          || util.to_str(i_o_kit.po_num)
                   END
                  )
                 );
    logs.dbg('ENTRY', lar_parm);
    env.tag();
    l_llr_dt := DATE '1900-02-28' + i_o_kit.llr_dt;
    l_eta_dt := DATE '1900-02-28' + i_o_kit.eta_date;

    OPEN l_cv
     FOR
       SELECT   kit_ord_t(t.ord_stat,
                          t.div_id,
                          t.llr_dt,
                          t.kit_typ,
                          t.ord_typ,
                          t.kit_item_num,
                          t.cust_num,
                          t.load_num,
                          t.stop_num,
                          t.eta_date,
                          t.po_num,
                          t.comp_item_num,
                          t.comp_qty,
                          t.order_num,
                          t.order_ln,
                          t.item_num,
                          t.uom,
                          t.ord_qty,
                          t.ratio,
                          t.seq
                         )
           FROM TABLE(kit_ord_fn(CURSOR(SELECT   b.statb, d.div_id, i_o_kit.llr_dt, k.kit_typ, a.dsorda, k.item_num,
                                                 se.cust_id, ld.load_num, se.stop_num, i_o_kit.eta_date, a.cpoa,
                                                 b.orditb, k.comp_qty
                                            FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, ordp100a a,
                                                 ordp120b b, kit_item_mstr_kt1m k
                                           WHERE d.div_id = i_o_kit.div_id
                                             AND ld.div_part = d.div_part
                                             AND ld.llr_dt = l_llr_dt
                                             AND ld.load_num = i_o_kit.load_num
                                             AND se.div_part = ld.div_part
                                             AND se.load_depart_sid = ld.load_depart_sid
                                             AND se.cust_id = i_o_kit.cust_num
                                             AND se.stop_num = i_o_kit.stop_num
                                             AND TRUNC(se.eta_ts) = l_eta_dt
                                             AND a.div_part = se.div_part
                                             AND a.load_depart_sid = se.load_depart_sid
                                             AND a.custa = se.cust_id
                                             AND a.excptn_sw = 'N'
                                             AND a.dsorda = i_o_kit.ord_typ
                                             AND NVL(a.cpoa, ' ') = NVL(i_o_kit.po_num, ' ')
                                             AND b.div_part = a.div_part
                                             AND b.ordnob = a.ordnoa
                                             AND b.statb = i_ord_stat
                                             AND b.excptn_sw = 'N'
                                             AND b.ordqtb > 0
                                             AND b.subrcb = 0
                                             AND k.div_part = d.div_part
                                             AND k.kit_typ = i_o_kit.kit_typ
                                             AND k.item_num = i_o_kit.kit_item_num
                                             AND k.comp_item_num = b.orditb
                                        GROUP BY b.statb, d.div_id, k.kit_typ, a.dsorda, k.item_num, se.cust_id,
                                                 ld.load_num, se.stop_num, a.cpoa, b.orditb, k.comp_qty
                                       )
                                )
                     ) t
       ORDER BY t.seq, t.order_num, t.order_ln;

    FETCH l_cv
    BULK COLLECT INTO l_t_kit_ords;

    CLOSE l_cv;

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_kit_ords);
  END kit_ord_tab_fn;

  /*
  ||----------------------------------------------------------------------------
  || CONTAINER_ID_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/06 | rhalpai | Original - created for PIR3209
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  || 10/22/15 | rhalpai | Remove div_part from MCLP230A. PIR15202
  || 11/11/15 | rhalpai | Replace sequence with new CNTNR_ID_SEQ_FN function to
  ||                    | get next divisional sequence number.
  ||                    | This will prevent duplicate container ids across
  ||                    | multiple billings due to sharing a common sequence
  ||                    | with only 6 bytes resulting in wrapping back to the
  ||                    | same sequence number. SDLS-11
  || 01/16/24 | rhalpai | Change logic to handle 3-digit div_part. PC-8533
  || 05/16/24 | rhalpai | Add logic to loop until a unique container id is found for div. This is to handle an apparent
  ||                    | bug where the last_number of the sequence was before the sequences used in the previous
  ||                    | release resulting in duplicate containers across releases. SDHD-2257633
  || 05/20/24 | rhalpai | Add logic to log when duplicate exists. SDHD-2257633
  ||----------------------------------------------------------------------------
  */
  FUNCTION container_id_fn(
    i_div        IN  VARCHAR2,
    i_rlse_ts    IN  DATE,
    i_item       IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_manual_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                           := 'OP_ALLOCATE_PK.CONTAINER_ID_FN';
    lar_parm             logs.tar_parm;
    l_cntnr_id           bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    l_exists_sw          VARCHAR2(1)                             := 'Y';
    l_div_part           NUMBER;
    l_dupl_cnt           PLS_INTEGER                             := -1;

    FUNCTION cntnr_id_fn
      RETURN VARCHAR2 IS
      l_cv  SYS_REFCURSOR;
      l_id  bill_cntnr_id_bc1c.orig_cntnr_id%TYPE;
    BEGIN
      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT '00'
                || COALESCE((CASE
                               WHEN i_manual_sw = 'Y' THEN '4'
                             END),
                            (SELECT TO_CHAR(a.cntnr_itm_typ)
                               FROM mclp230a a, sawp505e e
                              WHERE e.scbcte = a.sbcata
                                AND e.iteme = i_item
                                AND e.uome = i_uom),
                            (CASE
                               WHEN i_uom LIKE 'CI%' THEN '3'
                               WHEN i_uom LIKE '%I' THEN '2'
                               WHEN i_uom LIKE '%R' THEN '1'
                               ELSE '0'
                             END
                            )
                           )
                || TO_CHAR(i_rlse_ts, 'Y')
                || LPAD(d.div_part, 3, '0')
                || '000'
                || TO_CHAR(i_rlse_ts, 'DDD')
                || LPAD(cntnr_id_seq_fn(d.div_id), 6, '0')
           FROM div_mstr_di1d d
          WHERE d.div_id = i_div;

      logs.dbg('Fetch Cursor');

      FETCH l_cv
       INTO l_id;

      RETURN(l_id);
    END cntnr_id_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'ManualSw', i_manual_sw);
    logs.dbg('ENTRY', lar_parm);

    SELECT div_part
       INTO l_div_part
      FROM div_mstr_di1d d
     WHERE d.div_id = i_div;

    WHILE l_exists_sw = 'Y' LOOP
      l_dupl_cnt := l_dupl_cnt + 1;
      l_cntnr_id := cntnr_id_fn;
      l_cntnr_id := l_cntnr_id || check_digit_fn(l_cntnr_id);

      SELECT NVL(MAX('Y'), 'N')
        INTO l_exists_sw
        FROM dual
       WHERE EXISTS(SELECT 1
                      FROM bill_cntnr_id_bc1c bc
                     WHERE bc.div_part = l_div_part
                       AND bc.orig_cntnr_id = l_cntnr_id);
    END LOOP;

    IF l_dupl_cnt > 0 THEN
      logs.warn('Container Duplicates found: ' || l_dupl_cnt, lar_parm);
    END IF;   -- l_dupl_cnt > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cntnr_id);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END container_id_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_CIGS_SP
  ||  Driver for processing allocation of Cig order lines
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/01/10 | rhalpai | Original
  || 05/20/10 | rhalpai | Added logic to update event log. PIR8377
  || 08/26/10 | rhalpai | Changed to log step for each OP/CMS Inventory. PIR8531
  || 03/20/12 | rhalpai | Remove logic to Process Cig Allocation using OP
  ||                    | Inventory.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_cigs_sp(
    i_div          IN  VARCHAR2,
    i_rlse_ts      IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm        := 'OP_ALLOCATE_PK.PRCS_CIGS_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80)         := 'Initial';
    l_r_rlse             g_cur_rlse%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Get Release Info');
    l_r_rlse := rlse_info_fn(i_div, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
    logs.dbg('Process Cig Allocation using CMS Inventory');
    log_prcs_step_sp(l_r_rlse, g_c_prcs_beg_cig);
    prcs_cigs_cms_inv_sp(l_r_rlse);
    l_section := 'Set Cig Allocation Status to Complete';
    logs.dbg(l_section);
    upd_evnt_log_sp(l_r_rlse, l_section);
    upd_cig_alloc_stat_sp(l_r_rlse, g_c_cig_alloc_stat_compl);
    logs.dbg('Log End of Cig Product Allocation');
    log_prcs_step_sp(l_r_rlse, g_c_prcs_end_cig);
    upd_evnt_log_sp(l_r_rlse, 'Cig Product Allocation is Complete', 1);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      upd_cig_alloc_stat_sp(l_r_rlse, g_c_cig_alloc_stat_fail);
      upd_evnt_log_sp(l_r_rlse, 'Cig Product Allocation Failed', -1);
      logs.err(lar_parm);
  END prcs_cigs_sp;

  /*
  ||----------------------------------------------------------------------------
  || EVNT_CIG_ALLOC_SP
  ||  Create event to process Cig Allocation
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/10 | rhalpai | Original for PIR8377
  || 10/14/17 | rhalpai | Change to use new CIG_EVENT_MGR_PK.CREATE_INSTANCE.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE evnt_cig_alloc_sp(
    i_div      IN  VARCHAR2,
    i_rlse_ts  IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.EVNT_CIG_ALLOC_SP';
    lar_parm             logs.tar_parm;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
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
                    || i_rlse_ts
                    || '</value></row>'
                    || '</parameters>';
    logs.dbg('Create Event');
    cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                     i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                     i_event_dfn_id         => cig_constants_events_pk.evd_op_cig_alloc,
                                     i_parameters           => l_evnt_parms,
                                     i_div_nm               => i_div,
                                     i_is_script_fw_exec    => 'N',
                                     i_is_complete          => 'Y',
                                     i_pgm_id               => 'PLSQL',
                                     i_user_id              => i_user_id,
                                     o_event_que_id         => l_evnt_que_id
                                    );
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END evnt_cig_alloc_sp;

  /*
  ||----------------------------------------------------------------------------
  || ALLOCATE_SP
  ||  This procedure is the Controlling Procedure within this package.
  ||  It controls what is called and the sequence it is called in.
  ||      The "high-level" Sequence of processing is as follows -
  ||      1)  Determine the "Timestamp" for this release (Set by the Set Release
  ||          Strategy Screen)
  ||      2)  Allocate Items
  ||      3)  Allocate Orderlines for orders not allocated in #2 above
  ||      4)  Create Subs for orderlines not allocated in #2 and #3 above
  ||      5)  Allocate Sub Lines
  ||      6)  Update Partially Allocated Orders
  ||      7)  Extract Work Orders for Mainframe process
  ||      8)  Extract Orders for Mainframe process
  ||      9)  Build Manifest Reports Information
  ||     10)  Build Tote Forecast Entries
  ||     11)  Extract Tote Forecast Entries for Mainframe process
  ||     12)  Update Status of Order Lines and Headers that were Released
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/12/02 | JUSTANI | Added call to Load Balance Calc and added comments
  || 04/02/02 | SKADALI | Added call to update_eta_datatime proc
  || 09/12/02 | rhalpai | Replaced rensoft date lookup in MCLANE_DATE_CONVERT with
  ||                    | call to function.
  || 02/21/03 | rhalpai | Added logic to set global variable to indicate usage of
  ||                    | Cig Forcast System for slow moving item info. Added
  ||                    | truncation of slow moving item temp table.
  ||                    | Added logic to set MCLP370C entries in 'P' status to 'R'
  ||                    | for Test Bills.  For production billings these will be
  ||                    | set to 'R' in the release complete script that runs after
  ||                    | the BJ607J. This will keep users from closing loads that
  ||                    | are currently being billed.
  || 12/22/03 | rhalpai | Added logic to handle Cross-Dock orders
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed status out parm.
  ||                    | Removed status parm from calls to UPDATE_LLR_STATUS_SP,
  ||                    | LOAD_BAL_CALC_SP, CIG_ALLOCATE_FORECAST_SP,
  ||                    | ITEM_LEVEL_ALLOCATE_SP, ORDER_LEVEL_ALLOCATE_SP,
  ||                    | DEL_SUB_LNS_SP, CREATE_SUB_LNS_SP,
  ||                    | ALLOCATE_SUB_LINES_SP, EXTR_WRK_ORDS_SP,
  ||                    | EXTR_ORDS_SP, BUILD_REPORT_TABLE_SP, TOTE_FCAST_SP,
  ||                    | EXTR_TOTE_FCAST_MSGS_SP, UPD_ORD_STATS_SP.
  || 10/14/05 | rhalpai | Added logic to perform kit allocation - PIR2909
  ||                    | Added creation of item ration report - PIR1289
  || 03/02/06 | rhalpai | Added process control logic. IM200261
  || 04/07/06 | rhalpai | Added processing for Bundled Item Distributions PIR2545
  || 10/20/06 | rhalpai | Added calls to INS_PO_OVRRDS_SP and CUBING_OF_TOTES_SP.
  ||                    | Added update of status to LOAD_CLOS_CNTRL_BC2C. PIR3209
  || 03/05/07 | rhalpai | Changed call to INS_PO_OVRRDS_SP to include LLR parm.
  ||                    | IM290595
  || 08/11/08 | rhalpai | Changed logic to skip cig processing when the current
  ||                    | release has no cigs. PIR6364
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Removed update for ETA.
  ||                    | Changed to use new modules: ORD_LN_CNT_FN,
  ||                    | UPD_XDOCK_SP, TRUNCATE_TABLE_SP.
  ||                    | Changed update to use MCLANE_LOAD_LABEL_RLSE for
  ||                    | matching load list.
  ||                    | Added logging for steps between 10 and 390 and
  ||                    | changed to log step to be executed instead of step
  ||                    | that just finished executing.
  || 03/01/10 | rhalpai | Removed LLRDate and Status parms. Removed Process
  ||                    | Control logic. Changed logic to use LOG_PRCS_STEP_SP
  ||                    | to log process step. Changed logic to return Release
  ||                    | info when updating release seq nums. Removed logic
  ||                    | for Load Balance Calc. Removed CMS logic for calling
  ||                    | CIG_ALLOCATE_FORECAST_SP and BUILD_OPLTHC_TABLE_SP.
  ||                    | Removed logic to update X-Dock orders. Removed logic
  ||                    | for Bundled-Dist-Level and Kit-Level, Item-Level,
  ||                    | Order-Level Allocations. Removed logic for removing
  ||                    | unallocated sub lines, allocating original order lines
  ||                    | for deleted subs, creating conditional subs, and
  ||                    | allocating new subs. Removed CMS logic for calling
  ||                    | CIG_ALLOCATE_FORECAST_SP. Changed logic to start Cig
  ||                    | allocation in separate thread, process non-Cig
  ||                    | allocation and then wait for Cig allocation to end.
  ||                    | Added calls for UPD_WRK_ORD_STATS_SP, TAG_EXCPT_ORDS_SP,
  ||                    | and GOV_CNTL_RSTR_SP prior to EXTR_ORDS_SP. PIR0024
  || 05/20/10 | rhalpai | Added logic to update event log. PIR8377
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/05/11 | rhalpai | Added logic to apply Weekly Max Qtys. Added logic to
  ||                    | adjust PickQty for Weekly Max Cust Item and add
  ||                    | Weekly Max Log entries for remaining allocated OrdLns
  ||                    | after Upd Partially Allocated Orders. Add logic to
  ||                    | create WklyMaxQty Cut MQ records for non-testbills.
  ||                    | PIR6235
  || 12/08/11 | rhalpai | Add COMMIT before starting Cig Allocation. IM-037341
  || 05/22/20 | rhalpai | Add call to ECOM_MOQ_EXTR_SP. PIR19810
  ||----------------------------------------------------------------------------
  */
  PROCEDURE allocate_sp(
    i_r_rlse  IN  g_cur_rlse%ROWTYPE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.ALLOCATE_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80);
    l_rlse_has_cigs      BOOLEAN;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_rlse.div_id);
    logs.add_parm(lar_parm, 'RlseTS', i_r_rlse.rlse_ts_char);
    logs.add_parm(lar_parm, 'EvntQueId', i_r_rlse.evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_r_rlse.cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_r_rlse.cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Log Beginning of Allocate');
    log_prcs_step_sp(i_r_rlse, g_c_prcs_beg_alloc);
    logs.dbg('Apply Wkly Max Qtys');
    apply_wkly_maxs_sp(i_r_rlse);
    logs.dbg('Check Rlse has Cigs');
    l_rlse_has_cigs := rlse_has_cigs_fn(i_r_rlse);

    IF l_rlse_has_cigs THEN
      COMMIT;   -- Cig will need to be able to see applied changes
      logs.dbg('Set Cig Allocation Status to In-Process');
      upd_cig_alloc_stat_sp(i_r_rlse, g_c_cig_alloc_stat_inprcs);
      l_section := 'Start Cig Product Allocation';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_r_rlse, l_section);
      log_prcs_step_sp(i_r_rlse, g_c_prcs_start_cig);
      start_cig_alloc_sp(i_r_rlse);
    END IF;   -- l_rlse_has_cigs

    l_section := 'Process Non-Cig Product Allocation';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    prcs_noncigs_sp(i_r_rlse);

    IF l_rlse_has_cigs THEN
      l_section := 'Wait for Cig Allocation to Complete';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_r_rlse, l_section);
      wait_for_cig_alloc_sp(i_r_rlse);
    END IF;   -- l_rlse_has_cigs

    l_section := 'Upd Partially Allocated Orders';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_upd_partls);
    upd_partl_allocd_ords_sp(i_r_rlse);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_ords_allocd);
    logs.dbg('Upd and Log Remaining Allocated Wkly Max Qtys');
    upd_and_log_wkly_maxs_sp(i_r_rlse);
    l_section := 'PO Overrides';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_ins_po_ovrrd);
    ins_po_ovrrds_sp(i_r_rlse);
    l_section := 'Process Cubing of Totes';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_cube_tote);
    cubing_of_totes_sp(i_r_rlse);
    l_section := 'Extract Work Orders';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_ext_wrk_ords);
    extr_wrk_ords_sp(i_r_rlse);
    l_section := 'Set Work Order Entries to Released Status';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_upd_wrk_ord_stats);
    upd_wrk_ord_stats_sp(i_r_rlse);
    l_section := 'Tag Except Ords When All Good Ords for Load are Released';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_tag_excpt_ords);
    tag_excpt_ords_sp(i_r_rlse);
    l_section := 'Upd Ords for Gov Control Restricted';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_gov_cntl_rstr);
    gov_cntl_rstr_sp(i_r_rlse);
    l_section := 'Extract Orders';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_ext_ords);
    extr_ords_sp(i_r_rlse);
    l_section := 'Build Manifest Reports';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_build_mfst);
    op_manifest_reports_pk.build_report_table_sp(i_r_rlse.div_id, i_r_rlse.llr_dt, i_r_rlse.strtg_id, i_r_rlse.rlse_ts);
    l_section := 'Process Tote Forecast';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_tote_fcst);
    tote_fcast_sp(i_r_rlse);
    l_section := 'Extract Tote Forecast Messages';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_ext_tote_msgs);
    extr_tote_fcast_msgs_sp(i_r_rlse);

    IF i_r_rlse.test_bil_cd = '~' THEN
      l_section := 'Ecommerce Max Order Qty Extract';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_r_rlse, l_section);
      log_prcs_step_sp(i_r_rlse, g_c_prcs_ecom_moq_extr);
      ecom_moq_extr_sp(i_r_rlse);
    END IF;   -- i_r_rlse.test_bil_cd = '~'

    l_section := 'Set orders to released/allocated status';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_upd_ord_stat);
    upd_ord_stats_sp(i_r_rlse);

    IF i_r_rlse.test_bil_cd <> '~' THEN
      l_section := 'Unlock Load Close for Test Bill';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_r_rlse, l_section);
      log_prcs_step_sp(i_r_rlse, g_c_prcs_unlock_ld_clos);
      unlock_load_clos_sp(i_r_rlse);
      COMMIT;
    END IF;   -- i_r_rlse.test_bil_cd <> '~'

    l_section := 'Create Item Ration Report';
    logs.dbg(l_section);
    upd_evnt_log_sp(i_r_rlse, l_section);
    log_prcs_step_sp(i_r_rlse, g_c_prcs_itm_ration_rpt);
    item_ration_rpt_sp(i_r_rlse);

    IF i_r_rlse.test_bil_cd = '~' THEN
      l_section := 'Create IMQ62 WklyMaxQty Cut MQ Msgs';
      logs.dbg(l_section);
      upd_evnt_log_sp(i_r_rlse, l_section);
      log_prcs_step_sp(i_r_rlse, g_c_prcs_wkmaxqty_cut);

      INSERT INTO mclane_mq_put
                  (mq_msg_id, div_part, mq_msg_status, create_ts, last_chg_ts, mq_corr_put_id, mq_msg_data)
        SELECT   'IMQ62', i_r_rlse.div_part, 'OPN', i_r_rlse.rlse_ts, i_r_rlse.rlse_ts, 0,
                 i_r_rlse.div_id
                 || RPAD('IMQ62', 51)
                 || i_r_rlse.rlse_ts_char
                 || ci.cust_id
                 || LPAD(ci.catlg_num, 6, '0')
                 || LPAD(q.max_qty, 7, '0')
                 || LPAD(SUM(l.qty), 7, '0')
            FROM wkly_max_log_op3m l, wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q
           WHERE l.div_part = i_r_rlse.div_part
             AND l.rlse_ts = i_r_rlse.rlse_ts
             AND l.qty_typ = 'CUT'
             AND ci.div_part = l.div_part
             AND ci.cust_item_sid = l.cust_item_sid
             AND q.div_part = ci.div_part
             AND q.cust_item_sid = ci.cust_item_sid
             AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                               FROM wkly_max_qty_op2m q2
                              WHERE q2.div_part = ci.div_part
                                AND q2.cust_item_sid = ci.cust_item_sid
                                AND l.rlse_ts BETWEEN q2.eff_dt AND q2.end_dt)
        GROUP BY ci.cust_id, ci.catlg_num, q.max_qty
        ORDER BY 7;

      COMMIT;
    END IF;   -- i_r_rlse.test_bil_cd = '~'

    logs.dbg('Log End of Allocate');
    log_prcs_step_sp(i_r_rlse, g_c_prcs_end_alloc);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END allocate_sp;

  PROCEDURE allocate_sp(
    i_div          IN  VARCHAR2,
    i_rlse_ts      IN  DATE,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_r_rlse  g_cur_rlse%ROWTYPE;
  BEGIN
    l_r_rlse := rlse_info_fn(i_div, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);
    allocate_sp(l_r_rlse);
  END allocate_sp;

  /*
  ||----------------------------------------------------------------------------
  || RLSE_ALLOC_SP
  ||  Start the allocation process. Send MQ messages to the mainframe. Create
  ||  manifest reports. Ftp order extract (QOPRC07) and manfiest reports to the
  ||  mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original PIR3593
  || 01/08/07 | rhalpai | Added logic to check for test database and continue
  ||                    | processing when putting MQ messages to an MQ queue
  ||                    | that is not yet set up in the test system.
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  ||                    | Changed to log step to be executed instead of step
  ||                    | that just finished executing.
  || 03/01/10 | rhalpai | Removed LLRDate and TestBillCd parms and added logic
  ||                    | to look them up using ReleaseTS. Removed logic for
  ||                    | SET_STEP_SP, INIT_STEPS_SP and UPD_LLR_STATUS_SP.
  ||                    | Converted to use LOG_PRCS_STEP_SP for logging process
  ||                    | step. Moved Process Control logic from ALLOCATE_SP.
  ||                    | PIR0024
  || 05/20/10 | rhalpai | Added logic to update event log. PIR8377
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/05/11 | rhalpai | Added logic to send WklyMaxQty Cut MQ records
  ||                    | for non-testbills. PIR6235
  || 11/17/11 | rhalpai | Add logic to turn OFF RLSE_COMPL Process Control for
  ||                    | TestBills whenever RLSE_ALLOC is turned OFF.
  ||                    | IM-033180
  || 05/13/13 | rhalpai | Change logic to indicate TestDB when DatabaseName
  ||                    | does not begin with P. PIR11038
  || 07/01/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  || 05/22/20 | rhalpai | Add logic to ftp ECOM_MOQ extract. PIR19810
  ||----------------------------------------------------------------------------
  */
  PROCEDURE rlse_alloc_sp(
    i_div          IN  VARCHAR2,
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm        := 'OP_ALLOCATE_PK.RLSE_ALLOC_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80)         := 'Initial';
    l_r_rlse             g_cur_rlse%ROWTYPE;
    l_test_db_sw         VARCHAR2(1);
    l_llr_num            PLS_INTEGER;

    PROCEDURE mq_put_sp(
      i_r_rlse  IN  g_cur_rlse%ROWTYPE,
      i_msg_id  IN  VARCHAR2
    ) IS
      l_msg_id  mclane_mq_put.mq_msg_id%TYPE;
      l_rc      PLS_INTEGER;

      FUNCTION msg_exists_fn(
        i_div_part  IN  NUMBER,
        i_msg_id    IN  VARCHAR2,
        i_rlse_ts   IN  DATE
      )
        RETURN BOOLEAN IS
        l_cv         SYS_REFCURSOR;
        l_exists_sw  VARCHAR2(1);
      BEGIN
        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM mclane_mq_put p
            WHERE p.div_part = i_div_part
              AND p.mq_msg_id = i_msg_id
              AND p.create_ts = i_rlse_ts
              AND p.mq_msg_status = 'OPN';

        FETCH l_cv
         INTO l_exists_sw;

        RETURN(l_exists_sw IS NOT NULL);
      END msg_exists_fn;
    BEGIN
      log_prcs_step_sp(i_r_rlse, i_msg_id);
      l_msg_id :=(CASE
                    WHEN i_r_rlse.test_bil_cd = '~' THEN i_msg_id
                    ELSE SUBSTR(i_msg_id, 1, 1) || 'T' || SUBSTR(i_msg_id, 3)
                  END
                 );

      IF msg_exists_fn(i_r_rlse.div_part, l_msg_id, i_r_rlse.rlse_ts) THEN
        op_mq_message_pk.mq_put_sp(l_msg_id, i_r_rlse.div_id, NULL, l_rc);
        excp.assert((   l_rc = 0
                     OR l_test_db_sw = 'Y'), 'Failed to put ' || l_msg_id || ' msgs to MQ');
      END IF;   -- msg_exists_fn(i_r_rlse.div_part, l_msg_id, i_r_rlse.rlse_ts)
    END mq_put_sp;

    PROCEDURE file_to_upper_case_sp(
      i_local_file  IN  VARCHAR2
    ) IS
      l_cmd        typ.t_maxvc2;
      l_os_result  typ.t_maxvc2;
    BEGIN
      l_cmd := 'cat /ftptrans/'
               || i_local_file
               || ' | tr ''a-z'' ''A-Z'' > /tmp/tmp.$$;mv /tmp/tmp.$$ /ftptrans/'
               || i_local_file;
      logs.dbg(l_cmd);
      l_os_result := oscmd_fn(l_cmd, g_appl_srvr);
      logs.dbg(l_os_result);
    END file_to_upper_case_sp;

    PROCEDURE ftp_sp(
      i_r_rlse    IN  g_cur_rlse%ROWTYPE,
      i_rmt_file  IN  VARCHAR2
    ) IS
      l_local_file             VARCHAR2(100);
      l_c_archive_sw  CONSTANT VARCHAR2(1)   := 'Y';
    BEGIN
      log_prcs_step_sp(i_r_rlse, i_rmt_file);
      l_local_file := i_r_rlse.div_id
                      ||(CASE i_rmt_file
                           WHEN g_c_prcs_qoprc06 THEN '_CONTAINER_ORDERS_'
                           WHEN g_c_prcs_qoprc07 THEN '_ALLOCATE_ORDERS_'
                           WHEN g_c_prcs_opld01 THEN '_OPLD01_LOADING_MANIFEST_'
                           WHEN g_c_prcs_opld02 THEN '_OPLD02_LOAD_SUMMARY_'
                           WHEN g_c_prcs_opld03 THEN '_OPLD03_LOAD_RECAP_'
                           WHEN g_c_prcs_opld04 THEN '_OPLD04_RELEASE_SUMMARY_'
                           WHEN g_c_prcs_opld05 THEN '_OPLD05_RELEASE_RECAP_'
                           WHEN g_c_prcs_opld06 THEN '_OPLD06_TOTE_RECAP_'
                           WHEN g_c_prcs_opld07 THEN '_OPLD07_STOP_ORDER_RECAP_'
                           WHEN g_c_prcs_opld08 THEN '_OPLD08_STOP_SUMMARY_'
                           WHEN g_c_prcs_ecom_moq_extr THEN '_ECOM_MOQ_'
                         END
                        )
                      || i_r_rlse.rlse_ts_char;

      IF i_rmt_file NOT IN(g_c_prcs_qoprc06, g_c_prcs_qoprc07) THEN
        file_to_upper_case_sp(l_local_file);
      END IF;

      op_ftp_sp(i_r_rlse.div_id, l_local_file, i_rmt_file, l_c_archive_sw);
    END ftp_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Get Release Info');
    l_r_rlse := rlse_info_fn(i_div, i_evnt_que_id, i_cycl_id, i_cycl_dfn_id);

    IF ord_ln_cnt_fn(l_r_rlse, NULL, 'P') > 0 THEN
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_alloc,
                                                  op_process_control_pk.g_c_active,
                                                  l_r_rlse.user_id,
                                                  l_r_rlse.div_part
                                                 );
      l_llr_num := l_r_rlse.llr_dt - g_c_rensoft_seed_dt;
      l_test_db_sw :=(CASE
                        WHEN SUBSTR(ora_database_name, -1) = 'P' THEN 'N'
                        ELSE 'Y'
                      END);
      logs.dbg('Allocate Orders');
      allocate_sp(l_r_rlse);
      l_section := 'Send QOPRC12 Work Orders MQ Msgs';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      mq_put_sp(l_r_rlse, g_c_prcs_qoprc12);
      l_section := 'Send QOPRC18 Tote Forecast MQ Msgs';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      mq_put_sp(l_r_rlse, g_c_prcs_qoprc18);
      l_section := 'FTP Order Extract QOPRC06';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_qoprc06);
      l_section := 'FTP Order Extract QOPRC07';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_qoprc07);
      l_section := 'Create Manifest Reports';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      log_prcs_step_sp(l_r_rlse, g_c_prcs_create_mfst);
      op_manifest_reports_pk.manifest_reports_sp(l_llr_num, l_r_rlse.div_id, l_r_rlse.rlse_ts_char);
      l_section := 'FTP OPLD01 Loading Manifest';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld01);
      l_section := 'FTP OPLD02 Loading Summary';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld02);
      l_section := 'FTP OPLD03 Load Recap';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld03);
      l_section := 'FTP OPLD04 Release Summary';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld04);
      l_section := 'FTP OPLD05 Release Recap';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld05);
      l_section := 'FTP OPLD06 Tote Recap';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld06);
      l_section := 'FTP OPLD07 Stop Order Recap';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld07);
      l_section := 'FTP OPLD08 Stop Summary';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section);
      ftp_sp(l_r_rlse, g_c_prcs_opld08);

      IF l_r_rlse.test_bil_cd = '~' THEN
        l_section := 'FTP ECOM MOQ Extract';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_r_rlse, l_section);
        ftp_sp(l_r_rlse, g_c_prcs_ecom_moq_extr);
        l_section := 'Send IMQ62 WklyMaxQty Cut MQ Msgs';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_r_rlse, l_section);
        mq_put_sp(l_r_rlse, g_c_prcs_imq62);
      END IF;   -- l_r_rlse.test_bil_cd = '~'

      l_section := 'Send QOPRC20 Mainframe Trigger MQ Msg';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_r_rlse, l_section, 1);
      mq_put_sp(l_r_rlse, g_c_prcs_qoprc20);
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_alloc,
                                                  op_process_control_pk.g_c_inactive,
                                                  l_r_rlse.user_id,
                                                  l_r_rlse.div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                  op_process_control_pk.g_c_inactive,
                                                  l_r_rlse.user_id,
                                                  l_r_rlse.div_part
                                                 );

      IF l_r_rlse.test_bil_cd <> '~' THEN
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                    op_process_control_pk.g_c_inactive,
                                                    l_r_rlse.user_id,
                                                    l_r_rlse.div_part
                                                   );
      END IF;   -- l_r_rlse.test_bil_cd <> '~'
    END IF;   -- ord_ln_cnt_fn(l_r_rlse, NULL, 'P') > 0

    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rlse_alloc_sp;

  /*
  ||----------------------------------------------------------------------------
  || EVNT_ALLOC_SP
  ||  Create event to process Allocation
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/10 | rhalpai | Original for PIR8377
  || 10/14/17 | rhalpai | Change to use new CIG_EVENT_MGR_PK.CREATE_INSTANCE.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE evnt_alloc_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.EVNT_ALLOC_SP';
    lar_parm             logs.tar_parm;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Initialize');
    l_org_id := cig_organization_pk.get_div_id(i_div);
    l_evnt_parms := '<parameters>'
                    || '<row><sequence>'
                    || 1
                    || '</sequence><value>'
                    || i_div
                    || '</value></row>'
                    || '</parameters>';
    logs.dbg('Create Event');
    cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                     i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                     i_event_dfn_id         => cig_constants_events_pk.evd_op_allocate,
                                     i_parameters           => l_evnt_parms,
                                     i_div_nm               => i_div,
                                     i_is_script_fw_exec    => 'N',
                                     i_is_complete          => 'Y',
                                     i_pgm_id               => 'PLSQL',
                                     i_user_id              => i_user_id,
                                     o_event_que_id         => l_evnt_que_id
                                    );
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END evnt_alloc_sp;

  /*
  ||----------------------------------------------------------------------------
  || START_ALLOC_SP
  ||  Starts order allocation in a separate thread.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/10 | rhalpai | Original
  || 05/13/13 | rhalpai | Change logic to call xxopAlloc.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  || 07/01/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE start_alloc_sp(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ALLOCATE_PK.START_ALLOC_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_sid                VARCHAR2(10);
    l_cmd                typ.t_maxvc2;
    l_appl_srvr          VARCHAR2(20);
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    env.tag();
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_sid := SYS_CONTEXT('USERENV', 'DB_NAME');
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_cmd := '/local/prodcode/bin/xxopAlloc.sub "'
             || i_div
             || '" "'
             || i_user_id
             || '" "'
             || l_sid
             || '"';
    logs.dbg('Run Control-M Sub Script in Background' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
    env.untag();
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END start_alloc_sp;

  /*
  ||----------------------------------------------------------------------------
  || Add_DummyQOPRC20Entry_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/13/02 | rhalpai | Added test bill automation logic to change mq queue
  ||                    | names for test bills.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 05/05/09 | rhalpai | Reformatted and added standard error handling logic.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_dummyqoprc20entry_sp(
    i_div           IN      VARCHAR2,
    i_rlse_ts_char  IN      VARCHAR2,
    o_status        OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                := 'OP_ALLOCATE_PK.ADD_DUMMYQOPRC20ENTRY_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_rlse_ts  CONSTANT DATE                         := TO_DATE(i_rlse_ts_char, 'YYYYMMDDHH24MISS');
    l_fnsh_msg            VARCHAR2(416);
    l_test_bil_cd         rlse_op1z.test_bil_cd%TYPE;
    l_mq_msgid_trigger    VARCHAR2(7);
    l_cnt                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts_char);
    logs.info('ENTRY', lar_parm);
    -- Build the Finished Record Character String
    o_status := 'Good';
    logs.dbg('Test Bill Check');

    SELECT r.div_part, r.test_bil_cd
      INTO l_div_part, l_test_bil_cd
      FROM div_mstr_di1d d, rlse_op1z r
     WHERE d.div_id = i_div
       AND r.div_part = d.div_part
       AND r.rlse_ts = l_c_rlse_ts;

    l_mq_msgid_trigger :=(CASE
                            WHEN l_test_bil_cd = '~' THEN 'QOPRC20'
                            ELSE 'QTPRC20'
                          END);
    logs.dbg('Add QOPRC20 entry to mclane_mq_put table');
    l_fnsh_msg := i_div
                  || RPAD(l_mq_msgid_trigger, 8)
                  || RPAD(i_rlse_ts_char, 30)
                  || RPAD('ADD', 13)
                  || RPAD('DUMMY-STRATEGY', 40)
                  || RPAD('DUMMY-USER', 20)
                  || LPAD('0', 9, '0')
                  || LPAD('0', 9, '0')
                  || LPAD('0', 9, '0')
                  || LPAD('0', 3, '0');
    logs.dbg('Insert Finished Forecast MQ Record');
    l_cnt := op_mclane_mq_put_pk.ins_fn(l_div_part, l_mq_msgid_trigger, l_fnsh_msg, l_c_rlse_ts);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END add_dummyqoprc20entry_sp;
END op_allocate_pk;
/

