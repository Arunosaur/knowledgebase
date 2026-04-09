CREATE OR REPLACE PACKAGE op_split_ord_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_split_typ_strict_ord  CONSTANT VARCHAR2(10)  := 'STRICT ORD';
  g_c_split_typ_hazmat      CONSTANT VARCHAR2(10)  := 'HAZMAT';
  g_c_split_typ_vegas_hlms  CONSTANT VARCHAR2(10)  := 'VEGAS HLMS';
  g_c_split_typ_regbev      CONSTANT VARCHAR2(10)  := 'REGBEV';
  g_c_hazmat_msg            CONSTANT VARCHAR2(200)
    := 'Order contains a mixture of HazMat (hazardous material) and'
       || ' non-HazMat items for customer in a HazMat regulated state!';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION split_types_for_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN type_stab;

  FUNCTION split_ordln_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_split_typ    IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER DEFAULT NULL
  )
    RETURN type_stab;

  FUNCTION existing_split_ord_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_split_typ    IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER DEFAULT NULL
  )
    RETURN NUMBER;

  FUNCTION is_split_for_strict_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_conf_num     IN  VARCHAR2 DEFAULT NULL,
    i_mcl_cust     IN  VARCHAR2 DEFAULT NULL,
    i_po_num       IN  VARCHAR2 DEFAULT '~',
    i_t_mcl_items  IN  type_stab DEFAULT NULL,
    i_t_ord_lns    IN  type_stab DEFAULT NULL
  )
    RETURN VARCHAR2;

  FUNCTION split_ord_warning_msgs_fn(
    i_div_part          IN  NUMBER,
    i_ord_num           IN  NUMBER,
    i_conf_num          IN  VARCHAR2,
    i_mcl_cust          IN  VARCHAR2 DEFAULT NULL,
    i_po_num            IN  VARCHAR2 DEFAULT '~',
    i_t_mcl_items       IN  type_stab DEFAULT NULL,
    i_t_cancel_ord_lns  IN  type_stab DEFAULT NULL
  )
    RETURN type_stab;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE po_split_info_sp(
    i_div_part            IN      NUMBER,
    i_ord_num             IN      NUMBER,
    o_allw_split_sw       OUT     VARCHAR2,
    o_new_po_on_split_sw  OUT     VARCHAR2,
    o_allw_mix_sw         OUT     VARCHAR2,
    i_conf_num            IN      VARCHAR2 DEFAULT NULL,
    i_mcl_cust            IN      VARCHAR2 DEFAULT NULL,
    i_po_num              IN      VARCHAR2 DEFAULT '~'
  );
END op_split_ord_pk;
/

CREATE OR REPLACE PACKAGE BODY op_split_ord_pk IS
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
  || SPLIT_TYPES_FOR_ORD_FN
  ||  Get the different reasons order lines will be split to another order.
  ||  Used during Order Receipt.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 12/17/07 | rhalpai | Changed fetch to use bulk collect. PIR5341
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 08/10/12 | rhalpai | Add logic to handle Regulated Beverage items. PIR11647
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm. PIR15697
  || 02/14/18 | rhalpai | Add logic to handle new SPLCRP and SPLGRP split types.
  ||                    | PIR17722
  || 08/01/22 | rhalpai | Add logic for VAPCBD. PIR22000
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_types_for_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN type_stab IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_SPLIT_ORD_PK.SPLIT_TYPES_FOR_ORD_FN';
    lar_parm               logs.tar_parm;
    l_t_excl_regbev_corps  type_stab;
    l_t_split_typs         type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_reg_bev);

    SELECT   sd.split_typ
    BULK COLLECT INTO l_t_split_typs
        FROM split_dmn_op8s sd
       WHERE EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = i_div_part
                       AND a.ordnoa = i_ord_num
                       AND a.stata IN('O', 'I')
                       AND a.dsorda = 'R')
         AND (   EXISTS(SELECT 1
                          FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e
                         WHERE s.div_part = i_div_part
                           AND s.split_typ = sd.split_typ
                           AND si.div_part = s.div_part
                           AND si.cbr_vndr_id = s.cbr_vndr_id
                           AND e.iteme = si.item_num
                           AND e.uome = si.uom
                           AND EXISTS(SELECT 1
                                        FROM ordp120b b
                                       WHERE b.div_part = s.div_part
                                         AND b.ordnob = i_ord_num
                                         AND e.catite IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
              OR EXISTS(SELECT 1
                          FROM split_sta_itm_op1s s
                         WHERE s.split_typ = sd.split_typ
                           AND EXISTS(SELECT 1
                                        FROM ordp100a a, mclp030c ct, ordp120b b
                                       WHERE a.div_part = i_div_part
                                         AND a.ordnoa = i_ord_num
                                         AND ct.div_part = a.div_part
                                         AND ct.custc = a.custa
                                         AND ct.taxjrc = s.state_cd
                                         AND b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND s.mcl_item IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
              OR EXISTS(SELECT 1
                          FROM split_cus_itm_op1c s
                         WHERE s.split_typ = sd.split_typ
                           AND s.div_part = i_div_part
                           AND EXISTS(SELECT 1
                                        FROM ordp100a a, ordp120b b
                                       WHERE a.div_part = s.div_part
                                         AND a.ordnoa = i_ord_num
                                         AND a.custa = s.cbr_cust
                                         AND b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND s.mcl_item IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e bv
                         WHERE bv.div_part = i_div_part
                           AND bv.cls_typ = sd.split_typ
                           AND bv.cls_typ = 'REGBEV'
                           AND EXISTS(SELECT 1
                                        FROM ordp100a a, ordp120b b
                                       WHERE a.div_part = bv.div_part
                                         AND a.ordnoa = i_ord_num
                                         AND (   TRIM(REPLACE(a.cpoa, '0')) IS NULL
                                              OR EXISTS(SELECT 1
                                                          FROM TABLE(CAST(l_t_excl_regbev_corps AS type_stab)) t,
                                                               mclp020b cx
                                                         WHERE cx.div_part = a.div_part
                                                           AND cx.custb = a.custa
                                                           AND cx.corpb = TO_NUMBER(t.column_value))
                                             )
                                         AND b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND bv.catlg_num IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e bv
                         WHERE bv.div_part = i_div_part
                           AND bv.cls_typ = sd.split_typ
                           AND bv.cls_typ = 'VAPCBD'
                           AND EXISTS(SELECT 1
                                        FROM ordp100a a, ordp120b b
                                       WHERE a.div_part = bv.div_part
                                         AND a.ordnoa = i_ord_num
                                         AND b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND bv.catlg_num IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e s
                         WHERE s.div_part = i_div_part
                           AND s.cls_typ = sd.split_typ
                           AND s.cls_typ = 'SPLCRP'
                           AND EXISTS(SELECT 1
                                        FROM ordp100a a, mclp020b cx, ordp120b b
                                       WHERE a.div_part = s.div_part
                                         AND a.ordnoa = i_ord_num
                                         AND cx.div_part = a.div_part
                                         AND cx.custb = a.custa
                                         AND cx.corpb = TO_NUMBER(s.cls_id)
                                         AND b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND s.catlg_num IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e s
                         WHERE s.div_part = i_div_part
                           AND s.cls_typ = sd.split_typ
                           AND s.cls_typ = 'SPLGRP'
                           AND EXISTS(SELECT 1
                                        FROM ordp100a a, sysp200c c, ordp120b b
                                       WHERE a.div_part = s.div_part
                                         AND a.ordnoa = i_ord_num
                                         AND c.div_part = a.div_part
                                         AND c.acnoc = a.custa
                                         AND c.retgpc = s.cls_id
                                         AND b.div_part = a.div_part
                                         AND b.ordnob = a.ordnoa
                                         AND s.catlg_num IN(b.orgitb, b.orditb)
                                         AND b.statb IN('O', 'I')
                                         AND b.subrcb = 0
                                         AND b.lineb NOT IN(SELECT so.ord_ln
                                                              FROM split_ord_op2s so
                                                             WHERE so.div_part = i_div_part
                                                               AND so.ord_num = i_ord_num)))
             )
    ORDER BY sd.priorty, sd.split_typ;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_split_typs);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_types_for_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_ORDLN_FN
  ||  Return table of order lines for split type.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 08/10/12 | rhalpai | Add logic to handle Regulated Beverage items. PIR11647
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm. PIR15697
  || 02/14/18 | rhalpai | Add logic to handle new SPLCRP and SPLGRP split types.
  ||                    | PIR17722
  || 08/01/22 | rhalpai | Add logic for VAPCBD. PIR22000
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_ordln_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_split_typ    IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER DEFAULT NULL
  )
    RETURN type_stab IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_SPLIT_ORD_PK.SPLIT_ORDLN_FN';
    lar_parm               logs.tar_parm;
    l_t_excl_regbev_corps  type_stab;
    l_t_ord_lns            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.dbg('ENTRY', lar_parm);
    l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_reg_bev);

    SELECT   b.lineb AS ord_ln
    BULK COLLECT INTO l_t_ord_lns
        FROM ordp100a a, ordp120b b
       WHERE a.div_part = i_div_part
         AND a.ordnoa = i_ord_num
         AND a.stata = 'O'
         AND a.dsorda = 'R'
         AND NVL(a.pshipa, '1') IN('1', 'Y')
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND (   EXISTS(SELECT 1
                          FROM split_div_vnd_op3s s, sawp505e e, strct_item_op3v si
                         WHERE s.split_typ = i_split_typ
                           AND s.div_part = b.div_part
                           AND e.catite IN(b.orgitb, b.orditb)
                           AND si.div_part = s.div_part
                           AND si.item_num = e.iteme
                           AND si.uom = e.uome
                           AND s.cbr_vndr_id = si.cbr_vndr_id
                           AND (   i_cbr_vndr_id IS NULL
                                OR s.cbr_vndr_id = i_cbr_vndr_id))
              OR EXISTS(SELECT 1
                          FROM split_sta_itm_op1s s, mclp030c ct
                         WHERE s.split_typ = i_split_typ
                           AND ct.div_part = a.div_part
                           AND ct.custc = a.custa
                           AND s.state_cd = ct.taxjrc
                           AND s.mcl_item IN(b.orgitb, b.orditb))
              OR EXISTS(SELECT 1
                          FROM split_cus_itm_op1c s
                         WHERE s.split_typ = i_split_typ
                           AND s.div_part = a.div_part
                           AND s.cbr_cust = a.custa
                           AND s.mcl_item IN(b.orgitb, b.orditb))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e bv
                         WHERE i_split_typ = 'REGBEV'
                           AND bv.cls_typ = 'REGBEV'
                           AND bv.div_part = a.div_part
                           AND bv.catlg_num IN(b.orgitb, b.orditb)
                           AND (   TRIM(REPLACE(a.cpoa, '0')) IS NULL
                                OR EXISTS(SELECT 1
                                            FROM TABLE(CAST(l_t_excl_regbev_corps AS type_stab)) t, mclp020b cx
                                           WHERE cx.div_part = a.div_part
                                             AND cx.custb = a.custa
                                             AND cx.corpb = TO_NUMBER(t.column_value))
                               ))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e bv
                         WHERE i_split_typ = 'VAPCBD'
                           AND bv.cls_typ = 'VAPCBD'
                           AND bv.div_part = a.div_part
                           AND bv.catlg_num IN(b.orgitb, b.orditb))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e sc, mclp020b cx
                         WHERE i_split_typ = 'SPLCRP'
                           AND sc.cls_typ = 'SPLCRP'
                           AND sc.div_part = a.div_part
                           AND sc.catlg_num IN(b.orgitb, b.orditb)
                           AND cx.div_part = a.div_part
                           AND cx.custb = a.custa
                           AND cx.corpb = TO_NUMBER(sc.cls_id))
              OR EXISTS(SELECT 1
                          FROM item_grp_op2e sg, sysp200c c
                         WHERE i_split_typ = 'SPLGRP'
                           AND sg.cls_typ = 'SPLGRP'
                           AND sg.div_part = a.div_part
                           AND sg.catlg_num IN(b.orgitb, b.orditb)
                           AND c.div_part = a.div_part
                           AND c.acnoc = a.custa
                           AND c.retgpc = sg.cls_id)
             )
         AND b.lineb NOT IN(SELECT so.ord_ln
                              FROM split_ord_op2s so
                             WHERE so.div_part = i_div_part
                               AND so.ord_num = i_ord_num)
         AND b.lineb NOT IN(SELECT st.ord_ln
                              FROM strct_ord_op1o st
                             WHERE st.div_part = i_div_part
                               AND st.ord_num = i_ord_num)
    ORDER BY ord_ln;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_ord_lns);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_ordln_fn;

  /*
  ||----------------------------------------------------------------------------
  || EXISTING_SPLIT_ORD_FN
  ||  Find existing order for split_typ/llrdate/load/stop/cust.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 11/06/08 | rhalpai | Changed logic to use new STAT column. PIR5002
  || 08/10/12 | rhalpai | Add logic to handle Regulated Beverage items. PIR11647
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use LoadDepartSid. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm. PIR15697
  || 02/14/18 | rhalpai | Add logic to handle new SPLCRP and SPLGRP split types.
  ||                    | PIR17722
  || 08/01/22 | rhalpai | Add logic for VAPCBD. PIR22000
  ||----------------------------------------------------------------------------
  */
  FUNCTION existing_split_ord_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_split_typ    IN  VARCHAR2,
    i_cbr_vndr_id  IN  NUMBER DEFAULT NULL
  )
    RETURN NUMBER IS
    l_c_module      CONSTANT typ.t_maxfqnm := 'OP_SPLIT_ORD_PK.EXISTING_SPLIT_ORD_FN';
    lar_parm                 logs.tar_parm;
    l_t_excl_regbev_corps    type_stab;
    l_cv                     SYS_REFCURSOR;
    l_c_strict_ord  CONSTANT VARCHAR2(10)  := 'STRICT ORD';
    l_ord_num                NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.dbg('ENTRY', lar_parm);
    l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_reg_bev);

    OPEN l_cv
     FOR
       SELECT a.ordnoa
         FROM ordp100a a
        WHERE a.div_part = i_div_part
          AND a.stata = 'O'
          AND a.excptn_sw = 'N'
          AND a.dsorda = 'R'
          AND (   TRIM(REPLACE(a.cpoa, '0')) IS NULL
               OR a.cpoa = i_split_typ)
          AND a.ordnoa <> i_ord_num
          AND (a.dsorda, a.ldtypa, a.load_depart_sid, a.custa) =
                                                             (SELECT a2.dsorda, a2.ldtypa, a2.load_depart_sid, a2.custa
                                                                FROM ordp100a a2
                                                               WHERE a2.div_part = i_div_part
                                                                 AND a2.ordnoa = i_ord_num)
          AND a.ordnoa NOT IN(SELECT r.ordnor
                                FROM mclpinpr r
                               WHERE r.div_part = i_div_part)
          AND EXISTS(SELECT 1
                       FROM split_ord_op2s so
                      WHERE so.split_typ = i_split_typ
                        AND so.div_part = i_div_part
                        AND so.ord_num = a.ordnoa)
          AND (   i_split_typ <> l_c_strict_ord
               OR EXISTS(SELECT 1
                           FROM strct_ord_op1o st
                          WHERE st.div_part = a.div_part
                            AND st.cbr_vndr_id = i_cbr_vndr_id
                            AND st.ord_num = a.ordnoa
                            AND st.stat = 'URC')
              )
          AND NOT EXISTS(SELECT 1
                           FROM ordp120b b
                          WHERE b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND b.statb = 'O'
                            AND b.subrcb = 0
                            AND NOT EXISTS(SELECT 1
                                             FROM split_div_vnd_op3s s, strct_item_op3v si
                                            WHERE s.split_typ = i_split_typ
                                              AND s.div_part = si.div_part
                                              AND s.cbr_vndr_id = si.cbr_vndr_id
                                              AND si.div_part = b.div_part
                                              AND si.item_num = b.itemnb
                                              AND si.uom = b.sllumb
                                              AND (   i_cbr_vndr_id IS NULL
                                                   OR s.cbr_vndr_id = i_cbr_vndr_id))
                            AND NOT EXISTS(SELECT 1
                                             FROM split_sta_itm_op1s s, mclp030c ct
                                            WHERE s.split_typ = i_split_typ
                                              AND s.state_cd = ct.taxjrc
                                              AND ct.div_part = a.div_part
                                              AND ct.custc = a.custa
                                              AND s.mcl_item = b.orditb)
                            AND NOT EXISTS(SELECT 1
                                             FROM split_cus_itm_op1c s
                                            WHERE s.div_part = a.div_part
                                              AND s.split_typ = i_split_typ
                                              AND s.cbr_cust = a.custa
                                              AND s.mcl_item = b.orditb)
                            AND NOT EXISTS(SELECT 1
                                             FROM item_grp_op2e bv
                                            WHERE i_split_typ = 'REGBEV'
                                              AND bv.cls_typ = 'REGBEV'
                                              AND bv.div_part = b.div_part
                                              AND bv.catlg_num = b.orditb
                                              AND (   TRIM(REPLACE(a.cpoa, '0')) IS NULL
                                                   OR EXISTS(
                                                        SELECT 1
                                                          FROM TABLE(CAST(l_t_excl_regbev_corps AS type_stab)) t,
                                                               mclp020b cx
                                                         WHERE cx.div_part = a.div_part
                                                           AND cx.custb = a.custa
                                                           AND cx.corpb = TO_NUMBER(t.column_value))
                                                  ))
                            AND NOT EXISTS(SELECT 1
                                             FROM item_grp_op2e bv
                                            WHERE i_split_typ = 'VAPCBD'
                                              AND bv.cls_typ = 'VAPCBD'
                                              AND bv.div_part = b.div_part
                                              AND bv.catlg_num = b.orditb)
                            AND NOT EXISTS(SELECT 1
                                             FROM item_grp_op2e sc, mclp020b cx
                                            WHERE i_split_typ = 'SPLCRP'
                                              AND sc.cls_typ = 'SPLCRP'
                                              AND sc.div_part = b.div_part
                                              AND sc.catlg_num = b.orditb
                                              AND cx.div_part = a.div_part
                                              AND cx.custb = a.custa
                                              AND cx.corpb = TO_NUMBER(sc.cls_id))
                            AND NOT EXISTS(SELECT 1
                                             FROM item_grp_op2e sg, sysp200c c
                                            WHERE i_split_typ = 'SPLGRP'
                                              AND sg.cls_typ = 'SPLGRP'
                                              AND sg.div_part = b.div_part
                                              AND sg.catlg_num = b.orditb
                                              AND c.div_part = a.div_part
                                              AND c.acnoc = a.custa
                                              AND c.retgpc = sg.cls_id));

    FETCH l_cv
     INTO l_ord_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_ord_num);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END existing_split_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_SPLIT_FOR_STRICT_FN
  ||  Indicate whether order will be split for strict vendor item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm. Change logic to include new
  ||                    | AllowMixSw parm in call to PO_SPLIT_INFO_SP. PIR15697
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_split_for_strict_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_conf_num     IN  VARCHAR2 DEFAULT NULL,
    i_mcl_cust     IN  VARCHAR2 DEFAULT NULL,
    i_po_num       IN  VARCHAR2 DEFAULT '~',
    i_t_mcl_items  IN  type_stab DEFAULT NULL,
    i_t_ord_lns    IN  type_stab DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_SPLIT_ORD_PK.IS_SPLIT_FOR_STRICT_FN';
    lar_parm              logs.tar_parm;
    l_allw_split_sw       VARCHAR2(1)   := 'Y';
    l_new_po_on_split_sw  VARCHAR2(1);
    l_allw_mix_sw         VARCHAR2(1);
    l_cv                  SYS_REFCURSOR;
    l_split_sw            VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    logs.add_parm(lar_parm, 'MclItemsTab', i_t_mcl_items);
    logs.add_parm(lar_parm, 'OrdLnsTab', i_t_ord_lns);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get PO Split Info');
    op_split_ord_pk.po_split_info_sp(i_div_part,
                                     i_ord_num,
                                     l_allw_split_sw,
                                     l_new_po_on_split_sw,
                                     l_allw_mix_sw,
                                     i_conf_num,
                                     i_mcl_cust,
                                     i_po_num
                                    );

    IF l_allw_split_sw = 'Y' THEN
      IF NVL(i_ord_num, 0) > 0 THEN
        logs.dbg('Open Cursor for Existing Order');

        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM split_div_vnd_op3s s, strct_item_op3v si
            WHERE s.div_part = i_div_part
              AND s.split_typ = 'STRICT ORD'
              AND si.div_part = s.div_part
              AND si.cbr_vndr_id = s.cbr_vndr_id
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = si.div_part
                            AND a.ordnoa = i_ord_num
                            AND a.stata IN('O', 'I')
                            AND a.dsorda = 'R')
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a, ordp120b b
                              WHERE a.div_part = si.div_part
                                AND a.ordnoa = i_ord_num
                                AND NVL(a.pshipa, '1') IN('1', 'Y')
                                AND b.div_part = a.div_part
                                AND b.ordnob = i_ord_num
                                AND b.itemnb = si.item_num
                                AND b.sllumb = si.uom
                                AND b.statb IN('O', 'I')
                                AND b.lineb NOT IN(SELECT st.ord_ln
                                                     FROM strct_ord_op1o st
                                                    WHERE st.div_part = i_div_part
                                                      AND st.ord_num = i_ord_num)
                                AND (   i_t_ord_lns IS NULL
                                     OR b.lineb IN(SELECT t.column_value
                                                     FROM TABLE(CAST(i_t_ord_lns AS type_stab)) t)))
                   OR (    i_t_mcl_items IS NOT NULL
                       AND EXISTS(SELECT 1
                                    FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t, sawp505e e
                                   WHERE t.column_value = e.catite
                                     AND e.iteme = si.item_num
                                     AND e.uome = si.uom)
                      )
                  )
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a, ordp120b b
                              WHERE a.div_part = si.div_part
                                AND a.ordnoa = i_ord_num
                                AND b.div_part = a.div_part
                                AND b.ordnob = i_ord_num
                                AND b.statb IN('O', 'I')
                                AND NOT EXISTS(SELECT 1
                                                 FROM split_div_vnd_op3s s2, strct_item_op3v si2
                                                WHERE s2.split_typ = s.split_typ
                                                  AND s2.div_part = si2.div_part
                                                  AND s2.cbr_vndr_id = si2.cbr_vndr_id
                                                  AND si2.div_part = b.div_part
                                                  AND si2.item_num = b.itemnb
                                                  AND si2.uom = b.sllumb))
                   OR (    i_t_mcl_items IS NOT NULL
                       AND EXISTS(SELECT 1
                                    FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t, sawp505e e2
                                   WHERE t.column_value = e2.catite
                                     AND NOT EXISTS(SELECT 1
                                                      FROM split_div_vnd_op3s s2, strct_item_op3v si2
                                                     WHERE s2.split_typ = s.split_typ
                                                       AND s2.div_part = si2.div_part
                                                       AND s2.cbr_vndr_id = si2.cbr_vndr_id
                                                       AND si2.div_part = s.div_part
                                                       AND si2.item_num = e2.iteme
                                                       AND si2.uom = e2.uome))
                      )
                  );
      ELSE
        logs.dbg('Open Cursor for New Order');

        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM split_div_vnd_op3s s, strct_item_op3v si
            WHERE s.div_part = i_div_part
              AND s.split_typ = 'STRICT ORD'
              AND si.div_part = s.div_part
              AND si.cbr_vndr_id = s.cbr_vndr_id
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a, ordp120b b
                              WHERE a.connba = i_conf_num
                                AND a.div_part = si.div_part
                                AND a.stata IN('O', 'I')
                                AND a.dsorda = 'R'
                                AND NVL(a.pshipa, '1') IN('1', 'Y')
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND b.itemnb = si.item_num
                                AND b.sllumb = si.uom
                                AND b.statb IN('O', 'I')
                                AND (   i_t_ord_lns IS NULL
                                     OR b.lineb IN(SELECT t.column_value
                                                     FROM TABLE(CAST(i_t_ord_lns AS type_stab)) t)))
                   OR (    i_t_mcl_items IS NOT NULL
                       AND EXISTS(SELECT 1
                                    FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t, sawp505e e
                                   WHERE t.column_value = e.catite
                                     AND e.iteme = si.item_num
                                     AND e.uome = si.uom)
                      )
                  )
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a, ordp120b b
                              WHERE a.connba = i_conf_num
                                AND a.div_part = si.div_part
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND b.statb IN('O', 'I')
                                AND NOT EXISTS(SELECT 1
                                                 FROM split_div_vnd_op3s s2, strct_item_op3v si2
                                                WHERE s2.split_typ = s.split_typ
                                                  AND s2.div_part = si2.div_part
                                                  AND s2.cbr_vndr_id = si2.cbr_vndr_id
                                                  AND si.div_part = b.div_part
                                                  AND si.item_num = b.itemnb
                                                  AND si.uom = b.sllumb))
                   OR (    i_t_mcl_items IS NOT NULL
                       AND EXISTS(SELECT 1
                                    FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t, sawp505e e2
                                   WHERE t.column_value = e2.catite
                                     AND NOT EXISTS(SELECT 1
                                                      FROM split_div_vnd_op3s s2, strct_item_op3v si2
                                                     WHERE s2.div_part = s.div_part
                                                       AND s2.split_typ = s.split_typ
                                                       AND s2.div_part = si2.div_part
                                                       AND s2.cbr_vndr_id = si2.cbr_vndr_id
                                                       AND si2.item_num = e2.iteme
                                                       AND si2.uom = e2.uome))
                      )
                  );
      END IF;   -- NVL(i_ord_num, 0) > 0

      logs.dbg('Fetch Cursor');

      FETCH l_cv
       INTO l_split_sw;
    END IF;   -- l_allw_split_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_split_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_split_for_strict_fn;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_ORD_WARNING_MSGS_FN
  ||  Get warning msgs for order containing mixture of split and non-split items.
  ||  Used during CSR validation.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 04/04/08 | rhalpai | Changed cursor to include existing split item order
  ||                    | lines. IM397029
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 08/10/12 | rhalpai | Add logic to handle Regulated Beverage items. PIR11647
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm. Change logic to include new
  ||                    | AllowMixSw parm in call to PO_SPLIT_INFO_SP. PIR15697
  || 01/28/16 | rhalpai | Change logic to include cust state when matching for
  ||                    | HazMat items.
  || 02/14/18 | rhalpai | Add logic to handle new SPLCRP and SPLGRP split types.
  ||                    | PIR17722
  || 08/01/22 | rhalpai | Add logic for VAPCBD. PIR22000
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_ord_warning_msgs_fn(
    i_div_part          IN  NUMBER,
    i_ord_num           IN  NUMBER,
    i_conf_num          IN  VARCHAR2,
    i_mcl_cust          IN  VARCHAR2 DEFAULT NULL,
    i_po_num            IN  VARCHAR2 DEFAULT '~',
    i_t_mcl_items       IN  type_stab DEFAULT NULL,
    i_t_cancel_ord_lns  IN  type_stab DEFAULT NULL
  )
    RETURN type_stab IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_SPLIT_ORD_PK.SPLIT_ORD_WARNING_MSGS_FN';
    lar_parm               logs.tar_parm;
    l_allw_split_sw        VARCHAR2(1)   := 'Y';
    l_new_po_on_split_sw   VARCHAR2(1);
    l_allw_mix_sw          VARCHAR2(1);
    l_t_excl_regbev_corps  type_stab;
    l_t_split_msgs         type_stab     := type_stab();
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    logs.add_parm(lar_parm, 'MclItemsTab', i_t_mcl_items);
    logs.add_parm(lar_parm, 'CancelOrdLnsTab', i_t_cancel_ord_lns);
    logs.dbg('ENTRY', lar_parm);

    IF i_mcl_cust IS NOT NULL THEN
      logs.dbg('Get PO Split Info');
      po_split_info_sp(i_div_part,
                       i_ord_num,
                       l_allw_split_sw,
                       l_new_po_on_split_sw,
                       l_allw_mix_sw,
                       i_conf_num,
                       i_mcl_cust,
                       i_po_num
                      );
    END IF;   -- i_mcl_cust IS NOT NULL

    IF l_allw_split_sw = 'Y' THEN
      l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_reg_bev);
      logs.dbg('Open Cursor');

      SELECT   DECODE(sd.split_typ,
                      op_split_ord_pk.g_c_split_typ_hazmat, op_split_ord_pk.g_c_hazmat_msg,
                      'Order contains a mixture of Split-Order and non-Split-Order'
                      || ' items for a qualified customer of split type ['
                      || sd.split_typ
                      || ']!'
                     )
      BULK COLLECT INTO l_t_split_msgs
          FROM split_dmn_op8s sd
         WHERE sd.split_typ <> op_split_ord_pk.g_c_split_typ_strict_ord
           AND (   NVL(i_ord_num, 0) = 0
                OR EXISTS(SELECT 1
                            FROM ordp100a a
                           WHERE a.div_part = i_div_part
                             AND a.ordnoa = i_ord_num
                             AND a.stata IN('O', 'I')
                             AND a.dsorda = 'R')
               )
           AND (   EXISTS(SELECT 1
                            FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e
                           WHERE s.split_typ = sd.split_typ
                             AND s.div_part = i_div_part
                             AND si.div_part = s.div_part
                             AND si.cbr_vndr_id = s.cbr_vndr_id
                             AND e.iteme = si.item_num
                             AND e.uome = si.uom
                             AND (   EXISTS(SELECT 1
                                              FROM ordp120b b
                                             WHERE b.div_part = si.div_part
                                               AND b.ordnob = i_ord_num
                                               AND e.catite = b.orditb
                                               AND b.statb IN('O', 'I')
                                               AND b.subrcb = 0
                                               AND (   i_t_cancel_ord_lns IS NULL
                                                    OR b.lineb NOT IN(
                                                                      SELECT t.column_value
                                                                        FROM TABLE
                                                                                  (CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                                   ))
                                  OR EXISTS(SELECT 1
                                              FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                             WHERE t.column_value = e.catite)
                                 )
                             AND (   EXISTS(SELECT 1
                                              FROM ordp100a a, ordp120b b
                                             WHERE a.div_part = si.div_part
                                               AND a.ordnoa = i_ord_num
                                               AND b.div_part = a.div_part
                                               AND b.ordnob = i_ord_num
                                               AND b.statb = 'O'
                                               AND b.subrcb = 0
                                               AND NOT EXISTS(
                                                     SELECT 1
                                                       FROM split_div_vnd_op3s s2, strct_item_op3v si2
                                                      WHERE s2.split_typ = s.split_typ
                                                        AND s2.div_part = si2.div_part
                                                        AND s2.cbr_vndr_id = si2.cbr_vndr_id
                                                        AND si2.div_part = b.div_part
                                                        AND si2.item_num = b.itemnb
                                                        AND si2.uom = b.sllumb))
                                  OR EXISTS(SELECT 1
                                              FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                             WHERE NOT EXISTS(
                                                     SELECT 1
                                                       FROM sawp505e e2, split_div_vnd_op3s s2, strct_item_op3v si2
                                                      WHERE s2.split_typ = s.split_typ
                                                        AND s2.div_part = si.div_part
                                                        AND s2.div_part = si2.div_part
                                                        AND s2.cbr_vndr_id = si2.cbr_vndr_id
                                                        AND si2.item_num = e2.iteme
                                                        AND si2.uom = e2.uome
                                                        AND e2.catite = t.column_value))
                                 ))
                OR EXISTS(SELECT 1
                            FROM ordp100a a, mclp030c ct, split_sta_itm_op1s s
                           WHERE a.div_part = i_div_part
                             AND a.ordnoa = i_ord_num
                             AND ct.div_part = a.div_part
                             AND ct.custc = a.custa
                             AND ct.taxjrc = s.state_cd
                             AND s.split_typ = sd.split_typ
                             AND (   EXISTS(SELECT 1
                                              FROM ordp120b b
                                             WHERE b.div_part = i_div_part
                                               AND b.ordnob = i_ord_num
                                               AND b.orditb = s.mcl_item
                                               AND b.statb IN('O', 'I')
                                               AND b.subrcb = 0
                                               AND (   i_t_cancel_ord_lns IS NULL
                                                    OR b.lineb NOT IN(
                                                                      SELECT t.column_value
                                                                        FROM TABLE
                                                                                  (CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                                   ))
                                  OR EXISTS(SELECT 1
                                              FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                             WHERE t.column_value = s.mcl_item)
                                 )
                             AND (   EXISTS(SELECT 1
                                              FROM ordp120b b
                                             WHERE b.div_part = i_div_part
                                               AND b.ordnob = i_ord_num
                                               AND b.statb = 'O'
                                               AND b.subrcb = 0
                                               AND NOT EXISTS(
                                                     SELECT 1
                                                       FROM split_sta_itm_op1s s2
                                                      WHERE s2.split_typ = s.split_typ
                                                        AND s2.state_cd = s.state_cd
                                                        AND s2.mcl_item = b.orditb))
                                  OR EXISTS(SELECT 1
                                              FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                             WHERE NOT EXISTS(
                                                     SELECT 1
                                                       FROM split_sta_itm_op1s s2
                                                      WHERE s2.split_typ = s.split_typ
                                                        AND s2.state_cd = s.state_cd
                                                        AND s2.mcl_item = t.column_value))
                                 ))
                OR EXISTS(SELECT 1
                            FROM split_cus_itm_op1c s
                           WHERE s.split_typ = sd.split_typ
                             AND s.div_part = i_div_part
                             AND (   EXISTS(SELECT 1
                                              FROM ordp100a a, ordp120b b
                                             WHERE a.div_part = s.div_part
                                               AND a.ordnoa = i_ord_num
                                               AND a.custa = s.cbr_cust
                                               AND b.div_part = a.div_part
                                               AND b.ordnob = i_ord_num
                                               AND b.orditb = s.mcl_item
                                               AND b.statb IN('O', 'I')
                                               AND b.subrcb = 0
                                               AND (   i_t_cancel_ord_lns IS NULL
                                                    OR b.lineb NOT IN(
                                                                      SELECT t.column_value
                                                                        FROM TABLE
                                                                                  (CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                                   ))
                                  OR EXISTS(SELECT 1
                                              FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                             WHERE t.column_value = s.mcl_item)
                                 )
                             AND (   EXISTS(SELECT 1
                                              FROM ordp100a a, ordp120b b
                                             WHERE a.div_part = s.div_part
                                               AND a.ordnoa = i_ord_num
                                               AND b.div_part = a.div_part
                                               AND b.ordnob = i_ord_num
                                               AND b.statb = 'O'
                                               AND b.subrcb = 0
                                               AND NOT EXISTS(
                                                     SELECT 1
                                                       FROM split_cus_itm_op1c s2
                                                      WHERE s2.div_part = s.div_part
                                                        AND s2.split_typ = s.split_typ
                                                        AND s2.cbr_cust = a.custa
                                                        AND s2.mcl_item = b.orditb))
                                  OR EXISTS(SELECT 1
                                              FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                             WHERE NOT EXISTS(
                                                     SELECT 1
                                                       FROM split_cus_itm_op1c s2
                                                      WHERE s2.div_part = s.div_part
                                                        AND s2.split_typ = s.split_typ
                                                        AND s2.cbr_cust = s.cbr_cust
                                                        AND s2.mcl_item = t.column_value))
                                 ))
                OR EXISTS(
                     SELECT 1
                       FROM item_grp_op2e bv
                      WHERE bv.div_part = i_div_part
                        AND bv.cls_typ = sd.split_typ
                        AND bv.cls_typ = 'REGBEV'
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND (   TRIM(REPLACE(a.cpoa, '0')) IS NULL
                                               OR EXISTS(SELECT 1
                                                           FROM TABLE(CAST(l_t_excl_regbev_corps AS type_stab)) t,
                                                                mclp020b cx
                                                          WHERE cx.div_part = a.div_part
                                                            AND cx.custb = a.custa
                                                            AND cx.corpb = TO_NUMBER(t.column_value))
                                              )
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = i_ord_num
                                          AND b.statb IN('O', 'I')
                                          AND b.subrcb = 0
                                          AND (   i_t_cancel_ord_lns IS NULL
                                               OR b.lineb NOT IN(SELECT t.column_value
                                                                   FROM TABLE(CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                              )
                                          AND bv.div_part = b.div_part
                                          AND bv.catlg_num = b.orditb)
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE t.column_value = bv.catlg_num)
                            )
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND (   TRIM(REPLACE(a.cpoa, '0')) IS NULL
                                               OR EXISTS(SELECT 1
                                                           FROM TABLE(CAST(l_t_excl_regbev_corps AS type_stab)) t,
                                                                mclp020b cx
                                                          WHERE cx.div_part = a.div_part
                                                            AND cx.custb = a.custa
                                                            AND cx.corpb = TO_NUMBER(t.column_value))
                                              )
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb = 'O'
                                          AND b.subrcb = 0
                                          AND NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e bv2
                                                          WHERE bv2.cls_typ = sd.split_typ
                                                            AND bv2.div_part = b.div_part
                                                            AND bv2.catlg_num = b.orditb))
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e bv2
                                                          WHERE bv2.div_part = i_div_part
                                                            AND bv2.cls_typ = sd.split_typ
                                                            AND bv2.catlg_num = t.column_value))
                            ))
                OR EXISTS(
                     SELECT 1
                       FROM item_grp_op2e v
                      WHERE v.div_part = i_div_part
                        AND v.cls_typ = sd.split_typ
                        AND v.cls_typ = 'VAPCBD'
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = i_ord_num
                                          AND b.statb IN('O', 'I')
                                          AND b.subrcb = 0
                                          AND (   i_t_cancel_ord_lns IS NULL
                                               OR b.lineb NOT IN(SELECT t.column_value
                                                                   FROM TABLE(CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                              )
                                          AND v.div_part = b.div_part
                                          AND v.catlg_num = b.orditb)
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE t.column_value = v.catlg_num)
                            )
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb = 'O'
                                          AND b.subrcb = 0
                                          AND NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e bv2
                                                          WHERE bv2.cls_typ = sd.split_typ
                                                            AND bv2.div_part = b.div_part
                                                            AND bv2.catlg_num = b.orditb))
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e bv2
                                                          WHERE bv2.div_part = i_div_part
                                                            AND bv2.cls_typ = sd.split_typ
                                                            AND bv2.catlg_num = t.column_value))
                            ))
                OR EXISTS(
                     SELECT 1
                       FROM item_grp_op2e sc
                      WHERE sc.div_part = i_div_part
                        AND sc.cls_typ = sd.split_typ
                        AND sc.cls_typ = 'SPLCRP'
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, mclp020b cx, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND cx.div_part = a.div_part
                                          AND cx.custb = a.custa
                                          AND cx.corpb = TO_NUMBER(sc.cls_id)
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = i_ord_num
                                          AND b.statb IN('O', 'I')
                                          AND b.subrcb = 0
                                          AND (   i_t_cancel_ord_lns IS NULL
                                               OR b.lineb NOT IN(SELECT t.column_value
                                                                   FROM TABLE(CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                              )
                                          AND sc.div_part = b.div_part
                                          AND sc.catlg_num = b.orditb)
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE t.column_value = sc.catlg_num)
                            )
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, mclp020b cx, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND cx.div_part = a.div_part
                                          AND cx.custb = a.custa
                                          AND cx.corpb = TO_NUMBER(sc.cls_id)
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb = 'O'
                                          AND b.subrcb = 0
                                          AND NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e sc2
                                                          WHERE sc2.div_part = b.div_part
                                                            AND sc2.cls_typ = sd.split_typ
                                                            AND sc2.cls_id = LPAD(cx.corpb, 3, '0')
                                                            AND sc2.catlg_num = b.orditb))
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e sc2
                                                          WHERE sc2.div_part = i_div_part
                                                            AND sc2.cls_typ = sd.split_typ
                                                            AND sc2.cls_id = sc.cls_id
                                                            AND sc2.catlg_num = t.column_value))
                            ))
                OR EXISTS(
                     SELECT 1
                       FROM item_grp_op2e sg
                      WHERE sg.div_part = i_div_part
                        AND sg.cls_typ = sd.split_typ
                        AND sg.cls_typ = 'SPLGRP'
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, sysp200c c, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND c.div_part = a.div_part
                                          AND c.acnoc = a.custa
                                          AND c.retgpc = sg.cls_id
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = i_ord_num
                                          AND b.statb IN('O', 'I')
                                          AND b.subrcb = 0
                                          AND (   i_t_cancel_ord_lns IS NULL
                                               OR b.lineb NOT IN(SELECT t.column_value
                                                                   FROM TABLE(CAST(i_t_cancel_ord_lns AS type_stab)) t)
                                              )
                                          AND sg.div_part = b.div_part
                                          AND sg.catlg_num = b.orditb)
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE t.column_value = sg.catlg_num)
                            )
                        AND (   EXISTS(SELECT 1
                                         FROM ordp100a a, sysp200c c, ordp120b b
                                        WHERE a.div_part = i_div_part
                                          AND a.ordnoa = i_ord_num
                                          AND c.div_part = a.div_part
                                          AND c.acnoc = a.custa
                                          AND c.retgpc = sg.cls_id
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb = 'O'
                                          AND b.subrcb = 0
                                          AND NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e sg2
                                                          WHERE sg2.div_part = b.div_part
                                                            AND sg2.cls_typ = sd.split_typ
                                                            AND sg2.cls_id = c.retgpc
                                                            AND sg2.catlg_num = b.orditb))
                             OR EXISTS(SELECT 1
                                         FROM TABLE(CAST(i_t_mcl_items AS type_stab)) t
                                        WHERE NOT EXISTS(SELECT 1
                                                           FROM item_grp_op2e sg2
                                                          WHERE sg2.div_part = i_div_part
                                                            AND sg2.cls_typ = sd.split_typ
                                                            AND sg2.cls_id = sg.cls_id
                                                            AND sg2.catlg_num = t.column_value))
                            ))
               )
      ORDER BY sd.priorty;
    END IF;   -- l_allw_split_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_split_msgs);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_ord_warning_msgs_fn;

  /*
  ||----------------------------------------------------------------------------
  || PO_SPLIT_INFO_SP
  ||  Indicate whether customer allows an order split for an existing PO and
  ||  if done whether a new PO is required.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm. Add AllowMixSw out parm and
  ||                    | change logic to include new split_po_cd option "X" to
  ||                    | indicate NO split allowed for PO.
  ||                    | Note: new "X" option also allows mixed orders to be
  ||                    | treated as strict order and strict items in mixed order
  ||                    | are not marked with STRCTERR exception. PIR15697
  ||----------------------------------------------------------------------------
  */
  PROCEDURE po_split_info_sp(
    i_div_part            IN      NUMBER,
    i_ord_num             IN      NUMBER,
    o_allw_split_sw       OUT     VARCHAR2,
    o_new_po_on_split_sw  OUT     VARCHAR2,
    o_allw_mix_sw         OUT     VARCHAR2,
    i_conf_num            IN      VARCHAR2 DEFAULT NULL,
    i_mcl_cust            IN      VARCHAR2 DEFAULT NULL,
    i_po_num              IN      VARCHAR2 DEFAULT '~'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SPLIT_ORD_PK.PO_SPLIT_INFO_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT DECODE(TRIM(REPLACE(x.po, '0')),
                     NULL, 'Y',
                     DECODE(ma.split_po_cd, 'N', 'N', 'X', 'N', 'Y')
                    ) AS allw_split_sw,
              DECODE(ma.split_po_cd, 'R', 'Y', 'N') AS new_po_on_split_sw,
              DECODE(ma.split_po_cd, 'X', 'Y', 'N') AS allw_mix_sw
         FROM mclp100a ma, sysp200c c, mclp020b cx,
              (SELECT SUBSTR(y.cust_po, 1, 6) AS mcl_cust, SUBSTR(y.cust_po, 7) AS po
                 FROM (SELECT COALESCE((SELECT NVL(i_mcl_cust, cx2.mccusb) || DECODE(i_po_num, '~', a.cpoa, i_po_num)
                                          FROM ordp100a a, mclp020b cx2
                                         WHERE a.div_part = i_div_part
                                           AND a.ordnoa = i_ord_num
                                           AND cx2.div_part = a.div_part
                                           AND cx2.custb = a.custa),
                                       (SELECT NVL(i_mcl_cust, cx2.mccusb) || DECODE(i_po_num, '~', a.cpoa, i_po_num)
                                          FROM ordp100a a, mclp020b cx2
                                         WHERE a.div_part = i_div_part
                                           AND a.connba = i_conf_num
                                           AND cx2.div_part = a.div_part
                                           AND cx2.custb = a.custa),
                                       i_mcl_cust || i_po_num
                                      ) AS cust_po
                         FROM DUAL) y) x
        WHERE cx.div_part = i_div_part
          AND cx.mccusb = x.mcl_cust
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb
          AND ma.div_part = c.div_part
          AND ma.cstgpa = c.retgpc;

    FETCH l_cv
     INTO o_allw_split_sw, o_new_po_on_split_sw, o_allw_mix_sw;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END po_split_info_sp;
END op_split_ord_pk;
/

