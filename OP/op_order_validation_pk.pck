CREATE OR REPLACE PACKAGE op_order_validation_pk IS
  /**
  ||----------------------------------------------------------------------------
  || Package with functionality for inspecting orders for exceptions and making
  || log entries to the MCLP300D.
  ||----------------------------------------------------------------------------
  */
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
  /**
  ||----------------------------------------------------------------------------
  || Checks order line for max quantity violations.
  || Will use the lessor of the item max and order max parameters to determine
  || exception.
  || #param i_div_part       DivPart
  || #param i_ord_num        Order number.
  || #param i_ord_ln         Order line number.
  || #param i_catlg_num      McLane catalog item number.
  || #param io_ord_qty       Order quantity that will be adjusted.
  || #param i_byp_max_sw     Bypass max quantity switch from order.
  ||                         Valid values are:
  ||                         {*} 'Y' On
  ||                         {*} 'N' Off
  || #param i_allw_partl_sw  Allow partial shipments switch from order
  ||                         Valid values are:
  ||                         {*} 'Y' On
  ||                         {*} 'N' Off
  || #param i_item_max_qty   Max quantity from item table.
  || #param i_ord_max_qty    Max quantity from order. This comes down from the
  ||                         mainframe as the lessor of the item max or order
  ||                         max.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_max_qty_sp(
    i_div_part       IN      NUMBER,
    i_ord_num        IN      NUMBER,
    i_ord_ln         IN      NUMBER,
    i_catlg_num      IN      NUMBER,
    io_ord_qty       IN OUT  NUMBER,
    i_byp_max_sw     IN      VARCHAR2,
    i_allw_partl_sw  IN      VARCHAR2,
    i_item_max_qty   IN      NUMBER,
    i_ord_max_qty    IN      NUMBER DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Checks for 100% duplication by a process that runs at set interval (ie every 30 min)
  || Checks orders received during a passed range for 100% duplication within the order
  || as well as 100% duplication of a order received within 1 hour prior to this order.
  || exception.
  || #param i_chk_dt           End timestamp range for orders.
  || #param i_adj_mins         Interval of minutes to calculate begin timestamp for orders
  || #param i_div              Division ID.
  || #param o_msg              Returned message
  || #param o_is_dupl_found    Duplicate orders were found? True|False
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dup_order_check_sp(
    i_chk_dt         IN      DATE,
    i_adj_mins       IN      NUMBER,
    i_div            IN      VARCHAR2,
    o_msg            OUT     VARCHAR2,
    o_is_dupl_found  OUT     BOOLEAN
  );

  PROCEDURE check_reg_bev_for_item_sp(
    i_div           IN  VARCHAR2,
    i_t_catlg_nums  IN  type_stab
  );

  PROCEDURE check_vapcbd_for_item_sp(
    i_div           IN  VARCHAR2,
    i_t_catlg_nums  IN  type_stab
  );

  /**
  ||----------------------------------------------------------------------------
  || Checks order for header-level exceptions.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number.
  || #param i_err_rsn_cd       Returned error reason code. Will be NULL if no
  ||                           errors are found.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE validate_header_sp(
    i_div_part    IN      NUMBER,
    i_ord_num     IN      NUMBER,
    o_err_rsn_cd  OUT     VARCHAR2
  );

  /**
  ||----------------------------------------------------------------------------
  || Checks order for detail-level exceptions.
  || #param i_div_part    DivPart
  || #param i_ord_num     Order number.
  || #param i_ord_ln      Order line number.
  ||                      Will check all order lines when NULL.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE validate_details_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  );
END op_order_validation_pk;
/

CREATE OR REPLACE PACKAGE BODY op_order_validation_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_split_item IS RECORD(
    po_prfx  ordp100a.cpoa%TYPE,
    item     sawp505e.iteme%TYPE,
    uom      sawp505e.uome%TYPE,
    max_qty  PLS_INTEGER,
    ord_qty  PLS_INTEGER
  );

  TYPE g_tt_split_item IS TABLE OF g_rt_split_item;

  TYPE g_cvt_split_item IS REF CURSOR
    RETURN g_rt_split_item;

  TYPE g_rt_ord IS RECORD(
    ord_num  NUMBER,
    ord_ln   NUMBER,
    ord_qty  PLS_INTEGER
  );

  TYPE g_cvt_ord IS REF CURSOR
    RETURN g_rt_ord;

  TYPE g_rt_ord_dtl IS RECORD(
    div_part       NUMBER,
    ord_num        NUMBER,
    ord_ln         NUMBER,
    excptn_sw      VARCHAR2(1),
    catlg_num      NUMBER,
    cbr_item       sawp505e.iteme%TYPE,
    uom            sawp505e.uome%TYPE,
    ord_qty        PLS_INTEGER,
    qty_mult       PLS_INTEGER,
    max_qty        PLS_INTEGER,
    byp_max_sw     VARCHAR2(1),
    allw_partl_sw  VARCHAR2(1),
    not_shp_rsn    VARCHAR2(8),
    stat_cd        VARCHAR2(1)
  );

  -- Global Package Constants
  g_c_dist                    CONSTANT VARCHAR2(1)            := 'D';
  g_c_max_typ_item            CONSTANT VARCHAR2(4)            := 'ITEM';
  g_c_max_typ_ord             CONSTANT VARCHAR2(4)            := 'ORDR';
  g_c_max_typ_wkly            CONSTANT VARCHAR2(4)            := 'WKLY';
  /** Value for "Max Quantity Applied" exception */
  g_c_max_ord_qty_violation   CONSTANT mclp140a.rsncda%TYPE   := '002';
  /** Value for "Weekly Max Order Qty Applied" exception */
  g_c_wkly_max_qty_violation  CONSTANT mclp140a.rsncda%TYPE   := 'WKMAXQTY';
  /** Value for "Order Item is inactive" exception */
  g_c_inact_item              CONSTANT mclp140a.rsncda%TYPE   := '005';
  /** Value for "Customer on Hold" exception */
  g_c_cust_on_hold            CONSTANT mclp140a.rsncda%TYPE   := '007';
  /** Value for "Inactive Customer" exception */
  g_c_inact_cust              CONSTANT mclp140a.rsncda%TYPE   := '008';
  /** Value for "80% rule within order" exception */
  g_c_ord_contains_dupl       CONSTANT mclp140a.rsncda%TYPE   := '009';
  /** Value for "Item Not Found" exception */
  g_c_item_not_found          CONSTANT mclp140a.rsncda%TYPE   := '011';
  /** Value for "Invalid Customer" exception */
  g_c_invalid_cust            CONSTANT mclp140a.rsncda%TYPE   := '032';
  /** Value for "Possible Dupe Order (80% + items)" exception */
  g_c_dupl_ord                CONSTANT mclp140a.rsncda%TYPE   := '033';
  /** Value for "No Order with Valid Items" exception */
  g_c_noord_with_valid_items  CONSTANT mclp140a.rsncda%TYPE   := '036';
  /** Value for "Dummy Items on Order" exception */
  g_c_dummy_items_on_ord      CONSTANT mclp140a.rsncda%TYPE   := '038';
  /** Value for "No Stop Information Available" exception */
  g_c_no_stop_info_avail      CONSTANT mclp140a.rsncda%TYPE   := '060';
  /** Value for "Item Discontinued > xxx days " exception */
  g_c_item_disc_xxx_days      CONSTANT mclp140a.rsncda%TYPE   := '067';
  /** Value for "No Order Item Sent" exception */
  g_c_noord                   CONSTANT mclp140a.rsncda%TYPE   := '079';
  /** Value for "All details in error" exception */
  g_c_all_dtls_in_err         CONSTANT mclp140a.rsncda%TYPE   := '088';
  /** Value for "Dupe Order - COMET found" exception */
  g_c_comet_found_dupl_ord    CONSTANT mclp140a.rsncda%TYPE   := 'DCH';
  /** Value for "OK Dupe Order by COMET" exception */
  g_c_comet_rslvd_dupl        CONSTANT mclp140a.rsncda%TYPE   := 'DOO';
  /** Value for "Incr Qty to Multiple Factor" exception */
  g_c_invalid_item_mult       CONSTANT mclp140a.rsncda%TYPE   := 'MULTQTY';
  /** Value for "Ordered Qty is zero" exception */
  g_c_zero_ord_qty            CONSTANT mclp140a.rsncda%TYPE   := 'QTYZERO';
  /** Value for "OrdQty Not Multiple of MasterCase Qty" exception */
  g_c_mc_qty_err              CONSTANT mclp140a.rsncda%TYPE   := 'MCQTYERR';
  /** Value for "Invalid PO" exception */
  g_c_invalid_po              CONSTANT mclp140a.rsncda%TYPE   := 'INVPO';
  /** Value for "Regulated Beverage" exception */
  g_c_reg_bev                 CONSTANT mclp140a.rsncda%TYPE   := 'REGBEV';
  /** Value for "Self Bill" exception */
  g_c_self_bill               CONSTANT mclp140a.rsncda%TYPE   := 'SELFBILL';
  /** Value for "Vape/CBD" exception */
  g_c_vapcbd                  CONSTANT mclp140a.rsncda%TYPE   := 'VAPCBD';
  -- Global Package Exceptions
  g_e_bad_ord_num                      EXCEPTION;

  CURSOR g_cur_ord_info(
    b_div_part  NUMBER,
    b_ord_num   NUMBER
  ) IS
    SELECT dv.div_part, dv.div_id, NVL(d.mdupld, 9) AS dupl_chk_min, NVL(d.nordid, '999995') AS no_ord_item,
           NVL(d.disdad, 180) AS disc_days, NVL(d.dregld, 'DFLT') AS reg_dflt_load, ld.load_num, a.ordnoa AS ord_num,
           a.dsorda AS ord_typ, NVL(a.ipdtsa, ' ') AS ord_src, NVL(a.hdexpa, ' ') AS hdr_excptn_cd,
           c.statc AS cust_stat_cd, cx.corpb AS corp_cd
      FROM div_mstr_di1d dv, mclp130d d, ordp100a a, load_depart_op1f ld, sysp200c c, mclp020b cx
     WHERE dv.div_part = b_div_part
       AND d.div_part = dv.div_part
       AND a.div_part = d.div_part
       AND a.ordnoa = b_ord_num
       AND ld.div_part = a.div_part
       AND ld.load_depart_sid = a.load_depart_sid
       AND c.div_part(+) = a.div_part
       AND c.acnoc(+) = a.custa
       AND cx.div_part(+) = a.div_part
       AND cx.custb(+) = a.custa;

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ORD_INFO_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/08/15 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_info_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN g_cur_ord_info%ROWTYPE IS
    l_c_module  CONSTANT typ.t_maxfqnm            := 'OP_ORDER_VALIDATION_PK.ORD_INFO_FN';
    lar_parm             logs.tar_parm;
    l_r_ord_info         g_cur_ord_info%ROWTYPE;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);

    OPEN g_cur_ord_info(i_div_part, i_ord_num);

    FETCH g_cur_ord_info
     INTO l_r_ord_info;

    IF g_cur_ord_info%NOTFOUND THEN
      RAISE g_e_bad_ord_num;
    END IF;

    CLOSE g_cur_ord_info;

    RETURN(l_r_ord_info);
  EXCEPTION
    WHEN OTHERS THEN
      IF g_cur_ord_info%ISOPEN THEN
        CLOSE g_cur_ord_info;
      END IF;

      logs.err(lar_parm);
  END ord_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_DUP_WITHIN_ORD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/05 | rhalpai | Original
  || 05/23/11 | rhalpai | Changed logic to exclude catchweight items. PIR9238
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_dup_within_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_dup_pct   IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_cv        SYS_REFCURSOR;
    l_exist_sw  VARCHAR2(1);
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM (SELECT   COUNT(*) AS itm_cnt, SUM(COUNT(*) - 1) dup_cnt
                   FROM ordp100a a, ordp120b b
                  WHERE a.div_part = i_div_part
                    AND a.ordnoa = i_ord_num
                    AND b.div_part = a.div_part
                    AND b.ordnob = i_ord_num
                    AND b.statb = 'O'
                    AND NOT EXISTS(SELECT 1
                                     FROM mclp110b di
                                    WHERE di.div_part = b.div_part
                                      AND di.itemb = b.itemnb
                                      AND di.uomb = b.sllumb
                                      AND di.cwt_sw = 'Y')
               GROUP BY b.itemnb, b.sllumb) x
        WHERE (x.dup_cnt / x.itm_cnt * 100) >= i_dup_pct;

    FETCH l_cv
     INTO l_exist_sw;

    CLOSE l_cv;

    RETURN(l_exist_sw = 'Y');
  END is_dup_within_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_DUP_OF_ANOTHER_ORD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/05 | rhalpai | Original
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use load_depart_sid. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_dup_of_another_ord_fn(
    i_div_part        IN  NUMBER,
    i_ord_num         IN  NUMBER,
    i_non_excptn_cnt  IN  PLS_INTEGER,
    i_ord_src         IN  VARCHAR2,
    i_dupl_pct        IN  NUMBER,
    i_from_ts         IN  DATE DEFAULT NULL,
    i_to_ts           IN  DATE DEFAULT NULL
  )
    RETURN BOOLEAN IS
    l_cv        SYS_REFCURSOR;
    l_exist_sw  VARCHAR2(1);
  BEGIN
    IF (    i_dupl_pct >= 100
        AND i_from_ts IS NOT NULL) THEN
      -- 100% dup check
      OPEN l_cv
       FOR
         SELECT   'Y'
             FROM (SELECT   b.itemnb, b.sllumb, SUM(b.ordqtb) AS ord_qty
                       FROM ordp120b b
                      WHERE b.div_part = i_div_part
                        AND b.ordnob = i_ord_num
                        AND b.excptn_sw = 'N'
                   GROUP BY b.itemnb, b.sllumb) x,
                  (SELECT   b.ordnob, b.itemnb, b.sllumb, SUM(b.ordqtb) AS ord_qty
                       FROM ordp100a a, ordp120b b
                      WHERE a.div_part = i_div_part
                        AND a.ordnoa <> i_ord_num
                        AND a.excptn_sw = 'N'
                        AND a.stata = 'O'
                        AND a.dsorda = 'R'
                        AND NVL(a.ipdtsa, 'NULL') = NVL(i_ord_src, 'NULL')
                        AND a.ord_rcvd_ts BETWEEN i_from_ts AND i_to_ts
                        AND b.div_part = a.div_part
                        AND b.ordnob = a.ordnoa
                        AND b.excptn_sw = 'N'
                        AND b.statb = 'O'
                        AND EXISTS(SELECT 1
                                     FROM ordp100a a2, ordp120b b2
                                    WHERE a2.div_part = a.div_part
                                      AND a2.ordnoa = i_ord_num
                                      AND a2.load_depart_sid = a.load_depart_sid
                                      AND a2.custa = a.custa
                                      AND a2.dsorda = 'R'
                                      AND b2.div_part = a2.div_part
                                      AND b2.ordnob = a2.ordnoa
                                      AND b2.itemnb = b.itemnb
                                      AND b2.sllumb = b.sllumb
                                      AND b2.statb = 'O')
                   GROUP BY b.ordnob, b.itemnb, b.sllumb) y
            WHERE y.itemnb = x.itemnb
              AND y.sllumb = x.sllumb
              AND y.ord_qty = x.ord_qty
         GROUP BY y.ordnob
           HAVING (COUNT(*) / i_non_excptn_cnt * 100) >= i_dupl_pct;
    ELSE
      -- 80% dupl check
      OPEN l_cv
       FOR
         SELECT   'Y'
             FROM ordp100a a, ordp120b b
            WHERE a.div_part = i_div_part
              AND a.ordnoa <> i_ord_num
              AND a.excptn_sw = 'N'
              AND a.stata = 'O'
              AND a.dsorda = 'R'
              AND NVL(a.ipdtsa, 'NULL') = NVL(i_ord_src, 'NULL')
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.excptn_sw = 'N'
              AND b.statb = 'O'
              AND EXISTS(SELECT 1
                           FROM ordp100a a2, ordp120b b2
                          WHERE a2.div_part = a.div_part
                            AND a2.ordnoa = i_ord_num
                            AND a2.load_depart_sid = a.load_depart_sid
                            AND a2.custa = a.custa
                            AND a2.dsorda = 'R'
                            AND b2.div_part = a2.div_part
                            AND b2.ordnob = a2.ordnoa
                            AND b2.itemnb = b.itemnb
                            AND b2.sllumb = b.sllumb
                            AND b2.excptn_sw = 'N'
                            AND b2.statb = 'O')
         GROUP BY b.ordnob
           HAVING (COUNT(*) / i_non_excptn_cnt * 100) >= i_dupl_pct;
    END IF;   -- i_dupl_pct >= 100 AND i_from_ts IS NOT NULL

    FETCH l_cv
     INTO l_exist_sw;

    CLOSE l_cv;

    RETURN(l_exist_sw = 'Y');
  END is_dup_of_another_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_TEST_ORD_FN
  ||  Return 'Y' when order num is a test order or 'N' otherwise
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 07/12/11 | rhalpai | Changed cursor to check for existence order as test
  ||                    | in each order well. PIR6235
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_test_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_cv       SYS_REFCURSOR;
    l_test_sw  VARCHAR2(1)   := 'N';
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM DUAL
        WHERE EXISTS(SELECT 1
                       FROM ordp100a a
                      WHERE a.div_part = i_div_part
                        AND a.ordnoa = i_ord_num
                        AND a.dsorda = 'T')
           OR EXISTS(SELECT 1
                       FROM ordp900a a
                      WHERE a.div_part = i_div_part
                        AND a.ordnoa = i_ord_num
                        AND a.dsorda = 'T');

    FETCH l_cv
     INTO l_test_sw;

    RETURN(l_test_sw);
  END is_test_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_SPLIT_ORD_FN
  ||  Indicate if order line is for a group/item set up for SplitOrder.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/19/07 | rhalpai | Original
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_split_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_cv            SYS_REFCURSOR;
    l_split_ord_sw  VARCHAR2(1)   := 'N';
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM ordp100a a, ordp120b b, sysp200c c, rpt_parm_ap1e g
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND b.div_part = a.div_part
          AND b.ordnob = a.ordnoa
          AND b.lineb = i_ord_ln
          AND c.div_part = a.div_part
          AND c.acnoc = a.custa
          AND g.div_part = c.div_part
          AND g.rpt_typ = 'GROUP'
          AND g.val_cd = c.retgpc
          AND g.user_id = 'SPLITORD'
          AND EXISTS(SELECT 1
                       FROM rpt_parm_ap1e i
                      WHERE i.div_part = b.div_part
                        AND i.rpt_typ = 'ITEM'
                        AND i.val_cd = b.orditb
                        AND i.user_id = 'SPLITORD');

    FETCH l_cv
     INTO l_split_ord_sw;

    RETURN(l_split_ord_sw);
  END is_split_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_QTY_FN
  ||  Total item qty ordered for LLR/Load/Stop/Cust.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 05/23/07 | rhalpai | Changed cursor to properly handle exclusion of items
  ||                    | set up for SplitOrder for a customer group set up for
  ||                    | SplitOrder. PIR4274
  || 06/19/07 | rhalpai | Changed logic return zero for a customer group set up
  ||                    | for SplitOrder during Order-Max-Validation.
  ||                    | Will now check for SplitOrder first for an Order-Max
  ||                    | and immediately return zero when found. The previous
  ||                    | logic returned zero from the cursor for SplitOrders
  ||                    | but still manually added the order qty to the returned
  ||                    | value. IM315753
  || 03/28/11 | rhalpai | Changed cursor to include distributions found for
  ||                    | specified items. PIR10007
  || 04/05/11 | rhalpai | Changed cursor to include distributions found with
  ||                    | BypassMax OFF. SP11BIL
  || 07/12/11 | rhalpai | Added logic to handle Weekly Max Qty. PIR6235
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_qty_fn(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_catlg_num  IN  NUMBER,
    i_ord_qty    IN  PLS_INTEGER,
    i_max_typ    IN  VARCHAR2
  )
    RETURN PLS_INTEGER IS
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
    l_wkmaxqty_clean_dt  DATE;
    l_item_qty           PLS_INTEGER   := 0;
  BEGIN
    IF (   i_max_typ IN(g_c_max_typ_item, g_c_max_typ_wkly)
        OR (    i_max_typ = g_c_max_typ_ord
            AND is_split_ord_fn(i_div_part, i_ord_num, i_ord_ln) = 'N')
       ) THEN
      l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);

      -- Get summed qty excluding current order line (if exists)
      -- and then manually add current order qty.
      -- This will handle sub not yet inserted
      CASE i_max_typ
        WHEN g_c_max_typ_item THEN
          OPEN l_cv
           FOR
             SELECT NVL(SUM(DECODE(b.statb, 'O', b.ordqtb, b.pckqtb)), 0)
               FROM sawp505e e, ordp100a a, ordp120b b
              WHERE e.catite = i_catlg_num
                AND a.div_part = i_div_part
                AND a.dsorda IN('R', 'D')
                AND a.excptn_sw = 'N'
                AND a.stata NOT IN('I', 'S', 'C')
                AND (a.custa, a.load_depart_sid) =
                      (SELECT a2.custa, a2.load_depart_sid
                         FROM ordp100a a2, load_depart_op1f ld
                        WHERE a2.div_part = i_div_part
                          AND a2.ordnoa = i_ord_num
                          AND ld.div_part = a2.div_part
                          AND ld.load_depart_sid = a2.load_depart_sid
                          AND ld.load_num NOT IN(SELECT t.column_value
                                                   FROM TABLE(l_t_xloads) t))
                AND b.div_part = a.div_part
                AND b.ordnob = a.ordnoa
                AND b.itemnb = e.iteme
                AND b.sllumb = e.uome
                AND b.excptn_sw = 'N'
                AND b.statb NOT IN('I', 'S', 'C')
                AND b.subrcb <> 999
                AND (   NVL(a.pshipa, '1') IN('1', 'Y')
                     OR b.bymaxb IN('0', 'N'))
                AND 'Y' =(CASE
                            WHEN b.ordnob = i_ord_num
                            AND b.lineb = i_ord_ln THEN 'N'
                            ELSE 'Y'
                          END);
        WHEN g_c_max_typ_ord THEN
          OPEN l_cv
           FOR
             SELECT NVL(SUM(DECODE(b.statb, 'O', b.ordqtb, b.pckqtb)), 0)
               FROM sawp505e e, ordp100a a, ordp120b b
              WHERE e.catite = i_catlg_num
                AND a.div_part = i_div_part
                AND (a.custa, a.load_depart_sid) =
                      (SELECT a2.custa, a2.load_depart_sid
                         FROM ordp100a a2, load_depart_op1f ld
                        WHERE a2.div_part = i_div_part
                          AND a2.ordnoa = i_ord_num
                          AND ld.div_part = a2.div_part
                          AND ld.load_depart_sid = a2.load_depart_sid
                          AND ld.load_num NOT IN(SELECT t.column_value
                                                   FROM TABLE(l_t_xloads) t))
                AND a.dsorda IN('R', 'D')
                AND a.excptn_sw = 'N'
                AND a.stata NOT IN('I', 'S', 'C')
                AND b.div_part = a.div_part
                AND b.ordnob = a.ordnoa
                AND b.itemnb = e.iteme
                AND b.sllumb = e.uome
                AND b.excptn_sw = 'N'
                AND b.statb NOT IN('I', 'S', 'C')
                AND b.subrcb <> 999
                AND (   NVL(a.pshipa, '1') IN('1', 'Y')
                     OR b.bymaxb IN('0', 'N'))
                AND (   (    NVL(a.pshipa, '1') IN('0', 'N')
                         AND b.bymaxb IN('0', 'N'))
                     OR (    NVL(b.bymaxb, '0') IN('0', 'N')
                         AND NOT EXISTS(SELECT 1   -- resolved max qty exception
                                          FROM mclp300d d
                                         WHERE d.div_part = b.div_part
                                           AND d.ordnod = b.ordnob
                                           AND d.ordlnd = b.lineb
                                           AND d.reasnd = '002'
                                           AND d.last_chg_ts =
                                                 (SELECT MAX(d2.last_chg_ts)
                                                    FROM mclp300d d2
                                                   WHERE d2.div_part = d.div_part
                                                     AND d2.ordnod = d.ordnod
                                                     AND d2.ordlnd = d.ordlnd
                                                     AND d2.reasnd = d.reasnd)
                                           AND d.resexd = '1')
                        )
                    )
                AND 'Y' =(CASE
                            WHEN b.ordnob = i_ord_num
                            AND b.lineb = i_ord_ln THEN 'N'
                            ELSE 'Y'
                          END);
        WHEN g_c_max_typ_wkly THEN
          l_wkmaxqty_clean_dt := TRUNC(TO_DATE(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_wkmaxqty_cln_ts),
                                               'YYYYMMDDHH24MISS'
                                              )
                                      );

          OPEN l_cv
           FOR
             SELECT NVL(SUM(DECODE(b.statb, 'O', b.ordqtb, b.pckqtb)), 0)
               FROM (SELECT a.custa, a.load_depart_sid, ld.llr_dt AS llr_dt
                       FROM ordp100a a, load_depart_op1f ld
                      WHERE a.div_part = i_div_part
                        AND a.ordnoa = i_ord_num
                        AND ld.div_part = a.div_part
                        AND ld.load_depart_sid = a.load_depart_sid
                        AND ld.llr_dt > l_wkmaxqty_clean_dt
                        AND ld.llr_dt < l_wkmaxqty_clean_dt + 7
                        AND ld.load_num NOT IN(SELECT t.column_value
                                                 FROM TABLE(l_t_xloads) t)) o,
                    sawp505e e, wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q, ordp100a a, ordp120b b
              WHERE e.catite = i_catlg_num
                AND ci.div_part = i_div_part
                AND ci.cust_id = o.custa
                AND ci.catlg_num = e.catite
                AND q.div_part = ci.div_part
                AND q.cust_item_sid = ci.cust_item_sid
                AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                                  FROM wkly_max_qty_op2m q2
                                 WHERE q2.div_part = ci.div_part
                                   AND q2.cust_item_sid = ci.cust_item_sid
                                   AND o.llr_dt BETWEEN q2.eff_dt AND q2.end_dt)
                AND a.div_part = i_div_part
                AND a.custa = o.custa
                AND a.load_depart_sid = o.load_depart_sid
                AND a.excptn_sw = 'N'
                AND a.stata NOT IN('I', 'S', 'C')
                AND a.dsorda IN('R', 'D')
                AND a.dsorda = DECODE(q.dist_sw, 'N', 'R', a.dsorda)
                AND b.div_part = a.div_part
                AND b.ordnob = a.ordnoa
                AND b.itemnb = e.iteme
                AND b.sllumb = e.uome
                AND 'Y' =(CASE
                            WHEN b.ordnob = i_ord_num
                            AND b.lineb = i_ord_ln THEN 'N'
                            ELSE 'Y'
                          END)
                AND b.excptn_sw = 'N'
                AND b.statb NOT IN('I', 'S', 'C')
                AND b.subrcb <> 999;
      END CASE;

      FETCH l_cv
       INTO l_item_qty;

      -- Manually add current order quantity
      l_item_qty := l_item_qty + i_ord_qty;
    END IF;   -- i_max_typ IN(g_c_max_typ_item, g_c_max_typ_wkly) OR (i_max_typ = g_c_max_typ_ord AND is_split_ord_fn(i_div_part, i_ord_num, i_ord_ln) = 'N')

    RETURN(l_item_qty);
  END item_qty_fn;

  /*
  ||----------------------------------------------------------------------------
  || MAX_QTY_ORD_CUR_FN
  ||  Returns locked cursor of order lines and qyts for item for
  ||  LLR/Load/Stop/Cust used for Max Qty Process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 05/23/07 | rhalpai | Changed cursor to properly handle exclusion of items
  ||                    | set up for SplitOrder for a customer group set up for
  ||                    | SplitOrder. PIR4274
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 03/28/11 | rhalpai | Changed cursor to include distributions found for
  ||                    | specified items. PIR10007
  || 04/05/11 | rhalpai | Changed cursor to include distributions found with
  ||                    | BypassMax OFF. SP11BIL
  || 07/12/11 | rhalpai | Added logic to handle Weekly Max Qty. PIR6235
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  || 02/21/18 | rhalpai | Change sort of cursors to include PO descending between
  ||                    | OrdQty and OrdNum. PIR18024
  ||----------------------------------------------------------------------------
  */
  FUNCTION max_qty_ord_cur_fn(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_ord_ln     IN  NUMBER,
    i_catlg_num  IN  NUMBER,
    i_max_typ    IN  VARCHAR2
  )
    RETURN g_cvt_ord IS
    l_cv_ord             g_cvt_ord;
    l_t_xloads           type_stab;
    l_wkmaxqty_clean_dt  DATE;
  BEGIN
    CASE i_max_typ
      WHEN g_c_max_typ_item THEN
        OPEN l_cv_ord
         FOR
           SELECT     b.ordnob, b.lineb, b.ordqtb
                 FROM sawp505e e, ordp100a a, ordp120b b
                WHERE e.catite = i_catlg_num
                  AND a.div_part = i_div_part
                  AND (a.custa, a.load_depart_sid) = (SELECT a2.custa, a2.load_depart_sid
                                                        FROM ordp100a a2
                                                       WHERE a2.div_part = i_div_part
                                                         AND a2.ordnoa = i_ord_num)
                  AND a.excptn_sw = 'N'
                  AND a.dsorda IN('R', 'D')
                  AND a.stata = 'O'
                  AND b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.statb = 'O'
                  AND (   NVL(a.pshipa, '1') IN('1', 'Y')
                       OR b.bymaxb IN('0', 'N'))
                  AND b.subrcb <> 999
                  AND b.ordqtb > 0
                  AND 'Y' =(CASE
                              WHEN b.ordnob = i_ord_num
                              AND b.lineb = i_ord_ln THEN 'N'
                              ELSE 'Y'
                            END)
                  AND b.itemnb = e.iteme
                  AND b.sllumb = e.uome
                  AND b.excptn_sw = 'N'
             ORDER BY (CASE
                         WHEN a.pshipa IN('0', 'N') THEN 3
                         WHEN EXISTS(SELECT 1
                                       FROM rpt_name_ap7r r
                                      WHERE r.div_part = a.div_part
                                        AND r.user_id = 'SPLITORD'
                                        AND a.cpoa LIKE r.rpt_nm || '%') THEN 2
                         WHEN EXISTS(SELECT 1
                                       FROM rpt_parm_ap1e g, sysp200c c
                                      WHERE c.div_part = a.div_part
                                        AND c.acnoc = a.custa
                                        AND g.div_part = a.div_part
                                        AND g.rpt_typ = 'GROUP'
                                        AND g.val_cd = c.retgpc
                                        AND g.user_id = 'SPLITORD'
                                        AND EXISTS(SELECT 1
                                                     FROM rpt_parm_ap1e i
                                                    WHERE i.div_part = b.div_part
                                                      AND i.rpt_typ = 'ITEM'
                                                      AND i.val_cd = b.orditb
                                                      AND i.user_id = 'SPLITORD')) THEN 1
                         ELSE 0
                       END
                      ),
                      b.subrcb, b.ordqtb DESC, a.cpoa DESC, b.ordnob, b.lineb
           FOR UPDATE;
      WHEN g_c_max_typ_ord THEN
        OPEN l_cv_ord
         FOR
           SELECT     b.ordnob, b.lineb, b.ordqtb
                 FROM sawp505e e, ordp100a a, ordp120b b
                WHERE e.catite = i_catlg_num
                  AND a.div_part = i_div_part
                  AND (a.custa, a.load_depart_sid) = (SELECT a2.custa, a2.load_depart_sid
                                                        FROM ordp100a a2
                                                       WHERE a2.div_part = i_div_part
                                                         AND a2.ordnoa = i_ord_num)
                  AND a.excptn_sw = 'N'
                  AND a.dsorda IN('R', 'D')
                  AND a.stata = 'O'
                  AND b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.statb = 'O'
                  AND (   NVL(a.pshipa, '1') IN('1', 'Y')
                       OR b.bymaxb IN('0', 'N'))
                  AND b.subrcb <> 999
                  AND b.ordqtb > 0
                  AND 'Y' =(CASE
                              WHEN b.ordnob = i_ord_num
                              AND b.lineb = i_ord_ln THEN 'N'
                              ELSE 'Y'
                            END)
                  AND b.itemnb = e.iteme
                  AND b.sllumb = e.uome
                  AND (   (    NVL(a.pshipa, '1') IN('0', 'N')
                           AND b.bymaxb IN('0', 'N'))
                       OR (    NVL(b.bymaxb, '0') IN('0', 'N')
                           AND NOT EXISTS(SELECT 1   -- resolved max qty exception
                                            FROM mclp300d d
                                           WHERE d.div_part = b.div_part
                                             AND d.ordnod = b.ordnob
                                             AND d.ordlnd = b.lineb
                                             AND d.reasnd = '002'
                                             AND d.last_chg_ts =
                                                   (SELECT MAX(d2.last_chg_ts)
                                                      FROM mclp300d d2
                                                     WHERE d2.div_part = d.div_part
                                                       AND d2.ordnod = d.ordnod
                                                       AND d2.ordlnd = d.ordlnd
                                                       AND d2.reasnd = d.reasnd)
                                             AND d.resexd = '1')
                           AND NOT EXISTS(SELECT 1
                                            FROM rpt_parm_ap1e g, sysp200c c
                                           WHERE c.div_part = a.div_part
                                             AND c.acnoc = a.custa
                                             AND g.div_part = c.div_part
                                             AND g.rpt_typ = 'GROUP'
                                             AND g.val_cd = c.retgpc
                                             AND g.user_id = 'SPLITORD'
                                             AND EXISTS(SELECT 1
                                                          FROM rpt_parm_ap1e i
                                                         WHERE i.div_part = b.div_part
                                                           AND i.rpt_typ = 'ITEM'
                                                           AND i.val_cd = b.orditb
                                                           AND i.user_id = 'SPLITORD'))
                          )
                      )
                  AND b.excptn_sw = 'N'
             ORDER BY a.pshipa DESC, b.subrcb, b.ordqtb DESC, a.cpoa DESC, b.ordnob, b.lineb
           FOR UPDATE;
      WHEN g_c_max_typ_wkly THEN
        l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);
        l_wkmaxqty_clean_dt := TRUNC(TO_DATE(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_wkmaxqty_cln_ts),
                                             'YYYYMMDDHH24MISS'
                                            )
                                    );

        OPEN l_cv_ord
         FOR
           SELECT     b.ordnob, b.lineb, b.ordqtb
                 FROM (SELECT a.custa, a.load_depart_sid, ld.llr_dt AS llr_dt
                         FROM ordp100a a, load_depart_op1f ld
                        WHERE a.div_part = i_div_part
                          AND a.ordnoa = i_ord_num
                          AND ld.div_part = a.div_part
                          AND ld.load_depart_sid = a.load_depart_sid
                          AND ld.llr_dt > l_wkmaxqty_clean_dt
                          AND ld.llr_dt < l_wkmaxqty_clean_dt + 7
                          AND ld.load_num NOT IN(SELECT t.column_value
                                                   FROM TABLE(l_t_xloads) t)) o,
                      sawp505e e, wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q, ordp100a a, ordp120b b
                WHERE e.catite = i_catlg_num
                  AND ci.div_part = i_div_part
                  AND ci.cust_id = o.custa
                  AND ci.catlg_num = e.catite
                  AND q.div_part = ci.div_part
                  AND q.cust_item_sid = ci.cust_item_sid
                  AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                                    FROM wkly_max_qty_op2m q2
                                   WHERE q2.div_part = q.div_part
                                     AND q2.cust_item_sid = q.cust_item_sid
                                     AND o.llr_dt BETWEEN q2.eff_dt AND q2.end_dt)
                  AND a.div_part = ci.div_part
                  AND a.custa = o.custa
                  AND a.load_depart_sid = o.load_depart_sid
                  AND a.excptn_sw = 'N'
                  AND a.stata = 'O'
                  AND a.dsorda IN('R', 'D')
                  AND a.dsorda = DECODE(q.dist_sw, 'N', 'R', a.dsorda)
                  AND a.ipdtsa NOT IN('DVC', 'DVT')
                  AND b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.itemnb = e.iteme
                  AND b.sllumb = e.uome
                  AND b.statb = 'O'
                  AND b.ordqtb > 0
                  AND b.subrcb < 999
                  AND b.ntshpb IS NULL
                  AND 'Y' =(CASE
                              WHEN b.ordnob = i_ord_num
                              AND b.lineb = i_ord_ln THEN 'N'
                              ELSE 'Y'
                            END)
                  AND b.excptn_sw = 'N'
             ORDER BY (CASE
                         WHEN a.pshipa IN('0', 'N') THEN 3
                         WHEN EXISTS(SELECT 1
                                       FROM rpt_name_ap7r r
                                      WHERE r.div_part = a.div_part
                                        AND r.user_id = 'SPLITORD'
                                        AND a.cpoa LIKE r.rpt_nm || '%') THEN 2
                         WHEN EXISTS(SELECT 1
                                       FROM rpt_parm_ap1e g, sysp200c c
                                      WHERE c.div_part = a.div_part
                                        AND c.acnoc = a.custa
                                        AND g.div_part = c.div_part
                                        AND g.rpt_typ = 'GROUP'
                                        AND g.val_cd = c.retgpc
                                        AND g.user_id = 'SPLITORD'
                                        AND EXISTS(SELECT 1
                                                     FROM rpt_parm_ap1e i
                                                    WHERE i.div_part = b.div_part
                                                      AND i.rpt_typ = 'ITEM'
                                                      AND i.val_cd = b.orditb
                                                      AND i.user_id = 'SPLITORD')) THEN 1
                         ELSE 0
                       END
                      ),
                      b.subrcb, b.ordqtb DESC, a.cpoa DESC, b.ordnob, b.lineb
           FOR UPDATE;
    END CASE;

    RETURN(l_cv_ord);
  END max_qty_ord_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || WKLY_MAX_QTY_FN
  ||  Get Weekly Max Qty for order line
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/12/11 | rhalpai | Original - Created for PIR6235
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION wkly_max_qty_fn(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_catlg_num  IN  NUMBER
  )
    RETURN NUMBER IS
    l_wkmaxqty_clean_dt  DATE;
    l_cv                 SYS_REFCURSOR;
    l_wkly_max_qty       PLS_INTEGER   := 99999;
  BEGIN
    l_wkmaxqty_clean_dt := TRUNC(TO_DATE(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_wkmaxqty_cln_ts),
                                         'YYYYMMDDHH24MISS'
                                        )
                                );

    OPEN l_cv
     FOR
       SELECT GREATEST(0, q.max_qty - ci.pick_qty)
         FROM ordp100a a, load_depart_op1f ld, wkly_max_cust_item_op1m ci, wkly_max_qty_op2m q
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND a.dsorda IN('R', 'D')
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt > l_wkmaxqty_clean_dt
          AND ld.llr_dt < l_wkmaxqty_clean_dt + 7
          AND ci.div_part = a.div_part
          AND ci.cust_id = a.custa
          AND ci.catlg_num = i_catlg_num
          AND q.div_part = ci.div_part
          AND q.cust_item_sid = ci.cust_item_sid
          AND q.eff_dt = (SELECT MAX(q2.eff_dt)
                            FROM wkly_max_qty_op2m q2
                           WHERE q2.div_part = q.div_part
                             AND q2.cust_item_sid = q.cust_item_sid
                             AND ld.llr_dt BETWEEN q2.eff_dt AND q2.end_dt)
          AND a.dsorda = DECODE(q.dist_sw, 'N', 'R', a.dsorda);

    FETCH l_cv
     INTO l_wkly_max_qty;

    RETURN(l_wkly_max_qty);
  END wkly_max_qty_fn;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_ITEM_CUR_FN
  ||  Returns cursor of item info for Split Order Process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw.
  ||                    | Convert to use load_depart_sid. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_item_cur_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  )
    RETURN g_cvt_split_item IS
    l_cv_split_item  g_cvt_split_item;
  BEGIN
    OPEN l_cv_split_item
     FOR
       SELECT   UPPER(RTRIM(rn.rpt_nm)) AS po_prfx, e.iteme AS cbr_item, e.uome AS uom, b.maxqtb AS max_qty,
                (SELECT NVL(SUM(b2.ordqtb), 0)
                   FROM ordp100a a2, ordp120b b2
                  WHERE a2.div_part = i_div_part
                    AND a2.load_depart_sid = a.load_depart_sid
                    AND a2.custa = a.custa
                    AND NVL(UPPER(a2.cpoa), ' ') NOT LIKE UPPER(RTRIM(rn.rpt_nm)) || '%'
                    AND b2.div_part = a2.div_part
                    AND b2.ordnob = a2.ordnoa
                    AND b2.itemnb = e.iteme
                    AND b2.sllumb = e.uome
                    AND b2.statb NOT IN('I', 'S', 'C')
                    AND b2.excptn_sw = 'N'
                    AND b2.subrcb = 0
                    AND b2.lineb = FLOOR(b2.lineb)
                    AND NVL(a2.pshipa, '1') IN('1', 'Y')) AS ord_qty
           FROM ordp100a a, ordp120b b, sysp200c c, sawp505e e, rpt_name_ap7r rn, rpt_parm_ap1e rp1, rpt_parm_ap1e rp2
          WHERE a.div_part = i_div_part
            AND a.ordnoa = i_ord_num
            AND a.dsorda = 'R'
            AND NVL(UPPER(a.cpoa), ' ') NOT LIKE UPPER(RTRIM(rn.rpt_nm)) || '%'
            AND NVL(a.pshipa, '1') IN('1', 'Y')
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND (   i_ord_ln IS NULL
                 OR b.lineb = i_ord_ln)
            AND b.statb = 'O'
            AND b.subrcb = 0
            AND b.excptn_sw = 'N'
            AND b.lineb = FLOOR(b.lineb)
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND rn.div_part = a.div_part
            AND rn.user_id = 'SPLITORD'
            AND rp1.div_part = rn.div_part
            AND rp1.rpt_nm = rn.rpt_nm
            AND rp1.val_cd = e.catite
            AND rp2.div_part = rn.div_part
            AND rp2.rpt_nm = rn.rpt_nm
            AND rp2.val_cd = c.retgpc
       GROUP BY a.load_depart_sid, a.custa, rn.rpt_nm, e.iteme, e.uome, b.maxqtb;

    RETURN(l_cv_split_item);
  END split_item_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_ORD_CUR_FN
  ||  Returns locked cursor of order lines and qyts for item for
  ||  LLR/Load/Stop/Cust used for Split Order Process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw.
  ||                    | Convert to use load_depart_sid. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_ord_cur_fn(
    i_div_part  IN  NUMBER,
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  )
    RETURN g_cvt_ord IS
    l_cv_ord  g_cvt_ord;
  BEGIN
    OPEN l_cv_ord
     FOR
       SELECT     b.ordnob, b.lineb, b.ordqtb
             FROM ordp100a a, ordp120b b
            WHERE a.div_part = i_div_part
              AND (a.custa, a.load_depart_sid) = (SELECT a2.custa, a2.load_depart_sid
                                                    FROM ordp100a a2
                                                   WHERE a2.div_part = i_div_part
                                                     AND a2.ordnoa = i_ord_num)
              AND REPLACE(TRIM(a.cpoa), '0') IS NULL
              AND a.dsorda = 'R'
              AND a.stata = 'O'
              AND NVL(a.pshipa, '1') IN('1', 'Y')
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.statb = 'O'
              AND b.excptn_sw = 'N'
              AND b.subrcb = 0
              AND b.lineb = FLOOR(b.lineb)
              AND b.ordqtb > 0
              AND b.itemnb = i_cbr_item
              AND b.sllumb = i_uom
         ORDER BY (CASE
                     WHEN     b.ordnob = i_ord_num
                          AND b.lineb = i_ord_ln THEN 0
                     WHEN b.ordnob = i_ord_num THEN 1
                     ELSE 2
                   END), b.ordqtb DESC
       FOR UPDATE;

    RETURN(l_cv_ord);
  END split_ord_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_ORD_NUM_FN
  ||  Returns an existing order number previously used for split from current
  ||  order number if found.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 06/20/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw.
  ||                    | Convert to use load_depart_sid. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION split_ord_num_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_po_prfx   IN  VARCHAR2
  )
    RETURN NUMBER IS
    l_cv       SYS_REFCURSOR;
    l_ord_num  NUMBER;
  BEGIN
    OPEN l_cv
     FOR
       SELECT a.ordnoa
         FROM ordp100a a
        WHERE a.div_part = i_div_part
          AND (a.custa, a.load_depart_sid) = (SELECT a2.custa, a2.load_depart_sid
                                                FROM ordp100a a2
                                               WHERE a2.div_part = i_div_part
                                                 AND a2.ordnoa = i_ord_num)
          AND a.stata = 'O'
          AND a.excptn_sw = 'N'
          AND a.dsorda = 'R'
          AND UPPER(a.cpoa) LIKE UPPER(i_po_prfx) || '%'
          AND EXISTS(SELECT 1
                       FROM mclp300d d
                      WHERE d.div_part = a.div_part
                        AND d.ordnod = a.ordnoa
                        AND d.reasnd = 'SPLITORD'
                        AND d.descd LIKE '%' || i_ord_num || '%')
          AND ROWNUM = 1;

    FETCH l_cv
     INTO l_ord_num;

    RETURN(l_ord_num);
  END split_ord_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_TO_EXCPTN_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/05 | rhalpai | Original
  || 02/19/07 | Arun    | Removed the Insert to SYSP296A and changed it to call
  ||                    | OP_SYSP296A_PK instead.
  || 05-10-07 | rhalpai | Changed to use standard error handler.
  ||                    | Changed to prevent overriding status of cancelled
  ||                    | order lines. IM306687
  || 07/10/12 | rhalpai | Rename from MOVE_ORDER_TO_BAD_WELL_SP.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 08/05/13 | rhalpai | Change logic to include exception order lines when
  ||                    | overriding order status to Cancel or Suspend. IM-114142
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in calls to
  ||                    | OP_MCLP300D_PK.INS_SP, OP_SYSP296A_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_to_excptn_sp(
    i_div_part    IN  NUMBER,
    i_ord_num     IN  NUMBER,
    i_err_rsn_cd  IN  VARCHAR2,
    i_log_user    IN  VARCHAR2,
    i_log_ts      IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                  := 'OP_ORDER_VALIDATION_PK.UPD_ORD_TO_EXCPTN_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_ord_stat_ovrrd     mclp140a.ord_stat_ovrrd%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ErrRsnCd', i_err_rsn_cd);
    logs.add_parm(lar_parm, 'LogUser', i_log_user);
    logs.add_parm(lar_parm, 'LogTS', i_log_ts);
    logs.dbg('Get Order Status Override for Exception');

    OPEN l_cv
     FOR
       SELECT ord_stat_ovrrd
         FROM mclp140a
        WHERE rsncda = i_err_rsn_cd
          AND ord_stat_ovrrd <> 'O';

    FETCH l_cv
     INTO l_ord_stat_ovrrd;

    CLOSE l_cv;

    logs.dbg('Upd Order to Exception');

    UPDATE ordp120b b
       SET b.excptn_sw = 'Y',
           b.statb = DECODE(b.statb, 'C', 'C', NVL(l_ord_stat_ovrrd, b.statb)),
           b.ntshpb = i_err_rsn_cd
     WHERE b.div_part = i_div_part
       AND b.ordnob = i_ord_num
       AND b.excptn_sw = 'N';

    IF l_ord_stat_ovrrd IS NOT NULL THEN
      UPDATE ordp120b b
         SET b.statb = l_ord_stat_ovrrd,
             b.zipcdb = b.ntshpb,
             b.ntshpb = i_err_rsn_cd
       WHERE b.div_part = i_div_part
         AND b.ordnob = i_ord_num
         AND b.excptn_sw = 'Y'
         AND b.statb <> 'C';
    END IF;   -- l_ord_stat_ovrrd IS NOT NULL

    UPDATE ordp100a a
       SET a.excptn_sw = 'Y',
           a.stata = DECODE(a.stata, 'C', 'C', NVL(l_ord_stat_ovrrd, a.stata))
     WHERE a.div_part = i_div_part
       AND a.ordnoa = i_ord_num;

    logs.dbg('Log Exception');
    op_mclp300d_pk.ins_sp(i_div_part, i_ord_num, 0, i_err_rsn_cd, NULL, NULL, NULL, NULL);

    IF l_ord_stat_ovrrd = 'S' THEN
      logs.dbg('Log Suspended Order');
      -- Log suspended orders (needed by CSR's Suspend Inquiry screen)
      op_sysp296a_pk.ins_sp(i_div_part,
                            i_ord_num,
                            0,
                            i_log_user,
                            'ORDP100A',
                            'STATA',
                            l_ord_stat_ovrrd,
                            l_ord_stat_ovrrd,
                            NULL,
                            i_err_rsn_cd,
                            NULL,
                            NULL
                           );
    END IF;   -- l_ord_stat_ovrrd = 'S'
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_to_excptn_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_CUST_STAT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 11/28/11 | rhalpai | Add logic for new Test status 4 to treat as active.
  ||                    | PIR10211
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  ||                    | Pass DivPart in call to OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_cust_stat_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_c_act    CONSTANT VARCHAR2(1) := '1';
    l_c_inact  CONSTANT VARCHAR2(1) := '2';
    l_c_hold   CONSTANT VARCHAR2(1) := '3';
    l_c_test   CONSTANT VARCHAR2(1) := '4';
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_info.ord_typ <> g_c_dist) THEN
      CASE i_r_ord_info.cust_stat_cd
        WHEN l_c_act THEN
          NULL;
        WHEN l_c_test THEN
          NULL;
        WHEN l_c_inact THEN
          io_err_rsn_cd := g_c_inact_cust;
        WHEN l_c_hold THEN
          io_err_rsn_cd := g_c_cust_on_hold;
        ELSE
          io_err_rsn_cd := g_c_invalid_cust;
      END CASE;

      IF (    i_r_ord_info.load_num = i_r_ord_info.reg_dflt_load
          AND i_r_ord_info.cust_stat_cd IN(l_c_act, l_c_hold)) THEN
        op_mclp300d_pk.ins_sp(i_r_ord_info.div_part,
                              i_r_ord_info.ord_num,
                              0,
                              g_c_no_stop_info_avail,
                              NULL,
                              NULL,
                              NULL,
                              NULL
                             );
      END IF;   -- i_r_ord_info.load_num = i_r_ord_info.reg_dflt_load
    END IF;   -- io_err_rsn_cd IS NULL
  END check_cust_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_COMET_DUP_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_comet_dup_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_info.hdr_excptn_cd = g_c_comet_found_dupl_ord) THEN
      io_err_rsn_cd := g_c_comet_found_dupl_ord;
    END IF;
  END check_comet_dup_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_NO_ORD_DTLS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_no_ord_dtls_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_cv        SYS_REFCURSOR;
    l_exist_sw  VARCHAR2(1);
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_info.ord_typ <> 'N') THEN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM ordp120b
          WHERE div_part = i_r_ord_info.div_part
            AND ordnob = i_r_ord_info.ord_num;

      FETCH l_cv
       INTO l_exist_sw;

      IF l_cv%NOTFOUND THEN
        io_err_rsn_cd := g_c_noord;
      END IF;   -- l_cv%NOTFOUND

      CLOSE l_cv;
    END IF;   -- io_err_rsn_cd IS NULL
  END check_no_ord_dtls_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_DUPLICATES_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 05/20/05 | rhalpai | Changed to call common functions,
  ||                    | IS_DUP_WITHIN_ORD_FN and IS_DUP_OF_ANOTHER_ORD_FN,
  ||                    | created when moving duplicated logic in
  ||                    | OP_DUP_ORDER_CHECK_SP to this package. IM149431
  || 01/06/06 | rhalpai | Replaced hard-coded dup pct with look-up from parm table.
  ||                    | Replaced hard-coded order source with look-up from
  ||                    | table SUB_PRCS_ORD_SRC.
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  || 01/16/17 | rhalpai | Add logic for corp-level parm to include duplicate
  ||                    | check for any order source. PIR17140
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_duplicates_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_cv               SYS_REFCURSOR;
    l_resolved_dup_sw  VARCHAR2(1);
    l_non_excptn_cnt   PLS_INTEGER;
    l_max_dupl_pct     NUMBER(3);

    FUNCTION is_valid_ord_src_fn(
      i_div_part  IN  NUMBER,
      i_ord_src   IN  VARCHAR2
    )
      RETURN BOOLEAN IS
      l_cv        SYS_REFCURSOR;
      l_exist_sw  VARCHAR2(1)   := 'N';
    BEGIN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM sub_prcs_ord_src s
          WHERE s.div_part = i_div_part
            AND s.prcs_id = 'ORDER VALIDATION'
            AND s.prcs_sbtyp_cd = 'VDP'
            AND s.ord_src = i_ord_src;

      FETCH l_cv
       INTO l_exist_sw;

      RETURN(l_exist_sw = 'Y');
    END is_valid_ord_src_fn;
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_info.hdr_excptn_cd <> g_c_comet_rslvd_dupl
        AND (   is_valid_ord_src_fn(i_r_ord_info.div_part, i_r_ord_info.ord_src)
             OR op_parms_pk.val_exists_for_prfx_fn(i_r_ord_info.div_part,
                                                   op_const_pk.prm_chk_dups_anysrc,
                                                   TO_CHAR(i_r_ord_info.corp_cd)
                                                  ) = 'Y'
            )
       ) THEN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM mclp300d
          WHERE div_part = i_r_ord_info.div_part
            AND ordnod = i_r_ord_info.ord_num
            AND resexd = '1'
            AND reasnd IN(g_c_ord_contains_dupl, g_c_dupl_ord);

      FETCH l_cv
       INTO l_resolved_dup_sw;

      CLOSE l_cv;

      IF l_resolved_dup_sw IS NULL THEN
        SELECT COUNT(*)
          INTO l_non_excptn_cnt
          FROM ordp100a a, ordp120b b
         WHERE a.div_part = i_r_ord_info.div_part
           AND a.ordnoa = i_r_ord_info.ord_num
           AND NVL(a.pshipa, '1') IN('1', 'Y')
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.excptn_sw = 'N'
           AND b.statb = 'O';

        IF l_non_excptn_cnt > i_r_ord_info.dupl_chk_min THEN
          l_max_dupl_pct := op_parms_pk.val_fn(i_r_ord_info.div_part, op_const_pk.prm_chk_dups_pct);

          IF is_dup_within_ord_fn(i_r_ord_info.div_part, i_r_ord_info.ord_num, l_max_dupl_pct) THEN
            io_err_rsn_cd := g_c_ord_contains_dupl;
          END IF;

          IF (    io_err_rsn_cd IS NULL
              AND is_dup_of_another_ord_fn(i_r_ord_info.div_part,
                                           i_r_ord_info.ord_num,
                                           l_non_excptn_cnt,
                                           i_r_ord_info.ord_src,
                                           l_max_dupl_pct
                                          )
             ) THEN
            io_err_rsn_cd := g_c_dupl_ord;
          END IF;
        END IF;   -- l_non_excptn_cnt > i_r_ord_info.dupl_chk_min
      END IF;   -- l_resolved_dup_sw IS NULL
    END IF;   -- io_err_rsn_cd IS NULL
  END check_duplicates_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_NO_ORD_WITH_ITEMS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_no_ord_with_items_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_cv             SYS_REFCURSOR;
    l_valid_item_sw  VARCHAR2(1);
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_info.ord_typ = 'N') THEN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM ordp100a a, ordp120b b
          WHERE a.div_part = i_r_ord_info.div_part
            AND a.ordnoa = i_r_ord_info.ord_num
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb = 'O'
            AND b.orditb <> i_r_ord_info.no_ord_item;

      FETCH l_cv
       INTO l_valid_item_sw;

      CLOSE l_cv;

      IF l_valid_item_sw IS NOT NULL THEN
        io_err_rsn_cd := g_c_noord_with_valid_items;
      END IF;   -- l_valid_item_sw IS NOT NULL
    END IF;   -- io_err_rsn_cd IS NULL
  END check_no_ord_with_items_sp;

  /*
  ||----------------------------------------------------------------------------
  || FIX_DUMMY_ITEMS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 07/10/12 | rhalpai | Change from CURSOR to SELECT INTO.
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE fix_dummy_items_sp(
    io_r_ord_info  IN OUT NOCOPY  g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_dummy_cnt  PLS_INTEGER;
    l_othr_cnt   PLS_INTEGER;
  BEGIN
    IF io_err_rsn_cd IS NULL THEN
      SELECT SUM(DECODE(b.orditb, io_r_ord_info.no_ord_item, 1, 0)) AS dmy_cnt,
             SUM(DECODE(b.orditb, io_r_ord_info.no_ord_item, 0, 1)) AS oth_cnt
        INTO l_dummy_cnt,
             l_othr_cnt
        FROM ordp120b b
       WHERE b.div_part = io_r_ord_info.div_part
         AND b.ordnob = io_r_ord_info.ord_num
         AND b.statb = 'O';

      IF l_dummy_cnt > 0 THEN
        IF l_othr_cnt = 0 THEN
          IF io_r_ord_info.ord_typ <> 'N' THEN
            UPDATE ordp100a a
               SET a.dsorda = 'N'
             WHERE a.div_part = io_r_ord_info.div_part
               AND a.ordnoa = io_r_ord_info.ord_num;

            io_r_ord_info.ord_typ := 'N';
          END IF;   -- io_r_ord_info.ord_typ <> 'N'
        ELSE
          io_err_rsn_cd := g_c_dummy_items_on_ord;
        END IF;   -- l_othr_cnt = 0
      END IF;   -- l_dummy_cnt > 0
    END IF;   -- io_err_rsn_cd IS NULL
  END fix_dummy_items_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_INVALID_PO_SP
  ||  PO validation for minimum non-blank characters as required at Corp-Level
  ||  indicated by parm PO_NONBLNK_CHARS_###. The last 3 characters of the
  ||  parm will indicate the CorpCode and the integer value will indicate the
  ||  minimum number of non-blank characters required.
  ||  Bypass PO validation for test orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/12/10 | rhalpai | Original for PIR8909
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add OrdInfo input parm and ErrRsnCd in out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_invalid_po_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_t_po_nonblnk_corps  type_stab;
    l_t_po_nonblnk_chars  type_stab;
    l_cv                  SYS_REFCURSOR;
  BEGIN
    IF io_err_rsn_cd IS NULL THEN
      op_parms_pk.get_parms_for_prfx_sp(i_r_ord_info.div_part,
                                        op_const_pk.prm_po_nonblnk_chars,
                                        l_t_po_nonblnk_corps,
                                        l_t_po_nonblnk_chars,
                                        3,
                                        op_parms_pk.g_c_csr
                                       );

      OPEN l_cv
       FOR
         SELECT g_c_invalid_po
           FROM (SELECT i.corp_cd, v.min_char_cnt
                   FROM (SELECT TO_NUMBER(t.column_value) AS corp_cd, ROWNUM AS seq
                           FROM TABLE(l_t_po_nonblnk_corps) t) i,
                        (SELECT TO_NUMBER(t.column_value) AS min_char_cnt, ROWNUM AS seq
                           FROM TABLE(l_t_po_nonblnk_chars) t) v
                  WHERE v.seq = i.seq) x,
                ordp100a a, mclp020b cx
          WHERE a.div_part = i_r_ord_info.div_part
            AND a.ordnoa = i_r_ord_info.ord_num
            AND a.dsorda IN('R', 'D')
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND cx.corpb = x.corp_cd
            AND NVL(LENGTH(REPLACE(a.cpoa, ' ')), 0) < x.min_char_cnt;

      FETCH l_cv
       INTO io_err_rsn_cd;

      CLOSE l_cv;
    END IF;   -- io_err_rsn_cd IS NULL
  END check_invalid_po_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_BAD_ORD_QTY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdQty input parm and ErrRsnCd in out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_bad_ord_qty_sp(
    i_ord_qty      IN             NUMBER,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND NVL(i_ord_qty, 0) < 1) THEN
      io_err_rsn_cd := g_c_zero_ord_qty;
    END IF;
  END check_bad_ord_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_ITEM_STAT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdDtl,DiscDays,Dt input parms and ErrRsnCd in out
  ||                    | parm and ItemMaxQty out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_item_stat_sp(
    i_r_ord_dtl     IN             g_rt_ord_dtl,
    i_disc_days     IN             NUMBER,
    i_dt            IN             DATE,
    io_err_rsn_cd   IN OUT NOCOPY  VARCHAR2,
    o_item_max_qty  OUT            NUMBER
  ) IS
    l_rendate_num            PLS_INTEGER;
    l_cv                     SYS_REFCURSOR;
    l_item_stat_cd           mclp110b.statb%TYPE;
    l_disc_days              PLS_INTEGER;
    l_c_item_inact  CONSTANT VARCHAR2(3)           := 'INA';
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_dtl.allw_partl_sw = 'Y') THEN
      o_item_max_qty := NULL;
      l_rendate_num := TRUNC(i_dt) - DATE '1900-02-28';

      OPEN l_cv
       FOR
         SELECT di.statb, NVL(di.max_ord_qty, 99999), l_rendate_num - di.disdtb
           FROM mclp110b di, sawp505e e
          WHERE di.div_part = i_r_ord_dtl.div_part
            AND di.itemb = i_r_ord_dtl.cbr_item
            AND di.uomb = i_r_ord_dtl.uom
            AND e.iteme = di.itemb
            AND e.uome = di.uomb;

      FETCH l_cv
       INTO l_item_stat_cd, o_item_max_qty, l_disc_days;

      CLOSE l_cv;

      CASE
        WHEN l_item_stat_cd IS NULL THEN
          io_err_rsn_cd := g_c_item_not_found;
        WHEN     l_item_stat_cd = l_c_item_inact
             AND l_disc_days > i_disc_days THEN
          io_err_rsn_cd := g_c_item_disc_xxx_days;
        WHEN l_item_stat_cd = l_c_item_inact THEN
          io_err_rsn_cd := g_c_inact_item;
        ELSE
          NULL;
      END CASE;
    END IF;   -- io_err_rsn_cd IS NULL AND i_r_ord_dtl.allw_partl_sw = 'Y'
  END check_item_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_MSTRCS_QTY_SP
  ||  If customer is set up to order in master-case quantities and order line
  ||  is not a multiple of the master-case qty then either set exception to
  ||  MCQtyErr if order qty is less than master-case qty or reduce order qty to
  ||  multiple of master-case qty and log change as resolved MCQtyErr exception.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/15/09 | rhalpai | Original for PIR7548
  || 12/08/15 | rhalpai | Add DivPart,OrdNum,OrdLn input parms and OrdQty,
  ||                    | NotShipRsn in out parms. Pass DivPart in call to
  ||                    | OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_mstrcs_qty_sp(
    i_div_part      IN             NUMBER,
    i_ord_num       IN             NUMBER,
    i_ord_ln        IN             NUMBER,
    io_ord_qty      IN OUT NOCOPY  PLS_INTEGER,
    io_not_shp_rsn  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_t_mstr_cs_corps  type_stab;
    l_cv               SYS_REFCURSOR;
    l_item_num         sawp505e.iteme%TYPE;
    l_uom              sawp505e.uome%TYPE;
    l_mstrcs_qty       PLS_INTEGER;
    l_mod_qty          PLS_INTEGER;
  BEGIN
    IF io_not_shp_rsn IS NULL THEN
      l_t_mstr_cs_corps := op_parms_pk.parms_for_val_fn(i_div_part, op_const_pk.prm_alloc_mstr_cs, 'Y', 3);

      OPEN l_cv
       FOR
         SELECT e.iteme, e.uome, e.mulsle, MOD(NVL(io_ord_qty, b.ordqtb), e.mulsle)
           FROM ordp100a a, mclp020b cx, ordp120b b, sawp505e e
          WHERE a.div_part = i_div_part
            AND a.ordnoa = i_ord_num
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                              FROM TABLE(l_t_mstr_cs_corps) t)
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.lineb = i_ord_ln
            AND b.excptn_sw = 'N'
            AND e.iteme = b.itemnb
            AND e.uome = b.sllumb
            AND e.mulsle > 0;

      FETCH l_cv
       INTO l_item_num, l_uom, l_mstrcs_qty, l_mod_qty;

      CLOSE l_cv;

      IF l_mstrcs_qty > 0 THEN
        IF l_mod_qty > 0 THEN
          IF io_ord_qty < l_mstrcs_qty THEN
            io_not_shp_rsn := g_c_mc_qty_err;
          ELSE
            op_mclp300d_pk.ins_sp(i_div_part,
                                  i_ord_num,
                                  i_ord_ln,
                                  g_c_mc_qty_err,
                                  l_item_num,
                                  l_uom,
                                  io_ord_qty,
                                  io_ord_qty - l_mod_qty,
                                  '1'
                                 );
            io_ord_qty := io_ord_qty - l_mod_qty;
          END IF;   -- io_ord_qty < l_mstrcs_qty
        END IF;   -- l_mod_qty > 0
      END IF;   -- l_mstrcs_qty > 0
    END IF;   -- io_not_shp_rsn IS NULL
  END check_mstrcs_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || FIND_UNC_SUB_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Removed return status parm from call to OP_GET_SUBS_SP
  ||                    | Added SAVEPOINT and exception handler since call to
  ||                    | OP_GET_SUBS_SP may now raise an exception and the
  ||                    | current logic continues execution with No sub found.
  || 01/13/06 | rhalpai | Moved call to CHECK_ITEM_STAT_SP to
  ||                    | VALIDATE_DETAILS_SP. Added check for open order status
  ||                    | and check to allow discontinued or inactive items.
  ||                    | PIR3159
  || 06/06/07 | rhalpai | Removed restriction for open order status. IM312526
  || 07/10/12 | rhalpai | Change call to OP_GET_SUBS_SP to remove unused parms.
  || 12/08/15 | rhalpai | Add OrdDtl,ErrRsnCd input parms and SubFound out parm.
  ||                    | Add DivPart in call to OP_GET_SUBS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE find_unc_sub_sp(
    i_r_ord_dtl   IN      g_rt_ord_dtl,
    i_err_rsn_cd  IN      VARCHAR2,
    o_sub_found   OUT     VARCHAR2
  ) IS
    l_sub_msg  VARCHAR2(500);
  BEGIN
    IF (    (   i_err_rsn_cd IS NULL
             OR i_err_rsn_cd IN(g_c_item_disc_xxx_days, g_c_inact_item))
        AND i_r_ord_dtl.allw_partl_sw = 'Y'
       ) THEN
      BEGIN
        SAVEPOINT b4_unc_sub;
        op_get_subs_sp(i_r_ord_dtl.div_part, 'UNCSUB', i_r_ord_dtl.ord_num, i_r_ord_dtl.ord_ln, l_sub_msg, o_sub_found);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO SAVEPOINT b4_unc_sub;
          o_sub_found := 'No';
      END;
    END IF;   -- i_err_rsn_cd IS NULL
  END find_unc_sub_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_EXISTING_ERR_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdDtl,ErrRsnCd input parms and SubFound out parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_existing_err_sp(
    i_not_shp_rsn  IN             VARCHAR2,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_not_shp_rsn IS NOT NULL) THEN
      OPEN l_cv
       FOR
         SELECT i_not_shp_rsn
           FROM mclp140a
          WHERE rsncda = i_not_shp_rsn;

      FETCH l_cv
       INTO io_err_rsn_cd;

      CLOSE l_cv;
    END IF;   -- io_err_rsn_cd IS NULL AND i_not_shp_rsn IS NOT NULL
  END check_existing_err_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_QTY_MULT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 12/08/15 | rhalpai | Add OrdDtl input parm and OrdQty,ErrRsnCd in out parm.
  ||                    | Pass DivPart in call to OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_qty_mult_sp(
    i_r_ord_dtl    IN             g_rt_ord_dtl,
    io_ord_qty     IN OUT NOCOPY  PLS_INTEGER,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_rmaindr  PLS_INTEGER;
  BEGIN
    IF (    io_err_rsn_cd IS NULL
        AND i_r_ord_dtl.qty_mult > 1) THEN
      l_rmaindr := MOD(io_ord_qty, i_r_ord_dtl.qty_mult);

      IF l_rmaindr > 0 THEN
        io_err_rsn_cd := g_c_invalid_item_mult;
        op_mclp300d_pk.ins_sp(i_r_ord_dtl.div_part,
                              i_r_ord_dtl.ord_num,
                              i_r_ord_dtl.ord_ln,
                              io_err_rsn_cd,
                              i_r_ord_dtl.cbr_item,
                              i_r_ord_dtl.uom,
                              io_ord_qty,
                              io_ord_qty + i_r_ord_dtl.qty_mult - l_rmaindr
                             );
        io_ord_qty := io_ord_qty + i_r_ord_dtl.qty_mult - l_rmaindr;
      END IF;   -- l_rmaindr > 0
    END IF;   -- io_err_rsn_cd IS NULL AND i_r_ord_dtl.qty_mult > 1
  END check_qty_mult_sp;

  /*
  ||----------------------------------------------------------------------------
  || APPLY_MAX_SP
  ||  Apply and log (Item, Order or Wkly) Max Qty to order line and matching
  ||  lines for LLRDt,Load,Stop,Cust,Item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/12/11 | rhalpai | Original - Created for PIR6235
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in call to
  ||                    | OP_MCLP300D_PK.INS_SP. Replace call to GET_CBR_ITEM_SP
  ||                    | with common OP_ITEM_PK.CBR_ITEM_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE apply_max_sp(
    i_div_part       IN             NUMBER,
    i_ord_num        IN             NUMBER,
    i_ord_ln         IN             NUMBER,
    i_allw_partl_sw  IN             VARCHAR2,
    i_catlg_num      IN             NUMBER,
    i_max_typ        IN             VARCHAR2,
    i_max_qty        IN             NUMBER,
    io_ord_qty       IN OUT NOCOPY  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_ORDER_VALIDATION_PK.APPLY_MAX_SP';
    lar_parm             logs.tar_parm;
    l_cbr_item           sawp505e.iteme%TYPE;
    l_uom                sawp505e.uome%TYPE;
    l_summed_item_qty    PLS_INTEGER;
    l_qty_over_max       PLS_INTEGER;
    l_new_ord_qty        PLS_INTEGER;
    l_qty_reduced        PLS_INTEGER;
    l_rsn_cd             mclp140a.rsncda%TYPE;
    l_cv_ord             g_cvt_ord;
    l_r_ord              g_rt_ord;

    PROCEDURE log_sp(
      i_log_ord_num  IN  NUMBER,
      i_log_ord_ln   IN  NUMBER,
      i_log_ord_qty  IN  NUMBER
    ) IS
    BEGIN
      logs.dbg('Log Max Order Qty Violation');
      op_mclp300d_pk.ins_sp(i_div_part,
                            i_log_ord_num,
                            i_log_ord_ln,
                            l_rsn_cd,
                            l_cbr_item,
                            l_uom,
                            i_log_ord_qty,
                            l_new_ord_qty
                           );
    END log_sp;

    PROCEDURE adj_sp(
      i_adj_ord_num  IN  NUMBER,
      i_adj_ord_ln   IN  NUMBER,
      i_adj_ord_qty  IN  NUMBER
    ) IS
    BEGIN
      l_new_ord_qty :=(CASE
                         WHEN l_qty_over_max < i_adj_ord_qty THEN i_adj_ord_qty - l_qty_over_max
                         ELSE 0
                       END);
      l_qty_reduced := i_adj_ord_qty - l_new_ord_qty;

      IF (    i_adj_ord_num = i_ord_num
          AND i_adj_ord_ln = i_ord_ln) THEN
        IF io_ord_qty > 0 THEN
          log_sp(i_adj_ord_num, i_adj_ord_ln, i_adj_ord_qty);
          io_ord_qty := l_new_ord_qty;
        END IF;   -- i_ord_qty > 0
      ELSE
        log_sp(i_adj_ord_num, i_adj_ord_ln, i_adj_ord_qty);
      END IF;   -- i_adj_ord_num = i_ord_num AND i_adj_ord_ln = i_ord_ln

      l_qty_over_max := l_qty_over_max - l_qty_reduced;
    END adj_sp;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'AllwPartlSw', i_allw_partl_sw);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'MaxTyp', i_max_typ);
    logs.add_parm(lar_parm, 'MaxQty', i_max_qty);
    logs.add_parm(lar_parm, 'OrdQty', io_ord_qty);
    logs.dbg('Get CBR Item/UOM');
    op_item_pk.cbr_item_sp(i_catlg_num, l_cbr_item, l_uom);
    logs.dbg('Get Total Item Qty Ordered');
    l_summed_item_qty := item_qty_fn(i_div_part, i_ord_num, i_ord_ln, i_catlg_num, io_ord_qty, i_max_typ);

    IF l_summed_item_qty > i_max_qty THEN
      logs.dbg('Reduce Order Qty');
      l_qty_over_max := l_summed_item_qty - i_max_qty;
      l_rsn_cd :=(CASE
                    WHEN i_max_typ = g_c_max_typ_wkly THEN g_c_wkly_max_qty_violation
                    ELSE g_c_max_ord_qty_violation
                  END);

      IF i_allw_partl_sw = 'Y' THEN
        adj_sp(i_ord_num, i_ord_ln, io_ord_qty);
      END IF;   -- i_allw_partl_sw = 'Y'

      IF l_qty_over_max > 0 THEN
        logs.dbg('Get Order Lines to Adjust Qty');
        l_cv_ord := max_qty_ord_cur_fn(i_div_part, i_ord_num, i_ord_ln, i_catlg_num, i_max_typ);
        LOOP
          FETCH l_cv_ord
           INTO l_r_ord;

          EXIT WHEN(   l_qty_over_max <= 0
                    OR l_cv_ord%NOTFOUND);
          adj_sp(l_r_ord.ord_num, l_r_ord.ord_ln, l_r_ord.ord_qty);
          logs.dbg('Adjust Qty for Order Line');

          UPDATE ordp120b
             SET ordqtb = l_new_ord_qty
           WHERE div_part = i_div_part
             AND ordnob = l_r_ord.ord_num
             AND lineb = l_r_ord.ord_ln;
        END LOOP;

        CLOSE l_cv_ord;
      END IF;   -- l_qty_over_max > 0

      IF (    i_allw_partl_sw = 'N'
          AND l_qty_over_max > 0) THEN
        adj_sp(i_ord_num, i_ord_ln, io_ord_qty);
      END IF;   -- i_allw_partl_sw = 'N' AND l_qty_over_max > 0
    END IF;   -- l_summed_item_qty > i_max_qty
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END apply_max_sp;

  /*
  ||----------------------------------------------------------------------------
  || FIND_RND_SUB_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Removed return status parm from call to OP_GET_SUBS_SP
  ||                    | Added SAVEPOINT and exception handler since call to
  ||                    | OP_GET_SUBS_SP may now raise an exception and the
  ||                    | current logic continues execution with No sub found.
  || 01/13/06 | rhalpai | Added check for open order status. PIR3159
  || 06/06/07 | rhalpai | Removed restriction for open order status. IM312526
  || 07/10/12 | rhalpai | Change call to OP_GET_SUBS_SP to remove unused parms.
  || 12/08/15 | rhalpai | Add OrdDtl,ErrRsnCd input parms and SubFound out parm.
  ||                    | Add DivPart in call to OP_GET_SUBS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE find_rnd_sub_sp(
    i_r_ord_dtl   IN             g_rt_ord_dtl,
    i_err_rsn_cd  IN             VARCHAR2,
    io_sub_found  IN OUT NOCOPY  VARCHAR2
  ) IS
    lar_parm   logs.tar_parm;
    l_sub_msg  VARCHAR2(500);
  BEGIN
    IF (    i_err_rsn_cd IS NULL
        AND io_sub_found = 'No'
        AND i_r_ord_dtl.allw_partl_sw = 'Y') THEN
      logs.add_parm(lar_parm, 'DivPart', i_r_ord_dtl.div_part);
      logs.add_parm(lar_parm, 'OrdNum', i_r_ord_dtl.ord_num);
      logs.add_parm(lar_parm, 'OrdLn', i_r_ord_dtl.ord_ln);

      BEGIN
        SAVEPOINT b4_rnd_sub;
        op_get_subs_sp(i_r_ord_dtl.div_part, 'RNDSUB', i_r_ord_dtl.ord_num, i_r_ord_dtl.ord_ln, l_sub_msg,
                       io_sub_found);
      EXCEPTION
        WHEN OTHERS THEN
          logs.err(lar_parm, 'SubFound: ' || io_sub_found || 'SubMsg: ' || l_sub_msg, FALSE);
          ROLLBACK TO SAVEPOINT b4_rnd_sub;
          io_sub_found := 'No';
      END;
    END IF;   -- i_err_rsn_cd IS NULL
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      io_sub_found := 'No';
    WHEN TOO_MANY_ROWS THEN
      io_sub_found := 'No';
  END find_rnd_sub_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_REG_BEV_SP
  ||  Check for Regulated Beverage Exception.
  ||  Create Regulated Beverage exceptions for RegBev items when mixed with
  ||  non-RegBev items on same PO.
  ||  Undo RegBev exceptions when mix is no longer found.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/12 | rhalpai | Original for PIR10620
  || 01/26/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in calls to
  ||                    | OP_MAINTAIN_SUBS_PK.REVERT_SUB_SP, OP_MCLP300D_PK.INS_SP.
  || 10/17/22 | rhalpai | Change logic in IS_MIX_FN to include REGBEV exceptions. PIR22000
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_reg_bev_sp(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_po_num           IN  VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_ORDER_VALIDATION_PK.CHECK_REG_BEV_SP';
    lar_parm               logs.tar_parm;
    l_t_xloads             type_stab;
    l_t_excl_regbev_corps  type_stab;

    FUNCTION is_mix_fn
      RETURN BOOLEAN IS
      l_mix_sw  VARCHAR2(1);
    BEGIN
      logs.dbg('Check for Mix of RegBev and Non-RegBev on PO');

      SELECT DECODE(MIN(DECODE(bv.catlg_num, NULL, 0, 1)), MAX(DECODE(bv.catlg_num, NULL, 0, 1)), 'N', 'Y')
        INTO l_mix_sw
        FROM load_depart_op1f ld, mclp020b cx, ordp100a a, ordp120b b, sawp505e e, item_grp_op2e bv
       WHERE ld.div_part = i_div_part
         AND ld.load_depart_sid = i_load_depart_sid
         AND ld.load_num NOT IN(SELECT t.column_value
                                  FROM TABLE(l_t_xloads) t)
         AND cx.div_part = i_div_part
         AND cx.custb = i_cust_id
         AND cx.corpb NOT IN(SELECT TO_NUMBER(t.column_value)
                               FROM TABLE(l_t_excl_regbev_corps) t)
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.custa = i_cust_id
         AND a.cpoa = i_po_num
         AND a.dsorda IN('R', 'D')
         AND a.excptn_sw = 'N'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND (   b.excptn_sw = 'N'
              OR b.ntshpb = 'REGBEV')
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND bv.div_part(+) = i_div_part
         AND bv.cls_typ(+) = 'REGBEV'
         AND bv.catlg_num(+) = e.catite;

      RETURN(l_mix_sw = 'Y');
    END is_mix_fn;

    PROCEDURE undo_reg_bev_excptns_sp IS
      l_c_sysdate       CONSTANT DATE   := SYSDATE;
      l_c_curr_rendate  CONSTANT NUMBER := TRUNC(l_c_sysdate) - DATE '1900-02-28';
      l_c_curr_time     CONSTANT NUMBER := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));

      CURSOR l_cur_ords(
        b_div_part         NUMBER,
        b_load_depart_sid  NUMBER,
        b_cust_id          VARCHAR2,
        b_po_num           VARCHAR2,
        b_rsn_cd           VARCHAR2
      ) IS
        SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln
            FROM mclp300d md, ordp120b b, ordp100a a
           WHERE md.div_part = b_div_part
             AND md.reasnd = b_rsn_cd
             AND (md.ordnod, md.ordlnd, md.last_chg_ts) IN(SELECT   d2.ordnod, d2.ordlnd,
                                                                    MAX(d2.last_chg_ts) AS last_chg_ts
                                                               FROM mclp300d d2
                                                              WHERE d2.div_part = b_div_part
                                                                AND d2.reasnd = b_rsn_cd
                                                           GROUP BY d2.ordnod, d2.ordlnd)
             AND b.div_part = md.div_part
             AND b.ordnob = md.ordnod
             AND b.lineb = md.ordlnd
             AND b.ntshpb = b_rsn_cd
             AND b.statb = 'O'
             AND b.excptn_sw = 'Y'
             AND a.div_part = b.div_part
             AND a.ordnoa = b.ordnob
             AND a.load_depart_sid = b_load_depart_sid
             AND a.custa = b_cust_id
             AND a.cpoa = b_po_num
             AND a.dsorda IN('R', 'D')
        ORDER BY b.ordnob, b.lineb DESC;
    BEGIN
      logs.dbg('Get Ord Lns for Reprice');
      FOR l_r_ord IN l_cur_ords(i_div_part, i_load_depart_sid, i_cust_id, i_po_num, g_c_reg_bev) LOOP
        IF l_r_ord.ord_ln > FLOOR(l_r_ord.ord_ln) THEN
          logs.dbg('Revert Sub');
          op_maintain_subs_pk.revert_sub_sp(i_div_part, l_r_ord.ord_num, l_r_ord.ord_ln);
        ELSE
          logs.dbg('Upd Exception Switch on OrdDtl');

          UPDATE ordp120b b
             SET b.ntshpb = NULL,
                 b.zipcdb = NULL,
                 b.excptn_sw = 'N'
           WHERE b.div_part = i_div_part
             AND b.ordnob = l_r_ord.ord_num
             AND b.lineb = l_r_ord.ord_ln;

          logs.dbg('Upd Exception Switch if set on OrdHdr');

          UPDATE ordp100a a
             SET a.excptn_sw = 'N'
           WHERE a.div_part = i_div_part
             AND a.ordnoa = l_r_ord.ord_num
             AND a.excptn_sw = 'Y';

          logs.dbg('Resolve Exception');

          UPDATE mclp300d d
             SET d.resexd = '1',
                 d.resusd = 'UNREGBEV',
                 d.resdtd = l_c_curr_rendate,
                 d.restmd = l_c_curr_time
           WHERE d.div_part = i_div_part
             AND d.ordnod = l_r_ord.ord_num
             AND d.ordlnd = l_r_ord.ord_ln
             AND d.resexd = '0'
             AND d.reasnd = g_c_reg_bev
             AND d.last_chg_ts = (SELECT MAX(d2.last_chg_ts)
                                    FROM mclp300d d2
                                   WHERE d2.div_part = i_div_part
                                     AND d2.ordnod = l_r_ord.ord_num
                                     AND d2.ordlnd = l_r_ord.ord_ln
                                     AND d2.reasnd = g_c_reg_bev);
        END IF;   -- l_r_ord.ord_ln > FLOOR(l_r_ord.ord_ln)
      END LOOP;
    END undo_reg_bev_excptns_sp;

    PROCEDURE create_reg_bev_excptns_sp IS
      l_t_ord_nums  type_ntab;
      l_t_ord_lns   type_ntab;
      l_cbr_item    sawp505e.iteme%TYPE;
      l_uom         sawp505e.uome%TYPE;
      l_ord_qty     NUMBER;
    BEGIN
      logs.dbg('Get Ord Lns for Regulated Beverage Items');

      SELECT b.ordnob, b.lineb
      BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns
        FROM ordp100a a, ordp120b b, sawp505e e, item_grp_op2e bv
       WHERE a.div_part = i_div_part
         AND a.load_depart_sid = i_load_depart_sid
         AND a.custa = i_cust_id
         AND a.cpoa = i_po_num
         AND a.dsorda IN('R', 'D')
         AND a.excptn_sw = 'N'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND b.excptn_sw = 'N'
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND bv.div_part = a.div_part
         AND bv.cls_typ = 'REGBEV'
         AND bv.catlg_num = e.catite;

      IF l_t_ord_nums.COUNT > 0 THEN
        FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
          logs.dbg('Upd Order Line with Exception');

          UPDATE    ordp120b
                SET ntshpb = g_c_reg_bev,
                    zipcdb = g_c_reg_bev,
                    excptn_sw = 'Y'
              WHERE div_part = i_div_part
                AND ordnob = l_t_ord_nums(i)
                AND lineb = l_t_ord_lns(i)
          RETURNING itemnb, sllumb, ordqtb
               INTO l_cbr_item, l_uom, l_ord_qty;

          logs.dbg('Upd OrdHdr if All OrdDtl in Exception');

          UPDATE ordp100a a
             SET a.excptn_sw = 'Y'
           WHERE a.excptn_sw = 'N'
             AND a.div_part = i_div_part
             AND a.ordnoa = l_t_ord_nums(i)
             AND NOT EXISTS(SELECT 1
                              FROM ordp120b b
                             WHERE b.div_part = i_div_part
                               AND b.ordnob = l_t_ord_nums(i)
                               AND b.excptn_sw = 'N');

          logs.dbg('Log Exception');
          op_mclp300d_pk.ins_sp(i_div_part,
                                l_t_ord_nums(i),
                                l_t_ord_lns(i),
                                g_c_reg_bev,
                                l_cbr_item,
                                l_uom,
                                l_ord_qty,
                                l_ord_qty
                               );
        END LOOP;
      END IF;   -- l_t_ord_nums.COUNT > 0
    END create_reg_bev_excptns_sp;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);

    IF TRIM(REPLACE(i_po_num, '0')) IS NOT NULL THEN
      l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);
      l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_reg_bev);

      IF NOT is_mix_fn THEN
        logs.dbg('Undo RegBev Exceptions');
        undo_reg_bev_excptns_sp;
      END IF;   -- NOT is_mix_fn

      IF is_mix_fn THEN
        logs.dbg('Create RegBev Exceptions');
        create_reg_bev_excptns_sp;
      END IF;   -- is_mix_fn
    END IF;   -- TRIM(REPLACE(i_po_num, '0')) IS NOT NULL
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_reg_bev_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_REG_BEV_FOR_ORD_SP
  ||  Check for Regulated Beverage Exception for Order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/12 | rhalpai | Original for PIR10620
  || 01/26/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_reg_bev_for_ord_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm         := 'OP_ORDER_VALIDATION_PK.CHECK_REG_BEV_FOR_ORD_SP';
    lar_parm               logs.tar_parm;
    l_t_xloads             type_stab;
    l_t_excl_regbev_corps  type_stab;
    l_cv                   SYS_REFCURSOR;
    l_load_depart_sid      NUMBER;
    l_cust_id              sysp200c.acnoc%TYPE;
    l_po_num               ordp100a.cpoa%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);
    l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_reg_bev);
    logs.dbg('Open Cursor for Order Info');

    OPEN l_cv
     FOR
       SELECT a.load_depart_sid, a.custa, a.cpoa
         FROM ordp100a a, load_depart_op1f ld, mclp020b cx
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND TRIM(REPLACE(a.cpoa, '0')) IS NOT NULL
          AND a.dsorda IN('R', 'D')
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.load_num NOT IN(SELECT t.column_value
                                   FROM TABLE(l_t_xloads) t)
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND cx.corpb NOT IN(SELECT TO_NUMBER(t.column_value)
                                FROM TABLE(l_t_excl_regbev_corps) t);

    logs.dbg('Fetch Order Info');

    FETCH l_cv
     INTO l_load_depart_sid, l_cust_id, l_po_num;

    CLOSE l_cv;

    IF l_load_depart_sid IS NOT NULL THEN
      logs.dbg('Check for Mix of RegBev and Non-RegBev on PO');
      check_reg_bev_sp(i_div_part, l_load_depart_sid, l_cust_id, l_po_num);
    END IF;   -- l_load_depart_sid IS NOT NULL
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_reg_bev_for_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_SELF_BILL_SP
  ||  Self-Bill Item (RegBev) without Self-Bill indicator
  ||  Y in 3rd column of CustPassArea indicates Self-Bill
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/22/13 | rhalpai | Original for PIR13110
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in call to
  ||                    | OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_self_bill_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ORDER_VALIDATION_PK.CHECK_SELF_BILL_SP';
    lar_parm             logs.tar_parm;
    l_t_self_bill_corps  type_stab;
    l_t_ord_lns          type_ntab;
    l_cbr_item           sawp505e.iteme%TYPE;
    l_uom                sawp505e.uome%TYPE;
    l_ord_qty            NUMBER;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    l_t_self_bill_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_self_bill);
    logs.dbg('Get Ord Lns for Self-Bill Regulated Beverage Items');

    SELECT b.lineb
    BULK COLLECT INTO l_t_ord_lns
      FROM ordp100a a, mclp020b cx, ordp120b b, sawp505e e, item_grp_op2e bv
     WHERE a.div_part = i_div_part
       AND a.ordnoa = i_ord_num
       AND a.dsorda IN('R', 'D')
       AND a.excptn_sw = 'N'
       AND DECODE(SUBSTR(a.cspasa, 3, 1), 'Y', 'Y', 'N') = 'N'
       AND cx.div_part = a.div_part
       AND cx.custb = a.custa
       AND cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                         FROM TABLE(l_t_self_bill_corps) t)
       AND b.div_part = a.div_part
       AND b.ordnob = a.ordnoa
       AND b.statb IN('O', 'P')
       AND b.excptn_sw = 'N'
       AND e.iteme = b.itemnb
       AND e.uome = b.sllumb
       AND bv.div_part = a.div_part
       AND bv.cls_typ = 'REGBEV'
       AND bv.catlg_num = e.catite;

    IF l_t_ord_lns.COUNT > 0 THEN
      FOR i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST LOOP
        logs.dbg('Upd Order Line with Exception');

        UPDATE    ordp120b
              SET ntshpb = g_c_reg_bev,
                  zipcdb = g_c_reg_bev,
                  excptn_sw = 'Y'
            WHERE div_part = i_div_part
              AND ordnob = i_ord_num
              AND lineb = l_t_ord_lns(i)
        RETURNING itemnb, sllumb, ordqtb
             INTO l_cbr_item, l_uom, l_ord_qty;

        logs.dbg('Upd OrdHdr if All OrdDtl in Exception');

        UPDATE ordp100a a
           SET a.excptn_sw = 'Y'
         WHERE a.excptn_sw = 'N'
           AND a.div_part = i_div_part
           AND a.ordnoa = i_ord_num
           AND NOT EXISTS(SELECT 1
                            FROM ordp120b b
                           WHERE b.div_part = i_div_part
                             AND b.ordnob = i_ord_num
                             AND b.excptn_sw = 'N');

        logs.dbg('Log Exception');
        op_mclp300d_pk.ins_sp(i_div_part,
                              i_ord_num,
                              l_t_ord_lns(i),
                              g_c_self_bill,
                              l_cbr_item,
                              l_uom,
                              l_ord_qty,
                              l_ord_qty
                             );
      END LOOP;
    END IF;   -- l_t_ord_lns.COUNT > 0
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_self_bill_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_VAPCBD_SP
  ||  Check for Vape/CBD Exception.
  ||  Create Vape/CBD exceptions for customer in group with no split allowed
  ||  containing mix of Vape/CBD and non-Vape/CBD items on same PO.
  ||  For customer in group with no split allowed the order WITHOUT a PO
  ||  containing mixed items should NOT have Vape/CBD items marked as exceptions.
  ||  Undo VapeCBD exceptions when mix is no longer found.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/01/22 | rhalpai | Original for PIR22000
  || 10/17/22 | rhalpai | Change logic in IS_MIX_FN to include VAPCBD exceptions. PIR22000
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_vapcbd_sp(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_po_num           IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_VALIDATION_PK.CHECK_VAPCBD_SP';
    lar_parm             logs.tar_parm;
    l_t_xloads           type_stab;

    FUNCTION is_mix_fn
      RETURN BOOLEAN IS
      l_mix_sw  VARCHAR2(1);
    BEGIN
      logs.dbg('Check for Mix of VapeCBD and Non-VapeCBD on PO');

      SELECT DECODE(MIN(DECODE(v.catlg_num, NULL, 0, 1)), MAX(DECODE(v.catlg_num, NULL, 0, 1)), 'N', 'Y')
        INTO l_mix_sw
        FROM load_depart_op1f ld, ordp100a a, sysp200c c, mclp100a g, ordp120b b, sawp505e e, item_grp_op2e v
       WHERE ld.div_part = i_div_part
         AND ld.load_depart_sid = i_load_depart_sid
         AND ld.load_num NOT IN(SELECT t.column_value
                                  FROM TABLE(l_t_xloads) t)
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.custa = i_cust_id
         AND a.cpoa = i_po_num
         AND TRIM(REPLACE(a.cpoa, '0')) IS NOT NULL
         AND a.dsorda IN('R', 'D')
         AND a.excptn_sw = 'N'
         AND c.div_part = a.div_part
         AND c.acnoc = a.custa
         AND g.cstgpa = c.retgpc
         AND g.split_po_cd = 'N'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND (   b.excptn_sw = 'N'
              OR b.ntshpb = 'VAPCBD')
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND v.div_part(+) = i_div_part
         AND v.cls_typ(+) = 'VAPCBD'
         AND v.catlg_num(+) = e.catite;

      RETURN(l_mix_sw = 'Y');
    END is_mix_fn;

    PROCEDURE undo_vapcbd_excptns_sp IS
      l_c_sysdate       CONSTANT DATE   := SYSDATE;
      l_c_curr_rendate  CONSTANT NUMBER := TRUNC(l_c_sysdate) - DATE '1900-02-28';
      l_c_curr_time     CONSTANT NUMBER := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));

      CURSOR l_cur_ords(
        b_div_part         NUMBER,
        b_load_depart_sid  NUMBER,
        b_cust_id          VARCHAR2,
        b_po_num           VARCHAR2,
        b_rsn_cd           VARCHAR2
      ) IS
        SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln
            FROM mclp300d md, ordp120b b, ordp100a a
           WHERE md.div_part = b_div_part
             AND md.reasnd = b_rsn_cd
             AND (md.ordnod, md.ordlnd, md.last_chg_ts) IN(SELECT   d2.ordnod, d2.ordlnd,
                                                                    MAX(d2.last_chg_ts) AS last_chg_ts
                                                               FROM mclp300d d2
                                                              WHERE d2.div_part = b_div_part
                                                                AND d2.reasnd = b_rsn_cd
                                                           GROUP BY d2.ordnod, d2.ordlnd)
             AND b.div_part = md.div_part
             AND b.ordnob = md.ordnod
             AND b.lineb = md.ordlnd
             AND b.ntshpb = b_rsn_cd
             AND b.statb = 'O'
             AND b.excptn_sw = 'Y'
             AND a.div_part = b.div_part
             AND a.ordnoa = b.ordnob
             AND a.load_depart_sid = b_load_depart_sid
             AND a.custa = b_cust_id
             AND (   a.cpoa = b_po_num
                  OR (    a.cpoa IS NULL
                      AND b_po_num IS NULL))
             AND a.dsorda IN('R', 'D')
        ORDER BY b.ordnob, b.lineb DESC;
    BEGIN
      logs.dbg('Get Ord Lns for Reset');
      FOR l_r_ord IN l_cur_ords(i_div_part, i_load_depart_sid, i_cust_id, i_po_num, g_c_vapcbd) LOOP
        IF l_r_ord.ord_ln > FLOOR(l_r_ord.ord_ln) THEN
          logs.dbg('Revert Sub');
          op_maintain_subs_pk.revert_sub_sp(i_div_part, l_r_ord.ord_num, l_r_ord.ord_ln);
        ELSE
          logs.dbg('Upd Exception Switch on OrdDtl');

          UPDATE ordp120b b
             SET b.ntshpb = NULL,
                 b.zipcdb = NULL,
                 b.excptn_sw = 'N'
           WHERE b.div_part = i_div_part
             AND b.ordnob = l_r_ord.ord_num
             AND b.lineb = l_r_ord.ord_ln;

          logs.dbg('Upd Exception Switch if set on OrdHdr');

          UPDATE ordp100a a
             SET a.excptn_sw = 'N'
           WHERE a.div_part = i_div_part
             AND a.ordnoa = l_r_ord.ord_num
             AND a.excptn_sw = 'Y';

          logs.dbg('Resolve Exception');

          UPDATE mclp300d d
             SET d.resexd = '1',
                 d.resusd = 'UNVAPCBD',
                 d.resdtd = l_c_curr_rendate,
                 d.restmd = l_c_curr_time
           WHERE d.div_part = i_div_part
             AND d.ordnod = l_r_ord.ord_num
             AND d.ordlnd = l_r_ord.ord_ln
             AND d.resexd = '0'
             AND d.reasnd = g_c_vapcbd
             AND d.last_chg_ts = (SELECT MAX(d2.last_chg_ts)
                                    FROM mclp300d d2
                                   WHERE d2.div_part = i_div_part
                                     AND d2.ordnod = l_r_ord.ord_num
                                     AND d2.ordlnd = l_r_ord.ord_ln
                                     AND d2.reasnd = g_c_vapcbd);
        END IF;   -- l_r_ord.ord_ln > FLOOR(l_r_ord.ord_ln)
      END LOOP;
    END undo_vapcbd_excptns_sp;

    PROCEDURE create_vapcbd_excptns_sp IS
      l_t_ord_nums  type_ntab;
      l_t_ord_lns   type_ntab;
      l_cbr_item    sawp505e.iteme%TYPE;
      l_uom         sawp505e.uome%TYPE;
      l_ord_qty     NUMBER;
    BEGIN
      logs.dbg('Get Ord Lns for Vape/CBD Items');

      SELECT b.ordnob, b.lineb
      BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns
        FROM ordp100a a, ordp120b b, sawp505e e, item_grp_op2e v
       WHERE a.div_part = i_div_part
         AND a.load_depart_sid = i_load_depart_sid
         AND a.custa = i_cust_id
         AND a.cpoa = i_po_num
         AND a.dsorda IN('R', 'D')
         AND a.excptn_sw = 'N'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND b.excptn_sw = 'N'
         AND e.iteme = b.itemnb
         AND e.uome = b.sllumb
         AND v.div_part = a.div_part
         AND v.cls_typ = 'VAPCBD'
         AND v.catlg_num = e.catite;

      IF l_t_ord_nums.COUNT > 0 THEN
        FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
          logs.dbg('Upd Order Line with Exception');

          UPDATE    ordp120b
                SET ntshpb = g_c_vapcbd,
                    zipcdb = g_c_vapcbd,
                    excptn_sw = 'Y'
              WHERE div_part = i_div_part
                AND ordnob = l_t_ord_nums(i)
                AND lineb = l_t_ord_lns(i)
          RETURNING itemnb, sllumb, ordqtb
               INTO l_cbr_item, l_uom, l_ord_qty;

          logs.dbg('Upd OrdHdr if All OrdDtl in Exception');

          UPDATE ordp100a a
             SET a.excptn_sw = 'Y'
           WHERE a.excptn_sw = 'N'
             AND a.div_part = i_div_part
             AND a.ordnoa = l_t_ord_nums(i)
             AND NOT EXISTS(SELECT 1
                              FROM ordp120b b
                             WHERE b.div_part = i_div_part
                               AND b.ordnob = l_t_ord_nums(i)
                               AND b.excptn_sw = 'N');

          logs.dbg('Log Exception');
          op_mclp300d_pk.ins_sp(i_div_part,
                                l_t_ord_nums(i),
                                l_t_ord_lns(i),
                                g_c_vapcbd,
                                l_cbr_item,
                                l_uom,
                                l_ord_qty,
                                l_ord_qty
                               );
        END LOOP;
      END IF;   -- l_t_ord_nums.COUNT > 0
    END create_vapcbd_excptns_sp;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);
/*
    IF NOT is_mix_fn THEN
      logs.dbg('Undo Vape/CBD Exceptions');
      undo_vapcbd_excptns_sp;
    END IF;   -- NOT is_mix_fn
*/

    IF is_mix_fn THEN
      logs.dbg('Create Vape/CBD Exceptions');
      create_vapcbd_excptns_sp;
    ELSE
      logs.dbg('Undo Vape/CBD Exceptions');
      undo_vapcbd_excptns_sp;
    END IF;   -- is_mix_fn
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_vapcbd_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_VAPCBD_FOR_ORD_SP
  ||  Check for Vape/CBD Exception for Order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/01/22 | rhalpai | Original for PIR22000
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_vapcbd_for_ord_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ORDER_VALIDATION_PK.CHECK_VAPCBD_FOR_ORD_SP';
    lar_parm             logs.tar_parm;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
    l_load_depart_sid    NUMBER;
    l_cust_id            sysp200c.acnoc%TYPE;
    l_po_num             ordp100a.cpoa%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor for Order Info');

    OPEN l_cv
     FOR
       SELECT a.load_depart_sid, a.custa, a.cpoa
         FROM ordp100a a, load_depart_op1f ld
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND a.dsorda IN('R', 'D')
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.load_num NOT IN(SELECT t.column_value
                                   FROM TABLE(l_t_xloads) t);

    logs.dbg('Fetch Order Info');

    FETCH l_cv
     INTO l_load_depart_sid, l_cust_id, l_po_num;

    CLOSE l_cv;

    IF l_load_depart_sid IS NOT NULL THEN
      logs.dbg('Check for Mix of VapeCBD and Non-VapeCBD on PO');
      check_vapcbd_sp(i_div_part, l_load_depart_sid, l_cust_id, l_po_num);
    END IF;   -- l_load_depart_sid IS NOT NULL
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_vapcbd_for_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_ALL_DTLS_IN_ERR_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 12/08/15 | rhalpai | Add OrdDtl input parm and ErrRsnCd in out parm.
  ||                    | Pass DivPart in call to OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_all_dtls_in_err_sp(
    i_r_ord_info   IN             g_cur_ord_info%ROWTYPE,
    io_err_rsn_cd  IN OUT NOCOPY  VARCHAR2
  ) IS
    l_cv        SYS_REFCURSOR;
    l_exist_sw  VARCHAR2(1);
  BEGIN
    IF i_r_ord_info.ord_typ <> 'N' THEN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM ordp120b
          WHERE div_part = i_r_ord_info.div_part
            AND ordnob = i_r_ord_info.ord_num
            AND excptn_sw = 'N';

      FETCH l_cv
       INTO l_exist_sw;

      CLOSE l_cv;

      IF l_exist_sw IS NULL THEN
        io_err_rsn_cd := g_c_all_dtls_in_err;

        UPDATE ordp100a a
           SET a.excptn_sw = 'Y'
         WHERE a.div_part = i_r_ord_info.div_part
           AND a.ordnoa = i_r_ord_info.ord_num
           AND a.excptn_sw = 'N';

        op_mclp300d_pk.ins_sp(i_r_ord_info.div_part, i_r_ord_info.ord_num, 0, io_err_rsn_cd, NULL, NULL, NULL, NULL);
      END IF;   -- l_exist_sw IS NULL
    END IF;   -- i_r_ord_info.ord_typ <> 'N'
  END check_all_dtls_in_err_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_SPLIT_QTY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/05/07 | rhalpai | Original - Created for PIR4274
  || 12/20/07 | rhalpai | Changed to add SPLIT_ORD_OP2S entry for split order
  ||                    | lines. PIR5341
  || 11/10/10 | rhalpai | Removed references to unused nototb column. PIR5878
  || 07/10/12 | rhalpai | Add LoadDepartSid.
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in call to
  ||                    | OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_split_qty_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm        := 'OP_ORDER_VALIDATION_PK.CHECK_SPLIT_QTY_SP';
    lar_parm             logs.tar_parm;
    l_cv_split_item      g_cvt_split_item;
    l_t_split_item       g_tt_split_item;
    l_idx                PLS_INTEGER;
    l_qty_over_max       PLS_INTEGER;
    l_cv_split_ord       g_cvt_ord;
    l_r_split_ord        g_rt_ord;
    l_new_ord_qty        PLS_INTEGER;
    l_qty_reduced        PLS_INTEGER;
    l_split_ord_num      NUMBER;
    l_split_ord_ln       NUMBER;
    l_po_num             ordp100a.cpoa%TYPE;
    l_c_dt_str  CONSTANT VARCHAR2(10)         := TO_CHAR(SYSDATE, 'YYYY-MM-DD');
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('Initialize');
    l_t_split_item := g_tt_split_item();
    logs.dbg('Get Split Item Cursor');
    l_cv_split_item := split_item_cur_fn(i_div_part, i_ord_num, i_ord_ln);
    logs.dbg('Fetch Split Item Cursor');

    FETCH l_cv_split_item
    BULK COLLECT INTO l_t_split_item;

    IF l_t_split_item.COUNT > 0 THEN
      logs.dbg('Process Split Items');
      l_idx := l_t_split_item.FIRST;
      <<item_loop>>
      WHILE l_idx IS NOT NULL LOOP
        IF l_t_split_item(l_idx).ord_qty > l_t_split_item(l_idx).max_qty THEN
          l_qty_over_max := l_t_split_item(l_idx).ord_qty - l_t_split_item(l_idx).max_qty;
          logs.dbg('Get Split Order Cursor');
          l_cv_split_ord := split_ord_cur_fn(i_div_part,
                                             l_t_split_item(l_idx).item,
                                             l_t_split_item(l_idx).uom,
                                             i_ord_num,
                                             i_ord_ln
                                            );
          <<order_loop>>
          LOOP
            logs.dbg('Fetch Split Order Cursor');

            FETCH l_cv_split_ord
             INTO l_r_split_ord;

            EXIT WHEN l_qty_over_max <= 0
                  OR l_cv_split_ord%NOTFOUND;

            IF l_r_split_ord.ord_qty > l_qty_over_max THEN
              l_new_ord_qty := l_r_split_ord.ord_qty - l_qty_over_max;
            ELSE
              l_new_ord_qty := 0;
            END IF;   -- l_r_split_ord.ord_qty > l_qty_over_max

            l_qty_reduced := l_r_split_ord.ord_qty - l_new_ord_qty;
            logs.dbg('Attempt to Find Existing Split Order');
            l_split_ord_num := split_ord_num_fn(i_div_part, l_r_split_ord.ord_num, l_t_split_item(l_idx).po_prfx);

            IF l_split_ord_num IS NOT NULL THEN
              logs.dbg('Get Split OrdLn and PONum');

              SELECT a.cpoa, (SELECT COUNT(*)
                                FROM ordp120b b
                               WHERE b.div_part = a.div_part
                                 AND b.ordnob = a.ordnoa
                                 AND b.lineb = FLOOR(b.lineb))
                INTO l_po_num, l_split_ord_ln
                FROM ordp100a a
               WHERE a.div_part = i_div_part
                 AND a.ordnoa = l_split_ord_num;
            ELSE
              logs.dbg('Get Split OrdNum');

              SELECT ordp100a_ordnoa_seq.NEXTVAL
                INTO l_split_ord_num
                FROM DUAL;

              l_po_num := RPAD(l_t_split_item(l_idx).po_prfx, 20) || l_c_dt_str;
              l_split_ord_ln := 1;
              logs.dbg('Add Split Order Header');

              DECLARE
                l_r_ordp100a  ordp100a%ROWTYPE;
              BEGIN
                SELECT *
                  INTO l_r_ordp100a
                  FROM ordp100a a
                 WHERE a.div_part = i_div_part
                   AND a.ordnoa = l_r_split_ord.ord_num;

                l_r_ordp100a.ordnoa := l_split_ord_num;
                l_r_ordp100a.cpoa := l_po_num;
                l_r_ordp100a.ord_rcvd_ts := SYSDATE;

                INSERT INTO ordp100a
                     VALUES l_r_ordp100a;
              END;

              logs.dbg('Add Split Order Comment');

              DECLARE
                l_r_ordp140c  ordp140c%ROWTYPE;
              BEGIN
                SELECT *
                  INTO l_r_ordp140c
                  FROM ordp140c c
                 WHERE c.div_part = i_div_part
                   AND c.ordnoc = l_r_split_ord.ord_num;

                l_r_ordp140c.ordnoc := l_split_ord_num;

                INSERT INTO ordp140c
                     VALUES l_r_ordp140c;
              END;
            END IF;   -- l_split_ord_num IS NOT NULL

            logs.dbg('Upd OrdDtl');

            UPDATE ordp120b b
               SET b.orgqtb = l_new_ord_qty,
                   b.ordqtb = l_new_ord_qty,
                   b.bymaxb = '0'
             WHERE b.div_part = i_div_part
               AND b.ordnob = l_r_split_ord.ord_num
               AND b.lineb = l_r_split_ord.ord_ln;

            logs.dbg('Add Log Entry for Order Line');
            op_mclp300d_pk.ins_sp(i_div_part,
                                  l_r_split_ord.ord_num,
                                  l_r_split_ord.ord_ln,
                                  'QTYSPLIT',
                                  l_t_split_item(l_idx).item,
                                  l_t_split_item(l_idx).uom,
                                  l_r_split_ord.ord_qty,
                                  l_new_ord_qty,
                                  '1',
                                  4,
                                  'QTY SPLIT TO ORDER ' || l_split_ord_num || ' LINE ' || l_split_ord_ln
                                 );
            logs.dbg('Add Split OrdDtl');

            DECLARE
              l_r_ordp120b  ordp120b%ROWTYPE;
            BEGIN
              SELECT *
                INTO l_r_ordp120b
                FROM ordp120b b
               WHERE b.div_part = i_div_part
                 AND b.ordnob = l_r_split_ord.ord_num
                 AND b.lineb = l_r_split_ord.ord_ln;

              l_r_ordp120b.ordnob := l_split_ord_num;
              l_r_ordp120b.lineb := l_split_ord_ln;
              l_r_ordp120b.actqtb := l_qty_reduced;
              l_r_ordp120b.orgqtb := l_qty_reduced;
              l_r_ordp120b.ordqtb := l_qty_reduced;

              INSERT INTO ordp120b
                   VALUES l_r_ordp120b;
            END;

            logs.dbg('Add SPLIT_ORD_OP2S Entry for Split Order Line');

            INSERT INTO split_ord_op2s
                        (div_part, ord_num, ord_ln, split_typ, org_ord_num,
                         org_ord_ln
                        )
                 VALUES (i_div_part, l_split_ord_num, l_split_ord_ln, 'HEAVY LIQ', l_r_split_ord.ord_num,
                         l_r_split_ord.ord_ln
                        );

            logs.dbg('Add Log Entry for Split Order Line');
            op_mclp300d_pk.ins_sp(i_div_part,
                                  l_split_ord_num,
                                  l_split_ord_ln,
                                  'SPLITORD',
                                  l_t_split_item(l_idx).item,
                                  l_t_split_item(l_idx).uom,
                                  0,
                                  l_qty_reduced,
                                  '1',
                                  4,
                                  'QTY SPLIT FROM ORDER ' || l_r_split_ord.ord_num || ' LINE ' || l_r_split_ord.ord_ln
                                 );
            l_qty_over_max := l_qty_over_max - l_qty_reduced;
          END LOOP order_loop;
        END IF;   -- l_t_split_item(l_idx).ord_qty > l_t_split_item(l_idx).max_qty

        l_idx := l_t_split_item.NEXT(l_idx);
      END LOOP item_loop;
    END IF;   -- l_t_split_item.COUNT > 0
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_split_qty_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || CHECK_MAX_QTY_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 08/18/04 | BGOETZ  | Correct Problem with Min/Max applied process IM87866
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 06/07/05 | rhalpai | Changed to bypass order lines with a resolved
  ||                    | "Max Quantity" exception and those with the
  ||                    | "Bypass Max" indicator turned on.
  ||                    | Moved check for NULL error reason code from within
  ||                    | CHECK_MAX_QTY_SP to VALIDATE_DETAILS_SP just prior to
  ||                    | calling CHECK_MAX_QTY_SP. Since the error reason code
  ||                    | is stored in a global variable it was possible that
  ||                    | in a call to CHECK_MAX_QTY_SP within the same session
  ||                    | following a call where an exception was found the
  ||                    | validation logic would be bypassed. This is now more
  ||                    | likely since CHECK_MAX_QTY_SP will now be called from
  ||                    | OP_GET_SUBS_SP. IM155504
  || 04/05/07 | rhalpai | Change logic to always apply Item Max Qty
  ||                    | (mclp110b.max_ord_qty) and exclude Split Items for
  ||                    | customer's group when applying Order Max Qty. PIR4274
  || 03/28/11 | rhalpai | Changed logic to include distributions in order max
  ||                    | but exclude them from item max. PIR10007
  || 07/12/11 | rhalpai | Added logic to apply and log Weekly Max Qty as needed
  ||                    | for order line. PIR6235
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_max_qty_sp(
    i_div_part       IN      NUMBER,
    i_ord_num        IN      NUMBER,
    i_ord_ln         IN      NUMBER,
    i_catlg_num      IN      NUMBER,
    io_ord_qty       IN OUT  NUMBER,
    i_byp_max_sw     IN      VARCHAR2,
    i_allw_partl_sw  IN      VARCHAR2,
    i_item_max_qty   IN      NUMBER,
    i_ord_max_qty    IN      NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP';
    lar_parm             logs.tar_parm;
    l_item_max_qty       PLS_INTEGER   := NVL(i_item_max_qty, 99999);
    l_ord_max_qty        PLS_INTEGER   := NVL(i_ord_max_qty, 99999);
    l_wkly_max_qty       PLS_INTEGER;
    l_max_typ            VARCHAR2(4);
    l_max_qty            PLS_INTEGER   := 99999;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'OrdQty', io_ord_qty);
    logs.add_parm(lar_parm, 'BypMaxSw', i_byp_max_sw);
    logs.add_parm(lar_parm, 'AllwPartlSw', i_allw_partl_sw);
    logs.add_parm(lar_parm, 'ItemMaxQty', i_item_max_qty);
    logs.add_parm(lar_parm, 'OrderMaxQty', i_ord_max_qty);

    IF (    is_test_ord_fn(i_div_part, i_ord_num) = 'N'
        AND io_ord_qty > 0) THEN
      logs.dbg('Get Wkly Max');
      l_wkly_max_qty := wkly_max_qty_fn(i_div_part, i_ord_num, i_catlg_num);

      IF (    i_allw_partl_sw = 'Y'
          AND l_item_max_qty > 0
          AND l_item_max_qty < l_max_qty) THEN
        l_max_typ := g_c_max_typ_item;
        l_max_qty := l_item_max_qty;
      END IF;   -- i_allw_partl_sw = 'Y' AND l_item_max_qty > 0 AND l_item_max_qty < l_max_qty

      IF (    l_ord_max_qty > 0
          AND l_ord_max_qty < l_max_qty
          AND i_byp_max_sw = 'N') THEN
        l_max_typ := g_c_max_typ_ord;
        l_max_qty := l_ord_max_qty;
      END IF;   -- l_ord_max_qty > 0 AND l_ord_max_qty < l_max_qty AND i_byp_max_sw = 'N'

      IF l_wkly_max_qty < l_max_qty THEN
        l_max_typ := g_c_max_typ_wkly;
        l_max_qty := l_wkly_max_qty;
      END IF;   -- l_wkly_max_qty < l_max_qty

      IF l_max_qty < 99999 THEN
        logs.dbg('Apply Max');
        apply_max_sp(i_div_part, i_ord_num, i_ord_ln, i_allw_partl_sw, i_catlg_num, l_max_typ, l_max_qty, io_ord_qty);
      END IF;   -- l_max_qty < 99999
    END IF;   -- is_test_ord_fn(i_div_part, i_ord_num) = 'N' AND io_ord_qty > 0
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_max_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || DUP_ORDER_CHECK_SP
  ||  This procedure checks for duplicate orders in the good order well.
  ||  Dups that are found are moved to the bad order well and an entry is
  ||  made to the exception log. This procedure is called by a process that
  ||  runs at a specified interval.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/25/02 | SKADALI | Original
  || 04/13/02 | SKADALI | Modified to 100% Duplicate Check
  ||                    | Modified the orders_cur Query
  || 05/10/02 | SKADALI | Modified the orders_cur,get-other-order query
  || 07/15/02 | SKADALI | Clean The Code
  || 11/28/02 | SKADALI | Removed the unused variables
  || 05/20/05 | rhalpai | Moved the OP_DUP_ORDER_CHECK_SP to this package and
  ||                    | eliminated dead code and duplicated logic already used
  ||                    | within this package.
  ||                    | Changed cursor to bypass order sources in SUB_PRCS_ORD_SRC
  ||                    | (TXDOCK,XDOCK) during dup checking. IM149431
  || 01/06/06 | rhalpai | Replaced hard-coded dup pct with look-up from parm table.
  || 07/10/12 | rhalpai | Change call from MOVE_ORDER_TO_BAD_WELL_SP to
  ||                    | UPD_ORD_TO_EXCPTN_SP.
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/08/14 | rhalpai | Change cursor to calculate non_excptn_cnt using
  ||                    | unique items. IM-186041
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dup_order_check_sp(
    i_chk_dt         IN      DATE,
    i_adj_mins       IN      NUMBER,
    i_div            IN      VARCHAR2,
    o_msg            OUT     VARCHAR2,
    o_is_dupl_found  OUT     BOOLEAN
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm          := 'OP_ORDER_VALIDATION_PK.DUP_ORDER_CHECK_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_sysdate  CONSTANT DATE                   := SYSDATE;
    l_max_dup_pct         NUMBER(3);
    l_from_ts             DATE;
    l_to_ts               DATE;
    l_err_rsn_cd          mclp140a.rsncda%TYPE;
    l_dupl_ords_list      VARCHAR2(1000);

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_from_ts   DATE,
      b_to_ts     DATE
    ) IS
      SELECT   a.ordnoa AS ord_num, a.ipdtsa AS ord_src, a.ord_rcvd_ts,
               (SELECT COUNT(DISTINCT b2.itemnb || b2.sllumb)
                  FROM ordp120b b2
                 WHERE b2.div_part = b_div_part
                   AND b2.ordnob = a.ordnoa
                   AND b2.excptn_sw = 'N') AS non_excptn_cnt
          FROM mclp130d dp, ordp100a a, ordp120b b
         WHERE dp.div_part = b_div_part
           AND a.div_part = dp.div_part
           AND a.excptn_sw = 'N'
           AND a.stata = 'O'
           AND a.dsorda = 'R'
           AND a.ord_rcvd_ts BETWEEN b_from_ts AND b_to_ts
           AND NVL(a.hdexpa, 'NULL') <> 'DOO'
           AND NOT EXISTS(SELECT 1
                            FROM mclp300d d
                           WHERE d.div_part = a.div_part
                             AND d.ordnod = a.ordnoa
                             AND d.ordlnd = 0
                             AND d.resexd = '1')
           AND NOT EXISTS(SELECT 1
                            FROM sub_prcs_ord_src s
                           WHERE s.div_part = a.div_part
                             AND s.prcs_id = 'ORDER RECEIPT'
                             AND s.prcs_sbtyp_cd = 'BOV'
                             AND s.ord_src = a.ipdtsa)
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
      GROUP BY a.ordnoa, a.ipdtsa, a.ord_rcvd_ts, dp.mdupld
        HAVING SUM(DECODE(b.excptn_sw, 'N', 1)) > dp.mdupld
      ORDER BY a.ordnoa DESC;
  BEGIN
    logs.add_parm(lar_parm, 'ChkDt', i_chk_dt);
    logs.add_parm(lar_parm, 'AdjMins', i_adj_mins);
    logs.add_parm(lar_parm, 'Div', i_div);
    l_div_part := div_pk.div_part_fn(i_div);
    o_is_dupl_found := TRUE;
    l_max_dup_pct := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_dup_ord_chk_pct);
    l_from_ts := i_chk_dt -(i_adj_mins /(60 * 24));
    l_to_ts := i_chk_dt;
    FOR l_r_ord IN l_cur_ords(l_div_part, l_from_ts, l_to_ts) LOOP
      l_err_rsn_cd := NULL;
      logs.dbg('Check for Dups Within Order');

      IF is_dup_within_ord_fn(l_div_part, l_r_ord.ord_num, l_max_dup_pct) THEN
        l_err_rsn_cd := g_c_ord_contains_dupl;
      END IF;

      logs.dbg('Check for Dup of Another Order');

      IF (    l_err_rsn_cd IS NULL
          AND is_dup_of_another_ord_fn(l_div_part,
                                       l_r_ord.ord_num,
                                       l_r_ord.non_excptn_cnt,
                                       l_r_ord.ord_src,
                                       l_max_dup_pct,
                                       l_r_ord.ord_rcvd_ts - INTERVAL '60' MINUTE,
                                       l_r_ord.ord_rcvd_ts
                                      )
         ) THEN
        l_err_rsn_cd := g_c_dupl_ord;
      END IF;

      IF l_err_rsn_cd IS NOT NULL THEN
        logs.dbg('Upd Ord to Excptn');
        upd_ord_to_excptn_sp(l_div_part, l_r_ord.ord_num, l_err_rsn_cd, 'DUP_ORDER_CHECK', l_c_sysdate);
        COMMIT;
        logs.dbg('Create Dup Orders List');

        BEGIN
          l_dupl_ords_list :=(CASE
                                WHEN l_dupl_ords_list IS NULL THEN l_r_ord.ord_num
                                ELSE l_dupl_ords_list || ',' || l_r_ord.ord_num
                              END
                             );
        EXCEPTION
          WHEN VALUE_ERROR THEN
            NULL;
        END;
      END IF;   -- l_err_rsn_cd IS NOT NULL
    END LOOP;

    IF l_dupl_ords_list IS NOT NULL THEN
      o_msg := 'Duplicate orders: ' || cnst.newline_char || l_dupl_ords_list || cnst.newline_char;
    ELSE
      o_msg := 'No duplicate orders found';
      o_is_dupl_found := FALSE;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dup_order_check_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_REG_BEV_FOR_ITEM_SP
  ||  Check for Regulated Beverage Exception for Item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/12 | rhalpai | Original for PIR10620
  || 01/26/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_reg_bev_for_item_sp(
    i_div           IN  VARCHAR2,
    i_t_catlg_nums  IN  type_stab
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_ORDER_VALIDATION_PK.CHECK_REG_BEV_FOR_ITEM_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_t_xloads             type_stab;
    l_t_excl_regbev_corps  type_stab;

    CURSOR l_cur_ords(
      b_div_part             NUMBER,
      b_t_catlg_nums         type_stab,
      b_t_xloads             type_stab,
      b_t_excl_regbev_corps  type_stab
    ) IS
      SELECT   a.load_depart_sid, a.custa AS cust_id, a.cpoa AS po_num
          FROM sawp505e e, ordp120b b, ordp100a a, load_depart_op1f ld, mclp020b cx
         WHERE e.catite IN(SELECT t.column_value
                             FROM TABLE(b_t_catlg_nums) t)
           AND b.div_part = b_div_part
           AND b.itemnb = e.iteme
           AND b.sllumb = e.uome
           AND b.statb = 'O'
           AND a.div_part = b.div_part
           AND a.ordnoa = b.ordnob
           AND TRIM(REPLACE(a.cpoa, '0')) IS NOT NULL
           AND a.dsorda IN('R', 'D')
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND ld.load_num NOT IN(SELECT t.column_value
                                    FROM TABLE(b_t_xloads) t)
           AND cx.div_part = a.div_part
           AND cx.custb = a.custa
           AND cx.corpb NOT IN(SELECT TO_NUMBER(t.column_value)
                                 FROM TABLE(b_t_excl_regbev_corps) t)
      GROUP BY a.load_depart_sid, a.custa, a.cpoa;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CatlgNumTab', i_t_catlg_nums);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    l_t_excl_regbev_corps := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_excl_reg_bev);
    logs.dbg('Process Order Info for Item');
    FOR l_r_ord IN l_cur_ords(l_div_part, i_t_catlg_nums, l_t_xloads, l_t_excl_regbev_corps) LOOP
      logs.dbg('Check for Mix of RegBev and Non-RegBev on PO');
      check_reg_bev_sp(l_div_part, l_r_ord.load_depart_sid, l_r_ord.cust_id, l_r_ord.po_num);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_reg_bev_for_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_VAPCBD_FOR_ITEM_SP
  ||  Check for Vape/CBD Exception for Item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/01/22 | rhalpai | Original for PIR22000
  ||----------------------------------------------------------------------------
  */
  PROCEDURE check_vapcbd_for_item_sp(
    i_div           IN  VARCHAR2,
    i_t_catlg_nums  IN  type_stab
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_VALIDATION_PK.CHECK_VAPCBD_FOR_ITEM_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;

    CURSOR l_cur_ords(
      b_div_part      NUMBER,
      b_t_catlg_nums  type_stab,
      b_t_xloads      type_stab
    ) IS
      SELECT   a.load_depart_sid, a.custa AS cust_id, a.cpoa AS po_num
          FROM sawp505e e, ordp120b b, ordp100a a, load_depart_op1f ld
         WHERE e.catite IN(SELECT t.column_value
                             FROM TABLE(b_t_catlg_nums) t)
           AND b.div_part = b_div_part
           AND b.itemnb = e.iteme
           AND b.sllumb = e.uome
           AND b.statb = 'O'
           AND a.div_part = b.div_part
           AND a.ordnoa = b.ordnob
           AND a.dsorda IN('R', 'D')
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND ld.load_num NOT IN(SELECT t.column_value
                                    FROM TABLE(b_t_xloads) t)
      GROUP BY a.load_depart_sid, a.custa, a.cpoa;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CatlgNumTab', i_t_catlg_nums);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Process Order Info for Item');
    FOR l_r_ord IN l_cur_ords(l_div_part, i_t_catlg_nums, l_t_xloads) LOOP
      logs.dbg('Check for Mix of VapeCBD and Non-VapeCBD on PO');
      check_vapcbd_sp(l_div_part, l_r_ord.load_depart_sid, l_r_ord.cust_id, l_r_ord.po_num);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_vapcbd_for_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || VALIDATE_HEADER_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 02/12/03 | rhalpai | Added logic to log suspended orders to SYSP296A.
  ||                    | (needed by CSR's Suspend Inquiry screen)
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 05/20/05 | rhalpai | Moved existing logic to new common procedure,
  ||                    | UPD_ORD_TO_EXCPTN_SP, and changed call it. IM149431
  || 08/12/10 | rhalpai | Add call to CHECK_INVALID_PO_SP for PO validation.
  ||                    | PIR8909
  || 07/10/12 | rhalpai | Change call from MOVE_ORDER_TO_BAD_WELL_SP to
  ||                    | UPD_ORD_TO_EXCPTN_SP.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in call to
  ||                    | OP_MCLP300D_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE validate_header_sp(
    i_div_part    IN      NUMBER,
    i_ord_num     IN      NUMBER,
    o_err_rsn_cd  OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm            := 'OP_ORDER_VALIDATION_PK.VALIDATE_HEADER_SP';
    lar_parm              logs.tar_parm;
    l_r_ord_info          g_cur_ord_info%ROWTYPE;
    l_c_sysdate  CONSTANT DATE                     := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    o_err_rsn_cd := NULL;
    logs.dbg('Get Order/Customer Info');
    l_r_ord_info := ord_info_fn(i_div_part, i_ord_num);

    IF l_r_ord_info.ord_typ <> g_c_dist THEN
      logs.dbg('Check Customer Status');
      check_cust_stat_sp(l_r_ord_info, o_err_rsn_cd);
      logs.dbg('Check for Comet Dupl');
      check_comet_dup_sp(l_r_ord_info, o_err_rsn_cd);
      logs.dbg('Check for No Order Details');
      check_no_ord_dtls_sp(l_r_ord_info, o_err_rsn_cd);
      logs.dbg('Check for Duplicates');
      check_duplicates_sp(l_r_ord_info, o_err_rsn_cd);
      logs.dbg('Check for NoOrder with Items');
      check_no_ord_with_items_sp(l_r_ord_info, o_err_rsn_cd);
      logs.dbg('Fix Dummy Items');
      fix_dummy_items_sp(l_r_ord_info, o_err_rsn_cd);
      logs.dbg('Check for Invalid PO');
      check_invalid_po_sp(l_r_ord_info, o_err_rsn_cd);

      IF o_err_rsn_cd IS NOT NULL THEN
        logs.dbg('Upd Ord to Excptn');
        upd_ord_to_excptn_sp(i_div_part, i_ord_num, o_err_rsn_cd, 'ORDERRECEIPT', l_c_sysdate);

        IF o_err_rsn_cd = g_c_dummy_items_on_ord THEN
          logs.dbg('Dummy Items on Order');

          DECLARE
            l_t_ord_lns  type_ntab;
          BEGIN
            UPDATE    ordp120b b
                  SET b.ntshpb = g_c_item_not_found,
                      b.zipcdb = g_c_item_not_found,
                      b.excptn_sw = 'Y'
                WHERE b.div_part = i_div_part
                  AND b.ordnob = i_ord_num
                  AND b.orditb = l_r_ord_info.no_ord_item
            RETURNING         b.lineb
            BULK COLLECT INTO l_t_ord_lns;

            IF l_t_ord_lns.COUNT > 0 THEN
              FOR i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST LOOP
                op_mclp300d_pk.ins_sp(i_div_part, i_ord_num, l_t_ord_lns(i), g_c_item_not_found, NULL, NULL, NULL,
                                      NULL);
              END LOOP;
            END IF;   -- l_t_ord_lns.count > 0
          END;
        END IF;   -- l_err_rsn_cd = c_dummy_items_on_order
      END IF;   -- l_err_rsn_cd IS NOT NULL

      -- Check for Resolved Duplicate Order from Order Entry System
      IF l_r_ord_info.hdr_excptn_cd = g_c_comet_rslvd_dupl THEN
        logs.dbg('Log Comet Resolved Dup');
        op_mclp300d_pk.ins_sp(i_div_part, i_ord_num, 0, g_c_comet_rslvd_dupl, NULL, NULL, NULL, NULL, '1');
      END IF;
    END IF;   -- l_r_ord_info.ord_typ <> g_c_dist

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN g_e_bad_ord_num THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_header_sp;

  /*
  ||----------------------------------------------------------------------------
  || VALIDATE_DETAILS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/15/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status parm.
  || 06/07/05 | rhalpai | Moved check for NULL error reason code from within
  ||                    | CHECK_MAX_QTY_SP to VALIDATE_DETAILS_SP just prior to
  ||                    | calling CHECK_MAX_QTY_SP. Since the error reason code
  ||                    | is stored in a global variable it was possible that
  ||                    | in a call to CHECK_MAX_QTY_SP within the same session
  ||                    | following a call where an exception was found the
  ||                    | validation logic would be bypassed. This is now more
  ||                    | likely since CHECK_MAX_QTY_SP will now be called from
  ||                    | OP_GET_SUBS_SP. IM155504
  || 01/13/06 | rhalpai | Changed cursor to include order status 'P'. Moved call
  ||                    | to CHECK_ITEM_STAT_SP from within FIND_UNC_SUB_SP
  ||                    | to just before call to FIND_UNC_SUB_SP. Added logic
  ||                    | to save discontinued or inactive items exceptions to
  ||                    | the original not-ship-reason (zipcdb)_in the good
  ||                    | order well before calling checking for an
  ||                    | unconditional sub. If a sub exists this will allow
  ||                    | the GetSubs procedure to capture this info. PIR3159
  || 12/08/06 | rhalpai | Removed from cursor the restriction for authorized
  ||                    | items and not-ship-reason of NULL for order lines
  ||                    | in the exception well.
  || 01/15/07 | rhalpai | Changed to update not-ship-reason for exception
  ||                    | order lines before calling GetSubs procedure. IM280957
  || 04/05/07 | rhalpai | Changed to include a call to new Split Order process,
  ||                    | CHECK_SPLIT_QTY_SP, after call to
  ||                    | CHECK_ALL_DTLS_IN_ERR_SP. PIR4274
  || 06/19/07 | rhalpai | Changed to update v_from_good_order_well variable
  ||                    | after calls to OP_SWITCH_ORDER_WELLS_PK. This will
  ||                    | prevent unnecessary calls for Rounding Subs for order
  ||                    | lines that have been moved to the exception well and
  ||                    | will allow calls for Rounding Subs for order lines
  ||                    | that have been moved to the good well.
  || 11/06/08 | rhalpai | Changed cursor to override not-ship-reason with new
  ||                    | STRCTERR for strict order line with XCP status with
  ||                    | existing exception log entry. Initial exception will
  ||                    | be logged and applied to order line not-ship-reason
  ||                    | but will be replaced with STRCTERR during subsequent
  ||                    | order validation calls for order line in good well.
  ||                    | PIR5002
  || 05/15/09 | rhalpai | Added validation for Master-Case ordering customers.
  ||                    | PIR7548
  || 01/27/10 | rhalpai | Changed logic to always save orig-not-ship-rsn in
  ||                    | column zipcdb. IM563183
  || 07/12/11 | rhalpai | Changed logic to cursor one order line at a time to
  ||                    | reflect changes in order qty due to Max Qty. PIR6235
  || 09/26/11 | rhalpai | Change cursor to return Y/N for byp_max_sw. IM-029428
  || 01/25/12 | rhalpai | Add logic to check for Regulated Beverage exception.
  ||                    | PIR10620
  || 01/26/12 | rhalpai | Change logic to remove excepion order well.
  || 10/22/13 | rhalpai | Add logic to check for Self-Bill Item exception.
  ||                    | PIR13110
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in call to
  ||                    | OP_MCLP300D_PK.INS_SP.
  || 08/01/22 | rhalpai | Add logic to call CHECK_VAPCBD_FOR_ORD_SP. PIR22000
  || 10/17/22 | rhalpai | Add logic to only log new exceptions. PIR22000
  ||----------------------------------------------------------------------------
  */
  PROCEDURE validate_details_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm            := 'OP_ORDER_VALIDATION_PK.VALIDATE_DETAILS_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                     := SYSDATE;
    l_r_ord_info          g_cur_ord_info%ROWTYPE;
    l_t_ord_lns           type_ntab;
    l_cv                  SYS_REFCURSOR;
    l_r_ord_dtl           g_rt_ord_dtl;
    l_err_rsn_cd          mclp140a.rsncda%TYPE;
    l_not_shp_rsn         ordp120b.ntshpb%TYPE;
    l_ord_qty             PLS_INTEGER;
    l_item_max_qty        NUMBER;
    l_sub_found           VARCHAR2(3);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Order/Customer Info');
    l_r_ord_info := ord_info_fn(i_div_part, i_ord_num);
    logs.dbg('Get OrdLns');

    SELECT   b.lineb
    BULK COLLECT INTO l_t_ord_lns
        FROM ordp120b b
       WHERE b.div_part = i_div_part
         AND b.ordnob = i_ord_num
         AND b.lineb = NVL(i_ord_ln, b.lineb)
         AND b.statb IN('O', 'P')
         AND (   b.subrcb = 0
              OR b.subrcb IS NULL)
    ORDER BY b.orditb, b.lineb;

    IF l_t_ord_lns.COUNT > 0 THEN
      logs.dbg('Get Order Line');
      FOR i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST LOOP
        OPEN l_cv
         FOR
           SELECT b.div_part, b.ordnob AS ord_num, b.lineb AS ord_ln, b.excptn_sw, b.orditb AS catlg_num,
                  b.itemnb AS cbr_item, b.sllumb AS uom, b.ordqtb AS ord_qty, b.qtmulb AS qty_mult,
                  b.maxqtb AS max_qty, DECODE(b.bymaxb, '0', 'N', 'N', 'N', NULL, 'N', 'Y') AS byp_max_sw,
                  DECODE(a.pshipa, '1', 'Y', 'Y', 'Y', NULL, 'Y', 'N') AS allw_partl_sw,
                  NVL((SELECT 'STRCTERR'
                         FROM strct_ord_op1o so
                        WHERE so.div_part = b.div_part
                          AND so.ord_num = b.ordnob
                          AND so.ord_ln = b.lineb
                          AND so.stat = 'XCP'
                          AND EXISTS(SELECT 1
                                       FROM mclp300d d
                                      WHERE d.div_part = so.div_part
                                        AND d.ordnod = so.ord_num
                                        AND d.ordlnd = so.ord_ln)),
                      b.ntshpb
                     ) AS not_shp_rsn,
                  b.statb AS stat_cd
             FROM ordp100a a, ordp120b b
            WHERE a.div_part = i_div_part
              AND a.ordnoa = i_ord_num
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.lineb = l_t_ord_lns(i)
              AND b.statb IN('O', 'P')
              AND (   b.subrcb = 0
                   OR b.subrcb IS NULL);

        FETCH l_cv
         INTO l_r_ord_dtl;

        CLOSE l_cv;

        l_err_rsn_cd := NULL;
        l_sub_found := 'No';
        l_ord_qty := l_r_ord_dtl.ord_qty;
        l_not_shp_rsn := l_r_ord_dtl.not_shp_rsn;
        logs.dbg('Check for Bad Order Qty');
        check_bad_ord_qty_sp(l_r_ord_dtl.ord_qty, l_err_rsn_cd);
        logs.dbg('Check Item Status');
        check_item_stat_sp(l_r_ord_dtl, l_r_ord_info.disc_days, l_c_sysdate, l_err_rsn_cd, l_item_max_qty);

        IF (    l_r_ord_dtl.excptn_sw = 'N'
            AND l_err_rsn_cd IS NULL) THEN
          logs.dbg('Check for Master-Case Qty Requirements');
          check_mstrcs_qty_sp(i_div_part, i_ord_num, l_r_ord_dtl.ord_ln, l_ord_qty, l_err_rsn_cd);
        END IF;   -- l_r_ord.excptn_sw = 'N' AND l_err_rsn_cd IS NULL

        -- Bypass Unconditional Subbing during Allocation (P status)
        IF l_r_ord_dtl.stat_cd = 'O' THEN
          logs.dbg('Find Unconditional Sub');
          find_unc_sub_sp(l_r_ord_dtl, l_err_rsn_cd, l_sub_found);
        END IF;   -- l_r_ord_dtl.stat_cd = 'O'

        logs.dbg('Check for Existing Error');
        check_existing_err_sp(l_not_shp_rsn, l_err_rsn_cd);

        IF l_err_rsn_cd IS NOT NULL THEN
          IF l_r_ord_dtl.excptn_sw = 'N' THEN
            logs.dbg('Upd OrdLn to Excptn');

            UPDATE ordp120b b
               SET b.excptn_sw = 'Y'
             WHERE b.div_part = i_div_part
               AND b.ordnob = i_ord_num
               AND b.lineb = l_r_ord_dtl.ord_ln;

            UPDATE ordp100a a
               SET a.excptn_sw = 'Y'
             WHERE a.div_part = i_div_part
               AND a.ordnoa = i_ord_num
               AND a.excptn_sw = 'N'
               AND NOT EXISTS(SELECT 1
                                FROM ordp120b b
                               WHERE b.div_part = i_div_part
                                 AND b.ordnob = i_ord_num
                                 AND b.excptn_sw = 'N');

            l_r_ord_dtl.excptn_sw := 'Y';
          END IF;   -- l_r_ord.excptn_sw = 'N'

          logs.dbg('Upd Order Line with Exception');

          -- zipcdb used to save orig not-ship-reason code
          UPDATE ordp120b
             SET ntshpb = DECODE(l_sub_found, 'No', l_err_rsn_cd, ntshpb),
                 zipcdb = l_err_rsn_cd
           WHERE div_part = i_div_part
             AND ordnob = i_ord_num
             AND lineb = l_r_ord_dtl.ord_ln
             AND excptn_sw = 'Y';

          IF l_err_rsn_cd <> NVL(l_not_shp_rsn, '~') THEN
            logs.dbg('Log Exception');
            op_mclp300d_pk.ins_sp(i_div_part,
                                  i_ord_num,
                                  l_r_ord_dtl.ord_ln,
                                  l_err_rsn_cd,
                                  l_r_ord_dtl.cbr_item,
                                  l_r_ord_dtl.uom,
                                  l_r_ord_dtl.ord_qty,
                                  l_ord_qty
                                 );
          END IF;   -- l_err_rsn_cd <> NVL(l_not_shp_rsn, '~')
        END IF;   -- l_err_rsn_cd IS NOT NULL

        IF l_sub_found = 'No' THEN
          logs.dbg('Check for Bad Qty Multiple');
          check_qty_mult_sp(l_r_ord_dtl, l_ord_qty, l_err_rsn_cd);

          IF l_err_rsn_cd IS NULL THEN
            logs.dbg('Check for Max Qty Exception');
            check_max_qty_sp(i_div_part,
                             i_ord_num,
                             l_r_ord_dtl.ord_ln,
                             l_r_ord_dtl.catlg_num,
                             l_ord_qty,
                             l_r_ord_dtl.byp_max_sw,
                             l_r_ord_dtl.allw_partl_sw,
                             l_item_max_qty,
                             l_r_ord_dtl.max_qty
                            );
          END IF;   -- l_err_rsn_cd IS NULL
        END IF;   -- l_sub_found = 'No'

        IF (    l_err_rsn_cd IS NULL
            AND l_r_ord_dtl.excptn_sw = 'Y') THEN
          logs.dbg('Upd OrdLn to Non-Excptn');

          UPDATE ordp120b b
             SET b.excptn_sw = 'N'
           WHERE b.div_part = i_div_part
             AND b.ordnob = i_ord_num
             AND b.lineb = l_r_ord_dtl.ord_ln
             AND b.excptn_sw = 'Y';

          UPDATE ordp100a a
             SET a.excptn_sw = 'N'
           WHERE a.div_part = i_div_part
             AND a.ordnoa = i_ord_num
             AND a.excptn_sw = 'Y';

          l_r_ord_dtl.excptn_sw := 'N';
        END IF;   -- l_err_rsn_cd IS NULL

        -- adjust order if ord_qty has been changed
        -- by check_qty_mult_sp or check_max_qty_sp or check_mstrcs_qty_sp
        IF l_r_ord_dtl.ord_qty <> l_ord_qty THEN
          logs.dbg('Adjust Order Qty');

          UPDATE ordp120b
             SET ordqtb = l_ord_qty
           WHERE div_part = i_div_part
             AND ordnob = i_ord_num
             AND lineb = l_r_ord_dtl.ord_ln;
        END IF;   -- l_r_ord.ord_qty <> l_ord_qty

        IF l_r_ord_dtl.excptn_sw = 'N' THEN
          logs.dbg('Find Rounding Sub');
          find_rnd_sub_sp(l_r_ord_dtl, l_err_rsn_cd, l_sub_found);
        END IF;   -- l_r_ord.excptn_sw = 'N'
      END LOOP;
      logs.dbg('Check for Regulated Beverage Excption');
      check_reg_bev_for_ord_sp(i_div_part, i_ord_num);
      logs.dbg('Check for Self-Bill Item Excption');
      check_self_bill_sp(i_div_part, i_ord_num);
      logs.dbg('Check for Vape/CBD Excption');
      check_vapcbd_for_ord_sp(i_div_part, i_ord_num);
      logs.dbg('Check for All Details in Error');
      check_all_dtls_in_err_sp(l_r_ord_info, l_err_rsn_cd);

      IF NVL(l_err_rsn_cd, ' ') <> g_c_all_dtls_in_err THEN
        logs.dbg('Check for Qtys to be Split to Separate Orders');
        check_split_qty_sp(i_div_part, i_ord_num, i_ord_ln);
      END IF;
    END IF;   -- l_t_ord_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN g_e_bad_ord_num THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_details_sp;
END op_order_validation_pk;
/

