CREATE OR REPLACE PROCEDURE op_get_subs_sp(
  i_div_part   IN      NUMBER,
  i_sub_typ    IN      VARCHAR2,
  i_ord_num    IN      NUMBER,
  i_ord_ln     IN      NUMBER,
  o_msg        OUT     VARCHAR2,
  o_sub_found  OUT     VARCHAR2
) IS
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/24/01 | JUSTANI | Original
  || 08/27/01 | JUSTANI | 100 is valid Round Pct
  || 09/24/01 | JBARTON | Removed + RULE statements from SELECT statements Set
  ||                    | customer item # same as the original item if item #
  ||                    | from price routine is nulls.
  || 11/20/01 | SNAGABH | Added logic to sleep and re-try when connection to
  ||                    | MainFrame fails and send email to help desk after 1st
  ||                    | retry and a second email if process is successful
  || 01/13/02 | Sudheer | qualify the column names with table name
  || 02/05/02 | Sudheer | Added 'DIS" status for replacing/subbing the "TO" item
  || 03/04/02 | SNAGABH | Update to call OP_ITEM_PRICE_RETAIL_CICS_SP on initial
  ||                    | try, first, second retries and call
  ||                    | OP_ITEM_PRICE_RETAIL_AUTH_SP for all subsequent retries
  || 03/05/02 | rhalpai | Added a check for customer level conditional and
  ||                    | unconditional subs before looking at the group level.
  ||                    | Also, changed v_mail_msg contents and added an email
  ||                    | address to the v_email_addr list. Changed the first
  ||                    | email notification to wait for 3 retries.  Changed the
  ||                    | 'successful after re-trying' email to send when greater
  ||                    | than 3 retries.
  || 04/03/02 | JBARTON | Modified to use the div_item_alt table for SS/FC
  ||                    | rounding instead of the mclp060a table.
  ||                    | The div_item_alt table allows for the from and to CBR
  ||                    | item #s to be different whereas the mclp060a table does
  ||                    | not.
  || 07/15/02 | JBARTON | Modified to obtain and use the item level max quantity
  ||                    | value for the sub item. Added max quantity validation
  ||                    | logic from Order Receipt using the sub item's max qty
  || 03/24/03 | rhalpai | Changed to handle stops greater than 2 digits to correct
  ||                    | problem where an Invalid Number exception is generated
  ||                    | when formatting a 3 digit stop (more than 99 stops on DFLT
  ||                    | load) to '09'.  The formatting put '##' in the variable
  ||                    | which creates the Invalid Number. This happens during
  ||                    | the Max Qty Check for the Sub Item.
  || 06/26/03 | rhalpai | Changed RPISUB (replacement for inactive) and RPDSUB
  ||                    | (replacement for discontinued, inactive or partial out)
  ||                    | sections to exclude Corps listed in the Parm table under
  ||                    | 'EXCL_REPL'.  The is to handle Target's requirement of
  ||                    | not allowing subs.
  || 08/20/03 | JBARTON | Check for corporate accounts that want the hard retail
  ||                    | that they send to apply to all types of subs.
  ||                    | The default would be "N" (do not use hard retail)
  || 08/27/03 | rhalpai | Changed to pass order's original retail to pricing
  ||                    | routine to be used when the hard retail indicator is on.
  || 11/03/03 | rhalpai | Changed to handle subbing orders from both "Good" and
  ||                    | "Bad" order wells and handle bad order numbers. This
  ||                    | will allow us to conform to Business Rule for subbing
  ||                    | inactivated/discontinued items until the item is removed.
  ||                    | This previously only worked at Order Receipt time.
  ||                    | Changed lookup of "Hard Retail Override" and "Exclude
  ||                    | Replacements" parms to handle unlimited entries in the
  ||                    | parm table (previously limited to 6).
  || 11/24/03 | rhalpai | Changed NULL item max quantities from MCLP110B to be
  ||                    | stored as 99999 in g_item_max_ord_qty. This value will
  ||                    | be used when checking from Max Qty Violation and also
  ||                    | stored on the order detail line (MAXQTB) instead of 0
  ||                    | since original order line from the MF always contain a
  ||                    | value greater than 0. This fixes the problem with the
  ||                    | last install where Rounding Subs were not being applied
  ||                    | to orders when the FC item had a NULL Max Qty on MCLP110B.
  || 05/28/04 | rhalpai | Changes to support Cig System as cig inventory master.
  || 12/28/04 | rhalpai | Changed insert_sub_sp to include original ordered item.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status parm.
  || 06/07/05 | rhalpai | Changed to call OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP
  ||                    | IM155504
  || 01/06/06 | rhalpai | Replaced hard-coding of exception reason codes in cursor
  ||                    | at section 'Get Current Order Detail Info' with logic to
  ||                    | match those on the exception table MCLP140A that have
  ||                    | ATMPT_SUBS set to 'Y'. Changed update of original order
  ||                    | line in WHSP120B to store original exception in zipcdb.
  ||                    | PIR3159
  || 02/07/07 | rhalpai | Changed insert sub process to include ordered item in
  ||                    | order line cursor for original in exception well.
  ||                    | IM286463
  || 09/27/07 | rhalpai | Changed to bypass subbing to or from a strict item.
  ||                    | PIR5002
  || 06/02/09 | rhalpai | Added logic to apply Sub Qty Factor to Sub Order Qty.
  ||                    | PIR7657
  || 09/03/09 | rhalpai | Changed order detail info cursor to include not-ship-rsn
  ||                    | with NULL value which happens when sub line is repriced
  ||                    | with an original line that was in the exception well but
  ||                    | now the pricing removes the exception code (in this case
  ||                    | it was ITMSTRST and the restriction had since been
  ||                    | removed). Repricing a sub line reverts the sub, updates
  ||                    | updates the original not-ship-rsn to prev-not-ship-rsn
  ||                    | stored in ZIPCDB (which contained NULL in this case)
  ||                    | for lines in exception well, and reprices the original
  ||                    | line (in this case original was in the exception well
  ||                    | with a ITMSTRST but the state restrict has since been
  ||                    | removed so the not-ship-rsn for the exception line is
  ||                    | set to NULL. It then calls order validaion which
  ||                    | eventually calls GetSubs. Since it contained a NULL
  ||                    | not-ship-rsn it was excluded from the order detail info
  ||                    | cursor and no attempt was made to find the sub. No sub
  ||                    | was created even though the there was an unconditional
  ||                    | sub set up.
  ||                    | Also changed update of original order lines (when a sub
  ||                    | is found) to store any prev-not-ship-rsn in ZIPCDB and
  ||                    | always set the not-ship-rsn to the sub-type (ie: UNCSUB).
  ||                    | IM523987
  || 10/28/09 | rhalpai | Converted to use SUB_MSTR_OP5S. Added Corp Item Roundup
  ||                    | logic for Rounding Subs. PIR4342
  || 11/23/09 | rhalpai | Changed logic to improve performance. IM549327
  || 12/14/09 | rhalpai | Remove item status check for RPD subs. IM553961
  || 01/19/10 | rhalpai | Remove logic to set not-ship-rsn to prev-not-ship-rsn
  ||                    | for updates to original order lines in good well.
  ||                    | IM559884
  || 08/04/10 | rhalpai | Remove available inventory check for cigs from
  ||                    | conditional subs. PIR6473
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 11/07/11 | rhalpai | Change cursor for group rounding to include Full Case
  ||                    | to Pallet Rounding. PIR10416
  || 07/10/12 | rhalpai | Remove unused parms P_RND_PCT, P_ITEM_STAT, P_DIV_SUB_SW.
  ||                    | Remove unused columns.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 03/19/13 | rhalpai | Remove check for QtyAvail for Conditional Subs/Replacements.
  ||                    | PIR12452
  || 05/02/13 | rhalpai | Correct logic to allow adjusting sub order qty by a sub
  ||                    | qty factor for non-rounding subs. Add logic to use
  ||                    | current date for ETA for orders on reserved loads such
  ||                    | as DFLT.
  || 07/04/13 | rhalpai | Convert to use OP1G. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm and pass it in call to
  ||                    | OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP and pass
  ||                    | CatlgNum instead of CbrItem,UOM.
  || 06/02/17 | rhalpai | Change to call OP_PARMS_PK.VALS_FOR_PRFX_FN for parms.
  ||                    | PIR14910
  ||----------------------------------------------------------------------------
  */
  l_c_module        CONSTANT VARCHAR2(30)  := 'OP_GET_SUBS_SP';
  lar_parm                   logs.tar_parm;
  l_t_hrdrtl_ovrrd_corps     type_stab;
  l_t_excl_repl_corps        type_stab;
  l_c_unc_sub       CONSTANT VARCHAR2(6)   := 'UNCSUB';
  l_c_con_sub       CONSTANT VARCHAR2(6)   := 'CONSUB';
  l_c_rnd_sub       CONSTANT VARCHAR2(6)   := 'RNDSUB';
  l_c_cust_unc_sub  CONSTANT VARCHAR2(3)   := 'UCS';
  l_c_grp_unc_sub   CONSTANT VARCHAR2(3)   := 'UGP';
  l_c_rpi_sub       CONSTANT VARCHAR2(3)   := 'RPI';
  l_c_cust_con_sub  CONSTANT VARCHAR2(3)   := 'CCS';
  l_c_grp_con_sub   CONSTANT VARCHAR2(3)   := 'CGP';
  l_c_rpd_sub       CONSTANT VARCHAR2(3)   := 'RPD';
  l_c_div_sub       CONSTANT VARCHAR2(3)   := 'DIV';
  l_c_crp_rnd_sub   CONSTANT VARCHAR2(3)   := 'RND';
  l_c_grp_rnd_sub   CONSTANT VARCHAR2(3)   := 'SS';

  TYPE l_rt_ord IS RECORD(
    div             div_mstr_di1d.div_id%TYPE,
    div_part        PLS_INTEGER,
    cust_id         sysp200c.acnoc%TYPE,
    mcl_cust        mclp020b.mccusb%TYPE,
    grp             sysp200c.retgpc%TYPE,
    crp             PLS_INTEGER,
    eta_dt          DATE,
    item_pass_area  ordp120b.itpasb%TYPE,
    catlg_num       sawp505e.catite%TYPE,
    item_stat       mclp110b.statb%TYPE,
    ord_qty         PLS_INTEGER,
    orig_qty        PLS_INTEGER,
    byp_max_sw      VARCHAR2(1),
    hard_rtl_sw     VARCHAR2(1),
    rtl_amt         NUMBER,
    rtl_mult        PLS_INTEGER,
    accpt_div_subs  sysp200c.acdvsc%TYPE
  );

  TYPE l_cvt_ord IS REF CURSOR
    RETURN l_rt_ord;

  TYPE l_rt_sub IS RECORD(
    sub_typ        sub_mstr_op5s.sub_typ%TYPE,
    sub_cbr_item   sawp505e.iteme%TYPE,
    sub_uom        sawp505e.uome%TYPE,
    sub_catlg_num  sawp505e.catite%TYPE,
    prod_wgt       NUMBER,
    prod_vol       NUMBER,
    kit_sw         sawp505e.kite%TYPE,
    max_qty        PLS_INTEGER,
    nt_shp_rsn     ordp120b.ntshpb%TYPE,
    sub_cd         NUMBER,
    sub_ord_ln     NUMBER,
    qty_fctor      NUMBER(9, 4),
    sub_ord_qty    PLS_INTEGER
  );

  TYPE l_cvt_sub IS REF CURSOR
    RETURN l_rt_sub;

  TYPE l_rt_pricing IS RECORD(
    price_amt  NUMBER,
    rtl_amt    NUMBER,
    rtl_mult   PLS_INTEGER,
    mfst_catg  mclp210c.manctc%TYPE,
    tote_catg  mclp200b.totctb%TYPE,
    lbl_catg   PLS_INTEGER,
    invc_catg  PLS_INTEGER,
    cust_item  ordp120b.cusitb%TYPE,
    price_ts   DATE
  );

  PROCEDURE ord_info_sp(
    o_r_ord  OUT  l_rt_ord
  ) IS
    l_cv_ord  l_cvt_ord;
  BEGIN
    OPEN l_cv_ord
     FOR
       SELECT d.div_id, d.div_part, a.custa, cx.mccusb, c.retgpc, cx.corpb,
              DECODE(se.eta_ts, DATE '1900-01-01', TRUNC(SYSDATE), TRUNC(se.eta_ts)) AS eta_dt, b.itpasb, b.orditb,
              di.statb, b.ordqtb, b.orgqtb, NVL(b.bymaxb, '0'),
              DECODE(hdr.corp_cd, NULL, 'N', DECODE(b.rtfixb, '1', 'Y', 'Y', 'Y', 'N')) AS hard_rtl_sw,
              NVL(b.hdrtab, 0), NVL(b.hdrtmb, 0), c.acdvsc
         FROM div_mstr_di1d d, ordp120b b, ordp100a a, stop_eta_op1g se, sysp200c c, mclp020b cx, mclp110b di,
              (SELECT TO_NUMBER(t.column_value) AS corp_cd
                 FROM TABLE(CAST(l_t_hrdrtl_ovrrd_corps AS type_stab)) t) hdr
        WHERE d.div_part = i_div_part
          AND b.div_part = d.div_part
          AND b.ordnob = i_ord_num
          AND b.lineb = i_ord_ln
          AND b.subrcb = 0
          AND (   b.excptn_sw = 'N'
               OR b.ntshpb IN(SELECT ma.rsncda
                                FROM mclp140a ma
                               WHERE ma.atmpt_subs = 'Y'))
          AND a.div_part = b.div_part
          AND a.ordnoa = b.ordnob
          AND a.dsorda = 'R'
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = b.div_part
                            AND si.item_num = b.itemnb
                            AND si.uom = b.sllumb)
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa
          AND c.div_part = se.div_part
          AND c.acnoc = se.cust_id
          AND cx.div_part = se.div_part
          AND cx.custb = se.cust_id
          AND di.div_part = b.div_part
          AND di.itemb = b.itemnb
          AND di.uomb = b.sllumb
          AND hdr.corp_cd(+) = cx.corpb;

    FETCH l_cv_ord
     INTO o_r_ord;
  END ord_info_sp;

  PROCEDURE find_cust_unc_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'UNCSUB' AS nt_shp_rsn,
              1 AS sub_cd, i_ord_ln + .9 AS sub_ord_ln, s.qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
         FROM sub_mstr_op5s s, sawp505e e, mclp110b di
        WHERE s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'UCS'
          AND s.cls_typ = 'CUS'
          AND s.cls_id = i_r_ord.cust_id
          AND s.catlg_num = i_r_ord.catlg_num
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND e.catite = s.sub_item
          AND di.div_part = s.div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS')
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_cust_unc_sub_sp;

  PROCEDURE find_grp_unc_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'UNCSUB' AS nt_shp_rsn,
              1 AS sub_cd, i_ord_ln + .9 AS sub_ord_ln, s.qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
         FROM sub_mstr_op5s s, sawp505e e, mclp110b di
        WHERE s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'UGP'
          AND s.cls_typ = 'GRP'
          AND s.cls_id = i_r_ord.grp
          AND s.catlg_num = i_r_ord.catlg_num
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND e.catite = s.sub_item
          AND di.div_part = i_r_ord.div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS')
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_grp_unc_sub_sp;

  PROCEDURE find_rpi_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'RPISUB' AS nt_shp_rsn,
              2 AS sub_cd, i_ord_ln + .8 AS sub_ord_ln, s.qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
         FROM sub_mstr_op5s s, sawp505e e, mclp110b di
        WHERE s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'RPI'
          AND s.cls_typ = 'ITM'
          AND s.cls_id = 'ALL'
          AND s.catlg_num = i_r_ord.catlg_num
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND i_r_ord.item_stat = 'INA'
          AND i_r_ord.crp NOT IN(SELECT TO_NUMBER(t.column_value)
                                   FROM TABLE(CAST(l_t_excl_repl_corps AS type_stab)) t)
          AND e.catite = s.sub_item
          AND di.div_part = i_r_ord.div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS')
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_rpi_sub_sp;

  PROCEDURE find_cust_con_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'CONSUB' AS nt_shp_rsn,
              4 AS sub_cd, i_ord_ln + .6 AS sub_ord_ln, s.qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
         FROM sub_mstr_op5s s, sawp505e e, mclp110b di
        WHERE s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'CCS'
          AND s.cls_typ = 'CUS'
          AND s.cls_id = i_r_ord.cust_id
          AND s.catlg_num = i_r_ord.catlg_num
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND e.catite = s.sub_item
          AND di.div_part = i_r_ord.div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS')
          AND EXISTS(SELECT 1
                       FROM whsp300c w
                      WHERE w.div_part = di.div_part
                        AND w.itemc = di.itemb
                        AND w.uomc = di.uomb)
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_cust_con_sub_sp;

  PROCEDURE find_grp_con_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'CONSUB' AS nt_shp_rsn,
              4 AS sub_cd, i_ord_ln + .6 AS sub_ord_ln, s.qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
         FROM sub_mstr_op5s s, sawp505e e, mclp110b di
        WHERE s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'CGP'
          AND s.cls_typ = 'GRP'
          AND s.cls_id = i_r_ord.grp
          AND s.catlg_num = i_r_ord.catlg_num
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND e.catite = s.sub_item
          AND di.div_part = i_r_ord.div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS')
          AND EXISTS(SELECT 1
                       FROM whsp300c w
                      WHERE w.div_part = di.div_part
                        AND w.itemc = di.itemb
                        AND w.uomc = di.uomb)
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_grp_con_sub_sp;

  PROCEDURE find_rpd_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'RPDSUB' AS nt_shp_rsn,
              2 AS sub_cd, i_ord_ln + .5 AS sub_ord_ln, s.qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
         FROM sub_mstr_op5s s, sawp505e e, mclp110b di
        WHERE s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'RPI'
          AND s.cls_typ = 'ITM'
          AND s.cls_id = 'ALL'
          AND s.catlg_num = i_r_ord.catlg_num
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND i_r_ord.crp NOT IN(SELECT TO_NUMBER(t.column_value)
                                   FROM TABLE(CAST(l_t_excl_repl_corps AS type_stab)) t)
          AND e.catite = s.sub_item
          AND di.div_part = i_r_ord.div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS')
          AND EXISTS(SELECT 1
                       FROM whsp300c w
                      WHERE w.div_part = di.div_part
                        AND w.itemc = di.itemb
                        AND w.uomc = di.uomb)
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_rpd_sub_sp;

  PROCEDURE find_div_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    IF i_r_ord.accpt_div_subs = 'Y' THEN
      OPEN l_cv_sub
       FOR
         SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
                e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'DIVSUB' AS nt_shp_rsn,
                5 AS sub_cd, i_ord_ln + .4 AS sub_ord_ln, s.qty_fctor,
                LEAST(NVL(di.max_ord_qty, 99999), CEIL(ROUND(i_r_ord.ord_qty * s.qty_fctor, 1))) AS sub_ord_qty
           FROM sub_mstr_op5s s, sawp505e e, mclp110b di
          WHERE s.div_part = i_r_ord.div_part
            AND s.sub_typ = 'DIV'
            AND s.cls_typ = 'ITM'
            AND s.cls_id = 'ALL'
            AND s.catlg_num = i_r_ord.catlg_num
            AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
            AND e.catite = s.sub_item
            AND di.div_part = i_r_ord.div_part
            AND di.itemb = e.iteme
            AND di.uomb = e.uome
            AND di.statb IN('ACT', 'DIS')
            AND EXISTS(SELECT 1
                         FROM whsp300c w
                        WHERE w.div_part = di.div_part
                          AND w.itemc = di.itemb
                          AND w.uomc = di.uomb)
            AND NOT EXISTS(SELECT 1
                             FROM strct_item_op3v si
                            WHERE si.div_part = di.div_part
                              AND si.item_num = di.itemb
                              AND si.uom = di.uomb);

      FETCH l_cv_sub
       INTO o_r_sub;
    END IF;   -- i_r_ord.accpt_div_subs = 'Y'
  END find_div_sub_sp;

  PROCEDURE find_crp_rnd_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT s.sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num, e.wghte AS prod_wgt,
              e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty, 'RNDSUB' AS nt_shp_rsn,
              3 AS sub_cd, i_ord_ln + .7 AS sub_ord_ln, alt.qty_fctr AS qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999),
                    FLOOR(i_r_ord.ord_qty / alt.qty_fctr)
                    +(CASE
                        WHEN MOD(i_r_ord.ord_qty, alt.qty_fctr) >= s.qty_fctor * alt.qty_fctr / 100 THEN 1
                        ELSE 0
                      END)
                   ) AS sub_ord_qty
         FROM sawp505e i, div_item_alt alt, mclp110b di, sawp505e e, sub_mstr_op5s s
        WHERE i.catite = i_r_ord.catlg_num
          AND alt.div_part = i_r_ord.div_part
          AND alt.item_num = i.iteme
          AND alt.item_uom = i.uome
          AND alt.alt_typ = 'SS'
          AND di.div_part = alt.div_part
          AND di.itemb = alt.alt_item
          AND di.uomb = alt.alt_uom
          AND di.statb IN('ACT', 'DIS')
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb)
          AND e.iteme = di.itemb
          AND e.uome = di.uomb
          AND s.div_part = i_r_ord.div_part
          AND s.sub_typ = 'RND'
          AND s.cls_typ = 'CRP'
          AND s.cls_id = LPAD(i_r_ord.crp, 3, '0')
          AND s.catlg_num = i.catite
          AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt
          AND s.qty_fctor > 0
          AND (i_r_ord.ord_qty / alt.qty_fctr) >= LEAST(1, s.qty_fctor / 100);

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_crp_rnd_sub_sp;

  PROCEDURE find_grp_rnd_sub_sp(
    i_r_ord  IN      l_rt_ord,
    o_r_sub  OUT     l_rt_sub
  ) IS
    l_cv_sub  l_cvt_sub;
  BEGIN
    OPEN l_cv_sub
     FOR
       SELECT 'RND' AS sub_typ, e.iteme AS sub_cbr_item, e.uome AS sub_uom, e.catite AS sub_catlg_num,
              e.wghte AS prod_wgt, e.cubee AS prod_vol, e.kite AS kit_sw, NVL(di.max_ord_qty, 99999) AS max_qty,
              'RNDSUB' AS nt_shp_rsn, 3 AS sub_cd, i_ord_ln + .7 AS sub_ord_ln, alt.qty_fctr AS qty_fctor,
              LEAST(NVL(di.max_ord_qty, 99999),
                    FLOOR(i_r_ord.ord_qty / alt.qty_fctr)
                    +(CASE
                        WHEN MOD(i_r_ord.ord_qty, alt.qty_fctr) >=
                                       DECODE(alt.alt_typ, 'SS', ma.rndpra, 'FC', ma.fc_plt_rnd_pct) * alt.qty_fctr
                                       / 100 THEN 1
                        ELSE 0
                      END
                     )
                   ) AS sub_ord_qty
         FROM sawp505e i, div_item_alt alt, mclp110b di, sawp505e e, mclp100a ma
        WHERE i.catite = i_r_ord.catlg_num
          AND alt.div_part = i_r_ord.div_part
          AND alt.item_num = i.iteme
          AND alt.item_uom = i.uome
          AND alt.alt_typ IN('SS', 'FC')
          AND di.div_part = alt.div_part
          AND di.itemb = alt.alt_item
          AND di.uomb = alt.alt_uom
          AND di.statb IN('ACT', 'DIS')
          AND NOT EXISTS(SELECT 1
                           FROM strct_item_op3v si
                          WHERE si.div_part = di.div_part
                            AND si.item_num = di.itemb
                            AND si.uom = di.uomb)
          AND e.iteme = di.itemb
          AND e.uome = di.uomb
          AND ma.div_part = i_r_ord.div_part
          AND ma.cstgpa = i_r_ord.grp
          AND DECODE(alt.alt_typ, 'SS', ma.rndpra, 'FC', ma.fc_plt_rnd_pct) > 0
          AND (i_r_ord.ord_qty / alt.qty_fctr) >= LEAST(1,
                                                        DECODE(alt.alt_typ, 'SS', ma.rndpra, 'FC', ma.fc_plt_rnd_pct)
                                                        / 100
                                                       );

    FETCH l_cv_sub
     INTO o_r_sub;
  END find_grp_rnd_sub_sp;

  PROCEDURE get_pricing_sp(
    i_r_ord      IN      l_rt_ord,
    i_r_sub      IN      l_rt_sub,
    o_r_pricing  OUT     l_rt_pricing
  ) IS
    lar_pricing_parm            logs.tar_parm;
    l_c_sub_sw         CONSTANT VARCHAR2(1)            := 'Y';
    l_c_gmp_sw         CONSTANT VARCHAR2(1)            :=(CASE
                                                            WHEN i_r_sub.sub_uom LIKE 'GM%' THEN 'Y'
                                                            ELSE 'N'
                                                          END);
    l_c_hard_price_sw  CONSTANT VARCHAR2(1)            := 'N';
    l_c_dist_id        CONSTANT VARCHAR2(10)           := ' ';
    l_not_shp_rsn               ordp120b.ntshpb%TYPE;
    l_authzd_sw                 VARCHAR2(1);
    l_error_sw                  VARCHAR2(1);
    l_price_ts                  VARCHAR2(14);
    l_e_pricing_error           EXCEPTION;
  BEGIN
    logs.add_parm(lar_pricing_parm, 'Div', i_r_ord.div);
    logs.add_parm(lar_pricing_parm, 'MclCust', i_r_ord.mcl_cust);
    logs.add_parm(lar_pricing_parm, 'CatlgNum', i_r_sub.sub_catlg_num);
    logs.add_parm(lar_pricing_parm, 'SubTyp', i_r_sub.sub_typ);
    logs.add_parm(lar_pricing_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_pricing_parm, 'OrdLn', i_ord_ln);
    o_r_pricing.rtl_amt := i_r_ord.rtl_amt;
    o_r_pricing.rtl_mult := i_r_ord.rtl_mult;
    o_r_pricing.price_amt := 0;
    op_pricing_pk.retrieve_pricing_sp(i_r_ord.mcl_cust,
                                      i_r_sub.sub_catlg_num,
                                      i_r_ord.div,
                                      i_r_ord.cust_id,
                                      i_r_sub.sub_cbr_item,
                                      i_r_sub.sub_uom,
                                      TO_CHAR(i_r_ord.eta_dt, 'YYYYMMDD'),
                                      l_c_gmp_sw,
                                      l_c_sub_sw,
                                      i_r_sub.kit_sw,
                                      l_c_hard_price_sw,
                                      o_r_pricing.price_amt,
                                      i_r_ord.hard_rtl_sw,
                                      o_r_pricing.rtl_amt,
                                      o_r_pricing.rtl_mult,
                                      NVL(i_r_ord.item_pass_area, ' '),
                                      l_c_dist_id,
                                      o_r_pricing.mfst_catg,
                                      o_r_pricing.tote_catg,
                                      o_r_pricing.lbl_catg,
                                      o_r_pricing.invc_catg,
                                      l_authzd_sw,
                                      o_r_pricing.cust_item,
                                      l_not_shp_rsn,
                                      l_price_ts,
                                      l_error_sw,
                                      o_msg
                                     );

    IF l_error_sw = 'Y' THEN
      RAISE l_e_pricing_error;
    END IF;   -- l_error_sw = 'Y'

    -- << Authorized and No Errors >>
    IF (    l_authzd_sw = 'Y'
        AND (   l_error_sw IS NULL
             OR l_error_sw IN('N', ' '))
        AND TRIM(l_not_shp_rsn) IS NULL) THEN
      o_sub_found := 'Yes';
      o_r_pricing.price_ts := TO_DATE(l_price_ts, 'YYYYMMDDHH24MISS');
    ELSIF(    TRIM(o_msg) IS NULL
          AND l_not_shp_rsn IS NOT NULL) THEN
      o_msg := l_not_shp_rsn;
    END IF;
  EXCEPTION
    WHEN l_e_pricing_error THEN
      logs.warn('Call to OP_PRICING_PK.RETRIEVE_PRICING_SP returned Error: ' || o_msg,
                lar_parm,
                logs.parm_list(lar_pricing_parm) || ' ErrorSW: ' || l_error_sw
               );
    WHEN OTHERS THEN
      logs.err(SQLERRM,
               lar_parm,
               'Pricing error' || cnst.newline_char || logs.parm_list(lar_pricing_parm) || ' ErrorSW: ' || l_error_sw
              );
  END get_pricing_sp;

  PROCEDURE find_sub_sp(
    i_sub_typ    IN      VARCHAR2,
    i_r_ord      IN      l_rt_ord,
    o_r_sub      OUT     l_rt_sub,
    o_r_pricing  OUT     l_rt_pricing
  ) IS
  BEGIN
    CASE i_sub_typ
      WHEN l_c_cust_unc_sub THEN
        find_cust_unc_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_grp_unc_sub THEN
        find_grp_unc_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_rpi_sub THEN
        find_rpi_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_cust_con_sub THEN
        find_cust_con_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_grp_con_sub THEN
        find_grp_con_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_rpd_sub THEN
        find_rpd_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_div_sub THEN
        find_div_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_crp_rnd_sub THEN
        find_crp_rnd_sub_sp(i_r_ord, o_r_sub);
      WHEN l_c_grp_rnd_sub THEN
        find_grp_rnd_sub_sp(i_r_ord, o_r_sub);
    END CASE;

    IF o_r_sub.sub_typ IS NOT NULL THEN
      logs.dbg('Get Pricing');
      get_pricing_sp(i_r_ord, o_r_sub, o_r_pricing);

      IF o_r_pricing.price_ts IS NOT NULL THEN
        IF i_sub_typ NOT IN(l_c_crp_rnd_sub, l_c_grp_rnd_sub) THEN
          logs.dbg('Apply Sub Qty Factor');

          SELECT CEIL(ROUND(i_r_ord.ord_qty * NVL(MAX(s.qty_fctor), 1), 1))
            INTO o_r_sub.sub_ord_qty
            FROM sub_mstr_op5s s
           WHERE s.div_part = i_r_ord.div_part
             AND s.cls_typ = 'ITM'
             AND s.cls_id = 'ALL'
             AND s.sub_typ = 'QTY'
             AND s.catlg_num = i_r_ord.catlg_num
             AND s.sub_item = o_r_sub.sub_catlg_num
             AND i_r_ord.eta_dt BETWEEN s.start_dt AND s.end_dt;
        END IF;   -- i_sub_typ NOT IN(l_c_crp_rnd_sub, l_c_grp_rnd_sub)

        logs.dbg('Check Max Qty');
        op_order_validation_pk.check_max_qty_sp(i_r_ord.div_part,
                                                i_ord_num,
                                                o_r_sub.sub_ord_ln,
                                                o_r_sub.sub_catlg_num,
                                                o_r_sub.sub_ord_qty,
                                                i_r_ord.byp_max_sw,
                                                '0',
                                                o_r_sub.max_qty,
                                                o_r_sub.max_qty
                                               );

        IF o_r_sub.sub_ord_qty <= 0 THEN
          o_sub_found := 'No';
          o_msg := 'Any sub qty would exceed item max';
        END IF;   -- o_r_sub.sub_ord_qty <= 0
      END IF;   -- o_r_pricing.price_ts IS NOT NULL
    END IF;   -- o_r_sub.sub_typ IS NOT NULL
  END find_sub_sp;

  PROCEDURE ins_sub_sp(
    i_r_ord      IN  l_rt_ord,
    i_r_sub      IN  l_rt_sub,
    i_r_pricing  IN  l_rt_pricing
  ) IS
    l_r_ordp120b  ordp120b%ROWTYPE;
  BEGIN
    logs.dbg('Get Order Detail');

    SELECT *
      INTO l_r_ordp120b
      FROM ordp120b b
     WHERE b.div_part = i_r_ord.div_part
       AND b.ordnob = i_ord_num
       AND b.lineb = i_ord_ln;

    logs.dbg('OrdDtl Assignments');
    l_r_ordp120b.lineb := i_r_sub.sub_ord_ln;
    l_r_ordp120b.subrcb := i_r_sub.sub_cd;
    l_r_ordp120b.ntshpb := NULL;
    l_r_ordp120b.authb := '1';
    l_r_ordp120b.itemnb := i_r_sub.sub_cbr_item;
    l_r_ordp120b.sllumb := i_r_sub.sub_uom;
    l_r_ordp120b.orgitb := l_r_ordp120b.orditb;
    l_r_ordp120b.orditb := i_r_sub.sub_catlg_num;
    l_r_ordp120b.cusitb := NVL(TRIM(i_r_pricing.cust_item), l_r_ordp120b.orditb);
    l_r_ordp120b.maxqtb := i_r_sub.max_qty;
    l_r_ordp120b.orgqtb := i_r_sub.sub_ord_qty;
    l_r_ordp120b.ordqtb := i_r_sub.sub_ord_qty;
    l_r_ordp120b.actqtb := i_r_sub.sub_ord_qty;
    l_r_ordp120b.alcqtb := 0;
    l_r_ordp120b.itpasb := i_r_ord.item_pass_area;
    l_r_ordp120b.manctb := NVL(i_r_pricing.mfst_catg, '000');
    l_r_ordp120b.totctb :=(CASE
                             WHEN i_r_pricing.tote_catg IN('   ', '000') THEN NULL
                             ELSE i_r_pricing.tote_catg
                           END);
    l_r_ordp120b.labctb := i_r_pricing.lbl_catg;
    l_r_ordp120b.invctb := i_r_pricing.invc_catg;
    l_r_ordp120b.prfixb := '0';
    l_r_ordp120b.hdprcb := i_r_pricing.price_amt;
    l_r_ordp120b.hdrtab := i_r_pricing.rtl_amt;
    l_r_ordp120b.hdrtmb := i_r_pricing.rtl_mult;
    l_r_ordp120b.rtfixb :=(CASE
                             WHEN i_r_ord.hard_rtl_sw IN('1', 'Y') THEN '1'
                             ELSE '0'
                           END);
    l_r_ordp120b.prstdb := TRUNC(i_r_pricing.price_ts) - DATE '1900-02-28';
    l_r_ordp120b.prsttb := TO_NUMBER(TO_CHAR(i_r_pricing.price_ts, 'HH24MISS'));

    IF l_r_ordp120b.excptn_sw = 'Y' THEN
      l_r_ordp120b.excptn_sw := 'N';
      logs.dbg('Upd EXCPTN_SW on OrdHdr to N');

      UPDATE ordp100a a
         SET a.excptn_sw = 'N'
       WHERE a.div_part = i_r_ord.div_part
         AND a.ordnoa = i_ord_num
         AND a.excptn_sw = 'Y';

      logs.dbg('Mark order level exception as resolved');

      UPDATE mclp300d
         SET resexd = 1,
             resusd = 'OP_GET_SUBS_SP',
             resdtd = l_r_ordp120b.prstdb,
             restmd = l_r_ordp120b.prsttb
       WHERE div_part = i_r_ord.div_part
         AND ordnod = i_ord_num
         AND ordlnd = 0
         AND resexd <> 1;
    END IF;   -- l_r_ordp120b.excptn_sw = 'Y'

    logs.dbg('Add Sub');

    INSERT INTO ordp120b
         VALUES l_r_ordp120b;
  END ins_sub_sp;

  PROCEDURE upd_orig_ord_ln_sp(
    i_nt_shp_rsn  IN  VARCHAR2,
    i_orig_qty    IN  NUMBER
  ) IS
  BEGIN
    UPDATE ordp120b
       SET subrcb = DECODE(subrcb, 0, 999, subrcb),
           zipcdb = DECODE(excptn_sw, 'Y', ntshpb, zipcdb),
           ntshpb = i_nt_shp_rsn,
           orgqtb = NVL(i_orig_qty, orgqtb),
           ordqtb = NVL(i_orig_qty, ordqtb)
     WHERE div_part = i_div_part
       AND ordnob = i_ord_num
       AND lineb = i_ord_ln;
  END upd_orig_ord_ln_sp;

  PROCEDURE addl_ln_for_rnd_sub_sp(
    i_addl_ord_qty  IN  PLS_INTEGER
  ) IS
    l_r_ordp120b  ordp120b%ROWTYPE;
  BEGIN
    logs.dbg('Get Orig OrdLn');

    SELECT *
      INTO l_r_ordp120b
      FROM ordp120b
     WHERE div_part = i_div_part
       AND ordnob = i_ord_num
       AND lineb = i_ord_ln;

    logs.dbg('Set New OrdLn');

    SELECT COUNT(*) + 1
      INTO l_r_ordp120b.lineb
      FROM ordp120b b
     WHERE b.div_part = i_div_part
       AND b.ordnob = i_ord_num
       AND b.lineb = FLOOR(b.lineb);

    l_r_ordp120b.ordqtb := i_addl_ord_qty;
    l_r_ordp120b.alcqtb := 0;
    l_r_ordp120b.orgqtb := i_addl_ord_qty;
    l_r_ordp120b.actqtb := i_addl_ord_qty;
    l_r_ordp120b.ntshpb := NULL;
    l_r_ordp120b.subrcb := 0;
    logs.dbg('Add Rounding Sub Additional Line');

    INSERT INTO ordp120b
         VALUES l_r_ordp120b;
  END addl_ln_for_rnd_sub_sp;

  PROCEDURE main_sp IS
    l_r_ord             l_rt_ord;
    l_r_sub             l_rt_sub;
    l_r_pricing         l_rt_pricing;
    l_sub_typ           VARCHAR2(6);
    l_rnd_addl_ord_qty  PLS_INTEGER;
  BEGIN
    o_sub_found := 'No';
    l_sub_typ :=(CASE i_sub_typ
                   WHEN l_c_unc_sub THEN l_c_cust_unc_sub
                   WHEN l_c_con_sub THEN l_c_cust_con_sub
                   WHEN l_c_rnd_sub THEN l_c_crp_rnd_sub
                 END
                );

    IF l_sub_typ IS NOT NULL THEN
      l_t_hrdrtl_ovrrd_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_hrdrtl_ovrrd);
      l_t_excl_repl_corps := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_excl_repl);
      logs.dbg('Get Ord Info');
      ord_info_sp(l_r_ord);
      LOOP
        EXIT WHEN(   o_sub_found = 'Yes'
                  OR l_sub_typ IS NULL);
        logs.dbg('Find Sub');
        find_sub_sp(l_sub_typ, l_r_ord, l_r_sub, l_r_pricing);

        IF o_sub_found = 'Yes' THEN
          logs.dbg('Ins Sub');
          ins_sub_sp(l_r_ord, l_r_sub, l_r_pricing);

          IF l_sub_typ IN(l_c_crp_rnd_sub, l_c_grp_rnd_sub) THEN
            logs.dbg('Set Qtys for Rounding Sub');
            l_rnd_addl_ord_qty := GREATEST(0, l_r_ord.ord_qty -(l_r_sub.sub_ord_qty * l_r_sub.qty_fctor));
            l_r_ord.orig_qty := l_r_sub.sub_ord_qty * l_r_sub.qty_fctor;
          END IF;   -- l_sub_typ IN(l_c_crp_rnd_sub, l_c_grp_rnd_sub)

          logs.dbg('Upd Orig OrdLn');
          upd_orig_ord_ln_sp(l_r_sub.nt_shp_rsn, l_r_ord.orig_qty);

          IF l_rnd_addl_ord_qty > 0 THEN
            logs.dbg('Additional OrdLn for Rounding Sub');
            addl_ln_for_rnd_sub_sp(l_rnd_addl_ord_qty);
          END IF;   -- l_rnd_addl_ord_qty > 0
        ELSE
          l_sub_typ :=(CASE l_sub_typ
                         WHEN l_c_cust_unc_sub THEN l_c_grp_unc_sub
                         WHEN l_c_grp_unc_sub THEN l_c_rpi_sub
                         WHEN l_c_rpi_sub THEN NULL
                         WHEN l_c_cust_con_sub THEN l_c_grp_con_sub
                         WHEN l_c_grp_con_sub THEN l_c_rpd_sub
                         WHEN l_c_rpd_sub THEN l_c_div_sub
                         WHEN l_c_div_sub THEN NULL
                         WHEN l_c_crp_rnd_sub THEN l_c_grp_rnd_sub
                         WHEN l_c_grp_rnd_sub THEN NULL
                       END
                      );
        END IF;   -- o_sub_found = 'Yes'
      END LOOP;
    END IF;   -- l_sub_typ IS NOT NULL
  END main_sp;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'DivPart', i_div_part);
  logs.add_parm(lar_parm, 'SubTyp', i_sub_typ);
  logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
  logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
  logs.dbg('ENTRY', lar_parm);
  main_sp;
  timer.stopme(l_c_module || env.get_session_id);
  logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END op_get_subs_sp;
/

