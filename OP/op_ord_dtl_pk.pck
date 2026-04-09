CREATE OR REPLACE PACKAGE op_ord_dtl_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  SUBTYPE g_st_vchar IS VARCHAR2(100);

  TYPE g_rt_ord_dtl IS RECORD(
    excptn_sw           VARCHAR2(1),
    div_part            g_st_vchar,
    ord_num             g_st_vchar,
    ord_ln              g_st_vchar,
    stat_cd             g_st_vchar,
    sub_cd              g_st_vchar,
    nt_shp_rsn          g_st_vchar,
    orig_nt_shp_rsn     g_st_vchar,
    auth_cd             g_st_vchar,
    cbr_item            g_st_vchar,
    uom                 g_st_vchar,
    catlg_num           g_st_vchar,
    orig_item           g_st_vchar,
    cust_item           g_st_vchar,
    qty_mult            g_st_vchar,
    max_qty             g_st_vchar,
    byp_max_sw          g_st_vchar,
    orig_qty            g_st_vchar,
    ord_qty             g_st_vchar,
    act_qty             g_st_vchar,
    alloc_qty           g_st_vchar,
    pick_qty            g_st_vchar,
    item_pass_area      g_st_vchar,
    lbl_catg            g_st_vchar,
    mfst_catg           g_st_vchar,
    tote_catg           g_st_vchar,
    invc_catg           g_st_vchar,
    price_amt           g_st_vchar,
    rtl_amt             g_st_vchar,
    rtl_mult            g_st_vchar,
    hard_rtl_sw         g_st_vchar,
    hard_price_sw       g_st_vchar,
    price_dt            g_st_vchar,
    price_tm            g_st_vchar,
    rlse_ts             g_st_vchar,
    tbill_dist_atch_sw  g_st_vchar
  );

  TYPE g_tt_ord_dtls IS TABLE OF ordp120b%ROWTYPE;

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_status_unreleased  CONSTANT VARCHAR2(1) := '-';
  g_c_status_released    CONSTANT VARCHAR2(1) := '+';
  g_c_not                CONSTANT VARCHAR2(1) := '!';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION sub_line_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  )
    RETURN NUMBER;

  FUNCTION ord_dtl_rec_fn(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  )
    RETURN g_rt_ord_dtl;

  FUNCTION sel_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_ord_stat  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN ordp120b%ROWTYPE;

  FUNCTION sel_for_upd_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  )
    RETURN g_tt_ord_dtls;

  FUNCTION sel_for_upd_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN g_tt_ord_dtls;

  FUNCTION ins_fn(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  )
    RETURN NUMBER;

  FUNCTION ins_fn(
    i_excptn_sw           IN  VARCHAR2,
    i_div_part            IN  VARCHAR2,
    i_ord_num             IN  VARCHAR2,
    i_ord_ln              IN  VARCHAR2,
    i_stat_cd             IN  VARCHAR2,
    i_sub_cd              IN  VARCHAR2,
    i_nt_shp_rsn          IN  VARCHAR2,
    i_orig_nt_shp_rsn     IN  VARCHAR2,
    i_auth_cd             IN  VARCHAR2,
    i_cbr_item            IN  VARCHAR2,
    i_uom                 IN  VARCHAR2,
    i_catlg_num           IN  VARCHAR2,
    i_orig_item           IN  VARCHAR2,
    i_cust_item           IN  VARCHAR2,
    i_qty_mult            IN  VARCHAR2,
    i_max_qty             IN  VARCHAR2,
    i_byp_max_sw          IN  VARCHAR2,
    i_orig_qty            IN  VARCHAR2,
    i_ord_qty             IN  VARCHAR2,
    i_act_qty             IN  VARCHAR2,
    i_alloc_qty           IN  VARCHAR2,
    i_pick_qty            IN  VARCHAR2,
    i_item_pass_area      IN  VARCHAR2,
    i_lbl_catg            IN  VARCHAR2,
    i_mfst_catg           IN  VARCHAR2,
    i_tote_catg           IN  VARCHAR2,
    i_invc_catg           IN  VARCHAR2,
    i_price_amt           IN  VARCHAR2,
    i_rtl_amt             IN  VARCHAR2,
    i_rtl_mult            IN  VARCHAR2,
    i_hard_rtl_sw         IN  VARCHAR2,
    i_hard_price_sw       IN  VARCHAR2,
    i_price_dt            IN  VARCHAR2,
    i_price_tm            IN  VARCHAR2,
    i_rlse_ts             IN  VARCHAR2,
    i_tbill_dist_atch_sw  IN  VARCHAR2
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_excptn_sw            IN  VARCHAR2,
    i_div_part             IN  VARCHAR2,
    i_ord_num              IN  VARCHAR2,
    i_ord_ln               IN  VARCHAR2,
    i_stat_cd              IN  VARCHAR2,
    i_sub_cd               IN  VARCHAR2,
    i_nt_shp_rsn           IN  VARCHAR2,
    i_orig_nt_shp_rsn      IN  VARCHAR2,
    i_auth_cd              IN  VARCHAR2,
    i_cbr_item             IN  VARCHAR2,
    i_uom                  IN  VARCHAR2,
    i_catlg_num            IN  VARCHAR2,
    i_orig_item            IN  VARCHAR2,
    i_cust_item            IN  VARCHAR2,
    i_qty_mult             IN  VARCHAR2,
    i_max_qty              IN  VARCHAR2,
    i_byp_max_sw           IN  VARCHAR2,
    i_orig_qty             IN  VARCHAR2,
    i_ord_qty              IN  VARCHAR2,
    i_act_qty              IN  VARCHAR2,
    i_alloc_qty            IN  VARCHAR2,
    i_pick_qty             IN  VARCHAR2,
    i_item_pass_area       IN  VARCHAR2,
    i_lbl_catg             IN  VARCHAR2,
    i_mfst_catg            IN  VARCHAR2,
    i_tote_catg            IN  VARCHAR2,
    i_invc_catg            IN  VARCHAR2,
    i_price_amt            IN  VARCHAR2,
    i_rtl_amt              IN  VARCHAR2,
    i_rtl_mult             IN  VARCHAR2,
    i_hard_rtl_sw          IN  VARCHAR2,
    i_hard_price_sw        IN  VARCHAR2,
    i_price_dt             IN  VARCHAR2,
    i_price_tm             IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_tbill_dist_atch_sw   IN  VARCHAR2,
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_r_ord_dtl            IN  g_rt_ord_dtl,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N',
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL
  )
    RETURN NUMBER;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE init_sp(
    o_r_ord_dtl  OUT  ordp120b%ROWTYPE
  );

  /**
  ||----------------------------------------------------------------------------
  || Attempt to lock unbilled order line and its sub line and return whether lock was obtained.
  || The lock will be performed with NOWAIT option.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #param i_ord_ln           Order line
  || #param o_lock_sw          Return whether lock was obtained
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE lock_ord_ln_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_ord_ln    IN      NUMBER,
    o_lock_sw   OUT     VARCHAR2
  );

  PROCEDURE ins_sp(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  );

  PROCEDURE ins_sp(
    i_r_ord_dtl  IN  g_rt_ord_dtl
  );

  PROCEDURE ins_sp(
    i_excptn_sw           IN  VARCHAR2,
    i_div_part            IN  VARCHAR2,
    i_ord_num             IN  VARCHAR2,
    i_ord_ln              IN  VARCHAR2,
    i_stat_cd             IN  VARCHAR2,
    i_sub_cd              IN  VARCHAR2,
    i_nt_shp_rsn          IN  VARCHAR2,
    i_orig_nt_shp_rsn     IN  VARCHAR2,
    i_auth_cd             IN  VARCHAR2,
    i_cbr_item            IN  VARCHAR2,
    i_uom                 IN  VARCHAR2,
    i_catlg_num           IN  VARCHAR2,
    i_orig_item           IN  VARCHAR2,
    i_cust_item           IN  VARCHAR2,
    i_qty_mult            IN  VARCHAR2,
    i_max_qty             IN  VARCHAR2,
    i_byp_max_sw          IN  VARCHAR2,
    i_orig_qty            IN  VARCHAR2,
    i_ord_qty             IN  VARCHAR2,
    i_act_qty             IN  VARCHAR2,
    i_alloc_qty           IN  VARCHAR2,
    i_pick_qty            IN  VARCHAR2,
    i_item_pass_area      IN  VARCHAR2,
    i_lbl_catg            IN  VARCHAR2,
    i_mfst_catg           IN  VARCHAR2,
    i_tote_catg           IN  VARCHAR2,
    i_invc_catg           IN  VARCHAR2,
    i_price_amt           IN  VARCHAR2,
    i_rtl_amt             IN  VARCHAR2,
    i_rtl_mult            IN  VARCHAR2,
    i_hard_rtl_sw         IN  VARCHAR2,
    i_hard_price_sw       IN  VARCHAR2,
    i_price_dt            IN  VARCHAR2,
    i_price_tm            IN  VARCHAR2,
    i_rlse_ts             IN  VARCHAR2,
    i_tbill_dist_atch_sw  IN  VARCHAR2
  );

  PROCEDURE upd_sp(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  );

  PROCEDURE upd_sp(
    i_excptn_sw            IN  VARCHAR2,
    i_div_part             IN  VARCHAR2,
    i_ord_num              IN  VARCHAR2,
    i_ord_ln               IN  VARCHAR2,
    i_stat_cd              IN  VARCHAR2,
    i_sub_cd               IN  VARCHAR2,
    i_nt_shp_rsn           IN  VARCHAR2,
    i_orig_nt_shp_rsn      IN  VARCHAR2,
    i_auth_cd              IN  VARCHAR2,
    i_cbr_item             IN  VARCHAR2,
    i_uom                  IN  VARCHAR2,
    i_catlg_num            IN  VARCHAR2,
    i_orig_item            IN  VARCHAR2,
    i_cust_item            IN  VARCHAR2,
    i_qty_mult             IN  VARCHAR2,
    i_max_qty              IN  VARCHAR2,
    i_byp_max_sw           IN  VARCHAR2,
    i_orig_qty             IN  VARCHAR2,
    i_ord_qty              IN  VARCHAR2,
    i_act_qty              IN  VARCHAR2,
    i_alloc_qty            IN  VARCHAR2,
    i_pick_qty             IN  VARCHAR2,
    i_item_pass_area       IN  VARCHAR2,
    i_lbl_catg             IN  VARCHAR2,
    i_mfst_catg            IN  VARCHAR2,
    i_tote_catg            IN  VARCHAR2,
    i_invc_catg            IN  VARCHAR2,
    i_price_amt            IN  VARCHAR2,
    i_rtl_amt              IN  VARCHAR2,
    i_rtl_mult             IN  VARCHAR2,
    i_hard_rtl_sw          IN  VARCHAR2,
    i_hard_price_sw        IN  VARCHAR2,
    i_price_dt             IN  VARCHAR2,
    i_price_tm             IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_tbill_dist_atch_sw   IN  VARCHAR2,
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE upd_sp(
    i_r_ord_dtl            IN  g_rt_ord_dtl,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N',
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL
  );
END op_ord_dtl_pk;
/

CREATE OR REPLACE PACKAGE BODY op_ord_dtl_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_sp(
    i_r_upd                IN  g_rt_ord_dtl,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N',
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL
  ) IS
  BEGIN
    UPDATE ordp120b b
       SET b.statb = get_val_fn(b.statb, i_r_upd.stat_cd, i_use_passed_nulls_sw),
           b.excptn_sw = get_val_fn(b.excptn_sw, i_r_upd.excptn_sw, i_use_passed_nulls_sw),
           b.subrcb = get_val_fn(b.subrcb, i_r_upd.sub_cd, i_use_passed_nulls_sw),
           b.ntshpb = get_val_fn(b.ntshpb, i_r_upd.nt_shp_rsn, i_use_passed_nulls_sw),
           b.zipcdb = get_val_fn(b.zipcdb, i_r_upd.orig_nt_shp_rsn, i_use_passed_nulls_sw),
           b.authb = get_val_fn(b.authb, i_r_upd.auth_cd, i_use_passed_nulls_sw),
           b.itemnb = get_val_fn(b.itemnb, i_r_upd.cbr_item, i_use_passed_nulls_sw),
           b.sllumb = get_val_fn(b.sllumb, i_r_upd.uom, i_use_passed_nulls_sw),
           b.orditb = get_val_fn(b.orditb, i_r_upd.catlg_num, i_use_passed_nulls_sw),
           b.orgitb = get_val_fn(b.orgitb, i_r_upd.orig_item, i_use_passed_nulls_sw),
           b.cusitb = get_val_fn(b.cusitb, i_r_upd.cust_item, i_use_passed_nulls_sw),
           b.qtmulb = get_val_fn(b.qtmulb, i_r_upd.qty_mult, i_use_passed_nulls_sw),
           b.maxqtb = get_val_fn(b.maxqtb, i_r_upd.max_qty, i_use_passed_nulls_sw),
           b.bymaxb = get_val_fn(b.bymaxb, i_r_upd.byp_max_sw, i_use_passed_nulls_sw),
           b.orgqtb = get_val_fn(b.orgqtb, i_r_upd.orig_qty, i_use_passed_nulls_sw),
           b.ordqtb = get_val_fn(b.ordqtb, i_r_upd.ord_qty, i_use_passed_nulls_sw),
           b.actqtb = get_val_fn(b.actqtb, i_r_upd.act_qty, i_use_passed_nulls_sw),
           b.alcqtb = get_val_fn(b.alcqtb, i_r_upd.alloc_qty, i_use_passed_nulls_sw),
           b.pckqtb = get_val_fn(b.pckqtb, i_r_upd.pick_qty, i_use_passed_nulls_sw),
           b.itpasb = get_val_fn(b.itpasb, i_r_upd.item_pass_area, i_use_passed_nulls_sw),
           b.labctb = get_val_fn(b.labctb, i_r_upd.lbl_catg, i_use_passed_nulls_sw),
           b.manctb = get_val_fn(b.manctb, i_r_upd.mfst_catg, i_use_passed_nulls_sw),
           b.totctb = get_val_fn(b.totctb, i_r_upd.tote_catg, i_use_passed_nulls_sw),
           b.invctb = get_val_fn(b.invctb, i_r_upd.invc_catg, i_use_passed_nulls_sw),
           b.hdprcb = get_val_fn(b.hdprcb, i_r_upd.price_amt, i_use_passed_nulls_sw),
           b.hdrtab = get_val_fn(b.hdrtab, i_r_upd.rtl_amt, i_use_passed_nulls_sw),
           b.hdrtmb = get_val_fn(b.hdrtmb, i_r_upd.rtl_mult, i_use_passed_nulls_sw),
           b.rtfixb = get_val_fn(b.rtfixb, i_r_upd.hard_rtl_sw, i_use_passed_nulls_sw),
           b.prfixb = get_val_fn(b.prfixb, i_r_upd.hard_price_sw, i_use_passed_nulls_sw),
           b.prstdb = get_val_fn(b.prstdb, i_r_upd.price_dt, i_use_passed_nulls_sw),
           b.prsttb = get_val_fn(b.prsttb, i_r_upd.price_tm, i_use_passed_nulls_sw),
           b.shpidb = get_val_fn(b.shpidb, i_r_upd.rlse_ts, i_use_passed_nulls_sw),
           b.repckb = get_val_fn(b.repckb, i_r_upd.tbill_dist_atch_sw, i_use_passed_nulls_sw)
     WHERE b.div_part = i_r_upd.div_part
       AND b.ordnob = i_r_upd.ord_num
       AND b.lineb = NVL(i_r_upd.ord_ln, b.lineb)
       AND (   b.statb = NVL(i_ord_status_to_upd, b.statb)
            OR (    i_ord_status_to_upd = op_ord_dtl_pk.g_c_status_unreleased
                AND EXISTS(SELECT 1
                             FROM ordp100a a
                            WHERE a.div_part = b.div_part
                              AND a.ordnoa = b.ordnob
                              AND a.stata IN('O', 'I', 'S', 'C'))
               )
            OR (    i_ord_status_to_upd = op_ord_dtl_pk.g_c_status_released
                AND b.statb NOT IN('O', 'I', 'S', 'C'))
           );
  END upd_ord_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || SUB_LINE_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION sub_line_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  )
    RETURN NUMBER IS
    l_sub_ln  NUMBER;
    l_cv      SYS_REFCURSOR;
  BEGIN
    -- It is possible to have a partial (.10) sub as well as
    -- a conditional sub. The conditional sub line will be returned.
    OPEN l_cv
     FOR
       SELECT MAX(lineb)
         FROM ordp120b
        WHERE div_part = i_div_part
          AND ordnob = i_ord_num
          AND lineb > FLOOR(lineb)
          AND FLOOR(lineb) = i_ord_ln;

    FETCH l_cv
     INTO l_sub_ln;

    RETURN(l_sub_ln);
  END sub_line_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTL_REC_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtl_rec_fn(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  )
    RETURN g_rt_ord_dtl IS
    l_r_ord_dtl  g_rt_ord_dtl;
  BEGIN
    l_r_ord_dtl.excptn_sw := NVL(i_r_ordp120b.excptn_sw, 'N');
    l_r_ord_dtl.div_part := i_r_ordp120b.div_part;
    l_r_ord_dtl.ord_num := i_r_ordp120b.ordnob;
    l_r_ord_dtl.ord_ln := i_r_ordp120b.lineb;
    l_r_ord_dtl.stat_cd := i_r_ordp120b.statb;
    l_r_ord_dtl.sub_cd := i_r_ordp120b.subrcb;
    l_r_ord_dtl.nt_shp_rsn := i_r_ordp120b.ntshpb;
    l_r_ord_dtl.orig_nt_shp_rsn := i_r_ordp120b.zipcdb;
    l_r_ord_dtl.auth_cd := i_r_ordp120b.authb;
    l_r_ord_dtl.cbr_item := i_r_ordp120b.itemnb;
    l_r_ord_dtl.uom := i_r_ordp120b.sllumb;
    l_r_ord_dtl.catlg_num := i_r_ordp120b.orditb;
    l_r_ord_dtl.orig_item := i_r_ordp120b.orgitb;
    l_r_ord_dtl.cust_item := i_r_ordp120b.cusitb;
    l_r_ord_dtl.qty_mult := i_r_ordp120b.qtmulb;
    l_r_ord_dtl.max_qty := i_r_ordp120b.maxqtb;
    l_r_ord_dtl.byp_max_sw := i_r_ordp120b.bymaxb;
    l_r_ord_dtl.orig_qty := i_r_ordp120b.orgqtb;
    l_r_ord_dtl.ord_qty := i_r_ordp120b.ordqtb;
    l_r_ord_dtl.act_qty := i_r_ordp120b.actqtb;
    l_r_ord_dtl.alloc_qty := i_r_ordp120b.alcqtb;
    l_r_ord_dtl.pick_qty := i_r_ordp120b.pckqtb;
    l_r_ord_dtl.item_pass_area := i_r_ordp120b.itpasb;
    l_r_ord_dtl.lbl_catg := i_r_ordp120b.labctb;
    l_r_ord_dtl.mfst_catg := i_r_ordp120b.manctb;
    l_r_ord_dtl.tote_catg := i_r_ordp120b.totctb;
    l_r_ord_dtl.invc_catg := i_r_ordp120b.invctb;
    l_r_ord_dtl.price_amt := i_r_ordp120b.hdprcb;
    l_r_ord_dtl.rtl_amt := i_r_ordp120b.hdrtab;
    l_r_ord_dtl.rtl_mult := i_r_ordp120b.hdrtmb;
    l_r_ord_dtl.hard_rtl_sw := i_r_ordp120b.rtfixb;
    l_r_ord_dtl.hard_price_sw := i_r_ordp120b.prfixb;
    l_r_ord_dtl.price_dt := i_r_ordp120b.prstdb;
    l_r_ord_dtl.price_tm := i_r_ordp120b.prsttb;
    l_r_ord_dtl.rlse_ts := i_r_ordp120b.shpidb;
    l_r_ord_dtl.tbill_dist_atch_sw := i_r_ordp120b.repckb;
    RETURN(l_r_ord_dtl);
  END ord_dtl_rec_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEL_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 05/13/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION sel_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_ord_stat  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN ordp120b%ROWTYPE IS
    l_r_ordp120b  ordp120b%ROWTYPE;
  BEGIN
    CASE NVL(i_ord_stat, 'NULL')
      WHEN 'NULL' THEN
        SELECT *
          INTO l_r_ordp120b
          FROM ordp120b
         WHERE div_part = i_div_part
           AND ordnob = i_ord_num
           AND lineb = i_ord_ln;
      WHEN op_ord_dtl_pk.g_c_status_unreleased THEN
        SELECT b.*
          INTO l_r_ordp120b
          FROM ordp100a a, ordp120b b
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num
           AND a.stata IN('O', 'I', 'S', 'C')
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.lineb = i_ord_ln
           AND b.statb IN('O', 'I', 'S', 'C');
      WHEN op_ord_dtl_pk.g_c_status_released THEN
        SELECT b.*
          INTO l_r_ordp120b
          FROM ordp100a a, ordp120b b
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num
           AND a.stata IN('P', 'R', 'X', 'A')
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.lineb = i_ord_ln
           AND b.statb NOT IN('O', 'I', 'S', 'C');
      ELSE
        SELECT *
          INTO l_r_ordp120b
          FROM ordp120b
         WHERE div_part = i_div_part
           AND ordnob = i_ord_num
           AND lineb = i_ord_ln
           AND statb = i_ord_stat;
    END CASE;

    RETURN(l_r_ordp120b);
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      init_sp(l_r_ordp120b);
      RETURN(l_r_ordp120b);
  END sel_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEL_FOR_UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 05/13/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION sel_for_upd_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER
  )
    RETURN g_tt_ord_dtls IS
    l_t_ord_dtls  g_tt_ord_dtls;
  BEGIN
    SELECT     *
    BULK COLLECT INTO l_t_ord_dtls
          FROM ordp120b b
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.lineb >= FLOOR(i_ord_ln)
           AND b.lineb <= FLOOR(i_ord_ln) + .99
           AND b.statb IN('O', 'I', 'S')
      ORDER BY b.lineb
    FOR UPDATE NOWAIT;

    RETURN(l_t_ord_dtls);
  END sel_for_upd_fn;

  FUNCTION sel_for_upd_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN g_tt_ord_dtls IS
    l_t_ord_dtls  g_tt_ord_dtls;
  BEGIN
    SELECT     *
    BULK COLLECT INTO l_t_ord_dtls
          FROM ordp120b b
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.statb IN('O', 'I', 'S')
      ORDER BY b.lineb
    FOR UPDATE NOWAIT;

    RETURN(l_t_ord_dtls);
  END sel_for_upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  )
    RETURN NUMBER IS
  BEGIN
    ins_sp(i_r_ordp120b);
    RETURN(SQL%ROWCOUNT);
  END ins_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_excptn_sw           IN  VARCHAR2,
    i_div_part            IN  VARCHAR2,
    i_ord_num             IN  VARCHAR2,
    i_ord_ln              IN  VARCHAR2,
    i_stat_cd             IN  VARCHAR2,
    i_sub_cd              IN  VARCHAR2,
    i_nt_shp_rsn          IN  VARCHAR2,
    i_orig_nt_shp_rsn     IN  VARCHAR2,
    i_auth_cd             IN  VARCHAR2,
    i_cbr_item            IN  VARCHAR2,
    i_uom                 IN  VARCHAR2,
    i_catlg_num           IN  VARCHAR2,
    i_orig_item           IN  VARCHAR2,
    i_cust_item           IN  VARCHAR2,
    i_qty_mult            IN  VARCHAR2,
    i_max_qty             IN  VARCHAR2,
    i_byp_max_sw          IN  VARCHAR2,
    i_orig_qty            IN  VARCHAR2,
    i_ord_qty             IN  VARCHAR2,
    i_act_qty             IN  VARCHAR2,
    i_alloc_qty           IN  VARCHAR2,
    i_pick_qty            IN  VARCHAR2,
    i_item_pass_area      IN  VARCHAR2,
    i_lbl_catg            IN  VARCHAR2,
    i_mfst_catg           IN  VARCHAR2,
    i_tote_catg           IN  VARCHAR2,
    i_invc_catg           IN  VARCHAR2,
    i_price_amt           IN  VARCHAR2,
    i_rtl_amt             IN  VARCHAR2,
    i_rtl_mult            IN  VARCHAR2,
    i_hard_rtl_sw         IN  VARCHAR2,
    i_hard_price_sw       IN  VARCHAR2,
    i_price_dt            IN  VARCHAR2,
    i_price_tm            IN  VARCHAR2,
    i_rlse_ts             IN  VARCHAR2,
    i_tbill_dist_atch_sw  IN  VARCHAR2
  )
    RETURN NUMBER IS
  BEGIN
    ins_sp(i_excptn_sw,
           i_div_part,
           i_ord_num,
           i_ord_ln,
           i_stat_cd,
           i_sub_cd,
           i_nt_shp_rsn,
           i_orig_nt_shp_rsn,
           i_auth_cd,
           i_cbr_item,
           i_uom,
           i_catlg_num,
           i_orig_item,
           i_cust_item,
           i_qty_mult,
           i_max_qty,
           i_byp_max_sw,
           i_orig_qty,
           i_ord_qty,
           i_act_qty,
           i_alloc_qty,
           i_pick_qty,
           i_item_pass_area,
           i_lbl_catg,
           i_mfst_catg,
           i_tote_catg,
           i_invc_catg,
           i_price_amt,
           i_rtl_amt,
           i_rtl_mult,
           i_hard_rtl_sw,
           i_hard_price_sw,
           i_price_dt,
           i_price_tm,
           i_rlse_ts,
           i_tbill_dist_atch_sw
          );
    RETURN(SQL%ROWCOUNT);
  END ins_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  )
    RETURN NUMBER IS
  BEGIN
    upd_sp(i_r_ordp120b);
    RETURN(SQL%ROWCOUNT);
  END upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_excptn_sw            IN  VARCHAR2,
    i_div_part             IN  VARCHAR2,
    i_ord_num              IN  VARCHAR2,
    i_ord_ln               IN  VARCHAR2,
    i_stat_cd              IN  VARCHAR2,
    i_sub_cd               IN  VARCHAR2,
    i_nt_shp_rsn           IN  VARCHAR2,
    i_orig_nt_shp_rsn      IN  VARCHAR2,
    i_auth_cd              IN  VARCHAR2,
    i_cbr_item             IN  VARCHAR2,
    i_uom                  IN  VARCHAR2,
    i_catlg_num            IN  VARCHAR2,
    i_orig_item            IN  VARCHAR2,
    i_cust_item            IN  VARCHAR2,
    i_qty_mult             IN  VARCHAR2,
    i_max_qty              IN  VARCHAR2,
    i_byp_max_sw           IN  VARCHAR2,
    i_orig_qty             IN  VARCHAR2,
    i_ord_qty              IN  VARCHAR2,
    i_act_qty              IN  VARCHAR2,
    i_alloc_qty            IN  VARCHAR2,
    i_pick_qty             IN  VARCHAR2,
    i_item_pass_area       IN  VARCHAR2,
    i_lbl_catg             IN  VARCHAR2,
    i_mfst_catg            IN  VARCHAR2,
    i_tote_catg            IN  VARCHAR2,
    i_invc_catg            IN  VARCHAR2,
    i_price_amt            IN  VARCHAR2,
    i_rtl_amt              IN  VARCHAR2,
    i_rtl_mult             IN  VARCHAR2,
    i_hard_rtl_sw          IN  VARCHAR2,
    i_hard_price_sw        IN  VARCHAR2,
    i_price_dt             IN  VARCHAR2,
    i_price_tm             IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_tbill_dist_atch_sw   IN  VARCHAR2,
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER IS
  BEGIN
    upd_sp(i_excptn_sw,
           i_div_part,
           i_ord_num,
           i_ord_ln,
           i_stat_cd,
           i_sub_cd,
           i_nt_shp_rsn,
           i_orig_nt_shp_rsn,
           i_auth_cd,
           i_cbr_item,
           i_uom,
           i_catlg_num,
           i_orig_item,
           i_cust_item,
           i_qty_mult,
           i_max_qty,
           i_byp_max_sw,
           i_orig_qty,
           i_ord_qty,
           i_act_qty,
           i_alloc_qty,
           i_pick_qty,
           i_item_pass_area,
           i_lbl_catg,
           i_mfst_catg,
           i_tote_catg,
           i_invc_catg,
           i_price_amt,
           i_rtl_amt,
           i_rtl_mult,
           i_hard_rtl_sw,
           i_hard_price_sw,
           i_price_dt,
           i_price_tm,
           i_rlse_ts,
           i_tbill_dist_atch_sw,
           i_ord_status_to_upd,
           i_use_passed_nulls_sw
          );
    RETURN(SQL%ROWCOUNT);
  END upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_r_ord_dtl            IN  g_rt_ord_dtl,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N',
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL
  )
    RETURN NUMBER IS
  BEGIN
    upd_sp(i_r_ord_dtl, i_use_passed_nulls_sw, i_ord_status_to_upd);
    RETURN(SQL%ROWCOUNT);
  END upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || INIT_SP
  ||  Return record initialized with column defaults for order detail table
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/10/12 | rhalpai | Original
  || 05/13/13 | rhalpai | Add Div parm. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE init_sp(
    o_r_ord_dtl  OUT  ordp120b%ROWTYPE
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    l_cv := tbl_dflt_fn('ORDP120B');

    FETCH l_cv
     INTO o_r_ord_dtl;

    CLOSE l_cv;
  END init_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOCK_ORD_LN_SP
  ||  Attempt to lock unbilled order line and its sub line and return whether
  ||  lock was obtained.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE lock_ord_ln_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_ord_ln    IN      NUMBER,
    o_lock_sw   OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORD_DTL_PK.LOCK_ORD_LN_SP';
    lar_parm             logs.tar_parm;
    l_t_ord_dtls         g_tt_ord_dtls;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    o_lock_sw := 'N';
    SAVEPOINT b4_lock;
    l_t_ord_dtls := sel_for_upd_fn(i_div_part, i_ord_num, i_ord_ln);

    IF l_t_ord_dtls.COUNT > 0 THEN
      o_lock_sw := 'Y';
    ELSE
      ROLLBACK TO SAVEPOINT b4_lock;
    END IF;   -- l_t_ord_dtls.COUNT > 0
  EXCEPTION
    WHEN excp.gx_row_locked THEN
      logs.warn('RESOURCE_BUSY_NOWAIT occurred', lar_parm);
      ROLLBACK TO SAVEPOINT b4_lock;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END lock_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  ) IS
  BEGIN
    INSERT INTO ordp120b
         VALUES i_r_ordp120b;
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/18/06 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_r_ord_dtl  IN  g_rt_ord_dtl
  ) IS
    l_r_ordp120b  ordp120b%ROWTYPE;
  BEGIN
    init_sp(l_r_ordp120b);
    l_r_ordp120b.excptn_sw := i_r_ord_dtl.excptn_sw;
    l_r_ordp120b.div_part := i_r_ord_dtl.div_part;
    l_r_ordp120b.ordnob := i_r_ord_dtl.ord_num;
    l_r_ordp120b.lineb := i_r_ord_dtl.ord_ln;
    l_r_ordp120b.statb := i_r_ord_dtl.stat_cd;
    l_r_ordp120b.subrcb := i_r_ord_dtl.sub_cd;
    l_r_ordp120b.ntshpb := i_r_ord_dtl.nt_shp_rsn;
    l_r_ordp120b.zipcdb := i_r_ord_dtl.orig_nt_shp_rsn;
    l_r_ordp120b.authb := i_r_ord_dtl.auth_cd;
    l_r_ordp120b.itemnb := i_r_ord_dtl.cbr_item;
    l_r_ordp120b.sllumb := i_r_ord_dtl.uom;
    l_r_ordp120b.orditb := i_r_ord_dtl.catlg_num;
    l_r_ordp120b.orgitb := i_r_ord_dtl.orig_item;
    l_r_ordp120b.cusitb := i_r_ord_dtl.cust_item;
    l_r_ordp120b.qtmulb := i_r_ord_dtl.qty_mult;
    l_r_ordp120b.maxqtb := i_r_ord_dtl.max_qty;
    l_r_ordp120b.bymaxb := i_r_ord_dtl.byp_max_sw;
    l_r_ordp120b.orgqtb := i_r_ord_dtl.orig_qty;
    l_r_ordp120b.ordqtb := i_r_ord_dtl.ord_qty;
    l_r_ordp120b.actqtb := i_r_ord_dtl.act_qty;
    l_r_ordp120b.alcqtb := i_r_ord_dtl.alloc_qty;
    l_r_ordp120b.pckqtb := i_r_ord_dtl.pick_qty;
    l_r_ordp120b.itpasb := i_r_ord_dtl.item_pass_area;
    l_r_ordp120b.labctb := i_r_ord_dtl.lbl_catg;
    l_r_ordp120b.manctb := i_r_ord_dtl.mfst_catg;
    l_r_ordp120b.totctb := i_r_ord_dtl.tote_catg;
    l_r_ordp120b.invctb := i_r_ord_dtl.invc_catg;
    l_r_ordp120b.hdprcb := i_r_ord_dtl.price_amt;
    l_r_ordp120b.hdrtab := i_r_ord_dtl.rtl_amt;
    l_r_ordp120b.hdrtmb := i_r_ord_dtl.rtl_mult;
    l_r_ordp120b.rtfixb := i_r_ord_dtl.hard_rtl_sw;
    l_r_ordp120b.prfixb := i_r_ord_dtl.hard_price_sw;
    l_r_ordp120b.prstdb := i_r_ord_dtl.price_dt;
    l_r_ordp120b.prsttb := i_r_ord_dtl.price_tm;
    l_r_ordp120b.shpidb := i_r_ord_dtl.rlse_ts;
    l_r_ordp120b.repckb := i_r_ord_dtl.tbill_dist_atch_sw;
    ins_sp(l_r_ordp120b);
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_excptn_sw           IN  VARCHAR2,
    i_div_part            IN  VARCHAR2,
    i_ord_num             IN  VARCHAR2,
    i_ord_ln              IN  VARCHAR2,
    i_stat_cd             IN  VARCHAR2,
    i_sub_cd              IN  VARCHAR2,
    i_nt_shp_rsn          IN  VARCHAR2,
    i_orig_nt_shp_rsn     IN  VARCHAR2,
    i_auth_cd             IN  VARCHAR2,
    i_cbr_item            IN  VARCHAR2,
    i_uom                 IN  VARCHAR2,
    i_catlg_num           IN  VARCHAR2,
    i_orig_item           IN  VARCHAR2,
    i_cust_item           IN  VARCHAR2,
    i_qty_mult            IN  VARCHAR2,
    i_max_qty             IN  VARCHAR2,
    i_byp_max_sw          IN  VARCHAR2,
    i_orig_qty            IN  VARCHAR2,
    i_ord_qty             IN  VARCHAR2,
    i_act_qty             IN  VARCHAR2,
    i_alloc_qty           IN  VARCHAR2,
    i_pick_qty            IN  VARCHAR2,
    i_item_pass_area      IN  VARCHAR2,
    i_lbl_catg            IN  VARCHAR2,
    i_mfst_catg           IN  VARCHAR2,
    i_tote_catg           IN  VARCHAR2,
    i_invc_catg           IN  VARCHAR2,
    i_price_amt           IN  VARCHAR2,
    i_rtl_amt             IN  VARCHAR2,
    i_rtl_mult            IN  VARCHAR2,
    i_hard_rtl_sw         IN  VARCHAR2,
    i_hard_price_sw       IN  VARCHAR2,
    i_price_dt            IN  VARCHAR2,
    i_price_tm            IN  VARCHAR2,
    i_rlse_ts             IN  VARCHAR2,
    i_tbill_dist_atch_sw  IN  VARCHAR2
  ) IS
    l_r_ordp120b  ordp120b%ROWTYPE;
  BEGIN
    l_r_ordp120b.excptn_sw := NVL(i_excptn_sw, 'N');
    l_r_ordp120b.div_part := i_div_part;
    l_r_ordp120b.ordnob := i_ord_num;
    l_r_ordp120b.lineb := i_ord_ln;
    l_r_ordp120b.statb := i_stat_cd;
    l_r_ordp120b.subrcb := i_sub_cd;
    l_r_ordp120b.ntshpb := i_nt_shp_rsn;
    l_r_ordp120b.zipcdb := i_orig_nt_shp_rsn;
    l_r_ordp120b.authb := i_auth_cd;
    l_r_ordp120b.itemnb := i_cbr_item;
    l_r_ordp120b.sllumb := i_uom;
    l_r_ordp120b.orditb := i_catlg_num;
    l_r_ordp120b.orgitb := i_orig_item;
    l_r_ordp120b.cusitb := i_cust_item;
    l_r_ordp120b.qtmulb := i_qty_mult;
    l_r_ordp120b.maxqtb := i_max_qty;
    l_r_ordp120b.bymaxb := i_byp_max_sw;
    l_r_ordp120b.orgqtb := i_orig_qty;
    l_r_ordp120b.ordqtb := i_ord_qty;
    l_r_ordp120b.actqtb := i_act_qty;
    l_r_ordp120b.alcqtb := i_alloc_qty;
    l_r_ordp120b.pckqtb := i_pick_qty;
    l_r_ordp120b.itpasb := i_item_pass_area;
    l_r_ordp120b.labctb := i_lbl_catg;
    l_r_ordp120b.manctb := i_mfst_catg;
    l_r_ordp120b.totctb := i_tote_catg;
    l_r_ordp120b.invctb := i_invc_catg;
    l_r_ordp120b.hdprcb := i_price_amt;
    l_r_ordp120b.hdrtab := i_rtl_amt;
    l_r_ordp120b.hdrtmb := i_rtl_mult;
    l_r_ordp120b.rtfixb := i_hard_rtl_sw;
    l_r_ordp120b.prfixb := i_hard_price_sw;
    l_r_ordp120b.prstdb := i_price_dt;
    l_r_ordp120b.prsttb := i_price_tm;
    l_r_ordp120b.shpidb := i_rlse_ts;
    l_r_ordp120b.repckb := i_tbill_dist_atch_sw;
    ins_sp(l_r_ordp120b);
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_r_ordp120b  IN  ordp120b%ROWTYPE
  ) IS
  BEGIN
    UPDATE ordp120b
       SET ROW = i_r_ordp120b
     WHERE div_part = i_r_ordp120b.div_part
       AND ordnob = i_r_ordp120b.ordnob
       AND lineb = i_r_ordp120b.lineb;
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 06/08/07 | rhalpai | Changed to call UPD_SP passing record.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_excptn_sw            IN  VARCHAR2,
    i_div_part             IN  VARCHAR2,
    i_ord_num              IN  VARCHAR2,
    i_ord_ln               IN  VARCHAR2,
    i_stat_cd              IN  VARCHAR2,
    i_sub_cd               IN  VARCHAR2,
    i_nt_shp_rsn           IN  VARCHAR2,
    i_orig_nt_shp_rsn      IN  VARCHAR2,
    i_auth_cd              IN  VARCHAR2,
    i_cbr_item             IN  VARCHAR2,
    i_uom                  IN  VARCHAR2,
    i_catlg_num            IN  VARCHAR2,
    i_orig_item            IN  VARCHAR2,
    i_cust_item            IN  VARCHAR2,
    i_qty_mult             IN  VARCHAR2,
    i_max_qty              IN  VARCHAR2,
    i_byp_max_sw           IN  VARCHAR2,
    i_orig_qty             IN  VARCHAR2,
    i_ord_qty              IN  VARCHAR2,
    i_act_qty              IN  VARCHAR2,
    i_alloc_qty            IN  VARCHAR2,
    i_pick_qty             IN  VARCHAR2,
    i_item_pass_area       IN  VARCHAR2,
    i_lbl_catg             IN  VARCHAR2,
    i_mfst_catg            IN  VARCHAR2,
    i_tote_catg            IN  VARCHAR2,
    i_invc_catg            IN  VARCHAR2,
    i_price_amt            IN  VARCHAR2,
    i_rtl_amt              IN  VARCHAR2,
    i_rtl_mult             IN  VARCHAR2,
    i_hard_rtl_sw          IN  VARCHAR2,
    i_hard_price_sw        IN  VARCHAR2,
    i_price_dt             IN  VARCHAR2,
    i_price_tm             IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_tbill_dist_atch_sw   IN  VARCHAR2,
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_r_ord_dtl  g_rt_ord_dtl;
  BEGIN
    l_r_ord_dtl.excptn_sw := i_excptn_sw;
    l_r_ord_dtl.div_part := i_div_part;
    l_r_ord_dtl.ord_num := i_ord_num;
    l_r_ord_dtl.ord_ln := i_ord_ln;
    l_r_ord_dtl.stat_cd := i_stat_cd;
    l_r_ord_dtl.sub_cd := i_sub_cd;
    l_r_ord_dtl.nt_shp_rsn := i_nt_shp_rsn;
    l_r_ord_dtl.orig_nt_shp_rsn := i_orig_nt_shp_rsn;
    l_r_ord_dtl.auth_cd := i_auth_cd;
    l_r_ord_dtl.cbr_item := i_cbr_item;
    l_r_ord_dtl.uom := i_uom;
    l_r_ord_dtl.catlg_num := i_catlg_num;
    l_r_ord_dtl.orig_item := i_orig_item;
    l_r_ord_dtl.cust_item := i_cust_item;
    l_r_ord_dtl.qty_mult := i_qty_mult;
    l_r_ord_dtl.max_qty := i_max_qty;
    l_r_ord_dtl.byp_max_sw := i_byp_max_sw;
    l_r_ord_dtl.orig_qty := i_orig_qty;
    l_r_ord_dtl.ord_qty := i_ord_qty;
    l_r_ord_dtl.act_qty := i_act_qty;
    l_r_ord_dtl.alloc_qty := i_alloc_qty;
    l_r_ord_dtl.pick_qty := i_pick_qty;
    l_r_ord_dtl.item_pass_area := i_item_pass_area;
    l_r_ord_dtl.lbl_catg := i_lbl_catg;
    l_r_ord_dtl.mfst_catg := i_mfst_catg;
    l_r_ord_dtl.tote_catg := i_tote_catg;
    l_r_ord_dtl.invc_catg := i_invc_catg;
    l_r_ord_dtl.price_amt := i_price_amt;
    l_r_ord_dtl.rtl_amt := i_rtl_amt;
    l_r_ord_dtl.rtl_mult := i_rtl_mult;
    l_r_ord_dtl.hard_rtl_sw := i_hard_rtl_sw;
    l_r_ord_dtl.hard_price_sw := i_hard_price_sw;
    l_r_ord_dtl.price_dt := i_price_dt;
    l_r_ord_dtl.price_tm := i_price_tm;
    l_r_ord_dtl.rlse_ts := i_rlse_ts;
    l_r_ord_dtl.tbill_dist_atch_sw := i_tbill_dist_atch_sw;
    upd_sp(l_r_ord_dtl, i_ord_status_to_upd, i_use_passed_nulls_sw);
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 06/08/07 | rhalpai | Changed to call new UPD_ORD_SP and UPD_WHS_SP and
  ||                    | handle updating exception well when update not found
  ||                    | in good well when excptn_sw is not Y or N.
  || 01/20/12 | rhalpai | Convert to call UPD_ORD_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_r_ord_dtl            IN  g_rt_ord_dtl,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N',
    i_ord_status_to_upd    IN  VARCHAR2 DEFAULT NULL
  ) IS
  BEGIN
    upd_ord_sp(i_r_ord_dtl, i_use_passed_nulls_sw, i_ord_status_to_upd);
  END upd_sp;
END op_ord_dtl_pk;
/

