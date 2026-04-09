CREATE OR REPLACE PACKAGE op_types_pk IS
  /*
  ||---------------------------------------------------------------------------
  ||  Pre-defined types.
  ||---------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||---------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||---------------------------------------------------------------------------
  || 06/26/04 | rhalpai | Original
  || 12/02/05 | rhalpai | Changed rt_tagged_ords_parms: replaced (spread_alloc,
  ||                    | avrg_cust_qty, status) with (qty_avail, is_ration,
  ||                    | tot_ord_qty, cust_cnt, ord_stat) PIR1289
  ||                    | Changed rt_tagged_ords: removed (shp_cmplt, spread_qty)
  ||                    | added (adj_ord_qty) PIR1289
  ||                    | Added tt_tagged_ords_v PIR1289
  ||                    | Removed spread_alloc from rt_alloc PIR1289
  ||                    | Added tt_varchar_v, tt_num_v, rt_kit, tt_kit,
  ||                    | rt_kit_comp, tt_kit_comp, tt_kit_comp_qty_i,
  ||                    | tt_kit_seq_i, rt_kit_ord, tt_kit_ord, t_kit_ord_cur
  ||                    | PIR2909
  ||---------------------------------------------------------------------------
  */
  TYPE tt_varchars_i IS TABLE OF VARCHAR2(200)
    INDEX BY PLS_INTEGER;

  TYPE tt_varchars_v IS TABLE OF VARCHAR2(200)
    INDEX BY VARCHAR2(200);

  TYPE tt_nums_i IS TABLE OF NUMBER
    INDEX BY PLS_INTEGER;

  TYPE tt_nums_v IS TABLE OF NUMBER
    INDEX BY VARCHAR2(200);

  TYPE rt_alloc_fcst IS RECORD(
    div             VARCHAR2(2),
    cust_tax_juris  mclp030c.taxjrc%TYPE,
    cust_tax_city   mclp030c.tax_city_cd%TYPE,
    cust_tax_cnty   mclp030c.tax_cnty_cd%TYPE,
    mcl_item        sawp505e.catite%TYPE,
    ord_qty         PLS_INTEGER
  );
END op_types_pk;
/

