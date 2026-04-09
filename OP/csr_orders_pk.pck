CREATE OR REPLACE PACKAGE csr_orders_pk IS
  /**
  ||----------------------------------------------------------------------------
  || Package with functionality for inspecting orders for the CSR Workbench
  || Application.
  ||----------------------------------------------------------------------------
  */

--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_msg_hdr IS RECORD(
    ord_num         NUMBER,
    div_part        NUMBER,
    div             div_mstr_di1d.div_id%TYPE,
    cust_id         sysp200c.acnoc%TYPE,
    mcl_cust        mclp020b.mccusb%TYPE,
    load_typ        ordp100a.ldtypa%TYPE,
    ord_typ         ordp100a.dsorda%TYPE,
    ord_src         ordp100a.ipdtsa%TYPE,
    conf_num        ordp100a.connba%TYPE,
    ser_num         ordp100a.telsla%TYPE,
    trnsmt_ts       DATE,
    cust_pass_area  ordp100a.cspasa%TYPE,
    hdr_excptn_cd   ordp100a.hdexpa%TYPE,
    legcy_ref       ordp100a.legrfa%TYPE,
    po_num          ordp100a.cpoa%TYPE,
    allw_partl_sw   CHAR(1),
    shp_dt          DATE,
    ord_ln_cnt      NUMBER
  );

  TYPE g_cvt_msg_hdr IS REF CURSOR
    RETURN g_rt_msg_hdr;

  TYPE g_rt_msg_dtl IS RECORD(
    catlg_num       NUMBER,
    cbr_item        sawp505e.iteme%TYPE,
    uom             sawp505e.uome%TYPE,
    ord_qty         NUMBER,
    cust_item       ordp120b.cusitb%TYPE,
    item_pass_area  ordp120b.itpasb%TYPE,
    hard_rtl_sw     CHAR(1),
    rtl_amt         NUMBER,
    rtl_mult        NUMBER,
    hard_price_sw   CHAR(1),
    price_amt       NUMBER,
    orig_qty        NUMBER,
    byp_max_sw      CHAR(1),
    max_qty         NUMBER,
    qty_mult        NUMBER,
    ord_ln          NUMBER
  );

  TYPE g_tt_msg_dtls IS TABLE OF g_rt_msg_dtl;

  TYPE g_cvt_msg_dtl IS REF CURSOR
    RETURN g_rt_msg_dtl;

  TYPE g_tt_msgs IS TABLE OF mclane_mq_put.mq_msg_data%TYPE;

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_csr         CONSTANT VARCHAR2(1) := 'C';
  g_c_lost_load   CONSTANT VARCHAR2(1) := 'L';
  g_c_copy_order  CONSTANT VARCHAR2(1) := 'K';
  g_c_cancel      CONSTANT VARCHAR2(3) := 'CAN';
  g_c_suspend     CONSTANT VARCHAR2(3) := 'SUS';
  g_c_complete    CONSTANT VARCHAR2(3) := 'CMP';

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION order_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION order_detail_status_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  VARCHAR2,
    i_conf_num  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Used to search order info.
  || #param i_ord_stat                Order line status type:
  ||                                  Valid values are:
  ||                                  {*} 'UNBILLED'
  ||                                  {*} 'BILLED'
  ||                                  {*} 'ALL'
  || #param i_ord_typ                 Order type
  || #param i_store_num               Customer store number (Leading chars allowed).
  || #param i_crp_cd                  Corp code
  || #param i_conf_num                Order confirmation number (Leading chars allowed)
  || #param i_ord_num                 Order number (Leading chars allowed)
  || #param i_cust_id                 CBR customer number (Leading chars allowed)
  || #param i_mcl_cust                McLane catalog customer number (Leading chars allowed)
  || #param i_ser_num                 Telxon serial number (Leading chars allowed)
  || #param i_po_num                  Purchase order number (Leading chars allowed)
  || #param i_llr_dt_from             Load Label Release date range in YYYYMMDD format (From)
  || #param i_llr_dt_to               Load Label Release date range in YYYYMMDD format (To)
  || #param i_item                    Item number (Leading chars allowed)
  || #param i_item_typ                Type of passed item number
  ||                                  Valid values are:
  ||                                  {*} 'CBRITEM'
  ||                                  {*} 'MCLANEITEM'
  ||                                  {*} 'CUSTITEM'
  || #param i_incl_hist_sw            Include order history well in search? ('Y' or 'N')
  || #param i_div                     Division code
  || #param i_exact_ord_match_sw      Exact match on order num? ('Y' or 'N')
  || #param i_exact_conf_num_match_sw Exact match on confirmation num? ('Y' or 'N')
  || #param i_split_typ               Split type
  || #return                          Cursor of searched order header info
  ||----------------------------------------------------------------------------
  **/
  FUNCTION search_order_fn(
    i_ord_stat                 IN  VARCHAR2,
    i_ord_typ                  IN  VARCHAR2,
    i_store_num                IN  VARCHAR2,
    i_crp_cd                   IN  NUMBER,
    i_conf_num                 IN  VARCHAR2,
    i_ord_num                  IN  VARCHAR2,
    i_cust_id                  IN  VARCHAR2,
    i_mcl_cust                 IN  VARCHAR2,
    i_ser_num                  IN  VARCHAR2,
    i_po_num                   IN  VARCHAR2,
    i_llr_dt_from              IN  VARCHAR2,
    i_llr_dt_to                IN  VARCHAR2,
    i_item                     IN  VARCHAR2,
    i_item_typ                 IN  VARCHAR2,
    i_incl_hist_sw             IN  VARCHAR2,
    i_div                      IN  VARCHAR2,
    i_exact_ord_match_sw       IN  VARCHAR2,
    i_exact_conf_num_match_sw  IN  VARCHAR2,
    i_split_typ                IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Retrieve a cursor of the order detail lines from history for an order.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #return                   Cursor of order detail
  ||----------------------------------------------------------------------------
  **/
  FUNCTION hist_detail_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Retrieve a cursor of the order detail lines from history for an order.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #param i_maint_mode_sw    Maintenance mode? 'Y'/'N'
  || #param i_excptns_only_sw  Exceptions only? 'Y'/'N'
  || #param i_conf_num         Order confirmation number
  || #return                   Cursor of order detail
  ||----------------------------------------------------------------------------
  **/
  FUNCTION ord_detail_fn(
    i_div              IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_maint_mode_sw    IN  VARCHAR2,
    i_excptns_only_sw  IN  VARCHAR2 DEFAULT 'N',
    i_conf_num         IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Indicates whether an order is in History with a shipped status.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #return                   'Y' when found
  ||----------------------------------------------------------------------------
  **/
  FUNCTION in_hist_as_shipped_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Indicates whether an order is in History with a header-level exception.
  || The whole order was in the exception well.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #return                   'Y' when found
  ||----------------------------------------------------------------------------
  **/
  FUNCTION in_hist_with_hdr_exception_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Retrieve a delimited list of header exception reason descriptions.
  || It is possible to have 2 level-one excpetions when a customer has been
  || placed "on hold" and the customer's load/stop assigment has been removed.
  || i.e.: 1 Customer on Hold~1 No Stop Information Available
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #param i_use_hist_sw      Use history table? 'Y'/'N'
  || #return                   Header exception reason description
  ||----------------------------------------------------------------------------
  **/
  FUNCTION hdr_except_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_use_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Retrieve a cursor of exception order headers.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #param i_conf_num         Order confirmation number
  || #param i_maint_mode_sw    Maintenance mode? 'Y'/'N'
  || #param i_user_id          Maintenance UserID (used in conjunction with
  ||                           maintenance mode)
  || #return                   Cursor of order headers
  ||----------------------------------------------------------------------------
  **/
  FUNCTION except_order_hdr_list_fn(
    i_div            IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_conf_num       IN  VARCHAR2,
    i_maint_mode_sw  IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Retrieve a cursor of history order headers.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #return                   Cursor of order headers
  ||----------------------------------------------------------------------------
  **/
  FUNCTION hist_order_hdr_list_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Retrieve a cursor of order headers.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #param i_maint_mode_sw    Maintenance mode? 'Y'/'N'
  || #param i_user_id          Maintenance UserID (used in conjunction with
  ||                           maintenance mode)
  || #return                   Cursor of order headers
  ||----------------------------------------------------------------------------
  **/
  FUNCTION order_hdr_list_fn(
    i_div            IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_maint_mode_sw  IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Indicate whether an order contains order lines in billed status.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #return                   'Y' when billed status
  ||----------------------------------------------------------------------------
  **/
  FUNCTION is_billed_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Reason code list for reason type.
  || #param i_rsn_typ          Reason type
  || #return                   Cursor of reason codes
  ||----------------------------------------------------------------------------
  **/
  FUNCTION reason_cd_list_fn(
    i_rsn_typ  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Get cursor of audit information for an order line.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #param i_ord_ln           Order line number
  || #return                   Return cursor of audit info
  ||----------------------------------------------------------------------------
  **/
  FUNCTION audit_info_cur_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Get cursor of CSR restrictions for corp codes.
  || #param i_div              Division code
  || #return                   Cursor of CSR restrictions for corp codes
  ||----------------------------------------------------------------------------
  **/
  FUNCTION csr_restrictions_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  /**
  ||----------------------------------------------------------------------------
  || Returns number of entries in delimited list
  || #param i_list             Delimited list of strings
  || #param i_delimiter        Delimiter
  || #return                   Number of entries in delimited list
  ||----------------------------------------------------------------------------
  **/
  FUNCTION list_cnt_fn(
    i_list       IN  VARCHAR2,
    i_delimiter  IN  VARCHAR2 DEFAULT ','
  )
    RETURN PLS_INTEGER;

  /**
  ||----------------------------------------------------------------------------
  || Generate and return the next sequence for order confirmation number.
  || #return                   Next sequence for order confirmation number
  ||----------------------------------------------------------------------------
  **/
  FUNCTION next_conf_num_fn
    RETURN NUMBER;

  FUNCTION next_conf_num_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Return order comment.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #param i_hist_sw          Is from history well? Y/N
  || #return                   Order comment.
  ||----------------------------------------------------------------------------
  **/
  FUNCTION ord_comment_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_hist_sw   IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN VARCHAR2;

  FUNCTION test_fn(
    i_name       IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  /**
  ||----------------------------------------------------------------------------
  || Process updates needed to resend order to mainframe.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #param i_t_ord_lns        Table of order lines (NULL for all order lines)
  || #param i_commit_sw        Commit when done? Y/N
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE upd_for_resend_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_t_ord_lns  IN  type_ntab DEFAULT NULL,
    i_commit_sw  IN  VARCHAR2 DEFAULT 'N'
  );

  /**
  ||----------------------------------------------------------------------------
  || Set maintenance user for order.
  || #param i_div              Division code
  || #param i_user_id          Maintenance UserID (used in conjunction with maintenance mode)
  || #param i_ord_num          Order number
  || #param i_conf_num         Confirmation number (to reference new order)
  || #param i_commit_sw        Commit when done? Y/N
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE upd_maint_user_sp(
    i_div        IN  VARCHAR2,
    i_user_id    IN  VARCHAR2,
    i_ord_num    IN  NUMBER,
    i_conf_num   IN  VARCHAR2 DEFAULT NULL,
    i_commit_sw  IN  VARCHAR2 DEFAULT 'N'
  );

  /**
  ||----------------------------------------------------------------------------
  || Send order to mainframe.
  || #param i_div              Division code
  || #param i_ord_num          Order number
  || #param i_ord_ln_list      Order line number delimited ('~') list
  || #param i_conf_num         Confirmation number (to reference new order)
  || #param i_ord_comnt        Order Comment
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE send_to_mainframe_sp(
    i_div          IN  VARCHAR2,
    i_ord_num      IN  NUMBER,
    i_ord_ln_list  IN  VARCHAR2 DEFAULT NULL,
    i_conf_num     IN  VARCHAR2 DEFAULT NULL,
    i_ord_comnt    IN  VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Save Order.
  || Order may be resent to mainframe for reprocessing.
  || #param i_div              Two Character Division Code
  || #param i_ord_num          Order number
  || #param i_conf_num         Confirmation number (to reference new order)
  || #param i_test_sw          Test order flag
  || #param i_no_ord_sw        No order code
  || #param i_po_num           Purchase order number
  || #param i_cancel_ln_parm_list Delimited parameter list for cancelled lines
  ||                           'ord_ln~rsn_cd~auth_by`ord_ln~rsn_cd~auth_by'
  || #param i_new_dtl_parm_list Delimited parameter list for new lines
  ||                           'ord_ln~mcl_item~ord_qty~rsn_cd~auth_by`ord_ln~mcl_item~ord_qty~rsn_cd~auth_by'
  || #param i_upd_dtl_parm_list Delimited parameter list for updated lines
  ||                           'ord_ln~byp_max~ord_qty~rsn_cd~auth_by`ord_ln~byp_max~ord_qty~rsn_cd~auth_by'
  || #param i_user_id          Maintenance UserID (used in conjunction with maintenance mode)
  || #param i_add_on_ord_sw    Indicates existence of add-on order (ADC via CSR, ADK via keybatch)
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE save_ord_sp(
    i_div                  IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_conf_num             IN  VARCHAR2,
    i_test_sw              IN  VARCHAR2,
    i_no_ord_sw            IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_mcl_cust             IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_user_id              IN  VARCHAR2,
    i_add_on_ord_sw        IN  VARCHAR2 DEFAULT NULL
  );

  PROCEDURE validate_sp(
    i_div                  IN      VARCHAR2,
    i_actn_cd              IN      VARCHAR2,
    i_ord_num              IN      NUMBER,
    i_conf_num             IN      VARCHAR2,
    i_mcl_cust             IN      VARCHAR2,
    i_po_num               IN      VARCHAR2,
    i_cancel_ln_parm_list  IN      VARCHAR2,
    i_new_dtl_parm_list    IN      VARCHAR2,
    i_upd_dtl_parm_list    IN      VARCHAR2,
    i_resend_ln_list       IN      VARCHAR2,
    o_msg                  OUT     VARCHAR2
  );

  PROCEDURE cancel_ord_sp(
    i_div       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_conf_num  IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  );

  PROCEDURE suspend_ord_sp(
    i_div                  IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_conf_num             IN  VARCHAR2,
    i_test_sw              IN  VARCHAR2,
    i_no_ord_sw            IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_mcl_cust             IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_rsn_cd               IN  VARCHAR2,
    i_user_id              IN  VARCHAR2
  );

  PROCEDURE complete_ord_sp(
    i_div                  IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_conf_num             IN  VARCHAR2,
    i_test_sw              IN  VARCHAR2,
    i_no_ord_sw            IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_mcl_cust             IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_resend_ln_list       IN  VARCHAR2,
    i_user_id              IN  VARCHAR2,
    i_add_on_ord_sw        IN  VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Insert order header and details and route to mainframe.
  || #param i_r_ord_hdr        Record of order header info
  || #param i_t_ord_dtls       Table of order detail info
  || #param i_user_id          Maintenance UserID (used in conjunction with maintenance mode)
  || #param i_ord_comnt        Order Comment
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE ins_ord_sp(
    i_r_ord_hdr   IN  csr_orders_pk.g_rt_msg_hdr,
    i_t_ord_dtls  IN  csr_orders_pk.g_tt_msg_dtls,
    i_user_id     IN  VARCHAR2,
    i_ord_comnt   IN  VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Insert order header and details and route to mainframe.
  || #param i_div              Division code
  || #param i_conf_num         Confirmation number (to reference new order)
  || #param i_test_sw          Test order flag
  || #param i_mcl_cust         McLane catalog customer number
  || #param i_cust_id          CBR customer number
  || #param i_po_num           Purchase order number
  || #param i_load_typ         Load type
  || #param i_ord_src          Order input data source
  || #param i_shp_dt           Ship date
  || #param i_user_id          Maintenance UserID (used in conjunction with maintenance mode)
  || #param i_dtl_list         Delimited order detail list
  ||                           group-delimiter : '`', field-delimiter : '~'
  ||                           'mcl_item~cbr_item~uom~ord_qty`mcl_item~cbr_item~uom~ord_qty'
  || #param i_ord_comnt        Order Comment
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE ins_ord_sp(
    i_div        IN  VARCHAR2,
    i_conf_num   IN  VARCHAR2,
    i_test_sw    IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_cust_id    IN  VARCHAR2,
    i_po_num     IN  VARCHAR2,
    i_load_typ   IN  VARCHAR2,
    i_ord_typ    IN  VARCHAR2,
    i_ord_src    IN  VARCHAR2,
    i_shp_dt     IN  DATE,
    i_user_id    IN  VARCHAR2,
    i_dtl_list   IN  VARCHAR2,
    i_ord_comnt  IN  VARCHAR2 DEFAULT NULL
  );
END csr_orders_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_orders_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_cancel_ln IS RECORD(
    ord_ln   NUMBER,
    rsn_cd   sysp296a.rsncda%TYPE,
    auth_by  sysp296a.autbya%TYPE
  );

  TYPE g_tt_cancel_lns IS TABLE OF g_rt_cancel_ln;

  TYPE g_rt_ins_ln IS RECORD(
    ord_ln     NUMBER,
    catlg_num  sawp505e.catite%TYPE,
    ord_qty    PLS_INTEGER,
    rsn_cd     sysp296a.rsncda%TYPE,
    auth_by    sysp296a.autbya%TYPE
  );

  TYPE g_tt_ins_lns IS TABLE OF g_rt_ins_ln;

  TYPE g_rt_upd_ln IS RECORD(
    ord_ln      NUMBER,
    byp_max_sw  CHAR(1),
    ord_qty     PLS_INTEGER,
    rsn_cd      sysp296a.rsncda%TYPE,
    auth_by     sysp296a.autbya%TYPE
  );

  TYPE g_tt_upd_lns IS TABLE OF g_rt_upd_ln;

  g_c_default_rendate  CONSTANT NUMBER    := -58;

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ORD_NUM_FOR_CONF_NUM_FN
  ||  Return order number for confirmation number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/21/11 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_num_for_conf_num_fn(
    i_div_part  IN  NUMBER,
    i_conf_num  IN  VARCHAR2
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.ORD_NUM_FOR_CONF_NUM_FN';
    lar_parm             logs.tar_parm;
    l_ord_num            NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.dbg('ENTRY', lar_parm);

    BEGIN
      SELECT a.ordnoa
        INTO l_ord_num
        FROM ordp100a a
       WHERE a.div_part = i_div_part
         AND a.connba = i_conf_num;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN TOO_MANY_ROWS THEN
        NULL;
    END;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_ord_num);
  END ord_num_for_conf_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_NEW_ORD_FN
  ||  Indicate whether order was newly created.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/23/11 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_new_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.IS_NEW_ORD_FN';
    lar_parm             logs.tar_parm;
    l_new_sw             VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);

    SELECT MAX('Y')
      INTO l_new_sw
      FROM ordp100a a
     WHERE a.ordnoa = i_ord_num
       AND a.div_part = i_div_part
       AND a.stata = 'I'
       AND a.load_depart_sid = 0;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_new_sw = 'Y');
  END is_new_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || GENERATE_MQ_PUT_CORR_ID_FN
  ||  Generate and return the next correlation id for the mq_put process.
  ||
  ||  This function is called by OrderManagerDS.generateCorrID (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION generate_mq_put_corr_id_fn
    RETURN NUMBER IS
    l_corr_id  NUMBER;
  BEGIN
    SELECT mq_put_corr_put_id_seq.NEXTVAL
      INTO l_corr_id
      FROM DUAL;

    RETURN(l_corr_id);
  END generate_mq_put_corr_id_fn;

  /*
  ||----------------------------------------------------------------------------
  || NEXT_ORD_NUM_FN
  ||  Generate and return the next sequence for order number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/22/11 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION next_ord_num_fn
    RETURN NUMBER IS
    l_ord_num  PLS_INTEGER;
  BEGIN
    SELECT ordp100a_ordnoa_seq.NEXTVAL
      INTO l_ord_num
      FROM DUAL;

    RETURN(l_ord_num);
  END next_ord_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_HDR_MSG_FN
  ||  Return order header MQ data msg.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 01/03/08 | rhalpai | Changed to clear header exception, HDEXPA. IM366840
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_msg_fn(
    i_div_part    IN  NUMBER,
    i_create_typ  IN  VARCHAR2,
    i_r_ord_hdr   IN  csr_orders_pk.g_rt_msg_hdr,
    i_ord_comnt   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                    := 'CSR_ORDERS_PK.ORD_HDR_MSG_FN';
    lar_parm             logs.tar_parm;
    l_ord_comnt          ordp140c.commc%TYPE;
    l_chg_cd             CHAR(1);
    l_mq_msg_data        mclane_mq_put.mq_msg_data%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CreateTyp', i_create_typ);
    logs.add_parm(lar_parm, 'OrdNum', i_r_ord_hdr.ord_num);
    logs.add_parm(lar_parm, 'Div', i_r_ord_hdr.div);
    logs.add_parm(lar_parm, 'CustId', i_r_ord_hdr.cust_id);
    logs.add_parm(lar_parm, 'MclCust', i_r_ord_hdr.mcl_cust);
    logs.add_parm(lar_parm, 'LoadTyp', i_r_ord_hdr.load_typ);
    logs.add_parm(lar_parm, 'OrdTyp', i_r_ord_hdr.ord_typ);
    logs.add_parm(lar_parm, 'OrdSrc', i_r_ord_hdr.ord_src);
    logs.add_parm(lar_parm, 'ConfNum', i_r_ord_hdr.conf_num);
    logs.add_parm(lar_parm, 'SerNum', i_r_ord_hdr.ser_num);
    logs.add_parm(lar_parm, 'TrnsmtTs', i_r_ord_hdr.trnsmt_ts);
    logs.add_parm(lar_parm, 'CustPassArea', i_r_ord_hdr.cust_pass_area);
    logs.add_parm(lar_parm, 'HdrExcptnCd', i_r_ord_hdr.hdr_excptn_cd);
    logs.add_parm(lar_parm, 'LegcyRef', i_r_ord_hdr.legcy_ref);
    logs.add_parm(lar_parm, 'PoNum', i_r_ord_hdr.po_num);
    logs.add_parm(lar_parm, 'AllwPartlSw', i_r_ord_hdr.allw_partl_sw);
    logs.add_parm(lar_parm, 'ShpDt', i_r_ord_hdr.shp_dt);
    logs.add_parm(lar_parm, 'OrdLnCnt', i_r_ord_hdr.ord_ln_cnt);
    logs.add_parm(lar_parm, 'OrdComnt', i_ord_comnt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_ord_comnt := i_ord_comnt;

    IF (    i_ord_comnt IS NULL
        AND i_r_ord_hdr.ord_num IS NOT NULL) THEN
      logs.dbg('Get Order Comment');
      l_ord_comnt := ord_comment_fn(i_div_part, i_r_ord_hdr.ord_num);
    END IF;   -- i_ord_comnt IS NULL AND i_r_ord_hdr.ord_num IS NOT NULL

    logs.dbg('Check for Changed OrdSrc');

    UPDATE sysp296a sa
       SET sa.actna = 'M'
     WHERE sa.actna IS NULL
       AND sa.div_part = i_div_part
       AND sa.ordnoa = i_r_ord_hdr.ord_num
       AND sa.linea = 0
       AND sa.tblnma = 'ORDP100A'
       AND sa.fldnma = 'IPDTSA'
       AND sa.florga IN('ADC', 'ADK');

    l_chg_cd :=(CASE
                  WHEN SQL%ROWCOUNT > 0 THEN 'S'
                  ELSE ' '
                END);
    logs.dbg('Create Order Header MQ Msg');
    l_mq_msg_data := rpad_fn(i_r_ord_hdr.div, 2)
                     || 'QOCSR01 '
                     || rpad_fn(CASE i_create_typ
                                  WHEN csr_orders_pk.g_c_csr THEN 'CSRWORKBENCH'
                                  WHEN csr_orders_pk.g_c_lost_load THEN 'LOST LOAD CREATE'
                                  WHEN csr_orders_pk.g_c_copy_order THEN 'COPY ORDER CREATE'
                                END,
                                30
                               )
                     || RPAD('ADD', 13)
                     || 'H:'
                     || rpad_fn(i_r_ord_hdr.legcy_ref, 25)
                     || rpad_fn(i_r_ord_hdr.load_typ, 3)
                     ||(CASE i_r_ord_hdr.ord_typ
                          WHEN 'D' THEN 'DIS'
                          WHEN 'N' THEN 'NOO'
                          WHEN 'T' THEN 'TST'
                          ELSE 'REG'
                        END)
                     || TO_CHAR(i_r_ord_hdr.trnsmt_ts, 'YYYYMMDDHH24MISS')
                     || TO_CHAR(i_r_ord_hdr.shp_dt, 'YYYYMMDD')
                     || TO_CHAR(SYSDATE, 'YYYYMMDD')
                     || l_chg_cd   -- 'S' OrdSrc Chg, ' ' No Chg
                     || rpad_fn(i_r_ord_hdr.ord_src, 8)
                     || rpad_fn(i_r_ord_hdr.cust_pass_area, 25)
                     || rpad_fn(l_ord_comnt, 25)
                     || rpad_fn(i_r_ord_hdr.conf_num, 8)
                     || rpad_fn(i_r_ord_hdr.mcl_cust, 6)
                     || rpad_fn(i_r_ord_hdr.cust_id, 8)
                     || 'ACT'
                     || rpad_fn(i_r_ord_hdr.ser_num, 20)
                     ||(CASE
                          WHEN i_r_ord_hdr.ord_typ = 'N' THEN 'Y'
                          ELSE 'N'
                        END)
                     ||(CASE
                          WHEN i_r_ord_hdr.allw_partl_sw IN('1', 'Y') THEN 'Y'
                          ELSE 'N'
                        END)
                     || rpad_fn(i_r_ord_hdr.hdr_excptn_cd, 3)   -- hdexpa
                     || lpad_fn(i_r_ord_hdr.ord_ln_cnt, 7, '0')
                     || lpad_fn(i_r_ord_hdr.ord_num, 11, '0');
    logs.dbg(l_mq_msg_data);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_mq_msg_data);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_hdr_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTL_MSG_FN
  ||  Return order detail MQ data msg.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtl_msg_fn(
    i_po_num     IN  VARCHAR2,
    i_r_ord_dtl  IN  csr_orders_pk.g_rt_msg_dtl
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                    := 'CSR_ORDERS_PK.ORD_DTL_MSG_FN';
    lar_parm             logs.tar_parm;
    l_mq_msg_data        mclane_mq_put.mq_msg_data%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'PoNum', i_po_num);
    logs.add_parm(lar_parm, 'OrdLn', i_r_ord_dtl.ord_ln);
    logs.add_parm(lar_parm, 'CatlgNum', i_r_ord_dtl.catlg_num);
    logs.add_parm(lar_parm, 'CbrItem', i_r_ord_dtl.cbr_item);
    logs.add_parm(lar_parm, 'UOM', i_r_ord_dtl.uom);
    logs.add_parm(lar_parm, 'CustItem', i_r_ord_dtl.cust_item);
    logs.add_parm(lar_parm, 'OrdQty', i_r_ord_dtl.ord_qty);
    logs.add_parm(lar_parm, 'ItemPassArea', i_r_ord_dtl.item_pass_area);
    logs.add_parm(lar_parm, 'HardRtlSw', i_r_ord_dtl.hard_rtl_sw);
    logs.add_parm(lar_parm, 'RtlAmt', i_r_ord_dtl.rtl_amt);
    logs.add_parm(lar_parm, 'RtlMult', i_r_ord_dtl.rtl_mult);
    logs.add_parm(lar_parm, 'HardPriceSw', i_r_ord_dtl.hard_price_sw);
    logs.add_parm(lar_parm, 'PriceAmt', i_r_ord_dtl.price_amt);
    logs.add_parm(lar_parm, 'OrigQty', i_r_ord_dtl.orig_qty);
    logs.add_parm(lar_parm, 'BypMaxSw', i_r_ord_dtl.byp_max_sw);
    logs.add_parm(lar_parm, 'MaxQty', i_r_ord_dtl.max_qty);
    logs.add_parm(lar_parm, 'QtyMult', i_r_ord_dtl.qty_mult);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_mq_msg_data :=
      'D:'
      || rpad_fn(i_po_num, 30)
      || lpad_fn(i_r_ord_dtl.catlg_num, 6, '0')
      || lpad_fn(i_r_ord_dtl.cbr_item, 9, '0')
      || rpad_fn(i_r_ord_dtl.uom, 3)
      || rpad_fn(i_r_ord_dtl.cust_item, 10)
      || lpad_fn(i_r_ord_dtl.ord_qty, 9, '0')
      || rpad_fn(i_r_ord_dtl.item_pass_area, 20)
      ||(CASE
           WHEN i_r_ord_dtl.hard_rtl_sw IN('1', 'Y') THEN 'Y'
           ELSE 'N'
         END)
      ||(CASE
           WHEN i_r_ord_dtl.hard_price_sw IN('1', 'Y') THEN 'Y'
           ELSE 'N'
         END)
      || lpad_fn(TO_CHAR(TO_NUMBER((CASE
                                      WHEN i_r_ord_dtl.hard_rtl_sw IN('1', 'Y') THEN i_r_ord_dtl.rtl_amt
                                      ELSE '0'
                                    END)),
                         'FM0999999V99'
                        ),
                 9,
                 '0'
                )
      || lpad_fn(i_r_ord_dtl.rtl_mult, 5, '0')
      || lpad_fn(TO_CHAR(TO_NUMBER((CASE
                                      WHEN i_r_ord_dtl.hard_price_sw IN('1', 'Y') THEN i_r_ord_dtl.price_amt
                                      ELSE '0'
                                    END)),
                         'FM0999999V99'
                        ),
                 9,
                 '0'
                )
      || lpad_fn(i_r_ord_dtl.orig_qty, 9, '0')
      ||(CASE
           WHEN i_r_ord_dtl.byp_max_sw IN('1', 'Y') THEN 'Y'
           ELSE 'N'
         END)
      || ' '   -- rstfeb
      || LPAD('0', 3, '0')   -- manctb
      || LPAD('0', 3, '0')   -- totctb
      || ' '   -- authb
      || RPAD(' ', 8)   -- ntshpb
      || RPAD(' ', 14)   -- price ts
      || RPAD(' ', 10)   -- invnob
      || RPAD(' ', 8)   -- resgpb
      || lpad_fn(i_r_ord_dtl.max_qty, 9, '0')
      || RPAD(' ', 3)   -- dtexpb
      || lpad_fn(i_r_ord_dtl.qty_mult, 9, '0')
      || LPAD('0', 3, '0')   -- invctb
      || LPAD('0', 3, '0')   -- labctb
      || RPAD(' ', 8)   -- retgpb
      || lpad_fn(TO_CHAR(TO_NUMBER(i_r_ord_dtl.ord_ln), 'FM0999999V99'), 9, '0');
    logs.dbg(l_mq_msg_data);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_mq_msg_data);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_dtl_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_ALL_CANCELLED_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_all_cancelled_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_cv         SYS_REFCURSOR;
    l_cancel_sw  VARCHAR2(1)   := 'Y';
  BEGIN
    OPEN l_cv
     FOR
       SELECT 'N'
         FROM DUAL
        WHERE EXISTS(SELECT 1
                       FROM ordp120b b
                      WHERE b.div_part = i_div_part
                        AND b.ordnob = i_ord_num
                        AND b.statb <> 'C');

    FETCH l_cv
     INTO l_cancel_sw;

    RETURN(l_cancel_sw = 'Y');
  END is_all_cancelled_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTLS_SP
  ||  Return tables of detail MQ msgs and order lines.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 06/05/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 03/26/13 | rhalpai | Change to use a switch variable in SQL to indicate
  ||                    | whether the OrdLnList parameter is empty. This will
  ||                    | prevent ORA-01460 when OrdLnList contains more than
  ||                    | 4000 characters. IM-100877
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_dtls_sp(
    i_div_part     IN      NUMBER,
    i_ord_num      IN      NUMBER,
    i_ord_ln_list  IN      VARCHAR2,
    o_t_dtl_msgs   OUT     csr_orders_pk.g_tt_msgs,
    o_t_ord_lns    OUT     type_ntab
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'CSR_ORDERS_PK.ORD_DTLS_SP';
    lar_parm             logs.tar_parm;
    l_ord_lns_sw         VARCHAR2(1);
    l_t_ord_lns          type_ntab;
    l_t_msg_dtls         csr_orders_pk.g_tt_msg_dtls;
    l_po_num             ordp100a.cpoa%TYPE;
    l_idx                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLnList', i_ord_ln_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    o_t_ord_lns := type_ntab();
    o_t_dtl_msgs := csr_orders_pk.g_tt_msgs();

    IF i_ord_ln_list IS NULL THEN
      l_ord_lns_sw := 'N';
    ELSE
      l_ord_lns_sw := 'Y';
      logs.dbg('Parse Order Lines');
      l_t_ord_lns := num.parse_list(i_ord_ln_list, op_const_pk.field_delimiter);
    END IF;   -- i_ord_ln_list IS NULL

    logs.dbg('Get Order Details');

    SELECT   b.orditb,
             e.iteme,
             e.uome,
             b.ordqtb,
             b.cusitb,
             b.itpasb,
             b.rtfixb,
             b.hdrtab,
             b.hdrtmb,
             b.prfixb,
             b.hdprcb,
             b.orgqtb,
             b.bymaxb,
             b.maxqtb,
             b.qtmulb,
             b.lineb
    BULK COLLECT INTO l_t_msg_dtls
        FROM ordp100a a, ordp120b b, sawp505e e
       WHERE a.div_part = i_div_part
         AND a.ordnoa = i_ord_num
         AND e.catite(+) = b.orditb
         AND a.stata IN('O', 'I', 'S')
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.lineb = FLOOR(b.lineb)
         AND (   l_ord_lns_sw = 'N'
              OR b.lineb IN(SELECT t.column_value
                              FROM TABLE(CAST(l_t_ord_lns AS type_ntab)) t))
         AND b.statb IN('O', 'I', 'S')
    ORDER BY lineb;

    IF l_t_msg_dtls IS NOT NULL THEN
      logs.dbg('Get PO');

      SELECT a.cpoa
        INTO l_po_num
        FROM ordp100a a
       WHERE a.div_part = i_div_part
         AND a.ordnoa = i_ord_num;

      l_idx := l_t_msg_dtls.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        logs.dbg('Append Line Number to Order Line Table');
        o_t_ord_lns.EXTEND;
        o_t_ord_lns(o_t_ord_lns.LAST) := l_t_msg_dtls(l_idx).ord_ln;
        logs.dbg('Append Record to Order Detail Msgs Table');
        o_t_dtl_msgs.EXTEND;
        o_t_dtl_msgs(o_t_dtl_msgs.LAST) := ord_dtl_msg_fn(l_po_num, l_t_msg_dtls(l_idx));
        l_idx := l_t_msg_dtls.NEXT(l_idx);
      END LOOP;
    END IF;   -- l_msg_dtls IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_dtls_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_CANCEL_LN_SP
  ||  Parse delimited list to table for Cancel Order Line process.
  ||  List is in the following format:
  ||  ord_ln~rsn_cd~auth_by`ord_ln~rsn_cd~auth_by
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_cancel_ln_sp(
    i_parm_list     IN      VARCHAR2,
    o_t_cancel_lns  OUT     g_tt_cancel_lns
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.PARSE_CANCEL_LN_SP';
    lar_parm             logs.tar_parm;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    o_t_cancel_lns := g_tt_cancel_lns();
    logs.dbg('Parse Groups');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      logs.dbg('Parse Fields');
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_t_fields := NULL;
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
        o_t_cancel_lns.EXTEND;
        o_t_cancel_lns(l_idx).ord_ln := val_at_idx_fn(l_t_fields, 1);
        o_t_cancel_lns(l_idx).rsn_cd := val_at_idx_fn(l_t_fields, 2);
        o_t_cancel_lns(l_idx).auth_by := val_at_idx_fn(l_t_fields, 3);
        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;
    END IF;   -- l_t_grps IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END parse_cancel_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_INS_LN_SP
  ||  Parse delimited list to table for Insert Order Line process.
  ||  List is in the following format:
  ||  ord_ln~catlg_num~ord_qty~rsn_cd~auth_by`ord_ln~catlg_num~ord_qty~rsn_cd~auth_by
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_ins_ln_sp(
    i_parm_list  IN      VARCHAR2,
    o_t_ins_lns  OUT     g_tt_ins_lns
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.PARSE_INS_LN_SP';
    lar_parm             logs.tar_parm;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    o_t_ins_lns := g_tt_ins_lns();
    logs.dbg('Parse Groups');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      logs.dbg('Parse Fields');
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_t_fields := NULL;
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
        o_t_ins_lns.EXTEND;
        o_t_ins_lns(l_idx).ord_ln := val_at_idx_fn(l_t_fields, 1);
        o_t_ins_lns(l_idx).catlg_num := val_at_idx_fn(l_t_fields, 2);
        o_t_ins_lns(l_idx).ord_qty := val_at_idx_fn(l_t_fields, 3);
        o_t_ins_lns(l_idx).rsn_cd := val_at_idx_fn(l_t_fields, 4);
        o_t_ins_lns(l_idx).auth_by := val_at_idx_fn(l_t_fields, 5);
        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;
    END IF;   -- l_t_grps IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END parse_ins_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_UPD_LN_SP
  ||  Parse delimited list to table for Update Order Line process.
  ||  List is in the following format:
  ||  ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by`ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_upd_ln_sp(
    i_parm_list  IN      VARCHAR2,
    o_t_upd_lns  OUT     g_tt_upd_lns
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.PARSE_UPD_LN_SP';
    lar_parm             logs.tar_parm;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    o_t_upd_lns := g_tt_upd_lns();
    logs.dbg('Parse Groups');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      logs.dbg('Parse Fields');
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        l_t_fields := NULL;
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
        o_t_upd_lns.EXTEND;
        o_t_upd_lns(l_idx).ord_ln := val_at_idx_fn(l_t_fields, 1);
        o_t_upd_lns(l_idx).byp_max_sw := val_at_idx_fn(l_t_fields, 2);
        o_t_upd_lns(l_idx).ord_qty := val_at_idx_fn(l_t_fields, 3);
        o_t_upd_lns(l_idx).rsn_cd := val_at_idx_fn(l_t_fields, 4);
        o_t_upd_lns(l_idx).auth_by := val_at_idx_fn(l_t_fields, 5);
        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;
    END IF;   -- l_grps IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END parse_upd_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_ORD_HDR_SP
  ||  Create order header.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  || 11/05/14 | rhalpai | Add order source input parameter and load to order
  ||                    | header. PIR12893
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_ord_hdr_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_conf_num   IN  VARCHAR2,
    i_ord_src    IN  VARCHAR2,
    i_test_sw    IN  VARCHAR2,
    i_no_ord_sw  IN  VARCHAR2,
    i_load_typ   IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_po_num     IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm      := 'CSR_ORDERS_PK.INS_ORD_HDR_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE               := SYSDATE;
    l_r_ordp100a          ordp100a%ROWTYPE;
    l_cv                  SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'OrdSrc', i_ord_src);
    logs.add_parm(lar_parm, 'TestSw', i_test_sw);
    logs.add_parm(lar_parm, 'NoOrdSw', i_no_ord_sw);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PO', i_po_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    op_ord_hdr_pk.init_sp(l_r_ordp100a);
    l_r_ordp100a.ordnoa := i_ord_num;
    l_r_ordp100a.div_part := i_div_part;
    l_r_ordp100a.excptn_sw := 'N';
    l_r_ordp100a.load_depart_sid := 0;
    l_r_ordp100a.ldtypa := i_load_typ;
    l_r_ordp100a.cpoa := i_po_num;
    l_r_ordp100a.stata := 'I';
    l_r_ordp100a.dsorda :=(CASE
                             WHEN i_test_sw = 'T' THEN 'T'
                             WHEN i_no_ord_sw IN('1', 'Y') THEN 'N'
                             ELSE 'R'
                           END);
    l_r_ordp100a.ipdtsa := i_ord_src;
    l_r_ordp100a.connba := i_conf_num;
    l_r_ordp100a.telsla := NULL;
    l_r_ordp100a.trndta := g_c_default_rendate;
    l_r_ordp100a.trntma := TO_CHAR(l_c_sysdate, 'HH24MISS');
    l_r_ordp100a.ord_rcvd_ts := l_c_sysdate;
    l_r_ordp100a.cspasa := NULL;
    l_r_ordp100a.hdexpa := NULL;
    l_r_ordp100a.legrfa := NULL;
    l_r_ordp100a.pshipa := '1';
    l_r_ordp100a.shpja := g_c_default_rendate;
    l_r_ordp100a.mntusa := i_user_id;
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT c.acnoc
         FROM mclp020b cx, sysp200c c
        WHERE c.div_part = i_div_part
          AND c.acnoc = cx.custb
          AND cx.div_part = i_div_part
          AND cx.mccusb = i_mcl_cust;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_r_ordp100a.custa;

    logs.dbg('Add Order Header');

    INSERT INTO ordp100a
         VALUES l_r_ordp100a;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_ord_hdr_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOG_CHANGE_SP
  ||  Audit change. Writes an entry into Log table (sysp296a).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/14/05 | snagabh | Original
  || 02/19/07 | Arun    | The call is made to OP_SYSP296A_PK for sysp296a insert
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 12/08/15 | rhalpai | Add DivPart in call to OP_SYSP296A_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_change_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_user_id   IN  VARCHAR2,
    i_tbl_nm    IN  VARCHAR2,
    i_field_nm  IN  VARCHAR2,
    i_orig_val  IN  VARCHAR2,
    i_new_val   IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.LOG_CHANGE_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'TblNm', i_tbl_nm);
    logs.add_parm(lar_parm, 'FieldNm', i_field_nm);
    logs.add_parm(lar_parm, 'OrigVal', i_orig_val);
    logs.add_parm(lar_parm, 'NewVal', i_new_val);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
    op_sysp296a_pk.ins_sp(i_div_part,
                          i_ord_num,
                          i_ord_ln,
                          i_user_id,
                          i_tbl_nm,
                          i_field_nm,
                          i_orig_val,
                          i_new_val,
                          i_actn_cd,
                          i_rsn_cd,
                          i_auth_by,
                          NULL
                         );
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END log_change_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_HDR_SP
  ||  Change PO and/or customer number on order.
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 05/09/13 | rhalpai | Change logic to not change status for cancelled order
  ||                    | lines. IM-102610
  || 11/05/14 | rhalpai | Add order source input parameter and update order
  ||                    | header. PIR12893
  || 01/14/15 | rhalpai | Change logic to reset PO to NULL only when Cust
  ||                    | change without PO change. IM-239536
  || 05/12/22 | rhalpai | Add UserId parm and add logic to log changes in PO/CustId/OrdSrc. SDHD-1275110
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_hdr_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_po_num    IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2,
    i_ord_src   IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_ORDERS_PK.UPD_ORD_HDR_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_old_cust_id        ordp100a.custa%TYPE;
    l_old_mcl_cust       mclp020b.mccusb%TYPE;
    l_old_po_num         ordp100a.cpoa%TYPE;
    l_old_ord_src        ordp100a.ipdtsa%TYPE;
    l_cust_id            sysp200c.acnoc%TYPE;
    l_po_num             ordp100a.cpoa%TYPE;
    l_load_depart_sid    NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PO', i_po_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'OrdSrc', i_ord_src);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT a.custa, cx.mccusb, a.cpoa, a.ipdtsa
         FROM ordp100a a, mclp020b cx
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND a.stata IN('O', 'I', 'S');

    FETCH l_cv
     INTO l_old_cust_id, l_old_mcl_cust, l_old_po_num, l_old_ord_src;

    CLOSE l_cv;

    IF l_old_mcl_cust IS NOT NULL THEN
      IF (   NVL(i_mcl_cust, '~') <> l_old_mcl_cust
          OR NVL(i_po_num, '~') <> NVL(l_old_po_num, '~')
          OR NVL(i_ord_src, '~') <> NVL(l_old_ord_src, '~')
         ) THEN
        --  change PO to NULL when Cust changes without PO change
        l_po_num :=(CASE
                      WHEN     NVL(i_mcl_cust, '~') <> l_old_mcl_cust
                           AND NVL(i_po_num, '~') = NVL(l_old_po_num, '~') THEN NULL
                      ELSE i_po_num
                    END
                   );

        IF i_mcl_cust <> l_old_mcl_cust THEN
          l_cust_id := csr_customers_pk.cbr_cust_fn(i_div_part, i_mcl_cust);
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, DATE '1900-01-01', 'DFLT');
          op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, l_cust_id);
        END IF;   -- i_mcl_cust <> l_old_mcl_cust

        UPDATE ordp100a a
           SET a.cpoa = l_po_num,
               a.custa = NVL(l_cust_id, a.custa),
               a.ipdtsa = i_ord_src,
               a.stata = 'I',
               a.load_depart_sid = NVL(l_load_depart_sid, a.load_depart_sid)
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num
           AND a.stata IN('O', 'I', 'S');

        IF SQL%ROWCOUNT > 0 THEN
          IF i_mcl_cust <> l_old_mcl_cust THEN
            log_change_sp(i_div_part,
                          i_ord_num,
                          i_user_id,
                          'ORDP100A',
                          'CUSTA',
                          l_old_cust_id,
                          l_cust_id,
                          'M',
                          'QCHG09',
                          'CSR'
                         );
          END IF;   -- i_mcl_cust <> l_old_mcl_cust

          IF NVL(l_po_num, '~') <> NVL(l_old_po_num, '~') THEN
            log_change_sp(i_div_part,
                          i_ord_num,
                          i_user_id,
                          'ORDP100A',
                          'CPOA',
                          l_old_po_num,
                          l_po_num,
                          'M',
                          'QCHG09',
                          'CSR'
                         );
          END IF;   -- NVL(l_po_num, '~') <> NVL(l_old_po_num, '~')

          IF NVL(i_ord_src, '~') <> NVL(l_old_ord_src, '~') THEN
            log_change_sp(i_div_part,
                          i_ord_num,
                          i_user_id,
                          'ORDP100A',
                          'IPDTSA',
                          l_old_ord_src,
                          i_ord_src,
                          'M',
                          'QCHG09',
                          'CSR'
                         );
          END IF;   -- NVL(i_ord_src, '~') <> NVL(l_old_ord_src, '~')

          UPDATE ordp120b b
             SET b.statb = DECODE(b.statb, 'C', 'C', 'I')
           WHERE b.div_part = i_div_part
             AND b.ordnob = i_ord_num;
        END IF;   -- SQL%ROWCOUNT > 0
      END IF;   -- NVL(i_mcl_cust, '~') <> l_old_mcl_cust OR NVL(i_po_num, '~') <> NVL(l_old_po_num, '~') OR NVL(i_ord_src, '~') <> NVL(l_old_ord_src, '~')
    END IF;   -- l_old_mcl_cust IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_hdr_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_ORD_DTL_SP
  ||  Add new order lines.
  ||  List is in the following format:
  ||  ord_ln~catlg_num~ord_qty~rsn_cd~auth_by`ord_ln~catlg_num~ord_qty~rsn_cd~auth_by
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 08/26/10 | rhalpai | Remove unused columns. PIR8859
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 05/21/13 | dlbeal  | Correct code for departure date/time and eta date/time
  || 05/23/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_ord_dtl_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_parm_list  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'CSR_ORDERS_PK.INS_ORD_DTL_SP';
    lar_parm             logs.tar_parm;
    l_t_ins_lns          g_tt_ins_lns;
    l_r_ordp120b         ordp120b%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Parse');
    parse_ins_ln_sp(i_parm_list, l_t_ins_lns);

    IF (    l_t_ins_lns IS NOT NULL
        AND l_t_ins_lns.COUNT > 0) THEN
      logs.dbg('Initialize');
      op_ord_dtl_pk.init_sp(l_r_ordp120b);
      l_r_ordp120b.ordnob := i_ord_num;
      l_r_ordp120b.div_part := i_div_part;
      l_r_ordp120b.excptn_sw := 'N';
      l_r_ordp120b.statb := 'I';
      l_r_ordp120b.rtfixb := '0';
      l_r_ordp120b.prfixb := '0';
      l_r_ordp120b.hdrtab := 0;
      l_r_ordp120b.hdrtmb := 0;
      l_r_ordp120b.hdprcb := 0;
      l_r_ordp120b.bymaxb := '0';
      l_r_ordp120b.qtmulb := 0;
      l_r_ordp120b.prstdb := 0;
      l_r_ordp120b.prsttb := 0;
      l_r_ordp120b.maxqtb := 0;
      l_r_ordp120b.invctb := 0;
      l_r_ordp120b.labctb := 0;
      logs.dbg('Process New Lines');
      FOR i IN l_t_ins_lns.FIRST .. l_t_ins_lns.LAST LOOP
        l_r_ordp120b.lineb := l_t_ins_lns(i).ord_ln;
        l_r_ordp120b.orditb := l_t_ins_lns(i).catlg_num;
        logs.dbg('Get CBR Item/UOM');
        csr_items_pk.cbr_item_sp(l_t_ins_lns(i).catlg_num, l_r_ordp120b.itemnb, l_r_ordp120b.sllumb);
        l_r_ordp120b.ordqtb := l_t_ins_lns(i).ord_qty;
        l_r_ordp120b.orgqtb := l_t_ins_lns(i).ord_qty;
        logs.dbg('Add Order Line');

        INSERT INTO ordp120b
             VALUES l_r_ordp120b;

        IF TRIM(l_t_ins_lns(i).rsn_cd) IS NOT NULL THEN
          logs.dbg('Log Addition of New Line');
          log_change_sp(i_div_part,
                        i_ord_num,
                        '',
                        'ORDP120B',
                        '',
                        '',
                        '',
                        'I',
                        l_t_ins_lns(i).rsn_cd,
                        l_t_ins_lns(i).auth_by,
                        l_t_ins_lns(i).ord_ln
                       );
        END IF;   -- TRIM(l_t_ins_lns(i).rsn_cd) IS NOT NULL
      END LOOP;
    END IF;   -- l_t_ins_lns IS NOT NULL AND l_t_ins_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_ord_dtl_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_NEW_SP
  ||  Remove header and all lines from new order OR remove line from new order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_new_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.DEL_NEW_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);

    IF i_ord_ln IS NULL THEN
      logs.dbg('Remove Order');

      DELETE FROM ordp100a
            WHERE div_part = i_div_part
              AND ordnoa = i_ord_num
              AND stata = 'I';

      DELETE FROM ordp120b
            WHERE div_part = i_div_part
              AND ordnob = i_ord_num
              AND statb = 'I';
    ELSE
      logs.dbg('Remove Order Line');

      DELETE FROM ordp120b
            WHERE div_part = i_div_part
              AND ordnob = i_ord_num
              AND lineb = i_ord_ln
              AND statb = 'I';
    END IF;   -- i_ord_ln IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_new_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_ORD_SP
  ||  Cancel existing order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_ord_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.CANCEL_ORD_SP';
    lar_parm             logs.tar_parm;
    l_lock_sw            VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Lock Order');
    op_ord_hdr_pk.lock_ord_sp(i_div_part, i_ord_num, l_lock_sw);
    excp.assert((l_lock_sw = 'Y'), 'Order not found or unavailable');
    logs.dbg('Cancel Order Hdr');

    UPDATE ordp100a
       SET stata = 'C',
           mntusa = NULL
     WHERE div_part = i_div_part
       AND ordnoa = i_ord_num;

    logs.dbg('Cancel Lines for Order');

    UPDATE ordp120b
       SET statb = 'C'
     WHERE div_part = i_div_part
       AND ordnob = i_ord_num;

    logs.dbg('Log Status Change');
    log_change_sp(i_div_part, i_ord_num, i_user_id, 'ORDP100A', 'STATA', NULL, 'C', 'C', i_rsn_cd, i_auth_by);
    logs.dbg('Move Order to History');
    op_cleanup_pk.move_order_to_hist_sp(i_div_part, i_ord_num);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancel_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_ORD_LN_SP
  ||  Change line to cancelled status for existing order or remove line for new
  ||  order.
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_ord_ln_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.CANCEL_ORD_LN_SP';
    lar_parm             logs.tar_parm;
    l_t_ord_lns          type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF is_new_ord_fn(i_div_part, i_ord_num) THEN
      logs.dbg('Remove Line from New Order');
      del_new_sp(i_div_part, i_ord_num, i_ord_ln);
    ELSE
      logs.dbg('Cancel OrdLn');

      UPDATE    ordp120b b
            SET b.statb = 'C'
          WHERE b.div_part = i_div_part
            AND b.ordnob = i_ord_num
            AND FLOOR(b.lineb) = FLOOR(i_ord_ln)
            AND b.statb IN('O', 'I', 'S')
      RETURNING         b.lineb
      BULK COLLECT INTO l_t_ord_lns;

      IF l_t_ord_lns.COUNT > 0 THEN
        FOR i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST LOOP
          logs.dbg('Log Status Change for Order Line');
          log_change_sp(i_div_part,
                        i_ord_num,
                        i_user_id,
                        'ORDP120B',
                        'STATB',
                        NULL,
                        'C',
                        'M',
                        i_rsn_cd,
                        i_auth_by,
                        l_t_ord_lns(i)
                       );
        END LOOP;
      END IF;   -- l_t_ord_lns.COUNT > 0
    END IF;   -- is_new_ord_fn(i_div_part, i_ord_num)

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancel_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_ORD_LNS_SP
  ||  Change lines to cancelled status for existing order or remove lines for
  ||  new order.
  ||  List is in the following format:
  ||  ord_ln~rsn_cd~auth_by`ord_ln~rsn_cd~auth_by
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 07/16/12 | rhalpai | Change logic to set RsnCd and AuthBy variables to use
  ||                    | outside loop. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_ord_lns_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_parm_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_ORDERS_PK.CANCEL_ORD_LNS_SP';
    lar_parm             logs.tar_parm;
    l_t_cancel_lns       g_tt_cancel_lns;
    l_rsn_cd             sysp296a.rsncda%TYPE;
    l_auth_by            sysp296a.autbya%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF NVL(i_ord_num, 0) > 0 THEN
      logs.dbg('Parse');
      parse_cancel_ln_sp(i_parm_list, l_t_cancel_lns);

      IF (    l_t_cancel_lns IS NOT NULL
          AND l_t_cancel_lns.COUNT > 0) THEN
        logs.dbg('Cancel Order Lines');
        FOR i IN l_t_cancel_lns.FIRST .. l_t_cancel_lns.LAST LOOP
          l_rsn_cd := l_t_cancel_lns(i).rsn_cd;
          l_auth_by := l_t_cancel_lns(i).auth_by;
          cancel_ord_ln_sp(i_div_part,
                           i_ord_num,
                           l_t_cancel_lns(i).ord_ln,
                           l_t_cancel_lns(i).rsn_cd,
                           l_t_cancel_lns(i).auth_by,
                           i_user_id
                          );
        END LOOP;

        IF (    NVL(i_ord_num, 0) > 0
            AND is_all_cancelled_fn(i_div_part, i_ord_num)) THEN
          logs.dbg('Cancel Order Header');
          cancel_ord_sp(i_div_part, i_ord_num, l_rsn_cd, l_auth_by, i_user_id);
        END IF;   -- NVL(i_ord_num, 0) > 0 AND is_all_cancelled_fn(i_div_part, i_ord_num)
      END IF;   -- l_t_cancel_lns IS NOT NULL AND l_t_cancel_lns.COUNT > 0
    END IF;   -- NVL(i_ord_num, 0) > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancel_ord_lns_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_DTL_SP
  ||  Change order-qty/bypass-max for order lines.
  ||  List is in the following format:
  ||  ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by`ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart in call to OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP
  ||                    | and pass CatlgNum instead of CbrItem, UOM. Add DivPart
  ||                    | in call to OP_ORDER_VALIDATION_PK.VALIDATE_DETAILS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_dtl_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_parm_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.UPD_ORD_DTL_SP';
    lar_parm             logs.tar_parm;
    l_t_upd_lns          g_tt_upd_lns;
    l_lock_sw            VARCHAR2(1);
    l_cv                 SYS_REFCURSOR;
    l_old_ord_qty        PLS_INTEGER;
    l_sub_info           VARCHAR2(30);
    l_new_ord_qty        PLS_INTEGER;
    l_sub_ln             NUMBER;
    l_catlg_num          NUMBER;
    l_byp_max_sw         VARCHAR2(1);
    l_item_max           PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Parse');
    parse_upd_ln_sp(i_parm_list, l_t_upd_lns);

    IF (    l_t_upd_lns IS NOT NULL
        AND l_t_upd_lns.COUNT > 0) THEN
      logs.dbg('Process Upd Order Lines');
      FOR i IN l_t_upd_lns.FIRST .. l_t_upd_lns.LAST LOOP
        IF (    l_t_upd_lns(i).ord_qty IS NOT NULL
            AND l_t_upd_lns(i).ord_ln = FLOOR(l_t_upd_lns(i).ord_ln)) THEN
          logs.dbg('Lock Order Line/SubLine');
          op_ord_dtl_pk.lock_ord_ln_sp(i_div_part, i_ord_num, l_t_upd_lns(i).ord_ln, l_lock_sw);

          IF l_lock_sw = 'Y' THEN
            logs.dbg('Open Order Info Cursor');

            OPEN l_cv
             FOR
               SELECT b.ordqtb AS ord_qty,
                      NVL((SELECT LPAD(b2.lineb, 7) || e.catite || NVL(b2.bymaxb, '0') || NVL(i.max_ord_qty, 99999)
                             FROM ordp100a a2, ordp120b b2, mclp110b i, sawp505e e
                            WHERE a2.div_part = i_div_part
                              AND a2.ordnoa = i_ord_num
                              AND b2.div_part = i_div_part
                              AND b2.ordnob = i_ord_num
                              AND FLOOR(b2.lineb) = l_t_upd_lns(i).ord_ln
                              AND b2.lineb > FLOOR(b2.lineb)
                              AND i.div_part = a2.div_part
                              AND i.itemb = b2.itemnb
                              AND i.uomb = b2.sllumb
                              AND e.iteme = i.itemb
                              AND e.uome = i.uomb),
                          'N/A'
                         ) AS sub_info
                 FROM ordp120b b
                WHERE b.div_part = i_div_part
                  AND b.ordnob = i_ord_num
                  AND b.lineb = l_t_upd_lns(i).ord_ln;

            logs.dbg('Fetch Order Info Cursor');

            FETCH l_cv
             INTO l_old_ord_qty, l_sub_info;

            l_new_ord_qty := l_t_upd_lns(i).ord_qty;

            IF l_sub_info <> 'N/A' THEN
              l_sub_ln := SUBSTR(l_sub_info, 1, 7);
              l_catlg_num := TO_NUMBER(SUBSTR(l_sub_info, 8, 6));
              l_byp_max_sw := NVL(l_t_upd_lns(i).byp_max_sw, SUBSTR(l_sub_info, 14, 1));
              l_item_max := SUBSTR(l_sub_info, 15);
              logs.dbg('Check Max Qty');
              -- will reduce l_new_ord_qty if necessary
              op_order_validation_pk.check_max_qty_sp(i_div_part,
                                                      i_ord_num,
                                                      l_sub_ln,
                                                      l_catlg_num,
                                                      l_new_ord_qty,
                                                      l_byp_max_sw,
                                                      '0',
                                                      l_item_max,
                                                      l_item_max
                                                     );
            END IF;   -- l_sub_info <> 'N/A'

            logs.dbg('Upd Order Lines');

            UPDATE ordp120b b
               SET b.ordqtb = l_new_ord_qty,
                   b.bymaxb = NVL(l_t_upd_lns(i).byp_max_sw, b.bymaxb),
                   -- when changing ord-qty from zero reset not-ship-reason for validation
                   b.ntshpb = DECODE(b.ntshpb, 'QTYZERO', NULL, b.ntshpb),
                   b.excptn_sw = DECODE(b.ntshpb, 'QTYZERO', 'N', b.excptn_sw)
             WHERE b.div_part = i_div_part
               AND b.ordnob = i_ord_num
               AND FLOOR(b.lineb) = l_t_upd_lns(i).ord_ln;

            logs.dbg('Log Qty Change');
            log_change_sp(i_div_part,
                          i_ord_num,
                          i_user_id,
                          'ORDP120B',
                          'ORDQTB',
                          l_old_ord_qty,
                          l_new_ord_qty,
                          'M',
                          l_t_upd_lns(i).rsn_cd,
                          l_t_upd_lns(i).auth_by,
                          l_t_upd_lns(i).ord_ln
                         );

            IF l_sub_info = 'N/A' THEN
              logs.dbg('Validate Order Line');
              op_order_validation_pk.validate_details_sp(i_div_part, i_ord_num, l_t_upd_lns(i).ord_ln);
            END IF;   -- l_sub_info = 'N/A'
          END IF;   -- l_lock_sw = 'Y'
        END IF;   -- l_t_upd_lns(i).ord_qty IS NOT NULL AND l_t_upd_lns(i).ord_ln = FLOOR(l_t_upd_lns(i).ord_ln)
      END LOOP;
    END IF;   -- l_t_upd_lns IS NOT NULL AND l_t_upd_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_dtl_sp;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_ORD_HDR_SP
  ||  Save order customer and/or po changes.
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 11/05/14 | rhalpai | Add order source input parameter and pass to
  ||                    | UPD_ORD_HDR_SP, INS_ORD_HDR_SP. PIR12893
  || 05/12/22 | rhalpai | Add UserId in call to UPD_ORD_HDR_SP. SDHD-1275110
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_ord_hdr_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_conf_num   IN  VARCHAR2,
    i_ord_src    IN  VARCHAR2,
    i_test_sw    IN  VARCHAR2,
    i_no_ord_sw  IN  VARCHAR2,
    i_load_typ   IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_po_num     IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_ORDERS_PK.SAVE_ORD_HDR_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_mcl_cust           mclp020b.mccusb%TYPE;
    l_po_num             ordp100a.cpoa%TYPE;
    l_ord_src            ordp100a.ipdtsa%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'OrdSrc', i_ord_src);
    logs.add_parm(lar_parm, 'TestSw', i_test_sw);
    logs.add_parm(lar_parm, 'NoOrdSw', i_no_ord_sw);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor for Order');

    OPEN l_cv
     FOR
       SELECT cx.mccusb, a.cpoa, a.ipdtsa
         FROM ordp100a a, mclp020b cx
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa;

    logs.dbg('Fetch Cursor for Order');

    FETCH l_cv
     INTO l_mcl_cust, l_po_num, l_ord_src;

    CLOSE l_cv;

    IF l_mcl_cust IS NOT NULL THEN
      IF (   i_mcl_cust <> l_mcl_cust
          OR NVL(i_po_num, 'NULL') <> NVL(l_po_num, 'NULL')
          OR NVL(i_ord_src, 'NULL') <> NVL(l_ord_src, 'NULL')
         ) THEN
        logs.dbg('Upd Ord Hdr');
        upd_ord_hdr_sp(i_div_part, i_ord_num, i_po_num, i_mcl_cust, i_ord_src, i_user_id);
      END IF;   --  i_mcl_cust <> l_mcl_cust OR NVL(i_po_num, 'NULL') <> NVL(l_po_num, 'NULL') OR NVL(i_ord_src, 'NULL') <> NVL(l_ord_src, 'NULL')
    ELSE
      logs.dbg('Add Ord Hdr');
      ins_ord_hdr_sp(i_div_part,
                     i_ord_num,
                     i_conf_num,
                     i_ord_src,
                     i_test_sw,
                     i_no_ord_sw,
                     i_load_typ,
                     i_mcl_cust,
                     i_po_num,
                     i_user_id
                    );
    END IF;   -- l_mcl_cust IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END save_ord_hdr_sp;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_ORD_DTL_SP
  ||  Save order details.
  ||  I_CANCEL_LN_PARM_LIST is in the following format:
  ||  ord_ln~rsn_cd~auth_by`ord_ln~rsn_cd~auth_by
  ||  I_NEW_DTL_PARM_LIST is in the following format:
  ||  ord_ln~catlg_num~ord_qty~rsn_cd~auth_by`ord_ln~catlg_num~ord_qty~rsn_cd~auth_by
  ||  I_UPD_DTL_PARM_LIST is in the following format:
  ||  ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by`ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_ord_dtl_sp(
    i_div_part             IN  NUMBER,
    i_ord_num              IN  NUMBER,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_user_id              IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.SAVE_ORD_DTL_SP';
    lar_parm             logs.tar_parm;
    l_cancel_ln_cnt      PLS_INTEGER;
    l_ln_cnt             PLS_INTEGER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'CancelLnParmList', i_cancel_ln_parm_list);
    logs.add_parm(lar_parm, 'NewDtlParmList', i_new_dtl_parm_list);
    logs.add_parm(lar_parm, 'UpdDtlParmList', i_upd_dtl_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_new_dtl_parm_list IS NOT NULL THEN
      logs.dbg('Add Order Lines');
      ins_ord_dtl_sp(i_div_part, i_ord_num, i_new_dtl_parm_list);
    END IF;   -- i_new_dtl_parm_list IS NOT NULL

    IF i_cancel_ln_parm_list IS NOT NULL THEN
      logs.dbg('Get Count of Parms in List');
      l_cancel_ln_cnt := list_cnt_fn(i_cancel_ln_parm_list, op_const_pk.grp_delimiter);
      l_ln_cnt := NULL;

      IF i_new_dtl_parm_list IS NULL THEN
        logs.dbg('Open Cursor');

        OPEN l_cv
         FOR
           SELECT COUNT(*) AS ln_cnt
             FROM ordp120b
            WHERE div_part = i_div_part
              AND ordnob = i_ord_num;

        logs.dbg('Fetch Cursor');

        FETCH l_cv
         INTO l_ln_cnt;
      END IF;   -- i_new_dtl_parm_list IS NULL

      IF l_cancel_ln_cnt = l_ln_cnt THEN
        logs.dbg('Remove New Order');
        del_new_sp(i_div_part, i_ord_num);
      ELSE
        logs.dbg('Remove Lines for New Order');
        cancel_ord_lns_sp(i_div_part, i_ord_num, i_cancel_ln_parm_list, i_user_id);
      END IF;   -- l_cancel_ln_cnt = l_ln_cnt
    END IF;   -- i_cancel_ln_parm_list IS NOT NULL

    IF i_upd_dtl_parm_list IS NOT NULL THEN
      logs.dbg('Upd Order Lines');
      upd_ord_dtl_sp(i_div_part, i_ord_num, i_upd_dtl_parm_list, i_user_id);
    END IF;   -- i_upd_dtl_parm_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END save_ord_dtl_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  ||  ORDER_TYP_LIST_FN
  ||    Order type list.
  ||
  ||    This procedure is called by OrderTypeListDS.retrieveList (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION order_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.ORDER_TYP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   otypeo, otdsco
           FROM ordp991o
       ORDER BY 1 DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END order_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORDER_DETAIL_STATUS_FN
  ||  Get order status description in "MF/S/U/B/C/SH" format.
  ||
  ||  This function uses both header and detail table to compute Order Status.
  ||  This function replaces order_Status_Descr_fn()
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/16/06 | snagabh | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION order_detail_status_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  VARCHAR2,
    i_conf_num  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.ORDER_DETAIL_STATUS_FN';
    lar_parm                logs.tar_parm;
    l_ord_num               NUMBER;
    l_cv                    SYS_REFCURSOR;
    l_stat_descr            VARCHAR2(30);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(i_div_part, i_conf_num)
                 END);

    OPEN l_cv
     FOR
       WITH b AS
           (SELECT b1.statb
              FROM ordp120b b1
             WHERE b1.ordnob = l_ord_num
               AND b1.div_part = i_div_part
            UNION
            SELECT b3.statb
              FROM ordp920b b3
             WHERE b3.ordnob = l_ord_num
               AND b3.div_part = i_div_part),
           a AS
           (SELECT o1.stata, o1.mntusa
              FROM ordp100a o1
             WHERE o1.ordnoa = l_ord_num
               AND o1.div_part = i_div_part
            UNION ALL
            SELECT o2.stata, NULL mntusa
              FROM ordp900a o2
             WHERE o2.ordnoa = l_ord_num
               AND o2.div_part = i_div_part)
       SELECT (CASE
                 WHEN EXISTS(SELECT 1
                               FROM a
                              WHERE a.stata = 'I'
                                AND TRIM(a.mntusa) IS NOT NULL) THEN 'IC/'
                 WHEN EXISTS(SELECT 1
                               FROM a
                              WHERE a.stata = 'I')
                  OR EXISTS(SELECT 1
                              FROM b
                             WHERE b.statb = 'I') THEN 'MF/'
                 ELSE '  /'
               END
              )
              ||(CASE
                   WHEN EXISTS(SELECT 1
                                 FROM a
                                WHERE a.stata = 'S')
                    OR EXISTS(SELECT 1
                                FROM b
                               WHERE b.statb = 'S') THEN 'S/'
                   ELSE ' /'
                 END)
              ||(CASE
                   WHEN EXISTS(SELECT 1
                                 FROM b
                                WHERE b.statb = 'O') THEN 'U/'
                   ELSE ' /'
                 END)
              ||(CASE
                   WHEN EXISTS(SELECT 1
                                 FROM a
                                WHERE a.stata IN('P', 'R'))
                    OR EXISTS(SELECT 1
                                FROM b
                               WHERE b.statb IN('P', 'R')) THEN 'B/'
                   ELSE ' /'
                 END)
              ||(CASE
                   WHEN EXISTS(SELECT 1
                                 FROM a
                                WHERE a.stata = 'C')
                    OR EXISTS(SELECT 1
                                FROM b
                               WHERE b.statb = 'C') THEN 'C/'
                   ELSE ' /'
                 END)
              ||(CASE
                   WHEN EXISTS(SELECT 1
                                 FROM a
                                WHERE a.stata = 'A')
                    OR EXISTS(SELECT 1
                                FROM b
                               WHERE b.statb = 'A') THEN 'SH'
                   ELSE '  '
                 END) AS status_descr
         FROM DUAL;

    FETCH l_cv
     INTO l_stat_descr;

    CLOSE l_cv;

    CASE l_stat_descr
      WHEN '  / / / /C/  ' THEN
        l_stat_descr := 'CANCELLED';
      WHEN '  /S/ / / /  ' THEN
        l_stat_descr := 'SUSPENDED';
      ELSE
        NULL;
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_stat_descr);
  EXCEPTION
    WHEN OTHERS THEN
     logs.err(lar_parm);
  END order_detail_status_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEARCH_ORDER_FN
  ||  Returns cursor of searched order header info.
  ||
  ||  This function is called by SearchOrderDS.getData (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/06 | snagbh  | This replaces the original Search_Order_Fn.
  || 12/05/06 | Arun    | Changed the query to be dynamic
  || 12/20/06 | Arun    | Sorted the result set on Status and Order #.
  || 12/12/07 | rhalpai | Added split type as a new parm for selection criteria
  ||                    | and as a column in return cursor. PIR5341
  || 06/05/08 | rhalpai | Changed cursor to use order header status. PIR6364
  || 10/01/10 | rhalpai | Changed cursor to allow search on split type to include
  ||                    | distributions. PIR8859
  || 07/16/12 | rhalpai | Change logic to use MCLP020B for matching customer.
  ||                    | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 11/05/14 | rhalpai | Add order source to returned cursor. PIR12893
  || 06/18/18 | rhalpai | Increase l_split_typ_strng variable definition to
  ||                    | prevent value error. SDHD-321271
  || 08/13/19 | rhalpai | Convert from dynamic SQL to static. SDHD-454665
  || 12/11/25 | rhalpai | Change to just exclude cancel status in Split Type logic. SDHD-2533187
  ||----------------------------------------------------------------------------
  */
  FUNCTION search_order_fn(
    i_ord_stat                 IN  VARCHAR2,
    i_ord_typ                  IN  VARCHAR2,
    i_store_num                IN  VARCHAR2,
    i_crp_cd                   IN  NUMBER,
    i_conf_num                 IN  VARCHAR2,
    i_ord_num                  IN  VARCHAR2,
    i_cust_id                  IN  VARCHAR2,
    i_mcl_cust                 IN  VARCHAR2,
    i_ser_num                  IN  VARCHAR2,
    i_po_num                   IN  VARCHAR2,
    i_llr_dt_from              IN  VARCHAR2,
    i_llr_dt_to                IN  VARCHAR2,
    i_item                     IN  VARCHAR2,
    i_item_typ                 IN  VARCHAR2,
    i_incl_hist_sw             IN  VARCHAR2,
    i_div                      IN  VARCHAR2,
    i_exact_ord_match_sw       IN  VARCHAR2,
    i_exact_conf_num_match_sw  IN  VARCHAR2,
    i_split_typ                IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module        CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.SEARCH_ORDER_FN';
    lar_parm                   logs.tar_parm;
    l_c_cbr_cust_len  CONSTANT PLS_INTEGER   := 8;   -- CBR Customer Number Length
    l_c_mcl_num_len   CONSTANT PLS_INTEGER   := 6;   -- both McLane Customer Number and McLane Item No are 6 digits
    l_c_cbr_item_len  CONSTANT PLS_INTEGER   := 9;   -- CBR Item Number Length
    l_t_hist                   type_stab;
    l_cv                       SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'StoreNum', i_store_num);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'SerNum', i_ser_num);
    logs.add_parm(lar_parm, 'PO', i_po_num);
    logs.add_parm(lar_parm, 'LlrDtFrom', i_llr_dt_from);
    logs.add_parm(lar_parm, 'LlrDtTo', i_llr_dt_to);
    logs.add_parm(lar_parm, 'Item', i_item);
    logs.add_parm(lar_parm, 'ItemTyp', i_item_typ);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ExactOrdMatchSw', i_exact_ord_match_sw);
    logs.add_parm(lar_parm, 'ExactConfNumMatchSw', i_exact_conf_num_match_sw);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.dbg('ENTRY', lar_parm);

    IF (    i_incl_hist_sw = 'Y'
        AND NVL(i_ord_stat, '~') <> 'UNBILLED') THEN
      logs.dbg('Get History Data');

      SELECT x.mcl_cust
             || '~'
             || x.ord_typ
             || '~'
             || x.po_num
             || '~'
             || x.ord_rcvd_ts
             || '~'
             || x.conf_num
             || '~'
             || x.ord_num
             || '~'
             || x.stat_descr
             || '~'
             || x.tbl
             || '~'
             || TO_CHAR(x.ship_dt, 'YYYY-MM-DD')
             || '~'
             || x.maint_user
             || '~'
             || x.test_ord
             || '~'
             || x.has_excptn
             || '~'
             || x.corp_cd
             || '~'
             || x.hdr_stat
             || '~'
             || x.split_typ
             || '~'
             || x.ord_src
             || '~'
             || x.pre_post_sw AS dat
      BULK COLLECT INTO l_t_hist
        FROM (SELECT cx.mccusb AS mcl_cust, a.dsorda AS ord_typ, a.cpoa AS po_num,
                     TO_CHAR(a.ord_rcvd_ts, 'MM/DD/YYYY HH24:MI:SS') AS ord_rcvd_ts, a.connba AS conf_num,
                     a.ordnoa AS ord_num,
                     (CASE
                        WHEN(    a.stata = 'C'
                             AND NOT EXISTS(SELECT 1
                                              FROM ordp920b b
                                             WHERE b.div_part = a.div_part
                                               AND b.ordnob = a.ordnoa
                                               AND b.statb <> 'C')
                            ) THEN 'CANCELLED'
                        WHEN(    a.stata = 'S'
                             AND NOT EXISTS(SELECT 1
                                              FROM ordp920b b
                                             WHERE b.div_part = a.div_part
                                               AND b.ordnob = a.ordnoa
                                               AND b.statb <> 'S')
                            ) THEN 'SUSPENDED'
                        ELSE (CASE
                                WHEN(    a.stata = 'I'
                                     AND TRIM(a.mntusa) IS NOT NULL) THEN 'IC/'
                                WHEN a.stata = 'I' THEN 'MF/'
                                WHEN EXISTS(SELECT 1
                                              FROM ordp920b b
                                             WHERE b.div_part = a.div_part
                                               AND b.ordnob = a.ordnoa
                                               AND b.statb = 'I') THEN 'MF/'
                                ELSE '  /'
                              END
                             )
                             ||(CASE
                                  WHEN a.stata = 'C' THEN 'S/'
                                  WHEN EXISTS(SELECT 1
                                                FROM ordp920b b
                                               WHERE b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND b.statb = 'S') THEN 'S/'
                                  ELSE ' /'
                                END
                               )
                             ||(CASE
                                  WHEN EXISTS(SELECT 1
                                                FROM ordp920b b
                                               WHERE b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND b.statb = 'O') THEN 'U/'
                                  ELSE ' /'
                                END
                               )
                             ||(CASE
                                  WHEN a.stata IN('P', 'R') THEN 'B/'
                                  WHEN EXISTS(SELECT 1
                                                FROM ordp920b b
                                               WHERE b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND b.statb IN('P', 'R')) THEN 'B/'
                                  ELSE ' /'
                                END
                               )
                             ||(CASE
                                  WHEN a.stata = 'C' THEN 'C/'
                                  WHEN EXISTS(SELECT 1
                                                FROM ordp920b b
                                               WHERE b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND b.statb = 'C') THEN 'C/'
                                  ELSE ' /'
                                END
                               )
                             ||(CASE
                                  WHEN a.stata = 'A' THEN 'SH'
                                  WHEN EXISTS(SELECT 1
                                                FROM ordp920b b
                                               WHERE b.div_part = a.div_part
                                                 AND b.ordnob = a.ordnoa
                                                 AND b.statb = 'A') THEN 'SH'
                                  ELSE '  '
                                END
                               )
                      END
                     ) AS stat_descr,
                     2 AS tbl, DATE '1900-02-28' + a.shpja AS ship_dt, NULL AS maint_user,
                     DECODE(a.dsorda, 'T', 'T') AS test_ord,
                     (CASE
                        WHEN EXISTS(SELECT 1
                                      FROM ordp920b b
                                     WHERE b.div_part = a.div_part
                                       AND b.ordnob = a.ordnoa
                                       AND b.excptn_sw = 'Y'
                                       AND b.statb <> 'C') THEN 'Y'
                        ELSE 'N'
                      END
                     ) AS has_excptn,
                     cx.corpb AS corp_cd, a.stata AS hdr_stat,
                     (SELECT DISTINCT FIRST_VALUE(sd.split_typ) OVER(PARTITION BY sd.split_typ ORDER BY sd.priorty)
                                 FROM split_dmn_op8s sd
                                WHERE sd.split_typ = DECODE(i_split_typ, NULL, sd.split_typ, i_split_typ)
                                  AND (   EXISTS(SELECT 1
                                                   FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e,
                                                        ordp920b b
                                                  WHERE s.split_typ = sd.split_typ
                                                    AND s.div_part = a.div_part
                                                    AND s.div_part = si.div_part
                                                    AND s.cbr_vndr_id = si.cbr_vndr_id
                                                    AND si.item_num = e.iteme
                                                    AND si.uom = e.uome
                                                    AND b.div_part = a.div_part
                                                    AND b.ordnob = a.ordnoa
                                                    AND e.catite IN(b.orgitb, b.orditb)
                                                    AND b.excptn_sw = 'N'
                                                    AND b.statb <> 'C'
                                                    AND b.subrcb = 0)
                                       OR EXISTS(SELECT 1
                                                   FROM split_sta_itm_op1s s, ordp920b b, mclp030c mc
                                                  WHERE s.split_typ = sd.split_typ
                                                    AND mc.div_part = a.div_part
                                                    AND mc.custc = a.custa
                                                    AND b.div_part = a.div_part
                                                    AND b.ordnob = a.ordnoa
                                                    AND s.state_cd = mc.taxjrc
                                                    AND s.mcl_item IN(b.orgitb, b.orditb)
                                                    AND b.excptn_sw = 'N'
                                                    AND b.statb <> 'C'
                                                    AND b.subrcb = 0)
                                       OR EXISTS(SELECT 1
                                                   FROM split_cus_itm_op1c s, ordp920b b
                                                  WHERE s.split_typ = sd.split_typ
                                                    AND s.div_part = a.div_part
                                                    AND s.cbr_cust = a.custa
                                                    AND b.div_part = a.div_part
                                                    AND b.ordnob = a.ordnoa
                                                    AND s.mcl_item IN(b.orgitb, b.orditb)
                                                    AND b.excptn_sw = 'N'
                                                    AND b.statb <> 'C'
                                                    AND b.subrcb = 0)
                                      )) AS split_typ,
                     a.ipdtsa AS ord_src,
                     DECODE((SELECT c.tclscc
                               FROM sysp200c c
                              WHERE c.div_part = a.div_part
                                AND c.acnoc = a.custa), 'PRP', 'Y', 'N') AS pre_post_sw
                FROM div_mstr_di1d d, ordp900a a, mclp020b cx
               WHERE d.div_id = i_div
                 AND a.div_part = d.div_part
                 AND (   i_llr_dt_from IS NULL
                      OR a.ctofda >= TO_DATE(i_llr_dt_from, 'MM/DD/YYYY') - DATE '1900-02-28')
                 AND (   i_llr_dt_to IS NULL
                      OR a.ctofda <= TO_DATE(i_llr_dt_to, 'MM/DD/YYYY') - DATE '1900-02-28')
                 AND a.stata =(CASE
                                 WHEN i_ord_stat IS NULL THEN a.stata
                                 WHEN i_ord_stat = 'ALL' THEN a.stata
                                 WHEN(    i_ord_stat = 'UNBILLED'
                                      AND a.stata IN('O', 'S')) THEN a.stata
                                 WHEN(    i_ord_stat = 'BILLED'
                                      AND a.stata IN('P', 'R', 'A')) THEN a.stata
                                 ELSE i_ord_stat
                               END
                              )
                 AND a.dsorda = DECODE(TRIM(i_ord_typ), NULL, a.dsorda, i_ord_typ)
                 AND (   i_ord_num IS NULL
                      OR (    i_exact_ord_match_sw = 'Y'
                          AND a.ordnoa = i_ord_num)
                      OR (    i_exact_ord_match_sw = 'N'
                          AND a.ordnoa LIKE TRIM(i_ord_num) || '%')
                     )
                 AND (   i_conf_num IS NULL
                      OR (    i_exact_conf_num_match_sw = 'Y'
                          AND a.connba = i_conf_num)
                      OR (    i_exact_conf_num_match_sw = 'N'
                          AND a.connba LIKE i_conf_num || '%')
                     )
                 AND (   i_cust_id IS NULL
                      OR (    LENGTH(i_cust_id) = l_c_cbr_cust_len
                          AND a.custa = i_cust_id)
                      OR (    LENGTH(i_cust_id) < l_c_cbr_cust_len
                          AND a.custa LIKE i_cust_id || '%')
                     )
                 AND (   i_ser_num IS NULL
                      OR a.telsla LIKE i_ser_num || '%')
                 AND (   i_po_num IS NULL
                      OR UPPER(a.cpoa) LIKE UPPER(i_po_num) || '%')
                 AND cx.div_part = a.div_part
                 AND cx.custb = a.custa
                 AND (   i_mcl_cust IS NULL
                      OR (    LENGTH(i_mcl_cust) = l_c_mcl_num_len
                          AND cx.mccusb = i_mcl_cust)
                      OR (    LENGTH(i_mcl_cust) < l_c_mcl_num_len
                          AND cx.mccusb LIKE i_mcl_cust || '%')
                     )
                 AND (   i_store_num IS NULL
                      OR cx.storeb LIKE i_store_num || '%')
                 AND (   i_crp_cd IS NULL
                      OR cx.corpb = i_crp_cd)
                 AND (   i_item IS NULL
                      OR EXISTS(SELECT 1
                                  FROM ordp920b b
                                 WHERE b.div_part = a.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND (   (    i_item_typ = 'CBRITEM'
                                            AND LENGTH(i_item) = l_c_cbr_item_len
                                            AND b.itemnb = i_item
                                           )
                                        OR (    i_item_typ = 'CBRITEM'
                                            AND LENGTH(i_item) <> l_c_cbr_item_len
                                            AND b.itemnb LIKE i_item || '%'
                                           )
                                        OR (    i_item_typ = 'MCLANEITEM'
                                            AND LENGTH(i_item) = l_c_mcl_num_len
                                            AND b.orditb = i_item
                                           )
                                        OR (    i_item_typ = 'MCLANEITEM'
                                            AND LENGTH(i_item) <> l_c_mcl_num_len
                                            AND b.orditb LIKE i_item || '%'
                                           )
                                        OR (    i_item_typ = 'CUSTITEM'
                                            AND b.cusitb LIKE i_item || '%')
                                       ))
                     )
                 AND (   i_split_typ IS NULL
                      OR EXISTS(SELECT 1
                                  FROM split_ord_op2s s
                                 WHERE s.split_typ = i_split_typ
                                   AND s.div_part = a.div_part
                                   AND s.ord_num = a.ordnoa)
                      OR EXISTS(SELECT 1
                                  FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp920b b
                                 WHERE s.split_typ = i_split_typ
                                   AND s.div_part = d.div_part
                                   AND s.div_part = si.div_part
                                   AND s.cbr_vndr_id = si.cbr_vndr_id
                                   AND si.item_num = e.iteme
                                   AND si.uom = e.uome
                                   AND b.div_part = a.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND e.catite IN(b.orgitb, b.orditb)
                                   AND b.excptn_sw = 'N'
                                   AND b.statb <> 'C'
                                   AND b.subrcb = 0)
                      OR EXISTS(SELECT 1
                                  FROM split_sta_itm_op1s s, ordp920b b, mclp030c mc
                                 WHERE s.split_typ = i_split_typ
                                   AND b.div_part = d.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND mc.div_part = a.div_part
                                   AND mc.custc = a.custa
                                   AND s.state_cd = mc.taxjrc
                                   AND s.mcl_item IN(b.orgitb, b.orditb)
                                   AND b.excptn_sw = 'N'
                                   AND b.statb <> 'C'
                                   AND b.subrcb = 0)
                      OR EXISTS(SELECT 1
                                  FROM split_cus_itm_op1c s, ordp920b b
                                 WHERE s.split_typ = i_split_typ
                                   AND s.div_part = d.div_part
                                   AND b.div_part = a.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND s.cbr_cust = a.custa
                                   AND s.mcl_item IN(b.orgitb, b.orditb)
                                   AND b.excptn_sw = 'N'
                                   AND b.statb <> 'C'
                                   AND b.subrcb = 0)
                     )) x;
    END IF;   -- (    i_incl_hist_sw = 'Y'

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   cx.mccusb AS mcl_cust, a.dsorda AS ord_typ, a.cpoa AS po_num,
                TO_CHAR(a.ord_rcvd_ts, 'MM/DD/YYYY HH24:MI:SS') AS ord_rcvd_ts, a.connba AS conf_num,
                a.ordnoa AS ord_num,
                (CASE
                   WHEN(    a.stata = 'C'
                        AND NOT EXISTS(SELECT 1
                                         FROM ordp120b b
                                        WHERE b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb <> 'C')
                       ) THEN 'CANCELLED'
                   WHEN(    a.stata = 'S'
                        AND NOT EXISTS(SELECT 1
                                         FROM ordp120b b
                                        WHERE b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb <> 'S')
                       ) THEN 'SUSPENDED'
                   ELSE (CASE
                           WHEN(    a.stata = 'I'
                                AND TRIM(a.mntusa) IS NOT NULL) THEN 'IC/'
                           WHEN a.stata = 'I' THEN 'MF/'
                           WHEN EXISTS(SELECT 1
                                         FROM ordp120b b
                                        WHERE b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb = 'I') THEN 'MF/'
                           ELSE '  /'
                         END
                        )
                        ||(CASE
                             WHEN a.stata = 'C' THEN 'S/'
                             WHEN EXISTS(SELECT 1
                                           FROM ordp120b b
                                          WHERE b.div_part = a.div_part
                                            AND b.ordnob = a.ordnoa
                                            AND b.statb = 'S') THEN 'S/'
                             ELSE ' /'
                           END
                          )
                        ||(CASE
                             WHEN EXISTS(SELECT 1
                                           FROM ordp120b b
                                          WHERE b.div_part = a.div_part
                                            AND b.ordnob = a.ordnoa
                                            AND b.statb = 'O') THEN 'U/'
                             ELSE ' /'
                           END
                          )
                        ||(CASE
                             WHEN a.stata IN('P', 'R') THEN 'B/'
                             WHEN EXISTS(SELECT 1
                                           FROM ordp120b b
                                          WHERE b.div_part = a.div_part
                                            AND b.ordnob = a.ordnoa
                                            AND b.statb IN('P', 'R')) THEN 'B/'
                             ELSE ' /'
                           END
                          )
                        ||(CASE
                             WHEN a.stata = 'C' THEN 'C/'
                             WHEN EXISTS(SELECT 1
                                           FROM ordp120b b
                                          WHERE b.div_part = a.div_part
                                            AND b.ordnob = a.ordnoa
                                            AND b.statb = 'C') THEN 'C/'
                             ELSE ' /'
                           END
                          )
                        ||(CASE
                             WHEN a.stata = 'A' THEN 'SH'
                             WHEN EXISTS(SELECT 1
                                           FROM ordp120b b
                                          WHERE b.div_part = a.div_part
                                            AND b.ordnob = a.ordnoa
                                            AND b.statb = 'A') THEN 'SH'
                             ELSE '  '
                           END
                          )
                 END
                ) AS stat_descr,
                DECODE(a.excptn_sw, 'N', 0, 'Y', 1) AS tbl, DATE '1900-02-28' + a.shpja AS ship_dt,
                a.mntusa AS maint_user, DECODE(a.dsorda, 'T', 'T') AS test_ord,
                (CASE
                   WHEN EXISTS(SELECT 1
                                 FROM ordp120b b
                                WHERE b.div_part = a.div_part
                                  AND b.ordnob = a.ordnoa
                                  AND b.excptn_sw = 'Y'
                                  AND b.statb IN('O', 'S')) THEN 'Y'
                   ELSE 'N'
                 END
                ) AS has_excptn,
                cx.corpb AS corp_cd, a.stata AS hdr_stat,
                (SELECT DISTINCT FIRST_VALUE(sd.split_typ) OVER(PARTITION BY sd.split_typ ORDER BY sd.priorty)
                            FROM split_dmn_op8s sd
                           WHERE sd.split_typ = DECODE(i_split_typ, NULL, sd.split_typ, i_split_typ)
                             AND (   EXISTS(SELECT 1
                                              FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                             WHERE s.split_typ = sd.split_typ
                                               AND s.div_part = a.div_part
                                               AND s.div_part = si.div_part
                                               AND s.cbr_vndr_id = si.cbr_vndr_id
                                               AND si.item_num = e.iteme
                                               AND si.uom = e.uome
                                               AND b.div_part = a.div_part
                                               AND b.ordnob = a.ordnoa
                                               AND e.catite IN(b.orgitb, b.orditb)
                                               AND b.excptn_sw = 'N'
                                               AND b.statb <> 'C'
                                               AND b.subrcb = 0)
                                  OR EXISTS(SELECT 1
                                              FROM split_sta_itm_op1s s, ordp120b b, mclp030c mc
                                             WHERE s.split_typ = sd.split_typ
                                               AND mc.div_part = a.div_part
                                               AND mc.custc = a.custa
                                               AND b.div_part = a.div_part
                                               AND b.ordnob = a.ordnoa
                                               AND s.state_cd = mc.taxjrc
                                               AND s.mcl_item IN(b.orgitb, b.orditb)
                                               AND b.excptn_sw = 'N'
                                               AND b.statb <> 'C'
                                               AND b.subrcb = 0)
                                  OR EXISTS(SELECT 1
                                              FROM split_cus_itm_op1c s, ordp120b b
                                             WHERE s.split_typ = sd.split_typ
                                               AND s.div_part = a.div_part
                                               AND s.cbr_cust = a.custa
                                               AND b.div_part = a.div_part
                                               AND b.ordnob = a.ordnoa
                                               AND s.mcl_item IN(b.orgitb, b.orditb)
                                               AND b.excptn_sw = 'N'
                                               AND b.statb <> 'C'
                                               AND b.subrcb = 0)
                                 )) AS split_typ,
                a.ipdtsa AS ord_src,
                DECODE((SELECT c.tclscc
                          FROM sysp200c c
                         WHERE c.div_part = a.div_part
                           AND c.acnoc = a.custa), 'PRP', 'Y', 'N') AS pre_post_sw
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND (   i_llr_dt_from IS NULL
                 OR ld.llr_dt >= TO_DATE(i_llr_dt_from, 'MM/DD/YYYY'))
            AND (   i_llr_dt_to IS NULL
                 OR ld.llr_dt <= TO_DATE(i_llr_dt_to, 'MM/DD/YYYY'))
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.stata =(CASE
                            WHEN i_ord_stat IS NULL THEN a.stata
                            WHEN i_ord_stat = 'ALL' THEN a.stata
                            WHEN(    i_ord_stat = 'UNBILLED'
                                 AND a.stata IN('O', 'S')) THEN a.stata
                            WHEN(    i_ord_stat = 'BILLED'
                                 AND a.stata IN('P', 'R', 'A')) THEN a.stata
                            ELSE i_ord_stat
                          END
                         )
            AND a.dsorda = DECODE(TRIM(i_ord_typ), NULL, a.dsorda, i_ord_typ)
            AND (   i_ord_num IS NULL
                 OR (    i_exact_ord_match_sw = 'Y'
                     AND a.ordnoa = i_ord_num)
                 OR (    i_exact_ord_match_sw = 'N'
                     AND a.ordnoa LIKE TRIM(i_ord_num) || '%')
                )
            AND (   i_conf_num IS NULL
                 OR (    i_exact_conf_num_match_sw = 'Y'
                     AND a.connba = i_conf_num)
                 OR (    i_exact_conf_num_match_sw = 'N'
                     AND a.connba LIKE i_conf_num || '%')
                )
            AND (   i_cust_id IS NULL
                 OR (    LENGTH(i_cust_id) = l_c_cbr_cust_len
                     AND a.custa = i_cust_id)
                 OR (    LENGTH(i_cust_id) < l_c_cbr_cust_len
                     AND a.custa LIKE i_cust_id || '%')
                )
            AND (   i_ser_num IS NULL
                 OR a.telsla LIKE i_ser_num || '%')
            AND (   i_po_num IS NULL
                 OR UPPER(a.cpoa) LIKE UPPER(i_po_num) || '%')
            AND cx.div_part = a.div_part
            AND cx.custb = a.custa
            AND (   i_mcl_cust IS NULL
                 OR (    LENGTH(i_mcl_cust) = l_c_mcl_num_len
                     AND cx.mccusb = i_mcl_cust)
                 OR (    LENGTH(i_mcl_cust) < l_c_mcl_num_len
                     AND cx.mccusb LIKE i_mcl_cust || '%')
                )
            AND (   i_store_num IS NULL
                 OR cx.storeb LIKE i_store_num || '%')
            AND (   i_crp_cd IS NULL
                 OR cx.corpb = i_crp_cd)
            AND (   i_item IS NULL
                 OR EXISTS(SELECT 1
                             FROM ordp120b b
                            WHERE b.div_part = a.div_part
                              AND b.ordnob = a.ordnoa
                              AND (   (    i_item_typ = 'CBRITEM'
                                       AND LENGTH(i_item) = l_c_cbr_item_len
                                       AND b.itemnb = i_item
                                      )
                                   OR (    i_item_typ = 'CBRITEM'
                                       AND LENGTH(i_item) <> l_c_cbr_item_len
                                       AND b.itemnb LIKE i_item || '%'
                                      )
                                   OR (    i_item_typ = 'MCLANEITEM'
                                       AND LENGTH(i_item) = l_c_mcl_num_len
                                       AND b.orditb = i_item
                                      )
                                   OR (    i_item_typ = 'MCLANEITEM'
                                       AND LENGTH(i_item) <> l_c_mcl_num_len
                                       AND b.orditb LIKE i_item || '%'
                                      )
                                   OR (    i_item_typ = 'CUSTITEM'
                                       AND b.cusitb LIKE i_item || '%')
                                  ))
                )
            AND (   i_split_typ IS NULL
                 OR EXISTS(SELECT 1
                             FROM split_ord_op2s s
                            WHERE s.split_typ = i_split_typ
                              AND s.div_part = a.div_part
                              AND s.ord_num = a.ordnoa)
                 OR EXISTS(SELECT 1
                             FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                            WHERE s.split_typ = i_split_typ
                              AND s.div_part = d.div_part
                              AND s.div_part = si.div_part
                              AND s.cbr_vndr_id = si.cbr_vndr_id
                              AND si.item_num = e.iteme
                              AND si.uom = e.uome
                              AND b.div_part = a.div_part
                              AND b.ordnob = a.ordnoa
                              AND e.catite IN(b.orgitb, b.orditb)
                              AND b.excptn_sw = 'N'
                              AND b.statb <> 'C'
                              AND b.subrcb = 0)
                 OR EXISTS(SELECT 1
                             FROM split_sta_itm_op1s s, ordp120b b, mclp030c mc
                            WHERE s.split_typ = i_split_typ
                              AND b.div_part = d.div_part
                              AND b.ordnob = a.ordnoa
                              AND mc.div_part = a.div_part
                              AND mc.custc = a.custa
                              AND s.state_cd = mc.taxjrc
                              AND s.mcl_item IN(b.orgitb, b.orditb)
                              AND b.excptn_sw = 'N'
                              AND b.statb <> 'C'
                              AND b.subrcb = 0)
                 OR EXISTS(SELECT 1
                             FROM split_cus_itm_op1c s, ordp120b b
                            WHERE s.split_typ = i_split_typ
                              AND s.div_part = d.div_part
                              AND b.div_part = a.div_part
                              AND b.ordnob = a.ordnoa
                              AND s.cbr_cust = a.custa
                              AND s.mcl_item IN(b.orgitb, b.orditb)
                              AND b.excptn_sw = 'N'
                              AND b.statb <> 'C'
                              AND b.subrcb = 0)
                )
       UNION ALL
       SELECT   SUBSTR(t.column_value, 1, INSTR(t.column_value, '~') - 1) AS mcl_cust,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 1) + 1,
                       INSTR(t.column_value, '~', 1, 2) - 1 - INSTR(t.column_value, '~', 1, 1)
                      ) AS ord_typ,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 2) + 1,
                       INSTR(t.column_value, '~', 1, 3) - 1 - INSTR(t.column_value, '~', 1, 2)
                      ) AS po_num,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 3) + 1,
                       INSTR(t.column_value, '~', 1, 4) - 1 - INSTR(t.column_value, '~', 1, 3)
                      ) AS ord_rcvd_ts,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 4) + 1,
                       INSTR(t.column_value, '~', 1, 5) - 1 - INSTR(t.column_value, '~', 1, 4)
                      ) AS conf_num,
                TO_NUMBER(SUBSTR(t.column_value,
                                 INSTR(t.column_value, '~', 1, 5) + 1,
                                 INSTR(t.column_value, '~', 1, 6) - 1 - INSTR(t.column_value, '~', 1, 5)
                                )
                         ) AS ord_num,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 6) + 1,
                       INSTR(t.column_value, '~', 1, 7) - 1 - INSTR(t.column_value, '~', 1, 6)
                      ) AS stat_descr,
                TO_NUMBER(SUBSTR(t.column_value,
                                 INSTR(t.column_value, '~', 1, 7) + 1,
                                 INSTR(t.column_value, '~', 1, 8) - 1 - INSTR(t.column_value, '~', 1, 7)
                                )
                         ) AS tbl,
                TO_DATE(SUBSTR(t.column_value,
                               INSTR(t.column_value, '~', 1, 8) + 1,
                               INSTR(t.column_value, '~', 1, 9) - 1 - INSTR(t.column_value, '~', 1, 8)
                              ),
                        'YYYY-MM-DD'
                       ) AS ship_dt,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 9) + 1,
                       INSTR(t.column_value, '~', 1, 10) - 1 - INSTR(t.column_value, '~', 1, 9)
                      ) AS maint_user,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 10) + 1,
                       INSTR(t.column_value, '~', 1, 11) - 1 - INSTR(t.column_value, '~', 1, 10)
                      ) AS test_ord,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 11) + 1,
                       INSTR(t.column_value, '~', 1, 12) - 1 - INSTR(t.column_value, '~', 1, 11)
                      ) AS has_excptn,
                TO_NUMBER(SUBSTR(t.column_value,
                                 INSTR(t.column_value, '~', 1, 12) + 1,
                                 INSTR(t.column_value, '~', 1, 13) - 1 - INSTR(t.column_value, '~', 1, 12)
                                )
                         ) AS corp_cd,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 13) + 1,
                       INSTR(t.column_value, '~', 1, 14) - 1 - INSTR(t.column_value, '~', 1, 13)
                      ) AS hdr_stat,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 14) + 1,
                       INSTR(t.column_value, '~', 1, 15) - 1 - INSTR(t.column_value, '~', 1, 14)
                      ) AS split_typ,
                SUBSTR(t.column_value,
                       INSTR(t.column_value, '~', 1, 15) + 1,
                       INSTR(t.column_value, '~', 1, 16) - 1 - INSTR(t.column_value, '~', 1, 15)
                      ) AS ord_src,
                SUBSTR(t.column_value, INSTR(t.column_value, '~', 1, 16) + 1) AS pre_post_sw
           FROM TABLE(l_t_hist) t
       ORDER BY stat_descr, ord_num DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END search_order_fn;

  /*
  ||----------------------------------------------------------------------------
  || HIST_DETAIL_FN
  ||  Retrieve a cursor of the order detail lines from history for an order.
  ||
  ||  This function is called by OrderManagerDS.getDetailFromHistoryWell (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 01/21/06 | snagabh | Changes to display Cancel and Suspend reason for all
  ||                      detail lines
  || 06/20/06 | rhalpai | Changed to include latest log info for order line
  ||                    | when not-ship reason is NULL. IM244358
  || 02/19/07 | Arun    | Temporary solution to add MAX to projection
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION hist_detail_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER
  )
    RETURN SYS_REFCURSOR IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.HIST_DETAIL_FN';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_cv                    SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
      WITH x AS
           (SELECT sa.linea AS ord_ln,
                   TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS') + sa.datea AS ts,
                   sa.autbya AS auth_by,
                   (CASE
                      WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                      ELSE (SELECT ma.desca
                              FROM mclp140a ma
                             WHERE ma.rsncda = sa.rsncda)
                    END
                   ) AS rsn
              FROM sysp996a sa
             WHERE sa.div_part = l_div_part
               AND sa.ordnoa = i_ord_num
               AND sa.linea > 0
            UNION ALL
            SELECT d.ordlnd, d.last_chg_ts AS ts, '' AS auth_by, ma.desca AS rsn
              FROM mclp900d d, ordp920b b, mclp140a ma
             WHERE d.div_part = l_div_part
               AND d.ordnod = i_ord_num
               AND b.ordnob = d.ordnod
               AND b.lineb = d.ordlnd
               AND b.div_part = d.div_part
               AND ma.rsncda = d.reasnd
               AND (   b.excptn_sw = 'Y'
                    OR ma.info_sw = 'Y')),
           lg2 AS
           (SELECT lg.ord_ln, lg.ts, lg.auth_by, lg.rsn
              FROM x lg
             WHERE lg.ts = (SELECT MAX(x2.ts)
                              FROM x x2
                             WHERE x2.ord_ln = lg.ord_ln))
       SELECT   b.itemnb, b.ordqtb, b.pckqtb, e.shppke, b.orgitb, b.hdprcb, b.hdrtmb, b.hdrtab, NULL, b.maxqtb,
                b.statb, b.lineb, 0, NULL, NULL, b.itpasb, b.rtfixb, b.prfixb, b.hdprcb, b.bymaxb, NULL AS rstfeb,
                b.manctb, b.totctb, (SELECT MAX(lg2.auth_by)
                                       FROM lg2
                                      WHERE lg2.ord_ln = b.lineb) AS auth_by, b.prstdb, b.prsttb, NULL AS resgpb,
                b.qtmulb, b.invctb, b.labctb, b.cusitb, NULL, b.sllumb, b.orditb, a.orrtea, a.stopsa,
                (SELECT MAX(lg2.rsn)
                   FROM lg2
                  WHERE lg2.ord_ln = b.lineb) AS log_rsn, e.ctdsce, e.sizee, '', e2.ctdsce, b.orgqtb, b.alcqtb,
                b.pckqtb, (SELECT MAX(ma.desca)
                             FROM mclp140a ma
                            WHERE ma.rsncda = b.ntshpb) AS not_ship_rsn, b.actqtb, b.subrcb, 3 AS order_well
           FROM ordp900a a, ordp920b b, sawp505e e, sawp505e e2
          WHERE a.div_part = l_div_part
            AND a.ordnoa = i_ord_num
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND e.catite(+) = b.orditb
            AND e2.catite(+) = b.orgitb
       ORDER BY 12;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END hist_detail_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DETAIL_FN
  ||  Retrieve a cursor of the order detail lines for an order.
  ||
  ||  This function is called by OrderManagerDS.getDetailFromOrderWell (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 12/25/05 | snagabh | Several changes to minimize number of SQLs and functions
  ||                      that return Order Detail information.
  || 03/06/06 | snagabh | Changed to work with new confirmation number format.
  || 06/20/06 | rhalpai | Changed to include latest log info for order line
  ||                    | when not-ship reason is NULL. IM244358
  || 02/19/07 | Arun    | Temporary solution to add MAX to projection
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_detail_fn(
    i_div              IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_maint_mode_sw    IN  VARCHAR2,
    i_excptns_only_sw  IN  VARCHAR2 DEFAULT 'N',
    i_conf_num         IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.ORD_DETAIL_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'MaintModeSw', i_maint_mode_sw);
    logs.add_parm(lar_parm, 'ExcptnsOnlySw', i_excptns_only_sw);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);

    ----------------------------------------------------------------
    -- Include the Order Well from where the data is being returned.
    -- 0 : Good Order Well
    -- 1 : Exceptions Order Well
    ----------------------------------------------------------------
    OPEN l_cv
     FOR
       SELECT   b.itemnb, b.ordqtb, b.pckqtb, e.shppke, b.orgitb, b.hdprcb, b.hdrtmb, b.hdrtab, NULL, b.maxqtb,
                b.statb, b.lineb, 0, NULL, NULL, b.itpasb, b.rtfixb, b.prfixb, b.hdprcb, b.bymaxb, NULL AS rstfeb,
                b.manctb, b.totctb, lg.auth_by, b.prstdb, b.prsttb, NULL AS resgpb, b.qtmulb, b.invctb, b.labctb,
                b.cusitb, NULL, b.sllumb, b.orditb, ld.load_num, NVL(se.stop_num, 0) AS stop_num, lg.rsn AS log_rsn,
                e.ctdsce, e.sizee, '', e2.ctdsce, b.orgqtb, b.alcqtb, b.pckqtb,
                (SELECT MAX(ma.desca)
                   FROM mclp140a ma
                  WHERE ma.rsncda = b.ntshpb) AS not_ship_rsn, b.actqtb, b.subrcb, 0 AS order_well
           FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se, ordp120b b, sawp505e e, sawp505e e2,
                (SELECT DISTINCT x.ord_ln,
                                 FIRST_VALUE(x.auth_by) OVER(PARTITION BY x.ord_ln ORDER BY x.ts DESC) AS auth_by,
                                 FIRST_VALUE(x.rsn) OVER(PARTITION BY x.ord_ln ORDER BY x.ts DESC) AS rsn
                            FROM (SELECT sa.linea AS ord_ln,
                                         TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS')
                                         + sa.datea AS ts,
                                         sa.autbya AS auth_by,
                                         (CASE
                                            WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                                            ELSE (SELECT ma.desca
                                                    FROM mclp140a ma
                                                   WHERE ma.rsncda = sa.rsncda)
                                          END
                                         ) AS rsn
                                    FROM sysp296a sa
                                   WHERE sa.div_part = l_div_part
                                     AND sa.ordnoa = l_ord_num
                                     AND sa.linea > 0
                                  UNION ALL
                                  SELECT d.ordlnd, d.last_chg_ts AS ts, '' AS auth_by, ma.desca AS rsn
                                    FROM mclp300d d, mclp140a ma
                                   WHERE d.div_part = l_div_part
                                     AND d.ordnod = l_ord_num
                                     AND d.ordlnd > 0
                                     AND ma.rsncda = d.reasnd
                                     AND 'Y' =(CASE
                                                 WHEN EXISTS(
                                                       SELECT 1
                                                         FROM ordp120b b
                                                        WHERE b.div_part = d.div_part
                                                          AND b.ordnob = d.ordnod
                                                          AND b.lineb = d.ordlnd
                                                          AND b.excptn_sw = 'Y') THEN 'Y'
                                                 WHEN ma.info_sw = 'Y' THEN 'Y'
                                                 ELSE 'N'
                                               END
                                              )) x) lg
          WHERE a.div_part = l_div_part
            AND a.ordnoa = l_ord_num
            AND ld.div_part = a.div_part
            AND ld.load_depart_sid = a.load_depart_sid
            AND se.div_part(+) = a.div_part
            AND se.load_depart_sid(+) = a.load_depart_sid
            AND se.cust_id(+) = a.custa
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND (   i_excptns_only_sw = 'N'
                 OR b.excptn_sw = 'Y')
            AND 'Y' =(CASE
                        WHEN i_maint_mode_sw = 'N' THEN 'Y'
                        WHEN b.statb IN('O', 'S') THEN 'Y'
                        WHEN(    a.stata = 'I'
                             AND NOT EXISTS(SELECT 1
                                              FROM ordp120b b2
                                             WHERE b2.div_part = l_div_part
                                               AND b2.ordnob = l_ord_num
                                               AND b2.statb <> 'I')
                            ) THEN 'Y'
                        ELSE 'N'
                      END
                     )
            AND e.catite(+) = b.orditb
            AND e2.catite(+) = b.orgitb
            AND lg.ord_ln(+) = b.lineb
       ORDER BY 12;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_detail_fn;

  /*
  ||----------------------------------------------------------------------------
  || IN_HIST_AS_SHIPPED_FN
  ||  Indicates whether an order is in History with a shipped status.
  ||
  ||  This function is called by OrderManagerDS.getDisplayInfo (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION in_hist_as_shipped_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.IN_HIST_AS_SHIPPED_FN';
    lar_parm             logs.tar_parm;
    l_found_sw           VARCHAR2(1);
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);

    SELECT NVL(MAX('Y'), 'N')
      INTO l_found_sw
      FROM DUAL
     WHERE EXISTS(SELECT 1
                    FROM ordp920b
                   WHERE div_part = i_div_part
                     AND ordnob = i_ord_num
                     AND excptn_sw = 'N'
                     AND statb = 'A');

    RETURN(l_found_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END in_hist_as_shipped_fn;

  /*
  ||----------------------------------------------------------------------------
  || IN_HIST_WITH_HDR_EXCEPTION_FN
  ||  Indicates whether an order is in History with a header-level exception.
  ||  The whole order was in the exception well.
  ||
  ||  This function is called by OrderManagerDS.getDisplayInfo (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION in_hist_with_hdr_exception_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.IN_HIST_WITH_HDR_EXCEPTION_FN';
    lar_parm             logs.tar_parm;
    l_found_sw           VARCHAR2(1);
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);

    SELECT NVL(MAX('Y'), 'N')
      INTO l_found_sw
      FROM ordp900a
     WHERE div_part = i_div_part
       AND ordnoa = i_ord_num
       AND excptn_sw = 'Y';

    RETURN(l_found_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END in_hist_with_hdr_exception_fn;

  /*
  ||----------------------------------------------------------------------------
  || HDR_EXCEPT_FN
  ||  Return a delimited list of header exception reason descriptions.
  ||
  ||  It is possible to have 2 level-one exceptions when a customer has been
  ||  placed "on hold" and the customer's load/stop assignment has been removed.
  ||  i.e.: 1 Customer on Hold~1 No Stop Information Available
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/10/06 | rhalpai | Original IM244358
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  ||----------------------------------------------------------------------------
  */
  FUNCTION hdr_except_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_use_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.HDR_EXCEPT_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_hdr_except         typ.t_maxvc2;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'UseHistSw', i_use_hist_sw);
    logs.dbg('ENTRY', lar_parm);

    IF i_use_hist_sw = 'Y' THEN
      logs.dbg('Open Hist Log Cursor');

      OPEN l_cv
       FOR
         SELECT SUBSTR(MAX(y.str)KEEP (DENSE_RANK FIRST ORDER BY(y.levl) DESC), 2)
           FROM (SELECT     SYS_CONNECT_BY_PATH(x.val, '~') str, LEVEL levl
                       FROM (SELECT ma.desca val, ROWNUM rnum, LAG(ROWNUM, 1) OVER(ORDER BY ma.desca) AS connect_id
                               FROM mclp900d d, mclp140a ma
                              WHERE ma.rsncda = d.reasnd
                                AND d.div_part = i_div_part
                                AND d.ordnod = i_ord_num
                                AND d.ordlnd = 0
                                AND d.exlvld IN('1', '2')
                                AND d.resexd = 0) x
                 START WITH x.connect_id IS NULL
                 CONNECT BY PRIOR x.rnum = x.connect_id) y;
    ELSE
      logs.dbg('Open Current Log Cursor');

      OPEN l_cv
       FOR
         SELECT SUBSTR(MAX(y.str)KEEP (DENSE_RANK FIRST ORDER BY(y.levl) DESC), 2)
           FROM (SELECT     SYS_CONNECT_BY_PATH(x.val, '~') str, LEVEL levl
                       FROM (SELECT ma.desca val, ROWNUM rnum, LAG(ROWNUM, 1) OVER(ORDER BY ma.desca) AS connect_id
                               FROM mclp300d d, mclp140a ma
                              WHERE ma.rsncda = d.reasnd
                                AND d.div_part = i_div_part
                                AND d.ordnod = i_ord_num
                                AND d.ordlnd = 0
                                AND d.exlvld IN('1', '2')
                                AND d.resexd = 0) x
                 START WITH x.connect_id IS NULL
                 CONNECT BY PRIOR x.rnum = x.connect_id) y;
    END IF;

    logs.dbg('Fetch');

    FETCH l_cv
     INTO l_hdr_except;

    RETURN(l_hdr_except);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END hdr_except_fn;

  /*
  ||----------------------------------------------------------------------------
  || EXCEPT_ORDER_HDR_LIST_FN
  ||  Retrieve a cursor of exception order headers.
  ||
  ||  This function is called by OrderManagerDS.getHeaderFromExceptWell (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 03/02/06 | snagabh | Updated to return trndta, trntma and ord_rcvd_ts as a string.
  || 03/07/06 | snagabh | Added additional calls, upd_maint_user_sp(),
  ||                    | and ord_status_desc_fn(). This will reduce the number of
  ||                    | Oracle calls from Java for Order Retrieval.
  || 03/13/06 | snagabh | Pass Confirmation Number to Order_Status_Descr_Fn() function
  ||                    | call. Otherwise new orders with no order number will not
  ||                    | display correct status string (MF/ / / / format).
  || 03/15/06 | snagabh | Modified to call Order_Detail_Status_Fn() instead of order_status_descr_fn.
  || 07/10/06 | rhalpai | Changed cursor to include "header exception",
  ||                    | "status reason" and "auth by" columns. IM244358
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 08/14/12 | rhalpai | Replace references to column for line count on order
  ||                    | header with logic to count order detail lines.
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION except_order_hdr_list_fn(
    i_div            IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_conf_num       IN  VARCHAR2,
    i_maint_mode_sw  IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.EXCEPT_ORDER_HDR_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'MaintModeSw', i_maint_mode_sw);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);

    -- Tag Order as being maintained, if function called with maintenance flag as Y
    IF i_maint_mode_sw = 'Y' THEN
      logs.dbg('Upd Maint UserId: ' || i_user_id || ' for OrdNum: ' || l_ord_num);
      csr_orders_pk.upd_maint_user_sp(i_div, i_user_id, l_ord_num, '', 'Y');
    END IF;

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT a.custa,(CASE
                         WHEN c.acnoc IS NULL THEN '**Invalid Customer**'
                         ELSE c.namec
                       END), TRUNC(se.eta_ts) - DATE '1900-02-28' AS eta_dt, ld.load_num, se.stop_num,
              ld.llr_dt - DATE '1900-02-28' AS llr_dt, TO_NUMBER(TO_CHAR(ld.llr_ts, 'HH24MI')) AS llr_tm,
              TRUNC(se.eta_ts) - DATE '1900-02-28' AS eta_dt, TO_NUMBER(TO_CHAR(se.eta_ts, 'HH24MI')) AS eta_tm,
              d.div_id, NULL, DECODE(a.dsorda, 'N', 'Y', 'N') AS no_ord_sw, a.cpoa, a.ordnoa, a.connba, a.dsorda,
              d.div_id, a.ipdtsa, a.shpja, c.namec, NULL, c.shad1c, c.shad2c, c.shpctc, c.shpstc, c.shpzpc, a.ldtypa,
              a.trndta, LPAD(a.trntma, 6, '0'), NULL, NULL, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac,
              DECODE(a.dsorda, 'T', 'T') AS test_sw, d.div_id,
              (SELECT COUNT(1) AS cnt
                 FROM ordp120b b
                WHERE b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, NULL, a.legrfa, a.stata, cx.mccusb, a.telsla, cx.storeb,
              NVL(cx.corpb, 0), a.pshipa, TO_CHAR(a.ord_rcvd_ts, 'MM/DD/YYYY HH24:MI:SS'),
              csr_orders_pk.order_detail_status_fn(a.div_part, a.ordnoa) AS ord_stat, lg.rsn, lg.auth_by,
              csr_orders_pk.hdr_except_fn(a.div_part, a.ordnoa) AS hdr_except
         FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx,
              (SELECT DISTINCT x.ord_num, FIRST_VALUE(x.auth_by) OVER(ORDER BY x.ts DESC) AS auth_by,
                               FIRST_VALUE(x.rsn) OVER(ORDER BY x.ts DESC) AS rsn
                          FROM (SELECT sa.ordnoa AS ord_num,
                                       TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS')
                                       + sa.datea AS ts,
                                       (CASE
                                          WHEN sa.flchga = 'C' THEN sa.autbya
                                        END) AS auth_by,
                                       (CASE
                                          WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                                          ELSE (SELECT ma.desca
                                                  FROM mclp140a ma
                                                 WHERE ma.rsncda = sa.rsncda)
                                        END
                                       ) AS rsn
                                  FROM sysp296a sa
                                 WHERE sa.div_part = l_div_part
                                   AND sa.ordnoa = l_ord_num
                                   AND (   sa.linea IS NULL
                                        OR sa.linea = 0)
                                   AND EXISTS(SELECT 1
                                                FROM ordp100a a
                                               WHERE a.div_part = sa.div_part
                                                 AND a.ordnoa = sa.ordnoa
                                                 AND a.excptn_sw = 'Y'
                                                 AND a.stata IN('S', 'C'))
                                   AND sa.fldnma = 'STATA'
                                   AND sa.flchga IN('C', 'S')) x) lg
        WHERE d.div_part = l_div_part
          AND a.div_part = d.div_part
          AND a.ordnoa = l_ord_num
          AND a.excptn_sw = 'Y'
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa
          AND c.div_part(+) = a.div_part
          AND c.acnoc(+) = a.custa
          AND cx.div_part(+) = a.div_part
          AND cx.custb(+) = a.custa
          AND lg.ord_num(+) = a.ordnoa;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END except_order_hdr_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || HIST_ORDER_HDR_LIST_FN
  ||  Retrieve a cursor of history order headers.
  ||
  ||  This function is called by OrderManagerDS.getHeaderFromHistoryWell (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 03/02/06 | snagabh | Updated to return trndta, trntma and ord_rcvd_ts as a string.
  || 03/07/06 | snagabh | Added additional calls, In_Hist_As_Shipped_Fn(),
  ||                      In_Hist_With_Hdr_Exception_Fn() and ord_status_desc_fn().
  || 03/15/06 | snagabh | Modified to call Order_Detail_Status_Fn() instead of order_status_descr_fn.
  || 07/10/06 | rhalpai | Changed cursor to include "header exception",
  ||                    | "status reason" and "auth by" columns. IM244358
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  || 08/14/12 | rhalpai | Replace references to column for line count on order
  ||                    | header with logic to count order detail lines.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION hist_order_hdr_list_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER
  )
    RETURN SYS_REFCURSOR IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.HIST_ORDER_HDR_LIST_FN';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_cv                    SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       WITH x AS
           (SELECT sa.ordnoa AS ord_num,
                   TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS') + sa.datea AS ts,
                   (CASE
                      WHEN sa.flchga = 'C' THEN sa.autbya
                    END) AS auth_by,
                   (CASE
                      WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                      ELSE (SELECT ma.desca
                              FROM mclp140a ma
                             WHERE ma.rsncda = sa.rsncda)
                    END
                   ) AS rsn
              FROM sysp996a sa
             WHERE sa.div_part = l_div_part
               AND sa.ordnoa = i_ord_num
               AND (   sa.linea IS NULL
                    OR sa.linea = 0)
               AND EXISTS(SELECT 1
                            FROM ordp900a oa
                           WHERE oa.div_part = sa.div_part
                             AND oa.ordnoa = sa.ordnoa
                             AND oa.stata IN('S', 'C'))
               AND sa.fldnma = 'STATA'
               AND sa.flchga IN('C', 'S'))
       SELECT a.custa, c.namec, a.etadta, a.orrtea, a.stopsa, a.ctofda, a.ctofta, a.etadta, a.etatma, d.div_id, NULL,
              DECODE(a.dsorda, 'N', 'Y', 'N') AS no_ord_sw, a.cpoa, a.ordnoa, a.connba, a.dsorda, d.div_id, a.ipdtsa,
              a.shpja, c.namec, NULL, c.shad1c, c.shad2c, c.shpctc, c.shpstc, c.shpzpc, a.ldtypa, a.trndta,
              LPAD(a.trntma, 6, '0'), NULL, NULL, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac,
              DECODE(a.dsorda, 'T', 'T') AS test_sw, d.div_id,
              (SELECT COUNT(1) AS cnt
                 FROM ordp920b b
                WHERE b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.stata, x.mccusb, a.telsla, x.storeb, x.corpb,
              TO_CHAR(a.ord_rcvd_ts, 'MM/DD/YYYY HH24:MI:SS'),
              (CASE
                 WHEN csr_orders_pk.in_hist_as_shipped_fn(a.div_part, a.ordnoa) = 'Y' THEN 'SHIPPED'
                 WHEN csr_orders_pk.in_hist_with_hdr_exception_fn(a.div_part, a.ordnoa) = 'Y' THEN 'HELD'
                 ELSE csr_orders_pk.order_detail_status_fn(a.div_part, a.ordnoa, '')
               END
              ) AS ord_stat,
              lg2.rsn, lg2.auth_by, hdr_except_fn(a.div_part, a.ordnoa, 'Y') AS hdr_except
         FROM div_mstr_di1d d, ordp900a a, sysp200c c, mclp020b x, (SELECT lg.ord_num, lg.auth_by, lg.rsn
                                                                      FROM x lg
                                                                     WHERE lg.ts = (SELECT MAX(x2.ts)
                                                                                      FROM x x2)) lg2
        WHERE d.div_part = l_div_part
          AND a.div_part = d.div_part
          AND a.ordnoa = i_ord_num
          AND c.div_part(+) = a.div_part
          AND c.acnoc(+) = a.custa
          AND x.div_part(+) = a.div_part
          AND x.custb(+) = a.custa
          AND lg2.ord_num(+) = a.ordnoa;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END hist_order_hdr_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORDER_HDR_LIST_FN
  ||  Retrieve a cursor of order headers.
  ||
  ||  This function is called by OrderManagerDS.getHeaderFromOrderWell (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 03/02/06 | snagabh | Updated to return trndta, trntma and ord_rcvd_ts as a string.
  || 03/07/06 | snagabh | Added additional calls, upd_maint_user_sp(),
  ||                      and ord_status_desc_fn(). This will reduce the number of
  ||                      Oracle calls from Java for Order Retrieval.
  || 03/15/06 | snagabh | Modified to call Order_Detail_Status_Fn() instead of order_status_descr_fn.
  || 07/10/06 | rhalpai | Changed cursor to include "status reason" and
  ||                    | "auth by" columns. IM244358
  || 12/12/07 | rhalpai | Added split type to cursor. PIR5341
  || 06/05/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 10/01/10 | rhalpai | Changed cursor to allow search on split type to include
  ||                    | distributions. PIR8859
  || 11/10/10 | rhalpai | Changed cursor to use PSHIPA to indicate partial ship
  ||                    | is allowed. PIR5878
  || 08/14/12 | rhalpai | Replace references to column for line count on order
  ||                    | header with logic to count order detail lines.
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION order_hdr_list_fn(
    i_div            IN  VARCHAR2,
    i_ord_num        IN  NUMBER,
    i_maint_mode_sw  IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.ORDER_HDR_LIST_FN';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_cv                    SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'MaintModeSw', i_maint_mode_sw);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    -- Tag Order as being maintained, if function called with maintenance flag as Y
    IF i_maint_mode_sw = 'Y' THEN
      logs.dbg('Upd Maint User');
      csr_orders_pk.upd_maint_user_sp(i_div, i_user_id, i_ord_num, '', 'Y');
    END IF;

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       WITH x AS
           (SELECT sa.ordnoa AS ord_num,
                   TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS') + sa.datea AS ts,
                   (CASE
                      WHEN sa.flchga = 'C' THEN sa.autbya
                    END) AS auth_by,
                   (CASE
                      WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                      ELSE (SELECT ma.desca
                              FROM mclp140a ma
                             WHERE ma.rsncda = sa.rsncda)
                    END
                   ) AS rsn
              FROM sysp296a sa
             WHERE sa.div_part = l_div_part
               AND sa.ordnoa = i_ord_num
               AND (   sa.linea IS NULL
                    OR sa.linea = 0)
               AND EXISTS(SELECT 1
                            FROM ordp100a oa
                           WHERE oa.div_part = sa.div_part
                             AND oa.ordnoa = sa.ordnoa
                             AND oa.stata IN('S', 'C'))
               AND sa.fldnma = 'STATA'
               AND sa.flchga IN('C', 'S'))
       SELECT a.custa, c.namec, NVL(TRUNC(se.eta_ts), DATE '1900-01-01') - DATE '1900-02-28' AS eta_dt, ld.load_num,
              NVL(se.stop_num, 0) AS stop_num, ld.llr_dt - DATE '1900-02-28' AS llr_dt,
              TO_NUMBER(TO_CHAR(ld.llr_ts, 'HH24MI')) AS llr_tm,
              NVL(TRUNC(se.eta_ts), DATE '1900-01-01') - DATE '1900-02-28' AS eta_dt,
              NVL(TO_NUMBER(TO_CHAR(se.eta_ts, 'HH24MI')), 0) AS eta_tm, d.div_id, NULL,
              DECODE(a.dsorda, 'N', 'Y', 'N') AS no_ord_sw, a.cpoa, a.ordnoa, a.connba, a.dsorda, d.div_id, a.ipdtsa,
              a.shpja, c.namec, NULL AS lnames, c.shad1c, c.shad2c, c.shpctc, c.shpstc, c.shpzpc, a.ldtypa, a.trndta,
              LPAD(a.trntma, 6, '0'), NULL, NULL, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac,
              DECODE(a.dsorda, 'T', 'T') AS test_sw, d.div_id,
              (SELECT COUNT(1) AS cnt
                 FROM ordp120b b
                WHERE b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.stata, cx.mccusb, a.telsla, cx.storeb, cx.corpb,
              a.legrfa, a.pshipa, TO_CHAR(a.ord_rcvd_ts, 'MM/DD/YYYY HH24:MI:SS'),
              csr_orders_pk.order_detail_status_fn(a.div_part, a.ordnoa, '') AS ord_stat, lg2.rsn, lg2.auth_by,
              (SELECT DISTINCT FIRST_VALUE(sd.split_typ) OVER(PARTITION BY sd.split_typ ORDER BY sd.priorty)
                          FROM split_dmn_op8s sd
                         WHERE (   EXISTS(SELECT 1
                                            FROM split_div_vnd_op3s s, strct_item_op3v si, sawp505e e, ordp120b b
                                           WHERE s.div_part = a.div_part
                                             AND s.split_typ = sd.split_typ
                                             AND si.div_part = s.div_part
                                             AND si.cbr_vndr_id = s.cbr_vndr_id
                                             AND si.item_num = e.iteme
                                             AND si.uom = e.uome
                                             AND b.div_part = a.div_part
                                             AND b.ordnob = a.ordnoa
                                             AND e.catite IN(b.orgitb, b.orditb)
                                             AND b.statb = 'O'
                                             AND b.subrcb = 0)
                                OR EXISTS(SELECT 1
                                            FROM split_sta_itm_op1s s, ordp120b b, mclp030c mc
                                           WHERE s.split_typ = sd.split_typ
                                             AND mc.div_part = a.div_part
                                             AND mc.custc = a.custa
                                             AND b.div_part = a.div_part
                                             AND b.ordnob = a.ordnoa
                                             AND s.state_cd = mc.taxjrc
                                             AND s.mcl_item IN(b.orgitb, b.orditb)
                                             AND b.statb = 'O'
                                             AND b.subrcb = 0)
                                OR EXISTS(SELECT 1
                                            FROM split_cus_itm_op1c s, ordp120b b
                                           WHERE s.div_part = a.div_part
                                             AND s.split_typ = sd.split_typ
                                             AND s.cbr_cust = a.custa
                                             AND b.div_part = a.div_part
                                             AND b.ordnob = a.ordnoa
                                             AND s.mcl_item IN(b.orgitb, b.orditb)
                                             AND b.statb = 'O'
                                             AND b.subrcb = 0)
                               )) AS split_typ
         FROM div_mstr_di1d d, ordp100a a, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx,
              (SELECT lg.ord_num, lg.auth_by, lg.rsn
                 FROM x lg
                WHERE lg.ts = (SELECT MAX(x2.ts)
                                 FROM x x2)) lg2
        WHERE d.div_part = l_div_part
          AND a.div_part = d.div_part
          AND a.ordnoa = i_ord_num
          AND a.excptn_sw = 'N'
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND se.div_part(+) = a.div_part
          AND se.load_depart_sid(+) = a.load_depart_sid
          AND se.cust_id(+) = a.custa
          AND c.div_part = a.div_part
          AND c.acnoc = a.custa
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND lg2.ord_num(+) = a.ordnoa
          AND (   i_maint_mode_sw = 'N'
               OR (    a.stata IN('O', 'I', 'S')
                   AND NOT EXISTS(SELECT 1
                                    FROM mclpinpr r
                                   WHERE r.div_part = l_div_part
                                     AND r.ordnor = i_ord_num))
              );

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END order_hdr_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_BILLED_FN
  ||  Indicate whether an order contains order lines in billed status.
  ||
  ||  This function is called by OrderManagerDS.isOrderBilled (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 06/05/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_billed_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.IS_BILLED_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_billed_sw          VARCHAR2(1);
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    SELECT NVL((SELECT 'Y'
                  FROM ordp100a a
                 WHERE a.div_part = l_div_part
                   AND a.ordnoa = i_ord_num
                   AND a.stata IN('P', 'R', 'A')), 'N')
      INTO l_billed_sw
      FROM DUAL;

    RETURN(l_billed_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_billed_fn;

  /*
  ||----------------------------------------------------------------------------
  || REASON_CD_LIST_FN
  ||  Reason code list for reason type.
  ||
  ||  This procedure is called by ReasonCodeListDS.callList (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  ||----------------------------------------------------------------------------
  */
  FUNCTION reason_cd_list_fn(
    i_rsn_typ  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.REASON_CD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'RsnTyp', i_rsn_typ);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT rsncda, desca
         FROM mclp140a
        WHERE rsntpa = i_rsn_typ;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reason_cd_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || AUDIT_INFO_CUR_FN
  ||  Get cursor of audit information for an order line.
  ||
  ||  This function is called by AuditTrailDS.getAuditListFromDB (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 10/02/06 | rhalpai | Changed to return timestamp as a formatted string.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8859
  ||----------------------------------------------------------------------------
  */
  FUNCTION audit_info_cur_fn(
    i_div      IN  VARCHAR2,
    i_ord_num  IN  NUMBER,
    i_ord_ln   IN  NUMBER
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.AUDIT_INFO_CUR_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   formatted_date_time_fn(sa.datea, sa.timea, 'Y', 'MM/DD/YYYY HH24:MI:SS') AS ts, ma.desca,
                SUBSTR(sa.florga, 1, 10) AS b4_qty, SUBSTR(sa.flchga, 1, 10) AS aftr_qty, sa.autbya, sa.usera
           FROM sysp296a sa, mclp140a ma
          WHERE sa.div_part = l_div_part
            AND sa.ordnoa = i_ord_num
            AND sa.linea = i_ord_ln
            AND sa.rsncda = ma.rsncda(+)
       UNION
       SELECT   formatted_date_time_fn(sa.datea, sa.timea, 'Y', 'MM/DD/YYYY HH24:MI:SS') AS ts, ma.desca,
                SUBSTR(sa.florga, 1, 10) AS b4_qty, SUBSTR(sa.flchga, 1, 10) AS aftr_qty, sa.autbya, sa.usera
           FROM sysp996a sa, mclp140a ma
          WHERE sa.ordnoa = i_ord_num
            AND sa.div_part = l_div_part
            AND sa.linea = i_ord_ln
            AND sa.rsncda = ma.rsncda(+)
       UNION
       SELECT   TO_CHAR(md.last_chg_ts, 'MM/DD/YYYY HH24:MI:SS') AS ts, ma.desca, SUBSTR(md.qtyfrd, 1, 5),
                SUBSTR(md.qtytod, 1, 5), ' ', md.resusd
           FROM mclp300d md, mclp140a ma
          WHERE md.div_part = l_div_part
            AND md.ordnod = i_ord_num
            AND md.ordlnd = i_ord_ln
            AND md.reasnd = ma.rsncda(+)
            AND ma.info_sw = 'Y'
       UNION
       SELECT   TO_CHAR(md.last_chg_ts, 'MM/DD/YYYY HH24:MI:SS') AS ts, ma.desca, SUBSTR(md.qtyfrd, 1, 5),
                SUBSTR(md.qtytod, 1, 5), ' ', md.resusd
           FROM mclp900d md, mclp140a ma
          WHERE md.div_part = l_div_part
            AND md.ordnod = i_ord_num
            AND md.ordlnd = i_ord_ln
            AND md.reasnd = ma.rsncda(+)
            AND ma.info_sw = 'Y'
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END audit_info_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || CSR_RESTRICTIONS_FN
  ||  Get cursor of CSR restrictions for corp codes.
  ||
  ||  This function is called by OrderSearchDS.retrieveRestrictions (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 06/02/17 | rhalpai | Change to call OP_PARMS_PK.VALS_FOR_PRFX_FN for parms.
  ||                    | PIR14910
  ||----------------------------------------------------------------------------
  */
  FUNCTION csr_restrictions_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_cv             SYS_REFCURSOR;
    l_div_part       NUMBER;
    l_t_rstrct_ids   type_stab;
    l_t_rstrct_vals  type_stab;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);
    op_parms_pk.get_parms_for_prfx_sp(l_div_part,
                                      'RESTRICT',
                                      l_t_rstrct_ids,
                                      l_t_rstrct_vals,
                                      NULL,
                                      op_parms_pk.g_c_csr
                                     );

    OPEN l_cv
     FOR
       SELECT   i.parm, v.val
           FROM (SELECT SUBSTR(t.column_value, 1, LENGTH(t.column_value) - 4) AS parm, ROWNUM AS seq
                   FROM TABLE(CAST(l_t_rstrct_ids AS type_stab)) t) i,
                (SELECT t.column_value AS val, ROWNUM AS seq
                   FROM TABLE(CAST(l_t_rstrct_vals AS type_stab)) t) v
          WHERE i.seq = v.seq
       ORDER BY 1, 2;

    RETURN(l_cv);
  END csr_restrictions_fn;

-------------------------------------------------------------------------------

  /*
  ||----------------------------------------------------------------------------
  || LIST_CNT_FN
  ||  Returns number of entries in delimited list
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION list_cnt_fn(
    i_list       IN  VARCHAR2,
    i_delimiter  IN  VARCHAR2 DEFAULT ','
  )
    RETURN PLS_INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.LIST_CNT_FN';
    lar_parm             logs.tar_parm;
    l_list               typ.t_maxvc2;
    l_orig_len           PLS_INTEGER;
    l_new_len            PLS_INTEGER;
    l_cnt                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'List', i_list);
    logs.add_parm(lar_parm, 'Delimiter', i_delimiter);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_list := RTRIM(i_list, i_delimiter);
    l_orig_len := LENGTH(l_list);
    l_list := REPLACE(l_list, i_delimiter);
    l_new_len := LENGTH(l_list);

    IF l_new_len > 0 THEN
      l_cnt := l_orig_len - l_new_len + 1;
    ELSE
      l_cnt := 0;
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END list_cnt_fn;

  /*
  ||----------------------------------------------------------------------------
  || NEXT_CONF_NUM_FN
  ||  Generate and return the next sequence for order confirmation number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION next_conf_num_fn
    RETURN NUMBER IS
    l_conf_num  PLS_INTEGER;
  BEGIN
    SELECT ordp100a_connba_seq.NEXTVAL
      INTO l_conf_num
      FROM DUAL;

    RETURN(l_conf_num);
  END next_conf_num_fn;

  FUNCTION next_conf_num_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN('C' || next_conf_num_fn);
  END next_conf_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_COMMENT_FN
  ||  Return order comment.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_comment_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_hist_sw   IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_ORDERS_PK.ORD_COMMENT_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_ord_comnt          ordp140c.commc%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'HistSw', i_hist_sw);
    logs.dbg('ENTRY', lar_parm);

    IF NVL(UPPER(i_hist_sw), 'N') = 'N' THEN
      OPEN l_cv
       FOR
         SELECT c.commc
           FROM ordp140c c
          WHERE c.div_part = i_div_part
            AND c.ordnoc = i_ord_num;
    ELSE
      OPEN l_cv
       FOR
         SELECT c.commc
           FROM ordp940c c
          WHERE c.div_part = i_div_part
            AND c.ordnoc = i_ord_num;
    END IF;   -- NVL(UPPER(i_hist_sw), 'N') = 'N'

    FETCH l_cv
     INTO l_ord_comnt;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_ord_comnt);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_comment_fn;

  /*
  ||----------------------------------------------------------------------------
  || TEST_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION test_fn(
    i_name       IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_msg                   typ.t_maxvc2;
    l_c_delimiter  CONSTANT VARCHAR2(1)  := '`';
    l_t_parms               type_stab;
  BEGIN
    CASE UPPER(i_name)
      WHEN 'ORD_HDR_MSG_FN' THEN
        /*
          FUNCTION ord_hdr_msg_fn(
            i_div_part    IN  NUMBER,
            i_create_typ  IN  VARCHAR2,
            i_r_ord_hdr   IN  csr_orders_pk.g_rt_msg_hdr,
            i_ord_comnt   IN  VARCHAR2 DEFAULT NULL
          )
            RETURN VARCHAR2;

            i_div_part,
            i_create_typ,
            i_r_ord_hdr.ord_num,
            i_r_ord_hdr.div,
            i_r_ord_hdr.cust_id,
            i_r_ord_hdr.mcl_cust,
            i_r_ord_hdr.load_typ,
            i_r_ord_hdr.ord_typ,
            i_r_ord_hdr.ord_src,
            i_r_ord_hdr.conf_num,
            i_r_ord_hdr.ser_num,
            i_r_ord_hdr.trnsmt_ts,
            i_r_ord_hdr.cust_pass_area,
            i_r_ord_hdr.hdr_excptn_cd,
            i_r_ord_hdr.legcy_ref,
            i_r_ord_hdr.po_num,
            i_r_ord_hdr.allw_partl_sw,
            i_r_ord_hdr.shp_dt,
            i_r_ord_hdr.ord_ln_cnt,
            i_ord_comnt
        */
        l_t_parms := str.parse_list(i_parm_list, l_c_delimiter);

        DECLARE
          l_div_part    NUMBER;
          l_create_typ  VARCHAR2(1);
          l_r_ord_hdr   csr_orders_pk.g_rt_msg_hdr;
          l_ord_comnt   ordp140c.commc%TYPE;
        BEGIN
          l_div_part := val_at_idx_fn(l_t_parms, 1);
          l_create_typ := val_at_idx_fn(l_t_parms, 2);
          l_r_ord_hdr.ord_num := val_at_idx_fn(l_t_parms, 3);
          l_r_ord_hdr.div := val_at_idx_fn(l_t_parms, 4);
          l_r_ord_hdr.cust_id := val_at_idx_fn(l_t_parms, 5);
          l_r_ord_hdr.mcl_cust := val_at_idx_fn(l_t_parms, 6);
          l_r_ord_hdr.load_typ := val_at_idx_fn(l_t_parms, 7);
          l_r_ord_hdr.ord_typ := val_at_idx_fn(l_t_parms, 8);
          l_r_ord_hdr.ord_src := val_at_idx_fn(l_t_parms, 9);
          l_r_ord_hdr.conf_num := val_at_idx_fn(l_t_parms, 10);
          l_r_ord_hdr.ser_num := val_at_idx_fn(l_t_parms, 11);
          l_r_ord_hdr.trnsmt_ts := TO_DATE(val_at_idx_fn(l_t_parms, 12), 'YYYYMMDDHH24MISS');
          l_r_ord_hdr.cust_pass_area := val_at_idx_fn(l_t_parms, 13);
          l_r_ord_hdr.hdr_excptn_cd := val_at_idx_fn(l_t_parms, 14);
          l_r_ord_hdr.legcy_ref := val_at_idx_fn(l_t_parms, 15);
          l_r_ord_hdr.po_num := val_at_idx_fn(l_t_parms, 16);
          l_r_ord_hdr.allw_partl_sw := val_at_idx_fn(l_t_parms, 17);
          l_r_ord_hdr.shp_dt := TO_DATE(val_at_idx_fn(l_t_parms, 18), 'YYYYMMDD');
          l_r_ord_hdr.ord_ln_cnt := val_at_idx_fn(l_t_parms, 19);
          l_ord_comnt := val_at_idx_fn(l_t_parms, 20);
          l_msg := ord_hdr_msg_fn(l_div_part, l_create_typ, l_r_ord_hdr, l_ord_comnt);
        END;
      WHEN 'ORD_DTL_MSG_FN' THEN
        /*
          FUNCTION ord_dtl_msg_fn(
            i_po_num     IN  VARCHAR2,
            i_r_ord_dtl  IN  csr_orders_pk.g_rt_msg_dtl
          )
            RETURN VARCHAR2;

            i_r_ord_dtl.catlg_num,
            i_r_ord_dtl.cbr_item,
            i_r_ord_dtl.uom,
            i_r_ord_dtl.ord_qty,
            i_r_ord_dtl.cust_item,
            i_r_ord_dtl.item_pass_area,
            i_r_ord_dtl.hard_rtl_sw,
            i_r_ord_dtl.rtl_amt,
            i_r_ord_dtl.rtl_mult,
            i_r_ord_dtl.hard_price_sw,
            i_r_ord_dtl.price_amt,
            i_r_ord_dtl.orig_qty,
            i_r_ord_dtl.byp_max_sw,
            i_r_ord_dtl.max_qty,
            i_r_ord_dtl.qty_mult,
            i_r_ord_dtl.ord_ln
        */
        l_t_parms := str.parse_list(i_parm_list, l_c_delimiter);

        DECLARE
          l_po_num     VARCHAR2(30);
          l_r_msg_dtl  csr_orders_pk.g_rt_msg_dtl;
        BEGIN
          l_po_num := val_at_idx_fn(l_t_parms, 1);
          l_r_msg_dtl.catlg_num := val_at_idx_fn(l_t_parms, 2);
          l_r_msg_dtl.cbr_item := val_at_idx_fn(l_t_parms, 3);
          l_r_msg_dtl.uom := val_at_idx_fn(l_t_parms, 4);
          l_r_msg_dtl.ord_qty := val_at_idx_fn(l_t_parms, 5);
          l_r_msg_dtl.cust_item := val_at_idx_fn(l_t_parms, 6);
          l_r_msg_dtl.item_pass_area := val_at_idx_fn(l_t_parms, 7);
          l_r_msg_dtl.hard_rtl_sw := val_at_idx_fn(l_t_parms, 8);
          l_r_msg_dtl.rtl_amt := val_at_idx_fn(l_t_parms, 9);
          l_r_msg_dtl.rtl_mult := val_at_idx_fn(l_t_parms, 10);
          l_r_msg_dtl.hard_price_sw := val_at_idx_fn(l_t_parms, 11);
          l_r_msg_dtl.price_amt := val_at_idx_fn(l_t_parms, 12);
          l_r_msg_dtl.orig_qty := val_at_idx_fn(l_t_parms, 13);
          l_r_msg_dtl.byp_max_sw := val_at_idx_fn(l_t_parms, 14);
          l_r_msg_dtl.max_qty := val_at_idx_fn(l_t_parms, 15);
          l_r_msg_dtl.qty_mult := val_at_idx_fn(l_t_parms, 16);
          l_r_msg_dtl.ord_ln := val_at_idx_fn(l_t_parms, 17);
          l_msg := ord_dtl_msg_fn(l_po_num, l_r_msg_dtl);
        END;
      ELSE
        l_msg := i_name || ' not set up to test!';
    END CASE;

    RETURN(l_msg);
  END test_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FOR_RESEND_SP
  ||  Process updates needed to resend order to mainframe.
  ||    Delete sub lines.
  ||    Set order header and details to mainframe status
  ||    Set load to DFLT when resending entire order
  ||    Set order exceptions to resolved.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 05/09/13 | rhalpai | Change to not set load to DFLT when sending to
  ||                    | mainframe. IM-102610
  || 12/08/15 | rhalpai | Add UpdByDivPart in calls to OP_MCLP300D_PK.UPD_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_for_resend_sp(
    i_div_part   IN  NUMBER,
    i_ord_num    IN  NUMBER,
    i_t_ord_lns  IN  type_ntab DEFAULT NULL,
    i_commit_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.UPD_FOR_RESEND_SP';
    lar_parm             logs.tar_parm;
    l_is_ord_lvl         BOOLEAN;

    PROCEDURE del_subs_sp(
      i_div_part   IN  NUMBER,
      i_ord_num    IN  NUMBER,
      i_t_ord_lns  IN  type_ntab
    ) IS
    BEGIN
      IF l_is_ord_lvl THEN
        logs.dbg('Remove All Sub Lines');

        DELETE FROM ordp120b b
              WHERE b.div_part = i_div_part
                AND b.ordnob = i_ord_num
                AND b.lineb > FLOOR(b.lineb)
                AND b.statb IN('O', 'I', 'S');
      ELSE
        logs.dbg('Remove Sub Line');
        FORALL i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST
          DELETE FROM ordp120b b
                WHERE b.div_part = i_div_part
                  AND b.ordnob = i_ord_num
                  AND FLOOR(b.lineb) = i_t_ord_lns(i)
                  AND b.lineb > FLOOR(b.lineb)
                  AND b.statb IN('O', 'I', 'S');
      END IF;   -- l_is_ord_lvl
    END del_subs_sp;

    PROCEDURE set_mainframe_status_sp(
      i_div_part   IN  NUMBER,
      i_ord_num    IN  NUMBER,
      i_t_ord_lns  IN  type_ntab
    ) IS
    BEGIN
      IF l_is_ord_lvl THEN
        logs.dbg('Set Order Header to Mainframe Status');

        UPDATE ordp100a a
           SET a.stata = 'I'
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num;

        logs.dbg('Set All Order Lines to Mainframe Status');

        UPDATE ordp120b b
           SET b.statb = DECODE(b.statb, 'C', 'C', 'I')
         WHERE b.div_part = i_div_part
           AND b.ordnob = i_ord_num
           AND b.statb IN('O', 'I', 'S', 'C');
      ELSE
        logs.dbg('Set Order Header Status');

        UPDATE ordp100a a
           SET a.stata = 'I'
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num;

        logs.dbg('Set Order Lines to Mainframe Status');
        FORALL i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST
          UPDATE ordp120b b
             SET b.statb = 'I'
           WHERE b.div_part = i_div_part
             AND b.ordnob = i_ord_num
             AND b.lineb = i_t_ord_lns(i)
             AND b.statb IN('O', 'S');
      END IF;   -- l_is_ord_lvl
    END set_mainframe_status_sp;

    PROCEDURE resolve_exceptions_sp(
      i_div_part   IN  NUMBER,
      i_ord_num    IN  NUMBER,
      i_t_ord_lns  IN  type_ntab
    ) IS
      r_upd  op_mclp300d_pk.g_rt_upd;
    BEGIN
      r_upd.div_part := i_div_part;
      r_upd.ord_num := i_ord_num;
      r_upd.excpt_lvl := 2;
      r_upd.rslvd_cd := 1;

      IF l_is_ord_lvl THEN
        op_mclp300d_pk.upd_sp(r_upd, i_div_part, i_ord_num, NULL, NULL, NULL, NULL);
      ELSE
        FOR i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST LOOP
          r_upd.ord_ln := i_t_ord_lns(i);
          op_mclp300d_pk.upd_sp(r_upd, i_div_part, i_ord_num, i_t_ord_lns(i), NULL, NULL, NULL);
        END LOOP;
      END IF;   -- l_is_ord_lvl
    END resolve_exceptions_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLnsTab', i_t_ord_lns);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_is_ord_lvl :=(   i_t_ord_lns IS NULL
                    OR i_t_ord_lns(i_t_ord_lns.FIRST) IS NULL);
    logs.dbg('Del Subs');
    del_subs_sp(i_div_part, i_ord_num, i_t_ord_lns);
    logs.dbg('Set MF Status');
    set_mainframe_status_sp(i_div_part, i_ord_num, i_t_ord_lns);
    logs.dbg('Resolve Exceptions');
    resolve_exceptions_sp(i_div_part, i_ord_num, i_t_ord_lns);

    IF UPPER(i_commit_sw) = 'Y' THEN
      COMMIT;
    END IF;   -- UPPER(i_commit_sw) = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF UPPER(i_commit_sw) = 'Y' THEN
        ROLLBACK;
      END IF;   -- UPPER(i_commit_sw) = 'Y'

      logs.err(lar_parm);
  END upd_for_resend_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_MAINT_USER_SP
  ||  Set maintenance user for order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_maint_user_sp(
    i_div        IN  VARCHAR2,
    i_user_id    IN  VARCHAR2,
    i_ord_num    IN  NUMBER,
    i_conf_num   IN  VARCHAR2 DEFAULT NULL,
    i_commit_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.UPD_MAINT_USER_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);
    logs.dbg('Set Maint User for Order Num');

    UPDATE ordp100a
       SET mntusa = TRIM(i_user_id)
     WHERE div_part = l_div_part
       AND ordnoa = l_ord_num
       AND stata IN('O', 'I', 'S', 'C');

    IF UPPER(i_commit_sw) = 'Y' THEN
      COMMIT;
    END IF;   -- UPPER(i_commit_sw) = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF UPPER(i_commit_sw) = 'Y' THEN
        ROLLBACK;
      END IF;   -- UPPER(i_commit_sw) = 'Y'

      logs.err(lar_parm);
  END upd_maint_user_sp;

  /*
  ||----------------------------------------------------------------------------
  || SEND_TO_MAINFRAME_SP
  ||  Send order to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE send_to_mainframe_sp(
    i_div          IN  VARCHAR2,
    i_ord_num      IN  NUMBER,
    i_ord_ln_list  IN  VARCHAR2 DEFAULT NULL,
    i_conf_num     IN  VARCHAR2 DEFAULT NULL,
    i_ord_comnt    IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm              := 'CSR_ORDERS_PK.SEND_TO_MAINFRAME_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_ord_num              NUMBER;
    l_r_ord_hdr            csr_orders_pk.g_rt_msg_hdr;
    l_msg_id               VARCHAR2(9);
    l_corr_put_id          NUMBER;
    l_t_dtl_msgs           csr_orders_pk.g_tt_msgs;
    l_t_ord_lns            type_ntab                  := type_ntab();
    l_create_typ           VARCHAR2(1);
    l_idx                  PLS_INTEGER;
    l_cnt                  PLS_INTEGER;
    l_mq_put_return_cd     PLS_INTEGER;
    l_e_ord_hdr_not_found  EXCEPTION;
    l_e_ord_dtl_not_found  EXCEPTION;
    l_e_mq_put_failed      EXCEPTION;

    PROCEDURE ord_hdr_sp(
      i_div_part   IN      NUMBER,
      i_ord_num    IN      NUMBER,
      o_r_ord_hdr  OUT     csr_orders_pk.g_rt_msg_hdr
    ) IS
    BEGIN
      SELECT a.ordnoa,
             d.div_part,
             d.div_id,
             a.custa,
             cx.mccusb,
             a.ldtypa,
             a.dsorda,
             a.ipdtsa,
             a.connba,
             a.telsla,
             TO_DATE('19000228' || lpad_fn(a.trntma, 6, '0'), 'YYYYMMDDHH24MISS') + a.trndta,
             a.cspasa,
             a.hdexpa,
             a.legrfa,
             a.cpoa,
             (CASE
                WHEN a.pshipa IN('1', 'Y') THEN 'Y'
                ELSE 'N'
              END),
             DATE '1900-02-28' + a.shpja,
             (SELECT COUNT(*)
                FROM ordp120b b
               WHERE b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.lineb = FLOOR(b.lineb))
        INTO o_r_ord_hdr
        FROM ordp100a a, div_mstr_di1d d, mclp020b cx
       WHERE d.div_part = i_div_part
         AND a.div_part = d.div_part
         AND a.ordnoa = i_ord_num
         AND a.stata IN('O', 'I', 'S')
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE l_e_ord_hdr_not_found;
    END ord_hdr_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLnList', i_ord_ln_list);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'OrdComnt', i_ord_comnt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);
    logs.dbg('Get Order Header');
    ord_hdr_sp(l_div_part, l_ord_num, l_r_ord_hdr);
    logs.dbg('Get Order Details');
    ord_dtls_sp(l_div_part, l_ord_num, i_ord_ln_list, l_t_dtl_msgs, l_t_ord_lns);

    IF (   l_t_dtl_msgs IS NULL
        OR l_t_dtl_msgs.COUNT = 0) THEN
      RAISE l_e_ord_dtl_not_found;
    END IF;   -- t_dtl_msgs IS NULL OR t_dtl_msgs.COUNT = 0

    IF is_new_ord_fn(l_div_part, l_ord_num) THEN
      -- first time to mainframe for new order
      l_msg_id := 'CSRSEND';
      l_create_typ := SUBSTR(i_conf_num, 1, 1);
      l_r_ord_hdr.trnsmt_ts := SYSDATE;
    ELSE
      l_msg_id := 'CSRRESEND';
      l_create_typ := csr_orders_pk.g_c_csr;
      logs.dbg('Process Updates Needed to Resend Order');

      IF i_ord_ln_list IS NULL THEN
        upd_for_resend_sp(l_div_part, l_ord_num);
      ELSE
        upd_for_resend_sp(l_div_part, l_ord_num, l_t_ord_lns);
      END IF;   -- p_ord_ln_list IS NULL
    END IF;   -- is_new_ord_fn(l_div_part, l_ord_num)

    logs.dbg('Generate MQ Put Correlation ID');
    l_corr_put_id := generate_mq_put_corr_id_fn;
    logs.dbg('Add Order Header Put Msg');
    l_cnt := op_mclane_mq_put_pk.ins_fn(l_div_part,
                                        l_msg_id,
                                        ord_hdr_msg_fn(l_div_part, l_create_typ, l_r_ord_hdr, i_ord_comnt),
                                        SYSDATE,
                                        l_corr_put_id
                                       );
    logs.dbg('Add Order Detail Put Msgs');
    l_idx := l_t_dtl_msgs.FIRST;
    <<dtl_msg_loop>>
    WHILE l_idx IS NOT NULL LOOP
      l_cnt := op_mclane_mq_put_pk.ins_fn(l_div_part, l_msg_id, l_t_dtl_msgs(l_idx), SYSDATE, l_corr_put_id);
      l_idx := l_t_dtl_msgs.NEXT(l_idx);
    END LOOP dtl_msg_loop;
    -- Must commit prior to processing MQ msgs
    -- to allow processing in separate thread
    COMMIT;
    logs.dbg('Process MQ Put Msgs');
    op_mq_message_pk.mq_put_sp(l_msg_id, i_div, l_corr_put_id, l_mq_put_return_cd);

    IF l_mq_put_return_cd <> 0 THEN
      RAISE l_e_mq_put_failed;
    END IF;   -- l_mq_put_return_cd <> 0

    logs.dbg('Clear Maint User');
    upd_maint_user_sp(i_div, NULL, l_ord_num);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_ord_hdr_not_found THEN
      ROLLBACK;
      logs.warn('Order Header not found or unavailable', lar_parm);
    WHEN l_e_ord_dtl_not_found THEN
      ROLLBACK;
      logs.warn('Order Detail not found or unavailable', lar_parm);
    WHEN l_e_mq_put_failed THEN
      ROLLBACK;
      logs.err('MQ Put Failed', lar_parm);
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END send_to_mainframe_sp;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_ORD_SP
  ||  Save Order.
  ||  I_CANCEL_LN_PARM_LIST is in the following format:
  ||  ord_ln~rsn_cd~auth_by`ord_ln~rsn_cd~auth_by
  ||  I_NEW_DTL_PARM_LIST is in the following format:
  ||  ord_ln~catlg_num~ord_qty~rsn_cd~auth_by`ord_ln~catlg_num~ord_qty~rsn_cd~auth_by
  ||  I_UPD_DTL_PARM_LIST is in the following format:
  ||  ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by`ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 11/05/14 | rhalpai | Add i_add_on_ord_sw input parameter and logic to
  ||                    | determine order source.
  ||                    | (New orders: ADC when Y or CSRWRK when N, when N for
  ||                    | existing ADK/ADC orders: KEY when ADK or CSRWRK when
  ||                    | ADC) Pass order source to SAVE_ORD_HDR_SP. PIR12893
  || 05/12/22 | rhalpai | Remove log change for PO since done in UPD_ORD_HDR_SP. SDHD-1275110
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_ord_sp(
    i_div                  IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_conf_num             IN  VARCHAR2,
    i_test_sw              IN  VARCHAR2,
    i_no_ord_sw            IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_mcl_cust             IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_user_id              IN  VARCHAR2,
    i_add_on_ord_sw        IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_ORDERS_PK.SAVE_ORD_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_is_new_ord         BOOLEAN;
    l_ord_src            ordp100a.ipdtsa%TYPE;
    l_lock_sw            VARCHAR2(1);
    l_old_ord_src        ordp100a.ipdtsa%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'TestSw', i_test_sw);
    logs.add_parm(lar_parm, 'NoOrdSw', i_no_ord_sw);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'CancelLnParmList', i_cancel_ln_parm_list);
    logs.add_parm(lar_parm, 'NewDtlParmList', i_new_dtl_parm_list);
    logs.add_parm(lar_parm, 'UpdDtlParmList', i_upd_dtl_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'AddOnOrdSw', i_add_on_ord_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num := COALESCE((CASE
                             WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                           END),
                          ord_num_for_conf_num_fn(l_div_part, i_conf_num),
                          0
                         );
    l_is_new_ord :=(l_ord_num = 0);

    IF l_is_new_ord THEN
      l_ord_num := next_ord_num_fn;
      l_ord_src :=(CASE i_add_on_ord_sw
                     WHEN 'Y' THEN 'ADC'
                     ELSE 'CSRWRK'
                   END);
    ELSE
      logs.dbg('Lock Order');
      op_ord_hdr_pk.lock_ord_sp(l_div_part, l_ord_num, l_lock_sw);
      excp.assert((l_lock_sw = 'Y'), 'Order not found or unavailable');

      SELECT a.ipdtsa
        INTO l_old_ord_src
        FROM ordp100a a
       WHERE a.div_part = l_div_part
         AND a.ordnoa = l_ord_num;

      IF (    l_old_ord_src IN('ADK', 'ADC')
          AND i_add_on_ord_sw = 'N') THEN
        l_ord_src :=(CASE l_old_ord_src
                       WHEN 'ADK' THEN 'KEY'
                       WHEN 'ADC' THEN 'CSRWRK'
                     END);
      ELSE
        l_ord_src := l_old_ord_src;
      END IF;   -- l_old_ord_src IN('ADK', 'ADC') AND i_add_on_ord_sw = 'N'
    END IF;   -- l_is_new_ord

    logs.dbg('Save Order Header');
    save_ord_hdr_sp(l_div_part,
                    l_ord_num,
                    i_conf_num,
                    l_ord_src,
                    i_test_sw,
                    i_no_ord_sw,
                    i_load_typ,
                    i_mcl_cust,
                    i_po_num,
                    i_user_id
                   );
    logs.dbg('Save Order Details');
    save_ord_dtl_sp(l_div_part, l_ord_num, i_cancel_ln_parm_list, i_new_dtl_parm_list, i_upd_dtl_parm_list, i_user_id);

    IF l_is_new_ord THEN
      -- auto-commit for new orders only
      COMMIT;
    END IF;   -- l_is_new_ord

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END save_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || VALIDATE_SP
  ||  Return any error/warning messages prior to cancel/suspend/complete.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/05/07 | rhalpai | Original - Created for PIR5132/PIR5002/PIR5341
  || 08/09/10 | rhalpai | Add PO validation for minimum non-blank characters as
  ||                    | required at Corp-Level indicated by parm
  ||                    | PO_NONBLNK_CHARS_###. The last 3 characters of the
  ||                    | parm will indicate the CorpCode and the integer value
  ||                    | will indicate the minimum number of non-blank
  ||                    | characters required. PIR8909
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw. PIR11038
  || 12/08/15 | rhalpai | Add DivPart in calls to OP_SPLIT_ORD_PK.IS_SPLIT_FOR_STRICT_FN,
  ||                    | OP_SPLIT_ORD_PK.SPLIT_ORD_WARNING_MSGS_FN.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE validate_sp(
    i_div                  IN      VARCHAR2,
    i_actn_cd              IN      VARCHAR2,
    i_ord_num              IN      NUMBER,
    i_conf_num             IN      VARCHAR2,
    i_mcl_cust             IN      VARCHAR2,
    i_po_num               IN      VARCHAR2,
    i_cancel_ln_parm_list  IN      VARCHAR2,
    i_new_dtl_parm_list    IN      VARCHAR2,
    i_upd_dtl_parm_list    IN      VARCHAR2,
    i_resend_ln_list       IN      VARCHAR2,
    o_msg                  OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.VALIDATE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_is_new_ord         BOOLEAN;
    l_err_msg            typ.t_maxvc2;
    l_warn_msg           typ.t_maxvc2;

    PROCEDURE add_msg_sp(
      i_new_msg        IN      VARCHAR2,
      io_existing_msg  IN OUT  VARCHAR2
    ) IS
    BEGIN
      io_existing_msg := io_existing_msg ||(CASE
                                              WHEN io_existing_msg IS NOT NULL THEN cnst.newline_char
                                            END) || i_new_msg;
    END add_msg_sp;

    FUNCTION recapped_strict_fn(
      i_div_part    IN  NUMBER,
      i_ord_num     IN  NUMBER,
      i_is_new_ord  IN  BOOLEAN
    )
      RETURN VARCHAR2 IS
      l_msg  typ.t_maxvc2;
    BEGIN
      IF NOT i_is_new_ord THEN
        IF op_strict_order_pk.is_recapped_fn(i_div_part, i_ord_num) THEN
          l_msg := op_strict_order_pk.g_c_recapped_msg;
        END IF;   -- op_strict_order_pk.is_recapped_fn(i_div_part, i_ord_num)
      END IF;   -- NOT l_is_new_ord

      RETURN(l_msg);
    END recapped_strict_fn;

    FUNCTION recapped_strict_ord_lns_fn(
      i_div_part    IN  NUMBER,
      i_ord_num     IN  NUMBER,
      i_is_new_ord  IN  BOOLEAN,
      i_t_ord_lns   IN  type_stab,
      i_action      IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
      l_msg                        typ.t_maxvc2;
      l_c_max_ord_ln_cnt  CONSTANT PLS_INTEGER  := 10;
      l_cnt                        PLS_INTEGER  := 0;
    BEGIN
      IF (    NOT i_is_new_ord
          AND i_t_ord_lns IS NOT NULL
          AND i_t_ord_lns.COUNT > 0) THEN
        FOR i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST LOOP
          IF op_strict_order_pk.is_recapped_fn(i_div_part, i_ord_num, i_t_ord_lns(i)) THEN
            l_cnt := l_cnt + 1;

            IF l_cnt > l_c_max_ord_ln_cnt THEN
              l_cnt := 1;
              l_msg := l_msg || ',' || cnst.newline_char;
            END IF;   -- l_cnt > l_c_max_ord_ln_cnt

            l_msg := l_msg ||(CASE
                                WHEN l_cnt > 1 THEN ', '
                              END) || i_t_ord_lns(i);
          END IF;   -- op_strict_order_pk.is_recapped_fn(i_div_part, i_ord_num, i_t_ord_lns(i))
        END LOOP;

        IF l_msg IS NOT NULL THEN
          l_msg := 'For ' || i_action || ' Order Lines:' || cnst.newline_char || l_msg;
        END IF;   -- l_msg IS NOT NULL
      END IF;   -- NOT i_is_new_ord AND i_t_ord_lns IS NOT NULL AND i_t_ord_lns.COUNT > 0

      RETURN(l_msg);
    END recapped_strict_ord_lns_fn;

    PROCEDURE check_cancel_sp(
      i_div_part    IN      NUMBER,
      i_ord_num     IN      NUMBER,
      i_is_new_ord  IN      BOOLEAN,
      i_err_msg     IN      VARCHAR2,
      o_warn_msg    OUT     VARCHAR2
    ) IS
    BEGIN
      IF NOT i_is_new_ord THEN
        -- check errors
--        i_err_msg := COALESCE(NULL, NULL);
        IF i_err_msg IS NULL THEN
          -- check warnings
          add_msg_sp(recapped_strict_fn(i_div_part, i_ord_num, i_is_new_ord), o_warn_msg);
        END IF;   -- i_err_msg IS NULL
      END IF;   -- NOT i_is_new_ord
    END check_cancel_sp;

    PROCEDURE check_suspend_sp(
      i_div_part    IN      NUMBER,
      i_ord_num     IN      NUMBER,
      i_is_new_ord  IN      BOOLEAN,
      i_err_msg     IN      VARCHAR2,
      o_warn_msg    OUT     VARCHAR2
    ) IS
    BEGIN
      -- check errors
--      i_err_msg := COALESCE(NULL, NULL);
      IF i_err_msg IS NULL THEN
        -- check warnings
        add_msg_sp(recapped_strict_fn(i_div_part, i_ord_num, i_is_new_ord), o_warn_msg);
      END IF;   -- i_err_msg IS NULL
    END check_suspend_sp;

    PROCEDURE check_complete_sp(
      i_div_part             IN      NUMBER,
      i_ord_num              IN      NUMBER,
      i_is_new_ord           IN      BOOLEAN,
      i_conf_num             IN      VARCHAR2,
      i_mcl_cust             IN      VARCHAR2,
      i_po_num               IN      VARCHAR2,
      i_cancel_ln_parm_list  IN      VARCHAR2,
      i_new_dtl_parm_list    IN      VARCHAR2,
      i_upd_dtl_parm_list    IN      VARCHAR2,
      i_resend_ln_list       IN      VARCHAR2,
      o_err_msg              OUT     VARCHAR2,
      o_warn_msg             OUT     VARCHAR2
    ) IS
      l_t_cancel_ord_lns  type_stab := type_stab();
      l_t_catlg_nums      type_stab;

      FUNCTION po_reqd_fn(
        i_div_part  IN  NUMBER,
        i_ord_num   IN  NUMBER,
        i_mcl_cust  IN  VARCHAR2,
        i_po_num    IN  VARCHAR2
      )
        RETURN VARCHAR2 IS
        l_t_nonblnk_po_corps  type_stab;
        l_t_nonblnk_po_vals   type_stab;
        l_cv                  SYS_REFCURSOR;
        l_msg                 typ.t_maxvc2;
      BEGIN
        op_parms_pk.get_parms_for_prfx_sp(i_div_part,
                                          op_const_pk.prm_po_nonblnk_chars,
                                          l_t_nonblnk_po_corps,
                                          l_t_nonblnk_po_vals,
                                          3,
                                          op_parms_pk.g_c_csr
                                         );

        OPEN l_cv
         FOR
           SELECT 'Corp '
                  || LPAD(pp.crp_cd, 3, '0')
                  || ' requires a PO with at least '
                  || pp.val
                  || ' non-blank characters.'
             FROM mclp020b cx,
                  (SELECT i.crp_cd, v.val
                     FROM (SELECT TO_NUMBER(t.column_value) AS crp_cd, ROWNUM AS seq
                             FROM TABLE(CAST(l_t_nonblnk_po_corps AS type_stab)) t) i,
                          (SELECT TO_NUMBER(t.column_value) AS val, ROWNUM AS seq
                             FROM TABLE(CAST(l_t_nonblnk_po_vals AS type_stab)) t) v
                    WHERE i.seq = v.seq) pp
            WHERE cx.div_part = i_div_part
              AND cx.mccusb = i_mcl_cust
              AND pp.crp_cd = cx.corpb
              AND NVL(LENGTH(REPLACE(i_po_num, ' ')), 0) < pp.val
              AND NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = i_div_part
                                AND a.ordnoa = i_ord_num
                                AND a.dsorda IN('R', 'D'));

        FETCH l_cv
         INTO l_msg;

        RETURN(l_msg);
      END po_reqd_fn;

      FUNCTION cancel_ord_lns_fn(
        i_parm_list  IN  VARCHAR2
      )
        RETURN type_stab IS
        l_t_cancel_lns  g_tt_cancel_lns := g_tt_cancel_lns();
        l_t_ord_lns     type_stab       := type_stab();
      BEGIN
        IF i_parm_list IS NOT NULL THEN
          logs.dbg('Parse CancelOrdLns List');
          parse_cancel_ln_sp(i_parm_list, l_t_cancel_lns);
          logs.dbg('Add to table of CancelOrdLns');
          FOR i IN l_t_cancel_lns.FIRST .. l_t_cancel_lns.LAST LOOP
            l_t_ord_lns.EXTEND;
            l_t_ord_lns(l_t_ord_lns.LAST) := l_t_cancel_lns(i).ord_ln;
          END LOOP;
        END IF;   -- i_parm_list IS NOT NULL

        RETURN(l_t_ord_lns);
      END cancel_ord_lns_fn;

      FUNCTION catlg_nums_fn(
        i_parm_list  IN  VARCHAR2
      )
        RETURN type_stab IS
        l_t_ins_lns     g_tt_ins_lns;
        l_t_catlg_nums  type_stab    := type_stab();
      BEGIN
        IF i_parm_list IS NOT NULL THEN
          logs.dbg('Parse NewOrdLns List');
          parse_ins_ln_sp(i_parm_list, l_t_ins_lns);

          IF l_t_ins_lns.COUNT > 0 THEN
            logs.dbg('Add to table of CatlgNums');
            FOR i IN l_t_ins_lns.FIRST .. l_t_ins_lns.LAST LOOP
              l_t_catlg_nums.EXTEND;
              l_t_catlg_nums(l_t_catlg_nums.LAST) := l_t_ins_lns(i).catlg_num;
            END LOOP;
          END IF;   -- l_t_ins_lns.COUNT > 0
        END IF;   -- i_parm_list IS NOT NULL

        RETURN(l_t_catlg_nums);
      END catlg_nums_fn;

      FUNCTION upd_ord_lns_fn(
        i_parm_list  IN  VARCHAR2
      )
        RETURN type_stab IS
        l_t_upd_lns  g_tt_upd_lns := g_tt_upd_lns();
        l_t_ord_lns  type_stab    := type_stab();
      BEGIN
        IF i_parm_list IS NOT NULL THEN
          logs.dbg('Parse UpdOrdLns List');
          parse_upd_ln_sp(i_parm_list, l_t_upd_lns);
          logs.dbg('Add to table of UpdOrdLns');
          FOR i IN l_t_upd_lns.FIRST .. l_t_upd_lns.LAST LOOP
            l_t_ord_lns.EXTEND;
            l_t_ord_lns(l_t_ord_lns.LAST) := l_t_upd_lns(i).ord_ln;
          END LOOP;
        END IF;   -- i_parm_list IS NOT NULL

        RETURN(l_t_ord_lns);
      END upd_ord_lns_fn;

      FUNCTION is_ord_hdr_chg_fn(
        i_div_part  IN  NUMBER,
        i_ord_num   IN  NUMBER,
        i_mcl_cust  IN  VARCHAR2,
        i_po_num    IN  VARCHAR2
      )
        RETURN BOOLEAN IS
        l_cv      SYS_REFCURSOR;
        l_chg_sw  VARCHAR2(1)   := 'N';
      BEGIN
        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM ordp100a a, mclp020b cx
            WHERE a.div_part = i_div_part
              AND a.ordnoa = i_ord_num
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
              AND (   cx.mccusb <> NVL(i_mcl_cust, cx.mccusb)
                   OR a.cpoa <> NVL(i_po_num, a.cpoa)
                   OR a.stata = 'I');

        FETCH l_cv
         INTO l_chg_sw;

        RETURN(l_chg_sw = 'Y');
      END is_ord_hdr_chg_fn;

      PROCEDURE check_strict_sp(
        i_div_part           IN      NUMBER,
        i_ord_num            IN      NUMBER,
        i_is_new_ord         IN      BOOLEAN,
        i_conf_num           IN      VARCHAR2,
        i_mcl_cust           IN      VARCHAR2,
        i_po_num             IN      VARCHAR2,
        i_t_cancel_ord_lns   IN      type_stab,
        i_t_catlg_nums       IN      type_stab,
        i_upd_dtl_parm_list  IN      VARCHAR2,
        i_resend_ln_list     IN      VARCHAR2,
        io_warn_msg          IN OUT  VARCHAR2
      ) IS
        l_recapped_strict_msg   typ.t_maxvc2;
        l_split_for_strict_msg  typ.t_maxvc2;
        l_t_resend_ord_lns      type_stab    := type_stab();
        l_t_upd_ord_lns         type_stab    := type_stab();
      BEGIN
        logs.dbg('Detect Order Header Change');

        IF is_ord_hdr_chg_fn(i_div_part, i_ord_num, i_mcl_cust, i_po_num) THEN
          IF recapped_strict_fn(i_div_part, i_ord_num, i_is_new_ord) IS NOT NULL THEN
            l_recapped_strict_msg := '(for Order Header Change)';
          END IF;   -- recapped_strict_fn(l_div_part, l_ord_num, l_is_new_ord) IS NOT NULL

          IF op_split_ord_pk.is_split_for_strict_fn(i_div_part, i_ord_num, i_conf_num, i_mcl_cust, i_po_num) = 'Y' THEN
            l_split_for_strict_msg := op_strict_order_pk.g_c_has_strict_item_msg;
          END IF;   -- op_split_ord_pk.is_split_for_strict_fn(i_div_part, i_ord_num, NULL, i_mcl_cust, i_po_num) = 'Y'
        END IF;   -- is_ord_hdr_chg_fn(i_div_part, i_ord_num, i_mcl_cust, i_po_num)

        IF i_resend_ln_list IS NOT NULL THEN
          IF i_resend_ln_list = 'ALL' THEN
            IF recapped_strict_fn(i_div_part, i_ord_num, i_is_new_ord) IS NOT NULL THEN
              l_recapped_strict_msg := '(for Resend ALL Order Lines)';
            END IF;   -- recapped_strict_fn(i_div_part, i_ord_num, i_is_new_ord) IS NOT NULL

            IF op_split_ord_pk.is_split_for_strict_fn(i_div_part, i_ord_num, NULL, i_mcl_cust, i_po_num) = 'Y' THEN
              l_split_for_strict_msg := op_strict_order_pk.g_c_has_strict_item_msg;
            END IF;   -- op_split_ord_pk.is_split_for_strict_fn(i_div_part, i_ord_num, NULL, i_mcl_cust, i_po_num) = 'Y'
          ELSE
            logs.dbg('Parse Resend');
            l_t_resend_ord_lns := str.parse_list(i_resend_ln_list, op_const_pk.field_delimiter);
            add_msg_sp(recapped_strict_ord_lns_fn(i_div_part, i_ord_num, i_is_new_ord, l_t_resend_ord_lns, 'Resend'),
                       l_recapped_strict_msg
                      );

            IF op_split_ord_pk.is_split_for_strict_fn(i_div_part,
                                                      i_ord_num,
                                                      NULL,
                                                      i_mcl_cust,
                                                      i_po_num,
                                                      NULL,
                                                      l_t_resend_ord_lns
                                                     ) = 'Y' THEN
              l_split_for_strict_msg := op_strict_order_pk.g_c_has_strict_item_msg;
            END IF;   -- op_split_ord_pk.is_split_for_strict_fn(i_div_part, i_ord_num, NULL, i_mcl_cust, i_po_num, NULL, l_t_resend_ord_lns) = 'Y'
          END IF;   -- i_resend_ln_list = 'ALL'
        END IF;   -- i_resend_ln_list IS NOT NULL

        IF i_t_cancel_ord_lns.COUNT > 0 THEN
          add_msg_sp(recapped_strict_ord_lns_fn(i_div_part, i_ord_num, i_is_new_ord, i_t_cancel_ord_lns, 'Cancel'),
                     l_recapped_strict_msg
                    );
        END IF;   -- i_t_cancel_ord_lns.COUNT > 0

        IF i_upd_dtl_parm_list IS NOT NULL THEN
          l_t_upd_ord_lns := upd_ord_lns_fn(i_upd_dtl_parm_list);
          add_msg_sp(recapped_strict_ord_lns_fn(i_div_part, i_ord_num, i_is_new_ord, l_t_upd_ord_lns, 'Update'),
                     l_recapped_strict_msg
                    );
        END IF;   -- i_upd_dtl_parm_list IS NOT NULL

        IF (    i_t_catlg_nums.COUNT > 0
            AND l_split_for_strict_msg IS NULL) THEN
          logs.dbg('Check for Possible Order Split for Strict Items');

          IF op_split_ord_pk.is_split_for_strict_fn(i_div_part,
                                                    i_ord_num,
                                                    i_conf_num,
                                                    i_mcl_cust,
                                                    i_po_num,
                                                    i_t_catlg_nums
                                                   ) = 'Y' THEN
            l_split_for_strict_msg := op_strict_order_pk.g_c_has_strict_item_msg;
          END IF;   -- op_split_ord_pk.is_split_for_strict_fn(i_div_part, i_ord_num, i_conf_num, i_mcl_cust, i_po_num, i_catlg_nums) = 'Y'
        END IF;   -- i_t_catlg_nums.COUNT > 0 AND l_split_for_strict_msg IS NULL

        IF l_recapped_strict_msg IS NOT NULL THEN
          add_msg_sp(op_strict_order_pk.g_c_recapped_msg || cnst.newline_char || l_recapped_strict_msg, io_warn_msg);
        END IF;   -- l_recapped_strict_msg IS NOT NULL

        IF l_split_for_strict_msg IS NOT NULL THEN
          add_msg_sp(l_split_for_strict_msg, io_warn_msg);
        END IF;   -- l_split_for_strict_msg IS NOT NULL
      END check_strict_sp;

      PROCEDURE check_split_ord_sp(
        i_div_part          IN      NUMBER,
        i_ord_num           IN      NUMBER,
        i_conf_num          IN      VARCHAR2,
        i_mcl_cust          IN      VARCHAR2,
        i_po_num            IN      VARCHAR2,
        i_t_cancel_ord_lns  IN      type_stab,
        i_t_catlg_nums      IN      type_stab,
        io_warn_msg         IN OUT  VARCHAR2
      ) IS
        l_t_split_msgs  type_stab := type_stab();
      BEGIN
        l_t_split_msgs := op_split_ord_pk.split_ord_warning_msgs_fn(i_div_part,
                                                                    i_ord_num,
                                                                    i_conf_num,
                                                                    i_mcl_cust,
                                                                    i_po_num,
                                                                    i_t_catlg_nums,
                                                                    i_t_cancel_ord_lns
                                                                   );

        IF l_t_split_msgs.COUNT > 0 THEN
          FOR i IN l_t_split_msgs.FIRST .. l_t_split_msgs.LAST LOOP
            add_msg_sp(l_t_split_msgs(i), io_warn_msg);
          END LOOP;
        END IF;   -- l_split_msgs.COUNT > 0
      END check_split_ord_sp;
    BEGIN
      -- check errors
--      o_err_msg := COALESCE(NULL, NULL);
      o_err_msg := po_reqd_fn(i_div_part, i_ord_num, i_mcl_cust, i_po_num);

      IF o_err_msg IS NULL THEN
        -- check warnings
        l_t_cancel_ord_lns := cancel_ord_lns_fn(i_cancel_ln_parm_list);
        l_t_catlg_nums := catlg_nums_fn(i_new_dtl_parm_list);
        logs.dbg('Check Strict');
        check_strict_sp(i_div_part,
                        i_ord_num,
                        i_is_new_ord,
                        i_conf_num,
                        i_mcl_cust,
                        i_po_num,
                        l_t_cancel_ord_lns,
                        l_t_catlg_nums,
                        i_upd_dtl_parm_list,
                        i_resend_ln_list,
                        o_warn_msg
                       );
        logs.dbg('Check for Mixed Split Order Items');
        check_split_ord_sp(i_div_part,
                           i_ord_num,
                           i_conf_num,
                           i_mcl_cust,
                           i_po_num,
                           l_t_cancel_ord_lns,
                           l_t_catlg_nums,
                           o_warn_msg
                          );
      END IF;   -- o_err_msg IS NULL
    END check_complete_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'CancelLnParmList', i_cancel_ln_parm_list);
    logs.add_parm(lar_parm, 'NewDtlParmList', i_new_dtl_parm_list);
    logs.add_parm(lar_parm, 'UpdDtlParmList', i_upd_dtl_parm_list);
    logs.add_parm(lar_parm, 'ResendLnList', i_resend_ln_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);
    l_is_new_ord := is_new_ord_fn(l_div_part, l_ord_num);

    CASE i_actn_cd
      WHEN csr_orders_pk.g_c_cancel THEN
        logs.dbg('Check Cancel');
        check_cancel_sp(l_div_part, l_ord_num, l_is_new_ord, l_err_msg, l_warn_msg);
      WHEN csr_orders_pk.g_c_suspend THEN
        logs.dbg('Check Suspend');
        check_suspend_sp(l_div_part, l_ord_num, l_is_new_ord, l_err_msg, l_warn_msg);
      WHEN csr_orders_pk.g_c_complete THEN
        logs.dbg('Check Complete');
        check_complete_sp(l_div_part,
                          l_ord_num,
                          l_is_new_ord,
                          i_conf_num,
                          i_mcl_cust,
                          i_po_num,
                          i_cancel_ln_parm_list,
                          i_new_dtl_parm_list,
                          i_upd_dtl_parm_list,
                          i_resend_ln_list,
                          l_err_msg,
                          l_warn_msg
                         );
    END CASE;

    IF l_err_msg IS NOT NULL THEN
      o_msg := op_const_pk.msg_typ_err || op_const_pk.field_delimiter || l_err_msg;
    ELSIF l_warn_msg IS NOT NULL THEN
      o_msg := op_const_pk.msg_typ_info || op_const_pk.field_delimiter || l_warn_msg;
    END IF;   -- l_err_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_ORD_SP
  ||  Cancel existing order or remove new order.
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_ord_sp(
    i_div       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_conf_num  IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.CANCEL_ORD_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);

    IF l_ord_num IS NOT NULL THEN
      IF is_new_ord_fn(l_div_part, l_ord_num) THEN
        logs.dbg('Remove New Order');
        del_new_sp(l_div_part, l_ord_num);
      ELSE
        logs.dbg('Cancel Order');
        cancel_ord_sp(l_div_part, l_ord_num, i_rsn_cd, i_auth_by, i_user_id);
        COMMIT;
      END IF;   -- is_new_ord_fn(l_div_part, l_ord_num)
    END IF;   -- l_ord_num IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancel_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || SUSPEND_ORD_SP
  ||  Suspend order.
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE suspend_ord_sp(
    i_div                  IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_conf_num             IN  VARCHAR2,
    i_test_sw              IN  VARCHAR2,
    i_no_ord_sw            IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_mcl_cust             IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_rsn_cd               IN  VARCHAR2,
    i_user_id              IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_PK.SUSPEND_ORD_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_is_new_ord         BOOLEAN;
    l_lock_sw            VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'TestSw', i_test_sw);
    logs.add_parm(lar_parm, 'NoOrdSw', i_no_ord_sw);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'CancelLnParmList', i_cancel_ln_parm_list);
    logs.add_parm(lar_parm, 'NewDtlParmList', i_new_dtl_parm_list);
    logs.add_parm(lar_parm, 'UpdDtlParmList', i_upd_dtl_parm_list);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);
    l_is_new_ord := is_new_ord_fn(l_div_part, l_ord_num);
    logs.dbg('Save Order');
    save_ord_sp(i_div,
                i_ord_num,
                i_conf_num,
                i_test_sw,
                i_no_ord_sw,
                i_load_typ,
                i_mcl_cust,
                i_po_num,
                i_cancel_ln_parm_list,
                i_new_dtl_parm_list,
                i_upd_dtl_parm_list,
                i_user_id
               );
    logs.dbg('Lock Order');
    op_ord_hdr_pk.lock_ord_sp(l_div_part, l_ord_num, l_lock_sw);
    excp.assert((l_lock_sw = 'Y'), 'Order not found or unavailable');
    logs.dbg('Upd Order Hdr');

    UPDATE ordp100a a
       SET a.stata = 'S',
           a.mntusa = NULL
     WHERE a.div_part = l_div_part
       AND a.ordnoa = l_ord_num;

    logs.dbg('Upd Lines for Order');

    UPDATE ordp120b
       SET statb = 'S'
     WHERE div_part = l_div_part
       AND ordnob = l_ord_num
       AND statb IN('O', 'I');

    logs.dbg('Log Status Change');
    log_change_sp(l_div_part,
                  l_ord_num,
                  i_user_id,
                  'ORDP100A',
                  'STATA',
                  (CASE
                     WHEN l_is_new_ord THEN ' '
                   END),
                  'S',
                  (CASE
                     WHEN l_is_new_ord THEN 'C'
                     ELSE 'M'
                   END),
                  i_rsn_cd,
                  (CASE
                     WHEN l_is_new_ord THEN 'CSR'
                   END)
                 );
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END suspend_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || COMPLETE_ORD_SP
  ||  Complete order maintenance and send to mainframe if needed.
  ||  I_CANCEL_LN_PARM_LIST is in the following format:
  ||  ord_ln~rsn_cd~auth_by`ord_ln~rsn_cd~auth_by
  ||  I_NEW_DTL_PARM_LIST is in the following format:
  ||  ord_ln~catlg_num~ord_qty~rsn_cd~auth_by`ord_ln~catlg_num~ord_qty~rsn_cd~auth_by
  ||  I_UPD_DTL_PARM_LIST is in the following format:
  ||  ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by`ord_ln~byp_max_sw~ord_qty~rsn_cd~auth_by
  ||  I_RESEND_LN_LIST is in the following format:
  ||  ord_ln~ord_ln
  ||  Pass ord_num for existing or ref and null/zero ord_num for new.
  ||  Pass 'ALL' in P_RESEND_LN_LIST to unsuspend/resend order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 08/14/12 | rhalpai | Replace references to column for line count on order
  ||                    | header with logic to count order detail lines.
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 03/12/13 | rhalpai | Change logic to set l_ord_num after saving order.
  ||                    | IM-085795
  || 11/05/14 | rhalpai | Add i_add_on_ord_sw input parameter and pass to
  ||                    | SAVE_ORD_SP. PIR12893
  ||----------------------------------------------------------------------------
  */
  PROCEDURE complete_ord_sp(
    i_div                  IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_conf_num             IN  VARCHAR2,
    i_test_sw              IN  VARCHAR2,
    i_no_ord_sw            IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_mcl_cust             IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_cancel_ln_parm_list  IN  VARCHAR2,
    i_new_dtl_parm_list    IN  VARCHAR2,
    i_upd_dtl_parm_list    IN  VARCHAR2,
    i_resend_ln_list       IN  VARCHAR2,
    i_user_id              IN  VARCHAR2,
    i_add_on_ord_sw        IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_ORDERS_PK.COMPLETE_ORD_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_is_new_ord         BOOLEAN;
    l_lock_sw            VARCHAR2(1);
    l_is_save_only       BOOLEAN;
    l_is_send_all        BOOLEAN;
    l_cv                 SYS_REFCURSOR;
    l_stat               ordp100a.stata%TYPE   := '?';
    l_ord_ln_cnt         PLS_INTEGER           := -1;
    l_resend_cnt         PLS_INTEGER;
    l_send_ln_sw         VARCHAR2(1);
    l_ord_ln_list        typ.t_maxvc2;
    l_ord_ln             NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'TestSw', i_test_sw);
    logs.add_parm(lar_parm, 'NoOrdSw', i_no_ord_sw);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'CancelLnParmList', i_cancel_ln_parm_list);
    logs.add_parm(lar_parm, 'NewDtlParmList', i_new_dtl_parm_list);
    logs.add_parm(lar_parm, 'UpdDtlParmList', i_upd_dtl_parm_list);
    logs.add_parm(lar_parm, 'ResendLnList', i_resend_ln_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'AddOnOrdSw', i_add_on_ord_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Save Order');
    save_ord_sp(i_div,
                i_ord_num,
                i_conf_num,
                i_test_sw,
                i_no_ord_sw,
                i_load_typ,
                i_mcl_cust,
                i_po_num,
                i_cancel_ln_parm_list,
                i_new_dtl_parm_list,
                i_upd_dtl_parm_list,
                i_user_id,
                i_add_on_ord_sw
               );
    l_ord_num :=(CASE
                   WHEN NVL(i_ord_num, 0) > 0 THEN i_ord_num
                   ELSE ord_num_for_conf_num_fn(l_div_part, i_conf_num)
                 END);
    l_is_new_ord := is_new_ord_fn(l_div_part, l_ord_num);
    l_is_save_only := FALSE;

    IF (   l_is_new_ord
        OR i_resend_ln_list = 'ALL') THEN
      l_is_send_all := TRUE;
    ELSIF NOT l_is_new_ord THEN
      logs.dbg('Open Cursor for Order Hdr');

      OPEN l_cv
       FOR
         SELECT a.stata, (SELECT COUNT(*)
                            FROM ordp120b b
                           WHERE b.div_part = a.div_part
                             AND b.ordnob = a.ordnoa
                             AND b.lineb = FLOOR(b.lineb))
           FROM ordp100a a
          WHERE a.div_part = l_div_part
            AND a.ordnoa = l_ord_num;

      logs.dbg('Fetch Cursor for Order Hdr');

      FETCH l_cv
       INTO l_stat, l_ord_ln_cnt;

      l_resend_cnt := 0;

      IF i_resend_ln_list IS NOT NULL THEN
        logs.dbg('Get List Count');
        l_resend_cnt := list_cnt_fn(i_resend_ln_list, op_const_pk.field_delimiter);
      END IF;   -- i_resend_ln_list IS NOT NULL

      l_is_send_all :=(   l_stat = 'I'
                       OR l_resend_cnt = l_ord_ln_cnt);

      IF (    i_new_dtl_parm_list IS NULL
          AND i_resend_ln_list IS NULL
          AND NOT l_is_send_all) THEN
        l_send_ln_sw := 'N';
        logs.dbg('Open Cursor for Mainframe Status Check');

        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM DUAL
            WHERE EXISTS(SELECT 1
                           FROM ordp120b
                          WHERE div_part = l_div_part
                            AND ordnob = l_ord_num
                            AND statb = 'I');

        logs.dbg('Fetch Cursor for Mainframe Status Check');

        FETCH l_cv
         INTO l_send_ln_sw;

        l_is_save_only :=(l_send_ln_sw = 'N');
      END IF;   -- i_new_dtl_parm_list IS NULL AND i_resend_ln_list IS NULL AND NOT l_is_send_all
    END IF;   -- l_is_new_ord OR i_resend_ln_list = 'ALL'

    IF l_is_save_only THEN
      logs.dbg('Reset Maintenance User');
      upd_maint_user_sp(i_div, NULL, l_ord_num);
      COMMIT;
    ELSE
      logs.dbg('Lock Order');
      -- Lock was obtained during SAVE_ORD_SP but released due to commit
      op_ord_hdr_pk.lock_ord_sp(l_div_part, l_ord_num, l_lock_sw);
      -- Only raise error for existing order since all lines for new order
      -- may have been cancelled resulting in removal of order.
      -- This case is handled for existing order when setting l_is_save_only.
      excp.assert((   l_is_new_ord
                   OR l_lock_sw = 'Y'), 'Order not found or unavailable');

      IF l_is_send_all THEN
        logs.dbg('Send Complete Order to Mainframe');
        send_to_mainframe_sp(i_div, l_ord_num, NULL, i_conf_num);
      ELSE
        l_ord_ln_list := i_resend_ln_list;
        logs.dbg('Open Cursor for Lines in Mainframe Status');

        OPEN l_cv
         FOR
           SELECT lineb
             FROM ordp120b
            WHERE div_part = l_div_part
              AND ordnob = l_ord_num
              AND lineb = FLOOR(lineb)
              AND statb = 'I';

        LOOP
          logs.dbg('Fetch Cursor for Lines in Mainframe Status');

          FETCH l_cv
           INTO l_ord_ln;

          EXIT WHEN l_cv%NOTFOUND;
          logs.dbg('Build Order Line List');
          l_ord_ln_list := l_ord_ln_list
                           ||(CASE
                                WHEN l_ord_ln_list IS NOT NULL THEN op_const_pk.field_delimiter
                              END)
                           || l_ord_ln;
        END LOOP;
        logs.dbg('Send Selected Order Lines to Mainframe');
        send_to_mainframe_sp(i_div, l_ord_num, l_ord_ln_list);
      END IF;   -- l_is_send_all
    END IF;   -- l_is_save_only

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END complete_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_ORD_SP
  ||  Insert order header and details and route to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 07/16/12 | rhalpai | Change logic to eliminate unused columns. PIR11044
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_ord_sp(
    i_r_ord_hdr   IN  csr_orders_pk.g_rt_msg_hdr,
    i_t_ord_dtls  IN  csr_orders_pk.g_tt_msg_dtls,
    i_user_id     IN  VARCHAR2,
    i_ord_comnt   IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'CSR_ORDERS_PK.INS_ORD_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_num            NUMBER;
    l_r_ord_hdr          ordp100a%ROWTYPE;
    l_r_ord_dtl          ordp120b%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_r_ord_hdr.div);
    logs.add_parm(lar_parm, 'OrdNum', i_r_ord_hdr.ord_num);
    logs.add_parm(lar_parm, 'CustId', i_r_ord_hdr.cust_id);
    logs.add_parm(lar_parm, 'MclCust', i_r_ord_hdr.mcl_cust);
    logs.add_parm(lar_parm, 'LoadTyp', i_r_ord_hdr.load_typ);
    logs.add_parm(lar_parm, 'OrdTyp', i_r_ord_hdr.ord_typ);
    logs.add_parm(lar_parm, 'OrdSrc', i_r_ord_hdr.ord_src);
    logs.add_parm(lar_parm, 'ConfNum', i_r_ord_hdr.conf_num);
    logs.add_parm(lar_parm, 'SerNum', i_r_ord_hdr.ser_num);
    logs.add_parm(lar_parm, 'TrnsmtTs', i_r_ord_hdr.trnsmt_ts);
    logs.add_parm(lar_parm, 'CustPassArea', i_r_ord_hdr.cust_pass_area);
    logs.add_parm(lar_parm, 'HdrExcptnCd', i_r_ord_hdr.hdr_excptn_cd);
    logs.add_parm(lar_parm, 'LegcyRef', i_r_ord_hdr.legcy_ref);
    logs.add_parm(lar_parm, 'PONum', i_r_ord_hdr.po_num);
    logs.add_parm(lar_parm, 'AllwPartlSw', i_r_ord_hdr.allw_partl_sw);
    logs.add_parm(lar_parm, 'ShpDt', i_r_ord_hdr.shp_dt);
    logs.add_parm(lar_parm, 'OrdDtlsTab',(CASE
                     WHEN i_t_ord_dtls IS NULL THEN NULL
                     ELSE 'Count=' || i_t_ord_dtls.COUNT
                   END));
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'OrdComnt', i_ord_comnt);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_r_ord_hdr.div);

    IF (    i_t_ord_dtls IS NOT NULL
        AND i_t_ord_dtls.COUNT > 0) THEN
      logs.dbg('Add Order Header');
      op_ord_hdr_pk.init_sp(l_r_ord_hdr);
      l_ord_num := NVL(i_r_ord_hdr.ord_num, next_ord_num_fn);
      l_r_ord_hdr.ordnoa := l_ord_num;
      l_r_ord_hdr.excptn_sw := 'N';
      l_r_ord_hdr.div_part := l_div_part;
      l_r_ord_hdr.custa := i_r_ord_hdr.cust_id;
      l_r_ord_hdr.load_depart_sid := 0;
      l_r_ord_hdr.ldtypa := i_r_ord_hdr.load_typ;
      l_r_ord_hdr.cpoa := i_r_ord_hdr.po_num;
      l_r_ord_hdr.stata := 'I';
      l_r_ord_hdr.dsorda := i_r_ord_hdr.ord_typ;
      l_r_ord_hdr.ipdtsa := i_r_ord_hdr.ord_src;
      l_r_ord_hdr.connba := i_r_ord_hdr.conf_num;
      l_r_ord_hdr.telsla := i_r_ord_hdr.ser_num;
      l_r_ord_hdr.trndta := TRUNC(i_r_ord_hdr.trnsmt_ts) - DATE '1900-02-28';
      l_r_ord_hdr.trntma := TO_NUMBER(TO_CHAR(i_r_ord_hdr.trnsmt_ts, 'HH24MISS'));
      l_r_ord_hdr.ord_rcvd_ts := SYSDATE;
      l_r_ord_hdr.cspasa := i_r_ord_hdr.cust_pass_area;
      l_r_ord_hdr.hdexpa := i_r_ord_hdr.hdr_excptn_cd;
      l_r_ord_hdr.legrfa := i_r_ord_hdr.legcy_ref;
      l_r_ord_hdr.pshipa :=(CASE
                              WHEN i_r_ord_hdr.allw_partl_sw IN('1', 'Y') THEN 'Y'
                              ELSE 'N'
                            END);
      l_r_ord_hdr.shpja := TRUNC(i_r_ord_hdr.shp_dt) - DATE '1900-02-28';
      l_r_ord_hdr.mntusa := i_user_id;

      INSERT INTO ordp100a
           VALUES l_r_ord_hdr;

      logs.dbg('Add Order Detail Lines');
      op_ord_dtl_pk.init_sp(l_r_ord_dtl);
      l_r_ord_dtl.ordnob := l_ord_num;
      l_r_ord_dtl.div_part := l_div_part;
      l_r_ord_dtl.excptn_sw := 'N';
      l_r_ord_dtl.statb := 'I';
      FOR i IN i_t_ord_dtls.FIRST .. i_t_ord_dtls.LAST LOOP
        l_r_ord_dtl.lineb := i;
        l_r_ord_dtl.orditb := LPAD(i_t_ord_dtls(i).catlg_num, 6, '0');
        l_r_ord_dtl.itemnb := i_t_ord_dtls(i).cbr_item;
        l_r_ord_dtl.sllumb := i_t_ord_dtls(i).uom;
        l_r_ord_dtl.cusitb := i_t_ord_dtls(i).cust_item;
        l_r_ord_dtl.ordqtb := i_t_ord_dtls(i).ord_qty;
        l_r_ord_dtl.itpasb := i_t_ord_dtls(i).item_pass_area;
        l_r_ord_dtl.rtfixb := NVL(i_t_ord_dtls(i).hard_rtl_sw, 'N');
        l_r_ord_dtl.hdrtab := i_t_ord_dtls(i).rtl_amt;
        l_r_ord_dtl.hdrtmb := i_t_ord_dtls(i).rtl_mult;
        l_r_ord_dtl.prfixb := NVL(i_t_ord_dtls(i).hard_price_sw, 'N');
        l_r_ord_dtl.hdprcb := i_t_ord_dtls(i).price_amt;
        l_r_ord_dtl.orgqtb := i_t_ord_dtls(i).orig_qty;
        l_r_ord_dtl.bymaxb := NVL(i_t_ord_dtls(i).byp_max_sw, 'N');
        l_r_ord_dtl.qtmulb := NVL(i_t_ord_dtls(i).qty_mult, 1);

        INSERT INTO ordp120b
             VALUES l_r_ord_dtl;
      END LOOP;
      COMMIT;
      logs.dbg('Send Order to Mainframe');
      send_to_mainframe_sp(i_r_ord_hdr.div, l_ord_num, NULL, NULL, i_ord_comnt);
    END IF;   -- i_t_ord_dtls IS NOT NULL AND i_t_ord_dtls.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_ORD_SP
  ||  Insert order header and details and route to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/22/07 | rhalpai | Original
  || 01/21/08 | rhalpai | Added order confirmation number. PIR3593
  || 12/23/11 | rhalpai | Change logic to remove excepion order well.
  || 05/09/13 | rhalpai | Change logic to set DivId on order header. IM-102610
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_ord_sp(
    i_div        IN  VARCHAR2,
    i_conf_num   IN  VARCHAR2,
    i_test_sw    IN  VARCHAR2,
    i_mcl_cust   IN  VARCHAR2,
    i_cust_id    IN  VARCHAR2,
    i_po_num     IN  VARCHAR2,
    i_load_typ   IN  VARCHAR2,
    i_ord_typ    IN  VARCHAR2,
    i_ord_src    IN  VARCHAR2,
    i_shp_dt     IN  DATE,
    i_user_id    IN  VARCHAR2,
    i_dtl_list   IN  VARCHAR2,
    i_ord_comnt  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm               := 'CSR_ORDERS_PK.INS_ORD_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_catlg_nums       type_stab                   := type_stab();
    l_t_cbr_items        type_stab                   := type_stab();
    l_t_uoms             type_stab                   := type_stab();
    l_t_qtys             type_ntab                   := type_ntab();
    l_r_ord_hdr          csr_orders_pk.g_rt_msg_hdr;
    l_r_ord_dtl          csr_orders_pk.g_rt_msg_dtl;
    l_t_ord_dtls         csr_orders_pk.g_tt_msg_dtls;

    PROCEDURE parse_sp IS
      l_t_grps    type_stab;
      l_idx       PLS_INTEGER;
      l_t_fields  type_stab;
    BEGIN
      logs.dbg('Parse Groups of Parm Field Lists');
      l_t_grps := str.parse_list(i_dtl_list, op_const_pk.grp_delimiter);

      IF l_t_grps IS NOT NULL THEN
        l_idx := l_t_grps.FIRST;
        WHILE l_idx IS NOT NULL LOOP
          l_t_fields := NULL;
          logs.dbg('Parse Parm Field List');
          l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter);
          l_t_catlg_nums.EXTEND;
          l_t_catlg_nums(l_idx) := val_at_idx_fn(l_t_fields, 1);
          l_t_cbr_items.EXTEND;
          l_t_cbr_items(l_idx) := val_at_idx_fn(l_t_fields, 2);
          l_t_uoms.EXTEND;
          l_t_uoms(l_idx) := val_at_idx_fn(l_t_fields, 3);
          l_t_qtys.EXTEND;
          l_t_qtys(l_idx) := val_at_idx_fn(l_t_fields, 4);
          l_idx := l_t_grps.NEXT(l_idx);
        END LOOP;
      END IF;   -- l_t_grps IS NOT NULL
    END parse_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ConfNum', i_conf_num);
    logs.add_parm(lar_parm, 'TestSw', i_test_sw);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'LoadTyp', i_load_typ);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'OrdSrc', i_ord_src);
    logs.add_parm(lar_parm, 'ShpDt', i_shp_dt);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'DtlList', i_dtl_list);
    logs.add_parm(lar_parm, 'OrdComnt', i_ord_comnt);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF TRIM(i_conf_num) IS NOT NULL THEN
      logs.dbg('Parse Parm List');
      parse_sp;

      IF l_t_catlg_nums.COUNT > 0 THEN
        logs.dbg('Setup Order Header');
        l_r_ord_hdr.div_part := l_div_part;
        l_r_ord_hdr.div := i_div;
        l_r_ord_hdr.conf_num := i_conf_num;
        l_r_ord_hdr.ord_typ :=(CASE
                                 WHEN i_test_sw = 'T' THEN 'T'
                                 ELSE i_ord_typ
                               END);
        l_r_ord_hdr.mcl_cust := i_mcl_cust;
        l_r_ord_hdr.cust_id := i_cust_id;
        l_r_ord_hdr.load_typ := i_load_typ;
        l_r_ord_hdr.ord_src := i_ord_src;
        l_r_ord_hdr.shp_dt := i_shp_dt;
        l_r_ord_hdr.trnsmt_ts := SYSDATE;
        l_r_ord_hdr.po_num := i_po_num;
        logs.dbg('Add Order Detail Lines');
        l_t_ord_dtls := csr_orders_pk.g_tt_msg_dtls();
        FOR i IN l_t_catlg_nums.FIRST .. l_t_catlg_nums.LAST LOOP
          l_r_ord_dtl.catlg_num := l_t_catlg_nums(i);
          l_r_ord_dtl.cbr_item := l_t_cbr_items(i);
          l_r_ord_dtl.uom := l_t_uoms(i);
          l_r_ord_dtl.ord_qty := l_t_qtys(i);
          l_r_ord_dtl.orig_qty := l_t_qtys(i);
          l_t_ord_dtls.EXTEND;
          l_t_ord_dtls(l_t_ord_dtls.LAST) := l_r_ord_dtl;
        END LOOP;
        logs.dbg('Insert New Order and Send to Mainframe');
        ins_ord_sp(l_r_ord_hdr, l_t_ord_dtls, i_user_id, i_ord_comnt);
      END IF;   -- l_t_catlg_nums.COUNT > 0
    END IF;   -- TRIM(i_conf_num) IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_ord_sp;
END csr_orders_pk;
/

