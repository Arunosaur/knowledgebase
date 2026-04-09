CREATE OR REPLACE PACKAGE csr_orders_bill_pk IS
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
  FUNCTION query_text_fn(
    i_div       IN  VARCHAR2,
    i_query_nm  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION exception_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION ship_date_list_fn(
    i_div              IN  VARCHAR2,
    i_incl_suspend_sw  IN  VARCHAR2 DEFAULT 'Y'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION query_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION query_dtl_fn(
    i_div              IN  VARCHAR2,
    i_ord_typ          IN  VARCHAR2,
    i_llr_from         IN  VARCHAR2,
    i_llr_to           IN  VARCHAR2,
    i_ship_dt          IN  VARCHAR2,
    i_crp_cd           IN  NUMBER,
    i_grp              IN  VARCHAR2,
    i_excpt_cd         IN  VARCHAR2,
    i_po_num           IN  VARCHAR2,
    i_mfst_list        IN  VARCHAR2,
    i_cust_typ         IN  VARCHAR2 DEFAULT 'MCL',
    i_cust_list        IN  VARCHAR2,
    i_item_typ         IN  VARCHAR2 DEFAULT 'MCL',
    i_item_list        IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_incl_suspend_sw  IN  VARCHAR2 DEFAULT 'Y'
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE save_query_sp(
    i_div        IN  VARCHAR2,
    i_query_nm   IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE rename_query_sp(
    i_div           IN  VARCHAR2,
    i_query_nm      IN  VARCHAR2,
    i_new_query_nm  IN  VARCHAR2,
    i_user_id       IN  VARCHAR2
  );

  PROCEDURE delete_query_sp(
    i_div       IN  VARCHAR2,
    i_query_nm  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  );
END csr_orders_bill_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_orders_bill_pk IS
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
  || QUERY_TEXT
  ||  Return the associated query text for input query name.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/07 | vxranga | Original, PIR:799, Venkateswaran Ranganathan
  ||----------------------------------------------------------------------------
  */
  FUNCTION query_text_fn(
    i_div       IN  VARCHAR2,
    i_query_nm  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                    := 'CSR_ORDERS_BILL_PK.QUERY_TEXT';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_query_txt          billquery_qu1a.query_txt%TYPE;
    l_query_nm           billquery_qu1a.query_name%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'QueryNm', i_query_nm);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_query_nm := UPPER(TRIM(i_query_nm));

    OPEN l_cv
     FOR
       SELECT q.query_txt
         FROM div_mstr_di1d d, billquery_qu1a q
        WHERE d.div_id = i_div
          AND q.div_part = d.div_part
          AND q.query_name = l_query_nm;

    FETCH l_cv
     INTO l_query_txt;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_query_txt);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END query_text_fn;

  /*
  ||----------------------------------------------------------------------------
  || EXCEPTION_LIST_FN
  ||  Retrieve a list of exception descriptions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/28/07 | vxranga | Original, PIR:799, Venkateswaran Ranganathan
  ||----------------------------------------------------------------------------
  */
  FUNCTION exception_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_BILL_PK.EXCEPTION_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   a.rsncda, a.desca
           FROM mclp140a a
       GROUP BY a.rsncda, a.desca
       ORDER BY a.desca;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END exception_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || SHIP_DATE_LIST_FN
  ||  Build a list of Ship Dates for open (unassigned) distribution orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/28/07 | VXRANGA | Original. PIR799, Venkateswaran Ranganathan
  || 02/21/08 | VXRANGA | Modifications. PIR5804, Venkateswaran Ranganathan
  ||                    | Changes towards including switch for inclusion of
  ||                    | suspended orders and ship dates for DIST and P00* loads
  || 06/16/08 | rhalpai | Added sort by ShipDt to cursor.
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ship_date_list_fn(
    i_div              IN  VARCHAR2,
    i_incl_suspend_sw  IN  VARCHAR2 DEFAULT 'Y'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_BILL_PK.SHIP_DATE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'InclSuspendSw', i_incl_suspend_sw);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')
           FROM div_mstr_di1d d, load_depart_op1f ld, ordp100a a
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_ts = DATE '1900-01-01'
            AND (   ld.load_num = 'DIST'
                 OR ld.load_num BETWEEN 'P00P' AND 'P99P')
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.stata IN('O', 'I', DECODE(i_incl_suspend_sw, 'Y', 'S'))
            AND a.div_part = d.div_part
            AND a.dsorda = 'D'
            AND a.excptn_sw = 'N'
       GROUP BY a.shpja
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ship_date_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || QUERY_LIST_FN
  ||  Return cursor of query information used by bill query screen.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/28/07 | VXRANGA | Original - created for PIR799
  ||----------------------------------------------------------------------------
  */
  FUNCTION query_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_BILL_PK.QUERY_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   q.query_name, q.query_txt
           FROM div_mstr_di1d d, billquery_qu1a q
          WHERE d.div_id = i_div
            AND q.div_part = d.div_part
       ORDER BY q.query_name;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END query_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || QUERY_DTL_FN
  ||  Return the associated query text for input query name.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/07 | vxranga | Original, PIR:799, Venkateswaran Ranganathan
  || 02/21/08 | VXRANGA | Modifications. PIR5804, Venkateswaran Ranganathan
  ||                    | Changes towards including item desc/pack/size,
  ||                    | DIST/P**P (P00P to P99P) orders, query by single order#,
  ||                    | and inclusion of mainframe/suspended order lines
  || 05/21/08 | VXRANGA | Modifications. IM411196, Venkateswaran Ranganathan
  ||                    | - Changes to show the "Not Ship Reason" description
  ||                    |   if available else show "Not Ship Reason" code.
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 04/30/15 | rhalpai | Add CBR Cust column to returned cursor. PIR14890
  ||----------------------------------------------------------------------------
  */
  FUNCTION query_dtl_fn(
    i_div              IN  VARCHAR2,
    i_ord_typ          IN  VARCHAR2,
    i_llr_from         IN  VARCHAR2,
    i_llr_to           IN  VARCHAR2,
    i_ship_dt          IN  VARCHAR2,
    i_crp_cd           IN  NUMBER,
    i_grp              IN  VARCHAR2,
    i_excpt_cd         IN  VARCHAR2,
    i_po_num           IN  VARCHAR2,
    i_mfst_list        IN  VARCHAR2,
    i_cust_typ         IN  VARCHAR2 DEFAULT 'MCL',
    i_cust_list        IN  VARCHAR2,
    i_item_typ         IN  VARCHAR2 DEFAULT 'MCL',
    i_item_list        IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_incl_suspend_sw  IN  VARCHAR2 DEFAULT 'Y'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ORDERS_BILL_PK.QUERY_DTL_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_ship_dt            NUMBER;
    l_t_xloads           type_stab;
    l_t_custs            type_stab;
    l_t_items            type_stab;
    l_t_mfsts            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'LLRFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LLRTo', i_llr_to);
    logs.add_parm(lar_parm, 'ShipDt', i_ship_dt);
    logs.add_parm(lar_parm, 'CorpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'Grp', i_grp);
    logs.add_parm(lar_parm, 'ExcptCd', i_excpt_cd);
    logs.add_parm(lar_parm, 'PONum', i_po_num);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'CustTyp', i_cust_typ);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'ItemTyp', i_item_typ);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'UserID', i_user_id);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'InclSuspendSw', i_incl_suspend_sw);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_from := TO_DATE(i_llr_from, 'YYYY-MM-DD');
    l_llr_to := TO_DATE(i_llr_to, 'YYYY-MM-DD');
    l_ship_dt := TO_DATE(i_ship_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Parse manifest, customer, item list');
    l_t_mfsts := str.parse_list(i_mfst_list);
    l_t_custs := str.parse_list(i_cust_list);
    l_t_items := str.parse_list(i_item_list);

    IF i_ord_num IS NOT NULL THEN
      -- Query by OrderNo - this IF block was added
      -- for faster retrieval using single order# (does NOT query by All/Reg/Dist
      -- and related LLR/Ship date values)
      logs.dbg('Open Cursor - Query By Order Num');

      OPEN l_cv
       FOR
         SELECT   LPAD(cx.corpb, 3, '0') AS crp, c.retgpc AS grp, cx.mccusb, cx.custb, c.namec,
                  TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_dt, SUBSTR(a.legrfa, 1, 10) AS dist_id,
                  SUBSTR(a.legrfa, 12, 2) AS dist_sfx, a.ldtypa AS ld_typ, a.cpoa AS po_num, b.ordnob AS ord_num,
                  b.orditb AS mcl_item, b.hdrtab AS rtl_amt, b.hdprcb AS price_amt, b.ordqtb,
                  NVL(ma.desca, b.ntshpb) AS nt_shp_rsn, e.shppke AS pack, e.sizee AS sz, e.ctdsce AS descr,
                  DECODE(b.statb, 'O', 'Open', 'S', 'Suspended', 'I', 'Mainframe', b.statb) AS ln_stat,
                  a.dsorda AS ord_typ, ld.load_num, TO_CHAR(ld.llr_ts, 'YYYY-MM-DD') AS llr_dt
             FROM ordp100a a, load_depart_op1f ld, ordp120b b, mclp020b cx, sysp200c c, sawp505e e, mclp140a ma
            WHERE a.div_part = l_div_part
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
              AND c.div_part = a.div_part
              AND c.acnoc = a.custa
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.statb IN('O', 'I', DECODE(i_incl_suspend_sw, 'Y', 'S'))
              AND e.iteme(+) = b.itemnb
              AND e.uome(+) = b.sllumb   -- comparison of item# and uom here uses resp index (uses outer join)
              AND ma.rsncda(+) = b.ntshpb
              AND (   -- Query by order#
                   b.ordnob = i_ord_num)
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd)
              AND (   i_grp IS NULL
                   OR c.retgpc = i_grp)
              AND (   i_excpt_cd IS NULL
                   OR b.ntshpb = i_excpt_cd)
              AND (   i_po_num IS NULL
                   OR a.cpoa LIKE i_po_num || '%')
              AND (   i_mfst_list IS NULL
                   OR b.manctb IN(SELECT t.column_value
                                    FROM TABLE(CAST(l_t_mfsts AS type_stab)) t))
              AND (   i_cust_list IS NULL
                   OR (   (    i_cust_typ = 'MCL'
                           AND cx.mccusb IN(SELECT t.column_value
                                              FROM TABLE(CAST(l_t_custs AS type_stab)) t))
                       OR (    i_cust_typ = 'CBR'
                           AND cx.custb IN(SELECT t.column_value
                                             FROM TABLE(CAST(l_t_custs AS type_stab)) t))
                      )
                  )
              AND (   i_item_list IS NULL
                   OR (   (    i_item_typ = 'MCL'
                           AND b.orditb IN(SELECT t.column_value
                                             FROM TABLE(CAST(l_t_items AS type_stab)) t))
                       OR (    i_item_typ = 'CBR'
                           AND b.itemnb IN(SELECT t.column_value
                                             FROM TABLE(CAST(l_t_items AS type_stab)) t))
                      )
                  )
         ORDER BY 1, 2, 4, 6, 9;
    --ORDER BY CORP, GROUP, CUST #, SHIPDATE, ORDERTYPE
    ELSIF i_ord_num IS NULL THEN
      -- Query by Other - this ELSIF block was added to query by
      -- Other query by values (EXCEPT order#) such as All/Reg/Dist
      -- and related LLR/Ship dates resp.
      logs.dbg('Open Cursor - Query By: '
               ||(CASE
                    WHEN i_ord_typ = 'R' THEN 'Regular'
                    WHEN i_ord_typ = 'D' THEN 'Dist'
                    ELSE 'All'
                  END)
               || ' orders.'
              );

      OPEN l_cv
       FOR
         SELECT   LPAD(cx.corpb, 3, '0') AS crp, c.retgpc AS grp, cx.mccusb, cx.custb, c.namec,
                  TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD') AS ship_dt, SUBSTR(a.legrfa, 1, 10) AS dist_id,
                  SUBSTR(a.legrfa, 12, 2) AS dist_sfx, a.ldtypa AS ld_typ, a.cpoa AS po_num, b.ordnob AS ord_num,
                  b.orditb AS mcl_item, b.hdrtab AS rtl_amt, b.hdprcb AS price_amt, b.ordqtb,
                  NVL(ma.desca, b.ntshpb) AS nt_shp_rsn, e.shppke AS pack, e.sizee AS sz, e.ctdsce AS descr,
                  DECODE(b.statb, 'O', 'Open', 'S', 'Suspended', 'I', 'Mainframe', b.statb) AS ln_stat,
                  a.dsorda AS ord_typ, ld.load_num, TO_CHAR(ld.llr_ts, 'YYYY-MM-DD') AS llr_dt
             FROM ordp100a a, load_depart_op1f ld, ordp120b b, mclp020b cx, sysp200c c, sawp505e e, mclp140a ma
            WHERE ld.div_part = l_div_part
              AND (   NVL(i_ord_typ, 'D') = 'D'
                   OR ld.load_num NOT BETWEEN 'P00P' AND 'P99P')
              AND ld.load_num NOT IN(SELECT t.column_value
                                       FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                                      WHERE t.column_value <> 'DIST'
                                        AND t.column_value NOT BETWEEN 'P00P' AND 'P99P')
              AND a.div_part = ld.div_part
              AND a.load_depart_sid = ld.load_depart_sid
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
              AND c.div_part = a.div_part
              AND c.acnoc = a.custa
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.statb IN('O', 'I', DECODE(i_incl_suspend_sw, 'Y', 'S'))
              AND b.itemnb = e.iteme(+)
              AND e.uome(+) = b.sllumb   -- comparison of item# and uom here uses resp index
              AND ma.rsncda(+) = b.ntshpb
              AND (   -- All <Reg and Dist> (Or) Dist Order Type
                      (    NVL(i_ord_typ, 'D') = 'D'
                       AND a.dsorda = 'D'   -- newly added 2008-02-19
                       AND l_ship_dt > 0
                       AND a.shpja <= l_ship_dt
                      )
                   OR (    NVL(i_ord_typ, 'R') = 'R'
                       AND a.dsorda = 'R'
                       AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to)
                  )
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd)
              AND (   i_grp IS NULL
                   OR c.retgpc = i_grp)
              AND (   i_excpt_cd IS NULL
                   OR b.ntshpb = i_excpt_cd)
              AND (   i_po_num IS NULL
                   OR a.cpoa LIKE i_po_num || '%')
              AND (   i_mfst_list IS NULL
                   OR b.manctb IN(SELECT t.column_value
                                    FROM TABLE(CAST(l_t_mfsts AS type_stab)) t))
              AND (   i_cust_list IS NULL
                   OR (   (    i_cust_typ = 'MCL'
                           AND cx.mccusb IN(SELECT t.column_value
                                              FROM TABLE(CAST(l_t_custs AS type_stab)) t))
                       OR (    i_cust_typ = 'CBR'
                           AND cx.custb IN(SELECT t.column_value
                                             FROM TABLE(CAST(l_t_custs AS type_stab)) t))
                      )
                  )
              AND (   i_item_list IS NULL
                   OR (   (    i_item_typ = 'MCL'
                           AND b.orditb IN(SELECT t.column_value
                                             FROM TABLE(CAST(l_t_items AS type_stab)) t))
                       OR (    i_item_typ = 'CBR'
                           AND b.itemnb IN(SELECT t.column_value
                                             FROM TABLE(CAST(l_t_items AS type_stab)) t))
                      )
                  )
         ORDER BY 1, 2, 4, 6, 9;
    --ORDER BY CORP, GROUP, CUST #, SHIPDATE, ORDERTYPE
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END query_dtl_fn;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_QUERY_SP
  ||  Save query used by bill query screen.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/24/27 | VXRANGA | Original - created for PIR799
  ||
  || parms_list --> OrdTyp`LLRFrom`LLRTo`ShipDt`Corp`Group`ExcptCd`PO`Mfsts`CustType`Custs`ItemType`Items`OrderNo`includeSuspended
  || Example: parms_list --> OrdTyp`LLRFrom`LLRTo`ShipDt`Corp`Group`ExcptCd`PO`Mfst1,Mfst2,Mfst3`CustType`Cust1,Cust2,Cust3`ItemType`Item1,Item2,Item3`orderNo`includeSuspended
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_query_sp(
    i_div        IN  VARCHAR2,
    i_query_nm   IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                    := 'CSR_ORDERS_BILL_PK.SAVE_QUERY_SP';
    lar_parm             logs.tar_parm;
    l_query_nm           billquery_qu1a.query_name%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'QueryNm', i_query_nm);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserID', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_query_nm := UPPER(TRIM(i_query_nm));
    excp.assert((l_query_nm IS NOT NULL), 'Query name required!');
    logs.dbg('Upsert');
    MERGE INTO billquery_qu1a q
         USING (SELECT d.div_part
                  FROM div_mstr_di1d d
                 WHERE d.div_id = i_div) x
            ON (    q.div_part = x.div_part
                AND q.query_name = l_query_nm)
      WHEN MATCHED THEN   -- Entry exist - Update the entry for query
        UPDATE
           SET q.query_txt = i_parm_list, q.user_id = i_user_id, q.last_chg_ts = SYSDATE
      WHEN NOT MATCHED THEN   --Entry does NOT exist - Insert an entry for query
        INSERT(q.div_part, q.query_name, q.query_txt, q.user_id, q.last_chg_ts)
        VALUES(x.div_part, l_query_nm, i_parm_list, i_user_id, SYSDATE);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END save_query_sp;

  /*
  ||----------------------------------------------------------------------------
  || RENAME_QUERY_SP
  ||  Rename query information used by bill query screen.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/07 | VXRANGA | Original - created for PIR799
  ||----------------------------------------------------------------------------
  */
  PROCEDURE rename_query_sp(
    i_div           IN  VARCHAR2,
    i_query_nm      IN  VARCHAR2,
    i_new_query_nm  IN  VARCHAR2,
    i_user_id       IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                    := 'CSR_ORDERS_BILL_PK.RENAME_QUERY_SP';
    lar_parm             logs.tar_parm;
    l_query_nm           billquery_qu1a.query_name%TYPE;
    l_new_query_nm       billquery_qu1a.query_name%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'QueryNm', i_query_nm);
    logs.add_parm(lar_parm, 'NewQueryNm', i_new_query_nm);
    logs.add_parm(lar_parm, 'UserID', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_query_nm := UPPER(TRIM(i_query_nm));
    l_new_query_nm := UPPER(TRIM(i_new_query_nm));
    excp.assert((l_new_query_nm IS NOT NULL), 'New query name required!');
    logs.dbg('Rename Query');

    UPDATE billquery_qu1a q
       SET q.query_name = l_new_query_nm,
           q.user_id = i_user_id,
           q.last_chg_ts = SYSDATE
     WHERE q.div_part = (SELECT d.div_part
                           FROM div_mstr_di1d d
                          WHERE d.div_id = i_div)
       AND q.query_name = l_query_nm;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rename_query_sp;

  /*
  ||----------------------------------------------------------------------------
  || DELETE_QUERY_SP
  ||  Delete query information used by bill query screen.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/04/07 | VXRANGA | Original - created for PIR799
  ||----------------------------------------------------------------------------
  */
  PROCEDURE delete_query_sp(
    i_div       IN  VARCHAR2,
    i_query_nm  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                    := 'CSR_ORDERS_BILL_PK.DELETE_QUERY_SP';
    lar_parm             logs.tar_parm;
    l_query_nm           billquery_qu1a.query_name%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'QueryNm', i_query_nm);
    logs.add_parm(lar_parm, 'UserID', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_query_nm := UPPER(TRIM(i_query_nm));
    logs.dbg('Del');

    DELETE FROM billquery_qu1a q
          WHERE q.div_part = (SELECT d.div_part
                                FROM div_mstr_di1d d
                               WHERE d.div_id = i_div)
            AND q.query_name = l_query_nm;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END delete_query_sp;
END csr_orders_bill_pk;
/

