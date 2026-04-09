CREATE OR REPLACE PACKAGE csr_items_pk IS
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
  FUNCTION mcl_item_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  )
    RETURN VARCHAR2;

  /*
  ||----------------------------------------------------------------------------
  || Indicate whether a catalog (McLane) item exists in active status.
  || #param i_div              Div Id
  || #param i_mcl_item         McLane order item number
  || #return                   'Y' when active item found
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_valid_item_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  /*
  ||----------------------------------------------------------------------------
  || Get cursor of item info for a catalog (McLane) item.
  || #param i_mcl_item         McLane order item number
  || #return                   Cursor of item info
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_item_info_fn(
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_item_info_batch_fn(
    i_div        IN  VARCHAR2,
    i_item_list  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_item_availability_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_item_history_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2,
    i_cbr_cust  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_item_subs_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION search_by_mclane_item_no_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION search_by_description_fn(
    i_div    IN  VARCHAR2,
    i_descr  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE cbr_item_sp(
    i_mcl_item  IN      VARCHAR2,
    o_cbr_item  OUT     VARCHAR2,
    o_uom       OUT     VARCHAR2
  );
END csr_items_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_items_pk IS
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
  || MCL_ITEM_FN
  ||  Get McLane Item number for CBR Item number.
  ||  CBR item numbers are not unique unless the UOM value is included.
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/06/05 | snagabh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION mcl_item_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_ITEMS_PK.MCL_ITEM_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_mcl_item           sawp505e.catite%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CbrItem', i_cbr_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT e.catite
         FROM sawp505e e
        WHERE e.iteme = i_cbr_item
          AND e.uome = i_uom;

    FETCH l_cv
     INTO l_mcl_item;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_mcl_item);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mcl_item_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_VALID_ITEM_FN
  ||  Indicate whether a catalog (McLane) item exists in active status.
  ||
  ||  This function is called by ItemDS.isValid (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 08/07/06 | rhalpai | Changed cursor to include divisional item table.
  ||                    | IM244358
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_valid_item_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.IS_VALID_ITEM_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_valid_sw           VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM sawp505e e, mclp110b di
        WHERE e.catite = i_mcl_item
          AND di.div_part = l_div_part
          AND di.itemb = e.iteme
          AND di.uomb = e.uome
          AND di.statb IN('ACT', 'DIS');

    FETCH l_cv
     INTO l_valid_sw;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_valid_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_valid_item_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ITEM_INFO_FN
  ||  Get item info.
  ||
  ||  This function is called by ItemDS.getCalculateData (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_item_info_fn(
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.GET_ITEM_INFO_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT iteme, uome, ctdsce, shppke, sizee
         FROM sawp505e
        WHERE catite = i_mcl_item;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_item_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ITEM_INFO_BATCH_FN
  ||  Return item info for list of item numbers passed.
  ||
  ||  Input:  Comma delimited McLane Item Numbers
  ||  Output: Ref Cursor containing details of item numbers passed.
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/09/06 | snagabh | Original
  || 08/07/06 | rhalpai | Changed cursor to include divisional item table.
  ||                    | IM244358
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_item_info_batch_fn(
    i_div        IN  VARCHAR2,
    i_item_list  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.GET_ITEM_INFO_BATCH_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_item_list          typ.t_maxvc2;
    l_t_items            type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_item_list := REPLACE(REPLACE(i_item_list, ' '), '''');
    l_t_items := str.parse_list(l_item_list);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT t.column_value, i.iteme, i.uome, i.ctdsce, i.shppke, i.sizee
         FROM TABLE(CAST(l_t_items AS type_stab)) t,
              (SELECT e.catite, e.iteme, e.uome, e.ctdsce, e.shppke, e.sizee
                 FROM TABLE(CAST(l_t_items AS type_stab)) t, sawp505e e, mclp110b di
                WHERE e.catite = t.column_value
                  AND di.div_part = l_div_part
                  AND di.itemb = e.iteme
                  AND di.uomb = e.uome
                  AND di.statb IN('ACT', 'DIS')) i
        WHERE i.catite(+) = t.column_value;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_item_info_batch_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ITEM_AVAILABILITY_FN
  ||  Get item availability info.
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/05 | snagabh | Original
  || 08/07/06 | rhalpai | Added div parm and changed cursor to use it. IM244358
  || 11/10/10 | rhalpai | Removed references to tables for committed qty since
  ||                    | these tables are not maintained. Convert to use
  ||                    | standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_item_availability_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.GET_ITEM_AVAILABILITY_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   SUM(x.qohc - x.qalc - NVL(x.comqt, 0)) AS qty_available, SUM(x.qohc) AS qty_onhand,
                SUM(NVL(x.comqt, 0)) AS qty_committed, SUM(x.qalc) AS qty_allocated
           FROM (SELECT e.catite, NVL(c.qohc, 0) AS qohc, NVL(c.qalc, 0) AS qalc, 0 AS comqt
                   FROM div_mstr_di1d d, whsp300c c, sawp505e e
                  WHERE d.div_id = i_div
                    AND c.div_part = d.div_part
                    AND c.itemc = e.iteme
                    AND c.uomc = e.uome
                    AND e.catite = i_mcl_item) x
       GROUP BY x.catite;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_item_availability_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ITEM_HISTORY_FN
  ||  Get item ordering history for a given customer.
  ||
  ||  Parameters: 2  - Mclane Item Number  &
  ||                   CBR Customer Number
  ||
  ||  ****** NOTE: Need index created on ordp920b for this to run fast.
  ||  MCLANE ORDP920B_IDX_003  Normal  STATB, ORDITB, SLLUMB N   N tablespace op_ord_history01 pctfree 20 initrans 2 maxtrans 255 storage ( initial 3712k next 128k minextents 1 maxextents unlimited pctincrease 0 )
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/05 | snagabh | Original
  || 06/16/08 | rhalpai | Added sort by ShipDt/Pack to cursor.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  || 07/04/13 | rhalpai | Change reference CustId on OrdHdr. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_item_history_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2,
    i_cbr_cust  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.GET_ITEM_HISTORY_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.add_parm(lar_parm, 'CbrCust', i_cbr_cust);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   SUM(b.ordqtb) AS sold, TO_CHAR(DATE '1900-02-28' + b.depdtb, 'MM/DD/YYYY') AS shipdate,
                SUM(b.pckqtb) AS shipped, e.shppke AS pack
           FROM sawp505e e, ordp900a a, ordp920b b
          WHERE e.catite = i_mcl_item
            AND a.div_part = l_div_part
            AND a.custa = i_cbr_cust
            AND b.div_part = a.div_part
            AND b.ordnob = a.ordnoa
            AND b.statb <> 'C'
            AND b.orditb = e.catite
       GROUP BY b.depdtb, e.shppke
       ORDER BY b.depdtb, e.shppke;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_item_history_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_ITEM_SUBS_FN
  ||  Get list of Substutions allowed for given McLane Item number.
  ||
  ||  Parameters: 1  - Mclane Item Number
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/05 | snagabh | Original
  || 11/04/09 | rhalpai | Converted to use SUB_MSTR_OP5S. PIR4342
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_item_subs_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.GET_ITEM_SUBS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   e.catite AS subitem, e.ctdsce AS subdesc
           FROM sub_mstr_op5s s, sawp505e e
          WHERE s.div_part = l_div_part
            AND s.catlg_num = i_mcl_item
            AND s.sub_typ IN('UCS', 'UGP', 'CCS', 'CGP', 'DIV', 'RPI')
            AND e.catite = s.sub_item
       GROUP BY e.catite, e.ctdsce;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_item_subs_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEARCH_BY_MCLANE_ITEM_NO_FN
  ||  Return a list of matching items when searching by Mclane Item Number.
  ||  User can pass a partial item number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/17/04 | SNAGABH | Original
  || 02/26/06 | SNAGABH | Updated to only return Active items.
  || 03/16/06 | SNAGABH | Updated to return DIS (Discontinued) Items in addition
  ||                    | to ACT (Active) items.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION search_by_mclane_item_no_fn(
    i_div       IN  VARCHAR2,
    i_mcl_item  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.SEARCH_BY_MCLANE_ITEM_NO_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF LENGTH(i_mcl_item) = 6 THEN
      logs.dbg('Retrieve Item');

      OPEN l_cv
       FOR
         SELECT e.iteme, e.catite, e.ctdsce, e.sizee, e.shppke, e.uome
           FROM sawp505e e, mclp110b di
          WHERE e.catite = i_mcl_item
            AND di.div_part = l_div_part
            AND di.itemb = e.iteme
            AND di.uomb = e.uome
            AND di.statb IN('ACT', 'DIS');
    ELSE
      logs.dbg('Retrieve list of Items matching partial Item Number');

      OPEN l_cv
       FOR
         SELECT   e.iteme, e.catite, e.ctdsce, e.sizee, e.shppke, e.uome
             FROM sawp505e e
            WHERE e.catite LIKE i_mcl_item || '%'
              AND EXISTS(SELECT 1
                           FROM mclp110b di
                          WHERE di.div_part = l_div_part
                            AND di.itemb = e.iteme
                            AND di.uomb = e.uome
                            AND di.statb IN('ACT', 'DIS'))
         ORDER BY e.catite;
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END search_by_mclane_item_no_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEARCH_BY_DESCRIPTION_FN
  ||  Return a list of matching items when searching by description.
  ||  User can pass a partial item description.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/17/04 | SNAGABH | Original
  || 02/26/06 | SNAGABH | Updated to only return Active items.
  || 03/16/06 | SNAGABH | Updated to return DIS (Discontinued) Items in addition
  ||                    | to ACT (Active) items.
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  FUNCTION search_by_description_fn(
    i_div    IN  VARCHAR2,
    i_descr  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.SEARCH_BY_DESCRIPTION_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   e.iteme, e.catite, e.ctdsce, e.sizee, e.shppke, e.uome
           FROM sawp505e e
          WHERE UPPER(e.ctdsce) LIKE UPPER(i_descr || '%')
            AND EXISTS(SELECT 1
                         FROM mclp110b di
                        WHERE di.div_part = l_div_part
                          AND di.itemb = e.iteme
                          AND di.uomb = e.uome
                          AND di.statb IN('ACT', 'DIS'))
       ORDER BY e.catite;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END search_by_description_fn;

  /*
  ||----------------------------------------------------------------------------
  || CBR_ITEM_SP
  ||  Get CBR Item number and UOM for McLane Item number.
  ||  McLane Item numbers are unique. So we don't need the UOM value.
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/06/05 | snagabh | Original
  || 11/10/10 | rhalpai | Convert to use standard error handling logic. PIR5878
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cbr_item_sp(
    i_mcl_item  IN      VARCHAR2,
    o_cbr_item  OUT     VARCHAR2,
    o_uom       OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_ITEMS_PK.CBR_ITEM_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'MclItem', i_mcl_item);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT e.iteme, e.uome
         FROM sawp505e e
        WHERE e.catite = i_mcl_item;

    FETCH l_cv
     INTO o_cbr_item, o_uom;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cbr_item_sp;
END csr_items_pk;
/

