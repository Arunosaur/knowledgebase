CREATE OR REPLACE PACKAGE op_item_pk IS
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
  FUNCTION catlg_num_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  )
    RETURN NUMBER RESULT_CACHE;

  FUNCTION catlg_num_str_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  ) RETURN VARCHAR2 RESULT_CACHE;

  PROCEDURE cbr_item_sp(
    i_catlg_num  IN      NUMBER,
    o_cbr_item   OUT     VARCHAR2,
    o_uom        OUT     VARCHAR2
  );

  FUNCTION inv_item_fn(
    i_div_part   IN  NUMBER,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION inv_item_fn(
    i_div        IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION item_info_fn(
    i_item_num  IN  VARCHAR2,
    i_uom       IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION corp_item_search_fn(
    i_search_str  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION search_fn(
    i_div       IN  VARCHAR2,
    i_descr     IN  VARCHAR2,
    i_item_num  IN  VARCHAR2 DEFAULT NULL,
    i_typ       IN  VARCHAR2 DEFAULT 'MCL'
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END op_item_pk;
/

CREATE OR REPLACE PACKAGE BODY op_item_pk IS
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
  || CATLG_NUM_FN
  ||  Get McLane Catalog Item number for CBR Item/UOM.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/08/15 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION catlg_num_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  )
    RETURN NUMBER RESULT_CACHE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ITEM_PK.CATLG_NUM_FN';
    lar_parm             logs.tar_parm;
    l_catlg_num          NUMBER;
  BEGIN
    logs.add_parm(lar_parm, 'CbrItem', i_cbr_item);
    logs.add_parm(lar_parm, 'UOM', i_uom);

    SELECT e.catite
      INTO l_catlg_num
      FROM sawp505e e
     WHERE e.iteme = i_cbr_item
       AND e.uome = i_uom;

    RETURN(l_catlg_num);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END catlg_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || CATLG_NUM_STR_FN
  ||  Get McLane Catalog Item number in zero padded string format for CBR Item/UOM.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/08/15 | rhalpai | Original
  || 01/04/16 | rhalpai | Change RPAD to LPAD. SDOPS-117
  ||----------------------------------------------------------------------------
  */
  FUNCTION catlg_num_str_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  )
    RETURN VARCHAR2 RESULT_CACHE IS
  BEGIN
    RETURN(LPAD(catlg_num_fn(i_cbr_item, i_uom), 6, '0'));
  END catlg_num_str_fn;

  /*
  ||----------------------------------------------------------------------------
  || CBR_ITEM_SP
  ||  Get CBR Item number and UOM for McLane Catalog Item number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/08/15 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cbr_item_sp(
    i_catlg_num  IN      NUMBER,
    o_cbr_item   OUT     VARCHAR2,
    o_uom        OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ITEM_PK.CBR_ITEM_SP';
    lar_parm             logs.tar_parm;
    l_catlg_num_str      VARCHAR2(6);
  BEGIN
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    l_catlg_num_str := LPAD(i_catlg_num, 6, '0');

    SELECT e.iteme, e.uome
      INTO o_cbr_item, o_uom
      FROM sawp505e e
     WHERE e.catite = l_catlg_num_str;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cbr_item_sp;

  /*
  ||----------------------------------------------------------------------------
  || INV_ITEM_FN
  ||  Will return the inventory catalog item for a non-inventory item when found
  ||  otherwise return the passed item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/15/17 | rhalpai | Moved from OP_PROTECTED_INVENTORY_PK.
  ||----------------------------------------------------------------------------
  */
  FUNCTION inv_item_fn(
    i_div_part   IN  NUMBER,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_ITEM_PK.INV_ITEM_FN';
    lar_parm             logs.tar_parm;
    l_inv_item           sawp505e.catite%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.dbg('ENTRY', lar_parm);
    l_inv_item := i_catlg_num;

    BEGIN
      SELECT s.catite
        INTO l_inv_item
        FROM sawp505e i, mclp110b di, sawp505e s
       WHERE i.catite = i_catlg_num
         AND di.div_part = i_div_part
         AND di.statb = 'ACT'
         AND di.itemb = i.iteme
         AND di.uomb = i.uome
         AND s.iteme = di.sitemb
         AND s.uome = di.suomb;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_inv_item);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END inv_item_fn;

  FUNCTION inv_item_fn(
    i_div        IN  VARCHAR2,
    i_catlg_num  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN(inv_item_fn(div_pk.div_part_fn(i_div), i_catlg_num));
  END inv_item_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_INFO_FN
  ||  Return cursor of item information.
  ||
  ||  Item with UOM    = CBR Item
  ||  Item without UOM = McLane Catalog Item
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/29/06 | rhalpai | Original PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_info_fn(
    i_item_num  IN  VARCHAR2,
    i_uom       IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ITEM_PK.ITEM_INFO_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Item', i_item_num);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT e.catite, e.iteme, e.uome, e.ctdsce, e.shppke, e.sizee
         FROM sawp505e e
        WHERE (   (    e.catite = i_item_num
                   AND i_uom IS NULL)
               OR (    e.iteme = i_item_num
                   AND e.uome = i_uom));

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || CORP_ITEM_SEARCH_FN
  ||  Return cursor of corporate item information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/15/17 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION corp_item_search_fn(
    i_search_str  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ITEM_PK.CORP_ITEM_SEARCH_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_search_str         typ.t_maxcol;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'SearchStr', i_search_str);
    logs.dbg('ENTRY', lar_parm);

    IF num.ianb(i_search_str) THEN
      OPEN l_cv
       FOR
         SELECT   e.catite AS catlg_num, e.iteme AS cbr_item, e.uome AS uom, e.ctdsce AS descr, e.shppke AS pack,
                  e.sizee AS sz
             FROM sawp505e e
            WHERE e.catite LIKE i_search_str || '%'
               OR e.ctdsce LIKE '%' || i_search_str || '%'
         ORDER BY e.catite;
    ELSE
      l_search_str := '%' || UPPER(i_search_str) || '%';

      OPEN l_cv
       FOR
         SELECT   e.catite AS catlg_num, e.iteme AS cbr_item, e.uome AS uom, e.ctdsce AS descr, e.shppke AS pack,
                  e.sizee AS sz
             FROM sawp505e e
            WHERE UPPER(e.ctdsce) LIKE l_search_str
         ORDER BY e.ctdsce;
    END IF;   -- num.ianb(i_search_str)

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END corp_item_search_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEARCH_FN
  ||  Return cursor of item information for active/discontinued items starting
  ||  with passed value.
  ||
  ||  Valid Types:
  ||  MCL = McLane Catalog Item
  ||  CBR = CBR Item
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/29/06 | rhalpai | Original PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION search_fn(
    i_div       IN  VARCHAR2,
    i_descr     IN  VARCHAR2,
    i_item_num  IN  VARCHAR2 DEFAULT NULL,
    i_typ       IN  VARCHAR2 DEFAULT 'MCL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ITEM_PK.SEARCH_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_typ                VARCHAR2(3);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'ItemNum', i_item_num);
    logs.add_parm(lar_parm, 'Typ', i_typ);
    logs.dbg('ENTRY', lar_parm);
    l_typ := NVL(i_typ, 'MCL');

    OPEN l_cv
     FOR
       SELECT   e.catite, e.iteme, e.uome, e.ctdsce, e.shppke, e.sizee, di.statb
           FROM div_mstr_di1d d, sawp505e e, mclp110b di
          WHERE d.div_id = i_div
            AND di.div_part = d.div_part
            AND di.itemb = e.iteme
            AND di.uomb = e.uome
            AND di.statb IN('ACT', 'DIS')
            AND (   i_descr IS NULL
                 OR UPPER(e.ctdsce) LIKE UPPER(i_descr || '%'))
            AND (   i_item_num IS NULL
                 OR (    l_typ = 'MCL'
                     AND e.catite LIKE i_item_num || '%')
                 OR (    l_typ = 'CBR'
                     AND e.iteme LIKE i_item_num || '%')
                )
       ORDER BY (CASE
                   WHEN i_descr IS NOT NULL THEN e.ctdsce
                   WHEN l_typ = 'MCL' THEN e.catite
                   ELSE e.iteme || e.uome
                 END);

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END search_fn;
END op_item_pk;
/

