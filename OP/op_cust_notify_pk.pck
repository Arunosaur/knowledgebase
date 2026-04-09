CREATE OR REPLACE PACKAGE op_cust_notify_pk IS
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
  FUNCTION cust_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  DATE
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------

END op_cust_notify_pk;
/

CREATE OR REPLACE PACKAGE BODY op_cust_notify_pk IS
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
  ||-----------------------------------------------------------------------------
  || CUST_LIST_FN
  ||  Returns cursor of Load/Stop info for Div/LLR.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/09/19 | rhalpai | Original. PIR19049
  || 09/30/19 | rhalpai | Change logic to include history. SDHD-565226
  ||-----------------------------------------------------------------------------
  */
  FUNCTION cust_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  DATE
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUST_NOTIFY_PK.CUST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   ld.load_num, se.stop_num, se.cust_id, c.namec AS cust_nm, c.cnphnc AS cntct_phone,
                c.shpctc AS shp_city, c.shpstc AS shp_st,
                load_stat_udf(ld.div_part, ld.llr_dt, ld.load_num, se.stop_num) AS stop_stat
           FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, sysp200c c
          WHERE d.div_id = i_div
            AND ld.div_part = d.div_part
            AND ld.llr_dt = i_llr_dt
            AND se.div_part = ld.div_part
            AND se.load_depart_sid = ld.load_depart_sid
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = se.div_part
                          AND a.load_depart_sid = se.load_depart_sid
                          AND a.custa = se.cust_id
                          AND a.stata IN('O', 'P', 'R', 'A'))
            AND c.div_part = se.div_part
            AND c.acnoc = se.cust_id
       UNION ALL
       SELECT   a.orrtea AS load_num, a.stopsa AS stop_num, a.custa AS cust_id, c.namec AS cust_nm,
                c.cnphnc AS cntct_phone, c.shpctc AS shp_city, c.shpstc AS shp_st, 'Shipped' AS stop_stat
           FROM div_mstr_di1d d, ordp900a a, sysp200c c
          WHERE d.div_id = i_div
            AND a.div_part = d.div_part
            AND a.ctofda = (SELECT i_llr_dt - DATE '1900-02-28'
                              FROM DUAL)
            AND a.stata = 'A'
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
       GROUP BY a.orrtea, a.stopsa, a.custa, c.namec, c.cnphnc, c.shpctc, c.shpstc
       ORDER BY load_num, stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_list_fn;

END op_cust_notify_pk;
/

