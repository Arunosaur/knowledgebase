CREATE OR REPLACE PACKAGE csr_suspended_orders_pk IS
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
  FUNCTION retrieve_suspended_ord_list_fn(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END csr_suspended_orders_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_suspended_orders_pk IS
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
  || RETRIEVE_SUSPENDED_ORD_LIST_FN
  ||  Function to return the list of suspended orders in system
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/09/04 | SNAGABH | Original
  || 03/06/06 | SNAGABH | Changes to use new Confirmation Number format (C####
  ||                    | instead of CXX####; where #### is a number and XX is
  ||                    | 2 digit division code).
  || 03/07/06 | SNAGABH | Updated to return Order Well information.
  || 11/10/10 | rhalpai | Replace reference in cursor to Div column in
  ||                    | Exception Well with HDIVA. Convert to use standard
  ||                    | error handling logic. PIR5878
  || 11/28/11 | rhalpai | Add new Test status 4 to cursor. PIR10211
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_suspended_ord_list_fn(
    i_div      IN  VARCHAR2,
    i_user_id  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_SUSPENDED_ORDERS_PK.RETRIEVE_SUSPENDED_ORD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT TO_CHAR(DATE '1900-02-28' + s.datea, 'MM/DD/YY') || ' ' || LPAD(s.timea, 6, '0') AS suspnd_ts, o.custa,
              cx.mccusb, c.namec, DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS cust_stat,
              o.ordnoa, o.dsorda, c.cnphnc, TO_CHAR(ld.llr_ts, 'MM/DD/YY') AS llr_dt, ld.load_num, se.stop_num,
              s.usera, ma.desca, o.mntusa, o.connba, DECODE(o.excptn_sw, 'N', 0, 'Y', 1) AS tbl
         FROM div_mstr_di1d d, ordp100a o, load_depart_op1f ld, stop_eta_op1g se, sysp200c c, mclp020b cx, sysp296a s,
              mclp140a ma
        WHERE d.div_id = i_div
          AND o.div_part = d.div_part
          AND o.stata = 'S'
          AND ld.div_part = d.div_part
          AND ld.load_depart_sid = o.load_depart_sid
          AND se.div_part = d.div_part
          AND se.load_depart_sid = o.load_depart_sid
          AND se.cust_id = o.custa
          AND c.div_part = d.div_part
          AND c.acnoc = o.custa
          AND cx.div_part = d.div_part
          AND cx.custb = o.custa
          AND s.div_part = d.div_part
          AND s.ordnoa = o.ordnoa
          AND (   i_user_id IS NULL
               OR i_user_id = s.usera)
          AND s.fldnma = 'STATA'   -- status field; updated when order is suspended
          -- only select the latest matching entry from sysp296a
          AND (s.datea || LPAD(s.timea, 6, '0')) = (SELECT MAX(s2.datea || LPAD(s2.timea, 6, '0'))
                                                      FROM sysp296a s2
                                                     WHERE s2.div_part = d.div_part
                                                       AND s2.ordnoa = s.ordnoa
                                                       AND s2.fldnma = 'STATA'
                                                       AND s2.rsncda IS NOT NULL)
          AND ma.rsncda(+) = s.rsncda;

    -- substr(o.connba, 2) is used to include orders that were suspended
    -- during order creation in CSR. These orders do not have an order number,
    -- so the app saved the confirmation number without the first character
    -- (C) and CSR in Authorized by field.
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_suspended_ord_list_fn;
END csr_suspended_orders_pk;
/

