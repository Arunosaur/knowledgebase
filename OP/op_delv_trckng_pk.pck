CREATE OR REPLACE PACKAGE op_delv_trckng_pk IS
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
  PROCEDURE cust_load_stat_sp(
    i_fin_div_cd  IN      VARCHAR2,
    i_cust_id     IN      VARCHAR2,
    o_cur         OUT     SYS_REFCURSOR
  );

END op_delv_trckng_pk;
/

CREATE OR REPLACE PACKAGE BODY op_delv_trckng_pk IS
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
  || CUST_LOAD_STAT_SP
  ||  Return cursor of load status info for a cust.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 12/21/20 | rhalpai | Original. PIR20597
  || 04/30/20 | rhalpai | Add ETA to cursor and remove orders in open status. PIR20597
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE cust_load_stat_sp(
    i_fin_div_cd  IN      VARCHAR2,
    i_cust_id     IN      VARCHAR2,
    o_cur         OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DELV_TRCKNG_PK.CUST_LOAD_STAT_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'FinDivCd', i_fin_div_cd);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor');

    OPEN o_cur
     FOR
       SELECT   ld.llr_dt, ld.load_num, load_stat_udf(ld.div_part, ld.llr_dt, ld.load_num, se.stop_num) AS load_stat,
                se.eta_ts
           FROM div_mstr_di1d d, stop_eta_op1g se, load_depart_op1f ld
          WHERE d.fin_div_cd = i_fin_div_cd
            AND se.div_part = d.div_part
            AND se.cust_id = i_cust_id
            AND ld.div_part = se.div_part
            AND ld.load_depart_sid = se.load_depart_sid
            AND ld.llr_dt > DATE '1900-01-01'
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = se.div_part
                          AND a.load_depart_sid = se.load_depart_sid
                          AND a.custa = se.cust_id
                          AND a.dsorda = 'R'
                          AND a.stata IN('P', 'R', 'A'))
       ORDER BY se.eta_ts DESC, ld.load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_load_stat_sp;
BEGIN
  env.set_app_cd('OPCIG');
END op_delv_trckng_pk;
/

