CREATE OR REPLACE PACKAGE op_load_status_pk IS
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
  FUNCTION load_status_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  DATE,
    i_load_num  IN  VARCHAR2,
    i_stop_num  IN  NUMBER DEFAULT NULL
  )
    RETURN VARCHAR2;

  FUNCTION llr_dates_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION summary_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;
--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END op_load_status_pk;
/

CREATE OR REPLACE PACKAGE BODY op_load_status_pk IS
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
  || LOAD_STATUS_FN
  ||   Return status of load for LLR date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/12/06 | rhalpai | Original. PIR3593
  || 08/11/08 | rhalpai | Changed Partial portion of cursor to use header
  ||                    | status P. PIR6364
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_status_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  DATE,
    i_load_num  IN  VARCHAR2,
    i_stop_num  IN  NUMBER DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_STATUS_PK.LOAD_STATUS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_status             VARCHAR2(30);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    SELECT load_stat_udf(l_div_part, i_llr_dt, i_load_num, i_stop_num)
      INTO l_status
      FROM DUAL;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_status);
  END load_status_fn;

  /*
  ||----------------------------------------------------------------------------
  || LLR_DATES_FN
  ||   Build a cursor of LLR Dates for orders on valid loads excluding
  ||   DFLT,DIST,LOST,COPY,ROUT,TEST.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/12/06 | rhalpai | Original. PIR3593
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION llr_dates_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_LOAD_STATUS_PK.LLR_DATES_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt
           FROM load_depart_op1f ld
          WHERE ld.div_part = l_div_part
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND EXISTS(SELECT 1
                                       FROM ordp120b b
                                      WHERE b.div_part = a.div_part
                                        AND b.ordnob = a.ordnoa
                                        AND b.statb NOT IN('I', 'S', 'C')))
       GROUP BY ld.llr_dt
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END llr_dates_fn;

  /*
  ||----------------------------------------------------------------------------
  || SUMMARY_FN
  ||   Build a cursor of load status info for LLR date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/12/06 | rhalpai | Original. PIR3593
  || 08/03/09 | rhalpai | Add PICK_COMPL_SW column to cursor. PIR7342
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 11/19/10 | rhalpai | Remove Care Package logic. PIR5152
  || 09/10/12 | rhalpai | Add CWT_COMPL_SW column to cursor. PIR10251
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION summary_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module    CONSTANT typ.t_maxfqnm                       := 'OP_LOAD_STATUS_PK.SUMMARY_FN';
    lar_parm               logs.tar_parm;
    l_cv                   SYS_REFCURSOR;
    l_div_part             NUMBER;
    l_llr_dt               DATE;
    l_t_xloads             type_stab;
    l_t_parms              op_types_pk.tt_varchars_v;
    l_acs_load_clos_sw     appl_sys_parm_ap1s.vchar_val%TYPE;
    l_xdock_pick_compl_sw  appl_sys_parm_ap1s.vchar_val%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Get Parms');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                         op_const_pk.prm_acs_load_close || ',' || op_const_pk.prm_xdock_pick_compl
                                        );
    l_acs_load_clos_sw := l_t_parms(op_const_pk.prm_acs_load_close);
    l_xdock_pick_compl_sw := NVL(l_t_parms(op_const_pk.prm_xdock_pick_compl), 'N');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   c.loadc, c.destc, (SELECT load_stat_udf(l_div_part, l_llr_dt, c.loadc)
                                     FROM DUAL) AS status,
                DECODE(l_acs_load_clos_sw, 'N', 'Y', l.acs_load_clos_sw, 'Y', 'N') AS acs_compl, NULL AS cpo_compl,
                DECODE(l_xdock_pick_compl_sw, 'N', 'Y', l.pick_compl_sw, 'Y', 'N') AS pick_compl,
                COALESCE(l.cwt_compl_sw,
                         (SELECT 'Y'
                            FROM DUAL
                           WHERE EXISTS(SELECT 1
                                          FROM load_depart_op1f ld, ordp100a a, ordp120b b, mclp110b di
                                         WHERE ld.div_part = c.div_part
                                           AND ld.llr_dt = l_llr_dt
                                           AND ld.load_num = c.loadc
                                           AND a.div_part = ld.div_part
                                           AND a.load_depart_sid = ld.load_depart_sid
                                           AND b.div_part = a.div_part
                                           AND b.ordnob = a.ordnoa
                                           AND b.excptn_sw = 'N'
                                           AND b.statb = 'O'
                                           AND di.div_part = b.div_part
                                           AND di.itemb = b.itemnb
                                           AND di.uomb = b.sllumb
                                           AND di.cwt_sw = 'Y')),
                         'N'
                        ) AS cwt_compl_sw
           FROM mclp120c c, (SELECT lc.load_num, lc.acs_load_clos_sw, lc.pick_compl_sw, lc.cwt_compl_sw
                               FROM load_clos_cntrl_bc2c lc
                              WHERE lc.div_part = l_div_part
                                AND lc.llr_dt = l_llr_dt) l
          WHERE c.div_part = l_div_part
            AND c.loadc NOT IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
            AND l.load_num(+) = c.loadc
            AND EXISTS(SELECT 1
                         FROM load_depart_op1f ld, ordp100a a, ordp120b b
                        WHERE ld.div_part = c.div_part
                          AND ld.llr_dt = l_llr_dt
                          AND ld.load_num = c.loadc
                          AND a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND b.div_part = a.div_part
                          AND b.ordnob = a.ordnoa
                          AND b.statb NOT IN('I', 'S', 'C'))
       ORDER BY c.loadc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END summary_fn;
END op_load_status_pk;
/

