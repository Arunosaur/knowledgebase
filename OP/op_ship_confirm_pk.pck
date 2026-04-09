CREATE OR REPLACE PACKAGE op_ship_confirm_pk IS
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
  FUNCTION load_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION stop_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2,
    i_load    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION load_stop_totals_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2,
    i_load    IN  VARCHAR2,
    i_stop    IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE upd_totals_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load       IN  VARCHAR2,
    i_stop       IN  NUMBER,
    i_parm_list  IN  VARCHAR2
  );
END op_ship_confirm_pk;
/

CREATE OR REPLACE PACKAGE BODY op_ship_confirm_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_SHIP_CONFIRM_PK
  ||   All the procedures in this package are called by SHIP CONFIRM screens
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes     (Package Level Changes)
  ||----------------------------------------------------------------------------
  || 02/28/02 | sudheer | Original
  || 03/02/06 | RHALPAI | Added process constant and process restricted exception.
  ||                    | Added cursor LOAD_CUR.
  ||                    | Removed EMPTY_TEMP_TBL_SP, ADD_TO_TEMP_SP,
  ||                    | BACKOUT_TESTBILL_SP.
  ||                    | Added INS_QOPRC08_SP, INS_QOPRC17_SP, INS_QOPRC21_SP,
  ||                    | MOVE_WHS_TRANS_TO_HIST_SP, PROCESS_WHS_TRANS_SP,
  ||                    | UPD_ORDS_TO_SHIP_STAT_SP, all_tbill_loads_closed_fn,
  ||                    | LOAD_LOCK_SP, PROCESS_TBILLS_SP, PROCESS_NON_TBILLS_SP,
  ||                    | IS_PROCESS_COMPLETE_FN.
  || 04/05/06 | RHALPAI | Replaced PROCESS_TBILLS_SP and PROCESS_NON_TBILLS_SP
  ||                    | with PROCESS_TAGGED_LOADS_SP.
  ||                    | Removed IS_PROCESS_COMPLETE_FN.
  ||----------------------------------------------------------------------------
  */

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
  || LOAD_LIST_FN
  ||   Build a cursor of loads that are in "Released" (R) status.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3593
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SHIP_CONFIRM_PK.LOAD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   lc.load_num, (SELECT l.destc
                                FROM mclp120c l
                               WHERE l.div_part = d.div_part
                                 AND l.loadc = lc.load_num), TO_CHAR(lc.llr_dt, 'YYYY-MM-DD')
           FROM div_mstr_di1d d, load_clos_cntrl_bc2c lc
          WHERE d.div_id = i_div
            AND lc.div_part = d.div_part
            AND lc.load_status = 'R'
       ORDER BY 1, 3;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_LIST_FN
  ||   Build a cursor of stops for Div/LLR-Date/Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3593
  || 06/16/08 | RHALPAI | Added sort by Stop to cursor.
  || 08/26/10 | rhalpai | Replaced usage of to_rendate_dt within cursor with
  ||                    | reference to variable. Convert to use standard error
  ||                    | handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2,
    i_load    IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SHIP_CONFIRM_PK.STOP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_llr_num            PLS_INTEGER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'Load', i_load);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_llr_num := l_llr_dt - DATE '1900-02-28';
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   LPAD(mc.stopc, 2, '0')
           FROM div_mstr_di1d d, load_clos_cntrl_bc2c lc, mclp370c mc
          WHERE d.div_id = i_div
            AND lc.div_part = d.div_part
            AND lc.llr_dt = l_llr_dt
            AND lc.load_num = i_load
            AND lc.load_status = 'R'
            AND mc.div_part = lc.div_part
            AND mc.llr_date = l_llr_num
            AND mc.loadc = lc.load_num
       GROUP BY mc.stopc
       ORDER BY mc.stopc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_STOP_TOTALS_FN
  ||   Build a cursor of summary information for Load or Load and Stop.
  ||
  ||   Stop is NULL   = Load Totals for Div/LLR-Date
  ||   Stop is passed = Stop Totals for Div/LLR-Date/Load
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3209
  || 02/19/07 | rhalpai | Changed Load-Level cursor to sort by manifest seq,
  ||                    | tote category description. Changed Stop-Level cursor
  ||                    | to sort by manifest seq, tote category code. IM288889
  || 03/19/07 | rhalpai | Moved bag count in cursors to come back with totes
  ||                    | and boxes instead of with pallets and chep pallets.
  ||                    | IM290595
  || 03/24/08 | rhalpai | Added LOAD_CLOS_CNTRL_BC2C table to cursors. PIR3593
  || 05/06/08 | rhalpai | Added mfst seq to cursor. IM407466
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  || 07/01/19 | rhalpai | Add Peco pallet count. PIR19620
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_stop_totals_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2,
    i_load    IN  VARCHAR2,
    i_stop    IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module   CONSTANT typ.t_maxfqnm                       := 'OP_SHIP_CONFIRM_PK.LOAD_STOP_TOTALS_FN';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_cv                  SYS_REFCURSOR;
    l_llr_num             PLS_INTEGER;
    l_cubing_of_totes_sw  appl_sys_parm_ap1s.vchar_val%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'Load', i_load);
    logs.add_parm(lar_parm, 'Stop', i_stop);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_num := TO_DATE(i_llr_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_cubing_of_totes_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_cubing_of_totes);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   m.descc AS mfst_descr, mc.manctc, mc.totctc,
                (SELECT b.descb
                   FROM mclp200b b
                  WHERE b.div_part = mc.div_part
                    AND b.totctb = mc.totctc) AS tote_descr, NVL(SUM(mc.totsmc), 0) AS tote_cnt,
                NVL(SUM(mc.boxsmc), 0) AS box_cnt, NVL(SUM(mc.palsmc), 0) AS plt_cnt,
                NVL(SUM(mc.cpasmc), 0) AS chep_cnt, NVL(SUM(mc.peco_pallet_cnt), 0) AS peco_cnt,
                NVL(SUM(mc.bagsmc), 0) AS bag_cnt,
                (SELECT COUNT(DISTINCT c2.release_ts)
                   FROM mclp370c c2
                  WHERE c2.div_part = mc.div_part
                    AND c2.llr_date = l_llr_num
                    AND c2.loadc = i_load
                    AND c2.stopc = NVL(i_stop, c2.stopc)
                    AND c2.manctc = mc.manctc
                    AND NVL(c2.totctc, '~') = NVL(mc.totctc, '~')) AS rlse_cnt,
                TO_CHAR(mc.release_ts, 'YYYY-MM-DD HH24:MI:SS') AS release_ts,
                (CASE
                   WHEN(    l_cubing_of_totes_sw = 'Y'
                        AND EXISTS(SELECT 1
                                     FROM mclp370c c2, sysp200c sc, mclp100a ma
                                    WHERE c2.div_part = mc.div_part
                                      AND c2.llr_date = l_llr_num
                                      AND c2.loadc = i_load
                                      AND c2.stopc = NVL(i_stop, c2.stopc)
                                      AND sc.div_part = c2.div_part
                                      AND sc.acnoc = c2.custc
                                      AND ma.div_part = sc.div_part
                                      AND ma.cstgpa = sc.retgpc
                                      AND ma.cntnr_trckg_sw = 'Y')
                       ) THEN 'Y'
                   ELSE 'N'
                 END
                ) AS cntnr_trckng_cust,
                m.seqc
           FROM mclp370c mc, mclp210c m
          WHERE mc.div_part = l_div_part
            AND mc.llr_date = l_llr_num
            AND mc.loadc = i_load
            AND mc.stopc = NVL(i_stop, mc.stopc)
            AND m.div_part(+) = mc.div_part
            AND m.manctc(+) = mc.manctc
       GROUP BY mc.div_part, m.seqc, mc.manctc, m.descc, mc.totctc, mc.release_ts
       ORDER BY m.seqc, mc.manctc, mc.totctc, mc.release_ts;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_stop_totals_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_TOTALS_SP
  ||   Update summary information for Load or Load and Stop.
  ||
  ||  ParmList:
  ||  RlseTs~MfstCatg~ToteCatg~ToteCnt~BoxCnt~PalletCnt~ChepCnt~PicoCnt~BagCnt`RlseTs~MfstCatg~ToteCatg~ToteCnt~BoxCnt~PalletCnt~ChepCnt~PicoCnt~BagCnt
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/31/06 | rhalpai | Original. PIR3593
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  || 07/01/19 | rhalpai | Add Peco pallet count. PIR19620
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_totals_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load       IN  VARCHAR2,
    i_stop       IN  NUMBER,
    i_parm_list  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_SHIP_CONFIRM_PK.UPD_TOTALS_SP';
    lar_parm             logs.tar_parm;
    l_llr_num            NUMBER;
    l_t_grps             type_stab;
    l_idx                PLS_INTEGER;
    l_t_fields           type_stab;
    l_rlse_ts            DATE;
    l_mfst_categ         mclp370c.manctc%TYPE;
    l_tote_categ         mclp370c.totctc%TYPE;
    l_tote_cnt           PLS_INTEGER;
    l_box_cnt            PLS_INTEGER;
    l_pallet_cnt         PLS_INTEGER;
    l_chep_pallet_cnt    PLS_INTEGER;
    l_peco_pallet_cnt    PLS_INTEGER;
    l_bag_cnt            PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'Load', i_load);
    logs.add_parm(lar_parm, 'Stop', i_stop);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_num := TO_DATE(i_llr_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    logs.dbg('Parse Groups of Parm Field Lists');
    l_t_grps := str.parse_list(REPLACE(i_parm_list, ' '), op_const_pk.grp_delimiter);

    IF l_t_grps IS NOT NULL THEN
      l_idx := l_t_grps.FIRST;
      WHILE l_idx IS NOT NULL LOOP
        logs.dbg('Parse Parm Field List');
        l_t_fields := str.parse_list(l_t_grps(l_idx), op_const_pk.field_delimiter, 'N');
        logs.dbg('Set Parm Field Values');
        l_rlse_ts := TO_DATE(l_t_fields(1), 'YYYY-MM-DD HH24:MI:SS');
        l_mfst_categ := l_t_fields(2);
        l_tote_categ := RTRIM(l_t_fields(3));
        l_tote_cnt := l_t_fields(4);
        l_box_cnt := l_t_fields(5);
        l_pallet_cnt := l_t_fields(6);
        l_chep_pallet_cnt := l_t_fields(7);
        l_peco_pallet_cnt := l_t_fields(8);
        l_bag_cnt := l_t_fields(9);
        logs.dbg('Upd Counts');

        UPDATE mclp370c
           SET totsmc = l_tote_cnt,
               boxsmc = l_box_cnt,
               palsmc = l_pallet_cnt,
               cpasmc = l_chep_pallet_cnt,
               peco_pallet_cnt = l_peco_pallet_cnt,
               bagsmc = l_bag_cnt
         WHERE div_part = (SELECT div_part
                             FROM div_mstr_di1d
                            WHERE div_id = i_div)
           AND llr_date = l_llr_num
           AND loadc = i_load
           AND stopc = i_stop
           AND manctc = l_mfst_categ
           AND NVL(totctc, '~') = NVL(l_tote_categ, '~')
           AND release_ts = l_rlse_ts;

        l_idx := l_t_grps.NEXT(l_idx);
      END LOOP;
    END IF;   -- l_t_grps IS NOT NULL

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_totals_sp;
END op_ship_confirm_pk;
/

