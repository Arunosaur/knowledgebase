CREATE OR REPLACE PACKAGE op_wave_plan_pk IS
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
  PROCEDURE upd_grp_cnt_sp(
    i_div          IN  VARCHAR2,
    i_new_grp_cnt  IN  NUMBER
  );

  PROCEDURE load_seq_list_sp(
    i_div           IN      VARCHAR2,
    i_llr_dt        IN      VARCHAR2,
    o_cur_load_seq  OUT     SYS_REFCURSOR,
    o_lane_cnt      OUT     PLS_INTEGER,
    o_grp_cnt       OUT     PLS_INTEGER,
    o_cur_grp_cnt   OUT     SYS_REFCURSOR
  );

  PROCEDURE save_load_seq_sp(
    i_div           IN  VARCHAR2,
    i_llr_dt        IN  VARCHAR2,
    i_add_chg_list  IN  VARCHAR2,
    i_del_list      IN  VARCHAR2
  );
END op_wave_plan_pk;
/

CREATE OR REPLACE PACKAGE BODY op_wave_plan_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_date_fmt  CONSTANT VARCHAR2(10) := 'YYYY-MM-DD';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || GRP_CNT_LIST_FN
  ||  Build cursor of possible Group Counts.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/18/11 | rhalpai | Original for PIR10057
  ||----------------------------------------------------------------------------
  */
  FUNCTION grp_cnt_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.GRP_CNT_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_lane_cnt           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_lane_cnt := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_wp_lane_cnt);

    OPEN l_cv
     FOR
       SELECT   y.grp_cnt
           FROM (SELECT DISTINCT DECODE(x.lvl,
                                        1, 0,
                                        l_lane_cnt, 0,
                                        DECODE(MOD(l_lane_cnt, x.lvl), 0, x.lvl, 0)
                                       ) AS grp_cnt
                            FROM (SELECT     LEVEL AS lvl
                                        FROM DUAL
                                  CONNECT BY LEVEL <= l_lane_cnt) x) y
          WHERE y.grp_cnt > 0
       ORDER BY y.grp_cnt;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END grp_cnt_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_SEQ_LIST_FN
  ||  Build cursor of Loads for Orders on a given LLR date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/13/09 | rhalpai | Original for PIR7118
  || 08/26/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_seq_list_fn(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.LOAD_SEQ_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, g_c_date_fmt);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(NEXT_DAY(TO_DATE(i_llr_dt || LPAD(c.deptmc, 4, '0'), g_c_date_fmt || 'HH24MI')
                                 +(7 * c.depwkc)
                                 - 1,
                                 c.depdac
                                ),
                        'YYYY-MM-DD HH24:MI'
                       ) AS dep_ts,
                c.loadc AS load_num, c.destc AS dest, COUNT(DISTINCT o.stop_num) AS cur_stop_cnt, wpl.seq,
                wpl.stop_cnt
           FROM (SELECT ld.load_num, se.stop_num
                   FROM load_depart_op1f ld, stop_eta_op1g se
                  WHERE ld.div_part = l_div_part
                    AND ld.llr_dt = l_llr_dt
                    AND se.div_part = ld.div_part
                    AND se.load_depart_sid = ld.load_depart_sid
                    AND EXISTS(SELECT 1
                                 FROM ordp100a a
                                WHERE a.div_part = se.div_part
                                  AND a.load_depart_sid = se.load_depart_sid
                                  AND a.custa = se.cust_id
                                  AND a.stata = 'O')) o,
                mclp120c c, wave_plan_load_op2w wpl
          WHERE c.div_part = l_div_part
            AND c.loadc = o.load_num
            AND c.loadc NOT IN(SELECT t.column_value
                                 FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
            AND wpl.div_part(+) = l_div_part
            AND wpl.llr_dt(+) = l_llr_dt
            AND wpl.load_num(+) = o.load_num
       GROUP BY wpl.load_num, c.loadc, c.depdac, c.deptmc, c.depwkc, c.destc, wpl.seq, wpl.stop_cnt
       UNION ALL
       SELECT   TO_CHAR(NEXT_DAY(TO_DATE(i_llr_dt || LPAD(c.deptmc, 4, '0'), g_c_date_fmt || 'HH24MI')
                                 +(7 * c.depwkc)
                                 - 1,
                                 c.depdac
                                ),
                        'YYYY-MM-DD HH24:MI'
                       ) AS dep_ts,
                c.loadc AS load_num, c.destc AS dest, 0 AS cur_stop_cnt, wpl.seq, wpl.stop_cnt
           FROM wave_plan_load_op2w wpl, mclp120c c
          WHERE wpl.div_part = l_div_part
            AND wpl.llr_dt = l_llr_dt
            AND NOT EXISTS(SELECT 1
                             FROM load_depart_op1f ld, ordp100a a
                            WHERE ld.div_part = wpl.div_part
                              AND ld.llr_dt = wpl.llr_dt
                              AND ld.load_num = wpl.load_num
                              AND a.div_part = ld.div_part
                              AND a.load_depart_sid = ld.load_depart_sid
                              AND a.stata = 'O')
            AND c.div_part = wpl.div_part
            AND c.loadc = wpl.load_num
       ORDER BY seq NULLS FIRST, dep_ts, load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_seq_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_PARMS_SP
  ||  Get WavePlan parameters for Wave Lane Count and Lane Group Count.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/14/09 | rhalpai | Original for PIR7118
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_parms_sp(
    i_div       IN      VARCHAR2,
    o_lane_cnt  OUT     PLS_INTEGER,
    o_grp_cnt   OUT     PLS_INTEGER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm             := 'OP_WAVE_PLAN_PK.GET_PARMS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_parms            op_types_pk.tt_varchars_v;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part, op_const_pk.prm_wp_lane_cnt || ',' || op_const_pk.prm_wp_grp_cnt);
    o_lane_cnt := l_t_parms(op_const_pk.prm_wp_lane_cnt);
    o_grp_cnt := l_t_parms(op_const_pk.prm_wp_grp_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_parms_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_CHG_LOAD_SEQ_SP
  ||  Add/Change Load Sequence Info.
  ||  ParmList Format:
  ||    Load~StopCnt~Seq,Load~StopCnt~Seq,Load~StopCnt~Seq
  ||    0410~12~1,0415~5~2,0440~9~3
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/13/09 | rhalpai | Original for PIR7118
  || 05/14/09 | rhalpai | Renamed from save_load_seq_sp and added logic to
  ||                    | handle StopCnt in delimited ParmList. PIR7118
  || 08/03/10 | rhalpai | Added logic to first remove matching loads for the
  ||                    | LLRDate before processing add/chg logic. IM604491
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_chg_load_seq_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.ADD_CHG_LOAD_SEQ_SP';
    lar_parm                    logs.tar_parm;
    l_llr_dt                    DATE;
    l_c_grp_delimiter  CONSTANT VARCHAR2(1)   := ',';
    l_t_load_seqs               type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, g_c_date_fmt);
    logs.dbg('Parse');
    l_t_load_seqs := str.parse_list(i_parm_list, l_c_grp_delimiter);
    logs.dbg('Remove any existing entries');

    DELETE FROM wave_plan_load_op2w w
          WHERE w.div_part = (SELECT d.div_part
                                FROM div_mstr_di1d d
                               WHERE d.div_id = i_div)
            AND w.llr_dt = l_llr_dt
            AND w.load_num IN(SELECT SUBSTR(t.column_value,
                                            1,
                                            INSTR(t.column_value, op_const_pk.field_delimiter) - 1
                                           ) AS load_num
                                FROM TABLE(CAST(l_t_load_seqs AS type_stab)) t);

    logs.dbg('Add/Chg');
    MERGE INTO wave_plan_load_op2w w
         USING (SELECT d.div_part,
                       SUBSTR(t.column_value, 1, INSTR(t.column_value, op_const_pk.field_delimiter) - 1) AS load_num,
                       TO_NUMBER(SUBSTR(t.column_value,
                                        INSTR(t.column_value, op_const_pk.field_delimiter, 1, 1) + 1,
                                        INSTR(t.column_value, op_const_pk.field_delimiter, 1, 2)
                                        - INSTR(t.column_value, op_const_pk.field_delimiter, 1, 1)
                                        - 1
                                       )
                                ) AS stop_cnt,
                       TO_NUMBER(SUBSTR(t.column_value, INSTR(t.column_value, op_const_pk.field_delimiter, 1, 2) + 1)
                                ) AS seq
                  FROM div_mstr_di1d d, TABLE(CAST(l_t_load_seqs AS type_stab)) t
                 WHERE d.div_id = i_div) x
            ON (    w.div_part = x.div_part
                AND w.llr_dt = l_llr_dt
                AND w.load_num = x.load_num)
      WHEN MATCHED THEN
        UPDATE
           SET w.seq = x.seq, w.stop_cnt = x.stop_cnt
      WHEN NOT MATCHED THEN
        INSERT(div_part, llr_dt, load_num, seq, stop_cnt)
        VALUES(x.div_part, l_llr_dt, x.load_num, x.seq, x.stop_cnt);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_chg_load_seq_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_LOAD_SEQ_SP
  ||  Remove Load Sequence Info.
  ||  ParmList Format:
  ||    Load,Load,Load
  ||    0410,0415,0440
  ||    Load 0410, Load 0415, Load 0440
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/13/09 | rhalpai | Original for PIR7118
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_load_seq_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.DEL_LOAD_SEQ_SP';
    lar_parm                logs.tar_parm;
    l_llr_dt                DATE;
    l_c_delimiter  CONSTANT VARCHAR2(1)   := ',';
    l_t_loads               type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, g_c_date_fmt);
    logs.dbg('Parse');
    l_t_loads := str.parse_list(i_parm_list, l_c_delimiter);

    IF l_t_loads.COUNT > 0 THEN
      logs.dbg('Remove');
      FORALL i IN l_t_loads.FIRST .. l_t_loads.LAST
        DELETE FROM wave_plan_load_op2w w
              WHERE w.div_part = (SELECT d.div_part
                                    FROM div_mstr_di1d d
                                   WHERE d.div_id = i_div)
                AND w.llr_dt = l_llr_dt
                AND w.load_num = l_t_loads(i);
      COMMIT;
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_load_seq_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || UPD_GRP_CNT_SP
  ||  Set Group Count parm for Wave Plan.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/18/11 | rhalpai | Original for PIR10057
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_grp_cnt_sp(
    i_div          IN  VARCHAR2,
    i_new_grp_cnt  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.UPD_GRP_CNT_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'NewGrpCnt', i_new_grp_cnt);
    logs.info('ENTRY', lar_parm);
    op_parms_pk.merge_sp(div_pk.div_part_fn(i_div),
                         op_const_pk.prm_wp_grp_cnt,
                         op_parms_pk.g_c_int,
                         i_new_grp_cnt,
                         USER
                        );
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_grp_cnt_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_SEQ_LIST_SP
  ||  Build cursor of Loads for Orders on a given LLR date and get WavePlan
  ||  parameters for Wave Lane Count and Lane Group Count and build cursor of
  ||  possible Group Counts.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/14/09 | rhalpai | Original for PIR7118
  || 04/18/11 | rhalpai | Added cursor for possible Group Counts. PIR10057
  ||----------------------------------------------------------------------------
  */
  PROCEDURE load_seq_list_sp(
    i_div           IN      VARCHAR2,
    i_llr_dt        IN      VARCHAR2,
    o_cur_load_seq  OUT     SYS_REFCURSOR,
    o_lane_cnt      OUT     PLS_INTEGER,
    o_grp_cnt       OUT     PLS_INTEGER,
    o_cur_grp_cnt   OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.LOAD_SEQ_LIST_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get LoadSeq Cursor');
    o_cur_load_seq := load_seq_list_fn(i_div, i_llr_dt);
    logs.dbg('Get Parms');
    get_parms_sp(i_div, o_lane_cnt, o_grp_cnt);
    logs.dbg('Get Group Count List');
    o_cur_grp_cnt := grp_cnt_list_fn(i_div);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_seq_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_LOAD_SEQ_SP
  ||  Save Load Sequence Info.
  ||  AddChgList Format:
  ||    Load~StopCnt~Seq,Load~StopCnt~Seq,Load~StopCnt~Seq
  ||    0410~12~1,0415~5~2,0440~9~3
  ||  DelList Format:
  ||    Load,Load,Load
  ||    0410,0415,0440
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/14/09 | rhalpai | Original for PIR7118
  || 04/18/11 | rhalpai | Removed call to load_seq_list_sp. PIR10057
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_load_seq_sp(
    i_div           IN  VARCHAR2,
    i_llr_dt        IN  VARCHAR2,
    i_add_chg_list  IN  VARCHAR2,
    i_del_list      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_WAVE_PLAN_PK.SAVE_LOAD_SEQ_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'AddChgList', i_add_chg_list);
    logs.add_parm(lar_parm, 'DelList', i_del_list);
    logs.dbg('ENTRY', lar_parm);

    IF i_del_list IS NOT NULL THEN
      logs.dbg('Del Load Seq');
      del_load_seq_sp(i_div, i_llr_dt, i_del_list);
    END IF;   -- i_del_list

    IF i_add_chg_list IS NOT NULL THEN
      logs.dbg('Add/Chg Load Seq');
      add_chg_load_seq_sp(i_div, i_llr_dt, i_add_chg_list);
    END IF;   -- i_add_chg_list

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END save_load_seq_sp;
END op_wave_plan_pk;
/

