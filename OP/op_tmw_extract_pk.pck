CREATE OR REPLACE PACKAGE op_tmw_extract_pk IS
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
  FUNCTION extract_cur_fn(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE extract_sp(
    i_div      IN  VARCHAR2,
    i_file_nm  IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE,
    i_user_id  IN  VARCHAR2 DEFAULT 'MQ'
  );

  PROCEDURE import_sp(
    i_div  IN  VARCHAR2
  );
END op_tmw_extract_pk;
/

CREATE OR REPLACE PACKAGE BODY op_tmw_extract_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || UPD_STAT_SP
  ||  Update status of CustRteReq.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 12/08/18 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE upd_stat_sp(
    i_div_part     IN  NUMBER,
    i_stat_cd      IN  VARCHAR2,
    i_new_stat_cd  IN  VARCHAR2
  ) IS
    l_c_sysdate  CONSTANT DATE := SYSDATE;
  BEGIN
    UPDATE cust_rte_req_op4c r
       SET r.stat_cd = i_new_stat_cd,
           r.last_chg_ts = l_c_sysdate
     WHERE r.div_part = i_div_part
       AND r.stat_cd = i_stat_cd;
  END upd_stat_sp;

  /*
  ||-----------------------------------------------------------------------------
  || VALIDATE_SP
  ||  Validate CustRteReq.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 12/08/18 | rhalpai | Original. PIR18901
  || 11/01/21 | rhalpai | Remove validation for Order found in billed status for Cust on Old LLRDt. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE validate_sp(
    i_div_part  IN  NUMBER,
    i_stat_cd   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_EXTRACT_PK.VALIDATE_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'StatCd', i_stat_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Check for Matching Override');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'CAN',
           crr.err_msg = 'Matching Override Exists!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND crr.ord_num_list IS NULL
       AND EXISTS(SELECT 1
                    FROM cust_rte_ovrrd_rt3c cro
                   WHERE cro.div_part = crr.div_part
                     AND cro.cust_id = crr.cust_id
                     AND cro.llr_dt = crr.new_llr_dt
                     AND cro.load_num = crr.new_load_num
                     AND cro.depart_ts = crr.new_depart_ts
                     AND cro.stop_num = crr.new_stop_num
                     AND cro.eta_ts = crr.new_eta_ts);

/*    logs.dbg('Check Order found in billed status for Cust on Old LLRDt');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'CAN',
           crr.err_msg = 'Order found in billed status for Cust on Old LLRDt!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND crr.ord_num_list IS NULL
       AND EXISTS(SELECT 1
                    FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                   WHERE ld.div_part = crr.div_part
                     AND ld.llr_dt = crr.llr_dt
                     AND se.div_part = ld.div_part
                     AND se.load_depart_sid = ld.load_depart_sid
                     AND se.cust_id = crr.cust_id
                     AND a.div_part = se.div_part
                     AND a.load_depart_sid = se.load_depart_sid
                     AND a.custa = se.cust_id
                     AND a.stata IN('P', 'R', 'A'));*/

    logs.dbg('Check No open Order found for Cust on Old LLRDt/Load');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'CAN',
           crr.err_msg = 'No open Order found for Cust on Old LLRDt/Load!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND NOT EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = crr.div_part
                         AND ld.llr_dt = crr.llr_dt
                         AND ld.load_num = crr.load_num
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.cust_id = crr.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata = 'O'
                         AND (   crr.ord_num_list IS NOT NULL
                              OR NOT EXISTS(SELECT 1
                                              FROM TABLE(lob2table.separatedcolumns(crr.ord_num_list, '~')) t
                                             WHERE TO_NUMBER(t.column1) = a.ordnoa)
                             ));

    logs.dbg('Check for Change Matches Original');

/*    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'Change Matches Original!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND crr.new_llr_dt = crr.llr_dt
       AND crr.new_load_num = crr.load_num
       AND (   (    crr.ord_num_list IS NOT NULL   -- no overrides will be created
                AND (   EXISTS(SELECT 1
                                 FROM cust_rte_ovrrd_rt3c cro
                                WHERE cro.div_part = crr.div_part
                                  AND cro.cust_id = crr.cust_id
                                  AND cro.llr_dt = crr.llr_dt
                                  AND cro.load_num = crr.load_num)
                     OR EXISTS(SELECT 1
                                 FROM mclp040d md
                                WHERE md.div_part = crr.div_part
                                  AND md.custd = crr.cust_id
                                  AND md.loadd = crr.load_num)
                    )
               )
            OR EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se
                       WHERE ld.div_part = crr.div_part
                         AND ld.llr_dt = crr.new_llr_dt
                         AND ld.load_num = crr.new_load_num
                         AND ld.depart_ts = crr.new_depart_ts
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.cust_id = crr.cust_id
                         AND se.stop_num = crr.new_stop_num
                         AND se.eta_ts = crr.new_eta_ts
                         AND EXISTS(SELECT 1
                                      FROM ordp100a a
                                     WHERE a.div_part = se.div_part
                                       AND a.load_depart_sid = se.load_depart_sid
                                       AND a.custa = se.cust_id))
           );*/
    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'Change Matches Original!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND crr.new_llr_dt = crr.llr_dt
       AND crr.new_load_num = crr.load_num
       AND (   (    crr.ord_num_list IS NOT NULL   -- no overrides will be created
                AND (   EXISTS(SELECT 1
                                 FROM cust_rte_ovrrd_rt3c cro
                                WHERE cro.div_part = crr.div_part
                                  AND cro.cust_id = crr.cust_id
                                  AND cro.llr_dt = crr.new_llr_dt
                                  AND cro.load_num = crr.new_load_num
                                  AND cro.depart_ts = crr.new_depart_ts
                                  AND cro.stop_num = crr.new_stop_num
                                  AND cro.eta_ts = crr.new_eta_ts)
                     OR EXISTS(SELECT 1
                                 FROM mclp040d md, mclp120c mc
                                WHERE md.div_part = crr.div_part
                                  AND md.custd = crr.cust_id
                                  AND md.loadd = crr.new_load_num
                                  AND md.stopd = crr.new_stop_num
                                  AND md.dayrcd = TO_CHAR(crr.new_eta_ts, 'DY')
                                  AND md.etad = TO_NUMBER(TO_CHAR(crr.new_eta_ts, 'HH24MI'))
                                  AND mc.div_part = md.div_part
                                  AND mc.loadc = md.loadd
                                  AND mc.depdac = TO_CHAR(crr.new_depart_ts, 'DY')
                                  AND mc.deptmc = TO_NUMBER(TO_CHAR(crr.new_depart_ts, 'HH24MI')))
                    )
               )
            OR EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se
                       WHERE ld.div_part = crr.div_part
                         AND ld.llr_dt = crr.new_llr_dt
                         AND ld.load_num = crr.new_load_num
                         AND ld.depart_ts = crr.new_depart_ts
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.cust_id = crr.cust_id
                         AND se.stop_num = crr.new_stop_num
                         AND se.eta_ts = crr.new_eta_ts
                         AND EXISTS(SELECT 1
                                      FROM ordp100a a
                                     WHERE a.div_part = se.div_part
                                       AND a.load_depart_sid = se.load_depart_sid
                                       AND a.custa = se.cust_id))
           );

    logs.dbg('Check New Load does not exist');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'New Load does not exist!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND NOT EXISTS(SELECT 1
                        FROM mclp120c ld
                       WHERE ld.div_part = crr.div_part
                         AND ld.loadc = crr.new_load_num);

    logs.dbg('Check Another Cust Order found on New Load/Stop');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'Another Cust Order found on New Load/Stop!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND (   EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = crr.div_part
                         AND ld.llr_dt = crr.new_llr_dt
                         AND ld.load_num = crr.new_load_num
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = crr.new_stop_num
                         AND se.cust_id <> crr.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata IN('P', 'R', 'A'))
            OR EXISTS(SELECT 1
                        FROM cust_rte_req_op4c crr2
                       WHERE crr2.div_part = crr.div_part
                         AND crr2.stat_cd = i_stat_cd
                         AND crr2.new_load_num = crr.new_load_num
                         AND crr2.new_stop_num = crr.new_stop_num
                         AND crr2.cust_id <> crr.cust_id)
            OR EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = i_div_part
                         AND ld.llr_dt = crr.new_llr_dt
                         AND ld.load_num = crr.new_load_num
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = crr.new_stop_num
                         AND se.cust_id <> crr.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata = 'O'
                         AND NOT EXISTS(SELECT 1
                                          FROM cust_rte_req_op4c crr2
                                         WHERE crr2.div_part = se.div_part
                                           AND crr2.stat_cd = i_stat_cd
                                           AND crr2.cust_id = se.cust_id
                                           AND (   crr2.new_load_num <> ld.load_num
                                                OR crr2.new_stop_num <> se.stop_num)))
           );

    logs.dbg('Check New ETA Before New Departure');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'New ETA is before New Departure!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND crr.new_depart_ts > crr.new_eta_ts;

    logs.dbg('Check New Departure Before LLR');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'New Departure Before LLR!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND crr.new_llr_dt > crr.new_depart_ts;

    logs.dbg('Check Multiple New Departures for New Load');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'Multiple New Departures for New Load!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM cust_rte_req_op4c crr2
                   WHERE crr2.div_part = crr.div_part
                     AND crr2.new_llr_dt = crr.new_llr_dt
                     AND crr2.new_load_num = crr.new_load_num
                     AND crr2.stat_cd = i_stat_cd
                     AND crr2.new_depart_ts <> crr.new_depart_ts);

    logs.dbg('Check New Departure Greater than 14 days from LLRDate');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'New Departure Greater than 14 days from LLRDate!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND TRUNC(crr.new_depart_ts) > crr.new_llr_dt + 14;

    logs.dbg('Check New ETA Greater than 21 days from LLRDate');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'New ETA Greater than 21 days from LLRDate!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND TRUNC(crr.new_eta_ts) > crr.new_llr_dt + 21;

    logs.dbg('Check ETA Out of Sequence');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'ETA Out of Sequence!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM cust_rte_req_op4c crr2
                   WHERE crr2.div_part = crr.div_part
                     AND crr2.new_llr_dt = crr.new_llr_dt
                     AND crr2.new_load_num = crr.new_load_num
                     AND crr2.stat_cd = i_stat_cd
                     AND (   (    crr2.new_stop_num < crr.new_stop_num
                              AND crr2.new_eta_ts < crr.new_eta_ts)
                          OR (    crr2.new_stop_num > crr.new_stop_num
                              AND crr2.new_eta_ts > crr.new_eta_ts)
                         ));

    logs.dbg('Check Multiple records found for Cust/NewLLRDt/NewLoad');

    UPDATE cust_rte_req_op4c crr
       SET crr.stat_cd = 'ERR',
           crr.err_msg = 'Multiple records found for Cust/NewLLRDt/NewLoad!'
     WHERE crr.div_part = i_div_part
       AND crr.stat_cd = i_stat_cd
       AND (crr.cust_id, crr.new_llr_dt, crr.new_load_num) IN(
                                      SELECT   r.cust_id, r.new_llr_dt, r.new_load_num
                                          FROM cust_rte_req_op4c r
                                         WHERE r.div_part = i_div_part
                                           AND r.stat_cd = i_stat_cd
                                           AND r.ord_num_list IS NULL
                                      GROUP BY r.cust_id, r.new_llr_dt, r.new_load_num
                                        HAVING COUNT(*) > 1);

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_sp;

  /*
  ||-----------------------------------------------------------------------------
  || MOVE_ORDS_SP
  ||  Move Orders for Div per CustRteReq.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 12/08/18 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE move_ords_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_TMW_EXTRACT_PK.MOVE_ORDS_SP';
    lar_parm             logs.tar_parm;

    TYPE l_rt_load_ord IS RECORD(
      cust_id       sysp200c.acnoc%TYPE,
      llr_ts        DATE,
      load_num      mclp120c.loadc%TYPE,
      depart_ts     DATE,
      stop_num      NUMBER,
      eta_ts        DATE,
      t_ord_nums    type_ntab,
      new_llr_dt    DATE,
      new_load_num  mclp120c.loadc%TYPE,
      new_stop_num  NUMBER,
      new_eta_ts    DATE
    );

    TYPE l_tt_load_ords IS TABLE OF l_rt_load_ord;

    l_t_load_ords        l_tt_load_ords;
    l_llr_ts_save        DATE                  := DATE '0001-01-01';
    l_load_num_save      mclp120c.loadc%TYPE   := '~';
    l_load_depart_sid    NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Table of Order Load Info for Moves');

    SELECT   se.cust_id,
             ld.llr_ts,
             ld.load_num,
             ld.depart_ts,
             se.stop_num,
             se.eta_ts,
             CAST
               (MULTISET(SELECT a.ordnoa
                           FROM ordp100a a
                          WHERE a.div_part = se.div_part
                            AND a.load_depart_sid = se.load_depart_sid
                            AND a.custa = se.cust_id
                            AND (   crr.ord_num_list IS NULL
                                 OR a.ordnoa IN(SELECT TO_NUMBER(t.column1)
                                                  FROM TABLE(lob2table.separatedcolumns(crr.ord_num_list,
                                                                                        op_const_pk.field_delimiter
                                                                                       )
                                                            ) t)
                                )
                            AND a.stata IN('O', 'S')
                        ) AS type_ntab
               ) AS ord_nums,
             crr.new_llr_dt,
             crr.new_load_num,
             crr.new_stop_num,
             crr.new_eta_ts
    BULK COLLECT INTO l_t_load_ords
        FROM cust_rte_req_op4c crr, load_depart_op1f ld, stop_eta_op1g se
       WHERE crr.div_part = i_div_part
         AND crr.stat_cd = 'WRK'
         AND ld.div_part = crr.div_part
         AND ld.llr_dt = crr.llr_dt
         AND ld.load_num = crr.load_num
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = crr.cust_id
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = se.div_part
                       AND a.load_depart_sid = se.load_depart_sid
                       AND a.custa = se.cust_id
                       AND (   crr.ord_num_list IS NULL
                            OR a.ordnoa IN(SELECT TO_NUMBER(t.column1)
                                             FROM TABLE(lob2table.separatedcolumns(crr.ord_num_list,
                                                                                   op_const_pk.field_delimiter
                                                                                  )
                                                       ) t)
                           )
                       AND a.stata IN('O', 'S'))
    ORDER BY (CASE
                WHEN crr.ord_num_list IS NULL THEN 2
                ELSE 1
              END), crr.new_llr_dt, crr.new_load_num, se.load_depart_sid, se.cust_id;

    IF l_t_load_ords.COUNT > 0 THEN
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        IF NOT(    l_t_load_ords(i).new_llr_dt = l_llr_ts_save
               AND l_t_load_ords(i).new_load_num = l_load_num_save) THEN
          l_llr_ts_save := l_t_load_ords(i).new_llr_dt;
          l_load_num_save := l_t_load_ords(i).new_load_num;
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part,
                                                                   l_t_load_ords(i).new_llr_dt,
                                                                   l_t_load_ords(i).new_load_num
                                                                  );
        END IF;   -- NOT (l_t_load_ords(i).new_llr_dt = l_llr_ts_save AND l_t_load_ords(i).new_load_num = l_load_num_save)

        logs.dbg('Move Orders');
        op_order_load_pk.move_ords_sp(i_div_part,
                                      l_t_load_ords(i).cust_id,
                                      l_load_depart_sid,
                                      l_t_load_ords(i).new_stop_num,
                                      l_t_load_ords(i).new_eta_ts,
                                      l_t_load_ords(i).llr_ts,
                                      l_t_load_ords(i).load_num,
                                      l_t_load_ords(i).depart_ts,
                                      l_t_load_ords(i).stop_num,
                                      l_t_load_ords(i).eta_ts,
                                      'TMW_RTE',
                                      'TMW',
                                      l_t_load_ords(i).t_ord_nums
                                     );
      END LOOP;
    END IF;   -- l_t_load_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_ords_sp;

  /*
  ||-----------------------------------------------------------------------------
  || MOVE_SP
  ||  Validate and appy Moves for CustRteReq recs for Div.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 12/08/18 | rhalpai | Original. PIR18901
  || 01/21/21 | rhalpai | Add commit to allow changes to cust_rte_ovrrd_rt3c to be
  ||                    | seen by autonomous_transaction function used in move_ords_sp. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE move_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_EXTRACT_PK.MOVE_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Set Work Status');
    upd_stat_sp(i_div_part, 'OPN', 'WRK');
    logs.dbg('Validate');
    validate_sp(i_div_part, 'WRK');
    logs.dbg('Apply Overrides');
    MERGE INTO cust_rte_ovrrd_rt3c cro
         USING (SELECT r.div_part, r.cust_id, r.new_llr_dt, r.new_load_num, r.new_stop_num, r.new_depart_ts,
                       r.new_eta_ts
                  FROM cust_rte_req_op4c r
                 WHERE r.div_part = i_div_part
                   AND r.ord_num_list IS NULL
                   AND r.stat_cd = 'WRK') x
            ON (    cro.div_part = x.div_part
                AND cro.cust_id = x.cust_id
                AND cro.llr_dt = x.new_llr_dt
                AND cro.load_num = x.new_load_num)
      WHEN MATCHED THEN
        UPDATE
           SET cro.depart_ts = x.new_depart_ts, cro.stop_num = x.new_stop_num, cro.eta_ts = x.new_eta_ts,
               cro.depart_ovrrd_sw = 'Y', cro.stop_ovrrd_sw = 'Y', cro.eta_ovrrd_sw = 'Y'
      WHEN NOT MATCHED THEN
        INSERT(div_part, cust_id, llr_dt, load_num, depart_ts, stop_num, eta_ts, depart_ovrrd_sw, stop_ovrrd_sw,
               eta_ovrrd_sw)
        VALUES(x.div_part, x.cust_id, x.new_llr_dt, x.new_load_num, x.new_depart_ts, x.new_stop_num, x.new_eta_ts, 'Y',
               'Y', 'Y');
    COMMIT;   -- needed for move orders logic which calls an autonomous_transaction function which uses cust_rte_ovrrd_rt3c.
    logs.dbg('Move Orders');
    move_ords_sp(i_div_part);
    logs.dbg('Set Complete Status');
    upd_stat_sp(i_div_part, 'WRK', 'CMP');
    COMMIT;
    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END move_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_SQL_FN
  ||  Return SQL for TMW Routing Extract.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 02/01/18 | rhalpai | Original. PIR17950
  || 05/10/18 | rhalpai | Change BillTo to 'GRO'. PIR17950
  || 06/01/18 | rhalpai | Change ReferenceType4 to 'ORDTYP'. PIR17950
  || 07/03/18 | rhalpai | Change Earliest to use 1950-01-01 for unassigned/non-billable loads.
  ||                    | Change Latest to use 2049-12-31 for unassigned/non-billable loads.
  ||                    | Change LoadDate to use LLR date.
  ||                    | Add columns for HAZLBS for Freezer, Cooler, Dry. PIR17950
  || 04/29/20 | rhalpai | Add ReferenceType5 header with value of SLOT and ReferenceNumber5
  ||                    | header with value of MAN for P00 and AUTO for everything else.
  ||                    | Add restriction for cust status of Active or OnHold. PIR18901
  || 08/05/20 | rhalpai | Change logic to extract at customer level instead of order level. PIR18901
  || 11/04/20 | rhalpai | Add logic to flag orders on 7000 load series as manually routed. PIR18901
  || 01/21/21 | rhalpai | Add logic to exclude ECOM orders and remove load type P00 from being treated as MAN. PIR18901
  || 02/17/21 | rhalpai | Add logic to exclude ROUT load. PIR18901
  || 03/24/21 | rhalpai | Add logic to exclude DFLT,COPY,LOST,CARE loads. PIR18901
  || 04/19/21 | rhalpai | Add logic to flag orders on 3000 load series as manually routed. PIR18901
  || 09/07/21 | rhalpai | Add logic to exclude U*** loads. PIR18901
  || 09/13/21 | rhalpai | Add logic to flag orders on 2000 load series as manually routed. PIR18901
  || 11/01/21 | rhalpai | Remove ord_stat. Add logic to flag orders on assigned loads as AUTO and all othere loads as manually routed. PIR18901
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 01/11/22 | rhalpai | Remove 7000 series loads. PIR18901
  || 03/07/22 | rhalpai | Deprecated - replaced by EXTRACT_CUR_FN. PIR18901
  ||-----------------------------------------------------------------------------
  */
  FUNCTION extract_sql_fn(
    i_file_nm  IN  VARCHAR2
  )
    RETURN CLOB IS
    l_sql  CLOB;
  BEGIN
    l_sql :=
      TO_CLOB
        ('#!/bin/ksh
DIV=MC
. /local/prodcode/properties/op/xxopGeneric.properties
. /usr/local/bin/oraenv
sqlplus -S opcig_user/${DIV_PSWD}@${DSN_NAME} 1>/dev/null <<EOF
SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 0
SET LINESIZE 1000
SET DEFINE OFF
SET HEADING OFF
SET FEEDBACK OFF
EXEC env.set_app_cd(''OPCIG'');
SPOOL /ftptrans/'
         || i_file_nm
         || '
WITH prm AS(
  SELECT op_parms_pk.val_fn(0, ''TMW_EXTR_SHIP_DAYS'') AS ship_days, op_parms_pk.val_fn(0, ''TMW_EXTR_DEPART_DAYS'') AS depart_days
    FROM DUAL
), m AS(
  SELECT m.div_part, m.manctc, (CASE m.catg_typ_cd WHEN ''K'' THEN ''COOLER'' WHEN ''F'' THEN ''FREEZER'' ELSE ''DRY'' END) AS categ
    FROM mclp210c m
), o AS(
  SELECT d.fin_div_cd || d.div_id AS div_id, d.div_part, ld.llr_dt, ld.depart_ts, ld.load_num, a.custa AS cust_id, NVL(se.stop_num, 0) AS stop_num,
         NVL(se.eta_ts, DATE ''1900-01-01'') AS eta_ts, LPAD(cx.corpb, 3, ''0'') AS corp_cd,
--         a.ordnoa AS ord_num, a.dsorda AS ord_typ,
--         (CASE WHEN ld.load_num BETWEEN ''2000'' AND ''5999'' THEN ''MAN'' WHEN ld.load_num BETWEEN ''7000'' AND ''9999'' THEN ''MAN'' ELSE ''AUTO'' END) AS load_typ,
         ''OPEN'' AS ord_stat,
--         (CASE
--            WHEN EXISTS(SELECT 1
--                          FROM strct_ord_op1o s
--                         WHERE s.div_part = d.div_part
--                           AND s.ord_num = a.ordnoa
--                       ) THEN ''Y''
--            ELSE ''N''
--          END) AS strct_sw,
--         DATE ''1900-02-28'' + a.shpja AS ship_dt,
         m.categ,
         b.totctb, ct.outerb, ct.innerb, ct.boxb, ct.totcnb,
--         SUM(b.ordqtb) AS qty,
         SUM(DECODE(b.totctb, NULL, b.ordqtb)) AS cases,
         DECODE(ct.boxb,
                ''N'', DECODE(ct.pccntb,
                            ''Y'', CEIL(SUM(b.ordqtb) / ct.totcnb),
                            ''N'', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                      / DECODE(ct.innerb, NULL, .000001, 0, .000001, ct.innerb)
                                     )
                           )
               ) AS tote_cnt,
         SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube,
         SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
    FROM prm, div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se,
         mclp020b cx, sysp200c c, mclp100a g, ordp120b b, sawp505e e, mclp200b ct, m
   WHERE d.div_id IN(SELECT p.column_value
                       FROM TABLE(op_parms_pk.parms_for_val_fn(0, ''TMW_EXTR_DIV'', ''Y'', 2)) p)
     AND ld.div_part = d.div_part
     AND ld.llr_dt > DATE ''1900-01-01''
     AND ld.depart_ts <= TRUNC(SYSDATE) + prm.depart_days
     AND ld.load_num NOT LIKE ''U%''
     AND ld.load_num NOT LIKE ''7%''
     AND a.div_part = ld.div_part
     AND a.load_depart_sid = ld.load_depart_sid
     AND a.excptn_sw = ''N''
     AND a.stata IN(''O'', ''I'')
     AND a.dsorda IN(''R'', ''D'')
     AND a.ipdtsa NOT IN(SELECT p.column_value
                           FROM TABLE(op_parms_pk.vals_for_prfx_fn(0, ''ECOM_ORDSRC'')) p)
     AND NOT EXISTS(SELECT 1
                      FROM sub_prcs_ord_src s
                     WHERE s.div_part = a.div_part
                       AND s.ord_src = a.ipdtsa
                       AND s.prcs_id = ''LOAD BALANCE''
                       AND s.prcs_sbtyp_cd = ''BLB'')
     AND se.div_part(+) = a.div_part
     AND se.load_depart_sid(+) = a.load_depart_sid
     AND se.cust_id(+) = a.custa
     AND cx.div_part = a.div_part
     AND cx.custb = a.custa
     AND c.div_part = a.div_part
     AND c.acnoc = a.custa
     AND c.statc IN(''1'',''3'')
     AND g.div_part = c.div_part
     AND g.cstgpa = c.retgpc
     AND b.div_part = a.div_part
     AND b.ordnob = a.ordnoa
     AND b.excptn_sw = ''N''
     AND b.statb IN(''O'', ''I'')
     AND b.subrcb < 999
     AND b.ordqtb > 0
     AND b.ntshpb IS NULL
     AND NOT EXISTS(SELECT 1
                      FROM whsp300c w
                     WHERE w.div_part = b.div_part
                       AND w.itemc = b.itemnb
                       AND w.uomc = b.sllumb
                       AND w.taxjrc IS NULL
                       AND w.qavc = 0)
     AND e.iteme = b.itemnb
     AND e.uome = b.sllumb
     AND m.div_part = b.div_part
     AND m.manctc = b.manctb
     AND ct.div_part(+) = b.div_part
     AND ct.totctb(+) = b.totctb
GROUP BY d.fin_div_cd, d.div_id, d.div_part, ld.llr_dt, ld.depart_ts, ld.load_num, a.custa, se.stop_num, se.eta_ts, cx.corpb,
--         (CASE WHEN ld.load_num BETWEEN ''2000'' AND ''5999'' THEN ''MAN'' WHEN ld.load_num BETWEEN ''7000'' AND ''9999'' THEN ''MAN'' ELSE ''AUTO'' END),
         DECODE(g.cntnr_trckg_sw, ''N'', NULL, ''Y'', RTRIM(REPLACE(a.cpoa, ''0''))),
         m.categ, b.manctb, b.totctb, ct.outerb, ct.innerb, ct.pccntb, ct.boxb, ct.totcnb
  UNION ALL
  SELECT d.fin_div_cd || d.div_id AS div_id, d.div_part, ld.llr_dt, ld.depart_ts, ld.load_num, a.custa AS cust_id, NVL(se.stop_num, 0) AS stop_num,
         NVL(se.eta_ts, DATE ''1900-01-01'') AS eta_ts, LPAD(cx.corpb, 3, ''0'') AS corp_cd,
--         (CASE WHEN ld.load_num BETWEEN ''2000'' AND ''5999'' THEN ''MAN'' WHEN ld.load_num BETWEEN ''7000'' AND ''9999'' THEN ''MAN'' ELSE ''AUTO'' END) AS load_typ,
         DECODE(a.stata, ''A'', ''SHIP'', ''BILL'') AS ord_stat,
--         (CASE
--            WHEN EXISTS(SELECT 1
--                          FROM strct_ord_op1o s
--                         WHERE s.div_part = d.div_part
--                           AND s.ord_num = a.ordnoa
--                       ) THEN ''Y''
--            ELSE ''N''
--          END) AS strct_sw,
--         DATE ''1900-02-28'' + a.shpja AS ship_dt,
         m.categ,
         b.totctb, ct.outerb, ct.innerb, ct.boxb, ct.totcnb,
--         SUM(b.pckqtb) AS qty,
         SUM(CASE
               WHEN b.totctb IS NOT NULL THEN 0
               WHEN NOT EXISTS(SELECT 1
                                 FROM kit_item_mstr_kt1m k
                                WHERE k.div_part = d.div_part
                                  AND k.comp_item_num = e.catite) THEN b.pckqtb
               ELSE b.pckqtb
                    / (SELECT MAX(k.comp_qty)
                         FROM kit_item_mstr_kt1m k
                        WHERE k.div_part = d.div_part
                          AND k.comp_item_num = e.catite
                          AND k.comp_item_num =
                                (SELECT MAX(k2.comp_item_num)
                                   FROM kit_item_mstr_kt1m k2
                                  WHERE k2.div_part = k.div_part
                                    AND k2.kit_typ = k.kit_typ
                                    AND k2.item_num = k.item_num))
             END
            ) AS cases,
--         DECODE(ct.boxb,
--                ''N'', DECODE(ct.pccntb,
--                            ''Y'', CEIL(SUM(b.ordqtb) / ct.totcnb),
--                            ''N'', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
--                                      / DECODE(ct.innerb, NULL, .000001, 0, .000001, ct.innerb)
--                                     )
--                           )
--               ) AS tote_cnt,
         (SELECT SUM(DECODE(ct.boxb, ''Y'', mc.boxsmc, mc.totsmc))
            FROM mclp370c mc
           WHERE mc.div_part = d.div_part
             AND mc.llr_date = ld.llr_dt - DATE ''1900-02-28''
             AND mc.loadc = ld.load_num
             AND mc.stopc = se.stop_num
             AND mc.custc = a.custa
             AND mc.manctc = b.manctb
             AND mc.totctc = b.totctb) AS tote_cnt,
         SUM(NVL(e.cubee, .01) * b.pckqtb) AS prod_cube,
         SUM(NVL(e.wghte, .01) * b.pckqtb) AS prod_wt
    FROM prm, div_mstr_di1d d, load_depart_op1f ld, ordp100a a, stop_eta_op1g se,
         mclp020b cx, sysp200c c, mclp100a g, ordp120b b, sawp505e e, mclp200b ct, m
   WHERE d.div_id IN(SELECT p.column_value
                       FROM TABLE(op_parms_pk.parms_for_val_fn(0, ''TMW_EXTR_DIV'', ''Y'', 2)) p)
     AND ld.div_part = d.div_part
     AND ld.llr_dt > DATE ''1900-01-01''
     AND ld.depart_ts <= TRUNC(SYSDATE) + prm.depart_days
     AND ld.load_num NOT LIKE ''U%''
     AND ld.load_num NOT LIKE ''7%''
     AND a.div_part = ld.div_part
     AND a.load_depart_sid = ld.load_depart_sid
     AND a.excptn_sw = ''N''
     AND a.stata IN(''P'', ''R'', ''A'')
     AND a.dsorda IN(''R'', ''D'')
     AND a.ipdtsa NOT IN(SELECT p.column_value
                           FROM TABLE(op_parms_pk.vals_for_prfx_fn(0, ''ECOM_ORDSRC'')) p)
     AND NOT EXISTS(SELECT 1
                      FROM sub_prcs_ord_src s
                     WHERE s.div_part = a.div_part
                       AND s.ord_src = a.ipdtsa
                       AND s.prcs_id = ''LOAD BALANCE''
                       AND s.prcs_sbtyp_cd = ''BLB'')
     AND se.div_part(+) = a.div_part
     AND se.load_depart_sid(+) = a.load_depart_sid
     AND se.cust_id(+) = a.custa
     AND cx.div_part = a.div_part
     AND cx.custb = a.custa
     AND c.div_part = a.div_part
     AND c.acnoc = a.custa
     AND c.statc IN(''1'',''3'')
     AND g.div_part = c.div_part
     AND g.cstgpa = c.retgpc
     AND b.div_part = a.div_part
     AND b.ordnob = a.ordnoa
     AND b.excptn_sw = ''N''
     AND b.statb IN(''P'', ''T'', ''R'', ''A'')
     AND b.subrcb < 999
     AND b.pckqtb > 0
--     AND b.ntshpb IS NULL
     AND e.iteme = b.itemnb
     AND e.uome = b.sllumb
     AND m.div_part = b.div_part
     AND m.manctc = b.manctb
     AND ct.div_part(+) = b.div_part
     AND ct.totctb(+) = b.totctb
GROUP BY d.fin_div_cd, d.div_id, d.div_part, ld.llr_dt, ld.depart_ts, ld.load_num, a.custa, se.stop_num, se.eta_ts, cx.corpb,
--         a.ordnoa, a.dsorda,
--         (CASE WHEN ld.load_num BETWEEN ''2000'' AND ''5999'' THEN ''MAN'' WHEN ld.load_num BETWEEN ''7000'' AND ''9999'' THEN ''MAN'' ELSE ''AUTO'' END),
         DECODE(a.stata, ''A'', ''SHIP'', ''BILL''),
         DECODE(g.cntnr_trckg_sw, ''N'', NULL, ''Y'', RTRIM(REPLACE(a.cpoa, ''0''))),
--         a.shpja,
         m.categ, b.manctb, b.totctb, ct.outerb, ct.innerb, ct.pccntb, ct.boxb, ct.totcnb
), x AS(
  SELECT o.div_id, o.div_part, o.cust_id, o.load_num, o.stop_num, o.eta_ts, o.llr_dt, o.depart_ts, o.corp_cd,
--         o.ord_num, o.ord_typ,
         (CASE
            WHEN EXISTS(SELECT 1
                          FROM cust_rte_ovrrd_rt3c cro
                         WHERE cro.div_part = o.div_part
                           AND cro.cust_id = o.cust_id
                           AND cro.llr_dt = o.llr_dt
                           AND cro.load_num = o.load_num) THEN ''AUTO''
            WHEN EXISTS(SELECT 1
                          FROM mclp040d md
                         WHERE md.div_part = o.div_part
                           AND md.custd = o.cust_id
                           AND md.loadd = o.load_num) THEN ''AUTO''
            ELSE ''MAN''
          END) AS load_typ,
--         o.ord_stat,
--         o.strct_sw, o.ship_dt,
--         SUM(o.qty) AS qty,
         SUM(o.cases) AS cases,
         NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
         SUM(o.prod_cube) AS prod_cube,
         SUM(o.prod_wt) AS prod_wt,
         SUM(DECODE(o.categ, ''FREEZER'', DECODE(o.totctb, NULL, o.cases, 0), 0)) AS fzr_case,
         SUM(DECODE(o.categ, ''FREEZER'', o.prod_wt, 0)) AS fzr_wt,
         SUM(DECODE(o.categ, ''FREEZER'', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb), 0)) AS fzr_vol,
         SUM(DECODE(o.categ, ''COOLER'', DECODE(o.totctb, NULL, o.cases, 0), 0)) AS cool_case,
         SUM(DECODE(o.categ, ''COOLER'', o.prod_wt, 0)) AS cool_wt,
         SUM(DECODE(o.categ, ''COOLER'', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb), 0)) AS cool_vol,
         SUM(DECODE(o.categ, ''DRY'', DECODE(o.totctb, NULL, o.cases, 0), 0)) AS dry_case,
         SUM(DECODE(o.categ, ''DRY'', o.prod_wt, 0)) AS dry_wt,
         SUM(DECODE(o.categ, ''DRY'', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb), 0)) AS dry_vol
    FROM o
  GROUP BY o.div_id, o.div_part, o.cust_id, o.load_num, o.stop_num, o.eta_ts, o.llr_dt, o.depart_ts, o.corp_cd,
--           o.ord_num, o.ord_typ,
           (CASE
              WHEN EXISTS(SELECT 1
                            FROM cust_rte_ovrrd_rt3c cro
                           WHERE cro.div_part = o.div_part
                             AND cro.cust_id = o.cust_id
                             AND cro.llr_dt = o.llr_dt
                             AND cro.load_num = o.load_num) THEN ''AUTO''
              WHEN EXISTS(SELECT 1
                            FROM mclp040d md
                           WHERE md.div_part = o.div_part
                             AND md.custd = o.cust_id
                             AND md.loadd = o.load_num) THEN ''AUTO''
              ELSE ''MAN''
            END)--, o.ord_stat, o.strct_sw, o.ship_dt
)
SELECT ''RowAction''
       || CHR(9) || ''BillTo''
       || CHR(9) || ''Shipper''
       || CHR(9) || ''Consignee''
       || CHR(9) || ''Earliest''
       || CHR(9) || ''Latest''
       || CHR(9) || ''LoadDate''
       || CHR(9) || ''Source''
       || CHR(9) || ''ExternalIdMcLaneOrder''
       || CHR(9) || ''RollIntoExternalId''
       || CHR(9) || ''RevType1Subsidiary''
       || CHR(9) || ''RevType2Division''
       || CHR(9) || ''RevType3ChainCorp''
       || CHR(9) || ''ReferenceType1''
       || CHR(9) || ''ReferenceNumber1''
       || CHR(9) || ''ReferenceType2''
       || CHR(9) || ''ReferenceNumber2''
       || CHR(9) || ''ReferenceType3''
       || CHR(9) || ''ReferenceNumber3''
       || CHR(9) || ''ReferenceType4''
       || CHR(9) || ''ReferenceNumber4''
       || CHR(9) || ''ReferenceType5''
       || CHR(9) || ''ReferenceNumber5''
       || CHR(9) || ''Commodity1''
       || CHR(9) || ''Count1''
       || CHR(9) || ''CountUnit1''
       || CHR(9) || ''Weight1''
       || CHR(9) || ''WeightUnit1''
       || CHR(9) || ''Volume1''
       || CHR(9) || ''VolumeUnit1''
       || CHR(9) || ''Count1A''
       || CHR(9) || ''Count2Unit1A''
       || CHR(9) || ''Commodity2''
       || CHR(9) || ''Count2''
       || CHR(9) || ''CountUnit2''
       || CHR(9) || ''Weight2''
       || CHR(9) || ''WeightUnit2''
       || CHR(9) || ''Volume2''
       || CHR(9) || ''VolumeUnit2''
       || CHR(9) || ''Count2A''
       || CHR(9) || ''Count2Unit2A''
       || CHR(9) || ''Commodity3''
       || CHR(9) || ''Count3''
       || CHR(9) || ''CountUnit3''
       || CHR(9) || ''Weight3''
       || CHR(9) || ''WeightUnit3''
       || CHR(9) || ''Volume3''
       || CHR(9) || ''VolumeUnit3''
       || CHR(9) || ''Count3A''
       || CHR(9) || ''Count2Unit3A''
       || CHR(9) || ''ExtraData1''
       || CHR(9) || ''ExtraData2''
       || CHR(9) || ''ExtraData3''
  FROM dual
UNION ALL
SELECT extr
  FROM (SELECT
               ''ADD''
               || CHR(9)
               || ''GRO''
               || CHR(9)
               || x.div_id
               || CHR(9)
               || x.cust_id
               || CHR(9)
               || TO_CHAR((CASE x.eta_ts
                             WHEN DATE ''1900-01-01'' THEN DATE ''1950-01-01''
                             ELSE x.eta_ts - (CASE WHEN TO_CHAR(x.eta_ts, ''HH24'') = ''00'' THEN 1 ELSE 0 END)
                           END),
                          ''YYYY-MM-DD'')
               || CHR(9)
               || TO_CHAR((CASE x.eta_ts
                             WHEN DATE ''1900-01-01'' THEN DATE ''2049-12-31''
                             ELSE x.eta_ts + (CASE WHEN TO_CHAR(x.eta_ts, ''HH24'') = ''23'' THEN 1 ELSE 0 END)
                           END),
                          ''YYYY-MM-DD'')
               || CHR(9)
--               || (CASE
--                     WHEN (x.ord_typ = ''D'' AND x.llr_dt = DATE ''1900-01-01'') THEN TO_CHAR(x.ship_dt, ''YYYY-MM-DD'')
--                     ELSE TO_CHAR(x.llr_dt, ''YYYY-MM-DD'')
--                   END)
               || TO_CHAR(x.llr_dt, ''YYYY-MM-DD'')
               || CHR(9)
               || ''GROCERY''
               || CHR(9)
--               || x.ord_num
               || x.div_id
               || TO_CHAR(x.llr_dt, ''YYYYMMDD'')
               || x.load_num
               || x.cust_id
               || CHR(9)
               || CHR(9)
               || ''GRO''
               || CHR(9)
               || x.div_id
               || CHR(9)
               || x.corp_cd
               || CHR(9)
               || ''ROUTE''
               || CHR(9)
               || x.load_num
               || CHR(9)
               || ''STOP''
               || CHR(9)
               || x.stop_num
               || CHR(9)
               || ''SERIES''
               || CHR(9)
               || (CASE
                     WHEN x.load_num BETWEEN ''0100'' AND ''9799'' THEN ''0'' || SUBSTR(x.load_num, 2, 1) || ''00''
                     ELSE ''0000''
                   END)
               || CHR(9)
               || ''ORDTYP''
               || CHR(9)
--               || (CASE
--                    WHEN x.strct_sw = ''Y'' THEN ''STR''
--                    WHEN x.ord_typ = ''D'' THEN DECODE(SUBSTR(x.load_typ, 1, 1), ''P'', ''P00'', ''R00'')
--                    WHEN x.load_typ = ''GMP'' THEN ''GMP''
--                    ELSE ''STD''
--                   END)
               || ''''
               || CHR(9)
               || ''SLOT''
               || CHR(9)
--               || (CASE WHEN x.load_typ BETWEEN ''P00'' AND ''P99'' THEN ''MAN'' ELSE ''AUTO'' END)
               || x.load_typ
               || CHR(9)
               || ''FREEZER''
               || CHR(9)
               || x.fzr_case
               || CHR(9)
               || ''CAS''
               || CHR(9)
               || x.fzr_wt
               || CHR(9)
               || ''LBS''
               || CHR(9)
               || x.fzr_vol
               || CHR(9)
               || ''CUB''
               || CHR(9)
               || 0
               || CHR(9)
               || ''HAZLBS''
               || CHR(9)
               || ''COOLER''
               || CHR(9)
               || x.cool_case
               || CHR(9)
               || ''CAS''
               || CHR(9)
               || x.cool_wt
               || CHR(9)
               || ''LBS''
               || CHR(9)
               || x.cool_vol
               || CHR(9)
               || ''CUB''
               || CHR(9)
               || 0
               || CHR(9)
               || ''HAZLBS''
               || CHR(9)
               || ''DRY''
               || CHR(9)
               || x.dry_case
               || CHR(9)
               || ''CAS''
               || CHR(9)
               || x.dry_wt
               || CHR(9)
               || ''LBS''
               || CHR(9)
               || x.dry_vol
               || CHR(9)
               || ''CUB''
               || CHR(9)
               || 0
               || CHR(9)
               || ''HAZLBS''
               || CHR(9)
               || '' '' --x.ord_stat
               || CHR(9)
               || '' ''
               || CHR(9)
               || '' '' AS extr
          FROM x
         ORDER BY x.cust_id, x.load_typ
       );
SPOOL OFF
quit
EOF
'
        );
    RETURN(l_sql);
  END extract_sql_fn;

  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_CUR_FN
  ||  Return Cursor for TMW Routing Extract.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/07/22 | rhalpai | Original. PIR18901
  || 04/27/22 | rhalpai | Add logic to exclude state codes and add load status. PIR18901
  || 05/19/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  || 11/07/24 | rhalpai | Change logic to replace parm TMW_EXTR_DEPART_DAYS with TMW_EXTR_LLR_DAYS. MODTMS-79
  ||-----------------------------------------------------------------------------
  */
  FUNCTION extract_cur_fn(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  )
    RETURN SYS_REFCURSOR IS
    l_div_part   NUMBER;
    l_cv  SYS_REFCURSOR;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);
    OPEN l_cv
     FOR
      WITH m AS
           (SELECT m.manctc AS mfst_catg, (CASE m.catg_typ_cd
                                           WHEN 'K' THEN 'COOLER'
                                           WHEN 'F' THEN 'FREEZER'
                                           ELSE 'DRY'
                                           END) AS mfst_catg_typ
              FROM mclp210c m
             WHERE m.div_part = l_div_part),
           x AS
           (SELECT   d.fin_div_cd || d.div_id AS div_id, d.div_part, o.llr_dt, o.depart_ts, o.load_num, o.cust_id, o.stop_num, o.eta_ts, o.corp_cd,
                     (CASE
                        WHEN EXISTS(SELECT 1
                                      FROM cust_rte_ovrrd_rt3c cro
                                     WHERE cro.div_part = d.div_part
                                       AND cro.cust_id = o.cust_id
                                       AND cro.llr_dt = o.llr_dt
                                       AND cro.load_num = o.load_num) THEN 'AUTO'
                        WHEN EXISTS(SELECT 1
                                      FROM mclp040d md
                                     WHERE md.div_part = d.div_part
                                       AND md.custd = o.cust_id
                                       AND md.loadd = o.load_num) THEN 'AUTO'
                        ELSE 'MAN'
                      END
                     ) AS load_typ,
                     NVL(SUM(o.case_cnt), 0) AS case_cnt,
                     NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
                     NVL(SUM(o.prod_cube), 0) AS prod_cube,
                     NVL(SUM(o.prod_wt), 0) AS prod_wt,
                     SUM(DECODE(m.mfst_catg_typ, 'FREEZER', DECODE(o.tote_catg, NULL, o.case_cnt, 0), 0)) AS fzr_case,
                     SUM(DECODE(m.mfst_catg_typ, 'FREEZER', o.prod_wt, 0)) AS fzr_wt,
                     SUM(DECODE(m.mfst_catg_typ, 'FREEZER', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube), 0)
                        ) AS fzr_vol,
                     SUM(DECODE(m.mfst_catg_typ, 'COOLER', DECODE(o.tote_catg, NULL, o.case_cnt, 0), 0)) AS cool_case,
                     SUM(DECODE(m.mfst_catg_typ, 'COOLER', o.prod_wt, 0)) AS cool_wt,
                     SUM(DECODE(m.mfst_catg_typ, 'COOLER', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube), 0)
                        ) AS cool_vol,
                     SUM(DECODE(m.mfst_catg_typ, 'DRY', DECODE(o.tote_catg, NULL, o.case_cnt, 0), 0)) AS dry_case,
                     SUM(DECODE(m.mfst_catg_typ, 'DRY', o.prod_wt, 0)) AS dry_wt,
                     SUM(DECODE(m.mfst_catg_typ, 'DRY', DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube), 0)) AS dry_vol
                FROM TABLE(op_load_balance_pk.ord_dtl_fn(l_div_part,
                                                         CURSOR(
                                                           SELECT oh.*
                                                             FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part)) oh,
                                                                  (SELECT op_parms_pk.val_fn(0, 'TMW_EXTR_SHIP_DAYS') AS ship_days,
                                                                          op_parms_pk.val_fn(0, 'TMW_EXTR_LLR_DAYS') AS llr_days
                                                                     FROM DUAL) prm,
                                                                  load_depart_op1f ld, sysp200c c
                                                            WHERE ld.div_part = l_div_part
                                                              AND ld.load_depart_sid = oh.load_depart_sid
                                                              AND ld.llr_dt > DATE '1900-01-01'
                                                              AND ld.llr_dt <= TRUNC(i_prcs_ts) + prm.llr_days
                                                              AND ld.load_num NOT LIKE 'U%'
                                                              AND ld.load_num NOT LIKE '7%'
                                                              AND oh.ord_src NOT IN(SELECT p.column_value
                                                                                      FROM TABLE(op_parms_pk.vals_for_prfx_fn(0, 'ECOM_ORDSRC')) p)
                                                              AND c.div_part = l_div_part
                                                              AND c.acnoc = oh.cust_id
                                                              AND c.shpstc NOT IN(SELECT p.column_value
                                                                                     FROM TABLE(op_parms_pk.vals_for_prfx_fn(0, 'TMW_EXCL_ST')) p)
                                                         )
                                                        )
                          ) o,
                     div_mstr_di1d d, m
               WHERE d.div_part = l_div_part
--AND o.prod_cube > 0
                 AND m.mfst_catg(+) = o.mfst_catg
            GROUP BY d.fin_div_cd, d.div_id, d.div_part, o.llr_dt, o.depart_ts, o.load_num, o.cust_id, o.stop_num, o.eta_ts, o.corp_cd,
                     (CASE
                        WHEN EXISTS(SELECT 1
                                      FROM cust_rte_ovrrd_rt3c cro
                                     WHERE cro.div_part = d.div_part
                                       AND cro.cust_id = o.cust_id
                                       AND cro.llr_dt = o.llr_dt
                                       AND cro.load_num = o.load_num) THEN 'AUTO'
                        WHEN EXISTS(SELECT 1
                                      FROM mclp040d md
                                     WHERE md.div_part = d.div_part
                                       AND md.custd = o.cust_id
                                       AND md.loadd = o.load_num) THEN 'AUTO'
                        ELSE 'MAN'
                      END
                     ))
      SELECT 'RowAction'
             || CHR(9)
             || 'BillTo'
             || CHR(9)
             || 'Shipper'
             || CHR(9)
             || 'Consignee'
             || CHR(9)
             || 'Earliest'
             || CHR(9)
             || 'Latest'
             || CHR(9)
             || 'LoadDate'
             || CHR(9)
             || 'Source'
             || CHR(9)
             || 'ExternalIdMcLaneOrder'
             || CHR(9)
             || 'RollIntoExternalId'
             || CHR(9)
             || 'RevType1Subsidiary'
             || CHR(9)
             || 'RevType2Division'
             || CHR(9)
             || 'RevType3ChainCorp'
             || CHR(9)
             || 'ReferenceType1'
             || CHR(9)
             || 'ReferenceNumber1'
             || CHR(9)
             || 'ReferenceType2'
             || CHR(9)
             || 'ReferenceNumber2'
             || CHR(9)
             || 'ReferenceType3'
             || CHR(9)
             || 'ReferenceNumber3'
             || CHR(9)
             || 'ReferenceType4'
             || CHR(9)
             || 'ReferenceNumber4'
             || CHR(9)
             || 'ReferenceType5'
             || CHR(9)
             || 'ReferenceNumber5'
             || CHR(9)
             || 'Commodity1'
             || CHR(9)
             || 'Count1'
             || CHR(9)
             || 'CountUnit1'
             || CHR(9)
             || 'Weight1'
             || CHR(9)
             || 'WeightUnit1'
             || CHR(9)
             || 'Volume1'
             || CHR(9)
             || 'VolumeUnit1'
             || CHR(9)
             || 'Count1A'
             || CHR(9)
             || 'Count2Unit1A'
             || CHR(9)
             || 'Commodity2'
             || CHR(9)
             || 'Count2'
             || CHR(9)
             || 'CountUnit2'
             || CHR(9)
             || 'Weight2'
             || CHR(9)
             || 'WeightUnit2'
             || CHR(9)
             || 'Volume2'
             || CHR(9)
             || 'VolumeUnit2'
             || CHR(9)
             || 'Count2A'
             || CHR(9)
             || 'Count2Unit2A'
             || CHR(9)
             || 'Commodity3'
             || CHR(9)
             || 'Count3'
             || CHR(9)
             || 'CountUnit3'
             || CHR(9)
             || 'Weight3'
             || CHR(9)
             || 'WeightUnit3'
             || CHR(9)
             || 'Volume3'
             || CHR(9)
             || 'VolumeUnit3'
             || CHR(9)
             || 'Count3A'
             || CHR(9)
             || 'Count2Unit3A'
             || CHR(9)
             || 'ExtraData1'
             || CHR(9)
             || 'ExtraData2'
             || CHR(9)
             || 'ExtraData3'
        FROM DUAL
      UNION ALL
      SELECT extr
        FROM (SELECT   'ADD'
                       || CHR(9)
                       || 'GRO'
                       || CHR(9)
                       || x.div_id
                       || CHR(9)
                       || x.cust_id
                       || CHR(9)
                       || TO_CHAR((CASE x.eta_ts
                                     WHEN DATE '1900-01-01' THEN DATE '1950-01-01'
                                     ELSE x.eta_ts -(CASE
                                                       WHEN TO_CHAR(x.eta_ts, 'HH24') = '00' THEN 1
                                                       ELSE 0
                                                     END)
                                   END
                                  ),
                                  'YYYY-MM-DD'
                                 )
                       || CHR(9)
                       || TO_CHAR((CASE x.eta_ts
                                     WHEN DATE '1900-01-01' THEN DATE '2049-12-31'
                                     ELSE x.eta_ts +(CASE
                                                       WHEN TO_CHAR(x.eta_ts, 'HH24') = '23' THEN 1
                                                       ELSE 0
                                                     END)
                                   END
                                  ),
                                  'YYYY-MM-DD'
                                 )
                       || CHR(9)
                       || TO_CHAR(x.llr_dt, 'YYYY-MM-DD')
                       || CHR(9)
                       || 'GROCERY'
                       || CHR(9)
                       || x.div_id
                       || TO_CHAR(x.llr_dt, 'YYYYMMDD')
                       || x.load_num
                       || x.cust_id
                       || CHR(9)
                       || CHR(9)
                       || 'GRO'
                       || CHR(9)
                       || x.div_id
                       || CHR(9)
                       || x.corp_cd
                       || CHR(9)
                       || 'ROUTE'
                       || CHR(9)
                       || x.load_num
                       || CHR(9)
                       || 'STOP'
                       || CHR(9)
                       || x.stop_num
                       || CHR(9)
                       || 'SERIES'
                       || CHR(9)
                       ||(CASE
                            WHEN x.load_num BETWEEN '0100' AND '9799' THEN '0' || SUBSTR(x.load_num, 2, 1) || '00'
                            ELSE '0000'
                          END)
                       || CHR(9)
                       || 'ORDTYP'
                       || CHR(9)
                       || ''
                       || CHR(9)
                       || 'SLOT'
                       || CHR(9)
                       || x.load_typ
                       || CHR(9)
                       || 'FREEZER'
                       || CHR(9)
                       || x.fzr_case
                       || CHR(9)
                       || 'CAS'
                       || CHR(9)
                       || x.fzr_wt
                       || CHR(9)
                       || 'LBS'
                       || CHR(9)
                       || x.fzr_vol
                       || CHR(9)
                       || 'CUB'
                       || CHR(9)
                       || 0
                       || CHR(9)
                       || 'HAZLBS'
                       || CHR(9)
                       || 'COOLER'
                       || CHR(9)
                       || x.cool_case
                       || CHR(9)
                       || 'CAS'
                       || CHR(9)
                       || x.cool_wt
                       || CHR(9)
                       || 'LBS'
                       || CHR(9)
                       || x.cool_vol
                       || CHR(9)
                       || 'CUB'
                       || CHR(9)
                       || 0
                       || CHR(9)
                       || 'HAZLBS'
                       || CHR(9)
                       || 'DRY'
                       || CHR(9)
                       || x.dry_case
                       || CHR(9)
                       || 'CAS'
                       || CHR(9)
                       || x.dry_wt
                       || CHR(9)
                       || 'LBS'
                       || CHR(9)
                       || x.dry_vol
                       || CHR(9)
                       || 'CUB'
                       || CHR(9)
                       || 0
                       || CHR(9)
                       || 'HAZLBS'
                       || CHR(9)
                       || DECODE((SELECT lc.load_status
                                    FROM load_clos_cntrl_bc2c lc
                                   WHERE lc.div_part = x.div_part
                                     AND lc.llr_dt = x.llr_dt
                                     AND lc.load_num = x.load_num),
                                 NULL, 'OPEN',
                                 'A', 'SHIP',
                                 'BILL'
                                )   -- load_stat
                       || CHR(9)
                       || ' '
                       || CHR(9)
                       || ' ' AS extr
                  FROM x
              ORDER BY x.cust_id, x.load_typ);

    RETURN(l_cv);
  END extract_cur_fn;

  /*
  ||-----------------------------------------------------------------------------
  || EXTRACT_SP
  ||  Create routing extract file for TMW system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/07/22 | rhalpai | Original. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE extract_sp(
    i_div      IN  VARCHAR2,
    i_file_nm  IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE,
    i_user_id  IN  VARCHAR2 DEFAULT 'MQ'
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                       := 'OP_TMW_EXTRACT_PK.EXTRACT_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_tmw_extr_sw         VARCHAR2(1);
    l_c_prcs_id  CONSTANT VARCHAR2(30)   := 'TMW_EXTRACT';
    l_cv                  SYS_REFCURSOR;
    l_t_rpt_lns           typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get DivPart');
    l_div_part := div_pk.div_part_fn(i_div);
    l_tmw_extr_sw := op_parms_pk.val_fn(l_div_part, 'TMW_EXTR_DIV_' || i_div);

    IF l_tmw_extr_sw = 'Y' THEN
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(l_c_prcs_id, op_process_control_pk.g_c_active, i_user_id, l_div_part);
      logs.dbg('Get Extract Cursor');
      l_cv := op_tmw_extract_pk.extract_cur_fn(i_div, i_prcs_ts);
      logs.dbg('Fetch Extract Cursor');

      FETCH l_cv
      BULK COLLECT INTO l_t_rpt_lns;

      logs.dbg('Write');
      write_sp(l_t_rpt_lns, i_file_nm);
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(l_c_prcs_id,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    END IF;   -- l_tmw_extr_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      op_process_control_pk.set_process_status_sp(l_c_prcs_id,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extract_sp;

  /*
  ||-----------------------------------------------------------------------------
  || IMPORT_RPT_SP
  ||  Create and send TMW_POST_IMPORT csv file to TMW system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 01/10/19 | rhalpai | Original. PIR18901
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE import_rpt_sp(
    i_div_part   IN  NUMBER,
    i_create_ts  IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_TMW_EXTRACT_PK.IMPORT_RPT_SP';
    lar_parm             logs.tar_parm;
    l_div                VARCHAR2(2);
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_prod_test          VARCHAR2(50);
    l_file_nm            VARCHAR2(50);
    l_t_rpt_lns          typ.tas_maxvc2;
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CreateTs', i_create_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div := div_pk.div_id_fn(i_div_part);
    l_appl_srvr := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_appl_srvr);
    l_prod_test :=(CASE
                     WHEN UPPER(SYS_CONTEXT('USERENV', 'DB_NAME')) = 'OPCIGP' THEN 'Prod'
                     ELSE 'Test'
                   END);
    l_file_nm := 'TMW_POST_IMPORT_' || TO_CHAR(i_create_ts, 'YYYYMMDDHH24MISS') || '_' || l_div || '.csv';
    logs.dbg('Get Report Data');

    SELECT x.rpt_ln
    BULK COLLECT INTO l_t_rpt_lns
      FROM (SELECT 'CustRteReqSid'
                   || ','
                   || 'DivId'
                   || ','
                   || 'LLRDt'
                   || ','
                   || 'LoadNum'
                   || ','
                   || 'CustId'
                   || ','
                   || 'NewLLRDt'
                   || ','
                   || 'NewLoadNum'
                   || ','
                   || 'NewDepartTs'
                   || ','
                   || 'NewStopNum'
                   || ','
                   || 'NewEtaTs'
                   || ','
                   || 'OrdNumList'
                   || ','
                   || 'StatCd'
                   || ','
                   || 'ErrMsg'
                   || ','
                   || 'CreateTs'
                   || ','
                   || 'LastChgTs' AS rpt_ln
              FROM DUAL
            UNION ALL
            SELECT rpt_ln
              FROM (SELECT   r.cust_rte_req_sid
                             || ','
                             || d.div_id
                             || ','
                             || TO_CHAR(r.llr_dt, 'YYYY-MM-DD')
                             || ','
                             || r.load_num
                             || ','
                             || r.cust_id
                             || ','
                             || TO_CHAR(r.new_llr_dt, 'YYYY-MM-DD')
                             || ','
                             || r.new_load_num
                             || ','
                             || TO_CHAR(r.new_depart_ts, 'YYYY-MM-DD HH24:MI')
                             || ','
                             || r.new_stop_num
                             || ','
                             || TO_CHAR(r.new_eta_ts, 'YYYY-MM-DD HH24:MI')
                             || ','
                             || TO_CHAR(r.ord_num_list)
                             || ','
                             || r.stat_cd
                             || ','
                             || r.err_msg
                             || ','
                             || TO_CHAR(r.create_ts, 'YYYY-MM-DD HH24:MI:SS')
                             || ','
                             || TO_CHAR(r.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS rpt_ln
                        FROM cust_rte_req_op4c r, div_mstr_di1d d
                       WHERE r.div_part = i_div_part
                         AND r.create_ts = i_create_ts
                         AND d.div_part = r.div_part
                    ORDER BY r.cust_rte_req_sid)) x;

    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_file_nm);
    logs.dbg('OS Command Setup');
    l_cmd := 'mv /ftptrans/'
             || l_file_nm
             || ' '
             || '/TMW/'
             || l_prod_test
             || '/;mailx -s ''TMW OP Post Import Report for '
             || l_div
             || ''' TMWOPExceptions@mclane.mclaneco.com <<EOM'
             || cnst.newline_char
             || 'file:///\\mclane.mclaneco.com\MCLANE-ROOT$\APP-DATA\DivDATA\MC\TMW\'
             || l_prod_test
             || '\'
             || l_file_nm
             || cnst.newline_char
             || 'EOM'
             || cnst.newline_char;
    logs.dbg('Process Command' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.dbg('OS Result' || cnst.newline_char || l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END import_rpt_sp;

  /*
  ||-----------------------------------------------------------------------------
  || IMPORT_SP
  ||  Get CustRteReq for each Div and apply Moves.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 12/08/18 | rhalpai | Original. PIR18901
  || 01/10/19 | rhalpai | Add logic to create TMW_POST_IMPORT csv file. PIR18901
  || 11/01/21 | rhalpai | Add i_div parm. Add logic to remove unprocessed previous requests. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE import_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_TMW_EXTRACT_PK.IMPORT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_create_ts          DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get CreateTs');

    SELECT MAX(r.create_ts)
      INTO l_create_ts
      FROM cust_rte_req_op4c r
     WHERE r.div_part = l_div_part
       AND r.stat_cd = 'OPN';

    IF l_create_ts IS NOT NULL THEN
      logs.dbg('Remove unprocessed previous requests');

      DELETE FROM cust_rte_req_op4c r
            WHERE r.div_part = l_div_part
              AND r.create_ts IN(SELECT   rr.create_ts
                                     FROM cust_rte_req_op4c rr
                                    WHERE rr.div_part = l_div_part
                                      AND rr.stat_cd = 'OPN'
                                      AND rr.create_ts < l_create_ts
                                 GROUP BY rr.create_ts);

      logs.dbg('Process Move');
      move_sp(l_div_part);
      logs.dbg('Process ImportRpt');
      import_rpt_sp(l_div_part, l_create_ts);
    END IF;   -- l_create_ts IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END import_sp;
END op_tmw_extract_pk;
/

