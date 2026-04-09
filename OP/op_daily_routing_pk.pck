CREATE OR REPLACE PACKAGE op_daily_routing_pk IS
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
  FUNCTION file_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE audit_list_sp(
    i_div     IN      VARCHAR2,
    i_llr_dt  IN      VARCHAR2,
    o_cur     OUT     SYS_REFCURSOR
  );

  PROCEDURE load_sum_sp(
    i_div     IN      VARCHAR2,
    i_llr_dt  IN      VARCHAR2,
    o_cur     OUT     SYS_REFCURSOR
  );

  PROCEDURE stop_sum_sp(
    i_div       IN      VARCHAR2,
    i_llr_dt    IN      VARCHAR2,
    i_load_num  IN      VARCHAR2,
    o_cur       OUT     SYS_REFCURSOR
  );

  PROCEDURE tmw_export_sp(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  );

  PROCEDURE export_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE last_import_sp(
    i_div          IN      VARCHAR2,
    o_rte_grp_num  OUT     NUMBER,
    o_llr_dt       OUT     VARCHAR2,
    o_ts           OUT     VARCHAR2
  );

  PROCEDURE rte_import_list_sp(
    i_div          IN      VARCHAR2,
    i_rte_grp_num  IN      NUMBER,
    i_llr_dt       IN      VARCHAR2,
    o_cur          OUT     SYS_REFCURSOR
  );

  PROCEDURE import_sp(
    i_div             IN  VARCHAR2,
    i_remote_file_nm  IN  VARCHAR2,
    i_user_id         IN  VARCHAR2
  );

  PROCEDURE cancl_rte_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_cust_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE upd_rte_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_parm_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE move_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE undo_move_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );
END op_daily_routing_pk;
/

CREATE OR REPLACE PACKAGE BODY op_daily_routing_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_import IS RECORD(
    div            VARCHAR2(2),
    llr_dt         DATE,
    depart_ts      DATE,
    eta_ts         DATE,
    load_num       VARCHAR2(4),
    stop_num       PLS_INTEGER,
    mcl_cust       VARCHAR2(6),
    new_load       VARCHAR2(4),
    new_stop       PLS_INTEGER,
    new_depart_ts  DATE,
    new_eta_ts     DATE
  );

  g_e_parse_error          EXCEPTION;
  PRAGMA EXCEPTION_INIT(g_e_parse_error, -20001);
  g_e_invalid_div          EXCEPTION;
  PRAGMA EXCEPTION_INIT(g_e_invalid_div, -20002);
  g_e_mixed_llr_dates      EXCEPTION;
  PRAGMA EXCEPTION_INIT(g_e_mixed_llr_dates, -20003);
  g_c_ftp_user    CONSTANT VARCHAR2(80) := 'mclane\\\SVC_DIVDFTP_PARAGON_';
  g_c_ftp_pswd    CONSTANT VARCHAR2(80) := 'wD7EWBNzpDzoM+W/AEJG';
  g_c_actn_cancl  CONSTANT VARCHAR2(4)  := 'CNCL';
  g_c_actn_move   CONSTANT VARCHAR2(4)  := 'MOVE';
  g_c_actn_undo   CONSTANT VARCHAR2(4)  := 'UNDO';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || CLEAN_EXPORT_LOADS_FN
  ||  Return nested table of loads for passed LLR/LoadList that contain only
  ||  unbilled orders.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 04/12/16 | rhalpai | Change to use div_part input parm. PIR14660
  ||-----------------------------------------------------------------------------
  */
  FUNCTION clean_export_loads_fn(
    i_div_part   IN  NUMBER,
    i_llr_num    IN  NUMBER,
    i_load_list  IN  VARCHAR2
  )
    RETURN type_stab IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.CLEAN_EXPORT_LOADS_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_t_loads            type_stab;
    l_t_clean_loads      type_stab     := type_stab();
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRNum', i_llr_num);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.dbg('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Initialize');
      l_llr_dt := DATE '1900-02-28' + i_llr_num;
      logs.dbg('Parse');
      l_t_loads := str.parse_list(i_load_list, op_const_pk.field_delimiter);

      IF l_t_loads.COUNT > 0 THEN
        logs.dbg('Ensure loads contain only unbilled orders');

        SELECT   q.load_num
        BULK COLLECT INTO l_t_clean_loads
            FROM (SELECT   ld.load_num, MIN(DECODE(b.statb, 'O', '1', 'R', '3', 'A', '4', '2')) AS min_stat,
                           MAX(DECODE(b.statb, 'O', '1', 'R', '3', 'A', '4', '2')) AS max_stat
                      FROM TABLE(CAST(l_t_loads AS type_stab)) t, load_depart_op1f ld, ordp100a a, ordp120b b
                     WHERE ld.div_part = i_div_part
                       AND ld.llr_dt = l_llr_dt
                       AND ld.load_num = t.column_value
                       AND a.div_part = ld.div_part
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb NOT IN('I', 'S', 'C')
                  GROUP BY ld.load_num) q
        GROUP BY q.load_num
          HAVING MIN(q.min_stat) = '1'
             AND MAX(q.max_stat) = '1';
      END IF;   -- l_t_loads.COUNT > 0
    END IF;   -- i_load_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_t_clean_loads);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END clean_export_loads_fn;

  /*
  ||-----------------------------------------------------------------------------
  || FTP_PUT_SP
  ||  Send Routing extract file to Routing system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE ftp_put_sp(
    i_div      IN  VARCHAR2,
    i_file_nm  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_DAILY_ROUTING_PK.FTP_PUT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('OS Command Setup');
    l_cmd := 'cd /ftptrans;cp '
             || i_file_nm
             || ' /DivData/'
             || i_div
             || '/Paragon/Import-Export;zip -m -9 "'
             || i_file_nm
             || '.zip" "'
             || i_file_nm
             || '";mv "'
             || i_file_nm
             || '.zip" /ftptrans/transmitted_files';
    logs.dbg('Process Command' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.dbg('OS Result' || cnst.newline_char || l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ftp_put_sp;

  /*
  ||-----------------------------------------------------------------------------
  || FTP_GET_SP
  ||  Send Routing import file from Routing system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE ftp_get_sp(
    i_div             IN  VARCHAR2,
    i_remote_file_nm  IN  VARCHAR2,
    i_local_file_nm   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_DAILY_ROUTING_PK.FTP_GET_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RemoteFileNm', i_remote_file_nm);
    logs.add_parm(lar_parm, 'LocalFileNm', i_local_file_nm);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('OS Command Setup');
    l_cmd := 'cd /ftptrans;cp -T /DivData/'
             || i_div
             || '/Paragon/Import-Export/"'
             || i_remote_file_nm
             || '" "'
             || i_local_file_nm
             || '";chmod 666 "'
             || i_local_file_nm
             || '"';
    logs.dbg('Process Command' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.dbg('OS Result' || cnst.newline_char || l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ftp_get_sp;

  /*
  ||-----------------------------------------------------------------------------
  || PARSE_SP
  ||  Parse row from Routing system import file.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE parse_sp(
    i_buffer    IN      VARCHAR2,
    o_r_import  OUT     g_rt_import
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.PARSE_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Buffer', i_buffer);
    logs.dbg('ENTRY', lar_parm);
    o_r_import.div := SUBSTR(i_buffer, 1, 2);
    o_r_import.llr_dt := TO_DATE(SUBSTR(i_buffer, 3, 8), 'YYYYMMDD');
    o_r_import.depart_ts := TO_DATE(SUBSTR(i_buffer, 11, 12), 'YYYYMMDDHH24MI');
    o_r_import.eta_ts := TO_DATE(SUBSTR(i_buffer, 23, 12), 'YYYYMMDDHH24MI');
    o_r_import.load_num := LPAD(TRIM(SUBSTR(i_buffer, 35, 4)), 4, '0');
    o_r_import.stop_num := TO_NUMBER(SUBSTR(i_buffer, 39, 2));
    o_r_import.mcl_cust := SUBSTR(i_buffer, 41, 6);
    o_r_import.new_load := LPAD(TRIM(SUBSTR(i_buffer, 47, 4)), 4, '0');
    o_r_import.new_stop := TO_NUMBER(SUBSTR(i_buffer, 51, 2));
    o_r_import.new_depart_ts := TO_DATE(SUBSTR(i_buffer, 53, 12), 'YYYYMMDDHH24MI');
    o_r_import.new_eta_ts := TO_DATE(SUBSTR(i_buffer, 65, 12), 'YYYYMMDDHH24MI');
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN VALUE_ERROR THEN
      excp.throw(-20001, 'Invalid File Format');
  END parse_sp;

  /*
  ||-----------------------------------------------------------------------------
  || ARCHIVE_FILE_SP
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE archive_file_sp(
    i_div_part  IN  NUMBER,
    i_file_nm   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_DAILY_ROUTING_PK.ARCHIVE_FILE_SP';
    lar_parm             logs.tar_parm;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.dbg('ENTRY', lar_parm);
    l_appl_srvr := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_appl_srvr);
    l_cmd := 'cd /ftptrans;zip -m -9 '
             || i_file_nm
             || '.zip '
             || i_file_nm
             || ';mv '
             || i_file_nm
             || '.zip transmitted_files';
    logs.dbg('Process Command' || cnst.newline_char || l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.dbg('OS Result' || cnst.newline_char || l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END archive_file_sp;

  /*
  ||-----------------------------------------------------------------------------
  || PRCS_IMPORT_FILE_SP
  ||  Load data from Routing system import file.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE prcs_import_file_sp(
    i_div          IN      VARCHAR2,
    i_file_nm      IN      VARCHAR2,
    i_create_ts    IN      DATE,
    i_user_id      IN      VARCHAR2,
    o_rte_grp_num  OUT     NUMBER
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm      := 'OP_DAILY_ROUTING_PK.PRCS_IMPORT_FILE_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_r_file_handle        UTL_FILE.file_type;
    l_c_file_dir  CONSTANT VARCHAR2(50)       := '/ftptrans';
    l_buffer               typ.t_maxvc2;
    l_r_import             g_rt_import;
    l_llr_dt_save          DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'FileNm', i_file_nm);
    logs.add_parm(lar_parm, 'CreateTS', i_create_ts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Open File');
    l_r_file_handle := UTL_FILE.fopen(l_c_file_dir, i_file_nm, 'r');
    <<read_loop>>
    LOOP
      BEGIN
        logs.dbg('Get Line');
        UTL_FILE.get_line(l_r_file_handle, l_buffer);
        logs.dbg('Parse');
        parse_sp(l_buffer, l_r_import);

        IF l_r_import.div <> i_div THEN
          excp.throw(-20002, 'Invalid Division (' || l_r_import.div || ') in Import File');
        END IF;   -- r_import.div <> i_div

        IF l_llr_dt_save IS NULL THEN
          l_llr_dt_save := l_r_import.llr_dt;
        ELSIF l_r_import.llr_dt <> l_llr_dt_save THEN
          excp.throw(-20003,
                     'Mixed LLR Dates (' || l_llr_dt_save || ',' || l_r_import.llr_dt || ') found in Import File'
                    );
        END IF;   -- v_llr_dt_save IS NULL

        IF o_rte_grp_num IS NULL THEN
          logs.dbg('Add Route Grouping');

          INSERT INTO rte_grp_rt2g
                      (rte_grp_num, div_part, rte_grp, create_dt
                      )
               VALUES (rte_grp_num_seq.NEXTVAL, l_div_part, 'DLY_RTE_IMPORT', i_create_ts
                      )
            RETURNING rte_grp_num
                 INTO o_rte_grp_num;
        END IF;   -- p_rte_grp_num IS NULL

        logs.dbg('Add entry to CUST_AUTO_RTE_RT2C');

        INSERT INTO cust_auto_rte_rt2c
                    (rte_grp_num, cust_id, llr_dt, load_num, depart_ts, stop_num, eta_ts, new_load, new_depart_ts,
                     new_stop, new_eta_ts, stat_cd, last_chg_ts, user_id, err_msg, div_part)
          SELECT o_rte_grp_num, cx.custb, l_r_import.llr_dt, l_r_import.load_num, l_r_import.depart_ts,
                 l_r_import.stop_num, l_r_import.eta_ts, l_r_import.new_load, l_r_import.new_depart_ts,
                 l_r_import.new_stop, l_r_import.new_eta_ts,
                 (CASE
                    WHEN(    l_r_import.load_num = l_r_import.new_load
                         AND l_r_import.depart_ts = l_r_import.new_depart_ts
                         AND l_r_import.stop_num = l_r_import.new_stop
                         AND l_r_import.eta_ts = l_r_import.new_eta_ts
                        ) THEN 'NCH'
                    WHEN EXISTS(SELECT 1
                                  FROM cust_rte_ovrrd_rt3c cro
                                 WHERE cro.div_part = cx.div_part
                                   AND cro.cust_id = cx.custb
                                   AND cro.llr_dt = l_r_import.llr_dt
                                   AND cro.load_num = l_r_import.new_load
                                   AND cro.depart_ts = l_r_import.new_depart_ts
                                   AND cro.stop_num = l_r_import.new_stop
                                   AND cro.eta_ts = l_r_import.new_eta_ts) THEN 'CAN'
                    ELSE 'IMP'
                  END
                 ),
                 i_create_ts, i_user_id,
                 (CASE
                    WHEN(    l_r_import.load_num = l_r_import.new_load
                         AND l_r_import.depart_ts = l_r_import.new_depart_ts
                         AND l_r_import.stop_num = l_r_import.new_stop
                         AND l_r_import.eta_ts = l_r_import.new_eta_ts
                        ) THEN NULL
                    WHEN EXISTS(SELECT 1
                                  FROM cust_rte_ovrrd_rt3c cro
                                 WHERE cro.div_part = cx.div_part
                                   AND cro.cust_id = cx.custb
                                   AND cro.llr_dt = l_r_import.llr_dt
                                   AND cro.load_num = l_r_import.new_load
                                   AND cro.depart_ts = l_r_import.new_depart_ts
                                   AND cro.stop_num = l_r_import.new_stop
                                   AND cro.eta_ts = l_r_import.new_eta_ts) THEN 'Matching Override Exists!'
                  END
                 ),
                 cx.div_part
            FROM mclp020b cx
           WHERE cx.div_part = l_div_part
             AND cx.mccusb = l_r_import.mcl_cust;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          EXIT read_loop;
      END;
    END LOOP read_loop;
    logs.dbg('Close File');
    UTL_FILE.fclose(l_r_file_handle);
    logs.dbg('Archive File');
    archive_file_sp(l_div_part, i_file_nm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN g_e_parse_error OR g_e_invalid_div OR g_e_mixed_llr_dates OR DUP_VAL_ON_INDEX THEN
      IF UTL_FILE.is_open(l_r_file_handle) THEN
        UTL_FILE.fclose(l_r_file_handle);
      END IF;   -- UTL_FILE.is_open(l_r_file_handle)

      RAISE;
    WHEN OTHERS THEN
      IF UTL_FILE.is_open(l_r_file_handle) THEN
        UTL_FILE.fclose(l_r_file_handle);
      END IF;   -- UTL_FILE.is_open(l_r_file_handle)

      logs.err(lar_parm);
  END prcs_import_file_sp;

  /*
  ||-----------------------------------------------------------------------------
  || VALIDATE_SP
  ||  Validate imported data loaded to CUST_AUTO_RTE_RT2C.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 04/12/16 | rhalpai | Change to use div_part input parm. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE validate_sp(
    i_div_part     IN  NUMBER,
    i_rte_grp_num  IN  NUMBER,
    i_stat_cd      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.VALIDATE_SP';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'StatCd', i_stat_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get LLR');

    SELECT car.llr_dt
      INTO l_llr_dt
      FROM cust_auto_rte_rt2c car
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND ROWNUM = 1;

    logs.dbg('Reset for New Load does not exist');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = i_stat_cd,
           car.err_msg = NULL
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = 'ERR'
       AND car.err_msg = 'New Load does not exist!'
       AND EXISTS(SELECT 1
                    FROM mclp120c ld
                   WHERE ld.div_part = car.div_part
                     AND ld.loadc = car.new_load);

    logs.dbg('Reset for Multiple New Departures for New Load');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = i_stat_cd,
           car.err_msg = NULL
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = 'ERR'
       AND car.err_msg = 'Multiple New Departures for New Load!'
       AND NOT EXISTS(SELECT 1
                        FROM cust_auto_rte_rt2c car2
                       WHERE car2.div_part = car.div_part
                         AND car2.llr_dt = car.llr_dt
                         AND car2.new_load = car.new_load
                         AND (   car2.stat_cd = 'CMP'
                              OR (    car2.rte_grp_num = car.rte_grp_num
                                  AND car2.cust_id = car.cust_id)
                             )
                         AND car2.new_depart_ts <> car.new_depart_ts);

    logs.dbg('Reset for Another Cust Order found on New Load/Stop');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = i_stat_cd,
           car.err_msg = NULL
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = 'ERR'
       AND car.err_msg = 'Another Cust Order found on New Load/Stop!'
       AND NOT EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = car.div_part
                         AND ld.llr_dt = l_llr_dt
                         AND ld.load_num = car.new_load
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = car.new_stop
                         AND se.cust_id <> car.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata IN('P', 'R', 'A'))
       AND NOT EXISTS(SELECT 1
                        FROM cust_auto_rte_rt2c car2
                       WHERE car2.div_part = car.div_part
                         AND car2.rte_grp_num = i_rte_grp_num
                         AND car2.stat_cd = i_stat_cd
                         AND car2.new_load = car.new_load
                         AND car2.new_stop = car.new_stop
                         AND car2.cust_id <> car.cust_id)
       AND NOT EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = car.div_part
                         AND ld.llr_dt = l_llr_dt
                         AND ld.load_num = car.new_load
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = car.new_stop
                         AND se.cust_id <> car.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata = 'O'
                         AND NOT EXISTS(SELECT 1
                                          FROM cust_auto_rte_rt2c car2
                                         WHERE car2.div_part = se.div_part
                                           AND car2.rte_grp_num = i_rte_grp_num
                                           AND car2.stat_cd = i_stat_cd
                                           AND car2.cust_id = se.cust_id
                                           AND (   car2.new_load <> ld.load_num
                                                OR car2.new_stop <> se.stop_num)));

    logs.dbg('Reset for ETA Out of Sequence');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = i_stat_cd,
           car.err_msg = NULL
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = 'ERR'
       AND car.err_msg = 'ETA Out of Sequence!'
       AND NOT EXISTS(SELECT 1
                        FROM cust_auto_rte_rt2c car2
                       WHERE car2.div_part = car.div_part
                         AND car2.llr_dt = car.llr_dt
                         AND car2.new_load = car.new_load
                         AND car2.stat_cd IN(i_stat_cd, 'CMP')
                         AND (   (    car2.new_stop < car.new_stop
                                  AND car2.new_eta_ts < car.new_eta_ts)
                              OR (    car2.new_stop > car.new_stop
                                  AND car2.new_eta_ts > car.new_eta_ts)
                             ));

    logs.dbg('Check for Matching Override');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'CAN',
           car.err_msg = 'Matching Override Exists!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM cust_rte_ovrrd_rt3c cro
                   WHERE cro.div_part = car.div_part
                     AND cro.cust_id = car.cust_id
                     AND cro.llr_dt = car.llr_dt
                     AND cro.load_num = car.new_load
                     AND cro.depart_ts = car.new_depart_ts
                     AND cro.stop_num = car.new_stop
                     AND cro.eta_ts = car.new_eta_ts);

    logs.dbg('Check Order found in billed status for Cust/LLRDt');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'CAN',
           car.err_msg = 'Order found in billed status for Cust/LLRDt!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
                   WHERE ld.div_part = car.div_part
                     AND ld.llr_dt = l_llr_dt
                     AND se.div_part = ld.div_part
                     AND se.load_depart_sid = ld.load_depart_sid
                     AND se.cust_id = car.cust_id
                     AND a.div_part = se.div_part
                     AND a.load_depart_sid = se.load_depart_sid
                     AND a.custa = se.cust_id
                     AND b.div_part = a.div_part
                     AND b.ordnob = a.ordnoa
                     AND b.statb NOT IN('O', 'I', 'S', 'C'));

    logs.dbg('Check No open Order found for Cust/LLRDt on Old Load/Stop/ETA');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'CAN',
           car.err_msg = 'No open Order found for Cust/LLRDt on Old Load/Stop/ETA!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND NOT EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = car.div_part
                         AND ld.llr_dt = l_llr_dt
                         AND ld.load_num = car.load_num
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.cust_id = car.cust_id
                         AND se.stop_num = car.stop_num
                         AND se.eta_ts = car.eta_ts
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata = 'O');

    logs.dbg('Check for Change Matches Original');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'Change Matches Original!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND car.new_load = car.load_num
       AND car.new_stop = car.stop_num
       AND car.new_depart_ts = car.depart_ts
       AND car.new_eta_ts = car.eta_ts;

    logs.dbg('Check New Load does not exist');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'New Load does not exist!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND NOT EXISTS(SELECT 1
                        FROM mclp120c ld
                       WHERE ld.div_part = car.div_part
                         AND ld.loadc = car.new_load);

    logs.dbg('Check Another Cust Order found on New Load/Stop');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'Another Cust Order found on New Load/Stop!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND (   EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = car.div_part
                         AND ld.llr_dt = l_llr_dt
                         AND ld.load_num = car.new_load
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = car.new_stop
                         AND se.cust_id <> car.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata IN('P', 'R', 'A'))
            OR EXISTS(SELECT 1
                        FROM cust_auto_rte_rt2c car2
                       WHERE car2.div_part = car.div_part
                         AND car2.rte_grp_num = i_rte_grp_num
                         AND car2.stat_cd = i_stat_cd
                         AND car2.new_load = car.new_load
                         AND car2.new_stop = car.new_stop
                         AND car2.cust_id <> car.cust_id)
            OR EXISTS(SELECT 1
                        FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                       WHERE ld.div_part = i_div_part
                         AND ld.llr_dt = l_llr_dt
                         AND ld.load_num = car.new_load
                         AND se.div_part = ld.div_part
                         AND se.load_depart_sid = ld.load_depart_sid
                         AND se.stop_num = car.new_stop
                         AND se.cust_id <> car.cust_id
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata = 'O'
                         AND NOT EXISTS(SELECT 1
                                          FROM cust_auto_rte_rt2c car2
                                         WHERE car2.div_part = se.div_part
                                           AND car2.rte_grp_num = i_rte_grp_num
                                           AND car2.stat_cd = i_stat_cd
                                           AND car2.cust_id = se.cust_id
                                           AND (   car2.new_load <> ld.load_num
                                                OR car2.new_stop <> se.stop_num)))
           );

    logs.dbg('Check New ETA Before New Departure');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'New ETA is before New Departure!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND car.new_depart_ts > car.new_eta_ts;

    logs.dbg('Check New Departure Before LLR');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'New Departure Before LLR!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND car.llr_dt > car.new_depart_ts;

    logs.dbg('Check Multiple New Departures for New Load');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'Multiple New Departures for New Load!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM cust_auto_rte_rt2c car2
                   WHERE car2.div_part = car.div_part
                     AND car2.llr_dt = car.llr_dt
                     AND car2.new_load = car.new_load
                     AND car2.stat_cd IN(i_stat_cd, 'CMP')
                     AND car2.new_depart_ts <> car.new_depart_ts);

    logs.dbg('Check New Departure Greater than 14 days from LLRDate');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'New Departure Greater than 14 days from LLRDate!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND TRUNC(car.new_depart_ts) > car.llr_dt + 14;

    logs.dbg('Check New ETA Greater than 21 days from LLRDate');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'New ETA Greater than 21 days from LLRDate!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND TRUNC(car.new_eta_ts) > car.llr_dt + 21;

    logs.dbg('Check ETA Out of Sequence');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'ERR',
           car.err_msg = 'ETA Out of Sequence!'
     WHERE car.div_part = i_div_part
       AND car.rte_grp_num = i_rte_grp_num
       AND car.stat_cd = i_stat_cd
       AND EXISTS(SELECT 1
                    FROM cust_auto_rte_rt2c car2
                   WHERE car2.div_part = car.div_part
                     AND car2.llr_dt = car.llr_dt
                     AND car2.new_load = car.new_load
                     AND car2.stat_cd IN(i_stat_cd, 'CMP')
                     AND (   (    car2.new_stop < car.new_stop
                              AND car2.new_eta_ts < car.new_eta_ts)
                          OR (    car2.new_stop > car.new_stop
                              AND car2.new_eta_ts > car.new_eta_ts)
                         ));

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_sp;

  /*
  ||-----------------------------------------------------------------------------
  || UPD_STAT_SP
  ||  Assign new status to matching routing entries.
  ||
  ||  Cancel status can be assinged to matching entries in Import or Error status.
  ||  For Move, Work status can be assinged to matching entries in Import status.
  ||  For Move, Complete status can be assinged to entries in Work status.
  ||  For Undo, Work status can be assinged to matching entries in Complete status.
  ||  For Undo, Complete status can be assinged to entries in Work status.
  ||
  ||  CustList:
  ||  CustId~CustId
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 04/12/16 | rhalpai | Change to use div_part input parm. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE upd_stat_sp(
    i_div_part     IN  NUMBER,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  DATE,
    i_cust_list    IN  VARCHAR2,
    i_actn_cd      IN  VARCHAR2,
    i_new_stat_cd  IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.UPD_STAT_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_t_custs             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'NewStatCd', i_new_stat_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_cust_list IS NOT NULL THEN
      logs.dbg('Parse');
      l_t_custs := str.parse_list(i_cust_list, op_const_pk.field_delimiter);
      logs.dbg('Set Cancel Status');

      UPDATE cust_auto_rte_rt2c car
         SET car.stat_cd = i_new_stat_cd,
             car.last_chg_ts = l_c_sysdate,
             car.user_id = i_user_id
       WHERE car.div_part = i_div_part
         AND car.rte_grp_num = i_rte_grp_num
         AND car.llr_dt = i_llr_dt
         AND car.cust_id IN(SELECT t.column_value
                              FROM TABLE(CAST(l_t_custs AS type_stab)) t)
         AND (   (    i_actn_cd = g_c_actn_cancl
                  AND car.stat_cd IN('IMP', 'ERR'))
              OR (    i_actn_cd = g_c_actn_move
                  AND car.stat_cd = DECODE(i_new_stat_cd, 'WRK', 'IMP', 'CMP', 'WRK'))
              OR (    i_actn_cd = g_c_actn_undo
                  AND car.stat_cd = DECODE(i_new_stat_cd, 'WRK', 'CMP', 'CAN', 'WRK'))
             );
    END IF;   -- i_cust_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_stat_sp;

  /*
  ||-----------------------------------------------------------------------------
  || MOVE_ORDS_SP
  ||  Apply routing to matching orders and log changes.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/10/13 | rhalpai | Change logic to avoid ORA-3113 end-of-file on
  ||                    | communication channel. IM-118384
  || 04/12/16 | rhalpai | Change to use div_part input parm. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE move_ords_sp(
    i_div_part     IN  NUMBER,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  DATE,
    i_actn_cd      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_DAILY_ROUTING_PK.MOVE_ORDS_SP';
    lar_parm             logs.tar_parm;

    TYPE l_rt_load_ord IS RECORD(
      cust_id       sysp200c.acnoc%TYPE,
      llr_ts        DATE,
      load_num      mclp120c.loadc%TYPE,
      depart_ts     DATE,
      stop_num      NUMBER,
      eta_ts        DATE,
      t_ord_nums    type_ntab,
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
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Table of Order Load Info for Moves');

    SELECT   se.cust_id,
             ld.llr_ts,
             ld.load_num,
             ld.depart_ts,
             se.stop_num,
             se.eta_ts,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = se.div_part
                              AND a.load_depart_sid = se.load_depart_sid
                              AND a.custa = se.cust_id
                              AND a.stata IN('O', 'S')
                          ) AS type_ntab
                 ) AS ord_nums,
             DECODE(i_actn_cd, g_c_actn_move, car.new_load, g_c_actn_undo, car.load_num) AS new_load_num,
             DECODE(i_actn_cd, g_c_actn_move, car.new_stop, g_c_actn_undo, car.stop_num) AS new_stop_num,
             DECODE(i_actn_cd, g_c_actn_move, car.new_eta_ts, g_c_actn_undo, car.eta_ts) AS new_eta_ts
    BULK COLLECT INTO l_t_load_ords
        FROM cust_auto_rte_rt2c car, load_depart_op1f ld, stop_eta_op1g se
       WHERE car.div_part = i_div_part
         AND car.rte_grp_num = i_rte_grp_num
         AND car.llr_dt = i_llr_dt
         AND car.stat_cd = 'WRK'
         AND ld.div_part = car.div_part
         AND ld.llr_dt = car.llr_dt
         AND ld.load_num = DECODE(i_actn_cd, g_c_actn_move, car.load_num, g_c_actn_undo, car.new_load)
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = car.cust_id
         AND se.stop_num = DECODE(i_actn_cd, g_c_actn_move, car.stop_num, g_c_actn_undo, car.new_stop)
         AND se.eta_ts = DECODE(i_actn_cd, g_c_actn_move, car.eta_ts, g_c_actn_undo, car.new_eta_ts)
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = se.div_part
                       AND a.load_depart_sid = se.load_depart_sid
                       AND a.custa = se.cust_id
                       AND a.stata IN('O', 'S'))
    ORDER BY ld.llr_ts, new_load_num, se.load_depart_sid, se.cust_id;

    IF l_t_load_ords.COUNT > 0 THEN
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        IF NOT(    l_t_load_ords(i).llr_ts = l_llr_ts_save
               AND l_t_load_ords(i).new_load_num = l_load_num_save) THEN
          l_llr_ts_save := l_t_load_ords(i).llr_ts;
          l_load_num_save := l_t_load_ords(i).new_load_num;
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part,
                                                                   l_t_load_ords(i).llr_ts,
                                                                   l_t_load_ords(i).new_load_num
                                                                  );
        END IF;   -- NOT (l_t_load_ords(i).llr_ts = l_llr_ts_save AND l_t_load_ords(i).new_load_num = l_load_num_save)

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
                                      'DLY_RTE',
                                      i_user_id,
                                      l_t_load_ords(i).t_ord_nums
                                     );
      END LOOP;
    END IF;   -- l_t_load_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_ords_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || FILE_LIST_FN
  ||  Returns a cursor of text files from the Routing system.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  FUNCTION file_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_DAILY_ROUTING_PK.FILE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                VARCHAR2(500);
    l_file_list          typ.t_maxvc2;
    l_t_files            type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('OS Command Setup');
    l_cmd := 'cd /DivData/' || i_div || '/Paragon/Import-Export;ls -1 *.txt *.TXT 2>exit | egrep -v "ls:"';
    logs.dbg(l_cmd);
    logs.dbg('Process OS Command');
    l_file_list := TRIM(REPLACE(oscmd_fn(l_cmd, l_appl_srvr),
                                'Only McLane Authorized users are permitted to login to the McLane Network.'
                                || cnst.newline_char
                               )
                       );
    logs.dbg(l_file_list);

    IF l_file_list IS NOT NULL THEN
      logs.dbg('Parse');
      l_t_files := str.parse_list(l_file_list, cnst.newline_char);
    END IF;   -- l_file_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   t.column_value AS file_nm
           FROM TABLE(CAST(l_t_files AS type_stab)) t
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END file_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || AUDIT_LIST_SP
  ||  Returns cursor of Daily Routing Audit Info for LLRDt.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 10/28/13 | rhalpai | Change logic to include Load as part of primary key of
  ||                    | CUST_RTE_OVRRD_RT3C. IM-123463
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  ||----------------------------------------------------------------------------
  */
  PROCEDURE audit_list_sp(
    i_div     IN      VARCHAR2,
    i_llr_dt  IN      VARCHAR2,
    o_cur     OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.AUDIT_LIST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Open Cursor');

    OPEN o_cur
     FOR
       SELECT   x.cust_id, cx.mccusb AS mcl_cust, c.namec AS cust_nm, x.old_load,
                TO_CHAR(x.old_depart_ts, 'YYYY-MM-DD HH24:MI') AS old_depart_ts,
                (CASE
                   WHEN x.old_stop < 10 THEN '0'
                 END) || x.old_stop AS old_stop, TO_CHAR(x.old_eta_ts, 'YYYY-MM-DD HH24:MI') AS old_eta_ts, x.new_load,
                TO_CHAR(x.new_depart_ts, 'YYYY-MM-DD HH24:MI') AS new_depart_ts,
                (CASE
                   WHEN x.new_stop < 10 THEN '0'
                 END) || x.new_stop AS new_stop, TO_CHAR(x.new_eta_ts, 'YYYY-MM-DD HH24:MI') AS new_eta_ts,
                md.delwid AS delv_wndw,
                (CASE
                   WHEN x.new_eta_ts NOT BETWEEN TO_DATE(TO_CHAR(TRUNC(x.new_eta_ts), 'YYYYMMDD')
                                                         || SUBSTR(md.delwid, 1, 4),
                                                         'YYYYMMDDHH24MI'
                                                        )
                                             AND TO_DATE(TO_CHAR(TRUNC(x.new_eta_ts), 'YYYYMMDD')
                                                         || SUBSTR(md.delwid, -4),
                                                         'YYYYMMDDHH24MI'
                                                        ) THEN 'Y'
                 END
                ) AS out_wndw,
                c.cnphnc AS phone
           FROM (SELECT DISTINCT cro.cust_id,
                                 FIRST_VALUE(car.load_num) OVER(PARTITION BY car.cust_id ORDER BY rg.create_dt)
                                                                                                           AS old_load,
                                 FIRST_VALUE(car.depart_ts) OVER(PARTITION BY car.cust_id ORDER BY rg.create_dt)
                                                                                                      AS old_depart_ts,
                                 FIRST_VALUE(car.stop_num) OVER(PARTITION BY car.cust_id ORDER BY rg.create_dt)
                                                                                                           AS old_stop,
                                 FIRST_VALUE(car.eta_ts) OVER(PARTITION BY car.cust_id ORDER BY rg.create_dt)
                                                                                                         AS old_eta_ts,
                                 cro.load_num AS new_load, cro.depart_ts AS new_depart_ts, cro.stop_num AS new_stop,
                                 cro.eta_ts AS new_eta_ts
                            FROM cust_rte_ovrrd_rt3c cro, cust_auto_rte_rt2c car, rte_grp_rt2g rg
                           WHERE cro.div_part = l_div_part
                             AND cro.llr_dt = l_llr_dt
                             AND car.div_part = cro.div_part
                             AND car.cust_id = cro.cust_id
                             AND car.llr_dt = cro.llr_dt
                             AND car.new_load = cro.load_num
                             AND rg.div_part = car.div_part
                             AND rg.rte_grp_num = car.rte_grp_num
                             AND car.stat_cd = 'CMP') x,
                sysp200c c, mclp020b cx, mclp040d md
          WHERE c.div_part = l_div_part
            AND c.acnoc = x.cust_id
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
            AND md.div_part(+) = l_div_part
            AND md.custd(+) = x.cust_id
            AND md.loadd(+) = x.old_load
       ORDER BY x.new_load, x.new_stop DESC;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END audit_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_SUM_SP
  ||  Returns cursor of Load Summary Info for unbilled Loads on LLRDt.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 06/15/11 | rhalpai | Changed cursor to handle NULL order source as the
  ||                    | only non-bypassed order source. PIR9348
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers.
  ||                    | Change to use common div_part_fn. PIR14660
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||----------------------------------------------------------------------------
  */
  PROCEDURE load_sum_sp(
    i_div     IN      VARCHAR2,
    i_llr_dt  IN      VARCHAR2,
    o_cur     OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.LOAD_SUM_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_t_xloads           type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN o_cur
     FOR
       SELECT   o.load_num, l.destc AS destination,
                TO_CHAR(NVL(ROUND(SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube)), 1), 0),
                        'FM999999990.0'
                       ) AS ttl_cube,
                NVL(CEIL(SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube))
                         / DECODE(l.accubc, 0, NULL, l.accubc)
                         * 100
                        ),
                    0
                   ) AS cube_pct_full,
                TO_CHAR(NVL(ROUND(SUM(o.prod_cube), 1), 0), 'FM999999990.0') AS prod_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_wt), 1), 0), 'FM999999990.0') AS prod_wt,
                NVL(CEIL(SUM(o.prod_wt) / DECODE(l.acwgtc, 0, NULL, l.acwgtc) * 100), 0) AS wt_pct_full,
                COUNT(DISTINCT(o.stop_num)) AS stop_cnt,
                TO_CHAR(NVL(ROUND(SUM(o.dist_cube), 1), 0), 'FM999999990.0') AS dist_cube,
                TO_CHAR(NVL(ROUND(SUM(o.dist_wt), 1), 0), 'FM999999990.0') AS dist_wt,
                NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.outer_cube), 1), 0), 'FM999999990.0') AS tote_cube
           FROM (SELECT *
                   FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                                l_div_part,
                                CURSOR(
                                  SELECT *
                                    FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, 'OPEN')) oh
                                   WHERE oh.ord_stat = 'O'
                                     AND NOT EXISTS(SELECT 1
                                                      FROM ordp100a oa
                                                     WHERE oa.div_part = l_div_part
                                                       AND oa.load_depart_sid = oh.load_depart_sid
                                                       AND oa.stata IN('P', 'R', 'A'))
                                ),
                                'OPEN'
                                          )
                             ) od
                  WHERE od.prod_cube > 0
                ) o,
                mclp120c l
                    WHERE l.div_part = l_div_part
            AND l.loadc = o.load_num
                      AND l.loadc NOT IN(SELECT t.column_value
                                           FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
       GROUP BY o.load_num, l.destc, l.accubc, l.acwgtc
       ORDER BY o.load_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END load_sum_sp;

  /*
  ||-----------------------------------------------------------------------------
  || STOP_SUM_SP
  ||  Return cursor of stops for LLR/Load with balance info.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 06/15/11 | rhalpai | Changed cursor to handle NULL order source as the
  ||                    | only non-bypassed order source. PIR9348
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers.
  ||                    | Change to use common div_part_fn. PIR14660
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE stop_sum_sp(
    i_div       IN      VARCHAR2,
    i_llr_dt    IN      VARCHAR2,
    i_load_num  IN      VARCHAR2,
    o_cur       OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.STOP_SUM_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Open cursor');

    OPEN o_cur
     FOR
       SELECT   (CASE
                   WHEN o.stop_num < 10 THEN '0'
                 END) || o.stop_num AS stp, cx.mccusb AS mcl_cust, c.namec AS cust_name, c.shpctc AS city,
                c.shpstc AS st, c.shpzpc AS zip, op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone,
                TO_CHAR(o.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta, NVL(SUM(o.case_cnt), 0) AS cases,
                NVL(SUM(o.tote_cnt), 0) AS tote_cnt,
                TO_CHAR(NVL(ROUND(SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube)), 1), 0),
                        'FM999999990.0'
                       ) AS ttl_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_cube), 1), 0), 'FM999999990.0') AS prod_cube,
                TO_CHAR(NVL(ROUND(SUM(o.prod_wt), 1), 0), 'FM999999990.0') AS prod_wt,
                NVL(ROUND(100
                          -(SUM(DECODE(o.tote_cnt, NULL, NULL, o.prod_cube))
                            / SUM(DECODE(o.tote_cnt * o.inner_cube, 0, NULL, o.tote_cnt * o.inner_cube))
                            * 100
                           )
                         ),
                    0
                   ) AS tote_air_pct,
                TO_CHAR(NVL(ROUND(SUM(o.dist_cube), 1), 0), 'FM999999990.0') AS dist_cube,
                TO_CHAR(NVL(ROUND(SUM(o.dist_wt), 1), 0), 'FM999999990.0') AS dist_wt, o.cust_id,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.outer_cube), 1), 0), 'FM999999990.0') AS tote_cube,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.inner_cube), 1), 0), 'FM999999990.0') AS tote_inner_cube,
                TO_CHAR(NVL(ROUND(SUM(o.tote_cnt * o.inner_cube - o.prod_cube), 1), 0), 'FM999999990.0') AS tote_air_cube,
                c.shad1c AS addr_ln1, c.shad2c AS addr_ln2
           FROM (SELECT *
                   FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                                l_div_part,
                                CURSOR(
                                  SELECT *
                                    FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, 'OPEN', i_load_num)) oh
                                   WHERE oh.ord_stat = 'O'
                                ),
                                'OPEN'
                                          )
                             ) od
                  WHERE od.prod_cube > 0
                ) o, sysp200c c, mclp020b cx
          WHERE c.div_part = l_div_part
            AND c.acnoc = o.cust_id
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
       GROUP BY o.stop_num, cx.mccusb, o.cust_id, c.namec, c.shpctc, c.shpstc, c.shpzpc, c.cnphnc, c.shad1c, c.shad2c, o.eta_ts
       ORDER BY o.stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_sum_sp;

  /*
  ||-----------------------------------------------------------------------------
  || TMW_EXPORT_SP
  ||  Create and send TMW Routing extract file to Routing system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 08/16/18 | rhalpai | Original. PIR17950
  || 12/08/18 | rhalpai | Add ProdCube to cursor. PIR18901
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr process. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE tmw_export_sp(
    i_div     IN  VARCHAR2,
    i_llr_dt  IN  VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_DAILY_ROUTING_PK.TMW_EXPORT_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_llr_dt               DATE;
    l_file_nm              VARCHAR2(60);
    l_t_rpt_lns            typ.tas_maxvc2;
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/ftptrans';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_file_nm := i_div || '_DLY_RTE_TMW_EXPORT' || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') || '.csv';
    logs.dbg('Get Report Lines');

    SELECT z.extr
    BULK COLLECT INTO l_t_rpt_lns
      FROM (SELECT 'CBR,OrderID,EQCode,OrderGroupID,DestinationID,ProductTypeID,EarliestDate,LatestDate,TrailerCompartment,ReferenceID,Weight,Cube,ProdCube,Piece,Rt,Seq,OwnerType,ParentID,ParentName,Chain ID,ExtStoreID,DynamicManifestMsg,OriginalRouteID,OriginalStopID,OrgDispatch,OrgETA'
                                                                                                                AS extr
              FROM DUAL
            UNION ALL
            SELECT y.extr
              FROM (SELECT   x.cust_id
                             || ','
                             || x.ord_num
                             || ','
                             || x.fin_div_cd
                             || ','
                             || TO_CHAR(x.llr_dt, 'YYYYMMDD')
                             || ','
                             || x.cust_id
                             || ','
                             || x.categ
                             || ','
                             || ','
                             || ','
                             || DECODE(x.categ, 'FRZ', 1, 'REF', 2, 'DRY', 3)
                             || ','
                             || '0'
                             || ','
                             || x.prod_wt
                             || ','
                             || x.prod_vol
                             || ','
                             || x.prod_cube
                             || ','
                             || x.qty
                             || ','
                             || x.load_num
                             || ','
                             || DENSE_RANK() OVER(PARTITION BY x.fin_div_cd, x.llr_dt, x.load_num ORDER BY x.stop_num DESC)
                             || ','
                             || ','
                             || ','
                             || ','
                             || LPAD(x.corp_cd, 3, '0')
                             || ','
                             || ','
                             || ','
                             || x.load_num
                             || ','
                             || DENSE_RANK() OVER(PARTITION BY x.fin_div_cd, x.llr_dt, x.load_num ORDER BY x.stop_num DESC)
                             || ','
                             || TO_CHAR(x.depart_ts, 'DY HH24:MI')
                             || ','
                             || TO_CHAR(x.eta_ts, 'DY HH24:MI') AS extr
                        FROM (SELECT   o.fin_div_cd, o.div_id, o.llr_dt, o.load_num, o.depart_ts, o.cust_id, o.corp_cd,
                                       o.stop_num, o.eta_ts, o.ord_num, o.categ, SUM(o.qty) AS qty,
                                       SUM(o.prod_wt) AS prod_wt,
                                       SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outerb)) AS prod_vol,
                                       SUM(o.prod_cube) AS prod_cube
                                  FROM (SELECT   d.fin_div_cd, d.div_id, ld.llr_dt, ld.load_num, ld.depart_ts,
                                                 oh.cust_id, cx.corpb AS corp_cd,
                                                 NVL(se.stop_num, 0) AS stop_num, se.eta_ts, oh.ord_num,
                                                 (CASE m.catg_typ_cd
                                                    WHEN 'K' THEN 'REF'
                                                    WHEN 'F' THEN 'FRZ'
                                                    ELSE 'DRY'
                                                  END
                                                 ) AS categ,
                                                 b.totctb, t.outerb, t.innerb, SUM(b.ordqtb) AS qty,
                                                 DECODE(t.pccntb,
                                                        'Y', CEIL(SUM(b.ordqtb) / t.totcnb),
                                                        'N', CEIL(SUM(NVL(e.cubee, .01) * b.ordqtb)
                                                                  / DECODE(t.innerb,
                                                                           NULL, .000001,
                                                                           0, .000001,
                                                                           t.innerb
                                                                          )
                                                                 )
                                                       ) AS tote_cnt,
                                                 SUM(NVL(e.cubee, .01) * b.ordqtb) AS prod_cube,
                                                 SUM(NVL(e.wghte, .01) * b.ordqtb) AS prod_wt
                                            FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, 'OPEN')) oh,
                                                 div_mstr_di1d d, load_depart_op1f ld, mclp020b cx, stop_eta_op1g se,
                                                 ordp120b b, sawp505e e, mclp200b t, mclp210c m
                                           WHERE d.div_id = i_div
                                             AND ld.div_part = d.div_part
                                             AND ld.load_depart_sid = oh.load_depart_sid
                                             AND cx.div_part = d.div_part
                                             AND cx.custb = oh.cust_id
                                             AND se.div_part(+) = l_div_part
                                             AND se.load_depart_sid(+) = oh.load_depart_sid
                                             AND se.cust_id(+) = oh.cust_id
                                             AND b.div_part = d.div_part
                                             AND b.ordnob = oh.ord_num
                                             AND b.excptn_sw = 'N'
                                             AND b.statb = 'O'
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
                                             AND t.div_part(+) = b.div_part
                                             AND t.totctb(+) = b.totctb
                                        GROUP BY d.fin_div_cd, d.div_id, ld.llr_dt, ld.load_num, ld.depart_ts, oh.cust_id,
                                                 cx.corpb, se.stop_num, se.eta_ts, oh.ord_num,
                                                 (CASE m.catg_typ_cd
                                                    WHEN 'K' THEN 'REF'
                                                    WHEN 'F' THEN 'FRZ'
                                                    ELSE 'DRY'
                                                  END), b.totctb, t.outerb, t.innerb, t.pccntb, t.totcnb) o
                              GROUP BY o.fin_div_cd, o.div_id, o.llr_dt, o.load_num, o.depart_ts, o.cust_id, o.corp_cd,
                                       o.stop_num, o.eta_ts, o.ord_num, o.categ) x
                    ORDER BY x.fin_div_cd, x.llr_dt, x.load_num, x.stop_num DESC, x.ord_num) y) z;

    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
    l_t_rpt_lns.DELETE;   -- release memory
    logs.dbg('FTP File to Remote and Archive');
    ftp_put_sp(i_div, l_file_nm);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tmw_export_sp;

  /*
  ||-----------------------------------------------------------------------------
  || EXPORT_SP
  ||  Create and send Routing extract file of selected loads to Routing system.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 09/19/11 | rhalpai | Change cursor to use current Load/Stop as OrigLoadStop
  ||                    | when assigned Load/Stop not found for BillDay.
  ||                    | IM-029285
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw. PIR11038
  || 04/12/16 | rhalpai | Add PO break for Container Tracking Customers.
  ||                    | Change to use common div_part_fn. Pass div_part to
  ||                    | CLEAN_EXPORT_LOADS_FN. PIR14660
  || 02/02/17 | rhalpai | Add logic to create CSV file. PIR16183
  || 02/28/17 | rhalpai | Change logic for CSV report to have Seq field start
  ||                    | over at 1 for each load. PIR16183
  || 08/16/18 | rhalpai | Add call to TMW_EXPORT_SP. PIR17950
  || 09/09/19 | rhalpai | Change CSV extract to rename column OrgComputerStop to MclaneStop
  ||                    | and remove Type,OrgETA2,OrgDay,OrgComputerStop2 columns. PIR19778
  || 01/10/22 | rhalpai | Add logic to exclude items with no available inventory. PIR21395
  || 06/01/23 | rhalpai | Change logic to use common LoadBalance OrdHdr/OrdDtl processes. PIR18901
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE export_sp(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_list  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_DAILY_ROUTING_PK.EXPORT_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_llr_dt               DATE;
    l_llr_num              NUMBER;
    l_llr_dy               VARCHAR2(3);
    l_llr_dt_char          VARCHAR2(8);
    l_base_file_nm         VARCHAR2(60);
    l_txt_file_nm          VARCHAR2(60);
    l_csv_file_nm          VARCHAR2(60);
    l_t_loads              type_stab;
    l_t_txt_rpt_lns        typ.tas_maxvc2;
    l_t_csv_rpt_lns        typ.tas_maxvc2;
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/ftptrans';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    l_llr_num := TO_DATE(i_llr_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    l_llr_dy := TO_CHAR(l_llr_dt, 'DY');
    l_llr_dt_char := TO_CHAR(l_llr_dt, 'YYYYMMDD');
    l_base_file_nm := i_div || '_DLY_RTE_EXPORT' || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
    l_txt_file_nm := l_base_file_nm || '.txt';
    l_csv_file_nm := l_base_file_nm || '.csv';
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Get Nested Table of Loads containing only Unbilled Ords');
    l_t_loads := clean_export_loads_fn(l_div_part, l_llr_num, i_load_list);

    IF l_t_loads.COUNT > 0 THEN
      logs.dbg('Get Report Lines');

      WITH o AS
           (SELECT   cx.mccusb AS mcl_cust, o.load_num, o.stop_num, o.depart_ts, o.eta_ts,
                     NVL((SELECT md.loadd || md.stopd
                            FROM mclp040d md, mclp120c ld
                           WHERE md.div_part = l_div_part
                             AND md.custd = o.cust_id
                             AND ld.div_part = md.div_part
                             AND ld.loadc = md.loadd
                             AND ld.llrcdc = l_llr_dy
                             AND ROWNUM = 1),
                         o.load_num || o.stop_num
                        ) AS orig_load_stop,
                     GREATEST(CEIL(SUM(o.prod_wt)), 1) AS orig_wt,
                     GREATEST(CEIL(SUM(DECODE(o.tote_cnt, NULL, o.prod_cube, o.tote_cnt * o.outer_cube))), 1) AS orig_cube
                FROM (SELECT *
                        FROM TABLE(op_load_balance_pk.ord_dtl_fn(
                                     l_div_part,
                                     CURSOR(
                                       SELECT oh.*
                                         FROM TABLE(op_load_balance_pk.ord_hdr_fn(l_div_part, l_llr_dt, 'OPEN')) oh,
                                              load_depart_op1f ld
                                        WHERE ld.div_part = l_div_part
                                          AND ld.load_depart_sid = oh.load_depart_sid
                                          AND ld.load_num MEMBER OF l_t_loads
                                     ),
                                     'OPEN'
                                               )
                                  ) od
                       WHERE od.prod_cube > 0
                     ) o, mclp020b cx
               WHERE cx.div_part = l_div_part
                 AND cx.custb = o.cust_id
            GROUP BY o.load_num, o.stop_num, cx.mccusb, o.cust_id, o.depart_ts, o.eta_ts),
           ox AS
           (SELECT o.mcl_cust, o.load_num, o.stop_num, o.depart_ts, o.eta_ts, o.orig_load_stop, o.orig_wt, o.orig_cube
              FROM o
            UNION ALL
            SELECT x.mcl_cust, x.load_num, x.stop_num, x.depart_ts,
                   NEXT_DAY(TO_DATE(TO_CHAR(TRUNC(x.depart_ts) - 1 +(x.wkoffd * 7), 'YYYYMMDD') || LPAD(x.etad, 4, '0'),
                                    'YYYYMMDDHH24MI'
                                   ),
                            x.dayrcd
                           ) AS eta_ts,
                   x.load_num || x.stop_num AS orig_load_stop, 1 AS orig_wt, 1 AS orig_cube
              FROM (SELECT cx.mccusb AS mcl_cust, c.loadc AS load_num, md.stopd AS stop_num,
                           NEXT_DAY(TO_DATE(TO_CHAR(l_llr_dt - 1 +(c.depwkc * 7), 'YYYYMMDD') || LPAD(c.deptmc, 4, '0'),
                                            'YYYYMMDDHH24MI'
                                           ),
                                    c.depdac
                                   ) AS depart_ts,
                           md.wkoffd, md.etad, md.dayrcd
                      FROM mclp120c c, mclp040d md, mclp020b cx
                     WHERE c.div_part = l_div_part
                       AND c.loadc IN(SELECT DISTINCT o.load_num
                                                 FROM o)
                       AND md.div_part = c.div_part
                       AND md.loadd = c.loadc
                       AND cx.div_part = md.div_part
                       AND cx.custb = md.custd
                       AND NOT EXISTS(SELECT 1
                                        FROM o
                                       WHERE o.mcl_cust = cx.mccusb
                                         AND o.load_num = md.loadd)) x),
           z AS
           (SELECT ox.mcl_cust, ox.load_num, ox.stop_num, ox.depart_ts, ox.eta_ts, ox.orig_load_stop, ox.orig_wt,
                   ox.orig_cube, ROW_NUMBER() OVER(PARTITION BY ox.load_num ORDER BY ox.stop_num DESC) AS seq
              FROM ox),
           zz AS
           (
             SELECT 'ID>>>>>>>>ROUTENO>NDATA03>TRACTOR>MEASURE1>MEASURE2>TDATA03>>TDATA02>>TFIXEDDAY>TFIXEDTIME>NDATA09>NDATA08>TDATA01>>>>>>>>>>>>>>>>>>>>>>>>>>>' AS txt,
                    'Account,Rt,OrgRt,Weight,Cube,OrgETA,OrgDispatch,MclaneStop,UniqueID,Seq' AS csv
               FROM DUAL
             UNION ALL
             SELECT RPAD(z.mcl_cust, 10)
                    || RPAD(z.load_num, 8)
                    || rpad_fn(SUBSTR(z.orig_load_stop, 1, 4), 8)
                    || '48TEAM  '
                    || lpad_fn(z.orig_wt, 9, '0')
                    || lpad_fn(z.orig_cube, 9, '0')
                    || RPAD(SUBSTR(TO_CHAR(z.eta_ts, 'DY'), 1, 2) || TO_CHAR(z.eta_ts, '  HH24:MI'), 9)
                    || RPAD(SUBSTR(TO_CHAR(z.eta_ts, 'DY'), 1, 2) || TO_CHAR(z.eta_ts, '  HH24:MI'), 9)
                    || RPAD((CASE TO_CHAR(z.depart_ts, 'D')
                               WHEN '1' THEN '7'
                               WHEN '2' THEN '1'
                               WHEN '3' THEN '2'
                               WHEN '4' THEN '3'
                               WHEN '5' THEN '4'
                               WHEN '6' THEN '5'
                               WHEN '7' THEN '6'
                             END
                            ),
                            10
                           )
                    || RPAD(TO_CHAR(z.depart_ts, 'HH24MI'), 11)
                    || RPAD(z.stop_num, 8)
                    || rpad_fn(SUBSTR(z.orig_load_stop, 5), 8)
                    || i_div
                    || l_llr_dt_char
                    || TO_CHAR(z.depart_ts, 'YYYYMMDDHH24MI')
                    || TO_CHAR(z.eta_ts, 'YYYYMMDDHH24MI') AS txt,
                    z.mcl_cust
                    || ','
                    || z.load_num
                    || ','
                    || SUBSTR(z.orig_load_stop, 1, 4)
                    || ','
                    || z.orig_wt
                    || ','
                    || z.orig_cube
                    || ','
                    || RPAD(SUBSTR(TO_CHAR(z.eta_ts, 'DY'), 1, 2) || TO_CHAR(z.eta_ts, '  HH24:MI'), 9)
                    || ','
                    || TO_CHAR(z.depart_ts, 'HH24MI')
                    || ','
                    || z.stop_num
                    || ','
                    || i_div
                    || l_llr_dt_char
                    || TO_CHAR(z.depart_ts, 'YYYYMMDDHH24MI')
                    || TO_CHAR(z.eta_ts, 'YYYYMMDDHH24MI')
                    || ','
                    || z.seq AS csv
               FROM z)
      SELECT zz.txt, zz.csv
      BULK COLLECT INTO l_t_txt_rpt_lns, l_t_csv_rpt_lns
      FROM   zz;

      logs.dbg('Write TXT File');
      write_sp(l_t_txt_rpt_lns, l_txt_file_nm, l_c_file_dir);
      l_t_txt_rpt_lns.DELETE;   -- release memory
      logs.dbg('FTP TXT File to Remote and Archive');
      ftp_put_sp(i_div, l_txt_file_nm);
      logs.dbg('Write CSV File');
      write_sp(l_t_csv_rpt_lns, l_csv_file_nm, l_c_file_dir);
      l_t_csv_rpt_lns.DELETE;   -- release memory
      logs.dbg('FTP CSV File to Remote and Archive');
      ftp_put_sp(i_div, l_csv_file_nm);

      IF i_div MEMBER OF op_parms_pk.parms_for_val_fn(0, 'TMW_EXTR_DIV', 'Y', 2) THEN
        logs.dbg('TMW Export');
        tmw_export_sp(i_div, i_llr_dt);
      END IF;   -- i_div MEMBER OF op_parms_pk.parms_for_val_fn(0, 'TMW_EXTR_DIV', 'Y', 2)
    END IF;   -- l_t_loads.COUNT > 0

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
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
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END export_sp;

  /*
  ||-----------------------------------------------------------------------------
  || LAST_IMPORT_SP
  ||  Return RteGrpNum, Timestamp and LLRDt of most recent file import.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE last_import_sp(
    i_div          IN      VARCHAR2,
    o_rte_grp_num  OUT     NUMBER,
    o_llr_dt       OUT     VARCHAR2,
    o_ts           OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.LAST_IMPORT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Open cursor');

    SELECT MAX(x.rte_grp_num), MAX(x.llr_dt), MAX(x.ts)
      INTO o_rte_grp_num, o_llr_dt, o_ts
      FROM (SELECT DISTINCT FIRST_VALUE(rg.rte_grp_num) OVER(ORDER BY rg.create_dt DESC) AS rte_grp_num,
                            TO_CHAR(FIRST_VALUE(rg.create_dt) OVER(ORDER BY rg.create_dt DESC),
                                    'YYYY-MM-DD HH24:MI:SS'
                                   ) AS ts,
                            TO_CHAR(FIRST_VALUE(car.llr_dt) OVER(ORDER BY rg.create_dt DESC), 'YYYY-MM-DD') AS llr_dt
                       FROM rte_grp_rt2g rg, cust_auto_rte_rt2c car
                      WHERE rg.div_part = l_div_part
                        AND car.div_part = rg.div_part
                        AND car.rte_grp_num = rg.rte_grp_num) x;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END last_import_sp;

  /*
  ||-----------------------------------------------------------------------------
  || RTE_IMPORT_LIST_SP
  ||  Returns cursor of Imported Route Info returned from Routing System for LLRDt.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 04/12/16 | rhalpai | Change to use common div_part_fn. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE rte_import_list_sp(
    i_div          IN      VARCHAR2,
    i_rte_grp_num  IN      NUMBER,
    i_llr_dt       IN      VARCHAR2,
    o_cur          OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.RTE_IMPORT_LIST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Open cursor');

    OPEN o_cur
     FOR
       SELECT   car.load_num, TO_CHAR(car.depart_ts, 'YYYY-MM-DD HH24:MI') AS depart_ts,
                (CASE
                   WHEN car.stop_num < 10 THEN '0'
                 END) || car.stop_num AS stp, TO_CHAR(car.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts, cx.custb AS cust_id,
                cx.mccusb AS mcl_cust, c.namec AS cust_nm, c.shpctc AS city, c.shpstc AS st, c.shpzpc AS zip,
                op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone, car.new_load,
                TO_CHAR(car.new_depart_ts, 'YYYY-MM-DD HH24:MI') AS new_depart_ts,
                (CASE
                   WHEN car.new_stop < 10 THEN '0'
                 END) || car.new_stop AS new_stp, TO_CHAR(car.new_eta_ts, 'YYYY-MM-DD HH24:MI') AS new_eta_ts,
                CURSOR(SELECT   ROWNUM
                                ||(CASE
                                     WHEN ROWNUM < 10 THEN ' '
                                   END)
                                || ' '
                                || car2.load_num
                                || '/'
                                ||(CASE
                                     WHEN car2.stop_num < 10 THEN '0'
                                   END)
                                || car2.stop_num
                                || ' ==> '
                                || car2.new_load
                                || '/'
                                ||(CASE
                                     WHEN car2.new_stop < 10 THEN '0'
                                   END)
                                || car2.new_stop
                           FROM rte_grp_rt2g rg2, cust_auto_rte_rt2c car2
                          WHERE rg2.div_part = rg.div_part
                            AND rg2.rte_grp_num <> i_rte_grp_num
                            AND rg2.create_dt < rg.create_dt
                            AND car2.div_part = rg2.div_part
                            AND car2.rte_grp_num = rg2.rte_grp_num
                            AND car2.llr_dt = l_llr_dt
                            AND car2.cust_id = car.cust_id
                            AND car2.stat_cd = 'CMP'
                       ORDER BY rg2.create_dt
                      ) AS mov_hist,
                car.stat_cd, NVL2(car.err_msg, 'Y', 'N') AS err_sw, car.err_msg,
                TO_CHAR(car.last_chg_ts, 'YYYYMMDDHH24MISS') AS last_chg_ts, car.user_id
           FROM rte_grp_rt2g rg, cust_auto_rte_rt2c car, mclp020b cx, sysp200c c
          WHERE rg.div_part = l_div_part
            AND rg.rte_grp_num = i_rte_grp_num
            AND car.div_part = rg.div_part
            AND car.rte_grp_num = rg.rte_grp_num
            AND car.llr_dt = l_llr_dt
            AND cx.div_part = car.div_part
            AND cx.custb = car.cust_id
            AND c.div_part = car.div_part
            AND c.acnoc = car.cust_id
       ORDER BY car.new_load, car.new_stop;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rte_import_list_sp;

  /*
  ||-----------------------------------------------------------------------------
  || IMPORT_SP
  ||  Perform initial validations and load CUST_AUTO_RTE_RT2C from import file.
  ||  Abort import if more than one unique LLRDt is found. Set status to NCH
  ||  when OLD and NEW routing info match. Set status to ERR and set appropriate
  ||  error msg for validation failures. Set all other status to IMP.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 06/21/11 | rhalpai | Added logic to cancel any previous entries in import
  ||                    | status prior to importing new file. PIR9348
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Pass div_part to
  ||                    | VALIDATE_SP. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE import_sp(
    i_div             IN  VARCHAR2,
    i_remote_file_nm  IN  VARCHAR2,
    i_user_id         IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm                   := 'OP_DAILY_ROUTING_PK.IMPORT_SP';
    lar_parm                logs.tar_parm;
    l_c_sysdate    CONSTANT DATE                            := SYSDATE;
    l_div_part              NUMBER;
    l_local_file_nm         VARCHAR2(200);
    l_c_create_ts  CONSTANT DATE                            := l_c_sysdate;
    l_rte_grp_num           rte_grp_rt2g.rte_grp_num%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RemoteFileNm', i_remote_file_nm);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_local_file_nm := i_div || '_DLY_RTE_IMPORT_' || TO_CHAR(l_c_create_ts, 'YYYYMMDDHH24MISS');
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Get File from Routing System');
    ftp_get_sp(i_div, i_remote_file_nm, l_local_file_nm);
    logs.dbg('Cancel Previous Imported Entries');

    UPDATE cust_auto_rte_rt2c car
       SET car.stat_cd = 'CAN',
           car.last_chg_ts = l_c_sysdate,
           car.user_id = i_user_id,
           car.err_msg = 'Cancelled for New Import'
     WHERE car.div_part = l_div_part
       AND car.stat_cd = 'IMP';

    logs.dbg('Load Data from Import File');
    prcs_import_file_sp(i_div, l_local_file_nm, l_c_create_ts, i_user_id, l_rte_grp_num);
    logs.dbg('Validate Imported Data');
    validate_sp(l_div_part, l_rte_grp_num, 'IMP');
    COMMIT;
    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN g_e_parse_error OR g_e_invalid_div OR g_e_mixed_llr_dates OR DUP_VAL_ON_INDEX THEN
      logs.warn(SQLERRM, lar_parm);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      ROLLBACK;
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END import_sp;

  /*
  ||-----------------------------------------------------------------------------
  || CANCL_RTE_SP
  ||  Set matching routing entries to cancel status.
  ||
  ||  CustList:
  ||  CustId~CustId
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Pass div_part to
  ||                    | UPD_STAT_SP. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE cancl_rte_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_cust_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.CANCL_RTE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_cust_list IS NOT NULL THEN
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.dbg('Set Cancel Status');
      upd_stat_sp(l_div_part, i_rte_grp_num, l_llr_dt, i_cust_list, g_c_actn_cancl, 'CAN', i_user_id);
      COMMIT;
      logs.dbg('Reset Process to Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    END IF;   -- i_cust_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END cancl_rte_sp;

  /*
  ||-----------------------------------------------------------------------------
  || UPD_RTE_SP
  ||  Apply changes for ERR routing info to matching routing entries.
  ||  ParmList:
  ||  CustId~Load~DepartTS~Stop~EtaTS`CustId~Load~DepartTS~Stop~EtaTS
  ||  00000001~0400~2011-03-02 14:30~10~2011-03-02 18:00`00000002~0400~2011-03-02 14:30~20~2011-03-02 18:30
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Pass div_part to
  ||                    | VALIDATE_SP. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE upd_rte_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_parm_list    IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.UPD_RTE_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_div_part            NUMBER;
    l_llr_dt              DATE;
    l_t_grps              type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');

    IF i_parm_list IS NOT NULL THEN
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.dbg('Initialize');
      logs.dbg('Parse Groups of Parm Field Lists');
      l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);

      IF l_t_grps IS NOT NULL THEN
        logs.dbg('Upd Routing');
        FORALL i IN l_t_grps.FIRST .. l_t_grps.LAST
          UPDATE cust_auto_rte_rt2c car
             SET (car.new_load, car.new_depart_ts, car.new_stop, car.new_eta_ts) =
                   (SELECT MAX(DECODE(x.seq, 2, x.str)), TO_DATE(MAX(DECODE(x.seq, 3, x.str)), 'YYYY-MM-DD HH24:MI'),
                           TO_NUMBER(MAX(DECODE(x.seq, 4, x.str))),
                           TO_DATE(MAX(DECODE(x.seq, 5, x.str)), 'YYYY-MM-DD HH24:MI')
                      FROM (SELECT SUBSTR(v.str, v.start_pos, v.nxt_delim_pos - v.start_pos) AS str, ROWNUM AS seq
                              FROM (SELECT s.delim_pos + 1 AS start_pos, s.str,
                                           NVL(LEAD(s.delim_pos, 1) OVER(ORDER BY s.delim_pos),
                                               s.str_len + 1
                                              ) AS nxt_delim_pos
                                      FROM (SELECT     d.str,
                                                       INSTR(d.str, op_const_pk.field_delimiter, LEVEL) AS delim_pos,
                                                       LENGTH(d.str) AS str_len
                                                  FROM (SELECT l_t_grps(i) AS str
                                                          FROM DUAL) d
                                            CONNECT BY LEVEL <= LENGTH(d.str)) s) v
                             WHERE v.start_pos < v.nxt_delim_pos) x),
                 car.stat_cd = 'IMP',
                 car.err_msg = NULL,
                 car.last_chg_ts = l_c_sysdate,
                 car.user_id = i_user_id
           WHERE car.div_part = l_div_part
             AND car.rte_grp_num = i_rte_grp_num
             AND car.llr_dt = l_llr_dt
             AND car.cust_id = SUBSTR(l_t_grps(i), 1, 8)
             AND car.stat_cd = 'ERR';
        logs.dbg('Validate');
        validate_sp(l_div_part, i_rte_grp_num, 'IMP');
        COMMIT;
      END IF;   -- l_t_grps IS NOT NULL

      logs.dbg('Reset Process to Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    END IF;   -- i_parm_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END upd_rte_sp;

  /*
  ||-----------------------------------------------------------------------------
  || MOVE_SP
  ||  Validate moves for all routing entries in IMP status. Upon successful
  ||  validation, add entry to override table and move matching orders from OLD
  ||  route to NEW route and set status to CMP. Upon validation failure, set
  ||  status to ERR and error msg as applicable.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 06/21/11 | rhalpai | Changed logic to pass cursor variable to TO_LIST_FN
  ||                    | when creating delimited CustList to allow for larger
  ||                    | customer list. PIR9348
  || 11/02/11 | rhalpai | Remove logic to Close Cursor after call to TO_LIST_FN
  ||                    | which now closes the cursor. IM-033903
  || 10/28/13 | rhalpai | Change logic to include Load as part of primary key of
  ||                    | CUST_RTE_OVRRD_RT3C. IM-123463
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Pass div_part to
  ||                    | UPD_STAT_SP, VALIDATE_SP, MOVE_ORDS_SP. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE move_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.MOVE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
    l_cust_list          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Get Cust List');

    OPEN l_cv
     FOR
       SELECT car.cust_id
         FROM cust_auto_rte_rt2c car
        WHERE car.div_part = l_div_part
          AND car.rte_grp_num = i_rte_grp_num
          AND car.llr_dt = l_llr_dt
          AND car.stat_cd = 'IMP';

    l_cust_list := to_list_fn(l_cv, op_const_pk.field_delimiter);

    IF l_cust_list IS NOT NULL THEN
      logs.dbg('Set Work Status');
      upd_stat_sp(l_div_part, i_rte_grp_num, l_llr_dt, l_cust_list, g_c_actn_move, 'WRK', i_user_id);
      logs.dbg('Validate');
      validate_sp(l_div_part, i_rte_grp_num, 'WRK');
      logs.dbg('Apply Overrides');
      MERGE INTO cust_rte_ovrrd_rt3c cro
           USING (SELECT r.div_part, r.cust_id, r.llr_dt, r.load_num, r.stop_num, r.depart_ts, r.eta_ts, r.new_load,
                         r.new_stop, r.new_depart_ts, r.new_eta_ts
                    FROM cust_auto_rte_rt2c r
                   WHERE r.div_part = l_div_part
                     AND r.rte_grp_num = i_rte_grp_num
                     AND r.llr_dt = l_llr_dt
                     AND r.stat_cd = 'WRK') x
              ON (    cro.div_part = x.div_part
                  AND cro.cust_id = x.cust_id
                  AND cro.llr_dt = x.llr_dt
                  AND cro.load_num = x.new_load)
        WHEN MATCHED THEN
          UPDATE
             SET cro.depart_ts = x.new_depart_ts, cro.stop_num = x.new_stop, cro.eta_ts = x.new_eta_ts,
                 cro.depart_ovrrd_sw = 'Y', cro.stop_ovrrd_sw = 'Y', cro.eta_ovrrd_sw = 'Y'
        WHEN NOT MATCHED THEN
          INSERT(div_part, cust_id, llr_dt, load_num, depart_ts, stop_num, eta_ts, depart_ovrrd_sw, stop_ovrrd_sw,
                 eta_ovrrd_sw)
          VALUES(x.div_part, x.cust_id, x.llr_dt, x.new_load, x.new_depart_ts, x.new_stop, x.new_eta_ts, 'Y', 'Y', 'Y');
      logs.dbg('Move Orders');
      move_ords_sp(l_div_part, i_rte_grp_num, l_llr_dt, g_c_actn_move, i_user_id);
      logs.dbg('Set Complete Status');
      upd_stat_sp(l_div_part, i_rte_grp_num, l_llr_dt, l_cust_list, g_c_actn_move, 'CMP', i_user_id);
      COMMIT;
    END IF;   -- v_cust_list IS NOT NULL

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
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
                                                  i_user_id,
                                                  l_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END move_sp;

  /*
  ||-----------------------------------------------------------------------------
  || UNDO_MOVE_SP
  ||  Move all routing entries in CMP status from NEW route to OLD route, remove
  ||  customer route override entries, and set status to CAN.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original. PIR9348
  || 06/21/11 | rhalpai | Changed logic to pass cursor variable to TO_LIST_FN
  ||                    | when creating delimited CustList to allow for larger
  ||                    | customer list. PIR9348
  || 11/02/11 | rhalpai | Remove logic to Close Cursor after call to TO_LIST_FN
  ||                    | which now closes the cursor. IM-033903
  || 10/28/13 | rhalpai | Change logic to include Load as part of primary key of
  ||                    | CUST_RTE_OVRRD_RT3C. IM-123463
  || 04/12/16 | rhalpai | Change to use common div_part_fn. Pass div_part to
  ||                    | UPD_STAT_SP, MOVE_ORDS_SP. PIR14660
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE undo_move_sp(
    i_div          IN  VARCHAR2,
    i_rte_grp_num  IN  NUMBER,
    i_llr_dt       IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DAILY_ROUTING_PK.UNDO_MOVE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
    l_cust_list          typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RteGrpNum', i_rte_grp_num);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_llr_dt := TO_DATE(i_llr_dt, 'YYYY-MM-DD');
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Get Cust List');

    OPEN l_cv
     FOR
       SELECT car.cust_id
         FROM cust_auto_rte_rt2c car
        WHERE car.div_part = l_div_part
          AND car.rte_grp_num = i_rte_grp_num
          AND car.llr_dt = l_llr_dt
          AND car.stat_cd = 'CMP';

    l_cust_list := to_list_fn(l_cv, op_const_pk.field_delimiter);

    IF l_cust_list IS NOT NULL THEN
      logs.dbg('Set Work Status');
      upd_stat_sp(l_div_part, i_rte_grp_num, l_llr_dt, l_cust_list, g_c_actn_undo, 'WRK', i_user_id);
      logs.dbg('Remove Overrides');

      DELETE FROM cust_rte_ovrrd_rt3c cro
            WHERE cro.div_part = l_div_part
              AND cro.llr_dt = l_llr_dt
              AND (cro.cust_id, cro.load_num) IN(SELECT car.cust_id, car.new_load
                                                   FROM cust_auto_rte_rt2c car
                                                  WHERE car.div_part = l_div_part
                                                    AND car.rte_grp_num = i_rte_grp_num
                                                    AND car.llr_dt = l_llr_dt
                                                    AND car.stat_cd = 'WRK');

      logs.dbg('Move Orders');
      move_ords_sp(l_div_part, i_rte_grp_num, l_llr_dt, g_c_actn_undo, i_user_id);
      logs.dbg('Set Complete Status');
      upd_stat_sp(l_div_part, i_rte_grp_num, l_llr_dt, l_cust_list, g_c_actn_undo, 'CAN', i_user_id);
      COMMIT;
    END IF;   -- l_cust_list IS NOT NULL

    logs.dbg('Reset Process to Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_routing,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                l_div_part
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
                                                  i_user_id,
                                                  l_div_part
                                                 );
      ROLLBACK;
      logs.err(lar_parm);
  END undo_move_sp;
END op_daily_routing_pk;
/

