CREATE OR REPLACE PACKAGE op_process_common_pk IS
  /**
  ||----------------------------------------------------------------------------
  || The purpose of this package is to find out the procedures that are running
  || longer than the specified time, and send mail to the concerned group for a
  || fixed no. of times. Also create reports for different report processes and
  || send these reports to the concerned group.
  ||----------------------------------------------------------------------------
  **/

  /*
  ||----------------------------------------------------------------------------
  || OP_PROCESS_COMMON_PK
  ||----------------------------------------------------------------------------
  |             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/01 | Santosh | Original
  || 03/19/02 | Santosh | Changed the Parameters for Notify_Group_sp from
  ||                    | p_grp_id to p_process_id and i_division
  || 02/25/02 | Santosh | Changed GET_CLOSE_DAY_TIME procedure to a function,
  ||                    | also now passing another parameter p_type_flag.
  ||                    | Eliminated two other functions - check_close_day,
  ||                    | check_close_time.
  ||----------------------------------------------------------------------------
  */
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
  /**
  ||----------------------------------------------------------------------------
  || Check whether a day code and time is between two other day codes and times
  || #param i_day_cd             Compare day code
  || #param i_time               Compare time
  || #param i_from_day           From day code
  || #param i_from_time          From time
  || #param i_to_day             To day code
  || #param i_to_time            To time
  || #return                     {*} 'Y' Yes
  ||                             {*} 'N' No
  ||                             {*} 'E' Error
  ||----------------------------------------------------------------------------
  **/
  FUNCTION check_day_between_fn(
    i_day_cd     IN  VARCHAR2,
    i_time       IN  NUMBER,
    i_from_day   IN  VARCHAR2,
    i_from_time  IN  NUMBER,
    i_to_day     IN  VARCHAR2,
    i_to_time    IN  NUMBER
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Convert Day code into day number
  || #param i_day_cd             Day code
  || #return                     Day number
  ||----------------------------------------------------------------------------
  **/
  FUNCTION get_day_num_of_week(
    i_day_cd  IN  VARCHAR2
  )
    RETURN NUMBER;

  /**
  ||----------------------------------------------------------------------------
  || Convert Day Number into day code
  || #param i_day_num            Day number
  || #return                     Day code
  ||----------------------------------------------------------------------------
  **/
  FUNCTION get_day_code_of_week(
    i_day_num  IN  NUMBER
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Check whether a Closing Cut off day codes is NULL
  || If yes, Closing cut off day code and time would be the difference of
  || departure cut off time minus the value of PRCTMD (in Min.) column from
  || MCLP130D for that division
  || #param i_close_day_cd       Close day code
  || #param i_close_time         Close time
  || #param i_depart_day_cd      Departure day code
  || #param i_depart_time        Departure time
  || #param i_div_part           DivPart
  || #param i_typ                Type
  ||                             Valid values are:
  ||                             {*} 'D' Day
  ||                             {*} 'T' Time
  || #return                     Adjusted day code or time
  ||----------------------------------------------------------------------------
  **/
  FUNCTION get_close_day_time(
    i_close_day_cd   IN  VARCHAR2,
    i_close_time     IN  NUMBER,
    i_depart_day_cd  IN  VARCHAR2,
    i_depart_time    IN  NUMBER,
    i_div_part       IN  NUMBER,
    i_typ            IN  VARCHAR2
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  /**
  ||----------------------------------------------------------------------------
  || Procedure to find procedures running longer than the specified time.
  || It calls NOTIFY_GROUP procedure to send mail to the concerned group.
  || #param i_div                Division code
  || #param i_ctrl_m_interval    Interval for this process running under CTRL-M
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE monitor_process_sp(
    i_div              IN  VARCHAR2,
    i_ctrl_m_interval  IN  NUMBER DEFAULT 10
  );

  /**
  ||----------------------------------------------------------------------------
  || A common procedure to create TEMP tables, corresponding to Cutt off codes
  || and calls another common procedure op_process_reports_pk.gen_reports_sp
  || to generate reports, using the TEMP table, and send reports the the mail
  || group
  || #param i_div                Division code
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE report_process_common_sp(
    i_div  IN  VARCHAR2
  );

  /**
  ||----------------------------------------------------------------------------
  || Procedure to notify the group thru e-mail
  || #param i_div                Division code
  || #param i_prcs_id            Process ID
  || #param i_subj               Email subject
  || #param i_mail_msg1          Email message line 1
  || #param i_mail_msg2          Email message line 2
  || #param i_mail_msg3          Email message line 3
  || #param i_mail_msg4          Email message line 4
  || #param i_mail_msg5          Email message line 5
  || #param i_reply_addr         Reply email address
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE notify_group_sp(
    i_div         IN  VARCHAR2,
    i_prcs_id     IN  VARCHAR2,
    i_subj        IN  VARCHAR2 DEFAULT 'Process Monitor and Reporting System Mail',
    i_mail_msg1   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg2   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg3   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg4   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg5   IN  VARCHAR2 DEFAULT NULL,
    i_reply_addr  IN  VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Convert all (ORD, LLR, CLO, DEP) cut-off day codes and times into timestamps
  || #param i_ord_day_cd         Order Cutoff day code
  || #param i_ord_time           Order Cutoff time
  || #param i_llr_day_cd         LLR Cutoff day code
  || #param i_llr_time           LLR Cutoff time
  || #param i_close_day_cd       Load Close Cutoff day code
  || #param i_close_time         Load Close Cutoff time
  || #param i_depart_day_cd      Departure day code
  || #param i_depart_time        Departure time
  || #param o_ord_ts             Order Cutoff timestamp
  || #param o_llr_ts             LLR Cutoff timestamp
  || #param o_close_ts           Load Close Cutoff timestamp
  || #param o_depart_ts          Departure timestamp
  || #param i_curr_day_cd        Current day code
  || #param i_curr_time          Current time
  || #param i_curr_dt_str        Current date string
  || #param o_rc                 Return code
  || #param i_div_part           DivPart
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE convert_to_timestamp_sp(
    i_ord_day_cd     IN      VARCHAR2,
    i_ord_time       IN      NUMBER,
    i_llr_day_cd     IN      VARCHAR2,
    i_llr_time       IN      NUMBER,
    i_close_day_cd   IN      VARCHAR2,
    i_close_time     IN      NUMBER,
    i_depart_day_cd  IN      VARCHAR2,
    i_depart_time    IN      NUMBER,
    o_ord_ts         OUT     DATE,
    o_llr_ts         OUT     DATE,
    o_close_ts       OUT     DATE,
    o_depart_ts      OUT     DATE,
    i_curr_day_cd    IN      VARCHAR2,
    i_curr_time      IN      VARCHAR2,
    i_curr_dt_str    IN      VARCHAR2,
    o_rc             OUT     NUMBER,
    i_div_part       IN      NUMBER
  );

  /**
  ||----------------------------------------------------------------------------
  || Calls duplicate order check procedure and logs the output to a file
  || #param i_last_run_ts        Last run timestamp
  || #param i_div                Division code
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE duplicate_order_sp(
    i_last_run_ts  IN  DATE,
    i_div          IN  VARCHAR2
  );
END op_process_common_pk;
/

CREATE OR REPLACE PACKAGE BODY op_process_common_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_PROCESS_COMMON_PK
  ||
  || The purpose of this package is to find out the procedures that are running
  || longer than the specified time, and send mail to the concerned group for a
  || fixed no. of times. Also create reports for different report processes and
  || send these reports to the concerned group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/02 | Santosh | Original
  || 03/07/02 | Santosh | In function check_day_between_fn, Commented two lines
  ||                    | so that it will not return 'N', when in_day and
  ||                    | to_day are equal and In_time > to_time, because
  ||                    | to_time may be for next day, so now it returns 'Y'
  ||                    | for this case.
  ||                    | Fixed one bug in the Procedure get_close_day_time,
  ||                    | to fix time conversion error.(Changed 2359 to 2400)
  ||                    | Replaced sql_errors with sql_warnings proc at few places.
  || 03/14/02 | Santosh | report_process_common_sp - Changed to execute all
  ||                    | waiting processes one by one. Earlier it was executing
  ||                    | only the process that is waiting for longest period of
  ||                    | time out of all waiting processes.
  || 03/15/02 | Santosh | notify_group_sp - Changed passing parameters from
  ||                    | p_group_id to i_prcs_id and i_division
  || 03/15/02 | Santosh | monitor_process_sp - Changed SELECT INTO statement
  ||                    | into a cursor to monitor processes for any Logon
  ||                    | User Id.
  ||                    | Changed parameters in call to notify_group_sp, to be
  ||                    | in sync with modified notify_group_sp procedure.
  || 03/22/02 | Santosh | In monitor_process_sp, refining the logic to send
  ||                    | emails, send only one email within each computed Mail
  ||                    | interval for a 'Monitored' process.
  || 03/25/02 | Santosh | Changed GET_CLOSE_DAY_TIME procedure to a function,
  ||                    | eliminated two other functions - Check_close_day,
  ||                    | check_close_time. Tuned WHERE clauses of few SQLs and
  ||                    | CHECK_DAY_BETWEEN_FN.
  ||                    | Changed calls to Check_close_day and check_close_time
  ||                    | to the modified GET_CLOSE_DAY_TIME function.
  || 04/08/02 | Santosh | Modified cursors in all build% procedures to exclude
  ||                    | 'temp' loads (DFLT, DIST, LOST).
  || 04/12/02 | Santosh | Added a new proc duplicate_order_sp, to create a log
  ||                    | file for the execution of op_dup_order_check_sp
  ||                    | procedure.
  ||                    | report_process_common_sp - changed the direct call of
  ||                    | op_dup_order_check_sp with a call to
  ||                    | duplicate_order_sp.
  ||                    | In all proc header comments, Fixed wrongly typed
  ||                    | CREATE DATE as 2001 to 2002.
  || 04/18/02 | Santosh | Fixed Duplicate Mail notification, Changed
  ||                    | notify_group_sp.
  || 04/29/02 | Santosh | Added parenthesis to few SQLs in build_* procedures,
  ||                    | to fix the logic.
  || 05/09/02 | Santosh | Modified duplicate_order_sp to create log file, only
  ||                    | when the new parameter v_flag being passed from
  ||                    | op_dup_order_check_sp is TRUE.
  || 05/10/02 | Santosh | Added opcsr_logging_check_sp to execute a unix script
  ||                    | XXOPCSRLoggingCheck.scr to check whether the Logging
  ||                    | is ON, and if so, send an email to the group.
  || 05/10/02 | Santosh | Added gen_sql_util_errors_sp procedure to create a
  ||                    | report having every entry since last run, from
  ||                    | sql_utilties table for all errors (sql_type = 'E').
  ||                    | Modified report_process_common_sp.
  || 06/03/02 | Santosh | Fixed PL/SQL: numeric or value error, in
  ||                    | OPCSR_LOGGING_CHECK_SP.
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || BUILD_TEMP_O_L_SP
  ||
  || Builds a TEMP table having all Load data for the defined range (Order
  || cutoff and LLR cutOff), if the current date is between these two dates.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/25/02 | Santosh | Changed calls to Check_close_day and check_close_time
  ||                    | to GET_CLOSE_DAY_TIME function.
  || 04/08/02 | Santosh | Modified l_cur_loads cursor to exclude 'temp' loads
  ||                    | (DFLT, DIST, LOST).
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_temp_o_l_sp(
    i_curr_day_cd  IN  VARCHAR2,
    i_curr_time    IN  VARCHAR2,
    i_curr_dt_str  IN  VARCHAR2,
    i_div_part     IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.BUILD_TEMP_O_L_SP';
    lar_parm             logs.tar_parm;

    -- A cursor to get all records, if the current date is in between load cuttoff and LLR cutoff
    CURSOR l_cur_loads(
      b_curr_day_cd  VARCHAR2,
      b_curr_time    VARCHAR2,
      b_div_part     NUMBER
    ) IS
      SELECT mc.loadc AS load_num, mc.destc AS load_name, mc.ldordc AS ord_cut_off_day, mc.ldortc AS ord_cut_off_time,
             mc.llrcdc AS llr_cut_off_day, mc.llrctc llr_cut_off_time,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') AS close_cut_off_day,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') AS close_cut_off_time,
             mc.depdac AS dep_cut_off_day, mc.deptmc AS dep_cut_off_time
        FROM mclp120c mc
       WHERE mc.div_part = b_div_part
         AND mc.loadc NOT IN('DIST', 'DFLT', 'LOST')
         AND check_day_between_fn(b_curr_day_cd, b_curr_time, mc.ldordc, mc.ldortc, mc.llrcdc, mc.llrctc) = 'Y'
         AND mc.ldordc IS NOT NULL
         AND mc.llrcdc IS NOT NULL
         AND mc.depdac IS NOT NULL;

    l_ord_ts             DATE;
    l_llr_ts             DATE;
    l_close_ts           DATE;
    l_depart_ts          DATE;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Loads Cursor');
    FOR l_r_load IN l_cur_loads(i_curr_day_cd, i_curr_time, i_div_part) LOOP
      -- Calling procedure to convert all Cutoff day codes and times into it's corresponding timestamps
      logs.dbg('Calling CONVERT_TO_TIMESTAMP_SP');
      convert_to_timestamp_sp(l_r_load.ord_cut_off_day,
                              l_r_load.ord_cut_off_time,
                              l_r_load.llr_cut_off_day,
                              l_r_load.llr_cut_off_time,
                              l_r_load.close_cut_off_day,
                              l_r_load.close_cut_off_time,
                              l_r_load.dep_cut_off_day,
                              l_r_load.dep_cut_off_time,
                              l_ord_ts,
                              l_llr_ts,
                              l_close_ts,
                              l_depart_ts,
                              i_curr_day_cd,
                              i_curr_time,
                              i_curr_dt_str,
                              l_rc,
                              i_div_part
                             );
      -- Inserting record into TEMP table
      logs.dbg('Inserting record');

      IF l_rc <> 1 THEN
        INSERT INTO temp_load_cut_off
                    (load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
                    )
             VALUES (l_r_load.load_num, l_r_load.load_name, l_ord_ts, l_llr_ts, l_close_ts, l_depart_ts
                    );
      END IF;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END build_temp_o_l_sp;

  /*
  ||----------------------------------------------------------------------------
  || BUILD_TEMP_L_C_SP
  ||
  || Builds a TEMP table having all Load data for the defined range (LLR cutoff
  || and Close cutOff), if the current date is between these two dates.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/25/02 | Santosh | Changed calls to Check_close_day and check_close_time
  ||                    | to GET_CLOSE_DAY_TIME function.
  || 04/08/02 | Santosh | Modified l_cur_loads cursor to exclude 'temp' loads
  ||                    | (DFLT, DIST, LOST).
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_temp_l_c_sp(
    i_curr_day_cd  IN  VARCHAR2,
    i_curr_time    IN  VARCHAR2,
    i_curr_dt_str  IN  VARCHAR2,
    i_div_part     IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.BUILD_TEMP_L_C_SP';
    lar_parm             logs.tar_parm;

    -- A cursor to get all records, if the current date is in between LLR cuttoff and Close cutoff
    CURSOR l_cur_loads(
      b_div_part     NUMBER,
      b_curr_day_cd  VARCHAR2,
      b_curr_time    VARCHAR2
    ) IS
      SELECT mc.loadc AS load_num, mc.destc AS load_name, mc.ldordc AS ord_cut_off_day, mc.ldortc AS ord_cut_off_time,
             mc.llrcdc AS llr_cut_off_day, mc.llrctc AS llr_cut_off_time,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') AS close_cut_off_day,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') AS close_cut_off_time,
             mc.depdac AS dep_cut_off_day, mc.deptmc AS dep_cut_off_time
        FROM mclp120c mc
       WHERE mc.div_part = b_div_part
         AND mc.loadc NOT IN('DIST', 'DFLT', 'LOST')
         AND check_day_between_fn(b_curr_day_cd,
                                  b_curr_time,
                                  mc.llrcdc,
                                  mc.llrctc,
                                  get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D'),
                                  get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T')
                                 ) = 'Y';

    l_ord_ts             DATE;
    l_llr_ts             DATE;
    l_close_ts           DATE;
    l_depart_ts          DATE;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Loads Cursor');
    FOR l_r_load IN l_cur_loads(i_div_part, i_curr_day_cd, i_curr_time) LOOP
      -- Calling procedure to convert all Cutoff day codes and times into it's corresponding timestamps
      logs.dbg('Calling CONVERT_TO_TIMESTAMP_SP');
      convert_to_timestamp_sp(l_r_load.ord_cut_off_day,
                              l_r_load.ord_cut_off_time,
                              l_r_load.llr_cut_off_day,
                              l_r_load.llr_cut_off_time,
                              l_r_load.close_cut_off_day,
                              l_r_load.close_cut_off_time,
                              l_r_load.dep_cut_off_day,
                              l_r_load.dep_cut_off_time,
                              l_ord_ts,
                              l_llr_ts,
                              l_close_ts,
                              l_depart_ts,
                              i_curr_day_cd,
                              i_curr_time,
                              i_curr_dt_str,
                              l_rc,
                              i_div_part
                             );
      -- Inserting record into TEMP table
      logs.dbg('Inserting record');

      IF l_rc <> 1 THEN
        INSERT INTO temp_load_cut_off
                    (load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
                    )
             VALUES (l_r_load.load_num, l_r_load.load_name, l_ord_ts, l_llr_ts, l_close_ts, l_depart_ts
                    );
      END IF;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END build_temp_l_c_sp;

  /*
  ||----------------------------------------------------------------------------
  || BUILD_TEMP_LLR_SP
  ||
  || Builds a TEMP table having all Load data for 'Orders not released'
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/25/02 | Santosh | Changed calls to Check_close_day and check_close_time
  ||                    | to GET_CLOSE_DAY_TIME function. Tuned WHERE clause.
  || 04/08/02 | Santosh | Modified l_cur_loads cursor to exclude 'temp' loads
  ||                    | (DFLT, DIST, LOST).
  || 04/29/02 | Santosh | Added parenthesis in the WHERE clause to correct the
  ||                    | logic
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_temp_llr_sp(
    i_last_run_day_cd  IN  VARCHAR2,
    i_last_run_time    IN  VARCHAR2,
    i_curr_day_cd      IN  VARCHAR2,
    i_curr_time        IN  VARCHAR2,
    i_curr_dt_str      IN  VARCHAR2,
    i_div_part         IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.BUILD_TEMP_LLR_SP';
    lar_parm             logs.tar_parm;

    -- A cursor to get all records for 'Orders not released'
    CURSOR l_cur_loads(
      b_div_part         NUMBER,
      b_curr_day_cd      VARCHAR2,
      b_curr_time        VARCHAR2,
      b_last_run_day_cd  VARCHAR2,
      b_last_run_time    VARCHAR2
    ) IS
      SELECT mc.loadc AS load_num, mc.destc AS load_name, mc.ldordc AS ord_cut_off_day, mc.ldortc AS ord_cut_off_time,
             mc.llrcdc AS llr_cut_off_day, mc.llrctc AS llr_cut_off_time,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') AS close_cut_off_day,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') AS close_cut_off_time,
             mc.depdac AS dep_cut_off_day, mc.deptmc AS dep_cut_off_time
        FROM mclp120c mc
       WHERE mc.div_part = b_div_part
         AND mc.loadc NOT IN('DIST', 'DFLT', 'LOST')
         AND (   (    (    mc.llrcdc = b_last_run_day_cd
                       AND mc.llrctc >= b_last_run_time)
                  AND (   (    mc.llrcdc = b_curr_day_cd
                           AND mc.llrctc < b_curr_time)
                       OR (mc.llrcdc <> b_curr_day_cd))
                 )
              OR (    mc.llrcdc <> b_last_run_day_cd
                  AND mc.llrcdc = b_curr_day_cd
                  AND mc.llrctc < b_curr_time)
             );

    l_ord_ts             DATE;
    l_llr_ts             DATE;
    l_close_ts           DATE;
    l_depart_ts          DATE;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LastRunDay', i_last_run_day_cd);
    logs.add_parm(lar_parm, 'LastRunTime', i_last_run_time);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Loads Cursor');
    FOR l_r_load IN l_cur_loads(i_div_part, i_curr_day_cd, i_curr_time, i_last_run_day_cd, i_last_run_time) LOOP
      -- Calling procedure to convert all Cutoff day codes and times into it's corresponding timestamps
      logs.dbg('Calling CONVERT_TO_TIMESTAMP_SP');
      convert_to_timestamp_sp(l_r_load.ord_cut_off_day,
                              l_r_load.ord_cut_off_time,
                              l_r_load.llr_cut_off_day,
                              l_r_load.llr_cut_off_time,
                              l_r_load.close_cut_off_day,
                              l_r_load.close_cut_off_time,
                              l_r_load.dep_cut_off_day,
                              l_r_load.dep_cut_off_time,
                              l_ord_ts,
                              l_llr_ts,
                              l_close_ts,
                              l_depart_ts,
                              i_curr_day_cd,
                              i_curr_time,
                              i_curr_dt_str,
                              l_rc,
                              i_div_part
                             );
      -- Inserting record into TEMP table
      logs.dbg('Inserting record');

      IF l_rc <> 1 THEN
        INSERT INTO temp_load_cut_off
                    (load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
                    )
             VALUES (l_r_load.load_num, l_r_load.load_name, l_ord_ts, l_llr_ts, l_close_ts, l_depart_ts
                    );
      END IF;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END build_temp_llr_sp;

  /*
  ||----------------------------------------------------------------------------
  || BUILD_TEMP_CLO_SP
  ||
  || Builds a TEMP table having all Load data for 'Loads not Closed'
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/25/02 | Santosh | Changed calls to Check_close_day and check_close_time
  ||                    | to GET_CLOSE_DAY_TIME function. Tuned WHERE clause
  || 04/08/02 | Santosh | Modified l_cur_loads cursor to exclude 'temp' loads
  ||                    | (DFLT, DIST, LOST).
  || 04/29/02 | Santosh | Added parenthesis in the WHERE clause to correct the
  ||                    | logic
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_temp_clo_sp(
    i_last_run_day_cd  IN  VARCHAR2,
    i_last_run_time    IN  VARCHAR2,
    i_curr_day_cd      IN  VARCHAR2,
    i_curr_time        IN  VARCHAR2,
    i_curr_dt_str      IN  VARCHAR2,
    i_div_part         IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.BUILD_TEMP_CLO_SP';
    lar_parm             logs.tar_parm;

    -- A cursor to get all records for 'Loads not Closed'
    CURSOR l_cur_loads(
      b_div_part         NUMBER,
      b_curr_day_cd      VARCHAR2,
      b_curr_time        VARCHAR2,
      b_last_run_day_cd  VARCHAR2,
      b_last_run_time    VARCHAR2
    ) IS
      SELECT mc.loadc AS load_num, mc.destc AS load_name, mc.ldordc AS ord_cut_off_day, mc.ldortc AS ord_cut_off_time,
             mc.llrcdc AS llr_cut_off_day, mc.llrctc AS llr_cut_off_time,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') AS close_cut_off_day,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') AS close_cut_off_time,
             mc.depdac AS dep_cut_off_day, mc.deptmc AS dep_cut_off_time
        FROM mclp120c mc
       WHERE mc.div_part = b_div_part
         AND mc.loadc NOT IN('DIST', 'DFLT', 'LOST')
         AND (   (    (    get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') =
                                                                                                       b_last_run_day_cd
                       AND get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') >=
                                                                                                         b_last_run_time
                      )
                  AND (   (b_last_run_day_cd <> b_curr_day_cd)
                       OR (    b_last_run_day_cd = b_curr_day_cd
                           AND get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') <
                                                                                                             b_curr_time
                          )
                      )
                 )
              OR (    b_curr_day_cd <> b_last_run_day_cd
                  AND get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') = b_curr_day_cd
                  AND get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') < b_curr_time
                 )
             );

    l_ord_ts             DATE;
    l_llr_ts             DATE;
    l_close_ts           DATE;
    l_depart_ts          DATE;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LastRunDay', i_last_run_day_cd);
    logs.add_parm(lar_parm, 'LastRunTime', i_last_run_time);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Loads Cursor');
    FOR l_r_load IN l_cur_loads(i_div_part, i_curr_day_cd, i_curr_time, i_last_run_day_cd, i_last_run_time) LOOP
      -- Calling procedure to convert all Cutoff day codes and times into it's corresponding timestamps
      logs.dbg('Calling CONVERT_TO_TIMESTAMP_SP');
      convert_to_timestamp_sp(l_r_load.ord_cut_off_day,
                              l_r_load.ord_cut_off_time,
                              l_r_load.llr_cut_off_day,
                              l_r_load.llr_cut_off_time,
                              l_r_load.close_cut_off_day,
                              l_r_load.close_cut_off_time,
                              l_r_load.dep_cut_off_day,
                              l_r_load.dep_cut_off_time,
                              l_ord_ts,
                              l_llr_ts,
                              l_close_ts,
                              l_depart_ts,
                              i_curr_day_cd,
                              i_curr_time,
                              i_curr_dt_str,
                              l_rc,
                              i_div_part
                             );
      -- Inserting record into TEMP table
      logs.dbg('Inserting record');

      IF l_rc <> 1 THEN
        INSERT INTO temp_load_cut_off
                    (load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
                    )
             VALUES (l_r_load.load_num, l_r_load.load_name, l_ord_ts, l_llr_ts, l_close_ts, l_depart_ts
                    );
      END IF;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END build_temp_clo_sp;

  /*
  ||----------------------------------------------------------------------------
  || BUILD_TEMP_ORD_SP
  ||
  || Builds a TEMP table having all Load data for 'Order cutt off'
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/25/02 | Santosh | Changed calls to Check_close_day and check_close_time
  ||                    | to GET_CLOSE_DAY_TIME function. Tuned WHERE clause
  || 04/08/02 | Santosh | Modified l_cur_loads cursor to exclude 'temp' loads
  ||                    | (DFLT, DIST, LOST).
  || 04/29/02 | Santosh | Added parenthesis in the WHERE clause to correct the
  ||                    | logic
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_temp_ord_sp(
    i_last_run_day_cd  IN  VARCHAR2,
    i_last_run_time    IN  VARCHAR2,
    i_curr_day_cd      IN  VARCHAR2,
    i_curr_time        IN  VARCHAR2,
    i_curr_dt_str      IN  VARCHAR2,
    i_div_part         IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.BUILD_TEMP_ORD_SP';
    lar_parm             logs.tar_parm;

    -- A cursor to get all records for 'Order cutt off'
    CURSOR l_cur_loads(
      b_div_part         NUMBER,
      b_curr_day_cd      VARCHAR2,
      b_curr_time        VARCHAR2,
      b_last_run_day_cd  VARCHAR2,
      b_last_run_time    VARCHAR2
    ) IS
      SELECT mc.loadc AS load_num, mc.destc AS load_name, mc.ldordc AS ord_cut_off_day, mc.ldortc AS ord_cut_off_time,
             mc.llrcdc AS llr_cut_off_day, mc.llrctc AS llr_cut_off_time,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') AS close_cut_off_day,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') AS close_cut_off_time,
             mc.depdac AS dep_cut_off_day, mc.deptmc AS dep_cut_off_time
        FROM mclp120c mc
       WHERE mc.div_part = b_div_part
         AND mc.loadc NOT IN('DIST', 'DFLT', 'LOST')
         AND (   (    (    mc.ldordc = b_last_run_day_cd
                       AND mc.ldortc >= b_last_run_time)
                  AND (   (    mc.ldordc = b_curr_day_cd
                           AND mc.ldortc < b_curr_time)
                       OR (mc.ldordc <> b_curr_day_cd))
                 )
              OR (    mc.ldordc <> b_last_run_day_cd
                  AND mc.ldordc = b_curr_day_cd
                  AND mc.ldortc < b_curr_time)
             );

    l_ord_ts             DATE;
    l_llr_ts             DATE;
    l_close_ts           DATE;
    l_depart_ts          DATE;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LastRunDay', i_last_run_day_cd);
    logs.add_parm(lar_parm, 'LastRunTime', i_last_run_time);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Loads Cursor');
    FOR l_r_load IN l_cur_loads(i_div_part, i_curr_day_cd, i_curr_time, i_last_run_day_cd, i_last_run_time) LOOP
      -- Calling procedure to convert all Cutoff day codes and times into it's corresponding timestamps
      logs.dbg('Calling CONVERT_TO_TIMESTAMP_SP');
      convert_to_timestamp_sp(l_r_load.ord_cut_off_day,
                              l_r_load.ord_cut_off_time,
                              l_r_load.llr_cut_off_day,
                              l_r_load.llr_cut_off_time,
                              l_r_load.close_cut_off_day,
                              l_r_load.close_cut_off_time,
                              l_r_load.dep_cut_off_day,
                              l_r_load.dep_cut_off_time,
                              l_ord_ts,
                              l_llr_ts,
                              l_close_ts,
                              l_depart_ts,
                              i_curr_day_cd,
                              i_curr_time,
                              i_curr_dt_str,
                              l_rc,
                              i_div_part
                             );
      -- Inserting record into TEMP table
      logs.dbg('Inserting record');

      IF l_rc <> 1 THEN
        INSERT INTO temp_load_cut_off
                    (load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
                    )
             VALUES (l_r_load.load_num, l_r_load.load_name, l_ord_ts, l_llr_ts, l_close_ts, l_depart_ts
                    );
      END IF;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END build_temp_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || BUILD_TEMP_DEP_SP
  ||
  || Builds a TEMP table having all Load data for 'Departure cutt off'
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/25/02 | Santosh | Changed calls to Check_close_day and check_close_time
  ||                    | to GET_CLOSE_DAY_TIME function. Tuned WHERE clause
  || 04/08/02 | Santosh | Modified l_cur_loads cursor to exclude 'temp' loads
  ||                    | (DFLT, DIST, LOST).
  || 04/29/02 | Santosh | Added parenthesis in the WHERE clause to correct the
  ||                    | logic
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE build_temp_dep_sp(
    i_last_run_day_cd  IN  VARCHAR2,
    i_last_run_time    IN  VARCHAR2,
    i_curr_day_cd      IN  VARCHAR2,
    i_curr_time        IN  VARCHAR2,
    i_curr_dt_str      IN  VARCHAR2,
    i_div_part         IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.BUILD_TEMP_DEP_SP';
    lar_parm             logs.tar_parm;

    -- A cursor to get all records for 'Departure cutt off'
    CURSOR l_cur_loads(
      b_div_part         NUMBER,
      b_curr_day_cd      VARCHAR2,
      b_curr_time        VARCHAR2,
      b_last_run_day_cd  VARCHAR2,
      b_last_run_time    VARCHAR2
    ) IS
      SELECT mc.loadc AS load_num, mc.destc AS load_name, mc.ldordc AS ord_cut_off_day, mc.ldortc AS ord_cut_off_time,
             mc.llrcdc AS llr_cut_off_day, mc.llrctc AS llr_cut_off_time,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'D') AS close_cut_off_day,
             get_close_day_time(mc.lccdc, mc.lcctc, mc.depdac, mc.deptmc, mc.div_part, 'T') AS close_cut_off_time,
             mc.depdac AS dep_cut_off_day, mc.deptmc AS dep_cut_off_time
        FROM mclp120c mc
       WHERE mc.div_part = b_div_part
         AND mc.loadc NOT IN('DIST', 'DFLT', 'LOST')
         AND (   (    (    mc.depdac = b_last_run_day_cd
                       AND mc.deptmc >= b_last_run_time)
                  AND (   (    mc.depdac <> b_curr_day_cd
                           AND mc.depdac = b_curr_day_cd)
                       OR (mc.deptmc < b_curr_time))
                 )
              OR (    mc.depdac <> b_last_run_day_cd
                  AND mc.depdac = b_curr_day_cd
                  AND mc.deptmc < b_curr_time)
             );

    l_ord_ts             DATE;
    l_llr_ts             DATE;
    l_close_ts           DATE;
    l_depart_ts          DATE;
    l_rc                 PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LastRunDay', i_last_run_day_cd);
    logs.add_parm(lar_parm, 'LastRunTime', i_last_run_time);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Loads Cursor');
    FOR l_r_load IN l_cur_loads(i_div_part, i_curr_day_cd, i_curr_time, i_last_run_day_cd, i_last_run_time) LOOP
      -- Calling procedure to convert all Cutoff day codes and times into it's corresponding timestamps
      logs.dbg('Calling CONVERT_TO_TIMESTAMP_SP');
      convert_to_timestamp_sp(l_r_load.ord_cut_off_day,
                              l_r_load.ord_cut_off_time,
                              l_r_load.llr_cut_off_day,
                              l_r_load.llr_cut_off_time,
                              l_r_load.close_cut_off_day,
                              l_r_load.close_cut_off_time,
                              l_r_load.dep_cut_off_day,
                              l_r_load.dep_cut_off_time,
                              l_ord_ts,
                              l_llr_ts,
                              l_close_ts,
                              l_depart_ts,
                              i_curr_day_cd,
                              i_curr_time,
                              i_curr_dt_str,
                              l_rc,
                              i_div_part
                             );
      -- Inserting record into TEMP table
      logs.dbg('Inserting record');

      IF l_rc <> 1 THEN
        INSERT INTO temp_load_cut_off
                    (load_num, load_nm, ord_cut_off_ts, llr_cut_off_ts, clos_cut_off_ts, deptur_cut_off_ts
                    )
             VALUES (l_r_load.load_num, l_r_load.load_name, l_ord_ts, l_llr_ts, l_close_ts, l_depart_ts
                    );
      END IF;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END build_temp_dep_sp;

  /*
  ||----------------------------------------------------------------------------
  || OPCSR_LOGGING_CHECK_SP
  ||
  || Procedure executes a unix script XXOPCSRLoggingCheck.scr to check whether
  || the Logging is ON, and if so, send an email to the group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/10/02 | Santosh | Original
  || 06/03/02 | Santosh | Fixed PL/SQL: numeric or value error, in
  ||                    | OPCSR_LOGGING_CHECK_SP.
  ||                    | Increase the size for v_script_name.
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed unused variables.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  || 05/13/13 | rhalpai | Change logic to call xxopCSRLoggingCheck.scr with
  ||                    | wrapper for ssh to Application Server. PIR11038
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE opcsr_logging_check_sp(
    i_div_part  IN  NUMBER,
    i_prcs_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_PROCESS_COMMON_PK.OPCSR_LOGGING_CHECK_SP';
    lar_parm             logs.tar_parm;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;

    CURSOR l_cur_mail_list(
      b_div_part  NUMBER,
      b_prcs_id   VARCHAR2
    ) IS
      SELECT ci.email_addr
        FROM prcs_grp pg, grp_cntct gc, cntct_info ci
       WHERE pg.div_part = b_div_part
         AND pg.prcs_id = b_prcs_id
         AND gc.div_part = pg.div_part
         AND gc.grp_id = pg.grp_id
         AND ci.div_part = gc.div_part
         AND ci.cntct_id = gc.cntct_id;

    l_mail_list          VARCHAR2(512);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.dbg('ENTRY', lar_parm);
    l_appl_srvr := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('Process MailList');
    FOR l_r_email IN l_cur_mail_list(i_div_part, i_prcs_id) LOOP
      l_mail_list := l_mail_list || l_r_email.email_addr || ' ';
    END LOOP;

    IF l_mail_list IS NOT NULL THEN
      l_cmd := '/local/prodcode/bin/xxopCSRLoggingCheck.scr ' || l_mail_list;
      logs.info(l_cmd);
      l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
      logs.info(l_os_result);
    END IF;   -- l_mail_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END opcsr_logging_check_sp;

  /*
  ||----------------------------------------------------------------------------
  || GEN_SQL_UTIL_ERRORS_SP
  ||
  || Used to create a report having every entry since last run, from
  || sql_utilties table for all errors (sql_type = 'E'), and notify the
  || appropriate group
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/10/02 | Santosh | Original
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Changed logic to use correct columns on sql_utilities.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_sql_util_errors_sp(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_curr_ts      IN  DATE
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_COMMON_PK.GEN_SQL_UTIL_ERRORS_SP';
    lar_parm               logs.tar_parm;

    CURSOR l_cur_errs(
      b_div          VARCHAR2,
      b_last_run_ts  DATE
    ) IS
      SELECT   u.date_occurred, u.by_user, u.LOCATION, u.sql_executed, u.error_message, u.comments
          FROM div_mstr_di1d d, sql_utilities u
         WHERE d.div_id = b_div
           AND u.div_part = d.div_part
           AND u.utility_type = 'E'
           AND u.date_occurred > b_last_run_ts
      ORDER BY u.date_occurred DESC;

    l_heading              VARCHAR2(1000);
    l_subj                 VARCHAR2(256);
    l_seq                  NUMBER(6)      := 0;
    l_t_rpt_lns            typ.tas_maxvc2;
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/opcig/oplogs/java';
    l_is_data              BOOLEAN        := FALSE;
    l_file_nm              VARCHAR2(50);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTS', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurrTS', i_curr_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || '_sql_util_errors.log';
    l_subj := i_div || ' SQL UTILITIES ERRORS Report ' || TO_CHAR(i_curr_ts, 'MM/DD/YYYY HH24:MI');
    l_heading := '  '
                 || UPPER(i_prcs_id)
                 || ' Report, Division: '
                 || i_div
                 || cnst.newline_char
                 || '     Current Time : '
                 || TO_CHAR(i_curr_ts, 'MM/DD/YYYY HH24:MI')
                 || cnst.newline_char
                 || '     Last Run Time: '
                 || TO_CHAR(i_last_run_ts, 'MM/DD/YYYY HH24:MI')
                 || cnst.newline_char
                 || cnst.newline_char;
    util.append(l_t_rpt_lns, l_heading);
    util.append(l_t_rpt_lns,
                cnst.newline_char
                || '---------------------------------------------------------------------------------'
                || cnst.newline_char
                || ' SN#  Date/Time Occured           User'
                || cnst.newline_char
                || '      Location'
                || cnst.newline_char
                || '      SQL Executed'
                || cnst.newline_char
                || '      Error Messages'
                || cnst.newline_char
                || '      Comments'
                || cnst.newline_char
                || '---------------------------------------------------------------------------------'
                || cnst.newline_char
               );
    logs.dbg('Get Error Records');
    FOR l_r_err IN l_cur_errs(i_div, i_last_run_ts) LOOP
      l_seq := l_seq + 1;
      util.append(l_t_rpt_lns,
                  SUBSTR(cnst.newline_char
                         || '  '
                         || TO_CHAR(l_seq)
                         || '.  '
                         || TO_CHAR(l_r_err.date_occurred, 'MM/DD/YYYY HH24:MI:SS.FF6')
                         || '  '
                         || l_r_err.by_user
                         || cnst.newline_char
                         || '      '
                         || l_r_err.LOCATION
                         || cnst.newline_char
                         || '      '
                         || SUBSTR(l_r_err.sql_executed, 1, LENGTH(l_r_err.sql_executed) * .20)
                         || cnst.newline_char
                         || '      '
                         || SUBSTR(l_r_err.error_message, 1, LENGTH(l_r_err.error_message) * .30)
                         || cnst.newline_char
                         || '      '
                         || SUBSTR(l_r_err.comments, 1, LENGTH(l_r_err.comments) * .80)
                         || cnst.newline_char
                         || cnst.newline_char,
                         1,
                         2000
                        )
                 );
      l_is_data := TRUE;
    END LOOP;
    logs.dbg('Closing Message');
    util.append(l_t_rpt_lns, cnst.newline_char || cnst.newline_char || ' Total Error Records = ' || l_seq);
    util.append(l_t_rpt_lns,
                cnst.newline_char || cnst.newline_char || '         ----------- End Of The Report -----------'
               );
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);

    IF l_is_data THEN
      logs.dbg('Notify Group');
      op_process_common_pk.notify_group_sp(i_div, i_prcs_id, l_subj, l_c_file_dir || '/' || l_file_nm, l_heading);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_sql_util_errors_sp;

  /*
  ||----------------------------------------------------------------------------
  || GEN_APP_LOG_ERRORS_SP
  ||
  || Used to create a report having every entry since last run, from
  || app_log table for all errors, and notify the appropriate group
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/08/16 | rhalpai | Original
  || 10/30/17 | rhalpai | Change cursor to substring first 2K characters of the
  ||                    | CLOB data in APP_LOG.EXTRA column. SDHD-206634
  ||----------------------------------------------------------------------------
  */
  PROCEDURE gen_app_log_errors_sp(
    i_div          IN  VARCHAR2,
    i_prcs_id      IN  VARCHAR2,
    i_last_run_ts  IN  DATE,
    i_curr_ts      IN  DATE
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_COMMON_PK.GEN_APP_LOG_ERRORS_SP';
    lar_parm               logs.tar_parm;

    CURSOR l_cur_errs(
      b_last_run_ts  DATE
    ) IS
      SELECT SUBSTR(LPAD(ROW_NUMBER() OVER(ORDER BY l.log_ts DESC), 3)
                    || '. '
                    || REPLACE(   -- indent
                               regexp_replace(   -- remove blank lines
                                              TO_CHAR(l.log_ts, 'YYYY-MM-DD HH24:MI:SS.FF6')
                                              || ' '
                                              || l.routine_nm
                                              || ' : '
                                              || l.line_num
                                              || cnst.newline_char
                                              || l.log_txt
                                              || cnst.newline_char
                                              || DBMS_LOB.SUBSTR(l.extra, 2000),
                                              '(^|' || cnst.newline_char || ')' || cnst.newline_char || '+',
                                              '\1'
                                             ),
                               cnst.newline_char,
                               cnst.newline_char || '     '
                              ),
                    1,
                    2000
                   ) AS rpt_ln
        FROM app_log l
       WHERE l.sev_cd = 'ERROR'
         AND l.log_ts > b_last_run_ts;

    l_heading              VARCHAR2(1000);
    l_subj                 VARCHAR2(256);
    l_cnt                  PLS_INTEGER    := 0;
    l_t_rpt_lns            typ.tas_maxvc2;
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/opcig/oplogs/java';
    l_file_nm              VARCHAR2(50);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'LastRunTS', i_last_run_ts);
    logs.add_parm(lar_parm, 'CurrTS', i_curr_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_file_nm := i_div || '_app_log_errors.log';
    l_subj := i_div || ' APP LOG ERRORS Report ' || TO_CHAR(i_curr_ts, 'MM/DD/YYYY HH24:MI');
    l_heading := '  '
                 || UPPER(i_prcs_id)
                 || ' Report'
                 || cnst.newline_char
                 || '     Current Time : '
                 || TO_CHAR(i_curr_ts, 'MM/DD/YYYY HH24:MI')
                 || cnst.newline_char
                 || '     Last Run Time: '
                 || TO_CHAR(i_last_run_ts, 'MM/DD/YYYY HH24:MI')
                 || cnst.newline_char
                 || cnst.newline_char;
    util.append(l_t_rpt_lns, l_heading);
    util.append(l_t_rpt_lns,
                cnst.newline_char
                || RPAD('-', 80, '-')
                || cnst.newline_char
                || 'SN#  Date/Time Occurred         Routine Name : Line'
                || cnst.newline_char
                || '     Log Text'
                || cnst.newline_char
                || '     Extra'
                || cnst.newline_char
                || RPAD('-', 80, '-')
                || cnst.newline_char
               );
    logs.dbg('Get Error Records');
    FOR l_r_err IN l_cur_errs(i_last_run_ts) LOOP
      l_cnt := l_cnt + 1;
      util.append(l_t_rpt_lns, l_r_err.rpt_ln);
    END LOOP;
    logs.dbg('Closing Message');
    util.append(l_t_rpt_lns, cnst.newline_char || cnst.newline_char || ' Total Error Records = ' || l_cnt);
    util.append(l_t_rpt_lns,
                cnst.newline_char || cnst.newline_char || '         ----------- End Of The Report -----------'
               );
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);

    IF l_cnt > 0 THEN
      logs.dbg('Notify Group');
      op_process_common_pk.notify_group_sp(i_div, i_prcs_id, l_subj, l_c_file_dir || '/' || l_file_nm, l_heading);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END gen_app_log_errors_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_CUTOFF_DAY
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_cutoff_day(
    i_day_num       IN  NUMBER,
    i_curr_day_num  IN  NUMBER,
    i_curr_dt_str   IN  VARCHAR2
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.GET_CUTOFF_DAY';
    lar_parm             logs.tar_parm;
    l_curr_dt            DATE;
    l_cuttoff_day        DATE;
  BEGIN
    logs.add_parm(lar_parm, 'DayNum', i_day_num);
    logs.add_parm(lar_parm, 'CurrDayNum', i_curr_day_num);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    l_curr_dt := TO_DATE(i_curr_dt_str, 'MM/DD/YYYY');
    l_cuttoff_day :=(CASE
                       WHEN i_day_num = i_curr_day_num THEN l_curr_dt
                       WHEN(i_curr_day_num - i_day_num) > 0 THEN l_curr_dt -(i_curr_day_num - i_day_num)
                       ELSE l_curr_dt -(i_curr_day_num + 7 - i_day_num)
                     END
                    );
    RETURN(l_cuttoff_day);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_cutoff_day;

  /*
  ||----------------------------------------------------------------------------
  || GET_TIMESTAMP_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/02 | Santosh | Original
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_timestamp_fn(
    i_dt    IN  DATE,
    i_time  IN  VARCHAR2
  )
    RETURN DATE IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.GET_TIMESTAMP_FN';
    lar_parm             logs.tar_parm;
    l_ts                 DATE;
  BEGIN
    logs.add_parm(lar_parm, 'Dt', i_dt);
    logs.add_parm(lar_parm, 'Time', i_time);
    l_ts := TO_DATE(TO_CHAR(i_dt, 'MM/DD/YYYY') || i_time, 'MM/DD/YYYYHH24MI');
    RETURN(l_ts);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_timestamp_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || CHECK_DAY_BETWEEN_FN
  ||
  || Check whether a day code and time is between two other day codes and times
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/24/01 | JUSTANI | Original
  || 03/01/02 | Santosh | Using fn get_day_num_of_week
  || 03/07/02 | Santosh | Changed, not to return 'N', when and in_day and to_day
  ||                    | are equal and In_time > to_time, because to_time may
  ||                    | be for next day, so now it returns 'Y'for this case
  || 03/25/02 | Santosh | tuned code for the section 'Check for Weekend
  ||                    | Cross-Over of Input and TO Day' eliminated one
  ||                    | redundant comparison operator.
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed unused variables.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION check_day_between_fn(
    i_day_cd     IN  VARCHAR2,
    i_time       IN  NUMBER,
    i_from_day   IN  VARCHAR2,
    i_from_time  IN  NUMBER,
    i_to_day     IN  VARCHAR2,
    i_to_time    IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.CHECK_DAY_BETWEEN_FN';
    lar_parm             logs.tar_parm;
    l_between_sw         VARCHAR2(1);
    l_day_num            NUMBER(2);
    l_from_day_num       NUMBER(2);
    l_to_day_num         NUMBER(2);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DayCode', i_day_cd);
    logs.add_parm(lar_parm, 'Time', i_time);
    logs.add_parm(lar_parm, 'FromDay', i_from_day);
    logs.add_parm(lar_parm, 'FromTime', i_from_time);
    logs.add_parm(lar_parm, 'ToDay', i_to_day);
    logs.add_parm(lar_parm, 'ToTime', i_to_time);
    logs.dbg('ENTRY', lar_parm);
    l_day_num := get_day_num_of_week(i_day_cd);
    l_from_day_num := get_day_num_of_week(i_from_day);
    l_to_day_num := get_day_num_of_week(i_to_day);

    -- Check for Weekend Cross-Over of Input and TO Day
    IF (    l_day_num <= l_to_day_num
        AND l_to_day_num <= l_from_day_num) THEN
      l_day_num := l_day_num + 7;
      l_to_day_num := l_to_day_num + 7;
    END IF;

    -- Check for Weekend Cross-Over of To Day Only
    IF (    l_to_day_num <= l_from_day_num
        AND l_day_num >= l_from_day_num) THEN
      l_to_day_num := l_to_day_num + 7;
    END IF;

    l_between_sw :=(CASE
                      WHEN 0 IN(l_day_num, l_from_day_num, l_to_day_num) THEN 'E'
                      WHEN(    l_day_num = l_from_day_num
                           AND i_time < i_from_time) THEN 'N'
                      WHEN l_day_num BETWEEN l_from_day_num AND l_to_day_num THEN 'Y'
                      ELSE 'N'
                    END
                   );
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_between_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_day_between_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_DAY_NUM_OF_WEEK
  ||
  || Convert Day code into day number
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/02 | Santosh | Original
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_day_num_of_week(
    i_day_cd  IN  VARCHAR2
  )
    RETURN NUMBER IS
  BEGIN
    RETURN(CASE i_day_cd
             WHEN 'SUN' THEN 1
             WHEN 'MON' THEN 2
             WHEN 'TUE' THEN 3
             WHEN 'WED' THEN 4
             WHEN 'THU' THEN 5
             WHEN 'FRI' THEN 6
             WHEN 'SAT' THEN 7
             ELSE 0
           END
          );
  END get_day_num_of_week;

  /*
  ||----------------------------------------------------------------------------
  || GET_DAY_CODE_OF_WEEK
  ||
  || Convert Day Number into day code
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Converted nested IF to CASE statement.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_day_code_of_week(
    i_day_num  IN  NUMBER
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN(CASE i_day_num
             WHEN 1 THEN 'SUN'
             WHEN 2 THEN 'MON'
             WHEN 3 THEN 'TUE'
             WHEN 4 THEN 'WED'
             WHEN 5 THEN 'THU'
             WHEN 6 THEN 'FRI'
             WHEN 7 THEN 'SAT'
             ELSE 'SAT'
           END
          );
  END get_day_code_of_week;

  /*
  ||----------------------------------------------------------------------------
  || GET_CLOSE_DAY_TIME
  ||
  || Check whether a Closing Cut off day codes is NULL
  || If yes, Closing cut off day code and time would be the difference of
  || departure cut off time minus the value of PRCTMD (in Min.) column from
  || MCLP130D for that division
  ||
  || p_type_flag indicates whether day (D) or time (T)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/02 | Santosh | Original
  || 03/07/02 | Santosh | Fixed time conversion error.(Changed 2359 to 2400)
  || 03/25/02 | Santosh | Changed it from a proc. to a function, also now
  ||                    | passing another parameter p_type_flag. When
  ||                    | p_type_flag is 'D', it will return day code else time.
  || 04/29/02 | Santosh | Added i_divison to log for Exceptions
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed unused variables.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_close_day_time(
    i_close_day_cd   IN  VARCHAR2,
    i_close_time     IN  NUMBER,
    i_depart_day_cd  IN  VARCHAR2,
    i_depart_time    IN  NUMBER,
    i_div_part       IN  NUMBER,
    i_typ            IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.GET_CLOSE_DAY_TIME';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_close_cutoff_adj   PLS_INTEGER   := 0;
    l_close_hour_adj     PLS_INTEGER   := 0;
    l_close_day_num_adj  PLS_INTEGER   := 0;
    l_close_time_adj     PLS_INTEGER   := 0;
    l_close_day_cd_adj   VARCHAR2(3);
    l_depart_day_num     NUMBER(4);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CloseDayCd', i_close_day_cd);
    logs.add_parm(lar_parm, 'CloseTime', i_close_time);
    logs.add_parm(lar_parm, 'DepartDayCd', i_depart_day_cd);
    logs.add_parm(lar_parm, 'DepartTime', i_depart_time);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Typ', i_typ);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('DEP Day Code to DEP Day Number');
    l_depart_day_num := get_day_num_of_week(i_depart_day_cd);
    logs.dbg('Getting PRCTMD');

    IF i_close_day_cd IS NULL THEN
      OPEN l_cv
       FOR
         SELECT TRUNC(d.prctmd / 60)
           FROM mclp130d d
          WHERE d.div_part = i_div_part;

      FETCH l_cv
       INTO l_close_cutoff_adj;

      CLOSE l_cv;

      logs.dbg('Getting Closing Day and Time');

      IF (    l_close_cutoff_adj >= 0
          AND l_close_cutoff_adj < 24) THEN
        l_close_hour_adj := l_close_cutoff_adj;
      ELSE
        l_close_hour_adj := MOD(l_close_cutoff_adj, 24);
      END IF;

      -- Getting Closing Cutt Off day codes and time
      IF (i_depart_time - l_close_hour_adj * 100) < 0 THEN   -- If time goes to minus
        l_close_day_num_adj := l_depart_day_num - 1;   -- it would be one prev. day
        l_close_time_adj := i_depart_time + 2400 -(l_close_hour_adj * 100);   -- and time is also of prev day
      ELSE
        l_close_day_num_adj := l_depart_day_num;
        l_close_time_adj := i_depart_time -(l_close_hour_adj * 100);
      END IF;

      l_close_day_cd_adj := get_day_code_of_week(l_close_day_num_adj);
    ELSE
      l_close_time_adj := i_close_time;
      l_close_day_cd_adj := i_close_day_cd;
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(CASE UPPER(i_typ)
             WHEN 'D' THEN l_close_day_cd_adj
             ELSE TO_CHAR(l_close_time_adj)
           END);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_close_day_time;

  /*
  ||----------------------------------------------------------------------------
  || MONITOR_PROCESS_SP
  ||
  || Procedure to find procedures running longer than the specified time,
  || calls NOTIFY_GROUP_SP procedure to send mail to the concerned group.
  ||
  || i_ctrl_m_interval is the interval for this process runnig under CTRL-M.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/02 | Santosh | Original
  || 03/15/02 | Santosh | monitor_process_sp - Changed SELECT INTO statement
  ||                    | into a cursor to monitor processes for any Logon
  ||                    | User Id.
  ||                    | Changed parameters in call to notify_group_sp to be
  ||                    | in sync with modified notify_group_sp procedure.
  || 03/22/02 | Santosh | In monitor_process_sp, refining the logic to send
  ||                    | emails, send only one email within each computed Mail
  ||                    | interval for a 'Monitored' process.
  || 06/13/02 | Sarat N | Modified monitor process to use OP tables insted of
  ||                    | Oracle System Information to determine how long an OP
  ||                    | process is running. The Oracle System information
  ||                    | represents how long a connection to the Db is active
  ||                    | and not how long a process using that connection is
  ||                    | running.
  || 07/12/02 | Sarat N | Modified logic to compute the Maximum Process
  ||                    | Execution Time allowed rather than using a hard-coded
  ||                    | values in table.
  || 10/09/02 | Sarat N | Updated Max Execution Time for Allocate process to
  ||                    | have a min value of computed max execution time for
  ||                    | 1000 order lines. This will prevent alert emails from
  ||                    | being sent for releases with very few order lines.
  || 07/02/03 | Sarat N | Modified monitor_process_sp code to use NVL function
  ||                    | when computing total number of rows in a release.
  || 11/03/03 | Sarat N | Added a System Delay value to more accurately compute
  ||                    | the Maximum Allowed time for Allocate process.
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 04/10/06 | anchakr | Changed the error email subject and body text
  ||                    | in Monitor_process_sp against the IM211605
  || 04/12/06 | anchakr | Changed v_error_rec data population  process
  ||                    | in gen_sql_util_errors_sp to reduce the substr
  ||                    | input string size
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. Convert to use
  ||                    | standard error handling logic. PIR8531
  || 04/05/17 | jxpazho | Changing the team name from Billing to OP CMS
  ||                    | in the mail message
  ||----------------------------------------------------------------------------
  */
  PROCEDURE monitor_process_sp(
    i_div              IN  VARCHAR2,
    i_ctrl_m_interval  IN  NUMBER DEFAULT 10
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.MONITOR_PROCESS_SP';
    lar_parm             logs.tar_parm;

    -- Cursor to get all processes that are monitored for a division
    CURSOR l_cur_prcs(
      b_div  VARCHAR2
    ) IS
      SELECT d.div_part, dfn.prcs_id, dfn.max_exec_time_in_mins, dfn.intrvl_in_mins, dfn.num_of_notifctn,
             dfn.last_run_ts
        FROM div_mstr_di1d d, prcs_typ_descr tdesc, prcs_dfn dfn
       WHERE d.div_id = b_div
         AND tdesc.div_part = d.div_part
         AND tdesc.prcs_typ = 'MON'
         AND dfn.div_part = d.div_part
         AND dfn.prcs_id = tdesc.prcs_id;

    l_prcs_min_run_time  PLS_INTEGER   := 0;   -- How long the processs is running
    l_extra_run_time     PLS_INTEGER   := 0;   -- Difference of v_proc_running_time_min and max_exec_time_in_min
    l_mail_msg           VARCHAR2(512);   --      Message to be sent to the groupId list
    l_start_time         DATE;   --               Start time of the process
    l_num_of_notifctn    PLS_INTEGER;
    l_ord_ln_cnt         PLS_INTEGER;
    l_max_run_time       PLS_INTEGER;
    -----------------------------------------------------------------------------------
    -- Define a variable to hold the overall system delay that include
    -- time delays encountered in calling various processes, updating database
    -- tables, ftping files, sending MQ messages, etc.. This value (in minutes)
    -- will help in estimating more accurately the time required to process orderlines.
    -----------------------------------------------------------------------------------
    l_overall_sys_delay  PLS_INTEGER   := 10;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CtrlMInterval', i_ctrl_m_interval);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Process Information');
    <<cur_prcs_loop>>
    FOR l_r_prcs IN l_cur_prcs(i_div) LOOP
      -- To get the Start time and other parameters for the specified process
      IF (UPPER(l_r_prcs.prcs_id) = 'OP_ALLOCATE_PK.ALLOCATE_SP') THEN
        BEGIN
          -- Compute running time of Allocate Process
          SELECT TRUNC((SYSDATE - r.rlse_ts) * 24 * 60), r.rlse_ts, NVL(r.ord_ln_cnt, 0)
            INTO l_prcs_min_run_time, l_start_time, l_ord_ln_cnt
            FROM rlse_op1z r
           WHERE r.div_part = l_r_prcs.div_part
             AND r.stat_cd = 'P'
             AND r.test_bil_cd = '~'
             AND 890 > (SELECT MAX(td.seq)
                          FROM rlse_log_op2z rl, rlse_typ_dmn_op9z td
                         WHERE rl.div_part = r.div_part
                           AND rl.rlse_id = r.rlse_id
                           AND td.typ_id = rl.typ_id)
             AND r.rlse_ts = (SELECT MAX(r2.rlse_ts)
                                FROM rlse_op1z r2
                               WHERE r2.div_part = l_r_prcs.div_part);
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            -- No Release in progress at this time
            l_start_time := SYSDATE;
            l_prcs_min_run_time := 0;
        END;

        -------------------------------------------------------------------------------------
        -- Compute the maximum time allowed for Allocate Process to run.
        -- The max_exec_time_in_mins column value indicates how long Allocate can
        -- run for a set of 1000 Orders (TTL_ROWS column value). The Allocate process
        -- runtime computation should only include processing time in OP servers on Unix and
        -- should not include the time to process and print labels and reports on Mainframe.
        -- Minimum value for Max Execution should be set to the time allowed for 1000 orders.
        -------------------------------------------------------------------------------------
        IF l_ord_ln_cnt < 1000 THEN
          l_max_run_time := l_overall_sys_delay + l_r_prcs.max_exec_time_in_mins;
        ELSE
          l_max_run_time := l_overall_sys_delay + CEIL(l_r_prcs.max_exec_time_in_mins * l_ord_ln_cnt / 1000);
        END IF;

        IF l_prcs_min_run_time > l_max_run_time THEN
          l_extra_run_time := l_prcs_min_run_time - l_max_run_time;
          -- If running longer, check it should not send mail more than the no_of_notifications
          l_mail_msg := 'OP Order Release (Allocate) exceeded the Maximum Allowed Execution Time of '
                        || l_max_run_time
                        || ' minutes. It has been running for more than '
                        || l_prcs_min_run_time
                        || ' minutes. '
                        || 'Open a Priority P4 problem and assign it to the MIS OP/CMS team.';
          l_num_of_notifctn := l_r_prcs.num_of_notifctn;
          logs.dbg('Sending Mail');
          <<mail_loop>>
          LOOP
            -- Find where does v_extra_running_time  fits in the mail interval
            -- Mail interval would be (v_no_of_notify - 1 ) * l_r_prcs.intrvl_in_mins
            -- to  v_no_of_notify * l_r_prcs.intrvl_in_mins
            -- For v_no_of_notify = 3 , and   l_r_prcs.intrvl_in_mins = 20,  there
            -- would be three interval (0-20, 20 -40, 40-60)\
            -- Send only one email during these interval, independent of  p_Ctrl_M_interval
            -- that is accomplished by 'AND v_extra_running_time < (v_no_of_notify - 1 ) * l_r_prcs.intrvl_in_mins + p_Ctrl_M_interval' clause
            IF (    l_extra_run_time >= (l_num_of_notifctn - 1) * l_r_prcs.intrvl_in_mins
                AND l_extra_run_time <= l_num_of_notifctn * l_r_prcs.intrvl_in_mins
                AND l_extra_run_time < (l_num_of_notifctn - 1) * l_r_prcs.intrvl_in_mins + i_ctrl_m_interval
               ) THEN
              -- Calling notify_group procedure to send notification
              notify_group_sp(i_div,
                              l_r_prcs.prcs_id,
                              i_div || ' OP Set Release has exceeded the Maximum Allowed Execution Time',
                              l_mail_msg
                             );
              EXIT;
            END IF;

            l_num_of_notifctn := l_num_of_notifctn - 1;
            EXIT mail_loop WHEN l_num_of_notifctn = 0;
          END LOOP mail_loop;
        END IF;
      ELSE
        logs.err('Monitor process not implemented for specified process: ' || l_r_prcs.prcs_id, lar_parm, NULL, FALSE);
      END IF;

      logs.dbg('Updating PRCS_DFN');

      -- Updating Last Run Time Stamp
      UPDATE prcs_dfn
         SET last_run_ts = l_start_time
       WHERE div_part = l_r_prcs.div_part
         AND prcs_id = l_r_prcs.prcs_id;

      COMMIT;
    END LOOP cur_prcs_loop;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END monitor_process_sp;

  /*
  ||----------------------------------------------------------------------------
  || REPORT_PROCESS_COMMON_SP
  ||
  || Common Module to Check which process to run at this moment of time and run
  || that to generate a report, and sends email to the corresponding group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/05/02 | Santosh | Original
  || 03/14/02 | Santosh | Changed to execute all waiting processes one by one.
  ||                    | Earlier it was executing only the process that is
  ||                    | waiting for longest period of time out of all waiting
  ||                    | processes.
  || 04/12/02 | Santosh | Replaced the direct call of op_dup_order_check_sp
  ||                    | procedure with new proc. duplicate_order_sp, which in
  ||                    | turn calling op_dup_order_check_sp,
  || 05/10/02 | Santosh | Modified to include opcsr_logging_check_sp and
  ||                    | gen_sql_util_errors_sp
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed unused variables.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE report_process_common_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.REPORT_PROCESS_COMMON_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_curr_day_cd         VARCHAR2(3);   -- Current day code
    l_curr_time           VARCHAR2(4);
    l_last_run_day_cd     VARCHAR2(3);
    l_last_run_time       VARCHAR2(4);
    l_curr_dt_str         VARCHAR2(10);

    -- Cursor to get info for 'REP' type processes
    CURSOR l_cur_prcs(
      b_div_part  NUMBER,
      b_ts        DATE
    ) IS
      SELECT dfn.ROWID AS row_id, dfn.prcs_id, dfn.prcs_sbtyp_cd, dfn.last_run_ts, dfn.cut_off_adjstmnt_in_mins,
             dfn.last_run_ts + NUMTODSINTERVAL(dfn.intrvl_in_mins, 'minute') AS nxt_run_ts
        FROM prcs_typ_descr tdesc, prcs_dfn dfn
       WHERE tdesc.div_part = b_div_part
         AND tdesc.prcs_typ = 'REP'
         AND dfn.div_part = tdesc.div_part
         AND dfn.prcs_id = tdesc.prcs_id
         AND dfn.last_run_ts + NUMTODSINTERVAL(dfn.intrvl_in_mins, 'minute') <= b_ts;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_curr_dt_str := TO_CHAR(l_c_sysdate, 'MM/DD/YYYY');
    l_curr_day_cd := TO_CHAR(l_c_sysdate, 'DY');
    l_curr_time := TO_CHAR(l_c_sysdate, 'HH24MI');
    -- Checking for all 'REP' type processes
    logs.dbg('Process Cursor');
    FOR l_r_prcs IN l_cur_prcs(l_div_part, l_c_sysdate) LOOP
      -- If next run timestamp has passed the current time, but it's not waiting for more than
      -- 23 hours, then that process need to run
      logs.dbg('Check Processes');

      -- Checking for other processes, not constrained by last 24 hour period
      CASE l_r_prcs.prcs_sbtyp_cd
        WHEN 'LOG' THEN
          opcsr_logging_check_sp(l_div_part, l_r_prcs.prcs_id);
        WHEN 'UTL' THEN
          gen_sql_util_errors_sp(i_div, l_r_prcs.prcs_id, l_r_prcs.last_run_ts, l_c_sysdate);
        WHEN 'ERR' THEN
          gen_app_log_errors_sp(i_div, l_r_prcs.prcs_id, l_r_prcs.last_run_ts, l_c_sysdate);
        ELSE
          NULL;
      END CASE;

      IF l_r_prcs.nxt_run_ts >= l_c_sysdate - INTERVAL '23' HOUR THEN
        -- Getting day code and time of last run timestamp
        l_last_run_day_cd := TO_CHAR(l_r_prcs.last_run_ts, 'DY');
        l_last_run_time := TO_CHAR(l_r_prcs.last_run_ts, 'HH24MI');
        -- Check the value of the cuttoff code for the process and call corresponding procedure
        -- to build the TEMP table
        logs.dbg('Building TEMP Table');

        CASE l_r_prcs.prcs_sbtyp_cd
          WHEN 'O-L' THEN
            build_temp_o_l_sp(l_curr_day_cd, l_curr_time, l_curr_dt_str, l_div_part);
          WHEN 'L-C' THEN
            build_temp_l_c_sp(l_curr_day_cd, l_curr_time, l_curr_dt_str, l_div_part);
          WHEN 'LLR' THEN
            build_temp_llr_sp(l_last_run_day_cd, l_last_run_time, l_curr_day_cd, l_curr_time, l_curr_dt_str,
                              l_div_part);
          WHEN 'CLO' THEN
            build_temp_clo_sp(l_last_run_day_cd, l_last_run_time, l_curr_day_cd, l_curr_time, l_curr_dt_str,
                              l_div_part);
          WHEN 'ORD' THEN
            build_temp_ord_sp(l_last_run_day_cd, l_last_run_time, l_curr_day_cd, l_curr_time, l_curr_dt_str,
                              l_div_part);
          WHEN 'DEP' THEN
            build_temp_dep_sp(l_last_run_day_cd, l_last_run_time, l_curr_day_cd, l_curr_time, l_curr_dt_str,
                              l_div_part);
          WHEN 'DUP' THEN
            duplicate_order_sp(l_r_prcs.last_run_ts, i_div);
          ELSE
            NULL;
        END CASE;

        -- Call the procedure to run the report and send emails to the group
        logs.dbg('Generate Reports');
        op_process_reports_pk.gen_reports_sp(i_div,
                                             l_r_prcs.prcs_id,
                                             l_r_prcs.last_run_ts,
                                             l_c_sysdate,
                                             l_r_prcs.cut_off_adjstmnt_in_mins
                                            );
      END IF;

      logs.dbg('Update - PRCS_DFN');

      -- Update the last_run_ts with previously stored current timestamp
      UPDATE prcs_dfn
         SET last_run_ts = l_c_sysdate
       WHERE ROWID = l_r_prcs.row_id;

      COMMIT;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END report_process_common_sp;

  /*
  ||----------------------------------------------------------------------------
  || NOTIFY_GROUP_SP
  ||
  || Procedure to notify the group thru e-mail
  ||
  || p_group_id is the group id to send the mail
  || p_mail_msg is the message to be sent to the group
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 03/15/02 | Santosh | Changed passing parameters from p_group_id to
  ||                    | i_prcs_id and i_division
  || 04/18/02 | Santosh | Fixed Duplicate Mail notification, Moved the call to
  ||                    | sql_utilities_pkg.send_mail out of the loop.
  || 05/14/02 | Santosh | Changed l_cur_prcs_grps and l_cur_mail_list
  || 10/02/02 | Sarat N | Changed v_mail_list variable size to 4000 to handle
  ||                    | more emailId and match the size in
  ||                    | sql_utilities.send_mail proc.
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed unused variable.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE notify_group_sp(
    i_div         IN  VARCHAR2,
    i_prcs_id     IN  VARCHAR2,
    i_subj        IN  VARCHAR2 DEFAULT 'Process Monitor and Reporting System Mail',
    i_mail_msg1   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg2   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg3   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg4   IN  VARCHAR2 DEFAULT NULL,
    i_mail_msg5   IN  VARCHAR2 DEFAULT NULL,
    i_reply_addr  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_COMMON_PK.NOTIFY_GROUP_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_mail_list          VARCHAR2(4000);

    CURSOR l_cur_mail_list(
      b_div_part  NUMBER,
      b_prcs_id   VARCHAR2
    ) IS
      SELECT ci.email_addr
        FROM prcs_grp pg, grp_cntct gc, cntct_info ci
       WHERE pg.div_part = b_div_part
         AND pg.prcs_id = b_prcs_id
         AND gc.div_part = pg.div_part
         AND gc.grp_id = pg.grp_id
         AND ci.div_part = gc.div_part
         AND ci.cntct_id = gc.cntct_id;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'Subj', i_subj);
    logs.add_parm(lar_parm, 'MailMsg1', i_mail_msg1);
    logs.add_parm(lar_parm, 'MailMsg2', i_mail_msg2);
    logs.add_parm(lar_parm, 'MailMsg3', i_mail_msg3);
    logs.add_parm(lar_parm, 'MailMsg4', i_mail_msg4);
    logs.add_parm(lar_parm, 'MailMsg5', i_mail_msg5);
    logs.add_parm(lar_parm, 'ReplyAddr', i_reply_addr);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get Email List');
    FOR l_r_email IN l_cur_mail_list(l_div_part, i_prcs_id) LOOP
      l_mail_list := l_mail_list || l_r_email.email_addr || ', ';
    END LOOP;

    IF l_mail_list IS NOT NULL THEN
      logs.dbg('Send Mail');
      sql_utilities_pkg.send_mail(l_mail_list,
                                  i_subj,
                                  i_mail_msg1,
                                  i_mail_msg2,
                                  i_mail_msg3,
                                  i_mail_msg4,
                                  i_mail_msg5,
                                  i_reply_addr
                                 );
    END IF;   -- l_mail_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END notify_group_sp;

  /*
  ||----------------------------------------------------------------------------
  || CONVERT_TO_TIMESTAMP_SP
  ||
  || Converts all cutoff (Order, LLR, Close, Dep) day codes and times into it's
  || corresponding timestamps.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/11/02 | Santosh | Original
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed unused variables.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE convert_to_timestamp_sp(
    i_ord_day_cd     IN      VARCHAR2,
    i_ord_time       IN      NUMBER,
    i_llr_day_cd     IN      VARCHAR2,
    i_llr_time       IN      NUMBER,
    i_close_day_cd   IN      VARCHAR2,
    i_close_time     IN      NUMBER,
    i_depart_day_cd  IN      VARCHAR2,
    i_depart_time    IN      NUMBER,
    o_ord_ts         OUT     DATE,
    o_llr_ts         OUT     DATE,
    o_close_ts       OUT     DATE,
    o_depart_ts      OUT     DATE,
    i_curr_day_cd    IN      VARCHAR2,
    i_curr_time      IN      VARCHAR2,
    i_curr_dt_str    IN      VARCHAR2,
    o_rc             OUT     NUMBER,
    i_div_part       IN      NUMBER
  ) IS
    -- Variable declaration
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_COMMON_PK.CONVERT_TO_TIMESTAMP_SP';
    lar_parm             logs.tar_parm;
    l_curr_day_num       NUMBER(1);
    l_ord_day_num        NUMBER(1);
    l_llr_day_num        NUMBER(1);
    l_close_day_num      NUMBER(1);
    l_depart_day_num     NUMBER(1);
    l_ord_day            DATE;
    l_llr_day            DATE;
    l_close_day          DATE;
    l_depart_day         DATE;
    l_ord_time           VARCHAR2(4);
    l_llr_time           VARCHAR2(4);
    l_close_time         VARCHAR2(4);
    l_depart_time        VARCHAR2(4);
    l_is_check           BOOLEAN       := TRUE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'OrdDayCd', i_ord_day_cd);
    logs.add_parm(lar_parm, 'OrdTime', i_ord_time);
    logs.add_parm(lar_parm, 'LlrDayCd', i_llr_day_cd);
    logs.add_parm(lar_parm, 'LlrTime', i_llr_time);
    logs.add_parm(lar_parm, 'CloseDayCd', i_close_day_cd);
    logs.add_parm(lar_parm, 'CloseTime', i_close_time);
    logs.add_parm(lar_parm, 'DepartDayCd', i_depart_day_cd);
    logs.add_parm(lar_parm, 'DepartTime', i_depart_time);
    logs.add_parm(lar_parm, 'CurrDayCd', i_curr_day_cd);
    logs.add_parm(lar_parm, 'CurrTime', i_curr_time);
    logs.add_parm(lar_parm, 'CurrDtStr', i_curr_dt_str);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.dbg('ENTRY', lar_parm);
    -- Converting day codes into numeric week days number staring from SUN as 1
    logs.dbg('Converting day codes into numeric week days');
    o_rc := 0;
    o_ord_ts := NULL;
    o_llr_ts := NULL;
    o_close_ts := NULL;
    o_depart_ts := NULL;
    l_ord_day_num := get_day_num_of_week(i_ord_day_cd);
    l_llr_day_num := get_day_num_of_week(i_llr_day_cd);
    l_close_day_num := get_day_num_of_week(i_close_day_cd);
    l_depart_day_num := get_day_num_of_week(i_depart_day_cd);
    l_curr_day_num := get_day_num_of_week(i_curr_day_cd);

    IF (   l_ord_day_num = 0
        OR l_llr_day_num = 0
        OR l_close_day_num = 0
        OR l_depart_day_num = 0
        OR l_curr_day_num = 0) THEN
      o_rc := 1;
      GOTO end_of_module;
    END IF;

    logs.dbg('Converting times');
    l_ord_time :=(LPAD(i_ord_time, 4, 0));
    l_llr_time :=(LPAD(i_llr_time, 4, 0));
    l_close_time :=(LPAD(i_close_time, 4, 0));
    l_depart_time :=(LPAD(i_depart_time, 4, 0));
    -- If current date is in between Order and LLR cutoffs
    logs.dbg('Between Order and LLR cutoffs');

    IF (    l_is_check
        AND check_day_between_fn(i_curr_day_cd, i_curr_time, i_ord_day_cd, i_ord_time, i_llr_day_cd, i_llr_time) = 'Y'
       ) THEN
      -- Getting Order cutoff day (without time)
      l_ord_day := get_cutoff_day(l_ord_day_num, l_curr_day_num, i_curr_dt_str);
      -- Getting Order cutoff timestamp
      o_ord_ts := get_timestamp_fn(l_ord_day, l_ord_time);

      -- Getting other timestamp, calculating using Order cutoff day and differennce of day with
      -- other cutoffs , and then adding corresponding time
      IF (l_llr_day_num - l_ord_day_num) < 0 THEN
        l_llr_day := l_ord_day +(l_llr_day_num + 7 - l_ord_day_num);
      ELSE
        l_llr_day := l_ord_day +(l_llr_day_num - l_ord_day_num);
      END IF;

      o_llr_ts := get_timestamp_fn(l_llr_day, l_llr_time);

      IF (l_close_day_num - l_llr_day_num) < 0 THEN
        l_close_day := l_llr_day +(l_close_day_num + 7 - l_llr_day_num);
      ELSE
        l_close_day := l_llr_day +(l_close_day_num - l_llr_day_num);
      END IF;

      o_close_ts := get_timestamp_fn(l_close_day, l_close_time);

      IF (l_depart_day_num - l_close_day_num) < 0 THEN
        l_depart_day := l_close_day +(l_depart_day_num + 7 - l_close_day_num);
      ELSE
        l_depart_day := l_close_day +(l_depart_day_num - l_close_day_num);
      END IF;

      o_depart_ts := get_timestamp_fn(l_depart_day, l_depart_time);
      l_is_check := FALSE;
    END IF;

    -- If current date is in between LLR and Close cutoffs
    logs.dbg('Between LLR and Close cutoffs');

    IF (    l_is_check
        AND check_day_between_fn(i_curr_day_cd, i_curr_time, i_llr_day_cd, i_llr_time, i_close_day_cd, i_close_time) =
                                                                                                                     'Y'
       ) THEN
      -- Getting LLR cutoff day (without time)

      -- Getting Order cutoff day (without time)
      l_llr_day := get_cutoff_day(l_llr_day_num, l_curr_day_num, i_curr_dt_str);
      -- Getting Order cutoff timestamp
      o_llr_ts := get_timestamp_fn(l_llr_day, l_llr_time);

      -- Getting other timestamp, calculating using Order cutoff day and differennce of day with
      -- other cutoffs , and then adding corresponding time
      IF (l_close_day_num - l_llr_day_num) < 0 THEN
        l_close_day := l_llr_day +(l_close_day_num + 7 - l_llr_day_num);
      ELSE
        l_close_day := l_llr_day +(l_close_day_num - l_llr_day_num);
      END IF;

      o_close_ts := get_timestamp_fn(l_close_day, l_close_time);

      IF (l_depart_day_num - l_close_day_num) < 0 THEN
        l_depart_day := l_close_day +(l_depart_day_num + 7 - l_close_day_num);
      ELSE
        l_depart_day := l_close_day +(l_depart_day_num - l_close_day_num);
      END IF;

      o_depart_ts := get_timestamp_fn(l_depart_day, l_depart_time);

      IF (l_llr_day_num - l_ord_day_num) < 0 THEN
        l_ord_day := l_llr_day -(l_llr_day_num + 7 - l_ord_day_num);
      ELSE
        l_ord_day := l_llr_day -(l_llr_day_num - l_ord_day_num);
      END IF;

      o_ord_ts := get_timestamp_fn(l_ord_day, l_ord_time);
      l_is_check := FALSE;
    END IF;

    -- If current date is in between Close and Dep cutoffs
    logs.dbg('Between LLR and Close cutoffs');

    IF (    l_is_check
        AND check_day_between_fn(i_curr_day_cd,
                                 i_curr_time,
                                 i_close_day_cd,
                                 i_close_time,
                                 i_depart_day_cd,
                                 i_depart_time
                                ) = 'Y'
       ) THEN
      -- Getting Close cutoff day (without time)

      -- Getting Order cutoff day (without time)
      l_close_day := get_cutoff_day(l_close_day_num, l_curr_day_num, i_curr_dt_str);
      -- Getting Order cutoff timestamp
      o_close_ts := get_timestamp_fn(l_close_day, l_close_time);

      -- Getting other timestamp, calculating using Order cutoff day and differennce of day with
      -- other cutoffs , and then adding corresponding time
      IF (l_depart_day_num - l_close_day_num) < 0 THEN
        l_depart_day := l_close_day +(l_depart_day_num + 7 - l_close_day_num);
      ELSE
        l_depart_day := l_close_day +(l_depart_day_num - l_close_day_num);
      END IF;

      o_depart_ts := get_timestamp_fn(l_depart_day, l_depart_time);

      IF (l_close_day_num - l_llr_day_num) < 0 THEN
        l_llr_day := l_close_day -(l_close_day_num + 7 - l_llr_day_num);
      ELSE
        l_llr_day := l_close_day -(l_close_day_num - l_llr_day_num);
      END IF;

      o_llr_ts := get_timestamp_fn(l_llr_day, l_llr_time);

      IF (l_llr_day_num - l_ord_day_num) < 0 THEN
        l_ord_day := l_llr_day -(l_llr_day_num + 7 - l_ord_day_num);
      ELSE
        l_ord_day := l_llr_day -(l_llr_day_num - l_ord_day_num);
      END IF;

      o_ord_ts := get_timestamp_fn(l_ord_day, l_ord_time);
    END IF;

    <<end_of_module>>
    NULL;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END convert_to_timestamp_sp;

  /*
  ||----------------------------------------------------------------------------
  || DUPLICATE_ORDER_SP
  ||
  || Calls duplicate order check procedure op_dup_order_check_sp and logs the
  || output to a File.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/12/02 | Santosh | Original
  || 05/09/02 | Santosh | Modified to create log file, only when the new
  ||                    | parameter v_flag being passed from
  ||                    | op_dup_order_check_sp is TRUE.
  || 01/26/05 | rhalpai | Updated to standard format/naming convention.
  ||                    | Removed debugging print statements.
  ||                    | Changed error handler to new standard format.
  || 05/20/05 | rhalpai | Replaced call to OP_DUP_ORDER_CHECK_SP with call to
  ||                    | OP_ORDER_VALIDATION_PK.DUP_ORDER_CHECK_SP. IM149431
  || 08/26/10 | rhalpai | Convert to use standard error handling logic. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE duplicate_order_sp(
    i_last_run_ts  IN  DATE,
    i_div          IN  VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_PROCESS_COMMON_PK.DUPLICATE_ORDER_SP';
    lar_parm               logs.tar_parm;
    l_dupl_ord_msg         VARCHAR2(5000);
    l_dupl_adj_mins        PLS_INTEGER;
    l_is_dupl_found        BOOLEAN        := FALSE;
    l_file_nm              VARCHAR2(50);
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/opcig/oplogs/java';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LastRunTS', i_last_run_ts);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_dupl_adj_mins := TRUNC((SYSDATE - i_last_run_ts) * 24 * 60) + 5;
    logs.dbg('Duplicate Order Check');
    op_order_validation_pk.dup_order_check_sp(SYSDATE, l_dupl_adj_mins, i_div, l_dupl_ord_msg, l_is_dupl_found);

    IF l_is_dupl_found THEN
      l_file_nm := i_div || '_dup_order_check_sp.log_' || TO_CHAR(SYSDATE, 'MMDDYYYYHHMI');
      logs.dbg('Write File');
      io.write_line(l_dupl_ord_msg, l_file_nm, l_c_file_dir, 'W');
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END duplicate_order_sp;
END op_process_common_pk;
/

