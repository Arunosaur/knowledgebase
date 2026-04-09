CREATE OR REPLACE PACKAGE op_mq_message_pk IS
  /**
  ||----------------------------------------------------------------------------
  || Package with functionality for storing and retrieving MQ Messages in
  || MCLANE_MQ_GP_CONTROL, MCLANE_MQ_GET and MCLANE_MQ_PUT tables.
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
  FUNCTION mq_put_msgs_fn(
    i_div      IN  VARCHAR2,
    i_msg_id   IN  VARCHAR2,
    i_corr_id  IN  NUMBER
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  /**
  ||----------------------------------------------------------------------------
  || Route MQ messages on MCLANE_MQ_PUT to the mainframe via external process.
  || #param i_msg_id           MQ message ID.
  || #param i_div              Division ID.
  || #param i_corr_id          Correlation ID.
  || #param o_rc               Returned status code.
  || #param i_max_attempts     Max number of attempts to make to put msgs to MQ.
  || #param i_wait_secs        Number of seconds to wait between re-attempts.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE mq_put_sp(
    i_msg_id        IN      VARCHAR2,
    i_div           IN      VARCHAR2,
    i_corr_id       IN      NUMBER,
    o_rc            OUT     NUMBER,
    i_max_attempts  IN      PLS_INTEGER DEFAULT 3,
    i_wait_secs     IN      PLS_INTEGER DEFAULT 5
  );

  /**
  ||----------------------------------------------------------------------------
  || Add record to MCLANE_MQ_GET table.
  || #param i_data             MQ message data.
  || #param i_msg_id           MQ message ID.
  || #param i_div              Division ID.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE mq_get_proc(
    i_data    IN  VARCHAR2,
    i_msg_id  IN  VARCHAR2,
    i_div     IN  VARCHAR2
  );

  /**
  ||----------------------------------------------------------------------------
  || Change status to complete for record to MCLANE_MQ_PUT table.
  || #param i_msg_id           MQ message ID.
  || #param i_div              Division ID.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE mq_put_proc(
    i_msg_id  IN  VARCHAR2,
    i_div     IN  VARCHAR2
  );

  /**
  ||----------------------------------------------------------------------------
  || Get divisional MQ Get/Put Control info for message type.
  || #param i_msg_id           MQ message ID.
  || #param o_mq_queue         Returned MQ queue name.
  || #param o_mq_qmanager      Returned MQ queue manager.
  || #param o_descr            Returned description.
  || #param o_hdr_data         Returned MQ header data.
  || #param o_trans_mode       Returned transmit mode.
  || #param o_dtl_sw           Returned detail switch.
  || #param i_div              Division ID.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE mq_gp_control_proc(
    i_msg_id       IN      VARCHAR2,
    o_mq_queue     OUT     VARCHAR2,
    o_mq_qmanager  OUT     VARCHAR2,
    o_descr        OUT     VARCHAR2,
    o_hdr_data     OUT     VARCHAR2,
    o_trans_mode   OUT     VARCHAR2,
    o_dtl_sw       OUT     VARCHAR2,
    i_div          IN      VARCHAR2
  );

  /**
  ||----------------------------------------------------------------------------
  || Retrieve MQ connection information.
  || #param i_div                Division ID.
  || #param i_msg_id             MQ message ID.
  || #param o_mq_queue           Returned MQ queue name.
  || #param o_host_nm            Returned Unix host name.
  || #param o_mq_channel         Returned MQ channel.
  || #param o_mq_qmanager        Returned MQ queue manager.
  || #param o_mq_port            Returned MQ connection port.
  || #param o_concurrent_threads Returned number of concurrent threads for execution.
  || #param o_max_prcs_attmpt    Returned maximum number of process attempts.
  || #param o_cmnd               Returned command to process.
  || #param o_cmnd_typ           Returned command type.
  ||                             {*} 'D' Database
  ||                             {*} 'U' Unix
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE get_mq_connection_info_sp(
    i_div                 IN      VARCHAR2,
    i_msg_id              IN      VARCHAR2,
    o_mq_queue            OUT     VARCHAR2,
    o_host_nm             OUT     VARCHAR2,
    o_mq_channel          OUT     VARCHAR2,
    o_mq_qmanager         OUT     VARCHAR2,
    o_mq_port             OUT     NUMBER,
    o_concurrent_threads  OUT     NUMBER,
    o_max_prcs_attmpt     OUT     NUMBER,
    o_cmnd                OUT     VARCHAR2,
    o_cmnd_typ            OUT     VARCHAR2
  );

  PROCEDURE ins_get_msg_sp(
    i_div       IN  VARCHAR2,
    i_msg_id    IN  VARCHAR2,
    i_msg_data  IN  VARCHAR2
  );

  PROCEDURE upd_put_msg_stat_sp(
    i_div          IN  VARCHAR2,
    i_msg_id       IN  VARCHAR2,
    i_corr_id      IN  NUMBER,
    i_new_stat_cd  IN  VARCHAR2,
    i_put_id       IN  NUMBER DEFAULT NULL
  );
END op_mq_message_pk;
/

CREATE OR REPLACE PACKAGE BODY op_mq_message_pk IS
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/01 | Fei Wu  | Original
  || 12/06/01 | SNAGABH | Renamed from MCLANE_MESSAGE_PK to OP_MQ_MESSAGE_PK
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
  || MQ_PUT_MSGS_FN
  ||  Retrieve matching MQ PUT msgs.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/13/13 | rhalpai | Original
  || 11/20/24 | rhalpai | Change logic to sort properly when sequence wraps within a unit of work. SDHD-2102544
  ||----------------------------------------------------------------------------
  */
  FUNCTION mq_put_msgs_fn(
    i_div      IN  VARCHAR2,
    i_msg_id   IN  VARCHAR2,
    i_corr_id  IN  NUMBER
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MQ_MESSAGE_PK.MQ_PUT_MSGS_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'CorrId', i_corr_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   x.mq_msg_data, x.mq_put_id
           FROM all_sequences s,
                (SELECT p.mq_msg_data, p.mq_put_id,
                        MIN(p.mq_put_id) OVER(ORDER BY p.mq_put_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_id,
                        MAX(p.mq_put_id) OVER(ORDER BY p.mq_put_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_id
           FROM div_mstr_di1d d, mclane_mq_put p
          WHERE d.div_id = i_div
            AND p.div_part = d.div_part
            AND p.mq_msg_id = i_msg_id
            AND p.mq_corr_put_id = i_corr_id
                    AND p.mq_msg_status = 'OPN') x
          WHERE s.sequence_owner = 'OP'
            AND s.sequence_name = 'MQ_PUT_ID_SEQ'
       ORDER BY (CASE
                   WHEN     x.max_id >= s.max_value - 100000
                        AND x.min_id <= 100000 THEN(CASE
                                                      WHEN x.mq_put_id <= 100000 THEN x.mq_put_id + x.max_id
                                                      ELSE x.mq_put_id
                                                    END
                                                   )
                   ELSE x.mq_put_id
                 END
                );

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mq_put_msgs_fn;

  /*
  ||----------------------------------------------------------------------------
  || MQ_PUT_SP
  ||   Route MQ messages on MCLANE_MQ_PUT to the mainframe via external process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/01 | Fei Wu  | Original
  || 07/13/05 | rhalpai | Changed error handler to new standard format. PIR2051
  || 08/12/05 | rhalpai | Changed to handle a NULL "Correlation ID" parm.
  ||                    | This is done to allow calling the java process within
  ||                    | the Unix script without the correlation ID. Calling it
  ||                    | with a correlation ID treats all records as one msg
  ||                    | as done for orders (i.e. QOCOL01, CSRSEND) while
  ||                    | calling it without a correlation ID processes all
  ||                    | records as individual msgs. PIR2608
  || 08/21/07 | rhalpai | Added max_attempts and wait_secs parms as well as the
  ||                    | logic to utilize them to re-attempt sending msgs
  ||                    | to MQ upon failure. IM330962
  || 03/15/12 | rhalpai | Change logic to use OSCMD_FN. PIR11038
  || 05/13/13 | rhalpai | Change logic to call xxopMQPUT.scr with wrapper for
  ||                    | ssh to Application Server. PIR11038
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_put_sp(
    i_msg_id        IN      VARCHAR2,
    i_div           IN      VARCHAR2,
    i_corr_id       IN      NUMBER,
    o_rc            OUT     NUMBER,
    i_max_attempts  IN      PLS_INTEGER DEFAULT 3,
    i_wait_secs     IN      PLS_INTEGER DEFAULT 5
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                       := 'OP_MQ_MESSAGE_PK.MQ_PUT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_max_attempts       PLS_INTEGER                         := NVL(i_max_attempts, 3);
    l_wait_secs          PLS_INTEGER                         := NVL(i_wait_secs, 5);
    l_cnt                PLS_INTEGER                         := 0;
    l_log                VARCHAR2(100);
    l_cmd                typ.t_maxvc2;
    l_os_result          typ.t_maxvc2;
    l_appl_srvr          appl_sys_parm_ap1s.vchar_val%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CorrId', i_corr_id);
    logs.add_parm(lar_parm, 'MaxAttempts', i_max_attempts);
    logs.add_parm(lar_parm, 'WaitSecs', i_wait_secs);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_log := '/oplogs/java/' || i_div || '_' || i_msg_id || '_' || TO_CHAR(SYSDATE, 'mmddyyyy') || '.log';
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_cmd := '/local/prodcode/bin/xxopMQPUT.scr PUT '
             || UPPER(i_msg_id)
             || ' '
             || UPPER(i_div)
             || ' '
             || i_corr_id
             || ' | tee -a '
             || l_log;

    IF l_max_attempts < 1 THEN
      l_max_attempts := 1;
    END IF;   -- l_max_attempts < 1

    IF l_wait_secs < 0 THEN
      l_wait_secs := 0;
    END IF;   -- l_wait_secs < 0

    LOOP
      l_cnt := l_cnt + 1;
      logs.info(l_cmd);
      l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
      logs.info(l_os_result);
      logs.dbg('Check completion status');

      SELECT NVL(MAX(1), 0)
        INTO o_rc
        FROM div_mstr_di1d d, mclane_mq_put p
       WHERE d.div_id = i_div
         AND p.div_part = d.div_part
         AND p.mq_msg_id = i_msg_id
         AND p.mq_msg_status = 'OPN'
         AND (   i_corr_id IS NULL
              OR p.mq_corr_put_id = i_corr_id);

      EXIT WHEN(   o_rc = 0
                OR l_cnt >= l_max_attempts);
      logs.warn(LTRIM(l_os_result), lar_parm);
      logs.warn('Attempt ' || l_cnt || ' of ' || l_max_attempts || ' to put to MQ failed', lar_parm);
      DBMS_LOCK.sleep(l_wait_secs);
    END LOOP;

    IF o_rc <> 0 THEN
      logs.warn('All ' || l_max_attempts || ' attempts to put to MQ have failed', lar_parm);
    END IF;   -- i_rc <> 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mq_put_sp;

  /*
  ||----------------------------------------------------------------------------
  || MQ_GET_PROC
  ||   Add record to MCLANE_MQ_GET table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/01 | Fei Wu  | Original
  || 07/13/05 | rhalpai | Changed error handler to new standard format. PIR2051
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_get_proc(
    i_data    IN  VARCHAR2,
    i_msg_id  IN  VARCHAR2,
    i_div     IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MQ_MESSAGE_PK.MQ_GET_PROC';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Data', i_data);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    INSERT INTO mclane_mq_get
                (mq_msg_id, mq_msg_data, mq_msg_status, div_part)
      SELECT i_msg_id, i_data, 'OPN', d.div_part
        FROM div_mstr_di1d d
       WHERE d.div_id = i_div;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END mq_get_proc;

  /*
  ||----------------------------------------------------------------------------
  || MQ_PUT_PROC
  ||   Change status to complete for record to MCLANE_MQ_PUT table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/01 | Fei Wu  | Original
  || 07/13/05 | rhalpai | Converted update within cursor loop to a single update.
  ||                    | Changed error handler to new standard format. PIR2051
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_put_proc(
    i_msg_id  IN  VARCHAR2,
    i_div     IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MQ_MESSAGE_PK.MQ_PUT_PROC';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    UPDATE mclane_mq_put
       SET mq_msg_status = 'CMP'
     WHERE mq_msg_status = 'OPN'
       AND div_part = (SELECT div_part
                         FROM div_mstr_di1d
                        WHERE div_id = i_div)
       AND mq_msg_id = i_msg_id;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END mq_put_proc;

  /*
  ||----------------------------------------------------------------------------
  || MQ_GP_CONTROL_PROC
  ||   Get divisional MQ Get/Put Control info for message ID
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/01 | Fei Wu  | Original
  || 10/02/01 | rhalpai | Replaced "SELECT *"
  || 07/13/05 | rhalpai | Changed error handler to new standard format. PIR2051
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_gp_control_proc(
    i_msg_id       IN      VARCHAR2,
    o_mq_queue     OUT     VARCHAR2,
    o_mq_qmanager  OUT     VARCHAR2,
    o_descr        OUT     VARCHAR2,
    o_hdr_data     OUT     VARCHAR2,
    o_trans_mode   OUT     VARCHAR2,
    o_dtl_sw       OUT     VARCHAR2,
    i_div          IN      VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_MQ_MESSAGE_PK.MQ_GP_CONTROL_PROC';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_mq IS
      SELECT c.mq_queue, c.mq_qmanager, c.mq_msg_id_desc, c.mq_msg_hdr, c.transmit_mode, c.hdr_incl_sw
        FROM div_mstr_di1d d, mclane_mq_gp_control c
       WHERE d.div_id = i_div
         AND c.div_part = d.div_part
         AND c.mq_msg_id = i_msg_id;

    l_r_data             l_cur_mq%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open cursor');

    OPEN l_cur_mq;

    logs.dbg('Fetch cursor');

    FETCH l_cur_mq
     INTO l_r_data;

    IF l_cur_mq%FOUND THEN
      logs.dbg('Assign data');
      o_mq_queue := l_r_data.mq_queue;
      o_mq_qmanager := l_r_data.mq_qmanager;
      o_descr := l_r_data.mq_msg_id_desc;
      o_hdr_data := l_r_data.mq_msg_hdr;
      o_trans_mode := l_r_data.transmit_mode;
      o_dtl_sw := l_r_data.hdr_incl_sw;
    END IF;

    logs.dbg('Close cursor');

    CLOSE l_cur_mq;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_mq%ISOPEN THEN
        CLOSE l_cur_mq;
      END IF;

      logs.err(lar_parm);
  END mq_gp_control_proc;

  /*
  ||----------------------------------------------------------------------------
  || GET_MQ_CONNECTION_INFO_SP
  ||   Retrieve MQ connection information for a given message ID
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/23/03 | SNAGABH | Original
  || 07/13/05 | rhalpai | Changed error handler to new standard format. PIR2051
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_mq_connection_info_sp(
    i_div                 IN      VARCHAR2,
    i_msg_id              IN      VARCHAR2,
    o_mq_queue            OUT     VARCHAR2,
    o_host_nm             OUT     VARCHAR2,
    o_mq_channel          OUT     VARCHAR2,
    o_mq_qmanager         OUT     VARCHAR2,
    o_mq_port             OUT     NUMBER,
    o_concurrent_threads  OUT     NUMBER,
    o_max_prcs_attmpt     OUT     NUMBER,
    o_cmnd                OUT     VARCHAR2,
    o_cmnd_typ            OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_MQ_MESSAGE_PK.GET_MQ_CONNECTION_INFO_SP';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_mq IS
      SELECT c.mq_queue, c.hostname, c.mq_channel, c.mq_port, c.mq_qmanager, c.concurrent_threads, c.max_prcs_attmpt,
             c.cmnd, c.cmnd_typ
        FROM div_mstr_di1d d, mclane_mq_gp_control c
       WHERE d.div_id = i_div
         AND c.div_part = d.div_part
         AND c.mq_msg_id = i_msg_id;

    l_r_data             l_cur_mq%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open cursor');

    OPEN l_cur_mq;

    logs.dbg('Fetch cursor');

    FETCH l_cur_mq
     INTO l_r_data;

    IF l_cur_mq%FOUND THEN
      logs.dbg('Assign data');
      o_mq_queue := l_r_data.mq_queue;
      o_host_nm := l_r_data.hostname;
      o_mq_channel := l_r_data.mq_channel;
      o_mq_port := l_r_data.mq_port;
      o_mq_qmanager := l_r_data.mq_qmanager;
      o_concurrent_threads := l_r_data.concurrent_threads;
      o_max_prcs_attmpt := l_r_data.max_prcs_attmpt;
      o_cmnd := l_r_data.cmnd;
      o_cmnd_typ := l_r_data.cmnd_typ;
    END IF;

    logs.dbg('Close cursor');

    CLOSE l_cur_mq;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_cur_mq%ISOPEN THEN
        CLOSE l_cur_mq;
      END IF;

      logs.err(lar_parm);
  END get_mq_connection_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_GET_MSG_SP
  ||  Add MQ GET msg.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/13/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_get_msg_sp(
    i_div       IN  VARCHAR2,
    i_msg_id    IN  VARCHAR2,
    i_msg_data  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MQ_MESSAGE_PK.INS_GET_MSG_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'MsgData', i_msg_data);
    logs.dbg('ENTRY', lar_parm);

    INSERT INTO mclane_mq_get
                (mq_msg_status, div_part, mq_msg_id, mq_msg_data)
      SELECT 'OPN', d.div_part, i_msg_id, i_msg_data
        FROM div_mstr_di1d d
       WHERE d.div_id = i_div;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_get_msg_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_PUT_MSG_STAT_SP
  ||  Set MQ PUT msg status.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/13/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_put_msg_stat_sp(
    i_div          IN  VARCHAR2,
    i_msg_id       IN  VARCHAR2,
    i_corr_id      IN  NUMBER,
    i_new_stat_cd  IN  VARCHAR2,
    i_put_id       IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MQ_MESSAGE_PK.UPD_PUT_MSG_STAT_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'CorrId', i_corr_id);
    logs.add_parm(lar_parm, 'NewStatCd', i_new_stat_cd);
    logs.add_parm(lar_parm, 'PutId', i_put_id);
    logs.dbg('ENTRY', lar_parm);

    IF i_put_id IS NOT NULL THEN
      logs.dbg('Set Status using MQPutId');

      UPDATE mclane_mq_put p
         SET p.mq_msg_status = i_new_stat_cd,
             p.last_chg_ts = l_c_sysdate
       WHERE p.div_part = (SELECT d.div_part
                             FROM div_mstr_di1d d
                            WHERE d.div_id = i_div)
         AND p.mq_msg_id = i_msg_id
         AND p.mq_put_id = i_put_id
         AND p.mq_msg_status = 'OPN';
    ELSE
      logs.dbg('Set Status using MQCorrPutId');

      UPDATE mclane_mq_put p
         SET p.mq_msg_status = i_new_stat_cd,
             p.last_chg_ts = l_c_sysdate
       WHERE p.div_part = (SELECT d.div_part
                             FROM div_mstr_di1d d
                            WHERE d.div_id = i_div)
         AND p.mq_msg_id = i_msg_id
         AND p.mq_corr_put_id = i_corr_id
         AND p.mq_msg_status = 'OPN';
    END IF;   -- i_put_id IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_put_msg_stat_sp;
END op_mq_message_pk;
/

