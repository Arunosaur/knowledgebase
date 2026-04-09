CREATE OR REPLACE PACKAGE op_event_pk IS
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
  FUNCTION event_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION event_hist_list_fn(
    i_div  IN  VARCHAR2,
    i_dt   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION process_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE ins_event_sp(
    i_div            IN      VARCHAR2,
    i_evnt_nm        IN      VARCHAR2,
    i_evnt_descr     IN      VARCHAR2,
    i_email_rqst_sw  IN      VARCHAR2,
    i_reset_time     IN      NUMBER,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  );

  PROCEDURE upd_event_sp(
    i_div            IN      VARCHAR2,
    i_evnt_nm        IN      VARCHAR2,
    i_evnt_descr     IN      VARCHAR2,
    i_email_rqst_sw  IN      VARCHAR2,
    i_reset_time     IN      NUMBER,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  );

  PROCEDURE del_event_sp(
    i_div      IN      VARCHAR2,
    i_evnt_nm  IN      VARCHAR2,
    i_user_id  IN      VARCHAR2,
    o_msg      OUT     VARCHAR2
  );

  PROCEDURE ins_event_req_sp(
    i_div           IN      VARCHAR2,
    i_evnt_nm       IN      VARCHAR2,
    i_email_comnts  IN      VARCHAR2,
    i_user_id       IN      VARCHAR2,
    o_msg           OUT     VARCHAR2
  );

  PROCEDURE upd_group_contacts_sp(
    i_div            IN      VARCHAR2,
    i_evnt_nm        IN      VARCHAR2,
    i_cntct_id_list  IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  );
END op_event_pk;
/

CREATE OR REPLACE PACKAGE BODY op_event_pk IS
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
  || EVENT_LIST_FN
  ||  Returns cursor of event transactions for active events.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  FUNCTION event_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_EVENT_PK.EVENT_LIST_FN';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_cv                  SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   x.evnt_nm, x.evnt_desc, x.email_rqst_sw, x.reset_time, SUBSTR(x.rqst_ts_user, 1, 19) AS last_rqst_ts,
                SUBSTR(x.rqst_ts_user, 20) AS last_rqst_user, x.can_run
           FROM (SELECT m.evnt_nm, m.evnt_desc, m.email_rqst_sw, m.reset_time,
                        (SELECT TO_CHAR(t.evnt_rqst_ts, 'YYYY-MM-DD HH24:MI:SS') || t.user_id
                           FROM evnt_tran_ev1t t
                          WHERE t.div_part = m.div_part
                            AND t.evnt_nm = m.evnt_nm
                            AND t.evnt_rqst_ts = (SELECT MAX(t2.evnt_rqst_ts)
                                                    FROM evnt_tran_ev1t t2
                                                   WHERE t2.div_part = t.div_part
                                                     AND t2.evnt_nm = t.evnt_nm)) AS rqst_ts_user,
                        (CASE
                           WHEN m.nxt_rqst_ts <= l_c_sysdate THEN 'Y'
                           ELSE 'N'
                         END) AS can_run
                   FROM div_mstr_di1d d, evnt_mstr_ev1m m
                  WHERE d.div_id = i_div
                    AND m.div_part = d.div_part
                    AND m.actv_sw = 'Y') x
       ORDER BY x.evnt_nm;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END event_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || EVENT_HIST_LIST_FN
  ||  Returns cursor of event transactions for a date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  FUNCTION event_hist_list_fn(
    i_div  IN  VARCHAR2,
    i_dt   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_EVENT_PK.EVENT_HIST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_from_ts            DATE;
    l_to_ts              DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'Dt', i_dt);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_from_ts := TO_DATE(i_dt, 'YYYY-MM-DD');
    l_to_ts := TO_DATE(i_dt || ' 23:59:59', 'YYYY-MM-DD HH24:MI:SS');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   m.evnt_nm, m.evnt_desc, TO_CHAR(t.evnt_rqst_ts, 'YYYY-MM-DD HH24:MI:SS') AS rqst_ts, t.user_id,
                t.email_comnts, TO_CHAR(t.evnt_compl_notifctn_ts, 'YYYY-MM-DD HH24:MI:SS') AS evnt_compl_notifctn_ts,
                m.actv_sw
           FROM div_mstr_di1d d, evnt_tran_ev1t t, evnt_mstr_ev1m m
          WHERE d.div_id = i_div
            AND t.div_part = d.div_part
            AND t.evnt_rqst_ts BETWEEN l_from_ts AND l_to_ts
            AND m.div_part = t.div_part
            AND m.evnt_nm = t.evnt_nm
       ORDER BY t.evnt_rqst_ts;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END event_hist_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_LIST_FN
  ||  Returns cursor of process events.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  FUNCTION process_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_EVENT_PK.PROCESS_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    l_cv := op_contact_pk.process_list_fn(i_div, 'EVN');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END process_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_EVENT_SP
  ||  Add master event and notification process, process group and group info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_event_sp(
    i_div            IN      VARCHAR2,
    i_evnt_nm        IN      VARCHAR2,
    i_evnt_descr     IN      VARCHAR2,
    i_email_rqst_sw  IN      VARCHAR2,
    i_reset_time     IN      NUMBER,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm                   := 'OP_EVENT_PK.INS_EVENT_SP';
    lar_parm                logs.tar_parm;
    l_c_sysdate    CONSTANT DATE                            := SYSDATE;
    l_evnt_nm               evnt_mstr_ev1m.evnt_nm%TYPE;
    l_evnt_descr            evnt_mstr_ev1m.evnt_desc%TYPE;
    l_e_evnt_nm_invalid     EXCEPTION;
    l_e_evnt_descr_invalid  EXCEPTION;
    l_e_error               EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EvntNm', i_evnt_nm);
    logs.add_parm(lar_parm, 'EvntDescr', i_evnt_descr);
    logs.add_parm(lar_parm, 'EmailRqstSw', i_email_rqst_sw);
    logs.add_parm(lar_parm, 'ResetTime', i_reset_time);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);

    IF TRIM(i_evnt_nm) IS NULL THEN
      RAISE l_e_evnt_nm_invalid;
    END IF;   -- TRIM(i_evnt_mn) IS NULL

    IF TRIM(i_evnt_descr) IS NULL THEN
      RAISE l_e_evnt_descr_invalid;
    END IF;   -- TRIM(i_evnt_desc)

    logs.dbg('Initialize');
    l_evnt_nm := RTRIM(UPPER(i_evnt_nm));
    l_evnt_descr := RTRIM(i_evnt_descr);
    logs.dbg('Add Event');

    INSERT INTO evnt_mstr_ev1m
                (div_part, evnt_nm, evnt_desc, email_rqst_sw, reset_time, nxt_rqst_ts, actv_sw, user_id, last_chg_ts)
      SELECT d.div_part, l_evnt_nm, l_evnt_descr, i_email_rqst_sw, NVL(i_reset_time, 0), l_c_sysdate, 'Y', i_user_id,
             l_c_sysdate
        FROM div_mstr_di1d d
       WHERE d.div_id = i_div;

    logs.dbg('Add Process');
    op_contact_pk.ins_prcs_typ_descr_sp(i_div, l_evnt_nm, 'EVN', l_evnt_descr, o_msg);

    IF o_msg IS NOT NULL THEN
      RAISE l_e_error;
    END IF;   -- o_msg IS NOT NULL

    logs.dbg('Add Group Info');
    op_contact_pk.ins_grp_info_sp(i_div, l_evnt_nm, l_evnt_descr, o_msg);

    IF o_msg IS NOT NULL THEN
      RAISE l_e_error;
    END IF;   -- o_msg IS NOT NULL

    logs.dbg('Add Process Group');
    op_contact_pk.ins_prcs_grp_sp(i_div, l_evnt_nm, l_evnt_nm, o_msg);

    IF o_msg IS NOT NULL THEN
      RAISE l_e_error;
    END IF;   -- o_msg IS NOT NULL

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_evnt_nm_invalid THEN
      IF LENGTH(i_evnt_nm) > 0 THEN
        o_msg := 'Event name may not contain only spaces!';
      ELSE
        o_msg := 'Event name is required!';
      END IF;   -- LENGTH(p_evnt_nm) > 0
    WHEN l_e_evnt_descr_invalid THEN
      IF LENGTH(i_evnt_descr) > 0 THEN
        o_msg := 'Event description may not contain only spaces!';
      ELSE
        o_msg := 'Event description is required!';
      END IF;   -- LENGTH(p_evnt_nm) > 0
    WHEN DUP_VAL_ON_INDEX THEN
      o_msg := 'Duplicate event name, "' || l_evnt_nm || '" found!  No updates applied.';
      ROLLBACK;
    WHEN l_e_error THEN
      ROLLBACK;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END ins_event_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_EVENT_SP
  ||  Change master event.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_event_sp(
    i_div            IN      VARCHAR2,
    i_evnt_nm        IN      VARCHAR2,
    i_evnt_descr     IN      VARCHAR2,
    i_email_rqst_sw  IN      VARCHAR2,
    i_reset_time     IN      NUMBER,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm                   := 'OP_EVENT_PK.UPD_EVENT_SP';
    lar_parm                logs.tar_parm;
    l_c_sysdate    CONSTANT DATE                            := SYSDATE;
    l_evnt_nm               evnt_mstr_ev1m.evnt_nm%TYPE;
    l_evnt_descr            evnt_mstr_ev1m.evnt_desc%TYPE;
    l_e_evnt_nm_invalid     EXCEPTION;
    l_e_evnt_descr_invalid  EXCEPTION;
    l_e_evnt_nm_not_found   EXCEPTION;
    l_e_error               EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EvntNm', i_evnt_nm);
    logs.add_parm(lar_parm, 'EvntDescr', i_evnt_descr);
    logs.add_parm(lar_parm, 'EmailRqstSw', i_email_rqst_sw);
    logs.add_parm(lar_parm, 'ResetTime', i_reset_time);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);

    IF TRIM(i_evnt_nm) IS NULL THEN
      RAISE l_e_evnt_nm_invalid;
    END IF;   -- TRIM(i_evnt_mn) IS NULL

    IF TRIM(i_evnt_descr) IS NULL THEN
      RAISE l_e_evnt_descr_invalid;
    END IF;   -- TRIM(i_evnt_desc)

    logs.dbg('Initialize');
    l_evnt_nm := RTRIM(UPPER(i_evnt_nm));
    l_evnt_descr := RTRIM(i_evnt_descr);
    logs.dbg('Change Event');

    UPDATE evnt_mstr_ev1m m
       SET m.evnt_desc = l_evnt_descr,
           m.email_rqst_sw = i_email_rqst_sw,
           m.nxt_rqst_ts = m.nxt_rqst_ts
                           - NUMTODSINTERVAL(m.reset_time, 'MINUTE')
                           + NUMTODSINTERVAL(NVL(i_reset_time, 0), 'MINUTE'),
           m.reset_time = NVL(i_reset_time, 0),
           m.user_id = i_user_id,
           m.last_chg_ts = l_c_sysdate
     WHERE m.div_part = (SELECT d.div_part
                           FROM div_mstr_di1d d
                          WHERE d.div_id = i_div)
       AND m.evnt_nm = l_evnt_nm
       AND m.actv_sw = 'Y';

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_evnt_nm_not_found;
    END IF;   -- l_cv%ROWCOUNT = 0

    logs.dbg('Change Process');
    op_contact_pk.upd_prcs_typ_descr_sp(i_div, l_evnt_nm, 'EVN', l_evnt_descr, o_msg);

    IF o_msg IS NOT NULL THEN
      RAISE l_e_error;
    END IF;   -- o_msg IS NOT NULL

    logs.dbg('Change Group Info');
    op_contact_pk.upd_grp_info_sp(i_div, l_evnt_nm, l_evnt_descr, o_msg);

    IF o_msg IS NOT NULL THEN
      RAISE l_e_error;
    END IF;   -- o_msg IS NOT NULL

    COMMIT;
  EXCEPTION
    WHEN l_e_evnt_nm_invalid THEN
      IF LENGTH(i_evnt_nm) > 0 THEN
        o_msg := 'Event name may not contain only spaces!';
      ELSE
        o_msg := 'Event name is required!';
      END IF;   -- LENGTH(i_evnt_nm) > 0
    WHEN l_e_evnt_descr_invalid THEN
      IF LENGTH(i_evnt_descr) > 0 THEN
        o_msg := 'Event description may not contain only spaces!';
      ELSE
        o_msg := 'Event description is required!';
      END IF;   -- LENGTH(i_evnt_nm) > 0
    WHEN l_e_evnt_nm_not_found THEN
      o_msg := 'Active event name, "' || l_evnt_nm || '", not found!  No updates applied.';
    WHEN l_e_error THEN
      ROLLBACK;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_event_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_EVENT_SP
  ||  Remove master event and notification process, process group, group info
  ||  and group contact.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_event_sp(
    i_div      IN      VARCHAR2,
    i_evnt_nm  IN      VARCHAR2,
    i_user_id  IN      VARCHAR2,
    o_msg      OUT     VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                 := 'OP_EVENT_PK.DEL_EVENT_SP';
    lar_parm               logs.tar_parm;
    l_c_sysdate   CONSTANT DATE                          := SYSDATE;
    l_evnt_nm              evnt_mstr_ev1m.evnt_nm%TYPE;
    l_msg                  typ.t_maxvc2;
    l_e_evnt_nm_invalid    EXCEPTION;
    l_e_evnt_nm_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EvntNm', i_evnt_nm);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);

    IF TRIM(i_evnt_nm) IS NULL THEN
      RAISE l_e_evnt_nm_invalid;
    END IF;   -- TRIM(i_evnt_mn) IS NULL

    logs.dbg('Initialize');
    l_evnt_nm := RTRIM(UPPER(i_evnt_nm));
    logs.dbg('Inactivate Event');

    UPDATE evnt_mstr_ev1m m
       SET m.actv_sw = 'N',
           m.user_id = i_user_id,
           m.last_chg_ts = l_c_sysdate
     WHERE m.div_part = (SELECT d.div_part
                           FROM div_mstr_di1d d
                          WHERE d.div_id = i_div)
       AND m.evnt_nm = l_evnt_nm
       AND m.actv_sw = 'Y';

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_evnt_nm_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    logs.dbg('Remove Notification Process Type');
    op_contact_pk.del_prcs_typ_descr_sp(i_div, l_evnt_nm, l_msg, 'Y');
    logs.dbg('Remove Notification Group');
    op_contact_pk.del_grp_info_sp(i_div, l_evnt_nm, l_msg, 'Y');
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_evnt_nm_invalid THEN
      IF LENGTH(l_evnt_nm) > 0 THEN
        o_msg := 'Event name may not contain only spaces!';
      ELSE
        o_msg := 'Event name is required!';
      END IF;   -- LENGTH(l_evnt_nm) > 0
    WHEN l_e_evnt_nm_not_found THEN
      o_msg := 'Event name, "' || l_evnt_nm || '", not found!  No updates applied.';
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END del_event_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_EVENT_REQ_SP
  ||  Add event transaction and process email notification.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_event_req_sp(
    i_div           IN      VARCHAR2,
    i_evnt_nm       IN      VARCHAR2,
    i_email_comnts  IN      VARCHAR2,
    i_user_id       IN      VARCHAR2,
    o_msg           OUT     VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                       := 'OP_EVENT_PK.INS_EVENT_REQ_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_c_sysdate   CONSTANT DATE                                := SYSDATE;
    l_evnt_nm              evnt_tran_ev1t.evnt_nm%TYPE;
    l_email_comnts         evnt_tran_ev1t.email_comnts%TYPE;
    l_cv                   SYS_REFCURSOR;
    l_evnt_descr           evnt_mstr_ev1m.evnt_desc%TYPE;
    l_email_rqst_sw        evnt_mstr_ev1m.email_rqst_sw%TYPE;
    l_cntct_found_sw       VARCHAR2(1)                         := 'N';
    l_subject              VARCHAR2(300);
    l_msg                  typ.t_maxvc2;
    l_e_evnt_nm_invalid    EXCEPTION;
    l_e_evnt_nm_not_found  EXCEPTION;
    l_e_cntct_not_found    EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EvntNm', i_evnt_nm);
    logs.add_parm(lar_parm, 'EmailComnts', i_email_comnts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);

    IF TRIM(i_evnt_nm) IS NULL THEN
      RAISE l_e_evnt_nm_invalid;
    END IF;   -- TRIM(i_evnt_mn) IS NULL

    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_evnt_nm := RTRIM(UPPER(i_evnt_nm));
    l_email_comnts := NVL(TRIM(i_email_comnts), ' ');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT m.evnt_desc, m.email_rqst_sw,
              (CASE
                 WHEN EXISTS(SELECT 1
                               FROM prcs_grp pg, grp_cntct gc
                              WHERE pg.div_part = m.div_part
                                AND pg.prcs_id = m.evnt_nm
                                AND gc.div_part = pg.div_part
                                AND gc.grp_id = pg.grp_id) THEN 'Y'
                 ELSE 'N'
               END
              )
         FROM evnt_mstr_ev1m m
        WHERE m.div_part = l_div_part
          AND m.evnt_nm = l_evnt_nm;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_evnt_descr, l_email_rqst_sw, l_cntct_found_sw;

    IF l_cv%ROWCOUNT = 0 THEN
      RAISE l_e_evnt_nm_not_found;
    END IF;   -- l_cv%ROWCOUNT = 0

    IF l_cntct_found_sw <> 'Y' THEN
      RAISE l_e_cntct_not_found;
    END IF;   -- l_cntct_found_sw <> 'Y'

    logs.dbg('Add Event Request');

    INSERT INTO evnt_tran_ev1t
                (div_part, evnt_nm, evnt_rqst_ts, email_comnts,
                 evnt_compl_notifctn_ts, user_id, last_chg_ts
                )
         VALUES (l_div_part, l_evnt_nm, l_c_sysdate, l_email_comnts,
                 (CASE
                    WHEN l_cntct_found_sw = 'Y' THEN l_c_sysdate
                  END), i_user_id, l_c_sysdate
                );

    IF l_cntct_found_sw = 'Y' THEN
      logs.dbg('Notification Setup');
      l_subject := 'Division: ' || i_div || ', ' || l_evnt_descr;
      l_msg := 'Division: '
               || i_div
               || cnst.newline_char
               || cnst.newline_char
               || i_user_id
               || ' reports that '
               || l_evnt_descr
               || cnst.newline_char
               || cnst.newline_char
               || 'Comments: '
               || l_email_comnts
               || cnst.newline_char
               || cnst.newline_char
               || 'Notification Timestamp is '
               || TO_CHAR(l_c_sysdate, 'YYYY-MM-DD HH24:MI:SS')
               || '.';
      logs.dbg('Notify');
      op_process_common_pk.notify_group_sp(i_div, l_evnt_nm, l_subject, l_msg);
    END IF;   -- l_cntct_found_sw = 'Y'

    logs.dbg('Update Event');

    UPDATE evnt_mstr_ev1m m
       SET m.nxt_rqst_ts = l_c_sysdate + NUMTODSINTERVAL(m.reset_time, 'MINUTE')
     WHERE m.div_part = l_div_part
       AND m.evnt_nm = l_evnt_nm;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_evnt_nm_invalid THEN
      IF LENGTH(i_evnt_nm) > 0 THEN
        o_msg := 'Event name may not contain only spaces!';
      ELSE
        o_msg := 'Event name is required!';
      END IF;   -- LENGTH(i_evnt_nm) > 0
    WHEN l_e_evnt_nm_not_found THEN
      o_msg := 'Event name, "' || l_evnt_nm || '", not found!  No updates applied.';
    WHEN l_e_cntct_not_found THEN
      o_msg := 'Contact info for event name,"' || l_evnt_nm || '", not found!  No updates applied.';
    WHEN DUP_VAL_ON_INDEX THEN
      o_msg := 'Duplicate event name, "' || l_evnt_nm || '" found!  No updates applied.';
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END ins_event_req_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_GROUP_CONTACTS_SP
  ||  Set up notification group contacts for event.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | rhalpai | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_group_contacts_sp(
    i_div            IN      VARCHAR2,
    i_evnt_nm        IN      VARCHAR2,
    i_cntct_id_list  IN      VARCHAR2,
    i_user_id        IN      VARCHAR2,
    o_msg            OUT     VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                       := 'OP_EVENT_PK.UPD_GROUP_CONTACTS_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_t_cntct_ids          type_stab;
    l_cv                   SYS_REFCURSOR;
    l_email_rqst_sw        evnt_mstr_ev1m.email_rqst_sw%TYPE   := 'N';
    l_e_evnt_nm_not_found  EXCEPTION;
    l_e_remove_all_cntcts  EXCEPTION;
    l_e_no_cntcts_found    EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EvntNm', i_evnt_nm);
    logs.add_parm(lar_parm, 'CntctIdList', i_cntct_id_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT m.email_rqst_sw
         FROM evnt_mstr_ev1m m
        WHERE m.div_part = l_div_part
          AND m.evnt_nm = i_evnt_nm;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_email_rqst_sw;

    IF l_cv%ROWCOUNT = 0 THEN
      RAISE l_e_evnt_nm_not_found;
    END IF;   -- l_cv%ROWCOUNT = 0

    CLOSE l_cv;

    IF (    TRIM(i_cntct_id_list) IS NULL
        AND l_email_rqst_sw = 'Y') THEN
      RAISE l_e_remove_all_cntcts;
    END IF;   -- TRIM(i_cntct_id_list) IS NULL AND l_email_rqst_sw = 'Y'

    logs.dbg('Remove Group Contacts');

    DELETE FROM grp_cntct gc
          WHERE gc.div_part = l_div_part
            AND gc.grp_id = i_evnt_nm;

    logs.dbg('Parse Contact List');
    l_t_cntct_ids := str.parse_list(i_cntct_id_list, op_const_pk.field_delimiter);

    IF l_t_cntct_ids.COUNT > 0 THEN
      logs.dbg('Add Group Contacts');

      INSERT INTO grp_cntct
                  (div_part, grp_id, cntct_id)
        SELECT c.div_part, i_evnt_nm, c.cntct_id
          FROM cntct_info c, TABLE(CAST(l_t_cntct_ids AS type_stab)) t
         WHERE c.div_part = l_div_part
           AND c.cntct_id = t.column_value;

      IF SQL%ROWCOUNT = 0 THEN
        RAISE l_e_no_cntcts_found;
      END IF;   -- SQL%ROWCOUNT < l_t_cntct_ids.COUNT

      IF SQL%ROWCOUNT < l_t_cntct_ids.COUNT THEN
        o_msg := 'Changes applied for ' || SQL%ROWCOUNT || ' of ' || l_t_cntct_ids.COUNT || ' contacts.';
      END IF;   -- SQL%ROWCOUNT < l_t_cntct_ids.COUNT
    END IF;   -- t_cntct_ids.COUNT > 0

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_evnt_nm_not_found THEN
      o_msg := 'Event name, "' || i_evnt_nm || '", not found!  No updates applied.';
    WHEN l_e_remove_all_cntcts THEN
      o_msg := 'Cannot remove all contact for event with active email request switch!';
    WHEN l_e_no_cntcts_found THEN
      o_msg := 'No matching contacts found.  No updates applied.';
      ROLLBACK;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_group_contacts_sp;
END op_event_pk;
/

