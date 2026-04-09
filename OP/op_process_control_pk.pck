CREATE OR REPLACE PACKAGE op_process_control_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------
  g_c_active     CONSTANT VARCHAR2(1) := 'Y';
  g_c_inactive   CONSTANT VARCHAR2(1) := 'N';
  g_e_process_restricted  EXCEPTION;
  PRAGMA EXCEPTION_INIT(g_e_process_restricted, -20998);

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  /**
  ||----------------------------------------------------------------------------
  || Returns a collection of restricted processes that are active
  || #param i_prcs_id   Control Process ID.
  || #param i_div_part  DivPart.
  || #return            Collection of restricted processes
  ||----------------------------------------------------------------------------
  **/
  FUNCTION get_active_restrictions_fn(
    i_prcs_id   IN  VARCHAR2,
    i_div_part  IN  NUMBER
  )
    RETURN type_stab;

  /**
  ||----------------------------------------------------------------------------
  || Indicates whether the process should be restricted from executing
  || #param i_prcs_id   Control Process ID.
  || #param i_div_part  DivPart.
  || #param i_excl_list Exclusion list of restrictions.
  || #return            Should restrict execution?  (TRUE|FALSE)
  ||----------------------------------------------------------------------------
  **/
  FUNCTION is_restricted_fn(
    i_prcs_id    IN  VARCHAR2,
    i_div_part   IN  NUMBER,
    i_excl_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN BOOLEAN;

  /**
  ||----------------------------------------------------------------------------
  || Message that process is restricted and lists conflicting active processes
  || #param i_prcs_id   Control Process ID.
  || #param i_div_part  DivPart.
  || #return            Restricted message
  ||----------------------------------------------------------------------------
  **/
  FUNCTION restricted_msg_fn(
    i_prcs_id   IN  VARCHAR2,
    i_div_part  IN  NUMBER
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  /**
  ||----------------------------------------------------------------------------
  || Updates the process active status
  || #param i_prcs_id        Control Process ID.
  || #param i_active_sw      Active Status Switch  ('Y'|'N')
  || #param o_is_resticted   Is execution restricted?  (TRUE|FALSE)
  || #param i_user_id        UserID.
  || #param i_div_part       DivPart.
  || #param i_excl_list      Exclusion list of restrictions.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE upd_status_sp(
    i_prcs_id        IN      VARCHAR2,
    i_active_sw      IN      VARCHAR2,
    o_is_restricted  OUT     BOOLEAN,
    i_user_id        IN      VARCHAR2,
    i_div_part       IN      NUMBER,
    i_excl_list      IN      VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Attempts to set the process status to active. Will wait and re-attempt
  || a number of times when restricted.
  || #param i_prcs_id        Control Process ID.
  || #param o_is_resticted   Is execution restricted?  (TRUE|FALSE)
  || #param i_user_id        UserID.
  || #param i_div_part       DivPart.
  || #param i_excl_list      Exclusion list of restrictions.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE set_active_or_wait_sp(
    i_prcs_id        IN      VARCHAR2,
    o_is_restricted  OUT     BOOLEAN,
    i_user_id        IN      VARCHAR2,
    i_div_part       IN      NUMBER,
    i_excl_list      IN      VARCHAR2 DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Raise application error reflecting conflicting active processes
  || #param i_prcs_id   Control Process ID.
  || #param i_div_part  DivPart.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE raise_err_sp(
    i_prcs_id   IN  VARCHAR2,
    i_div_part  IN  NUMBER
  );

  /**
  ||----------------------------------------------------------------------------
  || Set process status to active or inactive.
  || An exception is raised when attempting to set process to active but the
  || process is restricted due to "Time Out" after max attempts has been made.
  || #param i_prcs_id     Control Process ID.
  || #param i_active_sw   Active Status Switch  ('Y'|'N')
  || #param i_user_id     UserID.
  || #param i_div_part    DivPart.
  || #param i_excl_list   Exclusion list of restrictions.
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE set_process_status_sp(
    i_prcs_id    IN  VARCHAR2,
    i_active_sw  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2,
    i_div_part   IN  NUMBER,
    i_excl_list  IN  VARCHAR2 DEFAULT NULL
  );
END op_process_control_pk;
/

CREATE OR REPLACE PACKAGE BODY op_process_control_pk IS
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
  || GET_ACTIVE_RESTRICTIONS_FN
  ||   Returns a collection of restricted processes that are active
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 12/19/07 | rhalpai | Added logic to block all processes during system
  ||                    | maintenance.
  || 05/20/10 | rhalpai | Added process description to cursor. PIR8377
  || 02/25/15 | rhalpai | Change logic to use new CN3P table and allow DivPart 0
  ||                    | for Corp-level processing and allow a NULL PrcsId parm
  ||                    | to bring back all active processes. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_active_restrictions_fn(
    i_prcs_id   IN  VARCHAR2,
    i_div_part  IN  NUMBER
  )
    RETURN type_stab IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_CONTROL_PK.GET_ACTIVE_RESTRICTIONS_FN';
    lar_parm             logs.tar_parm;
    l_t_rstrn_ids        type_stab;
  BEGIN
    logs.add_parm(lar_parm, 'PrcsID', i_prcs_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);

    SELECT d.prcs_id || ' ' || d.descr
    BULK COLLECT INTO l_t_rstrn_ids
      FROM prcs_cntl_actv_cn3p a, prcs_cntl_dfn_cn1p d
     WHERE a.div_part IN(0, DECODE(i_div_part, 0, a.div_part, i_div_part))
       AND d.prcs_id = a.prcs_id
       AND (   i_prcs_id IS NULL
            OR a.prcs_id IN('SYS_MAINT', i_prcs_id)
            OR EXISTS(SELECT 1
                        FROM prcs_cntl_rstrn_cn2p r
                       WHERE r.rstrn_id = a.prcs_id
                         AND r.prcs_id = i_prcs_id)
           );

    RETURN(l_t_rstrn_ids);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_active_restrictions_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_RESTRICTED_FN
  ||   Indicates whether the process should be restricted from executing
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 12/19/07 | rhalpai | Added logic to block all processes during system
  ||                    | maintenance.
  || 02/25/15 | rhalpai | Change logic to use new CN3P table and allow DivPart 0
  ||                    | for Corp-level processing and add exclusion list parm
  ||                    | with logic to allow exclusion of restricted processes.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_restricted_fn(
    i_prcs_id    IN  VARCHAR2,
    i_div_part   IN  NUMBER,
    i_excl_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN BOOLEAN IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_CONTROL_PK.IS_RESTRICTED_FN';
    lar_parm             logs.tar_parm;
    l_restricted_sw      VARCHAR2(1)   := 'N';
    l_cv                 SYS_REFCURSOR;
  BEGIN
    logs.add_parm(lar_parm, 'PrcsID', i_prcs_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ExclList', i_excl_list);

    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM prcs_cntl_actv_cn3p a
        WHERE a.div_part IN(0, DECODE(i_div_part, 0, a.div_part, i_div_part))
          AND (   a.prcs_id IN('SYS_MAINT', i_prcs_id)
               OR EXISTS(SELECT 1
                           FROM prcs_cntl_rstrn_cn2p r
                          WHERE r.rstrn_id = a.prcs_id
                            AND r.rstrn_id NOT IN(SELECT     regexp_substr(x.str, '[^,]+', 1, LEVEL) AS str
                                                        FROM (SELECT NVL(i_excl_list, ' ') AS str
                                                                FROM DUAL) x
                                                  CONNECT BY LEVEL <= LENGTH(regexp_replace(x.str, '[^,]+')) + 1)
                            AND r.prcs_id = i_prcs_id)
              );

    FETCH l_cv
     INTO l_restricted_sw;

    RETURN(l_restricted_sw = 'Y');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_restricted_fn;

  /*
  ||----------------------------------------------------------------------------
  || RESTRICTED_MSG_FN
  ||   Returns message indicating process is restricted and lists conflicting
  ||   active processes.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 05/20/10 | rhalpai | Increased size of v_msg since process descriptions are
  ||                    | now included. PIR8377
  || 02/25/15 | rhalpai | Change logic to allow DivPart 0 for Corp-level
  ||                    | processing. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION restricted_msg_fn(
    i_prcs_id   IN  VARCHAR2,
    i_div_part  IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_msg         typ.t_maxvc2;
    l_t_rstrctns  type_stab;
    l_idx         PLS_INTEGER;
  BEGIN
    l_msg := i_prcs_id || ' process restricted at this time. Conflicting active process(s): ';
    l_t_rstrctns := get_active_restrictions_fn(i_prcs_id, i_div_part);
    l_idx := l_t_rstrctns.FIRST;

    BEGIN
      WHILE l_idx IS NOT NULL LOOP
        l_msg := l_msg || cnst.newline_char || l_t_rstrctns(l_idx);
        l_idx := l_t_rstrctns.NEXT(l_idx);
      END LOOP;
    EXCEPTION
      WHEN VALUE_ERROR THEN
        NULL;
    END;

    RETURN(l_msg);
  END restricted_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_STATUS_SP
  ||   Updates the process active status
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 08/31/10 | DLBEAL  | Add userid and last_chg_ts
  || 10/05/10 | rhalpai | Change to only log 1st 8 chars of userid.
  || 02/25/15 | rhalpai | Change logic to use new CN3P table and allow DivPart 0
  ||                    | for Corp-level processing and add exclusion list parm
  ||                    | with logic to allow exclusion of restricted processes.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_status_sp(
    i_prcs_id        IN      VARCHAR2,
    i_active_sw      IN      VARCHAR2,
    o_is_restricted  OUT     BOOLEAN,
    i_user_id        IN      VARCHAR2,
    i_div_part       IN      NUMBER,
    i_excl_list      IN      VARCHAR2 DEFAULT NULL
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_PROCESS_CONTROL_PK.UPD_STATUS_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'PrcsID', i_prcs_id);
    logs.add_parm(lar_parm, 'ActiveSW', i_active_sw);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ExclList', i_excl_list);

    CASE i_active_sw
      WHEN g_c_inactive THEN
        o_is_restricted := FALSE;

        DELETE FROM prcs_cntl_actv_cn3p a
              WHERE a.prcs_id = i_prcs_id
                AND a.div_part = i_div_part;

        COMMIT;
      WHEN g_c_active THEN
        o_is_restricted := is_restricted_fn(i_prcs_id, i_div_part, i_excl_list);

        IF NOT o_is_restricted THEN
          INSERT INTO prcs_cntl_actv_cn3p
                      (prcs_id, div_part, user_id, last_chg_ts
                      )
               VALUES (i_prcs_id, i_div_part, SUBSTR(i_user_id, 1, 8), SYSDATE
                      );

          COMMIT;
        END IF;   -- NOT o_is_resticted
      ELSE
        NULL;
    END CASE;
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
      o_is_restricted := TRUE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_status_sp;

  /*
  ||----------------------------------------------------------------------------
  || SET_ACTIVE_OR_WAIT_SP
  ||   Attempts to set the process status to active. Will wait and re-attempt a
  ||   number of times when restricted.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 12/19/07 | rhalpai | Added logic to block all processes during system
  ||                    | maintenance.
  || 08/31/10 | DLBEAL  | Add userid/last chg ts
  || 02/25/15 | rhalpai | Change logic to use new CN3P table and allow DivPart 0
  ||                    | for Corp-level processing and add exclusion list parm
  ||                    | with logic to allow exclusion of restricted processes.
  ||                    | PIR11038
  || 10/20/17 | rhalpai | Change cursor to ensure only one row is returned.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE set_active_or_wait_sp(
    i_prcs_id        IN      VARCHAR2,
    o_is_restricted  OUT     BOOLEAN,
    i_user_id        IN      VARCHAR2,
    i_div_part       IN      NUMBER,
    i_excl_list      IN      VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module              CONSTANT typ.t_maxfqnm := 'OP_PROCESS_CONTROL_PK.SET_ACTIVE_OR_WAIT_SP';
    lar_parm                         logs.tar_parm;
    l_cv                             SYS_REFCURSOR;
    l_wait_secs                      PLS_INTEGER   := 5;
    l_max_attmpt                     PLS_INTEGER   := 12;
    l_sys_maint_sw                   VARCHAR2(1)   := 'N';
    l_cnt                            PLS_INTEGER   := 0;
    l_secs_waiting                   PLS_INTEGER   := 0;
    l_warn_secs                      PLS_INTEGER   := 0;
    l_c_warn_interval_mins  CONSTANT PLS_INTEGER   := 5;
  BEGIN
    logs.add_parm(lar_parm, 'PrcsID', i_prcs_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ExclList', i_excl_list);
    LOOP
      -- Lookup is inside the loop to include table changes while processing
      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT wait_secs, max_attmpt,
                NVL((SELECT 'Y'
                       FROM prcs_cntl_actv_cn3p a
                      WHERE a.div_part IN(0, i_div_part)
                        AND a.prcs_id = 'SYS_MAINT'
                        AND ROWNUM = 1), 'N') AS sys_maint
           FROM prcs_cntl_dfn_cn1p d
          WHERE d.prcs_id = i_prcs_id;

      logs.dbg('Fetch Cursor');

      FETCH l_cv
       INTO l_wait_secs, l_max_attmpt, l_sys_maint_sw;

      logs.dbg('Update active status if unrestricted');
      upd_status_sp(i_prcs_id, g_c_active, o_is_restricted, i_user_id, i_div_part, i_excl_list);

      IF l_sys_maint_sw = 'N' THEN
        -- suspend attempt count during system maintenance
        l_cnt := l_cnt + 1;
      END IF;   -- l_sys_maint_sw = 'N'

      EXIT WHEN(   NOT o_is_restricted
                OR l_cnt > l_max_attmpt);
      l_secs_waiting := l_cnt * l_wait_secs;

      IF l_secs_waiting >= l_warn_secs THEN
        l_warn_secs := l_warn_secs +(l_c_warn_interval_mins * 60);
        logs.warn(i_prcs_id
                  || ' '
                  ||(CASE
                       WHEN l_cnt = 1 THEN 'First Wait'
                       ELSE 'Waiting ' || l_secs_waiting / 60 || ' Minutes'
                     END)
                  || cnst.newline_char
                  || restricted_msg_fn(i_prcs_id, i_div_part),
                  lar_parm
                 );
      END IF;   -- l_secs_waiting >= l_warn_secs

      DBMS_LOCK.sleep(l_wait_secs);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END set_active_or_wait_sp;

  /*
  ||----------------------------------------------------------------------------
  || RAISE_ERR_SP
  ||   Raise application error reflecting conflicting active processes
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 02/25/15 | rhalpai | Change logic to allow DivPart 0 for Corp-level
  ||                    | processing. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE raise_err_sp(
    i_prcs_id   IN  VARCHAR2,
    i_div_part  IN  NUMBER
  ) IS
  BEGIN
    excp.throw(-20998, restricted_msg_fn(i_prcs_id, i_div_part));
  END raise_err_sp;

  /*
  ||----------------------------------------------------------------------------
  || SET_PROCESS_STATUS_SP
  ||   Set process status to active or inactive.
  ||   An exception is raised when attempting to set process to active but the
  ||   process is restricted due to "Time Out" after max attempts has been made.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Original
  || 08/31/10 | DLBEAL  | Add userid/last chg ts
  || 02/25/15 | rhalpai | Change logic to use new CN3P table and allow DivPart 0
  ||                    | for Corp-level processing and add exclusion list parm
  ||                    | with logic to allow exclusion of restricted processes.
  ||                    | PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE set_process_status_sp(
    i_prcs_id    IN  VARCHAR2,
    i_active_sw  IN  VARCHAR2,
    i_user_id    IN  VARCHAR2,
    i_div_part   IN  NUMBER,
    i_excl_list  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_is_prcs_restricted  BOOLEAN;
  BEGIN
    CASE i_active_sw
      WHEN g_c_active THEN
        set_active_or_wait_sp(i_prcs_id, l_is_prcs_restricted, i_user_id, i_div_part, i_excl_list);

        IF l_is_prcs_restricted THEN
          raise_err_sp(i_prcs_id, i_div_part);
        END IF;   -- l_is_prcs_restricted
      WHEN g_c_inactive THEN
        upd_status_sp(i_prcs_id, g_c_inactive, l_is_prcs_restricted, i_user_id, i_div_part);
      ELSE
        NULL;
    END CASE;
  END set_process_status_sp;
END op_process_control_pk;
/

