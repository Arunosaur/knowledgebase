CREATE OR REPLACE PACKAGE op_mclane_mq_put_pk IS
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
  FUNCTION sel_for_upd_fn(
    i_div_part   IN  NUMBER,
    i_mq_put_id  IN  NUMBER
  )
    RETURN mclane_mq_put%ROWTYPE;

  FUNCTION ins_fn(
    i_div_part        IN  NUMBER,
    i_mq_msg_id       IN  mclane_mq_put.mq_msg_id%TYPE,
    i_mq_msg_data     IN  mclane_mq_put.mq_msg_data%TYPE,
    i_create_ts       IN  mclane_mq_put.create_ts%TYPE DEFAULT SYSDATE,
    i_mq_corr_put_id  IN  NUMBER DEFAULT 0,
    i_mq_msg_status   IN  mclane_mq_put.mq_msg_status%TYPE DEFAULT 'OPN',
    i_last_chg_ts     IN  mclane_mq_put.last_chg_ts%TYPE DEFAULT SYSDATE
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_div_part          IN  NUMBER,
    i_mq_put_id         IN  NUMBER,
    i_mq_msg_id         IN  VARCHAR2,
    i_mq_msg_status     IN  VARCHAR2,
    i_mq_msg_data       IN  VARCHAR2,
    i_mq_corr_put_id    IN  VARCHAR2,
    i_use_passed_nulls  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER;

  FUNCTION del_fn(
    i_div_part   IN  NUMBER,
    i_mq_put_id  IN  NUMBER
  )
    RETURN NUMBER;
--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END op_mclane_mq_put_pk;
/

CREATE OR REPLACE PACKAGE BODY op_mclane_mq_put_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  FUNCTION get_val_fn(
    i_use_passed_nulls  IN  BOOLEAN,
    i_new_val           IN  VARCHAR2,
    i_existing_val      IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN((CASE
              WHEN i_use_passed_nulls THEN i_new_val
              WHEN UPPER(i_new_val) = 'NULL' THEN NULL
              ELSE NVL(i_new_val, i_existing_val)
            END
           )
          );
  END get_val_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  FUNCTION sel_for_upd_fn(
    i_div_part   IN  NUMBER,
    i_mq_put_id  IN  NUMBER
  )
    RETURN mclane_mq_put%ROWTYPE IS
    lar_parm           logs.tar_parm;
    l_r_mclane_mq_put  mclane_mq_put%ROWTYPE;
    l_cv               SYS_REFCURSOR;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqPutId', i_mq_put_id);

    OPEN l_cv
     FOR
       SELECT     *
             FROM mclane_mq_put
            WHERE div_part = i_div_part
              AND mq_put_id = i_mq_put_id
       FOR UPDATE;

    FETCH l_cv
     INTO l_r_mclane_mq_put;

    CLOSE l_cv;

    RETURN(l_r_mclane_mq_put);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END sel_for_upd_fn;

  FUNCTION ins_fn(
    i_div_part        IN  NUMBER,
    i_mq_msg_id       IN  mclane_mq_put.mq_msg_id%TYPE,
    i_mq_msg_data     IN  mclane_mq_put.mq_msg_data%TYPE,
    i_create_ts       IN  mclane_mq_put.create_ts%TYPE DEFAULT SYSDATE,
    i_mq_corr_put_id  IN  NUMBER DEFAULT 0,
    i_mq_msg_status   IN  mclane_mq_put.mq_msg_status%TYPE DEFAULT 'OPN',
    i_last_chg_ts     IN  mclane_mq_put.last_chg_ts%TYPE DEFAULT SYSDATE
  )
    RETURN NUMBER IS
    lar_parm  logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgId', i_mq_msg_id);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.add_parm(lar_parm, 'CreateTs', i_create_ts);
    logs.add_parm(lar_parm, 'MqCorrPutId', i_mq_corr_put_id);
    logs.add_parm(lar_parm, 'MqMsgStatus', i_mq_msg_status);
    logs.add_parm(lar_parm, 'LastChgTs', i_last_chg_ts);

    INSERT INTO mclane_mq_put
                (mq_msg_id, div_part, mq_msg_status, create_ts, last_chg_ts, mq_msg_data, mq_corr_put_id
                )
         VALUES (i_mq_msg_id, i_div_part, i_mq_msg_status, i_create_ts, i_last_chg_ts, i_mq_msg_data, i_mq_corr_put_id
                );

    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_fn;

  FUNCTION upd_fn(
    i_div_part          IN  NUMBER,
    i_mq_put_id         IN  NUMBER,
    i_mq_msg_id         IN  VARCHAR2,
    i_mq_msg_status     IN  VARCHAR2,
    i_mq_msg_data       IN  VARCHAR2,
    i_mq_corr_put_id    IN  VARCHAR2,
    i_use_passed_nulls  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER IS
    lar_parm            logs.tar_parm;
    l_r_mclane_mq_put   mclane_mq_put%ROWTYPE;
    l_use_passed_nulls  BOOLEAN                 :=(UPPER(i_use_passed_nulls) = 'Y');
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqPutId', i_mq_put_id);
    logs.add_parm(lar_parm, 'MqMsgId', i_mq_msg_id);
    logs.add_parm(lar_parm, 'MqMsgStatus', i_mq_msg_status);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.add_parm(lar_parm, 'MqCorrPutId', i_mq_corr_put_id);
    logs.add_parm(lar_parm, 'UsePassedNulls', i_use_passed_nulls);
    l_r_mclane_mq_put := sel_for_upd_fn(i_div_part, i_mq_put_id);

    IF l_r_mclane_mq_put.mq_put_id IS NOT NULL THEN
      l_r_mclane_mq_put.mq_msg_id := get_val_fn(l_use_passed_nulls, i_mq_msg_id, l_r_mclane_mq_put.mq_msg_id);
      l_r_mclane_mq_put.div_part := i_div_part;
      l_r_mclane_mq_put.mq_msg_status := get_val_fn(l_use_passed_nulls,
                                                    i_mq_msg_status,
                                                    l_r_mclane_mq_put.mq_msg_status
                                                   );
      l_r_mclane_mq_put.mq_msg_data := get_val_fn(l_use_passed_nulls, i_mq_msg_data, l_r_mclane_mq_put.mq_msg_data);
      l_r_mclane_mq_put.last_chg_ts := SYSDATE;

      IF i_mq_corr_put_id IS NOT NULL THEN
        l_r_mclane_mq_put.mq_corr_put_id := i_mq_corr_put_id;
      END IF;   -- i_mq_corr_put_id IS NOT NULL

      UPDATE mclane_mq_put
         SET ROW = l_r_mclane_mq_put
       WHERE div_part = i_div_part
         AND mq_put_id = i_mq_put_id;
    END IF;   -- l_r_mclane_mq_put.mq_put_id IS NOT NULL

    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_fn;

  FUNCTION del_fn(
    i_div_part   IN  NUMBER,
    i_mq_put_id  IN  NUMBER
  )
    RETURN NUMBER IS
    lar_parm  logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqPutId', i_mq_put_id);

    DELETE FROM mclane_mq_put
          WHERE div_part = i_div_part
            AND mq_put_id = i_mq_put_id;

    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_fn;
END op_mclane_mq_put_pk;
/

