CREATE OR REPLACE PACKAGE op_time_pk IS
  /*
  ||----------------------------------------------------------------------------
  || To implement:
  || Replace SYSDATE with op_time_pk.get_time_fn as needed (that's it!)
  ||
  || To use:
  || 1. exec procedure op_time_pk.set_time_sp
  ||   a. date format is DD-MON-YY HH24:MI:SS
  ||   b. using increment option will cause time to increment the date you have
  ||      it set to (default is for a fixed date)
  ||
  || Examples:
  ||   op_time_pk.set_time_sp ('25-dec-06');
  ||   op_time_pk.set_time_sp ('25-dec-06 14');
  ||   op_time_pk.set_time_sp ('25-dec-06 14:10');
  ||   op_time_pk.set_time_sp ('25-dec-06 14:10:12');
  ||   op_time_pk.set_time_sp ('25-dec-06 14:10', true);
  ||
  || 2. Calls to op_time_pk.get_time_fn will now return date based on step 1.
  ||   a. IMPORTANT: This setting is session level only meaning it will only
  ||      affect the session which set the date. If the process you kick off makes
  ||      an external call which in turn logs back into the database (ie. by way
  ||      of kicking off a script, directly, etc), this new connection will not
  ||      see change.
  ||
  || Example:
  ||   You login and set the time. Any calls you make to op_time_pk.get_time_fn
  ||   directly or by way of calling other packages will see the time you set.
  ||
  ||   All other sessions will continue to have the real time returned, they will
  ||   not see your date.
  ||
  || 3.  To reset back to normal, execute op_time_pk.reset_sp or log off
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
  FUNCTION get_time_fn
    RETURN DATE;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE set_time_sp(
    i_desired_dt    IN  VARCHAR2,
    i_is_increment  IN  BOOLEAN DEFAULT FALSE
  );

  PROCEDURE reset_sp;
END op_time_pk;
/

CREATE OR REPLACE PACKAGE BODY op_time_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_fake_dt       DATE;
  g_current_dt    DATE;
  g_is_increment  BOOLEAN;

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || GET_TIME_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/24/08 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_time_fn
    RETURN DATE IS
  BEGIN
    IF g_fake_dt IS NULL THEN
      RETURN SYSDATE;
    ELSE
      IF g_is_increment THEN
        RETURN g_fake_dt +(SYSDATE - g_current_dt);
      ELSE
        RETURN g_fake_dt;
      END IF;
    END IF;
  END get_time_fn;

  /*
  ||----------------------------------------------------------------------------
  || SET_TIME_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/24/08 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  PROCEDURE set_time_sp(
    i_desired_dt    IN  VARCHAR2,
    i_is_increment  IN  BOOLEAN DEFAULT FALSE
  ) IS
  BEGIN
    g_fake_dt := TO_DATE(i_desired_dt, 'DD-MON-YY HH24:MI:SS');
    g_current_dt := SYSDATE;
    g_is_increment := i_is_increment;
  END set_time_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/24/08 | rhalpai | Original. PIR3593
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_sp IS
  BEGIN
    g_fake_dt := NULL;
    g_current_dt := NULL;
    g_is_increment := FALSE;
  END reset_sp;
END op_time_pk;
/

