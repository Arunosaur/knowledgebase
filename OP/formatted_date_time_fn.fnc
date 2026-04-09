CREATE OR REPLACE FUNCTION formatted_date_time_fn(
  i_rendate      IN  PLS_INTEGER,
  i_time         IN  PLS_INTEGER,
  i_time_has_ss  IN  VARCHAR2 DEFAULT 'N',
  i_out_format   IN  VARCHAR2 DEFAULT 'YYYY-MM-DD HH24:MI'
)
  RETURN VARCHAR2 DETERMINISTIC IS
  PRAGMA UDF;
  /**
  ||----------------------------------------------------------------------------
  || Format passed date and time.
  || #param i_rendate      Rensoft date.
  || #param i_time         Time (HHMI or HHMISS).
  || #param i_time_has_ss  Indicate whether time parm includes seconds. (Y or N)
  || #param i_out_format   Date/Time format to be returned.
  || #return               Formatted date/time.
  ||----------------------------------------------------------------------------
  */
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/17/06 | RHALPAI | Original
  ||----------------------------------------------------------------------------
  */
  l_out_format  typ.t_maxvc2;
  l_time        VARCHAR2(6);
  l_date        DATE;
  l_ts          typ.t_maxvc2;
BEGIN
  l_out_format :=(CASE
                    WHEN TRIM(i_out_format) IS NULL THEN 'YYYY-MM-DD HH24:MI'
                    ELSE i_out_format
                  END);
  l_time :=(CASE NVL(TRIM(UPPER(i_time_has_ss)), 'N')
              WHEN 'N' THEN lpad_fn(i_time, 4, '0') || '00'
              ELSE lpad_fn(i_time, 6, '0')
            END
           );
  l_date := TO_DATE('19000228' || l_time, 'YYYYMMDDHH24MISS') + i_rendate;
  l_ts := TO_CHAR(l_date, l_out_format);
  RETURN(l_ts);
END formatted_date_time_fn;
/

