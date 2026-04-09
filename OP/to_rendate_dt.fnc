CREATE OR REPLACE FUNCTION to_rendate_dt(
  i_date  IN  DATE
)
  RETURN NUMBER DETERMINISTIC IS
  PRAGMA UDF;
  /**
  ||----------------------------------------------------------------------------
  || Get the corresponding Rensoft date value for a date object
  || #param p_date            Date
  || #return                  Rensoft date value
  ||----------------------------------------------------------------------------
  **/
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/10/03 | SNAGABH | Original
  || 07/28/04 | rhalpai | Reformatted and added Deterministic keyword to allow
  ||                    | the optimizer to avoid redundant calls with the same
  ||                    | parms by re-using the previous result.
  ||----------------------------------------------------------------------------
  */
  l_rensoft_seed_date  DATE      := TO_DATE('19000228', 'YYYYMMDD');
  l_rensoft_date       NUMBER(6);
BEGIN
  BEGIN
    l_rensoft_date := TRUNC(i_date) - l_rensoft_seed_date;
  EXCEPTION
    WHEN OTHERS THEN
      l_rensoft_date := 1;
  END;

  RETURN(l_rensoft_date);
END to_rendate_dt;
/

