CREATE OR REPLACE FUNCTION from_rendate_dt(
  i_rensoft_date  IN  NUMBER
)
  RETURN DATE DETERMINISTIC IS
  PRAGMA UDF;
  /**
  ||----------------------------------------------------------------------------
  || Get the corresponding date object for a Rensoft date value
  || #param i_rensoft_date    Rensoft date value
  || #return                  Date
  ||----------------------------------------------------------------------------
  **/
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/24/03 | rhalpai | Original
  || 07/28/04 | rhalpai | Reformatted and added Deterministic keyword to allow
  ||                    | the optimizer to avoid redundant calls with the same
  ||                    | parms by re-using the previous result.
  ||----------------------------------------------------------------------------
  */
  l_rensoft_seed_date  DATE := TO_DATE('19000228', 'YYYYMMDD');
  l_date               DATE;
BEGIN
  BEGIN
    l_date :=(l_rensoft_seed_date + i_rensoft_date);
  EXCEPTION
    WHEN OTHERS THEN
      l_date := TO_DATE('29990101', 'YYYYMMDD');
  END;

  RETURN(l_date);
END from_rendate_dt;
/

