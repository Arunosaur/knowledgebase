CREATE OR REPLACE FUNCTION from_rendate(
  i_rensoft_date  IN  NUMBER,
  i_date_format   IN  VARCHAR2 DEFAULT 'YYYYMMDD'
)
  RETURN VARCHAR2 DETERMINISTIC IS
  PRAGMA UDF;
  /**
  ||----------------------------------------------------------------------------
  || Get the corresponding formatted date string for a Rensoft date value
  || #param i_rensoft_date    Rensoft date value
  || #param i_date_format     Date string format
  || #return                  Formatted date string
  ||----------------------------------------------------------------------------
  **/
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/24/01 | JUSTANI | Original
  || 06/27/02 | SNAGABH | Use seed value for date conversion instead of
  ||                    | mclane_date_convert table. Consolidated date
  ||                    | conversion functionality in ren_date_to_cal_date
  ||                    | function into this function.
  || 07/28/04 | rhalpai | Reformatted and added Deterministic keyword to allow
  ||                    | the optimizer to avoid redundant calls with the same
  ||                    | parms by re-using the previous result.
  ||----------------------------------------------------------------------------
  */
  l_rensoft_seed_date  DATE         := TO_DATE('19000228', 'YYYYMMDD');
  l_date               VARCHAR2(24);
BEGIN
  BEGIN
    l_date := TO_CHAR((l_rensoft_seed_date + i_rensoft_date), i_date_format);
  EXCEPTION
    WHEN OTHERS THEN
      l_date := '29990101';
  END;

  RETURN(l_date);
END from_rendate;
/

