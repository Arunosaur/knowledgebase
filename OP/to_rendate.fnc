CREATE OR REPLACE FUNCTION to_rendate(
  i_date         IN  VARCHAR2,
  i_date_format  IN  VARCHAR2 DEFAULT NULL
)
  RETURN NUMBER DETERMINISTIC IS
  PRAGMA UDF;
  /**
  ||----------------------------------------------------------------------------
  || Get the corresponding Rensoft date value for a formatted date string
  || #param i_date            Formatted date string
  || #param i_date_format     Date string format
  || #return                  Rensoft date value
  ||----------------------------------------------------------------------------
  **/
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/24/01 | JUSTANI | Original
  || 12/11/01 | JBARTON | Changed default date on to be 000001 instead of
  ||                    | 999999 to correct problems with date assignments in
  ||                    | order receipt.
  || 06/27/02 | SNAGABH | Use seed value for date conversion instead of
  ||                    | mclane_date_convert table. Consolidated date
  ||                    | conversion functionality from to_rendate5 and
  ||                    | to_rendate6 functions into this function.
  || 01/09/03 | SNAGABH | Added additional option to pass the format in which
  ||                    | the data value is being passed. This helps eliminate
  ||                    | unnecessary conversions to the default date format.
  || 07/28/04 | rhalpai | Reformatted and added Deterministic keyword to allow
  ||                    | the optimizer to avoid redundant calls with the same
  ||                    | parms by re-using the previous result.
  ||----------------------------------------------------------------------------
  */
  l_rensoft_date       NUMBER(6);
  l_rensoft_seed_date  DATE         := TO_DATE('19000228', 'YYYYMMDD');
  l_date_string        VARCHAR2(8);
  l_year               PLS_INTEGER;
  l_trimmed_date       VARCHAR2(20);
BEGIN
  l_trimmed_date := TRIM(i_date);

  IF i_date_format IS NULL THEN
    BEGIN
      IF LENGTH(l_trimmed_date) = 8 THEN
        l_rensoft_date := TO_DATE(l_trimmed_date, 'YYYYMMDD') - l_rensoft_seed_date;
      ELSIF LENGTH(l_trimmed_date) = 6 THEN
        l_year := TO_NUMBER(SUBSTR(l_trimmed_date, 1, 2));

        -- Apply Y2K date conversion logic to determine the Century
        IF l_year > 49 THEN
          l_date_string := '19' || l_trimmed_date;
        ELSE
          l_date_string := '20' || l_trimmed_date;
        END IF;

        l_rensoft_date := TO_DATE(l_date_string, 'YYYYMMDD') - l_rensoft_seed_date;
      ELSE
        l_rensoft_date := 1;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        l_rensoft_date := 1;
    END;
  ELSE
    BEGIN
      l_rensoft_date := FLOOR(TO_DATE(l_trimmed_date, i_date_format) - l_rensoft_seed_date);
    EXCEPTION
      WHEN OTHERS THEN
        l_rensoft_date := 1;
    END;
  END IF;

  RETURN(l_rensoft_date);
END to_rendate;
/

