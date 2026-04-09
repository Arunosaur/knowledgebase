CREATE OR REPLACE FUNCTION get_val_fn(
  i_curr_val             IN  VARCHAR2,
  i_new_val              IN  VARCHAR2,
  i_use_passed_nulls_sw  IN  VARCHAR2
)
  RETURN VARCHAR2 DETERMINISTIC IS
  PRAGMA UDF;
  /*
  ||----------------------------------------------------------------------------
  || GET_VAL_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
BEGIN
  RETURN(CASE
           WHEN i_use_passed_nulls_sw = 'Y' THEN i_new_val
           WHEN i_new_val = 'NULL' THEN NULL
           ELSE NVL(i_new_val, i_curr_val)
         END
        );
END get_val_fn;
/

