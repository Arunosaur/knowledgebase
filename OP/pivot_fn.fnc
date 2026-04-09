CREATE OR REPLACE FUNCTION pivot_fn(
  i_num_rows  IN  NUMBER,
  i_start     IN  NUMBER DEFAULT 1,
  i_step      IN  NUMBER DEFAULT 1
)
  RETURN type_ntab PARALLEL_ENABLE PIPELINED IS
  /**
  ||----------------------------------------------------------------------------
  || Returns a table containing the specified number of rows of number values.
  || The values may begin at the specified starting value and increment by the
  || specified step value.
  || #param i_num_rows  Number of rows to create.
  || #param i_start     Starting value.
  || #param i_step      Stepping increment.
  || #return            Table of number values
  ||
  || Sample usage:
  ||   SELECT t.COLUMN_VALUE FROM TABLE(pivot_fn(20,100,5)) t;
  ||----------------------------------------------------------------------------
  */
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/13/05 | RHALPAI | Original
  ||----------------------------------------------------------------------------
  */
  l_cnt  PLS_INTEGER := 0;
  l_val  PLS_INTEGER := 0;
BEGIN
  l_val := i_start - i_step;
  WHILE l_cnt < i_num_rows LOOP
    l_cnt := l_cnt + 1;
    l_val := l_val + i_step;
    PIPE ROW(l_val);
  END LOOP;
  RETURN;
END pivot_fn;
/

