CREATE OR REPLACE FUNCTION val_at_idx_fn(
  i_t_tab  IN  type_stab,
  i_idx    IN  PLS_INTEGER
)
  RETURN VARCHAR2 IS
  /**
  ||----------------------------------------------------------------------------
  || Returns value at index for table object.
  || Will null when table object is null or index not found within table object.
  || #param i_t_tab            PLSQL VARCHAR2 table object
  || #param i_idx              Index to find value in table object
  || #return                   VARCHAR2 value found at index in table object
  ||----------------------------------------------------------------------------
  **/
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  l_val  typ.t_maxvc2;
BEGIN
  IF (    i_t_tab IS NOT NULL
      AND i_t_tab.EXISTS(i_idx)) THEN
    l_val := i_t_tab(i_idx);
  END IF;   -- i_t_tab IS NOT NULL AND i_t_tab.EXISTS(l_idx)

  RETURN(l_val);
END val_at_idx_fn;
/

