CREATE OR REPLACE FUNCTION tbl_dflt_fn(
  i_tbl_nm  IN  VARCHAR2
)
  RETURN SYS_REFCURSOR IS
  /*
  ||----------------------------------------------------------------------------
  || TBL_DFLT_FN
  ||  Return cursor of column defaults for table
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/19/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  l_sql  typ.t_maxvc2;
  l_cv   SYS_REFCURSOR;
BEGIN
  FOR l_r_col IN (SELECT   data_default
                      FROM all_tab_cols c
                     WHERE c.table_name = i_tbl_nm
                       AND c.column_id IS NOT NULL
                  ORDER BY c.column_id) LOOP
    l_sql := l_sql || ',' || NVL(RTRIM(l_r_col.data_default), 'null');
  END LOOP;
  l_sql := 'select ' || SUBSTR(l_sql, 2) || ' from dual';

  OPEN l_cv
   FOR l_sql;

  RETURN(l_cv);
END tbl_dflt_fn;
/

