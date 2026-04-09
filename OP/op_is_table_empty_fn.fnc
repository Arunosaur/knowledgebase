CREATE OR REPLACE FUNCTION op_is_table_empty_fn(
  i_tbl_nm  IN  VARCHAR2
)
  RETURN BOOLEAN AS
  /**
  ||----------------------------------------------------------------------------
  || Returns whether or not a table contains data.
  || This is much more efficient than doing a "COUNT(*)"! If function fails
  || for some reason a NULL will be returned.
  || #param p_tbl_name        Table to check for data content.
  || #return                  Table contains data? True/False/(Null if failure)
  ||----------------------------------------------------------------------------
  */

  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/04 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  l_c_module  typ.t_maxfqnm := 'OP_IS_TABLE_EMPTY_FN';
  lar_parm    logs.tar_parm;
  l_tbl_cnt   PLS_INTEGER;
  l_cv        SYS_REFCURSOR;
BEGIN
  logs.add_parm(lar_parm, 'TblNm', i_tbl_nm);

  BEGIN
    OPEN l_cv
     FOR 'SELECT NVL(MAX(1), 0) FROM DUAL WHERE EXISTS(SELECT 1 FROM ' || i_tbl_nm || ')';

    FETCH l_cv
     INTO l_tbl_cnt;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END;

  RETURN(l_tbl_cnt = 0);
END op_is_table_empty_fn;
/

