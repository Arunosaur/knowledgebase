CREATE OR REPLACE FUNCTION to_list_fn(
  i_cur        IN  SYS_REFCURSOR,
  i_delimiter  IN  VARCHAR2 DEFAULT ','
)
  RETURN VARCHAR2 IS
  /**
  ||----------------------------------------------------------------------------
  || Create a delimited list of strings from cursor.
  || #param p_cur          Cursor of varchar items to list.
  || #param p_delimiter    Character to delimit each item in list.
  || #return               Delimited list of varchar items.
  ||----------------------------------------------------------------------------
  */
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/17/06 | rhalpai | Original
  || 09/19/11 | rhalpai | Added logic to close cursor before returning. PIR7990
  ||----------------------------------------------------------------------------
  */
  l_item  typ.t_maxvc2;
  l_list  typ.t_maxvc2;
BEGIN
  LOOP
    FETCH i_cur
     INTO l_item;

    EXIT WHEN i_cur%NOTFOUND;
    l_list := l_list ||(CASE
                          WHEN l_list IS NULL THEN l_item
                          ELSE i_delimiter || l_item
                        END);
  END LOOP;

  CLOSE i_cur;

  RETURN(l_list);
END to_list_fn;
/

