CREATE OR REPLACE FUNCTION cntnr_id_seq_fn(
  i_div  IN  VARCHAR2
)
  RETURN VARCHAR2 IS
  PRAGMA UDF;
  l_id   VARCHAR2(6);
BEGIN
  EXECUTE IMMEDIATE 'SELECT LPAD(cntnr_id_' || i_div || '_seq.nextval, 6, ''0'') FROM DUAL'
               INTO l_id;

  RETURN(l_id);
END cntnr_id_seq_fn;
/

