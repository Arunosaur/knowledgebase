CREATE OR REPLACE FUNCTION lpad_fn(
  i_val  IN  VARCHAR2,
  i_len  IN  NUMBER,
  i_pad  IN  VARCHAR2 DEFAULT ' '
)
  RETURN VARCHAR2 DETERMINISTIC IS
  /**
  ||----------------------------------------------------------------------------
  || Takes the value passed and pads it within to the left with the pad character.
  || #param i_val      Value to be padded.
  || #param i_len      Length of return value.
  || #param i_pad      Pad character. Default is space.
  || #return           Padded return value.
  ||----------------------------------------------------------------------------
  */
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/09/05 | RHALPAI | Original
  ||----------------------------------------------------------------------------
  */
BEGIN
  RETURN(LPAD(NVL(i_val, i_pad), i_len, i_pad));
END lpad_fn;
/

