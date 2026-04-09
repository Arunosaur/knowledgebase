CREATE OR REPLACE TYPE "TAGGED_ORD_T" FORCE AS
  OBJECT( order_num NUMBER(11), line_num NUMBER(7,2), sub_code NUMBER(3), ord_qty NUMBER(9), adj_ord_qty NUMBER(9) )
/

