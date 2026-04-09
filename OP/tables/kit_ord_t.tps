CREATE OR REPLACE TYPE "KIT_ORD_T"                                          AS OBJECT( ord_stat VARCHAR2(1), div_id VARCHAR2(2), llr_dt NUMBER(6), kit_typ VARCHAR2(3), ord_typ VARCHAR2(1), kit_item_num VARCHAR2(6), cust_num VARCHAR2(8), load_num VARCHAR2(4), stop_num NUMBER(2), eta_date NUMBER(6), po_num VARCHAR2(30), comp_item_num VARCHAR2(6), comp_qty NUMBER(9), order_num NUMBER(11), order_ln NUMBER(7,2), item_num VARCHAR2(9), uom VARCHAR2(3), ord_qty NUMBER(9), ratio NUMBER(9,4), seq NUMBER(9) );
/

