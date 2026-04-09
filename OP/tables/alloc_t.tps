CREATE OR REPLACE TYPE "ALLOC_T"                                          AS OBJECT( rec_type VARCHAR2(40), div VARCHAR2(2), order_num NUMBER(11), line_num NUMBER(7, 2), sub_code NUMBER(3), shp_cmplt VARCHAR2(1), cust_num VARCHAR2(8), wrk_item_num VARCHAR2(25), wrk_uom VARCHAR2(3), slot_jrsdctn VARCHAR2(3), ord_qty NUMBER(9), alloc_qty NUMBER(9), cust_cnt NUMBER(9), gov_cntl_id NUMBER(9), gov_cntl_amt NUMBER(15, 4), gov_cntl_prd NUMBER(3), gov_cntl_pt NUMBER(15, 4), partition_hash VARCHAR2(80), sort_hash VARCHAR2(80) );
/

