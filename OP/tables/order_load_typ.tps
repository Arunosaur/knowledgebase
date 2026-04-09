CREATE OR REPLACE TYPE "ORDER_LOAD_TYP"                                          AS OBJECT (
  order_num                     NUMBER (11),
  order_type                    VARCHAR2 (1),
  div_id                        VARCHAR2 (3),
  cust_num                      VARCHAR2 (8),
  cust_round_group              VARCHAR2 (10),
  load_type                     VARCHAR2 (3),
  load_num                      VARCHAR2 (4),
  stop_num                      NUMBER (9,2),
  order_cutoff_date             NUMBER (6),
  order_cutoff_time             NUMBER (4),
  load_pricing_date             NUMBER (6),
  load_pricing_time             NUMBER (4),
  llr_cutoff_date               NUMBER (6),
  llr_cutoff_time               NUMBER (4),
  departure_date                NUMBER (6),
  departure_time                NUMBER (4),
  eta_date                      NUMBER (6),
  eta_time                      NUMBER (6)
);
/

