CREATE OR REPLACE FUNCTION eta_ts_fn(
  i_div_part  IN  NUMBER,
  i_llr_dt    IN  DATE,
  i_load_num  IN  VARCHAR2,
  i_cust_id   IN  VARCHAR2
)
  RETURN DATE IS
  PRAGMA UDF;
  /*
  ||---------------------------------------------------------------------------
  || ETA_TS_FN
  ||  Return ETA date/time assignment for LLR/Load/Cust
  ||---------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||---------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||---------------------------------------------------------------------------
  || 11/25/15 | rhalpai | Original for PIR14838
  ||---------------------------------------------------------------------------
  */
  l_llr_ts     DATE;
  l_depart_ts  DATE;
  l_eta_ts     DATE;
BEGIN
  op_order_load_pk.get_llr_depart_sp(i_div_part, i_llr_dt, i_load_num, l_llr_ts, l_depart_ts);

  SELECT COALESCE((SELECT cro.eta_ts
                     FROM cust_rte_ovrrd_rt3c cro
                    WHERE cro.div_part = i_div_part
                      AND cro.llr_dt = i_llr_dt
                      AND cro.load_num = i_load_num
                      AND cro.cust_id = i_cust_id
                      AND cro.eta_ovrrd_sw = 'Y'),
                  (SELECT x.eta_ts +(CASE
                                       WHEN x.eta_ts < l_depart_ts THEN 7
                                       ELSE 0
                                     END)
                     FROM (SELECT NEXT_DAY(TO_DATE(TO_CHAR(l_depart_ts - 1, 'YYYYMMDD') || LPAD(md.etad, 4, '0'),
                                                   'YYYYMMDDHH24MI'
                                                  ),
                                           md.dayrcd
                                          )
                                  + NVL(md.wkoffd, 0) * 7 AS eta_ts
                             FROM mclp040d md
                            WHERE md.div_part = i_div_part
                              AND md.loadd = i_load_num
                              AND md.custd = i_cust_id) x)
                 ) AS eta_ts
    INTO l_eta_ts
    FROM DUAL;

  RETURN(l_eta_ts);
END eta_ts_fn;
/

