CREATE OR REPLACE FUNCTION load_stat_udf(
  i_div_part  IN  NUMBER,
  i_llr_dt    IN  DATE,
  i_load_num  IN  VARCHAR2,
  i_stop_num  IN  NUMBER DEFAULT NULL
)
  RETURN VARCHAR2 IS
  PRAGMA UDF;
  /*
  ||----------------------------------------------------------------------------
  || LOAD_STAT_UDF
  ||   Return status of load or load/stop for LLR date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/16 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT typ.t_maxfqnm := 'LOAD_STAT_UDF';
  lar_parm             logs.tar_parm;
  l_load_stat          VARCHAR2(30);
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'DivPart', i_div_part);
  logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
  logs.add_parm(lar_parm, 'LoadNum', i_load_num);
  logs.add_parm(lar_parm, 'StopNum', i_stop_num);
  logs.dbg('ENTRY', lar_parm);

  SELECT COALESCE((SELECT DECODE(lc.load_status,
                                 'P', 'Billing In Process',
                                 'T', 'Load Close In Process',
                                 'E', 'Error',
                                 'X', 'Reserved'
                                ) AS stat
                     FROM load_clos_cntrl_bc2c lc
                    WHERE lc.div_part = i_div_part
                      AND lc.llr_dt = i_llr_dt
                      AND lc.load_num = i_load_num
                      AND i_stop_num IS NULL
                      AND lc.load_status IN('P', 'T', 'E', 'X')),
                  (SELECT (CASE
                             WHEN(    zz.low = '1'
                                  AND zz.hi = '1') THEN 'Open'
                             WHEN zz.hi = '2' THEN 'Billing In Process'
                             WHEN(    zz.low = '3'
                                  AND zz.hi = '3') THEN 'Billed'
                             WHEN zz.hi = '4' THEN 'Shipped'
                             WHEN(    zz.low = '1'
                                  AND zz.hi = '3') THEN(CASE
                                                          WHEN(    zz.partl_sw = 'Y'
                                                               AND zz.opn_sw = 'Y') THEN 'Partial w/Open'
                                                          WHEN(    zz.partl_sw = 'Y'
                                                               AND zz.opn_sw = 'N') THEN 'Partial'
                                                          ELSE 'Billed w/Open'
                                                        END
                                                       )
                             ELSE 'Unknown'
                           END
                          ) AS status
                     FROM (SELECT   q.load_depart_sid, MIN(q.stat) AS low, MAX(q.stat) AS hi,
                                    MAX(DECODE(q.hdr_stat, 'P', 'Y', 'N')) AS partl_sw,
                                    MAX(DECODE(q.hdr_stat, 'O', 'Y', 'N')) AS opn_sw
                               FROM (SELECT   ld.load_depart_sid, a.stata AS hdr_stat,
                                              DECODE(b.statb, 'O', '1', 'R', '3', 'A', '4', '2') AS stat
                                         FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
                                        WHERE ld.div_part = i_div_part
                                          AND ld.llr_dt = i_llr_dt
                                          AND ld.load_num = i_load_num
                                          AND se.div_part = ld.div_part
                                          AND se.load_depart_sid = ld.load_depart_sid
                                          AND se.stop_num = NVL(i_stop_num, se.stop_num)
                                          AND a.div_part = se.div_part
                                          AND a.load_depart_sid = se.load_depart_sid
                                          AND a.custa = se.cust_id
                                          AND b.div_part = a.div_part
                                          AND b.ordnob = a.ordnoa
                                          AND b.statb NOT IN('I', 'S', 'C')
                                     GROUP BY ld.load_depart_sid, a.stata, b.statb) q
                           GROUP BY q.load_depart_sid) zz
                    WHERE ROWNUM = 1),
                  'Unknown'
                 )
    INTO l_load_stat
    FROM DUAL;

  timer.stopme(l_c_module || env.get_session_id);
  logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  RETURN(l_load_stat);
END load_stat_udf;
/

