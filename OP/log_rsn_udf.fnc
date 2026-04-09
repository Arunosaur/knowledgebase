CREATE OR REPLACE FUNCTION log_rsn_udf(
  i_div_part  IN  NUMBER,
  i_ord_num   IN  NUMBER,
  i_ord_ln    IN  NUMBER
)
  RETURN VARCHAR2 IS
  PRAGMA UDF;
  /*
  ||----------------------------------------------------------------------------
  || LOG_RSN_UDF
  ||   Return most recent log reason for order line
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  l_c_module  CONSTANT typ.t_maxfqnm := 'LOAD_STAT_UDF';
  lar_parm             logs.tar_parm;
  l_hist_sw            VARCHAR2(1);
  l_log_rsn            VARCHAR2(100);
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'DivPart', i_div_part);
  logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
  logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
  logs.dbg('ENTRY', lar_parm);

  SELECT x.hist_sw
    INTO l_hist_sw
    FROM (SELECT 'N' AS hist_sw
            FROM ordp120b b
           WHERE b.div_part = i_div_part
             AND b.ordnob = i_ord_num
             AND b.lineb = i_ord_ln
          UNION ALL
          SELECT 'Y' AS hist_sw
            FROM ordp920b b
           WHERE b.div_part = i_div_part
             AND b.ordnob = i_ord_num
             AND b.lineb = i_ord_ln) x;

  IF l_hist_sw = 'N' THEN
    SELECT DISTINCT FIRST_VALUE(lg.rsn) OVER(ORDER BY lg.ts DESC) AS rsn
               INTO l_log_rsn
               FROM (SELECT ma.desca AS rsn, md.last_chg_ts AS ts
                       FROM mclp300d md, mclp140a ma
                      WHERE md.div_part = i_div_part
                        AND md.ordnod = i_ord_num
                        AND md.ordlnd = i_ord_ln
                        AND ma.rsncda = md.reasnd
                        AND 'Y' =(CASE
                                    WHEN EXISTS(SELECT 1
                                                  FROM ordp120b b
                                                 WHERE b.div_part = i_div_part
                                                   AND b.ordnob = i_ord_num
                                                   AND b.lineb = i_ord_ln
                                                   AND b.excptn_sw = 'Y') THEN 'Y'
                                    WHEN ma.info_sw = 'Y' THEN 'Y'
                                    ELSE 'N'
                                  END
                                 )
                     UNION ALL
                     SELECT (CASE
                               WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                               ELSE (SELECT ma.desca
                                       FROM mclp140a ma
                                      WHERE ma.rsncda = sa.rsncda)
                             END) AS rsn,
                            TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS') + sa.datea AS ts
                       FROM sysp296a sa
                      WHERE sa.div_part = i_div_part
                        AND sa.ordnoa = i_ord_num
                        AND sa.linea = i_ord_ln) lg;
  ELSE
    SELECT DISTINCT FIRST_VALUE(lg.rsn) OVER(ORDER BY lg.ts DESC) AS rsn
               INTO l_log_rsn
               FROM (SELECT ma.desca AS rsn, md.last_chg_ts AS ts
                       FROM mclp900d md, mclp140a ma
                      WHERE md.div_part = i_div_part
                        AND md.ordnod = i_ord_num
                        AND md.ordlnd = i_ord_ln
                        AND ma.rsncda = md.reasnd
                        AND 'Y' =(CASE
                                    WHEN EXISTS(SELECT 1
                                                  FROM ordp920b b
                                                 WHERE b.div_part = i_div_part
                                                   AND b.ordnob = i_ord_num
                                                   AND b.lineb = i_ord_ln
                                                   AND b.excptn_sw = 'Y') THEN 'Y'
                                    WHEN ma.info_sw = 'Y' THEN 'Y'
                                    ELSE 'N'
                                  END
                                 )
                     UNION ALL
                     SELECT (CASE
                               WHEN sa.rsncda = 'Other' THEN sa.rsntxa
                               ELSE (SELECT ma.desca
                                       FROM mclp140a ma
                                      WHERE ma.rsncda = sa.rsncda)
                             END) AS rsn,
                            TO_DATE('19000228' || LPAD(sa.timea, 6, '0'), 'YYYYMMDDHH24MISS') + sa.datea AS ts
                       FROM sysp996a sa
                      WHERE sa.div_part = i_div_part
                        AND sa.ordnoa = i_ord_num
                        AND sa.linea = i_ord_ln) lg;
  END IF;   -- l_hist_sw = 'N'

  timer.stopme(l_c_module || env.get_session_id);
  logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  RETURN(l_log_rsn);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN(NULL);
END log_rsn_udf;
/

