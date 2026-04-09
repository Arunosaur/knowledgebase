CREATE OR REPLACE PACKAGE op_forecast_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE fcast_sp(
    i_div     IN  VARCHAR2,
    i_run_ts  IN  DATE DEFAULT SYSDATE
  );
END op_forecast_pk;
/

CREATE OR REPLACE PACKAGE BODY op_forecast_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_fcast IS RECORD(
    catlg_num   NUMBER,
    item_fcast  NUMBER,
    sun_qty     NUMBER,
    mon_qty     NUMBER,
    tue_qty     NUMBER,
    wed_qty     NUMBER,
    thu_qty     NUMBER,
    fri_qty     NUMBER,
    sat_qty     NUMBER
  );

  TYPE g_cvt_fcast IS REF CURSOR
    RETURN g_rt_fcast;

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || HIST_FCAST_CUR_FN
  ||  Return Item Sales History Forecast cursor
  ||
  ||  Business Rules:
  ||    Beginning of the week is Sunday.
  ||    Look a 4 weeks of history prior to beginning of current week.
  ||    Customer's item sales for each week in history will be mapped to their
  ||      current billing schedule by LLR day.
  ||    Map customer item sales occurrence within week in history to their
  ||      current billing schedule
  ||      (i.e. current schedule is MON/WED while a week in history is TUE/THU.
  ||       Map sales from TUE to MON and THU to WED) when the number of billings
  ||       for a week in history match the number of loads currently assigned
  ||       to the customer)
  ||    When the number of billings for a week in history do not match the
  ||      customer's current schedule then sum the sales for the week in
  ||      history and divide it evenly across the current LLR schedule.
  ||
  ||  Notes:
  ||    Customer
  ||      All active/hold customers (excluding internal corps 997,998) with
  ||        load assignments for division
  ||    Customer LLR
  ||      Current customer LLR assignments.
  ||      DOW is the numeric day of week.
  ||      LLR_SEQ is needed later to match an occurrence within a week in
  ||        history to the corresponding occurrence of the customer's current
  ||        billing schedule.
  ||      LLR_CNT is needed later to compare the the number of billings per
  ||        week in history to the number of billing per week currently
  ||        assigned to the customer.
  ||    Customer History
  ||      Last 4 weeks of billing history used to determine mapping of
  ||        customer LLR dates in history to the current LLR schedule.
  ||      WK is week 1 thru 4.
  ||      BOW is the beginning of week LLR date.
  ||      LLR_SEQ is needed later to match item sales in history to the
  ||        current schedule.
  ||      LLR_CNT is needed later to compare the the number of billings per
  ||        week in history to the number of billing per week currently
  ||        assigned to the customer.
  ||    Customer History LLR Map
  ||      Maps customer sales history to current LLR schedule.
  ||    Item Customer Week
  ||      Gathers total item customer sales history by LLR and maps it to each
  ||        customer's current LLR schedule.
  ||      DOW_TTL is either the total customer item sales for each day of week
  ||        in history or (when they don't map appropriately) the average for
  ||        the week.
  ||      TTL is the total items sales by customer for the week.
  ||    Item Customer Forecast
  ||      Totals sales by item, customer, day of week.
  ||    Item Forecast
  ||      Totals item sales by day of week.
  ||      Ensures all days of the week are present in case there are no sales
  ||        for an item for a day.
  ||      DY_FCST maps the day of week (DOW) to the day of week from the item
  ||        customer forecast and returns (DOW_TTL / WK_TTL * ITEM_FCST) or
  ||        returns 0 when no sales exist for that day.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/25/15 | rhalpai | Original for PIR14738
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | PIR15885
  ||----------------------------------------------------------------------------
  */
  FUNCTION hist_fcast_cur_fn(
    i_div_part  IN  NUMBER,
    i_run_ts    IN  DATE
  )
    RETURN g_cvt_fcast IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_FORECAST_PK.HIST_FCAST_CUR_FN';
    lar_parm             logs.tar_parm;
    l_beg_of_wk_dt       DATE;
    l_cv                 g_cvt_fcast;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RunTs', i_run_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_beg_of_wk_dt := TRUNC(i_run_ts) -(TO_NUMBER(TO_CHAR(i_run_ts, 'D')) - 1);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
        WITH cust AS
             (SELECT c.acnoc AS cust_id
                FROM sysp200c c, mclp020b cx
               WHERE c.div_part = i_div_part
                 AND c.statc IN('1', '3')
                 AND cx.div_part = c.div_part
                 AND cx.custb = c.acnoc
                 AND cx.corpb NOT IN(998, 997)
                 AND EXISTS(SELECT 1
                              FROM mclp040d md, mclp120c mc
                             WHERE md.div_part = cx.div_part
                               AND md.custd = cx.custb
                               AND mc.div_part = md.div_part
                               AND mc.loadc = md.loadd)),
             cust_llr AS
             (SELECT cust.cust_id, mc.llrcdc AS llr_day,
                     DECODE(mc.llrcdc, 'SUN', 1, 'MON', 2, 'TUE', 3, 'WED', 4, 'THU', 5, 'FRI', 6, 'SAT', 7) AS dow,
                     NEXT_DAY(l_beg_of_wk_dt - 1, mc.llrcdc) AS llr_dt,
                     DENSE_RANK() OVER(PARTITION BY md.custd ORDER BY NEXT_DAY(l_beg_of_wk_dt - 1, mc.llrcdc)) AS llr_seq,
                     COUNT(*) OVER(PARTITION BY md.custd) AS llr_cnt
                FROM cust, mclp040d md, mclp120c mc
               WHERE md.div_part = i_div_part
                 AND md.custd = cust.cust_id
                 AND mc.div_part = md.div_part
                 AND mc.loadc = md.loadd),
             cust_hist AS
             (SELECT   w.cust_id, DATE '1900-02-28' + a.ctofda AS llr_dt, w.wk,
                       DENSE_RANK() OVER(PARTITION BY w.cust_id, w.wk ORDER BY a.ctofda) AS llr_seq,
                       COUNT(*) OVER(PARTITION BY w.cust_id, w.wk) AS llr_cnt
                  FROM (SELECT cl.cust_id, cl.llr_cnt, t.wk, l_beg_of_wk_dt -(t.wk * 7) AS bow
                          FROM (SELECT     LEVEL AS wk
                                      FROM DUAL
                                CONNECT BY LEVEL <= 4) t,
                               (SELECT DISTINCT cust_llr.cust_id, cust_llr.llr_cnt
                                           FROM cust_llr) cl) w,
                               ordp900a a
                 WHERE a.div_part = i_div_part
                   AND a.custa = w.cust_id
                   AND DATE '1900-02-28' + a.ctofda BETWEEN w.bow AND w.bow + 6
                   AND a.dsorda = 'R'
                   AND a.stata = 'A'
                   AND EXISTS(SELECT 1
                                FROM ordp920b b
                               WHERE b.div_part = a.div_part
                                 AND b.ordnob = a.ordnoa
                                 AND b.pckqtb > 0)
              GROUP BY w.cust_id, w.llr_cnt, w.wk, a.ctofda),
             cust_hist_llr_map AS
             (SELECT ch.cust_id, ch.llr_dt, cl.dow, cl.llr_cnt,
                     (CASE
                        WHEN(    cl.llr_cnt = ch.llr_cnt
                             AND cl.llr_seq = ch.llr_seq) THEN 'Y'
                        ELSE 'N'
                      END) AS map_sw
                FROM cust_llr cl, cust_hist ch
               WHERE cl.cust_id = ch.cust_id
                 AND (   cl.llr_cnt <> ch.llr_cnt
                      OR cl.llr_seq = ch.llr_seq)),
             itm_cust_wk AS
             (SELECT DISTINCT dif.catlg_num, dif.item_fcast, m.cust_id, m.dow,
                              DECODE(m.map_sw,
                                     'Y', SUM(b.pckqtb) OVER(PARTITION BY dif.catlg_num, m.cust_id, m.dow),
                                     SUM(b.pckqtb) OVER(PARTITION BY dif.catlg_num, m.cust_id) / m.llr_cnt
                                    ) AS dow_ttl
                         FROM div_item_fcast_op2f dif, cust_hist_llr_map m, ordp900a a, ordp920b b
                        WHERE dif.div_part = i_div_part
                          AND a.div_part = i_div_part
                          AND a.custa = m.cust_id
                          AND a.ctofda = m.llr_dt - DATE '1900-02-28'
                          AND a.dsorda = 'R'
                          AND a.stata = 'A'
                          AND b.div_part = a.div_part
                          AND b.ordnob = a.ordnoa
                          AND b.subrcb < 999
                          AND b.pckqtb > 0
                          AND b.statb = 'A'
                          AND b.orditb = dif.catlg_num),
             itm_cust_fcast AS
             (SELECT   icw.catlg_num, icw.item_fcast, icw.cust_id, icw.dow, SUM(icw.dow_ttl) AS dow_ttl
                  FROM itm_cust_wk icw
                 WHERE icw.dow_ttl > 0
              GROUP BY icw.catlg_num, icw.item_fcast, icw.cust_id, icw.dow),
             itm_fcast AS
             (SELECT   x.catlg_num, x.item_fcast, x.dy, ROUND(SUM(x.dy_fcast)) AS dy_fcast
                  FROM (SELECT DISTINCT icf.catlg_num, icf.item_fcast, t.dy,
                                        DECODE(t.dow,
                                               icf.dow,(SUM(icf.dow_ttl) OVER(PARTITION BY icf.catlg_num, icf.dow)
                                                        / SUM(icf.dow_ttl) OVER(PARTITION BY icf.catlg_num)
                                                        * icf.item_fcast
                                               ),
                                               0
                                              ) AS dy_fcast
                                   FROM (SELECT     LEVEL AS dow,
                                                    DECODE(LEVEL,
                                                           1, 'SUN',
                                                           2, 'MON',
                                                           3, 'TUE',
                                                           4, 'WED',
                                                           5, 'THU',
                                                           6, 'FRI',
                                                           7, 'SAT'
                                                          ) AS dy
                                               FROM DUAL
                                         CONNECT BY LEVEL <= 7) t,
                                        itm_cust_fcast icf) x
              GROUP BY x.catlg_num, x.item_fcast, x.dy)
        SELECT   *
            FROM (SELECT f.catlg_num, f.item_fcast, f.dy, f.dy_fcast
                    FROM itm_fcast f)
           PIVOT (SUM(dy_fcast)
             FOR dy IN('SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'))
        ORDER BY catlg_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END hist_fcast_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || DIST_FCAST_CUR_FN
  ||  Return Unbilled Dist Item Forecast cursor
  ||
  ||  Business Rules:
  ||    Runs for current date plus 1 for 7 days. (i.e. on TUE run for WED-TUE)
  ||    Assume 100% fulfillment of order quantity.
  ||    Include only unbilled regular distribution orders (no P00 dists).
  ||    Include distributions on customer's assigned loads and those not yet
  ||      attached (i.e. on DIST load).
  ||    Assume a regular order will be available for the distribution to attach
  ||      on all LLR days assigned to customer.
  ||
  ||  Notes:
  ||    Dt
  ||      Builds the range of LLR dates to be included
  ||    Assigned
  ||      Total order quantity by item, LLR day for distributions on customer's
  ||        assigned load.
  ||    Item Cust Load Qty
  ||      Total order quantity by item, customer, load for unassigned
  ||        distributions on DIST load.
  ||      Since distributions are attached to regular orders with an invoice
  ||        date (ETA date) >= distribution ship date, the LLRDate/Load/Cust
  ||        are used to determine the ETA to see if the distribution will
  ||        attach on that LLR day.
  ||      To limit data since distributions are loaded way in advance, only
  ||        select those with ship date within 7 days of LLR dates.
  ||    Item Cust Qty
  ||      Filter to only distributions with ship date <= ETA date and determine
  ||        first LLR day for item, customer. Assumption is that distributions
  ||        will bill on their first available billing.
  ||    Unassigned
  ||      Total order quantity by item, LLR day for unassigned distributions
  ||        on DIST load.
  ||    Item Forecast
  ||      Totals item sales by day of week.
  ||      Ensures all days of the week are present in case there are no orders
  ||        for an item for a day.
  ||      DY_FCST maps the day of week (DOW) to the day of week from the item
  ||        customer forecast and returns the total order quantity or zero
  ||        when no orders exist for that day.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/25/15 | rhalpai | Original for PIR14738
  || 07/05/16 | rhalpai | Change logic to return distinct values within
  ||                    | item_cust_qty portion of cursor.
  ||                    | Change logic to use UTIL_PK for input parameters.
  ||                    | PIR15885
  || 02/16/17 | rhalpai | Change logic within cursor to not overstate forecast
  ||                    | by a factor of 7. PIR16942
  ||----------------------------------------------------------------------------
  */
  FUNCTION dist_fcast_cur_fn(
    i_div_part  IN  NUMBER,
    i_run_ts    IN  DATE
  )
    RETURN g_cvt_fcast IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_FORECAST_PK.DIST_FCAST_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 g_cvt_fcast;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RunTs', i_run_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
        WITH dt AS
             (SELECT     TRUNC(i_run_ts) + LEVEL AS llr_dt, TO_CHAR(TRUNC(i_run_ts) + LEVEL, 'DY') AS llr_day
                    FROM DUAL
              CONNECT BY LEVEL <= 7),
             assigned AS
             (SELECT   dif.catlg_num, dif.item_fcast, dt.llr_day, SUM(b.ordqtb) AS ord_qty
                  FROM dt, load_depart_op1f ld, mclp120c mc, stop_eta_op1g se, sysp200c c, mclp040d md, ordp100a a, ordp120b b,
                       div_item_fcast_op2f dif
                 WHERE dif.div_part = i_div_part
                   AND ld.div_part = i_div_part
                   AND ld.llr_dt = dt.llr_dt
                   AND mc.div_part = ld.div_part
                   AND mc.loadc = ld.load_num
                   AND mc.test_bil_load_sw = 'N'
                   AND se.div_part = ld.div_part
                   AND se.load_depart_sid = ld.load_depart_sid
                   AND c.div_part = se.div_part
                   AND c.acnoc = se.cust_id
                   AND c.statc IN('1', '3')
                   AND md.div_part = ld.div_part
                   AND md.loadd = ld.load_num
                   AND md.custd = se.cust_id
                   AND a.div_part = se.div_part
                   AND a.load_depart_sid = se.load_depart_sid
                   AND a.custa = se.cust_id
                   AND a.dsorda = 'D'
                   AND a.stata = 'O'
                   AND a.excptn_sw = 'N'
                   AND b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.orditb = dif.catlg_num
              GROUP BY dif.catlg_num, dif.item_fcast, dt.llr_day),
             item_cust_load_qty AS
             (SELECT   dif.catlg_num, dif.item_fcast, dt.llr_day, dt.llr_dt, a.custa AS cust_id,
                       NVL(NEXT_DAY(DATE '1900-02-28' + a.shpja - 1, c.dist_frst_day), DATE '1900-02-28' + a.shpja) AS ship_dt,
                       eta_ts_fn(i_div_part, dt.llr_dt, md.loadd, a.custa) AS eta_ts, md.loadd AS load_num,
                       SUM(b.ordqtb) AS ord_qty
                  FROM dt, load_depart_op1f ld, mclp040d md, mclp120c mc, sysp200c c, ordp100a a, ordp120b b,
                       div_item_fcast_op2f dif
                 WHERE dif.div_part = i_div_part
                   AND ld.div_part = i_div_part
                   AND ld.llr_ts = DATE '1900-01-01'
                   AND ld.load_num = 'DIST'
                   AND md.div_part = i_div_part
                   AND mc.div_part = i_div_part
                   AND mc.loadc = md.loadd
                   AND mc.lbsgpc = DECODE(md.prod_typ, 'BTH', mc.lbsgpc, 'GRO', 'N', 'GMP', 'Y')
                   AND mc.llrcdc = dt.llr_day
                   AND mc.test_bil_load_sw = 'N'
                   AND mc.aadisc = 'Y'
                   AND a.div_part = ld.div_part
                   AND a.load_depart_sid = ld.load_depart_sid
                   AND a.custa = c.acnoc
                   AND a.dsorda = 'D'
                   AND a.shpja <= dt.llr_dt + 7 - DATE '1900-02-28'
                   AND a.ldtypa = DECODE(md.prod_typ, 'BTH', a.ldtypa, md.prod_typ)
                   AND a.stata = 'O'
                   AND a.excptn_sw = 'N'
                   AND c.div_part = i_div_part
                   AND c.acnoc = md.custd
                   AND c.statc IN('1', '3')
                   AND b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.orditb = dif.catlg_num
              GROUP BY dif.catlg_num, dif.item_fcast, dt.llr_day, dt.llr_dt, a.custa, a.shpja, c.dist_frst_day, md.loadd),
             item_cust_qty AS
             (SELECT DISTINCT iclq.catlg_num, iclq.item_fcast, iclq.ord_qty,
                     FIRST_VALUE(iclq.llr_day) OVER(PARTITION BY iclq.catlg_num, iclq.cust_id ORDER BY iclq.llr_dt) AS llr_day
                FROM item_cust_load_qty iclq
               WHERE iclq.ship_dt <= iclq.eta_ts),
             unassigned AS
             (SELECT   icq.catlg_num, icq.item_fcast, icq.llr_day, SUM(icq.ord_qty) AS ord_qty
                  FROM item_cust_qty icq
              GROUP BY icq.catlg_num, icq.item_fcast, icq.llr_day),
             itm_fcast AS
             (SELECT DISTINCT x.catlg_num, x.item_fcast, t.dy,
                              SUM(DECODE(t.dy, x.llr_day, x.ord_qty, 0)) OVER(PARTITION BY x.catlg_num, t.dy) AS dy_fcast
                         FROM (SELECT     LEVEL AS dow,
                                          DECODE(LEVEL,
                                                 1, 'SUN',
                                                 2, 'MON',
                                                 3, 'TUE',
                                                 4, 'WED',
                                                 5, 'THU',
                                                 6, 'FRI',
                                                 7, 'SAT'
                                                ) AS dy
                                     FROM DUAL
                               CONNECT BY LEVEL <= 7) t,
                              (SELECT a.catlg_num, a.item_fcast, a.llr_day, a.ord_qty
                                 FROM assigned a
                               UNION ALL
                               SELECT u.catlg_num, u.item_fcast, u.llr_day, u.ord_qty
                                 FROM unassigned u) x)
        SELECT   *
            FROM (SELECT f.catlg_num, f.item_fcast, f.dy, f.dy_fcast
                    FROM itm_fcast f)
           PIVOT (SUM(dy_fcast)
             FOR dy IN('SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'))
        ORDER BY catlg_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dist_fcast_cur_fn;

  /*
  ||----------------------------------------------------------------------------
  || MSSNG_ITEM_FCAST_CUR_FN
  ||  Return Missing Item Forecast cursor
  ||   Include items not found in history or distribution forecast. For items
  ||   with item_fcast > 0 take the item_fcast divided by the number of days the
  ||   division bills and assign that number to each day that the division bills.
  ||   i.e.: Div bills 6 days a week (MON-SAT)
  ||         Item 123456 has item_fcast 60
  ||         QTY for Sunday bucket will be 0
  ||         QTY for all other daily buckets will be 10 (60/6)
  ||   Missing items with item_fcast of 0 will have 0 qty in all daily buckets.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/17/15 | rhalpai | Original for PIR14738
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | PIR15885
  ||----------------------------------------------------------------------------
  */
  FUNCTION mssng_item_fcast_cur_fn(
    i_div_part   IN  NUMBER,
    i_t_rpt_lns  IN  typ.tas_maxvc2
  )
    RETURN g_cvt_fcast IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_FORECAST_PK.MSSNG_ITEM_FCAST_CUR_FN';
    lar_parm             logs.tar_parm;
    l_cv                 g_cvt_fcast;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RptLnsTab', i_t_rpt_lns);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   dif.catlg_num, dif.item_fcast,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',SUN,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS sun_qty,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',MON,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS mon_qty,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',TUE,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS tue_qty,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',WED,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS wed_qty,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',THU,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS thu_qty,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',FRI,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS fri_qty,
                DECODE(dif.item_fcast,
                       0, 0,
                       DECODE(INSTR(dd.dy, ',SAT,'), 0, 0, ROUND(dif.item_fcast / dd.cnt))
                      ) AS sat_qty
           FROM div_item_fcast_op2f dif,
                (SELECT   ','
                          || LISTAGG(x.dy, ',') WITHIN GROUP(ORDER BY x.dow)
                          || ',' AS dy, x.cnt
                     FROM (SELECT t.dy, t.dow, COUNT(*) OVER() AS cnt
                             FROM (SELECT     LEVEL AS dow,
                                              DECODE(LEVEL,
                                                     1, 'SUN',
                                                     2, 'MON',
                                                     3, 'TUE',
                                                     4, 'WED',
                                                     5, 'THU',
                                                     6, 'FRI',
                                                     7, 'SAT'
                                                    ) AS dy
                                         FROM DUAL
                                   CONNECT BY LEVEL <= 7) t
                            WHERE EXISTS(SELECT 1
                                           FROM mclp120c mc
                                          WHERE mc.div_part = i_div_part
                                            AND mc.llrcdc = t.dy
                                            AND EXISTS(SELECT 1
                                                         FROM mclp040d md, sysp200c c
                                                        WHERE md.div_part = mc.div_part
                                                          AND md.loadd = mc.loadc
                                                          AND c.div_part = md.div_part
                                                          AND c.acnoc = md.custd
                                                          AND c.statc = '1'))) x
                 GROUP BY x.cnt) dd
          WHERE dif.div_part = i_div_part
            AND NOT EXISTS(SELECT 1
                             FROM TABLE(i_t_rpt_lns) t
                            WHERE SUBSTR(t.column_value, 7, 6) = dif.catlg_num)
       ORDER BY dif.catlg_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mssng_item_fcast_cur_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || FCAST_SP
  ||  Create Daily Item Forecast Extract File and Send to Mainframe
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/25/15 | rhalpai | Original for PIR14738
  || 12/17/15 | rhalpai | Add logic to include items on the forecast table that
  ||                    | were not found in history or distribution forecast.
  ||                    | Use FCT as the record type for missing items. PIR14738
  || 07/05/16 | rhalpai | Change logic to use UTIL_PK for input parameters.
  ||                    | PIR15885
  ||----------------------------------------------------------------------------
  */
  PROCEDURE fcast_sp(
    i_div     IN  VARCHAR2,
    i_run_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_FORECAST_PK.FCAST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_file_nm            VARCHAR2(50);
    l_rmt_file           VARCHAR2(30);
    l_t_rpt_lns          typ.tas_maxvc2;

    PROCEDURE add_rpt_lns_sp(
      i_typ       IN  VARCHAR2,
      i_cv_fcast  IN  g_cvt_fcast
    ) IS
      l_r_fcast  g_rt_fcast;
    BEGIN
      LOOP
        FETCH i_cv_fcast
         INTO l_r_fcast;

        EXIT WHEN i_cv_fcast%NOTFOUND;
        util.append(l_t_rpt_lns,
                    RPAD(i_typ, 6)
                    || LPAD(l_r_fcast.catlg_num, 6, '0')
                    || TO_CHAR(l_r_fcast.item_fcast, 'FM099999')
                    || RPAD(' ', 10)
                    || LPAD(l_r_fcast.sun_qty, 6, '0')
                    || LPAD(l_r_fcast.mon_qty, 6, '0')
                    || LPAD(l_r_fcast.tue_qty, 6, '0')
                    || LPAD(l_r_fcast.wed_qty, 6, '0')
                    || LPAD(l_r_fcast.thu_qty, 6, '0')
                    || LPAD(l_r_fcast.fri_qty, 6, '0')
                    || LPAD(l_r_fcast.sat_qty, 6, '0')
                   );
      END LOOP;
    END add_rpt_lns_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RunTs', i_run_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_file_nm := i_div || '_ITMFCST_' || TO_CHAR(i_run_ts, 'YYYYMMDDHH24MISS') || '.txt';
    l_rmt_file := 'ITMFCST';
    logs.dbg('Add Hist Forecast');
    add_rpt_lns_sp('HST', hist_fcast_cur_fn(l_div_part, i_run_ts));
    logs.dbg('Add Dist Forecast');
    add_rpt_lns_sp('DIS', dist_fcast_cur_fn(l_div_part, i_run_ts));
    logs.dbg('Add Missing Item Forecast');
    add_rpt_lns_sp('FCT', mssng_item_fcast_cur_fn(l_div_part, l_t_rpt_lns));
    logs.dbg('Write');
    write_sp(l_t_rpt_lns, l_file_nm);
    logs.dbg('Ftp');
    op_ftp_sp(i_div, l_file_nm, l_rmt_file);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END fcast_sp;
END op_forecast_pk;
/

