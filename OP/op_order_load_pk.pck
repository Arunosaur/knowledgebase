CREATE OR REPLACE PACKAGE op_order_load_pk IS
  /*
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/22/02 | rhalpai | Original
  || 10/01/02 | rhalpai | Created common record type used in the "next load" SP's
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_load_info IS RECORD(
    llr_ts     DATE,
    load_num   VARCHAR2(4),
    depart_ts  DATE,
    stop_num   NUMBER,
    eta_ts     DATE
  );

  TYPE g_rt_order_load IS RECORD(
    order_num          NUMBER,
    order_type         ordp100a.dsorda%TYPE,
    div_part           NUMBER,
    cust_num           ordp100a.custa%TYPE,
    cust_round_group   sysp200c.rndgpc%TYPE,
    load_type          ordp100a.ldtypa%TYPE,
    load_num           mclp040d.loadd%TYPE,
    stop_num           NUMBER,
    order_cutoff_date  NUMBER,
    order_cutoff_time  NUMBER,
    load_pricing_date  NUMBER,
    load_pricing_time  NUMBER,
    llr_cutoff_date    NUMBER,
    llr_cutoff_time    NUMBER,
    departure_date     NUMBER,
    departure_time     NUMBER,
    eta_date           NUMBER,
    eta_time           NUMBER
  );

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION rec_to_obj_fn(
    i_r_ord_load  IN  g_rt_order_load
  ) RETURN order_load_typ;

  FUNCTION obj_to_rec_fn(
    i_o_ord_load  IN  order_load_typ
  ) RETURN g_rt_order_load;

  FUNCTION load_depart_sid_fn(
    i_div_part  IN  NUMBER,
    i_llr_ts    IN  DATE,
    i_load_num  IN  VARCHAR2
  )
   RETURN NUMBER;

  FUNCTION stop_num_fn(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_req_stop_num     IN  NUMBER DEFAULT NULL
  )
   RETURN NUMBER;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE get_llr_depart_sp(
    i_div_part   IN      NUMBER,
    i_llr_dt     IN      DATE,
    i_load_num   IN      VARCHAR2,
    o_llr_ts     OUT     DATE,
    o_depart_ts  OUT     DATE
  );

  PROCEDURE get_stop_eta_sp(
    i_div_part         IN      NUMBER,
    i_load_depart_sid  IN      NUMBER,
    i_cust_id          IN      VARCHAR2,
    o_stop_num         OUT     NUMBER,
    o_eta_ts           OUT     DATE,
    i_req_eta_ts       IN      DATE DEFAULT NULL,
    i_req_stop_num     IN      NUMBER DEFAULT NULL
  );

  PROCEDURE merge_stop_eta_sp(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_eta_ts           IN  DATE DEFAULT NULL,
    i_stop_num         IN  NUMBER DEFAULT NULL
  );

  PROCEDURE get_ord_load_info_sp(
    i_div_part         IN      NUMBER,
    i_cust_id          IN      VARCHAR2,
    i_load_depart_sid  IN      NUMBER,
    o_r_load_info      OUT     g_rt_load_info
  );

  PROCEDURE log_ord_move_sp(
    i_div_part         IN  NUMBER,
    i_ord_num          IN  NUMBER,
    i_r_bef_load_info  IN  g_rt_load_info,
    i_rsn_cd           IN  VARCHAR2,
    i_user_id          IN  VARCHAR2
  );

  PROCEDURE move_ords_sp(
    i_div_part         IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_load_depart_sid  IN  NUMBER,
    i_stop_num         IN  NUMBER,
    i_eta_ts           IN  DATE,
    i_old_llr_ts       IN  DATE,
    i_old_load_num     IN  VARCHAR2,
    i_old_depart_ts    IN  DATE,
    i_old_stop_num     IN  NUMBER,
    i_old_eta_ts       IN  DATE,
    i_rsn_cd           IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_t_ord_nums       IN  type_ntab
  );

  PROCEDURE move_ords_sp(
    i_div_part    IN  NUMBER,
    i_cust_id     IN  VARCHAR2,
    i_llr_ts      IN  DATE,
    i_load_num    IN  VARCHAR2,
    i_stop_num    IN  NUMBER,
    i_eta_ts      IN  DATE,
    i_rsn_cd      IN  VARCHAR2,
    i_user_id     IN  VARCHAR2,
    i_t_ord_nums  IN  type_ntab
  );

  PROCEDURE nxt_load_for_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_prcs_ts   IN      DATE,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  );

  PROCEDURE nxt_load_for_dist_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  );

  /*
  ||----------------------------------------------------------------------------
  || ATTACH_DIST_ORDS_SP
  ||   Used to assign the load info for a regular order to that of unassigned
  ||   available distribution orders. This is known as attaching distributions.
  ||   Commit and error handling are up to the calling program.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE attach_dist_ords_sp(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2
  );

  PROCEDURE syncload_sp(
    i_div_part    IN  NUMBER,
    i_rsn_cd      IN  VARCHAR2,
    i_t_ord_nums  IN  type_ntab,
    i_user_id     IN  VARCHAR2 DEFAULT USER
  );
END op_order_load_pk;
/

CREATE OR REPLACE PACKAGE BODY op_order_load_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ASSGN_DFLT_ORDS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/13 | rhalpai | Move logic from OP_MESSAGES_PK. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE assgn_dflt_ords_sp(
    i_div_part  IN  NUMBER,
    i_cust_id   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ORDER_LOAD_PK.ASSGN_DFLT_ORDS_SP';
    lar_parm             logs.tar_parm;
    l_llr_ts             DATE;
    l_load_num           mclp120c.loadc%TYPE;
    l_r_bef_load_info    g_rt_load_info;
    l_load_depart_sid    NUMBER;

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_cust_id   VARCHAR2
    ) IS
      SELECT a.ordnoa AS ord_num, a.custa AS cust_id, a.load_depart_sid
        FROM load_depart_op1f ld, ordp100a a
       WHERE ld.div_part = b_div_part
         AND ld.load_num = 'DFLT'
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.custa = b_cust_id
         AND a.stata = 'O';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Attach Default Reg Orders');
    <<dflt_reg_ords_loop>>
    FOR l_r_ord IN l_cur_ords(i_div_part, i_cust_id) LOOP
      logs.dbg('Find Load for Default Reg Order');
      op_order_load_pk.nxt_load_for_ord_sp(i_div_part, l_r_ord.ord_num, NULL, l_llr_ts, l_load_num);

      -- Found Load
      IF l_load_num IS NOT NULL THEN
        logs.dbg('Get Before Ord Load Info');
        op_order_load_pk.get_ord_load_info_sp(i_div_part, l_r_ord.cust_id, l_r_ord.load_depart_sid, l_r_bef_load_info);
        logs.dbg('Get LoadDepartSid');
        l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, l_llr_ts, l_load_num);
        logs.dbg('Upd StopEta');
        op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, l_r_ord.cust_id);
        logs.dbg('Upd Reg Ord Hdr');

        UPDATE ordp100a a
           SET a.load_depart_sid = l_load_depart_sid
         WHERE a.div_part = i_div_part
           AND a.ordnoa = l_r_ord.ord_num
           AND a.load_depart_sid <> l_load_depart_sid;

        logs.dbg('Log Reg Ord Move');
        op_order_load_pk.log_ord_move_sp(i_div_part, l_r_ord.ord_num, l_r_bef_load_info, i_rsn_cd, i_user_id);
        logs.dbg('Attach Dist to Default Order');
        op_order_load_pk.attach_dist_ords_sp(i_div_part, l_load_depart_sid, l_r_ord.cust_id, 'ASGNDFLTORDS');
      END IF;   -- v_load_num IS NOT NULL
    END LOOP dflt_reg_ords_loop;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END assgn_dflt_ords_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || REC_TO_OBJ_FN
  ||   Converts order load record to order load object.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION rec_to_obj_fn(
    i_r_ord_load  IN  g_rt_order_load
  )
    RETURN order_load_typ IS
  BEGIN
    RETURN(order_load_typ(i_r_ord_load.order_num,
                          i_r_ord_load.order_type,
                          div_pk.div_id_fn(i_r_ord_load.div_part),
                          i_r_ord_load.cust_num,
                          i_r_ord_load.cust_round_group,
                          i_r_ord_load.load_type,
                          i_r_ord_load.load_num,
                          i_r_ord_load.stop_num,
                          i_r_ord_load.order_cutoff_date,
                          i_r_ord_load.order_cutoff_time,
                          i_r_ord_load.load_pricing_date,
                          i_r_ord_load.load_pricing_time,
                          i_r_ord_load.llr_cutoff_date,
                          i_r_ord_load.llr_cutoff_time,
                          i_r_ord_load.departure_date,
                          i_r_ord_load.departure_time,
                          i_r_ord_load.eta_date,
                          i_r_ord_load.eta_time
                         )
          );
  END rec_to_obj_fn;

  /*
  ||----------------------------------------------------------------------------
  || OBJ_TO_REC_FN
  ||   Converts order load object to order load record.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION obj_to_rec_fn(
    i_o_ord_load  IN  order_load_typ
  )
    RETURN g_rt_order_load IS
    l_r_ord_load  g_rt_order_load;
  BEGIN
    IF i_o_ord_load IS NOT NULL THEN
      l_r_ord_load.order_num := i_o_ord_load.order_num;
      l_r_ord_load.order_type := i_o_ord_load.order_type;
      l_r_ord_load.div_part := div_pk.div_part_fn(i_o_ord_load.div_id);
      l_r_ord_load.cust_num := i_o_ord_load.cust_num;
      l_r_ord_load.order_type := i_o_ord_load.order_type;
      l_r_ord_load.cust_round_group := i_o_ord_load.cust_round_group;
      l_r_ord_load.load_type := i_o_ord_load.load_type;
      l_r_ord_load.load_num := i_o_ord_load.load_num;
      l_r_ord_load.stop_num := i_o_ord_load.stop_num;
      l_r_ord_load.order_cutoff_date := i_o_ord_load.order_cutoff_date;
      l_r_ord_load.order_cutoff_time := i_o_ord_load.order_cutoff_time;
      l_r_ord_load.load_pricing_date := i_o_ord_load.load_pricing_date;
      l_r_ord_load.load_pricing_time := i_o_ord_load.load_pricing_time;
      l_r_ord_load.llr_cutoff_date := i_o_ord_load.llr_cutoff_date;
      l_r_ord_load.llr_cutoff_time := i_o_ord_load.llr_cutoff_time;
      l_r_ord_load.departure_date := i_o_ord_load.departure_date;
      l_r_ord_load.departure_time := i_o_ord_load.departure_time;
      l_r_ord_load.eta_date := i_o_ord_load.eta_date;
      l_r_ord_load.eta_time := i_o_ord_load.eta_time;
    END IF;   -- i_o_ord_load IS NOT NULL

    RETURN(l_r_ord_load);
  END obj_to_rec_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOAD_DEPART_SID_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/09/11 | dlbeal | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION load_depart_sid_fn(
    i_div_part  IN  NUMBER,
    i_llr_ts    IN  DATE,
    i_load_num  IN  VARCHAR2
  )
    RETURN NUMBER IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.LOAD_DEPART_SID_FN';
    lar_parm              logs.tar_parm;
    l_load_depart_sid     NUMBER;
    l_c_dflt_ts  CONSTANT DATE          := DATE '1900-01-01';
    l_cv                  SYS_REFCURSOR;
    l_llr_ts              DATE;
    l_depart_ts           DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRTS', i_llr_ts);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);

    IF i_llr_ts = l_c_dflt_ts THEN
      l_llr_ts := l_c_dflt_ts;
      l_depart_ts := l_c_dflt_ts;
    ELSE
      logs.dbg('Get LLR/Depart');
      get_llr_depart_sp(i_div_part, i_llr_ts, i_load_num, l_llr_ts, l_depart_ts);
      logs.dbg('Upd Load/Depart Entry');

      UPDATE load_depart_op1f ld
         SET ld.llr_ts = l_llr_ts,
             ld.depart_ts = l_depart_ts
       WHERE ld.div_part = i_div_part
         AND ld.llr_ts IN(i_llr_ts, l_llr_ts)
         AND ld.load_num = i_load_num
         AND NOT EXISTS(SELECT 1
                          FROM ordp100a a
                         WHERE a.div_part = i_div_part
                           AND a.load_depart_sid = ld.load_depart_sid
                           AND a.stata IN('I', 'P', 'R', 'A'))
         AND (   ld.llr_ts <> l_llr_ts
              OR ld.depart_ts <> l_depart_ts);
    END IF;   -- i_llr_ts = l_c_dflt_ts

    logs.dbg('Open Load/Depart Cursor');

    OPEN l_cv
     FOR
       SELECT ld.load_depart_sid
         FROM load_depart_op1f ld
        WHERE ld.div_part = i_div_part
          AND ld.llr_ts IN(i_llr_ts, l_llr_ts)
          AND ld.load_num = i_load_num;

    logs.dbg('Fetch Load/Depart Cursor');

    FETCH l_cv
     INTO l_load_depart_sid;

    IF l_load_depart_sid IS NULL THEN
      logs.dbg('Add Load/Depart Entry');

      INSERT INTO load_depart_op1f
                  (load_depart_sid, div_part, llr_ts, load_num, depart_ts
                  )
           VALUES (op1f_load_depart_id_seq.NEXTVAL, i_div_part, l_llr_ts, i_load_num, l_depart_ts
                  )
        RETURNING load_depart_sid
             INTO l_load_depart_sid;
    END IF;   -- l_load_depart_sid IS NULL

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_load_depart_sid);
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
      COMMIT;

      SELECT ld.load_depart_sid
        INTO l_load_depart_sid
        FROM load_depart_op1f ld
       WHERE ld.div_part = i_div_part
         AND ld.llr_ts = l_llr_ts
         AND ld.load_num = i_load_num;

      RETURN(l_load_depart_sid);
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END load_depart_sid_fn;

  /*
  ||----------------------------------------------------------------------------
  || STOP_NUM_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/18/12 | dlbeal  | Original
  || 10/24/13 | rhalpai | Add logic to include match on Load for CustRteOvrrd.
  ||                    | IM-123463
  || 10/28/13 | rhalpai | Change logic to include StopNum from
  ||                    | CUST_RTE_OVRRD_RT3C when StopOvrrdSw is ON. IM-123463
  || 12/01/15 | rhalpai | Add div_part parm. PIR15202
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION stop_num_fn(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_req_stop_num     IN  NUMBER DEFAULT NULL
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.STOP_NUM_FN';
    lar_parm             logs.tar_parm;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
    l_stop_num           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'ReqStopNum', i_req_stop_num);
    logs.dbg('ENTRY', lar_parm);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT COALESCE((SELECT 0
                          FROM TABLE(CAST(l_t_xloads AS type_stab)) t
                         WHERE t.column_value = ld.load_num
                           AND t.column_value <> 'ROUT'),
                       (SELECT cro.stop_num
                          FROM cust_rte_ovrrd_rt3c cro
                         WHERE cro.div_part = ld.div_part
                           AND cro.llr_dt = ld.llr_dt
                           AND cro.load_num = ld.load_num
                           AND cro.cust_id = i_cust_id
                           AND cro.eta_ovrrd_sw = 'Y'),
                       (SELECT md.stopd
                          FROM mclp040d md
                         WHERE md.div_part = ld.div_part
                           AND md.loadd = ld.load_num
                           AND md.custd = i_cust_id),
                       i_req_stop_num,
                       (SELECT se.stop_num
                          FROM stop_eta_op1g se
                         WHERE se.div_part = i_div_part
                           AND se.load_depart_sid = i_load_depart_sid
                           AND se.cust_id = i_cust_id),
                       (SELECT MIN(y.stop_num)
                          FROM (SELECT     LEVEL AS stop_num
                                      FROM (SELECT MAX(se.stop_num) + 1 AS nxt_stop
                                              FROM stop_eta_op1g se
                                             WHERE se.div_part = i_div_part
                                               AND se.load_depart_sid = i_load_depart_sid) x
                                CONNECT BY LEVEL <= x.nxt_stop) y
                         WHERE NOT EXISTS(SELECT 1
                                            FROM stop_eta_op1g se2
                                           WHERE se2.div_part = i_div_part
                                             AND se2.load_depart_sid = i_load_depart_sid
                                             AND se2.stop_num = y.stop_num))
                      )
         FROM load_depart_op1f ld
        WHERE ld.div_part = i_div_part
          AND ld.load_depart_sid = i_load_depart_sid;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_stop_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_stop_num);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END stop_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LLR_DEPART_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/30/13 | rhalpai | Original
  || 05/13/13 | rhalpai | Change logic to add 1 week to LLRTs if LLRDt matches
  ||                    | DepartDt but LLRTime is before DepartTime. PIR11038
  || 10/28/13 | rhalpai | Change logic to include DepartTs from
  ||                    | CUST_RTE_OVRRD_RT3C when DepartOvrrdSw is ON.
  ||                    | IM-123463
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_EXISTS_FOR_PRFX_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_llr_depart_sp(
    i_div_part   IN      NUMBER,
    i_llr_dt     IN      DATE,
    i_load_num   IN      VARCHAR2,
    o_llr_ts     OUT     DATE,
    o_depart_ts  OUT     DATE
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.GET_LLR_DEPART_SP';
    lar_parm              logs.tar_parm;
    l_cv                  SYS_REFCURSOR;
    l_c_dflt_ts  CONSTANT DATE          := DATE '1900-01-01';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Check XLOAD Parm');

    IF op_parms_pk.val_exists_for_prfx_fn(i_div_part, op_const_pk.prm_xload, i_load_num) = 'Y' THEN
      o_llr_ts := l_c_dflt_ts;
    END IF;   -- op_parms_pk.val_exists_for_prfx_fn(i_div_part, op_const_pk.prm_xload, i_load_num) = 'Y'

    IF o_llr_ts = l_c_dflt_ts THEN
      o_depart_ts := l_c_dflt_ts;
    ELSE
      logs.dbg('Get LLRTs and DepartTs');

      OPEN l_cv
       FOR
         SELECT x.llr_ts,
                COALESCE((SELECT MAX(cro.depart_ts)
                            FROM cust_rte_ovrrd_rt3c cro
                           WHERE cro.div_part = i_div_part
                             AND cro.load_num = i_load_num
                             AND cro.llr_dt = TRUNC(i_llr_dt)
                             AND cro.depart_ovrrd_sw = 'Y'),
                         TO_DATE(TO_CHAR(NEXT_DAY(x.llr_ts - 1, x.depdac), 'YYYYMMDD') || LPAD(x.deptmc, 4, '0'),
                                 'YYYYMMDDHH24MI'
                                )
                         + NVL(x.depwkc, 0) * 7
                         +(CASE
                             WHEN(    x.depwkc = 0
                                  AND x.depdac = x.llrcdc
                                  AND x.deptmc < x.llrctc) THEN 7
                             ELSE 0
                           END),
                         l_c_dflt_ts
                        ) AS depart_ts
           FROM (SELECT l.depdac, l.deptmc, l.depwkc, l.llrcdc, l.llrctc,
                        NVL(TO_DATE(TO_CHAR(NEXT_DAY(i_llr_dt - 1, l.llrcdc), 'YYYYMMDD') || LPAD(l.llrctc, 4, '0'),
                                    'YYYYMMDDHH24MI'
                                   ),
                            l_c_dflt_ts
                           ) AS llr_ts
                   FROM div_mstr_di1d d, mclp120c l
                  WHERE d.div_part = i_div_part
                    AND l.div_part(+) = d.div_part
                    AND l.loadc(+) = i_load_num) x;

      FETCH l_cv
       INTO o_llr_ts, o_depart_ts;
    END IF;   -- o_llr_ts = l_c_dflt_ts

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_llr_depart_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_STOP_ETA_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/30/13 | rhalpai | Original
  || 10/08/13 | rhalpai | Change logic to retain Stop/ETA for LLR/Load/Cust
  ||                    | once Billed orders exist. IM-121647
  || 10/24/13 | rhalpai | Add logic to include match on Load for CustRteOvrrd.
  ||                    | IM-123463
  || 10/28/13 | rhalpai | Change logic to include EtaTs from
  ||                    | CUST_RTE_OVRRD_RT3C when EtaOvrrdSw is ON. IM-123463
  || 03/30/15 | rhalpai | Add logic to look-up the EtaTs for an existing
  ||                    | LoadDepartSid/CustId. IM-260723
  || 12/01/15 | rhalpai | Add div_part parm. PIR15202
  || 08/21/19 | rhalpai | Add logic to default ETA to Departure + 1 minute. PIR19563
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_stop_eta_sp(
    i_div_part         IN      NUMBER,
    i_load_depart_sid  IN      NUMBER,
    i_cust_id          IN      VARCHAR2,
    o_stop_num         OUT     NUMBER,
    o_eta_ts           OUT     DATE,
    i_req_eta_ts       IN      DATE DEFAULT NULL,
    i_req_stop_num     IN      NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.GET_STOP_ETA_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'ReqEtaTs', i_req_eta_ts);
    logs.add_parm(lar_parm, 'ReqStopNum', i_req_stop_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Retain Stop/ETA When Billed');

    OPEN l_cv
     FOR
       SELECT se.stop_num, se.eta_ts
         FROM stop_eta_op1g se
        WHERE se.div_part = i_div_part
          AND se.load_depart_sid = i_load_depart_sid
          AND se.cust_id = i_cust_id
          AND EXISTS(SELECT 1
                       FROM ordp100a a
                      WHERE a.div_part = i_div_part
                        AND a.load_depart_sid = i_load_depart_sid
                        AND a.custa = i_cust_id
                        AND a.stata IN('P', 'R', 'A'));

    FETCH l_cv
     INTO o_stop_num, o_eta_ts;

    CLOSE l_cv;

    IF o_stop_num IS NULL THEN
      logs.dbg('Get StopNum');
      o_stop_num := op_order_load_pk.stop_num_fn(i_div_part, i_load_depart_sid, i_cust_id, i_req_stop_num);
      logs.dbg('Get EtaTs');

      SELECT COALESCE((SELECT DATE '1900-01-01'
                         FROM load_depart_op1f ld
                        WHERE ld.div_part = i_div_part
                          AND ld.load_depart_sid = i_load_depart_sid
                          AND ld.llr_ts = DATE '1900-01-01'),
                      (SELECT cro.eta_ts
                         FROM load_depart_op1f ld, cust_rte_ovrrd_rt3c cro
                        WHERE ld.div_part = i_div_part
                          AND ld.load_depart_sid = i_load_depart_sid
                          AND cro.div_part = ld.div_part
                          AND cro.llr_dt = ld.llr_dt
                          AND cro.load_num = ld.load_num
                          AND cro.cust_id = i_cust_id
                          AND cro.eta_ovrrd_sw = 'Y'),
                      (SELECT x.eta_ts +(CASE
                                           WHEN x.eta_ts < x.depart_ts THEN 7
                                           ELSE 0
                                         END)
                         FROM (SELECT NEXT_DAY(TO_DATE(TO_CHAR(ld.depart_ts - 1, 'YYYYMMDD') || LPAD(md.etad, 4, '0'),
                                                       'YYYYMMDDHH24MI'
                                                      ),
                                               md.dayrcd
                                              )
                                      + NVL(md.wkoffd, 0) * 7 AS eta_ts,
                                      ld.depart_ts
                                 FROM load_depart_op1f ld, mclp040d md
                                WHERE ld.div_part = i_div_part
                                  AND ld.load_depart_sid = i_load_depart_sid
                                  AND md.div_part = ld.div_part
                                  AND md.loadd = ld.load_num
                                  AND md.custd = i_cust_id) x),
                      i_req_eta_ts,
                      (SELECT se.eta_ts
                         FROM stop_eta_op1g se
                        WHERE se.div_part = i_div_part
                          AND se.load_depart_sid = i_load_depart_sid
                          AND se.cust_id = i_cust_id),
                      (SELECT ld.depart_ts + INTERVAL '1' minute
                         FROM load_depart_op1f ld
                        WHERE ld.div_part = i_div_part
                          AND ld.load_depart_sid = i_load_depart_sid)
                     ) AS eta_ts
        INTO o_eta_ts
        FROM DUAL;
    END IF;   -- o_stop_num IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_stop_eta_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_STOP_ETA_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/19/12 | dlbeal  | Original
  || 12/10/12 | rhalpai | Change logic to bypass NULL CustId.
  || 12/01/15 | rhalpai | Add div_part parm. PIR15202
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_stop_eta_sp(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_eta_ts           IN  DATE DEFAULT NULL,
    i_stop_num         IN  NUMBER DEFAULT NULL
  ) IS
    l_stop_num  NUMBER;
    l_eta_ts    DATE;
  BEGIN
    IF i_cust_id IS NOT NULL THEN
      get_stop_eta_sp(i_div_part, i_load_depart_sid, i_cust_id, l_stop_num, l_eta_ts, i_eta_ts, i_stop_num);

      INSERT INTO stop_eta_op1g
                  (div_part, load_depart_sid, cust_id, stop_num, eta_ts
                  )
           VALUES (i_div_part, i_load_depart_sid, i_cust_id, l_stop_num, l_eta_ts
                  );
    END IF;   -- i_cust_id IS NOT NULL
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
      UPDATE stop_eta_op1g se
         SET se.stop_num = l_stop_num,
             se.eta_ts = l_eta_ts
       WHERE se.div_part = i_div_part
         AND se.load_depart_sid = i_load_depart_sid
         AND se.cust_id = i_cust_id
         AND (   se.stop_num <> l_stop_num
              OR se.eta_ts <> l_eta_ts);
  END merge_stop_eta_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_ORD_LOAD_INFO_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/30/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_ord_load_info_sp(
    i_div_part         IN      NUMBER,
    i_cust_id          IN      VARCHAR2,
    i_load_depart_sid  IN      NUMBER,
    o_r_load_info      OUT     g_rt_load_info
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.GET_ORD_LOAD_INFO_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.dbg('ENTRY', lar_parm);

    SELECT ld.llr_ts,
           ld.load_num,
           ld.depart_ts,
           se.stop_num,
           se.eta_ts
      INTO o_r_load_info
      FROM load_depart_op1f ld, stop_eta_op1g se
     WHERE ld.div_part = i_div_part
       AND ld.load_depart_sid = i_load_depart_sid
       AND se.div_part = ld.div_part
       AND se.load_depart_sid = ld.load_depart_sid
       AND se.cust_id = i_cust_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_ord_load_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOG_ORD_MOVE_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/30/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_ord_move_sp(
    i_div_part         IN  NUMBER,
    i_ord_num          IN  NUMBER,
    i_r_bef_load_info  IN  g_rt_load_info,
    i_rsn_cd           IN  VARCHAR2,
    i_user_id          IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ORDER_LOAD_PK.LOG_ORD_MOVE_SP';
    lar_parm             logs.tar_parm;
    l_cust_id            sysp200c.acnoc%TYPE;
    l_load_depart_sid    NUMBER;
    l_r_aft_load_info    g_rt_load_info;
    l_bef_descr          mclp300d.descd%TYPE;
    l_aft_descr          mclp300d.itemd%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'BefLLRTs', i_r_bef_load_info.llr_ts);
    logs.add_parm(lar_parm, 'BefLoadNum', i_r_bef_load_info.load_num);
    logs.add_parm(lar_parm, 'BefDepTs', i_r_bef_load_info.depart_ts);
    logs.add_parm(lar_parm, 'StopNum', i_r_bef_load_info.stop_num);
    logs.add_parm(lar_parm, 'BefEtaTs', i_r_bef_load_info.eta_ts);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Order Info');

    SELECT a.custa, a.load_depart_sid
      INTO l_cust_id, l_load_depart_sid
      FROM ordp100a a
     WHERE a.div_part = i_div_part
       AND a.ordnoa = i_ord_num;

    logs.dbg('Get Order Load Info');
    op_order_load_pk.get_ord_load_info_sp(i_div_part, l_cust_id, l_load_depart_sid, l_r_aft_load_info);
    logs.dbg('Check for changes');

    IF (   l_r_aft_load_info.llr_ts <> i_r_bef_load_info.llr_ts
        OR l_r_aft_load_info.load_num <> i_r_bef_load_info.load_num
        OR l_r_aft_load_info.depart_ts <> i_r_bef_load_info.depart_ts
        OR l_r_aft_load_info.stop_num <> i_r_bef_load_info.stop_num
        OR l_r_aft_load_info.eta_ts <> i_r_bef_load_info.eta_ts
       ) THEN
      l_bef_descr := 'B: L/S/R/E:'
                     || RPAD(i_r_bef_load_info.load_num, 4)
                     || '/'
                     || LPAD(i_r_bef_load_info.stop_num, 2, '0')
                     || '/'
                     || TO_CHAR(i_r_bef_load_info.llr_ts, 'YYMMDD')
                     || '/'
                     || TO_CHAR(i_r_bef_load_info.eta_ts, 'YYMMDDHH24MI');
      logs.dbg('Capture AFTER Data');
      l_aft_descr := RPAD(l_r_aft_load_info.load_num, 4)
                     || '/'
                     || LPAD(l_r_aft_load_info.stop_num, 2, '0')
                     || '/'
                     || TO_CHAR(l_r_aft_load_info.llr_ts, 'YYMMDD')
                     || '/'
                     || TO_CHAR(l_r_aft_load_info.eta_ts, 'YYMMDDHH24MI');
      logs.dbg('Log Changes');

      DECLARE
        r_mclp300d  mclp300d%ROWTYPE;
      BEGIN
        r_mclp300d.div_part := i_div_part;
        r_mclp300d.ordnod := i_ord_num;
        r_mclp300d.ordlnd := 0;
        r_mclp300d.reasnd := i_rsn_cd;
        r_mclp300d.descd := l_bef_descr;
        r_mclp300d.exlvld := 4;
        r_mclp300d.itemd := l_aft_descr;
        r_mclp300d.resexd := '0';
        r_mclp300d.resusd := i_user_id;
        op_mclp300d_pk.ins_sp(r_mclp300d);
      END;
    END IF;   -- check for changes

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END log_ord_move_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_ORDS_SP
  ||  Move orders and log order moves.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_ords_sp(
    i_div_part         IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_load_depart_sid  IN  NUMBER,
    i_stop_num         IN  NUMBER,
    i_eta_ts           IN  DATE,
    i_old_llr_ts       IN  DATE,
    i_old_load_num     IN  VARCHAR2,
    i_old_depart_ts    IN  DATE,
    i_old_stop_num     IN  NUMBER,
    i_old_eta_ts       IN  DATE,
    i_rsn_cd           IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_t_ord_nums       IN  type_ntab
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'OP_ORDER_LOAD_PK.MOVE_ORDS_SP';
    lar_parm             logs.tar_parm;
    l_llr_ts             DATE;
    l_load_num           mclp120c.loadc%TYPE;
    l_load_depart_sid    NUMBER;
    l_r_bef_load_info    g_rt_load_info;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'ETATs', i_eta_ts);
    logs.add_parm(lar_parm, 'OldLLRTs', i_old_llr_ts);
    logs.add_parm(lar_parm, 'OldLoadNum', i_old_load_num);
    logs.add_parm(lar_parm, 'OldDepartTs', i_old_depart_ts);
    logs.add_parm(lar_parm, 'OldStopNum', i_old_stop_num);
    logs.add_parm(lar_parm, 'OldETATs', i_old_eta_ts);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'OrdNumsTab', i_t_ord_nums);
    logs.dbg('ENTRY', lar_parm);

    IF (    i_t_ord_nums IS NOT NULL
        AND i_t_ord_nums.COUNT > 0) THEN
      logs.dbg('Get LLRTs, LoadNum');

      SELECT ld.llr_ts, ld.load_num
        INTO l_llr_ts, l_load_num
        FROM load_depart_op1f ld
       WHERE ld.div_part = i_div_part
         AND ld.load_depart_sid = i_load_depart_sid;

      logs.dbg('Call LoadDepartSidFn to Allow Upd of LLRTs,DepartTs');
      l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, l_llr_ts, l_load_num);
      logs.dbg('Upd StopEta');
      op_order_load_pk.merge_stop_eta_sp(i_div_part, i_load_depart_sid, i_cust_id, i_eta_ts, i_stop_num);
      logs.dbg('Upd Ord Hdr');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        UPDATE ordp100a a
           SET a.load_depart_sid = i_load_depart_sid
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_t_ord_nums(i)
           AND a.load_depart_sid <> i_load_depart_sid;
      logs.dbg('Set Before Ord Load Info');
      l_r_bef_load_info.llr_ts := i_old_llr_ts;
      l_r_bef_load_info.load_num := i_old_load_num;
      l_r_bef_load_info.depart_ts := i_old_depart_ts;
      l_r_bef_load_info.stop_num := i_old_stop_num;
      l_r_bef_load_info.eta_ts := i_old_eta_ts;
      logs.dbg('Log Ord Move');
      FOR i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST LOOP
        op_order_load_pk.log_ord_move_sp(i_div_part, i_t_ord_nums(i), l_r_bef_load_info, i_rsn_cd, i_user_id);
      END LOOP;
    END IF;   --  i_t_ord_nums IS NOT NULL AND i_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_ords_sp;

  PROCEDURE move_ords_sp(
    i_div_part    IN  NUMBER,
    i_cust_id     IN  VARCHAR2,
    i_llr_ts      IN  DATE,
    i_load_num    IN  VARCHAR2,
    i_stop_num    IN  NUMBER,
    i_eta_ts      IN  DATE,
    i_rsn_cd      IN  VARCHAR2,
    i_user_id     IN  VARCHAR2,
    i_t_ord_nums  IN  type_ntab
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_ORDER_LOAD_PK.MOVE_ORDS_SP';
    lar_parm             logs.tar_parm;

    TYPE l_rt_load_ords IS RECORD(
      llr_ts      DATE,
      load_num    VARCHAR2(4),
      depart_ts   DATE,
      stop_num    NUMBER,
      eta_ts      DATE,
      t_ord_nums  type_ntab
    );

    TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

    l_t_load_ords        l_tt_load_ords;
    l_load_depart_sid    NUMBER;
    l_t_ord_nums         type_ntab;
    l_r_bef_load_info    g_rt_load_info;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'LLRTs', i_llr_ts);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'StopNum', i_stop_num);
    logs.add_parm(lar_parm, 'ETATs', i_eta_ts);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'OrdNumsTab', i_t_ord_nums);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Load and Order Info');

    SELECT   ld.llr_ts,
             ld.load_num,
             ld.depart_ts,
             se.stop_num,
             se.eta_ts,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = i_div_part
                              AND a.custa = i_cust_id
                              AND a.load_depart_sid = ld.load_depart_sid
                              AND a.ordnoa IN(SELECT t.column_value
                                                FROM TABLE(CAST(i_t_ord_nums AS type_ntab)) t)
                              AND a.stata IN('O', 'S')
                          ) AS type_ntab
                 ) AS ord_nums
    BULK COLLECT INTO l_t_load_ords
        FROM stop_eta_op1g se, load_depart_op1f ld
       WHERE se.div_part = i_div_part
         AND se.cust_id = i_cust_id
         AND se.load_depart_sid IN(SELECT   a.load_depart_sid
                                       FROM ordp100a a
                                      WHERE a.div_part = i_div_part
                                        AND a.custa = i_cust_id
                                        AND a.ordnoa IN(SELECT t.column_value
                                                          FROM TABLE(CAST(i_t_ord_nums AS type_ntab)) t)
                                        AND a.stata IN('O', 'S')
                                   GROUP BY a.load_depart_sid)
         AND ld.div_part = se.div_part
         AND ld.load_depart_sid = se.load_depart_sid
    ORDER BY llr_ts, load_num;

    IF l_t_load_ords.COUNT > 0 THEN
      logs.dbg('Get New LoadDepartSid');
      l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, i_llr_ts, i_load_num);
      logs.dbg('Upd StopEta');
      op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, i_cust_id, i_eta_ts, i_stop_num);
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        l_t_ord_nums := l_t_load_ords(i).t_ord_nums;
        logs.dbg('Upd Ord Hdr');
        FORALL j IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
          UPDATE ordp100a a
             SET a.load_depart_sid = l_load_depart_sid
           WHERE a.div_part = i_div_part
             AND a.ordnoa = l_t_ord_nums(j)
             AND a.load_depart_sid <> l_load_depart_sid;
        logs.dbg('Set Before Ord Load Info');
        l_r_bef_load_info.llr_ts := l_t_load_ords(i).llr_ts;
        l_r_bef_load_info.load_num := l_t_load_ords(i).load_num;
        l_r_bef_load_info.depart_ts := l_t_load_ords(i).depart_ts;
        l_r_bef_load_info.stop_num := l_t_load_ords(i).stop_num;
        l_r_bef_load_info.eta_ts := l_t_load_ords(i).eta_ts;
        logs.dbg('Log Ord Move');
        FOR j IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
          op_order_load_pk.log_ord_move_sp(i_div_part, l_t_ord_nums(j), l_r_bef_load_info, i_rsn_cd, i_user_id);
        END LOOP;
      END LOOP;
    END IF;   -- l_t_load_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || NXT_LOAD_WITH_ETA_AFTR_SP
  ||  Return next LLR/Load for order with ETA after or equal to ship date (for distributions).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/28/19 | rhalpai | Original for PIR18852
  || 07/15/25 | rhalpai | Add logic to allow GMP order on GRO load while continuing to restrict GRO order on GMP load.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE nxt_load_with_eta_aftr_sp(
    i_div_part    IN      NUMBER,
    i_ord_num     IN      NUMBER,
    i_prcs_ts     IN      DATE,
    o_llr_ts      OUT     DATE,
    o_load_num    OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.NXT_LOAD_WITH_ETA_AFTR_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_dflt_prcs_ts        DATE;
    l_frst_dow            VARCHAR2(3);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_dflt_prcs_ts :=(CASE
                        WHEN i_prcs_ts IS NULL THEN l_c_sysdate
                        ELSE LEAST(i_prcs_ts, l_c_sysdate)
                      END);
    l_frst_dow := NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_frst_dow), 'SUN');
    logs.dbg('Get Load Info');

    WITH o AS
         (SELECT a.custa AS cust_id, DECODE(a.ldtypa, 'GMP', 'GMP', 'GRO') AS load_typ,
                 DATE '1900-02-28' + a.shpja AS shp_dt
            FROM ordp100a a
           WHERE a.div_part = i_div_part
             AND a.ordnoa = i_ord_num
             AND DATE '1900-02-28' + a.shpja >= l_dflt_prcs_ts),
         eta AS
         (SELECT o.shp_dt, o.cust_id, md.loadd AS load_num, md.wkoffd AS eta_wk, l.depdac AS depart_day,
                 l.deptmc AS depart_tm, l.depwkc AS depart_wk, l.llrcdc AS llr_day, l.llrctc AS llr_tm, l.llrwkc AS llr_wk,
                 dt_tm_fn(NEXT_DAY(o.shp_dt - 1, md.dayrcd), md.etad) AS eta_ts
            FROM o, mclp120c l, mclp040d md, sysp200c c
           WHERE l.div_part = i_div_part
             AND l.test_bil_load_sw = 'N'
             AND l.lbsgpc = DECODE(o.load_typ, 'GRO', 'N', l.lbsgpc)
             AND md.div_part = l.div_part
             AND md.loadd = l.loadc
             AND md.custd = o.cust_id
             AND md.prod_typ IN('BTH', o.load_typ)
             AND MOD((wk_beg_fn(NEXT_DAY(o.shp_dt - 1, md.dayrcd), l_frst_dow) - wk_beg_fn(NEXT_DAY(md.eff_dt - 1, l.llrcdc), l_frst_dow)) / 7,
                     md.recur_wk
                    ) = 0
             AND NEXT_DAY(o.shp_dt - 1, md.dayrcd) >= md.eff_dt
             AND c.div_part = md.div_part
             AND c.acnoc = md.custd
             AND c.statc IN('1', '3')),
         depart AS
         (SELECT eta.shp_dt, eta.cust_id, eta.load_num, eta.eta_ts,
                 dt_tm_fn(NEXT_DAY(eta.eta_ts - 7, eta.depart_day), eta.depart_tm) - NVL(eta.eta_wk, 0) * 7 AS depart_ts,
                 eta.depart_wk, eta.llr_day, eta.llr_tm, eta.llr_wk
            FROM eta
           WHERE eta.eta_ts >= eta.shp_dt),
         depart2 AS
         (SELECT depart.shp_dt, depart.cust_id, depart.load_num, depart.eta_ts,
                 (CASE
                    WHEN depart.depart_ts > depart.eta_ts THEN depart.depart_ts - 7
                    ELSE depart.depart_ts
                  END) AS depart_ts, depart.depart_wk, depart.llr_day, depart.llr_tm, depart.llr_wk
            FROM depart),
         llr AS
         (SELECT depart2.shp_dt, depart2.cust_id, depart2.load_num, depart2.eta_ts, depart2.depart_ts,
                 dt_tm_fn(NEXT_DAY(depart2.depart_ts - 7, depart2.llr_day) - NVL(depart2.depart_wk, 0) * 7,
                          depart2.llr_tm
                         )
                 - NVL(depart2.llr_wk, 0) * 7 AS llr_ts
            FROM depart2),
         llr2 AS
         (SELECT llr.shp_dt, llr.cust_id, llr.load_num, llr.eta_ts, llr.depart_ts,
                 (CASE
                    WHEN llr.llr_ts > llr.depart_ts THEN llr.llr_ts - 7
                    ELSE llr.llr_ts
                  END) AS llr_ts
            FROM llr),
         x AS
         (SELECT llr2.shp_dt, llr2.load_num, llr2.eta_ts, llr2.llr_ts
            FROM llr2
           WHERE NOT EXISTS(SELECT 1
                              FROM cust_rte_ovrrd_rt3c cro
                             WHERE cro.div_part = i_div_part
                               AND cro.cust_id = llr2.cust_id
                               AND cro.llr_dt = TRUNC(llr2.llr_ts)
                               AND cro.load_num = llr2.load_num)
          UNION ALL
          SELECT o.shp_dt, cro.load_num, cro.eta_ts, dt_tm_fn(cro.llr_dt, l.llrctc) AS llr_ts
            FROM o, mclp120c l, cust_rte_ovrrd_rt3c cro, sysp200c c
           WHERE l.div_part = i_div_part
             AND l.test_bil_load_sw = 'N'
             AND cro.div_part = l.div_part
             AND cro.load_num = l.loadc
             AND cro.cust_id = o.cust_id
             AND TRUNC(cro.eta_ts) >= o.shp_dt
             AND c.div_part = cro.div_part
             AND c.acnoc = cro.cust_id
             AND c.statc IN('1', '3')),
         xx AS
         (SELECT   x.llr_ts, x.load_num, x.eta_ts
              FROM x
             WHERE x.llr_ts >= l_dflt_prcs_ts
          ORDER BY x.eta_ts, x.llr_ts, x.load_num)
    SELECT xx.llr_ts, xx.load_num
      INTO o_llr_ts, o_load_num
      FROM xx
     WHERE ROWNUM = 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END nxt_load_with_eta_aftr_sp;

  /*
  ||----------------------------------------------------------------------------
  || NXT_LOAD_WITH_ETA_B4_SP
  ||  Return next LLR/Load for order with ETA before or equal to ship date (MustArriveByDate).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/28/19 | rhalpai | Original for PIR18852
  || 03/15/19 | rhalpai | Change logic to get ETA <= ShipDate. PIR18852
  || 12/20/24 | rhalpai | Change logic to get TRUNC(ETA) <= ShipDate and return first ETA descending with LLR >= ProcessTS. SDHD-2131514
  || 07/15/25 | rhalpai | Add logic to allow GMP order on GRO load while continuing to restrict GRO order on GMP load.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE nxt_load_with_eta_b4_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_prcs_ts   IN      DATE,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ORDER_LOAD_PK.NXT_LOAD_WITH_ETA_B4_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_dflt_prcs_ts        DATE;
    l_frst_dow            VARCHAR2(3);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_dflt_prcs_ts :=(CASE
                        WHEN i_prcs_ts IS NULL THEN l_c_sysdate
                        ELSE LEAST(i_prcs_ts, l_c_sysdate)
                      END);
    l_frst_dow := NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_frst_dow), 'SUN');
    logs.dbg('Get Load Info');


    WITH o AS
         (SELECT a.custa AS cust_id, DECODE(a.ldtypa, 'GMP', 'GMP', 'GRO') AS load_typ,
                 DATE '1900-02-28' + a.shpja AS shp_dt,
                 GREATEST(DATE '1900-02-28' + a.shpja, l_dflt_prcs_ts) AS prcs_ts
            FROM ordp100a a
           WHERE a.div_part = i_div_part
             AND a.ordnoa = i_ord_num),
         eta AS
         (SELECT o.prcs_ts, o.shp_dt, o.cust_id, md.loadd AS load_num, md.wkoffd AS eta_wk, l.depdac AS depart_day,
                 l.deptmc AS depart_tm, l.depwkc AS depart_wk, l.llrcdc AS llr_day, l.llrctc AS llr_tm, l.llrwkc AS llr_wk,
                 dt_tm_fn(NEXT_DAY(o.prcs_ts - 7, md.dayrcd), md.etad) AS eta_ts
            FROM o, mclp120c l, mclp040d md, sysp200c c
           WHERE l.div_part = i_div_part
             AND l.test_bil_load_sw = 'N'
             AND l.lbsgpc = DECODE(o.load_typ, 'GRO', 'N', l.lbsgpc)
             AND md.div_part = l.div_part
             AND md.loadd = l.loadc
             AND md.custd = o.cust_id
             AND md.prod_typ IN('BTH', o.load_typ)
             AND MOD((wk_beg_fn(NEXT_DAY(o.prcs_ts - 7, md.dayrcd), l_frst_dow) - wk_beg_fn(NEXT_DAY(md.eff_dt - 1, l.llrcdc), l_frst_dow)) / 7,
                     md.recur_wk
                    ) = 0
             AND NEXT_DAY(o.prcs_ts - 7, md.dayrcd) >= md.eff_dt
             AND c.div_part = md.div_part
             AND c.acnoc = md.custd
             AND c.statc IN('1', '3')),
         depart AS
         (SELECT eta.prcs_ts, eta.shp_dt, eta.cust_id, eta.load_num, eta.eta_ts,
                 dt_tm_fn(NEXT_DAY(eta.eta_ts - 7, eta.depart_day), eta.depart_tm) - NVL(eta.eta_wk, 0) * 7 AS depart_ts,
                 eta.depart_wk, eta.llr_day, eta.llr_tm, eta.llr_wk
            FROM eta
           WHERE TRUNC(eta.eta_ts) <= eta.shp_dt),
         depart2 AS
         (SELECT depart.prcs_ts, depart.shp_dt, depart.cust_id, depart.load_num, depart.eta_ts,
                 (CASE
                    WHEN depart.depart_ts > depart.eta_ts THEN depart.depart_ts - 7
                    ELSE depart.depart_ts
                  END) AS depart_ts, depart.depart_wk, depart.llr_day, depart.llr_tm, depart.llr_wk
            FROM depart),
         llr AS
         (SELECT depart2.prcs_ts, depart2.shp_dt, depart2.cust_id, depart2.load_num, depart2.eta_ts, depart2.depart_ts,
                 dt_tm_fn(NEXT_DAY(depart2.depart_ts - 7, depart2.llr_day) - NVL(depart2.depart_wk, 0) * 7,
                          depart2.llr_tm
                         )
                 - NVL(depart2.llr_wk, 0) * 7 AS llr_ts
            FROM depart2),
         llr2 AS
         (SELECT llr.prcs_ts, llr.shp_dt, llr.cust_id, llr.load_num, llr.eta_ts, llr.depart_ts,
                 (CASE
                    WHEN llr.llr_ts > llr.depart_ts THEN llr.llr_ts - 7
                    ELSE llr.llr_ts
                  END) AS llr_ts
            FROM llr),
         x AS
         (SELECT llr2.prcs_ts, llr2.shp_dt, llr2.load_num, llr2.eta_ts, llr2.llr_ts
            FROM llr2
           WHERE NOT EXISTS(SELECT 1
                              FROM cust_rte_ovrrd_rt3c cro
                             WHERE cro.div_part = i_div_part
                               AND cro.cust_id = llr2.cust_id
                               AND cro.llr_dt = TRUNC(llr2.llr_ts)
                               AND cro.load_num = llr2.load_num)
          UNION ALL
          SELECT o.prcs_ts, o.shp_dt, cro.load_num, cro.eta_ts, dt_tm_fn(cro.llr_dt, l.llrctc) AS llr_ts
            FROM o, mclp120c l, cust_rte_ovrrd_rt3c cro, sysp200c c
           WHERE l.div_part = i_div_part
             AND l.test_bil_load_sw = 'N'
             AND cro.div_part = l.div_part
             AND cro.load_num = l.loadc
             AND cro.cust_id = o.cust_id
             AND TRUNC(cro.eta_ts) >= o.shp_dt
             AND c.div_part = cro.div_part
             AND c.acnoc = cro.cust_id
             AND c.statc IN('1', '3')),
         xx AS
         (SELECT   x.llr_ts, x.load_num, x.eta_ts
              FROM x
             WHERE TRUNC(x.eta_ts) <= x.shp_dt
               AND TRUNC(x.eta_ts) <= x.prcs_ts
               AND x.llr_ts >= l_dflt_prcs_ts
          ORDER BY x.eta_ts DESC, x.llr_ts, x.load_num)
    SELECT xx.llr_ts, xx.load_num
      INTO o_llr_ts, o_load_num
      FROM xx
     WHERE ROWNUM = 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END nxt_load_with_eta_b4_sp;

  /*
  ||----------------------------------------------------------------------------
  || NXT_LOAD_WITH_LLR_AFTR_SP
  ||  Return next LLR/Load for order with LLR after or equal to process date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/28/19 | rhalpai | Original for PIR18852
  || 03/15/21 | rhalpai | Modify the SQL to handle recur_wk > 2. PIR21005
  || 07/15/25 | rhalpai | Add logic to allow GMP order on GRO load while continuing to restrict GRO order on GMP load.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE nxt_load_with_llr_aftr_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_prcs_ts   IN      DATE,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm             := 'OP_ORDER_LOAD_PK.NXT_LOAD_WITH_LLR_AFTR_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                      := SYSDATE;
    l_dflt_prcs_ts        DATE;
    l_t_parms             op_types_pk.tt_varchars_v;
    l_t_goodies_custs     type_stab;
    l_llr_offset_mins     NUMBER;
    l_frst_dow            VARCHAR2(3);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_dflt_prcs_ts :=(CASE
                        WHEN i_prcs_ts IS NULL THEN l_c_sysdate
                        ELSE LEAST(i_prcs_ts, l_c_sysdate)
                      END);
    l_t_parms := op_parms_pk.idx_vals_fn(i_div_part,
                                         op_const_pk.prm_llr_offset_mins || ',' || op_const_pk.prm_frst_dow);
    l_t_goodies_custs := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_goodies_cus);
    l_llr_offset_mins := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_llr_offset_mins)), 0);
    l_frst_dow := NVL(l_t_parms(op_const_pk.prm_frst_dow), 'SUN');
    logs.dbg('Get Load Info');

    WITH o AS(
      SELECT   oh.cust_id,
               -- treat non-GMP load types (such as P00) as GRO
               DECODE(oh.load_typ, 'GMP', 'GMP', 'GRO') AS load_typ,
               GREATEST(COALESCE(   -- override date for reg strict item orders
                                 NVL(op_strict_order_pk.prev_prod_rcpt_ts_fn(dv.div_id,
                                                                             so.cbr_vndr_id,
                                                                             MIN(dt_tm_fn(r.llr_dt, r.llr_time))
                                                                            ),
                                     MAX(so.prod_rcpt_ts)
                                    ) - l_llr_offset_mins / 24 / 60,
                                 -- use one minute after LLR date/time cutoff if LLR date is passed
                                 (CASE
                                    WHEN(    NVL(i_prcs_ts + INTERVAL '1' MINUTE, l_dflt_prcs_ts) < oh.shp_dt
                                         AND oh.cust_id IN(SELECT t.column_value
                                                             FROM TABLE(CAST(l_t_goodies_custs AS type_stab)) t)
                                        ) THEN oh.shp_dt
                                    ELSE i_prcs_ts + INTERVAL '1' MINUTE
                                  END
                                 ),
                                 l_dflt_prcs_ts
                                ),
                        l_dflt_prcs_ts
                       ) AS prcs_ts
          FROM (SELECT a.ordnoa AS ord_num, a.custa AS cust_id, a.ldtypa AS load_typ, DATE '1900-02-28' + a.shpja AS shp_dt
                  FROM ordp100a a
                 WHERE a.div_part = i_div_part
                   AND a.ordnoa = i_ord_num) oh
               INNER JOIN div_mstr_di1d dv
               ON dv.div_part = i_div_part
               LEFT OUTER JOIN strct_ord_op1o so
               ON (    so.div_part = i_div_part
                   AND so.ord_num = oh.ord_num
                   AND so.stat <> 'XCP'
                   AND EXISTS(SELECT 1
                                FROM ordp120b b
                               WHERE b.div_part = so.div_part
                                 AND b.ordnob = so.ord_num
                                 AND b.lineb = so.ord_ln
                                 AND b.statb <> 'C')
                  )
               LEFT OUTER JOIN reroute_rt1r r
               ON(    r.div_part = i_div_part
                  AND r.cust_id = oh.cust_id
                  AND so.prod_rcpt_ts BETWEEN r.eff_ts AND r.end_ts
                  AND GREATEST(so.prod_rcpt_ts, l_dflt_prcs_ts) <= dt_tm_fn(r.llr_dt, r.llr_time)
                 )
      GROUP BY dv.div_id, oh.cust_id, oh.load_typ, oh.shp_dt, so.cbr_vndr_id
    ), adj AS(
      SELECT -7 + (7 * (LEVEL - 1)) AS days
        FROM DUAL
      CONNECT BY LEVEL <= 52
    ), x AS(
      SELECT llr.prcs_ts, llr.load_num, llr.llr_ts
        FROM (SELECT o.prcs_ts, o.cust_id, md.loadd AS load_num,
                     dt_tm_fn(NEXT_DAY(md.eff_dt - 1, l.llrcdc)
                              +(wk_beg_fn(o.prcs_ts, l_frst_dow)
                                - wk_beg_fn(NEXT_DAY(md.eff_dt - 1, l.llrcdc), l_frst_dow)
                               )
                              +(NVL(l.llrwkc, 0) * 7),
                              l.llrctc
                             )
                     + adj.days AS llr_ts
                FROM o, mclp120c l, mclp040d md, sysp200c c, adj
               WHERE l.div_part = i_div_part
                 AND l.test_bil_load_sw = 'N'
                 AND l.lbsgpc = DECODE(o.load_typ, 'GRO', 'N', l.lbsgpc)
                 AND md.div_part = l.div_part
                 AND md.loadd = l.loadc
                 AND md.custd = o.cust_id
                 AND md.prod_typ IN('BTH', o.load_typ)
                 AND MOD((wk_beg_fn(o.prcs_ts, l_frst_dow) - wk_beg_fn(NEXT_DAY(md.eff_dt - 1, l.llrcdc), l_frst_dow) + adj.days)
                         / 7,
                         md.recur_wk
                        ) = 0
                 AND TRUNC(o.prcs_ts) >= md.eff_dt
                 AND c.div_part = md.div_part
                 AND c.acnoc = md.custd
                 AND c.statc IN('1', '3')
             ) llr
       WHERE NOT EXISTS(SELECT 1
                          FROM cust_rte_ovrrd_rt3c cro
                         WHERE cro.div_part = i_div_part
                           AND cro.cust_id = llr.cust_id
                           AND cro.llr_dt = TRUNC(llr.llr_ts)
                           AND cro.load_num = llr.load_num)
         AND llr.llr_ts >= llr.prcs_ts
      UNION ALL
      SELECT o.prcs_ts, cro.load_num,
             NEXT_DAY(dt_tm_fn(o.prcs_ts + adj.days, l.llrctc), l.llrcdc) + (NVL(l.llrwkc, 0) * 7) AS llr_ts
        FROM o, mclp120c l, cust_rte_ovrrd_rt3c cro, sysp200c c, adj
       WHERE l.div_part = i_div_part
         AND l.test_bil_load_sw = 'N'
         AND cro.div_part = l.div_part
         AND cro.load_num = l.loadc
         AND cro.cust_id = o.cust_id
         AND cro.llr_dt = NEXT_DAY(TRUNC(o.prcs_ts) + adj.days + (NVL(l.llrwkc, 0) * 7), l.llrcdc)
         AND c.div_part = cro.div_part
         AND c.acnoc = cro.cust_id
         AND c.statc IN('1', '3')
    )
    SELECT xx.llr_ts, xx.load_num
      INTO o_llr_ts, o_load_num
      FROM (SELECT   x.llr_ts, x.load_num
                FROM x
               WHERE x.llr_ts >= x.prcs_ts
            ORDER BY 1, 2) xx
     WHERE ROWNUM = 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END nxt_load_with_llr_aftr_sp;

  /*
  ||----------------------------------------------------------------------------
  || SEQ_ASSGN_LOAD_SP
  ||  Return next LLR/Load for sequentially assigned load order source.
  ||  Express orders will not have Cust Load assignments.
  ||  They are identified by order source of XPR.
  ||  Load series E100-E799 will be defined on mainframe for XPR use.
  ||  Stop numbers must be between 1 and 99.
  ||  Cust may have more than one order and subsequend orders for same LLR will be assigned to same Load/Stop.
  ||  New orders will be assigned to the earliest available LLR/Load within load series with an available Stop between 1 and 99.
  ||
  ||  Example:
  ||    Customer 1 sends in their first order which hits a Monday E100 load and is placed on Stop 1.
  ||    Customer 2 sends in their first order which hits a Monday E100 load and is placed on Stop 2.
  ||    Customer 3 sends in their first order which hits a Monday E100 load and is placed on Stop 3.
  ||    Customer 1 sends in their 2nd order which hits a Monday E100 load and is placed on Stop 1.
  ||    The next 96 customers send in their first orders which hits a Monday E100 load and is placed on Stop 4-99 respectively. Load E100 is now full of customers on all 99 stops.
  ||    The 100th customer send in their first orders which hits a Monday load. It falls to load E101 load and is placed on Stop 1.
  ||    The next order is for a customer 56 which already has an order on Load E100 Stop 56. Load E100 is still open and not billed. This new order should ball to Load E100 Stop 56.
  ||    It is now Monday at 10:01 AM (just past the Billing Cutoff for these loads). Customer 56 now sends in their 3rd order which is the first order received past the cutoff. This order will fall to load E200 Stop 1. Regardless if load E100 is billed or not.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/21/19 | rhalpai | Original for PIR19563
  || 07/15/25 | rhalpai | Add logic to allow GMP order on GRO load while continuing to restrict GRO order on GMP load.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE seq_assgn_load_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_prcs_ts   IN      DATE,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm             := 'OP_ORDER_LOAD_PK.SEQ_ASSGN_LOAD_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                      := SYSDATE;
    l_dflt_prcs_ts        DATE;
    l_load_min            VARCHAR2(4);
    l_load_max            VARCHAR2(4);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_dflt_prcs_ts :=(CASE
                        WHEN i_prcs_ts IS NULL THEN l_c_sysdate
                        ELSE LEAST(i_prcs_ts, l_c_sysdate)
                      END);
    l_load_min := op_parms_pk.val_fn(i_div_part, 'SAL_LOAD_MIN');
    l_load_max := op_parms_pk.val_fn(i_div_part, 'SAL_LOAD_MAX');
    logs.dbg('Get Load Info');

    WITH o AS
         (SELECT a.custa AS cust_id, a.ldtypa AS load_typ
            FROM ordp100a a
           WHERE a.div_part = i_div_part
             AND a.ordnoa = i_ord_num),
         lds AS
         (SELECT l.loadc AS load_num, l.llrcdc AS llr_day, l.llrctc AS llr_tm, l.llrwkc AS llr_wk
            FROM o, mclp120c l
           WHERE l.div_part = i_div_part
             AND l.loadc BETWEEN l_load_min AND l_load_max
             AND l.lbsgpc = DECODE(o.load_typ, 'GRO', 'N', l.lbsgpc)
             AND l.test_bil_load_sw = 'N'),
         adj AS
         (SELECT     -7 +(7 *(LEVEL - 1)) AS days
                FROM DUAL
          CONNECT BY LEVEL <= 10),
         x AS
         (SELECT llr.llr_ts, llr.load_num
            FROM (SELECT lds.load_num,
                         dt_tm_fn(NEXT_DAY(l_dflt_prcs_ts - 1, lds.llr_day), lds.llr_tm)
                         +(NVL(lds.llr_wk, 0) * 7)
                         + adj.days AS llr_ts
                    FROM lds, adj) llr
           WHERE llr.llr_ts >= l_dflt_prcs_ts
             AND NOT EXISTS(SELECT 1
                              FROM load_depart_op1f ld, ordp100a a
                             WHERE ld.div_part = i_div_part
                               AND ld.load_num = llr.load_num
                               AND ld.llr_dt = TRUNC(llr.llr_ts)
                               AND a.div_part = ld.div_part
                               AND a.load_depart_sid = ld.load_depart_sid
                               AND a.stata = 'A')
             AND EXISTS(SELECT 1
                          FROM (SELECT     LEVEL AS stop_num
                                      FROM DUAL
                                CONNECT BY LEVEL <= 99) s
                         WHERE NOT EXISTS(SELECT 1
                                            FROM load_depart_op1f ld, stop_eta_op1g se
                                           WHERE ld.div_part = i_div_part
                                             AND ld.load_num = llr.load_num
                                             AND ld.llr_dt = TRUNC(llr.llr_ts)
                                             AND se.div_part = ld.div_part
                                             AND se.load_depart_sid = ld.load_depart_sid
                                             AND se.stop_num = s.stop_num
                                             AND EXISTS(SELECT 1
                                                          FROM ordp100a a
                                                         WHERE a.div_part = se.div_part
                                                           AND a.load_depart_sid = se.load_depart_sid
                                                           AND a.custa = se.cust_id
                                                           AND a.stata IN('O', 'I', 'S', 'P', 'R'))))
          UNION ALL
          SELECT ld.llr_ts, ld.load_num
            FROM o, stop_eta_op1g se, load_depart_op1f ld
           WHERE se.div_part = i_div_part
             AND se.cust_id = o.cust_id
             AND ld.div_part = se.div_part
             AND ld.load_depart_sid = se.load_depart_sid
             AND ld.load_num IN(SELECT lds.load_num
                                  FROM lds)
             AND ld.llr_ts >= l_dflt_prcs_ts
             AND EXISTS(SELECT 1
                          FROM ordp100a a
                         WHERE a.div_part = se.div_part
                           AND a.load_depart_sid = se.load_depart_sid
                           AND a.custa = se.cust_id
                           AND a.stata IN('O', 'I', 'S', 'P', 'R')))
    SELECT DISTINCT FIRST_VALUE(x.llr_ts) OVER(ORDER BY x.llr_ts, x.load_num),
                    FIRST_VALUE(x.load_num) OVER(ORDER BY x.llr_ts, x.load_num)
      INTO o_llr_ts, o_load_num
    FROM            x;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END seq_assgn_load_sp;

  /*
  ||----------------------------------------------------------------------------
  || NXT_LOAD_FOR_ORD_SP
  ||  Returns the next available LLR/Load for the order.
  ||
  || Business Requirements:
  ||   GMP loads will only accept GMP orders with Stops of 'GMP' or 'BTH'
  ||   GRO loads with Stops of 'BTH' will accept GRO or GMP orders
  ||   GRO loads with Stops of 'GRO' or 'GMP' will only accept matching orders
  ||   Treat non-GMP orders as GRO
  ||   LLR must be >= Run Time
  ||   Depart must be >= LLR
  ||   ETA must be >= Depart
  ||   Order Cutoff and Load Pricing must be <= LLR
  ||   Handle week offsets
  ||   PrePost customers cannot be assigned to released loads
  ||   TestBill loads are not allowed
  ||   No inactive customers
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/13 | rhalpai | Original
  || 10/28/13 | rhalpai | Change logic to include Load as part of primary key
  ||                    | of CUST_RTE_OVRRD_RT3C. IM-123463
  || 12/07/15 | rhalpai | Change logic to support cust load/stop recurrence
  ||                    | logic (i.e.: WJ A/B bi-weekly load schedule). PIR14916
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.IDX_VALS_FN,
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN. PIR15427
  || 01/28/19 | rhalpai | Move cursor to NXT_LOAD_WITH_LLR_AFTR_SP and change to
  ||                    | be a wrapper to call appropriate process (NXT_LOAD_WITH_LLR_AFTR_SP
  ||                    | for regular orders, NXT_LOAD_WITH_ETA_AFTR_SP for distributions,
  ||                    | NXT_LOAD_WITH_ETA_B4_SP for MustArriveByDate passed in ship date).
  ||                    | PIR18852
  || 03/15/19 | rhalpai | Change logic to call NXT_LOAD_WITH_ETA_B4_SP for
  ||                    | MustArriveByDate and NXT_LOAD_WITH_ETA_AFTR_SP for
  ||                    | distributions (was reversed). PIR18852
  || 08/21/19 | rhalpai | Add logic to call SEQ_ASSGN_LOAD_SP for sequentially assigned load order source (Express orders). PIR19563
  ||----------------------------------------------------------------------------
  */
  PROCEDURE nxt_load_for_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    i_prcs_ts   IN      DATE,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm             := 'OP_ORDER_LOAD_PK.NXT_LOAD_FOR_ORD_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                      := SYSDATE;
    l_dflt_prcs_ts        DATE;
    l_t_eta_by_ord        type_stab;
    l_ship_dt             DATE;
    l_crp_cd              VARCHAR2(3);
    l_ord_typ             VARCHAR2(1);
    l_seq_assgn_load_sw   VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_dflt_prcs_ts :=(CASE
                        WHEN i_prcs_ts IS NULL THEN l_c_sysdate
                        ELSE LEAST(i_prcs_ts, l_c_sysdate)
                      END);
    l_t_eta_by_ord := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_eta_by_ord);
    logs.dbg('Get Order Info');

    SELECT DATE '1900-02-28' + a.shpja, LPAD(cx.corpb, 3, '0'), a.dsorda, DECODE(s.ord_src, NULL, 'N', 'Y')
      INTO l_ship_dt, l_crp_cd, l_ord_typ, l_seq_assgn_load_sw
      FROM ordp100a a, mclp020b cx, sub_prcs_ord_src s
     WHERE a.div_part = i_div_part
       AND a.ordnoa = i_ord_num
       AND cx.div_part = a.div_part
       AND cx.custb = a.custa
       AND s.div_part(+) = a.div_part
       AND s.prcs_id(+) = 'ORDER RECEIPT'
       AND s.prcs_sbtyp_cd(+) = 'SAL'
       AND s.ord_src(+) = a.ipdtsa;

    IF l_ord_typ = 'D' THEN
      logs.dbg('Get Load Info with ETA After for Dist');
      nxt_load_with_eta_aftr_sp(i_div_part, i_ord_num, i_prcs_ts, o_llr_ts, o_load_num);
    ELSIF (    l_ship_dt >= l_dflt_prcs_ts
           AND l_crp_cd MEMBER OF l_t_eta_by_ord) THEN
      logs.dbg('Get Load Info with ETA Before for EtaByOrd');
      nxt_load_with_eta_b4_sp(i_div_part, i_ord_num, i_prcs_ts, o_llr_ts, o_load_num);
    ELSIF l_seq_assgn_load_sw = 'Y' THEN
      logs.dbg('Get Load Info for SeqAssgnLoad Order');
      seq_assgn_load_sp(i_div_part, i_ord_num, i_prcs_ts, o_llr_ts, o_load_num);
    END IF;   -- l_ord_typ = 'D'

    IF o_load_num IS NULL THEN
      logs.dbg('Get Load Info with LLR After');
      nxt_load_with_llr_aftr_sp(i_div_part, i_ord_num, i_prcs_ts, o_llr_ts, o_load_num);
    END IF;   -- o_load_num IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END nxt_load_for_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || NXT_LOAD_FOR_DIST_ORD_SP
  ||  Returns the next available LLR/Load for the distribution order.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/13 | rhalpai | Original
  || 01/05/16 | rhalpai | Change logic to allow distributions to attach to
  ||                    | Strict Orders for customers with split_po_cd of X.
  ||                    | SDOPS-110
  || 08/03/18 | rhalpai | Add logic to call NXT_LOAD_FOR_ORD_SP passing ShipDate
  ||                    | as PrcsTS for DistOnly Customers. PIR18748
  || 11/02/18 | rhalpai | Add logic for parm offsets for shipdate at div and corp
  ||                    | levels where corp level overrides div level. PIR18748
  || 01/28/19 | rhalpai | Change to call NXT_LOAD_WITH_LLR_AFTR_SP for DistOnly Customers.
  ||                    | PIR18852
  || 03/02/22 | rhalpai | Change logic to only assign for active customers. PIR19208
  || 07/15/25 | rhalpai | Add logic to allow GMP order on GRO load while continuing to restrict GRO order on GMP load.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE nxt_load_for_dist_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    o_llr_ts    OUT     DATE,
    o_load_num  OUT     VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm          := 'OP_ORDER_LOAD_PK.NXT_LOAD_FOR_DIST_ORD_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                   := SYSDATE;
    l_c_curr_dt  CONSTANT DATE                   := TRUNC(l_c_sysdate);
    l_cv                  SYS_REFCURSOR;
    l_cust_id             sysp200c.acnoc%TYPE;
    l_crp_cd              NUMBER;
    l_load_typ            ordp100a.ldtypa%TYPE;
    l_dist_only_sw        VARCHAR2(1);
    l_shp_dt              DATE;
    l_max_shp_dt          DATE;
    l_shp_dt_offset       NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Open Cursor for Dist Order Info');

    -- l_max_shp_dt is mainly used to prevent pulling future distributions.
    -- In the event no reg order is available to attach until after the
    -- max ship we want the dist order to attach to the next avail reg order.
    OPEN l_cv
     FOR
       SELECT x.cust_id, x.crp_cd, x.load_typ, x.dist_only_sw, x.shp_dt,
              (CASE
                 WHEN x.shp_dt + x.dislad <= l_c_sysdate THEN DATE '2999-12-31'
                 ELSE l_c_curr_dt + x.dislad
               END
              ) AS max_shp_dt
         FROM (SELECT o.custa AS cust_id, cx.corpb AS crp_cd, o.ldtypa AS load_typ, ma.dist_only_sw, d.dislad,
                      NVL(NEXT_DAY(DATE '1900-02-28' + o.shpja - 1, c.dist_frst_day),
                          DATE '1900-02-28' + o.shpja
                         ) AS shp_dt
                 FROM mclp130d d, ordp100a o, sysp200c c, mclp100a ma, mclp020b cx
                WHERE d.div_part = i_div_part
                  AND o.div_part = d.div_part
                  AND o.ordnoa = i_ord_num
                  AND c.div_part = o.div_part
                  AND c.acnoc = o.custa
                  AND c.statc = '1'
                  AND ma.div_part = c.div_part
                  AND ma.cstgpa = c.retgpc
                  AND cx.div_part = o.div_part
                  AND cx.custb = o.custa) x;

    logs.dbg('Fetch Cursor for Dist Order Info');

    FETCH l_cv
     INTO l_cust_id, l_crp_cd, l_load_typ, l_dist_only_sw, l_shp_dt, l_max_shp_dt;

    IF l_cv%FOUND THEN
      IF l_dist_only_sw = 'Y' THEN
        IF l_shp_dt < l_c_curr_dt THEN
          l_shp_dt := l_c_curr_dt;
        END IF;   -- l_shp_dt < l_c_curr_dt

        l_shp_dt_offset := TO_NUMBER(COALESCE(op_parms_pk.val_fn(i_div_part,
                                                                 'DIST_ONLY_OFFSET_' || LPAD(l_crp_cd, 3, '0')
                                                                ),
                                              op_parms_pk.val_fn(i_div_part, 'DIST_ONLY_DIV_OFFSET'),
                                              '0'
                                             )
                                    );
        l_shp_dt := l_shp_dt + l_shp_dt_offset;

        IF l_shp_dt BETWEEN l_c_curr_dt + l_shp_dt_offset AND l_max_shp_dt - 1 THEN
          logs.dbg('NextLoadWithLlrAftr with ShipDt as PrcsTS');
          nxt_load_with_llr_aftr_sp(i_div_part, i_ord_num, l_shp_dt, o_llr_ts, o_load_num);
        END IF;   -- l_shp_dt BETWEEN l_c_curr_dt + l_shp_dt_offset AND l_max_shp_dt - 1
      ELSE
        logs.dbg('Open Cursor for Reg Order Load Info');

        OPEN l_cv
         FOR
           SELECT   ld.llr_ts, ld.load_num
               FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se, mclp040d d, mclp120c c
              WHERE a.div_part = i_div_part
                AND a.custa = l_cust_id
                AND a.ldtypa = DECODE(l_load_typ, 'GRO', 'GRO', a.ldtypa)
                AND a.dsorda = 'R'
                AND a.stata = 'O'
                AND ld.div_part = a.div_part
                AND ld.load_depart_sid = a.load_depart_sid
                AND se.div_part = a.div_part
                AND se.load_depart_sid = a.load_depart_sid
                AND se.cust_id = a.custa
                AND TRUNC(se.eta_ts) BETWEEN l_shp_dt AND l_max_shp_dt - 1
                AND d.div_part = ld.div_part
                AND d.loadd = ld.load_num
                AND d.stopd = se.stop_num
                AND d.custd = se.cust_id
                AND d.prod_typ IN('BTH', l_load_typ)
                AND c.div_part = d.div_part
                AND c.loadc = d.loadd
                AND c.lbsgpc = DECODE(l_load_typ, 'GRO', 'N', c.lbsgpc)
                AND c.test_bil_load_sw = 'N'
                AND c.aadisc = 'Y'
                AND NOT EXISTS(SELECT 1
                                 FROM sub_prcs_ord_src s
                                WHERE s.div_part = a.div_part
                                  AND s.prcs_id = 'ATTACH DIST'
                                  AND s.prcs_sbtyp_cd = 'BAD'
                                  AND s.ord_src = a.ipdtsa)
                AND EXISTS(SELECT 1
                             FROM ordp120b b
                            WHERE b.div_part = a.div_part
                              AND b.ordnob = a.ordnoa
                              AND b.statb = 'O')
                AND NOT EXISTS(SELECT 1
                                 FROM ordp120b b
                                WHERE b.div_part = a.div_part
                                  AND b.ordnob = a.ordnoa
                                  AND b.statb NOT IN('O', 'I', 'S', 'C'))
                AND NOT EXISTS(SELECT 1
                                 FROM ordp120b b, mclp140a ma
                                WHERE b.div_part = a.div_part
                                  AND b.ordnob = a.ordnoa
                                  AND b.excptn_sw = 'Y'
                                  AND ma.rsncda = b.ntshpb
                                  AND ma.exlvla = 1)
                AND NOT EXISTS(SELECT 1
                                 FROM mclpinpr r
                                WHERE r.div_part = a.div_part
                                  AND r.ordnor = a.ordnoa)
                AND NOT EXISTS(SELECT 1
                                 FROM split_ord_op2s s, sysp200c c, mclp100a g
                                WHERE s.div_part = a.div_part
                                  AND s.ord_num = a.ordnoa
                                  AND s.split_typ = 'STRICT ORD'
                                  AND c.div_part = a.div_part
                                  AND c.acnoc = a.custa
                                  AND g.div_part = c.div_part
                                  AND g.cstgpa = c.retgpc
                                  AND g.split_po_cd <> 'X')
           ORDER BY a.excptn_sw, se.eta_ts, a.ordnoa;

        logs.dbg('Fetch Cursor for Reg Order Load Info');

        FETCH l_cv
         INTO o_llr_ts, o_load_num;
      END IF;   -- l_dist_only_sw = 'Y'
    END IF;   -- l_cv%FOUND

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END nxt_load_for_dist_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || ATTACH_DIST_ORDS_SP
  ||   Used to assign the load info for a regular order to that of unassigned
  ||   available distribution orders. This is known as attaching distributions.
  ||   Commit and error handling are up to the calling program.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/31/08 | rhalpai | Original
  || 12/29/08 | rhalpai | Changed cursor to include restriction of load/stop
  ||                    | assignments with ATTCH_DIST_SW=Y. PIR6113
  || 11/10/10 | rhalpai | Remove reference to unused column in cursor. PIR5878
  || 03/28/11 | rhalpai | Changed logic to apply max qty for specified dist
  ||                    | items. PIR10007
  || 04/05/11 | rhalpai | Changed logic to apply max qty distributions with
  ||                    | BypassMax OFF. SP11BIL
  || 04/20/11 | rhalpai | Changed logic to exclude attachment to orders with
  ||                    | order sources that should not bill alone. PIR9910
  || 05/20/11 | rhalpai | Changed logic to use DIST_FRST_DAY for customer to
  ||                    | calculate ship date. Removed logic for ATTCH_DIST_SW.
  ||                    | PIR9030
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw. PIR11038
  || 12/08/15 | rhalpai | Add DivPart in call to OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP.
  || 01/05/16 | rhalpai | Change logic to allow distributions to attach to
  ||                    | Strict Orders for customers with split_po_cd of X.
  ||                    | SDOPS-110
  ||----------------------------------------------------------------------------
  */
  PROCEDURE attach_dist_ords_sp(
    i_div_part         IN  NUMBER,
    i_load_depart_sid  IN  NUMBER,
    i_cust_id          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm  := 'OP_ORDER_LOAD_PK.ATTACH_DIST_ORDS_SP';
    lar_parm                logs.tar_parm;
    l_load_typ              VARCHAR2(3);
    l_dist_load_depart_sid  NUMBER;
    l_cv                    SYS_REFCURSOR;
    l_t_ord_nums            type_ntab;
    l_r_bef_load_info       g_rt_load_info;
    l_c_rsn_cd     CONSTANT VARCHAR2(8)    := 'ATTCHDIS';
    l_ord_qty_save          PLS_INTEGER;

    CURSOR l_cur_ords(
      b_div_part         NUMBER,
      b_load_depart_sid  NUMBER,
      b_cust_id          VARCHAR2,
      b_t_ord_nums       type_ntab
    ) IS
      SELECT b.ordnob AS ord_num, b.lineb AS ord_ln, TO_NUMBER(b.orditb) AS catlg_num, b.ordqtb AS ord_qty,
             b.bymaxb AS byp_max_sw, DECODE(a.pshipa, '1', 'Y', 'Y', 'Y', 'N') AS allw_partl_sw,
             NVL(di.max_ord_qty, 99999) AS item_max_qty, b.maxqtb AS ord_max_qty
        FROM ordp100a a, ordp120b b, mclp110b di
       WHERE b.div_part = b_div_part
         AND (b.ordnob, b.lineb) IN(SELECT FIRST_VALUE(b2.ordnob) OVER(PARTITION BY e.catite ORDER BY a.pshipa DESC,
                                            b2.subrcb, b2.ordqtb DESC),
                                           FIRST_VALUE(b2.lineb) OVER(PARTITION BY e.catite ORDER BY a.pshipa DESC,
                                            b2.subrcb, b2.ordqtb DESC)
                                      FROM sawp505e e, ordp100a a, ordp120b b2
                                     WHERE EXISTS(SELECT 1
                                                    FROM ordp120b DO, TABLE(CAST(b_t_ord_nums AS type_ntab)) t
                                                   WHERE DO.div_part = b_div_part
                                                     AND DO.ordnob = t.column_value
                                                     AND DO.excptn_sw = 'N'
                                                     AND DO.bymaxb IN('0', 'N')
                                                     AND DO.itemnb = e.iteme
                                                     AND DO.sllumb = e.uome)
                                       AND a.div_part = b_div_part
                                       AND a.load_depart_sid = b_load_depart_sid
                                       AND a.custa = b_cust_id
                                       AND a.stata = 'O'
                                       AND a.excptn_sw = 'N'
                                       AND a.dsorda IN('R', 'D')
                                       AND b2.div_part = a.div_part
                                       AND b2.ordnob = a.ordnoa
                                       AND b2.itemnb = e.iteme
                                       AND b2.sllumb = e.uome
                                       AND b2.excptn_sw = 'N'
                                       AND b2.statb = 'O'
                                       AND b2.subrcb <> 999
                                       AND b2.ordqtb > 0)
         AND di.div_part = b.div_part
         AND di.itemb = b.itemnb
         AND di.uomb = b.sllumb
         AND a.div_part = b.div_part
         AND a.ordnoa = b.ordnob;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadDepartSid', i_load_depart_sid);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    SELECT MAX(DECODE(l.lbsgpc, 'Y', 'GMP', 'GRO'))
      INTO l_load_typ
      FROM load_depart_op1f ld, mclp120c l, ordp100a a
     WHERE ld.div_part = i_div_part
       AND ld.load_depart_sid = i_load_depart_sid
       AND l.div_part = ld.div_part
       AND l.loadc = ld.load_num
       AND l.aadisc = 'Y'
       AND a.div_part = ld.div_part
       AND a.load_depart_sid = ld.load_depart_sid
       AND a.custa = i_cust_id
       AND a.excptn_sw = 'N'
       AND a.stata IN('O', 'I')
       AND a.dsorda = 'R'
       AND NOT EXISTS(SELECT 1
                        FROM strct_ord_op1o so, sysp200c c, mclp100a g
                       WHERE so.div_part = a.div_part
                         AND so.ord_num = a.ordnoa
                         AND c.div_part = a.div_part
                         AND c.acnoc = a.custa
                         AND g.div_part = c.div_part
                         AND g.cstgpa = c.retgpc
                         AND g.split_po_cd <> 'X')
       AND NOT EXISTS(SELECT 1
                        FROM sub_prcs_ord_src s
                       WHERE s.div_part = a.div_part
                         AND s.prcs_id = 'ATTACH DIST'
                         AND s.prcs_sbtyp_cd = 'BAD'
                         AND s.ord_src = a.ipdtsa);

    IF l_load_typ IS NOT NULL THEN
      logs.dbg('Get DIST LoadDepartSid');

      SELECT ld.load_depart_sid
        INTO l_dist_load_depart_sid
        FROM load_depart_op1f ld
       WHERE ld.div_part = i_div_part
         AND ld.llr_ts = DATE '1900-01-01'
         AND ld.load_num = 'DIST';

      logs.dbg('Open Dist Orders Cursor');

      OPEN l_cv
       FOR
         SELECT a.ordnoa
           FROM stop_eta_op1g se, sysp200c c, ordp100a a
          WHERE se.div_part = i_div_part
            AND se.load_depart_sid = i_load_depart_sid
            AND se.cust_id = i_cust_id
            AND c.div_part = se.div_part
            AND c.acnoc = se.cust_id
            AND a.div_part = se.div_part
            AND a.load_depart_sid = l_dist_load_depart_sid
            AND a.custa = se.cust_id
            AND a.ldtypa = l_load_typ
            AND a.stata = 'O'
            AND a.dsorda = 'D'
            AND NVL(NEXT_DAY(DATE '1900-02-28' + a.shpja - 1, c.dist_frst_day), DATE '1900-02-28' + a.shpja) <=
                                                                                                               se.eta_ts
            AND NOT EXISTS(SELECT 1
                             FROM mclpinpr m
                            WHERE m.div_part = a.div_part
                              AND m.ordnor = a.ordnoa);

      logs.dbg('Open Dist Orders Cursor');

      FETCH l_cv
      BULK COLLECT INTO l_t_ord_nums;

      IF l_t_ord_nums.COUNT > 0 THEN
        logs.dbg('Get Before Ord Load Info');
        op_order_load_pk.get_ord_load_info_sp(i_div_part, i_cust_id, l_dist_load_depart_sid, l_r_bef_load_info);
        logs.dbg('Upd StopEta');
        op_order_load_pk.merge_stop_eta_sp(i_div_part, i_load_depart_sid, i_cust_id);
        logs.dbg('Move Dist Orders');
        FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
          UPDATE ordp100a a
             SET a.load_depart_sid = i_load_depart_sid
           WHERE a.div_part = i_div_part
             AND a.ordnoa = l_t_ord_nums(i)
             AND a.load_depart_sid <> i_load_depart_sid;
        logs.dbg('Log Dist Order Moves');
        FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
          op_order_load_pk.log_ord_move_sp(i_div_part, l_t_ord_nums(i), l_r_bef_load_info, l_c_rsn_cd, i_user_id);
        END LOOP;
        logs.dbg('Apply Max Qtys for Applicable Dist Orders');
        FOR l_r_ord IN l_cur_ords(i_div_part, i_load_depart_sid, i_cust_id, l_t_ord_nums) LOOP
          l_ord_qty_save := l_r_ord.ord_qty;
          logs.dbg('Check Max Qty');
          op_order_validation_pk.check_max_qty_sp(i_div_part,
                                                  l_r_ord.ord_num,
                                                  l_r_ord.ord_ln,
                                                  l_r_ord.catlg_num,
                                                  l_r_ord.ord_qty,
                                                  l_r_ord.byp_max_sw,
                                                  l_r_ord.allw_partl_sw,
                                                  l_r_ord.item_max_qty,
                                                  l_r_ord.ord_max_qty
                                                 );

          IF l_r_ord.ord_qty < l_ord_qty_save THEN
            logs.dbg('Adjust Order Qty');

            UPDATE ordp120b
               SET ordqtb = l_r_ord.ord_qty
             WHERE div_part = i_div_part
               AND ordnob = l_r_ord.ord_num
               AND lineb = l_r_ord.ord_ln;
          END IF;   -- l_r_ord.ord_qty < l_ord_qty_save
        END LOOP;
      END IF;   -- l_t_ord_nums.COUNT > 0
    END IF;   -- l_load_typ IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END attach_dist_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || SYNCLOAD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/13 | rhalpai | Move logic from OP_MESSAGES_PK. PIR11038
  || 02/17/14 | rhalpai | Remove treat_dist_as_reg parm. Change logic to treat
  ||                    | Dist orders as Reg orders when their log includes a
  ||                    | manual move via Forceload. PIR13455
  || 01/28/15 | rhalpai | Change logic to assign load to dflt_load from cursor
  ||                    | and assign LLRTs to the dflt_ts (1900-01-01) when
  ||                    | dflt_load does not match the DFLT,TEST or DIST test
  ||                    | cases which have their own assignment logic.
  ||                    | IM-243608
  || 02/06/15 | rhalpai | Change logic within Syncload to assign load to
  ||                    | dflt_load from cursor and assign LLRTs to the dflt_ts
  ||                    | (1900-01-01) whenever load is not set. IM-246609
  || 12/01/15 | rhalpai | Add user_id parm. PIR15202
  || 10/14/17 | rhalpai | Change to use constants package OP_CONST_PK. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE syncload_sp(
    i_div_part    IN  NUMBER,
    i_rsn_cd      IN  VARCHAR2,
    i_t_ord_nums  IN  type_ntab,
    i_user_id     IN  VARCHAR2 DEFAULT USER
  ) IS
    l_c_module      CONSTANT typ.t_maxfqnm         := 'OP_ORDER_LOAD_PK.SYNCLOAD_SP';
    lar_parm                 logs.tar_parm;
    l_llr_ts                 DATE;
    l_load_num               mclp120c.loadc%TYPE;
    l_dflt_load_save         mclp120c.loadc%TYPE   := '~';
    l_llr_ts_save            DATE                  := DATE '0001-01-01';
    l_load_num_save          mclp120c.loadc%TYPE   := '~';
    l_cust_id_save           sysp200c.acnoc%TYPE   := '~';
    l_c_dflt_ts     CONSTANT DATE                  := DATE '1900-01-01';
    l_load_depart_sid        NUMBER;
    l_load_depart_sid_save   NUMBER                := -1;
    l_c_reg         CONSTANT VARCHAR2(1)           := 'R';
    l_c_stat_opn    CONSTANT VARCHAR2(1)           := 'O';
    l_c_skip_load   CONSTANT VARCHAR2(6)           := 'SKIPLD';
    l_c_upd_userid  CONSTANT VARCHAR2(8)           := 'SYNCLOAD';

    TYPE l_rt_load_ord IS RECORD(
      load_depart_sid  NUMBER,
      cust_id          sysp200c.acnoc%TYPE,
      old_llr_ts       DATE,
      old_load_num     mclp120c.loadc%TYPE,
      old_depart_ts    DATE,
      old_stop_num     NUMBER,
      old_eta_ts       DATE,
      t_ord_nums       type_ntab
    );

    TYPE l_tt_load_ords IS TABLE OF l_rt_load_ord;

    l_r_load_ord             l_rt_load_ord;
    l_t_load_ords            l_tt_load_ords        := l_tt_load_ords();

    TYPE l_rt_attach_ord IS RECORD(
      load_depart_sid  NUMBER,
      cust_id          sysp200c.acnoc%TYPE
    );

    TYPE l_tt_attach_ords IS TABLE OF l_rt_attach_ord;

    l_t_attach_ords          l_tt_attach_ords      := l_tt_attach_ords();

    CURSOR l_cur_ords(
      b_div_part    NUMBER,
      b_t_ord_nums  type_ntab
    ) IS
      SELECT   a.ordnoa AS ord_num, se.cust_id, a.dsorda AS ord_typ, a.ldtypa AS load_typ, a.stata AS stat_cd,
               (CASE
                  WHEN a.dsorda = 'T' THEN 'TEST'
                  WHEN a.dsorda IN('R', 'N') THEN 'DFLT'
                  WHEN ld.load_num = 'DFLT' THEN 'DFLT'
                  WHEN(    a.dsorda = 'D'
                       AND (   (    a.ldtypa BETWEEN 'P00' AND 'P99'
                                AND (   EXISTS(SELECT 1
                                                 FROM mclp040d d
                                                WHERE d.div_part = a.div_part
                                                  AND d.loadd = ld.load_num
                                                  AND d.custd = a.custa)
                                     OR EXISTS(SELECT 1
                                                 FROM cust_rte_ovrrd_rt3c cro
                                                WHERE cro.div_part = a.div_part
                                                  AND cro.cust_id = a.custa
                                                  AND cro.llr_dt = ld.llr_dt
                                                  AND cro.load_num = ld.load_num
                                                  AND cro.stop_num = se.stop_num)
                                    )
                               )
                            OR (    a.ldtypa NOT BETWEEN 'P00' AND 'P99'
                                AND EXISTS(SELECT 1
                                             FROM mclp300d md
                                            WHERE md.div_part = a.div_part
                                              AND md.ordnod = a.ordnoa
                                              AND md.ordlnd = 0
                                              AND md.reasnd = 'FORCELD')
                               )
                           )
                      ) THEN 'DFLT'
                  WHEN a.ldtypa BETWEEN 'P00' AND 'P99' THEN a.ldtypa || 'P'
                  WHEN a.dsorda = 'D' THEN 'DIST'
                  ELSE 'DFLT'
                END
               ) AS dflt_load,
               ld.llr_ts, ld.load_num, ld.depart_ts, se.stop_num, se.eta_ts
          FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se
         WHERE a.div_part = b_div_part
           AND a.stata IN('O', 'S')
           AND a.ordnoa IN(SELECT t.column_value
                             FROM TABLE(CAST(b_t_ord_nums AS type_ntab)) t)
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND se.div_part = a.div_part
           AND se.load_depart_sid = a.load_depart_sid
           AND se.cust_id = a.custa
      ORDER BY a.custa, DECODE(a.dsorda, 'R', 1, 'D', 2, 'T', 3, 'N', 4), a.stata, a.ldtypa;

    PROCEDURE move_ords_sp IS
    BEGIN
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        IF l_t_load_ords(i).t_ord_nums.COUNT > 0 THEN
          op_order_load_pk.move_ords_sp(i_div_part,
                                        l_t_load_ords(i).cust_id,
                                        l_t_load_ords(i).load_depart_sid,
                                        NULL,
                                        NULL,
                                        l_t_load_ords(i).old_llr_ts,
                                        l_t_load_ords(i).old_load_num,
                                        l_t_load_ords(i).old_depart_ts,
                                        l_t_load_ords(i).old_stop_num,
                                        l_t_load_ords(i).old_eta_ts,
                                        i_rsn_cd,
                                        l_c_upd_userid,
                                        l_t_load_ords(i).t_ord_nums
                                       );
        END IF;   -- t_load_ords(i).t_ord_nums.COUNT > 0
      END LOOP;
    END move_ords_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'OrdNumsTab', i_t_ord_nums);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_syncload,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                i_div_part
                                               );
    FOR l_r_ord IN l_cur_ords(i_div_part, i_t_ord_nums) LOOP
      IF l_r_ord.dflt_load <> l_dflt_load_save THEN
        IF l_dflt_load_save <> '~' THEN
          logs.dbg('Move Orders');
          move_ords_sp;
          logs.dbg('Clear All But Last Record and Clear OrdNums');
          l_r_load_ord := l_t_load_ords(l_t_load_ords.LAST);
          l_r_load_ord.t_ord_nums := type_ntab();
          l_t_load_ords := l_tt_load_ords();
          l_t_load_ords.EXTEND;
          l_t_load_ords(l_t_load_ords.LAST) := l_r_load_ord;
          l_r_load_ord := NULL;
        END IF;   -- l_dflt_load_save <> '~'

        l_dflt_load_save := l_r_ord.dflt_load;
      END IF;   -- l_r_ord.dflt_load <> l_dflt_load_save

      IF l_r_ord.dflt_load IN('DFLT', 'TEST') THEN
        -- Reg Order
        logs.dbg('Get Next Load for Reg Order');
        op_order_load_pk.nxt_load_for_ord_sp(i_div_part, l_r_ord.ord_num, NULL, l_llr_ts, l_load_num);

        IF (    l_load_num IS NOT NULL
            AND i_rsn_cd = l_c_skip_load) THEN
          logs.dbg('Get Next Load Following Current Found Load');
          op_order_load_pk.nxt_load_for_ord_sp(i_div_part, l_r_ord.ord_num, l_llr_ts, l_llr_ts, l_load_num);
        END IF;   -- l_load_num IS NOT NULL AND i_rsn_cd = l_c_skip_load
      ELSIF l_r_ord.dflt_load = 'DIST' THEN
        -- Dist Order
        logs.dbg('Assign to Next Avail Load to Dist Order');
        op_order_load_pk.nxt_load_for_dist_ord_sp(i_div_part, l_r_ord.ord_num, l_llr_ts, l_load_num);
      ELSE
        -- assign to dflt
        l_llr_ts := l_c_dflt_ts;
        l_load_num := l_r_ord.dflt_load;
      END IF;   -- l_r_ord.dflt_load IN('DFLT', 'TEST')

      IF l_load_num IS NULL THEN
        -- assign to dflt when next load was not found
        l_llr_ts := l_c_dflt_ts;
        l_load_num := l_r_ord.dflt_load;
      END IF;   -- l_load_num IS NULL

      IF NOT(    l_r_ord.cust_id = l_cust_id_save
             AND l_llr_ts = l_llr_ts_save
             AND l_load_num = l_load_num_save) THEN
        IF (   l_llr_ts <> l_llr_ts_save
            OR l_load_num <> l_load_num_save) THEN
          l_llr_ts_save := l_llr_ts;
          l_load_num_save := l_load_num;
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, l_llr_ts, l_load_num);
        END IF;   -- l_llr_ts <> l_llr_ts_save OR l_load_num <> l_load_num_save

        IF (   l_load_depart_sid <> l_load_depart_sid_save
            OR l_r_ord.cust_id <> l_cust_id_save) THEN
          l_load_depart_sid_save := l_load_depart_sid;
          l_cust_id_save := l_r_ord.cust_id;
          logs.dbg('Add LoadOrds');
          l_t_load_ords.EXTEND;
          l_t_load_ords(l_t_load_ords.LAST).load_depart_sid := l_load_depart_sid;
          l_t_load_ords(l_t_load_ords.LAST).cust_id := l_r_ord.cust_id;
          l_t_load_ords(l_t_load_ords.LAST).old_llr_ts := l_r_ord.llr_ts;
          l_t_load_ords(l_t_load_ords.LAST).old_load_num := l_r_ord.load_num;
          l_t_load_ords(l_t_load_ords.LAST).old_depart_ts := l_r_ord.depart_ts;
          l_t_load_ords(l_t_load_ords.LAST).old_stop_num := l_r_ord.stop_num;
          l_t_load_ords(l_t_load_ords.LAST).old_eta_ts := l_r_ord.eta_ts;

          IF l_load_num <> l_r_ord.dflt_load THEN
            IF (    l_r_ord.ord_typ = l_c_reg
                AND l_r_ord.stat_cd = l_c_stat_opn) THEN
              logs.dbg('Add AttachOrds');
              l_t_attach_ords.EXTEND;
              l_t_attach_ords(l_t_attach_ords.LAST).load_depart_sid := l_load_depart_sid;
              l_t_attach_ords(l_t_attach_ords.LAST).cust_id := l_r_ord.cust_id;
            END IF;   -- l_r_ord_load.order_type = l_c_reg
          END IF;   -- l_load_num <> l_r_ord.dflt_load
        END IF;   -- l_load_depart_sid <> l_load_depart_sid_save OR l_r_ord.cust_id <> l_cust_id_save
      END IF;   -- NOT (    l_r_ord.cust_id = l_cust_id_save

      IF l_t_load_ords(l_t_load_ords.LAST).t_ord_nums IS NULL THEN
        l_t_load_ords(l_t_load_ords.LAST).t_ord_nums := type_ntab();
      END IF;   -- l_t_load_ords(l_t_load_ords.LAST).t_ord_nums

      l_t_load_ords(l_t_load_ords.LAST).t_ord_nums.EXTEND;
      l_t_load_ords(l_t_load_ords.LAST).t_ord_nums(l_t_load_ords(l_t_load_ords.LAST).t_ord_nums.LAST) := l_r_ord.ord_num;
    END LOOP;

    IF l_t_load_ords.COUNT > 0 THEN
      logs.dbg('Final Move Orders');
      move_ords_sp;
    END IF;   -- l_t_load_ords.COUNT > 0

    IF l_t_attach_ords.COUNT > 0 THEN
      FOR i IN l_t_attach_ords.FIRST .. l_t_attach_ords.LAST LOOP
        logs.dbg('Attach Dist to Order');
        op_order_load_pk.attach_dist_ords_sp(i_div_part,
                                             l_t_attach_ords(i).load_depart_sid,
                                             l_t_attach_ords(i).cust_id,
                                             l_c_upd_userid
                                            );
        logs.dbg('Assign Default Orders');
        op_order_load_pk.assgn_dflt_ords_sp(i_div_part, l_t_attach_ords(i).cust_id, i_rsn_cd, l_c_upd_userid);
      END LOOP;
    END IF;   -- l_t_attach_ords.COUNT > 0

    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_syncload,
                                                op_process_control_pk.g_c_inactive,
                                                i_user_id,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_syncload,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END syncload_sp;
END op_order_load_pk;
/

