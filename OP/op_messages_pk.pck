CREATE OR REPLACE PACKAGE op_messages_pk IS
/*
||-----------------------------------------------------------------------------
||             C H A N G E     L O G
||-----------------------------------------------------------------------------
|| Date     | USERID  | Changes
||-----------------------------------------------------------------------------
|| 07/06/01 | rhalpai | Original
|| 08/09/01 | rhalpai | Added QOPRC10, QOPRC11, QITEM02
|| 08/23/01 | rhalpai | Added QITEM05, QITEM15
|| 08/24/01 | rhalpai | Added QITEM16, QITEM17, QITEM18, is_number function
|| 08/28/01 | rhalpai | Added QITEM01, QCUST03
|| 09/07/01 | rhalpai | Added QCUST01, QCUST07, QCUST08, QCUST09, QCUST10
|| 09/10/01 | rhalpai | Added QITEM04
|| 05/02/02 | Santosh | Removed QITEM17_SP
|| 07/23/02 | rhalpai | Change syncload_sp to take only one input parm.
|| 05/15/03 | rhalpai | Added QGOVCTL_SP
|| 07/13/05 | rhalpai | Added QMANFRP_SP, QHAZMAT_SP, QSHLFTG_SP
|| 09/15/05 | ANCHAKR | Added QDIST02_SP
|| 08/16/07 | VXRANGA | Venkat's change for qitem01_sp to include CIG_TYPE (sawp505e.cig_type_cd) -- cig type code / pack size.
||
||-----------------------------------------------------------------------------
*/
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
  /*
  || Divisional
  ||   QITEM09 : Inventory replacement (X qty)
  ||   QITEM10 : Inventory Changes (+/- qty)
  ||   QITEM13 : Slot Maintenance (A/C/D)
  ||   QITEM19 : Inventory Extract for Compare
  */
  PROCEDURE qitem09_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem10_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem13_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE invmaint_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE last_refresh_inv_sp(
    i_div       IN      VARCHAR2,
    o_ts        OUT     VARCHAR2,
    o_rstrn_sw  OUT     VARCHAR2
  );

  PROCEDURE refresh_inv_sp(
    i_div     IN  VARCHAR2,
    i_pgm_id  IN  VARCHAR2 DEFAULT 'OP_UI'
  );

  PROCEDURE qitem19_sp(
    i_div     IN  VARCHAR2,
    i_pgm_id  IN  VARCHAR2
  );

  /*
  || Corporate
  ||   QDIST02 : Distribution Order Maintenance
  ||   QCUST01 : Customer Maintenance
  ||   QCUST03 : Divisional Customer Maintenance
  ||   QCUST05 : Load Maintenance
  ||   QCUST06 : Customer Load / Stop Maintenance
  ||   QCUST07 : Customer Jurisdiction Maintenance
  ||   QCUST08 : Customer Unconditional SUBS
  ||   QCUST09 : Customer Conditional SUBS
  ||   QCUST10 : Customer Grp Rounding % Maintenance
  ||   QITEM01 : Corp Item Maintenance
  ||   QITEM02 : Divisional Item Maintenance
  ||   QITEM04 : Item Kits (Displays) Maintenance
  ||   QITEM05 : Divisional Item Subs Maintenance
  ||   QITEM15 : Divisional Item REPL Maintenance
  ||   QITEM16 : Item Stamp Jurisdiction Maintenance
  ||   QITEM17 : Divisional Item Work Rules Maintenance
  ||   QITEM18 : Divisional Item Rounding Maintenance
  ||   QOPRC10 : Tote Category Maintenance
  ||   QOPRC11 : Manifest Category Maintenance
  ||   QPICCNF : ACS Batch Pick Confirm
  ||   CWTCOMPL: Catchweight Complete for Load
  ||   QGOVCTL : Government Control Maintenance
  ||   REROUTE : Reroute Maintenance
  ||   IMQ01   : Weekly Max Qty Maintenance
  ||   BUSMOVE : Business Moves/Continuity
  ||   QCATG01 : NACS Category Maintenance
  ||   QCATG02 : Scoreboard Category Maint
  ||   REQUEST : Generic Request
  ||   QOPMSGS : Generic OP Msg Queue
  */
  PROCEDURE qdist02_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust03_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust05_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust06_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust07_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust08_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust09_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcust10_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem02_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem04_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem05_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem15_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qitem18_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qoprc10_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qoprc11_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qpiccnf_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE cwtcompl_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qgovctl_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE reroute_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE imq01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE busmove_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcatg01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qcatg02_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE request_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE itmfcst_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE qopmsgs_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE ordcut_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE strctqty_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE strctmcq_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );
END op_messages_pk;
/

CREATE OR REPLACE PACKAGE BODY op_messages_pk IS
/*
||-----------------------------------------------------------------------------
||             C H A N G E     L O G
||-----------------------------------------------------------------------------
|| Date     | USERID  | Changes
||-----------------------------------------------------------------------------
|| 07/06/01 | rhalpai | Original
|| 08/09/01 | rhalpai | Added QOPRC10, QOPRC11, QITEM02 and added ORDER BY on
||                    | MESSAGE cursors
|| 08/24/01 | rhalpai | Added g_num_str variable, is_number function
|| 09/06/01 | rhalpai | Added variables for problem_notify_sp
|| 05/02/02 | Santosh | Removed qitem17_sp procedure, deleted blocks for
||                    | update/insert/delete to mclp060A table from qitem18_sp.
||                    | Changed qitem01_sp to check for any invalid row (for
||                    | passing v_Catite and if v_action is 'CHG' type) and
||                    | delete that.
|| 08/16/07 | VXRANGA | IM327223 - Changed qitem01_sp to include CIG_TYPE
||                    | (sawp505e.cig_typ_cd) -- cig type code / pack size.
|| 03/16/11 | rhalpai | Change g_cur_inv to use zone instead of slot for lookup.
||                    | PIR0024
|| 10/25/11 | rhalpai | Removed QOPRC41_SP. PIR10475
|| 12/29/25 | rhalpai | Change cursor g_cur_msg to remove non-displayable characters from mq_msg_data. SDHD-2530142
||-----------------------------------------------------------------------------
*/
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_tt_prbs IS TABLE OF PLS_INTEGER
    INDEX BY VARCHAR2(510);

  g_c_good          CONSTANT VARCHAR2(4) := 'Good';
  g_c_opn           CONSTANT VARCHAR2(3) := 'OPN';
  g_c_wrk           CONSTANT VARCHAR2(3) := 'WRK';
  g_c_compl         CONSTANT VARCHAR2(3) := 'CMP';
  g_c_prb           CONSTANT VARCHAR2(3) := 'PRB';
  g_c_add           CONSTANT VARCHAR2(3) := 'ADD';
  g_c_chg           CONSTANT VARCHAR2(3) := 'CHG';
  g_c_del           CONSTANT VARCHAR2(3) := 'DEL';
  g_c_ref           CONSTANT VARCHAR2(3) := 'REF';
  g_c_trg           CONSTANT VARCHAR2(3) := 'TRG';
  g_c_msg_data_len  CONSTANT NUMBER      := 750;

  CURSOR g_cur_msg(
    b_div_part   NUMBER,
    b_mq_msg_id  VARCHAR2
  ) IS
    -- replace non-displayable chars in mq_msg_data with ?
    SELECT   g.mq_get_id, REGEXP_REPLACE(g.mq_msg_data,'[^ -~]','?') AS mq_msg_data
        FROM mclane_mq_get g
       WHERE g.div_part = b_div_part
         AND g.mq_msg_id = b_mq_msg_id
         AND g.mq_msg_status = 'OPN'
    ORDER BY g.mq_get_id;

  CURSOR g_cur_inv(
    b_div_part   NUMBER,
    b_item_num   VARCHAR2,
    b_uom        VARCHAR2,
    b_whse_zone  VARCHAR2
  ) IS
    SELECT     w.qalc, w.qavc, w.zonec
          FROM whsp300c w
         WHERE w.div_part = b_div_part
           AND w.itemc = b_item_num
           AND w.uomc = b_uom
           AND NVL(w.taxjrc, ' ') = NVL(b_whse_zone, ' ')
    FOR UPDATE;

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || STRING_TO_NUM_FN
  ||  Converts string to number and returns NULL when there is an invalid value.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/27/05 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION string_to_num_fn(
    i_str  IN  VARCHAR2
  )
    RETURN NUMBER IS
  BEGIN
    RETURN(TO_NUMBER(TRIM(i_str)));
  EXCEPTION
    WHEN VALUE_ERROR THEN
      RETURN(NULL);
  END string_to_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || STRING_TO_DATE_FN
  ||  Converts string to date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION string_to_date_fn(
    i_str  IN  VARCHAR2,
    i_fmt  IN  VARCHAR2 DEFAULT 'YYYYMMDD'
  )
    RETURN DATE IS
  BEGIN
    RETURN(TO_DATE(TRIM(i_str), i_fmt));
  END string_to_date_fn;

  /*
  ||----------------------------------------------------------------------------
  || CATLG_NUM_FN
  ||  Return catalog item for CBR Item/UOM
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/28/09 | rhalpai | Original - created for PIR4342
  ||----------------------------------------------------------------------------
  */
  FUNCTION catlg_num_fn(
    i_cbr_item  IN  VARCHAR2,
    i_uom       IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_catlg_num  sawp505e.catite%TYPE;
  BEGIN
    BEGIN
      l_catlg_num := op_item_pk.catlg_num_str_fn(i_cbr_item, i_uom);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;

    RETURN(l_catlg_num);
  END catlg_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_RLSE_INIT_FN
  ||  Indicate whether SetRelease has just been initiated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/17/12 | rhalpai | Created for PIR10475
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_rlse_init_fn(
    i_div_part  IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_rlse_init_sw  VARCHAR2(1);
  BEGIN
    BEGIN
      SELECT 'Y'
        INTO l_rlse_init_sw
        FROM rlse_op1z r
       WHERE r.div_part = i_div_part
         AND r.rlse_ts = (SELECT MAX(r2.rlse_ts)
                            FROM rlse_op1z r2
                           WHERE r2.div_part = i_div_part)
         AND r.stat_cd = 'P'
         AND NOT EXISTS(SELECT 1
                          FROM rlse_log_op2z rl
                         WHERE rl.div_part = r.div_part
                           AND rl.rlse_id = r.rlse_id
                           AND rl.typ_id = 'BEGALC');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_rlse_init_sw := 'N';
    END;

    RETURN(l_rlse_init_sw);
  END is_rlse_init_fn;

  PROCEDURE ins_cust_item_sid_sp(
    i_div_part       IN      NUMBER,
    i_cust_id        IN      VARCHAR2,
    i_catlg_num      IN      NUMBER,
    o_cust_item_sid  OUT     NUMBER
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO wkly_max_cust_item_op1m
                (cust_item_sid, div_part, cust_id, catlg_num, pick_qty
                )
         VALUES (cust_item_sid_seq.NEXTVAL, i_div_part, i_cust_id, i_catlg_num, 0
                )
      RETURNING cust_item_sid
           INTO o_cust_item_sid;

    COMMIT;
  END ins_cust_item_sid_sp;

  /*
  ||----------------------------------------------------------------------------
  || CUST_ITEM_SID_FN
  ||  Get existing or create new CustItemSid.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/12 | rhalpai | Original for PIR10651
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_item_sid_fn(
    i_div_part   IN  NUMBER,
    i_cust_id    IN  VARCHAR2,
    i_catlg_num  IN  NUMBER
  )
    RETURN NUMBER IS
    l_cust_item_sid  NUMBER;
  BEGIN
    BEGIN
      SELECT ci.cust_item_sid
        INTO l_cust_item_sid
        FROM wkly_max_cust_item_op1m ci
       WHERE ci.div_part = i_div_part
         AND ci.cust_id = i_cust_id
         AND ci.catlg_num = i_catlg_num
         AND ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        ins_cust_item_sid_sp(i_div_part, i_cust_id, i_catlg_num, l_cust_item_sid);
    END;

    RETURN(l_cust_item_sid);
  END cust_item_sid_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_REF_MSG_FN
  ||  Indicates whether any QOPMSGS msgs for MsgId in WRK status has REF as the
  ||  change code.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/21/10 | rhalpai | Original for IM606867
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_ref_msg_fn(
    i_div_part  IN  NUMBER,
    i_msg_id    IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_ref_sw  VARCHAR2(1);
  BEGIN
    BEGIN
      SELECT 'Y'
        INTO l_ref_sw
        FROM mclane_mq_get g
       WHERE g.div_part = i_div_part
         AND g.mq_msg_id = 'QOPMSGS'
         AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = i_msg_id
         AND g.mq_msg_status = 'WRK'
         AND UPPER(SUBSTR(g.mq_msg_data, 41, 3)) = 'REF'
         AND ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_ref_sw := 'N';
    END;

    RETURN(l_ref_sw);
  END is_ref_msg_fn;

  /*
  ||----------------------------------------------------------------------------
  || ADD_EVNT_SP
  ||  Set parameters and initiate event for processing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/17 | rhalpai | Move event logic to common module and call new
  ||                    | CIG_EVENT_MGR_PK.CREATE_INSTANCE. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_evnt_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_evnt_dfn_id  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.ADD_EVNT_SP';
    lar_parm             logs.tar_parm;
    l_org_id             NUMBER;
    l_evnt_parms         CLOB;
    l_evnt_que_id        NUMBER;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntDfnId', i_evnt_dfn_id);
    logs.dbg('Initialize');
    l_org_id :=(CASE i_div
                  WHEN 'MC' THEN 1   -- because our CMS guy refuses to be a team player :)
                  ELSE cig_organization_pk.get_div_id(i_div)
                END
               );
    l_evnt_parms := '<parameters>'
                    || '<row><sequence>'
                    || 1
                    || '</sequence><value>'
                    || i_div
                    || '</value></row>'
                    || '<row><sequence>'
                    || 2
                    || '</sequence><value>'
                    || i_user_id
                    || '</value></row>'
                    || '</parameters>';
    logs.dbg('Create Event');
    cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                     i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                     i_event_dfn_id         => i_evnt_dfn_id,
                                     i_parameters           => l_evnt_parms,
                                     i_div_nm               => i_div,
                                     i_is_script_fw_exec    => 'N',
                                     i_is_complete          => 'Y',
                                     i_pgm_id               => 'PLSQL',
                                     i_user_id              => i_user_id,
                                     o_event_que_id         => l_evnt_que_id
                                    );
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_evnt_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_EVNT_LOG_SP
  ||  Update the event log
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/25/11 | rhalpai | Original for PIR10475
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_evnt_log_sp(
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER,
    i_evnt_msg     IN  VARCHAR2,
    i_finish_cd    IN  NUMBER DEFAULT 0
  ) IS
  BEGIN
    cig_event_mgr_pk.update_log_message(i_evnt_que_id,
                                        i_cycl_id,
                                        i_cycl_dfn_id,
                                        SUBSTR(i_evnt_msg, 1, 512),
                                        i_finish_cd
                                       );
  END upd_evnt_log_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_MSG_STATUS_SP
  ||  Updates the MQ Message Status and Last Changed Timestamp on MCLANE_MQ_GET.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/27/05 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_msg_status_sp(
    i_div_part   IN  NUMBER,
    i_mq_get_id  IN  NUMBER,
    i_msg_stat   IN  VARCHAR2
  ) IS
    l_c_sysdate  CONSTANT DATE := SYSDATE;
  BEGIN
    UPDATE mclane_mq_get
       SET mq_msg_status = i_msg_stat,
           last_chg_ts = l_c_sysdate
     WHERE div_part = i_div_part
       AND mq_get_id = i_mq_get_id
       AND mq_msg_status <> i_msg_stat;
  END upd_msg_status_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_STAT_SP
  ||  Update order hdr/dtl status and make log entry.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/08/07 | rhalpai | Original
  || 08/19/08 | rhalpai | Changed to correctly log original order status and
  ||                    | new order status. PIR6364
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_stat_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_new_stat  IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_user_id   IN  VARCHAR2,
    i_rsn_txt   IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_MESSAGES_PK.UPD_ORD_STAT_SP';
    lar_parm             logs.tar_parm;
    l_r_ord_hdr          ordp100a%ROWTYPE;
    l_r_sysp296a         sysp296a%ROWTYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'NewStat', i_new_stat);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'RsnTxt', i_rsn_txt);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get OrdHdr');
    l_r_ord_hdr := op_ord_hdr_pk.sel_fn(i_div_part, i_ord_num);

    IF (    l_r_ord_hdr.stata IN('O', 'I', 'S')
        AND l_r_ord_hdr.stata <> i_new_stat) THEN
      logs.dbg('Change Status on Ord Hdr');

      UPDATE ordp100a
         SET stata = i_new_stat
       WHERE div_part = i_div_part
         AND ordnoa = i_ord_num
         AND stata <> i_new_stat;

      logs.dbg('Change Status on Ord Dtl');

      UPDATE ordp120b
         SET statb = i_new_stat
       WHERE div_part = i_div_part
         AND ordnob = i_ord_num
         AND statb <> 'C'
         AND statb <> i_new_stat;

      logs.dbg('Log Status Change');
      l_r_sysp296a.div_part := i_div_part;
      l_r_sysp296a.ordnoa := i_ord_num;
      l_r_sysp296a.linea := 0;
      l_r_sysp296a.rsncda := i_rsn_cd;
      l_r_sysp296a.tblnma := 'ORDP100A';
      l_r_sysp296a.fldnma := 'STATA';
      l_r_sysp296a.florga := l_r_ord_hdr.stata;
      l_r_sysp296a.flchga := i_new_stat;
      l_r_sysp296a.usera := i_user_id;
      l_r_sysp296a.rsntxa := i_rsn_txt;
      op_sysp296a_pk.ins_sp(l_r_sysp296a);
    END IF;   -- l_r_ord_hdr.stata IN('O', 'I', 'S') AND l_r_ord_hdr.stata <> i_new_stat

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_INV_TRAN_SP
  ||  Log inventory quantity change/replacement transaction.
  ||  QITEM09 - Qty Replacement (= qty)
  ||  QITEM10 - Qty Change (+/- qty)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/06/07 | rhalpai | Original - Created for IM278669
  || 08/29/11 | rhalpai | Rename from INS_WHSP900R_SP and convert to use new
  ||                    | transaction tables. PIR7990
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_inv_tran_sp(
    i_div_part  IN  NUMBER,
    i_item_num  IN  VARCHAR2,
    i_uom       IN  VARCHAR2,
    i_zone      IN  VARCHAR2,
    i_aisl      IN  VARCHAR2,
    i_bin       IN  VARCHAR2,
    i_levl      IN  VARCHAR2,
    i_qty       IN  NUMBER,
    i_pgm_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.INS_INV_TRAN_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'ItemNum', i_item_num);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'Zone', i_zone);
    logs.add_parm(lar_parm, 'Aisl', i_aisl);
    logs.add_parm(lar_parm, 'Bin', i_bin);
    logs.add_parm(lar_parm, 'Level', i_levl);
    logs.add_parm(lar_parm, 'Qty', i_qty);
    logs.add_parm(lar_parm, 'PgmId', i_pgm_id);
    logs.dbg('ENTRY', lar_parm);

    INSERT INTO tran_op2t
                (div_part, tran_id, rlse_id, tran_typ, create_ts, pgm_id
                )
         VALUES (i_div_part, op1a_tran_id_seq.NEXTVAL, -1, 04, l_c_sysdate, i_pgm_id
                );

    INSERT INTO tran_item_op2i
                (div_part, tran_id, catlg_num, inv_zone, inv_aisle, inv_bin, inv_lvl, pick_zone, pick_aisle, pick_bin,
                 pick_lvl, qty)
      SELECT i_div_part, op1a_tran_id_seq.CURRVAL, e.catite, NVL(i_zone, '~'), i_aisl, i_bin, i_levl, NVL(i_zone, '~'),
             i_aisl, i_bin, i_levl, i_qty
        FROM sawp505e e
       WHERE e.iteme = i_item_num
         AND e.uome = i_uom;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_inv_tran_sp;

  /*
  ||----------------------------------------------------------------------------
  || ADD_PROBLEM_SP
  ||  Log problem and append to probs table for email notification.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/06/07 | rhalpai | Original - Created for IM278669
  || 05/03/12 | rhalpai | Add INVMAINT to inventory_sync_problem.log PIR11057
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_problem_sp(
    io_t_prbs  IN OUT NOCOPY  g_tt_prbs,
    i_div      IN             VARCHAR2,
    i_msg_id   IN             VARCHAR2,
    i_prb_hdr  IN             VARCHAR2,
    i_prb_dtl  IN             VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm  := 'OP_MESSAGES_PK.ADD_PROBLEM_SP';
    lar_parm                  logs.tar_parm;
    l_c_sysdate      CONSTANT DATE           := SYSDATE;
    l_idx                     VARCHAR2(510);
    l_c_mq_err_log   CONSTANT VARCHAR2(50)   := i_div || '_mqerrors_mclane.log';
    l_c_inv_err_log  CONSTANT VARCHAR2(50)   := i_div || '_inventory_sync_problem.log';
    l_c_file_dir     CONSTANT VARCHAR2(50)   := '/oplogs/interfaces';
    l_file_nm                 VARCHAR2(50);
    l_t_rpt_lns               typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'PrbHdr', i_prb_hdr);
    logs.add_parm(lar_parm, 'PrbDtl', i_prb_dtl);
    logs.dbg('ENTRY', lar_parm);
    l_idx := RPAD(i_div, 2) || RPAD(i_msg_id, 8) || i_prb_hdr;
    logs.dbg('Update Problem Table');
    io_t_prbs(l_idx) :=(CASE
                          WHEN io_t_prbs.EXISTS(l_idx) THEN io_t_prbs(l_idx) + 1
                          ELSE 1
                        END);
    logs.dbg('Set File Name');
    l_file_nm :=(CASE
                   WHEN i_msg_id = 'INVMAINT' THEN l_c_inv_err_log
                   WHEN i_msg_id LIKE 'QITEM%' THEN l_c_inv_err_log
                   ELSE l_c_mq_err_log
                 END
                );
    logs.dbg('Populate Report Line Table');
    util.append(l_t_rpt_lns, TO_CHAR(l_c_sysdate, 'YYYY-MM-DD HH24:MI:SS') || ' ' || i_msg_id || ' ' || i_prb_hdr);

    IF i_prb_dtl IS NOT NULL THEN
      util.append(l_t_rpt_lns, RPAD('-', 21 + LENGTH(i_msg_id)) || i_prb_dtl);
    END IF;   -- i_prb_dtl IS NOT NULL

    util.append(l_t_rpt_lns, '');
    logs.dbg('Write Error Log');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir, 'A');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_problem_sp;

  /*
  ||----------------------------------------------------------------------------
  || NOTIFY_SP
  ||  Email notification of problems.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/06/07 | rhalpai | Original - Created for IM278669
  ||----------------------------------------------------------------------------
  */
  PROCEDURE notify_sp(
    i_t_prbs  IN  g_tt_prbs
  ) IS
    l_c_init_val   CONSTANT VARCHAR2(1)    := '?';
    l_c_prcs_id    CONSTANT VARCHAR2(20)   := 'OP_MESSAGES_PK';
    l_c_mail_subj  CONSTANT VARCHAR2(50)   := 'Errors From OP MQ Interface - OP_MESSAGES_PK';
    l_mail_msg              VARCHAR2(4000);
    l_idx                   VARCHAR2(510);
    l_div                   VARCHAR2(2)    := l_c_init_val;
    l_div_save              VARCHAR2(2);
    l_msg_id                VARCHAR2(10);
    l_msg_id_save           VARCHAR2(10)   := l_c_init_val;
    l_prb                   VARCHAR2(500);
    l_prb_cnt               PLS_INTEGER;
  BEGIN
    IF i_t_prbs.COUNT > 0 THEN
      l_idx := i_t_prbs.FIRST;
      LOOP
        EXIT WHEN NOT i_t_prbs.EXISTS(l_idx);
        l_div := LTRIM(SUBSTR(l_idx, 1, 2));
        l_msg_id := LTRIM(SUBSTR(l_idx, 3, 8));
        l_prb := LTRIM(SUBSTR(l_idx, 11));
        l_prb_cnt := i_t_prbs(l_idx);

        IF l_div <> l_div_save THEN
          IF l_div_save <> l_c_init_val THEN
            op_process_common_pk.notify_group_sp(l_div, l_c_prcs_id, l_c_mail_subj, l_mail_msg);
          END IF;   -- l_div_save <> l_c_inital_value

          l_div_save := l_div;
        END IF;   -- l_div <> l_div_save

        IF l_msg_id <> l_msg_id_save THEN
          l_msg_id_save := l_msg_id;
          l_mail_msg := '\n' || l_mail_msg || l_msg_id || '\n';
          l_mail_msg := l_mail_msg || RPAD('*', LENGTH(l_msg_id), '*') || '\n';
        END IF;   -- l_msg_id <> l_msg_id_save

        l_mail_msg := l_mail_msg || LPAD(l_prb_cnt, 4) || ' - ' || l_prb || '\n';
        l_idx := i_t_prbs.NEXT(l_idx);
      END LOOP;
      op_process_common_pk.notify_group_sp(l_div, l_c_prcs_id, l_c_mail_subj, l_mail_msg);
    END IF;   -- i_t_prbs.COUNT > 0
  END notify_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_INV_SP
  ||  Add/Chg inventory table WHSP300C.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/17/12 | rhalpai | Created for PIR10475
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_inv_sp(
    i_div_part   IN  NUMBER,
    i_item_num   IN  VARCHAR2,
    i_uom        IN  VARCHAR2,
    i_whse_zone  IN  VARCHAR2,
    i_aisl       IN  VARCHAR2,
    i_bin        IN  VARCHAR2,
    i_levl       IN  VARCHAR2,
    i_qty        IN  NUMBER DEFAULT NULL
  ) IS
  BEGIN
    MERGE INTO whsp300c w
         USING (SELECT d.div_id
                  FROM div_mstr_di1d d
                 WHERE d.div_part = i_div_part) x
            ON (    w.div_part = i_div_part
                AND w.itemc = i_item_num
                AND w.uomc = i_uom
                AND w.zonec = x.div_id
                AND NVL(w.taxjrc, ' ') = NVL(i_whse_zone, ' '))
      WHEN MATCHED THEN
        UPDATE
           SET w.qohc = NVL(i_qty, w.qohc) + w.qalc, w.qavc = NVL(i_qty, w.qavc), w.aislc = i_aisl, w.binc = i_bin,
               w.levlc = i_levl
      WHEN NOT MATCHED THEN
        INSERT(div_part, itemc, zonec, aislc, binc, levlc, qohc, qalc, qavc, uomc, taxjrc)
        VALUES(i_div_part, i_item_num, x.div_id, i_aisl, i_bin, i_levl, NVL(i_qty, 0), 0, NVL(i_qty, 0), i_uom,
               i_whse_zone);
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
      NULL;
  END merge_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_MQ_PUT_SP
  ||  Create MQ PUT msgs
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/10 | rhalpai | Original for PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_mq_put_sp(
    i_div_part  IN  NUMBER,
    i_msg_id    IN  VARCHAR2,
    i_msg_data  IN  VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO mclane_mq_put
                (mq_msg_id, div_part, mq_msg_data, mq_msg_status
                )
         VALUES (i_msg_id, i_div_part, i_msg_data, 'OPN'
                );

    COMMIT;
  END ins_mq_put_sp;

  /*
  ||----------------------------------------------------------------------------
  || MQ_PUT_SP
  ||  Put msgs to MQ
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/11/10 | rhalpai | Original for PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_put_sp(
    i_div      IN  VARCHAR2,
    i_msg_id   IN  VARCHAR2,
    i_corr_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.MQ_PUT_SP';
    lar_parm             logs.tar_parm;
    l_rc                 NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'CorrId', i_corr_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Put Msgs to MQ');
    op_mq_message_pk.mq_put_sp(i_msg_id, i_div, i_corr_id, l_rc);
    excp.assert((l_rc = 0), 'Failed to put msgs to MQ');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mq_put_sp;

  /*
  ||----------------------------------------------------------------------------
  || OVRRD_ORD_STATS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/19/08 | rhalpai | Created from logic moved from QCUST01_SP. PIR6364
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ovrrd_ord_stats_sp(
    i_div_part  IN  NUMBER,
    i_cust_id   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                  := 'OP_MESSAGES_PK.OVRRD_ORD_STATS_SP';
    lar_parm             logs.tar_parm;
    l_ord_stat_ovrrd     mclp140a.ord_stat_ovrrd%TYPE;
    l_c_cancl   CONSTANT VARCHAR2(1)                    := 'C';
    l_c_suspnd  CONSTANT VARCHAR2(1)                    := 'S';
    l_t_sync_ords        type_ntab                      := type_ntab();

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_cust_id   VARCHAR2,
      b_stat_cd   VARCHAR2
    ) IS
      SELECT a.ordnoa AS ord_num, a.dsorda AS ord_typ, a.ldtypa AS load_typ
        FROM ordp100a a, load_depart_op1f ld
       WHERE a.div_part = b_div_part
         AND a.custa = b_cust_id
         AND a.stata IN('O', 'I', 'S')
         AND a.stata <> b_stat_cd
         AND (   a.dsorda IN('R', 'N', 'T')
              OR (    ld.load_num NOT IN('DIST', 'DFLT')
                  AND ld.load_num NOT LIKE 'P__P'))
         AND ld.div_part = a.div_part
         AND ld.load_depart_sid = a.load_depart_sid;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get Override Status Parm');

    SELECT a.ord_stat_ovrrd
      INTO l_ord_stat_ovrrd
      FROM mclp140a a
     WHERE a.rsncda = i_rsn_cd;

    FOR l_r_ord IN l_cur_ords(i_div_part, i_cust_id, l_ord_stat_ovrrd) LOOP
      IF l_r_ord.ord_typ = 'D' THEN
        -- Distribution Orders
        -- <<Suspend Special Dist>>
        IF (    SUBSTR(l_r_ord.load_typ, 1, 1) = 'P'
            AND l_ord_stat_ovrrd = l_c_suspnd) THEN
          logs.dbg('Suspend Special Distribution Order');
          upd_ord_stat_sp(i_div_part, l_r_ord.ord_num, l_ord_stat_ovrrd, i_rsn_cd, i_user_id);
        ELSE
          logs.dbg('Add Syncload Order');
          l_t_sync_ords.EXTEND;
          l_t_sync_ords(l_t_sync_ords.LAST) := l_r_ord.ord_num;
        END IF;   -- <<Suspend Special Dist>>
      ELSE
        -- Regular Orders
        logs.dbg('Update Order Status (Suspend/Cancel) for Reg Order');
        upd_ord_stat_sp(i_div_part, l_r_ord.ord_num, l_ord_stat_ovrrd, i_rsn_cd, i_user_id);

        IF l_ord_stat_ovrrd = l_c_cancl THEN
          logs.dbg('Move Cancelled Reg Order to History');
          op_cleanup_pk.move_order_to_hist_sp(i_div_part, l_r_ord.ord_num);
        END IF;   -- l_ord_stat_ovrrd = l_c_cancl
      END IF;   -- l_r_ord.ord_typ = 'D'

      COMMIT;
    END LOOP;

    IF l_t_sync_ords.COUNT > 0 THEN
      logs.dbg('Syncload for Dist Orders');
      op_order_load_pk.syncload_sp(i_div_part, i_rsn_cd, l_t_sync_ords);
      COMMIT;
    END IF;   -- l_t_sync_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ovrrd_ord_stats_sp;

  /*
  ||----------------------------------------------------------------------------
  || UNSUSPND_ORDS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/19/08 | rhalpai | Created from logic moved from QCUST01_SP.
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 04/01/14 | rhalpai | Change logic to include all distributions for cust
  ||                    | regardless of transmit date. PIR13690
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to
  ||                    | OP_REPRICE_PK.REPRICE_ORD_LN_SP.
  || 06/03/20 | rhalpai | Change logic to call Reprice in batch mode. SDHD-714711
  ||----------------------------------------------------------------------------
  */
  PROCEDURE unsuspnd_ords_sp(
    i_div_part  IN  NUMBER,
    i_cust_id   IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.UNSUSPND_ORDS_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_unsuspnd_days       PLS_INTEGER;
    l_trnsmt_deadline     NUMBER;
    l_t_ords              type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_unsuspnd_days := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_unsuspend_days);
    l_trnsmt_deadline := TRUNC(l_c_sysdate) - DATE '1900-02-28' - l_unsuspnd_days;
    logs.dbg('Get Orders');

    SELECT a.ordnoa
    BULK COLLECT INTO l_t_ords
      FROM ordp100a a
     WHERE a.div_part = i_div_part
       AND a.custa = i_cust_id
       AND (   a.dsorda = 'D'
            OR a.trndta >= l_trnsmt_deadline)
       AND a.stata = 'S';

    IF l_t_ords.COUNT > 0 THEN
      logs.dbg('Unsuspend and Reprice Orders');
      FOR i IN l_t_ords.FIRST .. l_t_ords.LAST LOOP
        logs.dbg('Unsuspend Order');
        upd_ord_stat_sp(i_div_part, l_t_ords(i), 'O', 'UNSUSPEND', i_user_id);
      END LOOP;
      COMMIT;
      logs.dbg('Reprice Cust Orders');
      op_reprice_pk.reprice_bulk_sp(div_pk.div_id_fn(i_div_part),
                                    op_reprice_pk.g_c_cust,
                                    op_reprice_pk.g_c_real_time,
                                    '1900-01-01~2999-12-31~' || csr_customers_pk.mcl_cust_fn(i_div_part, i_cust_id)
                                   );
    END IF;   -- l_t_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END unsuspnd_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || REASSIGN_CUST_ORDS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/19/08 | rhalpai | Original.
  || 05/02/11 | rhalpai | Changed cursor to ignore any order matching
  ||                    | LLRDate/Cust/Load/Stop entries from new override
  ||                    | table before processing Syncload. PIR9348
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. Convert to call new
  ||                    | SYNCLOAD_SP. PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reassign_cust_ords_sp(
    i_div_part      IN  NUMBER,
    i_cust_id       IN  VARCHAR2,
    i_rsn_cd        IN  VARCHAR2,
    i_load_num      IN  VARCHAR2 DEFAULT NULL,
    i_incl_dflt_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.REASSIGN_CUST_ORDS_SP';
    lar_parm             logs.tar_parm;
    l_load_depart_sid    NUMBER;
    l_t_ords             type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.add_parm(lar_parm, 'InclDfltSw', i_incl_dflt_sw);
    logs.info('ENTRY', lar_parm);

    IF i_load_num IS NOT NULL THEN
      logs.dbg('Get DFLT LoadDepartSid');
      l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, DATE '1900-01-01', 'DFLT');
      logs.dbg('Upd Special Dist to DFLT Load to Treat as Reg Orders');

      UPDATE    ordp100a a
            SET a.load_depart_sid = l_load_depart_sid
          WHERE a.div_part = i_div_part
            AND a.custa = i_cust_id
            AND a.stata IN('O', 'S')
            AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                       FROM load_depart_op1f ld, stop_eta_op1g se
                                      WHERE ld.div_part = i_div_part
                                        AND ld.load_num = i_load_num
                                        AND se.div_part = ld.div_part
                                        AND se.load_depart_sid = ld.load_depart_sid
                                        AND se.cust_id = i_cust_id)
      RETURNING         a.ordnoa
      BULK COLLECT INTO l_t_ords;

      IF l_t_ords.COUNT > 0 THEN
        logs.dbg('Merge StopEta');
        op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, i_cust_id);
        logs.dbg('Move Special Dist Orders to Cust Next Available Load');
        op_order_load_pk.syncload_sp(i_div_part, i_rsn_cd, l_t_ords);
        l_t_ords := NULL;
      END IF;   -- l_t_ords.COUNT > 0
    END IF;   -- i_load_num IS NOT NULL

    logs.dbg('Get Orders');

    SELECT a.ordnoa
    BULK COLLECT INTO l_t_ords
      FROM mclp120c l, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
     WHERE l.div_part = i_div_part
       AND l.test_bil_load_sw = 'N'
       AND a.div_part = l.div_part
       AND a.stata IN('O', 'S')
       AND a.custa = i_cust_id
       AND ld.div_part = a.div_part
       AND ld.load_depart_sid = a.load_depart_sid
       AND ld.load_num = l.loadc
       AND (   ld.load_num = i_load_num
            OR (    i_load_num IS NULL
                AND (   ld.load_num = DECODE(i_incl_dflt_sw, 'Y', 'DFLT')
                     OR ld.load_num IN(SELECT d.loadd
                                         FROM mclp040d d
                                        WHERE d.div_part = i_div_part
                                          AND d.custd = i_cust_id)
                    )
               )
           )
       AND se.div_part = ld.div_part
       AND se.load_depart_sid = ld.load_depart_sid
       AND se.cust_id = i_cust_id
       -- bypass ords on cust route overrides
       AND NOT EXISTS(SELECT 1
                        FROM cust_rte_ovrrd_rt3c cro
                       WHERE cro.div_part = ld.div_part
                         AND cro.cust_id = i_cust_id
                         AND cro.llr_dt = ld.llr_dt
                         AND cro.load_num = ld.load_num
                         AND cro.stop_num = se.stop_num);

    IF l_t_ords.COUNT > 0 THEN
      logs.dbg('Reassign Orders to Cust Next Available Load');
      op_order_load_pk.syncload_sp(i_div_part, i_rsn_cd, l_t_ords);
    END IF;   -- l_t_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reassign_cust_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORDS_FOR_CUST_STAT_CHG_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/19/08 | rhalpai | Created from logic moved from QCUST01_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ords_for_cust_stat_chg_sp(
    i_actn_cd        IN  VARCHAR2,
    i_div_part       IN  NUMBER,
    i_cust_id        IN  VARCHAR2,
    i_cust_old_stat  IN  VARCHAR2,
    i_cust_new_stat  IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  ) IS
    l_c_module         CONSTANT typ.t_maxfqnm          := 'OP_MESSAGES_PK.UPD_ORDS_FOR_CUST_STAT_CHG_SP';
    lar_parm                    logs.tar_parm;
    l_c_act            CONSTANT VARCHAR2(1)            := '1';
    l_c_inact          CONSTANT VARCHAR2(1)            := '2';
    l_c_hold           CONSTANT VARCHAR2(1)            := '3';
    l_c_cust_hold_cd   CONSTANT VARCHAR2(3)            := '007';
    l_c_cust_inact_cd  CONSTANT VARCHAR2(3)            := '008';
    l_rsn_cd                    mclp140a.rsncda%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'CustOldStat', i_cust_old_stat);
    logs.add_parm(lar_parm, 'CustNewStat', i_cust_new_stat);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);

    ----------------------------------------------------------------------------
    -- If customer is being deleted, inactivated or placed on hold then regular
    -- orders for that customer will be cancelled or suspended depending on the
    -- override status set on MCLP140A.  Cancelled regular order will be moved
    -- to history.  Assigned special distribution orders (forced P00's) for
    -- customer with override status of suspend will be suspended.  All other
    -- distribution orders will be reset to their respective default loads.
    --
    -- If customer is being re-activated or removed from hold status then
    -- previously suspended orders will be reset and reassigned to their next
    -- available load.  Distribution orders will attach as necessary.
    ----------------------------------------------------------------------------
    CASE
      WHEN(   i_actn_cd = g_c_del
           OR i_cust_new_stat IN(l_c_inact, l_c_hold)) THEN
        logs.dbg('Override Order Statuses');
        l_rsn_cd :=(CASE
                      WHEN i_cust_new_stat = l_c_hold THEN l_c_cust_hold_cd
                      ELSE l_c_cust_inact_cd
                    END);
        ovrrd_ord_stats_sp(i_div_part, i_cust_id, l_rsn_cd, i_user_id);
      WHEN(    i_cust_new_stat = l_c_act
           AND i_cust_old_stat IN(l_c_hold, l_c_inact)) THEN
        logs.dbg('Unsuspend Orders');
        unsuspnd_ords_sp(i_div_part, i_cust_id, i_user_id);
        logs.dbg('Reassign Orders for Customer to Next Available Load');
        l_rsn_cd := 'UNSUSPND';
        reassign_cust_ords_sp(i_div_part, i_cust_id, l_rsn_cd);
      ELSE
        NULL;
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ords_for_cust_stat_chg_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_MSG_STATS_SP
  ||  Set MQ msg status of QOPMSGS for MsgId
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/22/10 | rhalpai | Original - created for PIR8216
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_msg_stats_sp(
    i_div_part      IN      NUMBER,
    i_msg_id        IN      VARCHAR2,
    i_old_stat      IN      VARCHAR2,
    i_new_stat      IN      VARCHAR2,
    o_is_msg_found  OUT     BOOLEAN
  ) IS
    l_c_sysdate  CONSTANT DATE := SYSDATE;
  BEGIN
    UPDATE mclane_mq_get g
       SET g.mq_msg_status = i_new_stat,
           g.last_chg_ts = l_c_sysdate
     WHERE g.div_part = i_div_part
       AND g.mq_msg_id = 'QOPMSGS'
       AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = i_msg_id
       AND g.mq_msg_status = i_old_stat;

    o_is_msg_found :=(SQL%ROWCOUNT > 0);
    COMMIT;
  END upd_msg_stats_sp;

  /*
  ||----------------------------------------------------------------------------
  || QPINV01_SP
  ||  Maps messages that describe Protected Inventory entities.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/06/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qpinv01_sp(
    i_div_part     IN      NUMBER,
    i_mq_msg_data  IN      VARCHAR2,
    o_status       OUT     VARCHAR2
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.QPINV01_SP';
    lar_parm                   logs.tar_parm;
    l_div_id                   div_mstr_di1d.div_id%TYPE;
    l_actn_cd                  VARCHAR2(3);
    l_r_prtctd_inv             prtctd_inv_op1i%ROWTYPE;
    l_c_reg           CONSTANT VARCHAR2(1)                 := 'R';
    l_c_dist          CONSTANT VARCHAR2(1)                 := 'D';
    l_c_reg_and_dist  CONSTANT VARCHAR2(1)                 := 'B';

    PROCEDURE maintenance_sp(
      i_ord_typ  IN  VARCHAR2
    ) IS
      l_r_exist_prtctd_inv  prtctd_inv_op1i%ROWTYPE;
      l_prtctd_id           NUMBER;
      l_err_msg             op_protected_inventory_pk.g_err_msg%TYPE;

      PROCEDURE expiration_notify_sp IS
        l_c_process_id  CONSTANT VARCHAR2(40)  := 'OP_MESSAGES_PK.QPINV01_SP';
        l_mail_subj              VARCHAR2(100);
        l_mail_msg               VARCHAR2(450);
      BEGIN
        l_mail_subj := 'Existing Inventory Protection Expired Due To Date Overlap With New';
        l_mail_msg := 'Existing inventory protection has been expired due to overlap with new protection.\n'
                      || 'OLD\n'
                      || '---\n'
                      || 'ProtectID: '
                      || l_r_exist_prtctd_inv.prtctd_id
                      || '\n'
                      || 'Div      : '
                      || l_div_id
                      || '\n'
                      || 'Zone     : '
                      || NVL(l_r_exist_prtctd_inv.zone_id, 'NULL')
                      || '\n'
                      || 'Group    : '
                      || l_r_exist_prtctd_inv.grp_id
                      || '\n'
                      || 'Item     : '
                      || l_r_exist_prtctd_inv.ord_item_num
                      || '\n'
                      || 'OrderTyp : '
                      || l_r_exist_prtctd_inv.ord_typ_cd
                      || '\n'
                      || 'StartDt  : '
                      || TO_CHAR(l_r_exist_prtctd_inv.eff_dt, 'YYYYMMDD')
                      || '\n'
                      || 'EndDt    : '
                      || TO_CHAR(l_r_exist_prtctd_inv.end_dt, 'YYYYMMDD')
                      || '\n'
                      || '\n'
                      || 'NEW\n'
                      || '---\n'
                      || 'ProtectID: '
                      || l_prtctd_id
                      || '\n'
                      || 'Div      : '
                      || l_div_id
                      || '\n'
                      || 'Zone     : '
                      || NVL(l_r_prtctd_inv.zone_id, 'NULL')
                      || '\n'
                      || 'Group    : '
                      || l_r_prtctd_inv.grp_id
                      || '\n'
                      || 'Item     : '
                      || l_r_prtctd_inv.ord_item_num
                      || '\n'
                      || 'OrderTyp : '
                      || i_ord_typ
                      || '\n'
                      || 'StartDt  : '
                      || TO_CHAR(l_r_prtctd_inv.eff_dt, 'YYYYMMDD')
                      || '\n'
                      || 'EndDt    : '
                      || TO_CHAR(l_r_prtctd_inv.end_dt, 'YYYYMMDD')
                      || '\n';
        op_process_common_pk.notify_group_sp(l_div_id, l_c_process_id, l_mail_subj, l_mail_msg);
      END expiration_notify_sp;
    BEGIN
      logs.dbg('Get Existing Protection');
      l_r_exist_prtctd_inv := op_protected_inventory_pk.prtctd_inv_fn(l_r_prtctd_inv);
      logs.dbg('Handle Action');

      CASE
        WHEN l_actn_cd IN(g_c_add, g_c_chg) THEN
          IF l_r_exist_prtctd_inv.prtctd_id IS NULL THEN
            logs.dbg('Insert New Protection');
            op_protected_inventory_pk.ins_sp(l_div_id,
                                             l_r_prtctd_inv.grp_id,
                                             l_r_prtctd_inv.ord_item_num,
                                             l_r_prtctd_inv.prtctd_qty,
                                             i_ord_typ,
                                             l_r_prtctd_inv.eff_dt,
                                             l_r_prtctd_inv.end_dt,
                                             l_r_prtctd_inv.user_id,
                                             l_prtctd_id,
                                             o_status,
                                             l_err_msg
                                            );
          ELSE
            DECLARE
              l_systemid_sw  VARCHAR2(1);
            BEGIN
              l_systemid_sw := op_parms_pk.val_exists_for_prfx_fn(i_div_part,
                                                                  op_const_pk.prm_prtctd_inv_systemid,
                                                                  l_r_exist_prtctd_inv.create_user
                                                                 );

              IF l_systemid_sw = 'Y' THEN
                logs.dbg('Update Protection');
                op_protected_inventory_pk.upd_sp(l_div_id,
                                                 l_r_exist_prtctd_inv.prtctd_id,
                                                 l_r_prtctd_inv.grp_id,
                                                 l_r_prtctd_inv.ord_item_num,
                                                 l_r_prtctd_inv.eff_dt,
                                                 l_r_prtctd_inv.end_dt,
                                                 i_ord_typ,
                                                 l_r_prtctd_inv.zone_id,
                                                 l_r_prtctd_inv.user_id,
                                                 o_status,
                                                 l_err_msg
                                                );
              ELSE
                logs.dbg('Expire Protection Due to Date Overlap');
                op_protected_inventory_pk.expire_sp(l_div_id,
                                                    l_r_exist_prtctd_inv.prtctd_id,
                                                    l_r_prtctd_inv.user_id,
                                                    o_status,
                                                    l_err_msg
                                                   );

                IF o_status = 'Good' THEN
                  logs.dbg('Notify Group of Protection Expiration');
                  expiration_notify_sp;
                  logs.dbg('Add Replacement Protection');
                  op_protected_inventory_pk.ins_sp(l_div_id,
                                                   l_r_prtctd_inv.grp_id,
                                                   l_r_prtctd_inv.ord_item_num,
                                                   l_r_prtctd_inv.prtctd_qty,
                                                   i_ord_typ,
                                                   l_r_prtctd_inv.eff_dt,
                                                   l_r_prtctd_inv.end_dt,
                                                   l_r_prtctd_inv.user_id,
                                                   l_prtctd_id,
                                                   o_status,
                                                   l_err_msg
                                                  );
                END IF;   -- o_status = 'Good'
              END IF;   -- l_systemid_sw = 'Y'
            END;
          END IF;   -- l_r_exist_prtctd_inv.prtctd_id IS NULL
        WHEN l_actn_cd = g_c_del THEN
          logs.dbg('Expire Protection');
          op_protected_inventory_pk.expire_sp(l_div_id,
                                              l_r_exist_prtctd_inv.prtctd_id,
                                              l_r_prtctd_inv.user_id,
                                              o_status,
                                              l_err_msg
                                             );
        ELSE
          NULL;
      END CASE;

      IF o_status = 'Error' THEN
        o_status := l_err_msg;
      END IF;
    END maintenance_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qpinv01,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    o_status := 'Good';
    logs.dbg('Parse MQ Msg Data');
    l_div_id := SUBSTR(i_mq_msg_data, 1, 2);
    l_actn_cd := SUBSTR(i_mq_msg_data, 41, 3);
    l_r_prtctd_inv.user_id := RTRIM(SUBSTR(i_mq_msg_data, 54, 8));
    l_r_prtctd_inv.grp_id := l_div_id || SUBSTR(i_mq_msg_data, 62, 3);
    l_r_prtctd_inv.tax_jrsdctn := RTRIM(SUBSTR(i_mq_msg_data, 65, 2));
    l_r_prtctd_inv.ord_item_num := SUBSTR(i_mq_msg_data, 67, 6);
    l_r_prtctd_inv.eff_dt := TO_DATE(SUBSTR(i_mq_msg_data, 73, 10), 'YYYY-MM-DD');
    l_r_prtctd_inv.end_dt := TO_DATE(SUBSTR(i_mq_msg_data, 83, 10), 'YYYY-MM-DD');
    l_r_prtctd_inv.ord_typ_cd := SUBSTR(i_mq_msg_data, 93, 1);
    l_r_prtctd_inv.prtctd_qty := SUBSTR(i_mq_msg_data, 94, 9);
    -- set zone to div
    l_r_prtctd_inv.zone_id := l_div_id;
    l_r_prtctd_inv.div_part := i_div_part;
    logs.dbg('Maintenance for Order Type');

    IF l_r_prtctd_inv.ord_typ_cd = l_c_reg_and_dist THEN
      maintenance_sp(l_c_reg);
      maintenance_sp(l_c_dist);
    ELSE
      maintenance_sp(l_r_prtctd_inv.ord_typ_cd);
    END IF;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qpinv01,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qpinv01,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qpinv01_sp;

  /*
  ||----------------------------------------------------------------------------
  || QMANFRP_SP
  ||  Builds the Final "Summary Load Department Summary" (OPLD03) manifest
  ||  report for the LLR date passed in the MQ message and ftp's it to the
  ||  mainframe.
  ||
  ||  The process is as follows:
  ||    The division runs xxOPLD1J which sends the requested LLR date to OP as
  ||      an MQ message via QOPMSGS queue.
  ||    The MQ message is loaded into MCLANE_MQ_GET and processed by QOPMSGS_SP
  ||      which calls this module to create and ftp the report to the mainframe.
  ||    The ftp triggers the division's xxOPLD2J which prints the report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/13/05 | rhalpai | Original - created for PIR2652
  || 12/02/05 | rhalpai | Removed return status parm from call to op_ftp_sp PIR2051
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  || 11/01/10 | rhalpai | Change to call OP_MANIFEST_REPORTS_PK.SUMMARY_LOAD_DEPT_SUMMARY_SP
  ||                    | without status parm. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qmanfrp_sp(
    i_div_part     IN      NUMBER,
    i_mq_msg_data  IN      VARCHAR2,
    o_status       OUT     VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.QMANFRP_SP';
    lar_parm                logs.tar_parm;
    l_div_id                div_mstr_di1d.div_id%TYPE;
    l_llr_num               NUMBER;
    l_c_strtg_id   CONSTANT NUMBER                      := 0;
    l_c_create_ts  CONSTANT DATE                        := SYSDATE;
    l_c_rmt_file   CONSTANT VARCHAR2(20)                := 'OPLD03F';
    l_file_nm               VARCHAR2(80);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qmanfrp,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    o_status := g_c_good;
    logs.dbg('Parse');
    l_div_id := SUBSTR(i_mq_msg_data, 1, 2);
    l_llr_num := TO_DATE(SUBSTR(i_mq_msg_data, 54, 8), 'YYYYMMDD') - DATE '1900-02-28';
    logs.dbg('Build Manifest Report Table for Final Summary for LLR');
    op_manifest_reports_pk.build_report_table_sp(l_div_id, DATE '1900-02-28' + l_llr_num, l_c_strtg_id, l_c_create_ts);
    logs.dbg('Create OPLD03 Report');
    op_manifest_reports_pk.summary_load_dept_summary_sp(l_llr_num, l_div_id, l_c_create_ts, l_file_nm);
    logs.dbg('FTP Report to Mainframe');
    op_ftp_sp(l_div_id, l_file_nm, l_c_rmt_file);
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qmanfrp,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qmanfrp,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qmanfrp_sp;

  /*
  ||----------------------------------------------------------------------------
  || QHAZMAT_SP
  ||  Maintains HAZMAT_RPT entries in tables RPT_NAME_AP7R, RPT_PARM_AP1E and
  ||  then creates the Hazardous Materials Order Report and ftp it to the
  ||  mainframe.
  ||
  ||  The process is as follows:
  ||    The division runs xxOPHZ1J which sends MQ messages via QOPMSGS queue
  ||      for Hazardous States/Items.
  ||    The MQ message is loaded into MCLANE_MQ_GET and processed by QOPMSGS_SP
  ||      which calls this module to maintain the report tables and then create
  ||      and ftp the report to the mainframe.
  ||    The ftp triggers the division's xxOPHZ2J which prints the report.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/13/05 | rhalpai | Original - created for PIR2440 to replace the logic
  ||                    | being executed via the Unix "Div Menu"
  || 12/02/05 | rhalpai | Removed return status parm from call to op_ftp_sp PIR2051
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  || 12/06/07 | rhalpai | Replaced input parm MqMsgData with DivId. Changed to
  ||                    | maintain HAZMAT_RPT entries in tables SPLIT_DMN_OP8S,
  ||                    | SPLIT_STA_ITM_OP1S and/or create the HazMat Order Report
  ||                    | and ftp it to the mainframe. PIR5132
  || 09/21/10 | rhalpai | Changed logic to only remove all data when a REF msg
  ||                    | (indicating 1st msg) exists. IM606867
  || 10/12/10 | rhalpai | Add REF logic for create_ts in cursor to ITEM. IM606867
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qhazmat_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.QHAZMAT_SP';
    lar_parm                logs.tar_parm;
    l_div_id                div_mstr_di1d.div_id%TYPE;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)                 := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)                 := 'QHAZMAT';
    l_is_msg_found          BOOLEAN;
    l_cv                    SYS_REFCURSOR;

    PROCEDURE maint_sp IS
      l_t_state_cd  type_stab;
      l_t_mcl_item  type_stab;
      l_ref_sw      VARCHAR2(1);
    BEGIN
      logs.dbg('Fetch Maintenance Cursor');

      -- The following cartesian is done on purpose
      -- since the states and items are sent separately.
      WITH x AS(
        SELECT TRIM(SUBSTR(g.mq_msg_data, 54, 8)) AS typ,
               TRIM(SUBSTR(g.mq_msg_data, 62)) AS val
          FROM mclane_mq_get g
         WHERE g.div_part = i_div_part
           AND g.mq_msg_status IN(g_c_wrk, g_c_compl)
           AND g.mq_msg_id = l_c_mq_msg_id
           AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
           AND EXISTS(SELECT 1
                        FROM mclane_mq_get g2
                       WHERE g2.div_part = i_div_part
                         AND g2.mq_msg_id = l_c_mq_msg_id
                         AND TRIM(SUBSTR(g2.mq_msg_data, 3, 8)) = l_c_msg_id
                         AND SUBSTR(g2.mq_msg_data, 41, 3) <> 'RPT'
                         AND g2.mq_msg_status = g_c_wrk)
           AND g.create_ts >=
                 (SELECT MAX(g3.create_ts)
                    FROM mclane_mq_get g3
                   WHERE g3.div_part = i_div_part
                     AND g3.mq_msg_status IN(g_c_wrk, g_c_compl)
                     AND g3.mq_msg_id = l_c_mq_msg_id
                     AND TRIM(SUBSTR(g3.mq_msg_data, 3, 8)) = l_c_msg_id
                     AND SUBSTR(g3.mq_msg_data, 41, 3) = 'REF')
      ), s AS(
        SELECT DISTINCT x.val AS state
                   FROM x
                  WHERE x.typ = 'STATES'
      ), i AS(
        SELECT DISTINCT x.val AS item
                   FROM x
                  WHERE x.typ = 'ITEM'
      )
      SELECT s.state, i.item
      BULK COLLECT INTO l_t_state_cd, l_t_mcl_item
        FROM s, i;

      IF l_t_state_cd.COUNT > 0 THEN
        logs.dbg('Check if Reload is Required');
        l_ref_sw := is_ref_msg_fn(i_div_part, l_c_msg_id);

        IF l_ref_sw = 'Y' THEN
          logs.dbg('Add Split Domain Entry');

          INSERT INTO split_dmn_op8s
                      (split_typ, descr, priorty)
            SELECT 'HAZMAT', 'Hazardous Materials', 2
              FROM DUAL
             WHERE NOT EXISTS(SELECT 1
                                FROM split_dmn_op8s sd
                               WHERE sd.split_typ = 'HAZMAT');

          IF SQL%NOTFOUND THEN
            logs.dbg('Remove All Report Parm Entries');

            DELETE FROM split_sta_itm_op1s s
                  WHERE s.split_typ = 'HAZMAT';
          END IF;   -- SQL%NOTFOUND
        END IF;   -- l_ref_sw = 'Y'

        logs.dbg('Add Report Parm Entries');
        FORALL i IN l_t_state_cd.FIRST .. l_t_state_cd.LAST
          INSERT INTO split_sta_itm_op1s
                      (state_cd, mcl_item, split_typ
                      )
               VALUES (l_t_state_cd(i), l_t_mcl_item(i), 'HAZMAT'
                      );
      END IF;   -- l_t_state_cd.COUNT > 0
    END maint_sp;

    PROCEDURE report_sp IS
      l_found_sw             VARCHAR2(1)  := 'N';
      l_c_file_nm   CONSTANT VARCHAR2(30) := l_div_id || 'OPHZMT';
      l_c_rmt_file  CONSTANT VARCHAR2(20) := 'QHAZMAT';
    BEGIN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM mclane_mq_get g
          WHERE g.div_part = i_div_part
            AND g.mq_msg_status = g_c_wrk
            AND g.mq_msg_id = l_c_mq_msg_id
            AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
            AND TRIM(SUBSTR(g.mq_msg_data, 54, 6)) = 'REPORT';

      logs.dbg('Fetch Maintenance Cursor');

      FETCH l_cv
       INTO l_found_sw;

      CLOSE l_cv;

      IF l_found_sw = 'Y' THEN
        logs.dbg('Create HazMat Report');
        op_misc_reports_pk.hazmat_rpt_sp(l_div_id);
        logs.dbg('FTP Report to Mainframe');
        op_ftp_sp(l_div_id, l_c_file_nm, l_c_rmt_file);
      END IF;   -- l_found_sw = 'Y'
    END report_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qhazmat,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    l_div_id := div_pk.div_id_fn(i_div_part);
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Process Msgs');
      maint_sp;
      logs.dbg('Create and FTP HazMat Report');
      report_sp;
      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qhazmat,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qhazmat,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qhazmat_sp;

  /*
  ||----------------------------------------------------------------------------
  || QBNDL01_SP
  ||  Process messages that describe "all-or-nothing" bundled item distribution
  ||  data stored in table BUNDL_DIST_ITEM_BD1I.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/21/05 | rhalpai | Original - created for PIR2545
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qbndl01_sp(
    i_div_part     IN      NUMBER,
    i_mq_msg_data  IN      VARCHAR2,
    o_status       OUT     VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.QBNDL01_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd   VARCHAR2(3),
      dist_id   bundl_dist_item_bd1i.dist_id%TYPE,
      dist_sfx  NUMBER,
      item_num  bundl_dist_item_bd1i.item_num%TYPE,
      unq_cd    bundl_dist_item_bd1i.unq_cd%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.dist_id := SUBSTR(i_mq_msg_data, 54, 10);
      l_r_parsed.dist_sfx := SUBSTR(i_mq_msg_data, 64, 4);
      l_r_parsed.item_num := SUBSTR(i_mq_msg_data, 68, 9);
      l_r_parsed.unq_cd := SUBSTR(i_mq_msg_data, 77, 3);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM bundl_dist_item_bd1i bi
            WHERE bi.div_part = i_div_part
              AND bi.dist_id = i_r_msg.dist_id
              AND bi.dist_sfx = i_r_msg.dist_sfx
              AND bi.item_num = i_r_msg.item_num
              AND bi.unq_cd = i_r_msg.unq_cd;
    END del_sp;

    PROCEDURE add_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      INSERT INTO bundl_dist_item_bd1i
                  (div_part, dist_id, dist_sfx, item_num, unq_cd
                  )
           VALUES (i_div_part, i_r_msg.dist_id, i_r_msg.dist_sfx, i_r_msg.item_num, i_r_msg.unq_cd
                  );
    EXCEPTION
      WHEN DUP_VAL_ON_INDEX THEN
        NULL;   -- ignore duplicate values
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qbndl01,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    o_status := g_c_good;
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');

    CASE l_r_msg.actn_cd
      WHEN g_c_del THEN
        logs.dbg('Remove Bundle Dist Item');
        del_sp(l_r_msg);
      WHEN g_c_add THEN
        logs.dbg('Add Bundle Dist Item');
        add_sp(l_r_msg);
    END CASE;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qbndl01,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qbndl01,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qbndl01_sp;

  /*
  ||----------------------------------------------------------------------------
  || QSKIPLD_SP
  ||  Process messages for "Skip Load" data stored in RPT_PARM_AP1E.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/06 | rhalpai | Original - created for PIR3937
  || 11/13/06 | rhalpai | Changed move process to only process for regular
  ||                    | orders. IM267352
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  || 08/19/08 | rhalpai | Changed cursor to use order header status to indicate
  ||                    | unbilled order status. PIR6364
  || 09/21/10 | rhalpai | Changed logic to only remove all data when a REF msg
  ||                    | (indicating 1st msg) exists. IM606867
  || 04/04/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qskipld_sp(
    i_div_part  IN      NUMBER,
    o_status    OUT     VARCHAR2
  ) IS
    l_c_module          CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.QSKIPLD_SP';
    lar_parm                     logs.tar_parm;
    l_div_id                     div_mstr_di1d.div_id%TYPE;
    l_msg_stat                   VARCHAR2(3);
    l_c_mq_msg_id       CONSTANT VARCHAR2(7)                 := 'QOPMSGS';
    l_c_msg_id          CONSTANT VARCHAR2(7)                 := 'QSKIPLD';
    l_c_skip_load       CONSTANT VARCHAR2(6)                 := 'SKIPLD';
    l_c_empty           CONSTANT VARCHAR2(5)                 := 'EMPTY';
    l_c_init_val        CONSTANT VARCHAR2(1)                 := '~';
    l_msg_typ                    VARCHAR2(7);
    l_msg_typ_save               VARCHAR2(7)                 := l_c_init_val;
    l_c_maint           CONSTANT VARCHAR2(5)                 := 'MAINT';
    l_c_rpt_req         CONSTANT VARCHAR2(6)                 := 'RPTREQ';
    l_c_move_req        CONSTANT VARCHAR2(7)                 := 'MOVEREQ';
    l_c_item_recap_rpt  CONSTANT VARCHAR2(6)                 := 'SKPLDI';
    l_c_move_log_rpt    CONSTANT VARCHAR2(6)                 := 'SKPLDM';
    l_c_ord_dtl_rpt     CONSTANT VARCHAR2(6)                 := 'SKPLDD';
    l_c_ord_sum_rpt     CONSTANT VARCHAR2(6)                 := 'SKPLDS';

    FUNCTION first_msg_typ_fn(
      i_msg_stat  IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
      l_typ  VARCHAR2(7)   := l_c_empty;
      l_cv   SYS_REFCURSOR;
    BEGIN
      OPEN l_cv
       FOR
         SELECT TRIM(SUBSTR(g.mq_msg_data, 11, 7))
           FROM mclane_mq_get g
          WHERE g.div_part = i_div_part
            AND g.mq_msg_id = l_c_mq_msg_id
            AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
            AND g.mq_msg_status = i_msg_stat;

      FETCH l_cv
       INTO l_typ;

      RETURN(l_typ);
    END first_msg_typ_fn;

    PROCEDURE upd_msg_sp(
      i_old_stat  IN  VARCHAR2,
      i_new_stat  IN  VARCHAR2,
      i_msg_typ   IN  VARCHAR2
    ) IS
    BEGIN
      IF i_msg_typ <> l_c_init_val THEN
        UPDATE mclane_mq_get g
           SET g.mq_msg_status = i_new_stat
         WHERE g.div_part = i_div_part
           AND g.mq_msg_id = l_c_mq_msg_id
           AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
           AND TRIM(SUBSTR(g.mq_msg_data, 11, 7)) = i_msg_typ
           AND g.mq_msg_status = i_old_stat;

        COMMIT;
      END IF;   -- i_msg_typ <> c_init_val
    END upd_msg_sp;

    PROCEDURE ftp_rpt_sp(
      i_rmt_file  IN  VARCHAR2
    ) IS
      l_c_archive     CONSTANT VARCHAR2(1) := 'Y';
      l_c_local_file  CONSTANT VARCHAR2(8) := l_div_id || i_rmt_file;
    BEGIN
      op_ftp_sp(l_div_id, l_c_local_file, i_rmt_file, l_c_archive);
    END ftp_rpt_sp;

    PROCEDURE maint_sp IS
      l_ref_sw  VARCHAR2(1);
    BEGIN
      logs.dbg('Set SkipLoad Mainenance Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_maint,
                                                  op_process_control_pk.g_c_active,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.dbg('Check if Reload is Required');

      SELECT NVL(MAX('Y'), 'N')
        INTO l_ref_sw
        FROM mclane_mq_get g
       WHERE g.div_part = i_div_part
         AND g.mq_msg_id = l_c_mq_msg_id
         AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
         AND TRIM(SUBSTR(g.mq_msg_data, 11, 7)) = l_c_maint
         AND g.mq_msg_status = g_c_wrk
         AND UPPER(SUBSTR(g.mq_msg_data, 41, 3)) = 'REF';

      IF l_ref_sw = 'Y' THEN
        logs.dbg('Remove All Report Parm Entries');

        DELETE FROM rpt_parm_ap1e rp
              WHERE rp.div_part = i_div_part
                AND EXISTS(SELECT 1
                             FROM rpt_name_ap7r rn
                            WHERE rn.div_part = rp.div_part
                              AND rn.rpt_nm = rp.rpt_nm
                              AND rn.user_id = l_c_skip_load);

        logs.dbg('Remove All Report Name Entries');

        DELETE FROM rpt_name_ap7r
              WHERE div_part = i_div_part
                AND user_id = l_c_skip_load;

        logs.dbg('Add Report Name Entries');

        INSERT INTO rpt_name_ap7r
                    (div_part, rpt_nm, descr, user_id)
          SELECT   i_div_part, TRIM(SUBSTR(g.mq_msg_data, 63, 20)), TRIM(SUBSTR(g.mq_msg_data, 63, 20)) || ' REPORT',
                   l_c_skip_load
              FROM mclane_mq_get g
             WHERE g.div_part = i_div_part
               AND g.mq_msg_status = g_c_wrk
               AND g.mq_msg_id = l_c_mq_msg_id
               AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
          GROUP BY TRIM(SUBSTR(g.mq_msg_data, 63, 20));
      END IF;   -- l_ref_sw = 'Y'

      logs.dbg('Add Report Parm Entries');

      INSERT INTO rpt_parm_ap1e
                  (div_part, rpt_nm, rpt_typ, val_cd, user_id)
        SELECT g.div_part, TRIM(SUBSTR(g.mq_msg_data, 63, 20)), TRIM(SUBSTR(g.mq_msg_data, 83)),
               LPAD(TRIM(SUBSTR(g.mq_msg_data, 57, 6)), 6, '0'), LPAD(TRIM(SUBSTR(g.mq_msg_data, 54, 3)), 3, '0')
          FROM mclane_mq_get g
         WHERE g.div_part = i_div_part
           AND g.mq_msg_status = g_c_wrk
           AND g.mq_msg_id = l_c_mq_msg_id
           AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id;

      IF SQL%FOUND THEN
        logs.dbg('Create Item Recap Report');
        op_misc_reports_pk.skipld_items_rpt_sp(l_div_id);
        logs.dbg('FTP Item Recap Report');
        ftp_rpt_sp(l_c_item_recap_rpt);
      END IF;   -- SQL%FOUND

      logs.dbg('Set SkipLoad Mainenance Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
    END maint_sp;

    PROCEDURE order_reports_sp IS
    BEGIN
      logs.dbg('Set SkipLoad Report Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_rpt,
                                                  op_process_control_pk.g_c_active,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.dbg('Create Order Detail Report');
      op_misc_reports_pk.skipld_detail_rpt_sp(l_div_id);
      logs.dbg('FTP Order Detail Report');
      ftp_rpt_sp(l_c_ord_dtl_rpt);
      logs.dbg('Create Order Summary Report');
      op_misc_reports_pk.skipld_sum_rpt_sp(l_div_id);
      logs.dbg('FTP Order Summary Report');
      ftp_rpt_sp(l_c_ord_sum_rpt);
      logs.dbg('Set SkipLoad Report Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_rpt,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
    END order_reports_sp;

    PROCEDURE move_sp IS
      l_t_ords             type_ntab;
      l_c_log_ts  CONSTANT DATE      := SYSDATE;
    BEGIN
      logs.dbg('Set SkipLoad Move Order Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_move,
                                                  op_process_control_pk.g_c_active,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.dbg('Get Orders for Move');

      SELECT a.ordnoa
      BULK COLLECT INTO l_t_ords
        FROM ordp100a a, load_depart_op1f ld, mclp020b cx, mclp040d d
       WHERE a.div_part = i_div_part
         AND a.dsorda = 'R'
         AND a.stata = 'O'
         AND a.excptn_sw = 'N'
         AND ld.div_part = a.div_part
         AND ld.load_depart_sid = a.load_depart_sid
         AND d.div_part = a.div_part
         AND d.custd = a.custa
         AND d.loadd = ld.load_num
         AND cx.div_part = a.div_part
         AND cx.custb = a.custa
         AND NOT EXISTS(SELECT 1
                          FROM mclp300d md
                         WHERE md.div_part = a.div_part
                           AND md.ordnod = a.ordnoa
                           AND md.reasnd = l_c_skip_load)
         AND EXISTS(SELECT 1
                      FROM rpt_name_ap7r rn, rpt_parm_ap1e rp, sawp505e e, ordp120b b
                     WHERE rn.div_part = i_div_part
                       AND rn.user_id = l_c_skip_load
                       AND rp.rpt_nm = rn.rpt_nm
                       AND rp.div_part = cx.div_part
                       AND rp.val_cd = e.catite
                       AND rp.user_id = LPAD(cx.corpb, 3, '0')
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb = 'O'
                       AND b.excptn_sw = 'N'
                       AND e.iteme = b.itemnb
                       AND e.uome = b.sllumb);

      IF l_t_ords IS NOT NULL THEN
        logs.dbg('Call SYNCLOAD to Move Orders');
        op_order_load_pk.syncload_sp(i_div_part, l_c_skip_load, l_t_ords);
        logs.dbg('Create Move Log Report');
        op_misc_reports_pk.skipld_move_rpt_sp(l_div_id, l_c_log_ts);
        logs.dbg('FTP Move Log Report');
        ftp_rpt_sp(l_c_move_log_rpt);
      END IF;

      logs.dbg('Set SkipLoad Move Order Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_move,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
    END move_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    o_status := g_c_good;
    l_div_id := div_pk.div_id_fn(i_div_part);
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    l_msg_stat := g_c_opn;
    LOOP
      l_msg_typ := first_msg_typ_fn(l_msg_stat);

      IF l_msg_typ NOT IN(l_c_empty, l_msg_typ_save) THEN
        l_msg_typ_save := l_msg_typ;

        IF l_msg_stat = g_c_opn THEN
          logs.dbg('Tag Msgs to Process');
          upd_msg_sp(g_c_opn, g_c_wrk, l_msg_typ_save);
          l_msg_stat := g_c_wrk;

          CASE l_msg_typ_save
            WHEN l_c_maint THEN
              maint_sp;
            WHEN l_c_rpt_req THEN
              order_reports_sp;
            WHEN l_c_move_req THEN
              move_sp;
          END CASE;

          logs.dbg('Change Status of Tagged Msgs to Complete');
          upd_msg_sp(g_c_wrk, g_c_compl, l_msg_typ_save);
        END IF;   -- l_msg_stat = g_c_opn
      END IF;   -- l_msg_typ NOT IN(l_c_empty, l_msg_typ_save)

      EXIT WHEN l_msg_typ = l_c_empty;
    END LOOP;
    logs.dbg('Final Change Status of Tagged Msgs to Complete');
    upd_msg_sp(g_c_wrk, g_c_compl, l_msg_typ_save);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      o_status := SUBSTR(l_c_module || ' Error: ' || SQLERRM, 1, 500);
      logs.err(lar_parm, NULL, FALSE);
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      ROLLBACK;
      upd_msg_sp(g_c_wrk, g_c_prb, l_msg_typ_save);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_maint,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_move,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qskipld_rpt,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qskipld_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCTC501_SP
  ||  Process messages that request additional container tracking labels.
  ||  Generate requested amount of container tracking labels, store in
  ||  ADDL_CNTNR_ID_BC3C and ftp to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/13/06 | rhalpai | Original - created for PIR3209
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qctc501_sp(
    i_div_part     IN      NUMBER,
    i_mq_msg_data  IN      VARCHAR2,
    o_status       OUT     VARCHAR2
  ) IS
    l_c_module            CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCTC501_SP';
    lar_parm                       logs.tar_parm;
    l_div_id                       div_mstr_di1d.div_id%TYPE;
    l_req_cnt                      PLS_INTEGER;
    l_c_dflt_cnt          CONSTANT NUMBER(2)                          := 50;
    l_c_manual_cntnr_req  CONSTANT VARCHAR2(1)                        := 'Y';
    l_cntnr_id                     addl_cntnr_id_bc3c.cntnr_id%TYPE;
    l_c_sysdate           CONSTANT DATE                               := SYSDATE;
    l_t_rpt_lns                    typ.tas_maxvc2;
    l_c_file_dir          CONSTANT VARCHAR2(9)                        := '/ftptrans';
    l_c_rmt_file          CONSTANT VARCHAR2(30)                       := 'QCTC511';
    l_file_nm                      VARCHAR2(30);
    l_c_no_archive        CONSTANT VARCHAR2(1)                        := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qctc501,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    o_status := g_c_good;
    l_div_id := div_pk.div_id_fn(i_div_part);
    l_file_nm := l_div_id || l_c_rmt_file;
    logs.dbg('Parse MQ Message Data');
    l_req_cnt := NVL(string_to_num_fn(SUBSTR(i_mq_msg_data, 54, 3)), l_c_dflt_cnt);
    FOR i IN 1 .. l_req_cnt LOOP
      logs.dbg('Get the Container ID');
      l_cntnr_id := op_allocate_pk.container_id_fn(l_div_id, l_c_sysdate, NULL, NULL, l_c_manual_cntnr_req);
      logs.dbg('Add ADDL_CNTNR_ID_BC3C Record');

      INSERT INTO addl_cntnr_id_bc3c
                  (cntnr_id, create_ts
                  )
           VALUES (l_cntnr_id, l_c_sysdate
                  );

      logs.dbg('Add to Report');
      util.append(l_t_rpt_lns, LPAD(l_cntnr_id, 20, '0'));
    END LOOP;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
    logs.dbg('Ftp to the Mainframe');
    op_ftp_sp(l_div_id, l_file_nm, l_c_rmt_file, l_c_no_archive);
    COMMIT;
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qctc501,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qctc501,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qctc501_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCTCCUS_SP
  ||  Process messages that affect customer container tracking set up.
  ||  The CNTNR_TRCKG_SW on MCLP100A will be set to 'Y' or 'N'.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/06 | rhalpai | Original - created for PIR3209
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qctccus_sp(
    i_div_part     IN      NUMBER,
    i_mq_msg_data  IN      VARCHAR2,
    o_status       OUT     VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.QCTCCUS_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd  VARCHAR2(3),
      grp_id   mclp100a.cstgpa%TYPE
    );

    l_r_msg              l_rt_msg;
    l_e_invalid_actn_cd  EXCEPTION;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.grp_id := SUBSTR(i_msg_data, 1, 2) || LPAD(LTRIM(SUBSTR(i_mq_msg_data, 54, 3)), 3, '0');
      RETURN(l_r_parsed);
    END parse_msg_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qctccus,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    o_status := g_c_good;
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);

    IF l_r_msg.actn_cd NOT IN(g_c_add, g_c_del) THEN
      o_status := 'Invalid Action: ' || l_r_msg.actn_cd || ' for Group: ' || l_r_msg.grp_id;
      RAISE l_e_invalid_actn_cd;
    END IF;   -- l_r_msg.actn_cd NOT IN(g_c_add, g_c_del)

    logs.dbg('Update MCLP100A');

    UPDATE mclp100a a
       SET a.cntnr_trckg_sw = DECODE(l_r_msg.actn_cd, g_c_add, 'Y', 'N')
     WHERE a.div_part = i_div_part
       AND a.cstgpa = l_r_msg.grp_id;

    COMMIT;
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qctccus,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_actn_cd THEN
      logs.err(o_status, lar_parm, NULL, FALSE);
      o_status := l_c_module || ' ' || o_status;
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qctccus,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qctccus_sp;

  /*
  ||----------------------------------------------------------------------------
  || QSPLTORD_SP
  ||  Process messages that affect Split Order set up.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/10/07 | rhalpai | Original - created for PIR4274
  || 09/21/10 | rhalpai | Changed logic to only remove all data when a REF msg
  ||                    | (indicating 1st msg) exists. IM606867
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qspltord_sp(
    i_div_part  IN      NUMBER,
    o_status    OUT     VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.QSPLTORD_SP';
    lar_parm                logs.tar_parm;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)   := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)   := 'QSPLTORD';
    l_c_split_ord  CONSTANT VARCHAR2(8)   := 'SPLITORD';
    l_is_msg_found          BOOLEAN;

    PROCEDURE maint_sp IS
      l_ref_sw  VARCHAR2(1);
    BEGIN
      logs.dbg('Check if Reload is Required');
      l_ref_sw := is_ref_msg_fn(i_div_part, l_c_msg_id);

      IF l_ref_sw = 'Y' THEN
        logs.dbg('Remove All Report Parm Entries');

        DELETE FROM rpt_parm_ap1e rp
              WHERE rp.div_part = i_div_part
                AND rp.user_id = l_c_split_ord;

        logs.dbg('Remove All Report Name Entries');

        DELETE FROM rpt_name_ap7r
              WHERE div_part = i_div_part
                AND user_id = l_c_split_ord;

        logs.dbg('Add Report Name Entries');

        INSERT INTO rpt_name_ap7r
                    (div_part, rpt_nm, descr, user_id)
          SELECT i_div_part, x.rpt_nm, x.rpt_nm || ' REPORT', l_c_split_ord
            FROM (SELECT   TRIM(SUBSTR(g.mq_msg_data, 54, 20)) AS rpt_nm
                      FROM mclane_mq_get g
                     WHERE g.div_part = i_div_part
                       AND g.mq_msg_status = g_c_wrk
                       AND g.mq_msg_id = l_c_mq_msg_id
                       AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
                  GROUP BY TRIM(SUBSTR(g.mq_msg_data, 54, 20))) x
           WHERE NOT EXISTS(SELECT 1
                              FROM rpt_name_ap7r r
                             WHERE r.div_part = i_div_part
                               AND r.rpt_nm = x.rpt_nm);
      END IF;   -- l_ref_sw = 'Y'

      logs.dbg('Add Report Parm Entries');

      INSERT INTO rpt_parm_ap1e
                  (div_part, rpt_nm, rpt_typ, val_cd, user_id)
        SELECT i_div_part, x.rpt_nm, x.typ, x.val, l_c_split_ord
          FROM (SELECT DISTINCT TRIM(SUBSTR(g.mq_msg_data, 54, 20)) AS rpt_nm, TRIM(SUBSTR(g.mq_msg_data, 74, 8))
                                                                                                                 AS typ,
                                DECODE(TRIM(SUBSTR(g.mq_msg_data, 74, 8)),
                                       'GROUP', SUBSTR(g.mq_msg_data, 1, 2) || SUBSTR(g.mq_msg_data, 82, 3),
                                       SUBSTR(g.mq_msg_data, 82, 6)
                                      ) AS val
                           FROM mclane_mq_get g
                          WHERE g.div_part = i_div_part
                            AND g.mq_msg_status = g_c_wrk
                            AND g.mq_msg_id = l_c_mq_msg_id
                            AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id) x
         WHERE NOT EXISTS(SELECT 1
                            FROM rpt_parm_ap1e r
                           WHERE r.div_part = i_div_part
                             AND r.rpt_nm = x.rpt_nm
                             AND r.rpt_typ = x.typ
                             AND r.val_cd = x.val);
    END maint_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qspltord,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    o_status := g_c_good;
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Process Msgs');
      maint_sp;
      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qspltord,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      o_status := SUBSTR(l_c_module || ' Unhandled Error: ' || SQLERRM, 1, 500);
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qspltord,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qspltord_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDR_SP
  ||  Process message for maintenance to Vendor Master table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/07/07 | rhalpai | Original - created for PIR5002
  || 07/17/08 | rhalpai | Changed logic to remove references to columns on
  ||                    | VNDR_MSGR_OP1V that are no longer used. PIR5002
  || 11/10/15 | rhalpai | Add logic to support new cust_lvl_dtl_sw. PIR15456
  || 01/02/24 | rhalpai | Add logic to support new ENFORC_PO_QTY_SW column. PC-9546
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndr_sp(
    i_div_part     IN  NUMBER,
    i_mq_msg_data  IN  VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.VNDR_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div              VARCHAR2(2),
      actn_cd          VARCHAR2(3),
      cbr_vndr_id      NUMBER,
      dcs_vndr_id      NUMBER,
      vndr_nm          vndr_mstr_op1v.vndr_nm%TYPE,
      lead_days        NUMBER,
      cust_lvl_dtl_sw   VARCHAR2(1),
      enforc_po_qty_sw  VARCHAR2(1)
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cbr_vndr_id := string_to_num_fn(SUBSTR(i_msg_data, 54, 10));
      l_r_parsed.dcs_vndr_id := string_to_num_fn(SUBSTR(i_msg_data, 64, 10));
      l_r_parsed.vndr_nm := RTRIM(SUBSTR(i_msg_data, 74, 40));
      l_r_parsed.lead_days := string_to_num_fn(SUBSTR(i_msg_data, 114, 4));
      l_r_parsed.cust_lvl_dtl_sw := SUBSTR(i_msg_data, 118, 1);
      l_r_parsed.enforc_po_qty_sw := NVL(TRIM(SUBSTR(i_msg_data, 119, 1)), 'N');
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM vndr_mstr_op1v v
            WHERE v.div_part = i_div_part
              AND v.cbr_vndr_id = i_r_msg.cbr_vndr_id;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO vndr_mstr_op1v v
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    v.div_part = i_div_part
                  AND v.cbr_vndr_id = i_r_msg.cbr_vndr_id
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET v.dcs_vndr_id = i_r_msg.dcs_vndr_id, v.vndr_nm = i_r_msg.vndr_nm, v.lead_days = i_r_msg.lead_days,
                 v.cust_lvl_dtl_sw = i_r_msg.cust_lvl_dtl_sw, v.enforc_po_qty_sw = i_r_msg.enforc_po_qty_sw
        WHEN NOT MATCHED THEN
          INSERT(div_part, cbr_vndr_id, dcs_vndr_id, vndr_nm, lead_days, cust_lvl_dtl_sw, enforc_po_qty_sw)
          VALUES(i_div_part, i_r_msg.cbr_vndr_id, i_r_msg.dcs_vndr_id, i_r_msg.vndr_nm, i_r_msg.lead_days,
                 i_r_msg.cust_lvl_dtl_sw, i_r_msg.enforc_po_qty_sw);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndr,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');

    CASE
      WHEN l_r_msg.actn_cd = g_c_del THEN
        logs.dbg('Remove Entry');
        del_sp(l_r_msg);
      WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
        logs.dbg('Add/Chg Entry');
        merge_sp(l_r_msg);
    END CASE;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndr,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndr,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END vndr_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDRTS_SP
  ||  Process message for maintenance to Vendor Timestamp table for
  ||  POCutoffTS/ProdRcptTS/DatesForLeadDays.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/04/08 | rhalpai | Original - created for PIR5002
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndrts_sp(
    i_div_part     IN  NUMBER,
    i_mq_msg_data  IN  VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.VNDRTS_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div          VARCHAR2(2),
      actn_cd      VARCHAR2(3),
      cbr_vndr_id  NUMBER,
      ts_typ       vndr_ts_op4v.ts_typ%TYPE,
      ts           DATE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cbr_vndr_id := string_to_num_fn(SUBSTR(i_msg_data, 54, 10));
      l_r_parsed.ts_typ := RTRIM(SUBSTR(i_msg_data, 64, 3));
      l_r_parsed.ts := TO_DATE(SUBSTR(i_msg_data, 67, 16), 'YYYY-MM-DD HH24:MI');
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM vndr_ts_op4v v
            WHERE v.div_part = i_div_part
              AND v.cbr_vndr_id = i_r_msg.cbr_vndr_id
              AND v.ts_typ = i_r_msg.ts_typ
              AND v.ts = i_r_msg.ts;
    END del_sp;

    PROCEDURE add_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      INSERT INTO vndr_ts_op4v
                  (div_part, cbr_vndr_id, ts_typ, ts
                  )
           VALUES (i_div_part, i_r_msg.cbr_vndr_id, i_r_msg.ts_typ, i_r_msg.ts
                  );
    EXCEPTION
      WHEN DUP_VAL_ON_INDEX THEN
        NULL;   -- ignore duplicate values
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrts,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');

    CASE
      WHEN l_r_msg.actn_cd = g_c_del THEN
        logs.dbg('Remove Entry');
        del_sp(l_r_msg);
      WHEN l_r_msg.actn_cd = g_c_add THEN
        logs.dbg('Add/Chg Entry');
        add_sp(l_r_msg);
    END CASE;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrts,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrts,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END vndrts_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCTITM_SP
  ||  Process message for maintenance to Strict Item table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/07/07 | rhalpai | Original - created for PIR5002
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strctitm_sp(
    i_div_part     IN  NUMBER,
    i_mq_msg_data  IN  VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.STRCTITM_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div          VARCHAR2(2),
      actn_cd      VARCHAR2(3),
      cbr_vndr_id  NUMBER,
      item_num     strct_item_op3v.item_num%TYPE,
      uom          strct_item_op3v.uom%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cbr_vndr_id := string_to_num_fn(SUBSTR(i_msg_data, 54, 10));
      l_r_parsed.item_num := TRIM(SUBSTR(i_msg_data, 64, 9));
      l_r_parsed.uom := TRIM(SUBSTR(i_msg_data, 73, 3));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM strct_item_op3v si
            WHERE si.div_part = i_div_part
              AND si.cbr_vndr_id = i_r_msg.cbr_vndr_id
              AND si.item_num = i_r_msg.item_num
              AND si.uom = i_r_msg.uom;

      DELETE FROM split_div_vnd_op3s sv
            WHERE sv.div_part = i_div_part
              AND sv.cbr_vndr_id = i_r_msg.cbr_vndr_id
              AND sv.split_typ = op_split_ord_pk.g_c_split_typ_strict_ord
              AND NOT EXISTS(SELECT 1
                               FROM strct_item_op3v si
                              WHERE si.div_part = i_div_part
                                AND si.cbr_vndr_id = i_r_msg.cbr_vndr_id);
    END del_sp;

    PROCEDURE add_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      INSERT INTO strct_item_op3v
                  (div_part, cbr_vndr_id, item_num, uom
                  )
           VALUES (i_div_part, i_r_msg.cbr_vndr_id, i_r_msg.item_num, i_r_msg.uom
                  );

      INSERT INTO split_div_vnd_op3s
                  (div_part, cbr_vndr_id, split_typ
                  )
           VALUES (i_div_part, i_r_msg.cbr_vndr_id, op_split_ord_pk.g_c_split_typ_strict_ord
                  );
    EXCEPTION
      WHEN DUP_VAL_ON_INDEX THEN
        NULL;   -- ignore duplicate values
    END add_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctitm,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');

    CASE l_r_msg.actn_cd
      WHEN g_c_del THEN
        logs.dbg('Remove Entry');
        del_sp(l_r_msg);
      WHEN g_c_add THEN
        logs.dbg('Add Entry');
        add_sp(l_r_msg);
    END CASE;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctitm,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctitm,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END strctitm_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCTRS_SP
  ||  Create Strict Item Vendor Recap Summary Report and ftp to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/07/07 | rhalpai | Original - created for PIR5002
  || 08/01/08 | rhalpai | Changed call to STRCT_VNDR_RECAP_RPT_SP to include
  ||                    | remote file to allow automatic ftp. PIR5002
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strctrs_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.STRCTRS_SP';
    lar_parm               logs.tar_parm;
    l_div_id               div_mstr_di1d.div_id%TYPE;
    l_c_rmt_file  CONSTANT VARCHAR2(20)                := 'STRICT.RECAP.SUM.RPT';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctrs,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    l_div_id := div_pk.div_id_fn(i_div_part);
    logs.dbg('Create Strict Item Vendor Recap Summary Report');
    op_misc_reports_pk.strct_vndr_recap_rpt_sp(l_div_id, l_c_rmt_file);
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctrs,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctrs,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END strctrs_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCTD_SP
  ||  Create Strict Item Order Detail Report and ftp to mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/07/07 | rhalpai | Original - created for PIR5002
  || 08/01/08 | rhalpai | Changed call to STRCT_ORD_DTL_RPT_SP to include
  ||                    | remote file to allow automatic ftp. PIR5002
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strctd_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.STRCTD_SP';
    lar_parm               logs.tar_parm;
    l_div_id               div_mstr_di1d.div_id%TYPE;
    l_c_rmt_file  CONSTANT VARCHAR2(20)                := 'STRICT.ORDER.DTL.RPT';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctd,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    l_div_id := div_pk.div_id_fn(i_div_part);
    logs.dbg('Create Strict Item Vendor Recap Summary Report');
    op_misc_reports_pk.strct_ord_dtl_rpt_sp(l_div_id, l_c_rmt_file);
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctd,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctd,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END strctd_sp;

  /*
  ||----------------------------------------------------------------------------
  || SPLITORD_SP
  ||  Split Order Maintenance.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/12/07 | rhalpai | Original - created for PIR5341
  || 09/21/10 | rhalpai | Changed logic to only remove all data when a REF msg
  ||                    | (indicating 1st msg) exists. IM606867
  ||----------------------------------------------------------------------------
  */
  PROCEDURE splitord_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.SPLITORD_SP';
    lar_parm                logs.tar_parm;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)   := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)   := 'SPLITORD';
    l_is_msg_found          BOOLEAN;

    PROCEDURE maint_sp IS
      l_cv           SYS_REFCURSOR;
      l_t_split      type_stab     := type_stab();
      l_tbl          VARCHAR2(4);
      l_split_typ    VARCHAR2(10);
      l_t_cust_vals  type_stab;
      l_t_item_vals  type_stab;
      l_ref_sw       VARCHAR2(1);
    BEGIN
      logs.dbg('Open Split Cursor');

      OPEN l_cv
       FOR
         SELECT DISTINCT SUBSTR(g.mq_msg_data, 54, 14)
                    FROM mclane_mq_get g
                   WHERE g.div_part = i_div_part
                     AND g.mq_msg_status = g_c_wrk
                     AND g.mq_msg_id = l_c_mq_msg_id
                     AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id;

      logs.dbg('Fetch Split Cursor');

      FETCH l_cv
      BULK COLLECT INTO l_t_split;

      IF l_cv%ROWCOUNT > 0 THEN
        CLOSE l_cv;

        <<split_loop>>
        FOR i IN l_t_split.FIRST .. l_t_split.LAST LOOP
          l_tbl := UPPER(SUBSTR(l_t_split(i), 1, 4));
          l_split_typ := UPPER(TRIM(SUBSTR(l_t_split(i), 5, 10)));
          logs.dbg('Open Cust/Item Cursor');

          OPEN l_cv
           FOR
             SELECT TRIM(SUBSTR(g.mq_msg_data, 68, 8)) AS cust_val, TRIM(SUBSTR(g.mq_msg_data, 76, 12)) AS item_val
               FROM mclane_mq_get g
              WHERE g.div_part = i_div_part
                AND g.mq_msg_status = g_c_wrk
                AND g.mq_msg_id = l_c_mq_msg_id
                AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
                AND SUBSTR(g.mq_msg_data, 54, 14) = l_t_split(i);

          logs.dbg('Fetch Cust/Item Cursor');

          FETCH l_cv
          BULK COLLECT INTO l_t_cust_vals, l_t_item_vals;

          CLOSE l_cv;

          logs.dbg('Determine Maintenance Table');

          CASE l_tbl
            WHEN 'OP1C' THEN
              logs.dbg('Check if OP1C Reload is Required');
              l_ref_sw := is_ref_msg_fn(i_div_part, l_c_msg_id);

              IF l_ref_sw = 'Y' THEN
                logs.dbg('Remove Split Type Rows from OP1C');

                DELETE FROM split_cus_itm_op1c s
                      WHERE s.div_part = i_div_part
                        AND s.split_typ = l_split_typ;
              END IF;   -- l_ref_sw = 'Y'

              logs.dbg('Add Split Type Rows to OP1C');
              FORALL j IN l_t_cust_vals.FIRST .. l_t_cust_vals.LAST
                INSERT INTO split_cus_itm_op1c
                            (div_part, cbr_cust, mcl_item, split_typ
                            )
                     VALUES (i_div_part, l_t_cust_vals(j), l_t_item_vals(j), l_split_typ
                            );
            WHEN 'OP3S' THEN
              logs.dbg('Check if OP3S Reload is Required');
              l_ref_sw := is_ref_msg_fn(i_div_part, l_c_msg_id);

              IF l_ref_sw = 'Y' THEN
                logs.dbg('Remove Split Type Rows from OP3S');

                DELETE FROM split_div_vnd_op3s s
                      WHERE s.div_part = i_div_part
                        AND s.split_typ = l_split_typ;
              END IF;   -- l_ref_sw = 'Y'

              logs.dbg('Add Split Type Rows to OP3S');
              FORALL j IN l_t_cust_vals.FIRST .. l_t_cust_vals.LAST
                INSERT INTO split_div_vnd_op3s
                            (div_part, cbr_vndr_id, split_typ
                            )
                     VALUES (i_div_part, l_t_item_vals(j), l_split_typ
                            );
            WHEN 'OP1S' THEN
              logs.dbg('Check if OP1S Reload is Required');
              l_ref_sw := is_ref_msg_fn(i_div_part, l_c_msg_id);

              IF l_ref_sw = 'Y' THEN
                logs.dbg('Remove Split Type Rows from OP1S');

                DELETE FROM split_sta_itm_op1s s
                      WHERE s.split_typ = l_split_typ;
              END IF;   -- l_ref_sw = 'Y'

              logs.dbg('Add Split Type Rows to OP1S');
              FORALL j IN l_t_cust_vals.FIRST .. l_t_cust_vals.LAST
                INSERT INTO split_sta_itm_op1s
                            (state_cd, mcl_item, split_typ
                            )
                     VALUES (l_t_cust_vals(j), l_t_item_vals(j), l_split_typ
                            );
          END CASE;
        END LOOP split_loop;
      END IF;   -- l_cv%ROWCOUNT > 0
    END maint_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_splitord,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Process Msgs');
      maint_sp;
      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_splitord,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_splitord,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END splitord_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCORPCD_SP
  ||  Process message for maintenance to Corp Code table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/08 | rhalpai | Original - created for PIR5882
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcorpcd_sp(
    i_div_part     IN  NUMBER,
    i_mq_msg_data  IN  VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.QCORPCD_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd  VARCHAR2(3),
      crp_cd   NUMBER,
      crp_nm   corp_cd_dm1c.corp_nm%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.crp_cd := string_to_num_fn(SUBSTR(i_msg_data, 54, 3));
      l_r_parsed.crp_nm := NVL(RTRIM(SUBSTR(i_msg_data, 57, 30)), ' ');
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM corp_cd_dm1c c
            WHERE c.corp_cd = i_r_msg.crp_cd;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO corp_cd_dm1c c
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    c.corp_cd = i_r_msg.crp_cd
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET c.corp_nm = i_r_msg.crp_nm
        WHEN NOT MATCHED THEN
          INSERT(corp_cd, corp_nm)
          VALUES(i_r_msg.crp_cd, i_r_msg.crp_nm);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcorpcd,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');

    CASE
      WHEN l_r_msg.actn_cd = g_c_del THEN
        logs.dbg('Remove Entry');
        del_sp(l_r_msg);
      WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
        logs.dbg('Add/Chg Entry');
        merge_sp(l_r_msg);
    END CASE;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcorpcd,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcorpcd,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END qcorpcd_sp;

  /*
  ||----------------------------------------------------------------------------
  || QTYAUDIT_SP
  ||  Order Qty Audit Extract
  ||
  ||  Flow:
  ||  * MAR02 sends MQ trigger msg to Generic Queue to QOPMSGS for QTYAUDIT
  ||  * MQ Interface QOPMSGS_SP is triggered and runs QTYAUDIT Extract Process
  ||  * QTYAUDIT will extract qty changes for billed orders since last extract
  ||    run and FTP to mainframe
  ||  * FTP will trigger job on mainframe to load data to BD tables
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/27/09 | rhalpai | Original - created for PIR8100
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.MERGE_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qtyaudit_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.QTYAUDIT_SP';
    lar_parm                  logs.tar_parm;
    l_div_id                  div_mstr_di1d.div_id%TYPE;
    l_c_file_dir     CONSTANT VARCHAR2(30)                := '/ftptrans';
    l_file_nm                 VARCHAR2(30);
    l_c_rmt_file     CONSTANT VARCHAR2(30)                := 'QTYAUDIT';
    l_c_curr_run_ts  CONSTANT DATE                        := SYSDATE;
    l_last_run_ts             DATE;
    l_t_rpt_lns               typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qtyaudit,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    l_div_id := div_pk.div_id_fn(i_div_part);
    l_file_nm := l_div_id || '_QTYAUDIT';
    logs.dbg('Get Last Run TS');
    l_last_run_ts := TO_DATE(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_qty_audit_last_ts), 'YYYYMMDDHH24MISS');

    IF l_last_run_ts IS NOT NULL THEN
      logs.dbg('Get Report Lines');

      SELECT x.cust_id
             || x.catlg_num
             || LPAD(x.ord_num, 11, '0')
             || TO_CHAR(x.ord_ln, 'FM00000V00')
             || LPAD(x.orig_qty, 7, '0')
             || LPAD(x.qty_fro, 7, '0')
             || LPAD(x.qty_to, 7, '0')
             || RPAD(x.rsn_cd, 8)
             || RPAD(x.user_id, 20)
             || TO_CHAR(x.last_chg_ts, 'YYYYMMDDHH24MISS ')
             || TO_CHAR(x.ord_rcvd_ts, 'YYYYMMDDHH24MISS ')
             || RPAD(x.rlse_ts, 15)
      BULK COLLECT INTO l_t_rpt_lns
        FROM (SELECT   a.custa AS cust_id, b.orditb AS catlg_num, b.ordnob AS ord_num, b.lineb AS ord_ln,
                       b.orgqtb AS orig_qty, lg.qtyfrd AS qty_fro, lg.qtytod AS qty_to, lg.reasnd AS rsn_cd,
                       'ORD_VALIDATION' AS user_id, lg.last_chg_ts, a.ord_rcvd_ts, b.shpidb AS rlse_ts
                  FROM ordp120b b, ordp100a a, mclp300d lg
                 WHERE b.div_part = i_div_part
                   AND b.statb IN('R', 'A')
                   AND lg.div_part = b.div_part
                   AND lg.ordnod = b.ordnob
                   AND lg.ordlnd = b.lineb
                   AND lg.qtyfrd <> lg.qtytod
                   AND a.div_part = b.div_part
                   AND a.ordnoa = b.ordnob
                   AND a.stata IN('P', 'R', 'A')
                   AND (a.load_depart_sid, b.shpidb) IN(
                         SELECT ld.load_depart_sid, TO_CHAR(r.rlse_ts, 'YYYYMMDDHH24MISS')
                           FROM rlse_op1z r, load_depart_op1f ld
                          WHERE r.div_part = i_div_part
                            AND 'RLSECMP' = (SELECT DISTINCT FIRST_VALUE(rl.typ_id) OVER(ORDER BY rl.seq_of_events DESC)
                                                        FROM rlse_typ_dmn_op9z rtd, rlse_log_op2z rl
                                                       WHERE rtd.seq > -1
                                                         AND rtd.parnt_typ = 'RLSE'
                                                         AND rl.div_part = r.div_part
                                                         AND rl.rlse_id = r.rlse_id
                                                         AND rl.typ_id = rtd.typ_id)
                            AND r.ord_ln_cnt > 0
                            AND r.test_bil_cd = '~'
                            AND r.end_ts >= l_last_run_ts
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND EXISTS(SELECT 1
                                         FROM rlse_log_op2z rl
                                        WHERE rl.div_part = r.div_part
                                          AND rl.rlse_id = r.rlse_id
                                          AND rl.typ_id = 'LOAD'
                                          AND rl.val = ld.load_num))
              UNION ALL
              SELECT   a.custa AS cust_id, b.orditb AS catlg_num, b.ordnob AS ord_num, b.lineb AS ord_ln,
                       b.orgqtb AS orig_qty, TO_NUMBER(lg.florga) AS qty_fro, TO_NUMBER(lg.flchga) AS qty_to,
                       lg.rsncda AS rsn_cd, lg.usera AS user_id,
                       TO_DATE('19000228' || LPAD(lg.timea, 6, '0'), 'YYYYMMDDHH24MISS') + lg.datea AS last_chg_ts,
                       a.ord_rcvd_ts, b.shpidb AS rlse_ts
                  FROM ordp120b b, ordp100a a, sysp296a lg
                 WHERE b.div_part = i_div_part
                   AND b.statb IN('R', 'A')
                   AND lg.div_part = b.div_part
                   AND lg.ordnoa = b.ordnob
                   AND lg.linea = b.lineb
                   AND lg.fldnma = 'ORDQTB'
                   AND lg.florga <> lg.flchga
                   AND a.div_part = b.div_part
                   AND a.ordnoa = b.ordnob
                   AND a.stata IN('P', 'R', 'A')
                   AND (a.load_depart_sid, b.shpidb) IN(
                         SELECT ld.load_depart_sid, TO_CHAR(r.rlse_ts, 'YYYYMMDDHH24MISS')
                           FROM rlse_op1z r, load_depart_op1f ld
                          WHERE r.div_part = i_div_part
                            AND 'RLSECMP' = (SELECT DISTINCT FIRST_VALUE(rl.typ_id) OVER(ORDER BY rl.seq_of_events DESC)
                                                        FROM rlse_typ_dmn_op9z rtd, rlse_log_op2z rl
                                                       WHERE rtd.seq > -1
                                                         AND rtd.parnt_typ = 'RLSE'
                                                         AND rl.div_part = r.div_part
                                                         AND rl.rlse_id = r.rlse_id
                                                         AND rl.typ_id = rtd.typ_id)
                            AND r.ord_ln_cnt > 0
                            AND r.test_bil_cd = '~'
                            AND r.end_ts >= l_last_run_ts
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND EXISTS(SELECT 1
                                         FROM rlse_log_op2z rl
                                        WHERE rl.div_part = r.div_part
                                          AND rl.rlse_id = r.rlse_id
                                          AND rl.typ_id = 'LOAD'
                                          AND rl.val = ld.load_num))
              UNION ALL
              SELECT   a.custa AS cust_id, b.orditb AS catlg_num, b.ordnob AS ord_num, b.lineb AS ord_ln,
                       b.orgqtb AS orig_qty, b.maxqtb AS qty_fro, b.orgqtb AS qty_to, 'COMET' AS rsn_cd,
                       'COMET' AS user_id, a.ord_rcvd_ts AS last_chg_ts, a.ord_rcvd_ts, b.shpidb AS rlse_ts
                  FROM ordp120b b, ordp100a a
                 WHERE b.div_part = i_div_part
                   AND b.statb IN('R', 'A')
                   AND b.bymaxb = '1'
                   AND b.orgqtb = b.ordqtb
                   AND b.maxqtb > b.orgqtb
                   AND a.div_part = b.div_part
                   AND a.ordnoa = b.ordnob
                   AND (a.load_depart_sid, b.shpidb) IN(
                         SELECT ld.load_depart_sid, TO_CHAR(r.rlse_ts, 'YYYYMMDDHH24MISS')
                           FROM rlse_op1z r, load_depart_op1f ld
                          WHERE r.div_part = i_div_part
                            AND 'RLSECMP' = (SELECT DISTINCT FIRST_VALUE(rl.typ_id) OVER(ORDER BY rl.seq_of_events DESC)
                                                        FROM rlse_typ_dmn_op9z rtd, rlse_log_op2z rl
                                                       WHERE rtd.seq > -1
                                                         AND rtd.parnt_typ = 'RLSE'
                                                         AND rl.div_part = r.div_part
                                                         AND rl.rlse_id = r.rlse_id
                                                         AND rl.typ_id = rtd.typ_id)
                            AND r.ord_ln_cnt > 0
                            AND r.test_bil_cd = '~'
                            AND r.end_ts >= l_last_run_ts
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND EXISTS(SELECT 1
                                         FROM rlse_log_op2z rl
                                        WHERE rl.div_part = r.div_part
                                          AND rl.rlse_id = r.rlse_id
                                          AND rl.typ_id = 'LOAD'
                                          AND rl.val = ld.load_num))
              ORDER BY cust_id, catlg_num, ord_num, ord_ln, last_chg_ts) x;

      IF l_t_rpt_lns.COUNT > 0 THEN
        logs.dbg('Write');
        write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
        logs.dbg('FTP to mainframe');
        op_ftp_sp(l_div_id, l_file_nm, l_c_rmt_file);
      END IF;   -- l_t_rpt_lns.COUNT > 0

      logs.dbg('Set Last Run TS');
      op_parms_pk.merge_sp(i_div_part,
                           op_const_pk.prm_qty_audit_last_ts,
                           op_parms_pk.g_c_dt,
                           TO_CHAR(l_c_curr_run_ts, 'YYYYMMDDHH24MISS'),
                           'QTYAUDIT'
                          );
    END IF;   -- l_last_run_ts IS NOT NULL

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qtyaudit,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qtyaudit,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END qtyaudit_sp;

  /*
  ||----------------------------------------------------------------------------
  || SUB_MAINT_SP
  ||  Process Sub Maintenance
  ||  Calls OP_MAINTAIN_SUBS_PK.MAINT_SUB_SP which applies table changes and
  ||  adjusts order lines as necessary.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/28/09 | rhalpai | Original - created for PIR4342
  ||----------------------------------------------------------------------------
  */
  PROCEDURE sub_maint_sp(
    i_actn_cd    IN      VARCHAR2,
    i_div_part   IN      NUMBER,
    i_cls_typ    IN      VARCHAR2,
    i_cls_id     IN      VARCHAR2,
    i_catlg_num  IN      NUMBER,
    i_sub_typ    IN      VARCHAR2,
    i_sub_item   IN      NUMBER,
    i_qty_fctor  IN      NUMBER,
    i_start_dt   IN      DATE,
    i_end_dt     IN      DATE,
    i_user_id    IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2
  ) IS
  BEGIN
    op_maintain_subs_pk.maint_sub_sp(i_actn_cd,
                                     i_div_part,
                                     i_cls_typ,
                                     i_cls_id,
                                     i_catlg_num,
                                     i_sub_typ,
                                     i_sub_item,
                                     i_qty_fctor,
                                     i_start_dt,
                                     i_end_dt,
                                     i_user_id,
                                     o_err_msg
                                    );
  EXCEPTION
    WHEN OTHERS THEN
      o_err_msg := 'Error in call to OP_MAINTAIN_SUBS_PK.MAINT_SUB_SP!';
  END sub_maint_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDRCMPC_SP
  ||  Interface for maintaining Vendor Compliance Customer info
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/22/10 | rhalpai | Original - created for PIR8216
  || 03/03/10 | rhalpai | Added logic for new columns, BEG_DT and END_DT.
  ||                    | PIR8099
  || 03/30/10 | rhalpai | Added logic to check for existence of REF msg to
  ||                    | control whether to clear the table before adding rows.
  ||                    | Since the REF msg will always be the first msg from
  ||                    | the mainframe, this will handle the situation where
  ||                    | a group of msgs are processed as multiple groups due
  ||                    | to a lag in time they are received on the local MQ
  ||                    | queue. IM577692
  || 09/21/10 | rhalpai | Changed logic to use IS_REF_MSG_FN. IM606867
  || 02/29/12 | rhalpai | Remove REF logic and change to use merge statement.
  ||                    | PIR6682
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndrcmpc_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.VNDRCMPC_SP';
    lar_parm                logs.tar_parm;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)   := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)   := 'VNDRCMPC';
    l_is_msg_found          BOOLEAN;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpc,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Merge');
      MERGE INTO vndr_cmp_cust_op2l c
           USING (SELECT SUBSTR(g.mq_msg_data, 41, 3) AS actn_cd, TO_NUMBER(SUBSTR(g.mq_msg_data, 54, 9)) AS prof_id,
                         TO_NUMBER(SUBSTR(g.mq_msg_data, 63, 5)) AS div_part, SUBSTR(g.mq_msg_data, 68, 8) AS cust_id,
                         TO_NUMBER(SUBSTR(g.mq_msg_data, 76, 5)) AS cmp_qty,
                         TO_DATE(SUBSTR(g.mq_msg_data, 81, 10), 'YYYY-MM-DD') AS beg_dt,
                         TO_DATE(SUBSTR(g.mq_msg_data, 91, 10), 'YYYY-MM-DD') AS end_dt
                    FROM mclane_mq_get g
                   WHERE g.div_part = i_div_part
                     AND g.mq_msg_id = l_c_mq_msg_id
                     AND SUBSTR(g.mq_msg_data, 3, 8) = l_c_msg_id
                     AND g.mq_msg_status = g_c_wrk
                  ORDER BY g.mq_get_id) x
              ON (    c.prof_id = x.prof_id
                  AND c.div_part = x.div_part
                  AND c.cust_id = x.cust_id)
        WHEN MATCHED THEN
          UPDATE
             SET c.cmp_qty = x.cmp_qty, c.beg_dt = x.beg_dt, c.end_dt = x.end_dt
          DELETE
           WHERE x.actn_cd = 'DEL'
      WHEN NOT MATCHED THEN
          INSERT(prof_id, div_part, cust_id, cmp_qty, beg_dt, end_dt)
          VALUES(x.prof_id, x.div_part, x.cust_id, x.cmp_qty, x.beg_dt, x.end_dt);
      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpc,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpc,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END vndrcmpc_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDRCMPI_SP
  ||  Interface for maintaining Vendor Compliance Item info
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/22/10 | rhalpai | Original - created for PIR8216
  || 03/30/10 | rhalpai | Added logic to check for existence of REF msg to
  ||                    | control whether to clear the table before adding rows.
  ||                    | Since the REF msg will always be the first msg from
  ||                    | the mainframe, this will handle the situation where
  ||                    | a group of msgs are processed as multiple groups due
  ||                    | to a lag in time they are received on the local MQ
  ||                    | queue. IM577692
  || 09/21/10 | rhalpai | Changed logic to use IS_REF_MSG_FN. IM606867
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndrcmpi_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.VNDRCMPI_SP';
    lar_parm                logs.tar_parm;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)   := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)   := 'VNDRCMPI';
    l_is_msg_found          BOOLEAN;
    l_ref_sw                VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpi,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Check if Trunc is Required');
      l_ref_sw := is_ref_msg_fn(i_div_part, l_c_msg_id);

      IF l_ref_sw = 'Y' THEN
        logs.dbg('Trunc');
        truncate_table_sp('VNDR_CMP_ITEM_OP1L');
      END IF;   -- l_ref_sw = 'Y'

      logs.dbg('Add');

      INSERT INTO vndr_cmp_item_op1l
                  (prof_id, catlg_num, parnt_item, priorty)
        SELECT TO_NUMBER(SUBSTR(g.mq_msg_data, 54, 9)) AS prof_id, TO_NUMBER(SUBSTR(g.mq_msg_data, 63, 9)) AS catlg_num,
               TO_NUMBER(SUBSTR(g.mq_msg_data, 72, 9)) AS parnt_item, TO_NUMBER(SUBSTR(g.mq_msg_data, 81, 2))
                                                                                                             AS priorty
          FROM mclane_mq_get g
         WHERE g.div_part = i_div_part
           AND g.mq_msg_id = l_c_mq_msg_id
           AND SUBSTR(g.mq_msg_data, 3, 8) = l_c_msg_id
           AND g.mq_msg_status = g_c_wrk;

      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpi,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpi,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END vndrcmpi_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDRCMPQ_SP
  ||  Interface for maintaining Vendor Compliance Customer Item Qty.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/03/10 | rhalpai | Original - created for PIR8099
  || 03/30/10 | rhalpai | Added logic to check for existence of REF msg to
  ||                    | control whether to clear the table before adding rows.
  ||                    | Since the REF msg will always be the first msg from
  ||                    | the mainframe, this will handle the situation where
  ||                    | a group of msgs are processed as multiple groups due
  ||                    | to a lag in time they are received on the local MQ
  ||                    | queue. IM577692
  || 09/21/10 | rhalpai | Changed logic to use IS_REF_MSG_FN. IM606867
  || 02/29/12 | rhalpai | Replace REF logic with removal of all rows for
  ||                    | div/cust and change to use merge statement. PIR6682
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndrcmpq_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.VNDRCMPQ_SP';
    lar_parm                logs.tar_parm;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)   := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)   := 'VNDRCMPQ';
    l_is_msg_found          BOOLEAN;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpq,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Remove');

      DELETE FROM vndr_cmp_qty_op4l q
            WHERE (q.div_part, q.cust_id) IN(SELECT TO_NUMBER(SUBSTR(g.mq_msg_data, 63, 5)) AS div_part,
                                                    SUBSTR(g.mq_msg_data, 68, 8) AS cust_id
                                               FROM mclane_mq_get g
                                              WHERE g.div_part = i_div_part
                                                AND g.mq_msg_id = l_c_mq_msg_id
                                                AND SUBSTR(g.mq_msg_data, 3, 8) = l_c_msg_id
                                                AND SUBSTR(g.mq_msg_data, 41, 3) = 'REF'
                                                AND g.mq_msg_status = g_c_wrk);

      logs.dbg('Merge');
      MERGE INTO vndr_cmp_qty_op4l q
           USING (SELECT SUBSTR(g.mq_msg_data, 41, 3) AS actn_cd, TO_NUMBER(SUBSTR(g.mq_msg_data, 54, 9)) AS prof_id,
                         TO_NUMBER(SUBSTR(g.mq_msg_data, 63, 5)) AS div_part, SUBSTR(g.mq_msg_data, 68, 8) AS cust_id,
                         TO_NUMBER(SUBSTR(g.mq_msg_data, 76, 9)) AS parnt_item,
                         TO_NUMBER(SUBSTR(g.mq_msg_data, 85, 5)) AS cmp_qty
                    FROM mclane_mq_get g
                   WHERE g.div_part = i_div_part
                     AND g.mq_msg_id = l_c_mq_msg_id
                     AND SUBSTR(g.mq_msg_data, 3, 8) = l_c_msg_id
                     AND g.mq_msg_status = g_c_wrk
                   ORDER BY g.mq_get_id) x
              ON (    q.prof_id = x.prof_id
                  AND q.div_part = x.div_part
                  AND q.cust_id = x.cust_id
                  AND q.parnt_item = x.parnt_item)
        WHEN MATCHED THEN
          UPDATE
             SET q.cmp_qty = x.cmp_qty
          DELETE
           WHERE x.actn_cd = 'DEL'
      WHEN NOT MATCHED THEN
          INSERT(prof_id, div_part, cust_id, parnt_item, cmp_qty)
          VALUES(x.prof_id, x.div_part, x.cust_id, x.parnt_item, x.cmp_qty);
      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpq,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpq,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END vndrcmpq_sp;

  /*
  ||----------------------------------------------------------------------------
  || VNDRCMPP_SP
  ||  Interface for maintaining Vendor Compliance Profile.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/15/10 | rhalpai | Original for PIR8936
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndrcmpp_sp(
    i_div_part     IN  NUMBER,
    i_mq_msg_data  IN  VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.VNDRCMPP_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div      VARCHAR2(2),
      actn_cd  VARCHAR2(3),
      prof_id  NUMBER,
      descr    vndr_cmp_prof_op3l.descr%TYPE,
      pct      NUMBER,
      typ      vndr_cmp_prof_op3l.typ%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.prof_id := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.descr := RTRIM(SUBSTR(i_msg_data, 63, 40));
      l_r_parsed.pct := TO_NUMBER(SUBSTR(i_msg_data, 103, 2) || '.' || SUBSTR(i_msg_data, 105, 3));
      l_r_parsed.typ := SUBSTR(i_msg_data, 108, 3);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM vndr_cmp_prof_op3l p
            WHERE p.prof_id = i_r_msg.prof_id;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO vndr_cmp_prof_op3l p
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    p.prof_id = i_r_msg.prof_id
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET p.descr = i_r_msg.descr, p.pct = i_r_msg.pct, p.typ = i_r_msg.typ
        WHEN NOT MATCHED THEN
          INSERT(prof_id, descr, pct, typ)
          VALUES(i_r_msg.prof_id, i_r_msg.descr, i_r_msg.pct, i_r_msg.typ);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpp,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');

    CASE
      WHEN l_r_msg.actn_cd = g_c_del THEN
        logs.dbg('Remove Entry');
        del_sp(l_r_msg);
      WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
        logs.dbg('Add/Chg Entry');
        merge_sp(l_r_msg);
    END CASE;

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpp,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_vndrcmpp,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END vndrcmpp_sp;

  /*
  ||----------------------------------------------------------------------------
  || EDICANCL_SP
  ||  Attempt to cancel matching order line and return MQ msg to the mainframe
  ||  for successfully cancelled order lines.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/09/10 | rhalpai | Original - created for PIR9562
  || 02/04/11 | rhalpai | Add logic to cancel order header when last detail
  ||                    | line has been cancelled. IM-004248
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Change logic to pad GENTOMF MQ msgs to 250 characters.
  ||                    | PIR11910
  || 12/08/15 | rhalpai | Add DivPart in call to OP_SYSP296A_PK.INS_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE edicancl_sp(
    i_div_part     IN  NUMBER,
    i_mq_msg_data  IN  VARCHAR2
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.EDICANCL_SP';
    lar_parm             logs.tar_parm;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div             VARCHAR2(2),
      cust_id         sysp200c.acnoc%TYPE,
      po_num          ordp100a.cpoa%TYPE,
      item_pass_area  ordp120b.itpasb%TYPE
    );

    l_r_msg              l_rt_msg;
    l_cancl_sw           VARCHAR2(1)   := 'N';

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.cust_id := SUBSTR(i_msg_data, 54, 8);
      l_r_parsed.po_num := RTRIM(SUBSTR(i_msg_data, 62, 30));
      l_r_parsed.item_pass_area := RTRIM(SUBSTR(i_msg_data, 92, 20));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE log_sp(
      i_t_ord_nums  IN  type_ntab,
      i_t_ord_lns   IN  type_ntab
    ) IS
    BEGIN
      IF i_t_ord_lns.COUNT > 0 THEN
        FOR i IN i_t_ord_lns.FIRST .. i_t_ord_lns.LAST LOOP
          op_sysp296a_pk.ins_sp(i_div_part,
                                i_t_ord_nums(i),
                                i_t_ord_lns(i),
                                'EDICANCL_SP',
                                'ORDP120B',
                                'STATB',
                                'O',
                                'C',
                                'C',
                                'RCANC7',
                                'EDICANCL',
                                'CANCEL EDI ORDLN'
                               );
        END LOOP;
      END IF;   -- i_t_ord_lns.COUNT > 0
    END log_sp;

    PROCEDURE cancl_sp(
      i_r_msg     IN      l_rt_msg,
      o_cancl_sw  OUT     VARCHAR2
    ) IS
      l_t_ord_nums      type_ntab;
      l_t_ord_lns       type_ntab;
      l_t_ord_nums_unq  type_ntab;
    BEGIN
      logs.dbg('Cancel Ord Dtl');

      UPDATE    ordp120b b
            SET b.statb = 'C'
          WHERE b.div_part = i_div_part
            AND b.statb = 'O'
            AND b.ordnob IN(SELECT a.ordnoa
                              FROM ordp100a a
                             WHERE a.div_part = i_div_part
                               AND a.custa = i_r_msg.cust_id
                               AND a.cpoa = i_r_msg.po_num
                               AND a.dsorda = 'R'
                               AND a.stata IN('O', 'P'))
            AND b.itpasb = i_r_msg.item_pass_area
      RETURNING         b.ordnob, b.lineb
      BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns;

      IF l_t_ord_nums.COUNT > 0 THEN
        l_t_ord_nums_unq := SET(l_t_ord_nums);
        o_cancl_sw := 'Y';
        logs.dbg('Log Cancel');
        log_sp(l_t_ord_nums, l_t_ord_lns);
        logs.dbg('Cancel Hdr for Last Dtl Line');
        FORALL i IN l_t_ord_nums_unq.FIRST .. l_t_ord_nums_unq.LAST
          UPDATE ordp100a a
             SET a.stata = 'C'
           WHERE a.div_part = i_div_part
             AND a.ordnoa = l_t_ord_nums_unq(i)
             AND a.stata <> 'C'
             AND NOT EXISTS(SELECT 1
                              FROM ordp120b b
                             WHERE b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND b.statb <> 'C');
      END IF;   -- l_t_ord_nums.COUNT > 0
    END cancl_sp;

    PROCEDURE send_to_mf_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
      l_c_msg_id  CONSTANT VARCHAR2(8) := 'GENTOMF';
      l_rc                 NUMBER;
    BEGIN
      logs.dbg('Add Put Msg');

      INSERT INTO mclane_mq_put
                  (mq_msg_id, div_part, mq_msg_status,
                   mq_msg_data
                  )
           VALUES (l_c_msg_id, i_div_part, 'OPN',
                   RPAD(RPAD(i_r_msg.div || l_c_msg_id, 53)
                        || i_r_msg.cust_id
                        || rpad_fn(i_r_msg.po_num, 30)
                        || rpad_fn(i_r_msg.item_pass_area, 20),
                        250
                       )
                  );

      -- Must commit prior to processing MQ msgs to allow processing in separate thread
      COMMIT;
      logs.dbg('Process MQ Put Msg');
      op_mq_message_pk.mq_put_sp(l_c_msg_id, i_r_msg.div, NULL, l_rc);
      excp.assert((l_rc = 0), 'Failed to put msgs to MQ');
    END send_to_mf_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MqMsgData', i_mq_msg_data);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_edicancl,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    logs.dbg('Parse MQ Message Data');
    l_r_msg := parse_msg_fn(i_mq_msg_data);
    logs.dbg('Process Msg');
    cancl_sp(l_r_msg, l_cancl_sw);

    IF l_cancl_sw = 'Y' THEN
      logs.dbg('Send Cancel Status to MF');
      send_to_mf_sp(l_r_msg);
    END IF;   -- l_cancl_sw = 'Y'

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_edicancl,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_edicancl,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_edicancl,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm);
  END edicancl_sp;

  /*
  ||----------------------------------------------------------------------------
  || ITEMGRP_SP
  ||  Interface for maintaining Item Grouping.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/25/12 | rhalpai | Original for PIR10620
  || 02/14/18 | rhalpai | All logic to TRIM trailing spaces from data for CLS_ID.
  ||                    | PIR17722
  || 08/01/22 | rhalpai | Add order validation check for VAPCBD (OP_ORDER_VALIDATION_PK.CHECK_VAPCBD_FOR_ITEM_SP). PIR22000
  ||----------------------------------------------------------------------------
  */
  PROCEDURE itemgrp_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm               := 'OP_MESSAGES_PK.ITEMGRP_SP';
    lar_parm                logs.tar_parm;
    l_div_id                div_mstr_di1d.div_id%TYPE;
    l_c_mq_msg_id  CONSTANT VARCHAR2(7)                 := 'QOPMSGS';
    l_c_msg_id     CONSTANT VARCHAR2(8)                 := 'ITEMGRP';
    l_is_msg_found          BOOLEAN;

    PROCEDURE maint_sp IS
      l_t_catlg_nums        type_stab;
      l_c_sysdate  CONSTANT DATE      := SYSDATE;
    BEGIN
      logs.dbg('Apply Table Maintenance');
      MERGE INTO item_grp_op2e i
           USING (SELECT g.div_part, SUBSTR(g.mq_msg_data, 41, 3) AS actn_cd,
                         RTRIM(SUBSTR(g.mq_msg_data, 54, 10)) AS cls_id,
                         SUBSTR(g.mq_msg_data, 64, 6) AS cls_typ,
                         LPAD(TRIM(SUBSTR(g.mq_msg_data, 70, 6)), 6, '0') AS catlg_num
                    FROM mclane_mq_get g
                   WHERE g.div_part = i_div_part
                     AND g.mq_msg_status = g_c_wrk
                     AND g.mq_msg_id = l_c_mq_msg_id
                     AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id
                   ORDER BY g.mq_get_id) x
              ON (    i.div_part = x.div_part
                  AND i.cls_id = x.cls_id
                  AND i.cls_typ = x.cls_typ
                  AND i.catlg_num = x.catlg_num)
        WHEN MATCHED THEN
          UPDATE
             SET i.last_chg_ts = l_c_sysdate
          DELETE
           WHERE x.actn_cd = 'DEL'
      WHEN NOT MATCHED THEN
          INSERT(div_part, cls_id, cls_typ, catlg_num, last_chg_ts)
          VALUES(x.div_part, x.cls_id, x.cls_typ, x.catlg_num, l_c_sysdate);

      SELECT LPAD(TRIM(SUBSTR(g.mq_msg_data, 70, 6)), 6, '0') AS catlg_num
      BULK COLLECT INTO l_t_catlg_nums
        FROM mclane_mq_get g
       WHERE g.div_part = i_div_part
         AND g.mq_msg_status = g_c_wrk
         AND g.mq_msg_id = l_c_mq_msg_id
         AND TRIM(SUBSTR(g.mq_msg_data, 3, 8)) = l_c_msg_id;

      COMMIT;
      logs.dbg('Check RegBev for Item');
      op_order_validation_pk.check_reg_bev_for_item_sp(l_div_id, l_t_catlg_nums);
      logs.dbg('Check Vape/CBD for Item');
      op_order_validation_pk.check_vapcbd_for_item_sp(l_div_id, l_t_catlg_nums);
    END maint_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_itemgrp,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                i_div_part
                                               );
    l_div_id := div_pk.div_id_fn(i_div_part);
    -- This module is called from QOPMSGS_SP for each message, however,
    -- all will be processed with the first msg so we must prevent reprocessing
    logs.dbg('Tag Msgs to Process');
    upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_opn, g_c_wrk, l_is_msg_found);

    IF l_is_msg_found THEN
      logs.dbg('Process Msgs');
      maint_sp;
      logs.dbg('Change Status of Tagged Msgs to Complete');
      upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_compl, l_is_msg_found);
    END IF;   -- l_is_msg_found

    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_itemgrp,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                i_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF l_is_msg_found THEN
        upd_msg_stats_sp(i_div_part, l_c_msg_id, g_c_wrk, g_c_prb, l_is_msg_found);
      END IF;   -- l_is_msg_found

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_itemgrp,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  i_div_part
                                                 );
      logs.err(lar_parm, NULL, FALSE);
  END itemgrp_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || QITEM09_SP
  ||  These messages replace the current inventory with a new value.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/06/07 | rhalpai | Created QITEM09_SP from QITEM_SP.
  ||                    | Added Process Control logic for starting QITEM09.
  ||                    | Added Process Control resets for QITEM19 and CIG_INV
  ||                    | when processing FINISHED msg. IM278669
  || 05/20/10 | rhalpai | Added logic to create Allocation event as necessary
  ||                    | upon completion of inventory refresh. PIR8377
  || 10/11/10 | rhalpai | Add logic when processing finished msg to continue
  ||                    | CigOPRefresh process when its process control is
  ||                    | active. PIR8531
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 01/04/11 | rhalpai | Change logic to only continue Cig Inv Refresh for
  ||                    | finished msgs with QITEM19. IM638862
  || 03/16/11 | rhalpai | Change logic to parse and use zone from msg instead
  ||                    | of slot. PIR0024
  || 08/29/11 | rhalpai | Rename call for logging transaction from
  ||                    | INS_WHSP900R_SP to INS_INV_TRAN_SP. PIR7990
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 01/17/12 | rhalpai | Add logic to first process any open QITEM13 slot
  ||                    | changes. Mark any open QITEM10 inventory adjustments
  ||                    | to complete status before processing QITEM09
  ||                    | inventory replacement msgs. Change Finish logic to
  ||                    | call new IS_RLSE_INIT_FN function. Add logic to call
  ||                    | MERGE_INV_SP to handle new items or changed slots.
  ||                    | PIR10475
  || 09/10/13 | rhalpai | Change logic to only start SetRelease when finished
  ||                    | msg contains SETRLSE. IM-117950
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem09_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    -- Generic
    l_c_module   CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM09_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_msg_id   CONSTANT VARCHAR2(8)                        := 'QITEM09';
    l_c_sysdate  CONSTANT DATE                               := SYSDATE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      item_num   whsp300c.itemc%TYPE,
      uom        whsp300c.uomc%TYPE,
      aisl       whsp300c.aislc%TYPE,
      bin        whsp300c.binc%TYPE,
      levl       whsp300c.levlc%TYPE,
      qty        NUMBER,
      whse_zone  whsp300c.taxjrc%TYPE
    );

    l_t_prbs              g_tt_prbs;
    l_mq_msg_stat         mclane_mq_get.mq_msg_status%TYPE;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 63, 3);
      l_r_parsed.aisl := SUBSTR(i_msg_data, 66, 2);
      l_r_parsed.bin := SUBSTR(i_msg_data, 68, 3);
      l_r_parsed.levl := SUBSTR(i_msg_data, 71, 2);
      l_r_parsed.qty := NVL(string_to_num_fn(SUBSTR(i_msg_data, 73, 9)), 0);
      -- 1-byte filler
      l_r_parsed.whse_zone := RTRIM(SUBSTR(i_msg_data, 83, 3));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION is_finished_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN BOOLEAN IS
    BEGIN
      RETURN(UPPER(RTRIM(SUBSTR(i_msg_data, 54, 8))) = 'FINISHED');
    END is_finished_msg_fn;

    FUNCTION finished_msg_typ_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN(UPPER(TRIM(SUBSTR(i_msg_data, 63, 8))));
    END finished_msg_typ_fn;

    FUNCTION finished_msg_pgmid_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN(UPPER(TRIM(SUBSTR(i_msg_data, 71, 7))));
    END finished_msg_pgmid_fn;

    PROCEDURE finish_sp(
      i_div_part  IN  NUMBER,
      i_div       IN  VARCHAR2,
      i_msg_data  IN  VARCHAR2,
      i_user_id   IN  VARCHAR2
    ) IS
      l_set_rlse_sw  VARCHAR2(1) := 'N';
    BEGIN
      IF finished_msg_typ_fn(i_msg_data) = 'QITEM19' THEN
        logs.dbg('Set Inventory Refresh Process Inactive');
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem19,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    i_div_part
                                                   );

        IF (    is_rlse_init_fn(i_div_part) = 'Y'
            AND finished_msg_pgmid_fn(i_msg_data) = 'SETRLSE') THEN
          l_set_rlse_sw := 'Y';
        END IF;

        logs.dbg('Build and Execute Event to Continue Inventory Refresh');
        cig_bb100_op_refresh_procs_pk.build_evnt_cont_inv_refresh(i_div, l_set_rlse_sw);
        logs.dbg('Check for Initiation of SetRelease');

        IF l_set_rlse_sw = 'Y' THEN
          logs.dbg('Start Allocation');
          op_allocate_pk.start_alloc_sp(i_div, 'QITEM09');
        END IF;   -- l_set_rlse_sw = 'Y'
      ELSE
        logs.dbg('Set Cig Inventory Update Process Inactive');
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_cig_inv,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    i_div_part
                                                   );
      END IF;   -- finished_msg_typ_fn(i_msg_data) = 'QITEM19'
    END finish_sp;

    PROCEDURE process_msg_sp(
      i_div_part     IN             NUMBER,
      i_div          IN             VARCHAR2,
      i_msg_data     IN             VARCHAR2,
      i_user_id      IN             VARCHAR2,
      io_t_prbs      IN OUT NOCOPY  g_tt_prbs,
      o_mq_msg_stat  OUT            VARCHAR2
    ) IS
      l_r_msg            l_rt_msg;
      l_inv_sync_err_sw  VARCHAR2(1);
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Check for Finished Msg');

      IF is_finished_msg_fn(i_msg_data) THEN
        logs.dbg('Process for Finish Msg');
        finish_sp(i_div_part, i_div, i_msg_data, i_user_id);
      ELSE
        logs.dbg('Parse MQ Msg Data');
        l_r_msg := parse_msg_fn(i_msg_data);
        logs.dbg('Check for Inv Sync Problem');

        SELECT MAX('Y')
          INTO l_inv_sync_err_sw
          FROM whsp300c w
         WHERE w.div_part = i_div_part
           AND w.itemc = l_r_msg.item_num
           AND w.uomc = l_r_msg.uom
           AND w.zonec = i_div
           AND NVL(w.taxjrc, ' ') = NVL(l_r_msg.whse_zone, ' ')
           AND l_r_msg.qty < w.qalc;

        IF l_inv_sync_err_sw = 'Y' THEN
          o_mq_msg_stat := g_c_prb;
          logs.dbg('Log Inventory Sync Problem');
          add_problem_sp(io_t_prbs,
                         i_div,
                         l_c_msg_id,
                         'Inventory sync problem',
                         lpad_fn(i_div, 2)
                         || lpad_fn(l_r_msg.item_num, 9)
                         || rpad_fn(l_r_msg.uom, 3)
                         || rpad_fn(l_r_msg.whse_zone, 3)
                         || l_r_msg.aisl
                         || l_r_msg.bin
                         || l_r_msg.levl
                        );
        END IF;   -- v_inv_sync_err_sw = 'Y'

        logs.dbg('Merge WHSP300C');
        merge_inv_sp(i_div_part,
                     l_r_msg.item_num,
                     l_r_msg.uom,
                     l_r_msg.whse_zone,
                     l_r_msg.aisl,
                     l_r_msg.bin,
                     l_r_msg.levl,
                     l_r_msg.qty
                    );
        logs.dbg('Add Inventory Transaction');
        ins_inv_tran_sp(i_div_part,
                        l_r_msg.item_num,
                        l_r_msg.uom,
                        l_r_msg.whse_zone,
                        l_r_msg.aisl,
                        l_r_msg.bin,
                        l_r_msg.levl,
                        l_r_msg.qty,
                        l_c_msg_id
                       );
      END IF;   -- is_finished_msg_fn(i_msg_data)
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Process any Outstanding Slot Changes');
--      qitem13_sp(i_div, i_user_id);
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem09);
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem09,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.dbg('Upd Any Open Item InvAdj to Complete');

      UPDATE mclane_mq_get g
         SET g.mq_msg_status = 'CMP',
             g.last_chg_ts = l_c_sysdate
       WHERE g.div_part = l_div_part
         AND g.mq_msg_status = 'OPN'
         AND g.mq_msg_id = 'QITEM10';

      COMMIT;
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, i_div, l_r_msg.mq_msg_data, i_user_id, l_t_prbs, l_mq_msg_stat);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;

      IF l_t_prbs.COUNT > 0 THEN
        logs.dbg('Notify');
        notify_sp(l_t_prbs);
      END IF;   -- l_t_prbs.COUNT > 0

      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem09,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem09,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem09_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM10_SP
  ||  These messages either increase or decrease the quantity on the slot.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/06/07 | rhalpai | Created QITEM10_SP from QITEM_SP. IM278669
  || 01/04/11 | rhalpai | Change logic to process reset CIGINV process control
  ||                    | when finished msgs are found. IM638862
  || 03/16/11 | rhalpai | Change logic to parse and use catlg_num,rns_typ,
  ||                    | tran_typ,zone from msg. For each record for USB zone
  ||                    | capture inventory changes. After all MQ msgs are
  ||                    | processed, if USB inventory changes were captured
  ||                    | then call new CIG_BB105_OP_REFRESH_PROCS_PK.INV_ADJUST
  ||                    | with this info. PIR0024
  || 08/29/11 | rhalpai | Rename call for logging transaction from
  ||                    | INS_WHSP900R_SP to INS_INV_TRAN_SP. PIR7990
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem10_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                             := 'OP_MESSAGES_PK.QITEM10_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                               := 'QITEM10';

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      item_num   whsp300c.itemc%TYPE,
      uom        whsp300c.uomc%TYPE,
      catlg_num  sawp505e.catite%TYPE,
      aisl       whsp300c.aislc%TYPE,
      bin        whsp300c.binc%TYPE,
      levl       whsp300c.levlc%TYPE,
      adj_qty    PLS_INTEGER,
      rsn_cd     VARCHAR2(5),
      tran_cd    VARCHAR2(5),
      whse_zone  whsp300c.taxjrc%TYPE,
      oh_qty     PLS_INTEGER
    );

    l_t_prbs             g_tt_prbs;
    l_mq_msg_status      mclane_mq_get.mq_msg_status%TYPE;
    l_t_usb_inv_adjs     cig_bb105_op_refresh_procs_pk.inv_adj_tab := cig_bb105_op_refresh_procs_pk.inv_adj_tab();

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      IF SUBSTR(i_msg_data, 54, 9) = '000000000' THEN
        -- use "TO" area
        l_r_parsed.item_num := SUBSTR(i_msg_data, 86, 9);
        l_r_parsed.uom := SUBSTR(i_msg_data, 95, 3);
        l_r_parsed.catlg_num := SUBSTR(i_msg_data, 98, 6);
        l_r_parsed.aisl := SUBSTR(i_msg_data, 104, 2);
        l_r_parsed.bin := SUBSTR(i_msg_data, 106, 3);
        l_r_parsed.levl := SUBSTR(i_msg_data, 109, 2);
        l_r_parsed.adj_qty := NVL(string_to_num_fn(SUBSTR(i_msg_data, 111, 7)), 0);
      ELSE
        -- use "FROM" area
        l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
        l_r_parsed.uom := SUBSTR(i_msg_data, 63, 3);
        l_r_parsed.catlg_num := SUBSTR(i_msg_data, 66, 6);
        l_r_parsed.aisl := SUBSTR(i_msg_data, 72, 2);
        l_r_parsed.bin := SUBSTR(i_msg_data, 74, 3);
        l_r_parsed.levl := SUBSTR(i_msg_data, 77, 2);
        l_r_parsed.adj_qty := NVL(string_to_num_fn(SUBSTR(i_msg_data, 79, 7)), 0) * -1;
      END IF;

      l_r_parsed.rsn_cd := RTRIM(SUBSTR(i_msg_data, 118, 5));
      l_r_parsed.tran_cd := RTRIM(SUBSTR(i_msg_data, 123, 5));
      -- 1-byte filler
      l_r_parsed.whse_zone := RTRIM(SUBSTR(i_msg_data, 129, 3));
      l_r_parsed.oh_qty := NVL(string_to_num_fn(SUBSTR(i_msg_data, 132, 9)), 0);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION is_finished_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN BOOLEAN IS
    BEGIN
      RETURN(UPPER(RTRIM(SUBSTR(i_msg_data, 54, 8))) = 'FINISHED');
    END is_finished_msg_fn;

    PROCEDURE finish_sp(
      i_div_part  IN  NUMBER,
      i_user_id   IN  VARCHAR2
    ) IS
    BEGIN
      logs.dbg('Set Cig Inventory Update Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_cig_inv,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  i_div_part
                                                 );
    END finish_sp;

    PROCEDURE process_msg_sp(
      i_div_part        IN             NUMBER,
      i_div             IN             VARCHAR2,
      i_msg_data        IN             VARCHAR2,
      i_user_id         IN             VARCHAR2,
      io_t_prbs         IN OUT NOCOPY  g_tt_prbs,
      io_t_usb_inv_adj  IN OUT NOCOPY  cig_bb105_op_refresh_procs_pk.inv_adj_tab,
      o_mq_msg_stat     OUT            VARCHAR2
    ) IS
      l_r_msg          l_rt_msg;
      l_r_usb_slot     cig_bb105_op_refresh_procs_pk.item_slot_rec;
      l_r_usb_inv_adj  cig_bb105_op_refresh_procs_pk.inv_adj_rec;
      l_r_inv          g_cur_inv%ROWTYPE;
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Check for Finished Msg');

      IF is_finished_msg_fn(i_msg_data) THEN
        logs.dbg('Process for Finish Msg');
        finish_sp(i_div_part, i_user_id);
      ELSE
        logs.dbg('Parse MQ Msg Data');
        l_r_msg := parse_msg_fn(i_msg_data);

        IF l_r_msg.whse_zone = 'USB' THEN
          logs.dbg('Capture Bulk Inv Adjustments for CMS');
          l_r_usb_slot.mcl_item_# := l_r_msg.catlg_num;
          l_r_usb_slot.zone_nm := l_r_msg.whse_zone;
          l_r_usb_slot.slot_nm := l_r_msg.aisl || l_r_msg.bin || l_r_msg.levl;
          l_r_usb_inv_adj.item_slot := l_r_usb_slot;
          l_r_usb_inv_adj.adj_qty := l_r_msg.adj_qty;
          l_r_usb_inv_adj.rsn_cd := l_r_msg.rsn_cd;
          l_r_usb_inv_adj.oh_qty := l_r_msg.oh_qty;
          io_t_usb_inv_adj.EXTEND;
          io_t_usb_inv_adj(io_t_usb_inv_adj.LAST) := l_r_usb_inv_adj;
        END IF;   -- l_r_msg.whse_zone = 'USB'

        logs.dbg('Open Inv Cursor');

        OPEN g_cur_inv(i_div_part, l_r_msg.item_num, l_r_msg.uom, l_r_msg.whse_zone);

        logs.dbg('Fetch Inv Cursor');

        FETCH g_cur_inv
         INTO l_r_inv;

        logs.dbg('Validate');

        IF g_cur_inv%FOUND THEN
          IF (l_r_inv.qavc + l_r_msg.adj_qty) < 0 THEN
            l_r_inv.qavc := l_r_inv.qavc * -1;
          END IF;   -- (l_r_inv.qavc + l_r_msg.adj_qty) < 0

          logs.dbg('Update Inventory');

          UPDATE whsp300c
             SET qohc = qohc + l_r_msg.adj_qty,
                 qavc = qohc + l_r_msg.adj_qty - qalc
           WHERE CURRENT OF g_cur_inv;

          logs.dbg('Insert Inventory Transaction');
          ins_inv_tran_sp(i_div_part,
                          l_r_msg.item_num,
                          l_r_msg.uom,
                          l_r_inv.zonec,
                          l_r_msg.aisl,
                          l_r_msg.bin,
                          l_r_msg.levl,
                          l_r_msg.adj_qty,
                          l_r_msg.rsn_cd || l_r_msg.tran_cd
                         );
        ELSE
          o_mq_msg_stat := g_c_prb;
          logs.dbg('Log Record Not Found');
          add_problem_sp(io_t_prbs,
                         i_div,
                         l_c_msg_id,
                         'Record not found on WHSP300C',
                         lpad_fn(i_div, 2)
                         || lpad_fn(l_r_msg.item_num, 9)
                         || rpad_fn(l_r_msg.uom, 3)
                         || rpad_fn(l_r_msg.whse_zone, 3)
                         || l_r_msg.aisl
                         || l_r_msg.bin
                         || l_r_msg.levl
                        );
        END IF;   -- g_cur_inv%FOUND

        logs.dbg('Close Inv Cursor');

        CLOSE g_cur_inv;
      END IF;   -- is_finished_msg_fn(i_msg_data)
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;

        IF g_cur_inv%ISOPEN THEN
          CLOSE g_cur_inv;
        END IF;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem10);
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem10,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, i_div, l_r_msg.mq_msg_data, i_user_id, l_t_prbs, l_t_usb_inv_adjs, l_mq_msg_status);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_status);
        COMMIT;
      END LOOP;

      IF l_t_prbs.COUNT > 0 THEN
        logs.dbg('Notify');
        notify_sp(l_t_prbs);
      END IF;   -- l_t_prbs.COUNT > 0

      IF l_t_usb_inv_adjs.COUNT > 0 THEN
        logs.dbg('Process Bulk Inv Adjustments in CMS');
        cig_bb105_op_refresh_procs_pk.inv_adjust(i_div, l_t_usb_inv_adjs);
      END IF;   -- l_t_usb_inv_adjs.COUNT > 0

      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem10,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem10,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem10_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM13_SP
  ||  These messages assign an item to a slot or remove the item from the slot.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/19/01 | rhalpai | Original
  || 08/21/01 | rhalpai | Trim v_state and fix references
  || 09/05/01 | rhalpai | Add error handling logic
  || 09/06/01 | rhalpai | Changed to email once with error count fixed problem
  ||                    | to handle nulls with taxjrc
  || 09/28/01 | rhalpai | Modified Add/Change logic to always update and then
  ||                    | insert if no records were found
  || 10/09/01 | rhalpai | Removed hint in "delete from sawp300c"
  || 10/24/01 | rhalpai | Added logic to trap errors for ADD and CHG recs
  || 11/19/01 | rhalpai | Corrected cursor references to external variable
  || 12/10/01 | rhalpai | Changed to populate ZONEC on WHSP300C using division
  ||                    | and removed the lookup the lookup of SAWP505E.SCBCTE
  ||                    | which was previously updating ZONEC.
  || 10/13/03 | rhalpai | Added logic to require an OP Refresh to Cig system
  ||                    | during slot maintenance for cig items.
  || 12/23/03 | Arun    | Added Delete statement to remove LA Cigarettes from
  ||                    | whsp300c with State Slots as taxing jurisdiction.
  || 07/30/04 | rhalpai | Added logic to skip call to require_op_refresh_sp
  ||                    | when Cig System is inventory master.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Redesigned. Removed status parm, added process control,
  ||                    | changed error handler to use standard parm list.
  || 03/16/11 | rhalpai | Change logic to capture the slot maintenance for USB
  ||                    | zone. After all MQ msgs are processed, if USB slot
  ||                    | maintenance was captured the call new
  ||                    | CIG_BB105_OP_REFRESH_PROCS_PK.SLOT_MAINT with this
  ||                    | info. PIR0024
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 01/17/12 | rhalpai | Add logic to manually set ProcessControl active for
  ||                    | QITEM13 when SetRelease has just been initiated. This
  ||                    | will bypass process restrictions from running against
  ||                    | CIG_OP_REFRESH, TEST_BILL. Change logic to call the
  ||                    | new MERGE_INV_SP.PIR10475
  || 02/25/15 | rhalpai | Change logic to replace SELECT from PRCS_CNTL tables
  ||                    | with call to OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP
  ||                    | using exclude list parm. PIR11038
  || 02/25/15 | rhalpai | Change logic to include MSC with cleanup of LAC State
  ||                    | Slots. PIR14791
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem13_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    -- Generic
    l_c_module      CONSTANT typ.t_maxfqnm                                   := 'OP_MESSAGES_PK.QITEM13_SP';
    lar_parm                 logs.tar_parm;
    l_div_part               NUMBER;
    l_c_msg_id      CONSTANT VARCHAR2(7)                                     := 'QITEM13';
    l_excl_list              typ.t_maxvc2;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd   VARCHAR2(3),
      item_num  whsp300c.itemc%TYPE,
      uom       whsp300c.uomc%TYPE,
      jrsdctn   whsp300c.taxjrc%TYPE,
      aisl      whsp300c.aislc%TYPE,
      bin       whsp300c.binc%TYPE,
      levl      whsp300c.levlc%TYPE,
      mcl_item  sawp505e.catite%TYPE
    );

    l_mq_msg_stat            mclane_mq_get.mq_msg_status%TYPE;
    l_t_usb_slot_adjs        cig_bb105_op_refresh_procs_pk.item_slot_adj_tab
                                                                    := cig_bb105_op_refresh_procs_pk.item_slot_adj_tab
                                                                                                                      ();
    l_t_prbs                 g_tt_prbs;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 63, 3);
      l_r_parsed.jrsdctn := RTRIM(SUBSTR(i_msg_data, 66, 3));
      l_r_parsed.aisl := SUBSTR(i_msg_data, 69, 2);
      l_r_parsed.bin := SUBSTR(i_msg_data, 71, 3);
      l_r_parsed.levl := SUBSTR(i_msg_data, 74, 2);
      -- 5-bytes filler
      l_r_parsed.mcl_item := SUBSTR(i_msg_data, 81, 6);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE process_msg_sp(
      i_div_part         IN             NUMBER,
      i_div              IN             VARCHAR2,
      i_msg_data         IN             VARCHAR2,
      io_t_prbs          IN OUT NOCOPY  g_tt_prbs,
      io_t_usb_slot_adj  IN OUT NOCOPY  cig_bb105_op_refresh_procs_pk.item_slot_adj_tab,
      o_mq_msg_stat      OUT            VARCHAR2
    ) IS
      l_r_msg           l_rt_msg;
      l_r_usb_slot      cig_bb105_op_refresh_procs_pk.item_slot_rec;
      l_r_usb_slot_adj  cig_bb105_op_refresh_procs_pk.item_slot_adj_rec;
      l_onh_qty         PLS_INTEGER;
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Parse MQ Msg Data');
      l_r_msg := parse_msg_fn(i_msg_data);

      IF l_r_msg.jrsdctn = 'USB' THEN
        logs.dbg('Capture Bulk Slot Adjustments for CMS');
        l_r_usb_slot.mcl_item_# := l_r_msg.mcl_item;
        l_r_usb_slot.zone_nm := l_r_msg.jrsdctn;
        l_r_usb_slot.slot_nm := l_r_msg.aisl || l_r_msg.bin || l_r_msg.levl;
        l_r_usb_slot_adj.item_slot := l_r_usb_slot;
        l_r_usb_slot_adj.actn_cd := l_r_msg.actn_cd;
        io_t_usb_slot_adj.EXTEND;
        io_t_usb_slot_adj(io_t_usb_slot_adj.LAST) := l_r_usb_slot_adj;
      END IF;   -- l_r_msg.jrsdctn = 'USB'

      IF l_r_msg.actn_cd = 'DEL' THEN
        l_onh_qty := 0;
        logs.dbg('Delete Item');

        DELETE FROM whsp300c
              WHERE div_part = i_div_part
                AND itemc = l_r_msg.item_num
                AND uomc = l_r_msg.uom
                AND aislc = l_r_msg.aisl
                AND binc = l_r_msg.bin
                AND levlc = l_r_msg.levl
                AND NVL(taxjrc, ' ') = NVL(l_r_msg.jrsdctn, ' ')
          RETURNING qohc
               INTO l_onh_qty;

        IF l_onh_qty > 0 THEN
          o_mq_msg_stat := g_c_prb;
          logs.dbg('Log Delete Item with Onhand');
          add_problem_sp(io_t_prbs,
                         i_div,
                         l_c_msg_id,
                         'Delete Item With Onhand.',
                         lpad_fn(i_div, 2)
                         || lpad_fn(l_r_msg.item_num, 9)
                         || rpad_fn(l_r_msg.uom, 3)
                         || l_r_msg.aisl
                         || l_r_msg.bin
                         || l_r_msg.levl
                         || rpad_fn(l_r_msg.jrsdctn, 3)
                         || l_onh_qty
                        );
        END IF;   -- l_onh_qty > 0
      ELSIF l_r_msg.actn_cd IN('ADD', 'CHG') THEN
        logs.dbg('Merge WHSP300C');
        merge_inv_sp(i_div_part,
                     l_r_msg.item_num,
                     l_r_msg.uom,
                     l_r_msg.jrsdctn,
                     l_r_msg.aisl,
                     l_r_msg.bin,
                     l_r_msg.levl
                    );
      END IF;   -- l_r_msg.actn_cd = 'DEL'
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem13);
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);

      IF is_rlse_init_fn(l_div_part) = 'Y' THEN
        -- during SetRelease initiation bypass restrictions for CIG_OP_REFRESH, TEST_BILL
        l_excl_list := 'CIG_OP_REFRESH,TEST_BILL';
      END IF;   -- is_rlse_init_fn(l_div_part) = 'Y'

      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem13,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part,
                                                  l_excl_list
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, i_div, l_r_msg.mq_msg_data, l_t_prbs, l_t_usb_slot_adjs, l_mq_msg_stat);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;

      IF l_t_prbs.COUNT > 0 THEN
        logs.dbg('Notify');
        notify_sp(l_t_prbs);
      END IF;   -- l_t_prbs.COUNT > 0

      IF l_t_usb_slot_adjs.COUNT > 0 THEN
        logs.dbg('Process Bulk Slot Adjustments in CMS');
        cig_bb105_op_refresh_procs_pk.slot_maint(i_div, l_t_usb_slot_adjs);
      END IF;   -- l_t_usb_slot_adjs.COUNT > 0

      logs.dbg('DELETE LAC/MSC in State Slots');

      -- This delete is performed outside the loop and as well after the records
      -- got inserted. The reason is less I/O doing this way.
      DELETE FROM whsp300c w
            WHERE w.div_part = l_div_part
              AND EXISTS(SELECT 1
                           FROM div_item_alt a
                          WHERE a.alt_typ IN('LAC', 'MSC')
                            AND a.div_part = w.div_part
                            AND a.alt_item = w.itemc
                            AND a.alt_uom = w.uomc)
              AND w.taxjrc <> 'USB';

      COMMIT;

      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem13,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem13,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem13_sp;

  /*
  ||----------------------------------------------------------------------------
  || INVMAINT_SP
  ||  Inventory Maintenance
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/03/12 | rhalpai | Original for PIR11057
  || 02/25/15 | rhalpai | Change logic to replace SELECT from PRCS_CNTL tables
  ||                    | with call to OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP
  ||                    | using exclude list parm. PIR11038
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE invmaint_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    -- Generic
    l_c_module      CONSTANT typ.t_maxfqnm                                   := 'OP_MESSAGES_PK.INVMAINT_SP';
    lar_parm                 logs.tar_parm;
    l_div_part               NUMBER;
    l_c_msg_id      CONSTANT VARCHAR2(8)                                     := 'INVMAINT';
    l_excl_list              typ.t_maxvc2;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd    VARCHAR2(3),
      tran_cd    VARCHAR2(3),
      catlg_num  sawp505e.catite%TYPE,
      jrsdctn    whsp300c.taxjrc%TYPE,
      aisl       whsp300c.aislc%TYPE,
      bin        whsp300c.binc%TYPE,
      levl       whsp300c.levlc%TYPE,
      qty_onh    NUMBER,
      qty_from   NUMBER,
      qty_to     NUMBER,
      pgm_id     VARCHAR2(7)
    );

    l_mq_msg_stat            mclane_mq_get.mq_msg_status%TYPE;
    l_t_usb_slot_adjs        cig_bb105_op_refresh_procs_pk.item_slot_adj_tab
                                                                    := cig_bb105_op_refresh_procs_pk.item_slot_adj_tab
                                                                                                                      ();
    l_t_usb_inv_adjs         cig_bb105_op_refresh_procs_pk.inv_adj_tab    := cig_bb105_op_refresh_procs_pk.inv_adj_tab
                                                                                                                      ();
    l_t_prbs                 g_tt_prbs;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.tran_cd := SUBSTR(i_msg_data, 54, 3);
      l_r_parsed.catlg_num := SUBSTR(i_msg_data, 57, 6);
      -- 1-byte filler
      l_r_parsed.jrsdctn := RTRIM(SUBSTR(i_msg_data, 64, 3));
      -- 1-byte filler
      l_r_parsed.aisl := SUBSTR(i_msg_data, 68, 2);
      l_r_parsed.bin := SUBSTR(i_msg_data, 70, 3);
      l_r_parsed.levl := SUBSTR(i_msg_data, 73, 2);
      -- 1-byte filler
      l_r_parsed.qty_onh := NVL(string_to_num_fn(SUBSTR(i_msg_data, 76, 9)), 0);
      -- 1-byte filler
      l_r_parsed.qty_from := NVL(string_to_num_fn(SUBSTR(i_msg_data, 86, 9)), 0);
      -- 1-byte filler
      l_r_parsed.qty_to := NVL(string_to_num_fn(SUBSTR(i_msg_data, 96, 9)), 0);
      -- 1-byte filler
      l_r_parsed.pgm_id := SUBSTR(i_msg_data, 106, 7);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION is_finished_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN BOOLEAN IS
    BEGIN
      RETURN(UPPER(RTRIM(SUBSTR(i_msg_data, 54, 8))) = 'FINISHED');
    END is_finished_msg_fn;

    FUNCTION finished_msg_typ_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN(UPPER(TRIM(SUBSTR(i_msg_data, 63, 8))));
    END finished_msg_typ_fn;

    PROCEDURE finish_sp(
      i_div_part  IN  NUMBER,
      i_div       IN  VARCHAR2,
      i_msg_data  IN  VARCHAR2,
      i_user_id   IN  VARCHAR2
    ) IS
    BEGIN
      IF finished_msg_typ_fn(i_msg_data) = 'QITEM19' THEN
        logs.dbg('Set Inventory Refresh Process Inactive');
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem19,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    i_div_part
                                                   );
        logs.dbg('Build and Execute Event to Continue Inventory Refresh');
        cig_bb100_op_refresh_procs_pk.build_evnt_cont_inv_refresh(i_div);
        logs.dbg('Check for Initiation of SetRelease');

        IF is_rlse_init_fn(i_div_part) = 'Y' THEN
          logs.dbg('Start Allocation');
          op_allocate_pk.start_alloc_sp(i_div, 'QITEM09');
        END IF;   -- is_rlse_init_fn(i_div_part) = 'Y'
      ELSE
        logs.dbg('Set Cig Inventory Update Process Inactive');
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_cig_inv,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    i_div_part
                                                   );
      END IF;   -- finished_msg_typ_fn(i_msg_data) = 'QITEM19'
    END finish_sp;

    PROCEDURE process_msg_sp(
      i_div_part         IN             NUMBER,
      i_msg_data         IN             VARCHAR2,
      i_user_id          IN             VARCHAR2,
      io_t_prbs          IN OUT NOCOPY  g_tt_prbs,
      io_t_usb_slot_adj  IN OUT NOCOPY  cig_bb105_op_refresh_procs_pk.item_slot_adj_tab,
      io_t_usb_inv_adj   IN OUT NOCOPY  cig_bb105_op_refresh_procs_pk.inv_adj_tab,
      o_mq_msg_stat      OUT            VARCHAR2
    ) IS
      l_c_sysdate  CONSTANT DATE                                            := SYSDATE;
      l_r_msg               l_rt_msg;
      l_item_num            sawp505e.iteme%TYPE;
      l_uom                 sawp505e.uome%TYPE;
      l_cv                  SYS_REFCURSOR;
      l_new_slot            VARCHAR2(7);
      l_old_slot            VARCHAR2(7);
      l_qty_alloc           PLS_INTEGER;
      l_r_usb_slot          cig_bb105_op_refresh_procs_pk.item_slot_rec;
      l_r_usb_slot_adj      cig_bb105_op_refresh_procs_pk.item_slot_adj_rec;
      l_r_usb_inv_adj       cig_bb105_op_refresh_procs_pk.inv_adj_rec;
    BEGIN
      o_mq_msg_stat := g_c_compl;

      IF is_finished_msg_fn(i_msg_data) THEN
        logs.dbg('Process for Finish Msg');
        finish_sp(i_div_part, i_div, i_msg_data, i_user_id);
      ELSE
        logs.dbg('Parse MQ Msg Data');
        l_r_msg := parse_msg_fn(i_msg_data);
        l_new_slot := l_r_msg.aisl || l_r_msg.bin || l_r_msg.levl;
        logs.dbg('Get CBR Item Info');

        SELECT e.iteme, e.uome
          INTO l_item_num, l_uom
          FROM sawp505e e
         WHERE e.catite = l_r_msg.catlg_num;

        logs.dbg('Get Inv Info');

        OPEN l_cv
         FOR
           SELECT     w.aislc || w.binc || w.levlc, w.qalc
                 FROM whsp300c w
                WHERE w.div_part = i_div_part
                  AND w.itemc = l_item_num
                  AND w.uomc = l_uom
                  AND w.zonec = i_div
                  AND NVL(w.taxjrc, ' ') = NVL(l_r_msg.jrsdctn, ' ')
           FOR UPDATE;

        FETCH l_cv
         INTO l_old_slot, l_qty_alloc;

        CLOSE l_cv;

        IF l_r_msg.qty_onh < l_qty_alloc THEN
          logs.dbg('Log Inventory Sync Problem');
          add_problem_sp(io_t_prbs,
                         i_div,
                         l_c_msg_id,
                         'Inventory sync problem',
                         lpad_fn(i_div, 2)
                         || lpad_fn(l_item_num, 9)
                         || rpad_fn(l_uom, 3)
                         || rpad_fn(l_r_msg.jrsdctn, 3)
                         || l_new_slot
                         || lpad_fn(l_r_msg.qty_onh, 9)
                         || lpad_fn(l_qty_alloc, 9)
                        );
        END IF;   -- l_r_msg.qty_onh < l_qty_alloc

        IF SUBSTR(l_uom, 1, 2) = 'CI' THEN
          IF l_r_msg.jrsdctn = 'USB' THEN
            IF l_new_slot <> NVL(l_old_slot, '~') THEN
              logs.dbg('Capture Bulk Slot Adjustments for CMS');
              l_r_usb_slot.mcl_item_# := l_r_msg.catlg_num;
              l_r_usb_slot.zone_nm := l_r_msg.jrsdctn;
              l_r_usb_slot.slot_nm := l_new_slot;
              l_r_usb_slot_adj.item_slot := l_r_usb_slot;
              l_r_usb_slot_adj.actn_cd := l_r_msg.actn_cd;
              io_t_usb_slot_adj.EXTEND;
              io_t_usb_slot_adj(io_t_usb_slot_adj.LAST) := l_r_usb_slot_adj;
            END IF;   -- v_new_slot <> NVL(v_old_slot, '~')

            IF (   l_r_msg.qty_from > 0
                OR l_r_msg.qty_to > 0) THEN
              logs.dbg('Capture Bulk Inv Adjustments for CMS');
              l_r_usb_slot.mcl_item_# := l_r_msg.catlg_num;
              l_r_usb_slot.zone_nm := l_r_msg.jrsdctn;
              l_r_usb_slot.slot_nm := l_new_slot;
              l_r_usb_inv_adj.item_slot := l_r_usb_slot;
              l_r_usb_inv_adj.adj_qty :=(l_r_msg.qty_to - l_r_msg.qty_from);
              l_r_usb_inv_adj.rsn_cd := l_r_msg.tran_cd;
              l_r_usb_inv_adj.oh_qty := l_r_msg.qty_onh;
              io_t_usb_inv_adj.EXTEND;
              io_t_usb_inv_adj(io_t_usb_inv_adj.LAST) := l_r_usb_inv_adj;
            END IF;   -- l_r_msg.qty_from > 0 OR l_r_msg.qty_to > 0
          END IF;   -- l_r_msg.jrsdctn = 'USB'
        END IF;   -- SUBSTR(l_uom, 1, 2) = 'CI'

        IF l_r_msg.actn_cd = 'DEL' THEN
          logs.dbg('Delete Item');

          DELETE FROM whsp300c
                WHERE div_part = i_div_part
                  AND itemc = l_item_num
                  AND uomc = l_uom
                  AND zonec = i_div
                  AND NVL(taxjrc, ' ') = NVL(l_r_msg.jrsdctn, ' ');

          IF l_r_msg.qty_onh > 0 THEN
            logs.dbg('Log Delete Item with Onhand');
            add_problem_sp(io_t_prbs,
                           i_div,
                           l_c_msg_id,
                           'Delete Item With Onhand.',
                           lpad_fn(i_div, 2)
                           || lpad_fn(l_item_num, 9)
                           || rpad_fn(l_uom, 3)
                           || rpad_fn(l_r_msg.jrsdctn, 3)
                           || l_r_msg.qty_onh
                          );
          END IF;   -- l_r_msg.qty_onh > 0
        ELSIF l_r_msg.actn_cd IN('ADD', 'CHG') THEN
          logs.dbg('Merge WHSP300C');
          MERGE INTO whsp300c w
               USING (SELECT 1 tst
                        FROM DUAL) x
                  ON (    w.div_part = i_div_part
                      AND w.itemc = l_item_num
                      AND w.uomc = l_uom
                      AND w.zonec = i_div
                      AND NVL(w.taxjrc, ' ') = NVL(l_r_msg.jrsdctn, ' ')
                      AND x.tst > 0)
            WHEN MATCHED THEN
              UPDATE
                 SET w.qohc = l_r_msg.qty_onh + w.qalc, w.qavc = l_r_msg.qty_onh, w.aislc = l_r_msg.aisl,
                     w.binc = l_r_msg.bin, w.levlc = l_r_msg.levl
            WHEN NOT MATCHED THEN
              INSERT(div_part, itemc, uomc, taxjrc, zonec, aislc, binc, levlc, qohc, qalc, qavc)
              VALUES(i_div_part, l_item_num, l_uom, l_r_msg.jrsdctn, i_div, l_r_msg.aisl, l_r_msg.bin, l_r_msg.levl,
                     l_r_msg.qty_onh, 0, l_r_msg.qty_onh);
          logs.dbg('Log Inventory Transaction');

          INSERT INTO tran_op2t
                      (div_part, tran_id, rlse_id, tran_typ, create_ts,
                       pgm_id
                      )
               VALUES (i_div_part, op1a_tran_id_seq.NEXTVAL, -1, 04, l_c_sysdate,
                       l_r_msg.pgm_id || ' - ' || l_r_msg.tran_cd
                      );

          INSERT INTO tran_item_op2i
                      (div_part, tran_id, catlg_num, inv_zone,
                       inv_aisle, inv_bin, inv_lvl, pick_zone, pick_aisle, pick_bin,
                       pick_lvl,
                       qty
                      )
               VALUES (i_div_part, op1a_tran_id_seq.CURRVAL, l_r_msg.catlg_num, NVL(l_r_msg.jrsdctn, '~'),
                       l_r_msg.aisl, l_r_msg.bin, l_r_msg.levl, NVL(l_r_msg.jrsdctn, '~'), l_r_msg.aisl, l_r_msg.bin,
                       l_r_msg.levl,
                       (CASE
                          WHEN(    l_r_msg.qty_from = 0
                               AND l_r_msg.qty_to = 0) THEN l_r_msg.qty_onh
                          ELSE l_r_msg.qty_to - l_r_msg.qty_from
                        END
                       )
                      );
        END IF;   -- l_r_msg.actn_cd = 'DEL'
      END IF;   -- is_finished_msg_fn(i_msg_data)
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_invmaint);
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);

      IF is_rlse_init_fn(l_div_part) = 'Y' THEN
        -- during SetRelease initiation bypass restrictions for CIG_OP_REFRESH, TEST_BILL
        l_excl_list := 'CIG_OP_REFRESH,TEST_BILL';
      END IF;   -- is_rlse_init_fn(l_div_part) = 'Y'

      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_invmaint,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part,
                                                  l_excl_list
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part,
                       l_r_msg.mq_msg_data,
                       i_user_id,
                       l_t_prbs,
                       l_t_usb_slot_adjs,
                       l_t_usb_inv_adjs,
                       l_mq_msg_stat
                      );
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;

      IF l_t_prbs.COUNT > 0 THEN
        logs.dbg('Notify');
        notify_sp(l_t_prbs);
      END IF;   -- l_t_prbs.COUNT > 0

      IF l_t_usb_slot_adjs.COUNT > 0 THEN
        logs.dbg('Process Bulk Slot Adjustments in CMS');
        cig_bb105_op_refresh_procs_pk.slot_maint(i_div, l_t_usb_slot_adjs);
      END IF;   -- t_usb_slot_adjs.COUNT > 0

      IF l_t_usb_inv_adjs.COUNT > 0 THEN
        logs.dbg('Process Bulk Inv Adjustments in CMS');
        cig_bb105_op_refresh_procs_pk.inv_adjust(i_div, l_t_usb_inv_adjs);
      END IF;   -- l_t_usb_inv_adjs.COUNT > 0

      logs.dbg('DELETE LAC/MSC in State Slots');

      -- This delete is performed outside the loop and as well after the records
      -- got inserted. The reason is less I/O doing this way.
      DELETE FROM whsp300c w
            WHERE w.div_part = l_div_part
              AND EXISTS(SELECT 1
                           FROM div_item_alt a
                          WHERE a.alt_typ IN('LAC', 'MSC')
                            AND a.div_part = w.div_part
                            AND a.alt_item = w.itemc
                            AND a.alt_uom = w.uomc)
              AND w.taxjrc <> 'USB';

      COMMIT;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_invmaint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_invmaint,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END invmaint_sp;

  /*
  ||----------------------------------------------------------------------------
  || LAST_REFRESH_INV_SP
  ||  Return timestamp of last refresh OP inventory from mainframe and indicate
  ||  whether the refresh process is restricted from running.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/10 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE last_refresh_inv_sp(
    i_div       IN      VARCHAR2,
    o_ts        OUT     VARCHAR2,
    o_rstrn_sw  OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.LAST_REFRESH_INV_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get Last TS');

    SELECT TO_CHAR(MAX(g.create_ts), 'YYYY-MM-DD HH24:MI:SS')
      INTO o_ts
      FROM mclane_mq_get g
     WHERE g.div_part = l_div_part
       AND g.mq_msg_id = 'QITEM09'
       AND SUBSTR(g.mq_msg_data, 54, 8) = 'FINISHED';

    logs.dbg('Get Restriction');
    o_rstrn_sw :=(CASE
                    WHEN op_process_control_pk.is_restricted_fn(op_const_pk.prcs_cig_op_rfrsh, l_div_part) THEN 'Y'
                    WHEN op_process_control_pk.is_restricted_fn(op_const_pk.prcs_qitem19, l_div_part) THEN 'Y'
                    ELSE 'N'
                  END
                 );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END last_refresh_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || REFRESH_INV_SP
  ||  Refresh OP inventory from mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/10 | rhalpai | Original for PIR8377
  || 10/11/10 | rhalpai | Add pgm_id parm and logic to send UNFINISHED msg when
  ||                    | pgm_id indicates mainframe request and process is
  ||                    | unable to complete. Add logic to initiate CigOPRefresh
  ||                    | process from CMS. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE refresh_inv_sp(
    i_div     IN  VARCHAR2,
    i_pgm_id  IN  VARCHAR2 DEFAULT 'OP_UI'
  ) IS
    l_c_module               CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.REFRESH_INV_SP';
    lar_parm                          logs.tar_parm;
    l_div_part                        NUMBER;
    l_pgm_id                          evnt_que_parms_cg5e.user_id%TYPE;
    l_c_mf_req_id            CONSTANT VARCHAR2(6)                        := 'QIT20J';
    l_c_prcs_cig_op_refresh  CONSTANT VARCHAR2(30)                       := 'CIG_OP_REFRESH';
    l_c_prcs_qitem19         CONSTANT VARCHAR2(30)                       := 'QITEM19';
    l_c_msg_id               CONSTANT VARCHAR2(7)                        := 'QITEM19';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PgmId', i_pgm_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_pgm_id := NVL(i_pgm_id, l_c_mf_req_id);

    IF (    l_pgm_id = l_c_mf_req_id
        AND (   op_process_control_pk.is_restricted_fn(l_c_prcs_cig_op_refresh, l_div_part)
             OR op_process_control_pk.is_restricted_fn(l_c_prcs_qitem19, l_div_part)
            )
       ) THEN
      logs.dbg('Add UNFINISHED Msg');
      ins_mq_put_sp(l_div_part, l_c_msg_id, 'UNFINISHED');
      logs.dbg('Put to MQ');
      mq_put_sp(i_div, l_c_msg_id);
    ELSE
      logs.dbg('Build and Execute Inv Refresh Event');
      cig_bb100_op_refresh_procs_pk.build_evnt_inv_refresh(i_div, l_pgm_id);
    END IF;   -- l_pgm_id = l_c_mf_req_id

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      IF l_pgm_id = l_c_mf_req_id THEN
        ins_mq_put_sp(l_div_part, l_c_msg_id, 'UNFINISHED');
        mq_put_sp(i_div, l_c_msg_id);
      END IF;   -- l_pgm_id = l_c_mf_req_id

      logs.err(lar_parm);
  END refresh_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM19_SP
  ||  These messages are the current view of inventory on Oracle that is
  ||  AVAILABLE. We are assuming that the ALLOCATED inventory has already
  ||  decremented inventory on the ITM file (SSY0).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/24/01 | rhalpai | Original
  || 08/09/01 | rhalpai | Added insert for 'FINISHED'
  || 08/20/01 | rhalpai | fixed padding for aislc within v_mq_msg_data
  || 09/05/01 | rhalpai | fixed record layout problem
  || 11/19/01 | rhalpai | Corrected cursor references to external variable
  ||                    | Removed unreferenced variables
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 05/12/06 | rhalpai | Changed logic to prevent decimal values from being
  ||                    | sent for qty avail and replaced cursor loop with
  ||                    | single insert statement. IM225965
  || 01/06/07 | rhalpai | Added Process Control logic to lock out SetRelease
  ||                    | until xxQIT20J sends down QITEM09 inventory qty
  ||                    | replacement msgs including a final FINISHED QITEM09
  ||                    | msg. Removed status parm. IM278669
  || 02/11/08 | rhalpai | Added Unfinished trailer msg. PIR5414
  || 10/11/10 | rhalpai | Add pgm_id parm and logic to put all msgs to MQ.
  ||                    | Add logic to only send UNFINISHED MQ msg when pgm_id
  ||                    | indicates it is a mainframe request and to re-raise
  ||                    | the exception when the process is restricted. PIR8531
  || 01/28/13 | dlbeal  | Add an indicator ('B') to the qitem19 message when
  ||                    | the refresh is being called by set release.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem19_sp(
    i_div     IN  VARCHAR2,
    i_pgm_id  IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_MESSAGES_PK.QITEM19_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_c_msg_id     CONSTANT VARCHAR2(7)   := 'QITEM19';
    l_c_mf_req_id  CONSTANT VARCHAR2(6)   := 'QIT20J';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PgmId', i_pgm_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem19,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.dbg('Add Inventory Msgs');

    INSERT INTO mclane_mq_put
                (mq_msg_id, div_part, mq_msg_data, mq_msg_status)
      SELECT l_c_msg_id, w.div_part,
             RPAD(i_div, 2)
             || RPAD(l_c_msg_id, 8)
             || RPAD(' ', 43)
             || lpad_fn(w.itemc, 9, '0')
             || rpad_fn(w.uomc, 3)
             || rpad_fn(w.aislc, 2)
             || rpad_fn(w.binc, 3)
             || rpad_fn(w.levlc, 2)
             || lpad_fn(FLOOR(ABS(w.qavc)), 9, '0')
             || DECODE(i_pgm_id, 'SETRLSE', 'B', ' ')
             || rpad_fn(w.taxjrc, 3),
             'OPN'
        FROM whsp300c w
       WHERE w.div_part = l_div_part;

    COMMIT;
    logs.dbg('Add FINISHED Msg');
    ins_mq_put_sp(l_div_part, l_c_msg_id, 'FINISHED');
    logs.dbg('Put to Msgs to MQ for Mainframe Processing');
    mq_put_sp(i_div, l_c_msg_id);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);

      IF i_pgm_id = l_c_mf_req_id THEN
        ins_mq_put_sp(l_div_part, l_c_msg_id, 'UNFINISHED');
        mq_put_sp(i_div, l_c_msg_id);
      END IF;   -- i_pgm_id = l_c_mf_req_id

      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;

      IF i_pgm_id = l_c_mf_req_id THEN
        ins_mq_put_sp(l_div_part, l_c_msg_id, 'UNFINISHED');
        mq_put_sp(i_div, l_c_msg_id);
      END IF;   -- i_pgm_id = l_c_mf_req_id

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem19,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END qitem19_sp;

  /*
  ||----------------------------------------------------------------------------
  || QDIST02_SP
  ||  This messages will find the OP order and apply changes to order header
  ||  and order details tables
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/15/05 | anchakr | Original - Amar Nath Chakraborty
  || 04/27/06 | rhalpai | Added logic to update PO Number. PIR2978
  || 04/18/07 | rhalpai | Added process control, changed error handler to use
  ||                    | standard parm list.
  || 06/30/11 | rhalpai | Added logic to set FixedRetailSwitch OFF when new
  ||                    | retail is zero and old retail was greater than zero.
  ||                    | IM-021300
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 10/30/13 | rhalpai | Change logic to include OrigOrdQty when changing
  ||                    | OrdQty. IM-122084
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 04/20/22 | rhalpai | Add logic to include AllwPartlSw. PIR21059
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qdist02_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    -- Generic
    l_c_module     CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QDIST02_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_c_msg_id     CONSTANT VARCHAR2(7)                        := 'QDIST02';

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div_id         div_mstr_di1d.div_id%TYPE,
      actn_cd        VARCHAR2(3),
      legcy_ref      ordp100a.legrfa%TYPE,
      ord_qty_chg    VARCHAR2(1),
      ord_qty        NUMBER,
      comnt_chg      VARCHAR2(1),
      comnt          ordp140c.commc%TYPE,
      price_amt_chg  VARCHAR2(1),
      price_amt      NUMBER,
      rtl_amt_chg    VARCHAR2(1),
      rtl_amt        NUMBER,
      rtl_mult_chg   VARCHAR2(1),
      rtl_mult       NUMBER,
      po_num_chg     VARCHAR2(1),
      po_num             ordp100a.cpoa%TYPE,
      allw_partl_sw_chg  VARCHAR2(1),
      allw_partl_sw      VARCHAR2(1)
    );

    l_r_msg                 l_rt_msg;
    l_c_chg_found  CONSTANT VARCHAR2(1)                        := 'Y';
    l_msg_stat              mclane_mq_get.mq_msg_status%TYPE;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div_id := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.legcy_ref := SUBSTR(i_msg_data, 54, 25);
      l_r_parsed.ord_qty_chg := SUBSTR(i_msg_data, 79, 1);
      l_r_parsed.ord_qty := string_to_num_fn(SUBSTR(i_msg_data, 80, 7));
      l_r_parsed.comnt_chg := SUBSTR(i_msg_data, 87, 1);
      l_r_parsed.comnt := SUBSTR(i_msg_data, 88, 25);
      l_r_parsed.price_amt_chg := SUBSTR(i_msg_data, 113, 1);
      l_r_parsed.price_amt := string_to_num_fn(SUBSTR(i_msg_data, 114, 9)) / 100;
      l_r_parsed.rtl_amt_chg := SUBSTR(i_msg_data, 123, 1);
      l_r_parsed.rtl_amt := string_to_num_fn(SUBSTR(i_msg_data, 124, 9)) / 100;
      l_r_parsed.rtl_mult_chg := SUBSTR(i_msg_data, 133, 1);
      l_r_parsed.rtl_mult := string_to_num_fn(SUBSTR(i_msg_data, 134, 5));
      l_r_parsed.po_num_chg := SUBSTR(i_msg_data, 139, 1);
      l_r_parsed.po_num := RTRIM(SUBSTR(i_msg_data, 140, 22));
      l_r_parsed.allw_partl_sw_chg := RTRIM(SUBSTR(i_msg_data, 162, 1));
      l_r_parsed.allw_partl_sw := RTRIM(SUBSTR(i_msg_data, 163, 1));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE upd_order_sp(
      i_div_part    IN  NUMBER,
      i_legcy_ref   IN  VARCHAR2,
      i_ord_qty     IN  NUMBER,
      i_price_amt   IN  NUMBER,
      i_rtl_amt     IN  NUMBER,
      i_rtl_mult    IN  NUMBER,
      i_po_num_chg  IN  VARCHAR2,
      i_po_num             IN  VARCHAR2,
      i_allw_partl_sw_chg  IN  VARCHAR2,
      i_allw_partl_sw      IN  VARCHAR2
    ) IS
    BEGIN
      UPDATE ordp120b b
         SET b.ordqtb = NVL(i_ord_qty, b.ordqtb),
             b.orgqtb = NVL(i_ord_qty, b.orgqtb),
             b.hdprcb = NVL(i_price_amt, b.hdprcb),
             b.hdrtab = NVL(i_rtl_amt, b.hdrtab),
             b.rtfixb = DECODE(NVL(b.hdrtab, 0), 0, b.rtfixb, DECODE(i_rtl_amt, 0, '0', b.rtfixb)),
             b.hdrtmb = NVL(i_rtl_mult, b.hdrtmb)
       WHERE b.div_part = i_div_part
         AND b.ordnob = (SELECT a.ordnoa
                           FROM ordp100a a
                          WHERE a.div_part = i_div_part
                            AND a.legrfa = i_legcy_ref
                            AND a.dsorda = 'D'
                            AND a.stata = 'O')
         AND b.lineb = 1;

      IF l_c_chg_found IN(i_allw_partl_sw_chg, i_po_num_chg) THEN
        UPDATE ordp100a a
           SET a.cpoa = DECODE(i_po_num_chg, l_c_chg_found, i_po_num, a.cpoa),
               a.pshipa = DECODE(i_allw_partl_sw_chg,
                                 l_c_chg_found, DECODE(i_allw_partl_sw, 'Y', '1', '1', '1', '0'),
                                 a.pshipa
                                )
         WHERE a.div_part = i_div_part
           AND a.legrfa = i_legcy_ref
           AND a.dsorda = 'D'
           AND a.stata = 'O';
      END IF;   -- l_c_chg_found IN(i_allw_partl_sw_chg, i_po_num_chg)
    END upd_order_sp;

    PROCEDURE upd_ord_comments_sp(
      i_div_part   IN  NUMBER,
      i_legcy_ref  IN  VARCHAR2,
      i_comnt      IN  VARCHAR2
    ) IS
    BEGIN
      UPDATE ordp140c c
         SET c.commc = i_comnt
       WHERE c.div_part = i_div_part
         AND c.ordnoc = (SELECT a.ordnoa
                           FROM ordp100a a
                          WHERE a.div_part = i_div_part
                            AND a.legrfa = i_legcy_ref
                            AND a.dsorda = 'D'
                            AND a.stata = 'O');
    END upd_ord_comments_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qdist02);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qdist02,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        -- default to complete status (instead of problem status)
        l_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);

        -- Just to make sure action code is change only,
        -- If not bypass the record
        IF l_r_msg.actn_cd = g_c_chg THEN
          -- if atleast one change availbale Update the field has a change
          IF l_c_chg_found IN(l_r_msg.ord_qty_chg,
                              l_r_msg.price_amt_chg,
                              l_r_msg.rtl_amt_chg,
                              l_r_msg.rtl_mult_chg,
                              l_r_msg.po_num_chg,
                              l_r_msg.allw_partl_sw_chg
                             ) THEN
            logs.dbg('Update Order');
            upd_order_sp(l_div_part,
                         l_r_msg.legcy_ref,
                         (CASE
                            WHEN l_r_msg.ord_qty_chg = l_c_chg_found THEN l_r_msg.ord_qty
                          END),
                         (CASE
                            WHEN l_r_msg.price_amt_chg = l_c_chg_found THEN l_r_msg.price_amt
                          END),
                         (CASE
                            WHEN l_r_msg.rtl_amt_chg = l_c_chg_found THEN l_r_msg.rtl_amt
                          END),
                         (CASE
                            WHEN l_r_msg.rtl_mult_chg = l_c_chg_found THEN l_r_msg.rtl_mult
                          END),
                         l_r_msg.po_num_chg,
                         l_r_msg.po_num,
                         l_r_msg.allw_partl_sw_chg,
                         l_r_msg.allw_partl_sw
                        );

            IF SQL%NOTFOUND THEN
              l_msg_stat := g_c_prb;
            END IF;   -- SQL%NOTFOUND
          END IF;   --if at least one change available

          -- Update comments if yes
          IF l_r_msg.comnt_chg = l_c_chg_found THEN
            logs.dbg('Update Order Comment');
            upd_ord_comments_sp(l_div_part, l_r_msg.legcy_ref, l_r_msg.comnt);

            -- If no comments to update
            IF SQL%NOTFOUND THEN
              l_msg_stat := g_c_prb;
            END IF;   -- SQL%NOTFOUND
          END IF;   -- l_r_msg.comnt_chg = l_c_chg_found
        END IF;   -- l_r_msg.actn_cd = l_c_change

        logs.dbg('Update MQ Message Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_msg_stat);
        COMMIT;   -- Commit the Order Data updated successfully
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qdist02,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qdist02,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qdist02_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST01_SP
  ||  Maps messages that describe the Divisional Customer
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/07/01 | rhalpai | Original
  || 09/12/01 | rhalpai | Added logic to update ordp200s, ordp322r, sysp242m if
  ||                    | their inserts fail and corrected problem with sysp242m
  ||                    | not being updated
  || 10/09/01 | rhalpai | Added statc to "update sysp200c"
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/07/01 | rhalpai | Suspend orders for deleted customers or customers with
  ||                    | Hold or Inactive status
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 01/30/02 | rhalpai | Added insert to SYSP296A to track suspended orders
  || 02/04/02 | rhalpai | Changed parse message data for cigarette license
  ||                    | expire date to expect a YYYYMMDD format and store in
  ||                    | Rensoft format.
  || 07/12/02 | rhalpai | Added logic to maintain new pre-post column (tclscc)
  ||                    | and corrected section INSERT-SYSP296A by swapping
  ||                    | inserted values for usera and rsncda.
  || 08/12/02 | rhalpai | Changed to exclude pre-post update records separately
  ||                    | to exclude from the suspend logic and increase efficiency.
  ||                    | Changed suspend logic to include all orders for Deletes
  ||                    | and Inactives OR all Reg and attached Dist orders for Holds.
  || 12/10/02 | rhalpai | Added logic to keep the retail group number, retgpc, in
  ||                    | sync with the sub group, subgpc, on sysp200c.
  ||                    | Also added logic to keep security profiles in sync for
  ||                    | new customers.
  || 01/06/03 | rhalpai | Moved logic to keep security profiles in sync for new
  ||                    | customers to QCUST03_SP (MCLP020B entry is required).
  ||                    | Added logic to update customer orders when there is a
  ||                    | change in customer status. Orders may be Suspended,
  ||                    | Unsuspended or moved to History when a customer is
  ||                    | placed on hold or inactivated depending on the order
  ||                    | status override on MCLP140A. If a customer is then
  ||                    | reactivated all suspended order lines are repriced and
  ||                    | then orders are moved to next available load via SYNCLOAD.
  || 05/15/03 | rhalpai | Changed reprice_orderline_cur to include order lines from
  ||                    | WHSP120B where the sub code > 999 (was = 0). This is to
  ||                    | allow for the reprice of sub-lines that have been moved
  ||                    | to the exception well (i.e. "GRPREST", "1 Customer on Hold"
  ||                    | exception code=008).
  ||                    | Also, changed logic in HANDLE_STATUS_CHANGE_SP to retrieve
  ||                    | "S"uspended orders in orders_cur as long as the override
  ||                    | status is not set to "S"uspend. This will allow us to
  ||                    | cancel "S"uspended orders when a customer's status is
  ||                    | changed from "HoLD" to "INActive" and the "C"ancel
  ||                    | override status is selected for inactive customers.
  ||                    | Also, changed logic in UPDATE_ORDER_STATUS_SP to not
  ||                    | update status or log in SYSP296A where new status is the
  ||                    | same as the old status.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status from calls to
  ||                    | assign_order_to_default_sp and reprice_order_line_sp.
  || 08/08/05 | rhalpai | Added customer type. PIR2608
  || 12/02/05 | rhalpai | Converted to use merge statement. PIR2608
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 08/19/08 | rhalpai | Reformatted and removed check for detail with status
  ||                    | not in O,I,S,C in cursors using header status and then
  ||                    | moved logic to stand-alone procedures:
  ||                    | UPD_ORD_STAT_SP, OVRRD_ORD_STATS_SP, UNSUSPND_ORDS_SP,
  ||                    | UPD_ORDS_FOR_CUST_STAT_CHG_SP. PIR6364
  || 11/24/08 | rhalpai | Changed cig_lic_exp_dt definition to handle 8-digit
  ||                    | number (i.e.: 29991231). IM462653
  || 05/20/11 | rhalpai | Changed logic to maintain new DIST_FRST_DAY column.
  ||                    | PIR9030
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 11/28/11 | rhalpai | Add logic for new Test status 4. PIR10211
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 02/20/20 | rhalpai | Add logic for new CUST_TYP column. PIR19810
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST01_SP';
    lar_parm                  logs.tar_parm;
    l_div_part                NUMBER;
    l_c_msg_id       CONSTANT VARCHAR2(7)                        := 'QCUST01';
    l_mq_msg_stat             mclane_mq_get.mq_msg_status%TYPE;
    l_c_prepost_on   CONSTANT VARCHAR2(3)                        := 'PPY';
    l_c_prepost_off  CONSTANT VARCHAR2(3)                        := 'PPN';
    l_cv                      SYS_REFCURSOR;
    l_cust_old_stat           sysp200c.statc%TYPE;
    l_cust_typ                sysp200c.typecc%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div                   VARCHAR2(2),
      actn_cd               VARCHAR2(3),
      cust_num              sysp200c.acnoc%TYPE,
      cust_nm               sysp200c.namec%TYPE,
      hold_sw               VARCHAR2(1),
      max_ar_outstndng      sysp200c.tmamtc%TYPE,
      cur_ar_outstndng      sysp200c.outamc%TYPE,
      prnt_blnk_rtl_lbl     sysp200c.prblrc%TYPE,
      prnt_pick_lst         sysp200c.pntpkc%TYPE,
      cig_license           sysp200c.cglicc%TYPE,
      cig_lic_exp_dt        PLS_INTEGER,
      mssng_itm_catg_notfy  sysp200c.msitnc%TYPE,
      rout_typ              sysp200c.rttypc%TYPE,
      shp_addr1             sysp200c.shad1c%TYPE,
      shp_addr2             sysp200c.shad2c%TYPE,
      shp_city              sysp200c.shpctc%TYPE,
      shp_st                sysp200c.shpstc%TYPE,
      shp_zip               sysp200c.shpzpc%TYPE,
      shp_cntry             sysp200c.shpcnc%TYPE,
      bil_addr1             sysp200c.blad1c%TYPE,
      bil_addr2             sysp200c.blad2c%TYPE,
      bil_city              sysp200c.blcitc%TYPE,
      bil_st                sysp200c.blstc%TYPE,
      bil_zip               sysp200c.blzpc%TYPE,
      bil_cntry             sysp200c.blcnc%TYPE,
      cntct_addr            sysp200c.cntadc%TYPE,
      accpt_div_subs        sysp200c.acdvsc%TYPE,
      cntct_mthd            sysp200c.conmtc%TYPE,
      auto_dup_ord_prcssng  sysp200c.dupodc%TYPE,
      delv_cust             sysp200c.dlcstc%TYPE,
      stat                  sysp200c.statc%TYPE,
      hi_val_sw             sysp200c.hivalc%TYPE,
      csr                   sysp200c.csrc%TYPE,
      cntct_nm              sysp200c.cnnamc%TYPE,
      cntct_phone           sysp200c.cnphnc%TYPE,
      cntct_email           sysp200c.cnemac%TYPE,
      cntct_fax             sysp200c.cnfaxc%TYPE,
      cntct_pager           sysp200c.cnpagc%TYPE,
      rndng_grp             sysp200c.rndgpc%TYPE,
      sub_grp               sysp200c.subgpc%TYPE,
      allw_partls           sysp200c.alparc%TYPE,
      prepost_sw            sysp200c.tclscc%TYPE,
      cust_typ              sysp200c.cust_typ%TYPE
    );

    l_r_msg                   l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 102, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cust_num := RTRIM(SUBSTR(i_msg_data, 54, 8));
      l_r_parsed.prepost_sw := RTRIM(SUBSTR(i_msg_data, 657, 3));

      -- check prepost CHG
      IF NOT(    l_r_parsed.actn_cd = g_c_chg
             AND NVL(l_r_parsed.prepost_sw, ' ') IN(l_c_prepost_on, l_c_prepost_off)) THEN
        l_r_parsed.cust_nm := RTRIM(SUBSTR(i_msg_data, 62, 40));
        l_r_parsed.hold_sw := RTRIM(SUBSTR(i_msg_data, 104, 1));
        l_r_parsed.max_ar_outstndng := string_to_num_fn(SUBSTR(i_msg_data, 105, 8));
        l_r_parsed.cur_ar_outstndng := string_to_num_fn(SUBSTR(i_msg_data, 113, 8));
        l_r_parsed.prnt_blnk_rtl_lbl := RTRIM(SUBSTR(i_msg_data, 121, 1));
        l_r_parsed.prnt_pick_lst := RTRIM(SUBSTR(i_msg_data, 122, 1));
        l_r_parsed.cig_license := RTRIM(SUBSTR(i_msg_data, 123, 15));
        l_r_parsed.cig_lic_exp_dt := NVL(string_to_num_fn(SUBSTR(i_msg_data, 138, 8)), 0);

        IF l_r_parsed.cig_lic_exp_dt > 0 THEN
          l_r_parsed.cig_lic_exp_dt := TO_DATE(l_r_parsed.cig_lic_exp_dt, 'YYYYMMDD') - DATE '1900-02-28';
        END IF;   -- l_r_parsed.cig_lic_exp_dt > 0

        l_r_parsed.mssng_itm_catg_notfy := RTRIM(SUBSTR(i_msg_data, 146, 1));
        l_r_parsed.rout_typ := RTRIM(SUBSTR(i_msg_data, 147, 3));
        l_r_parsed.shp_addr1 := RTRIM(SUBSTR(i_msg_data, 150, 40));
        l_r_parsed.shp_addr2 := RTRIM(SUBSTR(i_msg_data, 190, 40));
        l_r_parsed.shp_city := RTRIM(SUBSTR(i_msg_data, 230, 30));
        l_r_parsed.shp_st := RTRIM(SUBSTR(i_msg_data, 260, 2));
        l_r_parsed.shp_zip := RTRIM(SUBSTR(i_msg_data, 262, 9));
        l_r_parsed.shp_cntry := RTRIM(SUBSTR(i_msg_data, 271, 3));
        l_r_parsed.bil_addr1 := RTRIM(SUBSTR(i_msg_data, 274, 40));
        l_r_parsed.bil_addr2 := RTRIM(SUBSTR(i_msg_data, 314, 40));
        l_r_parsed.bil_city := RTRIM(SUBSTR(i_msg_data, 354, 30));
        l_r_parsed.bil_st := RTRIM(SUBSTR(i_msg_data, 384, 2));
        l_r_parsed.bil_zip := RTRIM(SUBSTR(i_msg_data, 386, 9));
        l_r_parsed.bil_cntry := RTRIM(SUBSTR(i_msg_data, 395, 3));
        l_r_parsed.cntct_addr := RTRIM(SUBSTR(i_msg_data, 398, 30));
        l_r_parsed.accpt_div_subs := RTRIM(SUBSTR(i_msg_data, 428, 1));
        l_r_parsed.cntct_mthd := RTRIM(SUBSTR(i_msg_data, 429, 3));
        l_r_parsed.auto_dup_ord_prcssng := RTRIM(SUBSTR(i_msg_data, 432, 1));
        l_r_parsed.delv_cust := RTRIM(SUBSTR(i_msg_data, 433, 8));
        l_r_parsed.stat :=(CASE
                             WHEN l_r_parsed.hold_sw = 'Y' THEN '3'   -- hold
                             WHEN SUBSTR(i_msg_data, 441, 3) = 'ACT' THEN '1'   -- active
                             WHEN SUBSTR(i_msg_data, 441, 3) = 'INA' THEN '2'   -- inactive
                             WHEN SUBSTR(i_msg_data, 441, 3) = 'TST' THEN '4'   -- test
                           END
                          );
        l_r_parsed.hi_val_sw := RTRIM(SUBSTR(i_msg_data, 444, 1));
        l_r_parsed.csr := RTRIM(SUBSTR(i_msg_data, 445, 8));
        l_r_parsed.cntct_nm := RTRIM(SUBSTR(i_msg_data, 453, 40));
        l_r_parsed.cntct_phone := RTRIM(SUBSTR(i_msg_data, 493, 20));
        l_r_parsed.cntct_email := RTRIM(SUBSTR(i_msg_data, 513, 70));
        l_r_parsed.cntct_fax := RTRIM(SUBSTR(i_msg_data, 583, 20));
        l_r_parsed.cntct_pager := RTRIM(SUBSTR(i_msg_data, 603, 20));
        l_r_parsed.rndng_grp := RTRIM(SUBSTR(i_msg_data, 631, 8));
        l_r_parsed.sub_grp := RTRIM(SUBSTR(i_msg_data, 639, 8));
        l_r_parsed.allw_partls := RTRIM(SUBSTR(i_msg_data, 647, 1));
        l_r_parsed.cust_typ := RTRIM(SUBSTR(i_msg_data, 660, 3));
      END IF;   -- check prepost CHG

      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE upd_prepost_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      UPDATE sysp200c c
         SET c.tclscc = DECODE(i_r_msg.prepost_sw, l_c_prepost_on, 'PRP')
       WHERE c.div_part = i_div_part
         AND c.acnoc = i_r_msg.cust_num;
    END upd_prepost_sp;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      logs.dbg('Remove SYSP200C Entry');

      DELETE FROM sysp200c c
            WHERE c.div_part = i_div_part
              AND c.acnoc = i_r_msg.cust_num;

      IF SQL%ROWCOUNT > 0 THEN
        logs.dbg('Remove MCLP030C Entry');

        DELETE FROM mclp030c ct
              WHERE ct.div_part = i_div_part
                AND ct.custc = i_r_msg.cust_num;

        logs.dbg('Remove ORDP200S Entry');

        DELETE FROM ordp200s s
              WHERE s.div_part = i_div_part
                AND s.custs = i_r_msg.cust_num;

        logs.dbg('Remove ORDP322R Entry');

        DELETE FROM ordp322r r
              WHERE r.div_part = i_div_part
                AND r.custr = i_r_msg.cust_num;

        logs.dbg('Remove SYSP242M Entry');

        DELETE FROM sysp242m m
              WHERE m.div_part = i_div_part
                AND m.custm = i_r_msg.cust_num;
      END IF;   -- SQL%ROWCOUNT > 0
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      logs.dbg('Merge SYSP200C');
      MERGE INTO sysp200c c
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    c.div_part = i_div_part
                  AND c.acnoc = i_r_msg.cust_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET namec = i_r_msg.cust_nm, tmamtc = i_r_msg.max_ar_outstndng, outamc = i_r_msg.cur_ar_outstndng,
                 prblrc = i_r_msg.prnt_blnk_rtl_lbl, pntpkc = i_r_msg.prnt_pick_lst, cglicc = i_r_msg.cig_license,
                 cgexdc = i_r_msg.cig_lic_exp_dt, msitnc = i_r_msg.mssng_itm_catg_notfy, rttypc = i_r_msg.rout_typ,
                 shad1c = i_r_msg.shp_addr1, shad2c = i_r_msg.shp_addr2, shpctc = i_r_msg.shp_city,
                 shpstc = i_r_msg.shp_st, shpzpc = i_r_msg.shp_zip, shpcnc = i_r_msg.shp_cntry,
                 blad1c = i_r_msg.bil_addr1, blad2c = i_r_msg.bil_addr2, blcitc = i_r_msg.bil_city,
                 blstc = i_r_msg.bil_st, blzpc = i_r_msg.bil_zip, blcnc = i_r_msg.bil_cntry,
                 cntadc = i_r_msg.cntct_addr, acdvsc = i_r_msg.accpt_div_subs, conmtc = i_r_msg.cntct_mthd,
                 dupodc = i_r_msg.auto_dup_ord_prcssng, dlcstc = i_r_msg.delv_cust, hivalc = i_r_msg.hi_val_sw,
                 csrc = i_r_msg.csr, cnnamc = i_r_msg.cntct_nm, cnphnc = i_r_msg.cntct_phone,
                 cnemac = i_r_msg.cntct_email, cnfaxc = i_r_msg.cntct_fax, cnpagc = i_r_msg.cntct_pager,
                 rndgpc = i_r_msg.rndng_grp, subgpc = i_r_msg.sub_grp, alparc = i_r_msg.allw_partls,
                 add1c = i_r_msg.bil_addr1, add2c = i_r_msg.bil_addr2, cityc = i_r_msg.bil_city,
                 statec = i_r_msg.bil_st, zipc = i_r_msg.bil_zip, cntrc = i_r_msg.bil_cntry, statc = i_r_msg.stat,
                 retgpc = i_r_msg.sub_grp, typecc = l_cust_typ, cust_typ = i_r_msg.cust_typ
        WHEN NOT MATCHED THEN
          INSERT(acnoc, namec, div_part, tmamtc, outamc, prblrc, pntpkc, cglicc, cgexdc, msitnc, rttypc, shad1c, shad2c,
                 shpctc, shpstc, shpzpc, shpcnc, blad1c, blad2c, blcitc, blstc, blzpc, blcnc, cntadc, acdvsc, conmtc,
                 dupodc, dlcstc, statc, hivalc, csrc, cnnamc, cnphnc, cnemac, cnfaxc, cnpagc, rndgpc, subgpc, alparc,
                 add1c, add2c, cityc, statec, zipc, cntrc, cocc, tclscc, retgpc, typecc, cust_typ)
          VALUES(i_r_msg.cust_num, i_r_msg.cust_nm, i_div_part, i_r_msg.max_ar_outstndng, i_r_msg.cur_ar_outstndng,
                 i_r_msg.prnt_blnk_rtl_lbl, i_r_msg.prnt_pick_lst, i_r_msg.cig_license, i_r_msg.cig_lic_exp_dt,
                 i_r_msg.mssng_itm_catg_notfy, i_r_msg.rout_typ, i_r_msg.shp_addr1, i_r_msg.shp_addr2, i_r_msg.shp_city,
                 i_r_msg.shp_st, i_r_msg.shp_zip, i_r_msg.shp_cntry, i_r_msg.bil_addr1, i_r_msg.bil_addr2,
                 i_r_msg.bil_city, i_r_msg.bil_st, i_r_msg.bil_zip, i_r_msg.bil_cntry, i_r_msg.cntct_addr,
                 i_r_msg.accpt_div_subs, i_r_msg.cntct_mthd, i_r_msg.auto_dup_ord_prcssng, i_r_msg.delv_cust,
                 i_r_msg.stat, i_r_msg.hi_val_sw, i_r_msg.csr, i_r_msg.cntct_nm, i_r_msg.cntct_phone,
                 i_r_msg.cntct_email, i_r_msg.cntct_fax, i_r_msg.cntct_pager, i_r_msg.rndng_grp, i_r_msg.sub_grp,
                 i_r_msg.allw_partls, i_r_msg.bil_addr1, i_r_msg.bil_addr2, i_r_msg.bil_city, i_r_msg.bil_st,
                 i_r_msg.bil_zip, i_r_msg.bil_cntry, 'MCL', i_r_msg.prepost_sw, i_r_msg.sub_grp, l_cust_typ,
                 i_r_msg.cust_typ);
      logs.dbg('Merge ORDP200S');
      MERGE INTO ordp200s s
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    s.div_part = i_div_part
                  AND s.custs = i_r_msg.cust_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET adress = i_r_msg.shp_addr1, adrs2s = i_r_msg.shp_addr2, citys = i_r_msg.shp_city,
                 states = i_r_msg.shp_st, zipcds = i_r_msg.shp_zip, cntrys = i_r_msg.shp_cntry,
                 conams = i_r_msg.cntct_nm, emails = i_r_msg.cntct_email, faxs = i_r_msg.cntct_fax,
                 shpnms = i_r_msg.cust_nm
        WHEN NOT MATCHED THEN
          INSERT(div_part, custs, adress, adrs2s, citys, states, shps, zipcds, cntrys, cdelcs, conams, emails, faxs,
                 comps, shpnms)
          VALUES(i_div_part, i_r_msg.cust_num, i_r_msg.shp_addr1, i_r_msg.shp_addr2, i_r_msg.shp_city, i_r_msg.shp_st,
                 '0000', i_r_msg.shp_zip, i_r_msg.shp_cntry, '1', i_r_msg.cntct_nm, i_r_msg.cntct_email,
                 i_r_msg.cntct_fax, 'MCL', i_r_msg.cust_nm);
      logs.dbg('Merge ORDP322R');
      MERGE INTO ordp322r r
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    r.div_part = i_div_part
                  AND r.custr = i_r_msg.cust_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET conamr = i_r_msg.cntct_nm, phoner = i_r_msg.cntct_phone, emailr = i_r_msg.cntct_email,
                 faxr = i_r_msg.cntct_fax, cadd1r = i_r_msg.shp_addr1, cityr = i_r_msg.shp_city,
                 stater = i_r_msg.shp_st, zipcdr = i_r_msg.shp_zip, cntryr = i_r_msg.shp_cntry
        WHEN NOT MATCHED THEN
          INSERT(div_part, custr, conamr, phoner, emailr, faxr, cadd1r, cityr, stater, zipcdr, cntryr, statr, compr,
                 shptor)
          VALUES(i_div_part, i_r_msg.cust_num, i_r_msg.cntct_nm, i_r_msg.cntct_phone, i_r_msg.cntct_email,
                 i_r_msg.cntct_fax, i_r_msg.shp_addr1, i_r_msg.shp_city, i_r_msg.shp_st, i_r_msg.shp_zip,
                 i_r_msg.shp_cntry, '1', 'MCL', '0000');
      logs.dbg('Merge SYSP242M');
      MERGE INTO sysp242m m
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    m.div_part = i_div_part
                  AND m.custm = i_r_msg.cust_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET contnm = i_r_msg.cntct_nm, phonem = i_r_msg.cntct_phone, emailm = i_r_msg.cntct_email,
                 faxm = i_r_msg.cntct_fax, cadd1m = i_r_msg.cntct_addr, citym = i_r_msg.shp_city,
                 statem = i_r_msg.shp_st, zipcdm = i_r_msg.shp_zip, cntrym = i_r_msg.shp_cntry
        WHEN NOT MATCHED THEN
          INSERT(div_part, custm, contnm, phonem, emailm, faxm, cadd1m, citym, statem, zipcdm, cntrym, compm)
          VALUES(i_div_part, i_r_msg.cust_num, i_r_msg.cntct_nm, i_r_msg.cntct_phone, i_r_msg.cntct_email,
                 i_r_msg.cntct_fax, i_r_msg.cntct_addr, i_r_msg.shp_city, i_r_msg.shp_st, i_r_msg.shp_zip,
                 i_r_msg.shp_cntry, 'MCL');
    EXCEPTION
      WHEN OTHERS THEN
        logs.err(lar_parm, NULL, FALSE);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust01);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust01,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        -- check prepost CHG
        IF (    l_r_msg.actn_cd = g_c_chg
            AND l_r_msg.prepost_sw IN(l_c_prepost_on, l_c_prepost_off)) THEN
          logs.dbg('Chg Pre-Post Flag');
          upd_prepost_sp(l_div_part, l_r_msg);
        ELSE
          IF l_r_msg.actn_cd IN(g_c_del, g_c_chg) THEN
            logs.dbg('Get Old Cust Status');

            OPEN l_cv
             FOR
               SELECT c.statc
                 FROM sysp200c c
                WHERE c.div_part = l_div_part
                  AND c.acnoc = l_r_msg.cust_num;

            FETCH l_cv
             INTO l_cust_old_stat;

            CLOSE l_cv;

            IF l_r_msg.actn_cd = g_c_del THEN
              logs.dbg('Remove Entry');
              del_sp(l_div_part, l_r_msg);
            END IF;   -- l_r_msg.actn_cd = g_c_del
          END IF;   -- l_r_msg.actn_cd IN(g_c_del, g_c_chg)

          IF l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Get Cust Type');

            OPEN l_cv
             FOR
               SELECT COALESCE((SELECT c.typecc
                                  FROM sysp200c c
                                 WHERE c.div_part = l_div_part
                                   AND c.acnoc = l_r_msg.cust_num
                                   AND c.typecc IS NOT NULL),
                               (SELECT t.dmn_cd
                                  FROM op_cls_dmn_cd_typ t, mclp020b cx
                                 WHERE cx.div_part = l_div_part
                                   AND cx.custb = l_r_msg.cust_num
                                   AND t.div_part = cx.div_part
                                   AND t.cls_typ = 'CRPCDE'
                                   AND t.cls_id = LPAD(cx.corpb, 3, '0')
                                   AND t.dmn_typ = 'CUSTYP'
                                   AND t.dflt_sw = 'Y'),
                               (SELECT t.dmn_cd
                                  FROM op_cls_dmn_cd_typ t
                                 WHERE t.div_part = l_div_part
                                   AND t.cls_typ = 'CRPCDE'
                                   AND t.cls_id = 'ALL'
                                   AND t.dmn_typ = 'CUSTYP'
                                   AND t.dflt_sw = 'Y')
                              )
                 FROM DUAL;

            FETCH l_cv
             INTO l_cust_typ;

            CLOSE l_cv;

            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          END IF;   -- l_r_msg.actn_cd IN(g_c_add, g_c_chg)
        END IF;   -- check prepost CHG

        IF (   l_r_msg.actn_cd = g_c_del
            OR l_r_msg.stat <> l_cust_old_stat) THEN
          logs.dbg('Update Customer Orders for Status Change');
          upd_ords_for_cust_stat_chg_sp(l_r_msg.actn_cd,
                                        l_div_part,
                                        l_r_msg.cust_num,
                                        l_cust_old_stat,
                                        l_r_msg.stat,
                                        l_c_msg_id
                                       );
        END IF;   -- l_r_msg.actn_cd = g_c_del OR l_r_msg.stat <> l_cust_old_stat

        IF l_r_msg.actn_cd NOT IN(g_c_del, g_c_add, g_c_chg) THEN
          l_mq_msg_stat := g_c_prb;
        END IF;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust01,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust01,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust01_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST03_SP
  ||  Maps messages that describe the Divisional Customer
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/04/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 01/06/03 | rhalpai | Added logic to keep security profiles in sync for
  ||                    | new customers.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 08/08/05 | rhalpai | Added update to customer type. PIR2608
  || 12/02/05 | rhalpai | Converted to use merge statement. PIR2608
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 08/19/08 | rhalpai | Reformatted and changed to raise error for add/chg
  ||                    | with null mclane cust or corp code. IM457041
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 03/17/15 | rhalpai | Add logic to include match on mcl_cust when deleting.
  ||                    | Log error msg when no rows found for delete. IM-255704
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust03_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST03_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCUST03';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div        VARCHAR2(2),
      actn_cd    VARCHAR2(3),
      cust       mclp020b.custb%TYPE,
      mcl_cust   mclp020b.mccusb%TYPE,
      crp_cd     NUMBER,
      store_num  mclp020b.storeb%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cust := RTRIM(SUBSTR(i_msg_data, 54, 8));
      l_r_parsed.mcl_cust := RTRIM(SUBSTR(i_msg_data, 62, 6));
      l_r_parsed.crp_cd := string_to_num_fn(SUBSTR(i_msg_data, 68, 3));
      l_r_parsed.store_num := RTRIM(SUBSTR(i_msg_data, 71, 6));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp020b cx
            WHERE cx.div_part = i_div_part
              AND cx.custb = i_r_msg.cust
              AND cx.mccusb = i_r_msg.mcl_cust;

      IF SQL%ROWCOUNT = 0 THEN
        logs.warn('Not found for delete',
                  lar_parm,
                  'Div: '
                  || i_r_msg.div
                  || ' Cust: '
                  || i_r_msg.cust
                  || ' MclCust: '
                  || i_r_msg.mcl_cust
                 );
      END IF;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_c_sysdate  CONSTANT DATE := SYSDATE;
    BEGIN
      MERGE INTO mclp020b cx
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    cx.div_part = i_div_part
                  AND cx.custb = i_r_msg.cust
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET cx.mccusb = i_r_msg.mcl_cust, cx.corpb = i_r_msg.crp_cd, cx.storeb = i_r_msg.store_num,
                 cx.last_chg_ts = l_c_sysdate, cx.user_id = 'QCUST03'
        WHEN NOT MATCHED THEN
          INSERT(div_part, custb, mccusb, corpb, storeb, last_chg_ts, user_id)
          VALUES(i_div_part, i_r_msg.cust, i_r_msg.mcl_cust, i_r_msg.crp_cd, i_r_msg.store_num, l_c_sysdate, 'QCUST03');
    END merge_sp;

    PROCEDURE process_msg_sp(
      i_div_part     IN      NUMBER,
      i_msg_data     IN      VARCHAR2,
      o_mq_msg_stat  OUT     VARCHAR2
    ) IS
      l_r_msg  l_rt_msg;
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Parse MQ Msg Data');
      l_r_msg := parse_msg_fn(i_msg_data);
      logs.dbg('Process Msg');

      CASE
        WHEN l_r_msg.actn_cd = g_c_del THEN
          logs.dbg('Remove Entry');
          del_sp(i_div_part, l_r_msg);
        WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
          excp.assert((l_r_msg.mcl_cust IS NOT NULL), 'MclCust cannot be NULL');
          excp.assert((l_r_msg.crp_cd IS NOT NULL), 'CorpCd cannot be NULL');
          logs.dbg('Add/Chg Entry');
          merge_sp(i_div_part, l_r_msg);
      END CASE;
    EXCEPTION
      WHEN excp.gx_assert_fail THEN
        o_mq_msg_stat := g_c_prb;
        logs.err('Assertion Failure: ' || SQLERRM, lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust03);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust03,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, RTRIM(l_r_msg.mq_msg_data, CHR(0)), l_mq_msg_stat);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust03,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust03,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust03_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST05_SP
  ||  Maps messages that detail the different loads that the division can either
  ||  assign customers to for fixed routing ore for special billings (such as
  ||  X-Bills, P00 distributions, etc). It controls when the load should bill,
  ||  the discription, dates and times and other information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/30/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 04/09/02 | rhalpai | Added call to SYNCLOAD_SP whenever updates are applied
  || 05/27/02 | rhalpai | Changed cursor in SYNCLOAD-PROCESS to ensure all order
  ||                    | lines are in open status when calling SYNCLOAD_SP.
  || 07/23/02 | rhalpai | Changed call to SYNCLOAD_SP to pass only order_num parm
  ||                    | and updated OPEN_ORDERS_CUR cursor appropriately.
  || 08/19/02 | PCUNNIN | Add test_bil_load_sw to qcust05_sp
  || 10/31/02 | rhalpai | Changed to reset special distributions to their default
  ||                    | load when their current load is deleted. Added update
  ||                    | of LLR, departure and ETA dates for orders on load during
  ||                    | changes but customer not assigned to load. Changed logic
  ||                    | to call syncload_sp specifically for deletes or changes
  ||                    | with separate cursor loops.
  || 01/08/03 | rhalpai | Changed to include suspended orders in calls to SYNCLOAD_SP.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status from call to
  ||                    | assign_order_to_default_sp.
  || 04/18/07 | rhalpai | Redesigned. Removed status parm, added process control,
  ||                    | changed error handler to use standard parm list.
  || 11/08/07 | rhalpai | Changed to suspend regular orders on deleted testbill
  ||                    | load. IM346041
  || 08/19/08 | rhalpai | Remove check for detail with status not in O,I,S,C in
  ||                    | cursor orders_cur. PIR6364
  || 11/24/08 | rhalpai | Added default value for destination.
  || 05/02/11 | rhalpai | Changed cursor to ignore any orders matching
  ||                    | LLRDate/Cust/Load/Stop entries from new override
  ||                    | table before processing Syncload. PIR9348
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 02/17/14 | rhalpai | Change logic to combine assigned orders and unassigned
  ||                    | orders for syncload call within cursor. Change logic to
  ||                    | make a single call to syncload and remove
  ||                    | treat_dist_as_reg from call to syncload. PIR13455
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust05_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    -- Generic
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST05_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCUST05';

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div_id         div_mstr_di1d.div_id%TYPE,   --   div id
      actn_cd        VARCHAR2(3),   --          action code
      load_num       mclp120c.loadc%TYPE,   --  load number
      llr_day        mclp120c.llrcdc%TYPE,   -- load label release cutoff day of week
      llr_tm         NUMBER,   --               load label release cutoff time
      llr_wk         NUMBER,   --               load label release weeks add-on
      ord_prcs_day   mclp120c.lopwdc%TYPE,   -- load order process window start day
      ord_prcs_tm    NUMBER,   --               load order process window start HHMM
      dest           mclp120c.destc%TYPE,   --  destination
      trlr_sz        mclp120c.traszc%TYPE,   -- size of trailer
      trlr_typ       mclp120c.tratyc%TYPE,   -- type of trailer
      max_cube       NUMBER,   --               acceptable cube
      max_wt         NUMBER,   --               acceptable weight
      dep_day        mclp120c.depdac%TYPE,   -- departure day
      dep_tm         NUMBER,   --               departure time
      dep_wk         NUMBER,   --               departure weeks add-on
      trlr_rstrctns  mclp120c.trresc%TYPE,   -- trailer restrictions
      ord_cut_day    mclp120c.ldordc%TYPE,   -- load order cutoff day
      ord_cut_tm     NUMBER,   --               load order cutoff time
      ord_cut_wk     NUMBER,   --               load order cutoff weeks add-on
      prc_day        mclp120c.lpwdc%TYPE,   --  load pricing window day
      prc_tm         NUMBER,   --               load pricing window start time
      prc_wk         NUMBER,   --               load pricing window weeks add-on
      business_grp   VARCHAR2(3),   --          msg data contains 3 char bus group (GRO,GMP,etc)
      gmp            mclp120c.lbsgpc%TYPE,   -- load business group GMP ("Y" where v_business_grp = "GMP")
      attch_dist     mclp120c.aadisc%TYPE,   -- attach distribution to load (Y/N)
      tbill          mclp120c.test_bil_load_sw%TYPE   -- load dedicated to test bills
    );

    l_r_msg              l_rt_msg;
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    TYPE rt_load_info IS RECORD(
      load_num  mclp120c.loadc%TYPE,
      llr_day   VARCHAR2(3),
      llr_tm    NUMBER,
      llr_wk    NUMBER,
      dep_day   VARCHAR2(3),
      dep_tm    NUMBER,
      dep_wk    NUMBER
    );

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div_id := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := SUBSTR(i_msg_data, 41, 3);
      l_r_parsed.load_num := SUBSTR(i_msg_data, 54, 4);
      l_r_parsed.llr_day := SUBSTR(i_msg_data, 58, 3);
      l_r_parsed.llr_tm := NVL(string_to_num_fn(SUBSTR(i_msg_data, 61, 4)), 0);
      l_r_parsed.llr_wk := NVL(string_to_num_fn(SUBSTR(i_msg_data, 65, 1)), 0);
      l_r_parsed.dest := NVL(RTRIM(SUBSTR(i_msg_data, 66, 30)), 'NAME NOT DEFINED');
      l_r_parsed.trlr_sz := NVL(string_to_num_fn(SUBSTR(i_msg_data, 96, 2)), 0);
      l_r_parsed.trlr_typ := RTRIM(SUBSTR(i_msg_data, 98, 8));
      l_r_parsed.max_cube := NVL(string_to_num_fn(SUBSTR(i_msg_data, 106, 4)), 0);
      l_r_parsed.max_wt := NVL(string_to_num_fn(SUBSTR(i_msg_data, 110, 5)), 0);
      l_r_parsed.dep_day := SUBSTR(i_msg_data, 115, 3);
      l_r_parsed.dep_tm := NVL(string_to_num_fn(SUBSTR(i_msg_data, 118, 4)), 0);
      l_r_parsed.dep_wk := NVL(string_to_num_fn(SUBSTR(i_msg_data, 122, 1)), 0);
      l_r_parsed.trlr_rstrctns := RTRIM(SUBSTR(i_msg_data, 123, 20));
      l_r_parsed.ord_cut_day := SUBSTR(i_msg_data, 143, 3);
      l_r_parsed.ord_cut_tm := NVL(string_to_num_fn(SUBSTR(i_msg_data, 146, 4)), 0);
      l_r_parsed.ord_cut_wk := NVL(string_to_num_fn(SUBSTR(i_msg_data, 150, 1)), 0);
      l_r_parsed.prc_day := SUBSTR(i_msg_data, 151, 3);
      l_r_parsed.prc_tm := NVL(string_to_num_fn(SUBSTR(i_msg_data, 154, 4)), 0);
      l_r_parsed.prc_wk := NVL(string_to_num_fn(SUBSTR(i_msg_data, 158, 1)), 0);
      l_r_parsed.business_grp := SUBSTR(i_msg_data, 159, 3);
      l_r_parsed.gmp :=(CASE l_r_parsed.business_grp
                          WHEN 'GMP' THEN 'Y'
                          ELSE 'N'
                        END);
      l_r_parsed.ord_prcs_day := SUBSTR(i_msg_data, 162, 3);
      l_r_parsed.ord_prcs_tm := string_to_num_fn(SUBSTR(i_msg_data, 165, 4));
      l_r_parsed.attch_dist := SUBSTR(i_msg_data, 169, 1);
      l_r_parsed.tbill := SUBSTR(i_msg_data, 170, 1);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION load_info_fn(
      i_div_part  IN  NUMBER,
      i_load_num  IN  VARCHAR2
    )
      RETURN rt_load_info IS
      l_r_load_info  rt_load_info;
      l_cv_load      SYS_REFCURSOR;
    BEGIN
      OPEN l_cv_load
       FOR
         SELECT loadc, llrcdc, NVL(llrctc, 0), NVL(llrwkc, 0), depdac, NVL(deptmc, 0), NVL(depwkc, 0)
           FROM mclp120c
          WHERE div_part = i_div_part
            AND loadc = i_load_num;

      FETCH l_cv_load
       INTO l_r_load_info;

      RETURN(l_r_load_info);
    END load_info_fn;

    FUNCTION get_next_day_fn(
      i_start_dt  IN  DATE,
      i_time      IN  NUMBER,
      i_day       IN  VARCHAR2
    )
      RETURN DATE AS
    BEGIN
      RETURN NEXT_DAY(TO_DATE(TO_CHAR(i_start_dt, 'YYYYMMDD') || LPAD(i_time, 4, '0'), 'YYYYMMDDHH24MI'), i_day);
    END get_next_day_fn;

    PROCEDURE upd_ords_sp(
      i_div_part       IN  NUMBER,
      i_r_msg          IN  l_rt_msg,
      i_old_llr_wk     IN  NUMBER DEFAULT NULL,
      i_tbill_load_sw  IN  VARCHAR2 DEFAULT NULL
    ) IS
      TYPE l_rt_load_ords IS RECORD(
        llr_ts               DATE,
        depart_ts            DATE,
        cust_id              sysp200c.acnoc%TYPE,
        stop_num             NUMBER,
        eta_ts               DATE,
        t_suspnd_ords        type_ntab,
        t_sync_ords          type_ntab,
        t_adj_unassgnd_ords  type_ntab
      );

      TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

      l_t_load_ords            l_tt_load_ords;
      l_t_suspnd_ords          type_ntab              := type_ntab();
      l_t_sync_ords            type_ntab              := type_ntab();
      l_t_adj_unassgnd_ords    type_ntab              := type_ntab();
      l_llr_dt                 DATE;
      l_load_depart_sid        NUMBER;
      l_llr_ts                 DATE;
      l_depart_ts              DATE;
      l_stop_num               NUMBER;
      l_eta_ts                 DATE;
      l_c_log_rsn_cd  CONSTANT mclp300d.reasnd%TYPE   := 'CUSTLDSY';
    BEGIN
      logs.dbg('Get Load and Order Info');

      SELECT   ld.llr_ts,
               ld.depart_ts,
               se.cust_id,
               se.stop_num,
               se.eta_ts,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id
                                AND a.stata = 'O'
                                AND a.dsorda <> 'D'
                                AND i_r_msg.actn_cd = g_c_del
                                AND i_tbill_load_sw = 'Y'
                            ) AS type_ntab
                   ) AS suspnd_ords,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id
                                AND a.stata IN('O', 'S')
                                AND (   (   EXISTS(SELECT 1
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
                                     OR (    i_r_msg.actn_cd = g_c_del
                                         AND NOT EXISTS(SELECT 1
                                                          FROM mclp040d d
                                                         WHERE d.div_part = a.div_part
                                                           AND d.loadd = ld.load_num
                                                           AND d.custd = a.custa)
                                         AND NOT EXISTS(SELECT 1
                                                          FROM cust_rte_ovrrd_rt3c cro
                                                         WHERE cro.div_part = a.div_part
                                                           AND cro.cust_id = a.custa
                                                           AND cro.llr_dt = ld.llr_dt
                                                           AND cro.load_num = ld.load_num
                                                           AND cro.stop_num = se.stop_num)
                                        )
                                    )
                            ) AS type_ntab
                   ) AS sync_ords,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id
                                AND a.stata IN('O', 'S')
                                AND i_r_msg.actn_cd <> g_c_del
                                AND NOT EXISTS(SELECT 1
                                                 FROM mclp040d d
                                                WHERE d.div_part = a.div_part
                                                  AND d.loadd = ld.load_num
                                                  AND d.custd = a.custa)
                                AND NOT EXISTS(SELECT 1
                                                 FROM cust_rte_ovrrd_rt3c cro
                                                WHERE cro.div_part = a.div_part
                                                  AND cro.cust_id = a.custa
                                                  AND cro.llr_dt = ld.llr_dt
                                                  AND cro.load_num = ld.load_num
                                                  AND cro.stop_num = se.stop_num)
                            ) AS type_ntab
                   ) AS adj_unassgnd_ords
      BULK COLLECT INTO l_t_load_ords
          FROM load_depart_op1f ld, stop_eta_op1g se
         WHERE ld.div_part = i_div_part
           AND ld.load_num = i_r_msg.load_num
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata IN('O', 'S'))
           -- bypass ords on cust route overrides for non-tbill loads
           AND (   i_tbill_load_sw = 'Y'
                OR NOT EXISTS(SELECT 1
                                FROM cust_rte_ovrrd_rt3c cro
                               WHERE cro.div_part = ld.div_part
                                 AND cro.cust_id = se.cust_id
                                 AND cro.llr_dt = ld.llr_dt
                                 AND cro.load_num = ld.load_num
                                 AND cro.stop_num = se.stop_num)
               )
      ORDER BY ld.llr_ts, se.cust_id;

      IF l_t_load_ords.COUNT > 0 THEN
        logs.dbg('Upd Ords');
        FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
          l_t_suspnd_ords := l_t_load_ords(i).t_suspnd_ords;
          l_t_adj_unassgnd_ords := l_t_load_ords(i).t_adj_unassgnd_ords;

          IF l_t_load_ords(i).t_sync_ords.COUNT > 0 THEN
            logs.dbg('Append Assigned Ords');
            FOR j IN l_t_load_ords(i).t_sync_ords.FIRST .. l_t_load_ords(i).t_sync_ords.LAST LOOP
              l_t_sync_ords.EXTEND;
              l_t_sync_ords(l_t_sync_ords.LAST) := l_t_load_ords(i).t_sync_ords(j);
            END LOOP;
          END IF;   -- l_t_load_ords(i).t_sync_ords.COUNT > 0

          IF l_t_suspnd_ords.COUNT > 0 THEN
            logs.dbg('Suspend Order on Deleted Load');
            FOR j IN l_t_suspnd_ords.FIRST .. l_t_suspnd_ords.LAST LOOP
              upd_ord_stat_sp(i_div_part,
                              l_t_suspnd_ords(j),
                              'S',
                              'RSPN6',
                              'QCUST05',
                              'ORDER ON DELETED LOAD ' || i_r_msg.load_num
                             );
            END LOOP;
          END IF;   -- l_t_suspnd_ords.COUNT > 0

          IF l_t_adj_unassgnd_ords.COUNT > 0 THEN
            l_llr_dt := NEXT_DAY(TRUNC(l_t_load_ords(i).llr_ts) -(i_old_llr_wk * 7) - 1, i_r_msg.llr_day);
            logs.dbg('Get LLR/Depart');
            op_order_load_pk.get_llr_depart_sp(i_div_part, l_llr_dt, i_r_msg.load_num, l_llr_ts, l_depart_ts);
            logs.dbg('Get LoadDepartSid');
            l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, l_llr_ts, i_r_msg.load_num);
            -- adjust the eta date based on the difference in old and new depart dates
            l_eta_ts := l_t_load_ords(i).eta_ts +(TRUNC(l_depart_ts) - TRUNC(l_t_load_ords(i).depart_ts));

            -- use old eta_ts if adjusted eta_ts is before new depart_ts and old eta_ts is after
            -- else use date from new depart_ts + 1 and time from old eta_ts
            IF l_eta_ts < l_depart_ts THEN
              l_eta_ts :=(CASE
                            WHEN l_t_load_ords(i).eta_ts >= l_depart_ts THEN l_t_load_ords(i).eta_ts
                            ELSE TO_DATE(TO_CHAR(l_depart_ts + 1, 'YYYYMMDD')
                                         || TO_CHAR(l_t_load_ords(i).eta_ts, 'HH24MI'),
                                         'YYYYMMDDHH24MI'
                                        )
                          END
                         );
            END IF;   -- l_eta_ts < l_depart_ts

            logs.dbg('Get Stop/Eta');
            op_order_load_pk.get_stop_eta_sp(i_div_part,
                                             l_load_depart_sid,
                                             l_t_load_ords(i).cust_id,
                                             l_stop_num,
                                             l_eta_ts,
                                             l_eta_ts,
                                             l_t_load_ords(i).stop_num
                                            );
            logs.dbg('Move Ords');
            op_order_load_pk.move_ords_sp(i_div_part,
                                          l_t_load_ords(i).cust_id,
                                          l_load_depart_sid,
                                          l_stop_num,
                                          l_eta_ts,
                                          l_t_load_ords(i).llr_ts,
                                          i_r_msg.load_num,
                                          l_t_load_ords(i).depart_ts,
                                          l_t_load_ords(i).stop_num,
                                          l_t_load_ords(i).eta_ts,
                                          l_c_log_rsn_cd,
                                          i_user_id,
                                          l_t_adj_unassgnd_ords
                                         );
          END IF;   -- l_t_adj_unassgnd_ords.COUNT > 0
        END LOOP;

        IF l_t_sync_ords.COUNT > 0 THEN
          logs.dbg('Syncload Ords');
          op_order_load_pk.syncload_sp(i_div_part, l_c_log_rsn_cd, l_t_sync_ords);
        END IF;   -- l_t_sync_ords.COUNT > 0
      END IF;   -- l_t_load_ords.COUNT > 0
    END upd_ords_sp;

    PROCEDURE process_msg_sp(
      i_div_part     IN      NUMBER,
      i_msg_data     IN      VARCHAR2,
      o_mq_msg_stat  OUT     VARCHAR2
    ) IS
      l_r_msg        l_rt_msg;
      l_r_load_save  rt_load_info;
      l_tbill_sw     mclp120c.test_bil_load_sw%TYPE;
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Parse MQ Msg Data');
      l_r_msg := parse_msg_fn(i_msg_data);

      IF l_r_msg.actn_cd = g_c_del THEN
        logs.dbg('Remove Load');

        DELETE FROM mclp120c
              WHERE div_part = i_div_part
                AND loadc = l_r_msg.load_num
          RETURNING test_bil_load_sw
               INTO l_tbill_sw;

        IF SQL%FOUND THEN
          logs.dbg('Reassign Orders for Deleted Load');
          upd_ords_sp(i_div_part, l_r_msg, NULL, l_tbill_sw);
        END IF;   -- SQL%FOUND
      ELSIF l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
        logs.dbg('Save Current Load Info');
        l_r_load_save := load_info_fn(i_div_part, l_r_msg.load_num);
        logs.dbg('Merge MCLP120C');
        MERGE INTO mclp120c c
             USING (SELECT 1 tst
                      FROM DUAL) x
                ON (    c.div_part = i_div_part
                    AND c.loadc = l_r_msg.load_num
                    AND x.tst > 0)
          WHEN MATCHED THEN
            UPDATE
               SET c.llrcdc = l_r_msg.llr_day, c.llrctc = l_r_msg.llr_tm, c.llrwkc = l_r_msg.llr_wk,
                   c.lopwdc = l_r_msg.ord_prcs_day, c.lopwtc = l_r_msg.ord_prcs_tm, c.destc = l_r_msg.dest,
                   c.traszc = l_r_msg.trlr_sz, c.tratyc = l_r_msg.trlr_typ, c.accubc = l_r_msg.max_cube,
                   c.acwgtc = l_r_msg.max_wt, c.depdac = l_r_msg.dep_day, c.deptmc = l_r_msg.dep_tm,
                   c.depwkc = l_r_msg.dep_wk, c.trresc = l_r_msg.trlr_rstrctns, c.ldordc = l_r_msg.ord_cut_day,
                   c.ldortc = l_r_msg.ord_cut_tm, c.ldorwc = l_r_msg.ord_cut_wk, c.lpwdc = l_r_msg.prc_day,
                   c.lpwtc = l_r_msg.prc_tm, c.lpwwc = l_r_msg.prc_wk, c.lbsgpc = l_r_msg.gmp,
                   c.aadisc = l_r_msg.attch_dist, c.test_bil_load_sw = l_r_msg.tbill
          WHEN NOT MATCHED THEN
            INSERT(div_part, loadc, llrcdc, llrctc, llrwkc, lopwdc, lopwtc, destc, traszc, tratyc, accubc, acwgtc,
                   depdac, deptmc, depwkc, trresc, ldordc, ldortc, ldorwc, lpwdc, lpwtc, lpwwc, lbsgpc, aadisc,
                   test_bil_load_sw)
            VALUES(i_div_part, l_r_msg.load_num, l_r_msg.llr_day, l_r_msg.llr_tm, l_r_msg.llr_wk, l_r_msg.ord_prcs_day,
                   l_r_msg.ord_prcs_tm, l_r_msg.dest, l_r_msg.trlr_sz, l_r_msg.trlr_typ, l_r_msg.max_cube,
                   l_r_msg.max_wt, l_r_msg.dep_day, l_r_msg.dep_tm, l_r_msg.dep_wk, l_r_msg.trlr_rstrctns,
                   l_r_msg.ord_cut_day, l_r_msg.ord_cut_tm, l_r_msg.ord_cut_wk, l_r_msg.prc_day, l_r_msg.prc_tm,
                   l_r_msg.prc_wk, l_r_msg.gmp, l_r_msg.attch_dist, l_r_msg.tbill);

        -- if change to existing load
        IF l_r_load_save.load_num IS NOT NULL THEN
          IF (    l_r_msg.tbill = 'N'
              AND NOT(    l_r_msg.llr_day = l_r_load_save.llr_day
                      AND l_r_msg.llr_tm = l_r_load_save.llr_tm
                      AND l_r_msg.llr_wk = l_r_load_save.llr_wk
                      AND l_r_msg.dep_day = l_r_load_save.dep_day
                      AND l_r_msg.dep_tm = l_r_load_save.dep_tm
                      AND l_r_msg.dep_wk = l_r_load_save.dep_wk
                     )
             ) THEN
            -- Commit prior to upd ords since load_depart_sid_fn is autonomous
            COMMIT;
            logs.dbg('Upd Orders on Changed Load');
            upd_ords_sp(i_div_part, l_r_msg, NVL(l_r_load_save.llr_wk, 0));
          END IF;   -- l_r_msg.tbill = 'N'
        END IF;   -- l_r_load_save.load_num IS NOT NULL
      END IF;   -- l_r_msg.actn_cd = g_c_del
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust05);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust05,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, l_r_mq_msg.mq_msg_data, l_mq_msg_stat);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust05,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust05,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust05_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST06_SP
  ||  Maps messages that detail the load and stop that the customer is assigned
  ||  to for fixed routing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/31/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/09/01 | rhalpai | Changed "union" to "union all" in open_orders_cur
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 10/30/01 | rhalpai | Moved process to call syncload_sp ADD's/CHG's to run
  ||                    | only for REF's (indicating all messages in a batch are
  ||                    | processed)
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 05/27/02 | rhalpai | Changed cursor in SYNCLOAD-PROCESS to ensure all order
  ||                    | lines are in open status when calling SYNCLOAD_SP.
  ||                    | Changed call to syncload_sp to pass div id from
  ||                    | open_orders_cur instead of using l_divd variable with
  ||                    | value parsed from MQ message. (REF msgs have XX for div)
  || 07/22/02 | rhalpai | Split cursor in SYNCLOAD-PROCESS into 2 cursors with 2
  ||                    | calling cursor loops to improve efficiency.
  || 07/23/02 | rhalpai | Changed call to SYNCLOAD_SP to pass only order_num parm
  ||                    | and updated OPEN_ORDERS_CUR cursor appropriately.
  || 10/01/02 | rhalpai | Added insert to MCLANE_DELETE_LOAD when no rows are
  ||                    | updated during an ADD. Added code to update stop and eta
  ||                    | on special distribution orders when cust/load is changed.
  || 10/31/02 | rhalpai | Changed logic to call SYNCLOAD_SP.
  || 01/08/03 | rhalpai | Changed to include suspended orders in calls to SYNCLOAD_SP.
  || 03/14/03 | rhalpai | Changed logic to include new PROD_TYP column on MCLP040D.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status from call to
  ||                    | assign_order_to_default_sp.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 08/19/08 | rhalpai | Reformatted and changed to call common procedure,
  ||                    | REASSIGN_CUST_ORDS_SP, to initiate SYNCLOAD process
  ||                    | when appropriate. PIR6364
  || 11/24/08 | rhalpai | Added check for change in stop number to determine
  ||                    | whether to reassign customer orders.
  || 12/29/08 | rhalpai | Changed logic to use new ATTCH_DIST_SW column. PIR6113
  || 10/06/09 | rhalpai | Changed to use parm to bypass Default Dist logic for
  ||                    | not attaching Dist ords to cust load. Need to bypass
  ||                    | for WJ as they have their own MF system to handle
  ||                    | this. PIR8100
  || 05/20/11 | rhalpai | Removed logic for ATTCH_DIST_SW. PIR9030
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 12/22/14 | rhalpai | Add logic to reset DistFirstDay when removing or
  ||                    | changing load. PIR14445
  || 12/07/15 | rhalpai | Change logic to include new RECUR_WK, EFF_DT columns
  ||                    | on MCLP040D to allow support of cust load/stop
  ||                    | recurrence logic (i.e.: WJ A/B bi-weekly load schedule).
  ||                    | PIR14916
  || 09/16/16 | rhalpai | Change logic to treat recur_wk of zero from MQ msg as
  ||                    | one. SDOPS-499
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust06_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST06_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCUST06';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_c_rsn_cd  CONSTANT VARCHAR2(8)                        := 'CUSTLDSY';

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div            VARCHAR2(2),
      actn_cd        VARCHAR2(3),
      cust_num       mclp040d.custd%TYPE,
      load_num       mclp040d.loadd%TYPE,
      stop_num       NUMBER,
      eta_day        mclp040d.dayrcd%TYPE,
      eta_tm         NUMBER,
      eta_wk         NUMBER,
      delv_wndw      mclp040d.delwid%TYPE,
      hrs_opn        mclp040d.hropnd%TYPE,
      delv_rstrctns  mclp040d.delrsd%TYPE,
      prod_typ       mclp040d.prod_typ%TYPE,
      recur_wk       NUMBER,
      eff_dt         DATE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cust_num := RTRIM(SUBSTR(i_msg_data, 54, 8));
      l_r_parsed.load_num := RTRIM(SUBSTR(i_msg_data, 62, 4));
      l_r_parsed.stop_num := string_to_num_fn(SUBSTR(i_msg_data, 66, 2));
      l_r_parsed.eta_day := RTRIM(SUBSTR(i_msg_data, 68, 3));
      l_r_parsed.eta_tm := string_to_num_fn(SUBSTR(i_msg_data, 71, 4));
      l_r_parsed.eta_wk := string_to_num_fn(SUBSTR(i_msg_data, 75, 1));
      l_r_parsed.delv_wndw := TRIM(SUBSTR(i_msg_data, 76, 25));
      l_r_parsed.hrs_opn := TRIM(SUBSTR(i_msg_data, 101, 10));
      l_r_parsed.delv_rstrctns := TRIM(SUBSTR(i_msg_data, 111, 30));
      l_r_parsed.prod_typ := NVL(RTRIM(SUBSTR(i_msg_data, 141, 3)), 'GRO');
      l_r_parsed.recur_wk := NVL(string_to_num_fn(SUBSTR(i_msg_data, 145, 2)), 1);
      l_r_parsed.eff_dt := NVL(TO_DATE(TRIM(SUBSTR(i_msg_data, 147, 10)), 'YYYY-MM-DD'), DATE '1900-01-01');

      IF l_r_parsed.recur_wk = 0 THEN
        l_r_parsed.recur_wk := 1;
      END IF;

      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE reset_dist_frst_day_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      logs.dbg('Reset DistFrstDay');

      UPDATE sysp200c c
         SET c.dist_frst_day = NULL
       WHERE c.dist_frst_day IS NOT NULL
         AND c.div_part = i_div_part
         AND c.acnoc = i_r_msg.cust_num
         AND NOT EXISTS(SELECT 1
                          FROM mclp040d d
                         WHERE d.div_part = c.div_part
                           AND d.custd = c.acnoc
                           AND d.dayrcd = c.dist_frst_day);
    END reset_dist_frst_day_sp;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      logs.dbg('Remove MCLP040D Entry');

      DELETE FROM mclp040d d
            WHERE d.div_part = i_div_part
              AND d.custd = i_r_msg.cust_num
              AND d.loadd = i_r_msg.load_num;

      logs.dbg('Reset DistFrstDay for Removed Load Assignment');
      reset_dist_frst_day_sp(i_div_part, i_r_msg);
      logs.dbg('Reassign Cust Orders for Removed Load Assignment');
      reassign_cust_ords_sp(i_div_part, i_r_msg.cust_num, l_c_rsn_cd, i_r_msg.load_num);
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_cv           SYS_REFCURSOR;
      l_reassign_sw  VARCHAR2(1);
    BEGIN
      logs.dbg('Open Cursor to Indicate Order Reassignment');

      OPEN l_cv
       FOR
         SELECT (CASE
                   WHEN NOT(    d.dayrcd = i_r_msg.eta_day
                            AND d.etad = i_r_msg.eta_tm
                            AND d.wkoffd = i_r_msg.eta_wk
                            AND d.prod_typ = i_r_msg.prod_typ
                            AND d.stopd = i_r_msg.stop_num
                            AND d.recur_wk = i_r_msg.recur_wk
                            AND d.eff_dt = i_r_msg.eff_dt
                           ) THEN 'Y'
                   ELSE 'N'
                 END
                )
           FROM mclp040d d
          WHERE d.div_part = i_div_part
            AND d.custd = i_r_msg.cust_num
            AND d.loadd = i_r_msg.load_num;

      logs.dbg('Fetch Cursor to Indicate Order Reassignment');

      FETCH l_cv
       INTO l_reassign_sw;

      CLOSE l_cv;

      logs.dbg('Merge MCLP040D');
      MERGE INTO mclp040d d
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    d.div_part = i_div_part
                  AND d.custd = i_r_msg.cust_num
                  AND d.loadd = i_r_msg.load_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET d.stopd = i_r_msg.stop_num, d.dayrcd = i_r_msg.eta_day, d.etad = i_r_msg.eta_tm,
                 d.wkoffd = i_r_msg.eta_wk, d.delwid = i_r_msg.delv_wndw, d.hropnd = i_r_msg.hrs_opn,
                 d.delrsd = i_r_msg.delv_rstrctns, d.prod_typ = i_r_msg.prod_typ, d.recur_wk = i_r_msg.recur_wk,
                 d.eff_dt = i_r_msg.eff_dt
        WHEN NOT MATCHED THEN
          INSERT(div_part, custd, loadd, stopd, dayrcd, etad, wkoffd, delwid, hropnd, delrsd, prod_typ, recur_wk,
                 eff_dt)
          VALUES(i_div_part, i_r_msg.cust_num, i_r_msg.load_num, i_r_msg.stop_num, i_r_msg.eta_day, i_r_msg.eta_tm,
                 i_r_msg.eta_wk, i_r_msg.delv_wndw, i_r_msg.hrs_opn, i_r_msg.delv_rstrctns, i_r_msg.prod_typ,
                 i_r_msg.recur_wk, i_r_msg.eff_dt);

      IF l_reassign_sw IS NULL THEN
        logs.dbg('Reassign Cust Orders for New Load Assignment');
        reassign_cust_ords_sp(i_div_part, l_r_msg.cust_num, l_c_rsn_cd, NULL, 'Y');
      ELSIF l_reassign_sw = 'Y' THEN
        logs.dbg('Reset DistFrstDay for Changed Load Assignment');
        reset_dist_frst_day_sp(i_div_part, l_r_msg);
        logs.dbg('Reassign Cust Orders for Changed Load Assignment');
        reassign_cust_ords_sp(i_div_part, l_r_msg.cust_num, l_c_rsn_cd, l_r_msg.load_num);
      END IF;   -- l_cv%NOTFOUND
    EXCEPTION
      WHEN OTHERS THEN
        logs.err(lar_parm, NULL, FALSE);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust06);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust06,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      <<msg_loop>>
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP msg_loop;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust06,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust06,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust06_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST07_SP
  ||  Maps messages that describe the Customer Jurisdiction
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/07/01 | rhalpai | Original
  || 09/27/01 | rhalpai | trimmed taxjrc
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 02/05/02 | rhalpai | Updated to reflect index change on MCLP030C by
  ||                    | removing the reference to taxjrc from the Delete and
  ||                    | adding an Update statement for custc when an insert
  ||                    | fails with dups.
  || 12/05/02 | rhalpai | Added tax_city_cd and tax_cnty_cd.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 11/06/09 | rhalpai | Converted to use merge stmt. IM527525
  || 03/12/10 | WZROBIN | Added call for Cig OP Refresh. PIR6316
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust07_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST07_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCUST07';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div      VARCHAR2(2),
      actn_cd  VARCHAR2(3),
      cust     mclp030c.custc%TYPE,
      jrsdctn  mclp030c.taxjrc%TYPE,
      city_cd  NUMBER,
      cnty_cd  NUMBER
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cust := SUBSTR(i_msg_data, 54, 8);
      l_r_parsed.jrsdctn := RTRIM(SUBSTR(i_msg_data, 62, 3));
      l_r_parsed.city_cd := string_to_num_fn(SUBSTR(i_msg_data, 65, 3));
      l_r_parsed.cnty_cd := string_to_num_fn(SUBSTR(i_msg_data, 68, 3));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp030c ct
            WHERE ct.div_part = i_div_part
              AND ct.custc = i_r_msg.cust;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO mclp030c ct
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    ct.div_part = i_div_part
                  AND ct.custc = i_r_msg.cust
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET ct.taxjrc = i_r_msg.jrsdctn, ct.tax_city_cd = i_r_msg.city_cd, ct.tax_cnty_cd = i_r_msg.cnty_cd
        WHEN NOT MATCHED THEN
          INSERT(div_part, custc, taxjrc, tax_city_cd, tax_cnty_cd)
          VALUES(i_div_part, i_r_msg.cust, i_r_msg.jrsdctn, i_r_msg.city_cd, i_r_msg.cnty_cd);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust07);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust07,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust07,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust07,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust07_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST08_SP
  ||  Maps messages that describe Group/Customer Unconditional Subs
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/07/01 | rhalpai | Original
  || 09/12/01 | rhalpai | Added rtrim to customer grouping input
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 12/10/01 | rhalpai | Removed STDTC from where clause for Delete and Update
  ||                    | as it is no longer part of the unique index.
  || 12/17/01 | rhalpai | Added set for STDTC in Update
  || 03/15/02 | rhalpai | Added Customer Level Subs
  || 08/28/02 | rhalpai | Changed deletes and updates of unconditional subs to be
  ||                    | based on customer/item/uom for customer level subs and
  ||                    | div/group/item/uom for group level subs.
  || 11/18/02 | rhalpai | Changed to call OP_MAINTAIN_SUBS_PK for all maintenance.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status from calls to
  ||                    | op_maintain_subs_pk.maintain_cust_sub_sp and
  ||                    | op_maintain_subs_pk.maintain_group_sub_sp.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 10/28/09 | rhalpai | Converted to call new SUB_MAINT_SP. PIR4342
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust08_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST08_SP';
    lar_parm                  logs.tar_parm;
    l_div_part                NUMBER;
    l_c_msg_id       CONSTANT VARCHAR2(7)                        := 'QCUST08';
    l_mq_msg_stat             mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id               NUMBER;
    l_sub_maint_err_msg       typ.t_maxvc2;
    l_catlg_num               sawp505e.catite%TYPE;
    l_sub_item                sawp505e.catite%TYPE;
    l_c_msg_typ_grp  CONSTANT VARCHAR2(1)                        := 'G';
    l_c_msg_typ_cus  CONSTANT VARCHAR2(1)                        := 'C';
    l_c_sub_typ_grp  CONSTANT VARCHAR2(3)                        := 'GRP';
    l_c_sub_typ_cus  CONSTANT VARCHAR2(3)                        := 'CUS';
    l_c_unc_grp_sub  CONSTANT VARCHAR2(3)                        := 'UGP';
    l_c_unc_cus_sub  CONSTANT VARCHAR2(3)                        := 'UCS';

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div       VARCHAR2(2),
      actn_cd   VARCHAR2(3),
      grp_id    sysp200c.retgpc%TYPE,
      item_num  sawp505e.iteme%TYPE,
      uom       sawp505e.uome%TYPE,
      sub_item  sawp505e.iteme%TYPE,
      sub_uom   sawp505e.uome%TYPE,
      start_dt  DATE,
      end_dt    DATE,
      typ_cd    VARCHAR2(1),
      cust      sysp200c.acnoc%TYPE
    );

    l_r_msg                   l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.grp_id := RTRIM(SUBSTR(i_msg_data, 54, 8));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 62, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 71, 3);
      l_r_parsed.sub_item := SUBSTR(i_msg_data, 74, 9);
      l_r_parsed.sub_uom := SUBSTR(i_msg_data, 83, 3);
      l_r_parsed.start_dt := TO_DATE(SUBSTR(i_msg_data, 86, 8), 'YYYYMMDD');
      l_r_parsed.end_dt := TO_DATE(SUBSTR(i_msg_data, 94, 8), 'YYYYMMDD');
      l_r_parsed.typ_cd := SUBSTR(i_msg_data, 102, 1);
      l_r_parsed.cust := SUBSTR(i_msg_data, 103, 8);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION msg_string_fn(
      i_r_msg  IN  l_rt_msg
    )
      RETURN VARCHAR2 IS
      l_msg_str  typ.t_maxvc2;
    BEGIN
      l_msg_str := 'GetID:'
                   || l_mq_get_id
                   || ' ActnCd:'
                   || i_r_msg.actn_cd
                   || ' Div:'
                   || i_r_msg.div
                   || ' GrpCd:'
                   || i_r_msg.grp_id
                   || ' Item:'
                   || i_r_msg.item_num
                   || ' UOM:'
                   || i_r_msg.uom
                   || ' SubItem:'
                   || i_r_msg.sub_item
                   || ' SubUOM:'
                   || i_r_msg.sub_uom
                   || ' StartDt:'
                   || TO_CHAR(i_r_msg.start_dt, 'YYYYMMDD')
                   || ' EndDt:'
                   || TO_CHAR(i_r_msg.end_dt, 'YYYYMMDD')
                   || ' TypCd:'
                   || i_r_msg.typ_cd
                   || ' Cust:'
                   || i_r_msg.cust;
      RETURN(l_msg_str);
    END msg_string_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust08);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust08,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Get CatlgNum');
        l_catlg_num := catlg_num_fn(l_r_msg.item_num, l_r_msg.uom);
        logs.dbg('Get SubItem');
        l_sub_item := catlg_num_fn(l_r_msg.sub_item, l_r_msg.sub_uom);
        logs.dbg('Apply Sub Maintenance');
        sub_maint_sp(l_r_msg.actn_cd,
                     l_div_part,
                     (CASE l_r_msg.typ_cd
                        WHEN l_c_msg_typ_grp THEN l_c_sub_typ_grp
                        WHEN l_c_msg_typ_cus THEN l_c_sub_typ_cus
                      END
                     ),
                     (CASE l_r_msg.typ_cd
                        WHEN l_c_msg_typ_grp THEN l_r_msg.grp_id
                        WHEN l_c_msg_typ_cus THEN l_r_msg.cust
                      END
                     ),
                     l_catlg_num,
                     (CASE l_r_msg.typ_cd
                        WHEN l_c_msg_typ_grp THEN l_c_unc_grp_sub
                        WHEN l_c_msg_typ_cus THEN l_c_unc_cus_sub
                      END
                     ),
                     l_sub_item,
                     1,
                     l_r_msg.start_dt,
                     l_r_msg.end_dt,
                     l_c_msg_id,
                     l_sub_maint_err_msg
                    );

        IF l_sub_maint_err_msg IS NOT NULL THEN
          l_mq_msg_stat := g_c_prb;
          logs.warn(l_sub_maint_err_msg, lar_parm, msg_string_fn(l_r_msg));
        END IF;   -- l_sub_maint_err_msg IS NOT NULL

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust08,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust08,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust08_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST09_SP
  ||  Maps messages that describe Group/Customer Conditional Subs
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/07/01 | rhalpai | Original
  || 09/12/01 | rhalpai | Added rtrim to customer grouping input
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 12/10/01 | rhalpai | Removed STDTC from where clause for Delete and Update
  ||                    | as it is no longer part of the unique index.
  || 12/17/01 | rhalpai | Added set for STDTC in Update
  || 03/15/02 | rhalpai | Added Customer Level Subs
  || 08/28/02 | rhalpai | Changed deletes and updates of conditional subs to be
  ||                    | based on customer/item/uom for customer level subs and
  ||                    | div/group/item/uom for group level subs.
  || 11/18/02 | rhalpai | Changed to call OP_MAINTAIN_SUBS_PK for all maintenance.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status from calls to
  ||                    | op_maintain_subs_pk.maintain_cust_sub_sp and
  ||                    | op_maintain_subs_pk.maintain_group_sub_sp.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 10/28/09 | rhalpai | Converted to call new SUB_MAINT_SP. PIR4342
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust09_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST09_SP';
    lar_parm                  logs.tar_parm;
    l_div_part                NUMBER;
    l_c_msg_id       CONSTANT VARCHAR2(7)                        := 'QCUST09';
    l_mq_msg_stat             mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id               NUMBER;
    l_sub_maint_err_msg       typ.t_maxvc2;
    l_catlg_num               sawp505e.catite%TYPE;
    l_sub_item                sawp505e.catite%TYPE;
    l_c_msg_typ_grp  CONSTANT VARCHAR2(1)                        := 'G';
    l_c_msg_typ_cus  CONSTANT VARCHAR2(1)                        := 'C';
    l_c_sub_typ_grp  CONSTANT VARCHAR2(3)                        := 'GRP';
    l_c_sub_typ_cus  CONSTANT VARCHAR2(3)                        := 'CUS';
    l_c_con_grp_sub  CONSTANT VARCHAR2(3)                        := 'CGP';
    l_c_con_cus_sub  CONSTANT VARCHAR2(3)                        := 'CCS';

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div       VARCHAR2(2),
      actn_cd   VARCHAR2(3),
      grp_id    sysp200c.retgpc%TYPE,
      item_num  sawp505e.iteme%TYPE,
      uom       sawp505e.uome%TYPE,
      sub_item  sawp505e.iteme%TYPE,
      sub_uom   sawp505e.uome%TYPE,
      seq       PLS_INTEGER,
      start_dt  DATE,
      end_dt    DATE,
      typ_cd    VARCHAR2(1),
      cust      sysp200c.acnoc%TYPE
    );

    l_r_msg                   l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.grp_id := RTRIM(SUBSTR(i_msg_data, 54, 8));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 62, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 71, 3);
      l_r_parsed.sub_item := SUBSTR(i_msg_data, 74, 9);
      l_r_parsed.sub_uom := SUBSTR(i_msg_data, 83, 3);
      l_r_parsed.seq := string_to_num_fn(SUBSTR(i_msg_data, 86, 3));
      l_r_parsed.start_dt := TO_DATE(SUBSTR(i_msg_data, 89, 8), 'YYYYMMDD');
      l_r_parsed.end_dt := TO_DATE(SUBSTR(i_msg_data, 97, 8), 'YYYYMMDD');
      l_r_parsed.typ_cd := SUBSTR(i_msg_data, 105, 1);
      l_r_parsed.cust := SUBSTR(i_msg_data, 106, 8);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION msg_string_fn(
      i_r_msg  IN  l_rt_msg
    )
      RETURN VARCHAR2 IS
      l_msg_str  typ.t_maxvc2;
    BEGIN
      l_msg_str := 'GetID:'
                   || l_mq_get_id
                   || ' ActnCd:'
                   || i_r_msg.actn_cd
                   || ' Div:'
                   || i_r_msg.div
                   || ' GrpCd:'
                   || i_r_msg.grp_id
                   || ' Item:'
                   || i_r_msg.item_num
                   || ' UOM:'
                   || i_r_msg.uom
                   || ' SubItem:'
                   || i_r_msg.sub_item
                   || ' SubUOM:'
                   || i_r_msg.sub_uom
                   || ' Seq:'
                   || i_r_msg.seq
                   || ' StartDt:'
                   || TO_CHAR(i_r_msg.start_dt, 'YYYYMMDD')
                   || ' EndDt:'
                   || TO_CHAR(i_r_msg.end_dt, 'YYYYMMDD')
                   || ' TypCd:'
                   || i_r_msg.typ_cd
                   || ' Cust:'
                   || i_r_msg.cust;
      RETURN(l_msg_str);
    END msg_string_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust09);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust09,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Get CatlgNum');
        l_catlg_num := catlg_num_fn(l_r_msg.item_num, l_r_msg.uom);
        logs.dbg('Get SubItem');
        l_sub_item := catlg_num_fn(l_r_msg.sub_item, l_r_msg.sub_uom);
        logs.dbg('Apply Sub Maintenance');
        sub_maint_sp(l_r_msg.actn_cd,
                     l_div_part,
                     (CASE l_r_msg.typ_cd
                        WHEN l_c_msg_typ_grp THEN l_c_sub_typ_grp
                        WHEN l_c_msg_typ_cus THEN l_c_sub_typ_cus
                      END
                     ),
                     (CASE l_r_msg.typ_cd
                        WHEN l_c_msg_typ_grp THEN l_r_msg.grp_id
                        WHEN l_c_msg_typ_cus THEN l_r_msg.cust
                      END
                     ),
                     l_catlg_num,
                     (CASE l_r_msg.typ_cd
                        WHEN l_c_msg_typ_grp THEN l_c_con_grp_sub
                        WHEN l_c_msg_typ_cus THEN l_c_con_cus_sub
                      END
                     ),
                     l_sub_item,
                     1,
                     l_r_msg.start_dt,
                     l_r_msg.end_dt,
                     l_c_msg_id,
                     l_sub_maint_err_msg
                    );

        IF l_sub_maint_err_msg IS NOT NULL THEN
          l_mq_msg_stat := g_c_prb;
          logs.warn(l_sub_maint_err_msg, lar_parm, msg_string_fn(l_r_msg));
        END IF;   -- l_sub_maint_err_msg IS NOT NULL

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust09,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust09,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust09_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCUST10_SP
  ||  Maps messages that describe Group Rounding
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/07/01 | rhalpai | Original
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 09/04/02 | BLYONS  | Added Group_Name to MCLP100A
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 10/19/07 | rhalpai | Change to include new CUBE_BY_HC_SW column on MCLP100A.
  ||                    | PIR3209
  || 12/05/07 | rhalpai | Added logic to maintain new SPLIT_PO_CD column. PIR5132
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 11/07/11 | rhalpai | Change logic to handle new Full Case to Pallet
  ||                    | Rounding Percentage (FC_PLT_RND_PCT) column. PIR10416
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 08/03/18 | rhalpai | Add logic to maintain new DIST_ONLY_SW column and call
  ||                    | Syncload for existing dist orders when DIST_ONLY_SW
  ||                    | changes. PIR18748
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcust10_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCUST10_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCUST10';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd         VARCHAR2(3),
      grp_id          mclp100a.cstgpa%TYPE,
      rounding_pct    NUMBER,
      grp_nm          mclp100a.group_name%TYPE,
      cube_by_hc_sw   mclp100a.cube_by_hc_sw%TYPE,
      split_po_cd     mclp100a.split_po_cd%TYPE,
      fc_plt_rnd_pct  NUMBER,
      dist_only_sw    VARCHAR2(1)
    );

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.grp_id := RTRIM(SUBSTR(i_msg_data, 54, 8));
      l_r_parsed.rounding_pct := string_to_num_fn(SUBSTR(i_msg_data, 62, 3));
      l_r_parsed.grp_nm := RTRIM(SUBSTR(i_msg_data, 65, 25));
      l_r_parsed.cube_by_hc_sw :=(CASE UPPER(SUBSTR(i_msg_data, 90, 1))
                                    WHEN 'Y' THEN 'Y'
                                    ELSE 'N'
                                  END);
      l_r_parsed.split_po_cd := NVL(RTRIM(UPPER(SUBSTR(i_msg_data, 91, 1))), 'N');
      l_r_parsed.fc_plt_rnd_pct := NVL(string_to_num_fn(SUBSTR(i_msg_data, 92, 3)), 0);
      l_r_parsed.dist_only_sw :=(CASE UPPER(SUBSTR(i_msg_data, 95, 1))
                                   WHEN 'Y' THEN 'Y'
                                   ELSE 'N'
                                 END);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp100a ma
            WHERE ma.div_part = i_div_part
              AND ma.cstgpa = i_r_msg.grp_id;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_c_curr_dt  CONSTANT DATE        := TRUNC(SYSDATE);
      l_old_dist_only_sw    VARCHAR2(1);
      l_t_ords              type_ntab;
    BEGIN
      logs.dbg('Get Existing Info');

      SELECT NVL(MAX(ma.dist_only_sw), 'N')
        INTO l_old_dist_only_sw
        FROM mclp100a ma
       WHERE ma.div_part = i_div_part
         AND ma.cstgpa = i_r_msg.grp_id;

      logs.dbg('Merge');
      MERGE INTO mclp100a ma
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    ma.div_part = i_div_part
                  AND ma.cstgpa = i_r_msg.grp_id
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET ma.rndpra = i_r_msg.rounding_pct, ma.group_name = i_r_msg.grp_nm,
                 ma.cube_by_hc_sw = i_r_msg.cube_by_hc_sw, ma.split_po_cd = i_r_msg.split_po_cd,
                 ma.fc_plt_rnd_pct = i_r_msg.fc_plt_rnd_pct, ma.dist_only_sw = i_r_msg.dist_only_sw
        WHEN NOT MATCHED THEN
          INSERT(div_part, cstgpa, rndpra, group_name, cube_by_hc_sw, split_po_cd, fc_plt_rnd_pct, dist_only_sw)
          VALUES(i_div_part, i_r_msg.grp_id, i_r_msg.rounding_pct, i_r_msg.grp_nm, i_r_msg.cube_by_hc_sw,
                 i_r_msg.split_po_cd, i_r_msg.fc_plt_rnd_pct, i_r_msg.dist_only_sw);

      IF l_old_dist_only_sw <> i_r_msg.dist_only_sw THEN
        IF l_old_dist_only_sw = 'Y' THEN
          logs.dbg('Get Ords - DistOnlySw Y to N');

          SELECT a.ordnoa
          BULK COLLECT INTO l_t_ords
            FROM sysp200c c, ordp100a a, load_depart_op1f ld
           WHERE c.div_part = i_div_part
             AND c.retgpc = i_r_msg.grp_id
             AND a.div_part = c.div_part
             AND a.custa = c.acnoc
             AND a.dsorda = 'D'
             AND a.stata = 'O'
             AND ld.div_part = a.div_part
             AND ld.load_depart_sid = a.load_depart_sid
             AND EXISTS(SELECT 1
                          FROM mclp040d md
                         WHERE md.div_part = a.div_part
                           AND md.custd = a.custa
                           AND md.loadd = ld.load_num)
             AND NOT EXISTS(SELECT 1
                              FROM ordp100a a2
                             WHERE a2.div_part = a.div_part
                               AND a2.load_depart_sid = a.load_depart_sid
                               AND a2.dsorda = 'R'
                               AND a2.stata IN('O', 'I')
                               AND a2.excptn_sw = 'N');
        ELSE
          logs.dbg('Get Ords - DistOnlySw N to Y');

          SELECT a.ordnoa
          BULK COLLECT INTO l_t_ords
            FROM mclp130d dv, sysp200c c, ordp100a a, load_depart_op1f ld
           WHERE dv.div_part = i_div_part
             AND c.div_part = dv.div_part
             AND c.retgpc = i_r_msg.grp_id
             AND ld.div_part = dv.div_part
             AND ld.load_num = 'DIST'
             AND a.div_part = ld.div_part
             AND a.load_depart_sid = ld.load_depart_sid
             AND a.custa = c.acnoc
             AND a.dsorda = 'D'
             AND a.stata = 'O'
             AND a.shpja <= l_c_curr_dt + dv.dislad - DATE '1900-02-28';
        END IF;   -- l_old_dist_only_sw = 'Y'

        IF l_t_ords.COUNT > 0 THEN
          logs.dbg('Reassign DistOnly Cust Ords');
          op_order_load_pk.syncload_sp(i_div_part, 'DISTLDSY', l_t_ords);
        END IF;   -- l_t_ords.COUNT > 0
      END IF;   -- l_old_dist_only_sw <> i_r_msg.dist_only_sw
    END merge_sp;

    PROCEDURE process_msg_sp(
      i_div_part     IN      NUMBER,
      i_msg_data     IN      VARCHAR2,
      o_mq_msg_stat  OUT     VARCHAR2
    ) IS
      l_r_msg  l_rt_msg;
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Parse MQ Msg Data');
      l_r_msg := parse_msg_fn(i_msg_data);
      logs.dbg('Process Msg');

      CASE
        WHEN l_r_msg.actn_cd = g_c_del THEN
          logs.dbg('Remove Entry');
          del_sp(i_div_part, l_r_msg);
        WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
          logs.dbg('Add/Chg Entry');
          merge_sp(i_div_part, l_r_msg);
      END CASE;
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcust10);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust10,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, RTRIM(l_r_msg.mq_msg_data, CHR(0)), l_mq_msg_stat);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust10,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcust10,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcust10_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM01_SP
  ||  Maps messages that describe corporate item
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/27/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 09/20/01 | rhalpai | Corrected parsing msg data for cube, weight, size
  || 10/12/01 | rhalpai | Added logic to keep UNIX in sync with M/F by updating
  ||                    | weight and cube on ordp120b and whsp120b when
  ||                    | sawp505e is updated via CHG record
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Changed "nototb = ''," to "nototb = NULL,"
  ||                    | Removed unreferenced variables
  || 04/09/01 | SUDHEER | Added commits after update of ordp120b and whsp120b
  || 04/15/01 | SNAGABH | Updated to reflect new field sizes in incomming
  ||                    | messages
  || 05/02/02 | Santosh | Added section '(CHG) DELETE INVALID ROWS FROM SAWP505E'
  ||                    | to delete rows on SAWP505E where a match exists in
  ||                    | change records for the McLane Catalog Item but not
  ||                    | for the CBR Item/UOM.
  || 06/19/02 | rhalpai | Handle new upc-inner column on SAWP505E.
  ||                    | Add logic to use a 'REF' action code to delete the
  ||                    | existing Catalog Item from SAWP505E followed by an
  ||                    | insert with new CBR Item/UOM for the same Catalog Item
  ||                    | to SAWP505E
  ||                    | Update the Item/UOM on any orders for the catalog item.
  ||                    | Remove logic for action code 'ADD'.
  || 11/18/02 | rhalpai | Removed references to INVP100A.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 08/16/07 | VXRANGA | Venkat's change for qitem01_sp to include CIG_TYPE
  ||                    | (sawp505e.cig_typ_cd) -- cig type code / pack size.
  || 11/06/09 | rhalpai | Converted to use merge stmt. IM527525
  || 11/01/10 | rhalpai | Remove update of unused columns: volumb,twghtb,nototb.
  ||                    | PIR8531
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 02/25/15 | rhalpai | Change logic to pass div_part zero when setting
  ||                    | Process Control to set status at Corp-level (for all
  ||                    | divisions). PIR11038
  || 05/11/16 | jpazhoor| MF to send a single MC msg instead of one for each
  ||                    | division. op_messages_pk.qitem01_sp is called only
  ||                    | for NW.  Currently orders updates apply to NW
  ||                    | orders only. We will have to update orders for all
  ||                    | divisions.
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 07/12/21 | rhalpai | Add logo_sw. PIR21276
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM01_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QITEM01';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div          VARCHAR2(2),
      actn_cd      VARCHAR2(3),
      item_num     sawp505e.iteme%TYPE,
      uom          sawp505e.uome%TYPE,
      mstr_cs_qty  NUMBER,
      upc_item     sawp505e.upce%TYPE,
      ti           NUMBER,
      hi           NUMBER,
      vol          NUMBER,
      wgt          NUMBER,
      height       NUMBER,
      lngth        NUMBER,
      wdth         NUMBER,
      catlg_num    sawp505e.catite%TYPE,
      descr        sawp505e.ctdsce%TYPE,
      nacs_catg    sawp505e.nacse%TYPE,
      stamps_req   NUMBER,
      half_cs_qty  NUMBER,
      kit_sw       sawp505e.kite%TYPE,
      logo_sw      sawp505e.logo_sw%TYPE,
      bus_grp      sawp505e.bsgrpe%TYPE,
      scb_catg     sawp505e.scbcte%TYPE,
      sz           sawp505e.sizee%TYPE,
      pack         NUMBER,
      item_mult    NUMBER,
      upc_cs       sawp505e.upccse%TYPE,
      upc_inner    sawp505e.upc_inner%TYPE,
      cig_typ_cd   sawp505e.cig_typ_cd%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.uom := RTRIM(SUBSTR(i_msg_data, 63, 3));
      l_r_parsed.mstr_cs_qty := string_to_num_fn(SUBSTR(i_msg_data, 66, 4));
      l_r_parsed.upc_item := RTRIM(SUBSTR(i_msg_data, 70, 15));
      l_r_parsed.ti := string_to_num_fn(SUBSTR(i_msg_data, 85, 3));
      l_r_parsed.hi := string_to_num_fn(SUBSTR(i_msg_data, 88, 3));
      l_r_parsed.vol := string_to_num_fn(SUBSTR(i_msg_data, 91, 4) || '.' || SUBSTR(i_msg_data, 95, 3));
      l_r_parsed.wgt := string_to_num_fn(SUBSTR(i_msg_data, 98, 4) || '.' || SUBSTR(i_msg_data, 102, 2));
      l_r_parsed.height := string_to_num_fn(SUBSTR(i_msg_data, 104, 4) || '.' || SUBSTR(i_msg_data, 108, 2));
      l_r_parsed.lngth := string_to_num_fn(SUBSTR(i_msg_data, 110, 4) || '.' || SUBSTR(i_msg_data, 114, 2));
      l_r_parsed.wdth := string_to_num_fn(SUBSTR(i_msg_data, 116, 4) || '.' || SUBSTR(i_msg_data, 120, 2));
      l_r_parsed.catlg_num := RTRIM(SUBSTR(i_msg_data, 146, 6));
      l_r_parsed.descr := RTRIM(SUBSTR(i_msg_data, 152, 30));
      l_r_parsed.nacs_catg := SUBSTR(i_msg_data, 182, 3);
      l_r_parsed.stamps_req := string_to_num_fn(SUBSTR(i_msg_data, 189, 4));
      l_r_parsed.half_cs_qty := string_to_num_fn(SUBSTR(i_msg_data, 193, 4));
      l_r_parsed.kit_sw := SUBSTR(i_msg_data, 197, 1);
      l_r_parsed.logo_sw := SUBSTR(i_msg_data, 206, 1);
      l_r_parsed.bus_grp := SUBSTR(i_msg_data, 207, 3);
      l_r_parsed.scb_catg := SUBSTR(i_msg_data, 210, 3);
      l_r_parsed.sz := SUBSTR(i_msg_data, 213, 8);
      l_r_parsed.pack := string_to_num_fn(SUBSTR(i_msg_data, 221, 4));
      l_r_parsed.item_mult := string_to_num_fn(SUBSTR(i_msg_data, 225, 5));
      l_r_parsed.upc_cs := RTRIM(SUBSTR(i_msg_data, 230, 15));
      l_r_parsed.upc_inner := RTRIM(SUBSTR(i_msg_data, 245, 15));
      l_r_parsed.cig_typ_cd := NVL(RTRIM(SUBSTR(i_msg_data, 260, 3)), '000');
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      CASE i_r_msg.actn_cd
        WHEN g_c_del THEN
          DELETE FROM sawp505e e
                WHERE e.iteme = i_r_msg.item_num
                  AND e.uome = i_r_msg.uom;
        WHEN g_c_ref THEN
          DELETE FROM sawp505e e
                WHERE e.catite = i_r_msg.catlg_num;
        WHEN g_c_chg THEN
          DELETE FROM sawp505e e
                WHERE e.catite = i_r_msg.catlg_num
                  AND (   e.iteme <> i_r_msg.item_num
                       OR e.uome <> i_r_msg.uom);
      END CASE;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO sawp505e e
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    e.iteme = i_r_msg.item_num
                  AND e.uome = i_r_msg.uom
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET e.tie = i_r_msg.ti, e.hie = i_r_msg.hi, e.catite = i_r_msg.catlg_num, e.ctdsce = i_r_msg.descr,
                 e.nacse = i_r_msg.nacs_catg, e.nustme = i_r_msg.stamps_req, e.mlusbe = i_r_msg.item_mult,
                 e.bsgrpe = i_r_msg.bus_grp, e.scbcte = i_r_msg.scb_catg, e.sizee = i_r_msg.sz, e.shppke = i_r_msg.pack,
                 e.mulsle = i_r_msg.mstr_cs_qty, e.upce = i_r_msg.upc_item, e.cubee = i_r_msg.vol,
                 e.wghte = i_r_msg.wgt, e.hghte = i_r_msg.height, e.lgthe = i_r_msg.lngth, e.wdthe = i_r_msg.wdth,
                 e.upccse = i_r_msg.upc_cs, e.kite = i_r_msg.kit_sw, e.fmqtye = i_r_msg.half_cs_qty,
                 e.upc_inner = i_r_msg.upc_inner, e.cig_typ_cd = i_r_msg.cig_typ_cd, e.logo_sw = i_r_msg.logo_sw
        WHEN NOT MATCHED THEN
          INSERT(iteme, uome, state, cnvfce, tie, hie, catite, ctdsce, nacse, nustme, mlusbe, bsgrpe, scbcte, sizee,
                 shppke, mulsle, upce, cubee, wghte, hghte, lgthe, wdthe, upccse, kite, fmqtye, upc_inner, cig_typ_cd,
                 logo_sw)
          VALUES(i_r_msg.item_num, i_r_msg.uom, '1', 1, i_r_msg.ti, i_r_msg.hi, i_r_msg.catlg_num, i_r_msg.descr,
                 i_r_msg.nacs_catg, i_r_msg.stamps_req, i_r_msg.item_mult, i_r_msg.bus_grp, i_r_msg.scb_catg,
                 i_r_msg.sz, i_r_msg.pack, i_r_msg.mstr_cs_qty, i_r_msg.upc_item, i_r_msg.vol, i_r_msg.wgt,
                 i_r_msg.height, i_r_msg.lngth, i_r_msg.wdth, i_r_msg.upc_cs, i_r_msg.kit_sw, i_r_msg.half_cs_qty,
                 i_r_msg.upc_inner, i_r_msg.cig_typ_cd, i_r_msg.logo_sw);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem01);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem01,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  0
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg, g_c_ref) THEN
            IF l_r_msg.actn_cd IN(g_c_ref, g_c_chg) THEN
              logs.dbg('Remove for REF/CHG');
              del_sp(l_r_msg);
            END IF;   --  l_r_msg.actn_cd IN(g_c_ref, g_c_chg)

            logs.dbg('Add/Chg Entry');
            merge_sp(l_r_msg);

            IF l_r_msg.actn_cd = g_c_ref THEN
              logs.dbg('(REF) Upd with new item/uom');

              UPDATE ordp120b b
                 SET b.itemnb = l_r_msg.item_num,
                     b.sllumb = l_r_msg.uom
               WHERE b.orditb = l_r_msg.catlg_num
                 AND b.statb IN('O', 'S')
                 AND (   b.itemnb <> l_r_msg.item_num
                      OR b.sllumb <> l_r_msg.uom);

              logs.dbg('(REF) Upd with new catalog item');

              UPDATE ordp120b b
                 SET b.orditb = l_r_msg.catlg_num
               WHERE b.statb IN('O', 'S')
                 AND b.itemnb = l_r_msg.item_num
                 AND b.sllumb = l_r_msg.uom
                 AND b.orditb <> l_r_msg.catlg_num;
            ELSIF l_r_msg.actn_cd = g_c_chg THEN
              logs.dbg('(CHG) Upd catalog item');

              UPDATE ordp120b
                 SET orditb = l_r_msg.catlg_num
               WHERE statb IN('O', 'S', 'I')
                 AND itemnb = l_r_msg.item_num
                 AND sllumb = l_r_msg.uom
                 AND orditb <> l_r_msg.catlg_num;
            END IF;   -- l_r_msg.actn_cd = g_c_ref
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem01,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  0
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem01,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  0
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem01_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM02_SP
  ||  Maps messages that describe the Division's item information (valid items
  ||  and status and single-item information).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/09/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 04/01/02 | rhalpai | Added new columns (slow_mvng_item_sw,
  ||                    | min_prvntv_rplnsh_qty, min_reqd_rplnsh_qty) to Insert
  ||                    | section '(ADD/CHG) INSERT-MCLP110B' and Update section
  ||                    | '(CHG) UPDATE-MCLP110B'
  || 07/15/02 | rhalpai | Added logic to maintain new max_order_qty column.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 12/02/05 | rhalpai | Added manifest category. PIR2608
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 11/06/09 | rhalpai | Converted to use merge stmt. IM527525
  || 05/23/11 | rhalpai | Changed logic to maintain new CWT_SW column. PIR9238
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem02_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM02_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QITEM02';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div                    VARCHAR2(2),
      actn_cd                VARCHAR2(3),
      item_num               mclp110b.itemb%TYPE,
      uom                    mclp110b.uomb%TYPE,
      ord_typ_rstrctn_sw     mclp110b.ordtpb%TYPE,
      non_stk_item_sw        mclp110b.nonstb%TYPE,
      trans_sw               mclp110b.transb%TYPE,
      buyer                  mclp110b.buyerb%TYPE,
      hi_val_sw              mclp110b.hivalb%TYPE,
      stat                   mclp110b.statb%TYPE,
      disc_dt                NUMBER,
      sngl_item              mclp110b.sitemb%TYPE,
      sngl_uom               mclp110b.suomb%TYPE,
      slow_mvng_item_sw      mclp110b.slow_mvng_item_sw%TYPE,
      min_prvntv_rplnsh_qty  NUMBER,
      min_reqd_rplnsh_qty    NUMBER,
      max_ord_qty            NUMBER,
      mfst_catg              NUMBER,
      cwt_sw                 mclp110b.cwt_sw%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 63, 3);
      l_r_parsed.ord_typ_rstrctn_sw := SUBSTR(i_msg_data, 66, 1);
      l_r_parsed.non_stk_item_sw := SUBSTR(i_msg_data, 67, 1);
      l_r_parsed.trans_sw := SUBSTR(i_msg_data, 68, 1);
      l_r_parsed.buyer := RTRIM(SUBSTR(i_msg_data, 69, 8));
      l_r_parsed.hi_val_sw := SUBSTR(i_msg_data, 77, 1);
      l_r_parsed.stat := SUBSTR(i_msg_data, 78, 3);
      l_r_parsed.disc_dt :=(CASE SUBSTR(i_msg_data, 81, 8)
                              WHEN '00000000' THEN NULL
                              ELSE TO_DATE(SUBSTR(i_msg_data, 81, 8), 'YYYYMMDD') - DATE '1900-02-28'
                            END
                           );
      l_r_parsed.sngl_item := SUBSTR(i_msg_data, 89, 9);
      l_r_parsed.sngl_uom := SUBSTR(i_msg_data, 98, 3);
      l_r_parsed.slow_mvng_item_sw := SUBSTR(i_msg_data, 101, 1);
      l_r_parsed.min_prvntv_rplnsh_qty := string_to_num_fn(SUBSTR(i_msg_data, 102, 7));
      l_r_parsed.min_reqd_rplnsh_qty := string_to_num_fn(SUBSTR(i_msg_data, 109, 7));
      l_r_parsed.max_ord_qty := string_to_num_fn(SUBSTR(i_msg_data, 116, 4));

      -- for zero values reset back to NULL to allow table default to kick in
      IF l_r_parsed.max_ord_qty = 0 THEN
        l_r_parsed.max_ord_qty := NULL;
      END IF;

      l_r_parsed.mfst_catg := NVL(string_to_num_fn(SUBSTR(i_msg_data, 120, 3)), 0);
      l_r_parsed.cwt_sw :=(CASE SUBSTR(i_msg_data, 123, 1)
                             WHEN 'Y' THEN 'Y'
                             ELSE 'N'
                           END);
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp110b di
            WHERE di.div_part = i_div_part
              AND di.itemb = i_r_msg.item_num
              AND di.uomb = i_r_msg.uom;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO mclp110b di
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    di.div_part = i_div_part
                  AND di.itemb = i_r_msg.item_num
                  AND di.uomb = i_r_msg.uom
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET di.ordtpb = i_r_msg.ord_typ_rstrctn_sw, di.nonstb = i_r_msg.non_stk_item_sw,
                 di.transb = i_r_msg.trans_sw, di.buyerb = i_r_msg.buyer, di.hivalb = i_r_msg.hi_val_sw,
                 di.statb = i_r_msg.stat, di.disdtb = i_r_msg.disc_dt, di.sitemb = i_r_msg.sngl_item,
                 di.suomb = i_r_msg.sngl_uom, di.slow_mvng_item_sw = i_r_msg.slow_mvng_item_sw,
                 di.min_prvntv_rplnsh_qty = i_r_msg.min_prvntv_rplnsh_qty,
                 di.min_reqd_rplnsh_qty = i_r_msg.min_reqd_rplnsh_qty, di.max_ord_qty = i_r_msg.max_ord_qty,
                 di.mfst_catg = i_r_msg.mfst_catg, di.cwt_sw = i_r_msg.cwt_sw
        WHEN NOT MATCHED THEN
          INSERT(itemb, uomb, div_part, ordtpb, nonstb, transb, buyerb, hivalb, statb, disdtb, sitemb, suomb,
                 slow_mvng_item_sw, min_prvntv_rplnsh_qty, min_reqd_rplnsh_qty, max_ord_qty, mfst_catg, cwt_sw)
          VALUES(i_r_msg.item_num, i_r_msg.uom, i_div_part, i_r_msg.ord_typ_rstrctn_sw, i_r_msg.non_stk_item_sw,
                 i_r_msg.trans_sw, i_r_msg.buyer, i_r_msg.hi_val_sw, i_r_msg.stat, i_r_msg.disc_dt, i_r_msg.sngl_item,
                 i_r_msg.sngl_uom, i_r_msg.slow_mvng_item_sw, i_r_msg.min_prvntv_rplnsh_qty,
                 i_r_msg.min_reqd_rplnsh_qty, i_r_msg.max_ord_qty, i_r_msg.mfst_catg, i_r_msg.cwt_sw);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem02);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem02,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem02,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem02,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem02_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM04_SP
  ||  Maps messages that describe Kit Components
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/10/01 | rhalpai | Original
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 10/14/05 | rhalpai | Changed to update new kit table - PIR2909
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem04_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM04_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QITEM04';

    TYPE l_rt_msg IS RECORD(
      div_id         div_mstr_di1d.div_id%TYPE,
      actn_cd        VARCHAR2(3),
      kit_typ        kit_item_mstr_kt1m.kit_typ%TYPE,
      item_num       kit_item_mstr_kt1m.item_num%TYPE,
      comp_item_num  kit_item_mstr_kt1m.comp_item_num%TYPE,
      comp_qty       NUMBER,
      start_dt       DATE,
      end_dt         DATE,
      rtl_pck        NUMBER
    );

    l_r_msg              l_rt_msg;
    l_msg_stat           mclane_mq_get.mq_msg_status%TYPE;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed           l_rt_msg;
      l_c_dt_fmt  CONSTANT VARCHAR2(10) := 'YYYY-MM-DD';
    BEGIN
      l_r_parsed.div_id := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.kit_typ := SUBSTR(i_msg_data, 54, 3);
      l_r_parsed.item_num := SUBSTR(i_msg_data, 57, 6);
      l_r_parsed.comp_item_num := SUBSTR(i_msg_data, 63, 6);
      l_r_parsed.comp_qty := string_to_num_fn(SUBSTR(i_msg_data, 69, 7));
      l_r_parsed.start_dt := string_to_date_fn(SUBSTR(i_msg_data, 76, 10), l_c_dt_fmt);
      l_r_parsed.end_dt := string_to_date_fn(SUBSTR(i_msg_data, 86, 10), l_c_dt_fmt);
      l_r_parsed.rtl_pck := string_to_num_fn(SUBSTR(i_msg_data, 96, 7));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part       IN  NUMBER,
      i_kit_typ        IN  VARCHAR2,
      i_item_num       IN  VARCHAR2,
      i_comp_item_num  IN  VARCHAR2
    ) IS
    BEGIN
      DELETE FROM kit_item_mstr_kt1m
            WHERE div_part = i_div_part
              AND kit_typ = i_kit_typ
              AND item_num = i_item_num
              AND comp_item_num = i_comp_item_num;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_c_sysdate  CONSTANT DATE := SYSDATE;
    BEGIN
      MERGE INTO kit_item_mstr_kt1m k
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    k.div_part = i_div_part
                  AND k.kit_typ = i_r_msg.kit_typ
                  AND k.item_num = i_r_msg.item_num
                  AND k.comp_item_num = i_r_msg.comp_item_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET start_dt = i_r_msg.start_dt, end_dt = i_r_msg.end_dt, comp_qty = i_r_msg.comp_qty,
                 rtl_pck = i_r_msg.rtl_pck, last_chg_ts = l_c_sysdate
        WHEN NOT MATCHED THEN
          INSERT(div_part, kit_typ, item_num, comp_item_num, start_dt, end_dt, comp_qty, rtl_pck, last_chg_ts)
          VALUES(i_div_part, i_r_msg.kit_typ, i_r_msg.item_num, i_r_msg.comp_item_num, i_r_msg.start_dt, i_r_msg.end_dt,
                 i_r_msg.comp_qty, i_r_msg.rtl_pck, l_c_sysdate);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem04);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem04,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_msg_stat := g_c_compl;

        BEGIN
          logs.dbg('Parse MQ Message Data');
          l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);

          IF l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Delete KIT_ITEM_MSTR_KT1M');
            del_sp(l_div_part, l_r_msg.kit_typ, l_r_msg.item_num, l_r_msg.comp_item_num);

            IF SQL%ROWCOUNT = 0 THEN
              logs.warn('Not found for delete', lar_parm, 'MQGetID: ' || l_r_mq_msg.mq_get_id);
            END IF;   -- SQL%ROWCOUNT = 0
          ELSIF l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Merge KIT_ITEM_MSTR_KT1M');
            merge_sp(l_div_part, l_r_msg);
          END IF;   -- l_r_msg.actn_cd = c_delete
        EXCEPTION
          WHEN VALUE_ERROR THEN
            l_msg_stat := g_c_prb;
            logs.warn('Value Error', lar_parm, 'MQGetID:' || l_r_mq_msg.mq_get_id);
        END;

        logs.dbg('Update MQ Message Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_msg_stat);
        COMMIT;   -- Commit the Order Data updated successfully
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem04,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem04,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem04_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM05_SP
  ||  Maps messages that describe the Division's item sub information
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/22/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 11/17/03 | rhalpai | Changed logic to use Merge statement and call new
  ||                    | log_problem_sp for problems.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 10/28/09 | rhalpai | Converted to call new SUB_MAINT_SP. PIR4342
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem05_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM05_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QITEM05';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;
    l_sub_maint_err_msg  typ.t_maxvc2;
    l_catlg_num          sawp505e.catite%TYPE;
    l_sub_item           sawp505e.catite%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div       VARCHAR2(2),
      actn_cd   VARCHAR2(3),
      item_num  sawp505e.iteme%TYPE,
      uom       sawp505e.uome%TYPE,
      sub_item  sawp505e.iteme%TYPE,
      sub_uom   sawp505e.uome%TYPE,
      seq       PLS_INTEGER
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 63, 3);
      l_r_parsed.sub_item := SUBSTR(i_msg_data, 66, 9);
      l_r_parsed.sub_uom := SUBSTR(i_msg_data, 75, 3);
      l_r_parsed.seq := string_to_num_fn(SUBSTR(i_msg_data, 78, 3));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION msg_string_fn(
      i_r_msg  IN  l_rt_msg
    )
      RETURN VARCHAR2 IS
      l_msg_str  typ.t_maxvc2;
    BEGIN
      l_msg_str := 'GetID:'
                   || l_mq_get_id
                   || ' ActnCd:'
                   || i_r_msg.actn_cd
                   || ' Div:'
                   || i_r_msg.div
                   || ' Item:'
                   || i_r_msg.item_num
                   || ' UOM:'
                   || i_r_msg.uom
                   || ' SubItem:'
                   || i_r_msg.sub_item
                   || ' SubUOM:'
                   || i_r_msg.sub_uom
                   || ' Seq:'
                   || i_r_msg.seq;
      RETURN(l_msg_str);
    END msg_string_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem05);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem05,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Get CatlgNum');
        l_catlg_num := catlg_num_fn(l_r_msg.item_num, l_r_msg.uom);
        logs.dbg('Get SubItem');
        l_sub_item := catlg_num_fn(l_r_msg.sub_item, l_r_msg.sub_uom);
        logs.dbg('Apply Sub Maintenance');
        sub_maint_sp(l_r_msg.actn_cd,
                     l_div_part,
                     'ITM',
                     'ALL',
                     l_catlg_num,
                     'DIV',
                     l_sub_item,
                     1,
                     TO_DATE('19000101', 'YYYYMMDD'),
                     TO_DATE('29991231', 'YYYYMMDD'),
                     l_c_msg_id,
                     l_sub_maint_err_msg
                    );

        IF l_sub_maint_err_msg IS NOT NULL THEN
          l_mq_msg_stat := g_c_prb;
          logs.warn(l_sub_maint_err_msg, lar_parm, msg_string_fn(l_r_msg));
        END IF;   -- l_sub_maint_err_msg IS NOT NULL

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem05,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem05,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem05_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM15_SP
  ||  Maps messages that describe the Division's item replacement information
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/22/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 11/17/03 | rhalpai | Changed logic to check for matching orders and call
  ||                    | revert_to_original_sub_line_sp for DEL's, call
  ||                    | op_get_subs_sp for ADD's and call new log_problem_sp
  ||                    | for problems.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Removed return status from calls to
  ||                    | op_maintain_subs_pk.revert_to_original_sub_line_sp
  ||                    | and op_get_subs_sp.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 08/19/08 | rhalpai | Reformatted and changed to use order header status to
  ||                    | indicate unbilled order status. PIR6364
  || 10/28/09 | rhalpai | Converted to call new SUB_MAINT_SP. PIR4342
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem15_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM15_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QITEM15';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;
    l_sub_maint_err_msg  typ.t_maxvc2;
    l_catlg_num          sawp505e.catite%TYPE;
    l_sub_item           sawp505e.catite%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div        VARCHAR2(2),
      actn_cd    VARCHAR2(3),
      item_num   sawp505e.iteme%TYPE,
      uom        sawp505e.uome%TYPE,
      repl_item  sawp505e.iteme%TYPE,
      repl_uom   sawp505e.uome%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.item_num := RTRIM(SUBSTR(i_msg_data, 54, 9));
      l_r_parsed.uom := RTRIM(SUBSTR(i_msg_data, 63, 3));
      l_r_parsed.repl_item := RTRIM(SUBSTR(i_msg_data, 66, 9));
      l_r_parsed.repl_uom := RTRIM(SUBSTR(i_msg_data, 75, 3));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION msg_string_fn(
      i_r_msg  IN  l_rt_msg
    )
      RETURN VARCHAR2 IS
      l_msg_str  typ.t_maxvc2;
    BEGIN
      l_msg_str := 'GetID:'
                   || l_mq_get_id
                   || ' ActnCd:'
                   || i_r_msg.actn_cd
                   || ' Div:'
                   || i_r_msg.div
                   || ' Item:'
                   || i_r_msg.item_num
                   || ' UOM:'
                   || i_r_msg.uom
                   || ' ReplItem:'
                   || i_r_msg.repl_item
                   || ' ReplUOM:'
                   || i_r_msg.repl_uom;
      RETURN(l_msg_str);
    END msg_string_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem15);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem15,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Get CatlgNum');
        l_catlg_num := catlg_num_fn(l_r_msg.item_num, l_r_msg.uom);
        logs.dbg('Get SubItem');
        l_sub_item := catlg_num_fn(l_r_msg.repl_item, l_r_msg.repl_uom);
        logs.dbg('Apply Sub Maintenance');
        sub_maint_sp(l_r_msg.actn_cd,
                     l_div_part,
                     'ITM',
                     'ALL',
                     l_catlg_num,
                     'RPI',
                     l_sub_item,
                     1,
                     TO_DATE('19000101', 'YYYYMMDD'),
                     TO_DATE('29991231', 'YYYYMMDD'),
                     l_c_msg_id,
                     l_sub_maint_err_msg
                    );

        IF l_sub_maint_err_msg IS NOT NULL THEN
          l_mq_msg_stat := g_c_prb;
          logs.warn(l_sub_maint_err_msg, lar_parm, msg_string_fn(l_r_msg));
        END IF;   -- l_sub_maint_err_msg IS NOT NULL

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem15,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem15,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem15_sp;

  /*
  ||----------------------------------------------------------------------------
  || QITEM18_SP
  ||  Maps messages that describe item rounding rules
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/24/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 03/25/02 | rhalpai | Populate new DIV_ITEM_ALT table. This allows for items
  ||                    | to be linked together between Single Sell and Full Case,
  ||                    | even though they do not belong to the same CBR item.
  || 05/02/02 | Santosh | Deleted blocks for update/insert/delete to MCLP060A
  ||                    | table from qitem18_sp.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 11/06/09 | rhalpai | Converted to use merge stmt. IM527525
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qitem18_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QITEM18_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QITEM18';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div       VARCHAR2(2),
      actn_cd   VARCHAR2(3),
      item_num  div_item_alt.item_num%TYPE,
      uom       div_item_alt.item_uom%TYPE,
      priorty   NUMBER,
      alt_uom   div_item_alt.alt_uom%TYPE,
      qty_fctr  NUMBER,
      alt_item  div_item_alt.alt_item%TYPE,
      alt_typ   div_item_alt.alt_typ%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.item_num := SUBSTR(i_msg_data, 54, 9);
      l_r_parsed.uom := SUBSTR(i_msg_data, 63, 3);
      l_r_parsed.priorty := string_to_num_fn(SUBSTR(i_msg_data, 66, 3));
      l_r_parsed.alt_uom := SUBSTR(i_msg_data, 69, 3);
      l_r_parsed.qty_fctr := string_to_num_fn(SUBSTR(i_msg_data, 72, 5));
      l_r_parsed.alt_item := SUBSTR(i_msg_data, 77, 9);
      l_r_parsed.alt_typ := RTRIM(SUBSTR(i_msg_data, 86, 3));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM div_item_alt a
            WHERE a.div_part = i_div_part
              AND a.item_num = i_r_msg.item_num
              AND a.item_uom = i_r_msg.uom
              AND a.alt_typ = i_r_msg.alt_typ;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_c_sysdate  CONSTANT DATE := SYSDATE;
    BEGIN
      MERGE INTO div_item_alt a
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    a.div_part = i_div_part
                  AND a.item_num = i_r_msg.item_num
                  AND a.item_uom = i_r_msg.uom
                  AND a.alt_typ = i_r_msg.alt_typ
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET a.alt_item = i_r_msg.alt_item, a.alt_uom = i_r_msg.alt_uom, a.qty_fctr = i_r_msg.qty_fctr,
                 a.priorty = i_r_msg.priorty, a.last_chg_ts = l_c_sysdate
        WHEN NOT MATCHED THEN
          INSERT(div_part, item_num, item_uom, alt_typ, alt_item, alt_uom, qty_fctr, priorty, last_chg_ts)
          VALUES(i_div_part, i_r_msg.item_num, i_r_msg.uom, i_r_msg.alt_typ, i_r_msg.alt_item, i_r_msg.alt_uom,
                 i_r_msg.qty_fctr, i_r_msg.priorty, l_c_sysdate);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qitem18);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem18,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem18,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qitem18,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qitem18_sp;

  /*
  ||----------------------------------------------------------------------------
  || QOPRC10_SP
  ||  Maps messages that describe the Division's tote categories.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/09/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 08/19/08 | rhalpai | Reformatted and added logic to include setting box
  ||                    | indicator to value of piece indicator. PIR6364
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 11/28/11 | rhalpai | Change logic to use CIGS_USE_TOTES parm to determine
  ||                    | Box Switch. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qoprc10_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QOPRC10_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QOPRC10';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div                    VARCHAR2(2),
      actn_cd                VARCHAR2(3),
      tote_categ             mclp200b.totctb%TYPE,
      descr                  mclp200b.descb%TYPE,
      inner_cube             NUMBER,
      outer_cube             NUMBER,
      tote_consolidation_id  mclp200b.totidb%TYPE,
      mfst_categ             mclp200b.tmnctb%TYPE,
      max_piece_cnt          NUMBER,
      piece_cnt_sw           mclp200b.pccntb%TYPE,
      tote_cube_sw           mclp200b.totcub%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.tote_categ := RTRIM(SUBSTR(i_msg_data, 54, 3));
      l_r_parsed.descr := RTRIM(SUBSTR(i_msg_data, 57, 30));
      l_r_parsed.inner_cube := TO_NUMBER(SUBSTR(i_msg_data, 87, 4) || '.' || SUBSTR(i_msg_data, 91, 3));
      l_r_parsed.outer_cube := TO_NUMBER(SUBSTR(i_msg_data, 94, 4) || '.' || SUBSTR(i_msg_data, 98, 3));
      l_r_parsed.tote_consolidation_id := RTRIM(SUBSTR(i_msg_data, 101, 3));
      l_r_parsed.mfst_categ := RTRIM(SUBSTR(i_msg_data, 104, 3));
      l_r_parsed.max_piece_cnt := TO_NUMBER(SUBSTR(i_msg_data, 107, 4));
      l_r_parsed.piece_cnt_sw := RTRIM(SUBSTR(i_msg_data, 111, 1));
      l_r_parsed.tote_cube_sw := RTRIM(SUBSTR(i_msg_data, 112, 1));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp200b t
            WHERE t.div_part = i_div_part
              AND t.totctb = i_r_msg.tote_categ;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_box_sw  VARCHAR2(1);
    BEGIN
      l_box_sw :=(CASE
                    WHEN op_parms_pk.val_fn(i_div_part, op_const_pk.prm_cigs_use_totes) = 'Y' THEN 'N'
                    ELSE i_r_msg.piece_cnt_sw
                  END
                 );
      MERGE INTO mclp200b t
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    t.div_part = i_div_part
                  AND t.totctb = i_r_msg.tote_categ
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET descb = i_r_msg.descr, innerb = i_r_msg.inner_cube, outerb = i_r_msg.outer_cube,
                 totidb = i_r_msg.tote_consolidation_id, tmnctb = i_r_msg.mfst_categ, totcnb = i_r_msg.max_piece_cnt,
                 pccntb = i_r_msg.piece_cnt_sw, boxb = l_box_sw, totcub = i_r_msg.tote_cube_sw
        WHEN NOT MATCHED THEN
          INSERT(div_part, totctb, descb, innerb, outerb, totidb, tmnctb, totcnb, pccntb, boxb, totcub)
          VALUES(i_div_part, i_r_msg.tote_categ, i_r_msg.descr, i_r_msg.inner_cube, i_r_msg.outer_cube,
                 i_r_msg.tote_consolidation_id, i_r_msg.mfst_categ, i_r_msg.max_piece_cnt, i_r_msg.piece_cnt_sw,
                 l_box_sw, i_r_msg.tote_cube_sw);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qoprc10);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qoprc10,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qoprc10,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qoprc10,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qoprc10_sp;

  /*
  ||----------------------------------------------------------------------------
  || QOPRC11_SP
  ||  Maps messages that describe the Division's manifest categories.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/09/01 | rhalpai | Original
  || 09/06/01 | rhalpai | Changed to email once with error count
  || 10/26/01 | rhalpai | Changed commit point to within loop
  || 11/19/01 | rhalpai | Removed unreferenced variables
  || 06/19/02 | rhalpai | Add logic to keep MCLP340D in sync with ADD's and DEL's
  ||                    | to MCLP210C for MCLP310A "All Manifest Categories".
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 11/06/09 | rhalpai | Converted to use merge stmt. IM527525
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 11/22/17 | rhalpai | Change logic to handle new catg_typ_cd column. PIR17950
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qoprc11_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QOPRC11_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QOPRC11';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div          VARCHAR2(2),
      actn_cd      VARCHAR2(3),
      mfst_catg    mclp210c.manctc%TYPE,
      descr        mclp210c.descc%TYPE,
      seq          NUMBER,
      tote_dflt    mclp210c.totdfc%TYPE,
      catg_typ_cd  mclp210c.catg_typ_cd%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.descr := RTRIM(SUBSTR(i_msg_data, 54, 25));
      l_r_parsed.seq := string_to_num_fn(SUBSTR(i_msg_data, 79, 3));
      l_r_parsed.tote_dflt := SUBSTR(i_msg_data, 82, 3);
      l_r_parsed.mfst_catg := SUBSTR(i_msg_data, 85, 3);
      l_r_parsed.catg_typ_cd := NVL(RTRIM(SUBSTR(i_msg_data, 88, 1)), 'DUM');
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp210c m
            WHERE m.div_part = i_div_part
              AND m.manctc = i_r_msg.mfst_catg;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO mclp210c m
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    m.div_part = i_div_part
                  AND m.manctc = i_r_msg.mfst_catg
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET m.descc = i_r_msg.descr, m.seqc = i_r_msg.seq, m.totdfc = i_r_msg.tote_dflt, m.catg_typ_cd = i_r_msg.catg_typ_cd
        WHEN NOT MATCHED THEN
          INSERT(div_part, manctc, descc, seqc, totdfc, catg_typ_cd)
          VALUES(i_div_part, i_r_msg.mfst_catg, i_r_msg.descr, i_r_msg.seq, i_r_msg.tote_dflt, i_r_msg.catg_typ_cd);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qoprc11);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qoprc11,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qoprc11,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qoprc11,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qoprc11_sp;

  /*
  ||----------------------------------------------------------------------------
  || QPICCNF_SP
  ||  Processes Pick Confirm Batch and Clean Manifest messages.
  ||
  || Msg Data is expected in the following sequence:
  ||   ACS Container Adjustment Msgs (if applicable)
  ||   ACS Pick Adjustment Msgs
  ||   ACS Tote Adjustment Msgs
  ||   ACS Load Close Msg
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/30/03 | SNAGABH | Original
  || 03/04/04 | SNAGABH | Updated to handle new message format.
  || 05/17/04 | SNAGABH | Fix bug related to processing ACS Load Close and
  ||                    | Clean Manifest messages for multiple loads received
  ||                    | as a group.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 02/04/05 | SNAGABH | PIR# 1566 - Change "Update mclp370c" SQLs to not reset
  ||                    | totes, bags, boxes, pallets and pas values to 0. This
  ||                    | change is required for MI division which will have
  ||                    | both ACS and Non ACS items within a load unlike WJ.
  ||                    | This change will be a no impact to WJ as these
  ||                    | columns will start with a "0" value and will be
  ||                    | updated based on ACS messages.
  || 03/21/05 | rhalpai | Changed updates to MCLP370C to be applied when
  ||                    | load_status is in 'P' or 'R' status. Some MQ messages
  ||                    | are being sent during the mainframe billing process
  ||                    | while the load_status is still in 'P' status because
  ||                    | the "Release Complete" script has not yet run.
  || 06/21/05 | rhalpai | Changed to handle pick adjustments across multiple
  ||                    | order lines when allowed by parm. PIR1671
  || 10/27/05 | CXAMART | On update of the ORDP120B, update the NTSHPB based on
  ||                    | the value of the reason code from the PICCNF MQ message
  ||                    | PIR 179.
  || 09/08/06 | rhalpai | Changed to handle pick adjustments across multiple
  ||                    | releases. IM253278
  || 12/04/06 | rhalpai | Changed to handle container adjustments. PIR3209
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 05/11/07 | rhalpai | Change cursor for "Get Most Recent LLR Date and First Release"
  ||                    | to include stop in selection criteria. IM300931
  || 07/13/10 | rhalpai | Changed to use switch in pick adjust msg to indicate
  ||                    | whether to apply out at order-line-level (the actual
  ||                    | order line passed) or at customer-level (which applies
  ||                    | the total out qty to as many order lines necessary for
  ||                    | the same customer/item). PIR8819
  || 07/05/01 | rhalpai | Added logic to adjust PickQty on WklyMaxCustItem and
  ||                    | WklyMaxLog tables. PIR6235
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 04/04/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 12/30/13 | rhalpai | Change logic to bypass lineouts for Catchweight items.
  ||                    | PIR12765
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 04/25/22 | rhalpai | Add logic to use RsnCd T for NtShpRsn CAPOUT. PIR21762
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qpiccnf_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QPICCNF_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_c_msg_id     CONSTANT VARCHAR2(7)                        := 'QPICCNF';
    l_mq_msg_stat           mclane_mq_get.mq_msg_status%TYPE;
    l_piccnf_ord_ln_lvl_sw  VARCHAR2(1);
    l_load_num_sav          mclp120c.loadc%TYPE                := '0000';
    l_is_pic_cnf_msg        BOOLEAN                            := FALSE;
    l_ords_not_found_msg    typ.t_maxvc2;
    l_ord_num_sav           NUMBER                             := -1;
    l_ord_ln_sav            NUMBER                             := -1;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div                   div_mstr_di1d.div_id%TYPE,
      ord_num               NUMBER,
      catlg_num             sawp505e.catite%TYPE,
      out_qty               PLS_INTEGER,
      rsn_cd                VARCHAR2(3),
      mcl_cust              mclp020b.mccusb%TYPE,
      ord_ln                NUMBER,
      appl_id               VARCHAR2(5),
      load_num              mclp120c.loadc%TYPE,
      stop_num              NUMBER,
      mfst_catg             mclp210c.manctc%TYPE,
      tote_catg             mclp200b.totctb%TYPE,
      cntnr_cnt             PLS_INTEGER,
      cntnr_id              bill_cntnr_id_bc1c.orig_cntnr_id%TYPE,
      ship_qty              NUMBER,
      dspstn_err_sw         load_clos_cntrl_bc2c.dspstn_err_sw%TYPE,
      pct_dscrpncy          NUMBER,
      piccnf_ord_ln_lvl_sw  VARCHAR2(1)
    );

    l_r_msg                 l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.ord_num := SUBSTR(i_msg_data, 54, 11);
      l_r_parsed.catlg_num := SUBSTR(i_msg_data, 65, 6);
      l_r_parsed.out_qty := NVL(string_to_num_fn(SUBSTR(i_msg_data, 71, 4)), 0);
      l_r_parsed.rsn_cd := TRIM(SUBSTR(i_msg_data, 75, 3));
      l_r_parsed.mcl_cust := SUBSTR(i_msg_data, 78, 6);
      l_r_parsed.ord_ln := string_to_num_fn(SUBSTR(i_msg_data, 84, 7)) / 100;
      l_r_parsed.appl_id := SUBSTR(i_msg_data, 91, 5);
      l_r_parsed.load_num := SUBSTR(i_msg_data, 96, 4);
      l_r_parsed.stop_num := SUBSTR(i_msg_data, 100, 2);
      l_r_parsed.mfst_catg := SUBSTR(i_msg_data, 102, 3);
      l_r_parsed.tote_catg := SUBSTR(i_msg_data, 105, 3);
      l_r_parsed.cntnr_cnt := SUBSTR(i_msg_data, 108, 5);
      l_r_parsed.dspstn_err_sw := 'N';
      l_r_parsed.pct_dscrpncy := 0;

      IF (    l_r_parsed.ord_num = 0
          AND l_r_parsed.mfst_catg = '000'
          AND l_r_parsed.tote_catg = '000') THEN
        l_r_parsed.dspstn_err_sw := SUBSTR(i_msg_data, 113, 1);

        IF l_r_parsed.dspstn_err_sw <> 'Y' THEN
          l_r_parsed.dspstn_err_sw := 'N';
        END IF;   -- l_r_parsed.dspstn_err_sw <> 'Y'

        l_r_parsed.pct_dscrpncy := NVL(string_to_num_fn(SUBSTR(i_msg_data, 114, 3)), 0);

        IF l_r_parsed.pct_dscrpncy > 100 THEN
          l_r_parsed.pct_dscrpncy := 100;
        END IF;   -- l_r_parsed.pct_dscrpncy > 100
      ELSE
        l_r_parsed.cntnr_id := SUBSTR(i_msg_data, 113, 20);
        l_r_parsed.ship_qty := string_to_num_fn(SUBSTR(i_msg_data, 133, 7));
        l_r_parsed.piccnf_ord_ln_lvl_sw := SUBSTR(i_msg_data, 140, 1);
      END IF;   -- l_r_parsed.ord_num = 0 AND l_r_parsed.mfst_catg = '000' AND l_r_parsed.tote_catg = '000'

      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE upd_acs_load_clos_sw_sp(
      i_div_part       IN  NUMBER,
      i_load_num       IN  VARCHAR2,
      i_upd_val        IN  VARCHAR2,
      i_dspstn_err_sw  IN  VARCHAR2 DEFAULT NULL,
      i_pct_dscrpncy   IN  NUMBER DEFAULT NULL
    ) IS
    BEGIN
      UPDATE load_clos_cntrl_bc2c c
         SET c.dspstn_err_sw = DECODE(i_upd_val, 'P', c.dspstn_err_sw, i_dspstn_err_sw),
             c.pct_dscrpncy = DECODE(i_upd_val, 'P', c.pct_dscrpncy, i_pct_dscrpncy),
             c.acs_load_clos_sw =(CASE
                                    WHEN(    i_upd_val = 'Y'
                                         AND i_dspstn_err_sw = 'Y') THEN 'N'
                                    ELSE i_upd_val
                                  END)
       WHERE c.div_part = i_div_part
         AND c.load_num = i_load_num
         AND c.load_status IN('P', 'R')
         AND c.acs_load_clos_sw = DECODE(i_upd_val, 'Y', 'P', 'P', 'N')
         AND c.test_bil_load_sw = 'N';

      COMMIT;
    END upd_acs_load_clos_sw_sp;

    PROCEDURE upd_load_cntnr_sp(
      i_div_part            IN      NUMBER,
      i_r_msg               IN      l_rt_msg,
      o_ords_not_found_msg  OUT     VARCHAR2
    ) IS
      l_cv                        SYS_REFCURSOR;
      l_c_sysdate        CONSTANT DATE                 := SYSDATE;
      l_c_rendate_today  CONSTANT PLS_INTEGER          := TRUNC(l_c_sysdate) - DATE '1900-02-28';
      l_llr_num                   NUMBER;
      l_rlse_ts                   DATE;
      l_box_sw                    mclp200b.boxb%TYPE   := 'N';
    BEGIN
      IF (    i_r_msg.mfst_catg <> '000'
          AND i_r_msg.tote_catg <> '000') THEN
        -------------------------------------------------------------------
        -- Bypass updating tote/box count if the mainfest or tote
        -- categories are 0's (associated with Load level update messages).
        -- Update orders based on the Load/manifiest/tote information.
        -- When multiple releases exists update the qty of the first release
        -- to qty passed and zero out the qty for all other releases.
        -- If boxb = 'Y' on mclp200b then update boxsmc on mclp370c
        -- with the number of tote count value (box count in this case)
        -- passed for the manifest and category combination from the
        -- MQ message. Also update the acs_load_clos_sw to 'Y'.
        -- Else update totsmc on mclp370c with the tote count value passed
        -- for the manifest and category combination from the MQ message.
        -- Also update the acs_load_clos_sw to 'Y'.
        -- If row not found to update then bypass and log in sql_utilities
        -- table as a warning.
        -------------------------------------------------------------------
        SELECT NVL(MAX(b.boxb), 'N')
          INTO l_box_sw
          FROM mclp200b b
         WHERE b.div_part = i_div_part
           AND b.totctb = i_r_msg.tote_catg
           AND b.tmnctb = i_r_msg.mfst_catg;

        -- Get Most Recent LLR Date and First Release
        OPEN l_cv
         FOR
           SELECT   mc.llr_date, MIN(mc.release_ts)
               FROM mclp370c mc, load_clos_cntrl_bc2c lc
              WHERE mc.div_part = i_div_part
                AND mc.stopc = i_r_msg.stop_num
                AND mc.manctc = i_r_msg.mfst_catg
                AND mc.totctc = i_r_msg.tote_catg
                AND mc.load_status IN('P', 'R')
                AND mc.user_id IS NULL
                AND lc.div_part = mc.div_part
                AND lc.llr_dt = DATE '1900-02-28' + mc.llr_date
                AND lc.load_num = mc.loadc
                AND lc.load_num = i_r_msg.load_num
                AND lc.load_status IN('P', 'R')
                AND lc.acs_load_clos_sw = 'P'
                AND lc.test_bil_load_sw = 'N'
                AND (ABS(l_c_rendate_today - mc.llr_date)) = (SELECT MIN(ABS(l_c_rendate_today - mc2.llr_date))
                                                                FROM mclp370c mc2, load_clos_cntrl_bc2c lc2
                                                               WHERE mc2.div_part = mc.div_part
                                                                 AND mc2.loadc = mc.loadc
                                                                 AND mc2.stopc = mc.stopc
                                                                 AND mc2.manctc = mc.manctc
                                                                 AND mc2.totctc = mc.totctc
                                                                 AND mc2.load_status = mc.load_status
                                                                 AND mc2.user_id IS NULL
                                                                 AND lc2.div_part = mc2.div_part
                                                                 AND lc2.llr_dt = DATE '1900-02-28' + mc2.llr_date
                                                                 AND lc2.load_num = mc2.loadc
                                                                 AND lc2.load_status IN('P', 'R')
                                                                 AND lc2.acs_load_clos_sw = 'P'
                                                                 AND lc2.test_bil_load_sw = 'N')
           GROUP BY mc.llr_date;

        FETCH l_cv
         INTO l_llr_num, l_rlse_ts;

        UPDATE mclp370c c
           SET c.totsmc =(CASE
                            WHEN l_box_sw = 'Y' THEN c.totsmc
                            WHEN c.release_ts = l_rlse_ts THEN i_r_msg.cntnr_cnt
                            ELSE 0
                          END
                         ),
               c.boxsmc =(CASE
                            WHEN l_box_sw = 'N' THEN c.boxsmc
                            WHEN c.release_ts = l_rlse_ts THEN i_r_msg.cntnr_cnt
                            ELSE 0
                          END
                         ),
               c.last_ts_chg = l_c_sysdate
         WHERE c.div_part = i_div_part
           AND c.llr_date = l_llr_num
           AND c.loadc = i_r_msg.load_num
           AND c.stopc = i_r_msg.stop_num
           AND c.manctc = i_r_msg.mfst_catg
           AND c.totctc = i_r_msg.tote_catg
           AND c.load_status IN('P', 'R')
           AND c.user_id IS NULL
           AND EXISTS(SELECT 1
                        FROM load_clos_cntrl_bc2c lc
                       WHERE lc.div_part = i_div_part
                         AND lc.llr_dt = DATE '1900-02-28' + l_llr_num
                         AND lc.load_num = i_r_msg.load_num
                         AND lc.load_status IN('P', 'R')
                         AND lc.acs_load_clos_sw = 'P'
                         AND lc.test_bil_load_sw = 'N')
           AND EXISTS(SELECT 1
                        FROM mclp370c c2
                       WHERE c2.div_part = i_div_part
                         AND c2.llr_date = l_llr_num
                         AND c2.loadc = i_r_msg.load_num
                         AND c2.stopc = i_r_msg.stop_num
                         AND c2.manctc = i_r_msg.mfst_catg
                         AND c2.totctc = i_r_msg.tote_catg
                         AND c2.load_status IN('P', 'R')
                         AND c2.user_id IS NULL
                         AND c2.release_ts = l_rlse_ts);

        IF SQL%NOTFOUND THEN
          o_ords_not_found_msg := 'No matching orders found when updating '
                                  ||(CASE l_box_sw
                                       WHEN 'Y' THEN 'Box'
                                       ELSE 'Tote'
                                     END)
                                  || ' count on MCLP370c for Div: '
                                  || i_r_msg.div
                                  || ' Load: '
                                  || i_r_msg.load_num
                                  || ' Manifest Cat: '
                                  || i_r_msg.mfst_catg
                                  || ' Tote Cat: '
                                  || i_r_msg.tote_catg;
        END IF;   -- SQL%NOTFOUND
      END IF;   -- i_msg.mfst_catg <> '000' AND i_msg.tote_catg <> '000'
    END upd_load_cntnr_sp;

    PROCEDURE upd_ord_cntnr_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      IF (   i_r_msg.ord_num <> l_ord_num_sav
          OR i_r_msg.ord_ln <> l_ord_ln_sav) THEN
        l_ord_num_sav := i_r_msg.ord_num;
        l_ord_ln_sav := i_r_msg.ord_ln;

        DELETE FROM bill_cntnr_id_bc1c bc
              WHERE bc.div_part = i_div_part
                AND bc.ord_num = i_r_msg.ord_num
                AND bc.ord_ln_num = i_r_msg.ord_ln;
      END IF;   -- i_msg.ord_num <> l_ord_num_sav OR i_msg.ord_ln <> l_ord_ln_sav

      INSERT INTO bill_cntnr_id_bc1c
                  (div_part, ord_num, ord_ln_num, orig_cntnr_id, orig_qty
                  )
           VALUES (i_div_part, i_r_msg.ord_num, i_r_msg.ord_ln, i_r_msg.cntnr_id, i_r_msg.ship_qty
                  );
    END upd_ord_cntnr_sp;

    PROCEDURE upd_pick_sp(
      i_div_part  IN  NUMBER,
      i_ord_num   IN  NUMBER,
      i_ord_ln    IN  NUMBER,
      i_out_qty   IN  PLS_INTEGER,
      i_out_rsn   IN  VARCHAR2
    ) IS
      l_out_rsn  ordp120b.ntshpb%TYPE;
    BEGIN
      l_out_rsn :=(CASE
                     WHEN(   i_out_rsn IS NULL
                          OR i_out_rsn IN('C', 'L')) THEN '120'
                     WHEN i_out_rsn = 'S' THEN '121'
                     WHEN i_out_rsn = 'I' THEN '122'
                     WHEN i_out_rsn = 'T' THEN 'CAPOUT'
                   END
                  );

      UPDATE ordp120b
         SET pckqtb = GREATEST((pckqtb - i_out_qty), 0),   -- set to 0 when negative
             ntshpb = l_out_rsn
       WHERE div_part = i_div_part
         AND ordnob = i_ord_num
         AND lineb = i_ord_ln;

      IF SQL%ROWCOUNT > 0 THEN
        UPDATE wkly_max_log_op3m l
           SET l.qty = GREATEST(l.qty - i_out_qty, 0)
         WHERE l.div_part = i_div_part
           AND l.qty_typ = 'PCK'
           AND l.ord_num = i_ord_num
           AND l.ord_ln = i_ord_ln;

        UPDATE wkly_max_cust_item_op1m ci
           SET ci.pick_qty = GREATEST(ci.pick_qty - i_out_qty, 0)
         WHERE ci.div_part = i_div_part
           AND (ci.cust_id, ci.catlg_num) = (SELECT a.custa, b.orditb
                                               FROM ordp100a a, ordp120b b
                                              WHERE a.div_part = i_div_part
                                                AND a.ordnoa = i_ord_num
                                                AND b.div_part = a.div_part
                                                AND b.ordnob = i_ord_num
                                                AND b.lineb = i_ord_ln);
      END IF;   -- SQL%ROWCOUNT > 0
    END upd_pick_sp;

    PROCEDURE upd_ord_pick_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
      l_cwt_sw  VARCHAR2(1);
    BEGIN
      SELECT NVL(MAX(di.cwt_sw), 'N')
        INTO l_cwt_sw
        FROM sawp505e e, mclp110b di
       WHERE e.catite = i_r_msg.catlg_num
         AND di.div_part = i_div_part
         AND di.itemb = e.iteme
         AND di.uomb = e.uome;

      -- bypass adjustments for catchweight items
      IF l_cwt_sw = 'N' THEN
        IF TRIM(i_r_msg.cntnr_id) IS NOT NULL THEN
          upd_pick_sp(i_div_part, i_r_msg.ord_num, i_r_msg.ord_ln, i_r_msg.out_qty, i_r_msg.rsn_cd);

          UPDATE bill_cntnr_id_bc1c bc
             SET bc.adj_cntnr_id = DECODE(i_r_msg.cntnr_id, bc.orig_cntnr_id, bc.orig_cntnr_id, bc.adj_cntnr_id),
                 bc.adj_qty = GREATEST(0,
                                       DECODE(i_r_msg.cntnr_id,
                                              bc.orig_cntnr_id,(bc.orig_qty - i_r_msg.out_qty),
                                              (bc.adj_qty - i_r_msg.out_qty
                                              )
                                             )
                                      )
           WHERE bc.div_part = i_div_part
             AND bc.ord_num = i_r_msg.ord_num
             AND bc.ord_ln_num = i_r_msg.ord_ln
             AND NVL(bc.adj_cntnr_id, bc.orig_cntnr_id) = i_r_msg.cntnr_id;
        ELSIF i_r_msg.piccnf_ord_ln_lvl_sw = 'Y' THEN
          upd_pick_sp(i_div_part, i_r_msg.ord_num, i_r_msg.ord_ln, i_r_msg.out_qty, i_r_msg.rsn_cd);
        ELSE
          <<cust_lvl_block>>
          DECLARE
            l_out_qty  PLS_INTEGER;

            CURSOR l_cur_ords(
              b_div_part  NUMBER,
              b_ord_num   NUMBER,
              b_ord_ln    NUMBER
            ) IS
              SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln, NVL(b.pckqtb, 0) AS pick_qty
                  FROM ordp120b b, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
                 WHERE b.div_part = b_div_part
                   AND b.statb = 'R'
                   AND b.subrcb < 999
                   AND b.excptn_sw = 'N'
                   AND a.div_part = b.div_part
                   AND a.ordnoa = b.ordnob
                   AND ld.div_part = a.div_part
                   AND ld.load_depart_sid = a.load_depart_sid
                   AND se.div_part = a.div_part
                   AND se.load_depart_sid = a.load_depart_sid
                   AND se.cust_id = a.custa
                   AND EXISTS(SELECT 1
                                FROM ordp120b b2, ordp100a a2, load_depart_op1f ld2, stop_eta_op1g se2
                               WHERE b2.div_part = b.div_part
                                 AND b2.ordnob = b_ord_num
                                 AND b2.lineb = b_ord_ln
                                 AND b2.excptn_sw = 'N'
                                 AND b2.statb = b.statb
                                 AND b2.itemnb = b.itemnb
                                 AND b2.sllumb = b.sllumb
                                 AND a2.div_part = b2.div_part
                                 AND a2.ordnoa = b2.ordnob
                                 AND ld2.div_part = a2.div_part
                                 AND ld2.load_depart_sid = a2.load_depart_sid
                                 AND ld2.load_num = ld.load_num
                                 AND se2.div_part = a2.div_part
                                 AND se2.load_depart_sid = a2.load_depart_sid
                                 AND se2.cust_id = a2.custa
                                 AND se2.cust_id = se.cust_id
                                 AND se2.stop_num = se.stop_num
                                 AND TRUNC(se2.eta_ts) = TRUNC(se.eta_ts))
              ORDER BY a.dsorda DESC, b.pckqtb DESC, b.lineb DESC;

            l_r_ord    l_cur_ords%ROWTYPE;
          BEGIN
            OPEN l_cur_ords(i_div_part, i_r_msg.ord_num, i_r_msg.ord_ln);

            l_out_qty := i_r_msg.out_qty;
            <<ord_loop>>
            LOOP
              FETCH l_cur_ords
               INTO l_r_ord;

              EXIT ord_loop WHEN l_cur_ords%NOTFOUND
                             OR l_out_qty <= 0;

              IF l_r_ord.pick_qty < l_out_qty THEN
                upd_pick_sp(i_div_part, l_r_ord.ord_num, l_r_ord.ord_ln, l_r_ord.pick_qty, i_r_msg.rsn_cd);
                l_out_qty := l_out_qty - l_r_ord.pick_qty;
              ELSE
                upd_pick_sp(i_div_part, l_r_ord.ord_num, l_r_ord.ord_ln, l_out_qty, i_r_msg.rsn_cd);
                l_out_qty := 0;
              END IF;   -- l_r_ord.pick_qty < l_out_qty
            END LOOP ord_loop;

            CLOSE l_cur_ords;
          EXCEPTION
            WHEN OTHERS THEN
              IF l_cur_ords%ISOPEN THEN
                CLOSE l_cur_ords;
              END IF;

              RAISE;
          END cust_lvl_block;
        END IF;   -- TRIM(i_msg.cntnr_id) IS NOT NULL
      END IF;   -- l_cwt_sw = 'N'
    END upd_ord_pick_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qpiccnf);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qpiccnf,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.dbg('Is Order-Line-Level Pick Confirm');
      l_piccnf_ord_ln_lvl_sw := op_parms_pk.val_exists_for_prfx_fn(l_div_part, op_const_pk.prm_piccnf_ord_ln_lvl, 'Y');
      <<message_loop>>
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);

        -- Override with division-level parm when ON
        IF l_piccnf_ord_ln_lvl_sw = 'Y' THEN
          l_r_msg.piccnf_ord_ln_lvl_sw := 'Y';
        END IF;

        IF    l_r_msg.ord_num > 0
           OR l_r_msg.load_num <> '0000' THEN
          ----------------------------------------------------------------
          -- All Clean Manifest messages are received grouped by load
          -- number. So we can safely update all orders in the system
          -- for a given load after processing all messages for that load.
          -- Check if this is the first Clean Manifest messages being
          -- processed. If so, Update all entries for the load in 'R'
          -- status with a 'P' (in Processing) value in the
          -- acs_load_close_sw field. This will prevent the load from
          -- being closed while the Clean Manifest messages are being
          -- processed.
          -- If this is not the first message, check if the load number
          -- on the last message processed and the current message are
          -- the same. If the load numbers are not the same, first
          -- update all messages for the load on the last message.
          -- Note:
          -- During the OP allocation process the tote entries on the
          -- MCLP370C table are created and inserted with a 'P' status.
          -- MQ messages for non-ACS loads will be sent down during the
          -- mainframe billing process prior to sending the MQ message
          -- to process the "Release Complete" script which updates the
          -- load_status from 'P' to 'R'.
          ----------------------------------------------------------------
          IF l_r_msg.load_num <> l_load_num_sav THEN
            IF l_load_num_sav <> '0000' THEN
              upd_acs_load_clos_sw_sp(l_div_part, l_load_num_sav, 'Y', l_r_msg.dspstn_err_sw, l_r_msg.pct_dscrpncy);
            END IF;   -- l_load_num_sav <> '0000'

            IF l_r_msg.load_num <> '0000' THEN
              upd_acs_load_clos_sw_sp(l_div_part, l_r_msg.load_num, 'P');
            END IF;   -- l_r_msg.load_num <> '0000'

            l_load_num_sav := l_r_msg.load_num;
          END IF;   -- l_r_msg.load_num <> l_load_num_sav

          CASE
            WHEN l_r_msg.ord_num = 0 THEN
              logs.dbg('Clean Manifest Automation/ACS Load Close');
              upd_load_cntnr_sp(l_div_part, l_r_msg, l_ords_not_found_msg);

              IF l_ords_not_found_msg IS NOT NULL THEN
                logs.warn(l_ords_not_found_msg, lar_parm);
              END IF;   -- l_ords_not_found_msg IS NOT NULL
            WHEN(    l_r_msg.ord_num > 0
                 AND l_r_msg.out_qty = 0
                 AND TRIM(l_r_msg.cntnr_id) IS NOT NULL) THEN
              logs.dbg('PTL Container Adjustment');
              upd_ord_cntnr_sp(l_div_part, l_r_msg);
            ELSE
              logs.dbg('Automated Pick-Confirm Updates');
              l_is_pic_cnf_msg := TRUE;
              upd_ord_pick_sp(l_div_part, l_r_msg);
          END CASE;
        END IF;   -- l_r_msg.ord_num > 0 OR l_r_msg.load_num <> '0000'

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, g_c_compl);

        -- if the processed message is a pick confirm message commit,
        -- otherwise continue. Commits will be a load level (could have
        -- multiple messages per load) for clean manifest process.
        IF l_is_pic_cnf_msg THEN
          COMMIT;
          -- reset PC flag, since we could have PC and CM messages in
          -- the same batch of messages.
          l_is_pic_cnf_msg := FALSE;
        END IF;   -- v_is_pic_cnf_msg
      END LOOP message_loop;

      IF l_load_num_sav <> '0000' THEN
        -- Clean Manifest updates were applied at Load/Manifest/Tote level.
        -- Update remaining orders at the Load Level and commit any pending
        -- updates.
        upd_acs_load_clos_sw_sp(l_div_part, l_load_num_sav, 'Y', l_r_msg.dspstn_err_sw, l_r_msg.pct_dscrpncy);
      END IF;   -- l_load_num_sav <> '0000'

      -- commit all pending updates
      COMMIT;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qpiccnf,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qpiccnf,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qpiccnf_sp;

  /*
  ||----------------------------------------------------------------------------
  || CWTCOMPL_SP
  ||  Set Catchweight complete status for Load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/18/01 | rhalpai | Original for PIR10251
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cwtcompl_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.CWTCOMPL_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                        := 'CWTCOMPL';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div       VARCHAR2(2),
      actn_cd   VARCHAR2(3),
      llr_dt    DATE,
      load_num  VARCHAR2(4)
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.llr_dt := string_to_date_fn(SUBSTR(i_msg_data, 54, 10), 'YYYY-MM-DD');
      l_r_parsed.load_num := SUBSTR(i_msg_data, 64, 4);
      RETURN(l_r_parsed);
    END parse_msg_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_cwtcompl);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_cwtcompl,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        UPDATE load_clos_cntrl_bc2c lc
           SET lc.cwt_compl_sw = 'Y'
         WHERE lc.div_part = l_div_part
           AND lc.llr_dt = l_r_msg.llr_dt
           AND lc.load_num = l_r_msg.load_num
           AND lc.load_status = 'R'
           AND lc.cwt_compl_sw = 'N';

        IF SQL%ROWCOUNT = 0 THEN
          l_mq_msg_stat := g_c_prb;
        END IF;   -- SQL%ROWCOUNT = 0

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_cwtcompl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_cwtcompl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END cwtcompl_sp;

  /*
  ||----------------------------------------------------------------------------
  || QGOVCTL_SP
  ||  Maps messages that describe Government Control entities.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/15/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qgovctl_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QGOVCTL_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE                               := SYSDATE;
    l_div_part            NUMBER;
    l_mq_msg_stat         mclane_mq_get.mq_msg_status%TYPE;
    l_div                 div_mstr_di1d.div_id%TYPE;
    l_actn_cd             VARCHAR2(3);
    l_gov_cntl_id         NUMBER;

    CURSOR l_cur_gov_cntl_mqmsg(
      b_div_part  NUMBER
    ) IS
      SELECT   mq_get_id, SUBSTR(mq_msg_data, 3, 7) AS msg_id, mq_msg_data
          FROM mclane_mq_get
         WHERE div_part = b_div_part
           AND mq_msg_id = 'QGOVCTL'
           AND mq_msg_status = 'OPN'
      ORDER BY mq_get_id;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qgovctl);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qgovctl,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      <<gov_cntl_mqmsg_loop>>
      FOR l_r_mq_msg IN l_cur_gov_cntl_mqmsg(l_div_part) LOOP
        CASE l_r_mq_msg.msg_id
          WHEN 'QGCTL01' THEN
            ---------------------------------------------------------------------------
            -- Government Control ID
            ---------------------------------------------------------------------------
            DECLARE
              l_descr              gov_cntl_p600a.descr%TYPE;   --   gov control description
              l_gov_cntl_amt       NUMBER;   --                      gov control amount
              l_gov_cntl_prd       NUMBER;   --                      gov control period
              l_gov_cntl_hier_seq  NUMBER;   --                      gov control hierarchy seq
            BEGIN
              l_mq_msg_stat := g_c_compl;
              logs.dbg('Parse QGCTL01');
              l_div := SUBSTR(l_r_mq_msg.mq_msg_data, 1, 2);
              l_actn_cd := SUBSTR(l_r_mq_msg.mq_msg_data, 41, 3);
              l_gov_cntl_id := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 54, 9));
              l_descr := RTRIM(SUBSTR(l_r_mq_msg.mq_msg_data, 63, 40));
              l_gov_cntl_amt := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 103, 11)
                                          || '.'
                                          || SUBSTR(l_r_mq_msg.mq_msg_data, 114, 4)
                                         );
              l_gov_cntl_prd := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 118, 3));
              l_gov_cntl_hier_seq := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 121, 3));

              CASE
                WHEN(l_actn_cd = 'DEL') THEN
                  logs.dbg('(DEL) GOV_CNTL_P600A');

                  DELETE FROM gov_cntl_p600a a
                        WHERE a.div_part = l_div_part
                          AND a.gov_cntl_id = l_gov_cntl_id;
                WHEN(l_actn_cd IN('ADD', 'CHG')) THEN
                  logs.dbg('(ADD/CHG) GOV_CNTL_P600A');
                  MERGE INTO gov_cntl_p600a a
                       USING (SELECT 1 tst
                                FROM DUAL) x
                          ON (a.div_part = l_div_part
                          AND a.gov_cntl_id = l_gov_cntl_id
                          AND x.tst > 0)
                    WHEN MATCHED THEN
                      UPDATE
                         SET a.descr = l_descr, a.gov_cntl_amt = l_gov_cntl_amt, a.gov_cntl_prd = l_gov_cntl_prd,
                             a.gov_cntl_hier_seq = l_gov_cntl_hier_seq
                    WHEN NOT MATCHED THEN
                      INSERT(a.div_part, a.gov_cntl_id, a.descr, a.gov_cntl_amt, a.gov_cntl_prd, a.gov_cntl_hier_seq)
                      VALUES(l_div_part, l_gov_cntl_id, l_descr, l_gov_cntl_amt, l_gov_cntl_prd, l_gov_cntl_hier_seq);
                ELSE
                  NULL;
              END CASE;
            END;
          WHEN 'QGITM01' THEN
            ---------------------------------------------------------------------------
            -- Government Control Item
            ---------------------------------------------------------------------------
            DECLARE
              l_item_num     gov_cntl_item_p660a.item_num%TYPE;   -- item number
              l_uom          gov_cntl_item_p660a.uom%TYPE;   --      unit of measure
              l_gov_cntl_pt  NUMBER;   --                            gov control item point value
            BEGIN
              l_mq_msg_stat := g_c_compl;
              logs.dbg('Parse QGITM01');
              l_div := SUBSTR(l_r_mq_msg.mq_msg_data, 1, 2);
              l_actn_cd := SUBSTR(l_r_mq_msg.mq_msg_data, 41, 3);
              l_gov_cntl_id := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 54, 9));
              l_item_num := SUBSTR(l_r_mq_msg.mq_msg_data, 63, 9);
              l_uom := SUBSTR(l_r_mq_msg.mq_msg_data, 72, 3);
              l_gov_cntl_pt := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 75, 11)
                                         || '.'
                                         || SUBSTR(l_r_mq_msg.mq_msg_data, 86, 4)
                                        );

              CASE
                WHEN(l_actn_cd = 'DEL') THEN
                  logs.dbg('(DEL) DELETE-FROM-GOV_CNTL_ITEM_P660A');

                  DELETE FROM gov_cntl_item_p660a a
                        WHERE a.div_part = l_div_part
                          AND a.gov_cntl_id = l_gov_cntl_id
                          AND a.item_num = l_item_num
                          AND a.uom = l_uom;
                WHEN(l_actn_cd IN('ADD', 'CHG')) THEN
                  logs.dbg('(ADD/CHG) GOV_CNTL_ITEM_P660A');

                  BEGIN
                    MERGE INTO gov_cntl_item_p660a a
                         USING (SELECT 1 tst
                                  FROM DUAL) x
                            ON (a.div_part = l_div_part
                            AND a.gov_cntl_id = l_gov_cntl_id
                            AND a.item_num = l_item_num
                            AND a.uom = l_uom
                            AND x.tst > 0)
                      WHEN MATCHED THEN
                        UPDATE
                           SET a.gov_cntl_pt = l_gov_cntl_pt
                      WHEN NOT MATCHED THEN
                        INSERT(a.div_part, a.gov_cntl_id, a.item_num, a.uom, a.gov_cntl_pt)
                        VALUES(l_div_part, l_gov_cntl_id, l_item_num, l_uom, l_gov_cntl_pt);
                  EXCEPTION
                    WHEN excp.gx_parent_integrity_constraint THEN
                      l_mq_msg_stat := g_c_opn;
                  END;
                ELSE
                  NULL;
              END CASE;
            END;
          WHEN 'QGCST01' THEN
            ---------------------------------------------------------------------------
            -- Government Control Customer
            ---------------------------------------------------------------------------
            DECLARE
              l_cust_id  gov_cntl_cust_p640a.cust_num%TYPE;   -- customer number
            BEGIN
              l_mq_msg_stat := g_c_compl;
              logs.dbg('Parse QGCST01');
              l_div := SUBSTR(l_r_mq_msg.mq_msg_data, 1, 2);
              l_actn_cd := SUBSTR(l_r_mq_msg.mq_msg_data, 41, 3);
              l_gov_cntl_id := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 54, 9));
              l_cust_id := SUBSTR(l_r_mq_msg.mq_msg_data, 63, 8);

              CASE
                WHEN(l_actn_cd = 'DEL') THEN
                  logs.dbg('(DEL) Inactivate GOV_CNTL_CUST_P640A');

                  UPDATE gov_cntl_cust_p640a a
                     SET a.status = 2
                   WHERE a.div_part = l_div_part
                     AND a.gov_cntl_id = l_gov_cntl_id
                     AND a.cust_num = l_cust_id
                     AND a.status < 2;
                WHEN(l_actn_cd IN('ADD', 'CHG')) THEN
                  logs.dbg('(ADD) GOV_CNTL_CUST_P640A');

                  DECLARE
                    l_item_cntrl_exists_sw  VARCHAR2(1);
                  BEGIN
                    -- Check for gov control containing an item in another gov control
                    -- that is already attached to the customer and both have the same
                    -- sequence number.  (this is to prevent multiple rows from being
                    -- returned during allocation when a customer orders the item and
                    -- we look for the max sequence number to determine which gov control
                    -- id to use)
                    SELECT NVL(MAX('Y'), 'N')
                      INTO l_item_cntrl_exists_sw
                      FROM gov_cntl_p600a g, gov_cntl_item_p660a i
                     WHERE g.div_part = l_div_part
                       AND g.gov_cntl_id = l_gov_cntl_id
                       AND i.div_part = g.div_part
                       AND i.gov_cntl_id = g.gov_cntl_id
                       AND EXISTS(SELECT 1
                                    FROM gov_cntl_p600a g2, gov_cntl_cust_p640a c, gov_cntl_item_p660a i2
                                   WHERE g2.div_part = g.div_part
                                     AND g2.gov_cntl_hier_seq = g.gov_cntl_hier_seq
                                     AND c.div_part = g2.div_part
                                     AND c.gov_cntl_id = g2.gov_cntl_id
                                     AND c.cust_num = l_cust_id
                                     AND c.status < 2
                                     AND i2.div_part = g2.div_part
                                     AND i2.gov_cntl_id = g2.gov_cntl_id
                                     AND i2.gov_cntl_id <> i.gov_cntl_id
                                     AND i2.item_num = i.item_num
                                     AND i2.uom = i.uom);

                    IF l_item_cntrl_exists_sw = 'Y' THEN
                      l_mq_msg_stat := g_c_compl;
                    ELSE
                      -- Update Existing Unexpired Entry
                      UPDATE gov_cntl_cust_p640a c
                         SET c.status = 0
                       WHERE c.div_part = l_div_part
                         AND c.gov_cntl_id = l_gov_cntl_id
                         AND c.cust_num = l_cust_id
                         AND c.prd_beg_ts >= (SELECT (l_c_sysdate - g.gov_cntl_prd)
                                                FROM gov_cntl_p600a g
                                               WHERE g.div_part = l_div_part
                                                 AND g.gov_cntl_id = l_gov_cntl_id);

                      IF (SQL%NOTFOUND) THEN
                        -- Insert New Entry
                        INSERT INTO gov_cntl_cust_p640a
                                    (div_part, gov_cntl_id, cust_num, prd_beg_ts, shp_pts, tot_pts, status
                                    )
                             VALUES (l_div_part, l_gov_cntl_id, l_cust_id, l_c_sysdate, 0, 0, 0
                                    );
                      END IF;   -- (SQL%NOTFOUND)
                    END IF;
                  EXCEPTION
                    WHEN excp.gx_parent_integrity_constraint THEN
                      l_mq_msg_stat := g_c_opn;
                    WHEN DUP_VAL_ON_INDEX THEN
                      l_mq_msg_stat := g_c_compl;
                  END;
                ELSE
                  NULL;
              END CASE;
            END;
          WHEN 'QGCRD01' THEN
            ---------------------------------------------------------------------------
            -- Government Control Credits
            ---------------------------------------------------------------------------
            DECLARE
              l_cust_id     gov_cntl_cust_p640a.cust_num%TYPE;   -- customer number
              l_item_num    gov_cntl_item_p660a.item_num%TYPE;   -- item number
              l_uom         gov_cntl_item_p660a.uom%TYPE;   --     unit of measure
              l_qty         PLS_INTEGER;
              l_prd_beg_ts  gov_cntl_cust_p640a.prd_beg_ts%TYPE;   -- gov control period begin
              l_points      NUMBER;
            BEGIN
              l_mq_msg_stat := g_c_compl;
              logs.dbg('Parse QGCRD01');
              l_div := SUBSTR(l_r_mq_msg.mq_msg_data, 1, 2);
              l_cust_id := SUBSTR(l_r_mq_msg.mq_msg_data, 54, 8);
              l_item_num := SUBSTR(l_r_mq_msg.mq_msg_data, 62, 9);
              l_uom := SUBSTR(l_r_mq_msg.mq_msg_data, 71, 3);
              l_qty := TO_NUMBER(SUBSTR(l_r_mq_msg.mq_msg_data, 74, 5));
              logs.dbg('Get Gov Control Info');

              SELECT x.gov_cntl_id, x.gov_cntl_pt * l_qty, x.prd_beg_ts
                INTO l_gov_cntl_id, l_points, l_prd_beg_ts
                FROM (SELECT g.gov_cntl_id, i.gov_cntl_pt, c.prd_beg_ts
                        FROM gov_cntl_p600a g
                       INNER JOIN gov_cntl_cust_p640a c
                          ON (    c.div_part = g.div_part
                              AND c.gov_cntl_id = g.gov_cntl_id
                              AND c.cust_num = l_cust_id
                              AND c.prd_beg_ts = (SELECT MAX(prd_beg_ts)
                                                     FROM gov_cntl_cust_p640a x
                                                    WHERE x.div_part = c.div_part
                                                      AND x.gov_cntl_id = c.gov_cntl_id
                                                      AND x.cust_num = c.cust_num
                                                      AND x.status = 1)
                             )
                       INNER JOIN(mclp110b di
                                  INNER JOIN gov_cntl_item_p660a i
                                     ON (    i.div_part = di.div_part
                                         AND i.item_num = DECODE(di.suomb, NULL, di.itemb, di.sitemb)
                                         AND i.uom = DECODE(di.suomb, NULL, di.uomb, di.suomb)
                                        )
                                 )
                          ON (    di.div_part = c.div_part
                              AND di.itemb = l_item_num
                              AND di.uomb = l_uom
                              AND i.div_part = g.div_part
                              AND i.gov_cntl_id = g.gov_cntl_id
                             )
                       WHERE g.div_part = l_div_part
                       ORDER BY g.gov_cntl_hier_seq DESC
                      ) x
               WHERE ROWNUM = 1;

              logs.dbg('Apply Credits');

              -- Apply credit while preventing negatives
              UPDATE gov_cntl_cust_p640a a
                 SET a.shp_pts =
                     (SELECT nvl(c1.shp_pts, l_points) - l_points
                        FROM gov_cntl_cust_p640a c
                        LEFT OUTER JOIN gov_cntl_cust_p640a c1
                          ON (    c1.div_part = c.div_part
                              AND c1.gov_cntl_id = c.gov_cntl_id
                              AND c1.cust_num = c.cust_num
                              AND c1.prd_beg_ts = c.prd_beg_ts
                              AND (c1.shp_pts - l_points) > 0
                             )
                       WHERE c.div_part = a.div_part
                         AND c.gov_cntl_id = a.gov_cntl_id
                         AND c.cust_num = a.cust_num
                         AND c.prd_beg_ts = a.prd_beg_ts),
                     a.tot_pts =
                     (SELECT nvl(c2.tot_pts, l_points) - l_points
                        FROM gov_cntl_cust_p640a cc
                        LEFT OUTER JOIN gov_cntl_cust_p640a c2
                          ON (    c2.div_part = cc.div_part
                              AND c2.gov_cntl_id = cc.gov_cntl_id
                              AND c2.cust_num = cc.cust_num
                              AND c2.prd_beg_ts = cc.prd_beg_ts
                              AND (c2.tot_pts - l_points) > 0
                             )
                       WHERE cc.div_part = a.div_part
                         AND cc.gov_cntl_id = a.gov_cntl_id
                         AND cc.cust_num = a.cust_num
                         AND cc.prd_beg_ts = a.prd_beg_ts)
               WHERE a.div_part = l_div_part
                 AND a.gov_cntl_id = l_gov_cntl_id
                 AND a.cust_num = l_cust_id
                 AND a.prd_beg_ts = l_prd_beg_ts;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                -- if no gov control info found then blow off the credit
                l_mq_msg_stat := g_c_compl;
            END;
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP gov_cntl_mqmsg_loop;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qgovctl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, op_const_pk.prcs_qgovctl || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qgovctl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qgovctl_sp;

  /*
  ||----------------------------------------------------------------------------
  || REROUTE_SP
  ||  Process message for maintenance to Reroute table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/23/10 | rhalpai | Original - created for PIR7415
  || 02/04/11 | rhalpai | Converted from QOPMSGS to stand-alone REROUTE queue.
  ||                    | IM-004248
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reroute_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.REROUTE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'REROUTE';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div          VARCHAR2(2),
      actn_cd      VARCHAR2(3),
      load_num     reroute_rt1r.load_num%TYPE,
      stop_num     NUMBER,
      cust_id      reroute_rt1r.cust_id%TYPE,
      llr_day      reroute_rt1r.llr_day%TYPE,
      llr_dt       DATE,
      llr_time     NUMBER,
      depart_day   reroute_rt1r.depart_day%TYPE,
      depart_time  NUMBER,
      eta_day      reroute_rt1r.eta_day%TYPE,
      eta_time     NUMBER,
      eff_ts       DATE,
      end_ts       DATE
    );

    l_r_msg              l_rt_msg;

    FUNCTION day_fn(
      i_day_cd  IN  VARCHAR2
    )
      RETURN VARCHAR2 IS
    BEGIN
      RETURN(CASE i_day_cd
               WHEN 'SU' THEN 'SUN'
               WHEN 'MO' THEN 'MON'
               WHEN 'TU' THEN 'TUE'
               WHEN 'WE' THEN 'WED'
               WHEN 'TH' THEN 'THU'
               WHEN 'FR' THEN 'FRI'
               WHEN 'SA' THEN 'SAT'
             END
            );
    END day_fn;

    FUNCTION time_fn(
      i_time_cd  IN  VARCHAR2
    )
      RETURN NUMBER IS
    BEGIN
      -- format of p_time_cd is HH24:MI:SS
      -- returned time should be HH24MI
      RETURN(string_to_num_fn(SUBSTR(REPLACE(i_time_cd, ':'), 1, 4)));
    END time_fn;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.load_num := SUBSTR(i_msg_data, 54, 4);
      l_r_parsed.stop_num := string_to_num_fn(SUBSTR(i_msg_data, 58, 4));
      l_r_parsed.cust_id := SUBSTR(i_msg_data, 62, 8);
      l_r_parsed.llr_day := day_fn(SUBSTR(i_msg_data, 70, 2));
      l_r_parsed.llr_dt := TO_DATE(SUBSTR(i_msg_data, 72, 10), 'YYYY-MM-DD');
      l_r_parsed.llr_time := time_fn(SUBSTR(i_msg_data, 82, 8));
      l_r_parsed.depart_day := day_fn(SUBSTR(i_msg_data, 90, 2));
      l_r_parsed.depart_time := time_fn(SUBSTR(i_msg_data, 92, 8));
      l_r_parsed.eta_day := day_fn(SUBSTR(i_msg_data, 100, 2));
      l_r_parsed.eta_time := time_fn(SUBSTR(i_msg_data, 102, 8));
      l_r_parsed.eff_ts := TO_DATE(SUBSTR(i_msg_data, 110, 18), 'YYYY-MM-DDHH24:MI:SS');
      l_r_parsed.end_ts := TO_DATE(SUBSTR(i_msg_data, 128, 18), 'YYYY-MM-DDHH24:MI:SS');
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM reroute_rt1r r
            WHERE r.div_part = i_div_part
              AND r.cust_id = i_r_msg.cust_id
              AND r.load_num = i_r_msg.load_num
              AND r.llr_dt = i_r_msg.llr_dt;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO reroute_rt1r r
           USING (SELECT d.div_part
                    FROM div_mstr_di1d d
                   WHERE d.div_id = i_r_msg.div) x
              ON (    r.div_part = x.div_part
                  AND r.cust_id = i_r_msg.cust_id
                  AND r.load_num = i_r_msg.load_num
                  AND r.llr_dt = i_r_msg.llr_dt)
        WHEN MATCHED THEN
          UPDATE
             SET r.stop_num = i_r_msg.stop_num, r.llr_day = i_r_msg.llr_day, r.llr_time = i_r_msg.llr_time,
                 r.depart_day = i_r_msg.depart_day, r.depart_time = i_r_msg.depart_time, r.eta_day = i_r_msg.eta_day,
                 r.eta_time = i_r_msg.eta_time, r.eff_ts = i_r_msg.eff_ts, r.end_ts = i_r_msg.end_ts
        WHEN NOT MATCHED THEN
          INSERT(div_part, cust_id, load_num, stop_num, llr_day, llr_dt, llr_time, depart_day, depart_time, eta_day,
                 eta_time, eff_ts, end_ts)
          VALUES(x.div_part, i_r_msg.cust_id, i_r_msg.load_num, i_r_msg.stop_num, i_r_msg.llr_day, i_r_msg.llr_dt,
                 i_r_msg.llr_time, i_r_msg.depart_day, i_r_msg.depart_time, i_r_msg.eta_day, i_r_msg.eta_time,
                 i_r_msg.eff_ts, i_r_msg.end_ts);
    END merge_sp;

    PROCEDURE process_msg_sp(
      i_div_part     IN      NUMBER,
      i_msg_data     IN      VARCHAR2,
      o_mq_msg_stat  OUT     VARCHAR2
    ) IS
      l_r_msg  l_rt_msg;
    BEGIN
      o_mq_msg_stat := g_c_compl;
      logs.dbg('Parse MQ Msg Data');
      l_r_msg := parse_msg_fn(i_msg_data);
      logs.dbg('Process Msg');

      CASE
        WHEN l_r_msg.actn_cd = g_c_del THEN
          logs.dbg('Remove Entry');
          del_sp(i_div_part, l_r_msg);
        WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
          logs.dbg('Add/Chg Entry');
          merge_sp(l_r_msg);
      END CASE;
    EXCEPTION
      WHEN OTHERS THEN
        o_mq_msg_stat := g_c_prb;
        logs.err(lar_parm, 'MsgData: ' || i_msg_data, FALSE);
        ROLLBACK;
    END process_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_reroute);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_reroute,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        logs.dbg('Process MQ Msg');
        process_msg_sp(l_div_part, l_r_msg.mq_msg_data, l_mq_msg_stat);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_reroute,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_reroute,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END reroute_sp;

  /*
  ||----------------------------------------------------------------------------
  || IMQ01_SP
  ||  Interface for Weekly Max Qty maintenance
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/11 | rhalpai | Original for PIR6235
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 04/03/12 | rhalpai | Remove CUST_ITEM_SID_FN and INS_CUST_ITEM_SID_SP.
  ||                    | PIR10651
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 10/07/13 | rhalpai | Change delete logic to remove matching entry from
  ||                    | WKLY_MAX_CUST_ITEM_OP1M which will cascade delete
  ||                    | from WKLY_MAX_CUST_ITEM_OP1M. PIR11038
  || 11/19/13 | rhalpai | Change delete logic to remove entry from WklyMaxQty
  ||                    | table matching Cust/Item/EffDt and remove entry from
  ||                    | WklyMaxCustItem table when there are no more matching
  ||                    | Cust/Item entries in WklyMaxQty. IM-126167
  || 12/08/15 | rhalpai | Add DivPart in calls to OP_ORDER_VALIDATION_PK.CHECK_MAX_QTY_SP,
  ||                    | OP_MCLP300D_PK.INS_SP.
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 12/31/19 | rhalpai | Change parsing logic to handle 4-digit DivPart. PIR19060
  ||----------------------------------------------------------------------------
  */
  PROCEDURE imq01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.IMQ01_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                        := 'IMQ01';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div        VARCHAR2(2),
      actn_cd    VARCHAR2(3),
      div_part   NUMBER,
      cust_id    sysp200c.acnoc%TYPE,
      catlg_num  NUMBER,
      max_qty    NUMBER,
      eff_dt     DATE,
      end_dt     DATE,
      dist_sw    VARCHAR2(1),
      adj_qty    NUMBER
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.div_part := string_to_num_fn(SUBSTR(i_msg_data, 54, 4));
      l_r_parsed.cust_id := SUBSTR(i_msg_data, 58, 8);
      l_r_parsed.catlg_num := string_to_num_fn(SUBSTR(i_msg_data, 66, 6));
      l_r_parsed.max_qty := string_to_num_fn(SUBSTR(i_msg_data, 72, 7));
      l_r_parsed.eff_dt := string_to_date_fn(SUBSTR(i_msg_data, 79, 10), 'YYYY-MM-DD');
      l_r_parsed.end_dt := string_to_date_fn(SUBSTR(i_msg_data, 89, 10), 'YYYY-MM-DD');
      l_r_parsed.dist_sw := SUBSTR(i_msg_data, 99, 1);
      l_r_parsed.adj_qty := string_to_num_fn(SUBSTR(i_msg_data, 107, 1) || SUBSTR(i_msg_data, 100, 7));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE adj_pick_qty_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      UPDATE wkly_max_cust_item_op1m ci
         SET ci.pick_qty = GREATEST(0, ci.pick_qty + i_r_msg.adj_qty)
       WHERE ci.div_part = i_r_msg.div_part
         AND ci.cust_id = i_r_msg.cust_id
         AND ci.catlg_num = i_r_msg.catlg_num;
    END adj_pick_qty_sp;

    PROCEDURE undo_wkly_max_applied_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
      CURSOR l_cur_ords(
        b_div_part   NUMBER,
        b_cust_id    VARCHAR2,
        b_catlg_num  VARCHAR2
      ) IS
        SELECT   d.ordnod AS ord_num, d.ordlnd AS ord_ln, d.itemd AS cbr_item, d.uomd AS uom, d.qtytod AS ord_qty,
                 d.qtyfrd AS new_ord_qty
            FROM mclp300d d
           WHERE d.div_part = b_div_part
             AND d.reasnd = 'WKMAXQTY'
             AND (d.ordnod, d.ordlnd, d.last_chg_ts) IN(SELECT   d2.ordnod, d2.ordlnd,
                                                                 MAX(d2.last_chg_ts) AS last_chg_ts
                                                            FROM mclp300d d2
                                                           WHERE d2.div_part = b_div_part
                                                             AND d2.reasnd = 'WKMAXQTY'
                                                             AND (d2.itemd, d2.uomd) = (SELECT e.iteme, e.uome
                                                                                          FROM sawp505e e
                                                                                         WHERE e.catite = b_catlg_num)
                                                        GROUP BY d2.ordnod, d2.ordlnd)
             AND EXISTS(SELECT 1
                          FROM ordp120b b, ordp100a a
                         WHERE b.div_part = d.div_part
                           AND b.ordnob = d.ordnod
                           AND b.lineb = d.ordlnd
                           AND b.itemnb = d.itemd
                           AND b.sllumb = d.uomd
                           AND b.ordqtb = d.qtytod
                           AND b.statb = 'O'
                           AND a.div_part = b.div_part
                           AND a.ordnoa = b.ordnob
                           AND a.custa = b_cust_id)
             AND NOT EXISTS(SELECT 1
                              FROM mclp300d d2
                             WHERE d2.div_part = d.div_part
                               AND d2.ordnod = d.ordnod
                               AND d2.ordlnd = d.ordlnd
                               AND d2.last_chg_ts > d.last_chg_ts
                               AND d2.reasnd = 'WKMAXDEL')
        ORDER BY d.ordnod, d.ordlnd;
    BEGIN
      FOR l_r_ord IN l_cur_ords(i_r_msg.div_part, i_r_msg.cust_id, LPAD(i_r_msg.catlg_num, 6, '0')) LOOP
        UPDATE ordp120b b
           SET b.ordqtb = l_r_ord.new_ord_qty
         WHERE b.div_part = i_r_msg.div_part
           AND b.ordnob = l_r_ord.ord_num
           AND b.lineb = l_r_ord.ord_ln
           AND b.statb = 'O';

        IF SQL%ROWCOUNT > 0 THEN
          op_mclp300d_pk.ins_sp(i_r_msg.div_part,
                                l_r_ord.ord_num,
                                l_r_ord.ord_ln,
                                'WKMAXDEL',
                                l_r_ord.cbr_item,
                                l_r_ord.uom,
                                l_r_ord.ord_qty,
                                l_r_ord.new_ord_qty
                               );
          op_order_validation_pk.validate_details_sp(i_r_msg.div_part, l_r_ord.ord_num, l_r_ord.ord_ln);
        END IF;   -- SQL%ROWCOUNT = 0
      END LOOP;
    END undo_wkly_max_applied_sp;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM wkly_max_qty_op2m q
            WHERE q.div_part = i_r_msg.div_part
              AND q.cust_item_sid = (SELECT ci.cust_item_sid
                                       FROM wkly_max_cust_item_op1m ci
                                      WHERE ci.div_part = i_r_msg.div_part
                                        AND ci.cust_id = i_r_msg.cust_id
                                        AND ci.catlg_num = i_r_msg.catlg_num)
              AND q.eff_dt = i_r_msg.eff_dt;

      DELETE FROM wkly_max_cust_item_op1m ci
            WHERE ci.div_part = i_r_msg.div_part
              AND ci.cust_id = i_r_msg.cust_id
              AND ci.catlg_num = i_r_msg.catlg_num
              AND NOT EXISTS(SELECT 1
                               FROM wkly_max_qty_op2m q
                              WHERE q.div_part = i_r_msg.div_part
                                AND q.cust_item_sid = ci.cust_item_sid);

      IF SQL%ROWCOUNT > 0 THEN
        undo_wkly_max_applied_sp(i_r_msg);
      END IF;   -- SQL%ROWCOUNT > 0
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
      l_cust_item_sid  NUMBER;
    BEGIN
      l_cust_item_sid := cust_item_sid_fn(i_r_msg.div_part, i_r_msg.cust_id, i_r_msg.catlg_num);
      MERGE INTO wkly_max_qty_op2m q
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    q.div_part = i_r_msg.div_part
                  AND q.cust_item_sid = l_cust_item_sid
                  AND q.eff_dt = i_r_msg.eff_dt
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET q.end_dt = i_r_msg.end_dt, q.max_qty = i_r_msg.max_qty, q.dist_sw = i_r_msg.dist_sw
        WHEN NOT MATCHED THEN
          INSERT(div_part, cust_item_sid, eff_dt, end_dt, max_qty, dist_sw)
          VALUES(i_r_msg.div_part, l_cust_item_sid, i_r_msg.eff_dt, i_r_msg.end_dt, i_r_msg.max_qty, i_r_msg.dist_sw);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_imq01);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_wkmaxqty,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.adj_qty <> 0 THEN
            logs.dbg('Adj PickQty');
            adj_pick_qty_sp(l_r_msg);
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_wkmaxqty,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_wkmaxqty,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END imq01_sp;

  /*
  ||----------------------------------------------------------------------------
  || BUSMOVE_SP
  ||  Interface for Business Moves / Business Continuity
  ||  Import WklyMaxQty info, Orders in Shipped status for current week Sun-Sat,
  ||  orders in Suspended status, and orders in Open status from Old Division.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/12 | rhalpai | Original for PIR10651
  || 07/10/12 | dlbeal  | Added load_depart_sid
  || 07/10/12 | rhalpai | Add logic to update MF DB2 BE3T/BE4T for moved orders.
  ||                    | PIR10651
  || 04/04/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1G.
  ||                    | Change to use OrdTyp to indicate TestSw,NoOrdSw.
  ||                    | PIR11038
  || 02/07/14 | rhalpai | Move BusMove logic to its own package and add logic
  ||                    | to handle processing from single DB with multiple
  ||                    | divisions. PIR11038
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE busmove_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.BUSMOVE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                        := 'BUSMOVE';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      cust_id  sysp200c.acnoc%TYPE,
      old_div  VARCHAR2(2)
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.cust_id := SUBSTR(i_msg_data, 54, 8);
      l_r_parsed.old_div := SUBSTR(i_msg_data, 62, 2);
      RETURN(l_r_parsed);
    END parse_msg_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_busmove);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_busmove,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        BEGIN
          logs.dbg('Process Msg');
          op_bus_move_pk.move_cust_sp(i_div, l_r_msg.cust_id, l_r_msg.old_div);
        EXCEPTION
          WHEN OTHERS THEN
            logs.err(lar_parm, 'MQGetID: ' || l_mq_get_id, FALSE);
            l_mq_msg_stat := g_c_prb;
            ROLLBACK;
        END;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_busmove,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_busmove,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END busmove_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCATG01_SP
  ||  Process message for NACS category maintenance
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/18/13 | dbeal   | Original - created for PIR11038
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcatg01_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCATG01_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCATG01';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div      VARCHAR2(2),
      actn_cd  VARCHAR2(3),
      nacs     mclp220d.nacsd%TYPE,
      descr    mclp220d.descd%TYPE,
      nacsh    mclp220d.nacshd%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.nacs := RTRIM(SUBSTR(i_msg_data, 54, 3));
      l_r_parsed.descr := RTRIM(SUBSTR(i_msg_data, 57, 30));
      l_r_parsed.nacsh := RTRIM(SUBSTR(i_msg_data, 87, 11));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp220d d
            WHERE d.nacsd = i_r_msg.nacs;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO mclp220d d
           USING (SELECT 1 tst
                    FROM DUAL) x
              ON (    d.nacsd = i_r_msg.nacs
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET descd = i_r_msg.descr, nacshd = i_r_msg.nacsh
        WHEN NOT MATCHED THEN
          INSERT(nacsd, descd, nacshd)
          VALUES(i_r_msg.nacs, i_r_msg.descr, i_r_msg.nacsh);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcatg01);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcatg01,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcatg01,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcatg01,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcatg01_sp;

  /*
  ||----------------------------------------------------------------------------
  || QCATG02_SP
  ||  Scoreboard Category Maintenance
  ||  Corporate level MQ interface for Scoreboard Categories (MC.CUSPSCORECD4S)
  ||  produced by MCQCT02J.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/22/15 | rhalpai | Original - created for PIR15202
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qcatg02_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QCATG02_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'QCATG02';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd  VARCHAR2(3),
      catg_cd  mclp230a.sbcata%TYPE,
      descr    mclp230a.desca%TYPE
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.catg_cd := RTRIM(SUBSTR(i_msg_data, 54, 3));
      l_r_parsed.descr := RTRIM(SUBSTR(i_msg_data, 57, 30));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM mclp230a a
            WHERE a.sbcata = i_r_msg.catg_cd;
    END del_sp;

    PROCEDURE merge_sp(
      i_r_msg  IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO mclp230a a
           USING (SELECT 1 AS tst,
                         NVL((SELECT TO_NUMBER(SUBSTR(p.parm_id, 8, 1))
                                FROM appl_sys_parm_ap1s p
                               WHERE p.div_part = 0
                                 AND p.parm_id LIKE 'SCB_TYP%'
                                 AND p.vchar_val = i_r_msg.catg_cd),
                             0
                            ) AS cntnr_itm_typ
                    FROM DUAL) x
              ON (    a.sbcata = i_r_msg.catg_cd
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET desca = i_r_msg.descr, cntnr_itm_typ = x.cntnr_itm_typ
        WHEN NOT MATCHED THEN
          INSERT(sbcata, desca, cntnr_itm_typ)
          VALUES(i_r_msg.catg_cd, i_r_msg.descr, x.cntnr_itm_typ);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qcatg02);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcatg02,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcatg02,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qcatg02,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qcatg02_sp;

  /*
  ||----------------------------------------------------------------------------
  || REQUEST_SP
  ||  Generic MQ message request to process a command or script contained
  ||  within the MQ message.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/23/13 | rhalpai | Original - created for PIR11038
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  || 12/31/19 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove comand logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE request_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.REQUEST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'REQUEST';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;
    l_appl_srvr          VARCHAR2(20);
    l_os_result          typ.t_maxvc2;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div      VARCHAR2(2),
      actn_cd  VARCHAR2(3),
      cmd      typ.t_maxvc2
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.cmd := RTRIM(SUBSTR(i_msg_data, 54));
      RETURN(l_r_parsed);
    END parse_msg_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_request);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_request,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_r_msg.cmd);
        logs.dbg('Process Msg');
        l_os_result := oscmd_fn(l_r_msg.cmd, l_appl_srvr);
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_os_result);
        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_request,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_request,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END request_sp;

  /*
  ||----------------------------------------------------------------------------
  || ITMFCST_SP
  ||  Item Forecast Maintenance
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/25/15 | rhalpai | Original - created for PIR14738
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE itmfcst_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.ITMFCST_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(7)                        := 'ITMFCST';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      actn_cd     VARCHAR2(3),
      catlg_num   NUMBER,
      item_fcast  NUMBER
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 45, 3));
      l_r_parsed.catlg_num := string_to_num_fn(SUBSTR(i_msg_data, 60, 6));
      l_r_parsed.item_fcast := string_to_num_fn(SUBSTR(i_msg_data, 66, 6));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM div_item_fcast_op2f f
            WHERE f.div_part = i_div_part
              AND f.catlg_num = i_r_msg.catlg_num;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO div_item_fcast_op2f f
           USING (SELECT 1 AS tst
                    FROM DUAL) x
              ON (    f.div_part = i_div_part
                  AND f.catlg_num = i_r_msg.catlg_num
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET item_fcast = i_r_msg.item_fcast
        WHEN NOT MATCHED THEN
          INSERT(div_part, catlg_num, item_fcast)
          VALUES(i_div_part, i_r_msg.catlg_num, i_r_msg.item_fcast);
    END merge_sp;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_itmfcst);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_itmfcst,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd = g_c_trg THEN
            logs.dbg('Execute Forecast');
            op_forecast_pk.fcast_sp(i_div);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_itmfcst,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL
  EXCEPTION
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_itmfcst,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END itmfcst_sp;

  /*
  ||----------------------------------------------------------------------------
  || QOPMSGS_SP
  ||  Reads the generic QOPMSGS queue on MCLANE_MQ_GET and calls the appropriate
  ||  script for each message.  Since the messages are ordered by MQ_GET_ID they
  ||  are processed in the same order they are received.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/06/03 | rhalpai | Original
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  || 07/13/05 | rhalpai | Added call for QMANFRP msgs. PIR2652
  ||                    | Added calls for QHAZMAT, QSHLFTG msgs. PIR2440
  || 04/21/05 | rhalpai | Added call for QBNDL01 msgs. PIR2545
  || 09/14/06 | rhalpai | Added call for QSKIPLD msgs. PIR3937
  || 11/13/06 | rhalpai | Added call for QCTC501 msgs. PIR3209
  || 11/16/06 | rhalpai | Added call for QCTCCUS msgs. PIR3209
  || 04/10/07 | rhalpai | Added call for QSPLTORD msgs. PIR4274
  || 04/18/07 | rhalpai | Removed status parm, added process control, changed
  ||                    | error handler to use standard parm list.
  || 12/06/07 | rhalpai | Changed call to QHAZMAT_SP to replace parm MqMsgData
  ||                    | with DivId and to remove output status parm. PIR5132
  || 12/07/07 | rhalpai | Added calls for HOLDY, VNDR, VNDHOLDY, STRCTITM,
  ||                    | STRCTRS, STRCTD msgs. PIR5002
  || 12/12/07 | rhalpai | Added call for SPLITORD msgs. PIR5341
  || 04/03/08 | rhalpai | Added call for QCORPCD msgs. PIR5882
  || 07/04/08 | rhalpai | Added call for VNDRTS msgs. PIR5002
  || 10/27/09 | rhalpai | Add call for QTYAUDIT msgs. PIR8100
  || 10/28/09 | rhalpai | Add call for ITMRNDUP msgs. PIR4342
  || 01/22/10 | rhalpai | Add calls for VNDRCMPC and VNDRCMPI msgs. PIR8216
  || 03/03/10 | rhalpai | Add call for VNDRCMPQ msgs. PIR8099
  || 04/23/10 | rhalpai | Add call for REROUTE msgs. PIR7415
  || 07/15/10 | rhalpai | Add call for VNDRCMPP msgs. PIR8936
  || 12/09/10 | rhalpai | Add call for EDICANCL msgs. PIR9562
  || 02/04/11 | rhalpai | Removed call for REROUTE msgs. IM-004248
  || 10/25/11 | rhalpai | Add logic to process via event. PIR10475
  || 01/25/12 | rhalpai | Add call for ITEMGRP msgs. PIR10620
  || 01/19/16 | rhalpai | Remove logic to call ITMRNDUP_SP for ITMRNDUP msgid. PIR15101
  || 10/14/17 | rhalpai | Replace event logic with call to ADD_EVNT_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE qopmsgs_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.QOPMSGS_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_status             VARCHAR2(500);
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;
    l_msg_div_id         div_mstr_di1d.div_id%TYPE;
    l_msg_div_part       NUMBER;
    l_msg_id             mclane_mq_get.mq_msg_id%TYPE;
    l_msg_id_list        typ.t_maxvc2;
    l_t_prbs             g_tt_prbs;

    PROCEDURE add_problem_sp(
      i_div     IN  VARCHAR2,
      i_msg_id  IN  VARCHAR2,
      i_prb     IN  VARCHAR2
    ) IS
      l_idx                     VARCHAR2(510);
      l_t_rpt_lns               typ.tas_maxvc2;
      l_c_file_dir     CONSTANT VARCHAR2(50)   := '/oplogs/interfaces';
      l_file_nm                 VARCHAR2(50);
      l_c_mq_err_log   CONSTANT VARCHAR2(50)   := i_div || '_mqerrors_mclane.log';
      l_c_inv_err_log  CONSTANT VARCHAR2(50)   := i_div || '_inventory_sync_problem.log';
    BEGIN
      l_idx := RPAD(i_div, 2) || RPAD(i_msg_id, 8) || i_prb;
      logs.dbg('Update Problem Table');

      IF l_t_prbs.EXISTS(l_idx) THEN
        l_t_prbs(l_idx) := l_t_prbs(l_idx) + 1;
      ELSE
        l_t_prbs(l_idx) := 1;
      END IF;   -- l_t_prbs.EXISTS(v_idx)

      logs.dbg('Set File Name');
      l_file_nm :=(CASE
                     WHEN i_msg_id LIKE 'QITEM%' THEN l_c_inv_err_log
                     ELSE l_c_mq_err_log
                   END);
      logs.dbg('Add to Rpt Table');
      util.append(l_t_rpt_lns, TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' ' || i_msg_id || ' ' || i_prb);
      util.append(l_t_rpt_lns, '');
      logs.dbg('Write Error Log');
      write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir, 'A');
    END add_problem_sp;

    PROCEDURE notify_sp IS
      l_c_init_val    CONSTANT VARCHAR2(1)    := '?';
      l_c_process_id  CONSTANT VARCHAR2(20)   := 'OP_MESSAGES_PK';
      l_c_mail_subj   CONSTANT VARCHAR2(50)   := 'Errors From OP MQ Interface - OP_MESSAGES_PK';
      l_mail_msg               VARCHAR2(4000);
      l_idx                    VARCHAR2(510);
      l_div                    VARCHAR2(2)    := l_c_init_val;
      l_div_save               VARCHAR2(2);
      l_msg_id                 VARCHAR2(10);
      l_msg_id_save            VARCHAR2(10)   := l_c_init_val;
      l_prb                    VARCHAR2(500);
      l_prb_cnt                PLS_INTEGER;
    BEGIN
      IF l_t_prbs.COUNT > 0 THEN
        l_idx := l_t_prbs.FIRST;
        LOOP
          EXIT WHEN NOT l_t_prbs.EXISTS(l_idx);
          l_div := LTRIM(SUBSTR(l_idx, 1, 2));
          l_msg_id := LTRIM(SUBSTR(l_idx, 3, 8));
          l_prb := LTRIM(SUBSTR(l_idx, 11));
          l_prb_cnt := l_t_prbs(l_idx);

          IF l_div <> l_div_save THEN
            IF l_div_save <> l_c_init_val THEN
              op_process_common_pk.notify_group_sp(l_div, l_c_process_id, l_c_mail_subj, l_mail_msg);
            END IF;   -- l_div_save <> l_c_init_val

            l_div_save := l_div;
          END IF;   -- l_div <> l_div_save

          IF l_msg_id <> l_msg_id_save THEN
            l_msg_id_save := l_msg_id;
            l_mail_msg := '\n' || l_mail_msg || l_msg_id || '\n';
            l_mail_msg := l_mail_msg || RPAD('*', LENGTH(l_msg_id), '*') || '\n';
          END IF;   -- l_msg_id <> l_msg_id_save

          l_mail_msg := l_mail_msg || LPAD(l_prb_cnt, 4) || ' - ' || l_prb || '\n';
          l_idx := l_t_prbs.NEXT(l_idx);
        END LOOP;
        op_process_common_pk.notify_group_sp(l_div, l_c_process_id, l_c_mail_subj, l_mail_msg);
      END IF;   -- l_t_prbs.COUNT > 0
    END notify_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_qopmsgs);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qopmsgs,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_msg IN g_cur_msg(l_div_part, 'QOPMSGS') LOOP
        l_mq_get_id := l_r_msg.mq_get_id;
        l_msg_div_id := RTRIM(SUBSTR(l_r_msg.mq_msg_data, 1, 2));
        l_msg_div_part := div_pk.div_part_fn(l_msg_div_id);
        l_msg_id := RTRIM(SUBSTR(l_r_msg.mq_msg_data, 3, 8));
        upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Processing ' || l_msg_id);
        l_msg_id_list := l_msg_id_list
                         ||(CASE
                              WHEN l_msg_id_list IS NULL THEN l_msg_id
                              WHEN INSTR(l_msg_id_list, l_msg_id) = 0 THEN ',' || l_msg_id
                            END
                           );

        BEGIN
          l_status := g_c_good;

          CASE l_msg_id
            WHEN 'QPINV01' THEN
              qpinv01_sp(l_msg_div_part, l_r_msg.mq_msg_data, l_status);
            WHEN 'QMANFRP' THEN
              qmanfrp_sp(l_msg_div_part, l_r_msg.mq_msg_data, l_status);
            WHEN 'QHAZMAT' THEN
              qhazmat_sp(l_msg_div_part);
            WHEN 'QBNDL01' THEN
              qbndl01_sp(l_msg_div_part, l_r_msg.mq_msg_data, l_status);
            WHEN 'QSKIPLD' THEN
              qskipld_sp(l_msg_div_part, l_status);
            WHEN 'QCTC501' THEN
              qctc501_sp(l_msg_div_part, l_r_msg.mq_msg_data, l_status);
            WHEN 'QCTCCUS' THEN
              qctccus_sp(l_msg_div_part, l_r_msg.mq_msg_data, l_status);
            WHEN 'QSPLTORD' THEN
              qspltord_sp(l_msg_div_part, l_status);
            WHEN 'VNDR' THEN
              vndr_sp(l_msg_div_part, l_r_msg.mq_msg_data);
            WHEN 'VNDRTS' THEN
              vndrts_sp(l_msg_div_part, l_r_msg.mq_msg_data);
            WHEN 'STRCTITM' THEN
              strctitm_sp(l_msg_div_part, l_r_msg.mq_msg_data);
            WHEN 'STRCTRS' THEN
              strctrs_sp(l_msg_div_part);
            WHEN 'STRCTD' THEN
              strctd_sp(l_msg_div_part);
            WHEN 'SPLITORD' THEN
              splitord_sp(l_msg_div_part);
            WHEN 'QCORPCD' THEN
              qcorpcd_sp(l_msg_div_part, l_r_msg.mq_msg_data);
            WHEN 'QTYAUDIT' THEN
              qtyaudit_sp(l_msg_div_part);
            WHEN 'VNDRCMPC' THEN
              vndrcmpc_sp(l_msg_div_part);
            WHEN 'VNDRCMPI' THEN
              vndrcmpi_sp(l_msg_div_part);
            WHEN 'VNDRCMPQ' THEN
              vndrcmpq_sp(l_msg_div_part);
            WHEN 'VNDRCMPP' THEN
              vndrcmpp_sp(l_msg_div_part, l_r_msg.mq_msg_data);
            WHEN 'EDICANCL' THEN
              edicancl_sp(l_msg_div_part, l_r_msg.mq_msg_data);
            WHEN 'ITEMGRP' THEN
              itemgrp_sp(l_msg_div_part);
            ELSE
              NULL;
          END CASE;
        EXCEPTION
          WHEN OTHERS THEN
            l_status := SUBSTR(l_c_module || ' ' || l_msg_id || ' Unhandled Error: ' || SQLERRM, 1, 500);
        END;

        logs.dbg('Check Return Status');

        IF l_status = g_c_good THEN
          l_mq_msg_stat := g_c_compl;
        ELSE
          l_mq_msg_stat := g_c_prb;
          add_problem_sp(l_msg_div_id, l_msg_id, l_status);
        END IF;   -- l_status = g_c_good

        logs.dbg('Update MQ Message Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;

      IF l_t_prbs.COUNT > 0 THEN
        logs.dbg('Notify');
        notify_sp;
      END IF;   -- l_t_prbs.COUNT > 0

      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qopmsgs,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'QOPMSGS Complete for: ' || l_msg_id_list, 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_qopmsgs,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END qopmsgs_sp;

  /*
  ||----------------------------------------------------------------------------
  || ORDCUT_SP
  ||  Interface for Order Cuts for Volume Control
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/21 | rhalpai | Original for PIR21276
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ordcut_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.ORDCUT_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                        := 'ORDCUT';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      llr_dt         DATE,
      excl_tob_sw    VARCHAR2(1),
      excl_logo_sw   VARCHAR2(1),
      mfst_max_list  CLOB
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
      l_t_parms   lob_rows_t;
    BEGIN
      l_t_parms := lob2table.separatedcolumns2(TO_CLOB(SUBSTR(i_msg_data, 54)), op_const_pk.grp_delimiter);
      l_r_parsed.llr_dt := TO_DATE(l_t_parms(1).column1, 'YYYY-MM-DD');
      l_r_parsed.excl_tob_sw := l_t_parms(2).column1;
      l_r_parsed.excl_logo_sw := l_t_parms(3).column1;
      l_r_parsed.mfst_max_list := TO_CLOB(l_t_parms(4).column1);
      RETURN(l_r_parsed);
    END parse_msg_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_ordcut);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ordcut,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        BEGIN
          logs.dbg('Process Msg');
          op_mass_maint_pk.ord_cut_sp(i_div,
                                      l_r_msg.llr_dt,
                                      l_r_msg.excl_tob_sw,
                                      l_r_msg.excl_logo_sw,
                                      l_r_msg.mfst_max_list,
                                      l_c_module,
                                      i_user_id
                                     );
        EXCEPTION
          WHEN OTHERS THEN
            logs.err(lar_parm, 'MQGetID: ' || l_mq_get_id, FALSE);
            l_mq_msg_stat := g_c_prb;
            ROLLBACK;
        END;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ordcut,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_ordcut,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END ordcut_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCTQTY_SP
  ||  Update Order Line Qty to Enforce Strict PO Qty
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/02/24 | rhalpai | Original - Created for PC-9546
  || 03/10/25 | rhalpai | Change logic to handle case where customer orders contain multiple lines for same item but
  ||                    | vendor sends back only one line with summarized PO qty. In this case mainframe will send up
  ||                    | to 5 OrdNum/OrdLn associated with item on PO and Enforce PO Qty logic will apply adjustments
  ||                    | across all order lines sent as necessary. SDHD-2187208
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strctqty_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.STRCTQTY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                        := 'STRCTQTY';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;
    l_mq_get_id          NUMBER;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div      VARCHAR2(2),
      actn_cd  VARCHAR2(3),
      ord_num1  NUMBER,
      ord_ln1   NUMBER,
      po_qty    NUMBER,
      ord_num2  NUMBER,
      ord_ln2   NUMBER,
      ord_num3  NUMBER,
      ord_ln3   NUMBER,
      ord_num4  NUMBER,
      ord_ln4   NUMBER,
      ord_num5  NUMBER,
      ord_ln5   NUMBER
    );

    l_r_msg              l_rt_msg;
    l_t_ord_num          type_ntab;
    l_t_ord_ln           type_ntab;
    l_t_po_qty           type_ntab;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.ord_num1 := string_to_num_fn(SUBSTR(i_msg_data, 54, 11));
      l_r_parsed.ord_ln1 := string_to_num_fn(SUBSTR(i_msg_data, 65, 7)) * .01;
      l_r_parsed.po_qty := string_to_num_fn(SUBSTR(i_msg_data, 72, 5));
      l_r_parsed.ord_num2 := string_to_num_fn(SUBSTR(i_msg_data, 77, 11));
      l_r_parsed.ord_ln2 := string_to_num_fn(SUBSTR(i_msg_data, 88, 7)) * .01;
      l_r_parsed.ord_num3 := string_to_num_fn(SUBSTR(i_msg_data, 95, 11));
      l_r_parsed.ord_ln3 := string_to_num_fn(SUBSTR(i_msg_data, 106, 7)) * .01;
      l_r_parsed.ord_num4 := string_to_num_fn(SUBSTR(i_msg_data, 113, 11));
      l_r_parsed.ord_ln4 := string_to_num_fn(SUBSTR(i_msg_data, 124, 7)) * .01;
      l_r_parsed.ord_num5 := string_to_num_fn(SUBSTR(i_msg_data, 131, 11));
      l_r_parsed.ord_ln5 := string_to_num_fn(SUBSTR(i_msg_data, 142, 7)) * .01;
      RETURN(l_r_parsed);
    END parse_msg_fn;

    FUNCTION msg_string_fn(
      i_r_msg  IN  l_rt_msg
    )
      RETURN VARCHAR2 IS
      l_msg_str  typ.t_maxvc2;
    BEGIN
      l_msg_str := 'GetID:'
                   || l_mq_get_id
                   || ' ActnCd:'
                   || i_r_msg.actn_cd
                   || ' Div:'
                   || i_r_msg.div
                   || ' OrdNum1:'
                   || i_r_msg.ord_num1
                   || ' OrdLn1:'
                   || i_r_msg.ord_ln1
                   || ' PoQty:'
                   || i_r_msg.po_qty
                   || ' OrdNum2:'
                   || i_r_msg.ord_num2
                   || ' OrdLn2:'
                   || i_r_msg.ord_ln2
                   || ' OrdNum3:'
                   || i_r_msg.ord_num3
                   || ' OrdLn3:'
                   || i_r_msg.ord_ln3
                   || ' OrdNum4:'
                   || i_r_msg.ord_num4
                   || ' OrdLn4:'
                   || i_r_msg.ord_ln4
                   || ' OrdNum5:'
                   || i_r_msg.ord_num5
                   || ' OrdLn5:'
                   || i_r_msg.ord_ln5;
      RETURN(l_msg_str);
    END msg_string_fn;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_strctqty);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctqty,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        l_mq_get_id := l_r_mq_msg.mq_get_id;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Get PO Qty Changes');

        WITH o AS(
          SELECT s.ord_num, s.ord_ln, b.ordqtb AS ord_qty,
                 SUM(b.ordqtb) OVER(ORDER BY b.ordqtb, b.ordnob, b.lineb RANGE UNBOUNDED PRECEDING) AS run_ttl_ord_qty,
                 SUM(b.ordqtb) OVER(ORDER BY b.ordqtb, b.ordnob, b.lineb RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ttl_ord_qty
            FROM strct_ord_op1o s, ordp120b b
           WHERE s.div_part = l_div_part
             AND (   (    s.ord_num = l_r_msg.ord_num1
                      AND s.ord_ln = l_r_msg.ord_ln1)
                  OR (    s.ord_num = l_r_msg.ord_num2
                      AND s.ord_ln = l_r_msg.ord_ln2)
                  OR (    s.ord_num = l_r_msg.ord_num3
                      AND s.ord_ln = l_r_msg.ord_ln3)
                  OR (    s.ord_num = l_r_msg.ord_num4
                      AND s.ord_ln = l_r_msg.ord_ln4)
                  OR (    s.ord_num = l_r_msg.ord_num5
                      AND s.ord_ln = l_r_msg.ord_ln5)
                 )
             AND b.div_part = s.div_part
             AND b.ordnob = s.ord_num
             AND b.lineb = s.ord_ln
             AND b.statb = 'O'
        ), x AS(
          SELECT o.ord_num, o.ord_ln, o.ord_qty + (l_r_msg.po_qty - ttl_ord_qty) AS po_qty
            FROM o
           WHERE o.ttl_ord_qty < l_r_msg.po_qty
             AND ROWNUM = 1
          UNION ALL
          SELECT o.ord_num, o.ord_ln, GREATEST(0, l_r_msg.po_qty -(o.run_ttl_ord_qty - o.ord_qty)) AS po_qty
            FROM o
           WHERE o.run_ttl_ord_qty > l_r_msg.po_qty
        )
        SELECT x.ord_num, x.ord_ln, x.po_qty
        BULK COLLECT INTO l_t_ord_num, l_t_ord_ln, l_t_po_qty
          FROM x;

        IF (    l_t_ord_num IS NOT NULL
            AND l_t_ord_num.COUNT > 0) THEN
          BEGIN
        logs.dbg('Enforce PO Qty');
            FOR i IN l_t_ord_num.FIRST .. l_t_ord_num.LAST LOOP
              op_strict_order_pk.enforc_po_qty_sp(i_div, l_t_ord_num(i), l_t_ord_ln(i), l_t_po_qty(i));
            END LOOP;
        EXCEPTION
          WHEN OTHERS THEN
            l_mq_msg_stat := g_c_prb;
            logs.warn('Error in call to OP_STRICT_ORDER_PK.ENFORC_PO_QTY_SP', lar_parm, msg_string_fn(l_r_msg));
              ROLLBACK;
        END;
        END IF;   -- l_t_ord_num IS NOT NULL

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctqty,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctqty,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END strctqty_sp;

  /*
  ||----------------------------------------------------------------------------
  || STRCTMCQ_SP
  ||  Interface for new STRCT_MSTR_CS_OP3I table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/01/24 | rhalpai | Original - Created for PC-9784
  || 10/24/25 | rhalpai | Remove rsdl_qty. PC-10528
  ||----------------------------------------------------------------------------
  */
  PROCEDURE strctmcq_sp(
    i_div          IN  VARCHAR2,
    i_user_id      IN  VARCHAR2 DEFAULT 'MQ',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                      := 'OP_MESSAGES_PK.STRCTMCQ_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_c_msg_id  CONSTANT VARCHAR2(8)                        := 'STRCTMCQ';
    l_mq_msg_stat        mclane_mq_get.mq_msg_status%TYPE;

    -- Data Defined
    TYPE l_rt_msg IS RECORD(
      div          VARCHAR2(2),
      actn_cd      VARCHAR2(3),
      ss_item      VARCHAR2(6),
      fc_item      VARCHAR2(6),
      cbr_vndr_id  NUMBER
    );

    l_r_msg              l_rt_msg;

    FUNCTION parse_msg_fn(
      i_msg_data  IN  VARCHAR2
    )
      RETURN l_rt_msg IS
      l_r_parsed  l_rt_msg;
    BEGIN
      l_r_parsed.div := SUBSTR(i_msg_data, 1, 2);
      l_r_parsed.actn_cd := UPPER(SUBSTR(i_msg_data, 41, 3));
      l_r_parsed.ss_item := SUBSTR(i_msg_data, 54, 6);
      l_r_parsed.fc_item := SUBSTR(i_msg_data, 60, 6);
      l_r_parsed.cbr_vndr_id := string_to_num_fn(SUBSTR(i_msg_data, 74, 10));
      RETURN(l_r_parsed);
    END parse_msg_fn;

    PROCEDURE del_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      DELETE FROM strct_mstr_cs_op3i s
            WHERE s.div_part = i_div_part
              AND s.ss_item = i_r_msg.ss_item;
    END del_sp;

    PROCEDURE merge_sp(
      i_div_part  IN  NUMBER,
      i_r_msg     IN  l_rt_msg
    ) IS
    BEGIN
      MERGE INTO strct_mstr_cs_op3i s
           USING (SELECT 1 AS tst
                    FROM DUAL) x
              ON (    s.div_part = i_div_part
                  AND s.ss_item = i_r_msg.ss_item
                  AND x.tst > 0)
        WHEN MATCHED THEN
          UPDATE
             SET fc_item = i_r_msg.fc_item, cbr_vndr_id = i_r_msg.cbr_vndr_id
        WHEN NOT MATCHED THEN
          INSERT(div_part, ss_item, fc_item, cbr_vndr_id)
          VALUES(i_div_part, i_r_msg.ss_item, i_r_msg.fc_item, i_r_msg.cbr_vndr_id);
    END merge_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Add Event');
      add_evnt_sp(i_div, i_user_id, cig_constants_events_pk.evd_mq_strctmcq);
    ELSE
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctmcq,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      FOR l_r_mq_msg IN g_cur_msg(l_div_part, l_c_msg_id) LOOP
        l_mq_msg_stat := g_c_compl;
        logs.dbg('Parse MQ Message Data');
        l_r_msg := parse_msg_fn(l_r_mq_msg.mq_msg_data);
        logs.dbg('Process Msg');

        CASE
          WHEN l_r_msg.actn_cd = g_c_del THEN
            logs.dbg('Remove Entry');
            del_sp(l_div_part, l_r_msg);
          WHEN l_r_msg.actn_cd IN(g_c_add, g_c_chg) THEN
            logs.dbg('Add/Chg Entry');
            merge_sp(l_div_part, l_r_msg);
          ELSE
            l_mq_msg_stat := g_c_prb;
        END CASE;

        logs.dbg('Update MQ Msg Status');
        upd_msg_status_sp(l_div_part, l_r_mq_msg.mq_get_id, l_mq_msg_stat);
        COMMIT;
      END LOOP;
      logs.dbg('Set Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctmcq,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, l_c_msg_id || ' Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_strctmcq,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      upd_evnt_log_sp(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, 'Unhandled Error: ' || SQLERRM, -1);
      logs.err(lar_parm);
  END strctmcq_sp;
END op_messages_pk;
/

