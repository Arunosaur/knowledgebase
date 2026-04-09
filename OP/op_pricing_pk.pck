CREATE OR REPLACE PACKAGE op_pricing_pk IS
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
  PROCEDURE retrieve_pricing_sp(
    i_mcl_cust        IN      BINARY_INTEGER,
    i_catlg_num       IN      BINARY_INTEGER,
    i_div             IN      VARCHAR2,
    i_cbr_cust_id     IN      VARCHAR2,
    i_cbr_item_num    IN      BINARY_INTEGER,
    i_uom             IN      VARCHAR2,
    i_invc_dt         IN      VARCHAR2,
    i_gmp_sw          IN      VARCHAR2,
    i_sub_sw          IN      VARCHAR2,
    i_kit_sw          IN      VARCHAR2,
    i_hard_price_sw   IN      VARCHAR2,
    io_passed_price   IN OUT  NUMBER,
    i_hard_rtl_sw     IN      VARCHAR2,
    io_rtl_amt        IN OUT  NUMBER,
    io_mult_for_rtl   IN OUT  BINARY_INTEGER,
    i_item_pass_area  IN      VARCHAR2,
    i_dist_id         IN      VARCHAR2,
    o_mfst_catg       OUT     VARCHAR2,
    o_tote_catg       OUT     VARCHAR2,
    o_lbl_catg        OUT     VARCHAR2,
    o_invc_catg       OUT     VARCHAR2,
    o_auth_cd         OUT     VARCHAR2,
    o_cust_item       OUT     VARCHAR2,
    o_not_shp_rsn     OUT     VARCHAR2,
    o_price_ts        OUT     VARCHAR2,
    o_err_sw          OUT     VARCHAR2,
    o_err_msg         OUT     VARCHAR2
  );
END op_pricing_pk;
/

CREATE OR REPLACE PACKAGE BODY op_pricing_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  -- Retrieve pricing from VSAM through CICS
  PROCEDURE call_cics_for_pricing_sp(
    mclcust   IN      BINARY_INTEGER,
    catitem   IN      BINARY_INTEGER,
    divcode   IN      VARCHAR2,
    cbrcust   IN      VARCHAR2,
    cbritem   IN      BINARY_INTEGER,
    cbriuom   IN      VARCHAR2,
    invdate   IN      VARCHAR2,
    gmp       IN      VARCHAR2,
    rplsub    IN      VARCHAR2,
    dsply     IN      VARCHAR2,
    hrdprc    IN      VARCHAR2,
    price     IN OUT  FLOAT,
    hrdrtl    IN      VARCHAR2,
    retail    IN OUT  FLOAT,
    retmul    IN OUT  BINARY_INTEGER,
    itempass  IN      VARCHAR2,
    distid    IN      VARCHAR2,
    mancat    OUT     VARCHAR2,
    totcat    OUT     VARCHAR2,
    labcat    OUT     VARCHAR2,
    invcat    OUT     VARCHAR2,
    authcd    OUT     VARCHAR2,
    cusitem   OUT     VARCHAR2,
    noshprsn  OUT     VARCHAR2,
    pricets   OUT     VARCHAR2,
    errsw     OUT     VARCHAR2,
    errmsg    OUT     VARCHAR2
  ) AS
  EXTERNAL
    LIBRARY op_item_price_retail_cics_lib
    NAME "op_item_price_retail_cics_sp"
    LANGUAGE c
    PARAMETERS(
      mclcust LONG,
      catitem LONG,
      divcode STRING,
      cbrcust STRING,
      cbritem LONG,
      cbriuom STRING,
      invdate STRING,
      gmp STRING,
      rplsub STRING,
      dsply STRING,
      hrdprc STRING,
      price FLOAT,
      hrdrtl STRING,
      retail FLOAT,
      retmul LONG,
      itempass STRING,
      distid STRING,
      mancat STRING,
      totcat STRING,
      labcat STRING,
      invcat STRING,
      authcd STRING,
      cusitem STRING,
      noshprsn STRING,
      pricets STRING,
      errsw STRING,
      errmsg STRING
    );
/*
  -----------------------------------
  -- Retrieve pricing from DB2PWLM3
  -----------------------------------
  PROCEDURE call_wlm_for_pricing_sp(
    mclcust   IN      BINARY_INTEGER,
    catitem   IN      BINARY_INTEGER,
    divcode   IN      VARCHAR2,
    cbrcust   IN      VARCHAR2,
    cbritem   IN      BINARY_INTEGER,
    cbriuom   IN      VARCHAR2,
    invdate   IN      VARCHAR2,
    gmp       IN      VARCHAR2,
    rplsub    IN      VARCHAR2,
    dsply     IN      VARCHAR2,
    hrdprc    IN      VARCHAR2,
    price     IN OUT  FLOAT,
    hrdrtl    IN      VARCHAR2,
    retail    IN OUT  FLOAT,
    retmul    IN OUT  BINARY_INTEGER,
    itempass  IN      VARCHAR2,
    distid    IN      VARCHAR2,
    mancat    OUT     VARCHAR2,
    totcat    OUT     VARCHAR2,
    labcat    OUT     VARCHAR2,
    invcat    OUT     VARCHAR2,
    authcd    OUT     VARCHAR2,
    cusitem   OUT     VARCHAR2,
    noshprsn  OUT     VARCHAR2,
    pricets   OUT     VARCHAR2,
    errsw     OUT     VARCHAR2,
    errmsg    OUT     VARCHAR2
  ) AS
  EXTERNAL
    LIBRARY op_item_price_retail_auth_lib
    NAME "op_item_price_retail_auth_sp"
    LANGUAGE c
    PARAMETERS(
      mclcust LONG,
      catitem LONG,
      divcode STRING,
      cbrcust STRING,
      cbritem LONG,
      cbriuom STRING,
      invdate STRING,
      gmp STRING,
      rplsub STRING,
      dsply STRING,
      hrdprc STRING,
      price FLOAT,
      hrdrtl STRING,
      retail FLOAT,
      retmul LONG,
      itempass STRING,
      distid STRING,
      mancat STRING,
      totcat STRING,
      labcat STRING,
      invcat STRING,
      authcd STRING,
      cusitem STRING,
      noshprsn STRING,
      pricets STRING,
      errsw STRING,
      errmsg STRING
    );
*/
--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_PRICING_SP
  ||  This procedure will call the pricing modules a specified number of times
  ||  attempting to retrieve and return pricing information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/15/02 | rhalpai | Original
  || 09/09/02 | SNAGABH | Modified to read pricing retry parameters from
  ||                      appl_sys_parm_ap1s table.
  || 10/14/02 | SNAGABH | added global definitions for parameter values. Changed
  ||                      PRICING_EMAIL_ALERT to be read from parameters table.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE retrieve_pricing_sp(
    i_mcl_cust        IN      BINARY_INTEGER,
    i_catlg_num       IN      BINARY_INTEGER,
    i_div             IN      VARCHAR2,
    i_cbr_cust_id     IN      VARCHAR2,
    i_cbr_item_num    IN      BINARY_INTEGER,
    i_uom             IN      VARCHAR2,
    i_invc_dt         IN      VARCHAR2,
    i_gmp_sw          IN      VARCHAR2,
    i_sub_sw          IN      VARCHAR2,
    i_kit_sw          IN      VARCHAR2,
    i_hard_price_sw   IN      VARCHAR2,
    io_passed_price   IN OUT  NUMBER,
    i_hard_rtl_sw     IN      VARCHAR2,
    io_rtl_amt        IN OUT  NUMBER,
    io_mult_for_rtl   IN OUT  BINARY_INTEGER,
    i_item_pass_area  IN      VARCHAR2,
    i_dist_id         IN      VARCHAR2,
    o_mfst_catg       OUT     VARCHAR2,
    o_tote_catg       OUT     VARCHAR2,
    o_lbl_catg        OUT     VARCHAR2,
    o_invc_catg       OUT     VARCHAR2,
    o_auth_cd         OUT     VARCHAR2,
    o_cust_item       OUT     VARCHAR2,
    o_not_shp_rsn     OUT     VARCHAR2,
    o_price_ts        OUT     VARCHAR2,
    o_err_sw          OUT     VARCHAR2,
    o_err_msg         OUT     VARCHAR2
  ) IS
    l_c_module          CONSTANT typ.t_maxfqnm             := 'OP_PRICING_PK.RETRIEVE_PRICING_SP';
    lar_parm                     logs.tar_parm;
    l_div_part                   NUMBER;
    l_t_parms                    op_types_pk.tt_varchars_v;
    l_retry_cnt                  PLS_INTEGER               := 0;
    l_max_retry_cnt              PLS_INTEGER               := 5;
    l_sleep_increment            PLS_INTEGER               := 60;
    l_max_sleep_time             PLS_INTEGER               := 300;
    l_sleep_time                 PLS_INTEGER               := 0;
    l_retry_sw                   CHAR(1)                   := 'Y';
    l_email_retry_cnt            PLS_INTEGER               := 0;
    l_c_severe_err      CONSTANT VARCHAR2(1)               := 'S';
    l_c_pricing_err     CONSTANT VARCHAR2(1)               := 'Y';
    l_c_mail_subj       CONSTANT VARCHAR2(50)              := 'OP_PRICING_PK.retrieve_pricing_sp Failed';
    l_c_ok_mail_msg     CONSTANT VARCHAR2(1600)
      := '
*********************************************************************************************
                                      !! A T T E N T I O N !!

  OP Pricing Process issue previously brought to your attention (via e-mail)
  was successful within the allowable number of attempts.
  Please disregard any PRIOR e-mails requesting action regarding this process.

  NOTE: Any future e-mails regarding this process will need to be handled as a
             separate occurrence of this issue with the specified action taken.

  Thanks!
*********************************************************************************************';
    l_c_alert_mail_msg  CONSTANT VARCHAR2(1600)
      := '
*********************************************************************************************
                                      !! A T T E N T I O N !!

  OP Pricing Process failed.
  Several more tries will be attempted, but please do the following immediately:

   + Execute the following job MC.PERM.JCL(STRTPROC) and check the STATUS
     field on PROCEDURE XXQRP55 for a value of "STARTED".

   + If the STATUS field does NOT contain a value of "STARTED" then
     contact the OP on-call person immediately.

  Thanks!
*********************************************************************************************';
  BEGIN
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.add_parm(lar_parm, 'CatlgNum', i_catlg_num);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cbr_cust_id);
    logs.add_parm(lar_parm, 'ItemNum', i_cbr_item_num);
    logs.add_parm(lar_parm, 'UOM', i_uom);
    logs.add_parm(lar_parm, 'InvDate', i_invc_dt);
    logs.add_parm(lar_parm, 'GmpSw', i_gmp_sw);
    logs.add_parm(lar_parm, 'SubSw', i_sub_sw);
    logs.add_parm(lar_parm, 'KitSw', i_kit_sw);
    logs.add_parm(lar_parm, 'HdPriceSw', i_hard_price_sw);
    logs.add_parm(lar_parm, 'PricePassed', io_passed_price);
    logs.add_parm(lar_parm, 'HdRtlSw', i_hard_rtl_sw);
    logs.add_parm(lar_parm, 'HdRtlAmt', io_rtl_amt);
    logs.add_parm(lar_parm, 'RtlMult', io_mult_for_rtl);
    logs.add_parm(lar_parm, 'ItemPass', i_item_pass_area);
    logs.add_parm(lar_parm, 'DistId', i_dist_id);
    l_div_part := div_pk.div_part_fn(i_div);
    /*
    ||-------------------------------------------------------------------------
    || Logic to loop specified number of times after sleeping for specified
    || number of seconds times the retry count, each time the procedure failed.
    || Generally, this procedure fails because the stored proc on the mainframe
    || is down, which usually comes up in few seconds. After two trys, email
    || will be sent to Help Desk with instruction to correct the problem. If
    || the process is successful after additional re-trys, another email will
    || be sent to the Help Desk notifying the same. If the process is not
    || successful in the allowed number of re-trys, this procedure will set
    || error status and terminate.
    ||-------------------------------------------------------------------------
    */
    logs.dbg('Call-CICS-For-Pricing');
    LOOP
      call_cics_for_pricing_sp(i_mcl_cust,
                               i_catlg_num,
                               i_div,
                               i_cbr_cust_id,
                               i_cbr_item_num,
                               i_uom,
                               i_invc_dt,
                               i_gmp_sw,
                               i_sub_sw,
                               i_kit_sw,
                               i_hard_price_sw,
                               io_passed_price,
                               i_hard_rtl_sw,
                               io_rtl_amt,
                               io_mult_for_rtl,
                               i_item_pass_area,
                               i_dist_id,
                               o_mfst_catg,
                               o_tote_catg,
                               o_lbl_catg,
                               o_invc_catg,
                               o_auth_cd,
                               o_cust_item,
                               o_not_shp_rsn,
                               o_price_ts,
                               o_err_sw,
                               o_err_msg
                              );

      -- If this is the first attempt to get pricing and it failed,
      -- get pricing retry information from the database
      IF l_retry_cnt = 0 THEN
        l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                             op_const_pk.prm_pricing_retry_max
                                             || ','
                                             || op_const_pk.prm_pricing_sleep_incr
                                             || ','
                                             || op_const_pk.prm_pricing_sleep_max
                                             || ','
                                             || op_const_pk.prm_pricing_email_alert
                                            );
        l_max_retry_cnt := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_pricing_retry_max)), 5);
        l_sleep_increment := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_pricing_sleep_incr)), 60);
        l_max_sleep_time := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_pricing_sleep_max)), 300);
        l_email_retry_cnt := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_pricing_email_alert)), 0);
      ELSE
        -----------------------------------------------------------------------
        -- For all retrys, check the retry flag.
        -- If retry flag has been changed to 'N' terminate immediately (useful
        -- when we determine that the call to Mainframe is gonna fail and want
        -- to terminate this process immediately).
        -----------------------------------------------------------------------
        l_retry_sw := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_pricing_retry_sw), 'Y');
      END IF;

      EXIT WHEN(   o_err_sw <> l_c_severe_err
                OR l_retry_cnt >= l_max_retry_cnt
                OR l_retry_sw = 'N');

      IF l_retry_cnt = l_email_retry_cnt THEN
        logs.dbg('Send-Alert-Email-To-Help-Desk');
        -- Send alert email to help desk
        op_process_common_pk.notify_group_sp(i_div,
                                             'OP_PRICING_PK',
                                             l_c_mail_subj,
                                             'ERROR MESSAGE: '
                                             || o_err_msg
                                             || cnst.newline_char
                                             || 'ERROR_CODE: '
                                             || o_err_sw
                                             || cnst.newline_char
                                             || l_c_alert_mail_msg
                                            );
      END IF;

      -- Increment retry counter
      l_retry_cnt := l_retry_cnt + 1;
      -- Sleep for (Initial Sleep Interval x retry count) seconds or
      -- l_max_sleep_time seconds, which ever is smaller
      l_sleep_time := l_sleep_increment * l_retry_cnt;

      IF l_sleep_time > l_max_sleep_time THEN
        l_sleep_time := l_max_sleep_time;
      END IF;

      DBMS_LOCK.sleep(l_sleep_time);
    END LOOP;

    -- Check if process was successful after email was sent to help desk
    IF (    l_retry_cnt > l_email_retry_cnt
        AND o_err_sw <> l_c_severe_err) THEN
      logs.dbg('Send-OK-Email-To-Help-Desk');
      -- Process failed initially and email was sent to help desk
      -- Process was successful after re-trying
      -- Send OK email to help desk
      op_process_common_pk.notify_group_sp(i_div, 'OP_PRICING_PK', l_c_mail_subj, l_c_ok_mail_msg);
    END IF;

    CASE o_err_sw
      WHEN l_c_severe_err THEN
        excp.throw(-20999, 'OP_PRICING_PK.CALL_CICS_FOR_PRICING_SP returned Severe Error Code');
      WHEN l_c_pricing_err THEN
        logs.warn('Pricing error',
                  lar_parm,
                  'ErrorSw: ' || o_err_sw || ' ErrorMsg: ' || o_err_msg
                 );
      ELSE
        NULL;
    END CASE;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm, 'ErrorSw: ' || o_err_sw || ' ErrorMsg: ' || o_err_msg);
  END retrieve_pricing_sp;
END op_pricing_pk;
/

