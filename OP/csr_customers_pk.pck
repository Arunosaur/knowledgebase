CREATE OR REPLACE PACKAGE csr_customers_pk IS
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
  /**
  ||----------------------------------------------------------------------------
  || Get CBR customer number for McLane customer number.
  || #param i_div_part         DivPart
  || #param i_mcl_cust         McLane customer number
  || #return                   CBR customer number (NULL when not found)
  ||----------------------------------------------------------------------------
  **/
  FUNCTION cbr_cust_fn(
    i_div_part  IN  NUMBER,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  /**
  ||----------------------------------------------------------------------------
  || Get McLane customer number for CBR customer number.
  || #param i_div_part         DivPart
  || #param i_cbr_cust         CBR customer number
  || #return                   McLane customer number (NULL when not found)
  ||----------------------------------------------------------------------------
  **/
  FUNCTION mcl_cust_fn(
    i_div_part  IN  NUMBER,
    i_cbr_cust  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION get_corp_cd_cbr_cust_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN INTEGER;

  FUNCTION get_corp_cd_mcl_cust_fn(
    i_div       IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN INTEGER;

  FUNCTION get_cust_status_cbr_cust_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN INTEGER;

  FUNCTION get_cust_status_mcl_cust_fn(
    i_div       IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN INTEGER;

  FUNCTION get_customer_list_fn(
    i_div           IN  VARCHAR2,
    i_cust_id_typ   IN  VARCHAR2,
    i_cust_id       IN  VARCHAR2,
    i_crp_cd        IN  INTEGER,
    i_cust_stat_cd  IN  INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_customer_list_fn(
    i_search_typ    IN  VARCHAR2,
    i_div           IN  VARCHAR2,
    i_nm            IN  VARCHAR2,
    i_addr          IN  VARCHAR2,
    i_city          IN  VARCHAR2,
    i_state         IN  VARCHAR2,
    i_zip           IN  VARCHAR2,
    i_phone         IN  VARCHAR2,
    i_contact       IN  VARCHAR2,
    i_crp_cd        IN  INTEGER,
    i_cust_stat_cd  IN  INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_shipto_info_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;
--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END csr_customers_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_customers_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_cbr_cust_len  CONSTANT PLS_INTEGER := 8;
  g_c_mcl_cust_len  CONSTANT PLS_INTEGER := 6;

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || CBR_CUST_FN
  ||  Get CBR customer number for McLane customer number.
  ||
  ||  This function is called by OrderManagerDS.ResendToServerClicked (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 04/17/06 | SNAGABH | Correct Package name in error message.
  ||----------------------------------------------------------------------------
  */
  FUNCTION cbr_cust_fn(
    i_div_part  IN  NUMBER,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_CUSTOMERS_PK.CBR_CUST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_cbr_cust           mclp020b.custb%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT cx.custb
         FROM mclp020b cx
        WHERE cx.div_part = i_div_part
          AND cx.mccusb = i_mcl_cust;

    FETCH l_cv
     INTO l_cbr_cust;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cbr_cust);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cbr_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || MCL_CUST_FN
  ||  Get McLane customer number for CBR customer number.
  ||
  ||  This function is called by OrderManagerDS.saveNewOrderToDB (java).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/25/04 | rhalpai | Original
  || 04/17/06 | SNAGABH | Correct Package name in error message.
  ||----------------------------------------------------------------------------
  */
  FUNCTION mcl_cust_fn(
    i_div_part  IN  NUMBER,
    i_cbr_cust  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_CUSTOMERS_PK.MCL_CUST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_mcl_cust           mclp020b.mccusb%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'CbrCust', i_cbr_cust);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT cx.mccusb
         FROM mclp020b cx
        WHERE cx.div_part = i_div_part
          AND cx.custb = i_cbr_cust;

    FETCH l_cv
     INTO l_mcl_cust;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_mcl_cust);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mcl_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_CORP_CD_CBR_CUST_FN
  ||  Function to return the Corp Code value for given CBR Customer Number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/06/05 | SNAGABH | Original
  || 04/17/06 | SNAGABH | When corp code is not found, log as Warning instead of Error.
  ||                      This is to avoid unnecessary errors when opening Invalid Customer
  ||                      Orders.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_corp_cd_cbr_cust_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_CUSTOMERS_PK.GET_CORP_CD_CBR_CUST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_crp_cd             INTEGER       := -1;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT cx.corpb
         FROM mclp020b cx
        WHERE cx.div_part = l_div_part
          AND cx.custb = i_cust_id;

    FETCH l_cv
     INTO l_crp_cd;

    IF l_cv%NOTFOUND THEN
      logs.warn('Corp not found', lar_parm);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_crp_cd);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_corp_cd_cbr_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_CORP_CD_MCL_CUST_FN
  ||  Function to return the Corp Code value for given McLane Customer Number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/05 | SNAGABH | Original
  || 04/17/06 | SNAGABH | Correct Package name in error message.
  || 01/10/07 | rhalpai | Change to log warning instead of error when no data
  ||                    | found.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_corp_cd_mcl_cust_fn(
    i_div       IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_CUSTOMERS_PK.GET_CORP_CD_MCL_CUST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cbr_cust           mclp020b.custb%TYPE;
    l_crp_cd             INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_cbr_cust := cbr_cust_fn(l_div_part, i_mcl_cust);
    l_crp_cd := get_corp_cd_cbr_cust_fn(i_div, l_cbr_cust);

    IF l_crp_cd = -1 THEN
      logs.warn('Corp not found', lar_parm);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_crp_cd);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_corp_cd_mcl_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_CUST_STATUS_CBR_CUST_FN
  ||  Function to return the customer status code using CBR Customer number.
  ||  Status Code        Status
  ||      1              Active
  ||      2              In Active
  ||      3              On Hold
  ||     -1              Invalid Customer Number
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/05 | SNAGABH | Original
  || 04/17/06 | SNAGABH | Correct Package name in error message.
  ||                      When customer number not found, add a warning instead of error.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_cust_status_cbr_cust_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN INTEGER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_CUSTOMERS_PK.GET_CUST_STATUS_CBR_CUST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_stat_cd            INTEGER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_stat_cd := -1;

    OPEN l_cv
     FOR
       SELECT c.statc
         FROM sysp200c c
        WHERE c.div_part = l_div_part
          AND c.acnoc = i_cust_id;

    FETCH l_cv
     INTO l_stat_cd;

    IF l_cv%NOTFOUND THEN
      logs.warn('Cust not found', lar_parm);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_stat_cd);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_cust_status_cbr_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_CUST_STATUS_MCL_CUST_FN
  ||  Function to return the customer status code using McLane Customer number.
  ||  Status Code        Status
  ||      1              Active
  ||      2              In Active
  ||      3              On Hold
  ||     -1              Invalid Customer Number
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/05 | SNAGABH | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_cust_status_mcl_cust_fn(
    i_div       IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN INTEGER IS
    lar_parm    logs.tar_parm;
    l_div_part  NUMBER;
    l_cbr_cust  mclp020b.custb%TYPE;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_cbr_cust := cbr_cust_fn(l_div_part, i_mcl_cust);
    RETURN(get_cust_status_cbr_cust_fn(i_div, l_cbr_cust));
  END get_cust_status_mcl_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_CUSTOMER_LIST_FN
  ||  Function to return the list of customers that match the Account Number (McLane or CBR)
  ||  Search criteria.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/01/04 | SNAGABH | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_customer_list_fn(
    i_div           IN  VARCHAR2,
    i_cust_id_typ   IN  VARCHAR2,
    i_cust_id       IN  VARCHAR2,
    i_crp_cd        IN  INTEGER,
    i_cust_stat_cd  IN  INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_EXCEPTIONS_PK.GET_CUSTOMER_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_cust_id            sysp200c.acnoc%TYPE;
    l_cust_id_typ        VARCHAR2(10);
    l_len                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustIdTyp', i_cust_id_typ);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'CustStatCd', i_cust_stat_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_cust_id_typ := NVL(TRIM(i_cust_id_typ), ' ');
    l_cust_id := TRIM(i_cust_id);
    l_len := LENGTH(l_cust_id);

    CASE
      WHEN     l_cust_id_typ = 'CBR'
           AND l_len = g_c_cbr_cust_len THEN
        logs.dbg('Full CBR Cust');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb, c.statc
             FROM sysp200c c, mclp020b cx
            WHERE c.div_part = l_div_part
              AND c.acnoc = l_cust_id
              AND (   i_cust_stat_cd IS NULL
                   OR i_cust_stat_cd = c.statc)
              AND cx.div_part = c.div_part
              AND cx.custb = l_cust_id
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd);
      WHEN l_cust_id_typ = 'CBR' THEN
        logs.dbg('Partial CBR Cust');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb, c.statc
             FROM mclp020b cx, sysp200c c
            WHERE cx.div_part = l_div_part
              AND cx.custb LIKE l_cust_id || '%'
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd)
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb
              AND (   i_cust_stat_cd IS NULL
                   OR i_cust_stat_cd = c.statc);
      WHEN     l_cust_id_typ <> 'CBR'
           AND l_len = g_c_mcl_cust_len THEN
        logs.dbg('Full McLane Cust');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb, c.statc
             FROM mclp020b cx, sysp200c c
            WHERE cx.div_part = l_div_part
              AND cx.mccusb = l_cust_id
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd)
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb
              AND (   i_cust_stat_cd IS NULL
                   OR i_cust_stat_cd = c.statc);
      ELSE
        logs.dbg('Partial McLane Cust');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb, c.statc
             FROM mclp020b cx, sysp200c c
            WHERE cx.div_part = l_div_part
              AND cx.mccusb LIKE l_cust_id || '%'
              AND (   i_crp_cd IS NULL
                   OR cx.corpb = i_crp_cd)
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb
              AND (   i_cust_stat_cd IS NULL
                   OR i_cust_stat_cd = c.statc);
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_customer_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_CUSTOMER_LIST_FN
  ||  Function to return the list of customers that match the Account Details
  ||  Search criteria.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/04 | SNAGABH | Original
  || 11/04/04 | SNAGABH | Minor bug fixes and some performance improvement changes.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_customer_list_fn(
    i_search_typ    IN  VARCHAR2,
    i_div           IN  VARCHAR2,
    i_nm            IN  VARCHAR2,
    i_addr          IN  VARCHAR2,
    i_city          IN  VARCHAR2,
    i_state         IN  VARCHAR2,
    i_zip           IN  VARCHAR2,
    i_phone         IN  VARCHAR2,
    i_contact       IN  VARCHAR2,
    i_crp_cd        IN  INTEGER,
    i_cust_stat_cd  IN  INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.RETRIEVE_CUSTOMER_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_nm                 VARCHAR2(41);
    l_addr               VARCHAR2(41);
    l_city               VARCHAR2(41);
    l_state              VARCHAR2(3);
    l_zip                VARCHAR2(16);
    l_phone              VARCHAR2(21);
    l_contact            VARCHAR2(41);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'SearchTyp', i_search_typ);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'Nm', i_nm);
    logs.add_parm(lar_parm, 'Addr', i_addr);
    logs.add_parm(lar_parm, 'City', i_city);
    logs.add_parm(lar_parm, 'State', i_state);
    logs.add_parm(lar_parm, 'Zip', i_zip);
    logs.add_parm(lar_parm, 'Phone', i_phone);
    logs.add_parm(lar_parm, 'Contact', i_contact);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'CustStatCd', i_cust_stat_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_nm := UPPER(TRIM(i_nm));
    l_addr := UPPER(TRIM(i_addr));
    l_city := UPPER(TRIM(i_city));
    l_state := UPPER(TRIM(i_state));
    l_zip := TRIM(i_zip);
    l_phone := TRIM(i_phone);
    l_contact := UPPER(TRIM(i_contact));

    IF i_search_typ = 'BILLTO' THEN
      logs.dbg('Search using BILLTO criteria');

      OPEN l_cv
       FOR
         SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb, c.statc
           FROM sysp200c c, mclp020b cx
          WHERE c.div_part = l_div_part
            AND (   l_nm IS NULL
                 OR UPPER(c.namec) LIKE l_nm || '%')
            AND (   l_addr IS NULL
                 OR UPPER(c.add1c) LIKE l_addr || '%')
            AND (   l_city IS NULL
                 OR UPPER(c.cityc) LIKE l_city || '%')
            AND (   l_state IS NULL
                 OR c.statec LIKE l_state || '%')
            AND (   l_zip IS NULL
                 OR c.zipc LIKE l_zip || '%')
            AND (   l_phone IS NULL
                 OR c.cnphnc LIKE l_phone || '%')
            AND (   i_cust_stat_cd IS NULL
                 OR i_cust_stat_cd = c.statc)
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
            AND (   i_crp_cd IS NULL
                 OR cx.corpb = i_crp_cd);
    ELSE
      logs.dbg('Search using SHIPTO criteria');

      OPEN l_cv
       FOR
         SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
           FROM sysp200c c, mclp020b cx
          WHERE c.div_part = l_div_part
            AND (   l_nm IS NULL
                 OR UPPER(c.namec) LIKE l_nm || '%')
            AND (   l_addr IS NULL
                 OR UPPER(c.shad1c) LIKE l_addr || '%')
            AND (   l_city IS NULL
                 OR UPPER(c.shpctc) LIKE l_city || '%')
            AND (   l_state IS NULL
                 OR UPPER(c.shpstc) LIKE l_state || '%')
            AND (   l_zip IS NULL
                 OR c.shpzpc LIKE l_zip || '%')
            AND (   l_phone IS NULL
                 OR c.cnphnc LIKE l_phone || '%')
            AND (   l_contact IS NULL
                 OR UPPER(c.cnnamc) LIKE l_contact || '%')
            AND (   i_cust_stat_cd IS NULL
                 OR i_cust_stat_cd = c.statc)
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
            AND (   i_crp_cd IS NULL
                 OR cx.corpb = i_crp_cd);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_customer_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_SHIPTO_INFO_FN
  ||  Retrieve ShipTo information for customer using CBR Account Number.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/14/04 | SNAGABH | Original
  || 12/01/04 | SNAGABH | Minor bug fixes and some performance improvement changes.
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_shipto_info_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.GET_SHIPTO_INFO_FN';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_create_restricted_sw  INTEGER       := 0;
    l_cv                    SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((LENGTH(i_cust_id) = g_c_cbr_cust_len), 'Invalid CBR Account Number');
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_create_restricted_sw := csr_utilities_pk.is_action_restricted_fn(i_div,
                                                                       'RESTRICT_ORDCR8',
                                                                       mcl_cust_fn(l_div_part, i_cust_id)
                                                                      );

    OPEN l_cv
     FOR
       SELECT c.acnoc, '0000' AS shps, NULL AS titles, c.namec, NULL AS lnames, NULL AS sortas, c.shad1c, c.shad2c,
              NULL AS adrs3s, c.shpctc, c.shpstc, c.shpzpc, NULL AS contys, c.shpcnc, c.cnphnc, c.cnfaxc, c.cnemac,
              c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, l_create_restricted_sw
         FROM sysp200c c
        WHERE c.div_part = l_div_part
          AND c.acnoc = i_cust_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_shipto_info_fn;
END csr_customers_pk;
/

