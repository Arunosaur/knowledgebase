CREATE OR REPLACE PACKAGE csr_catalog_pk IS
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
  FUNCTION retrieve_customer_list_fn(
    i_div           IN  VARCHAR2,
    i_cust_num_typ  IN  VARCHAR2,
    i_cust_num      IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_customer_list_fn(
    i_search_typ  IN  VARCHAR2,
    i_div         IN  VARCHAR2,
    i_nm          IN  VARCHAR2,
    i_addr        IN  VARCHAR2,
    i_city        IN  VARCHAR2,
    i_state       IN  VARCHAR2,
    i_zip         IN  VARCHAR2,
    i_phone       IN  VARCHAR2,
    i_contact     IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_shipto_info_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;
--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
END csr_catalog_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_catalog_pk IS
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
  FUNCTION retrieve_customer_list_fn(
    i_div           IN  VARCHAR2,
    i_cust_num_typ  IN  VARCHAR2,
    i_cust_num      IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm         := 'CSR_EXCEPTIONS_PK.GET_CUSTOMER_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_cust_num           sysp200c.acnoc%TYPE;
    l_cust_num_typ       VARCHAR2(10);
    l_len                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustNumTyp', i_cust_num_typ);
    logs.add_parm(lar_parm, 'CustNum', i_cust_num);
    logs.dbg('Initialize');
    l_cust_num_typ := NVL(TRIM(i_cust_num_typ), ' ');
    l_cust_num := TRIM(i_cust_num);
    l_len := LENGTH(l_cust_num);

    CASE
      -- equals CBR cust number and full account number passed
    WHEN     l_cust_num_typ = 'CBR'
         AND l_len = g_c_cbr_cust_len THEN
        logs.dbg('Equal CBR Cust and full account number');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
             FROM div_mstr_di1d d, sysp200c c, mclp020b cx
            WHERE d.div_id = i_div
              AND c.div_part = d.div_part
              AND c.acnoc = l_cust_num
              AND cx.div_part = d.div_part
              AND cx.custb = l_cust_num;
      -- CBR customer number and partial account number passed
    WHEN l_cust_num_typ = 'CBR' THEN
        logs.dbg('Equal CBR cust and partial account number');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
             FROM div_mstr_di1d d, sysp200c c, mclp020b cx
            WHERE d.div_id = i_div
              AND c.div_part = d.div_part
              AND c.acnoc LIKE l_cust_num || '%'
              AND cx.div_part = c.div_part
              AND cx.custb = c.acnoc;
      -- equal McLane cust number and full account number passed
    WHEN     l_cust_num_typ <> 'CBR'
         AND l_len = g_c_mcl_cust_len THEN
        logs.dbg('Equal Mclane Cust and full account number');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_id = i_div
              AND cx.div_part = d.div_part
              AND cx.mccusb = l_cust_num
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb;
      ELSE
        -- like McLane cust number and partial account number passed
        logs.dbg('Equal McLane Cust and partial account number');

        OPEN l_cv
         FOR
           SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                  c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_id = i_div
              AND cx.div_part = d.div_part
              AND cx.mccusb LIKE l_cust_num || '%'
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb;
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_customer_list_fn;

  /*
  ||----------------------------------------------------------------------------
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
  FUNCTION retrieve_customer_list_fn(
    i_search_typ  IN  VARCHAR2,
    i_div         IN  VARCHAR2,
    i_nm          IN  VARCHAR2,
    i_addr        IN  VARCHAR2,
    i_city        IN  VARCHAR2,
    i_state       IN  VARCHAR2,
    i_zip         IN  VARCHAR2,
    i_phone       IN  VARCHAR2,
    i_contact     IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.RETRIEVE_CUSTOMER_LIST_FN';
    lar_parm             logs.tar_parm;
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
    logs.dbg('ENTRY', lar_parm);
    logs.add_parm(lar_parm, 'SearchTyp', i_search_typ);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'Nm', i_nm);
    logs.add_parm(lar_parm, 'Addr', i_addr);
    logs.add_parm(lar_parm, 'City', i_city);
    logs.add_parm(lar_parm, 'State', i_state);
    logs.add_parm(lar_parm, 'Zip', i_zip);
    logs.add_parm(lar_parm, 'Phone', i_phone);
    logs.add_parm(lar_parm, 'Contact', i_contact);
    logs.dbg('Initialize');
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
                c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
           FROM div_mstr_di1d d, sysp200c c, mclp020b cx
          WHERE d.div_id = i_div
            AND c.div_part = d.div_part
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
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
                 OR c.cnphnc LIKE l_phone || '%');
    ELSE
      logs.dbg('Search using SHIPTO criteria');

      OPEN l_cv
       FOR
         SELECT c.acnoc, c.titlec, c.namec, c.lnamec, c.add1c, c.add2c, c.add3c, c.cityc, c.statec, c.zipc, c.contyc,
                c.cntrc, c.cnphnc, c.faxnoc, c.emailc, c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac, cx.mccusb
           FROM div_mstr_di1d d, sysp200c c, mclp020b cx
          WHERE d.div_id = i_div
            AND c.div_part = d.div_part
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
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
                 OR UPPER(c.cnnamc) LIKE l_contact || '%');
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_customer_list_fn;

  /*
  ||----------------------------------------------------------------------------
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
  FUNCTION retrieve_shipto_info_fn(
    i_div      IN  VARCHAR2,
    i_cust_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.GET_SHIPTO_INFO_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((LENGTH(i_cust_id) <> g_c_cbr_cust_len), 'Invalid CBR Account Number');
    logs.dbg('Retrieve Ship To Information using CBR Customer Number');

    OPEN l_cv
     FOR
       SELECT c.acnoc, '0000' AS shps, NULL AS titles, c.namec, NULL AS lnames, NULL AS sortas, c.shad1c, c.shad2c,
              NULL AS adrs3s, c.shpctc, c.shpstc, c.shpzpc, NULL AS contys, c.shpcnc, c.cnphnc, c.cnfaxc, c.cnemac,
              c.cnnamc, c.cnphnc, c.cnfaxc, c.cnemac
         FROM div_mstr_di1d d, sysp200c c
        WHERE d.div_id = i_div
          AND c.div_part = d.div_part
          AND c.acnoc = i_cust_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.err('Assertion Failure: ' || SQLERRM, lar_parm);
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_shipto_info_fn;
END csr_catalog_pk;
/

