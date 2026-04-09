CREATE OR REPLACE PACKAGE op_customer_pk IS
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
  FUNCTION formatted_phone_fn(
    i_phone  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION cust_info_fn(
    i_div       IN  VARCHAR2,
    i_cust_id   IN  VARCHAR2,
    i_addr_typ  IN  VARCHAR2 DEFAULT 'BILLTO'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION search_fn(
    i_div        IN  VARCHAR2,
    i_nm         IN  VARCHAR2,
    i_addr       IN  VARCHAR2,
    i_city       IN  VARCHAR2,
    i_state      IN  VARCHAR2,
    i_zip_cd     IN  VARCHAR2,
    i_phone      IN  VARCHAR2,
    i_cntct      IN  VARCHAR2,
    i_crp_cd     IN  INTEGER,
    i_addr_typ   IN  VARCHAR2 DEFAULT 'BILLTO',
    i_cust_stat  IN  VARCHAR2 DEFAULT NULL,
    i_cust_typ   IN  VARCHAR2 DEFAULT 'MCL',
    i_cust_id    IN  VARCHAR2 DEFAULT NULL,
    i_store_num  IN  VARCHAR2 DEFAULT NULL,
    i_grp        IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION corp_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION group_list_fn(
    i_div     IN  VARCHAR2,
    i_crp_cd  IN  INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION is_cntnr_trckng_fn(
    i_div       IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN VARCHAR2;

  FUNCTION cust_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE get_load_stop_sp(
    i_div      IN      VARCHAR2,
    i_cust_id  IN      VARCHAR2,
    o_cur      OUT     SYS_REFCURSOR
  );

  PROCEDURE dist_frst_day_load_list_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  );

  PROCEDURE dist_frst_day_list_sp(
    i_div           IN      VARCHAR2,
    o_cur_cust      OUT     SYS_REFCURSOR,
    o_cur_dist_pct  OUT     SYS_REFCURSOR,
    i_load_num      IN      VARCHAR2 DEFAULT 'ALL'
  );

  PROCEDURE upd_dist_frst_day_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2
  );
END op_customer_pk;
/

CREATE OR REPLACE PACKAGE BODY op_customer_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || DIST_PCT_BY_DOW_FN
  ||  Return distribution percentage by day of week.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/11 | rhalpai | Original - created for PIR9030
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION dist_pct_by_dow_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module      CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.DIST_PCT_BY_DOW_FN';
    lar_parm                 logs.tar_parm;
    l_div_part               NUMBER;
    l_max_ship_dt            NUMBER;
    l_t_dist_frst_day_corps  type_stab;
    l_cv                     SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_max_ship_dt := TRUNC(SYSDATE + 30) - DATE '1900-02-28';
    l_t_dist_frst_day_corps := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_dist_frst_day);

    OPEN l_cv
     FOR
       SELECT   dy.eta_day,
                DECODE
                   (z.eta_day,
                    NULL, 0,
                    ROUND(z.cnt
                          / SUM(z.cnt) OVER(ORDER BY z.eta_day RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                          * 100,
                          0
                         )
                   ) AS pct
           FROM (SELECT     ROWNUM AS seq,
                            DECODE(ROWNUM,
                                   1, 'MON',
                                   2, 'TUE',
                                   3, 'WED',
                                   4, 'THU',
                                   5, 'FRI',
                                   6, 'SAT',
                                   7, 'SUN'
                                  ) AS eta_day
                       FROM DUAL
                 CONNECT BY ROWNUM <= 7) dy,
                (SELECT   y.eta_day, SUM(y.cnt) AS cnt
                     FROM (SELECT   md.dayrcd AS eta_day, COUNT(*) AS cnt
                               FROM TABLE(CAST(l_t_dist_frst_day_corps AS type_stab)) t, mclp020b cx, sysp200c c,
                                    mclp040d md, ordp100a a, load_depart_op1f ld
                              WHERE cx.div_part = l_div_part
                                AND cx.corpb = TO_NUMBER(t.column_value)
                                AND c.div_part = cx.div_part
                                AND c.acnoc = cx.custb
                                AND md.div_part = cx.div_part
                                AND md.custd = cx.custb
                                AND a.div_part = cx.div_part
                                AND a.custa = cx.custb
                                AND a.excptn_sw = 'N'
                                AND a.stata = 'O'
                                AND a.dsorda = 'D'
                                AND a.shpja <= l_max_ship_dt
                                AND ld.div_part = a.div_part
                                AND ld.load_depart_sid = a.load_depart_sid
                                AND ld.load_num = md.loadd
                           GROUP BY md.dayrcd
                           UNION ALL
                           SELECT   x.eta_day, COUNT(*) AS cnt
                               FROM (SELECT   a.ordnoa,
                                              TO_CHAR
                                                   (MIN(NEXT_DAY(DECODE(c.dist_frst_day,
                                                                        NULL, DATE '1900-02-28' + a.shpja,
                                                                        NEXT_DAY(DATE '1900-02-28' + a.shpja - 1,
                                                                                 c.dist_frst_day
                                                                                )
                                                                       )
                                                                 - 1,
                                                                 md.dayrcd
                                                                )
                                                       ),
                                                    'DY'
                                                   ) AS eta_day
                                         FROM TABLE(CAST(l_t_dist_frst_day_corps AS type_stab)) t, mclp020b cx,
                                              sysp200c c, mclp040d md, ordp100a a, load_depart_op1f ld
                                        WHERE cx.div_part = l_div_part
                                          AND cx.corpb = TO_NUMBER(t.column_value)
                                          AND c.div_part = cx.div_part
                                          AND c.acnoc = cx.custb
                                          AND md.div_part = cx.div_part
                                          AND md.custd = cx.custb
                                          AND a.div_part = cx.div_part
                                          AND a.custa = cx.custb
                                          AND a.excptn_sw = 'N'
                                          AND a.stata = 'O'
                                          AND a.dsorda = 'D'
                                          AND a.shpja <= l_max_ship_dt
                                          AND ld.div_part = a.div_part
                                          AND ld.load_depart_sid = a.load_depart_sid
                                          AND ld.load_num = 'DIST'
                                          AND ld.llr_ts = DATE '1900-01-01'
                                     GROUP BY a.ordnoa) x
                           GROUP BY x.eta_day) y
                 GROUP BY y.eta_day) z
          WHERE z.eta_day(+) = dy.eta_day
       ORDER BY dy.seq;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dist_pct_by_dow_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || FORMATTED_PHONE_FN
  ||  Format phone number to (###)###-####.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/06 | rhalpai | Original - created for PIR3937
  ||----------------------------------------------------------------------------
  */
  FUNCTION formatted_phone_fn(
    i_phone  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.FORMATTED_PHONE_FN';
    lar_parm             logs.tar_parm;
    l_phone              VARCHAR2(20);
    l_len                PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Phone', i_phone);
    logs.dbg('ENTRY', lar_parm);
    l_phone := TRIM(i_phone);

    IF l_phone <> 'N/A' THEN
      l_len := LENGTH(l_phone);
      logs.dbg('Format Phone');
      l_phone := (CASE
                    WHEN l_len > 10 THEN SUBSTR(l_phone, 1, l_len - 10)
                  END)
                 ||(CASE
                      WHEN l_len >= 10 THEN '('
                    END)
                 || SUBSTR(l_phone, -10, 3)
                 ||(CASE
                      WHEN l_len >= 10 THEN ')'
                    END)
                 || SUBSTR(l_phone, -7, 3)
                 || '-'
                 || SUBSTR(l_phone, -4);
    END IF;   -- l_phone <> 'N/A'

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_phone);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END formatted_phone_fn;

  /*
  ||----------------------------------------------------------------------------
  || CUST_INFO_FN
  ||  Return cursor of customer information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/06 | rhalpai | Original - created for PIR3937
  || 07/20/07 | rhalpai | Added address type parm and changed cursor to bring
  ||                    | back corresponding address info. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_info_fn(
    i_div       IN  VARCHAR2,
    i_cust_id   IN  VARCHAR2,
    i_addr_typ  IN  VARCHAR2 DEFAULT 'BILLTO'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.CUST_INFO_FN';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_cubing_of_totes_sw  VARCHAR2(1);
    l_addr_typ            VARCHAR2(6)   := NVL(i_addr_typ, 'BILLTO');
    l_cv                  SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'AddrTyp', i_addr_typ);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_cubing_of_totes_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_cubing_of_totes);

    OPEN l_cv
     FOR
       SELECT c.acnoc, c.titlec, c.namec, c.lnamec, DECODE(l_addr_typ, 'SHIPTO', c.shad1c, c.blad1c) AS addr1,
              DECODE(l_addr_typ, 'SHIPTO', c.shad2c, c.blad2c) AS addr2, NULL AS addr3,
              DECODE(l_addr_typ, 'SHIPTO', c.shpctc, c.blcitc) AS city,
              DECODE(l_addr_typ, 'SHIPTO', c.shpstc, c.blstc) AS st,
              DECODE(l_addr_typ, 'SHIPTO', c.shpzpc, c.blzpc) AS zip, NULL AS cnty,
              DECODE(l_addr_typ, 'SHIPTO', c.shpcnc, c.blcnc) AS cntry,
              op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone, op_customer_pk.formatted_phone_fn(c.cnfaxc) AS fax,
              TRIM(c.cnemac) AS email, TRIM(c.cnnamc) AS contact_nm,
              op_customer_pk.formatted_phone_fn(c.cnphnc) AS ship_to_phone,
              op_customer_pk.formatted_phone_fn(c.cnfaxc) AS ship_to_fax, TRIM(c.cnemac) AS ship_to_email,
              cx.mccusb AS mcl_cust,
              DECODE(c.statc, '1', 'Active', '2', 'Inactive', '3', 'On-Hold', c.statc) AS cust_stat,
              lpad_fn(cx.corpb, 3, '0') AS crp,
              NVL((SELECT 'Y'
                     FROM mclp100a ma
                    WHERE l_cubing_of_totes_sw = 'Y'
                      AND ma.div_part = c.div_part
                      AND ma.cstgpa = c.retgpc
                      AND ma.cntnr_trckg_sw = 'Y'),
                  'N'
                 ) AS cntnr_trckng_cust
         FROM mclp020b cx, sysp200c c
        WHERE cx.div_part = l_div_part
          AND i_cust_id IN(cx.mccusb, cx.custb)
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_info_fn;

  /*
  ||----------------------------------------------------------------------------
  || SEARCH_FN
  ||  Return cursor of customer information for multiple search criteria
  ||  starting with passed values.
  ||
  ||  Valid Address Types:
  ||  BILLTO = Bill-To Customer
  ||  SHIPTO = Ship-To Customer
  ||
  ||  Valid Customer Types:
  ||  MCL = McLane Customer
  ||  CBR = CBR Customer
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/06 | rhalpai | Original - created for PIR3937
  || 07/20/07 | rhalpai | Changed cursor to bring back address info
  ||                    | corresponding to address type parm. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION search_fn(
    i_div        IN  VARCHAR2,
    i_nm         IN  VARCHAR2,
    i_addr       IN  VARCHAR2,
    i_city       IN  VARCHAR2,
    i_state      IN  VARCHAR2,
    i_zip_cd     IN  VARCHAR2,
    i_phone      IN  VARCHAR2,
    i_cntct      IN  VARCHAR2,
    i_crp_cd     IN  INTEGER,
    i_addr_typ   IN  VARCHAR2 DEFAULT 'BILLTO',
    i_cust_stat  IN  VARCHAR2 DEFAULT NULL,
    i_cust_typ   IN  VARCHAR2 DEFAULT 'MCL',
    i_cust_id    IN  VARCHAR2 DEFAULT NULL,
    i_store_num  IN  VARCHAR2 DEFAULT NULL,
    i_grp        IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module   CONSTANT typ.t_maxfqnm          := 'OP_CUSTOMER_PK.SEARCH_FN';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_cubing_of_totes_sw  VARCHAR2(1);
    l_cv                  SYS_REFCURSOR;
    l_nm                  sysp200c.namec%TYPE;
    l_addr_typ            VARCHAR2(6);
    l_addr                sysp200c.shad1c%TYPE;
    l_city                sysp200c.shpctc%TYPE;
    l_state               sysp200c.shpstc%TYPE;
    l_zip_cd              sysp200c.shpzpc%TYPE;
    l_phone               sysp200c.cnphnc%TYPE;
    l_cntct               sysp200c.cnnamc%TYPE;
    l_cust_typ            VARCHAR2(3);
    l_cust_id             sysp200c.acnoc%TYPE;
    l_store_num           mclp020b.storeb%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'Nm', i_nm);
    logs.add_parm(lar_parm, 'Addr', i_addr);
    logs.add_parm(lar_parm, 'City', i_city);
    logs.add_parm(lar_parm, 'State', i_state);
    logs.add_parm(lar_parm, 'ZipCd', i_zip_cd);
    logs.add_parm(lar_parm, 'Phone', i_phone);
    logs.add_parm(lar_parm, 'Cntct', i_cntct);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.add_parm(lar_parm, 'AddrTyp', i_addr_typ);
    logs.add_parm(lar_parm, 'CustStat', i_cust_stat);
    logs.add_parm(lar_parm, 'CustTyp', i_cust_typ);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.add_parm(lar_parm, 'StoreNum', i_store_num);
    logs.add_parm(lar_parm, 'Grp', i_grp);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_cubing_of_totes_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_cubing_of_totes);
    l_nm := UPPER(TRIM(i_nm));
    l_addr_typ := NVL(i_addr_typ, 'BILLTO');
    l_addr := UPPER(TRIM(i_addr));
    l_city := UPPER(TRIM(i_city));
    l_state := UPPER(TRIM(i_state));
    l_zip_cd := TRIM(i_zip_cd);
    l_phone := TRIM(i_phone);
    l_cntct := UPPER(TRIM(i_cntct));
    l_cust_typ := NVL(i_cust_typ, 'MCL');
    l_cust_id := TRIM(i_cust_id);
    l_store_num := TRIM(i_store_num);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT c.acnoc, c.titlec, c.namec, c.lnamec, DECODE(l_addr_typ, 'SHIPTO', c.shad1c, c.blad1c) AS addr1,
              DECODE(l_addr_typ, 'SHIPTO', c.shad2c, c.blad2c) AS addr2, NULL AS addr3,
              DECODE(l_addr_typ, 'SHIPTO', c.shpctc, c.blcitc) AS city,
              DECODE(l_addr_typ, 'SHIPTO', c.shpstc, c.blstc) AS st,
              DECODE(l_addr_typ, 'SHIPTO', c.shpzpc, c.blzpc) AS zip, NULL AS cnty,
              DECODE(l_addr_typ, 'SHIPTO', c.shpcnc, c.blcnc) AS cntry,
              op_customer_pk.formatted_phone_fn(c.cnphnc) AS phone, op_customer_pk.formatted_phone_fn(c.cnfaxc) AS fax,
              TRIM(c.cnemac) AS email, TRIM(c.cnnamc) AS contact_nm,
              op_customer_pk.formatted_phone_fn(c.cnphnc) AS ship_to_phone,
              op_customer_pk.formatted_phone_fn(c.cnfaxc) AS ship_to_fax, TRIM(c.cnemac) AS ship_to_email,
              cx.mccusb AS mcl_cust,
              DECODE(c.statc, '1', 'Active', '2', 'Inactive', '3', 'On-Hold', c.statc) AS cust_stat,
              lpad_fn(cx.corpb, 3, '0') AS crp,
              NVL((SELECT 'Y'
                     FROM mclp100a ma
                    WHERE l_cubing_of_totes_sw = 'Y'
                      AND ma.div_part = c.div_part
                      AND ma.cstgpa = c.retgpc
                      AND ma.cntnr_trckg_sw = 'Y'),
                  'N'
                 ) AS cntnr_trckng_cust
         FROM sysp200c c, mclp020b cx
        WHERE c.div_part = l_div_part
          AND DECODE(l_cust_id, NULL, 'ALL', DECODE(l_cust_typ, 'MCL', cx.mccusb, cx.custb)) LIKE
                                                                                             NVL(l_cust_id, 'ALL')
                                                                                             || '%'
          AND DECODE(l_store_num, NULL, 'ALL', TRIM(cx.storeb)) LIKE NVL(l_store_num, 'ALL') || '%'
          AND DECODE(l_nm, NULL, 'ALL', UPPER(c.namec)) LIKE '%' || NVL(l_nm, 'ALL') || '%'
          AND DECODE(l_phone, NULL, 'ALL', c.cnphnc) LIKE NVL(l_phone, 'ALL') || '%'
          AND DECODE(l_cntct, NULL, 'ALL', UPPER(c.cnnamc)) LIKE NVL(l_cntct, 'ALL') || '%'
          AND DECODE(l_addr, NULL, 'ALL', UPPER(DECODE(l_addr_typ, 'SHIPTO', c.shad1c, c.blad1c))) LIKE
                                                                                         '%' || NVL(l_addr, 'ALL')
                                                                                         || '%'
          AND DECODE(l_city, NULL, 'ALL', UPPER(DECODE(l_addr_typ, 'SHIPTO', c.shpctc, c.blcitc))) LIKE
                                                                                                NVL(l_city, 'ALL')
                                                                                                || '%'
          AND DECODE(l_state, NULL, 'ALL', UPPER(DECODE(l_addr_typ, 'SHIPTO', c.shpstc, c.blstc))) LIKE
                                                                                               NVL(l_state, 'ALL')
                                                                                               || '%'
          AND DECODE(l_zip_cd, NULL, 'ALL', DECODE(l_addr_typ, 'SHIPTO', c.shpzpc, c.blzpc)) LIKE
                                                                                              NVL(l_zip_cd, 'ALL')
                                                                                              || '%'
          AND NVL(i_cust_stat, 'ALL') = DECODE(i_cust_stat, NULL, 'ALL', c.statc)
          AND NVL(i_grp, 'ALL') = DECODE(i_grp, NULL, 'ALL', c.retgpc)
          AND cx.div_part = c.div_part
          AND cx.custb = c.acnoc
          AND NVL(i_crp_cd, -1) = DECODE(i_crp_cd, NULL, -1, cx.corpb);

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END search_fn;

  /*
  ||----------------------------------------------------------------------------
  || CORP_LIST_FN
  ||  Return cursor of customer corp codes.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/06 | rhalpai | Original - created for PIR3937
  || 06/16/08 | rhalpai | Added sort by corp to cursor. IM419804
  ||----------------------------------------------------------------------------
  */
  FUNCTION corp_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.CORP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   LPAD(cc.corp_cd, 3, '0') AS corp_cd, cc.corp_nm
           FROM corp_cd_dm1c cc
       ORDER BY cc.corp_cd;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END corp_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GROUP_LIST_FN
  ||  Return cursor of customer group information.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/29/06 | rhalpai | Original - created for PIR3937
  || 11/27/07 | rhalpai | Changed cursor to sort on group number. PIR3593
  ||----------------------------------------------------------------------------
  */
  FUNCTION group_list_fn(
    i_div     IN  VARCHAR2,
    i_crp_cd  IN  INTEGER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.GROUP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   a.cstgpa, a.group_name
           FROM mclp100a a
          WHERE a.div_part = l_div_part
            AND (   i_crp_cd IS NULL
                 OR EXISTS(SELECT 1
                             FROM mclp020b cx, sysp200c c
                            WHERE cx.div_part = l_div_part
                              AND cx.corpb = i_crp_cd
                              AND c.div_part = cx.div_part
                              AND c.acnoc = cx.custb
                              AND c.retgpc = a.cstgpa)
                )
       ORDER BY a.cstgpa;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END group_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_CNTNR_TRCKNG_FN
  ||  Return whether customer is set up for Container Tracking.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/06 | rhalpai | Original - created for PIR3209
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_cntnr_trckng_fn(
    i_div       IN  VARCHAR2,
    i_mcl_cust  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.IS_CNTNR_TRCKNG_FN';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_cubing_of_totes_sw  VARCHAR2(1);
    l_cv                  SYS_REFCURSOR;
    l_cntnr_trckng_sw     VARCHAR2(1)   := 'N';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MclCust', i_mcl_cust);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_cubing_of_totes_sw := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_cubing_of_totes);

    IF l_cubing_of_totes_sw = 'Y' THEN
      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM mclp020b cx, sysp200c sc, mclp100a ma
          WHERE cx.div_part = l_div_part
            AND cx.mccusb = i_mcl_cust
            AND sc.div_part = cx.div_part
            AND sc.acnoc = cx.custb
            AND ma.div_part = sc.div_part
            AND ma.cstgpa = sc.retgpc
            AND ma.cntnr_trckg_sw = 'Y';

      FETCH l_cv
       INTO l_cntnr_trckng_sw;
    END IF;   -- l_cubing_of_totes_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cntnr_trckng_sw);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END is_cntnr_trckng_fn;

  /*
  ||----------------------------------------------------------------------------
  || CUST_LIST_FN
  ||  Return cursor of customers for division.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/03/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.CUST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT cx.custb AS cust_id, cx.mccusb AS mcl_cust, cx.storeb AS store_num, c.namec AS cust_nm
         FROM div_mstr_di1d d, mclp020b cx, sysp200c c
        WHERE d.div_id = i_div
          AND cx.div_part = d.div_part
          AND c.div_part = d.div_part
          AND c.acnoc = cx.custb;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LOAD_STOP_SP
  ||  Returns valid load/stop and delivery day for customer.
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/30/10 | dlbeal  | Original
  || 09/03/10 | dlbeal  | Format
  || 09/09/10 | wzrobin | added i_div
  || 09/14/10 | dlbeal  | Removed invalid loads(Test,P00P,DFLT,etc)
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_load_stop_sp(
    i_div      IN      VARCHAR2,
    i_cust_id  IN      VARCHAR2,
    o_cur      OUT     SYS_REFCURSOR
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.GET_LOAD_STOP_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CustId', i_cust_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);

    OPEN o_cur
     FOR
       SELECT md.loadd AS load_num, md.stopd AS stop_num, md.dayrcd AS dy
         FROM mclp040d md
        WHERE md.div_part = l_div_part
          AND md.custd = i_cust_id
       UNION
       SELECT ld.load_num, se.stop_num, TO_CHAR(se.eta_ts, 'DY') AS dy
         FROM load_depart_op1f ld, stop_eta_op1g se
        WHERE ld.div_part = l_div_part
          AND ld.load_num NOT IN(SELECT t.column_value
                                   FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
          AND se.div_part = ld.div_part
          AND se.load_depart_sid = ld.load_depart_sid
          AND se.cust_id = i_cust_id
          AND EXISTS(SELECT 1
                       FROM ordp100a a
                      WHERE a.div_part = se.div_part
                        AND a.load_depart_sid = se.load_depart_sid
                        AND a.custa = se.cust_id)
          AND NOT EXISTS(SELECT 1
                           FROM mclp040d d2
                          WHERE d2.div_part = ld.div_part
                            AND d2.loadd = ld.load_num
                            AND d2.stopd = se.stop_num);

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_load_stop_sp;

  /*
  ||----------------------------------------------------------------------------
  || DIST_FRST_DAY_LOAD_LIST_SP
  ||  Return cursor of Loads assigned to customer in DIST_FRST_DAY corp codes.
  ||  Load ALL will be prepended to the top of the list.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/20/15 | rhalpai | Original - created for PIR14445
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dist_frst_day_load_list_sp(
    i_div  IN      VARCHAR2,
    o_cur  OUT     SYS_REFCURSOR
  ) IS
    l_c_module      CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.DIST_FRST_DAY_LOAD_LIST_SP';
    lar_parm                 logs.tar_parm;
    l_div_part               NUMBER;
    l_t_dist_frst_day_corps  type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_dist_frst_day_corps := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_dist_frst_day);

    OPEN o_cur
     FOR
       SELECT 'ALL' AS load_num
         FROM DUAL
       UNION ALL
       SELECT x.load_num
         FROM (SELECT   md.loadd AS load_num
                   FROM TABLE(CAST(l_t_dist_frst_day_corps AS type_stab)) t, mclp020b cx, mclp040d md
                  WHERE cx.div_part = l_div_part
                    AND cx.corpb = TO_NUMBER(t.column_value)
                    AND md.div_part = cx.div_part
                    AND md.custd = cx.custb
               GROUP BY md.loadd
               ORDER BY md.loadd) x;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dist_frst_day_load_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || DIST_FRST_DAY_LIST_SP
  ||  Return cursor of customers indicating distribution first day and return a
  ||  cursor of distribution percentage by day of week.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/11 | rhalpai | Original - created for PIR9030
  || 03/20/15 | rhalpai | Changes logic to include a Load input parm for
  ||                    | filtering. Load ALL will indicate that all loads
  ||                    | should be returned. PIR14445
  ||----------------------------------------------------------------------------
  */
  PROCEDURE dist_frst_day_list_sp(
    i_div           IN      VARCHAR2,
    o_cur_cust      OUT     SYS_REFCURSOR,
    o_cur_dist_pct  OUT     SYS_REFCURSOR,
    i_load_num      IN      VARCHAR2 DEFAULT 'ALL'
  ) IS
    l_c_module      CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.DIST_FRST_DAY_LIST_SP';
    lar_parm                 logs.tar_parm;
    l_div_part               NUMBER;
    l_t_dist_frst_day_corps  type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LoadNum', i_load_num);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_dist_frst_day_corps := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_dist_frst_day);
    logs.dbg('Get Dist Pct By Day of Week');
    o_cur_dist_pct := dist_pct_by_dow_fn(i_div);
    logs.dbg('Open Cust Dist First Day Cursor');

    OPEN o_cur_cust
     FOR
       SELECT   x.cust_id, x.mcl_cust, x.cust_nm, x.city, x.st, x.load_eta_list,
                DECODE(x.dist_frst_day, NULL, 'Y', 'N') AS na,
                DECODE(x.dist_frst_day, 'MON', 'Y', DECODE(INSTR(x.load_eta_list, 'MON'), 0, NULL, 'N')) AS mon,
                DECODE(x.dist_frst_day, 'TUE', 'Y', DECODE(INSTR(x.load_eta_list, 'TUE'), 0, NULL, 'N')) AS tue,
                DECODE(x.dist_frst_day, 'WED', 'Y', DECODE(INSTR(x.load_eta_list, 'WED'), 0, NULL, 'N')) AS wed,
                DECODE(x.dist_frst_day, 'THU', 'Y', DECODE(INSTR(x.load_eta_list, 'THU'), 0, NULL, 'N')) AS thu,
                DECODE(x.dist_frst_day, 'FRI', 'Y', DECODE(INSTR(x.load_eta_list, 'FRI'), 0, NULL, 'N')) AS fri,
                DECODE(x.dist_frst_day, 'SAT', 'Y', DECODE(INSTR(x.load_eta_list, 'SAT'), 0, NULL, 'N')) AS sat,
                DECODE(x.dist_frst_day, 'SUN', 'Y', DECODE(INSTR(x.load_eta_list, 'SUN'), 0, NULL, 'N')) AS sun
           FROM (SELECT c.acnoc AS cust_id, cx.mccusb AS mcl_cust, c.namec AS cust_nm, c.shpctc AS city,
                        c.shpstc AS st,
                        to_list_fn(CURSOR(SELECT /*+ NO_QUERY_TRANSFORMATION */ md.loadd || ' ' || md.dayrcd
                                            FROM mclp040d md
                                           WHERE md.div_part = c.div_part
                                             AND md.custd = c.acnoc
                                        ORDER BY 1)
                                  ) AS load_eta_list,
                        c.dist_frst_day
                   FROM TABLE(CAST(l_t_dist_frst_day_corps AS type_stab)) t, mclp020b cx, sysp200c c
                  WHERE cx.div_part = l_div_part
                    AND cx.corpb = TO_NUMBER(t.column_value)
                    AND c.div_part = cx.div_part
                    AND c.acnoc = cx.custb
                    AND c.statc = '1'
                    AND (   i_load_num = 'ALL'
                         OR EXISTS(SELECT 1
                                     FROM mclp040d md
                                    WHERE md.div_part = cx.div_part
                                      AND md.custd = cx.custb
                                      AND md.loadd = i_load_num)
                        )) x
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END dist_frst_day_list_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_DIST_FRST_DAY_SP
  ||  Apply changes to customer distribution first day.
  ||
  ||  ParmList: CustId~DistFrstDay`CustId~DistFrstDay
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/22/11 | rhalpai | Original - created for PIR9030
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_dist_frst_day_sp(
    i_div        IN  VARCHAR2,
    i_parm_list  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CUSTOMER_PK.UPD_DIST_FRST_DAY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_grps             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Parse');
    l_t_grps := str.parse_list(i_parm_list, op_const_pk.grp_delimiter);
    logs.dbg('Upd');
    FORALL i IN l_t_grps.FIRST .. l_t_grps.LAST
      UPDATE sysp200c c
         SET c.dist_frst_day = TRIM(SUBSTR(l_t_grps(i), 10))
       WHERE c.div_part = l_div_part
         AND c.acnoc = SUBSTR(l_t_grps(i), 1, 8);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_dist_frst_day_sp;
END op_customer_pk;
/

