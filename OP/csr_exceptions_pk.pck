CREATE OR REPLACE PACKAGE csr_exceptions_pk IS
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
  FUNCTION retrieve_corp_code_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_suspend_rsn_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_exception_tp_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_exceptions_fn(
    i_div              IN  VARCHAR2,
    i_ord_typ          IN  VARCHAR2,
    i_excptn_cd        IN  VARCHAR2,
    i_unrslvd_only_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_cd           IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION retrieve_popup_exceptions_fn(
    i_div            IN  VARCHAR2,
    i_last_popup_ts  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE update_exception_resolved_sp(
    i_div       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_rslvd_sw  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2,
    i_rslvd_dt  IN  VARCHAR2,
    i_rslvd_tm  IN  NUMBER
  );
END csr_exceptions_pk;
/

CREATE OR REPLACE PACKAGE BODY csr_exceptions_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_CORP_CODE_LIST_FN
  ||  Function to return the list of valid Corp Codes.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/01/04 | SNAGABH | Original
  || 06/16/08 | rhalpai | Added sort by corp to cursor. IM419804
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_corp_code_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.RETRIEVE_CORP_CODE_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   cx.corpb
           FROM mclp020b cx
          WHERE cx.div_part = l_div_part
       GROUP BY cx.corpb
       ORDER BY cx.corpb;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_corp_code_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_SUSPEND_RSN_LIST_FN
  ||  Function to return the list of Suspend reason codes.
  ||  Exclude reason codes 8, 9 and 10.
  ||    CANCEL-ORDER  = 8
  ||    CANCEL-LINE   = 9
  ||    SUSPEND-ORDER = 10
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/26/05 | SNAGABH | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_suspend_rsn_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.RETRIEVE_SUSPEND_RSN_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   a.desca, a.rsncda
           FROM mclp140a a
          WHERE NVL(a.rsntpa, 8) = 10
       ORDER BY a.rsncda;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_suspend_rsn_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_EXCEPTION_TP_LIST_FN
  ||  Function to return the list of exception types in CSR.
  ||  Exclude reason codes 8, 9, 10, 11 and 98.
  ||    8 ->    CANCEL-ORDER
  ||    9 ->    CANCEL-LINE
  ||   10 ->    SUSPEND-ORDER
  ||   11 ->    Line Item Quantity Changes
  ||   98 ->    Load Balance or Ship Confirm updates
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/09/04 | SNAGABH | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_exception_tp_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.RETRIEVE_EXCEPTION_TP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   a.desca, a.rsncda
           FROM mclp140a a
          WHERE NVL(a.rsntpa, 0) NOT IN(8, 9, 10, 11, 98)
       ORDER BY a.desca;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_exception_tp_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_EXCEPTIONS_FN
  ||  Function to retrieve list of exceptions matching specified criteria.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/22/04 | SNAGABH | Original.
  || 08/14/04 | SNAGABH | Remove exception filtering by CSR user as this will not
  ||                    | be allowed in the new exceptions screen.
  || 03/30/05 | unknown | Added resolved by user and resolved date to result set.
  || 03/08/06 | SNAGABH | Updated to return Order well Information
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change cursor to look up MclCust from XRef tbl. PIR11038
  || 09/09/13 | rhalpai | Change logic to only show exceptions with valid cust
  ||                    | for Corp when passed and all exceptions (including
  ||                    | invalid cust) when Corp not passed. IM-118456
  || 01/03/18 | rhalpai | Change logic to restrict to orders in open status and
  ||                    | only level 1 exceptions unless a reason code is passed.
  ||                    | SDHD-235367
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_exceptions_fn(
    i_div              IN  VARCHAR2,
    i_ord_typ          IN  VARCHAR2,
    i_excptn_cd        IN  VARCHAR2,
    i_unrslvd_only_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_cd           IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'CSR_EXCEPTIONS_PK.RETRIEVE_EXCEPTIONS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_ord_typ            ordp100a.dsorda%TYPE;
    l_excptn_cd          mclp300d.reasnd%TYPE;
    l_unrslvd_only_sw    VARCHAR2(1);
    l_crp_cd             NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'ExcptnCd', i_excptn_cd);
    logs.add_parm(lar_parm, 'UnrslvdOnlySw', i_unrslvd_only_sw);
    logs.add_parm(lar_parm, 'CrpCd', i_crp_cd);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_ord_typ := TRIM(i_ord_typ);
    l_excptn_cd := TRIM(i_excptn_cd);
    l_unrslvd_only_sw := NVL(i_unrslvd_only_sw, 'N');
    l_crp_cd := TO_NUMBER(i_crp_cd);
    logs.dbg('Open Cursor for the All Level 1 and 2 Exceptions');

    -- Only check for no customer exceptions if the user has selected to
    -- see all exceptions or no customer type exceptions ('032' exception code).

    -- Return list of exception types.
    -- Only include reason type values of 0(Exception) and 99(Reprice filter)
    IF l_crp_cd IS NULL THEN
      logs.dbg('No CrpCd', lar_parm);

      OPEN l_cv
       FOR
         SELECT md.exlvld, a.ordnoa, md.resexd, a.custa, NVL(cx.mccusb, '000000'), NVL(c.namec, '-Customer Unknown'),
                a.mntusa, a.dsorda, md.resusd, DATE '1900-02-28' + md.resdtd AS rslv_dt, a.stata,
                DECODE(a.excptn_sw, 'N', 0, 'Y', 1) AS tbl
           FROM mclp300d md, ordp100a a, sysp200c c, mclp020b cx
          WHERE md.div_part = l_div_part
            AND md.exlvld IN('1', '2')
            AND (   l_unrslvd_only_sw = 'N'
                 OR (    l_unrslvd_only_sw <> 'N'
                     AND md.resexd = 0))
            AND a.div_part = md.div_part
            AND a.ordnoa = md.ordnod
            AND a.stata = 'O'
            AND (   l_ord_typ IS NULL
                 OR l_ord_typ = a.dsorda)
            AND (   (    l_excptn_cd IS NULL
                     AND md.exlvld = '1')
                 OR (    l_excptn_cd = '032'
                     AND a.excptn_sw = 'Y'
                     AND md.ordlnd = 0
                     AND md.reasnd = '032')
                 OR l_excptn_cd = md.reasnd
                )
            AND c.div_part(+) = a.div_part
            AND c.acnoc(+) = a.custa
            AND cx.div_part(+) = a.div_part
            AND cx.custb(+) = a.custa;
    ELSE
      logs.dbg('CrpCd Passed', lar_parm);

      OPEN l_cv
       FOR
         SELECT md.exlvld, a.ordnoa, md.resexd, a.custa, NVL(cx.mccusb, '000000'), NVL(c.namec, '-Customer Unknown'),
                a.mntusa, a.dsorda, md.resusd, DATE '1900-02-28' + md.resdtd AS rslv_dt, a.stata,
                DECODE(a.excptn_sw, 'N', 0, 'Y', 1) AS tbl
           FROM mclp020b cx, ordp100a a, sysp200c c, mclp300d md
          WHERE cx.div_part = l_div_part
            AND cx.corpb = l_crp_cd
            AND a.div_part = cx.div_part
            AND a.custa = cx.custb
            AND a.stata = 'O'
            AND (   l_ord_typ IS NULL
                 OR l_ord_typ = a.dsorda)
            AND c.div_part = a.div_part
            AND c.acnoc = a.custa
            AND md.div_part = a.div_part
            AND md.ordnod = a.ordnoa
            AND md.exlvld IN('1', '2')
            AND (   (    l_excptn_cd IS NULL
                     AND md.exlvld = '1')
                 OR (    l_excptn_cd = '032'
                     AND a.excptn_sw = 'Y'
                     AND md.ordlnd = 0
                     AND md.reasnd = '032')
                 OR l_excptn_cd = md.reasnd
                )
            AND (   l_unrslvd_only_sw = 'N'
                 OR (    l_unrslvd_only_sw <> 'N'
                     AND md.resexd = 0));
    END IF;   -- l_crp_cd IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_exceptions_fn;

  /*
  ||----------------------------------------------------------------------------
  || RETRIEVE_POPUP_EXCEPTIONS_FN
  ||  Function to retrieve list of exceptions to be displayed in the CSR
  ||  Exceptions Popup window.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/01/04 | SNAGABH | Initial Creation.
  || 02/03/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change cursor to look up MclCust from XRef tbl. PIR11038
  || 05/15/18 | rhalpai | Change cursor to return only orders in open status.
  ||                    | SDHD-304770
  ||----------------------------------------------------------------------------
  */
  FUNCTION retrieve_popup_exceptions_fn(
    i_div            IN  VARCHAR2,
    i_last_popup_ts  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.RETRIEVE_POPUP_EXCEPTIONS_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_last_popup_ts      DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LastPopupTs', i_last_popup_ts);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_last_popup_ts := TO_DATE(TRIM(i_last_popup_ts), 'YYYYMMDDHH24MISS');
    logs.dbg('Retrieve Popup screen exceptions');

    -- Only check for "no customer" exceptions if the user has selected to
    -- see all exceptions or "no customer type" exceptions ('032' exception code).
    -- Return list of exception types.
    -- Only include reason type values of 0(Exception) and 99(Reprice filter)
    OPEN l_cv
     FOR
       SELECT   md.ordnod AS ord_num, NVL(cx.mccusb, '000000') AS mcl_cust,
                NVL(c.namec, '-Customer Unknown') AS cust_nm, NVL(c.cnphnc, ' ') AS cust_phone,
                md.descd AS excptn_descr
           FROM mclp300d md, ordp100a a, sysp200c c, mclp020b cx
          WHERE md.div_part = l_div_part
            AND md.ordlnd = 0
            AND md.exlvld = 1
            AND md.resexd = '0'
            AND (   l_last_popup_ts IS NULL
                 OR md.last_chg_ts >= l_last_popup_ts)
            AND a.div_part = md.div_part
            AND a.ordnoa = md.ordnod
            AND a.stata = 'O'
            AND a.excptn_sw = 'Y'
            AND c.div_part(+) = a.div_part
            AND c.acnoc(+) = a.custa
            AND cx.div_part(+) = a.div_part
            AND cx.custb(+) = a.custa
       ORDER BY 3, 2;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END retrieve_popup_exceptions_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPDATE_EXCEPTION_RESOLVED_SP
  ||  Function to update the Exception Resolved field.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/01/04 | SNAGABH | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE update_exception_resolved_sp(
    i_div       IN  VARCHAR2,
    i_ord_num   IN  NUMBER,
    i_rslvd_sw  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2,
    i_rslvd_dt  IN  VARCHAR2,
    i_rslvd_tm  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'CSR_EXCEPTIONS_PK.UPDATE_EXCEPTION_RESOLVED_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_rslvd_dt           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'RslvdSw', i_rslvd_sw);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'RslvdDt', i_rslvd_dt);
    logs.add_parm(lar_parm, 'RslvdTm', i_rslvd_tm);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_rslvd_dt := TO_DATE(i_rslvd_dt, 'YYYY-MM-DD') - DATE '1900-02-28';
    logs.dbg('Upd Resolved Flag and Related Info in Exception Log');

    UPDATE mclp300d d
       SET d.resexd = i_rslvd_sw,
           d.resusd = i_user_id,
           d.resdtd = l_rslvd_dt,
           d.restmd = i_rslvd_tm
     WHERE d.div_part = l_div_part
       AND d.ordnod = i_ord_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END update_exception_resolved_sp;
END csr_exceptions_pk;
/

