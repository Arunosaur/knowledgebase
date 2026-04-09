CREATE OR REPLACE PACKAGE op_set_release_pk IS
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
  FUNCTION llr_dates_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_load_list_fn(
    i_div        IN  VARCHAR2,
    i_test_bill  IN  PLS_INTEGER,
    i_llr_dt     IN  VARCHAR2 DEFAULT NULL,
    i_strtg_id   IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_crp_cd_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_strtg_id  IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_manifest_cat_list_fn(
    i_div       IN  VARCHAR2,
    i_strtg_id  IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_strategy_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION get_last_llr_status_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE reassgn_loads_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE,
    i_user_id   IN  VARCHAR2,
    i_undo_sw   IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE undo_vndr_cmp_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  );

  PROCEDURE backout_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  );

  PROCEDURE delete_strategy_sp(
    i_div        IN  VARCHAR2,
    i_strtg_id   IN  NUMBER,
    i_commit_sw  IN  VARCHAR2 DEFAULT 'Y'
  );

  PROCEDURE save_strategy_sp(
    i_div        IN      VARCHAR2,
    i_strtg_nm   IN      VARCHAR2,
    i_load_list  IN      VARCHAR2,
    i_crp_list   IN      VARCHAR2,
    i_mfst_list  IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2,
    i_strtg_id   IN      NUMBER DEFAULT NULL
  );

  PROCEDURE validate_rlse_sp(
    i_div          IN      VARCHAR2,
    i_llr_dt       IN      VARCHAR2,
    i_load_list    IN      VARCHAR2,
    i_crp_list     IN      VARCHAR2,
    i_mfst_list    IN      VARCHAR2,
    i_test_bil_cd  IN      VARCHAR2,
    o_warn_msg     OUT     VARCHAR2,
    o_err_msg      OUT     VARCHAR2
  );

  PROCEDURE rlse_sp(
    i_div          IN  VARCHAR2,
    i_strtg_id     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_llr_dt       IN  VARCHAR2,
    i_load_list    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_mfst_list    IN  VARCHAR2,
    i_test_bil_cd  IN  VARCHAR2,
    i_forc_inv_sw  IN  VARCHAR2 DEFAULT 'N',
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  );

  PROCEDURE evnt_rlse_sp(
    i_div  IN  VARCHAR2
  );

  PROCEDURE start_rlse_sp(
    i_div          IN  VARCHAR2,
    i_strtg_id     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_llr_dt       IN  VARCHAR2,
    i_load_list    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_mfst_list    IN  VARCHAR2,
    i_test_bil_cd  IN  VARCHAR2,
    i_forc_inv_sw  IN  VARCHAR2 DEFAULT 'N'
  );
END op_set_release_pk;
/

CREATE OR REPLACE PACKAGE BODY op_set_release_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_dt_fmt       CONSTANT VARCHAR2(10) := 'YYYY-MM-DD';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || CLEAN_LIST_FN
  ||  Strip spaces and single quotes from list.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 06/18/08 | rhalpai | Added delimiter parm and removal of trailing delimiter
  ||                    | from list. PIR6019
  ||----------------------------------------------------------------------------
  */
  FUNCTION clean_list_fn(
    i_list       IN  VARCHAR2,
    i_delimiter  IN  VARCHAR2 DEFAULT ','
  )
    RETURN VARCHAR2 IS
  BEGIN
    RETURN(RTRIM(REPLACE(REPLACE(i_list, ' '), ''''), i_delimiter));
  END clean_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ADD_LIST_TO_TEMP_SP
  ||  Load list to temp table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE add_list_to_temp_sp(
    i_typ   IN  VARCHAR2,
    i_list  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.ADD_LIST_TO_TEMP_SP';
    lar_parm             logs.tar_parm;
    l_t_vals             type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Typ', i_typ);
    logs.add_parm(lar_parm, 'List', i_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Parse');
    l_t_vals := str.parse_list(clean_list_fn(i_list));
    logs.dbg('Add Vals');

    IF l_t_vals.COUNT > 0 THEN
      FORALL i IN l_t_vals.FIRST .. l_t_vals.LAST
        INSERT INTO temp_set_release_op1t
                    (typ, val
                    )
             VALUES (i_typ, l_t_vals(i)
                    );
    END IF;   --  l_t_vals.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END add_list_to_temp_sp;

  /*
  ||----------------------------------------------------------------------------
  || POPULATE_TEMP_SP
  ||  Load lists to temp table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 05/13/13 | rhalpai | Change references to truncate_table for temp table to
  ||                    | just delete from the temp table. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE populate_temp_sp(
    i_load_list       IN  VARCHAR2,
    i_crp_list        IN  VARCHAR2 DEFAULT NULL,
    i_mfst_catg_list  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.POPULATE_TEMP_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstCatgList', i_mfst_catg_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Trunc Temp Table');

    DELETE FROM temp_set_release_op1t;

    logs.dbg('Add Loads');
    add_list_to_temp_sp('LOAD', i_load_list);

    IF i_crp_list IS NOT NULL THEN
      logs.dbg('Add Stops');
      add_list_to_temp_sp('CORP', i_crp_list);
    END IF;

    IF i_mfst_catg_list IS NOT NULL THEN
      logs.dbg('Add Manifest Categories');
      add_list_to_temp_sp('MANF', i_mfst_catg_list);
    END IF;

--    analyze_table_sp('TEMP_SET_RELEASE_OP1T');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END populate_temp_sp;

  /*
  ||----------------------------------------------------------------------------
  || CHECK_REQUIRED_PARMS_FN
  ||  Returns an error message for invalid required parms.
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 06/18/08 | rhalpai | Added StopList and MfstCatgList parms and logic to
  ||                    | validate lengths of Load/Stop/MfstCatg lists. PIR6019
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 01/08/24 | rhalpai | Add restriction for 99 loads. SDHD-1804130
  ||----------------------------------------------------------------------------
  */
  FUNCTION check_required_parms_fn(
    i_div        IN  VARCHAR2,
    i_llr_dt     IN  VARCHAR2,
    i_load_list  IN  VARCHAR2,
    i_crp_list   IN  VARCHAR2,
    i_mfst_list  IN  VARCHAR2,
    i_delimiter  IN  VARCHAR2 DEFAULT ','
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.CHECK_REQUIRED_PARMS_FN';
    lar_parm             logs.tar_parm;

    FUNCTION is_valid_div_fn
      RETURN BOOLEAN IS
      l_found_sw  VARCHAR2(1);
    BEGIN
      SELECT NVL(MAX('Y'), 'N')
        INTO l_found_sw
        FROM div_mstr_di1d
       WHERE div_id = i_div;

      RETURN(l_found_sw = 'Y');
    END is_valid_div_fn;

    FUNCTION is_date_fn
      RETURN BOOLEAN IS
      l_dt  DATE;
    BEGIN
      l_dt := TO_DATE(i_llr_dt, g_c_dt_fmt);
      RETURN(TRUE);
    EXCEPTION
      WHEN OTHERS THEN
        RETURN(FALSE);
    END is_date_fn;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'Delimiter', i_delimiter);
    logs.info('ENTRY', lar_parm);
    RETURN((CASE
              WHEN TRIM(i_div) IS NULL THEN 'Division code is required.'
              WHEN NOT is_valid_div_fn THEN 'Invalid division code ' || i_div || '.'
              WHEN TRIM(i_llr_dt) IS NULL THEN 'LLR date is required.'
              WHEN NOT is_date_fn THEN 'Invalid LLR date ' || i_llr_dt || '. Please use ' || g_c_dt_fmt || ' format.'
              WHEN REPLACE(clean_list_fn(i_load_list, i_delimiter), i_delimiter) IS NULL THEN 'Load is required.'
              WHEN LENGTH(clean_list_fn(i_load_list, i_delimiter)) > 495 THEN 'Load List is too long. Only 99 loads allowed.'
              WHEN LENGTH(clean_list_fn(i_crp_list, i_delimiter)) > 2000 THEN 'Corp List is too long. Only 2000 chars allowed.'
              WHEN LENGTH(clean_list_fn(i_mfst_list, i_delimiter)) > 2000 THEN 'Manifest List is too long. Only 2000 chars allowed.'
            END
           )
          );
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END check_required_parms_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_TO_RLSE_FN
  ||  Check for existence of valid order in release.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/05/17 | rhalpai | Original for SDHD-224907
  || 02/14/22 | rhalpai | Add check for existence of order detail in open status with NtShpRsn of NULL. SDHD-1198052
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_to_rlse_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.ORD_TO_RLSE_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;
    l_exist_sw           VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Check for valid order');

    SELECT NVL(MAX('Y'), 'N')
      INTO l_exist_sw
      FROM DUAL
     WHERE EXISTS(SELECT 1
                    FROM load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
                   WHERE ld.div_part = i_div_part
                     AND ld.llr_dt = i_llr_dt
                     AND ld.load_num IN(SELECT t.val
                                          FROM temp_set_release_op1t t
                                         WHERE t.typ = 'LOAD')
                     AND se.div_part = ld.div_part
                     AND se.load_depart_sid = ld.load_depart_sid
                     AND cx.div_part = ld.div_part
                     AND cx.custb = se.cust_id
                     AND (   NOT EXISTS(SELECT 1
                                          FROM temp_set_release_op1t t
                                         WHERE t.typ = 'CORP')
                          OR cx.corpb IN(SELECT t.val
                                           FROM temp_set_release_op1t t
                                          WHERE t.typ = 'CORP'))
                     AND a.div_part = se.div_part
                     AND a.load_depart_sid = se.load_depart_sid
                     AND a.custa = se.cust_id
                     AND a.excptn_sw = 'N'
                     AND a.stata = 'O'
                     AND EXISTS(SELECT 1
                                  FROM ordp120b b
                                 WHERE b.div_part = a.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND b.statb = 'O'
                                   AND b.ntshpb IS NULL));

    IF l_exist_sw = 'N' THEN
      logs.dbg('Set Error Message');
      l_msg := 'No valid orders exist for Allocation.';
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_to_rlse_fn;

  /*
  ||----------------------------------------------------------------------------
  || MULT_CUST_FOR_LD_STOP_FN
  ||  Returns an error message listing of Loads/Stops for the LLR date that
  ||  have orders for multiple customers assigned.
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 06/16/08 | rhalpai | Added sort by Load/Stop to cursor.
  || 08/11/08 | rhalpai | Add P to header status list in cursor. PIR6364
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 11/04/13 | rhalpai | Add validation check for multiple customers assigned
  ||                    | to same LLR/Load/Stop. IM-123463
  || 11/04/13 | rhalpai | Add logic to ensure orders exist for each Cust.
  ||                    | IM-126743
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION mult_cust_for_ld_stop_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.MULT_CUST_FOR_LD_STOP_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_mult_cust(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   /*+ ORDERED */
               x.load_num, x.stop_num
          FROM (SELECT   ld.load_depart_sid, ld.load_num, se.stop_num
                    FROM temp_set_release_op1t t, load_depart_op1f ld, stop_eta_op1g se
                   WHERE t.typ = 'LOAD'
                     AND ld.div_part = b_div_part
                     AND ld.llr_dt = b_llr_dt
                     AND ld.load_num = t.val
                     AND se.div_part = ld.div_part
                     AND se.load_depart_sid = ld.load_depart_sid
                GROUP BY ld.load_depart_sid, ld.load_num, se.stop_num
                  HAVING COUNT(DISTINCT se.cust_id) > 1) x
         WHERE EXISTS(SELECT 1
                        FROM stop_eta_op1g se, mclp020b cx, ordp100a a
                       WHERE se.div_part = b_div_part
                         AND se.load_depart_sid = x.load_depart_sid
                         AND se.stop_num = x.stop_num
                         AND cx.div_part = se.div_part
                         AND cx.custb = se.cust_id
                         AND (   NOT EXISTS(SELECT 1
                                              FROM temp_set_release_op1t t
                                             WHERE t.typ = 'CORP')
                              OR cx.corpb IN(SELECT t.val
                                               FROM temp_set_release_op1t t
                                              WHERE t.typ = 'CORP'))
                         AND a.div_part = se.div_part
                         AND a.stata IN('O', 'P', 'R')
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                      HAVING COUNT(DISTINCT a.custa) > 1)
      ORDER BY x.load_num, x.stop_num;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_mult IN l_cur_mult_cust(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg || cnst.newline_char || l_r_mult.load_num || '   ' || LPAD(l_r_mult.stop_num, 2, '0');
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Error Message');
      l_msg :=
        'The following load and stop combinations selected for release have orders belonging to multiple customers.'
        || cnst.newline_char
        || 'These combinations can''t be released...'
        || cnst.newline_char
        || 'Load Stop'
        || cnst.newline_char
        || '---- ----'
        || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mult_cust_for_ld_stop_fn;

  /*
  ||----------------------------------------------------------------------------
  || SBSCRPTN_ORD_INV_FN
  ||  Returns an error msg listing items without sufficient inventory for
  ||  subscription orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/12/13 | rhalpai | Original for PIR12239.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. Change to call new
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION sbscrptn_ord_inv_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.SBSCRPTN_ORD_INV_FN';
    lar_parm             logs.tar_parm;
    l_t_goodies_custs    type_stab;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_items(
      b_div_part         NUMBER,
      b_llr_dt           DATE,
      b_t_goodies_custs  type_stab
    ) IS
      SELECT   x.catlg_num
          FROM (SELECT   b.orditb AS catlg_num, b.itemnb AS cbr_item, b.sllumb AS uom, SUM(b.ordqtb) AS ttl_ord_qty
                    FROM TABLE(CAST(b_t_goodies_custs AS type_stab)) t, load_depart_op1f ld, stop_eta_op1g se,
                         mclp020b cx, ordp100a a, ordp120b b
                   WHERE ld.div_part = b_div_part
                     AND ld.llr_dt = b_llr_dt
                     AND ld.load_num IN(SELECT t.val
                                          FROM temp_set_release_op1t t
                                         WHERE t.typ = 'LOAD')
                     AND se.div_part = ld.div_part
                     AND se.load_depart_sid = ld.load_depart_sid
                     AND se.cust_id = t.column_value
                     AND cx.div_part = se.div_part
                     AND cx.custb = se.cust_id
                     AND (   NOT EXISTS(SELECT 1
                                          FROM temp_set_release_op1t t
                                         WHERE t.typ = 'CORP')
                          OR cx.corpb IN(SELECT t.val
                                           FROM temp_set_release_op1t t
                                          WHERE t.typ = 'CORP'))
                     AND a.div_part = se.div_part
                     AND a.load_depart_sid = se.load_depart_sid
                     AND a.custa = se.cust_id
                     AND a.excptn_sw = 'N'
                     AND a.dsorda IN('R', 'D')
                     AND a.stata IN('O', 'P')
                     AND b.div_part = a.div_part
                     AND b.ordnob = a.ordnoa
                     AND b.excptn_sw = 'N'
                     AND b.statb IN('O', 'P')
                     AND b.subrcb < 999
                     AND b.ntshpb IS NULL
                GROUP BY b.orditb, b.itemnb, b.sllumb) x
         WHERE NOT EXISTS(SELECT 1
                            FROM whsp300c w
                           WHERE w.div_part = b_div_part
                             AND w.itemc = x.cbr_item
                             AND w.uomc = x.uom
                             AND w.qavc >= x.ttl_ord_qty)
      GROUP BY x.catlg_num
      ORDER BY 1;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);
    l_t_goodies_custs := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_goodies_cus);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_item IN l_cur_items(i_div_part, i_llr_dt, l_t_goodies_custs) LOOP
      l_msg := l_msg || cnst.newline_char || l_r_item.catlg_num;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      l_msg := 'Subscription Order Failure!'
               || cnst.newline_char
               || 'Insufficient inventory for the following items:'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END sbscrptn_ord_inv_fn;

  /*
  ||----------------------------------------------------------------------------
  || SBSCRPTN_ORD_PRICE_FN
  ||  Returns an error msg listing items with price less than minimum for
  ||  subscription orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/12/13 | rhalpai | Original for PIR12239.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. Change to call new
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN, OP_PARMS_PK.VAL_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION sbscrptn_ord_price_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.SBSCRPTN_ORD_PRICE_FN';
    lar_parm             logs.tar_parm;
    l_t_goodies_custs    type_stab;
    l_min_price          NUMBER;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_items(
      b_div_part         NUMBER,
      b_llr_dt           DATE,
      b_min_price        NUMBER,
      b_t_goodies_custs  type_stab
    ) IS
      SELECT   b.orditb AS catlg_num
          FROM TABLE(CAST(b_t_goodies_custs AS type_stab)) t, mclp020b cx, load_depart_op1f ld, stop_eta_op1g se,
               ordp100a a, ordp120b b
         WHERE cx.div_part = b_div_part
           AND cx.custb = t.column_value
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'CORP')
                OR cx.corpb IN(SELECT t.val
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'CORP'))
           AND ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND ld.load_num IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'LOAD')
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND se.cust_id = cx.custb
           AND a.div_part = se.div_part
           AND a.load_depart_sid = se.load_depart_sid
           AND a.custa = se.cust_id
           AND a.excptn_sw = 'N'
           AND a.dsorda IN('R', 'D')
           AND a.stata IN('O', 'P')
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.excptn_sw = 'N'
           AND b.statb IN('O', 'P')
           AND b.subrcb < 999
           AND b.ntshpb IS NULL
           AND b.hdprcb < b_min_price
      GROUP BY b.orditb
      ORDER BY 1;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_goodies_custs := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_goodies_cus);
    l_min_price := NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_goodies_min_prc), 0);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_item IN l_cur_items(i_div_part, i_llr_dt, l_min_price, l_t_goodies_custs) LOOP
      l_msg := l_msg || cnst.newline_char || l_r_item.catlg_num;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      l_msg := 'Subscription Order Failure!'
               || cnst.newline_char
               || 'Price less than $'
               || TO_CHAR(l_min_price, 'FM0.00')
               || ' for the following items:'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END sbscrptn_ord_price_fn;

  /*
  ||----------------------------------------------------------------------------
  || TEST_CUST_ON_LIVE_RLS_FN
  ||  Check for existence of customer with Test Status on load in release.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/28/11 | rhalpai | Original. PIR10211
  || 03/02/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION test_cust_on_live_rls_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.TEST_CUST_ON_LIVE_RLS_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   ld.load_num, se.cust_id
          FROM load_depart_op1f ld, stop_eta_op1g se, sysp200c c
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND ld.load_num IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'LOAD')
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND c.div_part = se.div_part
           AND c.acnoc = se.cust_id
           AND c.statc = '4'
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.excptn_sw = 'N'
                         AND a.stata IN('O', 'P'))
      ORDER BY ld.load_num, se.cust_id;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_ord IN l_cur_ords(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg || cnst.newline_char || l_r_ord.load_num || ' ' || l_r_ord.cust_id;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Error Message');
      l_msg := 'The following load(s) selected for release have customers in test status.'
               || cnst.newline_char
               || 'These load/custs can''t be included in a live billing release:'
               || cnst.newline_char
               || 'Load CustId'
               || cnst.newline_char
               || '---- --------'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END test_cust_on_live_rls_fn;

  /*
  ||----------------------------------------------------------------------------
  || TEST_ORDS_ON_LIVE_RLS_FN
  ||  Return error message if test orders exist.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 03/02/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw.
  ||                    | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  || 10/31/19 | rhalpai | Change cursor to restrict to open orders. SDHD-587776
  ||----------------------------------------------------------------------------
  */
  FUNCTION test_ords_on_live_rls_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.TEST_ORDS_ON_LIVE_RLS_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   ld.load_num, a.ordnoa AS ord_num
          FROM load_depart_op1f ld, ordp100a a
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND ld.load_num IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'LOAD')
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND a.dsorda = 'T'
           AND a.stata = 'O'
      ORDER BY ld.load_num, a.ordnoa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_ord IN l_cur_ords(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg || cnst.newline_char || l_r_ord.load_num || ' ' || LPAD(l_r_ord.ord_num, 11);
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Error Message');
      l_msg := 'The following load(s) selected for release have test orders.'
               || cnst.newline_char
               || 'These load/orders can''t be included in a live billing release:'
               || cnst.newline_char
               || 'Load Order'
               || cnst.newline_char
               || '---- -----------'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END test_ords_on_live_rls_fn;

  /*
  ||----------------------------------------------------------------------------
  || TBILL_WITH_RELEASED_ORDS_FN
  ||  Return error message if released orders exist.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION tbill_with_released_ords_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.TBILL_WITH_RELEASED_ORDS_FN';
    lar_parm             logs.tar_parm;
    l_msg                VARCHAR2(100);
    l_cv                 SYS_REFCURSOR;
    l_exist_sw           VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open cursor');

    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM temp_set_release_op1t t, load_depart_op1f ld
        WHERE t.typ = 'LOAD'
          AND ld.div_part = i_div_part
          AND ld.llr_dt = i_llr_dt
          AND ld.load_num = t.val
          AND EXISTS(SELECT 1
                       FROM ordp100a a, ordp120b b
                      WHERE a.div_part = ld.div_part
                        AND a.load_depart_sid = ld.load_depart_sid
                        AND b.div_part = a.div_part
                        AND b.ordnob = a.ordnoa
                        AND b.statb = 'R');

    logs.dbg('Fetch cursor');

    FETCH l_cv
     INTO l_exist_sw;

    IF l_exist_sw = 'Y' THEN
      l_msg := 'Cannot test bill loads previously released for same LLR date.';
    END IF;   -- l_exist_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END tbill_with_released_ords_fn;

  /*
  ||----------------------------------------------------------------------------
  || MISSING_WAVE_PLAN_FN
  ||  Return message for loads that have not been sequenced for wave planning.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/13/09 | rhalpai | Original for PIR7118
  || 10/14/17 | rhalpai | Change to use div_part input parm. Change to call new
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN, OP_PARMS_PK.VAL_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION missing_wave_plan_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.MISSING_WAVE_PLAN_FN';
    lar_parm              logs.tar_parm;
    l_wave_plan_sw        VARCHAR2(1);
    l_t_reassgn_to_loads  type_stab;
    l_msg                 typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_wave_plan_sw := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_wave_plan);

    IF l_wave_plan_sw = 'Y' THEN
      l_t_reassgn_to_loads := op_parms_pk.vals_for_prfx_fn(i_div_part, op_const_pk.prm_reassgn_load);

      IF i_load_list IS NOT NULL THEN
        logs.dbg('Load Temp Table');
        populate_temp_sp(i_load_list);
      END IF;   -- i_load_list IS NOT NULL

      logs.dbg('Get Missing Load List');

      SELECT to_list_fn(CURSOR(SELECT   t.val
                                   FROM temp_set_release_op1t t
                                  WHERE t.typ = 'LOAD'
                               MINUS
                               SELECT   x.column_value
                                   FROM TABLE(CAST(l_t_reassgn_to_loads AS type_stab)) x
                               MINUS
                               SELECT   wpl.load_num
                                   FROM wave_plan_load_op2w wpl
                                  WHERE wpl.div_part = i_div_part
                                    AND wpl.llr_dt = i_llr_dt
                               ORDER BY 1
                              )
                       )
        INTO l_msg
        FROM DUAL;

      IF l_msg IS NOT NULL THEN
        logs.dbg('Set Message');
        l_msg := 'The following loads must be sequenced before being included in Release:' || cnst.newline_char
                 || l_msg;
      END IF;   -- l_msg IS NOT NULL
    END IF;   -- l_wave_plan_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END missing_wave_plan_fn;

  /*
  ||----------------------------------------------------------------------------
  || LAST_RLSE_FN
  ||  Return error message for last release status.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION last_rlse_fn(
    i_div_part  IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm                := 'OP_SET_RELEASE_PK.LAST_RLSE_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;
    l_cv                 SYS_REFCURSOR;
    l_stat_cd            rlse_op1z.stat_cd%TYPE;
    l_test_bil_cd        rlse_op1z.test_bil_cd%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT TRIM(UPPER(r.stat_cd)), r.test_bil_cd
         FROM rlse_op1z r
        WHERE r.div_part = i_div_part
          AND r.rlse_ts = (SELECT MAX(r2.rlse_ts)
                             FROM rlse_op1z r2
                            WHERE r2.div_part = i_div_part);

    FETCH l_cv
     INTO l_stat_cd, l_test_bil_cd;

    l_msg :=(CASE
               WHEN(    l_stat_cd = 'P'
                    AND l_test_bil_cd = '~') THEN 'Another release in progress -- Try again later.'
               WHEN(    l_stat_cd = 'P'
                    AND l_test_bil_cd <> '~') THEN 'Test Bill in progress and all loads must be closed before continuing.'
               WHEN l_stat_cd = 'E' THEN 'Previous release had an error -- Please notify the IS department.'
             END
            );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END last_rlse_fn;

  /*
  ||----------------------------------------------------------------------------
  || MASS_REPRICE_REQD_FN
  ||  Return error message when Mass Reprice is required prior to SetRelease.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/12/14 | rhalpai | Original for PIR13614
  || 10/14/17 | rhalpai | Change to use div_part input parm. Change to call new
  ||                    | OP_PARMS_PK.IDX_VALS_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION mass_reprice_reqd_fn(
    i_div_part  IN  NUMBER
  )
    RETURN VARCHAR2 IS
    l_c_module   CONSTANT typ.t_maxfqnm             := 'OP_SET_RELEASE_PK.MASS_REPRICE_REQD_FN';
    lar_parm              logs.tar_parm;
    l_t_parms             op_types_pk.tt_varchars_v;
    l_load_cnt            NUMBER;
    l_main_rlse_load_cnt  NUMBER;
    l_last_rpcmass_hrs    NUMBER;
    l_last_rpcmass_ts     DATE;
    l_msg                 typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);

    -- apply only to production
    IF SUBSTR(ora_database_name, -1) = 'P' THEN
      logs.dbg('Initialize');
      l_t_parms := op_parms_pk.idx_vals_fn(i_div_part,
                                           op_const_pk.prm_main_rlse_load_cnt
                                           || ','
                                           || op_const_pk.prm_last_rpcmass_hrs
                                           || ','
                                           || op_const_pk.prm_last_rpcmass_ts
                                          );
      l_main_rlse_load_cnt := TO_NUMBER(l_t_parms(op_const_pk.prm_main_rlse_load_cnt));
      l_last_rpcmass_hrs := TO_NUMBER(l_t_parms(op_const_pk.prm_last_rpcmass_hrs));
      l_last_rpcmass_ts := TO_DATE(l_t_parms(op_const_pk.prm_last_rpcmass_ts), 'YYYYMMDDHH24MISS');
      logs.dbg('Get Load Count');

      SELECT COUNT(*)
        INTO l_load_cnt
        FROM temp_set_release_op1t t
       WHERE t.typ = 'LOAD';

      IF l_load_cnt >= l_main_rlse_load_cnt THEN
        IF l_last_rpcmass_ts < SYSDATE -(24 / l_last_rpcmass_hrs) THEN
          l_msg := 'MASS Reprice must run within '
                   || l_last_rpcmass_hrs
                   || ' hrs prior to release.'
                   || cnst.newline_char
                   || 'Last run completed at '
                   || TO_CHAR(l_last_rpcmass_ts, 'YYYY-MM-DD HH24:MI')
                   || '.';
        END IF;   -- l_last_rpcmass_ts < SYSDATE - (24 / l_last_rpcmass_hrs)
      END IF;   -- l_load_cnt >= l_main_rlse_load_cnt
    END IF;   -- SUBSTR(ora_database_name, -1) = 'P'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mass_reprice_reqd_fn;

  /*
  ||----------------------------------------------------------------------------
  || RSTR_LOAD_RANGE_FN
  ||  Return error message when order found outside restricted load range.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/23/22 | rhalpai | Original for 00000021755
  ||----------------------------------------------------------------------------
  */
  FUNCTION rstr_load_range_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.RSTR_LOAD_RANGE_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   ld.load_num, a.ordnoa AS ord_num, prm.ord_src, prm.load_range
          FROM (SELECT SUBSTR(x.parm_id, INSTR(x.parm_id, '_', -1) + 1) AS ord_src, x.val AS load_range,
                       SUBSTR(x.val, 1, 4) AS min_load, SUBSTR(x.val, -4) AS max_load
                  FROM (SELECT DISTINCT FIRST_VALUE(p.parm_id) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS parm_id,
                                        FIRST_VALUE(p.vchar_val) OVER(PARTITION BY p.parm_id ORDER BY p.div_part DESC) AS val
                                   FROM appl_sys_parm_ap1s p
                                  WHERE p.parm_id LIKE 'LOAD_RANGE%'
                                    AND p.div_part IN(0, b_div_part)) x) prm,
               load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND ld.load_num IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'LOAD')
           AND ld.load_num NOT BETWEEN prm.min_load AND prm.max_load
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND cx.div_part = ld.div_part
           AND cx.custb = se.cust_id
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'CORP')
                OR cx.corpb IN(SELECT t.val
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'CORP'))
           AND a.div_part = se.div_part
           AND a.load_depart_sid = se.load_depart_sid
           AND a.custa = se.cust_id
           AND a.excptn_sw = 'N'
           AND a.stata IN('O', 'P', 'R')
           AND a.ipdtsa = prm.ord_src
      ORDER BY ld.load_num, prm.ord_src, a.ordnoa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_ord IN l_cur_ords(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg
               || cnst.newline_char
               || l_r_ord.load_num
               || ' '
               || LPAD(l_r_ord.ord_num, 11)
               || ' '
               || RPAD(l_r_ord.ord_src, 8)
               || ' '
               || l_r_ord.load_range;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'The following order(s) are on load(s) outside of restricted load range:'
               || cnst.newline_char
               || 'Load Order       OrdSrc   LoadRange'
               || cnst.newline_char
               || '---- ----------- -------- ---------'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rstr_load_range_fn;

  /*
  ||----------------------------------------------------------------------------
  || OPEN_CIG_FCST_FN
  ||  Return message indicating existence of open Cig Forecast.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/18/08 | rhalpai | Original. PIR6019
  || 07/15/08 | rhalpai | Changed to use divisional parm to indicate whether to
  ||                    | bypass check for open cig forecast. IM428710
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  || 11/01/18 | rhalpai | Add logic to check for an open cig replenishment. PIR18923
  ||----------------------------------------------------------------------------
  */
  FUNCTION open_cig_fcst_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.OPEN_CIG_FCST_FN';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_allw_opn_cig_fcst_sw  VARCHAR2(1);
    l_ship_pt_id            PLS_INTEGER;
    l_cig_dpt_id            PLS_INTEGER;
    l_msg                   typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_allw_opn_cig_fcst_sw := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_allw_opn_cig_fcst), 'N');

    IF l_allw_opn_cig_fcst_sw = 'N' THEN
      logs.dbg('Get Cig Div ShipPoint ID');
      l_ship_pt_id := cig_organization_pk.get_primary_ship_pt_id(i_div);
      logs.dbg('Get Cig Department ID');
      l_cig_dpt_id := cig_location_pk.get_dept_id(l_ship_pt_id, 'CIGDEPT');
      logs.dbg('Check for Open Transaction in Cig Fcst');

      IF (   cig_bb100_op_refresh_procs_pk.is_cc_open(l_ship_pt_id, l_cig_dpt_id)
          OR cig_bb100_op_refresh_procs_pk.is_replen_open(l_ship_pt_id, l_cig_dpt_id)
         ) THEN
        l_msg := 'An open Cig Forecast exists!';
      END IF;
    END IF;   -- l_allw_opn_cig_fcst_sw = 'N'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END open_cig_fcst_fn;

  /*
  ||----------------------------------------------------------------------------
  || RELEASED_PRE_POST_CUST_FN
  ||  Return message listing pre-post customers with open orders that will NOT
  ||  be included in release because they were included in a previous release
  ||  for the same LLR date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION released_pre_post_cust_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.RELEASED_PRE_POST_CUST_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_custs(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   p.load_num, p.stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_name
          FROM prepost_load_op1p p, mclp020b cx, sysp200c c, temp_set_release_op1t t, load_depart_op1f ld
         WHERE p.div_part = b_div_part
           AND p.llr_date = b_llr_dt
           AND cx.div_part = p.div_part
           AND cx.custb = p.cust_num
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'CORP')
                OR cx.corpb IN(SELECT t.val
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'CORP'))
           AND c.div_part = p.div_part
           AND c.acnoc = p.cust_num
           AND t.typ = 'LOAD'
           AND t.val = p.load_num
           AND ld.div_part = p.div_part
           AND ld.llr_dt = p.llr_date
           AND ld.load_num = p.load_num
           AND EXISTS(SELECT 1
                        FROM ordp100a a, ordp120b b
                       WHERE a.div_part = ld.div_part
                         AND a.load_depart_sid = ld.load_depart_sid
                         AND a.custa = p.cust_num
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.statb = 'O')
      ORDER BY p.load_num, p.stop_num;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_cust IN l_cur_custs(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg
               || cnst.newline_char
               || l_r_cust.load_num
               || '   '
               || LPAD(l_r_cust.stop_num, 2, '0')
               || ' '
               || l_r_cust.mcl_cust
               || ' '
               || l_r_cust.cust_name;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg :=
        'The following pre-post customers with open orders were previously released with the same LLR date and will be ignored:'
        || cnst.newline_char
        || 'Load Stop Cust   Name'
        || cnst.newline_char
        || '---- ---- ------ -------------------------'
        || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END released_pre_post_cust_fn;

  /*
  ||----------------------------------------------------------------------------
  || PRE_POST_CUST_LIST_FN
  ||  Return message listing pre-post customers for which all manifest
  ||  categories will be included in release.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION pre_post_cust_list_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.PRE_POST_CUST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_custs(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   ld.load_num, se.stop_num, cx.mccusb AS mcl_cust, c.namec AS cust_nm
          FROM sysp200c c, mclp020b cx, load_depart_op1f ld, stop_eta_op1g se
         WHERE c.div_part = b_div_part
           AND c.tclscc = 'PRP'
           AND cx.div_part = c.div_part
           AND cx.custb = c.acnoc
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'CORP')
                OR cx.corpb IN(SELECT t.val
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'CORP'))
           AND ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND ld.load_num IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'LOAD')
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND se.cust_id = cx.custb
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.stata = 'O')
           AND NOT EXISTS(SELECT 1
                            FROM prepost_load_op1p p
                           WHERE p.div_part = ld.div_part
                             AND p.load_num = ld.load_num
                             AND p.stop_num = se.stop_num
                             AND p.cust_num = se.cust_id
                             AND p.llr_date = b_llr_dt)
      ORDER BY 1, 2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_cust IN l_cur_custs(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg
               || cnst.newline_char
               || l_r_cust.load_num
               || '   '
               || LPAD(l_r_cust.stop_num, 2, '0')
               || ' '
               || l_r_cust.mcl_cust
               || ' '
               || l_r_cust.cust_nm;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'All categories will be released for the following pre-post customers:'
               || cnst.newline_char
               || 'Load Stop Cust   Name'
               || cnst.newline_char
               || '---- ---- ------ -------------------------'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END pre_post_cust_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RELEASED_LOAD_LIST_FN
  ||  Return message if loads were released before for different LLR Dates.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION released_load_list_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module         CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.RELEASED_LOAD_LIST_FN';
    lar_parm                    logs.tar_parm;
    l_msg                       typ.t_maxvc2;
    l_c_init_val       CONSTANT VARCHAR2(1)   := '~';
    l_llr_dt_save               VARCHAR2(10)  := l_c_init_val;
    l_cnt                       PLS_INTEGER   := 0;
    l_c_loads_per_ln   CONSTANT PLS_INTEGER   := 4;
    l_c_load_list_pad  CONSTANT PLS_INTEGER   := 5 * l_c_loads_per_ln;
    l_load_list                 typ.t_maxvc2;

    CURSOR l_cur_released_loads(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT TO_CHAR(lc.llr_dt, g_c_dt_fmt) AS llr_dt, lc.load_num
        FROM load_clos_cntrl_bc2c lc, temp_set_release_op1t t
       WHERE lc.div_part = b_div_part
         AND lc.llr_dt <> b_llr_dt
         AND lc.load_status <> 'A'
         AND t.val = lc.load_num
         AND t.typ = 'LOAD';

    PROCEDURE add_line_to_msg_sp IS
    BEGIN
      l_msg := l_msg || RPAD(l_load_list, l_c_load_list_pad) || ' => ' || l_llr_dt_save || cnst.newline_char;
    END add_line_to_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    FOR l_r_rlse IN l_cur_released_loads(i_div_part, i_llr_dt) LOOP
      IF    l_r_rlse.llr_dt <> l_llr_dt_save
         OR l_cnt = l_c_loads_per_ln THEN
        IF l_llr_dt_save <> l_c_init_val THEN
          add_line_to_msg_sp;
          l_load_list := NULL;
          l_cnt := 0;
        END IF;   -- l_llr_dt_save <> l_c_init_val

        l_llr_dt_save := l_r_rlse.llr_dt;
      END IF;   -- l_r_rlse.llr <> l_llr_dt_save OR l_cnt > l_c_loads_per_ln

      l_load_list := l_load_list ||(CASE
                                      WHEN l_load_list IS NOT NULL THEN ','
                                    END) || l_r_rlse.load_num;
      l_cnt := l_cnt + 1;
    END LOOP;

    -- final add line
    IF l_load_list IS NOT NULL THEN
      add_line_to_msg_sp;
    END IF;   -- l_load_list IS NOT NULL

    IF l_msg IS NOT NULL THEN
      l_msg := 'The following load(s) selected for this release were released before for different LLR dates.'
               || cnst.newline_char
               || '[LOAD=>LLRDate]'
               || cnst.newline_char
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END released_load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RLSE_STRCT_B4_PROD_RCPT_FN
  ||  Return message warning order lines in Release for strict items exist with
  ||  a calculated product receipt date greater than the LLR date.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/31/07 | rhalpai | Original added for PIR5002
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 03/02/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION rlse_strct_b4_prod_rcpt_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL,
    i_mfst_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.RLSE_STRCT_B4_PROD_RCPT_FN';
    lar_parm             logs.tar_parm;
    l_crp_list           typ.t_maxvc2;
    l_mfst_list          typ.t_maxvc2;
    l_cv                 SYS_REFCURSOR;
    l_exist_sw           VARCHAR2(1);
    l_msg                typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');

    IF i_load_list IS NOT NULL THEN
      l_crp_list := clean_list_fn(i_crp_list);
      l_mfst_list := clean_list_fn(i_mfst_list);
      logs.dbg('Load temp table with Load/Stop/Mfst lists');
      populate_temp_sp(i_load_list, l_crp_list, l_mfst_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT 'Y'
         FROM strct_ord_op1o so, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b, mclp020b cx
        WHERE so.div_part = i_div_part
          AND TRUNC(so.prod_rcpt_ts) > i_llr_dt
          AND ld.div_part = i_div_part
          AND ld.llr_dt = i_llr_dt
          AND ld.load_num IN(SELECT t.val
                               FROM temp_set_release_op1t t
                              WHERE t.typ = 'LOAD')
          AND a.div_part = ld.div_part
          AND a.load_depart_sid = ld.load_depart_sid
          AND a.ordnoa = so.ord_num
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa
          AND NOT EXISTS(SELECT 1
                           FROM sysp200c c
                          WHERE c.div_part = se.div_part
                            AND c.acnoc = se.cust_id
                            AND c.tclscc = 'PRP'
                            AND EXISTS(SELECT 1
                                         FROM prepost_load_op1p p
                                        WHERE p.div_part = ld.div_part
                                          AND p.load_num = ld.load_num
                                          AND p.stop_num = se.stop_num
                                          AND p.cust_num = se.cust_id
                                          AND p.llr_date = i_llr_dt))
          AND cx.div_part = se.div_part
          AND cx.custb = se.cust_id
          AND (   NOT EXISTS(SELECT 1
                               FROM temp_set_release_op1t t
                              WHERE t.typ = 'CORP')
               OR cx.corpb IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'CORP'))
          AND b.div_part = so.div_part
          AND b.ordnob = so.ord_num
          AND b.lineb = so.ord_ln
          AND b.excptn_sw = 'N'
          AND b.statb = 'O'
          AND (   EXISTS(SELECT 1
                           FROM sysp200c c
                          WHERE c.div_part = se.div_part
                            AND c.acnoc = se.cust_id
                            AND c.tclscc = 'PRP')
               OR l_mfst_list IS NULL
               OR EXISTS(SELECT 1
                           FROM temp_set_release_op1t t
                          WHERE t.typ = 'MANF'
                            AND t.val = b.manctb)
              );

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_exist_sw;

    IF l_cv%FOUND THEN
      l_msg := 'Order line(s) exist for strict items prior to its' || ' calculated product receipt timestamp.';
    END IF;   -- l_cv%FOUND

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END rlse_strct_b4_prod_rcpt_fn;

  /*
  ||----------------------------------------------------------------------------
  || LONE_DIST_FN
  ||  Return message for loads/stops for Dist order without Reg orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/13/09 | rhalpai | Original
  || 04/20/11 | rhalpai | Changed logic to exclude orders with order sources
  ||                    | that should not bill alone. PIR9910
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 01/07/15 | rhalpai | Change logic to build list of Load/Stop outside of
  ||                    | cursor to prevent cursor column limitation of 2000
  ||                    | chars. IM-228705
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  || 08/03/18 | rhalpai | Change logic to exclude DistOnly Customers. PIR18748
  ||----------------------------------------------------------------------------
  */
  FUNCTION lone_dist_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.LONE_DIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_msg                typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Load/Stop Cursor');

    OPEN l_cv
     FOR
       SELECT   ld.load_num || '   ' || LPAD(se.stop_num, 2, '0')
           FROM load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
          WHERE ld.div_part = i_div_part
            AND ld.llr_dt = i_llr_dt
            AND ld.load_num IN(SELECT t.val
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'LOAD')
            AND se.div_part = ld.div_part
            AND se.load_depart_sid = ld.load_depart_sid
            AND NOT EXISTS(SELECT 1
                             FROM sysp200c c, mclp100a ma
                            WHERE c.div_part = se.div_part
                              AND c.acnoc = se.cust_id
                              AND ma.div_part = c.div_part
                              AND ma.cstgpa = c.retgpc
                              AND ma.dist_only_sw = 'Y')
            AND cx.div_part = se.div_part
            AND cx.custb = se.cust_id
            AND (   NOT EXISTS(SELECT 1
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'CORP')
                 OR cx.corpb IN(SELECT t.val
                                  FROM temp_set_release_op1t t
                                 WHERE t.typ = 'CORP'))
            AND a.div_part = ld.div_part
            AND a.load_depart_sid = ld.load_depart_sid
            AND a.custa = se.cust_id
            AND a.ldtypa NOT BETWEEN 'P00' AND 'P99'
            AND a.ipdtsa NOT IN(SELECT s.ord_src
                                  FROM sub_prcs_ord_src s
                                 WHERE s.div_part = i_div_part
                                   AND s.prcs_id = 'SET RELEASE'
                                   AND s.prcs_sbtyp_cd IN('BSR', 'NAO'))
            AND a.stata = 'O'
       GROUP BY ld.load_num, se.stop_num
         HAVING SUM(DECODE(a.dsorda, 'D', 1, 0)) = COUNT(*)
       ORDER BY ld.load_num, se.stop_num;

    logs.dbg('Get Load/Stop List');
    l_msg := to_list_fn(l_cv, cnst.newline_char);

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'The following loads/stops contain only distribution orders:'
               || cnst.newline_char
               || 'Load Stop'
               || cnst.newline_char
               || '---- ----'
               || cnst.newline_char
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END lone_dist_fn;

  /*
  ||----------------------------------------------------------------------------
  || WAVE_PLAN_STOP_CNT_CHK_FN
  ||  Return message for loads sequenced for wave planning that have a variance
  ||  in stop counts saved at sequence time compared to the current number of
  ||  stops. Report stop count changes +/- more than the lanes per group.
  ||  (lanes_per_grp = ttl_lane_cnt / ttl_grp_cnt)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/05/09 | rhalpai | Original for PIR7118
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm.
  ||                    | Change to call new OP_PARMS_PK.IDX_VALS_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION wave_plan_stop_cnt_chk_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm             := 'OP_SET_RELEASE_PK.WAVE_PLAN_STOP_CNT_CHK_FN';
    lar_parm             logs.tar_parm;
    l_t_parms            op_types_pk.tt_varchars_v;
    l_lanes_per_grp      NUMBER;
    l_msg                typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_parms := op_parms_pk.idx_vals_fn(i_div_part,
                                         op_const_pk.prm_wave_plan
                                         || ','
                                         || op_const_pk.prm_wp_grp_cnt
                                         || ','
                                         || op_const_pk.prm_wp_lane_cnt
                                        );

    IF l_t_parms(op_const_pk.prm_wave_plan) = 'Y' THEN
      l_lanes_per_grp := TO_NUMBER(l_t_parms(op_const_pk.prm_wp_lane_cnt))
                         / TO_NUMBER(l_t_parms(op_const_pk.prm_wp_grp_cnt));

      IF i_load_list IS NOT NULL THEN
        logs.dbg('Load Temp Table');
        populate_temp_sp(i_load_list);
      END IF;   -- i_load_list IS NOT NULL

      logs.dbg('Get Load List with StopCnt Variance');

      SELECT to_list_fn(CURSOR(SELECT   wpl.load_num || LPAD(wpl.stop_cnt, 11) || LPAD(ld.stop_cnt, 11)
                                   FROM (SELECT   ld.load_num, COUNT(DISTINCT se.stop_num) AS stop_cnt
                                             FROM temp_set_release_op1t t, load_depart_op1f ld, stop_eta_op1g se
                                            WHERE t.typ = 'LOAD'
                                              AND ld.div_part = i_div_part
                                              AND ld.llr_dt = i_llr_dt
                                              AND ld.load_num = t.val
                                              AND se.div_part = ld.div_part
                                              AND se.load_depart_sid = ld.load_depart_sid
                                              AND EXISTS(SELECT 1
                                                           FROM ordp100a a
                                                          WHERE a.div_part = se.div_part
                                                            AND a.load_depart_sid = se.load_depart_sid
                                                            AND a.custa = se.cust_id
                                                            AND a.stata = 'O')
                                         GROUP BY ld.load_num) ld,
                                        wave_plan_load_op2w wpl
                                  WHERE wpl.div_part = i_div_part
                                    AND wpl.llr_dt = i_llr_dt
                                    AND wpl.load_num = ld.load_num
                                    AND ABS(wpl.stop_cnt - ld.stop_cnt) > l_lanes_per_grp
                               ORDER BY 1
                              ),
                        cnst.newline_char
                       )
        INTO l_msg
        FROM DUAL;

      IF l_msg IS NOT NULL THEN
        logs.dbg('Set Message');
        l_msg := 'The sequence plan created for this Release contains loads that have'
                 || ' had the number of stops attached to them changed. This could'
                 || ' affect the overall sequence of the loads processed through'
                 || ' billing. Press Cancel to stop the release and review the'
                 || ' Sequence Plan or press Continue to proceed with the Release.'
                 || cnst.newline_char
                 || 'Load SeqStopCnt CurStopCnt'
                 || cnst.newline_char
                 || '---- ---------- ----------'
                 || cnst.newline_char
                 || l_msg;
      END IF;   -- l_msg IS NOT NULL
    END IF;   -- l_t_parms(op_const_pk.prm_wave_plan) = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END wave_plan_stop_cnt_chk_fn;

  /*
  ||----------------------------------------------------------------------------
  || ACS_CLOS_FN
  ||  Return msg for loads with dispositions already received (ACSCloseSw = Y).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/14/19 | rhalpai | Original for SDHD-499897.
  ||----------------------------------------------------------------------------
  */
  FUNCTION acs_clos_fn(
    i_div_part   IN  NUMBER,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.ACS_CLOS_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Get Load List');

    SELECT LISTAGG(lc.load_num, cnst.newline_char) WITHIN GROUP(ORDER BY lc.load_num)
      INTO l_msg
      FROM load_clos_cntrl_bc2c lc
     WHERE lc.div_part = i_div_part
       AND lc.load_num IN(SELECT t.val
                            FROM temp_set_release_op1t t
                           WHERE t.typ = 'LOAD')
       AND lc.load_status <> 'A'
       AND lc.acs_load_clos_sw = 'Y';

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'Dispositions have already been processed for the following loads:' || cnst.newline_char || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END acs_clos_fn;

  /*
  ||----------------------------------------------------------------------------
  || ACS_CLOS_ECOM_FN
  ||  Return msg for ECOM loads with dispositions already received (ACSCloseSw = Y).
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/14/24 | rhalpai | Original for SDHD-2117680.
  ||----------------------------------------------------------------------------
  */
  FUNCTION acs_clos_ecom_fn(
    i_div_part   IN  NUMBER,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.ACS_CLOS_ECOM_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Get Load List');

    SELECT LISTAGG(lc.load_num, cnst.newline_char) WITHIN GROUP(ORDER BY lc.load_num)
      INTO l_msg
      FROM load_clos_cntrl_bc2c lc
     WHERE lc.div_part = i_div_part
       AND lc.load_num IN(SELECT t.val
                            FROM temp_set_release_op1t t
                           WHERE t.typ = 'LOAD'
                             AND SUBSTR(t.val, 1, 1) IN('E', 'S'))
       AND lc.load_status <> 'A'
       AND lc.acs_load_clos_sw = 'Y';

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'Dispositions have already been processed for the following ECOM loads:'
               || cnst.newline_char
               || l_msg
               || cnst.newline_char
               || 'Please move unbilled orders to another load for billing.';
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END acs_clos_ecom_fn;

  /*
  ||----------------------------------------------------------------------------
  || MIX_ECOM_NONECOM_LOAD_FN
  ||  Return msg for ECOM loads found with non-ECOM loads.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/26/25 | rhalpai | Original for SDHD-2481774.
  ||----------------------------------------------------------------------------
  */
  FUNCTION mix_ecom_nonecom_load_fn(
    i_div_part   IN  NUMBER,
    i_load_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.MIX_ECOM_NONECOM_LOAD_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Get Load List');

    SELECT LISTAGG(t.val, cnst.newline_char) WITHIN GROUP(ORDER BY t.val)
      INTO l_msg
      FROM temp_set_release_op1t t
     WHERE t.typ = 'LOAD'
       AND SUBSTR(t.val, 1, 1) IN('E', 'S')
       AND EXISTS(SELECT 1
                    FROM temp_set_release_op1t t2
                   WHERE t2.typ = 'LOAD'
                     AND SUBSTR(t2.val, 1, 1) NOT IN('E', 'S'));

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'ECOM loads found mixed with regular (non-ECOM) loads:'
               || cnst.newline_char
               || l_msg
               || cnst.newline_char
               || 'Please remove ECOM loads from release or remove the non-ECOM loads from relase if trying to bill only ECOM.';
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mix_ecom_nonecom_load_fn;

  /*
  ||----------------------------------------------------------------------------
  || MAINTAINED_ORDS_FN
  ||  Return message listing orders currently be maintained by users.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  || 08/11/08 | rhalpai | Add P to header status list in cursor. PIR6364
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 03/02/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to use div_part input parm. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION maintained_ords_fn(
    i_div_part   IN  NUMBER,
    i_llr_dt     IN  DATE,
    i_load_list  IN  VARCHAR2 DEFAULT NULL,
    i_crp_list   IN  VARCHAR2 DEFAULT NULL
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.MAINTAINED_ORDS_FN';
    lar_parm             logs.tar_parm;
    l_msg                typ.t_maxvc2;

    CURSOR l_cur_ords(
      b_div_part  NUMBER,
      b_llr_dt    DATE
    ) IS
      SELECT   a.ordnoa AS ord_num, a.mntusa AS user_id
          FROM load_depart_op1f ld, stop_eta_op1g se, mclp020b cx, ordp100a a
         WHERE ld.div_part = b_div_part
           AND ld.llr_dt = b_llr_dt
           AND ld.load_num IN(SELECT t.val
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'LOAD')
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND cx.div_part = ld.div_part
           AND cx.custb = se.cust_id
           AND (   NOT EXISTS(SELECT 1
                                FROM temp_set_release_op1t t
                               WHERE t.typ = 'CORP')
                OR cx.corpb IN(SELECT t.val
                                 FROM temp_set_release_op1t t
                                WHERE t.typ = 'CORP'))
           AND a.div_part = se.div_part
           AND a.load_depart_sid = se.load_depart_sid
           AND a.custa = se.cust_id
           AND a.excptn_sw = 'N'
           AND a.stata IN('O', 'P', 'R')
           AND RTRIM(a.mntusa) IS NOT NULL
      ORDER BY a.ordnoa, a.mntusa;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.info('ENTRY', lar_parm);

    IF i_load_list IS NOT NULL THEN
      logs.dbg('Load Temp Table');
      populate_temp_sp(i_load_list, i_crp_list);
    END IF;   -- i_load_list IS NOT NULL

    logs.dbg('Open Cursor');
    FOR l_r_ord IN l_cur_ords(i_div_part, i_llr_dt) LOOP
      l_msg := l_msg || cnst.newline_char || LPAD(l_r_ord.ord_num, 11) || ' ' || l_r_ord.user_id;
    END LOOP;

    IF l_msg IS NOT NULL THEN
      logs.dbg('Set Message');
      l_msg := 'The following orders are being maintained.'
               || cnst.newline_char
               || 'If possible, contact the user(s) for course of action '
               || cnst.newline_char
               || 'OR click on ''Continue'' (the order(s) will still be included in the SetRelease):'
               || cnst.newline_char
               || 'OrderNum    UserId'
               || cnst.newline_char
               || '----------- --------'
               || l_msg;
    END IF;   -- l_msg IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_msg);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END maintained_ords_fn;

  /*
  ||----------------------------------------------------------------------------
  || WRAP_TEXT_FN
  ||  Return text with "New Line" characters inserted at specified length.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION wrap_text_fn(
    i_txt  IN  VARCHAR2,
    i_len  IN  PLS_INTEGER
  )
    RETURN VARCHAR2 IS
    l_wrapped_txt  typ.t_maxvc2;
    l_pos          PLS_INTEGER  := 1;
    l_max_len      PLS_INTEGER  := LENGTH(i_txt);
  BEGIN
    IF     i_txt IS NOT NULL
       AND i_len > 0 THEN
      WHILE l_pos < l_max_len LOOP
        l_wrapped_txt := l_wrapped_txt
                         ||(CASE
                              WHEN l_wrapped_txt IS NOT NULL THEN cnst.newline_char
                            END)
                         || SUBSTR(i_txt, l_pos, i_len);
        l_pos := l_pos + i_len;
      END LOOP;
    END IF;   -- i_txt IS NOT NULL AND i_len > 0

    RETURN(l_wrapped_txt);
  END wrap_text_fn;

  /*
  ||----------------------------------------------------------------------------
  || VNDR_CMP_SP
  ||  Apply Vendor Compliance order quantity changes
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/22/10 | rhalpai | Original - created for PIR8216
  || 02/24/10 | rhalpai | Changed logic to adjust the customer Default Vendor
  ||                    | Compliance order to meet compliance. PIR8099
  || 07/15/10 | rhalpai | Add profile type restriction for DVC to cursor.
  ||                    | PIR8936
  || 08/24/10 | rhalpai | Changed logic for efficiency.
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/02/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE vndr_cmp_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.VNDR_CMP_SP';
    lar_parm             logs.tar_parm;
    l_t_ord_nums         type_ntab;
    l_t_ord_lns          type_ntab;
    l_t_add_ord_qtys     type_ntab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Process Vendor Compliance Order Cursor');

    WITH vc AS
         (SELECT   r.llr_dt, c.cust_id, c.prof_id, c.cmp_qty, c.beg_dt, c.end_dt
              FROM vndr_cmp_prof_op3l p, vndr_cmp_cust_op2l c, rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld,
                   stop_eta_op1g se, ordp100a a, ordp120b b, sawp505e e, vndr_cmp_item_op1l vci
             WHERE p.typ = 'DVC'
               AND c.div_part = i_div_part
               AND c.prof_id = p.prof_id
               AND r.div_part = i_div_part
               AND r.rlse_ts = i_rlse_ts
               AND rl.div_part = r.div_part
               AND rl.rlse_id = r.rlse_id
               AND rl.typ_id = 'LOAD'
               AND ld.div_part = r.div_part
               AND ld.llr_dt = r.llr_dt
               AND ld.load_num = rl.val
               AND se.div_part = ld.div_part
               AND se.load_depart_sid = ld.load_depart_sid
               AND se.cust_id = c.cust_id
               AND TRUNC(se.eta_ts) BETWEEN c.beg_dt AND c.end_dt
               AND a.div_part = se.div_part
               AND a.load_depart_sid = se.load_depart_sid
               AND a.custa = se.cust_id
               AND a.excptn_sw = 'N'
               AND b.div_part = a.div_part
               AND b.ordnob = a.ordnoa
               AND b.subrcb < 999
               AND b.excptn_sw = 'N'
               AND b.statb = 'P'
               AND e.iteme = b.itemnb
               AND e.uome = b.sllumb
               AND vci.prof_id = c.prof_id
               AND vci.catlg_num = e.catite
          GROUP BY r.llr_dt, c.cust_id, c.prof_id, c.cmp_qty, c.beg_dt, c.end_dt),
         hist AS
         (SELECT vc.llr_dt, vc.cust_id, vc.prof_id, vc.cmp_qty, vc.beg_dt, vc.end_dt, h.parnt_item, h.catlg_num, h.ord_qty
            FROM vc,
                 (SELECT   c.prof_id, c.cust_id, c.cmp_qty, c.beg_dt, c.end_dt, vci.parnt_item, vci.catlg_num,
                           SUM(b.pckqtb) AS ord_qty
                      FROM vndr_cmp_prof_op3l p, vndr_cmp_cust_op2l c, vndr_cmp_item_op1l vci, ordp900a a, ordp920b b
                     WHERE p.typ = 'DVC'
                       AND c.div_part = i_div_part
                       AND c.prof_id = p.prof_id
                       AND a.div_part = c.div_part
                       AND a.custa = c.cust_id
                       AND a.etadta BETWEEN c.beg_dt - DATE '1900-02-28' AND c.end_dt - DATE '1900-02-28'
                       AND a.excptn_sw = 'N'
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.subrcb < 999
                       AND b.excptn_sw = 'N'
                       AND b.statb = 'A'
                       AND b.pckqtb > 0
                       AND vci.prof_id = p.prof_id
                       AND vci.catlg_num = b.orditb
                  GROUP BY c.prof_id, c.cust_id, c.cmp_qty, c.beg_dt, c.end_dt, vci.parnt_item, vci.catlg_num) h
           WHERE h.prof_id = vc.prof_id
             AND h.cust_id = vc.cust_id),
         vcc AS
         (SELECT   x.llr_dt, x.cust_id, x.prof_id, x.cmp_qty, x.beg_dt, x.end_dt
              FROM (SELECT   vc.llr_dt, vc.cust_id, vc.prof_id, vc.cmp_qty, vc.beg_dt, vc.end_dt,
                             SUM(DECODE(b.statb, 'P', b.ordqtb, b.pckqtb)) AS ord_qty
                        FROM vc, stop_eta_op1g se, load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e,
                             vndr_cmp_item_op1l vci
                       WHERE se.div_part = i_div_part
                         AND se.cust_id = vc.cust_id
                         AND TRUNC(se.eta_ts) BETWEEN vc.beg_dt AND vc.end_dt
                         AND ld.div_part = se.div_part
                         AND ld.load_depart_sid = se.load_depart_sid
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.excptn_sw = 'N'
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND (   (    b.statb = 'P'
                                  AND ld.llr_dt = vc.llr_dt)
                              OR (    b.statb IN('R', 'A')
                                  AND b.pckqtb > 0))
                         AND b.subrcb < 999
                         AND b.excptn_sw = 'N'
                         AND b.statb IN('P', 'R', 'A')
                         AND e.iteme = b.itemnb
                         AND e.uome = b.sllumb
                         AND vci.prof_id = vc.prof_id
                         AND vci.catlg_num = e.catite
                    GROUP BY vc.llr_dt, vc.cust_id, vc.prof_id, vc.cmp_qty, vc.beg_dt, vc.end_dt
                    UNION ALL
                    SELECT   h.llr_dt, h.cust_id, h.prof_id, h.cmp_qty, h.beg_dt, h.end_dt, SUM(h.ord_qty) AS ord_qty
                        FROM hist h
                    GROUP BY h.llr_dt, h.cust_id, h.prof_id, h.cmp_qty, h.beg_dt, h.end_dt) x
          GROUP BY x.llr_dt, x.cust_id, x.prof_id, x.cmp_qty, x.beg_dt, x.end_dt
            HAVING x.cmp_qty > SUM(x.ord_qty))
    SELECT o.ord_num, o.ord_ln,(q.cmp_qty - i.parnt_item_qty) AS add_ord_qty
    BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns, l_t_add_ord_qtys
    FROM   vndr_cmp_qty_op4l q,
           (SELECT dvc.cust_id, dvc.prof_id, dvc.catlg_num, TO_NUMBER(SUBSTR(dvc.ord_num_ln, 1, 11)) AS ord_num,
                   TO_NUMBER(SUBSTR(dvc.ord_num_ln, 12)) AS ord_ln
              FROM (SELECT   vcc.cust_id, vcc.prof_id, vci.catlg_num, MAX(LPAD(b.ordnob, 11, '0') || b.lineb) AS ord_num_ln
                        FROM vcc, stop_eta_op1g se, load_depart_op1f ld, ordp100a a, ordp120b b, vndr_cmp_item_op1l vci
                       WHERE se.div_part = i_div_part
                         AND se.cust_id = vcc.cust_id
                         AND TRUNC(se.eta_ts) BETWEEN vcc.beg_dt AND vcc.end_dt
                         AND ld.div_part = se.div_part
--                         AND ld.load_depart_sid = se.load_depart_sid
                         AND ld.llr_dt = vcc.llr_dt
                         AND a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.ipdtsa = 'DVC'
                         AND a.excptn_sw = 'N'
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.excptn_sw = 'N'
                         AND b.statb = 'P'
                         AND b.ntshpb IS NULL
                         AND vci.prof_id = vcc.prof_id
                         AND vci.catlg_num = b.orditb
                    GROUP BY vcc.cust_id, vcc.prof_id, vci.catlg_num) dvc) o,
           (SELECT y.cust_id, y.prof_id, y.parnt_item, y.catlg_num,
                   SUM(y.ord_qty) OVER(PARTITION BY y.cust_id, y.prof_id, y.parnt_item
                                       ORDER BY y.catlg_num
                                       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS parnt_item_qty
              FROM (SELECT   x.cust_id, x.prof_id, x.parnt_item, x.catlg_num, SUM(x.ord_qty) AS ord_qty
                        FROM (SELECT   vcc.cust_id, vcc.prof_id, vci.parnt_item, vci.catlg_num,
                                       SUM(DECODE(b.statb, 'P', b.ordqtb, b.pckqtb)) AS ord_qty
                                  FROM vcc, stop_eta_op1g se, load_depart_op1f ld, ordp100a a, ordp120b b, sawp505e e,
                                       vndr_cmp_item_op1l vci
                                 WHERE se.div_part = i_div_part
                                   AND se.cust_id = vcc.cust_id
                                   AND TRUNC(se.eta_ts) BETWEEN vcc.beg_dt AND vcc.end_dt
                                   AND ld.div_part = se.div_part
                                   AND ld.load_depart_sid = se.load_depart_sid
                                   AND a.div_part = se.div_part
                                   AND a.load_depart_sid = se.load_depart_sid
                                   AND a.custa = se.cust_id
                                   AND a.excptn_sw = 'N'
                                   AND b.div_part = a.div_part
                                   AND b.ordnob = a.ordnoa
                                   AND (   (    b.statb = 'P'
                                            AND ld.llr_dt = vcc.llr_dt)
                                        OR (    b.statb IN('R', 'A')
                                            AND b.pckqtb > 0))
                                   AND b.subrcb < 999
                                   AND b.excptn_sw = 'N'
                                   AND b.statb IN('P', 'R', 'A')
                                   AND e.iteme = b.itemnb
                                   AND e.uome = b.sllumb
                                   AND vci.prof_id = vcc.prof_id
                                   AND vci.catlg_num = e.catite
                              GROUP BY vcc.cust_id, vcc.prof_id, vci.parnt_item, vci.catlg_num
                              UNION ALL
                              SELECT h.cust_id, h.prof_id, h.parnt_item, h.catlg_num, h.ord_qty
                                FROM vcc, hist h
                               WHERE vcc.prof_id = h.prof_id
                                 AND vcc.cust_id = h.cust_id) x
                    GROUP BY x.cust_id, x.prof_id, x.parnt_item, x.catlg_num) y) i
     WHERE q.div_part = i_div_part
       AND q.prof_id = i.prof_id
       AND q.cust_id = i.cust_id
       AND q.parnt_item = i.parnt_item
       AND q.cmp_qty > i.parnt_item_qty
       AND o.prof_id = i.prof_id
       AND o.cust_id = i.cust_id
       AND o.catlg_num = i.catlg_num;

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Adjust Order Qty');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        UPDATE ordp120b b
           SET b.ordqtb = b.ordqtb + l_t_add_ord_qtys(i)
         WHERE b.div_part = i_div_part
           AND b.ordnob = l_t_ord_nums(i)
           AND b.lineb = l_t_ord_lns(i);
    END IF;   -- l_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END vndr_cmp_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || LLR_DATES_FN
  ||   Build a cursor of LLR Dates for open orders on valid loads excluding
  ||   DFLT,DIST,LOST,COPY.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/12/06 | rhalpai | Original. PIR3593
  || 06/16/08 | rhalpai | Added sort by LLRDt to cursor.
  || 11/01/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION llr_dates_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.LLR_DATES_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_xloads           type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   TO_CHAR(ld.llr_dt, 'YYYY-MM-DD')
           FROM load_depart_op1f ld
          WHERE ld.div_part = l_div_part
            AND ld.llr_ts > DATE '1900-01-01'
            AND ld.load_num NOT IN(SELECT t.column_value
                                     FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
            AND EXISTS(SELECT 1
                         FROM ordp100a a
                        WHERE a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.stata IN('O', 'P'))
       GROUP BY ld.llr_dt
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END llr_dates_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LOAD_LIST_FN
  ||  Returns a list of all loads when both LLR Date and Strategy ID are NULL,
  ||  otherwise, it returns loads with open orders for LLR Date (if passed) and
  ||  indicates the selected loads for Strategy ID (if passed).
  ||
  ||  When LLR Date and/or Strategy ID are passed, open orders must exist for
  ||  load and the load must not be in a closed (load_status='A') or closing
  ||  (user_id not NULL) state for the order's LLR Date.
  ||
  ||  The testbill parm applies when LLR Date and/or Strategy ID are passed.
  ||  A zero indicates a non-testbill and a non-zero indicates a testbill.
  ||  A load for a testbill cannot have order lines in a released ('R') status.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/21/06 | SNAGABH | Original
  || 02/22/10 | rhalpai | Changed to include loads used for Dist Reassign Loads
  ||                    | process. PIR7342
  || 03/01/10 | rhalpai | Recode cursor to avoid ORA600 [kokbcvb1] Oracle bug.
  ||                    | IM570707
  || 11/01/10 | rhalpai | Replace hard-coded excluded loads with use of parm
  ||                    | table. PIR8531
  || 02/16/12 | rhalpai | Convert to use new strategy tables. PIR10845
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VALS_FOR_PRFX_FN,
  ||                    | OP_PARMS_PK.GET_PARMS_FOR_PRFX_SP. PIR15427
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_load_list_fn(
    i_div        IN  VARCHAR2,
    i_test_bill  IN  PLS_INTEGER,
    i_llr_dt     IN  VARCHAR2 DEFAULT NULL,
    i_strtg_id   IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.GET_LOAD_LIST_FN';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_t_xloads              type_stab;
    l_t_reassgn_from_loads  type_stab;
    l_t_reassgn_to_loads    type_stab;
    l_cv                    SYS_REFCURSOR;
    l_llr_dt                DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'TestBill', i_test_bill);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'StrtgID', i_strtg_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    op_parms_pk.get_parms_for_prfx_sp(l_div_part,
                                      op_const_pk.prm_reassgn_load,
                                      l_t_reassgn_from_loads,
                                      l_t_reassgn_to_loads,
                                      4
                                     );

    IF i_llr_dt IS NOT NULL THEN
      l_llr_dt := TO_DATE(i_llr_dt, g_c_dt_fmt);
    END IF;

    logs.dbg('Retrieve list of loads');

    OPEN l_cv
     FOR
       SELECT   x.loadc, x.destc,
                (SELECT 'Y'
                   FROM rlse_strtg_atrbt_op4s s
                  WHERE s.div_part = l_div_part
                    AND s.strtg_id = i_strtg_id
                    AND s.typ_id = 'LOAD'
                    AND s.val = x.loadc) AS "Sel"
           FROM (SELECT c.loadc, c.destc
                   FROM mclp120c c
                  WHERE c.div_part = l_div_part
                    AND c.loadc NOT IN(SELECT t.column_value
                                         FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                    AND (   (    i_strtg_id IS NULL
                             AND l_llr_dt IS NULL)
                         OR EXISTS(SELECT 1
                                     FROM load_depart_op1f ld, ordp100a a, ordp120b b
                                    WHERE ld.div_part = c.div_part
                                      AND (   l_llr_dt IS NULL
                                           OR ld.llr_dt = l_llr_dt)
                                      AND ld.load_num = c.loadc
                                      AND a.div_part = ld.div_part
                                      AND a.load_depart_sid = ld.load_depart_sid
                                      AND b.div_part = a.div_part
                                      AND b.ordnob = a.ordnoa
                                      AND b.statb = 'O'
                                      AND NOT EXISTS(SELECT 1
                                                       FROM load_clos_cntrl_bc2c lc
                                                      WHERE lc.div_part = ld.div_part
                                                        AND lc.llr_dt = ld.llr_dt
                                                        AND lc.load_num = ld.load_num
                                                        AND lc.load_status IN('A', 'T'))
                                      AND (   i_test_bill = 0
                                           OR NOT EXISTS(SELECT 1
                                                           FROM ordp100a a2, ordp120b b2
                                                          WHERE a2.div_part = ld.div_part
                                                            AND a2.load_depart_sid = ld.load_depart_sid
                                                            AND b2.div_part = a2.div_part
                                                            AND b2.ordnob = a2.ordnoa
                                                            AND b2.statb = 'R')
                                          ))
                        )
                 UNION
                 SELECT l.loadc, l.destc
                   FROM (SELECT rf.from_load, rt.to_load
                           FROM (SELECT t.column_value AS from_load, ROWNUM AS seq
                                   FROM TABLE(CAST(l_t_reassgn_from_loads AS type_stab)) t) rf,
                                (SELECT t.column_value AS to_load, ROWNUM AS seq
                                   FROM TABLE(CAST(l_t_reassgn_to_loads AS type_stab)) t) rt
                          WHERE rf.seq = rt.seq) rl,
                        mclp120c l
                  WHERE rl.from_load NOT IN(SELECT t.column_value
                                              FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
                    AND l.div_part = l_div_part
                    AND l.loadc = rl.to_load
                    AND (   (    i_strtg_id IS NULL
                             AND l_llr_dt IS NULL)
                         OR EXISTS(SELECT 1
                                     FROM load_depart_op1f ld, ordp100a a, ordp120b b
                                    WHERE ld.div_part = l_div_part
                                      AND (   l_llr_dt IS NULL
                                           OR ld.llr_dt = l_llr_dt)
                                      AND ld.load_num = rl.from_load
                                      AND a.div_part = ld.div_part
                                      AND a.load_depart_sid = ld.load_depart_sid
                                      AND b.div_part = a.div_part
                                      AND b.ordnob = a.ordnoa
                                      AND b.statb = 'O'
                                      AND NOT EXISTS(SELECT 1
                                                       FROM load_clos_cntrl_bc2c lc
                                                      WHERE lc.div_part = ld.div_part
                                                        AND lc.llr_dt = ld.llr_dt
                                                        AND lc.load_num = ld.load_num
                                                        AND lc.load_status IN('A', 'T'))
                                      AND (   i_test_bill = 0
                                           OR NOT EXISTS(SELECT 1
                                                           FROM ordp100a a2, ordp120b b2
                                                          WHERE a2.div_part = ld.div_part
                                                            AND a2.load_depart_sid = ld.load_depart_sid
                                                            AND b2.div_part = a2.div_part
                                                            AND b2.ordnob = a2.ordnoa
                                                            AND b2.statb = 'R')
                                          ))
                        )) x
       ORDER BY "Sel", x.loadc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_load_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_CRP_CD_LIST_FN
  ||  Return list of Corp Codes with available Orders for Div/LLRDt
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/16/12 | rhalpai | Original for PIR10845
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_crp_cd_list_fn(
    i_div       IN  VARCHAR2,
    i_llr_dt    IN  VARCHAR2,
    i_strtg_id  IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.GET_CRP_CD_LIST_FN';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_llr_dt := TO_DATE(i_llr_dt, g_c_dt_fmt);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   LPAD(x.corpb, 3, '0') AS crp_cd, LPAD(x.corpb, 3, '0') || ' ' || c.corp_nm AS crp_nm,
                (SELECT 'Y'
                   FROM rlse_strtg_atrbt_op4s s
                  WHERE s.div_part = x.div_part
                    AND s.strtg_id = i_strtg_id
                    AND s.typ_id = 'CORP'
                    AND s.val IN('~', x.corpb)) AS "Sel"
           FROM (SELECT   d.div_part, cx.corpb
                     FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, mclp020b cx
                    WHERE d.div_id = i_div
                      AND ld.div_part = d.div_part
                      AND ld.llr_dt = l_llr_dt
                      AND se.div_part = ld.div_part
                      AND se.load_depart_sid = ld.load_depart_sid
                      AND cx.div_part = se.div_part
                      AND cx.custb = se.cust_id
                      AND EXISTS(SELECT 1
                                   FROM ordp100a a
                                  WHERE a.div_part = se.div_part
                                    AND a.load_depart_sid = se.load_depart_sid
                                    AND a.custa = se.cust_id
                                    AND a.stata IN('O', 'P'))
                 GROUP BY d.div_part, cx.corpb) x,
                corp_cd_dm1c c
          WHERE c.corp_cd(+) = x.corpb
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_crp_cd_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_MANIFEST_CAT_LIST_FN
  ||  Return list of all manifest categories along with indication of matching
  ||  manifest category for strategy ID when passed.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/06 | SNAGABH | Original
  || 05/22/06 | SNAGABH | Added additional logic to include Strategy selection.
  || 02/16/12 | rhalpai | Convert to use new strategy tables. PIR10845
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_manifest_cat_list_fn(
    i_div       IN  VARCHAR2,
    i_strtg_id  IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.GET_MANIFEST_CAT_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StrtgID', i_strtg_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Retrieve list of Manifest Categories');

    OPEN l_cv
     FOR
       SELECT   c.descc, c.manctc,
                (SELECT 'Y'
                   FROM rlse_strtg_atrbt_op4s s
                  WHERE s.div_part = c.div_part
                    AND s.strtg_id = i_strtg_id
                    AND s.typ_id = 'MFST'
                    AND s.val IN('~', c.manctc)) AS "Sel"
           FROM mclp210c c
          WHERE c.div_part = (SELECT d.div_part
                                FROM div_mstr_di1d d
                               WHERE d.div_id = i_div)
       ORDER BY "Sel", c.manctc;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_manifest_cat_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_STRATEGY_LIST_FN
  ||  Function to return the list of valid Strategies in the system
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/06 | SNAGABH | Original
  || 02/16/12 | rhalpai | Add DIV parm. Convert to use new strategy tables.
  ||                    | PIR10845
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_strategy_list_fn(
    i_div  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.GET_STRATEGY_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Retrieve list of Strategies');

    OPEN l_cv
     FOR
       SELECT   s.strtg_nm, s.strtg_id,
                (SELECT DECODE(MAX(sa.val), '~', 'Y', 'N')
                   FROM rlse_strtg_atrbt_op4s sa
                  WHERE sa.div_part = s.div_part
                    AND sa.strtg_id = s.strtg_id
                    AND sa.typ_id = 'CORP') AS all_corps_sw,
                (SELECT DECODE(MAX(sa.val), '~', 'Y', 'N')
                   FROM rlse_strtg_atrbt_op4s sa
                  WHERE sa.div_part = s.div_part
                    AND sa.strtg_id = s.strtg_id
                    AND sa.typ_id = 'MFST') AS all_mfst_sw
           FROM div_mstr_di1d d, rlse_strtg_op4t s
          WHERE d.div_id = i_div
            AND s.div_part = d.div_part
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_strategy_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LAST_LLR_STATUS_FN
  ||  Function to return the Status of last Billing Release.
  ||  Valid Return Values:
  ||        Complete      -  Last Billing Release Completed Successfully.
  ||        Live          -  A Live Billing is in Progress.
  ||        Test          -  A Test Billing is in Progress.
  ||        Error         -  Last Billing Release Ended with Errors.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/06 | SNAGABH | Original
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION get_last_llr_status_fn(
    i_div  IN  VARCHAR2
  )
    RETURN VARCHAR2 IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.GET_LAST_LLR_STATUS_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_stat               VARCHAR2(10)  := 'Complete';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT (CASE r.stat_cd
                 WHEN 'R' THEN 'Complete'
                 WHEN 'E' THEN 'Error'
                 WHEN 'P' THEN(CASE
                                 WHEN r.test_bil_cd = '~' THEN 'Live'
                                 ELSE 'Test'
                               END)
               END
              )
         FROM div_mstr_di1d d, rlse_op1z r
        WHERE d.div_id = i_div
          AND r.div_part = d.div_part
          AND r.rlse_ts = (SELECT MAX(r2.rlse_ts)
                             FROM rlse_op1z r2
                            WHERE r2.div_part = r.div_part);

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_stat;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_stat);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END get_last_llr_status_fn;

  /*
  ||----------------------------------------------------------------------------
  || REASSGN_LOADS_SP
  ||  Reassign distribution orders on specific loads to their alternate loads
  ||  and log the moves. Use the undo parm to reverse the move.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/18/09 | rhalpai | Original for PIR7868
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.GET_PARMS_FOR_PRFX_SP.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reassgn_loads_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE,
    i_user_id   IN  VARCHAR2,
    i_undo_sw   IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm         := 'OP_SET_RELEASE_PK.REASSGN_LOADS_SP';
    lar_parm                logs.tar_parm;
    l_c_sysdate    CONSTANT DATE                  := SYSDATE;
    l_t_reassgn_from_loads  type_stab;
    l_t_reassgn_to_loads    type_stab;

    TYPE l_rt_load_ords IS RECORD(
      llr_ts        DATE,
      load_num      NUMBER,
      depart_ts     DATE,
      cust_id       sysp200c.acnoc%TYPE,
      stop_num      NUMBER,
      eta_ts        DATE,
      t_ord_nums    type_ntab,
      new_load_num  NUMBER
    );

    TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

    l_t_load_ords           l_tt_load_ords;
    l_llr_ts_save           DATE                  := DATE '0001-01-01';
    l_load_num_save         mclp120c.loadc%TYPE   := '~';
    l_load_depart_sid       NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'UndoSw', i_undo_sw);
    logs.info('ENTRY', lar_parm);

    IF i_undo_sw = 'N' THEN
      op_parms_pk.get_parms_for_prfx_sp(i_div_part,
                                        op_const_pk.prm_reassgn_load,
                                        l_t_reassgn_from_loads,
                                        l_t_reassgn_to_loads,
                                        4
                                       );

      SELECT   ld.llr_ts,
               ld.load_num,
               ld.depart_ts,
               se.cust_id,
               se.stop_num,
               se.eta_ts,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id
                                AND a.dsorda = 'D'
                                AND a.excptn_sw = 'N'
                                AND EXISTS(SELECT 1
                                             FROM ordp120b b
                                            WHERE b.div_part = a.div_part
                                              AND b.ordnob = a.ordnoa
                                              AND b.statb = 'P')
                            ) AS type_ntab
                   ) AS ord_nums,
               rl.to_load AS new_load_num
      BULK COLLECT INTO l_t_load_ords
          FROM (SELECT rf.from_load, rt.to_load
                  FROM (SELECT t.column_value AS from_load, ROWNUM AS seq
                          FROM TABLE(CAST(l_t_reassgn_from_loads AS type_stab)) t) rf,
                       (SELECT t.column_value AS to_load, ROWNUM AS seq
                          FROM TABLE(CAST(l_t_reassgn_to_loads AS type_stab)) t) rt
                 WHERE rf.seq = rt.seq) rl,
               rlse_op1z r, rlse_log_op2z rl, rlse_log_op2z rl2, load_depart_op1f ld, stop_eta_op1g se
         WHERE r.div_part = i_div_part
           AND r.rlse_ts = i_rlse_ts
           AND rl.div_part = r.div_part
           AND rl.rlse_id = r.rlse_id
           AND rl.typ_id = 'LOAD'
           AND rl.val = rl.from_load
           AND rl2.div_part = r.div_part
           AND rl2.rlse_id = r.rlse_id
           AND rl2.typ_id = 'LOAD'
           AND rl2.val = rl.to_load
           AND ld.div_part = r.div_part
           AND ld.llr_dt = r.llr_dt
           AND ld.load_num = rl.to_load
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND EXISTS(SELECT 1
                        FROM ordp100a a, ordp120b b
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.dsorda = 'D'
                         AND a.excptn_sw = 'N'
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.statb = 'P')
      ORDER BY ld.llr_ts, new_load_num, se.load_depart_sid, se.cust_id;
    ELSE
      SELECT   ld.llr_ts,
               ld.load_num,
               ld.depart_ts,
               se.cust_id,
               se.stop_num,
               se.eta_ts,
               CAST(MULTISET(SELECT a.ordnoa
                               FROM ordp100a a
                              WHERE a.div_part = i_div_part
                                AND a.load_depart_sid = ld.load_depart_sid
                                AND a.custa = se.cust_id
                                AND a.excptn_sw = 'N'
                                AND EXISTS(SELECT 1
                                             FROM mclp300d md, rlse_op1z r,
                                                  (SELECT NVL(MIN(r2.rlse_ts), l_c_sysdate) AS nxt_rlse
                                                     FROM rlse_op1z r2
                                                    WHERE r2.div_part = i_div_part
                                                      AND r2.rlse_ts > i_rlse_ts) x
                                            WHERE r.div_part = i_div_part
                                              AND r.rlse_ts = i_rlse_ts
                                              AND md.div_part = a.div_part
                                              AND md.reasnd = 'REASGNLD'
                                              AND md.ordnod = a.ordnoa
                                              AND md.ordlnd = 0
                                              AND SUBSTR(md.itemd, 1, 4) = ld.load_num
                                              AND md.last_chg_ts BETWEEN r.rlse_ts AND x.nxt_rlse
                                              AND md.resusd = r.user_id)
                            ) AS type_ntab
                   ) AS ord_nums,
               o.fro_load AS new_load_num
      BULK COLLECT INTO l_t_load_ords
          FROM (SELECT md.ordnod AS ord_num, SUBSTR(md.descd, 12, 4) AS fro_load, SUBSTR(md.itemd, 1, 4) AS to_load
                  FROM mclp300d md, rlse_op1z r, (SELECT NVL(MIN(r2.rlse_ts), l_c_sysdate) AS nxt_rlse
                                                    FROM rlse_op1z r2
                                                   WHERE r2.div_part = i_div_part
                                                     AND r2.rlse_ts > i_rlse_ts) x
                 WHERE r.div_part = i_div_part
                   AND r.rlse_ts = i_rlse_ts
                   AND md.div_part = r.div_part
                   AND md.reasnd = 'REASGNLD'
                   AND md.ordlnd = 0
                   AND md.last_chg_ts BETWEEN r.rlse_ts AND x.nxt_rlse
                   AND md.resusd = r.user_id) o,
               rlse_op1z r, load_depart_op1f ld, stop_eta_op1g se
         WHERE r.div_part = i_div_part
           AND r.rlse_ts = i_rlse_ts
           AND ld.div_part = r.div_part
           AND ld.llr_dt = r.llr_dt
           AND ld.load_num = o.to_load
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.ordnoa = o.ord_num
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.excptn_sw = 'N')
      GROUP BY ld.load_depart_sid, ld.llr_ts, ld.load_num, ld.depart_ts, se.cust_id, se.stop_num, se.eta_ts, o.fro_load
      ORDER BY ld.llr_ts, new_load_num, ld.load_depart_sid, se.cust_id;
    END IF;   -- i_undo_sw = 'N'

    IF l_t_load_ords.COUNT > 0 THEN
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        IF NOT(    l_t_load_ords(i).llr_ts = l_llr_ts_save
               AND l_t_load_ords(i).new_load_num = l_load_num_save) THEN
          l_llr_ts_save := l_t_load_ords(i).llr_ts;
          l_load_num_save := l_t_load_ords(i).new_load_num;
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part,
                                                                   l_t_load_ords(i).llr_ts,
                                                                   l_t_load_ords(i).new_load_num
                                                                  );
        END IF;   -- NOT (l_t_load_ords(i).llr_ts = l_llr_ts_save AND l_t_load_ords(i).new_load_num = l_load_num_save)

        logs.dbg('Move Orders');
        op_order_load_pk.move_ords_sp(i_div_part,
                                      l_t_load_ords(i).cust_id,
                                      l_load_depart_sid,
                                      l_t_load_ords(i).stop_num,
                                      l_t_load_ords(i).eta_ts,
                                      l_t_load_ords(i).llr_ts,
                                      l_t_load_ords(i).load_num,
                                      l_t_load_ords(i).depart_ts,
                                      l_t_load_ords(i).stop_num,
                                      l_t_load_ords(i).eta_ts,
                                      'REASGNLD',
                                      i_user_id,
                                      l_t_load_ords(i).t_ord_nums
                                     );
      END LOOP;
    END IF;   -- l_t_load_ords.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reassgn_loads_sp;

  /*
  ||----------------------------------------------------------------------------
  || UNDO_VNDR_CMP_SP
  ||  Reverse Vendor Compliance order quantity changes
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/22/10 | rhalpai | Original - created for PIR8216
  || 02/24/10 | rhalpai | Changed logic to reset the customer Default Vendor
  ||                    | Compliance order to zero qtys. PIR8099
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/02/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE undo_vndr_cmp_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.UNDO_VNDR_CMP_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Reset OrdQty');

    UPDATE ordp120b b
       SET b.ordqtb = 0
     WHERE b.div_part = i_div_part
       AND b.ordqtb > 0
       AND b.excptn_sw = 'N'
       AND b.statb IN('P', 'O')
       AND b.ordnob IN(SELECT a.ordnoa
                         FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                        WHERE r.div_part = i_div_part
                          AND r.rlse_ts = i_rlse_ts
                          AND rl.div_part = r.div_part
                          AND rl.rlse_id = r.rlse_id
                          AND rl.typ_id = 'LOAD'
                          AND ld.div_part = r.div_part
                          AND ld.llr_dt = r.llr_dt
                          AND ld.load_num = rl.val
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.div_part = ld.div_part
                          AND a.ipdtsa = 'DVC'
                          AND a.excptn_sw = 'N');

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END undo_vndr_cmp_sp;

  /*
  ||----------------------------------------------------------------------------
  || BACKOUT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/26/04 | rhalpai | Original
  || 08/03/06 | rhalpai | Updated to use OP_PARMS_PK when looking up parms.
  ||                    | Changed to include whsec in SQL for WHSP300C.
  ||                    | Changed to include start_ts when updating MCLANE_LOAD_LABEL_RLSE.
  ||                    | Added update to reset load status on LOAD_CLOS_CNTRL_BC2C.
  ||                    | PIR3593
  || 08/11/08 | rhalpai | Add update of header status from P to O if all details
  ||                    | are in unbilled status. PIR6364
  || 09/18/09 | rhalpai | Added logic to undo reassignment of dist orders to
  ||                    | alternate load. PIR7868
  || 01/22/10 | rhalpai | Added logic to call UNDO_VNDR_CMP_SP. PIR8216
  || 06/01/10 | rhalpai | Added logic to release lock of mainframe inventory
  ||                    | updates. PIR8537
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE backout_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                := 'OP_SET_RELEASE_PK.BACKOUT_SP';
    lar_parm             logs.tar_parm;
    l_rlse_id            NUMBER;
    l_llr_dt             DATE;
    l_llr_num            NUMBER;
    l_forc_inv_sw        rlse_op1z.forc_inv_sw%TYPE;
    l_rc                 VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get Release Info');

    SELECT r.rlse_id, r.llr_dt, r.forc_inv_sw
      INTO l_rlse_id, l_llr_dt, l_forc_inv_sw
      FROM rlse_op1z r
     WHERE r.div_part = i_div_part
       AND r.rlse_ts = i_rlse_ts
       AND r.stat_cd = 'P';

    l_llr_num := l_llr_dt - DATE '1900-02-28';
    logs.dbg('Reassign Distribution Orders to Alt Load');
    reassgn_loads_sp(i_div_part, i_rlse_ts, 'BACKOUT', 'Y');
    logs.dbg('Undo Vendor Compliance OrdQty Adjustments');
    undo_vndr_cmp_sp(i_div_part, i_rlse_ts);
    logs.dbg('Reset Tagged Orders');

    BEGIN
      UPDATE ordp120b b
         SET b.statb = 'O'
       WHERE b.statb IN('P', 'X')
         AND b.div_part = i_div_part
         AND b.ordnob IN(SELECT a.ordnoa
                           FROM rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                          WHERE rl.div_part = i_div_part
                            AND rl.rlse_id = l_rlse_id
                            AND rl.typ_id = 'LOAD'
                            AND ld.div_part = rl.div_part
                            AND ld.llr_dt = l_llr_dt
                            AND ld.load_num = rl.val
                            AND a.div_part = ld.div_part
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.stata = 'P');

      UPDATE ordp100a a
         SET a.stata = 'O'
       WHERE a.div_part = i_div_part
         AND a.stata = 'P'
         AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                    FROM rlse_log_op2z rl, load_depart_op1f ld
                                   WHERE rl.div_part = i_div_part
                                     AND rl.rlse_id = l_rlse_id
                                     AND rl.typ_id = 'LOAD'
                                     AND ld.div_part = rl.div_part
                                     AND ld.llr_dt = l_llr_dt
                                     AND ld.load_num = rl.val)
         AND NOT EXISTS(SELECT 1
                          FROM ordp120b b
                         WHERE b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa
                           AND b.statb NOT IN('O', 'I', 'S', 'C'));
    END;

    DECLARE
      l_t_ord_nums  type_ntab;
    BEGIN
      logs.dbg('Reset DIST order temp reassign flag');

      UPDATE    ordp120b b
            SET b.repckb = NULL
          WHERE b.repckb = 'Y'
            AND b.div_part = i_div_part
            AND b.statb = 'O'
            AND EXISTS(SELECT 1
                         FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                        WHERE r.div_part = i_div_part
                          AND r.rlse_ts = i_rlse_ts
                          AND rl.div_part = r.div_part
                          AND rl.rlse_id = r.rlse_id
                          AND rl.typ_id = 'LOAD'
                          AND ld.div_part = r.div_part
                          AND ld.llr_dt = r.llr_dt
                          AND ld.load_num = rl.val
                          AND a.div_part = ld.div_part
                          AND a.load_depart_sid = ld.load_depart_sid
                          AND a.ordnoa = b.ordnob)
      RETURNING         b.ordnob
      BULK COLLECT INTO l_t_ord_nums;

      IF l_t_ord_nums.COUNT > 0 THEN
        logs.dbg('Remove Duplicates');
        l_t_ord_nums := SET(l_t_ord_nums);
        logs.dbg('Resync TestBill DIST order');
        op_order_load_pk.syncload_sp(i_div_part, 'BACKOUT', l_t_ord_nums);
      END IF;   -- l_t_ord_nums.COUNT > 0
    END;

    logs.dbg('Remove Prepost entries');

    DELETE FROM prepost_load_op1p p
          WHERE p.div_part = i_div_part
            AND p.last_chg_ts = i_rlse_ts;

    logs.dbg('Enable Load Close');

    UPDATE load_clos_cntrl_bc2c lc
       SET lc.load_status = 'R'
     WHERE lc.div_part = i_div_part
       AND lc.llr_dt = l_llr_dt
       AND lc.load_num IN(SELECT rl.val
                            FROM rlse_log_op2z rl
                           WHERE rl.div_part = i_div_part
                             AND rl.rlse_id = l_rlse_id
                             AND rl.typ_id = 'LOAD')
       AND lc.load_status = 'P';

    UPDATE mclp370c c
       SET c.load_status = 'R'
     WHERE c.div_part = i_div_part
       AND c.llr_date = l_llr_num
       AND c.loadc IN(SELECT rl.val
                        FROM rlse_log_op2z rl
                       WHERE rl.div_part = i_div_part
                         AND rl.rlse_id = l_rlse_id
                         AND rl.typ_id = 'LOAD')
       AND c.load_status = 'P';

    logs.dbg('Reset Forced Inventory');

    IF l_forc_inv_sw = 'Y' THEN
      DECLARE
        l_forc_inv_amt     NUMBER;
        l_forc_applied_sw  VARCHAR2(1);
      BEGIN
        l_forc_inv_amt := NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_forc_inv_amt), 0);

        SELECT NVL(MAX('Y'), 'N')
          INTO l_forc_applied_sw
          FROM (SELECT COUNT(*) AS cnt, SUM((CASE
                                               WHEN qavc >= l_forc_inv_amt THEN 1
                                               ELSE 0
                                             END)) frc_cnt
                  FROM whsp300c
                 WHERE div_part = i_div_part) x
         WHERE x.cnt = x.frc_cnt;

        IF l_forc_applied_sw = 'Y' THEN
          UPDATE whsp300c
             SET qohc = qohc - l_forc_inv_amt,
                 qavc = qavc - l_forc_inv_amt
           WHERE div_part = i_div_part;
        END IF;   -- l_forced_applied = 'Y'
      END;
    END IF;   -- l_forc_inv_sw = 'Y'

    logs.dbg('Upd Status');

    UPDATE rlse_op1z r
       SET r.stat_cd = 'R'
     WHERE r.stat_cd = 'P'
       AND r.div_part = i_div_part
       AND r.rlse_ts = i_rlse_ts;

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END backout_sp;

  /*
  ||----------------------------------------------------------------------------
  || DELETE_STRATEGY_SP
  ||  Delete the specified Release Strategy from the system
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/03/06 | SNAGABH | Original
  || 10/14/17 | rhalpai | Add Div and CommitSw input parms.
  ||                    | Move logic from DEL_STRATEGY_SP and use CommitSw to
  ||                    | determine when to commit. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE delete_strategy_sp(
    i_div        IN  VARCHAR2,
    i_strtg_id   IN  NUMBER,
    i_commit_sw  IN  VARCHAR2 DEFAULT 'Y'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.DELETE_STRATEGY_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StrtgID', i_strtg_id);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Remove associated Strategy Attributes');

    DELETE FROM rlse_strtg_atrbt_op4s sa
          WHERE sa.div_part = l_div_part
            AND sa.strtg_id = i_strtg_id;

    logs.dbg('Remove the Strategy Information');

    DELETE FROM rlse_strtg_op4t s
          WHERE s.div_part = l_div_part
            AND s.strtg_id = i_strtg_id;

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END delete_strategy_sp;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_STRATEGY_SP
  ||  Procedure to Save a new Strategy or Update an existing one.
  ||
  ||  PARAMETERS:
  ||    i_div       - Division ID
  ||    i_strtg_nm  - Strategy Name entered (can contain blanks and some special characters)
  ||    i_load_list - Comma delimited list of loads
  ||    i_crp_list  - Comma delimited list of Corp Codes ('~' = All)
  ||    i_mfst_list - Comma delimited list of manifest categories ('~' = All)
  ||    i_strtg_id  - Null if Inserting a new Strategy or id of the Strategy being Updated
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/21/06 | SNAGABH | Original
  || 02/16/12 | rhalpai | Add Div, CorpList parms.
  ||                    | Remove AllStops, AllCategories, StopList parms.
  ||                    | Convert to use new strategy tables. PIR10845
  || 10/14/17 | rhalpai | Call DELETE_STRATEGY_SP with Div and CommitSw set to N.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_strategy_sp(
    i_div        IN      VARCHAR2,
    i_strtg_nm   IN      VARCHAR2,
    i_load_list  IN      VARCHAR2,
    i_crp_list   IN      VARCHAR2,
    i_mfst_list  IN      VARCHAR2,
    o_err_msg    OUT     VARCHAR2,
    i_strtg_id   IN      NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                   := 'OP_SET_RELEASE_PK.SAVE_STRATEGY_SP';
    lar_parm             logs.tar_parm;
    l_strtg_nm           rlse_strtg_op4t.strtg_nm%TYPE;
    l_exist_sw           VARCHAR2(1);
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
    l_e_dup_strtg_nm     EXCEPTION;
    l_e_miss_load        EXCEPTION;
    l_t_loads            type_stab;
    l_t_crps             type_stab;
    l_t_mfsts            type_stab;
    l_strtg_id           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StrtgNm', i_strtg_nm);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'StrtgID', i_strtg_id);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    l_strtg_nm := UPPER(TRIM(i_strtg_nm));

    IF l_strtg_nm IS NULL THEN
      o_err_msg := 'Insert failed. Strategy name required!';
    ELSE
      IF i_strtg_id IS NULL THEN
        logs.dbg('Check for Duplicate Strategy Name');

        OPEN l_cv
         FOR
           SELECT 'Y'
             FROM rlse_strtg_op4t s
            WHERE s.div_part = l_div_part
              AND s.strtg_nm = l_strtg_nm;

        FETCH l_cv
         INTO l_exist_sw;

        IF l_exist_sw = 'Y' THEN
          RAISE l_e_dup_strtg_nm;
        END IF;   -- l_exist_sw = 'Y'
      END IF;   -- i_strtg_id IS NULL

      logs.dbg('Parse Load List to table');
      l_t_loads := str.parse_list(i_load_list);

      IF l_t_loads.COUNT = 0 THEN
        RAISE l_e_miss_load;
      END IF;   -- l_t_loads.COUNT = 0

      logs.dbg('Parse Corp Code List to table');
      l_t_crps := str.parse_list(NVL(i_crp_list, '~'));
      logs.dbg('Parse Mfst Category List to table');
      l_t_mfsts := str.parse_list(NVL(i_mfst_list, '~'));

      IF i_strtg_id IS NOT NULL THEN
        -- First Delete the Strategy (if exists)
        logs.dbg('Delete Strategy');
        delete_strategy_sp(i_div, i_strtg_id, 'N');
        l_strtg_id := i_strtg_id;
      ELSE
        logs.dbg('Get Strategy ID');

        SELECT op4t_strtg_id_seq.NEXTVAL
          INTO l_strtg_id
          FROM DUAL;
      END IF;   -- i_strtg_id IS NOT NULL

      logs.dbg('Add Strategy Information');

      INSERT INTO rlse_strtg_op4t
                  (div_part, strtg_id, strtg_nm
                  )
           VALUES (l_div_part, l_strtg_id, l_strtg_nm
                  );

      logs.dbg('Insert Loads');
      FORALL i IN l_t_loads.FIRST .. l_t_loads.LAST
        INSERT INTO rlse_strtg_atrbt_op4s
                    (div_part, strtg_id, typ_id, val
                    )
             VALUES (l_div_part, l_strtg_id, 'LOAD', l_t_loads(i)
                    );
      logs.dbg('Insert Manifest Categories');
      FORALL i IN l_t_crps.FIRST .. l_t_crps.LAST
        INSERT INTO rlse_strtg_atrbt_op4s
                    (div_part, strtg_id, typ_id, val
                    )
             VALUES (l_div_part, l_strtg_id, 'CORP', l_t_crps(i)
                    );
      logs.dbg('Insert Manifest Categories');
      FORALL i IN l_t_mfsts.FIRST .. l_t_mfsts.LAST
        INSERT INTO rlse_strtg_atrbt_op4s
                    (div_part, strtg_id, typ_id, val
                    )
             VALUES (l_div_part, l_strtg_id, 'MFST', l_t_mfsts(i)
                    );
      COMMIT;
    END IF;   -- l_strtg_nm IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_dup_strtg_nm THEN
      o_err_msg := 'Insert failed. Strategy name [' || l_strtg_nm || '] already exists!';
    WHEN l_e_miss_load THEN
      o_err_msg := 'Insert failed. Load required!';
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END save_strategy_sp;

  /*
  ||----------------------------------------------------------------------------
  || VALIDATE_RLSE_SP
  ||  Perform validation for Release Allocation process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Original. PIR3593
  || 10/31/07 | rhalpai | Added check for allocation release for order before
  ||                    | product receipt of a Strict Item. PIR5002
  || 01/15/08 | rhalpai | Added check for multiple ETA dates/times. IM370449
  || 06/18/08 | rhalpai | Added StopList/MfstList parms to call to
  ||                    | CHECK_REQUIRED_PARMS_FN. Added warning check for open
  ||                    | cig forecast. PIR6019
  || 04/13/09 | rhalpai | Added call to new MISSING_WAVE_PLAN_FN in error
  ||                    | validation and added call to LONE_DIST_FN in warning
  ||                    | validation. PIR7118
  || 06/05/09 | rhalpai | Added call to new WAVE_PLAN_STOP_CNT_CHK_FN in warning
  ||                    | validation. PIR7118
  || 05/20/10 | rhalpai | Added logic to check for mainframe inventory updates
  ||                    | processing in error validation. PIR8377
  || 07/28/10 | rhalpai | Moved check for open load for different LLR date from
  ||                    | warning validation to error validation. PIR8689
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 11/28/11 | rhalpai | Add validation call to new TEST_CUST_ON_LIVE_RLS_FN
  ||                    | for live production release. PIR10211
  || 02/15/12 | rhalpai | Add ProcessControl validation for RLSE_COMPL.
  ||                    | IM-043061
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 02/12/13 | rhalpai | Add logic to validate for Subscription Order errors
  ||                    | for sufficient inventory and min price. PIR12239
  || 10/03/13 | rhalpai | Remove validations for MultCustForLoadStop,
  ||                    | MultStopsForCustOnLoad, MultEtaForLoadStop since these
  ||                    | are now handled by table design. PIR11038
  || 11/04/13 | rhalpai | Add validation check for multiple customers assigned
  ||                    | to same LLR/Load/Stop. IM-123463
  || 03/12/14 | rhalpai | Add validation error check to ensure Mass Reprice has
  ||                    | run recently. PIR13614
  || 02/25/15 | rhalpai | Change IS_RESTRICTED_FN logic to replace SELECT from
  ||                    | PRCS_CNTL tables with call to
  ||                    | OP_PROCESS_CONTROL_PK.IS_RESTRICTED_FN with LOAD_CLOSE
  ||                    | in exclusion list parm. PIR11038
  || 10/14/17 | rhalpai | Add div_part in calls to OP_ANALYZE_BY_PARM_SP,
  ||                    | MULT_CUST_FOR_LD_STOP_FN, RELEASED_LOAD_LIST_FN,
  ||                    | SBSCRPTN_ORD_INV_FN, SBSCRPTN_ORD_PRICE_FN,
  ||                    | MISSING_WAVE_PLAN_FN, TEST_CUST_ON_LIVE_RLS_FN,
  ||                    | TEST_ORDS_ON_LIVE_RLS_FN, TBILL_WITH_RELEASED_ORDS_FN,
  ||                    | OP_PROCESS_CONTROL_PK.RESTRICTED_MSG_FN,
  ||                    | MASS_REPRICE_REQD_FN, RELEASED_PRE_POST_CUST_FN,
  ||                    | PRE_POST_CUST_LIST_FN, RLSE_STRCT_B4_PROD_RCPT_FN,
  ||                    | LONE_DIST_FN, WAVE_PLAN_STOP_CNT_CHK_FN,
  ||                    | MAINTAINED_ORDS_FN. PIR15427
  || 12/05/17 | rhalpai | Add validation error check to ensure a non-exception
  ||                    | order exists. SDHD-224907
  || 06/14/19 | rhalpai | Add warning validation call for ACS_CLOS_FN (load
  ||                    | dispositions already received). SDHD-499897
  || 11/23/22 | rhalpai | Add error restriction for order source outside of restricted load range. (i.e.: ECOM XPR/DSV) 00000021755
  || 12/14/24 | rhalpai | Add error msg when an ECOM (E or S load) in release already exists with dispositions already received (ACSCloseSw = Y). SDHD-2117680.
  || 11/26/25 | rhalpai | Add error msg when an ECOM (E or S) load is in release with a non-ECOM (non E or S) load. SDHD-2507231
  ||----------------------------------------------------------------------------
  */
  PROCEDURE validate_rlse_sp(
    i_div          IN      VARCHAR2,
    i_llr_dt       IN      VARCHAR2,
    i_load_list    IN      VARCHAR2,
    i_crp_list     IN      VARCHAR2,
    i_mfst_list    IN      VARCHAR2,
    i_test_bil_cd  IN      VARCHAR2,
    o_warn_msg     OUT     VARCHAR2,
    o_err_msg      OUT     VARCHAR2
  ) IS
    l_c_module            CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.VALIDATE_RLSE_SP';
    lar_parm                       logs.tar_parm;
    l_llr_dt                       DATE;
    l_div_part                     NUMBER;
    l_is_test_bill                 BOOLEAN       :=(NVL(i_test_bil_cd, '~') <> '~');
    l_c_input_delimiter   CONSTANT VARCHAR2(1)   := ',';
    l_c_output_delimiter  CONSTANT VARCHAR2(1)   := '~';
    l_c_loads_per_ln      CONSTANT PLS_INTEGER   := 10;

    FUNCTION is_restricted_fn(
      i_prcs_id  IN  VARCHAR2
    )
      RETURN BOOLEAN IS
      l_is_restricted  BOOLEAN;
    BEGIN
      l_is_restricted := op_process_control_pk.is_restricted_fn(i_prcs_id, l_div_part, op_const_pk.prcs_load_clos);
      RETURN(l_is_restricted);
    END is_restricted_fn;

    PROCEDURE add_msg_sp(
      i_new_msg  IN  VARCHAR2
    ) IS
    BEGIN
      IF i_new_msg IS NOT NULL THEN
        o_warn_msg := o_warn_msg || cnst.newline_char || l_c_output_delimiter || i_new_msg;
      END IF;
    END add_msg_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'TestBilCd', i_test_bil_cd);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Check required parms');
    o_err_msg := check_required_parms_fn(i_div, i_llr_dt, i_load_list, i_crp_list, i_mfst_list, l_c_input_delimiter);

    IF o_err_msg IS NULL THEN
      logs.dbg('Initialize');
      l_div_part := div_pk.div_part_fn(i_div);
      l_llr_dt := TO_DATE(i_llr_dt, g_c_dt_fmt);
      logs.dbg('Load temp table with Load/Stop/Mfst lists');
      populate_temp_sp(i_load_list, i_crp_list, i_mfst_list);
      logs.dbg('Analyze Tables');
      op_analyze_by_parm_sp(l_div_part, 'ANLYZ_VALID_RLSE');
      logs.dbg('Check for Errors');
      o_err_msg :=
        COALESCE
          (ord_to_rlse_fn(l_div_part, l_llr_dt),
           mult_cust_for_ld_stop_fn(l_div_part, l_llr_dt),
           released_load_list_fn(l_div_part, l_llr_dt),
           sbscrptn_ord_inv_fn(l_div_part, l_llr_dt),
           sbscrptn_ord_price_fn(l_div_part, l_llr_dt),
           (CASE
              WHEN NOT l_is_test_bill THEN missing_wave_plan_fn(l_div_part, l_llr_dt)
            END
           ),
           (CASE
              WHEN NOT l_is_test_bill THEN test_cust_on_live_rls_fn(l_div_part, l_llr_dt)
            END),
           (CASE
              WHEN NOT l_is_test_bill THEN test_ords_on_live_rls_fn(l_div_part, l_llr_dt)
            END),
           (CASE
              WHEN l_is_test_bill THEN tbill_with_released_ords_fn(l_div_part, l_llr_dt)
            END),
           last_rlse_fn(l_div_part),
           (CASE
              WHEN NOT l_is_test_bill THEN mass_reprice_reqd_fn(l_div_part)
            END),
           rstr_load_range_fn(l_div_part, l_llr_dt),
           mix_ecom_nonecom_load_fn(l_div_part),
           acs_clos_ecom_fn(l_div_part),
           (CASE
              WHEN is_restricted_fn(op_const_pk.prcs_rlse_compl) THEN op_process_control_pk.restricted_msg_fn
                                                                                           (op_const_pk.prcs_rlse_compl,
                                                                                            l_div_part
                                                                                           )
            END
           ),
           (CASE
              WHEN is_restricted_fn(op_const_pk.prcs_set_rlse) THEN op_process_control_pk.restricted_msg_fn
                                                                                             (op_const_pk.prcs_set_rlse,
                                                                                              l_div_part
                                                                                             )
            END
           )
          );

      -- Create Warning Messages
      IF o_err_msg IS NULL THEN
        o_warn_msg := 'The following load(s) will be released ('
                      ||(CASE
                           WHEN l_is_test_bill THEN 'TEST-BILL'
                           ELSE 'PRODUCTION'
                         END)
                      || ') for LLR date = '
                      || i_llr_dt
                      || cnst.newline_char
                      || wrap_text_fn(clean_list_fn(i_load_list), 5 * l_c_loads_per_ln);

        IF NOT l_is_test_bill THEN
          logs.dbg('Check for Open Transaction in Cig Fcst');
          add_msg_sp(open_cig_fcst_fn(i_div));
          logs.dbg('Check for Released Pre-Post Customers');
          add_msg_sp(released_pre_post_cust_fn(l_div_part, l_llr_dt));

          IF i_mfst_list IS NOT NULL THEN
            logs.dbg('All Categories included for Pre-Post Customers');
            add_msg_sp(pre_post_cust_list_fn(l_div_part, l_llr_dt));
          END IF;   -- i_mfst_list IS NOT NULL

          logs.dbg('Check for Strict Items Before Product Receipt');
          add_msg_sp(rlse_strct_b4_prod_rcpt_fn(l_div_part, l_llr_dt));
          logs.dbg('Check for Distributions without Reg Orders');
          add_msg_sp(lone_dist_fn(l_div_part, l_llr_dt));
          logs.dbg('Check for WavePlan Loads with StopCnt Changes');
          add_msg_sp(wave_plan_stop_cnt_chk_fn(l_div_part, l_llr_dt));
          logs.dbg('Check for ACS Load Dispositions Already Received');
          add_msg_sp(acs_clos_fn(l_div_part));
        END IF;   -- NOT l_is_test_bill

        logs.dbg('Check for Maintained Orders');
        add_msg_sp(maintained_ords_fn(l_div_part, l_llr_dt));
        logs.dbg('User Prompt');
        add_msg_sp('Are you sure?');
      END IF;   -- o_err_msg IS NULL
    END IF;   -- o_err_msg IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END validate_rlse_sp;

  /*
  ||----------------------------------------------------------------------------
  || RLSE_SP
  ||  Release allocation for processing.
  ||  Tag orders for allocation and start the allocation background process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/06 | rhalpai | Rewritten from OP_SET_STATUS_INPROCESS_FN. PIR3593
  || 05/12/08 | rhalpai | Add second call to tag_orders_sp. IM409689
  || 06/18/08 | rhalpai | Added StopList/MfstList parms to call to
  ||                    | CHECK_REQUIRED_PARMS_FN. Changed add_release_entry_sp
  ||                    | to include new columns for MCLANE_LOAD_LABEL_RLSE.
  ||                    | PIR6019
  || 08/11/08 | rhalpai | Added update of header status from O to P. PIR6364
  || 04/13/09 | rhalpai | Added logic to build and ftp LoadSeq file to
  ||                    | mainframe when div is set up for wave plan. PIR7118
  || 05/20/09 | rhalpai | Changed update to include order detail status X when
  ||                    | tagging order header status. IM506029
  || 09/18/09 | rhalpai | Added logic to reassign dist orders to alternate load.
  ||                    | PIR7868
  || 01/22/10 | rhalpai | Added logic to call VNDR_CMP_SP. PIR8216
  || 06/01/10 | rhalpai | Added logic to update event log during release process.
  ||                    | Added logic to the end of the process to either make
  ||                    | a call to start allocation for TestBills or make a
  ||                    | call to refresh inventory for Production runs. The
  ||                    | logic in the inventory refresh will automatically
  ||                    | start the allocation when its Finished-Msg is
  ||                    | processed. PIR8377
  ||                    | Added logic to lock out mainframe inventory updates
  ||                    | during Release. PIR8537
  || 10/13/10 | rhalpai | Change logic to include pgm_id in call to REFRESH_INV_SP
  ||                    | and inactivate RLSE_ALLOC ProcessControl when the
  ||                    | release will not continue. PIR8531
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 12/27/10 | rhalpai | Change cursor in REASSIGN_DIST_SP to bring back only
  ||                    | one entry per Dist Order found. IM637961
  ||                    | Change DEL_RLSE_ENTRY_SP to remove entries from
  ||                    | rlse_log_op2z and then from rlse_op1z for current
  ||                    | release. IM635811
  || 04/20/11 | rhalpai | Change logic to prevent releasing orders with order
  ||                    | sources that should not bill alone unless another reg
  ||                    | order for the customer with an order source without
  ||                    | this restriction is present. PIR9910
  || 11/17/11 | rhalpai | Add logic to turn OFF RLSE_COMPL Process Control
  ||                    | whenever RLSE_ALLOC is turned OFF. IM-033180
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 10/01/12 | rhalpai | Add logic to remove any distributions with delete
  ||                    | requests. PIR5250
  || 11/29/12 | rhalpai | Remove logic to recycle dist deletes. IM-074192
  || 03/02/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 09/03/15 | rhalpai | Add logic to TAG_ORDS_SP to subtract the count of
  ||                    | order lines updated to X status from ord_ln_cnt when
  ||                    | ord_ln_cnt > 0. This will prevent releasing with only
  ||                    | orders that do not allocate (TXDOCK). IM-310964
  || 10/14/17 | rhalpai | Change to call CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE.
  ||                    | Change to use constants package OP_CONST_PK.
  ||                    | Add div_part in calls to OP_ANALYZE_BY_PARM_SP,
  ||                    | OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP.
  ||                    | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  || 09/02/20 | rhalpai | Correct SQL in ReassignDist logic. SDHD-766471
  || 01/27/26 | rhalpai | Add logic to fix Ords with Mult LoadDepartSid for same Div/LLRDt/Load. SDHD-2579786
  ||----------------------------------------------------------------------------
  */
  PROCEDURE rlse_sp(
    i_div          IN  VARCHAR2,
    i_strtg_id     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_llr_dt       IN  VARCHAR2,
    i_load_list    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_mfst_list    IN  VARCHAR2,
    i_test_bil_cd  IN  VARCHAR2,
    i_forc_inv_sw  IN  VARCHAR2 DEFAULT 'N',
    i_evnt_que_id  IN  NUMBER,
    i_cycl_id      IN  NUMBER,
    i_cycl_dfn_id  IN  NUMBER
  ) IS
    l_c_module           CONSTANT typ.t_maxfqnm                     := 'OP_SET_RELEASE_PK.RLSE_SP';
    lar_parm                      logs.tar_parm;
    l_section                     VARCHAR2(80)                      := 'Initial';
    l_div_part                    NUMBER;
    l_msg                         typ.t_maxvc2;
    l_c_input_delimiter  CONSTANT VARCHAR2(1)                       := ',';
    l_prcs_id                     prcs_cntl_dfn_cn1p.prcs_id%TYPE;
    l_test_bil_cd                 rlse_op1z.test_bil_cd%TYPE        := NVL(i_test_bil_cd, '~');
    l_strtg_id                    NUMBER;
    l_llr_dt                      DATE;
    l_load_list                   typ.t_maxvc2;
    l_crp_list                    typ.t_maxvc2;
    l_mfst_list                   typ.t_maxvc2;
    l_refresh_inv_sw              VARCHAR2(1);
    l_rlse_ts                     DATE                              := SYSDATE;
    l_rlse_id                     NUMBER;
    l_ord_ln_cnt                  PLS_INTEGER                       := 0;
    l_c_all_ords         CONSTANT VARCHAR2(1)                       := 'A';
    l_is_test_bil                 BOOLEAN                           :=(l_test_bil_cd <> '~');
    l_is_forc_inv                 BOOLEAN                          :=(    l_is_test_bil
                                                                      AND NVL(i_forc_inv_sw, 'N') <> 'N');
    l_is_ok_to_alloc              BOOLEAN                           := FALSE;
    l_rc                          VARCHAR2(1);

    PROCEDURE lock_op1z_sp IS
      l_cv  SYS_REFCURSOR;
    BEGIN
      OPEN l_cv
       FOR
         SELECT     r.ROWID
               FROM rlse_op1z r
              WHERE r.div_part = l_div_part
         FOR UPDATE NOWAIT;
    END lock_op1z_sp;

    PROCEDURE lock_op1p_sp IS
      l_cv  SYS_REFCURSOR;
    BEGIN
      OPEN l_cv
       FOR
         SELECT     p.ROWID
               FROM prepost_load_op1p p
              WHERE p.div_part = l_div_part
         FOR UPDATE;
    END lock_op1p_sp;

    PROCEDURE upd_evnt_log_sp(
      i_evnt_msg   IN  VARCHAR2,
      i_finish_cd  IN  NUMBER DEFAULT 0
    ) IS
    BEGIN
      cig_event_mgr_pk.update_log_message(i_evnt_que_id, i_cycl_id, i_cycl_dfn_id, i_evnt_msg, i_finish_cd);
    END upd_evnt_log_sp;

    PROCEDURE add_rlse_entry_sp IS
      l_t_loads  type_stab;
      l_t_crps   type_stab;
      l_t_mfsts  type_stab;
    BEGIN
      INSERT INTO rlse_op1z
                  (div_part, rlse_ts, ord_ln_cnt, stat_cd, llr_dt, test_bil_cd,
                   forc_inv_sw, strtg_id, user_id
                  )
           VALUES (l_div_part, l_rlse_ts, 0, 'P', l_llr_dt, l_test_bil_cd,
                   DECODE(i_forc_inv_sw, 'N', 'N', NULL, 'N', 'Y'), l_strtg_id, i_user_id
                  )
        RETURNING rlse_id
             INTO l_rlse_id;

      l_t_loads := str.parse_list(l_load_list);
      l_t_crps := str.parse_list(l_crp_list);
      l_t_mfsts := str.parse_list(l_mfst_list);

      INSERT INTO rlse_log_op2z
                  (div_part, rlse_id, typ_id, val)
        SELECT l_div_part, l_rlse_id, 'SETRLSE', '~'
          FROM DUAL
        UNION ALL
        SELECT l_div_part, l_rlse_id, 'LOAD', t.column_value
          FROM TABLE(CAST(l_t_loads AS type_stab)) t
        UNION ALL
        SELECT l_div_part, l_rlse_id, 'CORP', t.column_value
          FROM TABLE(CAST(l_t_crps AS type_stab)) t
        UNION ALL
        SELECT l_div_part, l_rlse_id, 'MFST', t.column_value
          FROM TABLE(CAST(l_t_mfsts AS type_stab)) t;
    END add_rlse_entry_sp;

    PROCEDURE disable_load_clos_sp IS
    BEGIN
      UPDATE load_clos_cntrl_bc2c lc
         SET lc.load_status = 'P'
       WHERE lc.div_part = l_div_part
         AND lc.llr_dt = l_llr_dt
         AND lc.load_num IN(SELECT rl.val
                              FROM rlse_log_op2z rl
                             WHERE rl.div_part = l_div_part
                               AND rl.rlse_id = l_rlse_id
                               AND rl.typ_id = 'LOAD')
         AND lc.load_status = 'R';
    END disable_load_clos_sp;

    PROCEDURE forc_parallel_sp(
      i_enable  IN  BOOLEAN
    ) IS
    BEGIN
      IF i_enable THEN
        EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL QUERY';
      ELSE
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
      END IF;   -- i_enable
    END forc_parallel_sp;

    PROCEDURE tag_ords_sp IS
      l_bsr_cnt  PLS_INTEGER;
    BEGIN
      UPDATE ordp120b b
         SET b.statb =(CASE
                         WHEN EXISTS(SELECT 1
                                       FROM ordp100a a, sub_prcs_ord_src s
                                      WHERE a.div_part = b.div_part
                                        AND a.ordnoa = b.ordnob
                                        AND s.div_part = a.div_part
                                        AND s.prcs_id = 'SET RELEASE'
                                        AND s.prcs_sbtyp_cd = 'BSR'
                                        AND s.ord_src = a.ipdtsa) THEN 'X'
                         ELSE 'P'
                       END
                      )
       WHERE b.statb = 'O'
         AND b.div_part = l_div_part
         AND b.ordnob IN(SELECT a.ordnoa
                           FROM ordp100a a
                          WHERE a.div_part = l_div_part
                            AND (   l_test_bil_cd IN('A', a.dsorda)
                                 OR (    l_test_bil_cd = '~'
                                     AND (a.load_depart_sid, a.custa) NOT IN(
                                           SELECT se.load_depart_sid, se.cust_id
                                             FROM load_depart_op1f ld, stop_eta_op1g se, prepost_load_op1p p,
                                                  sysp200c c
                                            WHERE ld.div_part = l_div_part
                                              AND ld.llr_dt = l_llr_dt
                                              AND ld.load_num IN(
                                                    SELECT rl.val
                                                      FROM rlse_log_op2z rl
                                                     WHERE rl.rlse_id = l_rlse_id
                                                       AND rl.div_part = l_div_part
                                                       AND rl.typ_id = 'LOAD')
                                              AND se.div_part = ld.div_part
                                              AND se.load_depart_sid = ld.load_depart_sid
                                              AND p.div_part = ld.div_part
                                              AND p.load_num = ld.load_num
                                              AND p.stop_num = se.stop_num
                                              AND p.cust_num = se.cust_id
                                              AND p.llr_date = l_llr_dt
                                              AND c.div_part = se.div_part
                                              AND c.acnoc = se.cust_id
                                              AND c.tclscc = 'PRP')
                                    )
                                )
                            AND (a.load_depart_sid, a.custa) IN(
                                  SELECT se.load_depart_sid, se.cust_id
                                    FROM stop_eta_op1g se
                                   WHERE se.div_part = l_div_part
                                     AND se.load_depart_sid IN(
                                           SELECT ld.load_depart_sid
                                             FROM rlse_log_op2z rl, load_depart_op1f ld
                                            WHERE rl.div_part = l_div_part
                                              AND rl.rlse_id = l_rlse_id
                                              AND rl.typ_id = 'LOAD'
                                              AND ld.div_part = l_div_part
                                              AND ld.llr_dt = l_llr_dt
                                              AND ld.load_num = rl.val)
                                     AND (   l_crp_list IS NULL
                                          OR se.cust_id IN(SELECT cx.custb
                                                             FROM mclp020b cx, rlse_log_op2z rl
                                                            WHERE rl.div_part = l_div_part
                                                              AND rl.rlse_id = l_rlse_id
                                                              AND rl.typ_id = 'CORP'
                                                              AND cx.div_part = rl.div_part
                                                              AND cx.corpb = TO_NUMBER(rl.val))
                                         )))
         AND (   EXISTS(SELECT 1
                          FROM ordp100a a, sysp200c c
                         WHERE a.div_part = l_div_part
                           AND a.ordnoa = b.ordnob
                           AND c.div_part = a.div_part
                           AND c.acnoc = a.custa
                           AND c.tclscc = 'PRP')
              OR l_mfst_list IS NULL
              OR b.manctb IN(SELECT rl.val
                               FROM rlse_log_op2z rl
                              WHERE rl.div_part = l_div_part
                                AND rl.rlse_id = l_rlse_id
                                AND rl.typ_id = 'MFST')
             );

      l_ord_ln_cnt := l_ord_ln_cnt + SQL%ROWCOUNT;

      IF SQL%ROWCOUNT > 0 THEN
        SELECT COUNT(*)
          INTO l_bsr_cnt
          FROM ordp120b b
         WHERE b.div_part = l_div_part
           AND b.statb = 'X';

        l_ord_ln_cnt := l_ord_ln_cnt - l_bsr_cnt;
      END IF;   -- SQL%ROWCOUNT > 0

      IF NOT l_is_test_bil THEN
        -- untag orders with order sources that should not bill alone
        UPDATE ordp120b b
           SET b.statb = 'O'
         WHERE b.statb = 'P'
           AND b.div_part = l_div_part
           AND b.ordnob IN(SELECT a.ordnoa
                             FROM sub_prcs_ord_src s, ordp100a a
                            WHERE s.div_part = l_div_part
                              AND s.prcs_id = 'SET RELEASE'
                              AND s.prcs_sbtyp_cd = 'NAO'
                              AND a.div_part = s.div_part
                              AND a.ipdtsa = s.ord_src
                              AND (a.load_depart_sid, a.custa) IN(
                                    SELECT se.load_depart_sid, se.cust_id
                                      FROM stop_eta_op1g se
                                     WHERE se.div_part = l_div_part
                                       AND se.load_depart_sid IN(
                                             SELECT ld.load_depart_sid
                                               FROM rlse_log_op2z rl, load_depart_op1f ld
                                              WHERE rl.rlse_id = l_rlse_id
                                                AND rl.typ_id = 'LOAD'
                                                AND ld.div_part = rl.div_part
                                                AND ld.div_part = l_div_part
                                                AND ld.llr_dt = l_llr_dt
                                                AND ld.load_num = rl.val)
                                       AND (   l_crp_list IS NULL
                                            OR se.cust_id IN(
                                                 SELECT cx.custb
                                                   FROM mclp020b cx, rlse_log_op2z rl
                                                  WHERE rl.div_part = l_div_part
                                                    AND rl.rlse_id = l_rlse_id
                                                    AND rl.typ_id = 'CORP'
                                                    AND cx.div_part = rl.div_part
                                                    AND cx.corpb = TO_NUMBER(rl.val))
                                           ))
                              AND EXISTS(SELECT 1
                                           FROM ordp120b b2
                                          WHERE b2.div_part = l_div_part
                                            AND b2.ordnob = a.ordnoa
                                            AND b2.statb = 'P')
                              AND NOT EXISTS(SELECT 1
                                               FROM ordp100a a2
                                              WHERE a2.div_part = l_div_part
                                                AND a2.load_depart_sid = a.load_depart_sid
                                                AND a2.custa = a.custa
                                                AND a2.dsorda = 'R'
                                                AND (   a2.stata IN('P', 'R')
                                                     OR EXISTS(
                                                          SELECT 1
                                                            FROM ordp120b b
                                                           WHERE b.div_part = a2.div_part
                                                             AND b.ordnob = a2.ordnoa
                                                             AND b.statb = 'P')
                                                    )
                                                AND a2.ipdtsa NOT IN(
                                                      SELECT s2.ord_src
                                                        FROM sub_prcs_ord_src s2
                                                       WHERE s2.div_part = l_div_part
                                                         AND s2.prcs_id = 'SET RELEASE'
                                                         AND s2.prcs_sbtyp_cd IN('BSR', 'NAO'))));

        l_ord_ln_cnt := l_ord_ln_cnt - SQL%ROWCOUNT;
      END IF;   -- NOT l_is_test_bil
    END tag_ords_sp;

    PROCEDURE forc_inv_sp IS
      l_forc_inv_amt  NUMBER;
    BEGIN
      l_forc_inv_amt := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_forc_inv_amt), 0);

      UPDATE whsp300c
         SET qohc = qohc + l_forc_inv_amt,
             qavc = qavc + l_forc_inv_amt
       WHERE div_part = l_div_part;
    END forc_inv_sp;

    PROCEDURE reassign_dist_sp IS
      ----------------------------------------------------------------
      -- Temporarily reassign orders on DIST load with eta date
      -- less than or equal to the max eta date on customer's order.
      -- These orders will be put back after the backout runs at the
      -- end of the Test Bill Process.
      ----------------------------------------------------------------
      TYPE l_rt_load_ords IS RECORD(
        load_depart_sid  NUMBER,
        cust_id          sysp200c.acnoc%TYPE,
        stop_num         NUMBER,
        eta_ts           DATE,
        t_ord_nums       type_ntab
      );

      TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

      l_t_load_ords  l_tt_load_ords;
    BEGIN
      logs.dbg('Get Order Load Info');

      SELECT   ld.load_depart_sid,
               se.cust_id,
               se.stop_num,
               se.eta_ts,
               CAST(MULTISET(SELECT a2.ordnoa
                               FROM load_depart_op1f ld2, ordp100a a2
                              WHERE ld2.div_part = l_div_part
                                AND ld2.llr_ts = DATE '1900-01-01'
                                AND ld2.load_num = 'DIST'
                                AND a2.div_part = ld2.div_part
                                AND a2.load_depart_sid = ld2.load_depart_sid
                                AND a2.custa = se.cust_id
                                AND a2.shpja <= TRUNC(se.eta_ts) - DATE '1900-02-28'
                                AND a2.excptn_sw = 'N'
                                AND a2.dsorda = 'D'
                                AND a2.stata = 'O'
                                AND EXISTS(SELECT 1
                                             FROM ordp100a a
                                            WHERE a.div_part = se.div_part
                                              AND a.load_depart_sid = se.load_depart_sid
                                              AND a.custa = a.custa
                                              AND a.ldtypa = a2.ldtypa
                                              AND a.excptn_sw = 'N'
                                              AND a.dsorda IN('R', 'T')
                                              AND a.stata IN('O', 'P')
                                              AND EXISTS(SELECT 1
                                                           FROM ordp120b b
                                                          WHERE b.div_part = a.div_part
                                                            AND b.ordnob = a.ordnoa
                                                            AND b.statb = 'P'))
                            ) AS type_ntab
                   ) AS ord_nums
      BULK COLLECT INTO l_t_load_ords
          FROM rlse_log_op2z rl, load_depart_op1f ld, mclp020b cx, stop_eta_op1g se
         WHERE rl.div_part = l_div_part
           AND rl.rlse_id = l_rlse_id
           AND rl.typ_id IN('LOAD', 'CORP')
           AND ld.div_part = rl.div_part
           AND ld.llr_dt = l_llr_dt
           AND ld.load_num = DECODE(rl.typ_id, 'LOAD', rl.val)
           AND cx.div_part = se.div_part
           AND (   l_crp_list IS NULL
                OR cx.corpb = DECODE(rl.typ_id, 'CORP', rl.val))
           AND se.div_part = ld.div_part
           AND se.load_depart_sid = ld.load_depart_sid
           AND se.cust_id = cx.custb
           AND EXISTS(SELECT 1
                        FROM ordp100a a
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.excptn_sw = 'N'
                         AND a.stata IN('O', 'P')
                         AND EXISTS(SELECT 1
                                      FROM ordp120b b
                                     WHERE b.div_part = a.div_part
                                       AND b.ordnob = a.ordnoa
                                       AND b.statb = 'P'))
           AND EXISTS(SELECT 1
                        FROM ordp100a a, load_depart_op1f ld2, ordp100a a2
                       WHERE a.div_part = se.div_part
                         AND a.load_depart_sid = se.load_depart_sid
                         AND a.custa = se.cust_id
                         AND a.excptn_sw = 'N'
                         AND a.dsorda IN('R', 'T')
                         AND a.stata IN('O', 'P')
                         AND EXISTS(SELECT 1
                                      FROM ordp120b b
                                     WHERE b.div_part = a.div_part
                                       AND b.ordnob = a.ordnoa
                                       AND b.statb = 'P')
                         AND ld2.div_part = l_div_part
                         AND ld2.llr_ts = DATE '1900-01-01'
                         AND ld2.load_num = 'DIST'
                         AND a2.div_part = se.div_part
                         AND a2.load_depart_sid = ld2.load_depart_sid
                         AND a2.custa = se.cust_id
                         AND a2.shpja <= TRUNC(se.eta_ts) - DATE '1900-02-28'
                         AND a2.ldtypa = a.ldtypa
                         AND a2.excptn_sw = 'N'
                         AND a2.dsorda = 'D'
                         AND a2.stata = 'O')
      ORDER BY se.load_depart_sid, se.cust_id;

      IF l_t_load_ords.COUNT > 0 THEN
        FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
          logs.dbg('Move Orders');
          op_order_load_pk.move_ords_sp(l_div_part,
                                        l_t_load_ords(i).cust_id,
                                        l_t_load_ords(i).load_depart_sid,
                                        l_t_load_ords(i).stop_num,
                                        l_t_load_ords(i).eta_ts,
                                        DATE '1900-01-01',
                                        'DIST',
                                        DATE '1900-01-01',
                                        0,
                                        DATE '1900-01-01',
                                        'TESTBILL',
                                        'SETRLSE',
                                        l_t_load_ords(i).t_ord_nums
                                       );
          logs.dbg('Set Status and Flag Order to be Reset to DIST Load');
          FORALL j IN l_t_load_ords(i).t_ord_nums.FIRST .. l_t_load_ords(i).t_ord_nums.LAST
            UPDATE ordp120b b
               SET b.statb = 'P',
                   b.repckb = 'Y'
             WHERE b.div_part = l_div_part
               AND b.ordnob = l_t_load_ords(i).t_ord_nums(j);
          l_ord_ln_cnt := l_ord_ln_cnt + SQL%ROWCOUNT;
        END LOOP;
      END IF;   -- l_t_load_ords.COUNT > 0
    END reassign_dist_sp;

    PROCEDURE ins_prepost_entries_sp IS
    BEGIN
      INSERT INTO prepost_load_op1p
                  (div_part, load_num, stop_num, cust_num, llr_date, last_chg_ts)
        SELECT   l_div_part, ld.load_num, se.stop_num, c.acnoc, l_llr_dt, l_rlse_ts
            FROM rlse_log_op2z rl, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, sysp200c c, mclp020b cx
           WHERE rl.div_part = l_div_part
             AND rl.rlse_id = l_rlse_id
             AND rl.typ_id IN('LOAD', 'CORP')
             AND ld.div_part = l_div_part
             AND ld.llr_dt = l_llr_dt
             AND ld.load_num = DECODE(rl.typ_id, 'LOAD', rl.val)
             AND a.div_part = ld.div_part
             AND a.load_depart_sid = ld.load_depart_sid
             AND a.stata IN('O', 'P')
             AND se.div_part = a.div_part
             AND se.load_depart_sid = a.load_depart_sid
             AND se.cust_id = a.custa
             AND cx.div_part = a.div_part
             AND cx.custb = a.custa
             AND (   l_crp_list IS NULL
                  OR cx.corpb = DECODE(rl.typ_id, 'CORP', rl.val))
             AND c.div_part = a.div_part
             AND c.acnoc = a.custa
             AND c.tclscc = 'PRP'
             AND EXISTS(SELECT 1
                          FROM ordp120b b
                         WHERE b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa
                           AND b.statb = 'P')
             AND NOT EXISTS(SELECT 1
                              FROM prepost_load_op1p p
                             WHERE p.div_part = ld.div_part
                               AND p.load_num = ld.load_num
                               AND p.stop_num = se.stop_num
                               AND p.cust_num = se.cust_id
                               AND p.llr_date = l_llr_dt)
        GROUP BY ld.load_num, se.stop_num, c.acnoc;
    END ins_prepost_entries_sp;

    PROCEDURE upd_ord_hdrs_sp IS
    BEGIN
      UPDATE ordp100a a
         SET a.stata = 'P'
       WHERE a.stata = 'O'
         AND a.div_part = l_div_part
         AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                    FROM rlse_log_op2z rl, load_depart_op1f ld
                                   WHERE rl.div_part = l_div_part
                                     AND rl.rlse_id = l_rlse_id
                                     AND ld.div_part = l_div_part
                                     AND ld.llr_dt = l_llr_dt
                                     AND ld.load_num = rl.val)
         AND EXISTS(SELECT 1
                      FROM ordp120b b
                     WHERE b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb IN('P', 'X'));
    END upd_ord_hdrs_sp;

    PROCEDURE upd_rlse_ttl_rows_sp IS
    BEGIN
      UPDATE rlse_op1z
         SET ord_ln_cnt = l_ord_ln_cnt
       WHERE div_part = l_div_part
         AND rlse_ts = l_rlse_ts;
    END upd_rlse_ttl_rows_sp;

    PROCEDURE mult_load_depart_sid_sp IS
      TYPE l_rt_load_depart_ord IS RECORD(
        load_depart_sid      NUMBER,
        new_load_depart_sid  NUMBER,
        t_ord_num            type_ntab
      );

      TYPE l_tt_load_depart_ord IS TABLE OF l_rt_load_depart_ord;

      l_t_load_depart_ord        l_tt_load_depart_ord;
      l_load_depart_sid          NUMBER;
      l_new_load_depart_sid      NUMBER;
      l_t_ord_num                type_ntab;
    BEGIN
      SELECT ld.load_depart_sid,
             op_order_load_pk.load_depart_sid_fn(ld.div_part, ld.llr_ts, ld.load_num) AS new_load_depart_sid,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = ld.div_part
                              AND a.load_depart_sid = ld.load_depart_sid
                              AND a.stata IN('P', 'O')
                          ) AS type_ntab
                 ) AS ord_nums
      BULK COLLECT INTO l_t_load_depart_ord
        FROM load_depart_op1f ld
       WHERE ld.div_part = l_div_part
         AND ld.llr_dt = l_llr_dt
         AND ld.load_num IN(SELECT rl.val
                              FROM rlse_log_op2z rl
                             WHERE rl.div_part = l_div_part
                               AND rl.rlse_id = l_rlse_id)
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = ld.div_part
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.stata = 'P')
         AND EXISTS(SELECT 1
                      FROM load_depart_op1f ld2
                     WHERE ld2.div_part = ld.div_part
                       AND ld2.llr_dt = ld.llr_dt
                       AND ld2.load_num = ld.load_num
                       AND EXISTS(SELECT 1
                                    FROM ordp100a a2
                                   WHERE a2.div_part = ld2.div_part
                                     AND a2.load_depart_sid = ld2.load_depart_sid
                                     AND a2.stata = 'P')
                       AND ld2.load_depart_sid <> ld.load_depart_sid)
         AND ld.load_depart_sid <> op_order_load_pk.load_depart_sid_fn(ld.div_part, ld.llr_ts, ld.load_num);

      IF l_t_load_depart_ord.COUNT > 0 THEN
        FOR i IN l_t_load_depart_ord.FIRST .. l_t_load_depart_ord.LAST LOOP
          l_load_depart_sid := l_t_load_depart_ord(i).load_depart_sid;
          l_new_load_depart_sid := l_t_load_depart_ord(i).new_load_depart_sid;
          l_t_ord_num := l_t_load_depart_ord(i).t_ord_num;

          UPDATE stop_eta_op1g se
             SET se.load_depart_sid = l_new_load_depart_sid
           WHERE se.div_part = l_div_part
             AND se.load_depart_sid = l_load_depart_sid
             AND se.cust_id IN(SELECT   a.custa
                                 FROM ordp100a a
                                WHERE a.div_part = l_div_part
                                  AND a.ordnoa IN(SELECT t.column_value
                                                    FROM TABLE(CAST(l_t_ord_num AS type_ntab)) t)
                             GROUP BY a.custa);

          FORALL j IN l_t_ord_num.FIRST .. l_t_ord_num.LAST
            UPDATE ordp100a a
               SET a.load_depart_sid = l_new_load_depart_sid
             WHERE a.div_part = l_div_part
               AND a.ordnoa = l_t_ord_num(j)
               AND a.load_depart_sid <> l_new_load_depart_sid;
        END LOOP;

        logs.info('Mult LoadDepartSid Upd Cnt: ' || l_t_load_depart_ord.COUNT);
      END IF;   -- l_t_load_depart_ord.COUNT > 0
    END mult_load_depart_sid_sp;

    PROCEDURE send_wave_plan_sp IS
      l_data                 typ.t_maxvc2;
      l_c_file_dir  CONSTANT VARCHAR2(80) := '/ftptrans';
      l_c_file_nm   CONSTANT VARCHAR2(80) := i_div || '_LOADSEQ';
      l_c_rmt_file  CONSTANT VARCHAR2(30) := 'LOADSEQ';
    BEGIN
      IF op_parms_pk.val_fn(l_div_part, op_const_pk.prm_wave_plan) = 'Y' THEN
        logs.dbg('Build Load List');

        SELECT LPAD('@@', 65)
               || to_list_fn(CURSOR(SELECT   wpl.load_num
                                        FROM wave_plan_load_op2w wpl, rlse_log_op2z rl
                                       WHERE rl.div_part = l_div_part
                                         AND rl.rlse_id = l_rlse_id
                                         AND rl.typ_id = 'LOAD'
                                         AND wpl.div_part = rl.div_part
                                         AND wpl.llr_dt = l_llr_dt
                                         AND wpl.load_num = rl.val
                                    ORDER BY wpl.seq
                                   ),
                             '#'
                            )
               || '@'
          INTO l_data
          FROM DUAL;

        logs.dbg('Write File');
        io.write_line(l_data, l_c_file_nm, l_c_file_dir, 'W');
        logs.dbg('FTP to mainframe');
        op_ftp_sp(i_div, l_c_file_nm, l_c_rmt_file, 'N');
      END IF;   -- op_parms_pk.val_fn(l_div_part, op_const_pk.prm_wave_plan) = 'Y'
    END send_wave_plan_sp;

    PROCEDURE del_rlse_entry_sp IS
    BEGIN
      -- remove current load entries in rlse tables
      DELETE FROM rlse_log_op2z
            WHERE div_part = l_div_part
              AND rlse_id = l_rlse_id;

      DELETE FROM rlse_op1z
            WHERE div_part = l_div_part
              AND rlse_id = l_rlse_id;
    END del_rlse_entry_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StrtgID', i_strtg_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'TestBilCd', i_test_bil_cd);
    logs.add_parm(lar_parm, 'ForcInvSw', i_forc_inv_sw);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get DivPart');
    l_div_part := div_pk.div_part_fn(i_div);
    l_section := 'Check required parms and Last Release status';
    logs.dbg(l_section);
    upd_evnt_log_sp(l_section);
    l_msg := COALESCE(check_required_parms_fn(i_div, i_llr_dt, i_load_list, i_crp_list, i_mfst_list,
                                              l_c_input_delimiter),
                      last_rlse_fn(l_div_part)
                     );

    IF l_msg IS NULL THEN
      logs.dbg('Set SetRelease Process Active');
      l_prcs_id := op_const_pk.prcs_set_rlse;
      op_process_control_pk.set_process_status_sp(l_prcs_id, op_process_control_pk.g_c_active, i_user_id, l_div_part);
      logs.dbg('Initialize');
      l_strtg_id :=(CASE
                      WHEN TRIM(i_strtg_id) IS NULL THEN 999
                      ELSE TO_NUMBER(i_strtg_id)
                    END);
      l_llr_dt := TO_DATE(i_llr_dt, g_c_dt_fmt);
      l_load_list := clean_list_fn(i_load_list, l_c_input_delimiter);
      l_crp_list := clean_list_fn(i_crp_list, l_c_input_delimiter);
      l_mfst_list := clean_list_fn(i_mfst_list, l_c_input_delimiter);
      l_refresh_inv_sw := NVL(op_parms_pk.val_fn(l_div_part, op_const_pk.prm_inv_refresh), 'N');
      logs.dbg('Lock RLSE_OP1Z');
      lock_op1z_sp;
      logs.dbg('Add entry for current Release');
      add_rlse_entry_sp;
      -- Release lock on RLSE_OP1Z table
      COMMIT;
      l_section := 'Make Loads Unavailable for Closing';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      disable_load_clos_sp;
      COMMIT;
      ---------------------------------------------------------------
      -- Lock Pre-Post table before tagging any orders with P status,
      -- to prevent OrderReceipt from placing new orders for pre-post
      -- customers on billed loads.
      ---------------------------------------------------------------
      logs.dbg('Lock PREPOST_LOAD_OP1P table');
      lock_op1p_sp;
      logs.dbg('Analyze Tables');
      op_analyze_by_parm_sp(l_div_part, 'ANLYZ_RLSE');
--      forc_parallel_sp(TRUE);
      l_section := 'Tag Orders';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      tag_ords_sp;

--      forc_parallel_sp(FALSE);
      IF l_ord_ln_cnt > 0 THEN
        l_section := 'Reassign Distribution Orders to Alt Load';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        reassgn_loads_sp(l_div_part, l_rlse_ts, i_user_id);

        IF l_is_test_bil THEN
          -- Commit tagged orders
          COMMIT;

          IF l_is_forc_inv THEN
            l_section := 'Force Inventory';
            logs.dbg(l_section);
            upd_evnt_log_sp(l_section);
            forc_inv_sp;
            COMMIT;
          END IF;   -- l_is_forc_inv

          IF l_test_bil_cd = l_c_all_ords THEN
            l_section := 'Reassign DIST Orders';
            logs.dbg(l_section);
            upd_evnt_log_sp(l_section);
            reassign_dist_sp;
            COMMIT;
          END IF;   -- l_test_bil_cd = l_c_all_ords
        ELSE
          ------------------------------------------------------------------------
          -- Open/Inprocess found and tagged with P status.
          -- Now insert an entry in Pre-Post table for each load/stop/cust/llrdate
          -- combo being released/billed.
          ------------------------------------------------------------------------
          l_section := 'Add PrePost Entries';
          logs.dbg(l_section);
          upd_evnt_log_sp(l_section);
          ins_prepost_entries_sp;
          COMMIT;
        END IF;   -- l_is_test_bil

        l_section := 'Upd Order Header Status';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        upd_ord_hdrs_sp;
        l_section := 'Apply Vendor Compliance OrdQty Adjustments';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        vndr_cmp_sp(l_div_part, l_rlse_ts);
        l_section := 'Upd Total Rows on RLSE_OP1Z table';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        upd_rlse_ttl_rows_sp;
        l_section := 'Fix Ords with Mult LoadDepartSid for same Div/LLRDt/Load';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        mult_load_depart_sid_sp;
        COMMIT;

        IF l_is_test_bil THEN
          logs.dbg('Set TestBill Process Active');
          l_prcs_id := op_const_pk.prcs_test_bil;
          op_process_control_pk.set_process_status_sp(l_prcs_id,
                                                      op_process_control_pk.g_c_active,
                                                      i_user_id,
                                                      l_div_part
                                                     );
          l_msg := 'Test-Bill release initiated.';
        ELSE
          l_section := 'Send WavePlan LoadSeq to MF';
          logs.dbg(l_section);
          upd_evnt_log_sp(l_section);
          send_wave_plan_sp;
          l_msg := 'Production release initiated.';
        END IF;   -- l_is_test_bil

        l_is_ok_to_alloc := TRUE;
      ELSE
        l_msg := 'No orders meet the criteria.';
        l_section := 'BackOut Release';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        backout_sp(l_div_part, l_rlse_ts);
        l_section := 'Remove Release Entry';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        del_rlse_entry_sp;
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
      END IF;   -- l_ord_ln_cnt > 0

      COMMIT;
      logs.dbg('Set SetRelease Process Inactive');
      l_prcs_id := op_const_pk.prcs_set_rlse;
      op_process_control_pk.set_process_status_sp(l_prcs_id, op_process_control_pk.g_c_inactive, i_user_id, l_div_part);
    END IF;   -- l_msg IS NULL

    IF l_is_ok_to_alloc THEN
      IF (   l_refresh_inv_sw = 'A'
          OR (    l_refresh_inv_sw = 'Y'
              AND NOT l_is_test_bil)) THEN
        l_section := 'Refresh Inventory';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        -- This will send QITEM19 msgs to mainframe which will then send back
        -- QITEM09 msgs to replace inventory qtys. The QITEM09 process will
        -- also initiate the allocation after it processes its finished msg
        -- and confirms that SetRelease has been initiated.
        op_messages_pk.refresh_inv_sp(i_div, 'SETRLSE');
      ELSE
        l_section := 'Start Allocation';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        op_allocate_pk.start_alloc_sp(i_div, i_user_id);
      END IF;   -- l_refresh_inv_sw = 'A' OR (l_refresh_inv_sw = 'Y' AND NOT l_is_test_bil)

      upd_evnt_log_sp(l_msg, 1);
    ELSE
      upd_evnt_log_sp(l_msg, -1);
    END IF;   -- l_is_ok_to_alloc

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
      upd_evnt_log_sp(op_process_control_pk.restricted_msg_fn(l_prcs_id, l_div_part), -1);

      IF l_prcs_id = op_const_pk.prcs_test_bil THEN
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_set_rlse,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
      END IF;   -- l_prcs_id = op_const_pk.prcs_test_bil

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    WHEN excp.gx_row_locked THEN
      logs.warn(SQLERRM, lar_parm);
      upd_evnt_log_sp('Unable to Lock Table. Please try later.', -1);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_set_rlse,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
    WHEN OTHERS THEN
      logs.err(lar_parm, NULL, FALSE);
      ROLLBACK;
      forc_parallel_sp(FALSE);
      upd_evnt_log_sp('Release Failed with Unhandled Error', -1);
      backout_sp(l_div_part, l_rlse_ts);
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_set_rlse,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );

      IF l_test_bil_cd <> '~' THEN
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_test_bil,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
      END IF;

      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      RAISE;
  END rlse_sp;

  /*
  ||----------------------------------------------------------------------------
  || EVNT_RLSE_SP
  ||  Create event to process Release of Allocation
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/20/10 | rhalpai | Original for PIR8377
  || 10/14/17 | rhalpai | Change to call new CIG_EVENT_MGR_PK.BUILD_OBJ_STRING.
  ||                    | PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE evnt_rlse_sp(
    i_div  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.EVNT_RLSE_SP';
    lar_parm             logs.tar_parm;
    l_org_id             NUMBER;
    l_evnt_que_id        NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_org_id := cig_organization_pk.get_div_id(i_div);
    logs.dbg('Build Object String for Event');
    cig_event_mgr_pk.build_obj_string(i_org_id               => l_org_id,
                                      i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                      i_event_dfn_id         => cig_constants_events_pk.evd_op_release,
                                      i_div_nm               => i_div,
                                      i_is_script_fw_exec    => 'N',
                                      o_event_que_id         => l_evnt_que_id
                                     );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END evnt_rlse_sp;

  /*
  ||----------------------------------------------------------------------------
  || START_RLSE_SP
  ||  Starts release of order allocation in a separate thread.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/01/10 | rhalpai | Original
  || 11/01/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 11/17/11 | rhalpai | Add logic to turn ON RLSE_COMPL Process Control.
  ||                    | IM-033180
  || 02/15/12 | rhalpai | Add logic to capture state of ProcesControls for
  ||                    | RLSE_ALLOC and RLSE_COMPL and restore state if either
  ||                    | is restricted. IM-043061
  || 02/16/12 | rhalpai | Change logic to replace StopList parm with CrpList.
  ||                    | PIR10845
  || 05/13/13 | rhalpai | Change logic to call xxopRlse.sub with wrapper
  ||                    | for ssh to Application Server. PIR11038
  || 02/25/15 | rhalpai | Change 'Check PrcsCntl Active' logic to replace
  ||                    | SELECT from PRCS_CNTL tables with call to
  ||                    | OP_PROCESS_CONTROL_PK.GET_ACTIVE_RESTRICTIONS_FN to
  ||                    | gather active processes. PIR11038
  || 10/14/17 | rhalpai | Change to call new CIG_EVENT_MGR_PK.CREATE_INSTANCE.
  ||                    | Change to use constants package OP_CONST_PK.
  ||                    | Add div_part in calls to
  ||                    | OP_PROCESS_CONTROL_PK.GET_ACTIVE_RESTRICTIONS_FN,
  ||                    | OP_PROCESS_CONTROL_PK.SET_PROCESS_STATUS_SP.
  ||                    | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  PROCEDURE start_rlse_sp(
    i_div          IN  VARCHAR2,
    i_strtg_id     IN  VARCHAR2,
    i_user_id      IN  VARCHAR2,
    i_llr_dt       IN  VARCHAR2,
    i_load_list    IN  VARCHAR2,
    i_crp_list     IN  VARCHAR2,
    i_mfst_list    IN  VARCHAR2,
    i_test_bil_cd  IN  VARCHAR2,
    i_forc_inv_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_SET_RELEASE_PK.START_RLSE_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_t_actv_prcs         type_stab;
    l_prcs_rlse_alloc_sw  VARCHAR2(1);
    l_prcs_rlse_compl_sw  VARCHAR2(1);
    l_org_id              NUMBER;
    l_sid                 VARCHAR2(10);
    l_cmd                 typ.t_maxvc2;
    l_appl_srvr           VARCHAR2(20);
    l_os_result           typ.t_maxvc2;
    l_evnt_parms          CLOB;
    l_evnt_que_id         NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'StrtgID', i_strtg_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'LoadList', i_load_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'MfstList', i_mfst_list);
    logs.add_parm(lar_parm, 'TestBilCd', i_test_bil_cd);
    logs.add_parm(lar_parm, 'ForcInvSw', i_forc_inv_sw);
    logs.info('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Get Active PrcsCntl Collection');
    l_t_actv_prcs := op_process_control_pk.get_active_restrictions_fn(NULL, l_div_part);
    logs.dbg('Check PrcsCntl Active');

    SELECT (SELECT NVL(MAX('Y'), 'N')
              FROM TABLE(CAST(l_t_actv_prcs AS type_stab)) t
             WHERE t.column_value LIKE op_const_pk.prcs_rlse_alloc || '%'),
           (SELECT NVL(MAX('Y'), 'N')
              FROM TABLE(CAST(l_t_actv_prcs AS type_stab)) t
             WHERE t.column_value LIKE op_const_pk.prcs_rlse_compl || '%')
      INTO l_prcs_rlse_alloc_sw,
           l_prcs_rlse_compl_sw
      FROM DUAL;

    logs.dbg('Set RlseAlloc Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Set RlseCompl Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                op_process_control_pk.g_c_active,
                                                i_user_id,
                                                l_div_part
                                               );
    logs.dbg('Initialize');
    l_sid := SYS_CONTEXT('USERENV', 'DB_NAME');
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    l_cmd := '/local/prodcode/bin/xxopRlse.sub "' || i_div || '" "' || l_sid || '"';
    l_org_id := cig_organization_pk.get_div_id(i_div);
    l_evnt_parms := '<parameters>'
                    || '<row><sequence>'
                    || 1
                    || '</sequence><value>'
                    || i_div
                    || '</value></row>'
                    || '<row><sequence>'
                    || 2
                    || '</sequence><value>'
                    || i_strtg_id
                    || '</value></row>'
                    || '<row><sequence>'
                    || 3
                    || '</sequence><value>'
                    || i_user_id
                    || '</value></row>'
                    || '<row><sequence>'
                    || 4
                    || '</sequence><value>'
                    || i_llr_dt
                    || '</value></row>'
                    || '<row><sequence>'
                    || 5
                    || '</sequence><value>'
                    || i_load_list
                    || '</value></row>'
                    || '<row><sequence>'
                    || 6
                    || '</sequence><value>'
                    || i_crp_list
                    || '</value></row>'
                    || '<row><sequence>'
                    || 7
                    || '</sequence><value>'
                    || i_mfst_list
                    || '</value></row>'
                    || '<row><sequence>'
                    || 8
                    || '</sequence><value>'
                    || i_test_bil_cd
                    || '</value></row>'
                    || '<row><sequence>'
                    || 9
                    || '</sequence><value>'
                    || i_forc_inv_sw
                    || '</value></row>'
                    || '<row><sequence>'
                    || 10
                    || '</sequence><value>'
                    || NULL
                    || '</value></row>'
                    || '</parameters>';
    logs.dbg('Upd the Release Event Parms');
    cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                     i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                     i_event_dfn_id         => cig_constants_events_pk.evd_op_release,
                                     i_parameters           => l_evnt_parms,
                                     i_div_nm               => i_div,
                                     i_is_script_fw_exec    => 'N',
                                     i_is_complete          => 'N',
                                     i_pgm_id               => 'PLSQL',
                                     i_user_id              => i_user_id,
                                     o_event_que_id         => l_evnt_que_id
                                    );
    COMMIT;
    logs.dbg('Run Control-M Sub Script in Background');
    logs.info(l_cmd);
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    logs.info(l_os_result);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);

      IF l_prcs_rlse_alloc_sw = 'N' THEN
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
      END IF;   -- l_prcs_rlse_alloc_sw = 'N'

      IF l_prcs_rlse_compl_sw = 'N' THEN
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
      END IF;   -- l_prcs_rlse_compl_sw = 'N'
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_alloc,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_rlse_compl,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END start_rlse_sp;
END op_set_release_pk;
/

