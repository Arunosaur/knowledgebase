CREATE OR REPLACE PACKAGE op_mass_maint_pk IS
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
  FUNCTION ord_stat_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION ord_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION scbd_categ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION corp_list_fn(
    i_user_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION div_list_fn(
    i_crp_list  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_src_list_fn(
    i_crp_list  IN  VARCHAR2,
    i_div_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION grp_list_fn(
    i_crp_list  IN  VARCHAR2,
    i_div_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION cust_list_fn(
    i_crp_list    IN  VARCHAR2,
    i_div_list    IN  VARCHAR2 DEFAULT 'ALL',
    i_grp_list    IN  CLOB DEFAULT NULL,
    i_search_str  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION item_list_fn(
    i_search_str  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_dtl_list_fn(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_ord_qty          IN  NUMBER DEFAULT NULL,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_hdr_list_fn(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB DEFAULT NULL,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;


  FUNCTION recap_stat_list_fn(
    i_div_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_crp_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION vndr_search_fn(
    i_search_str  IN  VARCHAR2,
    i_div_list    IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION strct_ord_dtl_list_fn(
    i_ord_stat      IN  VARCHAR2 DEFAULT 'O',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_recap_stat    IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from      IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to        IN  VARCHAR2 DEFAULT '2999-12-31',
    i_vndr_list     IN  VARCHAR2 DEFAULT NULL,
    i_item_list     IN  CLOB DEFAULT NULL,
    i_crp_list      IN  VARCHAR2 DEFAULT NULL,
    i_cust_list     IN  CLOB DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_cut_list_fn(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_excl_tob_sw    IN  VARCHAR2,
    i_excl_logo_sw   IN  VARCHAR2,
    i_mfst_max_list  IN  CLOB,
    i_dtl_sw         IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE cancl_ord_sp(
    i_parm_list  IN  CLOB,
    i_auth_by    IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE cancl_ord_sp(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB DEFAULT NULL,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_auth_by          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  );

  PROCEDURE cancl_ord_ln_sp(
    i_parm_list  IN  CLOB,
    i_auth_by    IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  );

  PROCEDURE cancl_ord_ln_sp(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_ord_qty          IN  NUMBER DEFAULT NULL,
    i_auth_by          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  );

  PROCEDURE upd_ord_qty_sp(
    i_parm_list    IN  CLOB,
    i_new_ord_qty  IN  NUMBER,
    i_auth_by      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  );

  PROCEDURE upd_ord_qty_sp(
    i_crp_list         IN  VARCHAR2,
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_ord_qty          IN  NUMBER DEFAULT NULL,
    i_new_ord_qty      IN  NUMBER,
    i_auth_by          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  );

  PROCEDURE ord_cut_sp(
    i_div           IN  VARCHAR2,
    i_llr_dt        IN  DATE,
    i_excl_tob_sw   IN  VARCHAR2,
    i_excl_logo_sw  IN  VARCHAR2,
    i_mfst_max_list IN  CLOB,
    i_auth_by       IN  VARCHAR2,
    i_user_id       IN  VARCHAR2
  );

  FUNCTION cpcty_ord_mgmt_excl_typ_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION cpcty_ord_mgmt_excl_corp_list_fn
    RETURN SYS_REFCURSOR;

  FUNCTION cpcty_ord_mgmt_excl_item_list_fn
    RETURN SYS_REFCURSOR;

  PROCEDURE cpcty_ord_mgmt_excl_maint_sp(
    i_excl_typ  IN  VARCHAR2,
    i_list      IN  CLOB,
    i_user_id   IN  VARCHAR2
  );

  FUNCTION cpcty_ord_mgmt_item_list_fn(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_mfst_max_list  IN  CLOB
  ) RETURN SYS_REFCURSOR;

  PROCEDURE cpcty_ord_mgmt_apply_sp(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_mfst_max_list  IN  CLOB,
    i_auth_by        IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  );

END op_mass_maint_pk;
/

CREATE OR REPLACE PACKAGE BODY op_mass_maint_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  g_c_dt_fmt     CONSTANT VARCHAR2(10)  := 'YYYY-MM-DD';
  g_c_dt_tm_fmt  CONSTANT VARCHAR2(19)  := 'YYYY-MM-DD HH24:MI';

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ORD_STAT_LIST_FN
  ||  Returns cursor of Order Statuses.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_stat_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_STAT_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT 'O' AS stat_cd, 'Open' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'S' AS stat_cd, 'Suspend' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'C' AS stat_cd, 'Cancel' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'U' AS stat_cd, 'Unbilled' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'B' AS stat_cd, 'Billed' AS stat_descr
         FROM DUAL
       UNION ALL
       SELECT 'ALL' AS stat_cd, 'ALL' AS stat_descr
         FROM DUAL;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_stat_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_TYP_LIST_FN
  ||  Returns cursor of Order Types.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_TYP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT 'R' AS typ_cd, 'Reg' AS typ_descr
         FROM DUAL
       UNION ALL
       SELECT 'T' AS typ_cd, 'Test' AS typ_descr
         FROM DUAL
       UNION ALL
       SELECT 'ALL' AS typ_cd, 'ALL' AS typ_descr
         FROM DUAL;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || SCBD_CATEG_LIST_FN
  ||  Returns cursor of scoreboard categories.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/21 | rhalpai | Original for PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION scbd_categ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.SCBD_CATEG_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   a.sbcata AS scbd_categ_cd, a.desca AS descr
           FROM mclp230a a
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END scbd_categ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CORP_LIST_FN
  ||  Returns cursor of Corp Codes with existing orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list.
  ||                    | Add column to cursor to indicate whether orders exist for corp. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION corp_list_fn(
    i_user_id  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CORP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT 'ALL' AS corp_cd, 'ALL' AS corp_nm
         FROM DUAL
       UNION ALL
       SELECT x.corp_cd, x.corp_nm
         FROM (SELECT   LPAD(cc.corp_cd, 3, '0') AS corp_cd, cc.corp_nm,
                        (CASE
                           WHEN EXISTS(SELECT 1
                                         FROM mclp020b cx
                                        WHERE cx.corpb = cc.corp_cd
                                          AND EXISTS(SELECT 1
                                                       FROM ordp100a a
                                                      WHERE a.div_part = cx.div_part
                                                        AND a.custa = cx.custb)) THEN 'Y'
                           ELSE 'N'
                         END
                        ) AS ord_exist_sw
                   FROM corp_cd_dm1c cc
               ORDER BY cc.corp_cd) x;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END corp_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || DIV_LIST_FN
  ||  Returns cursor of Divisions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION div_list_fn(
    i_crp_list  IN  VARCHAR2,
    i_user_id   IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.DIV_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list = 'ALL' THEN
      NULL;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT t.column1
                                FROM TABLE(framework.lob2table.separatedcolumns(TO_CLOB(SUBSTR(i_crp_list, 2)),
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   d.div_id, d.div_nm, d.div_part
           FROM div_mstr_di1d d
          WHERE EXISTS(SELECT 1
                         FROM mclp020b cx
                        WHERE cx.div_part = d.div_part
                          AND (   i_crp_list = 'ALL'
                               OR cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                                                FROM TABLE(l_t_crps) t))
                          AND EXISTS(SELECT 1
                                       FROM ordp100a a
                                      WHERE a.div_part = cx.div_part
                                        AND a.custa = cx.custb))
       ORDER BY d.div_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END div_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_SRC_LIST_FN
  ||  Returns cursor of Order Sources.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_src_list_fn(
    i_crp_list  IN  VARCHAR2,
    i_div_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_SRC_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list = 'ALL' THEN
      NULL;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT t.column1
                                FROM TABLE(framework.lob2table.separatedcolumns(TO_CLOB(SUBSTR(i_crp_list, 2)),
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT 'ALL' AS ord_src
         FROM DUAL
       UNION ALL
       SELECT y.ord_src
         FROM (SELECT   a.ipdtsa AS ord_src
                   FROM div_mstr_di1d d, mclp020b cx, ordp100a a
                  WHERE d.div_part > 0
                    AND (   i_div_list = 'ALL'
                         OR d.div_id MEMBER OF l_t_divs)
                    AND cx.div_part = d.div_part
                    AND (   i_crp_list = 'ALL'
                         OR cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                                          FROM TABLE(l_t_crps) t))
                    AND a.div_part = cx.div_part
                    AND a.custa = cx.custb
                    AND a.ipdtsa IS NOT NULL
               GROUP BY a.ipdtsa
               ORDER BY a.ipdtsa) y;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_src_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || GRP_LIST_FN
  ||  Returns cursor of Customer Groups.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION grp_list_fn(
    i_crp_list  IN  VARCHAR2,
    i_div_list  IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.GRP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list = 'ALL' THEN
      NULL;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT t.column1
                                FROM TABLE(framework.lob2table.separatedcolumns(TO_CLOB(SUBSTR(i_crp_list, 2)),
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   g.cstgpa AS grp_id, d.div_id || g.group_name AS grp_nm
           FROM div_mstr_di1d d, mclp100a g
          WHERE d.div_part > 0
            AND (   i_div_list = 'ALL'
                 OR d.div_id MEMBER OF l_t_divs)
            AND g.div_part = d.div_part
            AND EXISTS(SELECT 1
                         FROM sysp200c c, mclp020b cx
                        WHERE c.div_part = g.div_part
                          AND c.retgpc = g.cstgpa
                          AND cx.div_part = c.div_part
                          AND cx.custb = c.acnoc
                          AND (   i_crp_list = 'ALL'
                               OR LPAD(cx.corpb, 3, '0') MEMBER OF l_t_crps)
                          AND EXISTS(SELECT 1
                                       FROM ordp100a a
                                      WHERE a.div_part = c.div_part
                                        AND a.custa = c.acnoc))
       ORDER BY g.cstgpa;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END grp_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CUST_LIST_FN
  ||  Returns cursor of Customer Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_list_fn(
    i_crp_list    IN  VARCHAR2,
    i_div_list    IN  VARCHAR2 DEFAULT 'ALL',
    i_grp_list    IN  CLOB DEFAULT NULL,
    i_search_str  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CUST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_grps             type_stab;
    l_search_str         typ.t_maxcol;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'SearchStr', i_search_str);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list = 'ALL' THEN
      NULL;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT t.column1
                                FROM TABLE(framework.lob2table.separatedcolumns(TO_CLOB(SUBSTR(i_crp_list, 2)),
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_grp_list IS NOT NULL THEN
      l_t_grps := strsplit_fn(i_grp_list, op_const_pk.field_delimiter);
    END IF;   -- i_grp_list IS NOT NULL

    logs.dbg('Open Cursor');

    CASE
      WHEN i_search_str IS NULL THEN
        OPEN l_cv
         FOR
           SELECT d.div_id, c.acnoc AS cust_id, c.namec AS cust_nm, c.shpctc AS city, c.shpstc AS st,
                  DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND cx.div_part = d.div_part
              AND (   i_crp_list = 'ALL'
                   OR LPAD(cx.corpb, 3, '0') MEMBER OF l_t_crps)
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb
              AND (   i_grp_list IS NULL
                   OR c.retgpc MEMBER OF l_t_grps)
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = c.div_part
                            AND a.custa = c.acnoc);
      WHEN num.ianb(i_search_str) THEN
        OPEN l_cv
         FOR
           SELECT d.div_id, c.acnoc AS cust_id, c.namec AS cust_nm, c.shpctc AS city, c.shpstc AS st,
                  DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND c.div_part = d.div_part
              AND (   c.acnoc LIKE i_search_str || '%'
                   OR c.namec LIKE '%' || i_search_str || '%')
              AND (   i_grp_list IS NULL
                   OR c.retgpc MEMBER OF l_t_grps)
              AND cx.div_part = c.div_part
              AND cx.custb = c.acnoc
              AND (   i_crp_list = 'ALL'
                   OR LPAD(cx.corpb, 3, '0') MEMBER OF l_t_crps)
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = c.div_part
                            AND a.custa = c.acnoc);
      ELSE
        l_search_str := '%' || UPPER(i_search_str) || '%';

        OPEN l_cv
         FOR
           SELECT d.div_id, c.acnoc AS cust_id, c.namec AS cust_nm, c.shpctc AS city, c.shpstc AS st,
                  DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND c.div_part = d.div_part
              AND UPPER(c.namec) LIKE l_search_str
              AND (   i_grp_list IS NULL
                   OR c.retgpc MEMBER OF l_t_grps)
              AND cx.div_part = c.div_part
              AND cx.custb = c.acnoc
              AND (   i_crp_list = 'ALL'
                   OR LPAD(cx.corpb, 3, '0') MEMBER OF l_t_crps)
              AND EXISTS(SELECT 1
                           FROM ordp100a a
                          WHERE a.div_part = c.div_part
                            AND a.custa = c.acnoc);
    END CASE;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cust_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ITEM_LIST_FN
  ||  Returns cursor of matching Item Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 03/15/17 | rhalpai | Change logic to call new OP_ITEM_PK.CORP_ITEM_SEARCH_FN.
  ||                    | SDHD-102466
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_list_fn(
    i_search_str  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ITEM_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'SearchStr', i_search_str);
    logs.dbg('ENTRY', lar_parm);
    l_cv := op_item_pk.corp_item_search_fn(i_search_str);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END item_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_DTL_LIST_FN
  ||  Returns cursor of Order Detail Info.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * CrpList      : Corp Code List i.e.: 010~500~501
  ||  * OrdStat      : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||  * DivIdList    : Division ID list i.e.: ALL or SW~MI~ME
  ||  * OrdTyp       : Order Type (R:Reg,T:Test,ALL:All)
  ||  * OrdSrcList   : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||    LLRFrom      : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (requires LLR_TO)
  ||    LLRTo        : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (requires LLR_FROM)
  ||    OrdRcvdAftr  : Search for orders received >= time in YYYY-MM-DD HH24:MI format
  ||  * ItemList     : Catalog Item List (include leading zeros) delimited by ~
  ||    POPrfxList   : List of the beginning characters for PO Numbers delimited by ~
  ||    GrpCdList    : Cust Group List  i.e.: SW006~SW010
  ||    CustList     : CustId List (include leading zeros) delimited by ~
  ||    OrdQty       : OrdQty >= passed value
  ||    ScbdCategList: Scoreboard category list delimited by ~
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list, exclusion corp list, scoreboard category list,
  ||                    | exclusion item list. Add scoreboard category to cursor. PIR21233
  || 09/09/21 | rhalpai | Change logic for exclusion item list to improve performance. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtl_list_fn(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_ord_qty          IN  NUMBER DEFAULT NULL,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_DTL_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_ord_rcvd_ts        DATE;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_ord_srcs         type_stab;
    l_t_items            type_stab;
    l_t_po_prfxs         type_stab;
    l_t_grps             type_stab;
    l_t_custs            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'OrdSrcList', i_ord_src_list);
    logs.add_parm(lar_parm, 'LlrFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LlrTo', i_llr_to);
    logs.add_parm(lar_parm, 'OrdRcvdAftr', i_ord_rcvd_aftr);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'PoPrfxList', i_po_prfx_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.add_parm(lar_parm, 'ScbdCategList', i_scbd_categ_list);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    excp.assert((i_item_list IS NOT NULL), 'ItemList cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
    l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
    l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

    IF i_crp_list = 'ALL' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT t.column1
                                FROM TABLE(framework.lob2table.separatedcolumns(TO_CLOB(SUBSTR(i_crp_list, 2)),
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF SUBSTR(i_item_list, 1, 1) = '-' THEN
      SELECT x.catite
      BULK COLLECT INTO l_t_items
        FROM (SELECT e.catite
                FROM sawp505e e
               WHERE (   i_scbd_categ_list IS NULL
                      OR e.scbcte IN(SELECT tt.column1
                                       FROM TABLE(framework.lob2table.separatedcolumns(i_scbd_categ_list,
                                                                                       op_const_pk.field_delimiter
                                                                                      )
                                                 ) tt)
                     )
              MINUS
              SELECT t.column1
                FROM TABLE(framework.lob2table.separatedcolumns(SUBSTR(i_item_list, 2), op_const_pk.field_delimiter)) t) x;
    ELSE
      l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
    END IF;   -- SUBSTR(i_item_list, 1, 1) = '-'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_ord_src_list <> 'ALL' THEN
      l_t_ord_srcs := strsplit_fn(i_ord_src_list, op_const_pk.field_delimiter);
    END IF;   -- i_ord_src_list <> 'ALL'

    IF i_po_prfx_list IS NOT NULL THEN
      l_t_po_prfxs := strsplit_fn(i_po_prfx_list, op_const_pk.field_delimiter);
    END IF;   -- i_po_prfx_list IS NOT NULL

    IF i_grp_list IS NOT NULL THEN
      l_t_grps := strsplit_fn(i_grp_list, op_const_pk.field_delimiter);
    END IF;   -- i_grp_list IS NOT NULL

    IF i_cust_list IS NOT NULL THEN
      l_t_custs := strsplit_fn(i_cust_list, op_const_pk.field_delimiter);
    END IF;   -- i_cust_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id, a.ordnoa AS ord_num,
              a.stata AS ord_stat, a.dsorda AS ord_typ, a.ipdtsa AS ord_src, a.cpoa AS po_num,
              TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num,
              TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
              (SELECT COUNT(*)
                 FROM ordp120b b2
                WHERE b2.div_part = a.div_part
                  AND b2.ordnob = a.ordnoa
                  AND b2.lineb = FLOOR(b2.lineb)) AS ord_ln_cnt, b.lineb AS ord_ln, b.statb AS ln_stat,
              e.catite AS catlg_num, b.ordqtb AS ord_qty,
              TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                      'YYYY-MM-DD HH24:MI:SS'
                     ) AS prc_ts,
              b.ntshpb AS nt_shp_rsn, b.excptn_sw, DECODE(b.statb, 'O', 'Y', 'S', 'Y', 'N') AS sel, e.scbcte AS scbd_categ
         FROM TABLE(l_t_crps) cc, div_mstr_di1d d, sawp505e e, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld,
              ordp120b b
        WHERE d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND e.catite MEMBER OF l_t_items
          AND cx.div_part = d.div_part
          AND cx.corpb = TO_NUMBER(cc.column_value)
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb
          AND (   i_grp_list IS NULL
               OR c.retgpc MEMBER OF l_t_grps)
          AND a.div_part = c.div_part
          AND a.custa = c.acnoc
          AND 'Y' =(CASE
                      WHEN i_ord_stat = 'C' THEN 'Y'
                      WHEN a.stata = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND a.stata IN('O', 'S', 'C', 'I') THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND a.stata IN('R', 'A', 'P', 'X') THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL
                      AND a.stata IN('O', 'S') THEN 'Y'
                    END
                   )
          AND a.dsorda IN('R', 'T')
          AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
          AND (   i_ord_src_list = 'ALL'
               OR a.ipdtsa MEMBER OF l_t_ord_srcs)
          AND a.ord_rcvd_ts >= l_ord_rcvd_ts
          AND (   i_po_prfx_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_po_prfxs) t
                          WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value)
--                          WHERE REGEXP_LIKE(a.cpoa, '^' || t.column_value || '*')
              )
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
          AND b.div_part = a.div_part
          AND b.ordnob = a.ordnoa
          AND 'Y' =(CASE
                      WHEN b.statb = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND b.statb IN('O', 'S', 'C', 'I') THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND b.statb IN('R', 'A', 'P', 'T', 'X') THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL
                      AND b.statb IN('O', 'S') THEN 'Y'
                    END
                   )
          AND b.subrcb < 999
          AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty)
          AND b.itemnb = e.iteme
          AND b.sllumb = e.uome;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_dtl_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_HDR_LIST_FN
  ||  Returns cursor of Order Header Info.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * CrpList      : Corp Code List i.e.: 010~500~501
  ||  * OrdStat      : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||  * DivIdList    : Division ID list i.e.: ALL or SW~MI~ME
  ||  * OrdTyp       : Order Type (R:Reg,T:Test,ALL:All)
  ||  * OrdSrcList   : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||    LLRFrom      : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (requires LLR_TO)
  ||    LLRTo        : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (requires LLR_FROM)
  ||    OrdRcvdAftr  : Search for orders received >= time in YYYY-MM-DD HH24:MI format
  ||    ItemList     : Catalog Item List (include leading zeros) delimited by ~
  ||    POPrfxList   : List of the beginning characters for PO Numbers delimited by ~
  ||    GrpCdList    : Cust Group List  i.e.: SW006~SW010
  ||    CustList     : CustId List (include leading zeros) delimited by ~
  ||    ScbdCategList: Scoreboard category list delimited by ~
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list, exclusion corp list, scoreboard category list,
  ||                    | exclusion item list. PIR21233
  || 09/09/21 | rhalpai | Change logic for exclusion item list to improve performance. PIR21233
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_list_fn(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB DEFAULT NULL,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_HDR_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_ord_rcvd_ts        DATE;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_ord_srcs         type_stab;
    l_t_items            type_stab;
    l_t_po_prfxs         type_stab;
    l_t_grps             type_stab;
    l_t_custs            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'OrdSrcList', i_ord_src_list);
    logs.add_parm(lar_parm, 'LlrFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LlrTo', i_llr_to);
    logs.add_parm(lar_parm, 'OrdRcvdAftr', i_ord_rcvd_aftr);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'PoPrfxList', i_po_prfx_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'ScbdCategList', i_scbd_categ_list);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
    l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
    l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

    IF i_crp_list = 'ALL' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT t.column1
                                FROM TABLE(framework.lob2table.separatedcolumns(TO_CLOB(SUBSTR(i_crp_list, 2)),
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_ord_src_list <> 'ALL' THEN
      l_t_ord_srcs := strsplit_fn(i_ord_src_list, op_const_pk.field_delimiter);
    END IF;   -- i_ord_src_list <> 'ALL'

    IF i_item_list IS NOT NULL THEN
      IF SUBSTR(i_item_list, 1, 1) = '-' THEN
        SELECT x.catite
        BULK COLLECT INTO l_t_items
          FROM (SELECT e.catite
                  FROM sawp505e e
                 WHERE (   i_scbd_categ_list IS NULL
                        OR e.scbcte IN(SELECT tt.column1
                                         FROM TABLE(framework.lob2table.separatedcolumns(i_scbd_categ_list,
                                                                                         op_const_pk.field_delimiter
                                                                                        )
                                                   ) tt)
                       )
                MINUS
                SELECT t.column1
                  FROM TABLE(framework.lob2table.separatedcolumns(SUBSTR(i_item_list, 2), op_const_pk.field_delimiter)) t) x;
      ELSE
        l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
      END IF;   -- SUBSTR(i_item_list, 1, 1) = '-'
    END IF;   -- i_item_list IS NOT NULL

    IF i_po_prfx_list IS NOT NULL THEN
      l_t_po_prfxs := strsplit_fn(i_po_prfx_list, op_const_pk.field_delimiter);
    END IF;   -- i_po_prfx_list IS NOT NULL

    IF i_grp_list IS NOT NULL THEN
      l_t_grps := strsplit_fn(i_grp_list, op_const_pk.field_delimiter);
    END IF;   -- i_grp_list IS NOT NULL

    IF i_cust_list IS NOT NULL THEN
      l_t_custs := strsplit_fn(i_cust_list, op_const_pk.field_delimiter);
    END IF;   -- i_cust_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id, a.ordnoa AS ord_num,
              a.stata AS ord_stat, a.dsorda AS ord_typ, a.ipdtsa AS ord_src, a.cpoa AS po_num,
              TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num,
              TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
              (SELECT COUNT(*)
                 FROM ordp120b b
                WHERE b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.excptn_sw,
              DECODE(a.stata, 'O', 'Y', 'S', 'Y', 'N') AS sel
         FROM TABLE(l_t_crps) t, div_mstr_di1d d, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld
        WHERE d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND cx.div_part = d.div_part
          AND cx.corpb = TO_NUMBER(t.column_value)
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb
          AND (   i_grp_list IS NULL
               OR c.retgpc MEMBER OF l_t_grps)
          AND a.div_part = c.div_part
          AND a.custa = c.acnoc
          AND 'Y' =(CASE
                      WHEN a.stata = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND a.stata IN('O', 'S', 'C', 'I') THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND a.stata IN('R', 'A', 'P', 'X') THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL
                      AND a.stata IN('O', 'S') THEN 'Y'
                    END
                   )
          AND a.dsorda IN('R', 'T')
          AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
          AND (   i_ord_src_list = 'ALL'
               OR a.ipdtsa MEMBER OF l_t_ord_srcs)
          AND a.ord_rcvd_ts >= l_ord_rcvd_ts
          AND (   i_po_prfx_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_po_prfxs) t
                          WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value)
--                          WHERE REGEXP_LIKE(a.cpoa, '^' || t.column_value || '*')
              )
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
          AND (   i_item_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_items) t, sawp505e e, ordp120b b
                          WHERE e.catite = t.column_value
                            AND b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND 'Y' =(CASE
                                        WHEN b.statb = i_ord_stat THEN 'Y'
                                        WHEN i_ord_stat = 'U'
                                        AND b.statb IN('O', 'S', 'C', 'I') THEN 'Y'
                                        WHEN i_ord_stat = 'B'
                                        AND b.statb IN('R', 'A', 'P', 'T', 'X') THEN 'Y'
                                        WHEN i_ord_stat = 'ALL' THEN 'Y'
                                        WHEN i_ord_stat IS NULL
                                        AND b.statb IN('O', 'S') THEN 'Y'
                                      END
                                     )
                            AND b.subrcb < 999
                            AND b.itemnb = e.iteme
                            AND b.sllumb = e.uome)
              );

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_hdr_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || RECAP_STAT_LIST_FN
  ||  Returns cursor Strict Order recap statuses.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/23/17 | rhalpai | Original for PIR17400
  ||----------------------------------------------------------------------------
  */
  FUNCTION recap_stat_list_fn(
    i_div_list  IN  VARCHAR2 DEFAULT 'ALL',
    i_crp_list  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.RECAP_STAT_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_crp_list IS NOT NULL THEN
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT 'ALL' AS recap_stat
         FROM DUAL
       UNION ALL
       SELECT y.recap_stat
         FROM (SELECT   so.stat AS recap_stat
                   FROM div_mstr_di1d d, strct_ord_op1o so
                  WHERE d.div_part > 0
                    AND (   i_div_list = 'ALL'
                         OR d.div_id MEMBER OF l_t_divs)
                    AND so.div_part = d.div_part
                    AND (   i_crp_list IS NULL
                         OR i_crp_list = 'ALL'
                         OR EXISTS(SELECT 1
                                     FROM ordp100a a, mclp020b cx
                                    WHERE a.div_part = so.div_part
                                      AND a.ordnoa = so.ord_num
                                      AND cx.div_part = a.div_part
                                      AND cx.custb = a.custa
                                      AND cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                                                        FROM TABLE(l_t_crps) t))
                        )
               GROUP BY so.stat
               ORDER BY so.stat) y;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END recap_stat_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || VNDR_SEARCH_FN
  ||  Returns cursor of matching distinct vendor info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/23/17 | rhalpai | Original for PIR17400
  ||----------------------------------------------------------------------------
  */
  FUNCTION vndr_search_fn(
    i_search_str  IN  VARCHAR2,
    i_div_list    IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.VNDR_SEARCH_FN';
    lar_parm             logs.tar_parm;
    l_t_divs             type_stab;
    l_cv                 SYS_REFCURSOR;
    l_search_str         typ.t_maxcol;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'SearchStr', i_search_str);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF num.ianb(i_search_str) THEN
      l_search_str := i_search_str || '%';

      OPEN l_cv
       FOR
         SELECT   v.cbr_vndr_id, v.dcs_vndr_id, v.vndr_nm
             FROM div_mstr_di1d d, vndr_mstr_op1v v
            WHERE (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND v.div_part = d.div_part
              AND (   v.cbr_vndr_id LIKE l_search_str
                   OR v.dcs_vndr_id LIKE l_search_str)
              AND EXISTS(SELECT 1
                           FROM strct_ord_op1o so
                          WHERE so.div_part = v.div_part
                            AND so.cbr_vndr_id = v.cbr_vndr_id)
         GROUP BY v.cbr_vndr_id, v.dcs_vndr_id, v.vndr_nm
         ORDER BY v.cbr_vndr_id;
    ELSE
      l_search_str := '%' || UPPER(i_search_str) || '%';

      OPEN l_cv
       FOR
         SELECT   v.cbr_vndr_id, v.dcs_vndr_id, v.vndr_nm
             FROM div_mstr_di1d d, vndr_mstr_op1v v
            WHERE (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND v.div_part = d.div_part
              AND UPPER(v.vndr_nm) LIKE l_search_str
              AND EXISTS(SELECT 1
                           FROM strct_ord_op1o so
                          WHERE so.div_part = v.div_part
                            AND so.cbr_vndr_id = v.cbr_vndr_id)
         GROUP BY v.cbr_vndr_id, v.dcs_vndr_id, v.vndr_nm
         ORDER BY v.vndr_nm;
    END IF;   -- num.ianb(i_search_str)

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END vndr_search_fn;

  /*
  ||----------------------------------------------------------------------------
  || STRCT_ORD_DTL_LIST_FN
  ||  Returns cursor of Strict Order Detail Info.
  ||
  ||  Parameter format:  (* indicates required)
  ||    OrdStat      : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||    DivIdList    : Division ID list i.e.: ALL or SW~MI~ME
  ||    InclHistSw   : Include order history (Y/N)
  ||    RecapStat    : Recap Status
  ||    LLRFrom      : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (requires LLR_TO)
  ||    LLRTo        : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (requires LLR_FROM)
  ||    VndrList     : CBR Vendor ID list delimited by ~
  ||    ItemList     : Catalog Item List (include leading zeros) delimited by ~
  ||    CrpList      : Corp Code List i.e.: 010~500~501
  ||    CustList     : CustId List (include leading zeros) delimited by ~
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/23/17 | rhalpai | Original for PIR17400
  || 09/12/18 | rhalpai | Add missing join on ordp120b.orditb to sawp505e.catite
  ||                    | in cursor. SDHD-366238
  ||----------------------------------------------------------------------------
  */
  FUNCTION strct_ord_dtl_list_fn(
    i_ord_stat      IN  VARCHAR2 DEFAULT 'O',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_recap_stat    IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from      IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to        IN  VARCHAR2 DEFAULT '2999-12-31',
    i_vndr_list     IN  VARCHAR2 DEFAULT NULL,
    i_item_list     IN  CLOB DEFAULT NULL,
    i_crp_list      IN  VARCHAR2 DEFAULT NULL,
    i_cust_list     IN  CLOB DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.STRCT_ORD_DTL_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_t_divs             type_stab;
    l_t_vndrs            type_stab;
    l_t_items            type_stab;
    l_t_crps             type_stab;
    l_t_custs            type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.add_parm(lar_parm, 'RecapStat', i_recap_stat);
    logs.add_parm(lar_parm, 'LlrFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LlrTo', i_llr_to);
    logs.add_parm(lar_parm, 'VndrList', i_vndr_list);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_incl_hist_sw IS NOT NULL), 'InclHistSw cannot be NULL');
    excp.assert((i_recap_stat IS NOT NULL), 'RecapStat cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
    l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_vndr_list IS NOT NULL THEN
      l_t_vndrs := strsplit_fn(i_vndr_list, op_const_pk.field_delimiter);
    END IF;   -- i_vndr_list IS NOT NULL

    IF i_item_list IS NOT NULL THEN
      l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
    END IF;   -- i_item_list IS NOT NULL

    IF NVL(i_crp_list, 'ALL') <> 'ALL' THEN
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- NVL(i_crp_list, 'ALL') <> 'ALL'

    IF i_cust_list IS NOT NULL THEN
      l_t_custs := strsplit_fn(i_cust_list, op_const_pk.field_delimiter);
    END IF;   -- i_cust_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT 'CURRENT' AS ord_well, d.div_id, v.cbr_vndr_id, v.dcs_vndr_id, v.vndr_nm, e.catite AS catlg_num,
              e.ctdsce AS descr, LPAD(cx.corpb, 3, '0') AS crp_cd, c.retgpc AS grp_cd, a.custa AS cbr_cust,
              cx.storeb AS store_num, cx.mccusb AS mcl_cust, so.ord_num, a.stata AS ord_stat,
              (SELECT COUNT(*)
                 FROM ordp120b b2
                WHERE b2.div_part = a.div_part
                  AND b2.ordnob = a.ordnoa
                  AND b2.lineb = FLOOR(b2.lineb)) AS ord_ln_cnt, so.ord_ln, b.statb AS ln_stat, b.ntshpb AS nt_shp_rsn,
              b.excptn_sw, a.ipdtsa AS ord_src,
              TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                      'YYYY-MM-DD HH24:MI:SS'
                     ) AS prc_ts,
              TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts, ld.load_num,
              TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, TO_CHAR(ld.llr_ts, 'HH24:MI') AS llr_tm,
              TO_CHAR(se.eta_ts, 'YYYY-MM-DD') AS eta_dt, b.ordqtb AS ord_qty, b.pckqtb AS ship_qty,
              TO_CHAR(TO_DATE(b.shpidb, 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS') AS rlse_ts, a.cpoa AS po_num,
              TO_CHAR(so.recap_ts, 'YYYY-MM-DD HH24:MI:SS') AS recap_ts,
              TO_CHAR(so.prod_rcpt_ts, 'YYYY-MM-DD HH24:MI:SS') AS prod_rcpt_ts,
              TO_CHAR(so.llr_at_recap, 'YYYY-MM-DD') AS llr_at_recap, so.stat AS recap_stat, so.recap_qty,
              DECODE(b.statb, 'O', 'Y', 'S', 'Y', 'N') AS sel
         FROM div_mstr_di1d d, strct_item_op3v i, sawp505e e, strct_ord_op1o so, ordp100a a, load_depart_op1f ld,
              mclp020b cx, ordp120b b, stop_eta_op1g se, sysp200c c, vndr_mstr_op1v v
        WHERE d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND i.div_part = d.div_part
          AND (   i_vndr_list IS NULL
               OR i.cbr_vndr_id IN(SELECT TO_NUMBER(t.column_value)
                                     FROM TABLE(l_t_vndrs) t))
          AND e.iteme = i.item_num
          AND e.uome = i.uom
          AND (   i_item_list IS NULL
               OR e.catite MEMBER OF l_t_items)
          AND so.div_part = i.div_part
          AND so.cbr_vndr_id = i.cbr_vndr_id
          AND (   i_recap_stat = 'ALL'
               OR so.stat = i_recap_stat)
          AND a.div_part = so.div_part
          AND a.ordnoa = so.ord_num
          AND a.dsorda IN('R', 'T')
          AND 'Y' =(CASE
                      WHEN i_ord_stat = 'C' THEN 'Y'
                      WHEN a.stata = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND a.stata IN('O', 'S', 'C', 'I') THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND a.stata IN('R', 'A', 'P', 'X') THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL
                      AND a.stata IN('O', 'S') THEN 'Y'
                    END
                   )
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND (   i_crp_list IS NULL
               OR i_crp_list = 'ALL'
               OR cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                                FROM TABLE(l_t_crps) t))
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND b.div_part = so.div_part
          AND b.ordnob = so.ord_num
          AND b.lineb = so.ord_ln
          AND b.orditb = e.catite
          AND 'Y' =(CASE
                      WHEN b.statb = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND b.statb IN('O', 'S', 'C', 'I') THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND b.statb IN('R', 'A', 'P', 'T', 'X') THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL
                      AND b.statb IN('O', 'S') THEN 'Y'
                    END
                   )
          AND b.subrcb < 999
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa
          AND c.div_part = a.div_part
          AND c.acnoc = a.custa
          AND v.div_part = i.div_part
          AND v.cbr_vndr_id = i.cbr_vndr_id
       UNION ALL
       SELECT 'HISTORY' AS ord_well, d.div_id, v.cbr_vndr_id, v.dcs_vndr_id, v.vndr_nm, e.catite AS catlg_num,
              e.ctdsce AS descr, LPAD(cx.corpb, 3, '0') AS crp_cd, c.retgpc AS grp_cd, a.custa AS cbr_cust,
              cx.storeb AS store_num, cx.mccusb AS mcl_cust, so.ord_num, a.stata AS ord_stat,
              (SELECT COUNT(*)
                 FROM ordp920b b2
                WHERE b2.div_part = a.div_part
                  AND b2.ordnob = a.ordnoa
                  AND b2.lineb = FLOOR(b2.lineb)) AS ord_ln_cnt, so.ord_ln, b.statb AS ln_stat, b.ntshpb AS nt_shp_rsn,
              b.excptn_sw, a.ipdtsa AS ord_src,
              TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                      'YYYY-MM-DD HH24:MI:SS'
                     ) AS prc_ts,
              TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts, a.orrtea AS load_num,
              TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt,
              TO_CHAR(TO_DATE('19000228' || LPAD(a.ctofta, 4, '0'), 'YYYYMMDDHH24MI'), 'HH24:MI') AS llr_tm,
              TO_CHAR(DATE '1900-02-28' + a.etadta, 'YYYY-MM-DD') AS eta_dt, b.ordqtb AS ord_qty, b.pckqtb AS ship_qty,
              TO_CHAR(TO_DATE(b.shpidb, 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS') AS rlse_ts, a.cpoa AS po_num,
              TO_CHAR(so.recap_ts, 'YYYY-MM-DD HH24:MI:SS') AS recap_ts,
              TO_CHAR(so.prod_rcpt_ts, 'YYYY-MM-DD HH24:MI:SS') AS prod_rcpt_ts,
              TO_CHAR(so.llr_at_recap, 'YYYY-MM-DD') AS llr_at_recap, so.stat AS recap_stat, so.recap_qty, 'N' AS sel
         FROM div_mstr_di1d d, strct_item_op3v i, sawp505e e, strct_ord_op1o so, ordp900a a, mclp020b cx, ordp920b b,
              sysp200c c, vndr_mstr_op1v v
        WHERE i_incl_hist_sw = 'Y'
          AND d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND i.div_part = d.div_part
          AND (   i_vndr_list IS NULL
               OR i.cbr_vndr_id IN(SELECT TO_NUMBER(t.column_value)
                                     FROM TABLE(l_t_vndrs) t))
          AND e.iteme = i.item_num
          AND e.uome = i.uom
          AND (   i_item_list IS NULL
               OR e.catite MEMBER OF l_t_items)
          AND so.div_part = i.div_part
          AND so.cbr_vndr_id = i.cbr_vndr_id
          AND (   i_recap_stat = 'ALL'
               OR so.stat = i_recap_stat)
          AND a.div_part = so.div_part
          AND a.ordnoa = so.ord_num
          AND a.dsorda IN('R', 'T')
          AND 'Y' =(CASE
                      WHEN i_ord_stat = 'C' THEN 'Y'
                      WHEN a.stata = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND a.stata = 'C' THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND a.stata = 'A' THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL THEN 'N'
                    END
                   )
          AND a.ctofda BETWEEN l_llr_from - DATE '1900-02-28' AND l_llr_to - DATE '1900-02-28'
          AND cx.div_part = a.div_part
          AND cx.custb = a.custa
          AND (   i_crp_list IS NULL
               OR i_crp_list = 'ALL'
               OR cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                                FROM TABLE(l_t_crps) t))
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND b.div_part = so.div_part
          AND b.ordnob = so.ord_num
          AND b.lineb = so.ord_ln
          AND b.orditb = e.catite
          AND 'Y' =(CASE
                      WHEN b.statb = i_ord_stat THEN 'Y'
                      WHEN i_ord_stat = 'U'
                      AND b.statb = 'C' THEN 'Y'
                      WHEN i_ord_stat = 'B'
                      AND b.statb = 'A' THEN 'Y'
                      WHEN i_ord_stat = 'ALL' THEN 'Y'
                      WHEN i_ord_stat IS NULL THEN 'N'
                    END
                   )
          AND b.subrcb < 999
          AND c.div_part = a.div_part
          AND c.acnoc = a.custa
          AND v.div_part = i.div_part
          AND v.cbr_vndr_id = i.cbr_vndr_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END strct_ord_dtl_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_CUT_LIST_FN
  ||  Returns cursor of order quantities to be reduced to meet max by manifest category.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/21 | rhalpai | Original for PIR21276
  || 07/12/21 | rhalpai | Replace hardcoded logo items with reference to sawp505e.logo_sw. PIR21276
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_cut_list_fn(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_excl_tob_sw    IN  VARCHAR2,
    i_excl_logo_sw   IN  VARCHAR2,
    i_mfst_max_list  IN  CLOB,
    i_dtl_sw         IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_CUT_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER        := div_pk.div_part_fn(i_div);
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Divt', i_div);
    logs.add_parm(lar_parm, 'LlrDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ExclTobSw', i_excl_tob_sw);
    logs.add_parm(lar_parm, 'ExclLogoSw', i_excl_logo_sw);
    logs.add_parm(lar_parm, 'MfstMaxList', i_mfst_max_list);
    logs.add_parm(lar_parm, 'DtlSw', i_dtl_sw);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Div cannot be NULL');
    excp.assert((i_llr_dt IS NOT NULL), 'LlrDt cannot be NULL');
    excp.assert((i_excl_tob_sw IN('Y', 'N')), 'ExclTobSw must by Y or N');
    excp.assert((i_excl_logo_sw IN('Y', 'N')), 'ExclLogoSw must by Y or N');
    excp.assert((i_mfst_max_list IS NOT NULL), 'MfstMaxList cannot be NULL');
    excp.assert((i_dtl_sw IN('Y', 'N')), 'DtlSw must by Y or N');
    logs.dbg('Initialize');
    logs.dbg('Get Orders for Qty Reduction');

    OPEN l_cv
     FOR
       WITH mfst AS
            (SELECT t.column1 AS mfst_categ, TO_NUMBER(t.column2) AS mfst_max
               FROM TABLE(framework.lob2table.separatedcolumns(i_mfst_max_list, '|', '~')) t),
            excl AS
            (SELECT e.catite AS excl_item
               FROM sawp505e e
              WHERE e.logo_sw = 'Y'
                AND i_excl_logo_sw = 'Y'
             UNION ALL
             SELECT e.catite AS excl_item
               FROM sawp505e e
              WHERE e.nacse IN('053', '055', '057', '059', '061')
                AND i_excl_tob_sw = 'Y'),
            ord AS
            (SELECT mfst.mfst_categ, mfst.mfst_max, se.cust_id, b.orditb AS catlg_num, b.ordnob AS ord_num, b.lineb AS ord_ln,
                    a.dsorda AS ord_typ, b.ordqtb AS ord_qty,
                    FIRST_VALUE(b.ordnob) OVER(PARTITION BY mfst.mfst_max, se.cust_id, b.orditb ORDER BY a.dsorda DESC, b.ordqtb, b.ordnob) AS frst_ord_num,
                    FIRST_VALUE(b.lineb) OVER(PARTITION BY mfst.mfst_max, se.cust_id, b.orditb ORDER BY a.dsorda DESC, b.ordqtb, b.ordnob) AS frst_ord_ln
               FROM mfst, load_depart_op1f ld, ordp100a a, stop_eta_op1g se, ordp120b b
              WHERE ld.div_part = l_div_part
                AND ld.llr_dt = i_llr_dt
                AND a.div_part = ld.div_part
                AND a.load_depart_sid = ld.load_depart_sid
                AND a.excptn_sw = 'N'
                AND se.div_part = a.div_part
                AND se.load_depart_sid = a.load_depart_sid
                AND se.cust_id = a.custa
                AND b.div_part = a.div_part
                AND b.ordnob = a.ordnoa
                AND b.subrcb < 999
                AND b.excptn_sw = 'N'
                AND b.statb = 'O'
                AND b.ordqtb > 0
                AND b.manctb = mfst.mfst_categ),
            ci AS
            (SELECT   ord.mfst_categ, mfst.mfst_max, COUNT(DISTINCT ord.cust_id || ord.catlg_num) AS cust_item_cnt
                 FROM mfst, ord
                WHERE ord.mfst_categ = mfst.mfst_categ
                  AND ord.ord_typ = 'R'
                  AND ord.catlg_num NOT IN(SELECT excl.excl_item
                                             FROM excl)
             GROUP BY ord.mfst_categ, mfst.mfst_max),
            x AS
            (SELECT   ord.mfst_categ, mfst.mfst_max, ci.cust_item_cnt,
                      SUM((CASE
                             WHEN EXISTS(SELECT 1
                                           FROM excl
                                          WHERE excl. excl_item = ord.catlg_num) THEN ord.ord_qty
                             ELSE 0
                           END)
                         ) AS excl_qty,
                      SUM((CASE
                             WHEN ord.ord_typ = 'D' THEN ord.ord_qty
                             ELSE 0
                           END)
                         ) AS dis_qty,
                      SUM(ord.ord_qty) AS ttl_ord_qty,
                      SUM(ord.ord_qty) - mfst.mfst_max AS cut_qty,
                      SUM((CASE
                             WHEN ord.ord_typ = 'R'
                             AND NOT EXISTS(SELECT 1
                                              FROM excl
                                             WHERE excl.excl_item = ord.catlg_num) THEN ord.ord_qty
                             ELSE 0
                           END)
                         )
                      - NVL(ci.cust_item_cnt, 0) AS ttl_avl_cut_qty,
                      ROUND(1
                            -((SUM(ord.ord_qty) - mfst.mfst_max)
                              / GREATEST(1,
                                         (SUM((CASE
                                                 WHEN ord.ord_typ = 'R'
                                                 AND NOT EXISTS(SELECT 1
                                                                  FROM excl
                                                                 WHERE excl.excl_item = ord.catlg_num) THEN ord.ord_qty
                                                 ELSE 0
                                               END
                                              )
                                             )
                                          - NVL(ci.cust_item_cnt, 0)
                                         )
                                        )
                             ),
                            2
                           ) AS cut_to_pct
                 FROM mfst, ord, ci
                WHERE ord.mfst_categ = mfst.mfst_categ
                  AND ci.mfst_categ(+) = ord.mfst_categ
             GROUP BY ord.mfst_categ, mfst.mfst_max, ci.cust_item_cnt),
            y AS
            (SELECT z.mfst_categ, z.ord_num, z.ord_ln, z.ord_qty, z.new_ord_qty,
                    z.cut_qty
                    - SUM(z.ord_qty - z.new_ord_qty) OVER(PARTITION BY z.mfst_categ ORDER BY z.ord_qty DESC ROWS UNBOUNDED PRECEDING) AS ttl_cut_qty
               FROM (SELECT x.mfst_categ, o.ord_num, o.ord_ln, o.ord_qty, x.cut_qty,
                            (CASE
                               WHEN x.cut_qty > x.ttl_avl_cut_qty THEN(CASE
                                                                         WHEN(    o.ord_num = o.frst_ord_num
                                                                              AND o.ord_ln = o.frst_ord_ln
                                                                             ) THEN 1
                                                                         ELSE 0
                                                                       END
                                                                      )
                               WHEN(    o.ord_num = o.frst_ord_num
                                    AND o.ord_ln = o.frst_ord_ln) THEN GREATEST(1, ROUND(o.ord_qty * x.cut_to_pct))
                               ELSE ROUND(o.ord_qty * x.cut_to_pct)
                             END
                            ) AS new_ord_qty
                       FROM x, ord o
                      WHERE o.mfst_categ = x.mfst_categ
                        AND o.ord_typ = 'R'
                        AND NOT EXISTS(SELECT 1
                                         FROM excl
                                        WHERE excl.excl_item = o.catlg_num)
                        AND x.cut_qty > 0) z)
       SELECT   NULL AS mfst_categ, NULL AS ttl_ord_qty, NULL AS mfst_max, NULL AS ttl_avl_cut_qty, NULL AS cut_qty,
                NULL AS cust_item_cnt, NULL AS excl_qty, NULL AS dis_qty, y.ord_qty, y.new_ord_qty, NULL AS ttl_cut_qty,
                NULL AS new_ttl_ord_qty, y.ord_num, y.ord_ln
           FROM y
          WHERE i_dtl_sw = 'Y'
            AND y.ttl_cut_qty >= 0
            AND y.new_ord_qty <> y.ord_qty
       UNION ALL
       SELECT   x.mfst_categ, x.ttl_ord_qty, x.mfst_max, x.ttl_avl_cut_qty, x.cut_qty, x.cust_item_cnt, x.excl_qty,
                x.dis_qty, SUM(y.ord_qty) AS ord_qty, SUM(y.new_ord_qty) AS new_ord_qty,
                SUM(y.ord_qty - y.new_ord_qty) AS ttl_cut_qty,
                x.ttl_ord_qty - SUM(y.ord_qty - y.new_ord_qty) AS new_ttl_ord_qty, NULL AS ord_num, NULL AS ord_ln
           FROM x, y
          WHERE i_dtl_sw = 'N'
            AND y.mfst_categ(+) = x.mfst_categ
            AND y.ttl_cut_qty(+) >= 0
            AND y.new_ord_qty(+) <> y.ord_qty(+)
       GROUP BY x.mfst_categ, x.mfst_max, x.ttl_ord_qty, x.ttl_avl_cut_qty, x.cut_qty, x.cust_item_cnt, x.excl_qty,
                x.dis_qty
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_cut_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CANCL_ORD_SP
  ||  Cancel orders.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * ParmList     : DivIdOrdNum~DivIdOrdNum
  ||                   Col Len Descr
  ||                     1   2 DivId
  ||                     3  11 OrdNum
  ||  * AuthBy       : Authorized By
  ||  * UserId       : User ID
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancl_ord_sp(
    i_parm_list  IN  CLOB,
    i_auth_by    IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CANCL_ORD_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_t_parms             type_stab;
    l_cv                  SYS_REFCURSOR;
    l_t_div_parts         type_ntab;
    l_t_ord_nums          type_ntab;
    l_dt                  NUMBER        := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_tm                  NUMBER        := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    set_userinfo_ctx.set_userinfo('MC_OP');
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_t_parms := strsplit_fn(i_parm_list, op_const_pk.field_delimiter);
    logs.dbg('Lock Orders');

    OPEN l_cv
     FOR
       SELECT        1
                FROM (SELECT SUBSTR(t.column_value, 1, 2) AS div_id, TO_NUMBER(SUBSTR(t.column_value, 3)) AS ord_num
                        FROM TABLE(l_t_parms) t) x, div_mstr_di1d d, ordp100a a, ordp120b b
               WHERE d.div_id = x.div_id
                 AND a.div_part = d.div_part
                 AND a.ordnoa = x.ord_num
                 AND a.stata IN('O', 'S')
                 AND a.dsorda IN('R', 'T')
                 AND b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
       FOR UPDATE OF a.stata, b.statb;

    logs.dbg('Get Div/OrdNums');

    SELECT a.div_part, a.ordnoa
    BULK COLLECT INTO l_t_div_parts, l_t_ord_nums
      FROM (SELECT SUBSTR(t.column_value, 1, 2) AS div_id, TO_NUMBER(SUBSTR(t.column_value, 3)) AS ord_num
              FROM TABLE(l_t_parms) t) x, div_mstr_di1d d, ordp100a a
     WHERE d.div_id = x.div_id
       AND a.div_part = d.div_part
       AND a.ordnoa = x.ord_num
       AND a.stata IN('O', 'S')
       AND a.dsorda IN('R', 'T');

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Upd OrdHdr');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        UPDATE ordp100a
           SET stata = 'C'
         WHERE div_part = l_t_div_parts(i)
           AND ordnoa = l_t_ord_nums(i);
      logs.dbg('Upd OrdDtl');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        UPDATE ordp120b b
           SET b.statb = 'C'
         WHERE b.div_part = l_t_div_parts(i)
           AND b.ordnob = l_t_ord_nums(i)
           AND b.statb <> 'C';
      logs.dbg('Log');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        INSERT INTO sysp296a
                    (div_part, ordnoa, linea, usera, tblnma, fldnma, florga, flchga, actna, rsncda,
                     datea, timea, autbya
                    )
             VALUES (l_t_div_parts(i), l_t_ord_nums(i), NULL, i_user_id, 'ORDP100A', 'STATA', NULL, 'C', 'C', 'RCANC7',
                     l_dt, l_tm, i_auth_by
                    );
      COMMIT;
    END IF;   -- l_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cancl_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCL_ORD_SP
  ||  Cancel orders. Wrapper for CANCL_ORD_SP.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * CrpList      : Corp Code List i.e.: 010~500~501
  ||  * OrdStat      : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||  * DivIdList    : Division ID list i.e.: ALL or SW~MI~ME
  ||  * OrdTyp       : Order Type (R:Reg,T:Test,ALL:All)
  ||  * OrdSrcList   : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||  * LLRFrom      : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (default 1900-01-01)
  ||  * LLRTo        : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (default 2999-12-31)
  ||  * OrdRcvdAftr  : Search for orders received >= time in YYYY-MM-DD HH24:MI format (default 1900-01-01)
  ||    ItemList     : Catalog Item List (include leading zeros) delimited by ~
  ||    POPrfxList   : List of the beginning characters for PO Numbers delimited by ~
  ||    GrpCdList    : Cust Group List  i.e.: SW006~SW010
  ||    CustList     : CustId List (include leading zeros) delimited by ~
  ||    AuthBy       : Authorized By
  ||    UserId       : User ID
  ||    ScbdCategList: Scoreboard category list delimited by ~
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list, exclusion corp list, scoreboard category list,
  ||                    | exclusion item list. PIR21233
  || 09/09/21 | rhalpai | Change logic for exclusion item list to improve performance. PIR21233
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancl_ord_sp(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB DEFAULT NULL,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_auth_by          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CANCL_ORD_SP';
    lar_parm             logs.tar_parm;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_ord_rcvd_ts        DATE;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_ord_srcs         type_stab;
    l_t_items            type_stab;
    l_t_po_prfxs         type_stab;
    l_t_grps             type_stab;
    l_t_custs            type_stab;
    l_cv                 SYS_REFCURSOR;
    l_parm_list          CLOB;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'OrdSrcList', i_ord_src_list);
    logs.add_parm(lar_parm, 'LlrFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LlrTo', i_llr_to);
    logs.add_parm(lar_parm, 'OrdRcvdAftr', i_ord_rcvd_aftr);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'PoPrfxList', i_po_prfx_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ScbdCategList', i_scbd_categ_list);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
    l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
    l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

    IF i_crp_list = 'ALL' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                                FROM TABLE(framework.lob2table.separatedcolumns(i_crp_list,
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_ord_src_list <> 'ALL' THEN
      l_t_ord_srcs := strsplit_fn(i_ord_src_list, op_const_pk.field_delimiter);
    END IF;   -- i_ord_src_list <> 'ALL'

    IF i_item_list IS NOT NULL THEN
      IF SUBSTR(i_item_list, 1, 1) = '-' THEN
        SELECT x.catite
        BULK COLLECT INTO l_t_items
          FROM (SELECT e.catite
                  FROM sawp505e e
                 WHERE (   i_scbd_categ_list IS NULL
                        OR e.scbcte IN(SELECT tt.column1
                                         FROM TABLE(framework.lob2table.separatedcolumns(i_scbd_categ_list,
                                                                                         op_const_pk.field_delimiter
                                                                                        )
                                                   ) tt)
                       )
                MINUS
                SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                  FROM TABLE(framework.lob2table.separatedcolumns(i_item_list, op_const_pk.field_delimiter)) t) x;
      ELSE
        l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
      END IF;   -- SUBSTR(i_item_list, 1, 1) = '-'
    END IF;   -- i_item_list IS NOT NULL

    IF i_po_prfx_list IS NOT NULL THEN
      l_t_po_prfxs := strsplit_fn(i_po_prfx_list, op_const_pk.field_delimiter);
    END IF;   -- i_po_prfx_list IS NOT NULL

    IF i_grp_list IS NOT NULL THEN
      l_t_grps := strsplit_fn(i_grp_list, op_const_pk.field_delimiter);
    END IF;   -- i_grp_list IS NOT NULL

    IF i_cust_list IS NOT NULL THEN
      l_t_custs := strsplit_fn(i_cust_list, op_const_pk.field_delimiter);
    END IF;   -- i_cust_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT RTRIM(XMLAGG(XMLELEMENT(e, d.div_id || LPAD(a.ordnoa, 11, '0'), op_const_pk.field_delimiter).EXTRACT('//text()')
                          ).getclobval(),
                    op_const_pk.field_delimiter
                   )
         FROM TABLE(l_t_crps) cc, div_mstr_di1d d, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld
        WHERE d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND cx.div_part = d.div_part
          AND cx.corpb = TO_NUMBER(cc.column_value)
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb
          AND (   i_grp_list IS NULL
               OR c.retgpc MEMBER OF l_t_grps)
          AND a.div_part = c.div_part
          AND a.custa = c.acnoc
          AND a.stata IN('O', 'S')
          AND a.stata = DECODE(i_ord_stat, NULL, a.stata, i_ord_stat)
          AND a.dsorda IN('R', 'T')
          AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
          AND (   i_ord_src_list = 'ALL'
               OR a.ipdtsa MEMBER OF l_t_ord_srcs)
          AND a.ord_rcvd_ts >= l_ord_rcvd_ts
          AND (   i_po_prfx_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_po_prfxs) t
                          WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value))
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
          AND (   i_item_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_items) t, sawp505e e, ordp120b b
                          WHERE e.catite = t.column_value
                            AND b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND b.statb IN('O', 'S')
                            AND b.statb = DECODE(i_ord_stat, NULL, b.statb, i_ord_stat)
                            AND b.subrcb < 999
                            AND b.itemnb = e.iteme
                            AND b.sllumb = e.uome)
              );

    FETCH l_cv
     INTO l_parm_list;

    CLOSE l_cv;

    IF l_parm_list IS NOT NULL THEN
    logs.dbg('Cancel Orders');
    cancl_ord_sp(l_parm_list, i_auth_by, i_user_id);
    END IF;   -- l_parm_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancl_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCL_ORD_LN_SP
  ||  Cancel order lines.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * ParmList     : DivIdOrdNumOrdLn~DivIdOrdNumOrdLn
  ||                   Col Len Descr
  ||                     1   2 DivId
  ||                     3  11 OrdNum
  ||                    14   7 OrdLn
  ||  * AuthBy       : Authorized By
  ||  * UserId       : User ID
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 03/15/17 | rhalpai | Add logic to cancel order header when all details have
  ||                    | been cancelled. SDHD-102466
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancl_ord_ln_sp(
    i_parm_list  IN  CLOB,
    i_auth_by    IN  VARCHAR2,
    i_user_id    IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CANCL_ORD_LN_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_t_parms             type_stab;
    l_cv                  SYS_REFCURSOR;
    l_t_div_parts         type_ntab;
    l_t_ord_nums          type_ntab;
    l_t_ord_lns           type_ntab;
    l_dt                  NUMBER        := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_tm                  NUMBER        := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    set_userinfo_ctx.set_userinfo('MC_OP');
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_t_parms := strsplit_fn(i_parm_list, op_const_pk.field_delimiter);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT        b.div_part, b.ordnob, b.lineb
                FROM (SELECT SUBSTR(t.column_value, 1, 2) AS div_id,
                             TO_NUMBER(SUBSTR(t.column_value, 3, 11)) AS ord_num,
                             TO_NUMBER(SUBSTR(t.column_value, 14)) AS ord_ln
                        FROM TABLE(l_t_parms) t) x,
                     div_mstr_di1d d, ordp100a a, ordp120b b
               WHERE d.div_id = x.div_id
                 AND a.div_part = d.div_part
                 AND a.ordnoa = x.ord_num
                 AND a.stata IN('O', 'S')
                 AND a.dsorda IN('R', 'T')
                 AND b.div_part = d.div_part
                 AND b.ordnob = x.ord_num
                 AND b.lineb = x.ord_ln
                 AND b.statb IN('O', 'S')
                 AND b.subrcb < 999
       FOR UPDATE OF b.statb;

    FETCH l_cv
    BULK COLLECT INTO l_t_div_parts, l_t_ord_nums, l_t_ord_lns;

    IF l_t_ord_lns.COUNT > 0 THEN
      logs.dbg('Upd OrdDtl');
      FORALL i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST
        UPDATE ordp120b b
           SET b.statb = 'C'
         WHERE b.div_part = l_t_div_parts(i)
           AND b.ordnob = l_t_ord_nums(i)
           AND b.lineb >= FLOOR(l_t_ord_lns(i))
           AND b.lineb <= FLOOR(l_t_ord_lns(i)) + .99;
      logs.dbg('Log');
      FORALL i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST
        INSERT INTO sysp296a
                    (div_part, ordnoa, linea, usera, tblnma, fldnma, florga, flchga, actna,
                     rsncda, datea, timea, autbya
                    )
             VALUES (l_t_div_parts(i), l_t_ord_nums(i), l_t_ord_lns(i), i_user_id, 'ORDP120B', 'STATB', 'O', 'C', 'M',
                     'RCANC7', l_dt, l_tm, i_auth_by
                    );
      logs.dbg('Cancel OrdHdr When All OrdDtl Are Cancelled');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        UPDATE ordp100a a
           SET a.stata = 'C'
         WHERE a.div_part = l_t_div_parts(i)
           AND a.ordnoa = l_t_ord_nums(i)
           AND a.stata = 'O'
           AND EXISTS(SELECT 1
                        FROM ordp120b b
                       WHERE b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.statb = 'C')
           AND 1 = (SELECT COUNT(DISTINCT b.statb)
                      FROM ordp120b b
                     WHERE b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa);
      COMMIT;
    END IF;   -- l_t_ord_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cancl_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCL_ORD_LN_SP
  ||  Cancel order lines. Wrapper for CANCL_ORD_LN_SP.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * CrpList      : Corp Code List i.e.: 010~500~501
  ||  * OrdStat      : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||  * DivIdList    : Division ID list i.e.: ALL or SW~MI~ME
  ||  * OrdTyp       : Order Type (R:Reg,T:Test,ALL:All)
  ||  * OrdSrcList   : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||  * LLRFrom      : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (default 1900-01-01)
  ||  * LLRTo        : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (default 2999-12-31)
  ||  * OrdRcvdAftr  : Search for orders received >= time in YYYY-MM-DD HH24:MI format (default 1900-01-01)
  ||  * ItemList     : Catalog Item List (include leading zeros) delimited by ~
  ||    POPrfxList   : List of the beginning characters for PO Numbers delimited by ~
  ||    GrpCdList    : Cust Group List  i.e.: SW006~SW010
  ||    CustList     : CustId List (include leading zeros) delimited by ~
  ||    OrdQty       : OrdQty >= passed value (default NULL for All)
  ||    AuthBy       : Authorized By
  ||    UserId       : User ID
  ||    ScbdCategList: Scoreboard category list delimited by ~
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list, exclusion corp list, scoreboard category list,
  ||                    | exclusion item list. PIR21233
  || 09/09/21 | rhalpai | Change logic for exclusion item list to improve performance. PIR21233
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancl_ord_ln_sp(
    i_crp_list         IN  VARCHAR2,
    i_ord_stat         IN  VARCHAR2 DEFAULT 'O',
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_ord_qty          IN  NUMBER DEFAULT NULL,
    i_auth_by          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CANCL_ORD_LN_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_ord_rcvd_ts        DATE;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_ord_srcs         type_stab;
    l_t_items            type_stab;
    l_t_po_prfxs         type_stab;
    l_t_grps             type_stab;
    l_t_custs            type_stab;
    l_parm_list          CLOB;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'OrdStat', i_ord_stat);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'OrdSrcList', i_ord_src_list);
    logs.add_parm(lar_parm, 'LlrFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LlrTo', i_llr_to);
    logs.add_parm(lar_parm, 'OrdRcvdAftr', i_ord_rcvd_aftr);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'PoPrfxList', i_po_prfx_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ScbdCategList', i_scbd_categ_list);
    logs.info('ENTRY', lar_parm);
    set_userinfo_ctx.set_userinfo('MC_OP');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    excp.assert((i_item_list IS NOT NULL), 'ItemList cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
    l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
    l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

    IF i_crp_list = 'ALL' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                                FROM TABLE(framework.lob2table.separatedcolumns(i_crp_list,
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF SUBSTR(i_item_list, 1, 1) = '-' THEN
      SELECT x.catite
      BULK COLLECT INTO l_t_items
        FROM (SELECT e.catite
                FROM sawp505e e
               WHERE (   i_scbd_categ_list IS NULL
                      OR e.scbcte IN(SELECT tt.column1
                                       FROM TABLE(framework.lob2table.separatedcolumns(i_scbd_categ_list,
                                                                                       op_const_pk.field_delimiter
                                                                                      )
                                                 ) tt)
                     )
              MINUS
              SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                FROM TABLE(framework.lob2table.separatedcolumns(i_item_list, op_const_pk.field_delimiter)) t) x;
    ELSE
      l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
    END IF;   -- SUBSTR(i_item_list, 1, 1) = '-'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_ord_src_list <> 'ALL' THEN
      l_t_ord_srcs := strsplit_fn(i_ord_src_list, op_const_pk.field_delimiter);
    END IF;   -- i_ord_src_list <> 'ALL'

    IF i_po_prfx_list IS NOT NULL THEN
      l_t_po_prfxs := strsplit_fn(i_po_prfx_list, op_const_pk.field_delimiter);
    END IF;   -- i_po_prfx_list IS NOT NULL

    IF i_grp_list IS NOT NULL THEN
      l_t_grps := strsplit_fn(i_grp_list, op_const_pk.field_delimiter);
    END IF;   -- i_grp_list IS NOT NULL

    IF i_cust_list IS NOT NULL THEN
      l_t_custs := strsplit_fn(i_cust_list, op_const_pk.field_delimiter);
    END IF;   -- i_cust_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT RTRIM(XMLAGG(XMLELEMENT(e,
                                      d.div_id || LPAD(b.ordnob, 11, '0') || LPAD(b.lineb, 7, '0'),
                                      op_const_pk.field_delimiter
                                     ).EXTRACT('//text()')
                          ).getclobval(),
                    op_const_pk.field_delimiter
                   )
         FROM TABLE(l_t_crps) cc, div_mstr_di1d d, sawp505e e, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld,
              ordp120b b
        WHERE d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND e.catite MEMBER OF l_t_items
          AND cx.div_part = d.div_part
          AND cx.corpb = TO_NUMBER(cc.column_value)
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb
          AND (   i_grp_list IS NULL
               OR c.retgpc MEMBER OF l_t_grps)
          AND a.div_part = c.div_part
          AND a.custa = c.acnoc
          AND a.stata IN('O', 'S')
          AND a.stata = DECODE(i_ord_stat, NULL, a.stata, i_ord_stat)
          AND a.dsorda IN('R', 'T')
          AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
          AND (   i_ord_src_list = 'ALL'
               OR a.ipdtsa MEMBER OF l_t_ord_srcs)
          AND a.ord_rcvd_ts >= l_ord_rcvd_ts
          AND (   i_po_prfx_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_po_prfxs) t
                          WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value))
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
          AND b.div_part = a.div_part
          AND b.ordnob = a.ordnoa
          AND b.statb IN('O', 'S')
          AND b.subrcb < 999
          AND b.statb = DECODE(i_ord_stat, NULL, b.statb, i_ord_stat)
          AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty)
          AND b.itemnb = e.iteme
          AND b.sllumb = e.uome;

    FETCH l_cv
     INTO l_parm_list;

    CLOSE l_cv;

    IF l_parm_list IS NOT NULL THEN
    logs.dbg('Cancel Ord Lns');
    cancl_ord_ln_sp(l_parm_list, i_auth_by, i_user_id);
    END IF;   -- l_parm_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cancl_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_QTY_SP
  ||  Update order quantities.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * ParmList     : DivIdOrdNumOrdLn~DivIdOrdNumOrdLn
  ||                   Col Len Descr
  ||                     1   2 DivId
  ||                     3  11 OrdNum
  ||                    14   7 OrdLn
  ||  * NewOrdQty    : New Order Qty
  ||  * AuthBy       : Authorized By
  ||  * UserId       : User ID
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_qty_sp(
    i_parm_list    IN  CLOB,
    i_new_ord_qty  IN  NUMBER,
    i_auth_by      IN  VARCHAR2,
    i_user_id      IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.UPD_ORD_QTY_SP';
    lar_parm              logs.tar_parm;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_t_parms             type_stab;
    l_cv                  SYS_REFCURSOR;
    l_t_div_parts         type_ntab;
    l_t_ord_nums          type_ntab;
    l_t_ord_lns           type_ntab;
    l_t_ord_qtys          type_ntab;
    l_dt                  NUMBER        := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_tm                  NUMBER        := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.add_parm(lar_parm, 'NewOrdQty', i_new_ord_qty);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    set_userinfo_ctx.set_userinfo('MC_OP');
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    excp.assert((i_new_ord_qty IS NOT NULL), 'NewOrdQty cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_t_parms := strsplit_fn(i_parm_list, op_const_pk.field_delimiter);
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT b.div_part, b.ordnob, b.lineb, b.ordqtb
         FROM (SELECT SUBSTR(t.column_value, 1, 2) AS div_id, TO_NUMBER(SUBSTR(t.column_value, 3, 11)) AS ord_num,
                      TO_NUMBER(SUBSTR(t.column_value, 14)) AS ord_ln
                 FROM TABLE(l_t_parms) t) x,
              div_mstr_di1d d, ordp100a a, ordp120b b
        WHERE d.div_id = x.div_id
          AND a.div_part = d.div_part
          AND a.ordnoa = x.ord_num
          AND a.stata = 'O'
          AND a.dsorda IN('R', 'T')
          AND b.div_part = d.div_part
          AND b.ordnob = x.ord_num
          AND b.lineb = x.ord_ln
          AND b.statb = 'O'
          AND b.subrcb < 999;

    FETCH l_cv
    BULK COLLECT INTO l_t_div_parts, l_t_ord_nums, l_t_ord_lns, l_t_ord_qtys;

    IF l_t_ord_lns.COUNT > 0 THEN
      logs.dbg('Upd OrdDtl');
      FORALL i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST
        UPDATE ordp120b b
           SET b.ordqtb = i_new_ord_qty,
               -- when changing ord-qty from zero reset not-ship-reason for validation
               b.ntshpb = DECODE(b.ntshpb, 'QTYZERO', NULL, b.ntshpb),
               b.excptn_sw = DECODE(b.ntshpb, 'QTYZERO', 'N', b.excptn_sw)
         WHERE b.div_part = l_t_div_parts(i)
           AND b.ordnob = l_t_ord_nums(i)
           AND b.lineb >= FLOOR(l_t_ord_lns(i))
           AND b.lineb <= FLOOR(l_t_ord_lns(i)) + .99;
      logs.dbg('Log');
      FORALL i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST
        INSERT INTO sysp296a
                    (div_part, ordnoa, linea, usera, tblnma, fldnma,
                     florga, flchga, actna, rsncda, datea, timea, autbya
                    )
             VALUES (l_t_div_parts(i), l_t_ord_nums(i), l_t_ord_lns(i), i_user_id, 'ORDP120B', 'ORDQTB',
                     l_t_ord_qtys(i), i_new_ord_qty, 'M', 'QCHG09', l_dt, l_tm, i_auth_by
                    );
      logs.dbg('Validate Ord Dtls');
      FOR i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST LOOP
        op_order_validation_pk.validate_details_sp(l_t_div_parts(i), l_t_ord_nums(i), l_t_ord_lns(i));
      END LOOP;
      COMMIT;
    END IF;   -- l_t_ord_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_ord_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_ORD_QTY_SP
  ||  Update order quantities. Wrapper for UPD_ORD_QTY_SP.
  ||
  ||  Parameter format:  (* indicates required)
  ||  * CrpList      : Corp Code List i.e.: 010~500~501
  ||  * OrdStat      : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||  * DivIdList    : Division ID list i.e.: ALL or SW~MI~ME
  ||  * OrdTyp       : Order Type (R:Reg,T:Test,ALL:All)
  ||  * OrdSrcList   : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||  * LLRFrom      : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (default 1900-01-01)
  ||  * LLRTo        : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (default 2999-12-31)
  ||  * OrdRcvdAftr  : Search for orders received >= time in YYYY-MM-DD HH24:MI format (default 1900-01-01)
  ||  * ItemList     : Catalog Item List (include leading zeros) delimited by ~
  ||    GrpCdList    : Cust Group List  i.e.: SW006~SW010
  ||    POPrfxList   : List of the beginning characters for PO Numbers delimited by ~
  ||    CustList     : CustId List (include leading zeros) delimited by ~
  ||    OrdQty       : OrdQty >= passed value (default NULL for All)
  ||    NewOrdQty    : New Order Qty
  ||    AuthBy       : Authorized By
  ||    UserId       : User ID
  ||    ScbdCategList: Scoreboard category list delimited by ~
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/16 | rhalpai | Original for PIR15190
  || 07/05/21 | rhalpai | Add logic to handle ALL for corp list, exclusion corp list, scoreboard category list,
  ||                    | exclusion item list. PIR21233
  || 09/09/21 | rhalpai | Change logic for exclusion item list to improve performance. PIR21233
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_ord_qty_sp(
    i_crp_list         IN  VARCHAR2,
    i_div_list         IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ          IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list     IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from         IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to           IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr    IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list        IN  CLOB,
    i_po_prfx_list     IN  CLOB DEFAULT NULL,
    i_grp_list         IN  CLOB DEFAULT NULL,
    i_cust_list        IN  CLOB DEFAULT NULL,
    i_ord_qty          IN  NUMBER DEFAULT NULL,
    i_new_ord_qty      IN  NUMBER,
    i_auth_by          IN  VARCHAR2,
    i_user_id          IN  VARCHAR2,
    i_scbd_categ_list  IN  CLOB DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.UPD_ORD_QTY_SP';
    lar_parm             logs.tar_parm;
    l_llr_from           DATE;
    l_llr_to             DATE;
    l_ord_rcvd_ts        DATE;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_ord_srcs         type_stab;
    l_t_items            type_stab;
    l_t_po_prfxs         type_stab;
    l_t_grps             type_stab;
    l_t_custs            type_stab;
    l_cv                 SYS_REFCURSOR;
    l_parm_list          CLOB;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'OrdTyp', i_ord_typ);
    logs.add_parm(lar_parm, 'OrdSrcList', i_ord_src_list);
    logs.add_parm(lar_parm, 'LlrFrom', i_llr_from);
    logs.add_parm(lar_parm, 'LlrTo', i_llr_to);
    logs.add_parm(lar_parm, 'OrdRcvdAftr', i_ord_rcvd_aftr);
    logs.add_parm(lar_parm, 'ItemList', i_item_list);
    logs.add_parm(lar_parm, 'PoPrfxList', i_po_prfx_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'CustList', i_cust_list);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.add_parm(lar_parm, 'NewOrdQty', i_new_ord_qty);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'ScbdCategList', i_scbd_categ_list);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    excp.assert((i_item_list IS NOT NULL), 'ItemList cannot be NULL');
    excp.assert((i_new_ord_qty IS NOT NULL), 'NewOrdQty cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
    l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
    l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

    IF i_crp_list = 'ALL' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c;
    ELSIF SUBSTR(i_crp_list, 1, 1) = '-' THEN
      SELECT LPAD(c.corp_cd, 3, '0')
      BULK COLLECT INTO l_t_crps
        FROM corp_cd_dm1c c
       WHERE c.corp_cd NOT IN(SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                                FROM TABLE(framework.lob2table.separatedcolumns(i_crp_list,
                                                                                op_const_pk.field_delimiter
                                                                               )
                                          ) t);
    ELSE
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list = 'ALL'

    IF SUBSTR(i_item_list, 1, 1) = '-' THEN
      SELECT x.catite
      BULK COLLECT INTO l_t_items
        FROM (SELECT e.catite
                FROM sawp505e e
               WHERE (   i_scbd_categ_list IS NULL
                      OR e.scbcte IN(SELECT tt.column1
                                       FROM TABLE(framework.lob2table.separatedcolumns(i_scbd_categ_list,
                                                                                       op_const_pk.field_delimiter
                                                                                      )
                                                 ) tt)
                     )
              MINUS
              SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                FROM TABLE(framework.lob2table.separatedcolumns(i_item_list, op_const_pk.field_delimiter)) t) x;
    ELSE
      l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
    END IF;   -- SUBSTR(i_item_list, 1, 1) = '-'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    IF i_ord_src_list <> 'ALL' THEN
      l_t_ord_srcs := strsplit_fn(i_ord_src_list, op_const_pk.field_delimiter);
    END IF;   -- i_ord_src_list <> 'ALL'

    IF i_po_prfx_list IS NOT NULL THEN
      l_t_po_prfxs := strsplit_fn(i_po_prfx_list, op_const_pk.field_delimiter);
    END IF;   -- i_po_prfx_list IS NOT NULL

    IF i_grp_list IS NOT NULL THEN
      l_t_grps := strsplit_fn(i_grp_list, op_const_pk.field_delimiter);
    END IF;   -- i_grp_list IS NOT NULL

    IF i_cust_list IS NOT NULL THEN
      l_t_custs := strsplit_fn(i_cust_list, op_const_pk.field_delimiter);
    END IF;   -- i_cust_list IS NOT NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT RTRIM(XMLAGG(XMLELEMENT(e,
                                      d.div_id || LPAD(b.ordnob, 11, '0') || LPAD(b.lineb, 7, '0'),
                                      op_const_pk.field_delimiter
                                     ).EXTRACT('//text()')
                          ).getclobval(),
                    op_const_pk.field_delimiter
                   )
         FROM TABLE(l_t_crps) cc, div_mstr_di1d d, sawp505e e, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld,
              ordp120b b
        WHERE d.div_part > 0
          AND (   i_div_list = 'ALL'
               OR d.div_id MEMBER OF l_t_divs)
          AND e.catite MEMBER OF l_t_items
          AND cx.div_part = d.div_part
          AND cx.corpb = TO_NUMBER(cc.column_value)
          AND (   i_cust_list IS NULL
               OR cx.custb MEMBER OF l_t_custs)
          AND c.div_part = cx.div_part
          AND c.acnoc = cx.custb
          AND (   i_grp_list IS NULL
               OR c.retgpc MEMBER OF l_t_grps)
          AND a.div_part = c.div_part
          AND a.custa = c.acnoc
          AND a.stata = 'O'
          AND a.dsorda IN('R', 'T')
          AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
          AND (   i_ord_src_list = 'ALL'
               OR a.ipdtsa MEMBER OF l_t_ord_srcs)
          AND a.ord_rcvd_ts >= l_ord_rcvd_ts
          AND (   i_po_prfx_list IS NULL
               OR EXISTS(SELECT 1
                           FROM TABLE(l_t_po_prfxs) t
                          WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value))
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
          AND b.div_part = a.div_part
          AND b.ordnob = a.ordnoa
          AND b.statb = 'O'
          AND b.subrcb < 999
          AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty)
          AND b.itemnb = e.iteme
          AND b.sllumb = e.uome;

    FETCH l_cv
     INTO l_parm_list;

    IF l_parm_list IS NOT NULL THEN
    logs.dbg('Upd Ord Qty');
    upd_ord_qty_sp(l_parm_list, i_new_ord_qty, i_auth_by, i_user_id);
    END IF;   -- l_parm_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END upd_ord_qty_sp;

  /*
  ||----------------------------------------------------------------------------
  || ORD_CUT_SP
  ||  Reduce order quantities to meet max by manifest category.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/21 | rhalpai | Original for PIR21276
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ord_cut_sp(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_excl_tob_sw    IN  VARCHAR2,
    i_excl_logo_sw   IN  VARCHAR2,
    i_mfst_max_list  IN  CLOB,
    i_auth_by        IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.ORD_CUT_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER        := div_pk.div_part_fn(i_div);
    l_cv                  SYS_REFCURSOR;
    l_t_junk1             type_stab;
    l_t_junk2             type_stab;
    l_t_junk3             type_stab;
    l_t_junk4             type_stab;
    l_t_junk5             type_stab;
    l_t_junk6             type_stab;
    l_t_junk7             type_stab;
    l_t_junk8             type_stab;
    l_t_junk9             type_stab;
    l_t_junk10            type_stab;
    l_t_ord_qtys          type_ntab;
    l_t_new_ord_qtys      type_ntab;
    l_t_ord_nums          type_ntab;
    l_t_ord_lns           type_ntab;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
    l_dt                  NUMBER        := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_tm                  NUMBER        := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Divt', i_div);
    logs.add_parm(lar_parm, 'LlrDt', i_llr_dt);
    logs.add_parm(lar_parm, 'ExclTobSw', i_excl_tob_sw);
    logs.add_parm(lar_parm, 'ExclLogoSw', i_excl_logo_sw);
    logs.add_parm(lar_parm, 'MfstMaxList', i_mfst_max_list);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Div cannot be NULL');
    excp.assert((i_llr_dt IS NOT NULL), 'LlrDt cannot be NULL');
    excp.assert((i_excl_tob_sw IN('Y', 'N')), 'ExclTobSw must by Y or N');
    excp.assert((i_excl_logo_sw IN('Y', 'N')), 'ExclLogoSw must by Y or N');
    excp.assert((i_mfst_max_list IS NOT NULL), 'MfstMaxList cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    logs.dbg('Get Orders for Qty Reduction');
    l_cv := op_mass_maint_pk.ord_cut_list_fn(i_div, i_llr_dt, i_excl_tob_sw, i_excl_logo_sw, i_mfst_max_list, 'Y');

    FETCH l_cv
    BULK COLLECT INTO l_t_junk1, l_t_junk2, l_t_junk3, l_t_junk4, l_t_junk5, l_t_junk6, l_t_junk7, l_t_junk8,
           l_t_ord_qtys, l_t_new_ord_qtys, l_t_junk9, l_t_junk10, l_t_ord_nums, l_t_ord_lns;

    IF l_t_ord_lns.COUNT > 0 THEN
      logs.dbg('Free Memory');
      l_t_junk1 := NULL;
      l_t_junk2 := NULL;
      l_t_junk3 := NULL;
      l_t_junk4 := NULL;
      l_t_junk5 := NULL;
      l_t_junk6 := NULL;
      l_t_junk7 := NULL;
      l_t_junk8 := NULL;
      l_t_junk9 := NULL;
      l_t_junk10 := NULL;
      logs.dbg('Upd OrdDtl');
      FORALL i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST
        UPDATE ordp120b b
           SET b.ordqtb = l_t_new_ord_qtys(i),
               -- when changing ord-qty from zero reset not-ship-reason for validation
               b.ntshpb = DECODE(b.ntshpb, 'QTYZERO', NULL, b.ntshpb),
               b.excptn_sw = DECODE(b.ntshpb, 'QTYZERO', 'N', b.excptn_sw)
         WHERE b.div_part = l_div_part
           AND b.ordnob = l_t_ord_nums(i)
           AND b.lineb >= FLOOR(l_t_ord_lns(i))
           AND b.lineb <= FLOOR(l_t_ord_lns(i)) + .99
           AND l_t_new_ord_qtys(i) <> l_t_ord_qtys(i);
      logs.dbg('Log');
      FORALL i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST
        INSERT INTO sysp296a
                    (div_part, ordnoa, linea, usera, tblnma, fldnma, florga, flchga, actna, rsncda, datea, timea,
                     autbya)
          SELECT l_div_part, l_t_ord_nums(i), l_t_ord_lns(i), i_user_id, 'ORDP120B', 'ORDQTB', l_t_ord_qtys(i),
                 l_t_new_ord_qtys(i), 'M', 'QCHG09', l_dt, l_tm, i_auth_by
            FROM DUAL
           WHERE l_t_new_ord_qtys(i) <> l_t_ord_qtys(i);
      logs.dbg('Validate Ord Dtls');
      FOR i IN l_t_ord_lns.FIRST .. l_t_ord_lns.LAST LOOP
        op_order_validation_pk.validate_details_sp(l_div_part, l_t_ord_nums(i), l_t_ord_lns(i));
      END LOOP;
--      COMMIT;
    END IF;   -- l_t_ord_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END ord_cut_sp;

  /*
  ||----------------------------------------------------------------------------
  || CPCTY_ORD_MGMT_EXCL_TYP_LIST_FN
  ||  Returns cursor of exclude types.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/08/21 | rhalpai | Original for PIR21367
  ||----------------------------------------------------------------------------
  */
  FUNCTION cpcty_ord_mgmt_excl_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CPCTY_ORD_MGMT_EXCL_TYP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   t.excl_typ, t.descr
           FROM cpcty_ord_mgmt_excl_typ_op3t t
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cpcty_ord_mgmt_excl_typ_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CPCTY_ORD_MGMT_EXCL_CORP_LIST_FN
  ||  Returns cursor of excluded corp codes.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/08/21 | rhalpai | Original for PIR21367
  ||----------------------------------------------------------------------------
  */
  FUNCTION cpcty_ord_mgmt_excl_corp_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CPCTY_ORD_MGMT_EXCL_CORP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   e.val AS corp_cd, c.corp_nm
           FROM cpcty_ord_mgmt_excl_op3e e, corp_cd_dm1c c
          WHERE e.excl_typ = 'CORP'
            AND LPAD(c.corp_cd, 3, '0') = e.val
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cpcty_ord_mgmt_excl_corp_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CPCTY_ORD_MGMT_EXCL_ITEM_LIST_FN
  ||  Returns cursor of excluded items.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/08/21 | rhalpai | Original for PIR21367
  ||----------------------------------------------------------------------------
  */
  FUNCTION cpcty_ord_mgmt_excl_item_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CPCTY_ORD_MGMT_EXCL_ITEM_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT   i.val AS catlg_num, e.shppke AS pck, e.sizee AS sz, e.ctdsce AS item_descr, e.scbcte AS scbd_categ,
                a.desca AS scbd_categ_descr
           FROM cpcty_ord_mgmt_excl_op3e i, sawp505e e, mclp230a a
          WHERE i.excl_typ = 'ITEM'
            AND e.catite = i.val
            AND a.sbcata(+) = e.scbcte
       ORDER BY 1;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cpcty_ord_mgmt_excl_item_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CPCTY_ORD_MGMT_EXCL_MAINT_SP
  ||  Add/Del entries for exclusions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/08/21 | rhalpai | Original for PIR21367
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cpcty_ord_mgmt_excl_maint_sp(
    i_excl_typ  IN  VARCHAR2,
    i_list      IN  CLOB,
    i_user_id   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CPCTY_ORD_MGMT_EXCL_MAINT_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ExclTyp', i_excl_typ);
    logs.add_parm(lar_parm, 'List', i_list);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_excl_typ IS NOT NULL), 'ExclTyp cannot be NULL');
    excp.assert((i_list IS NOT NULL), 'List cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');

    IF SUBSTR(i_list, 1, 1) = '-' THEN
      DELETE FROM cpcty_ord_mgmt_excl_op3e e
            WHERE e.excl_typ = i_excl_typ
              AND e.val IN(SELECT DECODE(ROWNUM, 1, SUBSTR(t.column1, 2), t.column1)
                             FROM TABLE(framework.lob2table.separatedcolumns(i_list, op_const_pk.field_delimiter)) t);
    ELSE
      INSERT INTO cpcty_ord_mgmt_excl_op3e
                  (excl_typ, val, user_id, last_chg_ts)
        SELECT DISTINCT i_excl_typ, t.column1, i_user_id, SYSDATE
                   FROM TABLE(framework.lob2table.separatedcolumns(i_list, op_const_pk.field_delimiter)) t
                  WHERE NOT EXISTS(SELECT 1
                                     FROM cpcty_ord_mgmt_excl_op3e e
                                    WHERE e.excl_typ = i_excl_typ
                                      AND e.val = t.column1);
    END IF;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cpcty_ord_mgmt_excl_maint_sp;

  /*
  ||----------------------------------------------------------------------------
  || CPCTY_ORD_MGMT_ITEM_LIST_FN
  ||  Returns cursor of targeted items by manifest category to apply capacity order management.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/08/21 | rhalpai | Original for PIR21367
  ||----------------------------------------------------------------------------
  */
  FUNCTION cpcty_ord_mgmt_item_list_fn(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_mfst_max_list  IN  CLOB
  ) RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_MAINT_PK.CPCTY_ORD_MGMT_ITEM_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       WITH mfst AS
            (SELECT t.column1 AS mfst_categ, TO_NUMBER(t.column2) AS mfst_max
               FROM TABLE(framework.lob2table.separatedcolumns(i_mfst_max_list, '`', '~')) t),
            xitm AS
            (SELECT e.val AS item
               FROM cpcty_ord_mgmt_excl_op3e e
              WHERE e.excl_typ = 'ITEM'),
            xcrp AS
                                         (SELECT TO_NUMBER(e.val) AS corp
                                            FROM cpcty_ord_mgmt_excl_op3e e
                                           WHERE e.excl_typ = 'CORP'),
            xx AS
            (SELECT   mfst.mfst_categ, mfst.mfst_max, b.orditb AS item, e.ctdsce AS item_descr, w.qavc AS qty_avl,
                      SUM(b.ordqtb) AS ord_qty
                 FROM mfst, xitm, div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, ordp120b b, sawp505e e,
                      whsp300c w
                WHERE d.div_id = i_div
                  AND ld.div_part = d.div_part
                  AND ld.llr_dt = i_llr_dt
                  AND a.div_part = ld.div_part
                  AND a.load_depart_sid = ld.load_depart_sid
                  AND a.dsorda = 'R'
                  AND a.stata = 'O'
                  AND a.excptn_sw = 'N'
                  AND cx.div_part = a.div_part
                  AND cx.custb = a.custa
                  AND cx.corpb NOT IN(SELECT xcrp.corp
                                        FROM xcrp)
                  AND b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.excptn_sw = 'N'
                  AND b.subrcb < 999
                  AND b.statb = 'O'
                  AND b.manctb = mfst.mfst_categ
                  AND e.catite = b.orditb
                  AND e.catite = xitm.item
                  AND w.div_part = b.div_part
                  AND w.itemc = b.itemnb
                  AND w.uomc = b.sllumb
                  AND w.taxjrc IS NULL
             GROUP BY mfst.mfst_categ, mfst.mfst_max, b.orditb, e.ctdsce, w.qavc
             UNION ALL
             SELECT   mfst.mfst_categ, mfst.mfst_max, b.orditb AS item, e.ctdsce AS item_descr, w.qavc AS qty_avl,
                      SUM(b.ordqtb) AS ord_qty
                 FROM mfst, div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, ordp120b b, sawp505e e, whsp300c w
                WHERE d.div_id = i_div
                  AND ld.div_part = d.div_part
                  AND ld.llr_dt = i_llr_dt
                  AND a.div_part = ld.div_part
                  AND a.load_depart_sid = ld.load_depart_sid
                  AND a.dsorda = 'D'
                  AND a.stata = 'O'
                  AND a.excptn_sw = 'N'
                  AND cx.div_part = a.div_part
                  AND cx.custb = a.custa
                  AND b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.excptn_sw = 'N'
                  AND b.subrcb < 999
                  AND b.statb = 'O'
                  AND b.manctb = mfst.mfst_categ
                  AND e.catite = b.orditb
                  AND w.div_part = b.div_part
                  AND w.itemc = b.itemnb
                  AND w.uomc = b.sllumb
                  AND w.taxjrc IS NULL
             GROUP BY mfst.mfst_categ, mfst.mfst_max, b.orditb, e.ctdsce, w.qavc),
            x AS
            (SELECT   xxx.mfst_categ, xxx.mfst_max, SUM(xxx.ord_qty) AS ord_qty,
                      SUM(LEAST(xxx.qty_avl, xxx.ord_qty)) AS ttl_qty
                 FROM (SELECT   xx.mfst_categ, xx.mfst_max, xx.qty_avl, SUM(xx.ord_qty) AS ord_qty
                           FROM xx
                       GROUP BY xx.mfst_categ, xx.mfst_max, xx.qty_avl) xxx
             GROUP BY xxx.mfst_categ, xxx.mfst_max),
            y AS
            (SELECT   mfst.mfst_categ, mfst.mfst_max, b.orditb AS item, e.ctdsce AS item_descr, w.qavc AS qty_avl,
                      SUM(b.ordqtb) AS ord_qty
                 FROM mfst, div_mstr_di1d d, load_depart_op1f ld, ordp100a a, mclp020b cx, ordp120b b, sawp505e e, whsp300c w
                WHERE d.div_id = i_div
                  AND ld.div_part = d.div_part
                  AND ld.llr_dt = i_llr_dt
                  AND a.div_part = ld.div_part
                  AND a.load_depart_sid = ld.load_depart_sid
                  AND a.dsorda = 'R'
                  AND a.stata = 'O'
                  AND a.excptn_sw = 'N'
                  AND cx.div_part = a.div_part
                  AND cx.custb = a.custa
                  AND cx.corpb NOT IN(SELECT xcrp.corp
                                        FROM xcrp)
                  AND b.div_part = a.div_part
                  AND b.ordnob = a.ordnoa
                  AND b.excptn_sw = 'N'
                  AND b.subrcb < 999
                  AND b.statb = 'O'
                  AND b.manctb = mfst.mfst_categ
                  AND e.catite = b.orditb
                  AND e.catite IN(SELECT e2.catite
                                    FROM sawp505e e2
                                  MINUS
                                  SELECT xitm.item
                                    FROM xitm)
                  AND w.div_part = b.div_part
                  AND w.itemc = b.itemnb
                  AND w.uomc = b.sllumb
                  AND w.taxjrc IS NULL
             GROUP BY mfst.mfst_categ, mfst.mfst_max, b.orditb, e.ctdsce, w.qavc),
            z AS
            (SELECT y.mfst_categ, y.mfst_max, x.ttl_qty AS excl_ttl_qty,(y.mfst_max - x.ttl_qty) AS adj_mfst_max, y.item,
                    y.item_descr, y.qty_avl, y.ord_qty,
                    SUM(LEAST(y.qty_avl, y.ord_qty)) OVER(PARTITION BY y.mfst_categ ORDER BY y.ord_qty DESC, y.item ROWS UNBOUNDED PRECEDING) AS ttl_qty
               FROM x, y
              WHERE x.mfst_categ = y.mfst_categ)
       SELECT   z.mfst_categ, RTRIM(XMLAGG(XMLELEMENT(e, z.item || '~')).EXTRACT('//text()').getclobval(), '~') AS item_list
           FROM z
          WHERE z.ttl_qty > z.adj_mfst_max
       GROUP BY z.mfst_categ;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END cpcty_ord_mgmt_item_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CPCTY_ORD_MGMT_APPLY_SP
  ||  Apply capacity order management.  Update order qty to zero to meet volume control limits per manifest category.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/08/21 | rhalpai | Original for PIR21367
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cpcty_ord_mgmt_apply_sp(
    i_div            IN  VARCHAR2,
    i_llr_dt         IN  DATE,
    i_mfst_max_list  IN  CLOB,
    i_auth_by        IN  VARCHAR2,
    i_user_id        IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_MASS_MAINT_PK.CPCTY_ORD_MGMT_APPLY_SP';
    lar_parm             logs.tar_parm;
    l_crp_list           typ.t_maxvc2;
    l_cv                 SYS_REFCURSOR;

    TYPE l_rt_mfst_item IS RECORD(
      mfst_categ  VARCHAR2(3),
      item_list   CLOB
    );

    l_r_mfst_item        l_rt_mfst_item;
    l_data_found_sw      VARCHAR2(1)    := 'N';
    l_ord_typ            VARCHAR2(1)    := 'R';
    l_ord_src_list       typ.t_maxvc2   := 'ALL';
    l_llr_from           VARCHAR2(10);
    l_llr_to             VARCHAR2(10);
    l_ord_rcvd_aftr      VARCHAR2(10)   := '1900-01-01';
    l_po_prfx_list       CLOB;
    l_grp_list           CLOB;
    l_cust_list          CLOB;
    l_ord_qty            NUMBER;
    l_new_ord_qty        NUMBER         := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ExclTyp', i_div);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.add_parm(lar_parm, 'MfstMaxList', i_mfst_max_list);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Div cannot be NULL');
    excp.assert((i_llr_dt IS NOT NULL), 'LLRDt cannot be NULL');
    excp.assert((i_mfst_max_list IS NOT NULL), 'MfstMaxList cannot be NULL');
    excp.assert((i_auth_by IS NOT NULL), 'AuthBy cannot be NULL');
    excp.assert((i_user_id IS NOT NULL), 'UserId cannot be NULL');
    logs.dbg('Initialize');
    l_llr_from := TO_CHAR(i_llr_dt, 'YYYY-MM-DD');
    l_llr_to := l_llr_from;
    logs.dbg('Get Corp List');

    SELECT '-' || LISTAGG(e.val, '~') WITHIN GROUP (ORDER BY e.val)
      INTO l_crp_list
      FROM cpcty_ord_mgmt_excl_op3e e
     WHERE e.excl_typ = 'CORP';

    logs.dbg('Get Mfst Item List');
    l_cv := op_mass_maint_pk.cpcty_ord_mgmt_item_list_fn(i_div, i_llr_dt, i_mfst_max_list);
    logs.dbg('Upd Order Qty to Zero');
    LOOP
      FETCH l_cv
       INTO l_r_mfst_item;

      EXIT WHEN l_cv%NOTFOUND;
      l_data_found_sw := 'Y';
      op_mass_maint_pk.upd_ord_qty_sp(l_crp_list,
                                       i_div,
                                       l_ord_typ,
                                       l_ord_src_list,
                                       l_llr_from,
                                       l_llr_to,
                                       l_ord_rcvd_aftr,
                                       l_r_mfst_item.item_list,
                                       l_po_prfx_list,
                                       l_grp_list,
                                       l_cust_list,
                                       l_ord_qty,
                                      l_new_ord_qty,
                                       i_auth_by,
                                       i_user_id
                                      );
    END LOOP;
    excp.assert((l_data_found_sw = 'Y'), 'No data found to process');
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END cpcty_ord_mgmt_apply_sp;
END op_mass_maint_pk;
/

