CREATE OR REPLACE PACKAGE op_mass_inquiry_pk IS
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

  FUNCTION corp_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION div_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_src_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION grp_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION cust_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_grp_list      IN  CLOB DEFAULT NULL,
    i_search_str    IN  VARCHAR2 DEFAULT NULL,
    i_store_num_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR;

  FUNCTION item_list_fn(
    i_search_str  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_list_fn(
    i_incl_hist_sw       IN  VARCHAR2 DEFAULT 'N',
    i_crp_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_stat           IN  VARCHAR2 DEFAULT 'O',
    i_div_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ            IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list       IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from           IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to             IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr      IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list          IN  CLOB DEFAULT NULL,
    i_po_prfx_list       IN  CLOB DEFAULT NULL,
    i_grp_list           IN  CLOB DEFAULT NULL,
    i_cust_list          IN  CLOB DEFAULT NULL,
    i_dist_id_prfx_list  IN  CLOB DEFAULT NULL,
    i_ord_num_list       IN  CLOB DEFAULT NULL,
    i_ord_qty            IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_hdr_list_fn(
    i_incl_hist_sw       IN  VARCHAR2 DEFAULT 'N',
    i_crp_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_stat           IN  VARCHAR2 DEFAULT 'O',
    i_div_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ            IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list       IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from           IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to             IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr      IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list          IN  CLOB DEFAULT NULL,
    i_po_prfx_list       IN  CLOB DEFAULT NULL,
    i_grp_list           IN  CLOB DEFAULT NULL,
    i_cust_list          IN  CLOB DEFAULT NULL,
    i_dist_id_prfx_list  IN  CLOB DEFAULT NULL,
    i_ord_num_list       IN  CLOB DEFAULT NULL,
    i_ord_qty            IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

  FUNCTION ord_dtl_list_fn(
    i_parm_list  IN  CLOB
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------

END op_mass_inquiry_pk;
/

CREATE OR REPLACE PACKAGE BODY op_mass_inquiry_pk IS
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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_stat_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ORD_STAT_LIST_FN';
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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_typ_list_fn
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ORD_TYP_LIST_FN';
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
       SELECT 'D' AS typ_cd, 'Dist' AS typ_descr
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
  || CORP_LIST_FN
  ||  Returns cursor of Corp Codes with existing orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION corp_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.CORP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');

    OPEN l_cv
     FOR
       SELECT 'ALL' AS corp_cd, 'ALL CORP CODES' AS corp_nm
         FROM DUAL
       UNION ALL
       SELECT x.corp_cd, x.corp_nm
         FROM (SELECT   LPAD(cc.corp_cd, 3, '0') AS corp_cd, cc.corp_nm
                   FROM corp_cd_dm1c cc
                  WHERE EXISTS(SELECT 1
                                 FROM mclp020b cx
                                WHERE cx.corpb = cc.corp_cd
                                  AND (   EXISTS(SELECT 1
                                                   FROM ordp100a a
                                                  WHERE a.div_part = cx.div_part
                                                    AND a.custa = cx.custb)
                                       OR (    i_incl_hist_sw = 'Y'
                                           AND EXISTS(SELECT 1
                                                        FROM ordp900a a
                                                       WHERE a.div_part = cx.div_part
                                                         AND a.custa = cx.custb)
                                          )
                                      ))
               ORDER BY cc.corp_cd) x;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION div_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.DIV_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list <> 'ALL' THEN
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list <> 'ALL'

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   d.div_id, d.div_nm, d.div_part
           FROM div_mstr_di1d d
          WHERE EXISTS(SELECT 1
                         FROM mclp020b cx
                        WHERE cx.div_part = d.div_part
                          AND (   i_crp_list = 'ALL'
                               OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                                FROM TABLE(l_t_crps) cc))
                          AND (   EXISTS(SELECT 1
                                           FROM ordp100a a
                                          WHERE a.div_part = cx.div_part
                                            AND a.custa = cx.custb)
                               OR (    i_incl_hist_sw = 'Y'
                                   AND EXISTS(SELECT 1
                                                FROM ordp900a a
                                               WHERE a.div_part = cx.div_part
                                                 AND a.custa = cx.custb)
                                  )
                              ))
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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_src_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ORD_SRC_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list <> 'ALL' THEN
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list <> 'ALL'

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
                         OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                          FROM TABLE(l_t_crps) cc))
                    AND a.div_part = cx.div_part
                    AND a.custa = cx.custb
                    AND a.ipdtsa IS NOT NULL
               UNION
               SELECT   a.ipdtsa AS ord_src
                   FROM div_mstr_di1d d, mclp020b cx, ordp900a a
                  WHERE i_incl_hist_sw = 'Y'
                    AND d.div_part > 0
                    AND (   i_div_list = 'ALL'
                         OR d.div_id MEMBER OF l_t_divs)
                    AND cx.div_part = d.div_part
                    AND (   i_crp_list = 'ALL'
                         OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                          FROM TABLE(l_t_crps) cc))
                    AND a.div_part = cx.div_part
                    AND a.custa = cx.custb
                    AND a.ipdtsa IS NOT NULL
               ORDER BY 1) y;

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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION grp_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.GRP_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list <> 'ALL' THEN
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list <> 'ALL'

    IF i_div_list <> 'ALL' THEN
      l_t_divs := strsplit_fn(i_div_list, op_const_pk.field_delimiter);
    END IF;   -- i_div_list <> 'ALL'

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   g.cstgpa AS grp_cd, d.div_id || g.group_name AS grp_nm
           FROM div_mstr_di1d d, mclp100a g, sysp200c c, mclp020b cx
          WHERE d.div_part > 0
            AND (   i_div_list = 'ALL'
                 OR d.div_id MEMBER OF l_t_divs)
            AND g.div_part = d.div_part
            AND c.div_part = g.div_part
            AND c.retgpc = g.cstgpa
            AND cx.div_part = c.div_part
            AND cx.custb = c.acnoc
            AND (   i_crp_list = 'ALL'
                 OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                  FROM TABLE(l_t_crps) cc))
            AND (   EXISTS(SELECT 1
                             FROM ordp100a a
                            WHERE a.div_part = cx.div_part
                              AND a.custa = cx.custb)
                 OR (    i_incl_hist_sw = 'Y'
                     AND EXISTS(SELECT 1
                                  FROM ordp900a a
                                 WHERE a.div_part = cx.div_part
                                   AND a.custa = cx.custb)
                    )
                )
       GROUP BY g.cstgpa, d.div_id, g.group_name
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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION cust_list_fn(
    i_incl_hist_sw  IN  VARCHAR2 DEFAULT 'N',
    i_crp_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_div_list      IN  VARCHAR2 DEFAULT 'ALL',
    i_grp_list      IN  CLOB DEFAULT NULL,
    i_search_str    IN  VARCHAR2 DEFAULT NULL,
    i_store_num_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.CUST_LIST_FN';
    lar_parm             logs.tar_parm;
    l_t_crps             type_stab;
    l_t_divs             type_stab;
    l_t_grps             type_stab;
    l_search_str         typ.t_maxcol;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
    logs.add_parm(lar_parm, 'CrpList', i_crp_list);
    logs.add_parm(lar_parm, 'DivList', i_div_list);
    logs.add_parm(lar_parm, 'GrpList', i_grp_list);
    logs.add_parm(lar_parm, 'SearchStr', i_search_str);
    logs.add_parm(lar_parm, 'StoreNumSw', i_store_num_sw);
    logs.dbg('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    logs.dbg('Initialize');

    IF i_crp_list <> 'ALL' THEN
      l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
    END IF;   -- i_crp_list <> 'ALL'

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
           SELECT d.div_id, c.acnoc AS cust_id, cx.storeb AS store_num, c.namec AS cust_nm, c.shpctc AS city,
                  c.shpstc AS st, DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND cx.div_part = d.div_part
              AND (   i_crp_list = 'ALL'
                   OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                    FROM TABLE(l_t_crps) cc))
              AND c.div_part = cx.div_part
              AND c.acnoc = cx.custb
              AND (   i_grp_list IS NULL
                   OR c.retgpc MEMBER OF l_t_grps)
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = c.div_part
                                AND a.custa = c.acnoc)
                   OR (    i_incl_hist_sw = 'Y'
                       AND EXISTS(SELECT 1
                                    FROM ordp900a a
                                   WHERE a.div_part = c.div_part
                                     AND a.custa = c.acnoc))
                  );
      WHEN i_store_num_sw = 'Y' THEN
        OPEN l_cv
         FOR
           SELECT d.div_id, c.acnoc AS cust_id, cx.storeb AS store_num, c.namec AS cust_nm, c.shpctc AS city,
                  c.shpstc AS st, DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND c.div_part = d.div_part
              AND cx.storeb LIKE i_search_str || '%'
              AND (   i_grp_list IS NULL
                   OR c.retgpc MEMBER OF l_t_grps)
              AND cx.div_part = c.div_part
              AND cx.custb = c.acnoc
              AND (   i_crp_list = 'ALL'
                   OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                    FROM TABLE(l_t_crps) cc))
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = c.div_part
                                AND a.custa = c.acnoc)
                   OR (    i_incl_hist_sw = 'Y'
                       AND EXISTS(SELECT 1
                                    FROM ordp900a a
                                   WHERE a.div_part = c.div_part
                                     AND a.custa = c.acnoc))
                  );
      WHEN num.ianb(i_search_str) THEN
        OPEN l_cv
         FOR
           SELECT d.div_id, c.acnoc AS cust_id, cx.storeb AS store_num, c.namec AS cust_nm, c.shpctc AS city,
                  c.shpstc AS st, DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
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
                   OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                    FROM TABLE(l_t_crps) cc))
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = c.div_part
                                AND a.custa = c.acnoc)
                   OR (    i_incl_hist_sw = 'Y'
                       AND EXISTS(SELECT 1
                                    FROM ordp900a a
                                   WHERE a.div_part = c.div_part
                                     AND a.custa = c.acnoc))
                  );
      ELSE
        l_search_str := '%' || UPPER(i_search_str) || '%';

        OPEN l_cv
         FOR
           SELECT d.div_id, c.acnoc AS cust_id, cx.storeb AS store_num, c.namec AS cust_nm, c.shpctc AS city,
                  c.shpstc AS st, DECODE(c.statc, '1', 'ACT', '2', 'INA', '3', 'HLD', '4', 'TST') AS stat
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
                   OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                    FROM TABLE(l_t_crps) cc))
              AND (   EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = c.div_part
                                AND a.custa = c.acnoc)
                   OR (    i_incl_hist_sw = 'Y'
                       AND EXISTS(SELECT 1
                                    FROM ordp900a a
                                   WHERE a.div_part = c.div_part
                                     AND a.custa = c.acnoc))
                  );
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
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION item_list_fn(
    i_search_str  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ITEM_LIST_FN';
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
  || ORD_LIST_FN
  ||  Returns cursor of Order Header Info with cursor of Order Detail Info.
  ||
  ||  Parameter format:
  ||   InclHistSw    : Include History (Y,N)
  ||   CrpList       : Corp Code List i.e.: 010~500~501
  ||   OrdStat       : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||   DivIdList     : Division ID list i.e.: ALL or SW~MI~ME
  ||   OrdTyp        : Order Type (R:Reg,T:Test,ALL:All)
  ||   OrdSrcList    : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||   LLRFrom       : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (requires LLR_TO)
  ||   LLRTo         : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (requires LLR_FROM)
  ||   OrdRcvdAftr   : Search for orders received >= time in YYYY-MM-DD HH24:MI format
  ||   ItemList      : Catalog Item List (include leading zeros) delimited by ~
  ||   POPrfxList    : List of the beginning characters for PO Numbers delimited by ~
  ||   GrpCdList     : Cust Group List  i.e.: SW006~SW010
  ||   CustList      : CustId List (include leading zeros) delimited by ~
  ||   DistIdPrfxList: List of the begining characters for Distribution ID delimited by ~
  ||   OrdNumList    : List of Order Numbers delimited by ~
  ||   OrdQty        : OrdQty >= passed value (default NULL for All)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/12/18 | rhalpai | Original for PIR17701
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_list_fn(
    i_incl_hist_sw       IN  VARCHAR2 DEFAULT 'N',
    i_crp_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_stat           IN  VARCHAR2 DEFAULT 'O',
    i_div_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ            IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list       IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from           IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to             IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr      IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list          IN  CLOB DEFAULT NULL,
    i_po_prfx_list       IN  CLOB DEFAULT NULL,
    i_grp_list           IN  CLOB DEFAULT NULL,
    i_cust_list          IN  CLOB DEFAULT NULL,
    i_dist_id_prfx_list  IN  CLOB DEFAULT NULL,
    i_ord_num_list       IN  CLOB DEFAULT NULL,
    i_ord_qty            IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ORD_LIST_FN';
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
    l_t_dist_id_prfxs    type_stab;
    l_t_ord_nums         type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
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
    logs.add_parm(lar_parm, 'DistIdPrfxList', i_dist_id_prfx_list);
    logs.add_parm(lar_parm, 'OrdNumList', i_ord_num_list);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    logs.dbg('Initialize');

    IF i_item_list IS NOT NULL THEN
      l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
    END IF;   -- i_item_list IS NOT NULL

    IF i_ord_num_list IS NOT NULL THEN
      l_t_ord_nums := strsplit_fn(i_ord_num_list, op_const_pk.field_delimiter);
      logs.dbg('Open OrdNum Cursor');

      OPEN l_cv
       FOR
         WITH ord AS(
           SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                  cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                  a.ipdtsa AS ord_src, a.cpoa AS po_num, TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num,
                  se.stop_num, TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                  (SELECT COUNT(*)
                     FROM ordp120b b
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.excptn_sw,
                  DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                  DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                  a.ldtypa AS load_typ, 'N' AS hist_sw
             FROM ordp100a a, div_mstr_di1d d, mclp020b cx, sysp200c c, load_depart_op1f ld, stop_eta_op1g se
            WHERE a.ordnoa IN(SELECT TO_NUMBER(o.column_value)
                                FROM TABLE(l_t_ord_nums) o)
              AND d.div_part = a.div_part
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
              AND c.div_part = a.div_part
              AND c.acnoc = a.custa
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND se.div_part(+) = a.div_part
              AND se.load_depart_sid(+) = a.load_depart_sid
              AND se.cust_id(+) = a.custa
              AND (   (    i_item_list IS NULL
                       AND i_ord_qty IS NULL)
                   OR EXISTS(SELECT 1
                               FROM ordp120b b
                              WHERE b.div_part = a.div_part
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
                                AND (   i_item_list IS NULL
                                     OR b.orditb MEMBER OF l_t_items)
                                AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                  )
           UNION ALL
           SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                  cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                  a.ipdtsa AS ord_src, a.cpoa AS po_num, TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt,
                  a.orrtea AS load_num, a.stopsa AS stop_num,
                  TO_CHAR(TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta,
                          'YYYY-MM-DD HH24:MI'
                         ) AS eta_ts,
                  TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                  (SELECT COUNT(*)
                     FROM ordp920b b
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.excptn_sw,
                  DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                  DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                  a.ldtypa AS load_typ, 'Y' AS hist_sw
             FROM ordp900a a, div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE i_incl_hist_sw = 'Y'
              AND a.ordnoa IN(SELECT TO_NUMBER(o.column_value)
                                FROM TABLE(l_t_ord_nums) o)
              AND d.div_part = a.div_part
              AND cx.div_part = a.div_part
              AND cx.custb = a.custa
              AND c.div_part = a.div_part
              AND c.acnoc = a.custa
              AND (   (    i_item_list IS NULL
                       AND i_ord_qty IS NULL)
                   OR EXISTS(SELECT 1
                               FROM ordp920b b
                              WHERE b.div_part = a.div_part
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
                                AND (   i_item_list IS NULL
                                     OR b.orditb MEMBER OF l_t_items)
                                AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                  )
         )
         SELECT o.corp_cd, o.div_id, o.grp_cd, o.cust_id, o.store_num, o.ord_num, o.ord_stat, o.ord_typ, o.ord_src,
                o.po_num, o.llr_dt, o.load_num, o.stop_num, o.eta_ts, o.ord_rcvd_ts, o.ord_ln_cnt, o.excptn_sw,
                o.dist_id, o.ship_dt, o.load_typ, o.hist_sw,
                CURSOR(SELECT   b.lineb AS ord_ln, b.statb AS ln_stat, e.catite AS catlg_num, e.ctdsce AS item_descr,
                                e.shppke AS pack, e.sizee AS sz, b.orgqtb AS org_qty, b.ordqtb AS ord_qty,
                                b.pckqtb AS pck_qty, e.upce AS rtl_upc, b.hdprcb AS prc_amt, b.hdrtab AS rtl_amt,
                                b.manctb AS mfst_categ, b.totctb AS tote_categ, LPAD(b.labctb, 3, '0') AS labl_categ,
                                LPAD(b.invctb, 3, '0') AS invc_categ,
                                TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                                        'YYYY-MM-DD HH24:MI:SS'
                                       ) AS prc_ts,
                                b.ntshpb AS nt_shp_rsn, log_rsn_udf(b.div_part, b.ordnob, b.lineb) AS log_rsn,
                                b.excptn_sw
                           FROM ordp120b b, sawp505e e
                          WHERE o.hist_sw = 'N'
                            AND b.div_part = o.div_part
                            AND b.ordnob = o.ord_num
                            AND e.catite = b.orditb
                       UNION ALL
                       SELECT   b.lineb AS ord_ln, b.statb AS ln_stat, e.catite AS catlg_num, e.ctdsce AS item_descr,
                                e.shppke AS pack, e.sizee AS sz, b.orgqtb AS org_qty, b.ordqtb AS ord_qty,
                                b.pckqtb AS pck_qty, e.upce AS rtl_upc, b.hdprcb AS prc_amt, b.hdrtab AS rtl_amt,
                                b.manctb AS mfst_categ, b.totctb AS tote_categ, LPAD(b.labctb, 3, '0') AS labl_categ,
                                LPAD(b.invctb, 3, '0') AS invc_categ,
                                TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                                        'YYYY-MM-DD HH24:MI:SS'
                                       ) AS prc_ts,
                                b.ntshpb AS nt_shp_rsn, log_rsn_udf(b.div_part, b.ordnob, b.lineb) AS log_rsn,
                                b.excptn_sw
                           FROM ordp920b b, sawp505e e
                          WHERE o.hist_sw = 'Y'
                            AND b.div_part = o.div_part
                            AND b.ordnob = o.ord_num
                            AND e.catite = b.orditb
                       ORDER BY ord_ln
                      ) AS dtl_cur
           FROM ord o;
    ELSE
      l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
      l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
      l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

      IF i_crp_list <> 'ALL' THEN
        l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
      END IF;   -- i_crp_list <> 'ALL'

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

      IF i_dist_id_prfx_list IS NOT NULL THEN
        l_t_dist_id_prfxs := strsplit_fn(i_dist_id_prfx_list, op_const_pk.field_delimiter);
      END IF;   -- i_dist_id_prfx_list IS NOT NULL

      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         WITH ord AS(
           SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                  cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                  a.ipdtsa AS ord_src, a.cpoa AS po_num, TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num,
                  se.stop_num, TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                  TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                  (SELECT COUNT(*)
                     FROM ordp120b b
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.excptn_sw,
                  DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                  DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                  a.ldtypa AS load_typ, 'N' AS hist_sw
             FROM div_mstr_di1d d, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
            WHERE d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND cx.div_part = d.div_part
              AND (   i_crp_list = 'ALL'
                   OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                    FROM TABLE(l_t_crps) cc))
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
              AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
              AND (   i_ord_src_list = 'ALL'
                   OR a.ipdtsa MEMBER OF l_t_ord_srcs)
              AND a.ord_rcvd_ts >= l_ord_rcvd_ts
              AND (   i_po_prfx_list IS NULL
                   OR EXISTS(SELECT 1
                               FROM TABLE(l_t_po_prfxs) t
                              WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value))
              AND (   i_dist_id_prfx_list IS NULL
                   OR (    a.dsorda = 'D'
                       AND EXISTS(SELECT 1
                                    FROM TABLE(l_t_dist_id_prfxs) t
                                   WHERE SUBSTR(a.legrfa, 1, LENGTH(t.column_value)) = t.column_value))
                  )
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
              AND se.div_part(+) = a.div_part
              AND se.load_depart_sid(+) = a.load_depart_sid
              AND se.cust_id(+) = a.custa
              AND (   (    i_item_list IS NULL
                       AND i_ord_qty IS NULL)
                   OR EXISTS(SELECT 1
                               FROM ordp120b b
                              WHERE b.div_part = a.div_part
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
                                AND (   i_item_list IS NULL
                                     OR b.orditb MEMBER OF l_t_items)
                                AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                  )
           UNION ALL
           SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                  cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                  a.ipdtsa AS ord_src, a.cpoa AS po_num, TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt,
                  a.orrtea AS load_num, a.stopsa AS stop_num,
                  TO_CHAR(TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta,
                          'YYYY-MM-DD HH24:MI'
                         ) AS eta_ts,
                  TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                  (SELECT COUNT(*)
                     FROM ordp920b b
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt, a.excptn_sw,
                  DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                  DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                  a.ldtypa AS load_typ, 'Y' AS hist_sw
             FROM ordp900a a, div_mstr_di1d d, mclp020b cx, sysp200c c
            WHERE i_incl_hist_sw = 'Y'
              AND d.div_part > 0
              AND (   i_div_list = 'ALL'
                   OR d.div_id MEMBER OF l_t_divs)
              AND cx.div_part = d.div_part
              AND (   i_crp_list = 'ALL'
                   OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                    FROM TABLE(l_t_crps) cc))
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
              AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
              AND (   i_ord_src_list = 'ALL'
                   OR a.ipdtsa MEMBER OF l_t_ord_srcs)
              AND a.ord_rcvd_ts >= l_ord_rcvd_ts
              AND (   i_po_prfx_list IS NULL
                   OR EXISTS(SELECT 1
                               FROM TABLE(l_t_po_prfxs) t
                              WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value))
              AND (   i_dist_id_prfx_list IS NULL
                   OR (    a.dsorda = 'D'
                       AND EXISTS(SELECT 1
                                    FROM TABLE(l_t_dist_id_prfxs) t
                                   WHERE SUBSTR(a.legrfa, 1, LENGTH(t.column_value)) = t.column_value))
                  )
              AND a.ctofda >= l_llr_from - DATE '1900-02-28'
              AND a.ctofda <= l_llr_to - DATE '1900-02-28'
              AND (   (    i_item_list IS NULL
                       AND i_ord_qty IS NULL)
                   OR EXISTS(SELECT 1
                               FROM ordp920b b
                              WHERE b.div_part = a.div_part
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
                                AND (   i_item_list IS NULL
                                     OR b.orditb MEMBER OF l_t_items)
                                AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                  )
         )
         SELECT o.corp_cd, o.div_id, o.grp_cd, o.cust_id, o.store_num, o.ord_num, o.ord_stat, o.ord_typ, o.ord_src,
                o.po_num, o.llr_dt, o.load_num, o.stop_num, o.eta_ts, o.ord_rcvd_ts, o.ord_ln_cnt, o.excptn_sw,
                o.dist_id, o.ship_dt, o.load_typ, o.hist_sw,
                CURSOR(SELECT   b.lineb AS ord_ln, b.statb AS ln_stat, e.catite AS catlg_num, e.ctdsce AS item_descr,
                                e.shppke AS pack, e.sizee AS sz, b.orgqtb AS org_qty, b.ordqtb AS ord_qty,
                                b.pckqtb AS pck_qty, e.upce AS rtl_upc, b.hdprcb AS prc_amt, b.hdrtab AS rtl_amt,
                                b.manctb AS mfst_categ, b.totctb AS tote_categ, LPAD(b.labctb, 3, '0') AS labl_categ,
                                LPAD(b.invctb, 3, '0') AS invc_categ,
                                TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                                        'YYYY-MM-DD HH24:MI:SS'
                                       ) AS prc_ts,
                                b.ntshpb AS nt_shp_rsn, log_rsn_udf(b.div_part, b.ordnob, b.lineb) AS log_rsn,
                                b.excptn_sw
                           FROM ordp120b b, sawp505e e
                          WHERE o.hist_sw = 'N'
                            AND b.div_part = o.div_part
                            AND b.ordnob = o.ord_num
                            AND e.catite = b.orditb
                       UNION ALL
                       SELECT   b.lineb AS ord_ln, b.statb AS ln_stat, e.catite AS catlg_num, e.ctdsce AS item_descr,
                                e.shppke AS pack, e.sizee AS sz, b.orgqtb AS org_qty, b.ordqtb AS ord_qty,
                                b.pckqtb AS pck_qty, e.upce AS rtl_upc, b.hdprcb AS prc_amt, b.hdrtab AS rtl_amt,
                                b.manctb AS mfst_categ, b.totctb AS tote_categ, LPAD(b.labctb, 3, '0') AS labl_categ,
                                LPAD(b.invctb, 3, '0') AS invc_categ,
                                TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                                        'YYYY-MM-DD HH24:MI:SS'
                                       ) AS prc_ts,
                                b.ntshpb AS nt_shp_rsn, log_rsn_udf(b.div_part, b.ordnob, b.lineb) AS log_rsn,
                                b.excptn_sw
                           FROM ordp920b b, sawp505e e
                          WHERE o.hist_sw = 'Y'
                            AND b.div_part = o.div_part
                            AND b.ordnob = o.ord_num
                            AND e.catite = b.orditb
                       ORDER BY ord_ln
                      ) AS dtl_cur
           FROM ord o;
    END IF;   -- i_ord_num_list IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN excp.gx_assert_fail THEN
      logs.warn('Assertion Failure: ' || SQLERRM, lar_parm);
      RAISE;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ord_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || ORD_HDR_LIST_FN
  ||  Returns cursor of Order Header Info.
  ||
  ||  Parameter format:
  ||   InclHistSw    : Include History (Y,N)
  ||   CrpList       : Corp Code List i.e.: 010~500~501
  ||   OrdStat       : Order Status (O:Open,S:Suspend,C:Cancel,U:Unbilled,B:Billed,ALL:All)
  ||   DivIdList     : Division ID list i.e.: ALL or SW~MI~ME
  ||   OrdTyp        : Order Type (R:Reg,T:Test,ALL:All)
  ||   OrdSrcList    : Order Source List: ALL or CSR~ADC~KEY~ADK
  ||   LLRFrom       : LLR Date Starting Range (inclusive) in YYYY-MM-DD format (requires LLR_TO)
  ||   LLRTo         : LLR Date Ending Range (inclusive) in YYYY-MM-DD format (requires LLR_FROM)
  ||   OrdRcvdAftr   : Search for orders received >= time in YYYY-MM-DD HH24:MI format
  ||   ItemList      : Catalog Item List (include leading zeros) delimited by ~
  ||   POPrfxList    : List of the beginning characters for PO Numbers delimited by ~
  ||   GrpCdList     : Cust Group List  i.e.: SW006~SW010
  ||   CustList      : CustId List (include leading zeros) delimited by ~
  ||   DistIdPrfxList: List of the begining characters for Distribution ID delimited by ~
  ||   OrdNumList    : List of Order Numbers delimited by ~
  ||   OrdQty        : OrdQty >= passed value (default NULL for All)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/02/18 | rhalpai | Original for SDHD-387346
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_hdr_list_fn(
    i_incl_hist_sw       IN  VARCHAR2 DEFAULT 'N',
    i_crp_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_stat           IN  VARCHAR2 DEFAULT 'O',
    i_div_list           IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_typ            IN  VARCHAR2 DEFAULT 'ALL',
    i_ord_src_list       IN  VARCHAR2 DEFAULT 'ALL',
    i_llr_from           IN  VARCHAR2 DEFAULT '1900-01-01',
    i_llr_to             IN  VARCHAR2 DEFAULT '2999-12-31',
    i_ord_rcvd_aftr      IN  VARCHAR2 DEFAULT '1900-01-01',
    i_item_list          IN  CLOB DEFAULT NULL,
    i_po_prfx_list       IN  CLOB DEFAULT NULL,
    i_grp_list           IN  CLOB DEFAULT NULL,
    i_cust_list          IN  CLOB DEFAULT NULL,
    i_dist_id_prfx_list  IN  CLOB DEFAULT NULL,
    i_ord_num_list       IN  CLOB DEFAULT NULL,
    i_ord_qty            IN  NUMBER DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ORD_HDR_LIST_FN';
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
    l_t_dist_id_prfxs    type_stab;
    l_t_ord_nums         type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'InclHistSw', i_incl_hist_sw);
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
    logs.add_parm(lar_parm, 'DistIdPrfxList', i_dist_id_prfx_list);
    logs.add_parm(lar_parm, 'OrdNumList', i_ord_num_list);
    logs.add_parm(lar_parm, 'OrdQty', i_ord_qty);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_incl_hist_sw IN('Y', 'N')), 'InclHistSw must be Y or N');
    excp.assert((i_crp_list IS NOT NULL), 'CorpCdList cannot be NULL');
    excp.assert((i_div_list IS NOT NULL), 'DivList cannot be NULL');
    excp.assert((i_ord_typ IS NOT NULL), 'OrderType cannot be NULL');
    excp.assert((i_ord_src_list IS NOT NULL), 'OrderSourceList cannot be NULL');
    excp.assert((i_llr_from IS NOT NULL), 'LLRFrom cannot be NULL');
    excp.assert((i_llr_to IS NOT NULL), 'LLRTo cannot be NULL');
    excp.assert((i_ord_rcvd_aftr IS NOT NULL), 'OrderReceivedAfter cannot be NULL');
    logs.dbg('Initialize');

    IF i_item_list IS NOT NULL THEN
      l_t_items := strsplit_fn(i_item_list, op_const_pk.field_delimiter);
    END IF;   -- i_item_list IS NOT NULL

    IF i_ord_num_list IS NOT NULL THEN
      l_t_ord_nums := strsplit_fn(i_ord_num_list, op_const_pk.field_delimiter);
      logs.dbg('Open OrdNum Cursor');

      OPEN l_cv
       FOR
         SELECT o.corp_cd, o.div_id, o.grp_cd, o.cust_id, o.store_num, o.ord_num, o.ord_stat, o.ord_typ, o.ord_src,
                o.po_num, o.llr_dt, o.load_num, o.stop_num, o.eta_ts, o.ord_rcvd_ts, o.ord_ln_cnt, o.excptn_sw,
                o.dist_id, o.ship_dt, o.load_typ, o.hist_sw
           FROM (SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd,
                        a.custa AS cust_id, cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat,
                        a.dsorda AS ord_typ, a.ipdtsa AS ord_src, a.cpoa AS po_num,
                        TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num, se.stop_num,
                        TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                        TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                        (SELECT COUNT(*)
                           FROM ordp120b b
                          WHERE b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt,
                        a.excptn_sw, DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                        DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                        a.ldtypa AS load_typ, 'N' AS hist_sw
                   FROM ordp100a a, div_mstr_di1d d, mclp020b cx, sysp200c c, load_depart_op1f ld, stop_eta_op1g se
                  WHERE a.ordnoa IN(SELECT TO_NUMBER(o.column_value)
                                      FROM TABLE(l_t_ord_nums) o)
                    AND d.div_part = a.div_part
                    AND cx.div_part = a.div_part
                    AND cx.custb = a.custa
                    AND c.div_part = a.div_part
                    AND c.acnoc = a.custa
                    AND ld.div_part = a.div_part
                    AND ld.load_depart_sid = a.load_depart_sid
                    AND se.div_part(+) = a.div_part
                    AND se.load_depart_sid(+) = a.load_depart_sid
                    AND se.cust_id(+) = a.custa
                    AND (   (    i_item_list IS NULL
                             AND i_ord_qty IS NULL)
                         OR EXISTS(SELECT 1
                                     FROM ordp120b b
                                    WHERE b.div_part = a.div_part
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
                                      AND (   i_item_list IS NULL
                                           OR b.orditb MEMBER OF l_t_items)
                                      AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                        )
                 UNION ALL
                 SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                        cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                        a.ipdtsa AS ord_src, a.cpoa AS po_num,
                        TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt, a.orrtea AS load_num,
                        a.stopsa AS stop_num,
                        TO_CHAR(TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta,
                                'YYYY-MM-DD HH24:MI'
                               ) AS eta_ts,
                        TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                        (SELECT COUNT(*)
                           FROM ordp920b b
                          WHERE b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt,
                        a.excptn_sw, DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                        DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                        a.ldtypa AS load_typ, 'Y' AS hist_sw
                   FROM ordp900a a, div_mstr_di1d d, mclp020b cx, sysp200c c
                  WHERE i_incl_hist_sw = 'Y'
                    AND a.ordnoa IN(SELECT TO_NUMBER(o.column_value)
                                      FROM TABLE(l_t_ord_nums) o)
                    AND d.div_part = a.div_part
                    AND cx.div_part = a.div_part
                    AND cx.custb = a.custa
                    AND c.div_part = a.div_part
                    AND c.acnoc = a.custa
                    AND (   (    i_item_list IS NULL
                             AND i_ord_qty IS NULL)
                         OR EXISTS(SELECT 1
                                     FROM ordp920b b
                                    WHERE b.div_part = a.div_part
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
                                      AND (   i_item_list IS NULL
                                           OR b.orditb MEMBER OF l_t_items)
                                      AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                        )) o;
    ELSE
      l_llr_from := TO_DATE(i_llr_from, g_c_dt_fmt);
      l_llr_to := TO_DATE(i_llr_to, g_c_dt_fmt);
      l_ord_rcvd_ts := TO_DATE(i_ord_rcvd_aftr, g_c_dt_tm_fmt);

      IF i_crp_list <> 'ALL' THEN
        l_t_crps := strsplit_fn(i_crp_list, op_const_pk.field_delimiter);
      END IF;   -- i_crp_list <> 'ALL'

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

      IF i_dist_id_prfx_list IS NOT NULL THEN
        l_t_dist_id_prfxs := strsplit_fn(i_dist_id_prfx_list, op_const_pk.field_delimiter);
      END IF;   -- i_dist_id_prfx_list IS NOT NULL

      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT o.corp_cd, o.div_id, o.grp_cd, o.cust_id, o.store_num, o.ord_num, o.ord_stat, o.ord_typ, o.ord_src,
                o.po_num, o.llr_dt, o.load_num, o.stop_num, o.eta_ts, o.ord_rcvd_ts, o.ord_ln_cnt, o.excptn_sw,
                o.dist_id, o.ship_dt, o.load_typ, o.hist_sw
           FROM (SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                        cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                        a.ipdtsa AS ord_src, a.cpoa AS po_num, TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt, ld.load_num,
                        se.stop_num, TO_CHAR(se.eta_ts, 'YYYY-MM-DD HH24:MI') AS eta_ts,
                        TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                        (SELECT COUNT(*)
                           FROM ordp120b b
                          WHERE b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt,
                        a.excptn_sw, DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                        DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                        a.ldtypa AS load_typ, 'N' AS hist_sw
                   FROM div_mstr_di1d d, mclp020b cx, sysp200c c, ordp100a a, load_depart_op1f ld, stop_eta_op1g se
                  WHERE d.div_part > 0
                    AND (   i_div_list = 'ALL'
                         OR d.div_id MEMBER OF l_t_divs)
                    AND cx.div_part = d.div_part
                    AND (   i_crp_list = 'ALL'
                         OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                          FROM TABLE(l_t_crps) cc))
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
                    AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
                    AND (   i_ord_src_list = 'ALL'
                         OR a.ipdtsa MEMBER OF l_t_ord_srcs)
                    AND a.ord_rcvd_ts >= l_ord_rcvd_ts
                    AND (   i_po_prfx_list IS NULL
                         OR EXISTS(SELECT 1
                                     FROM TABLE(l_t_po_prfxs) t
                                    WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value)
                        )
                    AND (   i_dist_id_prfx_list IS NULL
                         OR (    a.dsorda = 'D'
                             AND EXISTS(SELECT 1
                                          FROM TABLE(l_t_dist_id_prfxs) t
                                         WHERE SUBSTR(a.legrfa, 1, LENGTH(t.column_value)) = t.column_value)
                            )
                        )
                    AND ld.div_part = a.div_part
                    AND ld.load_depart_sid = a.load_depart_sid
                    AND ld.llr_dt BETWEEN l_llr_from AND l_llr_to
                    AND se.div_part(+) = a.div_part
                    AND se.load_depart_sid(+) = a.load_depart_sid
                    AND se.cust_id(+) = a.custa
                    AND (   (    i_item_list IS NULL
                             AND i_ord_qty IS NULL)
                         OR EXISTS(SELECT 1
                                     FROM ordp120b b
                                    WHERE b.div_part = a.div_part
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
                                      AND (   i_item_list IS NULL
                                           OR b.orditb MEMBER OF l_t_items)
                                      AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                        )
                 UNION ALL
                 SELECT LPAD(cx.corpb, 3, '0') AS corp_cd, d.div_part, d.div_id, c.retgpc AS grp_cd, a.custa AS cust_id,
                        cx.storeb AS store_num, a.ordnoa AS ord_num, a.stata AS ord_stat, a.dsorda AS ord_typ,
                        a.ipdtsa AS ord_src, a.cpoa AS po_num,
                        TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt, a.orrtea AS load_num,
                        a.stopsa AS stop_num,
                        TO_CHAR(TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta,
                                'YYYY-MM-DD HH24:MI'
                               ) AS eta_ts,
                        TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS') AS ord_rcvd_ts,
                        (SELECT COUNT(*)
                           FROM ordp920b b
                          WHERE b.div_part = a.div_part
                            AND b.ordnob = a.ordnoa
                            AND b.lineb = FLOOR(b.lineb)) AS ord_ln_cnt,
                        a.excptn_sw, DECODE(a.dsorda, 'D', SUBSTR(a.legrfa, 1, 13)) AS dist_id,
                        DECODE(a.dsorda, 'D', TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD')) AS ship_dt,
                        a.ldtypa AS load_typ, 'Y' AS hist_sw
                   FROM ordp900a a, div_mstr_di1d d, mclp020b cx, sysp200c c
                  WHERE i_incl_hist_sw = 'Y'
                    AND d.div_part > 0
                    AND (   i_div_list = 'ALL'
                         OR d.div_id MEMBER OF l_t_divs)
                    AND cx.div_part = d.div_part
                    AND (   i_crp_list = 'ALL'
                         OR cx.corpb IN(SELECT TO_NUMBER(cc.column_value)
                                          FROM TABLE(l_t_crps) cc))
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
                    AND a.dsorda = DECODE(i_ord_typ, 'ALL', a.dsorda, i_ord_typ)
                    AND (   i_ord_src_list = 'ALL'
                         OR a.ipdtsa MEMBER OF l_t_ord_srcs)
                    AND a.ord_rcvd_ts >= l_ord_rcvd_ts
                    AND (   i_po_prfx_list IS NULL
                         OR EXISTS(SELECT 1
                                     FROM TABLE(l_t_po_prfxs) t
                                    WHERE SUBSTR(a.cpoa, 1, LENGTH(t.column_value)) = t.column_value)
                        )
                    AND (   i_dist_id_prfx_list IS NULL
                         OR (    a.dsorda = 'D'
                             AND EXISTS(SELECT 1
                                          FROM TABLE(l_t_dist_id_prfxs) t
                                         WHERE SUBSTR(a.legrfa, 1, LENGTH(t.column_value)) = t.column_value)
                            )
                        )
                    AND a.ctofda >= l_llr_from - DATE '1900-02-28'
                    AND a.ctofda <= l_llr_to - DATE '1900-02-28'
                    AND (   (    i_item_list IS NULL
                             AND i_ord_qty IS NULL)
                         OR EXISTS(SELECT 1
                                     FROM ordp920b b
                                    WHERE b.div_part = a.div_part
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
                                      AND (   i_item_list IS NULL
                                           OR b.orditb MEMBER OF l_t_items)
                                      AND b.ordqtb >= DECODE(i_ord_qty, NULL, b.ordqtb, i_ord_qty))
                        )) o;
    END IF;   -- i_ord_num_list IS NOT NULL

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
  || ORD_DTL_LIST_FN
  ||  Returns cursor of Order Detail Info.
  ||
  ||  Parameter format:
  ||   ParmList      : Div~OrdNum~HistSw`Div~OrdNum~HistSw
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/02/18 | rhalpai | Original for SDHD-387346
  ||----------------------------------------------------------------------------
  */
  FUNCTION ord_dtl_list_fn(
    i_parm_list  IN  CLOB
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_MASS_INQUIRY_PK.ORD_DTL_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'ParmList', i_parm_list);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_parm_list IS NOT NULL), 'ParmList cannot be NULL');
    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln, b.statb AS ln_stat, e.catite AS catlg_num,
                e.ctdsce AS item_descr, e.shppke AS pack, e.sizee AS sz, b.orgqtb AS org_qty, b.ordqtb AS ord_qty,
                b.pckqtb AS pck_qty, e.upce AS rtl_upc, b.hdprcb AS prc_amt, b.hdrtab AS rtl_amt,
                b.manctb AS mfst_categ, b.totctb AS tote_categ, LPAD(b.labctb, 3, '0') AS labl_categ,
                LPAD(b.invctb, 3, '0') AS invc_categ,
                TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                        'YYYY-MM-DD HH24:MI:SS'
                       ) AS prc_ts,
                b.ntshpb AS nt_shp_rsn, log_rsn_udf(b.div_part, b.ordnob, b.lineb) AS log_rsn, b.excptn_sw
           FROM (SELECT d.div_part, TO_NUMBER(t.column2) AS ord_num
                   FROM TABLE(lob2table.separatedcolumns(i_parm_list,
                                                         op_const_pk.grp_delimiter,
                                                         op_const_pk.field_delimiter
                                                        )
                             ) t,
                        div_mstr_di1d d
                  WHERE t.column3 = 'N'
                    AND d.div_id = t.column1) o,
                ordp120b b, sawp505e e
          WHERE b.div_part = o.div_part
            AND b.ordnob = o.ord_num
            AND e.catite = b.orditb
       UNION ALL
       SELECT   b.ordnob AS ord_num, b.lineb AS ord_ln, b.statb AS ln_stat, e.catite AS catlg_num,
                e.ctdsce AS item_descr, e.shppke AS pack, e.sizee AS sz, b.orgqtb AS org_qty, b.ordqtb AS ord_qty,
                b.pckqtb AS pck_qty, e.upce AS rtl_upc, b.hdprcb AS prc_amt, b.hdrtab AS rtl_amt,
                b.manctb AS mfst_categ, b.totctb AS tote_categ, LPAD(b.labctb, 3, '0') AS labl_categ,
                LPAD(b.invctb, 3, '0') AS invc_categ,
                TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb,
                        'YYYY-MM-DD HH24:MI:SS'
                       ) AS prc_ts,
                b.ntshpb AS nt_shp_rsn, log_rsn_udf(b.div_part, b.ordnob, b.lineb) AS log_rsn, b.excptn_sw
           FROM (SELECT d.div_part, TO_NUMBER(t.column2) AS ord_num
                   FROM TABLE(lob2table.separatedcolumns(i_parm_list,
                                                         op_const_pk.grp_delimiter,
                                                         op_const_pk.field_delimiter
                                                        )
                             ) t,
                        div_mstr_di1d d
                  WHERE t.column3 = 'Y'
                    AND d.div_id = t.column1) o,
                ordp920b b, sawp505e e
          WHERE b.div_part = o.div_part
            AND b.ordnob = o.ord_num
            AND e.catite = b.orditb
       ORDER BY ord_num, ord_ln;

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
END op_mass_inquiry_pk;
/

