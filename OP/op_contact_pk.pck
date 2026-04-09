CREATE OR REPLACE PACKAGE op_contact_pk IS
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
  FUNCTION process_list_fn(
    i_div       IN  VARCHAR2,
    i_prcs_typ  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR;

  FUNCTION contact_list_fn(
    i_div     IN  VARCHAR2,
    i_grp_id  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE ins_prcs_typ_descr_sp(
    i_div        IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    i_prcs_typ   IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE upd_prcs_typ_descr_sp(
    i_div        IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    i_prcs_typ   IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE del_prcs_typ_descr_sp(
    i_div                  IN      VARCHAR2,
    i_prcs_id              IN      VARCHAR2,
    o_msg                  OUT     VARCHAR2,
    i_byp_prcs_grp_chk_sw  IN      VARCHAR2 DEFAULT 'N',
    i_commit_sw            IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE ins_prcs_grp_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE del_prcs_grp_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE ins_grp_info_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE upd_grp_info_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE del_grp_info_sp(
    i_div                   IN      VARCHAR2,
    i_grp_id                IN      VARCHAR2,
    o_msg                   OUT     VARCHAR2,
    i_byp_grp_cntct_chk_sw  IN      VARCHAR2 DEFAULT 'N',
    i_commit_sw             IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE ins_grp_cntct_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_cntct_id   IN      NUMBER,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE del_grp_cntct_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_cntct_id   IN      NUMBER,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE save_cntct_info_sp(
    i_div         IN      VARCHAR2,
    i_email_addr  IN      VARCHAR2,
    i_descr       IN      VARCHAR2,
    i_frst_nm     IN      VARCHAR2,
    i_last_nm     IN      VARCHAR2,
    o_msg         OUT     VARCHAR2,
    i_cntct_id    IN      NUMBER DEFAULT NULL
  );

  PROCEDURE del_cntct_info_sp(
    i_div                   IN      VARCHAR2,
    i_cntct_id              IN      NUMBER,
    i_byp_grp_cntct_chk_sw  IN      VARCHAR2 DEFAULT 'N',
    o_msg                   OUT     VARCHAR2,
    i_commit_sw             IN      VARCHAR2 DEFAULT 'N'
  );
END op_contact_pk;
/

CREATE OR REPLACE PACKAGE BODY op_contact_pk IS
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
  || PROCESS_LIST_FN
  ||  Returns cursor of Process Type Descriptions.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  FUNCTION process_list_fn(
    i_div       IN  VARCHAR2,
    i_prcs_typ  IN  VARCHAR2
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.PROCESS_LIST_FN';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsTyp', i_prcs_typ);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   p.prcs_id, p.prcs_typ, p.descr, TO_CHAR(p.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS') AS last_chg_ts
           FROM prcs_typ_descr p
          WHERE p.div_part = l_div_part
            AND p.prcs_typ = NVL(i_prcs_typ, p.prcs_typ)
       ORDER BY p.prcs_id;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END process_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || CONTACT_LIST_FN
  ||  Returns cursor of contacts.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  FUNCTION contact_list_fn(
    i_div     IN  VARCHAR2,
    i_grp_id  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN SYS_REFCURSOR IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.CONTACT_LIST_FN';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    OPEN l_cv
     FOR
       SELECT   c.cntct_id, c.descr, c.frst_nm, c.last_nm, c.email_addr, DECODE(gc.grp_id, NULL, 'N', 'Y') AS sel
           FROM cntct_info c, grp_cntct gc
          WHERE c.div_part = l_div_part
            AND gc.div_part(+) = c.div_part
            AND gc.grp_id(+) = i_grp_id
            AND gc.cntct_id(+) = c.cntct_id
       ORDER BY sel DESC, c.frst_nm, c.last_nm, c.email_addr;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
    RETURN(l_cv);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END contact_list_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_PRCS_TYP_DESCR_SP
  ||  Add Process Type Description.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_prcs_typ_descr_sp(
    i_div        IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    i_prcs_typ   IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                  := 'OP_CONTACT_PK.INS_PRCS_TYP_DESCR_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_prcs_id             prcs_typ_descr.prcs_id%TYPE;
    l_prcs_typ            prcs_typ_descr.prcs_typ%TYPE;
    l_descr               prcs_typ_descr.descr%TYPE;
    l_e_invalid_prcs_id   EXCEPTION;
    l_e_invalid_prcs_typ  EXCEPTION;
    l_e_invalid_descr     EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'PrcsTyp', i_prcs_typ);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_prcs_id := RTRIM(i_prcs_id);
    l_prcs_typ := RTRIM(i_prcs_typ);
    l_descr := RTRIM(i_descr);

    IF l_prcs_id IS NULL THEN
      RAISE l_e_invalid_prcs_id;
    END IF;   -- l_prcs_id IS NULL

    IF l_prcs_typ IS NULL THEN
      RAISE l_e_invalid_prcs_typ;
    END IF;   -- l_prcs_typ IS NULL

    IF l_descr IS NULL THEN
      RAISE l_e_invalid_descr;
    END IF;   -- l_descr IS NULL

    logs.dbg('Add Process Type Description');

    INSERT INTO prcs_typ_descr
                (div_part, prcs_id, prcs_typ, descr, last_chg_ts
                )
         VALUES (l_div_part, l_prcs_id, l_prcs_typ, l_descr, SYSDATE
                );

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_prcs_id THEN
      o_msg := 'Process ID required!  No updates applied.';
    WHEN l_e_invalid_prcs_typ THEN
      o_msg := 'Process Type required!  No updates applied.';
    WHEN l_e_invalid_descr THEN
      o_msg := 'Description required!  No updates applied.';
    WHEN DUP_VAL_ON_INDEX THEN
      o_msg := 'Duplicate Process Type Description found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END ins_prcs_typ_descr_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_PRCS_TYP_DESCR_SP
  ||  Change Process Type Description.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_prcs_typ_descr_sp(
    i_div        IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    i_prcs_typ   IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                  := 'OP_CONTACT_PK.UPD_PRCS_TYP_DESCR_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_prcs_id              prcs_typ_descr.prcs_id%TYPE;
    l_prcs_typ             prcs_typ_descr.prcs_typ%TYPE;
    l_descr                prcs_typ_descr.descr%TYPE;
    l_e_invalid_prcs_typ   EXCEPTION;
    l_e_invalid_descr      EXCEPTION;
    l_e_prcs_id_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'PrcsTyp', i_prcs_typ);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_prcs_typ := RTRIM(i_prcs_typ);
    l_descr := RTRIM(i_descr);

    IF l_prcs_typ IS NULL THEN
      RAISE l_e_invalid_prcs_typ;
    END IF;   -- l_prcs_typ IS NULL

    IF l_descr IS NULL THEN
      RAISE l_e_invalid_descr;
    END IF;   -- l_descr IS NULL

    logs.dbg('Change Process Type Description');

    UPDATE prcs_typ_descr p
       SET p.prcs_typ = l_prcs_typ,
           p.descr = l_descr,
           p.last_chg_ts = SYSDATE
     WHERE p.div_part = l_div_part
       AND p.prcs_id = i_prcs_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_prcs_id_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_prcs_typ THEN
      o_msg := 'Process Type required!  No updates applied.';
    WHEN l_e_invalid_descr THEN
      o_msg := 'Description required!  No updates applied.';
    WHEN l_e_prcs_id_not_found THEN
      o_msg := 'Process ID, "' || l_prcs_id || '" not found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END upd_prcs_typ_descr_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_PRCS_TYP_DESCR_SP
  ||  Remove Process Type Description.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_prcs_typ_descr_sp(
    i_div                  IN      VARCHAR2,
    i_prcs_id              IN      VARCHAR2,
    o_msg                  OUT     VARCHAR2,
    i_byp_prcs_grp_chk_sw  IN      VARCHAR2 DEFAULT 'N',
    i_commit_sw            IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.DEL_PRCS_TYP_DESCR_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_cv                   SYS_REFCURSOR;
    l_prcs_grp_found_sw    VARCHAR2(1)   := 'N';
    l_e_prcs_grp_exists    EXCEPTION;
    l_e_prcs_id_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'BypPrcsGrpChkSw', i_byp_prcs_grp_chk_sw);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF NVL(i_byp_prcs_grp_chk_sw, 'N') = 'N' THEN
      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM prcs_grp pg
          WHERE pg.div_part = l_div_part
            AND pg.prcs_id = i_prcs_id;

      logs.dbg('Fetch Cursor');

      FETCH l_cv
       INTO l_prcs_grp_found_sw;

      IF l_prcs_grp_found_sw = 'Y' THEN
        RAISE l_e_prcs_grp_exists;
      END IF;   -- l_prcs_grp_found_sw = 'Y'
    END IF;   -- NVL(i_byp_prcs_grp_chk_sw, 'N') = 'N'

    logs.dbg('Remove Process Type Description');

    DELETE FROM prcs_typ_descr p
          WHERE p.div_part = l_div_part
            AND p.prcs_id = i_prcs_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_prcs_id_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_prcs_grp_exists THEN
      o_msg := 'Process Group using Process ID, "' || i_prcs_id || '" exists!  No updates applied.';
    WHEN l_e_prcs_id_not_found THEN
      o_msg := 'Process ID, "' || i_prcs_id || '" not found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END del_prcs_typ_descr_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_PRCS_GRP_SP
  ||  Add Process Group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_prcs_grp_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm           := 'OP_CONTACT_PK.INS_PRCS_GRP_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_grp_id             prcs_grp.grp_id%TYPE;
    l_prcs_id            prcs_grp.prcs_id%TYPE;
    l_e_invalid_grp_id   EXCEPTION;
    l_e_invalid_prcs_id  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_grp_id := RTRIM(i_grp_id);
    l_prcs_id := RTRIM(i_prcs_id);

    IF l_grp_id IS NULL THEN
      RAISE l_e_invalid_grp_id;
    END IF;   -- l_grp_id IS NULL

    IF l_prcs_id IS NULL THEN
      RAISE l_e_invalid_prcs_id;
    END IF;   -- l_prcs_id IS NULL

    logs.dbg('Add Process Group');

    INSERT INTO prcs_grp
                (div_part, grp_id, prcs_id
                )
         VALUES (l_div_part, l_grp_id, l_prcs_id
                );

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_grp_id THEN
      o_msg := 'Process Group required!  No updates applied.';
    WHEN l_e_invalid_prcs_id THEN
      o_msg := 'Process ID required!  No updates applied.';
    WHEN DUP_VAL_ON_INDEX THEN
      o_msg := 'Duplicate Process Group found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END ins_prcs_grp_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_PRCS_GRP_SP
  ||  Remove Process Group.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_prcs_grp_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_prcs_id    IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.DEL_PRCS_GRP_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_e_prcs_grp_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'PrcsId', i_prcs_id);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    DELETE FROM prcs_grp pg
          WHERE pg.div_part = l_div_part
            AND pg.grp_id = i_grp_id
            AND pg.prcs_id = i_prcs_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_prcs_grp_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_prcs_grp_not_found THEN
      o_msg := 'Process Group not found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END del_prcs_grp_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_GRP_INFO_SP
  ||  Add Group Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_grp_info_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_CONTACT_PK.INS_GRP_INFO_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_grp_id             grp_info.grp_id%TYPE;
    l_descr              grp_info.descr%TYPE;
    l_e_invalid_grp_id   EXCEPTION;
    l_e_invalid_descr    EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_grp_id := RTRIM(i_grp_id);
    l_descr := RTRIM(i_descr);

    IF l_grp_id IS NULL THEN
      RAISE l_e_invalid_grp_id;
    END IF;   -- l_grp_id IS NULL

    IF l_descr IS NULL THEN
      RAISE l_e_invalid_descr;
    END IF;   -- l_descr IS NULL

    logs.dbg('Add Group Info');

    INSERT INTO grp_info
                (div_part, grp_id, descr
                )
         VALUES (l_div_part, l_grp_id, l_descr
                );

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_grp_id THEN
      o_msg := 'Process Group required!  No updates applied.';
    WHEN l_e_invalid_descr THEN
      o_msg := 'Description required!  No updates applied.';
    WHEN DUP_VAL_ON_INDEX THEN
      o_msg := 'Duplicate Group Info found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END ins_grp_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_GRP_INFO_SP
  ||  Change Group Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_grp_info_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_descr      IN      VARCHAR2,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_CONTACT_PK.UPD_GRP_INFO_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_grp_id             grp_info.grp_id%TYPE;
    l_descr              grp_info.descr%TYPE;
    l_e_invalid_grp_id   EXCEPTION;
    l_e_invalid_descr    EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_grp_id := RTRIM(i_grp_id);
    l_descr := RTRIM(i_descr);

    IF l_grp_id IS NULL THEN
      RAISE l_e_invalid_grp_id;
    END IF;   -- l_grp_id IS NULL

    IF l_descr IS NULL THEN
      RAISE l_e_invalid_descr;
    END IF;   -- l_descr IS NULL

    logs.dbg('Change Group Info');

    UPDATE grp_info g
       SET g.descr = l_descr
     WHERE g.div_part = l_div_part
       AND g.grp_id = l_grp_id;

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_grp_id THEN
      o_msg := 'Process Group required!  No updates applied.';
    WHEN l_e_invalid_descr THEN
      o_msg := 'Description required!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END upd_grp_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_GRP_INFO_SP
  ||  Remove Group Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_grp_info_sp(
    i_div                   IN      VARCHAR2,
    i_grp_id                IN      VARCHAR2,
    o_msg                   OUT     VARCHAR2,
    i_byp_grp_cntct_chk_sw  IN      VARCHAR2 DEFAULT 'N',
    i_commit_sw             IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.DEL_GRP_INFO_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_cv                  SYS_REFCURSOR;
    l_grp_cntct_found_sw  VARCHAR2(1)   := 'N';
    l_e_grp_cntct_exists  EXCEPTION;
    l_e_grp_id_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'BypGrpCntctChkSw', i_byp_grp_cntct_chk_sw);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF NVL(i_byp_grp_cntct_chk_sw, 'N') = 'N' THEN
      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM grp_cntct gc
          WHERE gc.div_part = l_div_part
            AND gc.grp_id = i_grp_id;

      logs.dbg('Fetch Cursor');

      FETCH l_cv
       INTO l_grp_cntct_found_sw;

      IF l_grp_cntct_found_sw = 'Y' THEN
        RAISE l_e_grp_cntct_exists;
      END IF;   -- l_grp_cntct_found_sw = 'Y'
    END IF;   -- NVL(i_byp_grp_cntct_chk_sw, 'N') = 'N'

    logs.dbg('Remove Group Info');

    DELETE FROM grp_info g
          WHERE g.div_part = l_div_part
            AND g.grp_id = i_grp_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_grp_id_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_grp_cntct_exists THEN
      o_msg := 'Group Contact using Group ID, "' || i_grp_id || '" exists!  No updates applied.';
    WHEN l_e_grp_id_not_found THEN
      o_msg := 'Group ID, "' || i_grp_id || '" not found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END del_grp_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_GRP_CNTCT_SP
  ||  Add Group Contact.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_grp_cntct_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_cntct_id   IN      NUMBER,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm          := 'OP_CONTACT_PK.INS_GRP_CNTCT_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_grp_id              grp_info.grp_id%TYPE;
    l_e_invalid_grp_id    EXCEPTION;
    l_e_invalid_cntct_id  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'CntctId', i_cntct_id);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_grp_id := RTRIM(i_grp_id);

    IF l_grp_id IS NULL THEN
      RAISE l_e_invalid_grp_id;
    END IF;   -- l_grp_id IS NULL

    IF i_cntct_id IS NULL THEN
      RAISE l_e_invalid_cntct_id;
    END IF;   -- i_cntct_id IS NULL

    logs.dbg('Add Group Contact');

    INSERT INTO grp_cntct
                (div_part, grp_id, cntct_id
                )
         VALUES (l_div_part, l_grp_id, i_cntct_id
                );

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_invalid_grp_id THEN
      o_msg := 'Process Group required!  No updates applied.';
    WHEN l_e_invalid_cntct_id THEN
      o_msg := 'Contact ID required!  No updates applied.';
    WHEN DUP_VAL_ON_INDEX THEN
      o_msg := 'Duplicate Group Contact found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END ins_grp_cntct_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_GRP_CNTCT_SP
  ||  Remove Group Contact.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_grp_cntct_sp(
    i_div        IN      VARCHAR2,
    i_grp_id     IN      VARCHAR2,
    i_cntct_id   IN      NUMBER,
    o_msg        OUT     VARCHAR2,
    i_commit_sw  IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module      CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.DEL_GRP_CNTCT_SP';
    lar_parm                 logs.tar_parm;
    l_div_part               NUMBER;
    l_e_grp_cntct_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'GrpId', i_grp_id);
    logs.add_parm(lar_parm, 'CntctId', i_cntct_id);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    DELETE FROM grp_cntct gc
          WHERE gc.div_part = l_div_part
            AND gc.grp_id = i_grp_id
            AND gc.cntct_id = i_cntct_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_grp_cntct_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_grp_cntct_not_found THEN
      o_msg := 'Group Contact not found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END del_grp_cntct_sp;

  /*
  ||----------------------------------------------------------------------------
  || SAVE_CNTCT_INFO_SP
  ||  Add/Change contact info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  || 03/17/08 | RHALPAI | Changed to exclude existing contact ID from duplicate
  ||                    | email address validation.  PIR3593
  ||----------------------------------------------------------------------------
  */
  PROCEDURE save_cntct_info_sp(
    i_div         IN      VARCHAR2,
    i_email_addr  IN      VARCHAR2,
    i_descr       IN      VARCHAR2,
    i_frst_nm     IN      VARCHAR2,
    i_last_nm     IN      VARCHAR2,
    o_msg         OUT     VARCHAR2,
    i_cntct_id    IN      NUMBER DEFAULT NULL
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm                := 'OP_CONTACT_PK.SAVE_CNTCT_INFO_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_email_addr            cntct_info.email_addr%TYPE;
    l_descr                 cntct_info.descr%TYPE;
    l_frst_nm               cntct_info.frst_nm%TYPE;
    l_last_nm               cntct_info.last_nm%TYPE;
    l_cv                    SYS_REFCURSOR;
    l_cntct_id              NUMBER;
    l_dup_email_addr_sw     VARCHAR2(1)                  := 'N';
    l_e_email_addr_invalid  EXCEPTION;
    l_e_descr_invalid       EXCEPTION;
    l_e_frst_nm_invalid     EXCEPTION;
    l_e_last_nm_invalid     EXCEPTION;
    l_e_dup_email_addr      EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'EmailAddr', i_email_addr);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'FrstNm', i_frst_nm);
    logs.add_parm(lar_parm, 'LastNm', i_last_nm);
    logs.add_parm(lar_parm, 'CntctId', i_cntct_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_email_addr := TRIM(i_email_addr);
    l_descr := TRIM(i_descr);
    l_frst_nm := TRIM(i_frst_nm);
    l_last_nm := TRIM(i_last_nm);

    IF l_email_addr IS NULL THEN
      RAISE l_e_email_addr_invalid;
    END IF;   -- l_email_addr IS NULL

    IF l_descr IS NULL THEN
      RAISE l_e_descr_invalid;
    END IF;   -- l_descr IS NULL

    IF l_frst_nm IS NULL THEN
      RAISE l_e_frst_nm_invalid;
    END IF;   -- l_frst_nm IS NULL

    IF l_last_nm IS NULL THEN
      RAISE l_e_last_nm_invalid;
    END IF;   -- l_last_nm IS NULL

    logs.dbg('Open Cursor');

    OPEN l_cv
     FOR
       SELECT (SELECT NVL(MAX(c.cntct_id), 0) + 1
                 FROM cntct_info c) AS cntct_id,
              (CASE
                 WHEN EXISTS(SELECT 1
                               FROM cntct_info c
                              WHERE c.div_part = l_div_part
                                AND c.email_addr = l_email_addr
                                AND c.cntct_id <> NVL(i_cntct_id, -1)) THEN 'Y'
                 ELSE 'N'
               END
              ) AS dup_email_addr
         FROM DUAL;

    logs.dbg('Fetch Cursor');

    FETCH l_cv
     INTO l_cntct_id, l_dup_email_addr_sw;

    IF l_dup_email_addr_sw = 'Y' THEN
      RAISE l_e_dup_email_addr;
    END IF;   -- l_dup_email_addr_sw = 'Y'

    logs.dbg('Save Contact Info');
    MERGE INTO cntct_info c
         USING (SELECT d.div_part
                  FROM div_mstr_di1d d
                 WHERE d.div_id = i_div) x
            ON (    c.cntct_id = i_cntct_id
                AND c.div_part = x.div_part)
      WHEN MATCHED THEN
        UPDATE
           SET email_addr = l_email_addr, descr = l_descr, frst_nm = l_frst_nm, last_nm = l_last_nm
      WHEN NOT MATCHED THEN
        INSERT(div_part, cntct_id, email_addr, descr, frst_nm, last_nm)
        VALUES(x.div_part, l_cntct_id, l_email_addr, l_descr, l_frst_nm, l_last_nm);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_email_addr_invalid THEN
      o_msg := 'Email address required!  No updates applied.';
    WHEN l_e_descr_invalid THEN
      o_msg := 'Description required!  No updates applied.';
    WHEN l_e_frst_nm_invalid THEN
      o_msg := 'First name required!  No updates applied.';
    WHEN l_e_last_nm_invalid THEN
      o_msg := 'Last name required!  No updates applied.';
    WHEN l_e_dup_email_addr THEN
      o_msg := 'Duplicate email address found!  No updates applied.';
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END save_cntct_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_CNTCT_INFO_SP
  ||  Remove Contact Info.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/21/08 | RHALPAI | Original - Created for PIR4512
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_cntct_info_sp(
    i_div                   IN      VARCHAR2,
    i_cntct_id              IN      NUMBER,
    i_byp_grp_cntct_chk_sw  IN      VARCHAR2 DEFAULT 'N',
    o_msg                   OUT     VARCHAR2,
    i_commit_sw             IN      VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm := 'OP_CONTACT_PK.DEL_CNTCT_INFO_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_cv                    SYS_REFCURSOR;
    l_grp_cntct_found_sw    VARCHAR2(1)   := 'N';
    l_e_grp_cntct_exists    EXCEPTION;
    l_e_cntct_id_not_found  EXCEPTION;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'CntctId', i_cntct_id);
    logs.add_parm(lar_parm, 'BypGrpCntctChkSw', i_byp_grp_cntct_chk_sw);
    logs.add_parm(lar_parm, 'CommitSw', i_commit_sw);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF NVL(i_byp_grp_cntct_chk_sw, 'N') = 'N' THEN
      logs.dbg('Open Cursor');

      OPEN l_cv
       FOR
         SELECT 'Y'
           FROM grp_cntct gc
          WHERE gc.div_part = l_div_part
            AND gc.cntct_id = i_cntct_id;

      logs.dbg('Fetch Cursor');

      FETCH l_cv
       INTO l_grp_cntct_found_sw;

      IF l_grp_cntct_found_sw = 'Y' THEN
        RAISE l_e_grp_cntct_exists;
      END IF;   -- l_grp_cntct_found_sw = 'Y'
    END IF;   -- NVL(i_byp_grp_cntct_chk_sw, 'N') = 'N'

    logs.dbg('Remove Contact Info');

    DELETE FROM cntct_info c
          WHERE c.div_part = l_div_part
            AND c.cntct_id = i_cntct_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE l_e_cntct_id_not_found;
    END IF;   -- SQL%ROWCOUNT = 0

    IF i_commit_sw = 'Y' THEN
      COMMIT;
    END IF;   -- i_commit_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN l_e_grp_cntct_exists THEN
      o_msg := 'Group Contact using Contact exists!  No updates applied.';
    WHEN l_e_cntct_id_not_found THEN
      o_msg := 'Contact not found!  No updates applied.';
    WHEN OTHERS THEN
      IF i_commit_sw = 'Y' THEN
        ROLLBACK;
      END IF;   -- i_commit_sw = 'Y'

      logs.err(lar_parm);
  END del_cntct_info_sp;
END op_contact_pk;
/

