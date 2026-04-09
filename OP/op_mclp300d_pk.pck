CREATE OR REPLACE PACKAGE op_mclp300d_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  TYPE g_rt_upd IS RECORD(
    div_part     NUMBER,
    ord_num      VARCHAR2(11),
    ord_ln       VARCHAR2(8),
    rsn_cd       mclp300d.reasnd%TYPE,
    descr        mclp300d.descd%TYPE,
    excpt_lvl    VARCHAR2(2),
    item_num     mclp300d.itemd%TYPE,
    uom          mclp300d.uomd%TYPE,
    rpl_item     mclp300d.repitd%TYPE,
    rpl_uom      mclp300d.repumd%TYPE,
    rpl_or_sub   mclp300d.repsbd%TYPE,
    frm_qty      VARCHAR2(14),
    to_qty       VARCHAR2(14),
    rslvd_cd     mclp300d.resexd%TYPE,
    excpt_descr  mclp300d.exdesd%TYPE,
    rslvd_user   mclp300d.resusd%TYPE,
    rslvd_dt     VARCHAR2(6),
    rslvd_tm     VARCHAR2(6)
  );

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION ins_fn(
    i_r_mclp300d  IN  mclp300d%ROWTYPE
  )
    RETURN NUMBER;

  FUNCTION ins_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_ord_ln       IN  NUMBER,
    i_rsn_cd       IN  mclp300d.reasnd%TYPE,
    i_descr        IN  mclp300d.descd%TYPE,
    i_excpt_descr  IN  mclp300d.exdesd%TYPE,
    i_excpt_lvl    IN  NUMBER,
    i_item_num     IN  mclp300d.itemd%TYPE,
    i_uom          IN  mclp300d.uomd%TYPE,
    i_rpl_item     IN  mclp300d.repitd%TYPE,
    i_rpl_uom      IN  mclp300d.repumd%TYPE,
    i_rpl_or_sub   IN  mclp300d.repsbd%TYPE,
    i_frm_qty      IN  NUMBER,
    i_to_qty       IN  NUMBER,
    i_rslvd_cd     IN  mclp300d.resexd%TYPE,
    i_rslvd_user   IN  mclp300d.resusd%TYPE,
    i_rslvd_dt     IN  NUMBER,
    i_rslvd_tm     IN  NUMBER
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_div_part             IN  VARCHAR2,
    i_ord_num              IN  VARCHAR2,
    i_ord_ln               IN  VARCHAR2,
    i_rsn_cd               IN  VARCHAR2,
    i_descr                IN  VARCHAR2,
    i_excpt_lvl            IN  VARCHAR2,
    i_item_num             IN  VARCHAR2,
    i_uom                  IN  VARCHAR2,
    i_rpl_item             IN  VARCHAR2,
    i_rpl_uom              IN  VARCHAR2,
    i_rpl_or_sub           IN  VARCHAR2,
    i_frm_qty              IN  VARCHAR2,
    i_to_qty               IN  VARCHAR2,
    i_rslvd_cd             IN  VARCHAR2,
    i_excpt_descr          IN  VARCHAR2,
    i_rslvd_user           IN  VARCHAR2,
    i_rslvd_dt             IN  VARCHAR2,
    i_rslvd_tm             IN  VARCHAR2,
    i_updby_div_part       IN  VARCHAR2,
    i_updby_ord_num        IN  VARCHAR2,
    i_updby_ord_ln         IN  VARCHAR2,
    i_updby_rslvd_cd       IN  VARCHAR2,
    i_updby_excpt_lvl      IN  VARCHAR2,
    i_updby_excpt_descr    IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER;

  FUNCTION del_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN NUMBER;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE init_sp(
    o_r_mclp300d  OUT  mclp300d%ROWTYPE
  );

  PROCEDURE ins_sp(
    i_r_mclp300d  IN  mclp300d%ROWTYPE
  );

  PROCEDURE ins_sp(
    i_div_part         IN  NUMBER,
    i_ord_num          IN  NUMBER,
    i_ord_ln           IN  NUMBER,
    i_rsn_cd           IN  VARCHAR2,
    i_item_num         IN  VARCHAR2,
    i_uom              IN  VARCHAR2,
    i_from_qty         IN  NUMBER,
    i_to_qty           IN  NUMBER,
    i_resolved_excptn  IN  VARCHAR2 DEFAULT '0',
    i_excptn_lvl       IN  NUMBER DEFAULT NULL,
    i_excptn_descr     IN  VARCHAR2 DEFAULT NULL
  );

  PROCEDURE upd_sp(
    i_r_upd                IN  g_rt_upd,
    i_updby_div_part       IN  VARCHAR2,
    i_updby_ord_num        IN  VARCHAR2,
    i_updby_ord_ln         IN  VARCHAR2,
    i_updby_rslvd_cd       IN  VARCHAR2,
    i_updby_excpt_lvl      IN  VARCHAR2,
    i_updby_excpt_descr    IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE del_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  );
END op_mclp300d_pk;
/

CREATE OR REPLACE PACKAGE BODY op_mclp300d_pk IS
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
  || INS_FN (record version)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Moved bulk of logic to new INS_SP and changed to call
  ||                    | it and then return the SQL row count.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_r_mclp300d  IN  mclp300d%ROWTYPE
  )
    RETURN NUMBER IS
  BEGIN
    ins_sp(i_r_mclp300d);
    RETURN(SQL%ROWCOUNT);
  END ins_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_FN (column version)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Changed to call new INS_SP and then return the SQL
  ||                    | row count.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_ord_ln       IN  NUMBER,
    i_rsn_cd       IN  mclp300d.reasnd%TYPE,
    i_descr        IN  mclp300d.descd%TYPE,
    i_excpt_descr  IN  mclp300d.exdesd%TYPE,
    i_excpt_lvl    IN  NUMBER,
    i_item_num     IN  mclp300d.itemd%TYPE,
    i_uom          IN  mclp300d.uomd%TYPE,
    i_rpl_item     IN  mclp300d.repitd%TYPE,
    i_rpl_uom      IN  mclp300d.repumd%TYPE,
    i_rpl_or_sub   IN  mclp300d.repsbd%TYPE,
    i_frm_qty      IN  NUMBER,
    i_to_qty       IN  NUMBER,
    i_rslvd_cd     IN  mclp300d.resexd%TYPE,
    i_rslvd_user   IN  mclp300d.resusd%TYPE,
    i_rslvd_dt     IN  NUMBER,
    i_rslvd_tm     IN  NUMBER
  )
    RETURN NUMBER IS
    l_r_mclp300d  mclp300d%ROWTYPE;
  BEGIN
    l_r_mclp300d.div_part := i_div_part;
    l_r_mclp300d.ordnod := i_ord_num;
    l_r_mclp300d.ordlnd := i_ord_ln;
    l_r_mclp300d.reasnd := i_rsn_cd;
    l_r_mclp300d.descd := i_descr;
    l_r_mclp300d.exlvld := i_excpt_lvl;
    l_r_mclp300d.itemd := i_item_num;
    l_r_mclp300d.uomd := i_uom;
    l_r_mclp300d.repitd := i_rpl_item;
    l_r_mclp300d.repumd := i_rpl_uom;
    l_r_mclp300d.repsbd := i_rpl_or_sub;
    l_r_mclp300d.qtyfrd := i_frm_qty;
    l_r_mclp300d.qtytod := i_to_qty;
    l_r_mclp300d.resexd := i_rslvd_cd;
    l_r_mclp300d.exdesd := i_excpt_descr;
    l_r_mclp300d.resusd := i_rslvd_user;
    l_r_mclp300d.resdtd := i_rslvd_dt;
    l_r_mclp300d.restmd := i_rslvd_tm;
    ins_sp(l_r_mclp300d);
    RETURN(SQL%ROWCOUNT);
  END ins_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Moved bulk of logic to new UPD_SP and changed to call
  ||                    | it and then return the SQL row count.
  || 12/08/15 | rhalpai | Add DivPart,UpdByDivPart input parms.
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_div_part             IN  VARCHAR2,
    i_ord_num              IN  VARCHAR2,
    i_ord_ln               IN  VARCHAR2,
    i_rsn_cd               IN  VARCHAR2,
    i_descr                IN  VARCHAR2,
    i_excpt_lvl            IN  VARCHAR2,
    i_item_num             IN  VARCHAR2,
    i_uom                  IN  VARCHAR2,
    i_rpl_item             IN  VARCHAR2,
    i_rpl_uom              IN  VARCHAR2,
    i_rpl_or_sub           IN  VARCHAR2,
    i_frm_qty              IN  VARCHAR2,
    i_to_qty               IN  VARCHAR2,
    i_rslvd_cd             IN  VARCHAR2,
    i_excpt_descr          IN  VARCHAR2,
    i_rslvd_user           IN  VARCHAR2,
    i_rslvd_dt             IN  VARCHAR2,
    i_rslvd_tm             IN  VARCHAR2,
    i_updby_div_part       IN  VARCHAR2,
    i_updby_ord_num        IN  VARCHAR2,
    i_updby_ord_ln         IN  VARCHAR2,
    i_updby_rslvd_cd       IN  VARCHAR2,
    i_updby_excpt_lvl      IN  VARCHAR2,
    i_updby_excpt_descr    IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER IS
    l_r_upd  g_rt_upd;
  BEGIN
    l_r_upd.div_part := i_div_part;
    l_r_upd.ord_num := i_ord_num;
    l_r_upd.ord_ln := i_ord_ln;
    l_r_upd.rsn_cd := i_rsn_cd;
    l_r_upd.descr := i_descr;
    l_r_upd.excpt_lvl := i_excpt_lvl;
    l_r_upd.item_num := i_item_num;
    l_r_upd.uom := i_uom;
    l_r_upd.rpl_item := i_rpl_item;
    l_r_upd.rpl_uom := i_rpl_uom;
    l_r_upd.rpl_or_sub := i_rpl_or_sub;
    l_r_upd.frm_qty := i_frm_qty;
    l_r_upd.to_qty := i_to_qty;
    l_r_upd.rslvd_cd := i_rslvd_cd;
    l_r_upd.excpt_descr := i_excpt_descr;
    l_r_upd.rslvd_user := i_rslvd_user;
    l_r_upd.rslvd_dt := i_rslvd_dt;
    l_r_upd.rslvd_tm := i_rslvd_tm;
    upd_sp(l_r_upd,
           i_updby_div_part,
           i_updby_ord_num,
           i_updby_ord_ln,
           i_updby_rslvd_cd,
           i_updby_excpt_lvl,
           i_updby_excpt_descr,
           i_use_passed_nulls_sw
          );
    RETURN(SQL%ROWCOUNT);
  END upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || DEL_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/04/06 | rhalpai | Original.
  || 12/11/06 | rhalpai | Moved bulk of logic to new DEL_SP and changed to call
  ||                    | it and then return the SQL row count.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION del_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN NUMBER IS
  BEGIN
    del_sp(i_div_part, i_ord_num);
    RETURN(SQL%ROWCOUNT);
  END del_fn;

  /*
  ||----------------------------------------------------------------------------
  || INIT_SP
  ||  Return record initialized with column defaults for order header table
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/19/13 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE init_sp(
    o_r_mclp300d  OUT  mclp300d%ROWTYPE
  ) IS
    l_cv                  SYS_REFCURSOR;
    l_c_sysdate  CONSTANT DATE          := SYSDATE;
  BEGIN
    l_cv := tbl_dflt_fn('MCLP300D');

    FETCH l_cv
     INTO o_r_mclp300d;

    CLOSE l_cv;

    o_r_mclp300d.exdtd := TO_NUMBER(TO_CHAR(l_c_sysdate, 'DD'));
    o_r_mclp300d.exmond := TO_NUMBER(TO_CHAR(l_c_sysdate, 'MM'));
    o_r_mclp300d.exyerd := TO_NUMBER(TO_CHAR(l_c_sysdate, 'YYYY'));
    o_r_mclp300d.extimd := TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS'));
    o_r_mclp300d.last_chg_ts := l_c_sysdate;
  END init_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP (record version)
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Moved bulk of logic from INS_FN and added look-up of
  ||                    | exception description and level when not passed.
  || 09/27/13 | rhalpai | Add userid to log entry. IM-120269
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_r_mclp300d  IN  mclp300d%ROWTYPE
  ) IS
    l_r_mclp300d  mclp300d%ROWTYPE;
    l_cv          SYS_REFCURSOR;
  BEGIN
    init_sp(l_r_mclp300d);
    l_r_mclp300d.div_part := i_r_mclp300d.div_part;
    l_r_mclp300d.ordnod := i_r_mclp300d.ordnod;
    l_r_mclp300d.ordlnd := i_r_mclp300d.ordlnd;
    l_r_mclp300d.reasnd := i_r_mclp300d.reasnd;
    l_r_mclp300d.descd := i_r_mclp300d.descd;
    l_r_mclp300d.exlvld := i_r_mclp300d.exlvld;
    l_r_mclp300d.itemd := i_r_mclp300d.itemd;
    l_r_mclp300d.uomd := i_r_mclp300d.uomd;
    l_r_mclp300d.qtyfrd := i_r_mclp300d.qtyfrd;
    l_r_mclp300d.qtytod := i_r_mclp300d.qtytod;
    l_r_mclp300d.resexd := i_r_mclp300d.resexd;
    l_r_mclp300d.resusd := i_r_mclp300d.resusd;

    IF i_r_mclp300d.descd IS NULL THEN
      OPEN l_cv
       FOR
         SELECT a.desca, NVL(i_r_mclp300d.exlvld, a.exlvla)
           FROM mclp140a a
          WHERE a.rsncda = l_r_mclp300d.reasnd;

      FETCH l_cv
       INTO l_r_mclp300d.descd, l_r_mclp300d.exlvld;
    END IF;   -- i_r_mclp300d.descd

    INSERT INTO mclp300d
         VALUES l_r_mclp300d;
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||  Add Exception Log Entry
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/05/11 | rhalpai | Original for PIR6235
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_div_part         IN  NUMBER,
    i_ord_num          IN  NUMBER,
    i_ord_ln           IN  NUMBER,
    i_rsn_cd           IN  VARCHAR2,
    i_item_num         IN  VARCHAR2,
    i_uom              IN  VARCHAR2,
    i_from_qty         IN  NUMBER,
    i_to_qty           IN  NUMBER,
    i_resolved_excptn  IN  VARCHAR2 DEFAULT '0',
    i_excptn_lvl       IN  NUMBER DEFAULT NULL,
    i_excptn_descr     IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_r_mclp300d  mclp300d%ROWTYPE;
  BEGIN
    l_r_mclp300d.div_part := i_div_part;
    l_r_mclp300d.ordnod := i_ord_num;
    l_r_mclp300d.ordlnd := i_ord_ln;
    l_r_mclp300d.reasnd := i_rsn_cd;
    l_r_mclp300d.descd := i_excptn_descr;
    l_r_mclp300d.exlvld := i_excptn_lvl;
    l_r_mclp300d.itemd := i_item_num;
    l_r_mclp300d.uomd := i_uom;
    l_r_mclp300d.qtyfrd := i_from_qty;
    l_r_mclp300d.qtytod := i_to_qty;
    l_r_mclp300d.resexd := i_resolved_excptn;
    ins_sp(l_r_mclp300d);
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Moved bulk of logic from UPD_FN and changed to use
  ||                    | new GET_VAL_FN instead of CASE statement.
  || 12/08/15 | rhalpai | Add UpdByDivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_r_upd                IN  g_rt_upd,
    i_updby_div_part       IN  VARCHAR2,
    i_updby_ord_num        IN  VARCHAR2,
    i_updby_ord_ln         IN  VARCHAR2,
    i_updby_rslvd_cd       IN  VARCHAR2,
    i_updby_excpt_lvl      IN  VARCHAR2,
    i_updby_excpt_descr    IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_c_sysdate  CONSTANT DATE      := SYSDATE;
    l_t_updby_ord_lns     type_ntab;
  BEGIN
    IF i_updby_ord_ln IS NOT NULL THEN
      l_t_updby_ord_lns := num.parse_list(i_updby_ord_ln);

      IF l_t_updby_ord_lns.COUNT > 0 THEN
        FORALL i IN l_t_updby_ord_lns.FIRST .. l_t_updby_ord_lns.LAST
          UPDATE mclp300d
             SET ordnod = get_val_fn(ordnod, i_r_upd.ord_num, i_use_passed_nulls_sw),
                 ordlnd = get_val_fn(ordlnd, i_r_upd.ord_ln, i_use_passed_nulls_sw),
                 reasnd = get_val_fn(reasnd, i_r_upd.rsn_cd, i_use_passed_nulls_sw),
                 descd = get_val_fn(descd, i_r_upd.descr, i_use_passed_nulls_sw),
                 exlvld = get_val_fn(exlvld, i_r_upd.excpt_lvl, i_use_passed_nulls_sw),
                 itemd = get_val_fn(itemd, i_r_upd.item_num, i_use_passed_nulls_sw),
                 uomd = get_val_fn(uomd, i_r_upd.uom, i_use_passed_nulls_sw),
                 repitd = get_val_fn(repitd, i_r_upd.rpl_item, i_use_passed_nulls_sw),
                 repumd = get_val_fn(repumd, i_r_upd.rpl_uom, i_use_passed_nulls_sw),
                 repsbd = get_val_fn(repsbd, i_r_upd.rpl_or_sub, i_use_passed_nulls_sw),
                 qtyfrd = get_val_fn(qtyfrd, i_r_upd.frm_qty, i_use_passed_nulls_sw),
                 qtytod = get_val_fn(qtytod, i_r_upd.to_qty, i_use_passed_nulls_sw),
                 exdtd = TO_NUMBER(TO_CHAR(l_c_sysdate, 'DD')),
                 exmond = TO_NUMBER(TO_CHAR(l_c_sysdate, 'MM')),
                 exyerd = TO_NUMBER(TO_CHAR(l_c_sysdate, 'YYYY')),
                 extimd = TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS')),
                 resexd = get_val_fn(resexd, i_r_upd.rslvd_cd, i_use_passed_nulls_sw),
                 exdesd = get_val_fn(exdesd, i_r_upd.excpt_descr, i_use_passed_nulls_sw),
                 resusd = get_val_fn(resusd, i_r_upd.rslvd_user, i_use_passed_nulls_sw),
                 resdtd = get_val_fn(resdtd, i_r_upd.rslvd_dt, i_use_passed_nulls_sw),
                 restmd = get_val_fn(restmd, i_r_upd.rslvd_tm, i_use_passed_nulls_sw),
                 last_chg_ts = l_c_sysdate
           WHERE div_part = i_updby_div_part
             AND ordnod = i_updby_ord_num
             AND ordlnd = l_t_updby_ord_lns(i)
             AND NVL(i_updby_rslvd_cd, resexd) = resexd
             AND NVL(i_updby_excpt_lvl, exlvld) = exlvld
             AND NVL(i_updby_excpt_descr, exdesd) = exdesd;
      END IF;   -- l_t_updby_ord_lns.COUNT > 0
    ELSE
      UPDATE mclp300d
         SET ordnod = get_val_fn(ordnod, i_r_upd.ord_num, i_use_passed_nulls_sw),
             ordlnd = get_val_fn(ordlnd, i_r_upd.ord_ln, i_use_passed_nulls_sw),
             reasnd = get_val_fn(reasnd, i_r_upd.rsn_cd, i_use_passed_nulls_sw),
             descd = get_val_fn(descd, i_r_upd.descr, i_use_passed_nulls_sw),
             exlvld = get_val_fn(exlvld, i_r_upd.excpt_lvl, i_use_passed_nulls_sw),
             itemd = get_val_fn(itemd, i_r_upd.item_num, i_use_passed_nulls_sw),
             uomd = get_val_fn(uomd, i_r_upd.uom, i_use_passed_nulls_sw),
             repitd = get_val_fn(repitd, i_r_upd.rpl_item, i_use_passed_nulls_sw),
             repumd = get_val_fn(repumd, i_r_upd.rpl_uom, i_use_passed_nulls_sw),
             repsbd = get_val_fn(repsbd, i_r_upd.rpl_or_sub, i_use_passed_nulls_sw),
             qtyfrd = get_val_fn(qtyfrd, i_r_upd.frm_qty, i_use_passed_nulls_sw),
             qtytod = get_val_fn(qtytod, i_r_upd.to_qty, i_use_passed_nulls_sw),
             exdtd = TO_NUMBER(TO_CHAR(l_c_sysdate, 'DD')),
             exmond = TO_NUMBER(TO_CHAR(l_c_sysdate, 'MM')),
             exyerd = TO_NUMBER(TO_CHAR(l_c_sysdate, 'YYYY')),
             extimd = TO_NUMBER(TO_CHAR(l_c_sysdate, 'HH24MISS')),
             resexd = get_val_fn(resexd, i_r_upd.rslvd_cd, i_use_passed_nulls_sw),
             exdesd = get_val_fn(exdesd, i_r_upd.excpt_descr, i_use_passed_nulls_sw),
             resusd = get_val_fn(resusd, i_r_upd.rslvd_user, i_use_passed_nulls_sw),
             resdtd = get_val_fn(resdtd, i_r_upd.rslvd_dt, i_use_passed_nulls_sw),
             restmd = get_val_fn(restmd, i_r_upd.rslvd_tm, i_use_passed_nulls_sw),
             last_chg_ts = l_c_sysdate
       WHERE div_part = i_updby_div_part
         AND ordnod = i_updby_ord_num
         AND NVL(i_updby_rslvd_cd, resexd) = resexd
         AND NVL(i_updby_excpt_lvl, exlvld) = exlvld
         AND NVL(i_updby_excpt_descr, exdesd) = exdesd;
    END IF;   -- i_updby_ord_ln IS NOT NULL
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/11/06 | rhalpai | Moved bulk of logic from DEL_FN.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
  BEGIN
    DELETE FROM mclp300d
          WHERE div_part = i_div_part
            AND ordnod = i_ord_num;
  END del_sp;
END op_mclp300d_pk;
/

