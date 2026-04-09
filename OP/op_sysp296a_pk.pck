CREATE OR REPLACE PACKAGE op_sysp296a_pk IS
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
  FUNCTION ins_fn(
    i_r_sysp296a  IN  sysp296a%ROWTYPE
  )
    RETURN NUMBER;

  FUNCTION ins_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_user_id   IN  VARCHAR2,
    i_tbl_nm    IN  VARCHAR2,
    i_fld_nm    IN  VARCHAR2,
    i_orig_val  IN  VARCHAR2,
    i_new_val   IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_rsn_txt   IN  VARCHAR2
  )
    RETURN NUMBER;

  FUNCTION del_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN NUMBER;

  FUNCTION del_date_less_eq_fn(
    i_div_part  IN  NUMBER,
    i_dt        IN  NUMBER
  )
    RETURN NUMBER;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE init_sp(
    o_r_sysp296a  OUT  sysp296a%ROWTYPE
  );

  PROCEDURE ins_sp(
    i_r_sysp296a  IN  sysp296a%ROWTYPE
  );

  PROCEDURE ins_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_user_id   IN  VARCHAR2,
    i_tbl_nm    IN  VARCHAR2,
    i_fld_nm    IN  VARCHAR2,
    i_orig_val  IN  VARCHAR2,
    i_new_val   IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_rsn_txt   IN  VARCHAR2
  );

  PROCEDURE del_ord_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  );

  PROCEDURE del_date_less_eq_sp(
    i_div_part  IN  NUMBER,
    i_dt        IN  NUMBER
  );
END op_sysp296a_pk;
/

CREATE OR REPLACE PACKAGE BODY op_sysp296a_pk IS
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
  || INS_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_r_sysp296a  IN  sysp296a%ROWTYPE
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SYSP296A_PK.INS_FN';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_r_sysp296a.div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_r_sysp296a.ordnoa);
    logs.add_parm(lar_parm, 'OrdLn', i_r_sysp296a.linea);
    logs.add_parm(lar_parm, 'UserId', i_r_sysp296a.usera);
    logs.add_parm(lar_parm, 'TblNm', i_r_sysp296a.tblnma);
    logs.add_parm(lar_parm, 'FieldNm', i_r_sysp296a.fldnma);
    logs.add_parm(lar_parm, 'OrigVal', i_r_sysp296a.florga);
    logs.add_parm(lar_parm, 'NewVal', i_r_sysp296a.flchga);
    logs.add_parm(lar_parm, 'ActnCd', i_r_sysp296a.actna);
    logs.add_parm(lar_parm, 'RsnCd', i_r_sysp296a.rsncda);
    logs.add_parm(lar_parm, 'RsnTxt', i_r_sysp296a.rsntxa);
    logs.add_parm(lar_parm, 'AuthBy', i_r_sysp296a.autbya);
    ins_sp(i_r_sysp296a);
    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_fn;

  /*
  ||----------------------------------------------------------------------------
  || INS_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_user_id   IN  VARCHAR2,
    i_tbl_nm    IN  VARCHAR2,
    i_fld_nm    IN  VARCHAR2,
    i_orig_val  IN  VARCHAR2,
    i_new_val   IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_rsn_txt   IN  VARCHAR2
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SYSP296A_PK.INS_FN';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'TblNm', i_tbl_nm);
    logs.add_parm(lar_parm, 'FieldNm', i_fld_nm);
    logs.add_parm(lar_parm, 'OrigVal', i_orig_val);
    logs.add_parm(lar_parm, 'NewVal', i_new_val);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'RsnTxt', i_rsn_txt);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    ins_sp(i_div_part,
           i_ord_num,
           i_ord_ln,
           i_user_id,
           i_tbl_nm,
           i_fld_nm,
           i_orig_val,
           i_new_val,
           i_actn_cd,
           i_rsn_cd,
           i_auth_by,
           i_rsn_txt
          );
    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_fn;

  /*
  ||----------------------------------------------------------------------------
  || DEL_ORD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION del_ord_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SYSP296A_PK.DEL_ORD_FN';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    del_ord_sp(i_div_part, i_ord_num);
    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_ord_fn;

  /*
  ||----------------------------------------------------------------------------
  || DEL_DATE_LESS_EQ_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION del_date_less_eq_fn(
    i_div_part  IN  NUMBER,
    i_dt        IN  NUMBER
  )
    RETURN NUMBER IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SYSP296A_PK.DEL_DATE_LESS_EQ_FN';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Dt', i_dt);
    del_date_less_eq_sp(i_div_part, i_dt);
    RETURN(SQL%ROWCOUNT);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_date_less_eq_fn;

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
    o_r_sysp296a  OUT  sysp296a%ROWTYPE
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    l_cv := tbl_dflt_fn('SYSP296A');

    FETCH l_cv
     INTO o_r_sysp296a;

    CLOSE l_cv;
  END init_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 02/19/07 | Arun    | The Source for Date and Time on SYSP296A are
  ||                      deferred to the time of insert.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_r_sysp296a  IN  sysp296a%ROWTYPE
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm      := 'OP_SYSP296A_PK.INS_SP';
    lar_parm              logs.tar_parm;
    l_r_sysp296a          sysp296a%ROWTYPE;
    l_c_sysdate  CONSTANT DATE               := SYSDATE;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_r_sysp296a.div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_r_sysp296a.ordnoa);
    logs.add_parm(lar_parm, 'OrdLn', i_r_sysp296a.linea);
    logs.add_parm(lar_parm, 'UserId', i_r_sysp296a.usera);
    logs.add_parm(lar_parm, 'TblNm', i_r_sysp296a.tblnma);
    logs.add_parm(lar_parm, 'FieldNm', i_r_sysp296a.fldnma);
    logs.add_parm(lar_parm, 'OrigVal', i_r_sysp296a.florga);
    logs.add_parm(lar_parm, 'NewVal', i_r_sysp296a.flchga);
    logs.add_parm(lar_parm, 'ActnCd', i_r_sysp296a.actna);
    logs.add_parm(lar_parm, 'RsnCd', i_r_sysp296a.rsncda);
    logs.add_parm(lar_parm, 'RsnTxt', i_r_sysp296a.rsntxa);
    logs.add_parm(lar_parm, 'AuthBy', i_r_sysp296a.autbya);
    init_sp(l_r_sysp296a);
    l_r_sysp296a.div_part := i_r_sysp296a.div_part;
    l_r_sysp296a.ordnoa := i_r_sysp296a.ordnoa;
    l_r_sysp296a.linea := i_r_sysp296a.linea;
    l_r_sysp296a.tblnma := i_r_sysp296a.tblnma;
    l_r_sysp296a.fldnma := i_r_sysp296a.fldnma;
    l_r_sysp296a.florga := i_r_sysp296a.florga;
    l_r_sysp296a.flchga := i_r_sysp296a.flchga;
    l_r_sysp296a.actna := i_r_sysp296a.actna;
    l_r_sysp296a.rsncda := i_r_sysp296a.rsncda;
    l_r_sysp296a.rsntxa := i_r_sysp296a.rsntxa;
    l_r_sysp296a.autbya := i_r_sysp296a.autbya;
    l_r_sysp296a.usera := i_r_sysp296a.usera;
    l_r_sysp296a.datea := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_r_sysp296a.timea := TO_CHAR(l_c_sysdate, 'HH24MISS');

    INSERT INTO sysp296a
         VALUES l_r_sysp296a;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_user_id   IN  VARCHAR2,
    i_tbl_nm    IN  VARCHAR2,
    i_fld_nm    IN  VARCHAR2,
    i_orig_val  IN  VARCHAR2,
    i_new_val   IN  VARCHAR2,
    i_actn_cd   IN  VARCHAR2,
    i_rsn_cd    IN  VARCHAR2,
    i_auth_by   IN  VARCHAR2,
    i_rsn_txt   IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm      := 'OP_SYSP296A_PK.INS_SP';
    lar_parm             logs.tar_parm;
    l_r_sysp296a         sysp296a%ROWTYPE;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'OrdLn', i_ord_ln);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'TblNm', i_tbl_nm);
    logs.add_parm(lar_parm, 'FieldNm', i_fld_nm);
    logs.add_parm(lar_parm, 'OrigVal', i_orig_val);
    logs.add_parm(lar_parm, 'NewVal', i_new_val);
    logs.add_parm(lar_parm, 'ActnCd', i_actn_cd);
    logs.add_parm(lar_parm, 'RsnCd', i_rsn_cd);
    logs.add_parm(lar_parm, 'RsnTxt', i_rsn_txt);
    logs.add_parm(lar_parm, 'AuthBy', i_auth_by);
    l_r_sysp296a.div_part := i_div_part;
    l_r_sysp296a.ordnoa := i_ord_num;
    l_r_sysp296a.linea := i_ord_ln;
    l_r_sysp296a.usera := i_user_id;
    l_r_sysp296a.tblnma := i_tbl_nm;
    l_r_sysp296a.fldnma := i_fld_nm;
    l_r_sysp296a.florga := i_orig_val;
    l_r_sysp296a.flchga := i_new_val;
    l_r_sysp296a.actna := i_actn_cd;
    l_r_sysp296a.rsncda := i_rsn_cd;
    l_r_sysp296a.autbya := i_auth_by;
    l_r_sysp296a.rsntxa := i_rsn_txt;
    ins_sp(l_r_sysp296a);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_ORD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_ord_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SYSP296A_PK.DEL_ORD_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);

    DELETE FROM sysp296a
          WHERE div_part = i_div_part
            AND ordnoa = i_ord_num;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_DATE_LESS_EQ_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_date_less_eq_sp(
    i_div_part  IN  NUMBER,
    i_dt        IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_SYSP296A_PK.DEL_DATE_LESS_EQ_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'Dt', i_dt);

    DELETE FROM sysp296a
          WHERE div_part = i_div_part
            AND datea <= i_dt;
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_date_less_eq_sp;
END op_sysp296a_pk;
/

