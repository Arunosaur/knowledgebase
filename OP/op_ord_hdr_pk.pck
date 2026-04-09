CREATE OR REPLACE PACKAGE op_ord_hdr_pk IS
--------------------------------------------------------------------------------
--                               PUBLIC CURSORS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                                PUBLIC TYPES
--------------------------------------------------------------------------------
  SUBTYPE g_st_vchar IS VARCHAR2(100);

  TYPE g_rt_ord_hdr IS RECORD(
    excptn_sw        g_st_vchar,
    ord_num          NUMBER,
    stat_cd          g_st_vchar,
    div_part         NUMBER,
    ord_typ          g_st_vchar,
    load_typ         g_st_vchar,
    ord_src          g_st_vchar,
    cust_id          g_st_vchar,
    conf_num         g_st_vchar,
    cust_pass_area   g_st_vchar,
    shp_dt           g_st_vchar,
    po_num           g_st_vchar,
    hdr_excptn_cd    g_st_vchar,
    maint_user_id    g_st_vchar,
    ser_num          g_st_vchar,
    trnsmt_dt        g_st_vchar,
    trnsmt_tm        g_st_vchar,
    rlse_ts          g_st_vchar,
    legcy_ref        g_st_vchar,
    ord_rcvd_ts      g_st_vchar,
    allw_partl_sw    g_st_vchar,
    load_depart_sid  NUMBER
  );

--------------------------------------------------------------------------------
--                 PUBLIC CONSTANTS, VARIABLES, EXCEPTIONS, ETC.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                              PUBLIC FUNCTIONS
--------------------------------------------------------------------------------
  FUNCTION sel_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN ordp100a%ROWTYPE;

  FUNCTION ins_fn(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  )
    RETURN NUMBER;

  FUNCTION ins_fn(
    i_excptn_sw        IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_stat_cd          IN  VARCHAR2,
    i_div_part         IN  NUMBER,
    i_ord_typ          IN  VARCHAR2,
    i_load_typ         IN  VARCHAR2,
    i_ord_src          IN  VARCHAR2,
    i_cust_id          IN  VARCHAR2,
    i_conf_num         IN  VARCHAR2,
    i_cust_pass_area   IN  VARCHAR2,
    i_shp_dt           IN  VARCHAR2,
    i_po_num           IN  VARCHAR2,
    i_hdr_excptn_cd    IN  VARCHAR2,
    i_maint_user_id    IN  VARCHAR2,
    i_ser_num          IN  VARCHAR2,
    i_trnsmt_dt        IN  VARCHAR2,
    i_trnsmt_tm        IN  VARCHAR2,
    i_rlse_ts          IN  VARCHAR2,
    i_legcy_ref        IN  VARCHAR2,
    i_ord_rcvd_ts      IN  VARCHAR2,
    i_allw_partl_sw    IN  VARCHAR2,
    i_load_depart_sid  IN  NUMBER
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_excptn_sw            IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_stat_cd              IN  VARCHAR2,
    i_div_part             IN  NUMBER,
    i_ord_typ              IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_ord_src              IN  VARCHAR2,
    i_cust_id              IN  VARCHAR2,
    i_conf_num             IN  VARCHAR2,
    i_cust_pass_area       IN  VARCHAR2,
    i_shp_dt               IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_hdr_excptn_cd        IN  VARCHAR2,
    i_maint_user_id        IN  VARCHAR2,
    i_ser_num              IN  VARCHAR2,
    i_trnsmt_dt            IN  VARCHAR2,
    i_trnsmt_tm            IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_legcy_ref            IN  VARCHAR2,
    i_ord_rcvd_ts          IN  VARCHAR2,
    i_allw_partl_sw        IN  VARCHAR2,
    i_load_depart_sid      IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER;

  FUNCTION upd_fn(
    i_r_ord_hdr            IN  g_rt_ord_hdr,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER;

  FUNCTION del_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_conf_num  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN NUMBER;

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE init_sp(
    o_r_ordp100a  OUT  ordp100a%ROWTYPE
  );

  /**
  ||----------------------------------------------------------------------------
  || Attempt to lock unbilled order header and details for order number.
  || The lock will be performed with NOWAIT option.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #param o_lock_sw          Indicate lock success Y or N
  || #param o_r_ord_hdr        OrdHdr record
  || #param o_t_ord_dtls       Table of OrdDtl records
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE lock_ord_sp(
    i_div_part    IN      NUMBER,
    i_ord_num     IN      NUMBER,
    o_lock_sw     OUT     VARCHAR2,
    o_r_ord_hdr   OUT     ordp100a%ROWTYPE,
    o_t_ord_dtls  OUT     op_ord_dtl_pk.g_tt_ord_dtls
  );

  /**
  ||----------------------------------------------------------------------------
  || Attempt to lock unbilled order header and details for order number.
  || The lock will be performed with NOWAIT option.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  || #param o_lock_sw          Indicate lock success Y or N
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE lock_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    o_lock_sw   OUT     VARCHAR2
  );

  PROCEDURE ins_sp(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  );

  PROCEDURE ins_sp(
    i_r_ord_hdr  IN  g_rt_ord_hdr
  );

  PROCEDURE ins_sp(
    i_excptn_sw        IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_stat_cd          IN  VARCHAR2,
    i_div_part         IN  NUMBER,
    i_ord_typ          IN  VARCHAR2,
    i_load_typ         IN  VARCHAR2,
    i_ord_src          IN  VARCHAR2,
    i_cust_id          IN  VARCHAR2,
    i_conf_num         IN  VARCHAR2,
    i_cust_pass_area   IN  VARCHAR2,
    i_shp_dt           IN  VARCHAR2,
    i_po_num           IN  VARCHAR2,
    i_hdr_excptn_cd    IN  VARCHAR2,
    i_maint_user_id    IN  VARCHAR2,
    i_ser_num          IN  VARCHAR2,
    i_trnsmt_dt        IN  VARCHAR2,
    i_trnsmt_tm        IN  VARCHAR2,
    i_rlse_ts          IN  VARCHAR2,
    i_legcy_ref        IN  VARCHAR2,
    i_ord_rcvd_ts      IN  VARCHAR2,
    i_allw_partl_sw    IN  VARCHAR2,
    i_load_depart_sid  IN  NUMBER
  );

  PROCEDURE upd_sp(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  );

  PROCEDURE upd_sp(
    i_excptn_sw            IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_stat_cd              IN  VARCHAR2,
    i_div_part             IN  NUMBER,
    i_ord_typ              IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_ord_src              IN  VARCHAR2,
    i_cust_id              IN  VARCHAR2,
    i_conf_num             IN  VARCHAR2,
    i_cust_pass_area       IN  VARCHAR2,
    i_shp_dt               IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_hdr_excptn_cd        IN  VARCHAR2,
    i_maint_user_id        IN  VARCHAR2,
    i_ser_num              IN  VARCHAR2,
    i_trnsmt_dt            IN  VARCHAR2,
    i_trnsmt_tm            IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_legcy_ref            IN  VARCHAR2,
    i_ord_rcvd_ts          IN  VARCHAR2,
    i_allw_partl_sw        IN  VARCHAR2,
    i_load_depart_sid      IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE upd_sp(
    i_r_ord_hdr            IN  g_rt_ord_hdr,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  );

  PROCEDURE del_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_conf_num  IN  VARCHAR2 DEFAULT NULL
  );
END op_ord_hdr_pk;
/

CREATE OR REPLACE PACKAGE BODY op_ord_hdr_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  FUNCTION sel_for_upd_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN ordp100a%ROWTYPE IS
    l_r_ord_hdr  ordp100a%ROWTYPE;
  BEGIN
    SELECT     *
          INTO l_r_ord_hdr
          FROM ordp100a a
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num
           AND a.stata IN('O', 'I', 'S')
    FOR UPDATE NOWAIT;

    RETURN(l_r_ord_hdr);
  END sel_for_upd_fn;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || SEL_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION sel_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN ordp100a%ROWTYPE IS
    l_r_ord_hdr  ordp100a%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_r_ord_hdr
      FROM ordp100a
     WHERE div_part = i_div_part
       AND ordnoa = i_ord_num;

    RETURN(l_r_ord_hdr);
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN(l_r_ord_hdr);
  END sel_fn;

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
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  )
    RETURN NUMBER IS
  BEGIN
    ins_sp(i_r_ordp100a);
    RETURN(SQL%ROWCOUNT);
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
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION ins_fn(
    i_excptn_sw        IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_stat_cd          IN  VARCHAR2,
    i_div_part         IN  NUMBER,
    i_ord_typ          IN  VARCHAR2,
    i_load_typ         IN  VARCHAR2,
    i_ord_src          IN  VARCHAR2,
    i_cust_id          IN  VARCHAR2,
    i_conf_num         IN  VARCHAR2,
    i_cust_pass_area   IN  VARCHAR2,
    i_shp_dt           IN  VARCHAR2,
    i_po_num           IN  VARCHAR2,
    i_hdr_excptn_cd    IN  VARCHAR2,
    i_maint_user_id    IN  VARCHAR2,
    i_ser_num          IN  VARCHAR2,
    i_trnsmt_dt        IN  VARCHAR2,
    i_trnsmt_tm        IN  VARCHAR2,
    i_rlse_ts          IN  VARCHAR2,
    i_legcy_ref        IN  VARCHAR2,
    i_ord_rcvd_ts      IN  VARCHAR2,
    i_allw_partl_sw    IN  VARCHAR2,
    i_load_depart_sid  IN  NUMBER
  )
    RETURN NUMBER IS
  BEGIN
    ins_sp(i_excptn_sw,
           i_ord_num,
           i_stat_cd,
           i_div_part,
           i_ord_typ,
           i_load_typ,
           i_ord_src,
           i_cust_id,
           i_conf_num,
           i_cust_pass_area,
           i_shp_dt,
           i_po_num,
           i_hdr_excptn_cd,
           i_maint_user_id,
           i_ser_num,
           i_trnsmt_dt,
           i_trnsmt_tm,
           i_rlse_ts,
           i_legcy_ref,
           i_ord_rcvd_ts,
           i_allw_partl_sw,
           i_load_depart_sid
          );
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
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  )
    RETURN NUMBER IS
  BEGIN
    upd_sp(i_r_ordp100a);
    RETURN(SQL%ROWCOUNT);
  END upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_excptn_sw            IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_stat_cd              IN  VARCHAR2,
    i_div_part             IN  NUMBER,
    i_ord_typ              IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_ord_src              IN  VARCHAR2,
    i_cust_id              IN  VARCHAR2,
    i_conf_num             IN  VARCHAR2,
    i_cust_pass_area       IN  VARCHAR2,
    i_shp_dt               IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_hdr_excptn_cd        IN  VARCHAR2,
    i_maint_user_id        IN  VARCHAR2,
    i_ser_num              IN  VARCHAR2,
    i_trnsmt_dt            IN  VARCHAR2,
    i_trnsmt_tm            IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_legcy_ref            IN  VARCHAR2,
    i_ord_rcvd_ts          IN  VARCHAR2,
    i_allw_partl_sw        IN  VARCHAR2,
    i_load_depart_sid      IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER IS
  BEGIN
    upd_sp(i_excptn_sw,
           i_ord_num,
           i_stat_cd,
           i_div_part,
           i_ord_typ,
           i_load_typ,
           i_ord_src,
           i_cust_id,
           i_conf_num,
           i_cust_pass_area,
           i_shp_dt,
           i_po_num,
           i_hdr_excptn_cd,
           i_maint_user_id,
           i_ser_num,
           i_trnsmt_dt,
           i_trnsmt_tm,
           i_rlse_ts,
           i_legcy_ref,
           i_ord_rcvd_ts,
           i_allw_partl_sw,
           i_load_depart_sid,
           i_use_passed_nulls_sw
          );
    RETURN(SQL%ROWCOUNT);
  END upd_fn;

  /*
  ||----------------------------------------------------------------------------
  || UPD_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  FUNCTION upd_fn(
    i_r_ord_hdr            IN  g_rt_ord_hdr,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  )
    RETURN NUMBER IS
  BEGIN
    upd_sp(i_r_ord_hdr, i_use_passed_nulls_sw);
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
  || 09/22/05 | rhalpai | Original.
  || 01/20/12 | rhalpai | Remove P_EXCPTN_SW parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION del_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_conf_num  IN  VARCHAR2 DEFAULT NULL
  )
    RETURN NUMBER IS
  BEGIN
    del_sp(i_div_part, i_ord_num, i_conf_num);
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
  || 07/10/12 | rhalpai | Original
  || 05/13/13 | rhalpai | Add Div parm. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE init_sp(
    o_r_ordp100a  OUT  ordp100a%ROWTYPE
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    l_cv := tbl_dflt_fn('ORDP100A');

    FETCH l_cv
     INTO o_r_ordp100a;

    CLOSE l_cv;
  END init_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOCK_ORD_SP  (OrdNum)
  ||  Attempt to lock unbilled order header and details and return header table
  ||  name if successful.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 06/20/08 | rhalpai | Removed check for detail with status not in O,I,S,C
  ||                    | in cursors. PIR6364
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE lock_ord_sp(
    i_div_part    IN      NUMBER,
    i_ord_num     IN      NUMBER,
    o_lock_sw     OUT     VARCHAR2,
    o_r_ord_hdr   OUT     ordp100a%ROWTYPE,
    o_t_ord_dtls  OUT     op_ord_dtl_pk.g_tt_ord_dtls
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORD_HDR_PK.LOCK_ORD_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    o_lock_sw := 'N';
    SAVEPOINT b4_lock;
    o_r_ord_hdr := sel_for_upd_fn(i_div_part, i_ord_num);
    o_t_ord_dtls := op_ord_dtl_pk.sel_for_upd_fn(i_div_part, i_ord_num);
    o_lock_sw := 'Y';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      ROLLBACK TO SAVEPOINT b4_lock;
    WHEN excp.gx_row_locked THEN
      logs.warn('RESOURCE_BUSY_NOWAIT occurred', lar_parm);
      ROLLBACK TO SAVEPOINT b4_lock;
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END lock_ord_sp;

  PROCEDURE lock_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    o_lock_sw   OUT     VARCHAR2
  ) IS
    l_r_ord_hdr   ordp100a%ROWTYPE;
    l_t_ord_dtls  op_ord_dtl_pk.g_tt_ord_dtls;
  BEGIN
    lock_ord_sp(i_div_part, i_ord_num, o_lock_sw, l_r_ord_hdr, l_t_ord_dtls);
  END lock_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  ) IS
  BEGIN
    INSERT INTO ordp100a
         VALUES i_r_ordp100a;
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/08/07 | rhalpai | Original
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add Div in call to INIT_SP. PIR11038
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_r_ord_hdr  IN  g_rt_ord_hdr
  ) IS
    l_r_ordp100a  ordp100a%ROWTYPE;
  BEGIN
    init_sp(l_r_ordp100a);
    l_r_ordp100a.div_part := i_r_ord_hdr.div_part;
    l_r_ordp100a.ordnoa := i_r_ord_hdr.ord_num;
    l_r_ordp100a.excptn_sw := i_r_ord_hdr.excptn_sw;
    l_r_ordp100a.stata := i_r_ord_hdr.stat_cd;
    l_r_ordp100a.dsorda := i_r_ord_hdr.ord_typ;
    l_r_ordp100a.ldtypa := i_r_ord_hdr.load_typ;
    l_r_ordp100a.ipdtsa := i_r_ord_hdr.ord_src;
    l_r_ordp100a.custa := i_r_ord_hdr.cust_id;
    l_r_ordp100a.connba := i_r_ord_hdr.conf_num;
    l_r_ordp100a.cspasa := i_r_ord_hdr.cust_pass_area;
    l_r_ordp100a.shpja := i_r_ord_hdr.shp_dt;
    l_r_ordp100a.cpoa := i_r_ord_hdr.po_num;
    l_r_ordp100a.hdexpa := i_r_ord_hdr.hdr_excptn_cd;
    l_r_ordp100a.mntusa := i_r_ord_hdr.maint_user_id;
    l_r_ordp100a.telsla := i_r_ord_hdr.ser_num;
    l_r_ordp100a.trndta := i_r_ord_hdr.trnsmt_dt;
    l_r_ordp100a.trntma := i_r_ord_hdr.trnsmt_tm;
    l_r_ordp100a.uschga := i_r_ord_hdr.rlse_ts;
    l_r_ordp100a.legrfa := i_r_ord_hdr.legcy_ref;
    l_r_ordp100a.ord_rcvd_ts := NVL(TO_DATE(i_r_ord_hdr.ord_rcvd_ts, 'YYYYMMDDHH24MISS'), SYSDATE);
    l_r_ordp100a.pshipa := i_r_ord_hdr.allw_partl_sw;

    IF i_r_ord_hdr.load_depart_sid IS NOT NULL THEN
      l_r_ordp100a.load_depart_sid := i_r_ord_hdr.load_depart_sid;
    END IF;

    ins_sp(l_r_ordp100a);
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
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_sp(
    i_excptn_sw        IN  VARCHAR2,
    i_ord_num          IN  NUMBER,
    i_stat_cd          IN  VARCHAR2,
    i_div_part         IN  NUMBER,
    i_ord_typ          IN  VARCHAR2,
    i_load_typ         IN  VARCHAR2,
    i_ord_src          IN  VARCHAR2,
    i_cust_id          IN  VARCHAR2,
    i_conf_num         IN  VARCHAR2,
    i_cust_pass_area   IN  VARCHAR2,
    i_shp_dt           IN  VARCHAR2,
    i_po_num           IN  VARCHAR2,
    i_hdr_excptn_cd    IN  VARCHAR2,
    i_maint_user_id    IN  VARCHAR2,
    i_ser_num          IN  VARCHAR2,
    i_trnsmt_dt        IN  VARCHAR2,
    i_trnsmt_tm        IN  VARCHAR2,
    i_rlse_ts          IN  VARCHAR2,
    i_legcy_ref        IN  VARCHAR2,
    i_ord_rcvd_ts      IN  VARCHAR2,
    i_allw_partl_sw    IN  VARCHAR2,
    i_load_depart_sid  IN  NUMBER
  ) IS
    l_r_ordp100a  ordp100a%ROWTYPE;
  BEGIN
    l_r_ordp100a.div_part := i_div_part;
    l_r_ordp100a.ordnoa := i_ord_num;
    l_r_ordp100a.excptn_sw := i_excptn_sw;
    l_r_ordp100a.stata := i_stat_cd;
    l_r_ordp100a.dsorda := i_ord_typ;
    l_r_ordp100a.ldtypa := i_load_typ;
    l_r_ordp100a.ipdtsa := i_ord_src;
    l_r_ordp100a.custa := i_cust_id;
    l_r_ordp100a.connba := i_conf_num;
    l_r_ordp100a.cspasa := i_cust_pass_area;
    l_r_ordp100a.shpja := i_shp_dt;
    l_r_ordp100a.cpoa := i_po_num;
    l_r_ordp100a.hdexpa := i_hdr_excptn_cd;
    l_r_ordp100a.mntusa := i_maint_user_id;
    l_r_ordp100a.telsla := i_ser_num;
    l_r_ordp100a.trndta := i_trnsmt_dt;
    l_r_ordp100a.trntma := i_trnsmt_tm;
    l_r_ordp100a.uschga := i_rlse_ts;
    l_r_ordp100a.legrfa := i_legcy_ref;
    l_r_ordp100a.ord_rcvd_ts := NVL(TO_DATE(i_ord_rcvd_ts, 'YYYYMMDDHH24MISS'), SYSDATE);
    l_r_ordp100a.pshipa := i_allw_partl_sw;

    IF i_load_depart_sid IS NOT NULL THEN
      l_r_ordp100a.load_depart_sid := i_load_depart_sid;
    END IF;

    ins_sp(l_r_ordp100a);
  END ins_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_r_ordp100a  IN  ordp100a%ROWTYPE
  ) IS
  BEGIN
    UPDATE ordp100a
       SET ROW = i_r_ordp100a
     WHERE div_part = i_r_ordp100a.div_part
       AND ordnoa = i_r_ordp100a.ordnoa;
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_excptn_sw            IN  VARCHAR2,
    i_ord_num              IN  NUMBER,
    i_stat_cd              IN  VARCHAR2,
    i_div_part             IN  NUMBER,
    i_ord_typ              IN  VARCHAR2,
    i_load_typ             IN  VARCHAR2,
    i_ord_src              IN  VARCHAR2,
    i_cust_id              IN  VARCHAR2,
    i_conf_num             IN  VARCHAR2,
    i_cust_pass_area       IN  VARCHAR2,
    i_shp_dt               IN  VARCHAR2,
    i_po_num               IN  VARCHAR2,
    i_hdr_excptn_cd        IN  VARCHAR2,
    i_maint_user_id        IN  VARCHAR2,
    i_ser_num              IN  VARCHAR2,
    i_trnsmt_dt            IN  VARCHAR2,
    i_trnsmt_tm            IN  VARCHAR2,
    i_rlse_ts              IN  VARCHAR2,
    i_legcy_ref            IN  VARCHAR2,
    i_ord_rcvd_ts          IN  VARCHAR2,
    i_allw_partl_sw        IN  VARCHAR2,
    i_load_depart_sid      IN  VARCHAR2,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
  BEGIN
    UPDATE ordp100a
       SET excptn_sw = DECODE(i_excptn_sw, '?', excptn_sw, NULL, excptn_sw, i_excptn_sw),
           stata =(CASE
                     WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_stat_cd
                     WHEN UPPER(i_stat_cd) = 'NULL' THEN NULL
                     ELSE NVL(i_stat_cd, stata)
                   END
                  ),
           dsorda =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_ord_typ
                      WHEN UPPER(i_ord_typ) = 'NULL' THEN NULL
                      ELSE NVL(i_ord_typ, dsorda)
                    END
                   ),
           ldtypa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_load_typ
                      WHEN UPPER(i_load_typ) = 'NULL' THEN NULL
                      ELSE NVL(i_load_typ, ldtypa)
                    END
                   ),
           ipdtsa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_ord_src
                      WHEN UPPER(i_ord_src) = 'NULL' THEN NULL
                      ELSE NVL(i_ord_src, ipdtsa)
                    END
                   ),
           custa =(CASE
                     WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_cust_id
                     WHEN UPPER(i_cust_id) = 'NULL' THEN NULL
                     ELSE NVL(i_cust_id, custa)
                   END
                  ),
           connba =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_conf_num
                      WHEN UPPER(i_conf_num) = 'NULL' THEN NULL
                      ELSE NVL(i_conf_num, connba)
                    END
                   ),
           cspasa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_cust_pass_area
                      WHEN UPPER(i_cust_pass_area) = 'NULL' THEN NULL
                      ELSE NVL(i_cust_pass_area, cspasa)
                    END
                   ),
           shpja =(CASE
                     WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_shp_dt
                     WHEN UPPER(i_shp_dt) = 'NULL' THEN NULL
                     ELSE NVL(i_shp_dt, shpja)
                   END
                  ),
           cpoa =(CASE
                    WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_po_num
                    WHEN UPPER(i_po_num) = 'NULL' THEN NULL
                    ELSE NVL(i_po_num, cpoa)
                  END
                 ),
           hdexpa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_hdr_excptn_cd
                      WHEN UPPER(i_hdr_excptn_cd) = 'NULL' THEN NULL
                      ELSE NVL(i_hdr_excptn_cd, hdexpa)
                    END
                   ),
           mntusa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_maint_user_id
                      WHEN UPPER(i_maint_user_id) = 'NULL' THEN NULL
                      ELSE NVL(i_maint_user_id, mntusa)
                    END
                   ),
           telsla =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_ser_num
                      WHEN UPPER(i_ser_num) = 'NULL' THEN NULL
                      ELSE NVL(i_ser_num, telsla)
                    END
                   ),
           trndta =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_trnsmt_dt
                      WHEN UPPER(i_trnsmt_dt) = 'NULL' THEN NULL
                      ELSE NVL(i_trnsmt_dt, trndta)
                    END
                   ),
           trntma =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_trnsmt_tm
                      WHEN UPPER(i_trnsmt_tm) = 'NULL' THEN NULL
                      ELSE NVL(i_trnsmt_tm, trntma)
                    END
                   ),
           uschga =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_rlse_ts
                      WHEN UPPER(i_rlse_ts) = 'NULL' THEN NULL
                      ELSE NVL(i_rlse_ts, uschga)
                    END
                   ),
           legrfa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_legcy_ref
                      WHEN UPPER(i_legcy_ref) = 'NULL' THEN NULL
                      ELSE NVL(i_legcy_ref, legrfa)
                    END
                   ),
           ord_rcvd_ts =(CASE
                           WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN TO_DATE(i_ord_rcvd_ts, 'YYYYMMDDHH24MISS')
                           WHEN UPPER(i_ord_rcvd_ts) = 'NULL' THEN NULL
                           ELSE NVL(TO_DATE(i_ord_rcvd_ts, 'YYYYMMDDHH24MISS'), ord_rcvd_ts)
                         END
                        ),
           pshipa =(CASE
                      WHEN UPPER(i_use_passed_nulls_sw) = 'Y' THEN i_allw_partl_sw
                      WHEN UPPER(i_allw_partl_sw) = 'NULL' THEN NULL
                      ELSE NVL(i_allw_partl_sw, pshipa)
                    END
                   ),
           load_depart_sid = NVL(i_load_depart_sid, load_depart_sid)
     WHERE div_part = i_div_part
       AND ordnoa = i_ord_num;
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Remove unused LoadInfo columns. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_sp(
    i_r_ord_hdr            IN  g_rt_ord_hdr,
    i_use_passed_nulls_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
  BEGIN
    upd_sp(i_r_ord_hdr.excptn_sw,
           i_r_ord_hdr.ord_num,
           i_r_ord_hdr.stat_cd,
           i_r_ord_hdr.div_part,
           i_r_ord_hdr.ord_typ,
           i_r_ord_hdr.load_typ,
           i_r_ord_hdr.ord_src,
           i_r_ord_hdr.cust_id,
           i_r_ord_hdr.conf_num,
           i_r_ord_hdr.cust_pass_area,
           i_r_ord_hdr.shp_dt,
           i_r_ord_hdr.po_num,
           i_r_ord_hdr.hdr_excptn_cd,
           i_r_ord_hdr.maint_user_id,
           i_r_ord_hdr.ser_num,
           i_r_ord_hdr.trnsmt_dt,
           i_r_ord_hdr.trnsmt_tm,
           i_r_ord_hdr.rlse_ts,
           i_r_ord_hdr.legcy_ref,
           i_r_ord_hdr.ord_rcvd_ts,
           i_r_ord_hdr.allw_partl_sw,
           i_r_ord_hdr.load_depart_sid,
           i_use_passed_nulls_sw
          );
  END upd_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/22/05 | rhalpai | Original.
  || 01/20/12 | rhalpai | Remove P_EXCPTN_SW parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_conf_num  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_ord_num  NUMBER;
  BEGIN
    IF i_conf_num IS NOT NULL THEN
      SELECT MAX(a.ordnoa)
        INTO l_ord_num
        FROM ordp100a a
       WHERE a.div_part = i_div_part
         AND a.connba = i_conf_num;
    ELSE
      l_ord_num := i_ord_num;
    END IF;   -- i_conf_num IS NOT NULL

    DELETE FROM ordp100a
          WHERE div_part = i_div_part
            AND ordnoa = l_ord_num;
  END del_sp;
END op_ord_hdr_pk;
/

