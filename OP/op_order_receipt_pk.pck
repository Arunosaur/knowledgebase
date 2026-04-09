CREATE OR REPLACE PACKAGE op_order_receipt_pk IS
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

--------------------------------------------------------------------------------
--                              PUBLIC PROCEDURES
--------------------------------------------------------------------------------
  PROCEDURE ins_ord_recpt_msg_sp(
    i_div      IN      VARCHAR2,
    i_msg_typ  IN      VARCHAR2,
    i_msg_id   IN      VARCHAR2,
    i_msg      IN      CLOB,
    o_msg_seq  OUT     NUMBER
  );

  PROCEDURE del_ord_by_legcy_ref_sp(
    i_div        IN  VARCHAR2,
    i_legcy_ref  IN  VARCHAR2 DEFAULT NULL
  );

  PROCEDURE process_ord_msg_sp(
    i_div      IN  VARCHAR2,
    i_msg_typ  IN  VARCHAR2,
    i_msg_seq  IN  NUMBER
  );

  PROCEDURE process_ord_msgs_sp(
    i_div      IN  VARCHAR2,
    i_msg_typ  IN  VARCHAR2
  );

  PROCEDURE finalize_sp(
    i_div      IN  VARCHAR2,
    i_msg_typ  IN  VARCHAR2
  );
END op_order_receipt_pk;
/

CREATE OR REPLACE PACKAGE BODY op_order_receipt_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_ORDER_RECEIPT_PK - package-level changes
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Reorganized and added logic to split orders containing
  ||                    | split order types. PIR5341/PIR5002
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------
  TYPE g_rt_msg_ord_hdr IS RECORD(
    div             div_mstr_di1d.div_id%TYPE,
    actn_cd         VARCHAR2(3),
    legcy_ref       ordp100a.legrfa%TYPE,
    load_typ        ordp100a.ldtypa%TYPE,
    ord_typ         ordp100a.dsorda%TYPE,
    trnsmt_ts       DATE,
    shp_dt          DATE,
    ord_src         ordp100a.ipdtsa%TYPE,
    cust_pass_area  ordp100a.cspasa%TYPE,
    comnt           ordp140c.commc%TYPE,
    conf_num        ordp100a.connba%TYPE,
    cust_id         ordp100a.custa%TYPE,
    stat_cd         ordp100a.stata%TYPE,
    ser_num         ordp100a.telsla%TYPE,
    no_ord_sw       CHAR(1),
    allw_partl_sw   CHAR(1),
    hdr_excptn_cd   ordp100a.hdexpa%TYPE,
    ord_num         NUMBER
  );

  TYPE g_rt_msg_ord_dtl IS RECORD(
    po_num          ordp100a.cpoa%TYPE,
    catlg_num       NUMBER,
    cbr_item        sawp505e.iteme%TYPE,
    uom             sawp505e.uome%TYPE,
    cust_item       ordp120b.cusitb%TYPE,
    ord_qty         NUMBER,
    item_pass_area  ordp120b.itpasb%TYPE,
    hard_rtl_sw     CHAR(1),
    hard_price_sw   CHAR(1),
    rtl_amt         NUMBER,
    rtl_mult        NUMBER,
    price_amt       NUMBER,
    orig_qty        NUMBER,
    byp_max_sw      CHAR(1),
    mfst_catg       NUMBER,
    tote_catg       NUMBER,
    auth_cd         ordp120b.authb%TYPE,
    not_shp_rsn     ordp120b.ntshpb%TYPE,
    price_ts        DATE,
    max_qty         NUMBER,
    qty_mult        NUMBER,
    invc_catg       NUMBER,
    lbl_catg        NUMBER,
    ord_ln          NUMBER
  );

  TYPE g_tt_msg_ord_dtls IS TABLE OF g_rt_msg_ord_dtl;

  TYPE g_rt_load_info IS RECORD(
    llr_ts    DATE,
    load_num  mclp120c.loadc%TYPE,
    stop_num  NUMBER,
    eta_ts    DATE
  );

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || LOCK_MSG_SP
  ||  Return locked record of OrderReceiptMsg for MsgSeq if available
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/17 | rhalpai | Original for SDHD-201869
  ||----------------------------------------------------------------------------
  */
  PROCEDURE lock_msg_sp(
    i_div_part  IN      NUMBER,
    i_msg_typ   IN      VARCHAR2,
    i_msg_seq   IN      NUMBER,
    o_r_msg     OUT     mclane_order_receipt_msgs%ROWTYPE
  ) IS
  BEGIN
    SELECT     *
          INTO o_r_msg
          FROM mclane_order_receipt_msgs m
         WHERE m.div_part = i_div_part
           AND m.msg_type = i_msg_typ
           AND m.msg_seq = i_msg_seq
           AND m.msg_status = 'O'
    FOR UPDATE NOWAIT;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    WHEN excp.gx_row_locked THEN
      NULL;
  END lock_msg_sp;

  /*
  ||----------------------------------------------------------------------------
  || NXT_MSG_SEQ_FN
  ||  Return MsgSeq for next locked OrderReceiptMsg if available
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/17 | rhalpai | Original for SDHD-201869
  || 02/13/18 | rhalpai | Change logic to use SKIP LOCKED option instead of NOWAIT.
  ||                    | SDHD-261014
  ||----------------------------------------------------------------------------
  */
  FUNCTION nxt_msg_seq_fn(
    i_div_part  IN  NUMBER,
    i_msg_typ   IN  VARCHAR2
  )
    RETURN NUMBER IS
    l_msg_seq  NUMBER;
    l_cv       SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT     m.msg_seq
             FROM mclane_order_receipt_msgs m
            WHERE m.div_part = i_div_part
              AND m.msg_type = i_msg_typ
              AND m.msg_status = 'O'
         ORDER BY m.msg_seq
       FOR UPDATE SKIP LOCKED;

    FETCH l_cv
     INTO l_msg_seq;

    CLOSE l_cv;

    RETURN(l_msg_seq);
  END nxt_msg_seq_fn;

  /*
  ||----------------------------------------------------------------------------
  || NXT_ORD_NUM_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION nxt_ord_num_fn
    RETURN NUMBER IS
    l_ord_num  NUMBER;
  BEGIN
    SELECT ordp100a_ordnoa_seq.NEXTVAL
      INTO l_ord_num
      FROM DUAL;

    RETURN(l_ord_num);
  END nxt_ord_num_fn;

  /*
  ||----------------------------------------------------------------------------
  || VAL_AT_POS_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  FUNCTION val_at_pos_fn(
    i_msg      IN      CLOB,
    i_msg_len  IN      PLS_INTEGER,
    i_col_len  IN      PLS_INTEGER,
    io_pos     IN OUT  PLS_INTEGER
  )
    RETURN VARCHAR2 IS
    l_len     PLS_INTEGER     := i_col_len;
    l_buffer  typ.t_maxvc2;
  BEGIN
    IF i_col_len <= i_msg_len THEN
      DBMS_LOB.READ(i_msg, l_len, io_pos, l_buffer);
    END IF;

    io_pos := io_pos + i_col_len;
    RETURN(TRIM(l_buffer));
  END val_at_pos_fn;

  /*
  ||----------------------------------------------------------------------------
  || VALID_ORD_STAT_FN
  ||  Check order header and detail statuses and indicate whether valid for
  ||  processing. This is done to ensure no order lines have been included in
  ||  an allocation release while order was being maintained and sent to
  ||  mainframe for processing.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/22/11 | rhalpai | Original for IM-007160
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  || 10/20/17 | rhalpai | Convert cursor to SELECT INTO. SDHD-201869
  ||----------------------------------------------------------------------------
  */
  FUNCTION valid_ord_stat_fn(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  )
    RETURN BOOLEAN IS
    l_cv        SYS_REFCURSOR;
    l_valid_sw  VARCHAR2(1);
  BEGIN
    SELECT 'Y'
      INTO l_valid_sw
      FROM ordp100a a
     WHERE a.div_part = i_div_part
       AND a.ordnoa = i_ord_num
       AND a.stata IN('O', 'I', 'S')
       AND NOT EXISTS(SELECT 1
                        FROM ordp120b b
                       WHERE b.div_part = i_div_part
                         AND b.ordnob = i_ord_num
                         AND b.statb NOT IN('O', 'I', 'S', 'C'));

    RETURN(TRUE);
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN(FALSE);
  END valid_ord_stat_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_TEST_ORD_SRC_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 07/10/12 | rhalpai | Remove parm P_MCL_CUST. Add parms i_div, P_CUST_ID.
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_test_ord_src_fn(
    i_div_part  IN  NUMBER,
    i_cust_id   IN  VARCHAR2,
    i_ord_src   IN  VARCHAR2
  )
    RETURN BOOLEAN IS
    l_exist_sw  VARCHAR2(1);
  BEGIN
    SELECT 'Y'
      INTO l_exist_sw
      FROM mclp020b cx, test_ord_src_crp_cd t
     WHERE cx.div_part = i_div_part
       AND cx.custb = i_cust_id
       AND t.ord_src = i_ord_src
       AND t.crp_cd = cx.corpb
       AND t.enable_sw = 'Y'
       AND ROWNUM = 1;

    RETURN(TRUE);
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN(FALSE);
  END is_test_ord_src_fn;

  /*
  ||----------------------------------------------------------------------------
  || IS_BYPASS_ORD_VALIDATION_FN
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  FUNCTION is_bypass_ord_validation_fn(
    i_div_part  IN  NUMBER,
    i_ord_src   IN  VARCHAR2
  )
    RETURN BOOLEAN IS
    l_bypass_sw  VARCHAR2(1);
  BEGIN
    SELECT 'Y'
      INTO l_bypass_sw
      FROM sub_prcs_ord_src s
     WHERE s.div_part = i_div_part
       AND s.prcs_id = 'ORDER RECEIPT'
       AND s.prcs_sbtyp_cd = 'BOV'
       AND s.ord_src = i_ord_src;

    RETURN(TRUE);
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN(FALSE);
  END is_bypass_ord_validation_fn;

  /*
  ||----------------------------------------------------------------------------
  || GET_LOAD_INFO_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_load_info_sp(
    i_div_part     IN      NUMBER,
    i_ord_num      IN      NUMBER,
    o_r_load_info  OUT     g_rt_load_info
  ) IS
    l_cv  SYS_REFCURSOR;
  BEGIN
    OPEN l_cv
     FOR
       SELECT ld.llr_ts, ld.load_num, se.stop_num, se.eta_ts
         FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se
        WHERE a.div_part = i_div_part
          AND a.ordnoa = i_ord_num
          AND ld.div_part = a.div_part
          AND ld.load_depart_sid = a.load_depart_sid
          AND ld.load_num NOT IN('DIST', 'DFLT', 'DUMY')
          AND se.div_part = a.div_part
          AND se.load_depart_sid = a.load_depart_sid
          AND se.cust_id = a.custa;

    FETCH l_cv
     INTO o_r_load_info;

    CLOSE l_cv;
  END get_load_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_HDR_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 07/10/12 | rhalpai | Remove reference to column RESCDA.
  ||                    | Change logic to use new package MsgOrdHdr record.
  ||                    | Remove parms P_ORD_COMMENTS, P_ACTN_CD.
  || 09/24/13 | rhalpai | Change logic to use a space for NULL order source.
  ||                    | IM-120024
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_hdr_sp(
    i_msg    IN      CLOB,
    io_pos   IN OUT  PLS_INTEGER,
    o_r_hdr  OUT     g_rt_msg_ord_hdr
  ) IS
    l_msg_len  PLS_INTEGER := 0;
  BEGIN
    l_msg_len := DBMS_LOB.getlength(i_msg);
    o_r_hdr.div := val_at_pos_fn(i_msg, l_msg_len, 2, io_pos);
    io_pos := io_pos + 8;   -- msg_typ
    io_pos := io_pos + 30;   -- appl_msg_id
    o_r_hdr.actn_cd := val_at_pos_fn(i_msg, l_msg_len, 3, io_pos);
    io_pos := io_pos + 10;   -- filler1
    io_pos := io_pos + 2;   -- hdr_typ
    o_r_hdr.legcy_ref := val_at_pos_fn(i_msg, l_msg_len, 25, io_pos);
    o_r_hdr.load_typ := val_at_pos_fn(i_msg, l_msg_len, 3, io_pos);
    o_r_hdr.ord_typ :=(CASE val_at_pos_fn(i_msg, l_msg_len, 3, io_pos)
                         WHEN 'DIS' THEN 'D'
                         WHEN 'TST' THEN 'T'
                         WHEN 'NOO' THEN 'N'
                         ELSE 'R'
                       END
                      );
    o_r_hdr.trnsmt_ts := TO_DATE(val_at_pos_fn(i_msg, l_msg_len, 14, io_pos),
                                 'YYYYMMDDHH24MISS'
                                );
    o_r_hdr.shp_dt := TO_DATE(val_at_pos_fn(i_msg, l_msg_len, 8, io_pos),
                              'YYYYMMDD'
                             );
    io_pos := io_pos + 8;   -- dflt pricing date
    io_pos := io_pos + 1;   -- reserve code
    o_r_hdr.ord_src := NVL(val_at_pos_fn(i_msg, l_msg_len, 8, io_pos), ' ');
    o_r_hdr.cust_pass_area := val_at_pos_fn(i_msg, l_msg_len, 25, io_pos);
    o_r_hdr.comnt := val_at_pos_fn(i_msg, l_msg_len, 25, io_pos);
    o_r_hdr.conf_num := val_at_pos_fn(i_msg, l_msg_len, 8, io_pos);
    io_pos := io_pos + 6;   -- mcl_cust
    o_r_hdr.cust_id := val_at_pos_fn(i_msg, l_msg_len, 8, io_pos);
    o_r_hdr.stat_cd := val_at_pos_fn(i_msg, l_msg_len, 3, io_pos);
    o_r_hdr.ser_num := val_at_pos_fn(i_msg, l_msg_len, 20, io_pos);
    o_r_hdr.no_ord_sw := val_at_pos_fn(i_msg, l_msg_len, 1, io_pos);

    IF o_r_hdr.no_ord_sw = 'Y' THEN
      o_r_hdr.ord_typ := 'N';
    END IF;   -- p_ord_hdr_rec.no_ord_sw = 'Y'

    o_r_hdr.allw_partl_sw := val_at_pos_fn(i_msg, l_msg_len, 1, io_pos);
    o_r_hdr.hdr_excptn_cd := val_at_pos_fn(i_msg, l_msg_len, 3, io_pos);
    io_pos := io_pos + 7;   -- ord_ln_cnt
    o_r_hdr.ord_num := TO_NUMBER(val_at_pos_fn(i_msg, l_msg_len, 11, io_pos));
  END parse_hdr_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_DTL_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 11/10/10 | rhalpai | Remove unused columns. Change logic to use weight and
  ||                    | cube from CorpItem table SAWP505E instead of from
  ||                    | OrderDetail table. PIR5878
  || 07/10/12 | rhalpai | Remove references to columns RSTFEB, INVNOB, RESGPB,
  ||                    | DTEXPB, RETGPB.
  ||                    | Change logic to use new package MsgOrdDtl record.
  ||                    | Remove parms P_ORD_HDR_REC, P_LOAD_INFO_REC.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_dtl_sp(
    i_msg      IN      CLOB,
    i_msg_len  IN      PLS_INTEGER,
    io_pos     IN OUT  PLS_INTEGER,
    o_r_dtl    OUT     g_rt_msg_ord_dtl
  ) IS
  BEGIN
    io_pos := io_pos + 2;
    o_r_dtl.po_num := val_at_pos_fn(i_msg, i_msg_len, 30, io_pos);
    o_r_dtl.catlg_num := val_at_pos_fn(i_msg, i_msg_len, 6, io_pos);
    o_r_dtl.cbr_item := val_at_pos_fn(i_msg, i_msg_len, 9, io_pos);
    o_r_dtl.uom := val_at_pos_fn(i_msg, i_msg_len, 3, io_pos);
    o_r_dtl.cust_item := val_at_pos_fn(i_msg, i_msg_len, 10, io_pos);
    o_r_dtl.ord_qty := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos));
    o_r_dtl.item_pass_area := val_at_pos_fn(i_msg, i_msg_len, 20, io_pos);
    o_r_dtl.hard_rtl_sw := val_at_pos_fn(i_msg, i_msg_len, 1, io_pos);
    o_r_dtl.hard_price_sw := val_at_pos_fn(i_msg, i_msg_len, 1, io_pos);
    o_r_dtl.rtl_amt := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos))
                       * .01;
    o_r_dtl.rtl_mult := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 5, io_pos));
    o_r_dtl.price_amt := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos))
                         * .01;
    o_r_dtl.orig_qty := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos));
    o_r_dtl.byp_max_sw := val_at_pos_fn(i_msg, i_msg_len, 1, io_pos);
    io_pos := io_pos + 1;   -- restk_fee_sw
    o_r_dtl.mfst_catg := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 3, io_pos));
    o_r_dtl.tote_catg := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 3, io_pos));
    o_r_dtl.auth_cd := val_at_pos_fn(i_msg, i_msg_len, 1, io_pos);
    o_r_dtl.not_shp_rsn := val_at_pos_fn(i_msg, i_msg_len, 8, io_pos);
    o_r_dtl.price_ts := TO_DATE(val_at_pos_fn(i_msg, i_msg_len, 14, io_pos),
                                'YYYYMMDDHH24MISS'
                               );
    io_pos := io_pos + 10;   -- invc_num
    io_pos := io_pos + 8;   -- rsrv_grp
    o_r_dtl.max_qty := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos));
    io_pos := io_pos + 3;   -- dtl_excptn_cd
    o_r_dtl.qty_mult := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos));
    o_r_dtl.invc_catg := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 3, io_pos));
    o_r_dtl.lbl_catg := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 3, io_pos));
    io_pos := io_pos + 8;   -- rtl_grp
    o_r_dtl.ord_ln := TO_NUMBER(val_at_pos_fn(i_msg, i_msg_len, 9, io_pos))
                      * .01;
  END parse_dtl_sp;

  /*
  ||----------------------------------------------------------------------------
  || PARSE_DTLS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 07/10/12 | rhalpai | Remove parms P_ORD_HDR_REC, P_LOAD_INFO_REC.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE parse_dtls_sp(
    i_msg     IN      CLOB,
    io_pos    IN OUT  PLS_INTEGER,
    o_t_dtls  OUT     g_tt_msg_ord_dtls
  ) IS
    l_msg_len  PLS_INTEGER      := 0;
    l_r_dtl    g_rt_msg_ord_dtl;
  BEGIN
    o_t_dtls := g_tt_msg_ord_dtls();
    l_msg_len := DBMS_LOB.getlength(i_msg);
    WHILE io_pos <= l_msg_len LOOP
      parse_dtl_sp(i_msg, l_msg_len, io_pos, l_r_dtl);
      o_t_dtls.EXTEND;
      o_t_dtls(o_t_dtls.LAST) := l_r_dtl;
    END LOOP;
  END parse_dtls_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_ORD_DTL_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/10/12 | rhalpai | Original
  || 08/31/12 | rhalpai | Change to handle invalid item numbers.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change logic to remove LoadInfo from OrdDtl. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_ord_dtl_sp(
    i_r_ord_hdr  IN  g_rt_msg_ord_hdr,
    i_r_ord_dtl  IN  g_rt_msg_ord_dtl
  ) IS
  BEGIN
    MERGE INTO ordp120b b
         USING (SELECT d.div_part, e.iteme, e.uome
                  FROM div_mstr_di1d d LEFT JOIN sawp505e e ON e.catite = LPAD(i_r_ord_dtl.catlg_num, 6, '0')
                 WHERE d.div_id = i_r_ord_hdr.div) x
            ON (    b.div_part = x.div_part
                AND b.ordnob = i_r_ord_hdr.ord_num
                AND b.lineb = i_r_ord_dtl.ord_ln)
      WHEN MATCHED THEN
        UPDATE
           SET b.excptn_sw = 'N', b.statb = 'O', b.subrcb = 0, b.orditb = LPAD(i_r_ord_dtl.catlg_num, 6, '0'),
               b.itemnb = x.iteme, b.sllumb = x.uome, b.orgitb = LPAD(i_r_ord_dtl.catlg_num, 6, '0'),
               b.qtmulb = i_r_ord_dtl.qty_mult, b.ordqtb = i_r_ord_dtl.ord_qty, b.orgqtb = i_r_ord_dtl.orig_qty,
               b.itpasb = i_r_ord_dtl.item_pass_area, b.prfixb = DECODE(i_r_ord_dtl.hard_price_sw, 'Y', 1, 0),
               b.hdprcb = i_r_ord_dtl.price_amt, b.rtfixb = DECODE(i_r_ord_dtl.hard_rtl_sw, 'Y', 1, 0),
               b.hdrtab = i_r_ord_dtl.rtl_amt, b.hdrtmb = i_r_ord_dtl.rtl_mult,
               b.manctb = LPAD(i_r_ord_dtl.mfst_catg, 3, '0'),
               b.totctb =(CASE
                            WHEN i_r_ord_dtl.tote_catg > 0 THEN LPAD(i_r_ord_dtl.tote_catg, 3, '0')
                          END), b.labctb = i_r_ord_dtl.lbl_catg, b.invctb = i_r_ord_dtl.invc_catg,
               b.cusitb = i_r_ord_dtl.cust_item, b.prstdb = TRUNC(i_r_ord_dtl.price_ts) - DATE '1900-02-28',
               b.prsttb = TO_NUMBER(TO_CHAR(i_r_ord_dtl.price_ts, 'HH24MISS')), b.ntshpb = i_r_ord_dtl.not_shp_rsn,
               b.authb = i_r_ord_dtl.auth_cd, b.maxqtb = i_r_ord_dtl.max_qty,
               b.bymaxb = DECODE(i_r_ord_dtl.byp_max_sw, 'Y', 1, 0), b.actqtb = i_r_ord_dtl.ord_qty
      WHEN NOT MATCHED THEN
        INSERT(div_part, ordnob, lineb, excptn_sw, statb, subrcb, orditb, itemnb, sllumb, orgitb, qtmulb, ordqtb,
               orgqtb, itpasb, prfixb, hdprcb, rtfixb, hdrtab, hdrtmb, manctb, totctb, labctb, invctb, cusitb, prstdb,
               prsttb, ntshpb, authb, maxqtb, bymaxb, actqtb)
        VALUES(x.div_part, i_r_ord_hdr.ord_num, i_r_ord_dtl.ord_ln, 'N', 'O', 0, LPAD(i_r_ord_dtl.catlg_num, 6, '0'),
               x.iteme, x.uome, LPAD(i_r_ord_dtl.catlg_num, 6, '0'), i_r_ord_dtl.qty_mult, i_r_ord_dtl.ord_qty,
               i_r_ord_dtl.orig_qty, i_r_ord_dtl.item_pass_area, DECODE(i_r_ord_dtl.hard_price_sw, 'Y', 1, 0),
               i_r_ord_dtl.price_amt, DECODE(i_r_ord_dtl.hard_rtl_sw, 'Y', 1, 0), i_r_ord_dtl.rtl_amt,
               i_r_ord_dtl.rtl_mult, LPAD(i_r_ord_dtl.mfst_catg, 3, '0'),
               (CASE
                  WHEN i_r_ord_dtl.tote_catg > 0 THEN LPAD(i_r_ord_dtl.tote_catg, 3, '0')
                END), i_r_ord_dtl.lbl_catg, i_r_ord_dtl.invc_catg, i_r_ord_dtl.cust_item,
               TRUNC(i_r_ord_dtl.price_ts) - DATE '1900-02-28', TO_NUMBER(TO_CHAR(i_r_ord_dtl.price_ts, 'HH24MISS')),
               i_r_ord_dtl.not_shp_rsn, i_r_ord_dtl.auth_cd, i_r_ord_dtl.max_qty,
               DECODE(i_r_ord_dtl.byp_max_sw, 'Y', 1, 0), i_r_ord_dtl.ord_qty);
  END merge_ord_dtl_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_ORD_HDR_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/10/12 | rhalpai | Original
  || 11/29/12 | rhalpai | Add logic to set LoadDepartSid and update StopEta.
  || 01/11/13 | rhalpai | Add logic to flag order as test. IM-077739
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to ruse OrdTyp to indicate NoOrdSw and remove
  ||                    | LoadInfo other than LoadDepartSid. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_ord_hdr_sp(
    i_r_ord_hdr    IN  g_rt_msg_ord_hdr,
    i_r_load_info  IN  g_rt_load_info,
    i_po_num       IN  VARCHAR2,
    i_ord_rcvd_ts  IN  DATE
  ) IS
    l_div_part         NUMBER;
    l_load_depart_sid  NUMBER;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_r_ord_hdr.div);
    l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(l_div_part, i_r_load_info.llr_ts, i_r_load_info.load_num);
    op_order_load_pk.merge_stop_eta_sp(l_div_part,
                                       l_load_depart_sid,
                                       i_r_ord_hdr.cust_id,
                                       i_r_load_info.eta_ts,
                                       i_r_load_info.stop_num
                                      );
    MERGE INTO ordp100a a
         USING (SELECT 1 AS tst
                  FROM DUAL) x
            ON (    a.div_part = l_div_part
                AND a.ordnoa = i_r_ord_hdr.ord_num
                AND x.tst > 0)
      WHEN MATCHED THEN
        UPDATE   -- no updates for ord_typ, ord_rcvd_ts
           SET a.excptn_sw = 'N', a.custa = i_r_ord_hdr.cust_id, a.ldtypa = i_r_ord_hdr.load_typ, a.cpoa = i_po_num,
               a.stata = 'O', a.ipdtsa = i_r_ord_hdr.ord_src, a.connba = i_r_ord_hdr.conf_num,
               a.telsla = i_r_ord_hdr.ser_num, a.trndta = TRUNC(i_r_ord_hdr.trnsmt_ts) - DATE '1900-02-28',
               a.trntma = TO_NUMBER(TO_CHAR(i_r_ord_hdr.trnsmt_ts, 'HH24MISS')), a.cspasa = i_r_ord_hdr.cust_pass_area,
               a.hdexpa = i_r_ord_hdr.hdr_excptn_cd, a.legrfa = i_r_ord_hdr.legcy_ref,
               a.shpja = TRUNC(i_r_ord_hdr.shp_dt) - DATE '1900-02-28', a.dsorda = i_r_ord_hdr.ord_typ,
               a.pshipa = DECODE(i_r_ord_hdr.allw_partl_sw, 'Y', 1, 0), load_depart_sid = l_load_depart_sid
      WHEN NOT MATCHED THEN
        INSERT(ordnoa, div_part, excptn_sw, custa, ldtypa, cpoa, stata, dsorda, ipdtsa, connba, telsla, trndta, trntma,
               ord_rcvd_ts, cspasa, hdexpa, legrfa, shpja, pshipa, load_depart_sid)
        VALUES(i_r_ord_hdr.ord_num, l_div_part, 'N', i_r_ord_hdr.cust_id, i_r_ord_hdr.load_typ, i_po_num, 'O',
               i_r_ord_hdr.ord_typ, i_r_ord_hdr.ord_src, i_r_ord_hdr.conf_num, i_r_ord_hdr.ser_num,
               TRUNC(i_r_ord_hdr.trnsmt_ts) - DATE '1900-02-28', TO_NUMBER(TO_CHAR(i_r_ord_hdr.trnsmt_ts, 'HH24MISS')),
               i_ord_rcvd_ts, i_r_ord_hdr.cust_pass_area, i_r_ord_hdr.hdr_excptn_cd, i_r_ord_hdr.legcy_ref,
               TRUNC(i_r_ord_hdr.shp_dt) - DATE '1900-02-28', DECODE(i_r_ord_hdr.allw_partl_sw, 'Y', 1, 0),
               l_load_depart_sid);
  END merge_ord_hdr_sp;

  /*
  ||----------------------------------------------------------------------------
  || MERGE_ORD_COMNT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/10/12 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE merge_ord_comnt_sp(
    i_r_ord_hdr  IN  g_rt_msg_ord_hdr
  ) IS
  BEGIN
    MERGE INTO ordp140c c
         USING (SELECT d.div_part
                  FROM div_mstr_di1d d
                 WHERE d.div_id = i_r_ord_hdr.div) x
            ON (    c.div_part = x.div_part
                AND c.ordnoc = i_r_ord_hdr.ord_num)
      WHEN MATCHED THEN
        UPDATE
           SET c.commc = i_r_ord_hdr.comnt
      WHEN NOT MATCHED THEN
        INSERT(div_part, ordnoc, commc, seqc)
        VALUES(x.div_part, i_r_ord_hdr.ord_num, i_r_ord_hdr.comnt, 0);
  END merge_ord_comnt_sp;

  /*
  ||----------------------------------------------------------------------------
  || INS_MCLPINPR_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 07/10/12 | rhalpai | Change logic to use new package MsgOrdHdr record.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_mclpinpr_sp(
    i_msg_id     IN  VARCHAR2,
    i_r_ord_hdr  IN  g_rt_msg_ord_hdr
  ) IS
  BEGIN
    INSERT INTO mclpinpr
                (div_part, ordnor, msgidr, customer_id, legrfr,
                 order_receipt_status, create_ts)
      SELECT d.div_part, i_r_ord_hdr.ord_num, i_msg_id, i_r_ord_hdr.cust_id,
             i_r_ord_hdr.legcy_ref, 0, SYSDATE
        FROM div_mstr_di1d d
       WHERE d.div_id = i_r_ord_hdr.div;
  END ins_mclpinpr_sp;

  /*
  ||----------------------------------------------------------------------------
  || COPY_ORD_HDR_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 05/09/12 | dlbeal  | Added load depart sid
  || 11/29/12 | rhalpai | Removed logic to set load_depart_sid to 0.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE copy_ord_hdr_sp(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_new_ord_num  IN  NUMBER,
    i_new_po       IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_r_ordp100a  ordp100a%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_r_ordp100a
      FROM ordp100a a
     WHERE a.div_part = i_div_part
       AND a.ordnoa = i_ord_num;

    l_r_ordp100a.ordnoa := i_new_ord_num;
    l_r_ordp100a.ord_rcvd_ts := SYSDATE;

    IF i_new_po IS NOT NULL THEN
      l_r_ordp100a.cpoa := i_new_po;
    END IF;   -- i_new_po IS NOT NULL

    INSERT INTO ordp100a
         VALUES l_r_ordp100a;
  END copy_ord_hdr_sp;

  /*
  ||----------------------------------------------------------------------------
  || COPY_ORD_COMMENT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE copy_ord_comment_sp(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_new_ord_num  IN  NUMBER
  ) IS
    l_r_ordp140c  ordp140c%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_r_ordp140c
      FROM ordp140c c
     WHERE c.div_part = i_div_part
       AND c.ordnoc = i_ord_num;

    l_r_ordp140c.ordnoc := i_new_ord_num;

    INSERT INTO ordp140c
         VALUES l_r_ordp140c;
  END copy_ord_comment_sp;

  /*
  ||----------------------------------------------------------------------------
  || COPY_ORD_LN_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE copy_ord_ln_sp(
    i_div_part     IN  NUMBER,
    i_ord_num      IN  NUMBER,
    i_ord_ln       IN  NUMBER,
    i_new_ord_num  IN  NUMBER,
    i_new_ord_ln   IN  NUMBER
  ) IS
    l_r_ordp120b  ordp120b%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_r_ordp120b
      FROM ordp120b b
     WHERE b.div_part = i_div_part
       AND b.ordnob = i_ord_num
       AND b.lineb = i_ord_ln;

    l_r_ordp120b.ordnob := i_new_ord_num;
    l_r_ordp120b.lineb := i_new_ord_ln;

    INSERT INTO ordp120b
         VALUES l_r_ordp120b;
  END copy_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || CANCEL_ORIG_ORD_LN_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE cancel_orig_ord_ln_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_ord_ln    IN  NUMBER,
    i_rsn_txt   IN  VARCHAR2
  ) IS
    l_r_sysp296a  sysp296a%ROWTYPE;
  BEGIN
    UPDATE ordp120b b
       SET b.statb = 'C'
     WHERE b.div_part = i_div_part
       AND b.ordnob = i_ord_num
       AND b.lineb = i_ord_ln;

    l_r_sysp296a.div_part := i_div_part;
    l_r_sysp296a.ordnoa := i_ord_num;
    l_r_sysp296a.linea := i_ord_ln;
    l_r_sysp296a.usera := 'ORDRCPT';
    l_r_sysp296a.tblnma := 'ORDP120B';
    l_r_sysp296a.fldnma := 'STATB';
    l_r_sysp296a.flchga := 'C';
    l_r_sysp296a.actna := 'M';
    l_r_sysp296a.rsncda := 'RCANC5';
    l_r_sysp296a.autbya := 'ORDRCPT';
    l_r_sysp296a.rsntxa := i_rsn_txt;
    op_sysp296a_pk.ins_sp(l_r_sysp296a);
  END cancel_orig_ord_ln_sp;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_ORD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 12/30/08 | rhalpai | Changed logic set order header status to cancel when
  ||                    | all details are in cancel status. IM468705
  || 01/26/11 | rhalpai | Add logic to include cust_id in call to
  ||                    | NXT_PROD_RCPT_TS_FN. IM-004248
  || 07/10/12 | rhalpai | Add logic to Get Next Order Line for Existing Order.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||                    | Add DivPart in call to OP_SPLIT_ORD_PK.EXISTING_SPLIT_ORD_FN,
  ||                    | COPY_ORD_COMMENT_SP, COPY_ORD_LN_SP, CANCEL_ORIG_ORD_LN_SP.
  ||                    | PIR15697
  ||----------------------------------------------------------------------------
  */
  PROCEDURE split_ord_sp(
    i_div_part         IN  NUMBER,
    i_ord_num          IN  NUMBER,
    i_split_typ        IN  VARCHAR2,
    i_new_po_on_split  IN  VARCHAR2,
    i_t_split_ord_lns  IN  type_stab,
    i_cbr_vndr_id      IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module      CONSTANT typ.t_maxfqnm               := 'OP_ORDER_RECEIPT_PK.SPLIT_ORD_SP';
    lar_parm                 logs.tar_parm;
    l_div_id                 div_mstr_di1d.div_id%TYPE;
    l_new_po                 ordp100a.cpoa%TYPE;
    l_new_ord_num            PLS_INTEGER;
    l_is_existing_ord        BOOLEAN                     := FALSE;
    l_msg_id                 mclpinpr.msgidr%TYPE;
    l_r_ord_hdr              g_rt_msg_ord_hdr;
    l_new_ord_ln             NUMBER;
    l_c_strict_ord  CONSTANT VARCHAR2(10)                := 'STRICT ORD';
    l_cust_id                sysp200c.acnoc%TYPE;
    l_c_sysdate     CONSTANT DATE                        := SYSDATE;
    l_prod_rcpt_ts           DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.add_parm(lar_parm, 'NewPoOnSplit', i_new_po_on_split);
    logs.add_parm(lar_parm, 'SplitOrdLns', i_t_split_ord_lns);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_id := div_pk.div_id_fn(i_div_part);

    IF (    i_t_split_ord_lns IS NOT NULL
        AND i_t_split_ord_lns.COUNT > 0) THEN
      logs.dbg('Get Existing Order');
      l_new_ord_num := op_split_ord_pk.existing_split_ord_fn(i_div_part, i_ord_num, i_split_typ, i_cbr_vndr_id);
      l_is_existing_ord :=(l_new_ord_num IS NOT NULL);

      IF NOT l_is_existing_ord THEN
        l_new_po :=(CASE i_new_po_on_split
                      WHEN 'Y' THEN i_split_typ
                    END);
        logs.dbg('Get Next Order Number');
        l_new_ord_num := nxt_ord_num_fn;
        logs.dbg('Copy Order Header');
        -- Override PO when required
        copy_ord_hdr_sp(i_div_part, i_ord_num, l_new_ord_num, l_new_po);
        logs.dbg('Copy Order Comment');
        copy_ord_comment_sp(i_div_part, i_ord_num, l_new_ord_num);
      END IF;   -- NOT l_is_existing_ord

      logs.dbg('Add to MCLPINPR to be included in PRCS_ORDS_SP');
      l_msg_id := l_new_ord_num;
      l_r_ord_hdr.ord_num := l_new_ord_num;
      l_r_ord_hdr.div := l_div_id;
      ins_mclpinpr_sp(l_msg_id, l_r_ord_hdr);

      IF i_split_typ = l_c_strict_ord THEN
        logs.dbg('Get CustId');

        SELECT a.custa
          INTO l_cust_id
          FROM ordp100a a
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num;

        logs.dbg('Get Product Receipt Timestamp');
        l_prod_rcpt_ts := op_strict_order_pk.nxt_prod_rcpt_ts_fn(l_div_id, l_cust_id, i_cbr_vndr_id, l_c_sysdate);
      END IF;   -- i_split_typ = l_c_strict_ord

      l_new_ord_ln := 0;
      <<ord_ln_loop>>
      FOR i IN i_t_split_ord_lns.FIRST .. i_t_split_ord_lns.LAST LOOP
        IF l_is_existing_ord THEN
          logs.dbg('Get Next Order Line for Existing Order');

          SELECT COUNT(*) + 1
            INTO l_new_ord_ln
            FROM ordp120b b
           WHERE b.div_part = i_div_part
             AND b.ordnob = l_new_ord_num
             AND b.lineb = FLOOR(b.lineb);
        ELSE
          l_new_ord_ln := l_new_ord_ln + 1;
        END IF;   -- l_is_existing_ord

        logs.dbg('Add Order Line');
        copy_ord_ln_sp(i_div_part, i_ord_num, i_t_split_ord_lns(i), l_new_ord_num, l_new_ord_ln);
        logs.dbg('Cancel Original Order Line');
        cancel_orig_ord_ln_sp(i_div_part, i_ord_num, i_t_split_ord_lns(i), 'SPLIT FOR ' || i_split_typ);
        logs.dbg('Add Split Order Line');

        INSERT INTO split_ord_op2s
                    (div_part, split_typ, ord_num, ord_ln, org_ord_num, org_ord_ln
                    )
             VALUES (i_div_part, i_split_typ, l_new_ord_num, l_new_ord_ln, i_ord_num, i_t_split_ord_lns(i)
                    );

        IF i_split_typ = l_c_strict_ord THEN
          logs.dbg('Add Strict Order Line');

          INSERT INTO strct_ord_op1o
                      (div_part, cbr_vndr_id, ord_num, ord_ln, prod_rcpt_ts
                      )
               VALUES (i_div_part, i_cbr_vndr_id, l_new_ord_num, l_new_ord_ln, l_prod_rcpt_ts
                      );
        END IF;   -- i_split_typ = l_c_strict_ord
      END LOOP ord_ln_loop;
      logs.dbg('Cancel Ord Hdr Status When All Details in Cancel Status');

      UPDATE ordp100a a
         SET a.stata = 'C'
       WHERE a.div_part = i_div_part
         AND a.ordnoa = i_ord_num
         AND NOT EXISTS(SELECT 1
                          FROM ordp120b b
                         WHERE b.div_part = i_div_part
                           AND b.ordnob = i_ord_num
                           AND b.statb <> 'C');
    END IF;   -- i_t_split_ord_lns IS NOT NULL AND i_t_split_ord_lns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_TYP_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 11/06/08 | rhalpai | Added logic to include RECAP_TS and new STAT column
  ||                    | when adding rows to STRCT_ORD_OP1O. Will use the
  ||                    | system date for RECAP_TS and XCP (exception) for STAT
  ||                    | when initial not-ship-reason is found and date of
  ||                    | 29990101 as RECAP_TS and URC (unrecapped) as STAT
  ||                    | when not found. PIR5002
  || 04/20/09 | rhalpai | Added logic to assign XCP status when adding
  ||                    | STRCT_ORD_OP1O entry for mixed Strict Order with PO
  ||                    | that cannot be split. PIR6758
  || 08/17/09 | rhalpai | Changed logic to exclude order lines with a
  ||                    | not-ship-reason code and include only order lines for
  ||                    | divisional items (on MLCP110B in ACT or DIS status)
  ||                    | when looking for mixed (Strict and Non-Strict) items.
  ||                    | PIR6758
  || 01/26/11 | rhalpai | Add logic to include cust_id in call to
  ||                    | NXT_PROD_RCPT_TS_FN. IM-004248
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||                    | Add AllowMixSw input parm and logic to bypass
  ||                    | exceptions in StrictOrd status and Order NotShipRsn
  ||                    | when order is mixed. Add DivPart in call to
  ||                    | OP_SPLIT_ORD_PK.SPLIT_ORDLN_FN. PIR15697
  ||----------------------------------------------------------------------------
  */
  PROCEDURE split_typ_sp(
    i_div_part            IN  NUMBER,
    i_ord_num             IN  NUMBER,
    i_split_typ           IN  VARCHAR2,
    i_allw_split_sw       IN  VARCHAR2,
    i_new_po_on_split_sw  IN  VARCHAR2,
    i_allw_mixed_sw       IN  VARCHAR2,
    i_cbr_vndr_id         IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module      CONSTANT typ.t_maxfqnm               := 'OP_ORDER_RECEIPT_PK.SPLIT_TYP_SP';
    lar_parm                 logs.tar_parm;
    l_div_id                 div_mstr_di1d.div_id%TYPE;
    l_t_split_ordlns         type_stab                   := type_stab();
    l_c_sysdate     CONSTANT DATE                        := SYSDATE;
    l_c_strict_ord  CONSTANT VARCHAR2(10)                := 'STRICT ORD';
    l_cust_id                sysp200c.acnoc%TYPE;
    l_prod_rcpt_ts           DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'SplitTyp', i_split_typ);
    logs.add_parm(lar_parm, 'AllwSplitSw', i_allw_split_sw);
    logs.add_parm(lar_parm, 'NewPoOnSplitSw', i_new_po_on_split_sw);
    logs.add_parm(lar_parm, 'AllwMixedSw', i_allw_mixed_sw);
    logs.add_parm(lar_parm, 'CbrVndrId', i_cbr_vndr_id);
    logs.dbg('ENTRY', lar_parm);
    l_div_id := div_pk.div_id_fn(i_div_part);
    logs.dbg('Get Order Lines for Split Type');
    l_t_split_ordlns := op_split_ord_pk.split_ordln_fn(i_div_part, i_ord_num, i_split_typ, i_cbr_vndr_id);

    IF l_t_split_ordlns.COUNT > 0 THEN
      IF i_allw_split_sw = 'Y' THEN
        logs.dbg('Process Split');
        split_ord_sp(i_div_part, i_ord_num, i_split_typ, i_new_po_on_split_sw, l_t_split_ordlns, i_cbr_vndr_id);
      ELSE
        logs.dbg('Add Entries for Order with PO that Cannot Be Split');
        FORALL i IN l_t_split_ordlns.FIRST .. l_t_split_ordlns.LAST
          INSERT INTO split_ord_op2s
                      (div_part, split_typ, ord_num, ord_ln, org_ord_num, org_ord_ln
                      )
               VALUES (i_div_part, i_split_typ, i_ord_num, l_t_split_ordlns(i), i_ord_num, l_t_split_ordlns(i)
                      );

        IF i_split_typ = l_c_strict_ord THEN
          logs.dbg('Get CustId');

          SELECT a.custa
            INTO l_cust_id
            FROM ordp100a a
           WHERE a.div_part = i_div_part
             AND a.ordnoa = i_ord_num;

          logs.dbg('Get Product Receipt Timestamp');
          l_prod_rcpt_ts := op_strict_order_pk.nxt_prod_rcpt_ts_fn(l_div_id, l_cust_id, i_cbr_vndr_id, l_c_sysdate);
          logs.dbg('Add Entries for Strict Orders with PO that Cannot Be Split');
          FORALL i IN l_t_split_ordlns.FIRST .. l_t_split_ordlns.LAST
            INSERT INTO strct_ord_op1o
                        (div_part, cbr_vndr_id, ord_num, ord_ln, prod_rcpt_ts, stat, recap_ts)
              SELECT b.div_part, i_cbr_vndr_id, i_ord_num, l_t_split_ordlns(i), l_prod_rcpt_ts,
                     (CASE
                        WHEN(    b.ntshpb IS NULL
                             AND (   i_allw_mixed_sw = 'Y'
                                  OR x.is_mixed_sw = 'N')) THEN 'URC'
                        ELSE 'XCP'
                      END),
                     (CASE
                        WHEN(    b.ntshpb IS NULL
                             AND (   i_allw_mixed_sw = 'Y'
                                  OR x.is_mixed_sw = 'N')) THEN DATE '2999-01-01'
                        ELSE l_c_sysdate
                      END
                     )
                FROM ordp120b b,
                     (SELECT NVL(MAX('Y'), 'N') AS is_mixed_sw
                        FROM ordp100a a
                       WHERE a.div_part = i_div_part
                         AND a.ordnoa = i_ord_num
                         AND a.stata = 'O'
                         AND a.dsorda = 'R'
                         AND EXISTS(SELECT 1
                                      FROM ordp120b b, mclp110b di
                                     WHERE b.div_part = di.div_part
                                       AND b.ordnob = i_ord_num
                                       AND b.statb IN('O', 'I')
                                       AND b.ntshpb IS NULL
                                       AND NOT EXISTS(SELECT 1
                                                        FROM split_div_vnd_op3s s, strct_item_op3v si
                                                       WHERE s.split_typ = 'STRICT ORD'
                                                         AND s.div_part = b.div_part
                                                         AND si.div_part = s.div_part
                                                         AND si.cbr_vndr_id = s.cbr_vndr_id
                                                         AND si.item_num = b.itemnb
                                                         AND si.uom = b.sllumb)
                                       AND di.div_part = i_div_part
                                       AND di.itemb = b.itemnb
                                       AND di.uomb = b.sllumb
                                       AND di.statb IN('ACT', 'DIS'))) x
               WHERE b.div_part = i_div_part
                 AND b.ordnob = i_ord_num
                 AND b.lineb = l_t_split_ordlns(i);

          IF i_allw_mixed_sw = 'N' THEN
            logs.dbg('Set Exception for Strict Orders with PO that Cannot Be Split');

            UPDATE ordp120b b
               SET b.ntshpb = 'STRCTERR'
             WHERE b.div_part = i_div_part
               AND b.ntshpb IS NULL
               AND b.statb IN('O', 'I')
               AND b.ordnob = i_ord_num
               AND b.lineb IN(SELECT so.ord_ln
                                FROM strct_ord_op1o so
                               WHERE so.div_part = i_div_part
                                 AND so.ord_num = i_ord_num
                                 AND so.stat = 'XCP'
                                 AND EXISTS(SELECT 1
                                              FROM ordp120b b2, mclp110b di
                                             WHERE b2.div_part = i_div_part
                                               AND b2.ordnob = i_ord_num
                                               AND b2.statb IN('O', 'I')
                                               AND b2.ntshpb IS NULL
                                               AND NOT EXISTS(
                                                     SELECT 1
                                                       FROM split_div_vnd_op3s s, strct_item_op3v si
                                                      WHERE s.split_typ = 'STRICT ORD'
                                                        AND s.div_part = b2.div_part
                                                        AND si.div_part = s.div_part
                                                        AND si.cbr_vndr_id = s.cbr_vndr_id
                                                        AND si.item_num = b2.itemnb
                                                        AND si.uom = b2.sllumb)
                                               AND di.div_part = b.div_part
                                               AND di.itemb = b.itemnb
                                               AND di.uomb = b.sllumb
                                               AND di.statb IN('ACT', 'DIS')));
          END IF;   -- i_allw_mixed_sw = 'N'
        END IF;   -- i_split_typ = l_c_strict_ord
      END IF;   -- i_allw_split_sw = 'Y'
    END IF;   -- l_t_split_ordlns.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_typ_sp;

  /*
  ||----------------------------------------------------------------------------
  || SPLIT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 11/16/07 | rhalpai | Original
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 12/08/15 | rhalpai | Replace Div input parm with DivPart input parm.
  ||                    | Change logic to include new AllowMixSw parm in call to
  ||                    | OP_SPLIT_ORD_PK.PO_SPLIT_INFO_SP and pass it in call
  ||                    | to SPLIT_TYP_SP. Add DivPart in calls to
  ||                    | OP_SPLIT_ORD_PK.SPLIT_TYPES_FOR_ORD_FN,
  ||                    | OP_SPLIT_ORD_PK.PO_SPLIT_INFO_SP. PIR15697
  ||----------------------------------------------------------------------------
  */
  PROCEDURE split_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm := 'OP_ORDER_RECEIPT_PK.SPLIT_SP';
    lar_parm              logs.tar_parm;
    l_t_split_typs        type_stab     := type_stab();
    l_allw_split_sw       VARCHAR2(1);
    l_new_po_on_split_sw  VARCHAR2(1);
    l_allw_mix_sw         VARCHAR2(1);

    CURSOR l_cur_vndr(
      b_div_part  NUMBER,
      b_ord_num   NUMBER
    ) IS
      SELECT   si.cbr_vndr_id
          FROM strct_item_op3v si
         WHERE si.div_part = b_div_part
           AND EXISTS(SELECT 1
                        FROM ordp100a a, ordp120b b
                       WHERE a.div_part = si.div_part
                         AND a.ordnoa = b_ord_num
                         AND b.div_part = a.div_part
                         AND b.ordnob = a.ordnoa
                         AND b.statb IN('O', 'I')
                         AND b.itemnb = si.item_num
                         AND b.sllumb = si.uom)
      GROUP BY si.cbr_vndr_id;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Split Types for Order');
    l_t_split_typs := op_split_ord_pk.split_types_for_ord_fn(i_div_part, i_ord_num);

    IF l_t_split_typs.COUNT > 0 THEN
      logs.dbg('Get PO Split Info');
      op_split_ord_pk.po_split_info_sp(i_div_part, i_ord_num, l_allw_split_sw, l_new_po_on_split_sw, l_allw_mix_sw);
      logs.dbg('Process Split Types');
      <<split_typs_loop>>
      FOR i IN l_t_split_typs.FIRST .. l_t_split_typs.LAST LOOP
        IF l_t_split_typs(i) = op_split_ord_pk.g_c_split_typ_strict_ord THEN
          <<vndr_loop>>
          FOR l_r_vndr IN l_cur_vndr(i_div_part, i_ord_num) LOOP
            logs.dbg('Process Strict Order Split Type');
            split_typ_sp(i_div_part,
                         i_ord_num,
                         l_t_split_typs(i),
                         l_allw_split_sw,
                         l_new_po_on_split_sw,
                         l_allw_mix_sw,
                         l_r_vndr.cbr_vndr_id
                        );
          END LOOP vndr_loop;
        ELSE
          logs.dbg('Process Split Type');
          split_typ_sp(i_div_part, i_ord_num, l_t_split_typs(i), l_allw_split_sw, l_new_po_on_split_sw, l_allw_mix_sw);
        END IF;   -- l_t_split_typs(i) = op_split_ord_pk.g_c_split_typ_strict_ord
      END LOOP split_typs_loop;
    END IF;   -- l_t_split_typs.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END split_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOCK_ORD_SP
  ||  Attempt to find and lock row for unprocessed order num on MCLPINPR.
  ||  Return indication of success.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/22/07 | rhalpai | Original
  || 12/08/15 | rhalpai | Add DivPart input parm.
  || 10/20/17 | rhalpai | Convert cursor to SELECT INTO. SDHD-201869
  ||----------------------------------------------------------------------------
  */
  PROCEDURE lock_ord_sp(
    i_div_part  IN      NUMBER,
    i_ord_num   IN      NUMBER,
    o_lock_sw   OUT     VARCHAR2
  ) IS
  BEGIN
    SELECT     'Y'
          INTO o_lock_sw
          FROM mclpinpr
         WHERE div_part = i_div_part
           AND ordnor = i_ord_num
           AND order_receipt_status = 0
    FOR UPDATE NOWAIT;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      o_lock_sw := 'N';
    WHEN excp.gx_row_locked THEN
      o_lock_sw := 'N';
  END lock_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || ATTACH_TO_ORD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate TestSw.
  ||                    | Convert to use OP1F and return LoadDepartSid. PIR11038
  || 12/08/15 | rhalpai | Add DivPart input parm.
  || 10/20/17 | rhalpai | Convert cursor to SELECT INTO. SDHD-201869
  ||----------------------------------------------------------------------------
  */
  PROCEDURE attach_to_ord_sp(
    i_div_part         IN      NUMBER,
    i_ord_num          IN      NUMBER,
    i_ord_src          IN      VARCHAR2,
    i_load_typ         IN      VARCHAR2,
    i_cust_id          IN      VARCHAR2,
    o_load_depart_sid  OUT     NUMBER
  ) IS
    l_attach_sw  VARCHAR2(1);
    l_cv         SYS_REFCURSOR;
  BEGIN
    SELECT 'Y'
      INTO l_attach_sw
      FROM sub_prcs_ord_src s
     WHERE s.div_part = i_div_part
       AND s.prcs_id = 'ORDER RECEIPT'
       AND s.prcs_sbtyp_cd = 'ALO'
       AND s.ord_src = i_ord_src;

    IF l_attach_sw = 'Y' THEN
      -----------------------------------------------------------------------
      -- Attempt to attach to existing order.
      -- Give preference to orders with order sources found in
      -- SUB_PRCS_ORD_SRC for subtype 'ALP' with the same load type with the
      -- lowest LLR date/time.
      -----------------------------------------------------------------------
      OPEN l_cv
       FOR
         SELECT   a.load_depart_sid
             FROM ordp100a a, load_depart_op1f ld, ordp120b b, sub_prcs_ord_src s
            WHERE a.div_part = i_div_part
              AND a.custa = i_cust_id
              AND a.ordnoa <> i_ord_num
              AND a.excptn_sw = 'N'
              AND a.dsorda = 'R'
              AND ld.div_part = a.div_part
              AND ld.load_depart_sid = a.load_depart_sid
              AND ld.llr_ts > DATE '1900-01-01'
              AND b.div_part = a.div_part
              AND b.ordnob = a.ordnoa
              AND b.excptn_sw = 'N'
              AND b.statb NOT IN('A', 'C', 'S')
              AND s.div_part(+) = a.div_part
              AND s.prcs_id(+) = 'ORDER RECEIPT'
              AND s.prcs_sbtyp_cd(+) = 'ALP'
              AND s.ord_src(+) = a.ipdtsa
              AND EXISTS(SELECT 1
                           FROM mclp120c c, mclp040d d
                          WHERE c.div_part = ld.div_part
                            AND c.loadc = ld.load_num
                            AND d.div_part(+) = c.div_part
                            AND d.loadd(+) = c.loadc
                            AND d.custd(+) = a.custa
                            AND i_load_typ =(CASE
                                               WHEN c.lbsgpc IN('Y', '1') THEN 'GMP'
                                               WHEN d.prod_typ = 'BTH' THEN i_load_typ
                                               ELSE NVL(d.prod_typ, i_load_typ)
                                             END
                                            )
                            AND c.test_bil_load_sw = 'N')
         ORDER BY (CASE
                     WHEN(    s.ord_src IS NOT NULL
                          AND a.ldtypa = i_load_typ) THEN 2
                     WHEN a.ldtypa = i_load_typ THEN 1
                     ELSE 0
                   END
                  ) DESC,
                  ld.llr_ts;

      FETCH l_cv
       INTO o_load_depart_sid;

      CLOSE l_cv;
    END IF;   -- l_attach_sw = 'Y'
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END attach_to_ord_sp;

  /*
  ||----------------------------------------------------------------------------
  || ASSIGN_LOAD_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 02/12/08 | rhalpai | Changed call to stop_num_fn to pass cbr cust instead
  ||                    | of mclane cust. IM379213
  ||                    | Added logic to assign order to COPY/LOST load. PIR3593
  || 02/25/08 | rhalpai | Changed to remove logic setting order type to GRO for
  ||                    |  GMP regular orders. IM381737
  || 07/30/08 | rhalpai | Added order number parm to call to ATTACH_DIST_SP.
  ||                    | IM432217
  || 08/11/08 | rhalpai | Removed status output parm and change exception
  ||                    | handler to log and then re-raise exception. IM435853
  || 11/19/10 | rhalpai | Changed logic to include CARE along with COPY and LOST
  ||                    | in check for MsgId when assigning Load/Stop to order.
  ||                    | PIR5152
  || 04/19/12 | dlbeal  | Change logic to populate orders load_depart_sid and
  ||                    | load info (load/stop,LLR,departure,ETA) from existing
  ||                    | entries in tables LOAD_DEPART_OP1F and STOP_ETA_OP1G
  ||                    | or create entries as needed.
  || 07/10/12 | rhalpai | Copy logic from old ASSIGN_LOAD_TO_ORDER_SP and
  ||                    | change to update load info when load found. Default
  ||                    | load is already assigned to order so no change
  ||                    | necessary.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change logic to use LoadDepart and StopEta. PIR11038
  || 03/17/15 | rhalpai | Add logic to look-up the load_depart_sid prior to
  ||                    | attaching distributions. IM-256790
  || 12/08/15 | rhalpai | Add DivPart input parm and pass in calls to
  ||                    | OP_ORDER_VALIDATION_PK.VALIDATE_HEADER_SP,
  ||                    | OP_ORDER_VALIDATION_PK.VALIDATE_DETAILS_SP,
  ||                    | ATTACH_TO_ORD_SP. PIR15697
  || 10/20/17 | rhalpai | Convert cursor to SELECT INTO. SDHD-201869
  ||----------------------------------------------------------------------------
  */
  PROCEDURE assign_load_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER,
    i_msg_id    IN  VARCHAR2
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm          := 'OP_ORDER_RECEIPT_PK.ASSIGN_LOAD_SP';
    lar_parm                   logs.tar_parm;
    l_c_dist          CONSTANT VARCHAR2(1)            := 'D';
    l_c_sysdate       CONSTANT DATE                   := SYSDATE;
    l_c_curr_rendate  CONSTANT PLS_INTEGER            := TRUNC(l_c_sysdate) - DATE '1900-02-28';
    l_max_shp_dt               PLS_INTEGER;
    l_load_typ                 ordp100a.ldtypa%TYPE;
    l_ord_src                  ordp100a.ipdtsa%TYPE;
    l_cust_id                  ordp100a.custa%TYPE;
    l_shp_dt                   NUMBER;
    l_ord_typ                  ordp100a.dsorda%TYPE;
    l_is_test_ord              BOOLEAN                := FALSE;
    l_find_load_sw             VARCHAR2(1);
    l_llr_ts                   DATE;
    l_load_num                 mclp120c.loadc%TYPE;
    l_load_depart_sid          NUMBER;
    l_is_ord_attched           BOOLEAN                := FALSE;
    l_hdr_err_rsn_cd           mclp140a.rsncda%TYPE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Get Order Info');

    SELECT l_c_curr_rendate + NVL(d.dislad, 7), NVL(a.ldtypa, 'GRO'), NVL(a.ipdtsa, ' '), a.custa,
           NVL(a.shpja, 99999), NVL(a.dsorda, 'R'), DECODE(ld.load_num, 'DFLT', 'Y', 'DIST', 'Y') AS find_load_sw
      INTO l_max_shp_dt, l_load_typ, l_ord_src, l_cust_id,
           l_shp_dt, l_ord_typ, l_find_load_sw
      FROM mclp130d d, ordp100a a, load_depart_op1f ld
     WHERE d.div_part = i_div_part
       AND a.div_part = d.div_part
       AND a.ordnoa = i_ord_num
       AND ld.div_part = a.div_part
       AND ld.load_depart_sid = a.load_depart_sid;

    logs.dbg('Initialize');
    l_is_test_ord :=(l_ord_typ = 'T');
    logs.dbg('Determine Attach-To Order');
    attach_to_ord_sp(i_div_part, i_ord_num, l_ord_src, l_load_typ, l_cust_id, l_load_depart_sid);
    l_is_ord_attched :=(l_load_depart_sid IS NOT NULL);

    IF (    l_find_load_sw = 'Y'
        AND NOT l_is_ord_attched
        AND NOT l_is_test_ord
        AND l_load_typ NOT BETWEEN 'P00' AND 'P99'
        AND (   l_ord_typ <> l_c_dist
             OR l_shp_dt < l_max_shp_dt)
       ) THEN
      IF l_ord_typ = l_c_dist THEN
        op_order_load_pk.nxt_load_for_dist_ord_sp(i_div_part, i_ord_num, l_llr_ts, l_load_num);
      ELSE
        op_order_load_pk.nxt_load_for_ord_sp(i_div_part, i_ord_num, NULL, l_llr_ts, l_load_num);
      END IF;   -- l_ord_typ = l_c_dist

      IF l_load_num IS NOT NULL THEN
        l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(i_div_part, l_llr_ts, l_load_num);
        op_order_load_pk.merge_stop_eta_sp(i_div_part, l_load_depart_sid, l_cust_id);
      END IF;   -- l_load_num IS NOT NULL
    END IF;   -- l_find_load_sw = 'Y' AND NOT l_is_ord_attched

    IF l_load_depart_sid IS NOT NULL THEN
      logs.dbg('Assign Load Info to Order');

      UPDATE ordp100a a
         SET a.load_depart_sid = l_load_depart_sid
       WHERE a.div_part = i_div_part
         AND a.ordnoa = i_ord_num
         AND a.load_depart_sid <> l_load_depart_sid;
    END IF;   -- l_load_depart_sid IS NOT NULL

    logs.dbg('Check to Bypass Order Validation');

    IF NOT is_bypass_ord_validation_fn(i_div_part, l_ord_src) THEN
      ---------------------------
      -- Order Header Validation
      ---------------------------
      logs.dbg('Header Level Validation');
      op_order_validation_pk.validate_header_sp(i_div_part, i_ord_num, l_hdr_err_rsn_cd);

      IF l_hdr_err_rsn_cd IS NULL THEN
        ---------------------------------------------------------
        -- Validate order details if there are no Header Errors
        ---------------------------------------------------------
        logs.dbg('Detail Level Validation');
        op_order_validation_pk.validate_details_sp(i_div_part, i_ord_num);
      END IF;   -- l_hdr_err_rsn_cd IS NULL
    END IF;   -- NOT is_bypass_ord_validation_fn(i_div_part, l_ord_src)

    IF (    NOT l_is_ord_attched
        AND l_ord_typ <> l_c_dist
        AND l_hdr_err_rsn_cd IS NULL) THEN
      IF l_load_depart_sid IS NULL THEN
        logs.dbg('Look Up LoadDepartSid');

        SELECT a.load_depart_sid
          INTO l_load_depart_sid
          FROM ordp100a a
         WHERE a.div_part = i_div_part
           AND a.ordnoa = i_ord_num;
      END IF;   -- l_load_depart_sid IS NULL

      ---------------------------------------------------
      -- Look for Distributions to attach to this order
      ---------------------------------------------------
      logs.dbg('Attach Dist to Reg Order');
      op_order_load_pk.attach_dist_ords_sp(i_div_part, l_load_depart_sid, l_cust_id, 'ORDRCPT');
    END IF;   -- NOT l_is_ord_attched

    logs.dbg('Remove entry from MCLPINPR');

    DELETE FROM mclpinpr
          WHERE div_part = i_div_part
            AND ordnor = i_ord_num;

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END assign_load_sp;

  /*
  ||----------------------------------------------------------------------------
  || PRCS_ORDS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 08/11/08 | rhalpai | Removed status output parm, removed logic to update
  ||                    | status in MCLPINPR, and removed status parm from call
  ||                    | to ASSIGN_LOAD_TO_ORDER_SP. IM435853
  || 07/10/12 | rhalpai | Copy logic from UPD_NEW_ORDS_SP.
  || 12/08/15 | rhalpai | Add DivPart input parm. Add DivPart in calls to
  ||                    | LOCK_ORD_SP, ASSIGN_LOAD_SP.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE prcs_ords_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_RECEIPT_PK.PRCS_ORDS_SP';
    lar_parm             logs.tar_parm;

    CURSOR l_cur_ords(
      b_div_part  NUMBER
    ) IS
      SELECT ordnor AS ord_num, msgidr AS msg_id
        FROM mclpinpr
       WHERE div_part = b_div_part
         AND order_receipt_status = 0;

    l_lock_sw            VARCHAR2(1);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Update New Orders');
    FOR l_r_ord IN l_cur_ords(i_div_part) LOOP
      logs.dbg('Lock Row on MCLPINPR for Order');
      lock_ord_sp(i_div_part, l_r_ord.ord_num, l_lock_sw);

      IF l_lock_sw = 'Y' THEN
        -- row was found and locked in MCLPINPR
        logs.dbg('Assign load to order');
        assign_load_sp(i_div_part, l_r_ord.ord_num, l_r_ord.msg_id);
      END IF;   -- l_lock_sw = 'Y'
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END prcs_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_MSG_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_msg_sp(
    i_div_part  IN  NUMBER,
    i_msg_seq   IN  NUMBER
  ) IS
  BEGIN
    DELETE FROM mclane_order_receipt_msgs
          WHERE div_part = i_div_part
            AND msg_seq = i_msg_seq;
  END del_msg_sp;

  /*
  ||----------------------------------------------------------------------------
  || LOG_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 08/11/08 | rhalpai | Changed to be autonomous trasaction. IM435853
  || 07/10/12 | rhalpai | Change logic to use new package MsgOrdHdr record.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_sp(
    i_msg_typ    IN  VARCHAR2,
    i_start_ts   IN  DATE,
    i_msg_stat   IN  VARCHAR2,
    i_descr      IN  VARCHAR2,
    i_msg_seq    IN  NUMBER,
    i_r_ord_hdr  IN  g_rt_msg_ord_hdr
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_RECEIPT_PK.LOG_SP';
    lar_parm             logs.tar_parm;
    l_msg                VARCHAR2(200);
  BEGIN
    logs.add_parm(lar_parm, 'MsgTyp', i_msg_typ);
    logs.add_parm(lar_parm, 'StartTs', i_start_ts);
    logs.add_parm(lar_parm, 'MsgStat', i_msg_stat);
    logs.add_parm(lar_parm, 'Descr', i_descr);
    logs.add_parm(lar_parm, 'MsgSeq', i_msg_seq);
    logs.add_parm(lar_parm, 'Div', i_r_ord_hdr.div);
    logs.add_parm(lar_parm, 'OrdNum', i_r_ord_hdr.ord_num);
    logs.add_parm(lar_parm, 'CustId', i_r_ord_hdr.cust_id);
    logs.add_parm(lar_parm, 'LegcyRef', i_r_ord_hdr.legcy_ref);
    l_msg := i_descr;

    IF i_msg_stat = 'C' THEN
      l_msg := i_descr || ' CustId: ' || i_r_ord_hdr.cust_id || ' LegcyRef: ' || i_r_ord_hdr.legcy_ref;
    END IF;

    INSERT INTO mclane_order_receipt_status
                (msg_type, div_part, msg_status, create_ts, end_ts, exception_desc, order_num, msg_seq)
      SELECT i_msg_typ, d.div_part, i_msg_stat, i_start_ts, SYSDATE, l_msg, i_r_ord_hdr.ord_num, i_msg_seq
        FROM div_mstr_di1d d
       WHERE d.div_id = i_r_ord_hdr.div;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END log_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_MSG_TO_FAIL_STAT_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 08/11/08 | rhalpai | Changed to be autonomous trasaction and removed open
  ||                    | status restriction. IM435853
  || 12/08/15 | rhalpai | Add DivPart input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_msg_to_fail_stat_sp(
    i_div_part  IN  NUMBER,
    i_msg_seq   IN  NUMBER
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_RECEIPT_PK.UPD_MSG_TO_FAIL_STAT_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'MsgSeq', i_msg_seq);

    UPDATE mclane_order_receipt_msgs
       SET msg_status = 'F'
     WHERE div_part = i_div_part
       AND msg_seq = i_msg_seq;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END upd_msg_to_fail_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_FATAL_MSGS_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 08/11/08 | rhalpai | Added i_div and p_msg_seqs parms and removed commit
  ||                    | and rollback. IM435853
  || 12/08/15 | rhalpai | Add Div input parm.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE process_fatal_msgs_sp(
    i_div         IN  VARCHAR2,
    i_t_msg_seqs  IN  type_ntab
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm           := 'OP_ORDER_RECEIPT_PK.PROCESS_FATAL_MSGS_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_c_prcs_id  CONSTANT prcs_dfn.prcs_id%TYPE   := 'ORDER RECEIPT';
    l_c_subj     CONSTANT VARCHAR2(80)            := 'OP Order Receipt Failure for Order after Second Attempt';
    l_c_msg      CONSTANT VARCHAR2(500)
      := 'An attempt to process one or more orders in the Order Receipt process'
         || ' has failed after two attempts.  Contact the OP on-call support'
         || ' immediately.  The order will remain in a FATAL status until it'
         || ' has been addressed by on-call support.';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgSeqTab', i_t_msg_seqs);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Change msg status to E');

    UPDATE mclane_order_receipt_msgs m
       SET m.msg_status = 'E'
     WHERE m.div_part = l_div_part
       AND m.msg_status = 'F'
       AND m.msg_seq IN(SELECT t.column_value
                          FROM TABLE(CAST(i_t_msg_seqs AS type_ntab)) t);

    IF SQL%ROWCOUNT > 0 THEN
      logs.dbg('Send user notification');
      op_process_common_pk.notify_group_sp(i_div, l_c_prcs_id, l_c_subj, l_c_msg);
    END IF;   -- SQL%ROWCOUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END process_fatal_msgs_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || INS_ORD_RECPT_MSG_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/26/11 | rhalpai | Original
  ||----------------------------------------------------------------------------
  */
  PROCEDURE ins_ord_recpt_msg_sp(
    i_div      IN      VARCHAR2,
    i_msg_typ  IN      VARCHAR2,
    i_msg_id   IN      VARCHAR2,
    i_msg      IN      CLOB,
    o_msg_seq  OUT     NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_RECEIPT_PK.INS_ORD_RECPT_MSG_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
  BEGIN
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgTyp', i_msg_typ);
    logs.add_parm(lar_parm, 'MsgId', i_msg_id);
    logs.add_parm(lar_parm, 'Msg', i_msg);
    l_div_part := div_pk.div_part_fn(i_div);

    INSERT INTO mclane_order_receipt_msgs
                (msg_seq, div_part, msg_type, msg_id, msg_status, long_msg
                )
         VALUES (ord_recpt_msgs_msg_num_seq.NEXTVAL, l_div_part, i_msg_typ, i_msg_id, 'O', i_msg
                )
      RETURNING msg_seq
           INTO o_msg_seq;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END ins_ord_recpt_msg_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_ORD_BY_LEGCY_REF_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 07/10/12 | rhalpai | Add parm i_div.
  || 10/01/12 | rhalpai | Add logic to recycle deletes. PIR5250
  || 11/29/12 | rhalpai | Change logic to add OrdNums in billed status to
  ||                    | DIST_DEL_RECYCL_OP4R table. IM-074192
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_ord_by_legcy_ref_sp(
    i_div        IN  VARCHAR2,
    i_legcy_ref  IN  VARCHAR2 DEFAULT NULL
  ) IS
    l_c_sysdate  CONSTANT DATE      := SYSDATE;
    l_div_part            NUMBER;
    l_t_ord_nums          type_ntab;
    l_t_legcy_refs        type_stab;
  BEGIN
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_legcy_ref IS NOT NULL THEN
      SELECT a.ordnoa, i_legcy_ref
      BULK COLLECT INTO l_t_ord_nums, l_t_legcy_refs
        FROM ordp100a a
       WHERE a.div_part = l_div_part
         AND a.legrfa = i_legcy_ref
         AND a.dsorda = 'D'
         AND a.stata IN('O', 'I', 'S', 'C');
    ELSE
      SELECT r.ord_num, a.legrfa AS legcy_ref
      BULK COLLECT INTO l_t_ord_nums, l_t_legcy_refs
        FROM dist_del_recycl_op4r r, ordp100a a
       WHERE r.div_part = l_div_part
         AND a.div_part = r.div_part
         AND a.ordnoa = r.ord_num
         AND a.stata IN('O', 'I', 'S', 'C');
    END IF;   -- i_legcy_ref IS NOT NULL

    IF l_t_ord_nums.COUNT > 0 THEN
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM mclp300d
              WHERE div_part = l_div_part
                AND ordnod = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM ordp120b
              WHERE div_part = l_div_part
                AND ordnob = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM ordp140c
              WHERE div_part = l_div_part
                AND ordnoc = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM ordp100a
              WHERE div_part = l_div_part
                AND ordnoa = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        INSERT INTO mclp900d
                    (div_part, ordnod, ordlnd, reasnd, descd, exlvld, itemd, qtyfrd,
                     qtytod, resexd, exdesd, resdtd, restmd, last_chg_ts
                    )
             VALUES (l_div_part, l_t_ord_nums(i), 0, 'DELDIST', 'Distribution Deleted: ' || l_t_legcy_refs(i), 6, 0, 0,
                     0, '1', 'DEL_ORD_BY_LEGCY_REF_SP', 0, 0, l_c_sysdate
                    );
    END IF;   -- l_t_ord_nums.COUNT > 0

    IF i_legcy_ref IS NOT NULL THEN
      INSERT INTO dist_del_recycl_op4r
                  (div_part, ord_num, create_ts)
        SELECT a.div_part, a.ordnoa AS ord_num, l_c_sysdate
          FROM ordp100a a
         WHERE a.div_part = l_div_part
           AND a.legrfa = i_legcy_ref
           AND a.stata NOT IN('O', 'I', 'S', 'C')
           AND a.dsorda = 'D'
           AND NOT EXISTS(SELECT 1
                            FROM dist_del_recycl_op4r r
                           WHERE r.div_part = a.div_part
                             AND r.ord_num = a.ordnoa);
    ELSE
      DELETE FROM dist_del_recycl_op4r r
            WHERE r.div_part = l_div_part
              AND NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = r.div_part
                                AND a.ordnoa = r.ord_num);
    END IF;   -- i_legcy_ref IS NOT NULL
  END del_ord_by_legcy_ref_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_ORD_MSG_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 02/12/08 | rhalpai | Changed logic to pass order header to log_sp to allow
  ||                    | capturing customer, line count, legacy ref num. IM379213
  ||                    | Added logic to assign order to COPY/LOST load. PIR3593
  || 08/11/08 | rhalpai | Removed status parm from call to UPD_NEW_ORDS_SP.
  ||                    | IM435853
  || 11/19/10 | rhalpai | Changed logic to use CARE as MsgId for MCLPINPR entry,
  ||                    | which will ultimately be used as the load assignment
  ||                    | and added logic to update the order number on
  ||                    | CARE_PKG_ORD_CP1C for new CARE orders. PIR5152
  || 02/22/11 | rhalpai | Add logic to bypass orders in invalid (billed) status
  ||                    | and remove any new order lines. IM-007160
  || 07/10/12 | rhalpai | Change logic to default the load assigned to the order.
  || 08/10/12 | rhalpai | Add logic to force non-mixed RegBev orders to GMP
  ||                    | load type. PIR11647
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 05/09/13 | rhalpai | Change logic to allow existing orders with load
  ||                    | assignments to keep them. IM-102610
  || 07/04/13 | rhalpai | Change logic to remove LoadInfo from OrdDtl and to
  ||                    | default LLRTs and ETATs of LoadInfo for OrdHdr.
  ||                    | PIR11038
  || 12/08/15 | rhalpai | Add Div input parm. Add DivPart in calls to
  ||                    | VALID_ORD_STAT_FN, IS_TEST_ORD_SRC_FN, SPLIT_SP,
  ||                    | DEL_MSG_SP, UPD_MSG_TO_FAIL_STAT_SP, PRCS_ORDS_SP.
  || 06/02/17 | rhalpai | Add logic to override order source from ADC to CSRWRK
  ||                    | and from ADK to KEY. PIR14910
  || 10/20/17 | rhalpai | Changed logic to retrieve locked msg for processing.
  ||                    | SDHD-201869
  || 11/21/18 | rhalpai | Add logic to remove duplicate open distributions when
  ||                    | new one is being created. SDHD-369938
  || 10/14/19 | rhalpai | Add logic to notify of matching billed distribution. PIR16403
  || 10/30/19 | rhalpai | Change logic for matching billed distribution to include
  ||                    | parm to bypass. PIR16403
  || 04/06/20 | rhalpai | Change logic for matching billed distribution to include
  ||                    | pending order header status to account for billing release
  ||                    | in process. PIR16403
  || 04/18/25 | rhalpai | Add logic to force non-mixed Strict to GMP for Crp in STRCT_GMP_CRP. PC-10400
  ||----------------------------------------------------------------------------
  */
  PROCEDURE process_ord_msg_sp(
    i_div                IN      VARCHAR2,
    i_msg_typ            IN      VARCHAR2,
    i_msg_seq            IN      NUMBER,
    o_bil_dupl_dist_msg  OUT     VARCHAR2
  ) IS
    l_c_module    CONSTANT typ.t_maxfqnm                       := 'OP_ORDER_RECEIPT_PK.PROCESS_ORD_MSG_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_r_msg                mclane_order_receipt_msgs%ROWTYPE;
    l_msg_pos              PLS_INTEGER                         := 1;
    l_r_msg_ord_hdr        g_rt_msg_ord_hdr;
    l_c_csr       CONSTANT VARCHAR2(3)                         := 'CSR';
    l_c_dis       CONSTANT VARCHAR2(3)                         := 'DIS';
    l_c_reg       CONSTANT VARCHAR2(3)                         := 'REG';
    l_new_ord_sw           VARCHAR2(1);
    l_cv                   SYS_REFCURSOR;
    l_c_start_ts  CONSTANT DATE                                := SYSDATE;
    l_po_num               ordp100a.cpoa%TYPE;
    l_r_load_info          g_rt_load_info;
    l_bil_dist_ord_num     NUMBER;
    l_t_msg_ord_dtls       g_tt_msg_ord_dtls;
    l_idx                  PLS_INTEGER;
    l_t_strct_gmp_crp      type_stab;
    l_log_msg              VARCHAR2(256);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgTyp', i_msg_typ);
    logs.add_parm(lar_parm, 'MsgSeq', i_msg_seq);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Lock Msg');
    lock_msg_sp(l_div_part, i_msg_typ, i_msg_seq, l_r_msg);

    IF l_r_msg.msg_id IS NOT NULL THEN
      logs.dbg('Parse Hdr');
      parse_hdr_sp(l_r_msg.long_msg, l_msg_pos, l_r_msg_ord_hdr);

      IF (    l_r_msg_ord_hdr.ord_src IN('ADC', 'ADK')
          AND op_parms_pk.val_fn(l_div_part, 'ADD_ON_ORD') = 'Y') THEN
        l_r_msg_ord_hdr.ord_src :=(CASE l_r_msg_ord_hdr.ord_src
                                     WHEN 'ADC' THEN 'CSRWRK'
                                     ELSE 'KEY'
                                   END);
      END IF;   -- l_r_msg_ord_hdr.ord_src IN('ADC','ADK') AND AND op_parms_pk.val_fn(l_div_part, 'ADD_ON_ORD') = 'Y'

      IF l_r_msg_ord_hdr.no_ord_sw IS NULL THEN
        l_log_msg := 'Empty message. It has been deleted from msg table.';
      ELSIF(    i_msg_typ = l_c_dis
            AND l_r_msg_ord_hdr.actn_cd = 'DEL') THEN
        logs.dbg('Remove Dist Order by Legacy Ref');
        del_ord_by_legcy_ref_sp(i_div, l_r_msg_ord_hdr.legcy_ref);
      ELSIF(    l_r_msg_ord_hdr.ord_num > 0
            AND NOT valid_ord_stat_fn(l_div_part, l_r_msg_ord_hdr.ord_num)) THEN
        l_log_msg := 'Order bypassed due to invalid status. Removed any new lines.';
        logs.dbg('Remove any pending new order lines');

        DELETE FROM ordp120b b
              WHERE b.div_part = l_div_part
                AND b.ordnob = l_r_msg_ord_hdr.ord_num
                AND b.statb = 'I'
                AND b.subrcb IS NULL;
      ELSE
        -- get order number
        IF l_r_msg_ord_hdr.ord_num = 0 THEN
          l_new_ord_sw := 'Y';
          l_r_msg_ord_hdr.ord_num := nxt_ord_num_fn;
        ELSE
          SELECT MAX('Y')
            INTO l_new_ord_sw
            FROM ordp100a a
           WHERE a.div_part = l_div_part
             AND a.ordnoa = l_r_msg_ord_hdr.ord_num
             AND a.stata = 'I'
             AND a.load_depart_sid = 0;
        END IF;   -- l_r_msg_ord_hdr.ord_num = 0

        IF l_new_ord_sw = 'Y' THEN
          l_r_msg.msg_id :=(CASE SUBSTR(l_r_msg_ord_hdr.conf_num, 1, 1)
                              WHEN 'K' THEN 'COPY'
                              WHEN 'L' THEN 'LOST'
                              WHEN 'P' THEN 'CARE'
                              ELSE l_r_msg.msg_id
                            END
                           );

          IF l_r_msg.msg_id = 'CARE' THEN
            UPDATE care_pkg_ord_cp1c cp1c
               SET cp1c.ord_num = l_r_msg_ord_hdr.ord_num,
                   cp1c.stat_cd = 'STG'
             WHERE cp1c.div_part = l_div_part
               AND cp1c.conf_num = l_r_msg_ord_hdr.conf_num;
          END IF;   -- l_msg_id = 'CARE'
        ELSE
          logs.dbg('Get Load Info');
          get_load_info_sp(l_div_part, l_r_msg_ord_hdr.ord_num, l_r_load_info);
        END IF;   -- l_new_ord_sw = 'Y'

        logs.dbg('Parse Details');
        parse_dtls_sp(l_r_msg.long_msg, l_msg_pos, l_t_msg_ord_dtls);

        IF i_msg_typ = l_c_dis THEN
          logs.dbg('Remove Any Existing Unbilled Duplicate Dist Order');
          del_ord_by_legcy_ref_sp(i_div, l_r_msg_ord_hdr.legcy_ref);

          IF op_parms_pk.val_fn(l_div_part, 'CHK_BIL_DUPL_DIST') = 'Y' THEN
            logs.dbg('Check For Existing Billed Duplicate Dist Order');

            SELECT MAX(a.ordnoa)
              INTO l_bil_dist_ord_num
              FROM ordp100a a, ordp120b b
             WHERE a.div_part = l_div_part
               AND a.legrfa = l_r_msg_ord_hdr.legcy_ref
               AND a.dsorda = 'D'
               AND a.stata IN('P', 'R', 'A')
               AND b.div_part = a.div_part
               AND b.ordnob = a.ordnoa
               AND (   b.pckqtb > 0
                    OR (    b.statb = 'P'
                        AND b.ordqtb > 0));

            IF l_bil_dist_ord_num IS NOT NULL THEN
              o_bil_dupl_dist_msg := l_r_msg_ord_hdr.legcy_ref
                                     || ','
                                     || TO_CHAR(l_r_msg_ord_hdr.shp_dt, 'YYYY-MM-DD')
                                     || ','
                                     || l_r_msg_ord_hdr.ord_num;
            END IF;   -- l_bil_dist_ord_num IS NOT NULL
          END IF;   -- op_parms_pk.val_fn(l_div_part, 'CHK_BIL_DUPL_DIST') = 'Y'

          IF l_r_msg_ord_hdr.load_typ = 'GMP' THEN
            logs.dbg('Chg GMP LoadTyp to GRO when no Cust GMP Load');

            OPEN l_cv
             FOR
               SELECT 'GRO'
                 FROM DUAL
                WHERE NOT EXISTS(SELECT 1
                                   FROM mclp040d st, mclp120c ld
                                  WHERE st.div_part = l_div_part
                                    AND st.custd = l_r_msg_ord_hdr.cust_id
                                    AND ld.div_part = st.div_part
                                    AND ld.loadc = st.loadd
                                    AND (   ld.lbsgpc = '1'
                                         OR st.prod_typ = 'BTH'));

            FETCH l_cv
             INTO l_r_msg_ord_hdr.load_typ;

            CLOSE l_cv;
          END IF;   -- l_r_msg_ord_hdr.load_typ = 'GMP'
        END IF;   -- i_msg_typ = l_c_dis

        logs.dbg('Check for Test Order Source');

        IF (    l_r_msg_ord_hdr.ord_typ <> 'T'
            AND is_test_ord_src_fn(l_div_part, l_r_msg_ord_hdr.cust_id, l_r_msg_ord_hdr.ord_src)
           ) THEN
          l_r_msg_ord_hdr.ord_typ := 'T';
        END IF;   -- l_r_msg_ord_hdr.ord_typ <> 'T' AND is_test_ord_src_fn(l_div_part, l_r_msg_ord_hdr.cust_id, l_r_msg_ord_hdr.ord_src)

        IF l_r_load_info.load_num IS NULL THEN
          l_r_load_info.load_num :=(CASE
                                      WHEN l_r_msg_ord_hdr.ord_typ = 'T' THEN 'TEST'
                                      WHEN l_r_msg.msg_id IN('COPY', 'LOST', 'CARE') THEN l_r_msg.msg_id
                                      WHEN l_r_msg_ord_hdr.load_typ LIKE 'P__' THEN l_r_msg_ord_hdr.load_typ || 'P'
                                      WHEN l_r_msg_ord_hdr.ord_typ = 'D' THEN 'DIST'
                                      ELSE 'DFLT'
                                    END
                                   );
          l_r_load_info.stop_num := 0;
          l_r_load_info.llr_ts := DATE '1900-01-01';
          l_r_load_info.eta_ts := DATE '1900-01-01';
        END IF;   -- l_r_load_info.load_num IS NULL

        IF (    l_t_msg_ord_dtls IS NOT NULL
            AND l_t_msg_ord_dtls.COUNT > 0) THEN
          logs.dbg('Load Order Details');
          l_idx := l_t_msg_ord_dtls.FIRST;
          l_po_num := l_t_msg_ord_dtls(l_idx).po_num;
          WHILE l_idx IS NOT NULL LOOP
            IF (   l_new_ord_sw = 'Y'
                OR l_t_msg_ord_dtls(l_idx).ord_ln = 0) THEN
              l_t_msg_ord_dtls(l_idx).ord_ln := l_idx;
            END IF;   -- l_new_ord_sw = 'Y' OR l_t_msg_ord_dtls(v_idx).ord_ln = 0

            logs.dbg('Add/Chg Order Detail');
            merge_ord_dtl_sp(l_r_msg_ord_hdr, l_t_msg_ord_dtls(l_idx));
            l_idx := l_t_msg_ord_dtls.NEXT(l_idx);
          END LOOP;
        END IF;   -- l_t_msg_ord_dtls IS NOT NULL AND l_t_msg_ord_dtls.COUNT > 0

        logs.dbg('Add/Chg Order Header');
        merge_ord_hdr_sp(l_r_msg_ord_hdr, l_r_load_info, l_po_num, l_c_start_ts);
        logs.dbg('Add Order Comment');
        merge_ord_comnt_sp(l_r_msg_ord_hdr);
        logs.dbg('Add MCLPINPR Entry');
        ins_mclpinpr_sp(l_r_msg.msg_id, l_r_msg_ord_hdr);
        logs.dbg('Process Order for Splits');
        split_sp(l_div_part, l_r_msg_ord_hdr.ord_num);

        IF op_parms_pk.val_fn(l_div_part, op_const_pk.prm_regbev_on_gmp) = 'Y' THEN
          logs.dbg('Force non-mixed RegBev to GMP');

          UPDATE ordp100a a
             SET a.ldtypa = 'GMP'
           WHERE a.div_part = l_div_part
             AND a.ldtypa = 'GRO'
             AND a.stata IN('O', 'I')
             AND a.dsorda = 'R'
             AND a.ordnoa IN(SELECT r.ordnor
                               FROM mclpinpr r
                              WHERE r.div_part = l_div_part)
             AND EXISTS(SELECT 1
                          FROM ordp120b b, sawp505e e, item_grp_op2e ig
                         WHERE b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa
                           AND b.excptn_sw = 'N'
                           AND e.iteme = b.itemnb
                           AND e.uome = b.sllumb
                           AND ig.div_part = a.div_part
                           AND ig.cls_typ = 'REGBEV'
                           AND ig.catlg_num = e.catite)
             AND NOT EXISTS(SELECT 1
                              FROM ordp120b b, sawp505e e
                             WHERE b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND e.iteme = b.itemnb
                               AND e.uome = b.sllumb
                               AND NOT EXISTS(SELECT 1
                                                FROM item_grp_op2e ig
                                               WHERE ig.div_part = b.div_part
                                                 AND ig.cls_typ = 'REGBEV'
                                                 AND ig.catlg_num = e.catite));
        END IF;   -- op_parms_pk.val_fn(l_div_part, op_const_pk.prm_regbev_on_gmp) = 'Y'

        l_t_strct_gmp_crp := op_parms_pk.vals_for_prfx_fn(l_div_part, 'STRCT_GMP_CRP');

        IF l_t_strct_gmp_crp.COUNT > 0 THEN
          logs.dbg('Force non-mixed Strict to GMP for Crp in STRCT_GMP_CRP');

          UPDATE ordp100a a
             SET a.ldtypa = 'GMP'
           WHERE a.div_part = l_div_part
             AND a.ldtypa = 'GRO'
             AND a.stata IN('O', 'I')
             AND a.dsorda = 'R'
             AND a.ordnoa IN(SELECT r.ordnor
                               FROM mclpinpr r
                              WHERE r.div_part = l_div_part)
             AND EXISTS(SELECT 1
                          FROM mclp020b cx
                         WHERE cx.div_part = a.div_part
                           AND cx.custb = a.custa
                           AND TO_CHAR(cx.corpb) MEMBER OF l_t_strct_gmp_crp)
             AND EXISTS(SELECT 1
                          FROM ordp120b b, strct_item_op3v ig
                         WHERE b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa
                           AND b.excptn_sw = 'N'
                           AND ig.div_part = b.div_part
                           AND ig.item_num = b.itemnb
                           AND ig.uom = b.sllumb)
             AND NOT EXISTS(SELECT 1
                              FROM ordp120b b
                             WHERE b.div_part = a.div_part
                               AND b.ordnob = a.ordnoa
                               AND NOT EXISTS(SELECT 1
                                                FROM strct_item_op3v ig
                                               WHERE ig.div_part = b.div_part
                                                 AND ig.item_num = b.itemnb
                                                 AND ig.uom = b.sllumb));
        END IF;   -- l_t_strct_gmp_crp.COUNT > 0

        logs.dbg('Assign Load and Validate MCLPINPR Orders');
        -- process load assignment and validation for current order and any split orders
        prcs_ords_sp(l_div_part);
      END IF;   -- l_r_msg_ord_hdr.no_ord_sw IS NULL

      logs.dbg('Remove Msg');
      del_msg_sp(l_div_part, i_msg_seq);
      logs.dbg('Log');
      log_sp(i_msg_typ, l_c_start_ts, 'C', l_log_msg, i_msg_seq, l_r_msg_ord_hdr);
      COMMIT;
    END IF;   -- l_r_msg.msg_id IS NOT NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      l_log_msg := SQLERRM;
      logs.err(lar_parm, NULL, FALSE);
      ROLLBACK;
      log_sp(i_msg_typ, l_c_start_ts, 'F', l_log_msg, i_msg_seq, l_r_msg_ord_hdr);
      upd_msg_to_fail_stat_sp(l_div_part, i_msg_seq);
  END process_ord_msg_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_ORD_MSG_SP
  ||  Wrapper called by java
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/30/19 | rhalpai | Original for PIR16403
  ||----------------------------------------------------------------------------
  */
  PROCEDURE process_ord_msg_sp(
    i_div      IN  VARCHAR2,
    i_msg_typ  IN  VARCHAR2,
    i_msg_seq  IN  NUMBER
  ) IS
    l_msg  typ.t_maxvc2;
  BEGIN
    process_ord_msg_sp(i_div, i_msg_typ, i_msg_seq, l_msg);
  END process_ord_msg_sp;

  /*
  ||----------------------------------------------------------------------------
  || PROCESS_ORD_MSGS_SP
  ||  Process open OrderReceiptMsgs for MsgTyp
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/20/17 | rhalpai | Original for SDHD-201869
  || 03/05/18 | rhalpai | Add Process Control logic for Distributions. PIR16403
  || 10/30/19 | rhalpai | Add logic to notify of matching billed distributions. PIR16403
  ||----------------------------------------------------------------------------
  */
  PROCEDURE process_ord_msgs_sp(
    i_div      IN  VARCHAR2,
    i_msg_typ  IN  VARCHAR2
  ) IS
    l_c_module     CONSTANT typ.t_maxfqnm  := 'OP_ORDER_RECEIPT_PK.PROCESS_ORD_MSGS_SP';
    lar_parm                logs.tar_parm;
    l_div_part              NUMBER;
    l_msg_seq               NUMBER;
    l_bil_dupl_dist_msg     typ.t_maxvc2;
    l_bil_dupl_dist_cnt     PLS_INTEGER    := 0;
    l_min_bil_dupl_dist     typ.t_maxvc2   := '~';
    l_max_bil_dupl_dist     typ.t_maxvc2   := ' ';
    l_c_prcs_id    CONSTANT VARCHAR2(30)   := 'BIL_DUPL_DIST';
    l_c_mail_subj  CONSTANT VARCHAR2(50)   := i_div || ' Order Receipt With Matching Billed Distribution';
    l_mail_msg              VARCHAR2(4000);
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgTyp', i_msg_typ);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);

    IF i_msg_typ = 'DIS' THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_order_receipt_dis,
                                                  op_process_control_pk.g_c_active,
                                                  USER,
                                                  l_div_part
                                                 );
    END IF;   -- i_msg_typ = 'DIS'

    LOOP
      l_msg_seq := nxt_msg_seq_fn(l_div_part, i_msg_typ);
      EXIT WHEN l_msg_seq IS NULL;
      process_ord_msg_sp(i_div, i_msg_typ, l_msg_seq, l_bil_dupl_dist_msg);

      IF l_bil_dupl_dist_msg IS NOT NULL THEN
        l_bil_dupl_dist_cnt := l_bil_dupl_dist_cnt + 1;
        l_min_bil_dupl_dist := LEAST(l_min_bil_dupl_dist, l_bil_dupl_dist_msg);
        l_max_bil_dupl_dist := GREATEST(l_max_bil_dupl_dist, l_bil_dupl_dist_msg);
      END IF;   -- l_bil_dupl_dist_msg IS NOT NULL
    END LOOP;

    IF i_msg_typ = 'DIS' THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_order_receipt_dis,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );

      IF l_bil_dupl_dist_cnt > 0 THEN
        l_mail_msg := l_bil_dupl_dist_cnt
                      || ' duplicates found. Min: '
                      || l_min_bil_dupl_dist
                      || ' Max: '
                      || l_max_bil_dupl_dist;
        op_process_common_pk.notify_group_sp('MC', l_c_prcs_id, l_c_mail_subj, l_mail_msg);
      END IF;   -- l_bil_dupl_dist_cnt > 0
    END IF;   -- i_msg_typ = 'DIS'

    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN op_process_control_pk.g_e_process_restricted THEN
      logs.warn(SQLERRM, lar_parm);
    WHEN OTHERS THEN
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_order_receipt_dis,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END process_ord_msgs_sp;

  /*
  ||----------------------------------------------------------------------------
  || FINALIZE_SP
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 09/26/07 | rhalpai | Original
  || 08/11/08 | rhalpai | Changed to process msgs by div and pass div/msg_seqs
  ||                    | parms to PROCESS_FATAL_MSGS_SP. IM435853
  || 12/08/15 | rhalpai | Add Div input parm and pass to PROCESS_ORD_MSG_SP,
  ||                    | PROCESS_FATAL_MSGS_SP.
  || 10/20/17 | rhalpai | Change logic to always reprocess any open orders.
  ||                    | SDHD-201869
  ||----------------------------------------------------------------------------
  */
  PROCEDURE finalize_sp(
    i_div      IN  VARCHAR2,
    i_msg_typ  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_ORDER_RECEIPT_PK.FINALIZE_SP';
    lar_parm             logs.tar_parm;
    l_div_part           NUMBER;
    l_t_msg_seqs         type_ntab     := type_ntab();
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'MsgTyp', i_msg_typ);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Reset fatal msgs to open status');

    UPDATE mclane_order_receipt_msgs m
       SET m.msg_status = 'O'
     WHERE m.msg_status = 'F'
       AND m.msg_type = i_msg_typ
       AND m.div_part = l_div_part
       AND m.msg_seq IN(SELECT   s.msg_seq
                            FROM mclane_order_receipt_status s
                           WHERE s.msg_status = 'F'
                             AND s.msg_type = m.msg_type
                             AND s.div_part = m.div_part
                             AND s.msg_seq = m.msg_seq
                        GROUP BY s.msg_seq, s.msg_status
                          HAVING COUNT(1) = 1);

    logs.dbg('Process Order Msgs');
    process_ord_msgs_sp(i_div, i_msg_typ);
    logs.dbg('Final process failure messages');
    process_fatal_msgs_sp(i_div, l_t_msg_seqs);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.dbg('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END finalize_sp;
END op_order_receipt_pk;
/

