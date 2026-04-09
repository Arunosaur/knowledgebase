CREATE OR REPLACE FUNCTION kit_ord_fn(
  i_cur  IN  SYS_REFCURSOR
)
  RETURN kit_ords_t PIPELINED IS
  /*
  ||----------------------------------------------------------------------------
  ||  Function:     KIT_ORD_FN
  ||  Description:  Table function returning kit component order info.
  ||    Adds the order info to passed cursor and pipes each row.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/14/05 | rhalpai | Original - PIR2909
  || 03/27/06 | rhalpai | Changed cursor to handle null PO's IM225137
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  ||----------------------------------------------------------------------------
  */
  l_o_kit_ord  kit_ord_t;

  CURSOR l_cur_ord(
    b_ord_stat   VARCHAR2,
    b_div        VARCHAR2,
    b_llr_dt     DATE,
    b_ord_typ    VARCHAR2,
    b_cust_id    VARCHAR2,
    b_load_num   VARCHAR2,
    b_stop_num   NUMBER,
    b_eta_dt     DATE,
    b_po_num     VARCHAR2,
    b_catlg_num  VARCHAR2,
    b_comp_qty   NUMBER
  ) IS
    SELECT   b.ordnob, b.lineb, b.itemnb, b.sllumb,(CASE
                                                      WHEN NVL(b.pckqtb, 0) > 0 THEN b.pckqtb
                                                      ELSE b.ordqtb
                                                    END) AS qty,
             ((CASE
                 WHEN NVL(b.pckqtb, 0) > 0 THEN b.pckqtb
                 ELSE b.ordqtb
               END) / b_comp_qty) AS ratio, ROWNUM AS seq
        FROM div_mstr_di1d d, load_depart_op1f ld, stop_eta_op1g se, ordp100a a, ordp120b b
       WHERE d.div_id = b_div
         AND ld.div_part = d.div_part
         AND ld.llr_dt = b_llr_dt
         AND ld.load_num = b_load_num
         AND se.div_part = ld.div_part
         AND se.load_depart_sid = ld.load_depart_sid
         AND se.cust_id = b_cust_id
         AND se.stop_num = b_stop_num
         AND TRUNC(se.eta_ts) = b_eta_dt
         AND a.div_part = se.div_part
         AND a.load_depart_sid = se.load_depart_sid
         AND a.custa = se.cust_id
         AND a.dsorda = b_ord_typ
         AND NVL(a.cpoa, ' ') = NVL(b_po_num, ' ')
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = b_ord_stat
         AND b.excptn_sw = 'N'
         AND b.ordqtb > 0
         AND b.subrcb = 0
         AND b.orditb = b_catlg_num
    ORDER BY ratio, b.ordnob;
BEGIN
  -- initialize
  l_o_kit_ord := kit_ord_t(NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL
                          );
  <<kit_ord_loop>>
  LOOP
    FETCH i_cur
     INTO l_o_kit_ord.ord_stat, l_o_kit_ord.div_id, l_o_kit_ord.llr_dt, l_o_kit_ord.kit_typ, l_o_kit_ord.ord_typ,
          l_o_kit_ord.kit_item_num, l_o_kit_ord.cust_num, l_o_kit_ord.load_num, l_o_kit_ord.stop_num,
          l_o_kit_ord.eta_date, l_o_kit_ord.po_num, l_o_kit_ord.comp_item_num, l_o_kit_ord.comp_qty;

    EXIT WHEN i_cur%NOTFOUND;
    <<order_loop>>
    FOR l_r_ord IN l_cur_ord(l_o_kit_ord.ord_stat,
                             l_o_kit_ord.div_id,
                             DATE '1900-02-28' + l_o_kit_ord.llr_dt,
                             l_o_kit_ord.ord_typ,
                             l_o_kit_ord.cust_num,
                             l_o_kit_ord.load_num,
                             l_o_kit_ord.stop_num,
                             DATE '1900-02-28' + l_o_kit_ord.eta_date,
                             l_o_kit_ord.po_num,
                             l_o_kit_ord.comp_item_num,
                             l_o_kit_ord.comp_qty
                            ) LOOP
      l_o_kit_ord.order_num := l_r_ord.ordnob;
      l_o_kit_ord.order_ln := l_r_ord.lineb;
      l_o_kit_ord.item_num := l_r_ord.itemnb;
      l_o_kit_ord.uom := l_r_ord.sllumb;
      l_o_kit_ord.ord_qty := l_r_ord.qty;
      l_o_kit_ord.ratio := l_r_ord.ratio;
      l_o_kit_ord.seq := l_r_ord.seq;
      PIPE ROW(l_o_kit_ord);
    END LOOP order_loop;
  END LOOP kit_ord_loop;
  RETURN;
END kit_ord_fn;
/

