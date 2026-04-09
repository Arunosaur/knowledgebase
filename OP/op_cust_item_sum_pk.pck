CREATE OR REPLACE PACKAGE op_cust_item_sum_pk IS
  -- Author  : DLBEAL
  -- Created : 7/18/2013 8:20:30 AM
  -- Purpose : Customer Item Summary Extract process
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
  PROCEDURE extract_sp(
    i_div  VARCHAR2
  );
END op_cust_item_sum_pk;
/

CREATE OR REPLACE PACKAGE BODY op_cust_item_sum_pk IS
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
  || EXTRACT_SP
  ||  Create a Customer Item Summary Extract process
  ||----------------------------------------------------------------------------
  || CHANGELOG
  ||----------------------------------------------------------------------------
  || DATE     | USER ID | CHANGES
  ||----------------------------------------------------------------------------
  || 08/22/13 | dlbeal  | Original
  || 01/27/14 | rhalpai | Change logic to handle order lines tagged for Release.
  ||                    | IM-140558
  ||----------------------------------------------------------------------------
  */
  PROCEDURE extract_sp(
    i_div  VARCHAR2
  ) IS
    l_c_module    CONSTANT VARCHAR2(200)  := 'OP_CUST_ITEM_SUM_PK.EXTRACT_SP';
    lar_parm               logs.tar_parm;
    l_div_part             NUMBER;
    l_t_cus_itm_sum_corps  type_stab;
    l_c_sysdate   CONSTANT DATE           := SYSDATE;
    l_min_ord_rcvd_dt      DATE;
    l_c_file_dir  CONSTANT VARCHAR2(50)   := '/ftptrans';
    l_file_nm              VARCHAR2(40);
    l_zip_file_nm          VARCHAR2(40);
    l_rmt_file             VARCHAR2(40);
    l_t_rpt_lns            typ.tas_maxvc2;

    CURSOR l_cur_extr(
      b_div_part             NUMBER,
      b_min_ord_rcvd_dt      DATE,
      b_t_cus_itm_sum_corps  type_stab
    ) IS
      SELECT   cx.corpb AS crp_cd, cx.custb AS cust_id, cx.storeb AS store_num, b.orditb AS catlg_num,
               b.cusitb AS cust_item, SUM((CASE
                                             WHEN b.statb IN('O', 'I', 'S', 'P') THEN b.ordqtb
                                             ELSE b.pckqtb
                                           END)) AS qty
          FROM mclp020b cx, ordp100a a, ordp120b b, sawp505e e
         WHERE cx.div_part = b_div_part
           AND cx.corpb IN(SELECT TO_NUMBER(t.column_value)
                             FROM TABLE(CAST(b_t_cus_itm_sum_corps AS type_stab)) t)
           AND a.div_part = cx.div_part
           AND a.custa = cx.custb
           AND a.dsorda = 'R'
           AND a.stata NOT IN('A', 'C')
           AND a.ipdtsa NOT IN(SELECT s.ord_src
                                 FROM sub_prcs_ord_src s
                                WHERE s.div_part = b_div_part
                                  AND s.prcs_id = 'CUS_ITM_SUM'
                                  AND s.prcs_sbtyp_cd = 'BCI')
           AND TRUNC(a.ord_rcvd_ts) > b_min_ord_rcvd_dt
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb <> 'C'
           AND b.subrcb < 999
           AND (   b.excptn_sw = 'N'
                OR EXISTS(SELECT 1
                            FROM mclp140a x
                           WHERE x.rsncda = b.ntshpb
                             AND x.rsntpa = 99))
           AND e.iteme = b.itemnb
           AND e.uome = b.sllumb
      GROUP BY cx.corpb, cx.custb, cx.storeb, b.orditb, b.cusitb
      ORDER BY cx.corpb, cx.custb, cx.storeb, b.orditb, b.cusitb;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.dbg('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_t_cus_itm_sum_corps := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_cus_itm_sum);
    l_min_ord_rcvd_dt := TRUNC(l_c_sysdate - 7);
    l_file_nm := i_div || '_CUS_ITM_SUM_' || TO_CHAR(l_c_sysdate, 'YYYYMMDDHH24MISS');
    l_zip_file_nm := l_file_nm || '.zip';
    l_rmt_file := 'OP.CUSITMSM.ZIP';
    logs.dbg('Build Report Table');
    FOR l_r_extr IN l_cur_extr(l_div_part, l_min_ord_rcvd_dt, l_t_cus_itm_sum_corps) LOOP
      util.append(l_t_rpt_lns,
                  lpad_fn(l_r_extr.crp_cd, 3, '0')
                  || lpad_fn(l_r_extr.cust_id, 8, '0')
                  || lpad_fn(l_r_extr.store_num, 6, '0')
                  || lpad_fn(l_r_extr.catlg_num, 6, '0')
                  || lpad_fn(l_r_extr.cust_item, 10, '0')
                  || lpad_fn(l_r_extr.qty, 7, '0')
                 );
    END LOOP;
    logs.dbg('Write File');
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
    logs.dbg('Zip and FTE the File');
    fte_sp(i_div, l_file_nm, l_rmt_file, l_zip_file_nm, l_c_module);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END extract_sp;
END op_cust_item_sum_pk;
/

