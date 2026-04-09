CREATE OR REPLACE FUNCTION op_instock_data_fn(
  i_div          IN  VARCHAR2,
  i_crp_cd_list  IN  VARCHAR2
)
  RETURN BOOLEAN IS
  /*
  ||----------------------------------------------------------------------------
  ||  DESCRIPTION: This function takes Corp Code values enclosed in
  ||               flower-braces as parameter and generates an output
  ||               file in /ftptrans directory that contains all items
  ||               that were ordered by customers belonging to
  ||               these corp codes and the sums of quantities
  ||               ordered for each item.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 10/13/02 | SNAGABH | Initial Creation
  || 06/16/08 | rhalpai | Added sort by item to cursor. IM419804
  || 02/03/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert cursor to use fields from OrdHdr. PIR11038
  ||----------------------------------------------------------------------------
  */
  l_c_module    CONSTANT typ.t_maxfqnm  := 'OP_INSTOCK_DATA_FN';
  lar_parm               logs.tar_parm;
  l_is_success           BOOLEAN        := TRUE;
  l_c_file_dir  CONSTANT VARCHAR2(16)   := '/ftptrans';
  l_file_nm              VARCHAR2(30)   := i_div || '_Instock_Data.txt';
  l_t_rpt_lns            typ.tas_maxvc2;

  CURSOR l_cur_item_qty(
    b_div  VARCHAR2
  ) IS
    SELECT   b.orditb AS item, LPAD(SUM(b.ordqtb), 8, '0') AS qty
        FROM div_mstr_di1d d, mclp020b cx, ordp100a a, ordp120b b
       WHERE d.div_id = b_div
         AND cx.div_part = d.div_part
         AND INSTR(i_crp_cd_list, '{' || cx.corpb || '}') > 0
         AND a.div_part = cx.div_part
         AND a.custa = cx.custb
         AND a.dsorda = 'R'
         AND a.excptn_sw = 'N'
         AND b.div_part = a.div_part
         AND b.ordnob = a.ordnoa
         AND b.statb = 'O'
         AND b.excptn_sw = 'N'
         AND b.ntshpb IS NULL   -- ignore main line for substitutions
    GROUP BY b.orditb
    ORDER BY b.orditb;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'CrpCdList', i_crp_cd_list);
  logs.info('ENTRY', lar_parm);

  BEGIN
    FOR l_r_item_qty IN l_cur_item_qty(i_div) LOOP
      util.append(l_t_rpt_lns, l_r_item_qty.item || l_r_item_qty.qty);
    END LOOP;
    write_sp(l_t_rpt_lns, l_file_nm, l_c_file_dir);
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm, NULL, FALSE);
      l_is_success := FALSE;
  END;

  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  RETURN(l_is_success);
END op_instock_data_fn;
/

