CREATE OR REPLACE PACKAGE op_cleanup_pk IS
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
  /**
  ||----------------------------------------------------------------------------
  || Move rows from APP_LOG to log file that are older than keep_days.
  || #param i_keep_days        Number of days to keep in app_log
  ||                             NULL triggers parm value lookup
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE archive_app_log(
    i_keep_days  IN  NUMBER DEFAULT NULL
  );

  /**
  ||----------------------------------------------------------------------------
  || Assign distributions to loads for DistOnly Customers based on ShipDate.
  || #param i_div_part         DivPart
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE assign_dist_only_sp(
    i_div_part  IN  NUMBER,
    i_prcs_ts   IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || Moves an order to the history tables.
  || #param i_div_part         DivPart
  || #param i_ord_num          Order number
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE move_order_to_hist_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  );

  /**
  ||----------------------------------------------------------------------------
  || Moves data to the history tables.
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE move_to_hist_sp(
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || OP data purge routines.
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE delete_all_sp(
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || Process any old OPN msgs on the Get table.
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE mq_get_sp(
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || Unbilled order reassignment process.
  || For all orders on assigned loads with past llr date it moves to next
  || available load.
  || For regular orders on loads not assigned to customer with eta date less
  || than current date minus X (from ORDER_REASSIGN_DAYS in APPL_SYS_PARM_AP1S)
  || it bumps the date out to next week.  (ie: Wed of next week)
  || For non-Pxx distributions with past llr date it resets to DIST load.
  || For Pxx distributions with past eta date it resets to PxxP load.
  || #param i_div              Division ID ie: MW,NE,SW,etc.
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE reassign_orders_sp(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || Miscellaneous cleanup processes.  These include table updates.
  || #param i_div              Division ID ie: MW,NE,SW,etc.
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE misc_cleanup_sp(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  );

  /**
  ||----------------------------------------------------------------------------
  || Controlling procedure for cleanup process.
  || #param i_div              Division ID ie: MW,NE,SW,etc.
  || #param i_prcs_ts          Process timestamp
  ||----------------------------------------------------------------------------
  **/
  PROCEDURE main_sp(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  );
END op_cleanup_pk;
/

CREATE OR REPLACE PACKAGE BODY op_cleanup_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ASSIGN_DFLT_ORDS_SP
  ||  For all orders on DFLT load with assigned loads move to next available load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 05/22/08 | rhalpai | Original for IM403870
  || 03/01/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add DivPart parm. PIR11038
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  || 10/11/19 | rhalpai | Fix cursor to include orders on DFLT load for customers
  ||                    | having assigned non-test loads. SDHD-576230
  ||----------------------------------------------------------------------------
  */
  PROCEDURE assign_dflt_ords_sp(
    i_div_part  IN  NUMBER
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm          := 'OP_CLEANUP_PK.ASSIGN_DFLT_ORDS_SP';
    lar_parm                  logs.tar_parm;
    l_t_ord_nums              type_ntab;
    l_c_assign_dflt  CONSTANT mclp300d.reasnd%TYPE   := 'ASGNDFLT';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Get Orders');

    SELECT a.ordnoa
    BULK COLLECT INTO l_t_ord_nums
      FROM load_depart_op1f ld, ordp100a a, sysp200c c
     WHERE ld.div_part = i_div_part
       AND ld.load_num = 'DFLT'
       AND a.div_part = ld.div_part
       AND a.load_depart_sid = ld.load_depart_sid
       AND a.stata = 'O'
       AND c.div_part = a.div_part
       AND c.acnoc = a.custa
       AND c.statc IN('1', '3')
       AND EXISTS(SELECT 1
                    FROM mclp040d md, mclp120c mc
                   WHERE md.div_part = a.div_part
                     AND md.custd = a.custa
                     AND mc.div_part = md.div_part
                     AND mc.loadc = md.loadd
                     AND a.ldtypa =(CASE
                                      WHEN(mc.lbsgpc IN('Y', '1')) THEN DECODE(md.prod_typ,
                                                                               'GMP', 'GMP',
                                                                               'BTH', 'GMP',
                                                                               NULL
                                                                              )
                                      WHEN(md.prod_typ = 'BTH') THEN a.ldtypa
                                      ELSE md.prod_typ
                                    END
                                   )
                     AND mc.test_bil_load_sw = 'N');

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Call Syncload for Orders on DFLT Load');
      op_order_load_pk.syncload_sp(i_div_part, l_c_assign_dflt, l_t_ord_nums, 'CLEANUP');
    END IF;   -- l_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END assign_dflt_ords_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_ORDS_TO_HIST_SP
  ||  Common process to move orders to history tables.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/15/17 | rhalpai | Moved logic from MOVE_ORDER_TO_HIST_SP and MOVE_TO_HIST_SP.
  ||                    | SDHD-102466
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_ords_to_hist_sp(
    i_t_div_parts  IN  type_ntab,
    i_t_ord_nums   IN  type_ntab
  ) IS
    /*
    ||----------------------------------------------------------------------------
    || insert into ordp920b and delete from ordp120b
    || insert into ordp940c and delete from ordp140c
    || insert into mclp900d and delete from mclp300d
    || insert into sysp996a and delete from sysp296a
    || delete from gov_cntl_log_p680a
    || delete from bill_cntnr_id_bc1c
    || delete from bill_po_ovride_bc1p
    || insert into ordp900a and delete from ordp100a
    ||----------------------------------------------------------------------------
    */
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CLEANUP_PK.MOVE_ORDS_TO_HIST_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPartsTab', i_t_div_parts);
    logs.add_parm(lar_parm, 'OrdNumsTab', i_t_ord_nums);
    logs.info('ENTRY', lar_parm);

    IF i_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Move ORDP120B to ORDP920B');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        INSERT INTO ordp920b
                    (excptn_sw, statb, ordnob, lineb, itemnb, ordqtb, alcqtb, pckqtb, shpidb, orgqtb, sllumb, cusitb,
                     orditb, itpasb, rtfixb, prfixb, hdrtab, hdrtmb, hdprcb, actqtb, bymaxb, manctb, totctb, authb,
                     ntshpb, prstdb, prsttb, maxqtb, qtmulb, invctb, labctb, depdtb, deptmb, subrcb, repckb, orgitb,
                     div_part)
          SELECT b.excptn_sw, b.statb, b.ordnob, b.lineb, b.itemnb, b.ordqtb, b.alcqtb, b.pckqtb, b.shpidb, b.orgqtb,
                 b.sllumb, b.cusitb, b.orditb, b.itpasb, b.rtfixb, b.prfixb, b.hdrtab, b.hdrtmb, b.hdprcb, b.actqtb,
                 b.bymaxb, b.manctb, b.totctb, b.authb, b.ntshpb, b.prstdb, b.prsttb, b.maxqtb, b.qtmulb, b.invctb,
                 b.labctb, TRUNC(ld.depart_ts) - DATE '1900-02-28', TO_NUMBER(TO_CHAR(ld.depart_ts, 'HH24MI')),
                 b.subrcb, b.repckb, b.orgitb, b.div_part
            FROM ordp100a a, ordp120b b, load_depart_op1f ld
           WHERE a.div_part = i_t_div_parts(i)
             AND a.ordnoa = i_t_ord_nums(i)
             AND b.div_part = a.div_part
             AND b.ordnob = a.ordnoa
             AND ld.div_part(+) = a.div_part
             AND ld.load_depart_sid(+) = a.load_depart_sid;
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM ordp120b
              WHERE div_part = i_t_div_parts(i)
                AND ordnob = i_t_ord_nums(i);
      logs.dbg('Move ORDP140C to ORDP940C');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        INSERT INTO ordp940c
                    (div_part, statc, ordnoc, seqc, commc, prtc)
          SELECT div_part, statc, ordnoc, NVL(seqc, 0), commc, prtc
            FROM ordp140c
           WHERE div_part = i_t_div_parts(i)
             AND ordnoc = i_t_ord_nums(i);
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM ordp140c
              WHERE div_part = i_t_div_parts(i)
                AND ordnoc = i_t_ord_nums(i);
      logs.dbg('Move MCLP300D to MCLP900D');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        INSERT INTO mclp900d
                    (ordnod, ordlnd, reasnd, descd, exlvld, itemd, uomd, repitd, repumd, repsbd, qtyfrd, qtytod, resexd,
                     exdesd, resusd, resdtd, restmd, last_chg_ts, div_part)
          SELECT ordnod, ordlnd, reasnd, descd, exlvld, itemd, uomd, repitd, repumd, repsbd, qtyfrd, qtytod, resexd,
                 exdesd, resusd, resdtd, restmd, last_chg_ts, div_part
            FROM mclp300d
           WHERE div_part = i_t_div_parts(i)
             AND ordnod = i_t_ord_nums(i);
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM mclp300d
              WHERE div_part = i_t_div_parts(i)
                AND ordnod = i_t_ord_nums(i);
      logs.dbg('Move SYSP296A to SYSP996A');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        INSERT INTO sysp996a
                    (div_part, ordnoa, linea, usera, tblnma, fldnma, florga, flchga, actna, rsncda, datea, timea,
                     autbya, rsntxa)
          SELECT div_part, ordnoa, linea, usera, tblnma, fldnma, florga, flchga, actna, rsncda, datea, timea, autbya,
                 rsntxa
            FROM sysp296a
           WHERE div_part = i_t_div_parts(i)
             AND ordnoa = i_t_ord_nums(i);
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM sysp296a
              WHERE div_part = i_t_div_parts(i)
                AND ordnoa = i_t_ord_nums(i);
      logs.dbg('Delete old GOV_CNTL_LOG_P680A entries');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM gov_cntl_log_p680a a
              WHERE a.div_part = i_t_div_parts(i)
                AND a.ord_num = i_t_ord_nums(i);
      logs.dbg('Delete old BILL_CNTNR_ID_BC1C entries');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM bill_cntnr_id_bc1c bc
              WHERE bc.div_part = i_t_div_parts(i)
                AND bc.ord_num = i_t_ord_nums(i);
      logs.dbg('Delete old BILL_PO_OVRIDE_BC1P entries');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM bill_po_ovride_bc1p bp
              WHERE bp.div_part = i_t_div_parts(i)
                AND bp.ord_num = i_t_ord_nums(i);
      logs.dbg('Move ORDP100A to ORDP900A');
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        INSERT INTO ordp900a
                    (excptn_sw, ordnoa, custa, shpja, dsorda, pshipa, ldtypa, trndta, trntma, ipdtsa, cspasa, stata,
                     telsla, hdexpa, mntusa, connba, ctofda, ctofta, etadta, etatma, legrfa, uschga, orrtea, stopsa,
                     cpoa, ord_rcvd_ts, div_part)
          SELECT a.excptn_sw, a.ordnoa, a.custa, a.shpja, a.dsorda, a.pshipa, a.ldtypa, a.trndta, a.trntma, a.ipdtsa,
                 a.cspasa, a.stata, a.telsla, a.hdexpa, a.mntusa, a.connba, ld.llr_ts - DATE '1900-02-28',
                 TO_NUMBER(TO_CHAR(ld.llr_ts, 'HH24MI')), TRUNC(NVL(se.eta_ts, a.ord_rcvd_ts)) - DATE '1900-02-28',
                 TO_NUMBER(TO_CHAR(se.eta_ts, 'HH24MI')), a.legrfa, a.uschga, ld.load_num, se.stop_num, a.cpoa,
                 a.ord_rcvd_ts, a.div_part
            FROM ordp100a a, load_depart_op1f ld, stop_eta_op1g se
           WHERE a.div_part = i_t_div_parts(i)
             AND a.ordnoa = i_t_ord_nums(i)
             AND ld.div_part(+) = a.div_part
             AND ld.load_depart_sid(+) = a.load_depart_sid
             AND se.div_part(+) = a.div_part
             AND se.load_depart_sid(+) = a.load_depart_sid
             AND se.cust_id(+) = a.custa;
      FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
        DELETE FROM ordp100a
              WHERE div_part = i_t_div_parts(i)
                AND ordnoa = i_t_ord_nums(i);
    END IF;   -- i_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END move_ords_to_hist_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || ARCHIVE_APP_LOG
  ||  Move rows from APP_LOG to log file that are older than keep_days.
  ||  Passing NULL for keep_days triggers parm lookup for log days to keep.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/15/17 | rhalpai | Original.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE archive_app_log(
    i_keep_days  IN  NUMBER DEFAULT NULL
  ) IS
    l_keep_days     NUMBER;
    l_rows_deleted  NUMBER;
    l_msg           typ.t_maxvc2;
  BEGIN
    l_keep_days := NVL(i_keep_days, parm.get_val('LOG_KEEP_DAYS'));
    app_log_api.trim_table(l_rows_deleted, l_keep_days, 'day', 'Y', logs.get_log_nm);
    l_msg := 'Archived APP_LOG. Kept '
             || l_keep_days
             || ' days and moved '
             || l_rows_deleted
             || ' rows to '
             || logs.get_log_path;
    logs.info(l_msg);
    io.p(l_msg);
  END archive_app_log;

  /*
  ||----------------------------------------------------------------------------
  || ASSIGN_DIST_ONLY_SP
  ||  Assign distributions to loads for DistOnly Customers based on ShipDate.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/03/18 | rhalpai | Original for PIR18748
  || 11/01/18 | rhalpai | Add logic for parm offsets for shipdate at div and corp
  ||                    | levels where corp level overrides div level. PIR18748
  ||----------------------------------------------------------------------------
  */
  PROCEDURE assign_dist_only_sp(
    i_div_part  IN  NUMBER,
    i_prcs_ts   IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm          := 'OP_CLEANUP_PK.ASSIGN_DIST_ONLY_SP';
    lar_parm                  logs.tar_parm;
    l_ship_num                NUMBER;
    l_t_ord_nums              type_ntab;
    l_c_assign_dist  CONSTANT mclp300d.reasnd%TYPE   := 'ASGNDIST';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_ship_num := TRUNC(i_prcs_ts) - DATE '1900-02-28';
    logs.dbg('Get Orders');

    SELECT a.ordnoa
    BULK COLLECT INTO l_t_ord_nums
      FROM load_depart_op1f ld, ordp100a a, mclp020b cx, sysp200c c, mclp100a ma
     WHERE ld.div_part = i_div_part
       AND ld.load_num = 'DIST'
       AND a.div_part = ld.div_part
       AND a.load_depart_sid = ld.load_depart_sid
       AND a.stata = 'O'
       AND cx.div_part = a.div_part
       AND cx.custb = a.custa
       AND a.shpja
           + TO_NUMBER(COALESCE(op_parms_pk.val_fn(i_div_part, 'DIST_ONLY_OFFSET_' || LPAD(cx.corpb, 3, '0')),
                                op_parms_pk.val_fn(i_div_part, 'DIST_ONLY_DIV_OFFSET'),
                                '0'
                               )
                      ) <= l_ship_num
/*       AND a.shpja
           + COALESCE((SELECT p.intgr_val
                         FROM appl_sys_parm_ap1s p
                        WHERE p.div_part IN(0, cx.div_part)
                          AND p.parm_id = 'DIST_ONLY_OFFSET_' || LPAD(cx.corpb, 3, '0')),
                      (SELECT p.intgr_val
                         FROM appl_sys_parm_ap1s p
                        WHERE p.div_part IN(0, cx.div_part)
                          AND p.parm_id = 'DIST_ONLY_DIV_OFFSET'),
                      0
                     ) <= l_ship_num*/
       AND c.div_part = a.div_part
       AND c.acnoc = a.custa
       AND c.statc IN('1', '3')
       AND ma.div_part = c.div_part
       AND ma.cstgpa = c.retgpc
       AND ma.dist_only_sw = 'Y'
       AND EXISTS(SELECT 1
                    FROM mclp040d md, mclp120c mc
                   WHERE md.div_part = a.div_part
                     AND md.custd = a.custa
                     AND mc.div_part = md.div_part
                     AND mc.loadc = md.loadd
                     AND a.ldtypa =(CASE
                                      WHEN(mc.lbsgpc IN('Y', '1')) THEN DECODE(md.prod_typ,
                                                                               'GMP', 'GMP',
                                                                               'BTH', 'GMP',
                                                                               NULL
                                                                              )
                                      WHEN(md.prod_typ = 'BTH') THEN a.ldtypa
                                      ELSE md.prod_typ
                                    END
                                   )
                     AND mc.test_bil_load_sw = 'N');

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Call Syncload for Orders on DFLT Load');
      op_order_load_pk.syncload_sp(i_div_part, l_c_assign_dist, l_t_ord_nums, 'CLEANUP');
    END IF;   -- l_t_ord_nums.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END assign_dist_only_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_ORDER_TO_HIST_SP
  ||  Inserts an order to history tables and then deletes it.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/10/02 | rhalpai | Original.
  || 04/30/03 | rhalpai | Added SYSP296A/SYSP996A.
  || 08/12/05 | rhalpai | Changed error handler to new standard format. Removed
  ||                    | "Status" out parm. PIR2051
  || 12/19/06 | rhalpai | Added logic to clean up Container tables
  ||                    | (BILL_CNTNR_ID_BC1C and BILL_PO_OVRIDE_BC1P). PIR3209
  || 08/26/10 | rhalpai | Remove unused columns. Convert to use standard error
  ||                    | handling logic. PIR8531
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 07/10/12 | rhalpai | Change logic to use new EXCPTN_SW column.
  ||                    | Remove references to columns CONNUA, RESCDA, RETGPB,
  ||                    | DTEXPB, RESGPB, INVNOB, RSTFEB, SHPQTB, TICKTB.
  || 03/01/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  || 03/15/17 | rhalpai | Moved common logic to MOVE_ORDS_TO_HIST_SP and changed
  ||                    | to call it. SDHD-102466
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_order_to_hist_sp(
    i_div_part  IN  NUMBER,
    i_ord_num   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CLEANUP_PK.MOVE_ORDER_TO_HIST_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'OrdNum', i_ord_num);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Move Order to Hist');
    move_ords_to_hist_sp(type_ntab(i_div_part), type_ntab(i_ord_num));
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END move_order_to_hist_sp;

  /*
  ||----------------------------------------------------------------------------
  || MOVE_TO_HIST_SP
  ||  Inserts to history tables and then deletes.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/10/02 | rhalpai | Original conversion from OP_MOVE_TO_HIST_AND_DELETE_SP.
  || 05/13/03 | rhalpai | Added logic to clean up Government Control Log table
  || 08/12/05 | rhalpai | Changed error handler to new standard format. Removed
  ||                    | "Status" out parm. PIR2051
  || 12/19/06 | rhalpai | Added logic to clean up Container tables
  ||                    | (BILL_CNTNR_ID_BC1C and BILL_PO_OVRIDE_BC1P).
  ||                    | Added logic to clean up LOAD_CLOS_CNTRL_BC2C as
  ||                    | corresponding rows from MCLP370C are removed. PIR3209
  || 03/05/07 | rhalpai | Removed logic to clean up BILL_PO_OVRIDE_BC1P. IM290595
  || 03/16/10 | rhalpai | Changed logic to include moving new transaction type 99
  ||                    | (avail inventory at allocation) from WHSP200R to
  ||                    | WHSP900R. PIR0024
  || 08/26/10 | rhalpai | Remove unused columns. Convert to use standard error
  ||                    | handling logic. PIR8531
  || 11/10/10 | rhalpai | Remove unused columns. PIR5878
  || 08/29/11 | rhalpai | Removed logic to move transactions to history. PIR7990
  || 07/10/12 | rhalpai | Change logic to use new EXCPTN_SW column.
  ||                    | Remove references to columns CONNUA, RESCDA, RETGPB,
  ||                    | DTEXPB, RESGPB, INVNOB, RSTFEB, SHPQTB, TICKTB.
  || 03/01/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  || 01/15/14 | rhalpai | Change logic to include cancelled orders regardless of
  ||                    | ETA. IM-137804
  || 08/15/14 | rhalpai | Change purge logic for MCLP370C/LOAD_CLOS_CNTRL_BC2C to
  ||                    | only clear LLR dates less than current date. IM-198067
  || 01/13/16 | rhalpai | Add logic to remove and matching entries from
  ||                    | LOAD_CUBE_PLAN_EXTR_OP1E when removing from
  ||                    | LOAD_CLOS_CNTRL_BC2C. PIR15617
  || 03/15/17 | rhalpai | Add ProcessTs input parm defaulted to SYSDATE to allow
  ||                    | for SYSDATE override.
  ||                    | Change to use constants package OP_CONST_PK.
  ||                    | Change to call new OP_PARMS_PK.VAL_FN.
  ||                    | Moved common logic to MOVE_ORDS_TO_HIST_SP and changed
  ||                    | to call it. SDHD-102466
  ||----------------------------------------------------------------------------
  */
  PROCEDURE move_to_hist_sp(
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module       CONSTANT typ.t_maxfqnm          := 'OP_CLEANUP_PK.MOVE_TO_HIST_SP';
    lar_parm                  logs.tar_parm;
    l_move_hist_days          PLS_INTEGER;
    l_rlse_ts_char            ordp100a.uschga%TYPE;
    l_eta_dt                  DATE;
    l_c_fetch_limit  CONSTANT NUMBER                 := 1000000;
    l_cv                      SYS_REFCURSOR;
    l_t_div_parts             type_ntab;
    l_t_ord_nums              type_ntab;
    l_t_div_load_llrs         type_stab;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_move_hist_days := op_parms_pk.val_fn(0, op_const_pk.prm_mov_to_hist_days);
    l_rlse_ts_char := TO_CHAR((i_prcs_ts - l_move_hist_days), 'YYYYMMDDHH24MISS');
    l_eta_dt := TRUNC(i_prcs_ts) - l_move_hist_days;
    logs.dbg('Open Archive Orders Cursor');

    OPEN l_cv
     FOR
       SELECT x.div_part, x.ord_num
         FROM (SELECT a.div_part, a.ordnoa AS ord_num
                 FROM ordp100a a
                WHERE a.stata = 'A'
                  AND a.uschga BETWEEN '20000101000000' AND l_rlse_ts_char
               UNION
               SELECT a.div_part, a.ordnoa AS ord_num
                 FROM ordp100a a
                WHERE a.stata = 'C'
               UNION
               SELECT a.div_part, a.ordnoa AS ord_num
                 FROM stop_eta_op1g se, ordp100a a
                WHERE TRUNC(se.eta_ts) < l_eta_dt
                  AND a.div_part = se.div_part
                  AND a.load_depart_sid = se.load_depart_sid
                  AND a.custa = se.cust_id
                  AND a.dsorda = 'N') x;

    logs.dbg('Move Orders to Hist');
    LOOP
      FETCH l_cv
      BULK COLLECT INTO l_t_div_parts, l_t_ord_nums LIMIT l_c_fetch_limit;

      EXIT WHEN l_t_ord_nums.COUNT = 0;
      move_ords_to_hist_sp(l_t_div_parts, l_t_ord_nums);
      COMMIT;
    END LOOP;
    -- free memory
    l_t_div_parts := NULL;
    l_t_ord_nums := NULL;
    logs.dbg('Delete old MCLP370C entries');

    DELETE FROM mclp370c c
          WHERE c.load_status = 'A'
            AND c.last_ts_chg <(i_prcs_ts - l_move_hist_days)
            AND c.llr_date <(TRUNC(i_prcs_ts) - DATE '1900-02-28')
      RETURNING       LPAD(c.div_part, 3, '0') || c.loadc || c.llr_date
    BULK COLLECT INTO l_t_div_load_llrs;

    IF     l_t_div_load_llrs IS NOT NULL
       AND l_t_div_load_llrs.COUNT > 0 THEN
      logs.dbg('Remove Duplicates');
      l_t_div_load_llrs := SET(l_t_div_load_llrs);
      logs.dbg('Delete old LOAD_CLOS_CNTRL_BC2C entries');
      FORALL i IN l_t_div_load_llrs.FIRST .. l_t_div_load_llrs.LAST
        DELETE FROM load_clos_cntrl_bc2c l
              WHERE l.div_part = TO_NUMBER(SUBSTR(l_t_div_load_llrs(i), 1, 3))
                AND l.load_num = SUBSTR(l_t_div_load_llrs(i), 4, 4)
                AND l.llr_dt = DATE '1900-02-28' + TO_NUMBER(SUBSTR(l_t_div_load_llrs(i), 8));
      logs.dbg('Delete old LOAD_CUBE_PLAN_EXTR_OP1E entries');
      FORALL i IN l_t_div_load_llrs.FIRST .. l_t_div_load_llrs.LAST
        DELETE FROM load_cube_plan_extr_op1e l
              WHERE l.div_part = TO_NUMBER(SUBSTR(l_t_div_load_llrs(i), 1, 3))
                AND l.load_num = SUBSTR(l_t_div_load_llrs(i), 4, 4)
                AND l.llr_dt = DATE '1900-02-28' + TO_NUMBER(SUBSTR(l_t_div_load_llrs(i), 8));
    END IF;   -- l_t_div_load_llrs IS NOT NULL AND l_t_div_load_llrs.COUNT > 0

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END move_to_hist_sp;

  /*
  ||----------------------------------------------------------------------------
  || DELETE_ALL_SP
  ||  OP data purge routines for the following tables:
  ||  - MCLP240B
  ||  - ORDP940C
  ||  - MCLP900D
  ||  - ORDP920B
  ||  - ORDP900A
  ||  - SYSP296A
  ||  - MCLANE_MQ_PUT
  ||  - MCLANE_MQ_GET
  ||  - MCLANE_ORDER_RECEIPT_STATUS
  ||  - RLSE_OP1Z
  ||  - RLSE_LOG_OP2Z
  ||  - TRAN_OP2T
  ||  - TRAN_ORD_OP2O
  ||  - TRAN_ITEM_OP2I
  ||  - TRAN_STAMP_OP2C
  ||  - MCLANE_MANIFEST_RPTS
  ||  - SYSP996A
  ||  - PREPOST_LOAD_OP1P
  ||  - GOV_CNTL_CUST_P640A
  ||  - CARE_PKG_ORD_CP1C
  ||  - BULK_OUT_BO1O
  ||  - RATION_ITEM_LOG_RL1I
  ||  - BUNDL_DIST_ITEM_BD1I
  ||  - BUNDL_DIST_CUST_BD1C
  ||  - BILL_PO_OVRIDE_BC1P
  ||  - RTE_STAT_RT1S
  ||  - CUST_AUTO_RTE_RT2C
  ||  - CUST_RTE_OVRRD_RT3C
  ||  - RTE_GRP_RT2G
  ||  - RTE_GRP_ORD_RT3O
  ||  - SQL_UTILITIES
  ||  - SPLIT_ORD_OP2S
  ||  - STRCT_ORD_OP1O
  ||  - EVNT_TRAN_EV1T
  ||  - EVNT_MSTR_EV1M
  ||  - WKLY_MAX_LOG_OP3M
  ||  - VNDR_CMP_CUST_OP2L
  ||  - VNDR_CMP_QTY_OP4L
  ||  - LOAD_DEPART_OP1F
  ||  - LOAD_LOG_OP3Z
  ||  - LOAD_RSN_OP3R
  ||  - DIST_DEL_RECYCL_OP4R
  ||  - CPY_ORD_CP3O
  ||  - REROUTE_LOG_RT2R
  ||  - CUST_RTE_REQ_OP4C
  ||  - CUST_DIST_RTE_REQ_OP5C
  ||  - CUST_DIST_RTE_EXTR_OP6C
  ||  - BILL_CNTNR_ID_BC1C
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/10/02 | rhalpai | Original conversion from OP_DELETE_ALL_SP
  || 04/30/03 | rhalpai | Added delete of SYSP996A. (Only cancelled orders were
  ||                    | previously deleted)
  || 05/13/03 | rhalpai | Added logic to clean up Government Control Customer table
  || 08/12/05 | rhalpai | Added logic to clean up CarePkg process tables
  ||                    | (CARE_PCKG_OP_CP2O, BULK_OUT_BO1O) PIR2608
  ||                    | Added logic to clean up RATION_ITEM_LOG_RL1I PIR1289
  ||                    | Changed error handler to new standard format. Removed
  ||                    | "Status" out parm. PIR2051
  || 05/04/05 | rhalpai | Added logic to clean up Bundle Dist tables
  || 03/05/07 | rhalpai | Added logic to clean up BILL_PO_OVRIDE_BC1P using parm.
  ||                    | IM290595
  || 07/30/07 | rhalpai | Added logic to clean up RTE_STAT_RT1S and SQL_UTILITIES
  ||                    | using parms. PIR3643
  || 11/08/07 | rhalpai | Added logic to clean up HOLDY_MSTR_OP1H and
  ||                    | SPLIT_ORD_OP2S, STRCT_ORD_OP1O using parms. PIR5002
  || 02/27/08 | rhalpai | Added logic to clean up EVNT_TRAN_EV1T and EVNT_MSTR_EV1M
  ||                    | using parm. PIR3593
  || 04/02/08 | rhalpai | Added logic to clean up RTE_GRP_ORD_RT3O and RTE_GRP_RT2G.
  ||                    | PIR5882
  || 08/11/08 | rhalpai | Removed logic for HOLDY_MSTR_OP1H as it is no longer
  ||                    | used. Changed cleanup of MCLP900D to use LAST_CHG_TS to
  ||                    | determine rows to remove.
  || 04/13/09 | rhalpai | Added logic to delete entries from WAVE_PLAN_LOAD_OP2W
  ||                    | for past LLR_DT entries. PIR7118
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. Convert to use
  ||                    | standard error handling logic. PIR8531
  || 11/19/10 | rhalpai | Replace logic for removing entries from CARE_PCKG_OP_CP2O
  ||                    | with logic to remove entries from CARE_PKG_ORD_CP1C
  ||                    | when the order no longer exists in ORDP100A or WHSP100A.
  ||                    | PIR5152
  || 03/14/11 | rhalpai | Added logic to remove entries from CUST_AUTO_RTE_RT2C
  ||                    | and CUST_RTE_OVRRD_RT3C when no open order exists for
  ||                    | matching Div/Cust/LLRDate. Changed logic to also
  ||                    | require non-existence of entry in CUST_AUTO_RTE_RT2C
  ||                    | for delete of RTE_GRP_RT2G. Removed unused tables.
  ||                    | PIR9348
  || 06/21/11 | rhalpai | Changed logic to remove entries from CUST_RTE_OVRRD_RT3C
  ||                    | when no open order exists for matching
  ||                    | Div/Cust/LLRDate/Load/Stop/DepartTs/EtaTs. PIR9348
  || 07/05/11 | rhalpai | Added logic to remove entries from Weekly Max Log.
  ||                    | PIR6235
  || 08/29/11 | rhalpai | Removed logic to delete transactions since this will be
  ||                    | performed via cascaded delete from RLSE_OP1Z. Add logic
  ||                    | to delete Item Adjustment transactions (TranTyp=4).
  ||                    | PIR7990
  || 02/29/12 | rhalpai | Add logic to remove entries from VNDR_CMP_CUST_OP2L and
  ||                    | VNDR_CMP_QTY_OP4L. PIR6682
  || 04/24/12 | dlbeal  | Add logic to remove LOAD_DEPART_OP1F entries where
  ||                    | LLR_DT > '1900-01-01' and no matching entry exists for
  ||                    | an order.
  || 09/19/12 | rhalpai | Add logic to remove entries from LOAD_LOG_OP3Z,
  ||                    | LOAD_RSN_OP3R. PIR10251
  || 10/01/12 | rhalpai | Add logic to remove entries from DIST_DEL_RECYCL_OP4R
  ||                    | older than DIST_DEL_RECYCL_DAYS without matching order
  ||                    | entries. PIR5250
  || 11/29/12 | rhalpai | Change logic for DIST_DEL_RECYCL_OP4R to use new
  ||                    | ORD_NUM column. IM-074192
  || 03/01/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F,OP1G. PIR11038
  || 12/09/13 | rhalpai | Change logic to only remove rows from LOAD_DEPART_OP1F
  ||                    | with LLR older than 7 days. IM-128579
  || 03/15/17 | rhalpai | Add ProcessTs input parm defaulted to SYSDATE to allow
  ||                    | for SYSDATE override.
  ||                    | Change to call new OP_PARMS_PK.IDX_VALS_FN.
  ||                    | Added logic to remove entries from REROUTE_LOG_RT2R.
  ||                    | SDHD-102466
  || 04/05/17 | rhalpai | Change logic for removal of RLSE_OP1Z entries to
  ||                    | process one div at a time and manually remove the
  ||                    | related child entries from TRAN_OP2T. SDHD-111273
  || 01/17/18 | rhalpai | Change logic to exclude DELDIST when removing orphan
  ||                    | entries from MCLP900D. PIR16403
  || 12/08/18 | rhalpai | Add logic to remove entries from CUST_RTE_REQ_OP4C. PIR18901
  || 06/12/19 | rhalpai | Change logic to clear entries in LOAD_LOG_OP3Z and
  ||                    | LOAD_RSN_OP3R based on new LOAD_LOG_DAYS parm. PIR18852
  || 01/11/22 | rhalpai | Add logic to remove entries from CUST_DIST_RTE_REQ_OP5C and CUST_DIST_RTE_EXTR_OP6C. PIR18901
  || 05/16/25 | rhalpai | Add logic to remove old/orphan entries from BILL_CNTNR_ID_BC1C. SDHD-2257633
  ||----------------------------------------------------------------------------
  */
  PROCEDURE delete_all_sp(
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module              CONSTANT typ.t_maxfqnm             := 'OP_CLEANUP_PK.DELETE_ALL_SP';
    lar_parm                         logs.tar_parm;
    l_c_trunc_prcs_ts       CONSTANT DATE                      := TRUNC(i_prcs_ts);
    l_c_commit_cnt          CONSTANT PLS_INTEGER               := 100000;
    l_c_mclp240b_days       CONSTANT PLS_INTEGER               := 2;
    l_c_ord_rcpt_stat_days  CONSTANT PLS_INTEGER               := 1;
    l_c_mfst_rpts_days      CONSTANT PLS_INTEGER               := 1;
    l_c_sysp996a_days       CONSTANT PLS_INTEGER               := 70;
    l_c_prepost_load_days   CONSTANT PLS_INTEGER               := 7;
    l_c_vndr_cmp_days       CONSTANT PLS_INTEGER               := 7;
    l_c_load_depart_days    CONSTANT PLS_INTEGER               := 7;
    l_t_parms                        op_types_pk.tt_varchars_v;
    l_mclp240b_deadline              DATE;
    l_del_all_deadline               DATE;
    l_mq_put_deadline                DATE;
    l_mq_get_deadline                DATE;
    l_ord_rcpt_stat_deadline         DATE;
    l_rlse_deadline                  DATE;
    l_mfst_rpts_deadline             DATE;
    l_sysp996a_deadline              DATE;
    l_prepost_load_deadline          DATE;
    l_gov_cntl_cust_deadline         DATE;
    l_ration_log_deadline            DATE;
    l_bill_po_ovride_deadline        DATE;
    l_cust_auto_rte_deadline         DATE;
    l_cust_rte_ovrrd_deadline        DATE;
    l_rte_grp_deadline               DATE;
    l_cust_rte_req_deadline          DATE;
    l_cust_dist_rte_req_deadline     DATE;
    l_cust_dist_rte_extr_deadline    DATE;
    l_sql_utilities_deadline         DATE;
    l_split_ord_deadline             DATE;
    l_strict_ord_deadline            DATE;
    l_evnt_deadline                  DATE;
    l_wave_plan_load_deadline        DATE;
    l_vndr_cmp_deadline              DATE;
    l_load_depart_deadline           DATE;
    l_dist_del_recycl_deadline       DATE;
    l_addl_cntnr_purge_deadline      DATE;
    l_reroute_log_deadline           DATE;
    l_load_log_deadline              DATE;

    FUNCTION parm_fn(
      i_parm_idx  IN  VARCHAR2,
      i_dflt_val  IN  NUMBER DEFAULT 0
    )
      RETURN PLS_INTEGER IS
    BEGIN
      RETURN(NVL(TO_NUMBER(l_t_parms(i_parm_idx)), i_dflt_val));
    END parm_fn;

    PROCEDURE del_mclp240b_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      LOOP
        DELETE FROM mclp240b
              WHERE statb = 'R'
                AND last_chg_ts < i_deadline
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_mclp240b_sp;

    PROCEDURE del_ords_sp(
      i_t_div_parts  IN  type_ntab,
      i_t_ord_nums   IN  type_ntab
    ) IS
    BEGIN
      IF i_t_ord_nums.COUNT > 0 THEN
        logs.dbg('Deleting ORDP940C');
        LOOP
          FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
            DELETE FROM ordp940c c
                  WHERE c.div_part = i_t_div_parts(i)
                    AND c.ordnoc = i_t_ord_nums(i)
                    AND ROWNUM <= l_c_commit_cnt;
          EXIT WHEN SQL%ROWCOUNT = 0;
          COMMIT;
        END LOOP;
        logs.dbg('Deleting MCLP900D');
        LOOP
          FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
            DELETE FROM mclp900d d
                  WHERE d.div_part = i_t_div_parts(i)
                    AND d.ordnod = i_t_ord_nums(i)
                    AND ROWNUM <= l_c_commit_cnt;
          EXIT WHEN SQL%ROWCOUNT = 0;
          COMMIT;
        END LOOP;
        logs.dbg('Deleting ORDP920B');
        LOOP
          FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
            DELETE FROM ordp920b b
                  WHERE b.div_part = i_t_div_parts(i)
                    AND b.ordnob = i_t_ord_nums(i)
                    AND ROWNUM <= l_c_commit_cnt;
          EXIT WHEN SQL%ROWCOUNT = 0;
          COMMIT;
        END LOOP;
        logs.dbg('Deleting ORDP900A');
        LOOP
          FORALL i IN i_t_ord_nums.FIRST .. i_t_ord_nums.LAST
            DELETE FROM ordp900a
                  WHERE div_part = i_t_div_parts(i)
                    AND ordnoa = i_t_ord_nums(i)
                    AND ROWNUM <= l_c_commit_cnt;
          EXIT WHEN SQL%ROWCOUNT = 0;
          COMMIT;
        END LOOP;
      END IF;   -- i_t_ord_nums.COUNT > 0
    END del_ords_sp;

    PROCEDURE del_orders_sp(
      i_deadline  IN  DATE
    ) IS
      l_t_div_parts  type_ntab;
      l_t_ord_nums   type_ntab;
    BEGIN
      SELECT div_part, ordnoa
      BULK COLLECT INTO l_t_div_parts, l_t_ord_nums
        FROM ordp900a a
       WHERE GREATEST(DATE '1900-02-28' + a.etadta, TRUNC(a.ord_rcvd_ts)) <= i_deadline;

      IF l_t_ord_nums.COUNT > 0 THEN
        logs.dbg('Deleting Orders');
        del_ords_sp(l_t_div_parts, l_t_ord_nums);
      END IF;   -- l_t_ord_nums.COUNT > 0
    END del_orders_sp;

    PROCEDURE del_sysp296a_sp(
      i_deadline  IN  DATE
    ) IS
      l_deadline  NUMBER := i_deadline - DATE '1900-02-28';
    BEGIN
      LOOP
        DELETE FROM sysp296a
              WHERE datea <= l_deadline
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_sysp296a_sp;

    PROCEDURE del_sysp996a_sp(
      i_deadline  IN  DATE
    ) IS
      l_deadline  NUMBER := i_deadline - DATE '1900-02-28';
    BEGIN
      -- exclude cancelled orders from delete since they are handled separately
      LOOP
        DELETE FROM sysp996a
              WHERE datea <= l_deadline
                AND rsncda NOT LIKE 'RCAN0%'
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_sysp996a_sp;

    PROCEDURE del_mclp900d_deldist_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      LOOP
        DELETE FROM mclp900d
              WHERE reasnd = 'DELDIST'
                AND TRUNC(last_chg_ts) <= i_deadline
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_mclp900d_deldist_sp;

    PROCEDURE del_mclane_mq_put_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      LOOP
        DELETE FROM mclane_mq_put
              WHERE last_chg_ts < i_deadline
                AND mq_msg_status = 'CMP'
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_mclane_mq_put_sp;

    PROCEDURE del_mclane_mq_get_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      LOOP
        DELETE FROM mclane_mq_get
              WHERE last_chg_ts < i_deadline
                AND mq_msg_status = 'CMP'
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_mclane_mq_get_sp;

    PROCEDURE del_mclane_order_receipt_st_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      LOOP
        DELETE FROM mclane_order_receipt_status
              WHERE create_ts < i_deadline
                AND msg_status = 'C'
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_mclane_order_receipt_st_sp;

    PROCEDURE del_rlse_op1z_sp(
      i_deadline  IN  DATE
    ) IS
      l_t_div_parts  type_ntab;
      l_t_rlse_ids   type_ntab;
    BEGIN
      SELECT d.div_part
      BULK COLLECT INTO l_t_div_parts
        FROM div_mstr_di1d d
       WHERE d.div_part > 0;

      FOR i IN l_t_div_parts.FIRST .. l_t_div_parts.LAST LOOP
        SELECT r.rlse_id
        BULK COLLECT INTO l_t_rlse_ids
          FROM rlse_op1z r
         WHERE r.div_part = l_t_div_parts(i)
           AND r.rlse_ts <> DATE '1900-01-01'
           AND (   (    r.end_ts < i_deadline
                    AND r.stat_cd = 'R')
                OR (    r.rlse_ts < i_deadline
                    AND NOT EXISTS(SELECT 1
                                     FROM mclane_manifest_rpts rpts
                                    WHERE rpts.div_part = r.div_part
                                      AND rpts.create_ts = r.rlse_ts)
                   )
               );

        IF l_t_rlse_ids.COUNT > 0 THEN
          DELETE FROM tran_op2t t
                WHERE t.div_part = l_t_div_parts(i)
                  AND t.rlse_id IN(SELECT t.column_value
                                     FROM TABLE(l_t_rlse_ids) t);

          COMMIT;

          DELETE FROM rlse_log_op2z rl
                WHERE rl.div_part = l_t_div_parts(i)
                  AND rl.rlse_id IN(SELECT t.column_value
                                      FROM TABLE(l_t_rlse_ids) t);

          DELETE FROM rlse_op1z r
                WHERE r.div_part = l_t_div_parts(i)
                  AND r.rlse_id IN(SELECT t.column_value
                                     FROM TABLE(l_t_rlse_ids) t);

          COMMIT;
        END IF;   -- l_t_rlse_ids.COUNT > 0

        DELETE FROM tran_op2t t
              WHERE t.div_part = l_t_div_parts(i)
                AND t.tran_typ = 4
                AND t.create_ts < i_deadline;

        COMMIT;
      END LOOP;
    END del_rlse_op1z_sp;

    PROCEDURE del_mclane_manifest_rpts_sp(
      i_deadline  IN  DATE
    ) IS
      l_deadline  NUMBER := i_deadline - DATE '1900-02-28';
    BEGIN
      LOOP
        DELETE FROM mclane_manifest_rpts a
              WHERE a.departure_date < l_deadline
                AND NOT EXISTS(SELECT 1
                                 FROM mclp370c c
                                WHERE a.div_part = c.div_part
                                  AND a.load_num = c.loadc
                                  AND a.departure_date = c.depdtc)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_mclane_manifest_rpts_sp;

    PROCEDURE del_canceled_ord_sp(
      i_deadline  IN  DATE
    ) IS
      l_deadline     NUMBER    := i_deadline - DATE '1900-02-28';
      l_t_div_parts  type_ntab;
      l_t_ord_nums   type_ntab;
    BEGIN
      SELECT div_part, ordnoa
      BULK COLLECT INTO l_t_div_parts, l_t_ord_nums
        FROM sysp996a
       WHERE rsncda LIKE 'RCAN0%'
         AND actna = 'C'
         AND datea < l_deadline;

      IF l_t_ord_nums.COUNT > 0 THEN
        del_ords_sp(l_t_div_parts, l_t_ord_nums);
        LOOP
          FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
            DELETE FROM sysp996a
                  WHERE div_part = l_t_div_parts(i)
                    AND ordnoa = l_t_ord_nums(i)
                    AND ROWNUM <= l_c_commit_cnt;
          EXIT WHEN SQL%ROWCOUNT = 0;
          COMMIT;
        END LOOP;
      END IF;   -- l_t_ord_nums.COUNT > 0
    END del_canceled_ord_sp;

    PROCEDURE del_prepost_load_op1p_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM prepost_load_op1p
            WHERE last_chg_ts < i_deadline;

      COMMIT;
    END del_prepost_load_op1p_sp;

    PROCEDURE del_gov_cntl_cust_p640a_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM gov_cntl_cust_p640a p640a
            WHERE p640a.prd_beg_ts < i_deadline
              AND (   (p640a.status = 2)
                   OR (p640a.gov_cntl_id, p640a.cust_num, p640a.prd_beg_ts) IN(
                        SELECT a1.gov_cntl_id, a1.cust_num, a1.prd_beg_ts
                          FROM gov_cntl_cust_p640a a1
                         WHERE a1.gov_cntl_id = p640a.gov_cntl_id
                           AND a1.div_part = p640a.div_part
                           AND a1.cust_num = p640a.cust_num
                           AND a1.prd_beg_ts < (SELECT MAX(a2.prd_beg_ts)
                                                  FROM gov_cntl_cust_p640a a2
                                                 WHERE a2.gov_cntl_id = a1.gov_cntl_id
                                                   AND a2.div_part = a1.div_part
                                                   AND a2.cust_num = a1.cust_num
                                                   AND a2.status < 2))
                  );

      COMMIT;
    END del_gov_cntl_cust_p640a_sp;

    PROCEDURE del_care_pkg_ord_cp1c_sp IS
    BEGIN
      DELETE FROM care_pkg_ord_cp1c cpo
            WHERE cpo.ord_num > 0
              AND NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = cpo.div_part
                                AND a.ordnoa = cpo.ord_num);

      COMMIT;
    END del_care_pkg_ord_cp1c_sp;

    PROCEDURE del_bulk_out_bo1o_sp IS
    BEGIN
      LOOP
        DELETE FROM bulk_out_bo1o bo1o
              WHERE NOT EXISTS(SELECT 1
                                 FROM ordp100a a
                                WHERE a.div_part = bo1o.div_part
                                  AND a.ordnoa = bo1o.ord_num)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_bulk_out_bo1o_sp;

    PROCEDURE del_ration_item_log_rl1i_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM ration_item_log_rl1i r
            WHERE r.release_ts < i_deadline;

      COMMIT;
    END del_ration_item_log_rl1i_sp;

    PROCEDURE del_bundl_dist_item_bd1i_sp IS
    BEGIN
      DELETE FROM bundl_dist_item_bd1i bi
            WHERE ROWID NOT IN(SELECT ROWID
                                 FROM bundl_dist_item_bd1i bi2
                                WHERE EXISTS(SELECT 1
                                               FROM ordp100a a, ordp120b b
                                              WHERE b.div_part = bi2.div_part
                                                AND b.itemnb = bi2.item_num
                                                AND b.sllumb = bi2.unq_cd
                                                AND b.subrcb = 0
                                                AND a.div_part = b.div_part
                                                AND a.ordnoa = b.ordnob
                                                AND SUBSTR(a.legrfa, 1, 10) = bi2.dist_id
                                                AND SUBSTR(a.legrfa, 12, 2) = LPAD(bi2.dist_sfx, 2, '0')
                                                AND a.dsorda = 'D'));

      COMMIT;
    END del_bundl_dist_item_bd1i_sp;

    PROCEDURE del_bundl_dist_cust_bd1c_sp IS
    BEGIN
      DELETE FROM bundl_dist_cust_bd1c bc
            WHERE NOT EXISTS(SELECT 1
                               FROM ordp100a oa
                              WHERE oa.div_part = bc.div_part
                                AND SUBSTR(oa.legrfa, 1, 10) = bc.dist_id
                                AND SUBSTR(oa.legrfa, 12, 2) = LPAD(bc.dist_sfx, 2, '0')
                                AND oa.custa = bc.cust_id);

      COMMIT;
    END del_bundl_dist_cust_bd1c_sp;

    PROCEDURE del_bill_po_ovride_bc1p_sp(
      i_deadline  IN  DATE
    ) IS
      l_deadline_char  VARCHAR2(14);
    BEGIN
      l_deadline_char := TO_CHAR(i_deadline, 'YYYYMMDDHH24MISS');

      DELETE FROM bill_po_ovride_bc1p bp
            WHERE EXISTS(SELECT 1
                           FROM ordp900a a
                          WHERE a.div_part = bp.div_part
                            AND a.ordnoa = bp.ord_num
                            AND a.uschga < l_deadline_char);

      COMMIT;

      -- remove any orphans
      DELETE FROM bill_po_ovride_bc1p bp
            WHERE NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = bp.div_part
                                AND a.ordnoa = bp.ord_num)
              AND NOT EXISTS(SELECT 1
                               FROM ordp900a a
                              WHERE a.div_part = bp.div_part
                                AND a.ordnoa = bp.ord_num);

      COMMIT;
    END del_bill_po_ovride_bc1p_sp;

    PROCEDURE del_cust_auto_rte_rt2c_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM cust_auto_rte_rt2c car
            WHERE EXISTS(SELECT 1
                           FROM rte_grp_rt2g rg
                          WHERE rg.div_part = car.div_part
                            AND rg.rte_grp_num = car.rte_grp_num)
              AND car.llr_dt < i_deadline
              AND NOT EXISTS(SELECT 1
                               FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                              WHERE ld.div_part = car.div_part
                                AND ld.llr_dt = car.llr_dt
                                AND ld.load_num IN(car.load_num, car.new_load)
                                AND se.div_part = ld.div_part
                                AND se.load_depart_sid = ld.load_depart_sid
                                AND se.cust_id = car.cust_id
                                AND a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id);

      COMMIT;
    END del_cust_auto_rte_rt2c_sp;

    PROCEDURE del_cust_rte_ovrrd_rt3c_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM cust_rte_ovrrd_rt3c cro
            WHERE cro.llr_dt < i_deadline
              AND NOT EXISTS(SELECT 1
                               FROM load_depart_op1f ld, stop_eta_op1g se, ordp100a a
                              WHERE ld.div_part = cro.div_part
                                AND ld.llr_dt = cro.llr_dt
                                AND ld.load_num = cro.load_num
                                AND ld.depart_ts = cro.depart_ts
                                AND se.div_part = ld.div_part
                                AND se.load_depart_sid = ld.load_depart_sid
                                AND se.cust_id = cro.cust_id
                                AND se.stop_num = cro.stop_num
                                AND se.eta_ts = cro.eta_ts
                                AND a.div_part = se.div_part
                                AND a.load_depart_sid = se.load_depart_sid
                                AND a.custa = se.cust_id);

      COMMIT;
    END del_cust_rte_ovrrd_rt3c_sp;

    PROCEDURE del_rte_grp_rt2g_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM rte_grp_rt2g g
            WHERE NOT EXISTS(SELECT 1
                               FROM cust_auto_rte_rt2c car
                              WHERE car.div_part = g.div_part
                                AND car.rte_grp_num = g.rte_grp_num)
              AND NOT EXISTS(SELECT 1
                               FROM rte_stat_rt1s r
                              WHERE r.rte_grp_num = g.rte_grp_num
                                AND (   r.stat_cd IN('SNT', 'WRK')
                                     OR r.end_dt >= i_deadline));

      COMMIT;
    END del_rte_grp_rt2g_sp;

    PROCEDURE del_sql_utilities_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM sql_utilities s
            WHERE s.date_occurred < i_deadline;

      COMMIT;
    END del_sql_utilities_sp;

    PROCEDURE del_split_ord_sp(
      i_deadline  IN  DATE
    ) IS
      l_deadline_char  VARCHAR2(14);
    BEGIN
      l_deadline_char := TO_CHAR(i_deadline, 'YYYYMMDDHH24MISS');
      LOOP
        DELETE FROM split_ord_op2s so
              WHERE NOT EXISTS(SELECT 1
                                 FROM ordp100a a
                                WHERE a.div_part = so.div_part
                                  AND a.ordnoa = so.ord_num)
                AND NOT EXISTS(SELECT 1
                                 FROM ordp900a a
                                WHERE a.div_part = so.div_part
                                  AND a.ordnoa = so.ord_num
                                  AND a.uschga > l_deadline_char)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_split_ord_sp;

    PROCEDURE del_strct_ord_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM strct_ord_op1o so
            WHERE NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = so.div_part
                                AND a.ordnoa = so.ord_num)
              AND NOT EXISTS(SELECT 1
                               FROM ordp920b b
                              WHERE b.div_part = so.div_part
                                AND b.ordnob = so.ord_num
                                AND b.lineb = so.ord_ln
                                AND so.recap_ts > i_deadline);

      COMMIT;
    END del_strct_ord_sp;

    PROCEDURE del_events_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM evnt_tran_ev1t t
            WHERE (t.div_part, t.evnt_nm) IN(SELECT m.div_part, m.evnt_nm
                                               FROM evnt_mstr_ev1m m
                                              WHERE m.actv_sw = 'N'
                                                AND TRUNC(GREATEST(m.last_chg_ts, m.nxt_rqst_ts)) <= i_deadline);

      DELETE FROM evnt_mstr_ev1m m
            WHERE m.actv_sw = 'N'
              AND TRUNC(GREATEST(m.last_chg_ts, m.nxt_rqst_ts)) <= i_deadline;

      COMMIT;
    END del_events_sp;

    PROCEDURE del_wave_plan_load_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM wave_plan_load_op2w w
            WHERE w.llr_dt < i_deadline
              AND NOT EXISTS(SELECT 1
                               FROM rlse_op1z r
                              WHERE r.div_part = w.div_part
                                AND r.llr_dt = w.llr_dt);

      COMMIT;
    END del_wave_plan_load_sp;

    PROCEDURE del_wkly_max_log_sp IS
    BEGIN
      LOOP
        DELETE FROM wkly_max_log_op3m l
              WHERE NOT EXISTS(SELECT 1
                                 FROM rlse_op1z r
                                WHERE r.div_part = l.div_part
                                  AND r.rlse_ts = l.rlse_ts)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_wkly_max_log_sp;

    PROCEDURE del_vndr_cmp_sp(
      i_deadline  IN  DATE
    ) IS
      l_t_div_parts  type_ntab;
      l_t_prof_ids   type_ntab;
      l_t_cust_ids   type_stab;
    BEGIN
      SELECT c.div_part, c.prof_id, c.cust_id
      BULK COLLECT INTO l_t_div_parts, l_t_prof_ids, l_t_cust_ids
        FROM vndr_cmp_cust_op2l c
       WHERE c.end_dt < i_deadline;

      IF l_t_prof_ids.COUNT > 0 THEN
        FORALL i IN l_t_prof_ids.FIRST .. l_t_prof_ids.LAST
          DELETE FROM vndr_cmp_qty_op4l q
                WHERE q.prof_id = l_t_prof_ids(i)
                  AND q.div_part = l_t_div_parts(i)
                  AND q.cust_id = l_t_cust_ids(i);
        FORALL i IN l_t_prof_ids.FIRST .. l_t_prof_ids.LAST
          DELETE FROM vndr_cmp_cust_op2l c
                WHERE c.prof_id = l_t_prof_ids(i)
                  AND c.div_part = l_t_div_parts(i)
                  AND c.cust_id = l_t_cust_ids(i);
        COMMIT;
      END IF;   -- l_t_prof_ids.count > 0
    END del_vndr_cmp_sp;

    PROCEDURE del_load_depart_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM load_depart_op1f l
            WHERE l.llr_ts > DATE '1900-01-01'
              AND l.llr_ts < i_deadline
              AND NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = l.div_part
                                AND a.load_depart_sid = l.load_depart_sid);

      COMMIT;
    END del_load_depart_sp;

    PROCEDURE del_load_log_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM load_log_op3z l
            WHERE EXISTS(SELECT 1
                           FROM load_rsn_op3r r
                          WHERE r.div_part = l.div_part
                            AND r.rsn_id = l.rsn_id
                            AND r.create_ts < i_deadline);

      DELETE FROM load_rsn_op3r r
            WHERE r.create_ts < i_deadline;

      COMMIT;
    END del_load_log_sp;

    PROCEDURE del_dist_del_recycl_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM dist_del_recycl_op4r r
            WHERE r.create_ts < i_deadline
              AND NOT EXISTS(SELECT 1
                               FROM ordp100a a
                              WHERE a.div_part = r.div_part
                                AND a.ordnoa = r.ord_num);

      COMMIT;
    END del_dist_del_recycl_sp;

    PROCEDURE del_cpy_ord_sp IS
    BEGIN
      DELETE FROM cpy_ord_cp3o co
            WHERE co.ROWID IN(SELECT co2.ROWID
                                FROM cpy_ord_cp3o co2
                              MINUS
                              SELECT co3.ROWID
                                FROM cpy_ord_cp3o co3, ordp100a o
                               WHERE o.connba = co3.conf_num
                                  OR o.ordnoa = co3.orig_ord_num
                              MINUS
                              SELECT co4.ROWID
                                FROM cpy_ord_cp3o co4, ordp900a h
                               WHERE h.connba = co4.conf_num
                                  OR h.ordnoa = co4.orig_ord_num);

      COMMIT;
    END del_cpy_ord_sp;

    PROCEDURE del_reroute_log_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM reroute_log_rt2r r
            WHERE r.create_ts < i_deadline;

      COMMIT;
    END del_reroute_log_sp;

    PROCEDURE del_addl_cntnr_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM addl_cntnr_id_bc3c ac
            WHERE ac.create_ts < i_deadline;

      COMMIT;
    END del_addl_cntnr_sp;

    PROCEDURE del_cust_rte_req_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM cust_rte_req_op4c r
            WHERE r.create_ts < i_deadline
              AND r.new_llr_dt < i_deadline;

      COMMIT;
    END del_cust_rte_req_sp;

    PROCEDURE del_cust_dist_rte_req_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM cust_dist_rte_req_op5c r
            WHERE r.create_ts < i_deadline
              AND r.new_eta_ts < i_deadline;

      COMMIT;
    END del_cust_dist_rte_req_sp;

    PROCEDURE del_cust_dist_rte_extr_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM cust_dist_rte_extr_op6c r
            WHERE r.last_chg_ts < i_deadline;

      COMMIT;
    END del_cust_dist_rte_extr_sp;

    PROCEDURE del_bill_cntnr_id_bc1c_sp(
      i_deadline  IN  DATE
    ) IS
    BEGIN
      DELETE FROM bill_cntnr_id_bc1c bc
            WHERE bc.create_dt < i_deadline;

      COMMIT;
    END del_bill_cntnr_id_bc1c_sp;

    PROCEDURE del_orphan_ords_sp IS
    BEGIN
      logs.dbg('Deleting Orphan MCLP300D');
      LOOP
        DELETE FROM mclp300d d
              WHERE NOT EXISTS(SELECT 1
                                 FROM ordp100a a
                                WHERE a.div_part = d.div_part
                                  AND a.ordnoa = d.ordnod)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
      logs.dbg('Deleting Orphan BILL_CNTNR_ID_BC1C');
      LOOP
        DELETE FROM bill_cntnr_id_bc1c bc
              WHERE NOT EXISTS(SELECT 1
                                 FROM ordp100a a
                                WHERE a.div_part = bc.div_part
                                  AND a.ordnoa = bc.ord_num)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
      logs.dbg('Deleting Orphan ORDP940C');
      LOOP
        DELETE FROM ordp940c c
              WHERE NOT EXISTS(SELECT 1
                                 FROM ordp900a a
                                WHERE a.div_part = c.div_part
                                  AND a.ordnoa = c.ordnoc)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
      logs.dbg('Deleting Orphan MCLP900D');
      LOOP
        DELETE FROM mclp900d d
              WHERE d.reasnd <> 'DELDIST'
                AND NOT EXISTS(SELECT 1
                                 FROM ordp900a a
                                WHERE a.div_part = d.div_part
                                  AND a.ordnoa = d.ordnod)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
      logs.dbg('Deleting Orphan ORDP920B');
      LOOP
        DELETE      /*+ ENABLE_PARALLEL_DML PARALLEL */FROM ordp920b b
              WHERE NOT EXISTS(SELECT 1
                                 FROM ordp900a a
                                WHERE a.div_part = b.div_part
                                  AND a.ordnoa = b.ordnob)
                AND ROWNUM <= l_c_commit_cnt;

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
      END LOOP;
    END del_orphan_ords_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_t_parms := op_parms_pk.idx_vals_fn(0,
                                         op_const_pk.prm_del_all_days
                                         || ','
                                         || op_const_pk.prm_mq_put_days
                                         || ','
                                         || op_const_pk.prm_mq_get_days
                                         || ','
                                         || op_const_pk.prm_rlse_days
                                         || ','
                                         || op_const_pk.prm_gov_cntl_cust_days
                                         || ','
                                         || op_const_pk.prm_ration_log_days
                                         || ','
                                         || op_const_pk.prm_bill_po_ovride_days
                                         || ','
                                         || op_const_pk.prm_routing_days
                                         || ','
                                         || op_const_pk.prm_sql_utilities_days
                                         || ','
                                         || op_const_pk.prm_split_ord_days
                                         || ','
                                         || op_const_pk.prm_strict_ord_days
                                         || ','
                                         || op_const_pk.prm_evnt_clnup_days
                                         || ','
                                         || op_const_pk.prm_dist_del_recycl_days
                                         || ','
                                         || op_const_pk.prm_addl_cntnr_purg
                                         || ','
                                         || op_const_pk.prm_reroute_log_days
                                         || ','
                                         || op_const_pk.prm_load_log_days
                                        );
    l_mclp240b_deadline := i_prcs_ts - l_c_mclp240b_days;
    l_del_all_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_del_all_days);
    l_mq_put_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_mq_put_days);
    l_mq_get_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_mq_get_days);
    l_ord_rcpt_stat_deadline := i_prcs_ts - l_c_ord_rcpt_stat_days;
    l_rlse_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_rlse_days);
    l_mfst_rpts_deadline := i_prcs_ts - l_c_mfst_rpts_days;
    l_sysp996a_deadline := i_prcs_ts - l_c_sysp996a_days;
    l_prepost_load_deadline := i_prcs_ts - l_c_prepost_load_days;
    l_gov_cntl_cust_deadline := l_c_trunc_prcs_ts - parm_fn(op_const_pk.prm_gov_cntl_cust_days);
    l_ration_log_deadline := l_c_trunc_prcs_ts - parm_fn(op_const_pk.prm_ration_log_days);
    l_bill_po_ovride_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_bill_po_ovride_days);
    l_cust_auto_rte_deadline := l_c_trunc_prcs_ts;
    l_cust_rte_ovrrd_deadline := l_c_trunc_prcs_ts;
    l_rte_grp_deadline := l_c_trunc_prcs_ts - parm_fn(op_const_pk.prm_routing_days);
    l_cust_rte_req_deadline := l_c_trunc_prcs_ts;
    l_cust_dist_rte_req_deadline := l_c_trunc_prcs_ts;
    l_cust_dist_rte_extr_deadline := l_c_trunc_prcs_ts - 30;
    l_sql_utilities_deadline := l_c_trunc_prcs_ts - parm_fn(op_const_pk.prm_sql_utilities_days);
    l_split_ord_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_split_ord_days);
    l_strict_ord_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_strict_ord_days);
    l_evnt_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_evnt_clnup_days, 999);
    l_wave_plan_load_deadline := l_c_trunc_prcs_ts;
    l_vndr_cmp_deadline := l_c_trunc_prcs_ts - l_c_vndr_cmp_days;
    l_load_depart_deadline := l_c_trunc_prcs_ts - l_c_load_depart_days;
    l_dist_del_recycl_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_dist_del_recycl_days, 999);
    l_addl_cntnr_purge_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_addl_cntnr_purg);
    l_reroute_log_deadline := l_c_trunc_prcs_ts - parm_fn(op_const_pk.prm_reroute_log_days, 999);
    l_load_log_deadline := i_prcs_ts - parm_fn(op_const_pk.prm_load_log_days, 180);

    EXECUTE IMMEDIATE 'alter session set commit_logging=batch';

    EXECUTE IMMEDIATE 'alter session set commit_wait=nowait';

    logs.info('Deleting MCLP240B');
    del_mclp240b_sp(l_mclp240b_deadline);
    logs.info('Delete Orders');
    del_orders_sp(l_del_all_deadline);
    logs.info('Deleting SYSP296A');
    del_sysp296a_sp(l_del_all_deadline);
    logs.info('Deleting SYSP996A');
    del_sysp996a_sp(l_del_all_deadline);
    logs.info('Deleting MCLP900D DELDIST');
    del_mclp900d_deldist_sp(l_del_all_deadline);
    logs.info('Deleting MCLANE_MQ_PUT status: CMP');
    del_mclane_mq_put_sp(l_mq_put_deadline);
    logs.info('Deleting MCLANE_MQ_GET status: CMP');
    del_mclane_mq_get_sp(l_mq_get_deadline);
    logs.info('Deleting MCLANE_ORDER_RECEIPT_STATUS status: C');
    del_mclane_order_receipt_st_sp(l_ord_rcpt_stat_deadline);
    logs.info('Deleting RLSE_OP1Z status: R');
    del_rlse_op1z_sp(l_rlse_deadline);
    logs.info('Deleting MCLANE_MANIFEST_RPTS');
    del_mclane_manifest_rpts_sp(l_mfst_rpts_deadline);
    logs.info('Canceled Order Cleanup');
    del_canceled_ord_sp(l_sysp996a_deadline);
    logs.info('Deleting PREPOST_LOAD_OP1P');
    del_prepost_load_op1p_sp(l_prepost_load_deadline);
    logs.info('Deleting GOV_CNTL_CUST_P640A');
    del_gov_cntl_cust_p640a_sp(l_gov_cntl_cust_deadline);
    logs.info('Deleting CARE_PKG_ORD_CP1C');
    del_care_pkg_ord_cp1c_sp;
    logs.info('Deleting BULK_OUT_BO1O');
    del_bulk_out_bo1o_sp;
    logs.info('Deleting RATION_ITEM_LOG_RL1I');
    del_ration_item_log_rl1i_sp(l_ration_log_deadline);
    logs.info('Deleting BUNDL_DIST_ITEM_BD1I');
    del_bundl_dist_item_bd1i_sp;
    logs.info('Deleting BUNDL_DIST_CUST_BD1C');
    del_bundl_dist_cust_bd1c_sp;
    logs.info('Deleting BILL_PO_OVRIDE_BC1P');
    del_bill_po_ovride_bc1p_sp(l_bill_po_ovride_deadline);
    logs.info('Deleting CUST_AUTO_RTE_RT2C');
    del_cust_auto_rte_rt2c_sp(l_cust_auto_rte_deadline);
    logs.info('Deleting CUST_RTE_OVRRD_RT3C');
    del_cust_rte_ovrrd_rt3c_sp(l_cust_rte_ovrrd_deadline);
    logs.info('Deleting RTE_GRP_RT2G');
    del_rte_grp_rt2g_sp(l_rte_grp_deadline);
    logs.info('Deleting CUST_RTE_REQ_OP4C');
    del_cust_rte_req_sp(l_cust_rte_req_deadline);
    logs.info('Deleting CUST_DIST_RTE_REQ_OP5C');
    del_cust_dist_rte_req_sp(l_cust_dist_rte_req_deadline);
    logs.info('Deleting CUST_DIST_RTE_EXTR_OP6C');
    del_cust_dist_rte_extr_sp(l_cust_dist_rte_extr_deadline);
    logs.info('Deleting SQL_UTILITIES');
    del_sql_utilities_sp(l_sql_utilities_deadline);
    logs.info('Deleting SPLIT_ORD_OP2S');
    del_split_ord_sp(l_split_ord_deadline);
    logs.info('Deleting STRCT_ORD_OP1O');
    del_strct_ord_sp(l_strict_ord_deadline);
    logs.info('Deleting EVNT_MSTR_EV1M and EVNT_TRAN_EV1T');
    del_events_sp(l_evnt_deadline);
    logs.info('Deleting WAVE_PLAN_LOAD_OP2W');
    del_wave_plan_load_sp(l_wave_plan_load_deadline);
    logs.info('Deleting WKLY_MAX_LOG_OP3M');
    del_wkly_max_log_sp;
    logs.info('Deleting VNDR_CMP_CUST_OP2L and VNDR_CMP_QTY_OP4L');
    del_vndr_cmp_sp(l_vndr_cmp_deadline);
    logs.info('Deleting LOAD_DEPART_OP1F');
    del_load_depart_sp(l_load_depart_deadline);
    logs.info('Deleting LOAD_LOG_OP3Z and LOAD_RSN_OP3R');
    del_load_log_sp(l_load_log_deadline);
    logs.info('Deleting DIST_DEL_RECYCL_OP4R');
    del_dist_del_recycl_sp(l_dist_del_recycl_deadline);
    logs.info('Deleting CPY_ORD_CP3O');
    del_cpy_ord_sp;
    logs.info('Deleting REROUTE_LOG_RT2R');
    del_reroute_log_sp(l_reroute_log_deadline);
    logs.info('Deleting BILL_CNTNR_ID_BC1C');
    del_bill_cntnr_id_bc1c_sp(l_del_all_deadline);
    logs.info('Deleting Orphan Orders');
    del_orphan_ords_sp;
    logs.info('Deleting Additional Containers');
    del_addl_cntnr_sp(l_addl_cntnr_purge_deadline);

    EXECUTE IMMEDIATE 'alter session set commit_logging=immediate';

    EXECUTE IMMEDIATE 'alter session set commit_wait=wait';

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      EXECUTE IMMEDIATE 'alter session set commit_logging=immediate';

      EXECUTE IMMEDIATE 'alter session set commit_wait=wait';

      ROLLBACK;
      logs.err(lar_parm);
  END delete_all_sp;

  /*
  ||----------------------------------------------------------------------------
  || MQ_GET_SP
  ||  Process any old OPN msgs on the Get table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 07/31/18 | rhalpai | Original for SDHD-328893.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE mq_get_sp(
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module   CONSTANT typ.t_maxfqnm                       := 'OP_CLEANUP_PK.MQ_GET_SP';
    lar_parm              logs.tar_parm;
    l_div_part            NUMBER;
    l_appl_srvr           appl_sys_parm_ap1s.vchar_val%TYPE;
    l_os_result           typ.t_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.dbg('ENTRY', lar_parm);
    l_div_part := 0;
    l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
    logs.dbg('Process for OPN msgs');
    FOR l_r IN (SELECT   '/local/prodcode/bin/zzopMsgs.sub '
                         || d.div_id
                         || ' '
                         || g.mq_msg_id AS cmd
                    FROM mclane_mq_get g, div_mstr_di1d d
                   WHERE g.div_part = d.div_part
                     AND g.mq_msg_status = 'OPN'
                GROUP BY d.div_id, g.mq_msg_id) LOOP
      logs.dbg('Process Command' || cnst.newline_char || l_r.cmd);
      l_os_result := oscmd_fn(l_r.cmd, l_appl_srvr);
      logs.dbg('OS Result' || cnst.newline_char || l_os_result);
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END mq_get_sp;

  /*
  ||----------------------------------------------------------------------------
  || REASSIGN_ORDERS_SP
  ||  Unbilled order reassignment process.
  ||  For all orders on assigned loads with past llr date it moves to next
  ||  available load.
  ||  For regular orders on loads not assigned to customer with past eta date it
  ||  bumps the date out to next week.  (ie: Wed of next week)
  ||  For non-Pxx distributions with past llr date it resets to DIST load.
  ||  For Pxx distributions with past eta date it resets to PxxP load.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/10/02 | rhalpai | Original
  || 01/16/03 | rhalpai | Changed unassigned reg orders to be based on LLR instead
  ||                    | of ETA date. Also changed section
  ||                    | 'Reassign past reg orders not on customer"s assigned loads'
  ||                    | to just bump out the LLR, ETA and Depart dates by 7 days.
  || 02/25/03 | rhalpai | Changed to log NO_DATA_FOUND orders as warnings since
  ||                    | these are mostly "NO-ORDER" orders.
  || 03/18/03 | rhalpai | Changed call to SYNCLOAD_SP to pass 'Y' instead of NULL
  ||                    | for p_treat_dist_as_reg parm. This will allow distribution
  ||                    | orders on the customer's assigned load after the LLR date
  ||                    | to be correctly reassigned to the customer's next available
  ||                    | load.
  || 11/14/03 | CNATIVI | Changed condition from "lineb=1" to "ROWNUM=1" in "Get
  ||                      Order Detail Info" section (same on dlineb)
  || 08/12/05 | rhalpai | Changed error handler to new standard format. Removed
  ||                    | "Status" out parm. PIR2051
  || 03/02/06 | rhalpai | Added process control logic. IM200261
  || 05/22/08 | rhalpai | Added call to ASSIGN_DFLT_ORDS_SP to process Syncload
  ||                    | for open orders on DFLT load for customers with assigned
  ||                    | loads. IM403870
  || 08/11/08 | rhalpai | Removed check for detail with status not in O,I,S,C in
  ||                    | cursor orders_cur and reformatted logic. PIR6364
  || 06/21/11 | rhalpai | Changed cursor to treat matching entries for Cust Route
  ||                    | Overrides as assigned loads. PIR9348
  || 03/01/12 | rhalpai | Change logic to remove excepion order well.
  || 05/13/13 | rhalpai | Add Div parm. PIR11038
  || 07/04/13 | rhalpai | Add Div parm. Change to use OrdTyp to indicate NoOrdSw.
  ||                    | Convert to use OP1F,OP1G. PIR11038
  || 02/17/14 | rhalpai | Change logic to combine assigned orders and unassigned
  ||                    | orders for syncload call within cursor. Change logic to
  ||                    | make a single call to syncload and remove
  ||                    | treat_dist_as_reg from call to syncload. PIR13455
  || 03/15/16 | rhalpai | Add ProcessTs input parm defaulted to SYSDATE to allow
  ||                    | for SYSDATE override. Change to use constants package
  ||                    | OP_CONST_PK. Change to call new OP_PARMS_PK.IDX_VALS_FN,
  ||                    | OP_PARMS_PK.VALS_FOR_PRFX_FN. Change logic to use
  ||                    | variable containing nested table of parm values. SDHD-102466
  || 08/03/18 | rhalpai | Add logic to call ASSIGN_DIST_ONLY_SP. PIR18748
  || 09/28/20 | rhalpai | Change logic to include Seq Assigned Load order sources
  ||                    | like XPR in the SyncOrds logic (for orders on assigned loads)
  ||                    | and exclude them from the AdjRegOrds logic (for regular
  ||                    | orders on unassigned loads). PIR20838
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reassign_orders_sp(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm             := 'OP_CLEANUP_PK.REASSIGN_ORDERS_SP';
    lar_parm                   logs.tar_parm;
    l_div_part                 NUMBER;
    l_curr_dt                  DATE;
    l_t_parms                  op_types_pk.tt_varchars_v;
    l_t_xloads                 type_stab;
    l_mv_assgnd_ord_days       PLS_INTEGER;
    l_mv_unassgnd_ord_days     PLS_INTEGER;
    l_reset_to_dist_days       PLS_INTEGER;
    l_reset_to_pxxp_days       PLS_INTEGER;
    l_reset_to_pxxp_add_days   PLS_INTEGER;
    l_assigned_order_deadline  DATE;
    l_unassigned_reg_deadline  DATE;
    l_dist_deadline            DATE;
    l_pxxp_deadline            DATE;

    TYPE l_rt_load_ords IS RECORD(
      llr_ts                    DATE,
      load_num                  mclp120c.loadc%TYPE,
      depart_ts                 DATE,
      cust_id                   sysp200c.acnoc%TYPE,
      stop_num                  NUMBER,
      eta_ts                    DATE,
      t_sync_ords               type_ntab             := type_ntab(),
      t_adj_unassgnd_reg_ords   type_ntab             := type_ntab(),
      t_adj_unassgnd_pxxp_ords  type_ntab             := type_ntab()
    );

    TYPE l_tt_load_ords IS TABLE OF l_rt_load_ords;

    l_t_load_ords              l_tt_load_ords;
    l_llr_dt                   DATE;
    l_llr_ts                   DATE;
    l_depart_ts                DATE;
    l_load_depart_sid          NUMBER;
    l_eta_ts                   DATE;
    l_stop_num                 NUMBER;
    l_c_user_id       CONSTANT VARCHAR2(20)              := 'CLEANUP_REASSIGN_ORD';
    l_c_reassign      CONSTANT VARCHAR2(8)               := 'REASSIGN';
    l_c_reset_dist    CONSTANT VARCHAR2(8)               := 'RESETDIS';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    l_curr_dt := TRUNC(i_prcs_ts);
    logs.dbg('Set Process Active');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_clnup_rsgn_ords,
                                                op_process_control_pk.g_c_active,
                                                USER,
                                                l_div_part
                                               );
    logs.dbg('Retrieve Parms');
    l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                         op_const_pk.prm_mv_assgnd_ord_days
                                         || ','
                                         || op_const_pk.prm_mv_unassgnd_ord_days
                                         || ','
                                         || op_const_pk.prm_reset_to_dist_days
                                         || ','
                                         || op_const_pk.prm_reset_to_pxxp_days
                                        );
    l_t_xloads := op_parms_pk.vals_for_prfx_fn(l_div_part, op_const_pk.prm_xload);
    l_mv_assgnd_ord_days := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_mv_assgnd_ord_days)), 0);
    l_mv_unassgnd_ord_days := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_mv_unassgnd_ord_days)), 0);
    l_reset_to_dist_days := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_reset_to_dist_days)), 0);
    l_reset_to_pxxp_days := NVL(TO_NUMBER(l_t_parms(op_const_pk.prm_reset_to_pxxp_days)), 0);
    logs.dbg('Adjust Add_Days');
    -- Handle parms being greater than 7 days
    -- (if the parm is 8 then we will need to add 14 days)
    l_reset_to_pxxp_add_days := (TRUNC(l_reset_to_pxxp_days / 7) + 1) * 7;
    logs.dbg('Set Deadlines');
    l_assigned_order_deadline := l_curr_dt - l_mv_assgnd_ord_days;
    l_unassigned_reg_deadline := l_curr_dt - l_mv_unassgnd_ord_days;
    l_dist_deadline := l_curr_dt - l_reset_to_dist_days;
    l_pxxp_deadline := l_curr_dt - l_reset_to_pxxp_days;
    logs.dbg('Get Orders for Reassignment');

    SELECT   ld.llr_ts,
             ld.load_num,
             ld.depart_ts,
             se.cust_id,
             se.stop_num,
             se.eta_ts,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = se.div_part
                              AND a.load_depart_sid = se.load_depart_sid
                              AND a.custa = se.cust_id
                              AND a.dsorda IN('R', 'D')
                              AND a.stata IN('O', 'S')
                              AND (   (    ld.llr_dt < l_assigned_order_deadline
                                       AND (   EXISTS(SELECT 1
                                                        FROM mclp040d d
                                                       WHERE d.div_part = a.div_part
                                                         AND d.loadd = ld.load_num
                                                         AND d.custd = a.custa)
                                            OR EXISTS(SELECT 1
                                                        FROM cust_rte_ovrrd_rt3c cro
                                                       WHERE cro.div_part = a.div_part
                                                         AND cro.cust_id = a.custa
                                                         AND cro.llr_dt = ld.llr_dt
                                                         AND cro.load_num = ld.load_num
                                                         AND cro.stop_num = se.stop_num)
                                            OR EXISTS(SELECT 1
                                                        FROM sub_prcs_ord_src s
                                                       WHERE s.div_part = a.div_part
                                                         AND s.prcs_id = 'ORDER RECEIPT'
                                                         AND s.prcs_sbtyp_cd = 'SAL'
                                                         AND s.ord_src = a.ipdtsa)
                                           )
                                      )
                                   OR (    ld.llr_dt < l_dist_deadline
                                       AND a.dsorda = 'D'
                                       AND a.ldtypa NOT BETWEEN 'P00' AND 'P99'
                                       AND NOT EXISTS(SELECT 1
                                                        FROM mclp040d d
                                                       WHERE d.div_part = a.div_part
                                                         AND d.loadd = ld.load_num
                                                         AND d.custd = a.custa)
                                       AND NOT EXISTS(SELECT 1
                                                        FROM cust_rte_ovrrd_rt3c cro
                                                       WHERE cro.div_part = a.div_part
                                                         AND cro.cust_id = a.custa
                                                         AND cro.llr_dt = ld.llr_dt
                                                         AND cro.load_num = ld.load_num
                                                         AND cro.stop_num = se.stop_num)
                                      )
                                  )
                          ) AS type_ntab
                 ) AS sync_ords,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = se.div_part
                              AND a.load_depart_sid = se.load_depart_sid
                              AND a.custa = se.cust_id
                              AND a.dsorda = 'R'
                              AND ld.llr_dt < l_unassigned_reg_deadline
                              AND a.stata IN('O', 'S')
                              AND NOT EXISTS(SELECT 1
                                               FROM mclp040d d
                                              WHERE d.div_part = a.div_part
                                                AND d.loadd = ld.load_num
                                                AND d.custd = a.custa)
                              AND NOT EXISTS(SELECT 1
                                               FROM cust_rte_ovrrd_rt3c cro
                                              WHERE cro.div_part = a.div_part
                                                AND cro.cust_id = a.custa
                                                AND cro.llr_dt = ld.llr_dt
                                                AND cro.load_num = ld.load_num
                                                AND cro.stop_num = se.stop_num)
                              AND NOT EXISTS(SELECT 1
                                               FROM sub_prcs_ord_src s
                                              WHERE s.div_part = a.div_part
                                                AND s.prcs_id = 'ORDER RECEIPT'
                                                AND s.prcs_sbtyp_cd = 'SAL'
                                                AND s.ord_src = a.ipdtsa)
                          ) AS type_ntab
                 ) AS adj_reg_ords,
             CAST(MULTISET(SELECT a.ordnoa
                             FROM ordp100a a
                            WHERE a.div_part = se.div_part
                              AND a.load_depart_sid = se.load_depart_sid
                              AND a.custa = se.cust_id
                              AND a.dsorda = 'D'
                              AND a.ldtypa BETWEEN 'P00' AND 'P99'
                              AND TRUNC(se.eta_ts) < l_pxxp_deadline
                              AND a.stata IN('O', 'S')
                              AND NOT EXISTS(SELECT 1
                                               FROM mclp040d d
                                              WHERE d.div_part = a.div_part
                                                AND d.loadd = ld.load_num
                                                AND d.custd = a.custa)
                              AND NOT EXISTS(SELECT 1
                                               FROM cust_rte_ovrrd_rt3c cro
                                              WHERE cro.div_part = a.div_part
                                                AND cro.cust_id = a.custa
                                                AND cro.llr_dt = ld.llr_dt
                                                AND cro.load_num = ld.load_num
                                                AND cro.stop_num = se.stop_num)
                          ) AS type_ntab
                 ) AS adj_pxxp_ords
    BULK COLLECT INTO l_t_load_ords
        FROM load_depart_op1f ld, stop_eta_op1g se
       WHERE ld.div_part = l_div_part
         AND ld.llr_ts > DATE '1900-01-01'
         AND ld.load_num NOT IN(SELECT t.column_value
                                  FROM TABLE(CAST(l_t_xloads AS type_stab)) t)
         AND se.load_depart_sid = ld.load_depart_sid
         AND EXISTS(SELECT 1
                      FROM ordp100a a
                     WHERE a.div_part = se.div_part
                       AND a.load_depart_sid = se.load_depart_sid
                       AND a.custa = se.cust_id
                       AND a.dsorda IN('R', 'D')
                       AND a.stata IN('O', 'S')
                       AND (   (    (   EXISTS(SELECT 1
                                                 FROM mclp040d md
                                                WHERE md.div_part = se.div_part
                                                  AND md.custd = se.cust_id
                                                  AND md.loadd = ld.load_num)
                                     OR EXISTS(SELECT 1
                                                 FROM cust_rte_ovrrd_rt3c cro
                                                WHERE cro.div_part = se.div_part
                                                  AND cro.cust_id = se.cust_id
                                                  AND cro.llr_dt = ld.llr_dt
                                                  AND cro.load_num = ld.load_num
                                                  AND cro.stop_num = se.stop_num)
                                     OR EXISTS(SELECT 1
                                                 FROM sub_prcs_ord_src s
                                                WHERE s.div_part = a.div_part
                                                  AND s.prcs_id = 'ORDER RECEIPT'
                                                  AND s.prcs_sbtyp_cd = 'SAL'
                                                  AND s.ord_src = a.ipdtsa)
                                    )
                                AND ld.llr_dt < l_assigned_order_deadline
                               )
                            OR (    NOT EXISTS(SELECT 1
                                                 FROM mclp040d md
                                                WHERE md.div_part = se.div_part
                                                  AND md.custd = se.cust_id
                                                  AND md.loadd = ld.load_num)
                                AND NOT EXISTS(SELECT 1
                                                 FROM cust_rte_ovrrd_rt3c cro
                                                WHERE cro.div_part = se.div_part
                                                  AND cro.cust_id = se.cust_id
                                                  AND cro.llr_dt = ld.llr_dt
                                                  AND cro.load_num = ld.load_num
                                                  AND cro.stop_num = se.stop_num)
                                AND NOT EXISTS(SELECT 1
                                                 FROM sub_prcs_ord_src s
                                                WHERE s.div_part = a.div_part
                                                  AND s.prcs_id = 'ORDER RECEIPT'
                                                  AND s.prcs_sbtyp_cd = 'SAL'
                                                  AND s.ord_src = a.ipdtsa)
                                AND (   (    a.dsorda = 'R'
                                         AND ld.llr_dt < l_unassigned_reg_deadline)
                                     OR (    a.dsorda = 'D'
                                         AND (   (    a.ldtypa BETWEEN 'P00' AND 'P99'
                                                  AND TRUNC(se.eta_ts) < l_pxxp_deadline
                                                 )
                                              OR (    a.ldtypa NOT BETWEEN 'P00' AND 'P99'
                                                  AND ld.llr_dt < l_dist_deadline)
                                             )
                                        )
                                    )
                               )
                           ))
    ORDER BY ld.llr_ts, se.cust_id;

    IF l_t_load_ords.COUNT > 0 THEN
      FOR i IN l_t_load_ords.FIRST .. l_t_load_ords.LAST LOOP
        IF l_t_load_ords(i).t_sync_ords.COUNT > 0 THEN
          logs.dbg('Syncload for Assigned Ords');
          op_order_load_pk.syncload_sp(l_div_part, l_c_reassign, l_t_load_ords(i).t_sync_ords, 'CLEANUP');
        END IF;   -- l_t_load_ords(i).t_sync_assgnd_ords.COUNT > 0

        IF l_t_load_ords(i).t_adj_unassgnd_reg_ords.COUNT > 0 THEN
          -- Update REG orders not on customer's assigned loads (Bump out by 7 days)
          logs.dbg('Update REG orders not on customer"s assigned loads');
          l_llr_dt := TRUNC(l_t_load_ords(i).llr_ts) + 7;
          logs.dbg('Get LLR/Depart');
          op_order_load_pk.get_llr_depart_sp(l_div_part, l_llr_dt, l_t_load_ords(i).load_num, l_llr_ts, l_depart_ts);
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(l_div_part, l_llr_ts, l_t_load_ords(i).load_num);
          -- adjust the eta date based on the difference in old and new depart dates
          l_eta_ts := l_t_load_ords(i).eta_ts +(TRUNC(l_depart_ts) - TRUNC(l_t_load_ords(i).depart_ts));
          logs.dbg('Get Stop/Eta');
          op_order_load_pk.get_stop_eta_sp(l_div_part,
                                           l_load_depart_sid,
                                           l_t_load_ords(i).cust_id,
                                           l_stop_num,
                                           l_eta_ts,
                                           l_eta_ts,
                                           l_t_load_ords(i).stop_num
                                          );
          logs.dbg('Move Ords');
          op_order_load_pk.move_ords_sp(l_div_part,
                                        l_t_load_ords(i).cust_id,
                                        l_load_depart_sid,
                                        l_stop_num,
                                        l_eta_ts,
                                        l_t_load_ords(i).llr_ts,
                                        l_t_load_ords(i).load_num,
                                        l_t_load_ords(i).depart_ts,
                                        l_t_load_ords(i).stop_num,
                                        l_t_load_ords(i).eta_ts,
                                        l_c_reassign,
                                        l_c_user_id,
                                        l_t_load_ords(i).t_adj_unassgnd_reg_ords
                                       );
        END IF;   -- l_t_load_ords(i).t_adj_unassgnd_reg_ords.COUNT > 0

        IF l_t_load_ords(i).t_adj_unassgnd_pxxp_ords.COUNT > 0 THEN
          -- Update Pxx Orders on Unassigned Load to 7 days from Today
          logs.dbg('Update Pxx Orders on Unassigned Load');
          l_llr_dt := TRUNC(l_t_load_ords(i).llr_ts) + l_reset_to_pxxp_add_days;
          logs.dbg('Get LLR/Depart');
          op_order_load_pk.get_llr_depart_sp(l_div_part, l_llr_dt, l_t_load_ords(i).load_num, l_llr_ts, l_depart_ts);
          logs.dbg('Get LoadDepartSid');
          l_load_depart_sid := op_order_load_pk.load_depart_sid_fn(l_div_part, l_llr_ts, l_t_load_ords(i).load_num);
          -- adjust the eta date based on the difference in old and new depart dates
          l_eta_ts := l_t_load_ords(i).eta_ts +(TRUNC(l_depart_ts) - TRUNC(l_t_load_ords(i).depart_ts));
          logs.dbg('Get Stop/Eta');
          op_order_load_pk.get_stop_eta_sp(l_div_part,
                                           l_load_depart_sid,
                                           l_t_load_ords(i).cust_id,
                                           l_stop_num,
                                           l_eta_ts,
                                           l_eta_ts,
                                           l_t_load_ords(i).stop_num
                                          );
          logs.dbg('Move Ords');
          op_order_load_pk.move_ords_sp(l_div_part,
                                        l_t_load_ords(i).cust_id,
                                        l_load_depart_sid,
                                        l_stop_num,
                                        l_eta_ts,
                                        l_t_load_ords(i).llr_ts,
                                        l_t_load_ords(i).load_num,
                                        l_t_load_ords(i).depart_ts,
                                        l_t_load_ords(i).stop_num,
                                        l_t_load_ords(i).eta_ts,
                                        l_c_reset_dist,
                                        l_c_user_id,
                                        l_t_load_ords(i).t_adj_unassgnd_pxxp_ords
                                       );
        END IF;   -- l_t_load_ords(i).t_adj_unassgnd_pxxp_ords.COUNT > 0
      END LOOP;
    END IF;   -- l_t_load_ords.COUNT > 0

    logs.dbg('Assign Orders on DFLT Load for Custs Now Assigned to Loads');
    assign_dflt_ords_sp(l_div_part);
--    logs.dbg('Assign Orders on DIST for DistOnly Customers');
--    assign_dist_only_sp(l_div_part, i_prcs_ts);
    COMMIT;
    logs.dbg('Set Process Inactive');
    op_process_control_pk.set_process_status_sp(op_const_pk.prcs_clnup_rsgn_ords,
                                                op_process_control_pk.g_c_inactive,
                                                USER,
                                                l_div_part
                                               );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_clnup_rsgn_ords,
                                                  op_process_control_pk.g_c_inactive,
                                                  USER,
                                                  l_div_part
                                                 );
      logs.err(lar_parm);
  END reassign_orders_sp;

  /*
  ||----------------------------------------------------------------------------
  || MISC_CLEANUP_SP
  ||  Miscellaneous cleanup processes.  These include table updates.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/10/02 | rhalpai | Original
  || 08/12/05 | rhalpai | Added "CarePkg CustTyp Mass Update" logic. PIR2608
  ||                    | Changed error handler to new standard format. Removed
  ||                    | "Status" out parm. PIR2051
  || 12/19/06 | rhalpai | Added logic to clean up of Additional Container table,
  ||                    | ADDL_CNTNR_ID_BC3C. PIR3209
  || 02/10/09 | rhalpai | Added call to RESET_SEQ_SP. IM478687
  || 03/14/11 | rhalpai | Added logic to Reset OrdHdr in ship status with OrdDtl
  ||                    | in open status and set AutoAttchDist for GRO UPS Loads.
  ||                    | Removed unused table.
  || 07/05/11 | rhalpai | Added logic to zero PickQty on WklyMaxCustItem table
  ||                    | for new week. PIR6235
  || 10/01/12 | rhalpai | Add logic to cancel old unbilled distribution orders.
  ||                    | PIR5250
  || 05/13/13 | rhalpai | Change to pass Div in call to RESET_SEQ_SP. PIR11038
  || 03/15/17 | rhalpai | Add logic to cancel order header when all details have
  ||                    | been cancelled. Add ProcessTs input parm defaulted
  ||                    | to SYSDATE to allow for SYSDATE override. Change to use
  ||                    | constants package OP_CONST_PK. Change to call new
  ||                    | OP_PARMS_PK.VAL_FN, OP_PARMS_PK.MERGE_SP. SDHD-102466
  || 06/28/17 | rhalpai | Remove call to UPD_AUTO_ATTCH_DIST_SP. SDHD-324712
  ||----------------------------------------------------------------------------
  */
  PROCEDURE misc_cleanup_sp(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm := 'OP_CLEANUP_PK.MISC_CLEANUP_SP';
    lar_parm                   logs.tar_parm;
    l_div_part                 NUMBER;
    l_prtctd_inv_cleanup_stat  VARCHAR2(5);

    PROCEDURE upd_cust_typ_sp(
      i_div_part  IN  NUMBER
    ) IS
    BEGIN
      UPDATE sysp200c c
         SET c.typecc = (SELECT dmn.dmn_cd
                           FROM op_cls_dmn_cd_typ dmn
                          WHERE dmn.div_part = c.div_part
                            AND dmn.cls_typ = 'CRPCDE'
                            AND (   EXISTS(SELECT 1
                                             FROM mclp020b cx
                                            WHERE cx.div_part = c.div_part
                                              AND cx.custb = c.acnoc
                                              AND LPAD(cx.corpb, 3, '0') = dmn.cls_id)
                                 OR (    dmn.cls_id = 'ALL'
                                     AND NOT EXISTS(SELECT 1
                                                      FROM mclp020b cx2, op_cls_dmn_cd_typ dmn2
                                                     WHERE cx2.div_part = c.div_part
                                                       AND cx2.custb = c.acnoc
                                                       AND dmn2.div_part = cx2.div_part
                                                       AND dmn2.cls_typ = 'CRPCDE'
                                                       AND dmn2.cls_id = LPAD(cx2.corpb, 3, '0')
                                                       AND dmn2.dmn_typ = 'CUSTYP'
                                                       AND dmn2.dflt_sw = 'Y')
                                    )
                                )
                            AND dmn.dmn_typ = 'CUSTYP'
                            AND dmn.dflt_sw = 'Y')
       WHERE c.div_part = i_div_part
         AND c.typecc IS NULL
         AND EXISTS(SELECT 1
                      FROM op_cls_dmn_cd_typ dmn, mclp020b cx
                     WHERE cx.div_part = c.div_part
                       AND cx.custb = c.acnoc
                       AND cx.corpb > 0
                       AND dmn.div_part = cx.div_part
                       AND dmn.cls_typ = 'CRPCDE'
                       AND dmn.cls_id IN('ALL', LPAD(cx.corpb, 3, '0'))
                       AND dmn.dmn_typ = 'CUSTYP'
                       AND dmn.dflt_sw = 'Y');

      COMMIT;
    END upd_cust_typ_sp;

    PROCEDURE reset_shp_hdr_with_opn_dtl_sp(
      i_div_part  IN  NUMBER
    ) IS
    BEGIN
      UPDATE ordp100a a
         SET (a.stata, a.uschga) =
               (SELECT NVL(MAX(b.statb), 'P'), NVL2(MAX(b.statb), NULL, a.uschga)
                  FROM ordp120b b
                 WHERE b.div_part = a.div_part
                   AND b.ordnob = a.ordnoa
                   AND b.statb IN('O', 'P', 'R', 'A')
                HAVING MAX(b.statb) = 'O'
                   AND MIN(b.statb) = 'O')
       WHERE a.div_part = i_div_part
         AND a.stata = 'A'
         AND EXISTS(SELECT 1
                      FROM ordp120b b
                     WHERE b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb = 'O');

      COMMIT;
    END reset_shp_hdr_with_opn_dtl_sp;

    PROCEDURE upd_auto_attch_dist_sp(
      i_div_part  IN  NUMBER
    ) IS
    BEGIN
      -- Turn ON Auto-Attach Dist for Groc UPS Loads with customers not assigned to any GMP or non-UPS loads
      logs.dbg('GRO UPS Loads w/Cust NOT assigned to any GMP/non-UPS Loads');

      UPDATE mclp120c c
         SET c.aadisc = 'Y'
       WHERE c.div_part = i_div_part
         AND c.aadisc IN('N', '0')
         AND c.lbsgpc IN('N', '0')
         AND c.loadc LIKE 'U%'
         AND EXISTS(SELECT 1
                      FROM mclp040d d
                     WHERE d.div_part = c.div_part
                       AND d.loadd = c.loadc
                       AND d.prod_typ IN('GRO', 'BTH')
                       AND NOT EXISTS(SELECT 1
                                        FROM mclp040d d2
                                       WHERE d2.div_part = d.div_part
                                         AND d2.custd = d.custd
                                         AND (   d2.prod_typ = 'GMP'
                                              OR d2.loadd NOT LIKE 'U%')));

      -- Turn OFF Auto-Attach Dist for Groc UPS Loads with customers assigned to GMP or non-UPS loads
      logs.dbg('GRO UPS Loads w/Cust assigned to GMP/non-UPS Loads');

      UPDATE mclp120c c
         SET c.aadisc = 'N'
       WHERE c.div_part = i_div_part
         AND c.aadisc IN('Y', '1')
         AND c.lbsgpc IN('N', '0')
         AND c.loadc LIKE 'U%'
         AND EXISTS(SELECT 1
                      FROM mclp040d d
                     WHERE d.div_part = c.div_part
                       AND d.loadd = c.loadc
                       AND (   (d.prod_typ = 'GMP')
                            OR (EXISTS(SELECT 1
                                         FROM mclp040d d2
                                        WHERE d2.div_part = d.div_part
                                          AND d2.custd = d.custd
                                          AND (   d2.prod_typ = 'GMP'
                                               OR d2.loadd NOT LIKE 'U%'))
                               )
                           ));

      COMMIT;
    END upd_auto_attch_dist_sp;

    PROCEDURE reset_wkly_max_pick_qty_sp(
      i_div_part  IN  NUMBER
    ) IS
    BEGIN
      IF TO_CHAR(i_prcs_ts, 'DY') = op_parms_pk.val_fn(i_div_part, op_const_pk.prm_wkmaxqty_cln_day) THEN
        UPDATE wkly_max_cust_item_op1m ci
           SET ci.pick_qty = 0
         WHERE ci.div_part = i_div_part;

        op_parms_pk.merge_sp(i_div_part,
                             op_const_pk.prm_wkmaxqty_cln_ts,
                             op_parms_pk.g_c_dt,
                             TO_CHAR(i_prcs_ts, 'YYYYMMDDHH24MISS'),
                             'CLEANUP'
                            );
        COMMIT;
      END IF;   -- TO_CHAR(i_prcs_ts, 'DY') = op_parms_pk.val_fn(i_div_part, op_const_pk.prm_wkmaxqty_cln_day)
    END reset_wkly_max_pick_qty_sp;

    PROCEDURE cancl_dist_ords_sp(
      i_div_part  IN  NUMBER
    ) IS
      l_reg_dist_max_ship   NUMBER;
      l_spcl_dist_max_ship  NUMBER;

      CURSOR ord_cur(
        b_div_part            NUMBER,
        b_reg_dist_max_ship   NUMBER,
        b_spcl_dist_max_ship  NUMBER
      ) IS
        SELECT a.ordnoa AS ord_num
          FROM ordp100a a
         WHERE a.div_part = b_div_part
           AND a.dsorda = 'D'
           AND a.stata IN('O', 'S')
           AND a.shpja <(CASE
                           WHEN a.ldtypa LIKE 'P__' THEN b_spcl_dist_max_ship
                           ELSE b_reg_dist_max_ship
                         END);
    BEGIN
      l_reg_dist_max_ship := TRUNC(i_prcs_ts)
                             - NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_max_ship_reg_dist), 999)
                             - DATE '1900-02-28';
      l_spcl_dist_max_ship := TRUNC(i_prcs_ts)
                              - NVL(op_parms_pk.val_fn(i_div_part, op_const_pk.prm_max_ship_spcl_dist), 999)
                              - DATE '1900-02-28';
      FOR r_ord IN ord_cur(i_div_part, l_reg_dist_max_ship, l_spcl_dist_max_ship) LOOP
        csr_orders_pk.cancel_ord_sp(i_div,
                                    r_ord.ord_num,
                                    NULL,
                                    'RCANC7',
                                    'Cancel Old Unbilled Dist Orders',
                                    'OP_CLEANUP_PK'
                                   );
      END LOOP;
    END cancl_dist_ords_sp;

    PROCEDURE cancl_hdr_for_cancl_dtls_sp(
      i_div_part  IN  NUMBER
    ) IS
      l_t_ord_nums  type_ntab;
    BEGIN
      SELECT a.ordnoa
      BULK COLLECT INTO l_t_ord_nums
        FROM ordp100a a
       WHERE a.div_part = i_div_part
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

      IF l_t_ord_nums.COUNT > 0 THEN
        FOR i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST LOOP
          csr_orders_pk.cancel_ord_sp(i_div, l_t_ord_nums(i), NULL, 'RCANC7', 'All OrdDtls Cancelled', 'OP_CLEANUP_PK');
        END LOOP;
      END IF;   --l_t_ord_nums.COUNT > 0
    END cancl_hdr_for_cancl_dtls_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div_part := div_pk.div_part_fn(i_div);
    logs.dbg('Clean up Protected Inventory');
    op_protected_inventory_pk.cleanup_sp(l_div_part, l_prtctd_inv_cleanup_stat);
    logs.dbg('CarePkg CustTyp Mass Update');
    upd_cust_typ_sp(l_div_part);
    logs.dbg('Reset OrdHdr with OrdDtl in Open Status');
    reset_shp_hdr_with_opn_dtl_sp(l_div_part);
    -- Turn ON Auto-Attach Dist for Groc UPS Loads with customers not assigned to any GMP or non-UPS loads
    -- Turn OFF Auto-Attach Dist for Groc UPS Loads with customers assigned to GMP or non-UPS loads
--    logs.dbg('Upd Auto-Attach Dist for GRO UPS Loads');
--    upd_auto_attch_dist_sp(l_div_part);
    logs.dbg('Reset WklyMax PickQty to Zero');
    reset_wkly_max_pick_qty_sp(l_div_part);
    logs.dbg('Cancel Old Unbilled Dist Orders');
    cancl_dist_ords_sp(l_div_part);
    logs.dbg('Cancel OrdHdr When All OrdDtl Are Cancelled');
    cancl_hdr_for_cancl_dtls_sp(l_div_part);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      logs.err(lar_parm);
  END misc_cleanup_sp;

  /*
  ||----------------------------------------------------------------------------
  || MAIN_SP
  ||  Controlling procedure for cleanup process.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/14/11 | rhalpai | Original
  || 08/31/12 | rhalpai | Add Process Control for Analyze. IM-065209
  || 05/13/13 | rhalpai | Change to pass Div in call to REASSIGN_ORDERS_SP.
  ||                    | PIR11038
  || 03/15/17 | rhalpai | Add ProcessTs input parm defaulted to SYSDATE to allow
  ||                    | for SYSDATE override.
  ||                    | Change to use constants package OP_CONST_PK. PIR17084
  || 07/31/18 | rhalpai | Add call to mq_get_sp. SDHD-328893.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE main_sp(
    i_div      IN  VARCHAR2,
    i_prcs_ts  IN  DATE DEFAULT SYSDATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_CLEANUP_PK.MAIN_SP';
    lar_parm             logs.tar_parm;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'PrcsTs', i_prcs_ts);
    logs.info('ENTRY', lar_parm);

    IF i_div = 'MC' THEN
      -- Since archive of app_log is based on SYSDATE, only run when i_prcs_ts is for same date
      IF TRUNC(i_prcs_ts) = TRUNC(SYSDATE) THEN
        logs.dbg('Archive APP_LOG');
        --archive_app_log;
      END IF;   -- TRUNC(i_prcs_ts) = TRUNC(SYSDATE)

      logs.dbg('Move to Hist');
      move_to_hist_sp(i_prcs_ts);
      logs.dbg('Delete All');
      delete_all_sp(i_prcs_ts);
      logs.dbg('MQ Get');
      mq_get_sp(i_prcs_ts);
    ELSE
      logs.dbg('Misc Cleanup');
      misc_cleanup_sp(i_div, i_prcs_ts);
      logs.dbg('Reassign Orders');
      reassign_orders_sp(i_div, i_prcs_ts);
    END IF;   -- i_div = 'MC'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END main_sp;
END op_cleanup_pk;
/

