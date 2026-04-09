CREATE OR REPLACE PACKAGE op_backout_llr_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_BACKOUT_LLR_PK
  ||   To Backout Release
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  ||          |         |
  ||----------------------------------------------------------------------------
  */
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

  PROCEDURE last_rlse_sp(
    i_div             IN      VARCHAR2,
    o_rlse_ts         OUT     VARCHAR2,
    o_cigwms_mstr_sw  OUT     VARCHAR2,
    o_enable_sw       OUT     VARCHAR2
  );
  -- deprecated
  PROCEDURE last_rlse_sp(
    i_div             IN      VARCHAR2,
    o_rlse_ts         OUT     VARCHAR2,
    o_cigwms_mstr_sw  OUT     VARCHAR2
  );

  /*
  ||----------------------------------------------------------------------------
  || BACKOUT_SP
  ||   This procedure makes sure that all of the information needed for the
  ||   backout is correct.  It will "default" information from the McLane Load
  ||   Label Release Table if necessary.
  ||
  ||   Step 000 - Orders are in Order Well with an 'O' Status
  ||   Step 010 - Orders are flagged with a 'P' Status
  ||   Step 390 - Orders are Allocated
  ||   Step 410 - Work Orders Extracted to MQ Put Table
  ||   Step 430 - Build Manifest Reports Table
  ||   Step 440 - Tote Forecast Table Build
  ||   Step 450 - Tote Forecast Messages Extracted to MQ Put Table
  ||   Step 490 - Update Orderlines and Headers with 'T' status
  ||   Step 495 - Update Orderlines and Inventory through Ship Confirm
  ||   Step 990 - LLR is Complete
  ||
  ||----------------------------------------------------------------------------
  */
  PROCEDURE backout_sp(
    i_div          IN  VARCHAR2,
    i_rlse_ts      IN  DATE DEFAULT NULL,
    i_user_id      IN  VARCHAR2 DEFAULT 'BACKOUT',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  );

  PROCEDURE backout_sp(
    i_div             IN  VARCHAR2,
    i_rlse_ts         IN  VARCHAR2,
    i_cigwms_only_sw  IN  VARCHAR2
  );
END op_backout_llr_pk;
/

CREATE OR REPLACE PACKAGE BODY op_backout_llr_pk IS
  /*
  ||----------------------------------------------------------------------------
  || OP_BACKOUT_LLR_PK
  ||   To Backout Release
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/19/02 | Santosh | All line marked as 04/19/02, to fix multiple backout
  ||                    | error.
  || 04/26/02 | Santosh | Changed the update to set the SHPIDB column to NULL
  ||                    | only when the status is changed to an 'O' on the
  ||                    | ORDP120B and WHSP120B tables, removed all other updates.
  ||                    | Put change log header in all procedures. Formatted to
  ||                    | comply with Standards.
  || 06/02/08 | rhalpai | Replaced RESET_LLR_TABLE_SP, UPD_LAST_GOOD_STEP_SP with
  ||                    | new UPD_RLSE_SP.
  ||----------------------------------------------------------------------------
  */
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || STEP_FN
  ||  Return StepNum for StepID
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/26/10 | rhalpai | Original for PIR8531
  ||----------------------------------------------------------------------------
  */
  FUNCTION step_fn(
    i_step_id  IN  VARCHAR2
  )
    RETURN PLS_INTEGER IS
    l_step_num  PLS_INTEGER;
  BEGIN
    SELECT d.seq
      INTO l_step_num
      FROM rlse_typ_dmn_op9z d
     WHERE d.typ_id = i_step_id;

    RETURN(l_step_num);
  END step_fn;

  /*
  ||----------------------------------------------------------------------------
  || LOG_STEP_SP
  ||  Log backout process step
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/26/10 | rhalpai | Original for PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE log_step_sp(
    i_div_part  IN  NUMBER,
    i_rlse_id   IN  NUMBER,
    i_typ_id    IN  VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO rlse_log_op2z
                (div_part, rlse_id, typ_id, create_ts, val
                )
         VALUES (i_div_part, i_rlse_id, i_typ_id, SYSDATE, '~'
                );

    COMMIT;
  END log_step_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_RESENT_DIST_SP
  ||  Remove distributions resent from mainframe due to DISTOUT.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/02/16 | jxpazho | Original for PIR6285
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_resent_dist_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_rlse_ts_char  CONSTANT VARCHAR2(14) := TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS');
    l_c_sysdate       CONSTANT DATE         := SYSDATE;
    l_t_ord_nums               type_ntab;
    l_t_legcy_refs             type_stab;
  BEGIN
    SELECT a2.ordnoa, a2.legrfa
    BULK COLLECT INTO l_t_ord_nums, l_t_legcy_refs
      FROM ordp100a a, ordp120b b, ordp100a a2
     WHERE a.div_part = i_div_part
       AND a.dsorda = 'D'
       AND a.stata IN('A', 'R')
       AND a.uschga = l_c_rlse_ts_char
       AND b.div_part = a.div_part
       AND b.ordnob = a.ordnoa
       AND b.ntshpb = 'DISOUT'
       AND b.statb IN('A', 'R')
       AND b.shpidb = l_c_rlse_ts_char
       AND a2.div_part = a.div_part
       AND a2.custa = a.custa
       AND a2.dsorda = 'D'
       AND a2.legrfa = a.legrfa
       AND a2.stata = 'O';

    IF l_t_ord_nums.COUNT > 0 THEN
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM mclp300d
              WHERE div_part = i_div_part
                AND ordnod = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM ordp120b
              WHERE div_part = i_div_part
                AND ordnob = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM ordp140c
              WHERE div_part = i_div_part
                AND ordnoc = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        DELETE FROM ordp100a
              WHERE div_part = i_div_part
                AND ordnoa = l_t_ord_nums(i);
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        INSERT INTO mclp900d
                    (div_part, ordnod, ordlnd, reasnd, descd, exlvld, itemd, qtyfrd,
                     qtytod, resexd, exdesd, resdtd, restmd, last_chg_ts
                    )
             VALUES (i_div_part, l_t_ord_nums(i), 0, 'DELDIST', 'Distribution Deleted: ' || l_t_legcy_refs(i), 6, 0, 0,
                     0, '1', 'BACKOUT', 0, 0, l_c_sysdate
                    );
    END IF;   -- l_t_ord_nums.COUNT > 0
  END del_resent_dist_sp;

  /*
  ||----------------------------------------------------------------------------
  || UPD_RLSE_SP
  ||   Updates the "Release" table.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 06/02/08 | rhalpai | Original
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE upd_rlse_sp(
    i_div_part  IN  NUMBER,
    i_rlse_id   IN  NUMBER,
    i_end_ts    IN  DATE DEFAULT NULL
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE rlse_op1z r
       SET r.stat_cd = DECODE(i_end_ts, NULL, 'P', 'R'),
           r.end_ts = NVL(i_end_ts, r.end_ts)
     WHERE r.div_part = i_div_part
       AND r.rlse_id = i_rlse_id;

    COMMIT;
  END upd_rlse_sp;

  /*
  ||----------------------------------------------------------------------------
  || GET_RLSE_INFO_SP
  ||  Retrieve info for specified ReleaseTS or last Release if ReleaseTS is not
  ||  specified for division.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/26/10 | rhalpai | Original for PIR8531
  ||----------------------------------------------------------------------------
  */
  PROCEDURE get_rlse_info_sp(
    i_div          IN      VARCHAR2,
    io_rlse_ts     IN OUT  DATE,
    o_div_part     OUT     NUMBER,
    o_rlse_id      OUT     NUMBER,
    o_step         OUT     NUMBER,
    o_llr_dt       OUT     DATE,
    o_test_bil_cd  OUT     VARCHAR2,
    o_forc_inv_sw  OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.GET_RLSE_INFO_SP';
    lar_parm             logs.tar_parm;
    l_cv                 SYS_REFCURSOR;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', io_rlse_ts);
    logs.info('ENTRY', lar_parm);

    OPEN l_cv
     FOR
       SELECT r.div_part, r.rlse_ts, r.rlse_id,
              NVL((SELECT DISTINCT FIRST_VALUE(rtd.seq) OVER(ORDER BY rl.seq_of_events DESC)
                              FROM rlse_typ_dmn_op9z rtd, rlse_log_op2z rl
                             WHERE rtd.seq > -1
                               AND rtd.parnt_typ = 'RLSE'
                               AND rl.div_part = r.div_part
                               AND rl.typ_id = rtd.typ_id
                               AND rl.rlse_id = r.rlse_id),
                  0
                 ) AS step,
              r.llr_dt, r.test_bil_cd, r.forc_inv_sw
         FROM div_mstr_di1d d, rlse_op1z r
        WHERE d.div_id = i_div
          AND r.div_part = d.div_part
          AND r.rlse_ts =(CASE
                            WHEN io_rlse_ts IS NOT NULL THEN io_rlse_ts
                            ELSE (SELECT MAX(r2.rlse_ts)
                                    FROM div_mstr_di1d d, rlse_op1z r2
                                   WHERE d.div_id = i_div
                                     AND r2.div_part = d.div_part)
                          END
                         );

    FETCH l_cv
     INTO o_div_part, io_rlse_ts, o_rlse_id, o_step, o_llr_dt, o_test_bil_cd, o_forc_inv_sw;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END get_rlse_info_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_LOAD_CLOS_SP
  ||   This procedure will re-set the Updating that was done in the Ship Confirm
  ||   Process for the Released Orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/19/02 | Santosh | All marked as 04/19/02
  || 09/05/02 | SNAGABH | Added logic to adjust Protected Inventory tables for
  ||                    | backed-out Load Close/Ship Confirm process.
  ||                    | Corrected problem in section
  ||                    | 'Update Ordp120b entries not allocated on Load' where
  ||                    |  LLR date was being compared to load number.
  || 12/13/02 | rhalpai | Changed section 'Update WHSP300C Entry' to add allocated
  ||                    | order qty instead of picked qty to total allocated qty.
  ||                    | This will resolve negatives for pick adjusted items when
  ||                    | allocations are later backed out via RESET_ALLOCATIONS_SP.
  || 02/17/03 | rhalpai | Removed deletion of MCLP370C entries. Corrected update
  ||                    | of ORDP100A for ORDP120B in 'R' status iso 'A'.
  ||                    | Changed Update of MCLP370C to include new RELEASE_TS.
  || 05/13/03 | rhalpai | Changed logic to handle Backout when all orders are in
  ||                    | error and there are no WHSP200R entries. Added parm to
  ||                    | control when to reset tote entries (MCLP370C) to release
  ||                    | status. This will allow users to select the load for
  ||                    | closing and there is no need to do this when backing
  ||                    | out the entire billing pass which ends up deleting the
  ||                    | entries anyway. (prevents chance of user closing load
  ||                    | while backout is processing).
  ||                    | Also, changed logic to improve efficiency.
  || 01/13/04 | rhalpai | Added logic to support WAWA.
  || 02/19/04 | rhalpai | Removed cleanup of QTPRC08,QTPRC17,QTPRC21 MQ messages
  ||                    | from the MCLANE_MQ_PUT since backout is called
  ||                    | automatically during Ship Confirm for Test Bills and
  ||                    | will remove the messages before the java process can
  ||                    | extract them and place them on MQ.
  || 06/28/04 | rhalpai | Changes to support Cig System as cig inventory master.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  ||                    | Removed status parm from calls to CIG_BACKOUT_SHIP_SP
  ||                    | and OP_PROTECTED_INVENTORY_PK.BACKOUT_SP.
  || 10/20/06 | rhalpai | Added update to new LOAD_CLOS_CNTRL_BC2C and added
  ||                    | delete for open QOPRC28 entries. PIR3209
  || 04/14/08 | rhalpai | Changed handle backout of partial allocation.
  || 06/02/08 | rhalpai | Reformatted.
  || 04/27/10 | rhalpai | Changed logic to processs inventory updates separately
  ||                    | for Cigs and Non-Cigs and only process for Cigs while
  ||                    | Cig System is NOT master of its inventory. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  || 07/10/12 | rhalpai | Remove references to unused column SHPQTB.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||                    | Remove p_use_cig_inv_sw Parm and convert logic to
  ||                    | assume CMS uses its own inventory.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_load_clos_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE,
    i_llr_dt    IN  DATE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm               := 'OP_BACKOUT_LLR_PK.RESET_LOAD_CLOS_SP';
    lar_parm                   logs.tar_parm;
    l_c_part_id       CONSTANT PLS_INTEGER                 := TO_NUMBER(TO_CHAR(i_rlse_ts, 'DD')) - 1;
    l_c_rlse_ts_char  CONSTANT VARCHAR2(14)                := TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS');
    l_div                      div_mstr_di1d.div_id%TYPE;
    l_ordp120b_upd_cnt         PLS_INTEGER                 := 0;
    l_whsp300c_upd_cnt         PLS_INTEGER                 := 0;
    l_qoprc08_del_cnt          PLS_INTEGER                 := 0;
    l_qoprc17_del_cnt          PLS_INTEGER                 := 0;
    l_qoprc28_del_cnt          PLS_INTEGER                 := 0;
    l_qoprc21_del_cnt          PLS_INTEGER                 := 0;

    PROCEDURE upd_inv_sp(
      i_div       IN      VARCHAR2,
      i_div_part  IN      NUMBER,
      i_rlse_ts   IN      DATE,
      i_part_id   IN      NUMBER,
      o_upd_cnt   OUT     PLS_INTEGER
    ) IS
      l_t_items          type_stab;
      l_t_uoms           type_stab;
      l_t_aisls          type_stab;
      l_t_bins           type_stab;
      l_t_lvls           type_stab;
      l_t_pick_qtys      type_ntab;
      l_t_pick_adj_qtys  type_ntab;
    BEGIN
      o_upd_cnt := 0;
      logs.dbg('Get Non-Cig Pick Qtys');

      SELECT   e.iteme, e.uome, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl, SUM(b.pckqtb) AS pick_qty,
               SUM(b.alcqtb - b.pckqtb) AS pick_adj_qty
      BULK COLLECT INTO l_t_items, l_t_uoms, l_t_aisls, l_t_bins, l_t_lvls, l_t_pick_qtys,
               l_t_pick_adj_qtys
          FROM rlse_op1z r, tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i, sawp505e e, ordp120b b, ordp100a a,
               load_depart_op1f ld
         WHERE r.div_part = i_div_part
           AND r.rlse_ts = i_rlse_ts
           AND op2t.div_part = r.div_part
           AND op2t.rlse_id = r.rlse_id
           AND op2t.part_id = i_part_id
           AND op2t.tran_typ = 11
           AND op2o.div_part = op2t.div_part
           AND op2o.tran_id = op2t.tran_id
           AND op2o.part_id = op2t.part_id
           AND op2i.div_part = op2t.div_part
           AND op2i.tran_id = op2t.tran_id
           AND op2i.part_id = op2t.part_id
           AND op2i.inv_zone = '~'
           AND e.catite = LPAD(op2i.catlg_num, 6, '0')
           AND b.div_part = op2o.div_part
           AND b.ordnob = op2o.ord_num
           AND b.lineb = op2o.ord_ln
           AND b.statb = 'A'
           AND b.sllumb NOT IN('CII', 'CIR', 'CIC')
           AND a.div_part = b.div_part
           AND a.ordnoa = b.ordnob
           AND a.ipdtsa NOT IN(SELECT s.ord_src
                                 FROM sub_prcs_ord_src s
                                WHERE s.div_part = i_div_part
                                  AND s.prcs_id = 'ALLOCATE'
                                  AND s.prcs_sbtyp_cd = 'BZI')
           AND ld.div_part = a.div_part
           AND ld.load_depart_sid = a.load_depart_sid
           AND ld.llr_dt = r.llr_dt
           AND ld.load_num IN(SELECT rl.val
                                FROM rlse_log_op2z rl
                               WHERE rl.div_part = r.div_part
                                 AND rl.rlse_id = r.rlse_id
                                 AND rl.typ_id = 'LOAD')
      GROUP BY e.iteme, e.uome, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl;

      IF l_t_items.COUNT > 0 THEN
        logs.dbg('Upd Non-Cig Inventory');
        ------------------------------------------------------
        -- qty on hand = qty on hand + pick qty
        -- qty alloc   = qty alloc + pick qty
        -- qty avail   = qty avail - (qty alloc - pick qty)
        ------------------------------------------------------
        FORALL i IN l_t_items.FIRST .. l_t_items.LAST
          UPDATE whsp300c w
             SET w.qohc = w.qohc + l_t_pick_qtys(i),
                 w.qalc = w.qalc + l_t_pick_adj_qtys(i)
           WHERE w.div_part = i_div_part
             AND w.itemc = l_t_items(i)
             AND w.uomc = l_t_uoms(i)
             AND w.zonec = i_div
             AND w.aislc = l_t_aisls(i)
             AND w.binc = l_t_bins(i)
             AND w.levlc = l_t_lvls(i);
        o_upd_cnt := o_upd_cnt + SQL%ROWCOUNT;
      END IF;   -- l_t_items.COUNT > 0
    END upd_inv_sp;

    PROCEDURE del_open_mq_msgs_sp(
      i_div_part         IN      NUMBER,
      i_rlse_ts          IN      DATE,
      i_llr_dt           IN      DATE,
      i_rlse_ts_char     IN      VARCHAR2,
      o_qoprc08_del_cnt  OUT     NUMBER,
      o_qoprc17_del_cnt  OUT     NUMBER,
      o_qoprc28_del_cnt  OUT     NUMBER,
      o_qoprc21_del_cnt  OUT     NUMBER
    ) IS
    BEGIN
      logs.dbg('Get MQ Msg Counts');

      SELECT SUM(DECODE(p.mq_msg_id, 'QOPRC08', 1, 0)), SUM(DECODE(p.mq_msg_id, 'QOPRC17', 1, 0)),
             SUM(DECODE(p.mq_msg_id, 'QOPRC28', 1, 0)), SUM(DECODE(p.mq_msg_id, 'QOPRC21', 1, 0))
        INTO o_qoprc08_del_cnt, o_qoprc17_del_cnt,
             o_qoprc28_del_cnt, o_qoprc21_del_cnt
        FROM mclane_mq_put p
       WHERE p.div_part = i_div_part
         AND p.mq_msg_status = 'OPN'
         AND p.mq_msg_id IN('QOPRC08', 'QOPRC17', 'QOPRC28', 'QOPRC21')
         AND (   (    p.mq_msg_id IN('QOPRC08', 'QOPRC17', 'QOPRC21')
                  AND p.create_ts = i_rlse_ts)
              OR (    p.mq_msg_id = 'QOPRC28'
                  AND p.create_ts >= i_rlse_ts
                  AND EXISTS(SELECT 1
                               FROM ordp100a a, ordp120b b, load_depart_op1f ld
                              WHERE a.div_part = i_div_part
                                AND a.ordnoa = TO_NUMBER(SUBSTR(p.mq_msg_data, 54, 11))
                                AND b.div_part = a.div_part
                                AND b.ordnob = a.ordnoa
                                AND b.lineb = TO_NUMBER(SUBSTR(p.mq_msg_data, 65, 7)
                                                        || '.'
                                                        || SUBSTR(p.mq_msg_data, 72, 2)
                                                       )
                                AND b.statb = 'A'
                                AND b.shpidb = i_rlse_ts_char
                                AND ld.div_part = a.div_part
                                AND ld.load_depart_sid = a.load_depart_sid
                                AND ld.llr_dt = i_llr_dt)
                 )
             );

      logs.dbg('Remove MQ Msgs');

      DELETE FROM mclane_mq_put p
            WHERE p.div_part = i_div_part
              AND p.mq_msg_status = 'OPN'
              AND p.mq_msg_id IN('QOPRC08', 'QOPRC17', 'QOPRC28', 'QOPRC21')
              AND (   (    p.mq_msg_id IN('QOPRC08', 'QOPRC17', 'QOPRC21')
                       AND p.create_ts = i_rlse_ts)
                   OR (    p.mq_msg_id = 'QOPRC28'
                       AND p.create_ts >= i_rlse_ts
                       AND EXISTS(SELECT 1
                                    FROM ordp100a a, ordp120b b, load_depart_op1f ld
                                   WHERE a.div_part = i_div_part
                                     AND a.ordnoa = TO_NUMBER(SUBSTR(p.mq_msg_data, 54, 11))
                                     AND b.div_part = a.div_part
                                     AND b.ordnob = a.ordnoa
                                     AND b.lineb = TO_NUMBER(SUBSTR(p.mq_msg_data, 65, 7)
                                                             || '.'
                                                             || SUBSTR(p.mq_msg_data, 72, 2)
                                                            )
                                     AND b.statb = 'A'
                                     AND b.shpidb = i_rlse_ts_char
                                     AND ld.div_part = a.div_part
                                     AND ld.load_depart_sid = a.load_depart_sid
                                     AND ld.llr_dt = i_llr_dt)
                      )
                  );
    END del_open_mq_msgs_sp;

    PROCEDURE upd_ords_sp(
      i_div_part          IN      NUMBER,
      i_rlse_ts           IN      DATE,
      i_rlse_ts_char      IN      VARCHAR2,
      o_ordp120b_upd_cnt  OUT     NUMBER
    ) IS
    BEGIN
      logs.dbg('Upd OrdHdr');

      UPDATE ordp100a a
         SET a.stata = 'R'
       WHERE a.stata = 'A'
         AND a.div_part = i_div_part
         AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                    FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld
                                   WHERE r.div_part = i_div_part
                                     AND r.rlse_ts = i_rlse_ts
                                     AND rl.div_part = r.div_part
                                     AND rl.rlse_id = r.rlse_id
                                     AND rl.typ_id = 'LOAD'
                                     AND ld.div_part = r.div_part
                                     AND ld.llr_dt = r.llr_dt
                                     AND ld.load_num = rl.val)
         AND (   a.uschga = i_rlse_ts_char
              OR EXISTS(SELECT 1
                          FROM ordp120b b
                         WHERE b.div_part = a.div_part
                           AND b.ordnob = a.ordnoa
                           AND b.shpidb = i_rlse_ts_char)
             );

      logs.dbg('Upd OrdDtl');

      UPDATE ordp120b b
         SET b.statb = 'R'
       WHERE b.statb = 'A'
         AND b.div_part = i_div_part
         AND EXISTS(SELECT 1
                      FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                     WHERE r.div_part = i_div_part
                       AND r.rlse_ts = i_rlse_ts
                       AND rl.div_part = r.div_part
                       AND rl.rlse_id = r.rlse_id
                       AND rl.typ_id = 'LOAD'
                       AND ld.div_part = r.div_part
                       AND ld.llr_dt = r.llr_dt
                       AND ld.load_num = rl.val
                       AND a.div_part = ld.div_part
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.ordnoa = b.ordnob)
         AND b.shpidb = i_rlse_ts_char;

      o_ordp120b_upd_cnt := SQL%ROWCOUNT;
    END upd_ords_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div := div_pk.div_id_fn(i_div_part);
    logs.dbg('Reset Protected Inventory');
    -- Number of items picked is less than allocated quantity.
    -- So, there were some outs. Adjust protected inventory for outs..
    op_protected_inventory_pk.backout_sp(i_div_part, i_rlse_ts, op_protected_inventory_pk.g_c_backout_ship);
    logs.dbg('Upd Inventory');
    upd_inv_sp(l_div, i_div_part, i_rlse_ts, l_c_part_id, l_whsp300c_upd_cnt);
    COMMIT;
    logs.dbg('Remove Open MQ Messages');
    del_open_mq_msgs_sp(i_div_part,
                        i_rlse_ts,
                        i_llr_dt,
                        l_c_rlse_ts_char,
                        l_qoprc08_del_cnt,
                        l_qoprc17_del_cnt,
                        l_qoprc28_del_cnt,
                        l_qoprc21_del_cnt
                       );
    COMMIT;
    logs.dbg('Upd Orders');
    upd_ords_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char, l_ordp120b_upd_cnt);
    COMMIT;
    logs.dbg('Empty Temp Table');
    truncate_table_sp('EOE_SUM_RPT_TEMP');
    logs.info('Updated Status of Order Lines'
              || cnst.newline_char
              || cnst.newline_char
              || ' ORDP120B Updates: '
              || l_ordp120b_upd_cnt
              || cnst.newline_char
              || ' WHSP300C Updates: '
              || l_whsp300c_upd_cnt
              || cnst.newline_char
              || ' QOPRC08 Deletes : '
              || l_qoprc08_del_cnt
              || cnst.newline_char
              || ' QOPRC17 Deletes : '
              || l_qoprc17_del_cnt
              || cnst.newline_char
              || ' QOPRC28 Deletes : '
              || l_qoprc28_del_cnt
              || cnst.newline_char
              || ' QOPRC21 Deletes : '
              || l_qoprc21_del_cnt,
              lar_parm
             );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reset_load_clos_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_ORD_LNS_SP
  ||   This procedure will re-set the order lines that were allocated in the
  ||   Release of Orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/19/02 | Santosh | All marked as 04/19/02
  || 04/25/02 | Santosh | Removed shpidb = NULL from Update of ordp120b and whsp120b
  || 05/13/03 | rhalpai | Added logic to handle Government Controlled (DEA) Items
  ||                    | and improve efficiency
  || 06/11/04 | rhalpai | Changed section 'Update ORDP120B - P entries' to handle
  ||                    | original order lines for RPD subs. Both RPD subs and
  ||                    | RPI subs are changed to ITEMREP during Allocate. RPI
  ||                    | subs remain since they were created prior to Allocate
  ||                    | but RPD subs are deleted and the original order lines
  ||                    | need to be reset.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 04/14/08 | rhalpai | Changed handle backout of partial allocation.
  || 06/02/08 | rhalpai | Reformatted and removed update statement that will not
  ||                    | find rows to process since its WHERE clause duplicates
  ||                    | the previous update statement and the status is included
  ||                    | in both the SET and the WHERE clauses.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_ord_lns_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.RESET_ORD_LNS_SP';
    lar_parm             logs.tar_parm;

    PROCEDURE reset_gov_cntl_sp(
      i_div_part  IN  NUMBER,
      i_rlse_ts   IN  DATE
    ) IS
    BEGIN
      logs.dbg('Reset Customer Gov Control Table');

      UPDATE gov_cntl_cust_p640a a
         SET (a.shp_pts, a.tot_pts, a.status) =
               (SELECT a.shp_pts - SUM(log1.shp_pts), a.tot_pts - SUM(log1.tot_pts), DECODE(MIN(log1.status), 0, 0, 1)
                  FROM gov_cntl_log_p680a log1
                 WHERE log1.div_part = a.div_part
                   AND log1.gov_cntl_id = a.gov_cntl_id
                   AND log1.cust_num = a.cust_num
                   AND log1.prd_beg_ts = a.prd_beg_ts
                   AND log1.release_ts = i_rlse_ts)
       WHERE a.div_part = i_div_part
         AND EXISTS(SELECT 1
                      FROM gov_cntl_log_p680a log4
                     WHERE log4.div_part = a.div_part
                       AND log4.gov_cntl_id = a.gov_cntl_id
                       AND log4.cust_num = a.cust_num
                       AND log4.prd_beg_ts = a.prd_beg_ts
                       AND log4.release_ts = i_rlse_ts);

      -- Remove GC Customer Entries that were created because an existing
      -- period had expired
      logs.dbg('Remove Newly Created Gov Control Customer Entries');

      DELETE FROM gov_cntl_cust_p640a c
            WHERE c.div_part = i_div_part
              AND c.prd_beg_ts = i_rlse_ts
              AND c.shp_pts = 0
              AND c.tot_pts = 0
              AND c.status = 1
              AND EXISTS(SELECT 1
                           FROM gov_cntl_cust_p640a c2
                          WHERE c2.div_part = c.div_part
                            AND c2.gov_cntl_id = c.gov_cntl_id
                            AND c2.cust_num = c.cust_num
                            AND c2.prd_beg_ts < c.prd_beg_ts
                            AND c2.status = 1);

      logs.dbg('Remove Gov Control Log Entries');

      DELETE FROM gov_cntl_log_p680a
            WHERE div_part = i_div_part
              AND release_ts = i_rlse_ts;
    END reset_gov_cntl_sp;

    PROCEDURE upd_ords_sp(
      i_div_part  IN  NUMBER,
      i_rlse_ts   IN  DATE
    ) IS
      l_release_ts_char   VARCHAR2(14) := TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS');
      l_ordp120b_upd_cnt  PLS_INTEGER  := 0;
    BEGIN
      logs.dbg('Upd OrdDtl - T entries');

      UPDATE ordp120b b
         SET b.statb = 'T'
       WHERE b.div_part = i_div_part
         AND b.statb = 'R'
         AND b.alcqtb > 0
         AND b.excptn_sw = 'N'
         AND EXISTS(SELECT 1
                      FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                     WHERE r.div_part = b.div_part
                       AND r.rlse_ts = i_rlse_ts
                       AND rl.div_part = r.div_part
                       AND rl.rlse_id = r.rlse_id
                       AND rl.typ_id = 'LOAD'
                       AND ld.div_part = r.div_part
                       AND ld.llr_dt = r.llr_dt
                       AND ld.load_num = rl.val
                       AND a.div_part = ld.div_part
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.ordnoa = b.ordnob)
         AND b.shpidb = l_release_ts_char;

      l_ordp120b_upd_cnt := SQL%ROWCOUNT;
      logs.info('Updated Order Lines to Status T', lar_parm, 'Update Count: ' || l_ordp120b_upd_cnt);
    END upd_ords_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Government Control Cleanup');
    reset_gov_cntl_sp(i_div_part, i_rlse_ts);
    logs.dbg('Upd Orders');
    upd_ords_sp(i_div_part, i_rlse_ts);
    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reset_ord_lns_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_TOTE_FCAST_MSGS_SP
  ||   This procedure will delete the Tote Forecast Messages that were written to
  ||   the MQ Put Table for the Released Orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  || 06/02/08 | rhalpai | Reformatted.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_tote_fcast_msgs_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.DEL_TOTE_FCAST_MSGS_SP';
    lar_parm             logs.tar_parm;
    l_del_cnt            PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Remove the QOPRC18 and QOPRC20 Entries');

    DELETE FROM mclane_mq_put p
          WHERE p.div_part = i_div_part
            AND p.create_ts = i_rlse_ts
            AND p.mq_msg_id IN('QOPRC18', 'QTPRC18', 'QOPRC20', 'QTPRC20');

    l_del_cnt := SQL%ROWCOUNT;
    COMMIT;
    logs.info('Deleted QOPRC18 and QOPRC20 Entries from the McLane_MQ_Put Table',
              lar_parm,
              'Delete Count: ' || l_del_cnt
             );
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_tote_fcast_msgs_sp;

  /*
  ||----------------------------------------------------------------------------
  || REVERSE_TOTE_FCAST_BUILD_SP
  ||   This procedure will reverse the updating from the Released Orders to the
  ||   Tote Forecast Table
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  ||          |         |
  || 02/11/02 | rhalpai | Updated tote_count_cursor and section
  ||                    | 'Build Tote Count Cursor' to handle new bag_count column
  ||                    | on MCLANE_MANIFEST_RPTS
  || 04/26/02 | Santosh | All marked as 04/19/02
  || 04/04/03 | rhalpai | Changed Update of MCLP370C to include new RELEASE_TS.
  || 05/13/03 | rhalpai | Changed logic to improve efficiency
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  ||                    | Changed to use types type_stab, type_ntab.
  || 06/02/08 | rhalpai | Reformatted.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/01/19 | rhalpai | Add logic to reset Peco pallet count. PIR19620
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reverse_tote_fcast_build_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.REVERSE_TOTE_FCAST_BUILD_SP';
    lar_parm             logs.tar_parm;
    l_t_load_nums        type_stab;
    l_t_stop_nums        type_ntab;
    l_t_cust_nums        type_stab;
    l_t_mfst_catgs       type_stab;
    l_t_tote_catgs       type_stab;
    l_t_tote_cnts        type_ntab;
    l_t_box_cnts         type_ntab;
    l_t_bag_cnts         type_ntab;
    l_upd_cnt            PLS_INTEGER   := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Load PL/SQL Tables with Tote Count Info');

    SELECT mr.load_num, mr.stop_num, mr.cust_num, mr.manifest_cat, mr.tote_cat, mr.tote_count, mr.box_count,
           mr.bag_count
    BULK COLLECT INTO l_t_load_nums, l_t_stop_nums, l_t_cust_nums, l_t_mfst_catgs, l_t_tote_catgs, l_t_tote_cnts, l_t_box_cnts,
           l_t_bag_cnts
      FROM rlse_op1z r, rlse_log_op2z rl, mclane_manifest_rpts mr
     WHERE r.div_part = i_div_part
       AND r.rlse_ts = i_rlse_ts
       AND rl.div_part = r.div_part
       AND rl.rlse_id = r.rlse_id
       AND rl.typ_id = 'LOAD'
       AND mr.div_part = r.div_part
       AND mr.create_ts = r.rlse_ts
       AND mr.llr_date = r.llr_dt - DATE '1900-02-28'
       AND mr.load_num = rl.val
       AND mr.strategy_id > 0;

    IF l_t_load_nums.COUNT > 0 THEN
      logs.dbg('Upd MCLP370C for Tote Counts');
      FORALL i IN l_t_load_nums.FIRST .. l_t_load_nums.LAST
        UPDATE mclp370c mc
           SET mc.totsmc = mc.totsmc - l_t_tote_cnts(i),
               mc.boxsmc = mc.boxsmc - l_t_box_cnts(i),
               mc.bagsmc = mc.bagsmc - l_t_bag_cnts(i),
               mc.palsmc = 0,
               mc.cpasmc = 0,
               mc.peco_pallet_cnt = 0
         WHERE mc.div_part = i_div_part
           AND mc.llr_date = (SELECT r.llr_dt - DATE '1900-02-28'
                                FROM rlse_op1z r
                               WHERE r.div_part = i_div_part
                                 AND r.rlse_ts = i_rlse_ts)
           AND mc.loadc = l_t_load_nums(i)
           AND mc.stopc = l_t_stop_nums(i)
           AND mc.manctc = l_t_mfst_catgs(i)
           AND NVL(mc.totctc, '000') = NVL(l_t_mfst_catgs(i), '000')
           AND mc.release_ts = i_rlse_ts;
      l_upd_cnt := SQL%ROWCOUNT;
    END IF;   -- l_t_load_nums.COUNT > 0

    COMMIT;
    logs.info('Updated TOTE FORECAST Entries from the MCLP370C Table', lar_parm, 'Update Count: ' || l_upd_cnt);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reverse_tote_fcast_build_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_MFST_BUILD_SP
  ||   This procedure Deletes the Entries in the Manifest Reports Table for the
  ||   Released Orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  || 06/02/08 | rhalpai | Reformatted.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_mfst_build_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.DEL_MFST_BUILD_SP';
    lar_parm             logs.tar_parm;
    l_del_cnt            PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Remove the Manifest Report Entries');

    DELETE FROM mclane_manifest_rpts r
          WHERE r.div_part = i_div_part
            AND r.create_ts = i_rlse_ts;

    l_del_cnt := SQL%ROWCOUNT;
    COMMIT;
    logs.info('Deleted Entries from the McLane_Manifest_Rpts Table', lar_parm, 'Delete Count: ' || l_del_cnt);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_mfst_build_sp;

  /*
  ||----------------------------------------------------------------------------
  || REVERSE_ORD_EXTR_SP
  ||   This procedure Updates the Status of Orders that were extracted to the
  ||   mainframe and not allocated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 06/20/05 | rhalpai | Added reverse for Gov Control Restricted.
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  || 06/02/08 | rhalpai | Reformatted and combined update statements.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 01/20/12 | rhalpai | Change to use new column EXCPTN_SW.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reverse_ord_extr_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.REVERSE_ORD_EXTR_SP';
    lar_parm                   logs.tar_parm;
    l_c_rlse_ts_char  CONSTANT VARCHAR2(14)  := TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS');
    l_ordp120b_upd_cnt         PLS_INTEGER   := 0;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Reset OUTS / Gov Control Restricted');

    UPDATE ordp120b b
       SET b.ntshpb = NULL
     WHERE b.div_part = i_div_part
       AND b.statb = 'P'
       AND b.excptn_sw = 'N'
       AND l_c_rlse_ts_char = NVL(b.shpidb, l_c_rlse_ts_char)
       AND EXISTS(SELECT 1
                    FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                   WHERE r.div_part = b.div_part
                     AND r.rlse_ts = i_rlse_ts
                     AND rl.div_part = r.div_part
                     AND rl.rlse_id = r.rlse_id
                     AND rl.typ_id = 'LOAD'
                     AND ld.div_part = r.div_part
                     AND ld.llr_dt = r.llr_dt
                     AND ld.load_num = rl.val
                     AND a.div_part = ld.div_part
                     AND a.load_depart_sid = ld.load_depart_sid
                     AND a.ordnoa = b.ordnob)
       AND b.ntshpb IN('ITMSTRST', 'DISOUT', 'INVOUT')
       AND (   b.ntshpb = DECODE((SELECT a.dsorda
                                    FROM ordp100a a
                                   WHERE a.div_part = b.div_part
                                     AND a.ordnoa = b.ordnob), 'D', 'DISOUT', 'INVOUT')
            OR (    b.ntshpb = 'ITMSTRST'
                AND EXISTS(SELECT 1
                             FROM gov_cntl_log_p680a a
                            WHERE a.div_part = b.div_part
                              AND a.ord_num = b.ordnob
                              AND a.ord_ln = b.lineb
                              AND a.release_ts = i_rlse_ts)
               )
           );

    l_ordp120b_upd_cnt := l_ordp120b_upd_cnt + SQL%ROWCOUNT;
    COMMIT;
    logs.info('Updated Entries from the Tables', lar_parm, 'ORDP120B Update Count: ' || l_ordp120b_upd_cnt);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reverse_ord_extr_sp;

  /*
  ||----------------------------------------------------------------------------
  || DEL_WRK_ORD_MSGS_SP
  ||   This procedure Deletes the Work Order Messages that were created for the
  ||   Released Orders.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  || 06/02/08 | rhalpai | Reformatted.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE del_wrk_ord_msgs_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.DEL_WRK_ORD_MSGS_SP';
    lar_parm             logs.tar_parm;
    l_del_cnt            PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Remove the QOPRC12 Entries');

    DELETE FROM mclane_mq_put
          WHERE div_part = i_div_part
            AND create_ts = i_rlse_ts
            AND mq_msg_id IN('QOPRC12', 'QTPRC12');

    l_del_cnt := SQL%ROWCOUNT;
    COMMIT;
    logs.info('Deleted QOPRC12 Entries from the McLane_MQ_Put Table', lar_parm, 'Delete Count: ' || l_del_cnt);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END del_wrk_ord_msgs_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_ALLOCATIONS_SP
  ||   This procedure Re-Sets the orders that were released as well as the
  ||   inventory that was allocated.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/19/02 | Santosh | All marked as 04/19/02
  || 04/26/02 | Santosh | Removed shpidb= NULL from Update of ordp120b, whsp120b.
  ||                    | Also change NULL to shpidb for insert into ordp120b.
  || 09/05/02 | SNAGABH | Added logic to adjust Protected Inventory tables for
  ||                    | backed out allocation
  || 10/17/03 | rhalpai | Added logic to improve efficiency.
  || 01/13/04 | rhalpai | Added logic to support WAWA.
  || 06/11/04 | rhalpai | Changed section 'Update Original Order Lines for
  ||                    | UnConditional Subs in ORDP120B' to update originals
  ||                    | instead of subs.
  || 06/28/04 | rhalpai | Changes to support Cig System as cig inventory master.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  ||                    | Removed status parm from calls to
  ||                    | OP_PROTECTED_INVENTORY_PK.BACKOUT_SP, OP_GET_SUBS_SP
  ||                    | and CIG_BACKOUT_ALLOCATION_SP.
  ||                    | Added SAVEPOINT and exception handler for call to
  ||                    | OP_GET_SUBS_SP since it may now raise an exception and
  ||                    | the current logic continues execution with No sub found.
  || 01/06/06 | rhalpai | Changed section 'Update Original WHSP120B entry for
  ||                    | Unconditional Subs Routine' to load the original
  ||                    | exception stored in zipcdb to ntshpb (not-ship-reason).
  || 10/20/06 | rhalpai | Added delete for new BILL_CNTNR_ID_BC1C and
  ||                    | BILL_PO_OVRIDE_BC1P tables. PIR3209
  || 04/14/08 | rhalpai | Changed to use standard error handler. Added p_use_cig_inv
  ||                    | parm. Changed to use types type_stab, type_ntab.
  || 06/02/08 | rhalpai | Reformatted and changed to handle reversing RPISUBs by
  ||                    | setting the correct subcode and not-ship-reason.
  || 04/16/10 | rhalpai | Changed logic to calculate total qtys for reversal of
  ||                    | inventory transfers and then apply updates to inventory.
  ||                    | This will handle case where available inventory is
  ||                    | temporarily reduced below zero (which fires a trigger to
  ||                    | set it to zero) resulting in the wrong qty. Also added
  ||                    | logic to remove transaction type 99 (avail qty at
  ||                    | allocation). PIR0024
  || 06/01/10 | rhalpai | Added logic to undo SplitPicks (order lines allocated
  ||                    | from multiple pick locations). PIR8377
  || 07/15/10 | rhalpai | Added logic to reset OrdQty to zero for USST Default
  ||                    | Vendor Compliance (DVT) order lines. PIR8936
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/05/11 | rhalpai | Added logic to reverse adjustments to PickQty on
  ||                    | WklyMaxCustItem table for release and remove WklyMaxLog
  ||                    | entries and remove WklyMaxQty Cut MQ Msgs. PIR6235
  || 08/29/11 | rhalpai | Convert to use new transaction tables. PIR7990
  || 07/10/12 | rhalpai | Change call to OP_GET_SUBS_SP to remove unused parms.
  ||                    | Remove unused column, TICKTB.
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  ||                    | Remove i_use_cig_inv_sw Parm and convert logic to
  ||                    | assume CMS uses its own inventory.
  || 05/13/13 | rhalpai | Change logic to include removal conditional subs in
  ||                    | status R. PIR11038
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 12/08/15 | rhalpai | Change logic to pass DivPart in call to OP_GET_SUBS_SP.
  || 10/14/17 | rhalpai | Change to call CIG_OP_ALLOCATE_MAINT_PK.BACKOUT. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_allocations_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE,
    i_llr_dt    IN  DATE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm               := 'OP_BACKOUT_LLR_PK.RESET_ALLOCATIONS_SP';
    lar_parm                   logs.tar_parm;
    l_div                      div_mstr_di1d.div_id%TYPE;
    l_ordp120b_upd_cnt         PLS_INTEGER                 := 0;
    l_whsp300c_upd_cnt         PLS_INTEGER                 := 0;
    l_mclp240b_del_cnt         PLS_INTEGER                 := 0;
    l_ordp120b_del_cnt         PLS_INTEGER                 := 0;
    l_tran_del_cnt             PLS_INTEGER                 := 0;
    l_c_part_id       CONSTANT PLS_INTEGER                 := TO_NUMBER(TO_CHAR(i_rlse_ts, 'DD')) - 1;
    l_c_rlse_ts_char  CONSTANT VARCHAR2(14)                := TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS');

    PROCEDURE reset_alloc_ords_sp(
      i_div               IN      VARCHAR2,
      i_div_part          IN      NUMBER,
      i_rlse_ts           IN      DATE,
      i_part_id           IN      NUMBER,
      o_whsp300c_upd_cnt  OUT     PLS_INTEGER,
      o_ordp120b_upd_cnt  OUT     PLS_INTEGER
    ) IS
      l_t_ords       type_ntab := type_ntab();
      l_t_ord_lns    type_ntab := type_ntab();
      l_t_items      type_stab := type_stab();
      l_t_uoms       type_stab := type_stab();
      l_t_qtys       type_ntab := type_ntab();
      l_t_aisls      type_stab := type_stab();
      l_t_bins       type_stab := type_stab();
      l_t_lvls       type_stab := type_stab();
      l_t_ord_qtys   type_ntab := type_ntab();
      l_t_org_qtys   type_ntab := type_ntab();
      l_t_spcl_ords  type_stab := type_stab();
    BEGIN
      logs.dbg('Get Allocated Orders');

      SELECT b.ordnob, b.lineb, e.iteme, e.uome, op2i.qty, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl, b.ordqtb,
             b.orgqtb, DECODE(s.ord_src, NULL, 'N', 'Y') AS spcl_ord
      BULK COLLECT INTO l_t_ords, l_t_ord_lns, l_t_items, l_t_uoms, l_t_qtys, l_t_aisls, l_t_bins, l_t_lvls, l_t_ord_qtys,
             l_t_org_qtys, l_t_spcl_ords
        FROM ordp120b b, ordp100a a, rlse_op1z r, tran_op2t op2t, tran_ord_op2o op2o, tran_item_op2i op2i, sawp505e e,
             sub_prcs_ord_src s
       WHERE r.div_part = i_div_part
         AND r.rlse_ts = i_rlse_ts
         AND op2t.div_part = r.div_part
         AND op2t.rlse_id = r.rlse_id
         AND op2t.part_id = i_part_id
         AND op2t.tran_typ = 11
         AND op2o.div_part = op2t.div_part
         AND op2o.tran_id = op2t.tran_id
         AND op2o.part_id = op2t.part_id
         AND op2i.div_part = op2t.div_part
         AND op2i.tran_id = op2t.tran_id
         AND op2i.part_id = op2t.part_id
         AND e.catite = LPAD(op2i.catlg_num, 6, '0')
         AND b.div_part = op2o.div_part
         AND b.ordnob = op2o.ord_num
         AND b.lineb = op2o.ord_ln
         AND b.statb = 'T'
         AND a.div_part = b.div_part
         AND a.ordnoa = b.ordnob
         AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                    FROM rlse_op1z rr, rlse_log_op2z rl, load_depart_op1f ld
                                   WHERE rr.div_part = i_div_part
                                     AND rr.rlse_ts = i_rlse_ts
                                     AND rl.div_part = rr.div_part
                                     AND rl.rlse_id = rr.rlse_id
                                     AND rl.typ_id = 'LOAD'
                                     AND ld.div_part = rr.div_part
                                     AND ld.llr_dt = rr.llr_dt
                                     AND ld.load_num = rl.val)
         AND s.div_part(+) = a.div_part
         AND s.prcs_id(+) = 'ALLOCATE'
         AND s.prcs_sbtyp_cd(+) = 'BZI'
         AND s.ord_src(+) = a.ipdtsa;

      IF l_t_ords.COUNT > 0 THEN
        logs.dbg('Upd WHSP300C entries');
        FORALL i IN l_t_ords.FIRST .. l_t_ords.LAST
          UPDATE whsp300c w1
             SET w1.qalc = NVL(w1.qalc, 0) - l_t_qtys(i),
                 w1.qavc = NVL(w1.qavc, 0) + l_t_qtys(i),
                 w1.qstmoc = NVL(w1.qstmoc, 0) - l_t_qtys(i)
           WHERE w1.div_part = i_div_part
             AND w1.itemc = l_t_items(i)
             AND w1.uomc = l_t_uoms(i)
             AND w1.zonec = i_div
             AND w1.aislc = l_t_aisls(i)
             AND w1.binc = l_t_bins(i)
             AND w1.levlc = l_t_lvls(i)
             AND l_t_spcl_ords(i) = 'N';
        o_whsp300c_upd_cnt := SQL%ROWCOUNT;
        logs.dbg('Upd Original Order Quantities for Partials');
        FORALL i IN l_t_ords.FIRST .. l_t_ords.LAST
          UPDATE ordp120b b
             SET b.statb = 'P',
                 b.alcqtb = 0,
                 b.pckqtb = 0,
                 b.ntshpb = NULL,
                 b.subrcb = 0,
                 b.ordqtb = b.ordqtb + l_t_ord_qtys(i),
                 b.orgqtb = b.orgqtb + l_t_org_qtys(i)
           WHERE b.div_part = i_div_part
             AND b.ordnob = l_t_ords(i)
             AND b.lineb = FLOOR(l_t_ord_lns(i))
             AND l_t_ord_lns(i) - FLOOR(l_t_ord_lns(i)) = .1;
        o_ordp120b_upd_cnt := SQL%ROWCOUNT;
        logs.dbg('Upd Allocated Orders');
        FORALL i IN l_t_ords.FIRST .. l_t_ords.LAST
          UPDATE ordp120b b
             SET b.statb = 'P',
                 b.alcqtb = 0,
                 b.pckqtb = 0,
                 b.ntshpb = NULL,
                 b.subrcb = DECODE(b.lineb - FLOOR(b.lineb), .9, 1, .8, 2, .7, 3, 0)
           WHERE b.div_part = i_div_part
             AND b.ordnob = l_t_ords(i)
             AND b.lineb = l_t_ord_lns(i)
             AND b.statb = 'T';
        o_ordp120b_upd_cnt := o_ordp120b_upd_cnt + SQL%ROWCOUNT;
      END IF;   -- l_t_ords.COUNT > 0
    END reset_alloc_ords_sp;

    PROCEDURE del_cutdowns_transfers_sp(
      i_div_part  IN      NUMBER,
      i_rlse_ts   IN      DATE,
      o_cnt       OUT     NUMBER
    ) IS
    BEGIN
      DELETE FROM mclp240b mb
            WHERE mb.div_part = i_div_part
              AND mb.last_chg_ts = i_rlse_ts;

      o_cnt := SQL%ROWCOUNT;
    END del_cutdowns_transfers_sp;

    PROCEDURE inv_transfers_sp(
      i_div       IN  VARCHAR2,
      i_div_part  IN  NUMBER,
      i_rlse_ts   IN  DATE,
      i_part_id   IN  NUMBER
    ) IS
      l_t_items  type_stab := type_stab();
      l_t_uoms   type_stab := type_stab();
      l_t_aisls  type_stab := type_stab();
      l_t_bins   type_stab := type_stab();
      l_t_lvls   type_stab := type_stab();
      l_t_qtys   type_ntab := type_ntab();
    BEGIN
      logs.dbg('Get Inventory Transfers');

      SELECT   e.iteme, e.uome, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl,
               SUM(DECODE(op2t.tran_typ, 21, op2i.qty * -1, 23, op2i.qty * -1, op2i.qty)) AS inv_qty
      BULK COLLECT INTO l_t_items, l_t_uoms, l_t_aisls, l_t_bins, l_t_lvls,
               l_t_qtys
          FROM rlse_op1z r, tran_op2t op2t, tran_item_op2i op2i, sawp505e e
         WHERE r.div_part = i_div_part
           AND r.rlse_ts = i_rlse_ts
           AND op2t.div_part = r.div_part
           AND op2t.rlse_id = r.rlse_id
           AND op2t.part_id = i_part_id
           AND op2t.tran_typ IN(21, 22, 23, 24)
           AND op2i.div_part = op2t.div_part
           AND op2i.part_id = op2t.part_id
           AND op2i.tran_id = op2t.tran_id
           AND e.catite = LPAD(op2i.catlg_num, 6, '0')
      GROUP BY e.iteme, e.uome, op2i.inv_aisle, op2i.inv_bin, op2i.inv_lvl;

      IF l_t_items.COUNT > 0 THEN
        logs.dbg('Revert Inventory Transfer');
        FORALL i IN l_t_items.FIRST .. l_t_items.LAST
          UPDATE whsp300c w
             SET w.qohc = w.qohc - l_t_qtys(i),
                 w.qavc = w.qavc - l_t_qtys(i)
           WHERE w.div_part = i_div_part
             AND w.itemc = l_t_items(i)
             AND w.uomc = l_t_uoms(i)
             AND w.zonec = i_div
             AND w.aislc = l_t_aisls(i)
             AND w.binc = l_t_bins(i)
             AND w.levlc = l_t_lvls(i);
      END IF;   -- l_t_items.COUNT > 0
    END inv_transfers_sp;

    PROCEDURE del_tran_sp(
      i_div_part  IN      NUMBER,
      i_rlse_ts   IN      DATE,
      i_part_id   IN      NUMBER,
      o_cnt       OUT     NUMBER
    ) IS
    BEGIN
      DELETE FROM tran_op2t op2t
            WHERE op2t.div_part = i_div_part
              AND op2t.rlse_id = (SELECT r.rlse_id
                                    FROM rlse_op1z r
                                   WHERE r.div_part = i_div_part
                                     AND r.rlse_ts = i_rlse_ts)
              AND op2t.part_id = i_part_id;

      o_cnt := SQL%ROWCOUNT;
    END del_tran_sp;

    PROCEDURE del_bill_cntnr_ids_sp(
      i_div_part      IN  NUMBER,
      i_rlse_ts       IN  DATE,
      i_rlse_ts_char  IN  VARCHAR2
    ) IS
    BEGIN
      DELETE FROM bill_cntnr_id_bc1c c
            WHERE c.div_part = i_div_part
              AND EXISTS(SELECT 1
                           FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a, ordp120b b
                          WHERE r.div_part = c.div_part
                            AND r.rlse_ts = i_rlse_ts
                            AND rl.div_part = r.div_part
                            AND rl.rlse_id = r.rlse_id
                            AND rl.typ_id = 'LOAD'
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND ld.load_num = rl.val
                            AND a.div_part = c.div_part
                            AND a.ordnoa = c.ord_num
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND b.ordnob = a.ordnoa
                            AND b.div_part = c.div_part
                            AND b.ordnob = c.ord_num
                            AND b.lineb = c.ord_ln_num
                            AND i_rlse_ts_char = NVL(b.shpidb, i_rlse_ts_char)
                            AND b.statb IN('P', 'T', 'R'));
    END del_bill_cntnr_ids_sp;

    PROCEDURE del_bill_po_ovrides_sp(
      i_div_part      IN  NUMBER,
      i_rlse_ts       IN  DATE,
      i_rlse_ts_char  IN  VARCHAR2
    ) IS
    BEGIN
      DELETE FROM bill_po_ovride_bc1p p
            WHERE p.div_part = i_div_part
              AND EXISTS(SELECT 1
                           FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a, ordp120b b
                          WHERE r.div_part = p.div_part
                            AND r.rlse_ts = i_rlse_ts
                            AND rl.div_part = r.div_part
                            AND rl.rlse_id = r.rlse_id
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND ld.load_num = rl.val
                            AND a.div_part = p.div_part
                            AND a.ordnoa = p.ord_num
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND b.ordnob = a.ordnoa
                            AND b.div_part = p.div_part
                            AND b.ordnob = p.ord_num
                            AND b.lineb = p.ord_ln_num
                            AND i_rlse_ts_char = NVL(b.shpidb, i_rlse_ts_char)
                            AND b.statb IN('P', 'T', 'R'));
    END del_bill_po_ovrides_sp;

    PROCEDURE undo_split_picks_sp(
      i_div_part      IN  NUMBER,
      i_rlse_ts       IN  DATE,
      i_rlse_ts_char  IN  VARCHAR2
    ) IS
      l_t_ord_nums   type_ntab;
      l_t_ord_lns    type_ntab;
      l_t_orig_qtys  type_ntab;
      l_t_ord_qtys   type_ntab;
    BEGIN
      logs.dbg('Get Orig Lines and Qtys');

      SELECT   b.ordnob AS ord_num, FLOOR(b.lineb * 10) / 10 AS ord_ln, SUM(b.orgqtb) AS orig_qty,
               SUM(b.ordqtb) AS ord_qty
      BULK COLLECT INTO l_t_ord_nums, l_t_ord_lns, l_t_orig_qtys,
               l_t_ord_qtys
          FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a, ordp120b b
         WHERE r.div_part = i_div_part
           AND r.rlse_ts = i_rlse_ts
           AND rl.div_part = r.div_part
           AND rl.rlse_id = r.rlse_id
           AND rl.typ_id = 'LOAD'
           AND ld.div_part = r.div_part
           AND ld.load_num = rl.val
           AND ld.llr_dt = r.llr_dt
           AND a.div_part = ld.div_part
           AND a.load_depart_sid = ld.load_depart_sid
           AND b.div_part = a.div_part
           AND b.ordnob = a.ordnoa
           AND b.statb IN('T', 'P')
           AND b.excptn_sw = 'N'
           AND i_rlse_ts_char = NVL(b.shpidb, i_rlse_ts_char)
           AND MOD(b.lineb, .1) BETWEEN .01 AND .09
      GROUP BY b.ordnob, FLOOR(b.lineb * 10) / 10;

      logs.dbg('Upd Orig Lines');
      FORALL i IN l_t_ord_nums.FIRST .. l_t_ord_nums.LAST
        UPDATE ordp120b b
           SET b.orgqtb = b.orgqtb + l_t_orig_qtys(i),
               b.ordqtb = b.ordqtb + l_t_ord_qtys(i)
         WHERE b.div_part = i_div_part
           AND b.ordnob = l_t_ord_nums(i)
           AND b.lineb = l_t_ord_lns(i);
      logs.dbg('Remove SplitPick Lines');

      DELETE FROM ordp120b b
            WHERE b.div_part = i_div_part
              AND b.statb IN('T', 'P')
              AND b.excptn_sw = 'N'
              AND i_rlse_ts_char = NVL(b.shpidb, i_rlse_ts_char)
              AND MOD(b.lineb, .1) BETWEEN .01 AND .09
              AND EXISTS(SELECT 1
                           FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                          WHERE r.div_part = b.div_part
                            AND r.rlse_ts = i_rlse_ts
                            AND rl.div_part = r.div_part
                            AND rl.rlse_id = r.rlse_id
                            AND rl.typ_id = 'LOAD'
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND ld.load_num = rl.val
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.div_part = b.div_part
                            AND a.ordnoa = b.ordnob);
    END undo_split_picks_sp;

    PROCEDURE del_cond_subs_sp(
      i_div_part      IN      NUMBER,
      i_rlse_ts       IN      DATE,
      i_rlse_ts_char  IN      VARCHAR2,
      o_cnt           OUT     NUMBER
    ) IS
    BEGIN
      DELETE FROM ordp120b b
            WHERE b.div_part = i_div_part
              AND b.statb IN('P', 'T', 'R')
              AND i_rlse_ts_char = NVL(b.shpidb, i_rlse_ts_char)
              AND b.excptn_sw = 'N'
              AND MOD(b.lineb, 1) BETWEEN .01 AND .69
              AND EXISTS(SELECT 1
                           FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                          WHERE r.div_part = b.div_part
                            AND r.rlse_ts = i_rlse_ts
                            AND rl.div_part = r.div_part
                            AND rl.rlse_id = r.rlse_id
                            AND rl.typ_id = 'LOAD'
                            AND ld.div_part = r.div_part
                            AND ld.llr_dt = r.llr_dt
                            AND ld.load_num = rl.val
                            AND a.load_depart_sid = ld.load_depart_sid
                            AND a.div_part = b.div_part
                            AND a.ordnoa = b.ordnob);

      o_cnt := SQL%ROWCOUNT;
    END del_cond_subs_sp;

    PROCEDURE upd_ord_lns_sp(
      i_div_part           IN             NUMBER,
      i_rlse_ts            IN             DATE,
      i_rlse_ts_char       IN             VARCHAR2,
      io_ordp120b_upd_cnt  IN OUT NOCOPY  PLS_INTEGER
    ) IS
    BEGIN
      UPDATE ordp120b b
         SET b.statb = 'P',
             b.alcqtb = 0,
             b.pckqtb = 0,
             b.ntshpb = DECODE(b.lineb - FLOOR(b.lineb),
                               0, DECODE((SELECT b2.lineb - FLOOR(b2.lineb)
                                            FROM ordp120b b2
                                           WHERE b2.div_part = b.div_part
                                             AND b2.ordnob = b.ordnob
                                             AND b2.subrcb BETWEEN 1 AND 997
                                             AND FLOOR(b2.lineb) = b.lineb
                                             AND b2.lineb > b.lineb),
                                         NULL, DECODE(b.excptn_sw, 'Y', b.ntshpb),
                                         .9, 'UNCSUB',
                                         .8, 'RPISUB',
                                         .7, 'RNDSUB',
                                         DECODE(b.excptn_sw, 'Y', NVL(b.zipcdb, b.ntshpb))
                                        ),
                               DECODE(b.excptn_sw, 'Y', b.ntshpb)
                              ),
             b.subrcb = DECODE(b.lineb - FLOOR(b.lineb),
                               0, DECODE((SELECT b2.lineb - FLOOR(b2.lineb)
                                            FROM ordp120b b2
                                           WHERE b2.div_part = b.div_part
                                             AND b2.ordnob = b.ordnob
                                             AND b2.subrcb BETWEEN 1 AND 997
                                             AND FLOOR(b2.lineb) = b.lineb
                                             AND b2.lineb > b.lineb),
                                         .9, 999,
                                         .8, 999,
                                         .7, 999,
                                         0
                                        ),
                               .9, 1,
                               .8, 2,
                               .7, 3,
                               0
                              )
       WHERE b.div_part = i_div_part
         AND (   (    b.statb IN('P', 'T')
                  AND (   b.alcqtb > 0
                       OR b.ntshpb IS NOT NULL))
              OR (    b.statb = 'R'
                  AND b.shpidb = i_rlse_ts_char)
             )
         AND EXISTS(SELECT 1
                      FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                     WHERE r.div_part = b.div_part
                       AND r.rlse_ts = i_rlse_ts
                       AND rl.div_part = r.div_part
                       AND rl.rlse_id = r.rlse_id
                       AND rl.typ_id = 'LOAD'
                       AND ld.div_part = r.div_part
                       AND ld.llr_dt = r.llr_dt
                       AND ld.load_num = rl.val
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.div_part = b.div_part
                       AND a.ordnoa = b.ordnob);

      io_ordp120b_upd_cnt := io_ordp120b_upd_cnt + SQL%ROWCOUNT;
    END upd_ord_lns_sp;

    PROCEDURE create_del_subs_sp(
      i_div_part      IN  NUMBER,
      i_rlse_ts       IN  DATE,
      i_rlse_ts_char  IN  VARCHAR2
    ) IS
      l_t_ords     type_ntab     := type_ntab();
      l_t_ord_lns  type_ntab     := type_ntab();
      l_t_sub_lns  type_ntab     := type_ntab();
      l_sub_msg    VARCHAR2(500);
      l_sub_found  VARCHAR2(3);
    BEGIN
      logs.dbg('Get Deleted Subs');

      SELECT b.ordnob, b.lineb, md.ordlnd AS sub_ln
      BULK COLLECT INTO l_t_ords, l_t_ord_lns, l_t_sub_lns
        FROM mclp300d md, ordp120b b, ordp100a a
       WHERE md.div_part = i_div_part
         AND md.exdesd = i_rlse_ts_char
         AND md.reasnd = 'ORSUBDEL'
         AND md.ordlnd > FLOOR(md.ordlnd)
         AND b.div_part = md.div_part
         AND b.ordnob = md.ordnod
         AND b.lineb = FLOOR(md.ordlnd)
         AND a.div_part = b.div_part
         AND a.ordnoa = b.ordnob
         AND a.load_depart_sid IN(SELECT ld.load_depart_sid
                                    FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld
                                   WHERE r.div_part = i_div_part
                                     AND r.rlse_ts = i_rlse_ts
                                     AND rl.div_part = r.div_part
                                     AND rl.rlse_id = r.rlse_id
                                     AND rl.typ_id = 'LOAD'
                                     AND ld.div_part = r.div_part
                                     AND ld.llr_dt = r.llr_dt
                                     AND ld.load_num = rl.val);

      IF l_t_ords.COUNT > 0 THEN
        logs.dbg('Upd Orig OrdLn for Uncond Sub');
        FORALL i IN l_t_ords.FIRST .. l_t_ords.LAST
          UPDATE ordp120b b
             SET b.subrcb = 0,
                 b.ntshpb = DECODE(b.excptn_sw, 'Y', b.zipcdb),
                 b.zipcdb = DECODE(b.excptn_sw, 'Y', b.zipcdb)
           WHERE b.div_part = i_div_part
             AND b.ordnob = l_t_ords(i)
             AND b.lineb = l_t_ord_lns(i);
        <<get_subs_loop>>
        FOR i IN l_t_ords.FIRST .. l_t_ords.LAST LOOP
          BEGIN
            SAVEPOINT b4_sub;
            logs.dbg('Get Uncond Sub');
            l_sub_found := 'No';
            op_get_subs_sp(i_div_part, 'UNCSUB', l_t_ords(i), l_t_ord_lns(i), l_sub_msg, l_sub_found);

            IF l_sub_found <> 'Yes' THEN
              logs.dbg('Get Round Sub');
              op_get_subs_sp(i_div_part, 'RNDSUB', l_t_ords(i), l_t_ord_lns(i), l_sub_msg, l_sub_found);
            END IF;   -- l_sub_found <> 'Yes'
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO SAVEPOINT b4_sub;
          END;
        END LOOP get_subs_loop;
        logs.dbg('Remove Log Entries for Unconditional Subs Deletes');
        FORALL i IN l_t_ords.FIRST .. l_t_ords.LAST
          DELETE FROM mclp300d md
                WHERE md.div_part = i_div_part
                  AND md.ordnod = l_t_ords(i)
                  AND md.ordlnd = l_t_sub_lns(i)
                  AND md.reasnd = 'ORSUBDEL';
      END IF;   -- l_t_ords.COUNT > 0
    END create_del_subs_sp;

    PROCEDURE reset_dvt_vndr_cmp_sp(
      i_div_part      IN  NUMBER,
      i_rlse_ts       IN  DATE,
      i_rlse_ts_char  IN  VARCHAR2
    ) IS
    BEGIN
      logs.dbg('Reset DVT OrdQty');

      UPDATE ordp120b b
         SET b.ordqtb = 0
       WHERE b.div_part = i_div_part
         AND b.ordqtb > 0
         AND b.statb = 'P'
         AND i_rlse_ts_char = NVL(b.shpidb, i_rlse_ts_char)
         AND b.excptn_sw = 'N'
         AND EXISTS(SELECT 1
                      FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                     WHERE r.div_part = b.div_part
                       AND r.rlse_ts = i_rlse_ts
                       AND rl.div_part = r.div_part
                       AND rl.rlse_id = r.rlse_id
                       AND rl.typ_id = 'LOAD'
                       AND ld.div_part = r.div_part
                       AND ld.llr_dt = r.llr_dt
                       AND ld.load_num = rl.val
                       AND a.load_depart_sid = ld.load_depart_sid
                       AND a.ipdtsa = 'DVT'
                       AND a.div_part = b.div_part
                       AND a.ordnoa = b.ordnob);
    END reset_dvt_vndr_cmp_sp;

    PROCEDURE reset_wkly_max_qty_sp(
      i_div_part  IN  NUMBER,
      i_rlse_ts   IN  DATE
    ) IS
    BEGIN
      logs.dbg('Upd PickQty for WklyMaxCustItem');

      UPDATE wkly_max_cust_item_op1m ci
         SET ci.pick_qty = GREATEST(0,
                                    ci.pick_qty
                                    - (SELECT SUM(l.qty)
                                         FROM wkly_max_log_op3m l
                                        WHERE l.div_part = ci.div_part
                                          AND l.rlse_ts = i_rlse_ts
                                          AND l.qty_typ = 'PCK'
                                          AND l.cust_item_sid = ci.cust_item_sid)
                                   )
       WHERE ci.div_part = i_div_part
         AND EXISTS(SELECT 1
                      FROM wkly_max_log_op3m l
                     WHERE l.div_part = ci.div_part
                       AND l.rlse_ts = i_rlse_ts
                       AND l.qty_typ = 'PCK'
                       AND l.cust_item_sid = ci.cust_item_sid);

      logs.dbg('Remove WklyMaxLog Entries');

      DELETE FROM wkly_max_log_op3m l
            WHERE l.div_part = i_div_part
              AND l.rlse_ts = i_rlse_ts;

      logs.dbg('Remove IMQ62 WklyMaxQty Cut MQ Msgs');

      DELETE FROM mclane_mq_put p
            WHERE p.div_part = i_div_part
              AND p.create_ts = i_rlse_ts
              AND p.mq_msg_id = 'IMQ62';
    END reset_wkly_max_qty_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Initialize');
    l_div := div_pk.div_id_fn(i_div_part);
    logs.dbg('Reset Protected Inventory');
    op_protected_inventory_pk.backout_sp(i_div_part, i_rlse_ts, op_protected_inventory_pk.g_c_backout_rlse);
    logs.dbg('Process Allocated Orders');
    reset_alloc_ords_sp(l_div, i_div_part, i_rlse_ts, l_c_part_id, l_whsp300c_upd_cnt, l_ordp120b_upd_cnt);
    logs.dbg('Remove SSEL Cutdowns / Cig Transfers');
    del_cutdowns_transfers_sp(i_div_part, i_rlse_ts, l_mclp240b_del_cnt);
    COMMIT;
    logs.dbg('Process Inventory Transfers');
    inv_transfers_sp(l_div, i_div_part, i_rlse_ts, l_c_part_id);
    logs.dbg('Remove Tran entries');
    del_tran_sp(i_div_part, i_rlse_ts, l_c_part_id, l_tran_del_cnt);
    COMMIT;
    logs.dbg('Remove the Billing Container IDs');
    del_bill_cntnr_ids_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char);
    logs.dbg('Remove the Billing PO Overrides');
    del_bill_po_ovrides_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char);
    logs.dbg('Undo SplitPick Order Lines');
    undo_split_picks_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char);
    logs.dbg('Remove the Conditional Subs from the Good Well');
    del_cond_subs_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char, l_ordp120b_del_cnt);
    COMMIT;
    logs.dbg('Upd Remaining Order Lines');
    upd_ord_lns_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char, l_ordp120b_upd_cnt);
    COMMIT;
    logs.dbg('Create Deleted Subs Cursor');
    create_del_subs_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char);
    COMMIT;
    logs.dbg('Reset USST Default Vendor Compliance DVT OrdQty');
    reset_dvt_vndr_cmp_sp(i_div_part, i_rlse_ts, l_c_rlse_ts_char);
    logs.dbg('Reset WklyMax PickQtys and Remove WklyMaxLog Entries');
    reset_wkly_max_qty_sp(i_div_part, i_rlse_ts);
    COMMIT;
    logs.info('Reset the Allocation of Orders',
              lar_parm,
              'Delete MCLP240B Count: '
              || l_mclp240b_del_cnt
              || cnst.newline_char
              || ' Delete Tran Count: '
              || l_tran_del_cnt
              || cnst.newline_char
              || ' Update ORDP120B Count: '
              || l_ordp120b_upd_cnt
              || cnst.newline_char
              || ' Delete ORDP120B Count: '
              || l_ordp120b_del_cnt
              || cnst.newline_char
              || ' Update WHSP300C Count: '
              || l_whsp300c_upd_cnt
             );
    logs.dbg('Back Out Cig Allocation');
    cig_op_allocate_maint_pk.backout(l_div, i_rlse_ts);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reset_allocations_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_FORC_INV_SP
  ||  Reset forced inventory buildup.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 08/26/10 | rhalpai | Original for PIR8531
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.VAL_FN. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_forc_inv_sp(
    i_div_part  IN  NUMBER,
    i_rlse_id   IN  NUMBER
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.RESET_FORC_INV_SP';
    lar_parm             logs.tar_parm;
    l_forc_inv_amt       PLS_INTEGER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseId', i_rlse_id);
    logs.info('ENTRY', lar_parm);
    l_forc_inv_amt := op_parms_pk.val_fn(i_div_part, op_const_pk.prm_forc_inv_amt);

    UPDATE whsp300c w
       SET w.qohc = w.qohc - l_forc_inv_amt,
           w.qavc = w.qavc - l_forc_inv_amt
     WHERE w.div_part = i_div_part
       AND 'Y' = (SELECT r.forc_inv_sw
                    FROM rlse_op1z r
                   WHERE r.div_part = i_div_part
                     AND r.rlse_id = i_rlse_id);

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  END reset_forc_inv_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_ORDS_TO_OPN_STAT_SP
  ||   This procedure Re-Sets the orders that were released back to a 'O' Status
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 04/26/02 | Santosh | added shipidb in Update of ordp120b, whsp120b
  || 01/13/04 | rhalpai | Added logic to support WAWA.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status out parm.
  || 04/14/08 | rhalpai | Changed to handle backout of partial allocation.
  || 05/19/08 | rhalpai | Changed updates for order detail wells to include rows
  ||                    | where SHPIDB has not yet been set to billing timestamp.
  ||                    | IM411577
  || 06/02/08 | rhalpai | Reformatted and changed to call new UPD_RLSE_SP.
  || 08/04/08 | rhalpai | Added logic to only update order headers that have
  ||                    | billed to fix problem where Suspended/Cancelled status
  ||                    | on order header is getting reset to Open. IM432974
  || 08/11/08 | rhalpai | Added logic to update order header status from R to P.
  ||                    | PIR6364
  || 10/13/08 | rhalpai | Changed to not use MAX release timestamp during update
  ||                    | of order header. IM451258
  || 04/27/10 | rhalpai | Removed unused LLR parm and removed logic to change the
  ||                    | PreviousNotShipReason stored in ZipCd of the Exception
  ||                    | Order Detail Line to NULL when it contains the same
  ||                    | value as current NotShipReason. PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 01/20/12 | rhalpai | Change logic to remove excepion order well.
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_ords_to_opn_stat_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module        CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.RESET_ORDS_TO_OPN_STAT_SP';
    lar_parm                   logs.tar_parm;
    l_c_rlse_ts_char  CONSTANT VARCHAR2(14)  := TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS');
    l_upd_cnt                  PLS_INTEGER   := 0;

    CURSOR l_cur_ords(
      b_div_part      NUMBER,
      b_rlse_ts       DATE,
      b_rlse_ts_char  VARCHAR2
    ) IS
      SELECT a.ordnoa AS ord_num
        FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
       WHERE r.div_part = b_div_part
         AND r.rlse_ts = b_rlse_ts
         AND rl.div_part = r.div_part
         AND rl.rlse_id = r.rlse_id
         AND rl.typ_id = 'LOAD'
         AND ld.div_part = r.div_part
         AND ld.llr_dt = r.llr_dt
         AND ld.load_num = rl.val
         AND a.div_part = ld.div_part
         AND a.load_depart_sid = ld.load_depart_sid
         AND a.stata IN('P', 'R', 'A')
         AND EXISTS(SELECT 1
                      FROM ordp120b b
                     WHERE b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.statb IN('P', 'X')
                       AND b_rlse_ts_char = NVL(b.shpidb, b_rlse_ts_char));
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Process Orders Cursor');
    FOR l_r_ord IN l_cur_ords(i_div_part, i_rlse_ts, l_c_rlse_ts_char) LOOP
      logs.dbg('Re-Set Status on ORDP120B Table');

      UPDATE ordp120b b
         SET b.statb = 'O',
             b.shpidb = NULL
       WHERE b.div_part = i_div_part
         AND b.statb IN('P', 'X')
         AND l_c_rlse_ts_char = NVL(b.shpidb, l_c_rlse_ts_char)
         AND b.ordnob = l_r_ord.ord_num;

      l_upd_cnt := l_upd_cnt + SQL%ROWCOUNT;
      logs.dbg('Re-Set Status on ORDP100A Table');

      UPDATE ordp100a a
         SET (a.stata, a.uschga) =
               (SELECT DECODE(MIN(b.shpidb), NULL, 'O', 'P'), MIN(b.shpidb)
                  FROM ordp120b b
                 WHERE b.div_part = a.div_part
                   AND b.ordnob = l_r_ord.ord_num
                   AND b.statb NOT IN('O', 'I', 'S', 'C'))
       WHERE a.div_part = i_div_part
         AND a.ordnoa = l_r_ord.ord_num
         AND a.stata IN('P', 'R', 'A')
         AND EXISTS(SELECT 1
                      FROM ordp120b b
                     WHERE b.div_part = a.div_part
                       AND b.ordnob = l_r_ord.ord_num
                       AND b.statb = 'O');

      COMMIT;
    END LOOP;
    logs.info('Updated ORDP120B and WHSP120B Entries to a Status of ''O''', lar_parm, 'Update Count: ' || l_upd_cnt);
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reset_ords_to_opn_stat_sp;

  /*
  ||----------------------------------------------------------------------------
  || RESET_TBILL_DIST_SP
  ||   Reassigns any "DIST" orders that were temporarily attached for test bill.
  ||   Calls syncload in case a regular order came in during the test that the
  ||   "DIST" order may have attached had it been available.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/02/06 | rhalpai | Moved logic from OP_SHIP_CONFIRM_PK.BACKOUT_TESTBILL_SP
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  || 06/02/08 | rhalpai | Reformatted.
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 07/04/13 | rhalpai | Convert to use OP1F. PIR11038
  || 02/17/14 | rhalpai | Change logic to remove treat_dist_as_reg from call to
  ||                    | syncload. PIR13455
  ||----------------------------------------------------------------------------
  */
  PROCEDURE reset_tbill_dist_sp(
    i_div_part  IN  NUMBER,
    i_rlse_ts   IN  DATE
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm          := 'OP_BACKOUT_LLR_PK.RESET_TBILL_DIST_SP';
    lar_parm             logs.tar_parm;
    l_t_ord_nums         type_ntab;
    l_c_reason  CONSTANT mclp300d.reasnd%TYPE   := 'TB_RESET';
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'DivPart', i_div_part);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.info('ENTRY', lar_parm);
    logs.dbg('Reset DIST order temp reassign flag');

    UPDATE    ordp120b b
          SET b.repckb = NULL
        WHERE b.div_part = i_div_part
          AND b.repckb = 'Y'
          AND b.statb = 'O'
          AND EXISTS(SELECT 1
                       FROM rlse_op1z r, rlse_log_op2z rl, load_depart_op1f ld, ordp100a a
                      WHERE r.div_part = i_div_part
                        AND r.rlse_ts = i_rlse_ts
                        AND rl.div_part = r.div_part
                        AND rl.rlse_id = r.rlse_id
                        AND rl.typ_id = 'LOAD'
                        AND ld.div_part = r.div_part
                        AND ld.llr_dt = r.llr_dt
                        AND ld.load_num = rl.val
                        AND a.load_depart_sid = ld.load_depart_sid
                        AND a.div_part = ld.div_part
                        AND a.ordnoa = b.ordnob)
    RETURNING         b.ordnob
    BULK COLLECT INTO l_t_ord_nums;

    IF l_t_ord_nums.COUNT > 0 THEN
      logs.dbg('Remove Duplicates');
      l_t_ord_nums := SET(l_t_ord_nums);
      logs.dbg('Resync TestBill DIST order');
      op_order_load_pk.syncload_sp(i_div_part, l_c_reason, l_t_ord_nums);
    END IF;   -- l_t_ord_nums.COUNT > 0

    COMMIT;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END reset_tbill_dist_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||----------------------------------------------------------------------------
  || LAST_RLSE_SP
  ||  Return Last Release Timestamp and indicate whether CIGWMS is master of
  ||  its inventory.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/11 | rhalpai | Original for PIR0024
  || 03/20/12 | rhalpai | Remove logic referencing CIG_USE_INVENTORY Parm and
  ||                    | convert logic to assume CMS uses its own inventory.
  || 11/22/17 | rhalpai | Change logic to add out parm to indicate whether
  ||                    | backout is enabled. PIR14766
  ||----------------------------------------------------------------------------
  */
  PROCEDURE last_rlse_sp(
    i_div             IN      VARCHAR2,
    o_rlse_ts         OUT     VARCHAR2,
    o_cigwms_mstr_sw  OUT     VARCHAR2,
    o_enable_sw       OUT     VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.LAST_RLSE_SP';
    lar_parm             logs.tar_parm;
    l_frst_step          NUMBER;
    l_last_step          NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.info('ENTRY', lar_parm);
    l_frst_step := step_fn('BEGALC');
    l_last_step := step_fn('QOPRC20');

    SELECT TO_CHAR(x.rlse_ts, 'YYYY-MM-DD HH24:MI:SS'), 'Y',
           (CASE
              WHEN x.last_step BETWEEN l_frst_step AND l_last_step THEN 'Y'
              ELSE 'N'
            END
           )
      INTO o_rlse_ts, o_cigwms_mstr_sw,
           o_enable_sw
      FROM div_mstr_di1d d,
           (SELECT r.div_part, r.rlse_ts,
                   (SELECT DISTINCT FIRST_VALUE(rtd.seq) OVER(ORDER BY l.create_ts DESC, rtd.seq DESC)
                               FROM rlse_log_op2z l, rlse_typ_dmn_op9z rtd
                              WHERE l.div_part = r.div_part
                                AND l.rlse_id = r.rlse_id
                                AND rtd.typ_id = l.typ_id
                                AND rtd.parnt_typ IS NOT NULL) AS last_step
              FROM rlse_op1z r
             WHERE (r.div_part, r.rlse_ts) = (SELECT   r.div_part, MAX(r.rlse_ts)
                                                  FROM div_mstr_di1d d, rlse_op1z r
                                                 WHERE d.div_id = i_div
                                                   AND r.div_part = d.div_part
                                              GROUP BY r.div_part)) x
     WHERE d.div_id = i_div
       AND x.div_part(+) = d.div_part;

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END last_rlse_sp;
  PROCEDURE last_rlse_sp(
    i_div             IN      VARCHAR2,
    o_rlse_ts         OUT     VARCHAR2,
    o_cigwms_mstr_sw  OUT     VARCHAR2
  ) IS
    l_enable_sw  VARCHAR2(1);
  BEGIN
    logs.info('ENTRY to deprecated LAST_RLSE_SP');
    last_rlse_sp(i_div, o_rlse_ts, o_cigwms_mstr_sw, l_enable_sw);
  END last_rlse_sp;

  /*
  ||----------------------------------------------------------------------------
  || BACKOUT_SP
  ||   This procedure makes sure that all of the information needed for the
  ||   backout is correct.  It will "default" information from the McLane Load
  ||   Label Release Table if necessary.
  ||
  ||   Step 000 - Orders are in Order Well with an 'O' Status
  ||   Step 010 - Orders are flagged with a 'P' Status
  ||   Step 390 - Orders are Allocated
  ||   Step 410 - Work Orders Extracted to MQ Put Table
  ||   Step 430 - Build Manifest Reports Table
  ||   Step 440 - Tote Forecast Table Build
  ||   Step 450 - Tote Forecast Messages Extracted to MQ Put Table
  ||   Step 490 - Update Orderlines and Headers with 'T' status
  ||   Step 495 - Update Orderlines and Inventory through Ship Confirm
  ||   Step 990 - LLR is Complete
  ||
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 12/13/02 | rhalpai | Added deletion of MCLP370C entries and logic to reset
  ||                    | forced inventory (test bill option).
  || 01/21/03 | rhalpai | Changed deletion of MCLP370C entries to be limited to
  ||                    | the release currently being backed out.
  || 01/21/03 | rhalpai | Moved (and restructured) deletion of MCLP370C entries
  ||                    | to be part of section 'Reverse the Allocation of Orders'
  ||                    | to handle partial backouts.
  ||                    | Removed some unreferenced variables.
  || 04/04/03 | rhalpai | Moved 'Reset Forced Inventory' section to be part of
  ||                    | if-condition for 'Reverse the Allocation of Orders'
  ||                    | section to avoid multiple executions.
  ||                    | Added deletion of PrePost entries for release being
  ||                    | backed out to allow PrePost to be re-billed customers.
  ||                    | Changed deletion of MCLP370C entries use new RELEASE_TS.
  || 05/13/03 | rhalpai | Changed logic to only update last good step of
  ||                    | MCLANE_LOAD_LABEL_RLSE if the previous out status was
  ||                    | good.  Added parm to call to RESET_LOAD_CLOS_SP to
  ||                    | control when to reset tote entries (MCLP370C) to release
  ||                    | status. This will allow users to select the load for
  ||                    | closing and there is no need to do this when backing
  ||                    | out the entire billing pass which ends up deleting the
  ||                    | entries anyway. (prevents chance of user closing load
  ||                    | while backout is processing).
  || 06/28/04 | rhalpai | Changes to support Cig System as cig inventory master.
  || 01/26/05 | rhalpai | Changed error handler to new standard format.
  ||                    | Changed warning log to new standard format.
  ||                    | Removed status and message out parms.
  ||                    | Removed status parm from calls to RESET_LLR_TABLE_SP,
  ||                    | RESET_LOAD_CLOS_SP, RESET_ORD_LNS_SP,
  ||                    | DEL_TOTE_FCAST_MSGS_SP, REVERSE_TOTE_FCAST_BUILD_SP,
  ||                    | DEL_MFST_BUILD_SP, REVERSE_ORD_EXTR_SP,
  ||                    | DEL_WRK_ORD_MSGS_SP, RESET_ALLOCATIONS_SP
  ||                    | and RESET_ORDS_TO_OPN_STAT_SP.
  || 03/02/06 | rhalpai | Added call to reset DIST orders moved for TestBill.
  ||                    | Added process control logic. IM200261
  || 04/20/06 | rhalpai | Moved logic to "Reset DIST orders moved for TestBill"
  ||                    | after call to RESET_ORDS_TO_OPN_STAT_SP which resets the
  ||                    | orders to 'O' status. The orders were still in 'P'
  ||                    | status cursor in RESET_TBILL_DIST_SP expects 'O' status
  ||                    | so the distribution orders moved from the "unassigned"
  ||                    | DIST load for TestBills were not being reset. IM221399
  || 10/20/06 | rhalpai | Added delete for new LOAD_CLOS_CNTRL_BC2C table. PIR3209
  || 04/14/08 | rhalpai | Changed to use standard error handler.
  ||                    | Changed calls to RESET_ALLOCATIONS_SP and
  ||                    | RESET_LOAD_CLOS_SP to include use_v_cig_inv as parm.
  || 06/02/08 | rhalpai | Reformatted and changed to call new UPD_RLSE_SP.
  || 09/18/09 | rhalpai | Added logic to undo reassignment of dist orders to
  ||                    | alternate load. PIR7868
  || 01/22/10 | rhalpai | Added logic to undo Vendor Compliance order qty changes.
  ||                    | PIR8216
  || 04/27/10 | rhalpai | Removed ToStep parm and changed logic to always back out
  ||                    | all steps and changed step numbers to match Allocate.
  ||                    | PIR0024
  || 08/26/10 | rhalpai | Convert to use new RLSE tables. PIR8531
  || 11/29/12 | rhalpai | Add logic to recycle dist deletes. IM-074192
  || 01/20/12 | rhalpai | Remove logic referencing CIG_USE_INVENTORY Parm and
  ||                    | convert logic to assume CMS uses its own inventory.
  || 02/07/14 | rhalpai | Add logic to process via event. PIR13462
  || 08/02/16 | jxpazho | Add logic to remove distributions resent from mainframe
  ||                    | for distribution outs. PIR6285
  || 10/14/17 | rhalpai | Change to use new CIG_EVENT_MGR_PK.CREATE_INSTANCE and
  ||                    | CIG_EVENT_MGR_PK.UPDATE_LOG_MESSAGE. PIR15427
  ||----------------------------------------------------------------------------
  */
  PROCEDURE backout_sp(
    i_div          IN  VARCHAR2,
    i_rlse_ts      IN  DATE DEFAULT NULL,
    i_user_id      IN  VARCHAR2 DEFAULT 'BACKOUT',
    i_evnt_que_id  IN  NUMBER DEFAULT NULL,
    i_cycl_id      IN  NUMBER DEFAULT NULL,
    i_cycl_dfn_id  IN  NUMBER DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm                := 'OP_BACKOUT_LLR_PK.BACKOUT_SP';
    lar_parm             logs.tar_parm;
    l_section            VARCHAR2(80);
    l_org_id             NUMBER;
    l_evnt_que_id        NUMBER;
    l_evnt_parms         CLOB;
    l_rlse_ts            DATE                         := i_rlse_ts;
    l_div_part           NUMBER;
    l_rlse_id            NUMBER;
    l_step               PLS_INTEGER;
    l_llr_dt             DATE;
    l_llr_num            PLS_INTEGER;
    l_test_bil_cd        rlse_op1z.test_bil_cd%TYPE;
    l_forc_inv_sw        rlse_op1z.forc_inv_sw%TYPE;

    PROCEDURE upd_evnt_log_sp(
      i_evnt_msg   IN  VARCHAR2,
      i_finish_cd  IN  NUMBER DEFAULT 0
    ) IS
    BEGIN
      cig_event_mgr_pk.update_log_message(i_evnt_que_id,
                                          i_cycl_id,
                                          i_cycl_dfn_id,
                                          SUBSTR(i_evnt_msg, 1, 512),
                                          i_finish_cd
                                         );
    END upd_evnt_log_sp;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'UserId', i_user_id);
    logs.add_parm(lar_parm, 'EvntQueId', i_evnt_que_id);
    logs.add_parm(lar_parm, 'CyclId', i_cycl_id);
    logs.add_parm(lar_parm, 'CyclDfnId', i_cycl_dfn_id);
    logs.info('ENTRY', lar_parm);
    excp.assert((i_div IS NOT NULL), 'Division Code is Required');

    IF i_evnt_que_id IS NULL THEN
      logs.dbg('Get OrgId');
      l_org_id := cig_organization_pk.get_div_id(i_div);
      l_evnt_parms := '<parameters>'
                      || '<row><sequence>'
                      || 1
                      || '</sequence><value>'
                      || i_div
                      || '</value></row>'
                      || '<row><sequence>'
                      || 2
                      || '</sequence><value>'
                      || TO_CHAR(i_rlse_ts, 'YYYYMMDDHH24MISS')
                      || '</value></row>'
                      || '<row><sequence>'
                      || 3
                      || '</sequence><value>'
                      || i_user_id
                      || '</value></row>'
                      || '</parameters>';
      logs.dbg('Create Event');
      cig_event_mgr_pk.create_instance(i_org_id               => l_org_id,
                                       i_cycle_dfn_id         => cig_constants_pk.cd_ondemand,
                                       i_event_dfn_id         => cig_constants_events_pk.evd_op_backout,
                                       i_parameters           => l_evnt_parms,
                                       i_div_nm               => i_div,
                                       i_is_script_fw_exec    => 'N',
                                       i_is_complete          => 'Y',
                                       i_pgm_id               => 'PLSQL',
                                       i_user_id              => i_user_id,
                                       o_event_que_id         => l_evnt_que_id
                                      );
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);
      logs.dbg('Set Process Active');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_backout,
                                                  op_process_control_pk.g_c_active,
                                                  i_user_id,
                                                  l_div_part
                                                 );
      logs.warn('Backout Started!!!', lar_parm);
      l_section := 'Get Release Info';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      get_rlse_info_sp(i_div, l_rlse_ts, l_div_part, l_rlse_id, l_step, l_llr_dt, l_test_bil_cd, l_forc_inv_sw);
      excp.assert((l_rlse_id IS NOT NULL), 'No Release Entry found');
      excp.assert((NVL(l_step, 0) > 0), 'Last Good Step Must be greater than Zero');
      logs.dbg('Log Backout of Release Begins');
      log_step_sp(l_div_part, l_rlse_id, 'BEGBACKOUT');
      l_llr_num := l_llr_dt - DATE '1900-02-28';

      IF (    l_test_bil_cd = '~'
          AND l_step >= step_fn('QOPRC20')) THEN
        logs.dbg('Remove Dist Resent from MF for DISOUTs');
        upd_evnt_log_sp(l_section);
        del_resent_dist_sp(l_div_part, l_rlse_ts);
      END IF;   -- l_test_bil_cd = '~' AND l_step >= step_fn('QOPRC20')

      IF l_step > step_fn('ENDALC') THEN
        l_section := 'Set Rlse Status to In-Process';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        upd_rlse_sp(l_div_part, l_rlse_id);
        log_step_sp(l_div_part, l_rlse_id, 'ENDALC');
        logs.info('Updated Status on RLSE_OP1Z', lar_parm);
        l_step := step_fn('ENDALC');
      END IF;   -- l_step > step_fn('ENDALC')

      IF l_step >= step_fn('ENDALC') THEN
        l_section := 'Re-Set the Load Close Process';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        reset_load_clos_sp(l_div_part, l_rlse_ts, l_llr_dt);
        log_step_sp(l_div_part, l_rlse_id, 'UPORDST');
        l_step := step_fn('UPORDST');
      END IF;   -- l_step >= step_fn('ENDALC')

      IF l_step >= step_fn('UPORDST') THEN
        l_section := 'Re-Set the Allocated OrderLines';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        reset_ord_lns_sp(l_div_part, l_rlse_ts);
        log_step_sp(l_div_part, l_rlse_id, 'BEGTOTM');
        l_step := step_fn('BEGTOTM');
      END IF;   -- l_step >= step_fn('UPORDST')

      IF l_step >= step_fn('BEGTOTM') THEN
        l_section := 'Remove the Tote Forecast Messages';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        del_tote_fcast_msgs_sp(l_div_part, l_rlse_ts);
        log_step_sp(l_div_part, l_rlse_id, 'BEGTOTF');
        l_step := step_fn('BEGTOTF');
      END IF;   -- l_step >= step_fn('BEGTOTM')

      IF l_step >= step_fn('BEGTOTF') THEN
        l_section := 'Reverse the Tote Forecast Build';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        reverse_tote_fcast_build_sp(l_div_part, l_rlse_ts);
        log_step_sp(l_div_part, l_rlse_id, 'BLDMFST');
        l_step := step_fn('BLDMFST');
      END IF;   -- l_step >= step_fn('BEGTOTF')

      IF l_step >= step_fn('BLDMFST') THEN
        l_section := 'Remove the Manifest Report Entries';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        del_mfst_build_sp(l_div_part, l_rlse_ts);
        log_step_sp(l_div_part, l_rlse_id, 'BEGEXTO');
        l_step := step_fn('BEGEXTO');
      END IF;   -- l_step >= step_fn('BLDMFST')

      IF l_step >= step_fn('BEGEXTO') THEN
        l_section := 'Reverse the Order Extract Process';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        reverse_ord_extr_sp(l_div_part, l_rlse_ts);
        log_step_sp(l_div_part, l_rlse_id, 'BEGEXTW');
        l_step := step_fn('BEGEXTW');
      END IF;   -- l_step >= step_fn('BEGEXTO')

      IF l_step >= step_fn('BEGEXTW') THEN
        l_section := 'Remove the Work Order Messages';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        del_wrk_ord_msgs_sp(l_div_part, l_rlse_ts);
        log_step_sp(l_div_part, l_rlse_id, 'BEGALC');
        l_step := step_fn('BEGALC');
      END IF;   -- l_step >= step_fn('BEGEXTW')

      IF l_step >= step_fn('BEGALC') THEN
        l_section := 'Remove Tote Entries';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);

        DELETE FROM mclp370c t
              WHERE t.div_part = l_div_part
                AND t.release_ts = l_rlse_ts;

        l_section := 'Remove Load Close Entries';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);

        DELETE FROM load_clos_cntrl_bc2c c
              WHERE c.div_part = l_div_part
                AND c.llr_dt = l_llr_dt
                AND NOT EXISTS(SELECT 1
                                 FROM mclp370c mc
                                WHERE mc.div_part = l_div_part
                                  AND mc.llr_date = l_llr_num
                                  AND mc.loadc = c.load_num);

        l_section := 'Remove Load Log Entries';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);

        DELETE FROM load_log_op3z l
              WHERE l.div_part = l_div_part
                AND NOT EXISTS(SELECT 1
                                 FROM load_clos_cntrl_bc2c lc
                                WHERE lc.div_part = l.div_part
                                  AND lc.llr_dt = l.llr_dt
                                  AND lc.load_num = l.load_num);

        DELETE FROM load_rsn_op3r r
              WHERE r.div_part = l_div_part
                AND NOT EXISTS(SELECT 1
                                 FROM load_log_op3z l
                                WHERE l.div_part = r.div_part
                                  AND l.rsn_id = r.rsn_id);

        l_section := 'Remove PrePost Entries';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);

        DELETE FROM prepost_load_op1p p
              WHERE p.div_part = l_div_part
                AND p.last_chg_ts = l_rlse_ts;

        COMMIT;
        l_section := 'Reverse the Allocation of Orders';
        upd_evnt_log_sp(l_section);
        reset_allocations_sp(l_div_part, l_rlse_ts, l_llr_dt);

        IF l_forc_inv_sw = 'Y' THEN
          l_section := 'Reset Forced Inventory';
          upd_evnt_log_sp(l_section);
          reset_forc_inv_sp(l_div_part, l_rlse_id);
          COMMIT;
        END IF;   -- l_forc_inv_sw = 'Y'
      END IF;   -- l_step >= step_fn('BEGALC')

      l_section := 'Re-Set the Released Orders to Open Status';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      reset_ords_to_opn_stat_sp(l_div_part, l_rlse_ts);
      l_section := 'Reassign Distribution Orders to Alt Load';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      op_set_release_pk.reassgn_loads_sp(l_div_part, l_rlse_ts, 'BACKOUT', 'Y');
      l_section := 'Undo Vendor Compliance OrdQty Adjustments';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      op_set_release_pk.undo_vndr_cmp_sp(l_div_part, l_rlse_ts);
      l_section := 'Process any pending Dist Ord Deletes';
      logs.dbg(l_section);
      upd_evnt_log_sp(l_section);
      op_order_receipt_pk.del_ord_by_legcy_ref_sp(i_div);

      IF l_test_bil_cd <> '~' THEN
        l_section := 'Reset DIST orders moved for TestBill';
        logs.dbg(l_section);
        upd_evnt_log_sp(l_section);
        reset_tbill_dist_sp(l_div_part, l_rlse_ts);
      END IF;   -- l_test_bil_cd <> '~'

      COMMIT;
      log_step_sp(l_div_part, l_rlse_id, 'ENDBACKOUT');
      upd_rlse_sp(l_div_part, l_rlse_id, SYSDATE);
      logs.dbg('Set Backout Process Inactive');
      op_process_control_pk.set_process_status_sp(op_const_pk.prcs_backout,
                                                  op_process_control_pk.g_c_inactive,
                                                  i_user_id,
                                                  l_div_part
                                                 );

      IF l_test_bil_cd <> '~' THEN
        logs.dbg('Set TestBill Process Inactive');
        op_process_control_pk.set_process_status_sp(op_const_pk.prcs_test_bil,
                                                    op_process_control_pk.g_c_inactive,
                                                    i_user_id,
                                                    l_div_part
                                                   );
      END IF;   -- l_test_bil_cd <> '~'

      upd_evnt_log_sp('Backout Complete', 1);
    END IF;   -- i_evnt_que_id IS NULL

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF i_evnt_que_id IS NOT NULL THEN
        upd_evnt_log_sp('Unhandled Error: ' || SQLERRM, -1);
      END IF;   -- i_evnt_que_id IS NOT NULL

      logs.err(lar_parm);
  END backout_sp;

  /*
  ||----------------------------------------------------------------------------
  || BACKOUT_SP
  ||  Wrapper for backing out CIGWMS only or OP and CIGWMS.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 02/08/11 | rhalpai | Original for PIR0024
  || 01/20/12 | rhalpai | Remove logic referencing CIG_USE_INVENTORY Parm and
  ||                    | convert logic to assume CMS uses its own inventory.
  ||----------------------------------------------------------------------------
  */
  PROCEDURE backout_sp(
    i_div             IN  VARCHAR2,
    i_rlse_ts         IN  VARCHAR2,
    i_cigwms_only_sw  IN  VARCHAR2
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_BACKOUT_LLR_PK.BACKOUT_SP';
    lar_parm             logs.tar_parm;
    l_rlse_ts            DATE;
    l_div_part           NUMBER;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'Div', i_div);
    logs.add_parm(lar_parm, 'RlseTS', i_rlse_ts);
    logs.add_parm(lar_parm, 'CigWmsOnlySW', i_cigwms_only_sw);
    logs.info('ENTRY', lar_parm);
    l_rlse_ts := TO_DATE(i_rlse_ts, 'YYYY-MM-DD HH24:MI:SS');

    IF i_cigwms_only_sw = 'Y' THEN
      cig_op_allocate_maint_pk.backout(i_div, l_rlse_ts);
    ELSE
      l_div_part := div_pk.div_part_fn(i_div);

      DELETE FROM prcs_cntl_actv_cn3p p
            WHERE p.div_part = l_div_part
              AND p.prcs_id IN(op_const_pk.prcs_alloc,
                               op_const_pk.prcs_alloc_cigs,
                               op_const_pk.prcs_rlse_alloc,
                               op_const_pk.prcs_rlse_compl
                              );

      COMMIT;
      backout_sp(i_div, l_rlse_ts);
    END IF;   -- i_cigwms_only_sw = 'Y'

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END backout_sp;
END op_backout_llr_pk;
/

