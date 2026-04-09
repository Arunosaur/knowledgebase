CREATE OR REPLACE PACKAGE op_dw_extract_pk IS
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
  PROCEDURE archive_sp(
    i_llr_dt  IN  DATE DEFAULT NULL
  );

END op_dw_extract_pk;
/

CREATE OR REPLACE PACKAGE BODY op_dw_extract_pk IS
--------------------------------------------------------------------------------
--                 PACKAGE CONSTANTS, VARIABLES, TYPES, EXCEPTIONS
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--                        PRIVATE FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || DW_EXTR_MAX_DT_FN
  ||  Returns data warehouse extract max date.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  FUNCTION dw_extr_max_dt_fn
    RETURN DATE IS
  BEGIN
    RETURN(TRUNC(SYSDATE) + TO_NUMBER(NVL(op_parms_pk.val_fn(0, 'DW_EXTR_MAX_DAYS'), -5)));
  END dw_extr_max_dt_fn;

  /*
  ||-----------------------------------------------------------------------------
  || LAST_DW_EXTR_DT_FN
  ||  Returns last data warehouse extract date.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  FUNCTION last_dw_extr_dt_fn
    RETURN DATE IS
    l_test_db_sw  VARCHAR2(1);
    l_dir         VARCHAR2(100);
    l_appl_srvr   appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd         typ.t_maxvc2;
    l_os_result   typ.t_maxvc2;
    l_valid_sw    VARCHAR2(1);
    l_dt          DATE;
  BEGIN
    l_test_db_sw :=(CASE
                      WHEN SUBSTR(ora_database_name, -1) = 'P' THEN 'N'
                      ELSE 'Y'
                    END);
    l_dir := (CASE l_test_db_sw
                WHEN 'N' THEN '/CognosPROD/Prod'
                ELSE '/CognosDEV/Test'
              END) || '/Datasource/OP_Data_Files';
    l_appl_srvr := op_parms_pk.val_fn(0, op_const_pk.prm_appl_srvr);
    l_cmd := 'ls '
             || l_dir
             || '/DW_EXTR_*.zip | grep -Eo ''[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}'' | sort -r | head -1';
    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
    l_os_result := SUBSTR(l_os_result, INSTR(l_os_result, CHR(10)) + 1);   -- remove first line:  Only McLane Authorized users are permitted to login to the McLane Network.

    SELECT DECODE(validate_conversion(l_os_result AS DATE, 'YYYY-MM-DD'), 1, 'Y', 'N')
      INTO l_valid_sw
      FROM DUAL;

    IF l_valid_sw = 'Y' THEN
      l_dt := TO_DATE(l_os_result, 'YYYY-MM-DD');
    ELSE
      l_dt := dw_extr_max_dt_fn - 1;
    END IF;

    RETURN(l_dt);
  END last_dw_extr_dt_fn;

  /*
  ||-----------------------------------------------------------------------------
  || HIST_ORD_HDR_FN
  ||  Returns history order header report lines.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  FUNCTION hist_ord_hdr_fn(
    i_t_ord_num  IN  type_ntab
  )
    RETURN typ.tas_maxvc2 IS
    l_t_rpt_lns  typ.tas_maxvc2;
  BEGIN
    SELECT a.div_part
           || ','
           || a.ordnoa
           || ',"'
           || a.custa
           || '","'
           || a.cpoa
           || '",'
           || TO_CHAR(DATE '1900-02-28' + a.shpja, 'YYYY-MM-DD HH24:MI:SS')
           || ',"'
           || a.dsorda
           || '","'
           || DECODE(a.pshipa, 'Y', 'Y', '1', 'Y', 'N')
           || '","'
           || a.orrtea
           || '",'
           || a.stopsa
           || ',"'
           || a.ldtypa
           || '",'
--           || a.trndta
--           || ','
--           || a.trntma
           || TO_CHAR(TO_DATE('19000228' || LPAD(a.trntma, 6, '0'), 'YYYYMMDDHH24MISS') + a.trndta, 'YYYY-MM-DD HH24:MI:SS')
           || ',"'
           || a.ipdtsa
           || '","'
           || a.cspasa
           || '","'
           || a.stata
           || '","'
           || a.telsla
           || '","'
           || a.hdexpa
           || '","'
           || a.uschga
           || '","'
           || a.mntusa
           || '","'
           || a.connba
           || '",'
--           || a.ctofda
--           || ','
--           || a.ctofta
           || TO_CHAR(TO_DATE('19000228' || LPAD(a.ctofta, 4, '0'), 'YYYYMMDDHH24MI') + a.ctofda, 'YYYY-MM-DD HH24:MI:SS')
           || ','
           || NVL((SELECT TO_CHAR(TO_DATE('19000228' || LPAD(b.deptmb, 4, '0'), 'YYYYMMDDHH24MI') + b.depdtb, 'YYYY-MM-DD HH24:MI:SS')
                     FROM ordp920b b
                    WHERE b.div_part = a.div_part
                      AND b.ordnob = a.ordnoa
                      AND ROWNUM = 1),
                  '1900-01-01 00:00:00'
                 )
           || ','
--           || a.etadta
--           || ','
--           || a.etatma
           || TO_CHAR(TO_DATE('19000228' || LPAD(a.etatma, 4, '0'), 'YYYYMMDDHH24MI') + a.etadta, 'YYYY-MM-DD HH24:MI:SS')
           || ',"'
           || a.rsncda
           || '","'
           || a.legrfa
           || '",'
           || TO_CHAR(a.ord_rcvd_ts, 'YYYY-MM-DD HH24:MI:SS')
           || ',"'
           || a.excptn_sw
           || '"'
      BULK COLLECT INTO l_t_rpt_lns
      FROM ordp900a a
     WHERE a.ordnoa IN(SELECT t.column_value FROM TABLE(i_t_ord_num) t);

    RETURN(l_t_rpt_lns);
  END hist_ord_hdr_fn;

  /*
  ||-----------------------------------------------------------------------------
  || HIST_ORD_DTL_FN
  ||  Returns history order detail report lines.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  FUNCTION hist_ord_dtl_fn(
    i_t_ord_num  IN  type_ntab
  )
    RETURN typ.tas_maxvc2 IS
    l_t_rpt_lns  typ.tas_maxvc2;
  BEGIN
    SELECT b.div_part
           || ','
           || b.ordnob
           || ','
           || b.lineb
           || ',"'
           || b.statb
           || '","'
           || b.itemnb
           || '",'
           || b.ordqtb
           || ','
           || b.alcqtb
           || ','
           || b.pckqtb
           || ',"'
           || b.shpidb
           || '",'
           || b.orgqtb
           || ',"'
           || b.sllumb
           || '","'
           || b.cusitb
           || '","'
           || b.zipcdb
           || '","'
           || b.orditb
           || '","'
           || b.itpasb
           || '","'
           || DECODE(b.rtfixb, 'Y', 'Y', '1', 'Y', 'N')
           || '","'
           || DECODE(b.prfixb, 'Y', 'Y', '1', 'Y', 'N')
           || '",'
           || b.hdrtab
           || ','
           || b.hdrtmb
           || ','
           || b.hdprcb
           || ','
           || b.actqtb
           || ',"'
           || DECODE(b.bymaxb, 'Y', 'Y', '1', 'Y', 'N')
           || '","'
           || b.manctb
           || '","'
           || b.totctb
           || '","'
           || DECODE(b.authb, 'Y', 'Y', '1', 'Y', 'N')
           || '","'
           || b.ntshpb
           || '",'
--           || b.prstdb
--           || ','
--           || b.prsttb
           || TO_CHAR(TO_DATE('19000228' || LPAD(b.prsttb, 6, '0'), 'YYYYMMDDHH24MISS') + b.prstdb, 'YYYY-MM-DD HH24:MI:SS')
           || ','
           || b.maxqtb
           || ','
           || b.qtmulb
           || ','
           || b.invctb
           || ','
           || b.labctb
--           || ','
--           || b.depdtb
--           || ','
--           || b.deptmb
--           || TO_CHAR(TO_DATE('19000228' || LPAD(b.deptmb, 4, '0'), 'YYYYMMDDHH24MI') + b.depdtb, 'YYYY-MM-DD HH24:MI:SS')
           || ',"'
           || b.subrcb
           || '","'
           || NVL(b.repckb, 'N')
           || '","'
           || b.orgitb
           || '","'
           || b.excptn_sw
           || '"'
      BULK COLLECT INTO l_t_rpt_lns
      FROM ordp920b b
     WHERE b.ordnob IN(SELECT t.column_value FROM TABLE(i_t_ord_num) t);

    RETURN(l_t_rpt_lns);
  END hist_ord_dtl_fn;

  /*
  ||-----------------------------------------------------------------------------
  || HIST_ORD_EXCPTN_FN
  ||  Returns history order exception report lines.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  FUNCTION hist_ord_excptn_fn(
    i_t_ord_num  IN  type_ntab
  )
    RETURN typ.tas_maxvc2 IS
    l_t_rpt_lns  typ.tas_maxvc2;
  BEGIN
    SELECT d.div_part
           || ','
           || d.ordnod
           || ','
           || d.ordlnd
           || ',"'
           || d.reasnd
           || '","'
           || d.descd
           || '",'
           || d.exlvld
           || ',"'
           || d.itemd
           || '","'
           || d.uomd
           || '","'
           || d.repitd
           || '","'
           || d.repumd
           || '","'
           || d.repsbd
           || '",'
           || d.qtyfrd
           || ','
           || d.qtytod
           || ','
           || d.exdtd
           || ','
           || d.exmond
           || ','
           || d.extimd
           || ',"'
           || d.resexd
           || '","'
           || d.exdesd
           || '","'
           || d.resusd
           || '",'
           || d.resdtd
           || ','
           || d.restmd
           || ','
           || TO_CHAR(d.last_chg_ts, 'YYYY-MM-DD HH24:MI:SS')
      BULK COLLECT INTO l_t_rpt_lns
      FROM mclp900d d
     WHERE d.ordnod IN(SELECT t.column_value FROM TABLE(i_t_ord_num) t);

    RETURN(l_t_rpt_lns);
  END hist_ord_excptn_fn;

  /*
  ||-----------------------------------------------------------------------------
  || HIST_ORD_AUDIT_FN
  ||  Returns history order audit report lines.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  FUNCTION hist_ord_audit_fn(
    i_t_ord_num  IN  type_ntab
  )
    RETURN typ.tas_maxvc2 IS
    l_t_rpt_lns  typ.tas_maxvc2;
  BEGIN
    SELECT a.div_part

           || ','
           || a.ordnoa
           || ','
           || a.linea
           || ',"'
           || a.acnoa
           || '","'
           || a.usera
           || '","'
           || a.tblnma
           || '","'
           || a.fldnma
           || '","'
           || a.florga
           || '","'
           || a.flchga
           || '","'
           || a.actna
           || '","'
           || a.rsncda
           || '",'
--           || a.datea
--           || ','
--           || a.timea
           || TO_CHAR(TO_DATE('19000228' || LPAD(a.timea, 6, '0'), 'YYYYMMDDHH24MISS') + a.datea, 'YYYY-MM-DD HH24:MI:SS')
           || ',"'
           || a.autbya
           || '","'
           || a.rsntxa
           || '"'
      BULK COLLECT INTO l_t_rpt_lns
      FROM sysp996a a
     WHERE a.ordnoa IN(SELECT t.column_value FROM TABLE(i_t_ord_num) t);

    RETURN(l_t_rpt_lns);
  END hist_ord_audit_fn;

  /*
  ||-----------------------------------------------------------------------------
  || ZIP_SP
  ||  Add to zip file
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE zip_sp(
    i_ts     IN  VARCHAR2,
    i_mv_sw  IN  VARCHAR2 DEFAULT 'N'
  ) IS
    l_test_db_sw  VARCHAR2(1);
    l_appl_srvr   appl_sys_parm_ap1s.vchar_val%TYPE;
    l_cmd         typ.t_maxvc2;
    l_os_result   typ.t_maxvc2;
  BEGIN
    l_test_db_sw :=(CASE
                      WHEN SUBSTR(ora_database_name, -1) = 'P' THEN 'N'
                      ELSE 'Y'
                    END);
    l_appl_srvr := op_parms_pk.val_fn(0, op_const_pk.prm_appl_srvr);
    l_cmd := 'cd /ftptrans;'
             || CHR(10)
             || 'chmod 666 *'
             || i_ts
             || '.csv;'
             || CHR(10)
             || 'zip -m -9 DW_EXTR_'
             || i_ts
             || '.zip *'
             || i_ts
             || '.csv;';

    IF i_mv_sw = 'Y' THEN
      l_cmd := l_cmd
               || CHR(10)
               || 'mv DW_EXTR_'
               || i_ts
               || '.zip '
               ||(CASE l_test_db_sw
                    WHEN 'N' THEN '/CognosPROD/Prod'
                    ELSE '/CognosDEV/Test'
                  END)
               || '/Datasource/OP_Data_Files';
--      l_cmd := l_cmd || CHR(10) || 'mv DW_EXTR_' || l_c_ts || '.zip /ftptrans/transmitted_files';
    END IF;

    l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
  END zip_sp;

  /*
  ||-----------------------------------------------------------------------------
  || ARCHIVE_LLR_SP
  ||  Extract tables for LLR, zip and move to data warehouse directory.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE archive_llr_sp(
    i_llr_dt  IN  DATE DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm  := 'OP_DW_EXTRACT_PK.ARCHIVE_LLR_SP';
    lar_parm             logs.tar_parm;
    l_llr_dt             DATE           := NVL(i_llr_dt, dw_extr_max_dt_fn);
    l_c_ts      CONSTANT VARCHAR2(10)   := TO_CHAR(l_llr_dt, 'YYYY-MM-DD');
    l_t_ord_num          type_ntab;
    l_t_rpt_lns          typ.tas_maxvc2;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.info('ENTRY', lar_parm);

    SELECT a.ordnoa
    BULK COLLECT INTO l_t_ord_num
      FROM ordp900a a
     WHERE DATE '1900-02-28' + a.ctofda = l_llr_dt;

    IF l_t_ord_num.COUNT > 0 THEN
      l_t_rpt_lns := hist_ord_hdr_fn(l_t_ord_num);
      write_sp(l_t_rpt_lns, 'ORDP900A_' || l_c_ts || '.csv');
      zip_sp(l_c_ts);
      l_t_rpt_lns := hist_ord_dtl_fn(l_t_ord_num);
      write_sp(l_t_rpt_lns, 'ORDP920B_' || l_c_ts || '.csv');
      zip_sp(l_c_ts);
      l_t_rpt_lns := hist_ord_excptn_fn(l_t_ord_num);
      write_sp(l_t_rpt_lns, 'MCLP900D_' || l_c_ts || '.csv');
      zip_sp(l_c_ts);
      l_t_rpt_lns := hist_ord_audit_fn(l_t_ord_num);
      write_sp(l_t_rpt_lns, 'SYSP996A_' || l_c_ts || '.csv');
      zip_sp(l_c_ts, 'Y');
    END IF;   -- l_t_ord_num.COUNT > 0

    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END archive_llr_sp;

--------------------------------------------------------------------------------
--                        PUBLIC FUNCTIONS AND PROCEDURES
--------------------------------------------------------------------------------
  /*
  ||-----------------------------------------------------------------------------
  || ARCHIVE_SP
  ||  Extract tables, zip and move to data warehouse directory.
  ||-----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||-----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||-----------------------------------------------------------------------------
  || 04/08/22 | rhalpai | Original. PIR21517
  ||-----------------------------------------------------------------------------
  */
  PROCEDURE archive_sp(
    i_llr_dt  IN  DATE DEFAULT NULL
  ) IS
    l_c_module  CONSTANT typ.t_maxfqnm := 'OP_DW_EXTRACT_PK.ARCHIVE_SP';
    lar_parm             logs.tar_parm;
    l_dt                 DATE;
    l_max_dt             DATE;
  BEGIN
    timer.startme(l_c_module || env.get_session_id);
    logs.add_parm(lar_parm, 'LLRDt', i_llr_dt);
    logs.info('ENTRY', lar_parm);
    l_dt := NVL(i_llr_dt, last_dw_extr_dt_fn + 1);
    l_max_dt := NVL(i_llr_dt, dw_extr_max_dt_fn);
    WHILE l_dt <= l_max_dt LOOP
      archive_llr_sp(l_dt);
      l_dt := l_dt + 1;
    END LOOP;
    timer.stopme(l_c_module || env.get_session_id);
    logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
  EXCEPTION
    WHEN OTHERS THEN
      logs.err(lar_parm);
  END archive_sp;
END op_dw_extract_pk;
/

