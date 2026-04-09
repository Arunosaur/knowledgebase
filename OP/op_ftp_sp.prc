CREATE OR REPLACE PROCEDURE op_ftp_sp(
  i_div         IN  VARCHAR2,
  i_local_file  IN  VARCHAR2,
  i_rmt_file    IN  VARCHAR2,
  i_archv_sw    IN  VARCHAR2 DEFAULT 'Y',
  i_gdg_sw      IN  VARCHAR2 DEFAULT 'N'
) IS
  /**
  ||----------------------------------------------------------------------------
  || Will ftp a local file on unix to the mainframe.
  || When archive is 'Y' the unix script will ftp in zip format and then
  || archive when done, otherwise, it will ftp in ascii (text,flat file) format.
  || #param i_div             Division ID ie: MW,NE,SW,etc.
  || #param i_local_file      The file name to ftp. If passed without path then
  ||                          the directory must match the source directory
  ||                          found in the divisional ftp properties file.
  || #param i_rmt_file        This is the final qualifier(s) of the dataset
  ||                          name on the mainframe.
  ||                          ie: RMTFILE in the following: SW.PERM.OP.RMTFILE
  || #param i_archv_sw        Archive local file after ftp?
  ||                          Valid values are:
  ||                          {*} 'Y' (default)
  ||                          {*} 'N' (any char other than 'Y' will not archive)
  || #param i_gdg_sw          Is remote file a GDG?
  ||                          Valid values are:
  ||                          {*} 'Y'
  ||                          {*} 'N' (default)
  ||----------------------------------------------------------------------------
  */

  /*
  ||----------------------------------------------------------------------------
  || OP_FTP_SP
  ||  Will ftp a local file on unix to the mainframe.
  ||----------------------------------------------------------------------------
  ||             C H A N G E     L O G
  ||----------------------------------------------------------------------------
  || Date     | USERID  | Changes
  ||----------------------------------------------------------------------------
  || 03/10/04 | rhalpai | Original
  || 12/02/05 | rhalpai | Changed to look up parms from unix properties file
  ||                    | instead of using AP1S parms table. This resolves
  ||                    | potential of updating production files from a test
  ||                    | database when AP1S table data has been copied from
  ||                    | a production database.
  ||                    | Changed error handler to new standard format. PIR2051
  || 08/27/09 | rhalpai | Added logic to handle ftp to a mainframe GDG
  || 10/05/09 | rhalpai | Change to ftp in binary for zipped file and ASCII for
  ||                    | non-zipped files. IM536447
  || 05/13/13 | rhalpai | Replace properties file with parms table. PIR11038
  || 11/25/15 | rhalpai | Change to pass div_part to op_parms_pk.
  || 10/14/17 | rhalpai | Change to call new OP_PARMS_PK.IDX_VALS_FN. PIR15427
  || 02/19/20 | rhalpai | Change oscmd_fn call to pass app server parameter and
  ||                    | remove command logic to ssh to app server. PIR19616
  ||----------------------------------------------------------------------------
  */
  l_c_module       CONSTANT VARCHAR2(30)                        := 'OP_FTP_SP';
  lar_parm                  logs.tar_parm;
  l_div_part                NUMBER;
  l_t_parms                 op_types_pk.tt_varchars_v;
  l_c_local_dir    CONSTANT VARCHAR2(20)                        := 'FTP_LOCAL_DIR';
  l_c_archv_dir    CONSTANT VARCHAR2(20)                        := 'FTP_ARCHIVE_DIR';
  l_c_rmt_loc      CONSTANT VARCHAR2(20)                        := 'FTP_RMT_LOCATION';
  l_c_rmt_host     CONSTANT VARCHAR2(20)                        := 'FTP_RMT_HOST';
  l_c_ftp_user_id  CONSTANT VARCHAR2(20)                        := 'FTP_USERID';
  l_c_ftp_pwd      CONSTANT VARCHAR2(20)                        := 'FTP_PWD';
  l_appl_srvr               appl_sys_parm_ap1s.vchar_val%TYPE;
  l_cmd                     typ.t_maxvc2;
  l_os_result               typ.t_maxvc2;
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'Div', i_div);
  logs.add_parm(lar_parm, 'LocalFile', i_local_file);
  logs.add_parm(lar_parm, 'RmtFile', i_rmt_file);
  logs.add_parm(lar_parm, 'ArchvSw', i_archv_sw);
  logs.add_parm(lar_parm, 'GdgSw', i_gdg_sw);
  logs.info('ENTRY', lar_parm);
  logs.dbg('Initialize');
  l_div_part := div_pk.div_part_fn(i_div);
  l_appl_srvr := op_parms_pk.val_fn(l_div_part, op_const_pk.prm_appl_srvr);
  l_t_parms := op_parms_pk.idx_vals_fn(l_div_part,
                                       l_c_local_dir
                                       || ','
                                       || l_c_archv_dir
                                       || ','
                                       || l_c_rmt_loc
                                       || ','
                                       || l_c_rmt_host
                                       || ','
                                       || l_c_ftp_user_id
                                       || ','
                                       || l_c_ftp_pwd
                                      );
  logs.dbg('Build Command');
  l_cmd := '/local/prodcode/bin/xxopFTP.scr "'
           || i_div
           || '" "'
           || l_t_parms(l_c_local_dir)
           || '" "'
           || i_local_file
           || '" "'
           || l_t_parms(l_c_rmt_loc)
           || '" "'
           || i_rmt_file
           || '" "'
           || l_t_parms(l_c_rmt_host)
           || '" "'
           || l_t_parms(l_c_ftp_user_id)
           || '" "'
           || l_t_parms(l_c_ftp_pwd)
           || '" "'
           ||(CASE NVL(i_archv_sw, 'Y')
                WHEN 'Y' THEN l_t_parms(l_c_archv_dir)
              END)
           || '" "'
           || NVL(i_gdg_sw, 'N')
           || '"';
  logs.dbg('Run Command');
  logs.dbg(l_cmd);
  l_os_result := oscmd_fn(l_cmd, l_appl_srvr);
  logs.info(l_os_result);
  logs.warn('FTP Failed!' || cnst.newline_char || l_os_result);
/*  excp.assert((INSTR(l_os_result, 'Transfer completed successfully') > 0),
              'FTP Failed!' || cnst.newline_char || l_os_result
             );*/
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    logs.err(lar_parm);
END op_ftp_sp;
/

