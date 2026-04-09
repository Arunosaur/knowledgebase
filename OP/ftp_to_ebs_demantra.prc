CREATE OR REPLACE PROCEDURE ftp_to_ebs_demantra(i_local_file    VARCHAR2,
                                                        i_remote_file   VARCHAR2,
                                                        i_archv_sw    IN  VARCHAR2 DEFAULT 'Y',
                                                        i_gdg_sw      IN  VARCHAR2 DEFAULT 'N'
                                                       )
   /*
   ||---------------------------------------------------------------------------
   || NAME                : ftp_to_ebs_demantra
   || DESCRIPTION         : FTP OP cuts data extract to EBS Demantra.
   ||
   || Check in /oplogs for status of SFTP.
   || Ftp script in /local/prodcode/bin/opEBS_FTP.scr
   ||---------------------------------------------------------------------------
   ||             C H A N G E     L O G
   ||---------------------------------------------------------------------------
   || Date       | USERID  | Changes
   ||---------------------------------------------------------------------------
   || 07/16/2021 | jxpazho    | Original
   ||---------------------------------------------------------------------------
   */
   AS
      l_tt_parms   LOGS.TAR_PARM;

      l_appl_server          VARCHAR2(20);
      l_local_dir            VARCHAR2(20);
      l_cmd                  VARCHAR2(1000);

      l_archive_loc          VARCHAR(250);
      l_user_id              VARCHAR2(25);
      l_password             VARCHAR2(25);
      l_remote_host_nm       VARCHAR2(250);
      l_remote_loc           VARCHAR2(250);
      l_os_result            VARCHAR2(32767);

      L_E_NO_DIVSION_NAME        EXCEPTION;
      L_E_NO_LABELDATA_FILE      EXCEPTION;
   BEGIN
      LOGS.add_parm(l_tt_parms, 'i_local_file', i_local_file);
      LOGS.add_parm(l_tt_parms, 'i_remote_file', i_remote_file);
      logs.add_parm(l_tt_parms, 'ArchvSw', i_archv_sw);
      logs.add_parm(l_tt_parms, 'GdgSw', i_gdg_sw);

      IF (   i_local_file IS NULL
          OR i_remote_file IS NULL
         )
      THEN
         RAISE  L_E_NO_LABELDATA_FILE;
      END IF;

      SELECT a.vchar_val
       INTO l_appl_server
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'APPL_SRVR';

       LOGS.info('l_appl_server: ', l_appl_server);

       SELECT a.vchar_val
       INTO l_local_dir
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'FTP_LOCAL_DIR';

       LOGS.info('l_local_dir: ', l_local_dir);

       SELECT a.vchar_val
       INTO l_archive_loc
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'FTP_ARCHIVE_DIR';

       LOGS.info('l_archive_loc: ', l_archive_loc);

       SELECT a.vchar_val
       INTO l_remote_host_nm
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'FTP_EBS_RMT_HOST';

       LOGS.info('l_remote_host_nm: ', l_remote_host_nm);

       SELECT a.vchar_val
       INTO l_user_id
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'FTP_EBS_USERID';

       LOGS.info('l_user_id: ', l_user_id);

       SELECT a.vchar_val
       INTO l_password
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'FTP_EBS_PWD';

       LOGS.info('l_password: ', l_password);

       SELECT a.vchar_val
       INTO l_remote_loc
       FROM appl_sys_parm_ap1s a
       WHERE a.appl_id = 'OP'
       AND   a.parm_id =  'FTP_EBS_RMT_LOCATION';

       LOGS.info('l_remote_loc: ', l_remote_loc);

      l_appl_server := OP_PARMS_PK.val_fn(div_pk.div_part_fn('MC'), 'APPL_SRVR'); -- just any div name to get the app server

      logs.dbg('Build Command');
      l_cmd := '/local/prodcode/bin/opEBS_FTP.scr "'
               || l_local_dir
               || '" "'
               || i_local_file
               || '" "'
               ||(CASE NVL(i_archv_sw, 'Y')
                    WHEN 'Y' THEN l_archive_loc
                  END)
               || '" "'
               || l_remote_host_nm
               || '" "'
               || l_remote_loc
               || '" "'
               || i_remote_file
               || '" "'
               || l_user_id
               || '" "'
               || l_password
               || '" "'
               || NVL(i_gdg_sw, 'N')
               || '"';
      logs.dbg('Run Command');
      logs.dbg(l_cmd);
      l_os_result := oscmd_fn(l_cmd, l_appl_server);
      logs.info(l_os_result);
      logs.warn('FTP Failed!' || cnst.newline_char || l_os_result);


   EXCEPTION

      WHEN L_E_NO_LABELDATA_FILE
      THEN
         LOGS.err(l_tt_parms);

      WHEN OTHERS
      THEN
         LOGS.err(l_tt_parms);

   END ftp_to_ebs_demantra;
/

