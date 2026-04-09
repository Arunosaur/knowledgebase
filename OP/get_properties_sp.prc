CREATE OR REPLACE PROCEDURE get_properties_sp(
  i_file_dir  IN      VARCHAR2,
  i_file_nm   IN      VARCHAR2,
  o_t_props   OUT     op_types_pk.tt_varchars_v
) IS
  l_c_module   CONSTANT VARCHAR2(30)       := 'GET_PROPERTIES_SP';
  lar_parm              logs.tar_parm;
  l_file_handle         UTL_FILE.file_type;
  l_c_read     CONSTANT VARCHAR2(1)        := 'r';
  l_buffer              VARCHAR2(4000);
  l_pos                 PLS_INTEGER;
  l_c_comment  CONSTANT VARCHAR2(1)        := '#';
  l_idx                 VARCHAR2(200);
  l_val                 VARCHAR2(200);
BEGIN
  timer.startme(l_c_module || env.get_session_id);
  logs.add_parm(lar_parm, 'FileDir', i_file_dir);
  logs.add_parm(lar_parm, 'FileNm', i_file_nm);
  logs.dbg('ENTRY', lar_parm);
  logs.dbg('Open file');
  l_file_handle := UTL_FILE.fopen(i_file_dir, i_file_nm, l_c_read);
  <<read_loop>>
  LOOP
    BEGIN
      logs.dbg('Get Line');
      UTL_FILE.get_line(l_file_handle, l_buffer);
      l_pos := INSTR(l_buffer, '=');

      IF     l_pos > 1
         AND SUBSTR(l_buffer, 1, 1) <> l_c_comment THEN
        l_val := TRIM(SUBSTR(l_buffer, l_pos + 1));
        l_idx := TRIM(SUBSTR(l_buffer, 1, l_pos - 1));
        -- handle exports
        -- i.e.: export DIVISION=WJ
        l_pos := INSTR(l_idx, ' ', -1);

        IF l_pos > 0 THEN
          l_idx := SUBSTR(l_idx, l_pos + 1);
        END IF;   -- l_pos > 0

        logs.dbg('Assign value to output table');
        o_t_props(l_idx) := l_val;
      END IF;   -- l_pos > 1
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        EXIT read_loop;
    END;
  END LOOP read_loop;
  logs.dbg('Close file');
  UTL_FILE.fclose(l_file_handle);
  timer.stopme(l_c_module || env.get_session_id);
  logs.info('RUNTIME: ' || timer.elapsed(l_c_module || env.get_session_id) || ' secs.');
EXCEPTION
  WHEN OTHERS THEN
    IF UTL_FILE.is_open(l_file_handle) THEN
      UTL_FILE.fclose(l_file_handle);
    END IF;

    logs.err(lar_parm);
END get_properties_sp;
/

