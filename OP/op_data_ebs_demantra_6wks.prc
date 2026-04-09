CREATE OR REPLACE PROCEDURE OP_DATA_EBS_DEMANTRA_6WKS(i_begin_date   DATE DEFAULT NULL,
                                                              i_end_date     DATE DEFAULT NULL)

   /*
   ||---------------------------------------------------------------------------
   || OP_DATA_EBS_DEMANTRA_6WKS
   || EBS - outs from MOQ and cut process
   || OP data extract for EBS DEMANTRA  - Just one time manual run for 6 weeks data
   || When NULL is passed for i_begin_date, it gets the value from 'EBS_LAST_RUN_DT' (AP1S table)
   || When NULL is passed for i_end_date, it gets the value from SYSDATE
   || For manual run the date range can be passed to the SP.
   || File is copied to /ftptrans directory and then SFTPed to the EBS server location
   || It is then zipped and archived to /ftptrans/transmitted_files directory
   ||---------------------------------------------------------------------------
   ||             C H A N G E     L O G
   ||---------------------------------------------------------------------------
   || Date       | USERID  | Changes
   ||---------------------------------------------------------------------------
   || 07/15/2021 | jxpazho    | Original
   || 08/24/2021 | jxpazho    | Include cancelled order lines along with order cuts.
   ||---------------------------------------------------------------------------
   */
   IS
      l_tt_parms   LOGS.TAR_PARM;

      CURSOR CSR1(p_begin_date          DATE,
                  p_end_date            DATE,
                  p_div_list            VARCHAR2,
                  p_offset_begin_dt     INTEGER,
                  p_offset_end_dt       INTEGER
                 )
      IS
        WITH
          divlists AS
            (SELECT LEVEL AS lvl, REGEXP_SUBSTR(p_div_list,'[^,]+', 1, LEVEL) AS div_nm
             FROM   dual
             CONNECT BY REGEXP_SUBSTR(p_div_list, '[^,]+', 1, LEVEL) IS NOT NULL
            ),
          aud AS
            (
            SELECT d.div_id, d.div_part, a.ordnoa AS ord_num, a.linea AS ord_ln, SUM(b.ordqtb) AS chg_qty
              FROM sysp296a a, div_mstr_di1d d, ordp120b b
              WHERE d.div_id IN ( SELECT  div_nm FROM divlists)  ---('MI','PA','MK','MG','SZ','NW')
                 AND a.rsncda = 'RCANC7'
                 AND a.fldnma = 'STATB'
                 AND a.flchga = 'C'
                 AND a.usera = 'atholt'
                 AND dt_tm_fn(DATE '1900-02-28' + a.datea, a.timea, 'HH24MISS') >= p_begin_date - p_offset_begin_dt     -- p_begin_date -5 days
                 AND dt_tm_fn(DATE '1900-02-28' + a.datea, a.timea, 'HH24MISS') <= p_end_date + p_offset_end_dt         -- p_end_date + 1 day
                 AND d.div_part = a.div_part
                 AND b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.lineb = a.linea
              GROUP BY d.div_id, d.div_part, a.ordnoa, a.linea
           UNION ALL
            SELECT d.div_id, d.div_part, a.ordnoa AS ord_num, a.linea AS ord_ln, SUM(b.ordqtb) AS chg_qty
              FROM sysp996a a, div_mstr_di1d d, ordp120b b
              WHERE d.div_id IN ( SELECT  div_nm FROM divlists)  ---('MI','PA','MK','MG','SZ','NW')
                 AND a.rsncda = 'RCANC7'
                 AND a.fldnma = 'STATB'
                 AND a.flchga = 'C'
                 AND a.usera = 'atholt'
                 AND dt_tm_fn(DATE '1900-02-28' + a.datea, a.timea, 'HH24MISS') >= p_begin_date - p_offset_begin_dt     -- p_begin_date -5 days
                 AND dt_tm_fn(DATE '1900-02-28' + a.datea, a.timea, 'HH24MISS') <= p_end_date + p_offset_end_dt         -- p_end_date + 1 day
                 AND d.div_part = a.div_part
                 AND b.div_part = a.div_part
                 AND b.ordnob = a.ordnoa
                 AND b.lineb = a.linea
              GROUP BY d.div_id, d.div_part, a.ordnoa, a.linea
           UNION ALL
            SELECT d.div_id, d.div_part, aa.ordnoa AS ord_num, aa.linea AS ord_ln, (SUM(aa.florga - aa.flchga)) AS chg_qty
              FROM div_mstr_di1d d, sysp296a aa
              WHERE d.div_id IN ( SELECT  div_nm FROM divlists)   ---('MI','PA','MK','MG','SZ','NW')
                 AND aa.div_part = d.div_part
                 AND aa.fldnma = 'ORDQTB'
                 AND aa.florga > aa.flchga
                 AND dt_tm_fn(DATE '1900-02-28' + aa.datea, aa.timea, 'HH24MISS') >= p_begin_date - p_offset_begin_dt     -- p_begin_date -5 days
                 AND dt_tm_fn(DATE '1900-02-28' + aa.datea, aa.timea, 'HH24MISS') <= p_end_date + p_offset_end_dt         -- p_end_date + 1 day
                 AND (aa.florga - aa.flchga) < 900
                 AND regexp_like(aa.flchga, '^\d+(\.\d+)?$')
                 AND regexp_like(aa.florga, '^\d+(\.\d+)?$')
              GROUP BY d.div_id, d.div_part, aa.ordnoa, aa.linea
           UNION ALL
             SELECT d.div_id, d.div_part, aa.ordnoa AS ord_num, aa.linea AS ord_ln, (SUM(aa.florga - aa.flchga )) AS chg_qty
                FROM div_mstr_di1d d, sysp996a aa
                WHERE d.div_id IN ( SELECT  div_nm FROM divlists)   ---('MI','PA','MK','MG','SZ','NW')
                  AND aa.div_part = d.div_part
                  AND aa.fldnma = 'ORDQTB'
                  AND aa.florga > aa.flchga
                  AND dt_tm_fn(DATE '1900-02-28' + aa.datea, aa.timea, 'HH24MISS') >= p_begin_date - p_offset_begin_dt  -- p_begin_date -5 days
                  AND dt_tm_fn(DATE '1900-02-28' + aa.datea, aa.timea, 'HH24MISS') <= p_end_date + p_offset_end_dt      -- p_end_date + 1 day
                  AND (aa.florga - aa.flchga) < 900
                  AND regexp_like(aa.flchga, '^\d+(\.\d+)?$')
                  AND regexp_like(aa.florga, '^\d+(\.\d+)?$')
                GROUP BY d.div_id, d.div_part, aa.ordnoa, aa.linea
           UNION ALL
             SELECT d.div_id, d.div_part, p.ordnod AS ord_num, p.ordlnd AS ord_ln, (SUM(p.qtyfrd - p.qtytod)) AS chg_qty
                FROM div_mstr_di1d d, mclp300d p
                WHERE d.div_id IN ( SELECT  div_nm FROM divlists)   ---('MI','PA','MK','MG','SZ','NW')
                   AND p.div_part = d.div_part
                   AND p.reasnd IN('002','WKMAXQTY')
                   AND p.qtyfrd > p.qtytod
                   AND p.last_chg_ts >= p_begin_date - p_offset_begin_dt   -- p_begin_date -5 days
                   AND p.last_chg_ts <= p_end_date + p_offset_end_dt       -- p_end_date + 1 day
                   AND (p.qtyfrd - p.qtytod) < 900
                   AND regexp_like(p.qtyfrd, '^\d+(\.\d+)?$')
                   AND regexp_like(p.qtytod, '^\d+(\.\d+)?$')
                GROUP BY d.div_id, d.div_part, p.ordnod, p.ordlnd
           UNION ALL
             SELECT d.div_id, d.div_part, h.ordnod AS ord_num, h.ordlnd AS ord_ln, (SUM(h.qtyfrd - h.qtytod)) AS chg_qty
                FROM div_mstr_di1d d, mclp900d h
              WHERE d.div_id IN ( SELECT  div_nm FROM divlists)   ---('MI','PA','MK','MG','SZ','NW')
                 AND h.div_part = d.div_part
                 AND h.reasnd IN('002','WKMAXQTY')
                 AND h.qtyfrd > h.qtytod
                 AND h.last_chg_ts >= p_begin_date - p_offset_begin_dt   -- p_begin_date -5 days
                 AND h.last_chg_ts <= p_end_date + p_offset_end_dt       -- p_end_date + 1 day
                 AND (h.qtyfrd - h.qtytod) < 900
                 AND regexp_like(h.qtyfrd, '^\d+(\.\d+)?$')
                 AND regexp_like(h.qtytod, '^\d+(\.\d+)?$')
              GROUP BY d.div_id, d.div_part, h.ordnod, h.ordlnd
            )
              SELECT a.load_date || '|' || a.div_id || '|' || a.orditb || '|' || LPAD(a.corpb, 3, '0')
                     || '|' || a.llr_dt || '|' || a.chg_qty cs
                FROM
                (
                  SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH:MI:SS') AS load_date,
                         aud.div_id,
                         b.orditb,
                         cx.corpb,
                         TO_CHAR(ld.llr_dt, 'YYYY-MM-DD') AS llr_dt,
                         SUM(aud.chg_qty) AS chg_qty
                    FROM aud, ordp100a a, load_depart_op1f ld, mclp020b cx, ordp120b b
                    WHERE a.div_part = aud.div_part
                       AND a.ordnoa = aud.ord_num
                       AND a.stata IN ('A','C')                    --
                       AND ld.div_part = a.div_part
                       AND ld.load_depart_sid = a.load_depart_sid
                       AND cx.div_part = a.div_part
                       AND cx.custb = a.custa
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.lineb = aud.ord_ln             --
                       AND ld.llr_dt >= p_begin_date        --
                       AND ld.llr_dt <= p_end_date          --
                   GROUP BY aud.div_id, b.orditb, cx.corpb, ld.llr_dt
                  UNION ALL
                    SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH:MI:SS') AS load_date,
                           aud.div_id,
                           b.orditb,
                           cx.corpb,
                           TO_CHAR(DATE '1900-02-28' + a.ctofda, 'YYYY-MM-DD') AS llr_dt,
                           SUM(aud.chg_qty) AS chg_qty
                      FROM aud, ordp900a a, mclp020b cx, ordp920b b
                    WHERE a.div_part = aud.div_part
                       AND a.ordnoa = aud.ord_num
                       AND a.stata IN ('A','C')                                 --
                       AND cx.div_part = a.div_part
                       AND cx.custb = a.custa
                       AND b.div_part = a.div_part
                       AND b.ordnob = a.ordnoa
                       AND b.lineb = aud.ord_ln                           --
                       AND DATE '1900-02-28' + a.ctofda >= p_begin_date   --
                       AND DATE '1900-02-28' + a.ctofda <= p_end_date     --
                    GROUP BY aud.div_id, b.orditb, cx.corpb, a.ctofda
                    ORDER BY 2,3,4,5
                ) a;

      TYPE OP_CUTS_TAB IS
        TABLE OF CSR1%ROWTYPE
        INDEX BY BINARY_INTEGER;

      l_tt_cuts           OP_CUTS_TAB;
      l_row               PLS_INTEGER;
      l_begin_date        DATE;
      l_end_date          DATE;
      l_offset_begin_dt   INTEGER;
      l_offset_end_dt     INTEGER;

      l_file_nm       VARCHAR2(500) ;
      l_div_list1      VARCHAR2(200);
      l_div_list2      VARCHAR2(200);
      l_div_list      VARCHAR2(500);
      l_file_handle   UTL_FILE.FILE_TYPE;

   BEGIN

       LOGS.info('Entry', l_tt_parms);

      -- Get last run date
      SELECT TO_DATE(TO_CHAR(a.dt_val, 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS')
        INTO l_begin_date
        FROM appl_sys_parm_ap1s a
        WHERE a.appl_id = 'OP'
        AND   a.parm_id =  'EBS_LAST_RUN_DT';

      -- Run until date
      l_end_date := SYSDATE;

      --
      l_begin_date := NVL(i_begin_date, l_begin_date);
      l_end_date   := NVL(i_end_date, l_end_date);

      LOGS.info('l_begin_date: ', TO_CHAR(l_begin_date, 'mm-dd-yyyy hh:mi:ss'));
      LOGS.info('l_end_date: ', TO_CHAR(l_end_date, 'mm-dd-yyyy hh:mi:ss'));

      -- Get offset begin date
      SELECT a.intgr_val
        INTO l_offset_begin_dt
        FROM appl_sys_parm_ap1s a
        WHERE a.appl_id = 'OP'
        AND   a.parm_id =  'EBS_OFFSET_BEGIN_DT';

      -- Get offset end date
      SELECT a.intgr_val
        INTO l_offset_end_dt
        FROM appl_sys_parm_ap1s a
        WHERE a.appl_id = 'OP'
        AND   a.parm_id =  'EBS_OFFSET_END_DT';

      LOGS.info('l_offset_begin_dt: ', l_offset_begin_dt);
      LOGS.info('l_offset_end_dt: ', l_offset_end_dt);

      -- Get file name
      SELECT SUBSTR(a.vchar_val ,1 ,(INSTR(vchar_val,'.') -1)) || TO_CHAR(l_end_date, 'mm-dd-yyyy') ||
              SUBSTR(a.vchar_val ,(INSTR(a.vchar_val,'.')))
        INTO l_file_nm
        FROM appl_sys_parm_ap1s a
        WHERE a.appl_id = 'OP'
        AND   a.parm_id =  'EBS_FILE_NAME';

      LOGS.info('l_file_nm: ', l_file_nm);

      -- Get Division lists
       SELECT a.vchar_val
         INTO l_div_list1
         FROM appl_sys_parm_ap1s a
         WHERE a.appl_id = 'OP'
         AND   a.parm_id =  'EBS_DIV_LIST1';

       SELECT a.vchar_val
         INTO l_div_list2
         FROM appl_sys_parm_ap1s a
         WHERE a.appl_id = 'OP'
         AND   a.parm_id =  'EBS_DIV_LIST2';

      l_div_list := TRIM(l_div_list1);

      IF TRIM(l_div_list2) IS NOT NULL THEN
        l_div_list := l_div_list || ',' || TRIM(l_div_list2);
      END IF;

      LOGS.info('l_div_list: ', l_div_list);

      --
      l_file_handle := UTL_FILE.fOpen(CIG_CONSTANT_LITERALS_PK.DIRECTORY, l_file_nm, 'A');

      OPEN csr1(l_begin_date, l_end_date, l_div_list, l_offset_begin_dt, l_offset_end_dt);
      LOOP
         FETCH CSR1 BULK COLLECT INTO l_tt_cuts LIMIT 1000;
         EXIT WHEN l_tt_cuts.count = 0;

         l_row := l_tt_cuts.FIRST;
         WHILE (l_row IS NOT NULL)
         LOOP
            UTL_FILE.putf(l_file_handle, l_tt_cuts(l_row).cs || '\n');
            l_row := l_tt_cuts.NEXT(l_row);
         END LOOP;
      END LOOP;
      CLOSE CSR1;

     IF UTL_FILE.is_open(l_file_handle)
     THEN
        UTL_FILE.fClose(l_file_handle);
     END IF;

     LOGS.info('File: ', l_file_nm);

     -- FTP
     OP_CODE.ftp_to_ebs_demantra(i_local_file => l_file_nm,
                                 i_remote_file => l_file_nm,
                                 i_archv_sw => 'Y',
                                 i_gdg_sw => 'N');

     LOGS.info('FTP : ', 'FTP Done');

     -- last run date
     UPDATE appl_sys_parm_ap1s p
       SET p.dt_val = l_end_date,
           p.user_id = 'OPEBS',
           p.last_chg_ts = SYSDATE
       WHERE p.appl_id = 'OP'
       AND   p.parm_id = 'EBS_LAST_RUN_DT'
       AND   p.div_part = 0;

     LOGS.info('EBS_LAST_RUN_DT : ', l_end_date);


   EXCEPTION
      WHEN OTHERS
      THEN
         LOGS.err(l_tt_parms);


   END OP_DATA_EBS_DEMANTRA_6WKS;
/

