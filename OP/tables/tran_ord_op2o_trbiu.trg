CREATE OR REPLACE TRIGGER 
"OP"."TRAN_ORD_OP2O_TRBIU" BEFORE UPDATE OR INSERT OF "PART_ID" ON "OP"."TRAN_ORD_OP2O" 
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW 
BEGIN
   IF updating THEN
      RAISE_APPLICATION_ERROR(-20001, 'Updating column PART_ID is not allowed');
   ELSE
      IF :new.part_id IS NULL THEN
         :new.part_id   := TO_NUMBER(TO_CHAR(sysdate, 'DD')) - 1;
      END IF;
   END IF;
END;
/

