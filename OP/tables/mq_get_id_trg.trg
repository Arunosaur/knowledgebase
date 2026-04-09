CREATE OR REPLACE TRIGGER 
"OP"."MQ_GET_ID_TRG" BEFORE UPDATE OR INSERT ON "OP"."MCLANE_MQ_GET" 
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW 
BEGIN

  IF inserting THEN

     SELECT mq_get_id_seq.nextval
       INTO :new.mq_get_id
       FROM dual;

  ELSIF updating THEN

        IF :old.mq_get_id != :new.mq_get_id THEN
           :new.mq_get_id := :old.mq_get_id;
        END IF;

  END IF;

END;
/

