CREATE OR REPLACE TRIGGER 
"OP"."MQ_PUT_ID_TRG" BEFORE UPDATE OR INSERT ON "OP"."MCLANE_MQ_PUT" 
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW 
BEGIN

  IF inserting THEN

     SELECT mq_put_id_seq.nextval
       INTO :new.mq_put_id
       FROM dual;

  ELSIF updating THEN

        IF :old.mq_put_id != :new.mq_put_id THEN
           :new.mq_put_id := :old.mq_put_id;
        END IF;

  END IF;

END;
/

