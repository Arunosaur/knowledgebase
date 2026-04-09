CREATE OR REPLACE TRIGGER 
"OP"."RLSE_LOG_OP2Z_TRBI" BEFORE INSERT ON "OP"."RLSE_LOG_OP2Z" 
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW 
begin
select seq_of_events_seq.nextval
  into :new.seq_of_events
  from dual;
end;
/

