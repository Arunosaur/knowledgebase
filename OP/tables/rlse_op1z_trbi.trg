CREATE OR REPLACE TRIGGER 
"OP"."RLSE_OP1Z_TRBI" BEFORE INSERT ON "OP"."RLSE_OP1Z" 
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW 
begin
select rlse_id_seq.nextval
  into :new.rlse_id
  from dual;
end;
/

