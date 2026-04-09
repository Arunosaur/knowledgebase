CREATE OR REPLACE TRIGGER 
"OP"."RT2R_TRAIUD" AFTER UPDATE OR INSERT OR DELETE ON "OP"."REROUTE_RT1R" 
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW 
declare
   v_typ   VARCHAR(3);
 begin
   IF INSERTING THEN
      v_typ   := 'INS';
   ELSIF UPDATING THEN
        v_typ := 'UPD';
   ELSIF DELETING THEN
        v_typ := 'DEL';
   END IF;

   INSERT INTO reroute_log_rt2r (old_div_part,
                                 new_div_part,
                                 old_cust_id,
                                 new_cust_id,
                                 old_load_num,
                                 new_load_num,
                                 old_llr_dt,
                                 new_llr_dt,
                                 old_stop_num,
                                 new_stop_num,
                                 old_llr_day,
                                 new_llr_day,
                                 old_llr_time,
                                 new_llr_time,
                                 old_depart_day,
                                 new_depart_day,
                                 old_depart_time,
                                 new_depart_time,
                                 old_eta_day,
                                 new_eta_day,
                                 old_eta_time,
                                 new_eta_time,
                                 old_eff_ts,
                                 new_eff_ts,
                                 old_end_ts,
                                 new_end_ts,
                                 typ,
                                 create_ts,
                                 service_name,
                                 user_id,
                                 os_user_id)
                         VALUES (:OLD.div_part,
                                 :NEW.div_part,
                                 :OLD.cust_id,
                                 :NEW.cust_id,
                                 :OLD.load_num,
                                 :NEW.load_num,
                                 :OLD.llr_dt,
                                 :NEW.llr_dt,
                                 :OLD.stop_num,
                                 :NEW.stop_num,
                                 :OLD.llr_day,
                                 :NEW.llr_day,
                                 :OLD.llr_time,
                                 :NEW.llr_time,
                                 :OLD.depart_day,
                                 :NEW.depart_day,
                                 :OLD.depart_time,
                                 :NEW.depart_time,
                                 :OLD.eta_day,
                                 :NEW.eta_day,
                                 :OLD.eta_time,
                                 :NEW.eta_time,
                                 :OLD.eff_ts,
                                 :NEW.eff_ts,
                                 :OLD.end_ts,
                                 :NEW.end_ts,
                                 v_typ,
                                 systimestamp,
                                 SYS_CONTEXT('USERENV','SERVICE_NAME'),
                                 SYS_CONTEXT('USERENV','SESSION_USER'),
                                 SYS_CONTEXT('USERENV','OS_USER'));

END;
/

