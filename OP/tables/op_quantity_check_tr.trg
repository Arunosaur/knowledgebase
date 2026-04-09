CREATE OR REPLACE TRIGGER "OP_QUANTITY_CHECK_TR" BEFORE UPDATE OR INSERT OF "ITEMC", "QOHC", "QALC", "QAVC" ON "OP"."WHSP300C"
REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW
DECLARE
  v_error_msg   VARCHAR2(250) := NULL;

BEGIN
/*-----------------------------------------------------------------------------------------*
 *             C H A N G E     L O G                                                       *
 *-----------------------------------------------------------------------------------------*
 *  Date    | USERID  |           Changes                                                  *
 *-----------------------------------------------------------------------------------------*
 * 08/20/02 | SNAGABH | Initial Creation.                                                  *
 *-----------------------------------------------------------------------------------------*/

  /*----------------------------------------------------------------------------------
   * The purpose of this trigger is to prevent Quantity fields from being updated
   * with negative values. The information logged in the sql_utilities table can
   * be used to identify the process that is trying to update quantity with negative
   * values and make necessary changes to correct it.
   *----------------------------------------------------------------------------------*/
  -- Check for Negative Quantity On Hand
  IF (:NEW.QOHC < 0)
  THEN
    v_error_msg := ' Negative OnHand Quantity:' ||
                   ' OLD/NEW QOHC= [' || :OLD.QOHC ||
                   '/' || :NEW.QOHC || ']. ';
    :NEW.QOHC := 0; -- Reset
  END IF;

  -- Check for Negative Allocated Quantity
  IF (:NEW.QALC < 0)
  THEN
    v_error_msg := v_error_msg || ' Negative Allocated Quantity:' ||
                                  ' OLD/NEW QALC= [' || :OLD.QALC ||
                                  '/' || :NEW.QALC || ']. ';
    :NEW.QALC := 0; -- Reset
  END IF;

  -- Check for Negative Available Quatity
  IF (:NEW.QAVC <0)
  THEN
    v_error_msg := v_error_msg || ' Negative Available Quantity:' ||
                                  ' OLD/NEW QAVC= [' || :OLD.QAVC ||
                                  '/' || :NEW.QAVC || ']. ';
    :NEW.QAVC := 0;
  END IF;


  IF (v_error_msg IS NOT NULL)
  THEN
    env.set_app_cd('OPCIG');
    logs.warn('[Item Number=' || :NEW.ITEMC || '] ' || v_error_msg);
  END IF;
END;
/

