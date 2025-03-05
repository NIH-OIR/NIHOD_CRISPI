CREATE OR REPLACE VIEW public.last_updated_time
AS SELECT DISTINCT "TimeStamp"
   FROM "CT_Data_Table"
 LIMIT 1;