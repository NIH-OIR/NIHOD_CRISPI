CREATE OR REPLACE VIEW public.last_updated_time
AS SELECT DISTINCT "TimeStamp"
   FROM "Protect_Data_Table"
 LIMIT 1;