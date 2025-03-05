CREATE OR REPLACE VIEW public.count1
AS SELECT protocol_count.ic,
    protocol_count.protocol_count,
    require_results_protocol_count.require_results_protocol_count,
    completed_protocol_count.completed_protocol_count,
    within_one_year_protocol_count.within_one_year_count,
    results_submit_in_time_count1.results_submit_in_time_count1,
    results_overdue_count.results_overdue_count,
    results_posted_count.results_posted_count,
    results_submit_in_time_count.results_submit_in_time_count
   FROM ( SELECT "Protrak_Data_Table".accrual_inst AS ic,
            count("Protrak_Data_Table".protocol_number) AS protocol_count
           FROM "Protrak_Data_Table"
          WHERE "Protrak_Data_Table".study_type = 'Interventional'::text
          GROUP BY "Protrak_Data_Table".accrual_inst
          ORDER BY "Protrak_Data_Table".accrual_inst) protocol_count
     LEFT JOIN ( SELECT results_temp.ic,
            count(results_temp.require_results) AS require_results_protocol_count
           FROM ( SELECT "Protrak_Data_Table".accrual_inst AS ic,
                        CASE
                            WHEN "Protrak_Data_Table".protrak_accrual_status = 'Withdrawn'::text THEN false
                            ELSE true
                        END AS require_results
                   FROM "Protrak_Data_Table"
                  WHERE "Protrak_Data_Table".study_type = 'Interventional'::text) results_temp
          WHERE results_temp.require_results = true
          GROUP BY results_temp.ic
          ORDER BY results_temp.ic) require_results_protocol_count ON protocol_count.ic = require_results_protocol_count.ic
     LEFT JOIN ( SELECT temp.ic,
            count(temp.meet_completion_date) AS completed_protocol_count
           FROM ( SELECT "Protrak_Data_Table".accrual_inst AS ic,
                        CASE
                            WHEN now() > to_timestamp("Protrak_Data_Table".primary_completion_date, 'MM/DD/YYYY'::text) THEN true
                            ELSE false
                        END AS meet_completion_date
                   FROM "Protrak_Data_Table"
                  WHERE "Protrak_Data_Table".study_type = 'Interventional'::text) temp
          WHERE temp.meet_completion_date = true
          GROUP BY temp.ic
          ORDER BY temp.ic) completed_protocol_count ON protocol_count.ic = completed_protocol_count.ic
     LEFT JOIN ( SELECT one_year_temp.ic,
            count(one_year_temp.within_one_year) AS within_one_year_count
           FROM ( SELECT "Protrak_Data_Table".accrual_inst AS ic,
                        CASE
                            WHEN now() > to_timestamp("Protrak_Data_Table".primary_completion_date, 'MM/DD/YYYY'::text) AND now() < (to_timestamp("Protrak_Data_Table".primary_completion_date, 'MM/DD/YYYY'::text) + '1 year'::interval) AND (to_timestamp(ct."firstSubmitDate", 'YYYY-MM-DD'::text) > (to_timestamp(ct."primaryCompletionDate", 'YYYY-MM-DD'::text) + '1 year'::interval) OR to_timestamp(ct."lastUpdateSubmitDate", 'YYYY-MM-DD'::text) > (to_timestamp(ct."primaryCompletionDate", 'YYYY-MM-DD'::text) + '1 year'::interval)) THEN true
                            ELSE false
                        END AS within_one_year
                   FROM "Protrak_Data_Table"
                     JOIN "CT_Data_Table" ct ON "Protrak_Data_Table".nct_number = ct."nctId"
                  WHERE "Protrak_Data_Table".study_type = 'Interventional'::text) one_year_temp
          WHERE one_year_temp.within_one_year = true
          GROUP BY one_year_temp.ic
          ORDER BY one_year_temp.ic) within_one_year_protocol_count ON protocol_count.ic = within_one_year_protocol_count.ic
     LEFT JOIN ( SELECT results_submit_in_time_temp1.ic,
            count(results_submit_in_time_temp1.results_submitted_in_time1) AS results_submit_in_time_count1
           FROM ( SELECT pdt.accrual_inst AS ic,
                        CASE
                            WHEN now() > to_timestamp(pdt.primary_completion_date, 'MM/DD/YYYY'::text) AND to_timestamp(ct."firstSubmitDate", 'YYYY-MM-DD'::text) > (to_timestamp(ct."primaryCompletionDate", 'YYYY-MM-DD'::text) + '1 year'::interval) THEN true
                            ELSE false
                        END AS results_submitted_in_time1
                   FROM "Protrak_Data_Table" pdt
                     JOIN "CT_Data_Table" ct ON pdt.nct_number = ct."nctId"
                  WHERE pdt.study_type = 'Interventional'::text) results_submit_in_time_temp1
          WHERE results_submit_in_time_temp1.results_submitted_in_time1 = true
          GROUP BY results_submit_in_time_temp1.ic
          ORDER BY results_submit_in_time_temp1.ic) results_submit_in_time_count1 ON protocol_count.ic = results_submit_in_time_count1.ic
     LEFT JOIN ( SELECT results_overdue_temp.ic,
            count(results_overdue_temp.results_overdue) AS results_overdue_count
           FROM ( SELECT pdt.accrual_inst AS ic,
                        CASE
                            WHEN now() > to_timestamp(pdt.primary_completion_date, 'MM/DD/YYYY'::text) AND ct."lastUpdateSubmitDate" = ''::text AND ct."firstSubmitDate" = ''::text THEN true
                            ELSE false
                        END AS results_overdue
                   FROM "Protrak_Data_Table" pdt
                     JOIN "CT_Data_Table" ct ON pdt.nct_number = ct."nctId"
                  WHERE pdt.study_type = 'Interventional'::text) results_overdue_temp
          WHERE results_overdue_temp.results_overdue = true
          GROUP BY results_overdue_temp.ic
          ORDER BY results_overdue_temp.ic) results_overdue_count ON protocol_count.ic = results_overdue_count.ic
     LEFT JOIN ( SELECT results_posted_temp.ic,
            count(results_posted_temp.results_posted) AS results_posted_count
           FROM ( SELECT pdt.accrual_inst AS ic,
                        CASE
                            WHEN now() > to_timestamp(pdt.primary_completion_date, 'MM/DD/YYYY'::text) AND (ct."lastUpdateSubmitDate" <> ''::text OR ct."firstSubmitDate" <> ''::text) THEN true
                            ELSE false
                        END AS results_posted
                   FROM "Protrak_Data_Table" pdt
                     JOIN "CT_Data_Table" ct ON pdt.nct_number = ct."nctId"
                  WHERE pdt.study_type = 'Interventional'::text) results_posted_temp
          WHERE results_posted_temp.results_posted = true
          GROUP BY results_posted_temp.ic
          ORDER BY results_posted_temp.ic) results_posted_count ON protocol_count.ic = results_posted_count.ic
     LEFT JOIN ( SELECT results_submit_in_time_temp.ic,
            count(results_submit_in_time_temp.results_submitted_in_time) AS results_submit_in_time_count
           FROM ( SELECT pdt.accrual_inst AS ic,
                        CASE
                            WHEN now() > to_timestamp(pdt.primary_completion_date, 'MM/DD/YYYY'::text) AND (to_timestamp(ct."lastUpdateSubmitDate", 'YYYY-MM-DD'::text) < to_timestamp(ct."primaryCompletionDate", 'YYYY-MM-DD'::text) OR to_timestamp(ct."firstSubmitDate", 'YYYY-MM-DD'::text) < to_timestamp(ct."primaryCompletionDate", 'YYYY-MM-DD'::text)) THEN true
                            ELSE false
                        END AS results_submitted_in_time
                   FROM "Protrak_Data_Table" pdt
                     JOIN "CT_Data_Table" ct ON pdt.nct_number = ct."nctId"
                  WHERE pdt.study_type = 'Interventional'::text) results_submit_in_time_temp
          WHERE results_submit_in_time_temp.results_submitted_in_time = true
          GROUP BY results_submit_in_time_temp.ic
          ORDER BY results_submit_in_time_temp.ic) results_submit_in_time_count ON protocol_count.ic = results_submit_in_time_count.ic;