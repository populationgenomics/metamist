-- migrate:up

SET search_path TO main;

alter table analysis add column sys_period tstzrange NOT NULL;
alter table analysis_cohort add column sys_period tstzrange NOT NULL;
alter table analysis_outputs add column sys_period tstzrange NOT NULL;
alter table analysis_runner add column sys_period tstzrange NOT NULL;
alter table analysis_sequencing_group add column sys_period tstzrange NOT NULL;
alter table assay add column sys_period tstzrange NOT NULL;
alter table assay_comment add column sys_period tstzrange NOT NULL;
alter table assay_external_id add column sys_period tstzrange NOT NULL;
alter table cohort add column sys_period tstzrange NOT NULL;
alter table cohort_sequencing_group add column sys_period tstzrange NOT NULL;
alter table cohort_template add column sys_period tstzrange NOT NULL;
alter table comment add column sys_period tstzrange NOT NULL;
alter table family add column sys_period tstzrange NOT NULL;
alter table family_comment add column sys_period tstzrange NOT NULL;
alter table family_external_id add column sys_period tstzrange NOT NULL;
alter table family_participant add column sys_period tstzrange NOT NULL;
alter table "group" add column sys_period tstzrange NOT NULL;
alter table group_member add column sys_period tstzrange NOT NULL;
alter table output_file add column sys_period tstzrange NOT NULL;
alter table participant add column sys_period tstzrange NOT NULL;
alter table participant_comment add column sys_period tstzrange NOT NULL;
alter table participant_external_id add column sys_period tstzrange NOT NULL;
alter table participant_phenotypes add column sys_period tstzrange NOT NULL;
alter table project add column sys_period tstzrange NOT NULL;
alter table project_comment add column sys_period tstzrange NOT NULL;
alter table project_member add column sys_period tstzrange NOT NULL;
alter table sample add column sys_period tstzrange NOT NULL;
alter table sample_comment add column sys_period tstzrange NOT NULL;
alter table sample_external_id add column sys_period tstzrange NOT NULL;
alter table sequencing_group add column sys_period tstzrange NOT NULL;
alter table sequencing_group_assay add column sys_period tstzrange NOT NULL;
alter table sequencing_group_comment add column sys_period tstzrange NOT NULL;
alter table sequencing_group_external_id add column sys_period tstzrange NOT NULL;

-- migrate:down

SET search_path TO main;

alter table analysis drop column sys_period;
alter table analysis_cohort drop column sys_period;
alter table analysis_outputs drop column sys_period;
alter table analysis_runner drop column sys_period;
alter table analysis_sequencing_group drop column sys_period;
alter table assay drop column sys_period;
alter table assay_comment drop column sys_period;
alter table assay_external_id drop column sys_period;
alter table cohort drop column sys_period;
alter table cohort_sequencing_group drop column sys_period;
alter table cohort_template drop column sys_period;
alter table comment drop column sys_period;
alter table family drop column sys_period;
alter table family_comment drop column sys_period;
alter table family_external_id drop column sys_period;
alter table family_participant drop column sys_period;
alter table "group" drop column sys_period;
alter table group_member drop column sys_period;
alter table output_file drop column sys_period;
alter table participant drop column sys_period;
alter table participant_comment drop column sys_period;
alter table participant_external_id drop column sys_period;
alter table participant_phenotypes drop column sys_period;
alter table project drop column sys_period;
alter table project_comment drop column sys_period;
alter table project_member drop column sys_period;
alter table sample drop column sys_period;
alter table sample_comment drop column sys_period;
alter table sample_external_id drop column sys_period;
alter table sequencing_group drop column sys_period;
alter table sequencing_group_assay drop column sys_period;
alter table sequencing_group_comment drop column sys_period;
alter table sequencing_group_external_id drop column sys_period;
