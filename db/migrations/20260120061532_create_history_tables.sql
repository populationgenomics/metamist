-- migrate:up

CREATE SCHEMA IF NOT EXISTS history;

SET search_path TO history, main;

create table analysis_history (like main.analysis);
create table analysis_cohort_history (like main.analysis_cohort);
create table analysis_outputs_history (like main.analysis_outputs);
create table analysis_runner_history (like main.analysis_runner);
create table analysis_sequencing_group_history (like main.analysis_sequencing_group);
create table assay_history (like main.assay);
create table assay_comment_history (like main.assay_comment);
create table assay_external_id_history (like main.assay_external_id);
create table cohort_history (like main.cohort);
create table cohort_sequencing_group_history (like main.cohort_sequencing_group);
create table cohort_template_history (like main.cohort_template);
create table comment_history (like main.comment);
create table family_history (like main.family);
create table family_comment_history (like main.family_comment);
create table family_external_id_history (like main.family_external_id);
create table family_participant_history (like main.family_participant);
create table group_history (like main."group");
create table group_member_history (like main.group_member);
create table output_file_history (like main.output_file);
create table participant_history (like main.participant);
create table participant_comment_history (like main.participant_comment);
create table participant_external_id_history (like main.participant_external_id);
create table participant_phenotypes_history (like main.participant_phenotypes);
create table project_history (like main.project);
create table project_comment_history (like main.project_comment);
create table project_member_history (like main.project_member);
create table sample_history (like main.sample);
create table sample_comment_history (like main.sample_comment);
create table sample_external_id_history (like main.sample_external_id);
create table sequencing_group_history (like main.sequencing_group);
create table sequencing_group_assay_history (like main.sequencing_group_assay);
create table sequencing_group_comment_history (like main.sequencing_group_comment);
create table sequencing_group_external_id_history (like main.sequencing_group_external_id);

-- migrate:down

SET search_path TO history;

drop table if exists sequencing_group_external_id_history;
drop table if exists sequencing_group_comment_history;
drop table if exists sequencing_group_assay_history;
drop table if exists sequencing_group_history;
drop table if exists sample_external_id_history;
drop table if exists sample_comment_history;
drop table if exists sample_history;
drop table if exists project_member_history;
drop table if exists project_comment_history;
drop table if exists project_history;
drop table if exists participant_phenotypes_history;
drop table if exists participant_external_id_history;
drop table if exists participant_comment_history;
drop table if exists participant_history;
drop table if exists output_file_history;
drop table if exists group_member_history;
drop table if exists group_history;
drop table if exists family_participant_history;
drop table if exists family_external_id_history;
drop table if exists family_comment_history;
drop table if exists family_history;
drop table if exists comment_history;
drop table if exists cohort_template_history;
drop table if exists cohort_sequencing_group_history;
drop table if exists cohort_history;
drop table if exists assay_external_id_history;
drop table if exists assay_comment_history;
drop table if exists assay_history;
drop table if exists analysis_sequencing_group_history;
drop table if exists analysis_runner_history;
drop table if exists analysis_outputs_history;
drop table if exists analysis_cohort_history;
drop table if exists analysis_history;

DROP SCHEMA IF EXISTS history;
