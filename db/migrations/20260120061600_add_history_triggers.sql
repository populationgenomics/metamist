-- migrate:up

CREATE EXTENSION IF NOT EXISTS temporal_tables;
SET search_path TO history, main;

CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.analysis FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.analysis_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.analysis_cohort FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.analysis_cohort_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.analysis_outputs FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.analysis_outputs_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.analysis_runner FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.analysis_runner_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.analysis_sequencing_group FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.analysis_sequencing_group_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.assay FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.assay_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.assay_comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.assay_comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.assay_external_id FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.assay_external_id_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.cohort FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.cohort_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.cohort_sequencing_group FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.cohort_sequencing_group_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.cohort_template FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.cohort_template_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.family FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.family_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.family_comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.family_comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.family_external_id FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.family_external_id_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.family_participant FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.family_participant_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main."group" FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.group_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.group_member FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.group_member_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.output_file FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.output_file_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.participant FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.participant_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.participant_comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.participant_comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.participant_external_id FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.participant_external_id_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.participant_phenotypes FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.participant_phenotypes_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.project FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.project_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.project_comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.project_comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.project_member FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.project_member_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sample FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sample_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sample_comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sample_comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sample_external_id FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sample_external_id_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sequencing_group FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sequencing_group_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sequencing_group_assay FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sequencing_group_assay_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sequencing_group_comment FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sequencing_group_comment_history', true);
CREATE TRIGGER versioning_trigger BEFORE INSERT OR UPDATE OR DELETE ON main.sequencing_group_external_id FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'history.sequencing_group_external_id_history', true);

-- migrate:down

SET search_path TO main;

DROP TRIGGER IF EXISTS versioning_trigger ON analysis;
DROP TRIGGER IF EXISTS versioning_trigger ON analysis_cohort;
DROP TRIGGER IF EXISTS versioning_trigger ON analysis_outputs;
DROP TRIGGER IF EXISTS versioning_trigger ON analysis_runner;
DROP TRIGGER IF EXISTS versioning_trigger ON analysis_sequencing_group;
DROP TRIGGER IF EXISTS versioning_trigger ON assay;
DROP TRIGGER IF EXISTS versioning_trigger ON assay_comment;
DROP TRIGGER IF EXISTS versioning_trigger ON assay_external_id;
DROP TRIGGER IF EXISTS versioning_trigger ON cohort;
DROP TRIGGER IF EXISTS versioning_trigger ON cohort_sequencing_group;
DROP TRIGGER IF EXISTS versioning_trigger ON cohort_template;
DROP TRIGGER IF EXISTS versioning_trigger ON comment;
DROP TRIGGER IF EXISTS versioning_trigger ON family;
DROP TRIGGER IF EXISTS versioning_trigger ON family_comment;
DROP TRIGGER IF EXISTS versioning_trigger ON family_external_id;
DROP TRIGGER IF EXISTS versioning_trigger ON family_participant;
DROP TRIGGER IF EXISTS versioning_trigger ON "group";
DROP TRIGGER IF EXISTS versioning_trigger ON group_member;
DROP TRIGGER IF EXISTS versioning_trigger ON output_file;
DROP TRIGGER IF EXISTS versioning_trigger ON participant;
DROP TRIGGER IF EXISTS versioning_trigger ON participant_comment;
DROP TRIGGER IF EXISTS versioning_trigger ON participant_external_id;
DROP TRIGGER IF EXISTS versioning_trigger ON participant_phenotypes;
DROP TRIGGER IF EXISTS versioning_trigger ON project;
DROP TRIGGER IF EXISTS versioning_trigger ON project_comment;
DROP TRIGGER IF EXISTS versioning_trigger ON project_member;
DROP TRIGGER IF EXISTS versioning_trigger ON sample;
DROP TRIGGER IF EXISTS versioning_trigger ON sample_comment;
DROP TRIGGER IF EXISTS versioning_trigger ON sample_external_id;
DROP TRIGGER IF EXISTS versioning_trigger ON sequencing_group;
DROP TRIGGER IF EXISTS versioning_trigger ON sequencing_group_assay;
DROP TRIGGER IF EXISTS versioning_trigger ON sequencing_group_comment;
DROP TRIGGER IF EXISTS versioning_trigger ON sequencing_group_external_id;

DROP EXTENSION IF EXISTS temporal_tables;
