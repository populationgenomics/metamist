-- migrate:up

SET search_path TO main;

CREATE INDEX idx_analysis_project ON analysis(project);
CREATE INDEX idx_analysis_type ON analysis(type);
CREATE INDEX idx_analysis_audit_log_id ON analysis(audit_log_id);

ALTER TABLE analysis ADD CONSTRAINT fk_analysis_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE analysis ADD CONSTRAINT fk_analysis_type FOREIGN KEY (type) REFERENCES analysis_type(id);
ALTER TABLE analysis ADD CONSTRAINT fk_analysis_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);



CREATE INDEX idx_analysis_cohort_cohort_id ON analysis_cohort(cohort_id);
CREATE INDEX idx_analysis_cohort_analysis_id ON analysis_cohort(analysis_id);
CREATE INDEX idx_analysis_cohort_audit_log_id ON analysis_cohort(audit_log_id);

ALTER TABLE analysis_cohort ADD CONSTRAINT fk_analysis_cohort_cohort_id FOREIGN KEY (cohort_id) REFERENCES cohort(id);
ALTER TABLE analysis_cohort ADD CONSTRAINT fk_analysis_cohort_analysis_id FOREIGN KEY (analysis_id) REFERENCES analysis(id);
ALTER TABLE analysis_cohort ADD CONSTRAINT fk_analysis_cohort_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE UNIQUE INDEX idx_analysis_outputs_analysis_file ON analysis_outputs(analysis_id, file_id, json_structure);
CREATE UNIQUE INDEX idx_analysis_outputs_analysis_output ON analysis_outputs(analysis_id, output, json_structure);
CREATE INDEX idx_analysis_outputs_file_id ON analysis_outputs(file_id);
CREATE INDEX idx_analysis_outputs_audit_log_id ON analysis_outputs(audit_log_id);


ALTER TABLE analysis_outputs ADD CONSTRAINT fk_analysis_outputs_analysis_id FOREIGN KEY (analysis_id) REFERENCES analysis(id);
ALTER TABLE analysis_outputs ADD CONSTRAINT fk_analysis_outputs_file_id FOREIGN KEY (file_id) REFERENCES output_file(id) ON DELETE CASCADE;
ALTER TABLE analysis_outputs ADD CONSTRAINT fk_analysis_outputs_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_analysis_runner_project ON analysis_runner(project);
CREATE INDEX idx_analysis_runner_submitting_user ON analysis_runner(submitting_user);
CREATE INDEX idx_analysis_runner_audit_log_id ON analysis_runner(audit_log_id);


ALTER TABLE analysis_runner ADD CONSTRAINT fk_analysis_runner_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE analysis_runner ADD CONSTRAINT fk_analysis_runner_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_analysis_sequencing_group_sequencing_group_id ON analysis_sequencing_group(sequencing_group_id);
CREATE INDEX idx_analysis_sequencing_group_audit_log_id ON analysis_sequencing_group(audit_log_id);
CREATE INDEX idx_analysis_sequencing_group_analysis_id ON analysis_sequencing_group(analysis_id);

ALTER TABLE analysis_sequencing_group ADD CONSTRAINT fk_analysis_sequencing_group_analysis_id FOREIGN KEY (analysis_id) REFERENCES analysis(id);
ALTER TABLE analysis_sequencing_group ADD CONSTRAINT fk_analysis_sequencing_group_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES sequencing_group(id);
ALTER TABLE analysis_sequencing_group ADD CONSTRAINT fk_analysis_sequencing_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE analysis_type ADD CONSTRAINT fk_analysis_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_assay_type ON assay(type);
CREATE INDEX idx_assay_sample_id ON assay(sample_id);
CREATE INDEX idx_assay_audit_log_id ON assay(audit_log_id);


ALTER TABLE assay ADD CONSTRAINT fk_assay_sample_id FOREIGN KEY (sample_id) REFERENCES sample(id);
ALTER TABLE assay ADD CONSTRAINT fk_assay_type FOREIGN KEY (type) REFERENCES assay_type(id);
ALTER TABLE assay ADD CONSTRAINT fk_assay_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_assay_comment_audit_log_id ON assay_comment(audit_log_id);
CREATE INDEX idx_assay_comment_assay_id ON assay_comment(assay_id);

ALTER TABLE assay_comment ADD CONSTRAINT fk_assay_comment_comment_id FOREIGN KEY (comment_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE assay_comment ADD CONSTRAINT fk_assay_comment_assay_id FOREIGN KEY (assay_id) REFERENCES assay(id);
ALTER TABLE assay_comment ADD CONSTRAINT fk_assay_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE UNIQUE INDEX idx_assay_external_id_project_external_id ON assay_external_id(project, external_id);
CREATE INDEX idx_assay_external_id_audit_log_id ON assay_external_id(audit_log_id);

ALTER TABLE assay_external_id ADD CONSTRAINT fk_assay_external_id_assay_id FOREIGN KEY (assay_id) REFERENCES assay(id);
ALTER TABLE assay_external_id ADD CONSTRAINT fk_assay_external_id_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE assay_external_id ADD CONSTRAINT fk_assay_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE assay_type ADD CONSTRAINT fk_assay_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_audit_log_auth_project ON audit_log(auth_project);
ALTER TABLE audit_log ADD CONSTRAINT fk_audit_log_auth_project FOREIGN KEY (auth_project) REFERENCES project(id);


COMMENT ON COLUMN cohort.template_id IS 'Following user feedback session, we are renaming the derived_from column to template_id to better reflect its purpose.';

CREATE INDEX idx_cohort_audit_log_id ON cohort(audit_log_id);
CREATE INDEX idx_cohort_project ON cohort(project);

ALTER TABLE cohort ADD CONSTRAINT fk_cohort_template_id FOREIGN KEY (template_id) REFERENCES cohort_template(id);
ALTER TABLE cohort ADD CONSTRAINT fk_cohort_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE cohort ADD CONSTRAINT fk_cohort_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_cohort_sequencing_group_sequencing_group_id ON cohort_sequencing_group(sequencing_group_id);
CREATE INDEX idx_cohort_sequencing_group_cohort_id ON cohort_sequencing_group(cohort_id);
CREATE INDEX idx_cohort_sequencing_group_audit_log_id ON cohort_sequencing_group(audit_log_id);


ALTER TABLE cohort_sequencing_group ADD CONSTRAINT fk_cohort_sequencing_group_cohort_id FOREIGN KEY (cohort_id) REFERENCES cohort(id);
ALTER TABLE cohort_sequencing_group ADD CONSTRAINT fk_cohort_sequencing_group_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES sequencing_group(id);
ALTER TABLE cohort_sequencing_group ADD CONSTRAINT fk_cohort_sequencing_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_cohort_template_project ON cohort_template(project);
CREATE INDEX idx_cohort_template_audit_log_id ON cohort_template(audit_log_id);

ALTER TABLE cohort_template ADD CONSTRAINT fk_cohort_template_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE cohort_template ADD CONSTRAINT fk_cohort_template_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_comment_parent_id ON comment(parent_id);
CREATE INDEX idx_comment_audit_log_id ON comment(audit_log_id);

ALTER TABLE comment ADD CONSTRAINT fk_comment_parent_id FOREIGN KEY (parent_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE comment ADD CONSTRAINT fk_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_family_audit_log_id ON family(audit_log_id);
CREATE INDEX idx_family_project ON family(project);

ALTER TABLE family ADD CONSTRAINT fk_family_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE family ADD CONSTRAINT fk_family_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_family_comment_family_id ON family_comment(family_id);
CREATE INDEX idx_family_comment_audit_log_id ON family_comment(audit_log_id);

ALTER TABLE family_comment ADD CONSTRAINT fk_family_comment_comment_id FOREIGN KEY (comment_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE family_comment ADD CONSTRAINT fk_family_comment_family_id FOREIGN KEY (family_id) REFERENCES family(id);
ALTER TABLE family_comment ADD CONSTRAINT fk_family_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE UNIQUE INDEX idx_family_external_id_project_external_id ON family_external_id(project, external_id);
CREATE INDEX idx_family_external_id_audit_log_id ON family_external_id(audit_log_id);


ALTER TABLE family_external_id ADD CONSTRAINT fk_family_external_id_family_id FOREIGN KEY (family_id) REFERENCES family(id);
ALTER TABLE family_external_id ADD CONSTRAINT fk_family_external_id_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE family_external_id ADD CONSTRAINT fk_family_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_family_participant_family_id ON family_participant(family_id);
CREATE INDEX idx_family_participant_paternal_participant_id ON family_participant(paternal_participant_id);
CREATE INDEX idx_family_participant_maternal_participant_id ON family_participant(maternal_participant_id);
CREATE INDEX idx_family_participant_audit_log_id ON family_participant(audit_log_id);

ALTER TABLE family_participant ADD CONSTRAINT fk_family_participant_family_id FOREIGN KEY (family_id) REFERENCES family(id);
ALTER TABLE family_participant ADD CONSTRAINT fk_family_participant_participant_id FOREIGN KEY (participant_id) REFERENCES participant(id);
ALTER TABLE family_participant ADD CONSTRAINT fk_family_participant_paternal_participant_id FOREIGN KEY (paternal_participant_id) REFERENCES participant(id);
ALTER TABLE family_participant ADD CONSTRAINT fk_family_participant_maternal_participant_id FOREIGN KEY (maternal_participant_id) REFERENCES participant(id);
ALTER TABLE family_participant ADD CONSTRAINT fk_family_participant_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE "group" ADD CONSTRAINT fk_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_group_member_group_id ON group_member(group_id);
ALTER TABLE group_member ADD CONSTRAINT fk_group_member_group_id FOREIGN KEY (group_id) REFERENCES "group"(id);
ALTER TABLE group_member ADD CONSTRAINT fk_group_member_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_output_file_parent_id ON output_file(parent_id);
CREATE INDEX idx_output_file_audit_log_id ON output_file(audit_log_id);
ALTER TABLE output_file ADD CONSTRAINT fk_output_file_parent_id FOREIGN KEY (parent_id) REFERENCES output_file(id) ON DELETE SET NULL;
ALTER TABLE output_file ADD CONSTRAINT fk_output_file_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_participant_audit_log_id ON participant(audit_log_id);
CREATE INDEX idx_participant_project ON participant(project);
ALTER TABLE participant ADD CONSTRAINT fk_participant_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE participant ADD CONSTRAINT fk_participant_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_participant_comment_participant_id ON participant_comment(participant_id);
CREATE INDEX idx_participant_comment_audit_log_id ON participant_comment(audit_log_id);

ALTER TABLE participant_comment ADD CONSTRAINT fk_participant_comment_comment_id FOREIGN KEY (comment_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE participant_comment ADD CONSTRAINT fk_participant_comment_participant_id FOREIGN KEY (participant_id) REFERENCES participant(id);
ALTER TABLE participant_comment ADD CONSTRAINT fk_participant_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE UNIQUE INDEX idx_participant_external_id_project_external_id ON participant_external_id(project, external_id);
CREATE INDEX idx_participant_external_id_audit_log_id ON participant_external_id(audit_log_id);

ALTER TABLE participant_external_id ADD CONSTRAINT fk_participant_external_id_participant_id FOREIGN KEY (participant_id) REFERENCES participant(id);
ALTER TABLE participant_external_id ADD CONSTRAINT fk_participant_external_id_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE participant_external_id ADD CONSTRAINT fk_participant_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE UNIQUE INDEX idx_participant_phenotypes_pid_hpo_description ON participant_phenotypes(participant_id, hpo_term, description);
CREATE INDEX idx_participant_phenotypes_audit_log_id ON participant_phenotypes(audit_log_id);

ALTER TABLE participant_phenotypes ADD CONSTRAINT fk_participant_phenotypes_participant_id FOREIGN KEY (participant_id) REFERENCES participant(id);
ALTER TABLE participant_phenotypes ADD CONSTRAINT fk_participant_phenotypes_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE project ADD CONSTRAINT fk_project_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_project_comment_project_id ON project_comment(project_id);
CREATE INDEX idx_project_comment_audit_log_id ON project_comment(audit_log_id);


ALTER TABLE project_comment ADD CONSTRAINT fk_project_comment_comment_id FOREIGN KEY (comment_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE project_comment ADD CONSTRAINT fk_project_comment_project_id FOREIGN KEY (project_id) REFERENCES project(id);
ALTER TABLE project_comment ADD CONSTRAINT fk_project_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_project_member_audit_log_id ON project_member(audit_log_id);
CREATE INDEX idx_project_member_member ON project_member(member);

ALTER TABLE project_member ADD CONSTRAINT fk_project_member_project_id FOREIGN KEY (project_id) REFERENCES project(id);
ALTER TABLE project_member ADD CONSTRAINT fk_project_member_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_sample_participant_id ON sample(participant_id);
CREATE INDEX idx_sample_type ON sample(type);
CREATE INDEX idx_sample_audit_log_id ON sample(audit_log_id);
CREATE INDEX idx_sample_project ON sample(project);
CREATE INDEX idx_sample_root_id ON sample(sample_root_id);
CREATE INDEX idx_sample_parent_id ON sample(sample_parent_id);

ALTER TABLE sample ADD CONSTRAINT fk_sample_participant_id FOREIGN KEY (participant_id) REFERENCES participant(id);
ALTER TABLE sample ADD CONSTRAINT fk_sample_type FOREIGN KEY (type) REFERENCES sample_type(id);
ALTER TABLE sample ADD CONSTRAINT fk_sample_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE sample ADD CONSTRAINT fk_sample_root_id FOREIGN KEY (sample_root_id) REFERENCES sample(id) ON DELETE CASCADE;
ALTER TABLE sample ADD CONSTRAINT fk_sample_parent_id FOREIGN KEY (sample_parent_id) REFERENCES sample(id) ON DELETE CASCADE;
ALTER TABLE sample ADD CONSTRAINT fk_sample_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE INDEX idx_sample_comment_sample_id ON sample_comment(sample_id);
CREATE INDEX idx_sample_comment_audit_log_id ON sample_comment(audit_log_id);

ALTER TABLE sample_comment ADD CONSTRAINT fk_sample_comment_comment_id FOREIGN KEY (comment_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE sample_comment ADD CONSTRAINT fk_sample_comment_sample_id FOREIGN KEY (sample_id) REFERENCES sample(id);
ALTER TABLE sample_comment ADD CONSTRAINT fk_sample_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


CREATE UNIQUE INDEX idx_sample_external_id_project_external_id ON sample_external_id(project, external_id);
CREATE INDEX idx_sample_external_id_audit_log_id ON sample_external_id(audit_log_id);

ALTER TABLE sample_external_id ADD CONSTRAINT fk_sample_external_id_sample_id FOREIGN KEY (sample_id) REFERENCES sample(id);
ALTER TABLE sample_external_id ADD CONSTRAINT fk_sample_external_id_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE sample_external_id ADD CONSTRAINT fk_sample_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE sample_type ADD CONSTRAINT fk_sample_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_sequencing_group_sample_id ON sequencing_group(sample_id);
CREATE INDEX idx_sequencing_group_type ON sequencing_group(type);
CREATE INDEX idx_sequencing_group_technology ON sequencing_group(technology);
CREATE INDEX idx_sequencing_group_audit_log_id ON sequencing_group(audit_log_id);

ALTER TABLE sequencing_group ADD CONSTRAINT fk_sequencing_group_sample_id FOREIGN KEY (sample_id) REFERENCES sample(id);
ALTER TABLE sequencing_group ADD CONSTRAINT fk_sequencing_group_type FOREIGN KEY (type) REFERENCES sequencing_type(id);
ALTER TABLE sequencing_group ADD CONSTRAINT fk_sequencing_group_technology FOREIGN KEY (technology) REFERENCES sequencing_technology(id);
ALTER TABLE sequencing_group ADD CONSTRAINT fk_sequencing_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_sequencing_group_assay_sequencing_group_id ON sequencing_group_assay(sequencing_group_id);
CREATE INDEX idx_sequencing_group_assay_assay_id ON sequencing_group_assay(assay_id);
CREATE INDEX idx_sequencing_group_assay_audit_log_id ON sequencing_group_assay(audit_log_id);

ALTER TABLE sequencing_group_assay ADD CONSTRAINT fk_sequencing_group_assay_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES sequencing_group(id);
ALTER TABLE sequencing_group_assay ADD CONSTRAINT fk_sequencing_group_assay_assay_id FOREIGN KEY (assay_id) REFERENCES assay(id);
ALTER TABLE sequencing_group_assay ADD CONSTRAINT fk_sequencing_group_assay_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

CREATE INDEX idx_sequencing_group_comment_audit_log_id ON sequencing_group_comment(audit_log_id);
CREATE INDEX idx_sequencing_group_comment_sequencing_group_id ON sequencing_group_comment(sequencing_group_id);

ALTER TABLE sequencing_group_comment ADD CONSTRAINT fk_sequencing_group_comment_comment_id FOREIGN KEY (comment_id) REFERENCES comment(id) ON DELETE CASCADE;
ALTER TABLE sequencing_group_comment ADD CONSTRAINT fk_sequencing_group_comment_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES sequencing_group(id);
ALTER TABLE sequencing_group_comment ADD CONSTRAINT fk_sequencing_group_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


COMMENT ON COLUMN sequencing_group_external_id.null_if_archived IS 'Setting this to NULL will allow the external id to be reused within a project if sgs are archived';
CREATE UNIQUE INDEX idx_sequencing_group_external_id_unique_idx
    ON sequencing_group_external_id(project, external_id, null_if_archived);

CREATE INDEX idx_sequencing_group_external_id_sequencing_group_id ON sequencing_group_external_id(sequencing_group_id);
CREATE INDEX idx_sequencing_group_external_id_audit_log_id ON sequencing_group_external_id(audit_log_id);

ALTER TABLE sequencing_group_external_id ADD CONSTRAINT fk_sequencing_group_external_id_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES sequencing_group(id);
ALTER TABLE sequencing_group_external_id ADD CONSTRAINT fk_sequencing_group_external_id_project FOREIGN KEY (project) REFERENCES project(id);
ALTER TABLE sequencing_group_external_id ADD CONSTRAINT fk_sequencing_group_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);


ALTER TABLE sequencing_platform ADD CONSTRAINT fk_sequencing_platform_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE sequencing_technology ADD CONSTRAINT fk_sequencing_technology_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

ALTER TABLE sequencing_type ADD CONSTRAINT fk_sequencing_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES audit_log(id);

-- migrate:down

SET search_path TO main;

-- Drop foreign keys and indexes for sequencing_type
ALTER TABLE sequencing_type DROP CONSTRAINT IF EXISTS fk_sequencing_type_audit_log_id;

-- Drop foreign keys and indexes for sequencing_technology
ALTER TABLE sequencing_technology DROP CONSTRAINT IF EXISTS fk_sequencing_technology_audit_log_id;

-- Drop foreign keys and indexes for sequencing_platform
ALTER TABLE sequencing_platform DROP CONSTRAINT IF EXISTS fk_sequencing_platform_audit_log_id;

-- Drop foreign keys and indexes for sequencing_group_external_id
ALTER TABLE sequencing_group_external_id DROP CONSTRAINT IF EXISTS fk_sequencing_group_external_id_audit_log_id;
ALTER TABLE sequencing_group_external_id DROP CONSTRAINT IF EXISTS fk_sequencing_group_external_id_project;
ALTER TABLE sequencing_group_external_id DROP CONSTRAINT IF EXISTS fk_sequencing_group_external_id_sequencing_group_id;
DROP INDEX IF EXISTS idx_sequencing_group_external_id_audit_log_id;
DROP INDEX IF EXISTS idx_sequencing_group_external_id_sequencing_group_id;
DROP INDEX IF EXISTS idx_sequencing_group_external_id_unique_idx;

-- Drop foreign keys and indexes for sequencing_group_comment
ALTER TABLE sequencing_group_comment DROP CONSTRAINT IF EXISTS fk_sequencing_group_comment_audit_log_id;
ALTER TABLE sequencing_group_comment DROP CONSTRAINT IF EXISTS fk_sequencing_group_comment_sequencing_group_id;
ALTER TABLE sequencing_group_comment DROP CONSTRAINT IF EXISTS fk_sequencing_group_comment_comment_id;
DROP INDEX IF EXISTS idx_sequencing_group_comment_sequencing_group_id;
DROP INDEX IF EXISTS idx_sequencing_group_comment_audit_log_id;

-- Drop foreign keys and indexes for sequencing_group_assay
ALTER TABLE sequencing_group_assay DROP CONSTRAINT IF EXISTS fk_sequencing_group_assay_audit_log_id;
ALTER TABLE sequencing_group_assay DROP CONSTRAINT IF EXISTS fk_sequencing_group_assay_assay_id;
ALTER TABLE sequencing_group_assay DROP CONSTRAINT IF EXISTS fk_sequencing_group_assay_sequencing_group_id;
DROP INDEX IF EXISTS idx_sequencing_group_assay_audit_log_id;
DROP INDEX IF EXISTS idx_sequencing_group_assay_assay_id;
DROP INDEX IF EXISTS idx_sequencing_group_assay_sequencing_group_id;

-- Drop foreign keys and indexes for sequencing_group
ALTER TABLE sequencing_group DROP CONSTRAINT IF EXISTS fk_sequencing_group_audit_log_id;
ALTER TABLE sequencing_group DROP CONSTRAINT IF EXISTS fk_sequencing_group_technology;
ALTER TABLE sequencing_group DROP CONSTRAINT IF EXISTS fk_sequencing_group_type;
ALTER TABLE sequencing_group DROP CONSTRAINT IF EXISTS fk_sequencing_group_sample_id;
DROP INDEX IF EXISTS idx_sequencing_group_audit_log_id;
DROP INDEX IF EXISTS idx_sequencing_group_technology;
DROP INDEX IF EXISTS idx_sequencing_group_type;
DROP INDEX IF EXISTS idx_sequencing_group_sample_id;

-- Drop foreign keys and indexes for sample_type
ALTER TABLE sample_type DROP CONSTRAINT IF EXISTS fk_sample_type_audit_log_id;

-- Drop foreign keys and indexes for sample_external_id
ALTER TABLE sample_external_id DROP CONSTRAINT IF EXISTS fk_sample_external_id_audit_log_id;
ALTER TABLE sample_external_id DROP CONSTRAINT IF EXISTS fk_sample_external_id_project;
ALTER TABLE sample_external_id DROP CONSTRAINT IF EXISTS fk_sample_external_id_sample_id;
DROP INDEX IF EXISTS idx_sample_external_id_audit_log_id;
DROP INDEX IF EXISTS idx_sample_external_id_project_external_id;

-- Drop foreign keys and indexes for sample_comment
ALTER TABLE sample_comment DROP CONSTRAINT IF EXISTS fk_sample_comment_audit_log_id;
ALTER TABLE sample_comment DROP CONSTRAINT IF EXISTS fk_sample_comment_sample_id;
ALTER TABLE sample_comment DROP CONSTRAINT IF EXISTS fk_sample_comment_comment_id;
DROP INDEX IF EXISTS idx_sample_comment_audit_log_id;
DROP INDEX IF EXISTS idx_sample_comment_sample_id;

-- Drop foreign keys and indexes for sample
ALTER TABLE sample DROP CONSTRAINT IF EXISTS fk_sample_audit_log_id;
ALTER TABLE sample DROP CONSTRAINT IF EXISTS fk_sample_parent_id;
ALTER TABLE sample DROP CONSTRAINT IF EXISTS fk_sample_root_id;
ALTER TABLE sample DROP CONSTRAINT IF EXISTS fk_sample_project;
ALTER TABLE sample DROP CONSTRAINT IF EXISTS fk_sample_type;
ALTER TABLE sample DROP CONSTRAINT IF EXISTS fk_sample_participant_id;
DROP INDEX IF EXISTS idx_sample_parent_id;
DROP INDEX IF EXISTS idx_sample_root_id;
DROP INDEX IF EXISTS idx_sample_project;
DROP INDEX IF EXISTS idx_sample_audit_log_id;
DROP INDEX IF EXISTS idx_sample_type;
DROP INDEX IF EXISTS idx_sample_participant_id;

-- Drop foreign keys and indexes for project_member
ALTER TABLE project_member DROP CONSTRAINT IF EXISTS fk_project_member_audit_log_id;
ALTER TABLE project_member DROP CONSTRAINT IF EXISTS fk_project_member_project_id;
DROP INDEX IF EXISTS idx_project_member_member;
DROP INDEX IF EXISTS idx_project_member_audit_log_id;

-- Drop foreign keys and indexes for project_comment
ALTER TABLE project_comment DROP CONSTRAINT IF EXISTS fk_project_comment_audit_log_id;
ALTER TABLE project_comment DROP CONSTRAINT IF EXISTS fk_project_comment_project_id;
ALTER TABLE project_comment DROP CONSTRAINT IF EXISTS fk_project_comment_comment_id;
DROP INDEX IF EXISTS idx_project_comment_audit_log_id;
DROP INDEX IF EXISTS idx_project_comment_project_id;

-- Drop foreign keys and indexes for project
ALTER TABLE project DROP CONSTRAINT IF EXISTS fk_project_audit_log_id;

-- Drop foreign keys and indexes for participant_phenotypes
ALTER TABLE participant_phenotypes DROP CONSTRAINT IF EXISTS fk_participant_phenotypes_audit_log_id;
ALTER TABLE participant_phenotypes DROP CONSTRAINT IF EXISTS fk_participant_phenotypes_participant_id;
DROP INDEX IF EXISTS idx_participant_phenotypes_audit_log_id;
DROP INDEX IF EXISTS idx_participant_phenotypes_pid_hpo_description;

-- Drop foreign keys and indexes for participant_external_id
ALTER TABLE participant_external_id DROP CONSTRAINT IF EXISTS fk_participant_external_id_audit_log_id;
ALTER TABLE participant_external_id DROP CONSTRAINT IF EXISTS fk_participant_external_id_project;
ALTER TABLE participant_external_id DROP CONSTRAINT IF EXISTS fk_participant_external_id_participant_id;
DROP INDEX IF EXISTS idx_participant_external_id_audit_log_id;
DROP INDEX IF EXISTS idx_participant_external_id_project_external_id;

-- Drop foreign keys and indexes for participant_comment
ALTER TABLE participant_comment DROP CONSTRAINT IF EXISTS fk_participant_comment_audit_log_id;
ALTER TABLE participant_comment DROP CONSTRAINT IF EXISTS fk_participant_comment_participant_id;
ALTER TABLE participant_comment DROP CONSTRAINT IF EXISTS fk_participant_comment_comment_id;
DROP INDEX IF EXISTS idx_participant_comment_audit_log_id;
DROP INDEX IF EXISTS idx_participant_comment_participant_id;

-- Drop foreign keys and indexes for participant
ALTER TABLE participant DROP CONSTRAINT IF EXISTS fk_participant_audit_log_id;
ALTER TABLE participant DROP CONSTRAINT IF EXISTS fk_participant_project;
DROP INDEX IF EXISTS idx_participant_project;
DROP INDEX IF EXISTS idx_participant_audit_log_id;

-- Drop foreign keys and indexes for output_file
ALTER TABLE output_file DROP CONSTRAINT IF EXISTS fk_output_file_audit_log_id;
ALTER TABLE output_file DROP CONSTRAINT IF EXISTS fk_output_file_parent_id;
DROP INDEX IF EXISTS idx_output_file_audit_log_id;
DROP INDEX IF EXISTS idx_output_file_parent_id;

-- Drop foreign keys and indexes for group_member
ALTER TABLE group_member DROP CONSTRAINT IF EXISTS fk_group_member_audit_log_id;
ALTER TABLE group_member DROP CONSTRAINT IF EXISTS fk_group_member_group_id;
DROP INDEX IF EXISTS idx_group_member_group_id;

-- Drop foreign keys and indexes for group
ALTER TABLE "group" DROP CONSTRAINT IF EXISTS fk_group_audit_log_id;

-- Drop foreign keys and indexes for family_participant
ALTER TABLE family_participant DROP CONSTRAINT IF EXISTS fk_family_participant_audit_log_id;
ALTER TABLE family_participant DROP CONSTRAINT IF EXISTS fk_family_participant_maternal_participant_id;
ALTER TABLE family_participant DROP CONSTRAINT IF EXISTS fk_family_participant_paternal_participant_id;
ALTER TABLE family_participant DROP CONSTRAINT IF EXISTS fk_family_participant_participant_id;
ALTER TABLE family_participant DROP CONSTRAINT IF EXISTS fk_family_participant_family_id;
DROP INDEX IF EXISTS idx_family_participant_audit_log_id;
DROP INDEX IF EXISTS idx_family_participant_maternal_participant_id;
DROP INDEX IF EXISTS idx_family_participant_paternal_participant_id;
DROP INDEX IF EXISTS idx_family_participant_family_id;

-- Drop foreign keys and indexes for family_external_id
ALTER TABLE family_external_id DROP CONSTRAINT IF EXISTS fk_family_external_id_audit_log_id;
ALTER TABLE family_external_id DROP CONSTRAINT IF EXISTS fk_family_external_id_project;
ALTER TABLE family_external_id DROP CONSTRAINT IF EXISTS fk_family_external_id_family_id;
DROP INDEX IF EXISTS idx_family_external_id_audit_log_id;
DROP INDEX IF EXISTS idx_family_external_id_project_external_id;

-- Drop foreign keys and indexes for family_comment
ALTER TABLE family_comment DROP CONSTRAINT IF EXISTS fk_family_comment_audit_log_id;
ALTER TABLE family_comment DROP CONSTRAINT IF EXISTS fk_family_comment_family_id;
ALTER TABLE family_comment DROP CONSTRAINT IF EXISTS fk_family_comment_comment_id;
DROP INDEX IF EXISTS idx_family_comment_audit_log_id;
DROP INDEX IF EXISTS idx_family_comment_family_id;

-- Drop foreign keys and indexes for family
ALTER TABLE family DROP CONSTRAINT IF EXISTS fk_family_audit_log_id;
ALTER TABLE family DROP CONSTRAINT IF EXISTS fk_family_project;
DROP INDEX IF EXISTS idx_family_project;
DROP INDEX IF EXISTS idx_family_audit_log_id;

-- Drop foreign keys and indexes for comment
ALTER TABLE comment DROP CONSTRAINT IF EXISTS fk_comment_audit_log_id;
ALTER TABLE comment DROP CONSTRAINT IF EXISTS fk_comment_parent_id;
DROP INDEX IF EXISTS idx_comment_audit_log_id;
DROP INDEX IF EXISTS idx_comment_parent_id;

-- Drop foreign keys and indexes for cohort_template
ALTER TABLE cohort_template DROP CONSTRAINT IF EXISTS fk_cohort_template_audit_log_id;
ALTER TABLE cohort_template DROP CONSTRAINT IF EXISTS fk_cohort_template_project;
DROP INDEX IF EXISTS idx_cohort_template_audit_log_id;
DROP INDEX IF EXISTS idx_cohort_template_project;

-- Drop foreign keys and indexes for cohort_sequencing_group
ALTER TABLE cohort_sequencing_group DROP CONSTRAINT IF EXISTS fk_cohort_sequencing_group_audit_log_id;
ALTER TABLE cohort_sequencing_group DROP CONSTRAINT IF EXISTS fk_cohort_sequencing_group_sequencing_group_id;
ALTER TABLE cohort_sequencing_group DROP CONSTRAINT IF EXISTS fk_cohort_sequencing_group_cohort_id;
DROP INDEX IF EXISTS idx_cohort_sequencing_group_audit_log_id;
DROP INDEX IF EXISTS idx_cohort_sequencing_group_cohort_id;
DROP INDEX IF EXISTS idx_cohort_sequencing_group_sequencing_group_id;

-- Drop foreign keys and indexes for cohort
ALTER TABLE cohort DROP CONSTRAINT IF EXISTS fk_cohort_audit_log_id;
ALTER TABLE cohort DROP CONSTRAINT IF EXISTS fk_cohort_project;
ALTER TABLE cohort DROP CONSTRAINT IF EXISTS fk_cohort_template_id;
DROP INDEX IF EXISTS idx_cohort_project;
DROP INDEX IF EXISTS idx_cohort_audit_log_id;

-- Drop foreign keys and indexes for audit_log
ALTER TABLE audit_log DROP CONSTRAINT IF EXISTS fk_audit_log_auth_project;
DROP INDEX IF EXISTS idx_audit_log_auth_project;

-- Drop foreign keys and indexes for assay_type
ALTER TABLE assay_type DROP CONSTRAINT IF EXISTS fk_assay_type_audit_log_id;

-- Drop foreign keys and indexes for assay_external_id
ALTER TABLE assay_external_id DROP CONSTRAINT IF EXISTS fk_assay_external_id_audit_log_id;
ALTER TABLE assay_external_id DROP CONSTRAINT IF EXISTS fk_assay_external_id_project;
ALTER TABLE assay_external_id DROP CONSTRAINT IF EXISTS fk_assay_external_id_assay_id;
DROP INDEX IF EXISTS idx_assay_external_id_audit_log_id;
DROP INDEX IF EXISTS idx_assay_external_id_project_external_id;

-- Drop foreign keys and indexes for assay_comment
ALTER TABLE assay_comment DROP CONSTRAINT IF EXISTS fk_assay_comment_audit_log_id;
ALTER TABLE assay_comment DROP CONSTRAINT IF EXISTS fk_assay_comment_assay_id;
ALTER TABLE assay_comment DROP CONSTRAINT IF EXISTS fk_assay_comment_comment_id;
DROP INDEX IF EXISTS idx_assay_comment_assay_id;
DROP INDEX IF EXISTS idx_assay_comment_audit_log_id;

-- Drop foreign keys and indexes for assay
ALTER TABLE assay DROP CONSTRAINT IF EXISTS fk_assay_audit_log_id;
ALTER TABLE assay DROP CONSTRAINT IF EXISTS fk_assay_type;
ALTER TABLE assay DROP CONSTRAINT IF EXISTS fk_assay_sample_id;
DROP INDEX IF EXISTS idx_assay_audit_log_id;
DROP INDEX IF EXISTS idx_assay_sample_id;
DROP INDEX IF EXISTS idx_assay_type;

-- Drop foreign keys and indexes for analysis_type
ALTER TABLE analysis_type DROP CONSTRAINT IF EXISTS fk_analysis_type_audit_log_id;

-- Drop foreign keys and indexes for analysis_sequencing_group
ALTER TABLE analysis_sequencing_group DROP CONSTRAINT IF EXISTS fk_analysis_sequencing_group_audit_log_id;
ALTER TABLE analysis_sequencing_group DROP CONSTRAINT IF EXISTS fk_analysis_sequencing_group_sequencing_group_id;
ALTER TABLE analysis_sequencing_group DROP CONSTRAINT IF EXISTS fk_analysis_sequencing_group_analysis_id;
DROP INDEX IF EXISTS idx_analysis_sequencing_group_analysis_id;
DROP INDEX IF EXISTS idx_analysis_sequencing_group_audit_log_id;
DROP INDEX IF EXISTS idx_analysis_sequencing_group_sequencing_group_id;

-- Drop foreign keys and indexes for analysis_runner
ALTER TABLE analysis_runner DROP CONSTRAINT IF EXISTS fk_analysis_runner_audit_log_id;
ALTER TABLE analysis_runner DROP CONSTRAINT IF EXISTS fk_analysis_runner_project;
DROP INDEX IF EXISTS idx_analysis_runner_audit_log_id;
DROP INDEX IF EXISTS idx_analysis_runner_submitting_user;
DROP INDEX IF EXISTS idx_analysis_runner_project;

-- Drop foreign keys and indexes for analysis_outputs
ALTER TABLE analysis_outputs DROP CONSTRAINT IF EXISTS fk_analysis_outputs_audit_log_id;
ALTER TABLE analysis_outputs DROP CONSTRAINT IF EXISTS fk_analysis_outputs_file_id;
ALTER TABLE analysis_outputs DROP CONSTRAINT IF EXISTS fk_analysis_outputs_analysis_id;
DROP INDEX IF EXISTS idx_analysis_outputs_audit_log_id;
DROP INDEX IF EXISTS idx_analysis_outputs_file_id;
DROP INDEX IF EXISTS idx_analysis_outputs_analysis_output;
DROP INDEX IF EXISTS idx_analysis_outputs_analysis_file;

-- Drop foreign keys and indexes for analysis_cohort
ALTER TABLE analysis_cohort DROP CONSTRAINT IF EXISTS fk_analysis_cohort_audit_log_id;
ALTER TABLE analysis_cohort DROP CONSTRAINT IF EXISTS fk_analysis_cohort_analysis_id;
ALTER TABLE analysis_cohort DROP CONSTRAINT IF EXISTS fk_analysis_cohort_cohort_id;
DROP INDEX IF EXISTS idx_analysis_cohort_audit_log_id;
DROP INDEX IF EXISTS idx_analysis_cohort_analysis_id;
DROP INDEX IF EXISTS idx_analysis_cohort_cohort_id;

-- Drop foreign keys and indexes for analysis
ALTER TABLE analysis DROP CONSTRAINT IF EXISTS fk_analysis_audit_log_id;
ALTER TABLE analysis DROP CONSTRAINT IF EXISTS fk_analysis_type;
ALTER TABLE analysis DROP CONSTRAINT IF EXISTS fk_analysis_project;
DROP INDEX IF EXISTS idx_analysis_audit_log_id;
DROP INDEX IF EXISTS idx_analysis_type;
DROP INDEX IF EXISTS idx_analysis_project;
