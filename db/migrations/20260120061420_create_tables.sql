-- migrate:up


CREATE SCHEMA IF NOT EXISTS main;

SET search_path TO main;

-- Create custom enum types
CREATE TYPE cohort_status AS ENUM ('active', 'archived');
CREATE TYPE comment_status AS ENUM ('active', 'deleted');
CREATE TYPE project_member_role AS ENUM ('reader', 'writer', 'contributor', 'project_admin', 'project_member_admin');

-- analysis table
CREATE TYPE analysis_status AS ENUM ('queued', 'in-progress', 'failed', 'completed', 'unknown');
CREATE TABLE analysis (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    type TEXT,
    output TEXT,
    status analysis_status,
    timestamp_completed TIMESTAMPTZ,
    project INTEGER NOT NULL,
    author TEXT,
    meta JSONB DEFAULT '{}',
    active BOOLEAN DEFAULT TRUE,
    on_behalf_of TEXT,
    audit_log_id INTEGER
);





-- analysis_cohort table
CREATE TABLE analysis_cohort (
    cohort_id INTEGER NOT NULL,
    analysis_id INTEGER NOT NULL,
    audit_log_id INTEGER
);



-- analysis_outputs table
CREATE TABLE analysis_outputs (
    analysis_id INTEGER NOT NULL,
    file_id INTEGER,
    output TEXT,
    json_structure TEXT,
    audit_log_id INTEGER,
    CONSTRAINT chk_file_id_output CHECK (
        (file_id IS NOT NULL AND output IS NULL) OR
        (file_id IS NULL AND output IS NOT NULL)
    )
);



-- analysis_runner table
CREATE TABLE analysis_runner (
    ar_guid text PRIMARY KEY,
    project INTEGER NOT NULL,
    timestamp TIMESTAMPTZ,
    access_level TEXT NOT NULL,
    repository TEXT NOT NULL,
    commit TEXT NOT NULL,
    output_path TEXT NOT NULL,
    script TEXT,
    description TEXT NOT NULL,
    driver_image TEXT NOT NULL,
    config_path TEXT,
    cwd TEXT,
    environment TEXT NOT NULL,
    hail_version TEXT,
    batch_url TEXT NOT NULL,
    submitting_user TEXT,
    meta JSONB DEFAULT '{}',
    audit_log_id INTEGER
);




-- analysis_sequencing_group table
CREATE TABLE analysis_sequencing_group (
    analysis_id INTEGER NOT NULL,
    sequencing_group_id INTEGER NOT NULL,
    audit_log_id INTEGER
);




-- analysis_type table
CREATE TABLE analysis_type (
    id text PRIMARY KEY,
    name TEXT NOT NULL,
    audit_log_id INTEGER
);




-- assay table
CREATE TABLE assay (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sample_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    meta JSONB DEFAULT '{}',
    author TEXT,
    audit_log_id INTEGER
);






-- assay_comment table
CREATE TABLE assay_comment (
    comment_id INTEGER PRIMARY KEY,
    assay_id INTEGER NOT NULL,
    audit_log_id INTEGER
);



-- assay_external_id table
CREATE TABLE assay_external_id (
    project INTEGER NOT NULL,
    assay_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    external_id TEXT NOT NULL,
    author TEXT,
    audit_log_id INTEGER,
    PRIMARY KEY (assay_id, name)
);



-- assay_type table
CREATE TABLE assay_type (
    id text PRIMARY KEY,
    name TEXT NOT NULL,
    audit_log_id INTEGER
);



-- audit_log table
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    author TEXT NOT NULL,
    on_behalf_of TEXT,
    ar_guid TEXT,
    comment TEXT,
    auth_project INTEGER,
    meta JSONB DEFAULT '{}'
);



-- cohort table
CREATE TABLE cohort (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    project INTEGER NOT NULL,
    template_id INTEGER,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    status cohort_status NOT NULL DEFAULT 'active',
    author TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    audit_log_id INTEGER

);


-- cohort_sequencing_group table
CREATE TABLE cohort_sequencing_group (
    cohort_id INTEGER NOT NULL,
    sequencing_group_id INTEGER NOT NULL,
    audit_log_id INTEGER
);




-- cohort_template table
CREATE TABLE cohort_template (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    criteria JSONB NOT NULL,
    project INTEGER NOT NULL,
    audit_log_id INTEGER
);



-- comment table
CREATE TABLE comment (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    parent_id INTEGER,
    content TEXT,
    status comment_status NOT NULL,
    audit_log_id INTEGER
);





-- family table
CREATE TABLE family (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    project INTEGER NOT NULL,
    description TEXT,
    coded_phenotype TEXT,
    author TEXT,
    audit_log_id INTEGER
);



-- family_comment table
CREATE TABLE family_comment (
    comment_id INTEGER PRIMARY KEY,
    family_id INTEGER NOT NULL,
    audit_log_id INTEGER
);




-- family_external_id table
CREATE TABLE family_external_id (
    project INTEGER NOT NULL,
    family_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    external_id TEXT NOT NULL,
    meta JSONB DEFAULT '{}',
    audit_log_id INTEGER NOT NULL,
    PRIMARY KEY (family_id, name)
);



-- family_participant table
CREATE TABLE family_participant (
    family_id INTEGER NOT NULL,
    participant_id INTEGER PRIMARY KEY,
    paternal_participant_id INTEGER,
    maternal_participant_id INTEGER,
    affected INTEGER,
    notes TEXT,
    author TEXT,
    audit_log_id INTEGER
);



-- group table
CREATE TABLE "group" (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT NOT NULL UNIQUE,
    audit_log_id INTEGER
);




-- group_member table
CREATE TABLE group_member (
    group_id INTEGER NOT NULL,
    member TEXT NOT NULL,
    author TEXT,
    audit_log_id INTEGER
);



-- output_file table
CREATE TABLE output_file (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    path TEXT NOT NULL UNIQUE,
    basename TEXT NOT NULL,
    dirname TEXT NOT NULL,
    nameroot TEXT NOT NULL,
    nameext TEXT,
    file_checksum TEXT,
    size BIGINT NOT NULL,
    meta JSONB DEFAULT '{}',
    valid BOOLEAN,
    parent_id INTEGER,
    audit_log_id INTEGER
);







-- participant table
CREATE TABLE participant (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    project INTEGER NOT NULL,
    author TEXT,
    reported_sex INTEGER,
    reported_gender TEXT,
    karyotype TEXT,
    meta JSONB DEFAULT '{}',
    audit_log_id INTEGER
);


-- participant_comment table
CREATE TABLE participant_comment (
    comment_id INTEGER PRIMARY KEY,
    participant_id INTEGER NOT NULL,
    audit_log_id INTEGER
);




-- participant_external_id table
CREATE TABLE participant_external_id (
    project INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    external_id TEXT NOT NULL,
    meta JSONB DEFAULT '{}',
    audit_log_id INTEGER NOT NULL,
    PRIMARY KEY (participant_id, name)
);



-- participant_phenotypes table
CREATE TABLE participant_phenotypes (
    participant_id INTEGER NOT NULL,
    hpo_term TEXT,
    description TEXT,
    author TEXT,
    value JSONB,
    audit_log_id INTEGER
);





-- project table
CREATE TABLE project (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(64) NOT NULL UNIQUE,
    dataset TEXT,
    meta JSONB DEFAULT '{}',
    author TEXT,
    audit_log_id INTEGER
);




-- project_comment table
CREATE TABLE project_comment (
    comment_id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    audit_log_id INTEGER
);




-- project_member table
CREATE TABLE project_member (
    project_id INTEGER NOT NULL,
    member TEXT NOT NULL,
    role project_member_role NOT NULL,
    audit_log_id INTEGER,
    PRIMARY KEY (project_id, member, role)
);



-- sample table
CREATE TABLE sample (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    project INTEGER NOT NULL,
    participant_id INTEGER,
    active BOOLEAN,
    meta JSONB DEFAULT '{}',
    type TEXT,
    sample_root_id INTEGER,
    sample_parent_id INTEGER,
    author TEXT,
    audit_log_id INTEGER
);



-- sample_comment table
CREATE TABLE sample_comment (
    comment_id INTEGER PRIMARY KEY,
    sample_id INTEGER NOT NULL,
    audit_log_id INTEGER
);


-- sample_external_id table
CREATE TABLE sample_external_id (
    project INTEGER NOT NULL,
    sample_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    external_id TEXT NOT NULL,
    meta JSONB DEFAULT '{}',
    audit_log_id INTEGER NOT NULL,
    PRIMARY KEY (sample_id, name)
);




-- sample_type table
CREATE TABLE sample_type (
    id text PRIMARY KEY,
    name TEXT NOT NULL,
    audit_log_id INTEGER
);




-- sequencing_group table
CREATE TABLE sequencing_group (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sample_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    technology TEXT NOT NULL,
    platform TEXT,
    meta JSONB DEFAULT '{}',
    archived BOOLEAN,
    author TEXT,
    audit_log_id INTEGER
);




-- sequencing_group_assay table
CREATE TABLE sequencing_group_assay (
    sequencing_group_id INTEGER NOT NULL,
    assay_id INTEGER NOT NULL,
    author TEXT,
    audit_log_id INTEGER
);




-- sequencing_group_comment table
CREATE TABLE sequencing_group_comment (
    comment_id INTEGER PRIMARY KEY,
    sequencing_group_id INTEGER NOT NULL,
    audit_log_id INTEGER
);



-- sequencing_group_external_id table
CREATE TABLE sequencing_group_external_id (
    project INTEGER NOT NULL,
    sequencing_group_id INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    name TEXT NOT NULL,
    author TEXT,
    null_if_archived smallint DEFAULT 1,
    audit_log_id INTEGER,
    PRIMARY KEY (sequencing_group_id, name)
);




-- sequencing_platform table
CREATE TABLE sequencing_platform (
    id text PRIMARY KEY,
    name TEXT NOT NULL,
    audit_log_id INTEGER
);





-- sequencing_technology table
CREATE TABLE sequencing_technology (
    id text PRIMARY KEY,
    name TEXT NOT NULL,
    audit_log_id INTEGER
);



-- sequencing_type table
CREATE TABLE sequencing_type (
    id text PRIMARY KEY,
    name TEXT NOT NULL,
    audit_log_id INTEGER
);

-- migrate:down

SET search_path TO main;

DROP TABLE IF EXISTS sequencing_type;
DROP TABLE IF EXISTS sequencing_technology;
DROP TABLE IF EXISTS sequencing_platform;
DROP TABLE IF EXISTS sequencing_group_external_id;
DROP TABLE IF EXISTS sequencing_group_comment;
DROP TABLE IF EXISTS sequencing_group_assay;
DROP TABLE IF EXISTS sequencing_group;
DROP TABLE IF EXISTS sample_type;
DROP TABLE IF EXISTS sample_external_id;
DROP TABLE IF EXISTS sample_comment;
DROP TABLE IF EXISTS sample;
DROP TABLE IF EXISTS project_member;
DROP TABLE IF EXISTS project_comment;
DROP TABLE IF EXISTS project;
DROP TABLE IF EXISTS participant_phenotypes;
DROP TABLE IF EXISTS participant_external_id;
DROP TABLE IF EXISTS participant_comment;
DROP TABLE IF EXISTS participant;
DROP TABLE IF EXISTS output_file;
DROP TABLE IF EXISTS group_member;
DROP TABLE IF EXISTS "group";
DROP TABLE IF EXISTS family_participant;
DROP TABLE IF EXISTS family_external_id;
DROP TABLE IF EXISTS family_comment;
DROP TABLE IF EXISTS family;
DROP TABLE IF EXISTS comment;
DROP TABLE IF EXISTS cohort_template;
DROP TABLE IF EXISTS cohort_sequencing_group;
DROP TABLE IF EXISTS cohort;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS assay_type;
DROP TABLE IF EXISTS assay_external_id;
DROP TABLE IF EXISTS assay_comment;
DROP TABLE IF EXISTS assay;
DROP TABLE IF EXISTS analysis_type;
DROP TABLE IF EXISTS analysis_sequencing_group;
DROP TABLE IF EXISTS analysis_runner;
DROP TABLE IF EXISTS analysis_outputs;
DROP TABLE IF EXISTS analysis_cohort;
DROP TABLE IF EXISTS analysis;

DROP TYPE IF EXISTS analysis_status;
DROP TYPE IF EXISTS project_member_role;
DROP TYPE IF EXISTS comment_status;
DROP TYPE IF EXISTS cohort_status;
