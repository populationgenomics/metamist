Writing: /dev/stdout
\restrict dbmate

-- Dumped from database version 18.3 (Debian 18.3-1.pgdg13+1)
-- Dumped by pg_dump version 18.3 (Debian 18.3-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: history; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA history;


--
-- Name: main; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA main;


--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA public;


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: analysis_status; Type: TYPE; Schema: main; Owner: -
--

CREATE TYPE main.analysis_status AS ENUM (
    'queued',
    'in-progress',
    'failed',
    'completed',
    'unknown'
);


--
-- Name: cohort_status; Type: TYPE; Schema: main; Owner: -
--

CREATE TYPE main.cohort_status AS ENUM (
    'active',
    'archived'
);


--
-- Name: comment_status; Type: TYPE; Schema: main; Owner: -
--

CREATE TYPE main.comment_status AS ENUM (
    'active',
    'deleted'
);


--
-- Name: project_member_role; Type: TYPE; Schema: main; Owner: -
--

CREATE TYPE main.project_member_role AS ENUM (
    'reader',
    'writer',
    'contributor',
    'project_admin',
    'project_member_admin'
);


--
-- Name: json_merge_patch(jsonb, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.json_merge_patch(target jsonb, patch jsonb) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
DECLARE
    result JSONB;
    key TEXT;
    patch_value JSONB;
    target_value JSONB;
BEGIN
    -- If patch is not an object, return the patch (replaces target entirely)
    IF jsonb_typeof(patch) != 'object' THEN
        RETURN patch;
    END IF;

    -- If patch is an object but target is not, start with empty object
    IF jsonb_typeof(target) != 'object' THEN
        target := '{}';
    END IF;

    -- Start with the target
    result := target;

    -- Iterate over each key in the patch
    FOR key IN SELECT jsonb_object_keys(patch)
    LOOP
        patch_value := patch -> key;

        -- If patch value is null, remove the key from result
        IF patch_value IS NULL OR jsonb_typeof(patch_value) = 'null' THEN
            result := result - key;
        ELSE
            -- Get the current target value for this key (may be null if key doesn't exist)
            target_value := result -> key;

            -- If target doesn't have this key, use null as the base
            IF target_value IS NULL THEN
                target_value := 'null'::jsonb;
            END IF;

            -- Recursively merge and set the result
            result := jsonb_set(result, ARRAY[key], json_merge_patch(target_value, patch_value));
        END IF;
    END LOOP;

    RETURN result;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: analysis_cohort_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.analysis_cohort_history (
    cohort_id integer CONSTRAINT analysis_cohort_cohort_id_not_null NOT NULL,
    analysis_id integer CONSTRAINT analysis_cohort_analysis_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT analysis_cohort_sys_period_not_null NOT NULL
);


--
-- Name: analysis_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.analysis_history (
    id integer CONSTRAINT analysis_id_not_null NOT NULL,
    type text,
    output text,
    status main.analysis_status,
    timestamp_completed timestamp with time zone,
    project integer CONSTRAINT analysis_project_not_null NOT NULL,
    author text,
    meta jsonb,
    active boolean,
    on_behalf_of text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT analysis_sys_period_not_null NOT NULL
);


--
-- Name: analysis_outputs_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.analysis_outputs_history (
    analysis_id integer CONSTRAINT analysis_outputs_analysis_id_not_null NOT NULL,
    file_id integer,
    output text,
    json_structure text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT analysis_outputs_sys_period_not_null NOT NULL
);


--
-- Name: analysis_runner_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.analysis_runner_history (
    ar_guid text CONSTRAINT analysis_runner_ar_guid_not_null NOT NULL,
    project integer CONSTRAINT analysis_runner_project_not_null NOT NULL,
    "timestamp" timestamp with time zone,
    access_level text CONSTRAINT analysis_runner_access_level_not_null NOT NULL,
    repository text CONSTRAINT analysis_runner_repository_not_null NOT NULL,
    commit text CONSTRAINT analysis_runner_commit_not_null NOT NULL,
    output_path text CONSTRAINT analysis_runner_output_path_not_null NOT NULL,
    script text,
    description text CONSTRAINT analysis_runner_description_not_null NOT NULL,
    driver_image text CONSTRAINT analysis_runner_driver_image_not_null NOT NULL,
    config_path text,
    cwd text,
    environment text CONSTRAINT analysis_runner_environment_not_null NOT NULL,
    hail_version text,
    batch_url text CONSTRAINT analysis_runner_batch_url_not_null NOT NULL,
    submitting_user text,
    meta jsonb,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT analysis_runner_sys_period_not_null NOT NULL
);


--
-- Name: analysis_sequencing_group_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.analysis_sequencing_group_history (
    analysis_id integer CONSTRAINT analysis_sequencing_group_analysis_id_not_null NOT NULL,
    sequencing_group_id integer CONSTRAINT analysis_sequencing_group_sequencing_group_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT analysis_sequencing_group_sys_period_not_null NOT NULL
);


--
-- Name: assay_comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.assay_comment_history (
    comment_id integer CONSTRAINT assay_comment_comment_id_not_null NOT NULL,
    assay_id integer CONSTRAINT assay_comment_assay_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT assay_comment_sys_period_not_null NOT NULL
);


--
-- Name: assay_external_id_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.assay_external_id_history (
    project integer CONSTRAINT assay_external_id_project_not_null NOT NULL,
    assay_id integer CONSTRAINT assay_external_id_assay_id_not_null NOT NULL,
    name text CONSTRAINT assay_external_id_name_not_null NOT NULL,
    external_id text CONSTRAINT assay_external_id_external_id_not_null NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT assay_external_id_sys_period_not_null NOT NULL
);


--
-- Name: assay_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.assay_history (
    id integer CONSTRAINT assay_id_not_null NOT NULL,
    sample_id integer CONSTRAINT assay_sample_id_not_null NOT NULL,
    type text CONSTRAINT assay_type_not_null NOT NULL,
    meta jsonb,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT assay_sys_period_not_null NOT NULL
);


--
-- Name: cohort_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.cohort_history (
    id integer CONSTRAINT cohort_id_not_null NOT NULL,
    project integer CONSTRAINT cohort_project_not_null NOT NULL,
    template_id integer,
    name text CONSTRAINT cohort_name_not_null NOT NULL,
    description text CONSTRAINT cohort_description_not_null NOT NULL,
    status main.cohort_status CONSTRAINT cohort_status_not_null NOT NULL,
    author text,
    "timestamp" timestamp with time zone CONSTRAINT cohort_timestamp_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT cohort_sys_period_not_null NOT NULL
);


--
-- Name: cohort_sequencing_group_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.cohort_sequencing_group_history (
    cohort_id integer CONSTRAINT cohort_sequencing_group_cohort_id_not_null NOT NULL,
    sequencing_group_id integer CONSTRAINT cohort_sequencing_group_sequencing_group_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT cohort_sequencing_group_sys_period_not_null NOT NULL
);


--
-- Name: cohort_template_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.cohort_template_history (
    id integer CONSTRAINT cohort_template_id_not_null NOT NULL,
    name text CONSTRAINT cohort_template_name_not_null NOT NULL,
    description text CONSTRAINT cohort_template_description_not_null NOT NULL,
    criteria jsonb CONSTRAINT cohort_template_criteria_not_null NOT NULL,
    project integer CONSTRAINT cohort_template_project_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT cohort_template_sys_period_not_null NOT NULL
);


--
-- Name: comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.comment_history (
    id integer CONSTRAINT comment_id_not_null NOT NULL,
    parent_id integer,
    content text,
    status main.comment_status CONSTRAINT comment_status_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT comment_sys_period_not_null NOT NULL
);


--
-- Name: family_comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.family_comment_history (
    comment_id integer CONSTRAINT family_comment_comment_id_not_null NOT NULL,
    family_id integer CONSTRAINT family_comment_family_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT family_comment_sys_period_not_null NOT NULL
);


--
-- Name: family_external_id_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.family_external_id_history (
    project integer CONSTRAINT family_external_id_project_not_null NOT NULL,
    family_id integer CONSTRAINT family_external_id_family_id_not_null NOT NULL,
    name text CONSTRAINT family_external_id_name_not_null NOT NULL,
    external_id text CONSTRAINT family_external_id_external_id_not_null NOT NULL,
    meta jsonb,
    audit_log_id integer CONSTRAINT family_external_id_audit_log_id_not_null NOT NULL,
    sys_period tstzrange CONSTRAINT family_external_id_sys_period_not_null NOT NULL
);


--
-- Name: family_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.family_history (
    id integer CONSTRAINT family_id_not_null NOT NULL,
    project integer CONSTRAINT family_project_not_null NOT NULL,
    description text,
    coded_phenotype text,
    author text,
    meta jsonb,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT family_sys_period_not_null NOT NULL
);


--
-- Name: family_participant_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.family_participant_history (
    family_id integer CONSTRAINT family_participant_family_id_not_null NOT NULL,
    participant_id integer CONSTRAINT family_participant_participant_id_not_null NOT NULL,
    paternal_participant_id integer,
    maternal_participant_id integer,
    affected integer,
    notes text,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT family_participant_sys_period_not_null NOT NULL
);


--
-- Name: group_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.group_history (
    id integer CONSTRAINT group_id_not_null NOT NULL,
    name text CONSTRAINT group_name_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT group_sys_period_not_null NOT NULL
);


--
-- Name: group_member_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.group_member_history (
    group_id integer CONSTRAINT group_member_group_id_not_null NOT NULL,
    member text CONSTRAINT group_member_member_not_null NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT group_member_sys_period_not_null NOT NULL
);


--
-- Name: output_file_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.output_file_history (
    id integer CONSTRAINT output_file_id_not_null NOT NULL,
    path text CONSTRAINT output_file_path_not_null NOT NULL,
    basename text CONSTRAINT output_file_basename_not_null NOT NULL,
    dirname text CONSTRAINT output_file_dirname_not_null NOT NULL,
    nameroot text CONSTRAINT output_file_nameroot_not_null NOT NULL,
    nameext text,
    file_checksum text,
    size bigint CONSTRAINT output_file_size_not_null NOT NULL,
    meta jsonb,
    valid boolean,
    parent_id integer,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT output_file_sys_period_not_null NOT NULL
);


--
-- Name: participant_comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.participant_comment_history (
    comment_id integer CONSTRAINT participant_comment_comment_id_not_null NOT NULL,
    participant_id integer CONSTRAINT participant_comment_participant_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT participant_comment_sys_period_not_null NOT NULL
);


--
-- Name: participant_external_id_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.participant_external_id_history (
    project integer CONSTRAINT participant_external_id_project_not_null NOT NULL,
    participant_id integer CONSTRAINT participant_external_id_participant_id_not_null NOT NULL,
    name text CONSTRAINT participant_external_id_name_not_null NOT NULL,
    external_id text CONSTRAINT participant_external_id_external_id_not_null NOT NULL,
    meta jsonb,
    audit_log_id integer CONSTRAINT participant_external_id_audit_log_id_not_null NOT NULL,
    sys_period tstzrange CONSTRAINT participant_external_id_sys_period_not_null NOT NULL
);


--
-- Name: participant_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.participant_history (
    id integer CONSTRAINT participant_id_not_null NOT NULL,
    project integer CONSTRAINT participant_project_not_null NOT NULL,
    author text,
    reported_sex integer,
    reported_gender text,
    karyotype text,
    meta jsonb,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT participant_sys_period_not_null NOT NULL
);


--
-- Name: participant_phenotypes_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.participant_phenotypes_history (
    participant_id integer CONSTRAINT participant_phenotypes_participant_id_not_null NOT NULL,
    hpo_term text,
    description text,
    author text,
    value jsonb,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT participant_phenotypes_sys_period_not_null NOT NULL
);


--
-- Name: project_comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.project_comment_history (
    comment_id integer CONSTRAINT project_comment_comment_id_not_null NOT NULL,
    project_id integer CONSTRAINT project_comment_project_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT project_comment_sys_period_not_null NOT NULL
);


--
-- Name: project_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.project_history (
    id integer CONSTRAINT project_id_not_null NOT NULL,
    name character varying(64) CONSTRAINT project_name_not_null NOT NULL,
    dataset text,
    meta jsonb,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT project_sys_period_not_null NOT NULL
);


--
-- Name: project_member_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.project_member_history (
    project_id integer CONSTRAINT project_member_project_id_not_null NOT NULL,
    member text CONSTRAINT project_member_member_not_null NOT NULL,
    role main.project_member_role CONSTRAINT project_member_role_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT project_member_sys_period_not_null NOT NULL
);


--
-- Name: sample_comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sample_comment_history (
    comment_id integer CONSTRAINT sample_comment_comment_id_not_null NOT NULL,
    sample_id integer CONSTRAINT sample_comment_sample_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT sample_comment_sys_period_not_null NOT NULL
);


--
-- Name: sample_external_id_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sample_external_id_history (
    project integer CONSTRAINT sample_external_id_project_not_null NOT NULL,
    sample_id integer CONSTRAINT sample_external_id_sample_id_not_null NOT NULL,
    name text CONSTRAINT sample_external_id_name_not_null NOT NULL,
    external_id text CONSTRAINT sample_external_id_external_id_not_null NOT NULL,
    meta jsonb,
    audit_log_id integer CONSTRAINT sample_external_id_audit_log_id_not_null NOT NULL,
    sys_period tstzrange CONSTRAINT sample_external_id_sys_period_not_null NOT NULL
);


--
-- Name: sample_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sample_history (
    id integer CONSTRAINT sample_id_not_null NOT NULL,
    project integer CONSTRAINT sample_project_not_null NOT NULL,
    participant_id integer,
    active boolean,
    meta jsonb,
    type text,
    sample_root_id integer,
    sample_parent_id integer,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT sample_sys_period_not_null NOT NULL
);


--
-- Name: sequencing_group_assay_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sequencing_group_assay_history (
    sequencing_group_id integer CONSTRAINT sequencing_group_assay_sequencing_group_id_not_null NOT NULL,
    assay_id integer CONSTRAINT sequencing_group_assay_assay_id_not_null NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT sequencing_group_assay_sys_period_not_null NOT NULL
);


--
-- Name: sequencing_group_comment_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sequencing_group_comment_history (
    comment_id integer CONSTRAINT sequencing_group_comment_comment_id_not_null NOT NULL,
    sequencing_group_id integer CONSTRAINT sequencing_group_comment_sequencing_group_id_not_null NOT NULL,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT sequencing_group_comment_sys_period_not_null NOT NULL
);


--
-- Name: sequencing_group_external_id_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sequencing_group_external_id_history (
    project integer CONSTRAINT sequencing_group_external_id_project_not_null NOT NULL,
    sequencing_group_id integer CONSTRAINT sequencing_group_external_id_sequencing_group_id_not_null NOT NULL,
    external_id text CONSTRAINT sequencing_group_external_id_external_id_not_null NOT NULL,
    name text CONSTRAINT sequencing_group_external_id_name_not_null NOT NULL,
    author text,
    null_if_archived smallint,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT sequencing_group_external_id_sys_period_not_null NOT NULL
);


--
-- Name: sequencing_group_history; Type: TABLE; Schema: history; Owner: -
--

CREATE TABLE history.sequencing_group_history (
    id integer CONSTRAINT sequencing_group_id_not_null NOT NULL,
    sample_id integer CONSTRAINT sequencing_group_sample_id_not_null NOT NULL,
    type text CONSTRAINT sequencing_group_type_not_null NOT NULL,
    technology text CONSTRAINT sequencing_group_technology_not_null NOT NULL,
    platform text,
    meta jsonb,
    archived boolean,
    author text,
    audit_log_id integer,
    sys_period tstzrange CONSTRAINT sequencing_group_sys_period_not_null NOT NULL
);


--
-- Name: analysis; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.analysis (
    id integer NOT NULL,
    type text,
    output text,
    status main.analysis_status,
    timestamp_completed timestamp with time zone,
    project integer NOT NULL,
    author text,
    meta jsonb DEFAULT '{}'::jsonb,
    active boolean DEFAULT true,
    on_behalf_of text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: analysis_cohort; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.analysis_cohort (
    cohort_id integer NOT NULL,
    analysis_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: analysis_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.analysis ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.analysis_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: analysis_outputs; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.analysis_outputs (
    analysis_id integer NOT NULL,
    file_id integer,
    output text,
    json_structure text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL,
    CONSTRAINT chk_file_id_output CHECK ((((file_id IS NOT NULL) AND (output IS NULL)) OR ((file_id IS NULL) AND (output IS NOT NULL))))
);


--
-- Name: analysis_runner; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.analysis_runner (
    ar_guid text NOT NULL,
    project integer NOT NULL,
    "timestamp" timestamp with time zone,
    access_level text NOT NULL,
    repository text NOT NULL,
    commit text NOT NULL,
    output_path text NOT NULL,
    script text,
    description text NOT NULL,
    driver_image text NOT NULL,
    config_path text,
    cwd text,
    environment text NOT NULL,
    hail_version text,
    batch_url text NOT NULL,
    submitting_user text,
    meta jsonb DEFAULT '{}'::jsonb,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: analysis_sequencing_group; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.analysis_sequencing_group (
    analysis_id integer NOT NULL,
    sequencing_group_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: analysis_type; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.analysis_type (
    id text NOT NULL,
    name text NOT NULL,
    audit_log_id integer
);


--
-- Name: assay; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.assay (
    id integer NOT NULL,
    sample_id integer NOT NULL,
    type text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: assay_comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.assay_comment (
    comment_id integer NOT NULL,
    assay_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: assay_external_id; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.assay_external_id (
    project integer NOT NULL,
    assay_id integer NOT NULL,
    name text NOT NULL,
    external_id text NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: assay_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.assay ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.assay_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: assay_type; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.assay_type (
    id text NOT NULL,
    name text NOT NULL,
    audit_log_id integer
);


--
-- Name: audit_log; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.audit_log (
    id integer NOT NULL,
    "timestamp" timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    author text NOT NULL,
    on_behalf_of text,
    ar_guid text,
    comment text,
    auth_project integer,
    meta jsonb DEFAULT '{}'::jsonb
);


--
-- Name: audit_log_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.audit_log ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.audit_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: cohort; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.cohort (
    id integer NOT NULL,
    project integer NOT NULL,
    template_id integer,
    name text NOT NULL,
    description text NOT NULL,
    status main.cohort_status DEFAULT 'active'::main.cohort_status NOT NULL,
    author text,
    "timestamp" timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: COLUMN cohort.template_id; Type: COMMENT; Schema: main; Owner: -
--

COMMENT ON COLUMN main.cohort.template_id IS 'Following user feedback session, we are renaming the derived_from column to template_id to better reflect its purpose.';


--
-- Name: cohort_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.cohort ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.cohort_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: cohort_sequencing_group; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.cohort_sequencing_group (
    cohort_id integer NOT NULL,
    sequencing_group_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: cohort_template; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.cohort_template (
    id integer NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    criteria jsonb NOT NULL,
    project integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: cohort_template_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.cohort_template ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.cohort_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.comment (
    id integer NOT NULL,
    parent_id integer,
    content text,
    status main.comment_status NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: comment_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.comment ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.comment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: family; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.family (
    id integer NOT NULL,
    project integer NOT NULL,
    description text,
    coded_phenotype text,
    author text,
    meta jsonb DEFAULT '{}'::jsonb,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: family_comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.family_comment (
    comment_id integer NOT NULL,
    family_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: family_external_id; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.family_external_id (
    project integer NOT NULL,
    family_id integer NOT NULL,
    name text NOT NULL,
    external_id text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    audit_log_id integer NOT NULL,
    sys_period tstzrange NOT NULL
);


--
-- Name: family_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.family ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.family_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: family_participant; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.family_participant (
    family_id integer NOT NULL,
    participant_id integer NOT NULL,
    paternal_participant_id integer,
    maternal_participant_id integer,
    affected integer,
    notes text,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: group; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main."group" (
    id integer NOT NULL,
    name text NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: group_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main."group" ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: group_member; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.group_member (
    group_id integer NOT NULL,
    member text NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: output_file; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.output_file (
    id integer NOT NULL,
    path text NOT NULL,
    basename text NOT NULL,
    dirname text NOT NULL,
    nameroot text NOT NULL,
    nameext text,
    file_checksum text,
    size bigint NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    valid boolean,
    parent_id integer,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: output_file_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.output_file ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.output_file_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: participant; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.participant (
    id integer NOT NULL,
    project integer NOT NULL,
    author text,
    reported_sex integer,
    reported_gender text,
    karyotype text,
    meta jsonb DEFAULT '{}'::jsonb,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: participant_comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.participant_comment (
    comment_id integer NOT NULL,
    participant_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: participant_external_id; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.participant_external_id (
    project integer NOT NULL,
    participant_id integer NOT NULL,
    name text NOT NULL,
    external_id text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    audit_log_id integer NOT NULL,
    sys_period tstzrange NOT NULL
);


--
-- Name: participant_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.participant ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.participant_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: participant_phenotypes; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.participant_phenotypes (
    participant_id integer NOT NULL,
    hpo_term text,
    description text,
    author text,
    value jsonb,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: project; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.project (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    dataset text,
    meta jsonb DEFAULT '{}'::jsonb,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: project_comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.project_comment (
    comment_id integer NOT NULL,
    project_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: project_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.project ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.project_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: project_member; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.project_member (
    project_id integer NOT NULL,
    member text NOT NULL,
    role main.project_member_role NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: sample; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sample (
    id integer NOT NULL,
    project integer NOT NULL,
    participant_id integer,
    active boolean NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    type text,
    sample_root_id integer,
    sample_parent_id integer,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: sample_comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sample_comment (
    comment_id integer NOT NULL,
    sample_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: sample_external_id; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sample_external_id (
    project integer NOT NULL,
    sample_id integer NOT NULL,
    name text NOT NULL,
    external_id text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    audit_log_id integer NOT NULL,
    sys_period tstzrange NOT NULL
);


--
-- Name: sample_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.sample ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.sample_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sample_type; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sample_type (
    id text NOT NULL,
    name text NOT NULL,
    audit_log_id integer
);


--
-- Name: schema_migrations; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: sequencing_group; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_group (
    id integer NOT NULL,
    sample_id integer NOT NULL,
    type text NOT NULL,
    technology text NOT NULL,
    platform text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb,
    archived boolean NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: sequencing_group_assay; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_group_assay (
    sequencing_group_id integer NOT NULL,
    assay_id integer NOT NULL,
    author text,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: sequencing_group_comment; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_group_comment (
    comment_id integer NOT NULL,
    sequencing_group_id integer NOT NULL,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: sequencing_group_external_id; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_group_external_id (
    project integer NOT NULL,
    sequencing_group_id integer NOT NULL,
    external_id text NOT NULL,
    name text NOT NULL,
    author text,
    null_if_archived smallint DEFAULT 1,
    audit_log_id integer,
    sys_period tstzrange NOT NULL
);


--
-- Name: COLUMN sequencing_group_external_id.null_if_archived; Type: COMMENT; Schema: main; Owner: -
--

COMMENT ON COLUMN main.sequencing_group_external_id.null_if_archived IS 'Setting this to NULL will allow the external id to be reused within a project if sgs are archived';


--
-- Name: sequencing_group_id_seq; Type: SEQUENCE; Schema: main; Owner: -
--

ALTER TABLE main.sequencing_group ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME main.sequencing_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sequencing_platform; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_platform (
    id text NOT NULL,
    name text NOT NULL,
    audit_log_id integer
);


--
-- Name: sequencing_technology; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_technology (
    id text NOT NULL,
    name text NOT NULL,
    audit_log_id integer
);


--
-- Name: sequencing_type; Type: TABLE; Schema: main; Owner: -
--

CREATE TABLE main.sequencing_type (
    id text NOT NULL,
    name text NOT NULL,
    audit_log_id integer
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: analysis analysis_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis
    ADD CONSTRAINT analysis_pkey PRIMARY KEY (id);


--
-- Name: analysis_runner analysis_runner_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_runner
    ADD CONSTRAINT analysis_runner_pkey PRIMARY KEY (ar_guid);


--
-- Name: analysis_type analysis_type_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_type
    ADD CONSTRAINT analysis_type_pkey PRIMARY KEY (id);


--
-- Name: assay_comment assay_comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_comment
    ADD CONSTRAINT assay_comment_pkey PRIMARY KEY (comment_id);


--
-- Name: assay_external_id assay_external_id_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_external_id
    ADD CONSTRAINT assay_external_id_pkey PRIMARY KEY (assay_id, name);


--
-- Name: assay assay_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay
    ADD CONSTRAINT assay_pkey PRIMARY KEY (id);


--
-- Name: assay_type assay_type_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_type
    ADD CONSTRAINT assay_type_pkey PRIMARY KEY (id);


--
-- Name: audit_log audit_log_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);


--
-- Name: cohort cohort_name_key; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort
    ADD CONSTRAINT cohort_name_key UNIQUE (name);


--
-- Name: cohort cohort_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort
    ADD CONSTRAINT cohort_pkey PRIMARY KEY (id);


--
-- Name: cohort_template cohort_template_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort_template
    ADD CONSTRAINT cohort_template_pkey PRIMARY KEY (id);


--
-- Name: comment comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.comment
    ADD CONSTRAINT comment_pkey PRIMARY KEY (id);


--
-- Name: family_comment family_comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_comment
    ADD CONSTRAINT family_comment_pkey PRIMARY KEY (comment_id);


--
-- Name: family_external_id family_external_id_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_external_id
    ADD CONSTRAINT family_external_id_pkey PRIMARY KEY (family_id, name);


--
-- Name: family_participant family_participant_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_participant
    ADD CONSTRAINT family_participant_pkey PRIMARY KEY (participant_id);


--
-- Name: family family_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family
    ADD CONSTRAINT family_pkey PRIMARY KEY (id);


--
-- Name: group group_name_key; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main."group"
    ADD CONSTRAINT group_name_key UNIQUE (name);


--
-- Name: group group_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main."group"
    ADD CONSTRAINT group_pkey PRIMARY KEY (id);


--
-- Name: output_file output_file_path_key; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.output_file
    ADD CONSTRAINT output_file_path_key UNIQUE (path);


--
-- Name: output_file output_file_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.output_file
    ADD CONSTRAINT output_file_pkey PRIMARY KEY (id);


--
-- Name: participant_comment participant_comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_comment
    ADD CONSTRAINT participant_comment_pkey PRIMARY KEY (comment_id);


--
-- Name: participant_external_id participant_external_id_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_external_id
    ADD CONSTRAINT participant_external_id_pkey PRIMARY KEY (participant_id, name);


--
-- Name: participant participant_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant
    ADD CONSTRAINT participant_pkey PRIMARY KEY (id);


--
-- Name: project_comment project_comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_comment
    ADD CONSTRAINT project_comment_pkey PRIMARY KEY (comment_id);


--
-- Name: project_member project_member_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_member
    ADD CONSTRAINT project_member_pkey PRIMARY KEY (project_id, member, role);


--
-- Name: project project_name_key; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project
    ADD CONSTRAINT project_name_key UNIQUE (name);


--
-- Name: project project_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project
    ADD CONSTRAINT project_pkey PRIMARY KEY (id);


--
-- Name: sample_comment sample_comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_comment
    ADD CONSTRAINT sample_comment_pkey PRIMARY KEY (comment_id);


--
-- Name: sample_external_id sample_external_id_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_external_id
    ADD CONSTRAINT sample_external_id_pkey PRIMARY KEY (sample_id, name);


--
-- Name: sample sample_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT sample_pkey PRIMARY KEY (id);


--
-- Name: sample_type sample_type_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_type
    ADD CONSTRAINT sample_type_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sequencing_group_comment sequencing_group_comment_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_comment
    ADD CONSTRAINT sequencing_group_comment_pkey PRIMARY KEY (comment_id);


--
-- Name: sequencing_group_external_id sequencing_group_external_id_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_external_id
    ADD CONSTRAINT sequencing_group_external_id_pkey PRIMARY KEY (sequencing_group_id, name);


--
-- Name: sequencing_group sequencing_group_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group
    ADD CONSTRAINT sequencing_group_pkey PRIMARY KEY (id);


--
-- Name: sequencing_platform sequencing_platform_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_platform
    ADD CONSTRAINT sequencing_platform_pkey PRIMARY KEY (id);


--
-- Name: sequencing_technology sequencing_technology_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_technology
    ADD CONSTRAINT sequencing_technology_pkey PRIMARY KEY (id);


--
-- Name: sequencing_type sequencing_type_pkey; Type: CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_type
    ADD CONSTRAINT sequencing_type_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: idx_analysis_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_audit_log_id ON main.analysis USING btree (audit_log_id);


--
-- Name: idx_analysis_cohort_analysis_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_cohort_analysis_id ON main.analysis_cohort USING btree (analysis_id);


--
-- Name: idx_analysis_cohort_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_cohort_audit_log_id ON main.analysis_cohort USING btree (audit_log_id);


--
-- Name: idx_analysis_cohort_cohort_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_cohort_cohort_id ON main.analysis_cohort USING btree (cohort_id);


--
-- Name: idx_analysis_outputs_analysis_file; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_analysis_outputs_analysis_file ON main.analysis_outputs USING btree (analysis_id, file_id, COALESCE(json_structure, ''::text));


--
-- Name: idx_analysis_outputs_analysis_output; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_analysis_outputs_analysis_output ON main.analysis_outputs USING btree (analysis_id, output, COALESCE(json_structure, ''::text));


--
-- Name: idx_analysis_outputs_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_outputs_audit_log_id ON main.analysis_outputs USING btree (audit_log_id);


--
-- Name: idx_analysis_outputs_file_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_outputs_file_id ON main.analysis_outputs USING btree (file_id);


--
-- Name: idx_analysis_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_project ON main.analysis USING btree (project);


--
-- Name: idx_analysis_runner_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_runner_audit_log_id ON main.analysis_runner USING btree (audit_log_id);


--
-- Name: idx_analysis_runner_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_runner_project ON main.analysis_runner USING btree (project);


--
-- Name: idx_analysis_runner_submitting_user; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_runner_submitting_user ON main.analysis_runner USING btree (submitting_user);


--
-- Name: idx_analysis_sequencing_group_analysis_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_sequencing_group_analysis_id ON main.analysis_sequencing_group USING btree (analysis_id);


--
-- Name: idx_analysis_sequencing_group_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_sequencing_group_audit_log_id ON main.analysis_sequencing_group USING btree (audit_log_id);


--
-- Name: idx_analysis_sequencing_group_sequencing_group_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_sequencing_group_sequencing_group_id ON main.analysis_sequencing_group USING btree (sequencing_group_id);


--
-- Name: idx_analysis_type; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_analysis_type ON main.analysis USING btree (type);


--
-- Name: idx_assay_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_assay_audit_log_id ON main.assay USING btree (audit_log_id);


--
-- Name: idx_assay_comment_assay_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_assay_comment_assay_id ON main.assay_comment USING btree (assay_id);


--
-- Name: idx_assay_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_assay_comment_audit_log_id ON main.assay_comment USING btree (audit_log_id);


--
-- Name: idx_assay_external_id_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_assay_external_id_audit_log_id ON main.assay_external_id USING btree (audit_log_id);


--
-- Name: idx_assay_external_id_project_external_id; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_assay_external_id_project_external_id ON main.assay_external_id USING btree (project, external_id);


--
-- Name: idx_assay_sample_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_assay_sample_id ON main.assay USING btree (sample_id);


--
-- Name: idx_assay_type; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_assay_type ON main.assay USING btree (type);


--
-- Name: idx_audit_log_auth_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_audit_log_auth_project ON main.audit_log USING btree (auth_project);


--
-- Name: idx_cohort_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_audit_log_id ON main.cohort USING btree (audit_log_id);


--
-- Name: idx_cohort_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_project ON main.cohort USING btree (project);


--
-- Name: idx_cohort_sequencing_group_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_sequencing_group_audit_log_id ON main.cohort_sequencing_group USING btree (audit_log_id);


--
-- Name: idx_cohort_sequencing_group_cohort_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_sequencing_group_cohort_id ON main.cohort_sequencing_group USING btree (cohort_id);


--
-- Name: idx_cohort_sequencing_group_sequencing_group_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_sequencing_group_sequencing_group_id ON main.cohort_sequencing_group USING btree (sequencing_group_id);


--
-- Name: idx_cohort_template_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_template_audit_log_id ON main.cohort_template USING btree (audit_log_id);


--
-- Name: idx_cohort_template_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_cohort_template_project ON main.cohort_template USING btree (project);


--
-- Name: idx_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_comment_audit_log_id ON main.comment USING btree (audit_log_id);


--
-- Name: idx_comment_parent_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_comment_parent_id ON main.comment USING btree (parent_id);


--
-- Name: idx_family_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_audit_log_id ON main.family USING btree (audit_log_id);


--
-- Name: idx_family_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_comment_audit_log_id ON main.family_comment USING btree (audit_log_id);


--
-- Name: idx_family_comment_family_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_comment_family_id ON main.family_comment USING btree (family_id);


--
-- Name: idx_family_external_id_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_external_id_audit_log_id ON main.family_external_id USING btree (audit_log_id);


--
-- Name: idx_family_external_id_project_external_id; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_family_external_id_project_external_id ON main.family_external_id USING btree (project, external_id);


--
-- Name: idx_family_participant_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_participant_audit_log_id ON main.family_participant USING btree (audit_log_id);


--
-- Name: idx_family_participant_family_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_participant_family_id ON main.family_participant USING btree (family_id);


--
-- Name: idx_family_participant_maternal_participant_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_participant_maternal_participant_id ON main.family_participant USING btree (maternal_participant_id);


--
-- Name: idx_family_participant_paternal_participant_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_participant_paternal_participant_id ON main.family_participant USING btree (paternal_participant_id);


--
-- Name: idx_family_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_family_project ON main.family USING btree (project);


--
-- Name: idx_group_member_group_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_group_member_group_id ON main.group_member USING btree (group_id);


--
-- Name: idx_output_file_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_output_file_audit_log_id ON main.output_file USING btree (audit_log_id);


--
-- Name: idx_output_file_parent_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_output_file_parent_id ON main.output_file USING btree (parent_id);


--
-- Name: idx_participant_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_participant_audit_log_id ON main.participant USING btree (audit_log_id);


--
-- Name: idx_participant_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_participant_comment_audit_log_id ON main.participant_comment USING btree (audit_log_id);


--
-- Name: idx_participant_comment_participant_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_participant_comment_participant_id ON main.participant_comment USING btree (participant_id);


--
-- Name: idx_participant_external_id_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_participant_external_id_audit_log_id ON main.participant_external_id USING btree (audit_log_id);


--
-- Name: idx_participant_external_id_project_external_id; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_participant_external_id_project_external_id ON main.participant_external_id USING btree (project, external_id);


--
-- Name: idx_participant_phenotypes_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_participant_phenotypes_audit_log_id ON main.participant_phenotypes USING btree (audit_log_id);


--
-- Name: idx_participant_phenotypes_pid_hpo_description; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_participant_phenotypes_pid_hpo_description ON main.participant_phenotypes USING btree (participant_id, hpo_term, description);


--
-- Name: idx_participant_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_participant_project ON main.participant USING btree (project);


--
-- Name: idx_project_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_project_comment_audit_log_id ON main.project_comment USING btree (audit_log_id);


--
-- Name: idx_project_comment_project_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_project_comment_project_id ON main.project_comment USING btree (project_id);


--
-- Name: idx_project_member_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_project_member_audit_log_id ON main.project_member USING btree (audit_log_id);


--
-- Name: idx_project_member_member; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_project_member_member ON main.project_member USING btree (member);


--
-- Name: idx_sample_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_audit_log_id ON main.sample USING btree (audit_log_id);


--
-- Name: idx_sample_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_comment_audit_log_id ON main.sample_comment USING btree (audit_log_id);


--
-- Name: idx_sample_comment_sample_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_comment_sample_id ON main.sample_comment USING btree (sample_id);


--
-- Name: idx_sample_external_id_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_external_id_audit_log_id ON main.sample_external_id USING btree (audit_log_id);


--
-- Name: idx_sample_external_id_project_external_id; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_sample_external_id_project_external_id ON main.sample_external_id USING btree (project, external_id);


--
-- Name: idx_sample_parent_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_parent_id ON main.sample USING btree (sample_parent_id);


--
-- Name: idx_sample_participant_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_participant_id ON main.sample USING btree (participant_id);


--
-- Name: idx_sample_project; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_project ON main.sample USING btree (project);


--
-- Name: idx_sample_root_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_root_id ON main.sample USING btree (sample_root_id);


--
-- Name: idx_sample_type; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sample_type ON main.sample USING btree (type);


--
-- Name: idx_sequencing_group_assay_assay_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_assay_assay_id ON main.sequencing_group_assay USING btree (assay_id);


--
-- Name: idx_sequencing_group_assay_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_assay_audit_log_id ON main.sequencing_group_assay USING btree (audit_log_id);


--
-- Name: idx_sequencing_group_assay_sequencing_group_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_assay_sequencing_group_id ON main.sequencing_group_assay USING btree (sequencing_group_id);


--
-- Name: idx_sequencing_group_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_audit_log_id ON main.sequencing_group USING btree (audit_log_id);


--
-- Name: idx_sequencing_group_comment_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_comment_audit_log_id ON main.sequencing_group_comment USING btree (audit_log_id);


--
-- Name: idx_sequencing_group_comment_sequencing_group_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_comment_sequencing_group_id ON main.sequencing_group_comment USING btree (sequencing_group_id);


--
-- Name: idx_sequencing_group_external_id_audit_log_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_external_id_audit_log_id ON main.sequencing_group_external_id USING btree (audit_log_id);


--
-- Name: idx_sequencing_group_external_id_sequencing_group_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_external_id_sequencing_group_id ON main.sequencing_group_external_id USING btree (sequencing_group_id);


--
-- Name: idx_sequencing_group_external_id_unique_idx; Type: INDEX; Schema: main; Owner: -
--

CREATE UNIQUE INDEX idx_sequencing_group_external_id_unique_idx ON main.sequencing_group_external_id USING btree (project, external_id, null_if_archived);


--
-- Name: idx_sequencing_group_sample_id; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_sample_id ON main.sequencing_group USING btree (sample_id);


--
-- Name: idx_sequencing_group_technology; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_technology ON main.sequencing_group USING btree (technology);


--
-- Name: idx_sequencing_group_type; Type: INDEX; Schema: main; Owner: -
--

CREATE INDEX idx_sequencing_group_type ON main.sequencing_group USING btree (type);


--
-- Name: analysis versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.analysis FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.analysis_history', 'true');


--
-- Name: analysis_cohort versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.analysis_cohort FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.analysis_cohort_history', 'true');


--
-- Name: analysis_outputs versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.analysis_outputs FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.analysis_outputs_history', 'true');


--
-- Name: analysis_runner versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.analysis_runner FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.analysis_runner_history', 'true');


--
-- Name: analysis_sequencing_group versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.analysis_sequencing_group FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.analysis_sequencing_group_history', 'true');


--
-- Name: assay versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.assay FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.assay_history', 'true');


--
-- Name: assay_comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.assay_comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.assay_comment_history', 'true');


--
-- Name: assay_external_id versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.assay_external_id FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.assay_external_id_history', 'true');


--
-- Name: cohort versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.cohort FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.cohort_history', 'true');


--
-- Name: cohort_sequencing_group versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.cohort_sequencing_group FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.cohort_sequencing_group_history', 'true');


--
-- Name: cohort_template versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.cohort_template FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.cohort_template_history', 'true');


--
-- Name: comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.comment_history', 'true');


--
-- Name: family versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.family FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.family_history', 'true');


--
-- Name: family_comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.family_comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.family_comment_history', 'true');


--
-- Name: family_external_id versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.family_external_id FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.family_external_id_history', 'true');


--
-- Name: family_participant versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.family_participant FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.family_participant_history', 'true');


--
-- Name: group versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main."group" FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.group_history', 'true');


--
-- Name: group_member versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.group_member FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.group_member_history', 'true');


--
-- Name: output_file versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.output_file FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.output_file_history', 'true');


--
-- Name: participant versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.participant FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.participant_history', 'true');


--
-- Name: participant_comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.participant_comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.participant_comment_history', 'true');


--
-- Name: participant_external_id versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.participant_external_id FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.participant_external_id_history', 'true');


--
-- Name: participant_phenotypes versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.participant_phenotypes FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.participant_phenotypes_history', 'true');


--
-- Name: project versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.project FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.project_history', 'true');


--
-- Name: project_comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.project_comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.project_comment_history', 'true');


--
-- Name: project_member versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.project_member FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.project_member_history', 'true');


--
-- Name: sample versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sample FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sample_history', 'true');


--
-- Name: sample_comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sample_comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sample_comment_history', 'true');


--
-- Name: sample_external_id versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sample_external_id FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sample_external_id_history', 'true');


--
-- Name: sequencing_group versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sequencing_group FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sequencing_group_history', 'true');


--
-- Name: sequencing_group_assay versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sequencing_group_assay FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sequencing_group_assay_history', 'true');


--
-- Name: sequencing_group_comment versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sequencing_group_comment FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sequencing_group_comment_history', 'true');


--
-- Name: sequencing_group_external_id versioning_trigger; Type: TRIGGER; Schema: main; Owner: -
--

CREATE TRIGGER versioning_trigger BEFORE INSERT OR DELETE OR UPDATE ON main.sequencing_group_external_id FOR EACH ROW EXECUTE FUNCTION public.versioning('sys_period', 'history.sequencing_group_external_id_history', 'true');


--
-- Name: analysis fk_analysis_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis
    ADD CONSTRAINT fk_analysis_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: analysis_cohort fk_analysis_cohort_analysis_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_cohort
    ADD CONSTRAINT fk_analysis_cohort_analysis_id FOREIGN KEY (analysis_id) REFERENCES main.analysis(id);


--
-- Name: analysis_cohort fk_analysis_cohort_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_cohort
    ADD CONSTRAINT fk_analysis_cohort_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: analysis_cohort fk_analysis_cohort_cohort_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_cohort
    ADD CONSTRAINT fk_analysis_cohort_cohort_id FOREIGN KEY (cohort_id) REFERENCES main.cohort(id);


--
-- Name: analysis_outputs fk_analysis_outputs_analysis_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_outputs
    ADD CONSTRAINT fk_analysis_outputs_analysis_id FOREIGN KEY (analysis_id) REFERENCES main.analysis(id);


--
-- Name: analysis_outputs fk_analysis_outputs_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_outputs
    ADD CONSTRAINT fk_analysis_outputs_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: analysis_outputs fk_analysis_outputs_file_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_outputs
    ADD CONSTRAINT fk_analysis_outputs_file_id FOREIGN KEY (file_id) REFERENCES main.output_file(id) ON DELETE CASCADE;


--
-- Name: analysis fk_analysis_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis
    ADD CONSTRAINT fk_analysis_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: analysis_runner fk_analysis_runner_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_runner
    ADD CONSTRAINT fk_analysis_runner_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: analysis_runner fk_analysis_runner_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_runner
    ADD CONSTRAINT fk_analysis_runner_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: analysis_sequencing_group fk_analysis_sequencing_group_analysis_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_sequencing_group
    ADD CONSTRAINT fk_analysis_sequencing_group_analysis_id FOREIGN KEY (analysis_id) REFERENCES main.analysis(id);


--
-- Name: analysis_sequencing_group fk_analysis_sequencing_group_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_sequencing_group
    ADD CONSTRAINT fk_analysis_sequencing_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: analysis_sequencing_group fk_analysis_sequencing_group_sequencing_group_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_sequencing_group
    ADD CONSTRAINT fk_analysis_sequencing_group_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES main.sequencing_group(id);


--
-- Name: analysis fk_analysis_type; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis
    ADD CONSTRAINT fk_analysis_type FOREIGN KEY (type) REFERENCES main.analysis_type(id);


--
-- Name: analysis_type fk_analysis_type_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.analysis_type
    ADD CONSTRAINT fk_analysis_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: assay fk_assay_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay
    ADD CONSTRAINT fk_assay_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: assay_comment fk_assay_comment_assay_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_comment
    ADD CONSTRAINT fk_assay_comment_assay_id FOREIGN KEY (assay_id) REFERENCES main.assay(id);


--
-- Name: assay_comment fk_assay_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_comment
    ADD CONSTRAINT fk_assay_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: assay_comment fk_assay_comment_comment_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_comment
    ADD CONSTRAINT fk_assay_comment_comment_id FOREIGN KEY (comment_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: assay_external_id fk_assay_external_id_assay_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_external_id
    ADD CONSTRAINT fk_assay_external_id_assay_id FOREIGN KEY (assay_id) REFERENCES main.assay(id);


--
-- Name: assay_external_id fk_assay_external_id_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_external_id
    ADD CONSTRAINT fk_assay_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: assay_external_id fk_assay_external_id_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_external_id
    ADD CONSTRAINT fk_assay_external_id_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: assay fk_assay_sample_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay
    ADD CONSTRAINT fk_assay_sample_id FOREIGN KEY (sample_id) REFERENCES main.sample(id);


--
-- Name: assay fk_assay_type; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay
    ADD CONSTRAINT fk_assay_type FOREIGN KEY (type) REFERENCES main.assay_type(id);


--
-- Name: assay_type fk_assay_type_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.assay_type
    ADD CONSTRAINT fk_assay_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: audit_log fk_audit_log_auth_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.audit_log
    ADD CONSTRAINT fk_audit_log_auth_project FOREIGN KEY (auth_project) REFERENCES main.project(id);


--
-- Name: cohort fk_cohort_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort
    ADD CONSTRAINT fk_cohort_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: cohort fk_cohort_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort
    ADD CONSTRAINT fk_cohort_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: cohort_sequencing_group fk_cohort_sequencing_group_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort_sequencing_group
    ADD CONSTRAINT fk_cohort_sequencing_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: cohort_sequencing_group fk_cohort_sequencing_group_cohort_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort_sequencing_group
    ADD CONSTRAINT fk_cohort_sequencing_group_cohort_id FOREIGN KEY (cohort_id) REFERENCES main.cohort(id);


--
-- Name: cohort_sequencing_group fk_cohort_sequencing_group_sequencing_group_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort_sequencing_group
    ADD CONSTRAINT fk_cohort_sequencing_group_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES main.sequencing_group(id);


--
-- Name: cohort_template fk_cohort_template_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort_template
    ADD CONSTRAINT fk_cohort_template_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: cohort fk_cohort_template_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort
    ADD CONSTRAINT fk_cohort_template_id FOREIGN KEY (template_id) REFERENCES main.cohort_template(id);


--
-- Name: cohort_template fk_cohort_template_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.cohort_template
    ADD CONSTRAINT fk_cohort_template_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: comment fk_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.comment
    ADD CONSTRAINT fk_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: comment fk_comment_parent_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.comment
    ADD CONSTRAINT fk_comment_parent_id FOREIGN KEY (parent_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: family fk_family_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family
    ADD CONSTRAINT fk_family_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: family_comment fk_family_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_comment
    ADD CONSTRAINT fk_family_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: family_comment fk_family_comment_comment_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_comment
    ADD CONSTRAINT fk_family_comment_comment_id FOREIGN KEY (comment_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: family_comment fk_family_comment_family_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_comment
    ADD CONSTRAINT fk_family_comment_family_id FOREIGN KEY (family_id) REFERENCES main.family(id);


--
-- Name: family_external_id fk_family_external_id_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_external_id
    ADD CONSTRAINT fk_family_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: family_external_id fk_family_external_id_family_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_external_id
    ADD CONSTRAINT fk_family_external_id_family_id FOREIGN KEY (family_id) REFERENCES main.family(id);


--
-- Name: family_external_id fk_family_external_id_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_external_id
    ADD CONSTRAINT fk_family_external_id_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: family_participant fk_family_participant_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_participant
    ADD CONSTRAINT fk_family_participant_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: family_participant fk_family_participant_family_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_participant
    ADD CONSTRAINT fk_family_participant_family_id FOREIGN KEY (family_id) REFERENCES main.family(id);


--
-- Name: family_participant fk_family_participant_maternal_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_participant
    ADD CONSTRAINT fk_family_participant_maternal_participant_id FOREIGN KEY (maternal_participant_id) REFERENCES main.participant(id);


--
-- Name: family_participant fk_family_participant_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_participant
    ADD CONSTRAINT fk_family_participant_participant_id FOREIGN KEY (participant_id) REFERENCES main.participant(id);


--
-- Name: family_participant fk_family_participant_paternal_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family_participant
    ADD CONSTRAINT fk_family_participant_paternal_participant_id FOREIGN KEY (paternal_participant_id) REFERENCES main.participant(id);


--
-- Name: family fk_family_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.family
    ADD CONSTRAINT fk_family_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: group fk_group_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main."group"
    ADD CONSTRAINT fk_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: group_member fk_group_member_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.group_member
    ADD CONSTRAINT fk_group_member_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: group_member fk_group_member_group_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.group_member
    ADD CONSTRAINT fk_group_member_group_id FOREIGN KEY (group_id) REFERENCES main."group"(id);


--
-- Name: output_file fk_output_file_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.output_file
    ADD CONSTRAINT fk_output_file_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: output_file fk_output_file_parent_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.output_file
    ADD CONSTRAINT fk_output_file_parent_id FOREIGN KEY (parent_id) REFERENCES main.output_file(id) ON DELETE SET NULL;


--
-- Name: participant fk_participant_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant
    ADD CONSTRAINT fk_participant_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: participant_comment fk_participant_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_comment
    ADD CONSTRAINT fk_participant_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: participant_comment fk_participant_comment_comment_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_comment
    ADD CONSTRAINT fk_participant_comment_comment_id FOREIGN KEY (comment_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: participant_comment fk_participant_comment_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_comment
    ADD CONSTRAINT fk_participant_comment_participant_id FOREIGN KEY (participant_id) REFERENCES main.participant(id);


--
-- Name: participant_external_id fk_participant_external_id_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_external_id
    ADD CONSTRAINT fk_participant_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: participant_external_id fk_participant_external_id_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_external_id
    ADD CONSTRAINT fk_participant_external_id_participant_id FOREIGN KEY (participant_id) REFERENCES main.participant(id);


--
-- Name: participant_external_id fk_participant_external_id_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_external_id
    ADD CONSTRAINT fk_participant_external_id_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: participant_phenotypes fk_participant_phenotypes_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_phenotypes
    ADD CONSTRAINT fk_participant_phenotypes_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: participant_phenotypes fk_participant_phenotypes_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant_phenotypes
    ADD CONSTRAINT fk_participant_phenotypes_participant_id FOREIGN KEY (participant_id) REFERENCES main.participant(id);


--
-- Name: participant fk_participant_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.participant
    ADD CONSTRAINT fk_participant_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: project fk_project_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project
    ADD CONSTRAINT fk_project_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: project_comment fk_project_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_comment
    ADD CONSTRAINT fk_project_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: project_comment fk_project_comment_comment_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_comment
    ADD CONSTRAINT fk_project_comment_comment_id FOREIGN KEY (comment_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: project_comment fk_project_comment_project_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_comment
    ADD CONSTRAINT fk_project_comment_project_id FOREIGN KEY (project_id) REFERENCES main.project(id);


--
-- Name: project_member fk_project_member_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_member
    ADD CONSTRAINT fk_project_member_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: project_member fk_project_member_project_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.project_member
    ADD CONSTRAINT fk_project_member_project_id FOREIGN KEY (project_id) REFERENCES main.project(id);


--
-- Name: sample fk_sample_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT fk_sample_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sample_comment fk_sample_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_comment
    ADD CONSTRAINT fk_sample_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sample_comment fk_sample_comment_comment_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_comment
    ADD CONSTRAINT fk_sample_comment_comment_id FOREIGN KEY (comment_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: sample_comment fk_sample_comment_sample_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_comment
    ADD CONSTRAINT fk_sample_comment_sample_id FOREIGN KEY (sample_id) REFERENCES main.sample(id);


--
-- Name: sample_external_id fk_sample_external_id_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_external_id
    ADD CONSTRAINT fk_sample_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sample_external_id fk_sample_external_id_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_external_id
    ADD CONSTRAINT fk_sample_external_id_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: sample_external_id fk_sample_external_id_sample_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_external_id
    ADD CONSTRAINT fk_sample_external_id_sample_id FOREIGN KEY (sample_id) REFERENCES main.sample(id);


--
-- Name: sample fk_sample_parent_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT fk_sample_parent_id FOREIGN KEY (sample_parent_id) REFERENCES main.sample(id) ON DELETE CASCADE;


--
-- Name: sample fk_sample_participant_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT fk_sample_participant_id FOREIGN KEY (participant_id) REFERENCES main.participant(id);


--
-- Name: sample fk_sample_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT fk_sample_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: sample fk_sample_root_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT fk_sample_root_id FOREIGN KEY (sample_root_id) REFERENCES main.sample(id) ON DELETE CASCADE;


--
-- Name: sample fk_sample_type; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample
    ADD CONSTRAINT fk_sample_type FOREIGN KEY (type) REFERENCES main.sample_type(id);


--
-- Name: sample_type fk_sample_type_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sample_type
    ADD CONSTRAINT fk_sample_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_group_assay fk_sequencing_group_assay_assay_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_assay
    ADD CONSTRAINT fk_sequencing_group_assay_assay_id FOREIGN KEY (assay_id) REFERENCES main.assay(id);


--
-- Name: sequencing_group_assay fk_sequencing_group_assay_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_assay
    ADD CONSTRAINT fk_sequencing_group_assay_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_group_assay fk_sequencing_group_assay_sequencing_group_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_assay
    ADD CONSTRAINT fk_sequencing_group_assay_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES main.sequencing_group(id);


--
-- Name: sequencing_group fk_sequencing_group_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group
    ADD CONSTRAINT fk_sequencing_group_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_group_comment fk_sequencing_group_comment_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_comment
    ADD CONSTRAINT fk_sequencing_group_comment_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_group_comment fk_sequencing_group_comment_comment_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_comment
    ADD CONSTRAINT fk_sequencing_group_comment_comment_id FOREIGN KEY (comment_id) REFERENCES main.comment(id) ON DELETE CASCADE;


--
-- Name: sequencing_group_comment fk_sequencing_group_comment_sequencing_group_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_comment
    ADD CONSTRAINT fk_sequencing_group_comment_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES main.sequencing_group(id);


--
-- Name: sequencing_group_external_id fk_sequencing_group_external_id_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_external_id
    ADD CONSTRAINT fk_sequencing_group_external_id_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_group_external_id fk_sequencing_group_external_id_project; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_external_id
    ADD CONSTRAINT fk_sequencing_group_external_id_project FOREIGN KEY (project) REFERENCES main.project(id);


--
-- Name: sequencing_group_external_id fk_sequencing_group_external_id_sequencing_group_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group_external_id
    ADD CONSTRAINT fk_sequencing_group_external_id_sequencing_group_id FOREIGN KEY (sequencing_group_id) REFERENCES main.sequencing_group(id);


--
-- Name: sequencing_group fk_sequencing_group_sample_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group
    ADD CONSTRAINT fk_sequencing_group_sample_id FOREIGN KEY (sample_id) REFERENCES main.sample(id);


--
-- Name: sequencing_group fk_sequencing_group_technology; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group
    ADD CONSTRAINT fk_sequencing_group_technology FOREIGN KEY (technology) REFERENCES main.sequencing_technology(id);


--
-- Name: sequencing_group fk_sequencing_group_type; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_group
    ADD CONSTRAINT fk_sequencing_group_type FOREIGN KEY (type) REFERENCES main.sequencing_type(id);


--
-- Name: sequencing_platform fk_sequencing_platform_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_platform
    ADD CONSTRAINT fk_sequencing_platform_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_technology fk_sequencing_technology_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_technology
    ADD CONSTRAINT fk_sequencing_technology_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- Name: sequencing_type fk_sequencing_type_audit_log_id; Type: FK CONSTRAINT; Schema: main; Owner: -
--

ALTER TABLE ONLY main.sequencing_type
    ADD CONSTRAINT fk_sequencing_type_audit_log_id FOREIGN KEY (audit_log_id) REFERENCES main.audit_log(id);


--
-- PostgreSQL database dump complete
--

\unrestrict dbmate


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20260110061420'),
    ('20260120061420'),
    ('20260120061443'),
    ('20260120061532'),
    ('20260120061600'),
    ('20260120061616'),
    ('20260120061617'),
    ('20260129010059'),
    ('20260212055045'),
    ('20260804021835');
