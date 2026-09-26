# Metamist Context

Metamist is a database and API for storing **de-identified -omics metadata**: the
participants, biological samples, sequencing, and downstream analyses involved in
population-genomics studies. This file fixes the domain vocabulary so code and
conversations use one word per concept.

## Language

### Core entities

**Project**:
The top-level organisational and access-control unit; every other entity belongs to
exactly one. Has a `name`, a `dataset`, and per-user `roles`.
_Avoid_: dataset (a Project *has* a dataset name, but the access-scoped entity is the Project), workspace, tenant

**Participant**:
A de-identified study subject (a person), carrying `external_ids`, reported sex/gender,
karyotype, and phenotypes.
_Avoid_: patient, subject, individual

**Family**:
A set of related Participants, used to express pedigree relationships.
_Avoid_: pedigree (that is the relationship data *within* a Family), kindred

**Sample**:
A biological specimen derived from a Participant. May be nested (a Sample can have a
parent/root Sample). Identified externally as a prefixed, checksummed string.
_Avoid_: specimen, biosample

**SequencingGroup**:
A group of sequences that are aligned and analysed together because they share the same
`type` + `technology` (e.g. genome + short-read). This — not the Sample — is the unit
that Analyses are keyed on. Members are immutable: changing them yields a new
SequencingGroup.
_Avoid_: sequence group, seqgroup, sequencing set, read group

**Assay**:
A wet-lab measurement performed on a Sample (e.g. a sequencing run), with a `type` and
free-form `meta`.
_Avoid_: sequence, sequencing (use Assay for the record, SequencingGroup for the analysis unit)

**Analysis**:
A computational result computed over SequencingGroups and/or Cohorts. Has a `type`,
a `status` (`queued` → `in-progress` → `completed`/`failed`/`unknown`), and `outputs`.
_Avoid_: job, run, result, pipeline output

**Cohort**:
A named, immutable set of SequencingGroups, instantiated from a CohortTemplate or from
selection criteria; has a `status` (`active`/`archived`/`invalid`).
_Avoid_: group, set, panel, batch

**Comment**:
A threaded annotation attached to one of a fixed set of entity types (Project, Family,
Sample, Assay, Participant, SequencingGroup). The thread of Comments on an entity is a
**Discussion**.
_Avoid_: note, annotation

**AnalysisRunner**:
A record of an analysis-runner invocation, identified by an `ar_guid`. (Provenance of
how an Analysis was produced; not the Analysis itself.)

**AuditLog**:
The record of who wrote a change and on whose behalf, attached to a Project.

### Identity & access

**External ID**:
A study-supplied identifier for an entity, stored as a dict keyed by **external org**
(the empty-string key `PRIMARY_EXTERNAL_ORG` is the primary org). Distinct from the
internal integer primary key.
_Avoid_: external_id (singular — entities carry a *map* of them, `external_ids`)

**Internal ID**:
The integer primary key used inside the database and internal models.

**Formatted ID**:
The external string form of an Internal ID: a prefix + the integer + a Luhn checksum
digit (Samples `XPGLCL…`, SequencingGroups `CPGLCL…`, Cohorts `COH…`). Prefixes are
environment-configurable.
_Avoid_: CPG ID, display ID (use Formatted ID; "CPG ID" leaks one specific prefix)

**ProjectMemberRole**:
A user's role within a Project: `reader`, `contributor`, `writer`, `project_admin`,
`project_member_admin`. **ReadAccessRoles** = {reader, contributor, writer};
**FullWriteAccessRoles** = {writer}.
_Avoid_: permission, scope, grant (the role is the unit; access is *derived* from roles)

### Model variants (naming convention)

Each entity is expressed as several Pydantic models with a consistent suffix convention.
Use these exact suffixes:

**Internal**:
The in-process representation (e.g. `SampleInternal`); carries Internal IDs.

**External** (unsuffixed transport model):
The API/transport representation (e.g. `Sample`); carries Formatted IDs. Produced by
`to_internal_model.to_external()`.

**Upsert** / **UpsertInternal**:
The write-path input models (e.g. `SampleUpsert`, `SampleUpsertInternal`).

**Nested**:
A variant embedding child entities (e.g. `NestedSequencingGroup` inside a Sample).

**GraphQL** (`GraphQLSample` etc.):
The Strawberry GraphQL representation, built via `from_internal()`.

### Code organisation (navigation terms)

**Layer** (`db/python/layers/`):
Business-logic module per entity; orchestrates Tables and is where authorization checks
currently live.

**Table** (`db/python/tables/`):
Data-access module per entity; owns SQL against the MariaDB schema.

**Filter** (`db/python/filters/`):
A typed query-criteria object built from `GenericFilter` primitives; passed into Tables
to constrain queries.

## Relationships

- A **Project** contains many **Participants**, **Samples**, **SequencingGroups**, **Analyses**, and **Cohorts**
- A **Participant** belongs to zero or more **Families** and has one or more **Samples**
- A **Sample** is derived from one **Participant** and has one or more **Assays** and **SequencingGroups**
- A **SequencingGroup** groups one or more **Assays** of matching type + technology
- An **Analysis** is computed over one or more **SequencingGroups** and/or **Cohorts**
- A **Cohort** is an immutable set of **SequencingGroups**
- A **Comment** attaches to exactly one entity (Project, Family, Sample, Assay, Participant, or SequencingGroup); its thread is a **Discussion**
- Every user holds zero or more **ProjectMemberRoles** per **Project**; read/write access is derived from those roles

## Example dialogue

> **Dev:** "If we re-run sequencing on a **Sample**, do we attach the new reads to the existing **SequencingGroup**?"
> **Domain expert:** "No — **SequencingGroup** members are immutable. New reads of a different type or technology form a *new* **SequencingGroup**. That's deliberate: an **Analysis** is keyed on the **SequencingGroup**, so reusing one would silently invalidate downstream results."
>
> **Dev:** "And the **Cohort** that **Analysis** ran over?"
> **Domain expert:** "Unaffected — a **Cohort** is a frozen set of **SequencingGroups**. To include the new one you build a new **Cohort**."

## Flagged ambiguities

- **"sequence" / "sequencing"** was used for three distinct things — resolved: the
  wet-lab record is an **Assay**, the analysis unit is a **SequencingGroup**, and a
  sequencing *run* is an Assay of sequencing type. Reserve "SequencingGroup" for the
  immutable analysis unit.
- **"ID"** was used for both the integer key and the prefixed string — resolved into
  **Internal ID** (int) vs **Formatted ID** (string with prefix + Luhn checksum).
- **"dataset" vs "Project"** — a Project *has* a `dataset` name, but the access-scoped
  domain entity is always the **Project**.
- **"external_id" (singular)** — entities carry a *map* of External IDs keyed by org
  (`external_ids`), not a single value.
