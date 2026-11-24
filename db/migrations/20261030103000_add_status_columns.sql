-- migrate:up

SET search_path TO main;

CREATE TYPE participant_status AS ENUM ('active', 'archived', 'on_hold', 'quarantined', 'deleted');
CREATE TYPE sample_status AS ENUM ('active', 'archived', 'on_hold', 'quarantined', 'deleted');
CREATE TYPE sequencing_group_status AS ENUM ('active', 'archived', 'on_hold', 'quarantined', 'deleted');
CREATE TYPE family_status AS ENUM ('active', 'archived', 'on_hold', 'quarantined', 'deleted');
CREATE TYPE assay_status AS ENUM ('active', 'archived', 'on_hold', 'quarantined', 'deleted');

-- FIXME These will all eventually be NOT NULL after we've migrated some data into them

ALTER TABLE participant ADD COLUMN status participant_status;

ALTER TABLE sample ADD COLUMN status sample_status;
ALTER TABLE sample DISABLE TRIGGER versioning_trigger;
UPDATE sample SET status = CASE WHEN active THEN 'active' ELSE 'archived' END::sample_status;
ALTER TABLE sample ENABLE TRIGGER versioning_trigger;
ALTER TABLE sample ALTER COLUMN status SET NOT NULL;

ALTER TABLE sequencing_group ADD COLUMN status sequencing_group_status;
ALTER TABLE family ADD COLUMN status family_status;
ALTER TABLE assay ADD COLUMN status assay_status;

SET search_path TO history, main;

ALTER TABLE participant_history ADD COLUMN status participant_status;
ALTER TABLE sample_history ADD COLUMN status sample_status;
ALTER TABLE sequencing_group_history ADD COLUMN status sequencing_group_status;
ALTER TABLE family_history ADD COLUMN status family_status;
ALTER TABLE assay_history ADD COLUMN status assay_status;


-- migrate:down

SET search_path TO history, main;

ALTER TABLE participant DROP COLUMN status;
ALTER TABLE sample DROP COLUMN status;
ALTER TABLE sequencing_group DROP COLUMN status;
ALTER TABLE family DROP COLUMN status;
ALTER TABLE assay DROP COLUMN status;

ALTER TABLE participant_history DROP COLUMN status;
ALTER TABLE sample_history DROP COLUMN status;
ALTER TABLE sequencing_group_history DROP COLUMN status;
ALTER TABLE family_history DROP COLUMN status;
ALTER TABLE assay_history DROP COLUMN status;

DROP TYPE IF EXISTS participant_status;
DROP TYPE IF EXISTS sample_status;
DROP TYPE IF EXISTS sequencing_group_status;
DROP TYPE IF EXISTS family_status;
DROP TYPE IF EXISTS assay_status;
