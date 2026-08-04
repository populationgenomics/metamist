-- migrate:up

SET search_path TO main;

-- These columns are never null in practice, and a check of the db
-- shows that they always have values. We can be stricter in the db 
-- which allows us to be more confident with types in the api
ALTER TABLE sample ALTER COLUMN active SET NOT NULL;
ALTER TABLE sequencing_group ALTER COLUMN platform SET NOT NULL;
ALTER TABLE sequencing_group ALTER COLUMN archived SET NOT NULL;

-- migrate:down

SET search_path TO main;

ALTER TABLE sample ALTER COLUMN active DROP NOT NULL;
ALTER TABLE sequencing_group ALTER COLUMN platform DROP NOT NULL;
ALTER TABLE sequencing_group ALTER COLUMN archived DROP NOT NULL;
