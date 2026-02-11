-- migrate:up
ALTER TABLE main.family
    ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;


ALTER TABLE history.family_history
    ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;



-- migrate:down
ALTER TABLE main.family DROP COLUMN meta;

ALTER TABLE history.family_history DROP COLUMN meta;