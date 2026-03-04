-- migrate:up

INSERT INTO "group" (name) VALUES ('project-creators'), ('members-admin');

-- migrate:down

DELETE FROM "group" WHERE name IN ('project-creators', 'members-admin');
