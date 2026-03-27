-- migrate:up

-- Create Basic Roles & Assign Memberships

-- This role allows connecting to the metamist DB
CREATE ROLE metamist_connect NOLOGIN;
GRANT CONNECT ON DATABASE metamist TO metamist_connect;


-- This is a login role, but we can't put the password in the migrations
-- so create it as NOLOGIN and then it'll be manually switched to a login role with a password
CREATE ROLE metamist_server NOLOGIN;

-- This role allows developers read only access to the database
CREATE ROLE developer_read_only NOLOGIN;

-- Give app, and devs access to connect to the database
GRANT metamist_connect to metamist_server, developer_read_only;

-- Setup schemas and set default table permissions

CREATE SCHEMA IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS history;

GRANT USAGE ON SCHEMA main TO metamist_connect;
GRANT USAGE ON SCHEMA history TO metamist_connect;
GRANT USAGE ON SCHEMA public TO metamist_connect;

-- Default permission for developer read only account
-- This will ensure that any new tables created in the main or history schema
-- will automatically have select permissions for the developer read only role
ALTER DEFAULT PRIVILEGES FOR ROLE metamist_superuser IN SCHEMA main, history
    GRANT SELECT ON TABLES TO developer_read_only;
ALTER DEFAULT PRIVILEGES FOR ROLE metamist_superuser IN SCHEMA main, history
    GRANT SELECT ON SEQUENCES TO developer_read_only;

-- Default permissions for app user
-- This will ensure that any new tables created in the main or history schema
-- will automatically have select, insert, update, delete permissions for the app role
ALTER DEFAULT PRIVILEGES FOR ROLE metamist_superuser IN SCHEMA main, history
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO metamist_server;
ALTER DEFAULT PRIVILEGES FOR ROLE metamist_superuser IN SCHEMA main, history
    GRANT USAGE, SELECT ON SEQUENCES TO metamist_server;

-- migrate:down

DROP SCHEMA IF EXISTS history CASCADE;
DROP SCHEMA IF EXISTS main CASCADE;

REVOKE CONNECT ON DATABASE metamist FROM metamist_connect;

DROP OWNED BY metamist_connect;
DROP OWNED BY metamist_server;
DROP OWNED BY developer_read_only;

DROP ROLE IF EXISTS developer_read_only;
DROP ROLE IF EXISTS metamist_connect;
DROP ROLE IF EXISTS metamist_server;
