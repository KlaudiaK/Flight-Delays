DROP DATABASE IF EXISTS "delays_db";
CREATE DATABASE "delays_db" WITH ENCODING 'UTF8';
DROP USER IF EXISTS "user";
CREATE USER "user" WITH PASSWORD 'password';
ALTER DATABASE "delays_db" OWNER TO "user";
ALTER SCHEMA public owner to "user";
GRANT ALL PRIVILEGES ON DATABASE "delays_db" TO "user";
GRANT USAGE ON SCHEMA public TO "user";
GRANT CREATE ON SCHEMA public TO "user";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "user";

\c "delays_db";
DROP TABLE IF EXISTS delays;
CREATE TABLE IF NOT EXISTS delays (
    day TIMESTAMP NOT NULL,
    state VARCHAR(200) NOT NULL,
    departure_count INTEGER NOT NULL,
    total_departure_delay INTEGER NOT NULL,
    arrival_count INTEGER NOT NULL,
    total_arrival_delay INTEGER NOT NULL,
    PRIMARY KEY (day, state)
    );
GRANT ALL PRIVILEGES ON TABLE delays TO "user";
ALTER TABLE delays OWNER TO "user";
