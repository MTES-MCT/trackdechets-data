-- Prod dabatabase
CREATE DATABASE pg_trackdechets_production
ENGINE = PostgreSQL('host.docker.internal:10001', 'database_name', 'postgres', 'mysecretpassword', 'data_eng',0);

CREATE DATABASE pg_trackdechets_gerico
ENGINE = PostgreSQL('host.docker.internal:00003', 'database_name', 'postgres', 'mysecretpassword', 'public',0);