CREATE SCHEMA IF NOT EXISTS public;

-- TRUNCATE TABLE IF EXISTS public.event_forms;

DROP TABLE IF EXISTS  public.event_forms;

CREATE TABLE IF NOT EXISTS public.event_forms (
id serial NOT NULL PRIMARY KEY,
form_id text NOT NULL,
event_happened_at timestamp NOT NULL,
event_type text NOT NULL,
payload json);
