-- PLAN.md §11.1: three tables, 50 rows each, seeded deterministically. No foreign keys.
CREATE TABLE users (
  id         uuid PRIMARY KEY,
  email      text NOT NULL,
  name       text,
  created_at timestamptz NOT NULL,
  avatar     bytea
);

CREATE TABLE lists (
  id       bigint PRIMARY KEY,
  owner_id uuid NOT NULL,
  title    text NOT NULL,
  archived boolean NOT NULL DEFAULT false,
  settings jsonb
);

CREATE TABLE todos (
  list_id  bigint NOT NULL,
  position int    NOT NULL,
  title    text   NOT NULL,
  done     boolean NOT NULL,
  due      date,
  estimate interval,
  priority numeric(5,2),
  score    double precision,
  tags     text[],
  notes    text,
  PRIMARY KEY (list_id, position)
);

-- Large notes stored out of line without compression: exercises the unchanged-TOAST path.
ALTER TABLE todos ALTER COLUMN notes SET STORAGE EXTERNAL;

INSERT INTO users
SELECT md5('user' || i)::uuid,
       'user' || i || '@example.com',
       CASE i % 7
         WHEN 0 THEN NULL
         WHEN 1 THEN E'tab\there'
         WHEN 2 THEN E'new\nline and \r return'
         WHEN 3 THEN E'back\\slash \\N not null'
         WHEN 4 THEN 'unicode ✓ ünïcödé 🦭'
         WHEN 5 THEN '"quoted", {braces}, it''s'
         ELSE ''
       END,
       timestamptz '2024-01-01 00:00:00+00' + i * interval '1 hour 1.5 seconds',
       CASE WHEN i % 3 = 0 THEN NULL ELSE decode(md5(i::text), 'hex') END
FROM generate_series(1, 50) AS i;

INSERT INTO lists
SELECT i,
       md5('user' || (i % 50 + 1))::uuid,
       'list ' || i,
       i % 3 = 0,
       CASE WHEN i % 4 = 0 THEN NULL
            ELSE jsonb_build_object('color', 'c' || i, 'n', i, 'nested', jsonb_build_array(i, 'x"y'))
       END
FROM generate_series(1, 50) AS i;

INSERT INTO todos
SELECT (i % 10) + 1,
       i,
       'todo ' || i,
       i % 2 = 0,
       CASE WHEN i % 5 = 0 THEN NULL ELSE date '2024-01-01' + i END,
       CASE WHEN i % 6 = 0 THEN NULL ELSE make_interval(mins => i, secs => i * 1.5) END,
       (i * 1.25)::numeric(5,2),
       CASE i % 9 WHEN 0 THEN 'NaN'::float8 WHEN 1 THEN 'Infinity' WHEN 2 THEN '-Infinity'
                  ELSE i * 0.1 END,
       CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN '{}'::text[]
                  ELSE ARRAY['a', 'b c', 'q"uote', 'back\slash', NULL] END,
       CASE WHEN i % 5 = 1 THEN repeat('large note ' || i || ' ', 400) ELSE 'note ' || i END
FROM generate_series(1, 50) AS i;
