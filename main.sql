 CREATE TABLE "objects" (
	"data" JSONB NOT NULL
		CHECK (("data" ? '_key'))
)

CREATE INDEX IF NOT EXISTS "idx__objects__key__score" ON "objects"(("data"->>'_key') ASC, (("data"->>'score')::numeric) DESC)

CREATE UNIQUE INDEX IF NOT EXISTS "uniq__objects__key"
ON "objects" ( (("data"->>'_key')) )
WHERE NOT ("data" ? 'score');

CREATE UNIQUE INDEX IF NOT EXISTS "uniq__objects__key__value"
ON "objects" ( (data->>'_key') ASC, (data->>'value') DESC );


CREATE INDEX IF NOT EXISTS "idx__objects__expireAt"
ON "objects" ( (CAST(data->>'expireAt' AS numeric)) ASC )
WHERE data ? 'expireAt';



CREATE TYPE LEGACY_OBJECT_TYPE AS ENUM (
    'hash',
    'zset',
    'set',
    'list',
    'string'
);

CREATE TABLE "legacy_object" (
	"_key" TEXT NOT NULL,
	"type" LEGACY_OBJECT_TYPE NOT NULL,
	"expireAt" TIMESTAMPTZ
		DEFAULT NULL
)

INSERT INTO "legacy_object" ("_key", "type", "expireAt")
SELECT "data"->>'_key', 'zset'::LEGACY_OBJECT_TYPE, MIN(CASE
	WHEN ("data" ? 'expireAt') THEN to_timestamp(("data"->>'expireAt')::double precision / 1000)
	ELSE NULL
END)
  FROM "objects"
 WHERE ("data" ? 'score')
   AND ("data"->>'value' IS NOT NULL)
   AND ("data"->>'score' IS NOT NULL)
 GROUP BY "data"->>'_key'

INSERT INTO "legacy_object" ("_key", "type", "expireAt")
SELECT "data"->>'_key', CASE
	WHEN (SELECT COUNT(*) FROM jsonb_object_keys("data" - 'expireAt')) = 2 THEN CASE
		WHEN ("data" ? 'value') OR ("data" ? 'data') THEN 'string'
		WHEN "data" ? 'array' THEN 'list'
		WHEN "data" ? 'members' THEN 'set'
		ELSE 'hash'
	END
	ELSE 'hash'
END::LEGACY_OBJECT_TYPE, CASE
	WHEN ("data" ? 'expireAt') THEN to_timestamp(("data"->>'expireAt')::double precision / 1000)
	ELSE NULL
END
  FROM "objects"
 WHERE NOT ("data" ? 'score')
   AND ("data"->>'_key') NOT LIKE '_imported_%:%'

ALTER TABLE "legacy_object"
	ADD PRIMARY KEY ( "_key" )

ALTER TABLE "legacy_object"
	ADD UNIQUE ( "_key", "type" )

CREATE INDEX "idx__legacy_object__expireAt" ON "legacy_object"("expireAt" ASC)

CREATE INDEX "idx__legacy_object__type" ON "legacy_object"("type")

CREATE TABLE "legacy_hash" (
	"_key" TEXT NOT NULL,
	"data" JSONB NOT NULL,
	"type" LEGACY_OBJECT_TYPE NOT NULL
		DEFAULT 'hash'::LEGACY_OBJECT_TYPE
		CHECK ( "type" = 'hash' )
)

CREATE TABLE "legacy_zset" (
	"_key" TEXT NOT NULL,
	"value" TEXT NOT NULL,
	"score" NUMERIC NOT NULL,
	"type" LEGACY_OBJECT_TYPE NOT NULL
		DEFAULT 'zset'::LEGACY_OBJECT_TYPE
		CHECK ( "type" = 'zset' )
)

CREATE TABLE "legacy_set" (
	"_key" TEXT NOT NULL,
	"member" TEXT NOT NULL,
	"type" LEGACY_OBJECT_TYPE NOT NULL
		DEFAULT 'set'::LEGACY_OBJECT_TYPE
		CHECK ( "type" = 'set' )
)

CREATE TABLE "legacy_list" (
	"_key" TEXT NOT NULL,
	"array" TEXT[] NOT NULL,
	"type" LEGACY_OBJECT_TYPE NOT NULL
		DEFAULT 'list'::LEGACY_OBJECT_TYPE
		CHECK ( "type" = 'list' )
)

CREATE TABLE "legacy_string" (
	"_key" TEXT NOT NULL,
	"data" TEXT NOT NULL,
	"type" LEGACY_OBJECT_TYPE NOT NULL
		DEFAULT 'string'::LEGACY_OBJECT_TYPE
		CHECK ( "type" = 'string' )
)

DROP TYPE IF EXISTS LEGACY_IMPORTED_TYPE;

CREATE TYPE LEGACY_IMPORTED_TYPE AS ENUM ( 'bookmark', 'category', 'favourite', 'group', 'message', 'post', 'room', 'topic', 'user', 'vote' )

CREATE TABLE "legacy_imported" (
	"type" LEGACY_IMPORTED_TYPE NOT NULL,
	"id" BIGINT NOT NULL,
	"data" JSONB NOT NULL
)

INSERT INTO "legacy_hash" ("_key", "data")
SELECT l."_key", o."data" - '_key' - 'expireAt'
  FROM "legacy_object" l
 INNER JOIN "objects" o
         ON l."_key" = o."data"->>'_key'
 WHERE l."type" = 'hash'

INSERT INTO "legacy_zset" ("_key", "value", "score")
SELECT l."_key", o."data"->>'value', (o."data"->>'score')::numeric
  FROM "legacy_object" l
 INNER JOIN "objects" o
         ON l."_key" = o."data"->>'_key'
 WHERE l."type" = 'zset'
   AND o."data"->>'value' IS NOT NULL
   AND o."data"->>'score' IS NOT NULL

INSERT INTO "legacy_set" ("_key", "member")
SELECT l."_key", jsonb_array_elements_text(o."data"->'members')
  FROM "legacy_object" l
 INNER JOIN "objects" o
         ON l."_key" = o."data"->>'_key'
 WHERE l."type" = 'set'

INSERT INTO "legacy_list" ("_key", "array")
SELECT l."_key", ARRAY(SELECT a.t FROM jsonb_array_elements_text(o."data"->'list') WITH ORDINALITY a(t, i) ORDER BY a.i ASC)
  FROM "legacy_object" l
 INNER JOIN "objects" o
         ON l."_key" = o."data"->>'_key'
 WHERE l."type" = 'list'

INSERT INTO "legacy_string" ("_key", "data")
SELECT l."_key", CASE
	WHEN o."data" ? 'value' THEN o."data"->>'value'
	ELSE o."data"->>'data'
END
  FROM "legacy_object" l
 INNER JOIN "objects" o
         ON l."_key" = o."data"->>'_key'
 WHERE l."type" = 'string'

INSERT INTO "legacy_imported" ("type", "id", "data")
SELECT (regexp_matches(o."data"->>'_key', '^_imported_(.*):'))[1]::LEGACY_IMPORTED_TYPE,
       (regexp_matches(o."data"->>'_key', ':(.*)$'))[1]::BIGINT,
       o."data" - '_key'
  FROM "objects" o
 WHERE (o."data"->>'_key') LIKE '_imported_%:%'
 
 
 CREATE VIEW "legacy_object_live" AS
SELECT "_key", "type"
  FROM "legacy_object"
 WHERE "expireAt" IS NULL
    OR "expireAt" > CURRENT_TIMESTAMP


ALTER TABLE "legacy_hash"
	ADD PRIMARY KEY ("_key")

ALTER TABLE "legacy_hash"
	ADD CONSTRAINT "fk__legacy_hash__key"
	FOREIGN KEY ("_key", "type")
	REFERENCES "legacy_object"("_key", "type")
	ON UPDATE CASCADE
	ON DELETE CASCADE

ALTER TABLE "legacy_zset"
	ADD PRIMARY KEY ("_key", "value")

ALTER TABLE "legacy_zset"
	ADD CONSTRAINT "fk__legacy_zset__key"
	FOREIGN KEY ("_key", "type")
	REFERENCES "legacy_object"("_key", "type")
	ON UPDATE CASCADE
	ON DELETE CASCADE


CREATE INDEX "idx__legacy_zset__key__score" ON "legacy_zset"("_key" ASC, "score" DESC)

ALTER TABLE "legacy_set"
	ADD PRIMARY KEY ("_key", "member")

ALTER TABLE "legacy_set"
	ADD CONSTRAINT "fk__legacy_set__key"
	FOREIGN KEY ("_key", "type")
	REFERENCES "legacy_object"("_key", "type")
	ON UPDATE CASCADE
	ON DELETE CASCADE

ALTER TABLE "legacy_list"
	ADD PRIMARY KEY ("_key")

ALTER TABLE "legacy_list"
	ADD CONSTRAINT "fk__legacy_list__key"
	FOREIGN KEY ("_key", "type")
	REFERENCES "legacy_object"("_key", "type")
	ON UPDATE CASCADE
	ON DELETE CASCADE

ALTER TABLE "legacy_string"
	ADD PRIMARY KEY ("_key")


ALTER TABLE "legacy_string"
	ADD CONSTRAINT "fk__legacy_string__key"
	FOREIGN KEY ("_key", "type")
	REFERENCES "legacy_object"("_key", "type")
	ON UPDATE CASCADE
	ON DELETE CASCADE

ALTER TABLE "legacy_imported"
	ADD PRIMARY KEY ("type", "id")

DROP TABLE "objects" cascade

DROP INDEX "idx__legacy_object__type"

ALTER TABLE "legacy_object" CLUSTER ON "legacy_object_pkey";
ALTER TABLE "legacy_hash" CLUSTER ON "legacy_hash_pkey";
ALTER TABLE "legacy_zset" CLUSTER ON "legacy_zset_pkey";
ALTER TABLE "legacy_set" CLUSTER ON "legacy_set_pkey";
ALTER TABLE "legacy_list" CLUSTER ON "legacy_list_pkey";
ALTER TABLE "legacy_string" CLUSTER ON "legacy_string_pkey";
ALTER TABLE "legacy_imported" CLUSTER ON "legacy_imported_pkey"


CLUSTER VERBOSE


ANALYZE VERBOSE "legacy_object"

ANALYZE VERBOSE "legacy_hash"

ANALYZE VERBOSE "legacy_zset"

ANALYZE VERBOSE "legacy_set"

ANALYZE VERBOSE "legacy_list"

ANALYZE VERBOSE "legacy_string"

ANALYZE VERBOSE "legacy_imported"


CREATE TABLE IF NOT EXISTS "session" (
	"sid" VARCHAR NOT NULL
		COLLATE "default",
	"sess" JSON NOT NULL,
	"expire" TIMESTAMP(6) NOT NULL
) WITH (OIDS=FALSE)


ALTER TABLE "session"
	ADD CONSTRAINT "session_pkey"
	PRIMARY KEY ("sid")
	NOT DEFERRABLE
	INITIALLY IMMEDIATE


ANALYZE VERBOSE "session"

