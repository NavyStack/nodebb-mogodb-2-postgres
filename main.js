import fs from "node:fs/promises";
import pkg from "pg";
import { MongoClient } from "mongodb";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
const { Pool: PgPool } = pkg;
dotenv.config();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const pgConnectionString = process.env.PG_CONNECTION_STRING ||
    (() => {
        const pgHost = process.env.PG_HOST;
        const pgPort = process.env.PG_PORT;
        const pgDatabase = process.env.PG_DATABASE;
        const pgUser = process.env.PG_USER;
        const pgPassword = process.env.PG_PASSWORD;
        return `postgres://${pgUser}:${pgPassword}@${pgHost}:${pgPort}/${pgDatabase}`;
    })();
const pgPool = new PgPool({ connectionString: pgConnectionString });
const mongoUri = process.env.MONGO_URI ||
    (() => {
        const mongoHost = process.env.MONGO_HOST;
        const mongoDatabase = process.env.MONGO_DATABASE;
        const mongoUser = process.env.MONGO_USER;
        const mongoPassword = process.env.MONGO_PASSWORD;
        return `mongodb+srv://${mongoUser}:${mongoPassword}@${mongoHost}/${mongoDatabase}?retryWrites=true&w=majority`;
    })();
const dbName = process.env.MONGO_DATABASE;
const collectionName = "objects";
async function exportFromMongoDB() {
    const client = new MongoClient(mongoUri);
    try {
        await client.connect();
        console.log("Connected to MongoDB");
        const db = client.db(dbName);
        const collection = db.collection(collectionName);
        const cursor = collection.find();
        const jsonData = [];
        for await (const doc of cursor) {
            jsonData.push(doc);
        }
        const jsonFilePath = path.join(__dirname, "data.json");
        await fs.writeFile(jsonFilePath, JSON.stringify(jsonData, null, 2));
        console.log(`Data successfully exported to ${jsonFilePath}`);
    }
    catch (error) {
        if (error instanceof Error) {
            console.error("Error exporting data:", error.message);
        }
        else {
            console.error("An unknown error occurred");
        }
    }
    finally {
        await client.close();
        console.log("MongoDB client closed.");
    }
}
async function executeQuery(query, successMessage, errorMessage) {
    try {
        await pgPool.query(query);
        console.log(successMessage);
    }
    catch (error) {
        if (error instanceof Error) {
            console.error(`${errorMessage}: ${error.message}`);
        }
        else {
            console.error(`${errorMessage}: An unknown error occurred`);
        }
    }
}
async function loadData() {
    try {
        console.log("Connected to PostgreSQL database.");
        await executeQuery(`CREATE TABLE "objects" (
        "data" JSONB NOT NULL
          CHECK (("data" ? '_key'))
      )`, "Table created or already exists.", "Failed to create table");
        const data = await fs.readFile(path.join(__dirname, "data.json"), "utf-8");
        const jsonData = JSON.parse(data);
        const insertPromises = jsonData.map((item) => pgPool.query('INSERT INTO "objects" (data) VALUES ($1)', [item]));
        await Promise.all(insertPromises);
        console.log("Data has been successfully ingested into the database.");
        const queries = [
            {
                query: ` CREATE INDEX IF NOT EXISTS "idx__objects__key__score" ON "objects"(("data"->>'_key') ASC, (("data"->>'score')::numeric) DESC);`,
                successMessage: "Create index on key__score",
                errorMessage: "Failed to Create index on key__score"
            },
            {
                query: `CREATE UNIQUE INDEX IF NOT EXISTS "uniq__objects__key" ON "objects"(("data"->>'_key')) WHERE NOT ("data" ? 'score');`,
                successMessage: "Create unique index on key",
                errorMessage: "Failed Create unique index on key"
            },
            {
                query: `CREATE UNIQUE INDEX IF NOT EXISTS "uniq__objects__key__value" ON "objects"(("data"->>'_key') ASC, ("data"->>'value') DESC) `,
                successMessage: "Create unique index on key__value",
                errorMessage: "Failed to Create unique index on key__value"
            },
            {
                query: ` CREATE INDEX IF NOT EXISTS "idx__objects__expireAt" ON "objects"((("data"->>'expireAt')::numeric) ASC) WHERE "data" ? 'expireAt';`,
                successMessage: "Create index on expireAt",
                errorMessage: "Failed to Create index on expireAt"
            },
            {
                query: `CREATE TYPE LEGACY_IMPORTED_TYPE AS ENUM ( 'bookmark', 'category', 'favourite', 'group', 'message', 'post', 'room', 'topic', 'user', 'vote' );`,
                successMessage: "Create type legacy_imported_type complete!",
                errorMessage: "Failed to Create type legacy_imported_type"
            },
            {
                query: `CREATE TABLE "legacy_object" (
            "_key" TEXT NOT NULL,
            "type" LEGACY_OBJECT_TYPE NOT NULL,
            "expireAt" TIMESTAMPTZ
            DEFAULT NULL
          );`,
                successMessage: "Create table legacy_object",
                errorMessage: "Failed to Create table legacy_object"
            },
            {
                query: `CREATE TABLE "legacy_hash" (
            "_key" TEXT NOT NULL,
            "data" JSONB NOT NULL,
            "type" LEGACY_OBJECT_TYPE NOT NULL
              DEFAULT 'hash'::LEGACY_OBJECT_TYPE
              CHECK ( "type" = 'hash' )
          );`,
                successMessage: "Create table legacy_hash complete!",
                errorMessage: "Failed to Create table legacy_hash"
            },
            {
                query: `CREATE TABLE "legacy_zset" (
          "_key" TEXT NOT NULL,
          "value" TEXT NOT NULL,
          "score" NUMERIC NOT NULL,
          "type" LEGACY_OBJECT_TYPE NOT NULL
            DEFAULT 'zset'::LEGACY_OBJECT_TYPE
            CHECK ( "type" = 'zset' )
        );`,
                successMessage: "Create table legacy_zset complete!",
                errorMessage: "Failed to Create table legacy_zset"
            },
            {
                query: `CREATE TABLE "legacy_set" (
          "_key" TEXT NOT NULL,
          "member" TEXT NOT NULL,
          "type" LEGACY_OBJECT_TYPE NOT NULL
            DEFAULT 'set'::LEGACY_OBJECT_TYPE
            CHECK ( "type" = 'set' )
        );`,
                successMessage: "Create table legacy_set complete!",
                errorMessage: "Failed to Create table legacy_set"
            },
            {
                query: `CREATE TABLE "legacy_list" (
          "_key" TEXT NOT NULL,
          "array" TEXT[] NOT NULL,
          "type" LEGACY_OBJECT_TYPE NOT NULL
            DEFAULT 'list'::LEGACY_OBJECT_TYPE
            CHECK ( "type" = 'list' )
        );`,
                successMessage: "Create table legacy_list complete!",
                errorMessage: "Failed to Create table legacy_list"
            },
            {
                query: `CREATE TABLE "legacy_string" (
          "_key" TEXT NOT NULL,
          "data" TEXT NOT NULL,
          "type" LEGACY_OBJECT_TYPE NOT NULL
            DEFAULT 'string'::LEGACY_OBJECT_TYPE
            CHECK ( "type" = 'string' )
        );`,
                successMessage: "Create table legacy_string complete!",
                errorMessage: "Failed to Create table legacy_string"
            },
            {
                query: `CREATE TABLE "legacy_imported" (
          "type" LEGACY_IMPORTED_TYPE NOT NULL,
          "id" BIGINT NOT NULL,
          "data" JSONB NOT NULL
        );`,
                successMessage: "Create table legacy_imported complete!",
                errorMessage: "Failed to Create table legacy_imported"
            },
            {
                query: `CREATE TABLE IF NOT EXISTS "session" (
            "sid" VARCHAR NOT NULL
              COLLATE "default",
            "sess" JSON NOT NULL,
            "expire" TIMESTAMP(6) NOT NULL
          ) WITH (OIDS=FALSE)`,
                successMessage: "Create table session complete!",
                errorMessage: "Failed to Create table session"
            },
            {
                query: `INSERT INTO "legacy_object" ("_key", "type", "expireAt")
          SELECT "data"->>'_key', 'zset'::LEGACY_OBJECT_TYPE, MIN(CASE
            WHEN ("data" ? 'expireAt') THEN to_timestamp(("data"->>'expireAt')::double precision / 1000)
            ELSE NULL
          END)
            FROM "objects"
          WHERE ("data" ? 'score')
            AND ("data"->>'value' IS NOT NULL)
            AND ("data"->>'score' IS NOT NULL)
          GROUP BY "data"->>'_key'`,
                successMessage: "Insert into legacy_object (zset)",
                errorMessage: 'Failed to create UNIQUE INDEX "uniq__objects__key"'
            },
            {
                query: `INSERT INTO "legacy_object" ("_key", "type", "expireAt")
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
            AND ("data"->>'_key') NOT LIKE '_imported_%:%'`,
                successMessage: "Insert into legacy_object (string, set, list, hash)",
                errorMessage: "Failed to Insert into legacy_object (string, set, list, hash)"
            },
            {
                query: `INSERT INTO "legacy_imported" ("type", "id", "data")
          SELECT (regexp_matches(o."data"->>'_key', '^_imported_(.*):'))[1]::LEGACY_IMPORTED_TYPE,
                (regexp_matches(o."data"->>'_key', ':(.*)$'))[1]::BIGINT,
                o."data" - '_key'
            FROM "objects" o
          WHERE (o."data"->>'_key') LIKE '_imported_%:%'`,
                successMessage: "Insert into legacy_imported complete!",
                errorMessage: "Failed to Insert into legacy_imported"
            },
            {
                query: `INSERT INTO "legacy_zset" ("_key", "value", "score")
          SELECT l."_key", o."data"->>'value', (o."data"->>'score')::numeric
            FROM "legacy_object" l
          INNER JOIN "objects" o
                  ON l."_key" = o."data"->>'_key'
          WHERE l."type" = 'zset'
            AND o."data"->>'value' IS NOT NULL
            AND o."data"->>'score' IS NOT NULL`,
                successMessage: "Insert into legacy_zset complete!",
                errorMessage: "Failed to Insert into legacy_zset"
            },
            {
                query: `INSERT INTO "legacy_set" ("_key", "member")
          SELECT l."_key", jsonb_array_elements_text(o."data"->'members')
            FROM "legacy_object" l
          INNER JOIN "objects" o
                  ON l."_key" = o."data"->>'_key'
          WHERE l."type" = 'set'`,
                successMessage: "Insert into legacy_set complete!",
                errorMessage: "Failed to Insert into legacy_set"
            },
            {
                query: `INSERT INTO "legacy_list" ("_key", "array")
          SELECT l."_key", ARRAY(SELECT a.t FROM jsonb_array_elements_text(o."data"->'list') WITH ORDINALITY a(t, i) ORDER BY a.i ASC)
            FROM "legacy_object" l
          INNER JOIN "objects" o
                  ON l."_key" = o."data"->>'_key'
          WHERE l."type" = 'list'`,
                successMessage: "Insert into legacy_list complete!",
                errorMessage: "Failed to Insert into legacy_list"
            },
            {
                query: `INSERT INTO "legacy_string" ("_key", "data")
          SELECT l."_key", CASE
            WHEN o."data" ? 'value' THEN o."data"->>'value'
            ELSE o."data"->>'data'
          END
            FROM "legacy_object" l
          INNER JOIN "objects" o
                  ON l."_key" = o."data"->>'_key'
          WHERE l."type" = 'string';`,
                successMessage: "Insert into legacy_string complete!",
                errorMessage: "Failed to Insert into legacy_string"
            },
            {
                query: `ALTER TABLE "legacy_object"
            ADD PRIMARY KEY ( "_key" );`,
                successMessage: "Add primary key to legacy_object successfully",
                errorMessage: 'Failed to ALTER TABLE "legacy_object'
            },
            {
                query: `ALTER TABLE "legacy_object"
            ADD UNIQUE ( "_key", "type" )`,
                successMessage: "Create unique index on key__type",
                errorMessage: "Failed to Create unique index on key__type"
            },
            {
                query: `CREATE INDEX "idx__legacy_object__expireAt" ON "legacy_object"("expireAt" ASC)`,
                successMessage: "Create index on expireAt complete!",
                errorMessage: "Failed to Create index on expireAt"
            },
            {
                query: `CREATE INDEX "idx__legacy_object__type" ON "legacy_object"("type")`,
                successMessage: "Create temporary index on typecomplete!",
                errorMessage: "Failed to Create temporary index on type"
            },
            {
                query: `CREATE INDEX "idx__legacy_zset__key__score" ON "legacy_zset"("_key" ASC, "score" DESC)`,
                successMessage: "Create index on key__score complete!",
                errorMessage: "Failed to Create index on key__score"
            },
            {
                query: `ALTER TABLE "legacy_hash"
            ADD PRIMARY KEY ("_key")`,
                successMessage: "Add primary key to legacy_hash complete!",
                errorMessage: "Failed to Add primary key to legacy_hash"
            },
            {
                query: `ALTER TABLE "legacy_hash"
          ADD CONSTRAINT "fk__legacy_hash__key"
          FOREIGN KEY ("_key", "type")
          REFERENCES "legacy_object"("_key", "type")
          ON UPDATE CASCADE
          ON DELETE CASCADE`,
                successMessage: "Add foreign key to legacy_hash complete!",
                errorMessage: "Failed to Add foreign key to legacy_hash"
            },
            {
                query: `ALTER TABLE "legacy_zset"
            ADD PRIMARY KEY ("_key", "value")`,
                successMessage: "Add primary key to legacy_zset complete!",
                errorMessage: "Failed to Add primary key to legacy_zset"
            },
            {
                query: `ALTER TABLE "legacy_zset"
          ADD CONSTRAINT "fk__legacy_zset__key"
          FOREIGN KEY ("_key", "type")
          REFERENCES "legacy_object"("_key", "type")
          ON UPDATE CASCADE
          ON DELETE CASCADE`,
                successMessage: "Add foreign key to legacy_zset complete!",
                errorMessage: "Failed to Add foreign key to legacy_zset"
            },
            {
                query: `ALTER TABLE "legacy_set"
            ADD PRIMARY KEY ("_key", "member")`,
                successMessage: "Add primary key to legacy_set complete!",
                errorMessage: "Failed to Add primary key to legacy_set"
            },
            {
                query: `ALTER TABLE "legacy_set"
          ADD CONSTRAINT "fk__legacy_set__key"
          FOREIGN KEY ("_key", "type")
          REFERENCES "legacy_object"("_key", "type")
          ON UPDATE CASCADE
          ON DELETE CASCADE`,
                successMessage: "Add foreign key to legacy_set complete!",
                errorMessage: "Failed to Add foreign key to legacy_set"
            },
            {
                query: `ALTER TABLE "legacy_list"
            ADD PRIMARY KEY ("_key")`,
                successMessage: "Add primary key to legacy_list complete!",
                errorMessage: "Failed to Add primary key to legacy_list"
            },
            {
                query: `ALTER TABLE "legacy_list"
          ADD CONSTRAINT "fk__legacy_list__key"
          FOREIGN KEY ("_key", "type")
          REFERENCES "legacy_object"("_key", "type")
          ON UPDATE CASCADE
          ON DELETE CASCADE`,
                successMessage: "Add foreign key to legacy_list complete!",
                errorMessage: "Failed to Add foreign key to legacy_list"
            },
            {
                query: `ALTER TABLE "legacy_string"
            ADD PRIMARY KEY ("_key")`,
                successMessage: "Add primary key to legacy_stringcomplete!",
                errorMessage: "Failed to Add primary key to legacy_string"
            },
            {
                query: `ALTER TABLE "legacy_string"
            ADD CONSTRAINT "fk__legacy_string__key"
            FOREIGN KEY ("_key", "type")
            REFERENCES "legacy_object"("_key", "type")
            ON UPDATE CASCADE
            ON DELETE CASCADE`,
                successMessage: "Add foreign key to legacy_string complete!",
                errorMessage: "Failed to Add foreign key to legacy_string"
            },
            {
                query: `ALTER TABLE "legacy_imported"
            ADD PRIMARY KEY ("type", "id")`,
                successMessage: "Add primary key to legacy_imported complete!",
                errorMessage: "Failed to Add primary key to legacy_imported"
            },
            {
                query: `DROP INDEX "idx__legacy_object__type"`,
                successMessage: "Drop temporary index on legacy_objects complete!",
                errorMessage: "Failed to Drop temporary index on legacy_objects"
            },
            {
                query: `CREATE VIEW "legacy_object_live" AS
          SELECT "_key", "type"
            FROM "legacy_object"
          WHERE "expireAt" IS NULL
              OR "expireAt" > CURRENT_TIMESTAMP`,
                successMessage: "Create view legacy_object_live complete!",
                errorMessage: 'Failed to Create view legacy_object_live"'
            },
            {
                query: `DROP TABLE "objects" CASCADE`,
                successMessage: "Drop table objects complete!",
                errorMessage: "Failed to Drop table objects"
            }
        ];
        for (const { query, successMessage, errorMessage } of queries) {
            await executeQuery(query, successMessage, errorMessage);
        }
    }
    catch (error) {
        if (error instanceof Error) {
            console.error("Error loading data:", error.message);
        }
        else {
            console.error("An unknown error occurred");
        }
    }
    finally {
        await pgPool.end();
        console.log("PostgreSQL pool has ended.");
    }
}
async function main() {
    await exportFromMongoDB();
    await loadData();
}
main().catch((error) => {
    console.error("An unexpected error occurred:", error);
});
