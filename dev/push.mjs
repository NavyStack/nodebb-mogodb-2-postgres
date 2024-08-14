import fs from "node:fs/promises"
import pkg from "pg"
const { Pool } = pkg

const connectionString = ""
const pool = new Pool({ connectionString })

async function loadData() {
  const client = await pool.connect()
  try {
    console.log("Attempting to connect to the database...")
    console.log("Connected to the database.")
    const data = await fs.readFile("data.json", "utf-8")
    const jsonData = JSON.parse(data)
    for (const item of jsonData) {
      await client.query('INSERT INTO "objects" (data) VALUES ($1)', [item])
    }
    console.log("Data has been successfully ingested into the database.")
  } catch (error) {
    console.error("An error occurred during the INSERT operation:", error)
  } finally {
    console.log("Releasing the database client...")
    client.release()
    console.log("Closing the database connection pool...")
    await pool.end()
    console.log("Database connection pool closed.")
  }
}

loadData()
