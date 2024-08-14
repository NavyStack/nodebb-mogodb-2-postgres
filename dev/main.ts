import { MongoClient } from "mongodb"
import fs from "fs"
import { fileURLToPath } from "url"
import path from "path"
import { dirname } from "path"

const uri = ""
const dbName = ""
const collectionName = ""

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

async function main(): Promise<void> {
  const client = new MongoClient(uri)

  try {
    await client.connect()
    console.log("Connected to MongoDB")

    const db = client.db(dbName)
    const collection = db.collection(collectionName)
    const cursor = collection.find()
    const jsonData: any[] = []

    for await (const doc of cursor) {
      jsonData.push(doc)
    }
    const jsonFilePath = path.join(__dirname, "data.json")
    fs.writeFileSync(jsonFilePath, JSON.stringify(jsonData, null, 2))
    console.log(`Data successfully exported to ${jsonFilePath}`)
  } catch (error) {
    console.error("Error exporting data:", error)
  } finally {
    await client.close()
    console.log("MongoDB client closed.")
  }
}

main().catch(console.error)
