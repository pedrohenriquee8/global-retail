import { readFileSync } from "fs";
import { Client } from "pg";

export async function runSqlScript(client: Client, path: string) {
  const sql = readFileSync(path, "utf-8");
  await client.query(sql);
  console.log(`✅ Executado: ${path}`);
}
