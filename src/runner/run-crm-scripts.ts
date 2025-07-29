import { join } from "path";
import { getDbClient } from "../utils/get-db-client";
import { runSqlScript } from "./run-sql";

export async function runCrmScripts() {
  const client = getDbClient("OLTP");
  await client.connect();

  try {
    await runSqlScript(client, join(__dirname, "../scripts/crm/init.sql"));
    await runSqlScript(client, join(__dirname, "../scripts/crm/inserts.sql"));
  } finally {
    await client.end();
  }
}
