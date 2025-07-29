import { join } from "path";
import { getDbClient } from "../utils/get-db-client";
import { runSqlScript } from "./run-sql";

export async function runDwInit() {
  const client = getDbClient("OLAP");
  await client.connect();

  try {
    await runSqlScript(client, join(__dirname, "../scripts/dw/init.sql"));
  } finally {
    await client.end();
  }
}
