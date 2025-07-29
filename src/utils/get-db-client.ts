import { Client } from "pg";

export function getDbClient(type: "OLTP" | "OLAP") {
  return new Client({
    host: "localhost",
    port: Number(process.env[`POSTGRES_${type}_PORT`]),
    user: process.env[`POSTGRES_${type}_USER`],
    password: process.env[`POSTGRES_${type}_PASSWORD`],
    database: process.env[`POSTGRES_${type}_DB`],
  });
}
