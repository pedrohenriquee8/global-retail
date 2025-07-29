import "dotenv/config";
import { getDbClient } from "./utils/get-db-client";
import { runCrmScripts } from "./runner/run-crm-scripts";
import { runDwInit } from "./runner/run-dw-scripts";
import * as etl from "./etl/load-dw";

async function main() {
  // 1) Inicializa CRM
  await runCrmScripts();

  // 2) Inicializa DW (tabelas)
  await runDwInit();

  // 3) Executa ETL
  const crm = getDbClient("OLTP");
  const dw = getDbClient("OLAP");
  await crm.connect();
  await dw.connect();
  try {
    await etl.loadDimTempo(crm, dw);
    console.log("✅ ETL concluído: Dimensão Tempo carregada.");
    await etl.loadDimProduto(crm, dw);
    console.log("✅ ETL concluído: Dimensão Produto carregada.");
    await etl.loadDimCliente(crm, dw);
    console.log("✅ ETL concluído: Dimensão Cliente carregada.");
    await etl.loadDimLoja(crm, dw);
    console.log("✅ ETL concluído: Dimensão Loja carregada.");
    await etl.loadDimPromocao(crm, dw);
    console.log("✅ ETL concluído: Dimensão Promoção carregada.");
    await etl.loadDimVendedor(crm, dw);
    console.log("✅ ETL concluído: Dimensão Vendedor carregada.");
    await etl.loadFatoVendas(crm, dw);
    console.log("✅ ETL concluído: Fato Vendas carregado.");
    console.log("🏁 ETL completo.");
  } finally {
    await crm.end();
    await dw.end();
  }
}

main().catch((err) => {
  console.error("❌ Erro geral:", err);
  process.exit(1);
});
