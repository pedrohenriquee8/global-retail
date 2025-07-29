import { Client } from "pg";

export async function loadDimTempo(crm: Client, dw: Client) {
  const { rows } = await crm.query<{ raw_date: string }>(`
    SELECT DISTINCT data_venda    AS raw_date FROM vendas
    UNION
    SELECT DISTINCT data_inicio   AS raw_date FROM promocoes
    UNION
    SELECT DISTINCT data_fim      AS raw_date FROM promocoes
  `);

  const existsStmt = `SELECT 1 FROM dim_tempo WHERE data = $1`;
  const insertStmt = `
    INSERT INTO dim_tempo (data, dia, mes, ano, trimestre)
    VALUES (
      $1::date,
      EXTRACT(DAY     FROM $1::date),
      EXTRACT(MONTH   FROM $1::date),
      EXTRACT(YEAR    FROM $1::date),
      EXTRACT(QUARTER FROM $1::date)
    )
  `;

  for (const { raw_date } of rows) {
    if (!raw_date) continue;

    let isoDate: string;

    if (/^\d{4}-\d{2}-\d{2}$/.test(raw_date)) {
      isoDate = raw_date;
    } else if (/^\d{2}\/\d{2}\/\d{4}$/.test(raw_date)) {
      const [dia, mes, ano] = raw_date.split("/");
      isoDate = `${ano}-${mes.padStart(2, "0")}-${dia.padStart(2, "0")}`;
    } else {
      console.warn(`[loadDimTempo] Ignorando formato inválido: ${raw_date}`);
      continue;
    }

    const { rowCount } = await dw.query(existsStmt, [isoDate]);
    if (rowCount && rowCount > 0) continue;

    await dw.query(insertStmt, [isoDate]);
  }
}

export async function loadDimProduto(crm: Client, dw: Client) {
  const { rows } = await crm.query<{
    nome: string | null;
    categoria: string | null;
  }>(`
    SELECT DISTINCT
      p.nome_produto    AS nome,
      cp.nome_categoria_produto AS categoria
    FROM produto p
    LEFT JOIN categoria_produto cp
      ON p.id_categoria_produto = cp.id_categoria_produto
  `);

  const existsStmt = `
    SELECT 1
      FROM dim_produto
     WHERE nome_produto     = $1
       AND categoria_produto = $2
  `;
  const insertStmt = `
    INSERT INTO dim_produto (nome_produto, categoria_produto)
    VALUES ($1, $2)
  `;

  for (const { nome, categoria } of rows) {
    if (!nome || !nome.trim()) {
      console.warn(
        `[loadDimProduto] Ignorando produto sem nome válido: '${nome}'`
      );
      continue;
    }

    const cat = categoria ?? "Não Informada";
    const { rowCount } = await dw.query(existsStmt, [nome.trim(), cat]);
    if (rowCount && rowCount > 0) continue;

    await dw.query(insertStmt, [nome.trim(), cat]);
  }
}

export async function loadDimCliente(crm: Client, dw: Client) {
  const { rows } = await crm.query<{
    nome: string | null;
    idade: number | null;
    genero: string | null;
    categoria: string | null;
    cidade: string | null;
    estado: string | null;
    regiao: string | null;
  }>(`
    SELECT DISTINCT
      c.nome_cliente         AS nome,
      c.idade                AS idade,
      c.genero               AS genero,
      cc.nome_categoria_cliente AS categoria,
      l.cidade               AS cidade,
      l.estado               AS estado,
      l.regiao               AS regiao
    FROM cliente c
    LEFT JOIN categoria_cliente cc
      ON c.id_categoria_cliente = cc.id_categoria_cliente
    LEFT JOIN localidade l
      ON c.id_localidade = l.id_localidade
  `);

  const existsStmt = `
    SELECT 1
      FROM dim_cliente
     WHERE nome_cliente       = $1
       AND idade              = $2
       AND genero             = $3
       AND categoria_cliente  = $4
       AND cidade             = $5
       AND estado             = $6
       AND regiao             = $7
  `;
  const insertStmt = `
    INSERT INTO dim_cliente
      (nome_cliente, idade, genero,
       categoria_cliente, cidade, estado, regiao)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
  `;

  for (const {
    nome,
    idade,
    genero,
    categoria,
    cidade,
    estado,
    regiao,
  } of rows) {
    if (!nome || !nome.trim()) {
      console.warn(
        `[loadDimCliente] Ignorando cliente sem nome válido: '${nome}'`
      );
      continue;
    }

    const cat = categoria ?? "Não Informada";
    const gen = genero ?? "Não Informado";
    const cid = cidade ?? "Não Informada";
    const est = estado ?? "Não Informado";
    const reg = regiao ?? "Não Informada";
    const age = idade ?? 0;

    const res = await dw.query(existsStmt, [
      nome.trim(),
      age,
      gen,
      cat,
      cid,
      est,
      reg,
    ]);
    if (res.rowCount && res.rowCount > 0) continue;

    await dw.query(insertStmt, [nome.trim(), age, gen, cat, cid, est, reg]);
  }
}

export async function loadDimLoja(crm: Client, dw: Client) {
  const { rows } = await crm.query<{
    nome: string | null;
    gerente: string | null;
    cidade: string | null;
    estado: string | null;
  }>(`
    SELECT DISTINCT
      nome_loja    AS nome,
      gerente_loja AS gerente,
      cidade       AS cidade,
      estado       AS estado
    FROM lojas
  `);

  const existsStmt = `
    SELECT 1
      FROM dim_loja
     WHERE nome_loja    = $1
       AND gerente_loja = $2
       AND cidade       = $3
       AND estado       = $4
  `;
  const insertStmt = `
    INSERT INTO dim_loja (nome_loja, gerente_loja, cidade, estado)
    VALUES ($1, $2, $3, $4)
  `;

  for (const { nome, gerente, cidade, estado } of rows) {
    if (!nome || !nome.trim()) {
      console.warn(`[loadDimLoja] Ignorando loja sem nome válido: '${nome}'`);
      continue;
    }

    const nm = nome.trim();
    const mgr = gerente && gerente.trim() ? gerente.trim() : "Não Informado";
    const cid = cidade && cidade.trim() ? cidade.trim() : "Não Informada";
    const est = estado && estado.trim() ? estado.trim() : "Não Informado";

    const res = await dw.query(existsStmt, [nm, mgr, cid, est]);
    if (res.rowCount && res.rowCount > 0) continue;

    await dw.query(insertStmt, [nm, mgr, cid, est]);
  }
}

export async function loadDimPromocao(crm: Client, dw: Client) {
  const { rows } = await crm.query<{
    nome: string | null;
    tipo: string | null;
    raw_inicio: string | null;
    raw_fim: string | null;
  }>(`
    SELECT
      nome_promocao    AS nome,
      tipo_desconto    AS tipo,
      data_inicio      AS raw_inicio,
      data_fim         AS raw_fim
    FROM promocoes
  `);

  function parseDate(raw: string | null): string | null {
    if (!raw) return null;
    const txt = raw.trim().toLowerCase();
    if (["n/a", "data inválida", "data invalida"].includes(txt)) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(txt)) return txt;
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(txt)) {
      const [d, m, y] = txt.split("/");
      return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
    }
    return null;
  }

  function normalizeTipo(
    raw: string | null
  ): "Valor Fixo" | "Valor Percentual" | "Não informado" {
    const txt = raw?.trim().toLowerCase() ?? "";
    if (txt.includes("fixo")) return "Valor Fixo";
    if (txt.includes("%") || txt.includes("percent")) return "Valor Percentual";
    return "Não informado";
  }

  const existsStmt = `
    SELECT 1
      FROM dim_promocao
     WHERE nome_promocao   = $1
       AND tipo_desconto   = $2
       AND data_inicio IS NOT DISTINCT FROM $3::date
       AND data_fim    IS NOT DISTINCT FROM $4::date
  `;
  const insertStmt = `
    INSERT INTO dim_promocao
      (nome_promocao, tipo_desconto, data_inicio, data_fim)
    VALUES ($1, $2, $3::date, $4::date)
  `;

  for (const { nome, tipo, raw_inicio, raw_fim } of rows) {
    if (!nome?.trim()) continue;
    const dtIni = parseDate(raw_inicio);
    const dtFim = parseDate(raw_fim);
    if (!dtIni || !dtFim) {
      console.warn(
        `[loadDimPromocao] Ignorando promoção sem datas válidas: "${nome}"`
      );
      continue;
    }

    const nm = nome.trim();
    const tpNorm = normalizeTipo(tipo);
    const { rowCount } = await dw.query(existsStmt, [nm, tpNorm, dtIni, dtFim]);
    if (rowCount && rowCount > 0) continue;

    await dw.query(insertStmt, [nm, tpNorm, dtIni, dtFim]);
  }
}

export async function loadDimVendedor(crm: Client, dw: Client) {
  const { rows } = await crm.query<{ nome: string | null }>(`
    SELECT DISTINCT nome_vendedor AS nome
      FROM vendedor
  `);

  const existsStmt = `
    SELECT 1
      FROM dim_vendedor
     WHERE nome_vendedor = $1
  `;
  const insertStmt = `
    INSERT INTO dim_vendedor (nome_vendedor)
    VALUES ($1)
  `;

  for (const { nome } of rows) {
    if (!nome || !nome.trim()) {
      console.warn(
        `[loadDimVendedor] Ignorando vendedor sem nome válido: '${nome}'`
      );
      continue;
    }

    const nm = nome.trim();
    const res = await dw.query(existsStmt, [nm]);
    if (res.rowCount && res.rowCount > 0) continue;

    await dw.query(insertStmt, [nm]);
  }
}

export async function loadFatoVendas(crm: Client, dw: Client) {
  function parseDate(raw: string | null): string | null {
    if (!raw) return null;
    const txt = raw?.trim().toLowerCase();
    if (["n/a", "data inválida", "data invalida"].includes(txt)) return null;
    let m: RegExpMatchArray | null;
    if ((m = txt.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/))) {
      const [, y, M, D] = m;
      return `${y}-${M.padStart(2, "0")}-${D.padStart(2, "0")}`;
    }
    if ((m = txt.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/))) {
      const [, d, M, y] = m;
      return `${y}-${M.padStart(2, "0")}-${d.padStart(2, "0")}`;
    }
    console.warn(
      `[loadFatoVendas] formato de data desconhecido, pulando: "${raw}"`
    );
    return null;
  }

  function normalizeTipo(
    raw: string | null
  ): "Valor Fixo" | "Valor Percentual" | "Não informado" {
    const txt = raw?.trim().toLowerCase() ?? "";
    if (txt.includes("fixo")) return "Valor Fixo";
    if (txt.includes("%") || txt.includes("percent")) return "Valor Percentual";
    return "Não informado";
  }

  const { rows } = await crm.query<{
    raw_date: string | null;
    nome_produto: string;
    cat_produto: string | null;
    nome_cliente: string | null;
    idade: number | null;
    genero: string | null;
    cat_cliente: string | null;
    cidade_cliente: string | null;
    estado_cliente: string | null;
    regiao_cliente: string | null;
    nome_loja: string | null;
    gerente_loja: string | null;
    cidade_loja: string | null;
    estado_loja: string | null;
    raw_inicio: string | null;
    raw_fim: string | null;
    nome_promocao: string | null;
    tipo_promocao: string | null;
    nome_vendedor: string | null;
    qtd_vendida: number;
    preco_venda: number;
  }>(`
    SELECT
      v.data_venda            AS raw_date,
      p.nome_produto          AS nome_produto,
      cp.nome_categoria_produto AS cat_produto,
      c.nome_cliente          AS nome_cliente,
      c.idade                 AS idade,
      c.genero                AS genero,
      cc.nome_categoria_cliente AS cat_cliente,
      lc.cidade               AS cidade_cliente,
      lc.estado               AS estado_cliente,
      lc.regiao               AS regiao_cliente,
      lo.nome_loja            AS nome_loja,
      lo.gerente_loja         AS gerente_loja,
      lo.cidade               AS cidade_loja,
      lo.estado               AS estado_loja,
      pr.data_inicio          AS raw_inicio,
      pr.data_fim             AS raw_fim,
      pr.nome_promocao        AS nome_promocao,
      pr.tipo_desconto        AS tipo_promocao,
      iv.qtd_vendida          AS qtd_vendida,
      iv.preco_venda          AS preco_venda,
      vd.nome_vendedor        AS nome_vendedor
    FROM vendas v
      JOIN item_vendas iv ON v.id_venda = iv.id_venda
      LEFT JOIN produto p ON iv.id_produto = p.id_produto
      LEFT JOIN categoria_produto cp ON p.id_categoria_produto = cp.id_categoria_produto
      LEFT JOIN cliente c ON v.id_cliente = c.id_cliente
      LEFT JOIN categoria_cliente cc ON c.id_categoria_cliente = cc.id_categoria_cliente
      LEFT JOIN localidade lc ON c.id_localidade = lc.id_localidade
      LEFT JOIN lojas lo ON v.id_loja = lo.id_loja
      LEFT JOIN promocoes pr ON iv.id_promocao_aplicada = pr.id_promocao
      LEFT JOIN vendedor vd ON v.id_vendedor = vd.id_vendedor
  `);

  const getTempo = `SELECT id_tempo FROM dim_tempo WHERE data = $1`;
  const getProduto = `
    SELECT id_produto FROM dim_produto
     WHERE nome_produto = $1
       AND categoria_produto = $2
  `;
  const getCliente = `
    SELECT id_cliente FROM dim_cliente
     WHERE nome_cliente = $1
       AND idade = $2
       AND genero = $3
       AND categoria_cliente = $4
       AND cidade = $5
       AND estado = $6
       AND regiao = $7
  `;
  const getLoja = `
    SELECT id_loja FROM dim_loja
     WHERE nome_loja = $1
       AND gerente_loja = $2
       AND cidade = $3
       AND estado = $4
  `;
  const getPromocao = `
    SELECT id_promocao FROM dim_promocao
     WHERE nome_promocao = $1
       AND tipo_desconto = $2
       AND data_inicio = $3::date
       AND data_fim = $4::date
  `;
  const getVendedor = `SELECT id_vendedor FROM dim_vendedor WHERE nome_vendedor = $1`;

  const insertStmt = `
    INSERT INTO fato_vendas
      (id_tempo, id_produto, id_cliente, id_loja, id_promocao, id_vendedor,
       qtd_vendida, preco_venda, valor_total)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
  `;

  for (const r of rows) {
    const saleDate = parseDate(r.raw_date);
    if (!saleDate) continue;

    const start = parseDate(r.raw_inicio);
    const end = parseDate(r.raw_fim);

    const catProd = r.cat_produto?.trim() ?? "Não Informada";
    const catCli = r.cat_cliente?.trim() ?? "Não Informada";
    const cityCli = r.cidade_cliente?.trim() ?? "Não Informada";
    const stCli = r.estado_cliente?.trim() ?? "Não Informado";
    const regCli = r.regiao_cliente?.trim() ?? "Não Informada";
    const mgrLoja = r.gerente_loja?.trim() ?? "Não Informado";
    const cityLoja = r.cidade_loja?.trim() ?? "Não Informada";
    const stLoja = r.estado_loja?.trim() ?? "Não Informado";
    const tpNorm = normalizeTipo(r.tipo_promocao);
    const seller = r.nome_vendedor?.trim();

    const tRes = await dw.query(getTempo, [saleDate]);
    if (!tRes.rowCount) continue;
    const idTempo = tRes.rows[0].id_tempo;

    const pRes = await dw.query(getProduto, [r.nome_produto?.trim(), catProd]);
    if (!pRes.rowCount) continue;
    const idProd = pRes.rows[0].id_produto;

    const cRes = await dw.query(getCliente, [
      r.nome_cliente?.trim(),
      r.idade ?? 0,
      r.genero?.trim() ?? "Não Informado",
      catCli,
      cityCli,
      stCli,
      regCli,
    ]);
    if (!cRes.rowCount) continue;
    const idCli = cRes.rows[0].id_cliente;

    const lRes = await dw.query(getLoja, [
      r.nome_loja?.trim()!,
      mgrLoja,
      cityLoja,
      stLoja,
    ]);
    if (!lRes.rowCount) continue;
    const idLoja = lRes.rows[0].id_loja;

    let idPromo: number | null = null;
    if (r.nome_promocao?.trim()) {
      const prRes = await dw.query(getPromocao, [
        r.nome_promocao.trim(),
        tpNorm,
        start,
        end,
      ]);
      if (prRes.rowCount) idPromo = prRes.rows[0].id_promocao;
    }

    const vRes = await dw.query(getVendedor, [seller]);
    if (!vRes.rowCount) continue;
    const idVend = vRes.rows[0].id_vendedor;

    const qtd_vendida = Math.abs(r.qtd_vendida ?? 0);

    await dw.query(insertStmt, [
      idTempo,
      idProd,
      idCli,
      idLoja,
      idPromo,
      idVend,
      qtd_vendida,
      r.preco_venda,
      r.preco_venda * qtd_vendida,
    ]);
  }
}
