-- ======================
-- src/scripts/dw/load.sql
-- ETL: Transformação e carga para o Data Warehouse
-- ======================
-- 1) Dimensão Tempo
INSERT INTO dim_tempo (data, dia, mes, ano, trimestre)
SELECT DISTINCT TO_DATE(v.data_venda, 'YYYY-MM-DD') AS data,
    EXTRACT(
        DAY
        FROM TO_DATE(v.data_venda, 'YYYY-MM-DD')
    ) AS dia,
    EXTRACT(
        MONTH
        FROM TO_DATE(v.data_venda, 'YYYY-MM-DD')
    ) AS mes,
    EXTRACT(
        YEAR
        FROM TO_DATE(v.data_venda, 'YYYY-MM-DD')
    ) AS ano,
    EXTRACT(
        QUARTER
        FROM TO_DATE(v.data_venda, 'YYYY-MM-DD')
    ) AS trimestre
FROM vendas v
WHERE v.data_venda IS NOT NULL
    AND v.data_venda ~ '^\d{4}-\d{2}-\d{2}';
-- 2) Dimensão Produto
INSERT INTO dim_produto (id_produto, nome_produto, categoria_produto)
SELECT DISTINCT p.id_produto,
    p.nome_produto,
    cp.nome_categoria_produto
FROM produto p
    JOIN categoria_produto cp ON p.id_categoria_produto = cp.id_categoria_produto
WHERE p.nome_produto IS NOT NULL
    AND p.id_categoria_produto IS NOT NULL;
-- 3) Dimensão Cliente
INSERT INTO dim_cliente (
        id_cliente,
        nome_cliente,
        idade,
        genero,
        categoria_cliente,
        cidade,
        estado,
        regiao
    )
SELECT DISTINCT c.id_cliente,
    c.nome_cliente,
    c.idade,
    c.genero,
    cc.nome_categoria_cliente,
    l.cidade,
    l.estado,
    l.regiao
FROM cliente c
    JOIN categoria_cliente cc ON c.id_categoria_cliente = cc.id_categoria_cliente
    JOIN localidade l ON c.id_localidade = l.id_localidade
WHERE c.nome_cliente IS NOT NULL
    AND c.id_categoria_cliente IS NOT NULL
    AND c.id_localidade IS NOT NULL;
-- 4) Dimensão Loja
INSERT INTO dim_loja (
        id_loja,
        nome_loja,
        gerente_loja,
        cidade,
        estado
    )
SELECT DISTINCT l.id_loja,
    l.nome_loja,
    l.gerente_loja,
    l.cidade,
    l.estado
FROM lojas l
WHERE l.nome_loja IS NOT NULL;
-- 5) Dimensão Promoção
INSERT INTO dim_promocao (
        id_promocao,
        nome_promocao,
        tipo_desconto,
        data_inicio,
        data_fim
    )
SELECT DISTINCT p.id_promocao,
    p.nome_promocao,
    p.tipo_desconto,
    TO_DATE(p.data_inicio, 'YYYY-MM-DD'),
    TO_DATE(p.data_fim, 'YYYY-MM-DD')
FROM promocoes p
WHERE p.nome_promocao IS NOT NULL
    AND p.data_inicio ~ '^\d{4}-\d{2}-\d{2}'
    AND p.data_fim ~ '^\d{4}-\d{2}-\d{2}';
-- 6) Dimensão Vendedor
INSERT INTO dim_vendedor (id_vendedor, nome_vendedor)
SELECT DISTINCT v.id_vendedor,
    v.nome_vendedor
FROM vendedor v
WHERE v.nome_vendedor IS NOT NULL;
-- 7) Fato Vendas
INSERT INTO fato_vendas (
        id_tempo,
        id_produto,
        id_cliente,
        id_loja,
        id_promocao,
        id_vendedor,
        qtd_vendida,
        preco_venda,
        valor_total
    )
SELECT dt.id_tempo,
    dp.id_produto,
    dc.id_cliente,
    dl.id_loja,
    dpr.id_promocao,
    dv.id_vendedor,
    iv.qtd_vendida,
    iv.preco_venda,
    v.valor_total
FROM item_vendas iv
    JOIN vendas v ON iv.id_venda = v.id_venda
    JOIN produto p ON iv.id_produto = p.id_produto
    JOIN cliente c ON v.id_cliente = c.id_cliente
    JOIN lojas l ON v.id_loja = l.id_loja
    JOIN vendedor vend ON v.id_vendedor = vend.id_vendedor
    LEFT JOIN promocoes pr ON iv.id_promocao_aplicada = pr.id_promocao -- associações com dimensões carregadas
    JOIN dim_tempo dt ON TO_DATE(v.data_venda, 'YYYY-MM-DD') = dt.data
    JOIN dim_produto dp ON p.id_produto = dp.id_produto
    JOIN dim_cliente dc ON c.id_cliente = dc.id_cliente
    JOIN dim_loja dl ON l.id_loja = dl.id_loja
    LEFT JOIN dim_promocao dpr ON pr.id_promocao = dpr.id_promocao
    JOIN dim_vendedor dv ON vend.id_vendedor = dv.id_vendedor
WHERE v.data_venda ~ '^\d{4}-\d{2}-\d{2}'
    AND iv.qtd_vendida IS NOT NULL
    AND iv.preco_venda IS NOT NULL
    AND v.valor_total IS NOT NULL;