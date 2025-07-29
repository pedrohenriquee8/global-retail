-- Base de dados: global_retail_dw
-- ======================
-- DROP TABLES (ordem reversa para respeitar dependências)
-- ======================
DROP TABLE IF EXISTS fato_vendas CASCADE;
DROP TABLE IF EXISTS dim_tempo CASCADE;
DROP TABLE IF EXISTS dim_produto CASCADE;
DROP TABLE IF EXISTS dim_cliente CASCADE;
DROP TABLE IF EXISTS dim_loja CASCADE;
DROP TABLE IF EXISTS dim_promocao CASCADE;
DROP TABLE IF EXISTS dim_vendedor CASCADE;
-- ======================
-- TABELAS DIMENSÃO
-- ======================
CREATE TABLE dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    dia INT,
    mes INT,
    ano INT,
    trimestre INT
);
CREATE TABLE dim_produto (
    id_produto SERIAL PRIMARY KEY,
    nome_produto VARCHAR(255),
    categoria_produto VARCHAR(100)
);
CREATE TABLE dim_cliente (
    id_cliente SERIAL PRIMARY KEY,
    nome_cliente VARCHAR(255),
    idade INT,
    genero VARCHAR(50),
    categoria_cliente VARCHAR(100),
    cidade VARCHAR(100),
    estado VARCHAR(50),
    regiao VARCHAR(50)
);
CREATE TABLE dim_loja (
    id_loja SERIAL PRIMARY KEY,
    nome_loja VARCHAR(150),
    gerente_loja VARCHAR(255),
    cidade VARCHAR(100),
    estado VARCHAR(50)
);
CREATE TABLE dim_promocao (
    id_promocao SERIAL PRIMARY KEY,
    nome_promocao VARCHAR(150),
    tipo_desconto VARCHAR(50),
    data_inicio DATE,
    data_fim DATE
);
CREATE TABLE dim_vendedor (
    id_vendedor SERIAL PRIMARY KEY,
    nome_vendedor VARCHAR(255)
);
-- ======================
-- TABELA FATO
-- ======================
CREATE TABLE fato_vendas (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INT REFERENCES dim_tempo(id_tempo),
    id_produto INT REFERENCES dim_produto(id_produto),
    id_cliente INT REFERENCES dim_cliente(id_cliente),
    id_loja INT REFERENCES dim_loja(id_loja),
    id_promocao INT REFERENCES dim_promocao(id_promocao),
    id_vendedor INT REFERENCES dim_vendedor(id_vendedor),
    qtd_vendida INT,
    preco_venda DECIMAL(10, 2),
    valor_total DECIMAL(12, 2)
);