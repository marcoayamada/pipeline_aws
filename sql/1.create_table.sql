DROP TABLE IF EXISTS vgsales;

CREATE TABLE vgsales
(
  rank          INTEGER NOT NULL,
  name          VARCHAR(200) NOT NULL,
  platform      VARCHAR(100),
  year          INTEGER NOT NULL,
  genre         VARCHAR(100) NOT NULL,
  publisher     VARCHAR(100) NOT NULL,
  na_sales      FLOAT4 NOT NULL,
  eu_sales      FLOAT4 NOT NULL,
  jp_sales      FLOAT4 NOT NULL,
  other_sales   FLOAT4 NOT NULL,
  global_sales  FLOAT4 NOT NULL
);
