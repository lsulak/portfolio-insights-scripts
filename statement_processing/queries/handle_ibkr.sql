-- name: insert_ibkr_deposits_and_withdrawals
INSERT INTO deposits_and_withdrawals
     SELECT id,
            SettleDate AS Date,
            CASE
                WHEN Amount > 0 THEN 'DEPOSIT'
                ELSE 'WITHDRAWAL'
            END AS Type,
            Currency,
            ROUND(Amount, 4) AS Amount,
            'IBKR' AS Remarks

       FROM tmp_table

      -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
      WHERE TRUE
        -- Deposits/Withdrawals between accounts is not 'real' deposit from my salary
        -- and thus should be included because I would otherwise increase my net worth
        -- by doing several transfers - which doesn't make sense.
        AND Description NOT LIKE 'Adjustment: Cash Receipt/Disbursement/Transfer (Transfer to %'

   ORDER BY Date

        -- Overlapping statements or processing of the same input file
        -- twice is all allowed. But duplicates are not allowed.
        ON CONFLICT(id) DO NOTHING


-- name: insert_ibkr_forex
INSERT INTO forex
     SELECT id,
            DATE(REPLACE(`Date/Time`, ',', '')) AS Date,
            SUBSTR(Symbol, 1, 3) AS CurrencySold,
            Currency AS CurrencyBought,
            Symbol AS CurrencyPairCode,
            ABS(ROUND(Quantity, 4)) AS CurrencySoldUnits,
            ROUND(`T.Price`, 4) AS PPU,
            ABS(ROUND(ComminUSD, 4)) AS Fees,
            'IBKR' AS Remarks

       FROM tmp_table

      -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
      WHERE TRUE

   ORDER BY Date

        -- Overlapping statements or processing of the same input file
        -- twice is all allowed. But duplicates are not allowed.
        ON CONFLICT(id) DO NOTHING


-- name: insert_ibkr_special_fees
INSERT INTO transactions
   SELECT DISTINCT
          -- This is ok, even if the unique ID is not the same for records groupped by Date, Currency, and Description,
          -- any ID from the same group should uniquely represent the row (we chose MIN, but it really doesn't matter).
          MIN(id),
          Date,
          'FEES' AS Type,
          'IBKR Fee' AS Item,
          Currency,
          .0 AS Units,
          .0 PPU,
          ABS(ROUND(SUM(Amount), 4)) AS Fees,
          .0 AS Taxes,
          1.0 AS StockSplitRatio,
          Description AS Remarks

     FROM tmp_table
 GROUP BY Date, Currency, Description
   HAVING SUM(Amount) != .0  -- there might be negative and positive fees, like it's given back - ignoring these

 ORDER BY Date

       -- Overlapping statements or processing of the same input file
       -- twice is all allowed. But duplicates are not allowed.
       ON CONFLICT(id) DO NOTHING


-- name: insert_ibkr_transactions_without_special_fees
WITH records_to_insert AS (

     SELECT DISTINCT
          id,
          DATE(REPLACE(`Date/Time`, ',', '')) AS Date,
          CASE
               WHEN Quantity > 0
               THEN 'BUY'
               ELSE 'SELL'
          END AS Type,
          Symbol AS Item,
          Currency,
          ABS(ROUND(Quantity, 4)) AS Units,
          ROUND(`T.Price`, 4) AS PPU,
          ABS(ROUND(`Comm/Fee`, 4)) AS Fees,
          .0 as Taxes,
          1.0 AS StockSplitRatio,
         '' AS Remarks

     FROM tmp_table_stocks

 ORDER BY `Date/Time`
)

INSERT INTO transactions

SELECT *
  FROM records_to_insert

 -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
 WHERE TRUE

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING


-- name: select_deduped_dividends_received
-- Sometimes there can be a false positives
-- (i.e. dividends -$10, then +$10 and then +$5, so the final would be $5).
  SELECT MIN(id) AS id, -- doesn't matter
         Currency,
         Date,
         Item,
         PPU,
         SUM(Amount) AS Amount

    FROM tmp_table_dividends
GROUP BY Currency, Date, Item, PPU


-- name: insert_ibkr_transactions_with_special_fees
WITH records_to_insert AS (

     SELECT DISTINCT
          stocks.id,
          DATE(REPLACE(stocks.`Date/Time`, ',', '')) AS Date,
          CASE
               WHEN stocks.Quantity > 0
               THEN 'BUY'
               ELSE 'SELL'
          END AS Type,
          stocks.Symbol AS Item,
          stocks.Currency,
          ABS(ROUND(stocks.Quantity, 4)) AS Units,
          ROUND(stocks.`T.Price`, 4) AS PPU,
          ABS(ROUND(stocks.`Comm/Fee`, 4)) AS Fees,
          ABS(ROUND(COALESCE(t_fees.Amount, 0), 4)) as Taxes,
          1.0 AS StockSplitRatio,
          COALESCE(t_fees.Description, '') AS Remarks

     FROM tmp_table_stocks AS stocks

LEFT JOIN tmp_table_tran_fees AS t_fees
       ON stocks.Symbol = t_fees.Symbol
      AND stocks.`Date/Time` = t_fees.`Date/Time`
      AND stocks.Quantity = t_fees.Quantity
      AND stocks.`T.Price` = t_fees.TradePrice

 ORDER BY stocks.`Date/Time`
)

INSERT INTO transactions

SELECT *
  FROM records_to_insert

 -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
 WHERE TRUE

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING


-- name: select_deduped_dividend_taxes
-- Sometimes there can be a false positives
-- I.e. tax -$10, then +$10 and then -$5, so the final would
-- be -$5 - SUM operation would be used because there would be
-- information about reversion in the Description column.
-- If there is no reversed info, then we just naively grab
-- the biggest one and make sure that it's negative.
  SELECT MIN(id) AS id, -- doesn't matter
         Currency,
         Date,
         Item,
         PPU,
         CASE
           WHEN LOWER(Description) LIKE '%reversed%'
           THEN SUM(Amount)
           ELSE -1 * ABS(MAX(Amount))
         END AS Amount

    FROM tmp_table_div_taxes
GROUP BY Currency, Date, Item, PPU


-- name: insert_dividend_records_without_taxes
WITH records_to_insert AS (

        SELECT DISTINCT
               id,
               Date,
               'DIVIDENDS' AS Type,
               Item,
               Currency,
               ROUND(Amount / PPU, 4) AS Units,
               ROUND(PPU, 4) AS PPU,
               0 AS Fees,
               .0 as Taxes,
               1.0 AS StockSplitRatio,
               '' AS Remarks

          FROM tmp_table_deduped_dividends

     ORDER BY date
)

INSERT INTO transactions

SELECT *
  FROM records_to_insert

 -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
 WHERE TRUE

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING


-- name: insert_dividend_records_with_taxes
WITH records_to_insert AS (

        SELECT DISTINCT
               div.id,
               div.Date,
               'DIVIDENDS' AS Type,
               div.Item,
               div.Currency,
               ROUND(div.Amount / div.PPU, 4) AS Units,
               ROUND(div.PPU, 4) AS PPU,
               .0 AS Fees,
               ABS(ROUND(COALESCE(tax.Amount, 0), 4)) as Taxes,
               1.0 AS StockSplitRatio,
               '' AS Remarks

          FROM tmp_table_deduped_dividends AS div

     LEFT JOIN tmp_table_deduped_taxes AS tax
            ON div.Currency = tax.Currency
           AND div.Date = tax.Date
           AND div.PPU = tax.PPU
           AND div.Item = tax.Item

     ORDER BY div.date
)

INSERT INTO transactions

SELECT *
  FROM records_to_insert

 -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
 WHERE TRUE

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING


-- name: validate_duplicit_dividend_records
  SELECT Date, Item, PPU, COUNT(*)
    FROM transactions
   WHERE Type == 'DIVIDENDS'
GROUP BY Date, Item, PPU
  HAVING COUNT(*) > 1
