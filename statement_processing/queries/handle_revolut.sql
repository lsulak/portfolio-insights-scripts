-- name: get_revolut_transactions
SELECT id,
       DATE(Date) AS Date,
       CASE Type
           WHEN 'SELL - MARKET' THEN 'SELL'
           WHEN 'BUY - MARKET' THEN 'BUY'
           ELSE ''
       END AS Type,
       Ticker AS Item,
       Currency,
       Quantity AS Units,
       CAST(REPLACE(REPLACE(REPLACE(Pricepershare, '$', ''), '"', ''), ',', '') AS DOUBLE) AS PPU,
       .0 AS Fees,
       .0 AS Taxes,
       1.0 AS StockSplitRatio,
       '' AS Remarks

  FROM tmp_table

 -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
 WHERE TRUE
   AND Type in ('SELL - MARKET', 'BUY - MARKET')


-- name: get_revolut_dividends
SELECT id,
       DATE(Date) AS Date,
       'DIVIDENDS' AS Type,
       Ticker AS Item,
       Currency,
       .0 AS Units,
       .0 AS PPU,
       .0 AS Fees,
       .0 AS Taxes,
       1.0 AS StockSplitRatio,
       CAST(SUBSTR(TotalAmount, 2) AS DOUBLE) AS TotalAmount,
       '' AS Remarks

  FROM tmp_table

 WHERE Type == 'DIVIDEND'


-- name: insert_revolut_fees
INSERT INTO transactions
   SELECT id,
          DATE(Date) AS Date,
          'FEES' AS Type,
          'Custory Fee Revolut' AS Item,
          Currency,
          .0 AS Units,
          .0 AS PPU,
          CAST(SUBSTR(TotalAmount, 3) AS DOUBLE) AS Fees,  -- always starts with '-' symbol and then currency
          .0 AS Taxes,
          1.0 AS StockSplitRatio,
          '' AS Remarks

     FROM tmp_table

    -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
    WHERE TRUE
      AND Type == 'CUSTODY FEE'

       -- Overlapping statements or processing of the same input file twice
       -- is all allowed. But duplicates are not allowed.
       ON CONFLICT(id) DO NOTHING;


-- name: get_revolut_stock_splits
   SELECT id,
          DATE(Date) AS Date,
          'SPLIT' AS Type,
          Ticker AS Item,
          Currency,
          .0 AS Units,
          .0 AS PPU,
          .0 AS Fees,
          .0 AS Taxes,
          Quantity AS TotalUnitsAfterSplit,
          1.0 AS StockSplitRatio,
          '' AS Remarks

     FROM tmp_table

    WHERE Type == 'STOCK SPLIT'


-- name: insert_revolut_transactions
INSERT INTO transactions
   SELECT id,
          Date,
          Type,
          Item,
          Currency,
          Units,
          PPU,
          Fees,
          Taxes,
          StockSplitRatio,
          Remarks

     FROM tmp_table_transactions_ready

    -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
    WHERE TRUE

       -- Overlapping statements or processing of the same input file twice
       -- is all allowed. But duplicates are not allowed.
       ON CONFLICT(id) DO NOTHING;
