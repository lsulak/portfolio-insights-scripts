-- name: insert_coinbase
INSERT INTO transactions
     SELECT id,
         DATE(Timestamp) AS Date,
         CASE TransactionType
             WHEN 'Sell' THEN 'SELL'
             ELSE 'BUY'
         END AS Type,
         Asset AS Item,
         QuantityTransacted AS Units,
         SpotPriceCurrency AS Currency,
         SpotPriceatTransaction AS PPU,
         CAST(Fees AS DOUBLE) AS Fees,
         .0 AS Taxes,
         1.0 AS StockSplitRatio,
         Notes AS Remarks

     FROM tmp_table

     -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
     WHERE TRUE

     -- Overlapping statements or processing of the same input file twice
     -- is all allowed. But duplicates are not allowed.
     ON CONFLICT(id) DO NOTHING
