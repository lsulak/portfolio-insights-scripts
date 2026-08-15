-- name: insert_coinbase!
INSERT INTO transactions
     SELECT id,
         DATE(Timestamp) AS Date,
         CASE TransactionType
             WHEN 'Sell' THEN 'SELL'
             WHEN 'Convert' THEN 'SELL'
             ELSE 'BUY'
         END AS Type,
         Asset AS Item,
         PriceCurrency AS Currency,
         ABS(QuantityTransacted) AS Units,
         PriceatTransaction AS PPU,
         CAST('FeesandorSpread' AS DOUBLE) AS Fees,
         .0 AS Taxes,
         1.0 AS StockSplitRatio,
         Notes AS Remarks

     FROM tmp_table

     -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
     WHERE TRUE
        AND TransactionType IN ('Sell', 'Buy', 'Staking Income', 'Convert')

     -- Overlapping statements or processing of the same input file twice
     -- is all allowed. But duplicates are not allowed.
     ON CONFLICT(id) DO NOTHING

-- name: insert_coinbase_deposits_and_withdrawals!
INSERT INTO deposits_and_withdrawals
     SELECT id,
            DATE(Timestamp) AS Date,
            UPPER(TransactionType) AS Type,
            `PriceCurrency` AS Currency,
            ABS(CAST(QuantityTransacted AS DOUBLE)) AS Amount,
            'Coinbase' AS Remarks

       FROM tmp_table

     -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
      WHERE TRUE
        AND TransactionType IN ('Deposit', 'Withdrawal')

   ORDER BY Timestamp

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING
