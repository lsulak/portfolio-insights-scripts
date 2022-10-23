-- name: insert_coinbase_pro_deposits_and_withdrawals
INSERT INTO deposits_and_withdrawals
     SELECT id,
            DATE(time) AS Date,
            UPPER(type) AS Type,
            `amount/balanceunit` AS Currency,
            CAST(amount AS DOUBLE) AS Amount,
            'Coinbase Pro' AS Remarks

       FROM tmp_table

     -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
      WHERE TRUE

   ORDER BY time

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING


-- name: insert_coinbase_pro_transactions
INSERT INTO transactions
     SELECT id,
             DATE(time) AS Date,
             UPPER(Type) AS Type,
             Item,
             Currency,
             Units,
             PPU,
             Fees,
             .0 AS Taxes,
             1.0 AS StockSplitRatio,
             '' AS Remarks

         FROM tmp_table

        -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
        WHERE TRUE

        -- Overlapping statements or processing of the same input file twice
        -- is all allowed. But duplicates are not allowed.
        ON CONFLICT(id) DO NOTHING
