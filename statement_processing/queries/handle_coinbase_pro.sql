-- name: insert_coinbase_pro_deposits_and_withdrawals
WITH records_to_insert AS (

     SELECT id,
            DATE(time) AS Date,
            UPPER(SUBSTR(type, 1, 1)) || SUBSTR(type, 2) AS Type,
            `amount/balanceunit` AS Currency,
            CAST(amount AS DOUBLE) AS Amount

       FROM tmp_table

      WHERE type IN ('deposit', 'withdrawal')
        AND `amount/balanceunit` in (:fiat_list)

   ORDER BY time
)

INSERT INTO deposits_and_withdrawals

SELECT *
  FROM records_to_insert

 -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
 WHERE TRUE

    -- Overlapping statements or processing of the same input file
    -- twice is all allowed. But duplicates are not allowed.
    ON CONFLICT(id) DO NOTHING


-- name: insert_coinbase_pro_transactions
INSERT INTO transactions
     SELECT id,
             DATE(time) AS Date,
             Type,
             Item,
             Units,
             Currency,
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
