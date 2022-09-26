-- name: insert_ibkr_deposits_and_withdrawals
INSERT INTO deposits_and_withdrawals
     SELECT id,
            SettleDate AS Date,
            CASE
                WHEN Amount > 0 THEN 'Deposit'
                ELSE 'Withdrawal'
            END AS Type,
            Currency,
            ROUND(Amount, 4) AS Amount
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
            ROUND(Quantity, 4) AS CurrencySoldUnits,
            ROUND(`T.Price`, 4) AS PPU,
            ROUND(ComminUSD, 4) AS Fees
       FROM tmp_table
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
          ROUND(SUM(Amount), 4) AS Fees,
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


-- name: insert_ibkr_transactions
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
          ROUND(stocks.Quantity, 4) AS Units,
          stocks.Currency,
          ROUND(stocks.`T.Price`, 4) AS PPU,
          ROUND(stocks.`Comm/Fee`, 4) AS Fees,
          ROUND(COALESCE(t_fees.Amount, 0), 4) as Taxes,
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
