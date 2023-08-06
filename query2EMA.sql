SELECT *
FROM (
    SELECT 
    event_time,
    ticker,
    close_price,
    sma as avg_close_price,
    ((close_price - emaprev) * (2.0/11.0) + emaprev) AS ema_close_price,
    `date`
    FROM (
        SELECT 
            event_time,
            ticker,
            close_price,
            sma,
            (close_price - LAG(sma, 1) OVER (ORDER BY event_time)) * (2.0/11.0) + LAG(sma, 1) OVER (ORDER BY event_time) as emaprev,
            `date`
        FROM (
            SELECT
                event_time,
                ticker,
                close_price,
                AVG(close_price) OVER (
                  PARTITION BY ticker
                  ORDER BY event_time
                  ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS sma,
                `date`
            FROM stock_table
        ) t
    ) t1
) t2
WHERE close_price < ema_close_price;