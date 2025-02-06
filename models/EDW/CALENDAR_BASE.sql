WITH RECURSIVE Calendar_BASE AS (
    SELECT 
        DATEFROMPARTS((YEAR(CURRENT_DATE) - 9), 1, 1) AS date
    UNION ALL
    SELECT 
        DATEADD(DAY, 1, date)
    FROM Calendar_BASE
    WHERE date < DATEADD(YEAR, 6, DATEFROMPARTS(YEAR(CURRENT_DATE), 1, 1)) - 1
)
SELECT 
    date,
    ROW_NUMBER() OVER (ORDER BY date) AS date_key
FROM Calendar_BASE

