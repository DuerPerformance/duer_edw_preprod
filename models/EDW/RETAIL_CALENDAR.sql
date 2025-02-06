
WITH retailcalendar AS (
    -- Start with the first known date
    SELECT 
        2014 AS retail_year,
        '2014-04-06'::DATE AS retail_start_date,
        364 AS days_in_year -- Regular year (52 weeks, 52 * 7 = 364 days)

    UNION ALL

    -- Recursively calculate the retail start date for the next years
    SELECT 
        retail_year + 1 AS retail_year,
        CASE 
            -- Add 53 weeks (371 days) in 2021, then every 6th year afterwards
            WHEN retail_year IN (2021, 2027, 2033) THEN DATEADD(day, 371, retail_start_date)
            -- Otherwise, add 52 weeks (364 days)
            ELSE DATEADD(day, 364, retail_start_date)
        END AS retail_start_date,
        CASE 
            -- Set the number of days for the 53-week years
            WHEN retail_year IN (2021, 2027, 2033) THEN 371
            ELSE 364
        END AS days_in_year
    FROM retail_calendar
    WHERE retail_year < 2030
)

SELECT * FROM RETAILCALENDAR
