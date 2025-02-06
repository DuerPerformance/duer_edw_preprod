{{  config(
        post_hook = [
        "drop table if exists EDW_PREPROD.EDW.RETAIL_CALENDAR",
        "drop table if exists EDW_PREPROD.EDW.CALENDAR_BASE"
    ]
) }}

WITH gregorian_calendar AS (
    SELECT
        date,
        date_key,
        DAYNAME(date) AS gregorian_day_name,
CASE 
    WHEN CEIL((DATEDIFF(day, DATE_TRUNC(YEAR, date), date) + 
        CASE WHEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) = 0 THEN  DAYOFWEEK(DATE_TRUNC(YEAR, date)) +7
         WHEN DAYOFWEEK(date) IN (1) THEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) + 2
         ELSE DAYOFWEEK(DATE_TRUNC(YEAR, date)) - 1 END) / 7) < 10
    THEN 'W0' || CEIL((DATEDIFF(day, DATE_TRUNC(YEAR, date), date) + 
        CASE WHEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) = 0 THEN  DAYOFWEEK(DATE_TRUNC(YEAR, date))   +7
        WHEN DAYOFWEEK(date) IN (1) THEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) + 2 
        ELSE DAYOFWEEK(DATE_TRUNC(YEAR, date)) - 1 
        END) / 7)
    ELSE 'W' || CEIL((DATEDIFF(day, DATE_TRUNC(YEAR, date), date) + 
        CASE WHEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) = 0 THEN  DAYOFWEEK(DATE_TRUNC(YEAR, date))   +7
      WHEN DAYOFWEEK(date) IN (1) THEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) + 2
        ELSE DAYOFWEEK(DATE_TRUNC(YEAR, date)) - 1 END) / 7)
END AS gregorian_week_name,
        TO_CHAR(date, 'Mon') AS gregorian_month_name,
        'Q' || TO_CHAR(CEIL(MONTH(date) / 3.0)) AS gregorian_quarter_name,
        'CY' || TO_CHAR(YEAR(date)) AS gregorian_year_name,
        DAY(date) as gregorian_date_of_month,
        DATEDIFF(DAY, DATE_TRUNC('QUARTER', date), date) + 1 AS gregorian_date_of_quarter,
          CASE WHEN DAYOFWEEK(date) = 0  AND  CEIL((DATEDIFF(day, DATE_TRUNC(YEAR, date), date) + 
        CASE WHEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) = 0 THEN  DAYOFWEEK(DATE_TRUNC(YEAR, date))   +7
      WHEN DAYOFWEEK(date) IN (1) THEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) + 2
        ELSE DAYOFWEEK(DATE_TRUNC(YEAR, date)) - 1 END) / 7) !=1 THEN 7
         WHEN  CEIL((DATEDIFF(day, DATE_TRUNC(YEAR, date), date) + 
        CASE WHEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) = 0 THEN  DAYOFWEEK(DATE_TRUNC(YEAR, date))   +7
      WHEN DAYOFWEEK(date) IN (1) THEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) + 2
        ELSE DAYOFWEEK(DATE_TRUNC(YEAR, date)) - 1 END) / 7) = 1 
                        THEN DAYOFMONTH(date)
         ELSE DAYOFWEEK(date) 
         END as gregorian_date_of_week,
        DATEDIFF(DAY,  TO_DATE(RIGHT(YEAR(date), 4) || '-01-01', 'YYYY-MM-DD'),date)+1 as gregorian_date_of_year,
 CASE WHEN MONTH(date) >=4 AND MONTH(date) <=6 THEN MONTH(date)-3
                WHEN MONTH(date) >=7 AND MONTH(date) <=9 THEN MONTH(date)-6 
                WHEN MONTH(date) >=10 AND MONTH(date) <=12 THEN MONTH(date)-9 
                ELSE MONTH(date) 
                END AS gregorian_month_of_quarter,
        MONTH(date)  as gregorian_month_of_year,
 CEIL((DATEDIFF(day, 
        CASE 
            WHEN DAYOFWEEK(DATE_TRUNC(month, date)) = 1 -- Monday
            THEN DATE_TRUNC(month, date) 
            WHEN DAYOFWEEK(DATE_TRUNC(month, date)) = 0 -- Sunday
            THEN DATE_TRUNC(month, date) - 6 
            ELSE DATEADD(day, -(DAYOFWEEK(DATE_TRUNC(month, date)) - 1), DATE_TRUNC(month,date)) 
        END, date) + 1) / 7) AS gregorian_week_of_month,
        --CEIL(DATEDIFF(day, DATE_TRUNC('QUARTER', date), date) / 7.0) AS gregorian_week_of_quarter,
CEIL((DATEDIFF(day, 
        CASE 
            WHEN DAYOFWEEK(DATE_TRUNC(quarter, date)) = 1 -- Monday
            THEN DATE_TRUNC(quarter, date) 
            WHEN DAYOFWEEK(DATE_TRUNC(quarter, date)) = 0 -- Sunday
            THEN DATE_TRUNC(quarter, date) - 6 
            ELSE DATEADD(day, -(DAYOFWEEK(DATE_TRUNC(quarter, date)) - 1), DATE_TRUNC(quarter,date)) 
        END, date) + 1) / 7) AS gregorian_week_of_quarter,
       
      CEIL((DATEDIFF(day, DATE_TRUNC(YEAR, date), date) + 
        CASE WHEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) = 0 THEN  DAYOFWEEK(DATE_TRUNC(YEAR, date))   +7
        WHEN DAYOFWEEK(date) IN (1) THEN DAYOFWEEK(DATE_TRUNC(YEAR, date)) + 2 
        ELSE DAYOFWEEK(DATE_TRUNC(YEAR, date)) - 1 
        END) / 7)
    
       
 AS gregorian_week_of_year,
        CASE WHEN  MONTH(date) < 10 THEN TO_CHAR(YEAR(date)) || '0' || TO_CHAR(MONTH(date)) 
                   ELSE TO_CHAR(YEAR(date)) || TO_CHAR(MONTH(date)) end as gregorian_month_serial,
        --TO_CHAR(YEAR(date)) ||   (FLOOR((DAYOFYEAR(date) - 1) / 7) + 1)
       -- as gregorian_week_serial ,
        
        DATEDIFF(month, '2015-01-01', date) + 1 AS gregorian_month_sequence,
        DATEDIFF(quarter, '2015-01-01', date) + 1 AS gregorian_quarter_sequence,
        DATEDIFF(year, '2015-01-01', date) +1  AS gregorian_year_sequence,
        CEIL(DATEDIFF(day, '2015-01-01', date) / 7.0) AS gregorian_week_sequence,
        DATEDIFF(day, '2015-01-01', date)+1 AS gregorian_date_sequence,
        
        
    FROM 
        {{ ref('CALENDAR_BASE') }}
),
 fiscal_year_start as (    SELECT     CASE 
            WHEN MONTH(cb.date) >= 4 THEN 
                
                   
                    TO_DATE(RIGHT(YEAR(cb.date), 4) || '-04-01', 'YYYY-MM-DD')
                
            ELSE 
                
                     TO_DATE(RIGHT(YEAR(cb.date) - 1, 4) || '-04-01', 'YYYY-MM-DD')
        END AS fiscal_year_start,cb.date,date_key FROM {{ ref('CALENDAR_BASE') }} cb),
fiscal_calendar AS (
    SELECT
        cb.date,
        date_key,
        DAYNAME(cb.date) AS fiscal_day_name,

CASE 
    WHEN CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
        CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0
         WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
         ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7) < 10
    THEN 'W0' || CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
        CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
         WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2 
        ELSE DAYOFWEEK(fiscal_year_start) - 1 
        END) / 7)
    ELSE 'W' || CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
        CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
        WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
        ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7)
END AS fiscal_week_name
,
        -- Fiscal Month Calculation
        'FM' || TO_CHAR(
            CASE 
                WHEN MONTH(cb.date) >= 4 THEN MONTH(cb.date) - 3
                ELSE MONTH(cb.date) + 9
            END, 'FM00'
        ) AS fiscal_month_name,

        -- Fiscal Quarter Calculation
        'FQ0' || TO_CHAR(
            CASE 
                WHEN MONTH(cb.date) >= 4 THEN CEIL((MONTH(cb.date) - 3) / 3.0)
                ELSE CEIL((MONTH(cb.date) + 9) / 3.0)
            END
        ) AS fiscal_quarter_name,

        -- Fiscal Year Calculation
        'FY' || RIGHT(
            CASE 
                WHEN MONTH(cb.date) >= 4 THEN TO_CHAR(YEAR(cb.date)+1)
                ELSE TO_CHAR(YEAR(cb.date) )
            END, 2
        ) AS fiscal_year_name,
        DAY(cb.date) as fiscal_date_of_month,
           CASE
            WHEN cb.date >= DATEADD(MONTH, 9, fiscal_year_start) THEN DATEDIFF(DAY,(DATEADD(MONTH, 9, fiscal_year_start)),cb.date) +1
            WHEN cb.date >= DATEADD(MONTH, 6, fiscal_year_start) THEN DATEDIFF(DAY, (DATEADD(MONTH, 6, fiscal_year_start)),cb.date) +1
            WHEN cb.date >= DATEADD(MONTH, 3, fiscal_year_start) THEN DATEDIFF(DAY,(DATEADD(MONTH, 3, fiscal_year_start)),cb.date) +1
            ELSE DATEDIFF(DAY,fiscal_year_start,cb.date) +1
        END AS fiscal_date_of_quarter,
        CASE WHEN CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                                              CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
                                                                             WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2 
                                                                            ELSE DAYOFWEEK(fiscal_year_start) - 1 
                                                                              END) / 7) = 1 
                THEN DAYOFMONTH(cb.date)
        ELSE CASE WHEN 
                      DAYOFWEEK(cb.date) <=6 THEN DAYOFWEEK(cb.date) +1 ELSE 1 END
                      END AS fiscal_date_of_week,
        DATEDIFF(DAY, fiscal_year_start,cb.date)+1 as fiscal_date_of_year,
     CASE WHEN MONTH(cb.date) >=4 AND MONTH(cb.date)<=6 THEN MONTH(cb.date)-3 
                WHEN MONTH(cb.date) >=7 AND MONTH(cb.date)<=9 THEN MONTH(cb.date)-6 
                WHEN MONTH(cb.date) >=10 AND MONTH(cb.date)<=12 THEN MONTH(cb.date)-9 
                ELSE MONTH(cb.date) 
                END AS fiscal_month_of_quarter,
    --  CEIL(DATEDIFF(day, DATE_TRUNC('MONTH', fiscal_year_start), cb.date) / 7.0) AS fiscal_week_of_month,
CEIL((DATEDIFF(day, 
        DATEADD(day, -(DAYOFWEEK(DATE_TRUNC(month, cb.date))), DATE_TRUNC(month, cb.date)), cb.date) + 1) / 7) AS fiscal_week_of_month,
    --CEIL(DATEDIFF(day, DATE_TRUNC('QUARTER', fiscal_year_start), cb.date) / 7.0) AS fiscal_week_of_quarter,
CASE 
    -- If the date is in Q1 of the fiscal year (April 1 - June 30)
    WHEN MONTH(cb.date) BETWEEN 4 AND 6 
    THEN CEIL((DATEDIFF(day, DATEFROMPARTS(YEAR(cb.date), 4, 1), cb.date) + (7-DAYOFWEEK(cb.date))) / 7.0)
    
    -- If the date is in Q2 of the fiscal year (July 1 - September 30)
    WHEN MONTH(cb.date) BETWEEN 7 AND 9
    THEN CEIL((DATEDIFF(day, DATEFROMPARTS(YEAR(cb.date), 7, 1), cb.date) + (7-DAYOFWEEK(cb.date))) / 7.0)
    
    -- If the date is in Q3 of the fiscal year (October 1 - December 31)
    WHEN MONTH(cb.date) BETWEEN 10 AND 12
    THEN CEIL((DATEDIFF(day, DATEFROMPARTS(YEAR(cb.date), 10, 1), cb.date) + (7-DAYOFWEEK(cb.date))) / 7.0)
    
    -- If the date is in Q4 of the fiscal year (January 1 - March 31) of the next year
    ELSE CEIL((DATEDIFF(day, DATEFROMPARTS(YEAR(cb.date) , 1, 1), cb.date) + (7-DAYOFWEEK(cb.date))) / 7.0)
END AS fiscal_week_of_quarter

,
       CASE 
                                           WHEN CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0
                                                WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
                                              ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7) < 10
                                                                        THEN '0' ||TO_CHAR(  CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                                              CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
                                                                             WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2 
                                                                            ELSE DAYOFWEEK(fiscal_year_start) - 1 
                                                                              END) / 7))
                                                                ELSE TO_CHAR( CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                                      CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
                                                                      WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
                                                                        ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7))
                                                                        END  as fiscal_week_of_year,
     CASE WHEN MONTH(cb.date) >=4
       
           THEN TO_CHAR(YEAR(cb.date) +1) || CASE 
                                           WHEN CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0
                                                WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
                                              ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7) < 10
                                                                        THEN '0' ||TO_CHAR(  CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                                              CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
                                                                             WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2 
                                                                            ELSE DAYOFWEEK(fiscal_year_start) - 1 
                                                                              END) / 7))
                                                                ELSE TO_CHAR( CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                                      CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
                                                                      WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
                                                                        ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7))
                                                                        END
 
           ELSE TO_CHAR(YEAR(cb.date)) ||
           
                                                    CASE 
                                                      WHEN CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
                                                     CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0
                                                         WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
                                                                ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7) < 10
                                                         THEN  '0' || TO_CHAR (CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
        CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
         WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2 
        ELSE DAYOFWEEK(fiscal_year_start) - 1 
        END) / 7))
    ELSE TO_CHAR(CEIL((DATEDIFF(day, fiscal_year_start, cb.date) + 
        CASE --WHEN DAYOFWEEK(fiscal_year_start) = 1 THEN 0 
        WHEN DAYOFWEEK(cb.date) IN (0,1) THEN DAYOFWEEK(fiscal_year_start) + 2
        ELSE DAYOFWEEK(fiscal_year_start) - 1 END) / 7))
        END

           
           end  as fiscal_week_serial,
      DATEDIFF(month, '2015-01-01', cb.date) + 10 AS fiscal_month_sequence,
      DATEDIFF(quarter, '2014-04-01', cb.date) + 1 AS fiscal_quarter_sequence,
       CASE 
                WHEN MONTH(date) in (1,2,3) THEN TO_CHAR(YEAR(cb.date) ) -TO_CHAR(2014)
                WHEN cb.date >= fiscal_year_start THEN TO_CHAR(YEAR(cb.date)+1) -TO_CHAR(2014)
                ELSE 0
            END     as fiscal_year_sequence,
      CEIL(DATEDIFF(day, '2014-04-01', cb.date) / 7.0) AS fiscal_week_sequence,
      DATEDIFF(day, '2015-01-01', date)+1 AS fiscal_date_sequence,
    
        
    FROM 
        fiscal_year_start cb
),

-----retail calculations as 
 retail AS (
      SELECT 
    cb.date,
    
        YEAR(cb.date) as retail_Year,
         rc.retail_start_date
       AS RETAIL_START_DATE,
        CASE
            WHEN cb.date >= rc.retail_start_date THEN rc.retail_start_date--rc.retail_start_date
            ELSE LAG(rc.retail_start_date,1) OVER (PARTITION BY DATE ORDER BY rc.retail_year)
        END AS actual_retail_start_date,
        rc.retail_start_date 
        as current_year_start_date,
        cb.date_key
        FROM   {{ ref('RETAIL_CALENDAR') }} rc 
   CROSS JOIN  {{ ref('CALENDAR_BASE') }} cb 

    
    order by cb.date)


,adjusted_retail_calendar as (

   SELECT date,date_key,actual_retail_start_date,current_year_start_date FROM 

retail WHERE RETAIL_YEAR = YEAR(RETAIL_START_DATE)
ORDER BY DATE )



, retail_days_since_start AS (
    SELECT
        
       actual_retail_start_date  as retail_calendar_start_date,date as date, DATEDIFF(day, actual_retail_start_date, date) +1 AS days_since_start,
    current_year_start_date,date_key from adjusted_retail_calendar
)

,retailcalendar AS (
    SELECT
    
    rs.retail_calendar_start_date,
    rs.current_year_start_date,
    rs.days_since_start,
        rs.date,
        rs.date_key,
        DAYNAME(rs.date) AS retail_day_name,
         'W' || (CASE WHEN FLOOR(days_since_start % 7) = 0 
              THEN FLOOR(days_since_start / 7)  
              ELSE FLOOR(days_since_start / 7) + 1 
              END) AS retail_week_name,
            CASE
            WHEN days_since_start <= 28 THEN 'FM' || TO_CHAR(1)
            WHEN days_since_start <= 63 AND days_since_start > 28 THEN 'FM' || TO_CHAR(2)
            WHEN days_since_start <= 91 AND days_since_start > 63 THEN 'FM' || TO_CHAR(3)
            WHEN days_since_start <= 119 AND days_since_start > 91 THEN 'FM' || TO_CHAR(4)
            WHEN days_since_start <= 154 AND days_since_start > 119 THEN 'FM' || TO_CHAR(5)
            WHEN days_since_start <= 182 AND days_since_start > 154 THEN 'FM' || TO_CHAR(6)
            WHEN days_since_start <= 210 AND days_since_start > 182 THEN 'FM' || TO_CHAR(7)
            WHEN days_since_start <= 245 AND days_since_start > 210 THEN 'FM' || TO_CHAR(8)
            WHEN days_since_start <= 273 AND days_since_start > 245 THEN 'FM' || TO_CHAR(9)
            WHEN days_since_start <= 301 AND days_since_start > 273 THEN 'FM' || TO_CHAR(10)
            WHEN days_since_start <= 336 AND days_since_start > 301 THEN 'FM' || TO_CHAR(11)
            ELSE 'FM' || TO_CHAR(12)
        END AS retail_month_name,
          'FQ' || TO_CHAR(
             CASE WHEN CEIL(days_since_start / 91.0) >= 4.0 THEN 4 
            ELSE CEIL(days_since_start / 91.0)
            END
        ) AS retail_quarter_name,
      CASE  WHEN rs.date >= rs.current_year_start_date 
         
    
    THEN TO_CHAR(YEAR(rs.date)+1) || '0' ||   ( CASE WHEN CEIL(days_since_start / 91.0) >= 4.0 THEN 4 
            ELSE CEIL(days_since_start / 91.0)
            END)
    ELSE TO_CHAR(YEAR(rs.date)) || '0' ||  (   CASE WHEN CEIL(days_since_start / 91.0) >= 4.0 THEN 4 
            ELSE CEIL(days_since_start / 91.0)
            END) END as retail_quarter_serial,
          'FY' || RIGHT(
            CASE 
                WHEN rs.date >= rs.current_year_start_date THEN TO_CHAR(YEAR(rs.date)+1)
                ELSE TO_CHAR(YEAR(rs.date) )
            END, 2
        ) AS retail_year_name,
         CASE 
                WHEN rs.date >= rs.current_year_start_date THEN TO_CHAR(YEAR(rs.date)+1)
                ELSE TO_CHAR(YEAR(rs.date) )
            END 
            as retail_year_serial,
      CASE
            WHEN days_since_start <= 28 THEN days_since_start 
            WHEN days_since_start <= 63 AND days_since_start > 28 THEN days_since_start - 28 
            WHEN days_since_start <= 91 AND days_since_start > 63 THEN days_since_start - 63 
            WHEN days_since_start <= 119 AND days_since_start > 91 THEN days_since_start - 91 
            WHEN days_since_start <= 154 AND days_since_start > 119 THEN days_since_start - 119 
            WHEN days_since_start <= 182 AND days_since_start > 154 THEN days_since_start - 154 
            WHEN days_since_start <= 210 AND days_since_start > 182 THEN days_since_start - 182 
            WHEN days_since_start <= 245 AND days_since_start > 210 THEN days_since_start - 210 
            WHEN days_since_start <= 273 AND days_since_start > 245 THEN days_since_start - 245 
            WHEN days_since_start <= 301 AND days_since_start > 273 THEN days_since_start - 273 
            WHEN days_since_start <= 336 AND days_since_start > 301 THEN days_since_start - 301 
            ELSE days_since_start - 336
        END AS retail_date_of_month,
      CASE 
        WHEN days_since_start <= 91 THEN days_since_start  -- Q1
        WHEN days_since_start > 91 AND days_since_start <= 182 THEN days_since_start - 91  -- Q2
        WHEN days_since_start > 182 AND days_since_start <= 273 THEN days_since_start - 182  -- Q3
        WHEN days_since_start > 273 THEN days_since_start - 273  -- Q4
    END AS retail_date_of_quarter,
     CASE WHEN DAYOFWEEK(rs.date) <=6 THEN DAYOFWEEK(rs.date) +1 ELSE 1 END AS retail_date_of_week,
     DATEDIFF(DAY, retail_calendar_start_date,rs.date)+1 as retail_date_of_year,
 CASE 
         WHEN days_since_start <=  28  THEN 1 -- For 1st month (4 weeks)
            WHEN days_since_start <=  63 AND days_since_start > 28 THEN 2 -- For 2nd month (5 weeks)
            WHEN days_since_start <=  91 AND days_since_start > 63  THEN 3  -- For 3rd month (4 weeks)
            WHEN days_since_start <=  119 AND days_since_start > 91 THEN 1
            WHEN days_since_start <=  154 AND days_since_start > 119 THEN 2 
            WHEN days_since_start <=  182 AND days_since_start > 154 THEN 3 
            WHEN days_since_start <=  210 AND days_since_start > 182 THEN 1 
            WHEN days_since_start <=  245 AND days_since_start > 210 THEN 2 
            WHEN days_since_start <=  273 AND days_since_start > 245 THEN 3 
            WHEN days_since_start <=  301 AND days_since_start > 273 THEN 1 
            WHEN days_since_start <=  336 AND days_since_start > 301 THEN 2 
            ELSE 3
    END AS retail_month_of_quarter,
     CASE 
         WHEN days_since_start <=  28  THEN CEIL((days_since_start ) / 7)  -- For 1st month (4 weeks)
            WHEN days_since_start <=  63 AND days_since_start > 28 THEN CEIL((days_since_start -28 ) / 7) -- For 2nd month (5 weeks)
            WHEN days_since_start <=  91 AND days_since_start > 63  THEN CEIL((days_since_start -63 ) / 7) -- For 3rd month (4 weeks)
            WHEN days_since_start <=  119 AND days_since_start > 91 THEN CEIL((days_since_start -91 ) / 7)
            WHEN days_since_start <=  154 AND days_since_start > 119 THEN CEIL((days_since_start -119 ) / 7)
            WHEN days_since_start <=  182 AND days_since_start > 154 THEN CEIL((days_since_start -154 ) / 7)
            WHEN days_since_start <=  210 AND days_since_start > 182 THEN CEIL((days_since_start -182 ) / 7)
            WHEN days_since_start <=  245 AND days_since_start > 210 THEN CEIL((days_since_start -210 ) / 7)
            WHEN days_since_start <=  273 AND days_since_start > 245 THEN CEIL((days_since_start -245 ) / 7)
            WHEN days_since_start <=  301 AND days_since_start > 273 THEN CEIL((days_since_start -273 ) / 7)
            WHEN days_since_start <=  336 AND days_since_start > 301 THEN CEIL((days_since_start -301 ) / 7)
            ELSE CEIL((days_since_start -336 ) / 7)
            
        END AS retail_week_of_month,
         CASE 
            WHEN days_since_start <= 91
                                   THEN CEIL((days_since_start ) / 7)  -- For 1st month of the quarter (4 weeks)
            WHEN days_since_start > 91 AND days_since_start <= 182 
                                THEN CEIL((days_since_start - 91 ) / 7)  -- For 2nd month of the quarter (5 weeks)
            WHEN days_since_start > 182 AND days_since_start <= 273
                                THEN  CEIL((days_since_start - 182 ) / 7)  
            ELSE CEIL((days_since_start - 273 ) / 7)   -- For 3rd month of the quarter (4 weeks)
        END AS retail_week_of_quarter,
     
     CASE WHEN FLOOR(days_since_start % 7) = 0 
              THEN FLOOR(days_since_start / 7)  
              ELSE FLOOR(days_since_start / 7) + 1 
              END  as retail_week_of_year,
      CASE WHEN rs.date > rs.current_year_start_date
        THEN TO_CHAR(YEAR(rs.date) +1) || (CASE WHEN FLOOR(days_since_start % 7) = 0 
              THEN FLOOR(days_since_start / 7)  
              ELSE FLOOR(days_since_start / 7) + 1 
              END ) 
        ELSE TO_CHAR(YEAR(rs.date) ) || (CASE WHEN FLOOR(days_since_start % 7) = 0 
              THEN FLOOR(days_since_start / 7)  
              ELSE FLOOR(days_since_start / 7) + 1 
              END) 
        END AS  retail_week_serial,
     
  CASE 
                WHEN rs.date >= rs.current_year_start_date THEN TO_CHAR(YEAR(rs.date)+1) -TO_CHAR(2014)
                ELSE TO_CHAR(YEAR(rs.date) ) -TO_CHAR(2014)
            END     as retail_year_sequence,
     CEIL(DATEDIFF(day, '2014-04-01', rs.date) / 7.0) AS retail_week_sequence,
     DATEDIFF(day, '2015-01-01', rs.date)+1 as retail_date_sequence
    FROM 
        retail_days_since_start rs
)
 ,  cte as(
SELECT
    'Gregorian' as Calendar_Type,
    g.date as DATE ,
    g.date_key,
    g.gregorian_day_name as DAY_NAME,
    g.gregorian_date_sequence as DATE_SEQUENCE,
    g.gregorian_date_of_year as DATE_OF_YEAR,
    g.gregorian_date_of_quarter as DATE_OF_QUARTER,
    g.gregorian_date_of_month as DATE_OF_MONTH,
    g.gregorian_date_of_week as DATE_OF_WEEK,
    g.gregorian_year_name as YEAR_NAME,
    ROUND(YEAR(g.date),0) as  YEAR_SERIAL,
    g.gregorian_year_sequence as YEAR_SEQUENCE,
    g.gregorian_quarter_name as QUARTER_NAME,
    TO_CHAR(YEAR(g.date)) || '0' || REPLACE( g.gregorian_quarter_name, 'Q', '' ) as QUARTER_SERIAL,
    g.gregorian_quarter_sequence as QUARTER_SEQUENCE,
    REPLACE( g.gregorian_quarter_name, 'Q', '' ) as QUARTER_OF_YEAR,
    g.gregorian_month_name as MONTH_NAME,
    g.gregorian_month_serial as MONTH_SERIAL,
    g.gregorian_month_sequence as MONTH_SEQUENCE,
    ROUND(g.gregorian_month_of_year,0) as MONTH_OF_YEAR,
    g.gregorian_month_of_quarter as MONTH_OF_QUARTER,
    g.gregorian_week_name as WEEK_NAME,
    TO_CHAR(YEAR(g.date)) || REPLACE(g.gregorian_week_name,'W','') as WEEK_SERIAL,
    g.gregorian_week_sequence as WEEK_SEQUENCE,
    g.gregorian_week_of_year as WEEK_OF_YEAR,
   
    g.gregorian_week_of_quarter as WEEK_OF_QUARTER,
    g.gregorian_week_of_month as WEEK_OF_MONTH

    FROM 
    gregorian_calendar g
  
   
    UNION ALL
    
   SELECT
   
   
   'DUER FISCAL CALENDAR' as CALENDAR_TYPE,
    
    f.date,
    f.date_key,
    f.fiscal_day_name,
    f.fiscal_date_sequence,
   f.fiscal_date_of_year,
    f.fiscal_date_of_quarter,
    f.fiscal_date_of_month,
    f.fiscal_date_of_week,
      f.fiscal_year_name,
    
   
    CASE WHEN MONTH(f.date)>=4
    THEN ROUND(YEAR(f.date) +1,0) 
    else ROUND(YEAR(f.date),0) END as fiscal_year_serial,
    f.fiscal_year_sequence, 
    f.fiscal_quarter_name,
    CASE WHEN MONTH(f.date)>=4
     THEN TO_CHAR(YEAR(f.date)+1) || '0' || REPLACE( f.fiscal_quarter_name, 'FQ0', '' ) 
     ELSE TO_CHAR(YEAR(f.date)) || '0' || REPLACE( f.fiscal_quarter_name, 'FQ0', '' ) END as fiscal_quarter_serial,
    f.fiscal_quarter_sequence,
    REPLACE( f.fiscal_quarter_name, 'FQ0', '' ) as fiscal_quarter_of_year,
     f.fiscal_month_name,
      CASE WHEN MONTH(f.date)>=4
     THEN CASE WHEN  REPLACE( f.fiscal_month_name, 'FM', '' )  <10 THEN TO_CHAR(YEAR(f.date)+1)  || TO_CHAR( REPLACE( 
     f.fiscal_month_name, 'FM', '' ) ) 
                ELSE TO_CHAR(YEAR(f.date)+1) || TO_CHAR(REPLACE( f.fiscal_month_name, 'FM', '' ))
        END
     ELSE 
     CASE WHEN  REPLACE( f.fiscal_month_name, 'FM', '' )  <10 THEN TO_CHAR(YEAR(f.date))  || TO_CHAR( REPLACE( 
     f.fiscal_month_name, 'FM', '' ) ) 
                ELSE TO_CHAR(YEAR(f.date)) || TO_CHAR(REPLACE( f.fiscal_month_name, 'FM', '' ))
     end 
     END as  fiscal_month_serial,
   f.fiscal_month_sequence,
   ROUND(REPLACE( f.fiscal_month_name, 'FM', '' ),0) as fiscal_month_of_year,
   f.fiscal_month_of_quarter,
   f.fiscal_week_name,
   f.fiscal_week_serial,
   f.fiscal_week_sequence,
   f.fiscal_week_of_year,
   f.fiscal_week_of_quarter,
   f.fiscal_week_of_month
    
   from  fiscal_calendar f
    

    UNION ALL

    SELECT
   
   
   'Retail Calendar' as CALENDAR_TYPE,
    r.date,
    r.date_key,
    r.retail_day_name,
    r.retail_date_sequence,
    r.retail_date_of_year,
    r.retail_date_of_quarter,
    r.retail_date_of_month,
    r.retail_date_of_week,    
    r.retail_year_name,
    ROUND(r.retail_year_serial,0),
    r.retail_year_sequence,
    r.retail_quarter_name,   
    r.retail_quarter_serial,
    CASE WHEN r.retail_year_sequence =1 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ))
         WHEN r.retail_year_sequence =2 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +4)
         WHEN r.retail_year_sequence =3 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' )+8 )
         WHEN r.retail_year_sequence =4 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' )+ 12)
         WHEN r.retail_year_sequence =5 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) + 16)
         WHEN r.retail_year_sequence =6 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) + 20)
         WHEN r.retail_year_sequence =7 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +24)
         WHEN r.retail_year_sequence =8 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +28)
         WHEN r.retail_year_sequence =9 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +32)
         WHEN r.retail_year_sequence =10 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +36)
         WHEN r.retail_year_sequence =11 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +40)
         WHEN r.retail_year_sequence =12 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) + 44)
         WHEN r.retail_year_sequence =13 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +48)
         WHEN r.retail_year_sequence =14 THEN TO_CHAR(REPLACE( r.retail_quarter_name, 'FQ', '' ) +52)
    
    
    
    END AS
    retail_quarter_sequence,
    REPLACE( r.retail_quarter_name, 'FQ', '' ) as retail_quarter_of_year,
    r.retail_month_name,
    CASE WHEN  r.date >= r.current_year_start_date 
         THEN CASE WHEN REPLACE( r.retail_month_name, 'FM', '' )  <=10 
                   THEN TO_CHAR(YEAR(r.date)+1) || '0' || TO_CHAR( 
                   REPLACE( r.retail_month_name, 'FM', '' ) ) 
                   ELSE TO_CHAR(YEAR(r.date)+1) || TO_CHAR(REPLACE( r.retail_month_name, 'FM', '' )) 
                   END
         
    ELSE 
      
         CASE WHEN  REPLACE( r.retail_month_name, 'FM', '' )  <10 
                   THEN TO_CHAR(YEAR(r.date)) || '0' || TO_CHAR( REPLACE( r.retail_month_name, 'FM', '' ) ) 
                   ELSE TO_CHAR(YEAR(r.date)) || TO_CHAR(REPLACE( r.retail_month_name, 'FM', '' )) 
                   END
         
         end as  retail_month_serial,
       CASE WHEN r.retail_year_sequence =1 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0))
         WHEN r.retail_year_sequence =2 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0)+12)
         WHEN r.retail_year_sequence =3 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0)+24 )
         WHEN r.retail_year_sequence =4 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0)+ 36)
         WHEN r.retail_year_sequence =5 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) + 48)
         WHEN r.retail_year_sequence =6 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) + 60)
         WHEN r.retail_year_sequence =7 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) +72)
         WHEN r.retail_year_sequence =8 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) +84)
         WHEN r.retail_year_sequence =9 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) +96)
         WHEN r.retail_year_sequence =10 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) +108)
         WHEN r.retail_year_sequence =11 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0)+120)
         WHEN r.retail_year_sequence =12 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) +132)
         WHEN r.retail_year_sequence =13 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) +144)
         WHEN r.retail_year_sequence =14 THEN TO_CHAR(ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0)+156)
    
    
    
    END AS
    retail_month_sequence,
    ROUND(REPLACE( r.retail_month_name, 'FM', '' ),0) as retail_month_of_year,    
    r.retail_month_of_quarter,   
    r.retail_week_name,
    r.retail_week_serial,
    r.retail_week_sequence,
    r.retail_week_of_year,
    r.retail_week_of_quarter,    
    r.retail_week_of_month

 from     retailcalendar r
    )
select CASE WHEN calendar_type = 'Gregorian' THEN CAST(CONCAT(date_key,1) as INT)
WHEN calendar_type = 'DUER FISCAL CALENDAR' THEN CAST(CONCAT(date_key,2) as INT)
WHEN calendar_type = 'Retail Calendar' THEN CAST(CONCAT(date_key,3) as INT) END AS date_key_2, * from cte