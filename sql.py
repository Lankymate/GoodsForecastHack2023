import gc
import datetime

import pyodbc
import numpy as np
import pandas as pd


query_features = '''
    select 
        *,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_1row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 13 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_7row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 20 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_14row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 36 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_30row_lag_avg,

        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 432 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_1row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 438 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_7row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 445 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_14row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 461 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_30row_lag_avg,

        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 228 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_1row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 234 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_7row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 241 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_14row_lag_avg,
        AVG(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 257 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_30row_lag_avg,

        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_1row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 13 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_7row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 20 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_14row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 36 PRECEDING AND 6 PRECEDING
        ) AS IsCorrect_location_product_30row_lag_std,

        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 432 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_1row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 438 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_7row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 445 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_14row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 461 PRECEDING AND 431 PRECEDING
        ) AS IsCorrect_location_30row_lag_std,

        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 228 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_1row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 234 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_7row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 241 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_14row_lag_std,
        STDEV(CAST(IsCorrect AS float)) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 257 PRECEDING AND 227 PRECEDING
        ) AS IsCorrect_product_30row_lag_std,


        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_product_1row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_product_7row_lag_count,

        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_1row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_7row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_14row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_30row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY LocationId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_location_100row_lag_count,

        COUNT(IsCorrect) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_product_1row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_product_7row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_product_14row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_product_30row_lag_count,
        COUNT(IsCorrect) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING
        ) AS IsCorrect_product_100row_lag_count,


        COUNT(LocationId) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
        ) AS LocationId_product_1row_lag_count_distinct,
        COUNT(LocationId) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LocationId_product_7row_lag_count_distinct,
        COUNT(LocationId) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LocationId_product_14row_lag_count_distinct,
        COUNT(LocationId) OVER (
            PARTITION BY ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS LocationId_product_30row_lag_count_distinct
    from 
        dbo.Features
'''
q = '''
with p as (
    SELECT
        LocationId,
        ProductId,
        ValidationDateTime,
        LastSale,
        LastPrice,
        belongs,
        AVG(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_1row_lag_avg,
        AVG(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_7row_lag_avg,
        AVG(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_14row_lag_avg,
        AVG(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_30row_lag_avg,

        SUM(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_1row_lag_sum,
        SUM(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_7row_lag_sum,
        SUM(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_14row_lag_sum,
        SUM(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_30row_lag_sum,

        STDEV(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_1row_lag_std,
        STDEV(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_7row_lag_std,
        STDEV(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_14row_lag_std,
        STDEV(CAST(LastSale AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastSale_location_product_30row_lag_std,

        AVG(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastPrice_location_product_1row_lag_avg,
        AVG(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastPrice_location_product_7row_lag_avg,
        AVG(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastPrice_location_product_14row_lag_avg,
        AVG(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastPrice_location_product_30row_lag_avg,

        CASE WHEN STDEV(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastPrice_change_location_product_1row_lag_std,
        CASE WHEN STDEV(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastPrice_change_location_product_7row_lag_std,
        CASE WHEN STDEV(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastPrice_change_location_product_14row_lag_std,
        CASE WHEN STDEV(CAST(LastPrice AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastPrice_change_location_product_30row_lag_std,

        (SUM(CASE WHEN LastSale = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING
        ) + 1) / (SUM(CASE WHEN LastSale = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) + 1) AS LastSale_1to2_row_zero_count,
        (SUM(CASE WHEN LastSale = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) + 1) / (SUM(CASE WHEN LastSale = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 8 PRECEDING
        ) + 1) AS LastSale_7to14_row_zero_count,
        (SUM(CASE WHEN LastSale = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) + 1) / (SUM(CASE WHEN LastSale = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 14 PRECEDING
        ) + 1) AS LastSale_7to30_row_zero_count

    FROM
    (
        select
            COALESCE(s.LocationId, f.LocationId) AS LocationId,
            COALESCE(s.ProductId, f.ProductId) AS ProductId,
            COALESCE(s.ValidationDateTime, f.ValidationDateTime) AS ValidationDateTime,
            s.LastSale AS LastSale,
            s.PriceSum AS PriceSum,
            s.LastPrice AS LastPrice,
            COALESCE(f.belongs, s.belongs) AS belongs
        from
        (
            SELECT
                LocationId,
                ProductId,
                Datetime as ValidationDateTime,
                CASE WHEN Quantity < 0 THEN 0 ELSE Quantity END AS LastSale,
                CASE WHEN PriceSum < 1 THEN 1 ELSE PriceSum END AS PriceSum,
                (CASE WHEN PriceSum < 0 THEN 0 ELSE PriceSum END) / (CASE WHEN Quantity < 1 THEN 1 ELSE Quantity END) as LastPrice,
                0 as belongs
            FROM dbo.LocationStateHourSales AS outerTable
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT LocationId, ProductId
                    FROM dbo.Features
                ) AS innerTable
                WHERE innerTable.LocationId = outerTable.LocationId
                AND innerTable.ProductId = outerTable.ProductId
            )
        ) AS s
        FULL OUTER JOIN
        (
            SELECT 
                LocationId,
                ProductId,
                ValidationDateTime,
                1 as belongs
            FROM dbo.Features
        ) AS f
            ON s.LocationId = f.LocationId
            AND s.ProductId = f.ProductId
            AND s.ValidationDateTime = f.ValidationDateTime
    ) as k
) 

select
    *
from
    p
where belongs = 1

'''
q1 = '''
with p as (
    SELECT
        LocationId,
        ProductId,
        ValidationDateTime,
        LastStock,
        belongs,
        AVG(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_1row_lag_avg,
        AVG(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_7row_lag_avg,
        AVG(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_14row_lag_avg,
        AVG(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_30row_lag_avg,

        SUM(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_1row_lag_sum,
        SUM(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_7row_lag_sum,
        SUM(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_14row_lag_sum,
        SUM(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_30row_lag_sum,

        STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_1row_lag_std,
        STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_7row_lag_std,
        STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_14row_lag_std,
        STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS LastStock_location_product_30row_lag_std,

        CASE WHEN STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastStock_change_location_product_1row_lag_std,
        CASE WHEN STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastStock_change_location_product_7row_lag_std,
        CASE WHEN STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastStock_change_location_product_14row_lag_std,
        CASE WHEN STDEV(CAST(LastStock AS float)) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) = 0 THEN 0 ELSE 1 END AS LastStock_change_location_product_30row_lag_std,

        (SUM(CASE WHEN LastStock = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING
        ) + 1) / (SUM(CASE WHEN LastStock = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) + 1) AS LastStock_1to2_row_zero_count,
        (SUM(CASE WHEN LastStock = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) + 1) / (SUM(CASE WHEN LastStock = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 8 PRECEDING
        ) + 1) AS LastStock_7to14_row_zero_count,
        (SUM(CASE WHEN LastStock = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) + 1) / (SUM(CASE WHEN LastStock = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY LocationId, ProductId
            ORDER BY ValidationDateTime
            ROWS BETWEEN 30 PRECEDING AND 14 PRECEDING
        ) + 1) AS LastStock_7to30_row_zero_count

    FROM
    (
        select
            COALESCE(s.LocationId, f.LocationId) AS LocationId,
            COALESCE(s.ProductId, f.ProductId) AS ProductId,
            COALESCE(f.Datetime, s.Datetime) AS Datetime,
            f.ValidationDateTime,
            s.LastStock AS LastStock,
            COALESCE(f.belongs, s.belongs) AS belongs
        from
        (
            SELECT
                LocationId,
                ProductId,
                DATEADD(DAY, 1, Datetime) as Datetime,
                CASE WHEN Quantity < 0 THEN 0 ELSE Quantity END AS LastStock,
                0 as belongs
            FROM dbo.LocationStateStocks AS outerTable
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT LocationId, ProductId
                    FROM dbo.Features
                ) AS innerTable
                WHERE innerTable.LocationId = outerTable.LocationId
                AND innerTable.ProductId = outerTable.ProductId
            )
        ) AS s
        FULL OUTER JOIN
        (
            SELECT 
                LocationId,
                ProductId,
                ValidationDateTime,
                Cast(CONVERT(varchar(8), ValidationDateTime, 112) as date) AS Datetime,
                1 as belongs
            FROM dbo.Features
        ) AS f
            ON s.LocationId = f.LocationId
            AND s.ProductId = f.ProductId
            AND s.Datetime = f.Datetime
    ) as k
) 

select
    *
from
    p
where belongs = 1

'''

if __name__ == '__main__':
    print([x for x in pyodbc.drivers()])

    conn_str = """
        Driver={ODBC Driver 17 for SQL Server};
        Server='server';
        Database='db';
        UID='participant';
        PWD='password';
    """

    conn = pyodbc.connect(conn_str)

    df = pd.read_sql(query_features, conn)
    df_sales = pd.read_sql(q, conn)
    df_stock = pd.read_sql(q1, conn)

    df.to_parquet('features', engine='fastparquet')
    df_sales.to_parquet('true_sales', engine='fastparquet')
    df_stock.to_parquet('true_stock', engine='fastparquet')
