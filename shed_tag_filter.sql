------------------------------------------------------------------------------------------------------------------------------
-- Author: Paul Carvalho (paul.carvalho@noaa.gov)
-- 
-- Description: Shed tag filter for detections table. 
------------------------------------------------------------------------------------------------------------------------------

-- 1. Find tagcodes with > 20000 detections and > 20 days between first and last detection at a particular general_location (15 sec runtime) ------------------------------
SELECT TagCode, general_location, n = COUNT(*), max = MAX(DateTime_PST), min = MIN(DateTime_PST), days = DATEDIFF(day, MIN(DateTime_PST) , MAX(DateTime_PST)) -- n_detects = number of detections for each tagcode at a general_location; duration = number of days between the first and last detections
INTO #tmp_table1 -- insert into a temporary table for later use
FROM detects_with_locations -- use this View 
GROUP BY TagCode, general_location; -- aggregate by tagcode and general_location

SELECT TagCode, general_location, n, max, min, days
INTO #tmp_table2 -- insert into another temporary table for later use
FROM #tmp_table1
WHERE (n > 20000 AND days > 20); -- only get tagcode + general location when the number of detections is > 20000 and days between first/last detections is 20

-- NOTE: remove #tmp_table1 here
DROP TABLE #tmp_table1;

-- -- 2. Record information for shed tags identified in step 1. IMPORTANT NOTE: This step may not be necessary.
-- BEGIN TRANSACTION
-- INSERT INTO shed_tags (TagCode, general_location, n, max, min, days)
-- SELECT TagCode, general_location, n, max, min, days
-- FROM #tmp_table2
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM shed_tags
--     WHERE shed_tags.TagCode = #tmp_table2.TagCode
--     AND shed_tags.general_location = #tmp_table2.general_location
--     AND shed_tags.n = #tmp_table2.n
--     AND shed_tags.max = #tmp_table2.[max]
-- )
-- OPTION(USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'))
-- COMMIT TRANSACTION;

-- 2. Get all of the detections for shed tags identified in #tmp_table2 and insert into a temporary table (~6 sec runtime) -----------
SELECT recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename, general_location 
INTO #tmp_table3
FROM detects_with_locations d
WHERE EXISTS(
            SELECT 1
            FROM #tmp_table2
            WHERE #tmp_table2.TagCode = d.TagCode AND
            #tmp_table2.general_location = d.general_location
);

-- DROP temporary table 2
DROP TABLE #tmp_table2;

-- 3. Save the first 1000 detections for a shed tag code (<1 sec runtime) ------------------------------------------------------------
WITH add_row_number AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY TagCode ORDER BY DateTime_PST) AS row_number
    FROM #tmp_table3
)
SELECT recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename, general_location, row_number
INTO #tmp_table4
FROM add_row_number
WHERE row_number < 1001;

-- 4. Remove all shed tag detections (~2 min runtime with shed tag code) -------------------------------------------------------------
UPDATE detects_tmp
SET shed_tag = 'Y'
WHERE EXISTS(
    SELECT 1
    FROM #tmp_table3
    WHERE #tmp_table3.TagCode = detects_tmp.TagCode
    AND #tmp_table3.recv_ID = detects_tmp.recv_ID
    AND #tmp_table3.DateTime_PST = detects_tmp.DateTime_PST
);

DELETE FROM detects_tmp
WHERE shed_tag = 'Y';

-- 5. Add first 100 detections back in the table -------------------------------------------------------------------------------------
INSERT INTO detects_tmp (recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename)
SELECT recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename
FROM #tmp_table4 tmp;

-- Drop temporary tables
DROP TABLE #tmp_table3;
DROP TABLE #tmp_table4;

-- -- EXTRA CODE USED FOR TESTING
-- -- replace rows that were removed during testing
-- INSERT INTO detects_tmp (recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename)
-- SELECT recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename
-- FROM detects
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM detects_tmp dt
--     WHERE dt.recv_ID = detects.recv_ID
--     AND dt.TagCode = detects.TagCode
--     AND dt.DateTime_Orig = detects.DateTime_Orig
--     AND dt.DateTime_PST = detects.DateTime_PST
--     AND dt.Temp = detects.Temp
--     AND dt.filename = detects.filename
-- );