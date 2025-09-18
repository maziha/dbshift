
from pyspark.sql.functions import row_number, when, col, lower, length, lit, rank
from pyspark.sql import Window

# Declare variables
min_amnt_bet = 500
risk_level = None  # Or assign a value if needed
location_keyword = None  # Or assign a value if needed

# Step 1: Create a temporary view
query = f"""
SELECT 
    ROW_NUMBER() OVER (ORDER BY PlayerId) AS RowNum,
    PlayerId,
    FirstName,
    Location,
    AmntBet,
    Total_No_Games,
    Time_Spent,
    Value_Of_Promotions_Used,
    Visit_Frequency_Days,
    Risk_Category,
    Age
FROM dbo.Player_360_Summary
WHERE AmntBet >= {min_amnt_bet}
  AND ({risk_level} IS NULL OR Risk_Category = '{risk_level}')
  AND ({location_keyword} IS NULL OR lower(Location) LIKE '%{location_keyword.lower()}%')
"""
spark.sql(query).createOrReplaceTempView("PlayerTemp")


# Step 2 & 3:  Processing using Spark SQL functions (no loop needed)
query = """
WITH PlayerData AS (
    SELECT 
        PlayerId,
        FirstName,
        Location,
        AmntBet,
        Total_No_Games,
        Time_Spent,
        Value_Of_Promotions_Used,
        Visit_Frequency_Days,
        Risk_Category,
        Age
    FROM PlayerTemp
),
EngagementScores AS (
    SELECT 
        *,
        IFNULL(Total_No_Games, 0) * 0.3 + IFNULL(Time_Spent, 0) * 0.4 + IFNULL(Value_Of_Promotions_Used, 0) * 0.2 - IFNULL(Visit_Frequency_Days, 0) * 0.1 AS EngagementScore,
        CASE WHEN length(regexp_replace(FirstName, '[^0-9]', '')) > 0 THEN 1 ELSE 0 END AS NameHasDigits,
        CASE 
            WHEN '{location_keyword}' IS NOT NULL AND lower(IFNULL(Location, '')) LIKE '%' || lower('{location_keyword}') || '%' THEN 1 
            ELSE 0 
        END AS LocContainsKeyword,
        CAST(Age AS INT) AS AgeInt
    FROM PlayerData
),
PlayerTiers AS (
  SELECT 
        *,
        CASE 
            WHEN EngagementScore >= 80 AND IFNULL(Visit_Frequency_Days, 999) <= 5 AND NameHasDigits = 0 AND AgeInt BETWEEN 25 AND 45 THEN 'Gold'
            WHEN EngagementScore >= 50 AND IFNULL(Visit_Frequency_Days, 999) <= 10 AND LocContainsKeyword = 1 THEN 'Silver'
            WHEN EngagementScore >= 30 AND IFNULL(AgeInt, 0) < 25 THEN 'Bronze'
            ELSE 'Newbie'
        END AS PlayerTier
    FROM EngagementScores
)
SELECT 
    PlayerId,
    PlayerTier,
    EngagementScore,
    Risk_Category,
    NameHasDigits,
    LocContainsKeyword,
    AgeInt,
    RANK() OVER (ORDER BY EngagementScore DESC, PlayerId) AS PlayerRank
FROM PlayerTiers
ORDER BY PlayerRank, PlayerTier DESC, EngagementScore DESC

"""
spark.sql(query).show()

#End_DBShift
