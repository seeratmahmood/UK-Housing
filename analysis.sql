USE property;

WITH CTE AS
(SELECT *,
ROW_NUMBER() OVER (ORDER BY Mortgage_Sales_Volume DESC) AS rn
FROM sales_type
WHERE Region_Name = 'England')

SELECT Year, Quarter, Mortgage_Average_Price, Mortgage_Sales_Volume FROM CTE
WHERE rn < 11;

WITH CTE1 AS
(SELECT s.*, i.All_Loans
FROM sales_type s
LEFT JOIN interest_rates i
ON s.Year = i.Year AND s.Quarter = i.Quarter)

SELECT Year, Quarter, Mortgage_Average_Price, Mortgage_Sales_Volume, All_Loans FROM
(SELECT *, ROW_NUMBER() OVER (ORDER BY Mortgage_Sales_Volume DESC) AS r
FROM CTE1
WHERE Region_Name = 'Great Britain') sq
WHERE sq.r < 11;

