
DROP DATABASE IF EXISTS property;

CREATE DATABASE IF NOT EXISTS property;
USE property;
SELECT 'CREATING DATABASE STRUCTURE' as 'INFO'; 
DROP TABLE IF EXISTS property_type,
sales_type,
property_age,
inflation,
interest_rates;

CREATE TABLE property_type (Year INT, Quarter INT, Region_Name VARCHAR(16), Area_Code CHAR(8), Detached_Average_Price FLOAT,Semi_Detached_Average_Price FLOAT,Terraced_Average_Price FLOAT,Flat_Average_Price FLOAT,Combined_Average_Price FLOAT,
PRIMARY KEY (Year, Quarter, Area_Code)
);
CREATE TABLE sales_type ( Year INT, Quarter INT, Region_Name VARCHAR(16), Area_Code CHAR(8), Cash_Average_Price FLOAT,Cash_Sales_Volume INT, Mortgage_Average_Price FLOAT, Mortgage_Sales_Volume INT, Combined_Average_Price FLOAT, Total_Sales INT,
FOREIGN KEY (Year, Quarter, Area_Code) REFERENCES property_type(Year, Quarter, Area_Code)  ON UPDATE CASCADE ON DELETE CASCADE 
);
CREATE TABLE property_age ( Year INT, Quarter INT, Region_Name VARCHAR(16), Area_Code CHAR(8), New_Build_Average_Price FLOAT, New_Build_Sales_Volume FLOAT, Existing_Property_Average_Price FLOAT, Existing_Property_Sales_Volume FLOAT, Combined_Average_Price FLOAT, Total_Sales FLOAT,
FOREIGN KEY (Year, Quarter, Area_Code) REFERENCES property_type(Year, Quarter, Area_Code)  ON UPDATE CASCADE ON DELETE CASCADE 
);
CREATE TABLE interest_rates (
Year INT, Quarter INT, All_Loans FLOAT,
FOREIGN KEY (Year, Quarter) REFERENCES property_type(Year, Quarter) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE inflation (
Year INT, Quarter INT, CIPH_Rate FLOAT,
FOREIGN KEY (Year, Quarter) REFERENCES property_type(Year, Quarter) ON UPDATE CASCADE ON DELETE CASCADE
);

LOAD DATA LOCAL INFILE 'C:\\Users\\saisagbon\\propertyproject\\clean_data\\1. Average-prices-Property-Type-2023-06.csv'
INTO TABLE property_type
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:\\Users\\saisagbon\\propertyproject\\clean_data\\2. New-and-Old-Sales 2023-06.csv'
INTO TABLE property_age
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:\\Users\\saisagbon\\propertyproject\\clean_data\\3. Cash-mortgage-sales-2023-06.csv'
INTO TABLE sales_type
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:\\Users\\saisagbon\\propertyproject\\clean_data\\4. BOE interest rates.csv'
INTO TABLE interest_rates
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:\\Users\\saisagbon\\propertyproject\\clean_data\\5. Annual CPIH inflation rates.csv'
INTO TABLE inflation
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;