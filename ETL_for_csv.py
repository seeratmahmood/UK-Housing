import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import datetime as dt

class ETL:
    def find_filenames(path_to_dir: str, suffix=".xlsx" ):
        """_summary_
            Find the files in a folder with a particular suffix.
        Args:
            path_to_dir (str): the path of the folder
            suffix (str, optional): the suffix of the files we are looking for. Defaults to ".xlsx".

        Returns:
            _type_: list of filenames in the folder
        """
        #Find the files in a folder with a particular suffix.
        filenames = listdir(path_to_dir)
        return sorted([filename for filename in filenames if filename.endswith( suffix )])

    def extract(file_name: str):
        """_summary_
            Extract data from a file.
        Args:
            file_name (str): the file

        Returns:
            _type_: the data
        """
        if file_name.split('.')[-1] == 'csv':
            return pd.read_csv(file_name)
        else:
            pass

    def select_cols(df, cols: list):
        """_summary_
            Filter dataframe by columns.
        Args:
            df (_type_): dataframe
            cols (list): list of column names

        Returns:
            _type_: new dataframe
        """
        return df[cols]
    
    def rename(dataframe, new_names: dict):
        """_summary_
            Rename the columns in the dataframe.
        Args:
            dataframe (_type_): dataframe
            new_names (dict): dictionary with current column names as keys and new column names as values.

        Returns:
            _type_: dataframe with new column names
        """
        
        dataframe.rename(
            new_names,
            inplace=True
        )
        return dataframe
    
    def years(df, yr: int):
        """_summary_
            Filter dataframe by year
        Args:
            df (_type_): dataframe
            yr (int): year

        Returns:
            _type_: new dataframe
        """
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
        #df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        #Filter the dataframe by year
        return df[df['Date'].dt.year >= yr]
    
    def group_1(df, prices: list):
        """_summary_
            Group the '1. Average-prices' csv file by quarter.
        Args:
            df (_type_): dataframe
            prices (list): list of columns with Average_Price in the name

        Returns:
            _type_: new dataframe
        """
        
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

        # Create a new column 'Quarter' to represent the quarter of each date
        df['Year'] = df['Date'].dt.year
        df['Quarter'] = df['Date'].dt.quarter

        # Group the data by 'Quarter', 'Region_Name' and calculate the mean for each group
        result = df.groupby(['Year', 'Quarter', 'Region_Name', 'Area_Code']).agg({
            prices[0]: 'mean',
            prices[1]: 'mean',
            prices[2]: 'mean',
            prices[3]: 'mean'
        }).reset_index()
        
        return result
        
    def group_others(df, price: list, sales: list):
        """_summary_
            Group the other two csv files by quarter.
        Args:
            df (_type_): dataframe
            price (list): list of columns with Average_Price in the name
            sales (list): list of columns with Sales_Volume in the name

        Returns:
            _type_: new dataframe
        """
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

        # Create a new column 'Quarter' to represent the quarter of each date
        df['Year'] = df['Date'].dt.year
        df['Quarter'] = df['Date'].dt.quarter

        # Group the data by 'Quarter', 'Region_Name' and calculate the mean for each group
        result = df.groupby(['Year', 'Quarter', 'Region_Name', 'Area_Code']).agg({
            price[0]: 'mean',
            sales[0]: 'sum',
            price[1]: 'mean',
            sales[1]: 'sum'
        }).reset_index()
        
        return result

    def combined_1(df, prices:list):
        """_summary_
            Find overall average price.
        Args:
            df (_type_): dataframe
            prices (list): list of columns with Average_Price in the name

        Returns:
            _type_: new dataframe
        """
        df['Combined_Average_Price'] = round(df[prices].mean(axis = 1), 2)
        return df
        
    def combined_others(df, prices: list, sales: list):
        """_summary_
            Find overall average price and sales volume.
        Args:
            df (_type_): dataframe
            price (list): list of columns with Average_Price in the name
            sales (list): list of columns with Sales_Volume in the name

        Returns:
            _type_: new dataframe
        """
        df['Combined_Average_Price'] = round(df[prices].mean(axis = 1), 2)
        df['Total_Sales'] = df[sales].sum(axis = 1)
        return df
        
    def load(targetfile: str, data_to_load):
        """_summary_
            Load the dataframe into a csv file.
        Args:
            targetfile (str): path of file
            data_to_load (_type_): dataframe
        """
        data_to_load.to_csv(targetfile, index=False)

    def ETL_process():
        """_summary_
            The whole ETL process.
        """
        path = os.path.abspath('raw_data')
        files = ETL.find_filenames(path, '.csv')

        for file in files:
            #extract the data
            try:
                path = os.path.abspath('raw_data') + '\\' + file
                df = ETL.extract(path)
            except FileNotFoundError:
                path = os.path.abspath('raw_data') + "/" + file
                df = ETL.extract(path)
                
            #Find list of columns with Average_Price in the column name
            price_cols = [col for col in df.columns if 'Average_Price' in col]
            
            #Find list of columns with Sales_Volume in the column name
            vol_cols = [col for col in df.columns if 'Sales_Volume' in col]
            
            #Filter the dataframe by the columns
            all = ['Date', 'Region_Name', 'Area_Code'] + price_cols + vol_cols
            df2 = df.filter(all)
            
            #Filter the dataframe by year
            df2 = ETL.years(df2, 2007)

            #Group data into year quarters
            try:
                df2 = ETL.group_1(df2, price_cols)
                df2 = ETL.combined_1(df2, price_cols)
            except:
                df2 = ETL.group_others(df2, price_cols, vol_cols)
                df2 = ETL.combined_others(df2, price_cols, vol_cols)
            
            #Load the data into csv file
            try:
                path = os.path.abspath('clean_data') + '/' + f"{file}"
                ETL.load(path, df2)
            except FileNotFoundError:
                path = os.path.abspath('clean_data') + '\\' + f"{file}"
                ETL.load(path, df2)
    
ETL.ETL_process()