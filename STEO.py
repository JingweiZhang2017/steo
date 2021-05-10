#!/usr/bin/python3
import os, sys
# import urllib
import urllib.request
import datetime
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
import numpy as np
np.seterr(divide='ignore', invalid='ignore')
from math import sqrt

# Series of Interest 
code_dict = [{'code':'WTIPUUS','description':'West Texas Intermediate Spot Average', 'category': 'Crude oil'},
#             {'code':'BREPUUS','description':'Brent Spot Average', 'category': 'Crude oil'},
             {'code':'MGWHUUS','description':'Gasoline', 'category': 'Liquid fuels – Refiner prices for resale'},
             {'code':'DSWHUUS','description':'Diesel Fuel', 'category': 'Liquid fuels – Refiner prices for resale'},
             {'code':'D2WHUUS','description':'Fuel Oil', 'category': 'Liquid fuels – Refiner prices for resale'},
             {'code':'MGRARUS','description':'Gasoline Regular Grade', 'category': 'Liquid fuels – Retail prices including Taxes'},
             {'code':'MGEIAUS','description':'Gasoline All Grades', 'category': 'Liquid fuels – Retail prices including Taxes'},
             {'code':'DSRTUUS','description':'On-highway Diesel Fuel', 'category': 'Liquid fuels – Retail prices including Taxes'},
             {'code':'D2RCAUS','description':'Heating Oil', 'category': 'Liquid fuels – Retail prices including Taxes'},
             {'code':'NGHHMCF','description':'Henry Hub Spot', 'category': 'Natural gas'},
             {'code':'NGRCUUS','description':'Retail prices – residential sector', 'category': 'Natural gas'},
             {'code':'ESRCUUS','description':'Retail prices – residential sector', 'category': 'Electricity'}
            ]

target_df = pd.DataFrame(code_dict)
month_list = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
end_year = datetime.datetime.now().year - 2000
end_month_index = datetime.datetime.now().month
year_list = [str(i) if i >9 else ('0' + str(i)) for i in list(range(5,(end_year + 1)))]
year_list.reverse()

def format_month_str(month):
    'month -  string in month list'
    month_int = str(month_list.index(month)+1)
    if len(month_int) == 1:
        month_int = '0' + month_int
    return month_int


def download_excel_data(outdir):
    
    '''
    This fucntion extracts the short-Term Energy Outlook monthly updates excel tables since 2005 from https://www.eia.gov/outlooks/steo/outlook.php#issues2021 to the defined local path (outdir)
    It can be re-run to extract the newly-released tables 
    
    '''

    file_count = 0
    for year in year_list:
        if year == str(end_year):
            # tables till the most recent month available for the current year
            month_list_year = month_list[:end_month_index]
        else:
            # tables of 12 months for previous years
            month_list_year = month_list
        for month in month_list_year:
            
            month_int = format_month_str(month)
            # data before 201306 are xls files while after it are xlsx files
            if (int(year) == 13 and month_list.index(month)<=5) or (int(year) < 13):
                excel_format = 'xls' 
            else:
                excel_format = 'xlsx'
            dls = "https://www.eia.gov/outlooks/steo/archives/%s%s_base.%s"%(month, year, excel_format)
            source_file_name = dls.split('/')[-1]
            target_file_name = '20'+ year + month_int + '.' + excel_format
            file_name_full = outdir + '/' + target_file_name
            if os.path.isfile(file_name_full) == False: 
#                 urllib.request.urlretrieve(dls, file_name_full)
                response = urllib.request.urlopen(dls)
                table = response.read()
                with open(file_name_full, "wb") as file:
                     file.write(table)
                
                if year == str(end_year):
                    try:
                        test_xlsx = pd.ExcelFile(file_name_full)
                        print('%s has been saved as %s'%(source_file_name, target_file_name))
                        file_count += 1
                    except:
                        logging.warning('%s is NOT ready yet'%source_file_name)
                        os.remove(file_name_full)
                else:
                    print('%s has been saved as %s'%(source_file_name, target_file_name))
                    file_count += 1
                
                
    logging.info('%d new files are created in the directory %s'%(file_count, outdir))


def extract_target_series_since0710(path, file_name):
    
    '''
    This function extracts rows of actual value and predictions of the target series in the input file.
    Target series are series of interest defined in the dictionary code_dict (such as 'WTIPUUS')
    
    Input:       string file names like '0710.xls' 
    Output:      a dictionary with KEYs as codes of price of interest and VALUEs as price value series indexed by time codes ('200810' means 'October of 2008'), the example is shown as follow:
  {'WTIPUUS':
    200301    32.96
    200302    35.83
    200303    33.51
    200304    28.17
    200305    28.11
            ...  
    200808       72
    200809       73
    200810       72
    200811     71.5
    200812       71,
   'NGHHMCF':
            ...
    }
    
    '''
    xls = pd.ExcelFile(path + '/' + file_name)
    file_index = file_name.split('.')[0]
    df = pd.read_excel(xls, '2tab')

    
    df.loc[1] = df.loc[1].fillna(method='ffill')
    df.at[1, 'Table of Contents'] = 'Year'
    df.at[2, 'Table of Contents'] = 'Month'
    df = df.set_index('Table of Contents').T
#     cols = ['Year', 'Month'] + target_df.code.tolist()
    df = df.reset_index().drop('index', axis = 1).drop(0)

    df['time_index'] = df.apply(lambda x: str(int(x.Year)) + format_month_str(x.Month.lower()), axis =1)
    df = df.set_index('time_index')
    price_dict = dict()
    pct_dict = dict()
    for target_price in target_df.code.tolist():
        price_dict[target_price] = df[target_price].rename(file_index)
        pct_dict[target_price] = price_dict[target_price].pct_change()
    return price_dict, pct_dict

def get_pred_matrix (series_df, target_price, end_year, start_year = '200810'):
    
    '''
    This function transforms the price series into a matrix (dataframe format) with  rows as time period (like 0810,0811 ...) 
    and columns as prices, the first column is the actual price followed by predicted prices 1 month up to 12 months before. 
    
         actual_price  pred_1month  pred_2month ......
    0810      76           80          110
    0811
    0812
    ......
    
    Input:  
    
    traget_price:  target Price code (e.g. 'WTIPUUS')
    end_year:      one year before the most recent year
    start_year:     '0810' as the earlies
    
    
    '''
    
    yy_matrix = pd.concat(series_df[target_price].tolist(),axis =1,sort=True)
    year_list = yy_matrix.columns.tolist()
    target_year_list = year_list[year_list.index(start_year):(year_list.index(end_year) + 1)]
    matrix_list = list()
    for target_year in target_year_list:
        year_list = yy_matrix.columns.tolist()
        t_index = year_list.index(target_year)
        cols = year_list[t_index-11:t_index+2]
        t_price_df = yy_matrix.loc[target_year][cols].tolist()
        t_price_df.reverse()
        matrix_list.append(t_price_df)
    matrix_df = pd.DataFrame(matrix_list, index = target_year_list)
    matrix_df.columns = ['actual'] + ['pred_%smonth'%str(i+1) for i in range(12)]
    return matrix_df

def transform_data(data_path, output_path, save = True):
    file_name_list = sorted([file for file in os.listdir(data_path) if file.split('.')[1].startswith('xls') and int(file.split('.')[0]) >= int('200710')])
    price_dict_list = list()
    pct_dict_list = list()
    for file_name in file_name_list:
        price_dict, pct_dict = extract_target_series_since0710(data_path, file_name)               
        price_dict_list.append(price_dict)
        pct_dict_list.append(pct_dict)
    price_series_df = pd.DataFrame(price_dict_list)
    pct_series_df = pd.DataFrame(pct_dict_list)
                            
    price_matrix_dict = dict()
    pct_matrix_dict = dict()
    for target_price in target_df.code.tolist():
        price_matrix_dict[target_price] = get_pred_matrix(price_series_df, target_price, end_year = file_name_list[-2].split('.')[0])
        pct_matrix_dict[target_price] = get_pred_matrix(pct_series_df, target_price, end_year = file_name_list[-2].split('.')[0])
        if save == True:
            price_matrix_dict[target_price].to_csv(output_path + '/price_%s.csv'%target_price)
            print('price_%s.csv is created'%target_price)
            pct_matrix_dict[target_price].to_csv(output_path + '/pct_%s.csv'%target_price)
            print('pct_%s.csv is created'%target_price)
    return price_matrix_dict, pct_matrix_dict

#   Forecast Evaluation  
def get_evaluation(matrix_dict, eval_path, dict_type):
    '''
    dict_type: 'price' (US dollar forecasts) or 'pct' (pct change forecasts)
    '''
    
    def mean_absolute_percentage_error(y_true, y_pred): 
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

    def absolute_errors(y_true, y_pred): 
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        return np.abs(y_true - y_pred)
    
    def get_metric_df(metric_series):
        df = pd.DataFrame(metric_series, columns = pred_cols)
        df.index = target_df.code.tolist()
        return df
    
    mae = list()
    rmse = list()
    mape = list()
    ae_dict = dict()
    ape_dict = dict()
    actual_list = list()
    for target_price in target_df.code.tolist():
        matrix_df = matrix_dict[target_price]
        y_true = matrix_df['actual']
        actual_list.append(y_true.rename(target_price))
        pred_cols = matrix_df.columns.tolist()[1:]
        t_mae = list()
        t_rmse = list()
        t_mape = list()
        ae_df = pd.DataFrame()
        ape_df = pd.DataFrame()
        for pred_col in pred_cols:
            y_pred = matrix_df[pred_col]
            t_rmse.append(sqrt(mean_squared_error(y_true, y_pred)))
            t_mae.append(mean_absolute_error(y_true, y_pred))
            t_mape.append(mean_absolute_percentage_error(y_true, y_pred))
            ae_df[pred_col.split('_')[1]] = absolute_errors(y_true, y_pred)
            ape_df[pred_col.split('_')[1]] = absolute_errors(y_true, y_pred)/y_true
        ae_dict[target_price] = ae_df 
        ape_dict[target_price] = ape_df 
        mae.append(t_mae)
        rmse.append(t_rmse)
        mape.append(t_mape)
    actual_df = pd.DataFrame(actual_list)
    mae_df = get_metric_df(mae)
    rmse_df = get_metric_df(rmse)
    mae_df.to_csv(eval_path + '/mae_%s.csv'%dict_type)
    rmse_df.to_csv(eval_path + '/rmse_%s.csv'%dict_type)
    if dict_type == 'price':
        get_metric_df(mape).to_csv(eval_path + '/mape_%s.csv'%dict_type)
    return actual_df, ae_dict, ape_dict


def main(argv):
    # create a data directory in your local
    paths = [os.getcwd() + path for path in ['/data','/pred','/eval']]
    def makedir(path):
        if os.path.exists(path) == False: 
            os.mkdir(path)
    for path in paths:
        makedir(path)
    logging.info('DOWNLOADING forecast tables to %s'%paths[0])
    # download excel tables to the defined directory
    download_excel_data(outdir = paths[0])
    logging.info('TRANSFORMING tables into X-month ahead forecasts under %s'%paths[1])
    # for each series of interest, transform the forecasts into matrix with rows denoting the reference period while the columns denoting the relevant forecast publication period (vintage).
    price_matrix_dict, pct_matrix_dict = transform_data(paths[0],paths[1])
    # create evaluations in metrics like mae, rmse and mape
    price_actual_df, price_ae_dict, price_ape_dict = get_eval(price_matrix_dict, paths[2], 'price')
    pct_actual_df, pct_ae_dict, pct_ape_dict = get_eval(pct_matrix_dict, paths[2], 'pct')
        
    


if __name__ == "__main__":
    main(sys.argv)