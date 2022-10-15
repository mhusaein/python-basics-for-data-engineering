import glob
import datetime as dt
import pandas as pd

def log(message):
    timestamp_format = '%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = dt.datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + ('\n'*2))

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process,encoding='unicode_escape',engine='python',skiprows=1)
    return df

def pload_cond(a):
    if float(a) > (142*1.18) : a = 'Over Load'
    elif float(a) < (130) : a='Under Load'
    else : a='In Range'
    return a

def apply_shift(z):
    if z.hour >= 8 and  z.hour < 20 :
        z = 'Day'
    else : z = "Night"
    return z

def extract():
    extracted_data = [extract_from_csv(csv) for csv in glob.glob(pathname='C:/Hussein/Ops Data/VIMS/*.csv')]
    return extracted_data

def transform_trupl(df_to_transform):
    #Get trupl df from extracted data
    data = df_to_transform[3]

    #Rename Columns
    data.columns = ['Time/Date', 'SMH', 'Eq_id', 'Pyld_t',
       'Travel_empty_t_s', 'Travel_empty_d_m',
       'Stopped_empty_t_s', 'Load_t_s',
       'Stopped_loaded_t_s', 'Loaded_travel_t_s',
       'Loaded_travel_d_m', 'Cycle_t_s',
       'Cycle_d_m', 'Loader_passes', 'Fuel_used_l']

    #Drop Empty rows and change column's types:
    data.dropna(inplace=True)
    data = data.astype({'Time/Date':'datetime64[ns]', 'Eq_id':'category', 'Loader_passes':'int', 'Fuel_used_l':'float32', 'SMH':'int', 'Pyld_t':'float32'})   

    #Convert Duration Columns into Seconds:
    dur_cols = [i for i in data.columns if '_t_s' in i]
    print('duration columns :',dur_cols)
    data[dur_cols] = data[dur_cols].apply(pd.to_timedelta).astype('timedelta64[s]').astype('int')

    #Convert miles into Km:
    dist_cols = [i for i in data.columns if '_d_m' in i]
    print('distance columns :',dist_cols)
    data[dist_cols] = data[dist_cols].astype('float32').apply(lambda x: x*1.6093)

    #Convertions on seperate columns:
    data['Date'] = data['Time/Date'].map(lambda x:x-dt.timedelta(days=1) if ( x.hour >= 0 and  x.hour < 8 ) else x).dt.date
    data['Shift'] = data['Time/Date'].dt.time.map(apply_shift).astype('category')
    data['Speed_w_stopped'] = ((data['Cycle_d_m']) / ((data['Cycle_t_s'])/60/60)).astype('float32')
    data['Speed_wo_stopped']=((data['Cycle_d_m']) / ((data['Cycle_t_s'] - data['Stopped_empty_t_s'] - data['Stopped_loaded_t_s'])/60/60)).astype('float32')
    data['Pyld_t'] = data['Pyld_t'] *0.9071
    data['Pyld_BCM'] = data['Pyld_t'] / 2.5
    data['Condition'] = data['Pyld_t'].map(pload_cond).astype('category')
    data['Fuel_used_l'] = data['Fuel_used_l'] * 4.546
    data['Hour'] = data['Time/Date'].dt.time.astype('string')

    #Drop Time/Date columns:
    data.drop('Time/Date', axis=1, inplace=True)

    #Reorder columns:
    data = data.reindex(columns=(data.columns[x] for x in [14,15,20,1,0,2,18,3,4,5,6,7,8,9,10,11,12,13,19,16,17]))

    data_cleaned, data_ol = outlier(data,['Pyld_t','Cycle_d_m','Cycle_t_s','Loaded_travel_d_m','Travel_empty_d_m','Travel_empty_t_s'])

    print(data_cleaned.describe())

    print('Removed {} rows, {}% of data'.format( data_ol.shape[0], round((data_ol.shape[0] / data.shape[0]) * 100,2)))
    
    print("Stats:\nOriginal: {}, Cleaned: {}, Outliers: {}, diff: {}".format(data.shape[0], data_cleaned.shape[0], data_ol.shape[0], data.shape[0]-data_cleaned.shape[0]-data_ol.shape[0] ))

    return data, data_cleaned, data_ol

def outlier(df_to_clean,cols_to_clean):
    df_outlier = pd.DataFrame(columns=df_to_clean.columns)
    outlier_count = df_outlier.shape[0]
    df_count = df_to_clean.shape[0]
    print("..... df count: {} .....".format(df_count))

    for i in range(len(cols_to_clean)):
        print("\n***** try: {} *****".format(i+1))
        lowerfence = df_to_clean[cols_to_clean[i]].quantile(0.0029)
        upperfence = df_to_clean[cols_to_clean[i]].quantile(0.9999)
        print("Cleaning On: {} \nupperfence = {} \nlowerfence = {}" .format(cols_to_clean[i], upperfence, lowerfence))
        df2 = df_to_clean.loc[(lowerfence > df_to_clean[cols_to_clean[i]]) | (df_to_clean[cols_to_clean[i]] > upperfence)]
        df_to_clean = df_to_clean.loc[(lowerfence <= df_to_clean[cols_to_clean[i]]) & (df_to_clean[cols_to_clean[i]] <= upperfence)]
        df_outlier = pd.concat([df_outlier,df2], axis=0)

        outlier_count = outlier_count + df2.shape[0]
        print("Cumulative_outliers: {} , %: {}, Count_outliers: {}".format(outlier_count,round((outlier_count/df_count)*100,2),df2.shape[0]))

    return df_to_clean, df_outlier
    
def load(data_to_load,destination):
    data_to_load.to_csv(destination, index = False)

log('Extract Phase Started')
dfs = extract()
log('Extract Phase Ended')

log('Transform Phase Started')
log('Transforming trupl....')
trupl, trupl_cleaned, trupl_ol = transform_trupl(dfs)
log('Transform trupl Ended')

log('Load Phase Started')
load(trupl,'C:/Hussein/Ops Data/VIMS//Export/trupl.csv')
load(trupl_cleaned,'C:/Hussein/Ops Data/VIMS//Export/trupl_cleaned.csv')
load(trupl_ol,'C:/Hussein/Ops Data/VIMS//Export/trupl_ol.csv')
log('Load Phase Ended')