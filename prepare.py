import pandas as pd


def prep_store_data_for_exploration(df):
    df.sale_date = df.sale_date.apply(lambda date: date[:-13])
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y')
    df = df.set_index('sale_date').sort_index()
    df = df.rename(columns={'sale_amount': 'quantity'})
    df['month'] = df.index.month
    df['day_of_week'] = df.index.day_name()
    df['sales_total'] = df.quantity * df.item_price
    return df



def prep_store_data(df):
    df.sale_date = df.sale_date.apply(lambda date: date[:-13])
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y')
    df = df.set_index('sale_date').sort_index()
    df = df.rename(columns={'sale_amount': 'quantity'})
    df['month'] = df.index.month
    df['day_of_week'] = df.index.day_name()
    df['sales_total'] = df.quantity * df.item_price
    # ensure that our data is on a MONTH BY MONTH representation by agg/resample on day: sum
    df_resampled = df.resample('M')[['sales_total','quantity']].sum()
    # remove leap day for the sake of congruent seasonal analysis
    #df_resampled = df_resampled[df_resampled.index != '2016-02-29']
    return df_resampled

def prep_opsd_data(df):
    df.columns = [column.replace('+' , '_').lower() for column in df]
    #reassign the date column to a datetime type
    df['date'] = pd.to_datetime(df.date)
    # set the index to the date column
    df = df.set_index('date').sort_index()
    # add a month column using the name of the month
    df['month'] = df.index.month_name()
    # add a year column
    df['year'] = df.index.year
    #impute missing values
    df = df.fillna(0)
    # total the values of the wind column and the solar column in the wind_solar column
    df['wind_solar'] = df.wind + df.solar
    
    return df