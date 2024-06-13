# import sys;sys.path.insert(1,'/Users/administrador/Documents/DataScience/Projects/Finviz')
import os
import pandas as pd
from datetime import datetime
from finviz_utils import (
    get_filters,
    get_dataframe_by_industry,
    get_dataframe_by_sector,
    get_dataframe_by_index,
)
from finviz_utils.constants import (
    CUSTOM_TABLE_ALL_FIELDS,
)
from dateutil.relativedelta import relativedelta
from earnings_calendar.constants import (
    EARNINGS_CALENDAR_FOLDER,
    TRACKED_INDUSTRIES,
    API_KEY,
    DATA_FOLDER,
    FINVIZ_RAW_DATA_FOLDER,
    FINVIZ_DATA_CALENDAR_FOLDER,
    INCLUDE_COLUMNS,
)


class MasterEarningsCalendar:

    @classmethod
    def get_whole_earnings_calendar(cls, 
                                    csv=False, 
                                    horizon='12month', 
                                    local_file='from-Feb2023EarningsCalendar.csv'):
        """
        doc: https://www.alphavantage.co/documentation/#earnings-calendar
        This functions makes a csv requests and transform the csv into a dataframe.
        to produce csv output mark csv as True 
        horizons = default 3months, choices=6month,12month

        """
        earnings_calendar_folder = EARNINGS_CALENDAR_FOLDER
        URL = 'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&apikey={}'.format(
            API_KEY)
        if csv:
            file_path = f'{earnings_calendar_folder}/{local_file}'
            data = pd.read_csv(file_path)
            data['reportDate'] = pd.to_datetime(data['reportDate'])
            data['days_left'] = data['reportDate'].apply(cls._compute_days_left)
            data = data.set_index('reportDate', drop=True)
            return data

        else:
            if horizon:
                URL += '&horizon={}'.format(horizon)
            data = pd.read_csv(URL)
            data['reportDate'] = pd.to_datetime(data['reportDate'])
            data['days_left'] = data['reportDate'].apply(
                cls._compute_days_left)
            data = data.set_index('reportDate', drop=True)

        return data.sort_index()

    @classmethod
    def update_local_earnings_calendar(cls, local_name='from-Feb2023EarningsCalendar.csv'):

        earnings_calendar_folder = EARNINGS_CALENDAR_FOLDER

        local = cls.get_whole_earnings_calendar(csv=True)
        web = cls.get_whole_earnings_calendar(csv=False)

        # Find the last date on the dataframe and add grab information
        # on the request from it 
        next_day = local.index[-1] + relativedelta(days=1)
        new = []
        for i in range(5):
            if not len(new):
                print(
                    f'Unable to find new data on {str(next_day)}, trying with the next day')
                next_day = next_day + relativedelta(days=1)
                new = web.loc[str(next_day):]
            else:
                break

        merged = pd.concat([local, new])
        print(local.info())
        print(merged.info())
        merged.to_csv(f'{earnings_calendar_folder}/{local_name}')
        print('File updated successfully')

        return merged
    
    @classmethod
    def get_local_earnings_calendar(cls, to_excel=False):
        local_earning_calendar = cls.get_whole_earnings_calendar(csv=True)
        if to_excel:
            local_earning_calendar.to_excel("output.xlsx", sheet_name='EarningsCalendar')
            return
        return local_earning_calendar

    @classmethod
    def _compute_days_left(cls, earnings_date):

        today = datetime.today()
        diff = earnings_date - today
        return int(diff.days)
    
    def get_earning_anomalies_for_ticker(self, ticker, price_history):
        pass

class FinvizDataCalendarGenerator:

    @classmethod
    def filters(cls, *args, **kwargs):
        return get_filters(*args, **kwargs)

    @classmethod
    def get_finviz_data_by(cls,
                           sector=None,
                           industry=None,
                           index=None,
                           table='Performance', 
                           details=True,
                           raw=False,
                           save=False):
        """
        source  <- append the source of the data to the Df, only
                   compatible when industry is true
        save    <- store it as a csv in the current directory
        raw     <- Get data from Finviz without appending it to the
                   calendar
        table   <- You can see more table formats in Finviz but only support 
                   Prformance and Overview
        """
        
        if industry and not index and not sector:
            finviz_data = get_dataframe_by_industry(
                industry, 
                details=details, 
                table=table)
            sub_folder = 'industries'
            tag = industry
        elif sector and not index and not industry:
            finviz_data = get_dataframe_by_sector(
                sector, 
                details=details, 
                table=table)
            sub_folder = 'sectors'
            tag = sector 
        elif index and not sector and not industry:
            finviz_data = get_dataframe_by_index(
                index, 
                details=details, 
                table=table)
            sub_folder = 'indexes'
            tag = index
        else:
            raise Exception(
                'You can only pass sector, industry, or index not several of them')
        if raw:
            if save:
                cls.store_data(local=True, 
                               sub_folder=sub_folder, 
                               data=finviz_data, 
                               raw=True, 
                               tag=tag)
            return finviz_data
        else:
            # file_path=False makes an API call
            earnings_calendar = cls.get_earning_calendar_for(
                finviz_data.T.index)
            finviz_calendar = cls.prepare_finviz_calendar(
                earnings_calendar, finviz_data, table, industry=industry, index=index)

            if save:
                cls.store_data(local=True, 
                               sub_folder=sub_folder,
                               data=finviz_calendar, 
                               raw=False,
                               tag=tag)

            return finviz_calendar

    @classmethod
    def gel_all_tracked_industries(cls, 
                                   table='Performance', 
                                   raw=False, 
                                   scope='all'):
        if scope == 'all':
            all_tracked_industries = []
            for industry in TRACKED_INDUSTRIES:
                print('Updating {}'.format(industry))
                industry_data = cls.get_finviz_data_by(industry=industry, 
                                                       table=table, 
                                                       raw=raw, 
                                                       save=True)
                industry_data.reset_index(inplace=True)
                all_tracked_industries.append(industry_data)
            result = pd.concat(all_tracked_industries)
        else:
            Exception("Not implemented use scope='all'")
        return result

    @classmethod
    def store_data(cls, local=True, sub_folder=None, data=None, raw=False, tag=None):

        if sub_folder not in ['industries', 'sectors', 'indexes']:
            raise Exception('Invalid subfolder {}'.format(sub_folder))

        if raw and local:
            data_folder = DATA_FOLDER + FINVIZ_RAW_DATA_FOLDER
        elif not raw and local:
            data_folder = DATA_FOLDER + FINVIZ_DATA_CALENDAR_FOLDER
        elif not local:
            raise Exception('Not local storage not implemented yet')


        folder_path = cls.build_path_from_date(data_folder=data_folder, 
                                               sub_folder=sub_folder, 
                                               tag=tag)
        data.to_csv(folder_path)
        print('Data saved to {}'.format(folder_path))
                
    
    @classmethod
    def build_path_from_date(cls, data_folder, sub_folder, tag):
        today = datetime.today()
        year = today.year
        month = today.month
        day = today.day
        
        if day <= 7:
            week = 1
        elif day > 7 and day <= 14:
            week = 2 
        elif day > 14 and day <= 21:
            week = 3
        elif day > 21:
            week = 4
        
        file_name = "calendar.csv"
        folder_path = f"{data_folder}/{sub_folder}/{tag.replace('-','').replace(' ','_')}/y{str(year)}/m{str(month).zfill(2)}/w{str(week).zfill(2)}/"
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
        
        return folder_path + file_name

    @classmethod
    def prepare_finviz_calendar(cls, 
                                earnings_calendar, 
                                finviz_data, 
                                table,
                                sort_value='days_left', 
                                industry=False,
                                index=False,
                                ):
        if table == 'Custom':
            columns = CUSTOM_TABLE_ALL_FIELDS
        else: 
            columns = INCLUDE_COLUMNS
    
        if industry and 'Industry' not in columns:
            columns.append('Industry')
        elif index and 'Index' not in columns:
            columns.append('Index')

        filtered_data = finviz_data.T[columns]
        earnings_calendar.rename(columns={'symbol': 'Ticker'}, inplace=True)
        earnings_calendar.reset_index(inplace=True)
        earnings_calendar = earnings_calendar.merge(
            filtered_data, 
            on='Ticker', 
            validate='many_to_one'
        )
        earnings_calendar.set_index('Ticker', inplace=True)
        earnings_calendar.sort_values(sort_value, inplace=True)
        earnings_calendar['dataDate'] = datetime.now().strftime("%Y-%m-%d")
        return earnings_calendar
    
    @classmethod
    def get_earning_calendar_for(cls, 
                                 symbols):
        """
        :symbols list of symbols 

        """
        data = MasterEarningsCalendar.get_whole_earnings_calendar(csv=True)
        filtered_results = data[data['symbol'].isin(symbols)]

        return filtered_results.sort_index()


class FinvizCalendarLoader:

    def __init__(self, source=None, name=None, year=None, month=None, week=None, data=None, path=None):
        
        if not data is None and isinstance(data, pd.DataFrame):
            self.data = self._load_raw_data(data)
        elif source and name and year and month and week:
            self.data = self._load_stored_data(source=source,
                                               name=name,
                                               year=year, 
                                               month=month, 
                                               week=week)
        elif path:
            self.data = self._load_from_path(path)
        else:
            raise Exception('No raw data or stored data information provided')

    def _load_raw_data(self, data):
        data = self._update_days_left(data)
        data = data.reset_index()
        data = data.set_index(['reportDate'])

        return data
    
    def _load_stored_data(self, source=None, name=None, year=None, month=None, week=None):
        
        month = 'm' + str(month).zfill(2)
        year = 'y' + str(year)
        week = 'w' + str(week).zfill(2)
        source = source.lower()
        if source not in ['indexes', 'sectors', 'industries']:
            raise Exception('Source is not valid {}'.format(source))
        df = pd.read_csv(f'{DATA_FOLDER}/{FINVIZ_DATA_CALENDAR_FOLDER}/{source}/{name}/{year}/{month}/{week}/calendar.csv')
        df = self._update_days_left(df)
        df = df.sort_values('days_left', ascending=True)
        df = df.set_index(['reportDate'])

        return df

    def _load_from_path(self, path): 
        df = pd.read_csv(path)
        df = self._update_days_left(df)
        df = df.set_index(['reportDate'])
        return df

    def _update_days_left(self, data):
        data['reportDate'] = pd.to_datetime(data['reportDate'])
        data['days_left'] = data['reportDate'].apply(MasterEarningsCalendar._compute_days_left)
        return data
    
    def former(self, days):
        if days == 0:
            raise Exception("0 is not allowed, that is the equivalent "
                            "of less than 24 hours")
        return self.data[(self.data['days_left'] < 0) & 
                         (self.data['days_left'] >= -days)]

    def upcoming(self, days=5):
        """Returns the upcoming earnings release"""
        return self.data[(self.data['days_left'] >= 0)
               & (self.data['days_left'] <= days)]

    def get_reported_by_days(self, days=5):
        return self.data[(self.data['days_left'] < 0) & 
                         (self.data['days_left'] > -days)]

    def get_reported_by_date(self, year, month):
        date = f'{str(year)}-{str(month)}'
        return self.data.loc[date]

    def upcoming_from_days(self, days, batch_size=15):
        return self.data[self.data['days_left'] >= days][:batch_size]

    def get_reported_by_ticker(self, ticker):
        return self.data[self.data['Ticker'] == ticker]
    
    def store_upcoming(self, days=5, path='upcoming.csv', keep='first'):
        """
        Use keep=last to preserve the data that is more recent
        this will store a new item until days left is equal to 0. Which means
        the report date less than 24 hours away.
        """
        old_data = pd.read_csv(path, index_col=0)
        upcoming = self.upcoming(days)
        return self._store(upcoming, old_data, keep)

    def store_former(self, days=5, path='former.csv', keep='first'):

        old_data = pd.read_csv(path, index_col=0)
        upcoming = self.upcoming(days)
        return self._store(upcoming, old_data, keep)
    
    def _store(self, upcoming, old_data, keep):

        new_data = pd.concat([old_data, upcoming], join='outer')
        new_data.index = pd.to_datetime(new_data.index)
        new_data = new_data.reset_index()
        new_data = new_data.drop_duplicates(['reportDate', 'Ticker'], keep=keep)
        new_data = self._update_days_left(new_data)
        new_data = new_data.sort_values('days_left')
        new_data = new_data.set_index(['reportDate','Ticker'])
        new_data['updatedAt'] = datetime.now().strftime("%Y-%m-%d")

        return new_data 
    
    def get_reported_by_days_passed(self, path='upcoming.csv', days=30):
        old_data = pd.read_csv('upcoming.csv', index_col=0)
        new_data = self._update_days_left(new_data)
        return new_data[(self.data['days_left'] < 0) & (new_data['days_left'] > -days)]
