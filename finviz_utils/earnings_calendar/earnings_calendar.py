import pandas as pd
from datetime import datetime
from finviz_utils.finviz_utils import (
    get_filters,
    get_dataframe_by_industry,
    get_dataframe_by_sector,
    get_dataframe_by_index,
)
from finviz_utils.constants import (
    CUSTOM_TABLE_ALL_FIELDS,
)
from dateutil.relativedelta import relativedelta
from finviz_utils.earnings_calendar.constants import (
    EARNINGS_CALENDAR_FOLDER,
    TRACKED_INDUSTRIES,
    INCLUDE_COLUMNS,
)
from finviz_utils.config import Config

config = Config()

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
        api_key = config.ALPHA_VANTAGE_API_KEY
        URL = 'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&apikey={}'.format(
            api_key)
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
    def _compute_days_left(cls, earnings_date):

        today = datetime.today()
        diff = earnings_date - today
        return int(diff.days)


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
                           raw=False):
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
        elif sector and not index and not industry:
            finviz_data = get_dataframe_by_sector(
                sector, 
                details=details, 
                table=table)
        elif index and not sector and not industry:
            finviz_data = get_dataframe_by_index(
                index, 
                details=details, 
                table=table)
        else:
            raise Exception('You can only pass sector, industry, or index not several of them')

        if raw:
            return finviz_data
        else:
            earnings_calendar = cls.get_earning_calendar_for(finviz_data.T.index)
            finviz_calendar = cls.prepare_finviz_calendar(
                earnings_calendar=earnings_calendar, 
                finviz_data=finviz_data, 
                table=table, 
                industry=industry, 
                index=index)
        
        return finviz_calendar

    @classmethod
    def gel_all_tracked_industries(cls, 
                                   table='Custom', 
                                   raw=False, 
                                   scope='all',
                                   industries=TRACKED_INDUSTRIES):
        if scope == 'all':
            all_tracked_industries = []
            for industry in industries:
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
        earnings_calendar.set_index('reportDate', inplace=True)
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

    def __init__(self, data=None, path=None):
        self.data = self._load_raw_data(data)

    def get_pre_earnings(self, days=5):
        """Returns the upcoming earnings release"""
        return self.data[(self.data['days_left'] >= 0)
               & (self.data['days_left'] <= days)]
    
    def get_post_earnings(self, days=5):
        return self.data[(self.data['days_left'] < 0) & 
                         (self.data['days_left'] >= -days)]

    def get_pre_earnings_from_this_month(self):
        return self.data[(self.data['days_left'] > -30) & 
                         (self.data['days_left'] < 0)]

    def get_pre_earnings_from_one_month(self, days=30):
        return self.data[(self.data['days_left'] > -60) & 
                         (self.data['days_left'] < -days)]
    
    def get_pre_earnings_from_two_months(self, days=30):
        return self.data[(self.data['days_left'] > -90) & 
                         (self.data['days_left'] < -days)]

    def get_report_dates_by_ticker(self, ticker):
        "returns a Series"
        return self.data[self.data['Ticker'] == ticker]['Ticker']
    
    def store_pre_earnings(self, days=5, path='upcoming.csv', keep='first'):
        """
        Use keep=last to preserve the data that is more recent
        this will store a new item until days left is equal to 0. Which means
        the report date less than 24 hours away.
        """
        old_data = pd.read_csv(path, index_col=0)
        pre_earnings = self.get_pre_earnings(days)
        return self._store(pre_earnings, old_data, path, keep)

    def store_post_earnings(self, days=5, path='reported.csv', keep='first'):
        old_data = pd.read_csv(path, index_col=0)
        post_earnings = self.get_post_earnings(days)
        return self._store(post_earnings, old_data, path, keep)
    
    def _store(self, new_data, old_data, path,  keep):

        new_data = pd.concat([old_data, new_data], join='outer')
        new_data.index = pd.to_datetime(new_data.index)
        try:
            new_data = new_data.reset_index()
        except Exception:
            pass
        new_data = new_data.drop_duplicates(['reportDate', 'Ticker'], keep=keep)
        new_data = self._update_days_left(new_data)
        new_data = new_data.sort_values('days_left')
        new_data = new_data.set_index(['reportDate'])
        new_data['updatedAt'] = datetime.now().strftime("%Y-%m-%d")
        new_data.drop(['index', 'level_0'], axis=1, inplace=True, errors='ignore')
        new_data.to_csv(path)
        return new_data 

    def _load_raw_data(self, data):
        data = data.reset_index()
        data = self._update_days_left(data)
        data = data.set_index(['reportDate'])
        return data

    def _load_from_path(self, path): 
        df = pd.read_csv(path)
        df = self._update_days_left(df)
        df = df.set_index(['reportDate'])
        return df

    def _update_days_left(self, data):
        data = data.reset_index(drop=True)
        data['reportDate'] = pd.to_datetime(data['reportDate'])
        data['days_left'] = data['reportDate'].apply(MasterEarningsCalendar._compute_days_left)
        return data

    def update_pre_earnings(self, days=5):
        self.store_pre_earnings(days=days, path=config.PRE_EARNINGS_KEEP_LAST_NAME, keep='last')
        self.store_pre_earnings(days=days, path=config.PRE_EARNINGS_KEEP_FIRST_NAME, keep='first')

    def update_post_earnings(self, days=5):
        self.store_post_earnings(days=days, path=config.POST_EARNINGS_KEEP_FIRST_NAME, keep='first')

    def load_stored_pre_earnings(self):
        first = pd.read_csv(config.PRE_EARNINGS_KEEP_LAST_NAME)
        first = self._update_days_left(first)
        first = first.set_index(['reportDate'])
        first = first.sort_values('days_left')
        first.drop(['index', 'level_0'], axis=1, inplace=True, errors='ignore')

        last = pd.read_csv(config.PRE_EARNINGS_KEEP_FIRST_NAME)
        last = self._update_days_left(last)
        last = last.set_index(['reportDate'])
        last = last.sort_values('days_left')
        first.drop(['index', 'level_0'], axis=1, inplace=True, errors='ignore')

        return first, last

    def load_stored_post_earnings(self):
        post = pd.read_csv(config.POST_EARNINGS_KEEP_FIRST_NAME)
        post = self._update_days_left(post)
        post =  post.set_index(['reportDate'])
        post.drop(['index', 'level_0'], axis=1, inplace=True, errors='ignore')
        return post.sort_values('days_left')
