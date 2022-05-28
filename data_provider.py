
from html_retriver import HtmlRetriver
import pandas as pd
import numpy as np
import re
from pandas_datareader.stooq import StooqDailyReader
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class FinancialIndicatorsProvider(HtmlRetriver):
    """Retrieves financial indicators from biznesradar.pl"""


    def __init__(self, profile:str='KGHM'):
        super().__init__()
        self.profile = profile

        html_links = [f'https://www.biznesradar.pl/wskazniki-wartosci-rynkowej/{profile},Q',
                      f'https://www.biznesradar.pl/wskazniki-rentownosci/{profile},Q',
                      f'https://www.biznesradar.pl/dywidenda/{profile}',
                      f'https://www.biznesradar.pl/rating/{profile}',
                      f'https://www.biznesradar.pl/raporty-finansowe-rachunek-zyskow-i-strat/{profile},Q',
                      f'https://www.biznesradar.pl/operacje/{profile}',
                      f'https://www.biznesradar.pl/notowania/{profile}']

        self.retrieve_html(html_links)
        self.ticker = self._get_ticker()

    def _get_ticker(self):
        ticker = self.retrieve_html_content(attrs={'itemprop': 'tickerSymbol'}, limit=1)['content']
        return ticker

    def _get_values(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-field': kwargs.get('data_field')}, limit=1)
        code2 = code1.find_all(class_ = "h")
        get_value = lambda x: self.retrieve_html_content(class_='value', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) else None, code2))
        return values

    def _get_sector_average_value(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-field': kwargs.get('data_field')}, limit=1)
        code2 = code1.find_all(class_ = "h")
        get_value = lambda x: self.retrieve_html_content(class_='sectorv', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) else None, code2))
        return values

    def _get_year_to_year_change(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-field': kwargs.get('data_field')}, limit=1)
        code2 = self.retrieve_html_content(class_ = "h", code=code1)
        get_value = lambda x: self.retrieve_html_content(class_='changeyy', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) and 'changeyy' in str(x) else None, code2))
        values = list(map(lambda x: 'None~None' if x==None else x, values))
        values = list(map(lambda x: x.split('~') if '~' in x else [None, None], values))
        return values

    def _get_quarter_to_quarter_change(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-field': kwargs.get('data_field')}, limit=1)
        code2 = code1.find_all(class_ = "h")
        get_value = lambda x: self.retrieve_html_content(class_='changeqq', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) and 'changeqq' in str(x) else None, code2))
        values = list(map(lambda x: 'None~None' if x==None else x, values))
        values = list(map(lambda x: x.split('~') if '~' in x else [None, None], values))
        return values

    def _get_time_itervals(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-symbol': kwargs.get('data_field')}, limit=1)
        code2 = code1.find_all(class_ = "h")
        get_value = lambda x: self.retrieve_html_content(class_='sectorv', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) else None, code2))


    def _get_quarters(self, data_field:str=None):
        first_table = self.retrieve_html_content(attrs={'data-field': data_field}, limit=1)
        quarters_raw = first_table.parent.find_all(class_='thq')
        quarters_func = lambda x: x.get_text().replace('\\n','').replace('\\t','')
        quarters = list(map(lambda x: quarters_func(x), quarters_raw))
        return quarters

    def _kwargs_remover(self, dict_:dict=None, value=None):
        """Removes keys with a given value from a dictionary"""
        dict_ = {k:v for k,v in dict_.items() if v!=value}

        return dict_
        
    def _unambiguous_none_comparer(self, filtered_value):
        """Checks whether filtered_value is equal to None"""
        if type(filtered_value) != type(None):
            return True
        if filtered_value != None:
            return False

    # def _args_remover(self, tuple_:tuple=None, value=None):
    #     """Removes given element from a tuple"""
    #     tuple_ = filter(~self._unambiguous_comparer, )

    #     return tuple_
    
    def _create_dataframe(self, *args):
        args = list(filter(self._unambiguous_none_comparer, args))
        input = np.hstack(args)
        data = pd.DataFrame(input)

        return data

    def _change_dimension(self, list_:list):
        """Transform given list into an array, where amount of columns is equal to dimension of the array"""
        list_ = np.array(list_)
        list_ = list_.reshape((len(list_), list_.ndim))

        return list_

    def net_profit(self, y2y_change:bool=True, q2q_change:bool=True):
        data_field = 'IncomeNetProfit'
        class_ = 'h'
        self.net_profit_values =  self._get_values(data_field=data_field, class_=class_)
        self.net_profit_values = self._change_dimension(self.net_profit_values)

        self.y2y_change_values, self.q2q_change_values = None, None

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
            self.y2y_change_values = self._change_dimension(self.y2y_change_values)

        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)
            self.q2q_change_values = self._change_dimension(self.q2q_change_values)

        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)
        
        return self._create_dataframe(self.net_profit_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values,
                                      self.quarters)
    def shares(self):
        data_field = 'ShareAmount'
        class_ = 'h'
        return self._get_values(data_field=data_field, class_=class_)

    def eps(self):
        data_field = 'Z'
        class_ = 'h'
        self.eps_values = self._get_values(data_field=data_field, class_=class_)
        self.eps_values = self._change_dimension(self.eps_values)
        
        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)

        return self._create_dataframe(self.eps_values,
                                      self.quarters)

    def price(self):
        data_field = 'Quote'
        class_ = 'h'
        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)

        self.price_values = self._get_values(data_field=data_field, class_=class_)
        self.price_values = self._change_dimension(self.price_values)

        return self._create_dataframe(self.price_values,
                                      self.quarters)

    def _stringify_date(self, column:pd.Series):
        """Takes datetime column, standardizes it and outputs a string representation"""
        stringify_func = lambda x: re.sub('\D','-', x)
        column = column.map(stringify_func)
        column = column.astype(str)

        return column

    def _fill_numeric_column(self, column:pd.Series, fill_value:float=0):
        """Fills missing values in numeric column"""
        acceptable_types = ('float', 'int', 'np.float64', 'np.float32', 'np.float16', 'np.int64', 'np.int32', 'np.int16')
        num_check_func = lambda x: fill_value if isinstance(x, acceptable_types) else fill_value
        column = column.map(num_check_func)
        column = column.astype(float)

        return column

    def daily_prices(self, *args, **kwargs):
        """Returns daily closing prices"""
        daily_prices = StooqDailyReader(symbols=f'{self.ticker}.PL', *args, **kwargs)
        daily_prices = daily_prices.read().reset_index(drop=False)
        daily_prices = daily_prices[['Date','Close']]

        return daily_prices

    def dividend_yield(self):
        """Calculated dividend yield by dividing price in day in """
        dividend_table = pd.read_html(f'https://www.biznesradar.pl/dywidenda/KGHM')[0]
        dividend_table['dzień wypłaty'] = dividend_table['dzień wypłaty'].map(lambda x: x.split(' ')[-1])
        dividend_table['dzień wypłaty'] = self._stringify_date(column=dividend_table['dzień wypłaty'])


        dividend_table['dzień wypłaty'] = dividend_table['dzień wypłaty'].map(lambda x: x.split(' ')[-1])
        earliest_dividend_year = str(dividend_table['wypłata za rok'].iloc[-1])
        daily_prices = self.daily_prices(start=earliest_dividend_year)
        daily_prices['Date'] = daily_prices['Date'].astype('str')

        dividend_table = pd.merge(dividend_table, daily_prices, how='left', left_on='dzień wypłaty', right_on='Date')
        dividend_table.loc[0, 'Close'] = daily_prices['Close'].iloc[0]
        dividend_table['łącznie dywidenda na akcję (zł)'] = dividend_table['łącznie dywidenda na akcję (zł)'].replace('-', 0)
        dividend_table['łącznie dywidenda na akcję (zł)'] = dividend_table['łącznie dywidenda na akcję (zł)'].astype('float')
        dividend_table['Close'] = dividend_table['Close'].astype(float)

        dividend_table['stopa dywidendy*'] = 100 * (dividend_table['łącznie dywidenda na akcję (zł)'] / dividend_table['Close'])
        dividend_table = dividend_table[['wypłata za rok', 'dzień wypłaty', 'stopa dywidendy*']]
        dividend_table = dividend_table.rename(columns={'wypłata za rok': 'payout for year',
                                                        'stopa dywidendy*': 'dividend yield',
                                                        'dzień wypłaty': 'payout date'})

        return dividend_table

    def price_to_earnings(self, sector_avg:bool=True, y2y_change:bool=True, q2q_change:bool=True):
        data_field = 'CZ'
        class_ = 'h'
        
        self.price_to_earnings_values = self._get_values(data_field=data_field, class_=class_)
        self.price_to_earnings_values = self._change_dimension(self.price_to_earnings_values)

        self.sector_avg_values, self.y2y_change_values, self.q2q_change_values = None, None, None
        if sector_avg:
            self.sector_avg_values = self._get_sector_average_value(data_field=data_field, class_=class_)
            self.sector_avg_values = self._change_dimension(self.sector_avg_values)

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
            self.y2y_change_values = self._change_dimension(self.y2y_change_values)

        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)
            self.q2q_change_values = self._change_dimension(self.q2q_change_values)

        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)

        return self._create_dataframe(self.price_to_earnings_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values,
                                      self.quarters)

    def price_to_book_value(self, sector_avg:bool=True, y2y_change:bool=True, q2q_change:bool=True):
        data_field = 'CWK'
        class_ = 'h'
        
        self.price_to_book_value_values =  self._get_values(data_field=data_field, class_=class_)
        self.price_to_book_value_values = self._change_dimension(self.price_to_book_value_values)

        self.sector_avg_values, self.y2y_change_values, self.q2q_change_values = None, None, None
        if sector_avg:
            self.sector_avg_values = self._get_sector_average_value(data_field=data_field, class_=class_)
            self.sector_avg_values = self._change_dimension(self.sector_avg_values)

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
            self.y2y_change_values = self._change_dimension(self.y2y_change_values)

        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)
            self.q2q_change_values = self._change_dimension(self.q2q_change_values)

        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)

        return self._create_dataframe(self.price_to_book_value_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values,
                                      self.quarters)

    def price_to_sales(self, sector_avg:bool=True, y2y_change:bool=True, q2q_change:bool=True):
        data_field = 'CP'
        class_ = 'h'

        self.price_to_sales_values =  self._get_values(data_field=data_field, class_=class_)
        self.price_to_sales_values = self._change_dimension(self.price_to_sales_values)

        self.sector_avg_values, self.y2y_change_values, self.q2q_change_values = None, None, None
        if sector_avg:
            self.sector_avg_values = self._get_sector_average_value(data_field=data_field, class_=class_)
            self.sector_avg_values = self._change_dimension(self.sector_avg_values)

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
            self.y2y_change_values = self._change_dimension(self.y2y_change_values)

        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)
            self.q2q_change_values = self._change_dimension(self.q2q_change_values)

        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)

        return self._create_dataframe(self.price_to_sales_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values,
                                      self.quarters)

    def roe(self, sector_avg:bool=True, y2y_change:bool=True, q2q_change:bool=True):
        data_field = 'ROE'
        class_ = 'h'

        self.roe_values =  self._get_values(data_field=data_field, class_=class_)
        self.roe_values = self._change_dimension(self.roe_values)

        self.sector_avg_values, self.y2y_change_values, self.q2q_change_values = None, None, None
        if sector_avg:
            self.sector_avg_values = self._get_sector_average_value(data_field=data_field, class_=class_)
            self.sector_avg_values = self._change_dimension(self.sector_avg_values)

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
            self.y2y_change_values = self._change_dimension(self.y2y_change_values)

        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)
            self.q2q_change_values = self._change_dimension(self.q2q_change_values)

        self.quarters = self._get_quarters(data_field=data_field)
        self.quarters = self._change_dimension(self.quarters)

        return self._create_dataframe(self.roe_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values,
                                      self.quarters)

    def piotroski_f_score(self):
        piotroski_table = self.retrieve_html_content(class_ = "rating-table full")[1].find_all(class_ = "data") 
        piotroski_table_text = (list(map(lambda x: x.get_text(), piotroski_table)))
        piotroski_table_text = np.unique(piotroski_table_text)
        regex_splitter = lambda x:re.split('\\\\n|\\\\t|poprzedni okres:[\s\d\.\%]+', x)
        splitted_text = list(map(regex_splitter, piotroski_table_text))
        clean_empty_strings_filter = lambda x: list(filter(None, x))
        empty_strings_removed = list(map(clean_empty_strings_filter, splitted_text))
        
        self.piotroski_f_score_table = pd.DataFrame(empty_strings_removed)
        self.piotroski_f_score_value = self.piotroski_f_score_table[self.piotroski_f_score_table.columns[-1]].astype(int).sum()
        print('F-SCORE = ', self.piotroski_f_score_value)

        return self.piotroski_f_score_table

    # def cape(self, inflation_rate:float, periods:int=40):
    #     """Calculates cape value based on quaterly data. This version of cape is median based."""

    #     eps = self.eps()
    #     eps.columns = ['value', 'key']
    #     eps['value'] = eps['value'].map(lambda x: x.replace(' ', '')).astype(float)
    #     eps_data_amt = len(eps)
    #     if eps_data_amt < periods:
    #         raise ValueError(f'''Required amount of periods is equal to {periods}, but amount
    #         of calculated periods for {self.profile} is equal to {eps_data_amt}.
    #         Try to decrease amount of periods.''')

    #     inflation_rate /= 100
    #     inflation_rate += 1
    #     inflation_cumulative = 1 - inflation_rate**periods


    #     eps['eps_adjusted'] = eps['value'] + (eps['value'] * inflation_cumulative)
    #     eps_rolling = eps[['eps_adjusted']].rolling(periods).median()
    #     eps_rolling['key'] = eps['key']

    #     shares_val = self.price()
    #     shares_val.columns = ['share_price', 'key']
    #     shares_val['share_price'] = shares_val['share_price'].map(lambda x: x.replace(' ', '')).astype(float)

    #     self.cape_value = pd.merge(eps_rolling, shares_val, on='key')
    #     self.cape_value['cape'] = self.cape_value['share_price'] / self.cape_value['eps_adjusted']
    #     self.cape_value = self.cape_value.drop(['share_price', 'eps_adjusted'], axis=1).dropna()
    #     self.cape_value = self.cape_value.reset_index(drop=True)

    #     return self.cape_value


        
if __name__ == '__main__':
    t = FinancialIndicatorsProvider('KGHM')
    t.dividend_yield()
