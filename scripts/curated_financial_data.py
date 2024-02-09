from datetime import datetime
from data_provider import FinancialDataProvider
from html_retriver import HtmlRetriver
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import *
import os

class CuratedFinancialData(FinancialDataProvider):
    """Retrieves financial data for a given period for companies provided"""


    def __init__(self, companies:list=None, indicators:list=None):
        self.companies = companies
        self.indicators = indicators

    def _get_indicator_kwargs(self, **kwargs):
        kwargs['sector_change'] = kwargs.get('sector_change') if kwargs.get('sector_change') is not None else True
        kwargs['q2q_change'] = kwargs.get('q2q_change') if kwargs.get('q2q_change') is not None else True
        kwargs['y2y_change'] = kwargs.get('y2y_change') if kwargs.get('y2y_change') is not None else True
        
        return kwargs



    def get(self, **kwargs):
        print('RETRIEVE FINANCIAL INDICATORS TABLE...\n')
        """Retrieves curated financial data for a given company"""
        kwargs = self._get_indicator_kwargs(**kwargs)
        self.companies_indicators = pd.DataFrame()
        for company in self.companies:
            current_date = datetime.now().strftime('%Y-%m-%d')
            file_path = f'../results/indicators_{current_date}.csv'
            if os.path.exists(file_path):
                existing_data = pd.read_csv(file_path)
                if company in existing_data['company'].values:
                    print(f"Data for {company} already exists. Skipping...")
                    continue

            print(company, self.companies.index(company))
            success = False
            attempts = 0
            while not success and attempts < 10:
                try:
                    f_data = FinancialDataProvider(profile=company)
                    success = True
                except:
                    attempts += 1
                    if attempts == 10:
                        raise ConnectionError(f"Failed to retrieve data for {company} after 10 attempts.")
            
            if not self.indicators:
                raise ValueError("No indicators provided.")
            for indicator_tuple in self.indicators:
                if len(indicator_tuple) != 2:
                    raise ValueError("Indicator must be a tuple of (indicator_name, indicator_function).")
                indicator_name, indicator = indicator_tuple
                if indicator == 'piotroski_f_score':
                    vars()[indicator_name + '_' + company] = getattr(f_data, indicator)()
                    continue

                vars()[indicator_name + '_' + company] = getattr(f_data, indicator)(kwargs.get('sector_change'), kwargs.get('q2q_change'), kwargs.get('y2y_change'))

            vars()[company] = vars()[self.indicators[0][0] + '_' + company]
            for indicator_name, _ in self.indicators[1:]:
                # In case when f score is not calculated
                if not vars()[indicator_name + '_' + company]['key'].iloc[0]:
                    continue
                vars()[company] = pd.merge(vars()[company], vars()[indicator_name + '_' + company], on='key')
            
            if kwargs.get('period') == 'latest':
                vars()[company] = vars()[company].iloc[-1:]
            elif isinstance(kwargs.get('period'), int):
                vars()[company] = vars()[company][vars()[company]['key'].map(lambda x: int(x[:4]) >= kwargs.get('period'))]

            vars()[company]['company'] = company

            self.companies_indicators = vars()[company].copy()
        

            current_date = datetime.now().strftime('%Y-%m-%d')
            if not os.path.exists(f'../results/indicators_{current_date}.csv'):
                self.companies_indicators.to_csv(f'../results/indicators_{current_date}.csv', index=False)
            else:
                self.companies_indicators.to_csv(f'../results/indicators_{current_date}.csv', mode='a', header=False, index=False)

        if kwargs.get('total_score'):
            self.companies_indicators['score'] = self.companies_indicators.select_dtypes([float, int]).sum(axis=1)

        return self.companies_indicators

    
    def get_dividends(self, **kwargs):
        print('\nRETRIEVE DIVIDENDS INFORMATION...\n')
        self.companies_dividends = pd.DataFrame()
        for company in self.companies:
            print(company, self.companies.index(company))
            f_data = FinancialDataProvider(profile=company)
            dividend = f_data.dividend_yield()
            if dividend is None:
                continue
            dividend['company'] = company

            if kwargs.get('period') == 'latest':
                dividend = dividend.iloc[-1:]
            elif isinstance(kwargs.get('period'), int):
                dividend = dividend[dividend['payout for year'].map(lambda x: int(x) >= kwargs.get('period'))]

            self.companies_dividends = pd.concat([self.companies_dividends, dividend], axis=0)

        return self.companies_dividends

    def get_csv_data(self, path:str):
        current_date = datetime.now().strftime('%Y-%m-%d')
        indicators_path = os.path.join(path, f'/indicators_{current_date}.csv')
        dividends_path = os.path.join(path, f'/dividends_{current_date}.csv')
        self.companies_indicators.to_csv(indicators_path)
        self.companies_dividends.to_csv(dividends_path)

class PolishStockMarketCompanies(HtmlRetriver):

    def __init__(self):

        wse = ['https://www.biznesradar.pl/gielda/akcje_gpw']
        new_connect = ['https://www.biznesradar.pl/gielda/newconnect']

        self.wse_html = self.retrieve_html(wse)
        self.new_connect_html = self.retrieve_html(new_connect)

    def retrieve_profile(self, stock_exchange_html):
        profiles_unpolished_list = stock_exchange_html.find_all(class_="qTableFull")[0].find_all(class_="s_tt")
        profiles_list = list(map(lambda x: x['href'].split('/')[-1], profiles_unpolished_list))

        return profiles_list

    def get_wse_profiles(self):
        """Return list of all profiles listed on WSE"""
        return self.retrieve_profile(self.wse_html)

    def get_new_connect_profiles(self):
        """Return list of all profiles listed on New Connect"""
        return self.retrieve_profile(self.new_connect_html)

    def get_profiles(self):
        """Returns the list of all profiles listed on both stock exchanges"""
        wse_list = self.retrieve_profile(self.wse_html)
        nc_list = self.retrieve_profile(self.new_connect_html)

        all_profiles = wse_list + nc_list
        all_profiles.sort(reverse=False)

        return all_profiles

if __name__ == '__main__':
    indicators = [('price_to_earnings', 'price_to_earnings'),
    ('roe', 'roe'),
    ('price_to_book_value', 'price_to_book_value'),
    ('price_to_sales', 'price_to_sales'),
    ('piotroski_f_score', 'piotroski_f_score')]
    
    profiles_getter = PolishStockMarketCompanies()
    profiles = profiles_getter.get_profiles()

    current_date = datetime.now().strftime('%Y-%m-%d')
    cfd = CuratedFinancialData(companies=profiles, indicators=indicators)
    indicators = cfd.get(sector_change=False, y2y_change=False, q2q_change=False, period='latest', total_score=True)
    dividends = cfd.get_dividends(period='latest')
    cfd.companies_dividends.to_csv(f'../results/dividends_{current_date}.csv')