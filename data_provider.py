### TO DO:
### 1) Add capability to set sector_avg, y2y_change, q2q_change as False

from html_retriver import HtmlRetriver
import pandas as pd
import numpy as np

class FinancialIndicatorsProvider(HtmlRetriver):
    """Retrieves financial indicators from biznesradar.pl"""


    def __init__(self, profile:str='KGHM'):
        super().__init__()
        
        html_links = [f'https://www.biznesradar.pl/wskazniki-wartosci-rynkowej/{profile},Q',
                      f'https://www.biznesradar.pl/wskazniki-rentownosci/{profile},Q',
                      f'https://www.biznesradar.pl/dywidenda/{profile}',
                      f'https://www.biznesradar.pl/rating/{profile}']

        self.retrieve_html(html_links)

    def _get_values(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-field': kwargs.get('data_field')}, limit=1)
        code2 = self.retrieve_html_content(class_ = "h", code=code1)
        get_value = lambda x: self.retrieve_html_content(class_='value', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) else None, code2))
        return values

    def _get_sector_average_value(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-field': kwargs.get('data_field')}, limit=1)
        code2 = self.retrieve_html_content(class_ = "h", code=code1)
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
        code2 = self.retrieve_html_content(class_ = "h", code=code1)
        get_value = lambda x: self.retrieve_html_content(class_='changeqq', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) and 'changeqq' in str(x) else None, code2))
        values = list(map(lambda x: 'None~None' if x==None else x, values))
        values = list(map(lambda x: x.split('~') if '~' in x else [None, None], values))
        return values

    def _get_time_itervals(self, **kwargs):
        code1 = self.retrieve_html_content(attrs={'data-symbol': kwargs.get('data_field')}, limit=1)
        code2 = self.retrieve_html_content(class_ = "h", code=code1)
        get_value = lambda x: self.retrieve_html_content(class_='sectorv', code=x)[0].get_text()
        values = list(map(lambda x: get_value(x) if len(x) else None, code2))

    def _get_quarters(self):
        first_table = self.retrieve_html_content(class_='report-table', limit=1)
        quarters_raw = first_table.find_all(class_='thq')
        quarters_func = lambda x: x.get_text().replace('\\n','').replace('\\t','')
        quarters = list(map(lambda x: quarters_func(x), quarters_raw))
        return quarters

    def _kwargs_remover(self, dict_:dict=None, value=None):
        """Removes keys with a given value from a dictionary"""
        dict_ = {k:v for k,v in dict_.items() if v!=value}

        return dict_
        
    def _args_remover(self, tuple_:tuple=None, value=None):
        """Removes keys with a given value from a dictionary"""
        tuple_ = tuple([v for v in tuple_ if v!=value])

        return tuple_

    def _create_dataframe(self, *args):
        quarters = self._get_quarters()
        quarters = self._change_dimension(quarters)
        # args = self._args_remover(tuple_=args, value=None)
        input = np.hstack([*args, quarters])
        data = pd.DataFrame(input)

        return data

    def _change_dimension(self, list_:list):
        """Transform given list into an array, where amount of columns is equal to dimension of the array"""
        list_ = np.array(list_)
        list_ = list_.reshape((len(list_), list_.ndim))

        return list_

    def price(self):
        data_field = 'Quote'
        class_ = 'h'
        return self._get_values(data_field=data_field, class_=class_)

    def dividend_yield(self):
        price = self.price()
        code1 = self.retrieve_html_content(class_='table-c')

    def price_to_earnings(self, sector_avg:bool=True, y2y_change:bool=True, q2q_change:bool=True):
        data_field = 'CZ'
        class_ = 'h'
        
        self.price_to_earnings_values = self._get_values(data_field=data_field, class_=class_)
        self.price_to_earnings_values = self._change_dimension(self.price_to_earnings_values)

        # self.sector_avg_values, self.y2y_change_values, self.q2q_change_values = None, None, None
        if sector_avg:
            self.sector_avg_values = self._get_sector_average_value(data_field=data_field, class_=class_)
            self.sector_avg_values = self._change_dimension(self.sector_avg_values)

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
            self.y2y_change_values = self._change_dimension(self.y2y_change_values)

        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)
            self.q2q_change_values = self._change_dimension(self.q2q_change_values)

        return self._create_dataframe(self.price_to_earnings_values,
                                      self.sector_avg_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values)

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

        return self._create_dataframe(self.price_to_book_value_values,
                                      self.sector_avg_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values)

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

        return self._create_dataframe(self.price_to_sales_values,
                                      self.sector_avg_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values)

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

        return self._create_dataframe(self.roe_values,
                                      self.sector_avg_values,
                                      self.y2y_change_values,
                                      self.q2q_change_values)

    def piotroski_f_score(self):
        pass


if __name__ == '__main__':
    t = FinancialIndicatorsProvider('KGHM')
    a = t.price_to_earnings(sector_avg=True, y2y_change=True, q2q_change=True)


b = t.retrieve_html_content(class_ = "rating-table full")[1].find_all(class_ = "data") 
b = list(map(lambda x: x.find_all(['th', 'span']), b))
list(map(lambda x: i for i in x))
np.unique(list(map(lambda x: x.get_text(), b)))

b[0].find_all(['th', 'span'])

# Dividend yield tries...
# t.html.find_all('tr')
## .get_text().replace('\\n', ' ').replace('\\t',' ')
# c = list(map(lambda x: x.find_all('th'), t.html.find_all('tr')))
# c = list(map(lambda x: x.replace('\\n', ' ').replace('\\t',' '), c))
# [v for v in c if v!=[]]

[b]