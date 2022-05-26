from data_retriver import DataRetriver
import pandas as pd
import numpy as np

class FinancialIndicators(DataRetriver):
    """Retrieves financial indicators from biznesradar.pl"""


    def __init__(self, profile:str='KGHM'):
        super().__init__()
        
        html_links = [f'https://www.biznesradar.pl/wskazniki-wartosci-rynkowej/{profile},Q',
                      f'https://www.biznesradar.pl/wskazniki-rentownosci/{profile},Q',
                      f'https://www.biznesradar.pl/dywidenda/{profile}']

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
        quarters_raw = self.retrieve_html_content(class_='thq h')
        quarters_func = lambda x: x.get_text().replace('\\n','').replace('\\t','')
        quarters = list(map(lambda x: quarters_func(x), quarters_raw))
        return quarters

    def _create_dataframe(self, *args):
        quarters = self._get_quarters()
        input = np.hstack([*args])
        data = pd.DataFrame(input)

        return data

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
        
        self.price_to_earnings = self._get_values(data_field=data_field, class_=class_)
        sector_avg_values, y2y_change_values, q2q_change_values = None, None, None
        if sector_avg:
            self.sector_avg_values = self._get_sector_average_value(data_field=data_field, class_=class_)

        if y2y_change:
            self.y2y_change_values = self._get_year_to_year_change(data_field=data_field, class_=class_)
        
        if q2q_change:
            self.q2q_change_values = self._get_quarter_to_quarter_change(data_field=data_field, class_=class_)

        # return self._create_dataframe(price_to_earnings, sector_avg_values, y2y_change_values, q2q_change_values)

    def price_to_book_value(self, sector_avg:bool=True):
        data_field = 'CWK'
        class_ = 'h'
        return self._get_values(data_field=data_field, class_=class_)

    def price_to_sales(self):
        data_field = 'CP'
        class_ = 'h'
        return self._get_values(data_field=data_field, class_=class_)

    def roe(self):
        data_field = 'ROE'
        class_ = 'h'
        return self._get_values(data_field=data_field, class_=class_)


if __name__ == '__main__':
    t = FinancialIndicators('KGHM')
    c = t.price_to_earnings()


a = np.array(t.sector_avg_values)
b = np.array(t.price_to_earnings)
c = np.array(t.y2y_change_values)
d = np.array(t.q2q_change_values)

np.hstack([a,c,d]).shape
c
t._create_dataframe(t.price_to_earnings,t.sector_avg_values,t.y2y_change_values,t.q2q_change_values)
#         quarters_raw = t.retrieve_html_content(class_='thq h')
#         list(map(lambda x: , quarters_raw))