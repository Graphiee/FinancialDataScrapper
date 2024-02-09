import requests 
from bs4 import BeautifulSoup
import time

class HtmlRetriver:
    """Retrieves HTML content from provided website"""
    
    def __init__(self):
        pass

    def retrieve_html(self, html_links:list=None):
        """Retrieves initial HTML string
           html_link: str, retrieves data from a website
        """
        request_html = ''
        for link in html_links:
            status_code = 0 
            ignore = False
            backoff = 1
            while (status_code not in range(200, 300)) and (not ignore):
                try:
                    response = requests.get(link)
                    status_code = response.status_code
                    link_output = response.content
                    if status_code not in range(200,300):
                        time.sleep(backoff)
                        backoff *= 2
                        ignore = True if backoff >= 32 else False
                    if ignore:
                        break
                except:
                    continue
            if ignore:
                continue
            request_html += str(link_output)
        self.html = BeautifulSoup(request_html, 'html.parser')

        return self.html

    def retrieve_html_content(self, *args, **kwargs):
        """Retrieves subcontent of previously retrieved HTML file. Kwargs are the same as in find_all method in beautiful soup"""
        if kwargs.get('code'):
            self.code = kwargs.get('code').find_all(*args, **kwargs)
        else: 
            self.code = self.html.find_all(*args, **kwargs)

        if not len(self.code):
            raise RuntimeError('Provided kwargs do not match html content')
        
        if kwargs.get('limit') == 1:
            self.code = self.code[0]
        
        return self.code
        