import requests
from lxml import html
from bs4 import BeautifulSoup
import hashlib
import json
# from airflow.hooks.http_hook import HttpHook

class G1Hook():
    
    def __init__(self, pages = 4, conn_id=None):
        self.pages = pages
        self.conn_id = conn_id or "g1_hook"
        
        # super().__init__(http_conn_id=self.conn_id)

    def get_news(self, url):
        page = requests.get(url)
        content = html.fromstring(page.content)
        soup = BeautifulSoup(page.content, 'html5lib')
        
        news_title = content.xpath('/html/body/div[1]/main/div[2]/div[1]/h1/text()')
        sub_title = content.xpath("/html/body/div[1]/main/div[2]/div[2]/h2/text()")
        
        news_article = soup.find_all('article')[0].get_text() if not Exception else ''

        published_date = content.xpath("/html/body/div[1]/main/div[3]/div[1]/div/div/p[2]/time/text()")
        published_date = published_date[0].split(' ')[1] if not Exception else ''

        datasource = 'G1'
        news = [ self.treatment_string(news_title),  self.treatment_string(sub_title), self.treatment_string(news_article), published_date , url, datasource] 
        for i in range(len(news)):
            news[i] = str(news[i]).strip("[]")
            news[i] = self.treatment_string(news[i])
        id_news = hashlib.md5(news[0].encode('utf-8')).hexdigest()
        news_json = {
            'id': id_news,
            "titulo" : news[0],
            "resumo" : news[1],
            "conteudo" : news[2],
            "data_publicacao" : news[3],
            "url_fonte": news[4],
            "fonte": news[5]
        }
        return news_json

    def treatment_string(self,string):
        s = ''.join(string)
        string = s
        string = string.replace('\n', ' ')
        string = string.replace('\r', ' ')
        string = string.replace('1 de 3', '')
        string = string.replace('2 de 3', '')
        string = string.replace('3 de 3', '')
        string = string.replace("'", "")
        
        ready = False
        while ready == False:
            pos = string.find("  ")
            if pos != -1:
                string = string.replace("  "," ")
            else:
                ready = True
                
        return string
  
    def run(self):
        
        for page in range(1, self.pages):
            req = requests.get(f"https://g1.globo.com/index/feed/pagina-{page}.ghtml")
            content = html.fromstring(req.content)
            for item in range(1, 11):
                notice = content.xpath(f"//*[@id='feed-placeholder']/div/div/div[2]/div/div/div/div[{item}]/div/div/div/div[2]/div/div/a/@href")
                try:
                    if 'https://g1.globo.com/globonews' in notice[0]:
                        pass
                    
                    elif 'https://g1.globo.com' in notice[0]:
                        yield self.get_news(notice[0])
                except Exception as e:
                    print("Error no primeiro try " + str(e))
                    break
        
         
if __name__ == '__main__':
    for pg in G1Hook().run():
        print(json.dumps(pg, indent=4, sort_keys=True))