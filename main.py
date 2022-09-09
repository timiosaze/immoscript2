from urllib import response
from bs4 import BeautifulSoup
import time
import certifi
import urllib3
import requests
from urllib3 import ProxyManager, make_headers
from urllib.request import Request, urlopen
import mysql.connector
from urllib.parse import urlparse
from fake_useragent import UserAgent
import random
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


from deep_translator import (GoogleTranslator,
                             MicrosoftTranslator,
                             PonsTranslator,
                             LingueeTranslator,
                             MyMemoryTranslator,
                             YandexTranslator,
                             PapagoTranslator,
                             DeeplTranslator,
                             QcriTranslator,
                             single_detection,
                             batch_detection)
ua = UserAgent()
chrome_ua = ua.chrome

# MYSQL CONNECTION PARAMS
cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='immoscoutdb')
cursor = cnx.cursor(buffered=True)
start = time.time()

count = 0
def status(str):
    print(str)

def inc(): 
    global count 
    count += 1


good_proxies = []

def clear_txt():
    f = open('good.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('good2.txt', 'r+')
    f.truncate(0) # need '0' when using r+

def proxies_list():
    headers={'User-Agent': chrome_ua}
    response = requests.get('https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt', headers=headers)
    with open("response.txt", "w") as f:
        f.write(response.text)
        f.close()

def proxies_arr():
    proxies_arr = []
    with open('response.txt', 'r') as reader:
        for line in reader.readlines():
            # print(line, end='')
            proxies_arr.append(line.strip())
    return proxies_arr

#get the list of free proxies
def getProxies():
    r = requests.get('https://free-proxy-list.net/')
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('tbody')
    proxies = []
    for row in table:
        if row.find_all('td')[4].text =='elite proxy':
            proxy = ':'.join([row.find_all('td')[0].text, row.find_all('td')[1].text])
            proxies.append(proxy)
        else:
            pass
    return proxies

def extract(proxy):
   
   
    headers={'User-Agent': chrome_ua}
   
    r = requests.get('https://www.immoscout24.ch/de/d/wohnung-kaufen-abtwil-ag/7217242', headers=headers, proxies={'http' :proxy,'https': proxy},timeout=2)
    if(r.status_code == 200):
        print(proxy, " is working ", r.status_code)
        with open("good2.txt", "a") as myfile:
            myfile.write(proxy)
            myfile.write('\n')
            myfile.close()
        good_proxies.append(proxy)
    return proxy





def getAllZurichRentProperties(proxy):
    status("GETTING RENT PROPERTIES....")
    ids = []
    page = []
    page = getTimeRange()
    one = page[0]
    two = page[1]
    for x in range(one, two):    
        time.sleep(1)
        proxies = {
                    'http' :proxy,
                    'https':proxy
                    }
        while True:
            try:
                response = requests.get('https://www.immoscout24.ch/de/immobilien/mieten/ort-zuerich?pn=' + str(page) + '&r=100', headers={'User-Agent': chrome_ua},  proxies=proxies,timeout=2)
            except requests.exceptions.Timeout:
                print("Timeout error, Retrying ...")
    
       
        soup = BeautifulSoup(response.text, "lxml")
        
        for a in soup.find_all('a',attrs = {'class':'Wrapper__A-kVOWTT'}):
            href = a['href']
            inc()
            status("gotten list " + str(count) + ": " + href)
            ids.append(href)
        status("appended page " + str(x))
    return ids

def getAllZurichBuyProperties(proxy):
    status("GETTING BUY PROPERTIES....")
    ids = []
    page = []
    page = getTimeRange()
    one = page[0]
    two = page[1]
    for x in range(one, two):    
        time.sleep(1)
        proxies = {
                    'http' :proxy,
                    'https':proxy
                    }
        while True:
            try:
                response = requests.get('https://www.immoscout24.ch/de/immobilien/kaufen/ort-zuerich?pn=' + str(page) + '&r=100', headers={'User-Agent': chrome_ua}, proxies=proxies,timeout=2)
                break
            except requests.exceptions.Timeout:
                print("Timeout error, Retrying ...")

        soup = BeautifulSoup(response.text, "lxml")
        for a in soup.find_all('a',attrs = {'class':'Wrapper__A-kVOWTT'}):
            href = a['href']
            inc()
            status("gotten list " + str(count) + ": " + href)
            ids.append(href)

        status("appended page " + str(x))
    return ids

def getTimeRange():
    arr = []
    timestamp = time.strftime('%H');
    hour = int(timestamp)
    arr = [1 + 2 * (hour - 1), 1 + 2 * (hour - 1) + 2]
    return arr


def getData(section, state, props, proxy):
    ids = props
    status("GETTING ALL DATA FOR ZURICH USING THEIR UNIQUE IDS....")
    for id in ids:
        start = time.time()
        new_id = str(id)
        proxies = {
                    'http' :proxy,
                    'https':proxy,
                    }
        while True:
            try:
                if(new_id.startswith('https')):
                    response = requests.get(new_id, headers={'User-Agent': chrome_ua}, proxies=proxies,timeout=2)
                else:
                    response = requests.get('https://www.immoscout24.ch' + new_id + '', headers={'User-Agent': chrome_ua}, proxies=proxies,timeout=2)
                break
            except requests.exceptions.Timeout:
                print("Timeout error, Retrying ...")
       
        soup = BeautifulSoup(response.text, "lxml")
        keys = list()
        vals = list()
        if(new_id.startswith('https')):
            desc = soup.find("h1", attrs={'data-test':'headerNameItem'})
            description = desc.find("div").text.strip()
            price = soup.find("div", attrs={'class':'b-complex-heading-info__title b-complex-heading-info__title--second'}).text.strip()
            street = soup.find("div", attrs={'data-test':'ComplexLocation'}).text.strip()
            city = street.split()[-1:][0]
            info = soup.find("div", attrs={'class':'b-complex-block__list'})
            info_list = info.find_all("div", attrs={'class':'b-complex-block__list-item'})
            for x in info_list:
                label = x.find("div", attrs={'class':'b-complex-block__list-item-label'}).text
                translated_label = GoogleTranslator(source='de', target='en').translate(text=label)
                keys.append(translated_label)
                val = x.find("div", attrs={'class':'b-complex-block__list-item-value'}).text
                val = val.replace('\r', '')
                val = val.replace('\n', '').strip()
                vals.append(val)
            rentalpairs =  dict(zip(keys, vals))
            community = ""
            livingSpace = ""
            floors = ""
            availability = ""
            try:
                livingSpace += rentalpairs['total area']
                floors += rentalpairs['Room']
                availability += rentalpairs['Available from']         
            except KeyError:
                why = "some ppt not found"
            number = soup.find("button", attrs={'class':'js-submit-button-number'})
            nom2 = number['alt']
           
        else:
           
            street =""
            try:
                street = soup.find("p", attrs={'class':'Box-cYFBPY fJcIoQ'}).text
                a = street.split()
                city = ','.join(str(x) for x in a[-3:])  
                
            except:
                street = ""
                city =""
            keys = list()
            vals = list()
            attris = soup.find('table',attrs = {'class':'DataTable__StyledTable-sc-1o2xig5-1 jbXaEC'})
            attr_body = attris.find('tbody')
            attrs = attr_body.find_all('tr')
            for x in attrs:
                tag = x.find('td').text
                translated = GoogleTranslator(source='de', target='en').translate(text=tag)
                keys.append(translated)
                vals.append(x.find('td').find_next('td').text)
            rentalpairs =  dict(zip(keys, vals))
            # print(rentalpairs)
            community = ""
            livingSpace = ""
            floors = ""
            availability = ""
            try:
                community += rentalpairs['Community']
                livingSpace += rentalpairs['living space']
                floors += rentalpairs['floor']
                availability += rentalpairs['Availability']         
            except KeyError:
                why = "some ppt not found"
            
            desc = soup.find('article',attrs = {'class':'Box-cYFBPY hKrxoH'})
            description = desc.find('h1').text
            try:
                nom = soup.find('div',attrs = {'class':'Box-cYFBPY Flex-feqWzG ezAvvv dCDRxm'})
                nom1 = nom.find('a',attrs = {'class':'Box-cYFBPY PseudoBox-fXdOzA Shell-fTlxHA eLSBpd iAUHrk gfKtRI PhoneNumber__PhoneNumberButton-sc-1txqtux-0 btWgJG'})
                nom2 = nom1.attrs['href']
            except Exception as e:
                nom2 = ""
            price = soup.find('h2',attrs = {'class':'Box-cYFBPY JEfxu'}).text

        vals = (new_id,)
        cursor.execute('SELECT propertylink FROM properties WHERE propertylink = %s', vals)
        cnx.commit()
        newcount = cursor.rowcount
        if(newcount == 0):
            sql = 'INSERT INTO properties(section, state, street, city, community, floors, availability, livingSpace, description, phonenumber,price,propertylink) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            sql_vals =  (section, state, street,city, community, floors, availability, livingSpace, description, nom2, price,new_id)

            cursor.execute(sql, sql_vals)
            cnx.commit()
            print("affected rows = " + str(cursor.rowcount))
        else:
            print("Already in Database")

       

                


print(getTimeRange())
# print(save_proxies)
start = time.time()

clear_txt()
for x in range(3):
    proxies_list()
    proxylist = proxies_arr()
    with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(extract, proxylist)
proxies = [*set(good_proxies)]
proxy = random.choice(proxies)
print("chosen proxy ", proxy)
print(len(proxies), " are working well")
getData("Rent", "Zurich",getAllZurichRentProperties(proxy), proxy)
getData("Buy", "Zurich",getAllZurichBuyProperties(proxy), proxy)

cursor.close()

end = time.time()

print(end - start)