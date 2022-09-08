from bs4 import BeautifulSoup
import time
import certifi
import urllib3
# import socket
# import socks
from urllib.request import Request, urlopen
import mysql.connector
from urllib.parse import urlparse
from fake_useragent import UserAgent
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
# MYSQL CONNECTION PARAMS
cnx = mysql.connector.connect(host='localhost', user='python', password='password',database='immoscoutdb')
cursor = cnx.cursor(buffered=True)
start = time.time()

count = 0
def status(str):
    print(str)

def inc(): 
    global count 
    count += 1

def getAllZurichRentProperties():
    ids = []
    page = []
    page = getTimeRange()
    one = page[0]
    two = page[1]
    for x in range(one, two):    
        time.sleep(1)
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        
       
        r = http.request('GET','https://www.immoscout24.ch/de',headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'})
        print(r.status)
        r = http.request('GET','https://www.immoscout24.ch/de/immobilien/mieten/ort-zuerich',headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'})
        print(r.status)
        # https://www.immoscout24.ch/de/immobilien/mieten/ort-zuerich
        soup = BeautifulSoup(r.data.decode('utf-8'), "lxml")
        
    #     for a in soup.find_all('a',attrs = {'class':'Wrapper__A-kVOWTT'}):
    #         print(a)
    #         href = a['href']
    #         inc()
    #         status("gotten list " + str(count) + ": " + href)
    #         ids.append(href)
    #     status("appended page " + str(x))
    # return ids

def getAllZurichBuyProperties():
    ids = []
    page = []
    page = getTimeRange()
    one = page[0]
    two = page[1]
    for x in range(one, two):    
        time.sleep(1)
        http = urllib3.PoolManager(ca_certs=certifi.where())

        # url = 'https://www.immoscout24.ch/de/immobilien/kaufen/ort-zuerich?pn=' + str(page) + '&r=100'
       
        r = http.request('GET', 'https://www.immoscout24.ch/de/immobilien/kaufen/ort-zuerich?pn=' + str(x) + '&r=100',headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'})
        print(r.status)
        soup = BeautifulSoup(r.data.decode('utf-8'), "lxml")
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


def getData(section, state, props):
    ids = props
    status("GETTING ALL DATA FOR SWITZERLAND RENT PROPERTIES USING THEIR UNIQUE IDS....")
    for id in ids:
        start = time.time()
        new_id = str(id)
        http = urllib3.PoolManager(ca_certs=certifi.where())

        if(new_id.startswith('https')):
            r = http.request('GET',new_id,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}, timeout=2.5)
        else:
            r = http.request('GET', 'https://www.immoscout24.ch' + new_id + '',headers={'User-Agent': ua.chrome}, timeout=2.5)
       
        soup = BeautifulSoup(r.data.decode('utf-8'), "lxml")
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

start = time.time()
getAllZurichRentProperties()
# getData("Rent", "Zurich", getAllZurichRentProperties())
# getData("Buy", "Zurich", getAllZurichBuyProperties())

cursor.close()
end = time.time()

print(end - start)