import numpy as np
import pandas as pd
from datetime import datetime
import pandas as pd
import numpy as np
import requests
import bs4
import urllib3
import config
import time
# import psycopg2
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
# from psycopg2 import OperationalError


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


Local_Dir = '/Users/cirillogirardi/airflow'


def find_swiss_watches_Cartier():
    urllib3.disable_warnings()
    agent = {"User-Agent": "Mozilla/5.0"}
    cartier_pages = []
    cartier = 'https://www.watches-of-switzerland.co.uk/c/Brands/Cartier?q=%3Arelevance&page=0'
    cartier_access = requests.get(cartier, verify=True, headers=agent)
    time.sleep(1)
    cartier_soup = soup(cartier_access.text, 'html.parser')
    # create a wrap to find the links for the individual watches
    time.sleep(1)
    cartier_wrap = cartier_soup.find_all(
        'a', {'href': lambda value: value and value.startswith('/Cartier')})
    for i in range(0, (len(cartier_wrap)-1)):
        cartier_pages.append(cartier_wrap[i].get("href"))

    cartier_pages_df = pd.DataFrame({'cartier_link': cartier_pages})
    cartier_pages_df.to_csv(Local_Dir + 'pages.csv')


def find_swiss_watches_AP():
    urllib3.disable_warnings()
    agent = {"User-Agent": "Mozilla/5.0"}
    website_pages = []
    website = "https://www.watches-of-switzerland.co.uk/c/Brands/Audemars-Piguet?q=%3Arelevance&page=0"
    website_access = requests.get(website, verify=True, headers=agent)
    time.sleep(1)
    website_soup = soup(website_access.text, 'html.parser')
    # create a wrap to find the links for the individual watches
    time.sleep(1)
    website_wrap = website_soup.find_all(
        'a', {'href': lambda value: value and value.startswith('/Audemars-Piguet')})
    for i in range(0, (len(website_wrap)-1)):
        website_pages.append(website_wrap[i].get("href"))

    AP_pages_df = pd.DataFrame({'AP_link': website_pages})
    AP_pages_df.to_csv(Local_Dir + 'pages1.csv')


def info_swiss_watches_AP():
    AP_pages_df = pd.read_csv(Local_Dir + 'pages1.csv')
    watch_code = []
    watch_guarantee = []
    watch_category = []
    watch_markers = []
    watch_strap = []
    watch_recipient = []
    watch_movement = []
    watch_dial = []
    watch_case = []
    watch_collection = []
    watch_reference = []
    watch_price = []
    watch_brand = []

    urllib3.disable_warnings()
    agent = {"User-Agent": "Mozilla/5.0"}

    for c in AP_pages_df['AP_link'][0:5]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = (AP_table[4]).text
        watch_markers.append(markers)

        strap = (AP_table[5]).text
        watch_strap.append(strap)

        recipient = (AP_table[6]).text
        watch_recipient.append(recipient)

        movement = (AP_table[7]).text
        watch_movement.append(movement)

        dial_color = (AP_table[8]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[9]).text
        watch_case.append(case_material)

        collection = (AP_table[10]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)
    for c in AP_pages_df['AP_link'][5:8]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = (AP_table[4]).text
        watch_markers.append(markers)

        strap = (AP_table[6]).text
        watch_strap.append(strap)

        recipient = (AP_table[7]).text
        watch_recipient.append(recipient)

        movement = (AP_table[8]).text
        watch_movement.append(movement)

        dial_color = (AP_table[9]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[10]).text
        watch_case.append(case_material)

        collection = (AP_table[11]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)

    for c in AP_pages_df['AP_link'][9:11]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = (AP_table[4]).text
        watch_markers.append(markers)

        strap = (AP_table[5]).text
        watch_strap.append(strap)

        recipient = (AP_table[6]).text
        watch_recipient.append(recipient)

        movement = (AP_table[7]).text
        watch_movement.append(movement)

        dial_color = (AP_table[8]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[9]).text
        watch_case.append(case_material)

        collection = (AP_table[10]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)

    for c in AP_pages_df['AP_link'][11:13]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = 'null'
        watch_markers.append(markers)

        strap = (AP_table[5]).text
        watch_strap.append(strap)

        recipient = (AP_table[6]).text
        watch_recipient.append(recipient)

        movement = (AP_table[7]).text
        watch_movement.append(movement)

        dial_color = (AP_table[8]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[9]).text
        watch_case.append(case_material)

        collection = (AP_table[11]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)

    for c in AP_pages_df['AP_link'][13:15]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = (AP_table[4]).text
        watch_markers.append(markers)

        strap = (AP_table[6]).text
        watch_strap.append(strap)

        recipient = (AP_table[7]).text
        watch_recipient.append(recipient)

        movement = (AP_table[8]).text
        watch_movement.append(movement)

        dial_color = (AP_table[9]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[10]).text
        watch_case.append(case_material)

        collection = (AP_table[11]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)

    for c in AP_pages_df['AP_link'][15:17]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = 'null'
        watch_markers.append(markers)

        strap = (AP_table[5]).text
        watch_strap.append(strap)

        recipient = (AP_table[6]).text
        watch_recipient.append(recipient)

        movement = (AP_table[7]).text
        watch_movement.append(movement)

        dial_color = (AP_table[8]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[9]).text
        watch_case.append(case_material)

        collection = (AP_table[11]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)

    for c in AP_pages_df['AP_link'][17:21]:
        AP_link = "https://www.watches-of-switzerland.co.uk" + c
        AP_access = requests.get(AP_link, verify=False, headers=agent)
        time.sleep(5)
        AP_soup = soup(AP_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        AP_table = AP_soup.find_all('span', {'class': 'specValue'})

        code = (AP_table[0]).text
        watch_code.append(code)

        guarantee = (AP_table[1]).text
        watch_guarantee.append(guarantee)

        category = (AP_table[2]).text
        watch_category.append(category)

        markers = (AP_table[4]).text
        watch_markers.append(markers)

        strap = (AP_table[6]).text
        watch_strap.append(strap)

        recipient = (AP_table[7]).text
        watch_recipient.append(recipient)

        movement = (AP_table[8]).text
        watch_movement.append(movement)

        dial_color = (AP_table[9]).text
        watch_dial.append(dial_color)

        case_material = (AP_table[10]).text
        watch_case.append(case_material)

        collection = (AP_table[12]).text
        watch_collection.append(collection)

        AP_spec = AP_soup.find_all('span', {'class': 'productManCode'})

        reference = AP_spec[0].text
        watch_reference.append(reference)

        AP_price = AP_soup.find_all('span', {'class': 'productPrice'})

        price = AP_price[0].get_text(strip=True)
        watch_price.append(price)

        AP_brand = AP_soup.find_all('span', {'class': 'productBrandName'})

        brand = AP_brand[0].get_text(strip=True)
        watch_brand.append(brand)

    AP_watch_df = pd.DataFrame({'watch_code': watch_code,
                                'watch_brand': watch_brand,
                                'watch_collection': watch_collection,
                                'watch_reference': watch_reference,
                                'watch_price': watch_price,
                                'watch_movement': watch_movement,
                                'watch_case': watch_case,
                                'watch_dial': watch_dial,
                                'watch_strap': watch_strap,
                                'watch_markers': watch_markers,
                                'watch_guarantee': watch_guarantee,
                                'watch_category': watch_category,
                                'watch_recipient': watch_recipient

                                })
    AP_watch_df.to_csv(Local_Dir + 'AP_watches.csv')


def info_swiss_watches_Cartier():
    cartier_pages_df = pd.read_csv(Local_Dir + 'pages.csv')

    cwatch_code = []
    cwatch_guarantee = []
    cwatch_category = []
    cwatch_markers = []
    cwatch_strap = []
    cwatch_recipient = []
    cwatch_movement = []
    cwatch_dial = []
    cwatch_case = []
    cwatch_collection = []
    cwatch_reference = []
    cwatch_price = []
    cwatch_brand = []

    for c in cartier_pages_df["cartier_link"][0:25]:
        cartier_link = "https://www.watches-of-switzerland.co.uk" + c
        urllib3.disable_warnings()
        agent = {"User-Agent": "Mozilla/5.0"}
        cartier_watch_access = requests.get(cartier_link, verify=False, headers=agent)
        time.sleep(5)
        cartier_watch_soup = soup(cartier_watch_access.text, 'html.parser')
        # find the table of information for each watch
        time.sleep(6)
        cartier_table = cartier_watch_soup.find_all('span', {'class': 'specValue'})

        code = (cartier_table[0]).text
        cwatch_code.append(code)

        guarantee = (cartier_table[1]).text
        cwatch_guarantee.append(guarantee)

        category = (cartier_table[2]).text
        cwatch_category.append(category)

        markers = (cartier_table[4]).text
        cwatch_markers.append(markers)

        strap = (cartier_table[6]).text
        cwatch_strap.append(strap)

        recipient = (cartier_table[7]).text
        cwatch_recipient.append(recipient)

        movement = (cartier_table[8]).text
        cwatch_movement.append(movement)

        dial_color = (cartier_table[9]).text
        cwatch_dial.append(dial_color)

        case_material = (cartier_table[10]).text
        cwatch_case.append(case_material)

        collection = (cartier_table[12]).text
        cwatch_collection.append(collection)

        Cartier_spec = cartier_watch_soup.find_all('span', {'class': 'productManCode'})

        reference = Cartier_spec[0].text
        cwatch_reference.append(reference)

        Cartier_price = cartier_watch_soup.find_all('span', {'class': 'productPrice'})

        price = Cartier_price[0].get_text(strip=True)
        cwatch_price.append(price)

        Cartier_brand = cartier_watch_soup.find_all('span', {'class': 'productBrandName'})

        brand = Cartier_brand[0].get_text(strip=True)
        cwatch_brand.append(brand)

    watch = pd.DataFrame({'watch_code': cwatch_code,
                          'brand': cwatch_brand,
                          'watch_collection': cwatch_collection,
                          'watch_reference': cwatch_reference,
                          'watch_price': cwatch_price,
                          'watch_movement': cwatch_movement,
                          'watch_case': cwatch_case,
                          'watch_dial': cwatch_dial,
                          'watch_strap': cwatch_strap,
                          'watch_markers': cwatch_markers,
                          'watch_guarantee': cwatch_guarantee,
                          'watch_category': cwatch_category,
                          'watch_recipient': cwatch_recipient
                          })

    watch.to_csv(Local_Dir + 'watches.csv')


def swiss_editing():
    AP = pd.read_csv(Local_Dir + 'AP_watches.csv')
    df = pd.read_csv(Local_Dir + 'watches.csv')
    df = df[~df['watch_collection'].astype(str).str.startswith('1')]

    AP = AP.append(df)
    AP.reset_index(inplace=True)
    AP.drop(columns=['index'], inplace=True)
    AP.drop(columns=['Unnamed: 0'], inplace=True)
    AP['watch_reference'] = AP['watch_reference'].str.split('/').str[0]
    AP.to_csv(Local_Dir + 'watches.csv')


def find_social_info():

    names = []
    company_names = []
    descriptions = []
    telephones = []
    websites = []
    facebooks = []
    twitters = []
    instagrams = []
    weibos = []
    googles = []
    lines = []
    wechats = []
    youtubes = []

    urllib3.disable_warnings()
    agent = {"User-Agent": "Mozilla/5.0"}

    AP = 'https://fashionbi.com/brands/audemars-piguet/financials'
    web = requests.get(AP, verify=True, headers=agent)
    time.sleep(8)
    page_soup = soup(web.content, 'html.parser')

    h_name = page_soup.find_all('div', {'class': 'mtop10'})
    name = h_name[0].text
    names.append(name)

    c_name = page_soup.find_all('div', {'class': 'col-sm-6'})[1]
    company = [i.strip() for i in c_name if i.name is None and i.strip() != '']
    company_name = company[0]
    company_names.append(company_name)

    info = page_soup.find_all('p')
    description = info[1].text
    descriptions.append(description)

    c_telephone = page_soup.find_all('div', {'class': 'col-sm-6'})[2]
    telephone = [i.strip() for i in c_telephone if i.name is None and i.strip() != '']
    company_telephone = telephone[0]
    telephones.append(company_telephone)

    media = page_soup.find_all('a', {'target': '_blank'})

    website = media[1].get("href")
    websites.append(website)

    facebook = media[2].get("href")
    facebooks.append(facebook)

    twitter = media[3].get("href")
    twitters.append(twitter)

    instagram = media[4].get("href")
    instagrams.append(instagram)

    weibo = media[5].get("href")
    weibos.append(weibo)

    google = media[6].get("href")
    googles.append(google)

    line = media[7].get("href")
    lines.append(line)

    wechat = media[8].get("href")
    wechats.append(wechat)

    youtube = media[9].get("href")
    youtubes.append(youtube)

    Cartier = 'https://fashionbi.com/brands/cartier/financials'
    Cartier_web = requests.get(Cartier, verify=True, headers=agent)
    time.sleep(8)
    page_soup1 = soup(Cartier_web.content, 'html.parser')

    h_name = page_soup1.find_all('div', {'class': 'mtop10'})
    name = h_name[0].text
    names.append(name)

    c_name = page_soup1.find_all('div', {'class': 'col-sm-6'})[7]
    company = [i.strip() for i in c_name if i.name is None and i.strip() != '']
    company_name = company[0]
    company_names.append(company_name)

    info = page_soup1.find_all('p')
    description = info[7].text
    descriptions.append(description)

    c_telephone = page_soup1.find_all('div', {'class': 'col-sm-6'})[8]
    telephone = [i.strip() for i in c_telephone if i.name is None and i.strip() != '']
    company_telephone = telephone[0]
    telephones.append(company_telephone)

    media = page_soup1.find_all('a', {'target': '_blank'})

    website = media[1].get("href")
    websites.append(website)

    facebook = media[2].get("href")
    facebooks.append(facebook)

    twitter = media[3].get("href")
    twitters.append(twitter)

    instagram = media[4].get("href")
    instagrams.append(instagram)

    weibo = media[5].get("href")
    weibos.append(weibo)

    google = 'null'
    googles.append(google)

    line = media[6].get("href")
    lines.append(line)

    wechat = 'null'
    wechats.append(wechat)

    youtube = media[10].get("href")
    youtubes.append(youtube)

    social_info_df = pd.DataFrame({'brand': names,
                                   'holding_company': company_names,
                                   'company_description': descriptions,
                                   'telephone_number': telephones,
                                   'company_website': websites,
                                   'facebook': facebooks,
                                   'twitter': twitters,
                                   'instagram': instagrams,
                                   'weibo': weibos,
                                   'google': googles,
                                   'line': lines,
                                   'wechat': wechats,
                                   'youtube': youtubes
                                   })

    social_info_df.to_csv(Local_Dir + 'social_info.csv')


def find_company_info_AP():
    urllib3.disable_warnings()
    agent = {"User-Agent": "Mozilla/5.0"}
    APlink = "https://en.wikipedia.org/wiki/Audemars_Piguet"
    APaccess = requests.get(APlink, verify=False, headers=agent)
    time.sleep(1)
    APpage_soup = soup(APaccess.text, 'html.parser')
    # find the table of information for each watch
    time.sleep(1)
    APtable = APpage_soup.find_all('table')
    APdf = pd.read_html(str(APtable))[0]
    # transpose the table
    APdf = APdf.T
    # only get the second row
    new_header = [['Brand', 'Type', 'Industry', 'Founded', 'Founder', 'Headquarters',
                   ' Area served', 'Key people', 'Products', 'Production output', 'Revenue', 'Number of employees',
                   'Subsidiaries', 'Website']]
    APdf = APdf[1:]  # take the data less the header row
    APdf.columns = new_header
    APdf['Brand'] = APdf['Brand'].replace(np.nan, 'Audemars Piguet')
    APdf.drop(['Key people', 'Production output', 'Revenue', 'Number of employees',
              'Subsidiaries', 'Website'], axis=1, inplace=True)
    APdf['Founder'] = APdf['Founder'].replace(
        'Jules Louis AudemarsEdward Auguste Piguet', 'Jules Louis Audemars & Edward Auguste Piguet')
    APdf.to_csv(Local_Dir + 'AP_company_info.csv')


def find_company_info_Cartier():
    urllib3.disable_warnings()
    agent = {"User-Agent": "Mozilla/5.0"}
    Clink = "https://en.wikipedia.org/wiki/Cartier_(jeweler)"
    Caccess = requests.get(Clink, verify=False, headers=agent)
    time.sleep(1)
    Cpage_soup = soup(Caccess.text, 'html.parser')
    # find the table of information for each watch
    time.sleep(1)
    Ctable = Cpage_soup.find_all('table')
    Cdf = pd.read_html(str(Ctable))[0]
    # transpose the table
    Cdf = Cdf.T
    # only get the second row
    new_header = [['Brand', 'Type', 'Industry', 'Founded', 'Founder', 'Headquarters',
                   ' Area served', 'Key people', 'Products', 'Revenue', 'Parent', 'Website']]
    Cdf = Cdf[1:]  # take the data less the header row
    Cdf.columns = new_header
    Cdf['Brand'] = Cdf['Brand'].replace(np.nan, 'Cartier')
    Cdf['Industry'] = Cdf['Industry'].replace(
        'Jewellery manufacturing,watchmaking', 'Jewellery manufacturing, Luxury watchmaking')
    Cdf.to_csv(Local_Dir + 'company_info.csv')


def find_financial_info():
    company_names = []
    employee_number = []
    revenues = []
    net_income_growths = []
    enviornmental_social_governance_rankings = []
    managing_directors = []
    board_chairmans = []
    directors = []
    subsidiaries_number = []

    link = 'https://www.dnb.com/business-directory/company-profiles.compagnie_financiere_richemont_sa.dc0156d49253885e64102460d6cdcd77.html'
    time.sleep(8)
    option = webdriver.ChromeOptions()
    time.sleep(8)
    driver = webdriver.Chrome("/Users/cirillogirardi/Downloads/chromedriver", options=option)
    time.sleep(6)
    driver.get(link)

    data = driver.find_elements_by_class_name('company-profile-header-title')
    company_name = data[0].text
    company_names.append(company_name)

    info = driver.find_elements_by_xpath("//span[@class='company_data_point']/span")
    employees = info[11].text
    employee_number.append(employees)

    revenue = info[12].text
    revenues.append(revenue)

    net_income_growth = info[14].text
    net_income_growths.append(net_income_growth)

    enviornmental_social_governance_ranking = info[20].text
    enviornmental_social_governance_rankings.append(enviornmental_social_governance_ranking)

    names = driver.find_elements_by_class_name("name")
    managing_director = names[0].text
    managing_directors.append(managing_director)

    board_chairman = names[1].text
    board_chairmans.append(board_chairman)

    director = names[2].text
    directors.append(director)

    sub = driver.find_elements_by_class_name('company_name')
    subsidiaries = sub[1].text
    subsidiaries_number.append(subsidiaries)

    a_link = 'https://www.dnb.com/business-directory/company-profiles.audemars_piguet_holding_sa.68acd45e712da185227042edb4487a38.html'
    time.sleep(8)
    option = webdriver.ChromeOptions()
    time.sleep(8)
    driverr = webdriver.Chrome("/Users/cirillogirardi/Downloads/chromedriver", options=option)
    time.sleep(6)
    driverr.get(a_link)

    data = driverr.find_elements_by_class_name('company-profile-header-title')
    a_company_name = data[0].text
    company_names.append(a_company_name)

    info = driverr.find_elements_by_xpath("//span[@class='company_data_point']/span")
    a_employees = info[12].text
    employee_number.append(a_employees)

    a_revenue = info[13].text
    revenues.append(a_revenue)

    a_net_income_growth = 'null'
    net_income_growths.append(a_net_income_growth)

    a_enviornmental_social_governance_ranking = info[16].text
    enviornmental_social_governance_rankings.append(a_enviornmental_social_governance_ranking)

    names = driverr.find_elements_by_class_name("name")
    a_managing_director = names[0].text
    managing_directors.append(a_managing_director)

    a_board_chairman = names[1].text
    board_chairmans.append(a_board_chairman)

    a_director = names[2].text
    directors.append(a_director)

    sub = driverr.find_elements_by_class_name('company_name')
    a_subsidiaries = sub[1].text
    subsidiaries_number.append(a_subsidiaries)

    company_financial_df = pd.DataFrame({'holding_company': company_names,
                                         'employees': employee_number,
                                         'revenue': revenues,
                                         'net_income_growth': net_income_growths,
                                         'enviornmental_social_governance_ranking': enviornmental_social_governance_rankings,
                                         'managing_director': managing_directors,
                                         'board_chairman': board_chairmans,
                                         'director': directors,
                                         'subsidiaries': subsidiaries_number
                                         })

    company_financial_df.to_csv(Local_Dir + 'financial_info.csv')


def company_editing():
    AP = df = pd.read_csv(Local_Dir + "AP_company_info.csv")
    df = pd.read_csv(Local_Dir + "company_info.csv")
    df = AP.append(df)
    df2 = pd.read_csv(Local_Dir + "social_info.csv")
    desc = df2["company_description"]
    df = df.join(desc)
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.columns = ['brand', 'company_type', 'industry', 'founded', 'founder', 'headquarters', 'area_served',
                  'products', 'company_description']
    df3 = pd.read_csv(Local_Dir + 'financial_info.csv')
    df3 = df3.reindex([1, 0])
    df3.reset_index(inplace=True)
    hc = df3['holding_company']
    df = df.join(hc)
    df.to_csv(Local_Dir + 'more_information.csv')


def find_chrono24():
    references_final = []
    year_final = []
    condition_final = []
    box_final = []
    seller_location_final = []
    case_diameter_final = []
    price_final = []
    rating_final = []
    for page in range(1, 4):
        Cartier_new = 'https://www.chrono24.com/search/index.htm?currencyId=USD&dosearch=true&manufacturerIds=43&maxAgeInDays=0&pageSize=120&redirectToSearchIndex=true&referenceNumber=w2sa0016%2443&referenceNumber=w51008q3%2443&referenceNumber=wsta0005%2443&referenceNumber=w6701004%2443&referenceNumber=wsta0051%2443&referenceNumber=wssa0039%2443&referenceNumber=wsta0052%2443&referenceNumber=wsta0053%2443&referenceNumber=wssa0037%2443&referenceNumber=wssa0018%2443&referenceNumber=wsta0041%2443&referenceNumber=wsta0040%2443&referenceNumber=w51007q4%2443&referenceNumber=w2sa0009%2443&referenceNumber=wssa0029%2443&referenceNumber=wsbb0048%2443&resultview=list&showpage={}&sortorder=0'.format(
            page)
        urllib3.disable_warnings()
        agent = {"User-Agent": "Mozilla/5.0"}
        web = requests.get(Cartier_new, verify=True, headers=agent)
        time.sleep(5)
        new_page_soup = soup(web.text, 'html.parser')
        new_wrap = new_page_soup.findAll(
            'div', {'class': 'article-item-container wt-search-result'})
        for i in range(0, (len(new_wrap)-1)):

            try:

                desc = new_wrap[i].find('div', {'class': 'article-description d-none d-sm-flex'})
                st = desc.find_all('strong')

            except:
                continue

            try:
                reference = st[4].text
                references_final.append(reference)
            except:
                references_final.append('null')

            try:
                YOP = st[2].text
                year_final.append(YOP)
            except:
                year_final.append('null')

            try:
                condition = st[3].text
                condition_final.append(condition)
            except:
                condition_final.append('null')

            try:
                box = st[5].text
                box_final.append(box)
            except:
                box_final.append('null')

            try:
                seller_location = st[6].text
                seller_location_final.append(seller_location)
            except:
                seller_location_final.append('null')

            try:
                case_diameter = st[7].text
                case_diameter_final.append(case_diameter)
            except:
                case_diameter_final.append('null')

            try:
                price = new_wrap[i].find('div', {'class': 'article-price'})
                pr = price.find_all('strong')
                watch_price = pr[0].text
                price_final.append(watch_price)
            except:
                price_final.append('null')

            try:
                rat = new_wrap[i].find('span', {'class': 'text-bold'})
                rating = rat.text
                rating_final.append(rating)
            except:
                rating_final.append('null')

    for x in range(1, 4):
        AP_new = 'https://www.chrono24.co.uk/search/index.htm?currencyId=GBP&dosearch=true&manufacturerIds=18&maxAgeInDays=0&pageSize=120&redirectToSearchIndex=true&referenceNumber=15500orood002cr01%2418&referenceNumber=26420roooa002ca01%2418&referenceNumber=77350stoo1261st01%2418&referenceNumber=26420ioooa009ca01%2418&referenceNumber=15500oroo1220or01%2418&referenceNumber=26420soooa600ca01%2418&referenceNumber=26231orzzd003ca01%2418&referenceNumber=15720stooa052ca01%2418&referenceNumber=67650stoo1261st01%2418&referenceNumber=77351stzz1261st01%2418&referenceNumber=26420tiooa027ca01%2418&referenceNumber=26231stzzd027ca01%2418&referenceNumber=77350sroo1261sr01%2418&referenceNumber=15720stooa009ca01%2418&referenceNumber=15720stooa027ca01%2418&referenceNumber=26420soooa002ca01%2418&referenceNumber=26231stzzd002ca01%2418&referenceNumber=67651srzz1261sr01%2418&referenceNumber=77351orzz1261or01%2418&resultview=list&showpage={}&sortorder=0'.format(
            x)
        urllib3.disable_warnings()
        agent = {"User-Agent": "Mozilla/5.0"}
        web = requests.get(AP_new, verify=True, headers=agent)
        time.sleep(5)
        new_page_soup = soup(web.text, 'html.parser')
        new_wrap = new_page_soup.findAll(
            'div', {'class': 'article-item-container wt-search-result'})
        for i in range(0, (len(new_wrap)-1)):

            try:

                desc = new_wrap[i].find('div', {'class': 'article-description d-none d-sm-flex'})
                st = desc.find_all('strong')

            except:
                continue

            try:
                reference = st[4].text
                references_final.append(reference)
            except:
                references_final.append('null')

            try:
                YOP = st[2].text
                year_final.append(YOP)
            except:
                year_final.append('null')

            try:
                condition = st[3].text
                condition_final.append(condition)
            except:
                condition_final.append('null')

            try:
                box = st[5].text
                box_final.append(box)
            except:
                box_final.append('null')

            try:
                seller_location = st[6].text
                seller_location_final.append(seller_location)
            except:
                seller_location_final.append('null')

            try:
                case_diameter = st[7].text
                case_diameter_final.append(case_diameter)
            except:
                case_diameter_final.append('null')

            try:
                price = new_wrap[i].find('div', {'class': 'article-price'})
                pr = price.find_all('strong')
                watch_price = pr[0].text
                price_final.append(watch_price)
            except:
                price_final.append('null')

            try:
                rat = new_wrap[i].find('span', {'class': 'text-bold'})
                rating = rat.text
                rating_final.append(rating)
            except:
                rating_final.append('null')

    watches_final = pd.DataFrame({'reference': references_final,
                                  'price': price_final,
                                  'conditions': condition_final,
                                  'production_year': year_final,
                                  'case_diameter': case_diameter_final,
                                  'box_and_papers': box_final,
                                  'seller_location': seller_location_final,
                                  'seller_rating': rating_final
                                  })
    watches_final.to_csv(Local_Dir + 'watches_final.csv')


def info_chrono24():
    df = pd.read_csv(Local_Dir + 'watches_final.csv')
    df = df.replace('\n\r\n', '', regex=True)
    df = df.replace('\r\n', '', regex=True)
    df = df.replace('\n', '', regex=True)
    df.to_csv(Local_Dir + 'final_watches.csv')


def chrono_editing():
    df = pd.read_csv(Local_Dir + 'final_watches.csv')
    df['reference'] = df['reference'].str.upper()
    df.drop(columns=['Unnamed: 0'], inplace=True)
    df.rename(columns={'reference': 'watch_reference'}, inplace=True)
    df.rename(columns={'price': 'aftermarket_price'}, inplace=True)
    df.rename(columns={'conditions': 'condition'}, inplace=True)
    df['watch_reference'] = df['watch_reference'].str.lstrip()
    df1 = pd.read_csv(Local_Dir + 'watches.csv')
    df1.drop(columns=['Unnamed: 0'], inplace=True)
    df1.rename(columns={'watch_price': 'price'}, inplace=True)
    df1['watch_reference'] = df1['watch_reference'].str.split('/').str[0]
    selected = df1[["watch_reference", "price", "watch_movement", "watch_case",
                    "watch_dial", "watch_strap", "watch_markers", "watch_guarantee",
                    "watch_recipient"]]
    df3 = pd.merge(df, selected, how='outer')
    df4 = df3[df3['watch_reference'].isin(df1['watch_reference'])]
    watch_id = ['W' + str(i) for i in range(0, len(df4))]
    df4.insert(0, "watch_id", watch_id)
    seller_id = ['S' + str(i) for i in range(0, len(df4))]
    df4.insert(7, "seller_id", seller_id)
    df4 = df4.reset_index(drop=True)
    df4.to_csv(Local_Dir + 'last.csv')


def upload_S3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'concurrency': 1,
    'retries': 0
}

with DAG(
    dag_id='final_watches',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(year=2022, month=1, day=1),
    catchup=False
) as dag:

    find_swiss_watches_Cartier = PythonOperator(
        task_id='find_swiss_watches_Cartier', python_callable=find_swiss_watches_Cartier)

    find_swiss_watches_AP = PythonOperator(
        task_id='find_swiss_watches_AP', python_callable=find_swiss_watches_AP)

    info_swiss_watches_Cartier = PythonOperator(
        task_id='info_swiss_watches_Cartier', python_callable=info_swiss_watches_Cartier)

    info_swiss_watches_AP = PythonOperator(
        task_id='info_swiss_watches_AP', python_callable=info_swiss_watches_AP)

    swiss_editing = PythonOperator(
        task_id='swiss_editing', python_callable=swiss_editing)

    find_social_info = PythonOperator(
        task_id='find_social_info', python_callable=find_social_info)

    find_company_info_Cartier = PythonOperator(
        task_id='find_company_info_Cartier', python_callable=find_company_info_Cartier)

    find_company_info_AP = PythonOperator(
        task_id='find_company_info_AP', python_callable=find_company_info_AP)

    find_financial_info = PythonOperator(
        task_id='find_financial_info', python_callable=find_financial_info)

    company_editing = PythonOperator(
        task_id='company_editing', python_callable=company_editing)

    find_chrono24 = PythonOperator(
        task_id='find_chrono24', python_callable=find_chrono24)

    info_chrono24 = PythonOperator(
        task_id='info_chrono24', python_callable=info_chrono24)

    sleep = BashOperator(
        task_id='sleep', bash_command='sleep 5')

    chrono_editing = PythonOperator(
        task_id='chrono_editing', python_callable=chrono_editing)

    sleep_again = BashOperator(
        task_id='sleep_again', bash_command='sleep 10')

    upload_S3_finalized_watches = PythonOperator(
        task_id='upload_S3_finalized_watches',
        python_callable=upload_S3,
        op_kwargs={
                'filename': Local_Dir + 'last.csv',
                'key': 'last.csv',
                'bucket_name': 'cartier-bucket'
        }
    )
    upload_S3_financial_info = PythonOperator(
        task_id='upload_S3_financial_info',
        python_callable=upload_S3,
        op_kwargs={
                'filename': Local_Dir + 'financial_info.csv',
                'key': 'financial_info.csv',
                'bucket_name': 'cartier-bucket'
        }
    )
    upload_S3_company_information = PythonOperator(
        task_id='upload_S3_company_information',
        python_callable=upload_S3,
        op_kwargs={
                'filename': Local_Dir + 'more_information.csv',
                'key': 'more_information.csv',
                'bucket_name': 'cartier-bucket'
        }
    )

    upload_S3_social_info = PythonOperator(
        task_id='upload_S3_social_info',
        python_callable=upload_S3,
        op_kwargs={
            'filename': Local_Dir + 'social_info.csv',
            'key': 'social_info.csv',
            'bucket_name': 'cartier-bucket'
        }
    )

    upload_S3_swiss_watches = PythonOperator(
        task_id='upload_S3_swiss_watches',
        python_callable=upload_S3,
        op_kwargs={
            'filename': Local_Dir + 'watches.csv',
            'key': 'watches.csv',
            'bucket_name': 'cartier-bucket'
        }
    )

    [find_chrono24, find_swiss_watches_Cartier, find_social_info, find_company_info_Cartier, find_company_info_AP, find_financial_info] >> sleep >> [
        info_swiss_watches_Cartier, info_swiss_watches_AP, info_chrono24] >> company_editing >> swiss_editing >> chrono_editing >> sleep_again >> [upload_S3_finalized_watches, upload_S3_financial_info, upload_S3_company_information, upload_S3_social_info, upload_S3_swiss_watches]
