
import requests
from bs4 import BeautifulSoup
import mysql.connector



#pgurl=input(f'Please enter the url - ')

DBHOST="localhost"
DBUSER="root"
#DBPASS="q46994W#33"
DBNM="ogrn"


pgn=[]

with open('links') as file:
    parsed = ''.join(file.readlines()).strip().split('\n')
    for parseds in parsed:
        urls = requests.get(parseds)
        pgurls = urls.content

        soup = BeautifulSoup(pgurls, "html.parser")

        a1 = soup.find('link', rel="canonical").get('href').strip()
        a2 = soup.find(class_ = 'company-name').get_text().strip()
        a4 = soup.find('div', class_='company-header__row').get_text().strip()
        a9 = soup.find('span', class_='company-info__text').get_text().strip()
        a10 = soup.find('address', itemprop="address").get_text().strip().replace("\n", "")
        a11 = soup.find('div', class_="founder-item__title").get_text().strip()
        if soup.find('div', class_='company-status active-yes') != None:
            a3 = soup.find('div', class_='company-status active-yes').get_text().strip()
        else:
            a3 = ''
        dls = soup.findAll('dl', class_="company-col")
        for dl in dls:
            if dl.findNext('dt', class_='company-info__title').get_text().strip() == 'ИНН/КПП':
                a5 = dl.findNext('dd').get_text().strip()
            if dl.findNext('dt',  class_='company-info__title').get_text().strip() == 'ОГРН':
                a6 = dl.findNext('dd').get_text().strip()
            if dl.findNext('dt',  class_='company-info__title').get_text().strip() == 'Дата регистрации':
                a7 = dl.findNext('dd').get_text().strip()
            if dl.findNext('dt',  class_='company-info__title').get_text().strip() == 'Уставный капитал':
                a8 = dl.findNext('dd').get_text().strip()
            else:
                pass

        count = 1
        data = {
            'a1':a1,
            'a2': a2,
            'a3': a3,
            'a4': a4,
            'a7': a7,
            'a8': a8,
            'a9': a9,
            'a10': a10,
            'a11': a11,
        }
        pgn.append(data)
        for pgns in pgn:
            count = count + 1
            print(f'{count}{pgns}')

        mydb = mysql.connector.connect(
            host=DBHOST,
            user=DBUSER,
            # password=DBPASS,
            database=DBNM,
        )
        mycursor = mydb.cursor()
        val = (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)
        sql = "INSERT INTO parsed3 (a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()

    mydb = mysql.connector.connect(
        host=DBHOST,
        user=DBUSER,
        #password=DBPASS,
        database=DBNM,
    )
    mycursor = mydb.cursor()
    sql1 = "UPDATE parsed3 SET parsed3.a11 = SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(parsed3.a7, ' ', 3),' ',2),' ',-1);"
    sql2 = "UPDATE parsed3 INNER JOIN months ON parsed3.a11 = months.mnts SET parsed3.mnt = months.mtnnm;"
    sql3 = "UPDATE parsed3 SET parsed3.dy = SUBSTRING_INDEX(parsed3.a7' ',1);"
    sql4 = "UPDATE parsed3 SET parsed3.yr = SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(parsed3.a7, ' ', 3),' ',-1),' ',-1);"
    sql5 = "UPDATE parsed3 SET parsed3.a11 = CONCAT(parsed3.dy, '.', parsed3.mnt, '.', parsed3.yr);"
    sql6 = "UPDATE parsed3 SET parsed3.rgn = SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(a5, ',', 3),',',2),',',-1);"
    mycursor.execute(sql1)
    mydb.commit()
    mycursor.execute(sql2)
    mydb.commit()
    mycursor.execute(sql3)
    mydb.commit()
    mycursor.execute(sql4)
    mydb.commit()
    mycursor.execute(sql5)
    mydb.commit()
    mycursor.execute(sql6)
    mydb.commit()
    mycursor.close()




