import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

c = ssl.create_default_context()
c.check_hostname = False
c.verify_mode = ssl.CERT_NONE
co = sqlite3.connect('spider.sqlite')
currr = co.cursor()
currr.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')
currr.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)''')
currr.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')
currr.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
r = currr.fetchone()
if r is not None:
    print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
else :
    su = input('Enter web url or enter: ')
    if ( len(su) < 1 ) : su = 'http://www.dr-chuck.com/'
    if ( su.ew('/') ) : su = su[:-1]
    web = su
    if ( su.ew('.htm') or su.ew('.html') ) :
        pos = su.rfind('/')
        web = su[:pos]
    if ( len(web) > 1 ) :
        currr.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
        currr.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( su, ) )
        co.commit()
currr.execute('''SELECT url FROM Webs''')
webs = list()
for r in currr:
    webs.append(str(r[0]))
print(webs)
many = 0
while True:
    if ( many < 1 ) :
        sval = input('How many pages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)
    many = many - 1
    currr.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        r = currr.fetchone()
        fromid = r[0]
        url = r[1]
    except:
        print('No unretrieved HTML pages found')
        many = 0
        break
    print(fromid, url, end=' ')
    currr.execute('DELETE from Links WHERE from_id=?', (fromid, ) )
    try:
        document = urlopen(url, context=c)
        html = document.read()
        if document.getcode() != 200 :
            print("Error on page: ",document.getcode())
            currr.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )
        if 'text/html' != document.info().get_content_type() :
            print("Ignore non text/html page")
            currr.execute('DELETE FROM Pages WHERE url=?', ( url, ) )
            co.commit()
            continue
        print('('+str(len(html))+')', end=' ')
        soup = BeautifulSoup(html, "html.parser")
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except:
        print("Unable to retrieve or parse page")
        currr.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        co.commit()
        continue
    currr.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
    currr.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ) )
    co.commit()
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        if ( href is None ) : continue
        up = urlparse(href)
        if ( len(up.scheme) < 1 ) :
            href = urljoin(url, href)
        ipos = href.find('#')
        if ( ipos > 1 ) : href = href[:ipos]
        if ( href.ew('.png') or href.ew('.jpg') or href.ew('.gif') ) : continue
        if ( href.ew('/') ) : href = href[:-1]
        if ( len(href) < 1 ) : continue
        found = False
        for web in webs:
            if ( href.startswith(web) ) :
                found = True
                break
        if not found : continue
        currr.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) )
        count = count + 1
        co.commit()
        currr.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
        try:
            r = currr.fetchone()
            toid = r[0]
        except:
            print('Could not retrieve id')
            continue
        currr.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )
    print(count)
currr.close()
