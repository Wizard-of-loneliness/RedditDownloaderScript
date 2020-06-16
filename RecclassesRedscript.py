import praw
import requests
import json
import os
import datetime
import mysql.connector
import logging
from xml.dom import minidom
import sys
import bs4
import lxml
import AuthandGVs
from time import sleep
from queue import Queue
from threading import Thread

reddit = praw.Reddit(client_id=AuthandGVs.Reddit_client_id,
                     client_secret=AuthandGVs.Reddit_client_secret,
                     username=AuthandGVs.Reddit_username,
                     password=AuthandGVs.Reddit_password,
                     user_agent=AuthandGVs.Reddit_user_agent)

subreddits = AuthandGVs.subreddit_list
download_path = AuthandGVs.download_path
DBchin = input(
    '\nDo you want to use DataBase for the Downloads to avoid Duplicate downloads?(Y(or any value)/N) : ')
if DBchin != '' and DBchin.lower()[0] == 'n':
    DBconn = False
    print('\nDatabase dependability terminated...............................')
else:
    try:
        mydb = mysql.connector.connect(host="localhost", user=AuthandGVs.mysql_user,
                                       passwd=AuthandGVs.mysql_password, database=AuthandGVs.mysql_database)
        mycurser = mydb.cursor(buffered=True)
    except Exception as e:
        print('\n' + str(e))
        print('Due to above error, MYSQL Database Dependecy for Downloads is terminated, which may lead to Duplicate Downloads')
        while True:
            dbinput = input('Do you wish to continue(Y/N) :')
            if dbinput != '' and dbinput.lower()[0] in ['y', 'n']:
                break
        if dbinput.lower()[0] == 'y':
            print('\nContinuing Downloads without Database Dependency..........')
            DBconn = False
        if dbinput.lower()[0] == 'n':
            print('\nPlease check DB GV values(host, user, passwd, database) and try again...Terminating the script...')
            sys.exit()
    else:
        DBconn = True

timestamp = str(datetime.datetime.now())
datep = f"'{timestamp[:10]}'"
logs_path = AuthandGVs.logs_path
log_filename = logs_path + 'RB' + timestamp[:10] + '.log'
logging.basicConfig(filename=log_filename,
                    level=logging.WARNING, format='%(message)s')
default_range = AuthandGVs.default_range
default_setting_type = AuthandGVs.default_setting_type
defaultlimit = AuthandGVs.defaultlimit
NewIDcounter = 0
Retrylist = []
sheerlist_dict = {}
downloaderQueue = Queue()
download_pauser = 0
loopbreakerlist = []


class Interfaces:
    def directimage(self, old):
        self.old = old
        new = download_path + self.old.split('/')[-1]
        image_response = requests.get(self.old)
        return [(new, image_response)]

    def gfyvid(self, old):
        self.old = old
        try:
            gfyID = self.old.split('/')[-1]
            gfyAPI_res = requests.get(
                f'https://api.gfycat.com/v1/gfycats/{gfyID}')
            gfyJson_res = json.loads(gfyAPI_res.text)
            gfyvid_url = gfyJson_res['gfyItem']['mp4Url']
            new = download_path + gfyID + '.mp4'
            image_response = requests.get(gfyvid_url)
            return [(new, image_response)]
        except KeyError:
            soup = bs4.BeautifulSoup(requests.get(self.old).text, 'lxml')
            if 'gfycat' in self.old:
                file_link = soup.select('#mp4Source')[0]['src']
            else:
                file_link = soup.select('source')[-1]['src']
            try:
                touple_list = self.directimage(file_link)
                return touple_list
            except requests.exceptions.ChunkedEncodingError:
                downloaderQueue.put(file_link)
            except Exception as scrap_error:
                logging.warning(str(hot_post)+'    ' + old +
                                '    '+str(scrap_error).replace(' ', '_'))
        except Exception as APIerror:
            logging.warning(str(hot_post)+'    ' + old +
                            '    '+str(APIerror).replace(' ', '_'))

    def imgur(self, old):
        self.old = old
        touple_list = [(None, None)]
        try:
            imgurID = self.old.split('/')[-1].split('_')[0]
        except:
            imgurID = self.old.split('/')[-1]
        header = {'Authorization': f'Client-ID {AuthandGVs.Imgur_ClientID}'}
        file_link_list = []
        if (('imgur.com/a/') not in self.old) and (('imgur.com/gallery/') not in self.old):
            imgurAPI_res = requests.get(
                f'https://api.imgur.com/3/image/{imgurID}', headers=header)
            imgurJson_res = json.loads(imgurAPI_res.text)
            file_link_list = [imgurJson_res['data']['link']]
        else:
            imgurAPI_res = requests.get(
                f"https://api.imgur.com/3/album/{imgurID}/images", headers=header)
            imgurJson_res = json.loads(imgurAPI_res.text)
            file_link_buffer = imgurJson_res['data']
            for fille_link_dict in file_link_buffer:
                file_link_list.append(fille_link_dict['link'])
        if len(file_link_list) < 11:
            touple_list = []
            for file_link in file_link_list:
                touple = self.directimage(file_link)[0]
                touple_list.append(touple)
        else:
            sheerlist_dict[imgurID] = file_link_list
        return touple_list

    def RedditVideo(self, old, hot_post, subreddit_POS):
        self.old = old
        self.hot_post = hot_post
        self.subreddit_POS = subreddit_POS
        regex = self.old.split('/')[-1]
        new = download_path + regex + '.mp4'
        touple_list = [(None, None)]
        try:
            fallback_url = self.hot_post.media['reddit_video']['fallback_url']
            video_response = requests.get(fallback_url)
            touple_list = [(new, video_response)]
        except TypeError:
            cross_post = self.crosspostIDpasser(self.hot_post)
            downloadprocess(cross_post, self.subreddit_POS)
        return touple_list

    def selfpostfunc(self, hot_post):
        self.hot_post = hot_post
        hot_post_valuer = list(self.hot_post.media_metadata.values())
        hot_post_check = list(self.hot_post.media_metadata.values())[0]
        touple_list = []
        if hot_post_check['e'] == 'Image':
            for hot_post_values in hot_post_valuer:
                file_link = hot_post_values['s']['u']
                touple = self.directimage(file_link)[0]
                touple_list.append(touple)
        elif hot_post_check['e'] == 'RedditVideo':
            urldecoy = str(hot_post_check['dashUrl'])
            dashurlrequest = requests.get(urldecoy)
            parsedvalue = minidom.parseString(dashurlrequest.content)
            dash_valuestore = parsedvalue.getElementsByTagName('BaseURL')[0]
            dash_value = dash_valuestore.firstChild.nodeValue
            indexvalue = urldecoy.index('DASHPlaylist.mpd')
            file_link = urldecoy[:indexvalue] + dash_value
            media_response = requests.get(file_link)
            new = download_path + \
                list(self.hot_post.media_metadata.keys())[0]+'.mp4'
            touple_list = [(new, media_response)]
        return touple_list

    def gifvtomp4(self, old):
        self.old = old
        mp4url = self.old.replace('.gifv', '.mp4')
        touple_list = self.directimage(mp4url)
        return touple_list

    def crosspostIDpasser(self, hot_post):
        self.hot_post = hot_post
        cross_post = reddit.submission(
            self.hot_post.crosspost_parent.split('_')[1])
        return cross_post

    def giphyAPI(self, old):
        self.old = old
        gifid = self.old.split('/')[self.old.split('/').index('media') + 1]
        headers = {'api_key': f'{AuthandGVs.giphyAPIKey}'}
        response = requests.get(
            f'https://api.giphy.com/v1/gifs/{gifid}', headers=headers)
        new = download_path + gifid + '.gif'
        filelink = json.loads(response.text)[
            'data']['images']['original']['url']
        media_response = requests.get(filelink)
        return [(new, media_response)]

    def listdownloader(self, link):
        self.link = link
        try:
            if '.gifv' in self.link:
                touple_list = self.gifvtomp4(self.link)
            else:
                touple_list = self.directimage(self.link)
            downloader(touple_list)
            print('Downloaded succefully..........')
        except:
            print('Retry/Download failed...........')


def downloader(touple_list):
    for tuple_ in touple_list:
        localpath, https_respone = tuple_
        if (localpath == None):
            pass
        else:
            valid_tuple = ('.jpg', '.png', '.gif', '.jpeg', '.mp4')
            while not localpath.lower().endswith(valid_tuple):
                localpath = localpath[:-1]
            with open(localpath, 'wb') as f:
                f.write(https_respone.content)
            global NewIDcounter
            NewIDcounter += 1
            f.close()


def cleanup(path=download_path):
    counter = 0
    today = datetime.datetime.now()
    for filee in os.listdir(path):
        newfilee = path + filee
        oldstamp = datetime.datetime.fromtimestamp(os.path.getmtime(newfilee))
        difference = today - oldstamp
        if (difference.days > 2):
            os.remove(newfilee)
            print(f'Removing {difference.days}-day(s) old file...........')
            counter += 1
    print(f'{counter} old files Deleted...........')


def showlogs():
    file_stat = os.stat(log_filename)
    if file_stat.st_size == 0:
        print('No issues found in today\'s batch...')
    else:
        f = open(log_filename, 'r')
        file_contents = f.read()
        print('Find below errors in today\'s batch.....\n')
        print(file_contents)
        f.close()


class DBInnterfaces:
    def DBchecker(self, hot_post):
        if DBconn:
            self.hot_post = hot_post
            unID = f"'{str(self.hot_post)}'"
            mycurser.execute(
                f"select somtinextra from urltable WHERE name = {unID}")
            downdate = mycurser.fetchone()
            return downdate
        else:
            return None

    def DBcommitter(self, hot_post, subreddit_POS):
        if DBconn:
            self.hot_post = hot_post
            self.subreddit_POS = subreddit_POS
            unID = f"'{str(self.hot_post)}'"
            sub_value = f"'{self.subreddit_POS}'"
            time2date = str(datetime.datetime.fromtimestamp(
                self.hot_post.created))[:10]
            post_date = f"'{time2date}'"
            mycurser.execute(
                f"insert into urltable values ({unID}, {datep}, {sub_value}, {post_date})")
            mydb.commit()
            print(
                f'New Reddit ID Added to Database.......{str(self.hot_post)}\n')
        else:
            pass


def downloadprocess(hot_post, subreddit_POS):
    Interface = Interfaces()
    DBInterface = DBInnterfaces()
    if not hot_post.stickied:
        downdate = DBInterface.DBchecker(hot_post)
        if downdate:
            print(hot_post.url)
            print(f'ID already found on {downdate[0]}......\n')
        else:
            old = hot_post.url
            if 'giphy.com' in old:
                touple_list = Interface.giphyAPI(old)
                downloader(touple_list)
            elif '.gifv' in old:
                touple_list = Interface.gifvtomp4(old)
                downloader(touple_list)
            elif 'v.redd.it' in old:
                touple_list = Interface.RedditVideo(
                    old, hot_post, subreddit_POS)
                downloader(touple_list)
            elif ('.png' in old) or ('.jpg' in old) or ('.gif' in old) or ('.jpeg' in old) or ('.mp4' in old):
                touple_list = Interface.directimage(old)
                downloader(touple_list)
            elif ('gfycat.com' in old) or ('redgifs' in old):
                touple_list = Interface.gfyvid(old)
                downloader(touple_list)
            elif ('imgur.com/' in old):
                touple_list = Interface.imgur(old)
                downloader(touple_list)
            elif hot_post.is_self:
                try:
                    touple_list = Interface.selfpostfunc(hot_post)
                    downloader(touple_list)
                except:
                    logging.warning(str(hot_post)+'    ' + old +
                                    '    '+'Non-Media_Item_found')
            elif ('/r/' in old) and ('/comments/' in old):
                try:
                    cross_post = Interface.crosspostIDpasser(hot_post)
                    downloadprocess(cross_post, subreddit_POS)
                except:
                    try:
                        startindex = old.index('/comments/')+10
                        endindex = old[startindex::].find('/') + startindex
                        post_ID = old[startindex:endindex]
                        cross_post = reddit.submission(post_ID)
                        downloadprocess(cross_post, subreddit_POS)
                    except:
                        pass
            else:
                logging.warning(str(hot_post)+'    ' +
                                old+'    '+'Incompatible_Link_error')
            DBInterface.DBcommitter(hot_post, subreddit_POS)


def paramsetter(setting_type):
    setterdict0 = {'h': 'hot', 'n': 'new', 'r': 'rising'}
    setterdict1 = {'t': 'top', 'c': 'controversial'}
    range_needs = False
    range_pass = 'no_need'
    if setting_type != '' and setting_type.lower()[0] in setterdict0:
        param = setterdict0[setting_type.lower()[0]]
    elif setting_type != '' and setting_type.lower()[0] in setterdict1:
        param = setterdict1[setting_type.lower()[0]]
        range_needs = True
    else:
        param = default_setting_type
    if range_needs:
        ranger = input('select the range(day/week/month/year/all):')
        rangerdict = {'d': 'day', 'w': 'week',
                      'm': 'month', 'y': 'year', 'a': 'all'}
        if ranger != '' and ranger.lower()[0] in rangerdict:
            range_pass = rangerdict[ranger.lower()[0]]
        else:
            range_pass = default_range
    return param, range_pass


def Queueadder(downloaderQueue):
    global download_pauser
    while True:
        touple_fq = downloaderQueue.get()
        download_pauser += 1
        downloaderQueue.task_done()
        if type(touple_fq) == tuple:
            hot_post, subreddit_POS = touple_fq
            try:
                downloadprocess(hot_post, subreddit_POS)
            except Exception as e:
                logging.warning(str(hot_post)+'    ' + hot_post.url +
                                '    '+str(e).replace(' ', '_'))
        else:
            Interface = Interfaces()
            try:
                Interface.listdownloader(touple_fq)
            except Exception as e1:
                logging.warning(str(hot_post)+'    ' +
                                hot_post.url + '    '+str(e1).replace(' ', '_'))
        download_pauser -= 1


def Sheerdownloadprocess():
    if sheerlist_dict == {}:
        print('No links found in sheer Dictionary\n')
    else:
        print('sheerlist found.......................................')
        serial = 0
        sheerbuffer = {}
        for id, links in sheerlist_dict.items():
            serial += 1
            sheerbuffer[serial] = sheerlist_dict[id]
            print(f'{serial}) No.of links associated with {id} are {len(links)}')
        sheer_input = input(
            "press Y(or anything) to proceed with sheer downloads\npress N to continue without sheer downloads...:")
        if sheer_input.lower() != '' and sheer_input.lower()[0] == 'n':
            print("Cancelling sheer Downloads.......................... ")
        else:
            actual_dict = {}
            while True:
                try:
                    prompti = input(
                        "choose numbers from above list, seperate numbers by ','(No spaces):")
                    prompti_list = prompti.split(',')
                    for choice in prompti_list:
                        actual_dict[int(choice)] = sheerbuffer[int(choice)]
                except ValueError:
                    print(
                        'please enter digits seperated by commas with no spaces from the above list.!')
                    continue
                except KeyError:
                    print('Please enter the exact values from the aboove list...!')
                    continue
                else:
                    print('values accepted......!')
                    break
            for listoflinks in actual_dict.values():
                for link in listoflinks:
                    downloaderQueue.put(link)
    global download_pauser
    while download_pauser > 1:
        print(f'Waiting for {download_pauser} download(s) to complete......')
        sleep(5)


print(f'\nDefault value for setting type is {default_setting_type}')
print(f'Default value for limit is {defaultlimit}')
print('You can provide null values to continue with default values......\n')
setting_type = input(
    'select the setting type(hot/top/new/raising/controversial):')
limit_input = input('Set a limit per subreddit(An Integer value):')
if limit_input == '':
    limitbuffer = defaultlimit
else:
    limitbuffer = int(limit_input)
param, range_value = paramsetter(setting_type)
for _ in range(30):
    t = Thread(target=Queueadder, args=(downloaderQueue, ), daemon=True)
    t.start()
print('\nDownloader threads are started....\n')
for subreddit_POS in subreddits:
    print(
        f'************************************{subreddit_POS}')
    subreddit = reddit.subreddit(subreddit_POS)
    if range_value == 'no_need':
        if param == 'hot':
            hot_posts = subreddit.hot(limit=limitbuffer)
        elif param == 'new':
            hot_posts = subreddit.new(limit=limitbuffer)
        else:
            hot_posts = subreddit.rising(limit=limitbuffer)
    else:
        if param == 'top':
            hot_posts = subreddit.top(range_value, limit=limitbuffer)
        else:
            hot_posts = subreddit.controversial(
                range_value, limit=limitbuffer)
    for hot_post in hot_posts:
        downloaderQueue.put((hot_post, subreddit_POS))
print('All posts have been scanned waiting for remaining downloads...........')
while downloaderQueue.qsize() > 0:
    print(
        f'{downloaderQueue.qsize()} download(s) Queued........')
    sleep(5)
downloaderQueue.join()
while download_pauser > 0:
    print(f'Waiting for {download_pauser} download(s) to complete......')
    loopbreakerlist.append(download_pauser)
    stopcount = 0
    for i in range(0, len(loopbreakerlist)-1):
        if loopbreakerlist[i] == loopbreakerlist[i+1]:
            stopcount += 1
        else:
            stopcount = 0
    if stopcount > 13:
        download_pauser = 0
        print(
            f'Cancelling {download_pauser} download(s) due to thread error....')
    sleep(4)
try:
    mydb.close()
except:
    pass
Sheerdownloadprocess()
cleanup()
print(f"\n{NewIDcounter} new post(s) have been downloaded.........\n")
# showlogs()
