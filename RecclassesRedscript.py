import praw
import requests
import json
import os
import datetime
import mysql.connector
import logging
from xml.dom import minidom
import AuthandGVs

reddit = praw.Reddit(client_id=AuthandGVs.Reddit_client_id,
                     client_secret=AuthandGVs.Reddit_client_secret,
                     username=AuthandGVs.Reddit_username,
                     password=AuthandGVs.Reddit_password,
                     user_agent=AuthandGVs.Reddit_user_agent)

subreddits = AuthandGVs.subreddit_list
download_path = AuthandGVs.download_path
mydb = mysql.connector.connect(host="localhost", user=AuthandGVs.mysql_user,
                               passwd=AuthandGVs.mysql_password, database=AuthandGVs.mysql_database)
mycurser = mydb.cursor(buffered=True)
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
sheerlist_dict = {}


class Interfaces:
    def directimage(self, old):
        self.old = old
        for media in ['.png', '.jpg', '.jpeg', '.gif', '.mp4']:
            if media in self.old:
                subst = media
                break
        end_index = self.old.index(subst) + len(subst)
        regex = self.old[len(self.old)-self.old[::-1].index('/'):end_index:]
        new = download_path + regex
        image_response = requests.get(self.old)
        return [(new, image_response)]

    def gfyvid(self, old):
        self.old = old
        try:
            gfyID = self.old[len(self.old)-self.old[::-1].index('/')::]
            gfyAPI_res = requests.get(
                f'https://api.gfycat.com/v1/gfycats/{gfyID}')
            gfyJson_res = json.loads(gfyAPI_res.text)
            gfyvid_url = gfyJson_res['gfyItem']['mp4Url']
            new = download_path + gfyID + '.mp4'
            image_response = requests.get(gfyvid_url)
            return [(new, image_response)]
        except Exception as gfyAPIError:
            if str(gfyAPIError) == "'gfyItem'":
                try:
                    print(
                        'Attempting to scrape the link off of HTML response..........')
                    html_respone = requests.get(self.old).text
                    start_index = html_respone.find(
                        'og:video:secure_url\" content=') + 30
                    splitlist = html_respone[start_index::].split('\"')
                    file_link = splitlist[0]
                    print(file_link)
                    new = download_path + \
                        file_link[len(file_link)-file_link[::-1].index('/')::]
                    image_response = requests.get(file_link)
                    return [(new, image_response)]
                except Exception as scrap_error:
                    print(scrap_error)
                    logging.warning(str(hot_post)+'    ' + old +
                                    '    '+str(scrap_error).replace(' ', '_'))

    def imgur(self, old):
        self.old = old
        imgurID = self.old[len(self.old)-self.old[::-1].index('/')::]
        header = {'Authorization': f'Client-ID {AuthandGVs.Imgur_ClientID}'}
        file_link_list = []
        touple_list = [('', '')]
        if ('imgur.com/a/') not in self.old:
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
            localpath_list, mediares_list = [], []
            for file_link in file_link_list:
                new = download_path + \
                    file_link[len(file_link)-file_link[::-1].index('/')::]
                localpath_list.append(new)
                media_response = requests.get(file_link)
                mediares_list.append(media_response)
            touple_list = list(zip(localpath_list, mediares_list))
        else:
            sheerlist_dict[imgurID] = file_link_list
        return touple_list

    def RedditVideo(self, old, fallback_url):
        self.old = old
        self.fallback_url = fallback_url
        regex = self.old[len(self.old)-self.old[::-1].index('/')::]
        new = download_path + regex + '.mp4'
        video_response = requests.get(self.fallback_url)
        return [(new, video_response)]

    def selfpostfunc(self, hot_post):
        self.hot_post = hot_post
        hot_post_valuer = list(self.hot_post.media_metadata.values())
        hot_post_check = list(self.hot_post.media_metadata.values())[0]
        path_list = []
        response_list = []
        print('self post found...Extracting data from media metadata....')
        if hot_post_check['e'] == 'Image':
            for hot_post_values in hot_post_valuer:
                file_link = hot_post_values['s']['u']
                print(file_link)
                media_response = requests.get(file_link)
                response_list.append(media_response)
                if ('.png' in file_link):
                    subst = '.png'
                elif ('.jpg' in file_link):
                    subst = '.jpg'
                end_index = file_link.index(subst) + 4
                regex = file_link[len(file_link) -
                                  file_link[::-1].index('/'):end_index:]
                new = download_path + regex
                path_list.append(new)
            touple_list = list(zip(path_list, response_list))
        elif hot_post_check['e'] == 'RedditVideo':
            urldecoy = str(hot_post_check['dashUrl'])
            dashurlrequest = requests.get(urldecoy)
            parsedvalue = minidom.parseString(dashurlrequest.content)
            dash_valuestore = parsedvalue.getElementsByTagName('BaseURL')[0]
            dash_value = dash_valuestore.firstChild.nodeValue
            indexvalue = urldecoy.index('DASHPlaylist.mpd')
            file_link = urldecoy[:indexvalue] + dash_value
            print(file_link)
            media_response = requests.get(file_link)
            new = download_path + \
                list(self.hot_post.media_metadata.keys())[0]+'.mp4'
            touple_list = [(new, media_response)]
        return touple_list

    def gifvtomp4(self, old):
        self.old = old
        mp4url = self.old.replace('.gifv', '.mp4')
        video_response = requests.get(mp4url)
        regex = mp4url[len(mp4url)-mp4url[::-1].index('/')::]
        new = download_path + regex
        return [(new, video_response)]


def downloader(touple_list):
    for tuple_ in touple_list:
        localpath, https_respone = tuple_
        if (localpath == ''):
            print(
                "Download averted due to passing of null values to Downloader function...........")
        else:
            while localpath[-1] not in ['g', 'f', '4']:
                localpath = localpath[:-1]
            with open(localpath, 'wb') as f:
                f.write(https_respone.content)
            global NewIDcounter
            NewIDcounter += 1


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
        self.hot_post = hot_post
        unID = f"'{str(self.hot_post)}'"
        mycurser.execute(
            f"select somtinextra from urltable WHERE name = {unID}")
        downdate = mycurser.fetchone()
        return downdate

    def DBcommitter(self, hot_post, subreddit_POS):
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
        print(f'New Reddit ID Added to Database.......{str(self.hot_post)}\n')


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
            print(hot_post.title + '.'*3)
            print(old)
            if '.gifv' in old:
                touple_list = Interface.gifvtomp4(old)
                downloader(touple_list)
            elif 'v.redd.it' in old:
                fallback_url = hot_post.media['reddit_video']['fallback_url']
                touple_list = Interface.RedditVideo(
                    old, fallback_url)
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
                    print('Non-media Item found.......................')

            elif ('/r/' in old) and ('/comments/' in old):
                try:
                    cross_post = reddit.submission(
                        hot_post.crosspost_parent.split('_')[1])
                    print('Cross post found...................')
                except:
                    print("Not a cross-post...Using ID from URL.......")
                    startindex = old.index('/comments/')+10
                    endindex = old[startindex::].find('/') + startindex
                    post_ID = old[startindex:endindex]
                    print(f'the {post_ID} is found....................')
                    cross_post = reddit.submission(post_ID)
                downloadprocess(cross_post, subreddit_POS)
            else:
                print('Incompatible or unwanted link found............')
                logging.warning(str(hot_post)+'    ' +
                                old+'    '+'Incompatible_Link_error')
            DBInterface.DBcommitter(hot_post, subreddit_POS)
    else:
        print('Mod post found........')


def paramsetter(setting_type):
    setterdict0 = {'h': 'hot', 'n': 'new', 'r': 'rising'}
    setterdict1 = {'t': 'top', 'c': 'controversial'}
    range_needs = False
    range_pass = 'no_need'
    if setting_type == '':
        param = default_setting_type
    elif setting_type.lower()[0] in setterdict0:
        param = setterdict0[setting_type.lower()[0]]
    elif setting_type.lower()[0] in setterdict1:
        param = setterdict1[setting_type.lower()[0]]
        range_needs = True
    else:
        print('invalid value error...Proceeding with default parametre')
        param = default_setting_type
    if range_needs:
        ranger = input('select the range(day/week/month/year/all):')
        rangerdict = {'d': 'day', 'w': 'week',
                      'm': 'month', 'y': 'year', 'a': 'all'}
        if ranger == '':
            print('proceeding with default value')
            range_pass = default_range
        elif ranger.lower()[0] in rangerdict:
            range_pass = rangerdict[ranger.lower()[0]]
        else:
            print('invalid value error...Proceeding with default parametre')
            range_pass = default_range
    return param, range_pass


def Sheerdownloadprocess():
    for id, links in sheerlist_dict.items():
        print(f'No. of links associated with {id} are {len(links)}')
    if sheerlist_dict == {}:
        print('No links found in sheer Dictionary\n')
    else:
        sheer_input = input(
            "press Y(or anything) to proceed with sheer downloads\npress N to continue without sheer downloads...:")
        if sheer_input.lower()[0] == 'n':
            print("Cancelling sheer Downloads.......................... ")
        else:
            Interface = Interfaces()
            for listoflinks in sheerlist_dict.values():
                for link in listoflinks:
                    if '.gifv' in link:
                        touple_list = Interface.gifvtomp4(link)
                    else:
                        touple_list = Interface.directimage(link)
                    downloader(touple_list)


print(f'\nDefault value for setting type is {default_setting_type}')
print(f'Default value for limit is {defaultlimit}')
print('provide null values to continue with default values......\n')
setting_type = input(
    'select the setting type(hot/top/new/raising/controversial):')
limit_input = input('Set a limit per subreddit(An Integer value):')
if limit_input == '':
    limitbuffer = defaultlimit
else:
    limitbuffer = int(limit_input)
param, range_value = paramsetter(setting_type)
for subreddit_POS in subreddits:
    print(
        f'******************************************{subreddit_POS}******************************************')
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
        try:
            downloadprocess(hot_post, subreddit_POS)
        except Exception as e:
            logging.warning(str(hot_post)+'    ' + hot_post.url +
                            '    '+str(e).replace(' ', '_'))
mydb.close()
Sheerdownloadprocess()
cleanup()
print(f"\n{NewIDcounter} new post(s) have been downloaded.........\n")
showlogs()
