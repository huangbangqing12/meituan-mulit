import csv
import random
from math import ceil
from pyquery import PyQuery as pq
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import requests
from settings import *
from multiprocessing import Pool,Process
import pymongo
import time
import json
import re
from network import *
import os


data_list=[]
range_name=''

client = pymongo.MongoClient(MONOG_URL)
db = client[MONOG_DB]



#页面抓取
def get_page(url,n=3):
    # user_agent = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36']
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        # 'Host':'zz.meituan.com',
        'Cookie': '_lxsdk_cuid='+str(random.randint(0000000,9999999))+'a094c4-0dc64a635-142c'+str(random.randint(0000,9999))+'-186a00-1643247a09645; __mta=8951'+str(random.randint(0000,9999)) +
                  '.1529825596093.1529825596093.152985' + str(random.randint(0000000,9999999))+'.2; _lxsdk_s=1643247a0a0-b52-f70-271%7C%7C2; domain=com; uuid=551e28ac6c' +
                  str(random.randint(00000000,999990999)) + 'a.1529825589.1.0.0; __mta=89512612.1529825596093.1529825596093.1529825596093.1; ci=73; rvct=73',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
    }
    # print(url)
    error_count = {'error_count': '1'}
    try:
        response = requests.get(url,allow_redirects=False,timeout=5,headers=headers)
        print(response.status_code,url)
        if response.status_code == 200:
            print('正在下载页面：'+url)
            response.encoding='utf-8'#乱码解决方法
            return response.text, url

        elif response.status_code == 301 or response.status_code == 302:
            # response = requests.get(url,timeout=5,headers=headers)
            # print(type(response.headers['Location']))
            if '/error/403' not in response.headers['Location']:
                print('店铺已关闭，无需处理')
                db.detail_index.update({'detail_url': url}, {'$set': {'status': '10'}})
                return None
            else:
                save_to_mongodb('error_data', error_count)
                get_page(url)

    except RequestException:
        #---------增加尝试次数的功能-------------
        #---------美团直接封IP-----直接DENY
        # print(RequestException)
        if n>0:
            n-=1
            print('尝试第'+str(3-n)+'重新抓取',url)
            time.sleep(3)
            save_to_mongodb('error_data',error_count)
            time.sleep(3)
            get_page(url,n)


#初始化一级分类中含有二级分类地址
def get_ranges_url(url,City_name):
    try:
        doc = BeautifulSoup(get_page(url)[0],'lxml')
        range_page = doc.select('#react > div > div > div.center-content.clearfix > div.left-content > div.filter-box > div.filter-section-wrapper > div:nth-of-type(1) > div.tags > div > div > a')
        # react > div > div > div.center-content.clearfix > div.left-content > div.filter-box > div.filter-section-wrapper > div:nth-child(1) > div.label
        # print(range_page)
        for i in range_page:
            range_url ='http:'+i.get('href')
            # print(range_url)
            if db.detail_url_log.find({'url': range_url}).count() == 0:
                doc = pq(get_page(range_url)[0]) #获取二级分类第一页
                # print(doc)
                try:
                    total_page = int(doc('#react > div > div > div.center-content.clearfix > div.left-content > nav > ul > li:nth-last-child(2)').text())
                except ValueError:
                    total_page = int(doc('#react > div > div > div.center-content.clearfix > div.left-content > nav > ul > li:nth-last-child(1)').text())
                    # react > div > div > div.center-content.clearfix > div.left-content > div.common-list

                range_data = {
                    'range_url':range_url,
                    'City_name':City_name,
                    'range_name':i.get_text(),#获取分类名称
                    'total_page':total_page,
                    'status':'0' #status：0 未处理，status：1 已处理
                }
                print('分类名称：'+i.get_text()+'    分类地址：'+range_url+'    分页数量：'+str(total_page))
                save_to_mongodb('range_index',range_data)
                save_to_mongodb('detail_url_log', {'url': range_url})
            else:
                print('已存在此链接，丢弃', range_url)
    except TypeError:
        print('解析异常,TypeError',url)
    except ValueError:
        print('解析异常，ValueError',url)



#获得所有二级分类的详情页链接
def get_index_page_url():
    for data in db.range_index.find({'status':'0'}): #取数据库二级分类的地址
        for i in range(1, int(data['total_page']) + 1):#根据二级分类的页面数量进行循环
            index_url = data['range_url']+'pn'+str(i) + '/'
            print(index_url)
            try:
                doc = BeautifulSoup(get_page(index_url)[0], 'lxml')
            except TypeError:
                time.sleep(15)
                doc = BeautifulSoup(get_page(index_url)[0], 'lxml')

            index_page = doc.select('.common-list-main .abstract-item')
            for second_page in index_page:#获取详情页链接
                # print(second_page)
                detail_url='http:' + second_page.select_one('a').get('href')
                if db.detail_url_log.find({'url': detail_url}).count() == 0:
                    detail_data = {
                        'range_url': data['range_url'],
                        'detail_url':detail_url,
                        'City_name':data['City_name'],
                        'range_name':data['range_name'],
                        'status': '0'  # status：0 未处理，status：1 已处理
                    }
                    print('详情页地址：',detail_url)
                    save_to_mongodb('detail_index', detail_data)
                    save_to_mongodb('detail_url_log',{'url':detail_url})
                else:
                    print('已存在此链接，丢弃',detail_url)
        db.range_index.update({'range_url':data['range_url']},{'$set':{'status':'1'}})

# #下载所有详情页页面
# def download_detail_html():
#     for data in db.detail_index.find({'status':'0'}):
#         print('正在下载页面:',data['detail_url'])
#         html = get_page(data['detail_url'])
#         index_data = {
#             'range_url': data['range_url'],
#             'detail_url':data['detail_url'],
#             'City_name':data['City_name'],
#             'range_name':data['range_name'],
#             'html':html,
#             'status':'0'
#         }
#         save_to_mongodb('detail_html',index_data)
#         db.detail_index.update({'detail_url':data['detail_url']},{'$set':{'status':'1'}})


def parse_detail_page(html):
    # print(html[0])
    if html:
        for i in db.detail_index.find({'status':'0','detail_url':html[1]}):
        # for i in db.detail_index.find({'detail_url': html[1]}):
            doc = BeautifulSoup(html[0],'lxml')
            print('正在解析：',html[1])
            # print(doc)
            try:
                #解析常规店铺信息
                if doc.select_one('.seller-info-head'):
                    # print('1')
                    re_shop_name = re.compile('<h1 class=.*?>(.*?)<', re.S)
                    re_shop_addr = re.compile('地址：.*?<span>(.*?)<', re.S)
                    re_shop_tel = re.compile('电话：.*?<span>(.*?)<', re.S)
                    if re.findall(re_shop_name,html[0]):
                        shop_name = re.findall(re_shop_name,html[0])[0]
                    else:
                        shop_name = ''
                    if re.findall(re_shop_addr,html[0]):
                        shop_addr = re.findall(re_shop_addr,html[0])[0]
                    else:
                        shop_addr = ''
                    if re.findall(re_shop_tel,html[0]):
                        shop_tel = re.findall(re_shop_tel,html[0])[0]
                    else:
                        shop_tel = ''

                    shop_data ={
                        'range_url':i['range_url'],
                        'detail_url':i['detail_url'],
                        'City_name':i['City_name'],
                        'range_name':i['range_name'],
                        'shop_name':shop_name,
                        'shop_addr':shop_addr,
                        'shop_tel' :shop_tel
                        }
                    # print(shop_data)
                else:
                    #解析结婚模块信息
                    # print('2')
                    shop_data ={
                        'range_url':i['range_url'],
                        'detail_url':i['detail_url'],
                        'City_name':i['City_name'],
                        'range_name':i['range_name'],
                        'shop_name':doc.select_one('.shop-title').get_text().strip(),
                        'shop_addr':doc.select_one('.shop-addr .fl').get_text().strip(),
                        'shop_tel' :doc.select_one('.shop-contact').get_text().strip()
                        }
                    # print(shop_data)
                save_to_mongodb('data',shop_data)
                db.detail_index.update({'detail_url':i['detail_url']},{'$set':{'status':'1'}})
            except AttributeError:
                db.detail_index.update({'detail_url': i['detail_url']}, {'$set': {'status': 'AttributeError'}})
                print('解析出错 ',i['detail_url'])
            except TypeError:
                db.detail_index.update({'detail_url': i['detail_url']}, {'$set': {'status': 'TypeError'}})
                print('解析出错 ', i['detail_url'])
    else:
        print('详细页解析错误')


def save_to_mongodb(table_name,result):
    try:
        if db[table_name].insert(result):
            pass
    except Exception:
        print(result+' 存储失败')

def save_to_csv():
    # for City_name in Citys.values():

    for City_name in db.data.aggregate([{'$group':{'_id':'$City_name'}}]):
        if SPIDER_TYPE == 1:
            City_name=City_name['_id']

        with open((City_name + time.strftime("%Y%m%d%I%M")+'.csv'),'a',newline='',encoding='utf-8-sig') as f:
            try:
                w = csv.writer(f)
                w.writerow(['序号','二级链接','店铺链接','城市名称','服务分类','店铺名称','店铺地址','店铺电话'])
                i = 1
                for city_data in db.data.find({'City_name':City_name}):
                    list = []
                    for v in city_data.values():
                        list.append(v)
                    a=[str(i)]+list[1:]
                    w.writerow(a)
                    i+=1
                print(City_name + '   写入文件完成》》》》》')
            finally:
                f.close()


def parse_province_city(city_pages):
    city_page = list(json.loads(city_pages[0]))
    # print(len(city_page))
    for p_code in range(0,(len(city_page))):
        if city_page[p_code]['provinceCode'] == Province_pid:
            for c_code in city_page[p_code]['cityInfoList']:
                if db.city.find({'city_name':c_code['name']}) is False:
                    city_data= {
                        'city_code':c_code['acronym'],
                        'city_name':c_code['name'],
                        'status':'0'
                    }
                    save_to_mongodb('city',city_data)
                # city_list[c_code['acronym']]=c_code['name']




if __name__=='__main__':
    # 监测是否要重新拨号的进程
    re_dail=Process(target=re_connect)
    re_dail.start()
    print('——————开始抓取——————')
    time.sleep(3)

    if SPIDER_TYPE == 2:
        for City_code, City_name in Citys.items():
            for class_name in Class_names:
                # 初始化抓取链接：城市与一级分类
                url = 'http://' + City_code + '.meituan.com/' + class_name + '/'
                print('一级地址：' + url)
                get_ranges_url(url, City_name)
                get_index_page_url()

    elif SPIDER_TYPE == 1:
        url = 'http://www.meituan.com/ptapi/getprovincecityinfo/'

        try:
            parse_province_city(get_page(url))
        except TypeError:
            time.sleep(5)
            parse_province_city(get_page(url))

        number = ceil(db.city.count({'status':'0'})/2)
        print('一共需要抓取 ' + str(number) +' 次，请耐心等待')
        for i in range(number):
            #每次取2个城市，防止数据库连接超时
            for code in db.city.find({'status':'0'}).batch_size(2):
                for class_name in Class_names:
                    # 初始化抓取链接：城市与一级分类
                    url = 'http://' + code['city_code'] + '.meituan.com/' + class_name + '/'
                    print('一级地址：' + url)
                    get_ranges_url(url, code['city_name'])
                    get_index_page_url()
                    print(code['city_name'] +' 的 ' + class_name + ' 已完成 ')
                    db.city.update({'city_code':code['city_code']},{'$set':{'status':'1'}})
            print('城市列表已完成 '+ str(i) + ' 次')

    print('开始多进程任务')
    p = Pool(7)
    data_info = db.detail_index.find({'status': '0'},no_cursor_timeout=True)
    for data in data_info:
        p.apply_async(get_page, args=(data['detail_url'],), callback=parse_detail_page)
    data_info.close()
    p.close()
    p.join()
    save_to_csv()

    # url = 'http://' + 'zz' + '.meituan.com/' + 'jiehun' + '/'
    # url = 'http://minquan.meituan.com/xiuxianyule/c20770/'
    # get_ranges_url(url, 'minquan')
    # get_index_page_url()
    # parse_detail_page(get_page(url))
