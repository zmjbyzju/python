# coding: utf-8
import time
import os
import sys
import re
import urllib
import gzip
import datetime
import urllib.request
import http.cookiejar
import lxml.html
import threading
import socket
import xlwt

MAX_CONNECTION = 100
CONNECTION_TIMEOUT = 5
CONNECTION_RETRIES = 10

COOKIE_FILE_NAME = 'cookie.txt'
RESULT_FILE_NAME = 'result.xls'

class HtmlDownloadMaster(object):
    def __init__(self):
        head = {'User-Agent':"Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0",
                'Accept':'image/png,image/*;q=0.8,*/*;q=0.5',
                'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding':'gzip, deflate',
                'Connection':'keep-alive',
                'DNT':'1',
                'Host': 'www.amazon.cn'}
        self.cookiejar = http.cookiejar.MozillaCookieJar(COOKIE_FILE_NAME)
        if os.path.exists(COOKIE_FILE_NAME):
            self.cookiejar.load(COOKIE_FILE_NAME, ignore_discard=True, ignore_expires=True)
        self.handler = urllib.request.HTTPCookieProcessor(self.cookiejar)
        self.opener = urllib.request.build_opener(self.handler)
        self.headers = [i for i  in head.items()]
        self.opener.addheaders = self.headers
        self.cookie_is_saved = False

        socket.setdefaulttimeout(CONNECTION_TIMEOUT)
        
   
    def get_remote_html(self, url):
        for i in range(CONNECTION_RETRIES):
            try:
                rsp = self.opener.open(url).read()
                break
            except Exception as e:
                pass
        else:
            raise Exception(e)
        if not self.cookie_is_saved:
            self.cookiejar.save(ignore_discard=True, ignore_expires=True)
            self.cookie_is_saved = True
        try:
            html = gzip.decompress(rsp).decode()
        except:
            html = rsp.decode()
        return html
        
        
class GetReviews(object):
    def __init__(self):
        self.downloader = HtmlDownloadMaster()
        self.review_list = []
        self.failed_download_pages = []
        self.product_name = ''
        self.result_book = xlwt.Workbook()


    def get_url_type(self, url):
        ''' return 'review' if url is for review page,
            or 'product' if url is for product page. 
        '''
        url_type = 'reviews' if 'product-reviews' in url else 'product'
        return url_type


    def get_first_review_page_url(self, url):
        ''' get reviews url from product page '''
        url_part_see_all_reviews = 'ref=cm_cr_dp_d_show_all_top?ie=UTF8&reviewerType=avp_only_reviews'
        url_part_sort_by_recent = 'ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=avp_only_reviews&formatType=all_formats&pageNumber=1&sortBy=recent'
        url_part_sort_by_helpful = url_part_sort_by_recent.replace('sortBy=recent', 'sortBy=helpful')
        url_part_sort_by_current = url_part_sort_by_recent.replace('formatType=all_formats', 'formatType=current_format')
        url_type = self.get_url_type(url)
        if url_type == 'product':
            print('getting product page ...')
            html = self.downloader.get_remote_html(url)
            tree = lxml.html.fromstring(html)
            xpath = "//a[@id='dp-summary-see-all-reviews']//@href"
            see_all_reviews_relative_url = tree.xpath(xpath)[0]
            see_all_reviews_url = "http://www.amazon.com%s"  % see_all_reviews_relative_url
            first_review_page_url = see_all_reviews_url.replace(url_part_see_all_reviews, url_part_sort_by_recent)
        else:
            first_review_page_url = url.replace(url_part_sort_by_helpful, url_part_sort_by_recent)
        return first_review_page_url


    def get_product_name_from_url(self, url):
        product_name = url.split('/')[3]
        self.product_name = product_name
        return product_name

        
    def get_all_reviews(self, url):
        print('Product: %s'  % self.get_product_name_from_url(url))
        first_review_page_url = self.get_first_review_page_url(url)
        print('getting reviews page 1 ...')
        first_review_page_html = self.downloader.get_remote_html(first_review_page_url)
        total_review_page_number = self.get_total_review_page_number_from_html(first_review_page_html)
        print('total review page is %s.'  % total_review_page_number)
        
        reviews_in_first_page = self.get_review_list_from_html(first_review_page_html)
        self.review_list.extend(reviews_in_first_page)

        if total_review_page_number == 1:
            return self.review_list
        
        lock=threading.Lock()
        download_thread_list = []
        for page in range(1, total_review_page_number + 1):
            url = re.sub('pageNumber=\d+', 'pageNumber=%s' % page, first_review_page_url)
            page_download_thread = threading.Thread(target=self.download_html_and_get_reviews, 
                                                    kwargs={'lock':lock, 'url':url})
            download_thread_list.append(page_download_thread)
        
        active_thread_list = []
        output_msg_lenth = 60
        while len(download_thread_list) > 0 or len(active_thread_list):
            active_thread_list = [i for i in active_thread_list if i.isAlive()]
            alive_thread_number = len(active_thread_list)
            free_thread_number = MAX_CONNECTION - alive_thread_number
            pending_thread_number = len(download_thread_list)
            lock.acquire()
            fail_num = len(self.failed_download_pages)
            lock.release()
            msg = 'pending %s pages, %s pages is downloading, %s pages error' % \
                  (pending_thread_number, alive_thread_number, fail_num)
            msg_lenth = len(msg)
            output_msg_lenth = max(output_msg_lenth, msg_lenth)
            new_msg = (' '*output_msg_lenth).replace(' '*msg_lenth, msg)
            sys.stdout.write('\r%s' % new_msg)
            for i in range(min(free_thread_number, pending_thread_number)):
                download_thread_list[0].start()
                active_thread_list.append(download_thread_list[0])
                download_thread_list.pop(0)
            time.sleep(0.5)
        print('\ndownload finished.')
        return self.review_list
    

    def download_html_and_get_reviews(self, lock, url):
        page_num = int(re.search('pageNumber=(\d+)', url).group(1))
        try:
            html = self.downloader.get_remote_html(url)
        except Exception as e:
            lock.acquire()
            self.failed_download_pages.append(page_num)
            lock.release()
            raise Exception(e)
        reviews_in_html = self.get_review_list_from_html(html)
        lock.acquire()
        self.review_list.extend(reviews_in_html)
        lock.release()
               

    def show_reviews_statistics(self):
        reviews_classified_by_month = self.classify_reviews_by_month(self.review_list)
        total_review_num = 0
        star_sum = 0
        print('%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s'  % \
             ('month', 'total', 'One', 'Two', 'Three', 'Four', 'Five', 'Avg', 'Star'))
        for month in sorted(reviews_classified_by_month.keys()):
            month_str = '%s-%s'  % (month.year, month.month)
            reviews = reviews_classified_by_month[month]
            reviews_classified_by_star = self.classify_reviews_by_star(reviews)
            one_star_num = len(reviews_classified_by_star.get(1, []))
            two_star_num = len(reviews_classified_by_star.get(2, []))
            three_star_num = len(reviews_classified_by_star.get(3, []))
            four_star_num = len(reviews_classified_by_star.get(4, []))
            five_star_num = len(reviews_classified_by_star.get(5, []))
            month_total_review_num = len(reviews)
            month_star_sum = one_star_num + two_star_num*2 + three_star_num*3 + four_star_num*4 + five_star_num*5
            if month_total_review_num == 0:
                month_avg_star = 0
            else:
                month_avg_star = round(month_star_sum/month_total_review_num, 2)
            star_sum = star_sum + month_star_sum
            total_review_num = total_review_num + month_total_review_num
            if total_review_num == 0:
                real_review = 0
            else:
                real_review = round(star_sum / total_review_num, 1)
            print('%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s\t%-4s'  % \
                 (month_str, month_total_review_num, one_star_num, two_star_num, three_star_num, 
                 four_star_num, five_star_num, month_avg_star, real_review))
        
        if self.failed_download_pages:   
            print('failed download pages:\n%s' % self.failed_download_pages)
                 

    def get_total_review_page_number_from_html(self, html):
        tree = lxml.html.fromstring(html)
        page_butten_list = tree.xpath("//*[@id='cm_cr-pagination_bar']/ul/li//text()")
        if len(page_butten_list) == 0:
            return 1
        page_butten_list = [i.replace(',', '') for i in page_butten_list]
        page_butten_list = [i for i in page_butten_list if i.strip().isdigit()]
        page_butten_list = [int(i) for i in page_butten_list]
        total_review_page_number = max(page_butten_list)
        return total_review_page_number

        
    def get_review_list_from_html(self, html):
        item_xpath = "//div[starts-with(@id, 'customer_review-')]"
        star_xpath = ".//a[@class='a-link-normal' and starts-with(@href, '/gp/customer-reviews/')]//@title"
        title_xpath = ".//a[contains(@class, 'review-title')]//text()"
        author_xpath = ".//a[contains(@class, 'author')]//text()"
        date_xpath = ".//span[contains(@class, 'review-date')]//text()"
        text_xpath = ".//span[contains(@class, 'review-text')]//text()"
        vote_xpath = ".//span[contains(@class, 'review-vote')]//text()"
        
        tree = lxml.html.fromstring(html)
        item_entries = tree.xpath(item_xpath)
        review_list = []
        for entry in item_entries:
            customer = entry.get('id')
            star_str = entry.xpath(star_xpath)[0]
            star = int(re.search('\d+', star_str).group())
            title = entry.xpath(title_xpath)[0]
            author = entry.xpath(author_xpath)[0]
            date_str = entry.xpath(date_xpath)[0].replace('on ', '')
            date = self.transform_review_date_format(date_str)
            text = entry.xpath(text_xpath)[0]
            vote_list = entry.xpath(vote_xpath)
            if len(vote_list):
                vote = int(re.search('\d+|One', vote_list[0]).group().replace('One', '1'))
            else:
                vote = 0
            elem = {'customer':customer,'star':star, 'title':title, 'author':author, 
                    'date':date, 'text':text, 'vote':vote}
            review_list.append(elem)
        return review_list
        
        
    def transform_review_date_format(self, date):
        ''' transform ''March 25, 2017' to datetime.datetime(2017, 3, 25) '''
        date = date.replace('January', '1')
        date = date.replace('February', '2')
        date = date.replace('March', '3')
        date = date.replace('April', '4')
        date = date.replace('May', '5')
        date = date.replace('June', '6')
        date = date.replace('July', '7')
        date = date.replace('August', '8')
        date = date.replace('September', '9')
        date = date.replace('October', '10')
        date = date.replace('November', '11')
        date = date.replace('December', '12')
        date = date.replace(',', '')
        
        elem = re.split(' +', date)
        elem = [int(i) for i in elem]
        return datetime.datetime(elem[2], elem[0], elem[1])
        
        
    def classify_reviews_by_month(self, review_list):
        ''' sort reviews by month 
        Argument:
            review_list(list): review_list
        Returns(dict):
            {2017.3:[a list of reviews during 2017.3], ...}
        '''
        reviews_classified_by_month = {}
        for review in review_list:
            date = review['date']
            # month = date.year + date.month * (0.1**len(str(date.month)))    # 2017.3, 2016.12
            month = datetime.datetime(date.year, date.month, 1)
            if month not in reviews_classified_by_month:
                reviews_classified_by_month[month] = []
            reviews_classified_by_month[month].append(review)
        return reviews_classified_by_month
            
            
    def classify_reviews_by_star(self, review_list):
        ''' sort reviews by star 
        Argument:
            review_list(list): review_list
        Returns(dict):
            {1:[1_star_review_list], ...}
        '''
        reviews_classified_by_star = {}
        for review in review_list:
            star = review['star']
            if star not in reviews_classified_by_star:
                reviews_classified_by_star[star] = []
            reviews_classified_by_star[star].append(review)
        return reviews_classified_by_star


    def sort_review_list_by_date(self, review_list):
        list_sorted = []
        date_list = list(set([i['date'] for i in review_list]))
        date_list.sort()
        date_list.reverse()
        for date in date_list:
            review_list_with_current_date = [i for i in review_list if i['date'] == date] 
            list_sorted.extend(review_list_with_current_date)
        return list_sorted


    def sort_review_list_by_vote(self, review_list):
        list_sorted = []
        vote_list = list(set([i['vote'] for i in review_list]))
        vote_list.sort()
        vote_list.reverse()
        for vote in vote_list:
            review_list_with_current_vote = [i for i in review_list if i['vote'] == vote] 
            list_sorted.extend(review_list_with_current_vote)
        return list_sorted


    def save_review_statistics(self):
        reviews_classified_by_month = self.classify_reviews_by_month(self.review_list)
        total_review_num = 0
        star_sum = 0
        sheet_statistics = self.result_book.add_sheet('Statistics')
        row = column = 0
        # sheet_statistics.write(row, column, self.product_name)
        # row = row + 1
        # column = 0
        for i in ('month', 'total num', 'One', 'Two', 'Three', 'Four', 'Five', 'month avg star', 'total avg star'):
            sheet_statistics.write(row, column, i)
            column = column + 1
        row = row + 1
        column = 0
        for month in sorted(reviews_classified_by_month.keys()):
            month_str = '%s-%s'  % (month.year, month.month)
            reviews = reviews_classified_by_month[month]
            reviews_classified_by_star = self.classify_reviews_by_star(reviews)
            one_star_num = len(reviews_classified_by_star.get(1, []))
            two_star_num = len(reviews_classified_by_star.get(2, []))
            three_star_num = len(reviews_classified_by_star.get(3, []))
            four_star_num = len(reviews_classified_by_star.get(4, []))
            five_star_num = len(reviews_classified_by_star.get(5, []))
            month_total_review_num = len(reviews)
            month_star_sum = one_star_num + two_star_num*2 + three_star_num*3 + four_star_num*4 + five_star_num*5
            if month_total_review_num == 0:
                month_avg_star = 0
            else:
                month_avg_star = round(month_star_sum/month_total_review_num, 2)
            star_sum = star_sum + month_star_sum
            total_review_num = total_review_num + month_total_review_num
            if total_review_num == 0:
                real_review = 0
            else:
                real_review = round(star_sum / total_review_num, 1)
            for i in (month_str, month_total_review_num, one_star_num, two_star_num, three_star_num, \
                      four_star_num, five_star_num, month_avg_star, real_review):
                sheet_statistics.write(row, column, i)
                column = column + 1
            row = row + 1
            column = 0
        
        if self.failed_download_pages:   
            sheet_statistics.write(row, column, 'failed download pages')
            column = column + 1
            sheet_statistics.write(row, column, self.failed_download_pages)
            ow = row + 1
            column = 0


    def save_all_reviews_text(self):
        ''' save all the review items into a sheet '''
        sheet_all_reviews = self.result_book.add_sheet('All Reviews')
        sheet_header = ('date', 'author', 'star', 'vote', 'title', 'text')
        row = column = 0
        for i in sheet_header:
            sheet_all_reviews.write(row, column, i)
            column = column + 1
        row = row + 1
        column = 0
        review_list = self.sort_review_list_by_date(self.review_list)
        for review_item in review_list:
            date = review_item['date']
            date_str = '%s-%s-%s'  % (date.year, date.month, date.day)
            sheet_all_reviews.write(row, column, date_str)
            column = column + 1
            sheet_all_reviews.write(row, column, review_item['author'])
            column = column + 1
            sheet_all_reviews.write(row, column, review_item['star'])
            column = column + 1
            sheet_all_reviews.write(row, column, review_item['vote'])
            column = column + 1
            sheet_all_reviews.write(row, column, review_item['title'])
            column = column + 1
            sheet_all_reviews.write(row, column, review_item['text'])

            row = row + 1
            column = 0


    def save_reviews_by_star(self):
        ''' save reviews with diffent star into differnt sheet '''
        reviews_classified_by_star = self.classify_reviews_by_star(self.review_list)
        sheet_header = ('date', 'author', 'star', 'vote', 'title', 'text')
        for star, reviews in reviews_classified_by_star.items():
            sheet_by_star = self.result_book.add_sheet('%s Star' % star)
            row = column = 0
            for i in sheet_header:
                sheet_by_star.write(row, column, i)
                column = column + 1
            row = row + 1
            column = 0
            reviews = self.sort_review_list_by_date(reviews)
            for review_item in reviews:
                date = review_item['date']
                date_str = '%s-%s-%s'  % (date.year, date.month, date.day)
                sheet_by_star.write(row, column, date_str)
                column = column + 1
                sheet_by_star.write(row, column, review_item['author'])
                column = column + 1
                sheet_by_star.write(row, column, review_item['star'])
                column = column + 1
                sheet_by_star.write(row, column, review_item['vote'])
                column = column + 1
                sheet_by_star.write(row, column, review_item['title'])
                column = column + 1
                sheet_by_star.write(row, column, review_item['text'])

                row = row + 1
                column = 0


    def save_most_helpful_reviews(self):
        review_list = self.sort_review_list_by_vote(self.review_list)
        sheet_by_vote = self.result_book.add_sheet('Most helpful')
        sheet_header = ('date', 'author', 'star', 'vote', 'title', 'text')
        row = column = 0
        for i in sheet_header:
            sheet_by_vote.write(row, column, i)
            column = column + 1
        row = row + 1
        column = 0
        for review_item in review_list:
            vote = review_item['vote']
            if vote < 5:
                break
            date = review_item['date']
            date_str = '%s-%s-%s'  % (date.year, date.month, date.day)
            sheet_by_vote.write(row, column, date_str)
            column = column + 1
            sheet_by_vote.write(row, column, review_item['author'])
            column = column + 1
            sheet_by_vote.write(row, column, review_item['star'])
            column = column + 1
            sheet_by_vote.write(row, column, review_item['vote'])
            column = column + 1
            sheet_by_vote.write(row, column, review_item['title'])
            column = column + 1
            sheet_by_vote.write(row, column, review_item['text'])

            row = row + 1
            column = 0


    def save_reviews_to_excel(self, dut_name=None):
        time_stap = time.strftime('%Y%m%d', time.localtime())
        if not dut_name:
            save_name = 'Reviews_%s_%s.xls'  % (self.product_name, time_stap)
        else:
            save_name = 'Reviews_%s_%s.xls'  % (dut_name, time_stap)
        self.save_review_statistics()
        self.save_all_reviews_text()
        self.save_reviews_by_star()
        self.save_most_helpful_reviews()
        self.result_book.save(save_name)


def get_reviews_for_cable_modem():
    cr700_url = 'https://www.amazon.com/TP-Link-Certified-Communications-Archer-CR700/dp/B012I96J3W/ref=sr_1_1?ie=UTF8&qid=1490972370&sr=8-1&keywords=cr700'
    tc7610_7620_url = 'https://www.amazon.com/TP-Link-343Mbps-Certified-Spectrum-TC-7610-E/dp/B01CH8ZNJ0/ref=sr_1_1?ie=UTF8&qid=1491060700&sr=8-1&keywords=tc-7610'
    tcw7960_url = 'https://www.amazon.com/TP-Link-Cable-Modem-Router-Communications/dp/B01EO5A3RQ/ref=sr_1_1?ie=UTF8&qid=1491061952&sr=8-1&keywords=tc-w7960'
    tc7620_only_url = 'https://www.amazon.com/TP-Link-Download-Certified-Communications-TC-7620/product-reviews/B01CVOLKKQ/ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=avp_only_reviews&formatType=current_format&pageNumber=1&sortBy=helpful'
    tc7610_only_url = 'https://www.amazon.com/TP-Link-343Mbps-Certified-Spectrum-TC-7610/product-reviews/B01CH8ZNJ0/ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=all_reviews&pageNumber=1&formatType=current_format'
    
    for url, dut_name in [(cr700_url, 'CR700'), (tc7610_7620_url, 'TC-7610_7620'), (tcw7960_url, 'TC-W7960'), 
                          (tc7620_only_url, 'TC-7620'), (tc7610_only_url, 'TC-7610')]:
        print('Start at %s'  % datetime.datetime.now())
        master = GetReviews()
        review_list = master.get_all_reviews(url)
        master.show_reviews_statistics()
        master.save_reviews_to_excel(dut_name=dut_name)
        print('Finish at %s\n'  % datetime.datetime.now())
        input()


if __name__=="__main__":
    # get_reviews_for_cable_modem()

    dut_name = input('dut name: ')
    url = input('product url or reviews url: ')
    print('Start at %s'  % datetime.datetime.now())
    master = GetReviews()
    review_list = master.get_all_reviews(url)
    master.show_reviews_statistics()
    master.save_reviews_to_excel(dut_name)
    print('Finish at %s\n'  % datetime.datetime.now())
    input()

    
        
