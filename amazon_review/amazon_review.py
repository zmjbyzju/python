# =======================================================================
# coding: utf-8
# Get product's amazon reviews for automatically, for study purpose only.
# Only for Amazon website for US.
# Updates will be release here:
#   https://github.com/wtangxyz/python/tree/master/amazon_review
# 
# This is a Python3 script, will not work with Python2.
# =======================================================================
import time
import os
import sys
import re
import gzip
import datetime
import urllib.request
import http.cookiejar
import lxml.html
import threading
import socket
import xlwt

MAX_CONNECTION = 100
CONNECTION_TIMEOUT = 10
CONNECTION_RETRIES = 10

socket.setdefaulttimeout(CONNECTION_TIMEOUT)

class HTMLDownloadMaster(object):

    COOKIE_FILE_NAME = 'cookie.txt'
    REQUEST_HEADER = {'User-Agent':"Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0",
                      'Accept':'image/png,image/*;q=0.8,*/*;q=0.5',
                      'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                      'Accept-Encoding':'gzip, deflate',
                      'Connection':'keep-alive',
                      'DNT':'1',
                      'Host': 'www.amazon.com'}

    def __init__(self):
        self.cookie_is_saved = False
        self.cookiejar = http.cookiejar.MozillaCookieJar(self.COOKIE_FILE_NAME)
        if os.path.exists(self.COOKIE_FILE_NAME):
            self.cookiejar.load(self.COOKIE_FILE_NAME, ignore_discard=True, ignore_expires=True)
        handler = urllib.request.HTTPCookieProcessor(self.cookiejar)
        self.opener = urllib.request.build_opener(handler)
        headers = [i for i  in self.REQUEST_HEADER.items()]
        self.opener.addheaders = headers

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
    ''' fetch reviews from url '''
    def __init__(self, url):
        self.downloader = HTMLDownloadMaster()
        self.review_list = []
        self.failed_download_pages = []

        _str_sort_by_helpful = 'sortBy=helpful'
        _str_sort_by_recent = 'sortBy=recent'
        _str_all_formats = 'formatType=all_formats'
        _str_current_format = 'formatType=current_format'
        url_part_sort_by_recent = 'ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=avp_only_reviews&formatType=all_formats&pageNumber=1&sortBy=recent'
        url_part_sort_by_recent_uk = 'ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&showViewpoints=1&sortBy=recent&pageNumber=1'
        
        if 'product-reviews' in url and re.search('pageNumber=\d+', url):
            _base_url = url.replace(_str_sort_by_helpful, _str_sort_by_recent)
        else:
            if 'amazon.co.uk' in url:
                product_id = re.search('(/(dp)|(/product-reviews))/(\w+)/', url).group(4)
                _base_url = 'https://www.amazon.co.uk/product-reviews/%s/%s'  % (product_id, url_part_sort_by_recent_uk)
            else:
                _url_for_review = url.replace('/dp/', '/product-reviews/')
                _base_url = '%s%s'  % (_url_for_review.split('ref=')[0], url_part_sort_by_recent)  
        self.base_url = _base_url


    def get_total_page_numbers(self):
        print('getting first review page ...')
        first_page_html = self.downloader.get_remote_html(self.base_url)
        tree = lxml.html.fromstring(first_page_html)
        page_butten_list = tree.xpath("//*[@id='cm_cr-pagination_bar']/ul/li//text()")
        if len(page_butten_list) == 0:
            return 1
        page_butten_list = [i.replace(',', '') for i in page_butten_list]
        page_butten_list = [i for i in page_butten_list if i.strip().isdigit()]
        page_butten_list = [int(i) for i in page_butten_list]
        total_page_numbers = max(page_butten_list)
        return total_page_numbers


    def get_product_name(self):
        if 'amazon.co.uk' in self.base_url:
            product_name = self.base_url.split('/')[4]
        else:
            product_name = self.base_url.split('/')[3]
        return product_name


    def fetch_reviews_from_html(self, html):
        item_xpath = "//div[starts-with(@id, 'customer_review-')]"
        star_xpath = ".//a[@class='a-link-normal' and starts-with(@href, '/gp/customer-reviews/')]//@title"
        title_xpath = ".//a[contains(@class, 'review-title')]//text()"
        author_xpath = ".//a[contains(@class, 'author')]//text()"
        date_xpath = ".//span[contains(@class, 'review-date')]//text()"
        text_xpath = ".//span[contains(@class, 'review-text')]//text()"
        vote_xpath = ".//span[contains(@class, 'review-vote')]//text()"
        
        review_list = []
        tree = lxml.html.fromstring(html)
        item_entries = tree.xpath(item_xpath)
        for entry in item_entries:
            customer = entry.get('id')
            star_str = entry.xpath(star_xpath)[0]
            star = int(re.search('\d+', star_str).group())
            title = entry.xpath(title_xpath)[0]
            author = entry.xpath(author_xpath)[0]
            date_str = entry.xpath(date_xpath)[0].replace('on ', '')
            date = self._transform_review_date_format(date_str)
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


    def fetch_reviews_from_page_number(self, page):
        url = re.sub('pageNumber=\d+', 'pageNumber=%s' % page, self.base_url)
        html = self.downloader.get_remote_html(url)
        review_list = self.fetch_reviews_from_html(html)
        return review_list


    def fetch_all_reviews(self):
        lock=threading.Lock()
        download_thread_list = []
        total_page_number = self.get_total_page_numbers()
        print('%s pages in total.'  % total_page_number)
        for page in range(1, total_page_number + 1):
            url = re.sub('pageNumber=\d+', 'pageNumber=%s' % page, self.base_url)
            page_download_thread = threading.Thread(target=self._fetch_reviews_and_extend_to_review_list, 
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
            msg = 'downloading %s pages, pending %s pages, %s pages failed ...' % \
                  (alive_thread_number, pending_thread_number, fail_num)
            msg_lenth = len(msg)
            output_msg_lenth = max(output_msg_lenth, msg_lenth)
            new_msg = (' '*output_msg_lenth).replace(' '*msg_lenth, msg)
            sys.stdout.write('\r%s' % new_msg)
            for i in range(min(free_thread_number, pending_thread_number)):
                download_thread_list[0].start()
                active_thread_list.append(download_thread_list[0])
                download_thread_list.pop(0)
            time.sleep(0.5)
        msg = 'html download finished, %s pages downloaded and %s failed.'  % (total_page_number - fail_num, fail_num)
        msg_lenth = len(msg)
        new_msg = (' '*output_msg_lenth).replace(' '*msg_lenth, msg)
        sys.stdout.write('\r%s\n' % new_msg)
        
        if self.failed_download_pages:
            print('Failed to get the following pages:\n%s'  % self.failed_download_pages)
        return self.review_list


    def _fetch_reviews_and_extend_to_review_list(self, lock, url):
        page_num = int(re.search('pageNumber=(\d+)', url).group(1))
        try:
            html = self.downloader.get_remote_html(url)
        except Exception as e:
            lock.acquire()
            self.failed_download_pages.append(page_num)
            lock.release()
            raise Exception(e)
        reviews_in_html = self.fetch_reviews_from_html(html)
        lock.acquire()
        self.review_list.extend(reviews_in_html)
        lock.release()
        
        
    def get_failed_page_list(self):
        return self.failed_download_pages


    def _transform_review_date_format(self, date):
        ''' transform ''March 25, 2017' to datetime.datetime(2017, 3, 25) '''
        
        # for uk, transform '11 March 2016' to 'March 11, 2016'
        if re.match('\A\d+ \w+ \d+\Z', date) is not None:
            date_split = date.split(' ')
            date = '%s %s, %s'  % (date_split[1], date_split[0], date_split[2])  
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

class ReviewsFilter(object):
    def __init__(self, review_list):
        self.review_list = review_list
        self.review_list_classified_by_month = None
        self.review_list_classified_by_star = None
                 
        
    def _get_review_list_classified_by_month(self):
        ''' classify reviews by month 
        Returns(dict):
            {datetime.datetime(2017, 3, 1):[a list of reviews during 2017.3], ...}
        '''
        if self.review_list_classified_by_month is not None:
            return self.review_list_classified_by_month
        review_list_classified_by_month = {}
        for review in self.review_list:
            date = review['date']
            month = datetime.datetime(date.year, date.month, 1)
            if month not in review_list_classified_by_month:
                review_list_classified_by_month[month] = []
            review_list_classified_by_month[month].append(review)
        self.review_list_classified_by_month = review_list_classified_by_month
        return review_list_classified_by_month


    def _get_review_list_classified_by_star(self):
        ''' classify reviews by star 
        Returns(dict):
            {1:[a list of reviews with star=1], ...}
        '''
        if self.review_list_classified_by_star is not None:
            return self.review_list_classified_by_star
        review_list_classified_by_star = {}
        for review in self.review_list:
            star = review['star']
            if star not in review_list_classified_by_star:
                review_list_classified_by_star[star] = []
            review_list_classified_by_star[star].append(review)
        self.review_list_classified_by_star = review_list_classified_by_star
        return review_list_classified_by_star


    def get_month_list(self):
        month_list = self._get_review_list_classified_by_month().keys()
        month_list = list(month_list)
        month_list.sort()
        return month_list

    def get_reviews_by_month(self, month):
        month = datetime.datetime(month.year, month.month, 1)
        review_list = self._get_review_list_classified_by_month().get(month, [])
        return review_list
            
    def get_reviews_by_star(self, star):
        review_list = self._get_review_list_classified_by_star().get(star, [])
        return review_list

    def sort_reviews_by_date(self, reverse=True):
        list_sorted = []
        date_list = list(set([i['date'] for i in self.review_list]))
        date_list.sort()
        if reverse:
            date_list.reverse()
        for date in date_list:
            review_list_with_current_date = [i for i in self.review_list if i['date'] == date] 
            list_sorted.extend(review_list_with_current_date)
        self.review_list = list_sorted
        return list_sorted

    def sort_reviews_by_vote(self, reverse=True):
        list_sorted = []
        vote_list = list(set([i['vote'] for i in self.review_list]))
        vote_list.sort()
        if reverse:
            vote_list.reverse()
        for vote in vote_list:
            review_list_with_current_vote = [i for i in self.review_list if i['vote'] == vote] 
            list_sorted.extend(review_list_with_current_vote)
        self.review_list = list_sorted
        return list_sorted

class StatisticsAndSave(object):
    def __init__(self, review_list):
        self.review_filter = ReviewsFilter(review_list)
        self.result_book = xlwt.Workbook()      # an excel book to save result
        self.statistic_result = None
        
    
    def _write_row_elements_into_data_sheet(self, sheet, row, column, elements):
        for elem in elements:
            sheet.write(row, column, elem)
            column = column + 1
        row = row + 1
        column = 0
        return (row, column)
    
    
    def _get_review_elements(self, review):
        ''' return review dict's values sorted by ('Date', 'Author', 'Star', 'Vote Number', 'Comment Title', 'Comment Text') '''
        date = review['date']
        date_str = '%s-%s'  % (date.year, date.month)
        author = review['author']
        star = review['star']
        vote = review['vote']
        title = review['title']
        text = review['text']
        return (date_str, author, star, vote, title, text)
                    
    
    def get_monthly_statistics(self):
        header = ('Month', 'Month num', 'Total num', 'One', 'Two', 'Three', 'Four', 'Five', 'Month Avg', 'Total Avg')
        if self.statistic_result is not None:
            return self.statistic_result
        else:
            self.statistic_result = []
            self.statistic_result.append(header)
        total_star_sum = total_review_num = 0
        month_list = self.review_filter.get_month_list()
        for month in sorted(month_list):
            month_str = '%s-%s'  % (month.year, month.month)
            reviews = self.review_filter.get_reviews_by_month(month)
            reviews_filter = ReviewsFilter(reviews)
            one_star_num = len(reviews_filter.get_reviews_by_star(1))
            two_star_num = len(reviews_filter.get_reviews_by_star(2))
            three_star_num = len(reviews_filter.get_reviews_by_star(3))
            four_star_num = len(reviews_filter.get_reviews_by_star(4))
            five_star_num = len(reviews_filter.get_reviews_by_star(5))
            month_total_review_num = len(reviews)
            month_star_sum = one_star_num + two_star_num*2 + three_star_num*3 + four_star_num*4 + five_star_num*5
            if month_total_review_num == 0:
                month_avg_star = 0
            else:
                month_avg_star = round(month_star_sum/month_total_review_num, 2)
            total_star_sum = total_star_sum + month_star_sum
            total_review_num = total_review_num + month_total_review_num
            if total_review_num == 0:
                total_avg_star = 0
            else:
                total_avg_star = round(total_star_sum / total_review_num, 1)
            
            elem = (month_str, month_total_review_num, total_review_num, one_star_num, two_star_num, 
                    three_star_num, four_star_num, five_star_num, month_avg_star, total_avg_star)
            self.statistic_result.append(elem)
        return self.statistic_result
                 
    
    def show_statistics(self):
        result = self.get_monthly_statistics()
        print('--'*50)
        for line in result:
            for i in line:
                print('%-10s'  % i, end='')
            print('')
        print('--'*50)
            
    def _save_statistics(self):
        sheet = self.result_book.add_sheet('Statistics')
        row = column = 0
        result = self.get_monthly_statistics()
        for line in result:
            row, column = self._write_row_elements_into_data_sheet(sheet, row, column, line)
            

    def _save_all_reviews_sorted_by_date(self):
        ''' save all the review items into a sheet '''
        sheet = self.result_book.add_sheet('All Reviews')
        sheet_header = ('Date', 'Author', 'Star', 'Vote Number', 'Comment Title', 'Comment Text')
        row = column = 0
        row, column = self._write_row_elements_into_data_sheet(sheet, row, column, sheet_header)
        all_reviews = self.review_filter.sort_reviews_by_date()
        for review in all_reviews:
            elements = self._get_review_elements(review)
            row, column = self._write_row_elements_into_data_sheet(sheet, row, column, elements)
        

    def _save_reviews_classified_by_star(self):
        ''' save reviews with diffent star into differnt sheet '''
        sheet_header = ('Date', 'Author', 'Star', 'Vote Number', 'Comment Title', 'Comment Text')
        for star in (1, 2, 3, 4, 5):
            sheet = self.result_book.add_sheet('%s Star' % star)
            row = column = 0
            row, column = self._write_row_elements_into_data_sheet(sheet, row, column, sheet_header)
            reviews = self.review_filter.get_reviews_by_star(star)
            for review in reviews:
                elements = self._get_review_elements(review)
                row, column = self._write_row_elements_into_data_sheet(sheet, row, column, elements)
        

    def _save_most_helpful_reviews(self, min_vote=5):
        sheet = self.result_book.add_sheet('Most Helpful')
        sheet_header = ('Date', 'Author', 'Star', 'Vote Number', 'Comment Title', 'Comment Text')
        row = column = 0
        row, column = self._write_row_elements_into_data_sheet(sheet, row, column, sheet_header)
        reviews = self.review_filter.sort_reviews_by_vote()
        for review in reviews:
            if review['vote'] < min_vote:
                break
            elements = self._get_review_elements(review)
            row, column = self._write_row_elements_into_data_sheet(sheet, row, column, elements)
            
    def save_all_to_excel(self, product_name=None):
        date_stap = time.strftime('%Y%m%d', time.localtime())
        time_stap = time.strftime('%H%M%S', time.localtime())
        if not product_name:
            save_name = 'Reviews_%s_%s.xls'  % (date_stap, time_stap)
        else:
            save_name = 'Reviews_%s_%s.xls'  % (product_name, date_stap)
        self._save_statistics()
        self._save_all_reviews_sorted_by_date()
        self._save_reviews_classified_by_star()
        self._save_most_helpful_reviews()
        self.result_book.save(save_name)

        
def get_reviews():
    product_name = input('Product name(optional): ').strip()
    url = input('Product page url or reviews page url: ').strip()
    # product_name = ''
    # url = 'https://www.amazon.com/Apple-Factory-Unlocked-Internal-Smartphone/dp/B00NQGP42Y/ref=sr_1_4?s=wireless&ie=UTF8&qid=1491201041&sr=1-4&keywords=iphone'
    start_time = datetime.datetime.now()
    print('Start at %s'  % str(start_time).split('.')[0])
    fetcher = GetReviews(url)
    if not product_name.strip():
        product_name = fetcher.get_product_name()
    print('Product: %s'  % product_name)
    review_list = fetcher.fetch_all_reviews()
    master = StatisticsAndSave(review_list)
    master.save_all_to_excel(product_name)
    master.show_statistics()
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    total_seconds = duration.days*24*3600 + duration.seconds
    hours = int(total_seconds/3600)
    mins = int((total_seconds - hours*3600) / 60)
    seconds = total_seconds - hours*3600 - mins*60
    print('End at %s, duration is %sh %smin %ss.'  % (str(end_time).split('.')[0], hours, mins, seconds))
     
if __name__=="__main__":
    while True:
        get_reviews()
    