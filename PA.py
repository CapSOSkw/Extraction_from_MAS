import requests
import pandas as pd
import re
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta
import schedule
from time import sleep
from bs4 import BeautifulSoup
import os,io
from lxml import etree, html
from sqlalchemy import create_engine
import pymysql
# os.chdir("/Users/KeyuanWu/Desktop/PAnumber/")
pd.options.mode.chained_assignment = None

def extract_digit(x):
    result = re.findall(r'\d+', str(x))
    if len(result) == 0 or int(result[0]) == 0:
        return None
    else:
        return result[0]


class PAnumber():

    def __init__(self):
        # self.end_date = datetime.today().date()  # Today'date
        # self.start_date = datetime.today().date()
        #
        # self.end_date = self.end_date.strftime("%m/%d/%Y")  # In correct format
        # self.start_date = self.start_date.strftime("%m/%d/%Y")

        self.end_date = "03/04/2018"            #用于手动更新
        self.start_date = "03/04/2018"

        self.USERNAME = "******"
        self.PASSWORD = "*********"

        # start = datetime.strptime(self.start_date, "%m/%d/%Y").strftime("%m_%d_%Y")
        # end = datetime.strptime(self.end_date, "%m/%d/%Y").strftime("%m_%d_%Y")
        # self.file_name = str(start) + "-" + str(end)

        self.login_url = 'https://www.medanswering.com/login.taf?_function=check'
        self.login_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        self.login_payload = {
            "User_Name": self.USERNAME,
            "Password": self.PASSWORD,
        }

        self.target_url = "https://www.medanswering.com/adminrosterexport.taf?"
        self.target_params = {
            "_function": "export",
            "Changed_Since_Vendor_Notified":"",
            "Correction_Status":"",
            "End_Date_Exported":"",
            "End_Effective_Date":"",
            "End_Service_End": self.end_date,
            "Exported":"",
            "First_Name":"",
            "Invoice_Number":"",
            "Last_Name":"",
            "Medicaid_County_Number":"",
            "Medicaid_Number":"",
            "PA_Submission_Result":"",
            "Part_of_Split_Series":"",
            "Printed":"",
            "Sort_By":"Sort_By:Service_Starts_Oldest_to_Newest",
            "Standing_Order":"",
            "Start_Date_Exported":"",
            "Start_DOB":"",
            "Start_Effective_Date":self.start_date,
            "Start_Service_End":"",
            "Status":"",
            "Transport_Type_ID":"",
        }


    def download_day_data(self):   #每天download一次单日数据，并入数据库中作为备份文件。

        cnx = create_engine(
            'mysql+pymysql://collision_dev:collision_dev123@collision.cluster-cycfznhuxf3w.us-east-1.rds.amazonaws.com:3306/PA_Number',
            echo=False)

        with requests.Session() as session:
            post_login = session.post(self.login_url, data=self.login_payload, headers=self.login_headers)

            target_page = session.get(self.target_url, params=self.target_params)
            # plain_target = target_page.text

            tree = html.fromstring(target_page.content)
            part_link = tree.xpath('/html/body/center/p[3]/b/a/@href')
            result_link = "https://www.medanswering.com" + part_link[0]

            temp_df = pd.read_csv(result_link, delimiter='\t')
            temp_df['PA_Number'] = temp_df['Prior Approval Number'].apply(lambda x: extract_digit(x))
            temp_df['Service Starts'] = temp_df['Service Starts'].apply(lambda x: datetime.strptime(str(x), "%m/%d/%y").strftime("%Y-%m-%d"))

            df = temp_df[['Recipient ID', 'Invoice Number', 'PA_Number','Service Starts', 'Ordering Provider', 'Item Code', 'Item Code Mod','$Amt', 'Qty','Days/Times']]
            df['PA_Update'] = datetime.today().date()
            # df = df.drop_duplicates(subset=['Invoice Number'], keep='first')

        df.to_sql(name='all_PA_records', con=cnx, if_exists='append', index=False)


    def download_15d_data(self, start, end):  # download 15天的数据，用于检测15天前的单子有没有拿到PA number

        params = {
            "_function": "export",
            "Changed_Since_Vendor_Notified":"",
            "Correction_Status":"",
            "End_Date_Exported":"",
            "End_Effective_Date":"",
            "End_Service_End": end,
            "Exported":"",
            "First_Name":"",
            "Invoice_Number":"",
            "Last_Name":"",
            "Medicaid_County_Number":"",
            "Medicaid_Number":"",
            "PA_Submission_Result":"",
            "Part_of_Split_Series":"",
            "Printed":"",
            "Sort_By":"Sort_By:Service_Starts_Oldest_to_Newest",
            "Standing_Order":"",
            "Start_Date_Exported":"",
            "Start_DOB":"",
            "Start_Effective_Date":start,
            "Start_Service_End":"",
            "Status":"",
            "Transport_Type_ID":"",}

        with requests.Session() as session:
            post_login = session.post(self.login_url, data=self.login_payload, headers=self.login_headers)
            target_page = session.get(self.target_url, params=params)
            tree = html.fromstring(target_page.content)
            part_link = tree.xpath('/html/body/center/p[3]/b/a/@href')
            result_link = "https://www.medanswering.com" + part_link[0]
            temp_df = pd.read_csv(result_link, delimiter='\t')
            temp_df['PA_Number'] = temp_df['Prior Approval Number'].apply(lambda x: extract_digit(x))
            temp_df['Service Starts'] = temp_df['Service Starts'].apply(
                lambda x: datetime.strptime(str(x), "%m/%d/%y").strftime("%Y-%m-%d"))

            df = temp_df[['Recipient ID', 'Invoice Number', 'PA_Number', 'Service Starts', 'Ordering Provider', 'Item Code', 'Item Code Mod','$Amt', 'Qty','Days/Times']]
            df['PA_Update'] = datetime.today().date()
            # df = df.drop_duplicates(subset=['Invoice Number'], keep='first')

        return df


    def update_PA(self):               #依据过了15天的数据来更新PA number
        cnx = create_engine(
            'mysql+pymysql://collision_dev:collision_dev123@collision.cluster-cycfznhuxf3w.us-east-1.rds.amazonaws.com:3306/PA_Number',
            echo=False)

        df = pd.read_sql("SELECT * FROM PA_Number.all_PA_records", con=cnx)

        # end_date = datetime.today().date()
        end_date = datetime.strptime(self.start_date, "%m/%d/%Y").date() #用于手动更新

        start_date = end_date - relativedelta(days=15)

        end_date_format = end_date.strftime("%m/%d/%Y")  # In correct format
        start_date_format = start_date.strftime("%m/%d/%Y")

        d15_df = self.download_15d_data(start_date_format, end_date_format)

        most_updated_df = d15_df.loc[d15_df["Service Starts"] == start_date]
        my_dict = most_updated_df.set_index("Invoice Number").T.to_dict('list')
        to_update_list = list(my_dict.keys())

        target_df = df.loc[df["Service Starts"] == str(start_date)]
        # print(target_df)


        for i in to_update_list:
            target_df.loc[target_df["Invoice Number"] == i, "PA_Number"] = my_dict[i][1]  # update PA number
            if my_dict[i][1] != None:
                target_df.loc[target_df["Invoice Number"] == i, "PA_Update"] = datetime.today().date()

        wrong_code = target_df.loc[target_df['$Amt']==0]
        wrong_code.to_sql(name='wrong_code', con=cnx, if_exists='append', index=False)   # 把amount为0的数据存入数据库

        # target_df.to_sql(name='test', con=cnx, if_exists='append', index=False)
        target_df_no_copy = target_df.drop_duplicates(subset=['Invoice Number'], keep='first')
        print(target_df_no_copy)
        target_df_no_copy.to_sql(name='update_PA_records', con=cnx, if_exists='append', index=False)


    def search_online(self, invoice):   #用于单个搜索，更新15天还没有拿到PA number的invoice number的状态， 是取消了还是正在等待？
        trip_auth_id = int(str(invoice)[:-1])
        cnx = create_engine(
            'mysql+pymysql://collision_dev:collision_dev123@collision.cluster-cycfznhuxf3w.us-east-1.rds.amazonaws.com:3306/PA_Number',
            echo=False)

        df_null = pd.read_sql("SELECT * FROM PA_Number.update_PA_records WHERE PA_Number is null", con=cnx) # get rows where pa number is null

        target_url = 'https://www.medanswering.com/admintrips.taf?'

        post_params = {
            "Invoice_Number": invoice,
            "Medicaid_County_Number":"",
            "Status":"",
            "PA_Submission_Result":"",
            "Correction_Status":"",
            "Changed_Since_Vendor_Notified":"",
            "Exported":"",
            "Medicaid_Number":"",
            "First_Name":"",
            "Last_Name":"",
            "Start_DOB":"",
            "Standing_Order":"",
            "Part_of_Split_Series":"",
            "Printed":"",
            "Transport_Type_ID":"",
            "Start_Effective_Date":"",
            "End_Effective_Date":"",
            "Start_Service_End":"",
            "End_Service_End":"",
            "Start_Date_Exported":"",
            "End_Date_Exported":"",
            "Sort_By":"Service_Starts_Oldest_to_Newest",
            "_function":"list",
        }

        get_params = {
            "_function":"detail",
            "Trip_Auth_ID": trip_auth_id
        }

        post_header = {
                         "Accept-Encoding": "gzip, deflate, br",
                        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                        "Cache-Control":"max-age=0",
                        "Content-Length":"428",
                        "Content-Type":"application/x-www-form-urlencoded",
                        "Origin":"https://www.medanswering.com",

                        "Connection": "keep-alive",
                        "Host":"www.medanswering.com",
                        "Referer":"https://www.medanswering.com/admintrips.taf?",
                        "Upgrade-Insecure-Requests":"1",
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',

        }

        with requests.Session() as session:
            post_login = session.post(self.login_url, data=self.login_payload, headers=self.login_headers)
            target_page = session.get(target_url, data=get_params,)
            soup = BeautifulSoup(target_page.text, 'html.parser')

            for tr in soup.findAll('tr'):
                for td in tr.findAll('td'):
                    status = re.sub('\s+', '', td.text)
                    if status == "Cancelled" or status == "Eligible" or status == "Cancelled/Reassign":
                        return status


    def update_status(self):       # 把PA number的状态（取消，等待， etc）更新进数据库
        # today_date = datetime.today().date()
        today_date = datetime.strptime(self.start_date, "%m/%d/%Y").date()    #手动更新日期

        start_date = today_date - relativedelta(days=15)

        cnx = create_engine(
            'mysql+pymysql://collision_dev:collision_dev123@collision.cluster-cycfznhuxf3w.us-east-1.rds.amazonaws.com:3306/PA_Number',
            echo=False)

        df = pd.read_sql("SELECT * FROM PA_Number.update_PA_records", con=cnx)

        d15_df = df.loc[(df["Service Starts"] == str(start_date)) & (df["PA_Number"].isnull())]

        d15_df["PA_Number"] = d15_df['Invoice Number'].apply(lambda x: self.search_online(x))
        d15_df["PA_Update"] = datetime.today().date()

        # df.update(d15_df)
        d15_df.to_sql(name='check_status_records', con=cnx, if_exists='append', index=False)


    def main(self):              # 一天运行一次main
        self.download_day_data()  # 先更新当日的数据

        self.update_PA()        # 下载15天前到今天的数据，用于更新15天前的PA number

        self.update_status()      # 更新状态，只更新15天还没拿到PA number的数据


if __name__ == "__main__":
    # schedule.every().day.at("09:15").do(PAnumber().main)
    #
    # while 1:
    #     schedule.run_pending()
    #     sleep(1)

    PAnumber().main()

