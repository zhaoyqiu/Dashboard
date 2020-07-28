#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import sqlalchemy

import os
#结果保存路径
output_path='F:/some_now/pro_output'

if not os.path.exists(output_path):
    os.makedirs(output_path)

#     engine = create_engine('mysql+pymysql://用户名:密码@ip地址:端口号/数据库名称')
adventure_conn_read = sqlalchemy.create_engine('mysql://frogdata001:Frogdata@144@106.13.128.83:3306/adventure_ods')
adventure_conn_tosql = sqlalchemy.create_engine('mysql+pymysql://frogdata05:Frogdata!1321@106.15.121.232:3306/datafrog05_adventure')
dw_order_by_day  = pd.read_sql_query("select * from dw_order_by_day ",
                                             con=adventure_conn_read) 


# In[46]:


# 读取每日新增用户表ods_customer
customer_sql="""
   select customer_key,
           chinese_territory,
           chinese_province,
           chinese_city
           from ods_customer"""

customer_info=pd.read_sql_query(customer_sql,con=adventure_conn_read)

def order_data(today_date, tomorrow_date, adventure_conn_read):
   '''
   读取今日的ods_sales_orders（订单明细表)
   :param tomorrow_date:
   :param today_date:
   :param adventure_conn_read: 读取数据库
   :return:
   '''
   order_sql = """select sales_order_key,
                     create_date,
                     customer_key,
                     english_product_name,
                     cpzl_zw,
                     cplb_zw,
                     unit_price  
           from ods_sales_orders  where create_date>='{today_date}' and create_date<'{tomorrow_date}'
           """.format(today_date=today_date, tomorrow_date=tomorrow_date)
   order_info = pd.read_sql_query(order_sql, con=adventure_conn_read)
   return order_info

def date_data(adventure_conn_to_sql):
   #读取日期维度表dim_date_df
   date_sql="""
       select create_date,
       is_current_year,
               is_last_year,
               is_yesterday,
               is_today,
               is_current_month,
               is_current_quarter
               from dim_date_df"""
   date_info = pd.read_sql_query(date_sql, con=adventure_conn_tosql)
   return date_info

def sum_data(order_info,customer_info,date_info):
   """
   order_info:当天订单明细表ods_sales_orders
       customer_info:每日新增用户表ods_customer
       date_info:日期维度表
   
   """
   #通过客户编号连接表
   sales_customer_order=pd.merge(order_info,customer_info,left_on='customer_key',
                                    right_on='customer_key',how='left')
    # 提取订单主键/订单日期/客户编号/产品名/产品子类/产品类别/产品单价/所在区域/所在省份/所在城市
   sales_customer_order=sales_customer_order[["sales_order_key", "create_date", "customer_key",
                                                    "english_product_name", "cpzl_zw", "cplb_zw", "unit_price",
                                                    "chinese_territory",
                                                    "chinese_province",
                                                    "chinese_city"]]
   # 形成按照 订单日期/产品名/产品子类/产品类别/所在区域/所在省份/所在城市的逐级聚合表，获得订单总量/客户总量/销售总金额
   sum_customer_order = sales_customer_order.groupby(["create_date", "english_product_name", "cpzl_zw", "cplb_zw",
                                                          "chinese_territory", "chinese_province",
                                                          "chinese_city"], as_index=False). \
           agg({'sales_order_key': pd.Series.nunique, 'customer_key': pd.Series.nunique,
                "unit_price": "sum"}).rename(columns={'sales_order_key': 'order_num', \
                                                      'customer_key': 'customer_num', 'unit_price': 'sum_amount', \
                                             "english_product_name": "product_name"}) 
   # 转化订单日期为字符型格式
   sum_customer_order['create_date'] = sum_customer_order['create_date'].apply(
                   lambda x: x.strftime('%Y-%m-%d'))      
   sum_customer_order = pd.merge(sum_customer_order, date_info, on='create_date', how='inner')             # 获取当日日期维度
   return sum_customer_order

#获取昨日时间
start_time=datetime.date.today()+datetime.timedelta(days=-1)
#获取今日时间
end_time=datetime.date.today()
#获取天数
interval_num=(end_time-start_time).days


for i in range(1,interval_num+1):
   start_date=(start_time+datetime.timedelta(days=i)).strftime('%Y-%m-%d')
   end_date=(start_time+datetime.timedelta(days=i+1)).strftime('%Y-%m-%d')
   #获取订单信息
   order_info=order_data(start_date,end_date,adventure_conn_read)
   #获取时间信息表
   date_info=date_data(adventure_conn_read)
   #将订单信息和商户信息进行融合并汇总
   # 形成按照 订单日期/产品名/产品子类/产品类别/所在区域/所在省份/所在城市的逐级聚合表，获得订单总量/客户总量/销售总金额+当日日期维度
   sum_customer_order=sum_data(order_info,customer_info,date_info)
   


# In[47]:


sum_customer_order


# In[21]:


customer_info


# In[13]:


order_info


# In[14]:


date_info


# In[ ]:




