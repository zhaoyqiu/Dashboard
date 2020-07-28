#!/usr/bin/env python
# coding: utf-8

# In[13]:


import datetime
from datetime import timedelta
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


# In[14]:


dw_order_by_day.info()


# In[15]:


#create_date转化为日期格式
dw_order_by_day['create_date']=pd.to_datetime(dw_order_by_day['create_date'])


# In[71]:


dw_order_by_day['create_date']


# In[17]:


#在dw_order_by_day中加上同比
#获得今日日期
today_date=datetime.date.today()


# In[18]:


today_date 


# In[55]:


def diff(stage,indictor):
    """
    stage:日期维度的判断，如：is_today内有[0,1]
    indictor:需取值字段，如：sum_amount(总金额)，sum_order(总订单量)
    输出：当前时间维度下总和，如年同期总和
    """
    #求当前日期维度stage下的indictor总和
    current_stage_indictor=dw_order_by_day[dw_order_by_day
                                          [stage]==1][indictor].sum()
    #取出当前日期维度下的前年对应日期列表
    before_stage_list=list(dw_order_by_day[dw_order_by_day[stage]==1]
                                          ['create_date']+timedelta(days=-365))
    #求当前日期维度下的前一年对应indictor总和
    before_stage_indictor=dw_order_by_day[dw_order_by_day['create_date']
                                                         .isin(before_stage_list)][indictor].sum()
    return current_stage_indictor,before_stage_indictor   


# In[58]:


#各阶段的金额（sum_amount）
today_amount, before_year_today_amount = diff('is_today', 'sum_amount')
yesterday_amount, before_year_yesterday_amount = diff('is_yesterday', 'sum_amount')
month_amount, before_year_month_amount = diff('is_current_month', 'sum_amount')
quarter_amount, before_year_quarter_amount = diff('is_current_quarter', 'sum_amount')
year_amount, before_year_year_amount = diff('is_current_year', 'sum_amount')


# In[59]:


#各阶段的订单数（sum_order）
today_order, before_year_today_order = diff('is_today', 'sum_order')
yesterday_order, before_year_yesterday_order = diff('is_yesterday', 'sum_order')
month_order, before_year_month_order = diff('is_current_month', 'sum_order')
quarter_order, before_year_quarter_order = diff('is_current_quarter', 'sum_order')
year_order, before_year_year_order = diff('is_current_year', 'sum_order')


# In[61]:


'''同比增长或同比下降(均与去年对比)：总金额/订单量/客单价，当日/昨日/当月/当季/当年/
    如：今天订单量110 前年今天订单量100，则110/100-1 = 0.1，增长10%'''
amount_dic = {'today_diff': [today_amount / before_year_today_amount - 1,
                                     today_order / before_year_today_order - 1,
                                     (today_amount / today_order) / (before_year_today_amount /
                                                                     before_year_today_order) - 1],
                      'yesterday_diff': [yesterday_amount / before_year_yesterday_amount - 1,
                                         yesterday_order / before_year_yesterday_order - 1,
                                         (yesterday_amount / yesterday_order) / (before_year_yesterday_amount /
                                                                                 before_year_yesterday_order) - 1],
                      'month_diff': [month_amount / before_year_month_amount - 1,
                                     month_order / before_year_month_order - 1,
                                     (month_amount / month_order) / (before_year_month_amount /
                                                                     before_year_month_order) - 1],
                      'quarter_diff': [quarter_amount / before_year_quarter_amount - 1,
                                       quarter_order / before_year_quarter_order - 1,
                                       (quarter_amount / quarter_order) / (before_year_quarter_amount /
                                                                           before_year_quarter_order) - 1],
                      'year_diff': [year_amount / before_year_year_amount - 1,
                                    year_order / before_year_year_order - 1,
                                    (year_amount / year_order) / (before_year_year_amount /
                                                                  before_year_year_order) - 1],
                      'flag': ['amount', 'order', 'avg']}  # 做符号简称，横向提取数据方便

amount_diff = pd.DataFrame(amount_dic)


# In[62]:


amount_diff


# In[70]:


amount_diff.to_sql('dw_amount_diff_zhaoyqiu', con=adventure_conn_tosql,if_exists='append', index=False)  # 存储为当日维度表


# In[68]:


amount_diff.to_excel(os.path.join(output_path,'amount_diff.xlsx'),index=False)


# In[ ]:




