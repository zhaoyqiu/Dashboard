#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import random
from database import Connector
import warnings
warnings.filterwarnings("ignore")
from log import get_logger,STUNDENT_NAME
logger = get_logger("dw_order_by_day.log")


# In[2]:


import os
#结果保存路径
output_path='F:/some_now/pro_output'

if not os.path.exists(output_path):
    os.makedirs(output_path)


# In[3]:


import pymysql
pymysql.install_as_MySQLdb()
import sqlalchemy


# In[4]:


if __name__ == "__main__":
    '''目的：生成dw_order_by_day每日环比表(销量订单聚合表+目标值+日期维度表)'''
#     engine = create_engine('mysql+pymysql://用户名:密码@ip地址:端口号/数据库名称')
    adventure_conn_read = sqlalchemy.create_engine('mysql://frogdata001:Frogdata@144@106.13.128.83:3306/adventure_ods')


# In[6]:


dw_order_by_day


# In[5]:


adventure_conn_tosql = sqlalchemy.create_engine('mysql+pymysql://frogdata05:Frogdata!1321@106.15.121.232:3306/datafrog05_adventure')


# In[12]:


#     engine = create_engine('mysql+pymysql://用户名:密码@ip地址:端口号/数据库名称')
adventure_conn_read = sqlalchemy.create_engine('mysql://frogdata001:Frogdata@144@106.13.128.83:3306/adventure_ods')

sum_amount_order = pd.read_sql_query("select * from ods_sales_orders ",
                                             con=adventure_conn_read)     


# In[13]:


sum_amount_order_=sum_amount_order.copy()


# In[38]:


sum_amount_order_1=sum_amount_order_.copy()


# ### (1)sum_amount_order（按天计算订单量和交易金额）

# In[14]:


#根据create_date分组求总和单价和客户数量
sum_amount_order=sum_amount_order.groupby(by='create_date').agg(
    {'unit_price':sum,'customer_key':pd.Series.nunique}).reset_index()

sum_amount_order.rename(columns={'unit_price':'sum_amount',
                                 'customer_key':'sum_order'},
                       inplace=True)
#客单价
sum_amount_order['amount_div_order']=    sum_amount_order['sum_amount']/sum_amount_order['sum_order']


# In[15]:


sum_amount_order.head(2)


# ### (2)sum_amount_order_goal(按照一定规则生成目标值)

# In[18]:


#利用空列表及循环生成对应随机值
sum_amount_goal_list=[]
sum_order_goal_list=[]
#获取sum_amount_order中的create_date
create_date_list=list(sum_amount_order['create_date'])


# In[21]:


for i in create_date_list:
    #生成一个在[0.85,1.1]随机数
    a=random.uniform(0.85,1.1)
    b=random.uniform(0.85,1.1)
#     对应日期下生成总金额(sum_amount)*a 的列
    amount_goal=list(sum_amount_order[sum_amount_order['create_date']==i]
                    ['sum_amount'])[0]*a
    #     对应日期下生成总订单数(sum_order)*b 的列
    order_goal=list(sum_amount_order[sum_amount_order['create_date']==i]
                   ['sum_order'])[0]*b
    #将生成的目标值加入空列表
    sum_amount_goal_list.append(amount_goal)
    sum_order_goal_list.append(order_goal)


# In[23]:


sum_amount_order_goal=pd.concat([sum_amount_order,pd.DataFrame({'sum_amount_goal':sum_amount_goal_list,'sum_order_goal':
                                                               sum_order_goal_list})],axis=1)
sum_amount_order_goal


# ### (3)date_info(日期数据)

# In[45]:


date_sql = """ 
      select create_date,
              is_current_year,
              is_last_year,
              is_yesterday,
              is_today,
              is_current_month,
              is_current_quarter
              from dim_date_df
              """

date_info = pd.read_sql_query(date_sql, con=adventure_conn_tosql)


# In[46]:


date_info


# ### （4）融合上面的数据

# In[52]:



"""

输入：
sum_amount_order_goal销量订单聚合目标表,
date_info日期维度表

输出：
amount_order_by_day销量订单聚合目标及日期维度表
"""


# In[49]:


#查看create_date
sum_amount_order_goal['create_date'].iloc[1]


# In[51]:


#转化create_date格式为标准日期格式
sum_amount_order_goal['create_date']=sum_amount_order_goal['create_date'].apply(lambda x:x.strftime('%Y-%m-%d'))

#通过主键create_date连接日期维度
amount_order_by_day=pd.merge(sum_amount_order_goal,date_info,
                            on='create_date',how='inner')


# In[53]:


amount_order_by_day


# ### （5）订单数据进行存储

# In[64]:


"""
待存储表：amount_order_by_day
欲存储engine:adventure_conn_tosql
"""
#将amount_order_by_day数据追加到数据库dw_order_by_day(每日环比表)当中
#先计算环比，因为日期已经是升序排列了，所以直接利用pct_change()即可
 # pct_change()表示当前元素与先前元素的相差百分比，默认竖向，例：前面元素x，当前元素y，公式 result = (y-x)/x
amount_order_by_day['amount_diff']=amount_order_by_day['sum_amount'].pct_change().fillna(0)

amount_order_by_day.to_sql('dw_order_by_day_zhaoyqiu', con=adventure_conn_tosql,
                                   if_exists='append', index=False)                                      # 追加数据至dw_order_by_day


# In[70]:


amount_order_by_day.to_excel(os.path.join(output_path,'amount_order_by_day.xlsx'),index=False)


# In[ ]:




