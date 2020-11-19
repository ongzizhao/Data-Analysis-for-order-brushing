import pandas as pd
import numpy as np

data=pd.read_csv("Copy of Copy of order_brush_order.csv")

shop_id=data["shopid"].unique().tolist()

data_by_shop=[data[data["shopid"]==id] for id in shop_id]

#create a dic with key = shop id, value = list of suspicious userid, initialise with empty list as value
suspicious={}
for id in shop_id:
    suspicious[id]=[]

for shop in data_by_shop:
    shop["event_time"]=pd.to_datetime(shop["event_time"])
    shop["mth"]=shop["event_time"].apply(lambda x:x.month)
    shop["day"]=shop["event_time"].apply(lambda x:x.day)
    shop["hour"]=shop["event_time"].apply(lambda x:x.hour)
    brushing=[]
    ##create a list of days
    days=shop["day"].unique().tolist()
    for day in days:
        day_df=shop[shop["day"]==day]
        hourly=day_df["hour"].unique().tolist()
        for hour in hourly:
            #find the numbers of order in a specific hour in a specific day
            hour_df=day_df[day_df["hour"]==hour]
            num_orders=len( hour_df["userid"])
            num_unique_buyers=  hour_df["userid"].nunique()
            unique_buyers=  hour_df["userid"].unique()
            concentration_rate = num_orders/num_unique_buyers
            if concentration_rate >=3:
                for buyer in unique_buyers:
                    # buys per unique buyer
                    buys= len(hour_df[hour_df["userid"]==buyer])
                    if buys>=3:
                        suspicious[hour_df["shopid"].unique()[0]].append(buyer)

key_list=list(suspicious.keys())

for k in key_list:
    suspicious[k].sort()
    # remove duplicates in the list if same buyer is caught order brushing in multiple time periods
    suspicious[k]=list(set(suspicious[k]))
    if len(suspicious[k])==0:
        suspicious[k].append(0)

submission=pd.DataFrame(data["shopid"].unique(),columns=["shopid"])
submission["userid"]=submission["shopid"].apply(lambda x:suspicious[x][0])
submission.to_csv("submission.csv",index=False)