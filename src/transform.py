import pandas as pd #type:ignore
from datetime import datetime

def customer_transform(dataframe):
    dataframe=dataframe.drop_duplicates()
    dataframe['registration_date']=pd.to_datetime(dataframe['registration_date'],format='mixed',errors='coerce')
    dataframe['email_flag']=dataframe['email'].astype(str).str.match(r'^[a-zA-Z0-9_.]+@[a-z]@\.[a-z]{2,}$',na=False)
    dataframe['email']=dataframe.apply(lambda row:f"{row['name'].lower().strip().replace(' ','_')}@email.com" if pd.isnull(row['email']) or row['email_flag']==False else row['email'], axis=1)
    dataframe.drop(columns=['email_flag'],inplace=True)
    dataframe['phone']=dataframe['phone'].astype(str).str.replace(r'()a-z','',regex=True)
    dataframe['phone_flag']=dataframe['phone'].astype(str).str.match(r'^\d{3}-\d{3}-\d{4}$',na=False)
    dataframe['phone']=dataframe.apply(lambda row:'000-000-0000' if pd.isnull(row['phone']) or row['phone_flag']==False else row['phone'],axis=1)
    dataframe.drop(columns=['phone_flag'],inplace=True)
    dataframe['name']=dataframe['name'].astype(str).str.title()
    return dataframe

def SCDType2(dataframe1,dataframe2):
    # add scd type 2 columns to existing data
    dataframe1['start_date']=dataframe1['registration_date']
    dataframe1['end_date']=None
    dataframe1['is_current']=True
    
    # identify the changes
    # we are identifying old customers whose changes are to be made
    dataframe_new=dataframe2[dataframe2['customer_id'].isin(dataframe1['customer_id'])]
    
    # close old records
    today=pd.to_datetime(datetime.today().date())
    dataframe1.loc[dataframe1['customer_id'].isin(dataframe_new['customer_id']),'end_date']=today
    dataframe1.loc[dataframe1['customer_id'].isin(dataframe_new['customer_id']),'is_current']=False

    # add scd type 2 columns to changes
    dataframe_new['start_date']=today
    dataframe_new['end_date']=None
    dataframe_new['is_current']=True

    # now add the new data to existing data and store in new dataframe named scdtype2
    df_type=pd.concat([dataframe1,dataframe_new],ignore_index=True)


    #identify the new entries and create a dataframe
    df_new_entries=dataframe2[~dataframe2['customer_id'].isin(dataframe1['customer_id'])]
    df_new_entries['start_date']=today
    df_new_entries['end_date']=None
    df_new_entries['is_current']=True

    df_type2=pd.concat([df_type,df_new_entries],ignore_index=True)


    return df_type2




