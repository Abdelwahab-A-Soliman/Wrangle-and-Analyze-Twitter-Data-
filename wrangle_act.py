#!/usr/bin/env python
# coding: utf-8

# # **Data Wrangling and Analyzing Twitter Data Project**

# ## 1. Import Libraries

# In[7]:


import pandas as pd
import numpy as np
import requests 
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
import tweepy
import os
from tweepy import OAuthHandler
import json
from timeit import default_timer as timer


# ## 2. Gather Data

# ### 2.1 Gather Downloaded Data

# In[8]:


# After the data had been downloaded from the provided link its stored in a dataframe
twitter_download = pd.read_csv('twitter-archive-enhanced.csv')


# ### 2.2 Gather Data from URL 

# In[9]:


# Second dataframe is downloaded programmatically using the url provided 
url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
file_name = url.split('/')[-1]
response = requests.get(url)
if not os.path.isfile(file_name):
    with open(file_name,'wb') as file :
        file.write(response.content)
        
image_data = pd.read_csv(file_name ,sep= '\t')


# ### 2.3 Gather Data from API

# In[10]:


consumer_key = 'FjNbxqkxgIE5wEojbVbRBim33'
consumer_secret = 'un34601fIilFd4wrNwGgzqZaucHNik5FhvFDGJScVlHuNGAH92'
access_token = '1348987927188156417-LJSjm2c2LWESNNnO2PAzMUF2aeeZmz'
access_secret = '0oyxoZOz2loGam5TRr0FYgga4aJKQoI9eX3kz9lrWkelx'
# To access API and use token 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# In[5]:


# Test the api
exp_tweet = api.get_status(twitter_download.tweet_id[1000], tweet_mode = 'extended')
content = exp_tweet._json
content


# In[7]:


# Loop over all tweet_ids and extract data for each one
file_name_json = 'tweet_json.txt'
error = []
if not os.path.isfile(file_name_json):
    with open(file_name_json,'w') as file_json :
        for id_ in twitter_download['tweet_id']:
            try:
                status = api.get_status(id_ , tweet_mode = 'extended')
                content = status._json
                json.dump(content,file_json)
                file_json.write('\n')
            except:
                print('Error found for id {}'.format(id_))
                error.append(id_)
            
            
            
          
            


# In[12]:


# Navigate through tweet_json.txt and save it to a dictionary 
json_dic = []
with open(file_name_json,'r') as file_json2 :
    for line in file_json2:
        tweet = json.loads(line)
        json_dic.append({'tweet_id' : tweet['id']
                     ,'full_text': tweet['full_text']
                     ,'favourite_count' :tweet['favorite_count']
                     ,'retweet_count': tweet['retweet_count']
                     })

# Transform the dictionary into a data frame
json_df = pd.DataFrame(json_dic)


# ## 3. Assess

# ### 3.1 Visual Assessment

# In[104]:


twitter_download.head()


# In[110]:


json_df.head()


# In[109]:


image_data.head()


# ### 3.2 Programmatic Assessment

# In[111]:


twitter_download.info()


# In[121]:


# Checking timestamp datatype
type(twitter_download['timestamp'][0])


# In[122]:


twitter_download.describe()


# In[124]:


twitter_download['rating_numerator'].sort_values()


# In[51]:


twitter_download['tweet_id'].duplicated().sum()


# In[ ]:


json_df.head()


# In[128]:


twitter_download[twitter_download['rating_denominator'] < 10]


# In[129]:


twitter_download[twitter_download['rating_denominator'] > 10]


# In[130]:


image_data.info()


# In[132]:


image_data.p1.value_counts()


# In[133]:


json_df.describe()


# In[134]:


json_df.info()


# ### 3.3 Data Quality Issues

# #### Twitter_Download

# 1. Unneeded columns ('retweeted_status_id','retweeted_status_user_id','in_reply_to_status_id','in_reply_to_user_id','source','retweeted_status_timestamp') 
# 2. timestamp data type should be date and found string (Validity)
# 3. rating_denominator data entered wrongly (Accuracy)
# 4. rating_numerator data entered wrongly (Accuracy)
# 5. Some records are retweets (Accuracy)
# 6. Inconsistent format in name column
# 7. Some names are wrongly extracted like (A , The ) (Accuracy)

# #### Image_data

# 1. Some data doesnt have images rows should be 2356 found 2075 (Misssing Data)
# 2. Some images arent for dogs (Accuracy)
# 3. Inconsistnet format for P1,P2,P3 should all be lower or upper case (3 quality issues)
# 4. Non descriptive column names

# #### Json_df

# 1.Some data are missing should be 2356 found 2331

# ### 3.4 Tidiness Issues

# #### Twitter_Download

# 1. Dogga,floofer,pupper,puppo should be in one coloumn called type

# #### Image_data

# 1. Predictions (Only dog photos ) should be in one column for better analysis

# #### Json_df

# 1. Json_df and twitter_download should be one data frame
# 

# ## 4. Clean

# In[243]:


# Before cleaning a copy of the data frames are created
json_clean = json_df.copy()
image_clean = image_data.copy()
download_clean = twitter_download.copy()


# ### 4.1 Dealing with Quality Issues

# #### 4.1.1 Define 

# 1. Joining image and download to find records with missing images and delete them
# 2. Remove Retweets and replies by droping those records
# 3. Remove unneeded columns mentioned above
# 4. Change timestamp to date datatype
# 5. Drop all tweets that doesnt include dogs 
# 6. Correct wrong rating_denominator values
# 7. Correct wrong rating_numerator values
# 8. Change non_descriptive image_clean column names 
# 9. Standardize the Prediction columns to be all capitalized
# 10. Standardize the Name column to be all capitalized
# 11. Correct Wrong names 'a' and 'the'

# #### 4.1.2 Code

# In[244]:


merged_df = pd.merge(download_clean,image_clean, on = ['tweet_id'], how='left')


# In[245]:


download_clean = merged_df


# In[246]:


# drop values with missing images
download_clean = download_clean.drop(download_clean[download_clean.jpg_url.isnull()].index)


# In[247]:


# drop retweets 
download_clean = download_clean[download_clean.retweeted_status_id.isnull()]


# In[248]:


# drop replies
download_clean = download_clean[download_clean.in_reply_to_status_id.isnull()]


# In[249]:


# drop unneeded columns
download_clean = download_clean.drop(columns =['retweeted_status_id','retweeted_status_user_id','in_reply_to_status_id','in_reply_to_user_id','source','retweeted_status_timestamp' ],axis=1 )


# In[250]:


# change timestamp column 
download_clean.timestamp = pd.to_datetime(download_clean.timestamp)


# In[251]:


# Remove records that arent dog images
download_clean = download_clean.drop(download_clean[(download_clean.p1_dog ==False) & (download_clean.p2_dog == False) & (download_clean.p3_dog == False)].index)


# In[252]:


# seprate again download_clean and image_clean
download_clean = download_clean.drop(columns = ['jpg_url','img_num','p1','p1_conf','p1_dog','p2','p2_conf','p2_dog','p3','p3_conf','p3_dog'])


# In[253]:


# Correct wrong rating_denominator "<10"
# First check the records after the previous droping process
download_clean[download_clean.rating_denominator <10]
# The first row doesnt have rating and the second should be 9 /10 after comparing it to the text


# In[254]:


# Replace values
download_clean.loc[516,'rating_numerator'] = 0
download_clean.loc[516,'rating_denominator'] = 0
download_clean.loc[2335,'rating_numerator'] = 9
download_clean.loc[2335,'rating_denominator'] = 10


# In[255]:


# Correct wrong rating_denominator "<10"
# Check the records after the previous droping process
download_clean[download_clean.rating_denominator > 10]


# 1. If the rating_denominator values are divisible by 10 then divide them with the first 2 digits (Because the photos contain more than one dog)
# 2. If the rating_denominator values are not divisible by 10  replace it with correct values

# In[256]:


# Correct rating_denominator values are not divisible by 10 after reviewing the text
download_clean.loc[1662,'rating_numerator'] = 10
download_clean.loc[1662,'rating_denominator'] = 10
download_clean.loc[1068,'rating_numerator'] = 14
download_clean.loc[1068,'rating_denominator'] = 10


# In[257]:


for ind in download_clean[download_clean.rating_denominator > 10].index:
     division = download_clean['rating_denominator'][ind].astype(str)
     division = int(division[:-1])
     download_clean['rating_denominator'][ind] /= division
     download_clean['rating_numerator'][ind] /= division


# In[258]:


# Fix rating_numerator 
# Checking the extreme values of rating_numerator ">14"
download_clean[download_clean.rating_numerator >14]


# In[259]:


# Fix the extreme values of rating_numerator ">14"
download_clean.loc[695,'rating_numerator'] = 9.75
download_clean.loc[763,'rating_numerator'] = 11.27
download_clean.loc[1712,'rating_numerator'] = 11.26


# In[260]:


# Change imag_clean non descriptive column names
image_clean.columns = ['tweet_id','jpg_url',
 'img_num','first_prediction','first_prediction_confidence','first_dog_photo'
    ,'second_prediction','second_prediction_confidence','second_dog_photo'
    ,'third_prediction','third_prediction_confidence','third_dog_photo']


# In[261]:


# Change all First,Second and third predictions to be lower case except first letter
image_clean['first_prediction'] = image_clean['first_prediction'].str.title()


# In[262]:


# Change name in download_clean to be all capitalized
download_clean['name'] =  download_clean['name'] .str.title()


# In[263]:


# Check all names and change the mistakes with highest rate because it will affect the analysis 
download_clean['name'].value_counts()[download_clean['name'].value_counts() > 1]


# In[264]:


# Remove 'a' and 'the' because the names arent mentioned in the tweets
download_clean['name'] = download_clean['name'].str.replace(r'A$','None')
download_clean['name'][download_clean['name'] == 'The' ] = 'None'


# #### 4.1.3 Test

# In[265]:


download_clean.info()


# In[266]:


download_clean.head()


# In[267]:


# Checking name column
download_clean['name'][download_clean['name'] == 'The' ]


# In[268]:


# Checking rating_numerator  
download_clean[download_clean.rating_numerator >14]


# In[269]:


# Cheking rating_denominator 
download_clean[download_clean.rating_denominator  > 10]


# In[270]:


# Checking Image_Clean
image_clean.head()


# ### 4.2 Dealing with Tidiness Issues

# #### 4.2.1 Define

# 1. Merge Dogga,floofer,pupper,puppo should be in one coloumn called type
# 2. Merge Predictions to be in one column called breed 
# 3. Merge both json_clean and download_clean in one dataframe
# 

# #### 4.2.2 Code

# In[271]:


# Merge Dogga,floofer,pupper,puppo should be in one coloumn called type
concat = download_clean.copy()
concat = concat.replace('None',"")
concat['type'] = concat['doggo'] + concat['floofer'] + concat['pupper'] +concat['puppo'] 
unneed_columns = ['doggo','floofer','pupper','puppo']
concat = concat.drop(unneed_columns,axis=1)
download_clean= concat


# In[272]:


# Create breed column according to predictions 
image_clean['breed'] = ""
for ind in image_clean.index:
    if image_clean['first_dog_photo'][ind] == True:
        image_clean['breed'][ind] = image_clean['first_prediction'][ind]
    elif image_clean['second_dog_photo'][ind] == True:
        image_clean['breed'][ind] = image_clean['second_prediction'][ind]
    else:
        image_clean['breed'][ind] = image_clean['third_prediction'][ind]

        


# In[273]:


# Merge json clean and download_clean in one data frame
df_clean = pd.merge(download_clean,json_clean, on = ['tweet_id'] ,how = 'left')
df_clean = df_clean.drop('full_text',axis=1)


# #### 4.2.3 Test

# In[274]:


df_clean.head()


# In[275]:


image_clean.head()


# ## 5. Final Assessment

# ### 5.1 Assess

# In[276]:


df_clean.head()


# In[277]:


image_clean.head()


# ### 5.2 Other Issues found

# 
# 1. images that arent dogs are still in image_clean df
# 2. Inconsistent breed format should be all capitalized

# ### 5.3 Clean

# In[278]:


# Remove All Triple false predictions 
image_clean = image_clean[~((image_clean['first_dog_photo'] == False) & (image_clean['second_dog_photo'] == False) & (image_clean['third_dog_photo'] == False))]
# Remove unneeded columns
columns_needed = ['tweet_id','jpg_url','img_num','breed']
image_clean = image_clean[columns_needed]


# In[279]:


# Capitalize breed format in image_clean
image_clean['breed'] = image_clean['breed'].str.title()


# ### 5.4 Test

# In[280]:


image_clean.head()


# ## 6. Storing Data

# In[281]:


file_name = 'twitter_archive_master.csv'
df_clean.to_csv(file_name)
    


# In[285]:


file_name_2 = 'twitter_image_cleaned.csv'
image_clean.to_csv(file_name_2)
    


# ## 7.Visualization

# In[286]:


# Create dataframe joining both data frames
data_visual = pd.merge(df_clean,image_clean,on=['tweet_id'],how='left')


# In[287]:


data_visual.head()


# In[306]:


visual_1 = data_visual.groupby('breed')['favourite_count'].mean()
visual_1.nlargest(10)


# In[323]:


chart1 = visual_1.nlargest(10).plot(kind = 'barh',title = 'Top 10 Breeds vs Favourite counts' ,color ='green',alpha=.5)
chart1.set_ylabel("")


# In[324]:


chart2 = visual_1.nsmallest(10).plot(kind = 'barh',title = 'Lowest 10 Breeds vs Favourite counts' ,color ='red',alpha=.5)
chart2.set_ylabel("")


# In[325]:


visual_2 = data_visual.groupby('breed')['retweet_count'].mean()
visual_2.nlargest(10)


# In[327]:


chart3 = visual_2.nlargest(10).plot(kind = 'barh',title = 'Top 10 Breeds vs Retweet counts' ,color ='green',alpha=.5)
chart3.set_ylabel("")


# In[329]:


chart3 = visual_2.nsmallest(10).plot(kind = 'barh',title = 'Lowest 10 Breeds vs Retweet counts' ,color ='red',alpha=.5)
chart3.set_ylabel("")


# In[358]:


visual_3 = data_visual[['breed','rating_numerator']]


# In[359]:


visual_3.breed.value_counts().nlargest(10).plot(kind='barh')


# In[381]:


visual_4 = data_visual[['type','rating_numerator']]
visual_4_pivot = visual_4[visual_4['type'].isin(['doggo','puppo','pupper','floofer'])].pivot(columns ='type',values='rating_numerator')


# In[383]:


visual_4_pivot.plot(kind='box')


# In[ ]:




