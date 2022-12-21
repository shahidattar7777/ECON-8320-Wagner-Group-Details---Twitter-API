import tweepy
import spacy
from spacy import displacy
import en_core_web_sm
import pandas as pd
import numpy as np


#Please use the Bearer token from your Twitter Developer Page to Access the Database
bearertoken=""
client = tweepy.Client(bearertoken)



tweets = []
nlp = en_core_web_sm.load()

#Extraction of ID which would serve as a primary key for our DataFrame.
#This can serve as an important parameter if a particular tweet requires some more probing
def extract_id(a):
    return a['id']


#Extracting the url of the tweet so that more details like videos, pictures could be explored
def extract_url(a):
    url=""
    try:
        url = a['entities']['urls'][0]['expanded_url']
    except:
        url = "NA"
    return url

#Extraction of Geopolitical entities mentioned in the tweet
def extract_location(a):
    b = nlp(a)
    return [ent.text for ent in b.ents if ent.label_ == 'GPE']

#Extraction of names from the tweets
def extract_names(a):
    b=nlp(a)
    return [ent.text for ent in b.ents if ent.label_ == 'PERSON']

#Extraction of Dates
def extract_date(a):
    b= nlp(a)
    return [ent.text for ent in b.ents if ent.label_ =='DATE']

#Extraction of time
def extract_time(a):
    b=nlp(a)
    return [ent.text for ent in b.ents if ent.label_ == 'TIME']

#Extraction of Text of the tweets
def extract_text(a):
    text = ''
    try:
        text = a['text']
    except:
        text = 'NA'

    return text

#Extraction of Activities related to the Wagner Group. We are extracting the verbs in the sentences and getting their noun forms using lemma_

def extract_activity(a):
    b=nlp(a)
    act= []
    for token in b:
        if token.pos_ == 'VERB':
            act.append(token.lemma_)
    return act

#Creating a list of Keys for Our Dictionary and the Dictionary as well
keys= ['id','text','activity', 'names','location','date','time','url']
dict1 = dict.fromkeys(keys)

#Create Multiple Lists which serve as Columns for our Dataframe
Id=[]
text=[]
act=[]
name=[]
location =[]
date=[]
time=[]
url=[]

#Querying 'wagner group' and using '-is:retweet' to exclude retweets along with Pagination to move through multiple pages
#And limiting our results to 1000
for tweet in tweepy.Paginator(client.search_recent_tweets,query= 'wagner group -is:retweet',
                                max_results=100, tweet_fields= ['entities','geo','id','referenced_tweets','created_at'],
                              place_fields=['contained_within','country','country_code','full_name','geo','id','name','place_type'],
                              expansions =['referenced_tweets.id','geo.place_id']).flatten(limit=1000):   

#Using our defined methods above to extract and append the information to the lists
    #ID
    Id.append(extract_id(tweet))
    #Text
    text1 = extract_text(tweet)
    text.append(text1)
    #Activity
    act.append(str(extract_activity(text1)).replace("'","")[1:-1])
    #Names
    name.append(str(extract_names(text1)).replace("'","")[1:-1])
    #Location
    location.append(str(extract_location(text1)).replace("'","")[1:-1])
    #Date
    date.append(str(extract_date(text1)).replace("'","")[1:-1])
    #Time
    time.append(str(extract_time(text1)).replace("'","")[1:-1])
    #Url
    url.append(extract_url(tweet)) 

dict1['id'] = Id
dict1['text']= text
dict1['activity']= act
dict1['names']=name
dict1['location']=location
dict1['date'] = date
dict1['time'] = time
dict1['url'] = url

#Creating a DataFrame from our dictionary
df = pd.DataFrame.from_dict(dict1)


print(df)

#Exporting the Dataframe to a CSV file
df.to_csv('df.csv')











