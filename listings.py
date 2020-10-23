# -*- coding: utf-8 -*-
"""
Created on 20l20-10-22

Authhor: NGC-6543
"""

import pandas as pd
import datetime # convert dates to timespan since 2016-01-04 (scrape date)
import math # check for NaN's 
import os

os.getcwd()
# os.chdir('./')
# os.getcwd()


###############################################################################
## Listings file data cleaning 

## Import listings.csv
listings_import = pd.read_csv('./source_data/listings.csv')

## keep only the desired fields
listingsDF = pd.DataFrame(listings_import, columns=[
'id'
,'host_id'
,'host_since'
,'host_location'
,'host_response_time'
,'host_response_rate'
,'host_is_superhost'
,'host_neighbourhood'
,'host_has_profile_pic'
,'host_identity_verified'
,'neighbourhood'
,'neighbourhood_group_cleansed'
,'zipcode'
,'latitude'
,'longitude'
,'property_type'
,'room_type'
,'accommodates'
,'bathrooms'
,'bedrooms'
,'beds'
,'bed_type'
,'price'
,'weekly_price'
,'monthly_price'
,'security_deposit'
,'cleaning_fee'
,'guests_included'
,'extra_people'
,'minimum_nights'
,'maximum_nights'
,'calendar_updated'
,'availability_30'
,'availability_60'
,'availability_90'
,'availability_365'
,'number_of_reviews'
,'first_review'
,'last_review'
,'review_scores_rating'
,'review_scores_accuracy'
,'review_scores_cleanliness'
,'review_scores_checkin'
,'review_scores_communication'
,'review_scores_location'
,'review_scores_value'
,'instant_bookable'
,'cancellation_policy'
,'require_guest_profile_picture'
,'require_guest_phone_verification'
,'calculated_host_listings_count'
,'reviews_per_month'
])

# drop the unused DF
del listings_import

## remove records that were dropped in the calendar file due to incorrect entry
listingsDF = listingsDF.loc[ ~listingsDF['id'].isin(['3308979','2715623','7733192','2459519','4825073']) ]

## drop these two records because they have mostly null values
listingsDF = listingsDF.loc[ ~listingsDF['id'].isin(['8354452','10235014']) ]

# replace bad zipcode containing newline character with corrected zipcode
listingsDF.loc[listingsDF['id'] == 9448215,'zipcode'] = '98122'


###############################################################################
## Listings file data transformation

## replace all 't/f' columns with 1/0

#listingsDF.info()

## check if 't' then replace with 1 else 0
#--- Function def
def item_replace(xstr):
    
    if xstr == 't':
        x = 1
    else:
        x = 0
        
        
    return x
#---
listingsDF['host_is_superhost'] = listingsDF['host_is_superhost'].map(item_replace)
listingsDF['host_has_profile_pic'] = listingsDF['host_has_profile_pic'].map(item_replace)
listingsDF['host_identity_verified'] = listingsDF['host_identity_verified'].map(item_replace)
listingsDF['instant_bookable'] = listingsDF['instant_bookable'].map(item_replace)
listingsDF['require_guest_profile_picture'] = listingsDF['require_guest_profile_picture'].map(item_replace)
listingsDF['require_guest_phone_verification'] = listingsDF['require_guest_phone_verification'].map(item_replace)


## Update dates to time intervals in days by 
## determining the time elapsed since 2016-01-04 (scrape date)
## ignore empty values (nan's)
#--- Function def
def date_replace(xstr):
    if type(xstr)!=float:
        xstr = int(  (datetime.datetime(2016,1,4) - datetime.datetime.strptime(xstr, "%Y-%m-%d")).days  )
    return xstr
#---
listingsDF['host_since'] = listingsDF['host_since'].map(date_replace)
listingsDF['first_review'] = listingsDF['first_review'].map(date_replace)
listingsDF['last_review'] = listingsDF['last_review'].map(date_replace)


# check to make sure pandas functions ignore missing values
#listingsDF.to_csv('./listingsDF_check.csv', index=False)
#listingsDF['first_review'].mean() # mean ignores NaNs.
#listingsDF['first_review'].count() # even count ignores NaNs.


## check if host_location is in seattle, ignore nan's
#--- Function def
def test_str(xstr):
    if type(xstr)!=float:
        if 'seattle' in xstr.lower():
            xstr = 1
        else:
            xstr = 0
    return xstr
#---
listingsDF['host_location'] = listingsDF['host_location'].map(test_str)

## check if host neighbourhood matches neighbourhood, if yes then 1 else 0 (nan's in either or both will be false)
listingsDF.loc[listingsDF['host_neighbourhood'] == listingsDF['neighbourhood'],'host_neighbourhood'] = 1
listingsDF.loc[listingsDF['host_neighbourhood'] != 1,'host_neighbourhood'] = 0

## drop neighbourhood column since it is not needed anymore
del listingsDF['neighbourhood']

## check if bed_type matches 'Real Bed', if yes then 1 else 0
listingsDF.loc[listingsDF['bed_type'] == 'Real Bed','bed_type'] = 1
listingsDF.loc[listingsDF['bed_type'] != 1,'bed_type'] = 0

## check property type matches, condense to 3 possible choices
listingsDF.loc[ (listingsDF['property_type'] == 'House') | (listingsDF['property_type'] == 'Townhouse')  ,'property_type'] = 'House'
listingsDF.loc[ (listingsDF['property_type'] == 'Apartment') | (listingsDF['property_type'] == 'Condominium')  ,'property_type'] = 'Apartment'
listingsDF.loc[ (listingsDF['property_type'] != 'House') & (listingsDF['property_type'] != 'Apartment')  ,'property_type'] = 'Other'

## check when calendar last updated, condense to two possible choices
listingsDF.loc[ (listingsDF['calendar_updated'] == 'today')
               | (listingsDF['calendar_updated'] == 'yesterday')
               | (listingsDF['calendar_updated'] == '2 days ago')
               | (listingsDF['calendar_updated'] == '3 days ago')
               | (listingsDF['calendar_updated'] == '4 days ago')
               | (listingsDF['calendar_updated'] == '5 days ago')
               | (listingsDF['calendar_updated'] == '6 days ago')
               , 'calendar_updated'] = 1
listingsDF.loc[ listingsDF['calendar_updated'] != 1   , 'calendar_updated'] = 0          

## if host_response_time is 'N/A' replace with 'unknown' 
listingsDF.loc[ (listingsDF['host_response_time'] == 'N/A') , 'host_response_time'] =  'unknown'

## replace string currency with float values
#--- Function def
def replace_currency(xstr):
    if type(xstr)!=float:
        xstr = str.replace(xstr,'$','')
        xstr = str.replace(xstr,',','')
        xstr = float(xstr)
    return xstr
#---
listingsDF['price'] = listingsDF['price'].map(replace_currency)
listingsDF['weekly_price'] = listingsDF['weekly_price'].map(replace_currency)
listingsDF['monthly_price'] = listingsDF['monthly_price'].map(replace_currency)
listingsDF['security_deposit'] = listingsDF['security_deposit'].map(replace_currency)
listingsDF['cleaning_fee'] = listingsDF['cleaning_fee'].map(replace_currency)
listingsDF['extra_people'] = listingsDF['extra_people'].map(replace_currency)

## replace string percentages with float values
#--- Function def
def replace_pct(xstr):
    if type(xstr)!=float:
        xstr = str.replace(xstr,'%','')
        xstr = float(xstr)
        xstr = xstr * .01
    return xstr
#---
listingsDF['host_response_rate'] = listingsDF['host_response_rate'].map(replace_pct)    


## replace with missing values in host_response_rate bedrooms bathrooms and beds with mean
def replaceNaN(mean, value):
    
    if math.isnan(value):
        value = mean
        
    return value
#---
    
listingsDF['host_response_rate'] = listingsDF['host_response_rate'].apply(lambda x: replaceNaN(listingsDF['host_response_rate'].mean(),x))
listingsDF['bathrooms'] = listingsDF['bathrooms'].apply(lambda x: replaceNaN(round(listingsDF['bathrooms'].mean(),2),x))
listingsDF['bedrooms'] = listingsDF['bedrooms'].apply(lambda x: replaceNaN(round(listingsDF['bedrooms'].mean(),2),x))
listingsDF['beds'] = listingsDF['beds'].apply(lambda x: replaceNaN(round(listingsDF['beds'].mean(),2),x))

## replace missing zipcodes with the most common zipcode for that neighborhood
listingsDF.loc[ (listingsDF['zipcode'] == '') & (listingsDF['neighbourhood_group_cleansed'] == 'Queen Anne')  ,'zipcode'] = '98109'
listingsDF.loc[ (listingsDF['zipcode'] == '') & (listingsDF['neighbourhood_group_cleansed'] == 'Ballard')  ,'zipcode'] = '98107'
listingsDF.loc[ (listingsDF['zipcode'] == '') & (listingsDF['neighbourhood_group_cleansed'] == 'Interbay')  ,'zipcode'] = '98119'
listingsDF.loc[ (listingsDF['zipcode'] == '') & (listingsDF['neighbourhood_group_cleansed'] == 'Capitol Hill')  ,'zipcode'] = '98102'
listingsDF.loc[ (listingsDF['zipcode'] == '') & (listingsDF['neighbourhood_group_cleansed'] == 'Central Area')  ,'zipcode'] = '98122'
listingsDF.loc[ (listingsDF['zipcode'] == '') & (listingsDF['neighbourhood_group_cleansed'] == 'Downtown')  ,'zipcode'] = '98101'


###############################################################################
## Calendar file data cleaning

## Import calendar.csv
calendar_import = pd.read_csv('./source_data/calendar.csv')


## remove records that were coded incorrectly
calendar_import = calendar_import.loc[ ~calendar_import['listing_id'].isin(['3308979','2715623','7733192','2459519','4825073']) ]

# remove any rows in cal_sum that have the following listing ids (based on analysis of listings file)
calendar_import = calendar_import.loc[ ~calendar_import['listing_id'].isin(['8354452','10235014']) ]

## check if 't' then replace with 1 else 0
#--- Function def
def item_replace(xstr):
    
    if xstr == 't':
        x = 1
    else:
        x = 0
        
    return x
#---
calendar_import['available'] = calendar_import['available'].map(item_replace)

#calendar_import.info()

## get the sum of available days for each listing for the year and put in new dataframe
df1 = calendar_import.groupby('listing_id')['available'].sum()

## use replace currency function (above) to replace string values with float
calendar_import['price'] = calendar_import['price'].map(replace_currency)  

## get the mean of price for each listing for the year and put in new dataframe
df2 = calendar_import.groupby('listing_id')['price'].mean()   

## round the mean price to two decimals
#--- Function def
def round_currency(xstr):
    xstr = round(xstr,2)
    return xstr
#---
df2 = df2.map(round_currency)

## merge the two summary dataframes
df1 = df1.reset_index()
df2 = df2.reset_index()
calendarDF = pd.merge(df1,
    df2,
    how='inner',
    on='listing_id')

calendarDF = calendarDF.rename(
    columns={"listing_id":"id", "price":"price_avg","available":"avail"})
    
del df1,df2,calendar_import

## merge with calendar file (must have run calendar file first)

listingsDF = pd.merge(listingsDF,
    calendarDF,
    how='inner',
    on='id')

del calendarDF

###############################################################################
## Create two fields with bins for categorizing availability and avg_price


listingsDF['AvailCat'] = 0
listingsDF.loc[ (listingsDF['avail'] >= 0) & (listingsDF['avail'] <=124), 'AvailCat' ] = 1
listingsDF.loc[ (listingsDF['avail'] >= 125) & (listingsDF['avail'] <=308), 'AvailCat' ] = 2
listingsDF.loc[ (listingsDF['avail'] >= 309) & (listingsDF['avail'] <=360), 'AvailCat' ] = 3
listingsDF.loc[ (listingsDF['avail'] >= 361) & (listingsDF['avail'] <=365), 'AvailCat' ] = 4

listingsDF['PriceCat'] = 0
listingsDF.loc[ (listingsDF['price_avg'] >= 20) & (listingsDF['price_avg'] <=76), 'PriceCat' ] = 1
listingsDF.loc[ (listingsDF['price_avg'] >= 76.06) & (listingsDF['price_avg'] <=109), 'PriceCat' ] = 2
listingsDF.loc[ (listingsDF['price_avg'] >= 109.29) & (listingsDF['price_avg'] <=163.14), 'PriceCat' ] = 3
listingsDF.loc[ (listingsDF['price_avg'] >= 163.25) & (listingsDF['price_avg'] <=1071), 'PriceCat' ] = 4

###########################################################################
## remove 'other'-coded neighbourhoods and drop all rows with empty values

listingsDF = listingsDF.loc[ listingsDF['neighbourhood_group_cleansed'] != 'Other neighborhoods' ]


###############################################################################
## Create summary data tables and visualizations

#import matplotlib.rcsetup as rcsetup
#print(rcsetup.all_backends) # looking into rendering issues
#import matplotlib.pyplot as plt; plt.rcdefaults()

import numpy as np
import matplotlib.pyplot as plt


## Count and availability of properties

# get tables
nb_count = listingsDF.groupby('neighbourhood_group_cleansed')['zipcode'].count()
nb_avail = listingsDF.groupby('neighbourhood_group_cleansed')['avail'].mean()

# convert index to column
nb_count = nb_count.reset_index()
nb_avail = nb_avail.reset_index()

# sorting can be done by value:
nb_count = nb_count.sort_values(by='zipcode', ascending=False)
nb_avail = nb_avail.sort_values(by='avail', ascending=False)

# round decimals on available:
nb_avail['avail'] = round(nb_avail['avail'],1)

# rename columns
nb_count = nb_count.rename(columns={"neighbourhood_group_cleansed":"neighborhood","zipcode":"count"})
nb_avail = nb_avail.rename(columns={"neighbourhood_group_cleansed":"neighborhood","avail":"avg days avail"})

# write out csv
#nb_count.to_csv('nb_count.csv', columns=['neighborhood', 'count'], sep=',', index=False)
#nb_avail.to_csv('nb_avail.csv', columns=['neighborhood', 'avg days avail'], sep=',', index=False)

# create visual: nb_avail
objects = tuple(nb_avail['neighborhood'])
y_pos = np.arange(len(objects))
plt.bar(y_pos, list(nb_avail['avg days avail']), align='center', alpha=0.5)
plt.xticks(y_pos, objects, rotation=90)
plt.ylabel('avg days available')
plt.title('Availability by Neighborhood')
plt.tight_layout()
fig1 = plt.gcf()
#plt.savefig('test2')
plt.show()
plt.draw()
fig1.savefig('./images/nb_avail.png')


# create visual: nb_count
objects = tuple(nb_count['neighborhood'])
y_pos = np.arange(len(objects))
plt.bar(y_pos, list(nb_count['count']), align='center', alpha=0.5)
plt.xticks(y_pos, objects, rotation=90)
plt.ylabel('Count')
plt.title('Listings by Neighborhood')
plt.tight_layout()

fig1 = plt.gcf()
plt.show()
plt.draw()
fig1.savefig('./images/nb_count.png')


###############################################################################
## lowest, average, and highest price properties

# get tables
nb_min_price = listingsDF.groupby('neighbourhood_group_cleansed')['price_avg'].min()
nb_mean_price = listingsDF.groupby('neighbourhood_group_cleansed')['price_avg'].mean()
nb_max_price = listingsDF.groupby('neighbourhood_group_cleansed')['price_avg'].max()

# reset index
nb_min_price = nb_min_price.reset_index()
nb_mean_price = nb_mean_price.reset_index()
nb_max_price = nb_max_price.reset_index()

# merge tables
nb_price = pd.merge(nb_min_price,nb_mean_price,how='inner',on='neighbourhood_group_cleansed')
nb_price = pd.merge(nb_price,nb_max_price,how='inner',on='neighbourhood_group_cleansed')

# drop unused
del nb_min_price,nb_mean_price,nb_max_price

# rename cols
nb_price = nb_price.rename(columns={"neighbourhood_group_cleansed":"neighborhood","price_avg_x":"min","price_avg_y":"avg","price_avg":"max"})

# sorting can be done by value:
nb_price = nb_price.sort_values(by='avg', ascending=False)

# round decimals on available:
nb_price['min'] = round(nb_price['min'],1)
nb_price['avg'] = round(nb_price['avg'],1)
nb_price['max'] = round(nb_price['max'],1)

# print csv
#nb_price.to_csv('nb_price.csv', columns=['neighborhood', 'min', 'avg', 'max'], sep=',', index=False)


# create visual: nb_price
objects = tuple(nb_price['neighborhood'])
n_groups = len(objects)
price_mins = tuple(nb_price['min'])
price_avgs = tuple(nb_price['avg'])
price_maxs = tuple(nb_price['max'])

fig, ax = plt.subplots()
index = np.arange(n_groups)
index = index*2

bar_width = 0.5
opacity = 0.8

rects1 = plt.bar(index - bar_width, price_mins, bar_width,
alpha=opacity,
color='b',
label='min')

rects2 = plt.bar(index, price_avgs, bar_width,
alpha=opacity,
color='g',
label='avg')

rects3 = plt.bar(index + bar_width, price_maxs, bar_width,
alpha=opacity,
color='r',
label='max')

plt.ylabel('Price')
plt.title('Prices by Neighborhood')
plt.xticks(index, objects, rotation=90)
plt.legend()
plt.tight_layout()

fig1 = plt.gcf()
plt.show()
plt.draw()
fig1.savefig('./images/nb_price.png')


###############################################################################
## Count by property types in each neighborhood

# group by neighborhood and property type
nb_count_property_type = listingsDF.groupby(['neighbourhood_group_cleansed','property_type'])['zipcode'].count()

# reset index
nb_count_property_type = nb_count_property_type.reset_index()

# pivot the table to get more columns
nb_count_property_type = nb_count_property_type.pivot(index = 'neighbourhood_group_cleansed'
                                                      ,columns = 'property_type'
                                                      ,values = 'zipcode')

# reset the index again
nb_count_property_type = nb_count_property_type.reset_index()

# sort by number of apartments
nb_count_property_type = nb_count_property_type.sort_values(by='Apartment', ascending=False)

# rename cols
nb_count_property_type = nb_count_property_type.rename(columns={"neighbourhood_group_cleansed":"neighborhood","Apartment":"Apartment","House":"House","Other":"Other"})

# print csv
#nb_count_property_type.to_csv('nb_count_property_type.csv', columns=['neighborhood', 'Apartment', 'House', 'Other'], sep=',', index=False)


# create visual nb_count_property_type
objects = tuple(nb_count_property_type['neighborhood'])
n_groups = len(objects)
means_apt = tuple(nb_count_property_type['Apartment'])
means_house = tuple(nb_count_property_type['House'])
means_other = tuple(nb_count_property_type['Other'])

fig, ax = plt.subplots()
index = np.arange(n_groups)
index = index*2

bar_width = 0.5
opacity = 0.8

rects1 = plt.bar(index - bar_width, means_apt, bar_width,
alpha=opacity,
color='b',
label='Apartment')

rects2 = plt.bar(index, means_house, bar_width,
alpha=opacity,
color='g',
label='House')

rects3 = plt.bar(index + bar_width, means_other, bar_width,
alpha=opacity,
color='r',
label='Other')

plt.ylabel('Count')
plt.title('Property type by Neighborhood')
plt.xticks(index, objects, rotation=90)
plt.legend()

plt.tight_layout()

fig1 = plt.gcf()
plt.show()
plt.draw()
fig1.savefig('./images/nb_count_property_type.png')

###############################################################################
## User ratings in each neighborhood

# group
nb_mean_rating = listingsDF.groupby('neighbourhood_group_cleansed')['review_scores_rating'].mean()

# reset index
nb_mean_rating = nb_mean_rating.reset_index()

# rename
nb_rating = nb_mean_rating

# sort by number of apartments
nb_rating = nb_rating.sort_values(by='review_scores_rating', ascending=False)

# rename cols
nb_rating = nb_rating.rename(columns={"neighbourhood_group_cleansed":"neighborhood","review_scores_rating":"Avg Rating"})

# round decimals:
nb_rating['Avg Rating'] = round(nb_rating['Avg Rating'],1)

# print csv
#nb_rating.to_csv('nb_rating.csv', columns=['neighborhood', 'Avg Rating'], sep=',', index=False)


# create visual nb_rating

objects = tuple(nb_rating['neighborhood'])
y_pos = np.arange(len(objects))
plt.bar(y_pos, list(nb_rating['Avg Rating']), align='center', alpha=0.5)
plt.xticks(y_pos, objects, rotation=90)
plt.ylabel('Rating')
plt.title('Mean Rating by Neighborhood')
plt.tight_layout()

fig1 = plt.gcf()
plt.show()
plt.draw()
fig1.savefig('./images/nb_rating.png')
