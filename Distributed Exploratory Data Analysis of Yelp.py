#import json library to parse json files into rdd's
import json
#------Question 1----------
#load relevant json file into variable
review_rdd = sc.textFile('/user/msc/datasets/yelp/review.json').map(lambda x: json.loads(x))
#map the json file into key value pairs
#then fitler on two variables
kv_review_rdd = review_rdd.map(lambda x: (x['review_id'], {'business_id': x['business_id'], 'cool': x['cool'], 'date': x['date'], 'funny': x['funny'], 'stars': x['stars'], 'text': x['text'], 'useful': x['useful'], 'user_id': x['user_id']})).filter(lambda x: x[1]["useful"]>30 and x[1]["funny"]>20)
#aggrigate by count
kv_review_rdd.count()
kv_review_rdd.take(3)

#------Question 2----------
#load relevant json file into variable
business_rdd = sc.textFile('/user/msc/datasets/yelp/business.json').map(lambda x: json.loads(x))
#map rdd into kv pairs, filter on 3 attributes then aggrigate in a count
bs_rdd = business_rdd.map(lambda x: (x['business_id'], {'address': x['address'], 'attributes': x['attributes'], 'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'], 'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'], 'postal_code': x['postal_code'], 'review_count': x['review_count'], 'stars': x['stars'], 'state': x['state']})).filter(lambda x: x[1]['city'].lower() in 'las vegas').filter(lambda x: 'Nightlife' in x[1]["categories"]).filter(lambda x: x[1]['stars']>=4.5).sortBy(lambda x: x[1]['stars'], ascending=False)

bs_rdd.map(lambda x: x[1]['name']).distinct().take(10)
bs_rdd.count()
#------Question 3----------
bus_kv = business_rdd.map(lambda x: (x['business_id'], {'City': x['city'], 'Category': x['categories']}))
rev_kv = review_rdd.map(lambda x: (x['business_id'], {'User': x['user_id'], 'Useful': x['useful']}))
#once data in correct form, then join together
bus_rev = bus_kv.join(rev_kv)
#combine the two dictionaries together using **method
bus_rev_kv = bus_rev.mapValues(lambda x: {**x[0], **x[1]}.filter(lambda x: x[1]['City'].lower() in 'urbana-champaign').filter(lambda x: 'Nightlife' in x[1]["Category"]).filter(lambda x: x[1]["Useful"]!=0)
bus_rev_kv.map(lambda x: (x[1]['User'], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], ascending=False).take(10)
#------Question 4----------
user_rdd = sc.textFile('/user/msc/datasets/yelp/user.json').map(lambda x: json.loads(x))
# ### Proposed Question: Do more active users produce more useful reviews? 
user_rdd.map(lambda x: (x['user_id'], {'Name': x['name'], 'count': x['review_count'], 'Useful': x['useful']})).map(lambda x: ('All', x[1]['Useful'])).mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()

user_rdd.map(lambda x: (x['user_id'], {'Name': x['name'], 'count': x['review_count'], 'Useful': x['useful']})).filter(lambda x: x[1]['count']> 5).map(lambda x: ('Active', x[1]['Useful'])).mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()

user_rdd.map(lambda x: (x['user_id'], {'Name': x['name'], 'count': x['review_count'], 'Useful': x['useful']})).filter(lambda x: x[1]['count']<=5).map(lambda x: ('Nonactive', x[1]['Useful'])).mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()
