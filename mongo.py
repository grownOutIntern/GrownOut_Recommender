from pymongo import MongoClient
from elasticsearch import Elasticsearch
import itertools
from bson.objectid import ObjectId
import math
mongo = "mongodb://mongolab1.grownout.com:27017,mongolab2.grownout.com:27017/grownout2"
job_server = MongoClient(mongo)
jobs=job_server.grownout2.job


mongo_server = MongoClient(['dev-mongo2.grownout.com:27017','dev-mongo1.grownout.com:27017'],replicaset='amoeba-mongo')
shortlisted_candidates = mongo_server['recommender'].shortlist


Elastic_Prod_Address = "prod-es1.grownout.com"
Elastic_Port = 6200
elastic_prod = Elasticsearch([{'host':Elastic_Prod_Address,'port':Elastic_Port}])

top_cities={}

def get_designation_score_present(company,companies_data,mapping_shortlisted,mapping_jobs):
	n=0
	score=0
	for i in mapping_shortlisted:
		try:
			cand_exp=elastic_prod.search(index=company,doc_type="candidate",body={ "query":{ "term":{ "_id":{ "value":i['candidate_id'] } } },"_source":["experiences"] })
		except Exception,e:
			print "no here",e
		try:
			job_designation=mapping_jobs[ObjectId(i['job_id'])]['title']
			print "job designation :" 
			print job_designation
			for j in cand_exp['hits']['hits'][0]['_source']['experiences']:
				if(j['current']==True):
					print "candidate current designation :" 
					print j['title']
					score=((score*n)+fuzz.WRatio(j['title'],job_designation))/(n+1)
					n+=1
		except Exception,e:
			print "here",e
	return score,n

def get_designation_score_past(company,companies_data,mapping_shortlisted,mapping_jobs):
	n=0
	score=0
	for i in mapping_shortlisted:
		try:
			cand_exp=elastic_prod.search(index=company,doc_type="candidate",body={ "query":{ "term":{ "_id":{ "value":i['candidate_id'] } } },"_source":["experiences"] })
		except Exception,e:
			print e
		try:
			job_designation=mapping_jobs[ObjectId(i['job_id'])]['title']
			print "job designation :"
			print job_designation
			for j in cand_exp['hits']['hits'][0]['_source']['experiences']:
				if(j['current']==False):
					print "selected designations past :"
					print j['title']
					score=((score*n)+fuzz.WRatio(j['title'],job_designation))/(n+1)
					n+=1
		except Exception,e:
			print e
	return score,n

def get_experience_score(company,mapping_shortlisted,mapping_jobs,):
	print "getting experience"
	experience=[0]*18
	for i in mapping_shortlisted:
		try:
			low=int( mapping_jobs[ObjectId(i['job_id'])]['experience']['min'] )
			high=int( mapping_jobs[ObjectId(i['job_id'])]['experience']['max'] )
			candidate=elastic_prod.search(index=i['company_id'],doc_type="candidate",body={"query": {
																							   	"term": {
																							    	"_id": {
																							    		"value": i['candidate_id']
																							      			}
																							    		}
																							    	},
																						    "_source" : [
																						    		"total_experience"
																						    			]
																						    })
			candidate_experience=candidate['hits']['hits'][0]['_source']['total_experience']/365.00
			experience[-2]=min(low,experience[-2])
			experience[-1]=max(high,experience[-1])
			if candidate_experience>=low and candidate_experience<=high:
				experience[int(math.floor(candidate_experience))]+=1
			else:
				if candidate_experience<low or candidate_experience>=high+1:
					experience[int(math.floor(candidate_experience))]+=1
				else:
					experience[int(math.ceil(candidate_experience))]+=1
		except Exception,e:
			print e
	return experience



# def get_skill_score(company,companies,mapping_shortlisted,mapping_jobs):
# 	if "skill_score" not in companies[company]:
# 		companies[company]['skill_score']=0
# 	score=[0.0,0.0]
# 	for i in mapping_shortlisted[company]:
# 		skills=[]
# 		try:
# 			for k in mapping_jobs[ObjectId(i['job_id'])]['skills']:
# 				skills.append(k)
# 			candidate=elastic_prod.search(index=i['company_id'],doc_type="candidate",body={"query": {
# 																						    "term": {
# 																						      "_id": {
# 																						        "value": i['candidate_id']
# 																						      }
# 																						    }
# 																						    }})
# 			for skill in candidate['hits']['hits'][0]['_source']['skill_tagged']:
# 				for j in skills:
# 					if j in skill['name']:
# 						score[0]+=skill['score']
# 						score[1]+=1
# 		except Exception, e:
# 			print e
# 	companies[company]['skill_score']=score[0]/(score[1]+1)

def get_colleges(company_id, mapping_shortlisted, mapping_jobs):
	print "getting colleges"
	print mapping_shortlisted
	colleges_hired={}
	education=[0,0]
	for i in mapping_shortlisted[company_id]:
		try:
			candidate=elastic_prod.search(index=i['company_id'],doc_type="candidate",body={"query": {
																							    "term": {
																							     	"_id": {
																							     		"value": i['candidate_id']
																							      			}
																							    		}
																							    	},
																							'_source' : [
																							 		'educations'
																							 ]
																							})
			
			print candidate['hits']['hits'][0]['_source']['educations']
			print "next"
			for college in candidate['hits']['hits'][0]['_source']['educations']:
				if college['institute']['name'] in colleges_hired:
					colleges_hired[college['institute']['name']]+=1
				else:
					colleges_hired[college['institute']['name']]=1
				if college['qualification']['name']=="Graduate":
					education[1]+=1
					education[0]=(education[0]*(education[1]-1))/education[1]
				elif college['qualification']['name']=="Post Graduate":
					education[1]+=1
					education[0]=(education[0]*(education[1]-1)+1)/education[1]
				else:
					education[1]+=1
					education[0]=(education[0]*(education[1]-1)+2)/education[1]
		except Exception, e:
			print e,i['candidate_id']
	return colleges_hired,education			
	# companies[company_id]['college_score']= dict(itertools.islice(companies[company_id]['college_score'].iteritems(), 10))

def get_top_cities():
	Prod_Address = "dev-es1.grownout.com"
	Port = 6200
	prod = Elasticsearch([{'host':Prod_Address,'port':Port}])
	data = prod.search(index = 'complete', doc_type = 'user', body = {'aggs':{
																			"top_cities":{
																				"terms":{
																					"field": "linkedin.current_location.name.raw",
        																			"size": 20
																					}
																				}			
																			}
		},search_type='count')
	print data
	data=data['aggregations']['top_cities']['buckets'][1:]
	for i in range(len(data)):
		top_cities[data[i]['key']]=i
	# print top_cities

		



def get_city_score(company, mapping_shortlisted, mapping_jobs):
	print "getting cities"
	print company
	# print mapping_shortlisted[company]
	cities=[0]*20
	try:

		for i in mapping_shortlisted[company]:
			print "yes"
			candidate_location=elastic_prod.search(index=i['company_id'], doc_type='candidate', body={ "query": {
																										    "term": {
																										     	"_id": {
																										     		"value": i['candidate_id']
																										      			}
																										    		}
																										    	},
																										'_source' : [
																										 		'linkedin.current_location.name'
																										 ]
																										})
			print candidate_location
			candidate_location = candidate_location['hits']['hits'][0]['_source']['linkedin']['current_location']['name']
			if candidate_location in top_cities:
				cities[top_cities[candidate_location]]+=1
	except Exception,e:
		print e, "        occurred from get_city_score"
	return cities

def create_behavior(company,companies_data,mapping_shortlisted,mapping_jobs):
	print "in create_behavior"
	time=0
	if company in companies_data:
		time=companies[company]['time']
		companies_data[company]['time']+=1
	else:
		companies_data[company]={}
	# getting college and education preferences for the company
	college_preferences,educational_preferences=get_colleges(company,mapping_shortlisted,mapping_jobs)
	
	#set colleges
	if "college_preferences" not in  companies_data[company]:
		companies_data[company]["college_preferences"]=college_preferences
	else:
		for i in companies_data[company]['college_preferences']:
			companies_data[company]['college_preferences'][i]*=(0.7**time)
		for i in college_preferences:
			if i in companies_data[company]['college_preferences']:
				companies_data[company]['college_preferences'][i]+=college_preferences[i]
		companies_data[company]['college_preferences']=dict(itertools.islice(companies_data[company_id]['college_preferences'].iteritems(), 10))

	#setting the experience preferences
	experience_preference=get_experience_score(company,mapping_shortlisted,mapping_jobs)
	if "experience_preference" not in companies_data[company]:
		companies_data[company]['experience_preference']=experience_preference
	else:
		companies_data[company]['experience_preference']=[i*(0.7**time) for i in companies_data[company]['experience_preference']]
		for i in range(len(educational_preferences)-2):
			companies_data[company]["experience_preferences"][i]+=experience_preference[i]
		companies_data[company]["experience_preferences"][-1]=max(companies_data[company]["experience_preferences"][-1]/(0.7**time),experience_preference[-1])
		companies_data[company]["experience_preferences"][-2]=min(companies_data[company]["experience_preferences"][-2]/(0.7**time),experience_preference[-2])
	

	#set educational_preferences
	if "educational_preferences" not in companies_data[company]:
		companies_data[company]['educational_preferences']=educational_preferences
	else:
		companies_data[company]['educational_preferences']=companies_data[company]['educational_preferences']
		st_edu_av=companies_data[company]['educational_preferences'][0]
		st_edu_no=companies_data[company]['educational_preferences'][1]
		st_edu_av=((st_edu_av*st_edu_no)+educational_preferences[0])/(st_edu_no+educational_preferences[1])
		st_edu_no=st_edu_no+educational_preferences
		companies_data[company]['educational_preferences'][0]=st_edu_av
		companies_data[company]['educational_preferences'][1]=st_edu_no

	#getting the preference score for cities
	print mapping_shortlisted
	city_score=get_city_score(company,mapping_shortlisted,mapping_jobs)
	if "city_score" not in  companies_data[company]:
		companies_data[company]["city_score"]=city_score
	else:
		companies_data[company]["city_score"]=[i*(0.7**time) for i in companies_data[company]["city_score"]]
		for i in range(len(city_score)):
			companies_data[company]["city_score"][i]+=city_score[i]

	# print companies_data

def main():
	companies_data={}
	mapping_shortlisted={}
	mapping_jobs={}
	for i in shortlisted_candidates.find():
		#shortlisted_candidates[i]['used_in_recommendation']=False
		if i['company_id'] not in mapping_shortlisted:
			mapping_shortlisted[i['company_id']] = [[],0]
		#if(shortlisted_candidates[i]['used_in_recommendation']==False):
		mapping_shortlisted[i['company_id']][0].append(i)
		mapping_shortlisted[i['company_id']][1]+=1
		#shortlisted_candidates[i]['used_in_recommendation']=True
	for i in jobs.find():
		mapping_jobs[i['_id']]=i

	for i in mapping_shortlisted:
		if(mapping_shortlisted[i][1]%1==0):
			print i
			create_behavior(i,companies_data,mapping_shortlisted[i][0],mapping_jobs)
	print companies_data	
# create_behavior('1125387',companies_data)
get_top_cities()
main()
# get_experience_score1()

