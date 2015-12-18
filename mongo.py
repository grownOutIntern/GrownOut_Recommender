from fuzzywuzzy import fuzz
# from Job_Classifier_V2 import job_classifier
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import itertools
from bson.objectid import ObjectId
import math
import logging

mongo = "mongodb://mongolab1.grownout.com:27017,mongolab2.grownout.com:27017/grownout2"
job_server = MongoClient(mongo)
jobs=job_server.grownout2.job
mongo_server = MongoClient(['dev-mongo2.grownout.com:27017','dev-mongo1.grownout.com:27017'],replicaset='amoeba-mongo')
shortlisted_candidates = mongo_server['recommender'].shortlist
behaviour=mongo_server['recommender'].behaviour
Elastic_Prod_Address = "prod-es1.grownout.com"
Elastic_Port = 6200
elastic_prod = Elasticsearch([{'host':Elastic_Prod_Address,'port':Elastic_Port}])

top_cities={}
map_cities={"Noida":"New Delhi","Gurgaon":"New Delhi","Gautam Budh Nagar district":"New Delhi","Thane":"Mumbai","South West Delhi, Delhi, India":"New Delhi"}
def get_designation_score_present(cand_exp,job_designation,score,n):
	try:
		# print "job designation :" 
		# print job_designation
		for exp in cand_exp:
			if(exp['current']==True):
				# print "candidate current designation :" 
				# print exp['title']
				score=((score*n)+fuzz.WRatio(exp['title'],job_designation))/(n+1)
				n+=1
	except Exception,e:
		print e," from present designation function"
	return [score,n]

def get_designation_score_past(cand_exp,job_designation,score,n):
	try:
		# print "job designation :"
		# print job_designation
		for exp in cand_exp:
			if(exp['current']==False):
				# print "selected designations past :"
				# print exp['title']
				score=((score*n)+fuzz.WRatio(exp['title'],job_designation))/(n+1)
				n+=1
	except Exception,e:
		print e," from past designaton score"
	return [score,n]

def get_experience_score(candidate_experience,low,high):
	print "getting experience"
	try:
		if candidate_experience>=low and candidate_experience<=high:
			percent=(candidate_experience-low)/(high-low)
			if percent<=.25:
				# experience[2][0]+=1
				return [2,0]
			elif spercent<=.5:
				# experience[2][1]+=1
				return [2,1]
			elif percent<=.75:
				# experience[2][2]+=1
				return [2,2]
			else:
				# experience[2][3]+=1
				return [2,3]
		else:
			if candidate_experience<low and candidate_experience>=low-1:
				# experience[1][0]+=1
				return [1,0]
			elif candidate_experience>high and candidate_experience<=high+1:
				# experience[3][0]+=1
				return [3,0]
			elif candidate_experience<low-1:
				# experience[0][0]+=1
				return [0,0]
			else:
				# experience[4][0]+=1
				return [4,0]
	except Exception,e:
		print e," from experience score"
	return [6,6]



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

def get_colleges(colleges_hired,education_list):
	print "getting colleges"
	education=-6
	for college in education_list:
		try:
			a=college['qualification']['name']
			col=college['institute']['name'].replace(".","")
			if col in colleges_hired:
				colleges_hired[col]+=1
			else:
				colleges_hired[col]=1
		except Exception,e:
			print e," from get colleges institution part"
		try:
			if college['qualification']['name']=="Graduate" or college['qualification']['name']=="Associate":
				if education<2:
					education=2
			elif college['qualification']['name']=="Post Graduate" or college['qualification']['name']=="Professional" or college['qualification']['name']=="Postgraduate Diploma":
				if education<3:
					education=3
			elif college['qualification']['name']=="Doctoral":
				education=4
			elif college['qualification']['name']=="Diploma" or college['qualification']['name']=="Undergraduate":
				if education<1:
					education=1
		except Exception,e:
			print e," from get colleges institution level part"
	return education-1
	# companies[company_id]['college_score']= dict(itertools.islice(companies[company_id]['college_score'].iteritems(), 10))

def get_top_cities():
	Prod_Address = "dev-es1.grownout.com"
	Port = 6200
	prod = Elasticsearch([{'host':Prod_Address,'port':Port}])
	try:
		data_agg = prod.search(index = 'complete', doc_type = 'user', body = {'aggs':{
																				"top_cities":{
																					"terms":{
																						"field": "linkedin.current_location.name.raw",
	        																			"size": 25
																						}
																					}			
																				}
			},search_type='count')
		data=data_agg['aggregations']['top_cities']['buckets'][1:]
		for i in range(len(data)):
			if data[i]['key'] not in map_cities:
				top_cities[data[i]['key']]=i
		top_cities['others']=len(data)
	except Exception,e:
		print e," from aggregations query getting the top cities"

	# print top_cities

		



def get_city_score(city):
	print "getting cities"
	if city in map_cities:
		return top_cities[map_cities[city]]
	elif city in top_cities:
		return top_cities[city]
	else:
		return 20
	return 200


def create_behavior(company,companies_data,mapping_shortlisted,mapping_jobs):
	print "in create_behavior"
	des_scores_t=[0,0,0,0];des_scores_nt=[0,0,0,0]
	college_preferences_t={};college_preferences_nt={}
	educational_preferences_t=[0]*4;educational_preferences_nt=[0]*4
	experience_preference_t=[[0],[0],[0,0,0,0],[0],[0]];experience_preference_nt=[[0],[0],[0,0,0,0],[0],[0]]
	city_score_t=[0.0]*21;city_score_nt=[0.0]*20
	#if companies_data[company]==None:
	#	companies_data[company]={}
	if company not in companies_data:
		companies_data[company]={}
	if 'tech' not in companies_data[company]:
		companies_data[company]['tech']={}
		companies_data[company]['non_tech']={}
	# getting college and education preferences for the company
	for i in mapping_shortlisted:
		try:
			candidate=elastic_prod.search(index=company,doc_type="candidate",body={ "query":{ "term":{ "_id":{ "value":i['candidate_id'] } } } })
			job=mapping_jobs[ObjectId(i['job_id'])]
			exp=0.0
			try:
				exp+=float(candidate['hits']['hits'][0]['_source']['total_experience'])/365.00
			except Exception, e:
				exp+=float(candidate['hits']['hits'][0]['_source']['linkedin']['total_experience'])/365.00
			b=get_experience_score(exp,float(job['experience']['min']),float(job['experience']['max']))
			a=get_city_score(candidate['hits']['hits'][0]['_source']['linkedin']['current_location']['name'])
			job_type=job_classifier(job)
			
			if(job_type=='tech job' or job_type=="not sure"):
				des_scores_t[0],des_scores_t[1]=get_designation_score_present(candidate['hits']['hits'][0]['_source']['experiences'],job['title'],des_scores_t[0],des_scores_t[1])
				des_scores_t[2],des_scores_t[3]=get_designation_score_past(candidate['hits']['hits'][0]['_source']['experiences'],job['title'],des_scores_t[2],des_scores_t[3])
				if a!=200:
					city_score_t[a]+=1
				education_level=get_colleges(college_preferences_t, candidate['hits']['hits'][0]['_source']['educations'])
				educational_preferences_t[education_level]+=1
				experience_preference_t[b[0]][b[1]]+=1

			elif(job_type=='non tech job' or job_type=="not sure"):
				des_scores_nt[0],des_scores_nt[1]=get_designation_score_present(candidate['hits']['hits'][0]['_source']['experiences'],job['title'],des_scores_nt[0],des_scores_nt[1])
				des_scores_nt[2],des_scores_nt[3]=get_designation_score_present(candidate['hits']['hits'][0]['_source']['experiences'],job['title'],des_scores_nt[2],des_scores_nt[3])
				if a!=200:
					city_score_nt[a]+=1
				education_level=get_colleges(college_preferences_nt, candidate['hits']['hits'][0]['_source']['educations'])
				educational_preferences_nt[education_level]+=1
				experience_preference_nt[b[0]][b[1]]+=1
		except Exception,e:
			print e," from create behavior func"

	

	# #set educational_preferences
	#for tech
	if "college_preferences" not in  companies_data[company]['tech']:
		companies_data[company]['tech']["college_preferences"]=college_preferences_t
	else:
		for i in companies_data[company]['tech']['college_preferences']:
			companies_data[company]['tech']['college_preferences'][i]*=(0.7)
		for i in college_preferences_t:
			if i in companies_data[company]['tech']['college_preferences']:
				companies_data[company]['tech']['college_preferences'][i]+=college_preferences_t[i]
			else:
				companies_data[company]['tech']['college_preferences'][i]=college_preferences_t[i]
	companies_data[company]['tech']['college_preferences']=dict(itertools.islice(companies_data[company]['tech']['college_preferences'].iteritems(), 10))

	if "des_scores" not in companies_data[company]['tech']:
		companies_data[company]['tech']['des_scores']=des_scores_t
	else:
		for i in range(len(companies_data[company]['tech']['des_scores'])):
			companies_data[company]['tech']['des_scores'][i]*=0.7
			companies_data[company]['tech']['des_scores'][i]+=des_scores_t[i]
	
	
	
	if "experience_preference" not in companies_data[company]['tech']:
		companies_data[company]['tech']['experience_preference']=experience_preference_t
	else:
		for i in range(5):
			for j in range(len(experience_preference_t[i])):
				companies_data[company]['tech']['experience_preference'][i][j]*=(0.7)
		for i in range(5):
			for j in range(len(experience_preference_t[i])):
				companies_data[company]['tech']['experience_preference'][i][j]+=experience_preference_t[i][j]
	if "educational_preferences" not in companies_data[company]['tech']:
		companies_data[company]['tech']['educational_preferences']=educational_preferences_t
	else:
		for i in range(8):
			companies_data[company]['tech']['educational_preferences'][i]*=(0.7)
		for i in range(8):
			companies_data[company]['tech']['educational_preferences'][i]+=educational_preferences_t[i]

	if "city_score" not in  companies_data[company]['tech']:
		companies_data[company]['tech']["city_score"]=city_score_t
	else:
		companies_data[company]['tech']["city_score"]=[i*(0.7) for i in companies_data[company]['tech']["city_score"]]
		for i in range(len(city_score)):
			companies_data[company]['tech']["city_score"][i]+=city_score_t[i]
	
	#for non tech
	if "des_scores" not in companies_data[company]['non_tech']:
		companies_data[company]['non_tech']['des_scores']=des_scores_nt
	else:
		for i in range(len(companies_data[company]['non_tech']['des_scores'])):
			companies_data[company]['non_tech']['des_scores'][i]*=0.7
			companies_data[company]['non_tech']['des_scores'][i]+=des_scores_nt[i]
	
	if "college_preferences" not in  companies_data[company]['non_tech']:
		companies_data[company]['non_tech']["college_preferences"]=college_preferences_nt
	else:
		for i in companies_data[company]['non_tech']['college_preferences']:
			companies_data[company]['non_tech']['college_preferences'][i]*=(0.7)
		for i in college_preferences_nt:
			if i in companies_data[company]['non_tech']['college_preferences']:
				companies_data[company]['non_tech']['college_preferences'][i]+=college_preferences_nt[i]
			else:
				companies_data[company]['non_tech']['college_preferences'][i]=college_preferences_nt[i]
	companies_data[company]['non_tech']['college_preferences']=dict(itertools.islice(companies_data[company]['non_tech']['college_preferences'].iteritems(), 10))

	if "experience_preference" not in companies_data[company]['non_tech']:
		companies_data[company]['non_tech']['experience_preference']=experience_preference_nt
	else:
		for i in range(5):
			for j in range(len(experience_preference_nt[i])):
				companies_data[company]['non_tech']['experience_preference'][i][j]*=(0.7)
		for i in range(5):
			for j in range(len(experience_preference_nt[i])):
				companies_data[company]['non_tech']['experience_preference'][i][j]+=experience_preference_nt[i][j]


	if "educational_preferences" not in companies_data[company]['non_tech']:
		companies_data[company]['non_tech']['educational_preferences']=educational_preferences_nt
	else:
		for i in range(8):
			companies_data[company]['non_tech']['educational_preferences'][i]*=(0.7)
		for i in range(8):
			companies_data[company]['non_tech']['educational_preferences'][i]+=educational_preferences_nt[i]	

	if "city_score" not in  companies_data[company]['non_tech']:
		companies_data[company]['non_tech']["city_score"]=city_score_nt
	else:
		companies_data[company]['non_tech']["city_score"]=[i*(0.7) for i in companies_data[company]['non_tech']["city_score"]]
		for i in range(len(city_score)):
			companies_data[company]['non_tech']["city_score"][i]+=city_score_nt[i]
	companies_data[company]['company_id']=company
	print companies_data[company]
	#try:
	#	behaviour.update({'company_id':company},{'$set':{'tech':companies_data[company]['tech'],'non_tech':companies_data[company]['non_tech']}})
	#except:
	behaviour.insert(companies_data[company])
	#print companies_data[company]

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
		#except Exception,e:
		#	print e
	for i in jobs.find():
		mapping_jobs[i['_id']]=i

	for i in mapping_shortlisted:
		if(mapping_shortlisted[i][1]%1==0):
			print i
			#try:
			#	companies_data[i]=behaviour.find_one({'company_id':i})
			#except Exception,e:
			#	print e
			create_behavior(i,companies_data,mapping_shortlisted[i][0],mapping_jobs)
			#break
# create_behavior('1125387',companies_data)
# get_top_cities()
# main()
# get_experience_score1()

