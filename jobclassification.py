from fuzzywuzzy import fuzz
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import itertools
from bson.objectid import ObjectId
import math
from fuzzywuzzy import fuzz
mongo = "mongodb://mongolab1.grownout.com:27017,mongolab2.grownout.com:27017/grownout2"
job_server = MongoClient(mongo)
jobs=job_server.grownout2.job
mongo_server = MongoClient(['dev-mongo2.grownout.com:27017','dev-mongo1.grownout.com:27017'],replicaset='amoeba-mongo')
shortlisted_candidates = mongo_server['recommender'].shortlist
Elastic_Prod_Address = "prod-es1.grownout.com"
Elastic_Port = 6200
elastic_prod = Elasticsearch([{'host':Elastic_Prod_Address,'port':Elastic_Port}])


def job_classifier(designation):
	#designation=job['title']
	tech_words = ["Coder","Database","SDE","Developement","cto","CTO","Engineer","Front Back End",
                  "Information","IT","Network","Programmer",'QA',
                  "Quality Assurance", "Tester","Graphics"
                  "Scientist","Software","System","Security","Technical","ui/ux","UI/UX", "user interface", "user experience", "Web","iOS","IOS","Android","Java","J2EE","Python"]
	non_tech_words = ["HR","hr","Administrative","professor","teacher","Manager","Director","Product","ceo","CEO","Recruiter","Consultant","Accountant","Human Resource","Business","Analyst","Sales","Marketing","Placement","Associate","Operations","executive","Finance"]
	
	tech_skills = ["Algorithms","Data Structures",]
	
	non_tech_skills = ["Leadership","Communication"]
	#product_words = ["product","manager"]
	#product_words = ["product manager", "product head", "product lead", "product owner", "vp product", "product management"]
	#designation = pre_process_field(designation,'!"\'()*,./:;<=>?@[\\]^_`{|}~-%')
	tech_score=0;non_tech_score=0;c1=0;c2=0;product_score=0;
    
	for word in tech_words:
		temp=fuzz.partial_ratio(designation,word)
		if(temp>66):
			c1+=1
			#print designation,word,temp
			tech_score+=temp
	
	
	for word in non_tech_words:
		temp=fuzz.partial_ratio(designation,word)
		if(temp>66):
			c2+=1
			#print designation,word,temp
			non_tech_score+=temp
	
	#print "non tech score 2 = ",fuzz.partial_ratio(designation,tech_words_2)
	#for word in product_words:
	#	product_score+=fuzz.ratio(designation,word)
	
	tech_score=tech_score
	non_tech_score=non_tech_score
	#print "tech score = ",tech_score
	
	#print "non tech score = ",non_tech_score
	print designation
	relative_score=tech_score-non_tech_score
	
	if(relative_score>=50):
		print "tech job"
		"""if(non_tech_score==0):
			skills=job['skills']
			for skill in skills:
				if skill not in tech_skills:
					tech_skills.append(skill)"""
	
	elif(relative_score<=-50):
		print "non tech job"
		"""if(tech_score==0):
			skills=job['skills']
			for skill in skills:
				if skill not in non_tech_skills:
					non_tech_skills.append(skill)"""
	
	else:
		print "not sure"
		"""tech_skill_score=0;non_tech_skill_score=0;
		job_skills=job['skills']
		for job_skill in job_skills:
			for tech_skill in tech_skills:
				temp=fuzz.partial_ratio(job_skill,tech_skill)
				if(temp>66):
					tech_skill_score+=temp
			for non_tech_skill in non_tech_skills:
				temp=fuzz.partial_ratio(job_skill,non_tech_skill)
				if(temp>66):
					non_tech_skill_score+=temp
		total_tech_score = tech_score + tech_skill_score
		total_non_tech_score = non_tech_score + non_tech_skill_score
		new_relative_score = total_tech_score - total_non_tech_score
		if(new_relative_score>=50):
			print "tech job"
		elif(new_relative_score<=-50):
			print "non tech job"
		else:
			print "still not sure" """

def main():
	mapping_shortlisted={}
	mapping_jobs={}
	companies={}
	for i in shortlisted_candidates.find():
		if i['company_id'] not in mapping_shortlisted:
			mapping_shortlisted[i['company_id']]=[]
		mapping_shortlisted[i['company_id']].append(i)
	for i in jobs.find():
		mapping_jobs[i['_id']]=i
	for i in shortlisted_candidates.find():
		try:
			companies[i['company_id']]+=1
		except Exception, e:
			companies[i['company_id']]=1
	h=0
	count=0
	for i in companies:
		if h!=-1:
			if companies[i]%1==0:
				count+=1
				print i
				if(count==5):
					break
				for j in mapping_shortlisted[i]:
					try:
						cand_exp=elastic_prod.search(index=i,doc_type="candidate",body={ "query":{ "term":{ "_id":{ "value":j['candidate_id'] } } },"_source":["experiences"] })		
						job_classifier(mapping_jobs[ObjectId(j['job_id'])])
					except Exception,e:
						print e

#main()

def new_main():
	temp=elastic_prod.search(index="complete",doc_type="user",body={
  									"aggs": {
    										"designation": {
      											"nested": {
        											"path": "linkedin.experiences"
      											},
      										"aggs": {
        									"top_des": {
          										"terms": {
            											"field": "linkedin.experiences.title.raw",
            											"size": 500
          												}
        											}
      											}
    										}
  											}
									},search_type="count")
	des_list=[]
	for i in temp['aggregations']['designation']['top_des']['buckets']:
		job_classifier(i["key"])
	




new_main()
