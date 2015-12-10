from pymongo import MongoClient
from elasticsearch import Elasticsearch
import itertools
from bson.objectid import ObjectId
import math
from fuzzywuzzy import fuzz
import re
import numpy as np
from scipy.optimize import curve_fit
mongo = "mongodb://mongolab1.grownout.com:27017,mongolab2.grownout.com:27017/grownout2"
job_server = MongoClient(mongo)
jobs=job_server.grownout2.job

def level_classifier(designation, work_ex_start, work_ex_end):
    #for the start and end, we will take the average of the two proababilities which will be multiplied with the designation probability
    we_start_prob = prob_workex(work_ex_start)
    we_end_prob = prob_workex(work_ex_end)

    designation_prob = prob_designation(designation)

    total_prob = []
    max = 0
    max_pos = 0
    count = 0
    while count < 4:
        total_prob.append((we_start_prob[count] + we_end_prob[count])*designation_prob[count])
        if max < total_prob[count]:
            max = total_prob[count]
            max_pos = count
        count += 1

    return max_pos+1


def prob_workex(work_ex):
    #this work ex is in years
    level1_prob = (5.63267150224e-7)*(work_ex**4) - (8.3793812395035e-5)*(work_ex**3) + 0.004283984029*(work_ex**2) - 0.091123795*work_ex + 0.8078083788
    level2_prob = (3.77e-8)*(work_ex**5) - (5.9215e-6)*(work_ex**4) + (3.45103e-4)*(work_ex**3) - (9.0236616e-3)*(work_ex**2) + 0.0931337792*work_ex + 0.0140669993
    level3_prob = (1.155053384675e-5)*(work_ex**3) - (11.107992e-4)*(work_ex**2) + 0.0344897159878*work_ex - 0.004344987358
    level4_prob = -(2.6419290474278e-7)*(work_ex**3) - (1.62968837845889e-4)*(work_ex**2) + 0.016088958263234*work_ex + 0.0665343474929792

    return assign_realistic_probabilities(level1_prob, level2_prob, level3_prob, level4_prob)


def assign_realistic_probabilities(level1_prob,level2_prob,level3_prob,level4_prob):
    #returning non negative values with giving 1% chance to every case
    level1_prob = max(level1_prob, 0.01)
    level2_prob = max(level2_prob, 0.01)
    level3_prob = max(level3_prob, 0.01)
    level4_prob = max(level4_prob, 0.01)
    #probability changed so as to maintain sum as 1
    total_prob = level1_prob + level2_prob + level3_prob + level4_prob
    level1_prob = level1_prob/total_prob
    level2_prob = level2_prob/total_prob
    level3_prob = level3_prob/total_prob
    level4_prob = level4_prob/total_prob

    return [level1_prob, level2_prob, level3_prob, level4_prob]


def get_designation_probability():
    designation_db = MONGO_SERVER['designation_database']
    designation_prob_collection = designation_db.prob_collection
    for designation in designation_prob_collection.find():
        constant.DESIGNATION_PROB_DICT[designation['name']] = [float(designation['level_1']),float(designation['level_2']),
                                                      float(designation['level_3']),float(designation['level_4'])]
def analysis():
    MID_MANAGER_LEVEL = ['senior', 'sr']
    MANAGER_LEVEL = ['manager', 'lead', 'head',  'leader', 'gerente','specialist']
    DIRECTOR_LEVEL = ['director', 'partner', 'general', 'managing', 'gm', 'dgm', 'agm']
    BOARD_LEVEL = ['president','md','vice','vp', 'avp', 'entrepreneur', 'owner', 'proprietor', 'chairman', 'founder', 'board', 'chief', 'ceo', 'cto', 'cfo', 'coo', 'cro', 'cmo', 'cso', 'cio']
    mongo_server = MongoClient(['dev-mongo2.grownout.com:27017','dev-mongo1.grownout.com:27017'],replicaset='amoeba-mongo')
    designation_db = mongo_server['designation_database']
    designation_prob_collection = designation_db.prob_collection

    designation_probs={}


    for designation in designation_prob_collection.find():
        designation_probs[designation['name']] = designation


    # naukri_tittle_exp={}
    print designation_probs
    for k in jobs.find():
        
        designation=k['title'].replace("-"," ")
        designation=re.sub('[^0-9a-zA-Z]+', ' ', designation)
        designation_words=designation.split(" ")
        prob_designation=[0.0]*4
        prob_experience=[0.0]*4
        for word in designation_words:
            
            if word.lower() in designation_probs:
                # print "entered true"
                prob_designation[0]+=float(designation_probs[word.lower()]['level_1'])
                prob_designation[1]+=float(designation_probs[word.lower()]['level_2'])
                prob_designation[2]+=float(designation_probs[word.lower()]['level_3'])
                prob_designation[3]+=float(designation_probs[word.lower()]['level_4'])
        # print prob_designation,"this is prob_designation", "  ",designation
        # break
        design_probs =prob_designation.index(max(prob_designation))
        # print design_probs
        match_designation=[0.0]*4
        for j in MID_MANAGER_LEVEL:
            match_designation[0]=max(fuzz.ratio(designation,j),match_designation[0])
        for j in MANAGER_LEVEL:
            match_designation[1]=max(fuzz.ratio(designation,j),match_designation[1])
        for j in DIRECTOR_LEVEL:
            match_designation[2]=max(fuzz.ratio(designation,j),match_designation[2])
        for j in BOARD_LEVEL:
            match_designation[3]=max(fuzz.ratio(designation,j),match_designation[3])
        min_exp_probs=prob_workex(float(k['experience']['min']))
        max_exp_probs=prob_workex(float(k['experience']['max']))
        # print "min ",min_exp_probs
        # print "max ",max_exp_probs
        for i in range(4):
            prob_experience[i]+=(((min_exp_probs[i])*0.8)+((max_exp_probs[i])))/1.8
        # print prob_experience," prob experience"
        design_exp=prob_experience.index(max(prob_experience))
        # print match_designation,"this is match designation"
        design_match=match_designation.index(max(match_designation))
        # if design_exp!=design_probs:
        print design_match,":",design_probs,":",design_exp,"     ",k['title']
def fiting_function(x,a,b,c,d,e):
    return a*(x**4)+b*(x**3)+c*(x**2)+d*x+e
def fit_curve(x,y):
    params=curve_fit(fiting_function,x,y)
    print params




    # print naukri_tittle_exp
analysis()
        