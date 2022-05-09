
from decimal import Decimal
from re import I
import pandas as pd
import numpy as np
import math

def __locPercentile(n, p=0.90):
    
    '''
    @param n: integer for percentile location calculation from a population. total number of population
    @param p: default= 0.90, float to select percentile population location to return
    :return: position of percentile p under as integer following the python round convention
    :business_rule: return number of contest participants who receives bonus score
    '''
    return round((1-p)*n)

def __rangeCalculation(numberOf_contest_participants, p=0.90):
    '''
    TODO complete definitions
    :param loc_percentile: integer for range division, number of participants who 
    receive bonus in a contest
    :return quantity of participants who receive third and fourth bonus range
    :business_rule: first and second bonus range are first 10 participants for all the contests
    
    '''
    range_for_percentile = __locPercentile(numberOf_contest_participants,p)
    ## punctuation for first 10 are keep for all contests
    range = (range_for_percentile - 10)/2
    return round(range)

def __bonusGrid():

    '''
    @return: dictionary = {key1: {key2: bonusValue}}
                        key1: string "rangen"
                        key2: times the participant enter the top 90% in previous contests (0-n)
                        bonusValue: bonus for that contest posicion and top 90% times.    
    '''
    score_level1 = [score for score in range(20, 100 + 1, 20)]
    score_level2 = [score for score in range(15, 100, 15)] #max number will be 90 
    score_level3 = [score for score in range(10, 100 + 1, 10)]
    score_level4 = [score for score in range(5, 100 + 1, 5)]

    score_list = [score_level1, score_level2, score_level3, score_level4]

    bonus_grid = {}
    bonus_grid["range1"]={}
    for n in range(5):
        bonus_grid["range1"][n]= score_list[0][n]
    
    bonus_grid["range2"]={}
    for n in range(6):
        bonus_grid["range2"][n] = score_list[1][n]
    
    bonus_grid["range3"]={}
    for n in range(10):
        bonus_grid["range3"][n] = score_list[2][n]

    bonus_grid["range4"]={}
    for n in range(20):
        bonus_grid["range4"][n] = score_list[3][n]

    return bonus_grid

def __bonusPerPositionAndTime(numberOf_contest_participants):
    '''
    @return: dictionary = {key1: {key2: bonusValue}}
                        key1: participant position (ranking) in a contest
                        key2: times the participant enter the top 90% in previous contests (0-n)
                        bonusValue: bonus for that contest position and top 90% times.
    
    '''
    positions = [ n for n in range(1, __locPercentile(numberOf_contest_participants) + 1)]
    bonus_per_position_and_time = {}
    bonus_grid = __bonusGrid()

    for position in positions:
    
        if  (1 <= position <= 5):
            bonus_per_position_and_time[position] = bonus_grid["range1"]
        
        if (6 <= position <= 10):
            bonus_per_position_and_time[position] = bonus_grid["range2"]

        if (11 <= position <= (11 + __rangeCalculation(numberOf_contest_participants)) + 1):
            bonus_per_position_and_time[position] = bonus_grid["range3"]

        if (11 + __rangeCalculation(numberOf_contest_participants) + 1 <= position\
             <= __locPercentile(numberOf_contest_participants) + 1):
            bonus_per_position_and_time[position] = bonus_grid["range4"]

    return bonus_per_position_and_time

def __isBonusCandidate(contest_ranking_position, numberOf_contest_participants):
    """
    TODO: contest_ranking_position hay que obtenerlo de DATABASE (PEDIRLO EN MAIN)
    return: Boolean
    """

    if contest_ranking_position <= __locPercentile(numberOf_contest_participants,p=0.90):
        return True
    else:
        return False

def bonusCalculator(contest_ranking_position, numberOf_contest_participants, times_in_top):
    """
    TODO: contest_ranking_position, contest_participants, times_in_top
     hay que obtenerlo de DATABASE (PEDIRLO EN MAIN)
    
    TODO: output Bonus hay que actualizarlo en DATABASE
    """
    bonus_perPosition_andTimesInTop = __bonusPerPositionAndTime(numberOf_contest_participants)

    if __isBonusCandidate(contest_ranking_position, numberOf_contest_participants):
        return times_in_top + 1, bonus_perPosition_andTimesInTop[contest_ranking_position][times_in_top]

    else: #no bonus
        return times_in_top, 0

def contestBasePoints(contest_ranking_position, numberOf_contest_participants):

    """
    TODO: contest_ranking_position hay que obtenerlo de DATABASE (PEDIRLO EN MAIN)
    TODO: contest participants hay que obtenerlo de DATABASE (PEDIRLO EN MAIN)
    TODO: output es una columna en TABLA RANKING HISTORICO (DATABASE)
    """

    contest_base_points = (1-((contest_ranking_position-1)/numberOf_contest_participants))*100
    return round(contest_base_points, 2)         

def contestPoints(contest_ranking_position, numberOf_contest_participants, times_in_top):

    """
    TODO: output es una columna en TABLA RANKING HISTORICO (DATABASE)
    """
    _,bonus = bonusCalculator(contest_ranking_position, numberOf_contest_participants, times_in_top)

    contest_total_points = contestBasePoints(contest_ranking_position, numberOf_contest_participants)\
                             + bonus

    return round(contest_total_points, 2)

def first_contest(new_data_storage, first_content_data, number_of_participants):
    for user_id in first_content_data.keys():

        ranking_position = float(first_content_data[user_id]['rank'])
        id_user = first_content_data[user_id]['id_user']
        id_contest = 1 
        base_points = contestBasePoints(ranking_position, number_of_participants)
        times_in_top = 0
        total_contest_points = contestPoints(ranking_position, number_of_participants,times_in_top)
        contests_count = 1
        new_times_in_top, bonus = bonusCalculator(ranking_position, number_of_participants\
                                                ,times_in_top)

        user_cumulated_points = total_contest_points
        is_contest_participant = 'Y'
    
        new_user_row = {'id_contest':id_contest, 'id_user':id_user, 'contest_base_points':base_points, 'bonus': bonus\
                        ,'contest_total_points': total_contest_points, 'user_cumulated_points': user_cumulated_points\
                        ,'contests_by_user_count':contests_count,'is_contest_participant':is_contest_participant\
                        ,'top_counter':new_times_in_top}   
            
        new_data_storage[id_user] = new_user_row

    return new_data_storage

def rankContestParticipants(userProcessedData):
    
    ordered = userProcessedData.values()
    ordered = sorted(ordered, key= lambda d: d['user_cumulated_points'],reverse=True)
    i = 0
    old_data = None
    for data in ordered:
        if old_data == None or old_data['user_cumulated_points'] != data['user_cumulated_points']:
            i +=1
        
        data['historic_rank'] = i
        old_data = data
           
    return ordered
 
if __name__ == '__main__':
    
    data= {1: {'user_cumulated_points': Decimal(34), 'col2': 'B'}, 2: {'user_cumulated_points': Decimal(20),'col2': 'D'}}
    print(rankContestParticipants(data))

print(contestPoints(45, 245, 1))