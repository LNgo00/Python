from decimal import Decimal
from DataBase_connection import open_connection, close_connection
from DataBase_user_contests import userContestDataTransformer, participantsCount, ScriptVariables
import sys
from DataBase_contests_historical_rank import contestsHistoricalRankingDataTransformer, insertContestHistoricalRank
from historical_score_1 import bonusCalculator, contestBasePoints, contestPoints, rankContestParticipants, first_contest

def historicalUpdate(connection, contest_rank_by_idUser,number_of_contest_participants,historical_records, id_contest):

    new_historical_db_data = {}    
    
    if id_contest == 1:
        first_contest(new_historical_db_data, contest_rank_by_idUser, number_of_contest_participants)

    else:

        #user contest participants (all users) with default data        

        for user_id in historical_records.keys():
        
            base_points = 0
            bonus = 0
            total_contest_points = 0
            user_cumulated_points = historical_records[user_id]['user_cumulated_points']
            contests_count = historical_records[user_id]['contests_by_user_count']
            is_contest_participant = 'N'
            times_in_top = historical_records[user_id]['top_counter']
            
            new_db_entry = {'id_contest':id_contest, 'id_user':user_id, 'contest_base_points':base_points, 'bonus': bonus\
                            ,'contest_total_points': total_contest_points, 'user_cumulated_points': user_cumulated_points\
                            ,'contests_by_user_count':contests_count,'is_contest_participant':is_contest_participant\
                            ,'top_counter':times_in_top}    

            new_historical_db_data[user_id] = new_db_entry

        #for participants in the last contest to update:
    
        for user_id in contest_rank_by_idUser.keys():

            ranking_position = contest_rank_by_idUser[user_id]['rank'] 
            id_user = contest_rank_by_idUser[user_id]['id_user']

            #is_new_participant = isNewParticipant(connection, id_user, id_contest)
            is_new_participant = id_user not in historical_records

            base_points = contestBasePoints(ranking_position, number_of_contest_participants) 
            
            if is_new_participant:
                
                times_in_top = 0
                user_cumulated_points = contestPoints(ranking_position, number_of_contest_participants,times_in_top)
                contests_count = 1
                new_times_in_top, bonus = bonusCalculator(ranking_position, number_of_contest_participants\
                                        ,times_in_top)
                total_contest_points = contestPoints(ranking_position, number_of_contest_participants\
                                                    ,times_in_top)
                is_contest_participant = 'Y'
                
                new_user_row = {'id_contest':id_contest, 'id_user':id_user, 'contest_base_points':base_points, 'bonus': bonus\
                            ,'contest_total_points': total_contest_points, 'user_cumulated_points': user_cumulated_points\
                            ,'contests_by_user_count':contests_count,'is_contest_participant':is_contest_participant\
                            ,'top_counter':new_times_in_top}   
                
                new_historical_db_data[id_user] = new_user_row

            else:                                                                                                                        
                times_in_top = historical_records[id_user]['top_counter']
                last_historical_points = float(historical_records[id_user]['user_cumulated_points'])
                user_contest_points = contestPoints(ranking_position, number_of_contest_participants,times_in_top) 
                user_cumulated_points = user_contest_points + last_historical_points
                contests_count = historical_records[id_user]['contests_by_user_count'] + 1                                

                new_times_in_top,bonus = bonusCalculator(ranking_position, number_of_contest_participants\
                                                        ,times_in_top)
                total_contest_points = contestPoints(ranking_position, number_of_contest_participants, times_in_top)

                is_contest_participant = 'Y'

                updated_row = {'id_contest':id_contest, 'id_user':id_user, 'contest_base_points':base_points, 'bonus': bonus\
                            ,'contest_total_points': total_contest_points, 'user_cumulated_points': user_cumulated_points\
                            ,'contests_by_user_count':contests_count,'is_contest_participant':is_contest_participant\
                            ,'top_counter':new_times_in_top}                  
                
                new_historical_db_data.update({id_user: updated_row })

    new_historical_db_data = rankContestParticipants(new_historical_db_data)    
    insertContestHistoricalRank(connection, new_historical_db_data)
        
    return new_historical_db_data

if __name__ == '__main__':

    connection = open_connection()
    
    #args = sys.argv[1:]

    contest_id_variables = ScriptVariables(connection)

    #read data: 
    #current_contest = ScriptVariables[0]['id_contest']
    current_contest = 9
    old_contest = 8
    contest_rank_by_idUser = userContestDataTransformer(connection, current_contest )#args)   
    number_of_contest_participants = participantsCount(connection,current_contest ) #args)

    # args[2] debe ser id del ultimo concurso computado en la base (el anterior al actual)
    historical_records = contestsHistoricalRankingDataTransformer(connection,  old_contest)#args[2])
   
    print(historicalUpdate(connection=connection, contest_rank_by_idUser=contest_rank_by_idUser\
                            ,number_of_contest_participants = number_of_contest_participants\
                            ,historical_records=historical_records,id_contest=current_contest))

    '''
    print('record elements: ', len(new_historical_db_data))  
    print('dictionary size: ', sys.getsizeof(historicalUpdate))  
    '''
    close_connection(connection)

