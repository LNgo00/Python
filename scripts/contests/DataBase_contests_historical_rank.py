
import mysql.connector as db
from mysql.connector import Error
from DataBase_connection import open_connection, close_connection
from decimal import Decimal

##TODO: setear mensaje de error si se indica Id_contest que no existe en tabla(Sql retorna record vacio)

#3.- funcion read tabla contests_historical(id_contest-1) --> return (id_historico, id_user, times_in_top, cumulatedHistoricalPoints)

def __readContestHistoricalRank(connection, id_contest):
    '''
    para actualizar field "cumulated points" de los usuarios que participaron en el ultimo contest: 
    (leer id_contest = id_ctual-1)
    para calcular bonus de concurso actual: top_counter
    '''
    
    try:
        cursor = connection.cursor(dictionary=True)    
        sql_select_query = "select id_user, user_cumulated_points, top_counter, contests_by_user_count from contests_historical_rank where id_contest = %s"
        cursor.execute(sql_select_query, (id_contest,))
        return cursor.fetchall()
        
    except db.Error as error:

        print("failed to get record from MySQL database:{}".format(error))   
    
    finally:
        cursor.close()

def contestsHistoricalRankingDataTransformer(connection, id_contest):
    '''
    @return: list of dicts obtained from MySQL is indexed to a dictionary where:
        dictionary_key = id_user
        dictionary_values = MySQL data (other dictionary)

    '''
    try:
        data_to_transform = __readContestHistoricalRank(connection, id_contest)
        data_dictionary = {}

        for row in data_to_transform:
            data_dictionary[row['id_user']]=row

        return data_dictionary

    except db.Error as error:

        print("Failed to transform data obtained at MySQL: {}".format(error))

def insertContestHistoricalRank(connection, new_db_data):
    '''
    TODO complete doc
    '''    
    try:
        cursor = connection.cursor(dictionary=True)
        sql_insert_query = ("INSERT INTO cryptobirds.contests_historical_rank "
            "(id_contest, id_user, contest_base_points, bonus, contest_total_points, user_cumulated_points,"
            "contests_by_user_count, is_contest_participant, top_counter, historic_rank) "
            "VALUES "
            "(%(id_contest)s, %(id_user)s, %(contest_base_points)s, %(bonus)s, %(contest_total_points)s, %(user_cumulated_points)s, "
            "%(contests_by_user_count)s, %(is_contest_participant)s, %(top_counter)s, %(historic_rank)s)")
        
        cursor.executemany(sql_insert_query, new_db_data)
        connection.commit()
        
    except db.Error as error:

        connection.rollback()
        print("failed to insert data from MySQL database:{}".format(error))   
        raise error

    finally:
        cursor.close()


if __name__ == '__main__':

    connection = open_connection()
    print("connection open")
    print("step 1")
    #print(__readContestHistoricalRank(connection, 6))
    #print(contestsHistoricalRankingDataTransformer(connection, 6))
    data = {1: {'id_contest': 7, 'id_user': 4312, 'contest_base_points': 0.27, 'bonus': 0, 'contest_total_points': 0.27, 'user_cumulated_points': 0.27, 'contests_by_user_count': 1, 'is_contest_participant': 'Y', 'top_counter': 0},\
            2: {'id_contest': 7, 'id_user': 961, 'contest_base_points': 0, 'bonus': 0, 'contest_total_points': 0, 'user_cumulated_points': Decimal('0.52'), 'contests_by_user_count': 1, 'is_contest_participant': 'N', 'top_counter': 0}   }
    #data = [{'id_contest': 7, 'id_user': 4312, 'contest_base_points': 0.27, 'bonus': 0, 'contest_total_points': 0.27, 'user_cumulated_points': 0.27, 'contests_by_user_count': 1, 'is_contest_participant': 'Y', 'top_counter': 0},\
    #        {'id_contest': 7, 'id_user': 961, 'contest_base_points': 0, 'bonus': 0, 'contest_total_points': 0, 'user_cumulated_points': Decimal('0.52'), 'contests_by_user_count': 1, 'is_contest_participant': 'N', 'top_counter': 0}]
 
    insertContestHistoricalRank(connection, data)
    print("data inserted")
    print("step 2")
    close_connection(connection)
    print("connection closed")


    
