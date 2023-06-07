from confluent_kafka import Consumer
#from google.cloud import bigquery
import ast
#from google.oauth2 import service_account
import mysql.connector as MySQLdb


################ Kafka Consumer #################
c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'tony-allen-consumer','auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

#Subscribe to topic
c.subscribe(['tony-allen-plays'])

def main():
    try:
        while True:
            msg=c.poll(timeout=1.0)  
            if msg is None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')

                res = ast.literal_eval(data)
                print(res)
                

        
                rows_to_insert = [res]
                print((rows_to_insert))
                mysql_connection = MySQLdb.connect(host=, port=3306, user=, passwd=, db=,auth_plugin='mysql_native_password')
                cursor =mysql_connection.cursor()
                sql="INSERT INTO kafka_stream(user_id ,artist, song_id ,event_type ,timestamp) VALUES (%s, %s, %s ,%s ,%s )"
                for row in rows_to_insert:
                    values = (row['user_id'],row['artist'],row['song_id'],row['event_type'],row['timestamp'])
                    errors=cursor.execute(sql,values)
                mysql_connection.commit()
                
                if errors==[]:
                    print("New rows added.")
                else:
                    print("Encountered erros while inserting rows: {}".format(errors))
    finally:
        c.close() 

if __name__ == "__main__":
    main()