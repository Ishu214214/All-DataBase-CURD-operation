import mysql.connector
from mysql.connector import Error


class Mysql_connection:
    def __init__(self):
        self.read_hostname = '127.0.0.1'
        self.read_db_name = 'Student'
        self.read_username = 'root'
        self.read_password = ''

    def getConnection(self):
        try:
            connection = mysql.connector.connect(host=self.read_hostname, database=self.read_db_name,
                                                 user=self.read_username,
                                                 password=self.read_password)

            if connection.is_connected():
                print("mysql db connected")
                return connection

        except Error as e:
            print(f"\033[41m"  + "*" * 10 + "Error while connecting to MySQLi: == {e}"  + "*"*10 +"\033[0m" )
        return None


    def delete_records_by_tokens(self, tokens):
        connection = self.getConnection()
        if connection is None:
            return

        try:
            cursor = connection.cursor()
            # Prepare the query to delete records based on tokens
            query = "DELETE FROM ubuy_pushnotificationappdeviceids WHERE device_tokenid IN ({})".format(
                ','.join(map(lambda x: "'" + x + "'", tokens))
            )
            print(f"\033[34m  {query}  \033[0m" )
            cursor.execute(query)
            connection.commit()
            print(f"Deleted {cursor.rowcount} rows")

        except Error as e:
            print(f"\033[41m"  + "*" * 10 + "Error deleting records:: == {e}"  + "*"*10 +"\033[0m" )

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")


    def fetch_data_with_limit_and_condition(self, limit, last_id):
            connection = self.getConnection()
            if connection is None:
                return None

            try:
                cursor = connection.cursor(dictionary=True)
                # Prepare the query to fetch records with a limit and condition
                query = "SELECT * FROM ubuy_pushnotificationappdeviceids WHERE id > %s LIMIT %s"
                cursor.execute(query, (last_id, limit))
                rows = cursor.fetchall()
                return rows

            except Error as e:
                print(f"\033[41m"  + "*" * 10 + "Error Featching records:: == {e}"  + "*"*10 +"\033[0m" )
                return None
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()


    def insert_success_tokens(self, tokens):
        connection = self.getConnection()
        if connection is None:
            return

        try:
            cursor = connection.cursor()
            # Prepare the query to insert success tokens
            query = "INSERT INTO ubuy_pushnotificationappdeviceids (device_tokenid, device_type, unique_key, country_name, created_at) VALUES (%s, %s, %s, %s, %s)"
            values = [(token['device_tokenid'], token['device_type'], token['unique_key'], token['country_name'], token['created_at']) for token in tokens]
            print(f"\033[34m  {query} and {values}  \033[0m" )
            cursor.executemany(query, values)
            connection.commit()
            print(f"Inserted {cursor.rowcount} rows")

        except Error as e:
            print(f"\033[41m"  + "*" * 10 + "Error Insert records:: == {e}"  + "*"*10 +"\033[0m" )
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

