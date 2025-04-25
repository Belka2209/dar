import psycopg2

import os
import dotenv
from psycopg2 import sql
import psycopg2.extras


dotenv.load_dotenv()
getenv = os.getenv


username = 'sa'
password = 220990
database = 'mydatabase'
# host = "amvera-ludmila-cnpg-panda-rw"
host = "panda-ludmila.amvera.io"
# host = "panda-ludmila.db-msk0.amvera.tech"
port = 5432


# print("db", username, password, database, host, port)
class dbConnection:
    # чтение параметров подключения
    def __init__(self):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.database = database

        self.conn = None
        self.cursor = None

    # подключение к БД
    def connect(self):
        self.conn = None
        try:
            # print("databases", databases)
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                port=self.port,
            )
            self.conn.autocommit = True
            # self.cursor = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as err:
            print(f"Database connection error:  {err}")

    # closing the connection\закрытие соединения
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
        self.conn = None

    # commiting query\запрос на фиксацию
    def commit(self):
        self.conn.commit()

    # rollbacking query\откат запроса
    def rollback(self):
        self.conn.rollback()

    # executing querys drop, create, insert,... \выполнение запросов
    def execute(self, query, args=None):
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        try:
            curs.execute(query, args)
            self.commit()
        except Exception as ex:
            self.conn.rollback()
            curs.close()
            raise ex

        return curs

    def execute_dict_one_record(self, query, args=None):
        """Получаем одную строку в виде словаря"""
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        try:
            curs.execute(query, args)
            # Получение названий столбцов
            column_names = [desc[0] for desc in curs.description]
            # Получение данных
            row = curs.fetchone()
            result_dict = dict(zip(column_names, row))
            self.commit()
        except Exception as ex:
            self.conn.rollback()
            curs.close()
            raise ex
            # result_dict = False
        finally:
            curs.close()
        # print("result_dict", result_dict)
        return result_dict

    def execute_list_all_record(self, query, args=None):
        """Получаем все строки в виде списка словарей"""
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            curs.execute(query, args)
            rows = curs.fetchall()
            result = [dict(row) for row in rows]
            curs.close()
        except Exception as ex:
            self.conn.rollback()
            curs.close()
            raise ex

        return result

    # executing query COUNT, SUM, MIN, ...\выполнение запросов
    def fetchone(self, query, args=None):
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        curs = self.execute(database, query, args)
        row = curs.fetchone()
        curs.close()
        return row

    # executing query returning more rows than one\выполнение запроса возвращающего более одной строки
    def fetchall(self, query, args=None):
        curs = self.execute(query, args)
        rows = curs.fetchall()
        curs.close()
        return rows

    # copying records of the table to the file\копирование записей таблицы в файл
    def copy_to(self, path_file, table_name, sep=","):
        if self.conn is None or self.conn.closed:
            self.connect()
        with open(path_file, "w+") as f:
            curs = self.conn.cursor()
            try:
                curs.copy_to(f, table_name, sep)
            except Exception:
                curs.close()
                raise Exception("Problem with writing to the file ".format(path_file))

    # copying records from the file to the table\копирование записей из файла в таблицу
    def copy_from(self, path_file, table_name, database, sep=","):
        if self.conn is None or self.conn.closed:
            self.connect(database)
        with open(path_file, "r") as f:
            curs = self.conn.cursor()
            try:
                curs.copy_from(f, table_name, sep)
            except Exception:
                curs.close()
                raise Exception(
                    "Problem with copying from the file {0} to the table {1}".format(
                        path_file, table_name
                    )
                )

    # Проверка на наличие  в БД
    def checking_for_db(self, table, field, field_value):
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        try:
            curs.execute(
                f"""SELECT EXISTS (SELECT 1 FROM {table} WHERE {field} = %s)""",
                (field_value,),
            )
            exists = curs.fetchone()[0]
        except Exception as ex:
            self.conn.rollback()
            curs.close()
            raise ex
        return exists  # Вернет True, если объект существует, иначе False

    # Получаем значение из БД
    def get_date_pg(self, get_value, table, field, field_value):
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        try:
            curs.execute(
                f"""SELECT {get_value} FROM {table} WHERE {field} = %s""",
                (field_value,),
            )
            exists = curs.fetchone()
        except Exception as ex:
            self.conn.rollback()
            curs.close()
            raise ex
        return exists

    async def create_database(self, login):
        """Создание базы данных."""
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        try:
            create_db_query = sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(login)
            )
            curs.execute(create_db_query)
            print(f"База данных '{login}' успешно создана.")
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
        finally:
            curs.close()

    # Проверка на наличие  в БД
    async def checking_for_db_record(self, query):
        if self.conn is None or self.conn.closed:
            self.connect()
        curs = self.conn.cursor()
        try:
            curs.execute(query)
            exists = curs.fetchone()[0]
        except Exception as ex:
            self.conn.rollback()
            curs.close()
            raise ex
        return exists  # Вернет True, если объект существует, иначе False
