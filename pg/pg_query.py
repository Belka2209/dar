import logging
import asyncpg
import psycopg2
from pg.pg_con import dbConnection
from functools import wraps

db = dbConnection()


def handle_asyncpg_errors(tablename: str = None):
    """
    Декоратор для обработки ошибок asyncpg

    Args:
        tablename (str, optional): Имя таблицы для логирования. По умолчанию None.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                # Логирование успешного выполнения можно добавить здесь
                # logging.info(f"Успешное выполнение для таблицы {tablename}")
                return result
            except asyncpg.exceptions.UndefinedTableError:
                error_msg = (
                    f"Таблица {tablename} не существует."
                    if tablename
                    else "Таблица не существует."
                )
                logging.error(error_msg)
                raise
            except asyncpg.exceptions.SyntaxOrAccessError as ex:
                error_msg = f"Ошибка синтаксиса SQL ({func.__name__}): {ex}"
                logging.error(error_msg)
                raise
            except asyncpg.exceptions.ConnectionDoesNotExistError:
                logging.error("Нет подключения к серверу.")
                raise
            except asyncpg.exceptions.PostgresError as ex:
                error_msg = f"Ошибка базы данных ({func.__name__}): {ex}"
                if tablename:
                    error_msg += f" (таблица: {tablename})"
                logging.error(error_msg)
                raise
            except psycopg2.errors.InsufficientPrivilege as ex:
                error_msg = f"Недостаточно прав для доступа к таблице {tablename}: {ex}"
                logging.error(error_msg)
                raise
            except psycopg2.errors.UndefinedTable as ex:
                error_msg = f"Таблица {tablename} не определена: {ex}"
                logging.error(error_msg)
                raise
            except Exception as ex:
                # Обработка любых других исключений
                logging.error(f"Неизвестная ошибка ({func.__name__}): {ex}")
                raise

        return wrapper

    return decorator


@handle_asyncpg_errors()
async def check_data_in_table(tablename, where_columns, value_columns):
    """Получаю True или False в зависимость есть ли значение  в колонке"""
    query = f"""SELECT EXISTS(SELECT 1 FROM {tablename} WHERE {where_columns} = %s)"""
    value = (value_columns,)
    res = db.execute_dict_one_record(query, value)
    return res.get("exists")


@handle_asyncpg_errors()
async def get_data_table(tablename):
    """Получаю все данные из таблицы"""
    query = f"""SELECT * FROM {tablename}"""
    res = db.execute_list_all_record(query)
    return res


# @handle_asyncpg_errors("tasks")
def save_data_in_tasks(
    task_id, task_name, task_description, start_comment_id, start_comment_text, context
):
    query = """INSERT INTO tasks (
            task_id,
            task_name,
            task_description,
            start_comment_id,
            start_comment_text,
            context
        )
        VALUES (%s, %s, %s, %s, %s, %s)"""
    value = (
        task_id,
        task_name,
        task_description,
        start_comment_id,
        start_comment_text,
        context,
    )
    db.execute(query, value)
    return True


@handle_asyncpg_errors()
async def get_data_table_where(tablename, column, value_columns):
    """Получаю все данные из таблицы по условию where"""
    query = f"""SELECT * FROM {tablename} WHERE {column} = %s"""
    value = (value_columns,)
    role_dict = db.execute_dict_one_record(query, value)
    return role_dict


@handle_asyncpg_errors()
async def save_different_data_in_table(
    tablename, column_where, value_where, column, value
):
    # Изменяю запись в таблицу
    base_query = (
        f"UPDATE {tablename} SET {column} = %s WHERE {column_where} = {value_where}"
    )
    value = (value,)
    query = base_query.format(value)
    db.execute(query, value)


# ANCHOR - не из этого приложения
# async def get_data_table_where_dict(tablename, column, value_columns):
#     """Получаю одну запись из таблицы по условию where"""
#     query = f"""SELECT * FROM {tablename} WHERE {column} = %s"""
#     value = (value_columns,)
#     # print("query", query)
#     try:
#         data_dict = db.execute_dict_one_record(query, value)
#         # print("res_data", data_dict)
#         # db.execute(query, value)
#         print("data_dict", data_dict)
#     except asyncpg.exceptions.UndefinedTableError:
#         logging.error(f"Таблица {tablename} не существует.")
#         raise
#     except asyncpg.exceptions.SyntaxOrAccessError as ex:
#         logging.error(f"Ошибка синтаксиса SQL save_data_in_table_task: {ex}")
#         raise
#     except asyncpg.exceptions.ConnectionDoesNotExistError:
#         logging.error("Нет подключения к серверу.")
#         raise
#     except asyncpg.exceptions.PostgresError as ex:
#         logging.error(f"Ошибка загрузки данных в таблицу {tablename}: {ex}")
#         raise
#     else:
#         logging.info(f"Данные получены из таблицы {tablename}")
#         return data_dict


# async def get_all_data_table_dict(tablename):
#     """Получаю все данные из таблицы"""
#     query = f"""SELECT * FROM {tablename}"""

#     # print("query", query)
#     try:
#         data_dict = db.execute_list_all_record(query)
#         # print("res_data", data_dict)
#         # db.execute(query, value)
#     except asyncpg.exceptions.UndefinedTableError:
#         logging.error(f"Таблица {tablename} не существует.")
#         raise
#     except asyncpg.exceptions.SyntaxOrAccessError as ex:
#         logging.error(f"Ошибка синтаксиса SQL get_all_data_table_dict: {ex}")
#         raise
#     except asyncpg.exceptions.ConnectionDoesNotExistError:
#         logging.error("Нет подключения к серверу.")
#         raise
#     except asyncpg.exceptions.PostgresError as ex:
#         logging.error(f"Ошибка загрузки данных в таблицу {tablename}: {ex}")
#         raise
#     else:
#         logging.info(f"Данные получены из таблицы {tablename}")
#         return data_dict


# async def get_all_data_table_dict_where(tablename, column, value_columns, column2):
#     """Получаю все данные из таблицы по условию where"""
#     query = f"""SELECT contractor_task, customer_task, customer_elapseditem_contractor FROM {tablename} WHERE {column} = %s and {column2} = True """
#     value = (value_columns,)
#     # print("query", query)
#     try:
#         data_dict = db.execute_list_all_record(query, value)
#         # print("res_data", data_dict)
#         # db.execute(query, value)
#     except asyncpg.exceptions.UndefinedTableError:
#         logging.error(f"Таблица {tablename} не существует.")
#         raise
#     except asyncpg.exceptions.SyntaxOrAccessError as ex:
#         logging.error(f"Ошибка синтаксиса SQL get_all_data_table_dict_where: {ex}")
#         raise
#     except asyncpg.exceptions.ConnectionDoesNotExistError:
#         logging.error("Нет подключения к серверу.")
#         raise
#     except asyncpg.exceptions.PostgresError as ex:
#         logging.error(f"Ошибка загрузки данных в таблицу {tablename}: {ex}")
#         raise
#     else:
#         logging.info(f"Данные получены из таблицы {tablename}")
#         return data_dict


# # async def get_data_table_where(tablename, column, where_columns, value_columns):
# #     """Получаю все данные из таблицы по условию where"""
# #     query = f"""SELECT {column} FROM {tablename} WHERE {where_columns} = %s"""
# #     value = (value_columns,)
# #     res = db.fetchall(query, value)
# #     res = res[0][0]
# #     return res


# async def get_data_id_task_in_table(where_columns, value_columns):
#     """Получаю True или False в зависимость есть ли номер задачи в колонке"""
#     query = f"""SELECT EXISTS(SELECT 1 FROM task WHERE {where_columns} = %s and is_deleted = True)"""
#     value = (value_columns,)
#     res = db.execute_dict_one_record(query, value)
#     res = res.get("exists")
#     return res


# async def check_data_in_table_identification(where_columns, value_columns):
#     """Получаю True или False в зависимость есть ли значение  в колонке"""
#     query = (
#         f"""SELECT EXISTS(SELECT 1 FROM identification WHERE {where_columns} = %s)"""
#     )
#     value = (value_columns,)
#     res = db.execute_dict_one_record(query, value)
#     res = res.get("exists")
#     return res


# async def check_data_in_table_identification_if_two_where(
#     where_columns, where_columns_two, value_columns, value_columns_two
# ):
#     """Получаю True или False в зависимость есть ли значение в колонке при двух условиях"""
#     query = f"""SELECT EXISTS(SELECT 1 FROM identification WHERE {where_columns} = %s and {where_columns_two} = %s)"""
#     value = (
#         value_columns,
#         value_columns_two,
#     )
#     res = db.execute_dict_one_record(query, value)
#     res = res.get("exists")
#     return res


# async def get_data_in_table_identification_if_two_where(
#     where_columns, where_columns_two, value_columns, value_columns_two
# ):
#     """Получаю Словарь в зависимость есть ли значение в колонке при двух условиях"""
#     query = f"""SELECT * FROM identification WHERE {where_columns} = %s and {where_columns_two} = %s"""
#     value = (
#         value_columns,
#         value_columns_two,
#     )
#     res = db.execute_dict_one_record(query, value)
#     # res = res.get("exists")
#     return res


# async def get_data_id_comment_in_table(where_columns, value_columns):
#     """Получаю True или False в зависимость есть ли номер задачи в колонке"""
#     query = f"""SELECT EXISTS(SELECT 1 FROM comments WHERE {where_columns} = %s)"""
#     value = (value_columns,)
#     res = db.execute_dict_one_record(query, value)
#     res = res.get("exists")
#     return res


# async def get_data_portal_in_table_identification(where_columns, value_columns):
#     """Получаю True или False в зависимость есть ли номер задачи в колонке"""
#     query = (
#         f"""SELECT EXISTS(SELECT 1 FROM identification WHERE {where_columns} = %s)"""
#     )
#     value = (value_columns,)
#     res = db.execute_dict_one_record(query, value)
#     res = res.get("exists")
#     return res


# async def save_data_in_table_task(uuid, id_task_enter, id_task_outgoing):
#     # Добавляю запись в таблицу task
#     base_query = (
#         "INSERT INTO task (uuid, customer_task, contractor_task) VALUES (%s, %s, %s)"
#     )
#     value = (uuid, id_task_enter, id_task_outgoing)
#     query = base_query.format(value)
#     try:
#         db.execute(query, value)
#     except asyncpg.exceptions.UndefinedTableError:
#         logging.error("Таблица task не существует.")
#         raise
#     except asyncpg.exceptions.SyntaxOrAccessError as ex:
#         logging.error(f"Ошибка синтаксиса SQL save_data_in_table_task: {ex}")
#         raise
#     except asyncpg.exceptions.ConnectionDoesNotExistError:
#         logging.error("Нет подключения к серверу.")
#         raise
#     except asyncpg.exceptions.PostgresError as ex:
#         logging.error(f"Ошибка загрузки данных в таблицу task: {ex}")
#         raise
#     else:
#         logging.info("Данные добавлены в таблицу task")
#         return True


# async def save_different_data_in_table_task(column_where, value_where, column, value):
#     # Изменяю запись папки в таблицу task
#     base_query = f"UPDATE task SET {column} = %s WHERE {column_where} = {value_where}"
#     value = (value,)
#     query = base_query.format(value)
#     # logging.info(f"Данные папки добавлены в таблицу task {base_query}")
#     # logging.info(f"Данные папки добавлены в таблицу task {query}")
#     try:
#         db.execute(query, value)
#     except asyncpg.exceptions.UndefinedTableError:
#         logging.error("Таблица task не существует.")
#         raise
#     except asyncpg.exceptions.SyntaxOrAccessError as ex:
#         logging.error(f"Ошибка синтаксиса SQL save_different_data_in_table_task: {ex}")
#         raise
#     except asyncpg.exceptions.ConnectionDoesNotExistError:
#         logging.error("Нет подключения к серверу.")
#         raise
#     except asyncpg.exceptions.PostgresError as ex:
#         logging.error(f"Ошибка загрузки данных в таблицу task: {ex}")
#         raise
#     else:
#         logging.info("Данные папки добавлены в таблицу task")
#         return True


# # async def save_data_in_table_comments(uuid, id_comment_enter, id_comment_outgoing):
# #     # Добавляю запись в таблицу task
# #     base_query = "INSERT INTO comments (uuid, customer_comment, contractor_comment) VALUES (%s, %s, %s)"
# #     value = (uuid, id_comment_enter, id_comment_outgoing)
# #     query = base_query.format(value)
# #     try:
# #         db.execute(query, value)
# #     except asyncpg.exceptions.UndefinedTableError:
# #         logging.error("Таблица task не существует.")
# #         raise
# #     except asyncpg.exceptions.SyntaxOrAccessError as ex:
# #         logging.error(f"Ошибка синтаксиса SQL save_data_in_table_task: {ex}")
# #         raise
# #     except asyncpg.exceptions.ConnectionDoesNotExistError:
# #         logging.error("Нет подключения к серверу.")
# #         raise
# #     except asyncpg.exceptions.PostgresError as ex:
# #         logging.error(f"Ошибка загрузки данных в таблицу task: {ex}")
# #         raise
# #     else:
# #         logging.info("Данные добавлены в таблицу task")
# #         return True


# # async def del_comment_in_table_comments(comment_id):
# #     base_query = f"DELETE FROM comments WHERE customer_comment = {comment_id} or contractor_comment = {comment_id}"
# #     try:
# #         db.execute(base_query)
# #     except asyncpg.exceptions.UndefinedTableError:
# #         logging.error("Таблица task не существует.")
# #         raise
# #     except asyncpg.exceptions.SyntaxOrAccessError as ex:
# #         logging.error(f"Ошибка синтаксиса SQL save_data_in_table_task: {ex}")
# #         raise
# #     except asyncpg.exceptions.ConnectionDoesNotExistError:
# #         logging.error("Нет подключения к серверу.")
# #         raise
# #     except asyncpg.exceptions.PostgresError as ex:
# #         logging.error(f"Ошибка загрузки данных в таблицу task: {ex}")
# #         raise
# #     else:
# #         logging.info("Данные добавлены в таблицу task")
# #         return True


# # async def get_list_contractor_portal():
# #     query = "SELECT DISTINCT contractor_portal FROM identification"
# #     try:
# #         res = db.fetchall(query)
# #         result_list = list(res[0])
# #         print("get_list_contractor_portal", result_list)

# #     except asyncpg.exceptions.UndefinedTableError:
# #         logging.error("Таблица identification не существует.")
# #         raise
# #     except asyncpg.exceptions.SyntaxOrAccessError as ex:
# #         logging.error(f"Ошибка синтаксиса SQL get_list_contractor_portal: {ex}")
# #         raise
# #     except asyncpg.exceptions.ConnectionDoesNotExistError:
# #         logging.error("Нет подключения к серверу.")
# #         raise
# #     except asyncpg.exceptions.PostgresError as ex:
# #         logging.error(f"Ошибка загрузки данных в таблицу identification: {ex}")
# #         raise
# #     else:
# #         logging.info("Данные get_list_contractor_portal получены")
# #         return result_list
