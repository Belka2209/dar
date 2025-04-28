import sqlite3
from typing import Optional, Dict, Any, Union
import psycopg2  # Для PostgreSQL
import pymysql  # Для MySQL
import cx_Oracle  # Для Oracle
import pyodbc  # Для MS SQL Server и других ODBC-совместимых СУБД


class DatabaseConnector:
    """Универсальный коннектор для работы с различными СУБД"""

    def __init__(self, db_type: str, **connection_params):
        """
        Инициализация подключения к БД

        :param db_type: Тип СУБД (sqlite, postgresql, mysql, oracle, mssql)
        :param connection_params: Параметры подключения, специфичные для каждой СУБД
        """
        self.db_type = db_type.lower()
        self.connection_params = connection_params
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        """Установка соединения с БД"""
        try:
            if self.db_type == "sqlite":
                self.connection = sqlite3.connect(**self.connection_params)
            elif self.db_type == "postgresql":
                self.connection = psycopg2.connect(**self.connection_params)
            elif self.db_type == "mysql":
                self.connection = pymysql.connect(**self.connection_params)
            elif self.db_type == "oracle":
                self.connection = cx_Oracle.connect(**self.connection_params)
            elif self.db_type == "mssql":
                self.connection = pyodbc.connect(**self.connection_params)
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            self.cursor = self.connection.cursor()
            print(f"Successfully connected to {self.db_type} database")

        except Exception as e:
            print(f"Connection error: {str(e)}")
            raise

    def execute_query(
        self, query: str, params: Optional[tuple] = None, fetch: bool = True
    ) -> Optional[list]:
        """
        Выполнение SQL запроса

        :param query: SQL запрос
        :param params: Параметры для запроса
        :param fetch: Флаг, указывающий нужно ли получать результат
        :return: Результат запроса или None
        """
        try:
            self.cursor.execute(query, params or ())
            if fetch:
                return self.cursor.fetchall()
            return None
        except Exception as e:
            print(f"Query execution error: {str(e)}")
            self.connection.rollback()
            raise

    def close(self) -> None:
        """Закрытие соединения с БД"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Database connection closed")

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Поддержка контекстного менеджера"""
        self.close()
