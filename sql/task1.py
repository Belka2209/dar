#  import pandas as pd
# from sqlalchemy import create_engine
# db_params = {
#     'host': "panda-ludmila.db-msk0.amvera.tech",
#     'database': 'mydatabase',
#     'user': 'postgres',
#     'password': '220990',
#     'port': '5432'
# }

# # Создаем строку подключения
# connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# # Создаем движок SQLAlchemy
# engine = create_engine(connection_string)
# # Путь к вашему CSV файлу


# csv_file = r"C:\Users\belka\Downloads\dump\statement.csv"

# # Читаем CSV файл
# df = pd.read_csv(csv_file, sep=';')

# df.to_sql('statement', engine, if_exists='replace', index=False)

import pandas as pd
from sqlalchemy import create_engine, text, types

# Настройки подключения к БД

DB_CONFIG = {
    "host": "panda-ludmila.db-msk0.amvera.tech",
    "database": "mydatabase",
    "user": "postgres",
    "password": "220990",
    "port": "5432",
}

# def create_connection():
#     """Создает соединение с PostgreSQL"""
#     connection_string = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
#     return create_engine(connection_string)

# def create_statement_table(engine):
#     """Создает таблицу Ведомость с указанной структурой"""
#     with engine.connect() as conn:
#         conn.execute(text("""
#         CREATE TABLE IF NOT EXISTS statement (
#             id_statement INTEGER,
#             id_student INTEGER NOT NULL,
#             id_lecturer INTEGER NOT NULL,
#             subject TEXT NOT NULL,
#             control TEXT NOT NULL,
#             id_group INTEGER NOT NULL,
#             id_specialty INTEGER NOT NULL,
#             id_ed_plan INTEGER NOT NULL,
#             grade TEXT NOT NULL,
#             date DATE NOT NULL,
#             PRIMARY KEY (id_statement, id_student, subject, date)
#         );
#         """))

# def load_statement_data(engine, csv_file):
#     """Загружает данные из CSV в таблицу"""
#     # Чтение CSV с обработкой даты и разделителем ;
#     df = pd.read_csv(csv_file, sep=';', parse_dates=['date'], dayfirst=True)

#     # Преобразование типов данных
#     df = df.astype({
#         'id_statement': 'int64',
#         'id_student': 'int64',
#         'id_lecturer': 'int64',
#         'id_group': 'int64',
#         'id_specialty': 'int64',
#         'id_ed_plan': 'int64',
#         'grade': 'str'
#     })

#     # Типы данных для SQLAlchemy
#     dtype = {
#         'id_statement': types.Integer(),
#         'id_student': types.Integer(),
#         'id_lecturer': types.Integer(),
#         'subject': types.String(length=100),
#         'control': types.String(length=50),
#         'id_group': types.Integer(),
#         'id_specialty': types.Integer(),
#         'id_ed_plan': types.Integer(),
#         'grade': types.String(length=20),
#         'date': types.Date()
#     }

#     # Загрузка данных в БД
#     df.to_sql(
#         'statement',
#         engine,
#         if_exists='append',
#         index=False,
#         dtype=dtype,
#         chunksize=1000
#     )

# if __name__ == "__main__":
#     try:
#         # Создаем подключение
#         engine = create_connection()

#         # Создаем таблицу
#         create_statement_table(engine)
#         print("Таблица 'statement' создана или уже существует")

#         # Загружаем данные
#         csv_path = r"C:\Users\belka\Downloads\dump\statement.csv"
#         # csv_path = 'statement.csv'
#         load_statement_data(engine, csv_path)
#         print(f"Данные из {csv_path} успешно загружены")

#         # Проверяем количество загруженных записей
#         with engine.connect() as conn:
#             result = conn.execute("SELECT COUNT(*) FROM statement")
#             count = result.scalar()
#             print(f"Всего записей в таблице: {count}")

#     except Exception as e:
#         print(f"Ошибка: {str(e)}")
#     finally:
#         engine.dispose()


def create_connection():
    """Создает соединение с PostgreSQL"""
    connection_string = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string)


# def create_student_table(engine):
#     """Создает таблицу Students с указанной структурой"""
#     with engine.begin() as conn:  # Используем begin() для автоматического коммита
#         conn.execute(
#             text("""
#         CREATE TABLE IF NOT EXISTS students (
#             id_student INTEGER PRIMARY KEY,
#             fio TEXT,
#             dob DATE ,
#             passport TEXT ,
#             student_pass TEXT ,
#             id_specialty INTEGER ,
#             faculty TEXT ,
#             id_group INTEGER ,
#             enrollment_date DATE,
#             date_deduction DATE,
#             scientific_director TEXT
#         )
#         """)
#         )  # Убрана точка с запятой в конце SQL


# def load_student_data(engine, csv_file):
#     """Загружает данные из CSV в таблицу students"""
#     # Чтение CSV с обработкой дат и разделителем ;
#     df = pd.read_csv(
#         csv_file,
#         sep=';',
#         dayfirst=True
#     )

#     # Проверка наличия необходимых столбцов
#     required_columns = ['dob', 'enrollment_date', 'date_deduction']
#     for col in required_columns:
#         if col not in df.columns:
#             raise ValueError(f"Отсутствует необходимый столбец: {col}")

#     # Преобразование дат
#     df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
#     df['enrollment_date'] = pd.to_datetime(df['enrollment_date'], errors='coerce')
#     df['date_deduction'] = pd.to_datetime(df['date_deduction'], errors='coerce')

#     # Преобразование типов данных
#     df = df.astype({
#         'id_student': 'int64',
#         'fio': 'str',
#         'passport': 'str',
#         'student_pass': 'str',
#         'id_specialty': 'int64',
#         'faculty': 'str',
#         'id_group': 'int64',
#         'scientific_director': 'str'
#     })

#     # Типы данных для SQLAlchemy
#     dtype = {
#         'id_student': types.Integer(),
#         'fio': types.String(length=100),
#         'dob': types.Date(),
#         'passport': types.String(length=20),
#         'student_pass': types.String(length=20),
#         'id_specialty': types.Integer(),
#         'faculty': types.String(length=100),
#         'id_group': types.Integer(),
#         'enrollment_date': types.Date(),
#         'date_deduction': types.Date(),
#         'scientific_director': types.String(length=100)
#     }

#     # Загрузка данных в БД
#     df.to_sql(
#         'students',
#         engine,
#         if_exists='append',
#         index=False,
#         dtype=dtype,
#         chunksize=1000
#     )


# if __name__ == "__main__":
#     try:
#         # Создаем подключение
#         engine = create_connection()

#         # Создаем таблицу
#         create_student_table(engine)
#         print("Таблица 'students' создана или уже существует")

#         # Загружаем данные
#         csv_path = "dar\pg\student.csv"
#         load_student_data(engine, csv_path)
#         print(f"Данные из {csv_path} успешно загружены")

#         # Проверяем количество загруженных записей
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT COUNT(*) FROM students"))
#             count = result.scalar()
#             print(f"Всего записей в таблице: {count}")

#     except Exception as e:
#         print(f"Ошибка: {str(e)}")
#     finally:
#         engine.dispose()


def create_specialty_table(engine):
    """Создает таблицу specialties с указанной структурой"""
    with engine.begin() as conn:  # Используем begin() для автоматического коммита
        conn.execute(
            text("""
        CREATE TABLE IF NOT EXISTS specialties (
            id_specialty INTEGER PRIMARY KEY,
            faculty TEXT NOT NULL,
            name TEXT NOT NULL,
            hours INTEGER NOT NULL
        )
        """)
        )


def load_specialty_data(engine, csv_file):
    """Загружает данные из CSV в таблицу specialties"""
    # Чтение CSV с разделителем ;
    df = pd.read_csv(csv_file, sep=";")

    # Проверка наличия необходимых столбцов
    required_columns = ["id_specialty", "faculty", "name", "hours"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Отсутствует необходимый столбец: {col}")

    # Преобразование типов данных
    df = df.astype(
        {"id_specialty": "int64", "faculty": "str", "name": "str", "hours": "int64"}
    )

    # Типы данных для SQLAlchemy
    dtype = {
        "id_specialty": types.Integer(),
        "faculty": types.String(length=100),
        "name": types.String(length=100),
        "hours": types.Integer(),
    }

    # Загрузка данных в БД
    df.to_sql(
        "specialties",
        engine,
        if_exists="append",
        index=False,
        dtype=dtype,
        chunksize=1000,
    )


# if __name__ == "__main__":
#     try:
#         # Создаем подключение
#         engine = create_connection()

#         # Создаем таблицу
#         create_specialty_table(engine)
#         print("Таблица 'specialties' создана или уже существует")

#         # Загружаем данные
#         csv_path = "dar\pg\specialty.csv"  # Или полный путь к файлу
#         load_specialty_data(engine, csv_path)
#         print(f"Данные из {csv_path} успешно загружены")

#         # Проверяем количество загруженных записей
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT COUNT(*) FROM specialties"))
#             count = result.scalar()
#             print(f"Всего записей в таблице: {count}")

#     except Exception as e:
#         print(f"Ошибка: {str(e)}")
#     finally:
#         engine.dispose()


def create_study_group_table(engine):
    """Создает таблицу study_groups с указанной структурой"""
    with engine.begin() as conn:
        conn.execute(
            text("""
        CREATE TABLE IF NOT EXISTS study_groups (
            id integer,
            id_group INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            id_specialty INTEGER NOT NULL,
            academic_year INTEGER NOT NULL,
            FOREIGN KEY (id_specialty) REFERENCES specialties(id_specialty)
        )
        """)
        )


def load_study_group_data(engine, csv_file):
    """Загружает данные из CSV в таблицу study_groups"""
    # Чтение CSV с разделителем ;
    df = pd.read_csv(csv_file, sep=";")

    # Переименование столбцов (удаление кавычек и лишних символов)
    df.columns = df.columns.str.replace('"', "").str.strip()

    # Проверка и обработка данных
    required_columns = ["id_group", "name", "id_specialty", "аcademic_year"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Отсутствует необходимый столбец: {col}")

    # Удаление дублирующего столбца id_specialty, если он есть
    df = df.loc[:, ~df.columns.duplicated()]

    # Переименование столбца academic_year (исправление опечатки)
    df = df.rename(columns={"аcademic_year": "academic_year"})

    # Преобразование типов данных
    df = df.astype(
        {
            "id_group": "int64",
            "name": "str",
            "id_specialty": "int64",
            "academic_year": "int64",
        }
    )

    # Типы данных для SQLAlchemy
    dtype = {
        "id_group": types.Integer(),
        "name": types.String(length=20),
        "id_specialty": types.Integer(),
        "academic_year": types.Integer(),
    }

    # Загрузка данных в БД
    df.to_sql(
        "study_groups",
        engine,
        if_exists="append",
        index=False,
        dtype=dtype,
        chunksize=1000,
    )


# if __name__ == "__main__":
#     try:
#         # Создаем подключение
#         engine = create_connection()

#         # Создаем таблицу
#         create_study_group_table(engine)
#         print("Таблица 'study_groups' создана или уже существует")

#         # Загружаем данные
#         csv_path = "dar\pg\study_group.csv"  # Или полный путь к файлу
#         load_study_group_data(engine, csv_path)
#         print(f"Данные из {csv_path} успешно загружены")

#         # Проверяем количество загруженных записей
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT COUNT(*) FROM study_groups"))
#             count = result.scalar()
#             print(f"Всего записей в таблице: {count}")

#     except Exception as e:
#         print(f"Ошибка: {str(e)}")
#     finally:
#         engine.dispose()

def create_lecturers_table(engine):
    """Создает таблицу lecturers с указанной структурой"""
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS lecturers (
            id_lecturer INTEGER,
            fio TEXT ,
            position TEXT ,
            date_employment DATE ,
            date_dismissal DATE,
            passport TEXT ,
            dob DATE 
        )
        """))

def load_lecturers_data(engine, csv_file):
    """Загружает данные из CSV в таблицу lecturers"""
    # Чтение CSV с разделителем ;
    df = pd.read_csv(csv_file, sep=';', parse_dates=['date_employment', 'date_dismissal', 'dob'], dayfirst=True)
    
    # Проверка наличия необходимых столбцов
    required_columns = ['id_lecturer', 'fio', 'position', 'date_employment', 
                       'date_dismissal', 'passport', 'dob']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Отсутствует необходимый столбец: {col}")
    
    # Преобразование типов данных
    df = df.astype({
        'id_lecturer': 'int64',
        'fio': 'str',
        'position': 'str',
        'passport': 'str'
    })
    
    # Типы данных для SQLAlchemy
    dtype = {
        'id_lecturer': types.Integer(),
        'fio': types.String(length=100),
        'position': types.String(length=100),
        'date_employment': types.Date(),
        'date_dismissal': types.Date(),
        'passport': types.String(length=20),
        'dob': types.Date()
    }
    
    # Загрузка данных в БД
    df.to_sql(
        'lecturers',
        engine,
        if_exists='append',
        index=False,
        dtype=dtype,
        chunksize=1000
    )

# if __name__ == "__main__":
#     try:
#         # Создаем подключение
#         engine = create_connection()
        
#         # Создаем таблицу
#         create_lecturers_table(engine)
#         print("Таблица 'lecturers' создана или уже существует")
        
#         # Загружаем данные
#         csv_path = "dar\pg\lecturer.csv"  # Или полный путь к файлу
#         load_lecturers_data(engine, csv_path)
#         print(f"Данные из {csv_path} успешно загружены")
        
#         # Проверяем количество загруженных записей
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT COUNT(*) FROM lecturers"))
#             count = result.scalar()
#             print(f"Всего записей в таблице: {count}")
            
#     except Exception as e:
#         print(f"Ошибка: {str(e)}")
#     finally:
#         engine.dispose()

def create_ed_plan_table(engine):
    """Создает таблицу ed_plan с указанной структурой"""
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ed_plan (
            id_ed_plan INTEGER PRIMARY KEY,
            subject TEXT NOT NULL,
            id_specialty INTEGER NOT NULL,
            id_lecturer INTEGER NOT NULL,
            id_group INTEGER NOT NULL,
            plan INTEGER NOT NULL,
            fact INTEGER NOT NULL,
            control TEXT NOT NULL,
            semester INTEGER NOT NULL,
            year INTEGER NOT NULL,
            FOREIGN KEY (id_specialty) REFERENCES specialties(id_specialty),
            FOREIGN KEY (id_group) REFERENCES study_groups(id_group)
        )
        """))

def load_ed_plan_data(engine, csv_file):
    """Загружает данные из CSV в таблицу ed_plan"""
    # Чтение CSV с разделителем ;
    df = pd.read_csv(csv_file, sep=';')
    
    # Проверка наличия необходимых столбцов
    required_columns = ['id_ed_plan', 'subject', 'id_specialty', 'id_lecturer', 
                       'id_group', 'plan', 'fact', 'control', 'semester', 'Year']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Отсутствует необходимый столбец: {col}")
    
    # Переименование столбца Year в year (для соответствия SQL стандартам)
    df = df.rename(columns={'Year': 'year'})
    
    # Преобразование типов данных
    df = df.astype({
        'id_ed_plan': 'int64',
        'subject': 'str',
        'id_specialty': 'int64',
        'id_lecturer': 'int64',
        'id_group': 'int64',
        'plan': 'int64',
        'fact': 'int64',
        'control': 'str',
        'semester': 'int64',
        'year': 'int64'
    })
    
    # Типы данных для SQLAlchemy
    dtype = {
        'id_ed_plan': types.Integer(),
        'subject': types.String(length=100),
        'id_specialty': types.Integer(),
        'id_lecturer': types.Integer(),
        'id_group': types.Integer(),
        'plan': types.Integer(),
        'fact': types.Integer(),
        'control': types.String(length=50),
        'semester': types.Integer(),
        'year': types.Integer()
    }
    
    # Загрузка данных в БД
    df.to_sql(
        'ed_plan',
        engine,
        if_exists='append',
        index=False,
        dtype=dtype,
        chunksize=1000
    )

if __name__ == "__main__":
    try:
        # Создаем подключение
        engine = create_connection()
        
        # Создаем таблицу
        create_ed_plan_table(engine)
        print("Таблица 'ed_plan' создана или уже существует")
        
        # Загружаем данные
        csv_path = "dar\pg\ed_plan.csv"  # Или полный путь к файлу
        load_ed_plan_data(engine, csv_path)
        print(f"Данные из {csv_path} успешно загружены")
        
        # Проверяем количество загруженных записей
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM ed_plan"))
            count = result.scalar()
            print(f"Всего записей в таблице: {count}")
            
    except Exception as e:
        print(f"Ошибка: {str(e)}")
    finally:
        engine.dispose()