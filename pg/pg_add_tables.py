from pg.pg_con import dbConnection
# from pg.pg_query import add_tables_comments, add_tables_identification, add_tables_task
from fastapi import APIRouter
db = dbConnection()
router = APIRouter(prefix="/pg", tags=["Pg"])

@router.get("/bd_create_tables")
def bd_create_tables():
    # Создаем таблицу
    # db.execute(add_tables_identification())
    # db.execute(add_tables_task())
    # db.execute(add_tables_comments())
    pass