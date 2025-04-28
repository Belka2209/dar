import csv
from datetime import datetime


def read_csv(filename):
    with open(filename, mode="r", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def gen_report():
    # Чтение данных из CSV файлов
    employees = read_csv("dar\python_four\Table_1.csv")
    salary = read_csv("dar\python_four\Table_2.csv")
    emails = read_csv("dar\python_four\Table_3.csv")
    print("salary", salary)

    data = []
    for employee in employees:
        cooperator = {}
        cooperator["Empl_ID"] = employee.get("ID")
        cooperator["FIO"] = (
            f"{employee.get('NAME1')} {employee.get('NAME2')} {employee.get('NAME3')}"
        )

        # Фильтруем записи зарплат для текущего сотрудника
        salary_records = [
            record
            for record in salary
            if record.get("ID") == employee.get("ID")
            if record.get("dt") is not None
            and datetime.strptime(record.get("dt"), "%Y-%m-%d").year == 2020
        ]
        # Проверяем, есть ли записи за 2020 год
        if not salary_records:
            continue

        # Подсчет зарплаты и бонусов
        salary_amount = sum(
            float(record.get("Amount"))
            for record in salary_records
            if record.get("Salary_Type") == "salary"
        )
        bonus_amount = sum(
            float(record.get("Amount"))
            for record in salary_records
            if record.get("Salary_Type") == "bonus"
        )

        # Расчет среднего значения
        cooperator["Salary"] = round(salary_amount / 12, 1)
        cooperator["Bonus"] = bonus_amount / 12


        # Добавляем запись для каждого электронного ящика
        employee_emails = [
            email.get("Email")
            for email in emails
            if email.get("Empl_ID") == cooperator.get("Empl_ID")
        ]
        if employee_emails:
            for email in employee_emails:
                cooperator_with_email = cooperator.copy()
                cooperator_with_email["Email"] = email
                data.append(cooperator_with_email)
        else:
            # Если у сотрудника нет email, добавляем запись без email
            cooperator["Email"] = None
            data.append(cooperator)

    # Проверяем наличие дубликатов
    seen = set()
    reports = []
    for record in data:
        # Преобразуем запись в неизменяемый объект (кортеж), чтобы использовать в множестве
        record_tuple = tuple(record.items())  # Преобразуем словарь в кортеж
        if record_tuple not in seen:
            seen.add(record_tuple)
            reports.append(record)

    # Форматируем вывод
    for report in reports:
        print(
            f"{report.get('Empl_ID')}, {report.get('FIO')}, {report.get('Salary')}, {report.get('Bonus')}, {report.get('Email')}"
        )


gen_report()
