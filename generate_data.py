#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from pymongo import ASCENDING, DESCENDING, MongoClient
from datetime import datetime, timedelta
import random
from bson import ObjectId
import hashlib

class UniversityDataGenerator:
    def __init__(self, connection_string='mongodb://localhost:27017'):
        self.client = MongoClient(connection_string)
        self.db = self.client['university']
        
        self.faculties = [
            "Факультет информационных технологий",
            "Факультет экономики и менеджмента", 
            "Факультет права",
            "Факультет гуманитарных наук",
            "Факультет естественных наук"
        ]
        
        self.departments_data = [
            {"name": "Кафедра прикладной информатики", "faculty": "Факультет информационных технологий"},
            {"name": "Кафедра вычислительной техники", "faculty": "Факультет информационных технологий"},
            {"name": "Кафедра экономической теории", "faculty": "Факультет экономики и менеджмента"},
            {"name": "Кафедра менеджмента", "faculty": "Факультет экономики и менеджмента"},
            {"name": "Кафедра гражданского права", "faculty": "Факультет права"},
            {"name": "Кафедра уголовного права", "faculty": "Факультет права"},
            {"name": "Кафедра истории", "faculty": "Факультет гуманитарных наук"},
            {"name": "Кафедра филологии", "faculty": "Факультет гуманитарных наук"},
            {"name": "Кафедра физики", "faculty": "Факультет естественных наук"},
            {"name": "Кафедра химии", "faculty": "Факультет естественных наук"}
        ]
        
        self.first_names = [
            "Александр", "Дмитрий", "Максим", "Сергей", "Андрей",
            "Алексей", "Артём", "Илья", "Кирилл", "Михаил",
            "Анна", "Екатерина", "Мария", "Наталья", "Ольга",
            "Татьяна", "Елена", "Дарья", "Виктория", "Полина"
        ]
        
        self.last_names = [
            "Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов",
            "Попов", "Васильев", "Соколов", "Михайлов", "Новиков",
            "Фёдорова", "Морозова", "Волкова", "Алексеева", "Лебедева",
            "Семёнова", "Егорова", "Павлова", "Козлова", "Степанова"
        ]
        
        self.disciplines_data = [
            {"name": "Программирование", "credits": 5, "hours_lecture": 48, "hours_practice": 32},
            {"name": "Базы данных", "credits": 4, "hours_lecture": 32, "hours_practice": 32},
            {"name": "Алгоритмы и структуры данных", "credits": 5, "hours_lecture": 48, "hours_practice": 32},
            {"name": "Математический анализ", "credits": 6, "hours_lecture": 64, "hours_practice": 32},
            {"name": "Линейная алгебра", "credits": 4, "hours_lecture": 48, "hours_practice": 16},
            {"name": "Экономическая теория", "credits": 3, "hours_lecture": 32, "hours_practice": 16},
            {"name": "Менеджмент", "credits": 3, "hours_lecture": 32, "hours_practice": 16},
            {"name": "Право", "credits": 3, "hours_lecture": 32, "hours_practice": 16},
            {"name": "История", "credits": 2, "hours_lecture": 32, "hours_practice": 0},
            {"name": "Иностранный язык", "credits": 4, "hours_lecture": 0, "hours_practice": 64},
            {"name": "Физика", "credits": 4, "hours_lecture": 32, "hours_practice": 32},
            {"name": "Химия", "credits": 4, "hours_lecture": 32, "hours_practice": 32}
        ]
        
        self.academic_degrees = [
            "Кандидат наук", "Доктор наук", "Без степени"
        ]
        
        self.academic_titles = [
            "Доцент", "Профессор", "Старший преподаватель", "Ассистент"
        ]
        
    def clear_database(self):
        print("Очистка базы данных...")
        collections = ['students', 'groups', 'disciplines', 'teachers', 'departments', 'academic_performance', 'semesters']
        
        for collection in collections:
            try:
                self.db[collection].drop()  
                print(f"  - {collection}: удалена")
            except Exception as e:
                print(f"  - {collection}: ошибка при удалении - {e}")
        
        print("  - База данных очищена")

    def generate_departments(self):
        print("\nГенерация кафедр...")
        departments = []
        
        for i, dept in enumerate(self.departments_data):
            department = {
                "department_code": f"DEPT{str(i+1).zfill(3)}",
                "name": dept["name"],
                "faculty": dept["faculty"],
                "phone": f"+7 (495) {random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
                "email": f"dept{i+1}@university.ru",
                "created_at": datetime.now()
            }
            departments.append(department)
        
        result = self.db.departments.insert_many(departments)
        print(f"  - Создано {len(result.inserted_ids)} кафедр")
        return result.inserted_ids
    
    def generate_teachers(self, department_ids):
        print("\nГенерация преподавателей...")
        teachers = []
        
        heads = {}
        for dept_id in department_ids:
            head_teacher = {
                "teacher_code": f"TCH{str(len(teachers)+1).zfill(4)}",
                "last_name": random.choice(self.last_names),
                "first_name": random.choice(self.first_names),
                "middle_name": random.choice(["Иванович", "Петрович", "Сергеевич", "Андреевич", "Александровна"]),
                "academic_degree": random.choice(["Доктор наук", "Кандидат наук"]),
                "academic_title": "Профессор",
                "position": "Заведующий кафедрой",
                "department_id": dept_id,
                "email": f"head.dept{dept_id}@university.ru",
                "phone": f"+7 (495) {random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
                "created_at": datetime.now()
            }
            teachers.append(head_teacher)
            heads[dept_id] = head_teacher
        
        for dept_id in department_ids:
            num_teachers = random.randint(5, 10)
            for _ in range(num_teachers):
                teacher = {
                    "teacher_code": f"TCH{str(len(teachers)+1).zfill(4)}",
                    "last_name": random.choice(self.last_names),
                    "first_name": random.choice(self.first_names),
                    "middle_name": random.choice(["Иванович", "Петрович", "Сергеевич", "Андреевич", "Александровна"]),
                    "academic_degree": random.choice(self.academic_degrees),
                    "academic_title": random.choice(self.academic_titles),
                    "position": random.choice(["Доцент", "Старший преподаватель", "Ассистент"]),
                    "department_id": dept_id,
                    "email": f"teacher{len(teachers)+1}@university.ru",
                    "phone": f"+7 (495) {random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
                    "created_at": datetime.now()
                }
                teachers.append(teacher)
        
        result = self.db.teachers.insert_many(teachers)
        print(f"  - Создано {len(result.inserted_ids)} преподавателей")
        
  
        for dept_id, head in heads.items():
            self.db.departments.update_one(
                {"_id": dept_id},
                {"$set": {"head_teacher_id": head["_id"]}}
            )
        
        return result.inserted_ids
    
    def generate_groups(self, department_ids):
    
        print("\nГенерация групп...")
        groups = []
        used_codes = set()  
        
        for dept_id in department_ids:
            years = [2022, 2023, 2024]
            for year in years:
                num_groups = random.randint(1, 2)
                for i in range(num_groups):
                    while True:
                        group_code = f"{str(year)[-2:]}-{random.randint(10, 99)}"
                        if group_code not in used_codes:
                            used_codes.add(group_code)
                            break
                    
                    group = {
                        "group_code": group_code,
                        "faculty": self.db.departments.find_one({"_id": dept_id})["faculty"],
                        "department_id": dept_id,
                        "specialty": random.choice(["Прикладная информатика", "Экономика", "Юриспруденция", "Филология"]),
                        "enrollment_year": year,
                        "created_at": datetime.now()
                    }
                    groups.append(group)
        
        if groups:
            result = self.db.groups.insert_many(groups)
            print(f"  - Создано {len(result.inserted_ids)} групп")
        else:
            print("  - Не создано ни одной группы")
        
        return [g["_id"] for g in groups] 
    
    def generate_students(self, group_ids):
        print("\nГенерация студентов...")
        students = []

        for group_id in group_ids:
            group = self.db.groups.find_one({"_id": group_id})
            num_students = random.randint(15, 25)
            
            for i in range(num_students):
                last_name = random.choice(self.last_names)
                first_name = random.choice(self.first_names)
                

                year = str(group["enrollment_year"])[2:]
                student_code = f"{year}{random.randint(10000, 99999)}"
                
                student = {
                    "student_code": student_code,
                    "last_name": last_name,
                    "first_name": first_name,
                    "middle_name": random.choice(["Иванович", "Петрович", "Сергеевна", "Андреевна"]),
                    "birth_date": datetime(random.randint(2000, 2005), random.randint(1, 12), random.randint(1, 28)),
                    "enrollment_year": group["enrollment_year"],
                    "group_id": group_id,
                    "status": random.choice(["обучается", "обучается", "обучается", "академический отпуск"]),
                    "is_budget": random.choice([True, False, True, True]),  
                    "email": f"{last_name.lower()}.{first_name.lower()}@student.university.ru",
                    "phone": f"+7 (9{random.randint(10, 99)}) {random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                students.append(student)
        
        result = self.db.students.insert_many(students)
        print(f"  - Создано {len(result.inserted_ids)} студентов")
        return result.inserted_ids
    
    def generate_disciplines(self):
        print("\nГенерация дисциплин...")
        disciplines = []
        
        for i, disc in enumerate(self.disciplines_data):
            discipline = {
                "discipline_code": f"DISC{str(i+1).zfill(3)}",
                "name": disc["name"],
                "description": f"Курс по дисциплине {disc['name']}",
                "credits": disc["credits"],
                "hours_lecture": disc["hours_lecture"],
                "hours_practice": disc["hours_practice"],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            disciplines.append(discipline)
        
        result = self.db.disciplines.insert_many(disciplines)
        print(f"  - Создано {len(result.inserted_ids)} дисциплин")
        return result.inserted_ids
    
    def generate_semesters(self):
        print("\nГенерация семестров...")
        semesters = []
        
        years = [2022, 2023, 2024]
        for year in years:
            for sem_num in [1, 2]:
                semester = {
                    "semester_code": f"{year}-{sem_num}",
                    "academic_year": f"{year}-{year+1}",
                    "semester_number": sem_num,
                    "start_date": datetime(year, 9 if sem_num == 1 else 2, 1),
                    "end_date": datetime(year+1, 1 if sem_num == 1 else 6, 30),
                    "is_current": (year == 2024 and sem_num == 1),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                semesters.append(semester)
        
        result = self.db.semesters.insert_many(semesters)
        print(f"  - Создано {len(result.inserted_ids)} семестров")
        return result.inserted_ids
    
    def generate_academic_performance(self, student_ids, discipline_ids, semester_ids, teacher_ids):
        print("\nГенерация успеваемости...")
        performances = []

        for student_id in student_ids:
            student = self.db.students.find_one({"_id": student_id})
            
            current_year = 2024
            years_studied = current_year - student["enrollment_year"]
            relevant_semesters = list(semester_ids)[:years_studied * 2]
            
            for semester_id in relevant_semesters:
                num_disciplines = random.randint(4, 6)
                semester_disciplines = random.sample(list(discipline_ids), min(num_disciplines, len(discipline_ids)))
                
                for discipline_id in semester_disciplines:
                    if random.random() < 0.9:
                        grade_type = random.choice(["экзамен", "зачет", "экзамен", "экзамен"])
                        if grade_type == "зачет":
                            grade_value = random.choice([0, 1]) 
                        else:
                            grade_value = random.randint(2, 5)  
                        
                        performance = {
                            "student_id": student_id,
                            "discipline_id": discipline_id,
                            "semester_id": semester_id,
                            "teacher_id": random.choice(list(teacher_ids)),
                            "grade_type": grade_type,
                            "grade_value": grade_value,
                            "grade_date": datetime(2024, random.randint(1, 6), random.randint(1, 25)),
                            "created_at": datetime.now()
                        }
                        performances.append(performance)
        
        if performances:
            result = self.db.academic_performance.insert_many(performances)
            print(f"  - Создано {len(result.inserted_ids)} записей об успеваемости")
        else:
            print("  - Нет данных для генерации успеваемости")
        
        return performances
    
    def create_indexes(self):
        print("\nСоздание индексов...")
        
        self.db.students.create_index([("student_code", ASCENDING)]) 
        self.db.students.create_index([("last_name", ASCENDING), ("first_name", ASCENDING)])
        self.db.students.create_index([("group_id", ASCENDING)])
        
        self.db.groups.create_index([("group_code", ASCENDING)], unique=True)
        self.db.groups.create_index([("department_id", ASCENDING)])

        self.db.disciplines.create_index([("discipline_code", ASCENDING)], unique=True)
        
        self.db.teachers.create_index([("teacher_code", ASCENDING)], unique=True)
        self.db.teachers.create_index([("department_id", ASCENDING)])

        self.db.departments.create_index([("department_code", ASCENDING)], unique=True)
        
        self.db.academic_performance.create_index([
            ("student_id", ASCENDING), 
            ("semester_id", ASCENDING), 
            ("discipline_id", ASCENDING)
        ])  
        self.db.academic_performance.create_index([("student_id", ASCENDING), ("semester_id", ASCENDING)])
        self.db.academic_performance.create_index([("discipline_id", ASCENDING), ("semester_id", ASCENDING)])
        
        
        print("  - Индексы созданы")
    
    def generate_all(self, clear_first=True):

        print("Генерация данных ...")
        
        if clear_first:
            self.clear_database()

        print("\nНастройка шардинга...")
        try:
            admin_db = self.client['admin']
            
            admin_db.command("enableSharding", "university")
            print("  - Шардинг включен для базы данных university")
        except Exception as e:
            if "already enabled" in str(e) or "already" in str(e):
                print("  - Шардинг уже был включен")
            else:
                print(f"  - Ошибка при включении шардинга БД: {e}")

        try:
            self.db.students.create_index([("group_id", ASCENDING)]) 
            self.db.students.create_index([("group_id", "hashed")]) 
            print("  - Создан индекс на group_id")
        except Exception as e:
            print(f"  - Индекс уже существует: {e}")
        

        try:
            admin_db = self.client['admin']
            admin_db.command("shardCollection", "university.students", key={"group_id": "hashed"})
            print("  - Коллекция students зашардирована")
        except Exception as e:
            if "already sharded" in str(e) or "already" in str(e):
                print("  - Коллекция students уже была зашардирована")
            else:
                print(f"  - Ошибка при шардировании: {e}")
        

        print("\nГенерация данных...")
        
        department_ids = self.generate_departments()
        teacher_ids = self.generate_teachers(department_ids)
        group_ids = self.generate_groups(department_ids)
        discipline_ids = self.generate_disciplines()
        semester_ids = self.generate_semesters()
        student_ids = self.generate_students(group_ids)
        self.generate_academic_performance(student_ids, discipline_ids, semester_ids, teacher_ids)
        
        self.create_indexes()
        
        
        print("\nИтог:")
        print(f"  - Кафедр: {self.db.departments.count_documents({})}")
        print(f"  - Преподавателей: {self.db.teachers.count_documents({})}")
        print(f"  - Групп: {self.db.groups.count_documents({})}")
        print(f"  - Студентов: {self.db.students.count_documents({})}")
        print(f"  - Дисциплин: {self.db.disciplines.count_documents({})}")
        print(f"  - Семестров: {self.db.semesters.count_documents({})}")
        print(f"  - Записей об успеваемости: {self.db.academic_performance.count_documents({})}")
        

        try:
            config_db = self.client['config']
            sharded_coll = config_db.collections.find_one({"_id": "university.students"})
            if sharded_coll:
                print("Коллекция students успешно шардирована")
                print(f"  - Ключ шардирования: {sharded_coll.get('key')}")
            else:
                print("Коллекция students не найдена в настройках шардинга")
        except Exception as e:
            print(f"Не удалось проверить статус шардинга: {e}")

if __name__ == "__main__":
    import sys
    

    if len(sys.argv) > 1:
        connection_string = sys.argv[1]
    else:
        connection_string = 'mongodb://localhost:27017'
    
    generator = UniversityDataGenerator(connection_string)
    generator.generate_all(clear_first=True)