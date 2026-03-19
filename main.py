import cmd
from pymongo import MongoClient
from datetime import datetime
import hashlib
from bson import ObjectId

class ShardedUniversityCLI(cmd.Cmd):
    intro = 'Распределенная система университета\n'

    intro += '\nДоступные команды:\n'
    intro += 'find_student <фамилия>      - поиск студентов\n'
    intro += 'add_student <фам,имя,группа,бюджет(bool)> - добавить студента\n'
    intro += 'list_groups                  - список групп\n'
    intro += 'shard_status                  - статус шардинга\n'
    intro += 'exit                          - выход\n'

    prompt = '(university)'
    
    def __init__(self):
        super().__init__()
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['university']
        self.admin_db = self.client['admin']
        
    def do_add_student(self, arg):
        try:
            last_name, first_name, group_id, is_budget = arg.split(',')
            
            group = self.db.groups.find_one({"group_code": group_id})
            if not group:
                print(f"Группа с кодом {group_id} не найдена")
                return
            
            student = {
                "student_code": f"STU{datetime.now().strftime('%y%m%d%H%M%S')}",
                "last_name": last_name,
                "first_name": first_name,
                "group_id": group['_id'],
                "enrollment_year": datetime.now().year,
                "is_budget": is_budget.lower() == 'true',
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            result = self.db.students.insert_one(student)
            print(f"Студент {last_name} {first_name} добавлен с ID: {result.inserted_id}")
            
        except Exception as e:
            print(f"Ошибка: {e}")
    
    def do_find_student(self, arg):
        try:
            students = self.db.students.find({"last_name": arg})
            count = 0
            for s in students:
                budget = "бюджет" if s.get('is_budget') else "коммерция"
                group = self.db.groups.find_one({"_id": s.get('group_id')})
                group_name = group['group_code'] if group else s.get('group_id')
                print(f"{s['last_name']} {s['first_name']} (группа {group_name}, {budget})")
                count += 1
            if count == 0:
                print(f"Студенты с фамилией '{arg}' не найдены")
        except Exception as e:
            print(f"Ошибка: {e}")
    
    def do_shard_status(self, arg):
        try:
            shards = self.admin_db.command("listShards")
            print("Статус шардинга:")
            print("\nШарды:")
            for shard in shards['shards']:
                print(f"   - {shard['_id']}: {shard['host']}")
            
            config_db = self.client['config']
            sharded_coll = config_db.collections.find_one({"_id": "university.students"})
            
            if sharded_coll:
                print(f"\nКоллекция students шардирована")
                print(f"Ключ шардирования: {sharded_coll.get('key')}")
            else:
                print(f"\nКоллекция students не шардирована")
            
            for collection in ['students', 'academic_performance']:
                stats = self.db.command("collStats", collection)
                print(f"\nКоллекция {collection}:")
                print(f"Документов: {stats.get('count', 0)}")
                print(f"Размер: {stats.get('size', 0) / 1024:.2f} KB")
                    
        except Exception as e:
            print(f"Ошибка: {e}")
    
    def do_list_groups(self, arg):
        try:
            groups = self.db.groups.find().limit(20)
            print("Группы:")
            for g in groups:
                print(f"  {g.get('group_code')}: {g.get('specialty')}")
        except Exception as e:
            print(f"Ошибка: {e}")
    
    def do_exit(self, arg):
        print("Выход...")
        self.client.close()
        return True

if __name__ == '__main__':
    ShardedUniversityCLI().cmdloop()