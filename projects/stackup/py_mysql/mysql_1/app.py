import mysql.connector

def sqlConnect():
  return mysql.connector.connect(
    host="127.0.2.2",
    user="root",
    password="init1234",
    database="employees_db",
    auth_plugin="mysql_native_password"
  )

def create_database():
  database = sqlConnect()
  db_cursor = database.cursor()
  db_cursor.execute("CREATE TABLE IF NOT EXISTS employees (id int AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), role VARCHAR(255), salary int)")
  db_cursor.execute("SELECT * FROM employees")


def add_employee():
  database = sqlConnect()
  db_cursor = database.cursor()
  name = input("Enter employee name: ")
  role = input("Enter employee role: ")
  salary = input("Enter employee salary: ")
  db_cursor.execute("INSERT INTO employees (name, role, salary) VALUES (%s, %s, %s)", (name, role, salary))
  database.commit()
  print(db_cursor.rowcount, "record inserted.")

def view_employees():
  database = sqlConnect()
  db_cursor = database.cursor()
  db_cursor.execute("SELECT * FROM employees")
  employees = db_cursor.fetchall()
  for employee in employees:
    print(employee)

def update_employee():
  database = sqlConnect()
  db_cursor = database.cursor()
  id = input("Enter employee id: ")
  name = input("Enter employee name: ")
  role = input("Enter employee role: ")
  salary = input("Enter employee salary: ")
  db_cursor.execute("UPDATE employees SET name = %s, role = %s, salary = %s WHERE id = %s", (name, role, salary, id))
  database.commit()
  print(db_cursor.rowcount, "record(s) affected")

def delete_employee():
  database = sqlConnect()
  db_cursor = database.cursor()
  id = input("Enter employee id: ")
  db_cursor.execute("DELETE FROM employees WHERE id = %s", (id,))
  database.commit()
  print(db_cursor.rowcount, "record(s) deleted")

def main():
  create_database()
  while True:
    print("1. Add employee\n2. View employees\n3. Update employee\n4. Delete employee\n5. Exit")
    choice = input("Enter your choice: ")
    if choice == "1":
      add_employee()
    elif choice == "2":
      view_employees()
    elif choice == "3":
      update_employee()
    elif choice == "4":
      delete_employee()
    elif choice == "5":
      break
    else:
      print("Invalid choice")

if __name__ == "__main__":
  main()

