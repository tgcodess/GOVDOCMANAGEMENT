import tabulate
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="akshidash",
    database="govdept"
)
mydb.autocommit = True


def insert_data(table_name):
    try:
        id_cursor = mydb.cursor()
        id_cursor.execute(f"desc {table_name}")
        column_names, type = [], []
        for i in id_cursor:
            column_names.append(i[0])
            type.append(i[1])
        init_str = f"INSERT INTO {table_name} VALUE("
        for i in range(len(column_names)):
            data = input(f"Enter {column_names[i]} ({type[i]}): ")
            if type[i][:7] == 'varchar':
                data = f"'{data}'"
            elif type[i] == "int":
                data = int(data)
            elif type[i] == 'tinyint(1)':
                data = bool(int(data))
            elif type[i] == 'date':
                data = f"DATE('{data}')"
            init_str += f"{data}, "
        init_str = init_str[:-2] + ")"
        print(init_str)
        try:
            id_cursor.execute(init_str)
            mydb.commit()
        except Exception as e:
            print(e)
        id_cursor.close()
    except mysql.connector.errors.IntegrityError as e:
        print(e)


def view_tables():
    vt_cursor = mydb.cursor()
    vt_cursor.execute("show tables")
    vt_return = vt_cursor.fetchall()
    all_tables_lower = [x[0].lower() for x in vt_return]  # Convert all table names to lowercase for comparison
    vt_cursor.close()
    """vt_cursor = mydb.cursor()
    vt_cursor.execute("show tables")
    vt_return = vt_cursor.fetchall()
    vt_cursor.close()"""
    return vt_return


def view_data(table_name):
    if (table_name,) not in view_tables():
        print("Table doesn't exist")
        return
    vd_cursor = mydb.cursor()
    vd_cursor.execute(f"select * from {table_name}")
    all_data = vd_cursor.fetchall()
    column_names = []
    for i in vd_cursor.description:
        column_names.append(i[0])
    print(tabulate.tabulate(all_data, headers=column_names, tablefmt='psql'))
    vd_cursor.close()


def menu():
    options = [('1. Insert Data for New Customer',), ('2. Insert Data for Old Customer',), ('3. View Data',),
               ('4. Update a Record',), ('5. Delete a Record from any table',), ('6. Run a Query',), ('7. Exit',)]
    print(tabulate.tabulate(options, headers=['Options'], tablefmt='psql'))
    int_q = False
    while not int_q:
        try:
            choice = int(input("Enter your choice: "))
            int_q = True
            return choice
        except ValueError:
            print("Invalid choice")


if __name__ == '__main__':
    ask = True
    while ask:
        choice = menu()
        if choice == 1:
            insert_data("customer")
            all_tables = view_tables()
            for (i,) in all_tables:
                choose = input(f"Do you want to insert data in {i} (y/n): ")
                if choose in ('y', 'Y'):
                    insert_data(i)
                else:
                    print(f"Skipping {i}")
        elif choice == 2:
            all_tables = view_tables()
            print(tabulate.tabulate(all_tables, headers=['Tables'], tablefmt='psql'))
            table = input("Enter table name: ")
            if table not in all_tables:
                print("Invalid table name")
                continue
            insert_data(table)
        elif choice == 3:
            all_tables = view_tables()
            print(tabulate.tabulate(all_tables, headers=['Tables'], tablefmt='psql'))
            table = input("Enter table name: ")
            if (table,) not in all_tables:
                print("Invalid table name")
                continue
            view_data(table)

        elif choice == 4:
            # show all tables
            all_tables = view_tables()
            print(tabulate.tabulate(all_tables, headers=['Tables'], tablefmt='psql'))
            table = input("Enter table name: ")
            if (table,) not in all_tables:
                print("Invalid table name")
                continue
            # show all columns
            all_columns, type, pk_index = [], [], 0
            id_cursor = mydb.cursor()
            id_cursor.execute(f"desc {table}")

            for i in id_cursor:
                if i not in ['customerid']:
                    all_columns.append((i[0],))
                    type.append(i[1])
                    if i[3] == 'PRI':
                        pk_index = all_columns.index((i[0],))
            all_columns = tuple(all_columns)
            print(tabulate.tabulate(all_columns, headers=['Columns'], tablefmt='psql'))
            pri = all_columns[pk_index][0]
            # ask for primary key value
            p_value = input(f"Enter value for {pri} ({type[pk_index]}): ")
            p_value = "'" + p_value + "'"
            # ask for column name
            column = input("Enter column name: ")
            if (column,) not in all_columns:
                print("Invalid column name")
                continue
            # ask for value
            value = input(f"Enter value for {column} ({type[all_columns.index((column,))]}): ")
            if type[all_columns.index((column,))][:7] == 'varchar':
                value = f"'{value}'"
            elif type[all_columns.index((column,))] == "int":
                value = int(value)
            elif type[all_columns.index((column,))] == 'tinyint(1)':
                value = bool(int(value))
            elif type[all_columns.index((column,))] == 'date':
                value = f"DATE('{value}')"
            # execute query
            try:
                id_cursor.execute(f"update {table} set {column} = {value} where {pri} = {p_value}")
                mydb.commit()
            except Exception as e:
                print(e)
                continue

            id_cursor.execute(f"select * from {table} where {pri} = {p_value}")
            column_names = []
            for i in id_cursor.description:
                column_names.append(i[0])
            print(tabulate.tabulate(id_cursor.fetchall(), headers=column_names, tablefmt='psql'))
            id_cursor.close()
        elif choice == 5:
            # show all tables
            all_tables = view_tables()
            print(tabulate.tabulate(all_tables, headers=['Tables'], tablefmt='psql'))
            table = input("Enter table name: ")
            if (table,) not in all_tables:
                print("Invalid table name")
                continue
            # show all columns
            all_columns, type, pk_index = [], [], 0
            id_cursor = mydb.cursor()
            id_cursor.execute(f"desc {table}")

            for i in id_cursor:
                if i not in ['customerid']:
                    all_columns.append((i[0],))
                    type.append(i[1])
                    if i[3] == 'PRI':
                        pk_index = all_columns.index((i[0],))
            all_columns = tuple(all_columns)
            print(tabulate.tabulate(all_columns, headers=['Columns'], tablefmt='psql'))
            pri = all_columns[pk_index][0]
            # ask for primary key value
            p_value = input(f"Enter value for {pri} ({type[pk_index]}): ")
            p_value = "'" + p_value + "'"
            # execute query
            try:
                id_cursor.execute(f"delete from {table} where {pri} = {p_value}")
                mydb.commit()
                print(f"{id_cursor.rowcount} record deleted from {table}")
            except Exception as e:
                print(e)
                continue
            id_cursor.close()
        elif choice == 6:
            query = input("Enter query: ")
            query_cursor = mydb.cursor()
            try:
                query_cursor.execute(query)
                print(tabulate.tabulate(query_cursor.fetchall(), headers=[i[0] for i in query_cursor.description], tablefmt='psql'))
            except Exception as e:
                print(e)
                continue
        elif choice == 7:
            ask = False
        else:
            print("Invalid choice")
            continue