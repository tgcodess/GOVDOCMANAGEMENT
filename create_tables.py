import datetime
import mysql.connector
import random
from faker import Faker

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="akshidash"
)


def generate_pan_number():
    text = 'QWERTYUIOPLKJHGFDSAZXCVBNM'
    pan_number = ''
    for _ in range(5):
        pan_number += random.choice(text)
    pan_number += str(random.randint(1000, 9999))
    pan_number += random.choice(text)
    return pan_number


def generate_card_number(length: int):
    text = '1234567890'
    aadhar_number = ''
    for _ in range(length):
        aadhar_number += random.choice(text)
    return aadhar_number


def generate_passport_number():
    passport_number = ""
    passport_number += random.choice('QWERTYUIOPLKJHGFDSAZXCVBNM')
    passport_number += str(random.randint(1000000, 9999999))
    return passport_number


def weighted_bool():
    return random.choices([True, False], weights=[0.8, 0.2], k=1)[0]


def create_database(name: str):
    cd_cursor = mydb.cursor()
    try:
        cd_cursor.execute(f"CREATE DATABASE {name}")
        print(f"Created Database '{name}'")
    except mysql.connector.errors.DatabaseError:
        print(f"Database '{name}' already exists")
    cd_cursor.execute(f"USE {name}")
    cd_cursor.close()


def restart(database: str):
    r_cursor = mydb.cursor()
    r_cursor.execute(f"DROP DATABASE {database}")
    print(f"Deleted Database '{database}'")


def create_table(name: str, columns: dict, additional_params=None):
    if additional_params is None:
        additional_params = []
    ct_cursor = mydb.cursor()
    init_str = f"CREATE TABLE {name} ("
    try:
        for key, value in columns.items():
            init_str += f"{key} {value}, "
        init_str = init_str[:-2]

        if additional_params:
            init_str += ","
            for param in additional_params:
                init_str += f"{param}, "
            init_str = init_str[:-2]
        init_str += ")"
        print(init_str)
        ct_cursor.execute(init_str)
        print(f"Created Table '{name}'")
    except mysql.connector.errors.ProgrammingError:
        print(f"Table '{name}' already exists")
    ct_cursor.close()


def insert_random_data(num_rows: int, table_name: str, ids=None):
    if ids is None:
        ids = []
    fake = Faker('en_IN')
    ir_cursor = mydb.cursor()
    try:
        if table_name == 'Customer':
            for _ in range(num_rows):
                r_int = fake.unique.random_int()
                ids.append(r_int)

                ir_cursor.execute(
                    f"INSERT INTO {table_name} VALUES ({r_int}, '{fake.first_name()}', '{fake.last_name()}', '{fake.date_of_birth()}', '{fake.building_number()}','{fake.street_name()}', '{fake.city()}', '{fake.state()}', 'India', '{fake.unique.phone_number()}', '{fake.unique.email()}')")

        elif table_name == 'PANCard':
            for _ in ids:
                ir_cursor.execute(
                    f"INSERT INTO {table_name} VALUES ('{generate_pan_number()}', {_}, '{fake.date_between_dates(date_end=datetime.date.today(), date_start=datetime.datetime(1972, 1, 1))}', {weighted_bool()})")
        elif table_name == 'AadharCard':
            for _ in ids:
                ir_cursor.execute(
                    f"INSERT INTO {table_name} VALUES ('{generate_card_number(12)}', {_}, '{fake.date_between_dates(date_end=datetime.date.today(), date_start=datetime.datetime(2010, 9, 29))}', {weighted_bool()})")
        elif table_name == 'BankAccount':
            for an in range(num_rows):
                c_id = random.choice(ids)
                ir_cursor.execute(
                    f"INSERT INTO {table_name} VALUES ('{generate_card_number(12)}', {c_id}, '{random.choice(['State Bank of India', 'Punjab National Bank', 'Union Bank of India', 'Canara Bank', 'HDFC Bank', 'ICICI Bank', 'Kotak Bank', 'Bank of Baroda', 'Axis Bank'])}', '{fake.city()}', '{generate_card_number(11)}', {weighted_bool()})")
        elif table_name == 'KYC':
            for _ in ids:
                ir_cursor.execute(
                    f"INSERT INTO {table_name} VALUES ('{fake.unique.random_int()}', {_}, '{random.choice(['PAN', 'Aadhar', 'Passport'])}', {weighted_bool()})")
        elif table_name == 'Passport':
            c_id = random.sample(ids, k=num_rows)
            for c in c_id:
                ir_cursor.execute(f'select DateOfBirth from Customer where CustomerID = {c}')
                dob = ir_cursor.fetchone()[0]
                issueDate = fake.date_between_dates(date_end=datetime.date.today(), date_start=dob)
                # if age is less than 18
                if issueDate.year - dob.year < 18:  # TODO: implement in PL/SQL
                    expiryDate = issueDate + datetime.timedelta(days=365 * 5)
                else:
                    expiryDate = issueDate + datetime.timedelta(days=365 * 10)
                ir_cursor.execute(
                    f"INSERT INTO {table_name} VALUES ('{generate_passport_number()}', {c}, '{issueDate}', '{expiryDate}', '{random.choice(['Civilian', 'Official', 'Diplomatic'])}')")

        mydb.commit()
        print(f"Inserted {num_rows} rows into '{table_name}'")
    except SyntaxError:
        print(f"Table '{table_name}' does not exist")
    ir_cursor.close()
    if table_name == 'Customer':
        return ids


def print_table(table_name: str):
    pt_cursor = mydb.cursor()
    try:
        pt_cursor.execute(f"SELECT * FROM {table_name}")
        for row in pt_cursor:
            print(row)
    except mysql.connector.errors.ProgrammingError:
        print(f"Table '{table_name}' does not exist")
    pt_cursor.close()


if '__main__' == __name__:
    create_database('GovDept')
    create_table('Customer', {
        'CustomerID': 'INT PRIMARY KEY',
        'FirstName': 'VARCHAR(50)',
        'LastName': 'VARCHAR(50)',
        'DateOfBirth': 'DATE',
        'HouseNumber': 'VARCHAR(100)',
        'Street': 'VARCHAR(100)',
        'City': 'VARCHAR(50)',
        'State': 'VARCHAR(50)',
        'Country': 'VARCHAR(50)',
        'Phone': 'VARCHAR(20)',
        'Email': 'VARCHAR(100)'
    }, ['UNIQUE (Email)', 'UNIQUE (Phone)', ])
    create_table('PANCard', {
        'PANNumber': 'VARCHAR(50) PRIMARY KEY',
        'CustomerID': 'INT',
        'IssuedDate': 'DATE',
        'Status': 'BOOLEAN'
    }, ['FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE', 'CHECK (IssuedDate >= "1972-01-01")'])

    create_table('AadharCard', {
        'AadharNumber': 'VARCHAR(20) PRIMARY KEY',
        'CustomerID': 'INT',
        'IssuedDate': 'DATE',
        'Status': 'BOOLEAN',
    }, ['FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE', 'CHECK (LENGTH(AadharNumber) = 12)', 'CHECK (IssuedDate >= "2010-09-29")', ])

    create_table('BankAccount', {
        'AccountNumber': 'VARCHAR(50) PRIMARY KEY',
        'CustomerID': 'INT',
        'BankName': 'VARCHAR(100)',
        'BranchName': 'VARCHAR(100)',
        'IFSCCode': 'VARCHAR(20)',
        'Status': 'BOOLEAN'
    }, ['FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE'], )

    create_table('KYC', {
        'KYCID': 'VARCHAR(50) PRIMARY KEY',
        'CustomerID': 'INT',
        'DocumentType': 'VARCHAR(50)',
        'Status': 'BOOLEAN'
    }, ['FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE', 'CHECK (DocumentType IN ("PAN", "Aadhar", "Passport"))'])

    create_table('Passport', {
        'PassportNumber': 'VARCHAR(50) PRIMARY KEY',
        'CustomerID': 'INT',
        'IssuedDate': 'DATE',
        'ExpiryDate': 'DATE',
        'Type': 'VARCHAR(50)'
    }, ['FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE', 'CHECK (TYPE IN ("Civilian", "Official", "Diplomatic"))'])
    unique_users = 1000
    bank_account_no = int(1.05 * unique_users)
    passport_holders = int(0.2 * unique_users)
    unique_ids = insert_random_data(unique_users, 'Customer')
    print(unique_ids)
    print_table('Customer')
    insert_random_data(unique_users, 'PANCard', unique_ids)
    print_table('PANCard')
    insert_random_data(unique_users, 'AadharCard', unique_ids)
    print_table('AadharCard')
    insert_random_data(bank_account_no, 'BankAccount', unique_ids)
    print_table('BankAccount')
    insert_random_data(unique_users, 'KYC', unique_ids)
    print_table('KYC')
    insert_random_data(passport_holders, 'Passport', unique_ids)
    print_table('Passport')
    # restart('GovDept')