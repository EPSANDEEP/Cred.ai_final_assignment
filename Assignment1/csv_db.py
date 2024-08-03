import psycopg2
import csv

conn = None
cur = None

csv_file_path = 'Assignment1\customers.csv'
with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    customers = list(reader)

try:
    
    conn = psycopg2.connect(
        host = "localhost", dbname = "demo", user = "postgres", password = "post@123", port = 5432
    )
    
    cur = conn.cursor()
    
    # Create customer_info
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_info (
            customer_id VARCHAR(255) PRIMARY KEY,f_name VARCHAR(255),
            l_name VARCHAR(255),city VARCHAR(255),country VARCHAR(255),
            personal_phnum VARCHAR(255),personal_email VARCHAR(255));
    """)
    
    #create customer_work_info
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_work_info (
            customer_id VARCHAR(255) PRIMARY KEY, full_name VARCHAR(255),
            office_loc VARCHAR(255),subsctiption_date DATE,website VARCHAR(255),
            work_phnum VARCHAR(255),work_email VARCHAR(255));
    """)
    
    for customer in customers:  # Loop over each row in the CSV data
        customer_id = customer['Customer Id'].upper()
        f_name = customer['First Name'].upper()
        l_name = customer['Last Name'].upper()
        city = customer['City']
        country = customer['Country']
        personal_phnum = customer['Phone 1']
        personal_email = customer['Email'].lower()
        subsctiption_date = customer['Subscription Date']
        website = customer['Website']
        work_phnum = customer['Phone 2']
        full_name = f"{customer['First Name']} {customer['Last Name']}"
        work_email = f"{customer['First Name'].lower()}_{customer['Last Name'].lower()}@sample.com"
        office_loc = 'Bangalore'

        cur.execute("""
        INSERT INTO customer_info (customer_id, f_name, l_name, city, country, personal_phnum, personal_email)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
        """, (customer_id, f_name, l_name, city, country, personal_phnum, personal_email))

        cur.execute("""
        INSERT INTO customer_work_info (customer_id, full_name, office_loc, subsctiption_date, website, work_phnum, work_email)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
        """, (customer_id, full_name, office_loc, subsctiption_date, website, work_phnum, work_email))

    # Commit the transactions
    conn.commit()

    # Display the first 5 rows from customer_info table for verification
    cur.execute("SELECT * FROM customer_info LIMIT 5;")
    customer_info_rows = cur.fetchall()
    print("First 5 rows from customer_info:")
    for row in customer_info_rows:
        print(row)

    cur.execute("SELECT * FROM customer_work_info LIMIT 5;")
    customer_work_info_rows = cur.fetchall()
    print("\nFirst 5 rows from customer_work_info:")
    for row in customer_work_info_rows:
        print(row)
    
except Exception as e:
    print(e)
    
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()