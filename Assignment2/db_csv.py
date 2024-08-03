import csv
import psycopg2
from datetime import datetime

search_string = input("Enter a string to search for customers: ")

conn = None
cur = None

try:
    conn = psycopg2.connect(
        host = "localhost", dbname = "demo", user = "postgres", password = "post@123", port = 5432
    )
    cur = conn.cursor()

    
    cur.execute("""SELECT ci.customer_id, CONCAT(ci.f_name, ' ', ci.l_name) AS full_name, ci.city, ci.personal_phnum,
        ci.personal_email, cwi.office_loc, cwi.subsctiption_date, cwi.work_phnum, cwi.work_email
        FROM customer_info ci JOIN customer_work_info cwi ON ci.customer_id = cwi.customer_id
        WHERE CONCAT(ci.f_name, ' ', ci.l_name) ILIKE %s""", (search_string + '%',))

    customers = cur.fetchall()

    csv_file_path = 'Final_Assgn\Assignment2\customer_details.csv'

    current_date = datetime.now().date() #currentdate

    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            'customer_id', 'full_name', 'city', 'personal_phnum', 
            'personal_email', 'office_loc', 'subsctiption_date', 
            'pending_days', 'work_phnum', 'work_email'
        ])#headers
        
        for customer in customers:   #row
            customer_id, full_name, city, personal_phnum, personal_email, office_loc, subsctiption_date, work_phnum, work_email = customer
            
            if subsctiption_date:
                pending_days = (current_date - subsctiption_date).days
            else:
                pending_days = 'N/A'
            
            writer.writerow([
                customer_id, full_name, city, personal_phnum, 
                personal_email, office_loc, subsctiption_date, 
                pending_days, work_phnum, work_email
            ])
except Exception as e:
    print(e)
    
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

print(f"Customer details have been written to {csv_file_path}")
