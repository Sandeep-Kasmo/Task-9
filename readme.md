📘 ETL Pipeline with Slowly Changing Dimensions (SCD Type-2)

This project implements a complete ETL (Extract–Transform–Load) pipeline using Python, Pandas, and MySQL.
It also performs SCD Type-2 to track historical changes in customer data.

📂 Project Structure
project/

|──src/

    │── extract.py
    
    │── transform.py
    
    │── load.py
    
    │── main.py

│── README.md


extract.py → Connects to MySQL and retrieves source tables

transform.py → Performs data cleaning and transformations

load.py → Creates target tables dynamically & loads data

main.py → Runs the ETL + SCD Type-2 flow end-to-end

🚀 Features
✔ 1. Extract

Connects to MySQL database

Loads source tables such as:

customer

orders

new_data (incoming customer changes)

✔ 2. Transform

Cleans raw data

Standardizes column formats

Generates transformed datasets for SCD

Applies a custom function:

customer_transform()

✔ 3. SCD Type-2 Implementation

Tracks historical changes using:

Old customer records

New customer incoming data

Uses:

SCDType2(old_df, new_df)


This generates:

New updated records

Old records closed with end_date

Flags current active rows

✔ 4. Load

Loads the final SCD2 output into MySQL:

Dynamically creates the table

Maps Pandas dtypes → MySQL datatypes

Inserts rows using executemany for performance

🛠 Technology Stack
Component	Technology
Language	Python 3
Database	MySQL
Libraries	Pandas, mysql-connector-python
Data Processing	SCD Type-2 Logic
▶️ How to Run the Project
1️⃣ Install requirements
pip install pandas mysql-connector-python

2️⃣ Configure Database Connection

Inside extract.py, update:

mysql.connector.connect(
    host="localhost",
    user="root",
    password="your_password",
    database="your_database"
)

3️⃣ Run the ETL Job
python main.py

📊 Output

The final SCD Type-2 table:

Tracks every change made to customer attributes

Maintains historical versions

Creates a complete audit trail

Always keeps one active record (is_current = 1)

Example final columns:

id, name, email, phone, address,
effective_from, effective_to, is_current

📌 Notes

The load function automatically:

Drops table if exists

Creates new table

Inserts all SCD2 records

Dtype mapping used:

int → INT

float → FLOAT

datetime → DATETIME

others → VARCHAR(255)

👨‍💻 Author

Sandeep Reddy

Python | Data Engineering | SQL
