import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    load_dotenv()
    return mysql.connector.connect(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB_NAME")
    )

def setup_database():
    """
    Connects to the MySQL database and creates the necessary tables.
    This function is idempotent.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        print("Successfully connected to MySQL database.")
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return

    # Create restaurants table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurants (
        establishment_id VARCHAR(255) NOT NULL,
        path VARCHAR(255) NOT NULL,
        name VARCHAR(512) NOT NULL,
        address TEXT,
        category TEXT,
        PRIMARY KEY (establishment_id, path)
    ) ENGINE=InnoDB
    """)

    # Create inspections table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS inspections (
        id INT PRIMARY KEY AUTO_INCREMENT,
        establishment_id VARCHAR(255) NOT NULL,
        establishment_path VARCHAR(255) NOT NULL,
        inspection_date DATE NOT NULL,
        score INT,
        purpose TEXT,
        FOREIGN KEY (establishment_id, establishment_path) REFERENCES restaurants (establishment_id, path)
    ) ENGINE=InnoDB
    """)

    # Create violations table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS violations (
        id INT PRIMARY KEY AUTO_INCREMENT,
        inspection_id INT NOT NULL,
        violation_text TEXT NOT NULL,
        is_critical BOOLEAN NOT NULL,
        FOREIGN KEY (inspection_id) REFERENCES inspections (id),
        UNIQUE KEY `uq_violation` (inspection_id, violation_text(255))
    ) ENGINE=InnoDB
    """)

    conn.commit()
    conn.close()
    print("MySQL database tables are set up and ready.")

if __name__ == "__main__":
    setup_database()