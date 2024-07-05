import psycopg2

def migrate_db():

    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="2778",
        host="db",
        port="5432"
    )
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacancies (
        id SERIAL PRIMARY KEY,
        company VARCHAR(255),
        vacancy VARCHAR(255),
        location VARCHAR(255),
        salary VARCHAR(255),
        skills TEXT,
        link TEXT,
        description TEXT
    );
    """)
    
    print("База данных создана")
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    migrate_db()
