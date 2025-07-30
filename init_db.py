import sqlite3

conn = sqlite3.connect('leads.db')
cur = conn.cursor()

# Create tables
cur.execute('''
CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    status TEXT DEFAULT 'pending'
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    company TEXT,
    score INTEGER,
    domain TEXT,
    label TEXT,
    description TEXT,
    campaign_id INTEGER,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
)
''')

conn.commit()
conn.close()
print("✅ Database initialized.")

print("✅ Database initialized with campaigns and sample leads.")
