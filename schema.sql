DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS profiles;

CREATE TABLE campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE leads (
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
);
