from flask import Flask, request, jsonify, render_template, g, redirect, url_for
import sqlite3
import requests


app = Flask(__name__)
DB_PATH = "leads.db"

# --- DB CONNECTION ---
def get_db():
    if not hasattr(g, '_database'):
        g._database = sqlite3.connect(DB_PATH)
    return g._database

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db:
        db.close()

# --- INIT DATABASE ---
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            domain TEXT,
            score INTEGER,
            company TEXT,
            campaign_id INTEGER,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
        )""")
        conn.commit()

# --- HOME PAGE: REDIRECT TO CAMPAIGNS LIST ---
@app.route('/')
def home():
    return redirect('/campaigns')

# --- SHOW ALL CAMPAIGNS ---
@app.route('/campaigns')
def campaigns():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT id, name, status FROM campaigns")
    campaigns = cursor.fetchall()
    return render_template("campaigns.html", campaigns=campaigns)

# --- VIEW CAMPAIGN LEADS ---
@app.route('/campaign/<int:campaign_id>')
def campaign_detail(campaign_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT id, first_name, last_name, email, company,domain,score,label,description FROM leads WHERE campaign_id = ?", (campaign_id,))
    leads = cursor.fetchall()
    return render_template("campaign_detail.html", leads=leads, campaign_id=campaign_id)

# --- APPROVE CAMPAIGN ---
@app.route('/approve/<int:campaign_id>', methods=['POST'])
def approve_campaign(campaign_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("UPDATE campaigns SET status = 'approved' WHERE id = ?", (campaign_id,))
    db.commit()
    return redirect('/campaigns')

# --- DELETE LEAD ---
@app.route('/delete_lead/<int:lead_id>/<int:campaign_id>', methods=['POST'])
def delete_lead(lead_id, campaign_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    db.commit()
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

@app.route('/add_lead/<int:campaign_id>', methods=['GET', 'POST'])
def add_lead(campaign_id):
    if request.method == 'POST':
        data = request.form
        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO leads (first_name, last_name, email, domain, score, company, label, description, campaign_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['first_name'],
            data['last_name'],
            data['email'],
            data.get('domain', ''),
            data.get('score', 0),
            data['company'],
            data.get('label', ''),
            data.get('description', ''),
            campaign_id
        ))
        db.commit()
        return redirect(url_for('campaign_detail', campaign_id=campaign_id))
    return render_template("add_lead.html", campaign_id=campaign_id)

@app.route('/send_to_n8n/<int:campaign_id>', methods=['POST'])
def send_to_n8n(campaign_id):
    db = get_db()
    cursor = db.cursor()

    # 1. Check campaign status
    cursor.execute("SELECT status FROM campaigns WHERE id = ?", (campaign_id,))
    status = cursor.fetchone()
    if not status or status[0] != 'approved':
        return "Campaign is not approved", 400

    # 2. Fetch all leads in campaign
    cursor.execute("""
        SELECT first_name, last_name, email, company, domain, score, label, description
        FROM leads WHERE campaign_id = ?
    """, (campaign_id,))
    leads = cursor.fetchall()

    leads_data = [
        {
            "first_name": lead[0],
            "last_name": lead[1],
            "email": lead[2],
            "company": lead[3],
            "domain": lead[4],
            "score": lead[5],
            "label": lead[6],
            "description": lead[7],
        } for lead in leads
    ]

    payload = {
        "campaign_id": campaign_id,
        "leads": leads_data
    }

    # 3. Send to n8n
    webhook_url = "https://frog-more-lizard.ngrok-free.app/webhook-test/f7ecb2fe-1f9c-4920-be0d-2cd6bbc93561"
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Sent to n8n successfully.")
    except requests.RequestException as e:
        print(f"❌ Failed to send to n8n: {e}")

    return redirect(url_for('campaigns'))
  # or whatever your campaign listing route is named


# --- UPLOAD ENDPOINT (FROM N8N) ---
@app.route('/upload', methods=['POST'])
def upload_leads():
    data = request.get_json()
    campaign_name = data.get("campaign_name")
    leads = data.get("leads")

    if not campaign_name or not leads:
        return jsonify({"status": "error", "message": "Missing campaign_name or leads"}), 400

    db = get_db()
    cursor = db.cursor()

    cursor.execute("INSERT INTO campaigns (name, status) VALUES (?, ?)", (campaign_name, "pending"))
    campaign_id = cursor.lastrowid

    for lead in leads:
        cursor.execute("""
            INSERT INTO leads (campaign_id, first_name, last_name, email, domain, score, company, label, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            campaign_id,
            lead.get("first_name"),
            lead.get("last_name"),
            lead.get("email"),
            lead.get("domain"),
            lead.get("score"),
            lead.get("company"),
            lead.get("label"),
            lead.get("Description")
        ))

    db.commit()
    return jsonify({"status": "success", "campaign_id": campaign_id})


@app.route('/delete_campaign/<int:campaign_id>', methods=['POST'])
def delete_campaign(campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    # Delete associated leads first
    cursor.execute("DELETE FROM leads WHERE campaign_id = ?", (campaign_id,))
    
    # Then delete the campaign
    cursor.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
    
    db.commit()
    return redirect(url_for('campaigns'))


if __name__ == '__main__':
    init_db()
    app.run(debug=True)
