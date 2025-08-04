from flask import Flask, request, jsonify, render_template, g, redirect, url_for, flash
import sqlite3
import requests
import csv
import io
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this to a random secret key
DB_PATH = "leads.db"

# Configuration for file uploads
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
            label TEXT,
            description TEXT,
            campaign_id INTEGER,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
        )""")
        
        # Add is_active column if it doesn't exist (for existing databases)
        try:
            c.execute("ALTER TABLE leads ADD COLUMN is_active INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            # Column already exists
            pass
            
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
    cursor.execute("SELECT id, name, status FROM campaigns ORDER BY id DESC")
    campaigns = cursor.fetchall()
    return render_template("campaigns.html", campaigns=campaigns)

# --- UPLOAD CSV ---
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'csv_file' not in request.files:
        flash('No file selected', 'error')
        return redirect(url_for('campaigns'))
    
    file = request.files['csv_file']
    campaign_name = request.form.get('campaign_name', '').strip()
    
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('campaigns'))
    
    if not campaign_name:
        flash('Campaign name is required', 'error')
        return redirect(url_for('campaigns'))
    
    if file and allowed_file(file.filename):
        try:
            # Read CSV content
            stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
            csv_input = csv.DictReader(stream)
            
            db = get_db()
            cursor = db.cursor()
            
            # Create new campaign
            cursor.execute("INSERT INTO campaigns (name, status) VALUES (?, ?)", (campaign_name, "pending"))
            campaign_id = cursor.lastrowid
            
            # Process CSV rows
            leads_added = 0
            for row in csv_input:
                # Map CSV columns to database fields (adjust field names as needed)
                first_name = row.get('first_name', row.get('First Name', '')).strip()
                last_name = row.get('last_name', row.get('Last Name', '')).strip()
                email = row.get('email', row.get('Email', '')).strip()
                company = row.get('company', row.get('Company', '')).strip()
                domain = row.get('domain', row.get('Domain', '')).strip()
                label = row.get('label', row.get('Label', row.get('Job Title', ''))).strip()
                description = row.get('description', row.get('Description', '')).strip()
                score = int(row.get('score', row.get('Score', 5)))
                
                # Skip empty rows
                if not email or not first_name:
                    continue
                
                cursor.execute("""
                    INSERT INTO leads (first_name, last_name, email, domain, score, company, label, description, campaign_id, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (first_name, last_name, email, domain, score, company, label, description, campaign_id, 1))
                leads_added += 1
            
            db.commit()
            flash(f'Successfully uploaded {leads_added} leads to campaign "{campaign_name}"', 'success')
            return redirect(url_for('campaign_detail', campaign_id=campaign_id))
            
        except Exception as e:
            flash(f'Error processing CSV file: {str(e)}', 'error')
            return redirect(url_for('campaigns'))
    else:
        flash('Invalid file type. Please upload a CSV file.', 'error')
        return redirect(url_for('campaigns'))

# --- VIEW CAMPAIGN LEADS ---
@app.route('/campaign/<int:campaign_id>')
def campaign_detail(campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    # Get campaign info
    cursor.execute("SELECT name FROM campaigns WHERE id = ?", (campaign_id,))
    campaign = cursor.fetchone()
    
    cursor.execute("""
        SELECT id, first_name, last_name, email, company, domain, score, label, description, is_active 
        FROM leads WHERE campaign_id = ? ORDER BY id
    """, (campaign_id,))
    leads = cursor.fetchall()
    
    campaign_name = campaign[0] if campaign else f"Campaign {campaign_id}"
    return render_template("campaign_detail.html", leads=leads, campaign_id=campaign_id, campaign_name=campaign_name)

# --- APPROVE CAMPAIGN ---
@app.route('/approve/<int:campaign_id>', methods=['POST'])
def approve_campaign(campaign_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("UPDATE campaigns SET status = 'approved' WHERE id = ?", (campaign_id,))
    db.commit()
    flash('Campaign approved successfully!', 'success')
    return redirect('/campaigns')

# --- DELETE LEAD ---
@app.route('/delete_lead/<int:lead_id>/<int:campaign_id>', methods=['POST'])
def delete_lead(lead_id, campaign_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    db.commit()
    flash('Lead deleted successfully!', 'success')
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

# --- TOGGLE LEAD STATUS (ACTIVATE/DEACTIVATE) ---
@app.route('/toggle_lead_status/<int:lead_id>/<int:campaign_id>', methods=['POST'])
def toggle_lead_status(lead_id, campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    # Get current status
    cursor.execute("SELECT is_active FROM leads WHERE id = ?", (lead_id,))
    current_status = cursor.fetchone()
    
    if current_status:
        new_status = 0 if current_status[0] == 1 else 1
        cursor.execute("UPDATE leads SET is_active = ? WHERE id = ?", (new_status, lead_id))
        db.commit()
        
        status_text = "activated" if new_status == 1 else "deactivated"
        flash(f'Lead {status_text} successfully!', 'success')
    else:
        flash('Lead not found!', 'error')
    
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

# --- BULK DELETE LEADS ---
@app.route('/bulk_delete_leads/<int:campaign_id>', methods=['POST'])
def bulk_delete_leads(campaign_id):
    lead_ids = request.form.getlist('lead_ids')
    if lead_ids:
        db = get_db()
        cursor = db.cursor()
        placeholders = ','.join('?' for _ in lead_ids)
        cursor.execute(f"DELETE FROM leads WHERE id IN ({placeholders})", lead_ids)
        db.commit()
        flash(f'Successfully deleted {len(lead_ids)} leads!', 'success')
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

# --- BULK ACTIVATE/DEACTIVATE LEADS ---
@app.route('/bulk_toggle_leads/<int:campaign_id>/<int:status>', methods=['POST'])
def bulk_toggle_leads(campaign_id, status):
    lead_ids = request.form.getlist('lead_ids')
    if lead_ids:
        db = get_db()
        cursor = db.cursor()
        placeholders = ','.join('?' for _ in lead_ids)
        cursor.execute(f"UPDATE leads SET is_active = ? WHERE id IN ({placeholders})", [status] + lead_ids)
        db.commit()
        
        action = "activated" if status == 1 else "deactivated"
        flash(f'Successfully {action} {len(lead_ids)} leads!', 'success')
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

# --- ADD LEAD ---
@app.route('/add_lead/<int:campaign_id>', methods=['GET', 'POST'])
def add_lead(campaign_id):
    if request.method == 'POST':
        data = request.form
        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO leads (first_name, last_name, email, domain, score, company, label, description, campaign_id, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['first_name'].strip(),
            data['last_name'].strip(),
            data['email'].strip(),
            data.get('domain', '').strip(),
            int(data.get('score', 0)),
            data['company'].strip(),
            data.get('label', '').strip(),
            data.get('description', '').strip(),
            campaign_id,
            1  # Default to active
        ))
        db.commit()
        flash('Lead added successfully!', 'success')
        return redirect(url_for('campaign_detail', campaign_id=campaign_id))
    return render_template("add_lead.html", campaign_id=campaign_id)

# --- SEND TO N8N (ONLY ACTIVE LEADS) ---
@app.route('/send_to_n8n/<int:campaign_id>', methods=['POST'])
def send_to_n8n(campaign_id):
    db = get_db()
    cursor = db.cursor()

    # 1. Check campaign status
    cursor.execute("SELECT status FROM campaigns WHERE id = ?", (campaign_id,))
    status = cursor.fetchone()
    if not status or status[0] != 'approved':
        flash('Campaign is not approved', 'error')
        return redirect(url_for('campaigns'))

    # 2. Fetch only ACTIVE leads in campaign
    cursor.execute("""
        SELECT first_name, last_name, email, company, domain, score, label, description
        FROM leads WHERE campaign_id = ? AND is_active = 1
    """, (campaign_id,))
    leads = cursor.fetchall()

    if not leads:
        flash('No active leads found in this campaign', 'error')
        return redirect(url_for('campaigns'))

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
    webhook_url = "https://frog-more-lizard.ngrok-free.app/webhook/f7ecb2fe-1f9c-4920-be0d-2cd6bbc93561"
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        flash(f'Campaign sent to n8n successfully! ({len(leads_data)} active profiles processed)', 'success')
        print(f"✅ Sent {len(leads_data)} active leads to n8n successfully.")
    except requests.RequestException as e:
        flash(f'Failed to send to n8n: {str(e)}', 'error')
        print(f"❌ Failed to send to n8n: {e}")

    return redirect(url_for('campaigns'))

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
            INSERT INTO leads (campaign_id, first_name, last_name, email, domain, score, company, label, description, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            campaign_id,
            lead.get("first_name"),
            lead.get("last_name"),
            lead.get("email"),
            lead.get("domain"),
            lead.get("score", 0),
            lead.get("company"),
            lead.get("label"),
            lead.get("Description"),  # Note: Capital D as in your original
            1  # Default to active
        ))

    db.commit()
    return jsonify({"status": "success", "campaign_id": campaign_id})

# --- DELETE CAMPAIGN ---
@app.route('/delete_campaign/<int:campaign_id>', methods=['POST'])
def delete_campaign(campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    # Delete associated leads first
    cursor.execute("DELETE FROM leads WHERE campaign_id = ?", (campaign_id,))
    
    # Then delete the campaign
    cursor.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
    
    db.commit()
    flash('Campaign deleted successfully!', 'success')
    return redirect(url_for('campaigns'))

if __name__ == '__main__':
    init_db()
    app.run(debug=True)