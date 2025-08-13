from flask import Flask, request, jsonify, render_template, g, redirect, url_for, flash
import sqlite3
import requests
import csv
import io
from werkzeug.utils import secure_filename
import os
from datetime import datetime

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
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            source TEXT,
            campaign_id INTEGER,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
        )""")

        
        
        # Add columns if they don't exist (for existing databases)
        try:
            c.execute("ALTER TABLE campaigns ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        except sqlite3.OperationalError:
            pass
            
        try:
            c.execute("ALTER TABLE leads ADD COLUMN is_active INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            pass
            
        try:
            c.execute("ALTER TABLE leads ADD COLUMN source TEXT")
        except sqlite3.OperationalError:
            pass

        try:
            c.execute("ALTER TABLE leads ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        except sqlite3.OperationalError:
            pass
        
        try:
            c.execute("ALTER TABLE campaigns ADD COLUMN is_merged INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
            
        conn.commit()

# --- UTILITY FUNCTIONS ---
def remove_duplicate_leads(leads_data):
    """
    Remove duplicate leads based on email address
    Returns unique leads and duplicate count
    """
    seen_emails = set()
    unique_leads = []
    duplicate_count = 0
    
    for lead in leads_data:
        email = lead.get('email', '').lower().strip()
        if email and email not in seen_emails:
            seen_emails.add(email)
            unique_leads.append(lead)
        else:
            duplicate_count += 1
    
    return unique_leads, duplicate_count

def get_campaign_profile_count(campaign_id):
    """Get the number of profiles in a campaign"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ?", (campaign_id,))
    count = cursor.fetchone()
    return count[0] if count else 0

# --- HOME PAGE: REDIRECT TO CAMPAIGNS LIST ---
@app.route('/')
def home():
    return redirect('/campaigns')

# --- SHOW ALL CAMPAIGNS WITH PROFILE COUNTS ---
@app.route('/campaigns')
def campaigns():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("""
        SELECT c.id, c.name, c.status, COUNT(l.id) as profile_count
        FROM campaigns c
        LEFT JOIN leads l ON c.id = l.campaign_id
        GROUP BY c.id, c.name, c.status
        ORDER BY c.id DESC
    """)
    campaigns = cursor.fetchall()
    return render_template("campaigns.html", campaigns=campaigns)

# --- MERGE CAMPAIGNS ---
# Replace your merge_campaigns function with this updated version
# This version handles the missing created_at column gracefully

# Replace your existing merge_campaigns function with this corrected version

@app.route('/merge_campaigns', methods=['POST'])
def merge_campaigns():
    campaign_ids = request.form.getlist('campaign_ids[]')
    merged_campaign_name = request.form.get('merged_campaign_name', '').strip()
    
    if len(campaign_ids) < 2:
        flash('Please select at least 2 campaigns to merge', 'error')
        return redirect(url_for('campaigns'))
    
    if not merged_campaign_name:
        flash('Please provide a name for the merged campaign', 'error')
        return redirect(url_for('campaigns'))
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Check if campaigns table has created_at column
        cursor.execute("PRAGMA table_info(campaigns)")
        columns_info = cursor.fetchall()
        has_created_at = any(col[1] == 'created_at' for col in columns_info)
        has_is_merged = any(col[1] == 'is_merged' for col in columns_info)
        
        # Verify all campaigns exist
        placeholders = ','.join('?' for _ in campaign_ids)
        cursor.execute(f"SELECT id, name FROM campaigns WHERE id IN ({placeholders})", campaign_ids)
        existing_campaigns = cursor.fetchall()
        
        if len(existing_campaigns) != len(campaign_ids):
            flash('Some selected campaigns do not exist', 'error')
            return redirect(url_for('campaigns'))
        
        # Get all leads from selected campaigns
        cursor.execute(f"""
            SELECT first_name, last_name, email, domain, score, company, label, description, 
                   COALESCE(source, 'Merged Campaign') as source
            FROM leads WHERE campaign_id IN ({placeholders})
        """, campaign_ids)
        all_leads = cursor.fetchall()
        
        if not all_leads:
            flash('No profiles found in selected campaigns', 'error')
            return redirect(url_for('campaigns'))
        
        # Convert to list of dictionaries for duplicate removal
        leads_data = []
        for lead in all_leads:
            leads_data.append({
                'first_name': lead[0] or '',
                'last_name': lead[1] or '',
                'email': lead[2] or '',
                'domain': lead[3] or '',
                'score': lead[4] or 5,
                'company': lead[5] or '',
                'label': lead[6] or '',
                'description': lead[7] or '',
                'source': lead[8] or 'Merged Campaign'
            })
        
        # Remove duplicates based on email
        unique_leads, duplicate_count = remove_duplicate_leads(leads_data)
        
        if not unique_leads:
            flash('No valid profiles found after removing duplicates', 'error')
            return redirect(url_for('campaigns'))
        
        # Create new merged campaign with conditional columns - FIXED THE MISSING PARAMETERS
        if has_created_at and has_is_merged:
            cursor.execute("""
                INSERT INTO campaigns (name, status, created_at, is_merged) 
                VALUES (?, 'pending', ?, 1)
            """, (merged_campaign_name, datetime.now()))
        elif has_created_at:
            cursor.execute("""
                INSERT INTO campaigns (name, status, created_at) 
                VALUES (?, 'pending', ?)
            """, (merged_campaign_name, datetime.now()))
        elif has_is_merged:
            # FIX: This was missing the parameter
            cursor.execute("""
                INSERT INTO campaigns (name, status, is_merged) 
                VALUES (?, 'pending', 1)
            """, (merged_campaign_name,))  # Added missing parameter
        else:
            cursor.execute("""
                INSERT INTO campaigns (name, status) 
                VALUES (?, 'pending')
            """, (merged_campaign_name,))
        
        merged_campaign_id = cursor.lastrowid
        
        # If is_merged column exists but wasn't handled above, update it
        if has_is_merged and not (has_created_at and has_is_merged):
            cursor.execute("""
                UPDATE campaigns SET is_merged = 1 
                WHERE id = ?
            """, (merged_campaign_id,))
        
        # Check if leads table has the required columns
        cursor.execute("PRAGMA table_info(leads)")
        leads_columns_info = cursor.fetchall()
        leads_columns = [col[1] for col in leads_columns_info]
        
        has_source = 'source' in leads_columns
        has_is_active = 'is_active' in leads_columns
        has_leads_created_at = 'created_at' in leads_columns
        
        # Insert unique leads into the new campaign
        leads_added = 0
        for lead in unique_leads:
            # Build dynamic INSERT query based on available columns
            columns = ['campaign_id', 'first_name', 'last_name', 'email', 'domain', 'score', 'company', 'label', 'description']
            values = [
                merged_campaign_id,
                lead['first_name'],
                lead['last_name'],
                lead['email'],
                lead['domain'],
                lead['score'],
                lead['company'],
                lead['label'],
                lead['description']
            ]
            
            if has_source:
                columns.append('source')
                values.append(lead['source'])
            
            if has_is_active:
                columns.append('is_active')
                values.append(1)
            
            if has_leads_created_at:
                columns.append('created_at')
                values.append(datetime.now())
            
            # Create the INSERT query
            columns_str = ', '.join(columns)
            placeholders_str = ', '.join(['?' for _ in columns])
            
            cursor.execute(f"""
                INSERT INTO leads ({columns_str})
                VALUES ({placeholders_str})
            """, values)
            leads_added += 1
        
        # Delete original campaigns and their leads
        campaign_names = [camp[1] for camp in existing_campaigns]
        
        # Delete leads from original campaigns
        cursor.execute(f"DELETE FROM leads WHERE campaign_id IN ({placeholders})", campaign_ids)
        
        # Delete original campaigns
        cursor.execute(f"DELETE FROM campaigns WHERE id IN ({placeholders})", campaign_ids)
        
        db.commit()
        
        success_message = f'Successfully merged {len(campaign_ids)} campaigns into "{merged_campaign_name}"'
        if duplicate_count > 0:
            success_message += f' - Removed {duplicate_count} duplicate profiles'
        success_message += f' - {leads_added} unique profiles added'
        
        flash(success_message, 'success')
        return redirect(url_for('campaign_detail', campaign_id=merged_campaign_id))
        
    except Exception as e:
        flash(f'Error merging campaigns: {str(e)}', 'error')
        return redirect(url_for('campaigns'))

# --- UPLOAD CSV WITH DUPLICATE DETECTION ---
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
            
            # Process CSV rows
            leads_data = []
            for row in csv_input:
                # Map CSV columns to database fields
                first_name = row.get('first_name', row.get('First Name', '')).strip()
                last_name = row.get('last_name', row.get('Last Name', '')).strip()
                email = row.get('email', row.get('Email', '')).strip()
                company = row.get('company', row.get('Company', '')).strip()
                domain = row.get('domain', row.get('Domain', '')).strip()
                label = row.get('label', row.get('Label', row.get('Job Title', ''))).strip()
                description = row.get('description', row.get('Description', '')).strip()
                source = row.get('source', row.get('Source', 'CSV Import')).strip()
                
                try:
                    score = int(row.get('score', row.get('Score', 5)))
                except (ValueError, TypeError):
                    score = 5
                
                # Skip empty rows
                if not email or not first_name:
                    continue
                
                leads_data.append({
                    'first_name': first_name,
                    'last_name': last_name,
                    'email': email,
                    'domain': domain,
                    'score': score,
                    'company': company,
                    'label': label,
                    'description': description,
                    'source': source
                })
            
            # Remove duplicates
            unique_leads, duplicate_count = remove_duplicate_leads(leads_data)
            
            if not unique_leads:
                flash('No valid profiles found in CSV file', 'error')
                return redirect(url_for('campaigns'))
            
            db = get_db()
            cursor = db.cursor()
            
            # Create new campaign
            cursor.execute("""
                INSERT INTO campaigns (name, status, created_at) 
                VALUES (?, 'pending', ?)
            """, (campaign_name, datetime.now()))
            campaign_id = cursor.lastrowid
            
            # Insert unique leads
            leads_added = 0
            for lead in unique_leads:
                cursor.execute("""
                    INSERT INTO leads (first_name, last_name, email, domain, score, company, label, description, source, campaign_id, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """, (
                    lead['first_name'], lead['last_name'], lead['email'], lead['domain'],
                    lead['score'], lead['company'], lead['label'], lead['description'],
                    lead['source'], campaign_id, datetime.now()
                ))
                leads_added += 1
            
            db.commit()
            
            success_message = f'Successfully uploaded {leads_added} unique profiles to campaign "{campaign_name}"'
            if duplicate_count > 0:
                success_message += f' - Removed {duplicate_count} duplicate profiles'
                
            flash(success_message, 'success')
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
        SELECT id, first_name, last_name, email, company, domain, score, label, description, source, is_active 
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
    flash('Profile deleted successfully!', 'success')
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
        flash(f'Profile {status_text} successfully!', 'success')
    else:
        flash('Profile not found!', 'error')
    
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
        flash(f'Successfully deleted {len(lead_ids)} profiles!', 'success')
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
        flash(f'Successfully {action} {len(lead_ids)} profiles!', 'success')
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

# --- ADD LEAD ---
# --- ADD LEAD --- (FIXED VERSION)
@app.route('/add_lead/<int:campaign_id>', methods=['GET', 'POST'])
def add_lead(campaign_id):
    if request.method == 'POST':
        data = request.form
        
        # Validate required fields
        required_fields = ['first_name', 'last_name', 'email', 'company', 'label', 'description']
        for field in required_fields:
            if not data.get(field, '').strip():
                flash(f'{field.replace("_", " ").title()} is required', 'error')
                return render_template("add_lead.html", campaign_id=campaign_id)
        
        # Validate email format
        email = data['email'].strip()
        if '@' not in email or '.' not in email.split('@')[1]:
            flash('Please enter a valid email address', 'error')
            return render_template("add_lead.html", campaign_id=campaign_id)
        
        try:
            db = get_db()
            cursor = db.cursor()
            
            # Check for duplicate email in the same campaign
            cursor.execute("SELECT id FROM leads WHERE email = ? AND campaign_id = ?", (email, campaign_id))
            if cursor.fetchone():
                flash('A profile with this email already exists in this campaign', 'error')
                return render_template("add_lead.html", campaign_id=campaign_id)
            
            # Check if leads table has the required columns
            cursor.execute("PRAGMA table_info(leads)")
            leads_columns_info = cursor.fetchall()
            leads_columns = [col[1] for col in leads_columns_info]
            
            has_source = 'source' in leads_columns
            has_is_active = 'is_active' in leads_columns
            has_created_at = 'created_at' in leads_columns
            
            # Build dynamic INSERT query based on available columns
            columns = ['campaign_id', 'first_name', 'last_name', 'email', 'domain', 'score', 'company', 'label', 'description']
            values = [
                campaign_id,
                data['first_name'].strip(),
                data['last_name'].strip(),
                email,
                data.get('domain', '').strip(),
                int(data.get('score', 5)),
                data['company'].strip(),
                data.get('label', '').strip(),
                data.get('description', '').strip()
            ]
            
            if has_source:
                columns.append('source')
                values.append(data.get('source', 'Manual').strip())
            
            if has_is_active:
                columns.append('is_active')
                values.append(1)
            
            if has_created_at:
                columns.append('created_at')
                values.append(datetime.now())
            
            # Create the INSERT query
            columns_str = ', '.join(columns)
            placeholders_str = ', '.join(['?' for _ in columns])
            
            cursor.execute(f"""
                INSERT INTO leads ({columns_str})
                VALUES ({placeholders_str})
            """, values)
            
            db.commit()
            flash('Profile added successfully!', 'success')
            return redirect(url_for('campaign_detail', campaign_id=campaign_id))
            
        except Exception as e:
            flash(f'Error adding profile: {str(e)}', 'error')
            return render_template("add_lead.html", campaign_id=campaign_id)
            
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
        SELECT first_name, last_name, email, company, domain, score, label, description, source
        FROM leads WHERE campaign_id = ? AND is_active = 1
    """, (campaign_id,))
    leads = cursor.fetchall()

    if not leads:
        flash('No active profiles found in this campaign', 'error')
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
            "source": lead[8],
        } for lead in leads
    ]

    payload = {
        "campaign_id": campaign_id,
        "leads": leads_data
    }

    # 3. Send to n8n
    #webhook_url = "https://frog-more-lizard.ngrok-free.app/webhook/f7ecb2fe-1f9c-4920-be0d-2cd6bbc93561" #active
    webhook_url = "https://frog-more-lizard.ngrok-free.app/webhook-test/f7ecb2fe-1f9c-4920-be0d-2cd6bbc93561" #test
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        flash(f'Campaign processed successfully! ({len(leads_data)} active profiles sent)', 'success')
        print(f"âœ… Sent {len(leads_data)} active leads to n8n successfully.")
    except requests.RequestException as e:
        flash(f'Failed to process campaign: {str(e)}', 'error')
        print(f"âŒ Failed to send to n8n: {e}")

    return redirect(url_for('campaigns'))

# --- UPLOAD ENDPOINT (FROM N8N) WITH DUPLICATE DETECTION ---
# --- UPLOAD ENDPOINT (FROM N8N) WITH DUPLICATE DETECTION --- (FIXED VERSION)
@app.route('/upload', methods=['POST'])
def upload_leads():
    data = request.get_json()
    print(f"Received data: {data}")  # Debug logging
    
    # Handle both array and object formats
    if isinstance(data, list):
        if len(data) > 0:
            data = data[0]
        else:
            return jsonify({"status": "error", "message": "Empty data array"}), 400
    
    campaign_name = data.get("campaign_name")
    leads = data.get("leads")

    if not campaign_name or not leads:
        return jsonify({"status": "error", "message": "Missing campaign_name or leads"}), 400

    try:
        # Process leads data and remove duplicates
        leads_data = []
        for lead in leads:
            description = lead.get("description") or lead.get("Description", "")
            source = lead.get("source") or lead.get("Source", "API Import")
            
            first_name = lead.get("first_name", "").strip()
            last_name = lead.get("last_name", "").strip()
            email = lead.get("email", "").strip()
            company = lead.get("company", "").strip()
            
            if not email or not first_name:
                continue
            
            try:
                score = int(lead.get("score", 5))
            except (ValueError, TypeError):
                score = 5
                
            leads_data.append({
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'domain': lead.get("domain", ""),
                'score': score,
                'company': company,
                'label': lead.get("label", ""),
                'description': description,
                'source': source
            })
        
        # Remove duplicates
        unique_leads, duplicate_count = remove_duplicate_leads(leads_data)
        
        if not unique_leads:
            return jsonify({"status": "error", "message": "No valid unique profiles found"}), 400

        db = get_db()
        cursor = db.cursor()

        # Check if campaigns table has created_at column
        cursor.execute("PRAGMA table_info(campaigns)")
        campaigns_columns_info = cursor.fetchall()
        campaigns_columns = [col[1] for col in campaigns_columns_info]
        has_created_at = 'created_at' in campaigns_columns

        # Create new campaign with conditional created_at column
        if has_created_at:
            cursor.execute("""
                INSERT INTO campaigns (name, status, created_at) 
                VALUES (?, 'pending', ?)
            """, (campaign_name, datetime.now()))
        else:
            cursor.execute("""
                INSERT INTO campaigns (name, status) 
                VALUES (?, 'pending')
            """, (campaign_name,))
        
        campaign_id = cursor.lastrowid

        # Check if leads table has the required columns
        cursor.execute("PRAGMA table_info(leads)")
        leads_columns_info = cursor.fetchall()
        leads_columns = [col[1] for col in leads_columns_info]
        
        has_source = 'source' in leads_columns
        has_is_active = 'is_active' in leads_columns
        has_leads_created_at = 'created_at' in leads_columns

        leads_added = 0
        for lead in unique_leads:
            # Build dynamic INSERT query based on available columns
            columns = ['campaign_id', 'first_name', 'last_name', 'email', 'domain', 'score', 'company', 'label', 'description']
            values = [
                campaign_id,
                lead['first_name'],
                lead['last_name'],
                lead['email'],
                lead['domain'],
                lead['score'],
                lead['company'],
                lead['label'],
                lead['description']
            ]
            
            if has_source:
                columns.append('source')
                values.append(lead['source'])
            
            if has_is_active:
                columns.append('is_active')
                values.append(1)
            
            if has_leads_created_at:
                columns.append('created_at')
                values.append(datetime.now())
            
            # Create the INSERT query
            columns_str = ', '.join(columns)
            placeholders_str = ', '.join(['?' for _ in columns])
            
            cursor.execute(f"""
                INSERT INTO leads ({columns_str})
                VALUES ({placeholders_str})
            """, values)
            leads_added += 1

        db.commit()
        
        response_data = {
            "status": "success", 
            "campaign_id": campaign_id,
            "leads_added": leads_added,
            "duplicates_removed": duplicate_count
        }
        
        print(f"âœ… Campaign created: {campaign_name} with {leads_added} unique leads (removed {duplicate_count} duplicates)")
        return jsonify(response_data)
        
    except Exception as e:
        print(f"âŒ Error processing upload: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

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

# --- GET CAMPAIGN STATS (API ENDPOINT) ---
@app.route('/api/campaign/<int:campaign_id>/stats')
def get_campaign_stats(campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ? AND is_active = 1", (campaign_id,))
    active_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ? AND is_active = 0", (campaign_id,))
    inactive_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ?", (campaign_id,))
    total_count = cursor.fetchone()[0]
    
    return jsonify({
        "active": active_count,
        "inactive": inactive_count,
        "total": total_count
    })

# --- EXPORT CAMPAIGN DATA ---
@app.route('/export_campaign/<int:campaign_id>')
def export_campaign(campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    # Get campaign name
    cursor.execute("SELECT name FROM campaigns WHERE id = ?", (campaign_id,))
    campaign = cursor.fetchone()
    campaign_name = campaign[0] if campaign else f"Campaign_{campaign_id}"
    
    # Get all leads
    cursor.execute("""
        SELECT first_name, last_name, email, company, domain, score, label, description, source, is_active
        FROM leads WHERE campaign_id = ? ORDER BY id
    """, (campaign_id,))
    leads = cursor.fetchall()
    
    # Create CSV response
    from flask import Response
    import io
    import csv
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['First Name', 'Last Name', 'Email', 'Company', 'Domain', 'Score', 'Label', 'Description', 'Source', 'Status'])
    
    # Write data
    for lead in leads:
        status = 'Active' if lead[9] == 1 else 'Inactive'
        writer.writerow([lead[0], lead[1], lead[2], lead[3], lead[4], lead[5], lead[6], lead[7], lead[8], status])
    
    output.seek(0)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={"Content-disposition": f"attachment; filename={campaign_name}_export.csv"}
    )

# --- API ENDPOINT TO CHECK FOR DUPLICATES ---
@app.route('/api/check_duplicates', methods=['POST'])
def check_duplicates():
    """API endpoint to check for duplicate emails across all campaigns or within specific campaigns"""
    data = request.get_json()
    emails = data.get('emails', [])
    campaign_ids = data.get('campaign_ids', [])  # Optional: check within specific campaigns
    
    if not emails:
        return jsonify({"error": "No emails provided"}), 400
    
    db = get_db()
    cursor = db.cursor()
    
    duplicates = []
    
    for email in emails:
        email = email.lower().strip()
        if campaign_ids:
            # Check within specific campaigns
            placeholders = ','.join('?' for _ in campaign_ids)
            cursor.execute(f"""
                SELECT l.email, c.name, c.id 
                FROM leads l 
                JOIN campaigns c ON l.campaign_id = c.id 
                WHERE LOWER(l.email) = ? AND l.campaign_id IN ({placeholders})
            """, [email] + campaign_ids)
        else:
            # Check across all campaigns
            cursor.execute("""
                SELECT l.email, c.name, c.id 
                FROM leads l 
                JOIN campaigns c ON l.campaign_id = c.id 
                WHERE LOWER(l.email) = ?
            """, (email,))
        
        existing = cursor.fetchall()
        if existing:
            duplicates.append({
                "email": email,
                "found_in": [{"campaign_name": row[1], "campaign_id": row[2]} for row in existing]
            })
    
    return jsonify({
        "duplicates_found": len(duplicates),
        "duplicates": duplicates
    })

# --- API ENDPOINT TO GET CAMPAIGN MERGE PREVIEW ---
@app.route('/api/merge_preview', methods=['POST'])
def merge_preview():
    """Get a preview of what would happen when merging campaigns"""
    campaign_ids = request.json.get('campaign_ids', [])
    
    if len(campaign_ids) < 2:
        return jsonify({"error": "At least 2 campaigns required"}), 400
    
    db = get_db()
    cursor = db.cursor()
    
    # Get campaign details
    placeholders = ','.join('?' for _ in campaign_ids)
    cursor.execute(f"""
        SELECT id, name, COUNT(l.id) as profile_count
        FROM campaigns c
        LEFT JOIN leads l ON c.id = l.campaign_id
        WHERE c.id IN ({placeholders})
        GROUP BY c.id, c.name
    """, campaign_ids)
    
    campaigns_info = cursor.fetchall()
    
    # Get all leads from selected campaigns
    cursor.execute(f"""
        SELECT email, first_name, last_name, company
        FROM leads WHERE campaign_id IN ({placeholders})
    """, campaign_ids)
    
    all_leads = cursor.fetchall()
    
    # Calculate duplicates
    email_count = {}
    for lead in all_leads:
        email = lead[0].lower().strip()
        if email in email_count:
            email_count[email] += 1
        else:
            email_count[email] = 1
    
    duplicates = {email: count for email, count in email_count.items() if count > 1}
    unique_count = len(email_count)
    total_count = len(all_leads)
    duplicate_count = total_count - unique_count
    
    return jsonify({
        "campaigns": [{"id": c[0], "name": c[1], "profile_count": c[2]} for c in campaigns_info],
        "total_profiles": total_count,
        "unique_profiles": unique_count,
        "duplicate_profiles": duplicate_count,
        "duplicates_detail": duplicates
    })

# Add this new route to your Flask app (test_upload.py)

# Update your existing merge_campaigns_page route in test_upload.py

@app.route('/merge_campaigns_page')
def merge_campaigns_page():
    """Display the merge campaigns page - show previously merged campaigns in table"""
    db = get_db()
    cursor = db.cursor()
    
    # Check if is_merged column exists
    cursor.execute("PRAGMA table_info(campaigns)")
    columns_info = cursor.fetchall()
    has_is_merged = any(col[1] == 'is_merged' for col in columns_info)
    
    if has_is_merged:
        # Get only previously merged campaigns for the table
        cursor.execute("""
            SELECT c.id, c.name, c.status, COUNT(l.id) as profile_count
            FROM campaigns c
            LEFT JOIN leads l ON c.id = l.campaign_id
            WHERE c.is_merged = 1
            GROUP BY c.id, c.name, c.status
            ORDER BY c.id DESC
        """)
    else:
        # If is_merged column doesn't exist, show all campaigns
        # You might want to create an empty list instead
        campaigns = []
    
    campaigns = cursor.fetchall() if has_is_merged else []
    return render_template("merge_campaigns_page.html", campaigns=campaigns)

# Add this new route to your test_upload.py file

@app.route('/api/available_campaigns')
def get_available_campaigns():
    """Get all campaigns available for merging (excluding already merged campaigns if needed)"""
    db = get_db()
    cursor = db.cursor()
    
    # Get all campaigns with their profile counts
    # You can modify this query to exclude certain campaigns if needed
    cursor.execute("""
        SELECT c.id, c.name, c.status, COUNT(l.id) as profile_count
        FROM campaigns c
        LEFT JOIN leads l ON c.id = l.campaign_id
        WHERE c.id IS NOT NULL
        GROUP BY c.id, c.name, c.status
        ORDER BY c.id DESC
    """)
    
    campaigns = cursor.fetchall()
    
    # Convert to list of dictionaries
    campaigns_list = []
    for campaign in campaigns:
        campaigns_list.append({
            'id': campaign[0],
            'name': campaign[1],
            'status': campaign[2],
            'profile_count': campaign[3] if campaign[3] else 0
        })
    
    return jsonify(campaigns_list)



if __name__ == '__main__':
    init_db()
    app.run(debug=True)