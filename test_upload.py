from flask import Flask, request, jsonify, render_template, g, redirect, url_for, flash
import sqlite3
import requests
import csv
import io
from werkzeug.utils import secure_filename
import os
from datetime import datetime
import uuid
import secrets

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
        # Add new column for unsubscribe status
        try:
            c.execute("ALTER TABLE leads ADD COLUMN unsubscribe_status TEXT DEFAULT 'subscribed'")
            print("✅ Added unsubscribe_status column to leads table")
        except sqlite3.OperationalError:
            print("ℹ️ unsubscribe_status column already exists")
            
        # Add unique token column for unsubscribe links
        try:
            c.execute("ALTER TABLE leads ADD COLUMN unsubscribe_token TEXT")
            print("✅ Added unsubscribe_token column to leads table")
        except sqlite3.OperationalError:
            print("ℹ️ unsubscribe_token column already exists")

        # Add this inside your init_db() function after the existing ALTER TABLE statements
        try:
            c.execute("ALTER TABLE leads ADD COLUMN email_status TEXT DEFAULT 'subscribed'")
            print("✅ Added email_status column to leads table")
        except sqlite3.OperationalError:
            print("ℹ️ email_status column already exists")
            # Add this inside your init_db() function after existing ALTER TABLE statements
        try:
            c.execute("ALTER TABLE campaigns ADD COLUMN description TEXT")
            print("✅ Added description column to campaigns table")
        except sqlite3.OperationalError:
            print("ℹ️ description column already exists")
            # Ensure existing distributed lists are marked correctly
        try:
            c.execute("UPDATE campaigns SET is_merged = 0 WHERE is_merged IS NULL")
            print("✅ Updated existing campaigns with is_merged = 0")
        except sqlite3.OperationalError:
            pass

            # Add processing tracking columns
        try:
            c.execute("ALTER TABLE campaigns ADD COLUMN last_processed_at TIMESTAMP")
            print("✅ Added last_processed_at column to campaigns table")
        except sqlite3.OperationalError:
            print("ℹ️ last_processed_at column already exists")
        try:
            c.execute("ALTER TABLE campaigns ADD COLUMN process_count INTEGER DEFAULT 0")
            print("✅ Added process_count column to campaigns table")
        except sqlite3.OperationalError:
            print("ℹ️ process_count column already exists")
        try:
            c.execute("ALTER TABLE campaigns ADD COLUMN processing_status TEXT DEFAULT 'not_sent'")
            print("✅ Added processing_status column to campaigns table")
        except sqlite3.OperationalError:
            print("ℹ️ processing_status column already exists")

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

def remove_duplicate_leads_with_status(leads_data):
    """
    Remove duplicate leads based on email address while preserving the LATEST email status
    Returns unique leads and duplicate count
    """
    email_map = {}
    duplicate_count = 0
    
    for lead in leads_data:
        email = lead.get('email', '').lower().strip()
        if not email:
            continue
            
        if email in email_map:
            # Duplicate found - keep the one with more recent/explicit status
            existing_lead = email_map[email]
            
            # Priority logic for preserving email status:
            # 1. Manual email_status takes precedence
            # 2. If both have manual status, keep current
            # 3. If neither has manual status, preserve external unsubscribe_status
            
            current_email_status = lead.get('email_status')
            existing_email_status = existing_lead.get('email_status')
            
            if current_email_status and not existing_email_status:
                # Current has manual status, existing doesn't - use current
                email_map[email] = lead
            elif not current_email_status and existing_email_status:
                # Existing has manual status, current doesn't - keep existing
                pass
            elif current_email_status == 'unsubscribed' or existing_email_status == 'unsubscribed':
                # If either is manually unsubscribed, preserve that status
                if current_email_status == 'unsubscribed':
                    email_map[email] = lead
                # else keep existing (which has unsubscribed status)
            else:
                # For other cases, use the current lead (most recent)
                email_map[email] = lead
            
            duplicate_count += 1
        else:
            email_map[email] = lead
    
    unique_leads = list(email_map.values())
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
        SELECT c.id, c.name, c.status, c.description, COUNT(l.id) as profile_count, 
               c.processing_status, c.last_processed_at, c.process_count
        FROM campaigns c
        LEFT JOIN leads l ON c.id = l.campaign_id
        WHERE (c.is_merged IS NULL OR c.is_merged = 0)
        GROUP BY c.id, c.name, c.status, c.description, c.processing_status, c.last_processed_at, c.process_count
        ORDER BY c.id DESC
    """)
    campaigns = cursor.fetchall()
    return render_template("campaigns.html", campaigns=campaigns)

# --- MERGE CAMPAIGNS ---
# Replace your merge_campaigns function with this updated version
# This version handles the missing created_at column gracefully

@app.route('/merge_campaigns', methods=['POST'])
def merge_campaigns():
    campaign_ids = request.form.getlist('campaign_ids[]')
    merged_campaign_name = request.form.get('merged_campaign_name', '').strip()
    merged_campaign_description = request.form.get('merged_campaign_description', '').strip()
    
    if len(campaign_ids) < 2:
        flash('Please select at least 2 campaigns to merge', 'error')
        return redirect(url_for('campaigns'))
    
    if not merged_campaign_name:
        flash('Please provide a name for the merged campaign', 'error')
        return redirect(url_for('campaigns'))
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Check if campaigns table has required columns
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
        
        # Get all leads from selected campaigns INCLUDING email subscription status
        cursor.execute(f"""
            SELECT first_name, last_name, email, domain, score, company, label, description, 
                   COALESCE(source, 'Merged Campaign') as source,
                   email_status, unsubscribe_status, unsubscribe_token
            FROM leads WHERE campaign_id IN ({placeholders})
        """, campaign_ids)
        all_leads = cursor.fetchall()
        
        if not all_leads:
            flash('No profiles found in selected campaigns', 'error')
            return redirect(url_for('campaigns'))
        
        # Convert to list of dictionaries for duplicate removal (PRESERVE EMAIL STATUS)
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
                'source': lead[8] or 'Merged Campaign',
                'email_status': lead[9],
                'unsubscribe_status': lead[10],
                'unsubscribe_token': lead[11] or generate_unsubscribe_token()
            })
        
        # Remove duplicates based on email (but preserve the LATEST email status)
        unique_leads, duplicate_count = remove_duplicate_leads_with_status(leads_data)
        
        if not unique_leads:
            flash('No valid profiles found after removing duplicates', 'error')
            return redirect(url_for('campaigns'))
        
        # Create new merged campaign - ALWAYS set is_merged = 1 for merged campaigns
        if has_created_at and has_is_merged:
            cursor.execute("""
                INSERT INTO campaigns (name, description, status, created_at, is_merged) 
                VALUES (?, ?, 'pending', ?, 1)
            """, (merged_campaign_name, merged_campaign_description, datetime.now()))
        elif has_created_at:
            cursor.execute("""
                INSERT INTO campaigns (name, description, status, created_at) 
                VALUES (?, ?, 'pending', ?)
            """, (merged_campaign_name, merged_campaign_description, datetime.now()))
        elif has_is_merged:
            cursor.execute("""
                INSERT INTO campaigns (name, description, status, is_merged) 
                VALUES (?, ?, 'pending', 1)
            """, (merged_campaign_name, merged_campaign_description))
        else:
            cursor.execute("""
                INSERT INTO campaigns (name, description, status) 
                VALUES (?, ?, 'pending')
            """, (merged_campaign_name, merged_campaign_description))
        
        merged_campaign_id = cursor.lastrowid
        
        # Ensure merged campaign is marked as merged (for cases where column exists)
        if has_is_merged:
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
        has_email_status = 'email_status' in leads_columns
        has_unsubscribe_status = 'unsubscribe_status' in leads_columns
        has_unsubscribe_token = 'unsubscribe_token' in leads_columns
        
        # Insert unique leads into the new merged campaign (PRESERVE EMAIL STATUS)
        leads_added = 0
        for lead in unique_leads:
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
            
            # PRESERVE EMAIL SUBSCRIPTION STATUS
            if has_email_status:
                columns.append('email_status')
                values.append(lead.get('email_status'))
            
            if has_unsubscribe_status:
                columns.append('unsubscribe_status')
                values.append(lead.get('unsubscribe_status'))
            
            if has_unsubscribe_token:
                columns.append('unsubscribe_token')
                values.append(lead.get('unsubscribe_token'))
            
            # Create the INSERT query
            columns_str = ', '.join(columns)
            placeholders_str = ', '.join(['?' for _ in columns])
            
            cursor.execute(f"""
                INSERT INTO leads ({columns_str})
                VALUES ({placeholders_str})
            """, values)
            leads_added += 1
        
        # DO NOT DELETE OR MARK ORIGINAL CAMPAIGNS - LEAVE THEM AS DISTRIBUTED LISTS
        # Original campaigns remain in first tab with is_merged = 0 or NULL
        
        db.commit()
        
        campaign_names = [camp[1] for camp in existing_campaigns]
        success_message = f'Successfully merged {len(campaign_ids)} distributed lists into "{merged_campaign_name}"'
        if duplicate_count > 0:
            success_message += f' - Removed {duplicate_count} duplicate profiles'
        success_message += f' - {leads_added} unique profiles added (original lists preserved)'
        
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
    campaign_description = request.form.get('campaign_description', '').strip()
    
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('campaigns'))
    ff
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
                INSERT INTO campaigns (name,description, status, created_at) 
                VALUES (?, ?,'pending', ?)
            """, (campaign_name,campaign_description, datetime.now()))
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
# Replace your existing campaign_detail route with this updated version:

@app.route('/campaign/<int:campaign_id>')
def campaign_detail(campaign_id):
    db = get_db()
    cursor = db.cursor()
    
    # Get campaign info
    cursor.execute("SELECT name FROM campaigns WHERE id = ?", (campaign_id,))
    campaign = cursor.fetchone()
    
    # UPDATED: Include email_status in the query (position 11)
    cursor.execute("""
        SELECT id, first_name, last_name, email, company, domain, score, label, description, source, is_active, email_status
        FROM leads WHERE campaign_id = ? ORDER BY id
    """, (campaign_id,))
    leads = cursor.fetchall()
    
    campaign_name = campaign[0] if campaign else f"Campaign {campaign_id}"
    
    # Get referrer parameter to determine where to go back
    referrer = request.args.get('referrer', 'campaigns')  # default to 'campaigns'
    
    return render_template("campaign_detail.html", 
                         leads=leads, 
                         campaign_id=campaign_id, 
                         campaign_name=campaign_name,
                         referrer=referrer)

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
# Replace your existing add_lead route with this updated version:

@app.route('/add_lead/<int:campaign_id>', methods=['GET', 'POST'])
def add_lead(campaign_id):
    # Get referrer parameter
    referrer = request.args.get('referrer', 'campaigns')
    
    if request.method == 'POST':
        data = request.form
        
        # Validate required fields
        required_fields = ['first_name', 'last_name', 'email', 'company', 'label', 'description']
        for field in required_fields:
            if not data.get(field, '').strip():
                flash(f'{field.replace("_", " ").title()} is required', 'error')
                return render_template("add_lead.html", campaign_id=campaign_id, referrer=referrer)
        
        # Validate email format
        email = data['email'].strip()
        if '@' not in email or '.' not in email.split('@')[1]:
            flash('Please enter a valid email address', 'error')
            return render_template("add_lead.html", campaign_id=campaign_id, referrer=referrer)
        
        try:
            db = get_db()
            cursor = db.cursor()
            
            # Check for duplicate email in the same campaign
            cursor.execute("SELECT id FROM leads WHERE email = ? AND campaign_id = ?", (email, campaign_id))
            if cursor.fetchone():
                flash('A profile with this email already exists in this campaign', 'error')
                return render_template("add_lead.html", campaign_id=campaign_id, referrer=referrer)
            
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
            return redirect(url_for('campaign_detail', campaign_id=campaign_id, referrer=referrer))
            
        except Exception as e:
            flash(f'Error adding profile: {str(e)}', 'error')
            return render_template("add_lead.html", campaign_id=campaign_id, referrer=referrer)
            
    return render_template("add_lead.html", campaign_id=campaign_id, referrer=referrer)

# --- SEND TO N8N (ONLY ACTIVE LEADS) ---
# # Add this function to your test_upload.py

# def generate_email_body_with_unsubscribe(lead_data, base_url="http://localhost:5000"):
#     """
#     Generate email body with unsubscribe link
    
#     Args:
#         lead_data: Dictionary containing lead information
#         base_url: Your application's base URL
        
#     Returns:
#         String containing HTML email body with unsubscribe link
#     """
    
#     first_name = lead_data.get('first_name', '')
#     last_name = lead_data.get('last_name', '')
#     email = lead_data.get('email', '')
#     company = lead_data.get('company', '')
#     unsubscribe_token = lead_data.get('unsubscribe_token', '')
    
#     # Construct unsubscribe URL
#     unsubscribe_url = f"{base_url}/unsubscribe/{unsubscribe_token}"
    
#     # Email template (you can customize this)
#     email_body = f"""
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <style>
#             body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
#             .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
#             .header {{ background: #667eea; color: white; padding: 20px; text-align: center; }}
#             .content {{ padding: 20px; background: #f9f9f9; }}
#             .footer {{ padding: 20px; font-size: 12px; color: #666; }}
#             .unsubscribe {{ margin-top: 20px; padding: 15px; background: #fff3cd; border-left: 4px solid #ffc107; }}
#             .unsubscribe a {{ color: #856404; text-decoration: none; font-weight: bold; }}
#         </style>
#     </head>
#     <body>
#         <div class="container">
#             <div class="header">
#                 <h2>Hello {first_name} {last_name}!</h2>
#             </div>
            
#             <div class="content">
#                 <p>We hope this email finds you well.</p>
                
#                 <p>We're reaching out from <strong>{company}</strong> regarding our latest campaign.</p>
                
#                 <p>[Your main email content goes here]</p>
                
#                 <p>Best regards,<br>
#                 Your Marketing Team</p>
#             </div>
            
#             <div class="footer">
#                 <div class="unsubscribe">
#                     <p><strong>Not interested?</strong></p>
#                     <p>If you no longer wish to receive emails from us, you can 
#                     <a href="{unsubscribe_url}">unsubscribe here</a>.</p>
#                     <p><small>This will remove you from all future email campaigns.</small></p>
#                 </div>
                
#                 <hr>
#                 <p>This email was sent to {email}</p>
#                 <p>© 2024 Your Company Name. All rights reserved.</p>
#             </div>
#         </div>
#     </body>
#     </html>
#     """
    
#     return email_body.strip()

# Modify your send_to_n8n function to include unsubscribe tokens
@app.route('/send_to_n8n/<int:campaign_id>', methods=['POST'])
def send_to_n8n(campaign_id):
    db = get_db()
    cursor = db.cursor()

    # Get excluded lead IDs from form if provided
    excluded_lead_ids = request.form.getlist('excluded_leads[]')
    # Get included lead IDs from form if provided (for include-only mode)
    included_lead_ids = request.form.getlist('included_leads[]')
    
    # Check campaign status
    cursor.execute("SELECT status FROM campaigns WHERE id = ?", (campaign_id,))
    status = cursor.fetchone()
    if not status or status[0] != 'approved':
        flash('Campaign is not approved', 'error')
        return redirect(url_for('campaigns'))

    # Build the query based on include/exclude mode
    if included_lead_ids:
        # Include mode: only process selected leads
        placeholders = ','.join('?' for _ in included_lead_ids)
        base_query = f"""
            SELECT id, first_name, last_name, email, company, domain, score, label, description, source, unsubscribe_token
            FROM leads 
            WHERE campaign_id = ? 
            AND is_active = 1 
            AND id IN ({placeholders})
            AND (
                email_status = 'subscribed'
                OR 
                (
                    (email_status IS NULL OR email_status = 'subscribed') 
                    AND (unsubscribe_status IS NULL OR unsubscribe_status = 'subscribed')
                )
            )
        """
        query_params = [campaign_id] + included_lead_ids
        mode_message = f"include only {len(included_lead_ids)} selected leads"
        
    else:
        # Exclude mode (default): process all except excluded leads
        base_query = """
            SELECT id, first_name, last_name, email, company, domain, score, label, description, source, unsubscribe_token
            FROM leads 
            WHERE campaign_id = ? 
            AND is_active = 1 
            AND (
                email_status = 'subscribed'
                OR 
                (
                    (email_status IS NULL OR email_status = 'subscribed') 
                    AND (unsubscribe_status IS NULL OR unsubscribe_status = 'subscribed')
                )
            )
        """
        query_params = [campaign_id]
        
        # Add exclusion condition if there are excluded leads
        if excluded_lead_ids:
            placeholders = ','.join('?' for _ in excluded_lead_ids)
            base_query += f" AND id NOT IN ({placeholders})"
            query_params.extend(excluded_lead_ids)
            mode_message = f"exclude {len(excluded_lead_ids)} selected duplicates"
        else:
            mode_message = "process all leads (no exclusions)"
    
    cursor.execute(base_query, query_params)
    leads = cursor.fetchall()

    if not leads:
        flash('No active, subscribed profiles found after filtering', 'error')
        return redirect(url_for('campaigns'))

    # Rest of your existing send_to_n8n logic remains the same
    leads_data = []
    base_url = request.url_root.rstrip('/')
    leads_without_tokens = []
    
    for lead in leads:
        lead_id = lead[0]
        unsubscribe_token = lead[10]
        
        if not unsubscribe_token:
            unsubscribe_token = generate_unsubscribe_token()
            cursor.execute("UPDATE leads SET unsubscribe_token = ? WHERE id = ?", (unsubscribe_token, lead_id))
            leads_without_tokens.append(lead_id)
        
        unsubscribe_url = f"{base_url}/unsubscribe/{unsubscribe_token}"
        
        lead_dict = {
            "lead_id": lead_id,
            "first_name": lead[1],
            "last_name": lead[2], 
            "email": lead[3],
            "company": lead[4],
            "domain": lead[5],
            "score": lead[6],
            "label": lead[7],
            "description": lead[8],
            "source": lead[9],
            "unsubscribe_url": unsubscribe_url,
            "unsubscribe_token": unsubscribe_token
        }
        
        leads_data.append(lead_dict)
    
    # Commit any token updates
    if leads_without_tokens:
        db.commit()

    # Continue with existing n8n sending logic...
    payload = {
        "campaign_id": campaign_id,
        "total_leads": len(leads_data),
        "processing_mode": mode_message,
        "leads": leads_data
    }

    webhook_url = "https://dory-logical-briefly.ngrok-free.app/webhook-test/f7ecb2fe-1f9c-4920-be0d-2cd6bbc93561"
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()

        cursor.execute("""
        UPDATE campaigns 
        SET processing_status = 'sent', 
            last_processed_at = ?, 
            process_count = COALESCE(process_count, 0) + 1
        WHERE id = ?
        """, (datetime.now(), campaign_id))
        db.commit()
        
        flash(f'Campaign processed successfully! ({len(leads_data)} profiles sent to n8n, mode: {mode_message})', 'success')

    except requests.RequestException as e:
        cursor.execute("""
        UPDATE campaigns 
        SET processing_status = 'failed'
        WHERE id = ?
        """, (campaign_id,))
        db.commit()
        flash(f'Failed to process campaign: {str(e)}', 'error')

    return redirect(url_for('campaigns'))
# Optional: Add a route to get just the unsubscribe URL for a specific lead
@app.route('/api/lead/<int:lead_id>/unsubscribe_url')
def get_lead_unsubscribe_url(lead_id):
    """Get unsubscribe URL for a specific lead"""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("SELECT unsubscribe_token FROM leads WHERE id = ?", (lead_id,))
    result = cursor.fetchone()
    
    if not result:
        return jsonify({"error": "Lead not found"}), 404
    
    token = result[0]
    if not token:
        # Generate token if it doesn't exist
        token = generate_unsubscribe_token()
        cursor.execute("UPDATE leads SET unsubscribe_token = ? WHERE id = ?", (token, lead_id))
        db.commit()
    
    base_url = request.url_root.rstrip('/')
    unsubscribe_url = f"{base_url}/unsubscribe/{token}"
    
    return jsonify({
        "lead_id": lead_id,
        "unsubscribe_url": unsubscribe_url,
        "unsubscribe_token": token
    })

# Optional: Bulk get unsubscribe URLs for multiple leads
@app.route('/api/campaign/<int:campaign_id>/unsubscribe_urls')
def get_campaign_unsubscribe_urls(campaign_id):
    """Get all unsubscribe URLs for leads in a campaign"""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT id, email, first_name, last_name, unsubscribe_token
        FROM leads 
        WHERE campaign_id = ? 
        AND is_active = 1 
        AND (unsubscribe_status IS NULL OR unsubscribe_status = 'subscribed')
    """, (campaign_id,))
    
    leads = cursor.fetchall()
    base_url = request.url_root.rstrip('/')
    
    urls_data = []
    for lead in leads:
        lead_id, email, first_name, last_name, token = lead
        
        if not token:
            token = generate_unsubscribe_token()
            cursor.execute("UPDATE leads SET unsubscribe_token = ? WHERE id = ?", (token, lead_id))
        
        unsubscribe_url = f"{base_url}/unsubscribe/{token}"
        
        urls_data.append({
            "lead_id": lead_id,
            "email": email,
            "name": f"{first_name} {last_name}",
            "unsubscribe_url": unsubscribe_url
        })
    
    if any(not lead[4] for lead in leads):  # If any tokens were missing
        db.commit()
    
    return jsonify({
        "campaign_id": campaign_id,
        "total_leads": len(urls_data),
        "unsubscribe_urls": urls_data
    })

# --- UPLOAD ENDPOINT (FROM N8N) WITH DUPLICATE DETECTION ---
# Fix the upload_leads function - replace the problematic section with this:

@app.route('/upload', methods=['POST'])
def upload_leads():
    data = request.get_json()
    print(f"Received data: {data}")

    # Handle both array and object formats
    if isinstance(data, list):
        if len(data) > 0:
            data = data[0]
        else:
            return jsonify({"status": "error", "message": "Empty data array"}), 400

    campaign_name = data.get("campaign_name")
    campaign_description = data.get("campaign_description", "")
    leads = data.get("leads")

    if not campaign_name or not leads:
        return jsonify({"status": "error", "message": "Missing campaign_name or leads"}), 400

    try:
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

            # Add unsubscribe token to each lead
            lead_dict = {
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'domain': lead.get("domain", ""),
                'score': score,
                'company': company,
                'label': lead.get("label", ""),
                'description': description,
                'source': source,
                'unsubscribe_token': generate_unsubscribe_token(),  # NEW
                'unsubscribe_status': 'subscribed'  # Default
            }
            leads_data.append(lead_dict)

        # Filter out unsubscribed leads before deduplication
        filtered_leads, unsubscribed_count, unsubscribed_emails = filter_unsubscribed_leads(leads_data)
        if unsubscribed_count > 0:
            print(f"🚫 Filtered out {unsubscribed_count} unsubscribed emails: {unsubscribed_emails}")

        # Remove duplicates
        unique_leads, duplicate_count = remove_duplicate_leads(filtered_leads)

        if not unique_leads:
            return jsonify({
                "status": "error",
                "message": "No valid unique profiles found after filtering unsubscribed leads"
            }), 400

        db = get_db()
        cursor = db.cursor()

        # Campaign insert - FIXED THIS SECTION
        cursor.execute("PRAGMA table_info(campaigns)")
        campaigns_columns = [col[1] for col in cursor.fetchall()]
        has_created_at = 'created_at' in campaigns_columns

        if has_created_at:
            cursor.execute("""
                INSERT INTO campaigns (name, description, status, created_at) 
                VALUES (?, ?, 'pending', ?)
            """, (campaign_name, campaign_description, datetime.now()))
        else:
            cursor.execute("""
                INSERT INTO campaigns (name, description, status) 
                VALUES (?, ?, 'pending')
            """, (campaign_name, campaign_description))  # FIXED: Removed /merge_campaigns
        
        campaign_id = cursor.lastrowid

        # Leads table column checks
        cursor.execute("PRAGMA table_info(leads)")
        leads_columns = [col[1] for col in cursor.fetchall()]

        has_source = 'source' in leads_columns
        has_is_active = 'is_active' in leads_columns
        has_leads_created_at = 'created_at' in leads_columns

        leads_added = 0
        for lead in unique_leads:
            columns = [
                'campaign_id', 'first_name', 'last_name', 'email', 'domain', 
                'score', 'company', 'label', 'description',
                'unsubscribe_token', 'unsubscribe_status'
            ]
            values = [
                campaign_id,
                lead['first_name'],
                lead['last_name'],
                lead['email'],
                lead['domain'],
                lead['score'],
                lead['company'],
                lead['label'],
                lead['description'],
                lead['unsubscribe_token'],
                lead['unsubscribe_status']
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

            placeholders_str = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)

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
            "duplicates_removed": duplicate_count,
            "unsubscribed_filtered": unsubscribed_count
        }

        print(f"✅ Campaign created: {campaign_name} with {leads_added} unique leads")
        return jsonify(response_data)

    except Exception as e:
        print(f"❌ Error processing upload: {str(e)}")
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
            SELECT c.id, c.name, c.status, c.description, COUNT(l.id) as profile_count,
                   c.processing_status, c.last_processed_at, c.process_count
            FROM campaigns c
            LEFT JOIN leads l ON c.id = l.campaign_id
            WHERE c.is_merged = 1 AND c.name NOT LIKE 'Original:%'
            GROUP BY c.id, c.name, c.status, c.description, c.processing_status, c.last_processed_at, c.process_count
            ORDER BY c.id DESC
        """)
    else:
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
        SELECT c.id, c.name, c.status,c.description, COUNT(l.id) as profile_count
        FROM campaigns c
        LEFT JOIN leads l ON c.id = l.campaign_id
        WHERE (c.is_merged IS NULL OR c.is_merged = 0)
        GROUP BY c.id, c.name, c.status,c.description,
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
            'description': campaign[3] or '',
            'profile_count': campaign[4] if campaign[4] else 0
        })
    
    return jsonify(campaigns_list)


def generate_unsubscribe_token():
    """Generate a unique token for unsubscribe links"""
    return secrets.token_urlsafe(32)

def update_lead_tokens():
    """Update existing leads with unsubscribe tokens if they don't have one"""
    db = get_db()
    cursor = db.cursor()
    
    # Get leads without tokens
    cursor.execute("SELECT id FROM leads WHERE unsubscribe_token IS NULL")
    leads_without_tokens = cursor.fetchall()
    
    for lead in leads_without_tokens:
        token = generate_unsubscribe_token()
        cursor.execute("UPDATE leads SET unsubscribe_token = ? WHERE id = ?", (token, lead[0]))
    
    db.commit()
    print(f"✅ Updated {len(leads_without_tokens)} leads with unsubscribe tokens")

# Simplified version - just show confirmation then redirect

@app.route('/unsubscribe/<token>')
def unsubscribe_confirmation(token):
    """Show simple unsubscribe confirmation page"""
    if not token:
        return "Invalid unsubscribe link", 400
    
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT id, first_name, last_name, email, unsubscribe_status 
        FROM leads 
        WHERE unsubscribe_token = ?
    """, (token,))
    
    lead = cursor.fetchone()
    
    if not lead:
        return "Invalid or expired unsubscribe link", 404
    
    lead_id, first_name, last_name, email, current_status = lead
    
    if current_status == 'unsubscribed':
        return f"<h2>Already Unsubscribed</h2><p>{first_name} {last_name}, you are already unsubscribed.</p>"
    
    return render_template("simple_unsubscribe.html", 
                         token=token,
                         name=f"{first_name} {last_name}",
                         email=email)

@app.route('/confirm_unsubscribe/<token>', methods=['POST'])
def confirm_unsubscribe(token):
    """Process unsubscribe and show simple message"""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT id, first_name, last_name, email 
        FROM leads 
        WHERE unsubscribe_token = ?
    """, (token,))
    
    lead = cursor.fetchone()
    
    if not lead:
        return "Invalid link", 404
    
    lead_id, first_name, last_name, email = lead
    
    # Unsubscribe
    cursor.execute("""
        UPDATE leads 
        SET unsubscribe_status = 'unsubscribed', is_active = 0 
        WHERE id = ?
    """, (lead_id,))
    
    db.commit()
    
    # Simple success message
    return f"""
    <div style="text-align: center; font-family: Arial; padding: 50px;">
        <h2 style="color: green;">✅ Unsubscribed Successfully</h2>
        <p><strong>{first_name} {last_name}</strong>, you've been unsubscribed from our emails.</p>
        <p style="color: gray;">You can now close this page.</p>
    </div>
    """

@app.route('/api/check_unsubscribed', methods=['POST'])
def check_unsubscribed_leads():
    """API endpoint to check if leads are unsubscribed before adding to campaigns"""
    data = request.get_json()
    emails = data.get('emails', [])
    
    if not emails:
        return jsonify({"error": "No emails provided"}), 400
    
    db = get_db()
    cursor = db.cursor()
    
    unsubscribed_leads = []
    
    for email in emails:
        email = email.lower().strip()
        cursor.execute("""
            SELECT email, first_name, last_name, unsubscribe_status
            FROM leads 
            WHERE LOWER(email) = ? AND unsubscribe_status = 'unsubscribed'
        """, (email,))
        
        result = cursor.fetchone()
        if result:
            unsubscribed_leads.append({
                "email": result[0],
                "name": f"{result[1]} {result[2]}",
                "status": result[3]
            })
    
    return jsonify({
        "unsubscribed_count": len(unsubscribed_leads),
        "unsubscribed_leads": unsubscribed_leads
    })

# Update your existing functions to handle unsubscribe status

def filter_unsubscribed_leads(leads_data):
    """
    Filter out unsubscribed leads and return both filtered leads and unsubscribed count
    Priority: Manual email_status overrides external unsubscribe_status
    """
    db = get_db()
    cursor = db.cursor()
    
    filtered_leads = []
    unsubscribed_emails = []
    
    for lead in leads_data:
        email = lead.get('email', '').lower().strip()
        if not email:
            continue
            
        # Check email subscription status with priority logic
        cursor.execute("""
            SELECT email, email_status, unsubscribe_status FROM leads 
            WHERE LOWER(email) = ?
            ORDER BY id DESC
            LIMIT 1
        """, (email,))
        
        result = cursor.fetchone()
        should_exclude = False
        
        if result:
            email_status = result[1]  # manual status
            unsubscribe_status = result[2]  # external status
            
            # Logic: Exclude only if unsubscribed and NOT manually overridden
            if email_status == 'unsubscribed':
                # Manually unsubscribed - always exclude
                should_exclude = True
            elif email_status == 'subscribed':
                # Manually subscribed - always include (overrides external unsubscribe)
                should_exclude = False
            elif unsubscribe_status == 'unsubscribed':
                # Externally unsubscribed and no manual override - exclude
                should_exclude = True
            # If both are NULL or 'subscribed', include the lead
        
        if should_exclude:
            unsubscribed_emails.append(email)
        else:
            # Add unsubscribe token if not present
            if not lead.get('unsubscribe_token'):
                lead['unsubscribe_token'] = generate_unsubscribe_token()
            # Ensure default subscription status for new leads
            if not lead.get('email_status'):
                lead['email_status'] = 'subscribed'
            filtered_leads.append(lead)
    
    return filtered_leads, len(unsubscribed_emails), unsubscribed_emails

# Add these new routes to your Flask app (test_upload.py)

@app.route('/toggle_email_status/<int:lead_id>/<int:campaign_id>', methods=['POST'])
def toggle_email_status(lead_id, campaign_id):
    """Toggle email subscription status for a lead internally"""
    db = get_db()
    cursor = db.cursor()
    
    # Get current email status and unsubscribe status
    cursor.execute("SELECT email_status, unsubscribe_status, email, first_name, last_name FROM leads WHERE id = ?", (lead_id,))
    current_data = cursor.fetchone()
    
    if current_data:
        current_email_status = current_data[0] or 'subscribed'  # Default to subscribed if NULL
        current_unsubscribe_status = current_data[1]
        email = current_data[2]
        name = f"{current_data[3]} {current_data[4]}"
        
        # Toggle logic
        new_status = 'unsubscribed' if current_email_status == 'subscribed' else 'subscribed'
        
        # If manually subscribing someone who was externally unsubscribed, override the external status
        if new_status == 'subscribed' and current_unsubscribe_status == 'unsubscribed':
            cursor.execute("""
                UPDATE leads 
                SET email_status = ?, unsubscribe_status = 'subscribed' 
                WHERE id = ?
            """, (new_status, lead_id))
            print(f"✅ Manual override: Resubscribed {email} (was externally unsubscribed)")
            flash(f'Profile {name} manually resubscribed (overriding external unsubscribe)!', 'success')
        else:
            # Normal toggle
            cursor.execute("UPDATE leads SET email_status = ? WHERE id = ?", (new_status, lead_id))
            action = "subscribed" if new_status == 'subscribed' else "unsubscribed"
            flash(f'Profile {name} {action} successfully!', 'success')
            print(f"📧 Email status changed: {email} -> {action}")
        
        db.commit()
    else:
        flash('Profile not found!', 'error')
    
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

@app.route('/bulk_email_status/<int:campaign_id>/<status>', methods=['POST'])
def bulk_email_status(campaign_id, status):
    """Bulk update email subscription status"""
    lead_ids = request.form.getlist('lead_ids')
    if lead_ids:
        db = get_db()
        cursor = db.cursor()
        placeholders = ','.join('?' for _ in lead_ids)
        cursor.execute(f"UPDATE leads SET email_status = ? WHERE id IN ({placeholders})", [status] + lead_ids)
        db.commit()
        
        action = "subscribed" if status == 'subscribed' else "unsubscribed"
        flash(f'Successfully {action} {len(lead_ids)} profiles!', 'success')
    return redirect(url_for('campaign_detail', campaign_id=campaign_id))

@app.route('/api/email_status_stats/<int:campaign_id>')
def get_email_status_stats(campaign_id):
    """Get email subscription statistics for a campaign"""
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ? AND email_status = 'subscribed'", (campaign_id,))
    subscribed_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ? AND email_status = 'unsubscribed'", (campaign_id,))
    unsubscribed_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = ?", (campaign_id,))
    total_count = cursor.fetchone()[0]
    
    return jsonify({
        "subscribed": subscribed_count,
        "unsubscribed": unsubscribed_count,
        "total": total_count
    })

@app.route('/process_confirmation/<int:campaign_id>')
def process_confirmation(campaign_id):
    """Show processing confirmation page with lead comparison options"""
    db = get_db()
    cursor = db.cursor()
    
    # Get current campaign info
    cursor.execute("SELECT name, status FROM campaigns WHERE id = ?", (campaign_id,))
    campaign = cursor.fetchone()
    
    if not campaign or campaign[1] != 'approved':
        flash('Campaign not found or not approved', 'error')
        return redirect(url_for('campaigns'))
    
    # Check if is_merged column exists
    cursor.execute("PRAGMA table_info(campaigns)")
    columns_info = cursor.fetchall()
    has_is_merged = any(col[1] == 'is_merged' for col in columns_info)
    
    # Get previously processed MERGED campaigns only (not distributed lists)
    if has_is_merged:
        cursor.execute("""
            SELECT c.id, c.name, c.last_processed_at, c.process_count, 
                   COUNT(l.id) as profile_count
            FROM campaigns c
            LEFT JOIN leads l ON c.id = l.campaign_id AND l.is_active = 1
            WHERE c.processing_status = 'sent' 
            AND c.id != ?
            AND c.is_merged = 1
            GROUP BY c.id, c.name, c.last_processed_at, c.process_count
            ORDER BY c.last_processed_at DESC
        """, (campaign_id,))
    else:
        # Fallback if is_merged column doesn't exist - you might want to add other criteria
        cursor.execute("""
            SELECT c.id, c.name, c.last_processed_at, c.process_count, 
                   COUNT(l.id) as profile_count
            FROM campaigns c
            LEFT JOIN leads l ON c.id = l.campaign_id AND l.is_active = 1
            WHERE c.processing_status = 'sent' 
            AND c.id != ?
            AND c.name NOT LIKE 'Original:%'
            GROUP BY c.id, c.name, c.last_processed_at, c.process_count
            ORDER BY c.last_processed_at DESC
        """, (campaign_id,))
    
    processed_campaigns = cursor.fetchall()
    
    return render_template("process_confirmation.html", 
                         campaign_id=campaign_id,
                         campaign_name=campaign[0],
                         processed_campaigns=processed_campaigns)

@app.route('/api/compare_leads/<int:current_campaign_id>/<int:processed_campaign_id>')
def compare_leads(current_campaign_id, processed_campaign_id):
    """Compare leads between current campaign and processed campaign"""
    db = get_db()
    cursor = db.cursor()
    
    # Get current campaign active leads
    cursor.execute("""
        SELECT id, first_name, last_name, email, company
        FROM leads 
        WHERE campaign_id = ? AND is_active = 1
        AND (
            email_status = 'subscribed'
            OR 
            (
                (email_status IS NULL OR email_status = 'subscribed') 
                AND (unsubscribe_status IS NULL OR unsubscribe_status = 'subscribed')
            )
        )
    """, (current_campaign_id,))
    current_leads = cursor.fetchall()
    
    # Get processed campaign leads (emails only for comparison)
    cursor.execute("""
        SELECT LOWER(TRIM(email)) as email
        FROM leads 
        WHERE campaign_id = ?
    """, (processed_campaign_id,))
    processed_emails = set([row[0] for row in cursor.fetchall()])
    
    # Find duplicates
    duplicates = []
    unique_leads = []
    
    for lead in current_leads:
        lead_email = lead[3].lower().strip()
        lead_data = {
            'id': lead[0],
            'first_name': lead[1],
            'last_name': lead[2],
            'email': lead[3],
            'company': lead[4]
        }
        
        if lead_email in processed_emails:
            duplicates.append(lead_data)
        else:
            unique_leads.append(lead_data)
    
    return jsonify({
        'total_leads': len(current_leads),
        'unique_leads': len(unique_leads),
        'duplicate_leads': len(duplicates),
        'duplicates': duplicates,
        'unique': unique_leads
    })

if __name__ == '__main__':
    init_db()
    app.run(debug=True)