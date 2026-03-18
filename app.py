"""
app.py — TrackED Web Application
Full-stack Flask app with:
- Email + password login (each organiser has their own account)
- Admin panel to create/manage user accounts and grant admin rights
- Program management (create, configure, per-user isolation)
- CSV upload → attendance processing
- Feedback upload → dedup + module/faculty mapping
- Report download (persistent disk storage)
- Multiple mentors and multiple excluded names per program
"""

import os, sys, json, shutil, re
from datetime import datetime
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from flask import (Flask, render_template, request, redirect,
                   url_for, session, flash, send_file, jsonify, g)
import sqlite3
try:
    import psycopg2
    import psycopg2.extras
    HAS_PG = True
except ImportError:
    HAS_PG = False

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

app = Flask(__name__)
app.jinja_env.globals['enumerate'] = enumerate
app.secret_key = os.environ.get('SECRET_KEY', 'tracked-app-stable-secret-2026')

# ── Storage paths ─────────────────────────────────────────────────────────────
# On Render: mount persistent disk at /data
# Locally: use ./data
DATA_DIR    = os.environ.get('DATA_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'instance'))
DB_PATH     = os.path.join(DATA_DIR, 'tracked.db')
PROGRAMS_DIR= os.path.join(DATA_DIR, 'programs')
UPLOADS_DIR = os.path.join(DATA_DIR, 'uploads')

for d in [DATA_DIR, PROGRAMS_DIR, UPLOADS_DIR]:
    os.makedirs(d, exist_ok=True)


# ── Database ──────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get('DATABASE_URL', '')


def get_db():
    if 'db' not in g:
        if DATABASE_URL and HAS_PG:
            # PostgreSQL on Render — persistent, free
            url = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
            conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
            conn.autocommit = False
            g.db   = conn
            g.db_type = 'pg'
        else:
            # SQLite locally
            conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            conn.row_factory = sqlite3.Row
            g.db   = conn
            g.db_type = 'sqlite'
    return g.db


def db_execute(sql, params=()):
    """Execute SQL on either PostgreSQL or SQLite."""
    db = get_db()
    # Convert ? placeholders to %s for PostgreSQL
    if getattr(g, 'db_type', 'sqlite') == 'pg':
        sql = sql.replace('?', '%s')
        cur = db.cursor()
        cur.execute(sql, params)
        db.commit()
        return cur
    else:
        return db_fetchone(sql, params)


def db_fetchone(sql, params=()):
    cur = db_execute(sql, params)
    row = cur.fetchone()
    if row and getattr(g, 'db_type', 'sqlite') == 'pg':
        return dict(row)
    return row


def db_fetchall(sql, params=()):
    cur = db_execute(sql, params)
    rows = cur.fetchall()
    if rows and getattr(g, 'db_type', 'sqlite') == 'pg':
        return [dict(r) for r in rows]
    return rows


@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db:
        try: db.close()
        except: pass


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT    UNIQUE NOT NULL,
            name       TEXT    NOT NULL,
            password   TEXT    NOT NULL,
            is_admin   INTEGER DEFAULT 0,
            created_at TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS programs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            slug         TEXT    UNIQUE NOT NULL,
            name         TEXT    NOT NULL,
            client_name  TEXT    NOT NULL,
            settings     TEXT    NOT NULL,
            created_by   INTEGER NOT NULL,
            created_at   TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS program_access (
            program_id INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            PRIMARY KEY (program_id, user_id),
            FOREIGN KEY (program_id) REFERENCES programs(id),
            FOREIGN KEY (user_id)    REFERENCES users(id)
        );
    ''')
    db.commit()

    # Create default super-admin if no users exist
    admin_email    = os.environ.get('ADMIN_EMAIL',    'admin@tracked.app')
    admin_password = os.environ.get('ADMIN_PASSWORD', 'admin123')
    admin_name     = os.environ.get('ADMIN_NAME',     'Admin')

    existing = db_fetchall('SELECT id FROM users WHERE email=?',
                          (admin_email,))
    if not existing:
        db_fetchone(
            'INSERT INTO users (email, name, password, is_admin) VALUES (?,?,?,1)',
            (admin_email, admin_name, generate_password_hash(admin_password))
        )
        db.commit()
        print(f"✅ Admin account created: {admin_email}")

    db.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def slugify(text):
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')


def program_dir(slug):
    return os.path.join(PROGRAMS_DIR, slug)


def report_path(slug):
    return os.path.join(program_dir(slug), 'output', 'Master_Report.xlsx')


def log_path(slug):
    return os.path.join(program_dir(slug), 'logs', 'run_log.json')


def load_log(slug):
    path = log_path(slug)
    if os.path.exists(path):
        with open(path) as f:
            try: return json.load(f)
            except: pass
    return {'runs': []}


def save_log(slug, log):
    path = log_path(slug)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(log, f, indent=2, default=str)


def ensure_program_dirs(slug):
    for sub in ['output', 'logs', 'uploads']:
        os.makedirs(os.path.join(program_dir(slug), sub), exist_ok=True)


def get_program(slug, user_id):
    """Get program if user has access."""
    db = get_db()
    row = db_execute('''
        SELECT p.* FROM programs p
        JOIN program_access pa ON pa.program_id = p.id
        WHERE p.slug=? AND pa.user_id=?
    ''', (slug, user_id))
    return row


def get_all_user_programs(user_id):
    db = get_db()
    return db_fetchone('''
        SELECT p.* FROM programs p
        JOIN program_access pa ON pa.program_id = p.id
        WHERE pa.user_id=?
        ORDER BY p.created_at DESC
    ''', (user_id,))


# ── Auth decorators ───────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to continue.', 'info')
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if not session.get('is_admin'):
            flash('Admin access required.', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '').strip()
        db       = get_db()
        user     = db_fetchall('SELECT * FROM users WHERE email=?',
                              (email,))
        if user and check_password_hash(user['password'], password):
            session.clear()
            session['user_id']  = user['id']
            session['email']    = user['email']
            session['name']     = user['name']
            session['is_admin'] = bool(user['is_admin'])
            next_url = request.args.get('next', url_for('dashboard'))
            return redirect(next_url)
        flash('Incorrect email or password.', 'error')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route('/dashboard')
@login_required
def dashboard():
    programs = get_all_user_programs(session['user_id'])
    prog_list = []
    for p in programs:
        settings     = json.loads(p['settings'])
        log          = load_log(p['slug'])
        sessions_done= len(log.get('runs', []))
        report_exists= os.path.exists(report_path(p['slug']))
        prog_list.append({
            'slug':          p['slug'],
            'name':          p['name'],
            'client_name':   p['client_name'],
            'sessions_done': sessions_done,
            'report_exists': report_exists,
            'last_updated':  log.get('runs', [{}])[-1].get('processed', '—')
                             if log.get('runs') else '—'
        })
    return render_template('dashboard.html', programs=prog_list)


# ── Create program ────────────────────────────────────────────────────────────

@app.route('/programs/new', methods=['GET', 'POST'])
@login_required
def new_program():
    if request.method == 'POST':
        name         = request.form.get('name', '').strip()
        client       = request.form.get('client', '').strip()
        threshold    = int(request.form.get('threshold', 50))
        name_format  = request.form.get('name_format', 'auto')
        exclude_raw  = request.form.get('exclude_names', '')
        mentor_raw   = request.form.get('mentor_names', '')

        # Parse comma or newline separated names
        def parse_names(raw):
            names = re.split(r'[,\n]', raw)
            return [n.strip() for n in names if n.strip()]

        exclude_names = parse_names(exclude_raw)
        mentor_names  = parse_names(mentor_raw)

        if not name or not client:
            flash('Program name and client name are required.', 'error')
            return render_template('new_program.html')

        slug = slugify(f"{client}-{name}")
        db   = get_db()

        # Check slug uniqueness
        existing = db_fetchone('SELECT id FROM programs WHERE slug=?',
                              (slug,))
        if existing:
            slug = f"{slug}-{datetime.now().strftime('%m%d%H%M')}"

        settings = {
            'name_format':   name_format,
            'exclude_names': exclude_names,
            'mentor_names':  mentor_names,
            'threshold':     threshold / 100,
        }

        db_fetchone(
            'INSERT INTO programs (slug, name, client_name, settings, created_by) VALUES (?,?,?,?,?)',
            (slug, name, client, json.dumps(settings), session['user_id'])
        )
        prog_id = db_execute('SELECT id FROM programs WHERE slug=?',
                             (slug,))['id']
        db_fetchone('INSERT INTO program_access (program_id, user_id) VALUES (?,?)',
                   (prog_id, session['user_id']))
        db.commit()

        ensure_program_dirs(slug)
        flash(f'Program "{name}" created successfully!', 'success')
        return redirect(url_for('program_detail', slug=slug))

    return render_template('new_program.html')


# ── Program detail ────────────────────────────────────────────────────────────

@app.route('/programs/<slug>')
@login_required
def program_detail(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found or you do not have access.', 'error')
        return redirect(url_for('dashboard'))

    settings     = json.loads(p['settings'])
    log          = load_log(slug)
    report_exists= os.path.exists(report_path(slug))

    return render_template('program.html',
                           program=p,
                           settings=settings,
                           log=log,
                           report_exists=report_exists)


# ── Upload attendance ─────────────────────────────────────────────────────────

@app.route('/programs/<slug>/upload-attendance', methods=['POST'])
@login_required
def upload_attendance(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify({'success': False, 'error': 'Program not found'}), 404

    file = request.files.get('csv_file')
    if not file or not file.filename.endswith('.csv'):
        return jsonify({'success': False,
                        'error': 'Please upload a .csv file'}), 400

    ensure_program_dirs(slug)
    upload_path = os.path.join(program_dir(slug), 'uploads',
                               f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}")
    file.save(upload_path)

    try:
        from smart_report import process_csv
        settings = json.loads(p['settings'])

        result = process_csv(
            csv_path      = upload_path,
            report_path   = report_path(slug),
            exclude_names = settings.get('exclude_names', []),
            mentor_names  = settings.get('mentor_names', []),
            threshold     = settings.get('threshold', 0.50),
            name_format   = settings.get('name_format', 'auto'),
        )

        if not result['success']:
            return jsonify({'success': False, 'error': result['error']})

        # Save to log
        log = load_log(slug)
        log.setdefault('runs', []).append({
            'file':        file.filename,
            'date':        result['date'],
            'title':       result['title'],
            'session_num': result['session_num'],
            'present':     result['present'],
            'absent':      result['absent'],
            'processed':   datetime.now().isoformat()
        })
        save_log(slug, log)

        return jsonify({
            'success':     True,
            'session_num': result['session_num'],
            'title':       result['title'],
            'date':        result['date'],
            'present':     result['present'],
            'absent':      result['absent'],
            'message':     (f"Session {result['session_num']} processed — "
                           f"{result['present']} present"
                           + (f", {len(result['absent'])} absent: "
                              f"{', '.join(result['absent'])}"
                              if result['absent'] else ', full attendance!'))
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Upload feedback ───────────────────────────────────────────────────────────

@app.route('/programs/<slug>/upload-feedback', methods=['POST'])
@login_required
def upload_feedback(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify({'success': False, 'error': 'Program not found'}), 404

    file = request.files.get('feedback_file')
    if not file or not (file.filename.endswith('.xlsx') or
                        file.filename.endswith('.xls')):
        return jsonify({'success': False,
                        'error': 'Please upload an .xlsx file'}), 400

    if not os.path.exists(report_path(slug)):
        return jsonify({'success': False,
                        'error': 'Process at least one attendance CSV first.'}), 400

    ensure_program_dirs(slug)
    upload_path = os.path.join(program_dir(slug), 'uploads',
                               f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}")
    file.save(upload_path)

    # Write minimal config for feedback processor
    config_path = os.path.join(program_dir(slug), 'config.json')
    if not os.path.exists(config_path):
        with open(config_path, 'w') as f:
            json.dump({'output_file': report_path(slug),
                       'feedback_column_map': {}}, f)

    try:
        from process_feedback import process_feedback
        settings = json.loads(p['settings'])

        # Check file size — warn if very large
        file_size_mb = os.path.getsize(upload_path) / (1024 * 1024)
        if file_size_mb > 10:
            return jsonify({'success': False,
                'error': f'File too large ({file_size_mb:.1f}MB). Max 10MB.'})

        result = process_feedback(
            fb_path     = upload_path,
            config_path = config_path,
            log_path    = log_path(slug),
            report_path = report_path(slug),
            prog_dir    = program_dir(slug),
        )

        if not result or not result.get('success'):
            err  = result.get('error', 'Unknown error') if result else 'Unknown'
            cols = result.get('available_columns', []) if result else []
            needs_mapping = result.get('needs_mapping', False) if result else False
            auto_map      = result.get('auto_map', {}) if result else {}

            # Save auto-detected partial map so mapping page is pre-filled
            if needs_mapping and auto_map:
                lp = log_path(slug)
                lg = load_log(slug)
                lg['feedback_columns'] = {k:v for k,v in auto_map.items() if v}
                save_log(slug, lg)

            return jsonify({
                'success':       False,
                'error':         err,
                'needs_mapping': needs_mapping,
                'available_columns': cols,
                'mapping_url':   url_for('feedback_columns', slug=slug)
                                 if needs_mapping else ''
            })

        return jsonify({
            'success':   True,
            'new_rows':  result['new_rows'],
            'skipped':   result['skipped_rows'],
            'message':   (f"{result['new_rows']} new feedback rows added"
                         + (f", {result['skipped_rows']} duplicates skipped"
                            if result['skipped_rows'] else ''))
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Download report ───────────────────────────────────────────────────────────

@app.route('/programs/<slug>/download')
@login_required
def download_report(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found.', 'error')
        return redirect(url_for('dashboard'))

    rpath = report_path(slug)
    if not os.path.exists(rpath):
        flash('No report yet. Upload a Teams CSV first.', 'error')
        return redirect(url_for('program_detail', slug=slug))

    log       = load_log(slug)
    today     = datetime.now().strftime('%d%b%Y')
    sessions_done = len(log.get('runs', []))
    filename  = (f"{p['client_name']}_{p['name']}_"
                 f"{sessions_done}Sessions_{today}.xlsx"
                 ).replace(' ', '_')
    return send_file(rpath, as_attachment=True, download_name=filename)


# ── Admin: user management ────────────────────────────────────────────────────

@app.route('/admin')
@admin_required
def admin_panel():
    db    = get_db()
    users = db_execute('SELECT * FROM users ORDER BY created_at DESC')
    return render_template('admin.html', users=users)


@app.route('/admin/users/new', methods=['POST'])
@admin_required
def admin_create_user():
    email    = request.form.get('email', '').strip().lower()
    name     = request.form.get('name', '').strip()
    password = request.form.get('password', '').strip()
    is_admin = 1 if request.form.get('is_admin') else 0

    if not email or not name or not password:
        flash('All fields required.', 'error')
        return redirect(url_for('admin_panel'))

    db = get_db()
    try:
        db_execute(
            'INSERT INTO users (email, name, password, is_admin) VALUES (?,?,?,?)',
            (email, name, generate_password_hash(password), is_admin)
        )
        db.commit()
        flash(f'Account created for {email}', 'success')
    except (sqlite3.IntegrityError, Exception) as integrity_err:
        if 'unique' not in str(integrity_err).lower() and 'duplicate' not in str(integrity_err).lower(): raise
        flash(f'Email {email} already exists.', 'error')

    return redirect(url_for('admin_panel'))


@app.route('/admin/users/<int:user_id>/toggle-admin', methods=['POST'])
@admin_required
def toggle_admin(user_id):
    if user_id == session['user_id']:
        flash("You can't change your own admin status.", 'error')
        return redirect(url_for('admin_panel'))
    db   = get_db()
    user = db_execute('SELECT * FROM users WHERE id=?', (user_id,))
    if user:
        db_fetchone('UPDATE users SET is_admin=? WHERE id=?',
                   (0 if user['is_admin'] else 1, user_id))
        db.commit()
        flash(f"Admin status updated for {user['email']}", 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/users/<int:user_id>/reset-password', methods=['POST'])
@admin_required
def reset_password(user_id):
    new_pw = request.form.get('new_password', '').strip()
    if not new_pw:
        flash('Password cannot be empty.', 'error')
        return redirect(url_for('admin_panel'))
    db = get_db()
    db_execute('UPDATE users SET password=? WHERE id=?',
               (generate_password_hash(new_pw), user_id))
    db.commit()
    flash('Password reset successfully.', 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    if user_id == session['user_id']:
        flash("You can't delete yourself.", 'error')
        return redirect(url_for('admin_panel'))
    db = get_db()
    db_execute('DELETE FROM program_access WHERE user_id=?', (user_id,))
    db_execute('DELETE FROM users WHERE id=?', (user_id,))
    db.commit()
    flash('User deleted.', 'success')
    return redirect(url_for('admin_panel'))


# ── Admin: share program with user ────────────────────────────────────────────

@app.route('/programs/<slug>/share', methods=['POST'])
@login_required
def share_program(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify({'success': False, 'error': 'Not found'}), 404

    email = request.form.get('email', '').strip().lower()
    db    = get_db()
    user  = db_execute('SELECT id FROM users WHERE email=?', (email,))
    if not user:
        flash(f'No account found for {email}', 'error')
        return redirect(url_for('program_detail', slug=slug))

    try:
        db_fetchone('INSERT INTO program_access (program_id, user_id) VALUES (?,?)',
                   (p['id'], user['id']))
        db.commit()
        flash(f'Program shared with {email}', 'success')
    except (sqlite3.IntegrityError, Exception) as integrity_err:
        if 'unique' not in str(integrity_err).lower() and 'duplicate' not in str(integrity_err).lower(): raise
        flash(f'{email} already has access.', 'info')

    return redirect(url_for('program_detail', slug=slug))


# ── Profile: change own password ──────────────────────────────────────────────

@app.route('/profile/change-password', methods=['POST'])
@login_required
def change_password():
    current = request.form.get('current_password', '').strip()
    new_pw  = request.form.get('new_password', '').strip()
    confirm = request.form.get('confirm_password', '').strip()

    db   = get_db()
    user = db_execute('SELECT * FROM users WHERE id=?',
                      (session['user_id'],))

    if not check_password_hash(user['password'], current):
        flash('Current password is incorrect.', 'error')
    elif new_pw != confirm:
        flash('New passwords do not match.', 'error')
    elif len(new_pw) < 6:
        flash('Password must be at least 6 characters.', 'error')
    else:
        db_fetchone('UPDATE users SET password=? WHERE id=?',
                   (generate_password_hash(new_pw), session['user_id']))
        db.commit()
        flash('Password changed successfully.', 'success')

    return redirect(url_for('dashboard'))


# ── Init and run ──────────────────────────────────────────────────────────────




with app.app_context():
    init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5000)


# ── 1. Edit program settings ──────────────────────────────────────────────────

@app.route('/programs/<slug>/edit', methods=['GET', 'POST'])
@login_required
def edit_program(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found.', 'error')
        return redirect(url_for('dashboard'))

    settings = json.loads(p['settings'])

    if request.method == 'POST':
        name         = request.form.get('name', '').strip()
        client       = request.form.get('client', '').strip()
        threshold    = int(request.form.get('threshold', 50))
        name_format  = request.form.get('name_format', 'auto')

        def parse_names(raw):
            names = re.split(r'[,\n]', raw)
            return [n.strip() for n in names if n.strip()]

        exclude_names = parse_names(request.form.get('exclude_names', ''))
        mentor_names  = parse_names(request.form.get('mentor_names', ''))

        new_settings = {
            'name_format':   name_format,
            'exclude_names': exclude_names,
            'mentor_names':  mentor_names,
            'threshold':     threshold / 100,
        }

        db = get_db()
        db_execute(
            'UPDATE programs SET name=?, client_name=?, settings=? WHERE slug=?',
            (name, client, json.dumps(new_settings), slug)
        )
        db.commit()
        flash('Program updated successfully.', 'success')
        return redirect(url_for('program_detail', slug=slug))

    return render_template('edit_program.html', program=p, settings=settings)


# ── 3. Delete program ─────────────────────────────────────────────────────────

@app.route('/programs/<slug>/delete', methods=['POST'])
@login_required
def delete_program(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found.', 'error')
        return redirect(url_for('dashboard'))

    db = get_db()
    db_execute('DELETE FROM program_access WHERE program_id=?', (p['id'],))
    db_execute('DELETE FROM programs WHERE id=?', (p['id'],))
    db.commit()

    # Remove program data folder
    pdir = program_dir(slug)
    if os.path.exists(pdir):
        shutil.rmtree(pdir)

    flash(f'Program "{p["name"]}" deleted.', 'success')
    return redirect(url_for('dashboard'))


# ── 5. Undo / reprocess a session ────────────────────────────────────────────

@app.route('/programs/<slug>/undo-session', methods=['POST'])
@login_required
def undo_session(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify({'success': False, 'error': 'Not found'}), 404

    session_num = request.form.get('session_num', type=int)
    if not session_num:
        return jsonify({'success': False, 'error': 'Session number required'})

    log = load_log(slug)
    runs = log.get('runs', [])

    # Find the run
    target = next((r for r in runs if r.get('session_num') == session_num), None)
    if not target:
        return jsonify({'success': False, 'error': 'Session not found in log'})

    # Remove from log
    log['runs'] = [r for r in runs if r.get('session_num') != session_num]
    save_log(slug, log)

    # Rebuild report from scratch using remaining runs
    rpath = report_path(slug)
    if os.path.exists(rpath):
        os.remove(rpath)

    remaining = sorted(log['runs'], key=lambda r: r.get('session_num', 0))

    if remaining:
        from smart_report import process_csv
        settings = json.loads(p['settings'])
        upload_folder = os.path.join(program_dir(slug), 'uploads')

        rebuilt = 0
        failed  = []
        for run in remaining:
            # Find the original uploaded file
            csv_candidates = [
                f for f in os.listdir(upload_folder)
                if run['file'] in f and f.endswith('.csv')
            ] if os.path.exists(upload_folder) else []

            if csv_candidates:
                csv_path = os.path.join(upload_folder, csv_candidates[0])
                result = process_csv(
                    csv_path      = csv_path,
                    report_path   = rpath,
                    exclude_names = settings.get('exclude_names', []),
                    mentor_names  = settings.get('mentor_names', []),
                    threshold     = settings.get('threshold', 0.50),
                    name_format   = settings.get('name_format', 'auto'),
                )
                if result['success']:
                    rebuilt += 1
                else:
                    failed.append(run['file'])
            else:
                failed.append(run['file'])

        msg = f"Session {session_num} removed. Report rebuilt with {rebuilt} session(s)."
        if failed:
            msg += f" Could not find original files for: {', '.join(failed)}. Use Rebuild Report to re-upload."
        flash(msg, 'success' if not failed else 'info')
    else:
        flash(f'Session {session_num} removed. No sessions remaining — report cleared.', 'success')

    return redirect(url_for('program_detail', slug=slug))


# ── 6. Rebuild report from scratch ───────────────────────────────────────────

@app.route('/programs/<slug>/rebuild', methods=['POST'])
@login_required
def rebuild_report(slug):
    """
    Delete the current report and reprocess all uploaded CSVs from scratch.
    Useful when report is corrupted or after editing program settings.
    """
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found.', 'error')
        return redirect(url_for('dashboard'))

    rpath        = report_path(slug)
    upload_folder= os.path.join(program_dir(slug), 'uploads')
    log          = load_log(slug)
    runs         = sorted(log.get('runs', []), key=lambda r: r.get('session_num', 0))

    if not runs:
        flash('No sessions in log to rebuild from.', 'error')
        return redirect(url_for('program_detail', slug=slug))

    # Delete current report
    if os.path.exists(rpath):
        os.remove(rpath)

    from smart_report import process_csv
    settings = json.loads(p['settings'])

    rebuilt = 0
    failed  = []

    for run in runs:
        csv_candidates = []
        if os.path.exists(upload_folder):
            csv_candidates = [
                f for f in os.listdir(upload_folder)
                if run['file'] in f and f.endswith('.csv')
            ]

        if csv_candidates:
            csv_path = os.path.join(upload_folder, csv_candidates[0])
            result   = process_csv(
                csv_path      = csv_path,
                report_path   = rpath,
                exclude_names = settings.get('exclude_names', []),
                mentor_names  = settings.get('mentor_names', []),
                threshold     = settings.get('threshold', 0.50),
                name_format   = settings.get('name_format', 'auto'),
            )
            if result['success']:
                rebuilt += 1
            else:
                failed.append(run['file'])
        else:
            failed.append(run['file'])

    if failed:
        flash(
            f'Rebuilt {rebuilt} session(s). '
            f'Missing original files for {len(failed)} session(s): {", ".join(failed)}. '
            f'Please re-upload those CSVs.',
            'info'
        )
    else:
        flash(f'Report rebuilt successfully from {rebuilt} session(s).', 'success')

    return redirect(url_for('program_detail', slug=slug))


# ── 8. Forgot password ────────────────────────────────────────────────────────

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """
    Simple forgot password — admin sets a temporary password,
    user enters email + new password directly (no email sending needed).
    For now: user requests reset, admin sees it and resets from admin panel.
    """
    if request.method == 'POST':
        email    = request.form.get('email', '').strip().lower()
        new_pw   = request.form.get('new_password', '').strip()
        confirm  = request.form.get('confirm_password', '').strip()

        if new_pw != confirm:
            flash('Passwords do not match.', 'error')
            return render_template('forgot_password.html')
        if len(new_pw) < 6:
            flash('Password must be at least 6 characters.', 'error')
            return render_template('forgot_password.html')

        # Check reset token
        token      = request.form.get('token', '').strip()
        reset_file = os.path.join(DATA_DIR, 'reset_requests.json')

        resets = {}
        if os.path.exists(reset_file):
            with open(reset_file) as f:
                try: resets = json.load(f)
                except: resets = {}

        if email not in resets or resets[email].get('token') != token:
            flash('Invalid or expired reset link. Please contact your admin.', 'error')
            return render_template('forgot_password.html')

        db = get_db()
        user = db_execute('SELECT id FROM users WHERE email=?', (email,))
        if not user:
            flash('No account found for this email.', 'error')
            return render_template('forgot_password.html')

        db_fetchone('UPDATE users SET password=? WHERE id=?',
                   (generate_password_hash(new_pw), user['id']))
        db.commit()

        # Remove used token
        del resets[email]
        with open(reset_file, 'w') as f:
            json.dump(resets, f)

        flash('Password reset successfully. Please log in.', 'success')
        return redirect(url_for('login'))

    token = request.args.get('token', '')
    email = request.args.get('email', '')
    return render_template('forgot_password.html', token=token, email=email)


@app.route('/admin/users/<int:user_id>/generate-reset', methods=['POST'])
@admin_required
def generate_reset_link(user_id):
    """Admin generates a reset link for a user."""
    import secrets
    db   = get_db()
    user = db_execute('SELECT * FROM users WHERE id=?', (user_id,))
    if not user:
        flash('User not found.', 'error')
        return redirect(url_for('admin_panel'))

    token      = secrets.token_urlsafe(32)
    reset_file = os.path.join(DATA_DIR, 'reset_requests.json')

    resets = {}
    if os.path.exists(reset_file):
        with open(reset_file) as f:
            try: resets = json.load(f)
            except: resets = {}

    resets[user['email']] = {
        'token':      token,
        'created_at': datetime.now().isoformat()
    }
    with open(reset_file, 'w') as f:
        json.dump(resets, f)

    # Build the reset URL
    base_url  = request.host_url.rstrip('/')
    reset_url = f"{base_url}/forgot-password?email={user['email']}&token={token}"

    flash(f'Reset link for {user["email"]}:', 'info')
    flash(reset_url, 'info')
    return redirect(url_for('admin_panel'))


# ── 9. Better report filename (already handled in download_report) ────────────
# Updated to include date in filename

@app.route('/programs/<slug>/download-v2')
@login_required
def download_report_v2(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found.', 'error')
        return redirect(url_for('dashboard'))

    rpath = report_path(slug)
    if not os.path.exists(rpath):
        flash('No report yet. Upload a Teams CSV first.', 'error')
        return redirect(url_for('program_detail', slug=slug))

    log   = load_log(slug)
    runs  = log.get('runs', [])
    today = datetime.now().strftime('%d%b%Y')
    sessions_done = len(runs)

    filename = (f"{p['client_name']}_{p['name']}_"
                f"{sessions_done}Sessions_{today}.xlsx"
                ).replace(' ', '_')

    return send_file(rpath, as_attachment=True, download_name=filename)


# ── 10. Fix feedback column mapping from UI ───────────────────────────────────

@app.route('/programs/<slug>/feedback-columns', methods=['GET', 'POST'])
@login_required
def feedback_columns(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Program not found.', 'error')
        return redirect(url_for('dashboard'))

    log_file = log_path(slug)
    log_data = {}
    if os.path.exists(log_file):
        with open(log_file) as f:
            try: log_data = json.load(f)
            except: log_data = {}

    current_map = log_data.get('feedback_columns', {})

    if request.method == 'POST':
        new_map = {}
        for field in ['date', 'participant', 'takeaways', 'rating', 'specific', 'other']:
            val = request.form.get(field, '').strip()
            if val:
                new_map[field] = val

        log_data['feedback_columns'] = new_map

        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2)

        flash('Feedback column mapping saved. Upload feedback again to apply.', 'success')
        return redirect(url_for('program_detail', slug=slug))

    return render_template('feedback_columns.html',
                           program=p,
                           current_map=current_map)


# ── Get sessions for feedback module picker ──────────────────────────────────

@app.route('/programs/<slug>/sessions-json')
@login_required
def sessions_json(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify([])
    log  = load_log(slug)
    runs = log.get('runs', [])
    return jsonify([
        {'title': r.get('title',''), 'date': r.get('date','')}
        for r in runs
    ])


# ── Upload nominations CSV (name, email) ──────────────────────────────────────

@app.route('/programs/<slug>/upload-nominations', methods=['POST'])
@login_required
def upload_nominations(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify({'success': False, 'error': 'Not found'}), 404

    file = request.files.get('nominations_file')
    if not file:
        return jsonify({'success': False, 'error': 'No file uploaded'})

    rpath = report_path(slug)
    if not os.path.exists(rpath):
        return jsonify({'success': False,
                        'error': 'Process attendance first before uploading nominations'})

    try:
        import pandas as pd
        from smart_report import fill_emails

        # Read CSV — expects columns: name, email (flexible header names)
        df = pd.read_csv(file) if file.filename.endswith('.csv') \
             else pd.read_excel(file)

        # Find name and email columns
        cols = [c.lower().strip() for c in df.columns]
        name_col  = next((df.columns[i] for i,c in enumerate(cols)
                         if 'name' in c), df.columns[0])
        email_col = next((df.columns[i] for i,c in enumerate(cols)
                         if 'email' in c or 'mail' in c), df.columns[1])

        from utils import normalise_name
        email_map = {
            normalise_name(str(row[name_col])).lower().strip(): str(row[email_col]).strip()
            for _, row in df.iterrows()
            if pd.notna(row[name_col]) and pd.notna(row[email_col])
        }

        filled = fill_emails(rpath, email_map)
        return jsonify({'success': True, 'filled': filled,
                        'message': f'{filled} email(s) added to report'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Edit report (view + edit cells before downloading) ────────────────────────

@app.route('/programs/<slug>/edit-report')
@login_required
def edit_report(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Not found', 'error')
        return redirect(url_for('dashboard'))

    rpath = report_path(slug)
    if not os.path.exists(rpath):
        flash('No report yet. Upload attendance first.', 'error')
        return redirect(url_for('program_detail', slug=slug))

    try:
        from openpyxl import load_workbook
        wb   = load_workbook(rpath, data_only=True)
        data = {}

        for sheet_name in wb.sheetnames:
            ws   = wb[sheet_name]
            rows = []
            for row in ws.iter_rows(min_row=1, max_row=ws.max_row,
                                    values_only=True):
                rows.append([str(v) if v is not None else '' for v in row])
            data[sheet_name] = rows

        return render_template('edit_report.html',
                               program=p,
                               sheets=data,
                               sheet_names=wb.sheetnames)
    except Exception as e:
        flash(f'Could not open report: {e}', 'error')
        return redirect(url_for('program_detail', slug=slug))


@app.route('/programs/<slug>/save-report', methods=['POST'])
@login_required
def save_report_edits(slug):
    """Save edited cells back to the Excel report."""
    p = get_program(slug, session['user_id'])
    if not p:
        return jsonify({'success': False, 'error': 'Not found'}), 404

    rpath = report_path(slug)
    if not os.path.exists(rpath):
        return jsonify({'success': False, 'error': 'Report not found'}), 404

    try:
        from openpyxl import load_workbook
        edits = request.json  # [{sheet, row, col, value}, ...]
        wb    = load_workbook(rpath)

        for edit in edits:
            sheet = edit.get('sheet')
            row   = int(edit.get('row')) + 1   # 0-indexed from JS → 1-indexed
            col   = int(edit.get('col')) + 1
            value = edit.get('value', '')

            if sheet in wb.sheetnames:
                # Try to preserve number types
                try:    value = float(value) if '.' in str(value) else int(value)
                except: pass
                wb[sheet].cell(row, col, value if value != '' else None)

        wb.save(rpath)
        return jsonify({'success': True, 'saved': len(edits)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Date → Module mapping ─────────────────────────────────────────────────────

@app.route('/programs/<slug>/date-map', methods=['GET', 'POST'])
@login_required
def date_map(slug):
    p = get_program(slug, session['user_id'])
    if not p:
        flash('Not found', 'error')
        return redirect(url_for('dashboard'))

    log_file = log_path(slug)
    log_data = {}
    if os.path.exists(log_file):
        with open(log_file) as f:
            try: log_data = json.load(f)
            except: log_data = {}

    if request.method == 'POST':
        # Save manual date→module mappings
        new_map = {}
        dates   = request.form.getlist('date')
        modules = request.form.getlist('module')
        for d, m in zip(dates, modules):
            d = d.strip(); m = m.strip()
            if d and m:
                new_map[d] = m

        log_data['feedback_date_map'] = new_map
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2)

        flash('Date mapping saved. Re-upload feedback to apply.', 'success')
        return redirect(url_for('program_detail', slug=slug))

    # Build current map — combine sessions from log + manual entries
    current_map = {}
    for run in log_data.get('runs', []):
        current_map[run.get('date', '')] = run.get('title', '')
    current_map.update(log_data.get('feedback_date_map', {}))

    return render_template('date_map.html',
                           program=p,
                           current_map=current_map)
