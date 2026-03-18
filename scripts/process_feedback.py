"""
process_feedback.py
====================
Each program has its own Forms with different questions.

Flow:
- First upload: reads columns, tries auto-detect, returns them to UI if mapping incomplete
- Once mapping is saved (via feedback_columns page): uses it permanently
- Every upload: adds only new rows, never duplicates
- Module name: matched from session log by date
- Faculty: first mentor name from program settings
"""
import os, sys, json
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
from openpyxl import load_workbook

# These are the only fields we absolutely need
REQUIRED = ['date', 'participant']

# Best-guess candidates for auto-detection (generic fallbacks)
DETECT = {
    'date':        ['start time', 'completion time', 'timestamp', 'submitted', 'date'],
    'participant': ['participant', 'name', 'respondent', 'your name', 'employee'],
    'takeaways':   ['takeaway', 'key learning', 'learning', 'what did you'],
    'rating':      ['rating', 'rate', 'score', 'stars', 'satisfaction'],
    'specific':    ['specific', 'feedback for', 'delivery', 'mentor', 'comment'],
    'other':       ['other', 'additional', 'anything else', 'suggest'],
}

COLS_KEY     = 'feedback_columns'
DATE_MAP_KEY = 'feedback_date_map'


def _detect(df_cols, candidates):
    for cand in candidates:
        for col in df_cols:
            if cand.lower() in str(col).lower():
                return col
    return None


def _load_log(log_path):
    if os.path.exists(log_path):
        with open(log_path) as f:
            try: return json.load(f)
            except: pass
    return {}


def _save_log(log_path, log):
    os.makedirs(os.path.dirname(log_path) or '.', exist_ok=True)
    with open(log_path, 'w') as f:
        json.dump(log, f, indent=2, default=str)


def _get_faculty(prog_dir):
    """Get first mentor name from settings.json."""
    path = os.path.join(prog_dir, 'settings.json')
    if os.path.exists(path):
        with open(path) as f:
            try:
                s = json.load(f)
                mentors = [m for m in s.get('mentor_names', [])
                           if not str(m).startswith('_')]
                return ', '.join(mentors) if mentors else ''
            except: pass
    return ''


def _parse_date(val):
    """Extract DD-MM-YYYY from any timestamp format."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (datetime, date)):
        dt = val if isinstance(val, datetime) else datetime.combine(val, datetime.min.time())
        return dt.strftime('%d-%m-%Y')
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'none', ''):
        return None
    formats = [
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d',
        '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m/%d/%Y',
        '%m/%d/%y %H:%M:%S', '%m/%d/%y %H:%M', '%m/%d/%y',
        '%m-%d-%y %H:%M:%S', '%m-%d-%y %H:%M', '%m-%d-%y',
        '%d-%m-%Y %H:%M:%S', '%d-%m-%Y',
        '%d/%m/%Y %H:%M:%S', '%d/%m/%Y',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime('%d-%m-%Y')
        except:
            try:
                return datetime.strptime(s.split()[0], fmt.split()[0]).strftime('%d-%m-%Y')
            except: continue
    return None


def _build_session_map(log):
    """Build {DD-MM-YYYY: session_title} from run log."""
    m = {}
    for run in log.get('runs', []):
        d, t = run.get('date',''), run.get('title','')
        if d and t: m[d] = t
    for run in log.get('processed_attendance', []):
        d = run.get('date','')
        t = run.get('session_title', run.get('title',''))
        if d and t: m[d] = t
    for d, t in log.get(DATE_MAP_KEY, {}).items():
        if d not in m: m[d] = t
    return m


def process_feedback(fb_path, config_path, log_path, report_path, prog_dir=None):
    """
    Returns:
      {'success': True, 'new_rows': N, 'skipped_rows': N}
      {'success': False, 'error': '...'}
      {'success': False, 'needs_mapping': True,
       'available_columns': [...], 'auto_map': {...}}
    """
    # ── Load file ─────────────────────────────────────────────────────────────
    try:
        df = pd.read_excel(fb_path)
    except Exception as e:
        return {'success': False, 'error': f'Could not open file: {e}'}

    if df.empty:
        return {'success': False, 'error': 'Feedback file is empty.'}

    available_columns = [str(c) for c in df.columns.tolist()]

    # ── Load log + column map ─────────────────────────────────────────────────
    log     = _load_log(log_path)
    col_map = log.get(COLS_KEY, {})

    # If no saved mapping — try auto-detect
    if not col_map:
        col_map = {}
        for field, cands in DETECT.items():
            col_map[field] = _detect(available_columns, cands)

        # Check if required fields were found
        missing_required = [f for f in REQUIRED if not col_map.get(f)]
        if missing_required:
            # Can't proceed — tell UI to show mapping screen
            return {
                'success':          False,
                'needs_mapping':    True,
                'available_columns': available_columns,
                'auto_map':         col_map,
                'error':            (f"Could not auto-detect: {missing_required}. "
                                     f"Please map the columns manually.")
            }

        # Auto-detect worked — save it
        log[COLS_KEY] = col_map
        _save_log(log_path, log)

    # ── Faculty from settings ─────────────────────────────────────────────────
    if prog_dir is None:
        prog_dir = os.path.dirname(os.path.dirname(log_path))
    faculty = _get_faculty(prog_dir)

    # ── Session map for module name lookup ────────────────────────────────────
    session_map   = _build_session_map(log)
    learned_dates = dict(log.get(DATE_MAP_KEY, {}))
    asked         = {}

    # ── Load workbook ─────────────────────────────────────────────────────────
    if not os.path.exists(report_path):
        return {'success': False, 'error': 'Report not found. Upload attendance first.'}

    wb = load_workbook(report_path)
    if 'Feedback' not in wb.sheetnames:
        return {'success': False, 'error': 'No Feedback sheet in report.'}

    ws = wb['Feedback']

    # Find next empty row + max Sno
    next_row = 2
    while ws.cell(next_row, 1).value is not None:
        next_row += 1

    max_sno = 0
    for row in range(2, next_row):
        v = ws.cell(row, 1).value
        try:
            if v: max_sno = max(max_sno, int(str(v)))
        except: pass

    # ── Process rows ──────────────────────────────────────────────────────────
    done_keys  = set(log.get('processed_feedback_keys', []))
    new_count  = 0
    skip_count = 0

    def get(row, field):
        col = col_map.get(field)
        if not col or col not in row.index: return ''
        v = row[col]
        if pd.isna(v): return ''
        return str(v)

    for _, row in df.iterrows():
        ts_raw = row[col_map['date']] if col_map.get('date') and col_map['date'] in row.index else ''
        ts_str = '' if pd.isna(ts_raw) else str(ts_raw)
        part   = get(row, 'participant')
        if not part: continue  # skip rows with no participant

        # Resolve module name from date
        row_date = _parse_date(ts_raw)

        if row_date and row_date in session_map:
            mod = session_map[row_date]
        elif row_date and row_date in asked:
            mod = asked[row_date]
        else:
            mod = 'Unknown'  # Will be fixable via feedback_columns page

        key = f"{ts_str}|{part.lower()}|{mod.lower()}"
        if key in done_keys:
            skip_count += 1
            continue

        # Write to feedback sheet
        max_sno += 1
        ws.cell(next_row, 1, max_sno)
        ws.cell(next_row, 2, ts_str)
        ws.cell(next_row, 3, mod)
        ws.cell(next_row, 4, faculty)
        ws.cell(next_row, 5, part)
        ws.cell(next_row, 6, get(row, 'takeaways'))
        rating = get(row, 'rating')
        try:    ws.cell(next_row, 7, float(rating))
        except: ws.cell(next_row, 7, rating)
        ws.cell(next_row, 8, get(row, 'specific'))
        ws.cell(next_row, 9, get(row, 'other'))

        done_keys.add(key)
        next_row  += 1
        new_count += 1

    wb.save(report_path)

    log['processed_feedback_keys'] = list(done_keys)
    log[DATE_MAP_KEY]              = {**learned_dates, **asked}
    log.setdefault('feedback_runs', []).append({
        'file':         os.path.basename(fb_path),
        'faculty':      faculty,
        'new_rows':     new_count,
        'skipped':      skip_count,
        'processed_at': datetime.now().isoformat()
    })
    _save_log(log_path, log)

    return {'success': True, 'new_rows': new_count, 'skipped_rows': skip_count}
