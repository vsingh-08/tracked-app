import re
import json
import os
from datetime import datetime


# ── Duration parser ───────────────────────────────────────────────────────────

def parse_duration_to_seconds(duration_str):
    """
    Parse Teams duration strings robustly.
    Handles: '2h 9m 46s', '33m 14s', '2h 8m', '59s', '2h', etc.
    Returns 0 on any failure — never crashes.
    """
    if not duration_str or str(duration_str).strip() == '':
        return 0
    s = str(duration_str).strip()
    total = 0
    try:
        h   = re.search(r'(\d+)\s*h', s)
        m   = re.search(r'(\d+)\s*m(?!s)', s)   # 'm' but not 'ms'
        sec = re.search(r'(\d+)\s*s', s)
        if h:   total += int(h.group(1))   * 3600
        if m:   total += int(m.group(1))   * 60
        if sec: total += int(sec.group(1))
    except Exception:
        return 0
    return total


# ── Date parser ───────────────────────────────────────────────────────────────

# All formats Teams has ever used — add more here if needed
_DATE_FORMATS = [
    '%m/%d/%y, %I:%M:%S %p',    # 3/09/26, 9:51:26 AM   ← Teams default
    '%m/%d/%Y, %I:%M:%S %p',    # 3/09/2026, 9:51:26 AM
    '%m/%d/%y, %I:%M %p',       # 3/09/26, 9:51 AM
    '%m/%d/%Y, %I:%M %p',       # 3/09/2026, 9:51 AM
    '%d/%m/%Y %H:%M:%S',        # 09/03/2026 09:51:26
    '%d/%m/%y %H:%M:%S',        # 09/03/26 09:51:26
    '%Y-%m-%d %H:%M:%S',        # 2026-03-09 09:51:26
    '%d-%m-%Y %H:%M:%S',        # 09-03-2026 09:51:26
]

def parse_teams_date(raw_str):
    """
    Try every known Teams date format.
    Returns (datetime_obj, None) on success.
    Returns (None, error_message) on failure — never crashes.
    """
    if not raw_str:
        return None, "Empty date string"
    s = str(raw_str).replace('"', '').strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt), None
        except ValueError:
            continue
    return None, (
        f"Unrecognised date format: '{s}'\n"
        f"Expected format like: '3/09/26, 9:51:26 AM'\n"
        f"Please check the Teams CSV and ensure dates look like: M/DD/YY, H:MM:SS AM/PM"
    )


# ── Name normalisation ────────────────────────────────────────────────────────

def normalise_name(raw_name, name_format='auto'):
    """
    Normalise a name from Teams into a clean "First Last" form.

    name_format options (set per-program in config):
      'auto'            → tries to detect format automatically
      'first_last'      → "Alice Johnson"             → "Alice Johnson"
      'last_first'      → "Johnson, Alice"             → "Alice Johnson"
      'last_first_co'   → "Johnson, Alice (XYZ Corp)"  → "Alice Johnson"
      'first_last_co'   → "Alice Johnson (XYZ Corp)"   → "Alice Johnson"
      'as_is'           → no change
    """
    if not raw_name:
        return ''
    name = str(raw_name).strip()

    # Strip company suffix in parentheses e.g. "(XYZ Corp)"
    name = re.sub(r'\s*\(.*?\)\s*$', '', name).strip()

    if name_format == 'as_is':
        return name

    # "Last, First" → "First Last"  (covers last_first and last_first_co after company stripped)
    if name_format in ('last_first', 'last_first_co') or (name_format == 'auto' and ',' in name):
        parts = name.split(',', 1)
        if len(parts) == 2:
            return f"{parts[1].strip()} {parts[0].strip()}"

    return name


def build_name_lookup(roster, name_format='auto'):
    """
    Build a dict: normalised_name → original_roster_name
    so we can always map back to the exact name used in the report.
    """
    lookup = {}
    for original in roster:
        normalised = normalise_name(original, name_format).lower()
        lookup[normalised] = original
    return lookup


# ── Fuzzy name matching ───────────────────────────────────────────────────────

def fuzzy_match_name(raw_csv_name, roster, name_format='auto', threshold=75):
    """
    Match a name from the Teams CSV against the roster.
    1. Normalise both sides first (strip company, handle Last, First)
    2. Exact match → score 100
    3. Token-based match (handles word order differences)
    4. Character Jaccard similarity as fallback

    Returns: (matched_roster_name, score, normalised_csv_name)
    """
    csv_normalised = normalise_name(raw_csv_name, name_format).lower().strip()
    lookup = build_name_lookup(roster, name_format)

    # 1. Exact match after normalisation
    if csv_normalised in lookup:
        return lookup[csv_normalised], 100, csv_normalised

    best_match = None
    best_score = 0

    for norm_roster, original in lookup.items():
        # 2. Token sort match — handles "Alice Johnson" vs "Johnson Alice"
        csv_tokens    = sorted(csv_normalised.split())
        roster_tokens = sorted(norm_roster.split())
        if csv_tokens == roster_tokens:
            return original, 98, csv_normalised

        # 3. All roster tokens present in csv name
        if all(t in csv_normalised for t in roster_tokens):
            score = 95
        elif any(t in csv_normalised for t in roster_tokens):
            # Partial token overlap
            matched = sum(1 for t in roster_tokens if t in csv_normalised)
            score = int((matched / len(roster_tokens)) * 85)
        else:
            # 4. Character Jaccard
            set_a = set(csv_normalised)
            set_b = set(norm_roster)
            if not set_a or not set_b:
                score = 0
            else:
                score = int(len(set_a & set_b) / len(set_a | set_b) * 100)

        if score > best_score:
            best_score = score
            best_match = original

    if best_score >= threshold:
        return best_match, best_score, csv_normalised
    return None, best_score, csv_normalised


# ── Config / log helpers ──────────────────────────────────────────────────────

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)


def load_log(log_path):
    if os.path.exists(log_path):
        with open(log_path, 'r') as f:
            return json.load(f)
    return {"processed_attendance": [], "processed_feedback": []}


def save_log(log_path, log_data):
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2, default=str)


# ── Teams CSV parser ──────────────────────────────────────────────────────────

def parse_teams_csv(filepath):
    """
    Parse a Microsoft Teams attendance CSV (UTF-16 encoded).
    Robust date handling — never crashes on unknown formats.
    Returns: (session_info dict, participants list, date_warnings list)
    """
    # Try UTF-16 first (standard Teams export), fall back to UTF-8
    content = None
    for enc in ('utf-16', 'utf-8-sig', 'utf-8'):
        try:
            with open(filepath, 'rb') as f:
                content = f.read().decode(enc)
            break
        except (UnicodeDecodeError, Exception):
            continue

    if content is None:
        raise ValueError(
            f"Could not read '{filepath}'. "
            "Expected a Microsoft Teams attendance CSV. "
            "Please re-download it from Teams and try again."
        )

    lines = content.replace('\r\n', '\n').replace('\r', '\n').split('\n')

    session_info  = {}
    participants  = []
    date_warnings = []
    in_participants = False

    for line in lines:
        parts = line.split('\t')
        key   = parts[0].strip().strip('"')

        # ── Summary section ──
        if key == 'Meeting title' and len(parts) > 1:
            session_info['meeting_title'] = parts[1].strip().strip('"')

        elif key == 'Start time' and len(parts) > 1:
            raw = parts[1].strip().strip('"')
            session_info['start_time'] = raw
            dt, err = parse_teams_date(raw)
            if dt:
                session_info['date'] = dt.strftime('%d-%m-%Y')
            else:
                session_info['date'] = 'Unknown'
                date_warnings.append(f"Start time: {err}")

        elif key == 'End time' and len(parts) > 1:
            session_info['end_time'] = parts[1].strip().strip('"')

        elif key == 'Meeting duration' and len(parts) > 1:
            raw = parts[1].strip().strip('"')
            session_info['duration_str']     = raw
            session_info['duration_seconds'] = parse_duration_to_seconds(raw)

        # ── Participant section header ──
        elif key == 'Name' and len(parts) > 3 and 'First Join' in parts[1]:
            in_participants = True
            continue

        # ── Stop at In-Meeting Activities ──
        elif '3. In-Meeting Activities' in key:
            in_participants = False

        # ── Participant data rows ──
        elif in_participants and key and key != 'Name':
            if len(parts) >= 4:
                name         = parts[0].strip().strip('"')
                first_join   = parts[1].strip().strip('"')
                last_leave   = parts[2].strip().strip('"')
                duration_str = parts[3].strip().strip('"')
                email        = parts[4].strip() if len(parts) > 4 else ''
                role         = parts[6].strip() if len(parts) > 6 else ''

                # Validate join/leave dates — warn but don't crash
                fj_dt, fj_err = parse_teams_date(first_join)
                ll_dt, ll_err = parse_teams_date(last_leave)
                if fj_err:
                    date_warnings.append(f"Participant '{name}' First Join: {fj_err}")
                if ll_err:
                    date_warnings.append(f"Participant '{name}' Last Leave: {ll_err}")

                if name:
                    participants.append({
                        'name':             name,
                        'first_join':       first_join,
                        'last_leave':       last_leave,
                        'duration_str':     duration_str,
                        'duration_seconds': parse_duration_to_seconds(duration_str),
                        'email':            email,
                        'role':             role,
                        'join_dt':          fj_dt,
                        'leave_dt':         ll_dt,
                    })

    return session_info, participants, date_warnings
