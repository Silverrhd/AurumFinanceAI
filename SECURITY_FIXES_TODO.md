# Security Fixes Required - TODO

## Overview
Multiple secrets are currently exposed in the public GitHub repository. These need to be fixed to secure the application.

---

## Critical Issues Found

### **Issue 1: openfigi_service.py - Hardcoded API Key**

**Location:** `aurum_backend/portfolio/services/openfigi_service.py:19`

**Problem:**
```python
API_KEY = 'bf21060a-0568-489e-8622-efcaf02e52cf'  # ProjectAurum key
```

**Fix Required:**
- Remove hardcoded line
- Update `__init__` to load from `os.environ.get('OPENFIGI_API_KEY')`
- Add validation to raise error if key is missing (same pattern as FMP service)

**Impact:** None - system will work the same, but key is now secure.

---

### **Issue 2: settings.py - Fallback Value**

**Location:** `aurum_backend/aurum_backend/settings.py:515`

**Problem:**
```python
OPENFIGI_API_KEY = os.environ.get('OPENFIGI_API_KEY', 'bf21060a-0568-489e-8622-efcaf02e52cf')
```

**Fix Required:**
```python
OPENFIGI_API_KEY = os.environ.get('OPENFIGI_API_KEY')
if not OPENFIGI_API_KEY:
    raise ValueError("OPENFIGI_API_KEY environment variable must be set")
```

**Impact:** Forces proper configuration - system will fail fast if key is missing.

---

### **Issue 3: OPENFIGI_API_SETUP.md - Documentation Leaks**

**Location:** `aurum_backend/OPENFIGI_API_SETUP.md` (lines 16, 55, 61, 114-119, 165)

**Problem:** Documentation shows actual API key in examples

**Fix Required:** Replace real key with placeholder in all examples:
```bash
# Before:
OPENFIGI_API_KEY=bf21060a-0568-489e-8622-efcaf02e52cf

# After:
OPENFIGI_API_KEY=your_openfigi_api_key_here
```

**Impact:** None - documentation still teaches configuration without leaking key.

---

### **Issue 4: backup_setup.sh - DB Password**

**Location:** `deployment/backup_setup.sh:21`

**Problem:**
```bash
PGPASSWORD=AurumSecure2025! pg_dump -h localhost -U aurumuser aurum_finance_prod
```

**Fix Required:** Delete this file - script isn't being used, backups aren't automated.

**Impact:** None - script isn't in use.

---

### **Issue 5: configure_app.sh - Multiple Credentials (CRITICAL)**

**Location:** `deployment/configure_app.sh` (lines 45, 59, 77, 87-95)

**Problem:** Contains:
- DB password: `AurumSecure2025!`
- OpenFIGI key
- Admin password: `ARDNd1163?`
- **All 42 client passwords** with usernames

**Fix Required:** Delete entire file - it's already deprecated and disabled (line 9 exits immediately).

**Impact:** None - file isn't used (we use `github_deploy.sh` now).

**⚠️ Critical Note:** Git history still contains these passwords. For maximum security:
1. Consider rotating all 42 client passwords
2. Change admin password
3. Consider using `git-filter-repo` to scrub Git history (advanced)

---

### **Issue 6: database_setup.sh - DB Password**

**Location:** `deployment/database_setup.sh:21`

**Problem:**
```bash
CREATE USER aurumuser WITH ENCRYPTED PASSWORD 'AurumSecure2025!';
```

**Fix Required:** Delete file - script is disabled (line 9) and DB already exists.

**Impact:** None - database setup complete, script not used.

---

## Implementation Steps

1. **Fix `openfigi_service.py`** - Make it load from environment
2. **Fix `settings.py`** - Remove fallback, make key required
3. **Redact `OPENFIGI_API_SETUP.md`** - Replace keys with placeholders
4. **Delete insecure scripts:**
   - `backup_setup.sh`
   - `configure_app.sh`
   - `database_setup.sh`
5. **Test locally** - Ensure OpenFIGI still works
6. **Commit and push** - Deploy changes

---

## Status
- [ ] Issue 1: openfigi_service.py fixed
- [ ] Issue 2: settings.py fixed
- [ ] Issue 3: Documentation redacted
- [ ] Issue 4: backup_setup.sh deleted
- [ ] Issue 5: configure_app.sh deleted
- [ ] Issue 6: database_setup.sh deleted
- [ ] Local testing complete
- [ ] Changes committed and pushed
- [ ] Production verified

---

## Notes
- All secrets are now properly managed via `/var/lib/aurumfinance/secrets.sh` on the server
- Deployment script (`github_deploy.sh`) correctly reads from secrets file
- Local development uses `.env` file (gitignored)
- Next deployment will work correctly with new secrets location
