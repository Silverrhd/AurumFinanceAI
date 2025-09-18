# Universal Client-Bank Integration System - Usage Guide

## Overview
This system allows seamless integration of new clients or new banks for existing clients without disrupting existing data.

## Quick Commands

### Single Date Integration
```bash
# New client (EI + STDSZ)
python universal_client_integrator.py --client EI --bank STDSZ --date 29_05_2025

# Existing client + new bank (VLP + PICTET)  
python universal_client_integrator.py --client VLP --bank PICTET --date 29_05_2025
```

### Batch Date Integration
```bash
# Process multiple dates with monitoring
python universal_client_integrator.py --client EI --bank STDSZ --date-range 29_05_2025:11_09_2025 --interactive
```

### Verification Commands
```bash
# Basic verification
python verify_integration.py --client EI

# Verify specific bank
python verify_integration.py --client EI --bank STDSZ

# Check dashboard cache
python verify_integration.py --client EI --check-cache

# Full integrity check
python verify_integration.py --client EI --bank STDSZ --check-integrity
```

## Integration Modes

### Replacement Mode (New Clients)
- **Triggered**: When client doesn't exist or has no snapshots
- **Behavior**: Creates fresh client data for each date
- **Use Case**: EI (new client) + STDSZ bank

### Additive Mode (Existing Clients + New Bank)
- **Triggered**: When client exists with existing snapshots
- **Behavior**: Preserves existing bank data, adds/updates target bank only
- **Use Case**: VLP (existing) + PICTET (new bank)

## Step-by-Step Workflow

### For New Client (EI + STDSZ)
1. **Process Files**: Upload STDSZ raw files → run enricher/combiner/transformer
2. **Integrate**: `python universal_client_integrator.py --client EI --bank STDSZ --date 29_05_2025`
3. **Verify**: `python verify_integration.py --client EI --bank STDSZ`
4. **Check**: Review results before proceeding to next date
5. **Repeat**: Continue for June 5, 12, etc.

### For Existing Client + New Bank (VLP + PICTET)
1. **Process Files**: Upload PICTET raw files → run enricher/combiner/transformer
2. **Integrate**: `python universal_client_integrator.py --client VLP --bank PICTET --date 29_05_2025`
3. **Verify**: `python verify_integration.py --client VLP --bank PICTET`
4. **Check**: Verify VLP's other banks (JPM, CS, LO) are unchanged
5. **Repeat**: Continue for remaining dates

## Expected Results

### After EI Integration
- **Database**: 46 clients (was 45)
- **EI Snapshots**: 16 dates (May 29 - Sept 11)
- **Dashboard**: Shows 46 clients in aggregations
- **Other Clients**: Completely unchanged

### After VLP + PICTET Integration  
- **Database**: Still 46 clients
- **VLP Snapshots**: Enhanced with PICTET positions
- **VLP Value**: Increased (all banks combined)
- **VLP Other Banks**: JPM/CS/LO positions preserved
- **Other Clients**: Completely unchanged

## Error Handling
- **Missing Files**: Clear error message about which files are missing
- **Integration Failures**: Transaction rollback ensures no partial data
- **Mode Detection**: Automatic based on client existence and data

## Safety Features
- **Auto Mode Detection**: Prevents accidental data loss
- **Transaction Safety**: Database rollback on errors
- **Verification Tools**: Comprehensive checks after integration
- **Interactive Mode**: Pause between dates for manual verification

## Files Created
- `universal_client_integrator.py` - Main integration tool
- `verify_integration.py` - Verification and monitoring tool
- `INTEGRATION_USAGE_GUIDE.md` - This usage guide