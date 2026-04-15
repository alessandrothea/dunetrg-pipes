#!/usr/bin/env python3
"""
test_credd.py — smoke-test for credd.add_user_cred(CredTypes.Kerberos, None)

Run this at CERN on a submit node with a valid Kerberos ticket before
submitting jobs with lar-condor.py or piper-condor.py.

Requirements:
  - Valid Kerberos ticket  (kinit <user>@CERN.CH)
  - HTCondor credd daemon running on the submit node
  - htcondor2 installed  (pip install htcondor2)

Usage:
  python3 test_credd.py
"""

import subprocess
import sys

# -------------------------------------------------------------------
# 1. Check for a valid Kerberos ticket
# -------------------------------------------------------------------
print("=== Kerberos ticket ===")
result = subprocess.run(["klist", "-s"], capture_output=True)
if result.returncode != 0:
    print("ERROR: no valid Kerberos ticket found.")
    print("  Run:  kinit <user>@CERN.CH")
    sys.exit(1)

klist = subprocess.run(["klist"], capture_output=True, text=True)
print(klist.stdout.strip())

# -------------------------------------------------------------------
# 2. Import htcondor2
# -------------------------------------------------------------------
print("\n=== htcondor2 import ===")
try:
    import htcondor2
    print(f"OK  (version: {htcondor2.version()})")
except ImportError as e:
    print(f"ERROR: {e}")
    print("  Install with:  pip install htcondor2")
    sys.exit(1)
except AttributeError:
    import htcondor2
    print("OK  (version unknown)")

# -------------------------------------------------------------------
# 3. Check CredTypes.Kerberos
# -------------------------------------------------------------------
print("\n=== htcondor2.CredTypes.Kerberos ===")
try:
    kerberos = htcondor2.CredTypes.Kerberos
    print(f"OK  (value: {kerberos})")
except AttributeError as e:
    print(f"ERROR: {e}")
    sys.exit(1)

# -------------------------------------------------------------------
# 4. Instantiate Credd
# -------------------------------------------------------------------
print("\n=== htcondor2.Credd() ===")
try:
    credd = htcondor2.Credd()
    print("OK")
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

# -------------------------------------------------------------------
# 5. add_user_cred(CredTypes.Kerberos, None)
# -------------------------------------------------------------------
print("\n=== credd.add_user_cred(CredTypes.Kerberos, None) ===")
credd_ok = False
try:
    credd.add_user_cred(htcondor2.CredTypes.Kerberos, None)
    print("OK")
    credd_ok = True
except Exception as e:
    print(f"WARNING: {e}")
    print("  This usually means condor_credmon_krb is not running on this submit node.")
    print("  At CERN, HTCondor typically delegates Kerberos credentials automatically")
    print("  via MY.SendCredential — job submission may still work without this step.")

# -------------------------------------------------------------------
# 6. Verify credential was stored (only if step 5 succeeded)
# -------------------------------------------------------------------
if credd_ok:
    print("\n=== credd.query_user_cred(CredTypes.Kerberos) ===")
    try:
        ts = credd.query_user_cred(htcondor2.CredTypes.Kerberos)
        if ts is not None:
            print(f"OK  (last updated: {ts})")
        else:
            print("WARNING: query returned None — credential may not have been stored")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

print("\nDone." + ("" if credd_ok else " (credd step skipped — see warning above)"))
