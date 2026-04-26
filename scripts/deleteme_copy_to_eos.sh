#!/bin/bash
# =============================================================================
# copy_to_project_eos.sh
#
# Copies /eos/user/t/thea/dune_trigger/eminus_1x8x14/
# to     /eos/project/d/dunedaq-eos/trigger-studies/vd-1x8x14/
#
# Authentication: Kerberos only (kinit thea@CERN.CH)
#
# Usage:
#   chmod +x copy_to_project_eos.sh
#   ./copy_to_project_eos.sh              # copy all files
#   ./copy_to_project_eos.sh --dry-run   # list files only, no copy
# =============================================================================

# --- Configuration -----------------------------------------------------------
SRC_EOS="root://eosuser.cern.ch"
DST_EOS="root://eosproject.cern.ch"

SRC_BASE="/eos/user/t/thea/dune_trigger/eminus_1x8x14"
DST_BASE="/eos/project/d/dunedaq-eos/trigger-studies/vd-1x8x14"

MAX_RETRIES=3
RETRY_DELAY=30

LOG_FILE="copy_eos_$(date +%Y%m%d_%H%M%S).log"
DRY_RUN=false

# --- Parse arguments ---------------------------------------------------------
for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# --- Logging helper ----------------------------------------------------------
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

# --- Preflight checks --------------------------------------------------------
log "===== EOS copy script started ====="
log "Source:      ${SRC_EOS}//${SRC_BASE}"
log "Destination: ${DST_EOS}//${DST_BASE}"
log "Log file:    ${LOG_FILE}"
[ "${DRY_RUN}" = true ] && log "*** DRY RUN MODE — no files will be copied ***"

# Check Kerberos token
klist -s
if [ $? -ne 0 ]; then
    log "ERROR: No valid Kerberos token. Run: kinit thea@CERN.CH"
    exit 1
fi
log "Kerberos token: OK  (expires: $(klist | grep krbtgt | awk '{print $3, $4}' | head -1))"
log "NOTE: For large transfers run inside 'screen' and renew with 'kinit -R' if needed."

# Check source is accessible
xrdfs ${SRC_EOS} stat ${SRC_BASE} > /dev/null 2>&1
if [ $? -ne 0 ]; then
    log "ERROR: Cannot access source: ${SRC_EOS}//${SRC_BASE}"
    exit 1
fi
log "Source: accessible"

# Check destination — test only the known-writable parent
xrdfs ${DST_EOS} stat /eos/project/d/dunedaq-eos > /dev/null 2>&1
if [ $? -ne 0 ]; then
    log "ERROR: Cannot access destination EOS: ${DST_EOS}//eos/project/d/dunedaq-eos"
    log "       Check Kerberos token and project EOS membership."
    exit 1
fi
log "Destination EOS: accessible"

# --- Enumerate all files recursively using eos find --------------------------
log "Enumerating files (this may take a moment for large trees)..."

echo "eos root://eosuser.cern.ch find -f ${SRC_BASE}"
FILELIST=$(eos root://eosuser.cern.ch find -f ${SRC_BASE} 2>/dev/null)

if [ -z "${FILELIST}" ]; then
    log "ERROR: No files found under ${SRC_BASE} — check the path is correct."
    exit 1
fi

TOTAL_FILES=$(echo "${FILELIST}" | grep -c .)
log "Found ${TOTAL_FILES} files to process"

# --- Dry run -----------------------------------------------------------------
if [ "${DRY_RUN}" = true ]; then
    log "--- Files that would be copied ---"
    while IFS= read -r filepath; do
        [ -z "${filepath}" ] && continue
        RELPATH="${filepath#${SRC_BASE}/}"
        log "  ${RELPATH}"
    done <<< "${FILELIST}"
    log "--- End of file list ---"
    log "Dry run complete. Exiting."
    exit 0
fi

# --- Create destination base directory ---------------------------------------
log "Creating destination base directory..."
xrdfs ${DST_EOS} mkdir -p ${DST_BASE} 2>/dev/null || true

# --- Copy files --------------------------------------------------------------
SUCCESS=0
SKIPPED=0
FAILED=0
FAILED_FILES=()
CURRENT=0

while IFS= read -r SRC_FILEPATH; do
    [ -z "${SRC_FILEPATH}" ] && continue

    CURRENT=$((CURRENT + 1))
    RELPATH="${SRC_FILEPATH#${SRC_BASE}/}"
    DST_FILEPATH="${DST_BASE}/${RELPATH}"
    DST_DIR=$(dirname "${DST_FILEPATH}")

    log "[${CURRENT}/${TOTAL_FILES}] ${RELPATH}"

    # Skip if already exists at destination — makes script safe to re-run
    if xrdfs ${DST_EOS} stat "${DST_FILEPATH}" > /dev/null 2>&1; then
        log "  SKIP — already exists"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Create destination subdirectory if needed
    xrdfs ${DST_EOS} mkdir -p "${DST_DIR}" 2>/dev/null || true

    # Copy with retries
    ATTEMPT=0
    COPY_OK=false

    until [ ${ATTEMPT} -ge ${MAX_RETRIES} ]; do
        ATTEMPT=$((ATTEMPT + 1))
        [ ${ATTEMPT} -gt 1 ] && log "  Retry ${ATTEMPT}/${MAX_RETRIES} — waiting ${RETRY_DELAY}s..." && sleep ${RETRY_DELAY}

        xrdcp \
            --cksum adler32:print \
            --force \
            --silent \
            "${SRC_EOS}//${SRC_FILEPATH}" \
            "${DST_EOS}//${DST_FILEPATH}" \
            >> "${LOG_FILE}" 2>&1

        if [ $? -eq 0 ]; then
            COPY_OK=true
            break
        fi
        log "  WARNING: xrdcp failed on attempt ${ATTEMPT}"
    done

    if [ "${COPY_OK}" = true ]; then
        log "  OK"
        SUCCESS=$((SUCCESS + 1))
    else
        log "  FAILED after ${MAX_RETRIES} attempts: ${RELPATH}"
        FAILED=$((FAILED + 1))
        FAILED_FILES+=("${RELPATH}")
    fi

done <<< "${FILELIST}"

# --- Summary -----------------------------------------------------------------
log ""
log "===== Copy complete ====="
log "  Total:     ${TOTAL_FILES}"
log "  Succeeded: ${SUCCESS}"
log "  Skipped:   ${SKIPPED}  (already at destination)"
log "  Failed:    ${FAILED}"

if [ ${FAILED} -gt 0 ]; then
    log ""
    log "Failed files:"
    for f in "${FAILED_FILES[@]}"; do
        log "  - ${f}"
    done
    log ""
    log "Re-run to retry — already-copied files will be skipped automatically."
    exit 1
fi

log ""
log "All files copied successfully to ${DST_EOS}//${DST_BASE}"
exit 0