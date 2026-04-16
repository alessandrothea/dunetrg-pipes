#!/bin/bash
echo "===== Job started: $(date) ====="
echo "Running on:   $(hostname)"
echo "Kerberos tokens:"
klist

# Create a subdirectory with an output file inside it.
# This mirrors piper-condor jobs, where each pipeline stage writes to its own
# subdirectory (gen/, g4/, detsim/, ...) that must be transferred back to EOS.
mkdir -p output
OUTFILE="output/test_job_${1}.txt"
echo "Hello from job ${1}" > ${OUTFILE}
echo "Host:    $(hostname)"  >> ${OUTFILE}
echo "Date:    $(date)"      >> ${OUTFILE}
echo "Condor cluster: ${1}"  >> ${OUTFILE}

echo "===== Job finished: $(date) ====="
exit 0
