#!/bin/bash

# Configuration
EOS_SERVER="root://eosuser.cern.ch"
EOS_OUTDIR="/eos/home-t/thea/condor_eos_test"
SUBMIT_DIR="${PWD}"

# Check Kerberos token
klist -s
if [ $? -ne 0 ]; then
    echo "ERROR: No valid Kerberos token. Run kinit first."
    exit 1
fi

# Create the job wrapper script.
# The job produces an output file and exits — no manual xrdcp needed.
# HTCondor transfers the file to EOS via the output_destination + XRootD plugin.
cat > job_wrapper.sh << 'EOF'
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
EOF
chmod +x job_wrapper.sh

# Create the condor submit file.
# transfer_output_files names the subdirectory to transfer (no trailing slash →
# the directory itself lands inside output_destination, preserving its name).
# MY.XRDCP_CREATE_DIR creates the destination on EOS automatically.
cat > test_eos.sub << EOF
universe                = vanilla
executable              = job_wrapper.sh
arguments               = \$(ProcId)
output                  = logs/job.\$(ClusterId).\$(ProcId).out.txt
error                   = logs/job.\$(ClusterId).\$(ProcId).err.txt
log                     = logs/job.\$(ClusterId).log
+JobFlavour             = "tomorrow"
should_transfer_files   = YES
transfer_input_files    = job_wrapper.sh
transfer_output_files   = output
output_destination      = ${EOS_SERVER}/${EOS_OUTDIR}/\$(ClusterId)/
MY.XRDCP_CREATE_DIR     = True
MY.SendCredential       = True
queue 3
EOF

# Create logs directory on AFS (not EOS, to avoid submission error)
mkdir -p logs

echo "Submitting test job..."
condor_submit test_eos.sub
