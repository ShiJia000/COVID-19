#!/bin/sh
# author by remram44

set -eu

git fetch -p origin

# Write header
ORIG_HEADER="MODZCTA,Positive,Total,zcta_cum.perc_pos"
echo "date,${ORIG_HEADER}"

# Loop on the commits to tests-by-zcta.csv
git log --reverse --format="%H %aI" origin/master -- tests-by-zcta.csv | while read commit date; do
    echo "Importing ${commit}" >&2
    # Write the file, without the header, with date prepended, to the output
    if [ "${commit}" = "946537b1647b4bc7c621716ef6f8e336de09068b" ]; then
        # This commit is missing a field
        git show ${commit}:tests-by-zcta.csv | tail -n +2 | (while read l; do echo "${date},${l},"; done)
    else
        git show ${commit}:tests-by-zcta.csv | tail -n +2 | (while read l; do echo "${date},${l}"; done)
    fi
done
