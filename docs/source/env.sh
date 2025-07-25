# shellcheck disable=SC2155
export AUTOSUBMIT_CONFIGURATION=$(mktemp -d)

echo $AUTOSUBMIT_CONFIGURATION

touch $AUTOSUBMIT_CONFIGURATION/.autosubmitrc

cat <<EOF > $AUTOSUBMIT_CONFIGURATION/.autosubmitrc
[database]
path = $AUTOSUBMIT_CONFIGURATION/autosubmit
filename = autosubmit.db
[local]
path = $AUTOSUBMIT_CONFIGURATION/autosubmit
[globallogs]
path = $AUTOSUBMIT_CONFIGURATION/autosubmit/logs
[structures]
path = $AUTOSUBMIT_CONFIGURATION/autosubmit/metadata/structures
[historicdb]
path = $AUTOSUBMIT_CONFIGURATION/autosubmit/metadata/data
[historiclog]
path = $AUTOSUBMIT_CONFIGURATION/autosubmit/metadata/logs
EOF