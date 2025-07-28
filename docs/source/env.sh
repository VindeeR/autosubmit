# shellcheck disable=SC2155
export AUTOSUBMIT_CONFIGURATION=$(mktemp -d)

touch $AUTOSUBMIT_CONFIGURATION/.autosubmitrc

AS_CONFIG="[database]
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
path = $AUTOSUBMIT_CONFIGURATION/autosubmit/metadata/logs"

echo $AS_CONFIG

echo $AUTOSUBMIT_CONFIGURATION/.autosubmitrc

cat <<EOF >> $AUTOSUBMIT_CONFIGURATION/.autosubmitrc
echo $AS_CONFIG
EOF

cat $AUTOSUBMIT_CONFIGURATION/.autosubmitrc