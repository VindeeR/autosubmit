# shellcheck disable=SC2155
export AUTOSUBMIT_CONFIGURATION=$(mktemp -d)

export AS_CONFIG="[database]
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

cat <<EOF > "$AUTOSUBMIT_CONFIGURATION"/.autosubmitrc
$AS_CONFIG
EOF

mkdir "$AUTOSUBMIT_CONFIGURATION"/autosubmit

autosubmit install
