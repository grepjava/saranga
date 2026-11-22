#!/bin/bash
# The image's entry point, as Artemis's docker-run.sh: on the first start,
# a broker instance is made in the working directory (the volume) and the
# files of etc-override are copied over its etc; then the instance's
# bin/artemis runs with the container's arguments ("run" by default).

set -e

if [[ ${ANONYMOUS_LOGIN,,} == "true" ]]; then
  LOGIN_OPTION="--allow-anonymous"
else
  LOGIN_OPTION="--require-login"
fi

if ! [ -f ./etc/broker.xml ]; then
    # The user and password are one argument each, whatever they hold
    # (Artemis's script splits them at spaces and expands wildcards in them).
    # EXTRA_ARGS is options, split at spaces but not expanded.
    read -r -a EXTRA <<< "${EXTRA_ARGS}"
    /opt/saranga/bin/saranga create --user "${ARTEMIS_USER}" --password "${ARTEMIS_PASSWORD}" \
        --silent "${LOGIN_OPTION}" "${EXTRA[@]}" .
    if [ -d ./etc-override ]; then
        for file in ./etc-override/*; do
            [ -e "$file" ] || continue
            echo "copying file to etc folder: ${file##*/}"
            cp "$file" ./etc || :
        done
    fi
else
    echo "skipping broker instance creation; instance already exists"
fi

exec ./bin/artemis "$@"
