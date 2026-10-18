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
    CREATE_ARGUMENTS="--user ${ARTEMIS_USER} --password ${ARTEMIS_PASSWORD} --silent ${LOGIN_OPTION} ${EXTRA_ARGS}"
    /opt/saranga/bin/saranga create ${CREATE_ARGUMENTS} .
    if [ -d ./etc-override ]; then
        for file in `ls ./etc-override`; do echo copying file to etc folder: $file; cp ./etc-override/$file ./etc || :; done
    fi
else
    echo "skipping broker instance creation; instance already exists"
fi

exec ./bin/artemis "$@"
