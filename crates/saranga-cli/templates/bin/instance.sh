#!/bin/sh
# The broker instance's command, as bin/artemis is an Artemis instance's:
# saranga, run for this instance (made by saranga create).

if [ -z "$ARTEMIS_INSTANCE" ] ; then

  ## resolve links - $0 may be a link to the instance's bin
  PRG="$0"
  saveddir=`pwd`

  # need this for relative symlinks
  dirname_prg=`dirname "$PRG"`
  cd "$dirname_prg"

  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '.*/.*' > /dev/null; then
    PRG="$link"
    else
    PRG=`dirname "$PRG"`"/$link"
    fi
  done

  ARTEMIS_INSTANCE=`dirname "$PRG"`
  cd "$saveddir"

  # make it fully qualified
  ARTEMIS_INSTANCE=`cd "$ARTEMIS_INSTANCE/.." && pwd`
fi

# At the start of its line, where upgrade looks for it.
if [ -z "$ARTEMIS_INSTANCE_ETC" ] ; then
ARTEMIS_INSTANCE_ETC='${artemis.instance.etc}'
fi
export ARTEMIS_INSTANCE ARTEMIS_INSTANCE_ETC

# SARANGA names another saranga to run.
exec "${SARANGA:-${saranga}}" "$@"
