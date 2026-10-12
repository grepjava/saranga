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

# The shell's history file, which Artemis's bin/artemis passes on from the
# profile it loads: artemis-utility.profile, but for run.
if [ -z "$ARTEMIS_PROFILE" ] ; then
  if [ "$1" = "run" ]; then
    ARTEMIS_PROFILE='artemis.profile'
  else
    ARTEMIS_PROFILE="artemis-utility.profile"
  fi
fi
if [ -f "$ARTEMIS_INSTANCE_ETC/${ARTEMIS_PROFILE}" ] ; then
  ARTEMIS_SHELL_HISTORY=`. "$ARTEMIS_INSTANCE_ETC/${ARTEMIS_PROFILE}" > /dev/null 2>&1; echo "$ARTEMIS_SHELL_HISTORY"`
fi
if [ -n "$ARTEMIS_SHELL_HISTORY" ] ; then
  export ARTEMIS_SHELL_HISTORY
else
  unset ARTEMIS_SHELL_HISTORY
fi

# SARANGA names another saranga to run.
exec "${SARANGA:-${saranga}}" "$@"
