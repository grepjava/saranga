"""Enables the example in the management.xml `artemis create` writes that
lets the view role read queues (a <match key="subcomponent=queues"> in a
comment).

    python -I enable_queue_access.py <etc/management.xml>
"""

import sys

path = sys.argv[1]
text = open(path, encoding="utf-8").read().replace("\r\n", "\n")
start = '<!--\n         <match domain="org.apache.activemq.artemis" key="subcomponent=queues">'
if start not in text:
    sys.exit(f"{path}: the queue example is not there")
text = text.replace(start, start[len("<!--\n         "):])
text = text.replace("</match>\n         -->", "</match>")
open(path, "w", encoding="utf-8", newline="\n").write(text)
