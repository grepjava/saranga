# Golden corpus

Ground truth recorded from the real Artemis broker (version in `COMPAT.md`).
Saranga's codecs must decode every file here and re-encode it byte for byte.

Each `<scenario>/` holds:
- `wire/conn-N/c2s.bin`, `s2c.bin`: raw TCP bytes for connection N, client to server and server to client.
- `data.tar.gz`: the broker's `data/` dir after a clean shutdown (journal, bindings, paging,
  large-messages). It is compressed because journal files are pre-allocated.
- `MANIFEST`: Artemis version and commit, plus generation time.

Never edit these files by hand. To regenerate:

    scripts/build-artemis.sh          # once: builds saranga/artemis-ref:<version>
    scripts/golden.sh <scenario>      # runs tests/golden-gen/scenarios/<Scenario>.java

Requires Docker and a JDK 17+ on the host.
