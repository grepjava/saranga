// Draws the benchmark charts in docs/benchmarks.md and README.md from the
// results.tsv files scripts/bench.sh writes: each chart as an SVG for a
// light page and another (-dark.svg) for a dark one.
//
//   node scripts/bench/charts.mjs JAVA_RESULTS.tsv [RUST_RESULTS.tsv] OUTDIR
//
// JAVA_RESULTS.tsv is a run with the default clients; RUST_RESULTS.tsv, if
// given, one with CLIENT=rust, for the chart comparing the two.

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const args = process.argv.slice(2);
if (args.length < 2 || args.length > 3) {
  console.error("usage: charts.mjs JAVA_RESULTS.tsv [RUST_RESULTS.tsv] OUTDIR");
  process.exit(2);
}
const out = args.at(-1);
const java = read(args[0]);
const rust = args.length === 3 ? read(args[1]) : null;

// results.tsv: scenario, broker (artemis, saranga or both), metric, value.
function read(file) {
  const r = {};
  for (const line of readFileSync(file, "utf8").split("\n")) {
    const [scenario, kind, metric, value] = line.trim().split("\t");
    if (value === undefined) continue;
    ((r[scenario] ??= {})[kind] ??= {})[metric] = Number(value);
  }
  return r;
}

const NAMES = {
  core: "Core",
  "core-persistent": "Core, persistent",
  amqp: "AMQP",
  "amqp-persistent": "AMQP, persistent",
  "core-large": "Core, 1 MiB persistent",
  "amqp-large": "AMQP, 1 MiB persistent",
  "many-queues": "Core, 100 queues",
  openwire: "OpenWire",
  "openwire-persistent": "OpenWire, persistent",
  stomp: "STOMP",
  "stomp-persistent": "STOMP, persistent",
  mqtt: "MQTT, QoS 0",
  "mqtt-persistent": "MQTT, QoS 1",
};

// The reference palette's tokens, its first two categorical slots (blue and
// orange) validated in both modes.
const THEMES = {
  light: {
    surface: "#fcfcfb", primary: "#0b0b0b", secondary: "#52514e", muted: "#898781",
    grid: "#e1e0d9", axis: "#c3c2b7", blue: "#2a78d6", orange: "#eb6834",
  },
  dark: {
    surface: "#1a1a19", primary: "#f0efec", secondary: "#c3c2b7", muted: "#898781",
    grid: "#2c2c2a", axis: "#383835", blue: "#3987e5", orange: "#d95926",
  },
};
// Each series' colour: Artemis blue and Saranga orange; in the chart of the
// two clients, the Java client blue and the Rust client orange.
const COLOURS = { Artemis: "blue", Saranga: "orange", "Java client": "blue", "Rust client": "orange" };
const colour = (t, name) => t[COLOURS[name]];

const W = 760;
const LABEL_W = 200; // the scenario names
const RIGHT = 90; // room for a value at a bar's tip
const FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif';

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const n = (v) => Math.round(v).toLocaleString("en-US");
const us = (v) => (v >= 1000 ? `${(v / 1000).toFixed(1)} ms` : `${n(v)} µs`);
const ms = (v) => `${(v / 1000).toFixed(1)} ms`;

// Round tick steps (1, 2, 2.5, 5 times a power of ten) giving about five ticks.
function ticks(max) {
  const raw = max / 5;
  const p = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * p).find((s) => s >= raw);
  const top = Math.ceil(max / step) * step;
  const t = [];
  for (let v = 0; v <= top + step / 1e6; v += step) t.push(v);
  return t;
}

// A bar from the baseline at x0, its data end rounded (4px), square at the baseline.
function bar(x0, y, w, h, fill, tip) {
  const r = Math.min(4, w, h / 2);
  const d = w <= r
    ? `M${x0} ${y}h${w}v${h}h${-w}z`
    : `M${x0} ${y}h${w - r}a${r} ${r} 0 0 1 ${r} ${r}v${h - 2 * r}a${r} ${r} 0 0 1 ${-r} ${r}h${-(w - r)}z`;
  return `<path d="${d}" fill="${fill}"><title>${esc(tip)}</title></path>`;
}

// One panel of horizontal bars: rows of {label, values}, one value a series,
// on an axis of its own. Returns its markup and height.
function panel(t, p, top) {
  const series = p.series;
  const thick = series.length > 1 ? 14 : 16;
  const gap = 2; // the surface between a row's bars
  const band = series.length * thick + (series.length - 1) * gap + 14;
  const max = Math.max(p.ref ?? 0, ...p.rows.flatMap((r) => r.values).filter((v) => v !== undefined));
  const tk = ticks(max);
  const x0 = LABEL_W, x1 = W - RIGHT;
  const x = (v) => x0 + (v / tk.at(-1)) * (x1 - x0);
  let s = "";
  let y = top;
  if (p.title) {
    s += `<text x="24" y="${y + 14}" font-size="13" font-weight="600" fill="${t.primary}">${esc(p.title)}</text>`;
    y += 26;
  }
  const plotTop = y;
  const plotBottom = plotTop + p.rows.length * band;
  for (const v of tk) {
    s += `<line x1="${x(v)}" x2="${x(v)}" y1="${plotTop}" y2="${plotBottom}" stroke="${v === 0 ? t.axis : t.grid}" stroke-width="1"/>`;
    s += `<text x="${x(v)}" y="${plotBottom + 16}" font-size="11" fill="${t.muted}" text-anchor="middle" style="font-variant-numeric: tabular-nums">${esc(p.tick(v))}</text>`;
  }
  if (p.ref !== undefined) {
    // Under the bars, so the values at their tips stay legible.
    s += `<line x1="${x(p.ref)}" x2="${x(p.ref)}" y1="${plotTop - 6}" y2="${plotBottom}" stroke="${t.secondary}" stroke-width="1.5"/>`;
    s += `<text x="${x(p.ref)}" y="${plotTop - 10}" font-size="11" fill="${t.secondary}" text-anchor="middle">${esc(p.refLabel)}</text>`;
  }
  p.rows.forEach((r, i) => {
    const by = plotTop + i * band + 7;
    const mid = by + (band - 14) / 2;
    s += `<text x="${x0 - 12}" y="${mid + 4}" font-size="12.5" fill="${t.primary}" text-anchor="end">${esc(r.label)}</text>`;
    r.values.forEach((v, j) => {
      const yy = by + j * (thick + gap);
      const name = series[j];
      if (v === undefined) {
        // Not run with that series: said, not drawn.
        s += `<text x="${x0 + 6}" y="${yy + thick / 2 + 4}" font-size="11.5" fill="${t.muted}">${esc(p.missing ?? "not run")}</text>`;
        return;
      }
      s += bar(x0, yy, Math.max(0, x(v) - x0), thick, colour(t, name), `${r.label}, ${name}: ${p.value(v)}`);
      // A value ending just short of the reference line is written past it.
      const lx = p.ref !== undefined && v < p.ref && x(p.ref) - x(v) < 40 ? x(p.ref) + 6 : x(v) + 6;
      s += `<text x="${lx}" y="${yy + thick / 2 + 4}" font-size="11.5" fill="${t.secondary}" stroke="${t.surface}" stroke-width="3" paint-order="stroke" style="font-variant-numeric: tabular-nums">${esc(p.value(v))}</text>`;
    });
  });
  return { svg: s, height: plotBottom + 28 - top };
}

// A chart: a title, a subtitle, a legend when there are two series, and its panels.
function chart(t, c) {
  let y = 20;
  let s = "";
  s += `<text x="24" y="${y + 16}" font-size="16" font-weight="600" fill="${t.primary}">${esc(c.title)}</text>`;
  y += 26;
  for (const line of [].concat(c.subtitle)) {
    s += `<text x="24" y="${y + 13}" font-size="12.5" fill="${t.secondary}">${esc(line)}</text>`;
    y += 18;
  }
  const series = c.panels[0].series;
  if (series.length > 1) {
    y += 6;
    let lx = 24;
    for (const name of series) {
      s += `<rect x="${lx}" y="${y + 2}" width="12" height="12" rx="3" fill="${colour(t, name)}"/>`;
      s += `<text x="${lx + 18}" y="${y + 12}" font-size="12.5" fill="${t.primary}">${esc(name)}</text>`;
      lx += 18 + name.length * 7.5 + 22;
    }
    y += 20;
  } else {
    y += 10; // room for a reference line's label
  }
  y += 18;
  for (const p of c.panels) {
    const r = panel(t, p, y);
    s += r.svg;
    y += r.height + 10;
  }
  if (c.note) {
    s += `<text x="24" y="${y + 6}" font-size="11.5" fill="${t.muted}">${esc(c.note)}</text>`;
    y += 18;
  }
  const h = y + 8;
  const desc = c.panels
    .flatMap((p) => p.rows.map((r) => `${p.title ? p.title + ", " : ""}${r.label}: ${r.values.map((v, j) => `${p.series[j]} ${v === undefined ? (p.missing ?? "not run") : p.value(v)}`).join(", ")}`))
    .join("; ");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${h}" viewBox="0 0 ${W} ${h}" role="img" aria-labelledby="title desc" font-family='${FONT}'>
<title id="title">${esc(c.title)}</title>
<desc id="desc">${esc(desc)}</desc>
<rect width="${W}" height="${h}" rx="8" fill="${t.surface}"/>
${s}
</svg>
`;
}

const both = (r, scenarios, metric) =>
  scenarios.filter((s) => r[s]?.saranga?.[metric] !== undefined && r[s]?.artemis?.[metric] !== undefined)
    .map((s) => ({ label: NAMES[s], values: [r[s].saranga[metric], r[s].artemis[metric]] }));
const SA = ["Saranga", "Artemis"];

const charts = {};

const unbounded = ["core", "core-persistent", "core-large", "amqp", "amqp-persistent", "amqp-large",
  "openwire", "openwire-persistent", "stomp", "stomp-persistent", "mqtt", "mqtt-persistent"]
  .filter((s) => java[s]?.saranga?.throughput && java[s]?.artemis?.throughput);
// Saranga's throughput over Artemis's with the same client: the Java
// clients, and the Rust ones where there are.
const ratio = (r, s) => (r?.[s]?.saranga?.throughput && r?.[s]?.artemis?.throughput
  ? r[s].saranga.throughput / r[s].artemis.throughput : undefined);
charts.throughput = {
  title: "Throughput: Saranga against Artemis",
  subtitle: ["Messages received a second with an unbounded producer, Saranga's as a multiple of Artemis's with the same",
    "client. Past the line Saranga is faster; short of it, slower."],
  panels: [{
    series: rust ? ["Java client", "Rust client"] : ["Java client"],
    rows: unbounded.map((s) => ({ label: NAMES[s], values: rust ? [ratio(java, s), ratio(rust, s)] : [ratio(java, s)] })),
    ref: 1, refLabel: "Artemis",
    missing: "no Rust client",
    tick: (v) => `${v}×`, value: (v) => `${v.toFixed(2)}×`,
  }],
};

charts["latency"] = {
  title: "Latency (p50): lower is better",
  subtitle: "Send to receipt, with both brokers at the same rate, half the lower of their two throughputs.",
  panels: [
    {
      title: "Without persistence",
      series: SA, rows: both(java, ["core", "amqp", "openwire", "stomp", "mqtt"], "p50_us"),
      tick: (v) => `${n(v)} µs`, value: us,
    },
    {
      title: "Persistent",
      series: SA, rows: both(java, ["core-persistent", "amqp-persistent", "openwire-persistent", "stomp-persistent", "mqtt-persistent", "many-queues"], "p50_us"),
      tick: (v) => `${v / 1000} ms`, value: ms,
    },
  ],
};

charts.cpu = {
  title: "Broker CPU a message: lower is better",
  subtitle: "Microseconds of the broker's CPU time for each message, with both brokers at the same rate.",
  panels: [{
    series: SA,
    rows: both(java, ["core", "core-persistent", "amqp", "amqp-persistent", "openwire", "stomp", "stomp-persistent", "mqtt", "mqtt-persistent", "many-queues"], "cpu_us"),
    tick: (v) => `${n(v)} µs`, value: (v) => `${n(v)} µs`,
  }],
  note: "1 KiB messages. The large-message and persistent OpenWire scenarios, in milliseconds, are in the tables.",
};

if (java.startup) {
  charts.startup = {
    title: "Startup: lower is better",
    subtitle: "From launching the broker to its acceptor taking a connection (median of five), and its memory then.",
    panels: [
      {
        title: "Time to the acceptor",
        series: SA, rows: [{ label: "Startup", values: [java.startup.saranga.ms, java.startup.artemis.ms] }],
        tick: (v) => `${n(v)} ms`, value: (v) => `${n(v)} ms`,
      },
      {
        title: "Resident memory when started",
        series: SA, rows: [{ label: "Memory", values: [java.startup.saranga.idle_kb / 1024, java.startup.artemis.idle_kb / 1024] }],
        tick: (v) => `${n(v)} MiB`, value: (v) => `${n(v)} MiB`,
      },
    ],
  };
}

if (rust) {
  const rows = [];
  for (const s of ["core", "core-persistent", "amqp", "amqp-persistent", "stomp", "mqtt"]) {
    for (const [client, r] of [["Java client", java], ["Rust client", rust]]) {
      if (r[s]?.saranga?.throughput && r[s]?.artemis?.throughput) {
        rows.push({ label: `${NAMES[s]}, ${client}`, values: [r[s].saranga.throughput, r[s].artemis.throughput] });
      }
    }
  }
  charts.clients = {
    title: "Throughput with a JVM client and with a Rust one",
    subtitle: ["Messages received a second with an unbounded producer: higher is better.",
      "The Java clients are Artemis's perf client for Core and AMQP and Load.java for STOMP and MQTT; the Rust ones,",
      "scripts/bench/load for Core, STOMP and MQTT and saranga perf client for AMQP."],
    panels: [{ series: SA, rows, tick: (v) => (v >= 1000 ? `${v / 1000}K` : `${v}`), value: n }],
  };
}

mkdirSync(out, { recursive: true });
for (const [name, c] of Object.entries(charts)) {
  writeFileSync(join(out, `${name}.svg`), chart(THEMES.light, c));
  writeFileSync(join(out, `${name}-dark.svg`), chart(THEMES.dark, c));
}
console.log(`${Object.keys(charts).length} charts in ${out}: ${Object.keys(charts).join(", ")}`);
