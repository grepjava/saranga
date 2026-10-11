// Phase 7 client matrix: AMQP 1.0 with AMQPNetLite 2.5.4, the same checks
// as tests/interop/clients/python/amqp_check.py.
//   dotnet run -- host:port [test-name...]
// Run through scripts/client-matrix.sh.

using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

var address = args.Length > 0 ? args[0] : "127.0.0.1:61616";
var only = new HashSet<string>(args.Skip(1));
var run = Guid.NewGuid().ToString("N")[..8];

string Queue(string name) => $"clients.dotnet.{name}.{run}";

async Task<(Connection, Session)> Connect()
{
    var c = await Connection.Factory.CreateAsync(new Address($"amqp://{address}"));
    return (c, new Session(c));
}

void Check(bool cond, string what)
{
    if (!cond) throw new Exception(what);
}

void Eq(object? want, object? got, string what)
{
    if (!Equals(want, got)) throw new Exception($"{what}: want <{want}> got <{got}>");
}

var tests = new SortedDictionary<string, Func<Task>>
{
    // Bodies, application properties and header fields come back.
    ["round_trip"] = async () =>
    {
        var (c, s) = await Connect();
        try
        {
            var q = Queue("roundtrip");
            var r = new ReceiverLink(s, "r", q);
            var snd = new SenderLink(s, "s", q);
            for (var i = 0; i < 3; i++)
            {
                var m = new Message($"hello {i}")
                {
                    Header = new Header { Durable = true, Priority = 7 },
                    Properties = new Properties { Subject = "greeting", CorrelationId = $"corr-{i}" },
                    ApplicationProperties = new ApplicationProperties()
                };
                m.ApplicationProperties["n"] = i;
                m.ApplicationProperties["who"] = "dotnet";
                await snd.SendAsync(m);
            }
            for (var i = 0; i < 3; i++)
            {
                var m = await r.ReceiveAsync(TimeSpan.FromSeconds(5));
                Check(m != null, "no message");
                Eq($"hello {i}", m!.Body, "body");
                Eq(i, m.ApplicationProperties["n"], "property n");
                Eq("dotnet", m.ApplicationProperties["who"], "property who");
                Eq("greeting", m.Properties.Subject, "subject");
                Eq($"corr-{i}", m.Properties.CorrelationId, "correlation id");
                Eq((byte)7, m.Header.Priority, "priority");
                Check(m.Header.Durable, "durable");
                r.Accept(m);
            }
        }
        finally
        {
            await c.CloseAsync();
        }
    },
    // A message modified as failed comes back, counted once more.
    ["modified_redelivers"] = async () =>
    {
        var (c, s) = await Connect();
        try
        {
            var q = Queue("modified");
            var r = new ReceiverLink(s, "r", q);
            await new SenderLink(s, "s", q).SendAsync(new Message("again"));
            var m = await r.ReceiveAsync(TimeSpan.FromSeconds(5));
            Check(m != null, "no message");
            Eq(0u, m!.Header?.DeliveryCount ?? 0u, "first delivery count");
            r.Modify(m, true);
            m = await r.ReceiveAsync(TimeSpan.FromSeconds(5));
            Check(m != null, "not redelivered");
            Eq("again", m!.Body, "redelivered body");
            Eq(1u, m.Header.DeliveryCount, "delivery count after a failed delivery");
            r.Accept(m);
        }
        finally
        {
            await c.CloseAsync();
        }
    },
    // A body far over a frame crosses whole.
    ["large_body"] = async () =>
    {
        var (c, s) = await Connect();
        try
        {
            var q = Queue("large");
            var r = new ReceiverLink(s, "r", q);
            var body = new byte[600_000];
            for (var i = 0; i < body.Length; i++) body[i] = (byte)(i * 7 % 251);
            await new SenderLink(s, "s", q).SendAsync(new Message { BodySection = new Data { Binary = body } });
            var m = await r.ReceiveAsync(TimeSpan.FromSeconds(10));
            Check(m != null, "no message");
            var got = (byte[])m!.Body;
            Check(got.SequenceEqual(body), "body differs");
            r.Accept(m);
        }
        finally
        {
            await c.CloseAsync();
        }
    },
    // An amqp-value map keeps its entries and their types.
    ["map_body"] = async () =>
    {
        var (c, s) = await Connect();
        try
        {
            var q = Queue("map");
            var r = new ReceiverLink(s, "r", q);
            var map = new Map { ["text"] = "t", ["number"] = 42, ["flag"] = true, ["float"] = 1.5 };
            await new SenderLink(s, "s", q).SendAsync(new Message(map));
            var m = await r.ReceiveAsync(TimeSpan.FromSeconds(5));
            Check(m != null, "no message");
            var got = (Map)m!.Body;
            foreach (var k in map.Keys) Eq(map[k], got[k], $"entry {k}");
            Eq(map.Count, got.Count, "entries");
            r.Accept(m);
        }
        finally
        {
            await c.CloseAsync();
        }
    },
    // A rejected message is not delivered again.
    ["rejected_is_gone"] = async () =>
    {
        var (c, s) = await Connect();
        try
        {
            var q = Queue("reject");
            var r = new ReceiverLink(s, "r", q);
            var snd = new SenderLink(s, "s", q);
            await snd.SendAsync(new Message("bad"));
            await snd.SendAsync(new Message("good"));
            var m = await r.ReceiveAsync(TimeSpan.FromSeconds(5));
            Eq("bad", m?.Body, "first");
            r.Reject(m);
            m = await r.ReceiveAsync(TimeSpan.FromSeconds(5));
            Eq("good", m?.Body, "second");
            r.Accept(m);
            m = await r.ReceiveAsync(TimeSpan.FromSeconds(1));
            Check(m == null, $"delivered again: {m?.Body}");
        }
        finally
        {
            await c.CloseAsync();
        }
    },
};

var failed = 0;
foreach (var (name, test) in tests)
{
    if (only.Count > 0 && !only.Contains(name)) continue;
    var sw = Stopwatch.StartNew();
    try
    {
        await test();
        Console.WriteLine($"PASS {"dotnet." + name,-40} {sw.ElapsedMilliseconds,5} ms");
    }
    catch (Exception e)
    {
        failed++;
        Console.WriteLine($"FAIL {"dotnet." + name,-40} {e.GetType().Name}: {e.Message}");
    }
}
Console.WriteLine(failed == 0 ? "ALL PASSED" : $"{failed} FAILED");
return failed == 0 ? 0 : 1;
