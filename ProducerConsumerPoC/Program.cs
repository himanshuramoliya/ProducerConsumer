using System.Linq;
using System.Threading;
using ProducerConsumerPoC;

var cancellationTokenSource = new CancellationTokenSource();
var system = new ProducerConsumerSystem<string>(consumerCount: 3, capacity: 10, handler: async item =>
{
    Console.WriteLine($"[Consumer] Processing item: {item}");
    await Task.Delay(500, cancellationTokenSource.Token).ConfigureAwait(false);
    Console.WriteLine($"[Consumer] Completed item: {item}");
});

await system.StartAsync(cancellationTokenSource.Token);

Console.WriteLine("Enqueuing work items...");

var producerTasks = Enumerable.Range(1, 20)
    .Select(i => system.EnqueueAsync($"Work-{i}", cancellationTokenSource.Token))
    .ToArray();

await Task.WhenAll(producerTasks).ConfigureAwait(false);

Console.WriteLine("All items enqueued. Waiting for consumers to finish...");

await system.StopAsync().ConfigureAwait(false);

Console.WriteLine("Shutdown complete.");
