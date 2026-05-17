# ProducerConsumerPoC

## Overview

This project demonstrates a simple producer/consumer system in .NET using a bounded `Channel<T>`.
The system exposes a queue API for producers and manages a fixed pool of consumer tasks that process queued items asynchronously.

## Core design

### `ProducerConsumerSystem<T>`

The main worker is implemented in `ProducerConsumerSystem.cs`.

Key responsibilities:
- maintain a bounded queue using `Channel<T>`
- start a fixed number of consumer tasks
- accept asynchronous enqueue requests from producers
- drain the queue fully before shutdown
- isolate handler exceptions so one bad item does not stop the consumer pool

### Queue implementation

The queue is built with `Channel.CreateBounded<T>(BoundedChannelOptions)`.

Important configured options:
- `capacity`: maximum number of queued items before producers wait
- `FullMode = BoundedChannelFullMode.Wait`: when the queue is full, writes asynchronously wait for free space
- `SingleReader = false` and `SingleWriter = false`: allows multiple producers and multiple consumers safely

### Producer behavior

Producers call `EnqueueAsync(item, cancellationToken)`.
- If the queue has room, the item is queued immediately.
- If the queue is full, the producer waits asynchronously until a consumer removes an item.
- This avoids busy waiting and keeps CPU usage low.

### Consumer behavior

Each consumer runs a loop in `ConsumerLoopAsync`:
1. Wait for data using `_channel.Reader.WaitToReadAsync(token)`
2. Read all available items with `_channel.Reader.TryRead(out var item)`
3. Invoke the configured `_handler(item)` for each item

The channel wait is asynchronous, so idle consumers do not spin or consume CPU while waiting.

### Shutdown flow

The shutdown process is managed by `StopAsync()`:
- `StopAsync()` calls `_channel.Writer.Complete()`
- Consumers continue reading remaining items until the channel is empty
- `Task.WhenAll(_consumers)` waits for all consumer tasks to finish

This ensures a graceful shutdown where enqueued work is still processed.

## Error handling

Inside each consumer loop, handler execution is wrapped in a `try/catch`.
This prevents a single handler exception from terminating the consumer task and allows the system to keep processing further items.

## Usage

In `Program.cs`, you can create and run the system like this:

```csharp
var system = new ProducerConsumerSystem<string>(consumerCount: 3, capacity: 10, handler: async item =>
{
    Console.WriteLine($"Processing {item}");
    await Task.Delay(500);
});

await system.StartAsync();
await system.EnqueueAsync("Work-1");
await system.StopAsync();
```

## Notes

- The bounded channel provides natural backpressure for producers.
- Consumers wait efficiently when the queue is empty.
- The system is best suited for workloads where tasks can be processed asynchronously and the number of consumers is fixed.
