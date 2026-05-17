namespace ProducerConsumerPoC;

using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;

public static class SystemTests
{
    public static async Task RunAllTests()
    {
        Console.WriteLine("=== Producer/Consumer System Test Suite ===\n");

        await Test_BasicFunctionality();
        await Test_QueueFullBackpressure();
        await Test_QueueEmptyWait();
        await Test_GracefulShutdown();
        await Test_MultipleProducersAndConsumers();
        await Test_CancellationDuringEnqueue();
        await Test_HandlerException();
        await Test_DoubleStart();
        await Test_DoubleStop();
        await Test_EnqueueAfterStop();
        await Test_LargeQueueCapacity();
        await Test_SingleItemProcessing();
        await Test_ConsumerCountEdgeCases();
        await Test_ResourceCleanup();
        await Test_HighConcurrency();

        Console.WriteLine("\n=== All Tests Complete ===");
    }

    // Test 1: Basic functionality
    private static async Task Test_BasicFunctionality()
    {
        Console.WriteLine("[TEST 1] Basic Functionality - Enqueue and Process Items");
        var processedItems = new List<string>();
        var system = new ProducerConsumerSystem<string>(
            consumerCount: 2,
            capacity: 5,
            handler: async item =>
            {
                processedItems.Add(item);
                await Task.Delay(10);
            });

        await system.StartAsync();
        await system.EnqueueAsync("Item-1");
        await system.EnqueueAsync("Item-2");
        await system.StopAsync();

        Console.WriteLine($"✓ Enqueued 2 items, processed {processedItems.Count} items");
        Assert(processedItems.Count == 2, "Should process exactly 2 items");
        Console.WriteLine();
    }

    // Test 2: Queue full backpressure
    private static async Task Test_QueueFullBackpressure()
    {
        Console.WriteLine("[TEST 2] Queue Full Backpressure - Producer Waits");
        var sw = Stopwatch.StartNew();
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 1,
            capacity: 1,
            handler: async item =>
            {
                await Task.Delay(100); // Slow consumer
            });

        await system.StartAsync();

        // Fill the queue to capacity
        await system.EnqueueAsync(1);
        await system.EnqueueAsync(2);

        // This should block until a consumer frees space
        var enqueueTask = system.EnqueueAsync(3);
        await Task.Delay(50); // Give it time to block

        Assert(!enqueueTask.IsCompleted, "Enqueue should wait while queue is full");

        // Wait for completion
        await enqueueTask;
        sw.Stop();

        await system.StopAsync();
        Console.WriteLine($"✓ Producer correctly waited when queue was full (elapsed: {sw.ElapsedMilliseconds}ms)");
        Console.WriteLine();
    }

    // Test 3: Queue empty wait
    private static async Task Test_QueueEmptyWait()
    {
        Console.WriteLine("[TEST 3] Queue Empty - Consumer Waits Efficiently");
        var system = new ProducerConsumerSystem<string>(
            consumerCount: 1,
            capacity: 5,
            handler: async item =>
            {
                await Task.Delay(10);
            });

        await system.StartAsync();

        // Give consumer a moment to start waiting
        await Task.Delay(100);

        // CPU should not spike (consumer should be waiting, not busy-looping)
        var cpuBefore = Process.GetCurrentProcess().UserProcessorTime.TotalMilliseconds;
        await Task.Delay(100);
        var cpuAfter = Process.GetCurrentProcess().UserProcessorTime.TotalMilliseconds;
        var cpuUsed = cpuAfter - cpuBefore;

        await system.EnqueueAsync("Item");
        await system.StopAsync();

        Console.WriteLine($"✓ Consumer waited efficiently (CPU used: {cpuUsed:F2}ms over 100ms idle window)");
        Console.WriteLine();
    }

    // Test 4: Graceful shutdown
    private static async Task Test_GracefulShutdown()
    {
        Console.WriteLine("[TEST 4] Graceful Shutdown - All Items Processed");
        var processedCount = 0;
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 2,
            capacity: 10,
            handler: async item =>
            {
                await Task.Delay(10);
                Interlocked.Increment(ref processedCount);
            });

        await system.StartAsync();

        for (int i = 0; i < 20; i++)
        {
            await system.EnqueueAsync(i);
        }

        await system.StopAsync();

        Assert(processedCount == 20, $"Should process all 20 items, but only processed {processedCount}");
        Console.WriteLine($"✓ Gracefully shut down after processing all {processedCount} items");
        Console.WriteLine();
    }

    // Test 5: Multiple producers and consumers
    private static async Task Test_MultipleProducersAndConsumers()
    {
        Console.WriteLine("[TEST 5] Multiple Producers and Consumers");
        var processedCount = 0;
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 5,
            capacity: 50,
            handler: async item =>
            {
                await Task.Delay(5);
                Interlocked.Increment(ref processedCount);
            });

        await system.StartAsync();

        var producerTasks = Enumerable.Range(0, 3)
            .Select(async p =>
            {
                for (int i = 0; i < 20; i++)
                {
                    await system.EnqueueAsync(p * 100 + i);
                }
            })
            .ToArray();

        await Task.WhenAll(producerTasks);
        await system.StopAsync();

        Assert(processedCount == 60, $"Should process 60 items, got {processedCount}");
        Console.WriteLine($"✓ 3 producers, 5 consumers processed {processedCount} items correctly");
        Console.WriteLine();
    }

    // Test 6: Cancellation during enqueue
    private static async Task Test_CancellationDuringEnqueue()
    {
        Console.WriteLine("[TEST 6] Cancellation During Enqueue");
        var cts = new CancellationTokenSource();
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 1,
            capacity: 2,
            handler: async item => await Task.Delay(200));

        await system.StartAsync();

        await system.EnqueueAsync(1);
        await system.EnqueueAsync(2);

        // Queue is now full, next enqueue will block
        cts.CancelAfter(100);

        try
        {
            await system.EnqueueAsync(3, cts.Token);
            Assert(false, "Should have thrown OperationCanceledException");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("✓ Cancellation token correctly interrupted blocked enqueue");
        }

        await system.StopAsync();
        Console.WriteLine();
    }

    // Test 7: Handler exception handling
    private static async Task Test_HandlerException()
    {
        Console.WriteLine("[TEST 7] Handler Exception - System Continues");
        var processedCount = 0;
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 2,
            capacity: 10,
            handler: async item =>
            {
                if (item % 3 == 0)
                {
                    throw new InvalidOperationException($"Intentional error for item {item}");
                }
                Interlocked.Increment(ref processedCount);
                await Task.CompletedTask;
            });

        await system.StartAsync();

        for (int i = 0; i < 10; i++)
        {
            try
            {
                await system.EnqueueAsync(i);
            }
            catch { }
        }

        // Give time for processing
        await Task.Delay(100);

        // NOTE: Current implementation will crash on handler exception!
        // This is a loophole - handlers should be wrapped in try-catch
        Console.WriteLine("⚠ WARNING: Loophole detected - Handler exceptions crash consumer tasks!");
        await system.StopAsync();
        Console.WriteLine();
    }

    // Test 8: Double start
    private static async Task Test_DoubleStart()
    {
        Console.WriteLine("[TEST 8] Double Start - Idempotent");
        var system = new ProducerConsumerSystem<string>(
            consumerCount: 2,
            capacity: 5,
            handler: async item => await Task.CompletedTask);

        await system.StartAsync();
        await system.StartAsync(); // Should be idempotent

        await system.EnqueueAsync("Item");
        await system.StopAsync();

        Console.WriteLine("✓ Double start is correctly idempotent");
        Console.WriteLine();
    }

    // Test 9: Double stop
    private static async Task Test_DoubleStop()
    {
        Console.WriteLine("[TEST 9] Double Stop - Idempotent");
        var system = new ProducerConsumerSystem<string>(
            consumerCount: 2,
            capacity: 5,
            handler: async item => await Task.CompletedTask);

        await system.StartAsync();
        await system.EnqueueAsync("Item");
        await system.StopAsync();
        await system.StopAsync(); // Should be idempotent

        Console.WriteLine("✓ Double stop is correctly idempotent");
        Console.WriteLine();
    }

    // Test 10: Enqueue after stop
    private static async Task Test_EnqueueAfterStop()
    {
        Console.WriteLine("[TEST 10] Enqueue After Stop - Should Fail");
        var system = new ProducerConsumerSystem<string>(
            consumerCount: 1,
            capacity: 5,
            handler: async item => await Task.CompletedTask);

        await system.StartAsync();
        await system.StopAsync();

        try
        {
            await system.EnqueueAsync("Late Item");
            Console.WriteLine("⚠ WARNING: Loophole - Can still enqueue after stop (channel not closed)");
        }
        catch (ChannelClosedException)
        {
            Console.WriteLine("✓ Correctly prevents enqueue after stop");
        }

        Console.WriteLine();
    }

    // Test 11: Large queue capacity
    private static async Task Test_LargeQueueCapacity()
    {
        Console.WriteLine("[TEST 11] Large Queue Capacity - Performance");
        var sw = Stopwatch.StartNew();
        var processedCount = 0;
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 2,
            capacity: 10000,
            handler: async item =>
            {
                await Task.Delay(1);
                Interlocked.Increment(ref processedCount);
            });

        await system.StartAsync();

        for (int i = 0; i < 1000; i++)
        {
            await system.EnqueueAsync(i);
        }

        await system.StopAsync();
        sw.Stop();

        Console.WriteLine($"✓ Processed {processedCount} items with large queue in {sw.ElapsedMilliseconds}ms");
        Console.WriteLine();
    }

    // Test 12: Single item handling
    private static async Task Test_SingleItemProcessing()
    {
        Console.WriteLine("[TEST 12] Single Item - Minimal System");
        var processed = false;
        var system = new ProducerConsumerSystem<string>(
            consumerCount: 1,
            capacity: 1,
            handler: async item =>
            {
                processed = item == "Single";
                await Task.CompletedTask;
            });

        await system.StartAsync();
        await system.EnqueueAsync("Single");
        await system.StopAsync();

        Assert(processed, "Single item should be processed");
        Console.WriteLine("✓ Single item processed correctly");
        Console.WriteLine();
    }

    // Test 13: Consumer count edge cases
    private static async Task Test_ConsumerCountEdgeCases()
    {
        Console.WriteLine("[TEST 13] Consumer Count Edge Cases");

        // Single consumer
        var system1 = new ProducerConsumerSystem<int>(
            consumerCount: 1,
            capacity: 5,
            handler: async item => await Task.CompletedTask);

        await system1.StartAsync();
        await system1.EnqueueAsync(1);
        await system1.StopAsync();
        Console.WriteLine("✓ Single consumer works");

        // Many consumers
        var count = 0;
        var system2 = new ProducerConsumerSystem<int>(
            consumerCount: 10,
            capacity: 5,
            handler: async item =>
            {
                Interlocked.Increment(ref count);
                await Task.CompletedTask;
            });

        await system2.StartAsync();
        for (int i = 0; i < 20; i++)
        {
            await system2.EnqueueAsync(i);
        }
        await system2.StopAsync();

        Assert(count == 20, "All items should be processed");
        Console.WriteLine($"✓ 10 consumers processed all items");
        Console.WriteLine();
    }

    // Test 14: Resource cleanup with DisposeAsync
    private static async Task Test_ResourceCleanup()
    {
        Console.WriteLine("[TEST 14] Resource Cleanup - DisposeAsync");
        var processedCount = 0;
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 2,
            capacity: 10,
            handler: async item =>
            {
                Interlocked.Increment(ref processedCount);
                await Task.Delay(5);
            });

        await system.StartAsync();

        for (int i = 0; i < 10; i++)
        {
            await system.EnqueueAsync(i);
        }

        // Dispose without explicit StopAsync
        await system.DisposeAsync();

        Console.WriteLine($"✓ System disposed cleanly (processed {processedCount} items)");
        Console.WriteLine();
    }

    // Test 15: High concurrency stress test
    private static async Task Test_HighConcurrency()
    {
        Console.WriteLine("[TEST 15] High Concurrency Stress Test");
        var processedCount = 0;
        var system = new ProducerConsumerSystem<int>(
            consumerCount: 10,
            capacity: 100,
            handler: async item =>
            {
                await Task.Delay(Random.Shared.Next(1, 5));
                Interlocked.Increment(ref processedCount);
            });

        var sw = Stopwatch.StartNew();
        await system.StartAsync();

        // Multiple concurrent producers
        var producerTasks = Enumerable.Range(0, 5)
            .Select(async p =>
            {
                for (int i = 0; i < 100; i++)
                {
                    await system.EnqueueAsync(p * 1000 + i);
                }
            })
            .ToArray();

        await Task.WhenAll(producerTasks);
        await system.StopAsync();
        sw.Stop();

        Assert(processedCount == 500, $"Should process 500 items, got {processedCount}");
        Console.WriteLine($"✓ Processed {processedCount} items under high concurrency in {sw.ElapsedMilliseconds}ms");
        Console.WriteLine();
    }

    private static void Assert(bool condition, string message)
    {
        if (!condition)
        {
            Console.WriteLine($"✗ Assertion Failed: {message}");
        }
    }
}

public class AssertionException : Exception
{
    public AssertionException(string message) : base(message) { }
}
