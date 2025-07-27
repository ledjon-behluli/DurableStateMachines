# DurableStateMachines

A suite of high-performance, specialized durable state machines for [Microsoft Orleans](https://github.com/dotnet/orleans). These state machines provide rich, structured data management inside a grain with efficient, fine-grained persistence.

[![NuGet](https://img.shields.io/nuget/v/DurableStateMachines.svg)](https://www.nuget.org/packages/DurableStateMachines/)

## Getting Started

### Registration

In your silo configuration, register the state machines.

```csharp
siloBuilder.Services.AddDurableStateMachines();
```

### Usage

Inherit from `DurableGrain` and inject one or more state machines using the `[FromKeyedServices]` attribute. The key provided is used to isolate the log-based storage for that state machine.

```csharp
public class JobSchedulerGrain(
    [FromKeyedServices("job-queue")] 
    IDurablePriorityQueue<string, int> jobs) 
	    : DurableGrain, IJobSchedulerGrain
{
    public async Task AddJob(string jobName, int priority)
    {
        // 1. Mutate the in-memory state.
        jobs.Enqueue(jobName, priority);

        // 2. Persist the change to the journal.
        await WriteStateAsync();
    }

    public Task<string> GetNextJob()
    {
        // Reads are always from the in-memory state.
        return Task.FromResult(jobs.Peek());
    }
}
```

---

## State Machines

* **Stack**
* **Priority Queue**
* **List Lookup**
* **Set Lookup**
* **Tree**
* **Graph**
* **Cancellation Token Source**

---

### `IDurableStack<T>`

A classic LIFO stack.

**Useful for:** Managing sequential tasks, command history (undo/redo), or processing items in reverse order of arrival.

```csharp
stack.Push(newItem);
await WriteStateAsync();

if (stack.TryPop(out var item))
{
    // Process 'item' ...
    await WriteStateAsync();
}
```

---

### `IDurablePriorityQueue<TElement, TPriority>`

A collection where items are dequeued based on priority (*lowest value first*).

**Useful for:** Task schedulers, job processing queues, or pathfinding algorithms.

```csharp
queue.Enqueue("Low priority task", 100);
queue.Enqueue("High priority task", 1);
await WriteStateAsync();

// Dequeue will always return "High priority task" first.
var nextTask = queue.Dequeue();
await WriteStateAsync();
```

---

### `IDurableListLookup<TKey, TValue>`

A one-to-many collection that maps a key to a **list** of values, allowing duplicates.

**Useful for:** Grouping items, such as log messages by category, or products by tags.

```csharp
lookup.Add("electronics", productId);
lookup.Add("on-sale", productId);

await WriteStateAsync();
```

#### Why not use `IDurableDictionary<TKey, List<TValue>>`?

Using a durable dictionary with `List<T>` means every change—like adding or   removing a single item—requires re-serializing and persisting the entire list. This leads to unnecessary overhead and coarse-grained writes.

`IDurableListLookup<TKey, TValue>` provides fin(er)-grained persistence, where only the specific operation is tracked and stored. It's more efficient for frequent updates and avoids full rewrites for elements of any given key.

---

### `IDurableSetLookup<TKey, TValue>`

A one-to-many collection that maps a key to a unique **set** of values.

**Useful for:** Managing relationships where duplicates are not allowed, such as user roles or category memberships. More efficient than `IDurableListLookup<,>` when uniqueness is required.

```csharp
// If the role is already present, this returns false and does nothing.
if (lookup.Add(userId, "Admin"))
{
    await WriteStateAsync();
}
```

#### Why not use `IDurableDictionary<TKey, HashSet<TValue>>`?

Using a durable dictionary with `HashSet<T>` means every change—like adding or   removing a single item—requires re-serializing and persisting the entire list. This leads to unnecessary overhead and coarse-grained writes.

`IDurableSetLookup<TKey, TValue>` provides fin(er)-grained persistence, where only the specific operation is tracked and stored. It's more efficient for frequent updates and avoids full rewrites for elements of any given key.

---

### `IDurableTree<T>`

A hierarchical data structure representing parent-child relationships. Models true hierarchies efficiently. Unlike a simple list, it provides fast traversal of children, parents, and descendants, and validates against cyclical relationships.

**Useful for:** Organizational charts, file systems, comment threads, or category trees.

```csharp
tree.SetRoot("/");
tree.Add("/", "system");
tree.Add("/", "users");
tree.Add("users", "Alice");
await WriteStateAsync();

// Moves the whole branch "/users" to "/system/users"
tree.Move("users", "system");
await WriteStateAsync();
```

---

### `IDurableGraph<TNode, TEdge>`

A flexible structure representing nodes connected by directed edges. A state machine for modeling complex, many-to-many relationships, including dependencies and cycles.

**Useful for:** Social networks, dependency graphs, network topologies, or even finite-state machines.

```csharp
graph.AddNode("TaskA");
graph.AddNode("TaskB");
graph.AddNode("TaskC");

graph.AddEdge("TaskA", "TaskB", "Depends on");
graph.AddEdge("TaskB", "TaskC", "Depends on");
graph.AddEdge("TaskC", "TaskA", "Blocks"); // Cycles are allowed!

await WriteStateAsync();
```

---

### `IDurableCancellationTokenSource`

A durable, persistent version of the standard  `CancellationTokenSource`. It allows a cancellation signal to survive grain deactivation and reactivation.

**Useful for:**

-   Implementing reliable, supervisory timeouts for long-running jobs.
-   Propagating a cancellation request across multiple grains or services.
-   Ensuring that a user-initiated cancellation is permanent and honored even if the grain restarts.

#### Key Concepts

-   **`IsCancellationPending`** -  This boolean reflects the  **immediate, in-memory status**. It returns  `true`  as soon as  `Cancel()`  is called or a  `CancelAfter()`  timer expires. Use this for quick, synchronous checks before the state is durably saved.
    
-   **`Token`** -  This is the standard  `CancellationToken`  you pass to other methods. This token is only signaled  **after**  the cancellation has been durably persisted via  `WriteStateAsync()`. This represents the  **committed**  state of cancellation.
    
-   **`Cancel()`** -  Sets  `IsCancellationPending`  to  `true`  in memory. To make the cancellation take effect and signal the  `Token`, you must follow up with a call to  `WriteStateAsync()`.
    
-   **`CancelAfter(TimeSpan delay)`** -  Schedules a future cancellation. This method has two key behaviors:
    
    1.  **Persisting the Intent:**  You must call  `WriteStateAsync()`  to make the scheduled timeout durable across restarts.
    2.  **Automatic Persistence:**  Once the timer expires, the component will  **automatically trigger its own persistence**, setting the final canceled state without requiring another manual  `WriteStateAsync()`  call.

#### Callbacks and Threading

Any callbacks registered via  `Token.Register()`  will execute on the default  `TaskScheduler`  (usually the .NET thread pool). This means callbacks will run  **outside the Orleans grain scheduler**, which is important to know for thread safety and accessing grain state.

#### Simple Cancellable Operation

> For more advanced examples, including job orchestration and distributed cancellation, see the [**playground**](https://github.com/ledjon-behluli/DurableStateMachines/blob/main/playground/DurableStateMachines.CTS/Program.cs) app.

```csharp
public class LongRunningTaskGrain(
    [FromKeyedServices("task-cancel")]
    IDurableCancellationTokenSource cancelSource)
        : DurableGrain, ILongRunningTaskGrain
{
    public async Task StartTaskWithTimeout(TimeSpan timeout)
    {
        cancelSource.CancelAfter(timeout);
        await WriteStateAsync();
        ProcessItems(cancelSource.Token).Ignore();
    }

    public async Task CancelTaskByUser()
    {
        cancelSource.Cancel();
        await WriteStateAsync();
    }

    private async Task ProcessItems(CancellationToken token)
    {
        try
        {
            await foreach (var item in GetItemsFromSource(token))
            {
                // This loop will be gracefully terminated
                // if 'token' is signaled.
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore
        }
    }
}
```

---

If you find it helpful, please consider giving it a ⭐ and share it!

Copyright © Ledjon Behluli
