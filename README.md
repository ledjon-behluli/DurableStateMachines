# DurableStateMachines

A suite of high-performance, specialized durable state machines for [Microsoft Orleans](https://github.com/dotnet/orleans). These state machines provide rich, structured data management inside a grain with efficient, fine-grained persistence.

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

If you find it helpful, please consider giving it a ⭐ and share it!

Copyright © Ledjon Behluli
