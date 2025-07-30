
# OrleansDurableStateMachines

A suite of high-performance, specialized durable state machines for [Microsoft Orleans](https://github.com/dotnet/orleans). These state machines provide rich, structured data management inside a grain with efficient, fine-grained persistence.

[![NuGet](https://img.shields.io/nuget/v/OrleansDurableStateMachines.svg)](https://www.nuget.org/packages/OrleansDurableStateMachines/)

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

* [Stack](#idurablestackt)
* [Priority Queue](#idurablepriorityqueuetelement-tpriority)
* [Ordered Set](#idurableorderedsett)
* [List Lookup](#idurablelistlookuptkey-tvalue)
* [Set Lookup](#idurablesetlookuptkey-tvalue)
* [Ordered Set Lookup](#idurableorderedsetlookuptkey-tvalue)
* [Tree](#idurabletreet)
* [Graph](#idurablegraphtnode-tedge)
* [Cancellation Token Source](#idurablecancellationtokensource)
* [Object](#idurableobjectt)

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

### `IDurableOrderedSet<T>`

A collection of unique items that preserves their original insertion order. It combines the uniqueness of a set with the ordering of a list.

**Useful for:** Tracking a sequence of unique events, processing queue for unique jobs.

```csharp
// Add returns true if the item was new
if (history.Add("user_logged_in"))
{
    await WriteStateAsync();
}

// This will return false and do nothing 
// because the item already exists.
history.Add("user_logged_in");

// The collection can be read efficiently in order.
foreach (var action in history.OrderedItems)
{
    // Process actions...
}
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

### `IDurableOrderedSetLookup<TKey, TValue>`

A one-to-many collection that maps a key to a **unique  set**  of values that maintains  **insertion order**. 

**Useful for:** Tracking user achievements in the exact order they were earned, managing ordered unique dependencies for a build system, storing timelines of unique events per entity.

```csharp
// Track the unique products a user viewed, in order.
lookup.Add(userId, "product-123");
lookup.Add(userId, "product-456");

// This call will return false and have no effect,
// as the product is already in the set for this user.
lookup.Add(userId, "product-123");

await WriteStateAsync();

// The values for a key are always returned 
// in their original insertion order.

// -> ["product-123", "product-456"]
var viewedProducts = lookup[userId];
```

**Why use  `IDurableOrderedSetLookup<,>`?**  

When you need both uniqueness and order for values associated with a key.

-   `IDurableListLookup<,>`  **allows duplicate** values!
    `IDurableOrderedSetLookup<,>`  **enforces uniqueness**, which is safer if duplicates are not desired.
    
-   `IDurableSetLookup<,>` **provides uniqueness** but does  **not  guarantee** any specific order.  `IDurableOrderedSetLookup<,>`  preserves the original **insertion order**, making it ideal for sequences, histories, or timelines.

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
### `IDurableObject<T>`

A streamlined container for a single complex object (POCO) that allows for direct mutation of its properties. The idea behind  `IDurableObject<T>`  came from the desire to create the "best of both worlds": an API with the simplicity of `IDurableValue<T>`, but with the ability to mutate a complex object directly like `IPersistentState<T>`.

> Ideally you should not be making the object complex, but instead use the appropriate state machines for lists, sets, dictionary, etc...

**Useful for:**  Managing any complex state class, like a  `UserProfile`,  `GameSession`, or  `OrderDetails`, without the ceremony of creating new instances for every change.

```csharp
// Direct mutation is the intended pattern.

state.Value.Counter++;
state.Value.LastUpdated = DateTime.UtcNow;
state.Value.Nested.Name = "New Nested Name";

await WriteStateAsync();

```

#### Why not use  `IDurableValue<T>`  for complex objects?

While possible, it's cumbersome and dare I say even error-prone. You must create a new object instance for every change, which can lead to verbose code and bugs if you forget to copy a property.

```csharp
// Assume you have an IDurableValue<MyState> named 'state'.

// ------------- Incorrect approach -------------

// We intuitively mutate the object directly. 
// This changes the in-memory object, but the state machine
// doesn't know a change occurred.

// The setter for 'state.Value' was never called, 
// so this write does nothing. 

state.Value.Counter++;
await WriteStateAsync();

// After a re-activation, the counter change above will be lost.

// ------------- Correct approach -------------

var newState = new MyState 
{ 
	Counter = state.Value.Counter + 1,
	Name = state.Value.Name
};

// We need to assign the new instance to trigger the setter.

state.Value = newState; 
await WriteStateAsync();

// Now the change is persisted correctly.
```

#### Why not just use  `IPersistentState<T>`?

`DurableState<T>`  (*the implementation of  `IPersistentState<T>`*) exists primarily for  **familiarity and migration**  from the classic Orleans state model. It works, but it **has to** bring along some jargon and potential confusion:

-   **Extra API Surface:**  It exposes the full  `IStorage<T>`  contract, including  `Etag`,  `ClearStateAsync()`, etc., which are not needed when using  `DurableGrain`.

-   **Confusing Write Calls:**  You can call  `state.State.WriteStateAsync()`, which feels disconnected from the  `DurableGrain`'s own  `WriteStateAsync()`  method.

`IDurableObject<T>`  is the purpose-built, ergonomic choice for this library. It provides only the essential features (`Value`,  `RecordExists`) in a cleaner package, making your grain code simpler and more predictable.

#### Why Can't  `Value`  be set to  `null`?

This is a deliberate design choice for safety and predictability. The  `get`  accessor for  `Value`  guarantees it will never return  `null`  (*it creates a new instance if one doesn't exist*). Allowing the setter to accept  `null`  would create a confusing and inconsistent state.

```csharp
// This is NOT allowed and will throw an ArgumentNullException:
state.Value = null;
```

By disallowing  `null`,  `IDurableObject<T>`  ensures that you can always safely access and mutate the state object without needing to perform null checks, preventing a common source of  `NullReferenceException`s.

#### Why no  `Etag`?

An `Etag` is a token used to detect race conditions where multiple processes might update the same record. This component doesn't need one because Orleans ensures only a single grain activation is the "writer," and all changes are appended to an immutable log using a custom binary protocol. Any process attempting to write to the log outside the state machine would corrupt the state, making external updates inherently not feasible, and even unsafe.

---

If you find it helpful, please consider giving it a ⭐ and share it!

Copyright © Ledjon Behluli
