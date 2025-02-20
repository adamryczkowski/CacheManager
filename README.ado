 = CacheManager

 == Overview

 CacheManager is a Python library that manages the cache of data that is too expensive to compute. It provides functionality to store, retrieve, and manage cached objects efficiently, ensuring that the cost of storing an object is less than the cost of re-computing it.

 == Installation

 To install CacheManager, you can use `pip`:

 ```
 pip install cachemanager
 ```

 Alternatively, you can install it using `poetry`:

 ```
 poetry add cachemanager
 ```

 == Usage

 Here is an example of how to use CacheManager in your project:

 === Initializing the Cache

 ```python
 from cachemanager import ObjectCache
 from pathlib import Path

 # Initialize the cache
 cache = ObjectCache.InitCache(Path("/path/to/cache"))
 ```

 === Storing an Object

```python
object_data = b"some data to cache"
compute_time = 0.5  # in minutes
item = cache.store_object(object_data, compute_time)
print(f"Stored object with utility: {item.utility}")
```

=== Retrieving an Object

```python
from entityhash import calc_hash

obj_hash = calc_hash(object_data)
retrieved_data = cache.get_object_by_hash(obj_hash)
if retrieved_data:
    print("Object retrieved from cache")
else:
    print("Object not found in cache")
```

=== Pruning the Cache

```python
cache.prune_cache(verbose=True)
```
