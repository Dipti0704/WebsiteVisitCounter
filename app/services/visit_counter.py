import time
import asyncio
from ..core.redis_manager import RedisManager

class VisitCounterService:
    """Service to manage visit counts with an application cache and Redis."""
    
    CACHE_TTL = 5
    FLUSH_INTERVAL = 30 

    def __init__(self):
        """Initialize with a RedisManager instance, in-memory cache, and a batch buffer."""
        self.redis_manager = RedisManager()
        self.visit_cache = {}  # { page_id: (visit_count, timestamp) }
        self.visit_buffer = {}  # { page_id: visit_count }
        self.flush_task = None  # Task should start only when needed

    def start_background_tasks(self):
        """✅ Start periodic flush task safely."""
        if not self.flush_task:
            self.flush_task = asyncio.create_task(self.periodic_flush())

    async def increment_visit(self, page_id: str) -> None:
        """✅ Increment visit count in memory and ensure immediate cache update."""
        self.visit_buffer[page_id] = self.visit_buffer.get(page_id, 0) + 1  # ✅ Store in buffer
        self.start_background_tasks()  # ✅ Ensure periodic flush task is running
        
        # 🔥 Update in-memory cache immediately to reflect changes
        current_time = time.time()
        cached_count = self.visit_cache.get(page_id, (0, current_time))[0]
        self.visit_cache[page_id] = (cached_count + 1, current_time)  # ✅ Cache update

    async def get_visit_count(self, page_id: str) -> int:
        """✅ Get visit count from memory buffer, in-memory cache, and Redis."""
        # 🔥 First, check in-memory cache for the most recent value
        await self.flush_to_redis()
        if page_id in self.visit_cache:
            cached_count, timestamp = self.visit_cache[page_id]
            return cached_count + self.visit_buffer.get(page_id, 0)  # ✅ Cache + buffer

        # 🔥 If not in cache, check Redis
        visit_count = self.redis_manager.get(f"visits:{page_id}")

        if visit_count is None:  # ✅ If cache miss, flush the buffer and re-fetch
            visit_count = self.redis_manager.get(f"visits:{page_id}") or 0  # ✅ Get updated count
        
        # 🔥 Update cache with the latest value from Redis
        current_time = time.time()
        self.visit_cache[page_id] = (visit_count, current_time)

        return visit_count + self.visit_buffer.get(page_id, 0)  # ✅ Include buffered visits

    async def flush_to_redis(self):
        """✅ Flush all buffered visit counts to Redis in batch."""
        if not self.visit_buffer:
            return  # Nothing to flush

        try:
            for page_id, count in self.visit_buffer.items():
                self.redis_manager.increment(f"visits:{page_id}", count)  # ✅ Batch write

                # 🔥 Update in-memory cache with the latest count from Redis
                total_count = self.redis_manager.get(f"visits:{page_id}") or 0
                self.visit_cache[page_id] = (total_count, time.time())  # ✅ Sync with Redis

            self.visit_buffer.clear()  # ✅ Clear buffer after successful flush
        except Exception as e:
            print(f"Error flushing visit buffer to Redis: {e}")  # ✅ Log errors properly
        
    async def periodic_flush(self):
        """✅ Background task to flush buffer to Redis every 30 seconds."""
        while True:
            await asyncio.sleep(self.FLUSH_INTERVAL)
            await self.flush_to_redis()


# import time
# import asyncio
# from typing import Dict, Tuple, Optional
# from ..core.redis_manager import RedisManager

# class VisitCounterService:
#     """Service to manage visit counts with write batching, application cache, and Redis."""
    
#     CACHE_TTL = 300  # 5 minutes cache TTL
#     FLUSH_INTERVAL = 30  # 30 seconds between flush operations
    
#     def __init__(self):
#         """Initialize service components."""
#         self.redis_manager = RedisManager()
#         self.visit_cache: Dict[str, Tuple[int, float]] = {}  # {page_id: (count, timestamp)}
#         self.visit_buffer: Dict[str, int] = {}  # {page_id: pending_count}
#         self.flush_task: Optional[asyncio.Task] = None
#         self.lock = asyncio.Lock()  # Protect concurrent access to buffer and cache
    
#     async def start(self):
#         """Start the background flush task."""
#         if not self.flush_task or self.flush_task.done():
#             self.flush_task = asyncio.create_task(self._periodic_flush())
    
#     async def stop(self):
#         """Stop the service and ensure final flush."""
#         if self.flush_task and not self.flush_task.done():
#             self.flush_task.cancel()
#             await self.flush_to_redis()  # Final flush before stopping
    
#     async def increment_visit(self, page_id: str) -> None:
#         """Increment visit count in buffer and update cache."""
#         async with self.lock:
#             # Update buffer
#             self.visit_buffer[page_id] = self.visit_buffer.get(page_id, 0) + 1
            
#             # Update cache if it exists
#             if page_id in self.visit_cache:
#                 count, _ = self.visit_cache[page_id]
#                 self.visit_cache[page_id] = (count + 1, time.time())
        
#         # Ensure background task is running
#         await self.start()

#     async def _get_redis_count(self, page_id: str) -> int:
#         """Get the current count from Redis without triggering a flush."""
#         try:
#             redis_count = await self.redis_manager.get(f"visits:{page_id}")
#             return int(redis_count) if redis_count is not None else 0
#         except Exception as e:
#             print(f"Error fetching from Redis: {e}")
#             return 0
    
#     async def get_visit_count(self, page_id: str) -> int:
#         """Get total visit count combining Redis and buffered counts."""
#         async with self.lock:
#             # Get buffer count first
#             buffer_count = self.visit_buffer.get(page_id, 0)
            
#             # Check if cache is valid
#             if page_id in self.visit_cache:
#                 count, timestamp = self.visit_cache[page_id]
#                 if time.time() - timestamp < self.CACHE_TTL:
#                     return count + buffer_count
            
#             # Cache miss or expired - get fresh count from Redis
#             redis_count = await self._get_redis_count(page_id)
            
#             # Update cache with Redis count
#             self.visit_cache[page_id] = (redis_count, time.time())
            
#             # Return total count
#             return redis_count + buffer_count
    
#     async def flush_to_redis(self) -> None:
#         """Flush buffered counts to Redis."""
#         async with self.lock:
#             if not self.visit_buffer:
#                 return
            
#             # Create local copy of buffer and clear it
#             buffer_to_flush = self.visit_buffer.copy()
#             self.visit_buffer.clear()
        
#         try:
#             # Use pipeline for atomic updates
#             pipeline = self.redis_manager.pipeline()
#             for page_id, count in buffer_to_flush.items():
#                 pipeline.incrby(f"visits:{page_id}", count)
            
#             # Execute pipeline and update cache with new counts
#             results = await pipeline.execute()
            
#             async with self.lock:
#                 for page_id, new_total in zip(buffer_to_flush.keys(), results):
#                     self.visit_cache[page_id] = (int(new_total), time.time())
                    
#         except Exception as e:
#             print(f"Error flushing to Redis: {e}")
#             # Restore buffer on failure
#             async with self.lock:
#                 for page_id, count in buffer_to_flush.items():
#                     self.visit_buffer[page_id] = (
#                         self.visit_buffer.get(page_id, 0) + count
#                     )
    
#     async def _periodic_flush(self) -> None:
#         """Background task to periodically flush buffer to Redis."""
#         try:
#             while True:
#                 await asyncio.sleep(self.FLUSH_INTERVAL)
#                 await self.flush_to_redis()
#         except asyncio.CancelledError:
#             # Ensure final flush on cancellation
#             await self.flush_to_redis()