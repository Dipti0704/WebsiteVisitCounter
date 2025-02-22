from ..core.redis_manager import RedisManager
import time

class VisitCounterService:
    """Service to manage visit counts with an application cache and Redis."""
    
    CACHE_TTL = 5

    def __init__(self, redis_manager: RedisManager = None):
        """Initialize with a RedisManager instance and inmemory cache."""
        self.redis_manager =  RedisManager()  # Allow dependency injection
        self.visit_cache = {}  # { page_id: (visit_count, timestamp) }

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page in Redis.
        
        Args:
            page_id: Unique identifier for the page.
        """
        try:
            new_count = await self.redis_manager.increment(f"visits:{page_id}")  
            self.visit_cache[page_id] = (new_count, time.time())  
        except Exception as e:
            print(f"Error incrementing visit count for {page_id}: {e}")

    async def get_visit_count(self, page_id: str) -> dict:
        """
        Get visit count for a page from the cacher or Redis.
        
        Args:
            page_id: Unique identifier for the page.
            
        Returns:
            A dictionary containing the visit count and the source of the data.
        """
        # Check if cache contains the page_id and hasn't expired
        if page_id in self.visit_cache:
            visit_count, timestamp = self.visit_cache[page_id]
            if time.time() - timestamp < self.CACHE_TTL:
                return visit_count

        # Cache miss or expired: Fetch from Redis
        try:
            visit_count = await self.redis_manager.get(f"visits:{page_id}") 
            if visit_count is None:
                visit_count = 0  
            self.visit_cache[page_id] = (visit_count, time.time())  
            return visit_count
        except Exception as e:
            print(f"Error retrieving visit count for {page_id}: {e}")
            return 0

