from ..core.redis_manager import RedisManager

class VisitCounterService:
    """Service to manage visit counts using Redis"""

    def __init__(self):
        """Initialize with a RedisManager instance"""
        self.redis_manager =  RedisManager()  # Allow dependency injection

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page in Redis.
        
        Args:
            page_id: Unique identifier for the page.
        """
        try:
            await self.redis_manager.increment(f"visits:{page_id}")  # ✅ Synchronous Redis
        except Exception as e:
            print(f"Error incrementing visit count for {page_id}: {e}")

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get visit count for a page from Redis.
        
        Args:
            page_id: Unique identifier for the page.
            
        Returns:
            Visit count (default 0 on failure).
        """
        try:
            count = await self.redis_manager.get(f"visits:{page_id}")
            return count if count is not None else 0  # ✅ Synchronous Redis
        except Exception as e:
            print(f"Error retrieving visit count for {page_id}: {e}")
            return 0
