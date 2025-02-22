import redis
from typing import Dict, List, Optional, Any
from .config import settings

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        
        for node in redis_nodes:
            pool = redis.ConnectionPool.from_url(
                url=node,
                password=settings.REDIS_PASSWORD,
                db=settings.REDIS_DB
            )
            self.connection_pools[node] = pool
            self.redis_clients[node] = redis.Redis(connection_pool=pool)

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        return next(iter(self.redis_clients.values()))

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        client = await self.get_connection(key)
        try:
            return client.incrby(key, amount)
        except redis.RedisError:
            return 0

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Value of the key or None if not found
        """
        client = await self.get_connection(key)
        try:
            value = client.get(key)
            return int(value) if value is not None else None
        except (redis.RedisError, ValueError):
            return None