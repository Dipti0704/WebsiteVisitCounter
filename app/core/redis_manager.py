import redis
from typing import Dict, Optional
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

    def get_connection(self, key: str) -> redis.Redis:
        """Get Redis connection"""
        return next(iter(self.redis_clients.values()))

    def increment(self, key: str, amount: int = 1) -> int:
        """Increment a counter in Redis"""
        client = self.get_connection(key)
        try:
            return client.incrby(key, amount)
        except redis.RedisError:
            return 0

    def get(self, key: str) -> Optional[int]:
        """Get value for a key from Redis"""
        client = self.get_connection(key)
        try:
            value = client.get(key)
            return int(value) if value is not None else None
        except (redis.RedisError, ValueError):
            return None
