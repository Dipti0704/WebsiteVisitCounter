from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager


class VisitCounterService:
    """Service to manage visit counts without using Redis"""
    
    visit_counts: Dict[str, int] = {}
    locks_per_page: Dict[str, asyncio.Lock] = {}  
     
    async def get_lock(self, page_id: str) -> asyncio.Lock:
        """Retrieve or create a lock for a specific page_id."""
        if page_id not in self.locks_per_page:  
            self.locks_per_page[page_id] = asyncio.Lock()  
        return self.locks_per_page[page_id]

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page.
        
        Args:
            page_id: Unique identifier for the page
        """
        lock = await self.get_lock(page_id)  
        async with lock:  
            if page_id in self.visit_counts:
                self.visit_counts[page_id] += 1
            else:
                self.visit_counts[page_id] = 1

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page.
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count (default 0 if not found)
        """
        lock = await self.get_lock(page_id)  
        async with lock:  
            return self.visit_counts.get(page_id, 0)
        