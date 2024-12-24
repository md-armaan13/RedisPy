# datastore.py
import asyncio
import random
from typing import Optional, Dict

class DataStore:
    def __init__(self):
        self.store: Dict[str, str] = {}
        self.expiry: Dict[str, int] = {}
        self.lock = asyncio.Lock()

    async def set(self, key: str, value: str, px: Optional[int] = None):
        async with self.lock:
            self.store[key] = value
            if px is not None:
                self.expiry[key] = int(asyncio.get_event_loop().time() * 1000) + px
            elif key in self.expiry:
                del self.expiry[key]

    async def get(self, key: str) -> Optional[str]:
        async with self.lock:
            if key in self.expiry:
                if self.expiry[key] <= int(asyncio.get_event_loop().time() * 1000):
                    del self.store[key]
                    del self.expiry[key]
                    return None
            return self.store.get(key)

    async def remove_expired_keys(self):
        async with self.lock:
            current_time = int(asyncio.get_event_loop().time() * 1000)
            keys_with_ttl = list(self.expiry_store.keys())
            if keys_with_ttl:
                keys_to_check = random.sample(keys_with_ttl, min(20, len(keys_with_ttl)))
                for key in keys_to_check:
                    with self.expiry_store_lock:
                        if key in self.expiry_store and self.expiry_store[key] <= current_time:
                            with self.data_store_lock:
                                self.data_store.pop(key, None)
                            self.expiry_store.pop(key, None)
                            print(f"Expired key deleted: {key}")
