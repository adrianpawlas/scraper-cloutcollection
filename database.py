"""
Database module for Supabase integration
"""
import json
from typing import Any, Optional
import requests


class SupabaseClient:
    """Simple Supabase client for database operations"""
    
    def __init__(self, url: str, anon_key: str):
        self.url = url.rstrip('/')
        self.anon_key = anon_key
        self.headers = {
            'apikey': anon_key,
            'Authorization': f'Bearer {anon_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
    
    def insert(self, table: str, data: dict) -> list[dict]:
        """Insert a single record or upsert"""
        endpoint = f'{self.url}/rest/v1/{table}'
        
        clean_data = self._convert_numpy_types(data)
        
        headers = self.headers.copy()
        headers['Prefer'] = 'resolution=merge-duplicates,return=representation'
        
        response = requests.post(
            endpoint,
            headers=headers,
            json=[clean_data]
        )
        
        if response.status_code not in (200, 201):
            print(f"Insert error: {response.status_code} - {response.text[:200]}")
            # Try regular insert as fallback
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=[clean_data]
            )
            if response.status_code not in (200, 201):
                if response.status_code == 409:
                    # Already exists - that's fine
                    return []
                print(f"Insert error: {response.status_code} - {response.text[:200]}")
                return []
        
        return response.json() if response.text else []
    
    def upsert(self, table: str, data: dict) -> list[dict]:
        """Upsert a record (insert or update on conflict)"""
        endpoint = f'{self.url}/rest/v1/{table}'
        
        clean_data = self._convert_numpy_types(data)
        
        # Use PUT for upsert with on_conflict
        headers = self.headers.copy()
        headers['Prefer'] = 'resolution=merge-duplicates'
        
        response = requests.put(
            endpoint,
            headers=headers,
            json=[clean_data]
        )
        
        if response.status_code not in (200, 201):
            print(f"Upsert error: {response.status_code} - {response.text}")
            
            # Try POST as fallback
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=[clean_data]
            )
        
        return response.json() if response.text else []
    
    def upsert_with_conflict(self, table: str, data: dict, conflict_keys: list[str]) -> list[dict]:
        """Upsert with specific conflict resolution"""
        endpoint = f'{self.url}/rest/v1/{table}'
        
        clean_data = self._convert_numpy_types(data)
        
        # Build the upsert query
        on_conflict = ','.join(conflict_keys)
        headers = self.headers.copy()
        headers['Prefer'] = f'resolution=merge-duplicates,return=representation'
        
        response = requests.post(
            endpoint,
            headers=headers,
            json=[clean_data]
        )
        
        if response.status_code not in (200, 201):
            print(f"Upsert error: {response.status_code} - {response.text}")
            raise Exception(f"Upsert failed: {response.text}")
        
        return response.json() if response.text else []
    
    def select(
        self, 
        table: str, 
        filters: Optional[dict] = None,
        columns: str = '*',
        limit: Optional[int] = None
    ) -> list[dict]:
        """Select records from table"""
        endpoint = f'{self.url}/rest/v1/{table}'
        
        params = {'select': columns}
        
        if filters:
            for key, value in filters.items():
                if isinstance(value, (list, tuple)):
                    params[f'{key}'] = f'eq.{",".join(str(v) for v in value)}'
                else:
                    params[f'{key}'] = f'eq.{value}'
        
        if limit:
            params['limit'] = limit
        
        response = requests.get(
            endpoint,
            headers=self.headers,
            params=params
        )
        
        if response.status_code != 200:
            print(f"Select error: {response.status_code} - {response.text}")
            return []
        
        return response.json() if response.text else []
    
    def delete(self, table: str, filters: dict) -> bool:
        """Delete records from table"""
        endpoint = f'{self.url}/rest/v1/{table}'
        
        params = {}
        for key, value in filters.items():
            params[key] = f'eq.{value}'
        
        response = requests.delete(
            endpoint,
            headers=self.headers,
            params=params
        )
        
        return response.status_code in (200, 204)
    
    def _convert_numpy_types(self, data: dict) -> dict:
        """Convert numpy/other types to JSON serializable format"""
        converted = {}
        
        for key, value in data.items():
            if value is None:
                converted[key] = None
            elif key in ('image_embedding', 'info_embedding'):
                # Handle embeddings specially for pgvector
                if value is None:
                    converted[key] = None
                elif isinstance(value, (list, tuple)):
                    # Convert to PostgreSQL vector string format
                    # Format: [val1,val2,...]
                    converted[key] = '[' + ','.join(str(float(v)) for v in value) + ']'
                elif hasattr(value, 'tolist'):
                    arr = value.tolist()
                    converted[key] = '[' + ','.join(str(float(v)) for v in arr) + ']'
                else:
                    converted[key] = str(value)
            elif isinstance(value, (list, tuple)):
                # Handle lists - they might contain embeddings
                converted[key] = [
                    self._item_to_native(v) for v in value
                ]
            elif hasattr(value, 'tolist'):
                # numpy array
                converted[key] = value.tolist()
            elif hasattr(value, 'item'):
                # numpy scalar
                converted[key] = value.item()
            elif isinstance(value, (int, float, str, bool)):
                converted[key] = value
            else:
                try:
                    json.dumps(value)
                    converted[key] = value
                except (TypeError, ValueError):
                    converted[key] = str(value)
        
        return converted
    
    def _item_to_native(self, item: Any) -> Any:
        """Convert single item to native Python type"""
        if item is None:
            return None
        elif hasattr(item, 'tolist'):
            return item.tolist()
        elif hasattr(item, 'item'):
            return item.item()
        elif isinstance(item, (int, float, str, bool)):
            return item
        else:
            try:
                return float(item)
            except (TypeError, ValueError):
                return str(item)


def create_supabase_client(url: str, anon_key: str) -> SupabaseClient:
    """Factory function to create Supabase client"""
    return SupabaseClient(url, anon_key)