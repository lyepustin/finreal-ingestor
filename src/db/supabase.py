from supabase import create_client
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class SupabaseClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize Supabase client"""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

        logger.debug(f"Initializing Supabase client with URL: {supabase_url}")
        logger.debug(f"API Key length: {len(supabase_key)} characters")
        
        if not supabase_key.startswith('eyJ'):
            logger.warning("The Supabase key doesn't start with 'eyJ'. This might not be the correct service role key.")

        self.client = create_client(supabase_url, supabase_key)
        logger.info("Supabase client initialized successfully")

    def get_client(self):
        """Get Supabase client instance"""
        return self.client 