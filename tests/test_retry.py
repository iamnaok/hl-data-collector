"""
Tests for Retry Logic
"""
import pytest
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.retry import retry_async, retry_sync, APIError, RateLimitError


class TestRetryAsync:
    """Tests for async retry decorator"""
    
    @pytest.mark.asyncio
    async def test_success_no_retry(self):
        """Successful function should not retry"""
        call_count = 0
        
        @retry_async(max_attempts=3)
        async def success_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await success_func()
        assert result == "success"
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Should retry on failure"""
        call_count = 0
        
        @retry_async(max_attempts=3, initial_delay=0.01)
        async def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
            return "success"
        
        result = await failing_func()
        assert result == "success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_max_attempts_exceeded(self):
        """Should raise after max attempts"""
        call_count = 0
        
        @retry_async(max_attempts=3, initial_delay=0.01)
        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")
        
        with pytest.raises(ConnectionError):
            await always_fails()
        
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_no_retry_on_non_retryable(self):
        """Should not retry on non-retryable exceptions"""
        call_count = 0
        
        @retry_async(max_attempts=3, exceptions=(ConnectionError,))
        async def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")
        
        with pytest.raises(ValueError):
            await raises_value_error()
        
        assert call_count == 1  # Should not retry


class TestRetrySync:
    """Tests for sync retry decorator"""
    
    def test_success_no_retry(self):
        """Successful function should not retry"""
        call_count = 0
        
        @retry_sync(max_attempts=3)
        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = success_func()
        assert result == "success"
        assert call_count == 1
    
    def test_retry_on_failure(self):
        """Should retry on failure"""
        call_count = 0
        
        @retry_sync(max_attempts=3, initial_delay=0.01)
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "success"
        
        result = failing_func()
        assert result == "success"
        assert call_count == 2


class TestCustomExceptions:
    """Tests for custom exception classes"""
    
    def test_api_error_with_status(self):
        """APIError should store status code"""
        error = APIError("Not found", status_code=404)
        assert error.status_code == 404
        assert "Not found" in str(error)
    
    def test_rate_limit_error(self):
        """RateLimitError should be retryable"""
        error = RateLimitError("Too many requests")
        assert isinstance(error, Exception)


# ============== Run Tests ==============

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
