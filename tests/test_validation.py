"""
Tests for Data Validation Module
"""
import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.validation import DataValidator, ValidationResult


class TestDataValidator:
    """Tests for DataValidator class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.validator = DataValidator()
    
    # ============== Price Validation Tests ==============
    
    def test_validate_price_valid_btc(self):
        """Valid BTC price should pass"""
        result = self.validator.validate_price("BTC", 89000)
        assert result.is_valid
        assert len(result.errors) == 0
    
    def test_validate_price_zero(self):
        """Zero price should fail"""
        result = self.validator.validate_price("BTC", 0)
        assert not result.is_valid
        assert len(result.errors) > 0
    
    def test_validate_price_negative(self):
        """Negative price should fail"""
        result = self.validator.validate_price("ETH", -100)
        assert not result.is_valid
    
    def test_validate_price_none(self):
        """None price should fail"""
        result = self.validator.validate_price("SOL", None)
        assert not result.is_valid
    
    def test_validate_price_outside_bounds_warning(self):
        """Price outside expected bounds should warn"""
        # BTC at $1 million is outside bounds
        result = self.validator.validate_price("BTC", 1_000_000)
        assert result.is_valid  # Still valid, just warns
        assert len(result.warnings) > 0
    
    # ============== Liquidation Cluster Validation Tests ==============
    
    def test_validate_cluster_valid(self):
        """Valid cluster should pass"""
        result = self.validator.validate_liquidation_cluster(
            coin="BTC",
            price_level=88000,
            size_usd=5_000_000,
            current_price=90000
        )
        assert result.is_valid
    
    def test_validate_cluster_tiny_size_warning(self):
        """Very small cluster should warn"""
        result = self.validator.validate_liquidation_cluster(
            coin="BTC",
            price_level=88000,
            size_usd=5_000,  # Below MIN_CLUSTER_SIZE_USD
            current_price=90000
        )
        assert result.is_valid  # Valid but warns
        assert len(result.warnings) > 0
    
    def test_validate_cluster_unrealistic_size(self):
        """Unrealistically large cluster should fail"""
        result = self.validator.validate_liquidation_cluster(
            coin="BTC",
            price_level=88000,
            size_usd=100_000_000_000,  # $100B is unrealistic
            current_price=90000
        )
        assert not result.is_valid
    
    def test_validate_cluster_far_from_price(self):
        """Cluster very far from current price should fail"""
        result = self.validator.validate_liquidation_cluster(
            coin="BTC",
            price_level=10000,  # Way below current
            size_usd=5_000_000,
            current_price=90000  # 89% away
        )
        # More than 100% away should fail
        # This is ~89% away, so should warn but pass
        assert len(result.warnings) > 0
    
    # ============== Position Validation Tests ==============
    
    def test_validate_position_valid(self):
        """Valid position should pass"""
        result = self.validator.validate_position(
            wallet="0x123...",
            coin="BTC",
            size_usd=100_000,
            leverage=10,
            liquidation_price=85000,
            current_price=90000
        )
        assert result.is_valid
    
    def test_validate_position_invalid_leverage(self):
        """Invalid leverage should fail"""
        result = self.validator.validate_position(
            wallet="0x123...",
            coin="BTC",
            size_usd=100_000,
            leverage=500,  # Too high
            liquidation_price=85000,
            current_price=90000
        )
        assert not result.is_valid
    
    def test_validate_position_invalid_liq_price(self):
        """Invalid liquidation price should fail"""
        result = self.validator.validate_position(
            wallet="0x123...",
            coin="BTC",
            size_usd=100_000,
            leverage=10,
            liquidation_price=-1000,  # Invalid
            current_price=90000
        )
        assert not result.is_valid
    
    # ============== Market Data Validation Tests ==============
    
    def test_validate_market_data_valid(self):
        """Valid market data should pass"""
        result = self.validator.validate_market_data("BTC", {
            "mark_price": 89000,
            "funding_rate": 0.0001,
            "open_interest_usd": 1_000_000_000,
            "volume_24h": 500_000_000
        })
        assert result.is_valid
    
    def test_validate_market_data_missing_price(self):
        """Missing price should fail"""
        result = self.validator.validate_market_data("BTC", {
            "funding_rate": 0.0001
        })
        assert not result.is_valid
    
    def test_validate_market_data_extreme_funding(self):
        """Extreme funding rate should warn"""
        result = self.validator.validate_market_data("BTC", {
            "mark_price": 89000,
            "funding_rate": 0.05,  # 5% per hour is extreme
        })
        assert result.is_valid  # Valid but warns
        assert len(result.warnings) > 0
    
    # ============== Liquidation Map Validation Tests ==============
    
    def test_validate_liq_map_valid(self):
        """Valid liquidation map should pass"""
        liq_map = {
            "long_liquidations": [
                {"price_center": 85000, "total_size_usd": 5_000_000}
            ],
            "short_liquidations": [
                {"price_center": 95000, "total_size_usd": 3_000_000}
            ]
        }
        result = self.validator.validate_liquidation_map("BTC", liq_map, 90000)
        assert result.is_valid
    
    def test_validate_liq_map_missing_key(self):
        """Missing required key should fail"""
        liq_map = {
            "long_liquidations": []
            # Missing short_liquidations
        }
        result = self.validator.validate_liquidation_map("BTC", liq_map, 90000)
        assert not result.is_valid
    
    def test_validate_liq_map_long_above_price_warning(self):
        """Long liquidation above price should warn"""
        liq_map = {
            "long_liquidations": [
                {"price_center": 95000, "total_size_usd": 5_000_000}  # Above current!
            ],
            "short_liquidations": []
        }
        result = self.validator.validate_liquidation_map("BTC", liq_map, 90000)
        assert result.is_valid  # Valid but warns
        assert len(result.warnings) > 0


# ============== Run Tests ==============

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
