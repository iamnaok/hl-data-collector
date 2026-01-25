"""
Data Validation Module

Provides sanity checks for liquidation data to catch
anomalies and corrupted data before it affects trading decisions.
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger("hl-collector.validation")


@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    warnings: List[str]
    errors: List[str]
    
    def __bool__(self):
        return self.is_valid


class DataValidator:
    """
    Validates liquidation and market data for sanity
    """
    
    # Price sanity bounds (based on typical crypto ranges)
    PRICE_BOUNDS = {
        "BTC": (10_000, 500_000),
        "ETH": (500, 50_000),
        "SOL": (5, 1_000),
        "DOGE": (0.01, 5),
        "ARB": (0.1, 50),
        "OP": (0.1, 50),
        "AVAX": (5, 500),
        "LINK": (1, 500),
        "SUI": (0.1, 50),
        "APT": (1, 100),
        "INJ": (1, 200),
        "TIA": (1, 100),
        "SEI": (0.01, 10),
        "WLD": (0.1, 50),
    }
    
    # Default bounds for unknown assets
    DEFAULT_PRICE_BOUNDS = (0.0001, 1_000_000)
    
    # Liquidation cluster sanity bounds
    MIN_CLUSTER_SIZE_USD = 10_000  # Clusters below this are suspicious
    MAX_CLUSTER_SIZE_USD = 10_000_000_000  # $10B is unrealistic
    
    # Position sanity bounds
    MIN_POSITION_SIZE_USD = 100
    MAX_POSITION_SIZE_USD = 1_000_000_000  # $1B
    
    # Leverage bounds
    MIN_LEVERAGE = 1
    MAX_LEVERAGE = 200
    
    def validate_price(self, coin: str, price: float) -> ValidationResult:
        """Validate a price value"""
        errors = []
        warnings = []
        
        if price is None:
            errors.append(f"{coin}: Price is None")
            return ValidationResult(False, warnings, errors)
        
        if price <= 0:
            errors.append(f"{coin}: Invalid price {price} (must be positive)")
            return ValidationResult(False, warnings, errors)
        
        bounds = self.PRICE_BOUNDS.get(coin, self.DEFAULT_PRICE_BOUNDS)
        min_price, max_price = bounds
        
        if price < min_price:
            warnings.append(f"{coin}: Price ${price:,.4f} below expected minimum ${min_price:,.4f}")
        
        if price > max_price:
            warnings.append(f"{coin}: Price ${price:,.4f} above expected maximum ${max_price:,.4f}")
        
        return ValidationResult(True, warnings, errors)
    
    def validate_liquidation_cluster(
        self,
        coin: str,
        price_level: float,
        size_usd: float,
        current_price: float
    ) -> ValidationResult:
        """Validate a liquidation cluster"""
        errors = []
        warnings = []
        
        # Check size
        if size_usd < self.MIN_CLUSTER_SIZE_USD:
            warnings.append(
                f"{coin}: Cluster size ${size_usd:,.0f} below minimum ${self.MIN_CLUSTER_SIZE_USD:,.0f}"
            )
        
        if size_usd > self.MAX_CLUSTER_SIZE_USD:
            errors.append(
                f"{coin}: Cluster size ${size_usd:,.0f} exceeds maximum ${self.MAX_CLUSTER_SIZE_USD:,.0f}"
            )
            return ValidationResult(False, warnings, errors)
        
        # Check price level relative to current price
        if current_price > 0:
            distance_pct = abs(price_level - current_price) / current_price * 100
            
            if distance_pct > 50:
                warnings.append(
                    f"{coin}: Cluster at ${price_level:,.2f} is {distance_pct:.1f}% from current price"
                )
            
            if distance_pct > 100:
                errors.append(
                    f"{coin}: Cluster at ${price_level:,.2f} is unrealistically far ({distance_pct:.1f}%) from current"
                )
                return ValidationResult(False, warnings, errors)
        
        return ValidationResult(len(errors) == 0, warnings, errors)
    
    def validate_position(
        self,
        wallet: str,
        coin: str,
        size_usd: float,
        leverage: float,
        liquidation_price: float,
        current_price: float
    ) -> ValidationResult:
        """Validate a position"""
        errors = []
        warnings = []
        
        # Size validation
        if size_usd < self.MIN_POSITION_SIZE_USD:
            warnings.append(f"Position size ${size_usd:,.0f} below tracking threshold")
        
        if size_usd > self.MAX_POSITION_SIZE_USD:
            errors.append(f"Position size ${size_usd:,.0f} exceeds realistic maximum")
            return ValidationResult(False, warnings, errors)
        
        # Leverage validation
        if leverage < self.MIN_LEVERAGE or leverage > self.MAX_LEVERAGE:
            errors.append(f"Invalid leverage {leverage}x (expected {self.MIN_LEVERAGE}-{self.MAX_LEVERAGE}x)")
            return ValidationResult(False, warnings, errors)
        
        # Liquidation price validation
        if liquidation_price <= 0:
            errors.append(f"Invalid liquidation price: {liquidation_price}")
            return ValidationResult(False, warnings, errors)
        
        # Check liquidation price relative to current
        if current_price > 0:
            distance_pct = abs(liquidation_price - current_price) / current_price * 100
            
            # Very close liquidations are suspicious (< 0.1%)
            if distance_pct < 0.1:
                warnings.append(f"Liquidation very close to current price ({distance_pct:.2f}%)")
            
            # Very far liquidations are also suspicious (> 90%)
            if distance_pct > 90:
                warnings.append(f"Liquidation very far from current price ({distance_pct:.1f}%)")
        
        return ValidationResult(len(errors) == 0, warnings, errors)
    
    def validate_liquidation_map(
        self,
        coin: str,
        liq_map: Dict,
        current_price: float
    ) -> ValidationResult:
        """Validate an entire liquidation map for a coin"""
        errors = []
        warnings = []
        
        if not liq_map:
            warnings.append(f"{coin}: Empty liquidation map")
            return ValidationResult(True, warnings, errors)
        
        # Validate structure
        required_keys = ["long_liquidations", "short_liquidations"]
        for key in required_keys:
            if key not in liq_map:
                errors.append(f"{coin}: Missing '{key}' in liquidation map")
        
        if errors:
            return ValidationResult(False, warnings, errors)
        
        # Validate long liquidations (should be BELOW current price)
        total_long_size = 0
        for cluster in liq_map.get("long_liquidations", []):
            price_level = cluster.get("price_center", 0)
            size = cluster.get("total_size_usd", 0)
            total_long_size += size
            
            if price_level > current_price:
                warnings.append(
                    f"{coin}: Long liquidation at ${price_level:,.2f} is ABOVE current ${current_price:,.2f}"
                )
        
        # Validate short liquidations (should be ABOVE current price)
        total_short_size = 0
        for cluster in liq_map.get("short_liquidations", []):
            price_level = cluster.get("price_center", 0)
            size = cluster.get("total_size_usd", 0)
            total_short_size += size
            
            if price_level < current_price:
                warnings.append(
                    f"{coin}: Short liquidation at ${price_level:,.2f} is BELOW current ${current_price:,.2f}"
                )
        
        # Check for extreme imbalance
        if total_long_size > 0 and total_short_size > 0:
            ratio = max(total_long_size, total_short_size) / min(total_long_size, total_short_size)
            if ratio > 100:
                warnings.append(
                    f"{coin}: Extreme long/short imbalance ({ratio:.0f}x)"
                )
        
        return ValidationResult(len(errors) == 0, warnings, errors)
    
    def validate_market_data(self, coin: str, data: Dict) -> ValidationResult:
        """Validate market data for a coin"""
        errors = []
        warnings = []
        
        # Required fields
        required_fields = ["mark_price", "funding_rate"]
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"{coin}: Missing required field '{field}'")
        
        if errors:
            return ValidationResult(False, warnings, errors)
        
        # Validate price
        price_result = self.validate_price(coin, data.get("mark_price", 0))
        errors.extend(price_result.errors)
        warnings.extend(price_result.warnings)
        
        # Validate funding rate (typically -0.1% to +0.1% per hour)
        funding = data.get("funding_rate", 0)
        if abs(funding) > 0.01:  # 1% per hour is extreme
            warnings.append(f"{coin}: Extreme funding rate {funding*100:.4f}%/hr")
        
        # Validate open interest if present
        oi = data.get("open_interest_usd", 0)
        if oi is not None and oi < 0:
            errors.append(f"{coin}: Invalid open interest {oi}")
        
        # Validate volume if present
        volume = data.get("volume_24h", 0)
        if volume is not None and volume < 0:
            errors.append(f"{coin}: Invalid volume {volume}")
        
        return ValidationResult(len(errors) == 0, warnings, errors)


# Global validator instance
validator = DataValidator()


def validate_and_log(
    validation_func,
    *args,
    log_warnings: bool = True,
    **kwargs
) -> bool:
    """
    Helper to run validation and log results
    
    Returns:
        True if valid, False otherwise
    """
    result = validation_func(*args, **kwargs)
    
    for error in result.errors:
        logger.error(f"Validation error: {error}")
    
    if log_warnings:
        for warning in result.warnings:
            logger.warning(f"Validation warning: {warning}")
    
    return result.is_valid
