# -*- coding: utf-8 -*-
"""
校验模块 (Validator)
日志格式校验、异常规则匹配、数据合法性检查
"""

import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from config import get_config


class ValidationStatus(Enum):
    """校验状态枚举"""
    VALID = "valid"
    INVALID = "invalid"


@dataclass
class ValidationResult:
    """校验结果数据类"""
    is_valid: bool
    status: ValidationStatus
    message: str
    extracted_data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.extracted_data is None:
            self.extracted_data = {}


class LogValidator:
    """
    日志校验器类
    使用PascalCase命名法
    """
    
    LOG_PATTERN = re.compile(
        r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(\w+)\s+(.*)$'
    )
    
    def __init__(self):
        self._config = get_config()
    
    def validate_line(self, log_line: str) -> ValidationResult:
        if not log_line or not log_line.strip():
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.INVALID,
                message="空日志行"
            )
        
        log_line = log_line.strip()
        extracted_data = {}
        
        match = self.LOG_PATTERN.match(log_line)
        if match:
            extracted_data["timestamp"] = match.group(1)
            extracted_data["level"] = match.group(2)
            extracted_data["message"] = match.group(3)
        
        level = self._extract_log_level(log_line)
        if level:
            extracted_data["level"] = level
        
        ip = self._extract_ip_address(log_line)
        if ip:
            extracted_data["ip_address"] = ip
        
        return ValidationResult(
            is_valid=True,
            status=ValidationStatus.VALID,
            message="日志格式有效",
            extracted_data=extracted_data
        )
    
    def _extract_log_level(self, log_line: str) -> Optional[str]:
        valid_levels = self._config.get_log_levels()
        for level in valid_levels:
            if level in log_line.upper():
                return level
        return None
    
    def _extract_ip_address(self, log_line: str) -> Optional[str]:
        ip_pattern = self._config.get_ip_pattern()
        match = re.search(ip_pattern, log_line)
        if match:
            ip = match.group(0)
            if self._is_valid_ip(ip):
                return ip
        return None
    
    def _is_valid_ip(self, ip: str) -> bool:
        parts = ip.split('.')
        if len(parts) != 4:
            return False
        for part in parts:
            try:
                num = int(part)
                if num < 0 or num > 255:
                    return False
            except ValueError:
                return False
        return True
    
    def is_exception_log(self, log_line: str) -> Tuple[bool, Optional[str]]:
        exception_levels = self._config.get_exception_levels()
        for level in exception_levels:
            if level in log_line.upper():
                return True, level
        
        exception_keywords = ["exception", "traceback", "failed", "failure", "fatal"]
        for keyword in exception_keywords:
            if keyword in log_line.lower():
                return True, "ERROR"
        
        return False, None
    
    def should_skip(self, log_line: str) -> bool:
        skip_keywords = self._config.get_skip_keywords()
        for keyword in skip_keywords:
            if keyword.upper() in log_line.upper():
                return True
        
        min_len = self._config.get_min_line_length()
        max_len = self._config.get_max_line_length()
        line_len = len(log_line)
        
        if line_len < min_len or line_len > max_len:
            return True
        
        return False
    
    def classify_log(self, log_line: str) -> Dict[str, Any]:
        result = {
            "line": log_line,
            "level": None,
            "is_exception": False,
            "exception_type": None,
            "category": "general"
        }
        
        level = self._extract_log_level(log_line)
        if level:
            result["level"] = level
        
        is_exception, exception_level = self.is_exception_log(log_line)
        if is_exception:
            result["is_exception"] = True
            result["exception_type"] = exception_level
        
        line_lower = log_line.lower()
        if "login" in line_lower or "logout" in line_lower:
            result["category"] = "authentication"
        elif "request" in line_lower or "response" in line_lower:
            result["category"] = "http"
        elif "database" in line_lower or "db" in line_lower:
            result["category"] = "database"
        elif "cache" in line_lower:
            result["category"] = "cache"
        elif result["is_exception"]:
            result["category"] = "exception"
        
        return result
