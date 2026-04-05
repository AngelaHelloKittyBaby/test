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


class LogLevel(Enum):
    """日志级别枚举"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class ValidationStatus(Enum):
    """校验状态枚举"""
    VALID = "valid"
    INVALID = "invalid"


@dataclass
class ValidationResult:
    """校验结果数据类"""
    is_valid: bool
    status: ValidationStatus
    timestamp: str = ""
    level: str = ""
    message: str = ""
    ip: str = ""


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
                status=ValidationStatus.INVALID
            )
        
        log_line = log_line.strip()
        
        match = self.LOG_PATTERN.match(log_line)
        if not match:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.INVALID
            )
        
        timestamp = match.group(1)
        level = match.group(2).upper()
        message = match.group(3)
        ip = self._extract_ip_address(message)
        
        valid_levels = [level.value for level in LogLevel]
        if level not in valid_levels:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.INVALID
            )
        
        return ValidationResult(
            is_valid=True,
            status=ValidationStatus.VALID,
            timestamp=timestamp,
            level=level,
            message=message,
            ip=ip or ""
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
    
    def classify_log(self, log_line: str) -> str:
        line_lower = log_line.lower()
        if "login" in line_lower or "logout" in line_lower or "auth" in line_lower:
            return "authentication"
        elif "request" in line_lower or "response" in line_lower or "api" in line_lower:
            return "http"
        elif "database" in line_lower or "db" in line_lower or "query" in line_lower:
            return "database"
        elif "cache" in line_lower:
            return "cache"
        elif "error" in line_lower or "exception" in line_lower or "failed" in line_lower:
            return "exception"
        else:
            return "general"


def get_validator() -> LogValidator:
    return LogValidator()
