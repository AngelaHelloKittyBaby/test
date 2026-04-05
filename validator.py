# -*- coding: utf-8 -*-
"""
日志格式校验模块 (LogValidator)
负责日志格式校验、异常规则匹配、数据合法性检查
"""

import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from config_manager import get_config_manager
from logger_system import get_logger


class ValidationStatus(Enum):
    """
    校验状态枚举
    使用PascalCase命名法
    """
    VALID = "valid"
    INVALID = "invalid"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class ValidationResult:
    """
    校验结果数据类
    """
    is_valid: bool
    status: ValidationStatus
    message: str
    extracted_data: Dict[str, Any] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.extracted_data is None:
            self.extracted_data = {}
        if self.errors is None:
            self.errors = []


class LogValidator:
    """
    日志校验器类
    使用PascalCase命名法
    负责日志格式校验和数据提取
    """
    
    # 常用日志格式正则表达式
    LOG_PATTERNS = {
        "standard": r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(\w+)\s+(.*)$',
        "apache": r'^(\S+)\s+(\S+)\s+(\S+)\s+\[([^\]]+)\]\s+"([^"]+)"\s+(\d+)\s+(\S+)',
        "syslog": r'^(\w+\s+\d+\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+(.*)$',
        "simple": r'^(\d{2}:\d{2}:\d{2})\s+(\w+)\s+(.*)$'
    }
    
    def __init__(self):
        """
        初始化日志校验器
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._compiled_patterns: Dict[str, re.Pattern] = {}
        self._compile_patterns()
    
    def _compile_patterns(self) -> None:
        """
        编译正则表达式模式
        私有方法，使用snake_case命名法
        """
        # 编译内置模式
        for name, pattern in self.LOG_PATTERNS.items():
            try:
                self._compiled_patterns[name] = re.compile(pattern)
            except re.error as e:
                self._logger.warning(f"日志模式 '{name}' 编译失败: {str(e)}")
        
        # 编译配置中的自定义模式
        custom_pattern = self._config.get_validation_pattern()
        if custom_pattern:
            try:
                self._compiled_patterns["custom"] = re.compile(custom_pattern)
            except re.error as e:
                self._logger.warning(f"自定义日志模式编译失败: {str(e)}")
    
    def validate_line(self, log_line: str, strict: bool = False) -> ValidationResult:
        """
        校验单行日志格式
        
        参数:
            log_line: 日志行内容
            strict: 是否使用严格模式
        
        返回:
            ValidationResult对象
        """
        if not log_line or not log_line.strip():
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.INVALID,
                message="空日志行",
                errors=["日志行为空"]
            )
        
        log_line = log_line.strip()
        extracted_data = {}
        errors = []
        
        # 尝试匹配各种日志格式
        matched = False
        for pattern_name, pattern in self._compiled_patterns.items():
            match = pattern.match(log_line)
            if match:
                matched = True
                extracted_data["pattern_type"] = pattern_name
                extracted_data["groups"] = match.groups()
                break
        
        if not matched and strict:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.INVALID,
                message="无法匹配任何已知日志格式",
                errors=["格式不匹配"]
            )
        
        # 提取日志级别
        log_level = self._extract_log_level(log_line)
        if log_level:
            extracted_data["level"] = log_level
        
        # 提取时间戳
        timestamp = self._extract_timestamp(log_line)
        if timestamp:
            extracted_data["timestamp"] = timestamp
        
        # 提取IP地址
        ip_address = self._extract_ip_address(log_line)
        if ip_address:
            extracted_data["ip_address"] = ip_address
        
        # 提取用户ID
        user_id = self._extract_user_id(log_line)
        if user_id:
            extracted_data["user_id"] = user_id
        
        # 校验日志级别
        valid_levels = self._config.get_log_levels()
        if log_level and log_level.upper() not in valid_levels:
            errors.append(f"未知的日志级别: {log_level}")
        
        # 判断校验结果
        if errors:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.WARNING,
                message="日志格式存在警告",
                extracted_data=extracted_data,
                errors=errors
            )
        
        return ValidationResult(
            is_valid=True,
            status=ValidationStatus.VALID,
            message="日志格式有效",
            extracted_data=extracted_data
        )
    
    def _extract_log_level(self, log_line: str) -> Optional[str]:
        """
        从日志行中提取日志级别
        
        参数:
            log_line: 日志行内容
        
        返回:
            日志级别字符串或None
        """
        valid_levels = self._config.get_log_levels()
        
        # 按优先级顺序匹配
        for level in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]:
            if level in log_line.upper():
                return level
        
        # 使用正则匹配
        level_pattern = r'\b(' + '|'.join(valid_levels) + r')\b'
        match = re.search(level_pattern, log_line, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        return None
    
    def _extract_timestamp(self, log_line: str) -> Optional[str]:
        """
        从日志行中提取时间戳
        
        参数:
            log_line: 日志行内容
        
        返回:
            时间戳字符串或None
        """
        # 标准格式: 2024-01-15 14:30:25
        standard_match = re.search(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}', log_line)
        if standard_match:
            return standard_match.group(0)
        
        # Apache格式: 15/Jan/2024:14:30:25
        apache_match = re.search(r'\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}', log_line)
        if apache_match:
            return apache_match.group(0)
        
        # 简单格式: 14:30:25
        simple_match = re.search(r'\d{2}:\d{2}:\d{2}', log_line)
        if simple_match:
            return simple_match.group(0)
        
        return None
    
    def _extract_ip_address(self, log_line: str) -> Optional[str]:
        """
        从日志行中提取IP地址
        
        参数:
            log_line: 日志行内容
        
        返回:
            IP地址字符串或None
        """
        ip_pattern = self._config.get_ip_pattern()
        match = re.search(ip_pattern, log_line)
        if match:
            ip = match.group(0)
            # 简单校验IP格式
            if self._is_valid_ip(ip):
                return ip
        return None
    
    def _is_valid_ip(self, ip: str) -> bool:
        """
        校验IP地址格式是否有效
        
        参数:
            ip: IP地址字符串
        
        返回:
            是否有效
        """
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
    
    def _extract_user_id(self, log_line: str) -> Optional[str]:
        """
        从日志行中提取用户ID
        
        参数:
            log_line: 日志行内容
        
        返回:
            用户ID字符串或None
        """
        user_id_pattern = self._config.get_user_id_pattern()
        match = re.search(user_id_pattern, log_line, re.IGNORECASE)
        if match:
            return match.group(1) if match.groups() else match.group(0)
        return None
    
    def is_exception_log(self, log_line: str) -> Tuple[bool, Optional[str]]:
        """
        判断是否为异常日志
        
        参数:
            log_line: 日志行内容
        
        返回:
            (是否为异常, 异常级别)
        """
        exception_levels = self._config.get_exception_levels()
        
        for level in exception_levels:
            if level in log_line.upper():
                return True, level
        
        # 检查异常关键词
        exception_keywords = ["exception", "traceback", "failed", "failure", "fatal"]
        for keyword in exception_keywords:
            if keyword in log_line.lower():
                return True, "ERROR"
        
        return False, None
    
    def classify_log(self, log_line: str) -> Dict[str, Any]:
        """
        对日志进行分类
        
        参数:
            log_line: 日志行内容
        
        返回:
            分类结果字典
        """
        result = {
            "line": log_line,
            "level": None,
            "is_exception": False,
            "exception_type": None,
            "category": "unknown"
        }
        
        # 提取日志级别
        level = self._extract_log_level(log_line)
        if level:
            result["level"] = level
        
        # 检查是否为异常
        is_exception, exception_level = self.is_exception_log(log_line)
        if is_exception:
            result["is_exception"] = True
            result["exception_type"] = exception_level
        
        # 分类
        if "login" in log_line.lower():
            result["category"] = "authentication"
        elif "request" in log_line.lower() or "response" in log_line.lower():
            result["category"] = "http"
        elif "database" in log_line.lower() or "db" in log_line.lower():
            result["category"] = "database"
        elif "cache" in log_line.lower():
            result["category"] = "cache"
        elif result["is_exception"]:
            result["category"] = "exception"
        else:
            result["category"] = "general"
        
        return result
    
    def batch_validate(self, log_lines: List[str], strict: bool = False) -> List[ValidationResult]:
        """
        批量校验日志行
        
        参数:
            log_lines: 日志行列表
            strict: 是否使用严格模式
        
        返回:
            校验结果列表
        """
        results = []
        for line in log_lines:
            result = self.validate_line(line, strict)
            results.append(result)
        return results
    
    def get_validation_stats(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """
        获取校验统计信息
        
        参数:
            results: 校验结果列表
        
        返回:
            统计信息字典
        """
        total = len(results)
        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = total - valid_count
        
        status_counts = {}
        for result in results:
            status = result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "total": total,
            "valid": valid_count,
            "invalid": invalid_count,
            "valid_rate": (valid_count / total * 100) if total > 0 else 0,
            "status_distribution": status_counts
        }
