# -*- coding: utf-8 -*-
"""
数据清洗与统计模块 (DataProcessor)
核心数据清洗、统计、异常分类逻辑
"""

import re
from typing import List, Dict, Any, Tuple, Counter, Optional
from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass

from config_manager import get_config_manager
from logger_system import get_logger
from validator import LogValidator, ValidationResult


@dataclass
class ProcessResult:
    """
    处理结果数据类
    """
    total_lines: int
    valid_lines: int
    invalid_lines: int
    exception_lines: int
    keyword_stats: Dict[str, int]
    level_stats: Dict[str, int]
    category_stats: Dict[str, int]
    time_distribution: Dict[str, int]
    ip_stats: Dict[str, int]
    exception_logs: List[Dict[str, Any]]
    processed_data: List[Dict[str, Any]]


class DataProcessor:
    """
    数据处理器类
    使用PascalCase命名法
    负责日志数据的清洗、统计和分析
    """
    
    def __init__(self):
        """
        初始化数据处理器
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._validator = LogValidator()
    
    def process_lines(self, lines: List[str]) -> ProcessResult:
        """
        处理日志行列表
        
        参数:
            lines: 日志行列表
        
        返回:
            ProcessResult对象
        """
        self._logger.info(f"开始处理 {len(lines)} 行日志数据")
        
        total_lines = len(lines)
        valid_lines = 0
        invalid_lines = 0
        exception_lines = 0
        
        keyword_stats = Counter()
        level_stats = Counter()
        category_stats = Counter()
        time_distribution = Counter()
        ip_stats = Counter()
        
        exception_logs = []
        processed_data = []
        
        # 获取配置
        keywords_to_count = self._config.get_keyword_stats()
        exception_levels = self._config.get_exception_levels()
        
        for i, line in enumerate(lines):
            # 进度报告
            if (i + 1) % 10000 == 0:
                self._logger.log_progress(i + 1, total_lines, "数据处理")
            
            # 校验日志
            validation_result = self._validator.validate_line(line)
            
            if not validation_result.is_valid:
                invalid_lines += 1
                continue
            
            valid_lines += 1
            
            # 分类日志
            classification = self._validator.classify_log(line)
            
            # 统计日志级别
            level = classification.get("level")
            if level:
                level_stats[level] += 1
            
            # 统计分类
            category = classification.get("category", "unknown")
            category_stats[category] += 1
            
            # 检查是否为异常
            is_exception = classification.get("is_exception", False)
            if is_exception:
                exception_lines += 1
                exception_logs.append({
                    "line": line,
                    "level": classification.get("exception_type"),
                    "category": category,
                    "timestamp": validation_result.extracted_data.get("timestamp"),
                    "ip_address": validation_result.extracted_data.get("ip_address"),
                    "user_id": validation_result.extracted_data.get("user_id")
                })
            
            # 关键词统计
            line_lower = line.lower()
            for keyword in keywords_to_count:
                if keyword.lower() in line_lower:
                    keyword_stats[keyword] += 1
            
            # 时间分布统计
            timestamp = validation_result.extracted_data.get("timestamp")
            if timestamp:
                hour = self._extract_hour(timestamp)
                if hour is not None:
                    time_distribution[hour] += 1
            
            # IP地址统计
            ip = validation_result.extracted_data.get("ip_address")
            if ip:
                ip_stats[ip] += 1
            
            # 保存处理后的数据
            processed_data.append({
                "line": line,
                "level": level,
                "category": category,
                "is_exception": is_exception,
                "timestamp": timestamp,
                "ip_address": ip,
                "user_id": validation_result.extracted_data.get("user_id"),
                "validation": validation_result
            })
        
        self._logger.info(f"数据处理完成: 有效 {valid_lines}, 无效 {invalid_lines}, 异常 {exception_lines}")
        
        return ProcessResult(
            total_lines=total_lines,
            valid_lines=valid_lines,
            invalid_lines=invalid_lines,
            exception_lines=exception_lines,
            keyword_stats=dict(keyword_stats),
            level_stats=dict(level_stats),
            category_stats=dict(category_stats),
            time_distribution=dict(time_distribution),
            ip_stats=dict(ip_stats),
            exception_logs=exception_logs,
            processed_data=processed_data
        )
    
    def _extract_hour(self, timestamp: str) -> Optional[str]:
        """
        从时间戳提取小时
        
        参数:
            timestamp: 时间戳字符串
        
        返回:
            小时字符串（如 "14"）或None
        """
        try:
            # 尝试匹配 HH:MM:SS 格式
            match = re.search(r'(\d{2}):\d{2}:\d{2}', timestamp)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def generate_statistics(self, process_result: ProcessResult) -> Dict[str, Any]:
        """
        生成统计报告
        
        参数:
            process_result: 处理结果
        
        返回:
            统计报告字典
        """
        top_n = self._config.get_top_n_limit()
        
        # 计算有效率和异常率
        valid_rate = (process_result.valid_lines / process_result.total_lines * 100) if process_result.total_lines > 0 else 0
        exception_rate = (process_result.exception_lines / process_result.valid_lines * 100) if process_result.valid_lines > 0 else 0
        
        stats = {
            "summary": {
                "total_lines": process_result.total_lines,
                "valid_lines": process_result.valid_lines,
                "invalid_lines": process_result.invalid_lines,
                "exception_lines": process_result.exception_lines,
                "valid_rate": round(valid_rate, 2),
                "exception_rate": round(exception_rate, 2)
            },
            "level_distribution": process_result.level_stats,
            "category_distribution": process_result.category_stats,
            "keyword_frequency": process_result.keyword_stats,
            "time_distribution": process_result.time_distribution,
            "top_ip_addresses": self._get_top_n(process_result.ip_stats, top_n),
            "exception_summary": {
                "count": len(process_result.exception_logs),
                "types": self._summarize_exceptions(process_result.exception_logs)
            }
        }
        
        return stats
    
    def _get_top_n(self, counter: Dict[str, int], n: int) -> List[Tuple[str, int]]:
        """
        获取Top N项
        
        参数:
            counter: 计数器字典
            n: 数量
        
        返回:
            Top N列表
        """
        sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:n]
    
    def _summarize_exceptions(self, exception_logs: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        汇总异常类型
        
        参数:
            exception_logs: 异常日志列表
        
        返回:
            异常类型统计
        """
        type_counter = Counter()
        for log in exception_logs:
            level = log.get("level", "UNKNOWN")
            type_counter[level] += 1
        return dict(type_counter)
    
    def analyze_patterns(self, lines: List[str]) -> Dict[str, Any]:
        """
        分析日志模式
        
        参数:
            lines: 日志行列表
        
        返回:
            模式分析结果
        """
        patterns = {
            "timestamp_formats": Counter(),
            "log_levels": Counter(),
            "common_prefixes": Counter()
        }
        
        for line in lines[:1000]:  # 只分析前1000行
            # 分析时间戳格式
            if re.search(r'\d{4}-\d{2}-\d{2}', line):
                patterns["timestamp_formats"]["YYYY-MM-DD"] += 1
            elif re.search(r'\d{2}/\w{3}/\d{4}', line):
                patterns["timestamp_formats"]["DD/Mon/YYYY"] += 1
            
            # 分析日志级别
            levels = ["ERROR", "WARN", "INFO", "DEBUG", "CRITICAL"]
            for level in levels:
                if level in line.upper():
                    patterns["log_levels"][level] += 1
                    break
        
        return {
            "timestamp_formats": dict(patterns["timestamp_formats"]),
            "log_levels": dict(patterns["log_levels"])
        }
    
    def extract_unique_values(self, processed_data: List[Dict[str, Any]], field: str) -> List[str]:
        """
        提取指定字段的唯一值
        
        参数:
            processed_data: 处理后的数据列表
            field: 字段名
        
        返回:
            唯一值列表
        """
        values = set()
        for item in processed_data:
            value = item.get(field)
            if value:
                values.add(str(value))
        return sorted(list(values))
    
    def filter_by_criteria(self, processed_data: List[Dict[str, Any]], 
                          criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        根据条件过滤数据
        
        参数:
            processed_data: 处理后的数据列表
            criteria: 过滤条件
        
        返回:
            过滤后的数据列表
        """
        filtered = processed_data
        
        # 按级别过滤
        if "level" in criteria:
            level = criteria["level"]
            filtered = [item for item in filtered if item.get("level") == level]
        
        # 按分类过滤
        if "category" in criteria:
            category = criteria["category"]
            filtered = [item for item in filtered if item.get("category") == category]
        
        # 按时间范围过滤
        if "time_range" in criteria:
            time_range = criteria["time_range"]
            # 实现时间范围过滤逻辑
            pass
        
        # 按IP过滤
        if "ip_address" in criteria:
            ip = criteria["ip_address"]
            filtered = [item for item in filtered if item.get("ip_address") == ip]
        
        return filtered


# 全局数据处理器实例
_processor_instance = None


def get_data_processor() -> DataProcessor:
    """
    获取全局数据处理器实例（单例模式）
    
    返回:
        DataProcessor实例
    """
    global _processor_instance
    if _processor_instance is None:
        _processor_instance = DataProcessor()
    return _processor_instance
