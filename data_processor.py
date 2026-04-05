# -*- coding: utf-8 -*-
"""
数据处理模块 (DataProcessor)
核心数据清洗、统计、异常分类逻辑
"""

import re
from typing import List, Dict, Any, Tuple, Optional
from collections import Counter
from dataclasses import dataclass
from config import get_config
from validator import LogValidator


@dataclass
class ProcessResult:
    """处理结果数据类"""
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


class DataProcessor:
    """
    数据处理器类
    使用PascalCase命名法
    """
    
    def __init__(self):
        self._config = get_config()
        self._validator = LogValidator()
    
    def process_lines(self, lines: List[str]) -> ProcessResult:
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
        
        keywords_to_count = self._config.get_keyword_stats()
        
        for line in lines:
            validation_result = self._validator.validate_line(line)
            
            if not validation_result.is_valid:
                invalid_lines += 1
                continue
            
            valid_lines += 1
            
            classification = self._validator.classify_log(line)
            
            level = classification.get("level")
            if level:
                level_stats[level] += 1
            
            category = classification.get("category", "general")
            category_stats[category] += 1
            
            is_exception = classification.get("is_exception", False)
            if is_exception:
                exception_lines += 1
                exception_logs.append({
                    "line": line,
                    "level": classification.get("exception_type"),
                    "category": category,
                    "timestamp": validation_result.extracted_data.get("timestamp"),
                    "ip_address": validation_result.extracted_data.get("ip_address")
                })
            
            line_lower = line.lower()
            for keyword in keywords_to_count:
                if keyword.lower() in line_lower:
                    keyword_stats[keyword] += 1
            
            timestamp = validation_result.extracted_data.get("timestamp")
            if timestamp:
                hour = self._extract_hour(timestamp)
                if hour is not None:
                    time_distribution[hour] += 1
            
            ip = validation_result.extracted_data.get("ip_address")
            if ip:
                ip_stats[ip] += 1
        
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
            exception_logs=exception_logs
        )
    
    def _extract_hour(self, timestamp: str) -> Optional[str]:
        try:
            match = re.search(r'(\d{2}):\d{2}:\d{2}', timestamp)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def generate_statistics(self, process_result: ProcessResult) -> Dict[str, Any]:
        top_n = self._config.get_top_n_limit()
        
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
        sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:n]
    
    def _summarize_exceptions(self, exception_logs: List[Dict[str, Any]]) -> Dict[str, int]:
        type_counter = Counter()
        for log in exception_logs:
            level = log.get("level", "UNKNOWN")
            type_counter[level] += 1
        return dict(type_counter)


_processor_instance: Optional[DataProcessor] = None


def get_data_processor() -> DataProcessor:
    global _processor_instance
    if _processor_instance is None:
        _processor_instance = DataProcessor()
    return _processor_instance
