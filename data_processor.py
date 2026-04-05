from dataclasses import dataclass, field
from typing import List, Dict, Any, Counter
from collections import Counter
from datetime import datetime
import re

from validator import get_validator, LogLevel


@dataclass
class ProcessingResult:
    total_lines: int = 0
    valid_lines: int = 0
    invalid_lines: int = 0
    level_distribution: Counter = field(default_factory=Counter)
    keyword_counts: Counter = field(default_factory=Counter)
    hour_distribution: Counter = field(default_factory=Counter)
    ip_distribution: Counter = field(default_factory=Counter)
    category_distribution: Counter = field(default_factory=Counter)
    error_logs: List[str] = field(default_factory=list)
    warning_logs: List[str] = field(default_factory=list)
    source_files: Dict[str, int] = field(default_factory=dict)


class DataProcessor:
    def __init__(self):
        self.validator = get_validator()
        self.keywords = [
            'auth', 'login', 'database', 'query', 'cache', 'api',
            'request', 'response', 'timeout', 'connection', 'memory'
        ]

    def _extract_hour(self, timestamp: str) -> str:
        try:
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            return f"{dt.hour:02d}:00"
        except Exception:
            return 'unknown'

    def _count_keywords(self, message: str) -> Dict[str, int]:
        message_lower = message.lower()
        counts = {}
        for keyword in self.keywords:
            if keyword in message_lower:
                counts[keyword] = 1
        return counts

    def process_lines(self, lines: List[str], source_file: str = None) -> ProcessingResult:
        result = ProcessingResult()
        
        if source_file:
            result.source_files[source_file] = 0

        for line in lines:
            result.total_lines += 1
            
            validation = self.validator.validate_line(line)
            
            if not validation.is_valid:
                result.invalid_lines += 1
                continue

            result.valid_lines += 1
            if source_file:
                result.source_files[source_file] += 1

            level = validation.level
            result.level_distribution[level] += 1

            hour = self._extract_hour(validation.timestamp)
            result.hour_distribution[hour] += 1

            if validation.ip:
                result.ip_distribution[validation.ip] += 1

            category = self.validator.classify_log(line)
            result.category_distribution[category] += 1

            keyword_hits = self._count_keywords(validation.message)
            for keyword, count in keyword_hits.items():
                result.keyword_counts[keyword] += count

            if level == LogLevel.ERROR.value:
                result.error_logs.append(line.strip())
            elif level == LogLevel.WARNING.value:
                result.warning_logs.append(line.strip())

        return result

    def merge_results(self, results: List[ProcessingResult]) -> ProcessingResult:
        merged = ProcessingResult()

        for result in results:
            merged.total_lines += result.total_lines
            merged.valid_lines += result.valid_lines
            merged.invalid_lines += result.invalid_lines
            merged.level_distribution.update(result.level_distribution)
            merged.keyword_counts.update(result.keyword_counts)
            merged.hour_distribution.update(result.hour_distribution)
            merged.ip_distribution.update(result.ip_distribution)
            merged.category_distribution.update(result.category_distribution)
            merged.error_logs.extend(result.error_logs)
            merged.warning_logs.extend(result.warning_logs)
            merged.source_files.update(result.source_files)

        return merged

    def generate_statistics(self, result: ProcessingResult) -> Dict[str, Any]:
        error_rate = (result.invalid_lines / result.total_lines * 100) if result.total_lines > 0 else 0

        return {
            'summary': {
                'total_lines': result.total_lines,
                'valid_lines': result.valid_lines,
                'invalid_lines': result.invalid_lines,
                'error_rate': round(error_rate, 2),
                'processing_time': datetime.now().isoformat()
            },
            'level_distribution': dict(result.level_distribution),
            'keyword_distribution': dict(result.keyword_counts.most_common(20)),
            'hour_distribution': dict(result.hour_distribution),
            'top_ips': dict(result.ip_distribution.most_common(10)),
            'category_distribution': dict(result.category_distribution),
            'source_files': result.source_files,
            'error_count': len(result.error_logs),
            'warning_count': len(result.warning_logs)
        }


def get_data_processor() -> DataProcessor:
    return DataProcessor()
