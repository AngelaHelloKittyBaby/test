# -*- coding: utf-8 -*-
"""
文件写入模块 (FileWriter)
负责输出报告、JSON统计文件、异常日志归档
"""

import os
import json
from typing import Dict, Any, List, Tuple
from datetime import datetime
from config import get_config
from data_processor import ProcessResult


class FileWriter:
    """
    文件写入器类
    使用PascalCase命名法
    """
    
    def __init__(self):
        self._config = get_config()
        self._readonly_files = set(self._config.get_readonly_files())
    
    def is_readonly(self, file_path: str) -> bool:
        normalized_path = os.path.normpath(file_path)
        for readonly in self._readonly_files:
            if os.path.normpath(readonly) == normalized_path:
                return True
        return False
    
    def ensure_directory(self, file_path: str) -> bool:
        directory = os.path.dirname(file_path)
        if not directory:
            return True
        try:
            os.makedirs(directory, exist_ok=True)
            return True
        except Exception:
            return False
    
    def write_text(self, file_path: str, content: str, encoding: str = "utf-8") -> bool:
        if self.is_readonly(file_path):
            return False
        
        if not self.ensure_directory(file_path):
            return False
        
        try:
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            return True
        except Exception:
            return False
    
    def write_json(self, file_path: str, data: Dict[str, Any], encoding: str = "utf-8") -> bool:
        if self.is_readonly(file_path):
            return False
        
        if not self.ensure_directory(file_path):
            return False
        
        try:
            with open(file_path, 'w', encoding=encoding) as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False
    
    def generate_markdown_report(self, statistics: Dict[str, Any], 
                                  exception_logs: List[Dict[str, Any]],
                                  input_files: List[str],
                                  execution_time: float) -> str:
        lines = []
        
        lines.append("# 日志分析报告")
        lines.append("")
        lines.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        lines.append("## 执行摘要")
        lines.append("")
        lines.append(f"- **执行时间**: {execution_time:.2f} 秒")
        lines.append(f"- **输入文件数**: {len(input_files)}")
        lines.append("")
        
        summary = statistics.get("summary", {})
        lines.append("## 数据统计")
        lines.append("")
        lines.append(f"- **总行数**: {summary.get('total_lines', 0)}")
        lines.append(f"- **有效行数**: {summary.get('valid_lines', 0)}")
        lines.append(f"- **异常数量**: {summary.get('exception_lines', 0)}")
        lines.append(f"- **有效率**: {summary.get('valid_rate', 0):.2f}%")
        lines.append("")
        
        level_dist = statistics.get("level_distribution", {})
        if level_dist:
            lines.append("## 日志级别分布")
            lines.append("")
            lines.append("| 级别 | 数量 |")
            lines.append("|------|------|")
            for level, count in sorted(level_dist.items()):
                lines.append(f"| {level} | {count} |")
            lines.append("")
        
        category_dist = statistics.get("category_distribution", {})
        if category_dist:
            lines.append("## 日志分类分布")
            lines.append("")
            lines.append("| 分类 | 数量 |")
            lines.append("|------|------|")
            for category, count in sorted(category_dist.items()):
                lines.append(f"| {category} | {count} |")
            lines.append("")
        
        keyword_freq = statistics.get("keyword_frequency", {})
        if keyword_freq:
            lines.append("## 关键词频率")
            lines.append("")
            lines.append("| 关键词 | 出现次数 |")
            lines.append("|--------|----------|")
            for keyword, count in sorted(keyword_freq.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"| {keyword} | {count} |")
            lines.append("")
        
        top_ips = statistics.get("top_ip_addresses", [])
        if top_ips:
            lines.append("## Top IP地址")
            lines.append("")
            lines.append("| IP地址 | 访问次数 |")
            lines.append("|--------|----------|")
            for ip, count in top_ips:
                lines.append(f"| {ip} | {count} |")
            lines.append("")
        
        if exception_logs:
            lines.append("## 异常汇总")
            lines.append("")
            lines.append(f"共发现 **{len(exception_logs)}** 条异常日志")
            lines.append("")
            
            exception_summary = statistics.get("exception_summary", {})
            types = exception_summary.get("types", {})
            if types:
                lines.append("### 异常类型分布")
                lines.append("")
                lines.append("| 类型 | 数量 |")
                lines.append("|------|------|")
                for exc_type, count in sorted(types.items()):
                    lines.append(f"| {exc_type} | {count} |")
                lines.append("")
            
            lines.append("### 异常日志示例（前10条）")
            lines.append("")
            lines.append("```")
            for i, log in enumerate(exception_logs[:10], 1):
                lines.append(f"{i}. [{log.get('level', 'UNKNOWN')}] {log.get('line', '')[:100]}")
            lines.append("```")
            lines.append("")
        
        if input_files:
            lines.append("## 输入文件")
            lines.append("")
            for file_path in input_files:
                lines.append(f"- `{file_path}`")
            lines.append("")
        
        return "\n".join(lines)
    
    def generate_json_statistics(self, statistics: Dict[str, Any],
                                  exception_logs: List[Dict[str, Any]],
                                  input_files: List[str],
                                  execution_time: float) -> Dict[str, Any]:
        return {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "execution_time_seconds": execution_time
            },
            "summary": statistics.get("summary", {}),
            "statistics": statistics,
            "exceptions": [
                {
                    "line": log.get("line", ""),
                    "level": log.get("level"),
                    "category": log.get("category"),
                    "timestamp": log.get("timestamp"),
                    "ip_address": log.get("ip_address")
                }
                for log in exception_logs
            ],
            "input_files": input_files
        }
    
    def generate_exception_archive(self, exception_logs: List[Dict[str, Any]]) -> str:
        lines = []
        
        lines.append(f"# 异常日志归档")
        lines.append(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"# 异常总数: {len(exception_logs)}")
        lines.append("=" * 80)
        lines.append("")
        
        for i, log in enumerate(exception_logs, 1):
            lines.append(f"[{i}]")
            lines.append(f"Level: {log.get('level', 'UNKNOWN')}")
            lines.append(f"Category: {log.get('category', 'unknown')}")
            lines.append(f"Timestamp: {log.get('timestamp', 'N/A')}")
            lines.append(f"IP: {log.get('ip_address', 'N/A')}")
            lines.append(f"Content: {log.get('line', '')}")
            lines.append("-" * 80)
            lines.append("")
        
        return "\n".join(lines)
    
    def generate_output_filename(self, suffix: str = "", extension: str = ".txt") -> str:
        output_dir = self._config.get_output_dir()
        prefix = self._config.get_output_prefix()
        timestamp = datetime.now().strftime(self._config.get_timestamp_format())
        
        if suffix:
            filename = f"{prefix}{timestamp}_{suffix}{extension}"
        else:
            filename = f"{prefix}{timestamp}{extension}"
        
        return os.path.join(output_dir, filename)


_writer_instance = None


def get_file_writer() -> FileWriter:
    global _writer_instance
    if _writer_instance is None:
        _writer_instance = FileWriter()
    return _writer_instance
