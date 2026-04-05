import os
import json
from typing import Dict, Any, List
from datetime import datetime

from config import get_config


class FileWriter:
    def __init__(self):
        self.config = get_config()
        self.output_dir = self.config.get_output_dir()
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except Exception:
            pass

    def _generate_filename(self, suffix: str, extension: str) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"log_report_{suffix}_{timestamp}.{extension}"

    def write_json_report(self, statistics: Dict[str, Any]) -> str:
        filename = self._generate_filename('statistics', 'json')
        file_path = os.path.join(self.output_dir, filename)

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(statistics, f, ensure_ascii=False, indent=2)
            return file_path
        except Exception as e:
            return f"Error writing JSON report: {str(e)}"

    def write_markdown_report(self, statistics: Dict[str, Any]) -> str:
        filename = self._generate_filename('analysis', 'md')
        file_path = os.path.join(self.output_dir, filename)

        try:
            content = self._generate_markdown_content(statistics)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return file_path
        except Exception as e:
            return f"Error writing Markdown report: {str(e)}"

    def _generate_markdown_content(self, stats: Dict[str, Any]) -> str:
        summary = stats['summary']

        content = f"""# 日志分析报告

## 生成时间
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 处理概览

| 指标 | 数值 |
|------|------|
| 总行数 | {summary['total_lines']:,} |
| 有效行数 | {summary['valid_lines']:,} |
| 无效行数 | {summary['invalid_lines']:,} |
| 错误率 | {summary['error_rate']}% |

## 日志级别分布

| 级别 | 数量 | 占比 |
|------|------|------|
"""

        total_valid = summary['valid_lines']
        for level, count in stats['level_distribution'].items():
            percentage = round(count / total_valid * 100, 2) if total_valid > 0 else 0
            content += f"| {level.upper()} | {count:,} | {percentage}% |\n"

        content += """
## 关键词分布

| 关键词 | 出现次数 |
|--------|----------|
"""
        for keyword, count in stats['keyword_distribution'].items():
            content += f"| {keyword} | {count:,} |\n"

        content += """
## 访问时段分布

| 时段 | 访问量 |
|------|--------|
"""
        for hour in sorted(stats['hour_distribution'].keys()):
            content += f"| {hour} | {stats['hour_distribution'][hour]:,} |\n"

        content += """
## 访问IP TOP10

| IP地址 | 访问次数 |
|--------|----------|
"""
        for ip, count in stats['top_ips'].items():
            content += f"| {ip} | {count:,} |\n"

        content += """
## 日志分类统计

| 分类 | 数量 |
|------|------|
"""
        for category, count in stats['category_distribution'].items():
            content += f"| {category} | {count:,} |\n"

        content += f"""

## 异常统计

- **ERROR 数量**: {stats['error_count']:,}
- **WARNING 数量**: {stats['warning_count']:,}

## 源文件统计

| 文件 | 有效行数 |
|------|----------|
"""
        for file_name, count in stats['source_files'].items():
            content += f"| {file_name} | {count:,} |\n"

        return content

    def write_error_logs(self, error_logs: List[str], warning_logs: List[str]) -> str:
        filename = self._generate_filename('exceptions', 'log')
        file_path = os.path.join(self.output_dir, filename)

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("ERROR 异常日志\n")
                f.write("=" * 80 + "\n\n")
                for log in error_logs:
                    f.write(log + "\n")

                f.write("\n" + "=" * 80 + "\n")
                f.write("WARNING 警告日志\n")
                f.write("=" * 80 + "\n\n")
                for log in warning_logs:
                    f.write(log + "\n")

            return file_path
        except Exception as e:
            return f"Error writing exception logs: {str(e)}"


def get_file_writer() -> FileWriter:
    return FileWriter()
