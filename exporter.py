# -*- coding: utf-8 -*-
"""
输出模块 (Exporter)
重构输出模块，支持多格式报告生成
"""

import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from abc import ABC, abstractmethod

from config_manager import get_config_manager
from logger_system import get_logger
from data_pipeline import PipelineResult


class BaseExporter(ABC):
    """
    导出器基类
    使用PascalCase命名法
    """
    
    def __init__(self, name: str):
        """
        初始化导出器
        
        参数:
            name: 导出器名称
        """
        self._name = name
        self._config = get_config_manager()
        self._logger = get_logger()
    
    @property
    def name(self) -> str:
        """获取导出器名称"""
        return self._name
    
    @abstractmethod
    def export(self, data: PipelineResult, output_path: str) -> bool:
        """
        导出数据
        
        参数:
            data: 流水线处理结果
            output_path: 输出文件路径
        
        返回:
            是否成功
        """
        pass
    
    def _ensure_directory(self, file_path: str) -> bool:
        """
        确保目录存在
        
        参数:
            file_path: 文件路径
        
        返回:
            是否成功
        """
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                return True
            except Exception as e:
                self._logger.error(f"创建目录失败: {directory} - {str(e)}")
                return False
        return True


class MarkdownExporter(BaseExporter):
    """
    Markdown报告导出器
    生成Markdown格式的分析报告
    """
    
    def __init__(self):
        """
        初始化Markdown导出器
        """
        super().__init__("MarkdownExporter")
    
    def export(self, data: PipelineResult, output_path: str) -> bool:
        """
        导出Markdown报告
        
        参数:
            data: 流水线处理结果
            output_path: 输出文件路径
        
        返回:
            是否成功
        """
        if not self._ensure_directory(output_path):
            return False
        
        try:
            content = self._generate_markdown(data)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self._logger.info(f"Markdown报告已生成: {output_path}")
            return True
            
        except Exception as e:
            self._logger.error(f"生成Markdown报告失败: {str(e)}")
            return False
    
    def _generate_markdown(self, data: PipelineResult) -> str:
        """
        生成Markdown内容
        
        参数:
            data: 流水线处理结果
        
        返回:
            Markdown内容字符串
        """
        lines = []
        
        # 标题
        lines.append("# 日志分析报告")
        lines.append("")
        lines.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # 执行摘要
        lines.append("## 执行摘要")
        lines.append("")
        lines.append(f"- **执行状态**: {'成功' if data.success else '失败'}")
        lines.append(f"- **执行时间**: {data.execution_time:.2f} 秒")
        lines.append(f"- **输入文件数**: {len(data.input_files)}")
        lines.append("")
        
        # 数据统计
        lines.append("## 数据统计")
        lines.append("")
        lines.append(f"- **总行数**: {data.total_lines}")
        lines.append(f"- **有效行数**: {data.valid_lines}")
        lines.append(f"- **异常数量**: {data.exception_count}")
        
        if data.total_lines > 0:
            valid_rate = (data.valid_lines / data.total_lines) * 100
            lines.append(f"- **有效率**: {valid_rate:.2f}%")
        
        lines.append("")
        
        # 详细统计
        if data.statistics:
            lines.append("## 详细统计")
            lines.append("")
            
            # 日志级别分布
            level_dist = data.statistics.get("level_distribution", {})
            if level_dist:
                lines.append("### 日志级别分布")
                lines.append("")
                lines.append("| 级别 | 数量 |")
                lines.append("|------|------|")
                for level, count in sorted(level_dist.items()):
                    lines.append(f"| {level} | {count} |")
                lines.append("")
            
            # 分类分布
            category_dist = data.statistics.get("category_distribution", {})
            if category_dist:
                lines.append("### 日志分类分布")
                lines.append("")
                lines.append("| 分类 | 数量 |")
                lines.append("|------|------|")
                for category, count in sorted(category_dist.items()):
                    lines.append(f"| {category} | {count} |")
                lines.append("")
            
            # 关键词频率
            keyword_freq = data.statistics.get("keyword_frequency", {})
            if keyword_freq:
                lines.append("### 关键词频率")
                lines.append("")
                lines.append("| 关键词 | 出现次数 |")
                lines.append("|--------|----------|")
                for keyword, count in sorted(keyword_freq.items(), key=lambda x: x[1], reverse=True):
                    lines.append(f"| {keyword} | {count} |")
                lines.append("")
            
            # Top IP地址
            top_ips = data.statistics.get("top_ip_addresses", [])
            if top_ips:
                lines.append("### Top IP地址")
                lines.append("")
                lines.append("| IP地址 | 访问次数 |")
                lines.append("|--------|----------|")
                for ip, count in top_ips:
                    lines.append(f"| {ip} | {count} |")
                lines.append("")
        
        # 异常汇总
        if data.exception_logs:
            lines.append("## 异常汇总")
            lines.append("")
            lines.append(f"共发现 **{len(data.exception_logs)}** 条异常日志")
            lines.append("")
            
            # 按级别分组
            exception_summary = data.statistics.get("exception_summary", {})
            types = exception_summary.get("types", {})
            if types:
                lines.append("### 异常类型分布")
                lines.append("")
                lines.append("| 类型 | 数量 |")
                lines.append("|------|------|")
                for exc_type, count in sorted(types.items()):
                    lines.append(f"| {exc_type} | {count} |")
                lines.append("")
            
            # 显示前10条异常
            lines.append("### 异常日志示例（前10条）")
            lines.append("")
            lines.append("```")
            for i, log in enumerate(data.exception_logs[:10], 1):
                lines.append(f"{i}. [{log.get('level', 'UNKNOWN')}] {log.get('line', '')[:100]}")
            lines.append("```")
            lines.append("")
        
        # 输入文件列表
        if data.input_files:
            lines.append("## 输入文件")
            lines.append("")
            for file_path in data.input_files:
                lines.append(f"- `{file_path}`")
            lines.append("")
        
        # 错误信息
        if data.errors:
            lines.append("## 错误信息")
            lines.append("")
            for error in data.errors:
                lines.append(f"- ⚠️ {error}")
            lines.append("")
        
        return "\n".join(lines)


class JsonExporter(BaseExporter):
    """
    JSON统计导出器
    生成JSON格式的统计结果
    """
    
    def __init__(self):
        """
        初始化JSON导出器
        """
        super().__init__("JsonExporter")
    
    def export(self, data: PipelineResult, output_path: str) -> bool:
        """
        导出JSON统计文件
        
        参数:
            data: 流水线处理结果
            output_path: 输出文件路径
        
        返回:
            是否成功
        """
        if not self._ensure_directory(output_path):
            return False
        
        try:
            json_data = self._generate_json(data)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            self._logger.info(f"JSON统计文件已生成: {output_path}")
            return True
            
        except Exception as e:
            self._logger.error(f"生成JSON统计文件失败: {str(e)}")
            return False
    
    def _generate_json(self, data: PipelineResult) -> Dict[str, Any]:
        """
        生成JSON数据
        
        参数:
            data: 流水线处理结果
        
        返回:
            JSON数据字典
        """
        return {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "success": data.success,
                "execution_time_seconds": data.execution_time
            },
            "summary": {
                "total_lines": data.total_lines,
                "valid_lines": data.valid_lines,
                "exception_count": data.exception_count,
                "input_files": data.input_files
            },
            "statistics": data.statistics,
            "exceptions": [
                {
                    "line": log.get("line", ""),
                    "level": log.get("level"),
                    "category": log.get("category"),
                    "timestamp": log.get("timestamp"),
                    "ip_address": log.get("ip_address"),
                    "user_id": log.get("user_id")
                }
                for log in data.exception_logs
            ],
            "errors": data.errors
        }


class ExceptionLogExporter(BaseExporter):
    """
    异常日志导出器
    将异常日志单独归档
    """
    
    def __init__(self):
        """
        初始化异常日志导出器
        """
        super().__init__("ExceptionLogExporter")
    
    def export(self, data: PipelineResult, output_path: str) -> bool:
        """
        导出异常日志文件
        
        参数:
            data: 流水线处理结果
            output_path: 输出文件路径
        
        返回:
            是否成功
        """
        if not data.exception_logs:
            self._logger.info("没有异常日志需要导出")
            return True
        
        if not self._ensure_directory(output_path):
            return False
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"# 异常日志归档\n")
                f.write(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# 异常总数: {len(data.exception_logs)}\n")
                f.write("=" * 80 + "\n\n")
                
                for i, log in enumerate(data.exception_logs, 1):
                    f.write(f"[{i}]\n")
                    f.write(f"Level: {log.get('level', 'UNKNOWN')}\n")
                    f.write(f"Category: {log.get('category', 'unknown')}\n")
                    f.write(f"Timestamp: {log.get('timestamp', 'N/A')}\n")
                    f.write(f"IP: {log.get('ip_address', 'N/A')}\n")
                    f.write(f"User: {log.get('user_id', 'N/A')}\n")
                    f.write(f"Content: {log.get('line', '')}\n")
                    f.write("-" * 80 + "\n\n")
            
            self._logger.info(f"异常日志已归档: {output_path}")
            return True
            
        except Exception as e:
            self._logger.error(f"导出异常日志失败: {str(e)}")
            return False


class ReportManager:
    """
    报告管理器
    统一管理所有报告导出
    """
    
    def __init__(self):
        """
        初始化报告管理器
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._exporters: Dict[str, BaseExporter] = {}
        self._register_default_exporters()
    
    def _register_default_exporters(self) -> None:
        """
        注册默认导出器
        """
        self.register_exporter("markdown", MarkdownExporter())
        self.register_exporter("json", JsonExporter())
        self.register_exporter("exceptions", ExceptionLogExporter())
    
    def register_exporter(self, name: str, exporter: BaseExporter) -> None:
        """
        注册导出器
        
        参数:
            name: 导出器标识名
            exporter: 导出器实例
        """
        self._exporters[name] = exporter
        self._logger.debug(f"已注册导出器: {name}")
    
    def generate_reports(self, data: PipelineResult, timestamp: str = None) -> Dict[str, str]:
        """
        生成所有报告
        
        参数:
            data: 流水线处理结果
            timestamp: 时间戳字符串
        
        返回:
            输出文件路径字典
        """
        if timestamp is None:
            timestamp = datetime.now().strftime(self._config.get_timestamp_format())
        
        output_dir = self._config.get_output_dir()
        prefix = self._config.get_output_prefix()
        
        output_files = {}
        
        # 生成Markdown报告
        md_path = os.path.join(output_dir, f"{prefix}{timestamp}.md")
        if self._exporters["markdown"].export(data, md_path):
            output_files["markdown"] = md_path
        
        # 生成JSON统计
        json_path = os.path.join(output_dir, f"{prefix}{timestamp}.json")
        if self._exporters["json"].export(data, json_path):
            output_files["json"] = json_path
        
        # 生成异常日志归档
        if data.exception_logs:
            exc_path = os.path.join(output_dir, f"{prefix}{timestamp}_exceptions.log")
            if self._exporters["exceptions"].export(data, exc_path):
                output_files["exceptions"] = exc_path
        
        return output_files


# 全局报告管理器实例
_report_manager_instance = None


def get_report_manager() -> ReportManager:
    """
    获取全局报告管理器实例（单例模式）
    
    返回:
        ReportManager实例
    """
    global _report_manager_instance
    if _report_manager_instance is None:
        _report_manager_instance = ReportManager()
    return _report_manager_instance
