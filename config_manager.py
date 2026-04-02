# -*- coding: utf-8 -*-
"""
配置管理模块 (ConfigManager)
负责加载YAML配置，管理路径、规则、过滤关键词
支持热加载配置
"""

import os
import yaml
import time
from typing import Dict, Any, List, Optional
from pathlib import Path


class ConfigManager:
    """
    配置管理器类
    使用PascalCase命名法
    """
    
    def __init__(self, config_path: str = "./settings.yaml"):
        """
        初始化配置管理器
        
        参数:
            config_path: YAML配置文件路径（相对路径）
        """
        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._last_load_time: float = 0
        self._last_modified_time: float = 0
        
        # 加载配置
        self._load_config()
    
    def _load_config(self) -> None:
        """
        从YAML文件加载配置
        私有方法，使用snake_case命名法
        """
        try:
            if not os.path.exists(self._config_path):
                raise FileNotFoundError(f"配置文件不存在: {self._config_path}")
            
            with open(self._config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file) or {}
            
            self._last_load_time = time.time()
            self._last_modified_time = os.path.getmtime(self._config_path)
            
        except yaml.YAMLError as e:
            raise ValueError(f"YAML配置解析错误: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {str(e)}")
    
    def reload_if_changed(self) -> bool:
        """
        如果配置文件已修改，则重新加载
        
        返回:
            bool: 是否执行了重新加载
        """
        try:
            current_modified_time = os.path.getmtime(self._config_path)
            if current_modified_time > self._last_modified_time:
                self._load_config()
                return True
            return False
        except Exception:
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置项，支持嵌套键（用点号分隔）
        
        参数:
            key: 配置键，如 "paths.input_dir"
            default: 默认值
        
        返回:
            配置值或默认值
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_input_dir(self) -> str:
        """
        获取输入目录路径
        
        返回:
            输入目录相对路径
        """
        return self.get("paths.input_dir", "./data/input")
    
    def get_output_dir(self) -> str:
        """
        获取输出目录路径
        
        返回:
            输出目录相对路径
        """
        return self.get("paths.output_dir", "./build/dist")
    
    def get_readonly_files(self) -> List[str]:
        """
        获取只读保护文件列表
        
        返回:
            只读文件路径列表（相对路径）
        """
        return self.get("paths.readonly_files", ["./data/input/legacy.log"])
    
    def get_allowed_extensions(self) -> List[str]:
        """
        获取允许的文件扩展名列表
        
        返回:
            扩展名列表，如 [".log"]
        """
        return self.get("file_formats.allowed_extensions", [".log"])
    
    def get_output_prefix(self) -> str:
        """
        获取输出文件名前缀
        
        返回:
            文件名前缀，如 "log_report_"
        """
        return self.get("naming.output_prefix", "log_report_")
    
    def get_timestamp_format(self) -> str:
        """
        获取时间戳格式
        
        返回:
            时间戳格式字符串
        """
        return self.get("naming.timestamp_format", "%Y%m%d_%H%M%S")
    
    def get_log_levels(self) -> List[str]:
        """
        获取支持的日志级别列表
        
        返回:
            日志级别列表
        """
        return self.get("log_parsing.log_levels", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    
    def get_exception_levels(self) -> List[str]:
        """
        获取需要提取的异常级别列表
        
        返回:
            异常级别列表
        """
        return self.get("log_parsing.exception_levels", ["ERROR", "CRITICAL"])
    
    def get_skip_keywords(self) -> List[str]:
        """
        获取需要跳过的关键词列表
        
        返回:
            关键词列表
        """
        return self.get("filters.skip_keywords", [])
    
    def get_min_line_length(self) -> int:
        """
        获取最小日志行长度
        
        返回:
            最小长度
        """
        return self.get("filters.min_line_length", 10)
    
    def get_max_line_length(self) -> int:
        """
        获取最大日志行长度
        
        返回:
            最大长度
        """
        return self.get("filters.max_line_length", 2048)
    
    def get_encoding(self) -> str:
        """
        获取文件编码格式
        
        返回:
            编码格式，如 "utf-8"
        """
        return self.get("filters.encoding", "utf-8")
    
    def get_encoding_errors(self) -> str:
        """
        获取编码错误处理方式
        
        返回:
            错误处理方式，如 "replace"
        """
        return self.get("filters.encoding_errors", "replace")
    
    def get_keyword_stats(self) -> List[str]:
        """
        获取需要统计的关键词列表
        
        返回:
            关键词列表
        """
        return self.get("statistics.keyword_stats", [])
    
    def get_time_window_minutes(self) -> int:
        """
        获取访问频次统计时间窗口（分钟）
        
        返回:
            时间窗口分钟数
        """
        return self.get("statistics.time_window_minutes", 60)
    
    def get_top_n_limit(self) -> int:
        """
        获取Top N统计限制
        
        返回:
            Top N数量
        """
        return self.get("statistics.top_n_limit", 10)
    
    def get_validation_pattern(self) -> Optional[str]:
        """
        获取日志格式校验正则表达式
        
        返回:
            正则表达式字符串或None
        """
        return self.get("validation.log_pattern")
    
    def get_ip_pattern(self) -> str:
        """
        获取IP地址提取正则表达式
        
        返回:
            正则表达式字符串
        """
        return self.get("validation.ip_pattern", r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b')
    
    def get_user_id_pattern(self) -> str:
        """
        获取用户ID提取正则表达式
        
        返回:
            正则表达式字符串
        """
        return self.get("validation.user_id_pattern", r'user[_-]?id[:\s]*([\w-]+)')
    
    def get_program_log_level(self) -> str:
        """
        获取程序日志级别
        
        返回:
            日志级别字符串
        """
        return self.get("program_logging.level", "INFO")
    
    def is_console_output_enabled(self) -> bool:
        """
        检查是否启用控制台输出
        
        返回:
            是否启用
        """
        return self.get("program_logging.console_output", True)
    
    def is_file_output_enabled(self) -> bool:
        """
        检查是否启用文件输出
        
        返回:
            是否启用
        """
        return self.get("program_logging.file_output", True)
    
    def get_program_log_file(self) -> str:
        """
        获取程序日志文件路径
        
        返回:
            日志文件相对路径
        """
        return self.get("program_logging.log_file", "./build/dist/app.log")
    
    def get_program_log_format(self) -> str:
        """
        获取程序日志格式
        
        返回:
            日志格式字符串
        """
        return self.get("program_logging.format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    def get_program_date_format(self) -> str:
        """
        获取程序日志日期格式
        
        返回:
            日期格式字符串
        """
        return self.get("program_logging.date_format", "%Y-%m-%d %H:%M:%S")
    
    def get_full_config(self) -> Dict[str, Any]:
        """
        获取完整配置字典
        
        返回:
            完整配置字典
        """
        return self._config.copy()


# 全局配置管理器实例
_config_instance: Optional[ConfigManager] = None


def get_config_manager(config_path: str = "./settings.yaml") -> ConfigManager:
    """
    获取全局配置管理器实例（单例模式）
    
    参数:
        config_path: 配置文件路径
    
    返回:
        ConfigManager实例
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigManager(config_path)
    return _config_instance
