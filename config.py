# -*- coding: utf-8 -*-
"""
配置模块 (Config)
加载YAML配置，管理路径、规则、过滤关键词
"""

import os
import yaml
from typing import Dict, Any, List, Optional


class Config:
    """
    配置管理器类
    使用PascalCase命名法
    """
    
    DEFAULT_CONFIG = {
        "paths": {
            "input_dir": "./data/input",
            "output_dir": "./build/dist",
            "readonly_files": ["./data/input/legacy.log"]
        },
        "file_formats": {
            "allowed_extensions": [".log"],
            "report_format": ".md",
            "stats_format": ".json"
        },
        "naming": {
            "output_prefix": "log_report_",
            "timestamp_format": "%Y%m%d_%H%M%S"
        },
        "log_parsing": {
            "log_levels": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "exception_levels": ["ERROR", "WARNING", "CRITICAL"],
            "timestamp_pattern": r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}",
            "ip_pattern": r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
        },
        "filters": {
            "skip_keywords": ["DEBUG"],
            "min_line_length": 10,
            "max_line_length": 2048
        },
        "statistics": {
            "keyword_stats": ["error", "warning", "login", "request", "response"],
            "top_n_limit": 10
        },
        "encoding": "utf-8"
    }
    
    def __init__(self, config_path: str = "./settings.yaml"):
        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        try:
            if os.path.exists(self._config_path):
                with open(self._config_path, 'r', encoding='utf-8') as f:
                    loaded_config = yaml.safe_load(f) or {}
                self._config = self._deep_merge(self.DEFAULT_CONFIG, loaded_config)
            else:
                self._config = self.DEFAULT_CONFIG.copy()
        except Exception:
            self._config = self.DEFAULT_CONFIG.copy()
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def get_input_dir(self) -> str:
        return self._config["paths"]["input_dir"]
    
    def get_output_dir(self) -> str:
        return self._config["paths"]["output_dir"]
    
    def get_readonly_files(self) -> List[str]:
        return self._config["paths"]["readonly_files"]
    
    def get_allowed_extensions(self) -> List[str]:
        return self._config["file_formats"]["allowed_extensions"]
    
    def get_output_prefix(self) -> str:
        return self._config["naming"]["output_prefix"]
    
    def get_timestamp_format(self) -> str:
        return self._config["naming"]["timestamp_format"]
    
    def get_log_levels(self) -> List[str]:
        return self._config["log_parsing"]["log_levels"]
    
    def get_exception_levels(self) -> List[str]:
        return self._config["log_parsing"]["exception_levels"]
    
    def get_timestamp_pattern(self) -> str:
        return self._config["log_parsing"]["timestamp_pattern"]
    
    def get_ip_pattern(self) -> str:
        return self._config["log_parsing"]["ip_pattern"]
    
    def get_skip_keywords(self) -> List[str]:
        return self._config["filters"]["skip_keywords"]
    
    def get_min_line_length(self) -> int:
        return self._config["filters"]["min_line_length"]
    
    def get_max_line_length(self) -> int:
        return self._config["filters"]["max_line_length"]
    
    def get_keyword_stats(self) -> List[str]:
        return self._config["statistics"]["keyword_stats"]
    
    def get_top_n_limit(self) -> int:
        return self._config["statistics"]["top_n_limit"]
    
    def get_encoding(self) -> str:
        return self._config["encoding"]


_config_instance: Optional[Config] = None


def get_config() -> Config:
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance
