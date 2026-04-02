# -*- coding: utf-8 -*-
"""
全局日志系统模块 (LoggerSystem)
负责记录程序运行状态，支持控制台和文件双输出
"""

import os
import sys
import logging
import logging.handlers
from typing import Optional
from datetime import datetime

from config_manager import get_config_manager


class LoggerSystem:
    """
    日志系统类
    使用PascalCase命名法
    提供统一的程序运行日志记录功能
    """
    
    def __init__(self, name: str = "LogAnalyzer"):
        """
        初始化日志系统
        
        参数:
            name: 日志记录器名称
        """
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        self._logger.handlers = []  # 清除已有处理器
        
        self._config = get_config_manager()
        self._setup_logger()
    
    def _setup_logger(self) -> None:
        """
        配置日志处理器
        私有方法，使用snake_case命名法
        """
        # 获取配置
        log_level = self._config.get_program_log_level()
        console_enabled = self._config.is_console_output_enabled()
        file_enabled = self._config.is_file_output_enabled()
        log_format = self._config.get_program_log_format()
        date_format = self._config.get_program_date_format()
        
        # 设置日志级别
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        self._logger.setLevel(level_map.get(log_level.upper(), logging.INFO))
        
        # 创建格式化器
        formatter = logging.Formatter(log_format, datefmt=date_format)
        
        # 控制台处理器
        if console_enabled:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self._logger.level)
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
        
        # 文件处理器
        if file_enabled:
            log_file = self._config.get_program_log_file()
            
            # 确保日志目录存在
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                try:
                    os.makedirs(log_dir, exist_ok=True)
                except Exception as e:
                    self._logger.warning(f"无法创建日志目录: {str(e)}")
            
            # 使用RotatingFileHandler限制文件大小
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(self._logger.level)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
    
    def debug(self, message: str) -> None:
        """
        记录DEBUG级别日志
        
        参数:
            message: 日志消息
        """
        self._logger.debug(message)
    
    def info(self, message: str) -> None:
        """
        记录INFO级别日志
        
        参数:
            message: 日志消息
        """
        self._logger.info(message)
    
    def warning(self, message: str) -> None:
        """
        记录WARNING级别日志
        
        参数:
            message: 日志消息
        """
        self._logger.warning(message)
    
    def error(self, message: str) -> None:
        """
        记录ERROR级别日志
        
        参数:
            message: 日志消息
        """
        self._logger.error(message)
    
    def critical(self, message: str) -> None:
        """
        记录CRITICAL级别日志
        
        参数:
            message: 日志消息
        """
        self._logger.critical(message)
    
    def exception(self, message: str) -> None:
        """
        记录异常信息（包含堆栈）
        
        参数:
            message: 日志消息
        """
        self._logger.exception(message)
    
    def log_start(self, task_name: str) -> None:
        """
        记录任务开始
        
        参数:
            task_name: 任务名称
        """
        self.info(f"=" * 60)
        self.info(f"开始执行任务: {task_name}")
        self.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.info(f"=" * 60)
    
    def log_end(self, task_name: str, status: str = "成功") -> None:
        """
        记录任务结束
        
        参数:
            task_name: 任务名称
            status: 任务状态
        """
        self.info(f"=" * 60)
        self.info(f"任务完成: {task_name}")
        self.info(f"状态: {status}")
        self.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.info(f"=" * 60)
    
    def log_progress(self, current: int, total: int, description: str = "") -> None:
        """
        记录进度信息
        
        参数:
            current: 当前进度
            total: 总进度
            description: 进度描述
        """
        percentage = (current / total * 100) if total > 0 else 0
        self.info(f"{description} 进度: {current}/{total} ({percentage:.1f}%)")
    
    def log_file_processed(self, file_path: str, line_count: int, valid_count: int) -> None:
        """
        记录文件处理结果
        
        参数:
            file_path: 文件路径
            line_count: 总行数
            valid_count: 有效行数
        """
        self.info(f"文件处理完成: {file_path}")
        self.info(f"  - 总行数: {line_count}")
        self.info(f"  - 有效行数: {valid_count}")
        self.info(f"  - 有效率: {(valid_count/line_count*100):.1f}%" if line_count > 0 else "  - 有效率: 0%")
    
    def get_logger(self) -> logging.Logger:
        """
        获取底层logger实例
        
        返回:
            logging.Logger实例
        """
        return self._logger


# 全局日志系统实例
_logger_instance: Optional[LoggerSystem] = None


def get_logger(name: str = "LogAnalyzer") -> LoggerSystem:
    """
    获取全局日志系统实例（单例模式）
    
    参数:
        name: 日志记录器名称
    
    返回:
        LoggerSystem实例
    """
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = LoggerSystem(name)
    return _logger_instance
