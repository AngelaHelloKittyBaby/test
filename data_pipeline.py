# -*- coding: utf-8 -*-
"""
数据处理流水线模块 (DataPipeline)
重构核心处理流水线，解耦读取/处理/输出
"""

import os
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

from config_manager import get_config_manager
from logger_system import get_logger
from log_reader import LogReader, ReadResult, get_log_reader
from data_processor import DataProcessor, ProcessResult, get_data_processor


@dataclass
class PipelineResult:
    """
    流水线处理结果数据类
    """
    success: bool
    input_files: List[str]
    total_lines: int
    valid_lines: int
    exception_count: int
    statistics: Dict[str, Any]
    exception_logs: List[Dict[str, Any]]
    errors: List[str]
    execution_time: float
    output_files: Dict[str, str]


class DataPipeline:
    """
    数据处理流水线类
    使用PascalCase命名法
    负责协调读取、处理、输出的完整流程
    """
    
    def __init__(self):
        """
        初始化数据处理流水线
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._reader = get_log_reader()
        self._processor = get_data_processor()
        
        # 中间处理钩子
        self._pre_process_hooks: List[Callable] = []
        self._post_process_hooks: List[Callable] = []
    
    def add_pre_process_hook(self, hook: Callable[[List[str]], List[str]]) -> None:
        """
        添加预处理钩子
        
        参数:
            hook: 处理函数，接收行列表，返回处理后的行列表
        """
        self._pre_process_hooks.append(hook)
        self._logger.debug(f"已添加预处理钩子: {hook.__name__}")
    
    def add_post_process_hook(self, hook: Callable[[ProcessResult], ProcessResult]) -> None:
        """
        添加后处理钩子
        
        参数:
            hook: 处理函数，接收ProcessResult，返回ProcessResult
        """
        self._post_process_hooks.append(hook)
        self._logger.debug(f"已添加后处理钩子: {hook.__name__}")
    
    def execute(self, input_dir: str = None) -> PipelineResult:
        """
        执行完整的数据处理流水线
        
        参数:
            input_dir: 输入目录，默认使用配置中的路径
        
        返回:
            PipelineResult对象
        """
        start_time = datetime.now()
        errors = []
        output_files = {}
        
        try:
            self._logger.log_start("数据处理流水线")
            
            # 步骤1: 读取日志文件
            read_results = self._step_read(input_dir)
            if not read_results:
                return self._create_error_result("没有成功读取任何日志文件", start_time)
            
            # 步骤2: 合并所有行
            all_lines = self._step_merge_lines(read_results)
            if not all_lines:
                return self._create_error_result("没有有效的日志行", start_time)
            
            # 步骤3: 预处理
            processed_lines = self._step_pre_process(all_lines)
            
            # 步骤4: 处理数据
            process_result = self._step_process(processed_lines)
            
            # 步骤5: 后处理
            process_result = self._step_post_process(process_result)
            
            # 步骤6: 生成统计
            statistics = self._processor.generate_statistics(process_result)
            
            # 计算执行时间
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # 收集输入文件列表
            input_files = [r.file_path for r in read_results if r.success]
            
            self._logger.log_end("数据处理流水线", "成功")
            
            return PipelineResult(
                success=True,
                input_files=input_files,
                total_lines=process_result.total_lines,
                valid_lines=process_result.valid_lines,
                exception_count=len(process_result.exception_logs),
                statistics=statistics,
                exception_logs=process_result.exception_logs,
                errors=errors,
                execution_time=execution_time,
                output_files=output_files
            )
            
        except Exception as e:
            self._logger.exception("数据处理流水线执行失败")
            errors.append(str(e))
            execution_time = (datetime.now() - start_time).total_seconds()
            return self._create_error_result(str(e), start_time, errors)
    
    def _step_read(self, input_dir: str = None) -> List[ReadResult]:
        """
        步骤1: 读取日志文件
        
        参数:
            input_dir: 输入目录
        
        返回:
            读取结果列表
        """
        self._logger.info("步骤1: 读取日志文件")
        
        if input_dir is None:
            input_dir = self._config.get_input_dir()
        
        read_results = self._reader.read_directory(input_dir)
        
        success_count = sum(1 for r in read_results if r.success)
        self._logger.info(f"读取完成: {success_count}/{len(read_results)} 个文件成功")
        
        return read_results
    
    def _step_merge_lines(self, read_results: List[ReadResult]) -> List[str]:
        """
        步骤2: 合并所有日志行
        
        参数:
            read_results: 读取结果列表
        
        返回:
            合并后的日志行列表
        """
        self._logger.info("步骤2: 合并日志行")
        
        all_lines = []
        for result in read_results:
            if result.success:
                all_lines.extend(result.lines)
        
        self._logger.info(f"合并完成: 共 {len(all_lines)} 行")
        return all_lines
    
    def _step_pre_process(self, lines: List[str]) -> List[str]:
        """
        步骤3: 预处理
        
        参数:
            lines: 日志行列表
        
        返回:
            预处理后的行列表
        """
        self._logger.info("步骤3: 执行预处理")
        
        processed_lines = lines
        
        # 执行注册的预处理钩子
        for hook in self._pre_process_hooks:
            try:
                processed_lines = hook(processed_lines)
                self._logger.debug(f"预处理钩子执行完成: {hook.__name__}")
            except Exception as e:
                self._logger.error(f"预处理钩子执行失败: {hook.__name__} - {str(e)}")
        
        self._logger.info(f"预处理完成: {len(processed_lines)} 行")
        return processed_lines
    
    def _step_process(self, lines: List[str]) -> ProcessResult:
        """
        步骤4: 处理数据
        
        参数:
            lines: 日志行列表
        
        返回:
            处理结果
        """
        self._logger.info("步骤4: 处理数据")
        
        process_result = self._processor.process_lines(lines)
        
        self._logger.info(
            f"处理完成: 总计 {process_result.total_lines}, "
            f"有效 {process_result.valid_lines}, "
            f"异常 {process_result.exception_lines}"
        )
        
        return process_result
    
    def _step_post_process(self, process_result: ProcessResult) -> ProcessResult:
        """
        步骤5: 后处理
        
        参数:
            process_result: 处理结果
        
        返回:
            处理后的结果
        """
        self._logger.info("步骤5: 执行后处理")
        
        # 执行注册的后处理钩子
        for hook in self._post_process_hooks:
            try:
                process_result = hook(process_result)
                self._logger.debug(f"后处理钩子执行完成: {hook.__name__}")
            except Exception as e:
                self._logger.error(f"后处理钩子执行失败: {hook.__name__} - {str(e)}")
        
        return process_result
    
    def _create_error_result(self, message: str, start_time: datetime, 
                            errors: List[str] = None) -> PipelineResult:
        """
        创建错误结果
        
        参数:
            message: 错误消息
            start_time: 开始时间
            errors: 错误列表
        
        返回:
            PipelineResult对象
        """
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return PipelineResult(
            success=False,
            input_files=[],
            total_lines=0,
            valid_lines=0,
            exception_count=0,
            statistics={},
            exception_logs=[],
            errors=errors or [message],
            execution_time=execution_time,
            output_files={}
        )
    
    def execute_single_file(self, file_path: str) -> PipelineResult:
        """
        处理单个文件
        
        参数:
            file_path: 文件路径
        
        返回:
            PipelineResult对象
        """
        start_time = datetime.now()
        
        try:
            self._logger.info(f"开始处理单个文件: {file_path}")
            
            # 读取文件
            read_result = self._reader.read_file(file_path)
            
            if not read_result.success:
                return self._create_error_result(
                    f"读取文件失败: {read_result.error_message}", 
                    start_time
                )
            
            # 处理数据
            process_result = self._processor.process_lines(read_result.lines)
            
            # 生成统计
            statistics = self._processor.generate_statistics(process_result)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return PipelineResult(
                success=True,
                input_files=[file_path],
                total_lines=process_result.total_lines,
                valid_lines=process_result.valid_lines,
                exception_count=len(process_result.exception_logs),
                statistics=statistics,
                exception_logs=process_result.exception_logs,
                errors=[],
                execution_time=execution_time,
                output_files={}
            )
            
        except Exception as e:
            self._logger.exception(f"处理文件失败: {file_path}")
            return self._create_error_result(str(e), start_time)


# 全局流水线实例
_pipeline_instance = None


def get_data_pipeline() -> DataPipeline:
    """
    获取全局数据处理流水线实例（单例模式）
    
    返回:
        DataPipeline实例
    """
    global _pipeline_instance
    if _pipeline_instance is None:
        _pipeline_instance = DataPipeline()
    return _pipeline_instance
