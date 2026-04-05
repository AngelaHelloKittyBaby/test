# -*- coding: utf-8 -*-
"""
插件式过滤规则模块 (FilterPlugins)
支持动态扩展的过滤规则管理器
"""

import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass

from config_manager import get_config_manager
from logger_system import get_logger


@dataclass
class FilterResult:
    """
    过滤结果数据类
    """
    should_filter: bool
    reason: str = ""
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class BaseFilterPlugin(ABC):
    """
    过滤器插件基类
    使用PascalCase命名法
    所有自定义过滤器必须继承此类
    """
    
    def __init__(self, name: str, priority: int = 100):
        """
        初始化过滤器插件
        
        参数:
            name: 过滤器名称
            priority: 优先级（数字越小优先级越高）
        """
        self._name = name
        self._priority = priority
        self._enabled = True
        self._logger = get_logger()
    
    @property
    def name(self) -> str:
        """获取过滤器名称"""
        return self._name
    
    @property
    def priority(self) -> int:
        """获取过滤器优先级"""
        return self._priority
    
    @property
    def enabled(self) -> bool:
        """获取过滤器启用状态"""
        return self._enabled
    
    def set_enabled(self, enabled: bool) -> None:
        """
        设置过滤器启用状态
        
        参数:
            enabled: 是否启用
        """
        self._enabled = enabled
    
    @abstractmethod
    def filter(self, log_line: str, context: Dict[str, Any] = None) -> FilterResult:
        """
        执行过滤逻辑
        
        参数:
            log_line: 日志行内容
            context: 上下文信息
        
        返回:
            FilterResult对象
        """
        pass
    
    def on_init(self) -> None:
        """
        过滤器初始化钩子
        子类可重写此方法进行初始化
        """
        pass
    
    def on_destroy(self) -> None:
        """
        过滤器销毁钩子
        子类可重写此方法进行清理
        """
        pass


class KeywordFilterPlugin(BaseFilterPlugin):
    """
    关键词过滤器插件
    过滤包含指定关键词的日志行
    """
    
    def __init__(self, keywords: List[str] = None):
        """
        初始化关键词过滤器
        
        参数:
            keywords: 需要过滤的关键词列表
        """
        super().__init__(name="KeywordFilter", priority=10)
        self._keywords = keywords or []
        self._compile_patterns()
    
    def _compile_patterns(self) -> None:
        """
        编译关键词为正则表达式模式
        """
        self._patterns = []
        for keyword in self._keywords:
            try:
                # 使用忽略大小写的匹配
                pattern = re.compile(re.escape(keyword), re.IGNORECASE)
                self._patterns.append((keyword, pattern))
            except re.error as e:
                self._logger.warning(f"关键词 '{keyword}' 正则编译失败: {str(e)}")
    
    def filter(self, log_line: str, context: Dict[str, Any] = None) -> FilterResult:
        """
        执行关键词过滤
        
        参数:
            log_line: 日志行内容
            context: 上下文信息
        
        返回:
            FilterResult对象
        """
        if not self._enabled:
            return FilterResult(should_filter=False)
        
        for keyword, pattern in self._patterns:
            if pattern.search(log_line):
                return FilterResult(
                    should_filter=True,
                    reason=f"匹配过滤关键词: {keyword}",
                    metadata={"matched_keyword": keyword}
                )
        
        return FilterResult(should_filter=False)
    
    def add_keyword(self, keyword: str) -> None:
        """
        动态添加过滤关键词
        
        参数:
            keyword: 关键词
        """
        self._keywords.append(keyword)
        try:
            pattern = re.compile(re.escape(keyword), re.IGNORECASE)
            self._patterns.append((keyword, pattern))
        except re.error as e:
            self._logger.warning(f"添加关键词 '{keyword}' 失败: {str(e)}")
    
    def remove_keyword(self, keyword: str) -> bool:
        """
        移除过滤关键词
        
        参数:
            keyword: 关键词
        
        返回:
            是否成功移除
        """
        if keyword in self._keywords:
            self._keywords.remove(keyword)
            self._compile_patterns()
            return True
        return False


class LengthFilterPlugin(BaseFilterPlugin):
    """
    长度过滤器插件
    过滤过长或过短的日志行
    """
    
    def __init__(self, min_length: int = 10, max_length: int = 2048):
        """
        初始化长度过滤器
        
        参数:
            min_length: 最小长度
            max_length: 最大长度
        """
        super().__init__(name="LengthFilter", priority=20)
        self._min_length = min_length
        self._max_length = max_length
    
    def filter(self, log_line: str, context: Dict[str, Any] = None) -> FilterResult:
        """
        执行长度过滤
        
        参数:
            log_line: 日志行内容
            context: 上下文信息
        
        返回:
            FilterResult对象
        """
        if not self._enabled:
            return FilterResult(should_filter=False)
        
        line_length = len(log_line)
        
        if line_length < self._min_length:
            return FilterResult(
                should_filter=True,
                reason=f"日志行过短 ({line_length} < {self._min_length})",
                metadata={"length": line_length, "min_length": self._min_length}
            )
        
        if line_length > self._max_length:
            return FilterResult(
                should_filter=True,
                reason=f"日志行过长 ({line_length} > {self._max_length})",
                metadata={"length": line_length, "max_length": self._max_length}
            )
        
        return FilterResult(should_filter=False)


class RegexFilterPlugin(BaseFilterPlugin):
    """
    正则表达式过滤器插件
    使用正则表达式匹配过滤
    """
    
    def __init__(self, patterns: List[str] = None):
        """
        初始化正则过滤器
        
        参数:
            patterns: 正则表达式模式列表
        """
        super().__init__(name="RegexFilter", priority=30)
        self._pattern_strings = patterns or []
        self._compiled_patterns: List[re.Pattern] = []
        self._compile_patterns()
    
    def _compile_patterns(self) -> None:
        """
        编译正则表达式
        """
        self._compiled_patterns = []
        for pattern_str in self._pattern_strings:
            try:
                pattern = re.compile(pattern_str, re.IGNORECASE)
                self._compiled_patterns.append(pattern)
            except re.error as e:
                self._logger.warning(f"正则表达式编译失败 '{pattern_str}': {str(e)}")
    
    def filter(self, log_line: str, context: Dict[str, Any] = None) -> FilterResult:
        """
        执行正则过滤
        
        参数:
            log_line: 日志行内容
            context: 上下文信息
        
        返回:
            FilterResult对象
        """
        if not self._enabled:
            return FilterResult(should_filter=False)
        
        for i, pattern in enumerate(self._compiled_patterns):
            if pattern.search(log_line):
                return FilterResult(
                    should_filter=True,
                    reason=f"匹配正则表达式 #{i+1}",
                    metadata={"pattern_index": i, "pattern": self._pattern_strings[i]}
                )
        
        return FilterResult(should_filter=False)
    
    def add_pattern(self, pattern: str) -> bool:
        """
        添加正则表达式模式
        
        参数:
            pattern: 正则表达式字符串
        
        返回:
            是否成功添加
        """
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
            self._pattern_strings.append(pattern)
            self._compiled_patterns.append(compiled)
            return True
        except re.error as e:
            self._logger.error(f"添加正则表达式失败: {str(e)}")
            return False


class FilterPluginManager:
    """
    过滤器插件管理器
    统一管理所有过滤器插件
    """
    
    def __init__(self):
        """
        初始化插件管理器
        """
        self._plugins: List[BaseFilterPlugin] = []
        self._logger = get_logger()
        self._config = get_config_manager()
        self._init_default_plugins()
    
    def _init_default_plugins(self) -> None:
        """
        初始化默认过滤器插件
        """
        # 从配置加载关键词过滤器
        skip_keywords = self._config.get_skip_keywords()
        if skip_keywords:
            keyword_filter = KeywordFilterPlugin(keywords=skip_keywords)
            self.register_plugin(keyword_filter)
            self._logger.info(f"已加载关键词过滤器，关键词数量: {len(skip_keywords)}")
        
        # 从配置加载长度过滤器
        min_length = self._config.get_min_line_length()
        max_length = self._config.get_max_line_length()
        length_filter = LengthFilterPlugin(min_length=min_length, max_length=max_length)
        self.register_plugin(length_filter)
        self._logger.info(f"已加载长度过滤器，范围: {min_length}-{max_length}")
    
    def register_plugin(self, plugin: BaseFilterPlugin) -> None:
        """
        注册过滤器插件
        
        参数:
            plugin: 过滤器插件实例
        """
        plugin.on_init()
        self._plugins.append(plugin)
        # 按优先级排序
        self._plugins.sort(key=lambda p: p.priority)
        self._logger.info(f"已注册过滤器插件: {plugin.name} (优先级: {plugin.priority})")
    
    def unregister_plugin(self, plugin_name: str) -> bool:
        """
        注销过滤器插件
        
        参数:
            plugin_name: 插件名称
        
        返回:
            是否成功注销
        """
        for i, plugin in enumerate(self._plugins):
            if plugin.name == plugin_name:
                plugin.on_destroy()
                self._plugins.pop(i)
                self._logger.info(f"已注销过滤器插件: {plugin_name}")
                return True
        return False
    
    def apply_filters(self, log_line: str, context: Dict[str, Any] = None) -> FilterResult:
        """
        应用所有过滤器
        
        参数:
            log_line: 日志行内容
            context: 上下文信息
        
        返回:
            FilterResult对象，如果任一过滤器返回should_filter=True则过滤
        """
        for plugin in self._plugins:
            if not plugin.enabled:
                continue
            
            try:
                result = plugin.filter(log_line, context)
                if result.should_filter:
                    return result
            except Exception as e:
                self._logger.error(f"过滤器 '{plugin.name}' 执行失败: {str(e)}")
                continue
        
        return FilterResult(should_filter=False)
    
    def get_plugin(self, name: str) -> Optional[BaseFilterPlugin]:
        """
        获取指定名称的插件
        
        参数:
            name: 插件名称
        
        返回:
            插件实例或None
        """
        for plugin in self._plugins:
            if plugin.name == name:
                return plugin
        return None
    
    def list_plugins(self) -> List[Dict[str, Any]]:
        """
        列出所有已注册的插件
        
        返回:
            插件信息列表
        """
        return [
            {
                "name": plugin.name,
                "priority": plugin.priority,
                "enabled": plugin.enabled
            }
            for plugin in self._plugins
        ]
    
    def clear_plugins(self) -> None:
        """
        清除所有插件
        """
        for plugin in self._plugins:
            plugin.on_destroy()
        self._plugins.clear()
        self._logger.info("已清除所有过滤器插件")


# 全局过滤器管理器实例
_filter_manager_instance: Optional[FilterPluginManager] = None


def get_filter_manager() -> FilterPluginManager:
    """
    获取全局过滤器管理器实例（单例模式）
    
    返回:
        FilterPluginManager实例
    """
    global _filter_manager_instance
    if _filter_manager_instance is None:
        _filter_manager_instance = FilterPluginManager()
    return _filter_manager_instance
