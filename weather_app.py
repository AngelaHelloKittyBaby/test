#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 定时获取城市天气小程序
功能：定时获取指定城市天气信息并打印，支持保存到本地日志
作者：AI Assistant
版本：1.0
"""

import requests
import schedule
import time
import json
import os
import sys
from datetime import datetime
from config import API_KEY, CITY_NAME, INTERVAL_MINUTES, SAVE_LOG


def get_weather_data(city: str, api_key: str) -> dict:
    """
    从和风天气API获取指定城市的实时天气数据
    
    参数:
        city: 城市名称（中文，如"北京"）
        api_key: 和风天气API密钥
    
    返回:
        dict: 包含天气信息的字典，失败返回None
    """
    # 和风天气城市查找API - 先获取城市ID
    geo_url = "https://geoapi.qweather.com/v2/city/lookup"
    geo_params = {
        "location": city,
        "key": api_key,
        "range": "cn"  # 限定中国范围
    }
    
    try:
        # 第一步：获取城市ID
        geo_response = requests.get(geo_url, params=geo_params, timeout=10)
        geo_response.raise_for_status()
        geo_data = geo_response.json()
        
        # 检查城市查找结果
        if geo_data.get("code") != "200" or not geo_data.get("location"):
            print(f"❌ 城市查找失败：{city}，错误码：{geo_data.get('code')}")
            return None
        
        # 获取第一个匹配城市的ID
        location_id = geo_data["location"][0]["id"]
        city_name = geo_data["location"][0]["name"]
        
        # 第二步：获取实时天气数据
        weather_url = "https://devapi.qweather.com/v7/weather/now"
        weather_params = {
            "location": location_id,
            "key": api_key
        }
        
        weather_response = requests.get(weather_url, params=weather_params, timeout=10)
        weather_response.raise_for_status()
        weather_data = weather_response.json()
        
        if weather_data.get("code") != "200":
            print(f"❌ 天气数据获取失败，错误码：{weather_data.get('code')}")
            return None
        
        # 提取并格式化天气信息
        now = weather_data["now"]
        result = {
            "city": city_name,
            "temperature": now.get("temp", "N/A"),
            "feels_like": now.get("feelsLike", "N/A"),
            "weather": now.get("text", "N/A"),
            "wind_direction": now.get("windDir", "N/A"),
            "wind_scale": now.get("windScale", "N/A"),
            "humidity": now.get("humidity", "N/A"),
            "pressure": now.get("pressure", "N/A"),
            "visibility": now.get("vis", "N/A"),
            "update_time": weather_data.get("updateTime", "N/A")
        }
        
        return result
        
    except requests.exceptions.Timeout:
        print("❌ 网络请求超时，请检查网络连接")
        return None
    except requests.exceptions.ConnectionError:
        print("❌ 网络连接错误，请检查网络连接")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络请求异常：{str(e)}")
        return None
    except json.JSONDecodeError:
        print("❌ 数据解析失败，API返回格式异常")
        return None
    except Exception as e:
        print(f"❌ 未知错误：{str(e)}")
        return None


def format_weather_output(weather_data: dict) -> str:
    """
    将天气数据格式化为美观的字符串输出
    
    参数:
        weather_data: 包含天气信息的字典
    
    返回:
        str: 格式化后的天气信息字符串
    """
    if not weather_data:
        return "❌ 无法获取天气信息"
    
    # 获取当前时间
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 构建输出字符串
    output = []
    output.append("=" * 50)
    output.append(f"🌍 城市：{weather_data['city']}")
    output.append(f"🌡️  温度：{weather_data['temperature']}°C")
    output.append(f"🌤️  天气：{weather_data['weather']}")
    output.append(f"💨 风向：{weather_data['wind_direction']} {weather_data['wind_scale']}级")
    output.append(f"💧 湿度：{weather_data['humidity']}%")
    output.append(f"🌡️  体感：{weather_data['feels_like']}°C")
    output.append(f"📊 气压：{weather_data['pressure']} hPa")
    output.append(f"👁️  能见度：{weather_data['visibility']} km")
    output.append(f"🕐 更新时间：{current_time}")
    output.append("=" * 50)
    
    return "\n".join(output)


def save_weather_log(weather_data: dict, log_file: str = "weather_log.txt"):
    """
    将天气信息保存到本地日志文件
    
    参数:
        weather_data: 包含天气信息的字典
        log_file: 日志文件路径
    """
    if not weather_data:
        return
    
    try:
        # 获取当前时间
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 构建日志内容（单行格式，便于后续分析）
        log_entry = (
            f"[{current_time}] "
            f"城市:{weather_data['city']} | "
            f"温度:{weather_data['temperature']}°C | "
            f"天气:{weather_data['weather']} | "
            f"风向:{weather_data['wind_direction']} | "
            f"风力:{weather_data['wind_scale']}级 | "
            f"湿度:{weather_data['humidity']}% | "
            f"体感:{weather_data['feels_like']}°C\n"
        )
        
        # 追加写入文件
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(f"✅ 天气记录已保存到 {log_file}")
        
    except PermissionError:
        print(f"❌ 无法写入日志文件：权限不足")
    except Exception as e:
        print(f"❌ 保存日志失败：{str(e)}")


def fetch_and_display_weather():
    """
    获取并显示天气信息的主函数
    这是定时任务调用的核心函数
    """
    print(f"\n🔄 正在获取天气信息... [{datetime.now().strftime('%H:%M:%S')}]")
    
    # 获取天气数据
    weather_data = get_weather_data(CITY_NAME, API_KEY)
    
    if weather_data:
        # 格式化并打印天气信息
        output = format_weather_output(weather_data)
        print(output)
        
        # 如果开启日志记录，保存到文件
        if SAVE_LOG:
            save_weather_log(weather_data)
    else:
        print("❌ 获取天气信息失败，将在下次定时任务重试")


def setup_schedule():
    """
    设置定时任务
    根据配置的时间间隔创建定时任务
    """
    # 清除所有现有任务
    schedule.clear()
    
    # 根据时间间隔设置定时任务
    if INTERVAL_MINUTES < 60:
        # 小于60分钟，使用分钟间隔
        schedule.every(INTERVAL_MINUTES).minutes.do(fetch_and_display_weather)
        print(f"⏰ 已设置定时任务：每 {INTERVAL_MINUTES} 分钟获取一次天气")
    else:
        # 大于等于60分钟，转换为小时
        hours = INTERVAL_MINUTES // 60
        schedule.every(hours).hours.do(fetch_and_display_weather)
        print(f"⏰ 已设置定时任务：每 {hours} 小时获取一次天气")
    
    # 立即执行一次，让用户马上看到效果
    print("🚀 立即执行首次天气获取...")
    fetch_and_display_weather()


def main():
    """
    程序主入口
    处理命令行参数，设置定时任务，进入主循环
    """
    # 检查配置是否有效
    if API_KEY == "your_api_key_here":
        print("\n" + "=" * 60)
        print("⚠️  警告：请先在 config.py 中填写您的和风天气API密钥！")
        print("=" * 60)
        print("\n使用步骤：")
        print("1. 访问 https://dev.qweather.com/ 注册账号")
        print("2. 创建应用获取 API Key")
        print("3. 将 API Key 填入 config.py 中的 API_KEY 变量")
        print("4. 重新运行程序\n")
        sys.exit(1)
    
    # 打印程序信息
    print("\n" + "=" * 60)
    print("🌤️  Python 定时天气获取程序")
    print("=" * 60)
    print(f"📍 目标城市：{CITY_NAME}")
    print(f"⏱️  时间间隔：{INTERVAL_MINUTES} 分钟")
    print(f"📝 日志记录：{'开启' if SAVE_LOG else '关闭'}")
    print("=" * 60 + "\n")
    
    # 设置定时任务
    setup_schedule()
    
    print("\n✨ 程序运行中，按 Ctrl+C 停止...\n")
    
    # 主循环：持续运行定时任务
    try:
        while True:
            # 运行所有待执行的任务
            schedule.run_pending()
            # 休眠1秒，避免CPU占用过高
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 程序已手动停止")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 程序运行出错：{str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
