@echo off
rem 一键启动足球数据采集融合服务（先确保已执行 pip install -e .）
"%~dp0.venv\Scripts\python.exe" -I -m fusion.app
