#!/bin/bash

# Устанавливаем переменные окружения для Flask
export FLASK_APP=ref.py
export FLASK_RUN_HOST=0.0.0.0
export FLASK_RUN_PORT=${PORT:-5000}

# Запуск приложения с помощью Gunicorn
gunicorn -w 4 -b 0.0.0.0:$FLASK_RUN_PORT ref:app
