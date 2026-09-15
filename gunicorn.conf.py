import os

# ВАЖНО:
# Один process, чтобы user_states, locks, кеши и background threads
# оставались общими для всего бота.
workers = 1

# Параллельные HTTP-запросы внутри одного process.
worker_class = "gthread"
threads = 8

bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"

# Octo/Grist иногда отвечают медленно.
timeout = 120
graceful_timeout = 30
keepalive = 5

# Не включать preload при текущей архитектуре.
preload_app = False

accesslog = "-"
errorlog = "-"
loglevel = "info"
