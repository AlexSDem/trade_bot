# Intraday Trading Bot (T-Invest)

Простой внутридневной торговый бот:
- рынок: акции РФ
- брокер: T-Invest
- стратегия: intraday mean reversion
- режим: пулл минутных свечей
- риск: ограниченный, без плеча

## Запуск
1. Установить Python 3.10+
2. `pip install -r requirements.txt`
3. Задать переменную окружения INVEST_TOKEN
4. Скопировать config.yaml.example → config.yaml
5. `python main.py`

## ВАЖНО
Никогда не коммитьте API-токены.
