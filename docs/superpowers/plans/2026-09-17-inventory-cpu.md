# Inventory CPU Reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking. Execute sequentially in the current session; this small change does not need subagents.

**Goal:** Снизить CPU отправителя при повторной сверке накопленной истории, сохранив прежние дневные digest и восстановление потерянных сервером событий.

**Architecture:** `Collector._inventory()` собирает дневные digest из существующих колонок `events.day`, `events.response_id`, `events.digest`. Проверка и хеширование события остаются при добавлении в ledger; общая функция `day_inventory()` остаётся эталоном и используется сервером. Формат протокола и расписание сверок сохраняются.

**Tech Stack:** Python 3.11+, stdlib, SQLite, unittest; PowerShell на Windows.

---

## Обоснование и границы

- Исходная версия: `bf402d77062395e479d97ad5205cc2532315452c`, ветка `durable-delivery`, рабочий каталог `C:\N\hse\codex-usage-lab`.
- Подтверждённая причина: `_inventory()` читает JSON всех событий и вызывает `day_inventory()`. Внутри неё каждое событие проходит `canonical_event()` дважды, хотя его digest уже записан в ledger.
- Предварительный замер на ASUS, 100 000 синтетических событий, одна сверка без сети: 5,31 с wall / 4,875 с CPU исходно; 0,232 с wall / 0,172 с CPU при использовании сохранённых digest. Результаты совпали. Это замер прототипа, не готовой реализации и не полного сетевого цикла.
- Изменения реализации: только `codex_usage.py` и `test_codex_usage.py`. Этот файл — запрошенный план.
- Обход сессий, проверка изменённого JSONL-префикса, сервер, схема БД, интервал 30 секунд, повторные сверки, ACK и активация сессий остаются в текущем виде.
- Расчёт по-прежнему проходит по всем событиям и сортирует записи внутри дня. Кеш дней и дополнительные индексы сейчас не требуются; необходимость следующих изменений определяется повторным профилем полного цикла.
- Предположение: локальный ledger сохраняет согласованность `data` и `digest`, которые записываются вместе после валидации и далее не меняются приложением. Новая сверка использует сохранённый digest и не перепроверяет JSON локальной строки. Обнаружение произвольного изменения `data` сторонним процессом не входит в гарантию этой оптимизации.

## Task 1: Зафиксировать совместимость результата

**Files:**
- Modify: `test_codex_usage.py`, класс `SenderTests` рядом с `test_canonical_validation_and_stable_daily_digest`.

- [x] Добавить один тест ниже. Он проверяет пустой ledger, несколько дней, разные ACK, порядок ID с общим префиксом и Unicode, добавление новых данных и повторное открытие базы. Сравнение выполняется с прежней `day_inventory()` через существующий локальный HTTP receiver.

```python
    def test_inventory_matches_reference_across_days_and_restart(self):
        batches = (
            [],
            [event("resp:z"), event("resp"), event("ответ"),
             event("next-day", timestamp_ms=1789689601000)],
            [event("later-same-day"),
             event("third-day", timestamp_ms=1789776001000)],
        )
        for batch in batches:
            with self.sender.db:
                for index, raw in enumerate(batch):
                    item = usage.canonical_event(raw)
                    self.sender.db.execute(
                        "INSERT INTO events(response_id,session,day,digest,data,total_tokens,acked) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (item["response_id"], item["session"], item["event_timestamp"][:10],
                         usage.event_digest(item), usage.compact(item),
                         item["total_tokens"], index % 2))
                    self.state["events"][item["response_id"]] = item
            expected = usage.day_inventory(self.state["events"].values())
            for _ in range(2):
                self.assertEqual(self.sender._inventory(), [])
                self.assertEqual(self.state["requests"][-1]["days"], expected)
                self.sender.close()
                self.sender = self.make_sender()
```

- [x] Запустить тест до изменения реализации:

```powershell
python -m unittest -v test_codex_usage.SenderTests.test_inventory_matches_reference_across_days_and_restart
```

Ожидается PASS: поведение должно сохраниться. Дефект производительности уже воспроизведён отдельным замером; искусственно ломающийся функциональный тест или нестабильный порог времени в unittest не добавлять.

## Task 2: Переиспользовать сохранённые digest

**Files:**
- Modify: `codex_usage.py`, начало `Collector._inventory()` (исходно строка 399).

- [x] Заменить единственную строку построения `days` следующим блоком:

```python
        grouped = {}
        for day, response_id, digest in self.db.execute("SELECT day,response_id,digest FROM events"):
            grouped.setdefault(day, []).append(response_id + ":" + digest + "\n")
        days = [dict(day=day, count=len(rows),
                     digest=hashlib.sha256("".join(sorted(rows)).encode()).hexdigest())
                for day, rows in sorted(grouped.items())]
```

Сортировать именно полные строки `response_id:digest\n`, как в прежнем алгоритме. Использовать все события независимо от `acked`: сверка должна обнаруживать потерю уже подтверждённой истории. Остальное тело метода оставить прежним.

- [x] Запустить тест совместимости повторно той же командой. Ожидается PASS.
- [x] Запустить существующие проверки клиента и реального receiver:

```powershell
python -m unittest -v test_codex_usage test_sync
```

Особенно проверить уже существующие тесты `test_lost_ack_replays_identical_event_after_restart`, `test_server_rollback_replays_acknowledged_ledger_and_activation`, `test_server_history_missing_from_local_ledger_is_not_synced` и `test_real_sender_recovers_lost_ack_and_empty_server_restore`. Они должны пройти без изменения ожиданий.

## Task 3: Измерить результат и проверить весь diff

**Files:** постоянных benchmark-скриптов и отчётов в репозитории не добавлять.

- [x] Одноразовым Python-замером сравнить исходную версию `bf402d7` и рабочий файл на одинаковой временной SQLite с 100 000 синтетических событий. Сетевой метод `_post` подменить локальной функцией: `inventory` возвращает пустые `resend_days`/`server_only_days`, `events` подтверждает переданные ID, `activate` подтверждает переданные сессии. Производственный endpoint не использовать.
- [x] Отдельно измерить `_inventory()` и `sync()` через `time.perf_counter()` и `time.process_time()`: один прогрев и три замера, сообщить медианы. При выполнении полный цикл исходной версии на 100 000 событий ограничен одним замером: фоновая нагрузка на ASUS существенно выросла, и повторение дорогого цикла создаёт ненужную нагрузку. Число замеров указывать явно. Генерацию данных и открытие базы исключить из интервала измерения. Сравнить полные массивы `days`, переданные в `_post`, и проверить нулевую ошибку синхронизации.
- [x] На отдельной копии актуального локального ledger, сделанной через SQLite backup из read-only соединения, измерить `scan()` и `sync()` до/после с той же подменённой сетью. Читать исходные журналы допустимо; менять можно только временную базу. Не выводить config, секреты и тексты сессий.
- [x] Критерии приёмки: дневные массивы совпадают полностью; CPU одной сверки на 100 000 событий снижается минимум в 10 раз относительно исходной версии на той же машине; полный локальный цикл без сети становится быстрее; тесты восстановления проходят. При несоблюдении критерия сначала разобрать профиль, а не автоматически расширять diff.
- [x] Выполнить полный набор тестов из `AGENTS.md`:

```powershell
python -m unittest -v test_server test_analytics test_pricing test_sync test_codex_usage
git diff --check
git diff --stat
git diff -- codex_usage.py test_codex_usage.py
```

Ожидается PASS всех тестов и отсутствие ошибок whitespace. Проверить, что каждая новая строка реализации нужна для сборки прежнего digest из существующих данных.
- [x] Показать итоговый diff, результаты тестов, замеры CPU/wall и ограничения: остаются чтение сохранённых digest, хеширование изменённых журналов и текущая стоимость серверной сверки.

## Обновление установленного отправителя

После реализации и проверки отдельным шагом обновить отправитель штатным `python codex_usage.py install`, затем выполнить `doctor` и `once`. Установщик делает backup и перезапускает только отправитель. Проверить сохранение `collector_id`/границ истории, совпадение SHA-256 исходного и установленного скриптов, `database_check=ok`, `backfill_pending=false`, отсутствие ошибок и опустошение очереди после доставки. Учесть, что `install` повторяет проверку истории: её стоимость не является стоимостью пустого фонового цикла.

Пользователь одобрил выполнение плана. Реализация и локальное обновление выполняются после проверок; изменения Git публикуются отдельной веткой для ревью. Merge и выпуск на Aeza остаются отдельными действиями после проверки конкретного diff; действующий сервер не нуждается в изменении протокола.


## Проверено при выполнении 17 сентября 2026

- Исходная версия: 95 тестов PASS. После изменения: 96 тестов PASS; новый тест совместимости проходил и до изменения алгоритма.
- Реализация: 6 строк вместо одной; один тест на 26 строк. Протокол, сервер и схема БД прежние.
- Итоговый замер одной сверки: 100 000 событий, одинаковая SQLite, один прогрев и три чередующиеся пары old/new. Медиана wall: 4,778 → 0,263 с; CPU: 4,344 → 0,234 с, снижение CPU в 18,53 раза. Все дневные массивы совпали.
- Полная синхронизация без сети на 100 000 событий и 1000 файлов: old 12,55 с wall / 10,31 с CPU (один запуск), new 3,29 / 3,14 с (медиана трёх). Фоновая нагрузка менялась, поэтому точный коэффициент ускорения полного цикла по этому замеру не заявляется.
- На копии текущего ledger дневные массивы old/new также совпали; медиана sync без сети: 1,95 → 0,77 с wall, 1,83 → 0,625 с CPU.
- Первую серию с аномально медленным baseline (58,61 с wall на сверку) исключили из итогового коэффициента; проверили результат чередующимися замерами после стабилизации времени.
- Одноразовые скрипты и результаты находятся вне Git: `%USERPROFILE%\.codex\tmp\usage-perf-20260917`. Измерения не обращались к production и не меняли рабочий ledger.
- Локальный отправитель обновлён штатным `install`; `doctor` и `once` успешны: SQLite `ok`, очередь 0, ошибок нет, `backfill_pending=false`. Идентичность и границы истории сохранены; все 11 056 прежних записей сохранены без изменений. 39 прежних исключений (33 foreign_machine, 6 unknown_owner) не изменились.
