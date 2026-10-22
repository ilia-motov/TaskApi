# TaskApi

MVP по тестовому заданию: .NET 9, ASP.NET Core Minimal API, PostgreSQL + EF Core, RabbitMQ.
Обработчики находятся в `TaskApi/Program.cs`.

## Запуск

Нужны .NET SDK 9 и запущенный Docker с Linux-контейнерами. Из папки решения:

```sh
docker compose up -d --wait
dotnet run --project TaskApi --launch-profile http
```

Compose запускает PostgreSQL и полноценный RabbitMQ с интерфейсом управления; API запускается отдельно.

- API: http://localhost:5052
- RabbitMQ Management: http://localhost:15679, логин/пароль `taskapi` / `taskapi`.
- Подключения настраиваются в `TaskApi/appsettings.json` или через переменные окружения, например `ConnectionStrings__Tasks` и `RabbitMq__HostName`.

PostgreSQL нужен при старте. Таблица создаётся через `EnsureCreatedAsync`; миграций нет, существующая схема автоматически не обновляется.

## API

| Метод | URL | Ответы |
| --- | --- | --- |
| POST | `/tasks` | 201; 400 при невалидном запросе |
| GET | `/tasks` | 200, все задачи |
| PUT | `/tasks/{id}/complete` | 200; 404; 409 при повторном или конкурентном завершении |
| DELETE | `/tasks/{id}` | 204; 404 |

Тело POST: `{"title":"Buy milk","priority":"High"}`.
`title` обязателен, не пустой и не из одних пробелов, максимум 200 символов.
`priority`: `Low`, `Medium`, `High`; по умолчанию `Medium`. Некорректные enum и GUID возвращают 400.
`CompletedAt` заполняется только при завершении.

Быстрая проверка в PowerShell:

```powershell
$api = 'http://localhost:5052'
$task = Invoke-RestMethod "$api/tasks" -Method Post -ContentType application/json -Body '{"title":"Buy milk","priority":"High"}'
Invoke-RestMethod "$api/tasks/$($task.id)/complete" -Method Put
Invoke-RestMethod "$api/tasks"
Invoke-RestMethod "$api/tasks/$($task.id)" -Method Delete
```

## Конкурентность и RabbitMQ

`RowVersion` использует PostgreSQL `xmin` через `IsRowVersion()`. Конфликт сохранения возвращает 409; повторное завершение не меняет дату и не публикует событие.

После сохранения БД публикуется JSON с полями `taskId`, `title`, `completedAt`, `priority` в durable topic exchange `task.events`, routing key `task.completed`.
HTTP-запрос асинхронно ожидает попытку отправки с общим таймаутом 3 секунды. При остановке на ожидание активной отправки и закрытие ресурсов отведено 5 секунд.

Publisher confirms опущены в рамках разрешённого упрощения ТЗ. Ошибки отправки логируются вместе с JSON события; задача остаётся завершённой независимо от доступности RabbitMQ.
Outbox и повторной доставки нет: сохранение БД и публикация не атомарны, событие может потеряться.

Для просмотра сообщения в RabbitMQ Management **до завершения задачи** создайте:

1. Exchange `task.events`: type `topic`, durable `true`, auto delete `false`.
2. Очередь, например `task.completed.debug`.
3. Binding от exchange к очереди с routing key `task.completed`.

API создаёт exchange лениво при первой публикации, очереди и consumer не создаёт. Без подходящего binding сообщение не сохранится в очереди.

## Интеграционный тест

```sh
dotnet test TaskApi.sln
```

Нужен запущенный Docker; предварительно запускать Compose и API не требуется.
Testcontainers поднимает отдельные PostgreSQL и RabbitMQ, затем удаляет их.
Тест проверяет создание и завершение через HTTP, получение реального сообщения consumer, состояние через GET и 409 при повторном завершении.

## Остановка

API: Ctrl+C. Инфраструктура: `docker compose down`.
Том PostgreSQL сохраняется. Постоянный том RabbitMQ не настроен; после пересоздания брокера объекты для наблюдения нужно создать заново.
