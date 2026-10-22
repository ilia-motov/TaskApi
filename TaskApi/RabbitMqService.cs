using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TaskApi;

public interface ITaskEventPublisher
{
    Task PublishAsync(TaskCompletedEvent message);
}

public sealed class RabbitMqOptions
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "taskapi";
    public string Password { get; set; } = "taskapi";
}

public sealed class RabbitMqService(IOptions<RabbitMqOptions> options, ILogger<RabbitMqService> logger)
    : ITaskEventPublisher, IHostedService
{
    public const string Exchange = "task.events";
    public const string RoutingKey = "task.completed";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter<Priority>() }
    };

    // Один канал нельзя использовать для одновременных публикаций.
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;
    private volatile bool _stopping;

    // Подключаемся лениво: недоступный RabbitMQ не мешает запуску API.
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task PublishAsync(TaskCompletedEvent message)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var entered = false;
        try
        {
            await _gate.WaitAsync(timeout.Token);
            entered = true;
            if (_stopping)
                throw new InvalidOperationException("RabbitMQ publisher is stopping.");

            await EnsureConnectedAsync(timeout.Token);
            await _channel!.BasicPublishAsync(
                exchange: Exchange,
                routingKey: RoutingKey,
                mandatory: false,
                basicProperties: new BasicProperties { ContentType = "application/json", Persistent = true },
                body: json,
                cancellationToken: timeout.Token);
        }
        catch (Exception ex)
        {
            // По ТЗ: завершение в БД сохраняется, неотправленное событие остаётся в логе.
            logger.LogWarning(ex, "Could not publish task.completed. Message: {Message}",
                System.Text.Encoding.UTF8.GetString(json));
        }
        finally
        {
            if (entered)
                _gate.Release();
        }
    }

    private async Task EnsureConnectedAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true } && _connection is { IsOpen: true })
            return;

        await CloseResourcesAsync(ct);
        var settings = options.Value;
        var factory = new ConnectionFactory
        {
            HostName = settings.HostName,
            Port = settings.Port,
            UserName = settings.UserName,
            Password = settings.Password,
            AutomaticRecoveryEnabled = false,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(2)
        };
        _connection = await factory.CreateConnectionAsync(ct);
        _channel = await _connection.CreateChannelAsync(cancellationToken: ct);
        await _channel.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true, cancellationToken: ct);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping = true;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var entered = false;
        try
        {
            // Даём начатой публикации закончиться, затем закрываем канал и соединение.
            await _gate.WaitAsync(timeout.Token);
            entered = true;
            await CloseResourcesAsync(timeout.Token);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "RabbitMQ shutdown did not finish within the allowed time.");
        }
        finally
        {
            if (entered)
                _gate.Release();
        }
    }

    private async Task CloseResourcesAsync(CancellationToken ct)
    {
        var channel = _channel;
        var connection = _connection;
        _channel = null;
        _connection = null;

        try
        {
            if (channel is not null)
            {
                try
                {
                    if (channel.IsOpen)
                        await channel.CloseAsync(cancellationToken: ct);
                }
                finally
                {
                    await channel.DisposeAsync().AsTask().WaitAsync(ct);
                }
            }
        }
        finally
        {
            if (connection is not null)
            {
                try
                {
                    if (connection.IsOpen)
                        await connection.CloseAsync(cancellationToken: ct);
                }
                finally
                {
                    await connection.DisposeAsync().AsTask().WaitAsync(ct);
                }
            }
        }
    }
}
