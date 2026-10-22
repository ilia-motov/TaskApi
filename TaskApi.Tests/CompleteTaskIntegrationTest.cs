using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;

namespace TaskApi.Tests;

public sealed class CompleteTaskIntegrationTest
{
    [Fact]
    public async Task CreateAndComplete_PersistsTaskAndPublishesEvent()
    {
        await using var postgres = new PostgreSqlBuilder("postgres:17-alpine").Build();
        await using var rabbit = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
        await Task.WhenAll(postgres.StartAsync(), rabbit.StartAsync());

        await using var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Tasks"] = postgres.GetConnectionString(),
                ["RabbitMq:HostName"] = rabbit.Hostname,
                ["RabbitMq:Port"] = rabbit.GetMappedPublicPort(5672).ToString(),
                ["RabbitMq:UserName"] = "rabbitmq",
                ["RabbitMq:Password"] = "rabbitmq"
            })));

        // Очередь и consumer нужны только тесту; приложение публикует в exchange.
        var connectionFactory = new ConnectionFactory { Uri = new Uri(rabbit.GetConnectionString()) };
        await using var connection = await connectionFactory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync(RabbitMqService.Exchange, ExchangeType.Topic, durable: true);
        var queue = await channel.QueueDeclareAsync();
        await channel.QueueBindAsync(queue.QueueName, RabbitMqService.Exchange, RabbitMqService.RoutingKey);

        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, args) =>
        {
            received.TrySetResult(args.Body.ToArray());
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer);

        using var client = factory.CreateClient();
        var json = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            Converters = { new JsonStringEnumConverter<Priority>() }
        };
        var createdResponse = await client.PostAsJsonAsync("/tasks", new { title = "Buy milk", priority = "High" });
        Assert.Equal(HttpStatusCode.Created, createdResponse.StatusCode);
        var created = (await createdResponse.Content.ReadFromJsonAsync<TaskItem>(json))!;
        Assert.False(created.IsCompleted);
        Assert.Null(created.CompletedAt);

        var completedResponse = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completedResponse.StatusCode);
        var completed = (await completedResponse.Content.ReadFromJsonAsync<TaskItem>(json))!;
        Assert.True(completed.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        var body = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        using var payload = JsonDocument.Parse(body);
        Assert.Equal("High", payload.RootElement.GetProperty("priority").GetString());
        var message = JsonSerializer.Deserialize<TaskCompletedEvent>(body, json)!;
        Assert.Equal(created.Id, message.TaskId);
        Assert.Equal("Buy milk", message.Title);
        Assert.Equal(completed.CompletedAt, message.CompletedAt);

        var stored = Assert.Single((await client.GetFromJsonAsync<TaskItem[]>("/tasks", json))!);
        Assert.True(stored.IsCompleted);
        // PostgreSQL хранит микросекунды, .NET DateTimeOffset — тики по 100 нс.
        Assert.InRange((stored.CompletedAt!.Value - message.CompletedAt).Duration(), TimeSpan.Zero, TimeSpan.FromMicroseconds(1));
        var repeated = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.Conflict, repeated.StatusCode);
    }
}
