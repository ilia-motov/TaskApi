using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<TaskDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Tasks")));

builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter<Priority>(allowIntegerValues: false)));

builder.Services.AddProblemDetails();

builder.Services.Configure<RouteHandlerOptions>(options => options.ThrowOnBadRequest = false);
builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection("RabbitMq"));
builder.Services.AddSingleton<RabbitMqService>();
builder.Services.AddSingleton<ITaskEventPublisher>(sp => sp.GetRequiredService<RabbitMqService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqService>());

var app = builder.Build();
app.UseExceptionHandler();

// Для MVP создаём схему при первом запуске. Изменения схемы потребуют миграций.
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
    await db.Database.EnsureCreatedAsync();
}

app.MapPost("/tasks", async (CreateTaskRequest request, TaskDbContext db, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.Title) || request.Title.Length > 200)
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["title"] = ["Title must contain 1 to 200 characters and cannot be blank."]
        });

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = request.Title,
        Priority = request.Priority,
        CreatedAt = DateTimeOffset.UtcNow
    };
    db.Tasks.Add(task);
    await db.SaveChangesAsync(ct);
    return Results.Created("/tasks", task);
});

app.MapGet("/tasks", async (TaskDbContext db, CancellationToken ct) =>
    await db.Tasks.AsNoTracking().ToListAsync(ct));

app.MapPut("/tasks/{id}/complete", async (Guid id, TaskDbContext db, ITaskEventPublisher publisher, CancellationToken ct) =>
{
    var task = await db.Tasks.FindAsync([id], ct);
    if (task is null)
        return Results.NotFound();
    if (task.IsCompleted)
        return Results.Conflict();

    task.IsCompleted = true;
    task.CompletedAt = DateTimeOffset.UtcNow;

    try
    {
        await db.SaveChangesAsync(ct);
    }
    catch (DbUpdateConcurrencyException)
    {
        return Results.Conflict();
    }

    // БД уже сохранена. Отключение HTTP-клиента не должно отменять публикацию.
    await publisher.PublishAsync(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt.Value, task.Priority));
    return Results.Ok(task);
});

app.MapDelete("/tasks/{id}", async (Guid id, TaskDbContext db, CancellationToken ct) =>
{
    var deleted = await db.Tasks.Where(task => task.Id == id).ExecuteDeleteAsync(ct);
    return deleted == 0 ? Results.NotFound() : Results.NoContent();
});

app.Run();

public partial class Program;
