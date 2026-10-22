using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public sealed class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();
        task.HasKey(x => x.Id);
        task.Property(x => x.Title).IsRequired().HasMaxLength(200);
        // Npgsql отображает uint RowVersion на системную колонку PostgreSQL xmin.
        task.Property(x => x.RowVersion).IsRowVersion();
    }
}
