namespace MessageQueue
{
    public interface IPersistentStorage<T>
    {
        Task WriteAsync(T item, CancellationToken cancellationToken = default);
        Task<T?> ReadAsync(CancellationToken cancellationToken = default);
        Task<bool> HasDataAsync(CancellationToken cancellationToken = default);
        Task ClearAsync(CancellationToken cancellationToken = default);
    }
}