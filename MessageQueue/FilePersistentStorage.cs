using System.Collections.Concurrent;
using System.Text.Json;

namespace MessageQueue
{
    public class FilePersistentStorage<T> : IPersistentStorage<T>, IDisposable
    {
        private readonly string _filePath;
        private readonly JsonSerializerOptions _jsonOptions;
        private volatile bool _disposed = false;
        private SemaphoreSlim? _fileSemaphore;
        private readonly object _disposeLock = new object();

        public FilePersistentStorage(string filePath)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            _fileSemaphore = new SemaphoreSlim(1, 1);
            _jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = false
            };

            // Ensure directory exists
            var directory = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
        }

        public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            var semaphore = GetSemaphoreOrThrow();

            await semaphore.WaitAsync(cancellationToken);
            try
            {
                ThrowIfDisposed(); // Double-check after acquiring lock

                var json = JsonSerializer.Serialize(item, _jsonOptions);
                await File.AppendAllTextAsync(_filePath, json + Environment.NewLine, cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async Task<T?> ReadAsync(CancellationToken cancellationToken = default)
        {
            var semaphore = GetSemaphoreOrThrow();

            await semaphore.WaitAsync(cancellationToken);
            try
            {
                ThrowIfDisposed(); // Double-check after acquiring lock

                if (!File.Exists(_filePath))
                    return default(T);

                // More efficient approach: read and process line by line
                return await ReadFirstLineAndUpdateFileAsync(cancellationToken);
            }
            catch (JsonException)
            {
                // Handle corrupted data - could log this
                return default(T);
            }
            catch (IOException)
            {
                // Handle file I/O issues - could retry or log
                return default(T);
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async Task<bool> HasDataAsync(CancellationToken cancellationToken = default)
        {
            var semaphore = GetSemaphoreOrThrow();

            await semaphore.WaitAsync(cancellationToken);
            try
            {
                ThrowIfDisposed(); // Double-check after acquiring lock

                if (!File.Exists(_filePath))
                    return false;

                // More efficient: just check if file has content without reading all lines
                using var reader = new StreamReader(_filePath);
                var firstLine = await reader.ReadLineAsync();
                return !string.IsNullOrWhiteSpace(firstLine);
            }
            catch (IOException)
            {
                return false;
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async Task ClearAsync(CancellationToken cancellationToken = default)
        {
            var semaphore = GetSemaphoreOrThrow();

            await semaphore.WaitAsync(cancellationToken);
            try
            {
                ThrowIfDisposed(); // Double-check after acquiring lock

                if (File.Exists(_filePath))
                {
                    await File.WriteAllTextAsync(_filePath, string.Empty, cancellationToken);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        private async Task<T?> ReadFirstLineAndUpdateFileAsync(CancellationToken cancellationToken)
        {
            // For better performance with large files, consider using a temporary file approach
            var lines = await File.ReadAllLinesAsync(_filePath, cancellationToken);
            if (lines.Length == 0)
                return default(T);

            var firstLine = lines[0];

            // Write remaining lines back
            if (lines.Length > 1)
            {
                var remainingLines = lines.Skip(1);
                await File.WriteAllLinesAsync(_filePath, remainingLines, cancellationToken);
            }
            else
            {
                await File.WriteAllTextAsync(_filePath, string.Empty, cancellationToken);
            }

            return JsonSerializer.Deserialize<T>(firstLine, _jsonOptions);
        }

        private SemaphoreSlim GetSemaphoreOrThrow()
        {
            ThrowIfDisposed();
            var semaphore = _fileSemaphore;
            if (semaphore == null)
                throw new ObjectDisposedException(nameof(FilePersistentStorage<T>));
            return semaphore;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FilePersistentStorage<T>));
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    lock (_disposeLock)
                    {
                        if (!_disposed) // Double-check locking pattern
                        {
                            _fileSemaphore?.Dispose();
                            _fileSemaphore = null;
                            _disposed = true;
                        }
                    }
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}