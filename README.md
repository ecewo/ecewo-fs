# File Operations

ecewo provides asynchronous file I/O operations using libuv's native API. All operations are non-blocking and run on libuv's thread pool, with automatic memory management through arena allocators and Node.js-style error-first callbacks.

## Table of Contents

0. [Installation](#installation)
1. [Quick Start](#quick-start)
    1. [Reading A File](#reading-a-file)
    2. [Writing A File](#writing-a-file)
2. [File Paths & Working Directory](#file-paths--working-directory)
    1. [Project Structure Example](#project-structure-example)
    2. [CMake Static Files Setup](#cmake-static-files-setup)
    3. [Running From Project Root](#running-from-project-root)
    4. [Running From Build Directory](#running-from-build-directory)
3. [API Reference](#api-reference)
    1. [`fs_read_file()`](#fs_read_file)
    2. [`fs_write_file()`](#fs_write_file)
    3. [`fs_append_file()`](#fs_append_file)
    4. [`fs_stat()`](#fs_stat)
    5. [`fs_unlink()`](#fs_unlink)
    6. [`fs_rename()`](#fs_rename)
    7. [`fs_mkdir()`](#fs_mkdir)
    8. [`fs_rmdir()`](#fs_rmdir)
4. [Advanced Examples](#advanced-examples)
    1. [Sequential File Operations](#sequential-file-operations)
    2. [Parallel File Operations](#parallel-file-operations)
    3. [File Upload Example](#file-upload-example)
5. [Memory Management](#memory-management)
6. [Error Handling](#error-handling)
7. [Common Error Codes](#common-error-codes)

> [!IMPORTANT]
>
> File operations use I/O-bound async (libuv), not CPU-bound workers. The main thread is never blocked.

## Installation

Add to your `CMakeLists.txt`:

```sh
ecewo_plugin(fs)

target_link_libraries(app PRIVATE
    ecewo::ecewo
    ecewo::fs
)
```

Initialize the file system module in your application:

```c
#include "ecewo.h"
#include "ecewo-fs.h"

int main(void) {
    server_init();
    
    // Initialize file system module
    if (fs_init() != 0) {
        fprintf(stderr, "Failed to initialize fs module\n");
        return 1;
    }
    
    // Your routes...
    get("/file", read_handler);
    
    // Register cleanup handler
    server_atexit(fs_cleanup);
    
    server_listen(3000);
    server_run();
    return 0;
}
```

## Quick Start

### Reading A File

```c
#include "ecewo.h"
#include "ecewo-fs.h"

static void on_file_read(const char *error, const char *data, size_t size, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    // data is allocated in req->arena - no need to free
    printf("Read %zu bytes\n", size);
    set_header(res, "Content-Type", "text/plain");
    reply(res, OK, data, size);
}

void read_handler(Req *req, Res *res) {
    // Read the public/data.txt
    // Pass req->arena for automatic memory management
    fs_read_file("public/data.txt", req->arena, on_file_read, res);
}
```

### Writing A File

```c
static void on_file_written(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Saved!");
}

void save_handler(Req *req, Res *res) {
    const char *content = req->body;
    fs_write_file("public/output.txt", content, strlen(content), on_file_written, res);
}
```

## File Paths & Working Directory

All file paths are relative to the directory where the server executable is run, not where the executable file is located.

```bash
# If you run server from project root:
/home/user/myproject$ ./build/server

# File paths are relative to /home/user/myproject/
# "data.txt" -> /home/user/myproject/data.txt
# "./logs/app.log" -> /home/user/myproject/logs/app.log
```

```bash
# If you run server from build folder:
/home/user/myproject/build$ ./server

# File paths are relative to /home/user/myproject/build/
# "data.txt" -> /home/user/myproject/build/data.txt
# "./logs/app.log" -> /home/user/myproject/build/logs/app.log
```

```c
// Relative to current working directory
fs_read_file("data.txt", req->arena, callback, user_data);           // ./data.txt
fs_read_file("logs/app.log", req->arena, callback, user_data);       // ./logs/app.log
fs_read_file("./config/settings.json", req->arena, callback, user_data); // ./config/settings.json
```

### Project Structure Example

```sh
myproject/
├── build/
│   ├── server          # Executable
│   └── public/         # Copied by CMake (see below)
│       ├── index.html
│       └── style.css
├── public/             # Source static files
│   ├── index.html
│   └── style.css
├── data/               # Data files
│   └── users.json
├── logs/               # Log files
│   └── app.log
├── uploads/            # User uploads
├── CMakeLists.txt
└── main.c
```

### CMake Static Files Setup

Add this to your `CMakeLists.txt` to automatically copy/symlink the `public/` directory:

```sh
# Platform-aware public directory handling
if(WIN32)
    # Windows: Copy directory (symlinks require admin privileges)
    add_custom_command(TARGET server POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_SOURCE_DIR}/public
            ${CMAKE_BINARY_DIR}/public
        COMMENT "Copying public directory to build folder"
    )
else()
    # Linux/Mac: Create symlink (faster, no duplication)
    add_custom_command(TARGET server POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${CMAKE_SOURCE_DIR}/public
            ${CMAKE_BINARY_DIR}/public
        COMMENT "Creating symlink to public directory"
    )
endif()
```

**What this does:**

- **Windows:** Copies `public/` -> `build/public/` on every build
- **Linux/Mac:** Creates symlink `build/public/` -> `../public/` (one-time)

Server can be run from either project root or build directory now, because static files are accessible via `public/` in both locations.

### Running From Project Root

```bash
cd /home/user/myproject
./build/server
```

**File paths in code:**

```c
// All relative to /home/user/myproject/

fs_read_file("data/users.json", req->arena, callback, user_data);
// -> /home/user/myproject/data/users.json

fs_write_file("logs/app.log", data, len, callback, user_data);
// -> /home/user/myproject/logs/app.log

fs_write_file("uploads/photo.jpg", img, size, callback, user_data);
// -> /home/user/myproject/uploads/photo.jpg
```

### Running From Build Directory

```bash
cd /home/user/myproject/build
./server
```

**File paths in code:**

```c
// All relative to /home/user/myproject/build/

fs_read_file("../data/users.json", req->arena, callback, user_data);
// -> /home/user/myproject/data/users.json

fs_write_file("../logs/app.log", data, len, callback, user_data);
// -> /home/user/myproject/logs/app.log

fs_write_file("../uploads/photo.jpg", img, size, callback, user_data);
// -> /home/user/myproject/uploads/photo.jpg
```

## API Reference

### `fs_read_file()`

Read entire file into memory asynchronously.

```c
int fs_read_file(const char *path, Arena *arena, fs_read_callback_t callback, void *user_data);
```

**Parameters:**

- `path`: File path to read
- `arena`: Arena allocator for file data (pass `req->arena` or `NULL` for malloc)
- `callback`: Function called when operation completes
- `user_data`: User context pointer (usually `Req*` or `Res*`)

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected (too many concurrent operations or invalid arguments)

**Callback Signature:**

```c
typedef void (*fs_read_callback_t)(
    const char *error,
    const char *data,
    size_t size,
    void *user_data
);
```

**Callback Parameters:**

- `error`: Error message on failure, `NULL` on success (valid only during callback)
- `data`: File content on success (see memory management below)
- `size`: File size in bytes
- `user_data`: The context pointer you passed

**Memory Management:**

- If `arena != NULL`: `data` is allocated in the arena and freed automatically with the arena
- If `arena == NULL`: `data` is `malloc`'d and **caller must call `free(data)`**

**Example with arena (recommended):**

```c
static void on_read(const char *error, const char *data, size_t size, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        char *msg = arena_sprintf(res->arena, "Error: %s", error);
        send_text(res, 500, msg);
        return;
    }
    
    printf("Read %zu bytes\n", size);
    send_text(res, 200, data);
    // No free() needed - data is in req->arena
}

void handler(Req *req, Res *res) {
    fs_read_file("config.json", req->arena, on_read, res);
}
```

**Example with malloc:**

```c
static void on_read(const char *error, const char *data, size_t size, void *user_data) {
    if (error) {
        printf("Error: %s\n", error);
        return;
    }
    
    printf("Read: %.*s\n", (int)size, data);
    free((void *)data);  // MUST FREE when using NULL arena
}

void somewhere() {
    fs_read_file("data.txt", NULL, on_read, NULL);
}
```

### `fs_write_file()`

Write data to file asynchronously (creates or truncates).

```c
int fs_write_file(const char *path, const void *data, size_t size,
                  fs_write_callback_t callback, void *user_data);
```

**Parameters:**

- `path`: File path to write
- `data`: Data to write (copied internally, safe to free after call returns)
- `size`: Data size in bytes
- `callback`: Completion callback
- `user_data`: User context pointer

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Callback Signature:**

```c
typedef void (*fs_write_callback_t)(const char *error, void *user_data);
```

**Callback Parameters:**

- `error`: Error message on failure, `NULL` on success (valid only during callback)
- `user_data`: The context pointer you passed

**Example:**

```c
static void on_saved(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_json(res, 200, "{\"status\":\"saved\"}");
}

void save_handler(Req *req, Res *res) {
    const char *json = "{\"status\":\"active\"}";
    fs_write_file("status.json", json, strlen(json), on_saved, res);
}
```

### `fs_append_file()`

Append data to file asynchronously (creates if doesn't exist).

```c
int fs_append_file(const char *path, const void *data, size_t size,
                   fs_write_callback_t callback, void *user_data);
```

**Parameters:**

Same as `fs_write_file()`

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Example:**

```c
static void on_logged(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Logged");
}

void log_handler(Req *req, Res *res) {
    char *log = arena_sprintf(req->arena, "[%ld] %s\n", time(NULL), req->body);
    fs_append_file("app.log", log, strlen(log), on_logged, res);
}
```

### `fs_stat()`

Get file statistics asynchronously.

```c
int fs_stat(const char *path, fs_stat_callback_t callback, void *user_data);
```

**Parameters:**

- `path`: File path to stat
- `callback`: Completion callback
- `user_data`: User context pointer

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Callback Signature:**

```c
typedef void (*fs_stat_callback_t)(
    const char *error,
    const uv_stat_t *stat,
    void *user_data
);
```

**Callback Parameters:**

- `error`: Error message on failure, `NULL` on success (valid only during callback)
- `stat`: File statistics on success (valid only during callback)
- `user_data`: The context pointer you passed

**Available stat fields:**

- `st_size`: File size in bytes
- `st_mtim`: Last modification time
- `st_atim`: Last access time
- `st_ctim`: Creation time
- `st_mode`: File permissions

**Example:**

```c
static void on_stat(const char *error, const uv_stat_t *stat, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 404, "File not found");
        return;
    }
    
    char *response = arena_sprintf(res->arena,
        "{"
        "\"size\":%lld,"
        "\"modified\":%lld"
        "}",
        (long long)stat->st_size,
        (long long)stat->st_mtim.tv_sec
    );
    
    send_json(res, 200, response);
}

void info_handler(Req *req, Res *res) {
    const char *path = get_query(req, "path");
    fs_stat(path, on_stat, res);
}
```

### `fs_unlink()`

Delete file asynchronously.

```c
int fs_unlink(const char *path, fs_write_callback_t callback, void *user_data);
```

**Parameters:**

- `path`: File path to delete
- `callback`: Completion callback (same as write callback)
- `user_data`: User context pointer

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Example:**

```c
static void on_deleted(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Deleted");
}

void delete_handler(Req *req, Res *res) {
    const char *file = get_query(req, "file");
    
    fs_unlink(file, on_deleted, res);
}
```

### `fs_rename()`

Rename or move file asynchronously.

```c
int fs_rename(const char *old_path, const char *new_path,
              fs_write_callback_t callback, void *user_data);
```

**Parameters:**

- `old_path`: Current file path
- `new_path`: New file path
- `callback`: Completion callback
- `user_data`: User context pointer

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Example:**

```c
static void on_renamed(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Renamed");
}

void rename_handler(Req *req, Res *res) {
    fs_rename("old.txt", "new.txt", on_renamed, res);
}
```

### `fs_mkdir()`

Create directory asynchronously.

```c
int fs_mkdir(const char *path, fs_write_callback_t callback, void *user_data);
```

**Parameters:**

- `path`: Directory path to create
- `callback`: Completion callback
- `user_data`: User context pointer

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Example:**

```c
static void on_dir_created(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Created");
}

void create_dir_handler(Req *req, Res *res) {
    fs_mkdir("uploads", on_dir_created, res);
}
```

### `fs_rmdir()`

Remove empty directory asynchronously.

```c
int fs_rmdir(const char *path, fs_write_callback_t callback, void *user_data);
```

**Parameters:**

- `path`: Directory path to remove (must be empty)
- `callback`: Completion callback
- `user_data`: User context pointer

**Returns:**
- `0` if operation was queued successfully
- `-1` if operation was rejected

**Example:**

```c
static void on_dir_removed(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Removed");
}

void remove_dir_handler(Req *req, Res *res) {
    fs_rmdir("temp", on_dir_removed, res);
}
```

## Advanced Examples

### Sequential File Operations

```c
// Step 2: Write processed content
static void on_written(const char *error, void *user_data) {
    Res *res = (Res *)user_data;

    if (error) {
        send_text(res, 500, error);
        return;
    }

    send_text(res, 200, "Processed and saved");
}

// Step 1: Read and process
static void on_read(const char *error, const char *data, size_t size, void *user_data) {
    Res *res = (Res *)user_data;

    if (error) {
        send_text(res, 500, error);
        return;
    }

    // Process file content (data is in arena, no need to free)
    char *processed = arena_sprintf(res->arena, "PROCESSED: %s", data);

    // Write processed content
    fs_write_file("output.txt", processed, strlen(processed), on_written, res);
}

void process_handler(Req *req, Res *res) {
    // Read, process, and write
    fs_read_file("public/input.txt", req->arena, on_read, res);
}
```

### Parallel File Operations

```c
#include "ecewo.h"
#include "ecewo-fs.h"

typedef struct
{
    Res *res;
    int completed;
    int total;
    char *file1_data;
    char *file2_data;
    char *file3_data;
} ParallelContext;

static void send_combined_response(ParallelContext *ctx) {
    if (ctx->completed == ctx->total) {
        char *response = arena_sprintf(ctx->res->arena,
            "{"
            "\"file1\":\"%s\","
            "\"file2\":\"%s\","
            "\"file3\":\"%s\""
            "}",
            ctx->file1_data ? ctx->file1_data : "error",
            ctx->file2_data ? ctx->file2_data : "error",
            ctx->file3_data ? ctx->file3_data : "error"
        );

        send_json(ctx->res, 200, response);
    }
}

static void on_file1(const char *error, const char *data, size_t size, void *user_data) {
    ParallelContext *ctx = (ParallelContext *)user_data;

    if (!error) {
        // data is already in arena, just save pointer
        ctx->file1_data = (char *)data;
    }

    ctx->completed++;
    send_combined_response(ctx);
}

static void on_file2(const char *error, const char *data, size_t size, void *user_data) {
    ParallelContext *ctx = (ParallelContext *)user_data;

    if (!error) {
        ctx->file2_data = (char *)data;
    }

    ctx->completed++;
    send_combined_response(ctx);
}

static void on_file3(const char *error, const char *data, size_t size, void *user_data) {
    ParallelContext *ctx = (ParallelContext *)user_data;

    if (!error) {
        ctx->file3_data = (char *)data;
    }

    ctx->completed++;
    send_combined_response(ctx);
}

void parallel_handler(Req *req, Res *res) {
    ParallelContext *ctx = arena_alloc(req->arena, sizeof(ParallelContext));
    ctx->res = res;
    ctx->completed = 0;
    ctx->total = 3;
    ctx->file1_data = NULL;
    ctx->file2_data = NULL;
    ctx->file3_data = NULL;

    // Start 3 parallel reads - all use req->arena for memory
    fs_read_file("public/file1.txt", req->arena, on_file1, ctx);
    fs_read_file("public/file2.txt", req->arena, on_file2, ctx);
    fs_read_file("public/file3.txt", req->arena, on_file3, ctx);
}
```

### File Upload Example

```c
static void on_uploaded(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        send_text(res, 500, error);
        return;
    }
    
    send_text(res, 200, "Uploaded");
}

void upload_handler(Req *req, Res *res) {
    // Get filename from request params
    const char *filename = get_param(req, "filename");
    if (!filename) {
        send_text(res, 400, "Missing filename");
        return;
    }
    
    // Build safe path
    char *filepath = arena_sprintf(req->arena, "uploads/%s", filename);
    
    // Save uploaded file (data is copied internally, req->body remains valid)
    fs_write_file(filepath, req->body, req->body_len, on_uploaded, res);
}
```

## Memory Management

ecewo-fs provides flexible memory management through arena allocators:

### Read Operations

**With arena (recommended):**
```c
void handler(Req *req, Res *res) {
    // Pass req->arena - file data will be automatically freed
    fs_read_file("data.txt", req->arena, on_read, res);
}

static void on_read(const char *error, const char *data, size_t size, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (!error) {
        // Use data directly - no free() needed
        send_text(res, 200, data);
    }
    // data is automatically freed when request completes
}
```

**Without arena (manual management):**
```c
void background_task() {
    // Pass NULL - you must free the data yourself
    fs_read_file("data.txt", NULL, on_read, NULL);
}

static void on_read(const char *error, const char *data, size_t size, void *user_data) {
    if (!error) {
        printf("%.*s\n", (int)size, data);
        free((void *)data);  // MUST FREE
    }
}
```

### Write Operations

Write operations always manage memory internally:

```c
void handler(Req *req, Res *res) {
    const char *data = "Hello, World!";
    
    // Data is copied internally - safe to free immediately after call
    fs_write_file("out.txt", data, strlen(data), on_write, res);
    
    // data can be freed or go out of scope here - it's already copied
}
```

### Error Messages

Error messages in callbacks are **valid only during the callback**:

```c
static void on_read(const char *error, const char *data, size_t size, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        // WRONG: Saving pointer for later use
        // char *saved = error;  // Dangling pointer after callback
        
        // RIGHT: Copy if you need it later
        char *copy = arena_sprintf(res->arena, "Error: %s", error);
        
        // Or use immediately
        send_text(res, 500, error);
    }
}
```

### Key Points

- **Read with arena**: Data freed automatically with arena (recommended for request handlers)
- **Read without arena**: You must `free(data)` after use
- **Write operations**: Data is copied internally, you can free your buffer immediately
- **Error messages**: Valid only during callback, copy if needed later

## Error Handling

All callbacks follow the error-first pattern:

```c
static void on_operation(const char *error, void *user_data) {
    Res *res = (Res *)user_data;
    
    if (error) {
        // Error occurred
        // error contains: "ENOENT: no such file or directory"
        
        if (strstr(error, "ENOENT")) {
            send_text(res, 404, "File not found");
        }
        else if (strstr(error, "EACCES")) {
            send_text(res, 403, "Permission denied");
        }
        else if (strstr(error, "EISDIR")) {
            send_text(res, 400, "Path is a directory");
        }
        else {
            send_text(res, 500, error);
        }
        
        return;
    }
    
    // Success
    send_text(res, 200, "OK");
}
```

## Common Error Codes

| Error Code | Meaning                 | HTTP Status | Example Cause                    |
|------------|-------------------------|-------------|----------------------------------|
| `ENOENT`   | File not found          | 404         | Reading non-existent file        |
| `EACCES`   | Permission denied       | 403         | No read/write permission         |
| `EISDIR`   | Is a directory          | 400         | Trying to read a directory       |
| `ENOTDIR`  | Not a directory         | 400         | Path component is not a dir      |
| `EEXIST`   | File already exists     | 409         | Creating file that exists        |
| `ENOSPC`   | No space left on device | 507         | Disk full                        |
| `EMFILE`   | Too many open files     | 503         | System file descriptor limit hit |

## Limits and Configuration

```c
// Maximum concurrent file operations (default: 100)
#define ECEWO_FS_MAX_CONCURRENT_OPS 100

// Maximum file size for read/write operations (default: 100MB)
#define ECEWO_FS_MAX_FILE_SIZE (100 * 1024 * 1024)
```

You can override these in your build:

```sh
target_compile_definitions(app PRIVATE
    ECEWO_FS_MAX_CONCURRENT_OPS=200
    ECEWO_FS_MAX_FILE_SIZE=209715200  # 200MB
)
```

## Statistics and Monitoring

Get file system operation statistics:

```c
#include "ecewo-fs.h"

void stats_handler(Req *req, Res *res) {
    fs_stats_t stats;
    fs_get_stats(&stats);
    
    char *json = arena_sprintf(req->arena,
        "{"
        "\"active_operations\":%d,"
        "\"peak_operations\":%d,"
        "\"total_reads\":%llu,"
        "\"total_writes\":%llu,"
        "\"total_bytes_read\":%llu,"
        "\"total_bytes_written\":%llu,"
        "\"failed_operations\":%d"
        "}",
        stats.active_operations,
        stats.peak_operations,
        (unsigned long long)stats.total_reads,
        (unsigned long long)stats.total_writes,
        (unsigned long long)stats.total_bytes_read,
        (unsigned long long)stats.total_bytes_written,
        stats.failed_operations
    );
    
    send_json(res, 200, json);
}
```

Reset statistics:

```c
fs_reset_stats();
```

Check if system can accept more operations:

```c
if (fs_can_accept_operation()) {
    // Queue operation
    fs_read_file("file.txt", req->arena, callback, res);
} else {
    // System at capacity
    send_text(res, 503, "Service temporarily unavailable");
}
```

## Shutdown and Cleanup

Always register `fs_cleanup()` with `server_atexit()` to ensure proper cleanup:

```c
int main(void) {
    server_init();
    fs_init();
    
    // Your routes...
    get("/file", read_handler);
    
    // Register cleanup - called automatically on shutdown
    server_atexit(fs_cleanup);
    
    server_listen(3000);
    server_run();
    return 0;
}
```

`fs_cleanup()` will:
- Wait up to 1 second for pending operations to complete
- Print a warning if operations are still active
- Clean up internal resources

The cleanup is automatically triggered when:
- Server receives a shutdown signal (SIGINT, SIGTERM)
- `server_run()` returns normally
- Application exits
