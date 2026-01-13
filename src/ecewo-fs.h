#ifndef ECEWO_FS_H
#define ECEWO_FS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ecewo.h"
#include "uv.h"
#include <stddef.h>
#include <stdbool.h>

// If arena provided: data is allocated in arena (auto-freed with arena)
// If arena is NULL: data is malloc'd (caller MUST free)

#ifndef ECEWO_FS_MAX_CONCURRENT_OPS
#define ECEWO_FS_MAX_CONCURRENT_OPS 100
#endif

#ifndef ECEWO_FS_MAX_FILE_SIZE
#define ECEWO_FS_MAX_FILE_SIZE (100 * 1024 * 1024) // 100 MB
#endif

typedef void (*fs_read_callback_t)(
    const char *error, // Error message (static string, do not free) or NULL on success
    const char *data, // File contents (see memory management above) or NULL on error
    size_t size, // Size of data in bytes
    void *user_data); // User-provided context pointer

typedef void (*fs_write_callback_t)(
    const char *error,
    void *user_data);

typedef void (*fs_stat_callback_t)(
    const char *error,
    const uv_stat_t *stat,
    void *user_data);

// Returns: 0 on success, -1 on failure
int fs_init(void);

// Should be called at application shutdown
// Waits for pending operations to complete (with timeout)
void fs_cleanup(void);

// Returns: 0 if operation queued, -1 if rejected (too many concurrent ops)
int fs_read_file(
    const char *path,
    Arena *arena,
    fs_read_callback_t callback,
    void *user_data);

// Returns: 0 if operation queued, -1 if rejected
int fs_write_file(
    const char *path,
    const void *data,
    size_t size,
    fs_write_callback_t callback,
    void *user_data);

// Append data to file asynchronously (creates if doesn't exist)
// Same semantics as fs_write_file but appends instead of truncating
// Returns: 0 if operation queued, -1 if rejected
int fs_append_file(
    const char *path,
    const void *data,
    size_t size,
    fs_write_callback_t callback,
    void *user_data);


// Returns: 0 if operation queued, -1 if rejected
int fs_stat(
    const char *path,
    fs_stat_callback_t callback,
    void *user_data);

// Delete file asynchronously
// Returns: 0 if operation queued, -1 if rejected
int fs_unlink(
    const char *path,
    fs_write_callback_t callback,
    void *user_data);

// Rename/move file asynchronously
// Returns: 0 if operation queued, -1 if rejected
int fs_rename(
    const char *old_path,
    const char *new_path,
    fs_write_callback_t callback,
    void *user_data);

// Returns: 0 if operation queued, -1 if rejected
int fs_mkdir(
    const char *path,
    fs_write_callback_t callback,
    void *user_data);

// Remove directory asynchronously (must be empty)
// Returns: 0 if operation queued, -1 if rejected
int fs_rmdir(
    const char *path,
    fs_write_callback_t callback,
    void *user_data);

// File system operation statistics
typedef struct {
  int active_operations; // Currently running operations
  int peak_operations; // Peak concurrent operations
  int queued_operations; // Operations waiting for slot
  uint64_t total_reads; // Total read operations
  uint64_t total_writes; // Total write operations
  uint64_t total_bytes_read; // Total bytes read
  uint64_t total_bytes_written; // Total bytes written
  int failed_operations; // Operations that failed
} fs_stats_t;

// Get current statistics
void fs_get_stats(fs_stats_t *stats);

// Reset statistics counters
void fs_reset_stats(void);

// Returns: 0 if can accept, -1 if at limit
int fs_can_accept_operation(void);

#ifdef __cplusplus
}
#endif

#endif
