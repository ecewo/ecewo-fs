#ifndef ECEWO_FS_H
#define ECEWO_FS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ecewo-fs-export.h"
#include <stddef.h>
#include <stdint.h>

// Opaque arena type from ecewo. Pass ecewo_req_arena(req) inside request handlers,
// or NULL to use malloc (caller must then free the data returned in the callback).
typedef struct ecewo_arena_s ecewo_arena_t;

// If arena provided: data is allocated in arena (auto-freed with arena)
// If arena is NULL: data is malloc'd (caller MUST free)

#ifndef ECEWO_FS_MAX_CONCURRENT_OPS
#define ECEWO_FS_MAX_CONCURRENT_OPS 100
#endif

#ifndef ECEWO_FS_MAX_FILE_SIZE
#define ECEWO_FS_MAX_FILE_SIZE (100 * 1024 * 1024) // 100 MB
#endif

/**
 * Opaque file metadata handle passed to `fs_stat_callback_t`.
 * Access fields through the `fs_stat_*()` accessor functions.
 * The pointer is valid only for the duration of the callback.
 */
typedef struct fs_stat_s fs_stat_t;

typedef void (*fs_read_callback_t)(
    const char *error, // Error message (do not free) or NULL on success
    const char *data,  // File contents or NULL on error
    size_t size,       // Size of data in bytes
    void *user_data);  // User-provided context pointer

typedef void (*fs_write_callback_t)(
    const char *error,
    void *user_data);

typedef void (*fs_stat_callback_t)(
    const char *error,
    const fs_stat_t *stat,
    void *user_data);

// ---------------------------------------------------------------------------
// LIFECYCLE
// ---------------------------------------------------------------------------

// Returns: 0 on success, -1 on failure
ECEWO_FS_EXPORT int fs_init(void);

// Should be called at application shutdown
ECEWO_FS_EXPORT void fs_cleanup(void);

// ---------------------------------------------------------------------------
// FILE OPERATIONS
// ---------------------------------------------------------------------------

// Returns: 0 if operation queued, -1 if rejected (too many concurrent ops)
ECEWO_FS_EXPORT int fs_read_file(
    const char *path,
    ecewo_arena_t *arena,
    fs_read_callback_t callback,
    void *user_data);

// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_write_file(
    const char *path,
    const void *data,
    size_t size,
    fs_write_callback_t callback,
    void *user_data);

// Append data to file asynchronously (creates if doesn't exist)
// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_append_file(
    const char *path,
    const void *data,
    size_t size,
    fs_write_callback_t callback,
    void *user_data);

// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_stat(
    const char *path,
    fs_stat_callback_t callback,
    void *user_data);

// Delete file asynchronously
// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_unlink(
    const char *path,
    fs_write_callback_t callback,
    void *user_data);

// Rename/move file asynchronously
// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_rename(
    const char *old_path,
    const char *new_path,
    fs_write_callback_t callback,
    void *user_data);

// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_mkdir(
    const char *path,
    fs_write_callback_t callback,
    void *user_data);

// Remove directory asynchronously (must be empty)
// Returns: 0 if operation queued, -1 if rejected
ECEWO_FS_EXPORT int fs_rmdir(
    const char *path,
    fs_write_callback_t callback,
    void *user_data);

// ---------------------------------------------------------------------------
// fs_stat_t ACCESSORS
// Valid only during the fs_stat_callback_t invocation.
// ---------------------------------------------------------------------------

ECEWO_FS_EXPORT uint64_t fs_stat_dev(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_mode(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_nlink(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_uid(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_gid(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_rdev(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_ino(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_size(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_blksize(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_blocks(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_flags(const fs_stat_t *stat);
ECEWO_FS_EXPORT uint64_t fs_stat_gen(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_atime_sec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_atime_nsec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_mtime_sec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_mtime_nsec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_ctime_sec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_ctime_nsec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_birthtime_sec(const fs_stat_t *stat);
ECEWO_FS_EXPORT int64_t  fs_stat_birthtime_nsec(const fs_stat_t *stat);

// ---------------------------------------------------------------------------
// STATISTICS
// ---------------------------------------------------------------------------

ECEWO_FS_EXPORT int      fs_stats_active_operations(void);
ECEWO_FS_EXPORT int      fs_stats_peak_operations(void);
ECEWO_FS_EXPORT int      fs_stats_queued_operations(void);
ECEWO_FS_EXPORT uint64_t fs_stats_total_reads(void);
ECEWO_FS_EXPORT uint64_t fs_stats_total_writes(void);
ECEWO_FS_EXPORT uint64_t fs_stats_total_bytes_read(void);
ECEWO_FS_EXPORT uint64_t fs_stats_total_bytes_written(void);
ECEWO_FS_EXPORT int      fs_stats_failed_operations(void);

// Reset all statistics counters
ECEWO_FS_EXPORT void fs_reset_stats(void);

// Returns: 1 if can accept, 0 if at limit, -1 if not initialized
ECEWO_FS_EXPORT int fs_can_accept_operation(void);

#ifdef __cplusplus
}
#endif

#endif
