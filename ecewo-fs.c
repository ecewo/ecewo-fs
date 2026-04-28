#include "ecewo-fs.h"
#include "ecewo.h"
#include "uv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct fs_stat_s {
  uv_stat_t uv;
};

typedef struct {
  int active_operations;
  int peak_operations;
  int queued_operations;
  uint64_t total_reads;
  uint64_t total_writes;
  uint64_t total_bytes_read;
  uint64_t total_bytes_written;
  int failed_operations;
  bool initialized;
} fs_module_state_t;

static fs_module_state_t fs_state = { 0 };

typedef struct fs_request_s {
  uv_fs_t fs_req;
  ecewo_arena_t *arena;      // user-provided arena for read data (NULL = none)
  ecewo_arena_t *work_arena; // arena for all internal allocations
  bool owns_work_arena;      // true when work_arena was borrowed from the pool
  void *user_data;

  fs_read_callback_t read_callback;
  fs_write_callback_t write_callback;
  fs_stat_callback_t stat_callback;

  char *data;
  size_t size;
  fs_stat_t stat;

  char *path;
  char *path2;

  uv_file file;
  size_t file_size;

  char *error_msg;
} fs_request_t;

// ---------------------------------------------------------------------------
// LIFECYCLE
// ---------------------------------------------------------------------------

int fs_init(void) {
  if (fs_state.initialized)
    return 0;

  fs_state.initialized = true;
  return 0;
}

void fs_cleanup(void) {
  if (!fs_state.initialized)
    return;

  if (fs_state.active_operations > 0) {
    fprintf(stderr, "[ecewo-fs] Warning: %d operations still active during cleanup\n",
            fs_state.active_operations);
  }

  fs_state.initialized = false;
}

// ---------------------------------------------------------------------------
// fs_stat_t ACCESSORS
// ---------------------------------------------------------------------------

uint64_t fs_stat_dev(const fs_stat_t *s)           { return s->uv.st_dev; }
uint64_t fs_stat_mode(const fs_stat_t *s)          { return s->uv.st_mode; }
uint64_t fs_stat_nlink(const fs_stat_t *s)         { return s->uv.st_nlink; }
uint64_t fs_stat_uid(const fs_stat_t *s)           { return s->uv.st_uid; }
uint64_t fs_stat_gid(const fs_stat_t *s)           { return s->uv.st_gid; }
uint64_t fs_stat_rdev(const fs_stat_t *s)          { return s->uv.st_rdev; }
uint64_t fs_stat_ino(const fs_stat_t *s)           { return s->uv.st_ino; }
uint64_t fs_stat_size(const fs_stat_t *s)          { return s->uv.st_size; }
uint64_t fs_stat_blksize(const fs_stat_t *s)       { return s->uv.st_blksize; }
uint64_t fs_stat_blocks(const fs_stat_t *s)        { return s->uv.st_blocks; }
uint64_t fs_stat_flags(const fs_stat_t *s)         { return s->uv.st_flags; }
uint64_t fs_stat_gen(const fs_stat_t *s)           { return s->uv.st_gen; }
int64_t  fs_stat_atime_sec(const fs_stat_t *s)     { return (int64_t)s->uv.st_atim.tv_sec; }
int64_t  fs_stat_atime_nsec(const fs_stat_t *s)    { return (int64_t)s->uv.st_atim.tv_nsec; }
int64_t  fs_stat_mtime_sec(const fs_stat_t *s)     { return (int64_t)s->uv.st_mtim.tv_sec; }
int64_t  fs_stat_mtime_nsec(const fs_stat_t *s)    { return (int64_t)s->uv.st_mtim.tv_nsec; }
int64_t  fs_stat_ctime_sec(const fs_stat_t *s)     { return (int64_t)s->uv.st_ctim.tv_sec; }
int64_t  fs_stat_ctime_nsec(const fs_stat_t *s)    { return (int64_t)s->uv.st_ctim.tv_nsec; }
int64_t  fs_stat_birthtime_sec(const fs_stat_t *s) { return (int64_t)s->uv.st_birthtim.tv_sec; }
int64_t  fs_stat_birthtime_nsec(const fs_stat_t *s){ return (int64_t)s->uv.st_birthtim.tv_nsec; }

// ---------------------------------------------------------------------------
// STATISTICS ACCESSORS
// ---------------------------------------------------------------------------

int fs_stats_active_operations(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.active_operations;
}

int fs_stats_peak_operations(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.peak_operations;
}

int fs_stats_queued_operations(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.queued_operations;
}

uint64_t fs_stats_total_reads(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.total_reads;
}

uint64_t fs_stats_total_writes(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.total_writes;
}

uint64_t fs_stats_total_bytes_read(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.total_bytes_read;
}

uint64_t fs_stats_total_bytes_written(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.total_bytes_written;
}

int fs_stats_failed_operations(void) {
  if (!fs_state.initialized) return 0;
  return fs_state.failed_operations;
}

void fs_reset_stats(void) {
  if (!fs_state.initialized)
    return;

  fs_state.total_reads = 0;
  fs_state.total_writes = 0;
  fs_state.total_bytes_read = 0;
  fs_state.total_bytes_written = 0;
  fs_state.failed_operations = 0;
  fs_state.peak_operations = 0;
}

int fs_can_accept_operation(void) {
  if (!fs_state.initialized)
    return -1;

  return (fs_state.active_operations < ECEWO_FS_MAX_CONCURRENT_OPS);
}

// ---------------------------------------------------------------------------
// INTERNAL HELPERS
// ---------------------------------------------------------------------------

static void fs_begin_operation(void) {
  fs_state.active_operations++;
  if (fs_state.active_operations > fs_state.peak_operations)
    fs_state.peak_operations = fs_state.active_operations;
}

static void fs_end_operation(void) {
  if (fs_state.active_operations > 0)
    fs_state.active_operations--;
}

static void fs_record_read(size_t bytes) {
  fs_state.total_reads++;
  fs_state.total_bytes_read += bytes;
}

static void fs_record_write(size_t bytes) {
  fs_state.total_writes++;
  fs_state.total_bytes_written += bytes;
}

static void fs_record_error(void) {
  fs_state.failed_operations++;
}

static char *make_error_msg(ecewo_arena_t *arena, int errcode) {
  return ecewo_sprintf(arena, "%s: %s", uv_err_name(errcode), uv_strerror(errcode));
}

// free_data: only meaningful for fs_read_file with no user arena (malloc'd data).
// In all other paths data lives in work_arena and is freed with it.
static void fs_request_cleanup(fs_request_t *req, bool free_data) {
  if (free_data && req->data && !req->arena)
    free(req->data);

  if (req->owns_work_arena) {
    ecewo_arena_t *a = req->work_arena;
    ecewo_arena_return(a); // also frees req itself
  }
}

// ---------------------------------------------------------------------------
// READ
// ---------------------------------------------------------------------------

static void read_close_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  uv_fs_req_cleanup(uv_req);

  if (req->read_callback)
    req->read_callback(NULL, req->data, req->size, req->user_data);

  fs_record_read(req->size);
  fs_end_operation();
  fs_request_cleanup(req, false); // user owns data
}

static void read_data_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    uv_fs_req_cleanup(uv_req);
    uv_fs_close((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, NULL);

    if (req->read_callback)
      req->read_callback(req->error_msg ? req->error_msg : "Read failed",
                         NULL, 0, req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, true);
    return;
  }

  req->size = (size_t)uv_req->result;
  req->data[req->size] = '\0';

  uv_fs_req_cleanup(uv_req);
  uv_fs_close((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, read_close_cb);
}

static void read_open_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->read_callback)
      req->read_callback(req->error_msg ? req->error_msg : "Open failed",
                         NULL, 0, req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->file = (uv_file)uv_req->result;
  uv_fs_req_cleanup(uv_req);

  if (req->arena) {
    req->data = ecewo_alloc(req->arena, req->file_size + 1);
  } else {
    req->data = malloc(req->file_size + 1);
  }

  if (!req->data) {
    uv_fs_close((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, NULL);

    if (req->read_callback)
      req->read_callback("Memory allocation failed", NULL, 0, req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  uv_buf_t buf = uv_buf_init(req->data, (unsigned int)req->file_size);
  uv_fs_read((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, &buf, 1, 0, read_data_cb);
}

static void read_stat_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->read_callback)
      req->read_callback(req->error_msg ? req->error_msg : "Stat failed",
                         NULL, 0, req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->file_size = (size_t)uv_req->statbuf.st_size;

  if (req->file_size > ECEWO_FS_MAX_FILE_SIZE) {
    uv_fs_req_cleanup(uv_req);

    if (req->read_callback)
      req->read_callback("File too large", NULL, 0, req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  uv_fs_req_cleanup(uv_req);
  uv_fs_open((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->path,
             UV_FS_O_RDONLY, 0, read_open_cb);
}

int fs_read_file(const char *path, ecewo_arena_t *arena, fs_read_callback_t callback, void *user_data) {
  if (!path || !callback) {
    fprintf(stderr, "[ecewo-fs] fs_read_file: Invalid arguments\n");
    return -1;
  }

  if (!fs_state.initialized) {
    fprintf(stderr, "[ecewo-fs] Module not initialized - call fs_init() first\n");
    return -1;
  }

  if (!fs_can_accept_operation()) {
    fprintf(stderr, "[ecewo-fs] Too many concurrent operations (%d/%d)\n",
            fs_state.active_operations, ECEWO_FS_MAX_CONCURRENT_OPS);
    return -1;
  }

  ecewo_arena_t *work = arena ? arena : ecewo_arena_borrow();
  bool owns = (arena == NULL);

  fs_request_t *req = ecewo_alloc(work, sizeof(fs_request_t));
  if (!req) {
    if (owns) ecewo_arena_return(work);
    return -1;
  }
  memset(req, 0, sizeof(*req));

  req->arena = arena;
  req->work_arena = work;
  req->owns_work_arena = owns;
  req->user_data = user_data;
  req->read_callback = callback;
  req->path = ecewo_strdup(work, path);
  req->fs_req.data = req;

  if (!req->path) {
    if (owns) ecewo_arena_return(work);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_stat((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->path, read_stat_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(work, result);
    callback(req->error_msg ? req->error_msg : "Stat failed", NULL, 0, user_data);
    fs_record_error();
    fs_end_operation();
    if (owns) ecewo_arena_return(work);
    return -1;
  }

  return 0;
}

// ---------------------------------------------------------------------------
// WRITE / APPEND
// ---------------------------------------------------------------------------

static void write_close_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  uv_fs_req_cleanup(uv_req);

  if (req->write_callback)
    req->write_callback(NULL, req->user_data);

  fs_record_write(req->size);
  fs_end_operation();
  fs_request_cleanup(req, false); // data lives in work_arena
}

static void write_data_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    uv_fs_req_cleanup(uv_req);
    uv_fs_close((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, NULL);

    if (req->write_callback)
      req->write_callback(req->error_msg ? req->error_msg : "Write failed",
                          req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false); // data lives in work_arena
    return;
  }

  req->size = (size_t)uv_req->result;
  uv_fs_req_cleanup(uv_req);
  uv_fs_close((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, write_close_cb);
}

static void write_open_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->write_callback)
      req->write_callback(req->error_msg ? req->error_msg : "Open failed",
                          req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->file = (uv_file)uv_req->result;
  uv_fs_req_cleanup(uv_req);

  uv_buf_t buf = uv_buf_init(req->data, (unsigned int)req->size);
  uv_fs_write((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->file, &buf, 1, 0, write_data_cb);
}

static int fs_write_internal(const char *path, const void *data, size_t size,
                              fs_write_callback_t callback, void *user_data, int flags) {
  if (!path || !data || !callback) {
    fprintf(stderr, "[ecewo-fs] fs_write: Invalid arguments\n");
    return -1;
  }

  if (!fs_state.initialized) {
    fprintf(stderr, "[ecewo-fs] Module not initialized - call fs_init() first\n");
    return -1;
  }

  if (!fs_can_accept_operation()) {
    fprintf(stderr, "[ecewo-fs] Too many concurrent operations\n");
    return -1;
  }

  if (size > ECEWO_FS_MAX_FILE_SIZE) {
    fprintf(stderr, "[ecewo-fs] Data too large (%zu bytes > %d max)\n",
            size, ECEWO_FS_MAX_FILE_SIZE);
    return -1;
  }

  ecewo_arena_t *work = ecewo_arena_borrow();

  fs_request_t *req = ecewo_alloc(work, sizeof(fs_request_t));
  if (!req) {
    ecewo_arena_return(work);
    return -1;
  }
  memset(req, 0, sizeof(*req));

  req->work_arena = work;
  req->owns_work_arena = true;
  req->user_data = user_data;
  req->write_callback = callback;
  req->path = ecewo_strdup(work, path);
  req->data = ecewo_memdup(work, (void *)data, size);
  req->size = size;
  req->fs_req.data = req;

  if (!req->path || !req->data) {
    ecewo_arena_return(work);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_open((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->path,
                          flags, 0644, write_open_cb);

  if (result < 0) {
    req->error_msg = make_error_msg(work, result);
    callback(req->error_msg ? req->error_msg : "Open failed", user_data);
    fs_record_error();
    fs_end_operation();
    ecewo_arena_return(work);
    return -1;
  }

  return 0;
}

int fs_write_file(const char *path, const void *data, size_t size,
                  fs_write_callback_t callback, void *user_data) {
  return fs_write_internal(path, data, size, callback, user_data,
                           UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_TRUNC);
}

int fs_append_file(const char *path, const void *data, size_t size,
                   fs_write_callback_t callback, void *user_data) {
  return fs_write_internal(path, data, size, callback, user_data,
                           UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_APPEND);
}

// ---------------------------------------------------------------------------
// STAT
// ---------------------------------------------------------------------------

static void stat_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->stat_callback)
      req->stat_callback(req->error_msg ? req->error_msg : "Stat failed",
                         NULL, req->user_data);

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->stat.uv = uv_req->statbuf;
  uv_fs_req_cleanup(uv_req);

  if (req->stat_callback)
    req->stat_callback(NULL, &req->stat, req->user_data);

  fs_end_operation();
  fs_request_cleanup(req, false);
}

int fs_stat(const char *path, fs_stat_callback_t callback, void *user_data) {
  if (!path || !callback)
    return -1;

  if (!fs_state.initialized) {
    fprintf(stderr, "[ecewo-fs] Module not initialized\n");
    return -1;
  }

  if (!fs_can_accept_operation())
    return -1;

  ecewo_arena_t *work = ecewo_arena_borrow();

  fs_request_t *req = ecewo_alloc(work, sizeof(fs_request_t));
  if (!req) {
    ecewo_arena_return(work);
    return -1;
  }
  memset(req, 0, sizeof(*req));

  req->work_arena = work;
  req->owns_work_arena = true;
  req->user_data = user_data;
  req->stat_callback = callback;
  req->path = ecewo_strdup(work, path);
  req->fs_req.data = req;

  if (!req->path) {
    ecewo_arena_return(work);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_stat((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->path, stat_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(work, result);
    callback(req->error_msg ? req->error_msg : "Stat failed", NULL, user_data);
    fs_record_error();
    fs_end_operation();
    ecewo_arena_return(work);
    return -1;
  }

  return 0;
}

// ---------------------------------------------------------------------------
// SIMPLE OPERATIONS (unlink, mkdir, rmdir, rename)
// ---------------------------------------------------------------------------

static void simple_op_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  char *error = NULL;
  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    error = req->error_msg;
    fs_record_error();
  }

  uv_fs_req_cleanup(uv_req);

  if (req->write_callback)
    req->write_callback(error, req->user_data);

  fs_end_operation();
  fs_request_cleanup(req, false);
}

typedef int (*uv_fs_op_t)(uv_loop_t *, uv_fs_t *, const char *, uv_fs_cb);
typedef int (*uv_fs_op_mode_t)(uv_loop_t *, uv_fs_t *, const char *, int, uv_fs_cb);

static int fs_simple_op(const char *path, fs_write_callback_t callback,
                         void *user_data, uv_fs_op_t op_fn) {
  if (!path || !callback)
    return -1;

  if (!fs_state.initialized || !fs_can_accept_operation())
    return -1;

  ecewo_arena_t *work = ecewo_arena_borrow();

  fs_request_t *req = ecewo_alloc(work, sizeof(fs_request_t));
  if (!req) {
    ecewo_arena_return(work);
    return -1;
  }
  memset(req, 0, sizeof(*req));

  req->work_arena = work;
  req->owns_work_arena = true;
  req->user_data = user_data;
  req->write_callback = callback;
  req->path = ecewo_strdup(work, path);
  req->fs_req.data = req;

  if (!req->path) {
    ecewo_arena_return(work);
    return -1;
  }

  fs_begin_operation();

  int result = op_fn((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->path, simple_op_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(work, result);
    callback(req->error_msg ? req->error_msg : "Operation failed", user_data);
    fs_record_error();
    fs_end_operation();
    ecewo_arena_return(work);
    return -1;
  }

  return 0;
}

static int fs_simple_op_mode(const char *path, fs_write_callback_t callback,
                              void *user_data, int mode, uv_fs_op_mode_t op_fn) {
  if (!path || !callback)
    return -1;

  if (!fs_state.initialized || !fs_can_accept_operation())
    return -1;

  ecewo_arena_t *work = ecewo_arena_borrow();

  fs_request_t *req = ecewo_alloc(work, sizeof(fs_request_t));
  if (!req) {
    ecewo_arena_return(work);
    return -1;
  }
  memset(req, 0, sizeof(*req));

  req->work_arena = work;
  req->owns_work_arena = true;
  req->user_data = user_data;
  req->write_callback = callback;
  req->path = ecewo_strdup(work, path);
  req->fs_req.data = req;

  if (!req->path) {
    ecewo_arena_return(work);
    return -1;
  }

  fs_begin_operation();

  int result = op_fn((uv_loop_t *)ecewo_get_loop(), &req->fs_req, req->path, mode, simple_op_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(work, result);
    callback(req->error_msg ? req->error_msg : "Operation failed", user_data);
    fs_record_error();
    fs_end_operation();
    ecewo_arena_return(work);
    return -1;
  }

  return 0;
}

int fs_unlink(const char *path, fs_write_callback_t callback, void *user_data) {
  return fs_simple_op(path, callback, user_data, uv_fs_unlink);
}

int fs_mkdir(const char *path, fs_write_callback_t callback, void *user_data) {
  return fs_simple_op_mode(path, callback, user_data, 0755, uv_fs_mkdir);
}

int fs_rmdir(const char *path, fs_write_callback_t callback, void *user_data) {
  return fs_simple_op(path, callback, user_data, uv_fs_rmdir);
}

static void rename_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  char *error = NULL;
  if (uv_req->result < 0) {
    req->error_msg = make_error_msg(req->work_arena, (int)uv_req->result);
    error = req->error_msg;
    fs_record_error();
  }

  uv_fs_req_cleanup(uv_req);

  if (req->write_callback)
    req->write_callback(error, req->user_data);

  fs_end_operation();
  fs_request_cleanup(req, false);
}

int fs_rename(const char *old_path, const char *new_path,
              fs_write_callback_t callback, void *user_data) {
  if (!old_path || !new_path || !callback)
    return -1;

  if (!fs_state.initialized || !fs_can_accept_operation())
    return -1;

  ecewo_arena_t *work = ecewo_arena_borrow();

  fs_request_t *req = ecewo_alloc(work, sizeof(fs_request_t));
  if (!req) {
    ecewo_arena_return(work);
    return -1;
  }
  memset(req, 0, sizeof(*req));

  req->work_arena = work;
  req->owns_work_arena = true;
  req->user_data = user_data;
  req->write_callback = callback;
  req->path = ecewo_strdup(work, old_path);
  req->path2 = ecewo_strdup(work, new_path);
  req->fs_req.data = req;

  if (!req->path || !req->path2) {
    ecewo_arena_return(work);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_rename((uv_loop_t *)ecewo_get_loop(), &req->fs_req,
                            req->path, req->path2, rename_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(work, result);
    callback(req->error_msg ? req->error_msg : "Rename failed", user_data);
    fs_record_error();
    fs_end_operation();
    ecewo_arena_return(work);
    return -1;
  }

  return 0;
}
