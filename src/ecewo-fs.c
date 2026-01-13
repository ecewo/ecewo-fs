#include "ecewo-fs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  // Operations tracking
  int active_operations;
  int peak_operations;
  int queued_operations;

  // Statistics
  uint64_t total_reads;
  uint64_t total_writes;
  uint64_t total_bytes_read;
  uint64_t total_bytes_written;
  int failed_operations;

  // Thread safety
  uv_mutex_t mutex;
  bool initialized;
} fs_module_state_t;

static fs_module_state_t fs_state = { 0 };

typedef struct fs_request_s {
  uv_fs_t fs_req;
  Arena *arena; // NULL = use malloc
  void *user_data;

  // Callbacks
  fs_read_callback_t read_callback;
  fs_write_callback_t write_callback;
  fs_stat_callback_t stat_callback;

  // Data
  char *data;
  size_t size;
  uv_stat_t stat;

  // Paths (owned by this struct)
  char *path;
  char *path2; // For rename operations

  // Internal state
  uv_file file;
  size_t file_size;

  // Error tracking
  char *error_msg; // Owned by this struct
} fs_request_t;

int fs_init(void) {
  if (fs_state.initialized)
    return 0;

  if (uv_mutex_init(&fs_state.mutex) != 0) {
    fprintf(stderr, "[ecewo-fs] Failed to initialize mutex\n");
    return -1;
  }

  fs_state.initialized = true;
  return 0;
}

void fs_cleanup(void) {
  if (!fs_state.initialized)
    return;

  uv_mutex_lock(&fs_state.mutex);

  int wait_count = 0;
  while (fs_state.active_operations > 0 && wait_count < 100) {
    uv_mutex_unlock(&fs_state.mutex);
    uv_sleep(10); // 10ms
    uv_mutex_lock(&fs_state.mutex);
    wait_count++;
  }

  if (fs_state.active_operations > 0) {
    fprintf(stderr, "[ecewo-fs] Warning: %d operations still active during cleanup\n",
            fs_state.active_operations);
  }

  fs_state.initialized = false;
  uv_mutex_unlock(&fs_state.mutex);
  uv_mutex_destroy(&fs_state.mutex);
}

void fs_get_stats(fs_stats_t *stats) {
  if (!stats || !fs_state.initialized)
    return;

  uv_mutex_lock(&fs_state.mutex);
  stats->active_operations = fs_state.active_operations;
  stats->peak_operations = fs_state.peak_operations;
  stats->queued_operations = fs_state.queued_operations;
  stats->total_reads = fs_state.total_reads;
  stats->total_writes = fs_state.total_writes;
  stats->total_bytes_read = fs_state.total_bytes_read;
  stats->total_bytes_written = fs_state.total_bytes_written;
  stats->failed_operations = fs_state.failed_operations;
  uv_mutex_unlock(&fs_state.mutex);
}

void fs_reset_stats(void) {
  if (!fs_state.initialized)
    return;

  uv_mutex_lock(&fs_state.mutex);
  fs_state.total_reads = 0;
  fs_state.total_writes = 0;
  fs_state.total_bytes_read = 0;
  fs_state.total_bytes_written = 0;
  fs_state.failed_operations = 0;
  fs_state.peak_operations = 0;
  uv_mutex_unlock(&fs_state.mutex);
}

int fs_can_accept_operation(void) {
  if (!fs_state.initialized)
    return -1;

  uv_mutex_lock(&fs_state.mutex);
  int can = (fs_state.active_operations < ECEWO_FS_MAX_CONCURRENT_OPS);
  uv_mutex_unlock(&fs_state.mutex);
  return can;
}

static void fs_begin_operation(void) {
  uv_mutex_lock(&fs_state.mutex);
  fs_state.active_operations++;
  if (fs_state.active_operations > fs_state.peak_operations)
    fs_state.peak_operations = fs_state.active_operations;
  uv_mutex_unlock(&fs_state.mutex);
}

static void fs_end_operation(void) {
  uv_mutex_lock(&fs_state.mutex);
  if (fs_state.active_operations > 0)
    fs_state.active_operations--;
  uv_mutex_unlock(&fs_state.mutex);
}

static void fs_record_read(size_t bytes) {
  uv_mutex_lock(&fs_state.mutex);
  fs_state.total_reads++;
  fs_state.total_bytes_read += bytes;
  uv_mutex_unlock(&fs_state.mutex);
}

static void fs_record_write(size_t bytes) {
  uv_mutex_lock(&fs_state.mutex);
  fs_state.total_writes++;
  fs_state.total_bytes_written += bytes;
  uv_mutex_unlock(&fs_state.mutex);
}

static void fs_record_error(void) {
  uv_mutex_lock(&fs_state.mutex);
  fs_state.failed_operations++;
  uv_mutex_unlock(&fs_state.mutex);
}

static char *make_error_msg(int errcode) {
  char *buf = malloc(256);
  if (!buf)
    return NULL;

  snprintf(buf, 256, "%s: %s", uv_err_name(errcode), uv_strerror(errcode));
  return buf;
}

static void fs_request_cleanup(fs_request_t *req, bool free_data) {
  if (!req)
    return;

  if (req->path)
    free(req->path);

  if (req->path2)
    free(req->path2);

  if (req->error_msg)
    free(req->error_msg);

  if (free_data && req->data && !req->arena) {
    free(req->data);
  }

  free(req);
}

static void read_close_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  uv_fs_req_cleanup(uv_req);

  if (req->read_callback) {
    req->read_callback(NULL, req->data, req->size, req->user_data);
  }

  fs_record_read(req->size);
  fs_end_operation();

  // Do not free data - user owns it (or it's in arena)
  fs_request_cleanup(req, false);
}

static void read_data_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    uv_fs_req_cleanup(uv_req);
    uv_fs_close(get_loop(), &req->fs_req, req->file, NULL);

    if (req->read_callback) {
      req->read_callback(req->error_msg ? req->error_msg : "Read failed",
                         NULL, 0, req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, true);
    return;
  }

  req->size = (size_t)uv_req->result;
  req->data[req->size] = '\0';

  uv_fs_req_cleanup(uv_req);
  uv_fs_close(get_loop(), &req->fs_req, req->file, read_close_cb);
}

static void read_open_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->read_callback) {
      req->read_callback(req->error_msg ? req->error_msg : "Open failed",
                         NULL, 0, req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->file = (uv_file)uv_req->result;
  uv_fs_req_cleanup(uv_req);

  if (req->arena) {
    req->data = arena_alloc(req->arena, req->file_size + 1);
  } else {
    req->data = malloc(req->file_size + 1);
  }

  if (!req->data) {
    uv_fs_close(get_loop(), &req->fs_req, req->file, NULL);

    if (req->read_callback) {
      req->read_callback("Memory allocation failed", NULL, 0, req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  uv_buf_t buf = uv_buf_init(req->data, (unsigned int)req->file_size);
  uv_fs_read(get_loop(), &req->fs_req, req->file, &buf, 1, 0, read_data_cb);
}

static void read_stat_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->read_callback) {
      req->read_callback(req->error_msg ? req->error_msg : "Stat failed",
                         NULL, 0, req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->file_size = (size_t)uv_req->statbuf.st_size;

  if (req->file_size > ECEWO_FS_MAX_FILE_SIZE) {
    uv_fs_req_cleanup(uv_req);

    if (req->read_callback) {
      req->read_callback("File too large", NULL, 0, req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  uv_fs_req_cleanup(uv_req);
  uv_fs_open(get_loop(), &req->fs_req, req->path,
             UV_FS_O_RDONLY, 0, read_open_cb);
}

int fs_read_file(const char *path, Arena *arena, fs_read_callback_t callback, void *user_data) {
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

  fs_request_t *req = calloc(1, sizeof(fs_request_t));
  if (!req) {
    fprintf(stderr, "[ecewo-fs] Memory allocation failed\n");
    return -1;
  }

  req->arena = arena;
  req->user_data = user_data;
  req->read_callback = callback;
  req->path = strdup(path);
  req->fs_req.data = req;

  if (!req->path) {
    free(req);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_stat(get_loop(), &req->fs_req, req->path, read_stat_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(result);
    callback(req->error_msg ? req->error_msg : "Stat failed", NULL, 0, user_data);
    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return -1;
  }

  return 0;
}

static void write_close_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  uv_fs_req_cleanup(uv_req);

  if (req->write_callback) {
    req->write_callback(NULL, req->user_data);
  }

  fs_record_write(req->size);
  fs_end_operation();
  fs_request_cleanup(req, true);
}

static void write_data_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    uv_fs_req_cleanup(uv_req);
    uv_fs_close(get_loop(), &req->fs_req, req->file, NULL);

    if (req->write_callback) {
      req->write_callback(req->error_msg ? req->error_msg : "Write failed",
                          req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, true);
    return;
  }

  req->size = (size_t)uv_req->result;
  uv_fs_req_cleanup(uv_req);
  uv_fs_close(get_loop(), &req->fs_req, req->file, write_close_cb);
}

static void write_open_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->write_callback) {
      req->write_callback(req->error_msg ? req->error_msg : "Open failed",
                          req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, true);
    return;
  }

  req->file = (uv_file)uv_req->result;
  uv_fs_req_cleanup(uv_req);

  uv_buf_t buf = uv_buf_init(req->data, (unsigned int)req->size);
  uv_fs_write(get_loop(), &req->fs_req, req->file, &buf, 1, 0, write_data_cb);
}

static int fs_write_internal(const char *path, const void *data, size_t size, fs_write_callback_t callback, void *user_data, int flags) {
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

  fs_request_t *req = calloc(1, sizeof(fs_request_t));
  if (!req)
    return -1;

  req->user_data = user_data;
  req->write_callback = callback;
  req->path = strdup(path);
  req->data = malloc(size);
  req->size = size;
  req->fs_req.data = req;

  if (!req->path || !req->data) {
    fs_request_cleanup(req, true);
    return -1;
  }

  memcpy(req->data, data, size);

  fs_begin_operation();

  int result = uv_fs_open(get_loop(), &req->fs_req, req->path,
                          flags, 0644, write_open_cb);

  if (result < 0) {
    req->error_msg = make_error_msg(result);
    callback(req->error_msg ? req->error_msg : "Open failed", user_data);
    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, true);
    return -1;
  }

  return 0;
}

int fs_write_file(const char *path, const void *data, size_t size, fs_write_callback_t callback, void *user_data) {
  return fs_write_internal(path, data, size, callback, user_data,
                           UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_TRUNC);
}

int fs_append_file(const char *path, const void *data, size_t size, fs_write_callback_t callback, void *user_data) {
  return fs_write_internal(path, data, size, callback, user_data,
                           UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_APPEND);
}

static void stat_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    uv_fs_req_cleanup(uv_req);

    if (req->stat_callback) {
      req->stat_callback(req->error_msg ? req->error_msg : "Stat failed",
                         NULL, req->user_data);
    }

    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return;
  }

  req->stat = uv_req->statbuf;
  uv_fs_req_cleanup(uv_req);

  if (req->stat_callback) {
    req->stat_callback(NULL, &req->stat, req->user_data);
  }

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

  fs_request_t *req = calloc(1, sizeof(fs_request_t));
  if (!req)
    return -1;

  req->user_data = user_data;
  req->stat_callback = callback;
  req->path = strdup(path);
  req->fs_req.data = req;

  if (!req->path) {
    free(req);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_stat(get_loop(), &req->fs_req, req->path, stat_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(result);
    callback(req->error_msg ? req->error_msg : "Stat failed", NULL, user_data);
    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return -1;
  }

  return 0;
}

static void simple_op_cb(uv_fs_t *uv_req) {
  fs_request_t *req = (fs_request_t *)uv_req->data;

  char *error = NULL;
  if (uv_req->result < 0) {
    req->error_msg = make_error_msg((int)uv_req->result);
    error = req->error_msg;
    fs_record_error();
  }

  uv_fs_req_cleanup(uv_req);

  if (req->write_callback) {
    req->write_callback(error, req->user_data);
  }

  fs_end_operation();
  fs_request_cleanup(req, false);
}

typedef int (*uv_fs_op_t)(uv_loop_t *, uv_fs_t *, const char *, uv_fs_cb);
typedef int (*uv_fs_op_mode_t)(uv_loop_t *, uv_fs_t *, const char *, int, uv_fs_cb);

static int fs_simple_op(const char *path, fs_write_callback_t callback, void *user_data, uv_fs_op_t op_fn) {
  if (!path || !callback)
    return -1;

  if (!fs_state.initialized || !fs_can_accept_operation())
    return -1;

  fs_request_t *req = calloc(1, sizeof(fs_request_t));
  if (!req)
    return -1;

  req->user_data = user_data;
  req->write_callback = callback;
  req->path = strdup(path);
  req->fs_req.data = req;

  if (!req->path) {
    free(req);
    return -1;
  }

  fs_begin_operation();

  int result = op_fn(get_loop(), &req->fs_req, req->path, simple_op_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(result);
    callback(req->error_msg ? req->error_msg : "Operation failed", user_data);
    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return -1;
  }

  return 0;
}

static int fs_simple_op_mode(const char *path, fs_write_callback_t callback, void *user_data, int mode, uv_fs_op_mode_t op_fn) {
  if (!path || !callback)
    return -1;

  if (!fs_state.initialized || !fs_can_accept_operation())
    return -1;

  fs_request_t *req = calloc(1, sizeof(fs_request_t));
  if (!req)
    return -1;

  req->user_data = user_data;
  req->write_callback = callback;
  req->path = strdup(path);
  req->fs_req.data = req;

  if (!req->path) {
    free(req);
    return -1;
  }

  fs_begin_operation();

  int result = op_fn(get_loop(), &req->fs_req, req->path, mode, simple_op_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(result);
    callback(req->error_msg ? req->error_msg : "Operation failed", user_data);
    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
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
    req->error_msg = make_error_msg((int)uv_req->result);
    error = req->error_msg;
    fs_record_error();
  }

  uv_fs_req_cleanup(uv_req);

  if (req->write_callback) {
    req->write_callback(error, req->user_data);
  }

  fs_end_operation();
  fs_request_cleanup(req, false);
}

int fs_rename(const char *old_path, const char *new_path, fs_write_callback_t callback, void *user_data) {
  if (!old_path || !new_path || !callback)
    return -1;

  if (!fs_state.initialized || !fs_can_accept_operation())
    return -1;

  fs_request_t *req = calloc(1, sizeof(fs_request_t));
  if (!req)
    return -1;

  req->user_data = user_data;
  req->write_callback = callback;
  req->path = strdup(old_path);
  req->path2 = strdup(new_path);
  req->fs_req.data = req;

  if (!req->path || !req->path2) {
    fs_request_cleanup(req, false);
    return -1;
  }

  fs_begin_operation();

  int result = uv_fs_rename(get_loop(), &req->fs_req,
                            req->path, req->path2, rename_cb);
  if (result < 0) {
    req->error_msg = make_error_msg(result);
    callback(req->error_msg ? req->error_msg : "Rename failed", user_data);
    fs_record_error();
    fs_end_operation();
    fs_request_cleanup(req, false);
    return -1;
  }

  return 0;
}
