inc = include_directories('../include')

db_srcs = [
  './alloc.c',
  './bufpool.c',
  './cqueue.c',
  './catalog.c',
  './pagestore.c',
  './record.c',
  './rwsxlock.c',
  './shtable.c',
  './slot.c',
  './utils.c',
]
db_lib = static_library('simple-db-lib', db_srcs, include_directories : inc)
