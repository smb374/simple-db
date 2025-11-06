inc = include_directories('../include')

db_srcs = [
  './bufpool.c',
  './pagestore.c',
  './rwsxlock.c',
  './shtable.c',
  './utils.c',
]
db_lib = static_library('simple-db-lib', db_srcs, include_directories : inc)
