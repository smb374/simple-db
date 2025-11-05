inc = include_directories('../include')

db_srcs = [
  './pagestore.c',
  './rwsxlock.c',
  './utils.c',
]
db_lib = static_library('simple-db-lib', db_srcs, include_directories : inc)
