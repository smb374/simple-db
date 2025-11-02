inc = include_directories('../include')

db_srcs = [
  './btree.c',
  './dblock.c',
  './gdt_page.c',
  './schema.c',
  './type.c',
  './utils.c',
]
db_lib = static_library('simple-db-lib', db_srcs, include_directories : inc)
