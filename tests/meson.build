inc = include_directories('../include')

# Find the Unity dependency which is a subproject
unity_dep = dependency('unity', fallback: ['unity', 'unity_dep'])

# Link with the main library defined in src/meson.build
test('test_gdt_page',
    executable('test_gdt_page_runner', 'test_gdt_page.c',
        include_directories: inc,
        dependencies: unity_dep,
        link_with: db_lib
    )
)

test('test_page',
    executable('test_page_runner', 'test_page.c',
        include_directories: inc,
        dependencies: unity_dep,
        link_with: db_lib
    )
)

test('test_dblock',
    executable('test_dblock_runner', 'test_dblock.c',
        include_directories: inc,
        dependencies: unity_dep,
        link_with: db_lib
    )
)

test('test_btree',
    executable('test_btree_runner', 'test_btree.c',
        include_directories: inc,
        dependencies: unity_dep,
        link_with: db_lib
    )
)
