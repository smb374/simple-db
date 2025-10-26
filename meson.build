project('simple-db', 'c',
  version : '0.1.0',
  default_options : ['warning_level=3',
                   'c_std=gnu11'])

unity_dep = dependency('unity', fallback : ['unity', 'unity_dep'])

subdir('src')
subdir('tests')
