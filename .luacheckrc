globals = {
    "box",
    "_TARANTOOL",
}

ignore = {
    -- Unused argument <self>.
    "212/self",
    -- Redefining a local variable.
    "411",
    -- Shadowing a local variable.
    "421",
    -- Shadowing an upvalue.
    "431",
    -- Shadowing an upvalue argument.
    "432",
}

include_files = {
    '.luacheckrc',
    '*.rockspec',
    '**/*.lua',
}

exclude_files = {
    '.rocks',
    'test.lua',
}
