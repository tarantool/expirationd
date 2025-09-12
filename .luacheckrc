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

redefined = false
globals = {'box', 'utf8', 'checkers', '_TARANTOOL'}
include_files = {'**/*.lua', '*.luacheckrc', '*.rockspec'}
exclude_files = {'**/*.rocks/', 'tmp/', 'sdk'}
max_line_length = 120
max_comment_line_length = 150
