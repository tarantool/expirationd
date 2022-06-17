local t = require("luatest")
local fio = require("fio")

local helpers = require("luatest.helpers")

helpers.project_root = fio.dirname(debug.sourcedir())

local function create_space(space_name, engine)
    local space_format = {
        {
            name = "id",
            type = "number"
        },
        {
            name = "first_name",
            type = "string"
        },
        {
            name = "value",
            type = "number",
            is_nullable = true
        },
        {
            name = "count",
            type = "number",
            is_nullable = true
        },
        {
            name = "non_unique_id",
            type = "number",
            is_nullable = true,
        },
        {
            name = "json_path_field",
            is_nullable = true,
        },
        {
            name = "multikey_field",
            is_nullable = true
        },
        {
            name = "functional_field",
            is_nullable = true
        },
    }

    local space = box.schema.create_space(space_name, {
        engine = engine
    })
    space:format(space_format)

    return space
end

function helpers.create_space_with_tree_index(engine)
    local space = create_space("tree", engine)

    space:create_index("primary", {
        type = "TREE",
        parts = {
            {
                field = 1
            }
        }
    })
    space:create_index("index_for_first_name", {
        type = "TREE",
        parts = {
            {
                field = 2
            }
        }
    })
    space:create_index("multipart_index", {
        type = "TREE",
        parts = {
            {
                field = 3,
                is_nullable = true
            },
            {
                field = 4,
                is_nullable = true
            }
        }
    })
    space:create_index("non_unique_index", {
        type = "TREE",
        parts = {
            {
                field = 5,
                is_nullable = true
            }
        },
        unique = false
    })

    if _TARANTOOL >= "2" then
        space:create_index("json_path_index", {
            type = "TREE",
            parts = {
                {
                    field = 6,
                    type = "scalar",
                    path = "age",
                    is_nullable = true
                }
            }
        })
        space:create_index("multikey_index", {
            type = "TREE",
            parts = {
                {
                    field = 7,
                    type = "str",
                    path = "data[*].name"
                }
            }
        })
        if engine ~= "vinyl" then
            space:create_index("functional_index", {
                type = "TREE",
                parts = {
                    {
                        field = 1,
                        type = "string"
                    }
                },
                func = "tree_func"
            })
        end
    end

    return space
end

function helpers.create_space_with_hash_index(engine)
    local space = create_space("hash", engine)
    space:create_index("primary", {
        type = "HASH",
        parts = {
            {
                field = 1
            }
        }
    })
    space:create_index("index_for_first_name", {
        type = "HASH",
        parts = {
            {
                field = 2
            }
        }
    })
    space:create_index("multipart_index", {
        type = "HASH",
        parts = {
            {
                field = 1
            },
            {
                field = 2
            }
         }
     })

    return space
end

function helpers.create_space_with_bitset_index(engine)
    local space = create_space("bitset", engine)
    space:create_index("primary", {
        type = "TREE",
        parts = {
            {
                field = 1
            }
        }
    })
    space:create_index("index_for_first_name", {
        type = "BITSET",
        parts = {
            {
                field = 2,
                type = "string"
            }
        },
        unique = false
    })

    return space
end

t.after_suite(function()
    fio.rmtree(t.datadir)
end)

t.before_suite(function()
    t.datadir = fio.tempdir()
    box.cfg{
        wal_dir    = t.datadir,
        memtx_dir  = t.datadir,
        vinyl_dir  = t.datadir,
        -- vinyl_memory is changed to test test_mvcc_vinyl_tx_conflict.
        -- TODO: Commented out until tarantool/expirationd#105 is resolved.
        -- vinyl_memory = 1024,
    }

    local tree_code = [[function(tuple)
        if tuple[8] then
            return {string.sub(tuple[8],2,2)}
        end
        return {tuple[2]}
    end]]
    if _TARANTOOL >= "2" then
        box.schema.func.create("tree_func", {
            body = tree_code,
            is_deterministic = true,
            is_sandboxed = true
        })
    end
end)

function helpers.is_expired_true()
    return true
end

function helpers.is_metrics_supported()
    local is_package, metrics = pcall(require, "metrics")
    if not is_package then
        return false
    end
    -- metrics >= 0.11.0 is required
    local counter = require('metrics.collectors.counter')
    return metrics.unregister_callback and counter.remove
end

function helpers.iterate_with_func(task)
    return task.index:pairs(task.start_key(), { iterator = task.iterator_type })
       :take_while(
            function()
                return task:process_while()
            end
        )
end

helpers.iteration_result = {}
function helpers.is_expired_debug(_, tuple)
    table.insert(helpers.iteration_result, tuple)
    return true
end

function helpers.tarantool_version()
    local major_minor_patch = _G._TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    return major, minor, patch
end

function helpers.vinyl_is_broken()
    -- Blocked by https://github.com/tarantool/tarantool/issues/6448
    local major, minor, patch = helpers.tarantool_version()

    -- Since Tarantool 1.10.11.
    local broken_v1_10 = major >= 1 and (minor > 10 or minor == 10 and patch >= 11)

    -- Tarantool >= 2.1.0 and < 2.8.2.
    local broken_v2 = (major >= 2 and major < 3) and (minor >= 1 and minor < 8 or minor == 8 and patch <= 2)

    return broken_v1_10 or broken_v2
end

function helpers.memtx_func_index_is_broken()
    local major, minor, patch = helpers.tarantool_version()
    if major > 2 or (major == 2 and minor == 8 and patch >= 4) or (major == 2 and minor >= 10) then
        return false
    end
    return true
end

return helpers
