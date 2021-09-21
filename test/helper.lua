local t = require("luatest")
local fio = require("fio")

local helpers = require("luatest.helpers")

local function create_space(space_name)
    local space_format = {
        {name = "id", type = "number"},
        {name = "first_name", type = "string"},
        {name = "value", type = "number", is_nullable = true},
        {name = "count", type = "number", is_nullable = true},
        {name = "non_unique_id", type = "number", is_nullable = true, unique = false},
        {name = "json_path_field", is_nullable = true, unique = false},
        {name = "multikey_field", is_nullable = true},
        {name = "functional_field", is_nullable = true},
    }

    local options = {}
    if space_name == "vinyl" then
        options.engine = "vinyl"
    end
    local space = box.schema.create_space(space_name, options)
    space:format(space_format)

    return space
end

function helpers.create_space_with_tree_index()
    local space = create_space("tree")
    space:create_index("primary", {type = "TREE", parts={ 1 }})
    space:create_index("index_for_first_name", {type = "TREE", parts={ 2 }})
    space:create_index("multipart_index", {type = "TREE", parts={ {3, is_nullable = true}, {4, is_nullable = true} }})
    space:create_index("non_unique_index", {type = "TREE", parts={ {5, is_nullable = true} }, unique = false})

    if _TARANTOOL >= "2" then
        space:create_index("json_path_index",
                {type = "TREE", parts = { {6, type = "scalar", path = "age", is_nullable = true} }})
        space:create_index("multikey_index",
                {type = "TREE", parts = { {7, type = "str", path = "data[*].name"} }} )
        space:create_index("functional_index",
                {type = "TREE", parts={ {1, type = "string"} }, func = "tree_func"})
    end

    return space
end

function helpers.create_space_with_vinyl()
    local space = create_space("vinyl")
    space:create_index("primary", {type = "TREE", parts={ 1 }})
    space:create_index("index_for_first_name", {type = "TREE", parts={ 2 }})
    space:create_index("multipart_index", {type = "TREE", parts={ {3, is_nullable = true}, {4, is_nullable = true} }})
    space:create_index("non_unique_index", {type = "TREE", parts={ {5} }, unique = false })

    if _TARANTOOL >= "2" then
        space:create_index("json_path_index",
                {type = "TREE", parts = { {6, type = "scalar", path = "age", is_nullable = true} }})
        space:create_index("multikey_index",
                {type = "TREE", parts = { {7, type = "str", path = "data[*].name", is_nullable = true} }})
    end

    return space
end

function helpers.create_space_with_hash_index()
    local space = create_space("hash")
    space:create_index("primary", {type = "HASH", parts={ 1 }} )
    space:create_index("index_for_first_name", {type = "HASH", parts={ 2 }} )
    space:create_index("multipart_index", {type = "HASH", parts={ {1}, {2} }})

    return space
end

function helpers.create_space_with_bitset_index()
    local space = create_space("bitset")
    space:create_index("primary", {type = "TREE", parts={ 1 }})
    space:create_index("index_for_first_name",
            {type = "BITSET", parts={ {field = 2, type = "string"} }, unique = false})

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
        vinyl_memory = 1024,
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

return helpers
