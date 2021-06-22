local t = require("luatest")
local fio = require("fio")

local helpers = require("luatest.helpers")

t.before_suite(function()
    t.datadir = fio.tempdir()
    box.cfg{
        wal_dir    = t.datadir,
        memtx_dir  = t.datadir,
        vinyl_dir  = t.datadir,
        vinyl_memory = 1024,
    }

    local tree = box.schema.create_space("tree", { if_not_exists = true })
    tree:format({
        {name = "id", type = "number"},
        {name = "first_name", type = "string"},
        {name = "value", type = "number", is_nullable = true},
        {name = "count", type = "number", is_nullable = true},
        {name = "non_unique_id", type = "number", is_nullable = true, unique = false},
        {name = "json_path_field", is_nullable = true, unique = false},
        {name = "multikey_field", is_nullable = true},
        {name = "functional_field", is_nullable = true},
    })
    tree:create_index("primary", {type = "TREE", parts={ 1 }})
    tree:create_index("index_for_first_name", {type = "TREE", parts={ 2 }})
    tree:create_index("multipart_index", {type = "TREE", parts={ {3, is_nullable = true}, {4, is_nullable = true} }})
    tree:create_index("non_unique_index", {type = "TREE", parts={ {5, is_nullable = true} }, unique = false})

    local hash = box.schema.create_space("hash", { if_not_exists = true })
    hash:format({
        {name = "id", type = "number"},
        {name = "first_name", type = "string"},
        {name = "value", type = "number", is_nullable = true},
        {name = "count", type = "number", is_nullable = true}
    })
    hash:create_index("primary", {type = "HASH", parts={ 1 }} )
    hash:create_index("index_for_first_name", {type = "HASH", parts={ 2 }} )
    hash:create_index("multipart_index", {type = "HASH", parts={ {1}, {2} }})

    local vinyl = box.schema.create_space("vinyl", { if_not_exists = true, engine = "vinyl" })
    vinyl:format({
        {name = "id", type = "number"},
        {name = "first_name", type = "string"},
        {name = "value", type = "number", is_nullable = true},
        {name = "count", type = "number", is_nullable = true},
        {name = "non_unique_id", type = "number", is_nullable = true, unique = false},
        {name = "json_path_field", is_nullable = true, unique = false},
        {name = "multikey_field", is_nullable = true, unique = false},
    })
    vinyl:create_index("primary", {type = "TREE", parts={ 1 }})
    vinyl:create_index("index_for_first_name", {type = "TREE", parts={ 2 }})
    vinyl:create_index("multipart_index", {type = "TREE", parts={ {3, is_nullable = true}, {4, is_nullable = true} }})
    vinyl:create_index("non_unique_index", {type = "TREE", parts={ {5} }, unique = false })

    if _TARANTOOL >= "2" then
        local tree_code = [[function(tuple)
            if tuple[8] then
                return {string.sub(tuple[8],2,2)}
            end
            return {tuple[2]}
        end]]
        box.schema.func.create("tree_func",
                {body = tree_code, is_deterministic = true, is_sandboxed = true})
        tree:create_index("json_path_index",
                {type = "TREE", parts = { {6, type = "scalar", path = "age", is_nullable = true} }})
        tree:create_index("multikey_index",
                {type = "TREE", parts = { {7, type = "str", path = "data[*].name"} }} )
        tree:create_index("functional_index",
                {type = "TREE", parts={ {1, type = "string"} }, func = "tree_func"})

        vinyl:create_index("json_path_index",
                {type = "TREE", parts = { {6, type = "scalar", path = "age", is_nullable = true} }})
        vinyl:create_index("multikey_index",
                {type = "TREE", parts = { {7, type = "str", path = "data[*].name", is_nullable = true} }})
    end

    local bitset = box.schema.create_space("bitset", { if_not_exists = true })
    bitset:create_index("primary", {type = "TREE", parts={ 1 }})
    bitset:create_index("index_for_first_name",
            {type = "BITSET", parts={ {field = 2, type = "string"} }, unique = false})
end)

t.after_suite(function()
    fio.rmtree(t.datadir)
end)

function helpers.init_spaces(g)
    g.tree = box.space.tree
    g.hash = box.space.hash
    g.vinyl = box.space.vinyl
    g.bitset = box.space.bitset
end

function helpers.truncate_spaces(g)
    g.tree:truncate()
    g.hash:truncate()
    g.vinyl:truncate()
end

function helpers.is_expired_true()
    return true
end

helpers.iteration_result = {}
function helpers.is_expired_debug(_, tuple)
    table.insert(helpers.iteration_result, tuple)
    return true
end

return helpers
