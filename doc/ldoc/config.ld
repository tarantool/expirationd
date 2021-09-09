file = 'expirationd'
title = 'expirationd API reference'

template = 'doc/ldoc/assets'
style = 'doc/ldoc/assets'
no_lua_ref = true
--no_summary = true

tparam_alias('table', 'table')
tparam_alias('integer', 'integer')
tparam_alias('boolean', 'boolean')

alias('array', function(tags, value, modifiers)
    if modifiers == nil then
        return 'param', value, {type = '{...}'}
    end

    -- next() is not in the scope.
    local subtype = modifiers.number ~= nil and 'number' or
        modifiers.string ~= nil and 'string' or
        modifiers.table ~= nil and 'table' or
        modifiers.integer ~= nil and 'integer' or
        modifiers.boolean ~= nil and 'boolean'
    return 'param', value, {type = ('{%s, ...}'):format(subtype)}
end)
alias('anchor', 'table')

-- Convert markdown reference style links into HTML.
local function convert_markdown_references(text)
    local refs = {}
    for _, line in pairs(text:split('\n')) do
       local anchor, url = line:lstrip():match('^%[([0-9])%]: (.*)$')
       if anchor then
          refs[anchor] = url
       end
    end

    for anchor, url in pairs(refs) do
        text = text:gsub('\n *%[' .. anchor .. '%]: [^\n]*', '')
        text = text:gsub('%[(.-)%]%[' .. anchor .. '%]',
            '<a href="' .. refs[anchor] .. '">%1</a>')
    end
    return text
end

-- Convert three-backticks code blocks to `<pre></pre>` HTML
-- elements.
local function convert_markdown_codeblocks(text)
    local res = ''
    for _, block in pairs(text:split('\n\n')) do
        local codeblock = block:match('^ *```\n(.*)[\n ]*```[\n ]*$')
        if codeblock then
            block = '<pre>' .. codeblock:rstrip() .. '</pre>\n'
        end
        res = res .. '\n\n' .. block
    end
    return res:sub(3)
end

-- Convert backticks to `<code></code>` HTML elements.
local function convert_markdown_backticks(text)
    return text:gsub('`(.-)`', '<code>%1</code>')
end

-- Convert blocks of lines starting from '- ' to an HTML list.
local function convert_markdown_lists(text)
    local res = ''
    for _, block in pairs(text:split('\n\n')) do
        local list = '<ul class="markdown_list">\n'
        local is_list = true
        for _, line in pairs(block:split('\n')) do
            local list_item = line:match('^ *%- (.*)$')
            is_list = is_list and list_item
            if not is_list then
                break
            end
            list = list .. '<li>' .. list_item .. '</li>\n'
        end
        list = list .. '</ul>'
        block = is_list and list or block
        res = res .. '\n\n' .. block
    end
    return res:sub(3)
end

-- Convert GFM syntax markdown tables into HTML tables.
local function convert_markdown_tables(text)
    local res = ''
    for _, block in pairs(text:split('\n\n')) do
        local tbl = '<table class="markdown_table">\n'
        local is_table = true
        local table_line = 1
        for _, line in pairs(block:split('\n')) do
            is_table = is_table and (line == '' or line:match('^ *|'))
            if not is_table then
                break
            end
            if line ~= '' then
                tbl = tbl .. '<tr>\n'
                for _, table_item in pairs(line:split('|')) do
                    if not table_item:match('^[ -]*$') then
                        if table_line == 1 then
                            tbl = tbl .. '<th>' .. table_item:strip() .. '</th>\n'
                        else
                            tbl = tbl .. '<td>' .. table_item:strip() .. '</td>\n'
                        end
                    end
                end
                tbl = tbl .. '</tr>\n'
                table_line = table_line + 1
            end
        end
        tbl = tbl .. '</table>\n'
        block = is_table and tbl or block
        res = res .. '\n\n' .. block
    end
    return res:sub(3)
end

local function highlight_xxx_notes(text)
    local from = 'XXX'
    local to = '<span class="xxx">XXX</span>'

    -- Don't wrap several times.
    text = text:gsub(to, '\x01')
    text = text:gsub(from, to)
    return text:gsub('\x01', to)
end

-- Apply a transformation `fun` to text properties of
-- given `item`, which are expected to be multiparagraph
-- free form text.
local function apply_to_descriptions(fun, item)
    if item.summary then
        item.summary = fun(item.summary)
    end

    if item.description then
        item.description = fun(item.description)
    end

    if item.params and item.params.map then
        for k, v in pairs(item.params.map) do
            item.params.map[k] = fun(v)
        end
    end

    if item.retgroups then
        for _, group in ipairs(item.retgroups) do
            for _, group_item in ipairs(group) do
                if group_item.text then
                    group_item.text = fun(group_item.text)
                end
            end
        end
    end
end

custom_display_name_handler = function(item, default_handler)
    apply_to_descriptions(convert_markdown_references, item)
    apply_to_descriptions(convert_markdown_codeblocks, item)
    apply_to_descriptions(convert_markdown_backticks, item)
    apply_to_descriptions(convert_markdown_lists, item)
    apply_to_descriptions(convert_markdown_tables, item)
    apply_to_descriptions(highlight_xxx_notes, item)
    return default_handler(item)
end

-- vim: ft=lua:
