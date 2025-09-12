-- Utility merges luacov coverage statistic files by coverage percentage into a single result
-- USAGE: <output_file> <input_file1> <input_file2> <input_file3> ...

local function read_file(filename)
    local file = io.open(filename, "r")
    if not file then
        error("Failed to open file: " .. filename)
    end

    local data = {}
    for line in file:lines() do
        table.insert(data, line)
    end

    file:close()
    return data
end

local function merge_coverage_data(files)
    local coverage_data = {}

    for _, filename in ipairs(files) do
        local data = read_file(filename)

        for i = 1, #data, 2 do
            local header = data[i]
            local counts = data[i + 1]

            local file_path = header:match(":(.+)")
            local line_counts = {}

            for count in counts:gmatch("%d+") do
                table.insert(line_counts, tonumber(count))
            end

            if not coverage_data[file_path] then
                coverage_data[file_path] = line_counts
            else
                for j = 1, #line_counts do
                    coverage_data[file_path][j] = (coverage_data[file_path][j] or 0) + line_counts[j]
                end
            end
        end
    end

    return coverage_data
end

local function write_merged_data_to_file(coverage_data, output_filename)
    local file = io.open(output_filename, "w")
    if not file then
        error("Failed to open file for writing: " .. output_filename)
    end

    for file_path, counts in pairs(coverage_data) do
        file:write(#counts .. ":" .. file_path .. "\n")
        file:write(table.concat(counts, " ") .. "\n")
    end

    file:close()
end

local files_list = table.deepcopy(arg)
files_list[-1] = nil
files_list[0] = nil
local output_filename = files_list[1]
table.remove(files_list, 1)

local merged_data = merge_coverage_data(files_list)
write_merged_data_to_file(merged_data, output_filename)
print("Luacovs merge: Done")
