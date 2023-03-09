local t = require("luatest")
local g = t.group('expirationd_versioning')
local expirationd = require('expirationd')

g.test_version = function()
    t.assert_type(expirationd._VERSION, 'string')
    t.assert_not_equals(string.find(expirationd._VERSION, "^%d+%.%d+%.%d+$"), nil)
end
