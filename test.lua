box.cfg{}

if box.space.origin == nil then
   a = box.schema.create_space('origin')
   a:create_index('first', {type = 'TREE', parts = {1, 'NUM'}})
else
   box.space.origin:truncate()
end
if box.space.cemetery == nil then
   b = box.schema.create_space('cemetery')
   b:create_index('first', {type = 'TREE', parts = {1, 'STR'}})
else
   box.space.cemetery:truncate()
end
expd = require('expirationd')
expd._debug = true

expd.do_test('origin', 'cemetery')

os.exit()
