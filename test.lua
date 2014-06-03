box.cfg{}

a = box.schema.create_space('origin')
a:create_index('first', {type = 'tree', parts = {0, 'NUM'}})
b = box.schema.create_space('cemetery')
b:create_index('first', {type = 'tree', parts = {0, 'STR'}})

expd = require('expirationd')
expd._debug = true

expd.do_test('origin', 'cemetery')

os.exit()
