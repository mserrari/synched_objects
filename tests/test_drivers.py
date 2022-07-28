import json
import os

from . import TESTING_FOLDER
from synched_objects.drivers import JsonDriver

def test_json_driver():

    file = TESTING_FOLDER / 'test_json_driver.json'
    file.unlink(missing_ok=True)
    
    drv = JsonDriver(filename=file, overwrite=True, append=False)
    
    data = []
    
    for j in range(4):
        t = [{'index': i, 'data': i**2} for i in range(j*5,(j+1)*5)]
        data.extend(t)
        drv.write(t)    
    del drv
    
    drv = JsonDriver(filename=file, overwrite=False, append=True)
    a = [{'index': i, 'data': i**2} for i in range(-3,0)]
    data.extend(a)
    drv.write(a)
    del drv
    
    
    drv = JsonDriver(filename=file, overwrite=False, append=True)
    b = [{'index': i**3, 'data': i/5} for i in range(-50,-40)]
    data.extend(b)
    drv.write(b)
    del drv
    
    
    loaded_data = json.load(open(file, mode='r'))
    
    assert len(data) == len(loaded_data)
    assert data == loaded_data

def test_rb_ab():
    
    file = TESTING_FOLDER / 'test_ab_rb.json'

    string = 'ABCDEFGHIJKLMONQRSTUVWXYZ'
    
    with open(file, mode='wb+') as fp:
        modified_string = string[:-2] + 'XX'
        fp.write(modified_string.encode())
    
    with open(file, mode='rb+') as fp:
        fp.seek(-2, os.SEEK_END)
        fp.truncate()
        fp.write('YZ'.encode())
    
    with open(file, mode='rb') as fp:
        line = fp.read()
        
        assert len(line) == len(string)
        assert line.decode() == string
