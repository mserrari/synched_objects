import json

from synched_objects.drivers import JsonDriver
from synched_objects.synched_lists import SynchedList, JsonSynchedList, DriverSynchedList

from . import TESTING_FOLDER

def data_generator(total=10):
    data = [{'index': i, 'data': i**2} for i in range(total)]
    i = 0
    while i < len(data):
        
        if i % 2 == 0 or i == len(data) - 1:
            yield data[i], 'append'
            i += 1
        else:
            yield data[i:i+3], 'extend'
            i += 3
            
def fill_with_data(l: SynchedList, total=20):
    data_gen = data_generator(total=total)
    output = list()
    
    for d, mode in data_gen:
        if mode == 'append':
            l.append(d)
            output.append(d)
        else:
            l.extend(d)
            output.extend(d)
            
    return output
    
def test_jsonsynchedlist():
    
    file = TESTING_FOLDER / 'json_synched_list.json'
    l = JsonSynchedList(file,
                        frequency=5,
                        overwrite=True)
    
    data = fill_with_data(l)
    l.flush() # flush
    
    with open(file, 'rb') as f:
        loaded_data = json.load(f)

    assert len(l) == len(data)
    assert len(data) == len(loaded_data)
    assert data == loaded_data
   
def test_driversynchedlist_jsondriver(total=20, frequency=5):
    
    """
    Testing append and extend of synched list using the json driver
    """
    
    file = TESTING_FOLDER / 'driver_synched_list.json'
    driver = JsonDriver(file, overwrite=True, append=True)
    l = DriverSynchedList(driver, frequency=frequency)

    data = fill_with_data(l)
    l.flush()
    
    with open(file, 'rb') as f:
        loaded_data = json.load(f)

    assert len(l) == len(data)
    assert len(data) == len(loaded_data)
    assert data == loaded_data