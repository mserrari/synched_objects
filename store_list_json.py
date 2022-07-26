from pathlib import Path
import json

class StoreListJson():
    
    def __init__(self, filename: str, frequency: int = 100, overwrite: bool = True) -> None:
        
        """ This a list wrapper that auto saves list content to a json in disk
        
        filename: path to json where to save data
        frequency: frequency of the disk flush
        overwrite: overwrite existing file
        
        """
        assert isinstance(frequency, int)
        assert frequency > 0
        assert isinstance(overwrite, bool)
        assert isinstance(filename, (str, Path))
        
        self.filename = Path(filename)
        self.frequency = frequency
        self.overwrite = overwrite
        
        if not self.overwrite and self.filename.is_file():
            raise FileExistsError('Cannot overwrite file')
        
        self.data = []
        self.lastflush = 0
        self.file = open(self.filename, "w")
        
        
    def push(self, element):
        """Add new element to list"""
        self.lastflush += 1
        self.data.append(element)
        
        if self.lastflush >= self.frequency:
            self.flush()
            
    def flush(self):
        """Flush data to disk"""
        self.file.seek(0)
        json.dump(self.data, self.file, indent=4)
        self.lastflush = 0
        
        
    def __del__(self):
        """Auto flush to disk when object is removed"""
        if hasattr(self, 'file'):
            self.flush()
            self.file.close()
       
       
if __name__ == '__main__':
    
    l = StoreListJson('store.json', frequency=5, overwrite=True)
    
    for i in range(99):
        l.push({'index': i, 'data': i**2})
        