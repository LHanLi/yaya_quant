import dill
import pickle


# 反序列化一个对象

def save_pkl(obj,filename,serial=False):
    if serial:
        obj = dill.dumps(obj)
    with open(filename,'wb') as file:
        pickle.dump(obj,file)

def read_pkl(filename,serial=False):
    with open(filename,'rb') as file:
        obj = pickle.load(file)
        if serial:
            obj = dill.loads(obj)
        return obj
