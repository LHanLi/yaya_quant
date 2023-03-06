

def save_pkl(a,filename):
    import pickle
    with open(filename,'wb') as file:
        pickle.dump(a,file)

def read_pkl(filename):
    import pickle
    with open(filename,'rb') as file:
        a = pickle.load(file)
        return a
