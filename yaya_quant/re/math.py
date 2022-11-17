import numpy as np
import copy


# convert continue variable to discrete and count
def scatter_continue(_a,lena):
    a = copy.copy(_a)
    a.sort()
    a_d = (a[-1]-a[0])/lena
    a_count = np.zeros(lena)
    a_cut = a[0] + a_d
    j = 0
    for i in a:
        if(i<=a_cut):
            a_count[j] += 1
        else:
            j += 1
            a_count[j] += 1
            a_cut += a_d
    return a_count

# label = 0, 1, 2, ...
def class_continue(_a,lena):
    a_max = a.max()
    a_min = a.min()
    new_a = -np.ones(len(a))
    linspace = np.linspace(a_min,a_max,n+1)
    
    for i in range(len(a)):
    diff = a[i]-linspace
    for j in range(len(diff)):
        if diff[j] >= 0:
            continue
        else:
            new_a[i]=j-1
            break
            
    return new_a
