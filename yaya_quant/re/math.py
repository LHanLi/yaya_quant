import numpy as np



# convert continue variable to discrete
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
