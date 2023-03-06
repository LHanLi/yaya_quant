import scipy
from yaya_quant.re import basic
import pandas as pd


# feature matrix X  n smaple * m feature
# predict value y, and discrete number y_n
# return chi2, p
def continue_chi2(X,y,y_n):
    from sklearn.preprocessing import LabelBinarizer
    # (1,0,0) (0,1,0) (0,0,1) for three label
    y = LabelBinarizer().fit_transform(basic.class_continue(y,y_n))
    observed = X.T.dot(y) # n feature * y_n
    
    feature_sum = X.sum(axis=0).reshape(1,-1)  # if class n is 1,1,1,1...   1*n feature matrix
    class_prob = y.mean(axis=0).reshape(1,-1) # 1*y_n matrix
    expected = feature_sum.T.dot(class_prob)  #  n feature * y_n
    
    chi2_statistic = ((observed - expected)**2/expected).sum(axis=1)
    
    return chi2_statistic, scipy.special.chdtrc(y_n - 1, chi2_statistic)

# array of label, a, b
def discrete_chi2(a,b):
    observe_matrix = pd.crosstab(a,b)
    total_sum = observe_matrix.sum().sum()
    a_sum = observe_matrix.sum(axis=0).values
    b_sum = observe_matrix.sum(axis=1).values
    expect_matrix = b_sum.reshape(1,-1).T.dot(a_sum.reshape(1,-1))/total_sum

    chi2_statistic = ((observe_matrix-expect_matrix)**2/expect_matrix).sum().sum()
    freedom_degree = (observe_matrix.shape[0]-1)*(observe_matrix.shape[1]-1)    

    return chi2_statistic, freedom_degree, scipy.special.chdtrc(y_n - 1, chi2_statistic)
  
