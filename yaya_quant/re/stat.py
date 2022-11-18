import scipy



# feature matrix X  n smaple * m feature
# predict value y, and discrete number y_n
def continue_chi2(X,y,y_n):
    from sklearn.preprocessing import LabelBinarizer
    # (1,0,0) (0,1,0) (0,0,1) for three label
    y = LabelBinarizer().fit_transform(yaya_math.class_continue(y,y_n))
    observed = X.T.dot(y) # n feature * y_n
    
    feature_sum = X.sum(axis=0).reshape(1,-1)  # if class n is 1,1,1,1...   1*n feature matrix
    class_prob = y.mean(axis=0).reshape(1,-1) # 1*y_n matrix
    expected = feature_sum.T.dot(class_prob)  #  n feature * y_n
    
    chi2_statistic = ((observed - expected)**2/expected).sum(axis=1)
    
    return chi2_statistic, scipy.special.chdtrc(y_n - 1, chi2_statistic)
  
  
  
  
