from sklearn import linear_model
from sklearn.datasets import make_regression 
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LassoLarsIC
from sklearn.pipeline import make_pipeline
import numpy as np
import matplotlib.pyplot as plt

# 一元线性回归
# x_t,y_t is array
# y_t = alpha + beta x_t + epsilon_t
def L_Reg(x, y):
    lxx = ((x-x.mean())**2).sum()
    lyy = ((y-y.mean())**2).sum()
    lxy = ((x-x.mean())*(y-y.mean())).sum()

# 斜率与截距
    beta = lxy/lxx
    alpha = y.mean() - beta*x.mean()

    return beta, alpha




# reg
def Lasso_reg(fit_corr, fit_value):
    # Cross validation 10, finding optimal alpha of Lasso
    model = make_pipeline(StandardScaler(), LassoCV(cv=10)).fit(fit_corr, fit_value)

    lasso = model[-1]

    min_index = np.where(lasso.mse_path_.mean(axis=-1)==min(lasso.mse_path_.mean(axis=-1)))
    alpha = lasso.mse_path_.mean(axis=-1)[min_index][0]


    # Lasso Regresion
    reg_Lasso = lasso
    reg_Lasso.fit(fit_corr, fit_value)

    return reg_Lasso
  
    
def OLS_reg(fit_corr,fit_value):
    reg_OLS = linear_model.LinearRegression(fit_intercept=False)
    reg_OLS.fit(fit_corr, fit_value)
    return reg_OLS

 
def assess(reg_model, fit_corr, fit_value):    
# assess
    rmse = (reg_model.predict(fit_corr)-fit_value).std()
    R2 = 1-rmse**2/np.array(fit_value).std()**2 
# plot
    m, s, _ = plt.stem(
    np.where(reg_model.coef_)[0],
    reg_model.coef_[reg_model.coef_ != 0],
    markerfmt="x",
    label="coef"
    )
    plt.setp([m, s], color="#2ca02c")
    
    plt.legend(loc="best")
    plt.title("RMSE: %.3f $R^2$ = %.3f" %(rmse,R2))
# save
    plt.savefig('coef.pdf')
    return rmse,R2
  
