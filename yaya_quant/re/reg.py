from sklearn import linear_model
from sklearn.datasets import make_regression
from sklearn.metrics import mean_squared_error 
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LassoLarsIC
from sklearn.pipeline import make_pipeline


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
  
 
def assess(reg_model, fit_corr, fit_value):    
# assess
    rmse = np.sqrt(mean_squared_error(fit_value,reg_model.predict(fit_corr)))
# plot
    m, s, _ = plt.stem(
    np.where(reg_Lasso.coef_)[0],
    reg_Lasso.coef_[reg_Lasso.coef_ != 0],
    markerfmt="x",
    label="coef"
    )
    plt.setp([m, s], color="#2ca02c")
    
    plt.legend(loc="best")
    plt.title("RMESE: %.3f " %(rmse))
# save
    plt.savefig('coef.pdf')
    return rmse
  
