

################################################## matplotlib  ##############################################
# in jupyter
# import matplotlib; matplotlib.use('Agg')  
# pylint: disable=multiple-statements
# row, column, 'col', 'row', width, high
def matplot(r=1, c=1, sharex=False, sharey=False, w=8, d=5):
  import seaborn as sns
  import matplotlib.pyplot as plt
  # don't use sns style
  sns.reset_orig()
  #plot
  #run configuration 
  plt.rcParams['font.size']=14
  plt.rcParams['font.family'] = 'KaiTi'
  #plt.rcParams['font.family'] = 'Arial'
  plt.rcParams["axes.unicode_minus"]=False #该语句解决图像中的“-”负号的乱码问题
  plt.rcParams['axes.linewidth']=1
  plt.rcParams['axes.grid']=True
  plt.rcParams['grid.linestyle']='--'
  plt.rcParams['grid.linewidth']=0.2
  plt.rcParams["savefig.transparent"]='True'
  plt.rcParams['lines.linewidth']=0.8
  plt.rcParams['lines.markersize'] = 1
   
  #保证图片完全展示
  plt.tight_layout()
    
  #subplot
  fig,ax = plt.subplots(r,c,sharex=sharex, sharey=sharey,figsize=(w,d))
  plt.subplots_adjust(left=None, bottom=None, right=None, top=None, hspace = None, wspace=0.5)
  
  return plt, fig, ax
"""
# multi legend
l1, = ax.plot***
l2, = ax2.plot***
plt.legend((l1,l2),(**,**))

# 保存图片去除白边
plt.savefig('dncompare.pdf',bbox_inches='tight')

# 设置刻度
x_major_locator = MultipleLocator(1)
y_major_locator = MultipleLocator(1)
ax[0].xaxis.set_major_locator(x_major_locator)
ax[0].yaxis.set_major_locator(y_major_locator)
ax[0].set_xlim(-6.5,3)
ax[0].set_ylim(-6.5,3)

# 中文楷体
import matplotlib
matplotlib.rc("font",family='KaiTi')
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
FONT = matplotlib.font_manager.FontProperties(fname = './FangSong.ttf')
# fontproperties = FONT

# 图片保存
plt.savefig
# 防止被裁剪掉信息
bbox_inches = 'tight'
"""
  




