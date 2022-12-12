import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator


def matplot(r,c):
  #plot
  #run configuration 
  plt.rcParams['font.size']=8
  #plt.rcParams['font.family'] = 'Arial'
  plt.rcParams['axes.linewidth']=0.5
  plt.rcParams['axes.grid']=True
  plt.rcParams['grid.linestyle']='--'
  plt.rcParams['grid.linewidth']=0.2
  plt.rcParams["savefig.transparent"]='True'
  plt.rcParams['lines.linewidth']=0.8
  plt.rcParams['lines.markersize'] = 1
    
  #subplot
  fig,ax = plt.subplots(r,c)
  plt.subplots_adjust(left=None, bottom=None, right=None, top=None, hspace = None, wspace=0.5)
  
  return fig,ax
"""
x_major_locator = MultipleLocator(1)
y_major_locator = MultipleLocator(1)
ax[0].xaxis.set_major_locator(x_major_locator)
ax[0].yaxis.set_major_locator(y_major_locator)
ax[0].set_xlim(-6.5,3)
ax[0].set_ylim(-6.5,3)


plt.savefig('dncompare.pdf')
"""

  
  
  
  
  
  
  
  
