import pandas as pd
import numpy as np

# 做任意两品种的某种属性之差
# attr： close open high low volume amount open_interest
# suppose main_A = 10, main_B = 01, zhengtao = True 2210-2301，  False  2210-2101
def plot_diff(attr='close',multi_A=1,secu_A='RB',main_A='10',multi_B=1,secu_B='RB',main_B='01',zhengtao=True):
    from sqlalchemy import create_engine
    from bokeh.plotting import figure, output_notebook, show ,save
    
    # read data from THS.trading_daily_price_1, 2006-6-13 之后
    starttime = pd._libs.tslibs.timestamps.Timestamp('2006-06-13 00:00:00')
    engine = create_engine('mysql+pymysql://root:a23187@127.0.0.1/THS')
    df_price =  pd.read_sql_table('trading_daily_price_1',engine,columns=['date','code','open','high','low','close','volume','open_interest'])
    df_price = df_price[df_price['date']>starttime]
    
    # 获取 secuA和secuB指定月份合约
    dataA = df_price[df_price.code.apply(lambda x: secu_A == ''.join([i for i in x[:2] if i.isalpha()]) and x.split('.')[0][-2:] == main_A)]
    # 提取code中是字母的部分，'.'前两位代表月份
    dataB = df_price[df_price.code.apply(lambda x: secu_B == ''.join([i for i in x[:2] if i.isalpha()]) and x.split('.')[0][-2:] == main_B)]


    # remove jiaogeyue
    dataA = dataA[dataA['date'].apply(lambda x: x.date().month != int(main_A))]
    dataB = dataB[dataB['date'].apply(lambda x: x.date().month != int(main_B))]

    # on the same day
    plot_df = dataA.merge(dataB, on='date')

    # attr of A and B
    compareA = attr + '_x'
    compareB = attr + '_y'
    # 差值
    plot_df['diff'] = multi_A * plot_df[compareA] - multi_B * plot_df[compareB]

    # sort by date
    plot_df.sort_values('date')
    #    plot_df['year'] = plot_df['date'].apply(lambda x: x.year)
    #    plot_df['month'] = plot_df['date'].apply(lambda x: x.date().month)
    # 获取code中代表年份月份信息，e.g. 2210 of RB2210
    code_x_line = plot_df.code_x.apply(lambda x: x.split('.')[0][-4:])
    code_y_line = plot_df.code_y.apply(lambda x: x.split('.')[0][-4:])
    # CZC品种2020年之后合约代码只有3位，例如210代表2210，将其补齐。
    code_x_line = code_x_line.apply(lambda x: '2' + x[1:] if(x[0].isalpha()) else x)
    code_y_line = code_y_line.apply(lambda x: '2' + x[1:] if(x[0].isalpha()) else x)
    # 2210<2301
    zhengtao_line = code_x_line <= code_y_line
    # 2210<2301 , 2110<2201 ...
    if(zhengtao == True):
        plot_df = plot_df[zhengtao_line]
    else:
        plot_df = plot_df[zhengtao_line.apply(lambda x:not x)]

    # all unique code (secu_A)
    code_list = list(plot_df['code_x'].unique())
    code_list.sort()
    # first is nearest
    code_list.reverse()


    from bokeh.models import ColumnDataSource, HoverTool
    from bokeh.plotting import figure, output_file, show
    from bokeh.io import output_notebook
#    from bokeh.models.widgets import CheckboxGroup

# list of colors RGB
    colors =[(255,0,0),(255,182,193),(255,0,255),(75,0,130),(100,149,237),(0,191,255),
             (0,255,255),(60,179,113),(255,255,0),(255,165,0),
             (128,128,128),(3,21,193),(0,128,128),(255,121,193),]

    # show in notebook
#    output_notebook()
    # datetime zuhouyiqi

    # get nearest secu's start and end date
    cur_df = plot_df[plot_df.code_x == code_list[0]]
    start_date = cur_df.iloc[0].date
    end_date = cur_df.iloc[-1].date
    # expression of value and period
    title = "%d %s %s- %d %s %s  %s-%s"%(multi_A,secu_A,main_A,multi_B,secu_B,main_B,start_date,end_date)

    # bokeh figure
    p = figure(  
       tools="pan,box_zoom,reset,save",  
       x_axis_label='date', y_axis_label='diff'  ,width=900, height=500
    )  

    color_cnt = 0
    # every secu_A in list
    for i in range(len(code_list)):
        cur_df = plot_df[plot_df.code_x == code_list[i]]
        # line of first/nearest secu is red and thicker
        if(i == 0):
        # x seq of date to 0,1,2,3,...
            p.line(np.arange(len(cur_df['date'])), cur_df['diff'], legend=code_list[i], line_width=3, line_color=colors[color_cnt])
        else:
            p.line(np.arange(len(cur_df['date'])), cur_df['diff'], legend=code_list[i], line_width=1, line_color=colors[color_cnt])
        color_cnt = color_cnt + 1

# put legend at right and add hover print values(y)
    p.add_layout(p.legend[0], 'right')
    h = HoverTool(tooltips=[
        ('Values', '@y')
    ])
    p.add_tools(h)

# decorte
    p.title.text = title
    p.legend.location = "top_left"
    p.grid.grid_line_alpha = 0
    p.xaxis.axis_label = 'Date'
    p.yaxis.axis_label = 'Value'
    p.ygrid.band_fill_color = "olive"
    p.ygrid.band_fill_alpha = 0.1


    show(p) 
    save(p,filename='diff.html')


# plot table diff muti secu
def plot_diff_table(diff_list):
    from bokeh.models import ColumnDataSource, HoverTool
    from bokeh.plotting import figure, output_file, show
    from bokeh.io import output_notebook
    
    
    # save file
    output_file(filename="diff.html", title="%s.html"%diff_list)
    
    
    
    from sqlalchemy import create_engine
    from bokeh.plotting import figure, output_notebook, show ,save
    
    # read data from THS.trading_daily_price_1, 2010-6-13 之后
    starttime = pd._libs.tslibs.timestamps.Timestamp('2010-06-13 00:00:00')
    engine = create_engine('mysql+pymysql://root:a23187@127.0.0.1/THS')
    df_price =  pd.read_sql_table('trading_daily_price_1',engine,columns=['date','code','open','high','low','close','volume','open_interest'])
    df_price = df_price[df_price['date']>starttime]
    
    # broker 多图并列
    layout_name = [] 
   # 创建变量的list
    createVar = locals()
    for i in range(len(diff_list)):  
        createVar['layout'+ str(i)] = 0
        layout_name.append(createVar['layout'+ str(i)])
    count = 0
    
    for i in range(len(diff_list)):
        attr = diff_list[i][0]
        multi_A = diff_list[i][1]
        secu_A = diff_list[i][2]
        main_A = diff_list[i][3]
        multi_B = diff_list[i][4]
        secu_B = diff_list[i][5]
        main_B = diff_list[i][6]
        zhengtao = diff_list[i][7]
        
        
        # 获取 secuA和secuB指定月份合约
        dataA = df_price[df_price.code.apply(lambda x: secu_A == ''.join([i for i in x[:2] if i.isalpha()]) and x.split('.')[0][-2:] == main_A)]
        # 提取code中是字母的部分，'.'前两位代表月份
        dataB = df_price[df_price.code.apply(lambda x: secu_B == ''.join([i for i in x[:2] if i.isalpha()]) and x.split('.')[0][-2:] == main_B)]


        # remove jiaogeyue
        dataA = dataA[dataA['date'].apply(lambda x: x.date().month != int(main_A))]
        dataB = dataB[dataB['date'].apply(lambda x: x.date().month != int(main_B))]

        # on the same day
        plot_df = dataA.merge(dataB, on='date')

        # attr of A and B
        compareA = attr + '_x'
        compareB = attr + '_y'
        # 差值
        plot_df['diff'] = multi_A * plot_df[compareA] - multi_B * plot_df[compareB]

        # sort by date
        plot_df.sort_values('date')
        #    plot_df['year'] = plot_df['date'].apply(lambda x: x.year)
        #    plot_df['month'] = plot_df['date'].apply(lambda x: x.date().month)
        # 获取code中代表年份月份信息，e.g. 2210 of RB2210
        code_x_line = plot_df.code_x.apply(lambda x: x.split('.')[0][-4:])
        code_y_line = plot_df.code_y.apply(lambda x: x.split('.')[0][-4:])
        # CZC品种2020年之后合约代码只有3位，例如210代表2210，将其补齐。
        code_x_line = code_x_line.apply(lambda x: '2' + x[1:] if(x[0].isalpha()) else x)
        code_y_line = code_y_line.apply(lambda x: '2' + x[1:] if(x[0].isalpha()) else x)
        # 2210<2301
        zhengtao_line = code_x_line <= code_y_line
        # 2210<2301 , 2110<2201 ...
        if(zhengtao == True):
            plot_df = plot_df[zhengtao_line]
        else:
            plot_df = plot_df[zhengtao_line.apply(lambda x:not x)]

        # all unique code (secu_A)
        code_list = list(plot_df['code_x'].unique())
        code_list.sort()
        # first is nearest
        code_list.reverse()

    #    from bokeh.models.widgets import CheckboxGroup

    # list of colors RGB
        colors =[(255,0,0),(255,182,193),(255,0,255),(75,0,130),(100,149,237),(0,191,255),
                 (0,255,255),(60,179,113),(255,255,0),(255,165,0),
                 (128,128,128),(3,21,193),(0,128,128),(255,121,193),]

        # show in notebook
    #    output_notebook()
        # datetime zuhouyiqi

        # get nearest secu's start and end date
        cur_df = plot_df[plot_df.code_x == code_list[0]]
        start_date = cur_df.iloc[0].date
        end_date = cur_df.iloc[-1].date
        # expression of value and period
        title = "%d %s %s- %d %s %s  %s-%s"%(multi_A,secu_A,main_A,multi_B,secu_B,main_B,start_date,end_date)

        # bokeh figure
        p = figure(  
           tools="pan,box_zoom,reset,save",  
           x_axis_label='date', y_axis_label='diff'  ,width=900, height=500
        )  

        color_cnt = 0
        # every secu_A in list
        for i in range(len(code_list)):
            cur_df = plot_df[plot_df.code_x == code_list[i]]
            # line of first/nearest secu is red and thicker
            if(i == 0):
            # x seq of date to 0,1,2,3,...
                p.line(np.arange(len(cur_df['date'])), cur_df['diff'], legend=code_list[i], line_width=3, line_color=colors[color_cnt])
            else:
                p.line(np.arange(len(cur_df['date'])), cur_df['diff'], legend=code_list[i], line_width=1, line_color=colors[color_cnt])
            color_cnt = color_cnt + 1
# 单击取消显示
        p.legend.click_policy = "hide"
    # put legend at right and add hover print values(y)
        p.add_layout(p.legend[0], 'right')
        h = HoverTool(tooltips=[
            ('Values', '@y')
        ])
        p.add_tools(h)

    # decorte
        p.title.text = title
        p.legend.location = "top_left"
        p.grid.grid_line_alpha = 0
        p.xaxis.axis_label = 'Date'
        p.yaxis.axis_label = 'Value'
        p.ygrid.band_fill_color = "olive"
        p.ygrid.band_fill_alpha = 0.1

        #create layout
        from bokeh.layouts import layout,gridplot
        # create layout
        layout = layout(
            [
                [p],
            ]
        )
        layout_name[count] = layout
        count+=1
    
    grid = gridplot(layout_name, ncols=1)
    show(grid)



def plot_diff_cfe_table(diff_list):
    from bokeh.models import ColumnDataSource, HoverTool
    from bokeh.plotting import figure, output_file, show
    from bokeh.io import output_notebook
    
    
    # save file
    output_file(filename="diff.html", title="%s.html"%diff_list)
    
    
    
    from sqlalchemy import create_engine
    from bokeh.plotting import figure, output_notebook, show ,save
    
    # read data from    financial_tpd.future_index
    engine = create_engine('mysql+pymysql://root:a23187@127.0.0.1/financial_tpd')
    df_price =  pd.read_sql_table('future_index',engine,columns=['date','secucode','open','high','low','close','volume','oi'])
    df_price = df_price.rename(columns={'secucode':'code'})
    
    # broker 多图并列
    layout_name = [] 
   # 创建变量的list
    createVar = locals()
    for i in range(len(diff_list)):  
        createVar['layout'+ str(i)] = 0
        layout_name.append(createVar['layout'+ str(i)])
    count = 0
    
    for i in range(len(diff_list)):
        attr = diff_list[i][0]
        multi_A = diff_list[i][1]
        secu_A = diff_list[i][2]
        main_A = diff_list[i][3]
        multi_B = diff_list[i][4]
        secu_B = diff_list[i][5]
        main_B = diff_list[i][6]
        zhengtao = diff_list[i][7]
        
        
        # 获取 secuA和secuB指定月份合约
        dataA = df_price[df_price.code.apply(lambda x: secu_A == ''.join([i for i in x[:2] if i.isalpha()]) and x.split('.')[0][-2:] == main_A)]
        # 提取code中是字母的部分，'.'前两位代表月份
        dataB = df_price[df_price.code.apply(lambda x: secu_B == ''.join([i for i in x[:2] if i.isalpha()]) and x.split('.')[0][-2:] == main_B)]


        # remove jiaogeyue
        dataA = dataA[dataA['date'].apply(lambda x: x.date().month != int(main_A))]
        dataB = dataB[dataB['date'].apply(lambda x: x.date().month != int(main_B))]

        # on the same day
        plot_df = dataA.merge(dataB, on='date')

        # attr of A and B
        compareA = attr + '_x'
        compareB = attr + '_y'
        
        # 差值 and normalize
        plot_df['diff'] = (multi_A * plot_df[compareA] - multi_B * plot_df[compareB])/plot_df[compareA]*plot_df[compareA].iloc[-1]
        
        # sort by date
        plot_df.sort_values('date')
        #    plot_df['year'] = plot_df['date'].apply(lambda x: x.year)
        #    plot_df['month'] = plot_df['date'].apply(lambda x: x.date().month)
        # 获取code中代表年份月份信息，e.g. 2210 of RB2210
        code_x_line = plot_df.code_x.apply(lambda x: x.split('.')[0][-4:])
        code_y_line = plot_df.code_y.apply(lambda x: x.split('.')[0][-4:])
        # CZC品种2020年之后合约代码只有3位，例如210代表2210，将其补齐。
        code_x_line = code_x_line.apply(lambda x: '2' + x[1:] if(x[0].isalpha()) else x)
        code_y_line = code_y_line.apply(lambda x: '2' + x[1:] if(x[0].isalpha()) else x)
        # 2210<2301
        zhengtao_line = code_x_line <= code_y_line
        # 2210<2301 , 2110<2201 ...
        if(zhengtao == True):
            plot_df = plot_df[zhengtao_line]
        else:
            plot_df = plot_df[zhengtao_line.apply(lambda x:not x)]

        # all unique code (secu_A)
        code_list = list(plot_df['code_x'].unique())
        code_list.sort()
        # first is nearest
        code_list.reverse()

    #    from bokeh.models.widgets import CheckboxGroup

    # list of colors RGB
        colors =[(255,0,0),(255,182,193),(255,0,255),(75,0,130),(100,149,237),(0,191,255),
                 (0,255,255),(60,179,113),(255,255,0),(255,165,0),
                 (128,128,128),(3,21,193),(0,128,128),(255,121,193),]

        # show in notebook
    #    output_notebook()
        # datetime zuhouyiqi

        # get nearest secu's start and end date
        cur_df = plot_df[plot_df.code_x == code_list[0]]
        start_date = cur_df.iloc[0].date
        end_date = cur_df.iloc[-1].date
        # expression of value and period
        title = "%d %s %s- %d %s %s  %s-%s"%(multi_A,secu_A,main_A,multi_B,secu_B,main_B,start_date,end_date)

        # bokeh figure
        p = figure(  
           tools="pan,box_zoom,reset,save",  
           x_axis_label='date', y_axis_label='diff'  ,width=900, height=500
        )  

        color_cnt = 0
        # every secu_A in list
        for i in range(len(code_list)):
            cur_df = plot_df[plot_df.code_x == code_list[i]]
            # line of first/nearest secu is red and thicker
            if(i == 0):
            # x seq of date to 0,1,2,3,...
                p.line(np.arange(len(cur_df['date'])), cur_df['diff'], legend=code_list[i], line_width=3, line_color=colors[color_cnt])
            else:
                p.line(np.arange(len(cur_df['date'])), cur_df['diff'], legend=code_list[i], line_width=1, line_color=colors[color_cnt])
            color_cnt = color_cnt + 1
# 单击取消显示
        p.legend.click_policy = "hide"
    # put legend at right and add hover print values(y)
        p.add_layout(p.legend[0], 'right')
        h = HoverTool(tooltips=[
            ('Values', '@y')
        ])
        p.add_tools(h)

    # decorte
        p.title.text = title
        p.legend.location = "top_left"
        p.grid.grid_line_alpha = 0
        p.xaxis.axis_label = 'Date'
        p.yaxis.axis_label = 'Value'
        p.ygrid.band_fill_color = "olive"
        p.ygrid.band_fill_alpha = 0.1

        #create layout
        from bokeh.layouts import layout,gridplot
        # create layout
        layout = layout(
            [
                [p],
            ]
        )
        layout_name[count] = layout
        count+=1
    
    grid = gridplot(layout_name, ncols=1)
    show(grid)


'''
# example

# single p
attr = 'close' # close open high low volume amount open_interest
multi_A = 1
secu_A = 'RB'
main_A = '06'
multi_B = 3
secu_B = 'RB'
main_B = '09'
zhengtao = True       # True 2206-2309  False  2206-2109

plot_diff(attr = attr, multi_A = multi_A, secu_A = secu_A, main_A = main_A, multi_B = multi_B, secu_B = secu_B, main_B = main_B,zhengtao=zhengtao)


# table
#I V JM J FG SA 1-5 5-9 9-1
#RB HC  1-5 5-10 10-1
#PP-3MA
#i rb hc j jm fg sa v pp-3ma
# 黑色 I JM J RB HC
# 化工 PVC FG SA 
# 跨品种 PP-3MA
diff_list = [['close',1,'I','01',1,'I','05',True],['close',1,'I','05',1,'I','09',True],['close',1,'I','09',1,'I','01',True],
             ['close',1,'RB','01',1,'RB','05',True],['close',1,'RB','05',1,'RB','10',True],['close',1,'RB','10',1,'RB','01',True],
             ['close',1,'HC','01',1,'HC','05',True],['close',1,'HC','05',1,'HC','10',True],['close',1,'HC','10',1,'HC','01',True],
             ['close',1,'J','01',1,'J','05',True],['close',1,'J','05',1,'J','09',True],['close',1,'J','09',1,'J','01',True],
             ['close',1,'JM','01',1,'JM','05',True],['close',1,'JM','05',1,'JM','09',True],['close',1,'JM','09',1,'JM','01',True],
             ['close',1,'FG','01',1,'FG','05',True],['close',1,'FG','05',1,'FG','09',True],['close',1,'FG','09',1,'FG','01',True],
             ['close',1,'SA','01',1,'SA','05',True],['close',1,'SA','05',1,'SA','09',True],['close',1,'SA','09',1,'SA','01',True],
             ['close',1,'V','01',1,'V','05',True],['close',1,'V','05',1,'V','09',True],['close',1,'V','09',1,'V','01',True],
             ['close',1,'PP','01',3,'MA','05',True],['close',1,'PP','05',3,'MA','09',True],['close',1,'PP','09',3,'MA','01',True]
            ]

plot_diff_table(diff_list)


# CEF
diff_list = [['close',1,'IC','08',1,'IC','12',True],['close',1,'IC','08',1,'IC','03',True]]

plot_diff_cef_table(diff_list)



'''
