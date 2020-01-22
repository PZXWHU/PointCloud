import math
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit

# 中文乱码和坐标轴负号处理。

plt.rc('font', family='SimHei', weight='bold')
plt.rcParams['axes.unicode_minus'] = False

def f_fit(x,a,b,q):
    return a*np.power(q,x)+b


#d dimension (1,2,3...)
#r refinement (0,1,2,3...)
#L maxLevel  (0,1,2,3...)

def createClod(d,r,L):
    # q = p_first + p_first*q + p_first*q*q + ... + p_first*math.pow(q,n)
    n = (L+1)*(r+1) - 1  # (0,1,2,3...)
    q = math.pow(math.pow(2,d),1.0/(r+1))
    p_first = (1-q)/(1-math.pow(q,n+1))
    data_list = []
    bar_width = 1.0/(r+1.0)
    for i in range(n+1):
        data_list.append([i*bar_width,p_first*math.pow(q,i)])
    x_data = [x[0] + bar_width/2 for x in data_list]
    y_data = [x[1] for x in data_list]
    y_accumluate_data = [sum(y_data[0:i+1]) for i in range(len(y_data))]


    plt.figure(1)
    plt.bar(x_data,y_data,width=bar_width)
    plt.grid(True, linestyle = '--',axis='y')
    plt.xticks(range(0,L+2),range(0,L+2))
    plt.plot(x_data,y_data,c = 'r')
    plt.show()

    plt.figure(2)
    plt.bar(x_data,y_accumluate_data,width=bar_width)
    plt.grid(True, linestyle = '--',axis='y')
    plt.xticks(range(0,L+2),range(0,L+2))
    plt.show()

    plt.figure(3)
    plt.barh(x_data,y_accumluate_data,height=bar_width)
    plt.grid(True, linestyle = '--',axis='x')
    plt.yticks(range(0,L+2),range(0,L+2))
    #plt.xticks(np.arange(0,1,0.1),np.arange(0,1,0.1))
    plt.show()


def clod(random,dimension,maxlevel):
    clod = (np.log((np.power(2,dimension*maxlevel+dimension)-1)*random+1)/(dimension*np.log(2)))
    return clod


if __name__ == '__main__':
    d = 1
    r = 100
    L =10
    n = (L+1)*(r+1) - 1  # (0,1,2,3...)
    q = math.pow(math.pow(2,d),1.0/(r+1))
    p_first = (1-q)/(1-math.pow(q,n+1))
    data_list = []
    bar_width = 1.0/(r+1.0)
    for i in range(n+1):
        data_list.append([i*bar_width,p_first*math.pow(q,i)])
    x_data = [x[0] + bar_width/2 for x in data_list]
    y_data = [x[1] for x in data_list]
    y_accumluate_data = [sum(y_data[0:i+1]) for i in range(len(y_data))]


    plt.figure(1)
    plt.bar(x_data,y_data,width=bar_width)
    plt.grid(True, linestyle = '--',axis='y')
    plt.xticks(range(0,L+2),range(0,L+2))
    #plt.plot(x_data,y_data,c = 'r')
    plt.plot([x[0] for x in data_list],[f_fit(x[0],p_first,0,math.pow(q,1.0/bar_width)) for x in data_list],c = 'r')
    plt.xlabel('层级')
    plt.ylabel('概率')
    plt.show()

    plt.figure(2)
    plt.bar(x_data,y_accumluate_data,width=bar_width)
    plt.grid(True, linestyle = '--',axis='y')
    plt.xticks(range(0,L+2),range(0,L+2))
    #plt.plot([x[0] for x in data_list],[f_fit(x[0],p_first*q/(q-1),p_first/(1-q),math.pow(q,1.0/bar_width)) for x in data_list],c = 'r')
    plt.xlabel('层级')
    plt.ylabel('累计概率')
    plt.show()

    plt.figure(3)
    #plt.barh(x_data,y_accumluate_data,height=bar_width)
    plt.grid(True, linestyle = '--',axis='x')
    plt.yticks(range(0,L+2),range(0,L+2))
    plt.plot(np.arange(0,1,0.01),[clod(rnd,d,L) for rnd in np.arange(0,1,0.01)],c = 'r')
    plt.xlabel('随机数')
    plt.ylabel('层级')
    plt.show()

