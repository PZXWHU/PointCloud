import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import random

mpl.rcParams["font.sans-serif"] = ["SimHei"]
mpl.rcParams["axes.unicode_minus"] = False


def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2, 0.2*height, '%s' % int(height),ha='center', va= 'bottom')



def is_contain_hbase(info):
    if "hbase" in info:
        return True
    else:
        return False

def is_time_normal(info):

    time = int(info.split(":")[4])
    if time>10 :
        return False
    else:
        return True


def is_spatial_query(info):
    query_info = info.split("hbase")[1]
    if "[" in query_info :
        return True
    else:
        return False


def is_single_query(info):
    query_info = info.split("hbase")[1]
    if "[" in query_info :
        return False
    else:
        return True

with open('query.log','r') as f:
    content = f.read().splitlines()
content = list(filter(is_contain_hbase, content))
content = list(filter(is_time_normal, content))
spatial_query_list = list(filter(is_spatial_query, content))
single_query_list = list(filter(is_single_query, content))

spatial_query_time = [int(info.split(":")[4]) for info in spatial_query_list]
single_query_time = [int(info.split(":")[4]) for info in single_query_list]

small_spatial_query_time = sum(random.sample(spatial_query_time, 32))
med1_spatial_query_time = sum(random.sample(spatial_query_time, 48))
med2_spatial_query_time = sum(random.sample(spatial_query_time, 57))
big_spatial_query_time = sum(random.sample(spatial_query_time, 70))

small_single_query_time = sum(random.sample(single_query_time, 73))
med1_single_query_time = sum(random.sample(single_query_time, 116))
med2_single_query_time = sum(random.sample(single_query_time, 146))
big_single_query_time = sum(random.sample(single_query_time, 192))

visble_node_num = [73,116,146,192]
single_query_num = visble_node_num
spatial_query_num = [32,48,57,70 ]

print("空间查询平均每次时间：" + str(np.mean(spatial_query_time)))
print("逐个查询平均每次时间：" + str(np.mean(single_query_time)))

x = np.arange(4)
tick_label = ['小','较小','大','较大']
spatial_query_time = [small_spatial_query_time,med1_spatial_query_time,med2_spatial_query_time,big_spatial_query_time]
single_query_time = [small_single_query_time ,med1_single_query_time,med2_single_query_time ,big_single_query_time ]
bar_width = 0.35

fig = plt.figure()

ax = fig.add_subplot(111)
a = ax.bar(x, single_query_time, bar_width, align="center",label = '逐个查询总时间',color="c", alpha=0.5)
b = ax.bar(x+bar_width , spatial_query_time, bar_width, align="center",label = '空间查询总时间', color="b", alpha=0.5)
autolabel(a)
autolabel(b)
ax.set_xlabel("数量级")
ax.set_ylabel("时间/ms")
ax.legend(loc=0)

ax2 = ax.twinx()
ax2.plot(x+bar_width/2, single_query_num,ls='--',label = '逐个查询总次数',marker="o")
ax2.plot(x+bar_width/2, spatial_query_num,ls='--',label = '空间查询总次数',marker="o")

for i in range(4):
    ax2.text(x[i]+bar_width/2,single_query_num[i]+10,single_query_num[i], color = "#8B0000",ha='center', va= 'bottom')
for i in range(4):
    ax2.text(x[i]+bar_width/2,spatial_query_num[i]+10,spatial_query_num[i],color = "#8B0000",ha='center', va= 'bottom')

ax.legend(loc='upper left')
ax2.legend(loc=1)

ax2.set_ylabel("次数")
ax2.tick_params(colors = "#8B0000")
ax.set_ylim(0,900)
ax2.set_ylim(0,250)

plt.xticks(x+bar_width/2, tick_label)

plt.grid(True, linestyle = '--',axis='y')
plt.xticks(x+bar_width/2, tick_label)
plt.show()