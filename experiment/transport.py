import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import random
mpl.rcParams["font.sans-serif"] = ["SimHei"]
mpl.rcParams["axes.unicode_minus"] = False

def autolabel(rects,bar_width):
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2, 1.03*height, '%s' % float(height),ha='center', va= 'bottom')


x = np.arange(4)
y = [5.772,9.570,12.945,17.545]
y1 = [8.517, 14.063, 18.631,25.275]

bar_width = 0.35
tick_label = ["小", "较小", "较大", "大"]

plt.figure(0)

a = plt.bar(x, y, bar_width, align="center",  label="WebSokcet")
autolabel(a,bar_width)

b = plt.bar(x+bar_width, y1, bar_width, align="center", label="HTTP")
autolabel(b,bar_width)

plt.xlabel("数量级")
plt.ylabel("时间/s")

plt.grid(True, linestyle = '--',axis='y')
plt.xticks(x+bar_width/2, tick_label)
plt.legend()

ax = plt.axes()
#去掉边框
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.show()
