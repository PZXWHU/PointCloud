#!/bin/bash

//执行脚本
#1、作为可执行程序
chmod +x ./test.sh  #使脚本具有执行权限
./test.sh  #执行脚本
#2、作为解释器参数
/bin/sh test.sh




//定义变量
your_name="runoob.com"
//用语句给变量赋值
filelist=`ls /etc` #语句在反引号里
filelist=$(ls /etc) #语句在$()里

//使用变量
echo $your_name
echo ${your_name}

//只读变量
myUrl="http://www.google.com"
readonly myUrl

//删除变量
unset variable_name

//变量类型
#运行shell时，会同时存在三种变量：
#1) 局部变量 局部变量在脚本或命令中定义，仅在当前shell实例中有效，其他shell启动的程序不能访问局部变量。
#2) 环境变量 所有的程序，包括shell启动的程序，都能访问环境变量，有些程序需要环境变量来保证其正常运行。必要的时候shell脚本也可以定义环境变量。
#3) shell变量 shell变量是由shell程序设置的特殊变量。shell变量中有一部分是环境变量，有一部分是局部变量，这些变量保证了shell的正常运行



//Shell 字符串
//1.单引号
#单引号字符串的限制：
#单引号里的任何字符都会原样输出，单引号字符串中的变量是无效的；
#单引号字串中不能出现单独一个的单引号（对单引号使用转义符后也不行），但可成对出现，作为字符串拼接使用。

//2.双引号
#双引号的优点：
#双引号里可以有变量
#双引号里可以出现转义字符

//3.拼接字符串
your_name="runoob"
# 使用双引号拼接
greeting="hello, "$your_name" !"
greeting_1="hello, ${your_name} !"
echo $greeting  $greeting_1
# 使用单引号拼接
greeting_2='hello, '$your_name' !'
greeting_3='hello, ${your_name} !'
echo $greeting_2  $greeting_3
//输出结果为:
hello, runoob ! hello, runoob !
hello, runoob ! hello, ${your_name} !

//4获取字符串长度
string="abcd"
echo ${#string} #输出 4

//5.提取子字符串
string="runoob is a great site"
echo ${string:1:4} # 输出 unoo

//6.查找子字符串
string="runoob is a great site"
echo `expr index "$string" io`  # 输出 4
#查找字符 i 或 o 的位置(哪个字母先出现就计算哪个)：





//Shell 数组
#bash支持一维数组（不支持多维数组），并且没有限定数组的大小。
#类似于 C 语言，数组元素的下标由 0 开始编号。获取数组中的元素要利用下标，下标可以是整数或算术表达式，其值应大于或等于 0。
array_name=(value0 value1 value2 value3) or
array_name=(
value0
value1
value2
value3
)  or
array_name[0]=value0
array_name[1]=value1
array_name[n]=valuen
#可以不使用连续的下标，而且下标的范围没有限制

//读取数组
#  ${数组名[下标]}
valuen=${array_name[n]}
#  使用 @ 符号可以获取数组中的所有元素，例如：
echo ${array_name[@]}

//获取数组的长度
# 取得数组元素的个数
length=${#array_name[@]}
# 或者
length=${#array_name[*]}
# 取得数组单个元素的长度
lengthn=${#array_name[n]}





//Shell 传递参数
#我们可以在执行 Shell 脚本时，向脚本传递参数，脚本内获取参数的格式为：$n。n 代表一个数字，1 为执行脚本的第一个参数，2 为执行脚本的第二个参数，以此类推
#其中 $0 为执行的文件名
#另外，还有几个特殊字符用来处理参数：
$#:传递到脚本的参数个数
$*:以一个单字符串显示所有向脚本传递的参数
# 如"$*"用「"」括起来的情况、以"$1 $2 … $n"的形式输出所有参数。
$$:脚本运行的当前进程ID号
$!:后台运行的最后一个进程的ID号
$@:与$*相同，但是使用时加引号，并在引号中返回每个参数
#如"$@"用「"」括起来的情况、以"$1" "$2" … "$n" 的形式输出所有参数。
$-:显示Shell使用的当前选项,与set命令功能相同.
$?:显示最后命令的退出状态,0表示没有错误,其他任何值表明有错误