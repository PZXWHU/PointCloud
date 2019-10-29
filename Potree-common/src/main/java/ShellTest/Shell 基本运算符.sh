#!/bin/bash

//算术运算符
#原生bash不支持简单的数学运算，但是可以通过其他命令来实现，例如 awk 和 expr，expr 最常用。
#expr 是一款表达式计算工具，使用它能完成表达式的求值操作。
val=`expr 2 + 2`
echo "两数之和为 : $val"
#两点注意：
#表达式和运算符之间要有空格，例如 2+2 是不对的，必须写成 2 + 2，这与我们熟悉的大多数编程语言不一样。
#完整的表达式要被 ` ` 包含，注意这个字符不是常用的单引号，在 Esc 键下边。

//关系运算符
#关系运算符只支持数字，不支持字符串，除非字符串的值是数字。
-eq
-ne
-gt
-lt
-ge
-le

//布尔运算符
! 非 [ ! false ] 返回 true
-o 或 [ $a -lt 20 -o $b -gt 100 ] 返回 true
-a 与 [ $a -lt 20 -a $b -gt 100 ] 返回 false

//逻辑运算符
#    && [[ $a -lt 100 && $b -gt 100 ]] 返回 false
#    || [[ $a -lt 100 || $b -gt 100 ]] 返回 true


//字符串运算符
#      =    检测两个字符串是否相等，相等返回 true。   [ $a = $b ] 返回 false。
#     !=
#     -z    检测字符串长度是否为0，为0返回 true。     [ -z $a ] 返回 false。
#     -n     检测字符串长度是否为0，不为0返回 true。   [ -n "$a" ] 返回 true
#     $      检测字符串是否为空，不为空返回 true。     [ $a ] 返回 true。


//文件测试运算符