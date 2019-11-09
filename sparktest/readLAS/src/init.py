'''
dir和package都可以通过  import 文件夹名.模块名  引入模块
dir和package都可以通过  form 文件夹名 import 模块名  引入模块

不同的是 不能直接import dir 没有什么效果 
但可以直接import package  会执行__init__.py文件   __init__.py文件中import的模块可以通过package.moudele访问
并且通过from package import * 可以引入__init__.py内引入的所有模块

这样做的好处：
当一个包中的模块太多时，通过一个一个import 文件夹名.模块名或者form 文件夹名 import 模块名太繁杂
但是通过package  只需要引入package进行管理即可，通过package.module访问
并且通过子包和子包的__init__.py文件，可以导入包下所有子包的模块



对于其余路径下载py文件，要想导入，先加到sys.path中，然后再导入。
__init__.py的作用：
package的标识，不能删除
定义package中的__all__，用来模糊导入
编写Python代码(不建议在__init__中写python模块，可以在包中在创建另外的模块来写，尽量保证__init__.py简单）

pipreqs . --encoding=utf8 --force
'''

