使用方法：
1. 安装python3，python3，python3！
2. 管理员权限打开cmd窗口，执行如下两个命令安装python库：
    pip3 install xlwt，
    pip3 install lxml 
3. 双击脚本，输入名称和产品url, 产品名称用于生成数据文件名，不传入将自动获取, url可以是产品主页url，也可以是评论页面url
4. 默认100个线程下载，网络不好可以减小一些，否则会有很多连接超时，修改文件头部MAX_CONNECTION值即可
