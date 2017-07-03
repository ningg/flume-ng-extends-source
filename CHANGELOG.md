##修改说明
###中文乱码问题
**在flume 原生的LineDeserializer中，中文字符进入buffer时可能会发生字符截断问题，之后在readchar的时候对半个汉字转码，造成乱码。造成这一现象的原因是文件一边在写flume一边在读，在某个时刻很可能读到半个字符。**

**解决方法**:

* 将inputCharset设置为ISO8859-1单字节编码的格式，LineDeserializer按照单字节解码，得到event后再主动将ISO8859-1转成源文件格式（GBK）

* 文件收集过程：FILE ->BUFF(->BYTEBUF)->CHAR->EVENT->SINK

* 解码过程：GBK-->getByte(GBK) -->decoder.deocde(ISO8859-1)-->event.setbody(char.getByte(ISO8859-1))-->new String(event.getbody, "GBK") -->new String(String.getByte( "UTF-8"), ''UTF-8")

因此增加了几个参数解决中文字符的问题
* producer.sources.s1.inputCharset=ISO8859-1（固定值）
* producer.sources.s1.originFileCharset=GBK（源文件编码格式）
* producer.sources.s1.needConvertAfterSource=false （是否在进入channel之前转为UTF-8格式，不是必须的参数）


###收集过的历史文件不想侵入源文件名
在原来实现中，修改后的文件会加一个后缀表示文件，如.COMPLETED, 在一些业务场景下不希望flume把原业务日志文件修改了名字。

**解决方法**:

在收集过程中，新建一个文件，当读到下一个新文件时，就会将原来读的文件的lastmodify按照targetFilename(如yyyy-MM-dd-HH)格式写入到一个文件中，这个文件专门记载已读的文件的时间戳，并以此时间来确定哪些是历史文件。

默认会在日志文件路径下为每个source新建一个以source名字为目录的路径，在下面生成这个文件，此处实现不需要单独再配置参数。

###嵌入了flume 原生的sourcecounter计数功能

为了统计方便，现在在自定义的source里面嵌入了一个sourcecounter，并且每隔10s中打印一次计数器里面的内容。