# flume-ng-extends-source

Extends source of Flume NG for tailing files and folders.




##场景

实时收集应用的运行日志，其日志特点：

* 应用运行时，每天都在指定目录下，产生一个新的日志文件，日志文件名称：`HH_2015-02-25.txt`，文件名中包含日期信息；
* 应用运行时，向当日日志文件，实时追加内容，例如，在2015年02月25日，应用的运行信息，会实时追加到`HH_2015-02-25.txt`文件中；


实际说个例子：

* 应用生成的日志文件，都在目录：`E:/app/log`下；
* 2015年02月24日，应用在目录`E:/app/log`下，生成文件`HH_2015-02-24.txt`，并将运行日志，实时追加到此文件中；
* 2015年02月25日，应用在目录`E:/app/log`下，生成新的文件`HH_2015-02-25.txt`，并将当日的运行日志，实时追加到此文件中；


##要求

在上述场景下，要求，实时收集应用的运行日志，整体性能上几点：

* 实时：日志一产生，就以秒级的延时，收集发送走；
* 可靠：一旦实时日志收集程序异常终止，保证重启之后，日志数据，不丢失，并且不重复发送；
* 历史日志文件处理策略：已经收集过的历史日志文件，应被立即删除，或者被移送到指定目录；

具体细节功能点上，要求几点：

* 中文编码：原始日志文件GBK编码，要避免乱码；
* 多行内容抽取：默认按行读取日志文件，而，实际场景中，多行日志文件才对应一个逻辑单元，需要将多行日志文件，抽取为单行；



##解决方案


###组件版本

工程中，涉及到的组件版本，见下表：

|组件|版本|
|----|----|
|Flume NG|1.5.2|


###方案灵感来源

遇到问题去收集资料，对现有的Flume source进行了简单的浏览，发现Flume的Spooling Directory Source机制，很有意思，几点：

* 遍历指定目录下的文件，收集文件内容；
* 对已经收集了内容的文件，进行删除或者重命名；
* 很高的可靠性，即使进程重启，不会造成Spooling Directory Source产生数据丢失和数据的重复发送；

这一机制，跟我们遇到的场景很像，具体差异点：

* Spooling Directory Source，不允许对文件内容进行追加；
* Spooling Directory Source定制方向：
	* 能够先遍历历史文件，并收集文件内容；
	* 最后，再遍历到当日日志文件（下文称目标文件），并实时收集目标文件的新增内容；
	* 次日时，结束对上一日的目标文件的监听收集，同时，自动转入当日的新的目标文件；


__特别说明__：

* 历史日志文件，按照日志文件的最后编辑日志来排序，日期最小的，为最早的历史日志文件；
* 默认约束：日志文件，约束如下：
	* 追加写入：只以追加方式，写入新增内容；而不能修改之前写入的内容；
	* 当日定论：次日不再编辑前一日的日志；





###可靠性分析

3种情况下，日志实时采集系统的可靠性，保证不丢失数据、不重复发送数据，几种情况：

* 日志一直在记录，隔日才启动实时收集程序；
* 实时收集程序，当日终止，当日重启；
* 实时收集程序，当日终止，次日重启；

上面都是借助meta文件来实现的；



###使用方法

如何使用SpoolDirctoryTailFileSource，具体：

* 打包工程，并将其配置为Flume的插件：
	* 将工程导出为jar包：`flume-ng-extends-source-0.8.0.jar`；
	* 在`FLUME_HOME`下，创建目录：`$FLUME_HOME/plugins.d/spool-dir-tail-file-source/lib`；
	* 将`flume-ng-extends-source-0.8.0.jar`放到`$FLUME_HOME/plugins.d/spool-dir-tail-file-source/lib`目录下；
	* __补充说明__：flume下安装插件的细节，参考[flume官网][Apache Flume NG--User Guide]
* 在配置文件中配置SpoolDirectoryTailFileSource，使其生效：
	* 下文将给出一个配置样板，同时会详细说明每个配置参数；
	
####SpoolDirectoryTailFileSource的配置参数

详细配置参数如下表：

|Property Name|	Default|	Description|
|--|--|--|
|**channels**|	–	 |  |
|**type**|	–	|The component type name, needs to be `spooldir`.|
|**spoolDir**|	–	|The directory from which to read files from.|
|fileSuffix|	`.COMPLETED`|	Suffix to append to completely ingested files|
|deletePolicy|	`never`|	When to delete completed files: `never` or `immediate`|
|fileHeader|	`false`|	Whether to add a header storing the absolute path filename.|
|fileHeaderKey|	`file`|	Header key to use when appending absolute path filename to event header.|
|basenameHeader|	`false`|	Whether to add a header storing the basename of the file.|
|basenameHeaderKey|	`basename`|	Header Key to use when appending basename of file to event header.|
|ignorePattern|	`^$`	|Regular expression specifying which files to ignore (skip)|
|trackerDir|	`.flumespooltail`|	Directory to store metadata related to processing of files. If this path is not an absolute path, then it is interpreted as relative to the spoolDir.|
|consumeOrder|	`oldest`	|In which order files in the spooling directory will be consumed `oldest`, `youngest` and `random`. In case of `oldest` and `youngest`, the last modified time of the files will be used to compare the files. In case of a tie, the file with smallest laxicographical order will be consumed first. In case of `random` any file will be picked randomly. When using `oldest` and `youngest` the whole directory will be scanned to pick the oldest/youngest file, which might be slow if there are a large number of files, while using random may cause old files to be consumed very late if new files keep coming in the spooling directory.|
|maxBackoff	|4000	|The maximum time (in millis) to wait between consecutive attempts to write to the channel(s) if the channel is full. The source will start at a low backoff and increase it exponentially each time the channel throws a ChannelException, upto the value specified by this parameter.|
|batchSize	|100|	Granularity at which to batch transfer to the channel|
|inputCharset|	`UTF-8`|	Character set used by deserializers that treat the input file as text.|
|decodeErrorPolicy|	`FAIL`|	What to do when we see a non-decodable character in the input file. `FAIL`: Throw an exception and fail to parse the file. `REPLACE`: Replace the unparseable character with the “replacement character” char, typically Unicode `U+FFFD`. `IGNORE`: Drop the unparseable character sequence.|
|deserializer|	`LINE`|	Specify the deserializer used to parse the file into events. Defaults to parsing each line as an event. The class specified must implement `EventDeserializer.Builder`.|
|deserializer.*	| 	|Varies per event deserializer.*(设置每个deseralizer的实现类，对应的配置参数)*|
|bufferMaxLines|	–	|(Obselete) This option is now ignored.|
|bufferMaxLineLength|	5000|	(Deprecated) Maximum length of a line in the commit buffer. Use `deserializer.maxLineLength` instead.|
|selector.type|	`replicating`|	`replicating` or `multiplexing`|
|selector.*	| 	|Depends on the `selector.type` value|
|interceptors|	–	|Space-separated list of interceptors|
|interceptors.*	|  |  |


**疑问**：上述，selector和interceptor的作用？

* selector：通过event对应的Header，来将event发送到对应的channel中；
* interceptor：？

####SpoolDirectoryTailFileSource的配置样板文件

在flume的配置文件`flume-conf.properties`中，配置`agent`下`spoolDirTailFile` source：

	# Spooling dir and tail file Source 
	agent.sources.spoolDirTailFile.type = com.github.ningg.flume.source.SpoolDirectoryTailFileSource
	agent.sources.spoolDirTailFile.spoolDir = /home/storm/goodjob/spoolDir
	agent.sources.spoolDirTailFile.fileSuffix = .COMPLETED
	agent.sources.spoolDirTailFile.ignorePattern = ^$
	agent.sources.spoolDirTailFile.targetPattern = .*(\\d){4}-(\\d){2}-(\\d){2}.*
	agent.sources.spoolDirTailFile.targetFilename = yyyy-MM-dd
	agent.sources.spoolDirTailFile.trackerDir = .flumespooltail
	agent.sources.spoolDirTailFile.consumeOrder = oldest
	agent.sources.spoolDirTailFile.batchSize = 100
	agent.sources.spoolDirTailFile.inputCharset = UTF-8
	agent.sources.spoolDirTailFile.decodeErrorPolicy = REPLACE
	agent.sources.spoolDirTailFile.deserializer = LINE




###潜在问题

几点：

* win server 2003下，指定目录的操作权限问题，即，启动的进程是否有目录的写权限；
* 直接使用Spooling Directory Source方式，默认已经放入spool directory的文件，不能再进行修改，否则异常；
	* 解决思路：去掉限制即可。




##其他对比方案

几种方案：

* 现有tail source
* 基于spooling directory source机制定制的tail source

几个方面：

* 可靠性：数据，不丢失、不重发；
* 处理速率：相同硬件下，更节约计算机资源；


文件命名：

* SpoolDirectoryTailFile
* TailFile
* TailDirectory




##参考来源

* [Apache Flume NG--User Guide][Apache Flume NG--User Guide]













[Apache Flume NG--User Guide]:			http://flume.apache.org/FlumeUserGuide.html

