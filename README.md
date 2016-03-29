# flume-ng-extends-source

Extends source of Flume NG for tailing files and folders.

* [SpoolDirectoryTailFileSource](#spooldirectorytailfilesource)：spooling directory, collect all the historical files and tail the target file;
* [TailFile]：TODO
* [TailDirectory]：TODO
* [KafkaSource](#kafkasource)：consume events from Kafka(Kafka_2.9.2_0.8.2.0)




------------------------

#SpoolDirectoryTailFileSource

* [场景](#场景)
* [要求](#要求)
* [解决方案](#解决方案)
	* [组件版本](#组件版本)
	* [预装环境](#预装环境)
	* [编译](#编译)
	* [安装插件](#安装插件)
	* [配置文件](#配置文件)
	* [配置参数详解](#配置参数详解)
	* [约定条件](#约定条件)
* [交流反馈](#交流反馈)
* [附录](#附录)
	* [方案灵感来源](#方案灵感来源)
	* [可靠性分析](#可靠性分析)
* [参考来源](#参考来源)
	
##场景

收集日志，具体场景：

* 应用运行时，每天都在指定目录下，产生一个新的日志文件，日志文件名称：`HH_2015-02-25.txt`，注：文件名中包含日期信息；
* 应用运行时，向当日日志文件追加内容，例如，在2015年02月25日，应用的运行信息，会实时追加到`HH_2015-02-25.txt`文件中；


实际说个例子：

* 应用生成的日志文件，都在目录：`E:/app/log`下；
* 2015年02月24日，应用在目录`E:/app/log`下，生成文件`HH_2015-02-24.txt`，并且，运行信息会实时追加到此文件中；
* 2015年02月25日，应用在目录`E:/app/log`下，生成新的文件`HH_2015-02-25.txt`，并且，将当日的运行信息实时追加到此文件中；

**特别说明**：此场景的解决方案就是下文提到的`SpoolDirectoryTailFileSource`。


##要求

在上述场景下，要求，实时收集应用的运行日志，整体性能上几点：

* 实时：日志产生后，应以秒级的延时，收集发送走；
* 可靠：一旦日志收集程序终止，保证重启之后，日志数据不丢失，并且不重复发送；
* 历史日志文件处理策略：已经收集过的历史日志文件，应立即删除，或者被移送到指定目录；



##解决方案


###组件版本

工程中，涉及到的组件版本，见下表：

|组件|版本|
|----|----|
|Flume NG|`1.5.2`|


###预装环境

编译工程前，需要预装环境：

* JDK 1.6+
* [Apache Maven 3][Apache Maven 3]



###编译

本工程使用[Apache Maven 3][Apache Maven 3]来编译，可以从[这里](http://maven.apache.org/download.cgi)下载不同OS环境下的Maven安装文件。

执行命令：`mvn clean package`，默认在`${project_root}/target/`目录下，编译生成`flume-ng-extends-source-x.x.x.jar`。




###安装插件

1. 按照上一部分[编译](#编译)获取工程的jar包：`flume-ng-extends-source-x.x.x.jar`；
1. 两种方法，可以在Flume下安装插件：

**方法一**：标准插件安装 *(Recommended Approach)*，具体步骤：

* 在`${FLUME_HOME}`找到目录`plugins.d`，如果没有找到这一目录，则创建目录`${FLUME_HOME}/plugins.d`；
* 在`${FLUME_HOME}/plugins.d`目录下，创建目录`flume-ng-extends-source`，并在其下创建`lib`和`libext`两个子目录；
* 将`flume-ng-extends-source-x.x.x.jar`复制到`plugins.d/flume-ng-extends-source/lib`目录中；

至此，安装插件后，目录结构如下：

	${FLUME_HOME}
	 |-- plugins.d
			|-- flume-ng-extends-source/lib
				|-- lib
					|-- flume-ng-extends-source-x.x.x.jar
				|-- libext

Flume插件安装的更多细节，参考[Flume User Guide](https://flume.apache.org/FlumeUserGuide.html#the-plugins-d-directory)

				
**疑问**：maven打包时，如何将当前jar包以及其依赖包都导出？
参考[thilinamb flume kafka sink](https://github.com/thilinamb/flume-ng-kafka-sink)


**方法二**：快速插件安装 *(Quick and Dirty Approach)*，具体步骤：

* 将`flume-ng-extends-source-x.x.x.jar`复制到`${FLUME_HOME}/lib`目录中；



###配置文件

在flume的配置文件`flume-conf.properties`中，配置`agent`下`SpoolDirectoryTailFileSource` source，参考代码如下：

	# Spooling dir and tail file Source 
	agent.sources.spoolDirTailFile.type = com.github.ningg.flume.source.SpoolDirectoryTailFileSource
	# on WIN plantform spoolDir should be format like: E:/program files/spoolDir
	# Note: the value of spoolDir MUST NOT be surrounded by quotation marks.
	agent.sources.spoolDirTailFile.spoolDir = /home/storm/goodjob/spoolDir
	agent.sources.spoolDirTailFile.fileSuffix = .COMPLETED
	agent.sources.spoolDirTailFile.deletePolicy = never 
	agent.sources.spoolDirTailFile.ignorePattern = ^$
	agent.sources.spoolDirTailFile.targetPattern = .*(\\d){4}-(\\d){2}-(\\d){2}.*
	agent.sources.spoolDirTailFile.targetFilename = yyyy-MM-dd
	agent.sources.spoolDirTailFile.trackerDir = .flumespooltail
	agent.sources.spoolDirTailFile.consumeOrder = oldest
	agent.sources.spoolDirTailFile.batchSize = 100
	agent.sources.spoolDirTailFile.inputCharset = UTF-8
	agent.sources.spoolDirTailFile.decodeErrorPolicy = REPLACE
	agent.sources.spoolDirTailFile.deserializer = LINE


###配置参数详解

详细配置参数如下表（Required properties are in **bold**.）：

|Property Name|	Default|	Description|
|------|------|------|
|**channels**|	–	 |  |
|**type**|	–	|The component type name, needs to be `com.github.ningg.flume.source.SpoolDirectoryTailFileSource`.|
|**spoolDir**|	–	|The directory from which to read files from.|
|fileSuffix|	`.COMPLETED`|	Suffix to append to completely ingested files|
|deletePolicy|	`never`|	When to delete completed files: `never` or `immediate`|
|fileHeader|	`false`|	Whether to add a header storing the absolute path filename.|
|fileHeaderKey|	`file`|	Header key to use when appending absolute path filename to event header.|
|basenameHeader|	`false`|	Whether to add a header storing the basename of the file.|
|basenameHeaderKey|	`basename`|	Header Key to use when appending basename of file to event header.|
|ignorePattern|	`^$`	|Regular expression specifying which files to ignore (skip)|
|**targetPattern**|	`.*(\\d){4}-(\\d){2}-(\\d){2}.*`	|Regular expression specifying which files to collect|
|**targetFilename**|	`yyyy-MM-dd`	|The Target File's DateFormat, which refers to [java.text.SimpleDateFormat][java.text.SimpleDateFormat]. Infact, there is strong relationship between Property: **targetFilename** and Property: **targetPattern** |
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


**补充**：上述，selector和interceptor的作用？

* selector：通过event对应的Header，来将event发送到对应的channel中；
* interceptor：在event进入channel之前，修改或者删除event，多个interceptor构成一条链；
	
	
###约定条件

使用上述`SpoolDirectoryTailFileSource`的几个约束：

* 按日期，每日生成一个新日志文件；
* 只以追加方式写入当日的日志文件；
* 在source 监听当日的日志文件时，其他进程不会删除当日的日志文件；
	* 思考：如果这一情况发生，怎么办？
	* 解决思路：能否自动重启Flume agent进程，或者只启动收集数据的线程？
* 不会在同一目录下，生成名称完全相同的文件；*（当`deletePolicy`=`immediate`时，无此限制）*
	* 思考：如果这一情况发生，怎么办？
	* 解决思路：技术上解决不是问题，关键是策略，当文件名称相同时，如何应对；


##交流反馈

如果你对这一工程有任何建议，几个途径联系我：

* 在工程下，提出[Isusses](https://github.com/ningg/flume-ng-extends-source/issues)	*（推荐）*
* [在bolg发表评论](http://ningg.github.io/project-flume-ng-extends-source/)


##附录


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

下述3种情况下，`SpoolDirectoryTailFileSource`都有很高的可靠性，保证不丢失数据、不重复发送数据，几种情况*（启动时间、重启时间，两个维度）*：

* 日志一直在追加更新，隔日才启动实时收集程序；
* 实时收集程序，当日终止，当日重启；
* 实时收集程序，当日终止，次日重启；

上面都是借助meta文件来实现的。




##参考来源

* [Apache Flume NG--User Guide][Apache Flume NG--User Guide]
* [java.text.SimpleDateFormat][java.text.SimpleDateFormat]
* [GitHub--tail flume][GitHub--tail flume]
* [Apache Flume NG(source)][Apache Flume NG(source)]


-----------------------------


#KafkaSource

（doing...）正在整理，具体包含：

* KafkaSource中事务保证；
* 如何将KafkaSource依赖的jar包，整理到zip中？



来源：Github上apache flume中kafka source

思考几点：

* Kafka Source中有没有事务保证（transaction）
	* Kafka Source中offset如何重置？（可参考storm-kafka）
	* Flume中channel可以rollback；

	
两类jar包：

* lib中jar包
	* `flume-ng-extends-source-x.x.x.jar`
* libext中jar包
	* `kafka_2.9.2-0.8.2.0.jar`
	* `kafka-clients-0.8.2.0.jar`
	* `metrics-core-2.2.0.jar`
	* `scala-library-2.9.2.jar`
	* `zkclient-0.3.jar`


注：关于KafkaSource用法的细节，参考[文章][Flume实现将Kafka中数据传入ElasticSearch中]。



##参考来源


* [Flume Deveploger Guide][Flume Deveploger Guide]















[Flume Deveploger Guide]:				http://flume.apache.org/FlumeDeveloperGuide.html
[Apache Flume NG--User Guide]:			http://flume.apache.org/FlumeUserGuide.html
[java.text.SimpleDateFormat]:			http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
[GitHub--tail flume]:					https://github.com/search?utf8=%E2%9C%93&q=tail+flume&type=Repositories&ref=searchresults
[Apache Maven 3]:						http://maven.apache.org/
[Apache Flume NG(source)]:				https://github.com/apache/flume



[Flume实现将Kafka中数据传入ElasticSearch中]:			/flume-kafka-source-elasticsearch-sink



