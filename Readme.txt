һ����ĿĿ��
1.������վ�����־������flume�����־��Ȼ��ͨ��kafka�洢��spark��kafka��������ݣ�Ȼ��洢��hbase���Ӷ���ã����������ҳ�Ŀγ̵ķ�������

2.��־��ʽ
$187.10.64.132  2018-11-15 19:48:35     "GET /course/list HTTP/1.1"     200     -
$156.124.167.30 2018-11-15 19:48:35     "GET /class/128.html HTTP/1.1"  404     -
$55.156.187.87  2018-11-15 19:48:35     "GET /class/146.html HTTP/1.1"  404     -
$72.132.64.46   2018-11-15 19:48:35     "GET /class/130.html HTTP/1.1"  200     -
$187.87.55.72   2018-11-15 19:48:35     "GET /learn/821 HTTP/1.1"       500     ht
tps://search.yahoo.com/search?p=Stormʵս
$156.87.124.10  2018-11-15 19:48:35     "GET /class/146.html HTTP/1.1"  200     -
$29.156.55.10   2018-11-15 19:48:35     "GET /class/128.html HTTP/1.1"  404     -
$29.10.46.30    2018-11-15 19:48:35     "GET /class/128.html HTTP/1.1"  200     -
$87.55.46.72    2018-11-15 19:48:35     "GET /class/128.html HTTP/1.1"  200     -

����ʵ��
1.python�ű�ʵ��ģ����־����
\pythonscript\python-log-generator.py
2.��������
��1��hdfs
��2��zookeeper
��3��hbase
��4��kafka server
			bin/kafka-server-start.sh -daemon config/server.properties

3.����flume��ز�����־streaming_project2.conf

exec-memory-kafka.sources = exec-source
exec-memory-kafka.sinks = kafka-sink
exec-memory-kafka.channels = memory-channel

exec-memory-kafka.sources.exec-source.type = exec
exec-memory-kafka.sources.exec-source.command = tail -F /opt/datas/project/logs/access.log
exec-memory-kafka.sources.exec-source.shell = /bin/sh -c

exec-memory-kafka.channels.memory-channel.type = memory

exec-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
exec-memory-kafka.sinks.kafka-sink.brokerList = bigdata.ibeifeng.com:9092
exec-memory-kafka.sinks.kafka-sink.topic = streamingtopic
exec-memory-kafka.sinks.kafka-sink.batchSize = 5
exec-memory-kafka.sinks.kafka-sink.requiredAcks = 1

exec-memory-kafka.sources.exec-source.channels = memory-channel
exec-memory-kafka.sinks.kafka-sink.channel = memory-channel

4.ִ��flume
bin/flume-ng agent \
--name exec-memory-kafka \
--conf conf \
--conf-file /opt/datas/project/streaming_project2.conf \
-Dflume.root.logger=INFO,console

5.streaming����kafka������
����ImoocStatStreamingApp_product.scala�����Խ���־����ͳ�ƽ���ϴ���hbase��

6.���ImoocStatStreamingApp_product.scala�����ڷ�����
bin/spark-submit --master local[2] \
--jars $(echo /opt/modules/hbase-0.98.6-hadoop2/lib/*.jar | tr ' ' ',') \
--class _0924MoocProject.ImoocStatStreamingApp_product \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
/opt/datas/project/scalaProjectMaven.jar \
bigdata.ibeifeng.com:2181/kafka08 test streamingtopic 1


