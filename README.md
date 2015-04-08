# Logstash HDFS plugin

An HDFS plugin for [Logstash](http://logstash.net). This plugin is provided as an external plugin (see Usage below) and is not part of the Logstash project.

# Usage

## Logstash 1.4.x

Run logstash with the `--pluginpath` (`-p`) command line argument to let logstash know where the plugin is. Also, you need to let Java know where your Hadoop JARs are, so set the `CLASSPATH` variable correctly.
On Logstash 1.4.x use the following command (ajusting paths as neccessary of course):

    LD_LIBRARY_PATH="/usr/lib/hadoop/lib/native" GEM_HOME=./logstash-1.4.2/vendor/bundle/jruby/1.9 CLASSPATH=$(find ./logstash-1.4.2/vendor/jar -type f -name '*.jar'|tr '\n' ':'):$(find /usr/lib/hadoop-hdfs -type f -name '*.jar' | tr '\n' ':'):$(find /usr/lib/hadoop -type f -name '*.jar' | tr '\n' ':'):/etc/hadoop/conf java org.jruby.Main -I./logstash-1.4.2/lib ./logstash-1.4.2/lib/logstash/runner.rb agent -f logstash.conf -p ./logstash-hdfs/lib

Note that logstash is not executed with `java -jar` because executable jars ignore external classpath. Instead we put the logstash jar on the class path and call the runner class.
Important: the Hadoop configuration dir containing `hdfs-site.xml` must be on the classpath.

## Logstash 1.5.x

Logstash 1.5.x supports distribution of plugins as rubygems which makes life a lot easier. To install the plugin from the version on rubygems:

    $LOGSTASH_DIR/bin/plugin install logstash-output-hdfs

Or from source (after checking out the source, run in checkout directory):

    $LOGSTASH_DIR/bin/plugin build logstash-output-hdfs.gemspec
    
Then run logstash with the following command:

    LD_LIBRARY_PATH="$HADOOP_DIR/lib/native" CLASSPATH=$(find $HADOOP_DIR/share/hadoop/common/lib/ -name '*.jar' | grep -v sources | tr '\n' ':'):$HADOOP_DIR/share/hadoop/hdfs/hadoop-hdfs-2.4.0.jar:$HADOOP_DIR/share/hadoop/common/hadoop-common-2.4.0.jar:$HADOOP_DIR/conf $LOGSTASH_DIR/bin/logstash agent -f logstash.conf

Hadoop paths may need adjustments depending on the distribution and version you are using. The important thing is to have `hadoop-hdfs`, `hadoop-common` and all the jar files in `common/lib` on the classpath.

The following comman line will work on most distributions (but will take a little longer to load since it loads many unnecessary jars):

    LD_LIBRARY_PATH="/usr/lib/hadoop/lib/native" CLASSPATH=$(find /usr/lib/hadoop-hdfs -type f -name '*.jar' | tr '\n' ':'):$(find /usr/lib/hadoop -type f -name '*.jar' | grep -v sources | tr '\n' ':'):/etc/hadoop/conf $LOGSTASH_DIR/bin/logstash agent -f logstash.conf
 

# HDFS Configuration

By default, the plugin will load Hadoop's configuration from the classpath.  However, a logstash configuration option named 'hadoop_config_resources' has
been added that will allow the user to pass in multiple configuration locations to override this default configuration.

    output {
            hdfs {
                path => "/path/to/output_file.log"
                hadoop_config_resources => ['path/to/configuration/on/classpath/hdfs-site.xml', 'path/to/configuration/on/classpath/core-site.xml']
            }
        }


# HDFS Append and rewriting files

Please note, HDFS versions prior to 2.x do not properly support append. See [HADOOP-8230](https://issues.apache.org/jira/browse/HADOOP-8230) for reference.
To enable append on HDFS, set _dfs.support.append_ in <tt>hdfs-site.conf</tt> (2.x) or _dfs.support.broken.append_ on 1.x, and use the *enable_append* config option:

    output {
        hdfs {
            path => "/path/to/output_file.log"
            enable_append => true
        }
    }

If append is not supported and the file already exists, the plugin will cowardly refuse to reopen the file for writing unless *enable_reopen* is set to true.
This is probably a very bad idea, you have been warned!

# HFDS Flush

Flush and sync don't actually work as promised on HDFS (see [HDFS-536](https://issues.apache.org/jira/browse/HDFS-536)).
In Hadoop 2.x, `hflush` provides flush-like functionality and the plugin will use `hflush` if it is available.
Nevertheless, flushing code has been left in the plugin in case `flush` and `sync` will work on some HDFS implementation.

# License

The plugin is released under the LGPL v3.

