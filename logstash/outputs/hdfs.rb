require "logstash/namespace"
require "logstash/outputs/base"

# HDFS output.
#
# Write events to files to HDFS. You can use fields from the
# event as parts of the filename.
class LogStash::Outputs::HDFS < LogStash::Outputs::Base

  config_name "hdfs"
  milestone 1

  # The path to the file to write. Event fields can be used here, 
  # like "/var/log/logstash/%{@source_host}/%{application}"
  config :path, :validate => :string, :required => true

  # The format to use when writing events to the file. This value
  # supports any string and can include %{name} and other dynamic
  # strings.
  #
  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string

  # Flush interval for flushing writes to log files. 0 will flush on every meesage
  # Flush doesn't actually work on most Hadoop 1.x versions. if you really care about flush, use 2.x 
  config :flush_interval, :validate => :number, :default => 60

  # Enable the use of append. This only works with Hadoop 2.x dfs.support.append or 1.x with dfs.support.broken.append
  config :enable_append, :validate => :boolean, :default => false

  # Enable re-opening files. This is a really a bad idea because HDFS will truncate files. Only use if you know what you're doing
  config :enable_reopen, :validate => :boolean, :default => false

  # The classpath resource locations of the hadoop configuration
  config :hadoop_config_resources, :validate => :array

  public
  def register
    require "java"
    java_import "org.apache.hadoop.fs.Path"
    java_import "org.apache.hadoop.fs.FileSystem"
    java_import "org.apache.hadoop.conf.Configuration"

    @files = {}
    now = Time.now
    @last_flush_cycle = now
    @last_stale_cleanup_cycle = now
    flush_interval = @flush_interval.to_i
    @stale_cleanup_interval = 10
    conf = Configuration.new

    if @hadoop_config_resources
      @hadoop_config_resources.each { |resource|
        conf.addResource(resource)
      }
    end

    @logger.info "Using Hadoop configuration: #{conf.get("fs.defaultFS")}"
    @hdfs = FileSystem.get(conf)
  end # def register

  public
  def receive(event)
    return unless output?(event)
    out = get_output_stream(event.sprintf(@path))

    if @message_format
      output = event.sprintf(@message_format)
    else
      output = event.to_json
    end
    output += "\n" unless output.end_with? "\n"

    out.write(output)

    flush(out)
    close_stale_files
  end # def receive

  def teardown
    @logger.debug("Teardown: closing files")
    @files.each do |path, fd|
      begin
        fd.close
        @logger.debug("Closed file #{path}", :fd => fd)
      rescue Exception => e
        @logger.error("Excpetion while flushing and closing files.", :exception => e)
      end
    end
    finished
  end

  private
  def get_output_stream(path_string)
    return @files[path_string] if @files.has_key?(path_string)
    path = Path.new(path_string)
    if @hdfs.exists(path)
      if enable_append
        begin
          dfs_data_output_stream = @hdfs.append(path)
        rescue java.io.IOException => e
          logger.error("Error opening path for append, trying to recover lease", :exception => e)
          recover_lease(path)
          retry
        end
      elsif enable_reopen
        logger.warn "Overwritting HDFS file", :path => path_string
        dfs_data_output_stream = @hdfs.create(path, true)
      else
        raise IOError, "Cowardly refusing to open pre existing file (#{path_string}) because HDFS will truncate the file!"
      end
    else
      dfs_data_output_stream = @hdfs.create(path)
    end
    @files[path_string] = DFSOutputStreamWrapper.new(dfs_data_output_stream)
  end

  def flush(fd)
    if flush_interval > 0
      flush_pending_files
    else
      fd.flush
    end
  end

  # every flush_interval seconds or so (triggered by events, but if there are no events there's no point flushing files anyway)
  def flush_pending_files
    return unless Time.now - @last_flush_cycle >= flush_interval
    @logger.debug("Starting flush cycle")
    @files.each do |path, fd|
      @logger.debug("Flushing file", :path => path, :fd => fd)
      fd.flush
    end
    @last_flush_cycle = Time.now
  end

  # every 10 seconds or so (triggered by events, but if there are no events there's no point closing files anyway)
  def close_stale_files
    now = Time.now
    return unless now - @last_stale_cleanup_cycle >= @stale_cleanup_interval
    @logger.info("Starting stale files cleanup cycle", :files => @files)
    inactive_files = @files.select { |path, file| not file.active }
    @logger.debug("%d stale files found" % inactive_files.count, :inactive_files => inactive_files)
    inactive_files.each do |path, file|
      @logger.info("Closing file %s" % path)
      file.close
      @files.delete(path)
    end
    # mark all files as inactive, a call to write will mark them as active again
    @files.each { |path, fd| fd.active = false }
    @last_stale_cleanup_cycle = now
  end

  def recover_lease(path)
    is_file_closed_available = @hdfs.respond_to? :isFileClosed
    start = Time.now
    first_retry = true

    until start - Time.now > 900 # 15 minutes timeout 
      recovered = @hdfs.recoverLease(path)
      return true if recovered
      # first retry is fast
      if first_retry
        sleep 4
        first_retry = false
        next
      end

      # on further retries we backoff and spin on isFileClosed in hopes of catching an early break
      61.times do
        return if is_closed_available and @hdfs.isFileClosed(path)
        sleep 1
      end
    end
    false
  end

  class DFSOutputStreamWrapper
    # reflection locks java objects, so only do this once
    if org.apache.hadoop.fs.FSDataOutputStream.instance_methods.include? :hflush
      # hadoop 2.x uses hflush
      FLUSH_METHOD = :hflush
    else
      FLUSH_METHOD = :flush
    end
    attr_accessor :active
    def initialize(output_stream)
      @output_stream = output_stream
    end
    def close
      @output_stream.close
    rescue IOException => e
      logger.error("Failed to close file", :exception => e)
    end
    def flush
      if FLUSH_METHOD == :hflush
        @output_stream.hflush
      else
        @output_stream.flush
        @output_stream.sync
      end
    rescue

    end
    def write(str)
      bytes = str.to_java_bytes
      @output_stream.write(bytes, 0, bytes.length)
      @active = true
    end
  end
end # class LogStash::Outputs::File

