# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/buffer"

class LogStash::Outputs::BatchedHttp < LogStash::Outputs::Base
  include Stud::Buffer
  # This output lets you `PUT` or `POST` events to a
  # generic HTTP(S) endpoint
  #
  # Additionally, you are given the option to customize
  # the headers sent as well as basic customization of the
  # event json itself.

  config_name "batched_http"
  milestone 1

  # URL to use
  config :url, :validate => :string, :required => :true

  # validate SSL?
  config :verify_ssl, :validate => :boolean, :default => true

  # What verb to use
  # only put and post are supported for now
  config :http_method, :validate => ["put", "post"], :required => :true

  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]
  config :headers, :validate => :hash

  # Content type
  #
  # If not specified, this defaults to the following:
  #
  # * if format is "json", "application/json"
  # * if format is "form", "application/x-www-form-urlencoded"
  config :content_type, :validate => :string

  # This lets you choose the structure and parts of the event that are sent.
  #
  #
  # For example:
  #
  #    mapping => ["foo", "%{host}", "bar", "%{type}"]
  config :mapping, :validate => :hash

  # Set the format of the http body.
  #
  # If form, then the body will be the mapping (or whole event) converted
  # into a query parameter string (foo=bar&baz=fizz...)
  #
  # If message, then the body will be the result of formatting the event according to message
  #
  # Otherwise, the event is sent as json.
  config :format, :validate => ["json", "form", "message"], :default => "json"

  config :message, :validate => :string
  config :batched, :validate => :string

  config :flush_size, :validate => :number, :default => 5000
  config :idle_flush_time, :validate => :number, :default => 1

  public
  def register
    require "ftw"
    require "uri"
    @agent = FTW::Agent.new
    # TODO(sissel): SSL verify mode?

    if @content_type.nil?
      case @format
        when "form" ; @content_type = "application/x-www-form-urlencoded"
        when "json" ; @content_type = "application/json"
      end
    end
    if @format == "message"
      if @message.nil?
        raise "message must be set if message format is used"
      end
      if @content_type.nil?
        raise "content_type must be set if message format is used"
      end
      unless @mapping.nil?
        @logger.warn "mapping is not supported and will be ignored if message format is used"
      end
    end
    buffer_initialize(
    	:max_items => @flush_size,
    	:max_interval => @idle_flush_time,
    	:logger => @logger
    )
  end # def register

  public
  def receive(event)
    return unless output?(event)

    buffer_receive(event)
  end

  def flush(events, teardown=false)
    event_collection = []
    events.each do |ev|
      event_collection << ev
    end
    post(event_collection)
  end

  def post(body)
    @logger.info("Post body: #{body}")

    batched = []
    evts = []
    body.each do |event|
      if @mapping
        evt = Hash.new
        @mapping.each do |k,v|
          evt[k] = event.sprintf(v % { :batched => @batched })
        end
        evts << evt
      else
        evts << event.to_hash
      end
      batched << event.sprintf(@batched)
    end

    case @http_method
    when "put"
      request = @agent.put(@url)
    when "post"
      request = @agent.post(@url)
    else
      @logger.error("Unknown verb:", :verb => @http_method)
    end

    if @headers
      @headers.each do |k,v|
        request.headers[k] = body.first.sprintf(v)
      end
    end

    request["Content-Type"] = @content_type

    begin
      if @format == "json"
        request.body = evts.to_json
      elsif @format == "message"
        request.body = @message % { :batched => batched.join(",") }
      else
        request.body = encode(evts)
      end
      #puts "#{request.port} / #{request.protocol}"
      #puts request
      #puts
      #puts request.body
      response = @agent.execute(request)

      # Consume body to let this connection be reused
      rbody = ""
      response.read_body { |c| rbody << c }
      #puts rbody
    rescue Exception => e
      @logger.warn("Unhandled exception", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
    end
  end # def post

  def encode(hash)
    return hash.collect do |key, value|
      CGI.escape(key) + "=" + CGI.escape(value)
    end.join("&")
  end # def encode

  def teardown
    buffer_flush(:final => true)
  end
end
