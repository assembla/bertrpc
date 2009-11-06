module BERTRPC
  class Action
    include Encodes

    def initialize(svc, req, mod, fun, args)
      @svc = svc
      @req = req
      @mod = mod
      @fun = fun
      @args = args
    end

    def execute
      bert_request = encode_ruby_request(t[@req.kind, @mod, @fun, @args])

      transaction(bert_request) do |sock, bert_response|
        response = decode_bert_response(bert_response)

        if response.is_a?(StringIO)
          if @svc.stream.nil?
            raise "Service stream must be set"
          end

          # skip empty reply
          reply = decode_bert_response read_response(sock)

          total = 0

          #read stream
          loop do
            len = sock.read(4).unpack("N").first
            break if len == 0

            chunk_size = 4 * 1024

            while len > 0
              c = len > chunk_size ? chunk_size : len
              total += c
              @svc.stream.write sock.read(c)
              len -= c
            end
          end

          return total
        else
          return response
        end
      end
    end

    #private

    def write(sock, bert)
      sock.write([bert.length].pack("N"))
      sock.write(bert)
    end

    def transaction(bert_request)
      sock = connect_to(@svc.host, @svc.port, @svc.timeout)

      if @req.options
        if @req.options[:cache] && @req.options[:cache][0] == :validation
          token = @req.options[:cache][1]
          info_bert = encode_ruby_request([:info, :cache, [:validation, token]])
          write(sock, info_bert)
        end
      end

      write(sock, bert_request)
      yield sock, read_response(sock)
      sock.close
    rescue Errno::ECONNREFUSED
      raise ConnectionError.new("Unable to connect to #{@svc.host}:#{@svc.port}")
    rescue Timeout::Error
      raise ReadTimeoutError.new("No response from #{@svc.host}:#{@svc.port} in #{@svc.timeout}s")
    end

    def read_response(sock)
      lenheader = sock.read(4)
      raise ProtocolError.new(ProtocolError::NO_HEADER) unless lenheader
      len = lenheader.unpack('N').first
      bert_response = sock.read(len)
      raise ProtocolError.new(ProtocolError::NO_DATA) unless bert_response
      bert_response
    end

    # Creates a socket object which does speedy, non-blocking reads
    # and can perform reliable read timeouts.
    #
    # Raises Timeout::Error on timeout.
    #
    #   +host+ String address of the target TCP server
    #   +port+ Integer port of the target TCP server
    #   +timeout+ Optional Integer (in seconds) of the read timeout
    def connect_to(host, port, timeout = nil)
      io = BufferedIO.new(TCPSocket.new(host, port))
      io.read_timeout = timeout
      io
    end
  end
end
