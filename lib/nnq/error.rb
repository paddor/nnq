# frozen_string_literal: true

module NNQ
  class Error < RuntimeError
  end


  class ClosedError < Error
  end


  class ProtocolError < Error
  end


  class TimeoutError < Error
  end


  class RequestCancelled < Error
  end


  class ConnectionRejected < Error
  end


  class TimedOut < Error
  end
end
