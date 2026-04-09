# frozen_string_literal: true

require_relative "socket"
require_relative "routing/req"
require_relative "routing/rep"

module NNQ
  # REQ (nng req0): client side of request/reply. Single in-flight
  # request per socket. #send_request blocks until the matching reply
  # comes back.
  #
  class REQ < Socket
    # Sends +body+ as a request, blocks until the matching reply
    # arrives. Returns the reply body (without the id header).
    def send_request(body)
      Reactor.run { @engine.routing.send_request(body) }
    end


    private

    def protocol
      Protocol::SP::Protocols::REQ_V0
    end


    def build_routing(engine)
      Routing::Req.new(engine)
    end
  end


  # REP (nng rep0): server side of request/reply. Strict alternation
  # of #receive then #send_reply, per request.
  #
  class REP < Socket
    # Blocks until the next request arrives. Returns the request body.
    def receive
      Reactor.run { @engine.routing.receive }
    end


    # Routes +body+ back to the pipe the most recent #receive came from.
    def send_reply(body)
      Reactor.run { @engine.routing.send_reply(body) }
    end


    private

    def protocol
      Protocol::SP::Protocols::REP_V0
    end


    def build_routing(engine)
      Routing::Rep.new(engine)
    end
  end
end
