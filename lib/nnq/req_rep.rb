# frozen_string_literal: true

require_relative "socket"
require_relative "routing/req"
require_relative "routing/rep"
require_relative "routing/req_raw"
require_relative "routing/rep_raw"

module NNQ
  # REQ (nng req0): client side of request/reply. Cooked mode keeps a
  # single in-flight request and matches replies by id; raw mode bypasses
  # the state machine entirely and delivers replies as
  # `[pipe, header, body]` tuples so the app can correlate verbatim.
  #
  class REQ0 < Socket
    # Cooked: sends +body+ as a request, blocks until the matching reply
    # arrives. Returns the reply body (without the id header). Raises in
    # raw mode — use {#send} / {#receive} there.
    def send_request(body)
      raise Error, "REQ#send_request not available in raw mode" if raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send_request(body) }
    end


    # Raw: round-robins +body+ to the next connected peer with
    # +header+ (typically `[id | 0x80000000].pack("N")`) written
    # verbatim between the SP length prefix and the body. Raises in
    # cooked mode.
    def send(body, header:)
      raise Error, "REQ#send not available in cooked mode" unless raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send(body, header: header) }
    end


    # Raw: blocks until the next reply arrives, returns
    # `[pipe, header, body]`. Raises in cooked mode.
    def receive
      raise Error, "REQ#receive not available in cooked mode" unless raw?
      Reactor.run { @engine.routing.receive }
    end


    private


    def protocol
      Protocol::SP::Protocols::REQ_V0
    end


    def build_routing(engine)
      raw? ? Routing::ReqRaw.new(engine) : Routing::Req.new(engine)
    end
  end


  # REP (nng rep0): server side of request/reply. Cooked mode strictly
  # alternates #receive / #send_reply and stashes the backtrace
  # internally; raw mode exposes the backtrace as an opaque +header+ and
  # the originating pipe as a live Connection, so the app can drive the
  # reply protocol itself (e.g. proxy/device use cases).
  #
  class REP0 < Socket
    # Cooked: returns the next request body. Raw: returns
    # `[pipe, header, body]`.
    def receive
      Reactor.run { @engine.routing.receive }
    end


    # Cooked: routes +body+ back to the pipe the most recent #receive
    # came from. Raises in raw mode.
    def send_reply(body)
      raise Error, "REP#send_reply not available in raw mode" if raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send_reply(body) }
    end


    # Raw: writes +body+ with +header+ (the opaque backtrace handed out
    # by a prior #receive) back to +to+ (the Connection from the same
    # tuple). Silent drop if +to+ is closed. Raises in cooked mode.
    def send(body, to:, header:)
      raise Error, "REP#send not available in cooked mode" unless raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send(body, to: to, header: header) }
    end


    private


    def protocol
      Protocol::SP::Protocols::REP_V0
    end


    def build_routing(engine)
      raw? ? Routing::RepRaw.new(engine) : Routing::Rep.new(engine)
    end
  end


  REQ = REQ0
  REP = REP0
end
