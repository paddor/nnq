# frozen_string_literal: true

require_relative "socket"
require_relative "routing/surveyor"
require_relative "routing/respondent"
require_relative "routing/surveyor_raw"
require_relative "routing/respondent_raw"

module NNQ
  # SURVEYOR (nng surveyor0): broadcast side of the survey pattern.
  # Cooked mode enforces a timed survey window and matches replies by
  # survey id; raw mode fans out with a caller-supplied +header+ and
  # delivers replies as `[pipe, header, body]` with no timer.
  #
  class SURVEYOR0 < Socket
    # Cooked: broadcasts +body+ as a survey to all connected respondents.
    def send_survey(body)
      raise Error, "SURVEYOR#send_survey not available in raw mode" if raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send_survey(body) }
    end


    # Raw: broadcasts +body+ with +header+ to all connected respondents
    # via per-conn send pumps (header is threaded through the
    # protocol-sp header kwarg — no concat). Raises in cooked mode.
    def send(body, header:)
      raise Error, "SURVEYOR#send not available in cooked mode" unless raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send(body, header: header) }
    end


    # Cooked: receives the next reply within the survey window, raises
    # {NNQ::TimedOut} on window expiry. Raw: returns `[pipe, header, body]`
    # and blocks indefinitely (no survey window).
    def receive
      Reactor.run { @engine.routing.receive }
    end


    private


    def protocol
      Protocol::SP::Protocols::SURVEYOR_V0
    end


    def build_routing(engine)
      raw? ? Routing::SurveyorRaw.new(engine) : Routing::Surveyor.new(engine)
    end
  end


  # RESPONDENT (nng respondent0): reply side of the survey pattern.
  # Cooked mode strictly alternates #receive / #send_reply; raw mode
  # exposes the backtrace as an opaque +header+ and the originating
  # surveyor pipe as a live Connection.
  #
  class RESPONDENT0 < Socket
    # Cooked: blocks until the next survey arrives. Raw: returns
    # `[pipe, header, body]`.
    def receive
      Reactor.run { @engine.routing.receive }
    end


    # Cooked: routes +body+ back to the surveyor that sent the most
    # recent survey. Raises in raw mode.
    def send_reply(body)
      raise Error, "RESPONDENT#send_reply not available in raw mode" if raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send_reply(body) }
    end


    # Raw: writes +body+ with +header+ back to +to+. Raises in cooked mode.
    def send(body, to:, header:)
      raise Error, "RESPONDENT#send not available in cooked mode" unless raw?
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send(body, to: to, header: header) }
    end


    private


    def protocol
      Protocol::SP::Protocols::RESPONDENT_V0
    end


    def build_routing(engine)
      raw? ? Routing::RespondentRaw.new(engine) : Routing::Respondent.new(engine)
    end
  end
end
