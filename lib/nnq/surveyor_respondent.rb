# frozen_string_literal: true

require_relative "socket"
require_relative "routing/surveyor"
require_relative "routing/respondent"

module NNQ
  # SURVEYOR (nng surveyor0): broadcast side of the survey pattern.
  # Sends a survey to all connected respondents, then collects replies
  # within a timed window (`options.survey_time`, default 1s).
  #
  # Only one outstanding survey at a time — sending a new survey
  # abandons the previous one. Respondents are not obliged to reply.
  #
  class SURVEYOR0 < Socket
    # Broadcasts +body+ as a survey to all connected respondents.
    def send_survey(body)
      Reactor.run { @engine.routing.send_survey(body) }
    end


    # Receives the next reply. Raises {NNQ::TimedOut} when the survey
    # window expires.
    #
    # @return [String] reply body
    def receive
      Reactor.run { @engine.routing.receive }
    end


    private


    def protocol
      Protocol::SP::Protocols::SURVEYOR_V0
    end


    def build_routing(engine)
      Routing::Surveyor.new(engine)
    end
  end


  # RESPONDENT (nng respondent0): reply side of the survey pattern.
  # Receives surveys, processes them, and optionally sends replies.
  # Strict alternation: #receive then #send_reply.
  #
  class RESPONDENT0 < Socket
    # Blocks until the next survey arrives.
    #
    # @return [String, nil] survey body, or nil if the socket was closed
    def receive
      Reactor.run { @engine.routing.receive }
    end


    # Routes +body+ back to the surveyor that sent the most recent survey.
    def send_reply(body)
      Reactor.run { @engine.routing.send_reply(body) }
    end


    private


    def protocol
      Protocol::SP::Protocols::RESPONDENT_V0
    end


    def build_routing(engine)
      Routing::Respondent.new(engine)
    end
  end
end
