# frozen_string_literal: true

require "protocol/sp"

module NNQ
  # Freezes module-level state so NNQ sockets can be used inside Ractors.
  # Call this once before spawning any Ractors that create NNQ sockets.
  #
  def self.freeze_for_ractors!
    Engine::CONNECTION_FAILED.freeze
    Engine::CONNECTION_LOST.freeze
    Engine::TRANSPORTS.freeze
  end
end

require_relative "nnq/version"
require_relative "nnq/error"
require_relative "nnq/options"
require_relative "nnq/connection"
require_relative "nnq/engine"
require_relative "nnq/socket"
require_relative "nnq/push_pull"
require_relative "nnq/pair"
require_relative "nnq/req_rep"
require_relative "nnq/pub_sub"
