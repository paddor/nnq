# frozen_string_literal: true

module NNQ
  module Routing
    # Shared backtrace parsing for SP protocols that use the
    # request-id / hop-stack wire format (REQ/REP, SURVEYOR/RESPONDENT).
    #
    # Wire format: one or more 4-byte big-endian words. The terminal
    # word (request or survey id) has its high bit set (0x80). Preceding
    # words (hop ids added by devices) have the high bit clear.
    #
    module Backtrace
      MAX_HOPS = 8 # nng's default ttl

      # Reads 4-byte BE words off the front of +body+, stopping at the
      # first one whose top byte has its high bit set. Returns
      # [backtrace_bytes, remaining_payload] or nil on malformed input.
      def parse_backtrace(body)
        offset = 0
        hops   = 0

        while hops < MAX_HOPS
          return nil if body.bytesize - offset < 4

          word    = body.byteslice(offset, 4)
          offset += 4
          hops   += 1

          if word.getbyte(0) & 0x80 != 0
            return [body.byteslice(0, offset).freeze, body.byteslice(offset..).freeze]
          end
        end

        nil # exceeded TTL without finding terminator
      end


      # Raw-mode TTL check: returns true if +header+ contains at least
      # MAX_HOPS 4-byte words (i.e. forwarding it would push total hops
      # over the cap). Cheap: just bytesize arithmetic.
      def self.too_many_hops?(header)
        header.bytesize >= MAX_HOPS * 4
      end

    end
  end
end
