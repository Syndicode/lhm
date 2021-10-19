# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

module Lhm
  class Error < StandardError
  end

  module Command
    def run(&block)
      validate

      if block_given?
        before
        update_state_before_block
        block.call(self)
        after
        update_state_after_block
      else
        update_state_before_execute
        value = execute
        update_state_after_execute

        value
      end
    rescue => e
      Lhm.logger.error "Error in class=#{self.class}, reverting. exception=#{e.class} message=#{e.message}"
      update_state_when_revert
      
      revert
      raise
    end

    private

    def validate
    end

    def revert
    end

    def update_state_when_revert
    end

    def update_state_before_execute
    end

    def update_state_after_execute
    end

    def update_state_before_block
    end

    def update_state_after_block
    end

    def execute
      raise NotImplementedError.new(self.class.name)
    end

    def before
    end

    def after
    end

    def error(msg)
      raise Error.new(msg)
    end
  end
end
