module Lhm
  module Printer
    class Output
      def write(message)
        print message
      end
    end

    class Base
      def initialize
        @output = Output.new
      end
    end

    class Percentage
      def initialize
        @max_length = 0
      end

      def notify(progress)
        Lhm.logger.info(progress)
      end

      def end
        write('100% complete')
      end

      def exception(e)
        Lhm.logger.error("failed: #{e}")
      end

      private

      def write(message)
        if (extra = @max_length - message.length) < 0
          @max_length = message.length
          extra = 0
        end

        Lhm.logger.info(message)
      end
    end

    class Dot < Base
      def notify(*)
        @output.write '.'
      end

      def end
        @output.write "\n"
      end
    end
  end
end
