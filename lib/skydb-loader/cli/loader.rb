require 'skydb-loader/cli/config'
require 'skydb-loader/loader'

module SkydbLoader
  module Cli
    module Loader
      def load(config)

        failures = SkydbLoader::Loader::load_from_folder(
          config.folder,
          config.table,
          config.db,
          config.separator,
          config.encloser
        )

        unless failures.empty?
          error = "Load of following files failed (reason in brackets):\n" + \
                  failures.map{|f| " - " + f}.join("\n")
          raise SkydbLoader::Loader::LoadError, error
        end
      end
      module_function :load
    end
  end
end
