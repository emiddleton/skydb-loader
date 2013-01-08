require 'skydb-loader/db'

module SkydbLoader
  module Cli
    module Config
      class ConfigError < ArgumentError; end

      LoadFolderConfig = Struct.new(:folder, :table, :db, :separator, :encloser)
      def get_config()
        LoadFolderConfig.new
      end
      module_function :get_config
    end
  end
end
