require 'skydb-loader/db'

module SkydbLoader
  module Loader
    class LoadError < ArgumentError; end
    
    def load_from_folder(folder, table, db, separator="\t", encloser='')

      unless SkydbLoader::Db.running?(db)
        raise LoadError, "Can not connect to Skydb at #{db.host}:#{db.port}"
      end

      unless SkydbLoader::Db.db_exists?(db)
        raise LoadError, "Can not access database at #{db.path}"
      end      

      files = Dir["#{folder}/**/*"].delete_if{|f|File.directory?(f)}
      
      if files.empty?
        raise LoadError, "No files fount in folder #{folder}"
      end

      files.each do |f|
        puts "Load file #{f} into table #{db.path}/#{table}"
        begin
          SkydbLoader::Db.load_file(f, table, db, separator, encloser)
        rescue LoadError => le
          puts "LOAD ERROR: %s" % le
          failures << "%s (%s)" % [f, le]
        end
      end
 
    end
    module_function :load_from_folder

  end
end

