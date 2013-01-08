require 'socket'
require 'timeout'
require 'csv'
require 'bundler'
require 'fileutils'
Bundler.require
require 'pp'

module SkydbLoader
  module Db
    DbConfig = Struct.new(:host,:port,:path)
      
    @@user_id_h = {}
    @@user_id_a = []
    
    def extract_user(user_hash)
      unless @@user_id_a.include? user_hash
        @@user_id_a << user_hash
        @@user_id_h[user_hash] = @@user_id_a.size
      end
      @@user_id_h[user_hash]
    end
    module_function :extract_user

    def running?(db)
      SkyDB.ping(host:db.host,port:db.port)
    end
    module_function :running?

    def db_exists?(db)
      File.exists?(db.path)
    end
    module_function :db_exists?

    def table_exists?(table,db)
      File.exists?(File.expand_path(db.path,table))
    end
    module_function :table_exists?

    def load_file(file, table, db, seperator='|', encloser='')
      SkyDB.table = table
      
      labels = Array.new(62)
      labels[2]  = :dt
      labels[3]  = :tm
      labels[9]  = :user_id
      labels[12] = :page_url
      labels[14] = :page_referrer
      labels[20] = :ev_category
      labels[21] = :ev_action
      labels[22] = :ev_label
      labels[23] = :ev_property
      labels[24] = :ev_value

      SkyDB.multi do
        CSV.foreach(file,{:col_sep => seperator,:quote_char =>"\0"}) do |row|

          # create hash removing rows without labels
          col_hash = Hash[*(labels.zip(row).delete_if{|a|a[0].nil?}.flatten)]

          # normalize nil values
          vals = col_hash.inject({}){|vs,v|vs[v[0]] = (v[1] =~ /^(\\\N|undefined)$/ ? nil : v[1]);vs}

          # parse timestamp
          dt,tm = vals.delete(:dt),vals.delete(:tm)
          next unless dt =~ /^\d\d\d\d-\d\d-\d\d$/ and tm =~ /^\d\d:\d\d:\d\d$/
          year,month,day =* dt.split(/-/)
          hour,minute,second =* tm.split(/-/)
          timestamp = Time.new(year,month,day,hour,minute,second)

          ev_category,ev_action = vals.delete(:ev_category),vals.delete(:ev_action)

          # load appropriate values for given action
          action = nil
          data = {}
          case

	  # regular url loading
          when ev_category.nil?
            url = vals.delete(:page_url)
            uri = URI.parse(url)
            action = {
              :name     => "page_view",
              :protocol => uri.scheme,
              :host     => uri.host,
              :port     => uri.port,
              :path     => uri.path,
            }
            action[:referrer] = vals.delete(:page_referrer) unless vals[:page_referrer].nil?
            action[:args]     = uri.query unless uri.query.nil?

	    # custom handling of locale encoded in url.
            action[:locale] = uri.path.gsub(/^\/(en|ja|tw|cn).*/,'\1') if uri.path =~ /^\/(en|ja|tw|cn).*/

	  # custom event to save flash version
          when (ev_category.downcase == 'adobeflash' and ev_action.downcase == 'pageloaded')
            action = {
              :name => 'custom.adobeflash.version'
            }
            data[:flash_version] = vals.delete(:ev_value) unless vals[:ev_value].nil?

	  # login event handling
          when (ev_action.downcase == 'login')
            action = {
              :name => 'custom.session.login'
            }
            data[:logged_in] = true
            data[:guid]  = vals.delete(:ev_property) unless vals[:ev_property].nil?
            data[:login] = vals.delete(:ev_label)    unless vals[:ev_label].nil?

	  # logout event handling
          when (ev_action.downcase == 'logout')
            action = {
              :name => 'custom.session.logout'
            }
            data[:logged_in] = false

	  # unknown custom event
          else
            action = {
              :name => "custom.#{ev_category}.#{ev_action}".downcase,
              :page => vals.delete(:page_url)
            }
            [:label,:property,:value].each do |atr|
              ev_key = "ev_#{atr}".to_sym
              action[atr] = vals.delete(ev_key) if vals[ev_key]
            end
          end

          user_id = extract_user(vals.delete(:user_id))
         
          event = {
            :object_id => user_id,
            :timestamp => timestamp,
            :action => action
          }
          event[:data] = data unless data.empty?
          pp event
          
          SkyDB.add_event(
            SkyDB::Event.new(event)
          )
        end
      end
    end
    module_function :load_file
  end

end
