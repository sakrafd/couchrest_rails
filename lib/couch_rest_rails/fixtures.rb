module CouchRestRails
  module Fixtures

    extend self
    
    def load(database)
      fixture_files = []
      return  "Database '#{database}' doesn't exists" unless (database == "*" ||
                                                              File.exist?(File.join(RAILS_ROOT, CouchRestRails.setup_path, database)))
      Dir[File.join(RAILS_ROOT, CouchRestRails.setup_path, database)].each do |db|
        db_name =COUCHDB_CONFIG[:db_prefix] +  File.basename( db) +
          COUCHDB_CONFIG[:db_suffix]
        res = CouchRest.get("#{COUCHDB_CONFIG[:host_path]}/#{db_name}") rescue nil
        if res
          db_con = CouchRest.database("#{COUCHDB_CONFIG[:host_path]}/#{db_name}")
          Dir.glob(File.join(RAILS_ROOT, CouchRestRails.fixture_path, "#{database}.yml")).each do |file|
            db_con.bulk_save(YAML::load(ERB.new(IO.read(file)).result).map {|f| f[1]})
            fixture_files << File.basename(file)
          end
        end
        if fixture_files.empty?
          return "No fixtures found in #{CouchRestRails.fixture_path}"
        else
          return "Loaded the following fixture files into '#{db}': #{fixture_files.join(', ')}"
        end
      end
    end

    def clear(database)
      unless (database == "*" || File.exist?(File.join(RAILS_ROOT, CouchRestRails.setup_path, database)))
        return  "Database '#{database}' doesn't exist"
      end

      Dir[File.join(RAILS_ROOT, CouchRestRails.setup_path, database)].each do |db|
        db_name = COUCHDB_CONFIG[:db_prefix] +  File.basename( db) + COUCHDB_CONFIG[:db_suffix]
        res = CouchRest.get("#{COUCHDB_CONFIG[:host_path]}/#{db_name}") rescue nil
        if res
          docs = []
          db_con = CouchRest.database("#{COUCHDB_CONFIG[:host_path]}/#{db_name}")
          rows = db_con.get("_all_docs")['rows']
          unless rows.nil?
            rows.each do |row|
              unless row['id'] =~ /^_design/
                docs << {"_id" => row['id'], "_rev" => row['value']['rev'], "_deleted" => true}
              end
            end
          end
          db_con.bulk_delete(docs) unless docs.empty?
          return "All non design documents from '#{database}' deleted successfully"
        else
          return "Unable to connect to database '#{database}'"
        end
      end
    rescue
      return "Unable to clear fixtures from '#{database}"
    end

    def dump(database)
      return  "Database '#{database}' doesn't exists" unless (database == "*" ||
                                                              File.exist?(File.join(RAILS_ROOT, CouchRestRails.setup_path, database)))
      Dir[File.join(RAILS_ROOT, CouchRestRails.setup_path, database)].each do |db|
        db_name =COUCHDB_CONFIG[:db_prefix] +  File.basename( db) +
          COUCHDB_CONFIG[:db_suffix]
        res = CouchRest.get("#{COUCHDB_CONFIG[:host_path]}/#{db_name}") rescue nil
        if res
          File.open(File.join(RAILS_ROOT, CouchRestRails.fixture_path, "#{database}.yml"), 'w' ) do |file|
            yaml_hash = {}
            db_con = CouchRest.database("#{COUCHDB_CONFIG[:host_path]}/#{db_name}")
            docs = db_con.documents(:include_docs =>true )
            docs["rows"].each { |data|
              doc = data["doc"]
              unless  (doc['_id'] =~ /^_design*/) == 0
                doc.delete('_rev')
                yaml_hash[doc['_id']] = doc
              end
            }
            file.write yaml_hash.to_yaml
          end
        end
      end
    end
  end
end
